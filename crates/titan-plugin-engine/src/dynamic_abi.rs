//! Stable in-process ABI for dynamically loaded Titan plugins.
//!
//! This is a shared-library boundary, not an RPC protocol. Rust traits and owned standard-library
//! values never cross it. A venue plugin keeps its ConnectorFactory internally and exposes an
//! optional, versioned C interface through `query_interface`; the corresponding host adapter wraps
//! that interface in the normal Rust trait.

use std::{
    ffi::{OsStr, c_void},
    fs::{self, File},
    io::Read,
    panic::{AssertUnwindSafe, catch_unwind},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU8, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use libloading::Library;
use semver::Version;
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::{
    ApiVersion, ErrorKind, EventPublishMetadata, EventPublisher, Plugin, PluginBundle,
    PluginContext, PluginError, PluginFactory, PluginInit, PluginManifest, StopReason,
    TraceContext, ValidationContext, engine_error,
};

pub const TITAN_PLUGIN_MAGIC: u64 = 0x5449_5441_4E50_4C47;
pub const TITAN_HOST_MAGIC: u64 = 0x5449_5441_4E48_4F53;
pub const TITAN_PLUGIN_ENTRY_SYMBOL: &[u8] = b"titan_plugin_entry_v1\0";
pub const TITAN_DYNAMIC_ABI_VERSION: ApiVersion = ApiVersion::new(1, 0);
pub const TITAN_MANIFEST_SCHEMA_MAJOR: u16 = 1;
pub const TITAN_MANIFEST_SCHEMA_MINOR: u16 = 0;
pub const MAX_DYNAMIC_MANIFEST_BYTES: usize = 1024 * 1024;
pub const MAX_DYNAMIC_ERROR_BYTES: usize = 16 * 1024;

pub type TitanPluginHandle = u64;
pub type TitanStatus = i32;
pub const TITAN_STATUS_OK: TitanStatus = 0;
pub const TITAN_STATUS_INVALID_ARGUMENT: TitanStatus = 1;
pub const TITAN_STATUS_HOST_ERROR: TitanStatus = 2;
pub const TITAN_STATUS_PANIC: TitanStatus = 3;

pub const TITAN_STOP_SHUTDOWN: u32 = 0;
pub const TITAN_STOP_RESTART: u32 = 1;
pub const TITAN_STOP_FAILURE: u32 = 2;

/// Buffer whose allocation and release both belong to the producer named by `free`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct TitanBuffer {
    pub data: *mut u8,
    pub len: usize,
    pub capacity: usize,
    pub free: Option<unsafe extern "C" fn(*mut u8, usize, usize)>,
}

impl Default for TitanBuffer {
    fn default() -> Self {
        Self {
            data: std::ptr::null_mut(),
            len: 0,
            capacity: 0,
            free: None,
        }
    }
}

impl TitanBuffer {
    /// Copies a foreign-owned buffer and invokes its matching release function exactly once.
    ///
    /// # Safety
    /// `data..data+len` must remain readable until `free` returns, and `free` must accept the
    /// original `(data, len, capacity)` tuple.
    pub unsafe fn copy_and_free(self) -> Result<Vec<u8>, PluginError> {
        if self.len > self.capacity
            || self.len > isize::MAX as usize
            || self.capacity > isize::MAX as usize
            || (self.len != 0 && self.data.is_null())
        {
            if let Some(free) = self.free {
                // SAFETY: even a malformed buffer descriptor remains producer-owned. The ABI
                // requires its release callback to accept the exact tuple it returned, allowing
                // the host to reject the contents without leaking the foreign allocation.
                unsafe { free(self.data, self.len, self.capacity) };
            }
            return Err(dynamic_error(
                ErrorKind::PluginFailed,
                "copy_dynamic_buffer",
                "plugin returned an invalid buffer",
            ));
        }
        if self.free.is_none() && (self.capacity != 0 || !self.data.is_null()) {
            return Err(dynamic_error(
                ErrorKind::PluginFailed,
                "copy_dynamic_buffer",
                "plugin-owned buffer has no release function",
            ));
        }
        let value = if self.len == 0 {
            Vec::new()
        } else {
            // SAFETY: guaranteed by the caller and validated for null/length above.
            unsafe { std::slice::from_raw_parts(self.data, self.len) }.to_vec()
        };
        if let Some(free) = self.free {
            // SAFETY: the function and allocation tuple are supplied by the same ABI producer.
            unsafe { free(self.data, self.len, self.capacity) };
        }
        Ok(value)
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct TitanEventMetadataV1 {
    pub source_id: u32,
    pub flags: u32,
    pub source_sequence: u64,
    pub exchange_ts: i64,
    pub receive_ts: i64,
    pub publish_ts: i64,
    pub routing_key: u64,
    pub trace_id: u64,
    pub causation_id: u64,
}

impl TitanEventMetadataV1 {
    pub fn trace(self) -> TraceContext {
        TraceContext {
            trace_id: self.trace_id,
            causation_id: self.causation_id,
        }
    }
}

/// Restricted callbacks supplied by the host when a dynamic instance starts. The plugin may keep
/// this pointer only until `stop` returns. Callback context is opaque and host-owned.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TitanHostApiV1 {
    pub magic: u64,
    pub struct_size: u32,
    pub abi_major: u16,
    pub abi_minor: u16,
    pub context: *mut c_void,
    pub publish_event: Option<
        unsafe extern "C" fn(
            *mut c_void,
            *const u8,
            usize,
            u32,
            *const u8,
            usize,
            TitanEventMetadataV1,
        ) -> TitanStatus,
    >,
    pub log: Option<unsafe extern "C" fn(*mut c_void, u32, *const u8, usize) -> TitanStatus>,
    pub now_ns: Option<unsafe extern "C" fn(*mut c_void) -> i64>,
    pub resolve_secret: Option<
        unsafe extern "C" fn(*mut c_void, *const u8, usize, *mut TitanBuffer) -> TitanStatus,
    >,
}

// SAFETY: the descriptor is only constructed with a stable, thread-safe host context. Its
// callbacks synchronize access to that context and contain Rust panics before returning over C.
unsafe impl Send for TitanHostApiV1 {}
// SAFETY: same invariant as `Send`; all callback-visible mutable state is behind a mutex.
unsafe impl Sync for TitanHostApiV1 {}

impl TitanHostApiV1 {
    pub const fn validate_header(&self) -> bool {
        self.magic == TITAN_HOST_MAGIC
            && self.struct_size >= std::mem::size_of::<Self>() as u32
            && self.abi_major == TITAN_DYNAMIC_ABI_VERSION.major
            && self.abi_minor <= TITAN_DYNAMIC_ABI_VERSION.minor
    }

    /// Host descriptor for a package root that exposes only domain-specific query interfaces.
    /// Domain adapters provide their own scoped event and secret callbacks per Connector.
    pub const fn lifecycle_only() -> Self {
        Self {
            magic: TITAN_HOST_MAGIC,
            struct_size: std::mem::size_of::<Self>() as u32,
            abi_major: TITAN_DYNAMIC_ABI_VERSION.major,
            abi_minor: TITAN_DYNAMIC_ABI_VERSION.minor,
            context: std::ptr::null_mut(),
            publish_event: None,
            log: None,
            now_ns: None,
            resolve_secret: None,
        }
    }
}

/// Entry descriptor returned by `titan_plugin_entry_v1`.
///
/// The original v1 prefix ends at `last_error`. New function pointers are appended and guarded by
/// `struct_size`, so a loader can diagnose an older prototype without reading past its allocation.
#[repr(C)]
#[derive(Clone, Copy)]
struct PluginApiHeaderV1 {
    magic: u64,
    struct_size: u32,
    abi_major: u16,
    abi_minor: u16,
    manifest_schema_major: u16,
    manifest_schema_minor: u16,
    required_feature_bits: u64,
    optional_feature_bits: u64,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct PluginApiV1 {
    pub magic: u64,
    pub struct_size: u32,
    pub abi_major: u16,
    pub abi_minor: u16,
    pub manifest_schema_major: u16,
    pub manifest_schema_minor: u16,
    pub required_feature_bits: u64,
    pub optional_feature_bits: u64,
    pub manifest_json: Option<unsafe extern "C" fn(*mut usize) -> *const u8>,
    pub create:
        Option<unsafe extern "C" fn(*const u8, usize, *mut TitanPluginHandle) -> TitanStatus>,
    pub destroy: Option<unsafe extern "C" fn(TitanPluginHandle) -> TitanStatus>,
    pub last_error: Option<unsafe extern "C" fn(*mut u8, usize) -> usize>,
    pub validate: Option<unsafe extern "C" fn(TitanPluginHandle) -> TitanStatus>,
    pub start:
        Option<unsafe extern "C" fn(TitanPluginHandle, *const TitanHostApiV1) -> TitanStatus>,
    pub quiesce: Option<unsafe extern "C" fn(TitanPluginHandle, u32) -> TitanStatus>,
    pub stop: Option<unsafe extern "C" fn(TitanPluginHandle) -> TitanStatus>,
    pub query_interface: Option<
        unsafe extern "C" fn(
            TitanPluginHandle,
            *const u8,
            usize,
            u16,
            *mut *const c_void,
        ) -> TitanStatus,
    >,
}

impl PluginApiV1 {
    pub const PREFIX_SIZE: u32 = (std::mem::offset_of!(Self, last_error)
        + std::mem::size_of::<Option<unsafe extern "C" fn(*mut u8, usize) -> usize>>())
        as u32;

    pub fn validate_descriptor(
        &self,
        host_abi: ApiVersion,
        supported_features: u64,
        manifest_schema_major: u16,
    ) -> Result<(), PluginError> {
        if self.magic != TITAN_PLUGIN_MAGIC {
            return Err(dynamic_error(
                ErrorKind::AbiVersionMismatch,
                "validate_dynamic_abi",
                "invalid plugin magic",
            ));
        }
        if self.struct_size < Self::PREFIX_SIZE {
            return Err(dynamic_error(
                ErrorKind::AbiVersionMismatch,
                "validate_dynamic_abi",
                "plugin descriptor is truncated",
            ));
        }
        if self.abi_major != host_abi.major || self.abi_minor > host_abi.minor {
            return Err(dynamic_error(
                ErrorKind::AbiVersionMismatch,
                "validate_dynamic_abi",
                "dynamic plugin ABI is incompatible",
            ));
        }
        if self.manifest_schema_major != manifest_schema_major {
            return Err(dynamic_error(
                ErrorKind::ManifestSchemaMismatch,
                "validate_dynamic_abi",
                "manifest schema major is incompatible",
            ));
        }
        if self.required_feature_bits & !supported_features != 0 {
            return Err(dynamic_error(
                ErrorKind::UnsupportedAbiFeature,
                "validate_dynamic_abi",
                "plugin requires unsupported ABI features",
            ));
        }
        if self.manifest_json.is_none()
            || self.create.is_none()
            || self.destroy.is_none()
            || self.last_error.is_none()
        {
            return Err(dynamic_error(
                ErrorKind::AbiVersionMismatch,
                "validate_dynamic_abi",
                "required descriptor function is null",
            ));
        }
        Ok(())
    }

    pub fn validate_complete(&self) -> Result<(), PluginError> {
        if self.struct_size < std::mem::size_of::<Self>() as u32 {
            return Err(dynamic_error(
                ErrorKind::AbiVersionMismatch,
                "validate_dynamic_abi",
                "plugin implements only the incomplete v1 prototype",
            ));
        }
        if self.validate.is_none()
            || self.start.is_none()
            || self.quiesce.is_none()
            || self.stop.is_none()
            || self.query_interface.is_none()
        {
            return Err(dynamic_error(
                ErrorKind::AbiVersionMismatch,
                "validate_dynamic_abi",
                "complete lifecycle or interface query function is null",
            ));
        }
        Ok(())
    }

    /// Backward-compatible name retained for callers of the original prototype.
    pub fn validate(
        &self,
        host_abi: ApiVersion,
        supported_features: u64,
        manifest_schema_major: u16,
    ) -> Result<(), PluginError> {
        self.validate_descriptor(host_abi, supported_features, manifest_schema_major)
    }

    fn error_message(&self, fallback: &str) -> Arc<str> {
        let Some(last_error) = self.last_error else {
            return Arc::from(fallback);
        };
        let mut buffer = vec![0_u8; MAX_DYNAMIC_ERROR_BYTES];
        // SAFETY: the descriptor was validated and the buffer is writable for its full length.
        let written = unsafe { last_error(buffer.as_mut_ptr(), buffer.len()) }.min(buffer.len());
        buffer.truncate(written);
        match std::str::from_utf8(&buffer) {
            Ok("") | Err(_) => Arc::from(fallback),
            Ok(message) => Arc::from(message),
        }
    }
}

type PluginEntryV1 = unsafe extern "C" fn() -> *const PluginApiV1;

struct DynamicLibraryInner {
    _library: Library,
    api: PluginApiV1,
    path: PathBuf,
    manifest: Arc<serde_json::Value>,
}

// The first dynamic-ABI release deliberately does not unload code during process lifetime. A
// connector may own foreign worker threads or callbacks that cannot be proven quiescent by Rust's
// type system. Instance/factory leases still express ownership, while this process lease provides
// the stronger safety guarantee required by the v1 design.
static PROCESS_LIBRARY_LEASES: OnceLock<Mutex<Vec<Arc<DynamicLibraryInner>>>> = OnceLock::new();

/// Cloneable code lease. Keeping this value alive prevents the shared library from unloading.
#[derive(Clone)]
pub struct DynamicLibraryLease(Arc<DynamicLibraryInner>);

impl DynamicLibraryLease {
    pub fn path(&self) -> &Path {
        &self.0.path
    }

    pub fn manifest(&self) -> &serde_json::Value {
        &self.0.manifest
    }

    pub fn api(&self) -> PluginApiV1 {
        self.0.api
    }

    pub fn create(&self, config_json: &[u8]) -> Result<DynamicPluginInstance, PluginError> {
        if serde_json::from_slice::<serde_json::Value>(config_json).is_err() {
            return Err(dynamic_error(
                ErrorKind::ConfigInvalid,
                "create_dynamic_plugin",
                "plugin configuration is not valid JSON",
            ));
        }
        let mut handle = 0;
        let create = self.0.api.create.expect("descriptor was validated");
        // SAFETY: config bytes and output handle pointer are valid for the call.
        check_status(
            &self.0.api,
            unsafe { create(config_json.as_ptr(), config_json.len(), &mut handle) },
            "create",
        )?;
        if handle == 0 {
            return Err(dynamic_error(
                ErrorKind::PluginFailed,
                "create_dynamic_plugin",
                "plugin returned an invalid zero handle",
            ));
        }
        Ok(DynamicPluginInstance {
            code: self.clone(),
            handle,
            lifecycle: AtomicU8::new(DYNAMIC_CREATED),
        })
    }
}

const DYNAMIC_CREATED: u8 = 0;
const DYNAMIC_VALIDATING: u8 = 1;
const DYNAMIC_VALIDATED: u8 = 2;
const DYNAMIC_STARTING: u8 = 3;
const DYNAMIC_STARTED: u8 = 4;
const DYNAMIC_QUIESCING: u8 = 5;
const DYNAMIC_QUIESCED: u8 = 6;
const DYNAMIC_STOPPED: u8 = 7;

pub struct DynamicPluginInstance {
    code: DynamicLibraryLease,
    handle: TitanPluginHandle,
    lifecycle: AtomicU8,
}

impl DynamicPluginInstance {
    pub fn code_lease(&self) -> DynamicLibraryLease {
        self.code.clone()
    }

    pub fn handle(&self) -> TitanPluginHandle {
        self.handle
    }

    pub fn validate(&self) -> Result<(), PluginError> {
        match self.lifecycle.compare_exchange(
            DYNAMIC_CREATED,
            DYNAMIC_VALIDATING,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {}
            Err(DYNAMIC_VALIDATED) => return Ok(()),
            Err(_) => {
                return Err(dynamic_state_error(
                    "validate_dynamic_plugin",
                    "plugin instance cannot be validated in its current lifecycle state",
                ));
            }
        }
        let call = self.code.0.api.validate.expect("complete ABI was checked");
        // SAFETY: handle was returned by this descriptor's create function.
        let result = check_status(&self.code.0.api, unsafe { call(self.handle) }, "validate");
        self.lifecycle.store(
            if result.is_ok() {
                DYNAMIC_VALIDATED
            } else {
                DYNAMIC_CREATED
            },
            Ordering::Release,
        );
        result
    }

    /// Starts the foreign instance with a borrowed host callback table.
    ///
    /// # Safety
    /// The plugin ABI permits the instance to retain `host` and its `context`. Both must remain
    /// valid, with all callback invariants upheld, until this instance's [`Self::stop`] returns.
    /// Prefer [`DynamicPluginSession::start`] or [`DynamicLifecyclePluginFactory`], which own the
    /// callback table for the required lifetime.
    pub unsafe fn start(&self, host: &TitanHostApiV1) -> Result<(), PluginError> {
        if !host.validate_header() {
            return Err(dynamic_error(
                ErrorKind::AbiVersionMismatch,
                "start_dynamic_plugin",
                "invalid host API descriptor",
            ));
        }
        self.lifecycle
            .compare_exchange(
                DYNAMIC_VALIDATED,
                DYNAMIC_STARTING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map_err(|_| {
                dynamic_state_error(
                    "start_dynamic_plugin",
                    "plugin instance must be validated exactly once before start",
                )
            })?;
        let call = self.code.0.api.start.expect("complete ABI was checked");
        // SAFETY: handle and host descriptor remain valid for the call. The ABI requires the host
        // descriptor to outlive plugin activity through stop.
        let result = check_status(
            &self.code.0.api,
            unsafe { call(self.handle, host) },
            "start",
        );
        self.lifecycle.store(
            if result.is_ok() {
                DYNAMIC_STARTED
            } else {
                DYNAMIC_VALIDATED
            },
            Ordering::Release,
        );
        result
    }

    pub fn quiesce(&self, reason: u32) -> Result<(), PluginError> {
        match self.lifecycle.compare_exchange(
            DYNAMIC_STARTED,
            DYNAMIC_QUIESCING,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {}
            Err(DYNAMIC_QUIESCED) | Err(DYNAMIC_STOPPED) => return Ok(()),
            Err(_) => {
                return Err(dynamic_state_error(
                    "quiesce_dynamic_plugin",
                    "plugin instance is not running",
                ));
            }
        }
        let call = self.code.0.api.quiesce.expect("complete ABI was checked");
        // SAFETY: handle was returned by this descriptor's create function.
        let result = check_status(
            &self.code.0.api,
            unsafe { call(self.handle, reason) },
            "quiesce",
        );
        self.lifecycle.store(
            if result.is_ok() {
                DYNAMIC_QUIESCED
            } else {
                DYNAMIC_STARTED
            },
            Ordering::Release,
        );
        result
    }

    pub fn stop(&mut self) -> Result<(), PluginError> {
        if self.lifecycle.load(Ordering::Acquire) == DYNAMIC_STOPPED {
            return Ok(());
        }
        let call = self.code.0.api.stop.expect("complete ABI was checked");
        // SAFETY: handle was returned by this descriptor's create function.
        let status = unsafe { call(self.handle) };
        // Stop is an at-most-once ABI transition even if the plugin reports failure. Destroy is
        // still required to reclaim the instance, and Drop must not invoke a failed stop twice.
        self.lifecycle.store(DYNAMIC_STOPPED, Ordering::Release);
        check_status(&self.code.0.api, status, "stop")
    }

    pub fn query_interface(
        &self,
        interface: &str,
        major: u16,
    ) -> Result<*const c_void, PluginError> {
        if !matches!(
            self.lifecycle.load(Ordering::Acquire),
            DYNAMIC_STARTED | DYNAMIC_QUIESCED
        ) {
            return Err(dynamic_state_error(
                "query_dynamic_interface",
                "plugin interface is unavailable before start or after stop",
            ));
        }
        if interface.is_empty() || interface.as_bytes().contains(&0) {
            return Err(dynamic_error(
                ErrorKind::UnsupportedAbiFeature,
                "query_dynamic_interface",
                "invalid interface name",
            ));
        }
        let call = self
            .code
            .0
            .api
            .query_interface
            .expect("complete ABI was checked");
        let mut output = std::ptr::null();
        // SAFETY: input bytes and output pointer are valid for the duration of the call.
        check_status(
            &self.code.0.api,
            unsafe {
                call(
                    self.handle,
                    interface.as_ptr(),
                    interface.len(),
                    major,
                    &mut output,
                )
            },
            "query_interface",
        )?;
        if output.is_null() {
            return Err(dynamic_error(
                ErrorKind::UnsupportedAbiFeature,
                "query_dynamic_interface",
                "plugin returned a null interface",
            ));
        }
        Ok(output)
    }
}

impl Drop for DynamicPluginInstance {
    fn drop(&mut self) {
        if self.lifecycle.load(Ordering::Acquire) != DYNAMIC_STOPPED {
            let _ = self.stop();
        }
        if let Some(destroy) = self.code.0.api.destroy {
            // SAFETY: the code lease is still alive and this handle has not previously been
            // destroyed by the owning instance.
            let _ = unsafe { destroy(self.handle) };
        }
    }
}

struct DynamicPluginSessionInner {
    instance: Mutex<DynamicPluginInstance>,
    host: Box<TitanHostApiV1>,
}

impl Drop for DynamicPluginSessionInner {
    fn drop(&mut self) {
        let instance = self
            .instance
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let _ = instance.quiesce(TITAN_STOP_SHUTDOWN);
        let _ = instance.stop();
        // `host` is intentionally still alive while stop executes.
        let _ = &self.host;
    }
}

/// Shared lifetime for one started dynamic package root and every domain factory obtained from it.
#[derive(Clone)]
pub struct DynamicPluginSession(Arc<DynamicPluginSessionInner>);

impl DynamicPluginSession {
    pub fn start(
        code: DynamicLibraryLease,
        config: &serde_json::Value,
    ) -> Result<Self, PluginError> {
        let config = serde_json::to_vec(config).map_err(|error| {
            dynamic_error(
                ErrorKind::ConfigInvalid,
                "serialize_dynamic_session_config",
                error.to_string(),
            )
        })?;
        let instance = code.create(&config)?;
        instance.validate()?;
        let host = Box::new(TitanHostApiV1::lifecycle_only());
        // SAFETY: the boxed host table is stored beside the instance and is dropped only after the
        // session's explicit stop in `DynamicPluginSessionInner::drop`.
        if let Err(error) = unsafe { instance.start(&host) } {
            drop(instance);
            return Err(error);
        }
        Ok(Self(Arc::new(DynamicPluginSessionInner {
            instance: Mutex::new(instance),
            host,
        })))
    }

    pub fn handle(&self) -> TitanPluginHandle {
        self.0
            .instance
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .handle()
    }

    pub fn query_interface(
        &self,
        interface: &str,
        major: u16,
    ) -> Result<*const c_void, PluginError> {
        self.0
            .instance
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .query_interface(interface, major)
    }

    pub fn code_lease(&self) -> DynamicLibraryLease {
        self.0
            .instance
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .code_lease()
    }
}

#[derive(Clone, Debug)]
pub struct DynamicPluginLoader {
    pub host_abi: ApiVersion,
    pub supported_feature_bits: u64,
    pub manifest_schema_major: u16,
    pub manifest_schema_minor: u16,
    pub max_manifest_bytes: usize,
}

impl Default for DynamicPluginLoader {
    fn default() -> Self {
        Self {
            host_abi: TITAN_DYNAMIC_ABI_VERSION,
            supported_feature_bits: 0,
            manifest_schema_major: TITAN_MANIFEST_SCHEMA_MAJOR,
            manifest_schema_minor: TITAN_MANIFEST_SCHEMA_MINOR,
            max_manifest_bytes: MAX_DYNAMIC_MANIFEST_BYTES,
        }
    }
}

impl DynamicPluginLoader {
    pub fn load_library(&self, path: impl AsRef<Path>) -> Result<DynamicLibraryLease, PluginError> {
        let code = self.load_library_unpinned(path.as_ref())?;
        pin_process_library(&code.0);
        Ok(code)
    }

    fn load_library_unpinned(&self, path: &Path) -> Result<DynamicLibraryLease, PluginError> {
        let path = canonical_file(path.as_ref(), "load_dynamic_plugin")?;
        // SAFETY: loading executes platform loader hooks. The returned Library is retained by every
        // code and instance lease, so resolved symbols cannot outlive it.
        let library = unsafe { Library::new(&path) }.map_err(|error| {
            dynamic_error(
                ErrorKind::PluginFailed,
                "load_dynamic_plugin",
                error.to_string(),
            )
        })?;
        // SAFETY: the symbol name and signature are the mandatory Titan v1 entry contract.
        let entry: PluginEntryV1 = unsafe {
            *library
                .get::<PluginEntryV1>(TITAN_PLUGIN_ENTRY_SYMBOL)
                .map_err(|error| {
                    dynamic_error(
                        ErrorKind::AbiVersionMismatch,
                        "load_dynamic_plugin",
                        error.to_string(),
                    )
                })?
        };
        // SAFETY: entry was resolved with the declared ABI; null is checked before dereference.
        let api_pointer = unsafe { entry() };
        if api_pointer.is_null() {
            return Err(dynamic_error(
                ErrorKind::AbiVersionMismatch,
                "load_dynamic_plugin",
                "plugin entry returned null",
            ));
        }
        if (api_pointer as usize) % std::mem::align_of::<PluginApiHeaderV1>() != 0 {
            return Err(dynamic_error(
                ErrorKind::AbiVersionMismatch,
                "load_dynamic_plugin",
                "plugin entry returned a misaligned descriptor",
            ));
        }
        // Read only the fixed header first. This is essential for diagnosing an older, shorter
        // descriptor without copying bytes beyond the object it actually exported.
        // SAFETY: every v1 entry descriptor starts with `PluginApiHeaderV1`.
        let header = unsafe { &*api_pointer.cast::<PluginApiHeaderV1>() };
        validate_api_header(
            header,
            self.host_abi,
            self.supported_feature_bits,
            self.manifest_schema_major,
            self.manifest_schema_minor,
        )?;
        if header.struct_size < std::mem::size_of::<PluginApiV1>() as u32 {
            return Err(dynamic_error(
                ErrorKind::AbiVersionMismatch,
                "validate_dynamic_abi",
                "plugin implements only the incomplete v1 prototype",
            ));
        }
        // SAFETY: a conforming entry returns a descriptor that remains valid while the library is
        // loaded. The header confirmed that the complete descriptor is present before this copy.
        let api = unsafe { *api_pointer };
        api.validate_descriptor(
            self.host_abi,
            self.supported_feature_bits,
            self.manifest_schema_major,
        )?;
        api.validate_complete()?;
        let manifest = read_manifest(api, self.max_manifest_bytes)?;
        validate_dynamic_manifest(&manifest, self.host_abi)?;
        let inner = Arc::new(DynamicLibraryInner {
            _library: library,
            api,
            path,
            manifest: Arc::new(manifest),
        });
        Ok(DynamicLibraryLease(inner))
    }

    pub fn load_package(
        &self,
        manifest_path: impl AsRef<Path>,
    ) -> Result<LoadedDynamicPackage, PluginError> {
        let manifest_path = canonical_file(manifest_path.as_ref(), "load_plugin_package")?;
        if self.max_manifest_bytes == 0 || self.max_manifest_bytes > MAX_DYNAMIC_MANIFEST_BYTES {
            return Err(dynamic_error(
                ErrorKind::ManifestInvalid,
                "load_plugin_package",
                "invalid host manifest size limit",
            ));
        }
        let manifest_len = fs::metadata(&manifest_path)
            .map_err(|error| {
                dynamic_error(
                    ErrorKind::ManifestInvalid,
                    "load_plugin_package",
                    error.to_string(),
                )
            })?
            .len();
        if manifest_len == 0 || manifest_len > self.max_manifest_bytes as u64 {
            return Err(dynamic_error(
                ErrorKind::ManifestInvalid,
                "load_plugin_package",
                "package manifest exceeds the configured size limit",
            ));
        }
        let bytes = fs::read(&manifest_path).map_err(|error| {
            dynamic_error(
                ErrorKind::ManifestInvalid,
                "load_plugin_package",
                error.to_string(),
            )
        })?;
        let package: DynamicPackageManifest = serde_json::from_slice(&bytes).map_err(|error| {
            dynamic_error(
                ErrorKind::ManifestInvalid,
                "load_plugin_package",
                error.to_string(),
            )
        })?;
        if package.schema_major != self.manifest_schema_major {
            return Err(dynamic_error(
                ErrorKind::ManifestSchemaMismatch,
                "load_plugin_package",
                "package manifest schema major is incompatible",
            ));
        }
        if package.schema_minor > self.manifest_schema_minor {
            return Err(dynamic_error(
                ErrorKind::ManifestSchemaMismatch,
                "load_plugin_package",
                "package manifest schema minor requires a ConfigurationAdapter migration",
            ));
        }
        let package_version = Version::parse(&package.package_version).map_err(|error| {
            dynamic_error(
                ErrorKind::ManifestInvalid,
                "load_plugin_package",
                error.to_string(),
            )
        })?;
        let root = manifest_path.parent().ok_or_else(|| {
            dynamic_error(
                ErrorKind::ManifestInvalid,
                "load_plugin_package",
                "package manifest has no parent directory",
            )
        })?;
        let library_path = canonical_file(&root.join(&package.library), "load_plugin_package")?;
        if !library_path.starts_with(root) {
            return Err(dynamic_error(
                ErrorKind::ManifestInvalid,
                "load_plugin_package",
                "package library escapes its package directory",
            ));
        }
        if let Some(expected) = package.sha256.as_deref() {
            let actual = sha256_file(&library_path)?;
            if !constant_time_hex_eq(expected, &actual) {
                return Err(dynamic_error(
                    ErrorKind::ManifestInvalid,
                    "load_plugin_package",
                    "plugin library digest does not match package manifest",
                ));
            }
        }
        let code = self.load_library_unpinned(&library_path)?;
        let embedded_manifest: PluginManifest = serde_json::from_value(code.manifest().clone())
            .map_err(|error| {
                dynamic_error(
                    ErrorKind::ManifestInvalid,
                    "load_plugin_package",
                    error.to_string(),
                )
            })?;
        if embedded_manifest.version != package_version {
            return Err(dynamic_error(
                ErrorKind::ManifestInvalid,
                "load_plugin_package",
                "package version and embedded plugin manifest version differ",
            ));
        }
        pin_process_library(&code.0);
        Ok(LoadedDynamicPackage {
            manifest_path,
            package_version,
            code,
        })
    }
}

fn pin_process_library(library: &Arc<DynamicLibraryInner>) {
    PROCESS_LIBRARY_LEASES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .push(library.clone());
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DynamicPackageManifest {
    pub schema_major: u16,
    #[serde(default)]
    pub schema_minor: u16,
    pub package_version: String,
    pub library: PathBuf,
    #[serde(default)]
    pub sha256: Option<String>,
}

pub struct LoadedDynamicPackage {
    pub manifest_path: PathBuf,
    pub package_version: Version,
    pub code: DynamicLibraryLease,
}

fn validate_api_header(
    header: &PluginApiHeaderV1,
    host_abi: ApiVersion,
    supported_features: u64,
    manifest_schema_major: u16,
    manifest_schema_minor: u16,
) -> Result<(), PluginError> {
    if header.magic != TITAN_PLUGIN_MAGIC {
        return Err(dynamic_error(
            ErrorKind::AbiVersionMismatch,
            "validate_dynamic_abi",
            "invalid plugin magic",
        ));
    }
    if header.struct_size < std::mem::size_of::<PluginApiHeaderV1>() as u32 {
        return Err(dynamic_error(
            ErrorKind::AbiVersionMismatch,
            "validate_dynamic_abi",
            "plugin descriptor header is truncated",
        ));
    }
    if header.abi_major != host_abi.major || header.abi_minor > host_abi.minor {
        return Err(dynamic_error(
            ErrorKind::AbiVersionMismatch,
            "validate_dynamic_abi",
            "dynamic plugin ABI is incompatible",
        ));
    }
    if header.manifest_schema_major != manifest_schema_major {
        return Err(dynamic_error(
            ErrorKind::ManifestSchemaMismatch,
            "validate_dynamic_abi",
            "manifest schema major is incompatible",
        ));
    }
    if header.manifest_schema_minor > manifest_schema_minor {
        return Err(dynamic_error(
            ErrorKind::ManifestSchemaMismatch,
            "validate_dynamic_abi",
            "plugin manifest schema minor requires a ConfigurationAdapter migration",
        ));
    }
    if header.required_feature_bits & !supported_features != 0 {
        return Err(dynamic_error(
            ErrorKind::UnsupportedAbiFeature,
            "validate_dynamic_abi",
            "plugin requires unsupported ABI features",
        ));
    }
    Ok(())
}

/// `PluginFactory` adapter for dynamic plugins that only need lifecycle callbacks and event
/// publication. Dynamic service and connector interfaces require a domain-specific adapter built
/// on [`DynamicPluginInstance::query_interface`]; Rust trait objects never cross this ABI.
pub struct DynamicLifecyclePluginFactory {
    manifest: &'static PluginManifest,
    package_version: Version,
    source: Arc<str>,
    code: DynamicLibraryLease,
}

impl DynamicLifecyclePluginFactory {
    pub fn from_package(package: LoadedDynamicPackage) -> Result<Self, PluginError> {
        let manifest: PluginManifest = serde_json::from_value(package.code.manifest().clone())
            .map_err(|error| {
                dynamic_error(
                    ErrorKind::ManifestInvalid,
                    "parse_dynamic_plugin_manifest",
                    error.to_string(),
                )
            })?;
        if manifest.version != package.package_version {
            return Err(dynamic_error(
                ErrorKind::ManifestInvalid,
                "parse_dynamic_plugin_manifest",
                "package version and plugin manifest version differ",
            ));
        }
        if !manifest.provides.is_empty()
            || !manifest.requires.is_empty()
            || !manifest.subscribes.is_empty()
        {
            return Err(dynamic_error(
                ErrorKind::UnsupportedAbiFeature,
                "parse_dynamic_plugin_manifest",
                "generic dynamic lifecycle adapter cannot export services, consume services, or subscribe to events; use a domain-specific query_interface adapter",
            ));
        }
        let source: Arc<str> = Arc::from(package.manifest_path.to_string_lossy().as_ref());
        // PluginFactory's registry contract requires a process-lifetime manifest. Registered
        // factories are process-lifetime objects, so retaining this small immutable value is
        // intentional and avoids exposing foreign manifest storage.
        let manifest = Box::leak(Box::new(manifest));
        Ok(Self {
            manifest,
            package_version: package.package_version,
            source,
            code: package.code,
        })
    }

    pub fn package_version(&self) -> &Version {
        &self.package_version
    }

    pub fn source(&self) -> &Arc<str> {
        &self.source
    }
}

impl PluginFactory for DynamicLifecyclePluginFactory {
    fn manifest(&self) -> &'static PluginManifest {
        self.manifest
    }

    fn create(&self, init: PluginInit) -> Result<PluginBundle, PluginError> {
        let config = serde_json::to_vec(init.config.value.as_ref()).map_err(|error| {
            dynamic_error(
                ErrorKind::ConfigInvalid,
                "serialize_dynamic_plugin_config",
                error.to_string(),
            )
        })?;
        let instance = self.code.create(&config)?;
        Ok(PluginBundle {
            lifecycle: Box::new(DynamicPluginLifecycle::new(instance)),
            service_exports: Vec::new(),
            subscription_bindings: Vec::new(),
        })
    }
}

struct DynamicHostContext {
    events: Mutex<Option<EventPublisher>>,
}

struct DynamicPluginLifecycle {
    instance: DynamicPluginInstance,
    host_context: Box<DynamicHostContext>,
    host_api: Box<TitanHostApiV1>,
}

impl DynamicPluginLifecycle {
    fn new(instance: DynamicPluginInstance) -> Self {
        let mut host_context = Box::new(DynamicHostContext {
            events: Mutex::new(None),
        });
        let context = (&mut *host_context as *mut DynamicHostContext).cast::<c_void>();
        let host_api = Box::new(TitanHostApiV1 {
            magic: TITAN_HOST_MAGIC,
            struct_size: std::mem::size_of::<TitanHostApiV1>() as u32,
            abi_major: TITAN_DYNAMIC_ABI_VERSION.major,
            abi_minor: TITAN_DYNAMIC_ABI_VERSION.minor,
            context,
            publish_event: Some(host_publish_event),
            log: Some(host_log),
            now_ns: Some(host_now_ns),
            // Secret resolution belongs to the AccountPlugin adapter, where SecretRef policy and
            // zeroization can be enforced. The generic host deliberately exposes no secret bytes.
            resolve_secret: None,
        });
        Self {
            instance,
            host_context,
            host_api,
        }
    }

    fn clear_runtime_context(&self) {
        *self
            .host_context
            .events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    }
}

impl Plugin for DynamicPluginLifecycle {
    fn validate(&self, _context: &ValidationContext) -> Result<(), PluginError> {
        self.instance.validate()
    }

    fn start(&mut self, context: &mut PluginContext) -> Result<(), PluginError> {
        *self
            .host_context
            .events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(context.events.clone());
        // SAFETY: `host_api` and its boxed context are fields of this lifecycle object. Field/drop
        // ordering plus the explicit Plugin::stop call keep both alive until foreign stop returns.
        if let Err(error) = unsafe { self.instance.start(&self.host_api) } {
            self.clear_runtime_context();
            return Err(error);
        }
        Ok(())
    }

    fn quiesce(&mut self, reason: StopReason) -> Result<(), PluginError> {
        let reason = match reason {
            StopReason::Shutdown => TITAN_STOP_SHUTDOWN,
            StopReason::Restart => TITAN_STOP_RESTART,
            StopReason::Failure => TITAN_STOP_FAILURE,
        };
        self.instance.quiesce(reason)
    }

    fn stop(&mut self) -> Result<(), PluginError> {
        let result = self.instance.stop();
        self.clear_runtime_context();
        result
    }
}

unsafe extern "C" fn host_publish_event(
    context: *mut c_void,
    event_type: *const u8,
    event_type_len: usize,
    schema_version: u32,
    payload: *const u8,
    payload_len: usize,
    metadata: TitanEventMetadataV1,
) -> TitanStatus {
    match catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: `TitanHostApiV1::context` is initialized from the boxed context and remains
        // stable until after the dynamic instance is stopped and destroyed.
        let Some(context) = (unsafe { (context as *mut DynamicHostContext).as_ref() }) else {
            return TITAN_STATUS_INVALID_ARGUMENT;
        };
        // SAFETY: ABI contract keeps both input ranges readable for this callback invocation.
        let Ok(event_type) = (unsafe { foreign_str(event_type, event_type_len) }) else {
            return TITAN_STATUS_INVALID_ARGUMENT;
        };
        // SAFETY: same callback-duration input contract as the event type.
        let Ok(payload) = (unsafe { foreign_bytes(payload, payload_len) }) else {
            return TITAN_STATUS_INVALID_ARGUMENT;
        };
        let events = context
            .events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let Some(events) = events else {
            return TITAN_STATUS_HOST_ERROR;
        };
        let publish_metadata = EventPublishMetadata {
            source_id: metadata.source_id,
            source_sequence: metadata.source_sequence,
            exchange_ts: metadata.exchange_ts,
            receive_ts: metadata.receive_ts,
            publish_ts: metadata.publish_ts,
            routing_key: metadata.routing_key,
            flags: metadata.flags,
        };
        match events.publish_with_metadata(
            event_type,
            schema_version,
            payload,
            publish_metadata,
            metadata.trace(),
        ) {
            Ok(()) => TITAN_STATUS_OK,
            Err(_) => TITAN_STATUS_HOST_ERROR,
        }
    })) {
        Ok(status) => status,
        Err(_) => TITAN_STATUS_PANIC,
    }
}

unsafe extern "C" fn host_log(
    _context: *mut c_void,
    _level: u32,
    message: *const u8,
    message_len: usize,
) -> TitanStatus {
    match catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: ABI contract keeps the log message readable for the callback invocation.
        unsafe { foreign_str(message, message_len) }
    })) {
        Ok(Ok(_)) => TITAN_STATUS_OK,
        Ok(Err(())) => TITAN_STATUS_INVALID_ARGUMENT,
        Err(_) => TITAN_STATUS_PANIC,
    }
}

unsafe extern "C" fn host_now_ns(_context: *mut c_void) -> i64 {
    catch_unwind(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos().min(i64::MAX as u128) as i64)
            .unwrap_or(0)
    })
    .unwrap_or(0)
}

unsafe fn foreign_bytes<'a>(pointer: *const u8, length: usize) -> Result<&'a [u8], ()> {
    if length == 0 {
        return Ok(&[]);
    }
    if pointer.is_null() || length > isize::MAX as usize {
        return Err(());
    }
    // SAFETY: ABI callers promise the input range remains readable for the callback duration.
    Ok(unsafe { std::slice::from_raw_parts(pointer, length) })
}

unsafe fn foreign_str<'a>(pointer: *const u8, length: usize) -> Result<&'a str, ()> {
    // SAFETY: caller provides the same readable range required by `foreign_bytes`.
    std::str::from_utf8(unsafe { foreign_bytes(pointer, length) }?).map_err(|_| ())
}

fn read_manifest(api: PluginApiV1, max_bytes: usize) -> Result<serde_json::Value, PluginError> {
    if max_bytes == 0 || max_bytes > MAX_DYNAMIC_MANIFEST_BYTES {
        return Err(dynamic_error(
            ErrorKind::ManifestInvalid,
            "read_dynamic_manifest",
            "invalid host manifest size limit",
        ));
    }
    let mut length = 0;
    let manifest = api.manifest_json.expect("descriptor was validated");
    // SAFETY: the function is part of the validated descriptor and writes one usize.
    let pointer = unsafe { manifest(&mut length) };
    if length == 0 || length > max_bytes || pointer.is_null() {
        return Err(dynamic_error(
            ErrorKind::ManifestInvalid,
            "read_dynamic_manifest",
            "plugin returned an invalid manifest buffer",
        ));
    }
    // SAFETY: the plugin guarantees the manifest buffer remains valid while its library is loaded.
    let bytes = unsafe { std::slice::from_raw_parts(pointer, length) };
    serde_json::from_slice(bytes).map_err(|error| {
        dynamic_error(
            ErrorKind::ManifestInvalid,
            "read_dynamic_manifest",
            error.to_string(),
        )
    })
}

fn validate_dynamic_manifest(
    value: &serde_json::Value,
    host_abi: ApiVersion,
) -> Result<(), PluginError> {
    let manifest: PluginManifest = serde_json::from_value(value.clone()).map_err(|error| {
        dynamic_error(
            ErrorKind::ManifestInvalid,
            "validate_dynamic_manifest",
            error.to_string(),
        )
    })?;
    crate::validate_manifest(&manifest, crate::CORE_RUNTIME_API_VERSION, host_abi)
}

fn check_status(
    api: &PluginApiV1,
    status: TitanStatus,
    operation: &'static str,
) -> Result<(), PluginError> {
    if status == TITAN_STATUS_OK {
        Ok(())
    } else {
        Err(dynamic_error(
            ErrorKind::PluginFailed,
            operation,
            api.error_message("dynamic plugin call failed"),
        ))
    }
}

fn canonical_file(path: &Path, operation: &'static str) -> Result<PathBuf, PluginError> {
    let value = fs::canonicalize(path)
        .map_err(|error| dynamic_error(ErrorKind::ManifestInvalid, operation, error.to_string()))?;
    if !value.is_file() || value.file_name() == Some(OsStr::new("")) {
        return Err(dynamic_error(
            ErrorKind::ManifestInvalid,
            operation,
            "path is not a regular file",
        ));
    }
    Ok(value)
}

fn sha256_file(path: &Path) -> Result<String, PluginError> {
    let mut file = File::open(path).map_err(|error| {
        dynamic_error(
            ErrorKind::ManifestInvalid,
            "verify_plugin_digest",
            error.to_string(),
        )
    })?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(|error| {
            dynamic_error(
                ErrorKind::ManifestInvalid,
                "verify_plugin_digest",
                error.to_string(),
            )
        })?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn constant_time_hex_eq(expected: &str, actual: &str) -> bool {
    if expected.len() != actual.len() || expected.len() != 64 {
        return false;
    }
    expected
        .bytes()
        .zip(actual.bytes())
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

fn dynamic_error(
    kind: ErrorKind,
    operation: &'static str,
    message: impl Into<Arc<str>>,
) -> PluginError {
    let mut error = engine_error(kind, operation, message);
    error.identity = crate::PluginIdentity::new("titan.dynamic", "loader");
    error
}

fn dynamic_state_error(operation: &'static str, message: &'static str) -> PluginError {
    dynamic_error(ErrorKind::RuntimeNotActive, operation, message)
}
