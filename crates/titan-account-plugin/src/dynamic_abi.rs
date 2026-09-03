//! Local C ABI adapter for dynamically loaded AccountConnectorFactory implementations.

use std::{
    ffi::c_void,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant, SystemTime},
};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use titan_plugin_engine::{
    ClosureResource, DynamicPluginSession, TITAN_STATUS_HOST_ERROR, TITAN_STATUS_INVALID_ARGUMENT,
    TITAN_STATUS_OK, TITAN_STATUS_PANIC, TitanBuffer, TitanPluginHandle, TitanStatus, TraceContext,
};
use zeroize::Zeroize;

use crate::{
    AccountCommandReceipt, AccountConnector, AccountConnectorContext,
    AccountConnectorDiagnosticSnapshot, AccountConnectorError, AccountConnectorFactory,
    AccountConnectorHealthSnapshot, AccountConnectorOperationSnapshot, AccountDefinition,
    AccountEventPublisher, AccountLifecycle, AccountStateSnapshot, AmendOrderCommand,
    BalanceSnapshot, CancelAllAfterCommand, CancelAllCommand, CancelOrderCommand, OperationId,
    OperationState, OrderFilter, OrderSnapshot, PositionFilter, PositionSnapshot, ReconcileScope,
    SecretRef, SourceStreamId, SubmitOrderCommand,
};

pub const TITAN_ACCOUNT_FACTORY_INTERFACE: &str = "titan.account.connector-factory";
pub const TITAN_ACCOUNT_FACTORY_MAGIC: u64 = 0x5449_5441_4e41_4343;
pub const TITAN_ACCOUNT_FACTORY_ABI_MAJOR: u16 = 1;
pub const TITAN_ACCOUNT_FACTORY_ABI_MINOR: u16 = 0;

pub type TitanAccountConnectorHandle = u64;

#[repr(C)]
struct AccountFactoryHeaderV1 {
    magic: u64,
    struct_size: u32,
    abi_major: u16,
    abi_minor: u16,
}

#[derive(Serialize, Deserialize)]
pub struct DynamicAccountCreateRequest {
    pub definition: AccountDefinition,
    pub account: crate::AccountHandle,
    pub account_stream: SourceStreamId,
    pub control_stream: SourceStreamId,
    pub command_queue_capacity: usize,
}
pub type TitanAccountJsonCall = unsafe extern "C" fn(
    TitanAccountConnectorHandle,
    *const u8,
    usize,
    *mut TitanBuffer,
) -> TitanStatus;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct TitanAccountHostApiV1 {
    pub struct_size: u32,
    pub context: *mut c_void,
    pub publish_account: Option<
        unsafe extern "C" fn(
            *mut c_void,
            *const u8,
            usize,
            *const u8,
            usize,
            u64,
            u64,
        ) -> TitanStatus,
    >,
    pub resolve_secret: Option<
        unsafe extern "C" fn(*mut c_void, *const u8, usize, *mut TitanBuffer) -> TitanStatus,
    >,
}

// SAFETY: context points to a stable boxed value containing thread-safe capabilities.
unsafe impl Send for TitanAccountHostApiV1 {}
// SAFETY: callback-visible state is immutable or internally synchronized.
unsafe impl Sync for TitanAccountHostApiV1 {}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct TitanAccountConnectorFactoryApiV1 {
    pub magic: u64,
    pub struct_size: u32,
    pub abi_major: u16,
    pub abi_minor: u16,
    pub connector_type: Option<unsafe extern "C" fn(TitanPluginHandle, *mut usize) -> *const u8>,
    pub create: Option<
        unsafe extern "C" fn(
            TitanPluginHandle,
            *const u8,
            usize,
            *const TitanAccountHostApiV1,
            *mut TitanAccountConnectorHandle,
        ) -> TitanStatus,
    >,
    pub destroy: Option<unsafe extern "C" fn(TitanAccountConnectorHandle) -> TitanStatus>,
    pub start: Option<unsafe extern "C" fn(TitanAccountConnectorHandle) -> TitanStatus>,
    pub stop: Option<unsafe extern "C" fn(TitanAccountConnectorHandle, u64) -> TitanStatus>,
    pub submit: Option<TitanAccountJsonCall>,
    pub amend: Option<TitanAccountJsonCall>,
    pub cancel: Option<TitanAccountJsonCall>,
    pub cancel_all: Option<TitanAccountJsonCall>,
    pub cancel_all_after: Option<TitanAccountJsonCall>,
    pub reconcile: Option<TitanAccountJsonCall>,
    pub orders: Option<TitanAccountJsonCall>,
    pub positions: Option<TitanAccountJsonCall>,
    pub balances: Option<TitanAccountJsonCall>,
    pub health: Option<TitanAccountJsonCall>,
    pub diagnostics: Option<TitanAccountJsonCall>,
    pub operation: Option<TitanAccountJsonCall>,
    pub last_error: Option<unsafe extern "C" fn(*mut u8, usize) -> usize>,
}

pub struct DynamicAccountConnectorFactory {
    connector_type: Arc<str>,
    api: TitanAccountConnectorFactoryApiV1,
    session: DynamicPluginSession,
}

impl DynamicAccountConnectorFactory {
    pub fn from_session(session: DynamicPluginSession) -> Result<Self, AccountConnectorError> {
        let pointer = session
            .query_interface(
                TITAN_ACCOUNT_FACTORY_INTERFACE,
                TITAN_ACCOUNT_FACTORY_ABI_MAJOR,
            )
            .map_err(dynamic_error)?;
        if (pointer as usize) % std::mem::align_of::<AccountFactoryHeaderV1>() != 0 {
            return Err(AccountConnectorError::rejected(
                "dynamic account factory descriptor is misaligned",
            ));
        }
        // SAFETY: every version-one account descriptor begins with this fixed header.
        let header = unsafe { &*pointer.cast::<AccountFactoryHeaderV1>() };
        if header.magic != TITAN_ACCOUNT_FACTORY_MAGIC
            || header.struct_size < std::mem::size_of::<TitanAccountConnectorFactoryApiV1>() as u32
            || header.abi_major != TITAN_ACCOUNT_FACTORY_ABI_MAJOR
            || header.abi_minor > TITAN_ACCOUNT_FACTORY_ABI_MINOR
        {
            return Err(AccountConnectorError::rejected(
                "incompatible dynamic account factory ABI",
            ));
        }
        // SAFETY: the versioned interface is retained by the dynamic session's code lease.
        let api = unsafe { *pointer.cast::<TitanAccountConnectorFactoryApiV1>() };
        validate_api(&api)?;
        let mut length = 0;
        let plugin_handle = session.handle();
        // SAFETY: descriptor validation checked the function pointer and output location.
        let pointer = unsafe { api.connector_type.unwrap()(plugin_handle, &mut length) };
        // SAFETY: connector_type returns immutable library storage retained by the session.
        let connector_type = unsafe { foreign_str(pointer, length) }
            .map_err(|_| AccountConnectorError::rejected("dynamic connector type is invalid"))?;
        if connector_type.is_empty() {
            return Err(AccountConnectorError::rejected(
                "dynamic connector type is empty",
            ));
        }
        Ok(Self {
            connector_type: Arc::from(connector_type),
            api,
            session,
        })
    }
}

impl AccountConnectorFactory for DynamicAccountConnectorFactory {
    fn connector_type(&self) -> &str {
        &self.connector_type
    }

    fn create(
        &self,
        definition: &AccountDefinition,
        context: AccountConnectorContext,
    ) -> Result<Arc<dyn AccountConnector>, AccountConnectorError> {
        let resources = context.resources.clone();
        let input = serde_json::to_vec(&DynamicAccountCreateRequest {
            definition: definition.clone(),
            account: context.account,
            account_stream: context.account_stream,
            control_stream: context.control_stream,
            command_queue_capacity: context.command_queue_capacity,
        })
        .map_err(dynamic_error)?;
        let mut host_context = Box::new(AccountHostContext {
            publisher: context.event_publisher,
            secrets: context.secrets,
        });
        let host = Box::new(TitanAccountHostApiV1 {
            struct_size: std::mem::size_of::<TitanAccountHostApiV1>() as u32,
            context: (&mut *host_context as *mut AccountHostContext).cast(),
            publish_account: Some(host_publish_account),
            resolve_secret: Some(host_resolve_secret),
        });
        let plugin_handle = self.session.handle();
        let mut handle = 0;
        // SAFETY: input and host descriptor remain valid; returned connector retains their owner.
        status(
            &self.api,
            unsafe {
                self.api.create.unwrap()(
                    plugin_handle,
                    input.as_ptr(),
                    input.len(),
                    &*host,
                    &mut handle,
                )
            },
            "create",
        )?;
        if handle == 0 {
            return Err(AccountConnectorError::rejected(
                "dynamic account factory returned a zero connector handle",
            ));
        }
        let connector = Arc::new(DynamicAccountConnector {
            api: self.api,
            handle,
            host_context,
            host,
            session: self.session.clone(),
            destroyed: Mutex::new(false),
        });
        let weak: Weak<DynamicAccountConnector> = Arc::downgrade(&connector);
        resources
            .register(
                "dynamic-account-connector",
                ClosureResource(Some(move || {
                    if let Some(connector) = weak.upgrade() {
                        connector
                            .stop(Instant::now() + Duration::from_secs(5))
                            .map_err(|error| {
                                titan_plugin_engine::PluginError::new(
                                    titan_plugin_engine::ErrorKind::ResourceReleaseFailed,
                                    titan_plugin_engine::PluginIdentity::new(
                                        "titan.account",
                                        "dynamic-connector",
                                    ),
                                    titan_plugin_engine::LifecycleState::Stopping,
                                    "stop_dynamic_account_connector",
                                    error.to_string(),
                                )
                            })?;
                    }
                    Ok(())
                })),
            )
            .map_err(dynamic_error)?;
        Ok(connector)
    }
}

struct AccountHostContext {
    publisher: AccountEventPublisher,
    secrets: crate::ScopedSecretResolver,
}

struct DynamicAccountConnector {
    api: TitanAccountConnectorFactoryApiV1,
    handle: TitanAccountConnectorHandle,
    host_context: Box<AccountHostContext>,
    host: Box<TitanAccountHostApiV1>,
    session: DynamicPluginSession,
    destroyed: Mutex<bool>,
}

// SAFETY: the dynamic AccountConnector ABI requires concurrent calls to be supported, and host
// state is immutable or internally synchronized.
unsafe impl Send for DynamicAccountConnector {}
// SAFETY: same contract as Send; the code lease remains live through `session`.
unsafe impl Sync for DynamicAccountConnector {}

impl DynamicAccountConnector {
    fn call<I: Serialize, O: DeserializeOwned>(
        &self,
        operation: &'static str,
        function: TitanAccountJsonCall,
        input: &I,
    ) -> Result<O, AccountConnectorError> {
        let input = serde_json::to_vec(input).map_err(dynamic_error)?;
        let mut output = TitanBuffer::default();
        // SAFETY: input and output live for the call and the connector handle belongs to this API.
        status(
            &self.api,
            unsafe { function(self.handle, input.as_ptr(), input.len(), &mut output) },
            operation,
        )?;
        // SAFETY: plugin returned a buffer under the TitanBuffer ownership contract.
        let bytes = unsafe { output.copy_and_free() }.map_err(dynamic_error)?;
        serde_json::from_slice(&bytes).map_err(dynamic_error)
    }
}

impl AccountConnector for DynamicAccountConnector {
    fn start(&self) -> Result<(), AccountConnectorError> {
        status(
            &self.api,
            unsafe { self.api.start.unwrap()(self.handle) },
            "start",
        )
    }

    fn stop(&self, deadline: Instant) -> Result<(), AccountConnectorError> {
        let timeout_ns = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or(Duration::ZERO)
            .as_nanos()
            .min(u64::MAX as u128) as u64;
        status(
            &self.api,
            unsafe { self.api.stop.unwrap()(self.handle, timeout_ns) },
            "stop",
        )
    }

    fn submit(
        &self,
        command: SubmitOrderCommand,
    ) -> Result<AccountCommandReceipt, AccountConnectorError> {
        self.call("submit", self.api.submit.unwrap(), &command)
    }

    fn amend(
        &self,
        command: AmendOrderCommand,
    ) -> Result<AccountCommandReceipt, AccountConnectorError> {
        self.call("amend", self.api.amend.unwrap(), &command)
    }

    fn cancel(
        &self,
        command: CancelOrderCommand,
    ) -> Result<AccountCommandReceipt, AccountConnectorError> {
        self.call("cancel", self.api.cancel.unwrap(), &command)
    }

    fn cancel_all(
        &self,
        command: CancelAllCommand,
    ) -> Result<AccountCommandReceipt, AccountConnectorError> {
        self.call("cancel_all", self.api.cancel_all.unwrap(), &command)
    }

    fn cancel_all_after(
        &self,
        command: CancelAllAfterCommand,
    ) -> Result<AccountCommandReceipt, AccountConnectorError> {
        self.call(
            "cancel_all_after",
            self.api.cancel_all_after.unwrap(),
            &command,
        )
    }

    fn reconcile(&self, scope: ReconcileScope) -> Result<OperationId, AccountConnectorError> {
        self.call("reconcile", self.api.reconcile.unwrap(), &scope)
    }

    fn orders(
        &self,
        filter: OrderFilter,
    ) -> Result<AccountStateSnapshot<OrderSnapshot>, AccountConnectorError> {
        self.call("orders", self.api.orders.unwrap(), &filter)
    }

    fn positions(
        &self,
        filter: PositionFilter,
    ) -> Result<AccountStateSnapshot<PositionSnapshot>, AccountConnectorError> {
        self.call("positions", self.api.positions.unwrap(), &filter)
    }

    fn balances(&self) -> Result<AccountStateSnapshot<BalanceSnapshot>, AccountConnectorError> {
        self.call("balances", self.api.balances.unwrap(), &())
    }

    fn health(&self) -> AccountConnectorHealthSnapshot {
        self.call("health", self.api.health.unwrap(), &())
            .unwrap_or_else(|error| AccountConnectorHealthSnapshot {
                state: AccountLifecycle::Failed,
                message: Arc::from(error.to_string()),
                observed_at: SystemTime::now(),
            })
    }

    fn diagnostics(&self) -> AccountConnectorDiagnosticSnapshot {
        self.call("diagnostics", self.api.diagnostics.unwrap(), &())
            .unwrap_or_else(|error| AccountConnectorDiagnosticSnapshot {
                summary: Arc::from(error.to_string()),
                external_order_count: 0,
                command_queue_depth: 0,
                account_epoch: 0,
                account_version: 0,
            })
    }

    fn operation(&self, id: OperationId) -> AccountConnectorOperationSnapshot {
        self.call("operation", self.api.operation.unwrap(), &id)
            .unwrap_or_else(|error| AccountConnectorOperationSnapshot {
                id,
                state: OperationState::Failed,
                detail: Arc::from(error.to_string()),
            })
    }
}

impl Drop for DynamicAccountConnector {
    fn drop(&mut self) {
        let destroyed = self.destroyed.get_mut().unwrap_or_else(|p| p.into_inner());
        if !*destroyed {
            let _ = unsafe { self.api.destroy.unwrap()(self.handle) };
            *destroyed = true;
        }
        let _ = (&self.host_context, &self.host, &self.session);
    }
}

unsafe extern "C" fn host_publish_account(
    context: *mut c_void,
    event_type: *const u8,
    event_type_len: usize,
    payload: *const u8,
    payload_len: usize,
    trace_id: u64,
    causation_id: u64,
) -> TitanStatus {
    callback_status(|| {
        // SAFETY: the proxy retains this boxed context until foreign connector destruction.
        let context = unsafe { (context as *const AccountHostContext).as_ref() }.ok_or(())?;
        // SAFETY: plugin owns readable callback inputs for the duration of this invocation.
        let event_type = unsafe { foreign_str(event_type, event_type_len) }?;
        let payload = unsafe { foreign_bytes(payload, payload_len) }?;
        context
            .publisher
            .publish(
                event_type,
                payload,
                TraceContext {
                    trace_id,
                    causation_id,
                },
            )
            .map_err(|_| ())
    })
}

unsafe extern "C" fn host_resolve_secret(
    context: *mut c_void,
    reference: *const u8,
    reference_len: usize,
    output: *mut TitanBuffer,
) -> TitanStatus {
    if output.is_null() {
        return TITAN_STATUS_INVALID_ARGUMENT;
    }
    match catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: context and input follow the callback lifetime contract.
        let context = unsafe { (context as *const AccountHostContext).as_ref() }.ok_or(())?;
        let reference = unsafe { foreign_str(reference, reference_len) }?;
        let secret = context
            .secrets
            .resolve(&SecretRef::new(reference))
            .map_err(|_| ())?;
        let mut bytes = secret.expose().to_vec();
        let buffer = TitanBuffer {
            data: bytes.as_mut_ptr(),
            len: bytes.len(),
            capacity: bytes.capacity(),
            free: Some(free_secret_buffer),
        };
        std::mem::forget(bytes);
        // SAFETY: output was checked non-null and points to caller-owned writable storage.
        unsafe { output.write(buffer) };
        Ok(())
    })) {
        Ok(Ok(())) => TITAN_STATUS_OK,
        Ok(Err(())) => TITAN_STATUS_HOST_ERROR,
        Err(_) => TITAN_STATUS_PANIC,
    }
}

unsafe extern "C" fn free_secret_buffer(data: *mut u8, len: usize, capacity: usize) {
    if data.is_null()
        || len > capacity
        || len > isize::MAX as usize
        || capacity > isize::MAX as usize
    {
        return;
    }
    // SAFETY: this exact allocation tuple was produced by host_resolve_secret and is freed once.
    let mut bytes = unsafe { Vec::from_raw_parts(data, len, capacity) };
    bytes.zeroize();
}

fn callback_status(call: impl FnOnce() -> Result<(), ()>) -> TitanStatus {
    match catch_unwind(AssertUnwindSafe(call)) {
        Ok(Ok(())) => TITAN_STATUS_OK,
        Ok(Err(())) => TITAN_STATUS_HOST_ERROR,
        Err(_) => TITAN_STATUS_PANIC,
    }
}

fn validate_api(api: &TitanAccountConnectorFactoryApiV1) -> Result<(), AccountConnectorError> {
    if api.magic != TITAN_ACCOUNT_FACTORY_MAGIC
        || api.struct_size < std::mem::size_of::<TitanAccountConnectorFactoryApiV1>() as u32
        || api.abi_major != TITAN_ACCOUNT_FACTORY_ABI_MAJOR
        || api.abi_minor > TITAN_ACCOUNT_FACTORY_ABI_MINOR
    {
        return Err(AccountConnectorError::rejected(
            "incompatible dynamic account factory ABI",
        ));
    }
    if api.connector_type.is_none()
        || api.create.is_none()
        || api.destroy.is_none()
        || api.start.is_none()
        || api.stop.is_none()
        || api.submit.is_none()
        || api.amend.is_none()
        || api.cancel.is_none()
        || api.cancel_all.is_none()
        || api.cancel_all_after.is_none()
        || api.reconcile.is_none()
        || api.orders.is_none()
        || api.positions.is_none()
        || api.balances.is_none()
        || api.health.is_none()
        || api.diagnostics.is_none()
        || api.operation.is_none()
        || api.last_error.is_none()
    {
        return Err(AccountConnectorError::rejected(
            "dynamic account factory ABI is truncated",
        ));
    }
    Ok(())
}

fn status(
    api: &TitanAccountConnectorFactoryApiV1,
    value: TitanStatus,
    operation: &str,
) -> Result<(), AccountConnectorError> {
    if value == TITAN_STATUS_OK {
        return Ok(());
    }
    let mut bytes = vec![0_u8; 16 * 1024];
    // SAFETY: descriptor validation checked the function and buffer is writable.
    let length =
        unsafe { api.last_error.unwrap()(bytes.as_mut_ptr(), bytes.len()) }.min(bytes.len());
    bytes.truncate(length);
    let detail = std::str::from_utf8(&bytes).unwrap_or("dynamic connector call failed");
    Err(AccountConnectorError::rejected(format!(
        "{operation}: {detail}"
    )))
}

unsafe fn foreign_bytes<'a>(pointer: *const u8, length: usize) -> Result<&'a [u8], ()> {
    if length == 0 {
        return Ok(&[]);
    }
    if pointer.is_null() || length > isize::MAX as usize {
        return Err(());
    }
    // SAFETY: caller guarantees this readable range for the returned borrow's use.
    Ok(unsafe { std::slice::from_raw_parts(pointer, length) })
}

unsafe fn foreign_str<'a>(pointer: *const u8, length: usize) -> Result<&'a str, ()> {
    // SAFETY: forwards the caller's readable-range guarantee.
    std::str::from_utf8(unsafe { foreign_bytes(pointer, length) }?).map_err(|_| ())
}

fn dynamic_error(error: impl std::fmt::Display) -> AccountConnectorError {
    AccountConnectorError::rejected(error.to_string())
}
