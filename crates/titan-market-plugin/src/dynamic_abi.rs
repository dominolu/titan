//! Local C ABI adapter for dynamically loaded MarketConnectorFactory implementations.

use std::{
    ffi::c_void,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant, SystemTime},
};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use titan_plugin_engine::{
    ClosureResource, DynamicPluginSession, TITAN_STATUS_HOST_ERROR, TITAN_STATUS_OK,
    TITAN_STATUS_PANIC, TitanBuffer, TitanPluginHandle, TitanStatus, TraceContext,
};

use crate::{
    AssetId, ConnectorDiagnosticSnapshot, ConnectorError, ConnectorHealth, ConnectorHealthSnapshot,
    ConnectorOperationSnapshot, InstrumentSnapshot, MarketConnector, MarketConnectorContext,
    MarketConnectorFactory, MarketEventPublisher, MarketSourceDefinition, MarketSourceHandle,
    MarketSubscribeRequest, MarketSubscription, OperationId, OperationState, SourceStreamId,
};

pub const TITAN_MARKET_FACTORY_INTERFACE: &str = "titan.market.connector-factory";
pub const TITAN_MARKET_FACTORY_MAGIC: u64 = 0x5449_5441_4e4d_4b54;
pub const TITAN_MARKET_FACTORY_ABI_MAJOR: u16 = 1;
pub const TITAN_MARKET_FACTORY_ABI_MINOR: u16 = 0;

pub type TitanMarketConnectorHandle = u64;

#[repr(C)]
struct MarketFactoryHeaderV1 {
    magic: u64,
    struct_size: u32,
    abi_major: u16,
    abi_minor: u16,
}

#[derive(Serialize, Deserialize)]
pub struct DynamicMarketCreateRequest {
    pub definition: MarketSourceDefinition,
    pub source: MarketSourceHandle,
    pub market_source_stream: SourceStreamId,
    pub control_source_stream: SourceStreamId,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct TitanMarketHostApiV1 {
    pub struct_size: u32,
    pub context: *mut c_void,
    pub publish_market: Option<
        unsafe extern "C" fn(
            *mut c_void,
            *const u8,
            usize,
            *const u8,
            usize,
            u32,
            i64,
            i64,
            u64,
            u64,
        ) -> TitanStatus,
    >,
    pub publish_control: Option<
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
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct TitanMarketConnectorFactoryApiV1 {
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
            *const TitanMarketHostApiV1,
            *mut TitanMarketConnectorHandle,
        ) -> TitanStatus,
    >,
    pub destroy: Option<unsafe extern "C" fn(TitanMarketConnectorHandle) -> TitanStatus>,
    pub start: Option<unsafe extern "C" fn(TitanMarketConnectorHandle) -> TitanStatus>,
    pub stop: Option<unsafe extern "C" fn(TitanMarketConnectorHandle, u64) -> TitanStatus>,
    pub subscribe: Option<
        unsafe extern "C" fn(
            TitanMarketConnectorHandle,
            *const u8,
            usize,
            *mut TitanBuffer,
        ) -> TitanStatus,
    >,
    pub unsubscribe: Option<
        unsafe extern "C" fn(TitanMarketConnectorHandle, u64, *mut TitanBuffer) -> TitanStatus,
    >,
    pub request_snapshot: Option<
        unsafe extern "C" fn(TitanMarketConnectorHandle, u32, *mut TitanBuffer) -> TitanStatus,
    >,
    pub instruments:
        Option<unsafe extern "C" fn(TitanMarketConnectorHandle, *mut TitanBuffer) -> TitanStatus>,
    pub health:
        Option<unsafe extern "C" fn(TitanMarketConnectorHandle, *mut TitanBuffer) -> TitanStatus>,
    pub diagnostics:
        Option<unsafe extern "C" fn(TitanMarketConnectorHandle, *mut TitanBuffer) -> TitanStatus>,
    pub operation: Option<
        unsafe extern "C" fn(TitanMarketConnectorHandle, u64, *mut TitanBuffer) -> TitanStatus,
    >,
    pub last_error: Option<unsafe extern "C" fn(*mut u8, usize) -> usize>,
}

// SAFETY: the context is a stable boxed value and the publisher it contains is thread-safe.
unsafe impl Send for TitanMarketHostApiV1 {}
// SAFETY: callbacks only invoke thread-safe publisher methods through the stable context.
unsafe impl Sync for TitanMarketHostApiV1 {}

pub struct DynamicMarketConnectorFactory {
    connector_type: Arc<str>,
    api: TitanMarketConnectorFactoryApiV1,
    session: DynamicPluginSession,
}

impl DynamicMarketConnectorFactory {
    pub fn from_session(session: DynamicPluginSession) -> Result<Self, ConnectorError> {
        let pointer = session
            .query_interface(
                TITAN_MARKET_FACTORY_INTERFACE,
                TITAN_MARKET_FACTORY_ABI_MAJOR,
            )
            .map_err(dynamic_error)?;
        if (pointer as usize) % std::mem::align_of::<MarketFactoryHeaderV1>() != 0 {
            return Err(ConnectorError::new(
                "dynamic market factory descriptor is misaligned",
            ));
        }
        // SAFETY: every version-one market descriptor begins with this fixed header.
        let header = unsafe { &*pointer.cast::<MarketFactoryHeaderV1>() };
        if header.magic != TITAN_MARKET_FACTORY_MAGIC
            || header.struct_size < std::mem::size_of::<TitanMarketConnectorFactoryApiV1>() as u32
            || header.abi_major != TITAN_MARKET_FACTORY_ABI_MAJOR
            || header.abi_minor > TITAN_MARKET_FACTORY_ABI_MINOR
        {
            return Err(ConnectorError::new(
                "incompatible dynamic market factory ABI",
            ));
        }
        // SAFETY: query_interface returned this versioned descriptor and the session retains code.
        let api = unsafe { *pointer.cast::<TitanMarketConnectorFactoryApiV1>() };
        validate_api(&api)?;
        let mut length = 0;
        let plugin_handle = session.handle();
        // SAFETY: descriptor validation checked this function pointer.
        let pointer = unsafe { api.connector_type.unwrap()(plugin_handle, &mut length) };
        let connector_type = unsafe { foreign_str(pointer, length) }
            .map_err(|_| ConnectorError::new("dynamic connector type is invalid UTF-8"))?;
        if connector_type.is_empty() {
            return Err(ConnectorError::new("dynamic connector type is empty"));
        }
        Ok(Self {
            connector_type: Arc::from(connector_type),
            api,
            session,
        })
    }
}

impl MarketConnectorFactory for DynamicMarketConnectorFactory {
    fn connector_type(&self) -> &str {
        &self.connector_type
    }

    fn create(
        &self,
        definition: &MarketSourceDefinition,
        context: MarketConnectorContext,
    ) -> Result<Arc<dyn MarketConnector>, ConnectorError> {
        let resources = context.resources.clone();
        let input = serde_json::to_vec(&DynamicMarketCreateRequest {
            definition: definition.clone(),
            source: context.source,
            market_source_stream: context.market_source_stream,
            control_source_stream: context.control_source_stream,
        })
        .map_err(dynamic_error)?;
        let mut host_context = Box::new(MarketHostContext {
            publisher: context.event_publisher,
        });
        let host = Box::new(TitanMarketHostApiV1 {
            struct_size: std::mem::size_of::<TitanMarketHostApiV1>() as u32,
            context: (&mut *host_context as *mut MarketHostContext).cast(),
            publish_market: Some(host_publish_market),
            publish_control: Some(host_publish_control),
        });
        let mut handle = 0;
        let plugin_handle = self.session.handle();
        // SAFETY: all buffers and descriptors remain valid for the call; the connector retains
        // host only while the returned proxy retains both host boxes.
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
            return Err(ConnectorError::new(
                "dynamic market factory returned a zero connector handle",
            ));
        }
        let connector = Arc::new(DynamicMarketConnector {
            api: self.api,
            handle,
            host_context,
            host,
            session: self.session.clone(),
            destroyed: Mutex::new(false),
        });
        let weak: Weak<DynamicMarketConnector> = Arc::downgrade(&connector);
        resources
            .register(
                "dynamic-market-connector",
                ClosureResource(Some(move || {
                    if let Some(connector) = weak.upgrade() {
                        connector
                            .stop(Instant::now() + Duration::from_secs(5))
                            .map_err(|error| {
                                titan_plugin_engine::PluginError::new(
                                    titan_plugin_engine::ErrorKind::ResourceReleaseFailed,
                                    titan_plugin_engine::PluginIdentity::new(
                                        "titan.market",
                                        "dynamic-connector",
                                    ),
                                    titan_plugin_engine::LifecycleState::Stopping,
                                    "stop_dynamic_market_connector",
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

struct MarketHostContext {
    publisher: MarketEventPublisher,
}

struct DynamicMarketConnector {
    api: TitanMarketConnectorFactoryApiV1,
    handle: TitanMarketConnectorHandle,
    host_context: Box<MarketHostContext>,
    host: Box<TitanMarketHostApiV1>,
    session: DynamicPluginSession,
    destroyed: Mutex<bool>,
}

// SAFETY: the foreign connector contract is explicitly thread-safe and all host state is
// immutable or internally synchronized for the descriptor lifetime.
unsafe impl Send for DynamicMarketConnector {}
// SAFETY: same ABI contract and synchronization invariant as `Send`.
unsafe impl Sync for DynamicMarketConnector {}

impl DynamicMarketConnector {
    fn output<T: DeserializeOwned>(
        &self,
        operation: &'static str,
        call: impl FnOnce(*mut TitanBuffer) -> TitanStatus,
    ) -> Result<T, ConnectorError> {
        let mut output = TitanBuffer::default();
        status(&self.api, call(&mut output), operation)?;
        // SAFETY: the plugin produced this buffer under the TitanBuffer ownership contract.
        let bytes = unsafe { output.copy_and_free() }.map_err(dynamic_error)?;
        serde_json::from_slice(&bytes).map_err(dynamic_error)
    }
}

impl MarketConnector for DynamicMarketConnector {
    fn start(&self) -> Result<(), ConnectorError> {
        status(
            &self.api,
            unsafe { self.api.start.unwrap()(self.handle) },
            "start",
        )
    }

    fn stop(&self, deadline: Instant) -> Result<(), ConnectorError> {
        let timeout = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or(Duration::ZERO)
            .as_nanos()
            .min(u64::MAX as u128) as u64;
        status(
            &self.api,
            unsafe { self.api.stop.unwrap()(self.handle, timeout) },
            "stop",
        )
    }

    fn subscribe(
        &self,
        request: MarketSubscribeRequest,
    ) -> Result<MarketSubscription, ConnectorError> {
        let input = serde_json::to_vec(&request).map_err(dynamic_error)?;
        self.output("subscribe", |output| unsafe {
            self.api.subscribe.unwrap()(self.handle, input.as_ptr(), input.len(), output)
        })
    }

    fn unsubscribe(&self, subscription: MarketSubscription) -> Result<OperationId, ConnectorError> {
        self.output("unsubscribe", |output| unsafe {
            self.api.unsubscribe.unwrap()(self.handle, subscription.id, output)
        })
    }

    fn request_snapshot(&self, asset_id: AssetId) -> Result<OperationId, ConnectorError> {
        self.output("request_snapshot", |output| unsafe {
            self.api.request_snapshot.unwrap()(self.handle, asset_id.0, output)
        })
    }

    fn instruments(&self) -> Arc<[InstrumentSnapshot]> {
        self.output::<Vec<InstrumentSnapshot>>("instruments", |output| unsafe {
            self.api.instruments.unwrap()(self.handle, output)
        })
        .map(Arc::from)
        .unwrap_or_default()
    }

    fn health(&self) -> ConnectorHealthSnapshot {
        self.output("health", |output| unsafe {
            self.api.health.unwrap()(self.handle, output)
        })
        .unwrap_or_else(|error| ConnectorHealthSnapshot {
            state: ConnectorHealth::Failed,
            message: Arc::from(error.to_string()),
            observed_at: SystemTime::now(),
        })
    }

    fn diagnostics(&self) -> ConnectorDiagnosticSnapshot {
        self.output("diagnostics", |output| unsafe {
            self.api.diagnostics.unwrap()(self.handle, output)
        })
        .unwrap_or_else(|error| ConnectorDiagnosticSnapshot {
            summary: Arc::from(error.to_string()),
        })
    }

    fn operation(&self, id: OperationId) -> ConnectorOperationSnapshot {
        self.output("operation", |output| unsafe {
            self.api.operation.unwrap()(self.handle, id.0, output)
        })
        .unwrap_or_else(|error| ConnectorOperationSnapshot {
            id,
            state: OperationState::Failed,
            detail: Arc::from(error.to_string()),
        })
    }
}

impl Drop for DynamicMarketConnector {
    fn drop(&mut self) {
        let destroyed = self.destroyed.get_mut().unwrap_or_else(|p| p.into_inner());
        if !*destroyed {
            let _ = unsafe { self.api.destroy.unwrap()(self.handle) };
            *destroyed = true;
        }
        let _ = (&self.host_context, &self.host, &self.session);
    }
}

unsafe extern "C" fn host_publish_market(
    context: *mut c_void,
    event_type: *const u8,
    event_type_len: usize,
    payload: *const u8,
    payload_len: usize,
    asset_id: u32,
    exchange_ts: i64,
    receive_ts: i64,
    trace_id: u64,
    causation_id: u64,
) -> TitanStatus {
    callback_status(|| {
        let context = unsafe { (context as *const MarketHostContext).as_ref() }.ok_or(())?;
        let event_type = unsafe { foreign_str(event_type, event_type_len) }?;
        let payload = unsafe { foreign_bytes(payload, payload_len) }?;
        context
            .publisher
            .publish_market(
                event_type,
                payload,
                AssetId(asset_id),
                exchange_ts,
                receive_ts,
                TraceContext {
                    trace_id,
                    causation_id,
                },
            )
            .map_err(|_| ())
    })
}

unsafe extern "C" fn host_publish_control(
    context: *mut c_void,
    event_type: *const u8,
    event_type_len: usize,
    payload: *const u8,
    payload_len: usize,
    trace_id: u64,
    causation_id: u64,
) -> TitanStatus {
    callback_status(|| {
        let context = unsafe { (context as *const MarketHostContext).as_ref() }.ok_or(())?;
        context
            .publisher
            .publish_control(
                unsafe { foreign_str(event_type, event_type_len) }?,
                unsafe { foreign_bytes(payload, payload_len) }?,
                TraceContext {
                    trace_id,
                    causation_id,
                },
            )
            .map_err(|_| ())
    })
}

fn callback_status(call: impl FnOnce() -> Result<(), ()>) -> TitanStatus {
    match catch_unwind(AssertUnwindSafe(call)) {
        Ok(Ok(())) => TITAN_STATUS_OK,
        Ok(Err(())) => TITAN_STATUS_HOST_ERROR,
        Err(_) => TITAN_STATUS_PANIC,
    }
}

fn validate_api(api: &TitanMarketConnectorFactoryApiV1) -> Result<(), ConnectorError> {
    if api.magic != TITAN_MARKET_FACTORY_MAGIC
        || api.struct_size < std::mem::size_of::<TitanMarketConnectorFactoryApiV1>() as u32
        || api.abi_major != TITAN_MARKET_FACTORY_ABI_MAJOR
        || api.abi_minor > TITAN_MARKET_FACTORY_ABI_MINOR
    {
        return Err(ConnectorError::new(
            "incompatible dynamic market factory ABI",
        ));
    }
    if api.connector_type.is_none()
        || api.create.is_none()
        || api.destroy.is_none()
        || api.start.is_none()
        || api.stop.is_none()
        || api.subscribe.is_none()
        || api.unsubscribe.is_none()
        || api.request_snapshot.is_none()
        || api.instruments.is_none()
        || api.health.is_none()
        || api.diagnostics.is_none()
        || api.operation.is_none()
        || api.last_error.is_none()
    {
        return Err(ConnectorError::new(
            "dynamic market factory ABI is truncated",
        ));
    }
    Ok(())
}

fn status(
    api: &TitanMarketConnectorFactoryApiV1,
    value: TitanStatus,
    operation: &str,
) -> Result<(), ConnectorError> {
    if value == TITAN_STATUS_OK {
        return Ok(());
    }
    let mut bytes = vec![0_u8; 16 * 1024];
    let length =
        unsafe { api.last_error.unwrap()(bytes.as_mut_ptr(), bytes.len()) }.min(bytes.len());
    bytes.truncate(length);
    let detail = std::str::from_utf8(&bytes).unwrap_or("dynamic connector call failed");
    Err(ConnectorError::new(format!("{operation}: {detail}")))
}

unsafe fn foreign_bytes<'a>(pointer: *const u8, length: usize) -> Result<&'a [u8], ()> {
    if length == 0 {
        return Ok(&[]);
    }
    if pointer.is_null() || length > isize::MAX as usize {
        return Err(());
    }
    Ok(unsafe { std::slice::from_raw_parts(pointer, length) })
}

unsafe fn foreign_str<'a>(pointer: *const u8, length: usize) -> Result<&'a str, ()> {
    std::str::from_utf8(unsafe { foreign_bytes(pointer, length) }?).map_err(|_| ())
}

fn dynamic_error(error: impl std::fmt::Display) -> ConnectorError {
    ConnectorError::new(error.to_string())
}
