//! Shared implementation used by the per-venue `cdylib` plugin packages.

use std::{
    cell::RefCell,
    collections::HashMap,
    ffi::c_void,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};

use serde::{Serialize, de::DeserializeOwned};
use titan_account_plugin as account;
use titan_market_plugin as market;
use titan_plugin_engine::{
    PluginError, PluginIdentity, ResourceScope, TITAN_STATUS_HOST_ERROR, TITAN_STATUS_OK,
    TITAN_STATUS_PANIC, TitanBuffer, TitanPluginHandle, TitanStatus, TraceContext,
};

#[cfg(feature = "binancefutures")]
use crate::market_plugin::BinanceFuturesMarketFactory;
#[cfg(feature = "hyperliquid")]
use crate::market_plugin::HyperliquidMarketFactory;
#[cfg(feature = "okx")]
use crate::market_plugin::OkxMarketFactory;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DynamicVenue {
    BinanceFutures,
    Okx,
    Hyperliquid,
}

impl DynamicVenue {
    pub const fn market_connector_type(self) -> &'static str {
        match self {
            Self::BinanceFutures => "binance-futures",
            Self::Okx => "okx",
            Self::Hyperliquid => "hyperliquid",
        }
    }

    pub const fn account_connector_type(self) -> &'static str {
        match self {
            Self::BinanceFutures => "binance-futures-account",
            Self::Okx => "okx-account",
            Self::Hyperliquid => "hyperliquid-account",
        }
    }
}

struct RootEntry {
    venue: DynamicVenue,
    running: bool,
}

struct MarketEntry {
    root: TitanPluginHandle,
    _scope: ResourceScope,
    connector: Arc<dyn market::MarketConnector>,
    _sink: Arc<ForeignMarketSink>,
}

struct AccountEntry {
    root: TitanPluginHandle,
    _scope: ResourceScope,
    connector: Arc<dyn account::AccountConnector>,
    _sink: Arc<ForeignAccountSink>,
    _secret_provider: Arc<ForeignSecretProvider>,
}

#[derive(Default)]
struct DynamicState {
    next_handle: u64,
    roots: HashMap<TitanPluginHandle, RootEntry>,
    markets: HashMap<u64, MarketEntry>,
    accounts: HashMap<u64, AccountEntry>,
}

fn state() -> &'static Mutex<DynamicState> {
    static STATE: OnceLock<Mutex<DynamicState>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(DynamicState::default()))
}

thread_local! {
    static LAST_ERROR: RefCell<String> = const { RefCell::new(String::new()) };
}

fn set_error(message: impl Into<String>) {
    let message = message.into();
    let _ = LAST_ERROR.try_with(|slot| {
        if let Ok(mut slot) = slot.try_borrow_mut() {
            *slot = message;
        }
    });
}

fn next_handle(state: &mut DynamicState) -> u64 {
    state.next_handle = state.next_handle.saturating_add(1).max(1);
    state.next_handle
}

pub unsafe extern "C" fn last_error(output: *mut u8, capacity: usize) -> usize {
    LAST_ERROR
        .try_with(|slot| {
            let Ok(message) = slot.try_borrow() else {
                return 0;
            };
            let length = message.len().min(capacity);
            if length != 0 && !output.is_null() {
                // SAFETY: caller supplies a writable buffer of `capacity` bytes.
                unsafe { std::ptr::copy_nonoverlapping(message.as_ptr(), output, length) };
            }
            length
        })
        .unwrap_or(0)
}

pub unsafe fn create_root_from_json(
    venue: DynamicVenue,
    input: *const u8,
    input_len: usize,
    output: *mut TitanPluginHandle,
) -> TitanStatus {
    ffi_status(|| {
        let input = unsafe { foreign_bytes(input, input_len) }
            .map_err(|_| "root configuration buffer is invalid".to_string())?;
        serde_json::from_slice::<serde_json::Value>(input)
            .map_err(|error| format!("root configuration is invalid: {error}"))?;
        create_root_inner(venue, output)
    })
}

fn create_root_inner(venue: DynamicVenue, output: *mut TitanPluginHandle) -> Result<(), String> {
    if output.is_null() {
        return Err("root output pointer is null".into());
    }
    let mut state = state().lock().unwrap_or_else(|p| p.into_inner());
    let handle = next_handle(&mut state);
    state.roots.insert(
        handle,
        RootEntry {
            venue,
            running: false,
        },
    );
    // SAFETY: output was checked and is owned by the caller for this call.
    unsafe { output.write(handle) };
    Ok(())
}

pub unsafe extern "C" fn destroy_root(handle: TitanPluginHandle) -> TitanStatus {
    ffi_status(|| {
        let mut state = state().lock().unwrap_or_else(|p| p.into_inner());
        if state.markets.values().any(|entry| entry.root == handle)
            || state.accounts.values().any(|entry| entry.root == handle)
        {
            return Err("cannot destroy plugin root while connectors are active".into());
        }
        state
            .roots
            .remove(&handle)
            .ok_or_else(|| "unknown plugin root".to_string())?;
        Ok(())
    })
}

pub unsafe extern "C" fn validate_root(handle: TitanPluginHandle) -> TitanStatus {
    ffi_status(|| {
        if state()
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .roots
            .contains_key(&handle)
        {
            Ok(())
        } else {
            Err("unknown plugin root".into())
        }
    })
}

pub unsafe extern "C" fn start_root(
    handle: TitanPluginHandle,
    _host: *const titan_plugin_engine::TitanHostApiV1,
) -> TitanStatus {
    ffi_status(|| {
        let mut state = state().lock().unwrap_or_else(|p| p.into_inner());
        let root = state
            .roots
            .get_mut(&handle)
            .ok_or_else(|| "unknown plugin root".to_string())?;
        root.running = true;
        Ok(())
    })
}

pub unsafe extern "C" fn quiesce_root(handle: TitanPluginHandle, _reason: u32) -> TitanStatus {
    ffi_status(|| {
        let mut state = state().lock().unwrap_or_else(|p| p.into_inner());
        let root = state
            .roots
            .get_mut(&handle)
            .ok_or_else(|| "unknown plugin root".to_string())?;
        root.running = false;
        Ok(())
    })
}

pub unsafe extern "C" fn stop_root(handle: TitanPluginHandle) -> TitanStatus {
    ffi_status(|| {
        let mut state = state().lock().unwrap_or_else(|p| p.into_inner());
        let root = state
            .roots
            .get_mut(&handle)
            .ok_or_else(|| "unknown plugin root".to_string())?;
        root.running = false;
        Ok(())
    })
}

pub unsafe extern "C" fn query_interface(
    handle: TitanPluginHandle,
    name: *const u8,
    name_len: usize,
    major: u16,
    output: *mut *const c_void,
) -> TitanStatus {
    ffi_status(|| {
        if output.is_null() {
            return Err("interface output pointer is null".into());
        }
        if !state()
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .roots
            .contains_key(&handle)
        {
            return Err("unknown plugin root".into());
        }
        // SAFETY: plugin engine supplies a readable name range for this call.
        let name = unsafe { foreign_str(name, name_len) }
            .map_err(|_| "interface name is invalid".to_string())?;
        let value = match (name, major) {
            (market::TITAN_MARKET_FACTORY_INTERFACE, market::TITAN_MARKET_FACTORY_ABI_MAJOR) => {
                (&MARKET_API as *const market::TitanMarketConnectorFactoryApiV1).cast()
            }
            (
                account::TITAN_ACCOUNT_FACTORY_INTERFACE,
                account::TITAN_ACCOUNT_FACTORY_ABI_MAJOR,
            ) => (&ACCOUNT_API as *const account::TitanAccountConnectorFactoryApiV1).cast(),
            _ => return Err("unsupported plugin interface".into()),
        };
        // SAFETY: output is non-null and points to caller-owned writable storage.
        unsafe { output.write(value) };
        Ok(())
    })
}

fn root_venue(handle: TitanPluginHandle) -> Result<DynamicVenue, String> {
    let state = state().lock().unwrap_or_else(|p| p.into_inner());
    let root = state
        .roots
        .get(&handle)
        .ok_or_else(|| "unknown plugin root".to_string())?;
    if !root.running {
        return Err("plugin root is not running".into());
    }
    Ok(root.venue)
}

fn market_factory(venue: DynamicVenue) -> Result<Arc<dyn market::MarketConnectorFactory>, String> {
    match venue {
        DynamicVenue::BinanceFutures => {
            #[cfg(feature = "binancefutures")]
            return Ok(Arc::new(BinanceFuturesMarketFactory));
            #[cfg(not(feature = "binancefutures"))]
            return Err("binance futures feature is disabled".into());
        }
        DynamicVenue::Okx => {
            #[cfg(feature = "okx")]
            return Ok(Arc::new(OkxMarketFactory));
            #[cfg(not(feature = "okx"))]
            return Err("okx feature is disabled".into());
        }
        DynamicVenue::Hyperliquid => {
            #[cfg(feature = "hyperliquid")]
            return Ok(Arc::new(HyperliquidMarketFactory));
            #[cfg(not(feature = "hyperliquid"))]
            return Err("hyperliquid feature is disabled".into());
        }
    }
}

fn account_factory(
    venue: DynamicVenue,
) -> Result<Arc<dyn account::AccountConnectorFactory>, String> {
    crate::account_plugin::venue_account_factories()
        .into_iter()
        .find(|factory| factory.connector_type() == venue.account_connector_type())
        .ok_or_else(|| format!("{} feature is disabled", venue.account_connector_type()))
}

unsafe extern "C" fn market_connector_type(
    root: TitanPluginHandle,
    length: *mut usize,
) -> *const u8 {
    catch_unwind(AssertUnwindSafe(|| {
        if length.is_null() {
            return std::ptr::null();
        }
        match root_venue(root) {
            Ok(venue) => {
                let value = venue.market_connector_type();
                unsafe { length.write(value.len()) };
                value.as_ptr()
            }
            Err(error) => {
                set_error(error);
                std::ptr::null()
            }
        }
    }))
    .unwrap_or(std::ptr::null())
}

unsafe extern "C" fn account_connector_type(
    root: TitanPluginHandle,
    length: *mut usize,
) -> *const u8 {
    catch_unwind(AssertUnwindSafe(|| {
        if length.is_null() {
            return std::ptr::null();
        }
        match root_venue(root) {
            Ok(venue) => {
                let value = venue.account_connector_type();
                unsafe { length.write(value.len()) };
                value.as_ptr()
            }
            Err(error) => {
                set_error(error);
                std::ptr::null()
            }
        }
    }))
    .unwrap_or(std::ptr::null())
}

#[derive(Clone, Copy)]
struct ForeignMarketSink {
    host: market::TitanMarketHostApiV1,
}

impl market::MarketEventSink for ForeignMarketSink {
    fn publish_market(
        &self,
        event_type: &str,
        payload: &[u8],
        asset_id: market::AssetId,
        exchange_ts: i64,
        receive_ts: i64,
        trace: TraceContext,
    ) -> Result<(), PluginError> {
        let status = unsafe {
            self.host.publish_market.unwrap()(
                self.host.context,
                event_type.as_ptr(),
                event_type.len(),
                payload.as_ptr(),
                payload.len(),
                asset_id.0,
                exchange_ts,
                receive_ts,
                trace.trace_id,
                trace.causation_id,
            )
        };
        callback_plugin_result(status, "publish_market")
    }

    fn publish_control(
        &self,
        event_type: &str,
        payload: &[u8],
        trace: TraceContext,
    ) -> Result<(), PluginError> {
        let status = unsafe {
            self.host.publish_control.unwrap()(
                self.host.context,
                event_type.as_ptr(),
                event_type.len(),
                payload.as_ptr(),
                payload.len(),
                trace.trace_id,
                trace.causation_id,
            )
        };
        callback_plugin_result(status, "publish_control")
    }
}

unsafe extern "C" fn market_create(
    root: TitanPluginHandle,
    input: *const u8,
    input_len: usize,
    host: *const market::TitanMarketHostApiV1,
    output: *mut market::TitanMarketConnectorHandle,
) -> TitanStatus {
    ffi_status(|| {
        if host.is_null() || output.is_null() {
            return Err("market create argument is null".into());
        }
        let venue = root_venue(root)?;
        // SAFETY: the host descriptor remains valid for the call and is copied immediately.
        let host = unsafe { *host };
        if host.struct_size < std::mem::size_of::<market::TitanMarketHostApiV1>() as u32
            || host.publish_market.is_none()
            || host.publish_control.is_none()
        {
            return Err("market host API is incompatible".into());
        }
        let request: market::DynamicMarketCreateRequest = unsafe { decode_json(input, input_len) }?;
        if request.definition.connector_type.as_ref() != venue.market_connector_type() {
            return Err("market connector type does not match plugin package".into());
        }
        let sink = Arc::new(ForeignMarketSink { host });
        let scope = ResourceScope::new(PluginIdentity::new(
            "titan.dynamic.market",
            request.definition.source_key.clone(),
        ));
        let context = market::MarketConnectorContext {
            source: request.source,
            instruments: request.definition.instruments.clone(),
            market_source_stream: request.market_source_stream,
            control_source_stream: request.control_source_stream,
            event_publisher: market::MarketEventPublisher::from_sink(sink.clone()),
            resources: scope.handle(),
        };
        let connector = market_factory(venue)?
            .create(&request.definition, context)
            .map_err(|error| error.to_string())?;
        let mut state = state().lock().unwrap_or_else(|p| p.into_inner());
        let handle = next_handle(&mut state);
        state.markets.insert(
            handle,
            MarketEntry {
                root,
                _scope: scope,
                connector,
                _sink: sink,
            },
        );
        unsafe { output.write(handle) };
        Ok(())
    })
}

unsafe extern "C" fn market_destroy(handle: u64) -> TitanStatus {
    ffi_status(|| {
        state()
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .markets
            .remove(&handle)
            .ok_or_else(|| "unknown market connector".to_string())?;
        Ok(())
    })
}

fn with_market<T>(
    handle: u64,
    call: impl FnOnce(&dyn market::MarketConnector) -> T,
) -> Result<T, String> {
    let connector = state()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .markets
        .get(&handle)
        .map(|entry| entry.connector.clone())
        .ok_or_else(|| "unknown market connector".to_string())?;
    Ok(call(connector.as_ref()))
}

unsafe extern "C" fn market_start(handle: u64) -> TitanStatus {
    ffi_status(|| with_market(handle, |c| c.start())?.map_err(|e| e.to_string()))
}

unsafe extern "C" fn market_stop(handle: u64, timeout_ns: u64) -> TitanStatus {
    ffi_status(|| {
        with_market(handle, |c| {
            c.stop(Instant::now() + Duration::from_nanos(timeout_ns))
        })?
        .map_err(|e| e.to_string())
    })
}

unsafe extern "C" fn market_subscribe(
    handle: u64,
    input: *const u8,
    input_len: usize,
    output: *mut TitanBuffer,
) -> TitanStatus {
    ffi_json(
        || {
            let request = unsafe { decode_json(input, input_len) }?;
            with_market(handle, |c| c.subscribe(request))?.map_err(|e| e.to_string())
        },
        output,
    )
}

unsafe extern "C" fn market_unsubscribe(
    handle: u64,
    id: u64,
    output: *mut TitanBuffer,
) -> TitanStatus {
    ffi_json(
        || {
            with_market(handle, |c| c.unsubscribe(market::MarketSubscription { id }))?
                .map_err(|e| e.to_string())
        },
        output,
    )
}

unsafe extern "C" fn market_request_snapshot(
    handle: u64,
    asset: u32,
    output: *mut TitanBuffer,
) -> TitanStatus {
    ffi_json(
        || {
            with_market(handle, |c| c.request_snapshot(market::AssetId(asset)))?
                .map_err(|e| e.to_string())
        },
        output,
    )
}

unsafe extern "C" fn market_instruments(handle: u64, output: *mut TitanBuffer) -> TitanStatus {
    ffi_json(|| with_market(handle, |c| c.instruments().to_vec()), output)
}

unsafe extern "C" fn market_health(handle: u64, output: *mut TitanBuffer) -> TitanStatus {
    ffi_json(|| with_market(handle, |c| c.health()), output)
}

unsafe extern "C" fn market_diagnostics(handle: u64, output: *mut TitanBuffer) -> TitanStatus {
    ffi_json(|| with_market(handle, |c| c.diagnostics()), output)
}

unsafe extern "C" fn market_operation(
    handle: u64,
    id: u64,
    output: *mut TitanBuffer,
) -> TitanStatus {
    ffi_json(
        || with_market(handle, |c| c.operation(market::OperationId(id))),
        output,
    )
}

#[derive(Clone, Copy)]
struct ForeignAccountSink {
    host: account::TitanAccountHostApiV1,
}

impl account::AccountEventSink for ForeignAccountSink {
    fn publish(
        &self,
        event_type: &str,
        payload: &[u8],
        trace: TraceContext,
    ) -> Result<(), PluginError> {
        let status = unsafe {
            self.host.publish_account.unwrap()(
                self.host.context,
                event_type.as_ptr(),
                event_type.len(),
                payload.as_ptr(),
                payload.len(),
                trace.trace_id,
                trace.causation_id,
            )
        };
        callback_plugin_result(status, "publish_account")
    }
}

#[derive(Clone, Copy)]
struct ForeignSecretProvider {
    host: account::TitanAccountHostApiV1,
}

impl account::SecretProvider for ForeignSecretProvider {
    fn resolve(
        &self,
        reference: &account::SecretRef,
    ) -> Result<account::SecretValue, account::AccountConnectorError> {
        let mut output = TitanBuffer::default();
        let status = unsafe {
            self.host.resolve_secret.unwrap()(
                self.host.context,
                reference.as_str().as_ptr(),
                reference.as_str().len(),
                &mut output,
            )
        };
        if status != TITAN_STATUS_OK {
            return Err(account::AccountConnectorError::new(
                account::AccountErrorKind::CredentialUnavailable,
                "credential unavailable",
            ));
        }
        let bytes = unsafe { output.copy_and_free() }.map_err(|_| {
            account::AccountConnectorError::new(
                account::AccountErrorKind::CredentialUnavailable,
                "credential unavailable",
            )
        })?;
        Ok(account::SecretValue::new(bytes))
    }
}

unsafe extern "C" fn account_create(
    root: TitanPluginHandle,
    input: *const u8,
    input_len: usize,
    host: *const account::TitanAccountHostApiV1,
    output: *mut account::TitanAccountConnectorHandle,
) -> TitanStatus {
    ffi_status(|| {
        if host.is_null() || output.is_null() {
            return Err("account create argument is null".into());
        }
        let venue = root_venue(root)?;
        let host = unsafe { *host };
        if host.struct_size < std::mem::size_of::<account::TitanAccountHostApiV1>() as u32
            || host.publish_account.is_none()
            || host.resolve_secret.is_none()
        {
            return Err("account host API is incompatible".into());
        }
        let request: account::DynamicAccountCreateRequest =
            unsafe { decode_json(input, input_len) }?;
        if request.definition.connector_type.as_ref() != venue.account_connector_type() {
            return Err("account connector type does not match plugin package".into());
        }
        let sink = Arc::new(ForeignAccountSink { host });
        let secret_provider = Arc::new(ForeignSecretProvider { host });
        let scope = ResourceScope::new(PluginIdentity::new(
            "titan.dynamic.account",
            request.definition.account_key.clone(),
        ));
        let context = account::AccountConnectorContext {
            account: request.account,
            instruments: request.definition.instruments.clone(),
            currencies: request.definition.currencies.clone(),
            ownership: request.definition.ownership.clone(),
            account_stream: request.account_stream,
            control_stream: request.control_stream,
            event_publisher: account::AccountEventPublisher::from_sink(
                request.account,
                sink.clone(),
            ),
            resources: scope.handle(),
            secrets: account::ScopedSecretResolver::scoped(
                request.definition.credential_ref.clone(),
                secret_provider.clone(),
            ),
            command_queue_capacity: request.command_queue_capacity,
        };
        let connector = account_factory(venue)?
            .create(&request.definition, context)
            .map_err(|error| error.to_string())?;
        let mut state = state().lock().unwrap_or_else(|p| p.into_inner());
        let handle = next_handle(&mut state);
        state.accounts.insert(
            handle,
            AccountEntry {
                root,
                _scope: scope,
                connector,
                _sink: sink,
                _secret_provider: secret_provider,
            },
        );
        unsafe { output.write(handle) };
        Ok(())
    })
}

unsafe extern "C" fn account_destroy(handle: u64) -> TitanStatus {
    ffi_status(|| {
        state()
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .accounts
            .remove(&handle)
            .ok_or_else(|| "unknown account connector".to_string())?;
        Ok(())
    })
}

fn with_account<T>(
    handle: u64,
    call: impl FnOnce(&dyn account::AccountConnector) -> T,
) -> Result<T, String> {
    let connector = state()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .accounts
        .get(&handle)
        .map(|entry| entry.connector.clone())
        .ok_or_else(|| "unknown account connector".to_string())?;
    Ok(call(connector.as_ref()))
}

unsafe extern "C" fn account_start(handle: u64) -> TitanStatus {
    ffi_status(|| with_account(handle, |c| c.start())?.map_err(|e| e.to_string()))
}

unsafe extern "C" fn account_stop(handle: u64, timeout_ns: u64) -> TitanStatus {
    ffi_status(|| {
        with_account(handle, |c| {
            c.stop(Instant::now() + Duration::from_nanos(timeout_ns))
        })?
        .map_err(|e| e.to_string())
    })
}

macro_rules! account_json_call {
    ($name:ident, $input:ty, $method:ident) => {
        unsafe extern "C" fn $name(
            handle: u64,
            input: *const u8,
            input_len: usize,
            output: *mut TitanBuffer,
        ) -> TitanStatus {
            ffi_json(
                || {
                    let request: $input = unsafe { decode_json(input, input_len) }?;
                    with_account(handle, |connector| connector.$method(request))?
                        .map_err(|error| error.to_string())
                },
                output,
            )
        }
    };
}

account_json_call!(account_submit, account::SubmitOrderCommand, submit);
account_json_call!(account_amend, account::AmendOrderCommand, amend);
account_json_call!(account_cancel, account::CancelOrderCommand, cancel);
account_json_call!(account_cancel_all, account::CancelAllCommand, cancel_all);
account_json_call!(
    account_cancel_all_after,
    account::CancelAllAfterCommand,
    cancel_all_after
);
account_json_call!(account_reconcile, account::ReconcileScope, reconcile);
account_json_call!(account_orders, account::OrderFilter, orders);
account_json_call!(account_positions, account::PositionFilter, positions);

unsafe extern "C" fn account_balances(
    handle: u64,
    _: *const u8,
    _: usize,
    output: *mut TitanBuffer,
) -> TitanStatus {
    ffi_json(
        || with_account(handle, |c| c.balances())?.map_err(|e| e.to_string()),
        output,
    )
}

unsafe extern "C" fn account_health(
    handle: u64,
    _: *const u8,
    _: usize,
    output: *mut TitanBuffer,
) -> TitanStatus {
    ffi_json(|| with_account(handle, |c| c.health()), output)
}

unsafe extern "C" fn account_diagnostics(
    handle: u64,
    _: *const u8,
    _: usize,
    output: *mut TitanBuffer,
) -> TitanStatus {
    ffi_json(|| with_account(handle, |c| c.diagnostics()), output)
}

unsafe extern "C" fn account_operation(
    handle: u64,
    input: *const u8,
    input_len: usize,
    output: *mut TitanBuffer,
) -> TitanStatus {
    ffi_json(
        || {
            let id: account::OperationId = unsafe { decode_json(input, input_len) }?;
            with_account(handle, |c| c.operation(id))
        },
        output,
    )
}

pub static MARKET_API: market::TitanMarketConnectorFactoryApiV1 =
    market::TitanMarketConnectorFactoryApiV1 {
        magic: market::TITAN_MARKET_FACTORY_MAGIC,
        struct_size: std::mem::size_of::<market::TitanMarketConnectorFactoryApiV1>() as u32,
        abi_major: market::TITAN_MARKET_FACTORY_ABI_MAJOR,
        abi_minor: market::TITAN_MARKET_FACTORY_ABI_MINOR,
        connector_type: Some(market_connector_type),
        create: Some(market_create),
        destroy: Some(market_destroy),
        start: Some(market_start),
        stop: Some(market_stop),
        subscribe: Some(market_subscribe),
        unsubscribe: Some(market_unsubscribe),
        request_snapshot: Some(market_request_snapshot),
        instruments: Some(market_instruments),
        health: Some(market_health),
        diagnostics: Some(market_diagnostics),
        operation: Some(market_operation),
        last_error: Some(last_error),
    };

pub static ACCOUNT_API: account::TitanAccountConnectorFactoryApiV1 =
    account::TitanAccountConnectorFactoryApiV1 {
        magic: account::TITAN_ACCOUNT_FACTORY_MAGIC,
        struct_size: std::mem::size_of::<account::TitanAccountConnectorFactoryApiV1>() as u32,
        abi_major: account::TITAN_ACCOUNT_FACTORY_ABI_MAJOR,
        abi_minor: account::TITAN_ACCOUNT_FACTORY_ABI_MINOR,
        connector_type: Some(account_connector_type),
        create: Some(account_create),
        destroy: Some(account_destroy),
        start: Some(account_start),
        stop: Some(account_stop),
        submit: Some(account_submit),
        amend: Some(account_amend),
        cancel: Some(account_cancel),
        cancel_all: Some(account_cancel_all),
        cancel_all_after: Some(account_cancel_all_after),
        reconcile: Some(account_reconcile),
        orders: Some(account_orders),
        positions: Some(account_positions),
        balances: Some(account_balances),
        health: Some(account_health),
        diagnostics: Some(account_diagnostics),
        operation: Some(account_operation),
        last_error: Some(last_error),
    };

fn ffi_status(call: impl FnOnce() -> Result<(), String>) -> TitanStatus {
    match catch_unwind(AssertUnwindSafe(call)) {
        Ok(Ok(())) => TITAN_STATUS_OK,
        Ok(Err(error)) => {
            set_error(error);
            TITAN_STATUS_HOST_ERROR
        }
        Err(_) => {
            set_error("dynamic connector panicked");
            TITAN_STATUS_PANIC
        }
    }
}

fn ffi_json<T: Serialize>(
    call: impl FnOnce() -> Result<T, String>,
    output: *mut TitanBuffer,
) -> TitanStatus {
    ffi_status(|| {
        if output.is_null() {
            return Err("output buffer pointer is null".into());
        }
        let value = call()?;
        let mut bytes = serde_json::to_vec(&value).map_err(|error| error.to_string())?;
        let buffer = TitanBuffer {
            data: bytes.as_mut_ptr(),
            len: bytes.len(),
            capacity: bytes.capacity(),
            free: Some(free_output),
        };
        std::mem::forget(bytes);
        unsafe { output.write(buffer) };
        Ok(())
    })
}

unsafe extern "C" fn free_output(data: *mut u8, len: usize, capacity: usize) {
    if data.is_null()
        || len > capacity
        || len > isize::MAX as usize
        || capacity > isize::MAX as usize
    {
        return;
    }
    let _ = unsafe { Vec::from_raw_parts(data, len, capacity) };
}

unsafe fn decode_json<T: DeserializeOwned>(
    input: *const u8,
    input_len: usize,
) -> Result<T, String> {
    let bytes = unsafe { foreign_bytes(input, input_len) }
        .map_err(|_| "input buffer is invalid".to_string())?;
    serde_json::from_slice(bytes).map_err(|error| error.to_string())
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

fn callback_plugin_result(status: TitanStatus, operation: &'static str) -> Result<(), PluginError> {
    if status == TITAN_STATUS_OK {
        Ok(())
    } else {
        Err(PluginError::new(
            titan_plugin_engine::ErrorKind::PluginFailed,
            PluginIdentity::new("titan.dynamic.connector", "publisher"),
            titan_plugin_engine::LifecycleState::Running,
            operation,
            "host callback rejected dynamic connector publication",
        ))
    }
}

#[cfg(test)]
mod dynamic_abi_tests {
    use super::*;

    #[test]
    fn root_create_rejects_invalid_foreign_ranges_before_reading() {
        let mut handle = 0;
        let oversized = unsafe {
            create_root_from_json(
                DynamicVenue::BinanceFutures,
                std::ptr::NonNull::<u8>::dangling().as_ptr(),
                isize::MAX as usize + 1,
                &mut handle,
            )
        };
        assert_eq!(oversized, TITAN_STATUS_HOST_ERROR);
        assert_eq!(handle, 0);

        let config = br#"{}"#;
        assert_eq!(
            unsafe {
                create_root_from_json(
                    DynamicVenue::BinanceFutures,
                    config.as_ptr(),
                    config.len(),
                    &mut handle,
                )
            },
            TITAN_STATUS_OK
        );
        assert_ne!(handle, 0);
        assert_eq!(unsafe { destroy_root(handle) }, TITAN_STATUS_OK);
    }
}
