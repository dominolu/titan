use std::{
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Instant, SystemTime},
};

pub use titan_runtime_abi::DecimalUnit;

use serde::{Deserialize, Serialize};
use titan_plugin_engine::{
    EventPublishMetadata, EventPublisher, PluginError, ResourceScopeHandle, TraceContext,
};
use zeroize::Zeroize;

use crate::{
    ACCOUNT_EVENT_SCHEMA_VERSION, ACCOUNT_EVENT_TYPES, AccountConnectorError, AccountErrorKind,
    AccountEventHeaderV1, decode_account_event_header,
};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[repr(transparent)]
pub struct AccountId(pub u32);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[repr(transparent)]
pub struct AssetId(pub u32);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[repr(transparent)]
pub struct CurrencyId(pub u32);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct AccountHandle {
    pub account_id: AccountId,
    pub generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AccountInstrumentBinding {
    pub native_symbol: Arc<str>,
    pub asset_id: AssetId,
    pub price_tick: DecimalUnit,
    pub quantity_lot: DecimalUnit,
    pub contract_multiplier: DecimalUnit,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AccountCurrencyBinding {
    pub native_currency: Arc<str>,
    pub currency_id: CurrencyId,
    pub amount_unit: DecimalUnit,
}

#[derive(Clone, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[repr(transparent)]
pub struct SecretRef(pub Arc<str>);

impl fmt::Debug for SecretRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretRef(REDACTED)")
    }
}

impl SecretRef {
    pub fn new(value: impl Into<Arc<str>>) -> Self {
        Self(value.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum OrderOwnershipPolicy {
    ManagedOnly { client_id_prefix: Arc<str> },
    ObserveAll,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ShutdownOrderPolicy {
    LeaveOpen,
    CancelAll,
    CancelAllAfter { timeout_ms: u64 },
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct AccountDefinition {
    pub account_key: Arc<str>,
    pub account_id: AccountId,
    pub connector_type: Arc<str>,
    pub credential_ref: SecretRef,
    pub connector_config: Arc<[u8]>,
    pub instruments: Arc<[AccountInstrumentBinding]>,
    pub currencies: Arc<[AccountCurrencyBinding]>,
    pub ownership: OrderOwnershipPolicy,
    pub shutdown_order_policy: ShutdownOrderPolicy,
    pub enabled: bool,
    pub definition_version: u64,
}

impl fmt::Debug for AccountDefinition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AccountDefinition")
            .field("account_key", &self.account_key)
            .field("account_id", &self.account_id)
            .field("connector_type", &self.connector_type)
            .field("credential_ref", &self.credential_ref)
            .field("connector_config", &"REDACTED")
            .field("instruments", &self.instruments)
            .field("currencies", &self.currencies)
            .field("ownership", &self.ownership)
            .field("shutdown_order_policy", &self.shutdown_order_policy)
            .field("enabled", &self.enabled)
            .field("definition_version", &self.definition_version)
            .finish()
    }
}

pub struct SecretValue(Vec<u8>);

impl SecretValue {
    pub fn new(value: Vec<u8>) -> Self {
        Self(value)
    }
    pub fn expose(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for SecretValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretValue(REDACTED)")
    }
}

impl Drop for SecretValue {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

pub trait SecretProvider: Send + Sync + 'static {
    fn resolve(&self, reference: &SecretRef) -> Result<SecretValue, AccountConnectorError>;
}

pub struct UnavailableSecretProvider;
impl SecretProvider for UnavailableSecretProvider {
    fn resolve(&self, _: &SecretRef) -> Result<SecretValue, AccountConnectorError> {
        Err(AccountConnectorError::new(
            AccountErrorKind::CredentialUnavailable,
            "credential unavailable",
        ))
    }
}

#[derive(Clone)]
pub struct ScopedSecretResolver {
    allowed: SecretRef,
    provider: Arc<dyn SecretProvider>,
    active: Arc<AtomicBool>,
}

impl ScopedSecretResolver {
    pub(crate) fn new(
        allowed: SecretRef,
        provider: Arc<dyn SecretProvider>,
        active: Arc<AtomicBool>,
    ) -> Self {
        Self {
            allowed,
            provider,
            active,
        }
    }

    pub fn resolve(&self, reference: &SecretRef) -> Result<SecretValue, AccountConnectorError> {
        if !self.active.load(Ordering::Acquire) || reference != &self.allowed {
            return Err(AccountConnectorError::new(
                AccountErrorKind::CredentialUnavailable,
                "credential unavailable",
            ));
        }
        self.provider.resolve(reference)
    }

    /// Creates a resolver whose authority is limited to one explicit reference. This is used by
    /// an in-process dynamic connector adapter; dropping the resolver drops the provider and its
    /// foreign callback context.
    pub fn scoped(allowed: SecretRef, provider: Arc<dyn SecretProvider>) -> Self {
        Self {
            allowed,
            provider,
            active: Arc::new(AtomicBool::new(true)),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[repr(transparent)]
pub struct SourceStreamId(pub u32);

#[derive(Clone)]
pub struct AccountEventPublisher {
    inner: Option<EventPublisher>,
    dynamic_sink: Option<Arc<dyn AccountEventSink>>,
    account: AccountHandle,
    account_stream: SourceStreamId,
    control_stream: SourceStreamId,
    account_sequence: Arc<Mutex<u64>>,
    control_sequence: Arc<Mutex<u64>>,
    admission: Arc<AtomicBool>,
}

/// Plugin-side event backend for a dynamically loaded AccountConnector. The concrete
/// implementation translates the binary account payload to the host's fixed-layout C callback.
pub trait AccountEventSink: Send + Sync + 'static {
    fn publish(
        &self,
        event_type: &str,
        payload: &[u8],
        trace: TraceContext,
    ) -> Result<(), PluginError>;
}

impl AccountEventPublisher {
    pub(crate) fn new(
        inner: EventPublisher,
        account: AccountHandle,
        account_stream: SourceStreamId,
        control_stream: SourceStreamId,
        admission: Arc<AtomicBool>,
    ) -> Self {
        Self {
            inner: Some(inner),
            dynamic_sink: None,
            account,
            account_stream,
            control_stream,
            account_sequence: Arc::new(Mutex::new(0)),
            control_sequence: Arc::new(Mutex::new(0)),
            admission,
        }
    }

    pub fn from_sink(account: AccountHandle, sink: Arc<dyn AccountEventSink>) -> Self {
        Self {
            inner: None,
            dynamic_sink: Some(sink),
            account,
            account_stream: SourceStreamId(0),
            control_stream: SourceStreamId(0),
            account_sequence: Arc::new(Mutex::new(0)),
            control_sequence: Arc::new(Mutex::new(0)),
            admission: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn publish(
        &self,
        event_type: &str,
        payload: &[u8],
        trace: TraceContext,
    ) -> Result<(), PluginError> {
        if !self.admission.load(Ordering::Acquire) {
            return Err(publisher_error("account publisher admission is closed"));
        }
        if !ACCOUNT_EVENT_TYPES.contains(&event_type) {
            return Err(publisher_error("account event type is not authorized"));
        }
        let header =
            decode_account_event_header(payload).map_err(|message| publisher_error(message))?;
        self.validate_header(header)?;
        let (expected_kind, expected_len) = crate::account_event_layout(event_type)
            .ok_or_else(|| publisher_error("account event type is not authorized"))?;
        if header.kind != expected_kind || payload.len() != expected_len {
            return Err(publisher_error(
                "account event kind or payload length does not match its schema",
            ));
        }
        if let Some(sink) = &self.dynamic_sink {
            return sink.publish(event_type, payload, trace);
        }
        let control = crate::is_control_event(event_type);
        let sequence_lock = if control {
            &self.control_sequence
        } else {
            &self.account_sequence
        };
        let stream = if control {
            self.control_stream
        } else {
            self.account_stream
        };
        let mut committed = sequence_lock.lock().unwrap_or_else(|p| p.into_inner());
        let source_sequence = committed.saturating_add(1);
        let result = self
            .inner
            .as_ref()
            .expect("core account publisher is present")
            .publish_with_metadata(
                event_type,
                ACCOUNT_EVENT_SCHEMA_VERSION,
                payload,
                EventPublishMetadata {
                    source_id: stream.0,
                    source_sequence,
                    exchange_ts: header.exchange_ts,
                    receive_ts: header.receive_ts,
                    routing_key: u64::from(self.account.account_id.0),
                    ..EventPublishMetadata::default()
                },
                trace,
            );
        if result.is_ok() {
            *committed = source_sequence;
        }
        result
    }

    pub fn publish_encoded<T: crate::AccountEventPayload>(
        &self,
        event: &T,
        trace: TraceContext,
    ) -> Result<(), PluginError> {
        if !self.admission.load(Ordering::Acquire) {
            return Err(publisher_error("account publisher admission is closed"));
        }
        let header = event.header();
        self.validate_header(header)?;
        let (expected_kind, expected_len) =
            crate::account_event_layout_version(T::EVENT_TYPE, T::SCHEMA_VERSION)
                .ok_or_else(|| publisher_error("account event type is not authorized"))?;
        if header.kind != expected_kind || T::ENCODED_LEN != expected_len {
            return Err(publisher_error(
                "account event kind or payload length does not match its schema",
            ));
        }
        if let Some(sink) = &self.dynamic_sink {
            let mut payload = vec![0_u8; T::ENCODED_LEN];
            event.encode_into(&mut payload).map_err(publisher_error)?;
            return sink.publish(T::EVENT_TYPE, &payload, trace);
        }
        let control = crate::is_control_event(T::EVENT_TYPE);
        let sequence_lock = if control {
            &self.control_sequence
        } else {
            &self.account_sequence
        };
        let stream = if control {
            self.control_stream
        } else {
            self.account_stream
        };
        let mut committed = sequence_lock.lock().unwrap_or_else(|p| p.into_inner());
        let source_sequence = committed.saturating_add(1);
        let mut reservation = self
            .inner
            .as_ref()
            .expect("core account publisher is present")
            .reserve_event_payload(
                T::EVENT_TYPE,
                T::SCHEMA_VERSION,
                T::ENCODED_LEN,
                EventPublishMetadata {
                    source_id: stream.0,
                    source_sequence,
                    exchange_ts: header.exchange_ts,
                    receive_ts: header.receive_ts,
                    routing_key: u64::from(self.account.account_id.0),
                    ..EventPublishMetadata::default()
                },
                trace,
            )?;
        event
            .encode_into(reservation.payload_mut())
            .map_err(publisher_error)?;
        reservation.commit()?;
        *committed = source_sequence;
        Ok(())
    }

    fn validate_header(&self, header: AccountEventHeaderV1) -> Result<(), PluginError> {
        if header.account_id != self.account.account_id.0
            || header.account_generation != self.account.generation
        {
            return Err(publisher_error(
                "account event header does not match scoped account",
            ));
        }
        Ok(())
    }

    pub fn close(&self) {
        self.admission.store(false, Ordering::Release);
    }
    pub fn is_open(&self) -> bool {
        self.admission.load(Ordering::Acquire)
    }
}

fn publisher_error(message: impl Into<Arc<str>>) -> PluginError {
    PluginError::new(
        titan_plugin_engine::ErrorKind::SubscriptionRejected,
        titan_plugin_engine::PluginIdentity::new("titan.account", "publisher"),
        titan_plugin_engine::LifecycleState::Running,
        "publish_account_event",
        message,
    )
}

#[derive(Clone)]
pub struct AccountConnectorContext {
    pub account: AccountHandle,
    pub instruments: Arc<[AccountInstrumentBinding]>,
    pub currencies: Arc<[AccountCurrencyBinding]>,
    pub ownership: OrderOwnershipPolicy,
    pub account_stream: SourceStreamId,
    pub control_stream: SourceStreamId,
    pub event_publisher: AccountEventPublisher,
    pub resources: ResourceScopeHandle,
    pub secrets: ScopedSecretResolver,
    pub command_queue_capacity: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[repr(transparent)]
pub struct Id128(pub [u8; 16]);
pub type CommandId = Id128;
pub type ClientOrderId = Id128;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[repr(transparent)]
pub struct OperationId(pub u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum OperationState {
    Pending,
    Succeeded,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AccountConnectorOperationSnapshot {
    pub id: OperationId,
    pub state: OperationState,
    pub detail: Arc<str>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AccountOperationSnapshot {
    pub id: OperationId,
    pub state: OperationState,
    pub detail: Arc<str>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum AccountLifecycle {
    Created,
    Starting,
    Connecting,
    Reconciling,
    Ready,
    Degraded,
    Invalidated,
    Stopping,
    Stopped,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AccountInstanceSnapshot {
    pub handle: AccountHandle,
    pub account_key: Arc<str>,
    pub connector_type: Arc<str>,
    pub definition_version: u64,
    pub enabled: bool,
    pub lifecycle: AccountLifecycle,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AccountConnectorHealthSnapshot {
    pub state: AccountLifecycle,
    pub message: Arc<str>,
    pub observed_at: SystemTime,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AccountConnectorDiagnosticSnapshot {
    pub summary: Arc<str>,
    pub external_order_count: u64,
    pub command_queue_depth: usize,
    pub account_epoch: u64,
    pub account_version: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum AccountSnapshotState {
    Ready,
    Reconciling,
    Invalidated,
    Stopped,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AccountStateSnapshot<T> {
    pub account: AccountHandle,
    pub state: AccountSnapshotState,
    pub committed_epoch: Option<u64>,
    pub committed_version: Option<u64>,
    pub captured_at: i64,
    pub items: Arc<[T]>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct OrderFilter {
    pub asset_id: Option<AssetId>,
    pub include_final: bool,
}
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PositionFilter {
    pub asset_id: Option<AssetId>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OrderSnapshot {
    pub asset_id: AssetId,
    pub side: u8,
    pub order_type: u8,
    pub time_in_force: u8,
    pub status: u8,
    pub price_ticks: i64,
    pub quantity_lots: i64,
    pub filled_quantity_lots: i64,
    pub client_order_id: Id128,
    pub venue_order_id: Id128,
    pub command_id: Id128,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PositionSnapshot {
    pub asset_id: AssetId,
    pub position_side: u8,
    pub margin_type: u8,
    pub quantity_lots: i64,
    pub entry_price_ticks: i64,
    pub liquidation_price_ticks: i64,
    pub realized_pnl_units: i64,
    pub unrealized_pnl_units: i64,
    pub margin_currency_id: CurrencyId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BalanceSnapshot {
    pub currency_id: CurrencyId,
    pub wallet_units: i64,
    pub available_units: i64,
    pub margin_units: i64,
    pub unrealized_pnl_units: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ReconcileScope {
    Full,
    Orders,
    Positions,
    Balances,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SubmitOrderCommand {
    pub command_id: CommandId,
    pub client_order_id: Option<ClientOrderId>,
    pub asset_id: AssetId,
    pub side: u8,
    pub order_type: u8,
    pub time_in_force: u8,
    pub price_ticks: i64,
    pub quantity_lots: i64,
    pub trace: TraceContext,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AmendOrderCommand {
    pub command_id: CommandId,
    pub asset_id: AssetId,
    pub client_order_id: Option<ClientOrderId>,
    pub venue_order_id: Option<Id128>,
    pub price_ticks: Option<i64>,
    pub quantity_lots: Option<i64>,
    pub trace: TraceContext,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CancelOrderCommand {
    pub command_id: CommandId,
    pub asset_id: AssetId,
    pub client_order_id: Option<ClientOrderId>,
    pub venue_order_id: Option<Id128>,
    pub trace: TraceContext,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CancelAllCommand {
    pub command_id: CommandId,
    pub asset_id: Option<AssetId>,
    pub trace: TraceContext,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CancelAllAfterCommand {
    pub command_id: CommandId,
    pub timeout_ms: u64,
    pub trace: TraceContext,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AccountCommandReceipt {
    pub account: AccountHandle,
    pub command_id: CommandId,
    pub client_order_id: Option<ClientOrderId>,
    pub accepted_at: i64,
}

pub trait AccountConnectorFactory: Send + Sync + 'static {
    fn connector_type(&self) -> &str;
    fn create(
        &self,
        definition: &AccountDefinition,
        context: AccountConnectorContext,
    ) -> Result<Arc<dyn AccountConnector>, AccountConnectorError>;
}

pub trait AccountConnector: Send + Sync + 'static {
    fn start(&self) -> Result<(), AccountConnectorError>;
    fn stop(&self, deadline: Instant) -> Result<(), AccountConnectorError>;
    fn submit(
        &self,
        command: SubmitOrderCommand,
    ) -> Result<AccountCommandReceipt, AccountConnectorError>;
    fn amend(
        &self,
        command: AmendOrderCommand,
    ) -> Result<AccountCommandReceipt, AccountConnectorError>;
    fn cancel(
        &self,
        command: CancelOrderCommand,
    ) -> Result<AccountCommandReceipt, AccountConnectorError>;
    fn cancel_all(
        &self,
        command: CancelAllCommand,
    ) -> Result<AccountCommandReceipt, AccountConnectorError>;
    fn cancel_all_after(
        &self,
        command: CancelAllAfterCommand,
    ) -> Result<AccountCommandReceipt, AccountConnectorError>;
    fn reconcile(&self, scope: ReconcileScope) -> Result<OperationId, AccountConnectorError>;
    fn orders(
        &self,
        filter: OrderFilter,
    ) -> Result<AccountStateSnapshot<OrderSnapshot>, AccountConnectorError>;
    fn positions(
        &self,
        filter: PositionFilter,
    ) -> Result<AccountStateSnapshot<PositionSnapshot>, AccountConnectorError>;
    fn balances(&self) -> Result<AccountStateSnapshot<BalanceSnapshot>, AccountConnectorError>;
    fn health(&self) -> AccountConnectorHealthSnapshot;
    fn diagnostics(&self) -> AccountConnectorDiagnosticSnapshot;
    fn operation(&self, id: OperationId) -> AccountConnectorOperationSnapshot;
}
