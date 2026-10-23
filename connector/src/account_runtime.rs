//! AccountService adapters for the existing venue connectors.
//!
//! The adapter runs each authenticated private stream on its own Tokio runtime and translates
//! venue facts directly into the stable account ABI. REST execution is driven independently by
//! the process-wide execution runtime.

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::JoinHandle,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use hftbacktest::types::{OrdType, Order, Side, Status, TimeInForce};
use titan_account_service as account;
use titan_account_service::SecretValue;
use titan_core_types::{ClosureResource, TraceContext};
use tokio::sync::{Notify, oneshot, watch};

use crate::{
    account_projection::AccountProjection,
    api::{
        AccountInfo, ApiMarginType, ApiOrderStatus, ApiOrderType, ApiPositionSide, ApiSide,
        ApiTimeInForce, Balance, BrokerApi, CancelOrderRequest, OrderInfo, PositionInfo,
        UnifiedOrderRequest,
    },
    connector::{
        AccountPublication, Connector, ConnectorBuilder, DirectPublication, PublishEvent,
        direct_publish_sender,
    },
};

const ACCOUNT_SIDE_BUY: u8 = 1;
const ACCOUNT_SIDE_SELL: u8 = 2;
const ACCOUNT_POSITION_SIDE_NET: u8 = 0;
const ACCOUNT_POSITION_SIDE_LONG: u8 = 1;
const ACCOUNT_POSITION_SIDE_SHORT: u8 = 2;
const ACCOUNT_BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(30);
const ACCOUNT_PENDING_CAPACITY: usize = 8192;
#[derive(Default)]
struct ReadinessFence {
    current: AtomicU64,
    committed: AtomicU64,
}
impl ReadinessFence {
    fn changed(&self) -> u64 {
        self.current.fetch_add(1, Ordering::AcqRel) + 1
    }
    fn commit(&self, revision: u64) -> bool {
        self.committed.store(revision, Ordering::Release);
        self.matches()
    }
    fn matches(&self) -> bool {
        self.current.load(Ordering::Acquire) == self.committed.load(Ordering::Acquire)
    }
}
struct SubmitSession {
    ready: Arc<AtomicBool>,
    active: Arc<AtomicBool>,
    committed: Arc<AtomicBool>,
    fence: Arc<ReadinessFence>,
    epoch: Arc<AtomicU64>,
    publisher: account::AccountEventPublisher,
}
impl SubmitSession {
    fn capture(self) -> crate::api::SubmitGuard {
        let revision = self.fence.current.load(Ordering::Acquire);
        let epoch = self.epoch.load(Ordering::Acquire);
        Arc::new(move || {
            if !self.ready.load(Ordering::Acquire)
                || !self.active.load(Ordering::Acquire)
                || !self.committed.load(Ordering::Acquire)
                || !self.publisher.is_open()
                || !self.fence.matches()
                || self.fence.current.load(Ordering::Acquire) != revision
                || self.epoch.load(Ordering::Acquire) != epoch
            {
                return Err(crate::api::ApiError::new(
                    "runtime",
                    "STALE_ACCOUNT_SESSION",
                    "account session changed before order transport admission",
                ));
            }
            Ok(())
        })
    }
}
struct PendingAccountFacts {
    values: Vec<AccountPublication>,
    capacity: usize,
}
impl PendingAccountFacts {
    fn new(capacity: usize) -> Self {
        Self {
            values: Vec::new(),
            capacity,
        }
    }
    fn push(&mut self, event: AccountPublication) -> bool {
        if self.values.len() >= self.capacity {
            return false;
        }
        self.values.push(event);
        true
    }
    fn clear(&mut self) {
        self.values.clear();
    }
    fn drain(&mut self) -> std::vec::Drain<'_, AccountPublication> {
        self.values.drain(..)
    }
}
const ACCOUNT_BALANCE_REFRESH_INTERVAL: Duration = Duration::from_secs(2);

struct Running {
    stop: Option<oneshot::Sender<Instant>>,
    thread: Option<JoinHandle<()>>,
    shutdown_result: Arc<Mutex<Option<Result<(), account::AccountConnectorError>>>>,
    requested_deadline: Option<Instant>,
}

#[async_trait::async_trait(?Send)]
trait ShutdownActions: Send + Sync {
    async fn cancel_all(&self) -> Result<(), String>;
    async fn cancel_all_after(&self, timeout_ms: u64) -> Result<(), String>;
}

#[async_trait::async_trait]
trait AccountBootstrapApi: Send + Sync {
    async fn validate(&self) -> Result<(), crate::api::ApiError>;
    async fn open_orders(&self, symbol: &str) -> Result<Vec<OrderInfo>, crate::api::ApiError>;
    async fn positions(&self) -> Result<Vec<PositionInfo>, crate::api::ApiError>;
    async fn position_mode(&self) -> Result<crate::api::ApiPositionMode, crate::api::ApiError>;
    async fn instrument_position_mode(
        &self,
        _symbol: &str,
    ) -> Result<crate::api::ApiPositionMode, crate::api::ApiError> {
        self.position_mode().await
    }
    fn position_currency(&self, _symbol: &str) -> Option<String> {
        None
    }
    async fn account(&self) -> Result<AccountInfo, crate::api::ApiError>;
    async fn order(
        &self,
        symbol: &str,
        client_order_id: &str,
    ) -> Result<OrderInfo, crate::api::ApiError>;
}

#[async_trait::async_trait]
impl<T: BrokerApi + ?Sized> AccountBootstrapApi for T {
    fn position_currency(&self, symbol: &str) -> Option<String> {
        BrokerApi::position_currency(self, symbol)
    }
    async fn position_mode(&self) -> Result<crate::api::ApiPositionMode, crate::api::ApiError> {
        self.get_account_position_mode().await
    }
    async fn instrument_position_mode(
        &self,
        symbol: &str,
    ) -> Result<crate::api::ApiPositionMode, crate::api::ApiError> {
        self.get_instrument_position_mode(symbol).await
    }
    async fn validate(&self) -> Result<(), crate::api::ApiError> {
        self.validate_account_configuration().await
    }

    async fn open_orders(&self, symbol: &str) -> Result<Vec<OrderInfo>, crate::api::ApiError> {
        self.get_open_orders(symbol).await
    }

    async fn positions(&self) -> Result<Vec<PositionInfo>, crate::api::ApiError> {
        self.get_positions(None).await
    }

    async fn account(&self) -> Result<AccountInfo, crate::api::ApiError> {
        self.get_account().await
    }
    async fn order(
        &self,
        symbol: &str,
        client_order_id: &str,
    ) -> Result<OrderInfo, crate::api::ApiError> {
        self.get_order(symbol, None, Some(client_order_id)).await
    }
}

struct VenueShutdownActions {
    connector: Arc<Mutex<Box<dyn Connector>>>,
    api: Arc<dyn BrokerApi>,
}

#[async_trait::async_trait(?Send)]
impl ShutdownActions for VenueShutdownActions {
    async fn cancel_all(&self) -> Result<(), String> {
        crate::connector::shutdown_connector(&self.connector).await
    }

    async fn cancel_all_after(&self, timeout_ms: u64) -> Result<(), String> {
        self.api
            .cancel_all_after(timeout_ms)
            .await
            .map_err(|error| error.to_string())
    }
}

async fn execute_shutdown_policy(
    actions: &impl ShutdownActions,
    policy: &account::ShutdownOrderPolicy,
    deadline: Instant,
    committed: bool,
) -> Result<(), account::AccountConnectorError> {
    if !committed {
        return Ok(());
    }
    match policy {
        account::ShutdownOrderPolicy::LeaveOpen => Ok(()),
        account::ShutdownOrderPolicy::CancelAll => {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match tokio::time::timeout(remaining, actions.cancel_all()).await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => Err(account::AccountConnectorError::rejected(error)),
                Err(_) => Err(account::AccountConnectorError::new(
                    account::AccountErrorKind::DeadlineExceeded,
                    "account connector shutdown deadline exceeded",
                )),
            }
        }
        account::ShutdownOrderPolicy::CancelAllAfter { timeout_ms } => {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match tokio::time::timeout(remaining, actions.cancel_all_after(*timeout_ms)).await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => Err(account::AccountConnectorError::rejected(error)),
                Err(_) => Err(account::AccountConnectorError::new(
                    account::AccountErrorKind::DeadlineExceeded,
                    "cancel-all-after shutdown deadline exceeded",
                )),
            }
        }
    }
}

#[derive(Default)]
struct IdInterner {
    by_text: HashMap<String, account::Id128>,
    by_id: HashMap<account::Id128, String>,
    next: u128,
    next_prune: usize,
}
impl IdInterner {
    fn intern(&mut self, text: &str) -> account::Id128 {
        if let Some(id) = self.by_text.get(text) {
            return *id;
        }
        let bytes = text.as_bytes();
        let id = if let Some(id) = parse_hex_id(text) {
            id
        } else if bytes.len() <= 15 {
            let mut out = [0; 16];
            out[0] = bytes.len() as u8;
            out[1..1 + bytes.len()].copy_from_slice(bytes);
            account::Id128(out)
        } else {
            self.next = self.next.saturating_add(1);
            account::Id128((u128::MAX - (self.next - 1)).to_le_bytes())
        };
        self.by_text.insert(text.to_owned(), id);
        self.by_id.insert(id, text.to_owned());
        id
    }

    fn resolve(&self, id: account::Id128) -> String {
        self.by_id.get(&id).cloned().unwrap_or_else(|| id_text(id))
    }

    fn contains_text(&self, text: &str) -> bool {
        self.by_text.contains_key(text)
    }
    fn prune(&mut self, image: &AccountProjection) {
        if self.by_id.len() < self.next_prune.max(4096) {
            return;
        }
        let retained = image.retained_ids();
        self.by_id.retain(|id, _| retained.contains(id));
        self.by_text.retain(|_, id| retained.contains(id));
        self.next_prune = self.by_id.len().saturating_mul(2).max(4096);
    }
}

struct AccountRuntime {
    context: account::AccountConnectorContext,
    shutdown_policy: account::ShutdownOrderPolicy,
    connector: Arc<Mutex<Box<dyn Connector>>>,
    api: Arc<dyn BrokerApi>,
    running: Mutex<Option<Running>>,
    started: AtomicBool,
    active: Arc<AtomicBool>,
    ready: Arc<AtomicBool>,
    readiness_fence: Arc<ReadinessFence>,
    committed: Arc<AtomicBool>,
    epoch: Arc<AtomicU64>,
    version: Arc<AtomicU64>,
    projection: Arc<Mutex<AccountProjection>>,
    encoder: Arc<Mutex<Option<Arc<AccountEventEncoder>>>>,
    startup_error: Arc<Mutex<Option<String>>>,
    ids: Arc<Mutex<IdInterner>>,
}

impl AccountRuntime {
    fn new(
        connector: Box<dyn Connector>,
        api: Arc<dyn BrokerApi>,
        shutdown_policy: account::ShutdownOrderPolicy,
        context: account::AccountConnectorContext,
    ) -> Arc<Self> {
        let committed = context.event_publisher.is_open();
        let value = Arc::new(Self {
            context,
            shutdown_policy,
            connector: Arc::new(Mutex::new(connector)),
            api,
            running: Mutex::new(None),
            started: AtomicBool::new(false),
            active: Arc::new(AtomicBool::new(false)),
            ready: Arc::new(AtomicBool::new(false)),
            readiness_fence: Arc::new(ReadinessFence::default()),
            committed: Arc::new(AtomicBool::new(committed)),
            epoch: Arc::new(AtomicU64::new(0)),
            version: Arc::new(AtomicU64::new(0)),
            projection: Arc::new(Mutex::new(AccountProjection::default())),
            encoder: Arc::new(Mutex::new(None)),
            startup_error: Arc::new(Mutex::new(None)),
            ids: Arc::new(Mutex::new(IdInterner::default())),
        });
        let weak = Arc::downgrade(&value);
        value
            .context
            .resources
            .register(
                "account-connector-runtime",
                ClosureResource(Some(move || close_weak(&weak))),
            )
            .expect("new account resource scope accepts its runtime");
        value
    }
    fn snapshot<T>(
        &self,
        epoch: u64,
        version: u64,
        items: Arc<[T]>,
    ) -> account::AccountStateSnapshot<T> {
        account::AccountStateSnapshot {
            account: self.context.account,
            state: if self.ready.load(Ordering::Acquire) && self.readiness_fence.matches() {
                account::AccountSnapshotState::Ready
            } else if self.active.load(Ordering::Acquire)
                || self
                    .startup_error
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .is_some()
            {
                account::AccountSnapshotState::Invalidated
            } else {
                account::AccountSnapshotState::Stopped
            },
            committed_epoch: (epoch > 0).then_some(epoch),
            committed_version: (epoch > 0).then_some(version),
            captured_at: now_ns(),
            items,
        }
    }
}

fn close_weak(weak: &Weak<AccountRuntime>) -> Result<(), titan_core_types::CoreError> {
    if let Some(runtime) = weak.upgrade() {
        let deadline = runtime
            .running
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .and_then(|running| running.requested_deadline)
            .unwrap_or_else(|| Instant::now() + Duration::from_secs(1));
        runtime.stop_inner(deadline).map_err(|error| {
            titan_core_types::CoreError::new(
                titan_core_types::ErrorKind::ResourceReleaseFailed,
                titan_core_types::ComponentIdentity::new("titan.account", "connector-runtime"),
                titan_core_types::ComponentState::Stopping,
                "close_account_runtime",
                error.to_string(),
            )
        })?;
    }
    Ok(())
}

impl account::AccountConnector for AccountRuntime {
    fn ledger_page(
        &self,
        from_ms: i64,
        to_ms: i64,
        page: u32,
        limit: u32,
    ) -> account::AccountLedgerFuture {
        let api = self.api.clone();
        Box::pin(async move {
            api.account_ledger_page(from_ms, to_ms, page, limit)
                .await
                .map_err(|e| {
                    account::AccountConnectorError::rejected(format!("ledger provider: {}", e.code))
                })
        })
    }
    fn start(&self) -> Result<(), account::AccountConnectorError> {
        crate::ensure_rustls_crypto_provider();
        if self.started.swap(true, Ordering::AcqRel) {
            return Err(rejected(
                "account connector cannot restart after it has started",
            ));
        }
        self.active.store(true, Ordering::Release);
        {
            let mut connector = self.connector.lock().unwrap_or_else(|p| p.into_inner());
            for b in self.context.instruments.iter() {
                connector.register_account(b.native_symbol.to_string());
            }
        }
        let runtime = match tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(error) => {
                self.started.store(false, Ordering::Release);
                self.active.store(false, Ordering::Release);
                return Err(rejected(format!(
                    "cannot create account async runtime: {error}"
                )));
            }
        };
        let api = self.api.clone();
        let context = self.context.clone();
        let epoch = self.epoch.clone();
        let version = self.version.clone();
        let ready = self.ready.clone();
        let fence = self.readiness_fence.clone();
        let committed = self.committed.clone();
        let projection = self.projection.clone();
        let encoder_slot = self.encoder.clone();
        let startup_error = self.startup_error.clone();
        let ids = self.ids.clone();
        let shutdown_policy = self.shutdown_policy.clone();
        let connector = self.connector.clone();
        let (stop_tx, mut stop_rx) = oneshot::channel::<Instant>();
        let shutdown_result = Arc::new(Mutex::new(None));
        let thread_shutdown_result = shutdown_result.clone();
        let thread = std::thread::Builder::new()
            .name(format!("account-{}", context.account.account_id.0))
            .spawn(move || {
                runtime.block_on(async move {
                    ready.store(false, Ordering::Release);
                    epoch.fetch_add(1, Ordering::AcqRel);
                    version.store(0, Ordering::Release);
                    let encoder = Arc::new(AccountEventEncoder {
                        context: context.clone(),
                        epoch: epoch.clone(),
                        version: version.clone(),
                        ready: ready.clone(),
                        ids: ids.clone(),
                        projection: projection.clone(),
                        publication: Mutex::new(()),
                    });
                    *encoder_slot.lock().unwrap_or_else(|p| p.into_inner()) = Some(encoder.clone());
                    let (connection_tx, mut connection_rx) = watch::channel((0_u64, false));
                    let stream_ready = Arc::new(AtomicBool::new(false));
                    let stream_ready_notify = Arc::new(Notify::new());
                    let bootstrapped = Arc::new(AtomicBool::new(false));
                    let pending = Arc::new(Mutex::new(PendingAccountFacts::new(ACCOUNT_PENDING_CAPACITY)));
                    let account_events = encoder.clone();
                    let callback_context = context.clone();
                    let callback_ready = stream_ready.clone();
                    let callback_notify = stream_ready_notify.clone();
                    let callback_bootstrapped = bootstrapped.clone();
                    let callback_pending = pending.clone();
                    let overflowed = Arc::new(AtomicBool::new(false));
                    let callback_overflowed = overflowed.clone();
                    let callback_fence = fence.clone();
                    let tx = direct_publish_sender(move |publication| match publication {
                        DirectPublication::Event(PublishEvent::PrivateStreamReady) => {
                            let revision = callback_fence.changed();
                            callback_bootstrapped.store(false, Ordering::Release);
                            account_events.ready.store(false, Ordering::Release);
                            callback_ready.store(true, Ordering::Release);
                            if revision > 1 { account_events.invalidate(account::invalidation_reason::PRIVATE_STREAM); }
                            callback_notify.notify_waiters();
                            connection_tx.send_modify(|value| { *value = (revision, true); });
                        }
                        DirectPublication::Account(AccountPublication::Error(error)) => {
                            let revision = callback_fence.changed();
                            callback_ready.store(false, Ordering::Release);
                            callback_bootstrapped.store(false, Ordering::Release);
                            connection_tx.send_modify(|value| { *value = (revision, false); });
                            tracing::warn!(
                                account_id = callback_context.account.account_id.0,
                                ?error,
                                "Account private stream reported an error."
                            );
                            account_events.invalidate(account::invalidation_reason::PRIVATE_STREAM);
                        }
                        DirectPublication::Account(event) => {
                            if !callback_bootstrapped.load(Ordering::Acquire) {
                                let mut pending = callback_pending
                                    .lock()
                                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                                if !callback_bootstrapped.load(Ordering::Acquire) {
                                    if !pending.push(event.clone()) {
                                        callback_overflowed.store(true, Ordering::Release);
                                        account_events.invalidate(account::invalidation_reason::BOOTSTRAP);
                                    }
                                    return;
                                }
                            }
                            if let Err(error) = account_events.publish(event) {
                                let revision = callback_fence.changed();
                                callback_bootstrapped.store(false, Ordering::Release);
                                connection_tx.send_modify(|value| { value.0 = revision; });
                                tracing::warn!(
                                    account_id = callback_context.account.account_id.0,
                                    ?error,
                                    "Account fact publication failed."
                                );
                                eprintln!(
                                    "account {} fact publication failed: {error}",
                                    callback_context.account.account_id.0
                                );
                                account_events.invalidate(
                                    account::invalidation_reason::DIRECT_FACT_PUBLICATION,
                                );
                            }
                        }
                        _ => {}
                    });
                    connector
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .run_account(tx);
                    let mut first_recovery = true;
                    let mut balance_tick = tokio::time::interval(ACCOUNT_BALANCE_REFRESH_INTERVAL);
                    balance_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    let stop = 'session: loop {
                        ready.store(false, Ordering::Release);
                        bootstrapped.store(false, Ordering::Release);
                        if !first_recovery {
                            epoch.fetch_add(1, Ordering::AcqRel);
                        }
                        first_recovery = false;
                        if overflowed.swap(false, Ordering::AcqRel) {
                            pending.lock().unwrap_or_else(|p| p.into_inner()).clear();
                        }
                        let recovery = async {
                            wait_for_private_stream(stream_ready.as_ref(), stream_ready_notify.as_ref(), ACCOUNT_BOOTSTRAP_TIMEOUT).await?;
                            let revision = *connection_rx.borrow_and_update();
                            let known = projection.lock().unwrap_or_else(|p| p.into_inner())
                                .orders(account::OrderFilter { asset_id: None, include_final: false }).1;
                            let snapshot = reconcile_account_bootstrap(&context, api.as_ref(), ids.as_ref(), &known).await?;
                            let mut buffered = pending.lock().unwrap_or_else(|p| p.into_inner());
                            if overflowed.load(Ordering::Acquire) || !stream_ready.load(Ordering::Acquire)
                                || *connection_rx.borrow() != revision {
                                return Err(rejected("account stream changed or overflowed during reconciliation"));
                            }
                            seed_managed_orders(&connector, &context, &snapshot, &ids)?;
                            encoder.install_bootstrap(snapshot)?;
                            for event in buffered.drain() {
                                encoder.publish(&event)?;
                            }
                            if !stream_ready.load(Ordering::Acquire) || *connection_rx.borrow() != revision || !fence.commit(revision.0) {
                                return Err(rejected("account connection changed before READY commit"));
                            }
                            encoder.publish_ready()?;
                            if !fence.matches() || !stream_ready.load(Ordering::Acquire) {
                                encoder.invalidate(account::invalidation_reason::BOOTSTRAP);
                                return Err(rejected("account connection changed during READY publication"));
                            }
                            bootstrapped.store(true, Ordering::Release);
                            Ok::<(), account::AccountConnectorError>(())
                        };
                        let outcome = tokio::select! {
                            stop = &mut stop_rx => break 'session stop,
                            result = tokio::time::timeout(ACCOUNT_BOOTSTRAP_TIMEOUT, recovery) => result
                                .unwrap_or_else(|_| Err(rejected("account reconciliation deadline exceeded"))),
                        };
                        match outcome {
                            Err(error) => {
                                *startup_error.lock().unwrap_or_else(|p| p.into_inner()) = Some(error.to_string());
                                encoder.invalidate(account::invalidation_reason::BOOTSTRAP);
                                tokio::select! {
                                    stop = &mut stop_rx => break 'session stop,
                                    _ = connection_rx.changed() => {},
                                    _ = tokio::time::sleep(Duration::from_secs(1)) => {},
                                }
                                continue;
                            }
                            Ok(()) => { *startup_error.lock().unwrap_or_else(|p| p.into_inner()) = None; }
                        }
                        loop {
                            tokio::select! {
                                stop = &mut stop_rx => break 'session stop,
                                changed = connection_rx.changed() => {
                                    if changed.is_err() { ready.store(false, Ordering::Release); }
                                    break;
                                },
                                _ = balance_tick.tick() => {
                                    let refreshed = tokio::select! {
                                        stop = &mut stop_rx => break 'session stop,
                                        value = tokio::time::timeout(ACCOUNT_BOOTSTRAP_TIMEOUT, api.get_account()) => value,
                                    };
                                    let result = refreshed.map_err(|_| rejected("balance refresh deadline exceeded"))
                                        .and_then(|value| value.map_err(|error| rejected(error.to_string())))
                                        .and_then(|value| encoder.refresh_balances(&value));
                                    if let Err(error) = result {
                                        *startup_error.lock().unwrap_or_else(|p| p.into_inner()) = Some(error.to_string());
                                        encoder.invalidate(account::invalidation_reason::BOOTSTRAP);
                                        break;
                                    }
                                }
                            }
                        }
                    };
                    let result = match stop {
                        Err(_) => Err(account::AccountConnectorError::new(
                            account::AccountErrorKind::ResourceReleaseFailed,
                            "account stop signal was dropped",
                        )),
                        Ok(deadline) => {
                            execute_shutdown_policy(
                                &VenueShutdownActions { connector: connector.clone(), api: api.clone() },
                                &shutdown_policy,
                                deadline,
                                committed.load(Ordering::Acquire),
                            ).await
                        }
                    };
                    *thread_shutdown_result
                        .lock()
                        .unwrap_or_else(|p| p.into_inner()) = Some(result);
                });
            })
            .map_err(|error| {
                self.started.store(false, Ordering::Release);
                self.active.store(false, Ordering::Release);
                rejected(error.to_string())
            })?;
        *self.running.lock().unwrap_or_else(|p| p.into_inner()) = Some(Running {
            stop: Some(stop_tx),
            thread: Some(thread),
            shutdown_result,
            requested_deadline: None,
        });
        Ok(())
    }
    fn activate(&self) -> Result<(), account::AccountConnectorError> {
        if !self.ready.load(Ordering::Acquire) || !self.readiness_fence.matches() {
            return Err(rejected("candidate connection changed after preparation"));
        }
        let encoder = self
            .encoder
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
            .ok_or_else(|| rejected("candidate account encoder is not prepared"))?;
        encoder.activate()?;
        if !self.readiness_fence.matches() {
            return Err(rejected("candidate session changed during activation"));
        }
        self.committed.store(true, Ordering::Release);
        Ok(())
    }

    fn stop(&self, deadline: Instant) -> Result<(), account::AccountConnectorError> {
        self.stop_inner(deadline)
    }
    fn orders(
        &self,
        filter: account::OrderFilter,
    ) -> Result<account::AccountStateSnapshot<account::OrderSnapshot>, account::AccountConnectorError>
    {
        let image = self.projection.lock().unwrap_or_else(|p| p.into_inner());
        let (version, items) = image.orders(filter);
        Ok(self.snapshot(image.epoch, version, items))
    }
    fn positions(
        &self,
        filter: account::PositionFilter,
    ) -> Result<
        account::AccountStateSnapshot<account::PositionSnapshot>,
        account::AccountConnectorError,
    > {
        let image = self.projection.lock().unwrap_or_else(|p| p.into_inner());
        let (version, items) = image.positions(filter);
        Ok(self.snapshot(image.epoch, version, items))
    }
    fn balances(
        &self,
    ) -> Result<
        account::AccountStateSnapshot<account::BalanceSnapshot>,
        account::AccountConnectorError,
    > {
        let image = self.projection.lock().unwrap_or_else(|p| p.into_inner());
        let (version, items) = image.balances();
        Ok(self.snapshot(image.epoch, version, items))
    }
    fn recovery_snapshot(
        &self,
    ) -> Result<account::AccountRecoverySnapshot, account::AccountConnectorError> {
        let image = self.projection.lock().unwrap_or_else(|p| p.into_inner());
        let epoch = image.epoch;
        let version = image.committed_version;
        account::AccountRecoverySnapshot::coherent(
            self.snapshot(
                epoch,
                version,
                image
                    .orders(account::OrderFilter {
                        asset_id: None,
                        include_final: true,
                    })
                    .1,
            ),
            self.snapshot(
                epoch,
                version,
                image.positions(account::PositionFilter::default()).1,
            ),
            self.snapshot(epoch, version, image.balances().1),
        )
    }
    fn health(&self) -> account::AccountConnectorHealthSnapshot {
        let ready = self.ready.load(Ordering::Acquire) && self.readiness_fence.matches();
        let startup_error = self
            .startup_error
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let message = if ready {
            Arc::from("private stream active")
        } else {
            startup_error
                .as_deref()
                .map(|error| Arc::from(format!("account bootstrap failed: {error}")))
                .unwrap_or_else(|| Arc::from("account connector not ready"))
        };
        account::AccountConnectorHealthSnapshot {
            state: if ready {
                account::AccountLifecycle::Ready
            } else if startup_error.is_some() {
                account::AccountLifecycle::Failed
            } else if self.active.load(Ordering::Acquire) {
                account::AccountLifecycle::Connecting
            } else {
                account::AccountLifecycle::Stopped
            },
            message,
            observed_at: SystemTime::now(),
        }
    }
    fn diagnostics(&self) -> account::AccountConnectorDiagnosticSnapshot {
        account::AccountConnectorDiagnosticSnapshot {
            summary: Arc::from("venue account adapter"),
            account_epoch: self.epoch.load(Ordering::Acquire),
            account_version: self.version.load(Ordering::Acquire),
        }
    }
    fn operation(&self, id: account::OperationId) -> account::AccountConnectorOperationSnapshot {
        account::AccountConnectorOperationSnapshot {
            id,
            state: account::OperationState::Failed,
            detail: Arc::from("operation not found"),
        }
    }

    fn direct_submit(&self, request: account::DirectNewOrderRequest) -> account::ExecutionFuture {
        if !self.committed.load(Ordering::Acquire)
            || !self.ready.load(Ordering::Acquire)
            || !self.readiness_fence.matches()
            || !self.active.load(Ordering::Acquire)
            || !self.context.event_publisher.is_open()
        {
            return Box::pin(async {
                Err(direct_rejected(
                    "ACCOUNT_NOT_READY",
                    "account reconciliation or stop has closed order admission",
                ))
            });
        }
        let guard = SubmitSession {
            ready: self.ready.clone(),
            active: self.active.clone(),
            committed: self.committed.clone(),
            fence: self.readiness_fence.clone(),
            epoch: self.epoch.clone(),
            publisher: self.context.event_publisher.clone(),
        }
        .capture();
        let api = self.api.clone();
        let connector = self.connector.clone();
        let binding = self
            .context
            .instruments
            .iter()
            .find(|binding| binding.asset_id == request.asset_id)
            .cloned();
        Box::pin(async move {
            guard().map_err(direct_api_error)?;
            let binding = binding.ok_or_else(|| {
                direct_rejected("ASSET_NOT_BOUND", "asset is not bound to account")
            })?;
            let side = match request.side {
                1 => ApiSide::Buy,
                -1 => ApiSide::Sell,
                raw => {
                    return Err(direct_rejected(
                        "INVALID_SIDE",
                        format!("invalid submit side {raw}"),
                    ));
                }
            };
            let order_type = match request.order_type {
                0 => ApiOrderType::Limit,
                1 => ApiOrderType::Market,
                _ => {
                    return Err(direct_rejected(
                        "INVALID_ORDER_TYPE",
                        "invalid submit order type",
                    ));
                }
            };
            if request.time_in_force > 3 || request.quantity_lots <= 0 {
                return Err(direct_rejected(
                    "INVALID_ORDER",
                    "invalid time in force or quantity",
                ));
            }
            let position_side = match request.position_side {
                0 => ApiPositionSide::Net,
                1 => ApiPositionSide::Long,
                2 => ApiPositionSide::Short,
                _ => {
                    return Err(direct_rejected(
                        "INVALID_POSITION_SIDE",
                        "invalid position side",
                    ));
                }
            };
            if request.reduce_only
                && matches!(
                    position_side,
                    ApiPositionSide::Long | ApiPositionSide::Short
                )
                && !matches!(
                    (position_side, side),
                    (ApiPositionSide::Long, ApiSide::Sell) | (ApiPositionSide::Short, ApiSide::Buy)
                )
            {
                return Err(direct_rejected(
                    "INVALID_POSITION_SIDE",
                    "reduce-only order must close the specified side",
                ));
            }
            let client_order_id = id_text(request.client_order_id);
            let tracked = managed_account_order(&request, &binding)
                .map_err(|error| direct_rejected("INVALID_ORDER", error.message.to_string()))?;
            connector
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .track_managed_order(&binding.native_symbol, &client_order_id, &tracked);
            let wire = UnifiedOrderRequest {
                symbol: binding.native_symbol.to_string(),
                side,
                order_type,
                price: (request.order_type == 0)
                    .then(|| from_units(request.price_ticks, binding.price_tick)),
                qty: from_units(request.quantity_lots, binding.quantity_lot),
                time_in_force: api_tif(request.time_in_force),
                reduce_only: request.reduce_only
                    || account::is_hedge_close(request.position_side, request.side),
                position_side: Some(position_side),
                client_order_id: Some(client_order_id.clone()),
                stop_price: None,
            };
            let result = api.submit_order_fenced(&wire, Some(guard)).await;
            if result.as_ref().is_err_and(|error| !error.outcome_unknown()) {
                connector
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .reject_pending_order(&client_order_id);
            }
            result.map(direct_order_info).map_err(direct_api_error)
        })
    }

    fn direct_cancel(
        &self,
        request: account::DirectCancelOrderRequest,
    ) -> account::ExecutionFuture {
        if !self.committed.load(Ordering::Acquire)
            || !self.active.load(Ordering::Acquire)
            || !self.context.event_publisher.is_open()
        {
            return Box::pin(async {
                Err(direct_rejected(
                    "ACCOUNT_STOPPED",
                    "account runtime has stopped",
                ))
            });
        }
        let api = self.api.clone();
        let ids = self.ids.clone();
        let binding = self
            .context
            .instruments
            .iter()
            .find(|binding| binding.asset_id == request.asset_id)
            .cloned();
        Box::pin(async move {
            let binding = binding.ok_or_else(|| {
                direct_rejected("ASSET_NOT_BOUND", "asset is not bound to account")
            })?;
            if request.client_order_id.is_none() && request.venue_order_id.is_none() {
                return Err(direct_rejected(
                    "INVALID_CANCEL",
                    "cancel requires an order identifier",
                ));
            }
            let wire = CancelOrderRequest {
                symbol: binding.native_symbol.to_string(),
                order_id: request.venue_order_id.map(|id| resolve_id(&ids, id)),
                client_order_id: request.client_order_id.map(|id| resolve_id(&ids, id)),
            };
            api.cancel_order(&wire)
                .await
                .map(direct_order_info)
                .map_err(direct_api_error)
        })
    }
}

fn direct_order_info(order: OrderInfo) -> account::DirectOrderInfo {
    account::DirectOrderInfo {
        client_order_id: (!order.client_order_id.is_empty()).then_some(order.client_order_id),
        venue_order_id: (!order.order_id.is_empty()).then_some(order.order_id),
        status: api_status(order.status),
    }
}

fn direct_api_error(error: crate::api::ApiError) -> account::DirectExecutionError {
    let outcome_unknown = error.outcome_unknown();
    account::DirectExecutionError {
        exchange: error.exchange,
        code: error.code,
        message: error.message,
        outcome_unknown,
    }
}

fn direct_rejected(
    code: impl Into<String>,
    message: impl Into<String>,
) -> account::DirectExecutionError {
    account::DirectExecutionError {
        exchange: "runtime",
        code: code.into(),
        message: message.into(),
        outcome_unknown: false,
    }
}

impl AccountRuntime {
    fn stop_inner(&self, deadline: Instant) -> Result<(), account::AccountConnectorError> {
        self.ready.store(false, Ordering::Release);
        self.active.store(false, Ordering::Release);
        self.context.event_publisher.close();
        let Some(mut running) = self
            .running
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
        else {
            return Ok(());
        };
        if let Some(stop) = running.stop.take() {
            running.requested_deadline = Some(deadline);
            let _ = stop.send(deadline);
        }
        if let Some(thread) = running.thread.take() {
            while !thread.is_finished() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(1));
            }
            if !thread.is_finished() {
                *self.startup_error.lock().unwrap_or_else(|p| p.into_inner()) =
                    Some("account runtime stop deadline exceeded".into());
                running.thread = Some(thread);
                *self.running.lock().unwrap_or_else(|p| p.into_inner()) = Some(running);
                return Err(account::AccountConnectorError::new(
                    account::AccountErrorKind::DeadlineExceeded,
                    "account runtime stop deadline exceeded",
                ));
            }
            thread
                .join()
                .map_err(|_| rejected("account runtime thread panicked"))?;
        }
        let result = running
            .shutdown_result
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
            .unwrap_or_else(|| {
                Err(account::AccountConnectorError::new(
                    account::AccountErrorKind::ResourceReleaseFailed,
                    "account runtime exited without shutdown result",
                ))
            });
        if let Err(error) = &result {
            *self.startup_error.lock().unwrap_or_else(|p| p.into_inner()) = Some(error.to_string());
        }
        result
    }
}

struct AccountBootstrapSnapshot {
    orders: Vec<(account::OrderSnapshot, i64)>,
    positions: Vec<(account::PositionSnapshot, i64)>,
    balances: Vec<(account::BalanceSnapshot, i64)>,
}

async fn wait_for_private_stream(
    ready: &AtomicBool,
    notify: &Notify,
    timeout: Duration,
) -> Result<(), account::AccountConnectorError> {
    tokio::time::timeout(timeout, async {
        loop {
            let notified = notify.notified();
            if ready.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    })
    .await
    .map_err(|_| {
        account::AccountConnectorError::new(
            account::AccountErrorKind::DeadlineExceeded,
            "private account stream startup timed out",
        )
    })
}

async fn load_account_bootstrap<A: AccountBootstrapApi + ?Sized>(
    context: &account::AccountConnectorContext,
    api: &A,
    ids: &Mutex<IdInterner>,
) -> Result<AccountBootstrapSnapshot, account::AccountConnectorError> {
    api.validate()
        .await
        .map_err(|error| rejected(error.to_string()))?;

    let mut orders = Vec::new();
    for binding in context.instruments.iter() {
        for value in api
            .open_orders(&binding.native_symbol)
            .await
            .map_err(|error| rejected(error.to_string()))?
        {
            if is_external_order(&context.ownership, ids, &value.client_order_id) {
                continue;
            }
            orders.push((
                order_snapshot(&value, binding, ids)?,
                value.update_time.saturating_mul(1_000_000),
            ));
        }
    }

    let mut positions = Vec::new();
    let values = api
        .positions()
        .await
        .map_err(|error| rejected(error.to_string()))?;
    for binding in context.instruments.iter() {
        let native_currency = api.position_currency(&binding.native_symbol);
        let currency = position_currency_binding(context, native_currency.as_deref())?;
        let margin_currency_id = currency.currency_id;
        let mode = api
            .instrument_position_mode(&binding.native_symbol)
            .await
            .map_err(|error| rejected(error.to_string()))?;
        let mut sides = std::collections::BTreeSet::new();
        for value in values
            .iter()
            .filter(|value| value.symbol.eq_ignore_ascii_case(&binding.native_symbol))
        {
            let side = account_position_side(value.position_side)?;
            if matches!(
                (mode, side),
                (crate::api::ApiPositionMode::Net, 1 | 2) | (crate::api::ApiPositionMode::Hedge, 0)
            ) {
                return Err(rejected("position side contradicts venue account mode"));
            }
            if !sides.insert(side) {
                return Err(rejected("duplicate position side for one asset"));
            }
            if sides.len() > 1 && sides.contains(&ACCOUNT_POSITION_SIDE_NET) {
                return Err(rejected(
                    "mixed net and hedge position records for one asset",
                ));
            }
            positions.push((
                position_snapshot(value, binding, currency)?,
                value.update_time.saturating_mul(1_000_000),
            ));
        }
        let required: &[u8] = match mode {
            crate::api::ApiPositionMode::Net => &[ACCOUNT_POSITION_SIDE_NET],
            crate::api::ApiPositionMode::Hedge => {
                &[ACCOUNT_POSITION_SIDE_LONG, ACCOUNT_POSITION_SIDE_SHORT]
            }
            crate::api::ApiPositionMode::Unknown if sides.is_empty() => {
                return Err(rejected(
                    "empty positions require a known venue position mode",
                ));
            }
            crate::api::ApiPositionMode::Unknown if sides.contains(&ACCOUNT_POSITION_SIDE_NET) => {
                &[ACCOUNT_POSITION_SIDE_NET]
            }
            crate::api::ApiPositionMode::Unknown => {
                &[ACCOUNT_POSITION_SIDE_LONG, ACCOUNT_POSITION_SIDE_SHORT]
            }
        };
        for &side in required {
            if sides.contains(&side) {
                continue;
            }
            positions.push((
                account::PositionSnapshot {
                    asset_id: binding.asset_id,
                    position_side: side,
                    margin_type: 2,
                    quantity_lots: 0,
                    entry_price_ticks: 0,
                    liquidation_price_ticks: 0,
                    realized_pnl_units: 0,
                    unrealized_pnl_units: 0,
                    margin_currency_id,
                },
                0,
            ));
        }
    }

    let account_info = api
        .account()
        .await
        .map_err(|error| rejected(error.to_string()))?;
    let mut balances = Vec::new();
    for value in &account_info.balances {
        if let Some(binding) = context
            .currencies
            .iter()
            .find(|binding| binding.native_currency.eq_ignore_ascii_case(&value.asset))
        {
            balances.push((
                balance_snapshot(value, binding)?,
                account_info.timestamp.saturating_mul(1_000_000),
            ));
        }
    }

    Ok(AccountBootstrapSnapshot {
        orders,
        positions,
        balances,
    })
}

async fn reconcile_account_bootstrap<A: AccountBootstrapApi + ?Sized>(
    context: &account::AccountConnectorContext,
    api: &A,
    ids: &Mutex<IdInterner>,
    known: &[account::OrderSnapshot],
) -> Result<AccountBootstrapSnapshot, account::AccountConnectorError> {
    let mut snapshot = load_account_bootstrap(context, api, ids).await?;
    for previous in known {
        if snapshot
            .orders
            .iter()
            .any(|(value, _)| value.client_order_id == previous.client_order_id)
        {
            continue;
        }
        let binding = context
            .instruments
            .iter()
            .find(|value| value.asset_id == previous.asset_id)
            .ok_or_else(|| rejected("recovery order asset is not bound"))?;
        let client_id = resolve_id(ids, previous.client_order_id);
        let value = api
            .order(&binding.native_symbol, &client_id)
            .await
            .map_err(|error| rejected(format!("unresolved recovery order: {error}")))?;
        snapshot.orders.push((
            order_snapshot(&value, binding, ids)?,
            value.update_time.saturating_mul(1_000_000),
        ));
    }
    Ok(snapshot)
}

fn seed_managed_orders(
    connector: &Mutex<Box<dyn Connector>>,
    context: &account::AccountConnectorContext,
    snapshot: &AccountBootstrapSnapshot,
    ids: &Mutex<IdInterner>,
) -> Result<(), account::AccountConnectorError> {
    let connector = connector.lock().unwrap_or_else(|p| p.into_inner());
    for (value, ts) in &snapshot.orders {
        if !matches!(value.status, 1 | 5) {
            continue;
        }
        let binding = context
            .instruments
            .iter()
            .find(|item| item.asset_id == value.asset_id)
            .ok_or_else(|| rejected("seed order asset is not bound"))?;
        let mut order = managed_account_order(
            &account::DirectNewOrderRequest {
                position_side: value.position_side,
                asset_id: value.asset_id,
                side: if value.side == ACCOUNT_SIDE_BUY {
                    1
                } else {
                    -1
                },
                order_type: value.order_type,
                time_in_force: value.time_in_force,
                price_ticks: value.price_ticks,
                quantity_lots: value.quantity_lots,
                reduce_only: value.reduce_only,
                client_order_id: value.client_order_id,
            },
            binding,
        )?;
        order.status = if value.status == 5 {
            Status::PartiallyFilled
        } else {
            Status::New
        };
        order.leaves_qty = from_units(
            value.quantity_lots - value.filled_quantity_lots,
            binding.quantity_lot,
        );
        order.exch_timestamp = *ts;
        connector.track_managed_order(
            &binding.native_symbol,
            &resolve_id(ids, value.client_order_id),
            &order,
        );
    }
    Ok(())
}

struct AccountEventEncoder {
    context: account::AccountConnectorContext,
    epoch: Arc<AtomicU64>,
    version: Arc<AtomicU64>,
    ready: Arc<AtomicBool>,
    ids: Arc<Mutex<IdInterner>>,
    projection: Arc<Mutex<AccountProjection>>,
    publication: Mutex<()>,
}
impl AccountEventEncoder {
    fn header(&self, kind: u16, flags: u16, ts: i64) -> account::AccountEventHeaderV1 {
        account::AccountEventHeaderV1 {
            account_id: self.context.account.account_id.0,
            kind,
            flags,
            account_generation: self.context.account.generation,
            account_epoch: self.epoch.load(Ordering::Acquire),
            account_version: self.version.fetch_add(1, Ordering::AcqRel) + 1,
            exchange_ts: ts,
            receive_ts: now_ns(),
        }
    }

    fn publish_ready(&self) -> Result<(), account::AccountConnectorError> {
        let _publication = self.publication.lock().unwrap_or_else(|p| p.into_inner());
        self.publish_ready_locked()
    }

    fn publish_ready_locked(&self) -> Result<(), account::AccountConnectorError> {
        let event = account::StreamStateChangedV1(account::StreamStateV1 {
            header: self.header(account::event_kind::STREAM_STATE_CHANGED, 0, now_ns()),
            state: account::AccountLifecycle::Ready as u8,
            reason_code: 0,
        });
        self.emit(&event)?;
        self.projection
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .commit(event.0.header);
        self.ready.store(true, Ordering::Release);
        Ok(())
    }

    fn emit<T: account::AccountEventPayload>(
        &self,
        event: &T,
    ) -> Result<(), account::AccountConnectorError> {
        // A replacement candidate commits its local image while external publication is closed.
        if !self.context.event_publisher.is_open() {
            return Ok(());
        }
        self.context
            .event_publisher
            .publish_encoded(event, TraceContext::default())
            .map_err(|error| rejected(error.to_string()))
    }

    fn install_bootstrap(
        &self,
        snapshot: AccountBootstrapSnapshot,
    ) -> Result<(), account::AccountConnectorError> {
        let _publication = self.publication.lock().unwrap_or_else(|p| p.into_inner());
        self.install_bootstrap_locked(snapshot)
    }

    fn install_bootstrap_locked(
        &self,
        snapshot: AccountBootstrapSnapshot,
    ) -> Result<(), account::AccountConnectorError> {
        let mut image = AccountProjection::default();
        image.epoch = self.epoch.load(Ordering::Acquire);
        for (value, ts) in snapshot.orders {
            let event = order_event(
                self.header(
                    account::event_kind::ORDER_CHANGED,
                    account::event_flags::SNAPSHOT | account::event_flags::UPSERT,
                    ts,
                ),
                &value,
            );
            self.emit(&event)?;
            image.order(&event);
        }
        for (value, ts) in snapshot.positions {
            let event = position_event(
                self.header(
                    account::event_kind::POSITION_CHANGED,
                    account::event_flags::SNAPSHOT | account::event_flags::UPSERT,
                    ts,
                ),
                &value,
            );
            self.emit(&event)?;
            image.position(&event);
        }
        for (value, ts) in snapshot.balances {
            let event = balance_event(
                self.header(
                    account::event_kind::BALANCE_CHANGED,
                    account::event_flags::SNAPSHOT | account::event_flags::UPSERT,
                    ts,
                ),
                &value,
            );
            self.emit(&event)?;
            image.balance(&event);
        }
        *self.projection.lock().unwrap_or_else(|p| p.into_inner()) = image;
        Ok(())
    }

    fn activate(&self) -> Result<(), account::AccountConnectorError> {
        let _publication = self.publication.lock().unwrap_or_else(|p| p.into_inner());
        if !self.ready.load(Ordering::Acquire) || !self.context.event_publisher.is_open() {
            return Err(rejected("account candidate is not ready for activation"));
        }
        let snapshot = {
            let image = self.projection.lock().unwrap_or_else(|p| p.into_inner());
            AccountBootstrapSnapshot {
                orders: image
                    .orders(account::OrderFilter {
                        asset_id: None,
                        include_final: true,
                    })
                    .1
                    .iter()
                    .cloned()
                    .map(|value| {
                        let ts = image.order_time(&value);
                        (value, ts)
                    })
                    .collect(),
                positions: image
                    .positions(account::PositionFilter::default())
                    .1
                    .iter()
                    .cloned()
                    .map(|value| {
                        let ts = image.position_time(&value);
                        (value, ts)
                    })
                    .collect(),
                balances: image
                    .balances()
                    .1
                    .iter()
                    .cloned()
                    .map(|value| {
                        let ts = image.balance_time(&value);
                        (value, ts)
                    })
                    .collect(),
            }
        };
        self.install_bootstrap_locked(snapshot)?;
        self.publish_ready_locked()
    }

    fn refresh_balances(&self, info: &AccountInfo) -> Result<(), account::AccountConnectorError> {
        for balance in &info.balances {
            self.publish(&AccountPublication::Balance {
                currency: balance.asset.clone(),
                wallet: balance.wallet_balance,
                available: Some(balance.available_balance),
                margin: Some(balance.margin_balance),
                unrealized_pnl: Some(balance.unrealized_pnl),
                exch_ts: info.timestamp.saturating_mul(1_000_000),
            })?;
        }
        Ok(())
    }

    fn cache_order(&self, event: &account::OrderChangedV1) {
        let mut image = self.projection.lock().unwrap_or_else(|p| p.into_inner());
        image.order(event);
        self.ids
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .prune(&image);
    }

    fn cache_position(&self, event: &account::PositionChangedV1) -> bool {
        self.projection
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .position(event)
    }

    fn publish(&self, event: &AccountPublication) -> Result<(), account::AccountConnectorError> {
        let _publication = self.publication.lock().unwrap_or_else(|p| p.into_inner());
        match event {
            AccountPublication::Order {
                position_side,
                symbol,
                client_order_id,
                venue_order_id,
                order,
            }
            | AccountPublication::OrderWithExecution {
                position_side,
                symbol,
                client_order_id,
                venue_order_id,
                order,
                ..
            } => {
                let binding = self
                    .context
                    .instruments
                    .iter()
                    .find(|b| b.native_symbol.eq_ignore_ascii_case(symbol))
                    .ok_or_else(|| rejected("unbound private order symbol"))?;
                let cumulative_filled_quantity_lots =
                    to_units(order.qty - order.leaves_qty, binding.quantity_lot)?;
                let venue_text = match venue_order_id {
                    Some(text) => text.clone(),
                    None => order.order_id.to_string(),
                };
                let mut ids = self.ids.lock().unwrap_or_else(|p| p.into_inner());
                let id = ids.intern(&venue_text);
                let client_order_id = client_order_id.as_deref().map(|text| ids.intern(text));
                drop(ids);

                let previous = self
                    .projection
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .cumulative(binding.asset_id.0, id);
                if self
                    .projection
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .order_is_stale(
                        binding.asset_id.0,
                        id,
                        order.exch_timestamp,
                        cumulative_filled_quantity_lots,
                    )
                {
                    return Ok(());
                }
                let publish_fill =
                    order.exec_qty > 0.0 && cumulative_filled_quantity_lots > previous;
                if publish_fill
                    && to_units(order.exec_qty, binding.quantity_lot)?
                        != cumulative_filled_quantity_lots - previous
                {
                    return Err(rejected(
                        "missing or inconsistent cumulative fill; account reconciliation required",
                    ));
                }

                let changed = account::OrderChangedV1 {
                    header: self.header(
                        account::event_kind::ORDER_CHANGED,
                        if order.active() {
                            account::event_flags::UPSERT
                        } else {
                            account::event_flags::FINAL
                        } | account::event_flags::position_flags(account_position_side(
                            *position_side,
                        )?) | if account::is_hedge_close(
                            account_position_side(*position_side)?,
                            if order.side == Side::Buy { 1 } else { -1 },
                        ) {
                            account::event_flags::REDUCE_ONLY
                        } else {
                            0
                        },
                        order.exch_timestamp,
                    ),
                    asset_id: binding.asset_id.0,
                    side: side(order.side),
                    order_type: order_type(order.order_type),
                    time_in_force: tif(order.time_in_force),
                    status: status(order.status),
                    price_ticks: order.price_tick,
                    quantity_lots: to_units(order.qty, binding.quantity_lot)?,
                    filled_quantity_lots: cumulative_filled_quantity_lots,
                    average_price_ticks: match event {
                        AccountPublication::OrderWithExecution { average_price, .. } => {
                            to_observation_units(*average_price, binding.price_tick)?
                        }
                        _ => order.exec_price_tick,
                    },
                    venue_order_id: id,
                    client_order_id: client_order_id.unwrap_or_default(),
                    ..Default::default()
                };
                self.emit(&changed)?;
                self.cache_order(&changed);
                if publish_fill {
                    let fill = account::FillV2 {
                        header: self.header(
                            account::event_kind::FILL,
                            account::event_flags::UPSERT,
                            order.exch_timestamp,
                        ),
                        asset_id: binding.asset_id.0,
                        side: side(order.side),
                        liquidity: u8::from(order.maker),
                        price_ticks: order.exec_price_tick,
                        last_fill_quantity_lots: to_units(order.exec_qty, binding.quantity_lot)?,
                        cumulative_filled_quantity_lots,
                        venue_order_id: id,
                        client_order_id: client_order_id.unwrap_or_default(),
                        ..Default::default()
                    };
                    self.emit(&fill)?;
                    self.projection
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .commit(fill.header);
                    tracing::info!(
                        account_id = fill.header.account_id,
                        asset_id = fill.asset_id,
                        side = fill.side,
                        last_fill_quantity_lots = fill.last_fill_quantity_lots,
                        cumulative_filled_quantity_lots = fill.cumulative_filled_quantity_lots,
                        "Account fill event published."
                    );
                }
                Ok(())
            }
            AccountPublication::Position {
                entry_price,
                unrealized_pnl,
                liquidation_price,
                position_side,
                symbol,
                qty,
                exch_ts,
            } => {
                let b = self
                    .context
                    .instruments
                    .iter()
                    .find(|b| b.native_symbol.eq_ignore_ascii_case(symbol))
                    .ok_or_else(|| rejected("unbound position symbol"))?;
                let header = self.header(
                    account::event_kind::POSITION_CHANGED,
                    account::event_flags::UPSERT,
                    *exch_ts,
                );
                let side = account_position_side(*position_side)?;
                let previous = {
                    let image = self.projection.lock().unwrap_or_else(|p| p.into_inner());
                    if !image.position_mode_matches(b.asset_id.0, side) {
                        return Err(rejected(
                            "position mode changed; account reconciliation required",
                        ));
                    }
                    image.position_image(b.asset_id.0, side).cloned()
                };
                let mut event = previous
                    .as_ref()
                    .map(|value| position_event(header, value))
                    .unwrap_or(account::PositionChangedV1 {
                        header,
                        asset_id: b.asset_id.0,
                        ..Default::default()
                    });
                event.position_side = account_position_side(*position_side)?;
                event.quantity_lots = to_units(*qty, b.quantity_lot)?;
                if (event.position_side == 1 && event.quantity_lots < 0)
                    || (event.position_side == 2 && event.quantity_lots > 0)
                {
                    return Err(rejected("position quantity contradicts position side"));
                }
                if let Some(price) = entry_price {
                    event.entry_price_ticks = to_observation_units(*price, b.price_tick)?;
                }
                if let Some(price) = liquidation_price {
                    event.liquidation_price_ticks = to_observation_units(*price, b.price_tick)?;
                }
                if let Some(pnl) = unrealized_pnl {
                    let currency = if previous.is_some() {
                        self.context
                            .currencies
                            .iter()
                            .find(|c| c.currency_id.0 == event.margin_currency_id)
                    } else {
                        self.context.currencies.first()
                    }
                    .ok_or_else(|| rejected("position margin currency binding missing"))?;
                    event.margin_currency_id = currency.currency_id.0;
                    event.unrealized_pnl_units = to_observation_units(*pnl, currency.amount_unit)?;
                }
                if event.quantity_lots == 0 {
                    event.entry_price_ticks = 0;
                    event.liquidation_price_ticks = 0;
                    event.unrealized_pnl_units = 0;
                }
                if self
                    .projection
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .position_is_stale(
                        event.asset_id,
                        event.position_side,
                        event.header.exchange_ts,
                    )
                {
                    return Ok(());
                }
                self.emit(&event)?;
                self.cache_position(&event);
                Ok(())
            }
            AccountPublication::Balance {
                currency,
                wallet,
                available,
                margin,
                unrealized_pnl,
                exch_ts,
            } => {
                let Some(binding) = self
                    .context
                    .currencies
                    .iter()
                    .find(|binding| binding.native_currency.eq_ignore_ascii_case(currency))
                else {
                    return Ok(());
                };
                let image = self.projection.lock().unwrap_or_else(|p| p.into_inner());
                let previous = image.balance_image(binding.currency_id.0).cloned();
                let event = account::BalanceChangedV1 {
                    header: self.header(
                        account::event_kind::BALANCE_CHANGED,
                        account::event_flags::UPSERT,
                        *exch_ts,
                    ),
                    currency_id: binding.currency_id.0,
                    wallet_units: to_observation_units(*wallet, binding.amount_unit)?,
                    available_units: available
                        .map(|value| to_observation_units(value, binding.amount_unit))
                        .transpose()?
                        .unwrap_or_else(|| {
                            previous.as_ref().map_or(0, |value| value.available_units)
                        }),
                    margin_units: margin
                        .map(|value| to_observation_units(value, binding.amount_unit))
                        .transpose()?
                        .unwrap_or_else(|| previous.as_ref().map_or(0, |value| value.margin_units)),
                    unrealized_pnl_units: unrealized_pnl
                        .map(|value| to_observation_units(value, binding.amount_unit))
                        .transpose()?
                        .unwrap_or_else(|| {
                            previous
                                .as_ref()
                                .map_or(0, |value| value.unrealized_pnl_units)
                        }),
                };
                let stale = image.balance_is_stale(event.currency_id, event.header.exchange_ts);
                drop(image);
                if stale {
                    return Ok(());
                }
                self.emit(&event)?;
                self.projection
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .balance(&event);
                Ok(())
            }
            AccountPublication::Error(_) => {
                self.invalidate_locked(account::invalidation_reason::PRIVATE_STREAM);
                Ok(())
            }
        }
    }

    fn invalidate(&self, reason_code: u32) {
        let _publication = self.publication.lock().unwrap_or_else(|p| p.into_inner());
        self.invalidate_locked(reason_code);
    }

    fn invalidate_locked(&self, reason_code: u32) {
        self.ready.store(false, Ordering::Release);
        publish_invalidated(&self.context, &self.epoch, &self.version, reason_code);
    }
}

fn publish_invalidated(
    context: &account::AccountConnectorContext,
    epoch: &AtomicU64,
    version: &AtomicU64,
    reason_code: u32,
) {
    if !context.event_publisher.is_open() {
        return;
    }
    let event = account::StreamInvalidatedV1(account::StreamStateV1 {
        header: header(
            context,
            epoch,
            version,
            account::event_kind::STREAM_INVALIDATED,
            0,
            now_ns(),
        ),
        state: account::AccountLifecycle::Invalidated as u8,
        reason_code,
    });
    if let Err(error) = context
        .event_publisher
        .publish_encoded(&event, TraceContext::default())
    {
        tracing::error!(
            account_id = context.account.account_id.0,
            reason_code,
            reason = account::invalidation_reason::name(reason_code),
            account_epoch = event.0.header.account_epoch,
            account_version = event.0.header.account_version,
            event_type = account::STREAM_INVALIDATED_EVENT,
            schema_version = <account::StreamInvalidatedV1 as account::AccountEventPayload>::SCHEMA_VERSION,
            payload_len = <account::StreamInvalidatedV1 as account::AccountEventPayload>::ENCODED_LEN,
            error = %error,
            "Account invalidation publication failed."
        );
        eprintln!(
            "account {} invalidation publication failed: reason={} reason_code={} event_type={} schema_version={} payload_len={}: {error}",
            context.account.account_id.0,
            account::invalidation_reason::name(reason_code),
            reason_code,
            account::STREAM_INVALIDATED_EVENT,
            <account::StreamInvalidatedV1 as account::AccountEventPayload>::SCHEMA_VERSION,
            <account::StreamInvalidatedV1 as account::AccountEventPayload>::ENCODED_LEN,
        );
    }
}

fn header(
    context: &account::AccountConnectorContext,
    epoch: &AtomicU64,
    version: &AtomicU64,
    kind: u16,
    flags: u16,
    ts: i64,
) -> account::AccountEventHeaderV1 {
    account::AccountEventHeaderV1 {
        account_id: context.account.account_id.0,
        kind,
        flags,
        account_generation: context.account.generation,
        account_epoch: epoch.load(Ordering::Acquire),
        account_version: version.fetch_add(1, Ordering::AcqRel) + 1,
        exchange_ts: ts,
        receive_ts: now_ns(),
    }
}

fn is_external_order(
    ownership: &account::OrderOwnershipPolicy,
    ids: &Mutex<IdInterner>,
    client_order_id: &str,
) -> bool {
    match ownership {
        account::OrderOwnershipPolicy::ObserveAll => false,
        account::OrderOwnershipPolicy::ManagedOnly { client_id_prefix } => {
            !client_order_id.starts_with(client_id_prefix.as_ref())
                && !ids
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .contains_text(client_order_id)
        }
    }
}

fn order_snapshot(
    value: &OrderInfo,
    binding: &account::AccountInstrumentBinding,
    ids: &Mutex<IdInterner>,
) -> Result<account::OrderSnapshot, account::AccountConnectorError> {
    let mut ids = ids.lock().unwrap_or_else(|p| p.into_inner());
    Ok(account::OrderSnapshot {
        position_side: match value.position_side {
            ApiPositionSide::Unknown => 0,
            side => account_position_side(side)?,
        },
        asset_id: binding.asset_id,
        side: match value.side {
            ApiSide::Buy => ACCOUNT_SIDE_BUY,
            ApiSide::Sell => ACCOUNT_SIDE_SELL,
            ApiSide::Unknown => return Err(rejected("unknown order side")),
        },
        order_type: u8::from(value.order_type == ApiOrderType::Market),
        time_in_force: match value.time_in_force {
            ApiTimeInForce::GTC => 0,
            ApiTimeInForce::GTX => 1,
            ApiTimeInForce::FOK => 2,
            ApiTimeInForce::IOC => 3,
            _ => 255,
        },
        status: api_status(value.status),
        price_ticks: to_units(value.price, binding.price_tick)?,
        quantity_lots: to_units(value.qty, binding.quantity_lot)?,
        filled_quantity_lots: to_units(value.executed_qty, binding.quantity_lot)?,
        reduce_only: value.reduce_only
            || account::is_hedge_close(
                match value.position_side {
                    ApiPositionSide::Long => 1,
                    ApiPositionSide::Short => 2,
                    _ => 0,
                },
                if value.side == ApiSide::Buy { 1 } else { -1 },
            ),
        client_order_id: ids.intern(&value.client_order_id),
        venue_order_id: ids.intern(&value.order_id),
        command_id: account::Id128::default(),
    })
}

fn account_position_side(side: ApiPositionSide) -> Result<u8, account::AccountConnectorError> {
    match side {
        ApiPositionSide::Net => Ok(ACCOUNT_POSITION_SIDE_NET),
        ApiPositionSide::Long => Ok(ACCOUNT_POSITION_SIDE_LONG),
        ApiPositionSide::Short => Ok(ACCOUNT_POSITION_SIDE_SHORT),
        ApiPositionSide::Unknown => Err(rejected("unknown position side")),
    }
}

fn position_currency_binding<'a>(
    context: &'a account::AccountConnectorContext,
    native: Option<&str>,
) -> Result<&'a account::AccountCurrencyBinding, account::AccountConnectorError> {
    let currency = if let Some(native) = native {
        context
            .currencies
            .iter()
            .find(|c| c.native_currency.eq_ignore_ascii_case(native))
    } else {
        context.currencies.first()
    };
    currency.ok_or_else(|| rejected("position margin currency binding missing"))
}

fn position_snapshot(
    value: &PositionInfo,
    binding: &account::AccountInstrumentBinding,
    currency: &account::AccountCurrencyBinding,
) -> Result<account::PositionSnapshot, account::AccountConnectorError> {
    if (value.position_side == ApiPositionSide::Long && value.qty < 0.0)
        || (value.position_side == ApiPositionSide::Short && value.qty > 0.0)
    {
        return Err(rejected("position quantity contradicts position side"));
    }
    Ok(account::PositionSnapshot {
        asset_id: binding.asset_id,
        position_side: account_position_side(value.position_side)?,
        margin_type: if value.margin_type == ApiMarginType::Isolated {
            1
        } else {
            2
        },
        quantity_lots: to_units(value.qty, binding.quantity_lot)?,
        entry_price_ticks: to_observation_units(value.entry_price, binding.price_tick)?,
        liquidation_price_ticks: to_observation_units(value.liquidation_price, binding.price_tick)?,
        realized_pnl_units: to_observation_units(value.realized_pnl, currency.amount_unit)?,
        unrealized_pnl_units: to_observation_units(value.unrealized_pnl, currency.amount_unit)?,
        margin_currency_id: currency.currency_id,
    })
}

fn balance_snapshot(
    value: &Balance,
    binding: &account::AccountCurrencyBinding,
) -> Result<account::BalanceSnapshot, account::AccountConnectorError> {
    Ok(account::BalanceSnapshot {
        currency_id: binding.currency_id,
        wallet_units: to_observation_units(value.wallet_balance, binding.amount_unit)?,
        available_units: to_observation_units(value.available_balance, binding.amount_unit)?,
        margin_units: to_observation_units(value.margin_balance, binding.amount_unit)?,
        unrealized_pnl_units: to_observation_units(value.unrealized_pnl, binding.amount_unit)?,
    })
}

fn order_event(
    mut header: account::AccountEventHeaderV1,
    value: &account::OrderSnapshot,
) -> account::OrderChangedV1 {
    header.flags |= account::event_flags::position_flags(value.position_side);
    if value.reduce_only {
        header.flags |= account::event_flags::REDUCE_ONLY;
    }
    account::OrderChangedV1 {
        header,
        asset_id: value.asset_id.0,
        side: value.side,
        order_type: value.order_type,
        time_in_force: value.time_in_force,
        status: value.status,
        price_ticks: value.price_ticks,
        quantity_lots: value.quantity_lots,
        filled_quantity_lots: value.filled_quantity_lots,
        client_order_id: value.client_order_id,
        venue_order_id: value.venue_order_id,
        command_id: value.command_id,
        ..Default::default()
    }
}

fn position_event(
    header: account::AccountEventHeaderV1,
    value: &account::PositionSnapshot,
) -> account::PositionChangedV1 {
    account::PositionChangedV1 {
        header,
        asset_id: value.asset_id.0,
        position_side: value.position_side,
        margin_type: value.margin_type,
        quantity_lots: value.quantity_lots,
        entry_price_ticks: value.entry_price_ticks,
        liquidation_price_ticks: value.liquidation_price_ticks,
        realized_pnl_units: value.realized_pnl_units,
        unrealized_pnl_units: value.unrealized_pnl_units,
        margin_currency_id: value.margin_currency_id.0,
    }
}

fn balance_event(
    header: account::AccountEventHeaderV1,
    value: &account::BalanceSnapshot,
) -> account::BalanceChangedV1 {
    account::BalanceChangedV1 {
        header,
        currency_id: value.currency_id.0,
        wallet_units: value.wallet_units,
        available_units: value.available_units,
        margin_units: value.margin_units,
        unrealized_pnl_units: value.unrealized_pnl_units,
    }
}

fn to_units(value: f64, unit: account::DecimalUnit) -> Result<i64, account::AccountConnectorError> {
    crate::units::integer_units(value, unit.as_f64(), true).map_err(rejected)
}
fn to_observation_units(
    value: f64,
    unit: account::DecimalUnit,
) -> Result<i64, account::AccountConnectorError> {
    crate::units::integer_units(value, unit.as_f64(), false).map_err(rejected)
}
fn from_units(value: i64, unit: account::DecimalUnit) -> f64 {
    crate::units::from_units(value, unit)
}
fn decimal_unit_f64(unit: &account::DecimalUnit) -> f64 {
    unit.as_f64()
}
fn managed_account_order(
    request: &account::DirectNewOrderRequest,
    binding: &account::AccountInstrumentBinding,
) -> Result<Order, account::AccountConnectorError> {
    let side = match request.side {
        1 => Side::Buy,
        -1 => Side::Sell,
        raw => return Err(rejected(format!("invalid submit side {raw}"))),
    };
    let order_type = if request.order_type == 0 {
        OrdType::Limit
    } else {
        OrdType::Market
    };
    let time_in_force = match request.time_in_force {
        0 => TimeInForce::GTC,
        1 => TimeInForce::GTX,
        2 => TimeInForce::FOK,
        3 => TimeInForce::IOC,
        _ => return Err(rejected("invalid submit time in force")),
    };
    let qty = from_units(request.quantity_lots, binding.quantity_lot);
    let order_id = crate::utils::managed_order_id(&request.client_order_id.0);
    let mut order = Order::new(
        order_id,
        request.price_ticks,
        decimal_unit_f64(&binding.price_tick),
        qty,
        side,
        order_type,
        time_in_force,
    );
    order.status = Status::New;
    order.req = Status::None;
    Ok(order)
}
fn id_text(id: account::Id128) -> String {
    id.0.iter().map(|b| format!("{b:02x}")).collect()
}
fn resolve_id(ids: &Mutex<IdInterner>, id: account::Id128) -> String {
    ids.lock().unwrap_or_else(|p| p.into_inner()).resolve(id)
}
fn parse_hex_id(text: &str) -> Option<account::Id128> {
    let text = text.strip_prefix("0x").unwrap_or(text);
    if text.len() != 32 {
        return None;
    }
    let mut bytes = [0_u8; 16];
    for (index, pair) in text.as_bytes().chunks_exact(2).enumerate() {
        let pair = std::str::from_utf8(pair).ok()?;
        bytes[index] = u8::from_str_radix(pair, 16).ok()?;
    }
    Some(account::Id128(bytes))
}
fn side(v: Side) -> u8 {
    match v {
        Side::Buy => 1,
        Side::Sell => 2,
        _ => 0,
    }
}
fn order_type(v: OrdType) -> u8 {
    match v {
        OrdType::Limit => 0,
        OrdType::Market => 1,
        _ => 255,
    }
}
fn tif(v: TimeInForce) -> u8 {
    v as u8
}
fn status(v: Status) -> u8 {
    v as u8
}
fn api_status(v: ApiOrderStatus) -> u8 {
    match v {
        ApiOrderStatus::New => 1,
        ApiOrderStatus::PartiallyFilled => 5,
        ApiOrderStatus::Filled => 3,
        ApiOrderStatus::Canceled => 4,
        ApiOrderStatus::Rejected => 6,
        ApiOrderStatus::Expired => 2,
        _ => 255,
    }
}
fn api_tif(v: u8) -> ApiTimeInForce {
    match v {
        0 => ApiTimeInForce::GTC,
        1 => ApiTimeInForce::GTX,
        2 => ApiTimeInForce::FOK,
        3 => ApiTimeInForce::IOC,
        _ => ApiTimeInForce::Unknown,
    }
}
fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(i64::MAX as u128) as i64
}
fn rejected(message: impl Into<Arc<str>>) -> account::AccountConnectorError {
    account::AccountConnectorError::rejected(message)
}

type BuildConnector = fn(&str) -> Result<Box<dyn Connector>, String>;
pub struct VenueAccountConnectorFactory {
    connector_type: &'static str,
    build: BuildConnector,
}
impl VenueAccountConnectorFactory {
    pub const fn new(connector_type: &'static str, build: BuildConnector) -> Self {
        Self {
            connector_type,
            build,
        }
    }
}
impl account::AccountConnectorFactory for VenueAccountConnectorFactory {
    fn connector_type(&self) -> &str {
        self.connector_type
    }
    fn create(
        &self,
        definition: &account::AccountDefinition,
        context: account::AccountConnectorContext,
    ) -> Result<Arc<dyn account::AccountConnector>, account::AccountConnectorError> {
        crate::ensure_rustls_crypto_provider();
        let secret = context.secrets.resolve(&definition.credential_ref)?;
        let config = merged_toml(&definition.connector_config, &secret)?;
        let connector = (self.build)(&config).map_err(rejected)?;
        let api = connector
            .broker_api()
            .ok_or_else(|| rejected("venue connector does not expose BrokerApi"))?;
        Ok(AccountRuntime::new(
            connector,
            api,
            definition.shutdown_order_policy.clone(),
            context,
        ))
    }
}

fn merged_toml(
    public: &[u8],
    secret: &SecretValue,
) -> Result<String, account::AccountConnectorError> {
    let public =
        std::str::from_utf8(public).map_err(|_| rejected("connector config must be UTF-8 TOML"))?;
    let private = std::str::from_utf8(secret.expose())
        .map_err(|_| rejected("credential secret must be UTF-8 TOML"))?;
    let mut root: toml::Value = if public.trim().is_empty() {
        toml::Value::Table(Default::default())
    } else {
        toml::from_str(public).map_err(|_| rejected("connector config is invalid"))?
    };
    let overlay: toml::Value =
        toml::from_str(private).map_err(|_| rejected("credential secret has invalid structure"))?;
    merge_value(&mut root, overlay);
    toml::to_string(&root).map_err(|_| rejected("merged connector config is invalid"))
}
fn merge_value(base: &mut toml::Value, overlay: toml::Value) {
    match (base, overlay) {
        (toml::Value::Table(base), toml::Value::Table(overlay)) => {
            for (k, v) in overlay {
                if let Some(old) = base.get_mut(&k) {
                    merge_value(old, v)
                } else {
                    base.insert(k, v);
                }
            }
        }
        (base, value) => *base = value,
    }
}

#[cfg(feature = "binancefutures")]
fn build_binance(config: &str) -> Result<Box<dyn Connector>, String> {
    crate::binancefutures::BinanceFutures::build_from(config)
        .map(|v| Box::new(v) as Box<dyn Connector>)
        .map_err(|e| e.to_string())
}
#[cfg(feature = "okx")]
fn build_okx(config: &str) -> Result<Box<dyn Connector>, String> {
    crate::okx::Okx::build_from(config)
        .map(|v| Box::new(v) as Box<dyn Connector>)
        .map_err(|e| e.to_string())
}
#[cfg(feature = "hyperliquid")]
fn build_hyperliquid(config: &str) -> Result<Box<dyn Connector>, String> {
    crate::hyperliquid::Hyperliquid::build_from(config)
        .map(|v| Box::new(v) as Box<dyn Connector>)
        .map_err(|e| e.to_string())
}

#[cfg(feature = "gatecrossex")]
fn build_gate_crossex(config: &str) -> Result<Box<dyn Connector>, String> {
    crate::gatecrossex::GateCrossEx::build_from(config)
        .map(|v| Box::new(v) as Box<dyn Connector>)
        .map_err(|e| e.to_string())
}

pub fn venue_account_factories() -> Vec<Arc<dyn account::AccountConnectorFactory>> {
    let values: Vec<Arc<dyn account::AccountConnectorFactory>> = vec![
        #[cfg(feature = "gatecrossex")]
        Arc::new(VenueAccountConnectorFactory::new(
            "gate-crossex-account",
            build_gate_crossex,
        )),
        #[cfg(feature = "binancefutures")]
        Arc::new(VenueAccountConnectorFactory::new(
            "binance-futures-account",
            build_binance,
        )),
        #[cfg(feature = "okx")]
        Arc::new(VenueAccountConnectorFactory::new("okx-account", build_okx)),
        #[cfg(feature = "hyperliquid")]
        Arc::new(VenueAccountConnectorFactory::new(
            "hyperliquid-account",
            build_hyperliquid,
        )),
    ];
    values
}

#[cfg(test)]
mod tests {
    use super::*;
    use titan_core_types::{ComponentIdentity, CoreError, ResourceScope};

    #[derive(Default)]
    struct RecordingSink {
        events: Mutex<Vec<(String, Vec<u8>)>>,
        reject: AtomicBool,
    }

    impl account::AccountEventSink for RecordingSink {
        fn publish(
            &self,
            event_type: &str,
            payload: &[u8],
            _: TraceContext,
        ) -> Result<(), CoreError> {
            if self.reject.load(Ordering::Acquire) {
                return Err(CoreError::new(
                    titan_core_types::ErrorKind::ControlQueueFull,
                    ComponentIdentity::new("test", "account-sink"),
                    titan_core_types::ComponentState::Running,
                    "publish",
                    "injected publication rejection",
                ));
            }
            self.events
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push((event_type.to_owned(), payload.to_vec()));
            Ok(())
        }
    }

    #[derive(Default)]
    struct FakeBootstrapApi {
        duplicate_positions: bool,
        hedge: Option<(f64, f64)>,
        empty_positions: bool,
    }

    #[async_trait::async_trait]
    impl AccountBootstrapApi for FakeBootstrapApi {
        async fn position_mode(&self) -> Result<crate::api::ApiPositionMode, crate::api::ApiError> {
            Ok(if self.hedge.is_some() {
                crate::api::ApiPositionMode::Hedge
            } else {
                crate::api::ApiPositionMode::Net
            })
        }
        async fn validate(&self) -> Result<(), crate::api::ApiError> {
            Ok(())
        }

        async fn open_orders(&self, symbol: &str) -> Result<Vec<OrderInfo>, crate::api::ApiError> {
            Ok(vec![OrderInfo {
                symbol: symbol.to_owned(),
                order_id: "venue-1".into(),
                client_order_id: "client-1".into(),
                side: ApiSide::Buy,
                order_type: ApiOrderType::Limit,
                status: ApiOrderStatus::New,
                price: 100.0,
                qty: 2.0,
                executed_qty: 0.0,
                avg_price: 0.0,
                leaves_qty: 2.0,
                time_in_force: ApiTimeInForce::GTC,
                reduce_only: true,
                position_side: ApiPositionSide::Net,
                create_time: 10,
                update_time: 11,
                stop_price: None,
            }])
        }

        async fn positions(&self) -> Result<Vec<PositionInfo>, crate::api::ApiError> {
            if self.empty_positions {
                return Ok(Vec::new());
            }
            let mut values = vec![PositionInfo {
                symbol: "BTCUSDT".into(),
                position_side: ApiPositionSide::Net,
                qty: 1.25,
                entry_price: 100.0,
                mark_price: 101.0,
                liquidation_price: 50.0,
                leverage: 2.0,
                margin_type: ApiMarginType::Cross,
                unrealized_pnl: 1.5,
                realized_pnl: 0.5,
                notional: 126.25,
                update_time: 12,
            }];
            if let Some((long, short)) = self.hedge {
                values[0].position_side = ApiPositionSide::Long;
                values[0].qty = long;
                let mut second = values[0].clone();
                second.position_side = ApiPositionSide::Short;
                second.qty = short;
                second.entry_price = 110.0;
                values.push(second);
            }
            if self.duplicate_positions {
                values.push(values[0].clone());
            }
            Ok(values)
        }

        async fn account(&self) -> Result<AccountInfo, crate::api::ApiError> {
            Ok(AccountInfo {
                total_wallet_balance: 1_000.0,
                total_margin_balance: 1_001.5,
                total_unrealized_pnl: 1.5,
                available_balance: 900.0,
                balances: vec![Balance {
                    asset: "USDT".into(),
                    wallet_balance: 1_000.0,
                    available_balance: 900.0,
                    unrealized_pnl: 1.5,
                    margin_balance: 1_001.5,
                }],
                timestamp: 13,
            })
        }
        async fn order(
            &self,
            symbol: &str,
            client: &str,
        ) -> Result<OrderInfo, crate::api::ApiError> {
            let mut order = self.open_orders(symbol).await?.remove(0);
            order.client_order_id = client.into();
            order.order_id = "missing-terminal".into();
            order.status = ApiOrderStatus::Canceled;
            order.executed_qty = 1.0;
            Ok(order)
        }
    }

    #[test]
    fn private_position_publication_is_encoded_as_an_account_fact() {
        let account_handle = account::AccountHandle {
            account_id: account::AccountId(7),
            generation: 3,
        };
        let sink = Arc::new(RecordingSink::default());
        let resources = ResourceScope::new(ComponentIdentity::new("connector", "account-test"));
        let secret_ref = account::SecretRef::new("secret://test/account");
        let context = account::AccountConnectorContext {
            account: account_handle,
            instruments: vec![account::AccountInstrumentBinding {
                native_symbol: Arc::from("BTCUSDT"),
                asset_id: account::AssetId(42),
                price_tick: account::DecimalUnit::new(1, 1).unwrap(),
                quantity_lot: account::DecimalUnit::new(1, 2).unwrap(),
                contract_multiplier: account::DecimalUnit::new(1, 0).unwrap(),
            }]
            .into(),
            currencies: Vec::new().into(),
            ownership: account::OrderOwnershipPolicy::ObserveAll,
            account_stream: account::SourceStreamId(11),
            control_stream: account::SourceStreamId(12),
            event_publisher: account::AccountEventPublisher::from_sink(
                account_handle,
                sink.clone(),
            ),
            resources: resources.handle(),
            secrets: account::ScopedSecretResolver::scoped(
                secret_ref,
                Arc::new(account::UnavailableSecretProvider),
            ),
        };
        let encoder = AccountEventEncoder {
            context,
            epoch: Arc::new(AtomicU64::new(5)),
            version: Arc::new(AtomicU64::new(0)),
            ready: Arc::new(AtomicBool::new(true)),
            ids: Arc::new(Mutex::new(IdInterner::default())),
            projection: Arc::new(Mutex::new(AccountProjection::default())),
            publication: Mutex::new(()),
        };

        encoder
            .publish(&AccountPublication::Position {
                entry_price: None,
                unrealized_pnl: None,
                liquidation_price: None,
                position_side: crate::api::ApiPositionSide::Net,
                symbol: "btcusdt".to_owned(),
                qty: -1.25,
                exch_ts: 123,
            })
            .unwrap();

        let events = sink
            .events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, account::POSITION_CHANGED_EVENT);
        let position = account::PositionChangedV1::decode(&events[0].1).unwrap();
        assert_eq!(position.header.account_id, 7);
        assert_eq!(position.header.account_generation, 3);
        assert_eq!(position.header.account_epoch, 5);
        assert_eq!(position.asset_id, 42);
        assert_eq!(position.position_side, ACCOUNT_POSITION_SIDE_NET);
        assert_eq!(position.quantity_lots, -125);
        let (_, cached) = encoder
            .projection
            .lock()
            .unwrap()
            .positions(account::PositionFilter::default());
        assert_eq!(cached.len(), 1);
        assert_eq!(cached[0].quantity_lots, -125);
        drop(events);
        sink.reject.store(true, Ordering::Release);
        let version = encoder
            .projection
            .lock()
            .unwrap()
            .positions(account::PositionFilter::default())
            .0;
        assert!(
            encoder
                .publish(&AccountPublication::Position {
                    entry_price: None,
                    unrealized_pnl: None,
                    liquidation_price: None,
                    position_side: crate::api::ApiPositionSide::Net,
                    symbol: "BTCUSDT".into(),
                    qty: -2.0,
                    exch_ts: 124
                })
                .is_err()
        );
        let (after, cached) = encoder
            .projection
            .lock()
            .unwrap()
            .positions(account::PositionFilter::default());
        assert_eq!(after, version);
        assert_eq!(cached[0].quantity_lots, -125);
    }

    #[test]
    fn managed_orders_use_scoped_sequence_and_rejection_keeps_canonical_facts() {
        let mut identities = std::collections::HashSet::new();
        let binding = account::AccountInstrumentBinding {
            native_symbol: Arc::from("BTCUSDT"),
            asset_id: account::AssetId(42),
            price_tick: account::DecimalUnit::new(1, 1).unwrap(),
            quantity_lot: account::DecimalUnit::new(1, 2).unwrap(),
            contract_multiplier: account::DecimalUnit::new(1, 0).unwrap(),
        };
        for sequence in [7_u64, 8] {
            let mut id = [9_u8; 16];
            id[8..].copy_from_slice(&sequence.to_le_bytes());
            let request = account::DirectNewOrderRequest {
                position_side: 0,
                asset_id: binding.asset_id,
                side: 1,
                order_type: 0,
                time_in_force: 0,
                price_ticks: 1000,
                quantity_lots: 2,
                reduce_only: false,
                client_order_id: account::Id128(id),
            };
            let mut order = managed_account_order(&request, &binding).unwrap();
            assert_eq!(
                order.order_id,
                managed_account_order(&request, &binding).unwrap().order_id
            );
            assert!(identities.insert(order.order_id));
            let mut other = request.clone();
            other.client_order_id.0[0] ^= 1;
            assert!(identities.insert(managed_account_order(&other, &binding).unwrap().order_id));
            crate::utils::reject_pending_order(&mut order);
            assert_eq!(order.status, Status::Rejected);
            order.status = Status::New;
            order.exch_timestamp = 100;
            crate::utils::reject_pending_order(&mut order);
            assert_eq!(order.status, Status::New);
            assert_eq!(order.exch_timestamp, 100);
        }
    }

    #[test]
    fn id_retention_follows_active_and_retained_terminal_orders() {
        let mut ids = IdInterner::default();
        let retained = ids.intern("active-client");
        for index in 0..5000 {
            ids.intern(&format!("historical-order-{index}"));
        }
        let mut image = AccountProjection::default();
        image.order(&account::OrderChangedV1 {
            client_order_id: retained,
            venue_order_id: retained,
            status: 1,
            ..Default::default()
        });
        ids.prune(&image);
        assert_eq!(ids.by_id.len(), 1);
        assert_eq!(ids.resolve(retained), "active-client");
    }

    #[tokio::test]
    async fn bootstrap_validates_populates_caches_and_publishes_ready_last() {
        let account_handle = account::AccountHandle {
            account_id: account::AccountId(8),
            generation: 1,
        };
        let sink = Arc::new(RecordingSink::default());
        let resources = ResourceScope::new(ComponentIdentity::new("connector", "bootstrap-test"));
        let secret_ref = account::SecretRef::new("secret://test/bootstrap");
        let context = account::AccountConnectorContext {
            account: account_handle,
            instruments: vec![account::AccountInstrumentBinding {
                native_symbol: Arc::from("BTCUSDT"),
                asset_id: account::AssetId(42),
                price_tick: account::DecimalUnit::new(1, 1).unwrap(),
                quantity_lot: account::DecimalUnit::new(1, 2).unwrap(),
                contract_multiplier: account::DecimalUnit::new(1, 0).unwrap(),
            }]
            .into(),
            currencies: vec![account::AccountCurrencyBinding {
                native_currency: Arc::from("USDT"),
                currency_id: account::CurrencyId(9),
                amount_unit: account::DecimalUnit::new(1, 2).unwrap(),
            }]
            .into(),
            ownership: account::OrderOwnershipPolicy::ObserveAll,
            account_stream: account::SourceStreamId(11),
            control_stream: account::SourceStreamId(12),
            event_publisher: account::AccountEventPublisher::from_sink(
                account_handle,
                sink.clone(),
            ),
            resources: resources.handle(),
            secrets: account::ScopedSecretResolver::scoped(
                secret_ref,
                Arc::new(account::UnavailableSecretProvider),
            ),
        };
        for (hedge, expected_sides) in [(None, vec![0]), (Some((0.0, 0.0)), vec![1, 2])] {
            let snapshot = load_account_bootstrap(
                &context,
                &FakeBootstrapApi {
                    hedge,
                    empty_positions: true,
                    ..Default::default()
                },
                &Mutex::new(IdInterner::default()),
            )
            .await
            .unwrap();
            assert_eq!(
                snapshot
                    .positions
                    .iter()
                    .map(|(p, _)| p.position_side)
                    .collect::<Vec<_>>(),
                expected_sides
            );
            assert!(
                snapshot
                    .positions
                    .iter()
                    .all(|(p, time)| p.quantity_lots == 0 && *time == 0)
            );
        }
        let ready = Arc::new(AtomicBool::new(true));
        let active = Arc::new(AtomicBool::new(true));
        let epoch = Arc::new(AtomicU64::new(1));
        let fence = Arc::new(ReadinessFence::default());
        let capture = || {
            SubmitSession {
                ready: ready.clone(),
                active: active.clone(),
                committed: Arc::new(AtomicBool::new(true)),
                fence: fence.clone(),
                epoch: epoch.clone(),
                publisher: context.event_publisher.clone(),
            }
            .capture()
        };
        let old = capture();
        old().unwrap();
        // A future made while Ready cannot dispatch after stop or a complete reconnect.
        active.store(false, Ordering::Release);
        assert_eq!(old().unwrap_err().code, "STALE_ACCOUNT_SESSION");
        active.store(true, Ordering::Release);
        let revision = fence.changed();
        fence.commit(revision);
        assert!(old().is_err());
        let old_epoch = capture();
        epoch.fetch_add(1, Ordering::AcqRel);
        assert!(old_epoch().is_err());
        // The same guard is carried across asynchronous venue preparation.
        let prepared = capture();
        let preparation = async {
            tokio::task::yield_now().await;
            crate::api::check_submit_guard(&Some(prepared))
        };
        ready.store(false, Ordering::Release);
        assert_eq!(preparation.await.unwrap_err().code, "STALE_ACCOUNT_SESSION");

        let encoder = AccountEventEncoder {
            context: context.clone(),
            epoch: Arc::new(AtomicU64::new(1)),
            version: Arc::new(AtomicU64::new(0)),
            ready: Arc::new(AtomicBool::new(false)),
            ids: Arc::new(Mutex::new(IdInterner::default())),
            projection: Arc::new(Mutex::new(AccountProjection::default())),
            publication: Mutex::new(()),
        };

        let snapshot = load_account_bootstrap(&context, &FakeBootstrapApi::default(), &encoder.ids)
            .await
            .unwrap();
        encoder.install_bootstrap(snapshot).unwrap();
        encoder.publish_ready().unwrap();

        assert!(encoder.ready.load(Ordering::Acquire));
        let (_, orders) = encoder
            .projection
            .lock()
            .unwrap()
            .orders(account::OrderFilter {
                asset_id: None,
                include_final: true,
            });
        assert_eq!(orders.len(), 1);
        assert!(orders[0].reduce_only);
        let mut missing = orders[0].clone();
        missing.client_order_id = encoder.ids.lock().unwrap().intern("missing-open");
        let reconciled = reconcile_account_bootstrap(
            &context,
            &FakeBootstrapApi::default(),
            &encoder.ids,
            &[missing],
        )
        .await
        .unwrap();
        assert_eq!(reconciled.orders.len(), 2);
        assert_eq!(
            reconciled.orders[1].0.status,
            api_status(ApiOrderStatus::Canceled)
        );
        assert_eq!(reconciled.orders[1].0.filled_quantity_lots, 100);
        drop(orders);
        assert_eq!(
            encoder
                .projection
                .lock()
                .unwrap()
                .positions(account::PositionFilter::default())
                .1[0]
                .quantity_lots,
            125
        );
        assert_eq!(
            encoder.projection.lock().unwrap().balances().1[0].wallet_units,
            100_000
        );
        {
            let events = sink.events.lock().unwrap();
            assert_eq!(events.len(), 4);
            let order_payload = events
                .iter()
                .find(|(event_type, _)| event_type == account::ORDER_CHANGED_EVENT)
                .map(|(_, payload)| payload)
                .unwrap();
            let order = account::OrderChangedV1::decode(order_payload).unwrap();
            assert_ne!(order.header.flags & account::event_flags::REDUCE_ONLY, 0);
            assert_eq!(
                events.last().unwrap().0,
                account::STREAM_STATE_CHANGED_EVENT
            );
        }
        #[cfg(feature = "hyperliquid")]
        {
            struct NoNetworkConnector;
            #[async_trait::async_trait]
            impl Connector for NoNetworkConnector {
                fn register(&mut self, _: String) {}
                fn order_manager(
                    &self,
                ) -> Arc<Mutex<dyn crate::connector::GetOrders + Send + 'static>> {
                    panic!("unused")
                }
                fn run(&mut self, _: crate::connector::PublishSender) {
                    panic!("no network allowed")
                }
                fn shutdown(&self) -> crate::connector::ShutdownFuture {
                    Box::pin(async { Ok(()) })
                }
            }
            let runtime = AccountRuntime::new(
                Box::new(NoNetworkConnector),
                Arc::new(crate::hyperliquid::client::HyperliquidClient::new(
                    "http://localhost",
                    "http://localhost",
                )),
                account::ShutdownOrderPolicy::LeaveOpen,
                context.clone(),
            );
            *runtime.projection.lock().unwrap() = encoder.projection.lock().unwrap().clone();
            runtime.ready.store(true, Ordering::Release);
            {
                let projection = runtime.projection.lock().unwrap();
                assert_ne!(
                    projection.orders(account::OrderFilter::default()).0,
                    projection.positions(account::PositionFilter::default()).0
                );
            }

            let snapshot = account::AccountConnector::recovery_snapshot(runtime.as_ref()).unwrap();
            assert_eq!(
                snapshot.orders.committed_version,
                snapshot.positions.committed_version
            );
            assert_eq!(
                snapshot.orders.committed_version,
                snapshot.balances.committed_version
            );
            assert!(!snapshot.orders.items.is_empty());
            assert!(!snapshot.positions.items.is_empty());
            assert!(!snapshot.balances.items.is_empty());
            runtime.active.store(true, Ordering::Release);
            runtime.epoch.store(1, Ordering::Release);
            let request = account::DirectNewOrderRequest {
                asset_id: account::AssetId(42),
                side: 1,
                order_type: 0,
                time_in_force: 0,
                price_ticks: 1000,
                quantity_lots: 1,
                reduce_only: false,
                position_side: 0,
                client_order_id: account::Id128([7; 16]),
            };
            let deferred =
                account::AccountConnector::direct_submit(runtime.as_ref(), request.clone());
            runtime.active.store(false, Ordering::Release);
            assert_eq!(deferred.await.unwrap_err().code, "STALE_ACCOUNT_SESSION");
            runtime.active.store(true, Ordering::Release);
            let deferred = account::AccountConnector::direct_submit(runtime.as_ref(), request);
            let revision = runtime.readiness_fence.changed();
            runtime.readiness_fence.commit(revision);
            assert_eq!(deferred.await.unwrap_err().code, "STALE_ACCOUNT_SESSION");
        }
        let previous_available = encoder.projection.lock().unwrap().balances().1[0].available_units;
        encoder
            .publish(&AccountPublication::Balance {
                currency: "USDT".into(),
                wallet: 900.0,
                available: None,
                margin: None,
                unrealized_pnl: None,
                exch_ts: now_ns(),
            })
            .unwrap();
        let balances = encoder.projection.lock().unwrap().balances().1;
        assert_eq!(balances[0].wallet_units, 90_000);
        assert_eq!(balances[0].available_units, previous_available);
        encoder
            .publish(&AccountPublication::Position {
                entry_price: None,
                unrealized_pnl: None,
                liquidation_price: None,
                position_side: crate::api::ApiPositionSide::Net,
                symbol: "BTCUSDT".into(),
                qty: 2.0,
                exch_ts: now_ns(),
            })
            .unwrap();
        let position = encoder
            .projection
            .lock()
            .unwrap()
            .positions(account::PositionFilter::default())
            .1[0]
            .clone();
        assert_eq!(position.quantity_lots, 200);
        assert_eq!(position.entry_price_ticks, 1000);
        assert_eq!(position.margin_currency_id, account::CurrencyId(9));
        assert_eq!(position.position_side, ACCOUNT_POSITION_SIDE_NET);
        assert!(
            load_account_bootstrap(
                &context,
                &FakeBootstrapApi {
                    duplicate_positions: true,
                    ..Default::default()
                },
                &encoder.ids
            )
            .await
            .is_err()
        );

        for quantities in [(2.0, -1.0), (0.0, 0.0), (2.0, 0.0)] {
            let snapshot = load_account_bootstrap(
                &context,
                &FakeBootstrapApi {
                    hedge: Some(quantities),
                    ..Default::default()
                },
                &encoder.ids,
            )
            .await
            .unwrap();
            assert_eq!(snapshot.positions.len(), 2);
            assert_eq!(snapshot.positions[0].0.position_side, 1);
            assert_eq!(snapshot.positions[1].0.position_side, 2);
            assert_eq!(
                snapshot.positions[0].0.quantity_lots,
                (quantities.0 * 100.0) as i64
            );
            assert_eq!(
                snapshot.positions[1].0.quantity_lots,
                (quantities.1 * 100.0) as i64
            );
            assert_eq!(snapshot.positions[1].0.entry_price_ticks, 1100);
            encoder.install_bootstrap(snapshot).unwrap();
            encoder
                .publish(&AccountPublication::Position {
                    position_side: ApiPositionSide::Long,
                    entry_price: Some(101.0),
                    unrealized_pnl: Some(0.0),
                    liquidation_price: None,
                    symbol: "BTCUSDT".into(),
                    qty: 0.0,
                    exch_ts: now_ns(),
                })
                .unwrap();
            let image = encoder.projection.lock().unwrap();
            let (_, positions) = image.positions(account::PositionFilter::default());
            assert_eq!(positions.len(), 2);
            assert_eq!(positions[0].quantity_lots, 0);
            assert_eq!(positions[1].quantity_lots, (quantities.1 * 100.0) as i64);
            assert_eq!(positions[1].entry_price_ticks, 1100);
        }

        // An empty REST hedge image must accept buffered facts with genuine older
        // exchange times; a locally fabricated bootstrap timestamp would drop them.
        encoder
            .install_bootstrap(
                load_account_bootstrap(
                    &context,
                    &FakeBootstrapApi {
                        hedge: Some((0.0, 0.0)),
                        empty_positions: true,
                        ..Default::default()
                    },
                    &encoder.ids,
                )
                .await
                .unwrap(),
            )
            .unwrap();
        for (position_side, qty) in [(ApiPositionSide::Long, 2.0), (ApiPositionSide::Short, -1.0)] {
            encoder
                .publish(&AccountPublication::Position {
                    position_side,
                    entry_price: Some(100.0),
                    unrealized_pnl: Some(0.0),
                    liquidation_price: Some(0.0),
                    symbol: "BTCUSDT".into(),
                    qty,
                    exch_ts: 1,
                })
                .unwrap();
        }
        {
            let image = encoder.projection.lock().unwrap();
            let (_, positions) = image.positions(account::PositionFilter::default());
            assert_eq!(
                positions
                    .iter()
                    .map(|p| p.quantity_lots)
                    .collect::<Vec<_>>(),
                vec![200, -100]
            );
        }

        // Candidate preparation must not require external publication to be open.
        context.event_publisher.close();
        let emitted = sink.events.lock().unwrap().len();
        encoder.ready.store(false, Ordering::Release);
        encoder
            .install_bootstrap(
                load_account_bootstrap(&context, &FakeBootstrapApi::default(), &encoder.ids)
                    .await
                    .unwrap(),
            )
            .unwrap();
        encoder.publish_ready().unwrap();
        assert!(encoder.ready.load(Ordering::Acquire));
        assert_eq!(sink.events.lock().unwrap().len(), emitted);
    }

    #[test]
    fn pending_account_facts_have_a_hard_limit_and_preserve_order() {
        let mut pending = PendingAccountFacts::new(2);
        for qty in [1.0, 2.0] {
            assert!(pending.push(AccountPublication::Position {
                entry_price: None,
                unrealized_pnl: None,
                liquidation_price: None,
                position_side: crate::api::ApiPositionSide::Net,
                symbol: "BTC".into(),
                qty,
                exch_ts: 0
            }));
        }
        assert!(!pending.push(AccountPublication::Position {
            entry_price: None,
            unrealized_pnl: None,
            liquidation_price: None,
            position_side: crate::api::ApiPositionSide::Net,
            symbol: "BTC".into(),
            qty: 3.0,
            exch_ts: 0
        }));
        let quantities: Vec<_> = pending
            .drain()
            .map(|event| match event {
                AccountPublication::Position { qty, .. } => qty,
                _ => panic!("unexpected buffered fact"),
            })
            .collect();
        assert_eq!(quantities, [1.0, 2.0]);
        assert!(pending.push(AccountPublication::Position {
            entry_price: None,
            unrealized_pnl: None,
            liquidation_price: None,
            position_side: crate::api::ApiPositionSide::Net,
            symbol: "BTC".into(),
            qty: 4.0,
            exch_ts: 0
        }));
    }

    #[test]
    fn a_new_connection_invalidates_a_late_ready_commit_from_the_previous_session() {
        let fence = ReadinessFence::default();
        let first = fence.changed();
        assert!(fence.commit(first));
        let next = fence.changed();
        assert!(!fence.matches());
        assert!(!fence.commit(first));
        assert!(fence.commit(next));
        fence.changed();
        assert!(!fence.matches());
    }

    #[tokio::test]
    async fn an_uncommitted_replacement_never_cancels_the_existing_accounts_orders() {
        struct Actions(AtomicU64);
        #[async_trait::async_trait(?Send)]
        impl ShutdownActions for Actions {
            async fn cancel_all(&self) -> Result<(), String> {
                self.0.fetch_add(1, Ordering::AcqRel);
                Ok(())
            }
            async fn cancel_all_after(&self, _: u64) -> Result<(), String> {
                self.0.fetch_add(1, Ordering::AcqRel);
                Ok(())
            }
        }
        let actions = Actions(AtomicU64::new(0));
        for policy in [
            account::ShutdownOrderPolicy::CancelAll,
            account::ShutdownOrderPolicy::CancelAllAfter { timeout_ms: 1000 },
        ] {
            execute_shutdown_policy(
                &actions,
                &policy,
                Instant::now() + Duration::from_secs(1),
                false,
            )
            .await
            .unwrap();
        }
        assert_eq!(actions.0.load(Ordering::Acquire), 0);
        execute_shutdown_policy(
            &actions,
            &account::ShutdownOrderPolicy::CancelAll,
            Instant::now() + Duration::from_secs(1),
            true,
        )
        .await
        .unwrap();
        assert_eq!(actions.0.load(Ordering::Acquire), 1);
    }
    #[test]
    fn account_facts_keep_incremental_price_average_and_settlement_units_distinct() {
        let handle = account::AccountHandle {
            account_id: account::AccountId(8),
            generation: 1,
        };
        let sink = Arc::new(RecordingSink::default());
        let resources = ResourceScope::new(ComponentIdentity::new(
            "connector",
            "execution-currency-test",
        ));
        let context = account::AccountConnectorContext {
            account: handle,
            instruments: vec![account::AccountInstrumentBinding {
                native_symbol: Arc::from("BTCUSDC"),
                asset_id: account::AssetId(42),
                price_tick: account::DecimalUnit::new(1, 1).unwrap(),
                quantity_lot: account::DecimalUnit::new(1, 2).unwrap(),
                contract_multiplier: account::DecimalUnit::new(1, 0).unwrap(),
            }]
            .into(),
            currencies: vec![
                account::AccountCurrencyBinding {
                    native_currency: Arc::from("USDT"),
                    currency_id: account::CurrencyId(9),
                    amount_unit: account::DecimalUnit::new(1, 2).unwrap(),
                },
                account::AccountCurrencyBinding {
                    native_currency: Arc::from("USDC"),
                    currency_id: account::CurrencyId(10),
                    amount_unit: account::DecimalUnit::new(1, 3).unwrap(),
                },
            ]
            .into(),
            ownership: account::OrderOwnershipPolicy::ObserveAll,
            account_stream: account::SourceStreamId(11),
            control_stream: account::SourceStreamId(12),
            event_publisher: account::AccountEventPublisher::from_sink(handle, sink.clone()),
            resources: resources.handle(),
            secrets: account::ScopedSecretResolver::scoped(
                account::SecretRef::new("secret://test/account"),
                Arc::new(account::UnavailableSecretProvider),
            ),
        };
        assert_eq!(
            position_currency_binding(&context, Some("USDC"))
                .unwrap()
                .currency_id,
            account::CurrencyId(10)
        );
        assert!(position_currency_binding(&context, Some("JPY")).is_err());
        let encoder = AccountEventEncoder {
            context,
            epoch: Arc::new(AtomicU64::new(1)),
            version: Arc::new(AtomicU64::new(0)),
            ready: Arc::new(AtomicBool::new(true)),
            ids: Arc::new(Mutex::new(IdInterner::default())),
            projection: Arc::new(Mutex::new(AccountProjection::default())),
            publication: Mutex::new(()),
        };
        let mut order = Order::new(
            1,
            1000,
            0.1,
            2.0,
            Side::Buy,
            OrdType::Limit,
            TimeInForce::GTC,
        );
        order.status = Status::PartiallyFilled;
        order.leaves_qty = 1.0;
        order.exec_qty = 1.0;
        order.exec_price_tick = 1000;
        order.exch_timestamp = 100;
        encoder
            .publish(&AccountPublication::OrderWithExecution {
                position_side: ApiPositionSide::Net,
                symbol: "BTCUSDC".into(),
                client_order_id: Some("client".into()),
                venue_order_id: Some("venue".into()),
                order: order.clone(),
                average_price: 100.0,
            })
            .unwrap();
        order.status = Status::Filled;
        order.leaves_qty = 0.0;
        order.exec_price_tick = 2000;
        order.exch_timestamp = 200;
        encoder
            .publish(&AccountPublication::OrderWithExecution {
                position_side: ApiPositionSide::Net,
                symbol: "BTCUSDC".into(),
                client_order_id: Some("client".into()),
                venue_order_id: Some("venue".into()),
                order,
                average_price: 150.0,
            })
            .unwrap();
        let events = sink.events.lock().unwrap();
        let changes: Vec<_> = events
            .iter()
            .filter(|(t, _)| t == account::ORDER_CHANGED_EVENT)
            .map(|(_, v)| account::OrderChangedV1::decode(v).unwrap())
            .collect();
        assert_eq!(changes[1].average_price_ticks, 1500);
        let fills: Vec<_> = events
            .iter()
            .filter(|(t, _)| t == account::FILL_EVENT)
            .map(|(_, v)| account::FillV2::decode(v).unwrap())
            .collect();
        assert_eq!(fills.len(), 2);
        assert_eq!(fills[1].price_ticks, 2000);
        assert_eq!(fills[1].last_fill_quantity_lots, 100);
        assert_eq!(fills[1].cumulative_filled_quantity_lots, 200);
        drop(events);
        encoder.cache_position(&account::PositionChangedV1 {
            asset_id: 42,
            position_side: 0,
            margin_currency_id: 10,
            ..Default::default()
        });
        encoder
            .publish(&AccountPublication::Position {
                symbol: "BTCUSDC".into(),
                position_side: ApiPositionSide::Net,
                qty: 1.0,
                entry_price: None,
                unrealized_pnl: Some(0.03),
                liquidation_price: None,
                exch_ts: 300,
            })
            .unwrap();
        let events = sink.events.lock().unwrap();
        let position = account::PositionChangedV1::decode(&events.last().unwrap().1).unwrap();
        assert_eq!(position.margin_currency_id, 10);
        assert_eq!(position.unrealized_pnl_units, 30);
    }
}
