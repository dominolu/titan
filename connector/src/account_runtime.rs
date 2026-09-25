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
use tokio::sync::{Notify, oneshot};

use crate::{
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

struct Running {
    stop: Option<oneshot::Sender<Instant>>,
    thread: Option<JoinHandle<()>>,
    shutdown_result: Arc<Mutex<Option<Result<(), account::AccountConnectorError>>>>,
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
    async fn account(&self) -> Result<AccountInfo, crate::api::ApiError>;
}

#[async_trait::async_trait]
impl<T: BrokerApi + ?Sized> AccountBootstrapApi for T {
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
}

struct VenueShutdownActions {
    connector: Arc<Mutex<Box<dyn Connector>>>,
    api: Arc<dyn BrokerApi>,
}

#[async_trait::async_trait(?Send)]
impl ShutdownActions for VenueShutdownActions {
    async fn cancel_all(&self) -> Result<(), String> {
        let connector = self.connector.lock().unwrap_or_else(|p| p.into_inner());
        connector.shutdown().await
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
) -> Result<(), account::AccountConnectorError> {
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
}

struct AccountRuntime {
    context: account::AccountConnectorContext,
    shutdown_policy: account::ShutdownOrderPolicy,
    connector: Arc<Mutex<Box<dyn Connector>>>,
    api: Arc<dyn BrokerApi>,
    running: Mutex<Option<Running>>,
    started: AtomicBool,
    active: AtomicBool,
    ready: Arc<AtomicBool>,
    epoch: Arc<AtomicU64>,
    version: Arc<AtomicU64>,
    orders: Arc<Mutex<Arc<[account::OrderSnapshot]>>>,
    positions: Arc<Mutex<Arc<[account::PositionSnapshot]>>>,
    balances: Arc<Mutex<Arc<[account::BalanceSnapshot]>>>,
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
        let value = Arc::new(Self {
            context,
            shutdown_policy,
            connector: Arc::new(Mutex::new(connector)),
            api,
            running: Mutex::new(None),
            started: AtomicBool::new(false),
            active: AtomicBool::new(false),
            ready: Arc::new(AtomicBool::new(false)),
            epoch: Arc::new(AtomicU64::new(0)),
            version: Arc::new(AtomicU64::new(0)),
            orders: Arc::new(Mutex::new(Arc::from([]))),
            positions: Arc::new(Mutex::new(Arc::from([]))),
            balances: Arc::new(Mutex::new(Arc::from([]))),
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
    fn snapshot<T>(&self, items: Arc<[T]>) -> account::AccountStateSnapshot<T> {
        account::AccountStateSnapshot {
            account: self.context.account,
            state: if self.ready.load(Ordering::Acquire) {
                account::AccountSnapshotState::Ready
            } else if self.active.load(Ordering::Acquire) {
                account::AccountSnapshotState::Invalidated
            } else {
                account::AccountSnapshotState::Stopped
            },
            committed_epoch: (self.epoch.load(Ordering::Acquire) > 0)
                .then(|| self.epoch.load(Ordering::Acquire)),
            committed_version: (self.version.load(Ordering::Acquire) > 0)
                .then(|| self.version.load(Ordering::Acquire)),
            captured_at: now_ns(),
            items,
        }
    }
}

fn close_weak(weak: &Weak<AccountRuntime>) -> Result<(), titan_core_types::CoreError> {
    if let Some(runtime) = weak.upgrade() {
        let _ = runtime.stop_inner(Instant::now() + Duration::from_secs(1));
    }
    Ok(())
}

impl account::AccountConnector for AccountRuntime {
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
        let orders = self.orders.clone();
        let positions = self.positions.clone();
        let balances = self.balances.clone();
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
                        fill_cumulative: Mutex::new(HashMap::new()),
                        orders: orders.clone(),
                        positions: positions.clone(),
                        balances: balances.clone(),
                    });
                    let stream_ready = Arc::new(AtomicBool::new(false));
                    let stream_ready_notify = Arc::new(Notify::new());
                    let bootstrapped = Arc::new(AtomicBool::new(false));
                    let pending = Arc::new(Mutex::new(Vec::<AccountPublication>::new()));
                    let account_events = encoder.clone();
                    let callback_context = context.clone();
                    let callback_ready = stream_ready.clone();
                    let callback_notify = stream_ready_notify.clone();
                    let callback_bootstrapped = bootstrapped.clone();
                    let callback_pending = pending.clone();
                    let tx = direct_publish_sender(move |publication| match publication {
                        DirectPublication::Event(PublishEvent::PrivateStreamReady) => {
                            callback_ready.store(true, Ordering::Release);
                            callback_notify.notify_waiters();
                            if callback_bootstrapped.load(Ordering::Acquire)
                                && let Err(error) = account_events.publish_ready()
                            {
                                tracing::warn!(
                                    account_id = callback_context.account.account_id.0,
                                    ?error,
                                    "Account ready publication failed."
                                );
                                account_events.invalidate(
                                    account::invalidation_reason::DIRECT_FACT_PUBLICATION,
                                );
                            }
                        }
                        DirectPublication::Account(AccountPublication::Error(error)) => {
                            callback_ready.store(false, Ordering::Release);
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
                                    pending.push(event.clone());
                                    return;
                                }
                            }
                            if let Err(error) = account_events.publish(event) {
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
                    let bootstrap = async {
                        let (snapshot, ()) = tokio::try_join!(
                            load_account_bootstrap(&context, api.as_ref(), ids.as_ref()),
                            wait_for_private_stream(
                                stream_ready.as_ref(),
                                stream_ready_notify.as_ref(),
                                ACCOUNT_BOOTSTRAP_TIMEOUT,
                            ),
                        )?;
                        let mut pending = pending
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        encoder.install_bootstrap(snapshot)?;
                        for event in pending.drain(..) {
                            encoder.publish(&event)?;
                        }
                        bootstrapped.store(true, Ordering::Release);
                        encoder.publish_ready()
                    };
                    tokio::pin!(bootstrap);
                    let stopped_during_bootstrap = tokio::select! {
                        result = &mut bootstrap => {
                            if let Err(error) = result {
                                *startup_error.lock().unwrap_or_else(|p| p.into_inner()) =
                                    Some(error.to_string());
                                encoder.invalidate(account::invalidation_reason::BOOTSTRAP);
                            } else {
                                *startup_error.lock().unwrap_or_else(|p| p.into_inner()) = None;
                            }
                            None
                        }
                        stop = &mut stop_rx => Some(stop),
                    };
                    let stop = match stopped_during_bootstrap {
                        Some(stop) => stop,
                        None => (&mut stop_rx).await,
                    };
                    let result = match stop {
                        Err(_) => Err(account::AccountConnectorError::new(
                            account::AccountErrorKind::ResourceReleaseFailed,
                            "account stop signal was dropped",
                        )),
                        Ok(deadline) => {
                            execute_shutdown_policy(
                                &VenueShutdownActions {
                                    connector: connector.clone(),
                                    api: api.clone(),
                                },
                                &shutdown_policy,
                                deadline,
                            )
                            .await
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
        });
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
        let values = self.orders.lock().unwrap_or_else(|p| p.into_inner());
        let items: Arc<[_]> = values
            .iter()
            .filter(|order| {
                filter.asset_id.is_none_or(|asset| asset == order.asset_id)
                    && (filter.include_final || matches!(order.status, 1 | 5))
            })
            .cloned()
            .collect::<Vec<_>>()
            .into();
        Ok(self.snapshot(items))
    }
    fn positions(
        &self,
        filter: account::PositionFilter,
    ) -> Result<
        account::AccountStateSnapshot<account::PositionSnapshot>,
        account::AccountConnectorError,
    > {
        let values = self.positions.lock().unwrap_or_else(|p| p.into_inner());
        let items: Arc<[_]> = values
            .iter()
            .filter(|o| filter.asset_id.is_none_or(|a| a == o.asset_id))
            .cloned()
            .collect::<Vec<_>>()
            .into();
        Ok(self.snapshot(items))
    }
    fn balances(
        &self,
    ) -> Result<
        account::AccountStateSnapshot<account::BalanceSnapshot>,
        account::AccountConnectorError,
    > {
        Ok(self.snapshot(
            self.balances
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone(),
        ))
    }
    fn health(&self) -> account::AccountConnectorHealthSnapshot {
        let ready = self.ready.load(Ordering::Acquire);
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
        let api = self.api.clone();
        let connector = self.connector.clone();
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
                reduce_only: request.reduce_only,
                position_side: None,
                client_order_id: Some(client_order_id),
                stop_price: None,
            };
            api.submit_order(&wire)
                .await
                .map(direct_order_info)
                .map_err(direct_api_error)
        })
    }

    fn direct_cancel(
        &self,
        request: account::DirectCancelOrderRequest,
    ) -> account::ExecutionFuture {
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
            let _ = stop.send(deadline);
        }
        if let Some(thread) = running.thread.take() {
            while !thread.is_finished() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(1));
            }
            if !thread.is_finished() {
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
    let margin_currency_id = context
        .currencies
        .first()
        .ok_or_else(|| rejected("position margin currency binding missing"))?
        .currency_id;
    for binding in context.instruments.iter() {
        let mut matched = false;
        for value in values
            .iter()
            .filter(|value| value.symbol.eq_ignore_ascii_case(&binding.native_symbol))
        {
            matched = true;
            positions.push((
                position_snapshot(value, binding, context)?,
                value.update_time.saturating_mul(1_000_000),
            ));
        }
        if !matched {
            positions.push((
                account::PositionSnapshot {
                    asset_id: binding.asset_id,
                    position_side: ACCOUNT_POSITION_SIDE_NET,
                    margin_type: 2,
                    quantity_lots: 0,
                    entry_price_ticks: 0,
                    liquidation_price_ticks: 0,
                    realized_pnl_units: 0,
                    unrealized_pnl_units: 0,
                    margin_currency_id,
                },
                now_ns(),
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

struct AccountEventEncoder {
    context: account::AccountConnectorContext,
    epoch: Arc<AtomicU64>,
    version: Arc<AtomicU64>,
    ready: Arc<AtomicBool>,
    ids: Arc<Mutex<IdInterner>>,
    fill_cumulative: Mutex<HashMap<account::Id128, i64>>,
    orders: Arc<Mutex<Arc<[account::OrderSnapshot]>>>,
    positions: Arc<Mutex<Arc<[account::PositionSnapshot]>>>,
    balances: Arc<Mutex<Arc<[account::BalanceSnapshot]>>>,
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
        let event = account::StreamStateChangedV1(account::StreamStateV1 {
            header: self.header(account::event_kind::STREAM_STATE_CHANGED, 0, now_ns()),
            state: account::AccountLifecycle::Ready as u8,
            reason_code: 0,
        });
        self.context
            .event_publisher
            .publish_encoded(&event, TraceContext::default())
            .map_err(|error| rejected(error.to_string()))?;
        self.ready.store(true, Ordering::Release);
        Ok(())
    }

    fn install_bootstrap(
        &self,
        snapshot: AccountBootstrapSnapshot,
    ) -> Result<(), account::AccountConnectorError> {
        let mut order_cache = Vec::with_capacity(snapshot.orders.len());
        for (value, exchange_ts) in snapshot.orders {
            let event = order_event(
                self.header(
                    account::event_kind::ORDER_CHANGED,
                    account::event_flags::SNAPSHOT | account::event_flags::UPSERT,
                    exchange_ts,
                ),
                &value,
            );
            self.context
                .event_publisher
                .publish_encoded(&event, TraceContext::default())
                .map_err(|error| rejected(error.to_string()))?;
            order_cache.push(value);
        }
        let mut position_cache = Vec::with_capacity(snapshot.positions.len());
        for (value, exchange_ts) in snapshot.positions {
            let event = position_event(
                self.header(
                    account::event_kind::POSITION_CHANGED,
                    account::event_flags::SNAPSHOT | account::event_flags::UPSERT,
                    exchange_ts,
                ),
                &value,
            );
            self.context
                .event_publisher
                .publish_encoded(&event, TraceContext::default())
                .map_err(|error| rejected(error.to_string()))?;
            position_cache.push(value);
        }
        let mut balance_cache = Vec::with_capacity(snapshot.balances.len());
        for (value, exchange_ts) in snapshot.balances {
            let event = balance_event(
                self.header(
                    account::event_kind::BALANCE_CHANGED,
                    account::event_flags::SNAPSHOT | account::event_flags::UPSERT,
                    exchange_ts,
                ),
                &value,
            );
            self.context
                .event_publisher
                .publish_encoded(&event, TraceContext::default())
                .map_err(|error| rejected(error.to_string()))?;
            balance_cache.push(value);
        }
        *self.orders.lock().unwrap_or_else(|p| p.into_inner()) = order_cache.into();
        *self.positions.lock().unwrap_or_else(|p| p.into_inner()) = position_cache.into();
        *self.balances.lock().unwrap_or_else(|p| p.into_inner()) = balance_cache.into();
        Ok(())
    }

    fn cache_order(&self, event: &account::OrderChangedV1) {
        let mut cache = self.orders.lock().unwrap_or_else(|p| p.into_inner());
        let mut next = cache.to_vec();
        let mut snapshot = account::OrderSnapshot {
            asset_id: account::AssetId(event.asset_id),
            side: event.side,
            order_type: event.order_type,
            time_in_force: event.time_in_force,
            status: event.status,
            price_ticks: event.price_ticks,
            quantity_lots: event.quantity_lots,
            filled_quantity_lots: event.filled_quantity_lots,
            reduce_only: event.header.flags & account::event_flags::REDUCE_ONLY != 0,
            client_order_id: event.client_order_id,
            venue_order_id: event.venue_order_id,
            command_id: event.command_id,
        };
        if let Some(existing) = next.iter_mut().find(|value| {
            value.asset_id == snapshot.asset_id && value.venue_order_id == snapshot.venue_order_id
        }) {
            // Legacy connector order observations do not carry reduce-only. Preserve the value
            // learned from the REST/account snapshot instead of silently downgrading it.
            snapshot.reduce_only |= existing.reduce_only;
            *existing = snapshot;
        } else {
            next.push(snapshot);
        }
        *cache = next.into();
    }

    fn cache_position(&self, event: &account::PositionChangedV1) {
        let mut cache = self.positions.lock().unwrap_or_else(|p| p.into_inner());
        let mut next = cache.to_vec();
        let snapshot = account::PositionSnapshot {
            asset_id: account::AssetId(event.asset_id),
            position_side: event.position_side,
            margin_type: event.margin_type,
            quantity_lots: event.quantity_lots,
            entry_price_ticks: event.entry_price_ticks,
            liquidation_price_ticks: event.liquidation_price_ticks,
            realized_pnl_units: event.realized_pnl_units,
            unrealized_pnl_units: event.unrealized_pnl_units,
            margin_currency_id: account::CurrencyId(event.margin_currency_id),
        };
        if let Some(existing) = next.iter_mut().find(|value| {
            value.asset_id == snapshot.asset_id && value.position_side == snapshot.position_side
        }) {
            *existing = snapshot;
        } else {
            next.push(snapshot);
        }
        *cache = next.into();
    }

    fn publish(&self, event: &AccountPublication) -> Result<(), account::AccountConnectorError> {
        match event {
            AccountPublication::Order {
                symbol,
                client_order_id,
                venue_order_id,
                order,
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

                if order.exec_qty > 0.0 {
                    let mut fills = self
                        .fill_cumulative
                        .lock()
                        .unwrap_or_else(|p| p.into_inner());
                    if fills
                        .get(&id)
                        .is_some_and(|quantity| cumulative_filled_quantity_lots <= *quantity)
                    {
                        // Duplicate or stale partial update for the same order.
                        let changed = account::OrderChangedV1 {
                            header: self.header(
                                account::event_kind::ORDER_CHANGED,
                                if order.active() {
                                    account::event_flags::UPSERT
                                } else {
                                    account::event_flags::FINAL
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
                            average_price_ticks: order.exec_price_tick,
                            venue_order_id: id,
                            client_order_id: client_order_id.unwrap_or_default(),
                            ..Default::default()
                        };
                        self.cache_order(&changed);
                        return self
                            .context
                            .event_publisher
                            .publish_encoded(&changed, TraceContext::default())
                            .map_err(|e| rejected(e.to_string()));
                    }
                    fills.insert(id, cumulative_filled_quantity_lots);
                }

                let changed = account::OrderChangedV1 {
                    header: self.header(
                        account::event_kind::ORDER_CHANGED,
                        if order.active() {
                            account::event_flags::UPSERT
                        } else {
                            account::event_flags::FINAL
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
                    average_price_ticks: order.exec_price_tick,
                    venue_order_id: id,
                    client_order_id: client_order_id.unwrap_or_default(),
                    ..Default::default()
                };
                self.cache_order(&changed);
                self.context
                    .event_publisher
                    .publish_encoded(&changed, TraceContext::default())
                    .map_err(|e| rejected(e.to_string()))?;
                if order.exec_qty > 0.0 {
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
                    self.context
                        .event_publisher
                        .publish_encoded(&fill, TraceContext::default())
                        .map_err(|e| rejected(e.to_string()))?;
                    tracing::info!(
                        account_id = fill.header.account_id,
                        asset_id = fill.asset_id,
                        side = fill.side,
                        last_fill_quantity_lots = fill.last_fill_quantity_lots,
                        cumulative_filled_quantity_lots = fill.cumulative_filled_quantity_lots,
                        "Account fill event published."
                    );
                }
                if order.status != Status::New && order.status != Status::PartiallyFilled {
                    self.fill_cumulative
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .remove(&id);
                }
                Ok(())
            }
            AccountPublication::Position {
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
                let event = account::PositionChangedV1 {
                    header: self.header(
                        account::event_kind::POSITION_CHANGED,
                        account::event_flags::UPSERT,
                        *exch_ts,
                    ),
                    asset_id: b.asset_id.0,
                    position_side: if *qty > 0.0 {
                        ACCOUNT_POSITION_SIDE_LONG
                    } else if *qty < 0.0 {
                        ACCOUNT_POSITION_SIDE_SHORT
                    } else {
                        ACCOUNT_POSITION_SIDE_NET
                    },
                    quantity_lots: to_units(*qty, b.quantity_lot)?,
                    ..Default::default()
                };
                self.cache_position(&event);
                self.context
                    .event_publisher
                    .publish_encoded(&event, TraceContext::default())
                    .map_err(|e| rejected(e.to_string()))
            }
            AccountPublication::Error(_) => {
                self.invalidate(account::invalidation_reason::PRIVATE_STREAM);
                Ok(())
            }
        }
    }

    fn invalidate(&self, reason_code: u32) {
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
        reduce_only: value.reduce_only,
        client_order_id: ids.intern(&value.client_order_id),
        venue_order_id: ids.intern(&value.order_id),
        command_id: account::Id128::default(),
    })
}

fn position_snapshot(
    value: &PositionInfo,
    binding: &account::AccountInstrumentBinding,
    context: &account::AccountConnectorContext,
) -> Result<account::PositionSnapshot, account::AccountConnectorError> {
    let currency = context
        .currencies
        .first()
        .ok_or_else(|| rejected("position margin currency binding missing"))?;
    Ok(account::PositionSnapshot {
        asset_id: binding.asset_id,
        position_side: match value.position_side {
            ApiPositionSide::Long => ACCOUNT_POSITION_SIDE_LONG,
            ApiPositionSide::Short => ACCOUNT_POSITION_SIDE_SHORT,
            ApiPositionSide::Net | ApiPositionSide::Unknown => ACCOUNT_POSITION_SIDE_NET,
        },
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
    if !value.is_finite() {
        return Err(rejected("non-finite venue decimal"));
    }
    let scaled = value * 10_f64.powi(i32::from(unit.scale())) / unit.coefficient() as f64;
    let rounded = scaled.round();
    if (scaled - rounded).abs() > 1e-7 || rounded < i64::MIN as f64 || rounded > i64::MAX as f64 {
        return Err(rejected(
            "venue decimal is not exactly representable in configured units",
        ));
    }
    Ok(rounded as i64)
}
fn to_observation_units(
    value: f64,
    unit: account::DecimalUnit,
) -> Result<i64, account::AccountConnectorError> {
    if !value.is_finite() {
        return Err(rejected("non-finite venue observation"));
    }
    let rounded =
        (value * 10_f64.powi(i32::from(unit.scale())) / unit.coefficient() as f64).round();
    if rounded < i64::MIN as f64 || rounded > i64::MAX as f64 {
        return Err(rejected(
            "venue observation exceeds configured integer range",
        ));
    }
    Ok(rounded as i64)
}
fn from_units(value: i64, unit: account::DecimalUnit) -> f64 {
    value as f64 * unit.coefficient() as f64 / 10_f64.powi(i32::from(unit.scale()))
}
fn decimal_unit_f64(unit: &account::DecimalUnit) -> f64 {
    unit.coefficient() as f64 / 10_f64.powi(i32::from(unit.scale()))
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
    let order_id = u64::from_le_bytes(request.client_order_id.0[..8].try_into().unwrap());
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

pub fn venue_account_factories() -> Vec<Arc<dyn account::AccountConnectorFactory>> {
    let mut values: Vec<Arc<dyn account::AccountConnectorFactory>> = Vec::new();
    #[cfg(feature = "binancefutures")]
    values.push(Arc::new(VenueAccountConnectorFactory::new(
        "binance-futures-account",
        build_binance,
    )));
    #[cfg(feature = "okx")]
    values.push(Arc::new(VenueAccountConnectorFactory::new(
        "okx-account",
        build_okx,
    )));
    #[cfg(feature = "hyperliquid")]
    values.push(Arc::new(VenueAccountConnectorFactory::new(
        "hyperliquid-account",
        build_hyperliquid,
    )));
    values
}

#[cfg(test)]
mod tests {
    use super::*;
    use titan_core_types::{ComponentIdentity, CoreError, ResourceScope};

    #[derive(Default)]
    struct RecordingSink {
        events: Mutex<Vec<(String, Vec<u8>)>>,
    }

    impl account::AccountEventSink for RecordingSink {
        fn publish(
            &self,
            event_type: &str,
            payload: &[u8],
            _: TraceContext,
        ) -> Result<(), CoreError> {
            self.events
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push((event_type.to_owned(), payload.to_vec()));
            Ok(())
        }
    }

    struct FakeBootstrapApi;

    #[async_trait::async_trait]
    impl AccountBootstrapApi for FakeBootstrapApi {
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
            Ok(vec![PositionInfo {
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
            }])
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
            fill_cumulative: Mutex::new(HashMap::new()),
            orders: Arc::new(Mutex::new(Arc::from([]))),
            positions: Arc::new(Mutex::new(Arc::from([]))),
            balances: Arc::new(Mutex::new(Arc::from([]))),
        };

        encoder
            .publish(&AccountPublication::Position {
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
        assert_eq!(position.position_side, ACCOUNT_POSITION_SIDE_SHORT);
        assert_eq!(position.quantity_lots, -125);
        let cached = encoder
            .positions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert_eq!(cached.len(), 1);
        assert_eq!(cached[0].quantity_lots, -125);
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
        let encoder = AccountEventEncoder {
            context: context.clone(),
            epoch: Arc::new(AtomicU64::new(1)),
            version: Arc::new(AtomicU64::new(0)),
            ready: Arc::new(AtomicBool::new(false)),
            ids: Arc::new(Mutex::new(IdInterner::default())),
            fill_cumulative: Mutex::new(HashMap::new()),
            orders: Arc::new(Mutex::new(Arc::from([]))),
            positions: Arc::new(Mutex::new(Arc::from([]))),
            balances: Arc::new(Mutex::new(Arc::from([]))),
        };

        let snapshot = load_account_bootstrap(&context, &FakeBootstrapApi, &encoder.ids)
            .await
            .unwrap();
        encoder.install_bootstrap(snapshot).unwrap();
        encoder.publish_ready().unwrap();

        assert!(encoder.ready.load(Ordering::Acquire));
        let orders = encoder.orders.lock().unwrap();
        assert_eq!(orders.len(), 1);
        assert!(orders[0].reduce_only);
        drop(orders);
        assert_eq!(encoder.positions.lock().unwrap()[0].quantity_lots, 125);
        assert_eq!(encoder.balances.lock().unwrap()[0].wallet_units, 100_000);
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
}
