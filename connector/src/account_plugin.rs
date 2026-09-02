//! AccountPlugin adapters for the existing venue connectors.
//!
//! The adapter keeps command admission synchronous and bounded, runs authenticated REST/private
//! stream work on a per-account Tokio runtime, and translates venue facts directly into the
//! stable account ABI. Exchange-specific REST/WS merging remains inside the venue connector.

use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::JoinHandle,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use hftbacktest::types::{LiveEvent, OrdType, Side, Status, TimeInForce};
use titan_account_plugin as account;
use titan_account_plugin::SecretValue;
use titan_plugin_engine::{ClosureResource, TraceContext};
use tokio::sync::{mpsc, oneshot};

use crate::{
    api::{
        AmendOrderRequest, ApiMarginType, ApiOrderStatus, ApiOrderType, ApiPositionSide, ApiSide,
        ApiTimeInForce, Balance, BrokerApi, CancelOrderRequest, OrderInfo, PositionInfo,
        UnifiedOrderRequest,
    },
    connector::{
        Connector, ConnectorBuilder, DirectPublication, PublishEvent, direct_publish_sender,
    },
};

const OPERATION_HISTORY_LIMIT: usize = 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
enum Command {
    Submit(account::SubmitOrderCommand),
    Amend(account::AmendOrderCommand),
    Cancel(account::CancelOrderCommand),
    CancelAll(account::CancelAllCommand),
    CancelAllAfter(account::CancelAllAfterCommand),
    Reconcile(account::ReconcileScope, account::OperationId),
    PrivateStreamReady,
}

struct Running {
    command: mpsc::Sender<Command>,
    stop: Option<oneshot::Sender<Instant>>,
    thread: Option<JoinHandle<()>>,
}

#[derive(Default)]
struct Operations {
    values: HashMap<account::OperationId, account::AccountConnectorOperationSnapshot>,
    terminal: VecDeque<account::OperationId>,
}
impl Operations {
    fn set(
        &mut self,
        id: account::OperationId,
        state: account::OperationState,
        detail: impl Into<Arc<str>>,
    ) {
        let terminal = state != account::OperationState::Pending;
        let was = self
            .values
            .get(&id)
            .is_some_and(|v| v.state != account::OperationState::Pending);
        self.values.insert(
            id,
            account::AccountConnectorOperationSnapshot {
                id,
                state,
                detail: detail.into(),
            },
        );
        if terminal && !was {
            self.terminal.push_back(id);
        }
        while self.terminal.len() > OPERATION_HISTORY_LIMIT {
            if let Some(id) = self.terminal.pop_front() {
                self.values.remove(&id);
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
    connector: Mutex<Option<Box<dyn Connector>>>,
    api: Arc<dyn BrokerApi>,
    running: Mutex<Option<Running>>,
    active: AtomicBool,
    ready: Arc<AtomicBool>,
    reconciling: Arc<AtomicBool>,
    epoch: Arc<AtomicU64>,
    version: Arc<AtomicU64>,
    next_operation: AtomicU64,
    operations: Arc<Mutex<Operations>>,
    orders: Arc<Mutex<Arc<[account::OrderSnapshot]>>>,
    positions: Arc<Mutex<Arc<[account::PositionSnapshot]>>>,
    balances: Arc<Mutex<Arc<[account::BalanceSnapshot]>>>,
    external_order_count: Arc<AtomicU64>,
    journal: Mutex<HashMap<account::CommandId, (Command, account::AccountCommandReceipt)>>,
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
            connector: Mutex::new(Some(connector)),
            api,
            running: Mutex::new(None),
            active: AtomicBool::new(false),
            ready: Arc::new(AtomicBool::new(false)),
            reconciling: Arc::new(AtomicBool::new(false)),
            epoch: Arc::new(AtomicU64::new(0)),
            version: Arc::new(AtomicU64::new(0)),
            next_operation: AtomicU64::new(1),
            operations: Arc::new(Mutex::new(Operations::default())),
            orders: Arc::new(Mutex::new(Arc::from([]))),
            positions: Arc::new(Mutex::new(Arc::from([]))),
            balances: Arc::new(Mutex::new(Arc::from([]))),
            external_order_count: Arc::new(AtomicU64::new(0)),
            journal: Mutex::new(HashMap::new()),
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
    fn receipt(
        id: account::CommandId,
        client: Option<account::ClientOrderId>,
        handle: account::AccountHandle,
    ) -> account::AccountCommandReceipt {
        account::AccountCommandReceipt {
            account: handle,
            command_id: id,
            client_order_id: client,
            accepted_at: now_ns(),
        }
    }
    fn admit(
        &self,
        id: account::CommandId,
        client: Option<account::ClientOrderId>,
        command: Command,
    ) -> Result<account::AccountCommandReceipt, account::AccountConnectorError> {
        let mut journal = self.journal.lock().unwrap_or_else(|p| p.into_inner());
        if let Some((old, receipt)) = journal.get(&id) {
            return if old == &command {
                Ok(receipt.clone())
            } else {
                Err(account::AccountConnectorError::new(
                    account::AccountErrorKind::CommandConflict,
                    "command id is already associated with different content",
                ))
            };
        }
        let tx = self
            .running
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .map(|r| r.command.clone())
            .ok_or_else(|| {
                account::AccountConnectorError::new(
                    account::AccountErrorKind::NotReady,
                    "account runtime is not running",
                )
            })?;
        tx.try_send(command.clone()).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => account::AccountConnectorError::new(
                account::AccountErrorKind::QueueFull,
                "account command queue is full",
            ),
            mpsc::error::TrySendError::Closed(_) => account::AccountConnectorError::new(
                account::AccountErrorKind::NotReady,
                "account command queue is closed",
            ),
        })?;
        let receipt = Self::receipt(id, client, self.context.account);
        journal.insert(id, (command, receipt.clone()));
        Ok(receipt)
    }
    fn snapshot<T>(&self, items: Arc<[T]>) -> account::AccountStateSnapshot<T> {
        account::AccountStateSnapshot {
            account: self.context.account,
            state: if self.reconciling.load(Ordering::Acquire) {
                account::AccountSnapshotState::Reconciling
            } else if self.ready.load(Ordering::Acquire) {
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

fn close_weak(weak: &Weak<AccountRuntime>) -> Result<(), titan_plugin_engine::PluginError> {
    if let Some(runtime) = weak.upgrade() {
        let _ = runtime.stop_inner(Instant::now() + Duration::from_secs(1));
    }
    Ok(())
}

impl account::AccountConnector for AccountRuntime {
    fn start(&self) -> Result<(), account::AccountConnectorError> {
        if self.active.swap(true, Ordering::AcqRel) {
            return Err(rejected("account connector already running"));
        }
        let mut connector = self
            .connector
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
            .ok_or_else(|| {
                rejected("account connector cannot restart after resources were released")
            })?;
        for b in self.context.instruments.iter() {
            connector.register_account(b.native_symbol.to_string());
        }
        let api = self.api.clone();
        let context = self.context.clone();
        let epoch = self.epoch.clone();
        let version = self.version.clone();
        let ready = self.ready.clone();
        let reconciling = self.reconciling.clone();
        let orders = self.orders.clone();
        let positions = self.positions.clone();
        let balances = self.balances.clone();
        let external_order_count = self.external_order_count.clone();
        let operations = self.operations.clone();
        let ids = self.ids.clone();
        let shutdown_policy = self.shutdown_policy.clone();
        let (command_tx, mut command_rx) = mpsc::channel(context.command_queue_capacity);
        let (stop_tx, mut stop_rx) = oneshot::channel();
        let recovery_tx = command_tx.clone();
        let thread = std::thread::Builder::new()
            .name(format!("account-{}", context.account.account_id.0))
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                    .expect("account tokio runtime");
                runtime.block_on(async move {
                    reconciling.store(true, Ordering::Release);
                    let bridge = Arc::new(LegacyBridge {
                        context: context.clone(),
                        epoch: epoch.clone(),
                        version: version.clone(),
                        ready: ready.clone(),
                        reconciling: reconciling.clone(),
                        ids: ids.clone(),
                        pending: Mutex::new(Vec::new()),
                    });
                    let bridge_events = bridge.clone();
                    let event_recovery = recovery_tx.clone();
                    let tx = direct_publish_sender(move |publication| {
                        let DirectPublication::Event(event) = publication else {
                            return;
                        };
                        match event {
                            PublishEvent::PrivateStreamReady => {
                                let _ = event_recovery.try_send(Command::PrivateStreamReady);
                            }
                            PublishEvent::LiveEvent(LiveEvent::Error(_)) => {
                                bridge_events.invalidate(1);
                            }
                            PublishEvent::LiveEvent(event) => {
                                if bridge_events.publish_live(event).is_err() {
                                    bridge_events.invalidate(2);
                                    let _ = event_recovery.try_send(Command::Reconcile(
                                        account::ReconcileScope::Full,
                                        account::OperationId(0),
                                    ));
                                }
                            }
                            _ => {}
                        }
                    });
                    connector.run_account(tx);
                    loop {
                        tokio::select! {
                            deadline = &mut stop_rx => {
                                let deadline = deadline.unwrap_or_else(|_| Instant::now());
                                let remaining = deadline.saturating_duration_since(Instant::now());
                                match shutdown_policy {
                                    account::ShutdownOrderPolicy::LeaveOpen => {}
                                    account::ShutdownOrderPolicy::CancelAll => {
                                        let _ = tokio::time::timeout(remaining, connector.shutdown()).await;
                                    }
                                    account::ShutdownOrderPolicy::CancelAllAfter { timeout_ms } => {
                                        let _ = tokio::time::timeout(
                                            remaining,
                                            api.cancel_all_after(timeout_ms),
                                        )
                                        .await;
                                    }
                                }
                                break;
                            }
                            command = command_rx.recv() => match command {
                                Some(command) => {
                                    handle_command(
                                        command,
                                        &context,
                                        &*api,
                                        &epoch,
                                        &version,
                                        &reconciling,
                                        &ready,
                                        &orders,
                                        &positions,
                                        &balances,
                                        &external_order_count,
                                        &operations,
                                        &ids,
                                        &recovery_tx,
                                    )
                                    .await;
                                    if bridge.replay().is_err() {
                                        bridge.invalidate(2);
                                        schedule_reconcile(recovery_tx.clone());
                                    }
                                }
                                None => break,
                            }
                        }
                    }
                });
            })
            .map_err(|e| rejected(e.to_string()))?;
        *self.running.lock().unwrap_or_else(|p| p.into_inner()) = Some(Running {
            command: command_tx,
            stop: Some(stop_tx),
            thread: Some(thread),
        });
        Ok(())
    }
    fn stop(&self, deadline: Instant) -> Result<(), account::AccountConnectorError> {
        self.stop_inner(deadline)
    }
    fn submit(
        &self,
        c: account::SubmitOrderCommand,
    ) -> Result<account::AccountCommandReceipt, account::AccountConnectorError> {
        let id = c.command_id;
        let client = c.client_order_id;
        self.admit(id, client, Command::Submit(c))
    }
    fn amend(
        &self,
        c: account::AmendOrderCommand,
    ) -> Result<account::AccountCommandReceipt, account::AccountConnectorError> {
        let id = c.command_id;
        let client = c.client_order_id;
        self.admit(id, client, Command::Amend(c))
    }
    fn cancel(
        &self,
        c: account::CancelOrderCommand,
    ) -> Result<account::AccountCommandReceipt, account::AccountConnectorError> {
        let id = c.command_id;
        let client = c.client_order_id;
        self.admit(id, client, Command::Cancel(c))
    }
    fn cancel_all(
        &self,
        c: account::CancelAllCommand,
    ) -> Result<account::AccountCommandReceipt, account::AccountConnectorError> {
        let id = c.command_id;
        self.admit(id, None, Command::CancelAll(c))
    }
    fn cancel_all_after(
        &self,
        c: account::CancelAllAfterCommand,
    ) -> Result<account::AccountCommandReceipt, account::AccountConnectorError> {
        let id = c.command_id;
        self.admit(id, None, Command::CancelAllAfter(c))
    }
    fn reconcile(
        &self,
        scope: account::ReconcileScope,
    ) -> Result<account::OperationId, account::AccountConnectorError> {
        self.ready.store(false, Ordering::Release);
        self.reconciling.store(true, Ordering::Release);
        let id = account::OperationId(self.next_operation.fetch_add(1, Ordering::Relaxed));
        self.operations
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .set(
                id,
                account::OperationState::Pending,
                "reconciliation queued",
            );
        let tx = self
            .running
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .map(|r| r.command.clone())
            .ok_or_else(|| {
                account::AccountConnectorError::new(
                    account::AccountErrorKind::NotReady,
                    "account runtime is not running",
                )
            })?;
        if let Err(error) = tx.try_send(Command::Reconcile(scope, id)) {
            let (kind, detail) = match error {
                mpsc::error::TrySendError::Full(_) => (
                    account::AccountErrorKind::QueueFull,
                    "account command queue is full",
                ),
                mpsc::error::TrySendError::Closed(_) => (
                    account::AccountErrorKind::NotReady,
                    "account command queue is closed",
                ),
            };
            self.operations
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .set(id, account::OperationState::Failed, detail);
            return Err(account::AccountConnectorError::new(kind, detail));
        }
        Ok(id)
    }
    fn orders(
        &self,
        filter: account::OrderFilter,
    ) -> Result<account::AccountStateSnapshot<account::OrderSnapshot>, account::AccountConnectorError>
    {
        let values = self.orders.lock().unwrap_or_else(|p| p.into_inner());
        let items: Arc<[_]> = values
            .iter()
            .filter(|o| filter.asset_id.is_none_or(|a| a == o.asset_id))
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
        account::AccountConnectorHealthSnapshot {
            state: if self.ready.load(Ordering::Acquire) {
                account::AccountLifecycle::Ready
            } else if self.reconciling.load(Ordering::Acquire) {
                account::AccountLifecycle::Reconciling
            } else if self.active.load(Ordering::Acquire) {
                account::AccountLifecycle::Connecting
            } else {
                account::AccountLifecycle::Stopped
            },
            message: Arc::from(if self.ready.load(Ordering::Acquire) {
                "private stream and reconciliation active"
            } else {
                "account connector not ready"
            }),
            observed_at: SystemTime::now(),
        }
    }
    fn diagnostics(&self) -> account::AccountConnectorDiagnosticSnapshot {
        let command_queue_depth = self
            .running
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .map_or(0, |running| {
                self.context
                    .command_queue_capacity
                    .saturating_sub(running.command.capacity())
            });
        account::AccountConnectorDiagnosticSnapshot {
            summary: Arc::from("venue account adapter"),
            external_order_count: self.external_order_count.load(Ordering::Acquire),
            command_queue_depth,
            account_epoch: self.epoch.load(Ordering::Acquire),
            account_version: self.version.load(Ordering::Acquire),
        }
    }
    fn operation(&self, id: account::OperationId) -> account::AccountConnectorOperationSnapshot {
        self.operations
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .values
            .get(&id)
            .cloned()
            .unwrap_or(account::AccountConnectorOperationSnapshot {
                id,
                state: account::OperationState::Failed,
                detail: Arc::from("operation not found"),
            })
    }
}

impl AccountRuntime {
    fn stop_inner(&self, deadline: Instant) -> Result<(), account::AccountConnectorError> {
        self.ready.store(false, Ordering::Release);
        self.active.store(false, Ordering::Release);
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
        self.context.event_publisher.close();
        Ok(())
    }
}

struct LegacyBridge {
    context: account::AccountConnectorContext,
    epoch: Arc<AtomicU64>,
    version: Arc<AtomicU64>,
    ready: Arc<AtomicBool>,
    reconciling: Arc<AtomicBool>,
    ids: Arc<Mutex<IdInterner>>,
    pending: Mutex<Vec<LiveEvent>>,
}
impl LegacyBridge {
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
    fn publish_live(&self, event: &LiveEvent) -> Result<(), account::AccountConnectorError> {
        if (self.reconciling.load(Ordering::Acquire) || !self.context.event_publisher.is_open())
            && !matches!(event, LiveEvent::Error(_))
        {
            self.pending
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(event.clone());
            return Ok(());
        }
        match event {
            LiveEvent::Order { symbol, order } => {
                let binding = self
                    .context
                    .instruments
                    .iter()
                    .find(|b| b.native_symbol.eq_ignore_ascii_case(symbol))
                    .ok_or_else(|| rejected("unbound private order symbol"))?;
                let id = self
                    .ids
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .intern(&order.order_id.to_string());
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
                    filled_quantity_lots: to_units(
                        order.qty - order.leaves_qty,
                        binding.quantity_lot,
                    )?,
                    average_price_ticks: order.exec_price_tick,
                    venue_order_id: id,
                    ..Default::default()
                };
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
                        cumulative_filled_quantity_lots: to_units(
                            order.qty - order.leaves_qty,
                            binding.quantity_lot,
                        )?,
                        venue_order_id: id,
                        ..Default::default()
                    };
                    self.context
                        .event_publisher
                        .publish_encoded(&fill, TraceContext::default())
                        .map_err(|e| rejected(e.to_string()))?;
                }
                Ok(())
            }
            LiveEvent::Position {
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
                    position_side: if *qty < 0.0 { 2 } else { 1 },
                    quantity_lots: to_units(*qty, b.quantity_lot)?,
                    ..Default::default()
                };
                self.context
                    .event_publisher
                    .publish_encoded(&event, TraceContext::default())
                    .map_err(|e| rejected(e.to_string()))
            }
            LiveEvent::Error(_) => {
                self.invalidate(1);
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn replay(&self) -> Result<(), account::AccountConnectorError> {
        if self.reconciling.load(Ordering::Acquire) || !self.context.event_publisher.is_open() {
            return Ok(());
        }
        let pending = std::mem::take(&mut *self.pending.lock().unwrap_or_else(|p| p.into_inner()));
        let mut iter = pending.into_iter();
        while let Some(event) = iter.next() {
            if let Err(error) = self.publish_live(&event) {
                let mut retained = Vec::with_capacity(iter.len() + 1);
                retained.push(event);
                retained.extend(iter);
                let mut queue = self.pending.lock().unwrap_or_else(|p| p.into_inner());
                retained.append(&mut *queue);
                *queue = retained;
                return Err(error);
            }
        }
        Ok(())
    }

    fn invalidate(&self, reason_code: u32) {
        self.ready.store(false, Ordering::Release);
        self.reconciling.store(true, Ordering::Release);
        publish_invalidated(&self.context, &self.epoch, &self.version, reason_code);
    }
}

async fn reconcile(
    context: &account::AccountConnectorContext,
    api: &dyn BrokerApi,
    scope: account::ReconcileScope,
    epoch: &AtomicU64,
    version: &AtomicU64,
    reconciling: &AtomicBool,
    ready: &AtomicBool,
    orders: &Mutex<Arc<[account::OrderSnapshot]>>,
    positions: &Mutex<Arc<[account::PositionSnapshot]>>,
    balances: &Mutex<Arc<[account::BalanceSnapshot]>>,
    external_order_count: &AtomicU64,
    ids: &Mutex<IdInterner>,
) -> Result<(), account::AccountConnectorError> {
    reconciling.store(true, Ordering::Release);
    ready.store(false, Ordering::Release);
    epoch.fetch_add(1, Ordering::AcqRel);
    version.store(0, Ordering::Release);
    let publish = context.event_publisher.is_open();
    let scope_code = reconcile_scope_code(scope);
    let started_header = header(
        context,
        epoch,
        version,
        account::event_kind::RECONCILE_STARTED,
        account::event_flags::SNAPSHOT,
        now_ns(),
    );
    if publish {
        context
            .event_publisher
            .publish_encoded(
                &account::ReconcileStartedV1(account::ReconcileV1 {
                    header: started_header,
                    scope: scope_code,
                    ..Default::default()
                }),
                TraceContext::default(),
            )
            .map_err(|e| rejected(e.to_string()))?;
    }
    if matches!(
        scope,
        account::ReconcileScope::Full | account::ReconcileScope::Orders
    ) {
        let mut next_orders = Vec::new();
        let mut external = 0_u64;
        for binding in context.instruments.iter() {
            for value in api
                .get_open_orders(&binding.native_symbol)
                .await
                .map_err(api_error)?
            {
                if let account::OrderOwnershipPolicy::ManagedOnly { client_id_prefix } =
                    &context.ownership
                {
                    let known = ids
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .contains_text(&value.client_order_id);
                    if !known && !value.client_order_id.starts_with(client_id_prefix.as_ref()) {
                        external = external.saturating_add(1);
                        continue;
                    }
                }
                let snapshot = order_snapshot(&value, binding, ids)?;
                if publish {
                    publish_order_snapshot(
                        context,
                        epoch,
                        version,
                        &snapshot,
                        &value,
                        account::event_flags::SNAPSHOT | account::event_flags::UPSERT,
                    )?;
                }
                next_orders.push(snapshot);
            }
        }
        external_order_count.store(external, Ordering::Release);
        *orders.lock().unwrap_or_else(|p| p.into_inner()) = next_orders.into();
    }
    if matches!(
        scope,
        account::ReconcileScope::Full | account::ReconcileScope::Positions
    ) {
        let mut next_positions = Vec::new();
        for value in api.get_positions(None).await.map_err(api_error)? {
            if let Some(binding) = context
                .instruments
                .iter()
                .find(|b| b.native_symbol.eq_ignore_ascii_case(&value.symbol))
            {
                let snapshot = position_snapshot(&value, binding, context)?;
                let event = position_event(
                    header(
                        context,
                        epoch,
                        version,
                        account::event_kind::POSITION_CHANGED,
                        account::event_flags::SNAPSHOT | account::event_flags::UPSERT,
                        value.update_time,
                    ),
                    &snapshot,
                );
                if publish {
                    context
                        .event_publisher
                        .publish_encoded(&event, TraceContext::default())
                        .map_err(|e| rejected(e.to_string()))?;
                }
                next_positions.push(snapshot);
            }
        }
        *positions.lock().unwrap_or_else(|p| p.into_inner()) = next_positions.into();
    }
    if matches!(
        scope,
        account::ReconcileScope::Full | account::ReconcileScope::Balances
    ) {
        let info = api.get_account().await.map_err(api_error)?;
        let mut next_balances = Vec::new();
        for value in &info.balances {
            if let Some(binding) = context
                .currencies
                .iter()
                .find(|b| b.native_currency.eq_ignore_ascii_case(&value.asset))
            {
                let snapshot = balance_snapshot(value, binding)?;
                let event = balance_event(
                    header(
                        context,
                        epoch,
                        version,
                        account::event_kind::BALANCE_CHANGED,
                        account::event_flags::SNAPSHOT | account::event_flags::UPSERT,
                        info.timestamp,
                    ),
                    &snapshot,
                );
                if publish {
                    context
                        .event_publisher
                        .publish_encoded(&event, TraceContext::default())
                        .map_err(|e| rejected(e.to_string()))?;
                }
                next_balances.push(snapshot);
            }
        }
        *balances.lock().unwrap_or_else(|p| p.into_inner()) = next_balances.into();
    }
    let terminal = version.load(Ordering::Acquire) + 1;
    let completed = account::ReconcileCompletedV1(account::ReconcileV1 {
        header: header(
            context,
            epoch,
            version,
            account::event_kind::RECONCILE_COMPLETED,
            0,
            now_ns(),
        ),
        terminal_version: terminal,
        scope: scope_code,
        success: 1,
    });
    if publish {
        context
            .event_publisher
            .publish_encoded(&completed, TraceContext::default())
            .map_err(|e| rejected(e.to_string()))?;
    }
    let ready_event = account::StreamStateChangedV1(account::StreamStateV1 {
        header: header(
            context,
            epoch,
            version,
            account::event_kind::STREAM_STATE_CHANGED,
            0,
            now_ns(),
        ),
        state: account::AccountLifecycle::Ready as u8,
        reason_code: 0,
    });
    if publish {
        context
            .event_publisher
            .publish_encoded(&ready_event, TraceContext::default())
            .map_err(|e| rejected(e.to_string()))?;
    }
    reconciling.store(false, Ordering::Release);
    ready.store(true, Ordering::Release);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn handle_command(
    command: Command,
    context: &account::AccountConnectorContext,
    api: &dyn BrokerApi,
    epoch: &AtomicU64,
    version: &AtomicU64,
    reconciling: &AtomicBool,
    ready: &AtomicBool,
    orders: &Mutex<Arc<[account::OrderSnapshot]>>,
    positions: &Mutex<Arc<[account::PositionSnapshot]>>,
    balances: &Mutex<Arc<[account::BalanceSnapshot]>>,
    external_order_count: &AtomicU64,
    operations: &Mutex<Operations>,
    ids: &Mutex<IdInterner>,
    recovery_tx: &mpsc::Sender<Command>,
) {
    let command = match command {
        Command::Reconcile(scope, id) => {
            let result = reconcile(
                context,
                api,
                scope,
                epoch,
                version,
                reconciling,
                ready,
                orders,
                positions,
                balances,
                external_order_count,
                ids,
            )
            .await;
            if result.is_err() {
                publish_invalidated(context, epoch, version, 2);
                schedule_reconcile(recovery_tx.clone());
            }
            if id.0 != 0 {
                operations.lock().unwrap_or_else(|p| p.into_inner()).set(
                    id,
                    if result.is_ok() {
                        account::OperationState::Succeeded
                    } else {
                        account::OperationState::Failed
                    },
                    result
                        .err()
                        .map_or_else(|| "reconciliation completed".to_string(), |e| e.to_string()),
                );
            }
            return;
        }
        Command::PrivateStreamReady => {
            let result = reconcile(
                context,
                api,
                account::ReconcileScope::Full,
                epoch,
                version,
                reconciling,
                ready,
                orders,
                positions,
                balances,
                external_order_count,
                ids,
            )
            .await;
            if result.is_err() {
                publish_invalidated(context, epoch, version, 2);
                schedule_reconcile(recovery_tx.clone());
            }
            return;
        }
        command => command,
    };
    let (id, client, trace, result): (
        account::CommandId,
        Option<account::ClientOrderId>,
        TraceContext,
        Result<Option<OrderInfo>, crate::api::ApiError>,
    ) = match &command {
        Command::Submit(c) => {
            let b = context
                .instruments
                .iter()
                .find(|b| b.asset_id == c.asset_id)
                .unwrap();
            let req = UnifiedOrderRequest {
                symbol: b.native_symbol.to_string(),
                side: if c.side == 1 {
                    ApiSide::Buy
                } else {
                    ApiSide::Sell
                },
                order_type: if c.order_type == 0 {
                    ApiOrderType::Limit
                } else {
                    ApiOrderType::Market
                },
                price: (c.order_type == 0).then(|| from_units(c.price_ticks, b.price_tick)),
                qty: from_units(c.quantity_lots, b.quantity_lot),
                time_in_force: api_tif(c.time_in_force),
                reduce_only: false,
                position_side: None,
                client_order_id: c.client_order_id.map(id_text),
                stop_price: None,
            };
            (
                c.command_id,
                c.client_order_id,
                c.trace,
                api.submit_order(&req).await.map(Some),
            )
        }
        Command::Amend(c) => {
            let b = context
                .instruments
                .iter()
                .find(|b| b.asset_id == c.asset_id)
                .unwrap();
            let req = AmendOrderRequest {
                symbol: b.native_symbol.to_string(),
                order_id: c.venue_order_id.map(|id| resolve_id(ids, id)),
                client_order_id: c.client_order_id.map(|id| resolve_id(ids, id)),
                new_price: c.price_ticks.map(|v| from_units(v, b.price_tick)),
                new_qty: c.quantity_lots.map(|v| from_units(v, b.quantity_lot)),
                new_stop_price: None,
            };
            (
                c.command_id,
                c.client_order_id,
                c.trace,
                api.amend_order(&req).await.map(Some),
            )
        }
        Command::Cancel(c) => {
            let b = context
                .instruments
                .iter()
                .find(|b| b.asset_id == c.asset_id)
                .unwrap();
            let req = CancelOrderRequest {
                symbol: b.native_symbol.to_string(),
                order_id: c.venue_order_id.map(|id| resolve_id(ids, id)),
                client_order_id: c.client_order_id.map(|id| resolve_id(ids, id)),
            };
            (
                c.command_id,
                c.client_order_id,
                c.trace,
                api.cancel_order(&req).await.map(Some),
            )
        }
        Command::CancelAll(c) => {
            let result = if let Some(asset) = c.asset_id {
                let b = context
                    .instruments
                    .iter()
                    .find(|b| b.asset_id == asset)
                    .unwrap();
                api.cancel_all_orders(&b.native_symbol).await
            } else {
                let mut result = Ok(());
                for b in context.instruments.iter() {
                    if let Err(e) = api.cancel_all_orders(&b.native_symbol).await {
                        result = Err(e);
                        break;
                    }
                }
                result
            };
            (c.command_id, None, c.trace, result.map(|_| None))
        }
        Command::CancelAllAfter(c) => (
            c.command_id,
            None,
            c.trace,
            api.cancel_all_after(c.timeout_ms).await.map(|_| None),
        ),
        Command::Reconcile(..) | Command::PrivateStreamReady => unreachable!(),
    };
    let outcome = if result.is_ok() { 1 } else { 2 };
    let event = account::CommandResultV1 {
        header: header(
            context,
            epoch,
            version,
            account::event_kind::COMMAND_RESULT,
            if result.is_ok() {
                account::event_flags::FINAL
            } else {
                account::event_flags::FINAL
            },
            now_ns(),
        ),
        command_id: id,
        client_order_id: client.unwrap_or_default(),
        outcome,
        final_result: 1,
        reason_code: u32::from(result.is_err()),
    };
    let _ = context.event_publisher.publish_encoded(&event, trace);
    if let Ok(Some(value)) = result {
        if let Some(binding) = context
            .instruments
            .iter()
            .find(|b| b.native_symbol.eq_ignore_ascii_case(&value.symbol))
        {
            if let Ok(snapshot) = order_snapshot(&value, binding, ids) {
                let _ = publish_order_snapshot(
                    context,
                    epoch,
                    version,
                    &snapshot,
                    &value,
                    if matches!(
                        value.status,
                        ApiOrderStatus::Filled
                            | ApiOrderStatus::Canceled
                            | ApiOrderStatus::Rejected
                            | ApiOrderStatus::Expired
                    ) {
                        account::event_flags::FINAL
                    } else {
                        account::event_flags::UPSERT
                    },
                );
            }
        }
    }
}

fn schedule_reconcile(tx: mpsc::Sender<Command>) {
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let _ = tx
            .send(Command::Reconcile(
                account::ReconcileScope::Full,
                account::OperationId(0),
            ))
            .await;
    });
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
    let _ = context
        .event_publisher
        .publish_encoded(&event, TraceContext::default());
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
fn publish_order_snapshot(
    context: &account::AccountConnectorContext,
    epoch: &AtomicU64,
    version: &AtomicU64,
    s: &account::OrderSnapshot,
    source: &OrderInfo,
    flags: u16,
) -> Result<(), account::AccountConnectorError> {
    let event = account::OrderChangedV1 {
        header: header(
            context,
            epoch,
            version,
            account::event_kind::ORDER_CHANGED,
            flags,
            source.update_time,
        ),
        asset_id: s.asset_id.0,
        side: s.side,
        order_type: s.order_type,
        time_in_force: s.time_in_force,
        status: s.status,
        price_ticks: s.price_ticks,
        quantity_lots: s.quantity_lots,
        filled_quantity_lots: s.filled_quantity_lots,
        client_order_id: s.client_order_id,
        venue_order_id: s.venue_order_id,
        command_id: s.command_id,
        average_price_ticks: 0,
    };
    context
        .event_publisher
        .publish_encoded(&event, TraceContext::default())
        .map_err(|e| rejected(e.to_string()))
}
fn order_snapshot(
    v: &OrderInfo,
    b: &account::AccountInstrumentBinding,
    ids: &Mutex<IdInterner>,
) -> Result<account::OrderSnapshot, account::AccountConnectorError> {
    let mut ids = ids.lock().unwrap_or_else(|p| p.into_inner());
    Ok(account::OrderSnapshot {
        asset_id: b.asset_id,
        side: if v.side == ApiSide::Buy { 1 } else { 2 },
        order_type: if v.order_type == ApiOrderType::Market {
            1
        } else {
            0
        },
        time_in_force: match v.time_in_force {
            ApiTimeInForce::GTC => 0,
            ApiTimeInForce::GTX => 1,
            ApiTimeInForce::FOK => 2,
            ApiTimeInForce::IOC => 3,
            _ => 255,
        },
        status: api_status(v.status),
        price_ticks: to_units(v.price, b.price_tick)?,
        quantity_lots: to_units(v.qty, b.quantity_lot)?,
        filled_quantity_lots: to_units(v.executed_qty, b.quantity_lot)?,
        client_order_id: ids.intern(&v.client_order_id),
        venue_order_id: ids.intern(&v.order_id),
        command_id: default_command(),
    })
}
fn position_snapshot(
    v: &PositionInfo,
    b: &account::AccountInstrumentBinding,
    context: &account::AccountConnectorContext,
) -> Result<account::PositionSnapshot, account::AccountConnectorError> {
    let currency = context
        .currencies
        .first()
        .ok_or_else(|| rejected("position margin currency binding missing"))?;
    Ok(account::PositionSnapshot {
        asset_id: b.asset_id,
        position_side: match v.position_side {
            ApiPositionSide::Long => 1,
            ApiPositionSide::Short => 2,
            _ => 0,
        },
        margin_type: if v.margin_type == ApiMarginType::Isolated {
            1
        } else {
            2
        },
        quantity_lots: to_units(v.qty, b.quantity_lot)?,
        entry_price_ticks: to_units(v.entry_price, b.price_tick)?,
        liquidation_price_ticks: to_units(v.liquidation_price, b.price_tick)?,
        realized_pnl_units: to_units(v.realized_pnl, currency.amount_unit)?,
        unrealized_pnl_units: to_units(v.unrealized_pnl, currency.amount_unit)?,
        margin_currency_id: currency.currency_id,
    })
}
fn position_event(
    h: account::AccountEventHeaderV1,
    s: &account::PositionSnapshot,
) -> account::PositionChangedV1 {
    account::PositionChangedV1 {
        header: h,
        asset_id: s.asset_id.0,
        position_side: s.position_side,
        margin_type: s.margin_type,
        quantity_lots: s.quantity_lots,
        entry_price_ticks: s.entry_price_ticks,
        liquidation_price_ticks: s.liquidation_price_ticks,
        realized_pnl_units: s.realized_pnl_units,
        unrealized_pnl_units: s.unrealized_pnl_units,
        margin_currency_id: s.margin_currency_id.0,
    }
}
fn balance_snapshot(
    v: &Balance,
    b: &account::AccountCurrencyBinding,
) -> Result<account::BalanceSnapshot, account::AccountConnectorError> {
    Ok(account::BalanceSnapshot {
        currency_id: b.currency_id,
        wallet_units: to_units(v.wallet_balance, b.amount_unit)?,
        available_units: to_units(v.available_balance, b.amount_unit)?,
        margin_units: to_units(v.margin_balance, b.amount_unit)?,
        unrealized_pnl_units: to_units(v.unrealized_pnl, b.amount_unit)?,
    })
}
fn balance_event(
    h: account::AccountEventHeaderV1,
    s: &account::BalanceSnapshot,
) -> account::BalanceChangedV1 {
    account::BalanceChangedV1 {
        header: h,
        currency_id: s.currency_id.0,
        wallet_units: s.wallet_units,
        available_units: s.available_units,
        margin_units: s.margin_units,
        unrealized_pnl_units: s.unrealized_pnl_units,
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
fn from_units(value: i64, unit: account::DecimalUnit) -> f64 {
    value as f64 * unit.coefficient() as f64 / 10_f64.powi(i32::from(unit.scale()))
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
fn default_command() -> account::CommandId {
    account::Id128::default()
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
fn reconcile_scope_code(scope: account::ReconcileScope) -> u8 {
    match scope {
        account::ReconcileScope::Full => 0,
        account::ReconcileScope::Orders => 1,
        account::ReconcileScope::Positions => 2,
        account::ReconcileScope::Balances => 3,
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
fn api_error(e: crate::api::ApiError) -> account::AccountConnectorError {
    rejected(format!("venue request failed: {}", e.code))
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
        if matches!(
            definition.ownership,
            account::OrderOwnershipPolicy::ObserveAll
        ) {
            return Err(rejected(
                "legacy venue adapter does not support ObserveAll ownership",
            ));
        }
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

    #[test]
    fn decimal_conversion_rejects_non_representable_values() {
        let unit: account::DecimalUnit = "0.001".parse().unwrap();
        assert_eq!(to_units(1.234, unit).unwrap(), 1234);
        assert_eq!(from_units(1234, unit), 1.234);
        assert!(to_units(1.2345, unit).is_err());
        assert!(to_units(f64::NAN, unit).is_err());
    }

    #[test]
    fn credential_overlay_is_merged_without_entering_public_definition() {
        let public = br#"api_url = "https://example.invalid"
safety_timeout_ms = 5000
"#;
        let secret = SecretValue::new(b"api_key = \"key\"\nsecret = \"value\"\n".to_vec());
        let merged = merged_toml(public, &secret).unwrap();
        let value: toml::Value = toml::from_str(&merged).unwrap();
        assert_eq!(value["api_key"].as_str(), Some("key"));
        assert_eq!(value["safety_timeout_ms"].as_integer(), Some(5000));
    }

    #[test]
    fn id_interner_round_trips_short_long_and_command_ids() {
        let mut ids = IdInterner::default();
        let short = ids.intern("venue-1");
        assert_eq!(ids.resolve(short), "venue-1");
        let long = ids.intern("a-venue-identifier-that-is-longer-than-fifteen-bytes");
        assert_eq!(
            ids.resolve(long),
            "a-venue-identifier-that-is-longer-than-fifteen-bytes"
        );
        let command = account::Id128([0xAB; 16]);
        let text = id_text(command);
        assert_eq!(ids.intern(&text), command);
    }

    #[test]
    fn reconcile_scope_codes_are_stable() {
        assert_eq!(reconcile_scope_code(account::ReconcileScope::Full), 0);
        assert_eq!(reconcile_scope_code(account::ReconcileScope::Orders), 1);
        assert_eq!(reconcile_scope_code(account::ReconcileScope::Positions), 2);
        assert_eq!(reconcile_scope_code(account::ReconcileScope::Balances), 3);
    }
}
