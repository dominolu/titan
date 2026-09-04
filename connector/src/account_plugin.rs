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

use hftbacktest::types::{OrdType, Order, Side, Status, TimeInForce};
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
        AccountPublication, Connector, ConnectorBuilder, DirectPublication, PublishEvent,
        direct_publish_sender,
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
    shutdown_result: Arc<Mutex<Option<Result<(), account::AccountConnectorError>>>>,
}

#[async_trait::async_trait]
trait ShutdownActions: Send + Sync {
    async fn cancel_all(&self) -> Result<(), String>;
    async fn cancel_all_after(&self, timeout_ms: u64) -> Result<(), String>;
}

#[async_trait::async_trait]
trait ReconcileApi: Send + Sync {
    async fn open_orders(&self, symbol: &str) -> Result<Vec<OrderInfo>, crate::api::ApiError>;
    async fn positions(&self) -> Result<Vec<PositionInfo>, crate::api::ApiError>;
    async fn account(&self) -> Result<crate::api::AccountInfo, crate::api::ApiError>;
}

#[async_trait::async_trait]
impl<T: BrokerApi + ?Sized> ReconcileApi for T {
    async fn open_orders(&self, symbol: &str) -> Result<Vec<OrderInfo>, crate::api::ApiError> {
        self.get_open_orders(symbol).await
    }

    async fn positions(&self) -> Result<Vec<PositionInfo>, crate::api::ApiError> {
        self.get_positions(None).await
    }

    async fn account(&self) -> Result<crate::api::AccountInfo, crate::api::ApiError> {
        self.get_account().await
    }
}

struct VenueShutdownActions<'a> {
    connector: &'a dyn Connector,
    api: &'a dyn BrokerApi,
}

#[async_trait::async_trait]
impl ShutdownActions for VenueShutdownActions<'_> {
    async fn cancel_all(&self) -> Result<(), String> {
        self.connector.shutdown().await
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
struct Operations {
    values: HashMap<account::OperationId, account::AccountConnectorOperationSnapshot>,
    terminal: VecDeque<account::OperationId>,
}

struct CommandJournalEntry {
    command: Command,
    receipt: account::AccountCommandReceipt,
    client_order_id: Option<String>,
}

struct CommandJournal {
    values: HashMap<account::CommandId, CommandJournalEntry>,
    order: VecDeque<account::CommandId>,
    capacity: usize,
}

impl CommandJournal {
    fn new(capacity: usize) -> Self {
        Self {
            values: HashMap::with_capacity(capacity),
            order: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    fn insert(
        &mut self,
        id: account::CommandId,
        command: Command,
        receipt: account::AccountCommandReceipt,
        client_order_id: Option<String>,
    ) {
        while self.values.len() >= self.capacity {
            if let Some(expired) = self.order.pop_front() {
                self.values.remove(&expired);
            }
        }
        self.order.push_back(id);
        self.values.insert(
            id,
            CommandJournalEntry {
                command,
                receipt,
                client_order_id,
            },
        );
    }

    fn release_by_command(&mut self, id: &account::CommandId) {
        if self.values.remove(id).is_some() {
            self.order.retain(|candidate| candidate != id);
        }
    }

    fn release_by_client(&mut self, client_order_id: &str) {
        let released: Vec<account::CommandId> = self
            .values
            .iter()
            .filter(|(_, entry)| {
                entry
                    .client_order_id
                    .as_deref()
                    .is_some_and(|candidate| candidate == client_order_id)
            })
            .map(|(id, _)| *id)
            .collect();
        for id in released {
            self.release_by_command(&id);
        }
    }
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
            || parse_hex_id(text)
                .map(|id| self.by_id.contains_key(&id))
                .unwrap_or(false)
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
    journal: Arc<Mutex<CommandJournal>>,
    ids: Arc<Mutex<IdInterner>>,
}

impl AccountRuntime {
    fn new(
        connector: Box<dyn Connector>,
        api: Arc<dyn BrokerApi>,
        shutdown_policy: account::ShutdownOrderPolicy,
        context: account::AccountConnectorContext,
    ) -> Arc<Self> {
        let journal_capacity = context.command_queue_capacity.max(1);
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
            journal: Arc::new(Mutex::new(CommandJournal::new(journal_capacity))),
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
        let client_order_id = client.map(|c| resolve_id(&self.ids, c));
        let mut journal = self.journal.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(entry) = journal.values.get(&id) {
            return if entry.command == command {
                Ok(entry.receipt.clone())
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
        journal.insert(id, command, receipt.clone(), client_order_id);
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
        let mut connector = match self
            .connector
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
        {
            Some(connector) => connector,
            None => {
                self.active.store(false, Ordering::Release);
                return Err(rejected(
                    "account connector cannot restart after resources were released",
                ));
            }
        };
        for b in self.context.instruments.iter() {
            connector.register_account(b.native_symbol.to_string());
        }
        let runtime = match tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(error) => {
                *self.connector.lock().unwrap_or_else(|p| p.into_inner()) = Some(connector);
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
        let reconciling = self.reconciling.clone();
        let orders = self.orders.clone();
        let positions = self.positions.clone();
        let balances = self.balances.clone();
        let external_order_count = self.external_order_count.clone();
        let operations = self.operations.clone();
        let ids = self.ids.clone();
        let journal = self.journal.clone();
        let shutdown_policy = self.shutdown_policy.clone();
        let (command_tx, mut command_rx) = mpsc::channel(context.command_queue_capacity);
        let (stop_tx, mut stop_rx) = oneshot::channel::<Instant>();
        let shutdown_result = Arc::new(Mutex::new(None));
        let thread_shutdown_result = shutdown_result.clone();
        let recovery_tx = command_tx.clone();
        let thread = std::thread::Builder::new()
            .name(format!("account-{}", context.account.account_id.0))
            .spawn(move || {
                runtime.block_on(async move {
                    reconciling.store(true, Ordering::Release);
                    let encoder = Arc::new(AccountEventEncoder {
                        context: context.clone(),
                        epoch: epoch.clone(),
                        version: version.clone(),
                        ready: ready.clone(),
                        reconciling: reconciling.clone(),
                        ids: ids.clone(),
                        pending: Mutex::new(VecDeque::with_capacity(
                            context.command_queue_capacity,
                        )),
                        pending_capacity: context.command_queue_capacity,
                    });
                    let account_events = encoder.clone();
                    let event_recovery = recovery_tx.clone();
                    let stream_journal = journal.clone();
                    let tx = direct_publish_sender(move |publication| {
                        match publication {
                            DirectPublication::Event(PublishEvent::PrivateStreamReady) => {
                                let _ = event_recovery.try_send(Command::PrivateStreamReady);
                            }
                            DirectPublication::Account(AccountPublication::Error(_)) => {
                                account_events.invalidate(1);
                                let _ = event_recovery.try_send(Command::Reconcile(
                                    account::ReconcileScope::Full,
                                    account::OperationId(0),
                                ));
                            }
                            DirectPublication::Account(event) => {
                                let terminal_client = match event {
                                    AccountPublication::Order {
                                        client_order_id: Some(client_order_id),
                                        order,
                                        ..
                                    } if !order.active() => Some(client_order_id.clone()),
                                    _ => None,
                                };
                                if let Some(client_order_id) = terminal_client {
                                    stream_journal
                                        .lock()
                                        .unwrap_or_else(|p| p.into_inner())
                                        .release_by_client(&client_order_id);
                                }
                                if account_events.publish(event).is_err() {
                                    account_events.invalidate(2);
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
                                let result = match deadline {
                                    Err(_) => Err(account::AccountConnectorError::new(
                                        account::AccountErrorKind::ResourceReleaseFailed,
                                        "account stop signal was dropped",
                                    )),
                                    Ok(deadline) => execute_shutdown_policy(
                                        &VenueShutdownActions {
                                            connector: connector.as_ref(),
                                            api: api.as_ref(),
                                        },
                                        &shutdown_policy,
                                        deadline,
                                    ).await,
                                };
                                *thread_shutdown_result.lock().unwrap_or_else(|p| p.into_inner()) = Some(result);
                                break;
                            }
                            command = command_rx.recv() => match command {
                                Some(command) => {
                                    handle_command(
                                        command,
                                        &context,
                                        &*api,
                                        connector.as_ref(),
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
                                        &journal,
                                        &recovery_tx,
                                    )
                                    .await;
                                    if encoder.replay().is_err() {
                                        encoder.invalidate(2);
                                        schedule_reconcile(recovery_tx.clone());
                                    }
                                }
                                None => break,
                            }
                        }
                    }
                });
            })
            .map_err(|error| {
                self.active.store(false, Ordering::Release);
                rejected(error.to_string())
            })?;
        *self.running.lock().unwrap_or_else(|p| p.into_inner()) = Some(Running {
            command: command_tx,
            stop: Some(stop_tx),
            thread: Some(thread),
            shutdown_result,
        });
        Ok(())
    }
    fn stop(&self, deadline: Instant) -> Result<(), account::AccountConnectorError> {
        self.stop_inner(deadline)
    }
    fn submit(
        &self,
        mut c: account::SubmitOrderCommand,
    ) -> Result<account::AccountCommandReceipt, account::AccountConnectorError> {
        // All supported venues accept the 32 hexadecimal characters produced by Id128. Reusing
        // command_id makes a retry queryable without generating a second exchange order.
        if c.client_order_id.is_none() {
            c.client_order_id = Some(c.command_id);
        }
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

struct AccountEventEncoder {
    context: account::AccountConnectorContext,
    epoch: Arc<AtomicU64>,
    version: Arc<AtomicU64>,
    ready: Arc<AtomicBool>,
    reconciling: Arc<AtomicBool>,
    ids: Arc<Mutex<IdInterner>>,
    pending: Mutex<VecDeque<AccountPublication>>,
    pending_capacity: usize,
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
    fn publish(&self, event: &AccountPublication) -> Result<(), account::AccountConnectorError> {
        if (self.reconciling.load(Ordering::Acquire) || !self.context.event_publisher.is_open())
            && !matches!(event, AccountPublication::Error(_))
        {
            let mut pending = self.pending.lock().unwrap_or_else(|p| p.into_inner());
            if pending.len() == self.pending_capacity {
                return Err(account::AccountConnectorError::new(
                    account::AccountErrorKind::QueueFull,
                    "account fact staging queue is full",
                ));
            }
            pending.push_back(event.clone());
            return Ok(());
        }
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
                let venue_text = match venue_order_id {
                    Some(text) => text.clone(),
                    None => order.order_id.to_string(),
                };
                let id = self
                    .ids
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .intern(&venue_text);
                let client_order_id = client_order_id.as_deref().map(|text| {
                    self.ids
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .intern(text)
                });
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
                    client_order_id: client_order_id.unwrap_or_default(),
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
                        client_order_id: client_order_id.unwrap_or_default(),
                        ..Default::default()
                    };
                    self.context
                        .event_publisher
                        .publish_encoded(&fill, TraceContext::default())
                        .map_err(|e| rejected(e.to_string()))?;
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
                    position_side: if *qty < 0.0 { 2 } else { 1 },
                    quantity_lots: to_units(*qty, b.quantity_lot)?,
                    ..Default::default()
                };
                self.context
                    .event_publisher
                    .publish_encoded(&event, TraceContext::default())
                    .map_err(|e| rejected(e.to_string()))
            }
            AccountPublication::Error(_) => {
                self.invalidate(1);
                Ok(())
            }
        }
    }

    fn replay(&self) -> Result<(), account::AccountConnectorError> {
        if self.reconciling.load(Ordering::Acquire) || !self.context.event_publisher.is_open() {
            return Ok(());
        }
        let pending = std::mem::take(&mut *self.pending.lock().unwrap_or_else(|p| p.into_inner()));
        let mut iter = pending.into_iter();
        while let Some(event) = iter.next() {
            if let Err(error) = self.publish(&event) {
                let mut retained = VecDeque::with_capacity(iter.len() + 1);
                retained.push_back(event);
                retained.extend(iter);
                let mut queue = self.pending.lock().unwrap_or_else(|p| p.into_inner());
                while retained.len() < self.pending_capacity {
                    let Some(event) = queue.pop_front() else {
                        break;
                    };
                    retained.push_back(event);
                }
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

async fn reconcile<A: ReconcileApi + ?Sized>(
    context: &account::AccountConnectorContext,
    api: &A,
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
                .open_orders(&binding.native_symbol)
                .await
                .map_err(api_error)?
            {
                if is_external_order(&context.ownership, ids, &value.client_order_id) {
                    external = external.saturating_add(1);
                    continue;
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
                        TraceContext::default(),
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
        for value in api.positions().await.map_err(api_error)? {
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
                        value.update_time * 1_000_000,
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
        let info = api.account().await.map_err(api_error)?;
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
                        info.timestamp * 1_000_000,
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

#[allow(clippy::too_many_arguments)]
async fn handle_command(
    command: Command,
    context: &account::AccountConnectorContext,
    api: &dyn BrokerApi,
    connector: &dyn Connector,
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
    journal: &Arc<Mutex<CommandJournal>>,
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
    let (id, client, trace, mut result): (
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
            let side = match c.side {
                1 => Some(ApiSide::Buy),
                // Strategy ABI sells are i8 -1, which enters this u8 domain as 255.
                2 | 255 => Some(ApiSide::Sell),
                _ => None,
            };
            let order_type = match c.order_type {
                0 => Some(ApiOrderType::Limit),
                1 => Some(ApiOrderType::Market),
                _ => None,
            };
            let validation_error = if side.is_none() {
                Some("invalid submit side")
            } else if order_type.is_none() {
                Some("invalid submit order type")
            } else if c.time_in_force > 3 {
                Some("invalid submit time in force")
            } else {
                None
            };
            let req = UnifiedOrderRequest {
                symbol: b.native_symbol.to_string(),
                side: side.unwrap(),
                order_type: order_type.unwrap(),
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
                if let Some(message) = validation_error {
                    Err(crate::api::ApiError::new(
                        "connector",
                        "INVALID_SUBMIT",
                        message,
                    ))
                } else {
                    if let Some(client_order_id) = c.client_order_id
                        && let Ok(order) = managed_account_order(c, b)
                    {
                        connector.track_managed_order(
                            &b.native_symbol,
                            &id_text(client_order_id),
                            &order,
                        );
                    }
                    api.submit_order(&req).await.map(Some)
                },
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
    let initially_unknown = result
        .as_ref()
        .err()
        .is_some_and(crate::api::ApiError::outcome_unknown);
    if initially_unknown
        && let Some((symbol, order_id, client_order_id)) =
            command_query_target(&command, context, ids)
        && let Ok(order) = api
            .get_order(&symbol, order_id.as_deref(), client_order_id.as_deref())
            .await
    {
        result = Ok(Some(order));
    }
    let unknown = result
        .as_ref()
        .err()
        .is_some_and(crate::api::ApiError::outcome_unknown);
    if unknown {
        ready.store(false, Ordering::Release);
        publish_invalidated(context, epoch, version, 3);
        schedule_reconcile(recovery_tx.clone());
    }
    if !unknown {
        let terminal_order = result
            .as_ref()
            .ok()
            .and_then(|order| order.as_ref())
            .is_some_and(|value| {
                matches!(
                    value.status,
                    ApiOrderStatus::Filled
                        | ApiOrderStatus::Canceled
                        | ApiOrderStatus::Rejected
                        | ApiOrderStatus::Expired
                )
            });
        let terminal_client = if terminal_order {
            client.map(|c| resolve_id(ids, c))
        } else {
            None
        };
        let release_any = terminal_order
            || result.is_err()
            || matches!(&command, Command::CancelAll(_) | Command::CancelAllAfter(_));
        let mut journal = journal.lock().unwrap_or_else(|p| p.into_inner());
        if release_any {
            journal.release_by_command(&id);
        }
        if let Some(client_order_id) = terminal_client {
            journal.release_by_client(&client_order_id);
        }
    }
    let outcome = if result.is_ok() {
        1
    } else if unknown {
        0
    } else {
        2
    };
    let event = account::CommandResultV1 {
        header: header(
            context,
            epoch,
            version,
            account::event_kind::COMMAND_RESULT,
            if unknown {
                0
            } else {
                account::event_flags::FINAL
            },
            now_ns(),
        ),
        command_id: id,
        client_order_id: client.unwrap_or_default(),
        outcome,
        final_result: u8::from(!unknown),
        reason_code: u32::from(result.is_err()),
    };
    let mut publication_failed = context
        .event_publisher
        .publish_encoded(&event, trace)
        .is_err();
    if let Ok(Some(value)) = result {
        if let Some(binding) = context
            .instruments
            .iter()
            .find(|b| b.native_symbol.eq_ignore_ascii_case(&value.symbol))
        {
            if let Ok(mut snapshot) = order_snapshot(&value, binding, ids) {
                snapshot.command_id = id;
                if let Some(client_order_id) = client {
                    snapshot.client_order_id = client_order_id;
                }
                publication_failed |= publish_order_snapshot(
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
                    trace,
                )
                .is_err();
            }
        }
    }
    if publication_failed && !unknown {
        ready.store(false, Ordering::Release);
        publish_invalidated(context, epoch, version, 4);
        schedule_reconcile(recovery_tx.clone());
    }
}

fn command_query_target(
    command: &Command,
    context: &account::AccountConnectorContext,
    ids: &Mutex<IdInterner>,
) -> Option<(String, Option<String>, Option<String>)> {
    let (asset_id, venue_order_id, client_order_id) = match command {
        Command::Submit(value) => (value.asset_id, None, value.client_order_id),
        Command::Amend(value) => (value.asset_id, value.venue_order_id, value.client_order_id),
        Command::Cancel(value) => (value.asset_id, value.venue_order_id, value.client_order_id),
        Command::CancelAll(_)
        | Command::CancelAllAfter(_)
        | Command::Reconcile(..)
        | Command::PrivateStreamReady => return None,
    };
    let symbol = context
        .instruments
        .iter()
        .find(|binding| binding.asset_id == asset_id)?
        .native_symbol
        .to_string();
    let ids = ids.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    Some((
        symbol,
        venue_order_id.map(|id| ids.resolve(id)),
        client_order_id.map(|id| ids.resolve(id)),
    ))
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
    trace: TraceContext,
) -> Result<(), account::AccountConnectorError> {
    let event = account::OrderChangedV1 {
        header: header(
            context,
            epoch,
            version,
            account::event_kind::ORDER_CHANGED,
            flags,
            // BrokerApi timestamps are venue milliseconds; the account ABI header is ns and must
            // stay in the same clock domain as private-stream facts for cross-source ordering.
            source.update_time * 1_000_000,
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
        .publish_encoded(&event, trace)
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
fn decimal_unit_f64(unit: &account::DecimalUnit) -> f64 {
    unit.coefficient() as f64 / 10_f64.powi(i32::from(unit.scale()))
}
fn managed_account_order(
    command: &account::SubmitOrderCommand,
    binding: &account::AccountInstrumentBinding,
) -> Result<Order, account::AccountConnectorError> {
    let side = if command.side == 1 {
        Side::Buy
    } else {
        Side::Sell
    };
    let order_type = if command.order_type == 0 {
        OrdType::Limit
    } else {
        OrdType::Market
    };
    let time_in_force = match command.time_in_force {
        0 => TimeInForce::GTC,
        1 => TimeInForce::GTX,
        2 => TimeInForce::FOK,
        3 => TimeInForce::IOC,
        _ => return Err(rejected("invalid submit time in force")),
    };
    let qty = from_units(command.quantity_lots, binding.quantity_lot);
    let order_id = u64::from_le_bytes(command.command_id.0[..8].try_into().unwrap());
    let mut order = Order::new(
        order_id,
        command.price_ticks,
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
    use titan_account_plugin::AccountConnector as _;

    #[derive(Default)]
    struct NoopOrders;

    impl crate::connector::GetOrders for NoopOrders {
        fn orders(&self, _: Option<String>) -> Vec<hftbacktest::types::Order> {
            Vec::new()
        }
    }

    struct LifecycleConnector {
        orders: Arc<Mutex<NoopOrders>>,
        shutdown_delay: Duration,
    }

    #[async_trait::async_trait]
    impl Connector for LifecycleConnector {
        fn register(&mut self, _: String) {}

        fn order_manager(&self) -> Arc<Mutex<dyn crate::connector::GetOrders + Send + 'static>> {
            self.orders.clone()
        }

        fn run(&mut self, _: crate::connector::PublishSender) {}

        fn submit(
            &self,
            _: String,
            _: hftbacktest::types::Order,
            _: crate::connector::PublishSender,
        ) {
        }

        fn cancel(
            &self,
            _: String,
            _: hftbacktest::types::Order,
            _: crate::connector::PublishSender,
        ) {
        }

        async fn shutdown(&self) -> Result<(), String> {
            tokio::time::sleep(self.shutdown_delay).await;
            Ok(())
        }
    }

    #[derive(Default)]
    struct ShutdownProbe {
        calls: Mutex<Vec<String>>,
        delay: Duration,
        error: Option<String>,
    }

    #[async_trait::async_trait]
    impl ShutdownActions for ShutdownProbe {
        async fn cancel_all(&self) -> Result<(), String> {
            self.calls.lock().unwrap().push("cancel_all".into());
            tokio::time::sleep(self.delay).await;
            self.error.clone().map_or(Ok(()), Err)
        }

        async fn cancel_all_after(&self, timeout_ms: u64) -> Result<(), String> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("cancel_all_after:{timeout_ms}"));
            tokio::time::sleep(self.delay).await;
            self.error.clone().map_or(Ok(()), Err)
        }
    }

    struct NoopAccountSink;
    impl account::AccountEventSink for NoopAccountSink {
        fn publish(
            &self,
            _: &str,
            _: &[u8],
            _: TraceContext,
        ) -> Result<(), titan_plugin_engine::PluginError> {
            Ok(())
        }
    }

    struct RecordingAccountSink(Mutex<Vec<String>>);
    impl account::AccountEventSink for RecordingAccountSink {
        fn publish(
            &self,
            event_type: &str,
            _: &[u8],
            _: TraceContext,
        ) -> Result<(), titan_plugin_engine::PluginError> {
            self.0.lock().unwrap().push(event_type.to_owned());
            Ok(())
        }
    }

    struct ReconcileProbe {
        order: OrderInfo,
        fail_orders: bool,
    }

    #[async_trait::async_trait]
    impl ReconcileApi for ReconcileProbe {
        async fn open_orders(&self, _: &str) -> Result<Vec<OrderInfo>, crate::api::ApiError> {
            if self.fail_orders {
                Err(crate::api::ApiError::transport("test", "injected"))
            } else {
                Ok(vec![self.order.clone()])
            }
        }

        async fn positions(&self) -> Result<Vec<PositionInfo>, crate::api::ApiError> {
            Ok(Vec::new())
        }

        async fn account(&self) -> Result<crate::api::AccountInfo, crate::api::ApiError> {
            Ok(crate::api::AccountInfo {
                total_wallet_balance: 10.0,
                total_margin_balance: 10.0,
                total_unrealized_pnl: 0.0,
                available_balance: 10.0,
                balances: vec![Balance {
                    asset: "USDT".into(),
                    wallet_balance: 10.0,
                    available_balance: 10.0,
                    unrealized_pnl: 0.0,
                    margin_balance: 10.0,
                }],
                timestamp: 20,
            })
        }
    }

    fn reconciliation_context(
        ownership: account::OrderOwnershipPolicy,
        sink: Arc<dyn account::AccountEventSink>,
    ) -> account::AccountConnectorContext {
        let account_handle = account::AccountHandle {
            account_id: account::AccountId(7),
            generation: 1,
        };
        let scope = titan_plugin_engine::ResourceScope::new(
            titan_plugin_engine::PluginIdentity::new("test", "account-reconcile"),
        );
        let unit: account::DecimalUnit = "0.1".parse().unwrap();
        account::AccountConnectorContext {
            account: account_handle,
            instruments: Arc::from([account::AccountInstrumentBinding {
                native_symbol: Arc::from("BTCUSDT"),
                asset_id: account::AssetId(11),
                price_tick: unit,
                quantity_lot: unit,
                contract_multiplier: "1".parse().unwrap(),
            }]),
            currencies: Arc::from([account::AccountCurrencyBinding {
                native_currency: Arc::from("USDT"),
                currency_id: account::CurrencyId(1),
                amount_unit: unit,
            }]),
            ownership,
            account_stream: account::SourceStreamId(1),
            control_stream: account::SourceStreamId(2),
            event_publisher: account::AccountEventPublisher::from_sink(account_handle, sink),
            resources: scope.handle(),
            secrets: account::ScopedSecretResolver::scoped(
                account::SecretRef::new("secret://test/account"),
                Arc::new(account::UnavailableSecretProvider),
            ),
            command_queue_capacity: 8,
        }
    }

    fn lifecycle_context(
        resources: titan_plugin_engine::ResourceScopeHandle,
    ) -> account::AccountConnectorContext {
        let account_handle = account::AccountHandle {
            account_id: account::AccountId(9),
            generation: 1,
        };
        account::AccountConnectorContext {
            account: account_handle,
            instruments: Arc::from([]),
            currencies: Arc::from([]),
            ownership: account::OrderOwnershipPolicy::ObserveAll,
            account_stream: account::SourceStreamId(1),
            control_stream: account::SourceStreamId(2),
            event_publisher: account::AccountEventPublisher::from_sink(
                account_handle,
                Arc::new(NoopAccountSink),
            ),
            resources,
            secrets: account::ScopedSecretResolver::scoped(
                account::SecretRef::new("secret://test/account"),
                Arc::new(account::UnavailableSecretProvider),
            ),
            command_queue_capacity: 2,
        }
    }

    fn lifecycle_runtime(
        scope: &titan_plugin_engine::ResourceScope,
        shutdown_policy: account::ShutdownOrderPolicy,
        shutdown_delay: Duration,
    ) -> Arc<AccountRuntime> {
        let venue = crate::binancefutures::BinanceFutures::build_from(
            "stream_url = \"ws://127.0.0.1:1\"\napi_url = \"http://127.0.0.1:1\"\n",
        )
        .unwrap();
        let api = venue.broker_api().unwrap();
        AccountRuntime::new(
            Box::new(LifecycleConnector {
                orders: Arc::new(Mutex::new(NoopOrders)),
                shutdown_delay,
            }),
            api,
            shutdown_policy,
            lifecycle_context(scope.handle()),
        )
    }

    fn external_order() -> OrderInfo {
        OrderInfo {
            symbol: "BTCUSDT".into(),
            order_id: "venue-1".into(),
            client_order_id: "manual-1".into(),
            side: ApiSide::Buy,
            order_type: ApiOrderType::Limit,
            status: ApiOrderStatus::New,
            price: 100.0,
            qty: 1.0,
            executed_qty: 0.0,
            avg_price: 0.0,
            leaves_qty: 1.0,
            time_in_force: ApiTimeInForce::GTC,
            reduce_only: false,
            position_side: ApiPositionSide::Long,
            create_time: 10,
            update_time: 11,
            stop_price: None,
        }
    }

    async fn run_reconcile_probe(
        context: &account::AccountConnectorContext,
        api: &ReconcileProbe,
    ) -> (
        Result<(), account::AccountConnectorError>,
        bool,
        bool,
        Arc<[account::OrderSnapshot]>,
        u64,
    ) {
        let epoch = AtomicU64::new(0);
        let version = AtomicU64::new(0);
        let reconciling = AtomicBool::new(false);
        let ready = AtomicBool::new(false);
        let orders = Mutex::new(Arc::from([]));
        let positions = Mutex::new(Arc::from([]));
        let balances = Mutex::new(Arc::from([]));
        let external = AtomicU64::new(0);
        let ids = Mutex::new(IdInterner::default());
        let result = reconcile(
            context,
            api,
            account::ReconcileScope::Full,
            &epoch,
            &version,
            &reconciling,
            &ready,
            &orders,
            &positions,
            &balances,
            &external,
            &ids,
        )
        .await;
        (
            result,
            ready.into_inner(),
            reconciling.into_inner(),
            orders.into_inner().unwrap(),
            external.into_inner(),
        )
    }

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

    #[test]
    fn command_journal_is_bounded_and_preserves_the_latest_receipts() {
        let command = |id: u8| {
            Command::CancelAllAfter(account::CancelAllAfterCommand {
                command_id: account::Id128([id; 16]),
                timeout_ms: 1,
                trace: TraceContext::default(),
            })
        };
        let receipt = |id: u8| account::AccountCommandReceipt {
            account: account::AccountHandle {
                account_id: account::AccountId(1),
                generation: 1,
            },
            command_id: account::Id128([id; 16]),
            client_order_id: None,
            accepted_at: i64::from(id),
        };
        let mut journal = CommandJournal::new(2);
        for id in 1..=3 {
            journal.insert(
                account::Id128([id; 16]),
                command(id),
                receipt(id),
                None,
            );
        }
        assert_eq!(journal.values.len(), 2);
        assert!(!journal.values.contains_key(&account::Id128([1; 16])));
        assert!(journal.values.contains_key(&account::Id128([2; 16])));
        assert!(journal.values.contains_key(&account::Id128([3; 16])));
    }

    #[test]
    fn command_journal_terminal_release_frees_command_and_client_entries() {
        let entry = |id: u8, client_id: Option<String>| {
            (
                account::Id128([id; 16]),
                Command::CancelAllAfter(account::CancelAllAfterCommand {
                    command_id: account::Id128([id; 16]),
                    timeout_ms: 1,
                    trace: TraceContext::default(),
                }),
                account::AccountCommandReceipt {
                    account: account::AccountHandle {
                        account_id: account::AccountId(1),
                        generation: 1,
                    },
                    command_id: account::Id128([id; 16]),
                    client_order_id: None,
                    accepted_at: i64::from(id),
                },
                client_id,
            )
        };
        let mut journal = CommandJournal::new(4);
        let (id_a, command_a, receipt_a, client_a) = entry(1, Some("client-a".to_owned()));
        let (id_b, command_b, receipt_b, client_b) = entry(2, Some("client-a".to_owned()));
        let (id_c, command_c, receipt_c, client_c) = entry(3, None);
        let (id_d, command_d, receipt_d, client_d) = entry(4, Some("client-d".to_owned()));
        journal.insert(id_a, command_a, receipt_a, client_a);
        journal.insert(id_b, command_b, receipt_b, client_b);
        journal.insert(id_c, command_c, receipt_c, client_c);
        journal.insert(id_d, command_d, receipt_d, client_d);

        journal.release_by_client("client-a");
        assert!(!journal.values.contains_key(&id_a));
        assert!(!journal.values.contains_key(&id_b));
        assert!(journal.values.contains_key(&id_c));
        assert!(journal.values.contains_key(&id_d));

        journal.release_by_command(&id_d);
        assert!(!journal.values.contains_key(&id_d));
        // A released command no longer consumes bounded capacity.
        let (id_e, command_e, receipt_e, _) = entry(5, None);
        journal.insert(id_e, command_e, receipt_e, None);
        assert_eq!(journal.values.len(), 2);
        assert!(journal.values.contains_key(&id_c));
        assert!(journal.values.contains_key(&id_e));
    }

    #[test]
    fn terminal_operation_history_stays_bounded_under_long_running_churn() {
        let mut operations = Operations::default();
        for value in 1..=10_000 {
            let id = account::OperationId(value);
            operations.set(id, account::OperationState::Pending, "queued");
            operations.set(id, account::OperationState::Succeeded, "done");
        }
        assert_eq!(operations.values.len(), OPERATION_HISTORY_LIMIT);
        assert_eq!(operations.terminal.len(), OPERATION_HISTORY_LIMIT);
        assert!(!operations.values.contains_key(&account::OperationId(1)));
        assert!(
            operations
                .values
                .contains_key(&account::OperationId(10_000))
        );
    }

    #[test]
    fn account_runtime_stop_is_idempotent_restart_is_rejected_and_scope_has_no_cycle() {
        let mut scope = titan_plugin_engine::ResourceScope::new(
            titan_plugin_engine::PluginIdentity::new("test", "account-lifecycle"),
        );
        let runtime = lifecycle_runtime(
            &scope,
            account::ShutdownOrderPolicy::LeaveOpen,
            Duration::ZERO,
        );
        let weak = Arc::downgrade(&runtime);
        runtime.start().unwrap();
        runtime
            .stop(Instant::now() + Duration::from_secs(1))
            .unwrap();
        runtime
            .stop(Instant::now() + Duration::from_secs(1))
            .unwrap();
        assert_eq!(
            runtime.start().unwrap_err().kind,
            account::AccountErrorKind::ConnectorRejected
        );
        drop(runtime);
        scope.close().unwrap();
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn account_runtime_retains_and_reaps_join_handle_after_stop_deadline() {
        let mut scope = titan_plugin_engine::ResourceScope::new(
            titan_plugin_engine::PluginIdentity::new("test", "account-stop-timeout"),
        );
        let runtime = lifecycle_runtime(
            &scope,
            account::ShutdownOrderPolicy::CancelAll,
            Duration::from_millis(50),
        );
        let weak = Arc::downgrade(&runtime);
        runtime.start().unwrap();
        assert_eq!(
            runtime.stop(Instant::now()).unwrap_err().kind,
            account::AccountErrorKind::DeadlineExceeded
        );
        std::thread::sleep(Duration::from_millis(10));
        assert_eq!(
            runtime
                .stop(Instant::now() + Duration::from_secs(1))
                .unwrap_err()
                .kind,
            account::AccountErrorKind::DeadlineExceeded
        );
        // The second call joined the retained worker and consumed its terminal error.
        runtime
            .stop(Instant::now() + Duration::from_secs(1))
            .unwrap();
        drop(runtime);
        scope.close().unwrap();
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn managed_only_distinguishes_external_orders_without_hiding_known_ids() {
        let ownership = account::OrderOwnershipPolicy::ManagedOnly {
            client_id_prefix: Arc::from("titan-"),
        };
        let ids = Mutex::new(IdInterner::default());
        ids.lock().unwrap().intern("manual-but-journaled");
        assert!(!is_external_order(&ownership, &ids, "titan-123"));
        assert!(!is_external_order(&ownership, &ids, "manual-but-journaled"));
        assert!(is_external_order(&ownership, &ids, "external-123"));
        assert!(!is_external_order(
            &account::OrderOwnershipPolicy::ObserveAll,
            &ids,
            "external-123"
        ));
    }

    #[tokio::test]
    async fn shutdown_policies_select_one_bounded_venue_action_and_propagate_failures() {
        let leave_open = ShutdownProbe::default();
        execute_shutdown_policy(
            &leave_open,
            &account::ShutdownOrderPolicy::LeaveOpen,
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert!(leave_open.calls.lock().unwrap().is_empty());

        let cancel_all = ShutdownProbe::default();
        execute_shutdown_policy(
            &cancel_all,
            &account::ShutdownOrderPolicy::CancelAll,
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert_eq!(cancel_all.calls.lock().unwrap().as_slice(), &["cancel_all"]);

        let cancel_after = ShutdownProbe {
            error: Some("venue rejected safety timer".into()),
            ..ShutdownProbe::default()
        };
        let error = execute_shutdown_policy(
            &cancel_after,
            &account::ShutdownOrderPolicy::CancelAllAfter { timeout_ms: 2_500 },
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind, account::AccountErrorKind::ConnectorRejected);
        assert_eq!(
            cancel_after.calls.lock().unwrap().as_slice(),
            &["cancel_all_after:2500"]
        );

        let timeout = ShutdownProbe {
            delay: Duration::from_millis(50),
            ..ShutdownProbe::default()
        };
        let error = execute_shutdown_policy(
            &timeout,
            &account::ShutdownOrderPolicy::CancelAll,
            Instant::now() + Duration::from_millis(1),
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind, account::AccountErrorKind::DeadlineExceeded);
    }

    #[tokio::test]
    async fn full_reconcile_commits_one_epoch_and_observe_all_keeps_external_orders() {
        let sink = Arc::new(RecordingAccountSink(Mutex::new(Vec::new())));
        let context =
            reconciliation_context(account::OrderOwnershipPolicy::ObserveAll, sink.clone());
        let api = ReconcileProbe {
            order: external_order(),
            fail_orders: false,
        };
        let epoch = AtomicU64::new(0);
        let version = AtomicU64::new(0);
        let reconciling = AtomicBool::new(false);
        let ready = AtomicBool::new(false);
        let orders = Mutex::new(Arc::from([]));
        let positions = Mutex::new(Arc::from([]));
        let balances = Mutex::new(Arc::from([]));
        let external = AtomicU64::new(99);
        let ids = Mutex::new(IdInterner::default());

        reconcile(
            &context,
            &api,
            account::ReconcileScope::Full,
            &epoch,
            &version,
            &reconciling,
            &ready,
            &orders,
            &positions,
            &balances,
            &external,
            &ids,
        )
        .await
        .unwrap();

        assert_eq!(epoch.load(Ordering::Acquire), 1);
        assert!(ready.load(Ordering::Acquire));
        assert!(!reconciling.load(Ordering::Acquire));
        assert_eq!(orders.lock().unwrap().len(), 1);
        assert_eq!(balances.lock().unwrap().len(), 1);
        assert_eq!(external.load(Ordering::Acquire), 0);
        assert_eq!(
            sink.0.lock().unwrap().as_slice(),
            &[
                account::RECONCILE_STARTED_EVENT,
                account::ORDER_CHANGED_EVENT,
                account::BALANCE_CHANGED_EVENT,
                account::RECONCILE_COMPLETED_EVENT,
                account::STREAM_STATE_CHANGED_EVENT,
            ]
        );
    }

    #[tokio::test]
    async fn failed_reconcile_never_opens_ready_and_managed_only_excludes_external_orders() {
        let sink = Arc::new(RecordingAccountSink(Mutex::new(Vec::new())));
        let context = reconciliation_context(
            account::OrderOwnershipPolicy::ManagedOnly {
                client_id_prefix: Arc::from("titan-"),
            },
            sink,
        );
        let successful = run_reconcile_probe(
            &context,
            &ReconcileProbe {
                order: external_order(),
                fail_orders: false,
            },
        )
        .await;
        assert!(successful.0.is_ok());
        assert!(successful.1);
        assert!(!successful.2);
        assert!(successful.3.is_empty());
        assert_eq!(successful.4, 1);

        let failed = run_reconcile_probe(
            &context,
            &ReconcileProbe {
                order: external_order(),
                fail_orders: true,
            },
        )
        .await;
        assert!(failed.0.is_err());
        assert!(!failed.1);
        assert!(failed.2);
    }

    #[tokio::test]
    async fn repeated_reconcile_failure_and_recovery_keeps_state_bounded_and_returns_ready() {
        let context = reconciliation_context(
            account::OrderOwnershipPolicy::ObserveAll,
            Arc::new(NoopAccountSink),
        );
        let healthy = ReconcileProbe {
            order: external_order(),
            fail_orders: false,
        };
        let failing = ReconcileProbe {
            order: external_order(),
            fail_orders: true,
        };
        let epoch = AtomicU64::new(0);
        let version = AtomicU64::new(0);
        let reconciling = AtomicBool::new(false);
        let ready = AtomicBool::new(false);
        let orders = Mutex::new(Arc::from([]));
        let positions = Mutex::new(Arc::from([]));
        let balances = Mutex::new(Arc::from([]));
        let external = AtomicU64::new(0);
        let ids = Mutex::new(IdInterner::default());

        for cycle in 0..2_000 {
            let api = if cycle % 10 == 0 { &failing } else { &healthy };
            let result = reconcile(
                &context,
                api,
                account::ReconcileScope::Full,
                &epoch,
                &version,
                &reconciling,
                &ready,
                &orders,
                &positions,
                &balances,
                &external,
                &ids,
            )
            .await;
            if cycle % 10 == 0 {
                assert!(result.is_err());
                assert!(!ready.load(Ordering::Acquire));
                assert!(reconciling.load(Ordering::Acquire));
            } else {
                result.unwrap();
                assert!(ready.load(Ordering::Acquire));
                assert!(!reconciling.load(Ordering::Acquire));
            }
            assert!(orders.lock().unwrap().len() <= 1);
            assert!(positions.lock().unwrap().is_empty());
            assert!(balances.lock().unwrap().len() <= 1);
        }

        // End on an authoritative successful snapshot after all injected failures.
        reconcile(
            &context,
            &healthy,
            account::ReconcileScope::Full,
            &epoch,
            &version,
            &reconciling,
            &ready,
            &orders,
            &positions,
            &balances,
            &external,
            &ids,
        )
        .await
        .unwrap();
        assert!(ready.load(Ordering::Acquire));
        assert_eq!(orders.lock().unwrap().len(), 1);
        assert_eq!(balances.lock().unwrap().len(), 1);
        assert_eq!(ids.lock().unwrap().by_text.len(), 2);
    }

    #[test]
    fn account_fact_staging_is_bounded_and_reports_queue_full() {
        let account_handle = account::AccountHandle {
            account_id: account::AccountId(7),
            generation: 1,
        };
        let scope = titan_plugin_engine::ResourceScope::new(
            titan_plugin_engine::PluginIdentity::new("test", "account-encoder"),
        );
        let secret_ref = account::SecretRef::new("secret://test/account");
        let context = account::AccountConnectorContext {
            account: account_handle,
            instruments: Arc::from([]),
            currencies: Arc::from([]),
            ownership: account::OrderOwnershipPolicy::ObserveAll,
            account_stream: account::SourceStreamId(1),
            control_stream: account::SourceStreamId(2),
            event_publisher: account::AccountEventPublisher::from_sink(
                account_handle,
                Arc::new(NoopAccountSink),
            ),
            resources: scope.handle(),
            secrets: account::ScopedSecretResolver::scoped(
                secret_ref,
                Arc::new(account::UnavailableSecretProvider),
            ),
            command_queue_capacity: 1,
        };
        let encoder = AccountEventEncoder {
            context,
            epoch: Arc::new(AtomicU64::new(1)),
            version: Arc::new(AtomicU64::new(0)),
            ready: Arc::new(AtomicBool::new(false)),
            reconciling: Arc::new(AtomicBool::new(true)),
            ids: Arc::new(Mutex::new(IdInterner::default())),
            pending: Mutex::new(VecDeque::with_capacity(1)),
            pending_capacity: 1,
        };
        let fact = AccountPublication::Position {
            symbol: "BTC".to_owned(),
            qty: 1.0,
            exch_ts: 1,
        };
        encoder.publish(&fact).unwrap();
        let error = encoder.publish(&fact).unwrap_err();
        assert_eq!(error.kind, account::AccountErrorKind::QueueFull);
        assert_eq!(encoder.pending.lock().unwrap().len(), 1);
    }
}
