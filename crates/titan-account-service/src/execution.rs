use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use crate::{AccountConnector, AccountHandle, AssetId, ClientOrderId, Id128};

pub type ExecutionTaskId = u64;
const LATENCY_BUCKETS: usize = 64;
pub type ExecutionFuture =
    Pin<Box<dyn Future<Output = Result<DirectOrderInfo, DirectExecutionError>> + Send + 'static>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectNewOrderRequest {
    pub asset_id: AssetId,
    /// Strategy ABI direction: +1 buy, -1 sell.
    pub side: i8,
    pub order_type: u8,
    pub time_in_force: u8,
    pub price_ticks: i64,
    pub quantity_lots: i64,
    pub reduce_only: bool,
    pub client_order_id: ClientOrderId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectCancelOrderRequest {
    pub asset_id: AssetId,
    pub client_order_id: Option<ClientOrderId>,
    pub venue_order_id: Option<Id128>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectOrderInfo {
    pub client_order_id: Option<String>,
    pub venue_order_id: Option<String>,
    pub status: u8,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectExecutionError {
    pub exchange: &'static str,
    pub code: String,
    pub message: String,
    pub outcome_unknown: bool,
}

impl std::fmt::Display for DirectExecutionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "[{}] {}: {}",
            self.exchange, self.code, self.message
        )
    }
}

impl std::error::Error for DirectExecutionError {}

pub trait DirectExecutionConnector: Send + Sync + 'static {
    fn submit(&self, request: DirectNewOrderRequest) -> ExecutionFuture;
    fn cancel(&self, request: DirectCancelOrderRequest) -> ExecutionFuture;
}

pub(crate) struct AccountConnectorExecution {
    connector: Arc<dyn AccountConnector>,
}

impl AccountConnectorExecution {
    pub(crate) fn new(connector: Arc<dyn AccountConnector>) -> Self {
        Self { connector }
    }
}

impl DirectExecutionConnector for AccountConnectorExecution {
    fn submit(&self, request: DirectNewOrderRequest) -> ExecutionFuture {
        self.connector.direct_submit(request)
    }

    fn cancel(&self, request: DirectCancelOrderRequest) -> ExecutionFuture {
        self.connector.direct_cancel(request)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ObservedExecutionOutcome {
    Accepted(DirectOrderInfo),
    Rejected(DirectExecutionError),
    Unknown(DirectExecutionError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionRequestKind {
    Submit,
    Cancel,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservedExecutionResult {
    pub task_id: ExecutionTaskId,
    pub account: AccountHandle,
    pub asset_id: AssetId,
    pub request_kind: ExecutionRequestKind,
    pub client_order_id: Option<ClientOrderId>,
    pub outcome: ObservedExecutionOutcome,
}

pub trait ExecutionObserver: Send + Sync + 'static {
    fn observe(&self, result: ObservedExecutionResult);
}

#[derive(Default)]
pub struct TracingExecutionObserver;

impl ExecutionObserver for TracingExecutionObserver {
    fn observe(&self, result: ObservedExecutionResult) {
        match &result.outcome {
            ObservedExecutionOutcome::Accepted(order) => tracing::info!(
                task_id = result.task_id,
                account_id = result.account.account_id.0,
                venue_order_id = order.venue_order_id.as_deref().unwrap_or_default(),
                "direct execution REST request accepted"
            ),
            ObservedExecutionOutcome::Rejected(error) => tracing::warn!(
                task_id = result.task_id,
                account_id = result.account.account_id.0,
                exchange = error.exchange,
                code = error.code,
                message = error.message.as_str(),
                "direct execution REST request rejected"
            ),
            ObservedExecutionOutcome::Unknown(error) => tracing::error!(
                task_id = result.task_id,
                account_id = result.account.account_id.0,
                exchange = error.exchange,
                code = error.code,
                message = error.message.as_str(),
                "direct execution REST outcome unknown"
            ),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SpawnError {
    RuntimeStopping,
    ExecutorSaturated,
    InvalidBinding,
}

impl std::fmt::Display for SpawnError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::RuntimeStopping => "execution runtime is stopping",
            Self::ExecutorSaturated => "execution runtime active-task limit reached",
            Self::InvalidBinding => "execution handle has an invalid account binding",
        })
    }
}

impl std::error::Error for SpawnError {}

struct ExecutionState {
    accepting: AtomicBool,
    active: AtomicUsize,
    max_active: usize,
    next_task_id: AtomicU64,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    spawned: AtomicU64,
    saturated: AtomicU64,
    stopping_rejections: AtomicU64,
    future_allocations: AtomicU64,
    spawn_latency: [AtomicU64; LATENCY_BUCKETS],
}

struct ActivePermit(Arc<ExecutionState>);

impl Drop for ActivePermit {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ExecutionMetricsSnapshot {
    pub active_tasks: usize,
    pub spawned_tasks: u64,
    pub saturated_rejections: u64,
    pub stopping_rejections: u64,
    /// One boxed connector future is mandated by `DirectExecutionConnector` per request.
    pub connector_future_allocations: u64,
    pub spawn_p50_ns: u64,
    pub spawn_p99_ns: u64,
    pub spawn_p999_ns: u64,
    pub spawn_max_ns: u64,
}

pub struct ExecutionRuntime {
    runtime: Option<tokio::runtime::Runtime>,
    state: Arc<ExecutionState>,
}

impl ExecutionRuntime {
    pub fn new(worker_threads: usize, max_active_tasks: usize) -> Result<Self, std::io::Error> {
        if worker_threads == 0 || max_active_tasks == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "execution worker and active-task capacities must be non-zero",
            ));
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .thread_name("titan-execution")
            .enable_all()
            .build()?;
        Ok(Self {
            runtime: Some(runtime),
            state: Arc::new(ExecutionState {
                accepting: AtomicBool::new(true),
                active: AtomicUsize::new(0),
                max_active: max_active_tasks,
                next_task_id: AtomicU64::new(1),
                tasks: Mutex::new(Vec::new()),
                spawned: AtomicU64::new(0),
                saturated: AtomicU64::new(0),
                stopping_rejections: AtomicU64::new(0),
                future_allocations: AtomicU64::new(0),
                spawn_latency: std::array::from_fn(|_| AtomicU64::new(0)),
            }),
        })
    }

    pub fn bind(
        &self,
        account: AccountHandle,
        connector: Arc<dyn DirectExecutionConnector>,
        observer: Arc<dyn ExecutionObserver>,
    ) -> ExecutionHandle {
        self.dispatcher().bind(account, connector, observer)
    }

    pub fn dispatcher(&self) -> ExecutionDispatcher {
        ExecutionDispatcher {
            executor: self
                .runtime
                .as_ref()
                .expect("runtime is alive")
                .handle()
                .clone(),
            state: self.state.clone(),
        }
    }

    pub fn active_tasks(&self) -> usize {
        self.state.active.load(Ordering::Acquire)
    }

    pub fn metrics(&self) -> ExecutionMetricsSnapshot {
        metrics_snapshot(&self.state)
    }

    pub fn shutdown(&mut self, deadline: Instant) {
        self.state.accepting.store(false, Ordering::Release);
        while self.active_tasks() != 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        let mut tasks = self.state.tasks.lock().unwrap_or_else(|p| p.into_inner());
        if self.active_tasks() != 0 {
            for task in tasks.iter() {
                if !task.is_finished() {
                    task.abort();
                }
            }
        }
        tasks.clear();
        drop(tasks);
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_timeout(deadline.saturating_duration_since(Instant::now()));
        }
    }
}

#[derive(Clone)]
pub struct ExecutionDispatcher {
    executor: tokio::runtime::Handle,
    state: Arc<ExecutionState>,
}

impl ExecutionDispatcher {
    pub fn bind(
        &self,
        account: AccountHandle,
        connector: Arc<dyn DirectExecutionConnector>,
        observer: Arc<dyn ExecutionObserver>,
    ) -> ExecutionHandle {
        ExecutionHandle {
            account,
            connector,
            executor: self.executor.clone(),
            observer,
            state: self.state.clone(),
        }
    }
}

impl Drop for ExecutionRuntime {
    fn drop(&mut self) {
        self.shutdown(Instant::now());
    }
}

#[derive(Clone)]
pub struct ExecutionHandle {
    account: AccountHandle,
    connector: Arc<dyn DirectExecutionConnector>,
    executor: tokio::runtime::Handle,
    observer: Arc<dyn ExecutionObserver>,
    state: Arc<ExecutionState>,
}

impl ExecutionHandle {
    pub fn account(&self) -> AccountHandle {
        self.account
    }

    pub fn submit(&self, request: DirectNewOrderRequest) -> Result<ExecutionTaskId, SpawnError> {
        let started = Instant::now();
        let asset_id = request.asset_id;
        let client_order_id = Some(request.client_order_id);
        self.spawn(
            asset_id,
            ExecutionRequestKind::Submit,
            client_order_id,
            || self.connector.submit(request),
            started,
        )
    }

    pub fn cancel(&self, request: DirectCancelOrderRequest) -> Result<ExecutionTaskId, SpawnError> {
        let started = Instant::now();
        let asset_id = request.asset_id;
        let client_order_id = request.client_order_id;
        self.spawn(
            asset_id,
            ExecutionRequestKind::Cancel,
            client_order_id,
            || self.connector.cancel(request),
            started,
        )
    }

    fn spawn(
        &self,
        asset_id: AssetId,
        request_kind: ExecutionRequestKind,
        client_order_id: Option<ClientOrderId>,
        make_future: impl FnOnce() -> ExecutionFuture,
        started: Instant,
    ) -> Result<ExecutionTaskId, SpawnError> {
        if !self.state.accepting.load(Ordering::Acquire) {
            self.state
                .stopping_rejections
                .fetch_add(1, Ordering::Relaxed);
            return Err(SpawnError::RuntimeStopping);
        }
        self.state
            .active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                (active < self.state.max_active).then_some(active + 1)
            })
            .map_err(|_| {
                self.state.saturated.fetch_add(1, Ordering::Relaxed);
                SpawnError::ExecutorSaturated
            })?;
        let permit = ActivePermit(self.state.clone());
        if !self.state.accepting.load(Ordering::Acquire) {
            self.state
                .stopping_rejections
                .fetch_add(1, Ordering::Relaxed);
            return Err(SpawnError::RuntimeStopping);
        }
        let task_id = self.state.next_task_id.fetch_add(1, Ordering::Relaxed);
        self.state
            .future_allocations
            .fetch_add(1, Ordering::Relaxed);
        let future = make_future();
        let observer = self.observer.clone();
        let account = self.account;
        let task = self.executor.spawn(async move {
            let _active = permit;
            let outcome = match future.await {
                Ok(order) => ObservedExecutionOutcome::Accepted(order),
                Err(error) if error.outcome_unknown => ObservedExecutionOutcome::Unknown(error),
                Err(error) => ObservedExecutionOutcome::Rejected(error),
            };
            observer.observe(ObservedExecutionResult {
                task_id,
                account,
                asset_id,
                request_kind,
                client_order_id,
                outcome,
            });
        });
        let mut tasks = self.state.tasks.lock().unwrap_or_else(|p| p.into_inner());
        tasks.retain(|task| !task.is_finished());
        tasks.push(task);
        self.state.spawned.fetch_add(1, Ordering::Relaxed);
        record_latency(
            &self.state.spawn_latency,
            started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
        );
        Ok(task_id)
    }
}

fn record_latency(buckets: &[AtomicU64; LATENCY_BUCKETS], value_ns: u64) {
    let bucket = if value_ns <= 1 {
        0
    } else {
        (63 - value_ns.leading_zeros()) as usize
    };
    buckets[bucket].fetch_add(1, Ordering::Relaxed);
}

fn metrics_snapshot(state: &ExecutionState) -> ExecutionMetricsSnapshot {
    let counts = std::array::from_fn::<_, LATENCY_BUCKETS, _>(|index| {
        state.spawn_latency[index].load(Ordering::Relaxed)
    });
    let total = counts.iter().sum();
    ExecutionMetricsSnapshot {
        active_tasks: state.active.load(Ordering::Acquire),
        spawned_tasks: state.spawned.load(Ordering::Relaxed),
        saturated_rejections: state.saturated.load(Ordering::Relaxed),
        stopping_rejections: state.stopping_rejections.load(Ordering::Relaxed),
        connector_future_allocations: state.future_allocations.load(Ordering::Relaxed),
        spawn_p50_ns: percentile(&counts, total, 500),
        spawn_p99_ns: percentile(&counts, total, 990),
        spawn_p999_ns: percentile(&counts, total, 999),
        spawn_max_ns: percentile(&counts, total, 1_000),
    }
}

fn percentile(counts: &[u64; LATENCY_BUCKETS], total: u64, permille: u64) -> u64 {
    if total == 0 {
        return 0;
    }
    let target = total.saturating_mul(permille).div_ceil(1_000).max(1);
    let mut cumulative = 0_u64;
    for (index, count) in counts.iter().enumerate() {
        cumulative = cumulative.saturating_add(*count);
        if cumulative >= target {
            return if index == 63 {
                u64::MAX
            } else {
                (1_u64 << (index + 1)).saturating_sub(1)
            };
        }
    }
    u64::MAX
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    struct PendingConnector;

    impl DirectExecutionConnector for PendingConnector {
        fn submit(&self, _: DirectNewOrderRequest) -> ExecutionFuture {
            Box::pin(std::future::pending())
        }

        fn cancel(&self, _: DirectCancelOrderRequest) -> ExecutionFuture {
            Box::pin(std::future::pending())
        }
    }

    struct ErrorConnector {
        unknown: bool,
    }

    struct PanickingConnector;

    impl DirectExecutionConnector for PanickingConnector {
        fn submit(&self, _: DirectNewOrderRequest) -> ExecutionFuture {
            panic!("injected future-construction panic")
        }

        fn cancel(&self, _: DirectCancelOrderRequest) -> ExecutionFuture {
            panic!("injected future-construction panic")
        }
    }

    impl DirectExecutionConnector for ErrorConnector {
        fn submit(&self, _: DirectNewOrderRequest) -> ExecutionFuture {
            let unknown = self.unknown;
            Box::pin(async move {
                Err(DirectExecutionError {
                    exchange: "test",
                    code: "TEST".into(),
                    message: "injected".into(),
                    outcome_unknown: unknown,
                })
            })
        }

        fn cancel(&self, _: DirectCancelOrderRequest) -> ExecutionFuture {
            self.submit(order())
        }
    }

    struct ChannelObserver(mpsc::Sender<ObservedExecutionResult>);

    impl ExecutionObserver for ChannelObserver {
        fn observe(&self, result: ObservedExecutionResult) {
            self.0.send(result).unwrap();
        }
    }

    fn account() -> AccountHandle {
        AccountHandle {
            account_id: crate::AccountId(7),
            generation: 1,
        }
    }

    fn order() -> DirectNewOrderRequest {
        DirectNewOrderRequest {
            asset_id: AssetId(9),
            side: 1,
            order_type: 0,
            time_in_force: 0,
            price_ticks: 10,
            quantity_lots: 1,
            reduce_only: false,
            client_order_id: Id128([3; 16]),
        }
    }

    #[test]
    fn enforces_process_limit_and_shutdown_admission() {
        let mut runtime = ExecutionRuntime::new(1, 1).unwrap();
        let (tx, _rx) = mpsc::channel();
        let handle = runtime.bind(
            account(),
            Arc::new(PendingConnector),
            Arc::new(ChannelObserver(tx)),
        );
        assert_eq!(handle.submit(order()), Ok(1));
        assert_eq!(handle.submit(order()), Err(SpawnError::ExecutorSaturated));
        let running = runtime.metrics();
        assert_eq!(running.active_tasks, 1);
        assert_eq!(running.spawned_tasks, 1);
        assert_eq!(running.saturated_rejections, 1);
        assert_eq!(running.connector_future_allocations, 1);
        assert!(running.spawn_p50_ns <= running.spawn_p99_ns);
        assert!(running.spawn_p99_ns <= running.spawn_p999_ns);
        assert!(running.spawn_p999_ns <= running.spawn_max_ns);
        runtime.shutdown(Instant::now());
        assert_eq!(handle.submit(order()), Err(SpawnError::RuntimeStopping));
        let stopped = runtime.metrics();
        assert_eq!(stopped.stopping_rejections, 1);
        assert_eq!(stopped.connector_future_allocations, 1);
    }

    #[test]
    fn observer_distinguishes_rejected_and_unknown() {
        for unknown in [false, true] {
            let mut runtime = ExecutionRuntime::new(1, 2).unwrap();
            let (tx, rx) = mpsc::channel();
            let handle = runtime.bind(
                account(),
                Arc::new(ErrorConnector { unknown }),
                Arc::new(ChannelObserver(tx)),
            );
            handle.submit(order()).unwrap();
            let observed = rx.recv_timeout(Duration::from_secs(1)).unwrap();
            assert_eq!(
                matches!(observed.outcome, ObservedExecutionOutcome::Unknown(_)),
                unknown
            );
            runtime.shutdown(Instant::now() + Duration::from_secs(1));
        }
    }

    #[test]
    fn future_construction_panic_releases_active_permit() {
        let runtime = ExecutionRuntime::new(1, 1).unwrap();
        let (tx, _rx) = mpsc::channel();
        let handle = runtime.bind(
            account(),
            Arc::new(PanickingConnector),
            Arc::new(ChannelObserver(tx)),
        );
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = handle.submit(order());
        }));
        assert!(result.is_err());
        assert_eq!(runtime.active_tasks(), 0);
    }
}
