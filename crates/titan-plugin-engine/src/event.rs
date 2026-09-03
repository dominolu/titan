use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread::JoinHandle,
    time::Duration,
};

use crate::{
    ActivationGate, BackgroundFlightRecorderExporter, CallbackBudget, CallbackMonitor,
    ColdAsyncRuntime, ErrorKind, EventQos, ExecutionModel, FlightRecorder, LifecycleState,
    PLUGIN_CALLBACK_STALLED_EVENT, PLUGIN_HEALTH_CHANGED_EVENT,
    PLUGIN_RUNTIME_EVENT_SCHEMA_VERSION, PluginError, PluginIdentity, ResourceScopeHandle,
    ThreadHeartbeat, TraceContext, trace_kind,
};

#[derive(Clone)]
pub(crate) struct SubscriptionExecutor {
    pub model: ExecutionModel,
    pub cpu_affinity: Option<usize>,
    pub background: Option<Arc<ColdAsyncRuntime>>,
    pub callback_budget: Option<CallbackBudget>,
    pub recorder: Arc<FlightRecorder>,
    pub exporter: Option<Arc<BackgroundFlightRecorderExporter>>,
    pub metrics: Arc<crate::PluginMetricSeries>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubscriptionSpec {
    pub event_type: Arc<str>,
    pub schema_version: u32,
    pub qos: EventQos,
    pub capacity: usize,
    pub routing_keys: Arc<[u64]>,
}

pub trait EventHandler: Send + Sync + 'static {
    fn handle(&self, event: EventView<'_>) -> Result<(), PluginError>;
}

#[derive(Clone, Copy)]
pub struct EventView<'a> {
    pub event_type: &'a str,
    pub schema_version: u32,
    pub payload: &'a [u8],
    pub trace: TraceContext,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct EventPublishMetadata {
    pub source_id: u32,
    pub source_sequence: u64,
    pub exchange_ts: i64,
    pub receive_ts: i64,
    pub publish_ts: i64,
    pub routing_key: u64,
    pub flags: u32,
}

#[derive(Clone)]
pub struct SubscriptionBinding {
    pub spec: SubscriptionSpec,
    pub handler: Arc<dyn EventHandler>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RouteVersion(pub u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RouteTransaction(pub u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SubscriptionCandidate(pub u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SubscriptionToken(pub u64);

/// Capabilities whose semantics are part of Core Runtime API v2.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct EventApiCapabilities(pub u64);

impl EventApiCapabilities {
    pub const PRIMARY_ASYNC_DELIVERY: u64 = 1 << 0;
    pub const RELIABLE_PENDING: u64 = 1 << 1;
    pub const SUBSCRIBER_WATERMARKS: u64 = 1 << 2;
    pub const SUBSCRIBER_HEALTH: u64 = 1 << 3;
    pub const SNAPSHOT_BARRIER: u64 = 1 << 4;
    pub const LANE_SAFE_POINT: u64 = 1 << 5;

    pub const V2_REQUIRED: Self = Self(
        Self::PRIMARY_ASYNC_DELIVERY
            | Self::RELIABLE_PENDING
            | Self::SUBSCRIBER_WATERMARKS
            | Self::SUBSCRIBER_HEALTH
            | Self::SNAPSHOT_BARRIER
            | Self::LANE_SAFE_POINT,
    );

    pub const fn contains(self, required: Self) -> bool {
        self.0 & required.0 == required.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DispatchOutcome {
    Delivered,
    Idle,
    Closed,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct EventReceiverDiagnostics {
    pub channel_depth: usize,
    pub pending_depth: usize,
    pub outstanding_handles: usize,
}

/// Consumer side of a SubscriberChannel. Implementations retain the EventLease while invoking
/// the handler; EventEngine implementations must never invoke handlers themselves.
pub trait EventReceiver: Send + Sync + 'static {
    fn dispatch_next(
        &self,
        handler: &dyn EventHandler,
        idle_wait: Duration,
    ) -> Result<DispatchOutcome, PluginError>;
    fn diagnostics(&self) -> EventReceiverDiagnostics {
        EventReceiverDiagnostics::default()
    }
}

/// An authorized, fixed-size market payload reserved from the runtime event arena.
/// Dropping without committing returns the reservation without publishing it.
pub trait EventPayloadReservation: Send {
    fn payload_mut(&mut self) -> &mut [u8];
    fn commit(self: Box<Self>) -> Result<(), PluginError>;
}

#[derive(Clone)]
pub struct CommittedSubscription {
    pub token: SubscriptionToken,
    pub mailbox_id: u64,
    pub receiver: Arc<dyn EventReceiver>,
}

impl std::fmt::Debug for CommittedSubscription {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CommittedSubscription")
            .field("token", &self.token)
            .field("mailbox_id", &self.mailbox_id)
            .finish_non_exhaustive()
    }
}

pub trait EventControl: Send + Sync + 'static {
    fn api_version(&self) -> crate::ApiVersion;
    fn api_capabilities(&self) -> EventApiCapabilities {
        EventApiCapabilities::default()
    }
    fn current_route_version(&self) -> RouteVersion;
    fn begin_route_update(&self, base: RouteVersion) -> Result<RouteTransaction, PluginError>;
    fn stage_subscription(
        &self,
        transaction: RouteTransaction,
        owner: &PluginIdentity,
        spec: &SubscriptionSpec,
    ) -> Result<SubscriptionCandidate, PluginError>;
    fn stage_subscription_in_mailbox(
        &self,
        transaction: RouteTransaction,
        owner: &PluginIdentity,
        mailbox: &str,
        spec: &SubscriptionSpec,
    ) -> Result<SubscriptionCandidate, PluginError> {
        let _ = mailbox;
        self.stage_subscription(transaction, owner, spec)
    }
    fn commit_at_safe_point(
        &self,
        transaction: RouteTransaction,
    ) -> Result<(RouteVersion, Vec<CommittedSubscription>), PluginError>;
    fn abort(&self, transaction: RouteTransaction);
    fn retire_subscription(&self, token: SubscriptionToken) -> Result<(), PluginError>;
    fn publish(
        &self,
        event_type: &str,
        schema_version: u32,
        payload: &[u8],
        trace: TraceContext,
    ) -> Result<(), PluginError>;
    fn publish_with_metadata(
        &self,
        event_type: &str,
        schema_version: u32,
        payload: &[u8],
        metadata: EventPublishMetadata,
        trace: TraceContext,
    ) -> Result<(), PluginError> {
        let _ = metadata;
        self.publish(event_type, schema_version, payload, trace)
    }
    fn reserve_market_batch(
        &self,
        event_type: &str,
        schema_version: u32,
        payload_length: usize,
        metadata: EventPublishMetadata,
        trace: TraceContext,
    ) -> Result<Box<dyn EventPayloadReservation>, PluginError> {
        let _ = (event_type, schema_version, payload_length, metadata, trace);
        Err(PluginError::new(
            ErrorKind::SubscriptionRejected,
            PluginIdentity::new("titan.core", "event-control"),
            LifecycleState::Running,
            "reserve_market_batch",
            "event control does not support market batch reservations",
        ))
    }

    fn reserve_event_payload(
        &self,
        event_type: &str,
        schema_version: u32,
        payload_length: usize,
        metadata: EventPublishMetadata,
        trace: TraceContext,
    ) -> Result<Box<dyn EventPayloadReservation>, PluginError> {
        let _ = (event_type, schema_version, payload_length, metadata, trace);
        Err(PluginError::new(
            ErrorKind::SubscriptionRejected,
            PluginIdentity::new("titan.core", "event-control"),
            LifecycleState::Running,
            "reserve_event_payload",
            "event control does not support event payload reservations",
        ))
    }
}

#[derive(Clone)]
pub struct EventPublisher {
    owner: PluginIdentity,
    allowed: Arc<std::collections::BTreeMap<Arc<str>, BTreeSet<u32>>>,
    gate: Arc<ActivationGate>,
    control: Arc<dyn EventControl>,
}

impl EventPublisher {
    fn authorize(&self, event_type: &str, schema_version: u32) -> Result<(), PluginError> {
        if !self
            .allowed
            .get(event_type)
            .is_some_and(|versions| versions.contains(&schema_version))
        {
            return Err(PluginError::new(
                ErrorKind::SubscriptionRejected,
                self.owner.clone(),
                LifecycleState::Running,
                "publish_event",
                format!("event {event_type}@{schema_version} is not authorized"),
            ));
        }
        if !self.gate.is_active() {
            return Err(PluginError::new(
                ErrorKind::RuntimeNotActive,
                self.owner.clone(),
                LifecycleState::Starting,
                "publish_event",
                "activation gate is closed",
            )
            .recoverable(true));
        }
        Ok(())
    }

    pub(crate) fn new(
        owner: PluginIdentity,
        allowed: std::collections::BTreeMap<Arc<str>, BTreeSet<u32>>,
        gate: Arc<ActivationGate>,
        control: Arc<dyn EventControl>,
    ) -> Self {
        Self {
            owner,
            allowed: Arc::new(allowed),
            gate,
            control,
        }
    }

    pub fn publish(
        &self,
        event_type: &str,
        schema_version: u32,
        payload: &[u8],
        trace: TraceContext,
    ) -> Result<(), PluginError> {
        self.publish_with_metadata(
            event_type,
            schema_version,
            payload,
            EventPublishMetadata::default(),
            trace,
        )
    }

    pub fn publish_with_metadata(
        &self,
        event_type: &str,
        schema_version: u32,
        payload: &[u8],
        metadata: EventPublishMetadata,
        trace: TraceContext,
    ) -> Result<(), PluginError> {
        self.authorize(event_type, schema_version)?;
        self.control
            .publish_with_metadata(event_type, schema_version, payload, metadata, trace)
    }

    pub fn reserve_market_batch(
        &self,
        event_type: &str,
        schema_version: u32,
        payload_length: usize,
        metadata: EventPublishMetadata,
        trace: TraceContext,
    ) -> Result<Box<dyn EventPayloadReservation>, PluginError> {
        self.authorize(event_type, schema_version)?;
        self.control.reserve_market_batch(
            event_type,
            schema_version,
            payload_length,
            metadata,
            trace,
        )
    }

    /// Reserves the event's declared arena pool and publishes only when the returned reservation
    /// is committed. Dropping it rolls the block back without emitting a partial event.
    pub fn reserve_event_payload(
        &self,
        event_type: &str,
        schema_version: u32,
        payload_length: usize,
        metadata: EventPublishMetadata,
        trace: TraceContext,
    ) -> Result<Box<dyn EventPayloadReservation>, PluginError> {
        self.authorize(event_type, schema_version)?;
        self.control.reserve_event_payload(
            event_type,
            schema_version,
            payload_length,
            metadata,
            trace,
        )
    }
}

#[derive(Clone)]
pub struct ScopedEventRouter {
    owner: PluginIdentity,
    allowed: Arc<std::collections::BTreeMap<(Arc<str>, u32), BTreeSet<EventQos>>>,
    max_capacity: usize,
    allowed_qos: Arc<BTreeSet<EventQos>>,
    gate: Arc<ActivationGate>,
    control: Arc<dyn EventControl>,
    resources: ResourceScopeHandle,
    executor: SubscriptionExecutor,
}

impl ScopedEventRouter {
    pub(crate) fn new(
        owner: PluginIdentity,
        allowed: std::collections::BTreeMap<(Arc<str>, u32), BTreeSet<EventQos>>,
        max_capacity: usize,
        allowed_qos: BTreeSet<EventQos>,
        gate: Arc<ActivationGate>,
        control: Arc<dyn EventControl>,
        resources: ResourceScopeHandle,
        executor: SubscriptionExecutor,
    ) -> Self {
        Self {
            owner,
            allowed: Arc::new(allowed),
            max_capacity,
            allowed_qos: Arc::new(allowed_qos),
            gate,
            control,
            resources,
            executor,
        }
    }

    pub fn subscribe(
        &self,
        spec: SubscriptionSpec,
        handler: Arc<dyn EventHandler>,
    ) -> Result<SubscriptionToken, PluginError> {
        let allowed = self
            .allowed
            .get(&(spec.event_type.clone(), spec.schema_version));
        if spec.capacity == 0
            || spec.capacity > self.max_capacity
            || !self.allowed_qos.contains(&spec.qos)
            || !allowed.is_some_and(|qos| qos.contains(&spec.qos))
        {
            return Err(PluginError::new(
                ErrorKind::SubscriptionRejected,
                self.owner.clone(),
                LifecycleState::Running,
                "dynamic_subscribe",
                "subscription exceeds granted capability",
            ));
        }
        let tx = self
            .control
            .begin_route_update(self.control.current_route_version())?;
        if let Err(error) = self.control.stage_subscription(tx, &self.owner, &spec) {
            self.control.abort(tx);
            return Err(error);
        }
        let (_, mut committed) = self.control.commit_at_safe_point(tx)?;
        if committed.len() != 1 {
            for subscription in committed {
                let _ = self.control.retire_subscription(subscription.token);
            }
            return Err(PluginError::new(
                ErrorKind::SubscriptionRejected,
                self.owner.clone(),
                LifecycleState::Running,
                "dynamic_subscribe",
                "event engine returned an invalid subscription set",
            ));
        }
        let subscription = committed.pop().ok_or_else(|| {
            PluginError::new(
                ErrorKind::SubscriptionRejected,
                self.owner.clone(),
                LifecycleState::Running,
                "dynamic_subscribe",
                "event engine did not return a token",
            )
        })?;
        let token = subscription.token;
        let runtime = match SubscriptionRuntime::start(
            self.owner.clone(),
            self.gate.clone(),
            subscription.receiver,
            handler,
            self.control.clone(),
            token,
            self.executor.clone(),
        ) {
            Ok(runtime) => runtime,
            Err(error) => {
                let _ = self.control.retire_subscription(token);
                return Err(error);
            }
        };
        self.resources.register("dynamic-subscription", runtime)?;
        Ok(token)
    }
}

pub(crate) struct SubscriptionRuntime {
    identity: PluginIdentity,
    stop: Arc<AtomicBool>,
    task: Option<SubscriptionTask>,
    control: Arc<dyn EventControl>,
    tokens: Vec<SubscriptionToken>,
    watchdog: Option<JoinHandle<()>>,
    stalled: Arc<AtomicBool>,
    heartbeat: ThreadHeartbeat,
    monitor: Option<CallbackMonitor>,
    receiver: Arc<dyn EventReceiver>,
}

struct MonitoredHandler {
    inner: Arc<dyn EventHandler>,
    monitor: Option<CallbackMonitor>,
    heartbeat: ThreadHeartbeat,
    recorder: Arc<FlightRecorder>,
    exporter: Option<Arc<BackgroundFlightRecorderExporter>>,
    metrics: Arc<crate::PluginMetricSeries>,
}

impl EventHandler for MonitoredHandler {
    fn handle(&self, event: EventView<'_>) -> Result<(), PluginError> {
        self.heartbeat.beat();
        self.recorder
            .record(event.trace, trace_kind::CALLBACK_BEGIN, 0, false);
        let started = std::time::Instant::now();
        let before_exceeded = self
            .monitor
            .as_ref()
            .and_then(|monitor| monitor.stats("subscription"))
            .map_or(0, |stats| stats.budget_exceeded);
        let guard = self
            .monitor
            .as_ref()
            .and_then(|monitor| monitor.begin("subscription"));
        let result = self.inner.handle(event);
        drop(guard);
        let duration = started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        let exceeded = self
            .monitor
            .as_ref()
            .and_then(|monitor| monitor.stats("subscription"))
            .is_some_and(|stats| stats.budget_exceeded > before_exceeded);
        if exceeded {
            self.recorder.record(
                event.trace,
                trace_kind::CALLBACK_BUDGET_EXCEEDED,
                duration,
                true,
            );
        }
        self.recorder.record(
            event.trace,
            trace_kind::CALLBACK_END,
            duration,
            result.is_err(),
        );
        self.metrics.callback(duration, exceeded);
        if result.is_err()
            && let Some(exporter) = &self.exporter
        {
            let frozen = self.recorder.freeze();
            let _ = exporter.try_export(frozen);
            self.recorder.unfreeze();
        }
        self.heartbeat.beat();
        result
    }
}

enum SubscriptionTask {
    Dedicated(JoinHandle<()>),
    Background {
        handle: tokio::task::JoinHandle<()>,
        completed: mpsc::Receiver<()>,
        _runtime: Arc<ColdAsyncRuntime>,
    },
}

impl SubscriptionRuntime {
    pub(crate) fn start(
        identity: PluginIdentity,
        gate: Arc<ActivationGate>,
        receiver: Arc<dyn EventReceiver>,
        handler: Arc<dyn EventHandler>,
        control: Arc<dyn EventControl>,
        token: SubscriptionToken,
        executor: SubscriptionExecutor,
    ) -> Result<Self, PluginError> {
        Self::start_group(
            identity,
            gate,
            receiver,
            handler,
            control,
            vec![token],
            executor,
        )
    }

    pub(crate) fn start_group(
        identity: PluginIdentity,
        gate: Arc<ActivationGate>,
        receiver: Arc<dyn EventReceiver>,
        handler: Arc<dyn EventHandler>,
        control: Arc<dyn EventControl>,
        tokens: Vec<SubscriptionToken>,
        executor: SubscriptionExecutor,
    ) -> Result<Self, PluginError> {
        let stop = Arc::new(AtomicBool::new(false));
        let diagnostics_receiver = receiver.clone();
        let runtime_stop = stop.clone();
        let error_identity = identity.clone();
        let stalled = Arc::new(AtomicBool::new(false));
        let heartbeat = ThreadHeartbeat::new();
        let monitor = if let Some(budget) = executor.callback_budget.clone() {
            let monitor = CallbackMonitor::default();
            monitor.register("subscription", budget);
            Some(monitor)
        } else {
            None
        };
        let handler = Arc::new(MonitoredHandler {
            inner: handler,
            monitor: monitor.clone(),
            heartbeat: heartbeat.clone(),
            recorder: executor.recorder.clone(),
            exporter: executor.exporter.clone(),
            metrics: executor.metrics.clone(),
        }) as Arc<dyn EventHandler>;
        let task = match executor.model {
            ExecutionModel::Dedicated => {
                if let Some(cpu) = executor.cpu_affinity
                    && !core_affinity::get_core_ids()
                        .is_some_and(|cores| cores.iter().any(|core| core.id == cpu))
                {
                    return Err(PluginError::new(
                        ErrorKind::ConfigInvalid,
                        error_identity,
                        LifecycleState::Starting,
                        "start_subscriber_runtime",
                        format!("CPU {cpu} is unavailable"),
                    ));
                }
                let runtime_gate = gate.clone();
                let (startup_tx, startup_rx) = mpsc::sync_channel(1);
                let cpu_affinity = executor.cpu_affinity;
                let thread = std::thread::Builder::new()
                    .name(format!("titan-subscriber-{}", identity.instance_id))
                    .spawn(move || {
                        let bound = cpu_affinity.is_none_or(|cpu| {
                            core_affinity::set_for_current(core_affinity::CoreId { id: cpu })
                        });
                        let _ = startup_tx.send(bound);
                        if !bound {
                            return;
                        }
                        if runtime_gate.wait_until_active() != crate::ActivationState::Active {
                            return;
                        }
                        while !runtime_stop.load(Ordering::Acquire) && runtime_gate.is_active() {
                            match receiver.dispatch_next(handler.as_ref(), Duration::from_millis(1))
                            {
                                Ok(DispatchOutcome::Delivered | DispatchOutcome::Idle) => {}
                                Ok(DispatchOutcome::Closed) | Err(_) => break,
                            }
                        }
                    })
                    .map_err(|error| {
                        PluginError::new(
                            ErrorKind::RuntimeStartFailed,
                            identity.clone(),
                            LifecycleState::Starting,
                            "start_subscriber_runtime",
                            error.to_string(),
                        )
                    })?;
                if !startup_rx.recv().unwrap_or(false) {
                    let _ = thread.join();
                    return Err(PluginError::new(
                        ErrorKind::RuntimeStartFailed,
                        identity,
                        LifecycleState::Starting,
                        "start_subscriber_runtime",
                        format!(
                            "failed to bind subscriber worker to CPU {}",
                            cpu_affinity.expect("an unbound worker cannot fail binding")
                        ),
                    ));
                }
                SubscriptionTask::Dedicated(thread)
            }
            ExecutionModel::Background => {
                let background = executor.background.ok_or_else(|| {
                    PluginError::new(
                        ErrorKind::RuntimeStartFailed,
                        error_identity,
                        LifecycleState::Starting,
                        "start_subscriber_runtime",
                        "background runtime is unavailable",
                    )
                })?;
                let (completed_tx, completed) = mpsc::sync_channel(1);
                let runtime_gate = gate.clone();
                let handle = background.try_spawn(async move {
                    if runtime_gate.wait_until_active_async().await
                        != crate::ActivationState::Active
                    {
                        let _ = completed_tx.send(());
                        return;
                    }
                    while !runtime_stop.load(Ordering::Acquire) && runtime_gate.is_active() {
                        match receiver.dispatch_next(handler.as_ref(), Duration::ZERO) {
                            Ok(DispatchOutcome::Delivered) => {}
                            Ok(DispatchOutcome::Idle) => {
                                tokio::time::sleep(Duration::from_millis(1)).await;
                            }
                            Ok(DispatchOutcome::Closed) | Err(_) => break,
                        }
                    }
                    let _ = completed_tx.send(());
                })?;
                SubscriptionTask::Background {
                    handle,
                    completed,
                    _runtime: background,
                }
            }
            ExecutionModel::Passive => {
                return Err(PluginError::new(
                    ErrorKind::ConfigInvalid,
                    error_identity,
                    LifecycleState::Starting,
                    "start_subscriber_runtime",
                    "passive plugins cannot own subscriber runtimes",
                ));
            }
        };
        let watchdog = match monitor.clone() {
            Some(monitor) => {
                let watchdog_stop = stop.clone();
                let watchdog_gate = gate.clone();
                let watchdog_stalled = stalled.clone();
                let watchdog_control = control.clone();
                let watchdog_identity = identity.clone();
                let watchdog_recorder = executor.recorder.clone();
                let watchdog_exporter = executor.exporter.clone();
                let threshold_us = executor
                    .callback_budget
                    .as_ref()
                    .expect("monitor requires a callback budget")
                    .stall_threshold_us;
                let scan_period = Duration::from_micros((threshold_us / 4).max(1));
                match std::thread::Builder::new()
                    .name(format!("titan-watchdog-{}", identity.instance_id))
                    .spawn(move || {
                        if watchdog_gate.wait_until_active() != crate::ActivationState::Active {
                            return;
                        }
                        while !watchdog_stop.load(Ordering::Acquire) && watchdog_gate.is_active() {
                            std::thread::park_timeout(scan_period);
                            if watchdog_stop.load(Ordering::Acquire) {
                                break;
                            }
                            if !monitor.scan_stalled(std::time::Instant::now()).is_empty()
                                && !watchdog_stalled.swap(true, Ordering::AcqRel)
                            {
                                watchdog_recorder.record(
                                    TraceContext::default(),
                                    trace_kind::CALLBACK_STALLED,
                                    threshold_us,
                                    true,
                                );
                                executor.metrics.stalled();
                                if let Some(exporter) = &watchdog_exporter {
                                    let frozen = watchdog_recorder.freeze();
                                    let _ = exporter.try_export(frozen);
                                    watchdog_recorder.unfreeze();
                                }
                                watchdog_gate.quiesce();
                                publish_stalled(
                                    &*watchdog_control,
                                    &watchdog_identity,
                                    threshold_us,
                                );
                                break;
                            }
                        }
                    }) {
                    Ok(watchdog) => Some(watchdog),
                    Err(error) => {
                        stop.store(true, Ordering::Release);
                        gate.stop();
                        stop_subscription_task(task);
                        return Err(PluginError::new(
                            ErrorKind::RuntimeStartFailed,
                            identity,
                            LifecycleState::Starting,
                            "start_callback_watchdog",
                            error.to_string(),
                        ));
                    }
                }
            }
            None => None,
        };
        Ok(Self {
            identity,
            stop,
            task: Some(task),
            control,
            tokens,
            watchdog,
            stalled,
            heartbeat,
            monitor,
            receiver: diagnostics_receiver,
        })
    }

    pub(crate) fn is_stalled(&self) -> bool {
        self.stalled.load(Ordering::Acquire)
    }

    pub(crate) fn heartbeat_age(&self) -> Duration {
        self.heartbeat.age(std::time::Instant::now())
    }

    pub(crate) fn callback_stats(&self) -> Option<crate::CallbackStats> {
        self.monitor
            .as_ref()
            .and_then(|monitor| monitor.stats("subscription"))
    }

    pub(crate) fn receiver_diagnostics(&self) -> EventReceiverDiagnostics {
        self.receiver.diagnostics()
    }
}

fn stop_subscription_task(task: SubscriptionTask) {
    match task {
        SubscriptionTask::Dedicated(thread) => {
            let _ = thread.join();
        }
        SubscriptionTask::Background {
            handle, completed, ..
        } => {
            let _ = completed.recv();
            handle.abort();
        }
    }
}

impl crate::Resource for SubscriptionRuntime {
    fn close(&mut self) -> Result<(), PluginError> {
        self.stop.store(true, Ordering::Release);
        let watchdog_error = self.watchdog.take().and_then(|watchdog| {
            watchdog.thread().unpark();
            watchdog.join().is_err().then(|| {
                PluginError::new(
                    ErrorKind::PluginFailed,
                    self.identity.clone(),
                    LifecycleState::Stopping,
                    "join_callback_watchdog",
                    "callback watchdog panicked",
                )
            })
        });
        let join_error = self.task.take().and_then(|task| match task {
            SubscriptionTask::Dedicated(thread) => thread.join().is_err().then(|| {
                PluginError::new(
                    ErrorKind::PluginFailed,
                    self.identity.clone(),
                    LifecycleState::Stopping,
                    "join_subscriber_runtime",
                    "subscriber runtime panicked",
                )
            }),
            SubscriptionTask::Background {
                handle, completed, ..
            } => {
                let failed = completed.recv().is_err();
                handle.abort();
                failed.then(|| {
                    PluginError::new(
                        ErrorKind::PluginFailed,
                        self.identity.clone(),
                        LifecycleState::Stopping,
                        "join_subscriber_runtime",
                        "background subscriber runtime panicked",
                    )
                })
            }
        });
        let retire_result = self
            .tokens
            .iter()
            .copied()
            .map(|token| self.control.retire_subscription(token))
            .find(Result::is_err)
            .unwrap_or(Ok(()));
        if let Some(error) = watchdog_error.or(join_error) {
            return Err(error);
        }
        retire_result
    }
}

fn publish_stalled(control: &dyn EventControl, identity: &PluginIdentity, threshold_us: u64) {
    let payload = serde_json::to_vec(&serde_json::json!({
        "schema_version": PLUGIN_RUNTIME_EVENT_SCHEMA_VERSION,
        "plugin_type": identity.plugin_type,
        "instance_id": identity.instance_id,
        "lifecycle_state": "Running",
        "health": "STALLED",
        "error_kind": "PluginFailed",
        "operation": "subscription_callback",
        "message": format!("subscription callback exceeded stall threshold of {threshold_us}us"),
        "cause_chain": [],
        "occurred_at_ns": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .min(u64::MAX as u128) as u64,
        "recoverable": false,
        "request_id": null,
    }))
    .unwrap_or_default();
    let _ = control.publish(
        PLUGIN_CALLBACK_STALLED_EVENT,
        PLUGIN_RUNTIME_EVENT_SCHEMA_VERSION,
        &payload,
        TraceContext::default(),
    );
    let _ = control.publish(
        PLUGIN_HEALTH_CHANGED_EVENT,
        PLUGIN_RUNTIME_EVENT_SCHEMA_VERSION,
        &payload,
        TraceContext::default(),
    );
}

#[cfg(test)]
mod execution_tests {
    use std::sync::{Mutex, atomic::AtomicUsize};

    use super::*;
    use crate::{
        ApiVersion, CommittedSubscription, EventApiCapabilities, Resource, RouteVersion,
        SubscriptionCandidate,
    };

    #[derive(Default)]
    struct ProbeControl {
        retired: AtomicUsize,
        published: Mutex<Vec<Arc<str>>>,
    }

    impl EventControl for ProbeControl {
        fn api_version(&self) -> ApiVersion {
            crate::CORE_RUNTIME_API_VERSION
        }
        fn api_capabilities(&self) -> EventApiCapabilities {
            EventApiCapabilities::V2_REQUIRED
        }
        fn current_route_version(&self) -> RouteVersion {
            RouteVersion(1)
        }
        fn begin_route_update(&self, _: RouteVersion) -> Result<RouteTransaction, PluginError> {
            unreachable!()
        }
        fn stage_subscription(
            &self,
            _: RouteTransaction,
            _: &PluginIdentity,
            _: &SubscriptionSpec,
        ) -> Result<SubscriptionCandidate, PluginError> {
            unreachable!()
        }
        fn commit_at_safe_point(
            &self,
            _: RouteTransaction,
        ) -> Result<(RouteVersion, Vec<CommittedSubscription>), PluginError> {
            unreachable!()
        }
        fn abort(&self, _: RouteTransaction) {}
        fn retire_subscription(&self, _: SubscriptionToken) -> Result<(), PluginError> {
            self.retired.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        fn publish(
            &self,
            event_type: &str,
            _: u32,
            _: &[u8],
            _: TraceContext,
        ) -> Result<(), PluginError> {
            self.published.lock().unwrap().push(Arc::from(event_type));
            Ok(())
        }
    }

    struct ProbeReceiver(Arc<AtomicUsize>);
    impl EventReceiver for ProbeReceiver {
        fn dispatch_next(
            &self,
            _: &dyn EventHandler,
            _: Duration,
        ) -> Result<DispatchOutcome, PluginError> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(DispatchOutcome::Closed)
        }
    }

    struct NoopHandler;
    impl EventHandler for NoopHandler {
        fn handle(&self, _: EventView<'_>) -> Result<(), PluginError> {
            Ok(())
        }
    }

    struct InvokeOnceReceiver(AtomicBool);
    impl EventReceiver for InvokeOnceReceiver {
        fn dispatch_next(
            &self,
            handler: &dyn EventHandler,
            _: Duration,
        ) -> Result<DispatchOutcome, PluginError> {
            if self.0.swap(true, Ordering::AcqRel) {
                return Ok(DispatchOutcome::Closed);
            }
            handler.handle(EventView {
                event_type: "fixture.event",
                schema_version: 1,
                payload: &[],
                trace: TraceContext::default(),
            })?;
            Ok(DispatchOutcome::Delivered)
        }
    }

    struct SlowHandler;
    impl EventHandler for SlowHandler {
        fn handle(&self, _: EventView<'_>) -> Result<(), PluginError> {
            std::thread::sleep(Duration::from_millis(40));
            Ok(())
        }
    }

    #[test]
    fn dedicated_and_background_subscribers_obey_the_same_activation_and_retirement_contract() {
        for model in [ExecutionModel::Dedicated, ExecutionModel::Background] {
            let gate = Arc::new(ActivationGate::new());
            let calls = Arc::new(AtomicUsize::new(0));
            let control = Arc::new(ProbeControl::default());
            let background = (model == ExecutionModel::Background)
                .then(|| Arc::new(ColdAsyncRuntime::new(1, 1).unwrap()));
            let mut runtime = SubscriptionRuntime::start_group(
                PluginIdentity::new("test", format!("{model:?}")),
                gate.clone(),
                Arc::new(ProbeReceiver(calls.clone())),
                Arc::new(NoopHandler),
                control.clone(),
                vec![SubscriptionToken(1)],
                SubscriptionExecutor {
                    model,
                    cpu_affinity: None,
                    background,
                    callback_budget: None,
                    recorder: Arc::new(FlightRecorder::new(64)),
                    exporter: None,
                    metrics: Arc::new(crate::PluginMetricSeries::default()),
                },
            )
            .unwrap();
            std::thread::sleep(Duration::from_millis(10));
            assert_eq!(calls.load(Ordering::Acquire), 0);
            assert!(gate.activate());
            let deadline = std::time::Instant::now() + Duration::from_secs(1);
            while calls.load(Ordering::Acquire) == 0 && std::time::Instant::now() < deadline {
                std::thread::yield_now();
            }
            assert_eq!(calls.load(Ordering::Acquire), 1);
            runtime.close().unwrap();
            assert_eq!(control.retired.load(Ordering::Acquire), 1);
        }
    }

    #[test]
    fn passive_subscriber_runtime_is_rejected_without_allocating_a_task() {
        let result = SubscriptionRuntime::start_group(
            PluginIdentity::new("test", "passive"),
            Arc::new(ActivationGate::new()),
            Arc::new(ProbeReceiver(Arc::new(AtomicUsize::new(0)))),
            Arc::new(NoopHandler),
            Arc::new(ProbeControl::default()),
            vec![SubscriptionToken(1)],
            SubscriptionExecutor {
                model: ExecutionModel::Passive,
                cpu_affinity: None,
                background: None,
                callback_budget: None,
                recorder: Arc::new(FlightRecorder::new(64)),
                exporter: None,
                metrics: Arc::new(crate::PluginMetricSeries::default()),
            },
        );
        assert_eq!(result.err().unwrap().kind, ErrorKind::ConfigInvalid);
    }

    #[test]
    fn callback_watchdog_quiesces_the_gate_and_publishes_standard_fault_events() {
        let gate = Arc::new(ActivationGate::new());
        let control = Arc::new(ProbeControl::default());
        let recorder = Arc::new(FlightRecorder::new(64));
        let metrics = Arc::new(crate::PluginMetricSeries::default());
        let mut runtime = SubscriptionRuntime::start_group(
            PluginIdentity::new("test", "stalled"),
            gate.clone(),
            Arc::new(InvokeOnceReceiver(AtomicBool::new(false))),
            Arc::new(SlowHandler),
            control.clone(),
            vec![SubscriptionToken(1)],
            SubscriptionExecutor {
                model: ExecutionModel::Dedicated,
                cpu_affinity: None,
                background: None,
                callback_budget: Some(CallbackBudget {
                    soft_budget_us: 1_000,
                    stall_threshold_us: 5_000,
                    max_consecutive_violations: 1,
                }),
                recorder: recorder.clone(),
                exporter: None,
                metrics: metrics.clone(),
            },
        )
        .unwrap();

        assert!(gate.activate());
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while std::time::Instant::now() < deadline {
            let events_published = control.published.lock().unwrap().len() == 2;
            if gate.state() == crate::ActivationState::Quiescing && events_published {
                break;
            }
            std::thread::yield_now();
        }

        assert!(runtime.is_stalled());
        assert_eq!(gate.state(), crate::ActivationState::Quiescing);
        let published = control.published.lock().unwrap();
        assert_eq!(published.len(), 2);
        assert_eq!(published[0].as_ref(), PLUGIN_CALLBACK_STALLED_EVENT);
        assert_eq!(published[1].as_ref(), PLUGIN_HEALTH_CHANGED_EVENT);
        drop(published);
        runtime.close().unwrap();
        let records = recorder.freeze();
        assert!(
            records
                .iter()
                .any(|record| { record.kind == trace_kind::CALLBACK_STALLED && record.force_keep })
        );
        assert!(records.iter().any(|record| {
            record.kind == trace_kind::CALLBACK_BUDGET_EXCEEDED && record.force_keep
        }));
        let metric = metrics.snapshot(PluginIdentity::new("test", "stalled"));
        assert_eq!(metric.callback_total, 1);
        assert_eq!(metric.callback_budget_exceeded_total, 1);
        assert_eq!(metric.callback_stalled_total, 1);
        assert_eq!(control.retired.load(Ordering::Acquire), 1);
    }
}
