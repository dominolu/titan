use std::{
    cmp::Ordering as CmpOrdering,
    collections::{BTreeMap, BinaryHeap, HashMap},
    ops::Bound::{Excluded, Unbounded},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use crossbeam_channel::{Receiver, Sender, TryRecvError, bounded};
use crossbeam_queue::ArrayQueue;
use titan_plugin_engine::{EventQos, PluginIdentity, SubscriptionSpec};

use crate::{
    EngineClock, EngineError, EngineMetrics, EventArena, EventClass, EventDescriptor, EventHeader,
    EventRecord, FaultKind, FaultSignal, PendingAllocation, PendingEntry, PoolKind, PublishError,
    PublishRequest, ReserveRequest, RuntimeHealth, RuntimeHealthSnapshot, RuntimeMode,
    SubscriberChannel, SubscriberChannelArgs, SubscriberHealth, SubscriberHealthSnapshot,
    SubscriberRoute, SubscriberState, TimerSignal, TracePoint, TraceStage, TrackedDelivery,
    config::{DrainBudgetConfig, EventEngineConfig},
};

pub(crate) struct StagedSubscription {
    pub owner: PluginIdentity,
    pub spec: SubscriptionSpec,
}

pub(crate) enum ControlCommand {
    Commit {
        base_version: u64,
        staged: Vec<StagedSubscription>,
        reply: Sender<Result<(u64, Vec<(u64, Arc<SubscriberChannel>)>), EngineError>>,
    },
    Retire {
        token: u64,
        reply: Sender<Result<Arc<SubscriberChannel>, EngineError>>,
    },
    Recover {
        token: u64,
        recovery_sequence: u64,
        reply: Sender<Result<(), EngineError>>,
    },
    Shutdown {
        reply: Sender<Vec<Arc<SubscriberChannel>>>,
    },
    ScheduleTimer {
        timer_id: u64,
        deadline_ns: u64,
        reply: Sender<Result<(), EngineError>>,
    },
}

type Catalog = HashMap<Arc<str>, HashMap<u32, Arc<EventDescriptor>>>;

pub(crate) struct EngineShared {
    pub config: Arc<EventEngineConfig>,
    pub arena: Arc<EventArena>,
    pub metrics: Arc<EngineMetrics>,
    pub critical_ingress: ArrayQueue<EventRecord>,
    pub market_ingress: ArrayQueue<EventRecord>,
    pub fault_signals: Arc<ArrayQueue<FaultSignal>>,
    pub control_tx: Sender<ControlCommand>,
    pub catalog: RwLock<Catalog>,
    pub next_event_type: AtomicU32,
    pub route_version: AtomicU64,
    pub next_transaction: AtomicU64,
    pub next_candidate: AtomicU64,
    pub running: AtomicBool,
    pub clock: EngineClock,
    pub health_registry: RwLock<BTreeMap<u64, Arc<SubscriberHealth>>>,
    pub transactions: Arc<Mutex<BTreeMap<u64, (u64, Vec<StagedSubscription>)>>>,
    pub timer_signals: ArrayQueue<TimerSignal>,
    pub runtime_health: RuntimeHealth,
    pub channel_registry: RwLock<BTreeMap<u64, Arc<SubscriberChannel>>>,
    pub trace_ring: Arc<ArrayQueue<TracePoint>>,
    pub pending_depth: AtomicUsize,
    pub pressure_scan_cursor: AtomicU64,
}

impl EngineShared {
    pub(crate) fn signal(&self, signal: FaultSignal) {
        if self.fault_signals.push(signal).is_err() {
            self.metrics
                .fault_signal_drop_total
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn trace(&self, point: TracePoint) {
        if self.trace_ring.push(point).is_err() {
            self.metrics
                .trace_ring_drop_total
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn descriptor(
        &self,
        event_type: &str,
        schema_version: u32,
    ) -> Option<Arc<EventDescriptor>> {
        self.catalog
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(event_type)
            .and_then(|versions| versions.get(&schema_version))
            .cloned()
    }

    fn enqueue_record(&self, record: EventRecord) -> Result<(), PublishError> {
        if !self.running.load(Ordering::Acquire) {
            return Err(PublishError::Stopped);
        }
        let class = record.descriptor.class;
        let source_sequence = record.header.source_sequence;
        let descriptor_id = record.descriptor.id;
        let trace = record.header.trace;
        let result = match class {
            EventClass::Critical => self
                .critical_ingress
                .push(record)
                .map_err(|_| PublishError::CriticalIngressFull),
            EventClass::Market => self
                .market_ingress
                .push(record)
                .map_err(|_| PublishError::MarketIngressFull),
        };
        match result {
            Ok(()) => {
                self.metrics.publish_total.fetch_add(1, Ordering::Relaxed);
                self.trace(TracePoint {
                    trace,
                    local_sequence: 0,
                    source_sequence,
                    subscriber_id: 0,
                    stage: TraceStage::Published,
                    timestamp_ns: self.clock.now_ns(),
                });
                Ok(())
            }
            Err(error) => {
                self.runtime_health
                    .mark_ingress_full(class == EventClass::Critical);
                self.metrics
                    .publish_rejected_total
                    .fetch_add(1, Ordering::Relaxed);
                self.signal(FaultSignal {
                    kind: match class {
                        EventClass::Critical => FaultKind::CriticalIngressFull,
                        EventClass::Market => FaultKind::MarketIngressFull,
                    },
                    subscriber_id: 0,
                    sequence: source_sequence,
                    detail: descriptor_id as u64,
                });
                Err(error)
            }
        }
    }
}

pub struct EventEngine {
    shared: Arc<EngineShared>,
    control_rx: Mutex<Option<Receiver<ControlCommand>>>,
    runtime: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Clone)]
pub struct EventEngineHandle {
    pub(crate) shared: Arc<EngineShared>,
    pub(crate) transactions: Arc<Mutex<BTreeMap<u64, (u64, Vec<StagedSubscription>)>>>,
}

impl EventEngine {
    pub fn new(config: EventEngineConfig) -> Result<Self, EngineError> {
        config.validate()?;
        let config = Arc::new(config);
        let metrics = Arc::new(EngineMetrics::default());
        let arena = EventArena::new(&config.arena, metrics.clone());
        let control_capacity = (config.subscribers.max_count * 4).max(64);
        let (control_tx, control_rx) = bounded(control_capacity);
        let shared = Arc::new(EngineShared {
            arena,
            metrics,
            critical_ingress: ArrayQueue::new(config.ingress.critical_capacity),
            market_ingress: ArrayQueue::new(config.ingress.market_capacity),
            fault_signals: Arc::new(ArrayQueue::new(config.fault_signal_ring.capacity)),
            control_tx,
            catalog: RwLock::new(HashMap::new()),
            next_event_type: AtomicU32::new(1),
            route_version: AtomicU64::new(0),
            next_transaction: AtomicU64::new(1),
            next_candidate: AtomicU64::new(1),
            running: AtomicBool::new(false),
            clock: EngineClock::new(),
            health_registry: RwLock::new(BTreeMap::new()),
            transactions: Arc::new(Mutex::new(BTreeMap::new())),
            timer_signals: ArrayQueue::new(config.dispatch.timer_capacity),
            runtime_health: RuntimeHealth::default(),
            channel_registry: RwLock::new(BTreeMap::new()),
            trace_ring: Arc::new(ArrayQueue::new(config.diagnostics.trace_ring_capacity)),
            pending_depth: AtomicUsize::new(0),
            pressure_scan_cursor: AtomicU64::new(0),
            config,
        });
        Ok(Self {
            shared,
            control_rx: Mutex::new(Some(control_rx)),
            runtime: Mutex::new(None),
        })
    }

    pub fn handle(&self) -> EventEngineHandle {
        EventEngineHandle {
            shared: self.shared.clone(),
            transactions: self.shared.transactions.clone(),
        }
    }

    pub fn start(&self) -> Result<(), EngineError> {
        if self
            .shared
            .running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(EngineError::AlreadyStarted);
        }
        let control_rx = self
            .control_rx
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
            .ok_or(EngineError::AlreadyStarted)?;
        let shared = self.shared.clone();
        let thread_shared = shared.clone();
        let runtime = thread::Builder::new()
            .name("titan-event-engine".into())
            .spawn(move || {
                let result = catch_unwind(AssertUnwindSafe(|| {
                    EventLoop::new(thread_shared.clone(), control_rx).run()
                }));
                if result.is_err() {
                    thread_shared.runtime_health.mark_event_loop_failed();
                    thread_shared.signal(FaultSignal {
                        kind: FaultKind::EventLoopFailed,
                        subscriber_id: 0,
                        sequence: 0,
                        detail: 0,
                    });
                    for channel in thread_shared
                        .channel_registry
                        .read()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .values()
                    {
                        channel.request_stop();
                    }
                }
                thread_shared.running.store(false, Ordering::Release);
            })
            .map_err(|error| {
                shared.running.store(false, Ordering::Release);
                EngineError::SubscriberRuntime(error.to_string())
            })?;
        *self
            .runtime
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(runtime);
        Ok(())
    }

    pub fn stop(&self) -> Result<(), EngineError> {
        let has_runtime = self
            .runtime
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_some();
        if !has_runtime {
            return Ok(());
        }
        let channels = if self.shared.running.load(Ordering::Acquire) {
            let (reply_tx, reply_rx) = bounded(1);
            self.shared
                .control_tx
                .try_send(ControlCommand::Shutdown { reply: reply_tx })
                .map_err(|_| EngineError::ControlQueueFull)?;
            reply_rx
                .recv_timeout(Duration::from_secs(5))
                .map_err(|_| EngineError::ControlTimeout)?
        } else {
            self.shared
                .channel_registry
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .values()
                .cloned()
                .collect()
        };
        for channel in channels {
            channel.stop_and_drain();
        }
        if let Some(runtime) = self
            .runtime
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            let _ = runtime.join();
        }
        while self.shared.critical_ingress.pop().is_some() {}
        while self.shared.market_ingress.pop().is_some() {}
        self.shared
            .channel_registry
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
        self.shared.running.store(false, Ordering::Release);
        let outstanding = self.shared.arena.outstanding_blocks();
        if outstanding != 0 {
            return Err(EngineError::OutstandingBlocks(outstanding));
        }
        if self.shared.runtime_health.snapshot().event_loop_failed {
            return Err(EngineError::EventLoopFailed);
        }
        Ok(())
    }

    pub fn metrics(&self) -> &Arc<EngineMetrics> {
        &self.shared.metrics
    }

    pub fn arena(&self) -> &Arc<EventArena> {
        &self.shared.arena
    }
}

impl Drop for EventEngine {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

impl EventEngineHandle {
    pub fn register_event(
        &self,
        event_type: impl Into<Arc<str>>,
        schema_version: u32,
        class: EventClass,
        pool: PoolKind,
    ) -> Result<Arc<EventDescriptor>, EngineError> {
        let event_type = event_type.into();
        let mut catalog = self
            .shared
            .catalog
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(existing) = catalog
            .get(event_type.as_ref())
            .and_then(|versions| versions.get(&schema_version))
        {
            return if existing.class == class && existing.pool == pool {
                Ok(existing.clone())
            } else {
                Err(EngineError::InvalidEvent)
            };
        }
        let id = self.shared.next_event_type.fetch_add(1, Ordering::Relaxed);
        let descriptor = Arc::new(EventDescriptor {
            id,
            event_type: event_type.clone(),
            schema_version,
            class,
            pool,
        });
        catalog
            .entry(event_type)
            .or_default()
            .insert(schema_version, descriptor.clone());
        Ok(descriptor)
    }

    pub fn try_publish(&self, request: PublishRequest<'_>) -> Result<(), PublishError> {
        if !self.shared.running.load(Ordering::Acquire) {
            return Err(PublishError::Stopped);
        }
        let descriptor = self
            .shared
            .descriptor(request.event_type, request.schema_version)
            .ok_or(PublishError::InvalidEvent)?;
        if request.source_sequence != 0
            && request.source_id as usize >= self.shared.config.ingress.max_sources
        {
            return Err(PublishError::InvalidEvent);
        }
        let mut reservation = match self
            .shared
            .arena
            .reserve(descriptor.pool, request.payload.len())
        {
            Ok(value) => value,
            Err(error) => {
                self.shared
                    .runtime_health
                    .mark_arena_pressure(descriptor.pool);
                self.shared
                    .metrics
                    .publish_rejected_total
                    .fetch_add(1, Ordering::Relaxed);
                self.shared.signal(FaultSignal {
                    kind: FaultKind::ArenaPressure,
                    subscriber_id: 0,
                    sequence: request.source_sequence,
                    detail: descriptor.pool as u64,
                });
                return Err(error);
            }
        };
        reservation.payload_mut().copy_from_slice(request.payload);
        let payload = reservation.commit();
        let record = EventRecord {
            descriptor: descriptor.clone(),
            header: EventHeader {
                source_id: request.source_id,
                event_type_id: descriptor.id,
                schema_version: request.schema_version,
                flags: request.flags,
                source_sequence: request.source_sequence,
                local_sequence: 0,
                exchange_ts: request.exchange_ts,
                receive_ts: request.receive_ts,
                publish_ts: request.publish_ts,
                routing_key: request.routing_key,
                trace: request.trace,
            },
            payload,
            ingress_at_ns: self.shared.clock.now_ns(),
        };
        self.shared.enqueue_record(record)
    }

    pub fn reserve_market_batch(
        &self,
        request: ReserveRequest<'_>,
    ) -> Result<MarketBatchReservation, PublishError> {
        if !self.shared.running.load(Ordering::Acquire) {
            return Err(PublishError::Stopped);
        }
        if request.source_sequence != 0
            && request.source_id as usize >= self.shared.config.ingress.max_sources
        {
            return Err(PublishError::InvalidEvent);
        }
        let descriptor = self
            .shared
            .descriptor(request.event_type, request.schema_version)
            .filter(|descriptor| {
                descriptor.class == EventClass::Market && descriptor.pool == PoolKind::MarketBatch
            })
            .ok_or(PublishError::InvalidEvent)?;
        let reservation = self
            .shared
            .arena
            .reserve(PoolKind::MarketBatch, request.payload_length)
            .map_err(|error| {
                self.shared
                    .runtime_health
                    .mark_arena_pressure(PoolKind::MarketBatch);
                self.shared.signal(FaultSignal {
                    kind: FaultKind::ArenaPressure,
                    subscriber_id: 0,
                    sequence: request.source_sequence,
                    detail: PoolKind::MarketBatch as u64,
                });
                error
            })?;
        Ok(MarketBatchReservation {
            shared: self.shared.clone(),
            descriptor: descriptor.clone(),
            header: EventHeader {
                source_id: request.source_id,
                event_type_id: descriptor.id,
                schema_version: request.schema_version,
                flags: request.flags,
                source_sequence: request.source_sequence,
                local_sequence: 0,
                exchange_ts: request.exchange_ts,
                receive_ts: request.receive_ts,
                publish_ts: request.publish_ts,
                routing_key: request.routing_key,
                trace: request.trace,
            },
            ingress_at_ns: self.shared.clock.now_ns(),
            reservation,
        })
    }

    pub fn pop_fault_signal(&self) -> Option<FaultSignal> {
        self.shared.fault_signals.pop()
    }

    pub fn pop_trace_point(&self) -> Option<TracePoint> {
        self.shared.trace_ring.pop()
    }

    pub fn runtime_health(&self) -> RuntimeHealthSnapshot {
        self.shared.runtime_health.snapshot()
    }

    pub fn queue_depths(&self) -> QueueDepthSnapshot {
        QueueDepthSnapshot {
            critical_ingress: self.shared.critical_ingress.len(),
            market_ingress: self.shared.market_ingress.len(),
            pending_dispatch: self.shared.pending_depth.load(Ordering::Acquire),
            fault_signals: self.shared.fault_signals.len(),
            trace_ring: self.shared.trace_ring.len(),
            timer_signals: self.shared.timer_signals.len(),
        }
    }

    pub fn clear_runtime_health(&self) {
        self.shared.runtime_health.clear();
    }

    pub fn now_ns(&self) -> u64 {
        self.shared.clock.now_ns()
    }

    pub fn schedule_timer(&self, timer_id: u64, deadline_ns: u64) -> Result<(), EngineError> {
        if !self.shared.running.load(Ordering::Acquire) {
            return Err(EngineError::NotRunning);
        }
        let (reply_tx, reply_rx) = bounded(1);
        self.shared
            .control_tx
            .try_send(ControlCommand::ScheduleTimer {
                timer_id,
                deadline_ns,
                reply: reply_tx,
            })
            .map_err(|_| EngineError::ControlQueueFull)?;
        reply_rx
            .recv_timeout(Duration::from_secs(5))
            .map_err(|_| EngineError::ControlTimeout)?
    }

    pub fn pop_timer_signal(&self) -> Option<TimerSignal> {
        self.shared.timer_signals.pop()
    }

    pub fn subscriber_health(&self, token: u64) -> Option<SubscriberHealthSnapshot> {
        self.shared
            .health_registry
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&token)
            .map(|health| health.snapshot())
    }

    pub fn top_pressure_subscribers(&self, limit: usize) -> Vec<(u64, SubscriberHealthSnapshot)> {
        let registry = self
            .shared
            .health_registry
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut result = registry
            .iter()
            .map(|(id, health)| (*id, health.snapshot()))
            .collect::<Vec<_>>();
        result.sort_unstable_by_key(|(_, health)| std::cmp::Reverse(health.outstanding_handles));
        result.truncate(limit);
        result
    }

    pub fn pressure_subscriber_batch(&self) -> Vec<(u64, SubscriberHealthSnapshot)> {
        let registry = self
            .shared
            .health_registry
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let budget = self
            .shared
            .config
            .diagnostics
            .pressure_scan_budget
            .min(registry.len());
        let cursor = self.shared.pressure_scan_cursor.load(Ordering::Acquire);
        let result = registry
            .range((Excluded(cursor), Unbounded))
            .chain(registry.range(..=cursor))
            .take(budget)
            .map(|(id, health)| (*id, health.snapshot()))
            .collect::<Vec<_>>();
        if let Some((last, _)) = result.last() {
            self.shared
                .pressure_scan_cursor
                .store(*last, Ordering::Release);
        }
        result
    }

    pub fn complete_recovery(&self, token: u64, recovery_sequence: u64) -> Result<(), EngineError> {
        if !self.shared.running.load(Ordering::Acquire) {
            return Err(EngineError::NotRunning);
        }
        let (reply_tx, reply_rx) = bounded(1);
        self.shared
            .control_tx
            .try_send(ControlCommand::Recover {
                token,
                recovery_sequence,
                reply: reply_tx,
            })
            .map_err(|_| EngineError::ControlQueueFull)?;
        reply_rx
            .recv_timeout(Duration::from_secs(5))
            .map_err(|_| EngineError::ControlTimeout)?
    }
}

pub struct MarketBatchReservation {
    shared: Arc<EngineShared>,
    descriptor: Arc<EventDescriptor>,
    header: EventHeader,
    ingress_at_ns: u64,
    reservation: crate::EventReservation,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct QueueDepthSnapshot {
    pub critical_ingress: usize,
    pub market_ingress: usize,
    pub pending_dispatch: usize,
    pub fault_signals: usize,
    pub trace_ring: usize,
    pub timer_signals: usize,
}

impl MarketBatchReservation {
    pub fn payload_mut(&mut self) -> &mut [u8] {
        self.reservation.payload_mut()
    }

    pub fn commit(self) -> Result<(), PublishError> {
        let record = EventRecord {
            descriptor: self.descriptor,
            header: self.header,
            payload: self.reservation.commit(),
            ingress_at_ns: self.ingress_at_ns,
        };
        self.shared.enqueue_record(record)
    }
}

struct FanoutContinuation {
    record: EventRecord,
    after_token: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TimerEntry {
    deadline_ns: u64,
    timer_id: u64,
}

impl Ord for TimerEntry {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        other
            .deadline_ns
            .cmp(&self.deadline_ns)
            .then_with(|| other.timer_id.cmp(&self.timer_id))
    }
}

impl PartialOrd for TimerEntry {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

struct EventLoop {
    shared: Arc<EngineShared>,
    control_rx: Receiver<ControlCommand>,
    routes: BTreeMap<u64, SubscriberRoute>,
    route_index: BTreeMap<u32, std::collections::BTreeSet<u64>>,
    retire_waiters: BTreeMap<u64, Sender<Result<Arc<SubscriberChannel>, EngineError>>>,
    next_token: u64,
    local_sequence: u64,
    global_pending: usize,
    reserved_pending_unused: usize,
    critical_continuation: Option<FanoutContinuation>,
    market_continuation: Option<FanoutContinuation>,
    shutdown: bool,
    shutdown_reply: Option<Sender<Vec<Arc<SubscriberChannel>>>>,
    idle_count: usize,
    timers: BinaryHeap<TimerEntry>,
    source_sequences: Vec<u64>,
    last_service_ns: [u64; 4],
    pending_cursor: u64,
}

impl EventLoop {
    fn new(shared: Arc<EngineShared>, control_rx: Receiver<ControlCommand>) -> Self {
        let timer_capacity = shared.config.dispatch.timer_capacity;
        let max_sources = shared.config.ingress.max_sources;
        Self {
            shared,
            control_rx,
            routes: BTreeMap::new(),
            route_index: BTreeMap::new(),
            retire_waiters: BTreeMap::new(),
            next_token: 1,
            local_sequence: 0,
            global_pending: 0,
            reserved_pending_unused: 0,
            critical_continuation: None,
            market_continuation: None,
            shutdown: false,
            shutdown_reply: None,
            idle_count: 0,
            timers: BinaryHeap::with_capacity(timer_capacity),
            source_sequences: vec![0; max_sources],
            last_service_ns: [0; 4],
            pending_cursor: 0,
        }
    }

    fn run(mut self) {
        if let Some(core_id) = self.shared.config.runtime.cpu_affinity {
            let _ = core_affinity::set_for_current(core_affinity::CoreId { id: core_id });
        }
        while !self.shutdown {
            let work = self.drain_once();
            if work > 0 {
                self.idle_count = 0;
                continue;
            }
            match self.shared.config.runtime.mode {
                RuntimeMode::Dedicated => std::hint::spin_loop(),
                RuntimeMode::SpinSleep => {
                    if self.idle_count < self.shared.config.runtime.spin_iterations {
                        self.idle_count += 1;
                        std::hint::spin_loop();
                    } else {
                        thread::sleep(Duration::from_micros(self.shared.config.runtime.sleep_us));
                    }
                }
            }
        }
    }

    fn drain_once(&mut self) -> usize {
        let started = Instant::now();
        let mut work = 0;
        if self.critical_continuation.is_none() && self.market_continuation.is_none() {
            work += self.process_control(64);
        }
        let critical = self.drain_critical(self.shared.config.dispatch.critical);
        self.observe_service(0, critical);
        work += critical;
        let pending = self.retry_pending(self.shared.config.dispatch.pending);
        self.observe_service(1, pending);
        work += pending;
        work += self.complete_retirements();
        let market = self.drain_market(self.shared.config.dispatch.market);
        self.observe_service(2, market);
        work += market;
        let timers = self.process_due_timers(self.shared.config.dispatch.timer);
        self.observe_service(3, timers);
        work += timers;
        work += self.try_complete_shutdown();
        let elapsed = started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        self.shared
            .metrics
            .drain_count
            .fetch_add(1, Ordering::Relaxed);
        self.shared.metrics.observe_drain_duration(elapsed);
        if elapsed > self.shared.config.dispatch.max_drain_once_ns {
            self.shared
                .metrics
                .drain_over_budget_total
                .fetch_add(1, Ordering::Relaxed);
        }
        self.shared
            .pending_depth
            .store(self.global_pending, Ordering::Release);
        work
    }

    fn process_control(&mut self, budget: usize) -> usize {
        let mut processed = 0;
        while processed < budget {
            let command = match self.control_rx.try_recv() {
                Ok(command) => command,
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
            };
            processed += 1;
            match command {
                ControlCommand::Commit {
                    base_version,
                    staged,
                    reply,
                } => {
                    let _ = reply.try_send(self.commit_routes(base_version, staged));
                }
                ControlCommand::Retire { token, reply } => {
                    if let Err(error) = self.begin_retire_route(token, reply.clone()) {
                        let _ = reply.try_send(Err(error));
                    }
                }
                ControlCommand::Recover {
                    token,
                    recovery_sequence,
                    reply,
                } => {
                    let _ = reply.try_send(self.recover_route(token, recovery_sequence));
                }
                ControlCommand::Shutdown { reply } => {
                    self.shared.running.store(false, Ordering::Release);
                    self.shutdown_reply = Some(reply);
                    for route in self.routes.values() {
                        if !route.channel.can_consume() {
                            route.channel.request_stop();
                        }
                    }
                    break;
                }
                ControlCommand::ScheduleTimer {
                    timer_id,
                    deadline_ns,
                    reply,
                } => {
                    let result = if self.timers.len() >= self.shared.config.dispatch.timer_capacity
                    {
                        Err(EngineError::TimerQueueFull)
                    } else {
                        self.timers.push(TimerEntry {
                            deadline_ns,
                            timer_id,
                        });
                        Ok(())
                    };
                    let _ = reply.try_send(result);
                }
            }
        }
        processed
    }

    fn commit_routes(
        &mut self,
        base_version: u64,
        staged: Vec<StagedSubscription>,
    ) -> Result<(u64, Vec<(u64, Arc<SubscriberChannel>)>), EngineError> {
        if base_version != self.shared.route_version.load(Ordering::Acquire) {
            return Err(EngineError::StaleRouteVersion);
        }
        if self.routes.len() + staged.len() > self.shared.config.subscribers.max_count {
            return Err(EngineError::SubscriberLimit);
        }
        for item in &staged {
            if item.spec.capacity <= self.shared.config.subscribers.critical_reserve {
                return Err(EngineError::InvalidSubscriptionCapacity);
            }
            if self
                .shared
                .descriptor(&item.spec.event_type, item.spec.schema_version)
                .is_none()
            {
                return Err(EngineError::InvalidEvent);
            }
        }
        let critical_guarantee = self.pending_guarantee();
        let existing_guarantee: usize = self
            .routes
            .values()
            .map(|route| route.pending_guarantee)
            .sum();
        let staged_guarantee = staged
            .iter()
            .filter(|item| {
                self.shared
                    .descriptor(&item.spec.event_type, item.spec.schema_version)
                    .is_some_and(|descriptor| descriptor.class == EventClass::Critical)
            })
            .count()
            .saturating_mul(critical_guarantee);
        if existing_guarantee.saturating_add(staged_guarantee)
            > self.shared.config.pending_dispatch.global_capacity
        {
            return Err(EngineError::InvalidSubscriptionCapacity);
        }
        let mut tokens = Vec::with_capacity(staged.len());
        for item in staged {
            let token = self.next_token;
            self.next_token += 1;
            let health = Arc::new(SubscriberHealth::default());
            let channel = SubscriberChannel::new(SubscriberChannelArgs {
                id: token,
                owner: item.owner,
                capacity: item.spec.capacity,
                critical_reserve: self.shared.config.subscribers.critical_reserve,
                high_ratio: self.shared.config.subscribers.lagging_high_watermark_ratio,
                low_ratio: self.shared.config.subscribers.recovery_low_watermark_ratio,
                health: health.clone(),
                idle_sleep: Duration::from_micros(self.shared.config.subscribers.idle_sleep_us),
                fault_signals: self.shared.fault_signals.clone(),
                trace_ring: self.shared.trace_ring.clone(),
                metrics: self.shared.metrics.clone(),
            });
            let descriptor = self
                .shared
                .descriptor(&item.spec.event_type, item.spec.schema_version)
                .expect("staged event was validated");
            let route_guarantee = if descriptor.class == EventClass::Critical {
                critical_guarantee
            } else {
                0
            };
            let route = SubscriberRoute::new(
                token,
                descriptor.id,
                &item.spec,
                channel.clone(),
                self.shared.config.pending_dispatch.per_subscriber_capacity,
                route_guarantee,
                ((self.shared.config.pending_dispatch.per_subscriber_capacity as f64
                    * self.shared.config.pending_dispatch.high_watermark_ratio)
                    .ceil() as usize)
                    .clamp(
                        1,
                        self.shared.config.pending_dispatch.per_subscriber_capacity,
                    ),
            );
            self.reserved_pending_unused += route_guarantee;
            self.shared
                .health_registry
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .insert(token, health);
            self.shared
                .channel_registry
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .insert(token, channel.clone());
            self.routes.insert(token, route);
            self.route_index
                .entry(descriptor.id)
                .or_default()
                .insert(token);
            tokens.push((token, channel));
        }
        let version = self.shared.route_version.fetch_add(1, Ordering::Release) + 1;
        Ok((version, tokens))
    }

    fn begin_retire_route(
        &mut self,
        token: u64,
        reply: Sender<Result<Arc<SubscriberChannel>, EngineError>>,
    ) -> Result<(), EngineError> {
        if !self.routes.contains_key(&token) || self.retire_waiters.contains_key(&token) {
            return Err(EngineError::UnknownSubscription(token));
        }
        let event_type_id = self
            .routes
            .get(&token)
            .expect("route existence was checked")
            .event_type_id;
        if let Some(tokens) = self.route_index.get_mut(&event_type_id) {
            tokens.remove(&token);
            if tokens.is_empty() {
                self.route_index.remove(&event_type_id);
            }
        }
        self.retire_waiters.insert(token, reply);
        self.shared.route_version.fetch_add(1, Ordering::Release);
        Ok(())
    }

    fn complete_retirements(&mut self) -> usize {
        let ready: Vec<u64> = self
            .retire_waiters
            .keys()
            .copied()
            .filter(|token| {
                self.routes
                    .get(token)
                    .is_some_and(|route| route.pending.is_empty())
            })
            .collect();
        let completed = ready.len();
        for token in ready {
            let mut route = self
                .routes
                .remove(&token)
                .expect("retiring route remains installed until pending is empty");
            self.remove_pending_reservation(&mut route);
            route.channel.request_stop();
            self.shared
                .health_registry
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&token);
            self.shared
                .channel_registry
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&token);
            if let Some(reply) = self.retire_waiters.remove(&token) {
                let _ = reply.try_send(Ok(route.channel));
            }
        }
        completed
    }

    fn discard_pending_as_gap(&mut self, route: &mut SubscriberRoute) {
        let guarantee = route.pending_guarantee;
        let old_len = route.pending.len();
        for entry in route.pending.drain(..) {
            route
                .channel
                .health()
                .record_gap(entry.delivery.local_sequence());
        }
        self.global_pending -= old_len;
        self.reserved_pending_unused += old_len.min(guarantee);
        route.channel.health().set_pending_depth(0);
        if old_len > 0 {
            self.shared
                .metrics
                .delivery_gap_total
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn recover_route(&mut self, token: u64, recovery_sequence: u64) -> Result<(), EngineError> {
        let route = self
            .routes
            .get_mut(&token)
            .ok_or(EngineError::UnknownSubscription(token))?;
        let health = route.channel.health();
        if health.state() != SubscriberState::ResyncRequired
            || health
                .delivery_gap()
                .is_some_and(|(_, last)| recovery_sequence < last)
        {
            return Err(EngineError::InvalidSubscriptionCapacity);
        }
        if health.outstanding_handles() != 0 {
            return Err(EngineError::RecoveryNotQuiescent(token));
        }
        health.begin_recovery(recovery_sequence);
        route.channel.clear_queued();
        health.finish_recovery();
        route.channel.resume();
        Ok(())
    }

    fn shutdown_routes(&mut self) -> Vec<Arc<SubscriberChannel>> {
        let mut channels = Vec::with_capacity(self.routes.len());
        let routes = std::mem::take(&mut self.routes);
        let mut retire_waiters = std::mem::take(&mut self.retire_waiters);
        self.route_index.clear();
        for (token, mut route) in routes {
            self.discard_pending_as_gap(&mut route);
            self.remove_pending_reservation(&mut route);
            route.channel.request_stop();
            if let Some(reply) = retire_waiters.remove(&token) {
                let _ = reply.try_send(Ok(route.channel.clone()));
            }
            channels.push(route.channel);
        }
        self.shared
            .health_registry
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
        self.shared
            .channel_registry
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
        channels
    }

    fn try_complete_shutdown(&mut self) -> usize {
        if self.shutdown_reply.is_none()
            || !self.shared.critical_ingress.is_empty()
            || !self.shared.market_ingress.is_empty()
            || self.critical_continuation.is_some()
            || self.market_continuation.is_some()
            || self.routes.values().any(|route| !route.pending.is_empty())
        {
            return 0;
        }
        let channels = self.shutdown_routes();
        if let Some(reply) = self.shutdown_reply.take() {
            let _ = reply.try_send(channels);
        }
        self.shutdown = true;
        1
    }

    fn pending_guarantee(&self) -> usize {
        match self.shared.config.pending_dispatch.allocation {
            PendingAllocation::Shared => {
                self.shared
                    .config
                    .pending_dispatch
                    .guaranteed_per_critical_subscriber
            }
            PendingAllocation::Guaranteed => {
                self.shared.config.pending_dispatch.per_subscriber_capacity
            }
        }
    }

    fn drain_critical(&mut self, budget: DrainBudgetConfig) -> usize {
        self.drain_ingress(EventClass::Critical, budget)
    }

    fn drain_market(&mut self, budget: DrainBudgetConfig) -> usize {
        self.drain_ingress(EventClass::Market, budget)
    }

    fn drain_ingress(&mut self, class: EventClass, budget: DrainBudgetConfig) -> usize {
        let started = Instant::now();
        let mut processed = 0;
        while processed < budget.max_items
            && started.elapsed().as_nanos() < budget.max_elapsed_ns as u128
        {
            let continuation = match class {
                EventClass::Critical => self.critical_continuation.take(),
                EventClass::Market => self.market_continuation.take(),
            };
            let mut continuation = if let Some(value) = continuation {
                value
            } else {
                let record = match class {
                    EventClass::Critical => self.shared.critical_ingress.pop(),
                    EventClass::Market => self.shared.market_ingress.pop(),
                };
                let Some(mut record) = record else { break };
                self.local_sequence = self
                    .local_sequence
                    .checked_add(1)
                    .expect("local sequence overflow");
                record.header.local_sequence = self.local_sequence;
                self.shared.trace(TracePoint {
                    trace: record.header.trace,
                    local_sequence: record.header.local_sequence,
                    source_sequence: record.header.source_sequence,
                    subscriber_id: 0,
                    stage: TraceStage::EventLoopDequeued,
                    timestamp_ns: self.shared.clock.now_ns(),
                });
                if !self.accept_source_sequence(&record) {
                    processed += 1;
                    continue;
                }
                FanoutContinuation {
                    record,
                    after_token: 0,
                }
            };
            let complete = self.route_step(&mut continuation);
            processed += 1;
            if !complete {
                self.shared
                    .metrics
                    .fanout_continuation_total
                    .fetch_add(1, Ordering::Relaxed);
                match class {
                    EventClass::Critical => self.critical_continuation = Some(continuation),
                    EventClass::Market => self.market_continuation = Some(continuation),
                }
            }
        }
        processed
    }

    fn route_step(&mut self, continuation: &mut FanoutContinuation) -> bool {
        let mut routed = 0;
        let max_fanout = self.shared.config.dispatch.max_fanout_per_step;
        let shared = &self.shared;
        let Some(tokens) = self.route_index.get(&continuation.record.descriptor.id) else {
            return true;
        };
        for &token in tokens.range((Excluded(continuation.after_token), Unbounded)) {
            let route = self
                .routes
                .get_mut(&token)
                .expect("route index only contains active routes");
            if !route.matches_routing_key(continuation.record.header.routing_key) {
                continue;
            }
            Self::deliver_to_route(
                shared,
                &mut self.global_pending,
                &mut self.reserved_pending_unused,
                route,
                &continuation.record,
            );
            routed += 1;
            if routed == max_fanout {
                continuation.after_token = token;
                return false;
            }
        }
        true
    }

    fn deliver_to_route(
        shared: &Arc<EngineShared>,
        global_pending: &mut usize,
        reserved_pending_unused: &mut usize,
        route: &mut SubscriberRoute,
        record: &EventRecord,
    ) {
        let pending_guarantee = route.pending_guarantee;
        let state = route.channel.health().state();
        if matches!(
            state,
            SubscriberState::ResyncRequired | SubscriberState::Failed | SubscriberState::Stopped
        ) {
            if record.descriptor.class == EventClass::Critical {
                route
                    .channel
                    .health()
                    .record_gap(record.header.local_sequence);
                shared
                    .metrics
                    .delivery_gap_total
                    .fetch_add(1, Ordering::Relaxed);
            } else {
                shared.metrics.drop_total.fetch_add(1, Ordering::Relaxed);
            }
            return;
        }
        let tracked = TrackedDelivery::new(
            record.delivery(),
            route.channel.health().clone(),
            shared.clock.clone(),
        );
        let dispatch_latency = shared
            .clock
            .now_ns()
            .saturating_sub(tracked.ingress_at_ns());
        let dispatch_trace =
            tracked.trace_point(route.id, TraceStage::Dispatched, shared.clock.now_ns());
        match record.descriptor.class {
            EventClass::Critical => {
                if !route.pending.is_empty() || state == SubscriberState::Pending {
                    Self::enqueue_pending(
                        shared,
                        global_pending,
                        reserved_pending_unused,
                        pending_guarantee,
                        route,
                        tracked,
                    );
                } else if let Err(tracked) = route.channel.try_push_critical(tracked) {
                    Self::enqueue_pending(
                        shared,
                        global_pending,
                        reserved_pending_unused,
                        pending_guarantee,
                        route,
                        tracked,
                    );
                } else {
                    shared
                        .metrics
                        .dispatch_total
                        .fetch_add(1, Ordering::Relaxed);
                    shared.metrics.observe_dispatch_latency(dispatch_latency);
                    shared.trace(dispatch_trace);
                }
            }
            EventClass::Market
                if !route.pending.is_empty() || state == SubscriberState::Pending =>
            {
                match route.qos {
                    EventQos::Latest => {
                        if route.channel.replace_latest(tracked) {
                            shared.metrics.drop_total.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    EventQos::BestEffort => {
                        drop(tracked);
                        shared.metrics.drop_total.fetch_add(1, Ordering::Relaxed);
                    }
                    EventQos::ReliableOrdered => Self::enter_resync(
                        shared,
                        global_pending,
                        reserved_pending_unused,
                        pending_guarantee,
                        route,
                        Some(tracked),
                        FaultKind::PendingFull,
                    ),
                }
            }
            EventClass::Market => match route.channel.try_push_market(tracked) {
                Ok(()) => {
                    shared
                        .metrics
                        .dispatch_total
                        .fetch_add(1, Ordering::Relaxed);
                    shared.metrics.observe_dispatch_latency(dispatch_latency);
                    shared.trace(dispatch_trace);
                }
                Err(tracked) => match route.qos {
                    EventQos::Latest => {
                        if route.channel.replace_latest(tracked) {
                            shared.metrics.drop_total.fetch_add(1, Ordering::Relaxed);
                        }
                        shared
                            .metrics
                            .dispatch_total
                            .fetch_add(1, Ordering::Relaxed);
                        shared.metrics.observe_dispatch_latency(dispatch_latency);
                        shared.trace(dispatch_trace);
                    }
                    EventQos::BestEffort => {
                        drop(tracked);
                        shared.metrics.drop_total.fetch_add(1, Ordering::Relaxed);
                    }
                    EventQos::ReliableOrdered => Self::enter_resync(
                        shared,
                        global_pending,
                        reserved_pending_unused,
                        pending_guarantee,
                        route,
                        Some(tracked),
                        FaultKind::PendingFull,
                    ),
                },
            },
        }
    }

    fn can_use_pending(
        shared: &EngineShared,
        global_pending: usize,
        reserved_pending_unused: usize,
        route_len: usize,
        guarantee: usize,
    ) -> bool {
        if global_pending >= shared.config.pending_dispatch.global_capacity {
            return false;
        }
        if route_len < guarantee {
            return true;
        }
        shared.config.pending_dispatch.global_capacity - global_pending > reserved_pending_unused
    }

    fn enqueue_pending(
        shared: &Arc<EngineShared>,
        global_pending: &mut usize,
        reserved_pending_unused: &mut usize,
        guarantee: usize,
        route: &mut SubscriberRoute,
        tracked: TrackedDelivery,
    ) {
        let old_len = route.pending.len();
        if old_len >= route.pending_capacity
            || !Self::can_use_pending(
                shared,
                *global_pending,
                *reserved_pending_unused,
                old_len,
                guarantee,
            )
        {
            Self::enter_resync(
                shared,
                global_pending,
                reserved_pending_unused,
                guarantee,
                route,
                Some(tracked),
                FaultKind::PendingFull,
            );
            return;
        }
        if old_len < guarantee {
            *reserved_pending_unused -= 1;
        }
        *global_pending += 1;
        route.pending.push_back(PendingEntry {
            delivery: tracked,
            enqueued_at_ns: shared.clock.now_ns(),
        });
        route
            .channel
            .health()
            .set_pending_depth(route.pending.len());
        if let Some(entry) = route.pending.back() {
            shared.trace(entry.delivery.trace_point(
                route.id,
                TraceStage::Pending,
                entry.enqueued_at_ns,
            ));
        }
        let previous = route
            .channel
            .health()
            .transition_nonterminal(SubscriberState::Pending);
        let crossed_high_watermark = old_len < route.pending_high_watermark
            && route.pending.len() >= route.pending_high_watermark;
        if previous.is_some_and(|state| state != SubscriberState::Pending) || crossed_high_watermark
        {
            shared.signal(FaultSignal {
                kind: FaultKind::SubscriberBackpressure,
                subscriber_id: route.id,
                sequence: route
                    .pending
                    .front()
                    .map(|entry| entry.delivery.local_sequence())
                    .unwrap_or(0),
                detail: route.pending.len() as u64,
            });
        }
    }

    fn enter_resync(
        shared: &Arc<EngineShared>,
        global_pending: &mut usize,
        reserved_pending_unused: &mut usize,
        guarantee: usize,
        route: &mut SubscriberRoute,
        current: Option<TrackedDelivery>,
        reason: FaultKind,
    ) {
        if let Some(current) = current {
            route.channel.health().record_gap(current.local_sequence());
        }
        let old_len = route.pending.len();
        for entry in route.pending.drain(..) {
            route
                .channel
                .health()
                .record_gap(entry.delivery.local_sequence());
        }
        *global_pending -= old_len;
        *reserved_pending_unused += old_len.min(guarantee);
        route.channel.health().set_pending_depth(0);
        route.channel.suspend();
        route.channel.clear_queued_as_gap();
        let entered_resync = route
            .channel
            .health()
            .transition_nonterminal(SubscriberState::ResyncRequired)
            .is_some();
        shared
            .metrics
            .delivery_gap_total
            .fetch_add(1, Ordering::Relaxed);
        if entered_resync {
            shared.metrics.resync_total.fetch_add(1, Ordering::Relaxed);
        }
        let sequence = route
            .channel
            .health()
            .delivery_gap()
            .map(|(_, last)| last)
            .unwrap_or(0);
        if entered_resync {
            shared.signal(FaultSignal {
                kind: reason,
                subscriber_id: route.id,
                sequence,
                detail: 0,
            });
        }
    }

    fn retry_pending(&mut self, budget: DrainBudgetConfig) -> usize {
        let started = Instant::now();
        let mut processed = 0;
        let max_age_ns = self
            .shared
            .config
            .pending_dispatch
            .max_age_ms
            .saturating_mul(1_000_000);
        let route_count = self.routes.len();
        let mut visited_without_attempt = 0;
        while processed < budget.max_items
            && started.elapsed().as_nanos() < budget.max_elapsed_ns as u128
            && visited_without_attempt < route_count
        {
            let Some(token) = self
                .routes
                .range((Excluded(self.pending_cursor), Unbounded))
                .next()
                .map(|(token, _)| *token)
                .or_else(|| self.routes.keys().next().copied())
            else {
                break;
            };
            self.pending_cursor = token;
            let route = self
                .routes
                .get_mut(&token)
                .expect("pending cursor selected an installed route");
            let guarantee = route.pending_guarantee;
            if matches!(
                route.channel.health().state(),
                SubscriberState::Failed | SubscriberState::Stopped
            ) {
                route.channel.suspend();
                route.channel.clear_queued_as_gap();
                let old_len = route.pending.len();
                for entry in route.pending.drain(..) {
                    route
                        .channel
                        .health()
                        .record_gap(entry.delivery.local_sequence());
                }
                self.global_pending -= old_len;
                self.reserved_pending_unused += old_len.min(guarantee);
                route.channel.health().set_pending_depth(0);
                if old_len > 0 {
                    self.shared
                        .metrics
                        .delivery_gap_total
                        .fetch_add(1, Ordering::Relaxed);
                }
                visited_without_attempt += 1;
                continue;
            }
            let Some(front) = route.pending.front() else {
                if route.channel.health().state() == SubscriberState::Pending
                    && route.channel.below_recovery_watermark()
                {
                    if route
                        .channel
                        .health()
                        .transition(SubscriberState::Pending, SubscriberState::Recovering)
                        && route
                            .channel
                            .health()
                            .transition(SubscriberState::Recovering, SubscriberState::Normal)
                    {
                        self.shared.signal(FaultSignal {
                            kind: FaultKind::SubscriberRecovered,
                            subscriber_id: route.id,
                            sequence: 0,
                            detail: 0,
                        });
                    }
                }
                visited_without_attempt += 1;
                continue;
            };
            visited_without_attempt = 0;
            let now = self.shared.clock.now_ns();
            if now.saturating_sub(front.enqueued_at_ns) > max_age_ns {
                Self::enter_resync(
                    &self.shared,
                    &mut self.global_pending,
                    &mut self.reserved_pending_unused,
                    guarantee,
                    route,
                    None,
                    FaultKind::PendingExpired,
                );
                processed += 1;
                continue;
            }
            let old_len = route.pending.len();
            let entry = route.pending.pop_front().expect("front was observed");
            route
                .channel
                .health()
                .set_pending_depth(route.pending.len());
            let dispatch_latency = now.saturating_sub(entry.delivery.ingress_at_ns());
            let dispatch_trace = entry
                .delivery
                .trace_point(route.id, TraceStage::Dispatched, now);
            self.global_pending -= 1;
            if old_len <= guarantee {
                self.reserved_pending_unused += 1;
            }
            match route.channel.try_push_critical(entry.delivery) {
                Ok(()) => {
                    self.shared
                        .metrics
                        .pending_retry_success
                        .fetch_add(1, Ordering::Relaxed);
                    self.shared
                        .metrics
                        .dispatch_total
                        .fetch_add(1, Ordering::Relaxed);
                    self.shared
                        .metrics
                        .observe_dispatch_latency(dispatch_latency);
                    self.shared.trace(dispatch_trace);
                }
                Err(delivery) => {
                    if old_len <= guarantee {
                        self.reserved_pending_unused -= 1;
                    }
                    self.global_pending += 1;
                    route.pending.push_front(PendingEntry {
                        delivery,
                        enqueued_at_ns: entry.enqueued_at_ns,
                    });
                    route
                        .channel
                        .health()
                        .set_pending_depth(route.pending.len());
                    self.shared
                        .metrics
                        .pending_retry_full
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
            processed += 1;
            if route.pending.is_empty()
                && route.channel.health().state() == SubscriberState::Pending
                && route.channel.below_recovery_watermark()
            {
                if route
                    .channel
                    .health()
                    .transition(SubscriberState::Pending, SubscriberState::Recovering)
                    && route
                        .channel
                        .health()
                        .transition(SubscriberState::Recovering, SubscriberState::Normal)
                {
                    self.shared.signal(FaultSignal {
                        kind: FaultKind::SubscriberRecovered,
                        subscriber_id: route.id,
                        sequence: 0,
                        detail: 0,
                    });
                }
            }
        }
        processed
    }

    fn remove_pending_reservation(&mut self, route: &mut SubscriberRoute) {
        let guarantee = route.pending_guarantee;
        let len = route.pending.len();
        self.global_pending -= len;
        self.reserved_pending_unused -= guarantee - len.min(guarantee);
        route.pending.clear();
        route.channel.health().set_pending_depth(0);
    }

    fn accept_source_sequence(&mut self, record: &EventRecord) -> bool {
        let sequence = record.header.source_sequence;
        if sequence == 0 {
            return true;
        }
        let last = &mut self.source_sequences[record.header.source_id as usize];
        if *last != 0 && sequence <= *last {
            self.shared
                .metrics
                .drop_total
                .fetch_add(1, Ordering::Relaxed);
            return false;
        }
        if *last != 0 && sequence != last.saturating_add(1) {
            self.shared
                .metrics
                .source_sequence_gap_total
                .fetch_add(1, Ordering::Relaxed);
            self.shared.signal(FaultSignal {
                kind: FaultKind::SourceSequenceGap,
                subscriber_id: 0,
                sequence,
                detail: record.header.source_id as u64,
            });
            self.shared
                .runtime_health
                .mark_source_gap(record.header.source_id, sequence);
        }
        *last = sequence;
        true
    }

    fn process_due_timers(&mut self, budget: DrainBudgetConfig) -> usize {
        let started = Instant::now();
        let mut processed = 0;
        while processed < budget.max_items
            && started.elapsed().as_nanos() < budget.max_elapsed_ns as u128
        {
            let now = self.shared.clock.now_ns();
            let Some(timer) = self.timers.peek().copied() else {
                break;
            };
            if timer.deadline_ns > now {
                break;
            }
            self.timers.pop();
            let lateness = now.saturating_sub(timer.deadline_ns);
            self.shared
                .metrics
                .timer_lateness_ns_max
                .fetch_max(lateness, Ordering::Relaxed);
            self.shared.metrics.observe_timer_lateness(lateness);
            if self
                .shared
                .timer_signals
                .push(TimerSignal {
                    timer_id: timer.timer_id,
                    deadline_ns: timer.deadline_ns,
                    fired_at_ns: now,
                })
                .is_err()
            {
                self.shared.signal(FaultSignal {
                    kind: FaultKind::TimerSignalFull,
                    subscriber_id: 0,
                    sequence: timer.timer_id,
                    detail: lateness,
                });
            }
            processed += 1;
        }
        processed
    }

    fn observe_service(&mut self, class: usize, processed: usize) {
        if processed == 0 {
            return;
        }
        let now = self.shared.clock.now_ns();
        let previous = std::mem::replace(&mut self.last_service_ns[class], now);
        if previous != 0 {
            self.shared
                .metrics
                .observe_service_gap(class, now.saturating_sub(previous));
        }
    }
}
