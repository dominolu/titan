use std::{
    collections::{BTreeSet, VecDeque},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread::Thread,
    time::{Duration, Instant},
};

use crossbeam_queue::ArrayQueue;
use titan_plugin_engine::{
    DispatchOutcome, ErrorKind, EventHandler, EventQos, EventReceiver, EventView, LifecycleState,
    PluginError, PluginIdentity, SubscriptionSpec,
};

use crate::{
    Delivery, EngineMetrics, FaultKind, FaultSignal, SubscriberHealth, SubscriberRuntimeMode,
    SubscriberState,
};
use crate::{TracePoint, TraceStage};

const ADMISSION_CLOSED: usize = 1_usize << (usize::BITS - 1);
const ADMISSION_COUNT_MASK: usize = ADMISSION_CLOSED - 1;

#[derive(Clone)]
pub(crate) struct EngineClock(Instant);

impl EngineClock {
    pub(crate) fn new() -> Self {
        Self(Instant::now())
    }

    pub(crate) fn now_ns(&self) -> u64 {
        self.0.elapsed().as_nanos().min(u64::MAX as u128) as u64
    }
}

pub(crate) struct TrackedDelivery {
    delivery: Delivery,
    health: Arc<SubscriberHealth>,
    clock: EngineClock,
}

impl TrackedDelivery {
    pub(crate) fn new(
        delivery: Delivery,
        health: Arc<SubscriberHealth>,
        clock: EngineClock,
    ) -> Self {
        health.on_admitted(delivery.header.local_sequence);
        health.on_enqueue(clock.now_ns());
        Self {
            delivery,
            health,
            clock,
        }
    }

    pub(crate) fn local_sequence(&self) -> u64 {
        self.delivery.header.local_sequence
    }

    pub(crate) fn ingress_at_ns(&self) -> u64 {
        self.delivery.ingress_at_ns
    }

    pub(crate) fn age_ns(&self) -> u64 {
        self.clock
            .now_ns()
            .saturating_sub(self.delivery.ingress_at_ns)
    }

    pub(crate) fn trace_point(
        &self,
        subscriber_id: u64,
        stage: TraceStage,
        timestamp_ns: u64,
    ) -> TracePoint {
        TracePoint {
            trace: self.delivery.header.trace,
            local_sequence: self.delivery.header.local_sequence,
            source_sequence: self.delivery.header.source_sequence,
            subscriber_id,
            stage,
            timestamp_ns,
        }
    }
}

impl Drop for TrackedDelivery {
    fn drop(&mut self) {
        self.health.on_release(self.clock.now_ns());
    }
}

pub struct EventLease {
    tracked: TrackedDelivery,
}

impl EventLease {
    pub fn event_type(&self) -> &str {
        &self.tracked.delivery.descriptor.event_type
    }

    pub fn schema_version(&self) -> u32 {
        self.tracked.delivery.descriptor.schema_version
    }

    pub fn header(&self) -> &crate::EventHeader {
        &self.tracked.delivery.header
    }

    pub fn payload(&self) -> &[u8] {
        self.tracked.delivery.payload.payload()
    }
}

pub(crate) struct SubscriberChannel {
    id: u64,
    owner: PluginIdentity,
    market_capacity: usize,
    high_watermark: usize,
    low_watermark: usize,
    queue: ArrayQueue<TrackedDelivery>,
    latest: ArrayQueue<TrackedDelivery>,
    health: Arc<SubscriberHealth>,
    admission: AtomicUsize,
    stop: AtomicBool,
    runtime_mode: SubscriberRuntimeMode,
    spin_iterations: usize,
    idle_sleep: Duration,
    cpu_affinity: Option<usize>,
    affinity_applied: AtomicBool,
    waiter: Mutex<Option<Thread>>,
    fault_signals: Arc<ArrayQueue<FaultSignal>>,
    trace_ring: Arc<ArrayQueue<TracePoint>>,
    metrics: Arc<EngineMetrics>,
}

pub(crate) struct SubscriberChannelArgs {
    pub id: u64,
    pub owner: PluginIdentity,
    pub capacity: usize,
    pub critical_reserve: usize,
    pub high_ratio: f64,
    pub low_ratio: f64,
    pub health: Arc<SubscriberHealth>,
    pub runtime_mode: SubscriberRuntimeMode,
    pub spin_iterations: usize,
    pub idle_sleep: Duration,
    pub cpu_affinity: Option<usize>,
    pub fault_signals: Arc<ArrayQueue<FaultSignal>>,
    pub trace_ring: Arc<ArrayQueue<TracePoint>>,
    pub metrics: Arc<EngineMetrics>,
}

impl SubscriberChannel {
    pub(crate) fn new(args: SubscriberChannelArgs) -> Arc<Self> {
        Arc::new(Self {
            id: args.id,
            owner: args.owner,
            market_capacity: args.capacity - args.critical_reserve,
            high_watermark: ((args.capacity as f64 * args.high_ratio).ceil() as usize)
                .clamp(1, args.capacity),
            low_watermark: ((args.capacity as f64 * args.low_ratio).floor() as usize)
                .min(args.capacity - 1),
            queue: ArrayQueue::new(args.capacity),
            latest: ArrayQueue::new(1),
            health: args.health,
            admission: AtomicUsize::new(0),
            stop: AtomicBool::new(false),
            runtime_mode: args.runtime_mode,
            spin_iterations: args.spin_iterations,
            idle_sleep: args.idle_sleep,
            cpu_affinity: args.cpu_affinity,
            affinity_applied: AtomicBool::new(false),
            waiter: Mutex::new(None),
            fault_signals: args.fault_signals,
            trace_ring: args.trace_ring,
            metrics: args.metrics,
        })
    }
}

impl EventReceiver for SubscriberChannel {
    fn dispatch_next(
        &self,
        handler: &dyn EventHandler,
        idle_wait: Duration,
    ) -> Result<DispatchOutcome, PluginError> {
        self.apply_affinity_once();
        if self.stop.load(Ordering::Acquire) || self.health.state() == SubscriberState::Failed {
            return Ok(DispatchOutcome::Closed);
        }
        let tracked = if let Some(tracked) = self.pop_next() {
            tracked
        } else {
            match self.runtime_mode {
                SubscriberRuntimeMode::Dedicated => {
                    std::hint::spin_loop();
                    return Ok(DispatchOutcome::Idle);
                }
                SubscriberRuntimeMode::SpinSleep => {
                    let mut value = None;
                    for _ in 0..self.spin_iterations {
                        std::hint::spin_loop();
                        if let Some(tracked) = self.pop_next() {
                            value = Some(tracked);
                            break;
                        }
                    }
                    let Some(tracked) = value else {
                        std::thread::sleep(idle_wait.max(self.idle_sleep));
                        return Ok(DispatchOutcome::Idle);
                    };
                    tracked
                }
                SubscriberRuntimeMode::Park => {
                    let current = std::thread::current();
                    *self.waiter.lock().unwrap_or_else(|p| p.into_inner()) = Some(current.clone());
                    let tracked = self.pop_next().or_else(|| {
                        std::thread::park_timeout(idle_wait.max(self.idle_sleep));
                        self.pop_next()
                    });
                    let mut waiter = self.waiter.lock().unwrap_or_else(|p| p.into_inner());
                    if waiter
                        .as_ref()
                        .is_some_and(|thread| thread.id() == current.id())
                    {
                        *waiter = None;
                    }
                    drop(waiter);
                    let Some(tracked) = tracked else {
                        return Ok(DispatchOutcome::Idle);
                    };
                    tracked
                }
            }
        };
        self.health
            .set_channel_depth(self.queue.len() + self.latest.len());
        self.recover_from_lagging_if_needed();
        self.metrics.observe_subscriber_latency(tracked.age_ns());
        let lease = EventLease { tracked };
        self.health.on_dispatched(lease.header().local_sequence);
        if self
            .trace_ring
            .push(TracePoint {
                trace: lease.header().trace,
                local_sequence: lease.header().local_sequence,
                source_sequence: lease.header().source_sequence,
                subscriber_id: self.id,
                stage: TraceStage::SubscriberReceived,
                timestamp_ns: lease.tracked.clock.now_ns(),
            })
            .is_err()
        {
            self.metrics
                .trace_ring_drop_total
                .fetch_add(1, Ordering::Relaxed);
        }
        let result = catch_unwind(AssertUnwindSafe(|| {
            handler.handle(EventView {
                event_type: lease.event_type(),
                schema_version: lease.schema_version(),
                payload: lease.payload(),
                trace: lease.header().trace,
            })
        }));
        match result {
            Ok(Ok(())) => {
                self.health.on_committed(lease.header().local_sequence);
                Ok(DispatchOutcome::Delivered)
            }
            Ok(Err(_)) | Err(_) => {
                self.close_admission_and_wait();
                self.health.set_state(SubscriberState::Failed);
                self.health.record_gap(lease.header().local_sequence);
                self.metrics
                    .delivery_gap_total
                    .fetch_add(1, Ordering::Relaxed);
                if self
                    .fault_signals
                    .push(FaultSignal {
                        kind: FaultKind::SubscriberFailed,
                        subscriber_id: self.id,
                        sequence: lease.header().local_sequence,
                        detail: 0,
                    })
                    .is_err()
                {
                    self.metrics
                        .fault_signal_drop_total
                        .fetch_add(1, Ordering::Relaxed);
                }
                self.clear_queued_as_gap();
                self.stop.store(true, Ordering::Release);
                Err(PluginError::new(
                    ErrorKind::PluginFailed,
                    self.owner.clone(),
                    LifecycleState::Running,
                    "event_handler",
                    "subscriber handler failed or panicked",
                ))
            }
        }
    }
}

impl SubscriberChannel {
    fn pop_next(&self) -> Option<TrackedDelivery> {
        self.queue.pop().or_else(|| self.latest.pop())
    }

    fn apply_affinity_once(&self) {
        if self
            .affinity_applied
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
            && let Some(core_id) = self.cpu_affinity
        {
            let _ = core_affinity::set_for_current(core_affinity::CoreId { id: core_id });
        }
    }

    fn wake_waiter(&self) {
        if self.runtime_mode == SubscriberRuntimeMode::Park
            && let Some(waiter) = self
                .waiter
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .as_ref()
        {
            waiter.unpark();
        }
    }

    pub(crate) fn try_push_critical(
        &self,
        delivery: TrackedDelivery,
    ) -> Result<(), TrackedDelivery> {
        if !self.begin_push() {
            return Err(delivery);
        }
        if !self.latest.is_empty() {
            self.end_push();
            return Err(delivery);
        }
        let result = self.queue.push(delivery);
        self.end_push();
        if result.is_ok() {
            if !self.can_accept() {
                self.clear_queued_as_gap();
                return result;
            }
            self.after_enqueue();
        }
        result
    }

    pub(crate) fn try_push_market(&self, delivery: TrackedDelivery) -> Result<(), TrackedDelivery> {
        if !self.begin_push() {
            return Err(delivery);
        }
        if self.queue.len() >= self.market_capacity {
            self.end_push();
            return Err(delivery);
        }
        let result = self.queue.push(delivery);
        self.end_push();
        if result.is_ok() {
            if !self.can_accept() {
                self.clear_queued_as_gap();
                return result;
            }
            self.after_enqueue();
        }
        result
    }

    pub(crate) fn replace_latest(&self, mut delivery: TrackedDelivery) -> bool {
        if !self.begin_push() {
            return false;
        }
        let mut replaced = false;
        loop {
            match self.latest.push(delivery) {
                Ok(()) => {
                    self.end_push();
                    if !self.can_accept() {
                        self.clear_queued_as_gap();
                        return replaced;
                    }
                    self.health
                        .set_channel_depth(self.queue.len() + self.latest.len());
                    self.wake_waiter();
                    return replaced;
                }
                Err(returned) => {
                    delivery = returned;
                    replaced |= self.latest.pop().is_some();
                }
            }
        }
    }

    fn after_enqueue(&self) {
        self.health
            .set_channel_depth(self.queue.len() + self.latest.len());
        self.wake_waiter();
        if self.queue.len() >= self.high_watermark
            && self
                .health
                .transition(SubscriberState::Normal, SubscriberState::Lagging)
        {
            if self
                .fault_signals
                .push(FaultSignal {
                    kind: FaultKind::SubscriberLagging,
                    subscriber_id: self.id,
                    sequence: 0,
                    detail: self.queue.len() as u64,
                })
                .is_err()
            {
                self.metrics
                    .fault_signal_drop_total
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub(crate) fn below_recovery_watermark(&self) -> bool {
        self.queue.len() + self.latest.len() <= self.low_watermark
    }

    pub(crate) fn clear_queued(&self) {
        while self.queue.pop().is_some() {}
        while self.latest.pop().is_some() {}
        self.health.set_channel_depth(0);
    }

    pub(crate) fn clear_queued_as_gap(&self) {
        while let Some(delivery) = self.queue.pop() {
            self.health.record_gap(delivery.local_sequence());
        }
        while let Some(delivery) = self.latest.pop() {
            self.health.record_gap(delivery.local_sequence());
        }
        self.health.set_channel_depth(0);
    }

    pub(crate) fn request_stop(&self) {
        self.close_admission_and_wait();
        self.stop.store(true, Ordering::Release);
        self.wake_waiter();
    }

    pub(crate) fn suspend(&self) {
        self.close_admission_and_wait();
    }

    pub(crate) fn resume(&self) {
        if !self.stop.load(Ordering::Acquire) && !self.health.state().is_terminal() {
            debug_assert_eq!(self.admission.load(Ordering::Acquire), ADMISSION_CLOSED);
            self.admission.store(0, Ordering::Release);
        }
    }

    fn can_accept(&self) -> bool {
        self.admission.load(Ordering::Acquire) & ADMISSION_CLOSED == 0
            && !self.stop.load(Ordering::Acquire)
            && !matches!(
                self.health.state(),
                SubscriberState::ResyncRequired
                    | SubscriberState::Failed
                    | SubscriberState::Stopped
            )
    }

    fn begin_push(&self) -> bool {
        let mut current = self.admission.load(Ordering::Acquire);
        loop {
            if current & ADMISSION_CLOSED != 0
                || self.stop.load(Ordering::Acquire)
                || matches!(
                    self.health.state(),
                    SubscriberState::ResyncRequired
                        | SubscriberState::Failed
                        | SubscriberState::Stopped
                )
            {
                return false;
            }
            assert!(
                current & ADMISSION_COUNT_MASK < ADMISSION_COUNT_MASK,
                "subscriber admission counter overflow"
            );
            match self.admission.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    fn end_push(&self) {
        let previous = self.admission.fetch_sub(1, Ordering::Release);
        assert!(previous & ADMISSION_COUNT_MASK > 0);
    }

    fn close_admission_and_wait(&self) {
        self.admission.fetch_or(ADMISSION_CLOSED, Ordering::AcqRel);
        while self.admission.load(Ordering::Acquire) & ADMISSION_COUNT_MASK != 0 {
            std::hint::spin_loop();
        }
    }

    fn recover_from_lagging_if_needed(&self) {
        if self.queue.len() + self.latest.len() <= self.low_watermark
            && self
                .health
                .transition(SubscriberState::Lagging, SubscriberState::Recovering)
        {
            if !self
                .health
                .transition(SubscriberState::Recovering, SubscriberState::Normal)
            {
                return;
            }
            if self
                .fault_signals
                .push(FaultSignal {
                    kind: FaultKind::SubscriberRecovered,
                    subscriber_id: self.id,
                    sequence: 0,
                    detail: 0,
                })
                .is_err()
            {
                self.metrics
                    .fault_signal_drop_total
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub(crate) fn can_consume(&self) -> bool {
        !self.stop.load(Ordering::Acquire)
    }

    pub(crate) fn stop_and_drain(&self) {
        self.request_stop();
        self.drain_and_release();
        if self.health.state() != SubscriberState::Failed {
            self.health.set_state(SubscriberState::Stopped);
        }
    }

    fn drain_and_release(&self) {
        self.clear_queued();
    }

    pub(crate) fn health(&self) -> &Arc<SubscriberHealth> {
        &self.health
    }
}

pub(crate) struct SubscriberRoute {
    pub id: u64,
    pub event_type_id: u32,
    pub routing_keys: BTreeSet<u64>,
    pub qos: EventQos,
    pub channel: Arc<SubscriberChannel>,
    pub pending: VecDeque<PendingEntry>,
    pub pending_capacity: usize,
    pub pending_guarantee: usize,
    pub pending_high_watermark: usize,
}

pub(crate) struct PendingEntry {
    pub delivery: TrackedDelivery,
    pub enqueued_at_ns: u64,
}

impl SubscriberRoute {
    pub(crate) fn new(
        id: u64,
        event_type_id: u32,
        spec: &SubscriptionSpec,
        channel: Arc<SubscriberChannel>,
        pending_capacity: usize,
        pending_guarantee: usize,
        pending_high_watermark: usize,
    ) -> Self {
        Self {
            id,
            event_type_id,
            routing_keys: spec.routing_keys.iter().copied().collect(),
            qos: spec.qos,
            channel,
            pending: VecDeque::with_capacity(pending_capacity),
            pending_capacity,
            pending_guarantee,
            pending_high_watermark,
        }
    }

    pub(crate) fn matches_routing_key(&self, key: u64) -> bool {
        self.routing_keys.is_empty() || self.routing_keys.contains(&key)
    }
}
