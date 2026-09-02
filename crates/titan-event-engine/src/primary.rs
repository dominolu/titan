use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{Receiver, SyncSender, sync_channel},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use crossbeam_queue::ArrayQueue;
use titan_plugin_engine::{EventHandler, EventQos, EventView};

use crate::{
    EngineError, EventDescriptor, EventHeader, FaultKind, FaultSignal, OwnedEvent, PoolKind,
    PublishError, PublishRequest, SubscriberHealth, SubscriberHealthSnapshot,
    SubscriberRuntimeMode, SubscriberState,
};

/// Per-event routing declaration for a PRIMARY asynchronous lane.
#[derive(Clone, Debug)]
pub struct PrimarySubscriptionSpec {
    pub event_type: Arc<str>,
    pub schema_version: u32,
    pub qos: EventQos,
    pub routing_keys: Arc<[u64]>,
}

#[derive(Clone, Debug)]
pub struct PrimaryAsyncLaneConfig {
    pub capacity: usize,
    pub critical_reserve: usize,
    pub reliable_pending_capacity: usize,
    pub snapshot_staging_capacity: usize,
    pub control_capacity: usize,
    pub runtime_mode: SubscriberRuntimeMode,
    pub spin_iterations: usize,
    pub idle_sleep: Duration,
    pub cpu_affinity: Option<usize>,
}

impl Default for PrimaryAsyncLaneConfig {
    fn default() -> Self {
        Self {
            capacity: 16_384,
            critical_reserve: 2_048,
            reliable_pending_capacity: 1_024,
            snapshot_staging_capacity: 4_096,
            control_capacity: 64,
            runtime_mode: SubscriberRuntimeMode::SpinSleep,
            spin_iterations: 256,
            idle_sleep: Duration::from_micros(10),
            cpu_affinity: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PrimaryLaneToken(pub u64);

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LaneProgress {
    pub admitted_sequence: u64,
    pub dispatched_sequence: u64,
    pub committed_sequence: u64,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SnapshotBarrierId(pub u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamBoundary {
    pub source_id: u32,
    pub stream_epoch: u64,
    pub source_sequence: u64,
}

#[derive(Clone, Debug)]
pub struct SnapshotBarrierRequest {
    pub source_ids: Arc<[u32]>,
    pub deadline: Instant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotBarrierState {
    Staging,
    Replaying,
    Completed,
    Failed,
}

#[derive(Clone, Debug)]
pub struct SnapshotBarrierSnapshot {
    pub id: SnapshotBarrierId,
    pub state: SnapshotBarrierState,
    pub staged_events: usize,
    pub replay_committed_sequence: u64,
}

type SafePointAction = Box<dyn FnOnce() -> Result<(), EngineError> + Send + 'static>;

struct ControlItem {
    action: SafePointAction,
    reply: SyncSender<Result<(), EngineError>>,
}

pub struct SafePointTicket {
    reply: Receiver<Result<(), EngineError>>,
}

impl SafePointTicket {
    pub fn wait(self, deadline: Instant) -> Result<(), EngineError> {
        let timeout = deadline.saturating_duration_since(Instant::now());
        self.reply
            .recv_timeout(timeout)
            .map_err(|_| EngineError::ControlTimeout)?
    }
}

struct PrimaryEvent {
    descriptor: Arc<EventDescriptor>,
    header: EventHeader,
    payload: OwnedEvent,
    admitted_sequence: u64,
    health: Arc<SubscriberHealth>,
}

impl Drop for PrimaryEvent {
    fn drop(&mut self) {
        self.health.on_release(0);
    }
}

struct Barrier {
    id: SnapshotBarrierId,
    sources: BTreeSet<u32>,
    deadline: Instant,
    state: SnapshotBarrierState,
    staged: VecDeque<PrimaryEvent>,
    snapshots: VecDeque<PrimaryEvent>,
    replay_committed_sequence: u64,
}

pub(crate) struct PrimaryAsyncLane {
    token: u64,
    routes: BTreeMap<u32, (EventQos, Arc<[u64]>)>,
    latest_keys: BTreeSet<(u32, u64)>,
    admission: Mutex<()>,
    capacity: usize,
    critical_reserve: usize,
    queue: ArrayQueue<PrimaryEvent>,
    latest: Mutex<BTreeMap<(u32, u64), PrimaryEvent>>,
    pending: ArrayQueue<PrimaryEvent>,
    control: ArrayQueue<ControlItem>,
    staging_capacity: usize,
    barrier: Mutex<Option<Barrier>>,
    next_barrier: AtomicU64,
    next_admitted: AtomicU64,
    health: Arc<SubscriberHealth>,
    handler: Arc<dyn EventHandler>,
    accepting: AtomicBool,
    stopping: AtomicBool,
    runtime_mode: SubscriberRuntimeMode,
    spin_iterations: usize,
    idle_sleep: Duration,
    cpu_affinity: Option<usize>,
    worker: OnceLock<thread::Thread>,
    join: Mutex<Option<JoinHandle<()>>>,
    fault_signals: Arc<ArrayQueue<FaultSignal>>,
}

impl PrimaryAsyncLane {
    pub(crate) fn start(
        token: u64,
        subscriptions: &[(u32, PrimarySubscriptionSpec)],
        config: PrimaryAsyncLaneConfig,
        handler: Arc<dyn EventHandler>,
        fault_signals: Arc<ArrayQueue<FaultSignal>>,
    ) -> Result<Arc<Self>, EngineError> {
        if config.capacity == 0
            || config.critical_reserve >= config.capacity
            || config.reliable_pending_capacity == 0
            || config.snapshot_staging_capacity == 0
            || config.control_capacity == 0
            || config.idle_sleep.is_zero()
            || (config.runtime_mode == SubscriberRuntimeMode::Dedicated
                && config.cpu_affinity.is_none())
        {
            return Err(EngineError::InvalidPrimaryLaneConfig);
        }
        let routes = subscriptions
            .iter()
            .map(|(descriptor, spec)| (*descriptor, (spec.qos, spec.routing_keys.clone())))
            .collect();
        let mut latest_keys = BTreeSet::new();
        for (descriptor, spec) in subscriptions {
            if spec.qos == EventQos::Latest {
                if spec.routing_keys.is_empty() {
                    return Err(EngineError::InvalidPrimaryLaneConfig);
                }
                latest_keys.extend(
                    spec.routing_keys
                        .iter()
                        .map(|routing_key| (*descriptor, *routing_key)),
                );
            }
        }
        let health = Arc::new(SubscriberHealth::default());
        let lane = Arc::new(Self {
            token,
            routes,
            latest_keys,
            admission: Mutex::new(()),
            capacity: config.capacity,
            critical_reserve: config.critical_reserve,
            queue: ArrayQueue::new(config.capacity),
            latest: Mutex::new(BTreeMap::new()),
            pending: ArrayQueue::new(config.reliable_pending_capacity),
            control: ArrayQueue::new(config.control_capacity),
            staging_capacity: config.snapshot_staging_capacity,
            barrier: Mutex::new(None),
            next_barrier: AtomicU64::new(1),
            next_admitted: AtomicU64::new(1),
            health,
            handler,
            accepting: AtomicBool::new(true),
            stopping: AtomicBool::new(false),
            runtime_mode: config.runtime_mode,
            spin_iterations: config.spin_iterations,
            idle_sleep: config.idle_sleep,
            cpu_affinity: config.cpu_affinity,
            worker: OnceLock::new(),
            join: Mutex::new(None),
            fault_signals,
        });
        let worker_lane = lane.clone();
        let join = thread::Builder::new()
            .name(format!("event-primary-lane-{token}"))
            .spawn(move || worker_lane.run())
            .map_err(|error| EngineError::SubscriberRuntime(error.to_string()))?;
        *lane.join.lock().unwrap_or_else(|p| p.into_inner()) = Some(join);
        Ok(lane)
    }

    pub(crate) fn matches(&self, descriptor_id: u32, routing_key: u64) -> bool {
        self.routes
            .get(&descriptor_id)
            .is_some_and(|(_, keys)| keys.is_empty() || keys.binary_search(&routing_key).is_ok())
    }

    pub(crate) fn enqueue(
        &self,
        descriptor: Arc<EventDescriptor>,
        header: EventHeader,
        payload: OwnedEvent,
    ) {
        if !self.accepting.load(Ordering::Acquire) || self.stopping.load(Ordering::Acquire) {
            return;
        }
        let Some((qos, _)) = self.routes.get(&descriptor.id) else {
            return;
        };
        let _admission = self.admission.lock().unwrap_or_else(|p| p.into_inner());
        let Ok(mut barrier_guard) = self.barrier.try_lock() else {
            self.invalidate(0, FaultKind::SubscriberBackpressure);
            return;
        };
        if let Some(barrier) = barrier_guard.as_mut()
            && barrier.state == SnapshotBarrierState::Staging
            && barrier.sources.contains(&header.source_id)
        {
            if Instant::now() >= barrier.deadline
                || barrier.staged.len() + barrier.snapshots.len() >= self.staging_capacity
            {
                barrier.state = SnapshotBarrierState::Failed;
                drop(barrier_guard);
                self.invalidate(header.source_sequence, FaultKind::PendingFull);
                return;
            }
            self.health.on_enqueue(0);
            barrier.staged.push_back(PrimaryEvent {
                descriptor,
                header,
                payload,
                admitted_sequence: 0,
                health: self.health.clone(),
            });
            return;
        }
        drop(barrier_guard);

        let market_limit = self.capacity - self.critical_reserve;
        let direct_allowed = self.pending.is_empty()
            && (*qos == EventQos::ReliableOrdered || self.queue.len() < market_limit);
        if direct_allowed && self.queue.len() < self.capacity {
            let event = self.make_admitted_event(descriptor, header, payload);
            if self.queue.push(event).is_err() {
                unreachable!("serialized admission reserved queue capacity");
            }
            self.after_enqueue();
            return;
        }
        match qos {
            EventQos::ReliableOrdered => {
                if self.pending.len() >= self.pending.capacity() {
                    self.invalidate(header.source_sequence, FaultKind::PendingFull);
                } else {
                    let event = self.make_admitted_event(descriptor, header, payload);
                    if self.pending.push(event).is_err() {
                        unreachable!("serialized admission reserved pending capacity");
                    }
                    self.health.set_pending_depth(self.pending.len());
                    self.health.transition_nonterminal(SubscriberState::Pending);
                    self.wake();
                }
            }
            EventQos::Latest => {
                let key = (descriptor.id, header.routing_key);
                if !self.latest_keys.contains(&key) {
                    self.invalidate(header.source_sequence, FaultKind::SubscriberBackpressure);
                    return;
                }
                let event = self.make_admitted_event(descriptor, header, payload);
                self.latest
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .insert(key, event);
                self.after_enqueue();
            }
            EventQos::BestEffort => {}
        }
    }

    fn make_admitted_event(
        &self,
        descriptor: Arc<EventDescriptor>,
        header: EventHeader,
        payload: OwnedEvent,
    ) -> PrimaryEvent {
        let sequence = self.next_admitted.fetch_add(1, Ordering::AcqRel);
        self.health.on_admitted(sequence);
        self.health.on_enqueue(0);
        PrimaryEvent {
            descriptor,
            header,
            payload,
            admitted_sequence: sequence,
            health: self.health.clone(),
        }
    }

    fn after_enqueue(&self) {
        self.health.set_channel_depth(
            self.queue.len() + self.latest.lock().unwrap_or_else(|p| p.into_inner()).len(),
        );
        self.wake();
    }

    fn invalidate(&self, source_sequence: u64, kind: FaultKind) {
        self.accepting.store(false, Ordering::Release);
        self.health.record_gap(source_sequence.max(1));
        self.health.set_state(SubscriberState::ResyncRequired);
        let _ = self.fault_signals.push(FaultSignal {
            kind,
            subscriber_id: self.token,
            sequence: source_sequence,
            detail: 0,
        });
        self.wake();
    }

    fn pop_event(&self) -> Option<PrimaryEvent> {
        let event = self.queue.pop().or_else(|| self.pending.pop()).or_else(|| {
            let mut latest = self.latest.lock().unwrap_or_else(|p| p.into_inner());
            let key = latest
                .iter()
                .min_by_key(|(_, event)| event.admitted_sequence)
                .map(|(key, _)| *key)?;
            latest.remove(&key)
        });
        self.health.set_pending_depth(self.pending.len());
        self.health.set_channel_depth(
            self.queue.len() + self.latest.lock().unwrap_or_else(|p| p.into_inner()).len(),
        );
        event
    }

    fn run(self: Arc<Self>) {
        let _ = self.worker.set(thread::current());
        if let Some(core_id) = self.cpu_affinity {
            let _ = core_affinity::set_for_current(core_affinity::CoreId { id: core_id });
        }
        loop {
            while let Some(control) = self.control.pop() {
                let result = catch_unwind(AssertUnwindSafe(control.action))
                    .map_err(|_| EngineError::SafePointPanicked)
                    .and_then(|result| result);
                let _ = control.reply.try_send(result);
            }
            if let Some(event) = self.pop_event() {
                self.health.on_dispatched(event.admitted_sequence);
                let result = catch_unwind(AssertUnwindSafe(|| {
                    self.handler.handle(EventView {
                        event_type: event.descriptor.event_type.as_ref(),
                        schema_version: event.descriptor.schema_version,
                        payload: event.payload.payload(),
                        trace: event.header.trace,
                    })
                }));
                if matches!(result, Ok(Ok(()))) {
                    self.health.on_committed(event.admitted_sequence);
                    if self.pending.is_empty() && self.health.state() == SubscriberState::Pending {
                        self.health.set_state(SubscriberState::Normal);
                    }
                } else {
                    self.health.record_gap(event.admitted_sequence);
                    self.health.set_state(SubscriberState::Failed);
                    self.accepting.store(false, Ordering::Release);
                    let _ = self.fault_signals.push(FaultSignal {
                        kind: FaultKind::SubscriberFailed,
                        subscriber_id: self.token,
                        sequence: event.admitted_sequence,
                        detail: event.descriptor.id as u64,
                    });
                }
                continue;
            }
            if self.stopping.load(Ordering::Acquire) {
                break;
            }
            match self.runtime_mode {
                SubscriberRuntimeMode::Dedicated => std::hint::spin_loop(),
                SubscriberRuntimeMode::SpinSleep => {
                    let mut ready = false;
                    for _ in 0..self.spin_iterations {
                        std::hint::spin_loop();
                        if !self.queue.is_empty()
                            || !self.pending.is_empty()
                            || !self
                                .latest
                                .lock()
                                .unwrap_or_else(|p| p.into_inner())
                                .is_empty()
                            || !self.control.is_empty()
                        {
                            ready = true;
                            break;
                        }
                    }
                    if !ready {
                        thread::park_timeout(self.idle_sleep);
                    }
                }
                SubscriberRuntimeMode::Park => thread::park_timeout(self.idle_sleep),
            }
        }
    }

    fn wake(&self) {
        if let Some(worker) = self.worker.get() {
            worker.unpark();
        }
    }

    pub(crate) fn stop_and_join(&self) {
        let _ = self.stop_and_join_until(Instant::now() + Duration::from_secs(30));
    }

    pub(crate) fn stop_and_join_until(&self, deadline: Instant) -> Result<(), EngineError> {
        self.accepting.store(false, Ordering::Release);
        self.stopping.store(true, Ordering::Release);
        self.wake();
        let mut join_slot = self.join.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(join) = join_slot.as_ref() {
            while !join.is_finished() {
                if Instant::now() >= deadline {
                    return Err(EngineError::PrimaryLaneStopTimeout(self.token));
                }
                thread::yield_now();
            }
        }
        if let Some(join) = join_slot.take() {
            let _ = join.join();
        }
        drop(join_slot);
        while self.queue.pop().is_some() {}
        while self.pending.pop().is_some() {}
        self.latest
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clear();
        while let Some(control) = self.control.pop() {
            let _ = control.reply.try_send(Err(EngineError::NotRunning));
        }
        self.health.set_channel_depth(0);
        self.health.set_pending_depth(0);
        if self.health.state() != SubscriberState::Failed {
            self.health.set_state(SubscriberState::Stopped);
        }
        Ok(())
    }

    fn submit_control(&self, action: SafePointAction) -> Result<SafePointTicket, EngineError> {
        if self.stopping.load(Ordering::Acquire) {
            return Err(EngineError::NotRunning);
        }
        let (reply, receiver) = sync_channel(1);
        self.control
            .push(ControlItem { action, reply })
            .map_err(|_| EngineError::ControlQueueFull)?;
        self.wake();
        Ok(SafePointTicket { reply: receiver })
    }

    fn begin_barrier(
        &self,
        request: SnapshotBarrierRequest,
    ) -> Result<SnapshotBarrierId, EngineError> {
        let _admission = self.admission.lock().unwrap_or_else(|p| p.into_inner());
        if request.source_ids.is_empty() || request.deadline <= Instant::now() {
            return Err(EngineError::InvalidSnapshotBarrier);
        }
        if !self.queue.is_empty()
            || !self.pending.is_empty()
            || !self
                .latest
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .is_empty()
            || self.health.dispatched_sequence() != self.health.committed_sequence()
        {
            return Err(EngineError::RecoveryNotQuiescent(self.token));
        }
        let mut guard = self.barrier.lock().unwrap_or_else(|p| p.into_inner());
        if guard.is_some() {
            return Err(EngineError::SnapshotBarrierActive);
        }
        let id = SnapshotBarrierId(self.next_barrier.fetch_add(1, Ordering::AcqRel));
        *guard = Some(Barrier {
            id,
            sources: request.source_ids.iter().copied().collect(),
            deadline: request.deadline,
            state: SnapshotBarrierState::Staging,
            staged: VecDeque::with_capacity(self.staging_capacity),
            snapshots: VecDeque::new(),
            replay_committed_sequence: 0,
        });
        self.health.set_state(SubscriberState::Recovering);
        self.accepting.store(true, Ordering::Release);
        Ok(id)
    }

    fn add_snapshot(&self, id: SnapshotBarrierId, event: PrimaryEvent) -> Result<(), EngineError> {
        let mut guard = self.barrier.lock().unwrap_or_else(|p| p.into_inner());
        let barrier = guard
            .as_mut()
            .ok_or(EngineError::UnknownSnapshotBarrier(id.0))?;
        if barrier.id != id || barrier.state != SnapshotBarrierState::Staging {
            return Err(EngineError::UnknownSnapshotBarrier(id.0));
        }
        if barrier.snapshots.len() + barrier.staged.len() >= self.staging_capacity {
            barrier.state = SnapshotBarrierState::Failed;
            return Err(EngineError::SnapshotStagingFull);
        }
        barrier.snapshots.push_back(event);
        Ok(())
    }

    fn provider_complete(
        &self,
        id: SnapshotBarrierId,
        boundaries: &[StreamBoundary],
    ) -> Result<u64, EngineError> {
        let _admission = self.admission.lock().unwrap_or_else(|p| p.into_inner());
        let mut guard = self.barrier.lock().unwrap_or_else(|p| p.into_inner());
        let barrier = guard
            .as_mut()
            .ok_or(EngineError::UnknownSnapshotBarrier(id.0))?;
        if barrier.id != id || barrier.state != SnapshotBarrierState::Staging {
            return Err(EngineError::UnknownSnapshotBarrier(id.0));
        }
        let by_source = boundaries
            .iter()
            .map(|boundary| {
                (
                    boundary.source_id,
                    (boundary.stream_epoch, boundary.source_sequence),
                )
            })
            .collect::<BTreeMap<_, _>>();
        if by_source.len() != boundaries.len()
            || by_source.len() != barrier.sources.len()
            || barrier
                .sources
                .iter()
                .any(|source| !by_source.contains_key(source))
        {
            barrier.state = SnapshotBarrierState::Failed;
            return Err(EngineError::SnapshotBoundaryMissing);
        }
        let mut replay = std::mem::take(&mut barrier.snapshots);
        replay.extend(barrier.staged.drain(..).filter(|event| {
            let (boundary_epoch, boundary_sequence) = by_source[&event.header.source_id];
            let event_epoch = event_stream_epoch(event).unwrap_or(boundary_epoch);
            event_epoch > boundary_epoch
                || (event_epoch == boundary_epoch
                    && event.header.source_sequence > boundary_sequence)
        }));
        if replay.len() + self.queue.len() > self.capacity {
            barrier.state = SnapshotBarrierState::Failed;
            return Err(EngineError::SnapshotStagingFull);
        }
        let mut last = self.health.committed_sequence();
        for mut event in replay {
            if event.admitted_sequence == 0 {
                event.admitted_sequence = self.next_admitted.fetch_add(1, Ordering::AcqRel);
                self.health.on_admitted(event.admitted_sequence);
            }
            last = last.max(event.admitted_sequence);
            self.queue
                .push(event)
                .map_err(|_| EngineError::SnapshotStagingFull)?;
        }
        barrier.replay_committed_sequence = last;
        barrier.state = SnapshotBarrierState::Replaying;
        self.after_enqueue();
        Ok(last)
    }

    fn complete_barrier(
        &self,
        id: SnapshotBarrierId,
        committed_sequence: u64,
    ) -> Result<(), EngineError> {
        let _admission = self.admission.lock().unwrap_or_else(|p| p.into_inner());
        let mut guard = self.barrier.lock().unwrap_or_else(|p| p.into_inner());
        let barrier = guard
            .as_mut()
            .ok_or(EngineError::UnknownSnapshotBarrier(id.0))?;
        if barrier.id != id || barrier.state != SnapshotBarrierState::Replaying {
            return Err(EngineError::UnknownSnapshotBarrier(id.0));
        }
        if committed_sequence < barrier.replay_committed_sequence
            || self.health.committed_sequence() < barrier.replay_committed_sequence
        {
            return Err(EngineError::SnapshotReplayNotCommitted);
        }
        barrier.state = SnapshotBarrierState::Completed;
        self.health.finish_recovery();
        self.accepting.store(true, Ordering::Release);
        *guard = None;
        Ok(())
    }

    fn abort_barrier(&self, id: SnapshotBarrierId) -> Result<(), EngineError> {
        let _admission = self.admission.lock().unwrap_or_else(|p| p.into_inner());
        let mut guard = self.barrier.lock().unwrap_or_else(|p| p.into_inner());
        let barrier = guard
            .as_ref()
            .ok_or(EngineError::UnknownSnapshotBarrier(id.0))?;
        if barrier.id != id {
            return Err(EngineError::UnknownSnapshotBarrier(id.0));
        }
        let sequence = barrier.replay_committed_sequence;
        *guard = None;
        drop(guard);
        self.invalidate(sequence, FaultKind::SnapshotRecoveryAborted);
        Ok(())
    }

    fn barrier_snapshot(&self) -> Option<SnapshotBarrierSnapshot> {
        self.barrier
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .map(|barrier| SnapshotBarrierSnapshot {
                id: barrier.id,
                state: barrier.state,
                staged_events: barrier.staged.len() + barrier.snapshots.len(),
                replay_committed_sequence: barrier.replay_committed_sequence,
            })
    }
}

fn event_stream_epoch(event: &PrimaryEvent) -> Option<u64> {
    matches!(
        event.descriptor.event_type.as_ref(),
        "titan.market.DepthBatch" | "titan.market.TradeBatch" | "titan.market.Bbo"
    )
    .then(|| {
        let payload = event.payload.payload();
        payload
            .get(12..20)
            .map(|bytes| u64::from_le_bytes(bytes.try_into().expect("slice length was checked")))
    })
    .flatten()
}

/// Handle to one EventEngine-owned PRIMARY lane and its isolated worker.
#[derive(Clone)]
pub struct PrimaryAsyncLaneHandle {
    pub(crate) lane: Arc<PrimaryAsyncLane>,
    pub(crate) shared: Arc<crate::engine::EngineShared>,
}

impl PrimaryAsyncLaneHandle {
    pub fn token(&self) -> PrimaryLaneToken {
        PrimaryLaneToken(self.lane.token)
    }

    pub fn health(&self) -> SubscriberHealthSnapshot {
        self.lane.health.snapshot()
    }

    pub fn progress(&self) -> LaneProgress {
        LaneProgress {
            admitted_sequence: self.lane.health.admitted_sequence(),
            dispatched_sequence: self.lane.health.dispatched_sequence(),
            committed_sequence: self.lane.health.committed_sequence(),
        }
    }

    pub fn submit_safe_point<F>(&self, action: F) -> Result<SafePointTicket, EngineError>
    where
        F: FnOnce() -> Result<(), EngineError> + Send + 'static,
    {
        self.lane.submit_control(Box::new(action))
    }

    pub fn begin_snapshot_barrier(
        &self,
        request: SnapshotBarrierRequest,
    ) -> Result<SnapshotBarrierId, EngineError> {
        self.lane.begin_barrier(request)
    }

    pub fn publish_snapshot_fact(
        &self,
        id: SnapshotBarrierId,
        request: PublishRequest<'_>,
    ) -> Result<(), EngineError> {
        let _admission = self
            .lane
            .admission
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let descriptor = self
            .shared
            .descriptor(request.event_type, request.schema_version)
            .ok_or(EngineError::InvalidEvent)?;
        if !self.lane.matches(descriptor.id, request.routing_key) {
            return Err(EngineError::InvalidEvent);
        }
        {
            let guard = self.lane.barrier.lock().unwrap_or_else(|p| p.into_inner());
            let barrier = guard
                .as_ref()
                .ok_or(EngineError::UnknownSnapshotBarrier(id.0))?;
            if barrier.id != id || !barrier.sources.contains(&request.source_id) {
                return Err(EngineError::InvalidSnapshotBarrier);
            }
        }
        let mut reservation = self
            .shared
            .arena
            .reserve(PoolKind::Snapshot, request.payload.len())
            .map_err(|error| match error {
                PublishError::PayloadTooLarge { .. } | PublishError::EventArenaExhausted(_) => {
                    EngineError::SnapshotStagingFull
                }
                _ => EngineError::InvalidEvent,
            })?;
        reservation.payload_mut().copy_from_slice(request.payload);
        let payload = reservation.commit();
        self.lane.health.on_enqueue(0);
        self.lane.add_snapshot(
            id,
            PrimaryEvent {
                descriptor,
                header: EventHeader {
                    source_id: request.source_id,
                    event_type_id: 0,
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
                admitted_sequence: 0,
                health: self.lane.health.clone(),
            },
        )
    }

    pub fn snapshot_provider_complete(
        &self,
        id: SnapshotBarrierId,
        boundaries: &[StreamBoundary],
    ) -> Result<u64, EngineError> {
        self.lane.provider_complete(id, boundaries)
    }

    pub fn complete_snapshot_barrier(
        &self,
        id: SnapshotBarrierId,
        committed_sequence: u64,
    ) -> Result<(), EngineError> {
        self.lane.complete_barrier(id, committed_sequence)
    }

    pub fn abort_snapshot_barrier(&self, id: SnapshotBarrierId) -> Result<(), EngineError> {
        self.lane.abort_barrier(id)
    }

    pub fn snapshot_barrier(&self) -> Option<SnapshotBarrierSnapshot> {
        self.lane.barrier_snapshot()
    }
}
