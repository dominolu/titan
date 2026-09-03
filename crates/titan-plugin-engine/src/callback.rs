use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering},
        mpsc::{SyncSender, sync_channel},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use crate::{CallbackBudget, TraceContext};

#[derive(Clone, Debug)]
pub struct CallbackStats {
    pub total: u64,
    pub budget_exceeded: u64,
    pub consecutive_violations: u32,
    pub last_duration: Duration,
    pub running_since: Option<Instant>,
    pub stalled: bool,
    pub violation_limit_reached: bool,
}

impl Default for CallbackStats {
    fn default() -> Self {
        Self {
            total: 0,
            budget_exceeded: 0,
            consecutive_violations: 0,
            last_duration: Duration::ZERO,
            running_since: None,
            stalled: false,
            violation_limit_reached: false,
        }
    }
}

struct CallbackRecord {
    budget: CallbackBudget,
    stats: CallbackStats,
}

#[derive(Clone, Default)]
pub struct CallbackMonitor {
    records: Arc<Mutex<BTreeMap<Arc<str>, CallbackRecord>>>,
}

pub struct CallbackGuard {
    name: Arc<str>,
    started: Instant,
    monitor: CallbackMonitor,
    finished: bool,
}

impl CallbackMonitor {
    pub fn register(&self, name: impl Into<Arc<str>>, budget: CallbackBudget) {
        self.records
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(
                name.into(),
                CallbackRecord {
                    budget,
                    stats: CallbackStats::default(),
                },
            );
    }

    pub fn begin(&self, name: &str) -> Option<CallbackGuard> {
        let started = Instant::now();
        let mut records = self.records.lock().unwrap_or_else(|p| p.into_inner());
        let record = records.get_mut(name)?;
        record.stats.running_since = Some(started);
        record.stats.stalled = false;
        Some(CallbackGuard {
            name: Arc::from(name),
            started,
            monitor: self.clone(),
            finished: false,
        })
    }

    pub fn scan_stalled(&self, now: Instant) -> Vec<Arc<str>> {
        let mut records = self.records.lock().unwrap_or_else(|p| p.into_inner());
        records
            .iter_mut()
            .filter_map(|(name, record)| {
                let stalled = record.stats.running_since.is_some_and(|started| {
                    now.saturating_duration_since(started)
                        >= Duration::from_micros(record.budget.stall_threshold_us)
                });
                if stalled && !record.stats.stalled {
                    record.stats.stalled = true;
                    Some(name.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn stats(&self, name: &str) -> Option<CallbackStats> {
        self.records
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(name)
            .map(|record| record.stats.clone())
    }

    pub fn callbacks_over_violation_limit(&self) -> Vec<Arc<str>> {
        self.records
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .filter_map(|(name, record)| record.stats.violation_limit_reached.then(|| name.clone()))
            .collect()
    }
}

impl CallbackGuard {
    pub fn finish(mut self) {
        self.record();
        self.finished = true;
    }
    fn record(&self) {
        let duration = self.started.elapsed();
        let mut records = self
            .monitor
            .records
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if let Some(record) = records.get_mut(&self.name) {
            record.stats.total += 1;
            record.stats.last_duration = duration;
            record.stats.running_since = None;
            if duration >= Duration::from_micros(record.budget.soft_budget_us) {
                record.stats.budget_exceeded += 1;
                record.stats.consecutive_violations += 1;
                record.stats.violation_limit_reached =
                    record.stats.consecutive_violations >= record.budget.max_consecutive_violations;
            } else {
                record.stats.consecutive_violations = 0;
                record.stats.violation_limit_reached = false;
            }
        }
    }
}

#[derive(Clone)]
pub struct ThreadHeartbeat {
    last_beat: Arc<Mutex<Instant>>,
}

impl Default for ThreadHeartbeat {
    fn default() -> Self {
        Self::new()
    }
}

impl ThreadHeartbeat {
    pub fn new() -> Self {
        Self {
            last_beat: Arc::new(Mutex::new(Instant::now())),
        }
    }

    pub fn beat(&self) {
        *self
            .last_beat
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Instant::now();
    }

    pub fn age(&self, now: Instant) -> Duration {
        now.saturating_duration_since(
            *self
                .last_beat
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        )
    }

    pub fn is_stale(&self, now: Instant, timeout: Duration) -> bool {
        self.age(now) >= timeout
    }
}

impl Drop for CallbackGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.record();
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TraceRecord {
    pub sequence: u64,
    pub trace: TraceContext,
    pub kind: u16,
    pub value: u64,
    pub force_keep: bool,
}

pub mod trace_kind {
    pub const CALLBACK_BEGIN: u16 = 1;
    pub const CALLBACK_END: u16 = 2;
    pub const CALLBACK_BUDGET_EXCEEDED: u16 = 3;
    pub const CALLBACK_STALLED: u16 = 4;
    pub const SERVICE_BEGIN: u16 = 10;
    pub const SERVICE_END: u16 = 11;
    pub const LIFECYCLE_FAILURE: u16 = 20;
}

struct TraceSlot {
    /// Non-zero while a writer owns the slot. A contending writer drops its record rather than
    /// blocking a runtime thread.
    writer: AtomicU64,
    sequence: AtomicU64,
    trace_id: AtomicU64,
    causation_id: AtomicU64,
    kind: AtomicU16,
    value: AtomicU64,
    force_keep: AtomicBool,
}

impl TraceSlot {
    fn new() -> Self {
        Self {
            writer: AtomicU64::new(0),
            sequence: AtomicU64::new(0),
            trace_id: AtomicU64::new(0),
            causation_id: AtomicU64::new(0),
            kind: AtomicU16::new(0),
            value: AtomicU64::new(0),
            force_keep: AtomicBool::new(false),
        }
    }

    fn snapshot(&self) -> Option<TraceRecord> {
        if self.writer.load(Ordering::Acquire) != 0 {
            return None;
        }
        let sequence = self.sequence.load(Ordering::Acquire);
        if sequence == 0 {
            return None;
        }
        let record = TraceRecord {
            sequence,
            trace: TraceContext {
                trace_id: self.trace_id.load(Ordering::Relaxed),
                causation_id: self.causation_id.load(Ordering::Relaxed),
            },
            kind: self.kind.load(Ordering::Relaxed),
            value: self.value.load(Ordering::Relaxed),
            force_keep: self.force_keep.load(Ordering::Relaxed),
        };
        // A writer may have claimed the slot after the first guard read. Both the guard and the
        // published sequence must remain stable for this to be a coherent record.
        (self.writer.load(Ordering::Acquire) == 0
            && self.sequence.load(Ordering::Acquire) == sequence)
            .then_some(record)
    }
}

/// Bounded lock-free in-memory flight recorder. Runtime threads perform a fixed number of atomic
/// writes and never wait for another writer. Formatting/export remains on a background consumer.
pub struct FlightRecorder {
    next_sequence: AtomicU64,
    slots: Box<[TraceSlot]>,
    dropped: AtomicU64,
    frozen: Mutex<Option<Arc<[TraceRecord]>>>,
}

impl FlightRecorder {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0);
        Self {
            next_sequence: AtomicU64::new(1),
            slots: (0..capacity)
                .map(|_| TraceSlot::new())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            dropped: AtomicU64::new(0),
            frozen: Mutex::new(None),
        }
    }
    pub fn record(&self, trace: TraceContext, kind: u16, value: u64, force_keep: bool) {
        let sequence = self.next_sequence.fetch_add(1, Ordering::Relaxed);
        let slot_index = (sequence.wrapping_sub(1) % self.slots.len() as u64) as usize;
        let slot = &self.slots[slot_index];
        if slot
            .writer
            .compare_exchange(0, sequence, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        slot.trace_id.store(trace.trace_id, Ordering::Relaxed);
        slot.causation_id
            .store(trace.causation_id, Ordering::Relaxed);
        slot.kind.store(kind, Ordering::Relaxed);
        slot.value.store(value, Ordering::Relaxed);
        slot.force_keep.store(force_keep, Ordering::Relaxed);
        slot.sequence.store(sequence, Ordering::Release);
        slot.writer.store(0, Ordering::Release);
    }
    pub fn dropped_records(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
    pub fn freeze(&self) -> Arc<[TraceRecord]> {
        let mut frozen = self.frozen.lock().unwrap_or_else(|p| p.into_inner());
        frozen
            .get_or_insert_with(|| {
                let mut records = self
                    .slots
                    .iter()
                    .filter_map(TraceSlot::snapshot)
                    .collect::<Vec<_>>();
                records.sort_unstable_by_key(|record| record.sequence);
                records.into()
            })
            .clone()
    }
    pub fn unfreeze(&self) {
        *self.frozen.lock().unwrap_or_else(|p| p.into_inner()) = None;
    }
}

pub trait FlightRecorderSink: Send + Sync + 'static {
    fn export(&self, records: Arc<[TraceRecord]>);
}

/// Bounded background exporter. Runtime threads only perform a non-blocking enqueue; formatting,
/// storage and telemetry conversion remain on this worker.
pub struct BackgroundFlightRecorderExporter {
    sender: SyncSender<Arc<[TraceRecord]>>,
    stop: Arc<AtomicBool>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl BackgroundFlightRecorderExporter {
    pub fn start(capacity: usize, sink: Arc<dyn FlightRecorderSink>) -> Result<Arc<Self>, String> {
        if capacity == 0 {
            return Err("flight recorder export capacity must be positive".into());
        }
        let (sender, receiver) = sync_channel(capacity);
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = stop.clone();
        let worker = std::thread::Builder::new()
            .name("plugin-flight-recorder-export".into())
            .spawn(move || {
                while !worker_stop.load(Ordering::Acquire) {
                    match receiver.recv_timeout(Duration::from_millis(50)) {
                        Ok(records) => sink.export(records),
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                    }
                }
                while let Ok(records) = receiver.try_recv() {
                    sink.export(records);
                }
            })
            .map_err(|error| error.to_string())?;
        Ok(Arc::new(Self {
            sender,
            stop,
            worker: Mutex::new(Some(worker)),
        }))
    }

    pub fn try_export(&self, records: Arc<[TraceRecord]>) -> bool {
        self.sender.try_send(records).is_ok()
    }
}

impl Drop for BackgroundFlightRecorderExporter {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self
            .worker
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            let _ = worker.join();
        }
    }
}

#[cfg(test)]
mod exporter_tests {
    use super::*;
    use std::sync::mpsc;

    struct ChannelSink(mpsc::Sender<(String, Arc<[TraceRecord]>)>);

    impl FlightRecorderSink for ChannelSink {
        fn export(&self, records: Arc<[TraceRecord]>) {
            let thread = std::thread::current().name().unwrap_or_default().to_owned();
            let _ = self.0.send((thread, records));
        }
    }

    #[test]
    fn forced_snapshot_is_exported_on_the_bounded_background_worker() {
        let recorder = FlightRecorder::new(4);
        recorder.record(
            TraceContext::default(),
            trace_kind::LIFECYCLE_FAILURE,
            7,
            true,
        );
        let (sender, receiver) = mpsc::channel();
        let exporter = BackgroundFlightRecorderExporter::start(1, Arc::new(ChannelSink(sender)))
            .expect("background exporter starts");
        assert!(exporter.try_export(recorder.freeze()));
        let (thread, records) = receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("snapshot exported");
        assert_eq!(thread, "plugin-flight-recorder-export");
        assert_eq!(records.len(), 1);
        assert!(records[0].force_keep);
    }

    #[test]
    fn concurrent_writers_publish_only_coherent_records_without_blocking() {
        let recorder = Arc::new(FlightRecorder::new(128));
        let writers = (0..8)
            .map(|writer| {
                let recorder = recorder.clone();
                std::thread::spawn(move || {
                    for value in 1..=10_000_u64 {
                        let encoded = ((writer as u64) << 48) | value;
                        recorder.record(
                            TraceContext {
                                trace_id: encoded,
                                causation_id: !encoded,
                            },
                            writer,
                            encoded,
                            writer % 2 == 0,
                        );
                    }
                })
            })
            .collect::<Vec<_>>();
        for writer in writers {
            writer.join().unwrap();
        }
        let records = recorder.freeze();
        assert!(!records.is_empty());
        assert!(records.len() <= 128);
        assert!(
            records
                .windows(2)
                .all(|pair| pair[0].sequence < pair[1].sequence)
        );
        for record in records.iter() {
            assert_eq!(record.trace.trace_id, record.value);
            assert_eq!(record.trace.causation_id, !record.value);
            assert_eq!(record.force_keep, record.kind % 2 == 0);
        }
    }
}
