use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
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
            } else {
                record.stats.consecutive_violations = 0;
            }
        }
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

/// Bounded in-memory flight recorder. Writers only take this thread-local recorder's lock;
/// formatting/export is intentionally left to a background consumer.
pub struct FlightRecorder {
    capacity: usize,
    next_sequence: AtomicU64,
    records: Mutex<VecDeque<TraceRecord>>,
    frozen: Mutex<Option<Arc<[TraceRecord]>>>,
}

impl FlightRecorder {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0);
        Self {
            capacity,
            next_sequence: AtomicU64::new(1),
            records: Mutex::new(VecDeque::with_capacity(capacity)),
            frozen: Mutex::new(None),
        }
    }
    pub fn record(&self, trace: TraceContext, kind: u16, value: u64, force_keep: bool) {
        let mut records = self.records.lock().unwrap_or_else(|p| p.into_inner());
        if records.len() == self.capacity {
            records.pop_front();
        }
        records.push_back(TraceRecord {
            sequence: self.next_sequence.fetch_add(1, Ordering::Relaxed),
            trace,
            kind,
            value,
            force_keep,
        });
    }
    pub fn freeze(&self) -> Arc<[TraceRecord]> {
        let mut frozen = self.frozen.lock().unwrap_or_else(|p| p.into_inner());
        frozen
            .get_or_insert_with(|| {
                self.records
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .iter()
                    .copied()
                    .collect::<Vec<_>>()
                    .into()
            })
            .clone()
    }
    pub fn unfreeze(&self) {
        *self.frozen.lock().unwrap_or_else(|p| p.into_inner()) = None;
    }
}
