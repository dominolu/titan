use std::sync::atomic::{AtomicU64, Ordering};

use crate::PoolKind;

const LATENCY_BUCKETS: usize = 64;

#[derive(Debug)]
pub struct LatencyHistogram {
    buckets: [AtomicU64; LATENCY_BUCKETS],
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LatencySummary {
    pub count: u64,
    pub p50_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: u64,
    pub max_ns: u64,
}

impl LatencyHistogram {
    pub fn record(&self, value_ns: u64) {
        let bucket = if value_ns <= 1 {
            0
        } else {
            (63 - value_ns.leading_zeros()) as usize
        };
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
    }

    pub fn summary(&self) -> LatencySummary {
        let counts = std::array::from_fn::<_, LATENCY_BUCKETS, _>(|index| {
            self.buckets[index].load(Ordering::Relaxed)
        });
        let total = counts.iter().sum();
        if total == 0 {
            return LatencySummary::default();
        }
        LatencySummary {
            count: total,
            p50_ns: percentile(&counts, total, 500),
            p99_ns: percentile(&counts, total, 990),
            p999_ns: percentile(&counts, total, 999),
            max_ns: percentile(&counts, total, 1_000),
        }
    }
}

fn percentile(counts: &[u64; LATENCY_BUCKETS], total: u64, permille: u64) -> u64 {
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

#[derive(Debug, Default)]
pub struct EngineMetrics {
    pub(crate) publish_total: AtomicU64,
    pub(crate) publish_rejected_total: AtomicU64,
    pub(crate) dispatch_total: AtomicU64,
    pub(crate) drop_total: AtomicU64,
    pub(crate) delivery_gap_total: AtomicU64,
    pub(crate) resync_total: AtomicU64,
    pub(crate) pending_retry_success: AtomicU64,
    pub(crate) pending_retry_full: AtomicU64,
    pub(crate) drain_count: AtomicU64,
    pub(crate) drain_duration_ns_max: AtomicU64,
    pub(crate) dispatch_latency_ns_max: AtomicU64,
    pub(crate) timer_lateness_ns_max: AtomicU64,
    pub(crate) source_sequence_gap_total: AtomicU64,
    pub(crate) service_gap_ns_max: [AtomicU64; 4],
    pub(crate) drain_over_budget_total: AtomicU64,
    pub(crate) fanout_continuation_total: AtomicU64,
    pub(crate) fault_signal_drop_total: AtomicU64,
    pub(crate) trace_ring_drop_total: AtomicU64,
    pub(crate) fast_lane_enqueue_total: AtomicU64,
    pub(crate) fast_lane_drop_total: AtomicU64,
    pub(crate) fast_lane_depth_max: AtomicU64,
    pub(crate) fast_lane_latency: LatencyHistogram,
    pub(crate) fast_lane_enqueue_latency: LatencyHistogram,
    pub(crate) arena_exhausted: [AtomicU64; 3],
    pub(crate) arena_pressure: [AtomicU64; 3],
    pub(crate) drain_latency: LatencyHistogram,
    pub(crate) dispatch_latency: LatencyHistogram,
    pub(crate) subscriber_latency: LatencyHistogram,
    pub(crate) timer_lateness: LatencyHistogram,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetricsSnapshot {
    pub publish_total: u64,
    pub publish_rejected_total: u64,
    pub dispatch_total: u64,
    pub drop_total: u64,
    pub delivery_gap_total: u64,
    pub resync_total: u64,
    pub pending_retry_success: u64,
    pub pending_retry_full: u64,
    pub drain_count: u64,
    pub drain_duration_ns_max: u64,
    pub dispatch_latency_ns_max: u64,
    pub timer_lateness_ns_max: u64,
    pub source_sequence_gap_total: u64,
    pub service_gap_ns_max: [u64; 4],
    pub drain_over_budget_total: u64,
    pub fanout_continuation_total: u64,
    pub fault_signal_drop_total: u64,
    pub trace_ring_drop_total: u64,
    pub fast_lane_enqueue_total: u64,
    pub fast_lane_drop_total: u64,
    pub fast_lane_depth_max: u64,
    pub fast_lane_latency: LatencySummary,
    pub fast_lane_enqueue_latency: LatencySummary,
    pub arena_exhausted: [u64; 3],
    pub arena_pressure: [u64; 3],
    pub drain_latency: LatencySummary,
    pub dispatch_latency: LatencySummary,
    pub subscriber_latency: LatencySummary,
    pub timer_lateness: LatencySummary,
}

impl EngineMetrics {
    pub fn snapshot(&self) -> MetricsSnapshot {
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        MetricsSnapshot {
            publish_total: load(&self.publish_total),
            publish_rejected_total: load(&self.publish_rejected_total),
            dispatch_total: load(&self.dispatch_total),
            drop_total: load(&self.drop_total),
            delivery_gap_total: load(&self.delivery_gap_total),
            resync_total: load(&self.resync_total),
            pending_retry_success: load(&self.pending_retry_success),
            pending_retry_full: load(&self.pending_retry_full),
            drain_count: load(&self.drain_count),
            drain_duration_ns_max: load(&self.drain_duration_ns_max),
            dispatch_latency_ns_max: load(&self.dispatch_latency_ns_max),
            timer_lateness_ns_max: load(&self.timer_lateness_ns_max),
            source_sequence_gap_total: load(&self.source_sequence_gap_total),
            service_gap_ns_max: std::array::from_fn(|index| load(&self.service_gap_ns_max[index])),
            drain_over_budget_total: load(&self.drain_over_budget_total),
            fanout_continuation_total: load(&self.fanout_continuation_total),
            fault_signal_drop_total: load(&self.fault_signal_drop_total),
            trace_ring_drop_total: load(&self.trace_ring_drop_total),
            fast_lane_enqueue_total: load(&self.fast_lane_enqueue_total),
            fast_lane_drop_total: load(&self.fast_lane_drop_total),
            fast_lane_depth_max: load(&self.fast_lane_depth_max),
            fast_lane_latency: self.fast_lane_latency.summary(),
            fast_lane_enqueue_latency: self.fast_lane_enqueue_latency.summary(),
            arena_exhausted: PoolKind::ALL.map(|pool| load(&self.arena_exhausted[pool.index()])),
            arena_pressure: PoolKind::ALL.map(|pool| load(&self.arena_pressure[pool.index()])),
            drain_latency: self.drain_latency.summary(),
            dispatch_latency: self.dispatch_latency.summary(),
            subscriber_latency: self.subscriber_latency.summary(),
            timer_lateness: self.timer_lateness.summary(),
        }
    }

    pub(crate) fn observe_drain_duration(&self, elapsed_ns: u64) {
        self.drain_latency.record(elapsed_ns);
        self.drain_duration_ns_max
            .fetch_max(elapsed_ns, Ordering::Relaxed);
    }

    pub(crate) fn observe_dispatch_latency(&self, elapsed_ns: u64) {
        self.dispatch_latency.record(elapsed_ns);
        self.dispatch_latency_ns_max
            .fetch_max(elapsed_ns, Ordering::Relaxed);
    }

    pub(crate) fn observe_service_gap(&self, class: usize, elapsed_ns: u64) {
        self.service_gap_ns_max[class].fetch_max(elapsed_ns, Ordering::Relaxed);
    }

    pub(crate) fn observe_subscriber_latency(&self, elapsed_ns: u64) {
        self.subscriber_latency.record(elapsed_ns);
    }

    pub(crate) fn observe_timer_lateness(&self, elapsed_ns: u64) {
        self.timer_lateness.record(elapsed_ns);
    }
}
