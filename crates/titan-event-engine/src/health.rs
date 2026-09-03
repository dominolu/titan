use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};

use crate::PoolKind;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum SubscriberState {
    Normal = 0,
    Lagging = 1,
    Pending = 2,
    Recovering = 3,
    ResyncRequired = 4,
    Failed = 5,
    Stopped = 6,
}

impl SubscriberState {
    fn from_raw(value: u8) -> Self {
        match value {
            0 => Self::Normal,
            1 => Self::Lagging,
            2 => Self::Pending,
            3 => Self::Recovering,
            4 => Self::ResyncRequired,
            5 => Self::Failed,
            _ => Self::Stopped,
        }
    }

    pub(crate) fn is_terminal(self) -> bool {
        matches!(self, Self::Failed | Self::Stopped)
    }
}

#[derive(Debug)]
pub struct SubscriberHealth {
    state: AtomicU8,
    outstanding_handles: AtomicUsize,
    oldest_handle_ns: AtomicU64,
    last_progress_ns: AtomicU64,
    gap_first_sequence: AtomicU64,
    gap_last_sequence: AtomicU64,
    recovery_sequence: AtomicU64,
    channel_depth: AtomicUsize,
    pending_depth: AtomicUsize,
    admitted_sequence: AtomicU64,
    dispatched_sequence: AtomicU64,
    committed_sequence: AtomicU64,
}

impl Default for SubscriberHealth {
    fn default() -> Self {
        Self {
            state: AtomicU8::new(SubscriberState::Normal as u8),
            outstanding_handles: AtomicUsize::new(0),
            oldest_handle_ns: AtomicU64::new(0),
            last_progress_ns: AtomicU64::new(0),
            gap_first_sequence: AtomicU64::new(0),
            gap_last_sequence: AtomicU64::new(0),
            recovery_sequence: AtomicU64::new(0),
            channel_depth: AtomicUsize::new(0),
            pending_depth: AtomicUsize::new(0),
            admitted_sequence: AtomicU64::new(0),
            dispatched_sequence: AtomicU64::new(0),
            committed_sequence: AtomicU64::new(0),
        }
    }
}

impl SubscriberHealth {
    pub fn state(&self) -> SubscriberState {
        SubscriberState::from_raw(self.state.load(Ordering::Acquire))
    }

    pub(crate) fn set_state(&self, state: SubscriberState) {
        self.state.store(state as u8, Ordering::Release);
    }

    pub(crate) fn transition(&self, from: SubscriberState, to: SubscriberState) -> bool {
        self.state
            .compare_exchange(from as u8, to as u8, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub(crate) fn transition_nonterminal(&self, to: SubscriberState) -> Option<SubscriberState> {
        let mut current = self.state();
        loop {
            if current.is_terminal() {
                return None;
            }
            if current == to {
                return Some(current);
            }
            match self.state.compare_exchange(
                current as u8,
                to as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(current),
                Err(actual) => current = SubscriberState::from_raw(actual),
            }
        }
    }

    pub fn outstanding_handles(&self) -> usize {
        self.outstanding_handles.load(Ordering::Relaxed)
    }

    pub fn oldest_handle_ns(&self) -> u64 {
        self.oldest_handle_ns.load(Ordering::Relaxed)
    }

    pub fn last_progress_ns(&self) -> u64 {
        self.last_progress_ns.load(Ordering::Relaxed)
    }

    pub fn delivery_gap(&self) -> Option<(u64, u64)> {
        let first = self.gap_first_sequence.load(Ordering::Acquire);
        (first != 0).then(|| (first, self.gap_last_sequence.load(Ordering::Acquire)))
    }

    pub fn recovery_sequence(&self) -> Option<u64> {
        let value = self.recovery_sequence.load(Ordering::Acquire);
        (value != 0).then_some(value)
    }

    pub fn admitted_sequence(&self) -> u64 {
        self.admitted_sequence.load(Ordering::Acquire)
    }

    pub fn dispatched_sequence(&self) -> u64 {
        self.dispatched_sequence.load(Ordering::Acquire)
    }

    pub fn committed_sequence(&self) -> u64 {
        self.committed_sequence.load(Ordering::Acquire)
    }

    pub(crate) fn on_admitted(&self, sequence: u64) {
        self.admitted_sequence
            .fetch_max(sequence, Ordering::Release);
    }

    pub(crate) fn on_dispatched(&self, sequence: u64) {
        self.dispatched_sequence
            .fetch_max(sequence, Ordering::Release);
    }

    pub(crate) fn on_committed(&self, sequence: u64) {
        self.committed_sequence
            .fetch_max(sequence, Ordering::Release);
    }

    pub(crate) fn on_enqueue(&self, now_ns: u64) {
        if self.outstanding_handles.fetch_add(1, Ordering::Relaxed) == 0 {
            self.oldest_handle_ns.store(now_ns, Ordering::Relaxed);
        }
    }

    pub(crate) fn on_release(&self, now_ns: u64) {
        let previous = self.outstanding_handles.fetch_sub(1, Ordering::Relaxed);
        assert!(previous > 0, "subscriber outstanding handle underflow");
        if previous == 1 {
            self.oldest_handle_ns.store(0, Ordering::Relaxed);
        }
        self.last_progress_ns.store(now_ns, Ordering::Relaxed);
    }

    pub(crate) fn record_gap(&self, sequence: u64) {
        let _ =
            self.gap_first_sequence
                .fetch_update(Ordering::Release, Ordering::Relaxed, |current| {
                    (current == 0 || sequence < current).then_some(sequence)
                });
        self.gap_last_sequence
            .fetch_max(sequence, Ordering::Release);
    }

    pub(crate) fn begin_recovery(&self, recovery_sequence: u64) {
        self.recovery_sequence
            .store(recovery_sequence, Ordering::Release);
        self.set_state(SubscriberState::Recovering);
    }

    pub(crate) fn finish_recovery(&self) {
        self.gap_first_sequence.store(0, Ordering::Release);
        self.gap_last_sequence.store(0, Ordering::Release);
        self.recovery_sequence.store(0, Ordering::Release);
        self.set_state(SubscriberState::Normal);
    }

    pub(crate) fn set_channel_depth(&self, depth: usize) {
        self.channel_depth.store(depth, Ordering::Relaxed);
    }

    pub(crate) fn set_pending_depth(&self, depth: usize) {
        self.pending_depth.store(depth, Ordering::Relaxed);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FaultKind {
    MarketIngressFull,
    CriticalIngressFull,
    ArenaPressure,
    PendingFull,
    PendingExpired,
    SubscriberFailed,
    SourceSequenceGap,
    TimerSignalFull,
    SubscriberLagging,
    SubscriberBackpressure,
    SubscriberRecovered,
    SnapshotRecoveryAborted,
    EventLoopFailed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FaultSignal {
    pub kind: FaultKind,
    pub subscriber_id: u64,
    pub sequence: u64,
    pub detail: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SubscriberHealthSnapshot {
    pub state: SubscriberState,
    pub outstanding_handles: usize,
    pub oldest_handle_ns: u64,
    pub last_progress_ns: u64,
    pub delivery_gap: Option<(u64, u64)>,
    pub recovery_sequence: Option<u64>,
    pub channel_depth: usize,
    pub pending_depth: usize,
    pub admitted_sequence: u64,
    pub dispatched_sequence: u64,
    pub committed_sequence: u64,
}

impl SubscriberHealth {
    pub fn snapshot(&self) -> SubscriberHealthSnapshot {
        SubscriberHealthSnapshot {
            state: self.state(),
            outstanding_handles: self.outstanding_handles(),
            oldest_handle_ns: self.oldest_handle_ns(),
            last_progress_ns: self.last_progress_ns(),
            delivery_gap: self.delivery_gap(),
            recovery_sequence: self.recovery_sequence(),
            channel_depth: self.channel_depth.load(Ordering::Relaxed),
            pending_depth: self.pending_depth.load(Ordering::Relaxed),
            admitted_sequence: self.admitted_sequence(),
            dispatched_sequence: self.dispatched_sequence(),
            committed_sequence: self.committed_sequence(),
        }
    }
}

#[derive(Debug, Default)]
pub struct RuntimeHealth {
    market_stream_invalid: AtomicBool,
    critical_ingress_backpressure: AtomicBool,
    arena_pressure_mask: AtomicU8,
    last_source_gap: AtomicU64,
    event_loop_failed: AtomicBool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RuntimeHealthSnapshot {
    pub market_stream_invalid: bool,
    pub critical_ingress_backpressure: bool,
    pub arena_pressure_mask: u8,
    pub last_source_gap: Option<u64>,
    pub event_loop_failed: bool,
}

impl RuntimeHealth {
    pub fn snapshot(&self) -> RuntimeHealthSnapshot {
        let last_source_gap = self.last_source_gap.load(Ordering::Acquire);
        RuntimeHealthSnapshot {
            market_stream_invalid: self.market_stream_invalid.load(Ordering::Acquire),
            critical_ingress_backpressure: self
                .critical_ingress_backpressure
                .load(Ordering::Acquire),
            arena_pressure_mask: self.arena_pressure_mask.load(Ordering::Acquire),
            last_source_gap: (last_source_gap != 0).then_some(last_source_gap),
            event_loop_failed: self.event_loop_failed.load(Ordering::Acquire),
        }
    }

    pub fn clear(&self) {
        self.market_stream_invalid.store(false, Ordering::Release);
        self.critical_ingress_backpressure
            .store(false, Ordering::Release);
        self.arena_pressure_mask.store(0, Ordering::Release);
        self.last_source_gap.store(0, Ordering::Release);
        self.event_loop_failed.store(false, Ordering::Release);
    }

    pub(crate) fn mark_ingress_full(&self, critical: bool) {
        if critical {
            self.critical_ingress_backpressure
                .store(true, Ordering::Release);
        } else {
            self.market_stream_invalid.store(true, Ordering::Release);
        }
    }

    pub(crate) fn mark_arena_pressure(&self, pool: PoolKind) {
        self.arena_pressure_mask
            .fetch_or(1 << pool.index(), Ordering::Release);
    }

    pub(crate) fn mark_source_gap(&self, source_id: u32, sequence: u64) {
        let encoded = ((source_id as u64) << 32) | sequence.min(u32::MAX as u64);
        self.last_source_gap
            .store(encoded.max(1), Ordering::Release);
    }

    pub(crate) fn mark_event_loop_failed(&self) {
        self.event_loop_failed.store(true, Ordering::Release);
    }
}
