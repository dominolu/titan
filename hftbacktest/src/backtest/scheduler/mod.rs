//! Deterministic scheduler shared by all execution modes.

use std::collections::BTreeMap;

mod timer;
pub use timer::{DuplicateTimerPolicy, TimerError, TimerEvent, TimerId, TimerQueue};

/// Version of the default same-timestamp phase contract.
pub const PHASE_CONTRACT_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[repr(u16)]
pub enum EventPhase {
    OldResponseDelivery = 10,
    ExchangeState = 20,
    MarketDelivery = 30,
    StrategyCallback = 40,
    CommandArrival = 50,
    Matching = 60,
    ZeroLatencyResponse = 70,
    Timer = 80,
    PostTradeRisk = 90,
}

/// Globally deterministic event ordering key.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct EventKey {
    pub timestamp: i64,
    pub phase: EventPhase,
    pub source_priority: u16,
    pub venue_no: u32,
    pub asset_no: u32,
    pub sequence: u64,
}

/// Event returned by [`GlobalScheduler`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScheduledEvent<T> {
    pub key: EventKey,
    pub payload: T,
}

/// Single-threaded deterministic scheduler.  `BTreeMap` is intentional here: it provides stable
/// ordering without imposing `Ord` on payloads and is only the initial P0-A implementation.  Hot
/// paths may later use a specialized heap while retaining the same `EventKey` contract.
pub struct GlobalScheduler<T> {
    events: BTreeMap<EventKey, T>,
    next_sequence: u64,
}

impl<T> Default for GlobalScheduler<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> GlobalScheduler<T> {
    pub fn new() -> Self {
        Self {
            events: BTreeMap::new(),
            next_sequence: 0,
        }
    }

    pub fn schedule(
        &mut self,
        timestamp: i64,
        phase: EventPhase,
        source_priority: u16,
        venue_no: u32,
        asset_no: u32,
        payload: T,
    ) -> EventKey {
        let key = EventKey {
            timestamp,
            phase,
            source_priority,
            venue_no,
            asset_no,
            sequence: self.next_sequence,
        };
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .expect("global scheduler sequence overflow");
        let previous = self.events.insert(key, payload);
        debug_assert!(previous.is_none());
        key
    }

    pub fn peek_key(&self) -> Option<EventKey> {
        self.events.first_key_value().map(|(key, _)| *key)
    }

    pub fn pop(&mut self) -> Option<ScheduledEvent<T>> {
        self.events
            .pop_first()
            .map(|(key, payload)| ScheduledEvent { key, payload })
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub fn reset(&mut self) {
        self.events.clear();
        self.next_sequence = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn orders_by_full_contract_and_preserves_insertion_sequence() {
        let mut scheduler = GlobalScheduler::new();
        scheduler.schedule(10, EventPhase::Timer, 0, 0, 0, "timer");
        scheduler.schedule(10, EventPhase::MarketDelivery, 1, 0, 1, "asset-1");
        scheduler.schedule(10, EventPhase::MarketDelivery, 0, 2, 0, "venue-2");
        scheduler.schedule(10, EventPhase::MarketDelivery, 0, 1, 0, "first");
        scheduler.schedule(10, EventPhase::MarketDelivery, 0, 1, 0, "second");
        scheduler.schedule(9, EventPhase::PostTradeRisk, 0, 0, 0, "earlier");

        let mut payloads = Vec::new();
        while let Some(event) = scheduler.pop() {
            payloads.push(event.payload);
        }
        assert_eq!(
            payloads,
            ["earlier", "first", "second", "venue-2", "asset-1", "timer"]
        );
    }

    #[test]
    fn reset_clears_events_and_restarts_sequence() {
        let mut scheduler = GlobalScheduler::new();
        let first = scheduler.schedule(1, EventPhase::Timer, 0, 0, 0, ());
        assert_eq!(first.sequence, 0);
        scheduler.reset();
        assert!(scheduler.is_empty());
        let after_reset = scheduler.schedule(1, EventPhase::Timer, 0, 0, 0, ());
        assert_eq!(after_reset.sequence, 0);
    }

    #[test]
    fn same_timestamp_professional_phase_contract_is_explicit_and_complete() {
        let mut scheduler = GlobalScheduler::new();
        for (phase, name) in [
            (EventPhase::PostTradeRisk, "post-risk"),
            (EventPhase::Timer, "timer"),
            (EventPhase::ZeroLatencyResponse, "zero-response"),
            (EventPhase::Matching, "matching"),
            (EventPhase::CommandArrival, "arrival"),
            (EventPhase::StrategyCallback, "callback"),
            (EventPhase::MarketDelivery, "tick-or-bar"),
            (EventPhase::ExchangeState, "status-or-funding"),
            (EventPhase::OldResponseDelivery, "old-response"),
        ] {
            scheduler.schedule(100, phase, 0, 0, 0, name);
        }
        let ordered: Vec<_> =
            std::iter::from_fn(|| scheduler.pop().map(|event| event.payload)).collect();
        assert_eq!(
            ordered,
            [
                "old-response",
                "status-or-funding",
                "tick-or-bar",
                "callback",
                "arrival",
                "matching",
                "zero-response",
                "timer",
                "post-risk",
            ]
        );
    }
}
