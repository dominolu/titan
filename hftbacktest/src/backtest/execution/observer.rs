use std::{
    cell::{Cell, UnsafeCell},
    collections::VecDeque,
    rc::Rc,
};

use super::MatchOutcome;
use crate::types::{OrdType, Order, OrderId, Side, Status, TimeInForce};

/// Allocation-free copy of the legacy Tick order fields needed by the migration adapter.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct LegacyOrderSnapshot {
    pub order_id: OrderId,
    pub price: f64,
    pub qty: f64,
    pub leaves_qty: f64,
    pub local_submit_ts: i64,
    pub side: Side,
    pub order_type: OrdType,
    pub time_in_force: TimeInForce,
    pub request: Status,
}

impl From<&Order> for LegacyOrderSnapshot {
    fn from(order: &Order) -> Self {
        Self {
            order_id: order.order_id,
            price: order.price(),
            qty: order.qty,
            leaves_qty: order.leaves_qty,
            local_submit_ts: order.local_timestamp,
            side: order.side,
            order_type: order.order_type,
            time_in_force: order.time_in_force,
            request: order.req,
        }
    }
}

/// Monomorphized exchange-time outcome hook. `NoopExecutionObserver` compiles away on legacy hot
/// paths; coordinator adapters can use a concrete observer without trait objects.
pub trait ExecutionObserver {
    fn on_outcome(&mut self, order_id: OrderId, outcome: MatchOutcome);

    #[inline]
    fn on_order_outcome(&mut self, order: &Order, outcome: MatchOutcome) {
        self.on_outcome(order.order_id, outcome);
    }
    fn reset(&mut self) {}
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NoopExecutionObserver;

impl ExecutionObserver for NoopExecutionObserver {
    #[inline(always)]
    fn on_outcome(&mut self, _order_id: OrderId, _outcome: MatchOutcome) {}
}

#[derive(Debug, Default)]
pub struct BufferedExecutionObserver {
    outcomes: Vec<ObservedOutcome>,
}

impl BufferedExecutionObserver {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            outcomes: Vec::with_capacity(capacity),
        }
    }

    pub fn as_slice(&self) -> &[ObservedOutcome] {
        &self.outcomes
    }

    pub fn drain(&mut self) -> impl Iterator<Item = ObservedOutcome> + '_ {
        self.outcomes.drain(..)
    }
}

impl ExecutionObserver for BufferedExecutionObserver {
    #[inline]
    fn on_outcome(&mut self, order_id: OrderId, outcome: MatchOutcome) {
        self.outcomes.push(ObservedOutcome {
            order_id,
            order: None,
            outcome,
        });
    }

    fn on_order_outcome(&mut self, order: &Order, outcome: MatchOutcome) {
        self.outcomes.push(ObservedOutcome {
            order_id: order.order_id,
            order: Some(order.into()),
            outcome,
        });
    }

    fn reset(&mut self) {
        self.outcomes.clear();
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ObservedOutcome {
    pub order_id: OrderId,
    pub order: Option<LegacyOrderSnapshot>,
    pub outcome: MatchOutcome,
}

/// Single-threaded exchange-to-engine outcome bus. It mirrors the existing order bus ownership
/// model and keeps the matcher independent from account/report/callback components.
#[derive(Clone, Debug, Default)]
pub struct OutcomeBus {
    queue: Rc<UnsafeCell<VecDeque<ObservedOutcome>>>,
    pending: Rc<Cell<bool>>,
}

impl OutcomeBus {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn pop_front(&mut self) -> Option<ObservedOutcome> {
        // Backtest scheduling is single-threaded and drains only between processor calls.
        let queue = unsafe { &mut *self.queue.get() };
        let outcome = queue.pop_front();
        if queue.is_empty() {
            self.pending.set(false);
        }
        outcome
    }

    pub fn len(&self) -> usize {
        unsafe { &*self.queue.get() }.len()
    }

    pub fn is_empty(&self) -> bool {
        !self.pending.get()
    }

    pub fn clear(&mut self) {
        unsafe { &mut *self.queue.get() }.clear();
        self.pending.set(false);
    }
}

impl ExecutionObserver for OutcomeBus {
    #[inline]
    fn on_outcome(&mut self, order_id: OrderId, outcome: MatchOutcome) {
        unsafe { &mut *self.queue.get() }.push_back(ObservedOutcome {
            order_id,
            order: None,
            outcome,
        });
        self.pending.set(true);
    }

    #[inline]
    fn on_order_outcome(&mut self, order: &Order, outcome: MatchOutcome) {
        unsafe { &mut *self.queue.get() }.push_back(ObservedOutcome {
            order_id: order.order_id,
            order: Some(order.into()),
            outcome,
        });
        self.pending.set(true);
    }

    fn reset(&mut self) {
        self.clear();
    }
}

#[cfg(test)]
mod bus_tests {
    use super::*;

    #[test]
    fn cloned_bus_preserves_exchange_order() {
        let mut producer = OutcomeBus::new();
        let mut consumer = producer.clone();
        producer.on_outcome(1, MatchOutcome::Accepted { exchange_ts: 10 });
        producer.on_outcome(1, MatchOutcome::Expired { exchange_ts: 11 });
        assert_eq!(consumer.pop_front().unwrap().order_id, 1);
        assert!(matches!(
            consumer.pop_front().unwrap().outcome,
            MatchOutcome::Expired { exchange_ts: 11 }
        ));
        assert!(consumer.is_empty());
    }
}
