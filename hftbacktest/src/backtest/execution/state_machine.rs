use thiserror::Error;

use super::ExecutionOrderRequest;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TriggerState {
    None,
    Pending,
    Triggered,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TriggerKind {
    StopMarket,
    StopLimit,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct OrderExtensions {
    pub trigger: Option<(TriggerKind, f64)>,
    pub gtd_expiry_ts: Option<i64>,
    pub contingency_id: Option<u64>,
    pub parent_order_id: Option<u64>,
    pub cancel_replace_correlation_id: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrderState {
    Initialized,
    Submitted,
    Accepted,
    PartiallyFilled,
    PendingCancel,
    Filled,
    Rejected,
    Canceled,
    Expired,
}

impl OrderState {
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Filled | Self::Rejected | Self::Canceled | Self::Expired
        )
    }

    pub const fn is_exchange_active(self) -> bool {
        matches!(
            self,
            Self::Accepted | Self::PartiallyFilled | Self::PendingCancel
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum OrderTransition {
    Submit,
    Accept,
    Reject,
    RequestCancel,
    CancelReject,
    Cancel,
    Fill { qty: f64 },
    Expire,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransitionResult {
    StateChanged,
    PartialFillWhileCancelPending,
}

#[derive(Clone, Copy, Debug, Error, PartialEq)]
pub enum OrderStateError {
    #[error("invalid order state transition from {state:?} using {transition:?}")]
    InvalidTransition {
        state: OrderState,
        transition: OrderTransition,
    },
    #[error("fill quantity must be finite and positive")]
    InvalidFillQuantity,
    #[error("fill quantity exceeds leaves quantity")]
    Overfill,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ExecutionOrder {
    pub request: ExecutionOrderRequest,
    pub state: OrderState,
    pub filled_qty: f64,
    pub leaves_qty: f64,
    pub exchange_arrival_ts: i64,
    pub last_exchange_ts: i64,
    pub last_delivery_ts: i64,
    pub extensions: OrderExtensions,
    pub trigger_state: TriggerState,
}

impl ExecutionOrder {
    pub fn new(request: ExecutionOrderRequest) -> Self {
        Self::new_with_extensions(request, OrderExtensions::default())
    }

    pub fn new_with_extensions(
        request: ExecutionOrderRequest,
        extensions: OrderExtensions,
    ) -> Self {
        Self {
            request,
            state: OrderState::Initialized,
            filled_qty: 0.0,
            leaves_qty: request.qty,
            exchange_arrival_ts: 0,
            last_exchange_ts: 0,
            last_delivery_ts: 0,
            trigger_state: if extensions.trigger.is_some() {
                TriggerState::Pending
            } else {
                TriggerState::None
            },
            extensions,
        }
    }

    pub fn transition(
        &mut self,
        transition: OrderTransition,
    ) -> Result<TransitionResult, OrderStateError> {
        let old_state = self.state;
        let result = match transition {
            OrderTransition::Submit if old_state == OrderState::Initialized => {
                self.state = OrderState::Submitted;
                TransitionResult::StateChanged
            }
            OrderTransition::Accept if old_state == OrderState::Submitted => {
                self.state = OrderState::Accepted;
                TransitionResult::StateChanged
            }
            OrderTransition::Reject if old_state == OrderState::Submitted => {
                self.state = OrderState::Rejected;
                TransitionResult::StateChanged
            }
            OrderTransition::RequestCancel
                if matches!(
                    old_state,
                    OrderState::Accepted | OrderState::PartiallyFilled
                ) =>
            {
                self.state = OrderState::PendingCancel;
                TransitionResult::StateChanged
            }
            OrderTransition::Cancel if old_state == OrderState::PendingCancel => {
                self.state = OrderState::Canceled;
                TransitionResult::StateChanged
            }
            OrderTransition::CancelReject if old_state == OrderState::PendingCancel => {
                self.state = if self.filled_qty > 0.0 {
                    OrderState::PartiallyFilled
                } else {
                    OrderState::Accepted
                };
                TransitionResult::StateChanged
            }
            OrderTransition::Expire
                if matches!(
                    old_state,
                    OrderState::Submitted
                        | OrderState::Accepted
                        | OrderState::PartiallyFilled
                        | OrderState::PendingCancel
                ) =>
            {
                self.state = OrderState::Expired;
                TransitionResult::StateChanged
            }
            OrderTransition::Fill { qty }
                if matches!(
                    old_state,
                    OrderState::Accepted | OrderState::PartiallyFilled | OrderState::PendingCancel
                ) =>
            {
                if !qty.is_finite() || qty <= 0.0 {
                    return Err(OrderStateError::InvalidFillQuantity);
                }
                let tolerance = f64::EPSILON * self.request.qty.abs().max(1.0) * 8.0;
                if qty - self.leaves_qty > tolerance {
                    return Err(OrderStateError::Overfill);
                }
                let applied_qty = qty.min(self.leaves_qty);
                self.filled_qty += applied_qty;
                self.leaves_qty -= applied_qty;
                if self.leaves_qty <= tolerance {
                    self.leaves_qty = 0.0;
                    self.state = OrderState::Filled;
                    TransitionResult::StateChanged
                } else if old_state == OrderState::PendingCancel {
                    // The exchange fill raced with an in-flight cancel. The cancel remains pending.
                    TransitionResult::PartialFillWhileCancelPending
                } else {
                    self.state = OrderState::PartiallyFilled;
                    TransitionResult::StateChanged
                }
            }
            _ => {
                return Err(OrderStateError::InvalidTransition {
                    state: old_state,
                    transition,
                });
            }
        };
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        backtest::execution::{InstrumentId, OrderOrigin, VenueId},
        types::{OrdType, Side, TimeInForce},
    };

    fn order(qty: f64) -> ExecutionOrder {
        ExecutionOrder::new(ExecutionOrderRequest {
            client_order_id: 1,
            venue_id: VenueId(2),
            instrument_id: InstrumentId(3),
            price: 100.0,
            qty,
            side: Side::Buy,
            time_in_force: TimeInForce::GTC,
            order_type: OrdType::Limit,
            reduce_only: false,
            origin: OrderOrigin::Strategy,
            local_submit_ts: 10,
        })
    }

    #[test]
    fn accepts_multiple_independent_partial_fills() {
        let mut order = order(10.0);
        order.transition(OrderTransition::Submit).unwrap();
        order.transition(OrderTransition::Accept).unwrap();
        order
            .transition(OrderTransition::Fill { qty: 3.0 })
            .unwrap();
        assert_eq!(order.state, OrderState::PartiallyFilled);
        assert_eq!(order.filled_qty, 3.0);
        assert_eq!(order.leaves_qty, 7.0);
        order
            .transition(OrderTransition::Fill { qty: 7.0 })
            .unwrap();
        assert_eq!(order.state, OrderState::Filled);
        assert_eq!(order.filled_qty, 10.0);
        assert_eq!(order.leaves_qty, 0.0);
    }

    #[test]
    fn fill_can_race_with_pending_cancel_without_losing_cancel() {
        let mut order = order(10.0);
        order.transition(OrderTransition::Submit).unwrap();
        order.transition(OrderTransition::Accept).unwrap();
        order.transition(OrderTransition::RequestCancel).unwrap();
        assert_eq!(
            order.transition(OrderTransition::Fill { qty: 4.0 }),
            Ok(TransitionResult::PartialFillWhileCancelPending)
        );
        assert_eq!(order.state, OrderState::PendingCancel);
        assert_eq!(order.filled_qty, 4.0);
        order.transition(OrderTransition::Cancel).unwrap();
        assert_eq!(order.state, OrderState::Canceled);
        assert_eq!(order.leaves_qty, 6.0);
    }

    #[test]
    fn rejects_overfill_and_terminal_transitions() {
        let mut order = order(1.0);
        order.transition(OrderTransition::Submit).unwrap();
        order.transition(OrderTransition::Accept).unwrap();
        assert_eq!(
            order.transition(OrderTransition::Fill { qty: 2.0 }),
            Err(OrderStateError::Overfill)
        );
        order
            .transition(OrderTransition::Fill { qty: 1.0 })
            .unwrap();
        assert!(order.state.is_terminal());
        assert!(matches!(
            order.transition(OrderTransition::RequestCancel),
            Err(OrderStateError::InvalidTransition { .. })
        ));
    }
}
