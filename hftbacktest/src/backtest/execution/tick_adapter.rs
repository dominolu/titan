use thiserror::Error;

use crate::types::{Order, Status};

use super::{ExecutionReason, MatchOutcome, ProposedFill};

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum LegacyTickAdapterError {
    #[error("legacy order response has no canonical outcome")]
    UnsupportedResponse,
    #[error("legacy fill response has invalid execution fields")]
    InvalidFill,
}

/// Behaviour-preserving bridge for current L2/L3 processors. The legacy processors remain the
/// source of matching truth during P0-B; their now-independent responses are normalized into the
/// shared `MatchOutcome` vocabulary before account/projector migration.
#[derive(Clone, Copy, Debug, Default)]
pub struct LegacyTickOutcomeAdapter;

impl LegacyTickOutcomeAdapter {
    pub fn adapt(&self, order: &Order) -> Result<MatchOutcome, LegacyTickAdapterError> {
        if order.req == Status::Rejected {
            return Ok(MatchOutcome::Rejected {
                exchange_ts: order.exch_timestamp,
                reason: ExecutionReason::ExchangeRisk,
            });
        }
        if order.exec_qty > 0.0 && matches!(order.status, Status::PartiallyFilled | Status::Filled)
        {
            let price = order.exec_price();
            if !price.is_finite() || price <= 0.0 || !order.exec_qty.is_finite() {
                return Err(LegacyTickAdapterError::InvalidFill);
            }
            return Ok(MatchOutcome::Fill(ProposedFill {
                exchange_ts: order.exch_timestamp,
                price,
                qty: order.exec_qty,
                maker: order.maker,
            }));
        }
        match order.status {
            Status::New => Ok(MatchOutcome::Accepted {
                exchange_ts: order.exch_timestamp,
            }),
            Status::Canceled => Ok(MatchOutcome::Canceled {
                exchange_ts: order.exch_timestamp,
            }),
            Status::Expired => Ok(MatchOutcome::Expired {
                exchange_ts: order.exch_timestamp,
            }),
            Status::Rejected => Ok(MatchOutcome::Rejected {
                exchange_ts: order.exch_timestamp,
                reason: ExecutionReason::Unknown(0),
            }),
            _ => Err(LegacyTickAdapterError::UnsupportedResponse),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{OrdType, Side, TimeInForce};

    fn response(status: Status, exec_qty: f64) -> Order {
        let mut order = Order::new(
            1,
            100,
            1.0,
            5.0,
            Side::Buy,
            OrdType::Limit,
            TimeInForce::GTC,
        );
        order.status = status;
        order.exch_timestamp = 10;
        order.exec_price_tick = 101;
        order.exec_qty = exec_qty;
        order.leaves_qty = 5.0 - exec_qty;
        order
    }

    #[test]
    fn maps_each_legacy_partial_response_to_one_fill() {
        let adapter = LegacyTickOutcomeAdapter;
        let first = adapter
            .adapt(&response(Status::PartiallyFilled, 3.0))
            .unwrap();
        let second = adapter.adapt(&response(Status::Filled, 2.0)).unwrap();
        assert_eq!(
            first,
            MatchOutcome::Fill(ProposedFill {
                exchange_ts: 10,
                price: 101.0,
                qty: 3.0,
                maker: false,
            })
        );
        assert_eq!(
            second,
            MatchOutcome::Fill(ProposedFill {
                exchange_ts: 10,
                price: 101.0,
                qty: 2.0,
                maker: false,
            })
        );
    }

    #[test]
    fn maps_order_lifecycle_responses() {
        let adapter = LegacyTickOutcomeAdapter;
        assert!(matches!(
            adapter.adapt(&response(Status::New, 0.0)),
            Ok(MatchOutcome::Accepted { .. })
        ));
        assert!(matches!(
            adapter.adapt(&response(Status::Canceled, 0.0)),
            Ok(MatchOutcome::Canceled { .. })
        ));
        assert!(matches!(
            adapter.adapt(&response(Status::Expired, 0.0)),
            Ok(MatchOutcome::Expired { .. })
        ));
    }
}
