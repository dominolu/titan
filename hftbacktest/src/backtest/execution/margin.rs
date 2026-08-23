use std::collections::BTreeMap;

use crate::types::Side;

use super::{
    CurrencyId, ExchangeAccountState, ExchangeRisk, ExecutionOrderRequest, InstrumentId,
    InstrumentRiskMetrics, InstrumentSpec, InstrumentType, PostTradeRisk, RiskAction,
    RiskActionSink, RiskDecision, RiskReason, VenueAccount, VenueId,
};

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MarginParameters {
    pub initial_margin_rate: f64,
    pub maintenance_margin_rate: f64,
    pub max_leverage: f64,
}

#[derive(Clone, Debug)]
struct MarginInstrument {
    spec: InstrumentSpec,
    params: MarginParameters,
    mark_price: f64,
    initial_mark_price: f64,
}

/// Basic deterministic cross-margin risk at VenueAccount scope. Every registered instrument
/// consumes the same collateral currency balance, allowing one fill to constrain another symbol.
#[derive(Clone, Debug)]
pub struct CrossMarginRisk {
    venue_id: VenueId,
    collateral_currency: CurrencyId,
    instruments: BTreeMap<InstrumentId, MarginInstrument>,
}

impl CrossMarginRisk {
    pub fn new(venue_id: VenueId, collateral_currency: CurrencyId) -> Self {
        Self {
            venue_id,
            collateral_currency,
            instruments: BTreeMap::new(),
        }
    }

    pub fn register(
        &mut self,
        spec: InstrumentSpec,
        params: MarginParameters,
        mark_price: f64,
    ) -> Result<(), RiskReason> {
        if spec.venue_id != self.venue_id
            || spec.margin_currency != self.collateral_currency
            || !mark_price.is_finite()
            || mark_price <= 0.0
            || !params.initial_margin_rate.is_finite()
            || params.initial_margin_rate <= 0.0
            || params.maintenance_margin_rate < 0.0
            || params.maintenance_margin_rate > params.initial_margin_rate
            || params.max_leverage <= 0.0
            || params.initial_margin_rate < 1.0 / params.max_leverage
        {
            return Err(RiskReason::Custom(1));
        }
        self.instruments.insert(
            spec.instrument_id,
            MarginInstrument {
                spec,
                params,
                mark_price,
                initial_mark_price: mark_price,
            },
        );
        Ok(())
    }

    pub fn update_mark(&mut self, instrument_id: InstrumentId, mark_price: f64) -> bool {
        if !mark_price.is_finite() || mark_price <= 0.0 {
            return false;
        }
        let Some(instrument) = self.instruments.get_mut(&instrument_id) else {
            return false;
        };
        instrument.mark_price = mark_price;
        true
    }

    fn notional(instrument: &MarginInstrument, qty: f64, price: f64) -> f64 {
        match instrument.spec.instrument_type {
            InstrumentType::Spot
            | InstrumentType::LinearFuture
            | InstrumentType::LinearPerpetual => qty.abs() * instrument.spec.contract_size * price,
            InstrumentType::InverseFuture | InstrumentType::InversePerpetual => {
                qty.abs() * instrument.spec.contract_size / price
            }
        }
    }

    fn required_margin(
        &self,
        account: &ExchangeAccountState,
        proposed: Option<(InstrumentId, f64, f64)>,
        maintenance: bool,
    ) -> f64 {
        self.instruments
            .iter()
            .map(|(id, instrument)| {
                let mut qty = account.account().position(*id).qty;
                let mut price = instrument.mark_price;
                if let Some((proposed_id, proposed_qty, proposed_price)) = proposed
                    && proposed_id == *id
                {
                    qty += proposed_qty;
                    price = proposed_price;
                }
                let rate = if maintenance {
                    instrument.params.maintenance_margin_rate
                } else {
                    instrument.params.initial_margin_rate
                };
                Self::notional(instrument, qty, price) * rate
            })
            .sum()
    }
}

impl ExchangeRisk for CrossMarginRisk {
    fn check_arrival(
        &mut self,
        request: &ExecutionOrderRequest,
        account: &ExchangeAccountState,
    ) -> RiskDecision {
        if request.venue_id != self.venue_id {
            return RiskDecision::Reject {
                reason: RiskReason::Custom(2),
            };
        }
        let Some(instrument) = self.instruments.get(&request.instrument_id) else {
            return RiskDecision::Reject {
                reason: RiskReason::Custom(3),
            };
        };
        let signed_qty = match request.side {
            Side::Buy => request.qty,
            Side::Sell => -request.qty,
            _ => {
                return RiskDecision::Reject {
                    reason: RiskReason::Custom(4),
                };
            }
        };
        let old_qty = account.account().position(request.instrument_id).qty;
        let new_qty = old_qty + signed_qty;
        if request.reduce_only && (old_qty == 0.0 || new_qty.abs() >= old_qty.abs()) {
            return RiskDecision::Reject {
                reason: RiskReason::ReduceOnlyViolation,
            };
        }
        let price = if request.price.is_finite() && request.price > 0.0 {
            request.price
        } else {
            instrument.mark_price
        };
        let required = self.required_margin(
            account,
            Some((request.instrument_id, signed_qty, price)),
            false,
        );
        if account.account().balance(self.collateral_currency) < required {
            RiskDecision::Reject {
                reason: RiskReason::InsufficientMargin,
            }
        } else {
            RiskDecision::Allow
        }
    }

    fn reset(&mut self) {
        for instrument in self.instruments.values_mut() {
            instrument.mark_price = instrument.initial_mark_price;
        }
    }
}

impl PostTradeRisk for CrossMarginRisk {
    fn on_account_change(&mut self, account: &ExchangeAccountState, out: &mut RiskActionSink) {
        let maintenance = self.required_margin(account, None, true);
        if account.account().balance(self.collateral_currency) >= maintenance {
            return;
        }
        for instrument_id in self.instruments.keys().copied() {
            if account.account().position(instrument_id).qty != 0.0 {
                out.push(RiskAction::Liquidate {
                    venue_id: self.venue_id,
                    instrument_id,
                    reason: RiskReason::InsufficientMargin,
                });
            }
        }
    }

    fn instrument_metrics(
        &self,
        account: &VenueAccount,
        instrument_id: InstrumentId,
    ) -> InstrumentRiskMetrics {
        let Some(instrument) = self.instruments.get(&instrument_id) else {
            return InstrumentRiskMetrics::default();
        };
        let position = account.position(instrument_id);
        let unrealized_pnl = if position.qty == 0.0 || position.avg_entry_price <= 0.0 {
            0.0
        } else {
            match instrument.spec.instrument_type {
                InstrumentType::Spot
                | InstrumentType::LinearFuture
                | InstrumentType::LinearPerpetual => {
                    (instrument.mark_price - position.avg_entry_price)
                        * position.qty
                        * instrument.spec.contract_size
                }
                InstrumentType::InverseFuture | InstrumentType::InversePerpetual => {
                    position.qty
                        * instrument.spec.contract_size
                        * (1.0 / position.avg_entry_price - 1.0 / instrument.mark_price)
                }
            }
        };
        let notional = Self::notional(instrument, position.qty, instrument.mark_price);
        InstrumentRiskMetrics {
            unrealized_pnl,
            initial_margin: notional * instrument.params.initial_margin_rate,
            maintenance_margin: notional * instrument.params.maintenance_margin_rate,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        backtest::execution::{AccountDelta, OrderOrigin},
        types::{OrdType, TimeInForce},
    };

    fn spec(id: u32) -> InstrumentSpec {
        InstrumentSpec {
            instrument_id: InstrumentId(id),
            asset_no: id,
            venue_id: VenueId(1),
            tick_size: 1.0,
            lot_size: 1.0,
            min_qty: 1.0,
            max_qty: 100.0,
            min_notional: 0.0,
            contract_size: 1.0,
            price_currency: CurrencyId(7),
            settlement_currency: CurrencyId(7),
            margin_currency: CurrencyId(7),
            instrument_type: InstrumentType::LinearPerpetual,
            cash_flow_mode: crate::backtest::execution::CashFlowMode::LegacyNotional,
            version: 1,
        }
    }

    fn request(id: u32, qty: f64, reduce_only: bool) -> ExecutionOrderRequest {
        ExecutionOrderRequest {
            client_order_id: id as u64,
            venue_id: VenueId(1),
            instrument_id: InstrumentId(id),
            price: 100.0,
            qty,
            side: Side::Buy,
            time_in_force: TimeInForce::GTC,
            order_type: OrdType::Limit,
            reduce_only,
            origin: OrderOrigin::Strategy,
            local_submit_ts: 0,
        }
    }

    #[test]
    fn two_instruments_share_collateral_and_reduce_only_uses_exchange_position() {
        let params = MarginParameters {
            initial_margin_rate: 0.1,
            maintenance_margin_rate: 0.05,
            max_leverage: 10.0,
        };
        let mut risk = CrossMarginRisk::new(VenueId(1), CurrencyId(7));
        risk.register(spec(1), params, 100.0).unwrap();
        risk.register(spec(2), params, 100.0).unwrap();
        let mut account = ExchangeAccountState::new(VenueId(1));
        account
            .account_mut()
            .set_balance(CurrencyId(7), 100.0)
            .unwrap();
        account
            .account_mut()
            .apply(AccountDelta {
                instrument_id: InstrumentId(1),
                position_delta: 5.0,
                trade_qty: 5.0,
                trade_value: 500.0,
                currency: CurrencyId(7),
                cash_delta: 0.0,
                fee: 0.0,
                funding: 0.0,
                execution_price: 100.0,
                realized_pnl: 0.0,
            })
            .unwrap();
        assert_eq!(
            risk.check_arrival(&request(2, 6.0, false), &account),
            RiskDecision::Reject {
                reason: RiskReason::InsufficientMargin
            }
        );
        assert_eq!(
            risk.check_arrival(&request(1, 1.0, true), &account),
            RiskDecision::Reject {
                reason: RiskReason::ReduceOnlyViolation
            }
        );
        assert!(risk.update_mark(InstrumentId(1), 110.0));
        let metrics = risk.instrument_metrics(account.account(), InstrumentId(1));
        assert_eq!(metrics.unrealized_pnl, 50.0);
        assert_eq!(metrics.initial_margin, 55.0);
        assert_eq!(metrics.maintenance_margin, 27.5);
    }
}
