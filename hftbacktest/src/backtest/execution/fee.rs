use super::{CurrencyId, ExecutionOrder, InstrumentSpec, ProposedFill};
use crate::backtest::models::FeeModel;
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FeeCharge {
    /// Positive is a cost; negative is a rebate.
    pub amount: f64,
    pub currency: CurrencyId,
}

pub struct FillFeeContext<'a> {
    pub order: &'a ExecutionOrder,
    pub fill: ProposedFill,
    pub instrument: &'a InstrumentSpec,
    pub trade_value: f64,
}

pub trait ExecutionFeeModel {
    fn charge(&mut self, context: &FillFeeContext<'_>) -> FeeCharge;

    fn model_id(&self) -> &'static str {
        "custom-fee"
    }

    fn model_version(&self) -> u32 {
        1
    }

    fn config_hash(&self) -> u64 {
        stable_fee_hash(
            self.model_id().as_bytes(),
            &[u64::from(self.model_version())],
        )
    }

    fn reset(&mut self) {}
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FeeRoundingMode {
    Nearest,
    TowardZero,
    Floor,
    Ceil,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FixedFeeAssessment {
    FirstFill,
    EveryFill,
}

pub struct FixedFeeModel {
    pub amount: f64,
    pub currency: CurrencyId,
    pub assessment: FixedFeeAssessment,
    charged_orders: BTreeSet<u64>,
}

impl FixedFeeModel {
    pub fn new(amount: f64, currency: CurrencyId, assessment: FixedFeeAssessment) -> Option<Self> {
        amount.is_finite().then_some(Self {
            amount,
            currency,
            assessment,
            charged_orders: BTreeSet::new(),
        })
    }
}

impl ExecutionFeeModel for FixedFeeModel {
    fn charge(&mut self, context: &FillFeeContext<'_>) -> FeeCharge {
        let should_charge = match self.assessment {
            FixedFeeAssessment::EveryFill => true,
            FixedFeeAssessment::FirstFill => self
                .charged_orders
                .insert(context.order.request.client_order_id),
        };
        FeeCharge {
            amount: if should_charge { self.amount } else { 0.0 },
            currency: self.currency,
        }
    }

    fn model_id(&self) -> &'static str {
        "fixed-fee"
    }

    fn config_hash(&self) -> u64 {
        stable_fee_hash(
            self.model_id().as_bytes(),
            &[
                self.amount.to_bits(),
                u64::from(self.currency.0),
                self.assessment as u64,
            ],
        )
    }

    fn reset(&mut self) {
        self.charged_orders.clear();
    }
}

pub struct RoundedFeeModel<F> {
    inner: F,
    increment: f64,
    mode: FeeRoundingMode,
}

impl<F> RoundedFeeModel<F> {
    pub fn new(inner: F, increment: f64, mode: FeeRoundingMode) -> Option<Self> {
        (increment.is_finite() && increment > 0.0).then_some(Self {
            inner,
            increment,
            mode,
        })
    }
}

impl<F: ExecutionFeeModel> ExecutionFeeModel for RoundedFeeModel<F> {
    fn charge(&mut self, context: &FillFeeContext<'_>) -> FeeCharge {
        let mut charge = self.inner.charge(context);
        let units = charge.amount / self.increment;
        let rounded = match self.mode {
            FeeRoundingMode::Nearest => units.round(),
            FeeRoundingMode::TowardZero => units.trunc(),
            FeeRoundingMode::Floor => units.floor(),
            FeeRoundingMode::Ceil => units.ceil(),
        };
        charge.amount = rounded * self.increment;
        charge
    }

    fn reset(&mut self) {
        self.inner.reset();
    }

    fn model_id(&self) -> &'static str {
        "rounded-fee"
    }

    fn config_hash(&self) -> u64 {
        stable_fee_hash(
            self.model_id().as_bytes(),
            &[
                self.inner.config_hash(),
                self.increment.to_bits(),
                self.mode as u64,
            ],
        )
    }
}

impl<F> ExecutionFeeModel for Box<F>
where
    F: ExecutionFeeModel + ?Sized,
{
    fn charge(&mut self, context: &FillFeeContext<'_>) -> FeeCharge {
        (**self).charge(context)
    }

    fn reset(&mut self) {
        (**self).reset();
    }

    fn model_id(&self) -> &'static str {
        (**self).model_id()
    }

    fn model_version(&self) -> u32 {
        (**self).model_version()
    }

    fn config_hash(&self) -> u64 {
        (**self).config_hash()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct NoFee {
    pub currency: CurrencyId,
}

impl ExecutionFeeModel for NoFee {
    fn charge(&mut self, _context: &FillFeeContext<'_>) -> FeeCharge {
        FeeCharge {
            amount: 0.0,
            currency: self.currency,
        }
    }

    fn model_id(&self) -> &'static str {
        "no-fee"
    }

    fn config_hash(&self) -> u64 {
        stable_fee_hash(self.model_id().as_bytes(), &[u64::from(self.currency.0)])
    }
}

#[derive(Clone, Copy, Debug)]
pub struct RateFeeModel {
    pub maker_rate: f64,
    pub taker_rate: f64,
    pub currency: CurrencyId,
}

#[derive(Clone, Debug)]
pub struct LegacyExecutionFeeAdapter<F> {
    inner: F,
    currency: CurrencyId,
}

impl<F> LegacyExecutionFeeAdapter<F> {
    pub fn new(inner: F, currency: CurrencyId) -> Self {
        Self { inner, currency }
    }
}

impl<F> ExecutionFeeModel for LegacyExecutionFeeAdapter<F>
where
    F: FeeModel,
{
    fn charge(&mut self, context: &FillFeeContext<'_>) -> FeeCharge {
        FeeCharge {
            amount: self.inner.amount_fields(
                context.order.request.side,
                context.fill.maker,
                context.fill.qty,
                context.trade_value,
            ),
            currency: self.currency,
        }
    }
}

impl ExecutionFeeModel for RateFeeModel {
    fn charge(&mut self, context: &FillFeeContext<'_>) -> FeeCharge {
        let rate = if context.fill.maker {
            self.maker_rate
        } else {
            self.taker_rate
        };
        FeeCharge {
            amount: context.trade_value * rate,
            currency: self.currency,
        }
    }

    fn model_id(&self) -> &'static str {
        "rate-fee"
    }

    fn config_hash(&self) -> u64 {
        stable_fee_hash(
            self.model_id().as_bytes(),
            &[
                self.maker_rate.to_bits(),
                self.taker_rate.to_bits(),
                u64::from(self.currency.0),
            ],
        )
    }
}

fn stable_fee_hash(id: &[u8], values: &[u64]) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in id {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    for value in values {
        for byte in value.to_le_bytes() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        backtest::execution::{
            CashFlowMode, ExecutionOrderRequest, InstrumentId, InstrumentType, OrderOrigin,
            ProposedFill, VenueId,
        },
        types::{OrdType, Side, TimeInForce},
    };

    fn context<'a>(order: &'a ExecutionOrder, spec: &'a InstrumentSpec) -> FillFeeContext<'a> {
        FillFeeContext {
            order,
            fill: ProposedFill {
                exchange_ts: 1,
                price: 10.0,
                qty: 1.0,
                maker: false,
            },
            instrument: spec,
            trade_value: 10.0,
        }
    }

    #[test]
    fn fixed_fee_assessment_is_explicit_resettable_and_hashed() {
        let spec = InstrumentSpec {
            instrument_id: InstrumentId(1),
            asset_no: 0,
            venue_id: VenueId(1),
            tick_size: 0.01,
            lot_size: 1.0,
            min_qty: 1.0,
            max_qty: 100.0,
            min_notional: 0.0,
            contract_size: 1.0,
            price_currency: CurrencyId(1),
            settlement_currency: CurrencyId(1),
            margin_currency: CurrencyId(1),
            instrument_type: InstrumentType::Spot,
            cash_flow_mode: CashFlowMode::LegacyNotional,
            version: 1,
        };
        let order = ExecutionOrder::new(ExecutionOrderRequest {
            client_order_id: 7,
            venue_id: VenueId(1),
            instrument_id: InstrumentId(1),
            price: 10.0,
            qty: 2.0,
            side: Side::Buy,
            time_in_force: TimeInForce::GTC,
            order_type: OrdType::Limit,
            reduce_only: false,
            origin: OrderOrigin::Strategy,
            local_submit_ts: 0,
        });
        let mut model =
            FixedFeeModel::new(0.25, CurrencyId(1), FixedFeeAssessment::FirstFill).unwrap();
        let hash = model.config_hash();
        assert_eq!(model.charge(&context(&order, &spec)).amount, 0.25);
        assert_eq!(model.charge(&context(&order, &spec)).amount, 0.0);
        model.reset();
        assert_eq!(model.charge(&context(&order, &spec)).amount, 0.25);
        assert_ne!(
            hash,
            FixedFeeModel::new(0.25, CurrencyId(1), FixedFeeAssessment::EveryFill)
                .unwrap()
                .config_hash()
        );
    }
}
