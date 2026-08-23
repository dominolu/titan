use super::{CurrencyId, ExecutionOrder, InstrumentSpec, ProposedFill};
use crate::backtest::models::FeeModel;

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

    fn reset(&mut self) {}
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FeeRoundingMode {
    Nearest,
    TowardZero,
    Floor,
    Ceil,
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
}
