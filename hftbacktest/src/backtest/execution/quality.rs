use std::collections::BTreeMap;

use crate::types::{OrdType, Order, Side};

use super::{InstrumentSpec, ProposedFill};

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct LiquidityKey {
    pub market_event_id: u64,
    pub side: Side,
    pub price: f64,
}

impl Eq for LiquidityKey {}

impl Ord for LiquidityKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.market_event_id, self.side as i8)
            .cmp(&(other.market_event_id, other.side as i8))
            .then_with(|| self.price.total_cmp(&other.price))
    }
}

impl PartialOrd for LiquidityKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub trait LiquidityConsumptionModel {
    fn claim(&mut self, key: LiquidityKey, proposed_qty: f64, historical_qty: f64) -> f64;
    fn reset(&mut self);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionQualityIdentity {
    pub model_id: &'static str,
    pub version: u32,
    pub parameters_hash: u64,
    pub seed: Option<u64>,
}

#[derive(Default)]
pub struct DisabledLiquidityConsumption;

impl LiquidityConsumptionModel for DisabledLiquidityConsumption {
    fn claim(&mut self, _key: LiquidityKey, proposed_qty: f64, _historical_qty: f64) -> f64 {
        proposed_qty
    }

    fn reset(&mut self) {}
}

#[derive(Default)]
pub struct HistoricalLiquidityConsumption {
    consumed: BTreeMap<LiquidityKey, f64>,
}

impl LiquidityConsumptionModel for HistoricalLiquidityConsumption {
    fn claim(&mut self, key: LiquidityKey, proposed_qty: f64, historical_qty: f64) -> f64 {
        if proposed_qty <= 0.0 || historical_qty <= 0.0 {
            return 0.0;
        }
        let consumed = self.consumed.entry(key).or_default();
        let claimed = proposed_qty.min((historical_qty - *consumed).max(0.0));
        *consumed += claimed;
        claimed
    }

    fn reset(&mut self) {
        self.consumed.clear();
    }
}

pub struct FillQualityContext<'a> {
    pub fill: ProposedFill,
    pub side: Side,
    pub limit_price: Option<f64>,
    pub available_qty: f64,
    pub instrument: &'a InstrumentSpec,
}

pub trait ExecutionQualityModel {
    fn adjust(&mut self, context: FillQualityContext<'_>) -> Option<ProposedFill>;
    fn identity(&self) -> ExecutionQualityIdentity;
    fn reset(&mut self);
}

/// Object-safe optional adapter used only when a Tick matcher enables execution realism. The
/// default matcher keeps this slot empty, preserving its monomorphized no-model hot path.
pub trait TickExecutionReality {
    fn adjust(
        &mut self,
        market_event_id: u64,
        order: &Order,
        historical_qty: f64,
        fill: ProposedFill,
    ) -> Option<ProposedFill>;
    fn identity(&self) -> ExecutionQualityIdentity;
    fn reset(&mut self);
}

pub struct InstrumentExecutionReality<L, Q> {
    instrument: InstrumentSpec,
    pipeline: ExecutionRealityPipeline<L, Q>,
}

impl<L, Q> InstrumentExecutionReality<L, Q>
where
    L: LiquidityConsumptionModel,
    Q: ExecutionQualityModel,
{
    pub fn new(instrument: InstrumentSpec, liquidity: L, quality: Q) -> Self {
        Self {
            instrument,
            pipeline: ExecutionRealityPipeline::new(liquidity, quality),
        }
    }
}

impl<L, Q> TickExecutionReality for InstrumentExecutionReality<L, Q>
where
    L: LiquidityConsumptionModel,
    Q: ExecutionQualityModel,
{
    fn adjust(
        &mut self,
        market_event_id: u64,
        order: &Order,
        historical_qty: f64,
        fill: ProposedFill,
    ) -> Option<ProposedFill> {
        let limit_price = (order.order_type == OrdType::Limit).then(|| order.price());
        self.pipeline.adjust(
            LiquidityKey {
                market_event_id,
                side: order.side,
                price: fill.price,
            },
            historical_qty,
            FillQualityContext {
                fill,
                side: order.side,
                limit_price,
                available_qty: historical_qty,
                instrument: &self.instrument,
            },
        )
    }

    fn identity(&self) -> ExecutionQualityIdentity {
        self.pipeline.quality.identity()
    }

    fn reset(&mut self) {
        self.pipeline.reset();
    }
}

/// Composes historical-liquidity ownership and execution quality in the only safe order: claim
/// liquidity first, then let quality reduce quantity or worsen price within the order limit.
pub struct ExecutionRealityPipeline<L, Q> {
    liquidity: L,
    quality: Q,
}

impl<L, Q> ExecutionRealityPipeline<L, Q>
where
    L: LiquidityConsumptionModel,
    Q: ExecutionQualityModel,
{
    pub fn new(liquidity: L, quality: Q) -> Self {
        Self { liquidity, quality }
    }

    pub fn adjust(
        &mut self,
        key: LiquidityKey,
        historical_qty: f64,
        mut context: FillQualityContext<'_>,
    ) -> Option<ProposedFill> {
        let claimed = self.liquidity.claim(key, context.fill.qty, historical_qty);
        if claimed <= 0.0 {
            return None;
        }
        context.fill.qty = claimed;
        context.available_qty = context.available_qty.min(claimed);
        self.quality.adjust(context)
    }

    pub fn reset(&mut self) {
        self.liquidity.reset();
        self.quality.reset();
    }
}

#[derive(Default)]
pub struct IdentityExecutionQuality;

impl ExecutionQualityModel for IdentityExecutionQuality {
    fn adjust(&mut self, context: FillQualityContext<'_>) -> Option<ProposedFill> {
        Some(context.fill)
    }

    fn identity(&self) -> ExecutionQualityIdentity {
        ExecutionQualityIdentity {
            model_id: "identity-execution-quality",
            version: 1,
            parameters_hash: 0,
            seed: None,
        }
    }

    fn reset(&mut self) {}
}

/// Seeded fill-probability/slippage model with no implicit system randomness.
pub struct SeededExecutionQuality {
    initial_seed: u64,
    state: u64,
    fill_probability: f64,
    slippage_ticks: u32,
}

impl SeededExecutionQuality {
    pub fn new(seed: u64, fill_probability: f64, slippage_ticks: u32) -> Option<Self> {
        if !(0.0..=1.0).contains(&fill_probability) {
            return None;
        }
        Some(Self {
            initial_seed: seed,
            state: seed,
            fill_probability,
            slippage_ticks,
        })
    }

    fn uniform(&mut self) -> f64 {
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((self.state >> 11) as f64) * (1.0 / ((1_u64 << 53) as f64))
    }
}

impl ExecutionQualityModel for SeededExecutionQuality {
    fn adjust(&mut self, context: FillQualityContext<'_>) -> Option<ProposedFill> {
        if self.uniform() > self.fill_probability {
            return None;
        }
        let direction = match context.side {
            Side::Buy => 1.0,
            Side::Sell => -1.0,
            _ => return None,
        };
        let mut price = context.fill.price
            + direction * self.slippage_ticks as f64 * context.instrument.tick_size;
        if let Some(limit) = context.limit_price {
            price = match context.side {
                Side::Buy => price.min(limit),
                Side::Sell => price.max(limit),
                _ => price,
            };
        }
        let qty = context.fill.qty.min(context.available_qty);
        if qty <= 0.0 {
            return None;
        }
        Some(ProposedFill {
            price,
            qty,
            ..context.fill
        })
    }

    fn identity(&self) -> ExecutionQualityIdentity {
        let mut hash = 0xcbf29ce484222325_u64;
        for byte in self
            .fill_probability
            .to_bits()
            .to_le_bytes()
            .into_iter()
            .chain(self.slippage_ticks.to_le_bytes())
        {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        ExecutionQualityIdentity {
            model_id: "seeded-probability-slippage",
            version: 1,
            parameters_hash: hash,
            seed: Some(self.initial_seed),
        }
    }

    fn reset(&mut self) {
        self.state = self.initial_seed;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backtest::execution::{CurrencyId, InstrumentId, InstrumentType, VenueId};

    #[test]
    fn historical_quantity_cannot_be_consumed_twice_and_reset_replays() {
        let key = LiquidityKey {
            market_event_id: 1,
            side: Side::Buy,
            price: 100.0,
        };
        let mut model = HistoricalLiquidityConsumption::default();
        assert_eq!(model.claim(key, 4.0, 5.0), 4.0);
        assert_eq!(model.claim(key, 4.0, 5.0), 1.0);
        model.reset();
        assert_eq!(model.claim(key, 4.0, 5.0), 4.0);
    }

    #[test]
    fn seeded_quality_replays_and_never_breaks_limit_or_liquidity() {
        let spec = InstrumentSpec {
            instrument_id: InstrumentId(1),
            asset_no: 0,
            venue_id: VenueId(1),
            tick_size: 1.0,
            lot_size: 1.0,
            min_qty: 1.0,
            max_qty: 10.0,
            min_notional: 0.0,
            contract_size: 1.0,
            price_currency: CurrencyId(1),
            settlement_currency: CurrencyId(1),
            margin_currency: CurrencyId(1),
            instrument_type: InstrumentType::Spot,
            cash_flow_mode: crate::backtest::execution::CashFlowMode::LegacyNotional,
            version: 1,
        };
        let mut model = SeededExecutionQuality::new(9, 1.0, 3).unwrap();
        let context = || FillQualityContext {
            fill: ProposedFill {
                exchange_ts: 1,
                price: 100.0,
                qty: 5.0,
                maker: false,
            },
            side: Side::Buy,
            limit_price: Some(101.0),
            available_qty: 2.0,
            instrument: &spec,
        };
        let first = model.adjust(context()).unwrap();
        let identity = model.identity();
        assert_eq!(identity.seed, Some(9));
        assert_ne!(identity.parameters_hash, 0);
        model.reset();
        assert_eq!(model.adjust(context()).unwrap(), first);
        assert_eq!((first.price, first.qty), (101.0, 2.0));

        let key = LiquidityKey {
            market_event_id: 77,
            side: Side::Buy,
            price: 100.0,
        };
        let mut pipeline = ExecutionRealityPipeline::new(
            HistoricalLiquidityConsumption::default(),
            IdentityExecutionQuality,
        );
        let make_context = || FillQualityContext {
            fill: ProposedFill {
                exchange_ts: 2,
                price: 100.0,
                qty: 4.0,
                maker: false,
            },
            side: Side::Buy,
            limit_price: Some(101.0),
            available_qty: 4.0,
            instrument: &spec,
        };
        assert_eq!(pipeline.adjust(key, 5.0, make_context()).unwrap().qty, 4.0);
        assert_eq!(pipeline.adjust(key, 5.0, make_context()).unwrap().qty, 1.0);
        assert!(pipeline.adjust(key, 5.0, make_context()).is_none());
        pipeline.reset();
        assert_eq!(pipeline.adjust(key, 5.0, make_context()).unwrap().qty, 4.0);
    }
}
