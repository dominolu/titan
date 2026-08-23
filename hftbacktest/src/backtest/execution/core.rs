use crate::backtest::scheduler::GlobalScheduler;

use super::{
    ExchangeAccountState, ExecutionEventProjector, InstrumentSpec, PortfolioLedger, VenueId,
};

/// Instrument-level owner. The matcher remains generic so Tick hot paths can stay monomorphized.
pub struct InstrumentExecutionCore<M> {
    pub spec: InstrumentSpec,
    pub matcher: M,
}

impl<M> InstrumentExecutionCore<M> {
    pub fn new(spec: InstrumentSpec, matcher: M) -> Self {
        Self { spec, matcher }
    }
}

/// Venue-level owner. `I` is the caller-selected instrument container and `R` is the risk
/// pipeline. Keeping both generic permits static Tick layouts as well as heterogeneous enums.
pub struct VenueExecutionCore<I, R> {
    pub venue_id: VenueId,
    pub exchange_account: ExchangeAccountState,
    pub risk: R,
    pub instruments: I,
}

impl<I, R> VenueExecutionCore<I, R> {
    pub fn new(venue_id: VenueId, risk: R, instruments: I) -> Self {
        Self {
            venue_id,
            exchange_account: ExchangeAccountState::new(venue_id),
            risk,
            instruments,
        }
    }
}

/// Engine-level ownership root shared by backtest modes. This is intentionally an orchestration
/// skeleton: legacy Tick and Bar loops are attached through adapters in later migration phases.
pub struct SharedExecutionEngine<V, T> {
    pub scheduler: GlobalScheduler<T>,
    pub local_portfolio: PortfolioLedger,
    pub venues: V,
    pub projector: ExecutionEventProjector,
}

impl<V, T> SharedExecutionEngine<V, T> {
    pub fn new(venues: V, projected_event_capacity: usize) -> Self {
        Self {
            scheduler: GlobalScheduler::new(),
            local_portfolio: PortfolioLedger::default(),
            venues,
            projector: ExecutionEventProjector::with_capacity(projected_event_capacity),
        }
    }

    /// Resets engine-owned transient state. Venue matchers and risk models are reset by their
    /// adapters because their reset contracts are model-specific.
    pub fn reset_engine_state(&mut self) {
        self.scheduler.reset();
        self.local_portfolio.reset();
        self.projector.reset();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backtest::execution::{
        AllowAllRisk, CurrencyId, InstrumentId, InstrumentType, RiskPipeline,
    };

    fn spec(id: u32, asset_no: u32, venue_id: VenueId) -> InstrumentSpec {
        InstrumentSpec {
            instrument_id: InstrumentId(id),
            asset_no,
            venue_id,
            tick_size: 0.01,
            lot_size: 0.001,
            min_qty: 0.001,
            max_qty: 100.0,
            min_notional: 0.0,
            contract_size: 1.0,
            price_currency: CurrencyId(1),
            settlement_currency: CurrencyId(1),
            margin_currency: CurrencyId(1),
            instrument_type: InstrumentType::LinearPerpetual,
            cash_flow_mode: crate::backtest::execution::CashFlowMode::LegacyNotional,
            version: 1,
        }
    }

    #[test]
    fn ownership_keeps_account_at_venue_not_instrument() {
        let venue_id = VenueId(1);
        let instruments = vec![
            InstrumentExecutionCore::new(spec(10, 0, venue_id), "tick"),
            InstrumentExecutionCore::new(spec(11, 1, venue_id), "bar"),
        ];
        let risk = RiskPipeline {
            local: AllowAllRisk,
            exchange: AllowAllRisk,
            post_trade: AllowAllRisk,
        };
        let venue = VenueExecutionCore::new(venue_id, risk, instruments);
        let engine: SharedExecutionEngine<_, ()> = SharedExecutionEngine::new(vec![venue], 16);

        assert_eq!(engine.venues[0].instruments.len(), 2);
        assert_eq!(
            engine.venues[0].exchange_account.account().venue_id(),
            venue_id
        );
        assert!(engine.local_portfolio.venue(venue_id).is_none());
    }
}
