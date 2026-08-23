use std::collections::BTreeMap;

use super::{
    ExchangeAccountState, ExecutionOrderRequest, InstrumentId, MarketStatus, PortfolioLedger,
    VenueAccount, VenueId,
};
use crate::types::OrderId;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RiskReason {
    InvalidInstrument,
    InvalidPrice,
    InvalidQuantity,
    DuplicateOrderId,
    PositionLimit,
    NotionalLimit,
    InsufficientBalance,
    InsufficientMargin,
    ReduceOnlyViolation,
    MarketClosed,
    Custom(u32),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RiskDecision {
    Allow,
    Reject { reason: RiskReason },
}

pub trait LocalPreTradeRisk {
    fn check(
        &mut self,
        request: &ExecutionOrderRequest,
        portfolio: &PortfolioLedger,
    ) -> RiskDecision;

    fn reset(&mut self) {}
}

pub trait ExchangeRisk {
    fn check_arrival(
        &mut self,
        request: &ExecutionOrderRequest,
        account: &ExchangeAccountState,
    ) -> RiskDecision;

    fn reset(&mut self) {}
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RiskAction {
    Cancel {
        venue_id: VenueId,
        instrument_id: InstrumentId,
        order_id: OrderId,
        reason: RiskReason,
    },
    Liquidate {
        venue_id: VenueId,
        instrument_id: InstrumentId,
        reason: RiskReason,
    },
}

#[derive(Debug, Default)]
pub struct RiskActionSink {
    actions: Vec<RiskAction>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct InstrumentRiskMetrics {
    pub unrealized_pnl: f64,
    pub initial_margin: f64,
    pub maintenance_margin: f64,
}

impl RiskActionSink {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            actions: Vec::with_capacity(capacity),
        }
    }

    pub fn push(&mut self, action: RiskAction) {
        self.actions.push(action);
    }

    pub fn as_slice(&self) -> &[RiskAction] {
        &self.actions
    }

    pub fn clear(&mut self) {
        self.actions.clear();
    }
}

pub trait PostTradeRisk {
    fn on_account_change(&mut self, account: &ExchangeAccountState, out: &mut RiskActionSink);

    fn instrument_metrics(
        &self,
        _account: &VenueAccount,
        _instrument_id: InstrumentId,
    ) -> InstrumentRiskMetrics {
        InstrumentRiskMetrics::default()
    }

    fn reset(&mut self) {}
}

/// Venue-scoped risk implementation used at exchange arrival and after account mutation.
/// Keeping both hooks on one object prevents margin configuration from drifting between stages.
pub trait VenueRisk: ExchangeRisk + PostTradeRisk {
    fn reset_all(&mut self) {
        ExchangeRisk::reset(self);
        PostTradeRisk::reset(self);
    }
}

impl<T> VenueRisk for T where T: ExchangeRisk + PostTradeRisk {}

#[derive(Clone, Copy, Debug, Default)]
pub struct AllowAllRisk;

impl LocalPreTradeRisk for AllowAllRisk {
    fn check(
        &mut self,
        _request: &ExecutionOrderRequest,
        _portfolio: &PortfolioLedger,
    ) -> RiskDecision {
        RiskDecision::Allow
    }
}

impl ExchangeRisk for AllowAllRisk {
    fn check_arrival(
        &mut self,
        _request: &ExecutionOrderRequest,
        _account: &ExchangeAccountState,
    ) -> RiskDecision {
        RiskDecision::Allow
    }
}

impl PostTradeRisk for AllowAllRisk {
    fn on_account_change(&mut self, _account: &ExchangeAccountState, _out: &mut RiskActionSink) {}
}

/// Composable exchange-stage market-status gate. Dynamic status events update this wrapper at
/// their scheduler boundary; the wrapped margin/venue model remains unchanged.
pub struct MarketStatusRisk<R> {
    inner: R,
    initial: BTreeMap<InstrumentId, MarketStatus>,
    current: BTreeMap<InstrumentId, MarketStatus>,
}

impl<R> MarketStatusRisk<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            initial: BTreeMap::new(),
            current: BTreeMap::new(),
        }
    }

    pub fn insert_initial(&mut self, instrument_id: InstrumentId, status: MarketStatus) {
        self.initial.insert(instrument_id, status);
        self.current.insert(instrument_id, status);
    }

    pub fn update(&mut self, instrument_id: InstrumentId, status: MarketStatus) -> bool {
        let Some(current) = self.current.get_mut(&instrument_id) else {
            return false;
        };
        *current = status;
        true
    }
}

impl<R: ExchangeRisk> ExchangeRisk for MarketStatusRisk<R> {
    fn check_arrival(
        &mut self,
        request: &ExecutionOrderRequest,
        account: &ExchangeAccountState,
    ) -> RiskDecision {
        if self.current.get(&request.instrument_id) != Some(&MarketStatus::Open) {
            return RiskDecision::Reject {
                reason: RiskReason::MarketClosed,
            };
        }
        self.inner.check_arrival(request, account)
    }

    fn reset(&mut self) {
        self.current.clone_from(&self.initial);
        self.inner.reset();
    }
}

impl<R: PostTradeRisk> PostTradeRisk for MarketStatusRisk<R> {
    fn on_account_change(&mut self, account: &ExchangeAccountState, out: &mut RiskActionSink) {
        self.inner.on_account_change(account, out);
    }

    fn reset(&mut self) {
        self.inner.reset();
    }

    fn instrument_metrics(
        &self,
        account: &VenueAccount,
        instrument_id: InstrumentId,
    ) -> InstrumentRiskMetrics {
        self.inner.instrument_metrics(account, instrument_id)
    }
}

/// Owns the three explicit risk stages. Routing code calls each method at its named boundary.
pub struct RiskPipeline<L, E, P> {
    pub local: L,
    pub exchange: E,
    pub post_trade: P,
}

impl<L, E, P> RiskPipeline<L, E, P>
where
    L: LocalPreTradeRisk,
    E: ExchangeRisk,
    P: PostTradeRisk,
{
    pub fn local_check(
        &mut self,
        request: &ExecutionOrderRequest,
        portfolio: &PortfolioLedger,
    ) -> RiskDecision {
        self.local.check(request, portfolio)
    }

    pub fn exchange_check(
        &mut self,
        request: &ExecutionOrderRequest,
        account: &ExchangeAccountState,
    ) -> RiskDecision {
        self.exchange.check_arrival(request, account)
    }

    pub fn post_trade(&mut self, account: &ExchangeAccountState, out: &mut RiskActionSink) {
        self.post_trade.on_account_change(account, out);
    }

    pub fn reset(&mut self) {
        self.local.reset();
        self.exchange.reset();
        self.post_trade.reset();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{OrdType, Side, TimeInForce};

    fn request() -> ExecutionOrderRequest {
        ExecutionOrderRequest {
            client_order_id: 1,
            venue_id: VenueId(2),
            instrument_id: InstrumentId(3),
            price: 100.0,
            qty: 1.0,
            side: Side::Buy,
            time_in_force: TimeInForce::GTC,
            order_type: OrdType::Limit,
            reduce_only: false,
            origin: super::super::OrderOrigin::Strategy,
            local_submit_ts: 10,
        }
    }

    #[test]
    fn allow_all_keeps_three_stages_explicit() {
        let mut pipeline = RiskPipeline {
            local: AllowAllRisk,
            exchange: AllowAllRisk,
            post_trade: AllowAllRisk,
        };
        let request = request();
        let portfolio = PortfolioLedger::default();
        let exchange = ExchangeAccountState::new(VenueId(2));
        let mut actions = RiskActionSink::default();

        assert_eq!(
            pipeline.local_check(&request, &portfolio),
            RiskDecision::Allow
        );
        assert_eq!(
            pipeline.exchange_check(&request, &exchange),
            RiskDecision::Allow
        );
        pipeline.post_trade(&exchange, &mut actions);
        assert!(actions.as_slice().is_empty());
    }

    #[test]
    fn market_status_gate_rejects_halted_and_reset_restores_initial_state() {
        let mut risk = MarketStatusRisk::new(AllowAllRisk);
        risk.insert_initial(InstrumentId(3), MarketStatus::Open);
        let account = ExchangeAccountState::new(VenueId(2));
        assert_eq!(
            risk.check_arrival(&request(), &account),
            RiskDecision::Allow
        );
        assert!(risk.update(InstrumentId(3), MarketStatus::Halted));
        assert_eq!(
            risk.check_arrival(&request(), &account),
            RiskDecision::Reject {
                reason: RiskReason::MarketClosed
            }
        );
        ExchangeRisk::reset(&mut risk);
        assert_eq!(
            risk.check_arrival(&request(), &account),
            RiskDecision::Allow
        );
    }
}
