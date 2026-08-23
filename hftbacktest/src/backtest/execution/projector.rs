use crate::{
    backtest::scheduler::TimerEvent,
    types::{OrderId, Side, Status},
};

use super::{AccountDelta, AccountReport, FundingReport, InstrumentId, LocalAccountView, VenueId};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionReportKind {
    Accepted,
    Rejected,
    Canceled,
    Expired,
    Fill,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionReason {
    None,
    LocalRisk,
    ExchangeRisk,
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
    InsufficientLiquidity,
    Expired,
    UserCanceled,
    Unknown(u32),
}

/// Canonical report consumed by the same projector for backtest and live adapters.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ExecutionReport {
    pub kind: ExecutionReportKind,
    pub reason: ExecutionReason,
    pub venue_id: VenueId,
    pub instrument_id: InstrumentId,
    pub asset_no: u32,
    pub order_id: OrderId,
    /// Exchange-assigned identifier, or zero before venue acceptance.
    pub venue_order_id: u64,
    pub exchange_ts: i64,
    pub delivery_ts: i64,
    pub sequence: u64,
    pub status: Status,
    pub side: Side,
    pub order_price: f64,
    pub order_qty: f64,
    pub exec_price: f64,
    pub exec_qty: f64,
    pub maker: bool,
    pub account_delta: Option<AccountDelta>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProjectedEventKind {
    Order,
    Filled,
    Position,
}

/// A lightweight projection descriptor. The runtime ABI adapter uses the canonical report and
/// local account view to populate its preallocated POD buffers in this declared order.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ProjectedEvent {
    pub kind: ProjectedEventKind,
    pub report: ExecutionReport,
    pub visible_position: f64,
}

#[derive(Debug, Default)]
pub struct ExecutionEventProjector {
    events: Vec<ProjectedEvent>,
    funding_events: Vec<ProjectedFundingEvent>,
    timer_events: Vec<TimerEvent>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ProjectedFundingEvent {
    pub report: FundingReport,
    pub visible_balance: f64,
    pub visible_funding: f64,
}

impl ExecutionEventProjector {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            events: Vec::with_capacity(capacity),
            funding_events: Vec::with_capacity(1),
            timer_events: Vec::with_capacity(1),
        }
    }

    /// Stable callback classification shared by report-backed backtests, connector adapters and
    /// legacy compatibility responses. The caller supplies whether local position actually
    /// changed; no exchange-only state is read here.
    pub fn visible_event_kinds(
        report: &ExecutionReport,
        position_changed: bool,
    ) -> [Option<ProjectedEventKind>; 3] {
        [
            Some(ProjectedEventKind::Order),
            (report.kind == ExecutionReportKind::Fill).then_some(ProjectedEventKind::Filled),
            position_changed.then_some(ProjectedEventKind::Position),
        ]
    }

    /// Timer has no account mutation, but still crosses the same local visibility projection
    /// boundary in backtest and live scheduling.
    pub fn project_timer(&mut self, event: TimerEvent) -> &[TimerEvent] {
        self.timer_events.clear();
        self.timer_events.push(event);
        &self.timer_events
    }

    /// Projects both backtest settlements and normalized live funding through the same local
    /// visibility boundary.
    pub fn project_funding(
        &mut self,
        report: FundingReport,
        local_account: &mut LocalAccountView,
    ) -> Result<&[ProjectedFundingEvent], super::AccountError> {
        self.funding_events.clear();
        self.timer_events.clear();
        local_account.deliver(report.account_report)?;
        let currency = report.event.currency;
        self.funding_events.push(ProjectedFundingEvent {
            report,
            visible_balance: local_account.account().balance(currency),
            visible_funding: local_account.account().funding(currency),
        });
        Ok(&self.funding_events)
    }

    /// Delivers the report to local state and emits the stable callback sequence:
    /// `on_order`, then `on_filled`, then `on_position` when position changed.
    pub fn project(
        &mut self,
        report: ExecutionReport,
        local_account: &mut LocalAccountView,
    ) -> Result<&[ProjectedEvent], super::AccountError> {
        self.events.clear();
        self.funding_events.clear();
        self.timer_events.clear();
        let old_position = local_account.account().position(report.instrument_id).qty;

        if let Some(delta) = report.account_delta {
            local_account.deliver(AccountReport {
                venue_id: report.venue_id,
                exchange_ts: report.exchange_ts,
                delivery_ts: report.delivery_ts,
                sequence: report.sequence,
                delta,
            })?;
        }

        let visible_position = local_account.account().position(report.instrument_id).qty;
        for kind in Self::visible_event_kinds(&report, visible_position != old_position)
            .into_iter()
            .flatten()
        {
            self.events.push(ProjectedEvent {
                kind,
                report,
                visible_position,
            });
        }
        Ok(&self.events)
    }

    /// Projects a connector-normalized report when the connector already owns and updates the
    /// strategy-visible account. This retains the exact callback classification/order without
    /// reading exchange-only state or applying an account delta twice.
    pub fn project_visible(
        &mut self,
        report: ExecutionReport,
        visible_position: f64,
        position_changed: bool,
    ) -> &[ProjectedEvent] {
        self.events.clear();
        self.funding_events.clear();
        self.timer_events.clear();
        for kind in Self::visible_event_kinds(&report, position_changed)
            .into_iter()
            .flatten()
        {
            self.events.push(ProjectedEvent {
                kind,
                report,
                visible_position,
            });
        }
        &self.events
    }

    pub fn reset(&mut self) {
        self.events.clear();
        self.funding_events.clear();
        self.timer_events.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backtest::execution::CurrencyId;

    fn fill_report() -> ExecutionReport {
        ExecutionReport {
            kind: ExecutionReportKind::Fill,
            reason: ExecutionReason::None,
            venue_id: VenueId(1),
            instrument_id: InstrumentId(2),
            asset_no: 0,
            order_id: 3,
            venue_order_id: 30,
            exchange_ts: 100,
            delivery_ts: 120,
            sequence: 0,
            status: Status::Filled,
            side: Side::Buy,
            order_price: 10.0,
            order_qty: 2.0,
            exec_price: 10.0,
            exec_qty: 2.0,
            maker: false,
            account_delta: Some(AccountDelta {
                instrument_id: InstrumentId(2),
                position_delta: 2.0,
                trade_qty: 2.0,
                trade_value: 20.0,
                currency: CurrencyId(1),
                cash_delta: -20.0,
                fee: 0.02,
                funding: 0.0,
                execution_price: 10.0,
                realized_pnl: 0.0,
            }),
        }
    }

    #[test]
    fn fill_projects_order_fill_position_after_local_update() {
        let mut projector = ExecutionEventProjector::with_capacity(3);
        let mut local = LocalAccountView::new(VenueId(1));
        let events = projector.project(fill_report(), &mut local).unwrap();

        assert_eq!(
            events.iter().map(|event| event.kind).collect::<Vec<_>>(),
            [
                ProjectedEventKind::Order,
                ProjectedEventKind::Filled,
                ProjectedEventKind::Position
            ]
        );
        assert!(events.iter().all(|event| event.visible_position == 2.0));
    }

    #[test]
    fn accepted_order_does_not_emit_fill_or_position() {
        let mut report = fill_report();
        report.kind = ExecutionReportKind::Accepted;
        report.status = Status::New;
        report.account_delta = None;
        let mut projector = ExecutionEventProjector::default();
        let mut local = LocalAccountView::new(VenueId(1));

        let events = projector.project(report, &mut local).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, ProjectedEventKind::Order);
    }
}
