use std::collections::BTreeSet;

use crate::types::{OrderId, Side, Status};

use super::{
    AccountDelta, ExecutionReason, ExecutionReport, ExecutionReportKind, FundingReport,
    InstrumentId, VenueId,
};

pub const LIVE_EXECUTION_ABI_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LiveOrderStatus {
    Accepted,
    Rejected,
    Canceled,
    Expired,
    PartiallyFilled,
    Filled,
    Unknown(u32),
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct LiveExecutionEvent {
    pub event_id: u128,
    pub venue_id: VenueId,
    pub instrument_id: InstrumentId,
    pub asset_no: u32,
    pub order_id: OrderId,
    pub venue_order_id: u64,
    pub exchange_ts: i64,
    pub delivery_ts: i64,
    pub sequence: u64,
    pub status: LiveOrderStatus,
    pub reason: ExecutionReason,
    pub side: Side,
    pub order_price: f64,
    pub order_qty: f64,
    pub exec_price: f64,
    pub exec_qty: f64,
    pub cumulative_filled_qty: f64,
    pub maker: bool,
    pub account_delta: Option<AccountDelta>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum LiveAdapterError {
    #[error("live execution ABI version mismatch")]
    AbiMismatch,
    #[error("live connector supplied an unknown order status")]
    UnknownStatus,
    #[error("live execution timestamp is invalid")]
    InvalidTimestamp,
    #[error("live fill is missing quantity or account delta")]
    InvalidFill,
}

/// Normalizes connector events into the exact report consumed by the backtest projector. Event-ID
/// deduplication removes reconnect duplicates while preserving distinct partial fills.
pub struct LiveExecutionAdapter {
    seen: BTreeSet<u128>,
}

impl LiveExecutionAdapter {
    pub fn new(connector_abi_version: u32) -> Result<Self, LiveAdapterError> {
        if connector_abi_version != LIVE_EXECUTION_ABI_VERSION {
            return Err(LiveAdapterError::AbiMismatch);
        }
        Ok(Self {
            seen: BTreeSet::new(),
        })
    }

    pub fn normalize(
        &mut self,
        event: LiveExecutionEvent,
    ) -> Result<Option<ExecutionReport>, LiveAdapterError> {
        if event.delivery_ts < event.exchange_ts {
            return Err(LiveAdapterError::InvalidTimestamp);
        }
        if self.seen.contains(&event.event_id) {
            return Ok(None);
        }
        let (kind, status) = match event.status {
            LiveOrderStatus::Accepted => (ExecutionReportKind::Accepted, Status::New),
            LiveOrderStatus::Rejected => (ExecutionReportKind::Rejected, Status::Rejected),
            LiveOrderStatus::Canceled => (ExecutionReportKind::Canceled, Status::Canceled),
            LiveOrderStatus::Expired => (ExecutionReportKind::Expired, Status::Expired),
            LiveOrderStatus::PartiallyFilled => {
                (ExecutionReportKind::Fill, Status::PartiallyFilled)
            }
            LiveOrderStatus::Filled => (ExecutionReportKind::Fill, Status::Filled),
            LiveOrderStatus::Unknown(_) => return Err(LiveAdapterError::UnknownStatus),
        };
        if kind == ExecutionReportKind::Fill
            && (event.exec_qty <= 0.0 || event.account_delta.is_none())
        {
            return Err(LiveAdapterError::InvalidFill);
        }
        self.seen.insert(event.event_id);
        Ok(Some(ExecutionReport {
            kind,
            reason: event.reason,
            venue_id: event.venue_id,
            instrument_id: event.instrument_id,
            asset_no: event.asset_no,
            order_id: event.order_id,
            venue_order_id: event.venue_order_id,
            exchange_ts: event.exchange_ts,
            delivery_ts: event.delivery_ts,
            sequence: event.sequence,
            status,
            side: event.side,
            order_price: event.order_price,
            order_qty: event.order_qty,
            exec_price: event.exec_price,
            exec_qty: event.exec_qty,
            cumulative_filled_qty: event.cumulative_filled_qty,
            maker: event.maker,
            account_delta: event.account_delta,
        }))
    }

    /// Deduplicates a connector Funding event and returns the canonical report consumed by the
    /// same projector as backtest Funding settlements.
    pub fn normalize_funding(
        &mut self,
        event_id: u128,
        report: FundingReport,
    ) -> Result<Option<FundingReport>, LiveAdapterError> {
        if report.delivery_ts < report.event.settlement_ts
            || report.account_report.delivery_ts != report.delivery_ts
            || report.account_report.exchange_ts != report.event.settlement_ts
        {
            return Err(LiveAdapterError::InvalidTimestamp);
        }
        if !self.seen.insert(event_id) {
            return Ok(None);
        }
        Ok(Some(report))
    }

    pub fn reset(&mut self) {
        self.seen.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backtest::execution::{
        AccountReport, CurrencyId, ExecutionEventProjector, FundingBoundary, FundingEvent,
        FundingPriceSource, LocalAccountView,
    };

    fn event(event_id: u128, sequence: u64, qty: f64) -> LiveExecutionEvent {
        LiveExecutionEvent {
            event_id,
            venue_id: VenueId(1),
            instrument_id: InstrumentId(2),
            asset_no: 0,
            order_id: 3,
            venue_order_id: 30,
            exchange_ts: 10,
            delivery_ts: 12,
            sequence,
            status: if qty == 1.0 {
                LiveOrderStatus::PartiallyFilled
            } else {
                LiveOrderStatus::Filled
            },
            reason: ExecutionReason::None,
            side: Side::Buy,
            order_price: 100.0,
            order_qty: 3.0,
            exec_price: 100.0,
            exec_qty: qty,
            cumulative_filled_qty: qty,
            maker: false,
            account_delta: Some(AccountDelta {
                instrument_id: InstrumentId(2),
                position_delta: qty,
                trade_qty: qty,
                trade_value: qty * 100.0,
                currency: CurrencyId(7),
                cash_delta: qty * -100.0,
                fee: 0.0,
                funding: 0.0,
                execution_price: 100.0,
                realized_pnl: 0.0,
            }),
        }
    }

    #[test]
    fn deduplicates_reconnect_event_but_preserves_distinct_partial_fills() {
        let mut adapter = LiveExecutionAdapter::new(LIVE_EXECUTION_ABI_VERSION).unwrap();
        let first = adapter.normalize(event(10, 0, 1.0)).unwrap().unwrap();
        assert!(adapter.normalize(event(10, 0, 1.0)).unwrap().is_none());
        let second = adapter.normalize(event(11, 1, 2.0)).unwrap().unwrap();
        let mut projector = ExecutionEventProjector::default();
        let mut local = LocalAccountView::new(VenueId(1));
        assert_eq!(projector.project(first, &mut local).unwrap().len(), 3);
        assert_eq!(projector.project(second, &mut local).unwrap().len(), 3);
        assert_eq!(local.account().position(InstrumentId(2)).qty, 3.0);
    }

    #[test]
    fn live_funding_uses_same_projector_and_deduplicates_reconnect() {
        let delta = AccountDelta {
            instrument_id: InstrumentId(2),
            position_delta: 0.0,
            trade_qty: 0.0,
            trade_value: 0.0,
            currency: CurrencyId(7),
            cash_delta: 0.0,
            fee: 0.0,
            funding: -0.2,
            execution_price: 0.0,
            realized_pnl: 0.0,
        };
        let report = FundingReport {
            event: FundingEvent {
                event_id: 8,
                venue_id: VenueId(1),
                instrument_id: InstrumentId(2),
                currency: CurrencyId(7),
                publication_ts: 80,
                effective_ts: 90,
                settlement_ts: 100,
                rate: 0.001,
                price_source: FundingPriceSource::Mark,
                mark_price: 100.0,
                boundary: FundingBoundary::BeforeSettlementEvents,
            },
            delivery_ts: 120,
            sequence: 0,
            position_qty: 2.0,
            amount: -0.2,
            account_report: AccountReport {
                venue_id: VenueId(1),
                exchange_ts: 100,
                delivery_ts: 120,
                sequence: 0,
                delta,
            },
        };
        let mut adapter = LiveExecutionAdapter::new(LIVE_EXECUTION_ABI_VERSION).unwrap();
        let normalized = adapter.normalize_funding(44, report).unwrap().unwrap();
        assert!(adapter.normalize_funding(44, report).unwrap().is_none());
        let mut projector = ExecutionEventProjector::default();
        let mut local = LocalAccountView::new(VenueId(1));
        let projected = projector.project_funding(normalized, &mut local).unwrap();
        assert_eq!(projected[0].visible_funding, -0.2);
        assert_eq!(local.account().balance(CurrencyId(7)), -0.2);
    }

    #[test]
    fn equivalent_backtest_and_live_reports_project_to_identical_abi_v9_bytes() {
        let live_event = event(99, 7, 1.0);
        let mut adapter = LiveExecutionAdapter::new(LIVE_EXECUTION_ABI_VERSION).unwrap();
        let live_report = adapter.normalize(live_event).unwrap().unwrap();
        let backtest_report = ExecutionReport {
            kind: ExecutionReportKind::Fill,
            reason: live_event.reason,
            venue_id: live_event.venue_id,
            instrument_id: live_event.instrument_id,
            asset_no: live_event.asset_no,
            order_id: live_event.order_id,
            venue_order_id: live_event.venue_order_id,
            exchange_ts: live_event.exchange_ts,
            delivery_ts: live_event.delivery_ts,
            sequence: live_event.sequence,
            status: Status::PartiallyFilled,
            side: live_event.side,
            order_price: live_event.order_price,
            order_qty: live_event.order_qty,
            exec_price: live_event.exec_price,
            exec_qty: live_event.exec_qty,
            cumulative_filled_qty: live_event.cumulative_filled_qty,
            maker: live_event.maker,
            account_delta: live_event.account_delta,
        };
        let mut backtest_orders = Vec::new();
        let mut backtest_fills = Vec::new();
        let mut live_orders = Vec::new();
        let mut live_fills = Vec::new();
        crate::backtest::execution::project_execution_report(
            &backtest_report,
            0,
            &mut backtest_orders,
            &mut backtest_fills,
        );
        crate::backtest::execution::project_execution_report(
            &live_report,
            0,
            &mut live_orders,
            &mut live_fills,
        );
        assert_eq!(backtest_orders, live_orders);
        assert_eq!(backtest_fills, live_fills);
        let bytes = |ptr: *const u8, len| unsafe { std::slice::from_raw_parts(ptr, len) };
        assert_eq!(
            bytes(
                backtest_orders.as_ptr().cast(),
                std::mem::size_of_val(backtest_orders.as_slice()),
            ),
            bytes(
                live_orders.as_ptr().cast(),
                std::mem::size_of_val(live_orders.as_slice()),
            )
        );
        assert_eq!(
            bytes(
                backtest_fills.as_ptr().cast(),
                std::mem::size_of_val(backtest_fills.as_slice()),
            ),
            bytes(
                live_fills.as_ptr().cast(),
                std::mem::size_of_val(live_fills.as_slice()),
            )
        );
    }
}
