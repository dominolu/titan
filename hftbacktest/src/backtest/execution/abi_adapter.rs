//! Conversion between engine-domain reports and stable Runtime ABI payloads.

use titan_runtime_abi::{FillEvent, OrderEvent};

use crate::types::{Order, Status};

use super::{
    ExecutionEventProjector, ExecutionReason, ExecutionReport, ExecutionReportKind, InstrumentId,
    ProjectedEventKind, VenueId,
};

pub const fn execution_reason_code(reason: ExecutionReason) -> u32 {
    use ExecutionReason::*;
    match reason {
        None => 0,
        LocalRisk => 1,
        ExchangeRisk => 2,
        InvalidInstrument => 3,
        InvalidPrice => 4,
        InvalidQuantity => 5,
        DuplicateOrderId => 6,
        PositionLimit => 7,
        NotionalLimit => 8,
        InsufficientBalance => 9,
        InsufficientMargin => 10,
        ReduceOnlyViolation => 11,
        MarketClosed => 12,
        InsufficientLiquidity => 13,
        Expired => 14,
        UserCanceled => 15,
        Unknown(code) => 0x8000_0000 | code,
    }
}

pub const fn execution_reason_from_code(code: u32) -> ExecutionReason {
    match code {
        0 => ExecutionReason::None,
        1 => ExecutionReason::LocalRisk,
        2 => ExecutionReason::ExchangeRisk,
        3 => ExecutionReason::InvalidInstrument,
        4 => ExecutionReason::InvalidPrice,
        5 => ExecutionReason::InvalidQuantity,
        6 => ExecutionReason::DuplicateOrderId,
        7 => ExecutionReason::PositionLimit,
        8 => ExecutionReason::NotionalLimit,
        9 => ExecutionReason::InsufficientBalance,
        10 => ExecutionReason::InsufficientMargin,
        11 => ExecutionReason::ReduceOnlyViolation,
        12 => ExecutionReason::MarketClosed,
        13 => ExecutionReason::InsufficientLiquidity,
        14 => ExecutionReason::Expired,
        15 => ExecutionReason::UserCanceled,
        value if value & 0x8000_0000 != 0 => ExecutionReason::Unknown(value & 0x7fff_ffff),
        value => ExecutionReason::Unknown(value),
    }
}

pub fn project_execution_report(
    report: &ExecutionReport,
    request: u8,
    orders: &mut Vec<OrderEvent>,
    fills: &mut Vec<FillEvent>,
) {
    let reason = execution_reason_code(report.reason);
    orders.push(OrderEvent {
        asset_no: report.asset_no as u64,
        order_id: report.order_id,
        venue_order_id: report.venue_order_id,
        exch_ts: report.exchange_ts,
        local_ts: report.delivery_ts,
        sequence: report.sequence,
        price: report.order_price,
        qty: report.order_qty,
        exec_price: report.exec_price,
        exec_qty: report.exec_qty,
        venue_no: report.venue_id.0,
        instrument_id: report.instrument_id.0,
        reason,
        side: report.side as i8,
        status: report.status as u8,
        request,
        maker: u8::from(report.maker),
        _reserved: [0; 4],
    });
    if ExecutionEventProjector::visible_event_kinds(report, false)
        .contains(&Some(ProjectedEventKind::Filled))
    {
        fills.push(FillEvent {
            asset_no: report.asset_no as u64,
            order_id: report.order_id,
            venue_order_id: report.venue_order_id,
            exch_ts: report.exchange_ts,
            local_ts: report.delivery_ts,
            sequence: report.sequence,
            price: report.exec_price,
            last_fill_qty: report.exec_qty,
            cumulative_filled_qty: report.cumulative_filled_qty,
            venue_no: report.venue_id.0,
            instrument_id: report.instrument_id.0,
            reason,
            side: report.side as i8,
            maker: u8::from(report.maker),
            _reserved: [0; 2],
        });
    }
}

pub fn project_order_response(
    asset_no: usize,
    delivery_ts: i64,
    order: &Order,
    orders: &mut Vec<OrderEvent>,
    fills: &mut Vec<FillEvent>,
) {
    let kind = if order.exec_qty > 0.0
        && matches!(order.status, Status::PartiallyFilled | Status::Filled)
    {
        ExecutionReportKind::Fill
    } else {
        match order.status {
            Status::Rejected => ExecutionReportKind::Rejected,
            Status::Canceled => ExecutionReportKind::Canceled,
            Status::Expired => ExecutionReportKind::Expired,
            _ => ExecutionReportKind::Accepted,
        }
    };
    let report = ExecutionReport {
        kind,
        reason: ExecutionReason::None,
        venue_id: VenueId(0),
        instrument_id: InstrumentId(asset_no as u32 + 1),
        asset_no: asset_no as u32,
        order_id: order.order_id,
        venue_order_id: 0,
        exchange_ts: order.exch_timestamp,
        delivery_ts,
        sequence: 0,
        status: order.status,
        side: order.side,
        order_price: order.price(),
        order_qty: order.qty,
        exec_price: order.exec_price(),
        exec_qty: order.exec_qty,
        cumulative_filled_qty: order.qty - order.leaves_qty,
        maker: order.maker,
        account_delta: None,
    };
    project_execution_report(&report, order.req as u8, orders, fills);
}
