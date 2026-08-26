//! Rust-owned event runtime with a stable, extensible C callback ABI.
//!
//! Rust owns event ordering, time, market state and matching. Foreign strategy code
//! (including Numba) only receives callbacks with one context pointer; it never owns the
//! event loop.

use std::{
    collections::{BTreeMap, VecDeque},
    ffi::c_void,
    ptr::NonNull,
};

use crate::{
    backtest::bar::{
        BarBatchMeta, BarExecutionError, BarExecutionState, BarFeed, BarFeedError,
        BarMatchingModel, ConfiguredBarMatcher, MaterializedBarFeed, NextOpenBarMatcher,
        OhlcBarMatcher, OhlcFillAssumption, PendingBarOrder, SignalCloseBarMatcher,
    },
    backtest::{
        execution::{
            ConditionalAction, ConditionalOrder, ConditionalOrderBook, CurrencyId, FundingBoundary,
            FundingConfig, FundingEvent, FundingFormula, FundingPositionSnapshot,
            FundingPriceSource, FundingRounding, FundingRoundingMode, InstrumentId,
            ScheduledFunding, TriggerKind, VenueId,
        },
        platform::{
            ContingencyAction, ContingencyGroup, ContingencyManager, ExecutionAlgorithm,
            PlatformCommandProducers, SimulationHook,
        },
        scheduler::{EventPhase, TimerEvent, TimerId, TimerQueue},
    },
    market_data::{BAR_EMPTY, Bar, BarHistory},
    types::{Event, Order, Status},
};

pub const STRATEGY_ABI_VERSION: u32 = 8;
pub const EVENT_SLOT_COUNT: usize = 32;

/// Stable callback identifiers. New event kinds must use a previously unused number;
/// existing values are ABI and data-log compatibility commitments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum StrategyEventKind {
    Start = 0,
    Order = 1,
    Filled = 2,
    Position = 3,
    Funding = 4,
    Bar = 5,
    Tick = 6,
    Timer = 7,
    Error = 8,
    Stop = 9,
}

impl StrategyEventKind {
    #[inline(always)]
    pub const fn slot(self) -> usize {
        self as usize
    }
}

/// Flat fill payload suitable for zero-copy foreign access.
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(C)]
pub struct FillEvent {
    pub asset_no: u64,
    pub order_id: u64,
    pub venue_order_id: u64,
    pub exch_ts: i64,
    pub local_ts: i64,
    pub sequence: u64,
    pub price: f64,
    pub qty: f64,
    pub venue_no: u32,
    pub instrument_id: u32,
    pub reason: u32,
    pub side: i8,
    pub maker: u8,
    pub _reserved: [u8; 2],
}

/// Flat order-response payload. One entry corresponds to one response received by the
/// local engine; partial fills are never collapsed into an order snapshot.
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(C)]
pub struct OrderEvent {
    pub asset_no: u64,
    pub order_id: u64,
    pub venue_order_id: u64,
    pub exch_ts: i64,
    pub local_ts: i64,
    pub sequence: u64,
    pub price: f64,
    pub qty: f64,
    pub exec_price: f64,
    pub exec_qty: f64,
    pub venue_no: u32,
    pub instrument_id: u32,
    pub reason: u32,
    pub side: i8,
    pub status: u8,
    pub request: u8,
    pub maker: u8,
    pub _reserved: [u8; 4],
}

/// Stable numeric reason code shared by backtest and live ABI payloads.
pub const fn execution_reason_code(reason: crate::backtest::execution::ExecutionReason) -> u32 {
    use crate::backtest::execution::ExecutionReason::*;
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

/// Decodes the stable numeric execution reason used by canonical live connectors.
pub const fn execution_reason_from_code(code: u32) -> crate::backtest::execution::ExecutionReason {
    use crate::backtest::execution::ExecutionReason;
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

/// Projects one canonical execution report into ABI v8 buffers.
pub fn project_execution_report(
    report: &crate::backtest::execution::ExecutionReport,
    request: u8,
    orders: &mut Vec<OrderEvent>,
    fills: &mut Vec<FillEvent>,
) {
    use crate::backtest::execution::{ExecutionEventProjector, ProjectedEventKind};
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
            qty: report.exec_qty,
            venue_no: report.venue_id.0,
            instrument_id: report.instrument_id.0,
            reason,
            side: report.side as i8,
            maker: u8::from(report.maker),
            _reserved: [0; 2],
        });
    }
}

/// Projects one locally delivered execution response into the stable runtime ABI buffers.
/// Backtest and live adapters use this same function; every partial fill remains a distinct item.
pub fn project_order_response(
    asset_no: usize,
    delivery_ts: i64,
    order: &Order,
    orders: &mut Vec<OrderEvent>,
    fills: &mut Vec<FillEvent>,
) {
    use crate::backtest::execution::{
        ExecutionReason, ExecutionReport, ExecutionReportKind, InstrumentId, VenueId,
    };
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
        maker: order.maker,
        account_delta: None,
    };
    project_execution_report(&report, order.req as u8, orders, fills);
}

/// Read-only top-of-book state refreshed by Rust before every market callback.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
#[repr(C)]
pub struct MarketState {
    pub best_bid: f64,
    pub best_ask: f64,
    pub best_bid_qty: f64,
    pub best_ask_qty: f64,
    pub tick_size: f64,
    pub lot_size: f64,
}

pub const ORDER_COMMAND_SUBMIT: u8 = 1;
pub const ORDER_COMMAND_CANCEL: u8 = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrderCommandDecodeError {
    InvalidKind,
    InvalidOrigin,
    InvalidSide,
    InvalidTimeInForce,
    InvalidOrderType,
    InvalidPrice,
    InvalidQuantity,
}

/// Preallocated command written by a foreign callback and consumed synchronously by Rust
/// after the callback returns. Modification is intentionally absent; use cancel/replace.
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(C)]
pub struct OrderCommand {
    pub kind: u8,
    pub side: i8,
    pub time_in_force: u8,
    pub order_type: u8,
    pub _reserved: [u8; 4],
    pub asset_no: u64,
    pub order_id: u64,
    pub price: f64,
    pub qty: f64,
    /// Stop trigger. Zero means an immediately active order.
    pub trigger_price: f64,
    /// GTD expiry on the global clock. Zero means no expiry.
    pub gtd_expiry_ts: i64,
}

impl OrderCommand {
    /// Decode the transport-only ABI object exactly once at the Rust boundary.
    pub fn decode_execution(
        self,
        now: i64,
        venue_id: VenueId,
        instrument_id: InstrumentId,
    ) -> Result<Option<crate::backtest::execution::ExecutionCommand>, OrderCommandDecodeError> {
        use crate::backtest::execution::{
            CancelRequest, ExecutionCommand, ExecutionOrderRequest, OrderOrigin,
        };
        match self.kind {
            0 => Ok(None),
            ORDER_COMMAND_CANCEL => Ok(Some(ExecutionCommand::Cancel(CancelRequest {
                client_order_id: self.order_id,
                venue_id,
                instrument_id,
                local_submit_ts: now,
            }))),
            ORDER_COMMAND_SUBMIT => {
                let origin = match self._reserved[2] {
                    0 => OrderOrigin::Strategy,
                    1 => OrderOrigin::ExecutionAlgorithm,
                    2 => OrderOrigin::Liquidation,
                    _ => return Err(OrderCommandDecodeError::InvalidOrigin),
                };
                let side = match self.side {
                    1 => crate::types::Side::Buy,
                    -1 => crate::types::Side::Sell,
                    _ => return Err(OrderCommandDecodeError::InvalidSide),
                };
                let time_in_force = match self.time_in_force {
                    0 => crate::types::TimeInForce::GTC,
                    1 => crate::types::TimeInForce::GTX,
                    2 => crate::types::TimeInForce::FOK,
                    3 => crate::types::TimeInForce::IOC,
                    _ => return Err(OrderCommandDecodeError::InvalidTimeInForce),
                };
                let order_type = match self.order_type {
                    0 => crate::types::OrdType::Limit,
                    1 => crate::types::OrdType::Market,
                    _ => return Err(OrderCommandDecodeError::InvalidOrderType),
                };
                if !self.qty.is_finite() || self.qty <= 0.0 {
                    return Err(OrderCommandDecodeError::InvalidQuantity);
                }
                if !self.price.is_finite()
                    || (order_type == crate::types::OrdType::Limit && self.price <= 0.0)
                {
                    return Err(OrderCommandDecodeError::InvalidPrice);
                }
                Ok(Some(ExecutionCommand::Submit(ExecutionOrderRequest {
                    client_order_id: self.order_id,
                    venue_id,
                    instrument_id,
                    price: self.price,
                    qty: self.qty,
                    side,
                    time_in_force,
                    order_type,
                    reduce_only: self._reserved[0] != 0,
                    origin,
                    local_submit_ts: now,
                })))
            }
            _ => Err(OrderCommandDecodeError::InvalidKind),
        }
    }
}

impl Default for OrderCommand {
    fn default() -> Self {
        Self {
            kind: 0,
            side: 0,
            time_in_force: 0,
            order_type: 0,
            _reserved: [0; 4],
            asset_no: 0,
            order_id: 0,
            price: 0.0,
            qty: 0.0,
            trigger_price: 0.0,
            gtd_expiry_ts: 0,
        }
    }
}

/// One asset's bar in a global same-timeframe close batch.
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(C)]
pub struct BarItem {
    pub asset_no: u64,
    pub bar: Bar,
}

/// Bar input record used by materialized Bar feeds and manifests.
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(C)]
pub struct TimedBarItem {
    pub asset_no: u64,
    pub timeframe_ns: i64,
    pub bar: Bar,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct RuntimeTimer {
    pub deadline_ts: i64,
    pub owner_id: u64,
    pub timer_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(C)]
pub struct RuntimeFunding {
    pub event_id: u64,
    pub asset_no: u32,
    pub venue_no: u32,
    pub instrument_id: u32,
    pub currency: u32,
    /// 0=mark, 1=index, high bit set=external source ID.
    pub price_source: u32,
    /// 0=before settlement events, 1=after settlement events.
    pub position_snapshot: u8,
    /// Currently 0=InstrumentNotional.
    pub formula: u8,
    /// 0=nearest, 1=toward-zero, 2=floor, 3=ceil.
    pub rounding_mode: u8,
    /// 0=before settlement events, 1=after settlement events.
    pub boundary: u8,
    pub publication_ts: i64,
    pub effective_ts: i64,
    pub settlement_ts: i64,
    pub delivery_ts: i64,
    pub rate: f64,
    pub mark_price: f64,
    pub position_qty: f64,
    pub amount: f64,
    pub rounding_increment: f64,
}

impl Default for RuntimeFunding {
    fn default() -> Self {
        Self {
            event_id: 0,
            asset_no: 0,
            venue_no: 0,
            instrument_id: 0,
            currency: 0,
            price_source: 0,
            position_snapshot: 0,
            formula: 0,
            rounding_mode: 0,
            boundary: 0,
            publication_ts: 0,
            effective_ts: 0,
            settlement_ts: 0,
            delivery_ts: 0,
            rate: 0.0,
            mark_price: 0.0,
            position_qty: 0.0,
            amount: 0.0,
            rounding_increment: 1e-12,
        }
    }
}

impl RuntimeFunding {
    pub fn config(self) -> Result<FundingConfig, MaterializedBarError> {
        let price_source = match self.price_source {
            0 => FundingPriceSource::Mark,
            1 => FundingPriceSource::Index,
            value if value & 0x8000_0000 != 0 => FundingPriceSource::External(value & 0x7fff_ffff),
            _ => return Err(MaterializedBarError::InvalidOrder),
        };
        let position_snapshot = match self.position_snapshot {
            0 => FundingPositionSnapshot::BeforeSettlementEvents,
            1 => FundingPositionSnapshot::AfterSettlementEvents,
            _ => return Err(MaterializedBarError::InvalidOrder),
        };
        let formula = match self.formula {
            0 => FundingFormula::InstrumentNotional,
            _ => return Err(MaterializedBarError::InvalidOrder),
        };
        let mode = match self.rounding_mode {
            0 => FundingRoundingMode::Nearest,
            1 => FundingRoundingMode::TowardZero,
            2 => FundingRoundingMode::Floor,
            3 => FundingRoundingMode::Ceil,
            _ => return Err(MaterializedBarError::InvalidOrder),
        };
        let boundary = match self.boundary {
            0 => FundingBoundary::BeforeSettlementEvents,
            1 => FundingBoundary::AfterSettlementEvents,
            _ => return Err(MaterializedBarError::InvalidOrder),
        };
        Ok(FundingConfig {
            price_source,
            position_snapshot,
            formula,
            currency: CurrencyId(self.currency),
            rounding: FundingRounding {
                increment: self.rounding_increment,
                mode,
            },
            boundary,
        })
    }

    pub fn from_report(
        asset_no: u32,
        report: crate::backtest::execution::FundingReport,
        config: FundingConfig,
    ) -> Self {
        let price_source = match config.price_source {
            FundingPriceSource::Mark => 0,
            FundingPriceSource::Index => 1,
            FundingPriceSource::External(source_id) => 0x8000_0000 | source_id,
        };
        let position_snapshot = match config.position_snapshot {
            FundingPositionSnapshot::BeforeSettlementEvents => 0,
            FundingPositionSnapshot::AfterSettlementEvents => 1,
        };
        let rounding_mode = match config.rounding.mode {
            FundingRoundingMode::Nearest => 0,
            FundingRoundingMode::TowardZero => 1,
            FundingRoundingMode::Floor => 2,
            FundingRoundingMode::Ceil => 3,
        };
        let boundary = match config.boundary {
            FundingBoundary::BeforeSettlementEvents => 0,
            FundingBoundary::AfterSettlementEvents => 1,
        };
        let formula = match config.formula {
            crate::backtest::execution::FundingFormula::InstrumentNotional => 0,
        };
        Self {
            event_id: report.event.event_id,
            asset_no,
            venue_no: report.event.venue_id.0,
            instrument_id: report.event.instrument_id.0,
            currency: report.event.currency.0,
            price_source,
            position_snapshot,
            formula,
            rounding_mode,
            boundary,
            publication_ts: report.event.publication_ts,
            effective_ts: report.event.effective_ts,
            settlement_ts: report.event.settlement_ts,
            delivery_ts: report.delivery_ts,
            rate: report.event.rate,
            mark_price: report.event.mark_price,
            position_qty: report.position_qty,
            amount: report.amount,
            rounding_increment: config.rounding.increment,
        }
    }
}

fn config_price_source(code: u32) -> FundingPriceSource {
    match code {
        0 => FundingPriceSource::Mark,
        1 => FundingPriceSource::Index,
        value if value & 0x8000_0000 != 0 => FundingPriceSource::External(value & 0x7fff_ffff),
        _ => unreachable!("RuntimeFunding is validated before enqueue"),
    }
}

/// Read-only ring metadata exposed to foreign callbacks.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct BarHistoryView {
    pub asset_no: u64,
    pub timeframe_ns: i64,
    pub bars_ptr: *const Bar,
    pub capacity: usize,
    pub len: usize,
    pub next: usize,
}

/// One normalized market event in a global TickBatch.
#[derive(Debug, Clone, PartialEq)]
#[repr(C)]
pub struct TickItem {
    pub asset_no: u64,
    pub event: Event,
}

/// Context passed as the single callback argument.
///
/// All pointer-backed views are read-only and valid only until the callback returns.
/// Calling back into `advance` or retaining a view across callbacks is invalid. `generation`
/// changes before every callback and can be checked by debug bindings.
#[derive(Debug)]
#[repr(C)]
pub struct StrategyRuntimeContext {
    pub abi_version: u32,
    pub struct_size: u32,
    pub event_kind: u32,
    pub stop_requested: u32,
    /// Runtime delivery clock. Backtests normally use local/delivery time; live mode uses
    /// the runtime's current local clock.
    pub now: i64,
    pub generation: u64,
    pub user_data: *mut c_void,
    pub bot_ptr: *mut c_void,

    pub ticks_ptr: *const TickItem,
    pub num_ticks: usize,
    pub bars_ptr: *const BarItem,
    pub num_bars: usize,
    pub bar_timeframe_ns: i64,
    pub bar_close_ts: i64,
    pub fills_ptr: *const FillEvent,
    pub num_fills: usize,
    pub orders_ptr: *const OrderEvent,
    pub num_orders: usize,
    pub histories_ptr: *const BarHistoryView,
    pub num_histories: usize,

    /// Event-specific POD payload for current and future event kinds.
    pub payload_ptr: *const c_void,
    pub payload_len: usize,
    pub state_f64_ptr: *mut f64,
    pub state_f64_len: usize,
    pub state_i64_ptr: *mut i64,
    pub state_i64_len: usize,
    pub commands_ptr: *mut OrderCommand,
    pub num_commands: usize,
    pub command_capacity: usize,
    pub positions_ptr: *const f64,
    pub num_positions: usize,
    pub markets_ptr: *const MarketState,
    pub num_markets: usize,
    pub last_error: i64,
}

impl Default for StrategyRuntimeContext {
    fn default() -> Self {
        Self {
            abi_version: STRATEGY_ABI_VERSION,
            struct_size: size_of::<Self>() as u32,
            event_kind: StrategyEventKind::Start as u32,
            stop_requested: 0,
            now: 0,
            generation: 0,
            user_data: std::ptr::null_mut(),
            bot_ptr: std::ptr::null_mut(),
            ticks_ptr: std::ptr::null(),
            num_ticks: 0,
            bars_ptr: std::ptr::null(),
            num_bars: 0,
            bar_timeframe_ns: 0,
            bar_close_ts: 0,
            fills_ptr: std::ptr::null(),
            num_fills: 0,
            orders_ptr: std::ptr::null(),
            num_orders: 0,
            histories_ptr: std::ptr::null(),
            num_histories: 0,
            payload_ptr: std::ptr::null(),
            payload_len: 0,
            state_f64_ptr: std::ptr::null_mut(),
            state_f64_len: 0,
            state_i64_ptr: std::ptr::null_mut(),
            state_i64_len: 0,
            commands_ptr: std::ptr::null_mut(),
            num_commands: 0,
            command_capacity: 0,
            positions_ptr: std::ptr::null(),
            num_positions: 0,
            markets_ptr: std::ptr::null(),
            num_markets: 0,
            last_error: 0,
        }
    }
}

impl StrategyRuntimeContext {
    #[inline]
    pub fn request_stop(&mut self) {
        self.stop_requested = 1;
    }

    fn clear_views(&mut self) {
        self.ticks_ptr = std::ptr::null();
        self.num_ticks = 0;
        self.bars_ptr = std::ptr::null();
        self.num_bars = 0;
        self.bar_timeframe_ns = 0;
        self.bar_close_ts = 0;
        self.fills_ptr = std::ptr::null();
        self.num_fills = 0;
        self.orders_ptr = std::ptr::null();
        self.num_orders = 0;
        self.payload_ptr = std::ptr::null();
        self.payload_len = 0;
    }
}

/// Native callback generated by a Numba `cfunc` bridge or supplied by another native
/// strategy implementation. Zero means success; non-zero requests an error stop.
pub type StrategyCallback = unsafe extern "C" fn(*mut StrategyRuntimeContext) -> i32;

#[derive(Clone)]
pub struct CallbackRegistry {
    callbacks: [Option<StrategyCallback>; EVENT_SLOT_COUNT],
}

impl Default for CallbackRegistry {
    fn default() -> Self {
        Self {
            callbacks: [None; EVENT_SLOT_COUNT],
        }
    }
}

impl CallbackRegistry {
    pub fn set(&mut self, kind: StrategyEventKind, callback: StrategyCallback) {
        self.callbacks[kind.slot()] = Some(callback);
    }

    pub fn set_custom(
        &mut self,
        event_id: u32,
        callback: StrategyCallback,
    ) -> Result<(), RuntimeError> {
        let slot = event_id as usize;
        if slot >= EVENT_SLOT_COUNT {
            return Err(RuntimeError::InvalidEventId(event_id));
        }
        self.callbacks[slot] = Some(callback);
        Ok(())
    }

    fn get(&self, event_id: u32) -> Option<StrategyCallback> {
        self.callbacks.get(event_id as usize).copied().flatten()
    }
}

/// Borrowed event payload returned by a Rust event source.
pub enum RuntimePayload<'a> {
    None,
    Ticks(&'a [TickItem]),
    Bars {
        timeframe_ns: i64,
        close_ts: i64,
        bars: &'a [BarItem],
    },
    Fills(&'a [FillEvent]),
    Orders(&'a [OrderEvent]),
    Pod {
        ptr: NonNull<c_void>,
        len: usize,
    },
}

pub struct RuntimeEvent<'a> {
    pub kind: u32,
    pub now: i64,
    pub payload: RuntimePayload<'a>,
}

/// Rust-owned source/scheduler contract. Implementations own their event queue, clock,
/// market state and matching engine. Events yielded at the same logical time must already
/// follow the documented deterministic priority.
pub trait RuntimeEventSource {
    type Error: std::error::Error + Send + Sync + 'static;

    fn next_event(&mut self) -> Result<Option<RuntimeEvent<'_>>, Self::Error>;

    /// Called after the foreign callback returns. Bar sources commit the current close to
    /// history here, which makes `history[-1]` mean the preceding bar during `on_bar`.
    fn after_callback(
        &mut self,
        _kind: u32,
        _ctx: &mut StrategyRuntimeContext,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Final resource/recorder hook. It is invoked exactly once after `on_stop`, including all
    /// callback and source error paths.
    fn finish(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn classify_error(
        &self,
        _error: &Self::Error,
    ) -> (
        crate::backtest::result::EngineComponent,
        crate::backtest::result::EngineErrorCode,
    ) {
        (
            crate::backtest::result::EngineComponent::DataSource,
            crate::backtest::result::EngineErrorCode::InvalidData,
        )
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MaterializedBarError {
    #[error("bar timeframe must be positive")]
    InvalidTimeframe,
    #[error("bar interval does not match its timeframe")]
    IntervalMismatch,
    #[error("partial bars cannot be delivered to on_bar")]
    PartialBar,
    #[error("on_bar accepts only bars marked complete")]
    IncompleteBar,
    #[error("invalid OHLCV values")]
    InvalidOhlcv,
    #[error("bar input must be sorted by (close_ts, timeframe_ns, asset_no)")]
    Unsorted,
    #[error("duplicate asset in one bar batch")]
    DuplicateAsset,
    #[error("bar data source I/O failed")]
    DataSource,
    #[error("invalid bar order command")]
    InvalidOrder,
    #[error("duplicate active bar order id {order_id} for asset {asset_no}")]
    DuplicateOrder { asset_no: u64, order_id: u64 },
    #[error("bar order command buffer overflow")]
    CommandOverflow,
    #[error("shared Bar execution error: {0}")]
    SharedExecution(#[from] BarExecutionError),
}

impl From<BarFeedError> for MaterializedBarError {
    fn from(error: BarFeedError) -> Self {
        match error {
            BarFeedError::InvalidTimeframe => Self::InvalidTimeframe,
            BarFeedError::IntervalMismatch => Self::IntervalMismatch,
            BarFeedError::PartialBar => Self::PartialBar,
            BarFeedError::IncompleteBar => Self::IncompleteBar,
            BarFeedError::InvalidOhlcv => Self::InvalidOhlcv,
            BarFeedError::Unsorted => Self::Unsorted,
            BarFeedError::DuplicateAsset => Self::DuplicateAsset,
            BarFeedError::Io | BarFeedError::NpySchema => Self::DataSource,
        }
    }
}

struct HistorySlot {
    asset_no: u64,
    timeframe_ns: i64,
    history: BarHistory,
}

#[derive(Clone, Copy)]
enum PendingBarAction {
    Submit {
        command: OrderCommand,
        local_submit_ts: i64,
        exchange_arrival_ts: i64,
    },
    Cancel {
        command: OrderCommand,
        local_submit_ts: i64,
        exchange_arrival_ts: i64,
    },
}

impl PendingBarAction {
    fn exchange_arrival_ts(self) -> i64 {
        match self {
            Self::Submit {
                exchange_arrival_ts,
                ..
            }
            | Self::Cancel {
                exchange_arrival_ts,
                ..
            } => exchange_arrival_ts,
        }
    }
}

fn action_precedes_matching(key: crate::backtest::scheduler::EventKey, exchange_ts: i64) -> bool {
    key.timestamp < exchange_ts
        || (key.timestamp == exchange_ts && key.phase < EventPhase::Matching)
}

/// Rust-owned source for already materialized, closed Bar records.
///
/// Records may contain multiple assets and timeframes. They must be sorted by
/// `(close_ts, timeframe_ns, asset_no)`. Matching defaults to conservative NextOpen; callers may
/// explicitly select OHLC or same-close assumptions. Only the globally smallest timeframe is
/// executable, so a slower Bar can never expose a future execution Bar.
pub struct MaterializedBarSource {
    feed: MaterializedBarFeed,
    histories: Vec<HistorySlot>,
    views: Vec<BarHistoryView>,
    commands: Vec<OrderCommand>,
    matcher: ConfiguredBarMatcher,
    execution: BarExecutionState,
    execution_cursor: usize,
    order_batch: Vec<OrderEvent>,
    fill_batch: Vec<FillEvent>,
    pending_bar_deliveries: VecDeque<PendingBarDelivery>,
    current_bar_delivery: Option<PendingBarDelivery>,
    feed_latency_ns: i64,
    entry_latency_ns: i64,
    action_scheduler: crate::backtest::scheduler::GlobalScheduler<PendingBarAction>,
    platform: PlatformCommandProducers,
    platform_scratch: Vec<crate::backtest::execution::ExecutionCommand>,
    contingencies: ContingencyManager,
    held_contingent_orders: BTreeMap<(u64, u64), PendingBarOrder>,
    contingency_report_cursor: usize,
    conditional_books: Vec<ConditionalOrderBook>,
    conditional_orders: BTreeMap<(u64, u64), PendingBarOrder>,
    gtd_orders: BTreeMap<(u64, u64), (i64, PendingBarOrder)>,
    conditional_scratch: Vec<ConditionalAction>,
    timers: TimerQueue,
    scheduler: crate::backtest::scheduler::GlobalScheduler<BarBoundary>,
    configured_timers: Vec<RuntimeTimer>,
    timer_scratch: Vec<TimerEvent>,
    current_timer: RuntimeTimer,
    funding: VecDeque<ScheduledFunding>,
    configured_funding: Vec<RuntimeFunding>,
    current_funding: RuntimeFunding,
    funding_callback_pending: bool,
    next_risk_order_id: u64,
    execution_timeframe_ns: i64,
    last_execution_closes: Vec<Option<(i64, f64)>>,
    last_bar_delivery_ts: i64,
    terminal_close_started: bool,
    end_policy: crate::backtest::result::EndPolicy,
    data_end_ts: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BarBoundary {
    ResponseDelivery,
    FundingSettlement,
    OrderExpiry,
    CommandArrival,
    Timer,
    MarketOpen,
    BarClose,
}

struct PendingBarDelivery {
    delivery_ts: i64,
    meta: BarBatchMeta,
    bars: Vec<BarItem>,
}

impl MaterializedBarSource {
    pub fn new(
        records: &[TimedBarItem],
        history_capacity: usize,
    ) -> Result<Self, MaterializedBarError> {
        let feed = MaterializedBarFeed::new(records)?;

        let mut keys: Vec<(u64, i64)> = feed
            .records()
            .iter()
            .map(|record| (record.asset_no, record.timeframe_ns))
            .collect();
        keys.sort_unstable();
        keys.dedup();
        let histories: Vec<_> = keys
            .into_iter()
            .map(|(asset_no, timeframe_ns)| HistorySlot {
                asset_no,
                timeframe_ns,
                history: BarHistory::new(history_capacity),
            })
            .collect();
        let views = histories.iter().map(Self::view).collect();
        let num_assets = feed
            .records()
            .iter()
            .map(|record| record.asset_no as usize + 1)
            .max()
            .unwrap_or(0);
        let execution_timeframe_ns = feed
            .records()
            .iter()
            .map(|record| record.timeframe_ns)
            .min()
            .unwrap_or(0);
        let data_end_ts = feed
            .records()
            .iter()
            .map(|record| record.bar.close_ts)
            .max()
            .unwrap_or(i64::MIN);
        let mut execution_assets = vec![false; num_assets];
        for record in feed.records() {
            if record.timeframe_ns == execution_timeframe_ns {
                execution_assets[record.asset_no as usize] = true;
            }
        }
        Ok(Self {
            feed,
            histories,
            views,
            commands: vec![OrderCommand::default(); 1024],
            matcher: ConfiguredBarMatcher::NextOpen(NextOpenBarMatcher::new(
                execution_timeframe_ns,
                execution_assets,
            )),
            execution: BarExecutionState::new(num_assets),
            execution_cursor: 0,
            order_batch: Vec::with_capacity(1),
            fill_batch: Vec::with_capacity(1),
            pending_bar_deliveries: VecDeque::with_capacity(4),
            current_bar_delivery: None,
            feed_latency_ns: 0,
            entry_latency_ns: 0,
            action_scheduler: crate::backtest::scheduler::GlobalScheduler::new(),
            platform: PlatformCommandProducers::with_capacity(1024),
            platform_scratch: Vec::with_capacity(16),
            contingencies: ContingencyManager::default(),
            held_contingent_orders: BTreeMap::new(),
            contingency_report_cursor: 0,
            conditional_books: (0..num_assets)
                .map(|_| ConditionalOrderBook::default())
                .collect(),
            conditional_orders: BTreeMap::new(),
            gtd_orders: BTreeMap::new(),
            conditional_scratch: Vec::with_capacity(8),
            timers: TimerQueue::default(),
            scheduler: crate::backtest::scheduler::GlobalScheduler::new(),
            configured_timers: Vec::new(),
            timer_scratch: Vec::with_capacity(4),
            current_timer: RuntimeTimer {
                deadline_ts: 0,
                owner_id: 0,
                timer_id: 0,
            },
            funding: VecDeque::with_capacity(8),
            configured_funding: Vec::new(),
            current_funding: RuntimeFunding::default(),
            funding_callback_pending: false,
            next_risk_order_id: u64::MAX,
            execution_timeframe_ns,
            last_execution_closes: vec![None; num_assets],
            last_bar_delivery_ts: i64::MIN,
            terminal_close_started: false,
            end_policy: crate::backtest::result::EndPolicy::DrainAll,
            data_end_ts,
        })
    }

    fn view(slot: &HistorySlot) -> BarHistoryView {
        BarHistoryView {
            asset_no: slot.asset_no,
            timeframe_ns: slot.timeframe_ns,
            bars_ptr: slot.history.as_ptr(),
            capacity: slot.history.capacity(),
            len: slot.history.len(),
            next: slot.history.next_index(),
        }
    }

    fn refresh_views(&mut self) {
        for (view, slot) in self.views.iter_mut().zip(&self.histories) {
            *view = Self::view(slot);
        }
    }

    fn next_boundary(
        &mut self,
        market: Option<(i64, BarBoundary)>,
        bar_delivery: Option<i64>,
    ) -> Option<BarBoundary> {
        use crate::backtest::scheduler::EventPhase;
        self.scheduler.reset();
        if let Some(key) = self.execution.next_delivery_key() {
            self.scheduler.schedule(
                key.timestamp,
                key.phase,
                key.source_priority,
                key.venue_no,
                key.asset_no,
                BarBoundary::ResponseDelivery,
            );
        }
        if let Some(funding) = self.funding.front() {
            let phase = match funding.event.boundary {
                FundingBoundary::BeforeSettlementEvents => EventPhase::ExchangeState,
                FundingBoundary::AfterSettlementEvents => EventPhase::PostMatchingSettlement,
            };
            self.scheduler.schedule(
                funding.event.settlement_ts,
                phase,
                0,
                funding.event.venue_id.0,
                funding.asset_no,
                BarBoundary::FundingSettlement,
            );
        }
        if let Some(timestamp) = self.next_expiry_ts() {
            self.scheduler.schedule(
                timestamp,
                EventPhase::ExchangeState,
                0,
                0,
                0,
                BarBoundary::OrderExpiry,
            );
        }
        if let Some(key) = self.action_scheduler.peek_key() {
            self.scheduler.schedule(
                key.timestamp,
                key.phase,
                key.source_priority,
                key.venue_no,
                key.asset_no,
                BarBoundary::CommandArrival,
            );
        }
        if let Some(timestamp) = self.next_timer_ts() {
            self.scheduler
                .schedule(timestamp, EventPhase::Timer, 0, 0, 0, BarBoundary::Timer);
        }
        if let Some((timestamp, boundary)) = market {
            let phase = if boundary == BarBoundary::MarketOpen {
                EventPhase::Matching
            } else {
                EventPhase::MarketDelivery
            };
            self.scheduler.schedule(timestamp, phase, 0, 0, 0, boundary);
        }
        if let Some(timestamp) = bar_delivery {
            self.scheduler.schedule(
                timestamp,
                EventPhase::MarketDelivery,
                0,
                0,
                0,
                BarBoundary::BarClose,
            );
        }
        let event = self.scheduler.pop()?;
        let limit = match self.end_policy {
            crate::backtest::result::EndPolicy::DrainAll => None,
            crate::backtest::result::EndPolicy::StopAtDataEnd => Some(self.data_end_ts),
            crate::backtest::result::EndPolicy::StopAtTime(timestamp) => Some(timestamp),
        };
        if limit.is_some_and(|limit| event.key.timestamp > limit) {
            None
        } else {
            Some(event.payload)
        }
    }

    pub fn configure_context(&mut self, ctx: &mut StrategyRuntimeContext) {
        ctx.histories_ptr = self.views.as_ptr();
        ctx.num_histories = self.views.len();
        ctx.commands_ptr = self.commands.as_mut_ptr();
        ctx.command_capacity = self.commands.len();
        ctx.num_commands = 0;
        ctx.positions_ptr = self.execution.positions().as_ptr();
        ctx.num_positions = self.execution.positions().len();
    }

    pub fn set_end_policy(&mut self, policy: crate::backtest::result::EndPolicy) {
        self.end_policy = policy;
    }

    fn close_terminal_positions(&mut self) -> Result<(), MaterializedBarError> {
        self.terminal_close_started = true;
        for asset_no in 0..self.execution.positions().len() {
            let position = self.execution.exchange_position(asset_no).unwrap_or(0.0);
            if position == 0.0 {
                continue;
            }
            let Some((close_ts, close)) = self.last_execution_closes[asset_no] else {
                return Err(MaterializedBarError::InvalidOrder);
            };
            let terminal_ts = close_ts.max(self.last_bar_delivery_ts);
            let order_id = self.next_risk_order_id;
            self.next_risk_order_id = self.next_risk_order_id.wrapping_sub(1);
            let command = OrderCommand {
                kind: ORDER_COMMAND_SUBMIT,
                side: if position > 0.0 { -1 } else { 1 },
                time_in_force: 3,
                order_type: 1,
                _reserved: [1, 0, 2, 0],
                asset_no: asset_no as u64,
                order_id,
                price: close,
                qty: position.abs(),
                ..OrderCommand::default()
            };
            if !self.execution.arrive(command, terminal_ts, terminal_ts)? {
                continue;
            }
            self.execution
                .apply(&[crate::backtest::bar::BarMatchOutcome::Fill {
                    command,
                    local_submit_ts: terminal_ts,
                    exchange_ts: terminal_ts,
                    price: close,
                    qty: position.abs(),
                }])?;
        }
        Ok(())
    }

    /// Configures when a closed Bar becomes locally visible. The Bar itself continues to contain
    /// only `open_ts`/`close_ts`; visibility is represented by the scheduler envelope.
    pub fn configure_feed_latency(
        &mut self,
        feed_latency_ns: i64,
    ) -> Result<(), MaterializedBarError> {
        if feed_latency_ns < 0 {
            return Err(MaterializedBarError::InvalidOrder);
        }
        self.feed_latency_ns = feed_latency_ns;
        Ok(())
    }

    pub fn schedule_timer(&mut self, timer: RuntimeTimer) {
        if let Some(existing) = self.configured_timers.iter_mut().find(|existing| {
            existing.owner_id == timer.owner_id && existing.timer_id == timer.timer_id
        }) {
            *existing = timer;
        } else {
            self.configured_timers.push(timer);
        }
        self.enqueue_timer(timer);
    }

    fn enqueue_timer(&mut self, timer: RuntimeTimer) {
        self.timers
            .schedule(
                timer.deadline_ts,
                TimerId {
                    owner_id: timer.owner_id,
                    timer_id: timer.timer_id,
                },
            )
            .expect("default timer duplicate policy is Replace");
    }

    pub fn cancel_timer(&mut self, owner_id: u64, timer_id: u64) -> bool {
        self.timers.cancel(TimerId { owner_id, timer_id })
    }

    pub fn schedule_funding(
        &mut self,
        funding: RuntimeFunding,
    ) -> Result<(), MaterializedBarError> {
        let config = funding.config()?;
        self.execution
            .configure_funding(funding.asset_no as usize, config)?;
        if let Some(existing) = self
            .configured_funding
            .iter_mut()
            .find(|existing| existing.event_id == funding.event_id)
        {
            *existing = funding;
        } else {
            self.configured_funding.push(funding);
        }
        self.enqueue_funding(funding);
        Ok(())
    }

    fn enqueue_funding(&mut self, funding: RuntimeFunding) {
        let scheduled = ScheduledFunding {
            asset_no: funding.asset_no,
            event: FundingEvent {
                event_id: funding.event_id,
                venue_id: VenueId(funding.venue_no),
                instrument_id: InstrumentId(funding.instrument_id),
                currency: CurrencyId(funding.currency),
                publication_ts: funding.publication_ts,
                effective_ts: funding.effective_ts,
                settlement_ts: funding.settlement_ts,
                rate: funding.rate,
                price_source: config_price_source(funding.price_source),
                mark_price: funding.mark_price,
                boundary: if funding.boundary == 0 {
                    FundingBoundary::BeforeSettlementEvents
                } else {
                    FundingBoundary::AfterSettlementEvents
                },
            },
            delivery_ts: funding.delivery_ts,
        };
        let index = self
            .funding
            .iter()
            .position(|queued| {
                (
                    queued.event.settlement_ts,
                    queued.event.boundary as u8,
                    queued.asset_no,
                    queued.event.event_id,
                ) > (
                    scheduled.event.settlement_ts,
                    scheduled.event.boundary as u8,
                    scheduled.asset_no,
                    scheduled.event.event_id,
                )
            })
            .unwrap_or(self.funding.len());
        self.funding.insert(index, scheduled);
    }

    pub fn execution_reports(&self) -> &[crate::backtest::execution::ExecutionReport] {
        self.execution.reports()
    }

    pub fn funding_reports(&self) -> &[crate::backtest::execution::FundingReport] {
        self.execution.funding_reports()
    }

    pub fn account_snapshots(
        &self,
    ) -> (
        Vec<crate::backtest::result::AccountSnapshot>,
        Vec<crate::backtest::result::AccountSnapshot>,
    ) {
        self.execution.account_snapshots()
    }

    pub fn configure_execution(
        &mut self,
        configs: Vec<crate::backtest::execution::SharedTickExecutionConfig>,
        response_latency_ns: i64,
    ) -> Result<(), MaterializedBarError> {
        self.execution = BarExecutionState::new_with_configs(
            self.execution.positions().len(),
            configs,
            response_latency_ns,
        )?;
        self.execution_cursor = 0;
        self.order_batch.clear();
        self.fill_batch.clear();
        Ok(())
    }

    pub fn configure_transport(
        &mut self,
        entry_latency_ns: i64,
        response_latency_ns: i64,
    ) -> Result<(), MaterializedBarError> {
        if entry_latency_ns < 0 || response_latency_ns < 0 {
            return Err(MaterializedBarError::InvalidOrder);
        }
        self.entry_latency_ns = entry_latency_ns;
        if !self.execution.set_response_latency(response_latency_ns) {
            return Err(MaterializedBarError::InvalidOrder);
        }
        Ok(())
    }

    pub fn configure_ohlc_matching(
        &mut self,
        assumption: OhlcFillAssumption,
        volume_participation: f64,
    ) -> Result<(), MaterializedBarError> {
        let execution_timeframe_ns = self
            .feed
            .records()
            .iter()
            .map(|record| record.timeframe_ns)
            .min()
            .ok_or(MaterializedBarError::InvalidOrder)?;
        let mut execution_assets = vec![false; self.execution.positions().len()];
        for record in self.feed.records() {
            if record.timeframe_ns == execution_timeframe_ns {
                execution_assets[record.asset_no as usize] = true;
            }
        }
        self.matcher = ConfiguredBarMatcher::Ohlc(
            OhlcBarMatcher::new(
                execution_timeframe_ns,
                execution_assets,
                assumption,
                volume_participation,
            )
            .ok_or(MaterializedBarError::InvalidOrder)?,
        );
        Ok(())
    }

    pub fn configure_signal_close_matching(&mut self) -> Result<(), MaterializedBarError> {
        let execution_timeframe_ns = self
            .feed
            .records()
            .iter()
            .map(|record| record.timeframe_ns)
            .min()
            .ok_or(MaterializedBarError::InvalidOrder)?;
        let mut execution_assets = vec![false; self.execution.positions().len()];
        for record in self.feed.records() {
            if record.timeframe_ns == execution_timeframe_ns {
                execution_assets[record.asset_no as usize] = true;
            }
        }
        self.matcher = ConfiguredBarMatcher::SignalClose(SignalCloseBarMatcher::new(
            execution_timeframe_ns,
            execution_assets,
        ));
        Ok(())
    }

    pub fn configure_local_risk<R>(&mut self, risk: R)
    where
        R: crate::backtest::execution::LocalPreTradeRisk + 'static,
    {
        self.execution.configure_local_risk(risk);
    }

    pub fn configure_venue_risk<R>(&mut self, venue_id: VenueId, risk: R)
    where
        R: crate::backtest::execution::VenueRisk + 'static,
    {
        self.execution.configure_venue_risk(venue_id, risk);
    }

    pub fn add_execution_algorithm<A: ExecutionAlgorithm + 'static>(&mut self, algorithm: A) {
        self.platform.add_algorithm(algorithm);
    }

    pub fn add_simulation_hook<H: SimulationHook + 'static>(&mut self, hook: H) {
        self.platform.add_hook(hook);
    }

    pub fn register_contingency(&mut self, group: ContingencyGroup) -> bool {
        self.contingencies.insert(group)
    }

    pub fn set_exchange_balance(
        &mut self,
        venue_id: VenueId,
        currency: CurrencyId,
        balance: f64,
    ) -> Result<(), MaterializedBarError> {
        self.execution
            .set_exchange_balance(venue_id, currency, balance)?;
        Ok(())
    }

    pub fn reset(&mut self) -> Result<(), MaterializedBarError> {
        self.feed.reset()?;
        for slot in &mut self.histories {
            slot.history.reset();
        }
        self.refresh_views();
        self.commands.fill(OrderCommand::default());
        self.matcher.reset();
        self.execution.reset();
        self.execution_cursor = 0;
        self.order_batch.clear();
        self.fill_batch.clear();
        self.pending_bar_deliveries.clear();
        self.current_bar_delivery = None;
        self.action_scheduler.reset();
        self.platform.reset();
        self.platform_scratch.clear();
        self.contingencies.reset();
        self.held_contingent_orders.clear();
        self.contingency_report_cursor = 0;
        for book in &mut self.conditional_books {
            book.reset();
        }
        self.conditional_orders.clear();
        self.gtd_orders.clear();
        self.conditional_scratch.clear();
        self.timers.reset();
        self.scheduler.reset();
        for index in 0..self.configured_timers.len() {
            let timer = self.configured_timers[index];
            self.enqueue_timer(timer);
        }
        self.timer_scratch.clear();
        self.funding.clear();
        for index in 0..self.configured_funding.len() {
            let funding = self.configured_funding[index];
            self.enqueue_funding(funding);
        }
        self.funding_callback_pending = false;
        self.next_risk_order_id = u64::MAX;
        self.last_execution_closes.fill(None);
        self.last_bar_delivery_ts = i64::MIN;
        self.terminal_close_started = false;
        Ok(())
    }

    pub fn clear_results(&mut self) {
        self.execution.clear_results();
        self.execution_cursor = 0;
        self.contingency_report_cursor = 0;
        self.order_batch.clear();
        self.fill_batch.clear();
    }

    pub fn projected_execution_events(
        &self,
    ) -> &[(usize, crate::backtest::execution::ProjectedEvent)] {
        self.execution.projected()
    }

    fn execution_command_to_abi(
        &self,
        command: crate::backtest::execution::ExecutionCommand,
    ) -> Result<OrderCommand, MaterializedBarError> {
        use crate::backtest::execution::ExecutionCommand;
        let (venue_id, instrument_id) = match command {
            ExecutionCommand::Submit(request) => (request.venue_id, request.instrument_id),
            ExecutionCommand::Cancel(request) => (request.venue_id, request.instrument_id),
        };
        let asset_no = self
            .execution
            .asset_for_instrument(venue_id, instrument_id)
            .ok_or(MaterializedBarError::InvalidOrder)? as u64;
        Ok(match command {
            ExecutionCommand::Submit(request) => OrderCommand {
                kind: ORDER_COMMAND_SUBMIT,
                side: request.side as i8,
                time_in_force: match request.time_in_force {
                    crate::types::TimeInForce::GTC => 0,
                    crate::types::TimeInForce::GTX => 1,
                    crate::types::TimeInForce::FOK => 2,
                    crate::types::TimeInForce::IOC => 3,
                    crate::types::TimeInForce::Unsupported => {
                        return Err(MaterializedBarError::InvalidOrder);
                    }
                },
                order_type: match request.order_type {
                    crate::types::OrdType::Limit => 0,
                    crate::types::OrdType::Market => 1,
                    crate::types::OrdType::Unsupported => {
                        return Err(MaterializedBarError::InvalidOrder);
                    }
                },
                _reserved: [
                    u8::from(request.reduce_only),
                    0,
                    match request.origin {
                        crate::backtest::execution::OrderOrigin::Strategy => 0,
                        crate::backtest::execution::OrderOrigin::ExecutionAlgorithm => 1,
                        crate::backtest::execution::OrderOrigin::Liquidation => 2,
                    },
                    0,
                ],
                asset_no,
                order_id: request.client_order_id,
                price: request.price,
                qty: request.qty,
                trigger_price: 0.0,
                gtd_expiry_ts: 0,
            },
            ExecutionCommand::Cancel(request) => OrderCommand {
                kind: ORDER_COMMAND_CANCEL,
                asset_no,
                order_id: request.client_order_id,
                ..OrderCommand::default()
            },
        })
    }

    fn dispatch_platform_event(
        &mut self,
        key: crate::backtest::scheduler::EventKey,
    ) -> Result<(), MaterializedBarError> {
        self.platform_scratch.clear();
        self.platform
            .collect(key, &mut self.platform_scratch)
            .map_err(|_| MaterializedBarError::CommandOverflow)?;
        if self.platform_scratch.len() > self.commands.len() {
            return Err(MaterializedBarError::CommandOverflow);
        }
        let commands = std::mem::take(&mut self.platform_scratch);
        let command_count = commands.len();
        for (index, command) in commands.iter().copied().enumerate() {
            self.commands[index] = self.execution_command_to_abi(command)?;
        }
        self.platform_scratch = commands;
        self.platform_scratch.clear();
        let mut context = StrategyRuntimeContext {
            now: key.timestamp,
            num_commands: command_count,
            ..StrategyRuntimeContext::default()
        };
        self.process_commands(&mut context, true)
    }

    fn process_contingency_reports(&mut self, now: i64) -> Result<(), MaterializedBarError> {
        let reports: Vec<_> = self.execution.reports()[self.contingency_report_cursor..].to_vec();
        self.contingency_report_cursor = self.execution.reports().len();
        let mut actions = Vec::new();
        for report in reports {
            self.contingencies
                .on_report(report.order_id, report.status, &mut actions);
        }
        for action in actions.drain(..) {
            let order_id = match action {
                ContingencyAction::Activate(order_id) | ContingencyAction::Cancel(order_id) => {
                    order_id
                }
            };
            let held_key = self
                .held_contingent_orders
                .keys()
                .find(|(_, candidate)| *candidate == order_id)
                .copied();
            match action {
                ContingencyAction::Activate(_) => {
                    let Some(key) = held_key else { continue };
                    let mut pending = self.held_contingent_orders.remove(&key).unwrap();
                    pending.local_submit_ts = now;
                    pending.eligible_after = now.saturating_add(self.entry_latency_ns);
                    if pending.command._reserved[1] == 0 {
                        if !self.matcher.submit(pending) {
                            return Err(MaterializedBarError::DuplicateOrder {
                                asset_no: key.0,
                                order_id,
                            });
                        }
                    } else {
                        self.conditional_orders.insert(key, pending);
                    }
                    if let Some((_, stored)) = self.gtd_orders.get_mut(&key) {
                        *stored = pending;
                    }
                    self.schedule_action(
                        EventPhase::PostTradeRisk,
                        PendingBarAction::Submit {
                            command: pending.command,
                            local_submit_ts: now,
                            exchange_arrival_ts: now.saturating_add(self.entry_latency_ns),
                        },
                    );
                }
                ContingencyAction::Cancel(_) => {
                    if let Some(key) = held_key {
                        let pending = self.held_contingent_orders.remove(&key).unwrap();
                        self.gtd_orders.remove(&key);
                        self.execution.cancel(
                            pending.command,
                            pending.local_submit_ts,
                            now,
                            true,
                        )?;
                    } else if let Some(asset_no) = self
                        .execution
                        .reports()
                        .iter()
                        .rev()
                        .find(|report| report.order_id == order_id)
                        .map(|report| u64::from(report.asset_no))
                    {
                        self.schedule_action(
                            EventPhase::PostTradeRisk,
                            PendingBarAction::Cancel {
                                command: OrderCommand {
                                    kind: ORDER_COMMAND_CANCEL,
                                    asset_no,
                                    order_id,
                                    ..OrderCommand::default()
                                },
                                local_submit_ts: now,
                                exchange_arrival_ts: now,
                            },
                        );
                    }
                }
            }
        }
        Ok(())
    }

    fn process_commands(
        &mut self,
        ctx: &mut StrategyRuntimeContext,
        allow_submit: bool,
    ) -> Result<(), MaterializedBarError> {
        if ctx.num_commands > self.commands.len() {
            return Err(MaterializedBarError::CommandOverflow);
        }
        for index in 0..ctx.num_commands {
            let command = self.commands[index];
            let decoded = command
                .decode_execution(
                    ctx.now,
                    VenueId(0),
                    InstrumentId(command.asset_no as u32 + 1),
                )
                .map_err(|_| MaterializedBarError::InvalidOrder)?;
            match decoded {
                Some(crate::backtest::execution::ExecutionCommand::Submit(request)) => {
                    if !allow_submit
                        || command.asset_no as usize >= self.execution.positions().len()
                        || !self.matcher.supports_asset(command.asset_no as usize)
                        || command._reserved[1] > 2
                        || (command._reserved[1] != 0
                            && (!command.trigger_price.is_finite() || command.trigger_price <= 0.0))
                        || (command._reserved[1] == 2
                            && (!command.price.is_finite() || command.price <= 0.0))
                        || (command.gtd_expiry_ts != 0
                            && command.gtd_expiry_ts
                                <= ctx.now.saturating_add(self.entry_latency_ns))
                    {
                        return Err(MaterializedBarError::InvalidOrder);
                    }
                    debug_assert_eq!(request.client_order_id, command.order_id);
                    match self.execution.check_local_submit(command, ctx.now)? {
                        crate::backtest::execution::RiskDecision::Allow => {}
                        crate::backtest::execution::RiskDecision::Reject { reason } => {
                            self.execution.reject_local(command, ctx.now, reason)?;
                            continue;
                        }
                    }
                    if self.contingencies.should_reject(command.order_id) {
                        self.execution.reject_local(
                            command,
                            ctx.now,
                            crate::backtest::execution::RiskReason::Custom(0xC001),
                        )?;
                        continue;
                    }
                    let pending = PendingBarOrder {
                        command,
                        local_submit_ts: ctx.now,
                        eligible_after: ctx.now.saturating_add(self.entry_latency_ns),
                    };
                    let duplicate = self
                        .conditional_orders
                        .contains_key(&(command.asset_no, command.order_id))
                        || self
                            .gtd_orders
                            .contains_key(&(command.asset_no, command.order_id))
                        || self
                            .held_contingent_orders
                            .contains_key(&(command.asset_no, command.order_id));
                    if duplicate {
                        self.execution.reject_duplicate_local(command, ctx.now)?;
                        continue;
                    }
                    if self.contingencies.should_hold(command.order_id) {
                        self.held_contingent_orders
                            .insert((command.asset_no, command.order_id), pending);
                        if command.gtd_expiry_ts != 0 {
                            self.gtd_orders.insert(
                                (command.asset_no, command.order_id),
                                (command.gtd_expiry_ts, pending),
                            );
                        }
                        continue;
                    }
                    let inserted = if command._reserved[1] == 0 {
                        self.matcher.submit(pending)
                    } else {
                        self.conditional_orders
                            .insert((command.asset_no, command.order_id), pending)
                            .is_none()
                    };
                    if !inserted {
                        self.execution.reject_duplicate_local(command, ctx.now)?;
                        continue;
                    }
                    if command.gtd_expiry_ts != 0 {
                        self.gtd_orders.insert(
                            (command.asset_no, command.order_id),
                            (command.gtd_expiry_ts, pending),
                        );
                    }
                    self.schedule_action(
                        EventPhase::CommandArrival,
                        PendingBarAction::Submit {
                            command,
                            local_submit_ts: ctx.now,
                            exchange_arrival_ts: ctx.now.saturating_add(self.entry_latency_ns),
                        },
                    );
                }
                Some(crate::backtest::execution::ExecutionCommand::Cancel(request)) => {
                    debug_assert_eq!(request.client_order_id, command.order_id);
                    if let Some(pending) = self
                        .held_contingent_orders
                        .remove(&(command.asset_no, command.order_id))
                    {
                        self.gtd_orders
                            .remove(&(command.asset_no, command.order_id));
                        self.execution
                            .cancel(command, pending.local_submit_ts, ctx.now, true)?;
                        continue;
                    }
                    self.schedule_action(
                        EventPhase::CommandArrival,
                        PendingBarAction::Cancel {
                            command,
                            local_submit_ts: ctx.now,
                            exchange_arrival_ts: ctx.now.saturating_add(self.entry_latency_ns),
                        },
                    );
                }
                None => {}
            }
        }
        for command in &mut self.commands[..ctx.num_commands] {
            *command = OrderCommand::default();
        }
        ctx.num_commands = 0;
        Ok(())
    }

    fn match_at_next_open(&mut self, meta: BarBatchMeta) -> Result<(), MaterializedBarError> {
        if matches!(self.matcher, ConfiguredBarMatcher::SignalClose(_)) {
            return Ok(());
        }
        self.matcher.on_batch(meta, self.feed.batch());
        self.execution.apply(self.matcher.outcomes())?;
        for outcome in self.matcher.outcomes() {
            let command = match outcome {
                crate::backtest::bar::BarMatchOutcome::Fill { command, .. }
                | crate::backtest::bar::BarMatchOutcome::Expired { command, .. } => command,
            };
            self.gtd_orders
                .remove(&(command.asset_no, command.order_id));
        }
        self.process_conditional_bar(meta)?;
        let exchange_ts = self
            .feed
            .batch()
            .first()
            .map_or(meta.close_ts, |item| item.bar.open_ts);
        self.process_contingency_reports(exchange_ts)?;
        self.enqueue_risk_actions(exchange_ts)?;
        Ok(())
    }

    fn match_at_signal_close(
        &mut self,
        meta: BarBatchMeta,
        bars: &[BarItem],
    ) -> Result<(), MaterializedBarError> {
        if !matches!(self.matcher, ConfiguredBarMatcher::SignalClose(_)) {
            return Ok(());
        }
        self.matcher.on_batch(meta, bars);
        self.execution.apply(self.matcher.outcomes())?;
        for outcome in self.matcher.outcomes() {
            let command = match outcome {
                crate::backtest::bar::BarMatchOutcome::Fill { command, .. }
                | crate::backtest::bar::BarMatchOutcome::Expired { command, .. } => command,
            };
            self.gtd_orders
                .remove(&(command.asset_no, command.order_id));
        }
        self.process_contingency_reports(meta.close_ts)?;
        self.enqueue_risk_actions(meta.close_ts)?;
        Ok(())
    }

    fn process_conditional_bar(&mut self, meta: BarBatchMeta) -> Result<(), MaterializedBarError> {
        for index in 0..self.feed.batch().len() {
            let item = self.feed.batch()[index];
            self.conditional_scratch.clear();
            self.conditional_books[item.asset_no as usize].evaluate_range(
                meta.close_ts,
                item.bar.low,
                item.bar.high,
                &mut self.conditional_scratch,
            );
            for action_index in 0..self.conditional_scratch.len() {
                let action = self.conditional_scratch[action_index];
                let order_id = match action {
                    ConditionalAction::Trigger { order_id, .. }
                    | ConditionalAction::Expire { order_id } => order_id,
                };
                let Some(mut pending) = self.conditional_orders.remove(&(item.asset_no, order_id))
                else {
                    continue;
                };
                match action {
                    ConditionalAction::Trigger { .. } => {
                        pending.command._reserved[1] = 0;
                        pending.eligible_after = meta.close_ts;
                        if let Some((_, gtd)) = self.gtd_orders.get_mut(&(item.asset_no, order_id))
                        {
                            *gtd = pending;
                        }
                        if !self.matcher.submit(pending) {
                            return Err(MaterializedBarError::DuplicateOrder {
                                asset_no: item.asset_no,
                                order_id,
                            });
                        }
                    }
                    ConditionalAction::Expire { .. } => {
                        self.gtd_orders.remove(&(item.asset_no, order_id));
                        self.execution.apply(&[
                            crate::backtest::bar::BarMatchOutcome::Expired {
                                command: pending.command,
                                local_submit_ts: pending.local_submit_ts,
                                exchange_ts: meta.close_ts,
                            },
                        ])?;
                    }
                }
            }
            self.conditional_scratch.clear();
        }
        Ok(())
    }

    fn enqueue_risk_actions(&mut self, now: i64) -> Result<(), MaterializedBarError> {
        for action in self.execution.take_risk_actions() {
            match action {
                crate::backtest::execution::RiskAction::Cancel {
                    venue_id,
                    instrument_id,
                    order_id,
                    ..
                } => {
                    let Some(asset_no) =
                        self.execution.asset_for_instrument(venue_id, instrument_id)
                    else {
                        return Err(MaterializedBarError::InvalidOrder);
                    };
                    self.schedule_action(
                        EventPhase::PostTradeRisk,
                        PendingBarAction::Cancel {
                            command: OrderCommand {
                                kind: ORDER_COMMAND_CANCEL,
                                asset_no: asset_no as u64,
                                order_id,
                                ..Default::default()
                            },
                            local_submit_ts: now,
                            exchange_arrival_ts: now,
                        },
                    );
                }
                crate::backtest::execution::RiskAction::Liquidate {
                    venue_id,
                    instrument_id,
                    ..
                } => {
                    let Some(asset_no) =
                        self.execution.asset_for_instrument(venue_id, instrument_id)
                    else {
                        return Err(MaterializedBarError::InvalidOrder);
                    };
                    let position = self.execution.exchange_position(asset_no).unwrap_or(0.0);
                    if position == 0.0 {
                        continue;
                    }
                    let command = OrderCommand {
                        kind: ORDER_COMMAND_SUBMIT,
                        side: if position > 0.0 { -1 } else { 1 },
                        time_in_force: 3,
                        order_type: 1,
                        _reserved: [1, 0, 2, 0],
                        asset_no: asset_no as u64,
                        order_id: self.next_risk_order_id,
                        price: 0.0,
                        qty: position.abs(),
                        trigger_price: 0.0,
                        gtd_expiry_ts: 0,
                    };
                    self.next_risk_order_id = self.next_risk_order_id.saturating_sub(1);
                    if !self.matcher.submit(PendingBarOrder {
                        command,
                        local_submit_ts: now,
                        eligible_after: now,
                    }) {
                        return Err(MaterializedBarError::DuplicateOrder {
                            asset_no: command.asset_no,
                            order_id: command.order_id,
                        });
                    }
                    self.schedule_action(
                        EventPhase::PostTradeRisk,
                        PendingBarAction::Submit {
                            command,
                            local_submit_ts: now,
                            exchange_arrival_ts: now,
                        },
                    );
                }
            }
        }
        Ok(())
    }

    fn schedule_action(&mut self, phase: EventPhase, action: PendingBarAction) {
        let timestamp = action.exchange_arrival_ts();
        let command = match action {
            PendingBarAction::Submit { command, .. } | PendingBarAction::Cancel { command, .. } => {
                command
            }
        };
        self.action_scheduler
            .schedule(timestamp, phase, 0, 0, command.asset_no as u32, action);
    }

    fn next_expiry_ts(&self) -> Option<i64> {
        self.gtd_orders.values().map(|(expiry, _)| *expiry).min()
    }

    fn next_timer_ts(&self) -> Option<i64> {
        self.timer_scratch
            .first()
            .map(|timer| timer.deadline_ts)
            .or_else(|| self.timers.next_timestamp())
    }

    fn next_timer_event(&mut self) -> Option<RuntimeEvent<'_>> {
        let deadline_ts = self.next_timer_ts()?;
        if self.timer_scratch.is_empty() {
            self.timers.drain_due(deadline_ts, &mut self.timer_scratch);
        }
        let timer = self.execution.project_timer(self.timer_scratch.remove(0));
        self.current_timer = RuntimeTimer {
            deadline_ts: timer.deadline_ts,
            owner_id: timer.id.owner_id,
            timer_id: timer.id.timer_id,
        };
        let bytes = unsafe {
            std::slice::from_raw_parts(
                (&self.current_timer as *const RuntimeTimer).cast::<u8>(),
                std::mem::size_of::<RuntimeTimer>(),
            )
        };
        Some(RuntimeEvent {
            kind: StrategyEventKind::Timer as u32,
            now: deadline_ts,
            payload: RuntimePayload::Pod {
                ptr: NonNull::new(bytes.as_ptr().cast_mut().cast::<c_void>()).unwrap(),
                len: bytes.len(),
            },
        })
    }

    fn process_next_funding(&mut self) -> Result<bool, MaterializedBarError> {
        let Some(funding) = self.funding.pop_front() else {
            return Ok(false);
        };
        self.execution.settle_funding(funding)?;
        Ok(true)
    }

    fn deliver_next_runtime_event(&mut self) -> Result<bool, MaterializedBarError> {
        if self.execution.next_is_funding() {
            let (asset_no, report) = self.execution.deliver_next_funding()?.unwrap();
            self.current_funding = self
                .configured_funding
                .iter()
                .find(|funding| funding.event_id == report.event.event_id)
                .copied()
                .unwrap_or_default();
            self.current_funding.asset_no = asset_no as u32;
            self.current_funding.delivery_ts = report.delivery_ts;
            self.current_funding.position_qty = report.position_qty;
            self.current_funding.amount = report.amount;
            self.funding_callback_pending = true;
            return Ok(true);
        }
        self.execution.deliver_next()?;
        Ok(false)
    }

    fn next_funding_event(&mut self) -> RuntimeEvent<'_> {
        self.funding_callback_pending = false;
        let bytes = unsafe {
            std::slice::from_raw_parts(
                (&self.current_funding as *const RuntimeFunding).cast::<u8>(),
                std::mem::size_of::<RuntimeFunding>(),
            )
        };
        RuntimeEvent {
            kind: StrategyEventKind::Funding as u32,
            now: self.current_funding.delivery_ts,
            payload: RuntimePayload::Pod {
                ptr: NonNull::new(bytes.as_ptr().cast_mut().cast::<c_void>()).unwrap(),
                len: bytes.len(),
            },
        }
    }

    fn process_next_expiry(&mut self) -> Result<bool, MaterializedBarError> {
        let Some(next_expiry) = self.next_expiry_ts() else {
            return Ok(false);
        };
        let expired: Vec<_> = self
            .gtd_orders
            .iter()
            .filter_map(|(key, (expiry, pending))| {
                (*expiry <= next_expiry).then_some((*key, *pending))
            })
            .collect();
        for ((asset_no, order_id), pending) in expired {
            self.gtd_orders.remove(&(asset_no, order_id));
            self.held_contingent_orders.remove(&(asset_no, order_id));
            self.conditional_orders.remove(&(asset_no, order_id));
            self.conditional_books[asset_no as usize].cancel(order_id);
            self.matcher.cancel(asset_no, order_id);
            self.execution
                .apply(&[crate::backtest::bar::BarMatchOutcome::Expired {
                    command: pending.command,
                    local_submit_ts: pending.local_submit_ts,
                    exchange_ts: next_expiry,
                }])?;
        }
        Ok(true)
    }

    fn process_next_action(&mut self) -> Result<bool, MaterializedBarError> {
        let Some(action) = self.action_scheduler.pop().map(|event| event.payload) else {
            return Ok(false);
        };
        match action {
            PendingBarAction::Submit {
                command,
                local_submit_ts,
                exchange_arrival_ts,
            } => {
                if self
                    .execution
                    .arrive(command, local_submit_ts, exchange_arrival_ts)?
                {
                    if command._reserved[1] != 0 {
                        let trigger_kind = if command._reserved[1] == 1 {
                            TriggerKind::StopMarket
                        } else {
                            TriggerKind::StopLimit
                        };
                        if !self.conditional_books[command.asset_no as usize].insert(
                            ConditionalOrder {
                                order_id: command.order_id,
                                side: if command.side == 1 {
                                    crate::types::Side::Buy
                                } else {
                                    crate::types::Side::Sell
                                },
                                trigger_kind,
                                trigger_price: command.trigger_price,
                                gtd_expiry_ts: (command.gtd_expiry_ts != 0)
                                    .then_some(command.gtd_expiry_ts),
                            },
                        ) {
                            return Err(MaterializedBarError::DuplicateOrder {
                                asset_no: command.asset_no,
                                order_id: command.order_id,
                            });
                        }
                    }
                } else {
                    self.matcher.cancel(command.asset_no, command.order_id);
                    self.conditional_orders
                        .remove(&(command.asset_no, command.order_id));
                    self.gtd_orders
                        .remove(&(command.asset_no, command.order_id));
                }
            }
            PendingBarAction::Cancel {
                command,
                local_submit_ts,
                exchange_arrival_ts,
            } => {
                let canceled = self.matcher.cancel(command.asset_no, command.order_id)
                    || self.conditional_books[command.asset_no as usize].cancel(command.order_id);
                self.conditional_orders
                    .remove(&(command.asset_no, command.order_id));
                self.gtd_orders
                    .remove(&(command.asset_no, command.order_id));
                self.execution
                    .cancel(command, local_submit_ts, exchange_arrival_ts, canceled)?;
            }
        }
        Ok(true)
    }

    fn next_execution_event(&mut self) -> Option<RuntimeEvent<'_>> {
        use crate::backtest::execution::ProjectedEventKind;

        let (_, projected) = *self.execution.projected().get(self.execution_cursor)?;
        self.execution_cursor += 1;
        let report = projected.report;
        match projected.kind {
            ProjectedEventKind::Order => {
                self.order_batch.clear();
                self.order_batch.push(OrderEvent {
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
                    reason: execution_reason_code(report.reason),
                    side: report.side as i8,
                    status: report.status as u8,
                    request: 0,
                    maker: u8::from(report.maker),
                    _reserved: [0; 4],
                });
                Some(RuntimeEvent {
                    kind: StrategyEventKind::Order as u32,
                    now: report.delivery_ts,
                    payload: RuntimePayload::Orders(&self.order_batch),
                })
            }
            ProjectedEventKind::Filled => {
                self.fill_batch.clear();
                self.fill_batch.push(FillEvent {
                    asset_no: report.asset_no as u64,
                    order_id: report.order_id,
                    venue_order_id: report.venue_order_id,
                    exch_ts: report.exchange_ts,
                    local_ts: report.delivery_ts,
                    sequence: report.sequence,
                    price: report.exec_price,
                    qty: report.exec_qty,
                    venue_no: report.venue_id.0,
                    instrument_id: report.instrument_id.0,
                    reason: execution_reason_code(report.reason),
                    side: report.side as i8,
                    maker: u8::from(report.maker),
                    _reserved: [0; 2],
                });
                Some(RuntimeEvent {
                    kind: StrategyEventKind::Filled as u32,
                    now: report.delivery_ts,
                    payload: RuntimePayload::Fills(&self.fill_batch),
                })
            }
            ProjectedEventKind::Position => Some(RuntimeEvent {
                kind: StrategyEventKind::Position as u32,
                now: report.delivery_ts,
                payload: RuntimePayload::None,
            }),
        }
    }
}

impl RuntimeEventSource for MaterializedBarSource {
    type Error = MaterializedBarError;

    fn classify_error(
        &self,
        error: &Self::Error,
    ) -> (
        crate::backtest::result::EngineComponent,
        crate::backtest::result::EngineErrorCode,
    ) {
        use crate::backtest::result::{EngineComponent, EngineErrorCode};
        match error {
            MaterializedBarError::InvalidTimeframe
            | MaterializedBarError::IntervalMismatch
            | MaterializedBarError::PartialBar
            | MaterializedBarError::IncompleteBar
            | MaterializedBarError::InvalidOhlcv
            | MaterializedBarError::Unsorted
            | MaterializedBarError::DuplicateAsset
            | MaterializedBarError::DataSource => {
                (EngineComponent::DataSource, EngineErrorCode::InvalidData)
            }
            MaterializedBarError::InvalidOrder
            | MaterializedBarError::DuplicateOrder { .. }
            | MaterializedBarError::CommandOverflow => (
                EngineComponent::Strategy,
                EngineErrorCode::InvalidConfiguration,
            ),
            MaterializedBarError::SharedExecution(error) => match error {
                BarExecutionError::Coordinator(_) => {
                    (EngineComponent::Matching, EngineErrorCode::InvalidState)
                }
                BarExecutionError::Account(_) => {
                    (EngineComponent::Account, EngineErrorCode::AccountInvariant)
                }
                BarExecutionError::Funding(_) => {
                    (EngineComponent::Account, EngineErrorCode::AccountInvariant)
                }
                BarExecutionError::Instrument(_) | BarExecutionError::InvalidConfiguration => (
                    EngineComponent::Configuration,
                    EngineErrorCode::InvalidConfiguration,
                ),
            },
        }
    }

    fn next_event(&mut self) -> Result<Option<RuntimeEvent<'_>>, Self::Error> {
        loop {
            if self.contingency_report_cursor < self.execution.reports().len() {
                let now = self.execution.reports()[self.contingency_report_cursor..]
                    .iter()
                    .map(|report| report.exchange_ts)
                    .max()
                    .unwrap();
                self.process_contingency_reports(now)?;
                continue;
            }
            if self.execution_cursor < self.execution.projected().len() {
                return Ok(self.next_execution_event());
            }
            if self.funding_callback_pending {
                return Ok(Some(self.next_funding_event()));
            }
            let market = self
                .feed
                .peek_open_ts()?
                .map(|timestamp| (timestamp, BarBoundary::MarketOpen));
            let bar_delivery = self
                .pending_bar_deliveries
                .front()
                .map(|delivery| delivery.delivery_ts);
            if market.is_none() && bar_delivery.is_none() && !self.terminal_close_started {
                self.close_terminal_positions()?;
                continue;
            }
            match self.next_boundary(market, bar_delivery) {
                Some(BarBoundary::ResponseDelivery) => {
                    self.deliver_next_runtime_event()?;
                }
                Some(BarBoundary::FundingSettlement) => {
                    self.process_next_funding()?;
                }
                Some(BarBoundary::OrderExpiry) => {
                    self.process_next_expiry()?;
                }
                Some(BarBoundary::CommandArrival) => {
                    self.process_next_action()?;
                }
                Some(BarBoundary::Timer) => return Ok(self.next_timer_event()),
                Some(BarBoundary::MarketOpen) => {
                    let meta = self
                        .feed
                        .next_batch()?
                        .expect("peeked Bar open must have a batch");
                    let exchange_ts = self
                        .feed
                        .batch()
                        .first()
                        .map_or(meta.close_ts, |item| item.bar.open_ts);
                    if meta.timeframe_ns == self.execution_timeframe_ns {
                        for item in self.feed.batch() {
                            if item.bar.flags & BAR_EMPTY == 0 {
                                self.last_execution_closes[item.asset_no as usize] =
                                    Some((item.bar.close_ts, item.bar.close));
                            }
                        }
                    }
                    self.dispatch_platform_event(crate::backtest::scheduler::EventKey {
                        timestamp: exchange_ts,
                        phase: EventPhase::MarketDelivery,
                        source_priority: 0,
                        venue_no: 0,
                        asset_no: 0,
                        sequence: 0,
                    })?;
                    while self
                        .action_scheduler
                        .peek_key()
                        .is_some_and(|key| action_precedes_matching(key, exchange_ts))
                    {
                        self.process_next_action()?;
                    }
                    self.match_at_next_open(meta)?;
                    let delivery = PendingBarDelivery {
                        delivery_ts: meta.close_ts.saturating_add(self.feed_latency_ns),
                        meta,
                        bars: self.feed.batch().to_vec(),
                    };
                    let index = self
                        .pending_bar_deliveries
                        .iter()
                        .position(|queued| {
                            (
                                queued.delivery_ts,
                                queued.meta.close_ts,
                                queued.meta.timeframe_ns,
                            ) > (
                                delivery.delivery_ts,
                                delivery.meta.close_ts,
                                delivery.meta.timeframe_ns,
                            )
                        })
                        .unwrap_or(self.pending_bar_deliveries.len());
                    self.pending_bar_deliveries.insert(index, delivery);
                }
                Some(BarBoundary::BarClose) => {
                    self.current_bar_delivery = self.pending_bar_deliveries.pop_front();
                    let delivery = self.current_bar_delivery.as_ref().unwrap();
                    return Ok(Some(RuntimeEvent {
                        kind: StrategyEventKind::Bar as u32,
                        now: delivery.delivery_ts,
                        payload: RuntimePayload::Bars {
                            timeframe_ns: delivery.meta.timeframe_ns,
                            close_ts: delivery.meta.close_ts,
                            bars: &delivery.bars,
                        },
                    }));
                }
                None => return Ok(None),
            }
        }
    }

    fn after_callback(
        &mut self,
        kind: u32,
        ctx: &mut StrategyRuntimeContext,
    ) -> Result<(), Self::Error> {
        let signal_close_batch = if kind == StrategyEventKind::Bar as u32
            && matches!(self.matcher, ConfiguredBarMatcher::SignalClose(_))
        {
            self.current_bar_delivery
                .as_ref()
                .map(|delivery| (delivery.meta, delivery.bars.clone()))
        } else {
            None
        };
        if kind == StrategyEventKind::Bar as u32 {
            let delivery = self
                .current_bar_delivery
                .as_ref()
                .expect("Bar callback must have one pending delivery");
            self.last_bar_delivery_ts = self.last_bar_delivery_ts.max(delivery.delivery_ts);
            for item in &delivery.bars {
                if let Some(slot) = self.histories.iter_mut().find(|slot| {
                    slot.asset_no == item.asset_no
                        && slot.timeframe_ns == item.bar.close_ts - item.bar.open_ts
                }) {
                    slot.history.push(item.bar);
                }
            }
            self.refresh_views();
            self.current_bar_delivery = None;
        }
        self.process_commands(
            ctx,
            kind != StrategyEventKind::Error as u32 && kind != StrategyEventKind::Stop as u32,
        )?;
        if let Some((meta, bars)) = signal_close_batch {
            while self
                .action_scheduler
                .peek_key()
                .is_some_and(|key| action_precedes_matching(key, meta.close_ts))
            {
                self.process_next_action()?;
            }
            self.match_at_signal_close(meta, &bars)?;
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("invalid strategy event id {0}")]
    InvalidEventId(u32),
    #[error("strategy callback for event {event_id} returned error code {code}")]
    Callback { event_id: u32, code: i32 },
    #[error("{component:?} failed: {context}")]
    Source {
        component: crate::backtest::result::EngineComponent,
        code: crate::backtest::result::EngineErrorCode,
        context: String,
    },
}

fn classified_source_error<S: RuntimeEventSource>(source: &S, error: S::Error) -> RuntimeError {
    let (component, code) = source.classify_error(&error);
    RuntimeError::Source {
        component,
        code,
        context: error.to_string(),
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct RuntimeRunStats {
    pub callback_count: [u64; EVENT_SLOT_COUNT],
    pub market_event_count: u64,
    pub order_event_count: u64,
    pub fill_event_count: u64,
    pub start_exchange_ts: i64,
    pub end_exchange_ts: i64,
    pub start_delivery_ts: i64,
    pub end_delivery_ts: i64,
    pub termination: crate::backtest::result::RunTermination,
}

impl Default for RuntimeRunStats {
    fn default() -> Self {
        Self {
            callback_count: [0; EVENT_SLOT_COUNT],
            market_event_count: 0,
            order_event_count: 0,
            fill_event_count: 0,
            start_exchange_ts: i64::MAX,
            end_exchange_ts: i64::MIN,
            start_delivery_ts: i64::MAX,
            end_delivery_ts: i64::MIN,
            termination: crate::backtest::result::RunTermination::DataEnd,
        }
    }
}

fn runtime_event_exchange_bounds(event: &RuntimeEvent<'_>) -> Option<(i64, i64)> {
    match &event.payload {
        RuntimePayload::Ticks(items) => items
            .iter()
            .map(|item| item.event.exch_ts)
            .min()
            .zip(items.iter().map(|item| item.event.exch_ts).max()),
        RuntimePayload::Bars { close_ts, bars, .. } => bars
            .iter()
            .map(|item| item.bar.open_ts)
            .min()
            .map(|open_ts| (open_ts, *close_ts)),
        RuntimePayload::Orders(items) => items
            .iter()
            .map(|item| item.exch_ts)
            .min()
            .zip(items.iter().map(|item| item.exch_ts).max()),
        RuntimePayload::Fills(items) => items
            .iter()
            .map(|item| item.exch_ts)
            .min()
            .zip(items.iter().map(|item| item.exch_ts).max()),
        RuntimePayload::Pod { ptr, len }
            if event.kind == StrategyEventKind::Funding as u32
                && *len == std::mem::size_of::<RuntimeFunding>() =>
        {
            let funding = unsafe { &*ptr.as_ptr().cast::<RuntimeFunding>() };
            Some((funding.settlement_ts, funding.settlement_ts))
        }
        RuntimePayload::None | RuntimePayload::Pod { .. } => None,
    }
}

/// Runs an event source entirely under Rust ownership. Foreign code is entered only for
/// registered event callbacks and always receives the same single context pointer.
pub fn run_event_runtime<S: RuntimeEventSource>(
    source: &mut S,
    callbacks: &CallbackRegistry,
    ctx: &mut StrategyRuntimeContext,
) -> Result<(), RuntimeError> {
    run_event_runtime_counted(source, callbacks, ctx).map(|_| ())
}

pub fn run_event_runtime_counted<S: RuntimeEventSource>(
    source: &mut S,
    callbacks: &CallbackRegistry,
    ctx: &mut StrategyRuntimeContext,
) -> Result<RuntimeRunStats, RuntimeError> {
    let mut stats = RuntimeRunStats::default();
    ctx.clear_views();
    stats.callback_count[StrategyEventKind::Start.slot()] += 1;
    let start_result = dispatch(StrategyEventKind::Start as u32, callbacks, ctx).and_then(|()| {
        source
            .after_callback(StrategyEventKind::Start as u32, ctx)
            .map_err(|error| classified_source_error(source, error))
    });

    let result = match start_result {
        Err(error) => Err(error),
        Ok(()) => loop {
            if ctx.stop_requested != 0 {
                stats.termination = crate::backtest::result::RunTermination::StrategyStop;
                break Ok(());
            }
            let event = match source.next_event() {
                Ok(event) => event,
                Err(error) => break Err(classified_source_error(source, error)),
            };
            let Some(event) = event else {
                break Ok(());
            };
            if (event.kind as usize) < EVENT_SLOT_COUNT {
                stats.callback_count[event.kind as usize] += 1;
            }
            stats.start_delivery_ts = stats.start_delivery_ts.min(event.now);
            stats.end_delivery_ts = stats.end_delivery_ts.max(event.now);
            if let Some((exchange_start, exchange_end)) = runtime_event_exchange_bounds(&event) {
                stats.start_exchange_ts = stats.start_exchange_ts.min(exchange_start);
                stats.end_exchange_ts = stats.end_exchange_ts.max(exchange_end);
            }
            match &event.payload {
                RuntimePayload::Ticks(items) => stats.market_event_count += items.len() as u64,
                RuntimePayload::Bars { bars, .. } => stats.market_event_count += bars.len() as u64,
                RuntimePayload::Orders(items) => stats.order_event_count += items.len() as u64,
                RuntimePayload::Fills(items) => stats.fill_event_count += items.len() as u64,
                RuntimePayload::None | RuntimePayload::Pod { .. } => {}
            }
            populate_context(ctx, &event);
            let kind = event.kind;
            if let Err(error) = dispatch(kind, callbacks, ctx) {
                break Err(error);
            }
            if let Err(error) = source.after_callback(kind, ctx) {
                break Err(classified_source_error(source, error));
            }
        },
    };

    // Give the strategy one native error notification before stopping. The original error
    // remains authoritative if on_error itself fails.
    let error_hook = if result.is_err() {
        stats.callback_count[StrategyEventKind::Error.slot()] += 1;
        if ctx.last_error == 0 {
            ctx.last_error = -1;
        }
        ctx.clear_views();
        dispatch(StrategyEventKind::Error as u32, callbacks, ctx).and_then(|()| {
            source
                .after_callback(StrategyEventKind::Error as u32, ctx)
                .map_err(|error| classified_source_error(source, error))
        })
    } else {
        Ok(())
    };

    // Stop is guaranteed exactly once even when an earlier callback failed.
    ctx.clear_views();
    stats.callback_count[StrategyEventKind::Stop.slot()] += 1;
    let stop_result = dispatch(StrategyEventKind::Stop as u32, callbacks, ctx);
    let stop_hook = source
        .after_callback(StrategyEventKind::Stop as u32, ctx)
        .map_err(|error| classified_source_error(source, error));
    let finish_hook = source
        .finish()
        .map_err(|error| classified_source_error(source, error));
    result
        .and(error_hook)
        .and(stop_result)
        .and(stop_hook)
        .and(finish_hook)?;
    if stats.start_exchange_ts == i64::MAX {
        stats.start_exchange_ts = 0;
        stats.end_exchange_ts = 0;
    }
    if stats.start_delivery_ts == i64::MAX {
        stats.start_delivery_ts = 0;
        stats.end_delivery_ts = 0;
    }
    Ok(stats)
}

pub fn run_event_runtime_scoped<S: RuntimeEventSource>(
    run_id: u64,
    source: &mut S,
    callbacks: &CallbackRegistry,
    ctx: &mut StrategyRuntimeContext,
) -> Result<RuntimeRunStats, crate::backtest::result::StructuredEngineError> {
    use crate::backtest::{
        result::{EngineComponent, EngineErrorCode, StructuredEngineError},
        scheduler::{EventKey, EventPhase},
    };
    run_event_runtime_counted(source, callbacks, ctx).map_err(|error| {
        let (component, code) = match &error {
            RuntimeError::Callback { .. } => {
                (EngineComponent::Strategy, EngineErrorCode::CallbackFailed)
            }
            RuntimeError::InvalidEventId(_) => {
                (EngineComponent::Scheduler, EngineErrorCode::InvalidState)
            }
            RuntimeError::Source {
                component, code, ..
            } => (*component, *code),
        };
        StructuredEngineError {
            run_id,
            component,
            event_key: (ctx.event_kind != StrategyEventKind::Start as u32 || ctx.now != 0)
                .then_some(EventKey {
                    timestamp: ctx.now,
                    phase: EventPhase::StrategyCallback,
                    source_priority: 0,
                    venue_no: 0,
                    asset_no: 0,
                    sequence: ctx.generation,
                }),
            code,
            context: error.to_string(),
        }
    })
}

fn populate_context(ctx: &mut StrategyRuntimeContext, event: &RuntimeEvent<'_>) {
    ctx.clear_views();
    ctx.event_kind = event.kind;
    ctx.now = event.now;
    match &event.payload {
        RuntimePayload::None => {}
        RuntimePayload::Ticks(ticks) => {
            ctx.ticks_ptr = ticks.as_ptr();
            ctx.num_ticks = ticks.len();
        }
        RuntimePayload::Bars {
            timeframe_ns,
            close_ts,
            bars,
        } => {
            ctx.bars_ptr = bars.as_ptr();
            ctx.num_bars = bars.len();
            ctx.bar_timeframe_ns = *timeframe_ns;
            ctx.bar_close_ts = *close_ts;
        }
        RuntimePayload::Fills(fills) => {
            ctx.fills_ptr = fills.as_ptr();
            ctx.num_fills = fills.len();
        }
        RuntimePayload::Orders(orders) => {
            ctx.orders_ptr = orders.as_ptr();
            ctx.num_orders = orders.len();
        }
        RuntimePayload::Pod { ptr, len } => {
            ctx.payload_ptr = ptr.as_ptr();
            ctx.payload_len = *len;
        }
    }
}

fn dispatch(
    event_id: u32,
    callbacks: &CallbackRegistry,
    ctx: &mut StrategyRuntimeContext,
) -> Result<(), RuntimeError> {
    ctx.event_kind = event_id;
    ctx.generation = ctx.generation.wrapping_add(1);
    let Some(callback) = callbacks.get(event_id) else {
        return Ok(());
    };
    // Safety: registration requires the caller to provide a valid native callback with the
    // declared ABI. `ctx` remains pinned for the duration of this synchronous invocation.
    let code = unsafe { callback(ctx) };
    if code == 0 {
        Ok(())
    } else {
        ctx.last_error = code as i64;
        Err(RuntimeError::Callback { event_id, code })
    }
}

/// Prepared, reusable Bar runtime. Read-only data, callbacks and compiled strategy pointers are
/// retained; `reset` rewinds every mutable engine/context buffer before the next run.
pub struct PreparedBarRuntime {
    pub source: MaterializedBarSource,
    pub callbacks: CallbackRegistry,
    pub context: StrategyRuntimeContext,
    pub result_template: crate::backtest::result::BacktestResult,
    next_run_id: u64,
}

impl PreparedBarRuntime {
    pub fn new(
        mut source: MaterializedBarSource,
        callbacks: CallbackRegistry,
        mut context: StrategyRuntimeContext,
        result_template: crate::backtest::result::BacktestResult,
    ) -> Self {
        source.set_end_policy(result_template.end_policy);
        source.configure_context(&mut context);
        Self {
            source,
            callbacks,
            context,
            result_template,
            next_run_id: 1,
        }
    }
}

impl crate::backtest::result::ReusableRuntime for PreparedBarRuntime {
    type Error = crate::backtest::result::StructuredEngineError;
    type Output = crate::backtest::result::BacktestResult;

    fn run_once(&mut self) -> Result<Self::Output, Self::Error> {
        use std::time::Instant;

        self.source.configure_context(&mut self.context);
        let start = Instant::now();
        let cpu_start = crate::backtest::result::process_cpu_time_ns();
        let stats = run_event_runtime_scoped(
            self.next_run_id,
            &mut self.source,
            &self.callbacks,
            &mut self.context,
        )?;
        let mut result = self.result_template.clone();
        result.run_id = self.next_run_id;
        self.next_run_id += 1;
        result.wall_time_ns = start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
        result.cpu_time_ns =
            crate::backtest::result::process_cpu_time_ns().saturating_sub(cpu_start);
        result.start_exchange_ts = stats.start_exchange_ts;
        result.end_exchange_ts = stats.end_exchange_ts;
        result.start_delivery_ts = stats.start_delivery_ts;
        result.end_delivery_ts = stats.end_delivery_ts;
        result.market_event_count = stats.market_event_count;
        result.callback_count = stats.callback_count.to_vec();
        let reports = self.source.execution_reports();
        let counts = crate::backtest::result::execution_report_counts(reports);
        result.order_count = counts.order_count;
        result.fill_count += counts.fill_count;
        result.reject_count += counts.reject_count;
        result.cancel_count += counts.cancel_count;
        result.expire_count += counts.expire_count;
        result.execution_reports.extend_from_slice(reports);
        for report in self.source.funding_reports().iter().copied() {
            result.record_funding(report);
        }
        let (exchange_final, local_delivered_final) = self.source.account_snapshots();
        result.exchange_final = exchange_final;
        result.local_delivered_final = local_delivered_final;
        result.termination = stats.termination;
        Ok(result)
    }

    fn reset(&mut self) -> Result<(), Self::Error> {
        self.source
            .reset()
            .map_err(|error| crate::backtest::result::StructuredEngineError {
                run_id: self.next_run_id,
                component: crate::backtest::result::EngineComponent::DataSource,
                event_key: None,
                code: crate::backtest::result::EngineErrorCode::InvalidData,
                context: error.to_string(),
            })?;
        self.context.clear_views();
        self.context.stop_requested = 0;
        self.context.last_error = 0;
        self.context.num_commands = 0;
        self.context.generation = self.context.generation.wrapping_add(1);
        if !self.context.state_f64_ptr.is_null() {
            unsafe {
                std::slice::from_raw_parts_mut(
                    self.context.state_f64_ptr,
                    self.context.state_f64_len,
                )
                .fill(0.0);
            }
        }
        if !self.context.state_i64_ptr.is_null() {
            unsafe {
                std::slice::from_raw_parts_mut(
                    self.context.state_i64_ptr,
                    self.context.state_i64_len,
                )
                .fill(0);
            }
        }
        self.source.configure_context(&mut self.context);
        Ok(())
    }

    fn clear_results(&mut self) {
        self.source.clear_results();
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use super::*;
    use crate::{
        backtest::result::{
            BacktestResult, ModelIdentity, PreparedRunner, ReproducibilityMetadata,
        },
        market_data::{BAR_COMPLETE, Bar},
    };

    #[derive(Default)]
    struct State {
        calls: Vec<u32>,
        history_commits: usize,
        finalized: usize,
    }

    unsafe extern "C" fn record(ctx: *mut StrategyRuntimeContext) -> i32 {
        let ctx = unsafe { &mut *ctx };
        let state = unsafe { &mut *(ctx.user_data as *mut State) };
        state.calls.push(ctx.event_kind);
        0
    }

    unsafe extern "C" fn fail_start(ctx: *mut StrategyRuntimeContext) -> i32 {
        let ctx = unsafe { &mut *ctx };
        let state = unsafe { &mut *(ctx.user_data as *mut State) };
        state.calls.push(ctx.event_kind);
        -7
    }

    unsafe extern "C" fn request_stop(ctx: *mut StrategyRuntimeContext) -> i32 {
        let ctx = unsafe { &mut *ctx };
        ctx.stop_requested = 1;
        0
    }

    struct Source {
        events: Vec<(u32, i64)>,
        next: usize,
        state: *mut State,
    }

    impl RuntimeEventSource for Source {
        type Error = Infallible;

        fn next_event(&mut self) -> Result<Option<RuntimeEvent<'_>>, Self::Error> {
            let Some(&(kind, now)) = self.events.get(self.next) else {
                return Ok(None);
            };
            self.next += 1;
            Ok(Some(RuntimeEvent {
                kind,
                now,
                payload: RuntimePayload::None,
            }))
        }

        fn after_callback(
            &mut self,
            kind: u32,
            _ctx: &mut StrategyRuntimeContext,
        ) -> Result<(), Self::Error> {
            if kind == StrategyEventKind::Bar as u32 {
                unsafe { (*self.state).history_commits += 1 };
            }
            Ok(())
        }

        fn finish(&mut self) -> Result<(), Self::Error> {
            unsafe { (*self.state).finalized += 1 };
            Ok(())
        }
    }

    #[test]
    fn rust_owns_loop_and_dispatches_extensible_callbacks() {
        let mut state = State::default();
        let mut source = Source {
            events: vec![
                (StrategyEventKind::Filled as u32, 1),
                (StrategyEventKind::Bar as u32, 2),
                (StrategyEventKind::Tick as u32, 2),
                (10, 3),
            ],
            next: 0,
            state: &mut state,
        };
        let mut callbacks = CallbackRegistry::default();
        for kind in [
            StrategyEventKind::Start,
            StrategyEventKind::Filled,
            StrategyEventKind::Bar,
            StrategyEventKind::Tick,
            StrategyEventKind::Stop,
        ] {
            callbacks.set(kind, record);
        }
        callbacks.set_custom(10, record).unwrap();
        let mut ctx = StrategyRuntimeContext {
            user_data: (&mut state as *mut State).cast(),
            ..Default::default()
        };

        run_event_runtime(&mut source, &callbacks, &mut ctx).unwrap();

        assert_eq!(
            state.calls,
            vec![
                StrategyEventKind::Start as u32,
                StrategyEventKind::Filled as u32,
                StrategyEventKind::Bar as u32,
                StrategyEventKind::Tick as u32,
                10,
                StrategyEventKind::Stop as u32,
            ]
        );
        assert_eq!(state.history_commits, 1);
    }

    #[test]
    fn stop_is_dispatched_when_start_fails() {
        let mut state = State::default();
        let mut source = Source {
            events: Vec::new(),
            next: 0,
            state: &mut state,
        };
        let mut callbacks = CallbackRegistry::default();
        callbacks.set(StrategyEventKind::Start, fail_start);
        callbacks.set(StrategyEventKind::Error, record);
        callbacks.set(StrategyEventKind::Stop, record);
        let mut ctx = StrategyRuntimeContext {
            user_data: (&mut state as *mut State).cast(),
            ..Default::default()
        };

        assert!(run_event_runtime(&mut source, &callbacks, &mut ctx).is_err());
        assert_eq!(
            state.calls,
            vec![
                StrategyEventKind::Start as u32,
                StrategyEventKind::Error as u32,
                StrategyEventKind::Stop as u32
            ]
        );
        assert_eq!(state.finalized, 1);
    }

    fn reproducibility_metadata() -> ReproducibilityMetadata {
        let identity = ModelIdentity::new("test", 1);
        ReproducibilityMetadata {
            engine_version: env!("CARGO_PKG_VERSION").into(),
            git_revision: "test".into(),
            strategy_id: "prepared-bar".into(),
            strategy_version: "1".into(),
            runtime_abi_version: STRATEGY_ABI_VERSION,
            phase_contract_version: crate::backtest::scheduler::PHASE_CONTRACT_VERSION,
            data_manifest_hash: 11,
            config_hash: 12,
            matching: ModelIdentity::new("next-open", 1),
            fee: identity.clone(),
            latency: identity.clone(),
            risk: identity.clone(),
            execution_quality: identity,
            random_seed: 7,
        }
    }

    #[test]
    fn prepared_bar_runtime_replays_identically_one_hundred_times() {
        let records: Vec<_> = (0..8)
            .map(|index| TimedBarItem {
                asset_no: 0,
                timeframe_ns: 60,
                bar: Bar {
                    open_ts: index * 60,
                    close_ts: (index + 1) * 60,
                    open: 100.0 + index as f64,
                    high: 101.0 + index as f64,
                    low: 99.0 + index as f64,
                    close: 100.5 + index as f64,
                    volume: 10.0,
                    quote_volume: 1_000.0,
                    buy_volume: 5.0,
                    trade_count: 2,
                    flags: BAR_COMPLETE,
                },
            })
            .collect();
        let source = MaterializedBarSource::new(&records, 4).unwrap();
        let mut callbacks = CallbackRegistry::default();
        callbacks.set(StrategyEventKind::Bar, record);
        let mut state = State::default();
        let context = StrategyRuntimeContext {
            user_data: (&mut state as *mut State).cast(),
            ..Default::default()
        };
        let runtime = PreparedBarRuntime::new(
            source,
            callbacks,
            context,
            BacktestResult::empty(reproducibility_metadata()),
        );
        let mut runner = PreparedRunner::new(runtime);
        let mut expected = None;
        for _ in 0..100 {
            let result = runner.run().unwrap();
            let core = (
                result.metadata.clone(),
                result.start_delivery_ts,
                result.end_delivery_ts,
                result.market_event_count,
                result.callback_count.clone(),
                result.order_count,
                result.fill_count,
                result.execution_reports.clone(),
            );
            if let Some(expected) = &expected {
                assert_eq!(&core, expected);
            } else {
                expected = Some(core);
            }
            runner.reset().unwrap();
        }
        assert_eq!(runner.run_count(), 100);
        assert_eq!(state.history_commits, 0);
        assert_eq!(state.calls.len(), 800);
    }

    #[test]
    fn bar_runtime_advances_to_scheduled_events_after_data_and_reset_restores_them() {
        let records = [TimedBarItem {
            asset_no: 0,
            timeframe_ns: 60,
            bar: Bar {
                open_ts: 0,
                close_ts: 60,
                open: 1.0,
                high: 1.0,
                low: 1.0,
                close: 1.0,
                volume: 1.0,
                quote_volume: 1.0,
                buy_volume: 0.0,
                trade_count: 1,
                flags: BAR_COMPLETE,
            },
        }];
        let mut source = MaterializedBarSource::new(&records, 1).unwrap();
        source.schedule_timer(RuntimeTimer {
            deadline_ts: 100,
            owner_id: 7,
            timer_id: 9,
        });
        source
            .schedule_funding(RuntimeFunding {
                event_id: 1,
                asset_no: 0,
                venue_no: 0,
                instrument_id: 1,
                currency: 0,
                price_source: 0,
                position_snapshot: 0,
                formula: 0,
                rounding_mode: 0,
                boundary: 0,
                publication_ts: 70,
                effective_ts: 75,
                settlement_ts: 80,
                delivery_ts: 90,
                rate: 0.001,
                mark_price: 1.0,
                position_qty: 0.0,
                amount: 0.0,
                rounding_increment: 1e-12,
            })
            .unwrap();
        let mut callbacks = CallbackRegistry::default();
        callbacks.set(StrategyEventKind::Timer, record);
        callbacks.set(StrategyEventKind::Funding, record);
        let mut state = State::default();
        let mut context = StrategyRuntimeContext {
            user_data: (&mut state as *mut State).cast(),
            ..Default::default()
        };
        source.configure_context(&mut context);
        run_event_runtime(&mut source, &callbacks, &mut context).unwrap();
        assert_eq!(
            state.calls,
            [
                StrategyEventKind::Funding as u32,
                StrategyEventKind::Timer as u32
            ]
        );
        assert_eq!(context.now, 100);
        assert_eq!(source.funding_reports().len(), 1);
        source.reset().unwrap();
        assert!(source.funding_reports().is_empty());
        source.configure_context(&mut context);
        run_event_runtime(&mut source, &callbacks, &mut context).unwrap();
        assert_eq!(
            state.calls,
            [
                StrategyEventKind::Funding as u32,
                StrategyEventKind::Timer as u32,
                StrategyEventKind::Funding as u32,
                StrategyEventKind::Timer as u32,
            ]
        );
        assert_eq!(source.funding_reports().len(), 1);
    }

    #[test]
    fn stop_at_data_end_does_not_drain_future_timer_or_funding() {
        let records = [TimedBarItem {
            asset_no: 0,
            timeframe_ns: 60,
            bar: Bar {
                open_ts: 0,
                close_ts: 60,
                open: 1.0,
                high: 1.0,
                low: 1.0,
                close: 1.0,
                volume: 1.0,
                quote_volume: 1.0,
                buy_volume: 0.0,
                trade_count: 1,
                flags: BAR_COMPLETE,
            },
        }];
        let mut source = MaterializedBarSource::new(&records, 1).unwrap();
        source.set_end_policy(crate::backtest::result::EndPolicy::StopAtDataEnd);
        source.schedule_timer(RuntimeTimer {
            deadline_ts: 61,
            owner_id: 1,
            timer_id: 1,
        });
        source
            .schedule_funding(RuntimeFunding {
                event_id: 1,
                asset_no: 0,
                venue_no: 0,
                instrument_id: 1,
                currency: 0,
                price_source: 0,
                position_snapshot: 0,
                formula: 0,
                rounding_mode: 0,
                boundary: 0,
                publication_ts: 50,
                effective_ts: 60,
                settlement_ts: 61,
                delivery_ts: 61,
                rate: 0.001,
                mark_price: 1.0,
                position_qty: 0.0,
                amount: 0.0,
                rounding_increment: 1e-12,
            })
            .unwrap();
        let mut callbacks = CallbackRegistry::default();
        callbacks.set(StrategyEventKind::Bar, record);
        callbacks.set(StrategyEventKind::Funding, record);
        callbacks.set(StrategyEventKind::Timer, record);
        let mut state = State::default();
        let mut context = StrategyRuntimeContext {
            user_data: (&mut state as *mut State).cast(),
            ..Default::default()
        };
        source.configure_context(&mut context);
        run_event_runtime(&mut source, &callbacks, &mut context).unwrap();
        assert_eq!(state.calls, [StrategyEventKind::Bar as u32]);
        assert!(source.funding_reports().is_empty());
    }

    #[test]
    fn same_timestamp_bar_funding_delivery_and_timer_follow_phase_contract() {
        let records = [TimedBarItem {
            asset_no: 0,
            timeframe_ns: 60,
            bar: Bar {
                open_ts: 0,
                close_ts: 60,
                open: 1.0,
                high: 1.0,
                low: 1.0,
                close: 1.0,
                volume: 1.0,
                quote_volume: 1.0,
                buy_volume: 0.0,
                trade_count: 1,
                flags: BAR_COMPLETE,
            },
        }];
        let mut source = MaterializedBarSource::new(&records, 1).unwrap();
        source.schedule_timer(RuntimeTimer {
            deadline_ts: 60,
            owner_id: 1,
            timer_id: 1,
        });
        source
            .schedule_funding(RuntimeFunding {
                event_id: 1,
                asset_no: 0,
                venue_no: 0,
                instrument_id: 1,
                currency: 0,
                price_source: 0,
                position_snapshot: 0,
                formula: 0,
                rounding_mode: 0,
                boundary: 0,
                publication_ts: 50,
                effective_ts: 55,
                settlement_ts: 60,
                delivery_ts: 60,
                rate: 0.001,
                mark_price: 1.0,
                position_qty: 0.0,
                amount: 0.0,
                rounding_increment: 1e-12,
            })
            .unwrap();
        let mut callbacks = CallbackRegistry::default();
        callbacks.set(StrategyEventKind::Bar, record);
        callbacks.set(StrategyEventKind::Funding, record);
        callbacks.set(StrategyEventKind::Timer, record);
        let mut state = State::default();
        let mut context = StrategyRuntimeContext {
            user_data: (&mut state as *mut State).cast(),
            ..Default::default()
        };
        source.configure_context(&mut context);
        run_event_runtime(&mut source, &callbacks, &mut context).unwrap();
        assert_eq!(
            state.calls,
            [
                StrategyEventKind::Bar as u32,
                StrategyEventKind::Funding as u32,
                StrategyEventKind::Timer as u32,
            ]
        );
    }

    #[test]
    fn bar_feed_latency_lives_in_envelope_and_does_not_block_next_market_open() {
        let records: Vec<_> = [10, 20]
            .into_iter()
            .map(|close_ts| TimedBarItem {
                asset_no: 0,
                timeframe_ns: 10,
                bar: Bar {
                    open_ts: close_ts - 10,
                    close_ts,
                    open: close_ts as f64,
                    high: close_ts as f64,
                    low: close_ts as f64,
                    close: close_ts as f64,
                    volume: 1.0,
                    quote_volume: 1.0,
                    buy_volume: 0.0,
                    trade_count: 1,
                    flags: BAR_COMPLETE,
                },
            })
            .collect();
        let mut source = MaterializedBarSource::new(&records, 2).unwrap();
        source.configure_feed_latency(5).unwrap();
        let (kind, now, close_ts) = {
            let event = source.next_event().unwrap().unwrap();
            let RuntimePayload::Bars { close_ts, .. } = event.payload else {
                panic!("expected delayed Bar");
            };
            (event.kind, event.now, close_ts)
        };
        assert_eq!(
            (kind, now, close_ts),
            (StrategyEventKind::Bar as u32, 15, 10)
        );
        // Bar 2 market-open at t=10 was processed before Bar 1 became visible at t=15.
        assert_eq!(source.pending_bar_deliveries.len(), 1);
        let mut context = StrategyRuntimeContext::default();
        source
            .after_callback(StrategyEventKind::Bar as u32, &mut context)
            .unwrap();
        let (now, close_ts) = {
            let event = source.next_event().unwrap().unwrap();
            let RuntimePayload::Bars { close_ts, .. } = event.payload else {
                panic!("expected second delayed Bar");
            };
            (event.now, close_ts)
        };
        assert_eq!((now, close_ts), (25, 20));
        assert_eq!(records[0].bar.close_ts, 10);
    }

    #[test]
    fn prepared_result_separates_exchange_and_delivery_time_and_reports_strategy_stop() {
        let records = three_test_bars();
        let mut source = MaterializedBarSource::new(&records, 1).unwrap();
        source.configure_feed_latency(5).unwrap();
        let mut callbacks = CallbackRegistry::default();
        callbacks.set(StrategyEventKind::Bar, request_stop);
        let mut runtime = PreparedBarRuntime::new(
            source,
            callbacks,
            StrategyRuntimeContext::default(),
            BacktestResult::empty(reproducibility_metadata()),
        );
        let result = crate::backtest::result::ReusableRuntime::run_once(&mut runtime).unwrap();
        assert_eq!(
            result.termination,
            crate::backtest::result::RunTermination::StrategyStop
        );
        assert_eq!((result.start_exchange_ts, result.end_exchange_ts), (0, 10));
        assert_eq!((result.start_delivery_ts, result.end_delivery_ts), (15, 15));
        assert!(result.cpu_time_ns > 0);
    }

    #[test]
    fn empty_runtime_normalizes_timestamp_bounds() {
        let mut state = State::default();
        let mut source = Source {
            events: Vec::new(),
            next: 0,
            state: &mut state,
        };
        let stats = run_event_runtime_counted(
            &mut source,
            &CallbackRegistry::default(),
            &mut StrategyRuntimeContext::default(),
        )
        .unwrap();
        assert_eq!((stats.start_exchange_ts, stats.end_exchange_ts), (0, 0));
        assert_eq!((stats.start_delivery_ts, stats.end_delivery_ts), (0, 0));
    }

    #[test]
    fn delivery_only_events_do_not_change_exchange_time_bounds() {
        let mut state = State::default();
        let mut source = Source {
            events: vec![(StrategyEventKind::Position as u32, 500)],
            next: 0,
            state: &mut state,
        };
        let stats = run_event_runtime_counted(
            &mut source,
            &CallbackRegistry::default(),
            &mut StrategyRuntimeContext::default(),
        )
        .unwrap();
        assert_eq!((stats.start_exchange_ts, stats.end_exchange_ts), (0, 0));
        assert_eq!((stats.start_delivery_ts, stats.end_delivery_ts), (500, 500));
    }

    #[test]
    fn cancel_command_decodes_without_submit_only_fields() {
        let command = OrderCommand {
            kind: ORDER_COMMAND_CANCEL,
            asset_no: 2,
            order_id: 9,
            ..OrderCommand::default()
        };
        let decoded = command
            .decode_execution(100, VenueId(3), InstrumentId(4))
            .unwrap()
            .unwrap();
        let crate::backtest::execution::ExecutionCommand::Cancel(cancel) = decoded else {
            panic!("cancel command must remain a cancel");
        };
        assert_eq!(cancel.client_order_id, 9);
        assert_eq!(cancel.local_submit_ts, 100);
    }

    #[test]
    fn command_bridge_preserves_execution_algorithm_origin() {
        let command = OrderCommand {
            kind: ORDER_COMMAND_SUBMIT,
            side: 1,
            time_in_force: 3,
            order_type: 1,
            _reserved: [0, 0, 1, 0],
            asset_no: 0,
            order_id: 10,
            qty: 1.0,
            ..OrderCommand::default()
        };
        let decoded = command
            .decode_execution(7, VenueId(0), InstrumentId(1))
            .unwrap()
            .unwrap();
        let crate::backtest::execution::ExecutionCommand::Submit(request) = decoded else {
            panic!("expected submit");
        };
        assert_eq!(
            request.origin,
            crate::backtest::execution::OrderOrigin::ExecutionAlgorithm
        );
    }

    #[test]
    fn late_inserted_post_trade_action_precedes_future_transport() {
        let records = [TimedBarItem {
            asset_no: 0,
            timeframe_ns: 10,
            bar: Bar {
                open_ts: 0,
                close_ts: 10,
                open: 1.0,
                high: 1.0,
                low: 1.0,
                close: 1.0,
                volume: 1.0,
                quote_volume: 1.0,
                buy_volume: 0.0,
                trade_count: 1,
                flags: BAR_COMPLETE,
            },
        }];
        let mut source = MaterializedBarSource::new(&records, 1).unwrap();
        let action = |order_id, exchange_arrival_ts| PendingBarAction::Cancel {
            command: OrderCommand {
                kind: ORDER_COMMAND_CANCEL,
                order_id,
                ..OrderCommand::default()
            },
            local_submit_ts: 0,
            exchange_arrival_ts,
        };
        source.schedule_action(EventPhase::CommandArrival, action(1, 200));
        source.schedule_action(EventPhase::PostTradeRisk, action(2, 150));
        let first = source.action_scheduler.pop().unwrap();
        let second = source.action_scheduler.pop().unwrap();
        assert_eq!(first.key.timestamp, 150);
        assert_eq!(first.key.phase, EventPhase::PostTradeRisk);
        assert_eq!(second.key.timestamp, 200);
    }

    #[test]
    fn same_time_post_trade_action_does_not_precede_matching() {
        let mut scheduler = crate::backtest::scheduler::GlobalScheduler::new();
        scheduler.schedule(10, EventPhase::PostTradeRisk, 0, 0, 0, ());
        let key = scheduler.peek_key().unwrap();
        assert!(!action_precedes_matching(key, 10));
        assert!(action_precedes_matching(key, 11));
    }

    struct EmitOnce {
        order_id: u64,
        emitted: bool,
    }

    impl ExecutionAlgorithm for EmitOnce {
        fn on_event(
            &mut self,
            key: crate::backtest::scheduler::EventKey,
            out: &mut Vec<crate::backtest::execution::ExecutionCommand>,
        ) {
            if self.emitted {
                return;
            }
            self.emitted = true;
            out.push(crate::backtest::execution::ExecutionCommand::Submit(
                crate::backtest::execution::ExecutionOrderRequest {
                    client_order_id: self.order_id,
                    venue_id: VenueId(0),
                    instrument_id: InstrumentId(1),
                    price: 0.0,
                    qty: 1.0,
                    side: crate::types::Side::Buy,
                    time_in_force: crate::types::TimeInForce::IOC,
                    order_type: crate::types::OrdType::Market,
                    reduce_only: false,
                    origin: crate::backtest::execution::OrderOrigin::ExecutionAlgorithm,
                    local_submit_ts: key.timestamp,
                },
            ));
        }

        fn reset(&mut self) {
            self.emitted = false;
        }
    }

    struct EmitOnceHook(EmitOnce);

    impl SimulationHook for EmitOnceHook {
        fn on_event(
            &mut self,
            key: crate::backtest::scheduler::EventKey,
            out: &mut Vec<crate::backtest::execution::ExecutionCommand>,
        ) {
            self.0.on_event(key, out);
        }

        fn reset(&mut self) {
            self.0.reset();
        }
    }

    fn three_test_bars() -> Vec<TimedBarItem> {
        (0..3)
            .map(|index| TimedBarItem {
                asset_no: 0,
                timeframe_ns: 10,
                bar: Bar {
                    open_ts: index * 10,
                    close_ts: (index + 1) * 10,
                    open: 100.0,
                    high: 101.0,
                    low: 99.0,
                    close: 100.0,
                    volume: 100.0,
                    quote_volume: 10_000.0,
                    buy_volume: 50.0,
                    trade_count: 10,
                    flags: BAR_COMPLETE,
                },
            })
            .collect()
    }

    #[test]
    fn algorithms_and_hooks_enter_the_real_bar_execution_path() {
        let records = three_test_bars();
        let mut source = MaterializedBarSource::new(&records, 1).unwrap();
        source.add_execution_algorithm(EmitOnce {
            order_id: 41,
            emitted: false,
        });
        source.add_simulation_hook(EmitOnceHook(EmitOnce {
            order_id: 42,
            emitted: false,
        }));
        let callbacks = CallbackRegistry::default();
        let mut context = StrategyRuntimeContext::default();
        source.configure_context(&mut context);
        run_event_runtime(&mut source, &callbacks, &mut context).unwrap();
        for order_id in [41, 42] {
            assert!(source.execution_reports().iter().any(|report| {
                report.order_id == order_id
                    && report.kind == crate::backtest::execution::ExecutionReportKind::Fill
            }));
        }
    }

    #[test]
    fn after_boundary_funding_observes_same_time_bar_fill() {
        let records = three_test_bars();
        let mut source = MaterializedBarSource::new(&records, 1).unwrap();
        source.add_execution_algorithm(EmitOnce {
            order_id: 51,
            emitted: false,
        });
        source
            .schedule_funding(RuntimeFunding {
                event_id: 9,
                asset_no: 0,
                venue_no: 0,
                instrument_id: 1,
                currency: 0,
                price_source: 0,
                position_snapshot: 1,
                formula: 0,
                rounding_mode: 0,
                boundary: 1,
                publication_ts: -2,
                effective_ts: -1,
                settlement_ts: 0,
                delivery_ts: 0,
                rate: 0.001,
                mark_price: 100.0,
                position_qty: 0.0,
                amount: 0.0,
                rounding_increment: 1e-12,
            })
            .unwrap();
        let mut context = StrategyRuntimeContext::default();
        source.configure_context(&mut context);
        run_event_runtime(&mut source, &CallbackRegistry::default(), &mut context).unwrap();
        let report = source.funding_reports()[0];
        assert_eq!(report.position_qty, 1.0);
        assert!((report.amount + 0.1).abs() < 1e-12);
    }

    #[test]
    fn bracket_children_activate_then_oco_cancel_through_shared_execution() {
        let records = three_test_bars();
        let mut source = MaterializedBarSource::new(&records, 1).unwrap();
        assert!(source.register_contingency(ContingencyGroup {
            group_id: 7,
            kind: crate::backtest::platform::ContingencyKind::Bracket,
            venue_id: VenueId(0),
            instrument_id: InstrumentId(1),
            parent: Some(1),
            children: vec![2, 3],
        }));
        let submit = |order_id, side, order_type, price| OrderCommand {
            kind: ORDER_COMMAND_SUBMIT,
            side,
            time_in_force: 0,
            order_type,
            asset_no: 0,
            order_id,
            price,
            qty: 1.0,
            ..OrderCommand::default()
        };
        source.commands[0] = submit(1, 1, 1, 0.0);
        source.commands[1] = submit(2, -1, 0, 100.0);
        source.commands[2] = submit(3, -1, 0, 200.0);
        let mut context = StrategyRuntimeContext {
            now: -1,
            num_commands: 3,
            ..StrategyRuntimeContext::default()
        };
        source.process_commands(&mut context, true).unwrap();
        assert_eq!(source.held_contingent_orders.len(), 2);

        source.configure_context(&mut context);
        run_event_runtime(&mut source, &CallbackRegistry::default(), &mut context).unwrap();
        assert!(source.execution_reports().iter().any(|report| {
            report.order_id == 2
                && report.kind == crate::backtest::execution::ExecutionReportKind::Fill
        }));
        assert!(source.execution_reports().iter().any(|report| {
            report.order_id == 3
                && report.kind == crate::backtest::execution::ExecutionReportKind::Canceled
        }));
    }
}
