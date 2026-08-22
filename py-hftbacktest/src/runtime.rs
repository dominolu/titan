#![allow(clippy::not_unsafe_ptr_arg_deref)]

use std::marker::PhantomData;

use hftbacktest::{
    backtest::Backtest,
    depth::MarketDepth,
    market_data::{BAR_COMPLETE, BAR_EMPTY, BAR_PARTIAL, BarHistory},
    prelude::{Bot, ElapseResult},
    runtime::{
        BarHistoryView, BarItem, CallbackRegistry, FillEvent, MarketState, ORDER_COMMAND_CANCEL,
        ORDER_COMMAND_SUBMIT, OrderCommand, OrderEvent, RuntimeEvent, RuntimeEventSource,
        RuntimePayload, StrategyCallback, StrategyEventKind, StrategyRuntimeContext, TickItem,
        TimedBarItem, run_event_runtime,
    },
    types::{Event, OrdType, Order, Side, Status, TimeInForce},
};

use crate::backtest::{HashMapMarketDepthBacktest, ROIVectorMarketDepthBacktest};
#[cfg(feature = "live")]
use crate::live::{HashMapMarketDepthLiveBot, ROIVectorMarketDepthLiveBot};

struct TickFrameSource<'a, B, MD> {
    hbt: &'a mut B,
    frame_interval: i64,
    max_tick_batch: usize,
    ticks: Vec<TickItem>,
    fills: Vec<FillEvent>,
    order_events: Vec<OrderEvent>,
    order_pending: bool,
    fill_pending: bool,
    position_pending: bool,
    tick_pending: bool,
    commands: Vec<OrderCommand>,
    positions: Vec<f64>,
    markets: Vec<MarketState>,
    ended: bool,
    delivered_end: bool,
    _depth: PhantomData<MD>,
}

trait RuntimeBotEvents {
    fn set_runtime_capture(&mut self, enabled: bool);
    fn runtime_feed_events(&self) -> &[(usize, Event)];
    fn clear_runtime_feed_events(&mut self);
    fn runtime_order_events(&self) -> &[(usize, i64, Order)];
    fn clear_runtime_order_events(&mut self);
}

impl<MD> RuntimeBotEvents for Backtest<MD> {
    fn set_runtime_capture(&mut self, enabled: bool) {
        Backtest::set_runtime_capture(self, enabled);
    }
    fn runtime_feed_events(&self) -> &[(usize, Event)] {
        Backtest::runtime_feed_events(self)
    }

    fn clear_runtime_feed_events(&mut self) {
        Backtest::clear_runtime_feed_events(self);
    }

    fn runtime_order_events(&self) -> &[(usize, i64, Order)] {
        Backtest::runtime_order_events(self)
    }

    fn clear_runtime_order_events(&mut self) {
        Backtest::clear_runtime_order_events(self);
    }
}

#[cfg(feature = "live")]
impl RuntimeBotEvents for HashMapMarketDepthLiveBot {
    fn set_runtime_capture(&mut self, enabled: bool) {
        self.set_runtime_capture(enabled);
    }
    fn runtime_feed_events(&self) -> &[(usize, Event)] {
        self.runtime_feed_events()
    }
    fn clear_runtime_feed_events(&mut self) {
        self.clear_runtime_feed_events();
    }
    fn runtime_order_events(&self) -> &[(usize, i64, Order)] {
        self.runtime_order_events()
    }
    fn clear_runtime_order_events(&mut self) {
        self.clear_runtime_order_events();
    }
}

#[cfg(feature = "live")]
impl RuntimeBotEvents for ROIVectorMarketDepthLiveBot {
    fn set_runtime_capture(&mut self, enabled: bool) {
        self.set_runtime_capture(enabled);
    }
    fn runtime_feed_events(&self) -> &[(usize, Event)] {
        self.runtime_feed_events()
    }
    fn clear_runtime_feed_events(&mut self) {
        self.clear_runtime_feed_events();
    }
    fn runtime_order_events(&self) -> &[(usize, i64, Order)] {
        self.runtime_order_events()
    }
    fn clear_runtime_order_events(&mut self) {
        self.clear_runtime_order_events();
    }
}

#[derive(Debug, thiserror::Error)]
enum TickRuntimeError<E: std::error::Error + Send + Sync + 'static> {
    #[error(transparent)]
    Bot(E),
    #[error("invalid order command")]
    InvalidOrder,
    #[error("global TickBatch length {len} exceeds configured maximum {max}")]
    TickBatchOverflow { len: usize, max: usize },
}

#[derive(Debug, thiserror::Error)]
enum BarFeedError {
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
    #[error("invalid bar order command")]
    InvalidOrder,
    #[error("duplicate active bar order id {order_id} for asset {asset_no}")]
    DuplicateOrder { asset_no: u64, order_id: u64 },
    #[error("bar order command buffer overflow")]
    CommandOverflow,
}

struct HistorySlot {
    asset_no: u64,
    timeframe_ns: i64,
    history: BarHistory,
}

#[derive(Clone, Copy)]
struct PendingBarOrder {
    command: OrderCommand,
    eligible_after: i64,
}

struct MaterializedBarSource {
    records: Vec<TimedBarItem>,
    cursor: usize,
    batch: Vec<BarItem>,
    histories: Vec<HistorySlot>,
    views: Vec<BarHistoryView>,
    commands: Vec<OrderCommand>,
    orders: Vec<PendingBarOrder>,
    fills: Vec<FillEvent>,
    positions: Vec<f64>,
    bar_pending: bool,
    execution_timeframe_ns: i64,
    execution_assets: Vec<bool>,
}

impl MaterializedBarSource {
    fn new(records: &[TimedBarItem], history_capacity: usize) -> Result<Self, BarFeedError> {
        let records = records.to_vec();
        for (index, record) in records.iter().enumerate() {
            if record.timeframe_ns <= 0 {
                return Err(BarFeedError::InvalidTimeframe);
            }
            if record.bar.close_ts - record.bar.open_ts != record.timeframe_ns {
                return Err(BarFeedError::IntervalMismatch);
            }
            if record.bar.flags & BAR_PARTIAL != 0 {
                return Err(BarFeedError::PartialBar);
            }
            if record.bar.flags & BAR_COMPLETE == 0 {
                return Err(BarFeedError::IncompleteBar);
            }
            let prices_valid = record.bar.open.is_finite()
                && record.bar.high.is_finite()
                && record.bar.low.is_finite()
                && record.bar.close.is_finite()
                && record.bar.high >= record.bar.open.max(record.bar.close)
                && record.bar.low <= record.bar.open.min(record.bar.close)
                && record.bar.low <= record.bar.high;
            let nan_empty = record.bar.flags & BAR_EMPTY != 0
                && record.bar.open.is_nan()
                && record.bar.high.is_nan()
                && record.bar.low.is_nan()
                && record.bar.close.is_nan();
            if (!prices_valid && !nan_empty)
                || !record.bar.volume.is_finite()
                || record.bar.volume < 0.0
            {
                return Err(BarFeedError::InvalidOhlcv);
            }
            if index > 0 {
                let previous = records[index - 1];
                if (record.bar.close_ts, record.timeframe_ns, record.asset_no)
                    < (
                        previous.bar.close_ts,
                        previous.timeframe_ns,
                        previous.asset_no,
                    )
                {
                    return Err(BarFeedError::Unsorted);
                }
            }
        }

        let mut keys: Vec<(u64, i64)> = records
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
        let num_assets = records
            .iter()
            .map(|record| record.asset_no as usize + 1)
            .max()
            .unwrap_or(0);
        let execution_timeframe_ns = records
            .iter()
            .map(|record| record.timeframe_ns)
            .min()
            .unwrap_or(0);
        let mut execution_assets = vec![false; num_assets];
        for record in &records {
            if record.timeframe_ns == execution_timeframe_ns {
                execution_assets[record.asset_no as usize] = true;
            }
        }
        Ok(Self {
            records,
            cursor: 0,
            batch: Vec::new(),
            histories,
            views,
            commands: vec![OrderCommand::default(); 1024],
            orders: Vec::new(),
            fills: Vec::new(),
            positions: vec![0.0; num_assets],
            bar_pending: false,
            execution_timeframe_ns,
            execution_assets,
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

    fn configure_context(&mut self, ctx: &mut StrategyRuntimeContext) {
        ctx.histories_ptr = self.views.as_ptr();
        ctx.num_histories = self.views.len();
        ctx.commands_ptr = self.commands.as_mut_ptr();
        ctx.command_capacity = self.commands.len();
        ctx.num_commands = 0;
        ctx.positions_ptr = self.positions.as_ptr();
        ctx.num_positions = self.positions.len();
    }

    fn process_commands(
        &mut self,
        ctx: &mut StrategyRuntimeContext,
        allow_submit: bool,
    ) -> Result<(), BarFeedError> {
        if ctx.num_commands > self.commands.len() {
            return Err(BarFeedError::CommandOverflow);
        }
        for command in self.commands[..ctx.num_commands].iter().copied() {
            match command.kind {
                ORDER_COMMAND_SUBMIT => {
                    if !allow_submit {
                        return Err(BarFeedError::InvalidOrder);
                    }
                    if command.asset_no as usize >= self.positions.len()
                        || !self.execution_assets[command.asset_no as usize]
                        || !matches!(command.side, -1 | 1)
                        || !command.price.is_finite()
                        || command.qty <= 0.0
                        || !command.qty.is_finite()
                        || command.time_in_force > 3
                        || command.order_type > 1
                    {
                        return Err(BarFeedError::InvalidOrder);
                    }
                    if self.orders.iter().any(|order| {
                        order.command.asset_no == command.asset_no
                            && order.command.order_id == command.order_id
                    }) {
                        return Err(BarFeedError::DuplicateOrder {
                            asset_no: command.asset_no,
                            order_id: command.order_id,
                        });
                    }
                    self.orders.push(PendingBarOrder {
                        command,
                        eligible_after: ctx.now,
                    });
                }
                ORDER_COMMAND_CANCEL => {
                    if let Some(index) = self.orders.iter().position(|order| {
                        order.command.asset_no == command.asset_no
                            && order.command.order_id == command.order_id
                    }) {
                        self.orders.swap_remove(index);
                    }
                }
                0 => {}
                _ => return Err(BarFeedError::InvalidOrder),
            }
        }
        ctx.num_commands = 0;
        Ok(())
    }

    /// Conservative NextOpen execution. An order becomes eligible only after the callback
    /// that created it has returned, and is never tested against that callback's bar.
    fn match_at_next_open(&mut self, open_ts: i64, timeframe_ns: i64) {
        self.fills.clear();
        if timeframe_ns != self.execution_timeframe_ns {
            return;
        }
        let mut index = 0;
        while index < self.orders.len() {
            let pending = self.orders[index];
            let order = pending.command;
            let Some(item) = self
                .batch
                .iter()
                .find(|item| item.asset_no == order.asset_no)
            else {
                index += 1;
                continue;
            };
            if item.bar.flags & BAR_EMPTY != 0 || item.bar.open_ts < pending.eligible_after {
                index += 1;
                continue;
            }
            let open = item.bar.open;
            let executable = order.order_type == 1
                || (order.side == 1 && open <= order.price)
                || (order.side == -1 && open >= order.price);
            if executable {
                let asset_no = order.asset_no as usize;
                self.positions[asset_no] += f64::from(order.side) * order.qty;
                self.fills.push(FillEvent {
                    asset_no: order.asset_no,
                    order_id: order.order_id,
                    exch_ts: open_ts,
                    local_ts: open_ts,
                    price: open,
                    qty: order.qty,
                    side: order.side,
                    maker: 0,
                    _reserved: [0; 6],
                });
                self.orders.swap_remove(index);
            } else if matches!(order.time_in_force, 2 | 3) {
                // FOK/IOC are evaluated once at the first eligible open.
                self.orders.swap_remove(index);
            } else {
                index += 1;
            }
        }
    }
}

impl RuntimeEventSource for MaterializedBarSource {
    type Error = BarFeedError;

    fn next_event(&mut self) -> Result<Option<RuntimeEvent<'_>>, Self::Error> {
        if self.bar_pending {
            self.bar_pending = false;
            let first = self.batch[0];
            return Ok(Some(RuntimeEvent {
                kind: StrategyEventKind::Bar as u32,
                now: first.bar.close_ts,
                payload: RuntimePayload::Bars {
                    timeframe_ns: first.bar.close_ts - first.bar.open_ts,
                    close_ts: first.bar.close_ts,
                    bars: &self.batch,
                },
            }));
        }
        let Some(first) = self.records.get(self.cursor).copied() else {
            return Ok(None);
        };
        let close_ts = first.bar.close_ts;
        let timeframe_ns = first.timeframe_ns;
        self.batch.clear();
        while let Some(record) = self.records.get(self.cursor).copied() {
            if record.bar.close_ts != close_ts || record.timeframe_ns != timeframe_ns {
                break;
            }
            if self
                .batch
                .last()
                .is_some_and(|item| item.asset_no == record.asset_no)
            {
                return Err(BarFeedError::DuplicateAsset);
            }
            self.batch.push(BarItem {
                asset_no: record.asset_no,
                bar: record.bar,
            });
            self.cursor += 1;
        }
        self.match_at_next_open(first.bar.open_ts, timeframe_ns);
        if self.fills.is_empty() {
            Ok(Some(RuntimeEvent {
                kind: StrategyEventKind::Bar as u32,
                now: close_ts,
                payload: RuntimePayload::Bars {
                    timeframe_ns,
                    close_ts,
                    bars: &self.batch,
                },
            }))
        } else {
            self.bar_pending = true;
            Ok(Some(RuntimeEvent {
                kind: StrategyEventKind::Filled as u32,
                now: first.bar.open_ts,
                payload: RuntimePayload::Fills(&self.fills),
            }))
        }
    }

    fn after_callback(
        &mut self,
        kind: u32,
        _ctx: &mut StrategyRuntimeContext,
    ) -> Result<(), Self::Error> {
        if kind == StrategyEventKind::Bar as u32 {
            for item in &self.batch {
                if let Some(slot) = self.histories.iter_mut().find(|slot| {
                    slot.asset_no == item.asset_no
                        && slot.timeframe_ns == item.bar.close_ts - item.bar.open_ts
                }) {
                    slot.history.push(item.bar);
                }
            }
            self.refresh_views();
        }
        self.process_commands(
            _ctx,
            kind != StrategyEventKind::Error as u32 && kind != StrategyEventKind::Stop as u32,
        )
    }
}

impl<'a, B, MD> TickFrameSource<'a, B, MD>
where
    MD: MarketDepth,
    B: Bot<MD> + RuntimeBotEvents,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    fn new(hbt: &'a mut B, frame_interval: i64, max_tick_batch: usize) -> Self {
        let num_assets = hbt.num_assets();
        hbt.set_runtime_capture(true);
        hbt.clear_runtime_feed_events();
        hbt.clear_runtime_order_events();
        Self {
            hbt,
            frame_interval,
            max_tick_batch,
            ticks: Vec::with_capacity(max_tick_batch.min(4096)),
            fills: Vec::new(),
            order_events: Vec::new(),
            order_pending: false,
            fill_pending: false,
            position_pending: false,
            tick_pending: false,
            commands: vec![OrderCommand::default(); 1024],
            positions: vec![0.0; num_assets],
            markets: vec![MarketState::default(); num_assets],
            ended: false,
            delivered_end: false,
            _depth: PhantomData,
        }
    }

    fn configure_context(&mut self, ctx: &mut StrategyRuntimeContext) {
        self.refresh_markets();
        ctx.commands_ptr = self.commands.as_mut_ptr();
        ctx.command_capacity = self.commands.len();
        ctx.num_commands = 0;
        ctx.positions_ptr = self.positions.as_ptr();
        ctx.num_positions = self.positions.len();
        ctx.markets_ptr = self.markets.as_ptr();
        ctx.num_markets = self.markets.len();
    }

    fn refresh_positions(&mut self) -> bool {
        let mut changed = false;
        for (asset_no, position) in self.positions.iter_mut().enumerate() {
            let next = self.hbt.position(asset_no);
            changed |= next != *position;
            *position = next;
        }
        changed
    }

    fn refresh_markets(&mut self) {
        for (asset_no, market) in self.markets.iter_mut().enumerate() {
            let depth = self.hbt.depth(asset_no);
            *market = MarketState {
                best_bid: depth.best_bid(),
                best_ask: depth.best_ask(),
                best_bid_qty: depth.best_bid_qty(),
                best_ask_qty: depth.best_ask_qty(),
                tick_size: depth.tick_size(),
                lot_size: depth.lot_size(),
            };
        }
    }

    fn process_commands(
        &mut self,
        ctx: &mut StrategyRuntimeContext,
        allow_submit: bool,
    ) -> Result<(), TickRuntimeError<B::Error>> {
        let count = ctx.num_commands.min(self.commands.len());
        for command in self.commands[..count].iter().copied() {
            match command.kind {
                ORDER_COMMAND_SUBMIT => {
                    if !allow_submit {
                        return Err(TickRuntimeError::InvalidOrder);
                    }
                    let side = match command.side {
                        1 => Side::Buy,
                        -1 => Side::Sell,
                        _ => return Err(TickRuntimeError::InvalidOrder),
                    };
                    let tif = match command.time_in_force {
                        0 => TimeInForce::GTC,
                        1 => TimeInForce::GTX,
                        2 => TimeInForce::FOK,
                        3 => TimeInForce::IOC,
                        _ => return Err(TickRuntimeError::InvalidOrder),
                    };
                    let order_type = match command.order_type {
                        0 => OrdType::Limit,
                        1 => OrdType::Market,
                        _ => return Err(TickRuntimeError::InvalidOrder),
                    };
                    match side {
                        Side::Buy => {
                            self.hbt
                                .submit_buy_order(
                                    command.asset_no as usize,
                                    command.order_id,
                                    command.price,
                                    command.qty,
                                    tif,
                                    order_type,
                                    false,
                                )
                                .map_err(TickRuntimeError::Bot)?;
                        }
                        Side::Sell => {
                            self.hbt
                                .submit_sell_order(
                                    command.asset_no as usize,
                                    command.order_id,
                                    command.price,
                                    command.qty,
                                    tif,
                                    order_type,
                                    false,
                                )
                                .map_err(TickRuntimeError::Bot)?;
                        }
                        _ => unreachable!(),
                    }
                }
                ORDER_COMMAND_CANCEL => {
                    self.hbt
                        .cancel(command.asset_no as usize, command.order_id, false)
                        .map_err(TickRuntimeError::Bot)?;
                }
                0 => {}
                _ => return Err(TickRuntimeError::InvalidOrder),
            }
        }
        ctx.num_commands = 0;
        Ok(())
    }
}

impl<B, MD> RuntimeEventSource for TickFrameSource<'_, B, MD>
where
    MD: MarketDepth,
    B: Bot<MD> + RuntimeBotEvents,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    type Error = TickRuntimeError<B::Error>;

    fn next_event(&mut self) -> Result<Option<RuntimeEvent<'_>>, Self::Error> {
        if self.delivered_end {
            return Ok(None);
        }

        if self.order_pending {
            self.order_pending = false;
            let now = self
                .order_events
                .iter()
                .map(|order| order.local_ts)
                .max()
                .unwrap_or_else(|| self.hbt.current_timestamp());
            return Ok(Some(RuntimeEvent {
                kind: StrategyEventKind::Order as u32,
                now,
                payload: RuntimePayload::Orders(&self.order_events),
            }));
        }
        if self.fill_pending {
            self.fill_pending = false;
            let now = self
                .fills
                .iter()
                .map(|fill| fill.local_ts)
                .max()
                .unwrap_or_else(|| self.hbt.current_timestamp());
            return Ok(Some(RuntimeEvent {
                kind: StrategyEventKind::Filled as u32,
                now,
                payload: RuntimePayload::Fills(&self.fills),
            }));
        }
        if self.position_pending {
            self.position_pending = false;
            return Ok(Some(RuntimeEvent {
                kind: StrategyEventKind::Position as u32,
                now: self.hbt.current_timestamp(),
                payload: RuntimePayload::None,
            }));
        }
        if self.tick_pending {
            self.tick_pending = false;
            return Ok(Some(RuntimeEvent {
                kind: StrategyEventKind::Tick as u32,
                now: self.hbt.current_timestamp(),
                payload: RuntimePayload::Ticks(&self.ticks),
            }));
        }

        let result = self
            .hbt
            .wait_next_feed(true, self.frame_interval)
            .map_err(TickRuntimeError::Bot)?;
        self.ended = result == ElapseResult::EndOfData;
        self.ticks.clear();
        for (asset_no, event) in self.hbt.runtime_feed_events() {
            if self.ticks.len() == self.max_tick_batch {
                return Err(TickRuntimeError::TickBatchOverflow {
                    len: self.ticks.len() + 1,
                    max: self.max_tick_batch,
                });
            }
            self.ticks.push(TickItem {
                asset_no: *asset_no as u64,
                event: event.clone(),
            });
        }
        self.hbt.clear_runtime_feed_events();
        self.fills.clear();
        self.order_events.clear();
        for (asset_no, recv_ts, order) in self.hbt.runtime_order_events() {
            self.order_events.push(OrderEvent {
                asset_no: *asset_no as u64,
                order_id: order.order_id,
                exch_ts: order.exch_timestamp,
                local_ts: *recv_ts,
                price: order.price(),
                qty: order.qty,
                exec_price: order.exec_price(),
                exec_qty: order.exec_qty,
                side: order.side as i8,
                status: order.status as u8,
                request: order.req as u8,
                maker: u8::from(order.maker),
                _reserved: [0; 4],
            });
            if order.exec_qty > 0.0
                && matches!(order.status, Status::PartiallyFilled | Status::Filled)
            {
                self.fills.push(FillEvent {
                    asset_no: *asset_no as u64,
                    order_id: order.order_id,
                    exch_ts: order.exch_timestamp,
                    local_ts: *recv_ts,
                    price: order.exec_price(),
                    qty: order.exec_qty,
                    side: order.side as i8,
                    maker: u8::from(order.maker),
                    _reserved: [0; 6],
                });
            }
        }
        self.hbt.clear_runtime_order_events();
        self.position_pending = self.refresh_positions();
        self.refresh_markets();
        self.order_pending = !self.order_events.is_empty();
        self.fill_pending = !self.fills.is_empty();
        // A pure order-response boundary should not synthesize an empty market-data callback.
        // Preserve the periodic empty callback only when the max-wait interval elapsed.
        self.tick_pending = !self.ticks.is_empty() || result == ElapseResult::Ok;
        self.next_event()
    }

    fn after_callback(
        &mut self,
        kind: u32,
        ctx: &mut StrategyRuntimeContext,
    ) -> Result<(), Self::Error> {
        self.process_commands(
            ctx,
            kind != StrategyEventKind::Error as u32 && kind != StrategyEventKind::Stop as u32,
        )?;
        if kind == StrategyEventKind::Tick as u32 {
            self.hbt.clear_last_trades(None);
            if self.ended {
                self.delivered_end = true;
            }
        }
        if kind == StrategyEventKind::Stop as u32 {
            self.hbt.set_runtime_capture(false);
        }
        Ok(())
    }
}

unsafe fn callback(addr: usize) -> Option<StrategyCallback> {
    if addr == 0 {
        None
    } else {
        // Safety: addresses are supplied by Numba `cfunc` objects with the exact callback ABI.
        Some(unsafe { std::mem::transmute::<usize, StrategyCallback>(addr) })
    }
}

unsafe fn callback_registry(addresses: *const usize, len: usize) -> CallbackRegistry {
    let mut registry = CallbackRegistry::default();
    let addresses = if addresses.is_null() {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(addresses, len.min(32)) }
    };
    for (event_id, &addr) in addresses.iter().enumerate() {
        if let Some(callback) = unsafe { callback(addr) } {
            // The fixed table is already range-checked by the slice bound.
            registry.set_custom(event_id as u32, callback).unwrap();
        }
    }
    registry
}

unsafe fn run_tick_runtime<B, MD>(
    hbt: &mut B,
    ctx: &mut StrategyRuntimeContext,
    callback_addresses: *const usize,
    callback_count: usize,
    frame_interval: i64,
    max_tick_batch: usize,
) -> i64
where
    MD: MarketDepth,
    B: Bot<MD> + RuntimeBotEvents,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    if frame_interval <= 0 || max_tick_batch == 0 {
        return -2;
    }
    ctx.bot_ptr = (hbt as *mut B).cast();
    let callbacks = unsafe { callback_registry(callback_addresses, callback_count) };
    let mut source = TickFrameSource::new(hbt, frame_interval, max_tick_batch);
    source.configure_context(ctx);
    match run_event_runtime(&mut source, &callbacks, ctx) {
        Ok(()) => 0,
        Err(error) => {
            eprintln!("strategy runtime failed: {error}");
            -1
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn hashmapbt_run_tick_runtime(
    hbt_ptr: *mut HashMapMarketDepthBacktest,
    ctx_ptr: *mut StrategyRuntimeContext,
    callbacks: *const usize,
    callback_count: usize,
    frame_interval: i64,
    max_tick_batch: usize,
) -> i64 {
    if hbt_ptr.is_null() || ctx_ptr.is_null() {
        return -3;
    }
    unsafe {
        run_tick_runtime(
            &mut *hbt_ptr,
            &mut *ctx_ptr,
            callbacks,
            callback_count,
            frame_interval,
            max_tick_batch,
        )
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn roivecbt_run_tick_runtime(
    hbt_ptr: *mut ROIVectorMarketDepthBacktest,
    ctx_ptr: *mut StrategyRuntimeContext,
    callbacks: *const usize,
    callback_count: usize,
    frame_interval: i64,
    max_tick_batch: usize,
) -> i64 {
    if hbt_ptr.is_null() || ctx_ptr.is_null() {
        return -3;
    }
    unsafe {
        run_tick_runtime(
            &mut *hbt_ptr,
            &mut *ctx_ptr,
            callbacks,
            callback_count,
            frame_interval,
            max_tick_batch,
        )
    }
}

#[cfg(feature = "live")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hashmaplive_run_tick_runtime(
    hbt_ptr: *mut HashMapMarketDepthLiveBot,
    ctx_ptr: *mut StrategyRuntimeContext,
    callbacks: *const usize,
    callback_count: usize,
    frame_interval: i64,
    max_tick_batch: usize,
) -> i64 {
    if hbt_ptr.is_null() || ctx_ptr.is_null() {
        return -3;
    }
    unsafe {
        run_tick_runtime(
            &mut *hbt_ptr,
            &mut *ctx_ptr,
            callbacks,
            callback_count,
            frame_interval,
            max_tick_batch,
        )
    }
}

#[cfg(feature = "live")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn roiveclive_run_tick_runtime(
    hbt_ptr: *mut ROIVectorMarketDepthLiveBot,
    ctx_ptr: *mut StrategyRuntimeContext,
    callbacks: *const usize,
    callback_count: usize,
    frame_interval: i64,
    max_tick_batch: usize,
) -> i64 {
    if hbt_ptr.is_null() || ctx_ptr.is_null() {
        return -3;
    }
    unsafe {
        run_tick_runtime(
            &mut *hbt_ptr,
            &mut *ctx_ptr,
            callbacks,
            callback_count,
            frame_interval,
            max_tick_batch,
        )
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn run_materialized_bar_runtime(
    records_ptr: *const TimedBarItem,
    record_count: usize,
    ctx_ptr: *mut StrategyRuntimeContext,
    callbacks: *const usize,
    callback_count: usize,
    history_capacity: usize,
) -> i64 {
    if records_ptr.is_null() || ctx_ptr.is_null() {
        return -3;
    }
    let records = unsafe { std::slice::from_raw_parts(records_ptr, record_count) };
    let mut source = match MaterializedBarSource::new(records, history_capacity) {
        Ok(source) => source,
        Err(error) => {
            eprintln!("invalid materialized bar input: {error}");
            return -4;
        }
    };
    let ctx = unsafe { &mut *ctx_ptr };
    source.configure_context(ctx);
    let callbacks = unsafe { callback_registry(callbacks, callback_count) };
    match run_event_runtime(&mut source, &callbacks, ctx) {
        Ok(()) => 0,
        Err(error) => {
            eprintln!("bar strategy runtime failed: {error}");
            -1
        }
    }
}

/// Exports sizes and offsets used by the Numba dtype mirror.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn strategy_runtime_layout(sizes: *mut usize, ctx_offsets: *mut usize) {
    use hftbacktest::{
        market_data::Bar,
        runtime::{BarItem, FillEvent, MarketState, OrderCommand, OrderEvent},
    };

    let sizes_out = unsafe { std::slice::from_raw_parts_mut(sizes, 10) };
    sizes_out.copy_from_slice(&[
        size_of::<StrategyRuntimeContext>(),
        size_of::<TickItem>(),
        size_of::<Bar>(),
        size_of::<BarItem>(),
        size_of::<FillEvent>(),
        size_of::<TimedBarItem>(),
        size_of::<BarHistoryView>(),
        size_of::<OrderCommand>(),
        size_of::<OrderEvent>(),
        size_of::<MarketState>(),
    ]);

    let offsets = unsafe { std::slice::from_raw_parts_mut(ctx_offsets, 40) };
    offsets.copy_from_slice(&[
        std::mem::offset_of!(StrategyRuntimeContext, abi_version),
        std::mem::offset_of!(StrategyRuntimeContext, struct_size),
        std::mem::offset_of!(StrategyRuntimeContext, event_kind),
        std::mem::offset_of!(StrategyRuntimeContext, stop_requested),
        std::mem::offset_of!(StrategyRuntimeContext, now),
        std::mem::offset_of!(StrategyRuntimeContext, generation),
        std::mem::offset_of!(StrategyRuntimeContext, user_data),
        std::mem::offset_of!(StrategyRuntimeContext, bot_ptr),
        std::mem::offset_of!(StrategyRuntimeContext, ticks_ptr),
        std::mem::offset_of!(StrategyRuntimeContext, num_ticks),
        std::mem::offset_of!(StrategyRuntimeContext, bars_ptr),
        std::mem::offset_of!(StrategyRuntimeContext, num_bars),
        std::mem::offset_of!(StrategyRuntimeContext, bar_timeframe_ns),
        std::mem::offset_of!(StrategyRuntimeContext, bar_close_ts),
        std::mem::offset_of!(StrategyRuntimeContext, fills_ptr),
        std::mem::offset_of!(StrategyRuntimeContext, num_fills),
        std::mem::offset_of!(StrategyRuntimeContext, orders_ptr),
        std::mem::offset_of!(StrategyRuntimeContext, num_orders),
        std::mem::offset_of!(StrategyRuntimeContext, histories_ptr),
        std::mem::offset_of!(StrategyRuntimeContext, num_histories),
        std::mem::offset_of!(StrategyRuntimeContext, payload_ptr),
        std::mem::offset_of!(StrategyRuntimeContext, payload_len),
        std::mem::offset_of!(StrategyRuntimeContext, state_f64_ptr),
        std::mem::offset_of!(StrategyRuntimeContext, state_f64_len),
        std::mem::offset_of!(StrategyRuntimeContext, state_i64_ptr),
        std::mem::offset_of!(StrategyRuntimeContext, state_i64_len),
        std::mem::offset_of!(StrategyRuntimeContext, commands_ptr),
        std::mem::offset_of!(StrategyRuntimeContext, num_commands),
        std::mem::offset_of!(StrategyRuntimeContext, command_capacity),
        std::mem::offset_of!(StrategyRuntimeContext, positions_ptr),
        std::mem::offset_of!(StrategyRuntimeContext, num_positions),
        std::mem::offset_of!(StrategyRuntimeContext, markets_ptr),
        std::mem::offset_of!(StrategyRuntimeContext, num_markets),
        std::mem::offset_of!(StrategyRuntimeContext, last_error),
        0,
        0,
        0,
        0,
        0,
        0,
    ]);
}
