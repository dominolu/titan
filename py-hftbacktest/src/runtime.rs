#![allow(clippy::not_unsafe_ptr_arg_deref)]

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    marker::PhantomData,
};

use hftbacktest::{
    backtest::{
        Backtest,
        bar::{BarFeed, MaterializedBarFeed, OhlcFillAssumption},
    },
    depth::MarketDepth,
    market_data::BarHistory,
    prelude::{Bot, ElapseResult},
    runtime::{
        BarHistoryView, CallbackRegistry, FillEvent, MarketState, MaterializedBarSource,
        ORDER_COMMAND_CANCEL, ORDER_COMMAND_SUBMIT, OrderCommand, OrderEvent, RuntimeEvent,
        RuntimeEventSource, RuntimeFunding, RuntimePayload, RuntimeTimer, StrategyCallback,
        StrategyEventKind, StrategyRuntimeContext, TickItem, TimedBarItem, project_order_response,
        run_event_runtime,
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
    report_projected_positions: Vec<bool>,
    markets: Vec<MarketState>,
    ended: bool,
    delivered_end: bool,
    clock_started: bool,
    conditional_orders: BTreeMap<(u64, u64), OrderCommand>,
    active_gtd: BTreeMap<(u64, u64), (i64, OrderCommand)>,
    synthetic_order_events: VecDeque<OrderEvent>,
    suppressed_terminal: BTreeSet<(u64, u64)>,
    conditional_scratch: Vec<((u64, u64), OrderCommand)>,
    timers: hftbacktest::backtest::scheduler::TimerQueue,
    timer_scratch: Vec<hftbacktest::backtest::scheduler::TimerEvent>,
    current_timer: RuntimeTimer,
    funding: VecDeque<hftbacktest::backtest::execution::ScheduledFunding>,
    pending_funding: VecDeque<(u32, hftbacktest::backtest::execution::FundingReport)>,
    external_funding: VecDeque<RuntimeFunding>,
    funding_engines: Vec<hftbacktest::backtest::execution::FundingEngine>,
    next_funding_sequence: u64,
    current_funding: RuntimeFunding,
    _depth: PhantomData<MD>,
}

trait RuntimeBotEvents {
    type RuntimeError;
    fn register_runtime_order_extensions(
        &mut self,
        _asset_no: usize,
        _order_id: u64,
        _reduce_only: bool,
    ) {
    }
    fn set_runtime_capture(&mut self, enabled: bool);
    fn runtime_feed_events(&self) -> &[(usize, Event)];
    fn clear_runtime_feed_events(&mut self);
    fn runtime_order_events(&self) -> &[(usize, i64, Order)];
    fn clear_runtime_order_events(&mut self);
    fn runtime_funding_events(&self) -> &[(usize, RuntimeFunding)];
    fn clear_runtime_funding_events(&mut self);
    fn settle_runtime_funding(
        &mut self,
        scheduled: hftbacktest::backtest::execution::ScheduledFunding,
        engine: &mut hftbacktest::backtest::execution::FundingEngine,
        sequence: u64,
    ) -> Result<Option<hftbacktest::backtest::execution::FundingReport>, Self::RuntimeError>;
    fn deliver_runtime_funding(
        &mut self,
        report: hftbacktest::backtest::execution::FundingReport,
    ) -> Result<(), Self::RuntimeError>;
}

impl<MD> RuntimeBotEvents for Backtest<MD>
where
    MD: MarketDepth,
{
    type RuntimeError = hftbacktest::backtest::BacktestError;
    fn register_runtime_order_extensions(
        &mut self,
        asset_no: usize,
        order_id: u64,
        reduce_only: bool,
    ) {
        Backtest::register_runtime_order_extensions(self, asset_no, order_id, reduce_only);
    }
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
    fn runtime_funding_events(&self) -> &[(usize, RuntimeFunding)] {
        &[]
    }
    fn clear_runtime_funding_events(&mut self) {}
    fn settle_runtime_funding(
        &mut self,
        scheduled: hftbacktest::backtest::execution::ScheduledFunding,
        engine: &mut hftbacktest::backtest::execution::FundingEngine,
        sequence: u64,
    ) -> Result<Option<hftbacktest::backtest::execution::FundingReport>, Self::RuntimeError> {
        Backtest::settle_runtime_funding(self, scheduled, engine, sequence).map(Some)
    }
    fn deliver_runtime_funding(
        &mut self,
        report: hftbacktest::backtest::execution::FundingReport,
    ) -> Result<(), Self::RuntimeError> {
        Backtest::deliver_runtime_funding(self, report)
    }
}

#[cfg(feature = "live")]
impl RuntimeBotEvents for HashMapMarketDepthLiveBot {
    type RuntimeError = hftbacktest::live::BotError;
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
    fn runtime_funding_events(&self) -> &[(usize, RuntimeFunding)] {
        self.runtime_funding_events()
    }
    fn clear_runtime_funding_events(&mut self) {
        self.clear_runtime_funding_events();
    }
    fn settle_runtime_funding(
        &mut self,
        _scheduled: hftbacktest::backtest::execution::ScheduledFunding,
        _engine: &mut hftbacktest::backtest::execution::FundingEngine,
        _sequence: u64,
    ) -> Result<Option<hftbacktest::backtest::execution::FundingReport>, Self::RuntimeError> {
        Ok(None)
    }
    fn deliver_runtime_funding(
        &mut self,
        _report: hftbacktest::backtest::execution::FundingReport,
    ) -> Result<(), Self::RuntimeError> {
        Ok(())
    }
}

#[cfg(feature = "live")]
impl RuntimeBotEvents for ROIVectorMarketDepthLiveBot {
    type RuntimeError = hftbacktest::live::BotError;
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
    fn runtime_funding_events(&self) -> &[(usize, RuntimeFunding)] {
        self.runtime_funding_events()
    }
    fn clear_runtime_funding_events(&mut self) {
        self.clear_runtime_funding_events();
    }
    fn settle_runtime_funding(
        &mut self,
        _scheduled: hftbacktest::backtest::execution::ScheduledFunding,
        _engine: &mut hftbacktest::backtest::execution::FundingEngine,
        _sequence: u64,
    ) -> Result<Option<hftbacktest::backtest::execution::FundingReport>, Self::RuntimeError> {
        Ok(None)
    }
    fn deliver_runtime_funding(
        &mut self,
        _report: hftbacktest::backtest::execution::FundingReport,
    ) -> Result<(), Self::RuntimeError> {
        Ok(())
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

impl<'a, B, MD> TickFrameSource<'a, B, MD>
where
    MD: MarketDepth,
    B: Bot<MD> + RuntimeBotEvents<RuntimeError = B::Error>,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    fn new(hbt: &'a mut B, frame_interval: i64, max_tick_batch: usize) -> Self {
        let num_assets = hbt.num_assets();
        hbt.set_runtime_capture(true);
        hbt.clear_runtime_feed_events();
        hbt.clear_runtime_order_events();
        hbt.clear_runtime_funding_events();
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
            report_projected_positions: vec![false; num_assets],
            markets: vec![MarketState::default(); num_assets],
            ended: false,
            delivered_end: false,
            clock_started: false,
            conditional_orders: BTreeMap::new(),
            active_gtd: BTreeMap::new(),
            synthetic_order_events: VecDeque::new(),
            suppressed_terminal: BTreeSet::new(),
            conditional_scratch: Vec::with_capacity(16),
            timers: hftbacktest::backtest::scheduler::TimerQueue::default(),
            timer_scratch: Vec::with_capacity(4),
            current_timer: RuntimeTimer {
                deadline_ts: 0,
                owner_id: 0,
                timer_id: 0,
            },
            funding: VecDeque::with_capacity(8),
            pending_funding: VecDeque::with_capacity(8),
            external_funding: VecDeque::with_capacity(8),
            funding_engines: (0..num_assets)
                .map(|_| {
                    hftbacktest::backtest::execution::FundingEngine::new(
                        hftbacktest::backtest::execution::FundingRounding {
                            increment: 1e-12,
                            mode: hftbacktest::backtest::execution::FundingRoundingMode::Nearest,
                        },
                    )
                    .unwrap()
                })
                .collect(),
            next_funding_sequence: 0,
            current_funding: RuntimeFunding::default(),
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

    fn schedule_timer(&mut self, timer: RuntimeTimer) {
        self.timers
            .schedule(
                timer.deadline_ts,
                hftbacktest::backtest::scheduler::TimerId {
                    owner_id: timer.owner_id,
                    timer_id: timer.timer_id,
                },
            )
            .expect("default timer duplicate policy is Replace");
    }

    fn next_timer_ts(&self) -> Option<i64> {
        self.timer_scratch
            .first()
            .map(|event| event.deadline_ts)
            .or_else(|| self.timers.next_timestamp())
    }

    fn next_timer_event(&mut self) -> Option<RuntimeEvent<'_>> {
        let deadline = self.next_timer_ts()?;
        if self.timer_scratch.is_empty() {
            self.timers.drain_due(deadline, &mut self.timer_scratch);
        }
        let event = self.timer_scratch.remove(0);
        self.current_timer = RuntimeTimer {
            deadline_ts: event.deadline_ts,
            owner_id: event.id.owner_id,
            timer_id: event.id.timer_id,
        };
        let bytes = unsafe {
            std::slice::from_raw_parts(
                (&self.current_timer as *const RuntimeTimer).cast::<u8>(),
                std::mem::size_of::<RuntimeTimer>(),
            )
        };
        Some(RuntimeEvent {
            kind: StrategyEventKind::Timer as u32,
            now: deadline,
            payload: RuntimePayload::Pod {
                ptr: std::ptr::NonNull::new(bytes.as_ptr().cast_mut().cast()).unwrap(),
                len: bytes.len(),
            },
        })
    }

    fn schedule_funding(&mut self, funding: RuntimeFunding) {
        let scheduled = hftbacktest::backtest::execution::ScheduledFunding {
            asset_no: funding.asset_no,
            event: hftbacktest::backtest::execution::FundingEvent {
                event_id: funding.event_id,
                venue_id: hftbacktest::backtest::execution::VenueId(funding.venue_no),
                instrument_id: hftbacktest::backtest::execution::InstrumentId(
                    funding.instrument_id,
                ),
                currency: hftbacktest::backtest::execution::CurrencyId(funding.currency),
                publication_ts: funding.publication_ts,
                effective_ts: funding.effective_ts,
                settlement_ts: funding.settlement_ts,
                rate: funding.rate,
                mark_price: funding.mark_price,
                boundary: hftbacktest::backtest::execution::FundingBoundary::BeforeSettlementEvents,
            },
            delivery_ts: funding.delivery_ts,
        };
        let index = self
            .funding
            .iter()
            .position(|queued| {
                (
                    queued.event.settlement_ts,
                    queued.asset_no,
                    queued.event.event_id,
                ) > (
                    scheduled.event.settlement_ts,
                    scheduled.asset_no,
                    scheduled.event.event_id,
                )
            })
            .unwrap_or(self.funding.len());
        self.funding.insert(index, scheduled);
    }

    fn next_funding_ts(&self) -> Option<i64> {
        let settlement = self.funding.front().map(|event| event.event.settlement_ts);
        let delivery = self
            .pending_funding
            .front()
            .map(|(_, report)| report.delivery_ts);
        let scheduled = match (settlement, delivery) {
            (Some(settlement), Some(delivery)) => Some(settlement.min(delivery)),
            (settlement, delivery) => settlement.or(delivery),
        };
        match (
            scheduled,
            self.external_funding.front().map(|event| event.delivery_ts),
        ) {
            (Some(scheduled), Some(external)) => Some(scheduled.min(external)),
            (scheduled, external) => scheduled.or(external),
        }
    }

    fn process_funding_boundary(&mut self) -> Result<bool, TickRuntimeError<B::Error>> {
        let settlement = self
            .funding
            .front()
            .map(|event| event.event.settlement_ts)
            .unwrap_or(i64::MAX);
        let delivery = self
            .pending_funding
            .front()
            .map(|(_, report)| report.delivery_ts)
            .unwrap_or(i64::MAX);
        let external = self
            .external_funding
            .front()
            .map(|event| event.delivery_ts)
            .unwrap_or(i64::MAX);
        if external <= settlement && external <= delivery {
            self.current_funding = self.external_funding.pop_front().unwrap();
            return Ok(true);
        }
        if delivery <= settlement {
            let Some((asset_no, report)) = self.pending_funding.pop_front() else {
                return Ok(false);
            };
            self.hbt
                .deliver_runtime_funding(report)
                .map_err(TickRuntimeError::Bot)?;
            self.current_funding = RuntimeFunding {
                event_id: report.event.event_id,
                asset_no,
                venue_no: report.event.venue_id.0,
                instrument_id: report.event.instrument_id.0,
                currency: report.event.currency.0,
                publication_ts: report.event.publication_ts,
                effective_ts: report.event.effective_ts,
                settlement_ts: report.event.settlement_ts,
                delivery_ts: report.delivery_ts,
                rate: report.event.rate,
                mark_price: report.event.mark_price,
                position_qty: report.position_qty,
                amount: report.amount,
            };
            return Ok(true);
        }
        let Some(scheduled) = self.funding.pop_front() else {
            return Ok(false);
        };
        let asset_no = scheduled.asset_no as usize;
        if let Some(report) = self
            .hbt
            .settle_runtime_funding(
                scheduled,
                &mut self.funding_engines[asset_no],
                self.next_funding_sequence,
            )
            .map_err(TickRuntimeError::Bot)?
        {
            self.next_funding_sequence = self.next_funding_sequence.wrapping_add(1);
            let index = self
                .pending_funding
                .iter()
                .position(|(_, queued)| {
                    (queued.delivery_ts, queued.sequence) > (report.delivery_ts, report.sequence)
                })
                .unwrap_or(self.pending_funding.len());
            self.pending_funding
                .insert(index, (scheduled.asset_no, report));
        }
        Ok(false)
    }

    fn funding_event(&self) -> RuntimeEvent<'_> {
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
                ptr: std::ptr::NonNull::new(bytes.as_ptr().cast_mut().cast()).unwrap(),
                len: bytes.len(),
            },
        }
    }

    fn refresh_positions(&mut self) -> bool {
        let mut changed = false;
        for (asset_no, position) in self.positions.iter_mut().enumerate() {
            if self.report_projected_positions[asset_no] {
                continue;
            }
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

    fn submit_to_bot(&mut self, command: OrderCommand) -> Result<(), TickRuntimeError<B::Error>> {
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
        self.hbt.register_runtime_order_extensions(
            command.asset_no as usize,
            command.order_id,
            command._reserved[0] & 1 != 0,
        );
        match side {
            Side::Buy => self
                .hbt
                .submit_buy_order(
                    command.asset_no as usize,
                    command.order_id,
                    command.price,
                    command.qty,
                    tif,
                    order_type,
                    false,
                )
                .map(|_| ())
                .map_err(TickRuntimeError::Bot),
            Side::Sell => self
                .hbt
                .submit_sell_order(
                    command.asset_no as usize,
                    command.order_id,
                    command.price,
                    command.qty,
                    tif,
                    order_type,
                    false,
                )
                .map(|_| ())
                .map_err(TickRuntimeError::Bot),
            _ => unreachable!(),
        }
    }

    fn synthesize_order(&mut self, command: OrderCommand, now: i64, status: Status) {
        self.synthetic_order_events.push_back(OrderEvent {
            asset_no: command.asset_no,
            order_id: command.order_id,
            exch_ts: now,
            local_ts: now,
            price: command.price,
            qty: command.qty,
            exec_price: 0.0,
            exec_qty: 0.0,
            side: command.side,
            status: status as u8,
            request: 0,
            maker: 0,
            _reserved: [0; 4],
        });
    }

    fn process_commands(
        &mut self,
        ctx: &mut StrategyRuntimeContext,
        allow_submit: bool,
    ) -> Result<(), TickRuntimeError<B::Error>> {
        let count = ctx.num_commands.min(self.commands.len());
        for index in 0..count {
            let command = self.commands[index];
            match command.kind {
                ORDER_COMMAND_SUBMIT => {
                    if !allow_submit {
                        return Err(TickRuntimeError::InvalidOrder);
                    }
                    if !matches!(command.side, -1 | 1)
                        || command._reserved[1] > 2
                        || (command._reserved[1] != 0
                            && (!command.trigger_price.is_finite() || command.trigger_price <= 0.0))
                        || (command.gtd_expiry_ts != 0 && command.gtd_expiry_ts <= ctx.now)
                    {
                        return Err(TickRuntimeError::InvalidOrder);
                    }
                    if command._reserved[1] != 0 {
                        if self
                            .conditional_orders
                            .insert((command.asset_no, command.order_id), command)
                            .is_some()
                        {
                            return Err(TickRuntimeError::InvalidOrder);
                        }
                        self.synthesize_order(command, ctx.now, Status::New);
                    } else {
                        self.submit_to_bot(command)?;
                        if command.gtd_expiry_ts != 0 {
                            self.active_gtd.insert(
                                (command.asset_no, command.order_id),
                                (command.gtd_expiry_ts, command),
                            );
                        }
                    }
                }
                ORDER_COMMAND_CANCEL => {
                    if let Some(original) = self
                        .conditional_orders
                        .remove(&(command.asset_no, command.order_id))
                    {
                        self.synthesize_order(original, ctx.now, Status::Canceled);
                    } else {
                        self.active_gtd
                            .remove(&(command.asset_no, command.order_id));
                        self.hbt
                            .cancel(command.asset_no as usize, command.order_id, false)
                            .map_err(TickRuntimeError::Bot)?;
                    }
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
    B: Bot<MD> + RuntimeBotEvents<RuntimeError = B::Error>,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    type Error = TickRuntimeError<B::Error>;

    fn next_event(&mut self) -> Result<Option<RuntimeEvent<'_>>, Self::Error> {
        if self.delivered_end
            && self.next_timer_ts().is_none()
            && self.next_funding_ts().is_none()
            && self.synthetic_order_events.is_empty()
        {
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
        if self.next_funding_ts().is_some_and(|timestamp| {
            self.delivered_end || (self.clock_started && timestamp <= self.hbt.current_timestamp())
        }) {
            if self.process_funding_boundary()? {
                return Ok(Some(self.funding_event()));
            }
            return self.next_event();
        }
        if self.tick_pending {
            self.tick_pending = false;
            return Ok(Some(RuntimeEvent {
                kind: StrategyEventKind::Tick as u32,
                now: self.hbt.current_timestamp(),
                payload: RuntimePayload::Ticks(&self.ticks),
            }));
        }

        if let Some(event) = self.synthetic_order_events.pop_front() {
            self.order_events.clear();
            self.order_events.push(event);
            return Ok(Some(RuntimeEvent {
                kind: StrategyEventKind::Order as u32,
                now: event.local_ts,
                payload: RuntimePayload::Orders(&self.order_events),
            }));
        }

        if self.next_timer_ts().is_some_and(|deadline| {
            self.delivered_end || (self.clock_started && deadline <= self.hbt.current_timestamp())
        }) {
            return Ok(self.next_timer_event());
        }

        let next_gtd = self
            .conditional_orders
            .values()
            .filter_map(|command| (command.gtd_expiry_ts != 0).then_some(command.gtd_expiry_ts))
            .chain(self.active_gtd.values().map(|(deadline, _)| *deadline))
            .min();
        let original_interval = self.frame_interval;
        let next_deadline = [next_gtd, self.next_timer_ts(), self.next_funding_ts()]
            .into_iter()
            .flatten()
            .min();
        if let Some(deadline) = next_deadline
            && self.clock_started
        {
            self.frame_interval = self
                .frame_interval
                .min(deadline.saturating_sub(self.hbt.current_timestamp()).max(1));
        }

        let result = self
            .hbt
            .wait_next_feed(true, self.frame_interval)
            .map_err(TickRuntimeError::Bot)?;
        self.clock_started = true;
        self.frame_interval = original_interval;
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
            let key = (*asset_no as u64, order.order_id);
            if self.suppressed_terminal.remove(&key) {
                continue;
            }
            project_order_response(
                *asset_no,
                *recv_ts,
                order,
                &mut self.order_events,
                &mut self.fills,
            );
            if matches!(
                order.status,
                Status::Filled | Status::Canceled | Status::Expired | Status::Rejected
            ) {
                self.active_gtd.remove(&key);
            }
        }
        self.hbt.clear_runtime_order_events();
        for (_, event) in self.hbt.runtime_funding_events() {
            let index = self
                .external_funding
                .iter()
                .position(|queued| {
                    (queued.delivery_ts, queued.event_id) > (event.delivery_ts, event.event_id)
                })
                .unwrap_or(self.external_funding.len());
            self.external_funding.insert(index, *event);
        }
        self.hbt.clear_runtime_funding_events();
        let now = self.hbt.current_timestamp();
        self.conditional_scratch.clear();
        for (key, command) in &self.conditional_orders {
            if command.gtd_expiry_ts != 0 && command.gtd_expiry_ts <= now {
                self.conditional_scratch.push((*key, *command));
            }
        }
        for index in 0..self.conditional_scratch.len() {
            let (key, command) = self.conditional_scratch[index];
            self.conditional_orders.remove(&key);
            self.synthesize_order(command, now, Status::Expired);
        }
        self.conditional_scratch.clear();
        for (key, (deadline, command)) in &self.active_gtd {
            if *deadline <= now {
                self.conditional_scratch.push((*key, *command));
            }
        }
        for index in 0..self.conditional_scratch.len() {
            let (key @ (asset_no, order_id), command) = self.conditional_scratch[index];
            self.active_gtd.remove(&key);
            self.hbt
                .cancel(asset_no as usize, order_id, false)
                .map_err(TickRuntimeError::Bot)?;
            self.suppressed_terminal.insert(key);
            self.synthesize_order(command, now, Status::Expired);
        }
        self.conditional_scratch.clear();
        for (key, command) in &self.conditional_orders {
            let triggered = self.ticks.iter().any(|item| {
                item.asset_no == command.asset_no
                    && item.event.px.is_finite()
                    && item.event.px > 0.0
                    && if command.side == 1 {
                        item.event.px >= command.trigger_price
                    } else {
                        item.event.px <= command.trigger_price
                    }
            });
            if triggered {
                self.conditional_scratch.push((*key, *command));
            }
        }
        for index in 0..self.conditional_scratch.len() {
            let (key, mut command) = self.conditional_scratch[index];
            self.conditional_orders.remove(&key);
            command._reserved[1] = 0;
            self.submit_to_bot(command)?;
            if command.gtd_expiry_ts != 0 {
                self.active_gtd
                    .insert(key, (command.gtd_expiry_ts, command));
            }
        }
        self.position_pending = self.refresh_positions();
        // Live connectors and legacy Python Backtest wrappers may not expose the shared local
        // portfolio through Bot::position. The canonical response itself is already at the local
        // visibility boundary, so use its independent fills as the compatibility projection.
        if !self.position_pending && !self.fills.is_empty() {
            for fill in &self.fills {
                let position = &mut self.positions[fill.asset_no as usize];
                *position += fill.qty * f64::from(fill.side);
                self.report_projected_positions[fill.asset_no as usize] = true;
            }
            self.position_pending = true;
        }
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

struct HybridHistorySlot {
    asset_no: u64,
    timeframe_ns: i64,
    history: BarHistory,
}

#[derive(Clone, Copy)]
enum BufferedTickKind {
    Tick,
    Order,
    Filled,
    Position,
}

/// Deterministic merge source used by Python hybrid mode. Bars are signal-only; every strategy
/// command is forwarded to the Tick Bot, so a Bar matcher can never double-fill the same order.
struct HybridFrameSource<'a, B, MD> {
    tick: TickFrameSource<'a, B, MD>,
    tick_frame_interval: i64,
    bars: MaterializedBarFeed,
    bar_schedule: Vec<(i64, i64)>,
    next_bar_group: usize,
    bar_pending_commit: bool,
    histories: Vec<HybridHistorySlot>,
    history_views: Vec<BarHistoryView>,
    buffered_kind: Option<BufferedTickKind>,
    buffered_now: i64,
    ticks: Vec<TickItem>,
    orders: Vec<OrderEvent>,
    fills: Vec<FillEvent>,
    tick_exhausted: bool,
}

#[derive(Debug, thiserror::Error)]
enum HybridRuntimeError<E: std::error::Error + Send + Sync + 'static> {
    #[error(transparent)]
    Tick(#[from] TickRuntimeError<E>),
    #[error("hybrid Bar feed failed: {0}")]
    Bar(#[from] hftbacktest::backtest::bar::BarFeedError),
    #[error("hybrid Tick execution source ended before the remaining Bar signal stream")]
    MissingTickData,
}

impl<'a, B, MD> HybridFrameSource<'a, B, MD>
where
    MD: MarketDepth,
    B: Bot<MD> + RuntimeBotEvents<RuntimeError = B::Error>,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    fn new(
        hbt: &'a mut B,
        records: &[TimedBarItem],
        history_capacity: usize,
        frame_interval: i64,
        max_tick_batch: usize,
    ) -> Result<Self, hftbacktest::backtest::bar::BarFeedError> {
        let bars = MaterializedBarFeed::new(records)?;
        let mut bar_schedule = Vec::new();
        for record in records {
            let key = (record.bar.close_ts, record.timeframe_ns);
            if bar_schedule.last().copied() != Some(key) {
                bar_schedule.push(key);
            }
        }
        let mut keys: Vec<_> = records
            .iter()
            .map(|record| (record.asset_no, record.timeframe_ns))
            .collect();
        keys.sort_unstable();
        keys.dedup();
        let histories: Vec<_> = keys
            .into_iter()
            .map(|(asset_no, timeframe_ns)| HybridHistorySlot {
                asset_no,
                timeframe_ns,
                history: BarHistory::new(history_capacity),
            })
            .collect();
        let history_views = histories.iter().map(Self::history_view).collect();
        Ok(Self {
            tick: TickFrameSource::new(hbt, frame_interval, max_tick_batch),
            tick_frame_interval: frame_interval,
            bars,
            bar_schedule,
            next_bar_group: 0,
            bar_pending_commit: false,
            histories,
            history_views,
            buffered_kind: None,
            buffered_now: 0,
            ticks: Vec::with_capacity(max_tick_batch.min(4096)),
            orders: Vec::new(),
            fills: Vec::new(),
            tick_exhausted: false,
        })
    }

    fn history_view(slot: &HybridHistorySlot) -> BarHistoryView {
        BarHistoryView {
            asset_no: slot.asset_no,
            timeframe_ns: slot.timeframe_ns,
            bars_ptr: slot.history.as_ptr(),
            capacity: slot.history.capacity(),
            len: slot.history.len(),
            next: slot.history.next_index(),
        }
    }

    fn refresh_history_views(&mut self) {
        for (view, slot) in self.history_views.iter_mut().zip(&self.histories) {
            *view = Self::history_view(slot);
        }
    }

    fn configure_context(&mut self, ctx: &mut StrategyRuntimeContext) {
        self.tick.configure_context(ctx);
        ctx.histories_ptr = self.history_views.as_ptr();
        ctx.num_histories = self.history_views.len();
    }

    fn next_bar_close(&self) -> Option<i64> {
        self.bar_schedule
            .get(self.next_bar_group)
            .map(|(close_ts, _)| *close_ts)
    }

    fn buffer_tick(&mut self) -> Result<(), TickRuntimeError<B::Error>> {
        if self.buffered_kind.is_some() || self.tick_exhausted {
            return Ok(());
        }
        if let Some(close_ts) = self.next_bar_close() {
            let now = self.tick.hbt.current_timestamp();
            let until_close = close_ts.saturating_sub(now);
            self.tick.frame_interval = self.tick_frame_interval.min(until_close.max(1));
        } else {
            self.tick.frame_interval = self.tick_frame_interval;
        }
        let Some(event) = self.tick.next_event()? else {
            self.tick_exhausted = true;
            return Ok(());
        };
        self.buffered_now = event.now;
        match event.payload {
            RuntimePayload::Ticks(items) => {
                self.ticks.clear();
                self.ticks.extend_from_slice(items);
                self.buffered_kind = Some(BufferedTickKind::Tick);
            }
            RuntimePayload::Orders(items) => {
                self.orders.clear();
                self.orders.extend_from_slice(items);
                self.buffered_kind = Some(BufferedTickKind::Order);
            }
            RuntimePayload::Fills(items) => {
                self.fills.clear();
                self.fills.extend_from_slice(items);
                self.buffered_kind = Some(BufferedTickKind::Filled);
            }
            RuntimePayload::None => self.buffered_kind = Some(BufferedTickKind::Position),
            RuntimePayload::Bars { .. } | RuntimePayload::Pod { .. } => unreachable!(),
        }
        Ok(())
    }

    fn buffered_event(&mut self) -> Option<RuntimeEvent<'_>> {
        let kind = self.buffered_kind.take()?;
        let (kind, payload) = match kind {
            BufferedTickKind::Tick => (
                StrategyEventKind::Tick as u32,
                RuntimePayload::Ticks(&self.ticks),
            ),
            BufferedTickKind::Order => (
                StrategyEventKind::Order as u32,
                RuntimePayload::Orders(&self.orders),
            ),
            BufferedTickKind::Filled => (
                StrategyEventKind::Filled as u32,
                RuntimePayload::Fills(&self.fills),
            ),
            BufferedTickKind::Position => {
                (StrategyEventKind::Position as u32, RuntimePayload::None)
            }
        };
        Some(RuntimeEvent {
            kind,
            now: self.buffered_now,
            payload,
        })
    }

    fn next_bar_event(&mut self) -> Result<Option<RuntimeEvent<'_>>, HybridRuntimeError<B::Error>> {
        let Some(meta) = self.bars.next_batch()? else {
            return Ok(None);
        };
        self.next_bar_group += 1;
        self.bar_pending_commit = true;
        Ok(Some(RuntimeEvent {
            kind: StrategyEventKind::Bar as u32,
            now: meta.close_ts,
            payload: RuntimePayload::Bars {
                timeframe_ns: meta.timeframe_ns,
                close_ts: meta.close_ts,
                bars: self.bars.batch(),
            },
        }))
    }
}

impl<B, MD> RuntimeEventSource for HybridFrameSource<'_, B, MD>
where
    MD: MarketDepth,
    B: Bot<MD> + RuntimeBotEvents<RuntimeError = B::Error>,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    type Error = HybridRuntimeError<B::Error>;

    fn next_event(&mut self) -> Result<Option<RuntimeEvent<'_>>, Self::Error> {
        self.buffer_tick()?;
        let bar_close = self.next_bar_close();
        if self.tick_exhausted && bar_close.is_some() {
            return Err(HybridRuntimeError::MissingTickData);
        }
        // Tick-side reports and market data at the close boundary are visible before the closed
        // Bar signal. This includes every Tick in the half-open Bar interval exactly once.
        if self
            .buffered_kind
            .is_some_and(|_| self.buffered_now <= bar_close.unwrap_or(i64::MAX))
        {
            return Ok(self.buffered_event());
        }
        if bar_close.is_some() {
            return self.next_bar_event();
        }
        Ok(self.buffered_event())
    }

    fn after_callback(
        &mut self,
        kind: u32,
        ctx: &mut StrategyRuntimeContext,
    ) -> Result<(), Self::Error> {
        self.tick.after_callback(kind, ctx)?;
        if kind == StrategyEventKind::Bar as u32 && self.bar_pending_commit {
            for item in self.bars.batch() {
                if let Some(slot) = self.histories.iter_mut().find(|slot| {
                    slot.asset_no == item.asset_no
                        && slot.timeframe_ns == item.bar.close_ts - item.bar.open_ts
                }) {
                    slot.history.push(item.bar);
                }
            }
            self.refresh_history_views();
            self.bar_pending_commit = false;
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
    timers: &[RuntimeTimer],
    funding: &[RuntimeFunding],
) -> i64
where
    MD: MarketDepth,
    B: Bot<MD> + RuntimeBotEvents<RuntimeError = B::Error>,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    if ctx.abi_version != hftbacktest::runtime::STRATEGY_ABI_VERSION
        || ctx.struct_size as usize != std::mem::size_of::<StrategyRuntimeContext>()
    {
        return -5;
    }
    if frame_interval <= 0 || max_tick_batch == 0 {
        return -2;
    }
    ctx.bot_ptr = (hbt as *mut B).cast();
    let callbacks = unsafe { callback_registry(callback_addresses, callback_count) };
    let mut source = TickFrameSource::new(hbt, frame_interval, max_tick_batch);
    for timer in timers.iter().copied() {
        source.schedule_timer(timer);
    }
    for event in funding.iter().copied() {
        source.schedule_funding(event);
    }
    source.configure_context(ctx);
    match run_event_runtime(&mut source, &callbacks, ctx) {
        Ok(()) => 0,
        Err(error) => {
            eprintln!("strategy runtime failed: {error}");
            -1
        }
    }
}

unsafe fn run_hybrid_runtime<B, MD>(
    hbt: &mut B,
    records: &[TimedBarItem],
    ctx: &mut StrategyRuntimeContext,
    callback_addresses: *const usize,
    callback_count: usize,
    history_capacity: usize,
    frame_interval: i64,
    max_tick_batch: usize,
    timers: &[RuntimeTimer],
    funding: &[RuntimeFunding],
) -> i64
where
    MD: MarketDepth,
    B: Bot<MD> + RuntimeBotEvents<RuntimeError = B::Error>,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    if ctx.abi_version != hftbacktest::runtime::STRATEGY_ABI_VERSION
        || ctx.struct_size as usize != std::mem::size_of::<StrategyRuntimeContext>()
    {
        return -5;
    }
    if frame_interval <= 0 || max_tick_batch == 0 {
        return -2;
    }
    ctx.bot_ptr = (hbt as *mut B).cast();
    let callbacks = unsafe { callback_registry(callback_addresses, callback_count) };
    let mut source = match HybridFrameSource::new(
        hbt,
        records,
        history_capacity,
        frame_interval,
        max_tick_batch,
    ) {
        Ok(source) => source,
        Err(error) => {
            eprintln!("invalid hybrid Bar input: {error}");
            return -4;
        }
    };
    for timer in timers.iter().copied() {
        source.tick.schedule_timer(timer);
    }
    for event in funding.iter().copied() {
        source.tick.schedule_funding(event);
    }
    source.configure_context(ctx);
    match run_event_runtime(&mut source, &callbacks, ctx) {
        Ok(()) => 0,
        Err(error) => {
            eprintln!("hybrid strategy runtime failed: {error}");
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
            &[],
            &[],
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
            &[],
            &[],
        )
    }
}

macro_rules! scheduled_tick_backtest_entry {
    ($name:ident, $bot:ty) => {
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn $name(
            hbt_ptr: *mut $bot,
            timers_ptr: *const RuntimeTimer,
            timer_count: usize,
            funding_ptr: *const RuntimeFunding,
            funding_count: usize,
            ctx_ptr: *mut StrategyRuntimeContext,
            callbacks: *const usize,
            callback_count: usize,
            frame_interval: i64,
            max_tick_batch: usize,
        ) -> i64 {
            if hbt_ptr.is_null()
                || ctx_ptr.is_null()
                || (timer_count > 0 && timers_ptr.is_null())
                || (funding_count > 0 && funding_ptr.is_null())
            {
                return -3;
            }
            let timers = if timer_count == 0 {
                &[]
            } else {
                unsafe { std::slice::from_raw_parts(timers_ptr, timer_count) }
            };
            let funding = if funding_count == 0 {
                &[]
            } else {
                unsafe { std::slice::from_raw_parts(funding_ptr, funding_count) }
            };
            unsafe {
                run_tick_runtime(
                    &mut *hbt_ptr,
                    &mut *ctx_ptr,
                    callbacks,
                    callback_count,
                    frame_interval,
                    max_tick_batch,
                    timers,
                    funding,
                )
            }
        }
    };
}

scheduled_tick_backtest_entry!(
    hashmapbt_run_scheduled_tick_runtime,
    HashMapMarketDepthBacktest
);
scheduled_tick_backtest_entry!(
    roivecbt_run_scheduled_tick_runtime,
    ROIVectorMarketDepthBacktest
);
#[cfg(feature = "live")]
scheduled_tick_backtest_entry!(
    hashmaplive_run_scheduled_tick_runtime,
    HashMapMarketDepthLiveBot
);
#[cfg(feature = "live")]
scheduled_tick_backtest_entry!(
    roiveclive_run_scheduled_tick_runtime,
    ROIVectorMarketDepthLiveBot
);

macro_rules! hybrid_backtest_entry {
    ($name:ident, $bot:ty) => {
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn $name(
            hbt_ptr: *mut $bot,
            records_ptr: *const TimedBarItem,
            record_count: usize,
            ctx_ptr: *mut StrategyRuntimeContext,
            callbacks: *const usize,
            callback_count: usize,
            history_capacity: usize,
            frame_interval: i64,
            max_tick_batch: usize,
        ) -> i64 {
            if hbt_ptr.is_null() || ctx_ptr.is_null() || (record_count > 0 && records_ptr.is_null())
            {
                return -3;
            }
            let records = if record_count == 0 {
                &[]
            } else {
                unsafe { std::slice::from_raw_parts(records_ptr, record_count) }
            };
            unsafe {
                run_hybrid_runtime(
                    &mut *hbt_ptr,
                    records,
                    &mut *ctx_ptr,
                    callbacks,
                    callback_count,
                    history_capacity,
                    frame_interval,
                    max_tick_batch,
                    &[],
                    &[],
                )
            }
        }
    };
}

hybrid_backtest_entry!(hashmapbt_run_hybrid_runtime, HashMapMarketDepthBacktest);
hybrid_backtest_entry!(roivecbt_run_hybrid_runtime, ROIVectorMarketDepthBacktest);

macro_rules! scheduled_hybrid_backtest_entry {
    ($name:ident, $bot:ty) => {
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn $name(
            hbt_ptr: *mut $bot,
            records_ptr: *const TimedBarItem,
            record_count: usize,
            timers_ptr: *const RuntimeTimer,
            timer_count: usize,
            funding_ptr: *const RuntimeFunding,
            funding_count: usize,
            ctx_ptr: *mut StrategyRuntimeContext,
            callbacks: *const usize,
            callback_count: usize,
            history_capacity: usize,
            frame_interval: i64,
            max_tick_batch: usize,
        ) -> i64 {
            if hbt_ptr.is_null()
                || ctx_ptr.is_null()
                || (record_count > 0 && records_ptr.is_null())
                || (timer_count > 0 && timers_ptr.is_null())
                || (funding_count > 0 && funding_ptr.is_null())
            {
                return -3;
            }
            let records = if record_count == 0 {
                &[]
            } else {
                unsafe { std::slice::from_raw_parts(records_ptr, record_count) }
            };
            let timers = if timer_count == 0 {
                &[]
            } else {
                unsafe { std::slice::from_raw_parts(timers_ptr, timer_count) }
            };
            let funding = if funding_count == 0 {
                &[]
            } else {
                unsafe { std::slice::from_raw_parts(funding_ptr, funding_count) }
            };
            unsafe {
                run_hybrid_runtime(
                    &mut *hbt_ptr,
                    records,
                    &mut *ctx_ptr,
                    callbacks,
                    callback_count,
                    history_capacity,
                    frame_interval,
                    max_tick_batch,
                    timers,
                    funding,
                )
            }
        }
    };
}

scheduled_hybrid_backtest_entry!(
    hashmapbt_run_scheduled_hybrid_runtime,
    HashMapMarketDepthBacktest
);
scheduled_hybrid_backtest_entry!(
    roivecbt_run_scheduled_hybrid_runtime,
    ROIVectorMarketDepthBacktest
);

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
            &[],
            &[],
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
            &[],
            &[],
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
    if ctx.abi_version != hftbacktest::runtime::STRATEGY_ABI_VERSION
        || ctx.struct_size as usize != std::mem::size_of::<StrategyRuntimeContext>()
    {
        return -5;
    }
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

#[unsafe(no_mangle)]
pub unsafe extern "C" fn run_scheduled_materialized_bar_runtime(
    records_ptr: *const TimedBarItem,
    record_count: usize,
    timers_ptr: *const RuntimeTimer,
    timer_count: usize,
    funding_ptr: *const RuntimeFunding,
    funding_count: usize,
    ctx_ptr: *mut StrategyRuntimeContext,
    callbacks: *const usize,
    callback_count: usize,
    history_capacity: usize,
) -> i64 {
    if records_ptr.is_null()
        || ctx_ptr.is_null()
        || (timer_count > 0 && timers_ptr.is_null())
        || (funding_count > 0 && funding_ptr.is_null())
    {
        return -3;
    }
    let records = unsafe { std::slice::from_raw_parts(records_ptr, record_count) };
    let timers = if timer_count == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(timers_ptr, timer_count) }
    };
    let funding = if funding_count == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(funding_ptr, funding_count) }
    };
    let mut source = match MaterializedBarSource::new(records, history_capacity) {
        Ok(source) => source,
        Err(error) => {
            eprintln!("invalid materialized bar input: {error}");
            return -4;
        }
    };
    for timer in timers.iter().copied() {
        source.schedule_timer(timer);
    }
    for event in funding.iter().copied() {
        source.schedule_funding(event);
    }
    let ctx = unsafe { &mut *ctx_ptr };
    if ctx.abi_version != hftbacktest::runtime::STRATEGY_ABI_VERSION
        || ctx.struct_size as usize != std::mem::size_of::<StrategyRuntimeContext>()
    {
        return -5;
    }
    source.configure_context(ctx);
    let callbacks = unsafe { callback_registry(callbacks, callback_count) };
    match run_event_runtime(&mut source, &callbacks, ctx) {
        Ok(()) => 0,
        Err(error) => {
            eprintln!("scheduled bar strategy runtime failed: {error}");
            -1
        }
    }
}

/// Configured Bar runtime used by the Python facade for explicit OHLC assumptions. Mode 0 keeps
/// conservative NextOpen compatibility, 1 enables Touch and 2 enables ConservativeOhlc.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn run_configured_materialized_bar_runtime(
    records_ptr: *const TimedBarItem,
    record_count: usize,
    timers_ptr: *const RuntimeTimer,
    timer_count: usize,
    funding_ptr: *const RuntimeFunding,
    funding_count: usize,
    ctx_ptr: *mut StrategyRuntimeContext,
    callbacks: *const usize,
    callback_count: usize,
    history_capacity: usize,
    matching_mode: u32,
    volume_participation: f64,
) -> i64 {
    if records_ptr.is_null()
        || ctx_ptr.is_null()
        || (timer_count > 0 && timers_ptr.is_null())
        || (funding_count > 0 && funding_ptr.is_null())
    {
        return -3;
    }
    let records = unsafe { std::slice::from_raw_parts(records_ptr, record_count) };
    let timers = if timer_count == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(timers_ptr, timer_count) }
    };
    let funding = if funding_count == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(funding_ptr, funding_count) }
    };
    let mut source = match MaterializedBarSource::new(records, history_capacity) {
        Ok(source) => source,
        Err(error) => {
            eprintln!("invalid configured bar input: {error}");
            return -4;
        }
    };
    let assumption = match matching_mode {
        0 => None,
        1 => Some(OhlcFillAssumption::Touch),
        2 => Some(OhlcFillAssumption::Conservative),
        _ => return -4,
    };
    if let Some(assumption) = assumption
        && source
            .configure_ohlc_matching(assumption, volume_participation)
            .is_err()
    {
        return -4;
    }
    for timer in timers.iter().copied() {
        source.schedule_timer(timer);
    }
    for event in funding.iter().copied() {
        source.schedule_funding(event);
    }
    let ctx = unsafe { &mut *ctx_ptr };
    if ctx.abi_version != hftbacktest::runtime::STRATEGY_ABI_VERSION
        || ctx.struct_size as usize != std::mem::size_of::<StrategyRuntimeContext>()
    {
        return -5;
    }
    source.configure_context(ctx);
    let callbacks = unsafe { callback_registry(callbacks, callback_count) };
    match run_event_runtime(&mut source, &callbacks, ctx) {
        Ok(()) => 0,
        Err(error) => {
            eprintln!("configured bar strategy runtime failed: {error}");
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
