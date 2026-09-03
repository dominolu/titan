use std::{
    collections::{HashMap, hash_map::Entry},
    time::{Duration, Instant},
};

use chrono::Utc;
use rand::Rng;
use thiserror::Error;
use tracing::{debug, info};

use crate::{
    backtest::execution::{
        AccountDelta, AccountReport, CurrencyId, ExecutionEventProjector, ExecutionReason,
        FundingBoundary, FundingEvent, FundingReport, InstrumentId, LIVE_EXECUTION_ABI_VERSION,
        LiveExecutionAdapter, LiveExecutionEvent, LiveOrderStatus, PortfolioLedger, ProjectedEvent,
        VenueId,
    },
    depth::{L2MarketDepth, MarketDepth},
    live::{Instrument, ipc::Channel},
    types::{
        Bot, BuildError, ElapseResult, Event, LOCAL_ASK_DEPTH_EVENT, LOCAL_BID_DEPTH_EVENT,
        LOCAL_BUY_TRADE_EVENT, LOCAL_SELL_TRADE_EVENT, LiveError, LiveEvent, LiveRequest, OrdType,
        Order, OrderId, OrderRequest, Side, StateValues, Status, TimeInForce, WaitOrderResponse,
    },
};
use titan_runtime_abi::RuntimeFunding;

#[derive(Error, Debug)]
pub enum BotError {
    #[error("OrderIdExist")]
    OrderIdExist,
    #[error("AssetNotFound")]
    InstrumentNotFound,
    #[error("OrderNotFound")]
    OrderNotFound,
    #[error("InvalidOrderStatus")]
    InvalidOrderStatus,
    #[error("Timeout")]
    Timeout,
    #[error("Interrupted")]
    Interrupted,
    #[error("UnsupportedOperation: {0}")]
    UnsupportedOperation(&'static str),
    #[error("Custom: {0}")]
    Custom(String),
}

pub type ErrorHandler = Box<dyn Fn(LiveError) -> Result<(), BotError>>;
pub type OrderRecvHook = Box<dyn Fn(&Order, &Order) -> Result<(), BotError>>;

fn generate_random_id() -> u64 {
    // Initialize the random number generator
    let mut rng = rand::rng();

    // Generate a random u64 value
    rng.random::<u64>()
}

/// Live [`LiveBot`] builder.
pub struct LiveBotBuilder<MD> {
    id: u64,
    instruments: Vec<Instrument<MD>>,
    error_handler: Option<ErrorHandler>,
    order_hook: Option<OrderRecvHook>,
}

impl<MD> Default for LiveBotBuilder<MD> {
    fn default() -> Self {
        Self::new()
    }
}

impl<MD> LiveBotBuilder<MD> {
    /// Constructs a builder to construct [`LiveBot`] instances.
    pub fn new() -> Self {
        Self {
            id: generate_random_id(),
            instruments: Default::default(),
            error_handler: None,
            order_hook: None,
        }
    }

    /// Registers an instrument.
    pub fn register(self, instrument: Instrument<MD>) -> Self {
        Self {
            instruments: {
                let mut instruments = self.instruments;
                instruments.push(instrument);
                instruments
            },
            ..self
        }
    }

    /// Registers the error handler to deal with an error from connectors.
    pub fn error_handler<Handler>(self, handler: Handler) -> Self
    where
        Handler: Fn(LiveError) -> Result<(), BotError> + 'static,
    {
        Self {
            error_handler: Some(Box::new(handler)),
            ..self
        }
    }

    /// Registers the order response receive hook.
    pub fn order_recv_hook<Hook>(self, hook: Hook) -> Self
    where
        Hook: Fn(&Order, &Order) -> Result<(), BotError> + 'static,
    {
        Self {
            order_hook: Some(Box::new(hook)),
            ..self
        }
    }

    /// Sets the bot ID. It must be unique among all bots connected to the same `Connector`.
    pub fn id(self, id: u64) -> Self {
        Self { id, ..self }
    }

    /// Builds a live [`LiveBot`] based on the registered connectors and assets.
    pub fn build<CH>(self) -> Result<LiveBot<CH, MD>, BuildError>
    where
        CH: Channel,
    {
        let id = self.id;
        let mut channel = CH::build(&self.instruments)?;

        // Requests to prepare a given asset for trading.
        // The Connector will send the current orders on this asset.
        for (inst_no, instrument) in self.instruments.iter().enumerate() {
            info!(
                connector_name = instrument.connector_name,
                symbol = instrument.symbol,
                "Registers the instrument."
            );
            channel
                .send(
                    id,
                    inst_no,
                    LiveRequest::RegisterInstrument {
                        symbol: instrument.symbol.clone(),
                        tick_size: instrument.tick_size,
                        lot_size: instrument.lot_size,
                    },
                )
                .map_err(|error| BuildError::Error(anyhow::Error::from(error)))?;
        }

        Ok(LiveBot {
            id,
            channel,
            instruments: self.instruments,
            error_handler: self.error_handler,
            order_hook: self.order_hook,
            runtime_feed_events: Vec::new(),
            runtime_order_events: Vec::new(),
            runtime_projected_events: Vec::new(),
            runtime_funding_events: Vec::new(),
            runtime_execution_adapter: LiveExecutionAdapter::new(LIVE_EXECUTION_ABI_VERSION)
                .expect("built-in live execution ABI must match"),
            runtime_projector: ExecutionEventProjector::with_capacity(3),
            runtime_local_portfolio: PortfolioLedger::default(),
            next_runtime_execution_sequence: 0,
            runtime_capture_enabled: false,
        })
    }
}

/// A live trading bot.
///
/// Provides the same interface as the backtesters in [`backtest`](`crate::backtest`).
///
/// ```
/// use hftbacktest::{
///     live::{Instrument, LiveBotBuilder, ipc::iceoryx::IceoryxUnifiedChannel},
///     prelude::HashMapMarketDepth,
/// };
///
/// let tick_size = 0.1;
/// let lot_size = 1.0;
///
/// let mut hbt = LiveBotBuilder::new()
///     .register(Instrument::new(
///         "connector_name",
///         "symbol",
///         tick_size,
///         lot_size,
///         HashMapMarketDepth::new(tick_size, lot_size),
///         0
///     ))
///     .build::<IceoryxUnifiedChannel>()
///     .unwrap();
/// ```
pub struct LiveBot<CH, MD> {
    id: u64,
    channel: CH,
    instruments: Vec<Instrument<MD>>,
    error_handler: Option<ErrorHandler>,
    order_hook: Option<OrderRecvHook>,
    runtime_feed_events: Vec<(usize, Event)>,
    runtime_order_events: Vec<(usize, i64, Order)>,
    runtime_projected_events: Vec<(usize, ProjectedEvent)>,
    runtime_funding_events: Vec<(usize, RuntimeFunding)>,
    runtime_execution_adapter: LiveExecutionAdapter,
    runtime_projector: ExecutionEventProjector,
    runtime_local_portfolio: PortfolioLedger,
    next_runtime_execution_sequence: u64,
    runtime_capture_enabled: bool,
}

impl<CH, MD> LiveBot<CH, MD>
where
    CH: Channel,
    MD: MarketDepth + L2MarketDepth,
{
    pub fn runtime_feed_events(&self) -> &[(usize, Event)] {
        &self.runtime_feed_events
    }

    pub fn clear_runtime_feed_events(&mut self) {
        self.runtime_feed_events.clear();
    }

    pub fn runtime_order_events(&self) -> &[(usize, i64, Order)] {
        &self.runtime_order_events
    }

    pub fn clear_runtime_order_events(&mut self) {
        self.runtime_order_events.clear();
    }

    pub fn drain_runtime_projected_events(&mut self, output: &mut Vec<(usize, ProjectedEvent)>) {
        output.append(&mut self.runtime_projected_events);
    }

    pub fn runtime_funding_events(&self) -> &[(usize, RuntimeFunding)] {
        &self.runtime_funding_events
    }

    pub fn clear_runtime_funding_events(&mut self) {
        self.runtime_funding_events.clear();
    }

    pub fn set_runtime_capture(&mut self, enabled: bool) {
        if enabled && !self.runtime_capture_enabled {
            self.runtime_execution_adapter.reset();
            self.runtime_projector.reset();
            self.runtime_local_portfolio.reset();
            self.next_runtime_execution_sequence = 0;
        }
        self.runtime_capture_enabled = enabled;
        if !enabled {
            self.runtime_feed_events.clear();
            self.runtime_order_events.clear();
            self.runtime_projected_events.clear();
            self.runtime_funding_events.clear();
        }
    }

    #[inline]
    fn process_feed_event(&mut self, inst_no: usize, event: Event) {
        if self.runtime_capture_enabled {
            self.runtime_feed_events.push((inst_no, event.clone()));
        }
        let instrument = unsafe { self.instruments.get_unchecked_mut(inst_no) };
        instrument.last_feed_latency = Some((event.exch_ts, event.local_ts));
        if event.is(LOCAL_BID_DEPTH_EVENT) {
            instrument
                .depth
                .update_bid_depth(event.px, event.qty, event.exch_ts);
        } else if event.is(LOCAL_ASK_DEPTH_EVENT) {
            instrument
                .depth
                .update_ask_depth(event.px, event.qty, event.exch_ts);
        } else if (event.is(LOCAL_BUY_TRADE_EVENT) || event.is(LOCAL_SELL_TRADE_EVENT))
            && instrument.last_trades.capacity() > 0
        {
            instrument.last_trades.push(event);
        }
    }

    fn process_event<const WAIT_NEXT_FEED: bool>(
        &mut self,
        inst_no: usize,
        ev: LiveEvent,
        wait_order_response: WaitOrderResponse,
    ) -> Result<ElapseResult, BotError> {
        match ev {
            LiveEvent::Feed { event, .. } => {
                self.process_feed_event(inst_no, event);
                if WAIT_NEXT_FEED {
                    return Ok(ElapseResult::MarketFeed);
                }
            }
            LiveEvent::FeedBatch { events, .. } => {
                for event in events {
                    self.process_feed_event(inst_no, event);
                }
                if WAIT_NEXT_FEED {
                    return Ok(ElapseResult::MarketFeed);
                }
            }
            LiveEvent::Order { order, .. } => {
                debug!(%inst_no, ?order, "Event::Order");
                let recv_ts = Utc::now().timestamp_nanos_opt().unwrap();
                if self.runtime_capture_enabled {
                    self.runtime_order_events
                        .push((inst_no, recv_ts, order.clone()));
                    let fill = order.exec_qty > 0.0
                        && matches!(order.status, Status::PartiallyFilled | Status::Filled);
                    let status = if fill {
                        if order.status == Status::Filled {
                            LiveOrderStatus::Filled
                        } else {
                            LiveOrderStatus::PartiallyFilled
                        }
                    } else {
                        match order.status {
                            Status::Rejected => LiveOrderStatus::Rejected,
                            Status::Canceled => LiveOrderStatus::Canceled,
                            Status::Expired => LiveOrderStatus::Expired,
                            _ => LiveOrderStatus::Accepted,
                        }
                    };
                    let instrument_id = InstrumentId(inst_no as u32 + 1);
                    let account_delta = fill.then_some(AccountDelta {
                        instrument_id,
                        position_delta: order.exec_qty * f64::from(order.side as i8),
                        trade_qty: order.exec_qty,
                        trade_value: order.exec_price() * order.exec_qty,
                        currency: CurrencyId(0),
                        cash_delta: 0.0,
                        fee: 0.0,
                        funding: 0.0,
                        execution_price: order.exec_price(),
                        realized_pnl: 0.0,
                    });
                    let sequence = self.next_runtime_execution_sequence;
                    self.next_runtime_execution_sequence = self
                        .next_runtime_execution_sequence
                        .checked_add(1)
                        .expect("live execution sequence overflow");
                    let event = LiveExecutionEvent {
                        event_id: u128::from(sequence),
                        venue_id: VenueId(0),
                        instrument_id,
                        asset_no: inst_no as u32,
                        order_id: order.order_id,
                        // Legacy connectors do not expose this field. Preserve a stable non-zero
                        // compatibility identity while canonical connectors use the adapter API.
                        venue_order_id: order.order_id,
                        exchange_ts: order.exch_timestamp,
                        delivery_ts: recv_ts,
                        sequence,
                        status,
                        reason: if order.status == Status::Rejected {
                            ExecutionReason::Unknown(1)
                        } else {
                            ExecutionReason::None
                        },
                        side: order.side,
                        order_price: order.price(),
                        order_qty: order.qty,
                        exec_price: order.exec_price(),
                        exec_qty: order.exec_qty,
                        cumulative_filled_qty: order.qty - order.leaves_qty,
                        maker: order.maker,
                        account_delta,
                    };
                    if let Some(report) = self
                        .runtime_execution_adapter
                        .normalize(event)
                        .map_err(|error| BotError::Custom(error.to_string()))?
                    {
                        self.runtime_projected_events.extend(
                            self.runtime_projector
                                .project(
                                    report,
                                    self.runtime_local_portfolio
                                        .venue_mut_or_insert(report.venue_id),
                                )
                                .map_err(|error| BotError::Custom(error.to_string()))?
                                .iter()
                                .copied()
                                .map(|event| (inst_no, event)),
                        );
                    }
                }
                let received_order_resp = match wait_order_response {
                    WaitOrderResponse::Any => true,
                    WaitOrderResponse::Specified {
                        asset_no: wait_order_asset_no,
                        order_id: wait_order_id,
                    } if wait_order_id == order.order_id && wait_order_asset_no == inst_no => true,
                    _ => false,
                };
                let instrument = unsafe { self.instruments.get_unchecked_mut(inst_no) };
                instrument.last_order_latency =
                    Some((order.local_timestamp, order.exch_timestamp, recv_ts));
                match instrument.orders.entry(order.order_id) {
                    Entry::Occupied(mut entry) => {
                        let ex_order = entry.get_mut();
                        if let Some(hook) = self.order_hook.as_mut() {
                            hook(ex_order, &order)?;
                        }
                        if order.exch_timestamp >= ex_order.exch_timestamp {
                            if ex_order.status == Status::Canceled
                                || ex_order.status == Status::Expired
                                || ex_order.status == Status::Filled
                            {
                                // Ignores the update since the current status is the final status.
                            } else {
                                ex_order.update(&order);
                            }
                        }
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(order);
                    }
                }
                if received_order_resp {
                    return Ok(ElapseResult::OrderResponse);
                }
            }
            LiveEvent::ExecutionReport {
                event_id,
                venue_no,
                instrument_id,
                asset_no,
                order_id,
                venue_order_id,
                exchange_ts,
                delivery_ts,
                sequence,
                status,
                reason,
                side,
                order_price,
                order_qty,
                leaves_qty,
                exec_price,
                exec_qty,
                maker,
                local_submit_ts,
                time_in_force,
                order_type,
                request,
                account_delta,
                ..
            } => {
                if asset_no as usize != inst_no {
                    return Err(BotError::Custom(
                        "canonical live execution asset does not match channel routing".into(),
                    ));
                }
                let status = match status {
                    1 => LiveOrderStatus::Accepted,
                    2 => LiveOrderStatus::Rejected,
                    3 => LiveOrderStatus::Canceled,
                    4 => LiveOrderStatus::Expired,
                    5 => LiveOrderStatus::PartiallyFilled,
                    6 => LiveOrderStatus::Filled,
                    value => LiveOrderStatus::Unknown(value),
                };
                let side = match side {
                    1 => Side::Buy,
                    -1 => Side::Sell,
                    _ => return Err(BotError::Custom("invalid canonical live side".into())),
                };
                let legacy_status = match status {
                    LiveOrderStatus::Accepted => Status::New,
                    LiveOrderStatus::Rejected => Status::Rejected,
                    LiveOrderStatus::Canceled => Status::Canceled,
                    LiveOrderStatus::Expired => Status::Expired,
                    LiveOrderStatus::PartiallyFilled => Status::PartiallyFilled,
                    LiveOrderStatus::Filled => Status::Filled,
                    LiveOrderStatus::Unknown(_) => Status::Unsupported,
                };
                let time_in_force = match time_in_force {
                    0 => TimeInForce::GTC,
                    1 => TimeInForce::GTX,
                    2 => TimeInForce::FOK,
                    3 => TimeInForce::IOC,
                    _ => TimeInForce::Unsupported,
                };
                let order_type = match order_type {
                    0 => OrdType::Limit,
                    1 => OrdType::Market,
                    _ => OrdType::Unsupported,
                };
                let request = match request {
                    0 => Status::None,
                    1 => Status::New,
                    4 => Status::Canceled,
                    _ => Status::Unsupported,
                };
                let account_delta = account_delta.map(|delta| AccountDelta {
                    instrument_id: InstrumentId(delta.instrument_id),
                    position_delta: delta.position_delta,
                    trade_qty: delta.trade_qty,
                    trade_value: delta.trade_value,
                    currency: CurrencyId(delta.currency),
                    cash_delta: delta.cash_delta,
                    fee: delta.fee,
                    funding: delta.funding,
                    execution_price: delta.execution_price,
                    realized_pnl: delta.realized_pnl,
                });
                if self.runtime_capture_enabled
                    && let Some(report) = self
                        .runtime_execution_adapter
                        .normalize(LiveExecutionEvent {
                            event_id,
                            venue_id: VenueId(venue_no),
                            instrument_id: InstrumentId(instrument_id),
                            asset_no,
                            order_id,
                            venue_order_id,
                            exchange_ts,
                            delivery_ts,
                            sequence,
                            status,
                            reason: crate::backtest::execution::execution_reason_from_code(reason),
                            side,
                            order_price,
                            order_qty,
                            exec_price,
                            exec_qty,
                            cumulative_filled_qty: order_qty - leaves_qty,
                            maker,
                            account_delta,
                        })
                        .map_err(|error| BotError::Custom(error.to_string()))?
                {
                    self.runtime_projected_events.extend(
                        self.runtime_projector
                            .project(
                                report,
                                self.runtime_local_portfolio
                                    .venue_mut_or_insert(report.venue_id),
                            )
                            .map_err(|error| BotError::Custom(error.to_string()))?
                            .iter()
                            .copied()
                            .map(|event| (inst_no, event)),
                    );
                }
                let instrument = unsafe { self.instruments.get_unchecked_mut(inst_no) };
                let tick_size = instrument.tick_size;
                let order = Order {
                    qty: order_qty,
                    leaves_qty,
                    exec_qty,
                    exec_price_tick: (exec_price / tick_size).round() as i64,
                    price_tick: (order_price / tick_size).round() as i64,
                    tick_size,
                    exch_timestamp: exchange_ts,
                    local_timestamp: local_submit_ts,
                    order_id,
                    q: Box::new(()),
                    maker,
                    order_type,
                    req: request,
                    status: legacy_status,
                    side,
                    time_in_force,
                };
                instrument.last_order_latency = Some((local_submit_ts, exchange_ts, delivery_ts));
                match instrument.orders.entry(order_id) {
                    Entry::Occupied(mut entry) => {
                        let existing = entry.get_mut();
                        if let Some(hook) = self.order_hook.as_mut() {
                            hook(existing, &order)?;
                        }
                        if exchange_ts >= existing.exch_timestamp
                            && !matches!(
                                existing.status,
                                Status::Canceled
                                    | Status::Expired
                                    | Status::Filled
                                    | Status::Rejected
                            )
                        {
                            existing.update(&order);
                        }
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(order);
                    }
                }
                return Ok(ElapseResult::OrderResponse);
            }
            LiveEvent::Position { qty, .. } => {
                unsafe { self.instruments.get_unchecked_mut(inst_no) }
                    .state
                    .position = qty;
            }
            LiveEvent::Funding {
                funding_rate,
                next_funding_time,
                ..
            } => {
                debug!(
                    %inst_no,
                    funding_rate,
                    next_funding_time,
                    "Event::Funding"
                );
            }
            LiveEvent::FundingSettlement {
                event_id,
                amount,
                position_qty,
                funding_rate,
                mark_price,
                currency,
                exch_ts,
                ..
            } => {
                if self.runtime_capture_enabled {
                    let instrument_id = InstrumentId(inst_no as u32 + 1);
                    let event = FundingEvent {
                        event_id,
                        venue_id: VenueId(0),
                        instrument_id,
                        currency: CurrencyId(currency),
                        publication_ts: exch_ts,
                        effective_ts: exch_ts,
                        settlement_ts: exch_ts,
                        rate: funding_rate,
                        price_source: crate::backtest::execution::FundingPriceSource::Mark,
                        mark_price,
                        boundary: FundingBoundary::BeforeSettlementEvents,
                    };
                    let delta = AccountDelta {
                        instrument_id,
                        position_delta: 0.0,
                        trade_qty: 0.0,
                        trade_value: 0.0,
                        currency: CurrencyId(currency),
                        cash_delta: 0.0,
                        fee: 0.0,
                        funding: amount,
                        execution_price: 0.0,
                        realized_pnl: 0.0,
                    };
                    let report = FundingReport {
                        event,
                        delivery_ts: exch_ts,
                        sequence: event_id,
                        position_qty,
                        amount,
                        account_report: AccountReport {
                            venue_id: VenueId(0),
                            exchange_ts: exch_ts,
                            delivery_ts: exch_ts,
                            sequence: event_id,
                            delta,
                        },
                    };
                    if let Some(report) = self
                        .runtime_execution_adapter
                        .normalize_funding((1_u128 << 127) | u128::from(event_id), report)
                        .map_err(|error| BotError::Custom(error.to_string()))?
                    {
                        self.runtime_projector
                            .project_funding(
                                report,
                                self.runtime_local_portfolio
                                    .venue_mut_or_insert(report.event.venue_id),
                            )
                            .map_err(|error| BotError::Custom(error.to_string()))?;
                        self.runtime_funding_events.push((
                            inst_no,
                            RuntimeFunding {
                                event_id,
                                asset_no: inst_no as u32,
                                venue_no: 0,
                                instrument_id: instrument_id.0,
                                currency,
                                price_source: 0,
                                position_snapshot: 0,
                                formula: 0,
                                rounding_mode: 0,
                                boundary: 0,
                                publication_ts: exch_ts,
                                effective_ts: exch_ts,
                                settlement_ts: exch_ts,
                                delivery_ts: exch_ts,
                                rate: funding_rate,
                                mark_price,
                                position_qty,
                                amount,
                                rounding_increment: 1e-12,
                            },
                        ));
                    }
                }
                if WAIT_NEXT_FEED {
                    return Ok(ElapseResult::MarketFeed);
                }
            }
            LiveEvent::Error(error) => {
                if let Some(handler) = self.error_handler.as_mut() {
                    handler(error)?;
                }
            }
            LiveEvent::BatchStart | LiveEvent::BatchEnd => {
                unreachable!();
            }
        }
        Ok(ElapseResult::Ok)
    }

    fn elapse_<const WAIT_NEXT_FEED: bool>(
        &mut self,
        duration: i64,
        wait_order_response: WaitOrderResponse,
    ) -> Result<ElapseResult, BotError> {
        let instant = Instant::now();
        let duration = Duration::from_nanos(duration as u64);
        let mut remaining_duration = duration;
        let mut batch_mode = false;
        let mut wait_resp_received = false;

        loop {
            match self.channel.recv_timeout(self.id, remaining_duration) {
                Ok((_, LiveEvent::BatchStart)) => {
                    batch_mode = true;
                }
                Ok((_, LiveEvent::BatchEnd)) => {
                    batch_mode = false;
                    // If batch event processing ends and the waiting response has already been
                    // received, return immediately without checking the elapsed time.
                    if wait_resp_received {
                        return Ok(ElapseResult::Ok);
                    }
                }
                Ok((inst_no, ev)) => {
                    match self.process_event::<WAIT_NEXT_FEED>(inst_no, ev, wait_order_response)? {
                        ElapseResult::Ok => {
                            // Keeps receiving events until the elapsed time is reached.
                        }
                        ElapseResult::EndOfData => {
                            unreachable!()
                        }
                        ElapseResult::MarketFeed => {
                            wait_resp_received = true;
                            if !batch_mode {
                                return Ok(ElapseResult::MarketFeed);
                            }
                        }
                        ElapseResult::OrderResponse => {
                            wait_resp_received = true;
                            if !batch_mode {
                                return Ok(ElapseResult::OrderResponse);
                            }
                        }
                    }
                }
                Err(BotError::Timeout) => {
                    return Ok(ElapseResult::Ok);
                }
                Err(BotError::Interrupted) => {
                    return Ok(ElapseResult::EndOfData);
                }
                Err(error) => {
                    return Err(error);
                }
            }

            let elapsed = instant.elapsed();
            // While processing events in batch mode, all events in a batch should be processed
            // together without interruption.
            if !batch_mode && elapsed > duration {
                return Ok(ElapseResult::Ok);
            }
            remaining_duration = duration
                .saturating_sub(elapsed)
                .max(Duration::from_micros(1));
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn submit_order(
        &mut self,
        asset_no: usize,
        order_id: u64,
        price: f64,
        qty: f64,
        time_in_force: TimeInForce,
        order_type: OrdType,
        wait: bool,
        side: Side,
    ) -> Result<ElapseResult, BotError> {
        let instrument = self
            .instruments
            .get_mut(asset_no)
            .ok_or(BotError::InstrumentNotFound)?;
        if instrument.orders.contains_key(&order_id) {
            return Err(BotError::OrderIdExist);
        }
        let symbol = instrument.symbol.clone();
        let tick_size = instrument.tick_size;
        let order = Order {
            order_id,
            price_tick: (price / tick_size).round() as i64,
            qty,
            leaves_qty: qty,
            tick_size,
            side,
            time_in_force,
            order_type,
            status: Status::New,
            local_timestamp: Utc::now().timestamp_nanos_opt().unwrap(),
            req: Status::New,
            exec_price_tick: 0,
            exch_timestamp: 0,
            exec_qty: 0.0,
            // Invalid information
            q: Box::new(()),
            maker: false,
        };
        let order_id = order.order_id;
        instrument.orders.insert(order_id, order.clone());

        self.channel
            .send(self.id, asset_no, LiveRequest::Order { symbol, order })?;

        if wait {
            // fixme: timeout should be specified by the argument.
            return self.wait_order_response(asset_no, order_id, 60_000_000_000);
        }
        Ok(ElapseResult::Ok)
    }
}

impl<CH, MD> Bot<MD> for LiveBot<CH, MD>
where
    CH: Channel,
    MD: MarketDepth + L2MarketDepth,
{
    type Error = BotError;

    #[inline]
    fn current_timestamp(&self) -> i64 {
        Utc::now().timestamp_nanos_opt().unwrap()
    }

    #[inline]
    fn num_assets(&self) -> usize {
        self.instruments.len()
    }

    #[inline]
    fn position(&self, asset_no: usize) -> f64 {
        self.state_values(asset_no).position
    }

    #[inline]
    fn state_values(&self, asset_no: usize) -> &StateValues {
        // todo: implement the missing fields. Trade values need to be changed to a rolling manner,
        //       unlike the current Python implementation, to support live trading.
        &self.instruments.get(asset_no).unwrap().state
    }

    #[inline]
    fn depth(&self, asset_no: usize) -> &MD {
        &self.instruments.get(asset_no).unwrap().depth
    }

    #[inline]
    fn last_trades(&self, asset_no: usize) -> &[Event] {
        self.instruments
            .get(asset_no)
            .unwrap()
            .last_trades
            .as_slice()
    }

    fn clear_last_trades(&mut self, asset_no: Option<usize>) {
        match asset_no {
            Some(asset_no) => {
                self.instruments
                    .get_mut(asset_no)
                    .unwrap()
                    .last_trades
                    .clear();
            }
            None => {
                for asset_no in 0..self.instruments.len() {
                    self.instruments
                        .get_mut(asset_no)
                        .unwrap()
                        .last_trades
                        .clear();
                }
            }
        }
    }

    #[inline]
    fn orders(&self, asset_no: usize) -> &HashMap<OrderId, Order> {
        &self.instruments.get(asset_no).unwrap().orders
    }

    #[inline]
    fn submit_buy_order(
        &mut self,
        asset_no: usize,
        order_id: OrderId,
        price: f64,
        qty: f64,
        time_in_force: TimeInForce,
        order_type: OrdType,
        wait: bool,
    ) -> Result<ElapseResult, Self::Error> {
        self.submit_order(
            asset_no,
            order_id,
            price,
            qty,
            time_in_force,
            order_type,
            wait,
            Side::Buy,
        )
    }

    #[inline]
    fn submit_sell_order(
        &mut self,
        asset_no: usize,
        order_id: OrderId,
        price: f64,
        qty: f64,
        time_in_force: TimeInForce,
        order_type: OrdType,
        wait: bool,
    ) -> Result<ElapseResult, Self::Error> {
        self.submit_order(
            asset_no,
            order_id,
            price,
            qty,
            time_in_force,
            order_type,
            wait,
            Side::Sell,
        )
    }

    fn submit_order(
        &mut self,
        asset_no: usize,
        order: OrderRequest,
        wait: bool,
    ) -> Result<ElapseResult, Self::Error> {
        self.submit_order(
            asset_no,
            order.order_id,
            order.price,
            order.qty,
            order.time_in_force,
            order.order_type,
            wait,
            order.side,
        )
    }

    #[inline]
    fn modify(
        &mut self,
        _asset_no: usize,
        _order_id: OrderId,
        _price: f64,
        _qty: f64,
        _wait: bool,
    ) -> Result<ElapseResult, Self::Error> {
        Err(BotError::UnsupportedOperation(
            "live order modification is disabled; cancel and submit a replacement order",
        ))
    }

    #[inline]
    fn cancel(
        &mut self,
        asset_no: usize,
        order_id: OrderId,
        wait: bool,
    ) -> Result<ElapseResult, Self::Error> {
        let instrument = self
            .instruments
            .get_mut(asset_no)
            .ok_or(BotError::InstrumentNotFound)?;
        let symbol = instrument.symbol.clone();
        let order = instrument
            .orders
            .get_mut(&order_id)
            .ok_or(BotError::OrderNotFound)?;
        if !order.cancellable() {
            return Err(BotError::InvalidOrderStatus);
        }
        order.req = Status::Canceled;
        order.local_timestamp = Utc::now().timestamp_nanos_opt().unwrap();

        self.channel.send(
            self.id,
            asset_no,
            LiveRequest::Order {
                symbol,
                order: order.clone(),
            },
        )?;

        if wait {
            // fixme: timeout should be specified by the argument.
            return self.wait_order_response(asset_no, order_id, 60_000_000_000);
        }
        Ok(ElapseResult::Ok)
    }

    #[inline]
    fn clear_inactive_orders(&mut self, asset_no: Option<usize>) {
        match asset_no {
            Some(inst_no) => {
                if let Some(instrument) = self.instruments.get_mut(inst_no) {
                    instrument.orders.retain(|_, order| order.active());
                }
            }
            None => {
                for instrument in self.instruments.iter_mut() {
                    instrument.orders.retain(|_, order| order.active());
                }
            }
        }
    }

    #[inline]
    fn wait_order_response(
        &mut self,
        asset_no: usize,
        order_id: OrderId,
        timeout: i64,
    ) -> Result<ElapseResult, Self::Error> {
        self.elapse_::<false>(timeout, WaitOrderResponse::Specified { asset_no, order_id })
    }

    #[inline]
    fn wait_next_feed(
        &mut self,
        include_order_resp: bool,
        timeout: i64,
    ) -> Result<ElapseResult, Self::Error> {
        if include_order_resp {
            self.elapse_::<true>(timeout, WaitOrderResponse::Any)
        } else {
            self.elapse_::<true>(timeout, WaitOrderResponse::None)
        }
    }

    #[inline]
    fn elapse(&mut self, duration: i64) -> Result<ElapseResult, Self::Error> {
        self.elapse_::<false>(duration, WaitOrderResponse::None)
    }

    #[inline]
    fn elapse_bt(&mut self, _duration: i64) -> Result<ElapseResult, Self::Error> {
        Ok(ElapseResult::Ok)
    }

    fn close(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn feed_latency(&self, asset_no: usize) -> Option<(i64, i64)> {
        self.instruments.get(asset_no).unwrap().last_feed_latency
    }

    fn order_latency(&self, asset_no: usize) -> Option<(i64, i64, i64)> {
        self.instruments.get(asset_no).unwrap().last_order_latency
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{depth::HashMapMarketDepth, live::ipc::Channel, types::LiveAccountDelta};
    use std::collections::VecDeque;

    struct TestChannel;

    struct FundingChannel(Option<LiveEvent>);

    struct CanonicalExecutionChannel(VecDeque<LiveEvent>);

    impl Channel for TestChannel {
        fn build<MD>(_instruments: &[Instrument<MD>]) -> Result<Self, BuildError> {
            Ok(Self)
        }

        fn recv_timeout(
            &mut self,
            _id: u64,
            _timeout: Duration,
        ) -> Result<(usize, LiveEvent), BotError> {
            Err(BotError::Timeout)
        }

        fn send(
            &mut self,
            _id: u64,
            _inst_no: usize,
            _request: LiveRequest,
        ) -> Result<(), BotError> {
            Ok(())
        }
    }

    impl Channel for FundingChannel {
        fn build<MD>(_instruments: &[Instrument<MD>]) -> Result<Self, BuildError> {
            Ok(Self(Some(LiveEvent::FundingSettlement {
                symbol: "BTC".into(),
                event_id: 7,
                amount: -0.25,
                position_qty: 2.0,
                funding_rate: 0.001,
                mark_price: 125.0,
                currency: 1,
                exch_ts: 100,
            })))
        }

        fn recv_timeout(
            &mut self,
            _id: u64,
            _timeout: Duration,
        ) -> Result<(usize, LiveEvent), BotError> {
            self.0
                .take()
                .map(|event| (0, event))
                .ok_or(BotError::Timeout)
        }

        fn send(
            &mut self,
            _id: u64,
            _inst_no: usize,
            _request: LiveRequest,
        ) -> Result<(), BotError> {
            Ok(())
        }
    }

    impl Channel for CanonicalExecutionChannel {
        fn build<MD>(_instruments: &[Instrument<MD>]) -> Result<Self, BuildError> {
            let event = LiveEvent::ExecutionReport {
                symbol: "BTC".into(),
                event_id: 44,
                venue_no: 3,
                instrument_id: 8,
                asset_no: 0,
                order_id: 9,
                venue_order_id: 99,
                exchange_ts: 100,
                delivery_ts: 120,
                sequence: 7,
                status: 5,
                reason: 0,
                side: 1,
                order_price: 100.0,
                order_qty: 2.0,
                leaves_qty: 1.0,
                exec_price: 100.0,
                exec_qty: 1.0,
                maker: false,
                local_submit_ts: 90,
                time_in_force: 0,
                order_type: 0,
                request: 1,
                account_delta: Some(LiveAccountDelta {
                    instrument_id: 8,
                    position_delta: 1.0,
                    trade_qty: 1.0,
                    trade_value: 100.0,
                    currency: 1,
                    cash_delta: -100.0,
                    fee: 0.1,
                    funding: 0.0,
                    execution_price: 100.0,
                    realized_pnl: 0.0,
                }),
            };
            Ok(Self(VecDeque::from([event.clone(), event])))
        }

        fn recv_timeout(
            &mut self,
            _id: u64,
            _timeout: Duration,
        ) -> Result<(usize, LiveEvent), BotError> {
            self.0
                .pop_front()
                .map(|event| (0, event))
                .ok_or(BotError::Timeout)
        }

        fn send(
            &mut self,
            _id: u64,
            _inst_no: usize,
            _request: LiveRequest,
        ) -> Result<(), BotError> {
            Ok(())
        }
    }

    #[test]
    fn live_modify_is_explicitly_disabled() {
        let mut bot = LiveBotBuilder::new()
            .register(Instrument::new(
                "test",
                "BTC",
                0.1,
                0.001,
                HashMapMarketDepth::new(0.1, 0.001),
                16,
            ))
            .build::<TestChannel>()
            .unwrap();

        let error = bot.modify(0, 1, 100.0, 1.0, false).unwrap_err();
        assert!(matches!(error, BotError::UnsupportedOperation(_)));
    }

    #[test]
    fn live_feed_batch_is_applied_and_captured_as_one_boundary() {
        let mut bot = LiveBotBuilder::new()
            .register(Instrument::new(
                "test",
                "BTC",
                0.1,
                0.001,
                HashMapMarketDepth::new(0.1, 0.001),
                0,
            ))
            .build::<TestChannel>()
            .unwrap();
        bot.set_runtime_capture(true);
        let bid = Event {
            ev: LOCAL_BID_DEPTH_EVENT,
            exch_ts: 10,
            local_ts: 20,
            px: 100.0,
            qty: 1.0,
            order_id: 0,
            ival: 0,
            fval: 0.0,
        };
        let ask = Event {
            ev: LOCAL_ASK_DEPTH_EVENT,
            px: 101.0,
            ..bid.clone()
        };

        assert_eq!(
            bot.process_event::<true>(
                0,
                LiveEvent::FeedBatch {
                    instrument_id: 42,
                    events: vec![bid.clone(), ask.clone()],
                },
                WaitOrderResponse::None,
            )
            .unwrap(),
            ElapseResult::MarketFeed
        );
        assert_eq!(bot.runtime_feed_events(), &[(0, bid), (0, ask)]);
        assert_eq!(bot.feed_latency(0), Some((10, 20)));
    }

    #[test]
    fn canonical_live_execution_uses_connector_event_id_for_reconnect_deduplication() {
        let mut bot = LiveBotBuilder::new()
            .register(Instrument::new(
                "connector_name",
                "BTC",
                0.1,
                0.001,
                HashMapMarketDepth::new(0.1, 0.001),
                0,
            ))
            .build::<CanonicalExecutionChannel>()
            .unwrap();
        bot.set_runtime_capture(true);
        assert_eq!(
            bot.wait_next_feed(true, 1_000_000).unwrap(),
            ElapseResult::OrderResponse
        );
        assert_eq!(
            bot.wait_next_feed(true, 1_000_000).unwrap(),
            ElapseResult::OrderResponse
        );
        let mut projected = Vec::new();
        bot.drain_runtime_projected_events(&mut projected);
        assert_eq!(projected.len(), 3);
        assert_eq!(projected[0].1.report.venue_order_id, 99);
        assert_eq!(projected[1].1.report.sequence, 7);
        assert_eq!(projected[2].1.visible_position, 1.0);
        assert_eq!(
            bot.orders(0).get(&9).unwrap().status,
            Status::PartiallyFilled
        );
    }

    #[test]
    fn live_funding_settlement_is_captured_for_the_unified_runtime() {
        let mut bot = LiveBotBuilder::new()
            .register(Instrument::new(
                "test",
                "BTC",
                0.1,
                0.001,
                HashMapMarketDepth::new(0.1, 0.001),
                16,
            ))
            .build::<FundingChannel>()
            .unwrap();
        bot.set_runtime_capture(true);
        assert_eq!(
            bot.wait_next_feed(true, 1_000).unwrap(),
            ElapseResult::MarketFeed
        );
        let event = bot.runtime_funding_events()[0].1;
        assert_eq!((event.event_id, event.delivery_ts), (7, 100));
        assert_eq!((event.amount, event.position_qty), (-0.25, 2.0));
    }
}
