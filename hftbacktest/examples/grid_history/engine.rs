use super::data::{self, BUY_TRADE, FUNDING, MarketEvent, QUOTE, SELL_TRADE};
use anyhow::{Context, Result, bail, ensure};
use hftbacktest::{
    backtest::execution::*,
    types::{OrdType, Side, Status, TimeInForce},
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    fs::{self, File},
    io::{BufWriter, Write},
    path::Path,
    sync::Arc,
};
use titan_strategy_runtime::*;

const TICK: f64 = 0.1;
const LOT: f64 = 0.001;
const MONEY: f64 = TICK * LOT;
const MS: i64 = 1_000_000;
const SECOND: i64 = 1_000_000_000;
const INSTRUMENT: InstrumentId = InstrumentId(0);
const CURRENCY: CurrencyId = CurrencyId(0);
const VENUE: VenueId = VenueId(0);

struct Layout(Value);
impl Layout {
    fn new(path: &Path, compiled_strategy: &LoadedNativeStrategyV13) -> Result<Self> {
        let value: Value = serde_json::from_slice(&fs::read(path)?)?;
        ensure!(
            value["state_len"].as_u64() == Some(compiled_strategy.manifest.state.byte_len as u64),
            "layout size differs from compiled_strategy"
        );
        ensure!(
            value["schema_version"].as_u64()
                == Some(u64::from(compiled_strategy.manifest.state.version)),
            "layout version differs from compiled_strategy"
        );
        ensure!(
            value["schema_hash"].as_str()
                == Some(hex(&compiled_strategy.manifest.state.hash).as_str()),
            "layout hash differs from compiled_strategy"
        );
        for field in value["fields"]
            .as_object()
            .context("layout fields")?
            .values()
        {
            let offset = field["offset"].as_u64().context("field offset")?;
            let size = field["size"].as_u64().context("field size")?;
            ensure!(
                size > 0
                    && size <= 8
                    && offset + size <= compiled_strategy.manifest.state.byte_len as u64,
                "layout field outside state"
            );
        }
        Ok(Self(value))
    }
    fn bytes<'a>(&self, state: &'a [u8], name: &str) -> &'a [u8] {
        let field = &self.0["fields"][name];
        let offset = field["offset"].as_u64().expect("known field") as usize;
        let size = field["size"].as_u64().expect("known size") as usize;
        &state[offset..offset + size]
    }
    fn int(&self, state: &[u8], name: &str) -> i64 {
        let raw = self.bytes(state, name);
        let mut bytes = [0; 8];
        bytes[..raw.len()].copy_from_slice(raw);
        i64::from_le_bytes(bytes)
    }
    fn float(&self, state: &[u8], name: &str) -> f64 {
        f64::from_le_bytes(self.bytes(state, name).try_into().expect("f64 field"))
    }
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[derive(Clone, Copy)]
struct Model {
    latency: i64,
    participation: f64,
    maker: f64,
    taker: f64,
    ioc_impact_ticks: i64,
}
impl Model {
    fn new(stress: bool) -> Self {
        if stress {
            Self {
                latency: 500 * MS,
                participation: 0.01,
                maker: 0.0005,
                taker: 0.001,
                ioc_impact_ticks: 5,
            }
        } else {
            Self {
                latency: 50 * MS,
                participation: 0.1,
                maker: 0.0002,
                taker: 0.0005,
                ioc_impact_ticks: 1,
            }
        }
    }
}

enum Due {
    Arrival(StagedCommandV13),
    Report(ExecutionReport, u64, Option<TitanPositionView>),
    Fill(TitanFillView),
    Position(TitanPositionView),
    Cancel(TitanCancelEventView),
    Funding(AccountDelta),
}
#[derive(Default)]
struct Metrics {
    peak_equity: f64,
    max_drawdown: f64,
    max_drawdown_pct: f64,
    max_position_lots: i64,
    max_position_notional: f64,
    max_reserved_notional: f64,
    max_gross_order_notional: f64,
    fills: u64,
    partial_fills: u64,
    submits: u64,
    cancels: u64,
    cancel_lost_race: u64,
    rejects: u64,
}
impl Metrics {
    fn equity(&mut self, equity: f64) {
        self.peak_equity = self.peak_equity.max(equity);
        let drawdown = self.peak_equity - equity;
        self.max_drawdown = self.max_drawdown.max(drawdown);
        self.max_drawdown_pct = self
            .max_drawdown_pct
            .max(drawdown / self.peak_equity * 100.0);
    }
}

struct Replay {
    adapter: OfflineV13Adapter,
    compiled_strategy: Arc<LoadedNativeStrategyV13>,
    layout: Layout,
    coordinator: ExecutionCoordinator<RateFeeModel>,
    exchange: ExchangeAccountState,
    local: VenueAccount,
    local_position: TitanPositionView,
    local_orders: BTreeMap<u64, TitanActiveOrderView>,
    exchange_orders: BTreeMap<u64, TitanSubmitOrderRequest>,
    due: BTreeMap<(i64, u64), Due>,
    serial: u64,
    version: u64,
    public_version: u64,
    market: TitanMarketView,
    quote_ts: i64,
    last_tick_callback_ns: i64,
    now: i64,
    next_timer: i64,
    model: Model,
    initial_capital: f64,
    leverage: f64,
    metrics: Metrics,
    journal: VecDeque<String>,
    fills: BufWriter<File>,
    equity: BufWriter<File>,
    last_sample: i64,
    stop_started: bool,
    stopped_ns: Option<i64>,
    business_stopped_ns: Option<i64>,
    callbacks: u64,
}

impl Replay {
    fn new(
        compiled_strategy: Arc<LoadedNativeStrategyV13>,
        layout: Layout,
        output: &Path,
        model: Model,
    ) -> Result<Self> {
        let state = compiled_strategy.initial_state.as_bytes();
        let capital = layout.int(state, "config.allocated_capital_tick_lots") as f64 * MONEY;
        let leverage = layout.int(state, "config.leverage") as f64;
        ensure!(capital > 0.0 && leverage > 0.0, "invalid capital/leverage");
        ensure!(
            (layout.float(state, "config.quote_units_per_tick_lot") - 1.0).abs() < f64::EPSILON,
            "history runner uses one balance unit = one tick-lot"
        );
        for name in [
            "config.account_no",
            "config.asset_no",
            "config.quote_currency_no",
        ] {
            ensure!(
                layout.int(state, name) == 0,
                "history fixture uses binding 0"
            );
        }
        let account = TitanAccountView {
            account_no: 0,
            state: 4,
            account_epoch: 1,
            account_sequence: 1,
            ..Default::default()
        };
        let local_position = TitanPositionView {
            account_sequence: 1,
            ..Default::default()
        };
        let mut exchange = ExchangeAccountState::new(VENUE);
        exchange.seed_balance(CURRENCY, capital)?;
        let mut local = VenueAccount::new(VENUE);
        local.set_balance(CURRENCY, capital)?;
        let adapter = OfflineV13Adapter::new(OfflineV13Config {
            compiled_strategy: compiled_strategy.clone(),
            strategy_instance_id: 1,
            generation: 1,
            command_capacity: 256,
            markets: vec![TitanMarketView::default()],
            positions: vec![local_position],
            balances: vec![TitanBalanceView {
                total_units: (capital / MONEY).round() as i64,
                available_units: (capital / MONEY).round() as i64,
                ..Default::default()
            }],
            accounts: vec![account],
            active_orders: vec![],
        })?;
        let mut fills = BufWriter::new(File::create(output.join("fills.csv"))?);
        writeln!(
            fills,
            "exchange_ns,delivery_ns,order_id,side,price,qty,maker,fee_usdt,position_btc"
        )?;
        let mut equity = BufWriter::new(File::create(output.join("equity.csv"))?);
        writeln!(
            equity,
            "time_ns,mid_usdt,equity_usdt,position_btc,position_notional_usdt,reserved_notional_usdt"
        )?;
        Ok(Self {
            adapter,
            compiled_strategy,
            layout,
            coordinator: ExecutionCoordinator::new(
                VENUE,
                RateFeeModel {
                    maker_rate: model.maker,
                    taker_rate: model.taker,
                    currency: CURRENCY,
                },
            ),
            exchange,
            local,
            local_position,
            local_orders: BTreeMap::new(),
            exchange_orders: BTreeMap::new(),
            due: BTreeMap::new(),
            serial: 0,
            version: 1,
            public_version: 1,
            market: TitanMarketView::default(),
            quote_ts: 0,
            last_tick_callback_ns: 0,
            now: 0,
            next_timer: 0,
            model,
            initial_capital: capital,
            leverage,
            metrics: Metrics {
                peak_equity: capital,
                ..Default::default()
            },
            journal: VecDeque::new(),
            fills,
            equity,
            last_sample: 0,
            stop_started: false,
            stopped_ns: None,
            business_stopped_ns: None,
            callbacks: 0,
        })
    }
    fn int(&self, name: &str) -> i64 {
        self.layout.int(self.adapter.state_bytes(), name)
    }
    fn float(&self, name: &str) -> f64 {
        self.layout.float(self.adapter.state_bytes(), name)
    }
    fn note(&mut self, message: String) {
        if self.journal.len() == 128 {
            self.journal.pop_front();
        }
        self.journal.push_back(format!("{} {message}", self.now));
    }
    fn schedule(&mut self, time: i64, event: Due) {
        self.serial += 1;
        self.due.insert((time, self.serial), event);
    }
    fn mid(&self) -> f64 {
        (self.market.best_bid_ticks + self.market.best_ask_ticks) as f64 * TICK / 2.0
    }
    fn pos_lots(&self) -> i64 {
        (self.exchange.account().position(INSTRUMENT).qty / LOT).round() as i64
    }
    fn equity_value(&self) -> f64 {
        let position = self.exchange.account().position(INSTRUMENT);
        self.exchange.account().balance(CURRENCY)
            + position.qty * (self.mid() - position.avg_entry_price)
    }
    fn project(&mut self) {
        let position = self.local.position(INSTRUMENT);
        let equity =
            self.local.balance(CURRENCY) + position.qty * (self.mid() - position.avg_entry_price);
        let frozen: f64 = self
            .local_orders
            .values()
            .filter(|order| order.reduce_only == 0)
            .map(|order| {
                (order.qty_lots - order.cumulative_filled_lots) as f64
                    * order.price_ticks as f64
                    * MONEY
                    / self.leverage
            })
            .sum();
        self.adapter.update_public_state(
            vec![self.market],
            vec![self.local_position],
            vec![TitanBalanceView {
                total_units: (self.local.balance(CURRENCY) / MONEY).floor() as i64,
                available_units: ((equity
                    - position.qty.abs() * self.mid() / self.leverage
                    - frozen)
                    / MONEY)
                    .floor() as i64,
                account_sequence: self.public_version,
                ..Default::default()
            }],
            vec![TitanAccountView {
                state: 4,
                account_epoch: 1,
                account_sequence: self.public_version,
                ..Default::default()
            }],
            self.local_orders.values().copied().collect(),
        );
    }
    fn commands(&mut self, commands: Vec<StagedCommandV13>, callback: &str) -> Result<()> {
        self.callbacks += 1;
        if !matches!(callback, "bbo" | "timer")
            || !commands.is_empty()
            || matches!(self.int("grid.status"), 9 | 10)
        {
            self.note(format!(
                "callback={callback} status={} error={} expected={} commands={}",
                self.int("grid.status"),
                self.int("grid.last_error"),
                self.int("grid.expected_position_lots"),
                commands.len()
            ));
        }
        ensure!(
            !matches!(self.int("grid.status"), 9 | 10),
            "strategy failed in {callback}: status={} error={}",
            self.int("grid.status"),
            self.int("grid.last_error")
        );
        for command in commands {
            match command {
                StagedCommandV13::Submit { order_id, request } => {
                    ensure!(
                        !self.local_orders.contains_key(&order_id),
                        "duplicate local order ID"
                    );
                    self.local_orders.insert(
                        order_id,
                        TitanActiveOrderView {
                            order_id,
                            asset_no: request.asset_no,
                            account_no: request.account_no,
                            price_ticks: request.price_ticks,
                            qty_lots: request.qty_lots,
                            side: request.side,
                            order_type: request.order_type,
                            time_in_force: request.time_in_force,
                            reduce_only: request.reduce_only,
                            position_side: request.position_side,
                            status: 1,
                            created_ts_ns: self.now,
                            updated_ts_ns: self.now,
                            ..Default::default()
                        },
                    );
                    self.metrics.submits += 1;
                    self.note(format!(
                        "submit {order_id} side={} price={} qty={} reduce={} tif={}",
                        request.side,
                        request.price_ticks,
                        request.qty_lots,
                        request.reduce_only,
                        request.time_in_force
                    ));
                }
                StagedCommandV13::Cancel { request, .. } => {
                    if let Some(order) = self.local_orders.get_mut(&request.order_id) {
                        order.status = 5;
                        order.updated_ts_ns = self.now;
                    }
                    self.metrics.cancels += 1;
                    self.note(format!("cancel {}", request.order_id));
                }
            }
            self.schedule(self.now + self.model.latency, Due::Arrival(command));
        }
        if self.adapter.is_stopped() && self.stopped_ns.is_none() {
            self.stopped_ns = Some(self.now);
        }
        if self.int("grid.status") == 8 && self.business_stopped_ns.is_none() {
            self.business_stopped_ns = Some(self.now);
        }
        Ok(())
    }
    fn report(&mut self, report: ExecutionReport) {
        self.version += 1;
        let position = (report.kind == ExecutionReportKind::Fill).then(|| {
            let position = self.exchange.account().position(INSTRUMENT);
            TitanPositionView {
                qty_lots: (position.qty / LOT).round() as i64,
                average_price_ticks: (position.avg_entry_price / TICK).round() as i64,
                realized_pnl_ticks: (position.realized_pnl / MONEY).round() as i64,
                account_sequence: self.version,
                ..Default::default()
            }
        });
        self.schedule(
            self.now + self.model.latency,
            Due::Report(report, self.version, position),
        );
    }
    fn spec(&self) -> InstrumentSpec {
        InstrumentSpec {
            instrument_id: INSTRUMENT,
            asset_no: 0,
            venue_id: VENUE,
            tick_size: TICK,
            lot_size: LOT,
            min_qty: LOT,
            max_qty: 1000.0,
            min_notional: 5.0,
            contract_size: 1.0,
            price_currency: CURRENCY,
            settlement_currency: CURRENCY,
            margin_currency: CURRENCY,
            instrument_type: InstrumentType::LinearPerpetual,
            cash_flow_mode: CashFlowMode::DerivativePnl,
            version: 1,
        }
    }
    fn fill(&mut self, order_id: u64, qty: i64, price: i64, maker: bool) -> Result<()> {
        if qty == 0 {
            return Ok(());
        }
        let request = self.exchange_orders[&order_id];
        if request.reduce_only != 0 {
            ensure!(
                reducing_lots(self.pos_lots(), request.side) >= qty,
                "reduce-only fill would reverse position"
            );
        }
        let report = self.coordinator.fill(
            order_id,
            ProposedFill {
                exchange_ts: self.now,
                price: price as f64 * TICK,
                qty: qty as f64 * LOT,
                maker,
            },
            self.now + self.model.latency,
            &self.spec(),
            &mut self.exchange,
        )?;
        self.metrics.fills += 1;
        if report.status != Status::Filled {
            self.metrics.partial_fills += 1;
        }
        writeln!(
            self.fills,
            "{},{},{},{},{:.1},{:.3},{},{:.8},{:.3}",
            self.now,
            self.now + self.model.latency,
            order_id,
            request.side,
            price as f64 * TICK,
            qty as f64 * LOT,
            maker,
            report.account_delta.unwrap().fee,
            self.pos_lots() as f64 * LOT
        )?;
        self.note(format!(
            "fill {order_id} qty={qty} px={price} cum={} maker={maker}",
            report.cumulative_filled_qty / LOT
        ));
        if report.status == Status::Filled {
            self.exchange_orders.remove(&order_id);
        }
        self.report(report);
        self.measure(false)?;
        Ok(())
    }
    fn arrive(&mut self, command: StagedCommandV13) -> Result<()> {
        match command {
            StagedCommandV13::Submit { order_id, request } => {
                let execution = ExecutionOrderRequest {
                    client_order_id: order_id,
                    venue_id: VENUE,
                    instrument_id: INSTRUMENT,
                    price: request.price_ticks as f64 * TICK,
                    qty: request.qty_lots as f64 * LOT,
                    side: if request.side == 1 {
                        Side::Buy
                    } else {
                        Side::Sell
                    },
                    order_type: OrdType::Limit,
                    time_in_force: if request.time_in_force == 4 {
                        TimeInForce::GTX
                    } else {
                        TimeInForce::IOC
                    },
                    reduce_only: request.reduce_only != 0,
                    origin: OrderOrigin::Strategy,
                    local_submit_ts: self.now - self.model.latency,
                };
                self.coordinator.submit(execution, &self.spec())?;
                let crossing = if request.side == 1 {
                    request.price_ticks >= self.market.best_ask_ticks
                } else {
                    request.price_ticks <= self.market.best_bid_ticks
                };
                let stale = self.quote_ts == 0
                    || self.now - self.quote_ts >= self.int("config.market_stale_after_ns");
                let pos = self.pos_lots();
                let reduction = reducing_lots(pos, request.side);
                let frozen: f64 = self
                    .exchange_orders
                    .iter()
                    .filter(|(_, order)| order.reduce_only == 0)
                    .map(|(id, order)| {
                        let leaves = self.coordinator.order(*id).map_or(order.qty_lots, |order| {
                            ((order.request.qty - order.filled_qty) / LOT).round() as i64
                        });
                        leaves as f64 * order.price_ticks as f64 * MONEY / self.leverage
                    })
                    .sum();
                let margin = (pos.abs() as f64 * LOT * self.mid()
                    + request.qty_lots as f64 * request.price_ticks as f64 * MONEY)
                    / self.leverage
                    + frozen;
                let reason = if request.reduce_only != 0 && reduction == 0 {
                    Some(ExecutionReason::ReduceOnlyViolation)
                } else if stale {
                    Some(ExecutionReason::MarketClosed)
                } else if request.time_in_force == 4 && crossing {
                    Some(ExecutionReason::ExchangeRisk)
                } else if request.reduce_only == 0 && margin > self.equity_value() {
                    Some(ExecutionReason::InsufficientMargin)
                } else {
                    None
                };
                if let Some(reason) = reason {
                    let report = self.coordinator.reject(
                        order_id,
                        self.now,
                        self.now + self.model.latency,
                        reason,
                    )?;
                    self.metrics.rejects += 1;
                    self.report(report);
                    return Ok(());
                }
                self.exchange_orders.insert(order_id, request);
                let accepted =
                    self.coordinator
                        .accept(order_id, self.now, self.now + self.model.latency)?;
                self.report(accepted);
                if request.time_in_force == 2 {
                    let price = if request.side == 1 {
                        self.market.best_ask_ticks + self.model.ioc_impact_ticks
                    } else {
                        self.market.best_bid_ticks - self.model.ioc_impact_ticks
                    };
                    let executable = if request.side == 1 {
                        price <= request.price_ticks
                    } else {
                        price >= request.price_ticks
                    };
                    let available = if request.side == 1 {
                        self.market.best_ask_qty_lots
                    } else {
                        self.market.best_bid_qty_lots
                    };
                    let mut qty = request
                        .qty_lots
                        .min((available as f64 * self.model.participation).floor() as i64);
                    if request.reduce_only != 0 {
                        qty = qty.min(reduction);
                    }
                    if executable && qty > 0 {
                        self.fill(order_id, qty, price, false)?;
                    }
                    if self.exchange_orders.remove(&order_id).is_some() {
                        let report = self.coordinator.expire(
                            order_id,
                            self.now,
                            self.now + self.model.latency,
                        )?;
                        self.report(report);
                    }
                }
            }
            StagedCommandV13::Cancel { request, .. } => {
                if self.exchange_orders.remove(&request.order_id).is_some() {
                    self.coordinator.request_cancel(request.order_id)?;
                    let report = self.coordinator.cancel(
                        request.order_id,
                        self.now,
                        self.now + self.model.latency,
                    )?;
                    self.report(report);
                } else {
                    self.metrics.cancel_lost_race += 1;
                    self.schedule(
                        self.now + self.model.latency,
                        Due::Cancel(TitanCancelEventView {
                            order_id: request.order_id,
                            account_no: request.account_no,
                            asset_no: request.asset_no,
                            request_result: 1,
                            event_ts_ns: self.now,
                            ..Default::default()
                        }),
                    );
                }
            }
        }
        Ok(())
    }
    fn deliver(&mut self, event: Due) -> Result<()> {
        match event {
            Due::Arrival(command) => self.arrive(command)?,
            Due::Report(report, version, position) => {
                self.public_version = self.public_version.max(version);
                if let Some(delta) = report.account_delta {
                    self.local.apply(delta)?;
                }
                let status = match report.status {
                    Status::New => 2,
                    Status::PartiallyFilled => 3,
                    Status::Filled => 4,
                    Status::Canceled => 6,
                    Status::Rejected => 7,
                    Status::Expired => 8,
                    _ => bail!("unsupported report status"),
                };
                let mut view = *self
                    .local_orders
                    .get(&report.order_id)
                    .context("missing local intent for report")?;
                view.status = status;
                view.cumulative_filled_lots = (report.cumulative_filled_qty / LOT).round() as i64;
                view.updated_ts_ns = self.now;
                view.account_sequence = version;
                if matches!(status, 4 | 6..=8) {
                    self.local_orders.remove(&report.order_id);
                } else {
                    self.local_orders.insert(report.order_id, view);
                }
                self.project();
                let event = TitanOrderEventView {
                    order_id: view.order_id,
                    account_no: view.account_no,
                    asset_no: view.asset_no,
                    price_ticks: view.price_ticks,
                    qty_lots: view.qty_lots,
                    cumulative_filled_lots: view.cumulative_filled_lots,
                    status,
                    account_sequence: version,
                    event_ts_ns: self.now,
                    ..Default::default()
                };
                let commands = self.adapter.on_orders(self.now, &[event])?;
                self.commands(commands, "order")?;
                if let Some(position) = position {
                    self.schedule(
                        self.now + 1000,
                        Due::Fill(TitanFillView {
                            order_id: view.order_id,
                            account_no: view.account_no,
                            asset_no: view.asset_no,
                            fill_price_ticks: (report.exec_price / TICK).round() as i64,
                            fill_qty_lots: (report.exec_qty / LOT).round() as i64,
                            cumulative_filled_lots: view.cumulative_filled_lots,
                            account_sequence: version,
                            exchange_ts_ns: report.exchange_ts,
                            receive_ts_ns: self.now,
                            side: view.side,
                            final_fill: u8::from(status == 4),
                            ..Default::default()
                        }),
                    );
                    self.schedule(self.now + MS, Due::Position(position));
                }
            }
            Due::Fill(fill) => {
                self.project();
                let commands = self.adapter.on_fills(self.now, &[fill])?;
                self.commands(commands, "fill")?;
            }
            Due::Position(position) => {
                if position.account_sequence > self.local_position.account_sequence {
                    self.local_position = position;
                }
                self.project();
                let event = TitanPositionEventView {
                    qty_lots: self.local_position.qty_lots,
                    account_sequence: self.local_position.account_sequence,
                    event_ts_ns: self.now,
                    ..Default::default()
                };
                let commands = self.adapter.on_positions(self.now, &[event])?;
                self.commands(commands, "position")?;
            }
            Due::Cancel(event) => {
                self.project();
                let commands = self.adapter.on_cancels(self.now, &[event])?;
                self.commands(commands, "cancel")?;
            }
            Due::Funding(delta) => {
                self.local.apply(delta)?;
                self.project();
                let event = TitanBalanceEventView {
                    event_ts_ns: self.now,
                    ..Default::default()
                };
                let commands = self.adapter.on_balances(self.now, &[event])?;
                self.commands(commands, "funding")?;
            }
        }
        Ok(())
    }
    fn advance(&mut self, until: i64, inclusive: bool) -> Result<()> {
        loop {
            let due_ts = self
                .due
                .first_key_value()
                .map_or(i64::MAX, |((ts, _), _)| *ts);
            let ts = due_ts.min(self.next_timer);
            if ts > until || (!inclusive && ts == until) {
                break;
            }
            self.now = ts;
            if due_ts <= self.next_timer {
                let (_, event) = self.due.pop_first().unwrap();
                self.deliver(event)?;
            } else {
                self.next_timer += 100 * MS;
                if !self.adapter.is_stopped() {
                    self.project();
                    let commands = self.adapter.on_timer(
                        self.now,
                        &TitanTimerView {
                            fired_ts_ns: self.now,
                            scheduled_ts_ns: self.now,
                            ..Default::default()
                        },
                    )?;
                    self.commands(commands, "timer")?;
                }
            }
        }
        self.now = until;
        Ok(())
    }
    fn market_event(&mut self, event: MarketEvent) -> Result<()> {
        self.now = event.ts;
        match event.kind {
            QUOTE => {
                // Preserve source cents in the cache. Executable quotes round outwards to the
                // model's 0.1 USDT order tick; sub-tick historical prints are never rounded.
                self.market.best_bid_ticks = event.a / 10;
                self.market.best_bid_qty_lots = event.b;
                self.market.best_ask_ticks = (event.c + 9) / 10;
                self.market.best_ask_qty_lots = event.d;
                self.market.source_sequence += 1;
                self.quote_ts = self.now;
                if !self.adapter.is_stopped() && self.now - self.last_tick_callback_ns >= 100 * MS {
                    self.last_tick_callback_ns = self.now;
                    self.project();
                    let commands = self.adapter.on_ticks(
                        self.now,
                        &[TitanTickView {
                            receive_ts_ns: self.now,
                            exchange_ts_ns: self.now,
                            ..Default::default()
                        }],
                    )?;
                    self.commands(commands, "bbo")?;
                }
                self.measure(false)?;
            }
            SELL_TRADE | BUY_TRADE => {
                let mut volume = (event.b as f64 * self.model.participation).floor() as i64;
                let mut orders: Vec<_> = self
                    .exchange_orders
                    .iter()
                    .filter_map(|(id, request)| {
                        (request.time_in_force == 4
                            && penetrates(request.side, request.price_ticks, event))
                        .then_some((*id, *request))
                    })
                    .collect();
                orders.sort_by_key(|(id, request)| {
                    (
                        if request.side == 1 {
                            -request.price_ticks
                        } else {
                            request.price_ticks
                        },
                        *id,
                    )
                });
                for (id, request) in orders {
                    if volume == 0 {
                        break;
                    }
                    let order = self.coordinator.order(id).unwrap();
                    let leaves = ((order.request.qty - order.filled_qty) / LOT).round() as i64;
                    let mut qty = leaves.min(volume);
                    if request.reduce_only != 0 {
                        qty = qty.min(reducing_lots(self.pos_lots(), request.side));
                    }
                    if qty > 0 {
                        self.fill(id, qty, request.price_ticks, true)?;
                        volume -= qty;
                    }
                }
            }
            FUNDING => {
                let rate = f64::from_bits(event.a as u64);
                let payment = -self.exchange.account().position(INSTRUMENT).qty * self.mid() * rate;
                let delta = AccountDelta {
                    funding: payment,
                    ..AccountDelta::zero(INSTRUMENT, CURRENCY)
                };
                self.exchange.account_mut().apply(delta)?;
                self.schedule(self.now + self.model.latency, Due::Funding(delta));
                self.measure(true)?;
            }
            _ => bail!("invalid market event"),
        }
        Ok(())
    }
    fn measure(&mut self, force: bool) -> Result<()> {
        if self.quote_ts == 0 {
            return Ok(());
        }
        let pos = self.pos_lots();
        let equity = self.equity_value();
        self.metrics.equity(equity);
        let mut buys = 0;
        let mut sells = 0;
        let mut gross = 0.0;
        // Include requests still in transmission and orders awaiting terminal delivery.
        // Filled quantity already contributes to the exchange position; do not count it twice.
        for (id, order) in &self.local_orders {
            let exchange_filled = self
                .coordinator
                .order(*id)
                .map_or(0, |o| (o.filled_qty / LOT).round() as i64);
            let leaves = order.qty_lots - order.cumulative_filled_lots.max(exchange_filled);
            if order.side == 1 {
                buys += leaves;
            } else {
                sells += leaves;
            }
            gross += leaves as f64 * order.price_ticks as f64 * MONEY;
        }
        let bound = (pos + buys).abs().max((pos - sells).abs()) as f64 * LOT * self.mid();
        let notional = pos.abs() as f64 * LOT * self.mid();
        self.metrics.max_position_lots = self.metrics.max_position_lots.max(pos.abs());
        self.metrics.max_position_notional = self.metrics.max_position_notional.max(notional);
        self.metrics.max_reserved_notional = self.metrics.max_reserved_notional.max(bound);
        self.metrics.max_gross_order_notional = self.metrics.max_gross_order_notional.max(gross);
        ensure!(
            equity > notional * 0.005,
            "maintenance margin breached; no fabricated liquidation fills"
        );
        if force || self.now - self.last_sample >= 60 * SECOND {
            writeln!(
                self.equity,
                "{},{:.2},{:.8},{:.3},{:.8},{:.8}",
                self.now,
                self.mid(),
                equity,
                pos as f64 * LOT,
                notional,
                bound
            )?;
            self.last_sample = self.now;
        }
        Ok(())
    }
    fn replay(&mut self, events: &[MarketEvent]) -> Result<()> {
        ensure!(!events.is_empty(), "empty history");
        self.now = events[0].ts;
        self.next_timer = self.now + 100 * MS;
        self.adapter.start(self.now)?;
        let end = events.last().unwrap().ts;
        let stop_at = end / (86_400 * SECOND) * (86_400 * SECOND) + (86_400 - 30) * SECOND;
        let mut index = 0;
        while index < events.len() {
            let ts = events[index].ts;
            self.advance(ts, false)?;
            if !self.stop_started && ts >= stop_at {
                self.stop_started = true;
                self.project();
                let commands = self.adapter.stop(self.now)?;
                self.commands(commands, "stop")?;
            }
            while index < events.len() && events[index].ts == ts {
                self.market_event(events[index])?;
                index += 1;
            }
            self.advance(ts, true)?;
        }
        self.advance(end, true)?;
        self.measure(true)?;
        ensure!(
            self.adapter.is_stopped(),
            "stop did not complete by end of historical data"
        );
        ensure!(
            self.exchange_orders.is_empty() && self.local_orders.is_empty(),
            "stop left orders"
        );
        ensure!(
            self.pos_lots() == 0 && self.int("grid.expected_position_lots") == 0,
            "stop left inventory"
        );
        let gross = self.exchange.account().balance(CURRENCY) - self.initial_capital
            + self.exchange.account().fee(CURRENCY)
            - self.exchange.account().funding(CURRENCY);
        ensure!(
            (gross - self.float("grid.gross_total_tick_lots") * MONEY).abs() < 0.01,
            "strategy gross PnL differs from independent account ledger: {gross} vs {}",
            self.float("grid.gross_total_tick_lots") * MONEY
        );
        Ok(())
    }
    fn result(&self, error: Option<String>, data_path: &Path) -> Value {
        let account = self.exchange.account();
        let net = self.equity_value() - self.initial_capital;
        let flatten_started = self.layout.0["fields"]
            .get("grid.flatten_started_ns")
            .map(|_| self.int("grid.flatten_started_ns"));
        let flatten_duration_ms = self
            .business_stopped_ns
            .zip(flatten_started)
            .filter(|(stopped, started)| *started > 0 && stopped >= started)
            .map(|(stopped, started)| (stopped - started) as f64 / MS as f64);
        json!({ "schema_version": 1, "success": error.is_none(), "error": error,
            "data": data_path, "source_version": self.compiled_strategy.manifest.source_version.as_ref(),
            "compiled_strategy_digest": hex(&self.compiled_strategy.manifest.compiled_strategy_digest), "state_schema_hash": hex(&self.compiled_strategy.manifest.state.hash),
            "direction": self.int("config.direction"), "initial_capital_usdt": self.initial_capital,
            "lower_usdt": self.int("config.lower_price_ticks") as f64 * TICK, "upper_usdt": self.int("config.upper_price_ticks") as f64 * TICK,
            "leverage": self.leverage, "max_notional_limit_usdt": self.int("config.max_notional_tick_lots") as f64 * MONEY,
            "net_pnl_usdt": net, "net_return_pct": net / self.initial_capital * 100.0,
            "gross_pnl_usdt": net + account.fee(CURRENCY) - account.funding(CURRENCY),
            "fees_usdt": account.fee(CURRENCY), "funding_credit_usdt": account.funding(CURRENCY),
            "max_drawdown_usdt": self.metrics.max_drawdown, "max_drawdown_pct": self.metrics.max_drawdown_pct,
            "max_position_btc": self.metrics.max_position_lots as f64 * LOT, "max_position_notional_usdt": self.metrics.max_position_notional,
            "max_pending_bound_notional_usdt": self.metrics.max_reserved_notional, "max_gross_order_notional_usdt": self.metrics.max_gross_order_notional,
            "fills": self.metrics.fills, "partial_fills": self.metrics.partial_fills, "submits": self.metrics.submits, "cancels": self.metrics.cancels,
            "rejections": self.metrics.rejects, "cancel_lost_fill_race": self.metrics.cancel_lost_race,
            "final_position_btc": self.pos_lots() as f64 * LOT, "strategy_status": self.int("grid.status"), "strategy_error": self.int("grid.last_error"),
            "stop_reason": self.int("grid.stop_reason"), "stopped_ns": self.stopped_ns, "callbacks": self.callbacks,
            "business_stopped_ns": self.business_stopped_ns, "flatten_started_ns": flatten_started, "flatten_duration_ms": flatten_duration_ms,
            "model": { "maker_fee_rate": self.model.maker, "taker_fee_rate": self.model.taker,
                "one_way_latency_ms": self.model.latency / MS, "position_delivery_extra_ms": 1,
                "participation": self.model.participation, "maker_fill": "strict trade-through only; historical aggressor side; shared print volume budget",
                "ioc_impact_ticks": self.model.ioc_impact_ticks, "strategy_tick_min_interval_ms": 100, "matching_and_risk_use_all_quotes": true, "end_stop_seconds_before_midnight": 30,
                "source_price_unit_usdt": 0.01, "order_tick_usdt": TICK, "lot_btc": LOT, "quote_rounding": "bid floor and ask ceil to order tick; prints retain exact cents",
                "funding_valuation": "historical funding rate times current full-resolution BBO mid; historical mark price not available",
                "risk_bound": "conservative absolute net after every pending buy or every pending sell, including in-flight and awaiting-terminal requests" } })
    }
}

// Match predicates are deliberately strict: a touch alone does not establish queue priority.
fn penetrates(side: u8, limit: i64, event: MarketEvent) -> bool {
    (side == 1 && event.kind == SELL_TRADE && event.a < limit * 10)
        || (side == 2 && event.kind == BUY_TRADE && event.a > limit * 10)
}
fn reducing_lots(position: i64, side: u8) -> i64 {
    if side == 1 {
        (-position).max(0)
    } else {
        position.max(0)
    }
}
pub fn run(
    strategy_path: &Path,
    layout_path: &Path,
    data_path: &Path,
    output: &Path,
    stress: bool,
) -> Result<Value> {
    fs::create_dir_all(output)?;
    let loader =
        NativeStrategyLoaderV13::new(output.join("native-cache"), V13TrustPolicy::default());
    let compiled_strategy = Arc::new(loader.load(strategy_path)?);
    ensure!(
        compiled_strategy.manifest.strategy_type.as_ref() == "grid",
        "this verification runner is for the grid compiled_strategy"
    );
    let layout = Layout::new(layout_path, &compiled_strategy)?;
    let events = data::read(data_path)?;
    let mut replay = Replay::new(compiled_strategy, layout, output, Model::new(stress))?;
    let error = replay
        .replay(&events)
        .err()
        .map(|error| format!("{error:#}"));
    let result = replay.result(error, data_path);
    replay.fills.flush()?;
    replay.equity.flush()?;
    fs::write(
        output.join("journal.txt"),
        replay
            .journal
            .iter()
            .cloned()
            .collect::<Vec<_>>()
            .join("\n"),
    )?;
    fs::write(
        output.join("result.json"),
        serde_json::to_vec_pretty(&result)?,
    )?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn maker_needs_opposite_aggressor_and_strict_penetration() {
        let trade = MarketEvent {
            ts: 1,
            kind: SELL_TRADE,
            a: 990,
            b: 20,
            c: 0,
            d: 0,
        };
        assert!(penetrates(1, 100, trade));
        assert!(!penetrates(1, 99, trade));
        assert!(!penetrates(2, 98, trade));
        assert!(penetrates(
            2,
            98,
            MarketEvent {
                kind: BUY_TRADE,
                ..trade
            }
        ));
        assert!(penetrates(1, 100, MarketEvent { a: 999, ..trade }));
        assert!(penetrates(
            2,
            100,
            MarketEvent {
                kind: BUY_TRADE,
                a: 1001,
                ..trade
            }
        ));
    }
    #[test]
    fn drawdown_uses_running_equity_peak_and_includes_loss_before_recovery() {
        let mut metrics = Metrics {
            peak_equity: 10000.0,
            ..Default::default()
        };
        for equity in [9900.0, 11000.0, 8800.0, 12000.0] {
            metrics.equity(equity);
        }
        assert_eq!(metrics.max_drawdown, 2200.0);
        assert_eq!(metrics.max_drawdown_pct, 20.0);
    }
    #[test]
    fn reduce_only_capacity_cannot_reverse_or_increase() {
        assert_eq!(reducing_lots(3, 1), 0);
        assert_eq!(reducing_lots(3, 2), 3);
        assert_eq!(reducing_lots(-2, 1), 2);
        assert_eq!(reducing_lots(-2, 2), 0);
    }
}
