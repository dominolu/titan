//! Rust-native callback strategy interface with a two-level market → instrument context.
//!
//! The backtest and live bots both implement the [`Bot`] trait; this module adds a thin
//! callback layer on top of the same ``elapse`` loop so strategies can be written as
//! [`Strategy`] implementations with the same performance tier as the raw loop:
//!
//! * `on_tick` — fired once per *global frame* (default 1 ms) with a [`StrategyCtx`]
//!   snapshot of every market and instrument plus the bot for order operations.
//! * `on_bar` — fired at global bar boundaries (bars are aligned to the global frame
//!   clock, so cross-market bars are comparable).
//!
//! The context is a two-level structure (`market → instruments`) so single-market and
//! multi-market strategies are expressed the same way, while cross-market vs same-market
//! access is structurally explicit. Persistent strategy state lives in fixed-size
//! `state` slots (global / per-market / per-instrument).

use crate::{
    depth::MarketDepth,
    prelude::{Bot, ElapseResult, Event, BUY_EVENT},
};

/// Number of `f64` slots per strategy state block.
pub const STATE_SIZE: usize = 64;

/// Strategy specification: how the bot's flat assets are grouped into markets.
#[derive(Debug, Clone, Default)]
pub struct StrategySpec {
    /// Each inner vector lists the asset numbers of one market.
    /// Must partition ``0..num_assets`` exactly once.
    pub markets: Vec<Vec<usize>>,
    /// Per-asset symbol id (indexed by asset number).
    pub symbol_ids: Vec<i64>,
}

/// Errors produced while building the strategy context.
#[derive(Debug, Clone, thiserror::Error)]
pub enum StrategyError {
    #[error("invalid strategy spec: {0}")]
    InvalidSpec(String),
}

/// A single trade bar (OHLCV) accumulated from frame trades.
#[derive(Debug, Clone, Copy)]
pub struct Bar {
    pub open_ts: i64,
    pub o: f64,
    pub h: f64,
    pub l: f64,
    pub c: f64,
    pub v: f64,
    pub inited: bool,
}

impl Default for Bar {
    fn default() -> Self {
        Self {
            open_ts: 0,
            o: 0.0,
            h: 0.0,
            l: 0.0,
            c: 0.0,
            v: 0.0,
            inited: false,
        }
    }
}

/// Per-instrument frame snapshot + persistent strategy state.
#[derive(Debug, Clone)]
pub struct InstrumentCtx {
    /// The bot's flat asset number this instrument maps to.
    pub asset_no: usize,
    pub symbol_id: i64,
    pub tick_size: f64,
    pub lot_size: f64,
    /// Global frame boundary timestamp (nanoseconds), identical across markets.
    pub frame_ts: i64,
    /// Exchange timestamp of the last trade in this frame (market-local clock).
    pub exch_ts: i64,
    /// Number of trades in this frame.
    pub n: usize,
    /// Zero-copy pointer into the bot's last-trades buffer.
    trades_ptr: *const Event,
    pub last_px: f64,
    pub last_qty: f64,
    pub bid: f64,
    pub bid_qty: f64,
    pub ask: f64,
    pub ask_qty: f64,
    pub mid: f64,
    pub spread: f64,
    pub frame_volume: f64,
    pub frame_buy_vol: f64,
    pub frame_sell_vol: f64,
    pub frame_vwap: f64,
    pub bar: Bar,
    pub position: f64,
    pub state: [f64; STATE_SIZE],
}

impl Default for InstrumentCtx {
    fn default() -> Self {
        Self {
            asset_no: 0,
            symbol_id: 0,
            tick_size: 0.0,
            lot_size: 0.0,
            frame_ts: 0,
            exch_ts: 0,
            n: 0,
            trades_ptr: std::ptr::null(),
            last_px: 0.0,
            last_qty: 0.0,
            bid: 0.0,
            bid_qty: 0.0,
            ask: 0.0,
            ask_qty: 0.0,
            mid: 0.0,
            spread: 0.0,
            frame_volume: 0.0,
            frame_buy_vol: 0.0,
            frame_sell_vol: 0.0,
            frame_vwap: 0.0,
            bar: Bar::default(),
            position: 0.0,
            state: [0.0; STATE_SIZE],
        }
    }
}

impl InstrumentCtx {
    /// Zero-copy view of this frame's trades.
    ///
    /// **Frame-scoped contract**: the returned slice is valid only during the
    /// `on_tick`/`on_bar` callback that filled this context. The driver refreshes the
    /// snapshot and clears the bot's trade buffer on every frame, so a `StrategyCtx`
    /// cached across callbacks must not be used to read `trades()` later — it will
    /// observe stale counts/pointers from the frame it was filled in. Snapshot scalars
    /// (BBO, bar, position) are safe to cache; only the trades view is frame-scoped.
    pub fn trades(&self) -> &[Event] {
        if self.n == 0 {
            return &[];
        }
        // SAFETY: trades_ptr points into the bot's last-trades buffer which lives for the
        // whole run; n was copied from the buffer length at fill time. Valid while the
        // callback runs (documented above).
        unsafe { std::slice::from_raw_parts(self.trades_ptr, self.n) }
    }
}

/// One venue (exchange/connector): its instruments plus venue-level state.
#[derive(Debug, Clone)]
pub struct MarketCtx {
    pub market_id: i64,
    pub market_state: [f64; STATE_SIZE],
    pub instruments: Vec<InstrumentCtx>,
}

impl Default for MarketCtx {
    fn default() -> Self {
        Self {
            market_id: 0,
            market_state: [0.0; STATE_SIZE],
            instruments: Vec::new(),
        }
    }
}

/// Top-level strategy context: global frame clock, markets, global state.
#[derive(Debug, Clone)]
pub struct StrategyCtx {
    pub frame_ts: i64,
    pub next_bar_ts: i64,
    pub state_global: [f64; STATE_SIZE],
    pub markets: Vec<MarketCtx>,
}

impl Default for StrategyCtx {
    fn default() -> Self {
        Self {
            frame_ts: 0,
            next_bar_ts: 0,
            state_global: [0.0; STATE_SIZE],
            markets: Vec::new(),
        }
    }
}

impl StrategyCtx {
    /// Builds the two-level context from a spec, allocating one instrument per asset.
    pub fn new(spec: &StrategySpec, num_assets: usize) -> Result<Self, StrategyError> {
        let flat: Vec<usize> = spec.markets.iter().flatten().copied().collect();
        let mut sorted = flat.clone();
        sorted.sort_unstable();
        if sorted != (0..num_assets).collect::<Vec<_>>() {
            return Err(StrategyError::InvalidSpec(
                "markets must partition all assets 0..num_assets-1 exactly once".to_string(),
            ));
        }
        if spec.symbol_ids.len() != num_assets {
            return Err(StrategyError::InvalidSpec(
                "symbol_ids must have one id per asset".to_string(),
            ));
        }

        let mut markets = Vec::with_capacity(spec.markets.len());
        for (m, assets) in spec.markets.iter().enumerate() {
            let mut instruments = Vec::with_capacity(assets.len());
            for (i, &asset_no) in assets.iter().enumerate() {
                instruments.push(InstrumentCtx {
                    asset_no,
                    symbol_id: spec.symbol_ids[asset_no],
                    ..Default::default()
                });
            }
            markets.push(MarketCtx {
                market_id: m as i64,
                instruments,
                ..Default::default()
            });
        }
        Ok(StrategyCtx {
            markets,
            ..Default::default()
        })
    }
}

/// Callback strategy interface.
///
/// `hbt` is passed so callbacks can place/cancel orders and query resting orders through
/// the bot; `ctx` carries the two-level market snapshot and persistent state.
///
/// Context lifetime rules:
/// * `ctx` is valid during the callback and is reused across frames — the strategy keeps
///   its own state in the `state` slots, not by holding `ctx` between calls.
/// * `ctx.instruments[..].trades()` is frame-scoped (see [`InstrumentCtx::trades`]).
/// * State slot conventions: `state_global` for cross-market state, `market_state` for
///   per-venue state, `instrument.state` for per-symbol state. Slot assignment should be
///   documented per strategy (e.g. constants `SLOT_*`); no two meanings per slot.
pub trait Strategy<MD: MarketDepth, E> {
    /// Called once per global frame.
    fn on_tick(&mut self, hbt: &mut impl Bot<MD, Error = E>, ctx: &mut StrategyCtx);

    /// Called at global bar boundaries. Defaults to a no-op.
    fn on_bar(&mut self, _hbt: &mut impl Bot<MD, Error = E>, _ctx: &mut StrategyCtx) {}
}

/// Fills one instrument's frame snapshot directly from the bot.
fn fill<MD: MarketDepth>(hbt: &impl Bot<MD>, asset_no: usize, frame_ts: i64, inst: &mut InstrumentCtx) {
    inst.frame_ts = frame_ts;

    let depth = hbt.depth(asset_no);
    inst.bid = depth.best_bid();
    inst.ask = depth.best_ask();
    inst.bid_qty = depth.best_bid_qty();
    inst.ask_qty = depth.best_ask_qty();
    inst.mid = if inst.bid > 0.0 && inst.ask > 0.0 {
        (inst.bid + inst.ask) * 0.5
    } else {
        0.0
    };
    inst.spread = if inst.bid > 0.0 && inst.ask > 0.0 {
        inst.ask - inst.bid
    } else {
        0.0
    };
    inst.position = hbt.position(asset_no);
    inst.tick_size = depth.tick_size();
    inst.lot_size = depth.lot_size();

    let trades = hbt.last_trades(asset_no);
    inst.n = trades.len();
    inst.trades_ptr = trades.as_ptr();

    let mut volume = 0.0f64;
    let mut buy_vol = 0.0f64;
    let mut sell_vol = 0.0f64;
    let mut vwap_num = 0.0f64;
    for t in trades.iter() {
        volume += t.qty;
        if t.is(BUY_EVENT) {
            buy_vol += t.qty;
        } else {
            sell_vol += t.qty;
        }
        vwap_num += t.px * t.qty;
    }
    inst.frame_volume = volume;
    inst.frame_buy_vol = buy_vol;
    inst.frame_sell_vol = sell_vol;
    inst.frame_vwap = if volume > 0.0 { vwap_num / volume } else { 0.0 };

    if let Some(last) = trades.last() {
        inst.last_px = last.px;
        inst.last_qty = last.qty;
        inst.exch_ts = last.exch_ts;
    }

    // Trade-bar accumulation (aligned to the global frame clock).
    if inst.n > 0 {
        if !inst.bar.inited {
            inst.bar.open_ts = inst.frame_ts;
            inst.bar.o = trades[0].px;
            inst.bar.h = trades[0].px;
            inst.bar.l = trades[0].px;
            inst.bar.inited = true;
        }
        for t in trades.iter() {
            if t.px > inst.bar.h {
                inst.bar.h = t.px;
            }
            if t.px < inst.bar.l {
                inst.bar.l = t.px;
            }
        }
        inst.bar.c = trades[trades.len() - 1].px;
        inst.bar.v += volume;
    }
}

/// Runs the global-frame callback loop on a backtest or live bot.
///
/// Each frame:
/// 1. elapses the whole bot (all assets advance on the same clock),
/// 2. refreshes every instrument snapshot from the bot,
/// 3. calls `strategy.on_tick(hbt, ctx)` once,
/// 4. closes global bars and calls `strategy.on_bar(hbt, ctx)` at boundaries,
/// 5. clears the per-asset trade buffers.
///
/// The final partial frame is delivered before the loop stops, matching the engine's
/// end-of-data semantics. Bars are anchored to the first frame; a data gap spanning
/// several bar intervals produces a single merged bar.
pub fn run_strategy<MD, E, S>(
    hbt: &mut impl Bot<MD, Error = E>,
    strategy: &mut S,
    ctx: &mut StrategyCtx,
    frame_interval: i64,
    bar_interval: i64,
) -> Result<(), E>
where
    MD: MarketDepth,
    S: Strategy<MD, E>,
{
    let n_assets = hbt.num_assets();
    // asset_no -> (market_idx, instrument_idx)，由 ctx 结构推导，避免与 spec 脱节。
    let mut locs: Vec<(usize, usize)> = vec![(0, 0); n_assets];
    for (m, market) in ctx.markets.iter().enumerate() {
        for (i, inst) in market.instruments.iter().enumerate() {
            if inst.asset_no < n_assets {
                locs[inst.asset_no] = (m, i);
            }
        }
    }
    loop {
        let r = hbt.elapse(frame_interval)?;
        let ts = hbt.current_timestamp();
        ctx.frame_ts = ts;

        for asset_no in 0..n_assets {
            let (m, i) = locs[asset_no];
            let inst = &mut ctx.markets[m].instruments[i];
            fill(hbt, asset_no, ts, inst);
        }

        strategy.on_tick(hbt, ctx);

        if ctx.next_bar_ts == 0 {
            ctx.next_bar_ts = ts + bar_interval;
        }
        if ts >= ctx.next_bar_ts {
            strategy.on_bar(hbt, ctx);
            for market in ctx.markets.iter_mut() {
                for inst in market.instruments.iter_mut() {
                    inst.bar = Bar::default();
                }
            }
            while ctx.next_bar_ts <= ts {
                ctx.next_bar_ts += bar_interval;
            }
        }

        for asset_no in 0..n_assets {
            hbt.clear_last_trades(Some(asset_no));
        }

        if r != ElapseResult::Ok {
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        backtest::{
            Backtest,
            BacktestError,
            DataSource,
            ExchangeKind::NoPartialFillExchange,
            L2AssetBuilder,
            assettype::LinearAsset,
            data::Data,
            models::{
                CommonFees,
                ConstantLatency,
                PowerProbQueueFunc3,
                ProbQueueModel,
                TradingValueFeeModel,
            },
        },
        depth::HashMapMarketDepth,
        types::{
            DEPTH_SNAPSHOT_EVENT, EXCH_EVENT, LOCAL_EVENT, SELL_EVENT, TRADE_EVENT, OrdType,
            TimeInForce,
        },
    };

    fn event(ev: u64, ts: i64, px: f64, qty: f64) -> Event {
        Event {
            ev,
            exch_ts: ts,
            local_ts: ts,
            px,
            qty,
            order_id: 0,
            ival: 0,
            fval: 0.0,
        }
    }

    fn data_for(snap_bids: &[(f64, f64)], snap_asks: &[(f64, f64)], trades: &[(i64, f64, f64, bool)]) -> Data<Event> {
        let mut rows = Vec::new();
        for (px, qty) in snap_bids {
            rows.push(event(DEPTH_SNAPSHOT_EVENT | BUY_EVENT | EXCH_EVENT | LOCAL_EVENT, 1_000_000_000, *px, *qty));
        }
        for (px, qty) in snap_asks {
            rows.push(event(DEPTH_SNAPSHOT_EVENT | SELL_EVENT | EXCH_EVENT | LOCAL_EVENT, 1_000_000_000, *px, *qty));
        }
        for (t, px, qty, side_buy) in trades {
            let f = TRADE_EVENT | if *side_buy { BUY_EVENT } else { SELL_EVENT } | EXCH_EVENT | LOCAL_EVENT;
            rows.push(event(f, *t, *px, *qty));
        }
        Data::from_data(&rows)
    }

    struct TestStrategy {
        frames: i64,
        mid0_sum: f64,
        mid1_sum: f64,
        trades0: i64,
        trades1: i64,
        bars: i64,
        first_bar_open: f64,
        first_bar_vol: f64,
        bar_recorded: bool,
    }

    impl<MD: MarketDepth> Strategy<MD, BacktestError> for TestStrategy {
        fn on_tick(&mut self, _hbt: &mut impl Bot<MD, Error = BacktestError>, ctx: &mut StrategyCtx) {
            self.frames += 1;
            self.mid0_sum += ctx.markets[0].instruments[0].mid;
            self.mid1_sum += ctx.markets[1].instruments[0].mid;
            self.trades0 += ctx.markets[0].instruments[0].trades().len() as i64;
            self.trades1 += ctx.markets[1].instruments[0].trades().len() as i64;
            ctx.state_global[0] += 1.0;
        }

        fn on_bar(&mut self, _hbt: &mut impl Bot<MD, Error = BacktestError>, ctx: &mut StrategyCtx) {
            self.bars += 1;
            if !self.bar_recorded {
                let bar = ctx.markets[0].instruments[0].bar;
                self.first_bar_open = bar.o;
                self.first_bar_vol = bar.v;
                self.bar_recorded = true;
            }
        }
    }

    fn build_backtest() -> Backtest<HashMapMarketDepth> {
        let btc = data_for(
            &[(100.0, 1.0), (99.0, 2.0)],
            &[(101.0, 1.0), (102.0, 2.0)],
            &[
                (1_100_000_000, 100.0, 1.0, false),
                (1_200_000_000, 101.0, 2.0, true),
                (1_400_000_000, 100.5, 1.0, false),
                (1_700_000_000, 102.0, 1.0, true),
            ],
        );
        let eth = data_for(
            &[(10.0, 5.0), (9.0, 5.0)],
            &[(11.0, 5.0), (12.0, 5.0)],
            &[
                (1_150_000_000, 10.0, 5.0, false),
                (1_250_000_000, 10.5, 3.0, true),
                (1_450_000_000, 11.0, 2.0, false),
            ],
        );

        let asset = |data: Data<Event>| {
            L2AssetBuilder::default()
                .data(vec![DataSource::Data(data)])
                .latency_model(ConstantLatency::new(0, 0))
                .asset_type(LinearAsset::new(1.0))
                .fee_model(TradingValueFeeModel::new(CommonFees::new(0.0, 0.0)))
                .queue_model(ProbQueueModel::new(PowerProbQueueFunc3::new(3.0)))
                .exchange(NoPartialFillExchange)
                .depth(|| HashMapMarketDepth::new(0.1, 0.001))
                .last_trades_capacity(16)
                .build()
                .unwrap()
        };

        Backtest::builder()
            .add_asset(asset(btc))
            .add_asset(asset(eth))
            .build()
            .unwrap()
    }

    #[test]
    fn test_two_market_strategy() {
        let mut backtester = build_backtest();
        let spec = StrategySpec {
            markets: vec![vec![0], vec![1]],
            symbol_ids: vec![1, 2],
        };
        let mut ctx = StrategyCtx::new(&spec, 2).unwrap();
        let mut strategy = TestStrategy {
            frames: 0,
            mid0_sum: 0.0,
            mid1_sum: 0.0,
            trades0: 0,
            trades1: 0,
            bars: 0,
            first_bar_open: 0.0,
            first_bar_vol: 0.0,
            bar_recorded: false,
        };

        run_strategy::<_, _, _>(&mut backtester, &mut strategy, &mut ctx, 100_000_000, 500_000_000).unwrap();

        assert_eq!(strategy.frames, 7);
        assert!((strategy.mid0_sum - 7.0 * 100.5).abs() < 1e-9, "mid0={}", strategy.mid0_sum);
        assert!((strategy.mid1_sum - 7.0 * 10.5).abs() < 1e-9, "mid1={}", strategy.mid1_sum);
        assert_eq!(strategy.trades0, 4);
        assert_eq!(strategy.trades1, 3);
        assert!(strategy.bars >= 1);
        assert!((strategy.first_bar_open - 100.0).abs() < 1e-9);
        assert!((strategy.first_bar_vol - 4.0).abs() < 1e-9);
        assert!((ctx.state_global[0] - 7.0).abs() < 1e-9);
    }

    #[test]
    fn test_invalid_spec() {
        let spec = StrategySpec {
            markets: vec![vec![0], vec![1]],
            symbol_ids: vec![1],
        };
        assert!(StrategyCtx::new(&spec, 2).is_err());

        let spec = StrategySpec {
            markets: vec![vec![0, 1], vec![2]],
            symbol_ids: vec![1, 1, 2],
        };
        assert!(StrategyCtx::new(&spec, 2).is_err());
    }

    #[test]
    fn test_single_market_multi_instrument() {
        let mut backtester = build_backtest();
        let spec = StrategySpec {
            markets: vec![vec![0, 1]],
            symbol_ids: vec![1, 2],
        };
        let mut ctx = StrategyCtx::new(&spec, 2).unwrap();

        struct SingleMarketStrategy {
            frames: i64,
            trades0: i64,
            trades1: i64,
        }

        impl<MD: MarketDepth> Strategy<MD, BacktestError> for SingleMarketStrategy {
            fn on_tick(
                &mut self,
                _hbt: &mut impl Bot<MD, Error = BacktestError>,
                ctx: &mut StrategyCtx,
            ) {
                self.frames += 1;
                self.trades0 += ctx.markets[0].instruments[0].trades().len() as i64;
                self.trades1 += ctx.markets[0].instruments[1].trades().len() as i64;
            }
        }

        let mut strategy = SingleMarketStrategy {
            frames: 0,
            trades0: 0,
            trades1: 0,
        };
        run_strategy::<_, _, _>(&mut backtester, &mut strategy, &mut ctx, 100_000_000, 500_000_000).unwrap();
        assert_eq!(strategy.frames, 7);
        assert_eq!(strategy.trades0, 4);
        assert_eq!(strategy.trades1, 3);
        assert_eq!(ctx.markets.len(), 1);
        assert_eq!(ctx.markets[0].instruments.len(), 2);
    }

    #[test]
    fn test_order_placement_from_callback() {
        // P1-1: 回调里通过 hbt 下单 → 后续成交 → 持仓更新。
        let mut backtester = build_backtest();
        let spec = StrategySpec {
            markets: vec![vec![0]],
            symbol_ids: vec![1],
        };
        let mut ctx = StrategyCtx::new(&spec, 1).unwrap();

        struct OrderStrategy {
            submitted: bool,
            final_position: f64,
        }

        impl<MD: MarketDepth> Strategy<MD, BacktestError> for OrderStrategy {
            fn on_tick(
                &mut self,
                hbt: &mut impl Bot<MD, Error = BacktestError>,
                _ctx: &mut StrategyCtx,
            ) {
                if !self.submitted {
                    // 买一挂在远高于盘口的价格，后续卖单会成交它。
                    hbt.submit_buy_order(
                        0,
                        1,
                        200.0,
                        1.0,
                        TimeInForce::GTC,
                        OrdType::Limit,
                        false,
                    )
                    .unwrap();
                    self.submitted = true;
                } else {
                    self.final_position = hbt.position(0);
                }
            }
        }

        let mut strategy = OrderStrategy {
            submitted: false,
            final_position: 0.0,
        };
        run_strategy::<_, _, _>(&mut backtester, &mut strategy, &mut ctx, 100_000_000, 500_000_000).unwrap();

        assert!(strategy.submitted);
        assert!(
            (strategy.final_position - 1.0).abs() < 1e-9,
            "order should fill and set position, got {}",
            strategy.final_position
        );
        assert!((backtester.position(0) - 1.0).abs() < 1e-9);
    }

    #[test]
    fn test_bar_reset_between_bars() {
        // P1-2: on_bar 重置后，第二根 bar 的 OHLCV 必须独立正确。
        let data = data_for(
            &[(100.0, 1.0), (99.0, 2.0)],
            &[(101.0, 1.0), (102.0, 2.0)],
            &[
                (1_100_000_000, 100.0, 1.0, false),
                (1_200_000_000, 101.0, 2.0, true),
                (1_400_000_000, 100.5, 1.0, false),
                (1_700_000_000, 102.0, 1.0, true),
                (2_200_000_000, 103.0, 2.0, true),
                (2_400_000_000, 102.5, 1.0, false),
            ],
        );
        let asset = L2AssetBuilder::default()
            .data(vec![DataSource::Data(data)])
            .latency_model(ConstantLatency::new(0, 0))
            .asset_type(LinearAsset::new(1.0))
            .fee_model(TradingValueFeeModel::new(CommonFees::new(0.0, 0.0)))
            .queue_model(ProbQueueModel::new(PowerProbQueueFunc3::new(3.0)))
            .exchange(NoPartialFillExchange)
            .depth(|| HashMapMarketDepth::new(0.1, 0.001))
            .last_trades_capacity(16)
            .build()
            .unwrap();
        let mut backtester = Backtest::builder().add_asset(asset).build().unwrap();
        let spec = StrategySpec {
            markets: vec![vec![0]],
            symbol_ids: vec![1],
        };
        let mut ctx = StrategyCtx::new(&spec, 1).unwrap();

        struct BarStrategy {
            bars: i64,
            bar1: Bar,
            bar2: Bar,
        }

        impl<MD: MarketDepth> Strategy<MD, BacktestError> for BarStrategy {
            fn on_tick(
                &mut self,
                _hbt: &mut impl Bot<MD, Error = BacktestError>,
                _ctx: &mut StrategyCtx,
            ) {
            }

            fn on_bar(
                &mut self,
                _hbt: &mut impl Bot<MD, Error = BacktestError>,
                ctx: &mut StrategyCtx,
            ) {
                self.bars += 1;
                let bar = ctx.markets[0].instruments[0].bar;
                match self.bars {
                    1 => self.bar1 = bar,
                    2 => self.bar2 = bar,
                    _ => {}
                }
            }
        }

        let mut strategy = BarStrategy {
            bars: 0,
            bar1: Bar::default(),
            bar2: Bar::default(),
        };
        run_strategy::<_, _, _>(&mut backtester, &mut strategy, &mut ctx, 100_000_000, 500_000_000).unwrap();

        assert_eq!(strategy.bars, 2);
        // bar1: trades 1.1/1.2/1.4
        assert!((strategy.bar1.o - 100.0).abs() < 1e-9);
        assert!((strategy.bar1.c - 100.5).abs() < 1e-9);
        assert!((strategy.bar1.v - 4.0).abs() < 1e-9);
        // bar2: trade 1.7（重置后独立聚合，不含 bar1 数据）
        assert!((strategy.bar2.o - 102.0).abs() < 1e-9);
        assert!((strategy.bar2.c - 102.0).abs() < 1e-9);
        assert!((strategy.bar2.v - 1.0).abs() < 1e-9);
    }
}
