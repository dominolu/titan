#![allow(clippy::not_unsafe_ptr_arg_deref)]

//! Rust-defined strategy context: two-level market → instrument snapshots.
//!
//! The layout is the canonical definition; Python mirrors it as structured dtypes in
//! `eventbot.py` and allocates the arrays. Each frame the event loop calls
//! `fill_strategy_ctx` once per asset to refresh the snapshot fields (BBO, trades view,
//! frame aggregates, trade bar) directly from the bot — zero-copy and without touching
//! the Python interpreter.

use hftbacktest::{
    depth::MarketDepth,
    prelude::{BUY_EVENT, Bot},
};

use crate::backtest::{HashMapMarketDepthBacktest, ROIVectorMarketDepthBacktest};

#[cfg(feature = "live")]
use crate::live::{HashMapMarketDepthLiveBot, ROIVectorMarketDepthLiveBot};

/// Per-instrument frame snapshot + persistent strategy state.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct InstrumentCtx {
    /// Global frame boundary timestamp (nanoseconds), identical across markets.
    pub frame_ts: i64,
    /// Exchange timestamp of the last trade in this frame (market-local clock).
    pub exch_ts: i64,
    /// Number of trades in this frame.
    pub n: i64,
    /// Zero-copy pointer into the bot's last-trades buffer (valid for `n` events).
    pub trades_ptr: usize,
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
    pub bar_open_ts: i64,
    pub bar_o: f64,
    pub bar_h: f64,
    pub bar_l: f64,
    pub bar_c: f64,
    pub bar_v: f64,
    /// 0 = no bar accumulated yet; the event loop resets it after `on_bar`.
    pub bar_inited: i64,
    pub position: f64,
    pub symbol_id: i64,
    pub tick_size: f64,
    pub lot_size: f64,
    /// Pointer to the per-instrument strategy state (64 f64s), allocated by Python.
    /// Kept as a pointer because Numba nested-array fields are read-only.
    pub state_ptr: usize,
}

impl Default for InstrumentCtx {
    fn default() -> Self {
        // POD: all fields are plain scalars/arrays.
        unsafe { std::mem::zeroed() }
    }
}

/// One venue (exchange / connector): its instruments plus venue-level state.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct MarketCtx {
    pub market_id: i64,
    pub n_instruments: i64,
    /// Pointer to the contiguous instrument array of this market.
    pub instruments_ptr: usize,
    /// Pointer to the market-level strategy state (64 f64s).
    pub market_state_ptr: usize,
}

impl Default for MarketCtx {
    fn default() -> Self {
        unsafe { std::mem::zeroed() }
    }
}

/// Top-level strategy context: global frame clock, market array, global state.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct StrategyCtx {
    pub frame_ts: i64,
    pub n_markets: i64,
    /// Pointer to the market array.
    pub markets_ptr: usize,
    /// Next global bar boundary (nanoseconds).
    pub next_bar_ts: i64,
    /// Pointer to the global strategy state (64 f64s).
    pub state_global_ptr: usize,
}

impl Default for StrategyCtx {
    fn default() -> Self {
        unsafe { std::mem::zeroed() }
    }
}

/// Fills the dynamic snapshot fields of one instrument from the bot.
fn fill_instrument<MD: MarketDepth>(hbt: &impl Bot<MD>, asset_no: usize, ctx: &mut InstrumentCtx) {
    ctx.frame_ts = hbt.current_timestamp();

    let depth = hbt.depth(asset_no);
    ctx.bid = depth.best_bid();
    ctx.ask = depth.best_ask();
    ctx.bid_qty = depth.best_bid_qty();
    ctx.ask_qty = depth.best_ask_qty();
    ctx.mid = if ctx.bid > 0.0 && ctx.ask > 0.0 {
        (ctx.bid + ctx.ask) * 0.5
    } else {
        0.0
    };
    ctx.spread = if ctx.bid > 0.0 && ctx.ask > 0.0 {
        ctx.ask - ctx.bid
    } else {
        0.0
    };
    ctx.position = hbt.position(asset_no);
    ctx.tick_size = depth.tick_size();
    ctx.lot_size = depth.lot_size();

    let trades = hbt.last_trades(asset_no);
    ctx.n = trades.len() as i64;
    ctx.trades_ptr = trades.as_ptr() as usize;

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
    ctx.frame_volume = volume;
    ctx.frame_buy_vol = buy_vol;
    ctx.frame_sell_vol = sell_vol;
    ctx.frame_vwap = if volume > 0.0 { vwap_num / volume } else { 0.0 };

    if let Some(last) = trades.last() {
        ctx.last_px = last.px;
        ctx.last_qty = last.qty;
        ctx.exch_ts = last.exch_ts;
    }

    // Trade-bar accumulation (aligned to the global frame clock).
    if ctx.n > 0 {
        if ctx.bar_inited == 0 {
            ctx.bar_open_ts = ctx.frame_ts;
            ctx.bar_o = trades[0].px;
            ctx.bar_h = trades[0].px;
            ctx.bar_l = trades[0].px;
            ctx.bar_inited = 1;
        }
        for t in trades.iter() {
            if t.px > ctx.bar_h {
                ctx.bar_h = t.px;
            }
            if t.px < ctx.bar_l {
                ctx.bar_l = t.px;
            }
        }
        ctx.bar_c = trades[trades.len() - 1].px;
        ctx.bar_v += volume;
    }
}

/// Dispatches the fill to the concrete bot type.
/// `kind`: 0 = HashMap backtest, 1 = ROIVector backtest, 2 = HashMap live, 3 = ROIVector live.
#[unsafe(no_mangle)]
pub extern "C" fn fill_strategy_ctx(
    hbt_ptr: usize,
    kind: i32,
    asset_no: usize,
    ctx_ptr: *mut InstrumentCtx,
) {
    let ctx = unsafe { &mut *ctx_ptr };
    match kind {
        0 => {
            let hbt = unsafe { &*(hbt_ptr as *const HashMapMarketDepthBacktest) };
            fill_instrument(hbt, asset_no, ctx);
        }
        1 => {
            let hbt = unsafe { &*(hbt_ptr as *const ROIVectorMarketDepthBacktest) };
            fill_instrument(hbt, asset_no, ctx);
        }
        #[cfg(feature = "live")]
        2 => {
            let hbt = unsafe { &*(hbt_ptr as *const HashMapMarketDepthLiveBot) };
            fill_instrument(hbt, asset_no, ctx);
        }
        #[cfg(feature = "live")]
        3 => {
            let hbt = unsafe { &*(hbt_ptr as *const ROIVectorMarketDepthLiveBot) };
            fill_instrument(hbt, asset_no, ctx);
        }
        _ => {}
    }
}

/// Exports the canonical struct sizes so Python can verify its mirrored dtypes match the
/// Rust layout (protects against silent memory corruption when fields are added).
#[unsafe(no_mangle)]
pub extern "C" fn strategy_ctx_layout(instr: *mut usize, market: *mut usize, strategy: *mut usize) {
    unsafe {
        *instr = size_of::<InstrumentCtx>();
        *market = size_of::<MarketCtx>();
        *strategy = size_of::<StrategyCtx>();
    }
}
