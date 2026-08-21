//! Rust 版做市示例策略（对标 README 中的 Python 示例）。
//!
//! 同一个 `MarketMaking` 实现直接跑回测（`Backtest`）和实盘（`LiveBot`），
//! 通过 [`Strategy`] trait 的 `on_tick`/`on_bar` 回调驱动：
//!
//! * 每帧从两级 `StrategyCtx` 快照读取行情（`market -> instrument`）；
//! * 持久状态放在 `instrument.state` 槽位（参见 `SLOT_*` 常量）；
//! * 通过回调里的 `hbt`（`&mut impl Bot`）下单/撤单。
//!
//! 下单语义与 Python 示例一致：以价格 tick 作为订单 ID，同一价格只挂一单，
//! 盘口变化或风险越界时撤旧单、挂新单，全部使用 GTX（post-only）限价单。

use hftbacktest::prelude::{
    Bot, InstrumentCtx, MarketDepth, OrdType, Side, Strategy, StrategyCtx, TimeInForce,
};

/// Per-instrument state slot: EWMA of frame-to-frame mid volatility.
pub const SLOT_VOLATILITY: usize = 0;
/// Per-instrument state slot: previous frame mid (input to the volatility estimate).
pub const SLOT_PREV_MID: usize = 1;

/// Ask-side order ids are `price_tick | ASK_ID_BASE` while bid-side ids are
/// `price_tick`, so bid/ask ids can never collide even on a one-tick-wide quote,
/// while keeping one order per price level.
pub const ASK_ID_BASE: u64 = 1 << 40;

/// Market-making strategy parameters and runtime counters.
#[derive(Debug, Clone)]
pub struct MarketMaking {
    /// Alpha coefficient: `reservation = mid + a * forecast`.
    pub a: f64,
    /// Risk-aversion coefficient: `reservation -= b * risk`.
    pub b: f64,
    /// Volatility scale: `risk = (c + volatility) * position`.
    pub c: f64,
    /// Half-spread scale: `half_spread = (c + volatility) * hs`.
    pub hs: f64,
    /// Maximum |position * mid| in quote currency.
    pub max_notional_position: f64,
    /// Notional value (quote currency) per order.
    pub notional_qty: f64,
    /// Frame counter (observability only; not part of the strategy state).
    pub frames: u64,
    /// Closed-bar counter.
    pub bars: u64,
    /// Orders submitted so far.
    pub orders_placed: u64,
    /// Orders canceled so far.
    pub orders_canceled: u64,
}

impl MarketMaking {
    pub fn new(
        a: f64,
        b: f64,
        c: f64,
        hs: f64,
        max_notional_position: f64,
        notional_qty: f64,
    ) -> Self {
        Self {
            a,
            b,
            c,
            hs,
            max_notional_position,
            notional_qty,
            frames: 0,
            bars: 0,
            orders_placed: 0,
            orders_canceled: 0,
        }
    }

    fn update_quotes<MD, E>(&mut self, hbt: &mut impl Bot<MD, Error = E>, inst: &mut InstrumentCtx)
    where
        MD: MarketDepth,
        E: std::fmt::Debug,
    {
        let asset_no = inst.asset_no;
        hbt.clear_inactive_orders(Some(asset_no));

        let depth = hbt.depth(asset_no);
        let tick_size = depth.tick_size();
        let lot_size = depth.lot_size();
        let mid = inst.mid;
        if !mid.is_finite() || mid <= 0.0 {
            // 尚无完整双边盘口，跳过本帧。
            return;
        }
        let position = inst.position;

        // 帧间 mid 波动的 EWMA，写入 instrument 状态槽（两级 ctx 的持久状态用法）。
        let prev_mid = inst.state[SLOT_PREV_MID];
        let frame_return = if prev_mid > 0.0 {
            (mid - prev_mid).abs() / prev_mid
        } else {
            0.0
        };
        let volatility = 0.05 * frame_return + 0.95 * inst.state[SLOT_VOLATILITY];
        inst.state[SLOT_VOLATILITY] = volatility;
        inst.state[SLOT_PREV_MID] = mid;

        // Alpha 与波动率在这里都是占位实现，可替换为任意指标。
        let forecast = 0.0;
        let risk = (self.c + volatility) * position;
        let half_spread = (self.c + volatility) * self.hs;

        // 公允价值 = mid + a * forecast；风险倾斜 = -b * risk。
        let reservation_price = mid + self.a * forecast - self.b * risk;
        let new_bid = reservation_price - half_spread;
        let new_ask = reservation_price + half_spread;

        // 不越过盘口：bid 不高于 best_bid，ask 不低于 best_ask。
        let new_bid_tick = ((new_bid / tick_size).round() as i64).clamp(1, depth.best_bid_tick());
        let new_ask_tick = ((new_ask / tick_size).round() as i64).max(depth.best_ask_tick());

        let order_qty = (self.notional_qty / mid / lot_size).round() * lot_size;
        if order_qty <= 0.0 {
            return;
        }

        let buy_limit_exceeded = position * mid > self.max_notional_position;
        let sell_limit_exceeded = position * mid < -self.max_notional_position;

        // 先扫描挂单，收集需要撤的单；不能边借 hbt.orders() 边调 hbt.cancel()。
        let mut update_bid = true;
        let mut update_ask = true;
        let mut cancels: Vec<u64> = Vec::new();
        for (&order_id, order) in hbt.orders(asset_no) {
            match order.side {
                Side::Buy => {
                    if order.price_tick == new_bid_tick || buy_limit_exceeded {
                        update_bid = false;
                    }
                    if order.cancellable() && (update_bid || buy_limit_exceeded) {
                        cancels.push(order_id);
                    }
                }
                Side::Sell => {
                    if order.price_tick == new_ask_tick || sell_limit_exceeded {
                        update_ask = false;
                    }
                    if order.cancellable() && (update_ask || sell_limit_exceeded) {
                        cancels.push(order_id);
                    }
                }
                Side::None | Side::Unsupported => {}
            }
        }

        for order_id in cancels {
            match hbt.cancel(asset_no, order_id, false) {
                Ok(_) => self.orders_canceled += 1,
                Err(error) => tracing::warn!(?error, asset_no, order_id, "cancel failed"),
            }
        }

        if update_bid && !buy_limit_exceeded {
            let order_id = new_bid_tick as u64;
            match hbt.submit_buy_order(
                asset_no,
                order_id,
                new_bid_tick as f64 * tick_size,
                order_qty,
                TimeInForce::GTX,
                OrdType::Limit,
                false,
            ) {
                Ok(_) => self.orders_placed += 1,
                Err(error) => tracing::warn!(?error, asset_no, order_id, "bid submit failed"),
            }
        }
        if update_ask && !sell_limit_exceeded {
            let order_id = new_ask_tick as u64 | ASK_ID_BASE;
            match hbt.submit_sell_order(
                asset_no,
                order_id,
                new_ask_tick as f64 * tick_size,
                order_qty,
                TimeInForce::GTX,
                OrdType::Limit,
                false,
            ) {
                Ok(_) => self.orders_placed += 1,
                Err(error) => tracing::warn!(?error, asset_no, order_id, "ask submit failed"),
            }
        }

        // 观测性：每秒打一条行情快照，方便实盘验证 on_tick 收到真实行情。
        if self.frames % 1000 == 1 {
            tracing::info!(
                asset_no,
                frame_ts = inst.frame_ts,
                mid,
                bid = inst.bid,
                ask = inst.ask,
                position,
                orders = hbt.orders(asset_no).len(),
                "tick snapshot"
            );
        }
    }
}

impl<MD, E> Strategy<MD, E> for MarketMaking
where
    MD: MarketDepth,
    E: std::fmt::Debug,
{
    fn on_tick(&mut self, hbt: &mut impl Bot<MD, Error = E>, ctx: &mut StrategyCtx) {
        self.frames += 1;
        // 两级 ctx：market -> instrument，单市场/多市场写法一致。
        for market in ctx.markets.iter_mut() {
            for inst in market.instruments.iter_mut() {
                self.update_quotes(hbt, inst);
            }
        }
    }

    fn on_bar(&mut self, _hbt: &mut impl Bot<MD, Error = E>, ctx: &mut StrategyCtx) {
        self.bars += 1;
        for market in ctx.markets.iter() {
            for inst in market.instruments.iter() {
                if inst.bar.inited {
                    tracing::info!(
                        asset_no = inst.asset_no,
                        open = inst.bar.o,
                        high = inst.bar.h,
                        low = inst.bar.l,
                        close = inst.bar.c,
                        volume = inst.bar.v,
                        "bar closed"
                    );
                }
            }
        }
    }
}
