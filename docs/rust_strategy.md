# Rust 策略（Strategy trait）用法

> 当前状态说明：本页描述当前已经可运行的 Rust 策略接口。最终面向策略作者的目标接口
> 是 Numba Python 单参数回调 `@njit def on_tick(s)` / `@njit def on_bar(s)`，并显式
> 支持 Bar、Tick 和 Hybrid 数据源。完整需求见
> [Bar/Tick 回测与实盘统一策略接口](bar_tick_numba_strategy.md)。Rust trait 将作为核心
> 内部接口、测试接口或迁移期兼容层保留，不应被视为最终 Python API。

Rust 策略通过 `hftbacktest::strategy::Strategy` trait 编写，跑在 hftbacktest
回测引擎（`Backtest`）上；实盘走 Titan 的 EventEngine/PluginEngine 新链路。
可运行的最小参考实现是 [examples](../examples/src/market_making.rs) crate 里的
`MarketMaking`（对标根 README 的 Python 做市示例）。

## 回调入口

```rust
pub trait Strategy<MD: MarketDepth, E> {
    fn on_tick(&mut self, hbt: &mut impl Bot<MD, Error = E>, ctx: &mut StrategyCtx);
    fn on_bar(&mut self, hbt: &mut impl Bot<MD, Error = E>, ctx: &mut StrategyCtx) {}
}
```

* `on_tick`：每个**全局帧**触发一次（默认 1 ms，由 `run_strategy` 的 `frame_interval`
  决定），所有市场共用同一帧时钟，因此跨市场比较天然对齐。
* `on_bar`：在全局 bar 边界触发（`bar_interval`），bar 对齐帧时钟；默认是空操作。
  `ctx` 里每个品种的 `inst.bar` 是上一根已完成 bar 的 OHLCV 快照，回调返回后由
  驱动循环重置。
* `hbt` 是 `&mut impl Bot`，与 `elapse` 手写循环同一性能层级；下单/撤单/查单都从
  这里调用。

驱动循环：

```rust
use hftbacktest::prelude::{run_strategy, run_strategy_for};

// 回测：跑到数据末尾自然结束。
run_strategy(&mut backtester, &mut strategy, &mut ctx, frame_interval_ns, bar_interval_ns)?;

// 实盘：行情不会结束，用时长截止（也可以 run_seconds = 0 一直跑，Ctrl-C 退出）。
run_strategy_for(&mut hbt, &mut strategy, &mut ctx, frame_interval_ns, bar_interval_ns, max_duration_ns)?;
```

## 两级 ctx：market → instrument

`StrategyCtx` 是快照结构，每个回调重新填充（同一对象复用，不要跨回调持有它）：

```text
StrategyCtx
├── frame_ts: i64           全局帧时间戳（纳秒）
├── next_bar_ts: i64        下一个 bar 边界
├── state_global: [f64; 64] 全局策略状态槽
└── markets: Vec<MarketCtx> 每个 venue/连接器一个
    ├── market_id: i64
    ├── market_state: [f64; 64]
    └── instruments: Vec<InstrumentCtx>  每个品种一个
        ├── tick_size / lot_size
        ├── frame_ts / exch_ts / n
        ├── last_px / last_qty
        ├── bid / ask / bid_qty / ask_qty / mid / spread
        ├── frame_volume / frame_buy_vol / frame_sell_vol / frame_vwap
        ├── bar: Bar           上一根 bar 的 OHLCV（on_bar 里读取）
        ├── position: f64
        ├── state: [f64; 64]   品种级策略状态槽
        └── trades() -> &[Event]  本帧逐笔成交（上下文自有快照）
```

读取方式（单市场、多市场写法一致）：

```rust
fn on_tick(&mut self, hbt: &mut impl Bot<MD, Error = E>, ctx: &mut StrategyCtx) {
    for market in ctx.markets.iter() {
        for inst in market.instruments.iter() {
            let mid = inst.mid;
            let position = inst.position;
            let trades = inst.trades(); // 当前上下文所持有的安全快照
            // ...
        }
    }
}
```

`trades()` 返回上下文持有的成交快照。上下文会在下一帧复用其分配；如果确实要跨帧保存，
克隆 `InstrumentCtx` 或复制所需字段即可，不依赖机器人的内部缓冲区生命周期。

## 状态槽：固定大小、按约定分配

持久状态放在固定 64 个 `f64` 槽位里，避免每次回调重新分配，也方便跨市场/跨品种共享：

* `ctx.state_global`：跨市场状态（如组合层指标）。
* `ctx.markets[m].market_state`：单个 venue 的状态。
* `ctx.markets[m].instruments[i].state`：单个品种的状态。

每个策略应该用常量声明槽位含义，且一个槽只放一个量，例如
[examples/src/market_making.rs](../examples/src/market_making.rs) 里：

```rust
pub const SLOT_VOLATILITY: usize = 0; // 帧间 mid 波动率 EWMA
pub const SLOT_PREV_MID: usize = 1;   // 上一帧 mid
```

槽位内容由策略自己读写；`on_tick` 里 `inst.state[SLOT_VOLATILITY]` 直接原地更新即可。

## 下单：通过回调里的 `hbt`

```rust
use hftbacktest::prelude::{OrdType, TimeInForce};

// 买一：GTX = post-only，避免 Maker/Taker 翻转。
hbt.submit_buy_order(
    asset_no,          // 品种编号（0..num_assets）
    order_id,          // u64，本地必须唯一；示例策略用价格 tick 派生，保证同价一单
    price,             // 原始价格（f64）
    qty,               // 数量（f64，按 lot_size 对齐）
    TimeInForce::GTX,
    OrdType::Limit,
    false,             // wait：false = 不阻塞帧循环，订单响应事件回流后下帧可见
)?;

// 卖一 / 撤单 / 清理已结束订单：
hbt.submit_sell_order(asset_no, order_id, price, qty, TimeInForce::GTC, OrdType::Limit, false)?;
hbt.cancel(asset_no, order_id, false)?;
hbt.clear_inactive_orders(Some(asset_no)); // 从本地订单表移除已成交/已撤销的单
```

要点：

* `wait = true` 会阻塞当前回调直到订单响应（Python 风格），回测里会推进到响应时刻；
  事件驱动写法更推荐 `wait = false`，下一帧从 `hbt.orders(asset_no)` 看到最新状态。
* 持仓由 `LiveEvent::Position` 回流更新，`ctx` 里每个品种的 `position` 就是实时值。
* 撤单时先收集 `order_id` 再调用 `hbt.cancel(...)`，不要边遍历 `hbt.orders()` 边撤
  （Rust 借用规则）。
* 实盘改单被明确禁用；`modify()` 返回 `UnsupportedOperation`。请先撤销原订单，收到撤单
  确认后再提交替代订单。

## 实盘接线

旧连接器进程 + iceoryx IPC 的实盘路径已随重构删除。当前实盘统一走 `titan` CLI：
PluginEngine 动态加载交易所插件包，Market/AccountPlugin 经 EventEngine 与策略 consumer 相连；
Rust 侧市场做市示例 `MarketMaking` 保留用于回测，Numba 策略与配置见 README 与
[`bar_tick_numba_strategy.md`](bar_tick_numba_strategy.md)。

## 回测接线

```console
# 合成数据（无需文件）
cargo run -p titan-examples --bin backtest

# 归一化 npz 数据（collector 采集后转换，key = "data"）
cargo run -p titan-examples --bin backtest -- --data /path/to/btc.npz
```
