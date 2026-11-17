# Rust 策略接口（已退役）

> **状态：已退役（ABI V14，2026-11）。** 本文档保留为历史说明与迁移指引。
> hftbacktest 的 `strategy` 模块（`Strategy` trait、`StrategyCtx`、`run_strategy`、
> `run_strategy_for` 及 6 个单元测试）已删除；`hftbacktest::prelude` 不再导出它们。

## 为什么退役

1. 策略接口只有一条生产路径：Numba Python 单参数回调，经 Rust 编译器产出 ABI V14 产物，
   由 `titan-strategy-runtime` 加载执行。
2. `hftbacktest::strategy` 是第二条回测专用路径，与 V13/V14 的上下文布局、恢复语义、
   对象上下文各自演化，形成"影子 ABI"。
3. 维护两套上下文意味着每次 ABI 变更都要同步两处，且回测与实盘的执行语义无法保证一致。

## 现在怎么做回测

- 策略源码（`strategies/*.py`）→ `titan strategy compile` → 产物中的内联 TESTS（原生回调）
  → **历史回测**：`cargo run -p hftbacktest --example grid_history`，以真实 BBO/成交数据驱动
  同一份 `.titan`，撮合、延迟与费用由 HftBacktest 提供。
- CLI 的 `titan run -e backtest`（冻结 trace 回放）**已移除**：它只比对订单意图序列，没有盈亏与
  撮合，无法回答历史回测回答不了的问题。相关配置与轨迹夹具已一并删除。

## 迁移指引

| 旧用法 | 新做法 |
| --- | --- |
| `hftbacktest::prelude::{run_strategy, run_strategy_for}` | `cargo run -p hftbacktest --example grid_history -- prepare/run` |
| 实现 `Strategy` trait 的 `on_tick/on_bar` | 实现 `@njit def on_tick(ctx)` / `on_bar(ctx)`，声明 `SPEC` 与 `TESTS` |
| 通过 `StrategyCtx` 读行情 | 通过 `ctx.market()/ctx.ticks()/ctx.bars()/ctx.depth()` 读取视图 |
| 通过策略参数写死交易所单位 | 通过 `ctx.symbol()/account_info()/currency()/pair()/status()/fee()` 读权威对象 |

参考：[Bar/Tick 回测与实盘统一策略接口](bar_tick_numba_strategy.md)、
[策略观测协议](strategy_observation_protocol.md)。
