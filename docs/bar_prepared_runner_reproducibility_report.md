# Bar PreparedRunner 与双均线复现验收

日期：2026-08-23
工具链：Rust 1.94.0 release、CPython 3.11、Numba、ta-numba 0.4.0
数据：`data/AAPL_1m_all_sources.parquet`，`source=polygon_s3`，264,190 根 1 分钟完整 Bar。

## 验收结果

| 路径 | 轮数 | 一致性 | 中位耗时 | 结果摘要 |
|---|---:|---|---:|---|
| Rust 原生 Materialized Bar replay | 100 | 输入与 callback 数稳定 | 8.843 ms | 约 29,926,330 Bar/s |
| Rust `PreparedBarRuntime` reset/reuse 单测 | 100 | BacktestResult 核心字段逐轮一致 | 测试内不作为性能基准 | 生命周期、feed、history、账户和 buffer 均重置 |
| Rust + Numba 预计算 SMA 双均线 | 100 | 核心策略状态逐轮完全一致 | 510.964 ms | 金叉 3,012、死叉 3,012、成交量 6,023、最终持仓 1 |

Numba 行通过 `examples/dual_ma_bar_backtest.py --runs 100` 验证。每轮复用已编译 callback
和 init 阶段预计算的 SMA 数组，显式清零策略状态；Rust 仍拥有事件循环、Bar history、
NextOpen 撮合和订单/成交回调。100 轮总运行时间为 52.453389 秒。

Rust 原生行通过：

```text
cargo run --release -p titan-examples --bin backtest -- \
  --data-kind bar --data data/AAPL_1m_all_sources.parquet \
  --bar-source polygon_s3 --runs 100
```

原生 100 轮总时间 0.882801 秒，单轮 mean 8.828 ms、p95 9.325 ms。

## 结论

AC-RST-001 的 100 次复现要求已由 Rust PreparedRunner 和真实 Numba 双均线两条路径覆盖。
Numba 路径的主要成本仍是每根 Bar 的策略 callback 与策略逻辑；SMA 已在 init 阶段一次性
预计算，`on_bar(s)` 只读取对齐数组，不在热路径重复滚动计算。
