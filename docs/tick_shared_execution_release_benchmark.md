# Tick 共享执行迁移 Release 性能基线

## 目的

验证 P0-B 中 L2/L3 matcher 增加 exchange-time `OutcomeBus` 后，纯 Tick 热路径相对
`NoopExecutionObserver` 冻结基线的吞吐下降不超过 3%（REQ-PERF-001）。

## 方法

- 二者使用相同的 `NoPartialFillExchange`、`RiskAdverseQueueModel`、HashMap depth、Reader
  和 100 万条 L2 depth 更新；
- 每轮包含一个市价单，以覆盖 outcome 生成和 drain；
- 唯一实验变量是 matcher observer：Noop 对照组与 OutcomeBus 实验组；
- A/B 每轮交替执行次序，避免 CPU 频率和温度漂移系统性偏向一组；
- release 编译，5 轮 warmup，30 轮计时；详细审计关闭；
- benchmark 源码：`examples/src/bin/tick_execution_benchmark.rs`。

命令：

```console
cargo build --release -p titan-examples --bin tick_execution_benchmark
target/release/tick_execution_benchmark --events 1000000 --runs 30 --warmup 5
```

## 环境

- Git 基线：`16f4b13` 加当前共享执行层工作区修改；
- CPU：Apple M1 Pro；
- OS：Darwin 25.5.0 arm64；
- Rust/Cargo：1.94.0；
- 日期：2026-08-23。

## 结果

| 样本 | Noop events/s | OutcomeBus events/s | 回归 |
|---|---:|---:|---:|
| 1 | 18,083,868 | 17,993,953 | 0.500% |
| 2 | 18,134,690 | 18,069,990 | 0.358% |
| 中位 | 18,109,279 | 18,031,972 | **0.429%** |

结论：0.429% 小于 3% 门槛，通过 P0-B release 性能验收。关键优化是在绝大多数不产生
订单结果的 market event 后先检查 `OutcomeBus::is_empty()`，避免每 Tick 进入可变
`VecDeque::pop_front` 路径。

## 最终实现复验（2026-08-24）

在 Tick exchange-arrival risk、post-trade liquidation 和可选执行真实性模型全部接入后，使用相同
参数重新执行三次 A/B：

| 样本 | Noop events/s | OutcomeBus events/s | 回归 |
|---|---:|---:|---:|
| 1 | 18,004,479 | 17,998,290 | +0.034% |
| 2 | 17,942,004 | 17,868,455 | +0.412% |
| 3 | 17,988,291 | 18,058,786 | -0.390% |

逐样本回归中位为 **+0.034%**，继续通过 3% 门槛。`OutcomeBus` 使用共享 pending 位完成空队列快速
判断；只有实际产生订单 outcome 时才访问 `VecDeque`。历史流动性与成交质量模型默认关闭，启用时仅在
`PartialFillExchange` 成交候选路径执行，不影响默认 Tick market-data 热路径。
