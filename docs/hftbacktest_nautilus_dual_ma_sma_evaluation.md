# HFTBacktest 与 Nautilus Trader 双均线 SMA 生命周期评测报告

## 1. 报告目的

本报告合并以下两份现有评测：

- [Titan/HFTBacktest SMA 评测](dual_ma_sma_evaluation.md)
- [Nautilus Trader SMA 生命周期评测](/Users/dominolu/dev/nautilus_trader/backtest_reports/dual_ma_sma_lifecycle_benchmark_report.md)

目标是比较两个系统中双均线策略的 SMA 生命周期设计、热路径和实测性能，并明确哪些数字可以
直接比较、哪些只能作为系统级观察。

## 2. 首要口径结论

两份报告中的“在 init 中处理 SMA”不是同一种实现：

| 系统与方案 | 初始化阶段发生的事情 | Bar 到达时发生的事情 |
|---|---|---|
| HFTBacktest：逐 Bar SMA | 不预计算 SMA 数值 | 复制历史窗口并调用两次 `sma_numba` |
| HFTBacktest：`init` 预计算 | 对完整 close 序列计算两条 SMA 数组 | 按 Bar 索引直接读取 SMA 数值 |
| Nautilus：`on_start` 创建 | 在 `on_start` 创建两个指标对象 | 引擎逐 Bar 更新两个 SMA 对象 |
| Nautilus：`__init__` 创建 | 在策略 `__init__` 创建两个指标对象 | 引擎仍然逐 Bar 更新两个 SMA 对象 |

因此：

- HFTBacktest 的 `init` 是**指标数值预计算**；
- Nautilus 的 `__init__` 是**指标对象提前实例化**，不是指标数值预计算。

Nautilus 两种方案只改变对象创建时点，没有把 264,190 根 Bar 的 SMA 计算移出回测热路径。
不能把 Nautilus `__init__` 方案当成 HFTBacktest `init` 预计算方案的等价实现。

## 3. 共同数据与策略参数

两套评测使用相同的 AAPL Polygon 1 分钟 Bar 数据：

| 项目 | 值 |
|---|---|
| 标的 | AAPL |
| 来源 | Polygon / `polygon_s3` |
| Bar 周期 | 1 分钟 |
| 时间范围 | 2021-08-02 09:30 UTC 至 2026-08-21 19:59 UTC |
| Bar 数量 | 264,190 |
| 短 SMA | 20 |
| 长 SMA | 50 |
| 金叉数量 | 3,012 |
| 死叉数量 | 3,012 |

虽然行情与信号参数相同，但交易和运行时配置并不完全相同：

| 项目 | HFTBacktest | Nautilus Trader |
|---|---|---|
| 目标仓位 | 1 | 100 股 |
| 成交模型 | 简化 NextOpen market execution | Nautilus BacktestEngine 执行链 |
| 账户/组合 | 精简 position 状态 | CASH / NETTING、缓存和投资组合 |
| 风控 | 当前 Bar runtime 精简命令路径 | 风控引擎 bypass，但保留引擎事件链 |
| 回测结束持仓 | 保留 1 单位多仓 | 策略生命周期配置可能触发结束平仓 |
| 订单数量 | 6,023 | 6,024 |

订单数差异说明两套测试的停止生命周期或最终平仓语义不同，不能把总耗时差异全部归因于 SMA。

## 4. 测试环境

两套测试都运行于 Apple M1 Pro，但软件环境不同：

| 项目 | HFTBacktest | Nautilus Trader |
|---|---|---|
| 操作系统 | Darwin 25.5.0 arm64 | Apple M1 Pro 环境 |
| Python | 3.11.5 | 3.13.2 |
| Numba | 0.61.2 | 不适用于原生 `SimpleMovingAverage` 方案 |
| ta-numba | 0.4.0 | 未使用 |
| NautilusTrader | 不适用 | 1.225.0 |
| Rust toolchain 配置 | 1.94.0 | Nautilus 自身构建环境 |

## 5. HFTBacktest 评测结果

### 5.1 方案 H-A：每次 `on_bar` 计算 SMA

每根 Bar：

1. 获取当前 close；
2. 获取 Rust 历史视图；
3. 分配 51 元素输入数组并复制 50 个历史 close；
4. 调用短周期 `sma_numba`；
5. 调用长周期 `sma_numba`；
6. 判断交叉并下单。

callback bridge 只初始化一次后的结果：

| 指标 | 结果 |
|---|---:|
| 264,190 Bar 总耗时 | 139.897 ms |
| 每 Bar | 529.5 ns |
| 吞吐量 | 1,888,461 Bar/s |

`on_bar` 净计算约 123.1 ms，其中数组构造和两次批量 `sma_numba` 合计约占 87.5%。

### 5.2 方案 H-B：`init` 预计算完整 SMA 数组

初始化阶段：

```python
short_ma = sma_numba(closes, 20, 20)
long_ma = sma_numba(closes, 50, 50)
```

`on_bar` 阶段：

```python
previous_short = short_ma[index - 1]
current_short = short_ma[index]
previous_long = long_ma[index - 1]
current_long = long_ma[index]
```

结果：

| 指标 | 结果 |
|---|---:|
| SMA 稳定初始化耗时 | 32.171 ms |
| 仅 callback/event runtime | 19.066 ms |
| runtime 每 Bar | 72.2 ns |
| runtime 吞吐量 | 13,856,603 Bar/s |
| init + runtime | 51.237 ms |
| init + runtime 等效吞吐量 | 5,156,235 Bar/s |

相对方案 H-A：

- `on_bar`/event runtime 提升 7.34 倍；
- 单次回测包含指标初始化后提升 2.73 倍；
- 相同 SMA 数组复用于多轮回测时，后续轮次接近 19 ms。

### 5.3 HFTBacktest 正确性

两种方式结果一致：

| 指标 | 结果 |
|---|---:|
| 最终短 SMA | 309.739210 |
| 最终长 SMA | 309.528284 |
| 金叉 / 死叉 | 3,012 / 3,012 |
| 买单 / 卖单 | 3,012 / 3,011 |
| 累计成交数量 | 6,023 |
| 最终持仓 | 1 |
| 下单错误 | 0 |

完整 Python 测试为 `23 tests OK`，另有 1 项外部 fixture 测试跳过。

## 6. Nautilus Trader 评测结果

### 6.1 方案 N-A：在 `on_start` 创建指标对象

100 次 `engine.run()`：

| 指标 | 结果 |
|---|---:|
| 总 Bar 数量 | 26,419,000 |
| 纯回测总耗时 | 2,668.369 s |
| 单轮平均 | 26.684 s |
| 单轮中位数 | 27.372 s |
| P95 | 29.441 s |
| 最快 / 最慢 | 23.454 / 31.264 s |
| 标准差 | 2.122 s |
| 平均吞吐量 | 9,900.80 Bar/s |
| 数据及引擎初始化 | 0.829 s |
| 99 次 reset | 1.796 s |

计时只覆盖 `engine.run()`；数据读取、引擎构造和策略注册不包含在单轮时间内。

### 6.2 方案 N-B：在 `__init__` 创建指标对象

一次完整运行：

| 指标 | 结果 |
|---|---:|
| 回测耗时 | 27.940 s |
| 吞吐量 | 9,455.74 Bar/s |
| 数据及引擎初始化 | 0.766 s |
| 引擎迭代 | 264,190 |
| 金叉 / 死叉 | 3,012 / 3,012 |
| 订单 | 6,024 |

相对 N-A 的 100 次分布，27.940 秒：

- 比平均值高 4.71%；
- 比中位数高 2.08%；
- 低于 P95；
- 位于 23.454～31.264 秒正常范围内。

因为 N-B 只有一个样本，现有数据不能证明指标对象移到 `__init__` 后变快或变慢。

### 6.3 Nautilus 生命周期语义

当前 `DualMa` 在 `__init__` 中构造：

```python
self.short_ma = SimpleMovingAverage(config.short_period)
self.long_ma = SimpleMovingAverage(config.long_period)
```

随后在 `on_start` 注册：

```python
self.register_indicator_for_bars(self.config.bar_type, self.short_ma)
self.register_indicator_for_bars(self.config.bar_type, self.long_ma)
```

引擎在每次 `on_bar` 之前使用当前 Bar 更新两个指标。`on_bar` 读取 `short_ma.value` 和
`long_ma.value`。因此 N-A 与 N-B 的逐 Bar 指标计算量基本相同，性能接近是符合预期的。

## 7. 合并性能视图

| 系统/方案 | SMA 数值计算位置 | 计时边界 | 单轮时间 | 吞吐量 |
|---|---|---|---:|---:|
| H-A：HFT 逐 Bar SMA | `on_bar` 内两次批量调用 | callback + Rust Bar runtime | 0.139897 s | 1.89M/s |
| H-B：HFT 预计算，runtime | init 已完成 | callback + Rust Bar runtime | 0.019066 s | 13.86M/s |
| H-B：HFT 预计算，总计算 | init + callback runtime | SMA init + runtime | 0.051237 s | 5.16M/s |
| N-A：Nautilus `on_start` 对象 | 引擎逐 Bar 增量更新 | `engine.run()` | 平均 26.684 s | 9.90K/s |
| N-B：Nautilus `__init__` 对象 | 引擎逐 Bar增量更新 | `engine.run()` | 27.940 s | 9.46K/s |

按已观测的墙钟数字计算，N-A 平均值相对于：

- H-A 慢约 191 倍；
- H-B 仅 runtime 慢约 1,400 倍；
- H-B 包含 SMA init 后慢约 521 倍。

这些倍率是**当前两个完整方案的系统级观测值**，不是纯 SMA 算法或底层回测引擎的等价基准。

## 8. 为什么不能把跨系统倍率解释成单一引擎速度

### 8.1 计时边界不同

HFTBacktest 数字来自已准备 callback bridge 后的精简 materialized Bar runtime；Nautilus 数字来自
完整 `engine.run()`，包含数据引擎、策略、指标、缓存、投资组合、订单、执行和生命周期事件链。

### 8.2 SMA 算法生命周期不同

H-B 把完整数据的 SMA 数值计算移到事件循环之前；N-A/N-B 都在引擎运行中逐 Bar 更新 SMA。
H-B 因此减少的不只是对象创建，而是 264,190 次指标更新工作。

### 8.3 策略调用模型不同

HFTBacktest 使用 Rust 直接调用 Numba C ABI `on_bar(s)`；Nautilus 示例使用策略对象的 Python
`on_bar(bar)`，并通过完整引擎组件分发事件。

### 8.4 交易生命周期不同

两套测试的目标数量、最终持仓、订单总数、账户和结束处理并不一致。订单事件链成本也不能直接归一化。

### 8.5 样本设计不同

HFTBacktest 使用同一 bridge 下的多次稳定样本中位数；N-A 有 100 次长时间运行，但存在明显时间趋势；
N-B 只有 1 次。跨报告缺少完全对称的交替 A/B 采样。

## 9. 架构层面的结论

### 9.1 对 HFTBacktest

静态 Bar 文件回测应优先采用完整 SMA 数组预计算：

- `on_bar` 只做 O(1) 索引读取；
- 避免每根 Bar 分配三个数组；
- 避免重复复制历史窗口；
- 相同数据、周期的指标数组可以跨策略参数组合复用。

为了防止前视访问，预计算数组后续应封装为只暴露当前值和历史值的 `IndicatorView`，而不是让策略
任意索引完整未来数组。

### 9.2 对 Nautilus Trader

把指标对象从 `on_start` 移到 `__init__` 改善的是生命周期清晰度和 reset 复用，不会明显减少
`engine.run()` 的逐 Bar 工作。如果目标是测试真正的指标预计算，需要另做 N-C 方案：

1. 在运行前生成完整 `short_ma`、`long_ma` 数组；
2. `on_bar` 根据严格单调索引读取当前/前一个值；
3. 禁止未来索引；
4. 保持现有订单、投资组合和执行链不变。

只有 N-C 才与 H-B 的 SMA 生命周期相同。

### 9.3 对回测与实盘一致性

完整数组预计算只能用于历史数据已知的回测。实盘应使用 O(1) streaming SMA。要保持相同策略代码，
建议抽象统一指标访问语义：

```text
策略 on_bar
    └── SMA view: current / previous / history
            ├── 回测后端：预计算数组 + 当前游标
            └── 实盘后端：滚动状态 + 环形历史
```

策略只访问“当前可见”的指标值，指标后端由运行模式选择。

## 10. 建议的公平跨系统复测方案

要得到可解释的 HFTBacktest/Nautilus 性能对比，应统一以下条件：

1. 使用完全相同的 264,190 根 Bar 和时间戳；
2. 统一目标仓位、最终平仓规则和预期订单数；
3. 两边都实现逐 Bar streaming SMA 与完整数组预计算 SMA；
4. 分开报告数据加载、指标初始化、引擎初始化和 `run` 时间；
5. 两边均提前完成 JIT/扩展预热；
6. 每个方案至少运行 30～100 次；
7. 采用交替执行顺序，避免温度、调度和缓存时间趋势；
8. 同时报告中位数、P95、标准差和置信区间；
9. 单独提供无订单、固定订单数量两个版本，区分行情/策略成本与交易事件链成本；
10. 明确报告“精简策略 runtime”还是“完整交易系统 engine”的性能。

## 11. 最终结论

1. 两个系统在相同数据上均得到 3,012 次金叉和 3,012 次死叉，核心信号语义一致。
2. HFTBacktest 真正把 SMA 数值移到 init 预计算后，callback runtime 从 139.897 ms 降至
   19.066 ms；包含稳定初始化后为 51.237 ms。
3. Nautilus 的 `__init__` 修改只移动指标对象创建时点，SMA 仍逐 Bar 更新，因此 27.940 秒处于原
   100 次基准的正常波动范围内。
4. 当前跨系统墙钟差距同时包含指标生命周期、事件模型和交易系统功能范围差异，不能表述为纯引擎
   或纯 SMA 性能差距。
5. 下一步最有价值的对称实验，是在 Nautilus 中增加真正的完整 SMA 数组预计算方案 N-C，并统一
   两套系统的订单和停止语义后重新测量。
