# 双均线 SMA 计算方式评测报告

## 1. 评测目标

评估 Rust 驱动 Bar 事件循环下，两种 `ta-numba` SMA 使用方式对双均线策略性能的影响：

1. **方式 A：`on_bar` 逐 Bar 计算**
   每次回调读取最近 50 根历史 Bar，构造输入数组，并分别调用一次短周期和长周期
   `ta_numba.trend.sma_numba`。
2. **方式 B：`init` 全量预计算**
   初始化阶段对完整 close 序列各调用一次 `sma_numba`，生成对齐的短、长 SMA 数组；
   `on_bar` 只读取当前和前一个索引。

本报告重点评估稳定运行阶段的策略热路径。Numba callback bridge 只创建一次，不计入每根
Bar 的计算时间。

## 2. 测试环境

| 项目 | 配置 |
|---|---|
| 处理器 | Apple M1 Pro |
| 操作系统 | Darwin 25.5.0, arm64 |
| Python | 3.11.5 |
| NumPy | 2.2.6 |
| Numba | 0.61.2 |
| ta-numba | 0.4.0 |
| Polars | 1.32.3 |
| Rust toolchain 配置 | 1.94.0 |
| 代码基准提交 | `16f4b13` 加当前未提交修改 |

## 3. 数据与策略参数

| 项目 | 值 |
|---|---:|
| 输入文件 | `data/AAPL_1m_all_sources.parquet` |
| 数据来源 | `polygon_s3` |
| Bar 周期 | 1 分钟 |
| Bar 数量 | 264,190 |
| 短均线周期 | 20 |
| 长均线周期 | 50 |
| 下单数量 | 1 |
| 成交方式 | NextOpen market execution |
| 历史容量 | 51 |

测试先完成 Numba JIT 预热，然后在同一进程、同一个已编译 callback bridge 下重复执行，
使用稳定样本的中位数。两种方式使用相同的 Rust Bar 事件源、撮合规则、交叉判断和下单逻辑。

## 4. 两种实现

### 4.1 方式 A：`on_bar` 逐 Bar 调用 `sma_numba`

每根 Bar 执行以下工作：

```python
values = np.empty(long_period + 1, dtype=np.float64)
for index in range(long_period):
    values[index] = closes[index - long_period]
values[long_period] = current_close

short_values = sma_numba(values[-(short_period + 1):], short_period, short_period)
long_values = sma_numba(values, long_period, long_period)
```

策略最终只使用两个输出数组的最后两个值，但每根 Bar 都需要重新分配输入和输出数组。

### 4.2 方式 B：`init` 全量预计算

初始化阶段一次性执行：

```python
short_ma = sma_numba(closes, short_period, short_period)
long_ma = sma_numba(closes, long_period, long_period)
```

`on_bar` 热路径只读取当前与前一个位置：

```python
previous_short = short_ma[bar_index - 1]
current_short = short_ma[bar_index]
previous_long = long_ma[bar_index - 1]
current_long = long_ma[bar_index]
```

当前实现位于：

- `py-hftbacktest/hftbacktest/strategies/dual_ma.py`
- `examples/dual_ma_bar_backtest.py`

## 5. 正确性结果

两种方式的输出完全一致：

| 指标 | 结果 |
|---|---:|
| 最终短均线 | 309.739210 |
| 最终长均线 | 309.528284 |
| 金叉次数 | 3,012 |
| 死叉次数 | 3,012 |
| 买单数量 | 3,012 |
| 卖单数量 | 3,011 |
| 累计成交数量 | 6,023 |
| 最终持仓 | 1 |
| 下单错误 | 0 |

这说明将 SMA 从逐 Bar 批量调用移动到初始化阶段，没有改变交叉信号和撮合结果。

## 6. 性能结果

### 6.1 callback 净运行性能

| 指标 | 方式 A：逐 Bar SMA | 方式 B：init 预计算 | 改善 |
|---|---:|---:|---:|
| 264,190 Bar 总耗时 | 139.897 ms | 19.066 ms | 7.34 倍 |
| 每 Bar 平均耗时 | 529.5 ns | 72.2 ns | 7.34 倍 |
| 吞吐量 | 约 1.89M bars/s | 约 13.86M bars/s | 7.34 倍 |

这里的方式 B 时间包含 Rust Bar 推进、Numba callback、交叉判断、下单和成交处理，但不包含
一次性的 SMA 初始化。

### 6.2 初始化成本

| 场景 | 方式 B SMA 初始化耗时 |
|---|---:|
| 首次调用，包含 `sma_numba` JIT | 67.693 ms |
| JIT 后稳定调用 | 32.171 ms |

稳定状态下，如果一次回测同时计入预计算与事件运行：

```text
32.171 ms init + 19.066 ms runtime = 51.237 ms
```

相对于方式 A 的 139.897 ms，包含指标初始化后仍提升约 **2.73 倍**。如果相同数据和周期的
SMA 数组可以在多个参数组合之间复用，则后续回测只承担约 19 ms 的事件运行成本。

### 6.3 方式 A 的 `on_bar` 耗时分解

| 过程 | 增量耗时 | `on_bar` 计算占比 |
|---|---:|---:|
| 查找当前 Bar close | 1.2 ms | 1.0% |
| 获取历史视图 | 12.5 ms | 10.2% |
| 分配数组并复制 50 个历史 close | 46.0 ms | 37.4% |
| 两次 `sma_numba` | 61.6 ms | 50.1% |
| 交叉判断、持仓、下单和成交 | 1.7 ms | 1.4% |
| `on_bar` 净计算 | 123.1 ms | 100% |

数组构造和两次批量 SMA 合计占 `on_bar` 计算约 **87.5%**，是方式 A 的主要瓶颈。

## 7. 内存与分配行为

### 方式 A

每根 Bar 最多分配三个数组：

- 51 个 `float64` 的 SMA 输入数组；
- 21 个 `float64` 的短 SMA 输出数组；
- 51 个 `float64` 的长 SMA 输出数组。

264,190 根 Bar 对应约 792,570 次数组分配。仅数组数据区的累计分配流量约 260 MB，另外还要
复制约 106 MB 的历史 close 数据。实际峰值内存不等于累计分配量，但高频分配会增加热路径成本。

### 方式 B

初始化后长期保存两条完整 SMA 数组：

```text
264,190 × 8 bytes × 2 ≈ 4.03 MiB
```

`on_bar` 不再创建 SMA 输入或输出数组。方式 B 用约 4 MiB 常驻内存换取显著降低的逐 Bar
计算和分配成本。

## 8. 回测与实盘适用性

### 回测

方式 B 更适合完整 Bar 文件回测：数据在启动前已经存在，指标可以一次批量计算，策略事件仍按
时间顺序只读取当前和过去的值。

当前实现通过单调 `bar_index` 只访问：

- `bar_index - 1`；
- `bar_index`。

尽管完整指标数组在内存中已经存在，策略逻辑不得读取未来索引。后续可以把预计算数组封装成只暴露
当前位置的 `IndicatorView`，从结构上进一步限制前视访问。

### 实盘

方式 B 不能直接用于实盘，因为未来 close 序列不存在。实盘应使用 O(1) streaming SMA：

```python
sum = sum + new_close - expired_close
sma = sum / period
```

`ta_numba.stream.SMA` 当前使用 Python 对象和 `deque`，不能直接嵌入本项目的 Numba nopython
`on_bar(s)` 热路径。要保持策略代码一致，建议后续提供统一的指标访问接口：

- 回测后端：从预计算数组读取当前值；
- 实盘后端：从滚动指标状态读取当前值；
- 策略侧：使用相同的当前/前值访问语义，不感知指标来源。

## 9. 端到端执行说明

直接启动示例文件的首次进程执行结果：

```text
real 6.34 s
user 5.30 s
sys  0.51 s
```

这个时间包含 Python 进程启动、Numba/Polars/扩展模块导入、Parquet 加载以及首次 JIT，不能代表
稳定的 `on_bar` 性能。已导入模块后的 Parquet 加载约 0.05～0.16 秒；预转换 NPY 加载约
0.007 秒。

## 10. 测试结果

完整 Python 测试：

```text
Ran 23 tests in 10.010s
OK (skipped=1)
```

跳过项依赖外部市场数据 fixture，与双均线策略无关。`git diff --check` 通过。

## 11. 结论

对于静态 Bar 文件回测，应采用 **方式 B：init 全量预计算**：

- 信号和成交结果与逐 Bar SMA 完全一致；
- `on_bar` callback 净性能提升约 7.34 倍；
- 包含稳定状态指标初始化后，单次回测仍提升约 2.73 倍；
- 同一指标结果复用于多个回测时，收益更明显。

方式 A 保留逐事件计算语义，但批量 `sma_numba` API 会在每根 Bar 重复分配和复制，不适合作为
高性能 streaming 指标实现。实盘应增加 Numba 原生的 O(1) streaming SMA 后端，并通过统一
指标访问接口保持回测与实盘策略代码一致。
