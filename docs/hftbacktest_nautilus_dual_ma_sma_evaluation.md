# HFTBacktest（Rust + Numba Python）与 Nautilus Trader 双均线 SMA 评测

## 评测缘起

在做这次测试之前，我原本认为 HFTBacktest 和 Nautilus Trader 都采用了 Rust + Python 架构，性能
差异应该不会太大。但真正使用相同的 Bar 数据和双均线策略进行测试后，两者的回测速度竟然相差两个
数量级以上。这个结果促使我重新研究两套系统的执行链，而不是只看它们都使用了 Rust 这一表面特征。

这次评估源于我最近一直使用 Qlib 进行模型训练，并在训练完成后使用 Nautilus Trader 做策略回测。
随着使用时间增加，我逐渐感觉回测阶段的速度偏慢。此前做做市策略回测时，我接触过 HFTBacktest；
原版 HFTBacktest 主要面向 Tick 级高频回测，没有我需要的 Bar 级事件回测，因此当时无法直接用于
Qlib 模型的 Bar 级策略验证。

在深入研究 Rust 调用 Python 的机制后，我发现两者虽然都可以称为 Rust + Python，但热路径存在本质
区别：HFTBacktest 使用 Numba 将 Python 策略编译为原生机器码，再由 Rust 通过 C ABI 直接调用；
Nautilus Trader 则将 Rust BacktestEngine 通过 PyO3 暴露给 Python，运行时仍然逐事件回调普通
Python 策略。前者的策略热路径不再经过 CPython 解释器，后者仍受 Python 字节码、对象模型、PyO3
边界和 GIL 串行化影响。这正是本文进一步改造 HFTBacktest 并开展对比评测的直接原因。

## 项目背景

在进行这次评测之前，我对原版 HFTBacktest 做了一次运行时级重构：将原来由策略主动调用
`elapse()` 推进回测时钟的循环接口，改造成由 Rust 调度器统一驱动的事件回调模型。策略不再拥有或
实现事件循环，只需要编写 Numba `nopython` 的 `on_tick(s)` 和 `on_bar(s)`；Rust 负责行情推进、
Tick/Bar 批次、事件顺序、历史数据、订单撮合和账户状态，并通过稳定的 C ABI 主动调用策略。这套接口
同时用于回测和实盘，使同一份策略代码不需要因为运行模式不同而重写主循环。

在实盘侧，我进一步抽象了统一 Broker API，并将交易所连接器拆成独立进程，通过 iceoryx2 共享内存
与策略运行时传递归一化行情、订单和成交事件。目前已经增加 **Binance Spot、Binance Futures、
Bybit、OKX 和 Hyperliquid** 等主流加密交易所连接器。也就是说，这次改造不只是给 HFTBacktest 增加
两个回调函数，而是把它扩展成了“Rust 统一事件内核 + Numba 策略回调 + 多 Broker 实盘连接器”的
回测与实盘一体化架构。本文后续的性能比较，正是基于这套新的 Rust + Numba 回调运行时。

## 1. 评测目的

本文比较两种双均线回测架构：

- **HFTBacktest**：Rust 驱动回测循环，通过 C ABI 直接调用 Numba 编译后的 Python 策略；
- **Nautilus Trader**：Rust BacktestEngine 通过 PyO3 暴露给 Python，并在事件循环中回调 Python 策略。

两者都使用 Rust 实现核心能力，真正的本质区别不是“Rust 与 Python”本身，而是：**每根 Bar 到达时，
策略热路径是在 Numba 原生机器码中执行，还是必须回到 CPython 解释器中执行。**

## 2. 核心结果

在相同的 264,190 根 AAPL 1 分钟 Bar 上：

| 实现 | 单轮耗时 | 单 Bar 等效成本 | 吞吐量 |
|---|---:|---:|---:|
| HFTBacktest（含 SMA 预计算） | 51.237 ms | 约 194 ns | 5.16M Bar/s |
| Nautilus Trader | 26.684 s（100 次平均） | 约 101 μs | 9.90K Bar/s |

按当前实测墙钟时间，Nautilus 约慢 **521 倍**。

这个倍率是两个具体实现的系统级结果，不是 PyO3 本身的固定开销，也不能简单表述为“GIL 导致慢
521 倍”。更准确的解释是：Nautilus 每根 Bar 都经过 PyO3 进入 Python 策略，受 CPython 字节码、
Python 对象、跨语言调用和 GIL 串行化共同影响；HFTBacktest 则始终停留在 Rust 与 Numba 原生代码
组成的热路径中。

## 3. 数据与配置

| 项目 | 值 |
|---|---|
| 标的 | AAPL |
| 数据来源 | Polygon / `polygon_s3` |
| Bar 周期 | 1 分钟 |
| 时间范围 | 2021-08-02 至 2026-08-21 |
| Bar 数量 | 264,190 |
| 短 / 长 SMA | 20 / 50 |
| 金叉 / 死叉 | 3,012 / 3,012 |
| 测试机器 | Apple M1 Pro |

交易配置并未完全对齐：HFTBacktest 使用 1 单位仓位、精简 NextOpen 撮合和 position 状态，共产生
6,023 个订单；Nautilus 使用 100 股仓位、CASH / NETTING 账户和完整执行链，共产生 6,024 个订单。
因此，本文重点比较当前两套实现的实测数据，并明确数字适用的边界。

## 4. 测试方法与计时口径

| 项目 | HFTBacktest | Nautilus Trader |
|---|---|---|
| 运行方式 | Numba JIT 预热后，在同一已编译 callback bridge 下重复测试 | 同一已加载数据的引擎连续运行 100 次 |
| 统计口径 | 稳定样本中位数 | 100 次均值、中位数、P95、极值和标准差 |
| 主计时范围 | SMA 预计算 + Rust/Numba event runtime | `engine.run()` |
| 不计入 | 进程启动、模块导入、数据加载、首次 JIT | 数据读取、引擎构造、策略注册 |
| 轮次复用 | callback bridge 和已加载数据 | 已加载数据和引擎，轮次间执行 `reset()` |

HFTBacktest 的主比较时间为 51.237 ms，已经包含稳定状态下的 SMA 预计算；Nautilus 采用 100 次
`engine.run()` 的平均值 26.684 s。选择这个口径，是为了避免只拿 HFTBacktest 的 19.066 ms 热路径
与 Nautilus 完整运行时间比较，从而人为放大差距。

## 5. 性能评测结果

### 5.1 整体性能

| 指标 | HFTBacktest | Nautilus Trader | 对比 |
|---|---:|---:|---:|
| 单轮 Bar | 264,190 | 264,190 | 相同 |
| 主比较耗时 | 51.237 ms | 26.684 s | Nautilus 约慢 521 倍 |
| 单 Bar 等效成本 | 约 194 ns | 约 101 μs | Nautilus 约高 521 倍 |
| 吞吐量 | 5,156,235 Bar/s | 9,900.80 Bar/s | HFTBacktest 约高 521 倍 |

26.684 s ÷ 0.051237 s ≈ 520.8，因此正文将主结果取整为 **521 倍**。这三个指标来自同一组耗时，
分别从单轮时间、单事件成本和吞吐量三个角度表达相同结果，并不是三项独立加速。

### 5.2 HFTBacktest 耗时拆分

| 阶段 | 耗时 | 占合计时间 | 说明 |
|---|---:|---:|---|
| SMA 稳定初始化 | 32.171 ms | 62.8% | 两条完整 SMA 数组批量计算 |
| Rust + Numba runtime | 19.066 ms | 37.2% | Bar 推进、信号、下单与精简成交处理 |
| 合计 | 51.237 ms | 100% | 本文主比较口径 |

HFTBacktest 仅 runtime 为 72.2 ns/Bar、约 13.86M Bar/s。如果相同数据与 SMA 参数用于多轮模型或
策略参数评估，预计算数组可以复用，后续单轮接近 19 ms。但这个场景相对 Nautilus 平均耗时约
1,400 倍的数字不作为主结论，因为它排除了 HFTBacktest 的指标初始化。

### 5.3 Nautilus 100 次运行分布

| 指标 | 结果 |
|---|---:|
| 总运行次数 | 100 |
| 总处理 Bar | 26,419,000 |
| `engine.run()` 总耗时 | 2,668.369 s |
| 单轮平均 | 26.684 s |
| 单轮中位数 | 27.372 s |
| P95 | 29.441 s |
| 最快 / 最慢 | 23.454 / 31.264 s |
| 标准差 | 2.122 s |
| 平均吞吐量 | 9,900.80 Bar/s |
| 数据及引擎初始化 | 0.829 s |
| 99 次 `reset()` | 1.796 s |

即使使用 Nautilus 最快一轮 23.454 s 与 HFTBacktest 的完整 51.237 ms 比较，差距仍约为 **458 倍**；
使用中位数比较约为 **534 倍**。因此，521 倍不是由 Nautilus 某一次异常慢的样本造成，而是处于其
100 次运行分布的正常范围内。

### 5.4 信号与订单结果

| 指标 | HFTBacktest | Nautilus Trader |
|---|---:|---:|
| 金叉 | 3,012 | 3,012 |
| 死叉 | 3,012 | 3,012 |
| 订单 | 6,023 | 6,024 |
| 最终短 SMA | 309.739210 | 未纳入原报告汇总 |
| 最终长 SMA | 309.528284 | 未纳入原报告汇总 |
| 下单错误 | 0 | 未报告 |

两边得到完全相同的交叉信号数量，说明核心双均线信号语义一致。订单数相差 1，来自目标仓位、停止
生命周期或最终平仓规则的差异，而不是信号缺失。

## 6. 数据结果如何解释

前文已经说明两套系统的调用架构，这里只保留与数字归因直接相关的差异：

| 影响项 | HFTBacktest | Nautilus Trader |
|---|---|---|
| 策略热路径 | Numba 原生机器码 | CPython 策略回调 |
| Rust/Python 边界 | C ABI 单指针 | PyO3 Python 方法与对象边界 |
| SMA | 运行前批量预计算 | 运行中逐 Bar 更新 |
| 执行链 | 精简 NextOpen、命令和 position 状态 | 完整订单、执行、缓存、账户和组合状态 |

当前数据足以证明两套**具体实现**在该策略上相差约 521 倍，但不足以把 521 倍精确分配给 GIL、PyO3、
SMA 或订单系统。尤其需要注意：

- 这是完整实现路径的比较，不是两个 Rust 撮合函数的微基准；
- GIL 限制 Python 回调并行扩展，但单线程下还存在字节码、对象和边界成本；
- Nautilus 维护的交易系统状态更多，HFTBacktest 当前 Bar 路径更精简；
- 两边的订单数量、目标仓位和结束语义没有完全对齐。

## 7. 结论

在本次 AAPL 双均线回测中，HFTBacktest 处理 264,190 根 Bar 的完整时间为 51.237 ms，Nautilus
Trader 的 100 次平均为 26.684 s，实测差距约 **521 倍**。即使选择 Nautilus 最快样本，差距仍约
458 倍，说明结果不是单次运行波动造成的。

这个结果验证了此次 HFTBacktest 改造的价值：当模型训练完成后需要反复执行 Bar 级策略验证、参数
搜索或滚动回测时，Rust + Numba callback 可以显著压缩回测阶段耗时。

若要进一步定量拆分性能来源，下一步应分别测试 Nautilus 纯 Rust 策略、空 Python `on_bar`、统一的
预计算 SMA，以及完全相同的订单和账户语义。完成这些控制实验后，才能回答 521 倍中各组件分别贡献
了多少。
