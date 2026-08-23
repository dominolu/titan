# Titan/HFTBacktest 与 Nautilus Trader 回测引擎功能对比清单

> 分析基线：当前工作区，2026-08-23。Nautilus 侧以
> `/Users/dominolu/dev/nautilus_trader/docs/concepts/backtest_engine_implementation_analysis.md`
> 为基准；Titan 侧以当前 Rust、Python/Numba runtime、Bar runtime、连接器和统计模块源码为准。

## 1. 定位结论

两个项目的目标不同：

| 项目 | 主要定位 |
|---|---|
| Titan/HFTBacktest | 面向 HFT 和加密货币永续合约的低延迟研究/实盘内核，重点是双时间戳、订单簿重放、订单延迟、队列位置和 Rust/Numba 原生热路径 |
| Nautilus Trader | 历史事件驱动的完整交易系统仿真平台，重点是统一 Data/Risk/Execution/Portfolio/Account 生命周期和广泛的交易品种、订单及编排能力 |

Titan 当前还包含两条能力边界明显不同的回测路径：

1. **Tick/L2/L3 回测引擎**：成熟主引擎，支持订单簿、延迟、队列、部分成交和多资产事件归并；
2. **Materialized Bar + Numba runtime**：新实现，使用保守 NextOpen 撮合，功能范围明显小于 Tick 主引擎。

因此，本清单分别标注 Titan Tick 引擎和 Titan Bar runtime，不能用 Tick 引擎的能力替代 Bar
runtime 的现状。

## 2. 状态标记

| 标记 | 含义 |
|---|---|
| ✅ | 已实现，能力基本对应 |
| 🟢 | Titan 在该 HFT 维度更直接或更强 |
| 🟡 | 部分实现、接口受限或只在某条运行路径实现 |
| 🔵 | 设计不同，不宜简单判定优劣 |
| 🚧 | 已设计或预留，但尚未接线 |
| ❌ | 当前未实现 |
| ➖ | 不适用或不是当前项目目标 |

## 3. 总体架构与事件循环

| 功能 | Nautilus Trader | Titan Tick/L2/L3 | Titan Bar/Numba | 结论 |
|---|---|---|---|---|
| Rust/原生核心 | Cython/PyO3/Rust 及 Python 组件组合 | ✅ 全 Rust 核心 | ✅ Rust 事件源 + Numba C ABI | Titan 热路径更精简 |
| 单线程确定性事件时间轴 | ✅ | ✅ `EventSet` 固定顺序 | ✅ 按 `(close_ts,timeframe,asset)` | 基本对应 |
| 完整交易内核组件 | Data/Risk/Execution/Cache/Portfolio/Trader/Emulator | ❌ 无独立完整组件管线 | ❌ | Nautilus 明显更完整 |
| 本地与交易所双时钟 | `ts_event`/`ts_init` | 🟢 `exch_ts`/`local_ts` 分别驱动 exchange/local processor | 🟡 当前 Bar 只按 `close_ts` 投递 | Titan Tick 是核心优势 |
| 行情、订单请求、订单响应统一调度 | ✅ | 🟢 四类事件：LocalData、LocalOrder、ExchData、ExchOrder | 🟡 Bar/Filled 两阶段 | Tick 路径对应且偏 HFT |
| 同时间戳固定优先级 | ✅，含 timer、exchange、data、command | ✅ 固定事件槽和 asset 顺序 | ✅ NextOpen fill 先于当前 Bar callback | 均可重复，但顺序语义不同 |
| 当前时间结算直到稳定 | ✅ 命令和撮合反复处理 | 🟡 订单事件按最早时间持续推进，无通用组件稳定循环 | 🟡 callback 后同步消费命令 | Nautilus 更通用 |
| Timer 调度 | ✅ 动态 timer，数据耗尽后仍可推进 | ❌ Rust Bot 只有主动 `elapse`/帧等待 | 🚧 ABI 有 `Timer`，Python 启动时拒绝 | Titan 重要缺口 |
| 启动/停止生命周期 | ✅ | 🟡 Rust `Strategy` trait 只有 tick/bar；Numba runtime 有 start/stop | ✅ `on_start/on_stop`，stop 保证一次 | 新 runtime 较完整 |
| Error 生命周期 | ✅ 组件错误处理 | 🟡 常规 Rust `Result` | ✅ `on_error` 后保证 `on_stop` | Bar/Numba 已实现 |
| Reset 后重复运行 | ✅ 完整 `reset()` | ❌ 通常重建 Backtest/Reader/strategy | ❌ 状态需调用方手动重建或清零 | Titan 批量研究缺口 |
| Streaming 连续回测生命周期 | ✅ `run(streaming=True)` + `end()` | 🟡 顺序文件块 Reader 连续读取 | ❌ MaterializedBarSource 一次持有全部记录 | Tick 部分支持 |
| 多引擎并发 | 同进程多 Node 不支持，顺序运行 | 🟡 独立实例可构建，但内部 `Rc/RefCell` 限制线程迁移 | 🟡 可多进程 | 两边都需明确编排 |

## 4. 历史数据和可见时间

| 功能 | Nautilus Trader | Titan Tick/L2/L3 | Titan Bar/Numba | 结论 |
|---|---|---|---|---|
| Bar | ✅ | 🟡 兼容 Rust frame 聚合路径 | ✅ 显式 materialized Bar | Titan 新 Bar 路径已落地 |
| Quote/BBO Tick | ✅ | ✅ BBO/深度事件标志和 MarketState | ❌ | Tick 支持 |
| Trade Tick | ✅ | ✅ | ❌ | Tick 支持 |
| L2 MBP 增量/快照 | ✅ | 🟢 完整重建与撮合 | ❌ | Titan 强项 |
| L3 MBO 增量/快照 | ✅ | 🟢 `L3FIFOQueueModel` | ❌ | Titan 强项 |
| Depth10 | ✅ 独立类型 | 🟡 可归一化成深度事件，没有独立 Depth10 类型 | ❌ | 数据模型不同 |
| Instrument 更新 | ✅ | ❌ 静态 tick/lot/asset 配置 | ❌ | Titan 缺口 |
| InstrumentStatus/Close | ✅ | ❌ | ❌ | Titan 缺口 |
| CustomData | ✅ | ❌ 固定 flat `Event` | 🚧 ABI 有 POD custom slot，但无数据源接线 | Titan 缺口 |
| Funding 数据 | 🟡 可用 CustomData/SimulationModule 扩展，源报告未列出内置 Funding 模型 | 🟡 实盘连接器有统一 Funding 事件，回测未结算 | 🚧 callback 槽存在但未接线 | 对永续合约是高优先级缺口 |
| 多流 K 路归并 | ✅ 最小堆 + priority | ✅ EventSet 合并资产、双时钟和订单事件 | 🟡 只合并已排序 Bar 记录 | 两者模型不同 |
| 自动排序 | ✅ `sort_data()` | ❌ 文件必须按调用方顺序提供 | ✅ Bar 启动时严格验证排序，不自动修复 | Titan 偏 fail-fast |
| 同时间戳自定义流优先级 | ✅ | ❌ 固定 asset/event slot 顺序 | ❌ 固定排序 | Titan 不可配置 |
| 行情可见时间 | `ts_init` | 🟢 `local_ts` 明确表示本地收到时间 | 🟡 默认 `close_ts`，Bar feed latency 未接入 | Tick 更精确 |
| Feed latency | 通过 `ts_init` 体现 | 🟢 原始 `exch_ts/local_ts` + latency offset | 🚧 设计存在，Bar 尚未实现 | Titan Tick 强项 |
| 文件格式 | Catalog/Parquet/自定义 client | NPY/NPZ flat Event | Parquet 和 flat NPY 示例入口 | Nautilus 数据生态更完整 |
| 大数据分块 | ✅ generator/catalog chunk | ✅ Tick Reader 按文件块加载、释放并可并行预取 | ❌ 当前 Bar 源复制并持有完整数组 | Bar 需要补齐 |
| mmap | Catalog/后端决定 | 🟡 当前 Reader 加载 NPY/NPZ；无统一 mmap API | 🟡 Python 可 mmap NPY，但 FFI 创建源时仍复制 | 尚未端到端零复制 |

## 5. 多市场、多品种和行情状态

| 功能 | Nautilus Trader | Titan Tick/L2/L3 | Titan Bar/Numba | 结论 |
|---|---|---|---|---|
| 多 Venue | ✅ 每 Venue 一个 SimulatedExchange | ✅ 每 asset 可配置独立 exchange model，StrategySpec 可分 market | 🟡 asset_no，无完整 Venue 对象 | Tick 对应，Bar 较弱 |
| 多 Instrument | ✅ | ✅ | ✅ | 均支持 |
| 跨市场联合回测 | ✅ | 🟢 全局 EventSet 和 TickBatch | 🟡 多资产/多周期 BarBatch | Titan 核心能力 |
| L1/L2/L3 book type | ✅ 显式类型和数据校验 | 🟡 L2/L3 为第一类；BBO 可用但无独立 L1 exchange model | ❌ | Nautilus 类型更完整 |
| 多种深度容器 | 内部订单簿实现 | 🟢 HashMap、ROI Vector、BTree、L3 | ❌ 只暴露 OHLCV | Titan 可针对 HFT 优化 |
| 数据不足时拒绝错误撮合 | ✅ L2/L3 缺对应数据会拒绝 | 🟡 由 builder 类型和输入约定保证，缺少统一能力审计 | ✅ Bar 不会伪装 Tick | Titan 还需统一 manifest 校验 |
| 市场状态/交易时段 | ✅ InstrumentStatus、交易状态 | ❌ | ❌ | Titan 缺口 |
| 动态订阅 | ✅ DataEngine/client | ❌ 回测数据启动前固定 | ❌ | Titan 缺口 |

## 6. Bar 回测能力

| 功能 | Nautilus Trader | Titan Bar/Numba | 结论 |
|---|---|---|---|
| 已完成 Bar 显式输入 | ✅ EXTERNAL Bar | ✅ `BAR_COMPLETE` 且拒绝 partial | 对应 |
| Bar 时间模型 | `ts_init` 决定可见性 | `[open_ts,close_ts)`，默认 close 投递 | Titan 定义明确，但缺 delivery latency |
| 多资产同周期批次 | 经 DataEngine 逐事件分发 | ✅ 同 close/timeframe 一次全局 BarBatch | Titan callback 边界更少 |
| 多周期 | ✅ | ✅；只有全局最小周期可撮合 | Titan 规则更保守 |
| Bar 历史环 | Indicator/cache/history 体系 | ✅ 固定容量 Rust 环，当前 Bar 回调后提交 | Titan 已避免当前 Bar 重复/前视 |
| Bar 内合成路径 | O-H-L-C / adaptive O-L-H-C | 🔵 不合成路径 | 设计不同 |
| 新订单同 Bar 成交 | 可按已更新市场状态在同时间戳处理 | 🔵 明确禁止；最早 NextOpen | Titan 更保守但模型更少 |
| NextOpen | 可通过策略/模型表达 | ✅ 第一版内置 | Titan 已实现 |
| Touch/Conservative OHLC | Bar path 可触发多类订单 | 🚧 文档设计，未实现 | Titan 缺口 |
| VolumeLimited | 可利用 Bar volume/Fill 模型近似 | 🚧 未实现 | Titan 缺口 |
| BID/ASK Bar 合成 Quote | ✅ | ❌ | Titan 缺口 |
| 空 Bar | 数据模型决定 | ✅ `BAR_EMPTY` 明确且不撮合 | Titan 语义清楚 |
| Bar 费用 | ✅ FeeModel/账户链 | ❌ 当前只更新 position | Titan 高优先级缺口 |
| Bar 延迟 | ✅ LatencyModel/命令队列 | ❌ | Titan 高优先级缺口 |
| Bar 部分成交 | ✅ 取决于模型/事件 | ❌ 全量成交或不成交 | Titan 当前简化 |
| Bar 订单事件 | 完整 order/fill/portfolio | 🟡 只有 `on_filled`；`on_order/on_position` 拒绝 | Titan callback 缺口 |
| Hybrid Bar 信号 + Tick 撮合 | 可在统一事件流组合 | 🚧 Titan 明确拒绝，精确合并器未实现 | Titan 高优先级缺口 |

## 7. Tick、订单簿和队列仿真

| 功能 | Nautilus Trader | Titan Tick/L2/L3 | 结论 |
|---|---|---|---|
| Tick-by-tick 回放 | ✅ | 🟢 核心能力 | 对应 |
| 全订单簿重建 | ✅ | 🟢 核心能力 | 对应 |
| L2 队列位置 | 可选 queue position | 🟢 RiskAdverse + 多种概率 QueueModel | Titan 内置模型更丰富 |
| L3 FIFO 队列 | ✅ L3 matching | 🟢 按市场订单 ID 重建 FIFO | Titan 强项 |
| 部分成交 | ✅ | ✅ `PartialFillExchange` | 对应 |
| 无部分成交快速模型 | 可由 FillModel/配置表达 | ✅ `NoPartialFillExchange` | Titan 提供专用高性能模型 |
| 历史流动性消耗 | ✅ 可选 `liquidity_consumption` | ❌ 无跨策略订单的独立消耗账本，历史簿不被永久改变 | Titan 缺口 |
| 被动单排队 | ✅ 可选 | 🟢 QueueModel 是核心配置 | Titan 强项 |
| 市场冲击 | ❌ 历史市场外生，只能压力建模 | ❌ 历史市场外生 | 共同限制 |
| 价格/数量精度验证 | ✅ 严格拒绝 | 🟡 价格转换为 tick 时 round，数量按 lot 参与模型；缺统一严格拒绝 | Nautilus 更严格 |

## 8. 订单类型和订单状态机

| 功能 | Nautilus Trader | Titan Tick/L2/L3 | Titan Bar/Numba | 结论 |
|---|---|---|---|---|
| Market | ✅ | ✅ 代码路径支持 | ✅ | 对应 |
| Limit | ✅ | ✅ | ✅ | 对应 |
| Market-to-Limit | ✅ | ❌ | ❌ | Titan 缺口 |
| Stop Market | ✅ | ❌ | ❌ | Titan 缺口 |
| Stop Limit | ✅ | ❌ | ❌ | Titan 缺口 |
| Market/Limit If Touched | ✅ | ❌ | ❌ | Titan 缺口 |
| Trailing Stop | ✅ | ❌ | ❌ | Titan 缺口 |
| GTC | ✅ | ✅ | ✅ | 对应 |
| IOC | ✅ | ✅ PartialFillExchange | ✅ 首个 eligible open 后处理 | Tick 对应，Bar 简化 |
| FOK | ✅ | ✅ PartialFillExchange | 🟡 Bar 无部分成交，等价于首个 open 全成或撤 | Bar 语义有限 |
| GTD | ✅ | ❌ | ❌ | Titan 缺口 |
| Post-only | ✅ | ✅ `GTX` | 🟡 枚举接受，但 NextOpen 模型无真实 maker queue | Tick 对应 |
| Reduce-only | ✅ | ❌ | ❌ | 永续交易的重要缺口 |
| 单笔撤单 | ✅ | ✅ | ✅ | 对应 |
| 批量/全部撤单 | ✅ | 🟡 策略遍历订单逐个撤；无统一 exchange 批量状态机 | ❌ callback 只有单笔 cancel | Titan 部分支持 |
| Modify | ✅ | 🟡 Tick 核心可修改；当前实盘和 Numba 公共 API 按需求禁用 | ❌，要求 cancel/replace | 有意设计差异 |
| OTO | ✅ | ❌ | ❌ | Titan 缺口 |
| OCO | ✅ | ❌ | ❌ | Titan 缺口 |
| OUO | ✅ | ❌ | ❌ | Titan 缺口 |
| Bracket | ✅ | ❌ | ❌ | Titan 缺口 |
| AT_OPEN/AT_CLOSE | Nautilus 当前也未实现 | ❌ | ❌ | 共同缺口 |
| 订单恢复/重建 | ✅ Cache 中活动订单可恢复 | ❌ 回测重建无恢复生命周期 | ❌ | Titan 缺口 |

## 9. Fill、Fee、Latency 和扩展模型

| 功能 | Nautilus Trader | Titan Tick/L2/L3 | Titan Bar/Numba | 结论 |
|---|---|---|---|---|
| Maker/Taker fee | ✅ | ✅ TradingValue/TradingQty fee | ❌ | Tick 对应 |
| Directional fee/交易税 | 可自定义 FeeModel | 🟢 内置 DirectionalFees | ❌ | Titan Tick 已支持 |
| 固定费用 | ✅ | ✅ FlatPerTrade | ❌ | Tick 对应 |
| Per-contract fee | ✅ | ✅ TradingQty 可表达 | ❌ | 基本对应 |
| 自定义 FeeModel | ✅ | ✅ Rust trait | ❌ Bar 尚未接入 | Tick 可扩展 |
| 固定订单延迟 | ✅ | ✅ ConstantLatency，分 entry/response | ❌ | Tick 对应 |
| 历史订单延迟 | 可自定义/模型 | 🟢 IntpOrderLatency 直接插值历史请求/交易所/响应时间 | ❌ | Titan HFT 优势 |
| insert/update/cancel 独立延迟 | ✅ | 🟡 Latency trait 按订单调用，但内置模型主要 entry/response | ❌ | Nautilus 配置更细 |
| Feed latency | `ts_init-ts_event` | 🟢 原生双时间戳 | ❌ Bar 尚未接入 | Titan Tick 优势 |
| Fill probability | ✅ FillModel | 🟡 无通用随机 FillModel；队列概率模型解决的是队列推进比例 | ❌ | Titan 缺口 |
| Slippage probability | ✅ | ❌ 内置 exchange 按簿/价格成交，可自定义 Processor | ❌ | Titan 缺口 |
| 动态替换模型 | ✅ `change_fill_model` | ❌ builder 构造期静态泛型/trait object | ❌ | Titan 不支持热替换 |
| 自定义 exchange processor | SimulationModule/Fill hooks | 🟢 可自行实现 Processor/LocalProcessor/Queue/Latency/Fee | 🟡 RuntimeEventSource trait 可扩展 | Titan 底层扩展强、产品化弱 |
| SimulationModule | ✅ 生命周期式插件 | ❌ 无统一 module 生命周期 | 🚧 custom event slot 预留 | Titan 缺口 |

## 10. 账户、组合、风险和衍生品

| 功能 | Nautilus Trader | Titan Tick/L2/L3 | Titan Bar/Numba | 结论 |
|---|---|---|---|---|
| Position | ✅ | ✅ 每 asset 净仓位 | ✅ 每 asset 净仓位 | 基础能力已有 |
| NETTING | ✅ | ✅ 单一净仓位语义 | ✅ | 对应基础模式 |
| HEDGING/独立 position ID | ✅ | ❌ | ❌ | Titan 缺口 |
| Cash account | ✅ | 🟡 只有 balance/position/fee 数值状态，无账户规则 | ❌ 只有 position | Titan 不完整 |
| Margin account | ✅ | ❌ | ❌ | Titan 高优先级缺口 |
| 初始资金 | ✅ | 🟡 核心 State 从 0 开始，分析层另传 book size | ❌ | Titan 缺口 |
| 单/多币种余额 | ✅ | ❌ 每 asset 独立数值，不是币种账户 | ❌ | Titan 缺口 |
| 杠杆 | ✅ | ❌ | ❌ | Titan 缺口 |
| 初始/维持保证金 | ✅ | ❌ | ❌ | Titan 缺口 |
| 清算/强平 | 可通过账户/交易所规则扩展 | ❌ | ❌ | 永续交易关键缺口 |
| 已实现/未实现 PnL | ✅ Portfolio/Account | 🟡 balance + equity 可计算，但无完整组合归因 | ❌ | Titan 部分支持 |
| Commission 入账 | ✅ | ✅ Tick State | ❌ Bar | Tick 对应 |
| RiskEngine | ✅ 独立组件 | ❌ 策略自行风控 | ❌ | Titan 明显缺口 |
| 订单前风控 | ✅ | ❌ 只有基本请求/撮合校验 | 🟡 数值和 asset 范围校验 | Titan 缺口 |
| 现金卖空/借贷规则 | ✅ 可配置 | ❌ | ❌ | Titan 缺口 |
| 账户冻结 | ✅ | ❌ | ❌ | Titan 缺口 |
| Funding 结算 | 可由数据/模块支持 | ❌ 只有实盘 Funding 消息，回测账户不结算 | ❌ | 永续交易 P0 缺口 |
| FX rollover | ✅ 内置模块 | ❌ | ❌ | 当前非核心目标 |
| 期货到期/结算 | ✅ | ❌ | ❌ | 可按产品路线决定 |
| 期权到期/行权 | ✅ | ❌ | ❌ | 当前非核心目标 |

## 11. 策略 API、事件和回测/实盘一致性

| 功能 | Nautilus Trader | Titan Rust Strategy/Bot | Titan Numba runtime | 结论 |
|---|---|---|---|---|
| 回测/实盘共用策略 API | ✅ 平台组件一致 | 🟢 同一个 `Bot` trait | 🟡 Tick 已同时接回测/实盘；Bar 实盘未接 | Titan Tick 已兑现 |
| 原生策略热路径 | Cython/PyO3 + Python strategy | 🟢 纯 Rust | 🟢 Rust→C ABI→Numba nopython | Titan 性能优势 |
| `on_tick` | ✅ | ✅ 全局 frame | ✅ 全局 TickBatch | 已支持 |
| `on_bar` | ✅ | ✅ 兼容路径从 frame trades 聚合 | ✅ 显式 materialized BarBatch | 回测已支持 |
| `on_start` | ✅ | ❌ trait 无此回调 | ✅ | Numba 已支持 |
| `on_stop` | ✅ | ❌ trait 无此回调 | ✅ 恰好一次 | Numba 已支持 |
| `on_order` | ✅ | 通过 orders 查询/响应推进 | ✅ Tick；❌ Bar | 部分支持 |
| `on_filled` | ✅ | 通过订单响应 | ✅ Tick 和 Bar | 已支持 |
| `on_position` | ✅ | ctx position 快照 | ✅ Tick；❌ Bar | 部分支持 |
| `on_funding` | 🟡 可通过自定义数据/模块表达，源报告未列出同名内置回调 | 实盘 Bot 收到 Funding 但 Strategy trait 无回调 | 🚧 槽存在，启动时拒绝 | 未接线 |
| `on_timer` | ✅ | 通过 frame/elapse 间接表达 | 🚧 槽存在，启动时拒绝 | 未接线 |
| `on_error` | 组件错误机制 | Rust `Result` | ✅ | Numba 已支持 |
| 历史 Bar 访问 | Cache/indicator/history | Rust ctx 只保留当前聚合 Bar | ✅ 固定环、负索引 | Numba Bar 已支持 |
| 指标预计算 | 可在策略外实现 | 策略自管 | ✅ 回测策略可 init 全量数组 | Titan 已验证高性能路径 |
| Actor | ✅ | ❌ | ❌ | Titan 缺口 |
| ExecutionAlgorithm | ✅ | ❌ | ❌ | Titan 缺口 |
| Custom event slots | MessageBus/custom data | ❌ | 🟡 固定 32 槽，源尚需实现 | Titan ABI 已预留 |
| 动态字符串 symbol | ✅ typed ID | 初始化后 asset_no | 初始化后 asset_no | Titan 热路径更轻 |

## 12. 结果、统计、报告和批量编排

| 功能 | Nautilus Trader | Titan | 结论 |
|---|---|---|---|
| 结构化 BacktestResult | ✅ run/config/id/count/PnL/return | ❌ 无统一结果对象 | Titan 缺口 |
| Orders report | ✅ | ❌ 可查询当前订单，但无标准报告 | Titan 缺口 |
| Fills report | ✅ | 🟡 Numba fill 事件/内部订单响应，无标准导出 | Titan 缺口 |
| Positions report | ✅ | 🟡 recorder 记录净仓位 | Titan 部分支持 |
| Account report | ✅ | ❌ | Titan 缺口 |
| Equity/PnL 时间序列 | ✅ | ✅ BacktestRecorder | 对应基础能力 |
| CSV/NPZ 导出 | 可生成多类报告 | ✅ recorder 导出 CSV/NPZ | Titan 基础能力已有 |
| Return/Sharpe/Sortino/MDD | ✅ | ✅ Python stats | 对应 |
| 交易量/交易次数/仓位指标 | ✅ | ✅ | 对应基础指标 |
| 多币种 PnL | ✅ | ❌ | Titan 缺口 |
| 高层 Catalog 查询 | ✅ ParquetDataCatalog | ❌ 示例层直接读文件 | Titan 缺口 |
| RunConfig | ✅ Venue/Data/Engine/Run 配置对象 | 🟡 Rust typed builder + Python builder，无统一 run config | Nautilus 更产品化 |
| 批量 runs 编排 | ✅ BacktestNode | 🟡 示例 CLI `--runs`，无通用 Node | Titan 缺口 |
| 参数扫描复用 | ✅ reset + 已加载数据 | 🟡 可复用预计算数组，但 Backtest/runtime 要重建 | Titan 需 runner lifecycle |
| 关闭结束分析 | ✅ `run_analysis=False` | 分析本来在运行外 | 🔵 Titan 核心更精简 |

## 13. 扩展性对比

| 扩展点 | Nautilus Trader | Titan | 结论 |
|---|---|---|---|
| FillModel | ✅ | 🟡 需自定义 exchange Processor | Nautilus 接口更直接 |
| FeeModel | ✅ | ✅ Rust trait | 对应 |
| LatencyModel | ✅ | ✅ Rust trait | 对应，Titan 有历史插值模型 |
| MarginModel | ✅ | ❌ | Titan 缺口 |
| QueueModel | 可选 queue position | 🟢 多种 L2/L3 模型和自定义 trait | Titan 强项 |
| SimulationModule | ✅ | ❌ | Titan 缺口 |
| DataClient/CustomData | ✅ | ❌ | Titan 缺口 |
| RuntimeEventSource | 引擎内部扩展 | ✅ Rust trait | Titan 底层可扩展 |
| ExecutionAlgorithm | ✅ | ❌ | Titan 缺口 |
| Live connector | 平台适配器体系 | ✅ Binance/OKX/Hyperliquid/Bybit 等独立进程连接器 | 两者都有，Titan 聚焦加密 |
| 共享内存 IPC | 不是该文档重点 | 🟢 iceoryx2 connector↔bot | Titan 实盘架构特点 |

## 14. Titan 已有优势清单

以下能力不应为了模仿 Nautilus 而弱化：

- [x] `exch_ts` 与 `local_ts` 双时间戳行情回放；
- [x] 订单 entry/response 双向延迟；
- [x] 历史订单延迟插值模型；
- [x] L2 RiskAdverse/概率队列模型；
- [x] L3 市场订单 ID + FIFO 队列模型；
- [x] PartialFill 和 NoPartialFill 两种明确 exchange model；
- [x] 多资产、本地/交易所、行情/订单四类事件统一最早时间调度；
- [x] Rust 原生策略和 Numba nopython 单参数 callback；
- [x] 同一个 Tick Bot 接口用于回测和实盘；
- [x] 全局 TickBatch，避免逐资产 Python callback；
- [x] Bar 模式不伪造 Tick/盘口；
- [x] Bar `on_bar` 新订单禁止使用刚关闭 Bar 成交；
- [x] 固定容量 Bar 历史环和明确的前一根索引语义；
- [x] 独立进程交易所连接器和共享内存 IPC；
- [x] 加密永续交易所统一 Broker API 和 Funding 实盘事件。

## 15. 建议补齐清单

### P0：符合 Titan 当前加密永续/HFT 定位的核心缺口

- [ ] **回测 Funding 事件和账户结算**：读取资金费率/支付事件，更新 balance/equity，并接通
  `on_funding(s)`；
- [ ] **保证金、杠杆和强平模型**：至少支持永续合约的 collateral、initial/maintenance
  margin、可用余额和 liquidation；
- [ ] **Hybrid 精确合并器**：Bar 负责信号，Tick/L2 负责执行，并固定同时间戳顺序；
- [ ] **Bar runtime 接入 Fee/Latency/State**：不能只有 position；
- [ ] **统一 BacktestResult**：至少包含时间范围、事件数、订单数、成交、持仓、费用、资金费、
  realized/unrealized PnL 和墙钟耗时；
- [ ] **可复用 Runner/Reset 生命周期**：保留数据和编译 callback，完整重置订单、持仓、历史环和
  策略状态；
- [ ] **严格订单精度和能力校验**：价格、数量、TIF、order type、数据粒度在启动/提交时 fail-fast；
- [ ] **回测与实盘 Bar 来源**：Canonical streaming builder、交易所 Candle closed 事件和断线补齐。

### P1：完整事件策略和执行真实性

- [ ] 接通 `on_timer(s)`；
- [ ] Bar 模式接通 `on_order(s)`、`on_position(s)`；
- [ ] 支持 reduce-only；
- [ ] 支持 stop-market、stop-limit 和 GTD；
- [ ] 增加可选 historical liquidity consumption，防止多个主动单重复消费同一档历史数量；
- [ ] 增加可插拔 Fill/Slippage 模型，不与 QueueModel 混为一体；
- [ ] 支持 Bar Touch、ConservativeOhlc 和 VolumeLimited 模型，同时继续保留 NextOpen；
- [ ] 为多流相同时间戳提供显式稳定 priority 配置；
- [ ] 增加标准 orders/fills/positions/account 报告；
- [ ] Bar 数据实现真正分块/流式源，避免全量复制。

### P2：平台化能力

- [ ] 独立 RiskEngine 或最小可插拔 pre-trade risk layer；
- [ ] OTO/OCO/bracket 条件单状态机；
- [ ] Actor 和 ExecutionAlgorithm 扩展接口；
- [ ] CustomData 和自定义 DataClient；
- [ ] 统一 Catalog、RunConfig 和批量 BacktestNode；
- [ ] 多币种账户与组合级 PnL；
- [ ] 动态市场状态和交易时段；
- [ ] 指标数组封装成只暴露当前/历史位置的 `IndicatorView`，结构上阻止回测前视。

### 可暂缓：与当前加密永续定位不直接相关

- [ ] HEDGING position ID 体系；
- [ ] FX rollover；
- [ ] 期权到期、行权和复杂 Greeks 生命周期；
- [ ] 通用股票现金账户借贷规则；
- [ ] Market-to-Limit、MIT、trailing stop、OUO 等较低优先级订单类型；
- [ ] 与 Nautilus 完全相同的通用 MessageBus/Actor 平台。

## 16. 建议实施顺序

```text
1. Result + Reset + 能力校验
        ↓
2. Funding + 永续保证金/强平 + Bar fee/latency
        ↓
3. Hybrid + 实盘 Bar builder/candle recovery
        ↓
4. Timer + 完整 order/position/funding callbacks
        ↓
5. reduce-only/stop/GTD + liquidity consumption/slippage
        ↓
6. 风控、报告、Catalog 和批量编排平台化
```

优先顺序的理由是：Titan 当前定位是加密永续和 HFT，因此资金费、保证金、延迟、队列、Hybrid
和回测/实盘一致性，比期权行权、通用 Actor 或复杂条件单更直接影响回测可信度。

## 17. 主要源码依据

Titan：

- [`hftbacktest/src/backtest/mod.rs`](../hftbacktest/src/backtest/mod.rs)：资产 builder、EventSet
  主循环和 Bot 实现；
- [`hftbacktest/src/backtest/evs.rs`](../hftbacktest/src/backtest/evs.rs)：本地/交易所、行情/订单
  事件优先级；
- [`hftbacktest/src/runtime.rs`](../hftbacktest/src/runtime.rs)：Numba ABI、生命周期和 materialized
  Bar source；
- [`hftbacktest/src/types.rs`](../hftbacktest/src/types.rs)：订单、TIF、状态和统一 Bot API；
- [`hftbacktest/src/backtest/models`](../hftbacktest/src/backtest/models)：Latency、Queue 和 Fee 模型；
- [`hftbacktest/src/backtest/proc`](../hftbacktest/src/backtest/proc)：L2/L3、Partial/NoPartial exchange；
- [`hftbacktest/src/backtest/data/reader.rs`](../hftbacktest/src/backtest/data/reader.rs)：分块文件读取和并行预取；
- [`hftbacktest/src/backtest/state.rs`](../hftbacktest/src/backtest/state.rs)：基础 position/balance/fee 状态；
- [`hftbacktest/src/backtest/recorder.rs`](../hftbacktest/src/backtest/recorder.rs)：回测记录导出；
- [`py-hftbacktest/src/runtime.rs`](../py-hftbacktest/src/runtime.rs)：Tick runtime 到
  order/fill/position/tick callback 的接线；
- [`docs/bar_tick_numba_strategy.md`](bar_tick_numba_strategy.md)：Bar/Tick/Hybrid 目标和当前状态。

Nautilus Trader：

- [`backtest_engine_implementation_analysis.md`](/Users/dominolu/dev/nautilus_trader/docs/concepts/backtest_engine_implementation_analysis.md)。

## 18. 最终判断

Titan 当前已经是一个强项明确的 HFT 回测内核，而不是完整的交易系统仿真平台：

- 在 Tick/L2/L3、双时间戳、延迟和队列位置方面，已经具备与 Nautilus 对应甚至更专门的能力；
- 在 Rust/Numba 原生 callback 性能和加密交易所实盘接线上，Titan 有清晰优势；
- 在 Bar runtime、账户/组合、风险、Funding 结算、订单状态机、Timer、生命周期 reset、报告和高层
  编排方面，当前明显弱于 Nautilus；
- Nautilus 的功能广度不应全部照搬。Titan 应先补齐影响加密永续回测真实性和回测/实盘一致性的
  P0/P1 能力，同时保持精简、确定和高性能的核心架构。
