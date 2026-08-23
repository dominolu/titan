# Titan Tick/Bar 专业回测引擎共享执行层方案

> 状态：历史架构方案；实现与验收已完成，当前状态以
> [`shared_execution_layer_implementation_tasks.md`](shared_execution_layer_implementation_tasks.md) 为准。目标是在不把 Bar 伪装成 Tick、不降低 Tick/L2/L3 性能和真实性的
> 前提下，抽取共享执行层，使 Tick 与 Bar 获得一致的订单、延迟、费用、账户、事件和结果语义。

## 1. 背景与问题

以下内容是设计启动时冻结的问题快照；当前实现状态和逐项证据见
[`shared_execution_layer_acceptance_report.md`](shared_execution_layer_acceptance_report.md)。设计启动时有两条独立回测路径：

### Tick/L2/L3 主引擎

已经实现：

- local/exchange 双时钟和双处理器；
- 行情、请求、交易所处理、响应四类事件调度；
- entry/response latency；
- 完整 `Order` 状态；
- Market/Limit、GTC/GTX/FOK/IOC；
- L2/L3 订单簿、队列模型；
- PartialFill/NoPartialFill exchange；
- FeeModel、AssetType 和 `StateValues`；
- order/fill/position runtime 事件。

### Materialized Bar runtime（设计前快照）

当前 `MaterializedBarSource` 同时承担：

- Bar 数据读取和批次调度；
- 历史环维护；
- command buffer 消费；
- pending order 存储；
- NextOpen 撮合；
- position 更新；
- fill 事件生成。

当时它只保存 `OrderCommand`、`PendingBarOrder`、`FillEvent` 和 `positions: Vec<f64>`，没有复用
Tick 引擎的订单、延迟、费用、账户和响应管线。因此当前 Bar 引擎只是时间语义正确的第一阶段
实现，不是与 Tick 引擎同等级的专业执行引擎。该差距现已由共享 coordinator、Venue 账户、
全局 Scheduler、Projector 和统一 Result/Audit 路径补齐。

## 2. 目标

### 2.1 功能目标

Tick 与 Bar 必须共享：

- 完整订单对象和状态转换；
- submit/cancel 处理；
- 请求 entry latency；
- 响应 response latency；
- ack/reject/expire/cancel/fill/partial-fill 报告；
- FeeModel；
- AssetType 和账户账本；
- position、balance、fee、trading volume/value；
- `on_order`、`on_filled`、`on_position`；
- Recorder 和结构化 BacktestResult；
- reset 和重复运行生命周期；
- 能力校验、时间戳不变量和确定性顺序。

Tick 与 Bar 分别实现：

- Tick：订单簿价格、深度、queue position 和逐笔成交；
- Bar：NextOpen、ConservativeOhlc、Touch、VolumeLimited 等显式 Bar FillModel。

### 2.2 性能目标

- Tick release 基准不得因为抽象层退化超过 3%；
- 共享层热路径不得使用字符串、JSON、动态 Python 对象；
- Numba callback 仍然是稳定 C ABI 单指针调用；
- callback 内 command/event buffer 启动时预分配；
- Tick matching 保留单态化泛型路径，不强制所有模型走 trait object；
- Bar 继续按事件跳转，不进入固定 frame 空转；
- 不为了架构统一创建虚假 Tick 或虚假盘口。

### 2.3 兼容目标

- 保持现有 `on_tick(s)`、`on_bar(s)` 单参数策略形式；
- 保持现有 Tick 策略的成交结果、订单时间戳和队列结果；
- 默认 Bar 配置仍为零延迟、零费用、NextOpen 全量成交；
- 当前双均线 Bar 示例的信号、6,023 成交数量和最终持仓保持一致；
- 公共策略 API 继续禁用 modify，修改使用 cancel/replace；
- ABI 必须显式升级并在 Python 导入时验证布局。

## 3. 非目标

第一轮共享执行层不同时实现以下平台化功能：

- 不把 Bar 转换成合成 Tick 后交给 L2 exchange；
- 不在第一阶段实现 OCO/OTO/OUO；
- 不在第一阶段实现期权行权和通用衍生品到期；
- 不强制引入类似 Nautilus 的完整 MessageBus；
- 不在第一阶段恢复 modify 公共接口；
- 不用 Bar high/low 让 `on_bar` 新订单回到刚结束的 Bar 成交。

## 4. 设计原则

### 4.1 共享执行，不共享市场事实

```text
                  SharedExecutionCore
        ┌─────────────────────────────────────┐
        │ OrderStateMachine                   │
        │ Request/Response Transport          │
        │ Fee + Account Ledger                │
        │ Execution Reports                   │
        │ Runtime Event Projection            │
        │ Recorder / Result / Reset            │
        └─────────────────────────────────────┘
                    ▲                 ▲
                    │                 │
          TickMatchingModel      BarMatchingModel
          L2/L3/queue/depth      NextOpen/OHLC/Volume
```

共享层管理“订单和资金发生了什么”；市场模型只决定“在什么市场时间、以什么价格和数量成交”。

### 4.2 数据源、调度器、撮合模型分离

当前 `MaterializedBarSource` 将三者耦合。目标拆成：

```text
DataSource
    └── 产生带 exchange/delivery 时间的市场事件

Scheduler
    └── 合并 market、command arrival、response delivery、timer

MatchingModel
    └── 消费市场事件和已到达交易所的订单，产生 MatchOutcome

SharedExecutionCore
    └── 更新订单状态、费用、账户并产生 ExecutionReport
```

### 4.3 Bar 不含 available_ts

继续保持 Bar 数据结构只有 `open_ts` 和 `close_ts`。投递时间属于事件信封：

```rust
pub struct EventEnvelope<T> {
    pub exch_ts: i64,
    pub delivery_ts: i64,
    pub sequence: u64,
    pub payload: T,
}
```

回测 Bar 默认：

```text
exch_ts     = close_ts
delivery_ts = close_ts + feed_latency
```

不得向 `Bar` 增加 `available_ts`。

## 5. 建议模块结构

```text
hftbacktest/src/backtest/
├── execution/
│   ├── mod.rs
│   ├── command.rs          # 内部命令，不等于 FFI OrderCommand
│   ├── order_manager.rs    # 本地/交易所订单状态
│   ├── state_machine.rs    # 状态转换和不变量
│   ├── transport.rs        # entry/response latency 调度
│   ├── report.rs           # Ack/Reject/Fill/Cancel/Expire
│   ├── account.rs          # position/balance/fee/PnL
│   ├── capabilities.rs     # 模型能力矩阵
│   ├── event_sink.rs       # 预分配 execution event buffer
│   └── result.rs           # BacktestResult
├── matching/
│   ├── mod.rs
│   ├── tick/
│   │   ├── l2.rs
│   │   ├── l3.rs
│   │   ├── partial.rs
│   │   └── no_partial.rs
│   └── bar/
│       ├── next_open.rs
│       ├── conservative_ohlc.rs
│       ├── touch.rs
│       └── volume_limited.rs
├── scheduler/
│   ├── mod.rs
│   ├── event_key.rs
│   └── event_queue.rs
└── data/
```

迁移期间可以保留原文件，通过 adapter 逐步转发，不要求一次性移动全部源码。

### 5.1 当前源码职责迁移表

| 当前源码 | 当前职责 | 目标位置/处理 |
|---|---|---|
| `backtest/order.rs` | 双向 OrderBus 和 latency | 扩展为 `execution/transport.rs`，增加稳定 sequence |
| `backtest/state.rs` | position/balance/fee | 迁移为 `execution/account.rs`，保持兼容 facade |
| `proc/local.rs` | L2 local market + local orders + account | 拆成 LocalMarketView、OrderManager、AccountLedger |
| `proc/l3_local.rs` | L3 local market + 重复执行逻辑 | 与 L2 共用执行部分，只保留 L3 market adapter |
| `proc/nopartialfillexchange.rs` | L2 no-partial matching + 状态修改 | 先包装为 TickMatching adapter，再逐步返回 MatchOutcome |
| `proc/partialfillexchange.rs` | L2 partial matching + 状态修改 | 同上 |
| `proc/l3_nopartialfillexchange.rs` | L3 FIFO matching | 保留队列算法，改为 MatchOutcome 输出 |
| `backtest/mod.rs::Backtest` | Tick 调度与 Bot | 调用共享 scheduler/execution，保持 Bot facade |
| `runtime.rs::MaterializedBarSource` | Bar feed、matching、position、fill | 拆成 MaterializedBarFeed + BarMatchingModel |
| `py-hftbacktest/src/runtime.rs::TickFrameSource` | Tick report 转 callback | 替换为共享 RuntimeEventProjector |
| `py-hftbacktest/hftbacktest/eventbot.py` | Python 配置、bridge、返回 state | 增加 PreparedRunner/BacktestResult，保留旧 facade |

## 6. 共享领域模型

### 6.1 FFI command 与内部 command 分离

Numba 写入的 `OrderCommand` 是 ABI 数据传输格式，不应作为引擎内部订单模型。

```rust
pub enum ExecutionCommand {
    Submit(OrderRequest),
    Cancel(CancelRequest),
}

pub struct OrderRequest {
    pub asset_no: usize,
    pub order_id: u64,
    pub side: Side,
    pub order_type: OrdType,
    pub time_in_force: TimeInForce,
    pub price: f64,
    pub qty: f64,
    pub local_ts: i64,
}
```

FFI 边界只负责验证和转换：

```text
OrderCommand -> validate -> ExecutionCommand -> SharedExecutionCore
```

### 6.2 完整订单状态

保留现有 `Order` 的核心字段，并明确区分：

- local request state；
- exchange order state；
- cumulative filled quantity；
- last fill quantity/price；
- leaves quantity；
- request/local/exchange/response timestamps；
- maker/taker；
- rejection/expiration reason；
- model-specific queue metadata。

建议后续将 `req: Status` 拆为独立 `RequestStatus`，避免订单状态和请求状态复用同一个 enum；第一阶段
可用 adapter 保持二进制和行为兼容。

### 6.3 ExecutionReport

共享执行层只向上游发送不可变报告：

```rust
pub enum ExecutionReport {
    Accepted(OrderSnapshot),
    Rejected(OrderSnapshot, RejectReason),
    PartiallyFilled(FillReport),
    Filled(FillReport),
    Canceled(OrderSnapshot),
    Expired(OrderSnapshot, ExpireReason),
}

pub struct FillReport {
    pub asset_no: usize,
    pub order_id: u64,
    pub exch_ts: i64,
    pub price: f64,
    pub qty: f64,
    pub liquidity: LiquiditySide,
    pub fee: f64,
}
```

一次部分成交必须对应一条独立 FillReport，禁止把多个 fill 折叠成最后订单快照。

### 6.4 MatchOutcome

撮合模型不直接修改账户，只返回市场事实：

```rust
pub enum MatchOutcome {
    NoMatch,
    Resting,
    Reject(RejectReason),
    Expire(ExpireReason),
    Fill {
        order_id: u64,
        exch_ts: i64,
        price: f64,
        qty: f64,
        liquidity: LiquiditySide,
    },
}
```

共享状态机消费 outcome，统一更新 `leaves_qty/status`，再计算 fee 和生成报告。

## 7. 核心接口

### 7.1 MatchingModel

```rust
pub trait MatchingModel {
    type MarketEvent<'a>;
    type Error;

    fn capabilities(&self) -> MatchingCapabilities;

    fn on_order_arrival(
        &mut self,
        ts: i64,
        order: &Order,
        out: &mut impl MatchSink,
    ) -> Result<(), Self::Error>;

    fn on_cancel_arrival(
        &mut self,
        ts: i64,
        order_id: u64,
        out: &mut impl MatchSink,
    ) -> Result<(), Self::Error>;

    fn on_market_event(
        &mut self,
        event: Self::MarketEvent<'_>,
        out: &mut impl MatchSink,
    ) -> Result<(), Self::Error>;

    fn reset(&mut self);
}
```

实际实现可为性能使用泛型和内联；上面的 trait 是职责边界，不要求热路径使用动态分发。

### 7.2 SharedExecutionCore

```rust
pub struct SharedExecutionCore<M, LM, AT, FM> {
    matching: M,
    orders: OrderManager,
    transport: OrderTransport<LM>,
    account: AccountLedger<AT, FM>,
    reports: ExecutionEventBuffer,
    sequence: u64,
}
```

核心入口：

```rust
submit(local_ts, request)
cancel(local_ts, request)
process_command_arrivals(ts)
process_market_event(event)
process_report_deliveries(ts)
next_internal_timestamp()
snapshot()
reset()
```

### 7.3 Capability contract

每个 matching model 必须声明：

```rust
pub struct MatchingCapabilities {
    pub market: bool,
    pub limit: bool,
    pub gtc: bool,
    pub gtx: bool,
    pub ioc: bool,
    pub fok: bool,
    pub partial_fill: bool,
    pub maker_taker: bool,
    pub queue_position: bool,
    pub requires_depth: bool,
}
```

builder 在启动时校验配置，submit 时再次校验订单组合。禁止像当前 Bar runtime 一样接受 GTX，
却把所有成交固定标为 taker。

## 8. 共享订单传输和延迟

### 8.1 复用并升级 OrderBus

当前 `OrderBus` 已支持：

- local → exchange entry latency；
- exchange → local response latency；
- 负 entry latency 表示技术拒绝；
- 请求/响应按时间有序。

将它抽成通用 `OrderTransport<LM>`，但需要增加稳定 sequence：

```text
(delivery_ts, sequence, message)
```

避免相同时间戳依赖 Vec 或 asset 插入顺序。

### 8.2 Bar 延迟语义

假设 Bar N 在 `T` 关闭，策略在 delivery time `D` 收到：

```text
Bar N close/exchange time = T
Bar delivery time         = D = T + feed_latency
order request time        = D
exchange arrival          = D + entry_latency
```

订单只能参与 arrival 之后的第一个 Bar execution opportunity。

例如下一根 Bar open 同样是 `T`：

- entry latency = 0 且 delivery `D=T`：可参加下一根 open；
- entry latency > 0：已经错过下一根 open，必须等待后续 execution opportunity；
- response latency > 0：成交已在 exchange time 发生，但策略直到 response delivery 才看到
  `on_order/on_filled/on_position`。

## 9. 共享账户和费用

### 9.1 第一阶段账户范围

先达到 Tick 当前能力对齐：

- position；
- balance；
- cumulative fee；
- number of fills；
- trading volume；
- trading value；
- equity；
- LinearAsset/InverseAsset；
- TradingValue/TradingQty/FlatPerTrade/Directional fee。

### 9.2 AccountLedger

```rust
pub struct AccountLedger<AT, FM> {
    values: StateValues,
    asset_type: AT,
    fee_model: FM,
    initial_balance: f64,
}
```

账户只在 fill report 到达 local 时更新策略可见状态，以保持 response latency 语义。Exchange 侧如需
风险或 reduce-only 判断，应维护独立 exchange position，不得提前修改 local 可见账户。

### 9.3 后续专业账户能力

共享执行层稳定后继续扩展：

- realized/unrealized PnL 拆分；
- initial balance；
- collateral/free balance；
- leverage；
- initial/maintenance margin；
- liquidation；
- funding settlement。

这些能力必须同时服务 Tick 和 Bar，不能再次在某个数据模式中单独实现。

## 10. Tick 引擎迁移方案

### 10.1 保持 Tick 市场模型不变

以下代码先作为 adapter 保留：

- `NoPartialFillExchange`；
- `PartialFillExchange`；
- `L3NoPartialFillExchange`；
- QueueModel/L3QueueModel；
- HashMap/ROI/BTree/L3 depth。

先把它们产生的订单变更转换为 `MatchOutcome`，不要第一步重写撮合算法。

### 10.2 抽取 Local 共性

当前 `Local` 和 `L3Local` 大量重复：

- local order map；
- submit/modify/cancel；
- response 接收；
- State apply fill；
- order latency；
- trades/market state。

迁移为：

```text
LocalMarketView<MD>       # depth、trade、feed latency
LocalExecutionView       # OrderManager、AccountLedger、report delivery
```

L2/L3 只在市场深度更新逻辑上不同。

### 10.3 移除 exchange 侧重复账户

当前 local 和 exchange 各有一个 `State`，同一 fill 在 exchange 和 local 各 apply 一次。Exchange
侧 State 不对策略暴露，主要用于内部状态。迁移时：

- 策略可见账本只保留 local AccountLedger；
- exchange 若需要仓位约束，保留独立 ExchangePosition，不计算重复 fee；
- fee 在 fill outcome 转 report 时计算一次；
- differential test 必须确认现有公开 StateValues 不变。

### 10.4 Tick 事件顺序兼容

第一阶段严格保留现有相同时间戳顺序。当前 `EventSet` 的槽位索引是
`4 * asset_no + event_kind`，因此 tie-break 实际按 `(asset_no, event_kind)`，其中：

```text
event_kind: LocalData -> LocalOrderResponse -> ExchangeData -> ExchangeOrderArrival
```

即使未来认为顺序需要调整，也必须另开版本和迁移说明，不能在共享层重构中静默改变回测结果。

## 11. Bar 引擎迁移方案

### 11.1 拆分 MaterializedBarSource

目标：

```text
MaterializedBarFeed
├── records/cursor/batch
├── history rings
└── Bar delivery envelopes

BarMatchingModel
├── active exchange orders
├── execution opportunity
└── MatchOutcome

SharedExecutionCore
├── command latency
├── order states/reports
├── fee/account
└── callbacks/result
```

删除 `MaterializedBarSource` 中直接维护的：

- `orders: Vec<PendingBarOrder>`；
- `fills: Vec<FillEvent>`；
- `positions: Vec<f64>`；
- `process_commands()`；
- `match_at_next_open()`。

这些职责分别进入共享执行层和 `NextOpenMatchingModel`。

### 11.2 Bar matching models

#### NextOpen

- 市价单在下一个 eligible open 全量成交；
- 限价买仅 `open <= limit`；
- 限价卖仅 `open >= limit`；
- 不使用当前或下一 Bar high/low；
- 不支持真实 partial fill；
- 不支持 GTX/maker 语义，应在 capability check 明确拒绝；
- GTC 保留；IOC/FOK 在首次 eligible open 处理。

#### ConservativeOhlc

- 只允许订单使用提交之后完整发生的 Bar；
- 对同 Bar high/low 顺序不确定时选择对策略最不利结果；
- fill 时间不得早于 Bar close，避免用完整 OHLC 后伪造早期可见成交；
- 明确记录 `synthetic=true` 或 model id。

#### Touch

- resting limit 被后续 Bar 触及即成交；
- 不承诺真实队列或可成交量；
- 适合逻辑验证，不用于大单执行真实性。

#### VolumeLimited

- 最大成交量由 `participation_rate * eligible_bar.volume` 限制；
- 多订单按稳定 price-time/sequence 分配；
- fill 在 Bar close 或明确的 synthetic path 时间产生；
- 不允许在 open 使用尚未发生的整根 Bar volume；
- 支持 partial fill 和 leaves quantity。

如需要 open 部分成交，输入必须额外提供可审计的 `open_volume`，不能从总 Bar volume 猜测。

### 11.3 Bar 事件顺序

以 Bar N 在 `T` 关闭为例：

```text
1. 处理 delivery_ts < D 的 order reports/timers
2. 在 D 投递 Bar N
3. on_bar(s)
4. callback 返回后提交 commands
5. commands 经 entry latency 到达 exchange
6. 到达下一次 Bar execution opportunity 时撮合
7. 生成 exchange reports
8. 经 response latency 投递：on_order -> on_filled -> on_position
9. 当前 Bar callback 返回后才提交到 history ring
```

同一时间戳使用 `(timestamp, priority, sequence)`。为保持当前已经验收的 NextOpen 语义，
`Bar N close == Bar N+1 open == T` 时，必须先让策略看到 Bar N，再处理 Bar N+1 open：

```text
priority 0: 此前已经产生且在 T 到期的 response delivery
priority 1: completed Bar delivery（Bar N close）
priority 2: strategy commands generated by on_bar
priority 3: zero-latency command arrival / 同时间戳命令结算
priority 4: market execution opportunity（Bar N+1 open）
priority 5: zero-latency execution report delivery
priority 6: callback-created same-time timer（后续）
```

具体数值可以调整，但必须形成公开、测试固定的契约。特别要覆盖上一根 close 与下一根 open 同为 T
的情况。若 Bar delivery 或 entry latency 使订单在 T 之后才到达，它必须错过 T 的 open。

## 12. Runtime callback 投影

共享执行层报告转换为统一 callback 顺序：

```text
ExecutionReport batch
    -> on_order(s)
    -> on_filled(s)    # 仅包含每一笔独立 fill
    -> on_position(s)  # position 确实改变时
```

Tick 和 Bar 使用相同转换器，不再分别手写事件拼装。

需要扩展 `StrategyRuntimeContext`：

- `StateValues`/account views；
- 完整 reject/expire reason；
- cumulative/last fill 字段；
- 可选 model/source 标记；
- 后续 funding/account payload。

ABI 从 5 升级为 6：

1. Rust `#[repr(C)]`；
2. Python dtype 同步；
3. 导入时校验 size/alignment/offset；
4. 不复用旧字段含义；
5. 旧 ABI 明确拒绝，不做不安全兼容。

## 13. Builder 和配置

### 13.1 统一 ExecutionConfig

```rust
pub struct ExecutionConfig<LM, AT, FM> {
    pub latency_model: LM,
    pub asset_type: AT,
    pub fee_model: FM,
    pub initial_balance: f64,
    pub command_capacity: usize,
    pub event_capacity: usize,
}
```

Tick builder 和 Bar builder 都必须提供它。

### 13.2 BarExecutionConfig

```rust
pub enum BarMatchingKind {
    NextOpen,
    ConservativeOhlc,
    Touch,
    VolumeLimited { participation_rate: f64 },
}

pub struct BarExecutionConfig<...> {
    pub common: ExecutionConfig<...>,
    pub matching: BarMatchingKind,
    pub feed_latency: BarFeedLatency,
    pub execution_timeframe_ns: Option<i64>,
}
```

默认值保持当前行为：

```text
matching        = NextOpen
entry latency   = 0
response latency= 0
fee             = 0
initial balance = 0
```

### 13.3 Fail-fast 校验

启动阶段检查：

- 数据模式和 matching model 一致；
- partial fill 配置是否被 model 支持；
- GTX 是否被 model 支持；
- tick size、lot size、价格和数量精度；
- timeframe 与执行周期；
- fee/asset/account 配置完整；
- command/event buffer 容量；
- Bar 排序、重复、完整性和 interval；
- L2/L3 模型是否提供对应行情。

## 14. Result、Recorder 和 Reset

### 14.1 BacktestResult

```rust
pub struct BacktestResult {
    pub start_ts: i64,
    pub end_ts: i64,
    pub wall_time_ns: u64,
    pub market_event_count: u64,
    pub callback_count: [u64; EVENT_SLOT_COUNT],
    pub order_count: u64,
    pub fill_count: u64,
    pub reject_count: u64,
    pub cancel_count: u64,
    pub expire_count: u64,
    pub account: Vec<AccountSnapshot>,
}
```

详细订单/成交可由可选 recorder 收集，避免默认给高频回测增加无界内存。

### 14.2 Reset contract

`reset()` 必须清除：

- data cursor；
- scheduler queue 和 sequence；
- local/exchange orders；
- latency transport；
- matching model/queue position；
- account/position/fee；
- runtime event buffers；
- Bar history rings；
- user state；
- stop/error 状态和 counters。

可以保留：

- 已加载只读数据；
- 策略已编译 callback；
- builder/config；
- 预计算只读指标数组。

## 15. 测试方案

### 15.1 Tick characterization tests

重构前先冻结 golden results：

- Market/Limit；
- GTC/GTX/IOC/FOK；
- submit/cancel/modify 内核行为；
- entry/response latency；
- 技术拒绝；
- maker/taker；
- partial fills 序列；
- L2 RiskAdverse/Prob queue；
- L3 FIFO；
- fee/balance/equity；
- 相同时间戳事件顺序；
- 多资产事件归并。

迁移后要求订单快照、fill 顺序、时间戳和 StateValues 完全一致。

### 15.2 Tick/Bar 共享执行一致性测试

对不依赖市场模型的行为运行同一组 contract tests：

- duplicate order ID；
- invalid qty/price；
- submit ack；
- cancel ack/reject；
- response latency；
- partial fill 状态转换；
- `qty = cum_fill + leaves_qty`；
- fee 只记一次；
- position 只在 local fill delivery 后可见；
- order/fill/position callback 顺序；
- reset 后结果一致。

### 15.3 Bar 专项测试

- Bar N 下单绝不使用 Bar N high/low；
- zero latency 可参加下一根同时间 open；
- positive entry latency 错过下一 open；
- response latency 延后 callback 和账户可见性；
- canceled order 不再成交且产生 cancel report；
- IOC/FOK/GTC 正确；
- NextOpen 明确拒绝 GTX；
- VolumeLimited 多次 partial fill；
- 多订单按稳定 sequence 分配 volume；
- empty Bar 不撮合；
- 多周期只使用配置的 execution timeframe；
- 多资产同 close 一次 BarBatch；
- history `[-1]` 仍是前一根；
- on_order/on_filled/on_position 事件完整；
- fee/balance/equity 与手工计算一致。

### 15.4 Property tests

- `0 <= leaves_qty <= qty`；
- cumulative fill 单调且不超过 qty；
- terminal order 不能再次成交；
- cancel/expire/fill 终态互斥；
- `request_ts <= exchange_ts <= response_ts`；
- local position 等于已投递 fill 的带符号和；
- fee 等于所有已投递 fill fee 之和；
- 相同输入、配置和 seed 事件日志 hash 相同；
- 策略不能看到 delivery time 之后的数据。

### 15.5 性能测试

至少保留：

- 纯 Rust Tick release baseline；
- Rust + Numba Tick；
- L2 queue-heavy market making；
- L3 FIFO；
- Bar NextOpen 无订单；
- Bar NextOpen 高频下单；
- Bar VolumeLimited partial fill；
- 100 次 reset/reuse；
- event recorder 关闭/开启两档。

验收：

- Tick 中位数退化不超过 3%；
- Tick P95 退化不超过 5%；
- 默认 recorder 关闭时无逐事件堆增长；
- Bar callback bridge 复用；
- Bar NextOpen 保持百万级 Bar/s；
- 所有 benchmark 输出配置和 git revision。

## 16. 分阶段迁移计划

### Phase 0：冻结语义和基准

- [ ] 为现有 Tick/Bar 生成 golden event logs；
- [ ] 固定相同时间戳事件顺序；
- [ ] 保存 release benchmark；
- [ ] 写 ADR：共享执行、不共享市场事实；
- [ ] 禁止在此阶段新增订单功能。

交付物：characterization tests、benchmark 数据、ADR。

### Phase 1：共享 report、状态机和能力声明

- [ ] 新建 `execution/report.rs`；
- [ ] 新建 `state_machine.rs`；
- [ ] 新建 capability matrix；
- [ ] 用 adapter 从现有 Tick `Order` 生成 reports；
- [ ] 不改变 Tick 主循环和撮合代码。

交付物：可独立测试的 OrderStateMachine 和 ExecutionReport。

### Phase 2：共享 transport、local order manager 和 account

- [ ] 抽取/升级 OrderBus；
- [ ] 合并 Local/L3Local 的订单响应共性；
- [ ] 抽取 AccountLedger；
- [ ] fee 统一只计算一次；
- [ ] 保持 Tick differential tests 全绿。

交付物：Tick 首先运行在共享执行层上，行为不变。

### Phase 3：Tick matching adapter

- [ ] NoPartialFillExchange adapter；
- [ ] PartialFillExchange adapter；
- [ ] L3 adapter；
- [ ] MatchOutcome 转换；
- [ ] 移除 exchange 侧重复 fee/account；
- [ ] 完成 Tick 性能验收。

交付物：Tick 引擎完成迁移，旧路径仍可 feature flag 回退一个版本。

### Phase 4：Bar NextOpen 接入共享执行层

- [ ] 拆分 MaterializedBarFeed；
- [ ] 实现 NextOpenMatchingModel；
- [ ] 接入 latency/fee/account；
- [ ] 接通 on_order/on_position；
- [ ] cancel/expire/reject 生成报告；
- [ ] 默认配置回归当前双均线结果。

交付物：Bar 达到 Tick 当前订单与账户语义的基础对齐。

### Phase 5：专业 Bar fill models

- [ ] ConservativeOhlc；
- [ ] Touch；
- [ ] VolumeLimited；
- [ ] 模型能力校验；
- [ ] partial fill 和稳定流动性分配；
- [ ] model id/合成成交标记。

交付物：多种明确、可解释、不会暗中前视的 Bar 撮合模型。

### Phase 6：统一 runtime、Result 和 Reset

- [x] ABI v7（含 venue order ID、sequence、venue/instrument、reason）；
- [ ] 统一 order/fill/position event projector；
- [ ] account views；
- [ ] BacktestResult；
- [ ] 可选详细 recorder；
- [ ] PreparedRunner + reset/reuse；
- [ ] Python/Rust API 文档和迁移指南。

交付物：专业回测生命周期和批量运行接口。

### Phase 7：永续专业能力

- [ ] funding 回测数据源和 `on_funding`；
- [ ] funding account settlement；
- [ ] reduce-only；
- [ ] margin/leverage/liquidation；
- [ ] Tick/Bar/Hybrid 共用账户结果。

交付物：符合 Titan 加密永续定位的完整账户和风控基础。

## 17. 兼容和发布策略

### 17.1 Feature flag 双跑

迁移 Tick 时短期保留：

```text
legacy_execution
shared_execution
```

测试环境对同一数据双跑并比较 event log hash。生产默认保持 legacy，直到正确性和性能验收完成。

### 17.2 API 兼容

- 原 `run_event_bot()` 保留；
- 新增 `prepare_event_bot()` 或 `PreparedRunner`，支持 bridge/data/indicator 复用；
- 原返回 user `state` 保留；
- 新接口返回 `BacktestResult`，旧接口可从 result 取 user state；
- modify 继续不暴露给 Numba；
- 不支持的模型/订单组合由启动或 submit 明确拒绝。

### 17.3 数据兼容

- Tick flat Event 不变；
- Bar schema 不增加 available_ts；
- latency 和 source 写在 manifest/config/event envelope；
- 新增 schema/ABI version；
- 旧 Bar 文件在默认 zero-latency NextOpen 模式下继续可用。

## 18. 风险与控制

| 风险 | 控制措施 |
|---|---|
| Tick 性能下降 | 泛型单态化、adapter 迁移、逐阶段 benchmark、3% 门槛 |
| Tick 结果静默变化 | golden event log、differential dual-run、固定 tie priority |
| Bar 引入前视 | execution opportunity 与 delivery 分离、禁止使用提交前 Bar、property tests |
| Fee 重复计算 | fee 只在 MatchOutcome→ExecutionReport 计算一次，local ledger 只应用 report |
| Local/Exchange position 混淆 | 分离 ExchangePosition 与 local AccountLedger |
| 相同时间事件不稳定 | `(timestamp,priority,sequence)` 统一键 |
| 抽象层过重 | 热路径泛型/inline，事件 buffer 预分配，不强制 trait object |
| Partial fill 伪真实性 | 每个 Bar model 明确能力和时间语义，NextOpen 不宣称 partial |
| ABI 错位 | ABI v7、Rust/Python offset 测试、旧 ABI fail-fast |
| 大规模重写难回退 | feature flag、adapter、一次只迁移一个职责 |

## 19. 验收定义

共享执行层方案完成必须同时满足：

- [ ] Tick 所有现有测试和 golden event hash 通过；
- [ ] Tick release 性能满足退化门槛；
- [ ] Bar 支持 fee、entry/response latency 和 StateValues；
- [ ] Bar 支持完整 order/fill/position events；
- [ ] Bar cancel/reject/expire 有可观察报告；
- [ ] 至少一个 Bar model 支持可解释 partial fill；
- [ ] Tick/Bar 使用同一 OrderStateMachine、Transport、AccountLedger 和 event projector；
- [ ] NextOpen 默认结果向后兼容；
- [ ] `on_bar` 新订单不能在刚结束 Bar 成交；
- [ ] reset 后重跑结果完全一致；
- [ ] BacktestResult 和可选详细报告可用；
- [ ] 文档明确每个 matching model 的数据要求、成交假设和限制。

## 20. 最终架构判断

正确方向不是“让 Bar 调用 Tick 引擎”，而是：

```text
Tick 和 Bar 共用专业执行系统
Tick 和 Bar 保留不同的市场撮合事实
```

共享层负责订单、延迟、费用、账户和事件一致性；Tick 模型继续专注真实订单簿和队列，Bar 模型
提供明确、保守、可配置的 OHLCV 成交假设。先无行为变化地迁移 Tick，再让 Bar 接入共享层，能把
重构风险和性能风险控制在可验证范围内。
