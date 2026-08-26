# Titan Tick/Bar 共享执行层完整需求规格

> 文档状态：需求基线（已完成实现与最终验收，2026-08-24）
> 验收证据：[`shared_execution_layer_acceptance_report.md`](shared_execution_layer_acceptance_report.md)
> 适用范围：Titan/HFTBacktest 的 Tick/L2/L3、Bar、Hybrid 回测，以及与实盘策略事件的统一
> 依据：
>
> - [`shared_execution_layer_tick_bar_architecture.md`](shared_execution_layer_tick_bar_architecture.md)
> - [`shared_execution_layer_gap_analysis.md`](shared_execution_layer_gap_analysis.md)
> - [`titan_nautilus_backtest_engine_feature_checklist.md`](titan_nautilus_backtest_engine_feature_checklist.md)

## 1. 文档目的

本文把共享执行层的架构方向转换成可以实现、测试和验收的明确需求。本文定义：

- 系统边界和组件所有权；
- Tick、Bar、Hybrid、回测和实盘共用的订单与账户语义；
- exchange time 与 local-visible time 的处理；
- 多 Venue、多 Instrument、Funding、Timer 和风险事件的确定性调度；
- 性能、复现、审计、重置和兼容要求；
- 分阶段交付范围和完成标准。

本文不规定每个 Rust 文件的最终名称，但所有实现不得违反本文的所有权、时序和不变量。

## 2. 规范用语

| 用语 | 含义 |
|---|---|
| **必须（MUST）** | 进入目标架构和验收的硬性要求 |
| **禁止（MUST NOT）** | 实现不得出现的行为 |
| **应该（SHOULD）** | 默认应实现；偏离时必须记录理由和影响 |
| **可以（MAY）** | 可选能力，不阻塞当前阶段验收 |

需求编号在实现、测试和评审中保持稳定。需求被推迟时，必须标记目标阶段，不得直接删除。

## 3. 范围

### 3.1 本期范围

共享执行体系必须覆盖：

1. Tick/L2/L3 回测；
2. Materialized 和 Streaming Bar 回测；
3. Tick + Bar Hybrid 回测；
4. Market、Limit、Cancel 以及现有 TIF；
5. 订单请求、交易所处理和响应延迟；
6. Fee、Position、Balance、PnL、Funding 和基础 Margin；
7. 订单、成交、持仓、资金和 Timer 策略事件；
8. 回测与实盘统一事件投影；
9. 多 Venue、多 Instrument 和共享账户；
10. 可重置、可重复、可审计的批量运行。

### 3.2 非目标

以下能力不阻塞第一版共享执行层，但接口不得阻止后续增加：

- 完整 OCO、OTO、OUO、Bracket 订单；
- 通用 Actor/MessageBus 平台；
- 完整 Catalog 查询系统和分布式任务编排；
- 期权行权和全部衍生品生命周期；
- 用合成 Tick 模拟 Bar 内部路径；
- 恢复公共 modify API；修改订单继续使用 cancel/replace；
- 对历史市场施加永久市场冲击。

## 4. 总体目标与约束

### 4.1 功能目标

- **REQ-GEN-001**：Tick 和 Bar 必须共用订单状态机、传输延迟、费用、账户、报告、事件投影和结果统计。
- **REQ-GEN-002**：Tick 和 Bar 必须保留各自独立的市场事实和撮合模型，不得将 Bar 伪装成 Tick。
- **REQ-GEN-003**：同一策略代码必须能够在回测和实盘中使用同一组单参数回调，例如 `on_tick(s)`、`on_bar(s)`、`on_order(s)`、`on_filled(s)`、`on_position(s)`、`on_funding(s)`、`on_timer(s)`、`on_start(s)`、`on_stop(s)`。
- **REQ-GEN-004**：Rust 必须拥有事件队列、时钟、行情状态、撮合、账户和运行循环；Numba 只处理策略事件，不得实现 `_run_loop`。
- **REQ-GEN-005**：所有目标运行模式必须使用相同的订单标识、状态、reason code、成交字段和账户字段定义。
- **REQ-GEN-006**：`on_tick(s)` 每次必须接收一次全局 TickBatch 回调；同一逻辑时间的多个 Venue/Instrument Tick 通过 `asset_no/venue_no` 区分，不得按品种重复跨桥调用。
- **REQ-GEN-007**：`on_bar(s)` 每次必须接收 Scheduler 已确定的全局 BarBatch；batch key、资产排序和多周期分组规则必须稳定并写入 phase contract。

### 4.2 性能目标

- **REQ-PERF-001**：Tick release 基准相对冻结基线的吞吐下降不得超过 3%。
- **REQ-PERF-002**：热路径不得使用 JSON、字符串分发、Python 对象或每事件堆分配。
- **REQ-PERF-003**：Tick matching 必须保留 Rust 单态化泛型能力，不得强制全部通过 trait object。
- **REQ-PERF-004**：Numba callback 必须保持稳定的 C ABI 单指针调用；事件和命令 buffer 必须预分配并复用。
- **REQ-PERF-005**：Bar 必须按事件跳转，不得按固定时间步空转。
- **REQ-PERF-006**：Streaming feed 不得要求将全部历史数据复制进 Rust 常驻内存。
- **REQ-PERF-007**：详细审计默认关闭；开启时必须分块写出或使用有界 buffer，禁止无界内存增长。

### 4.3 确定性目标

- **REQ-DET-001**：相同代码、配置、数据、模型版本和随机种子必须产生逐事件一致的结果。
- **REQ-DET-002**：所有随机模型必须使用显式 seed；禁止使用隐式系统随机源。
- **REQ-DET-003**：同时间戳事件顺序必须由全局排序键决定，不得依赖 HashMap 顺序、指针地址或线程调度。
- **REQ-DET-004**：并列事件必须保留数据源内的稳定顺序。

## 5. 目标组件与所有权

```text
BacktestEngine / LiveRuntime
├── GlobalScheduler
├── DataSources
├── TimerQueue
├── LocalGateway
│   ├── LocalOrderManager
│   ├── LocalPreTradeRisk
│   └── LocalPortfolioView
├── VenueExecutionCore[venue]
│   ├── OrderTransport
│   ├── ExchangeRisk
│   ├── ExchangeAccountState
│   └── InstrumentMatchingCore[instrument]
│       ├── TickMatchingModel 或 BarMatchingModel
│       ├── ExchangeOrderStore
│       └── 可选执行真实性模型
├── ExecutionEventProjector
├── RuntimeCallbackBridge
└── BacktestResult / AuditRecorder
```

### 5.1 Engine 层

- **REQ-OWN-001**：Engine 必须拥有全局 Scheduler、DataSources、TimerQueue、LocalGateway、Venue 集合、Projector 和 Result。
- **REQ-OWN-002**：Engine 必须维护唯一逻辑时间轴，禁止每个 Instrument 独立推进导致跨资产事件乱序。
- **REQ-OWN-003**：Engine 必须负责运行生命周期：prepare、start、run、stop、reset 和 dispose。

### 5.2 Venue 层

- **REQ-OWN-010**：每个 Venue 必须拥有一个或多个账户级 `ExchangeAccountState`，不得把账户复制到每个 Instrument matcher。
- **REQ-OWN-011**：Venue 必须拥有交易所侧风险校验、订单传输配置和 Instrument matcher 集合。
- **REQ-OWN-012**：同 Venue 下多个 Instrument 必须可以共享 collateral、手续费等级、保证金和 Funding 汇总。

### 5.3 Instrument 层

- **REQ-OWN-020**：MatchingModel 必须是 Instrument 级组件，只决定订单在市场条件下能否成交、成交价格、数量和 maker/taker 属性。
- **REQ-OWN-021**：MatchingModel 禁止直接更新策略可见账户、直接调用策略 callback 或直接生成 Python 对象。
- **REQ-OWN-022**：Tick、Bar matcher 必须通过统一 `MatchOutcome` 向共享执行层输出结果。

### 5.4 高层平台边界

- **REQ-BOUND-001**：Catalog、RunConfig、BatchNode 属于 Data/Orchestration 层，不得进入 SharedExecutionCore。
- **REQ-BOUND-002**：Actor 属于策略运行时，不得成为撮合前置依赖。
- **REQ-BOUND-003**：ExecutionAlgorithm 是独立的 command producer，必须通过与策略相同的 command 接口提交订单。
- **REQ-BOUND-004**：CustomData 通过 DataSource 和 Scheduler 扩展槽接入，不得修改订单状态机。
- **REQ-BOUND-005**：SimulationModule 只能通过显式 Venue hooks 接入，并必须支持 reset。

## 6. 统一领域模型

### 6.1 InstrumentSpec

每个 Instrument 必须由统一、可版本化的静态或动态规格描述，至少包含：

```rust
pub struct InstrumentSpec {
    pub instrument_id: InstrumentId,
    pub asset_no: u32,
    pub venue_no: u32,
    pub tick_size: f64,
    pub lot_size: f64,
    pub min_qty: f64,
    pub max_qty: f64,
    pub min_notional: f64,
    pub contract_size: f64,
    pub price_currency: CurrencyId,
    pub settlement_currency: CurrencyId,
    pub margin_currency: CurrencyId,
    pub instrument_type: InstrumentType,
}
```

- **REQ-INST-001**：价格、数量和名义金额检查必须集中使用 InstrumentSpec，禁止散落在 FFI、builder 和 matcher 中形成不同规则。
- **REQ-INST-002**：非法精度、低于最小数量、超过最大数量或低于最小名义金额必须产生结构化 reject reason。
- **REQ-INST-003**：第一阶段可以只支持启动时静态 InstrumentSpec，但数据结构必须预留版本和生效时间。
- **REQ-INST-004**：后续 InstrumentUpdate、MarketStatus、交易时段、到期和结算事件必须可由 Scheduler 调度。

### 6.2 Order 与请求

- **REQ-ORD-001**：FFI `OrderCommand` 只作为 ABI 传输结构；进入 Rust 后必须转换为内部 `ExecutionCommand`。
- **REQ-ORD-002**：内部订单必须具有稳定的 client order ID、venue order ID、instrument、side、type、TIF、price、qty、filled qty、状态和时间戳。
- **REQ-ORD-003**：订单必须预留 `TriggerState`、GTD expiry、reduce-only、contingency ID、parent/child、origin 和 cancel/replace correlation ID。
- **REQ-ORD-004**：未启用的订单能力必须在提交时 fail-fast 拒绝，不得静默降级。
- **REQ-ORD-005**：公共 API 禁止 modify；cancel/replace 必须表现为两个可审计且具有各自延迟的请求。

### 6.3 订单状态

最低状态集合必须覆盖：

```text
Initialized -> Submitted -> Accepted -> PartiallyFilled -> Filled
                       \-> Rejected
                       \-> PendingCancel -> Canceled
                       \-> Expired
```

- **REQ-ORD-010**：所有状态转换必须由统一状态机校验；非法转换必须返回内部错误并进入审计。
- **REQ-ORD-011**：每个 execution report 必须携带 exchange timestamp、delivery timestamp、sequence、order ID、状态和 reason code。
- **REQ-ORD-012**：partial fill 后 cancel 必须保留已成交数量和费用，剩余量进入 Canceled。
- **REQ-ORD-013**：duplicate client order ID 必须区分 local reject 和 exchange reject。
- **REQ-ORD-014**：cancel 与 fill、expire 与 fill 同时发生时必须按全局 phase contract 决定，结果不得依赖容器遍历顺序。
- **REQ-ORD-015**：FOK 必须在一次原子检查中确认全部可成交数量；IOC 必须成交可成交部分并立即取消余量。

### 6.4 MatchOutcome 与 ExecutionReport

- **REQ-REP-001**：Matcher 只能输出 `MatchOutcome`，例如 NoFill、Fill、PartialFill、Reject、Expire、CancelResult。
- **REQ-REP-002**：共享执行层必须根据 MatchOutcome 执行状态转换、Fee 计算、exchange account 更新并生成 ExecutionReport。
- **REQ-REP-003**：同一次 fill 的费用只能计算一次；投递到 local 时不得重复计算。
- **REQ-REP-004**：每笔部分成交必须保留为独立 fill，不得为了 callback 或统计而折叠。

## 7. Exchange 状态与 Local 可见状态

系统必须同时维护两个状态视图：

```text
exchange event time E:
    ExchangeAccountState 立即更新
    ExecutionReport 进入 response transport

local delivery time L:
    LocalAccountView / LocalPortfolioView 更新
    策略 callback 才能看到该变化
```

- **REQ-ACC-001**：ExchangeAccountState 是交易所真实状态，用于 margin、reduce-only、后续订单校验、Funding 和 liquidation。
- **REQ-ACC-002**：LocalAccountView 是策略已收到报告后的可见状态。
- **REQ-ACC-003**：`E < L` 时，策略禁止提前读取 exchange 侧成交、仓位、余额或费用变化。
- **REQ-ACC-004**：`E < L` 时，交易所必须使用已经发生的 exchange 状态校验后续到达订单，即使策略尚未收到旧报告。
- **REQ-ACC-005**：ExecutionReport 必须携带足够的确定性 account delta，使 local view 在投递时得到与对应 exchange 变化一致的结果。
- **REQ-ACC-006**：BacktestResult 必须可以分别报告 exchange-final 和 local-delivered-final；正常结束前必须定义是否 drain 全部响应。默认必须 drain。

### 7.1 VenueAccount 与 PortfolioLedger

- **REQ-ACC-010**：VenueAccount 必须聚合同 Venue 的 balance、position、fee、realized/unrealized PnL、margin、Funding 和 trading volume/value。
- **REQ-ACC-011**：PortfolioLedger 必须聚合已投递到 local 的多个 VenueAccount，不得绕过响应延迟直接读取 exchange state。
- **REQ-ACC-012**：账户必须明确 base/quote/settlement/margin currency，禁止用无币种语义的单一 `f64 balance` 代替多币种账本。
- **REQ-ACC-013**：第一阶段可只启用单结算币种，但内部 key 必须包含 CurrencyId。
- **REQ-ACC-014**：净持仓 NETTING 为第一阶段默认；HEDGING 可后置，但 PositionId 扩展不得被数据模型永久封死。

## 8. 全局调度器与时间语义

### 8.1 时间字段

- `exchange_ts`：市场或交易所事件实际发生时间；
- `delivery_ts`：事件到达本地、策略可见的时间；
- `open_ts`：Bar 覆盖区间开始时间；
- `close_ts`：Bar 覆盖区间结束时间。

- **REQ-TIME-001**：Bar 数据结构禁止增加 `available_ts`。
- **REQ-TIME-002**：Bar 的可见时间必须由 EventEnvelope.delivery_ts 表示；默认 `delivery_ts = close_ts + feed_latency`。
- **REQ-TIME-003**：Tick 必须保留原生 exch_ts/local_ts，不得在共享层折叠为单时间戳。
- **REQ-TIME-004**：订单必须分别记录 local submit、exchange arrival、exchange event 和 local response delivery 时间。

### 8.2 全局排序键

```rust
pub struct EventKey {
    pub timestamp: i64,
    pub phase: u16,
    pub source_priority: u16,
    pub venue_no: u32,
    pub asset_no: u32,
    pub sequence: u64,
}
```

- **REQ-SCH-001**：所有 Venue、Instrument、market data、command arrival、report delivery、Funding、Timer 和 status event 必须进入同一个全局逻辑调度器。
- **REQ-SCH-002**：比较顺序必须严格为 `(timestamp, phase, source_priority, venue_no, asset_no, sequence)`，除非兼容测试证明某类旧 Tick 事件需要固定例外；例外必须显式编码并记录。
- **REQ-SCH-003**：source_priority 必须由配置或数据清单固定，禁止运行中隐式改变。
- **REQ-SCH-004**：sequence 必须单调且同源稳定；输入缺少 sequence 时必须在 ingest 阶段确定性生成。
- **REQ-SCH-005**：数据耗尽后，如仍有 Timer、Funding、订单 arrival 或 report delivery，Scheduler 必须继续推进。

### 8.3 同时间戳 phase contract

默认 phase 顺序必须显式定义为：

1. 到期但尚未投递的旧 response delivery；
2. Instrument/MarketStatus，以及配置为 `BeforeSettlementEvents` 的 Funding 结算；
3. 该时点前已完成的市场数据本地投递；
4. 策略 callback；
5. callback 产生的 command 和零 entry latency arrival；
6. 当前时点的 exchange matching opportunity；
7. 配置为 `AfterSettlementEvents` 的 Funding 结算；
8. 零 response latency report delivery；
9. 到期 Timer callback；
10. post-trade risk/liquidation 产生的后续命令，直到当前时点稳定。

- **REQ-SCH-010**：具体 phase 数值必须集中定义，并写入兼容测试。
- **REQ-SCH-011**：callback 在当前时点产生的事件必须反复处理到稳定，或明确排入未来时间；禁止遗漏在队列中。
- **REQ-SCH-012**：若为了保持现有 Tick EventSet 顺序需要调整上述默认次序，必须以 characterization test 的结果为准，并在配置 manifest 中记录 phase contract 版本。

当前实现的 phase contract 为 v2。Funding boundary 是交换所结算语义，不是 callback 排序提示：
`BeforeSettlementEvents` 必须在同时间 matching 前读取仓位，`AfterSettlementEvents` 必须在 matching 后、
零延迟本地回报前读取仓位；其 callback 仍由独立 `delivery_ts` 决定。

### 8.4 Bar 时序

对 Bar N `[open_ts, close_ts)`，默认 NextOpen 语义必须为：

```text
旧响应投递
-> Bar N close 时投递完整 Bar
-> on_bar(s)
-> 消费策略命令
-> 零 entry latency 请求到达交易所
-> 最早在 Bar N+1 open 撮合
-> 零 response latency 报告投递
```

- **REQ-BAR-001**：`on_bar` 中提交的订单禁止利用刚结束 Bar N 的 high/low/volume 成交。
- **REQ-BAR-002**：正 entry latency 使订单错过 N+1 open 时，必须等待下一次符合模型的撮合机会。
- **REQ-BAR-003**：callback 开始时，当前 Bar 尚未写入历史环；`open[-1]` 必须表示前一根已关闭 Bar，`open[-2]` 表示前两根。负索引采用 Python 语义，非负索引从当前保留窗口最老的 Bar 开始，因此 `open[1]` 表示窗口内第二老的 Bar，而不是前一根。当前 Bar 只能在 callback 返回后进入历史环；OHLCV 的索引规则必须完全一致。
- **REQ-BAR-004**：空 Bar 不得创造流动性或成交机会。

### 8.5 Hybrid 时序

对同一时点 T：

1. 处理 `event_ts < T` 的 Tick/Depth；
2. 投递 `[T-period, T)` 的完整 Bar；
3. 调用 `on_bar(s)`；
4. 处理命令及零 entry latency arrival；
5. 处理 `event_ts >= T` 的 Tick/Depth；
6. Tick matcher 产生报告并按 response latency 投递。

- **REQ-HYB-001**：Hybrid 必须共用一套 OrderManager、VenueAccount、Portfolio 和 Projector。
- **REQ-HYB-002**：Bar 只作为信号数据时，成交必须由配置指定的 Tick matcher 决定，不得同时由 Bar matcher 重复成交。
- **REQ-HYB-003**：每个 Instrument 必须明确 `execution_source = Tick | Bar`，禁止自动猜测。
- **REQ-HYB-004**：缺失 Tick 区间的行为必须配置为 Error、NoLiquidity 或显式 BarFallback；默认必须是 Error。

## 9. 风险系统

共享层必须预留并调用三个风险阶段：

```rust
pub trait LocalPreTradeRisk {
    fn check(&mut self, request: &OrderRequest, portfolio: &PortfolioView)
        -> RiskDecision;
}

pub trait ExchangeRisk {
    fn check_arrival(&mut self, order: &Order, account: &ExchangeAccountState)
        -> RiskDecision;
}

pub trait PostTradeRisk {
    fn on_account_change(&mut self, account: &ExchangeAccountState,
                         out: &mut RiskActionSink);
}
```

- **REQ-RISK-001**：调用链必须是 `strategy -> LocalPreTradeRisk -> transport -> ExchangeRisk -> matching -> account update -> PostTradeRisk`。
- **REQ-RISK-002**：第一阶段必须提供 `AllowAllRisk`，但不得省略任何调用点。
- **REQ-RISK-003**：local reject 不进入 entry transport；exchange reject 必须经过 response latency 返回策略。
- **REQ-RISK-004**：所有 RiskDecision 必须包含稳定 reason code 和可选数值限制。
- **REQ-RISK-005**：PostTradeRisk 必须通过 RiskActionSink 产生可审计命令，禁止直接篡改订单或持仓。
- **REQ-RISK-006**：reduce-only 校验必须使用 exchange position，而不是延迟后的 local position。
- **REQ-RISK-007**：Margin、leverage、liquidation 必须在 VenueAccount 级实现，支持多个 Instrument 共同影响可用保证金。

## 10. Funding

```text
FundingSource
-> FundingRate/Settlement event
-> GlobalScheduler
-> ExchangeAccountState.apply_funding()
-> AccountReport 经 delivery policy
-> LocalAccountView
-> on_funding(s)
-> BacktestResult
```

- **REQ-FUND-001**：Funding 必须是独立账户事件，禁止伪造成 fill。
- **REQ-FUND-002**：Funding 数据必须区分 rate publication time、rate effective time 和 settlement time。
- **REQ-FUND-003**：配置必须明确 mark/index price 来源、position snapshot 时点、计算公式、币种、舍入方式和边界包含规则。
- **REQ-FUND-004**：同结算时间多 Instrument 必须按 EventKey 确定性执行。
- **REQ-FUND-005**：exchange account 在 settlement time 更新；local account 和 `on_funding` 在 delivery time 更新/触发。
- **REQ-FUND-006**：实盘 Funding 事件必须通过同一个 ExecutionEventProjector 投影为相同 ABI payload。
- **REQ-FUND-007**：结果必须分别统计每个 Venue、Instrument、Currency 的 Funding，并提供总和。
- **REQ-FUND-008**：reset 必须重置 Funding cursor、累计金额和待投递报告。

## 11. Timer

- **REQ-TMR-001**：Timer 必须是 Scheduler 的第一类事件，并支持 schedule、cancel、next_timestamp、drain_due 和 reset。
- **REQ-TMR-002**：无行情时系统仍必须推进到下一个 Timer。
- **REQ-TMR-003**：Timer callback 中产生的订单必须经过正常 local risk 和 transport，不得直接进入 matcher。
- **REQ-TMR-004**：同 owner、timer_id 的重复注册行为必须配置为 Replace 或 Reject；默认 Replace。
- **REQ-TMR-005**：同时间 Timer 的顺序必须由 owner ID、timer ID 和 sequence 固定。
- **REQ-TMR-006**：回测和实盘必须向策略提供相同的 Timer payload。

## 12. Tick 执行真实性

### 12.1 Historical liquidity consumption

- **REQ-TICK-001**：系统必须提供可选 `LiquidityConsumptionModel`，防止多个策略订单重复消费同一历史事件的同一档流动性。
- **REQ-TICK-002**：消耗账本属于 Tick matcher 邻接组件，不得进入 AccountLedger。
- **REQ-TICK-003**：默认 `DisabledLiquidityConsumption` 必须保持旧 Tick 行为。
- **REQ-TICK-004**：启用时，可用量必须按 market event ID、side、price level 和已消费量计算。
- **REQ-TICK-005**：历史事件切换、reset 和重跑必须正确清理消耗状态。

### 12.2 ExecutionQualityModel

- **REQ-TICK-010**：滑点、主动成交质量、成交概率和压力模型必须与 QueueModel 分离。
- **REQ-TICK-011**：QueueModel 只负责被动订单前方队列估计。
- **REQ-TICK-012**：ExecutionQualityModel 只能调整 matcher 提议的 fill，不能突破订单限价、可用历史流动性或 InstrumentSpec。
- **REQ-TICK-013**：随机执行质量模型必须记录 model ID、version、parameters hash 和 seed。
- **REQ-TICK-014**：默认 IdentityExecutionQuality 必须保持现有成交结果。

## 13. Bar 数据与实盘 Bar

### 13.1 BarFeed

统一接口必须至少支持：

```rust
pub trait BarFeed {
    fn peek_key(&self) -> Option<EventKey>;
    fn next_batch(&mut self, out: &mut BarBatchBuffer)
        -> Result<FeedStatus, FeedError>;
    fn reset(&mut self) -> Result<(), FeedError>;
}
```

- **REQ-DATA-001**：必须提供 InMemory 和至少一种端到端分块 feed；目标实现包括 ChunkedNpy、Parquet 和 Mmap。
- **REQ-DATA-002**：feed 必须验证 `(delivery_ts/close_ts, timeframe, venue, asset, sequence)` 排序和重复记录。
- **REQ-DATA-003**：Bar 数据必须显式标识 complete/partial/empty；回测默认只允许 complete Bar。
- **REQ-DATA-004**：Bar schema 只包含市场事实，不包含 available_ts。
- **REQ-DATA-005**：分块边界不得改变 batch 分组、历史环或 callback 次数。
- **REQ-DATA-006**：reset 后相同 feed 必须从相同逻辑起点重放。

### 13.2 实盘 BarSource

- **REQ-LIVEBAR-001**：必须支持交易所原生 closed candle 和本地 canonical builder 两种来源，并显式配置优先级。
- **REQ-LIVEBAR-002**：canonical builder 必须定义 watermark、允许迟到窗口、缺口、空 Bar 和乱序规则。
- **REQ-LIVEBAR-003**：断线恢复必须支持 REST 补齐，并以 `(instrument, timeframe, open_ts)` 去重。
- **REQ-LIVEBAR-004**：已投递给策略的 complete Bar 禁止回写；迟到修正必须产生独立 correction/diagnostic，不得静默改变历史。
- **REQ-LIVEBAR-005**：恢复后的 Bar 必须进入与正常实时 Bar 相同的 callback/history 路径。

## 14. 回测与实盘统一事件投影

```text
Backtest MatchOutcome -> ExecutionReport ─┐
                                          ├-> ExecutionEventProjector
Live connector Order/Fill/AccountEvent ───┘  -> ABI event buffers -> callbacks
```

- **REQ-PROJ-001**：回测和实盘必须共用 ExecutionEventProjector。
- **REQ-PROJ-002**：Projector 必须统一 order status、reason code、fill、position、account、Funding 和 Timer payload。
- **REQ-PROJ-003**：Projector 只能投影策略已可见的 local event，禁止直接读取 exchange-only state。
- **REQ-PROJ-004**：同一 report 的 callback 顺序必须固定；默认 `on_order -> on_filled -> on_position/on_funding`。
- **REQ-PROJ-005**：position callback 只在策略可见 position 实际变化时触发。
- **REQ-PROJ-006**：实盘 connector 的重复事件必须通过稳定事件 ID 去重；去重不得折叠合法的多个 partial fills。
- **REQ-PROJ-007**：未知实盘状态必须显式报错或映射为 Unknown reason，禁止静默映射为 Accepted/Filled。
- **REQ-PROJ-008**：ABI 版本和 dtype layout 必须启动时验证；不兼容必须 fail-fast。

## 15. Fee、PnL、Margin 与舍入

- **REQ-FEE-001**：FeeModel 必须返回 fee amount、currency 和 maker/taker classification。
- **REQ-FEE-002**：maker rebate 必须允许为负费用。
- **REQ-FEE-003**：费用舍入必须由 Venue/Currency 规则确定，并记录在配置 hash 中。
- **REQ-FEE-004**：固定 per-order/per-trade 费用必须明确在首次 fill、每次 fill 或 order terminal 时收取，禁止模型实现自行含糊决定。
- **REQ-FEE-005**：PnL 必须使用 InstrumentSpec.contract_size 和结算币种计算。
- **REQ-FEE-006**：Bar 与 Tick 使用相同 FeeModel 时，对相同 fill 序列必须产生相同账户 delta。

## 16. 能力声明与配置校验

每个 matcher、risk、account、feed 和 projector 必须提供稳定的 capability/model descriptor。

- **REQ-CAP-001**：运行前必须验证订单类型、TIF、partial fill、post-only、reduce-only、margin、Funding、数据类型和 execution source 是否受支持。
- **REQ-CAP-002**：不支持的组合必须 fail-fast，不得自动退化，例如用 Bar 代替缺失的 L2 撮合。
- **REQ-CAP-003**：所有模型必须具有稳定 ID 和 version；参数必须参与 config hash。
- **REQ-CAP-004**：显式允许的能力降级必须写入 BacktestResult.warning/capability_downgrade，不得只写日志。
- **REQ-CAP-005**：Engine、Venue 和 Instrument 配置必须在运行开始后冻结；动态更新只能通过带时间戳事件执行。

## 17. Result、审计与可复现性

### 17.1 BacktestResult

结果必须至少包括：

```text
engine_version / git_revision
strategy_id / strategy_version
runtime_abi_version / phase_contract_version
data_manifest_hash / config_hash
matching / fee / latency / risk / execution-quality model ID+version
random_seed
start/end exchange time
start/end delivery time
wall time / CPU time
order/fill/reject/cancel/expire counts
positions / balances / fees / PnL / margin / funding
warnings / capability downgrades
exchange-final / local-delivered-final state
```

- **REQ-RES-001**：BacktestResult 必须足以判断两次结果是否来自相同输入和模型。
- **REQ-RES-002**：结束策略必须明确为 DrainAll、StopAtDataEnd 或 StopAtTime；默认 DrainAll。
- **REQ-RES-003**：结果必须区分运行失败、策略停止、数据结束和风险终止。
- **REQ-RES-004**：统计不得通过重新执行策略计算。

### 17.2 AuditRecorder

- **REQ-AUD-001**：可选审计必须覆盖 command、risk decision、order transition、execution report、fill、account delta、Funding、liquidation 和 diagnostic。
- **REQ-AUD-002**：每条记录必须包含 EventKey 或可还原 EventKey 的字段。
- **REQ-AUD-003**：审计文件必须包含 schema/version 和 run ID。
- **REQ-AUD-004**：禁用审计时热路径开销必须接近零；启用时必须有界或分块输出。

## 18. Reset、重复运行与资源生命周期

必须区分以下操作：

| 操作 | 语义 |
|---|---|
| `reset()` | 保留配置，所有运行状态恢复到初始值 |
| `rewind_data()` | 只重置数据源 cursor/cache |
| `clear_results()` | 只清统计和 recorder 输出状态 |
| `dispose()` | 释放资源，实例不可再次运行 |

- **REQ-RST-001**：reset 必须覆盖 Scheduler、orders、transport、exchange/local accounts、portfolio、risk、Funding、Timer、RNG、liquidity consumption、execution quality、streaming feed、market status、history ring、callback buffers、result 和 recorder。
- **REQ-RST-002**：reset 后重复运行必须与新建等价实例逐事件一致。
- **REQ-RST-003**：reset 不得改变配置、模型参数或随机 seed 初始值。
- **REQ-RST-004**：dispose 后调用 run/reset 必须返回明确错误，不得产生未定义行为。
- **REQ-RST-005**：策略 `on_start` 每次 run 必须调用一次；`on_stop` 每次 run 必须恰好调用一次，包括 `on_start` 或中途 callback 失败。callback 失败时必须先调用 `on_error`，再调用 `on_stop`；`on_error` 和 `on_stop` 只允许撤单，不允许提交新订单。

## 19. 错误处理

- **REQ-ERR-001**：配置错误和 capability 不兼容必须在运行前返回结构化错误。
- **REQ-ERR-002**：运行时数据乱序、无效状态转换、账户不变量破坏必须停止运行，不得继续生成不可信结果。
- **REQ-ERR-003**：策略 callback 错误必须触发 `on_error`，随后按生命周期约定触发 `on_stop`。
- **REQ-ERR-004**：错误必须携带 run ID、component、event key、稳定 code 和上下文；禁止只返回自由文本。
- **REQ-ERR-005**：错误路径不得跳过 recorder flush 和资源清理。

## 20. 测试与验收

### 20.1 Tick 无行为迁移

- **AC-TICK-001**：冻结代表性 L2/L3、Partial/NoPartial、QueueModel、Latency、Fee 数据集。
- **AC-TICK-002**：迁移前后订单状态、fill、价格、数量、exchange/local timestamps、fee 和最终账户逐事件一致。
- **AC-TICK-003**：现有 Tick release 吞吐回退不超过 3%。

### 20.2 Bar 专业执行

- **AC-BAR-001**：默认零延迟、maker/taker 千分之一成交额费用、NextOpen 配置保持现有双均线策略信号、fill 数量和最终持仓；费用必须进入 canonical AccountDelta、现金与净值。
- **AC-BAR-002**：覆盖 ack/reject/cancel/expire/partial/full fill、Fee、entry/response latency 和 local/exchange 双状态。
- **AC-BAR-003**：验证 `on_bar` 新单不能使用当前 Bar high/low，正延迟能正确错过 next open。
- **AC-BAR-004**：InMemory 与 Chunked/Parquet feed 的 callback、history 和结果逐事件一致。

### 20.3 多资产与账户

- **AC-ACC-001**：同 Venue 两个 Instrument 共用 collateral，任一品种成交会影响另一品种的 exchange risk 可用保证金。
- **AC-ACC-002**：在 response latency 窗口中，exchange risk 看到新状态，而策略仍看到旧 local state。
- **AC-ACC-003**：多币种费用和 Funding 分币种守恒，Portfolio 汇总可追溯到 Venue/Instrument 明细。

### 20.4 Scheduler、Hybrid、Timer、Funding

- **AC-SCH-001**：打乱输入文件枚举顺序后，只要 DataManifest/source priority 相同，结果仍一致。
- **AC-SCH-002**：同时间 Bar close、Tick、order arrival、fill response、Funding 和 Timer 的顺序符合 phase contract。
- **AC-HYB-001**：Bar 产生信号、Tick 提供撮合时不存在双重成交或前视。
- **AC-TMR-001**：无行情情况下 Timer 仍触发并可正常提交延迟订单。
- **AC-FUND-001**：Funding 在 exchange settlement 和 local delivery 两个时间点分别更新正确状态。

### 20.5 回测与实盘事件一致性

- **AC-PROJ-001**：给 Projector 输入语义等价的回测 reports 和实盘 events，产生字节布局兼容且字段一致的 callback batch。
- **AC-PROJ-002**：重复实盘事件被去重，多个合法 partial fill 不被折叠。
- **AC-PROJ-003**：策略不需要根据 backtest/live 分支解析订单和成交事件。

### 20.6 Reset 与复现

- **AC-RST-001**：同一 PreparedRunner 连续运行 100 次，逐事件 hash 和 BacktestResult 核心字段一致。
- **AC-RST-002**：reset 后所有队列、cursor、账户、历史环、RNG 和 recorder 状态无泄漏。
- **AC-RES-001**：修改任一数据、模型参数、phase contract 或 seed，对应 manifest/config hash 必须变化。

## 21. 分阶段交付

### P0-A：冻结架构骨架

必须交付：

- Engine/Venue/Instrument 所有权；
- VenueAccount + PortfolioLedger；
- ExchangeAccountState + LocalAccountView；
- GlobalScheduler/EventKey/phase contract；
- InstrumentSpec；
- 三阶段 Risk 空实现和调用点；
- ExecutionEventProjector 接口；
- capability/model descriptor；
- 本文对应的接口级测试骨架。

退出条件：上述所有权和接口完成评审，后续能力不需要再次把 Account 从 Instrument 中迁出。

### P0-B：Tick 无行为迁移

必须交付：

- 共享 OrderStateMachine、ExecutionReport、Transport；
- Tick matcher adapter；
- Fee 和基础 VenueAccount 接线；
- characterization/golden differential；
- release 性能门槛。

退出条件：AC-TICK 全部通过。

### P0-C：Bar 专业基础

必须交付：

- NextOpen 进入共享执行层；
- latency、fee、account、完整订单事件；
- Streaming BarFeed 接口与一个分块实现；
- Result、Audit 基础、PreparedRunner 和完整 reset。

退出条件：AC-BAR、AC-RST 基础项全部通过。

### P0-D：统一专业运行时

必须交付：

- Funding；
- 基础 margin/leverage/liquidation/reduce-only；
- Hybrid；
- Timer；
- live event adapter；
- native/canonical live Bar source 和恢复去重。

退出条件：AC-ACC、AC-HYB、AC-TMR、AC-FUND、AC-PROJ 全部通过。

### P1：执行真实性

包括：

- Historical liquidity consumption；
- ExecutionQuality/Slippage；
- ConservativeOhlc、Touch、VolumeLimited；
- Stop Market、Stop Limit、GTD；
- Instrument/MarketStatus；
- 标准分块 Audit reports。

### P2：平台能力

包括：

- DataManifest/Catalog；
- RunConfig/BatchNode；
- CustomData；
- Simulation hooks；
- OCO/OTO/Bracket；
- 完整多币种 Portfolio；
- ExecutionAlgorithm command producer。

## 22. 需求追踪表

| 原缺口 | 本文覆盖章节 | 最低阶段 |
|---|---|---|
| VenueAccount + PortfolioLedger | 5、7 | P0-A |
| Exchange/Local 双状态 | 7 | P0-A |
| 全局确定性 Scheduler | 8 | P0-A |
| Engine/Venue/Instrument | 5 | P0-A |
| 三阶段 Risk | 9 | P0-A |
| Funding | 10 | P0-D |
| Hybrid | 8.5 | P0-D |
| 统一 Projector | 14 | P0-A/P0-D |
| Timer | 11 | P0-D |
| Tick liquidity/slippage | 12 | P1 |
| Streaming/live Bar | 13 | P0-C/P0-D |
| InstrumentSpec | 6.1 | P0-A |
| 订单未来扩展状态 | 6.2、6.3 | P0-A/P1 |
| Result/Audit | 17 | P0-C |
| Reset contract | 18 | P0-C |
| 平台能力边界 | 5.4 | P2 |

## 23. Definition of Done

共享执行层不能仅以“代码可编译”视为完成。对应阶段必须同时满足：

1. 该阶段所有 MUST 需求已实现，或有经批准的延期记录；
2. 对应验收测试通过；
3. Tick 性能和 ABI 兼容门槛通过；
4. BacktestResult 包含可复现元数据；
5. reset 后重复运行无状态泄漏；
6. 文档中的 phase contract、模型 ID 和配置字段与实现一致；
7. 不支持的能力能够 fail-fast，而不是静默产生近似结果；
8. 回测与实盘策略事件无需模式分支。

只有满足以上条件，才能认定 Tick、Bar 和 Hybrid 已建立同一套专业共享执行基础。
