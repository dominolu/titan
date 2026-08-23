# 共享执行层方案对照功能清单的缺口分析

> 本文保留为实施前差距基线；当前实现与最终验收状态以
> [`shared_execution_layer_implementation_tasks.md`](shared_execution_layer_implementation_tasks.md) 为准。

> 对照文档：
>
> - [`shared_execution_layer_tick_bar_architecture.md`](shared_execution_layer_tick_bar_architecture.md)
> - [`titan_nautilus_backtest_engine_feature_checklist.md`](titan_nautilus_backtest_engine_feature_checklist.md)
>
> 目的：检查共享执行层方案是否足以承载功能清单中的专业回测需求，并区分哪些能力必须进入
> 共享核心、哪些属于相邻基础设施、哪些可以后置。
>
> 本文结论已经整理为可实现、可验收的正式需求基线：
> [`shared_execution_layer_requirements.md`](shared_execution_layer_requirements.md)。

## 1. 总体结论

现有共享执行层方案已经正确覆盖第一层问题：

- Tick/Bar 共用订单状态机；
- entry/response latency；
- FeeModel；
- 基础 AccountLedger；
- ack/reject/cancel/expire/fill/partial-fill；
- `on_order/on_filled/on_position`；
- BacktestResult；
- reset/reuse；
- MatchingModel 能力声明；
- Tick 无行为迁移和 Bar 多 FillModel；
- ABI、测试和性能门槛。

但当前方案仍主要是“单资产 matching model + 内嵌账户”的执行内核。如果直接按现有结构实施，后续
加入同 Venue 多品种共享保证金、组合风控、Funding、Hybrid、实盘统一事件和流式数据时，可能再次
重构核心所有权。

最需要在编码前修正的不是再增加订单类型，而是以下五个结构问题：

1. AccountLedger 不能内嵌在单个 instrument matching core；
2. 必须同时建模 exchange state 和 local-visible state；
3. Scheduler 必须是跨 Venue/Instrument 的全局调度器；
4. 回测 ExecutionReport 必须与实盘 LiveEvent 使用同一个策略事件投影；
5. Hybrid、Timer、Funding 和流式 Bar 必须进入明确阶段，而不是只留在需求清单。

## 2. 覆盖状态总表

| 功能域 | 当前共享层方案 | 评审结果 | 需要完善 |
|---|---|---|---|
| 完整订单状态 | 已设计 | 基本充分 | 预留 trigger/condition 状态，明确 cancel/fill race |
| 请求/响应延迟 | 已设计 | 基本充分 | 增加请求 overtaking 策略和 market-data delivery latency |
| Fee | 已设计 | 基本充分 | fee currency、rounding、rebate 和一次性计算不变量 |
| 单资产账户 | 已设计 | 已覆盖 | 保持 |
| 多资产共享账户 | 未设计 | 关键缺口 | VenueAccount + PortfolioLedger |
| Local/Exchange 双状态 | 只在文字中提到 | 不充分 | 明确两个状态视图和更新时点 |
| Pre-trade risk | 未设计 | 关键缺口 | 预留 LocalRisk/ExchangeRisk/PostTradeRisk trait |
| Margin/Leverage/Liquidation | Phase 7 列项 | 抽象不足 | 账户所有权现在就要支持跨品种 |
| Funding | Phase 7 列项 | 数据/调度不足 | FundingSource、settlement、callback、结果字段 |
| Tick matching | adapter 方案完整 | 已覆盖 | 增加 liquidity consumption/slippage 扩展面 |
| Bar matching | 四种模型 | 基本充分 | 更明确价格/时间/volume 分配和模型 ID |
| Historical liquidity consumption | 未设计 | 缺失 | Tick matching 邻接组件 |
| Fill/Slippage model | 只有 Bar model | 缺失 | 与 QueueModel 分离的 ExecutionQualityModel |
| Hybrid | 未进入实施 Phase | P0 缺口 | 全局 scheduler 的独立 Hybrid phase |
| Timer | 只出现在 priority 描述 | 缺失 | TimerQueue 和无行情推进语义 |
| 实盘 Bar | 未设计 | P0 缺口 | Canonical builder、native candle、恢复去重 |
| 回测/实盘事件一致性 | 未设计 live adapter | 关键缺口 | 统一 ExecutionEventProjector |
| Streaming Bar | 未设计 | 缺失 | chunked BarFeed，不允许全量复制 |
| Instrument/Market status | 未设计 | 缺失 | InstrumentSpec 和 MarketStatusEvent |
| CustomData | 未设计 | 可后置 | Scheduler/DataSource 扩展槽 |
| Result/Recorder | 已设计 | 部分充分 | 增加可复现 metadata、订单/账户报告 |
| Reset/PreparedRunner | 已设计 | 基本充分 | 定义插件、RNG、data source reset |
| Catalog/RunConfig | 未设计 | 可后置 | 属于 orchestration，不应塞入执行核心 |
| Actor/ExecutionAlgorithm | 明确非第一阶段 | 可后置 | 预留 command producer 接口即可 |

## 3. 必须在实现前调整的核心架构

### 3.1 AccountLedger 所有权必须提升

当前方案：

```rust
pub struct SharedExecutionCore<M, LM, AT, FM> {
    matching: M,
    orders: OrderManager,
    transport: OrderTransport<LM>,
    account: AccountLedger<AT, FM>,
}
```

这个结构倾向于“一套 matching model 对应一个账户”。它可以复制当前每 asset `State`，但不能正确
承载：

- 同一交易所多个永续合约共享 collateral；
- cross margin；
- 一个品种盈利释放另一个品种的可用保证金；
- Venue 级手续费等级；
- 多资产 Funding 汇总；
- 组合级风险和强平；
- 跨 Venue Portfolio 汇总。

建议改成三级所有权：

```text
BacktestEngine
├── GlobalScheduler
├── LocalPortfolio
│   ├── VenueAccount[venue]
│   └── PortfolioView
└── VenueExecutionCore[venue]
    ├── OrderTransport
    ├── ExchangeAccountState
    └── InstrumentMatchingCore[instrument]
        ├── MatchingModel
        └── ExchangeOrderStore
```

原则：

- MatchingModel 是 instrument 级；
- Account 是 venue/account 级；
- Portfolio 是 engine/trader 级；
- FeeModel 可以 venue 或 instrument 覆盖；
- AccountLedger 不得复制到每个 instrument；
- Bar 和 Tick instrument 必须可以挂在同一个 VenueAccount 下。

### 3.2 必须明确 Exchange 与 Local 两个状态视图

订单在交易所成交时间 `E`，策略在响应到达时间 `L` 才知道成交：

```text
E: ExchangeAccountState 立即更新
L: LocalAccountView / Portfolio 才更新并触发 callback
```

两者用途不同：

- ExchangeAccountState：交易所侧 reduce-only、margin、liquidation、后续订单校验；
- LocalAccountView：策略实际可见的 position/balance/fee；
- PortfolioView：聚合已经投递到 local 的账户状态；
- ExecutionReport：携带 exchange 发生的 fee/fill/account delta，经 response latency 投递。

如果只在 local response 时更新唯一账户，exchange 在响应延迟期间无法正确校验后续命令；如果只在
exchange fill 时更新唯一账户，策略会提前看到尚未收到的成交。

现有方案提到独立 `ExchangePosition`，但应提升为明确的 `ExchangeAccountState` 接口，而不只是
一条实现备注。

### 3.3 Scheduler 必须是全局而不是每 core 局部

专业多资产回测必须在一个时间轴上合并：

- Tick exchange market event；
- Tick local delivery event；
- completed Bar delivery；
- Bar execution opportunity；
- order command arrival；
- execution report delivery；
- Funding settlement；
- Timer；
- Instrument/Market status；
- Hybrid 的 Bar 与 Tick 流。

统一 key 建议为：

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

`sequence` 保证同源稳定顺序；`source_priority` 允许同时间多流显式配置；`phase` 固定执行语义。

当前方案已经设计 Bar 内部 priority，但还需要统一到 Tick、多 Venue、Funding 和 Timer 的全局
priority contract。

### 3.4 引入 Engine、Venue、Instrument 三层，而不是直接 Core 数组

建议最终结构：

```text
BacktestEngine
├── Scheduler
├── DataSources
├── LocalGateway
│   ├── LocalOrderManager
│   ├── LocalRisk
│   └── LocalPortfolio
├── Venues
│   └── VenueExecutionCore
│       ├── Transport
│       ├── ExchangeRisk
│       ├── ExchangeAccount
│       └── InstrumentMatcher[]
└── RuntimeEventProjector
```

这样可以保留 Titan 的精简性，同时避免未来保证金、Funding 和跨品种账户再次推翻共享层。

## 4. 风险系统需要先预留接口

功能清单把 RiskEngine 识别为 Titan 明显缺口。第一阶段不必实现 Nautilus 同等级的完整 RiskEngine，
但共享层必须现在预留三个阶段，否则以后会侵入 OrderManager 和 AccountLedger：

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
    fn on_account_change(&mut self, account: &ExchangeAccountState, out: &mut RiskActionSink);
}
```

第一阶段默认实现为 `AllowAllRisk`，但调用点必须存在：

```text
strategy command
  -> LocalPreTradeRisk
  -> entry transport
  -> ExchangeRisk
  -> matching
  -> ExchangeAccount update
  -> PostTradeRisk / liquidation actions
```

reduce-only、保证金不足拒单和强平才能在同一框架内实现。

## 5. Funding 设计仍不完整

共享层方案只把 Funding 放在 Phase 7，但功能清单将它列为加密永续 P0。需要补充完整链路：

```text
FundingSource
  -> FundingRate/Payment market event
  -> GlobalScheduler
  -> ExchangeAccountState.apply_funding()
  -> response/delivery policy
  -> LocalAccountView
  -> on_funding(s)
  -> BacktestResult.funding
```

必须明确：

- rate 的生效时间和 payment 的结算时间；
- mark/index price 来源；
- position snapshot 取哪个时间点；
- 多品种同一结算时间顺序；
- exchange state 与 local-visible state 的时间差；
- Funding 是否作为独立账户事件而不是伪造 fill；
- reset 后 Funding cursor 和累计值清零；
- live connector Funding 如何投影成同一个策略事件。

建议把 Funding 从 Phase 7 前移到共享账户稳定后的首个专业能力阶段。

## 6. Hybrid 必须进入明确实施阶段

功能清单把 Hybrid 列为 P0，但共享层方案的迁移 Phase 没有 Hybrid。共享层完成后应新增独立阶段：

```text
Materialized/Streaming Bar Feed ─┐
                                 ├─> GlobalScheduler -> one ExecutionCore
Tick/Depth Feed ─────────────────┘
```

固定同时间 `T` 的语义：

1. 处理 `event_ts < T` 的 Tick/Depth；
2. 投递 `[T-period,T)` 的 Bar；
3. `on_bar(s)`；
4. 处理 strategy commands 和 zero-latency arrivals；
5. 处理 `event_ts >= T` 的 Tick/Depth；
6. Tick matching 产生报告并按 response latency 投递。

Hybrid 不需要新的账户或订单状态机，正是验证共享执行层边界是否正确的关键场景。

## 7. 回测与实盘事件必须使用同一个 Projector

现有方案只描述回测 `ExecutionReport -> callback`，没有说明实盘 connector 事件如何接入。

建议：

```text
Backtest MatchOutcome
    -> ExecutionReport ─┐
                        ├─> ExecutionEventProjector
LiveEvent Order/Fill ───┘       -> on_order/on_filled/on_position/on_funding
```

统一的不是实盘撮合和账户模拟，而是：

- 订单状态和 reason 编码；
- fill 不折叠；
- callback 顺序；
- position/account payload；
- Funding payload；
- ABI dtype；
- 策略可见字段。

否则共享执行层只统一 Tick/Bar 回测，仍无法兑现回测/实盘策略事件一致性。

## 8. Timer 需要成为 Scheduler 第一类事件

当前共享层文档只在 Bar priority 中提到“后续 timer”，没有模块和 Phase。需要增加：

```rust
pub trait TimerQueue {
    fn schedule(&mut self, owner: ComponentId, timer_id: u64, ts: i64);
    fn cancel(&mut self, owner: ComponentId, timer_id: u64);
    fn next_timestamp(&self) -> Option<i64>;
    fn drain_due(&mut self, ts: i64, out: &mut TimerSink);
}
```

验收语义：

- 没有行情时仍推进到 timer；
- callback 新建同时间 timer 的顺序固定；
- reset 清空 timer；
- timer 不能绕过 risk/transport 直接下单；
- 回测与实盘使用相同 timer callback payload。

## 9. Tick 执行真实性仍有两个遗漏

### 9.1 Historical liquidity consumption

功能清单指出：多个策略主动订单可能重复消费同一份历史深度。共享层方案没有消耗账本。

建议将它放在 Tick matching 层而不是 AccountLedger：

```rust
pub trait LiquidityConsumptionModel {
    fn available(&self, market_event_id: u64, side: Side, price_tick: i64) -> f64;
    fn consume(&mut self, market_event_id: u64, side: Side, price_tick: i64, qty: f64);
    fn reset_market_event(&mut self, market_event_id: u64);
}
```

默认 `DisabledLiquidityConsumption` 保持兼容；专业模式显式启用。

### 9.2 Fill/Slippage 与 QueueModel 分离

QueueModel 解决被动单前方数量，不应该同时承担：

- 主动单滑点；
- 限价成交概率；
- 随机拒绝/成交；
- price impact 压力模型。

建议预留：

```rust
pub trait ExecutionQualityModel {
    fn adjust_fill(&mut self, ctx: &FillContext, proposed: ProposedFill)
        -> AdjustedFill;
}
```

所有随机模型必须由显式 seed 驱动，并把 seed 写入 BacktestResult。

## 10. Bar 数据和实盘 Bar 能力尚未进入方案

### 10.1 Streaming BarFeed

当前方案把 `MaterializedBarSource` 拆成 `MaterializedBarFeed`，但仍未解决全量复制。建议先定义：

```rust
pub trait BarFeed {
    fn peek_key(&self) -> Option<EventKey>;
    fn next_batch(&mut self, out: &mut BarBatchBuffer) -> Result<FeedStatus, FeedError>;
    fn reset(&mut self) -> Result<(), FeedError>;
}
```

实现：

- InMemoryBarFeed；
- ChunkedNpyBarFeed；
- ParquetBarFeed；
- MmapBarFeed；
- HybridBarFeed adapter。

共享 execution core 不应关心数据是否一次加载。

### 10.2 实盘 BarSource

功能清单要求：

- Canonical streaming builder；
- exchange native closed candle；
- watermark/迟到数据规则；
- 断线 REST 补齐；
- `(asset,timeframe,open_ts)` 去重；
- 已发布 Bar 不回写。

这些不属于 SharedExecutionCore，但必须作为相邻 `RuntimeEventSource` 进入路线图，并通过同一个
Bar callback/history/projector。

## 11. InstrumentSpec 和市场状态需要成为共享配置

当前 capability matrix 只有订单/撮合布尔能力，还缺少统一 InstrumentSpec：

```rust
pub struct InstrumentSpec {
    pub asset_no: usize,
    pub venue_no: usize,
    pub tick_size: f64,
    pub lot_size: f64,
    pub min_qty: f64,
    pub max_qty: f64,
    pub min_notional: f64,
    pub contract_size: f64,
    pub settlement_currency: CurrencyId,
    pub margin_currency: CurrencyId,
}
```

订单精度和能力校验应基于 InstrumentSpec，不能继续散落在 depth、builder 和 FFI command 验证中。

后续增加：

- InstrumentUpdate；
- MarketStatusEvent；
- trading session；
- expiry/settlement hooks。

第一阶段可只支持静态 spec，但结构必须可版本化更新。

## 12. 订单状态机应为未来类型预留状态

共享层方案把 stop/OCO 等列为非第一阶段，这是合理的；但 StateMachine 现在应避免只适配
Market/Limit。

建议预留：

- `TriggerState`：Inactive/Armed/Triggered；
- `ContingencyId`；
- parent/child relationship；
- GTD expire timestamp；
- reduce-only flag；
- close-only/liquidation origin；
- cancel/replace correlation ID。

第一阶段能力矩阵拒绝这些字段，但数据结构和 report reason 不应阻止后续扩展。

需要补充 race contract：

- cancel 与 fill 同时间；
- partial fill 后 cancel；
- FOK 多档流动性检查；
- order 到期与 market event 同时间；
- liquidation order 与策略订单优先级；
- duplicate client order ID 的 local/exchange reject 区别。

## 13. Result 和审计信息还不够专业

现有 BacktestResult 主要是计数和账户快照。建议增加可复现 metadata：

```text
engine_version
git_revision
strategy_id/version
runtime_abi_version
data_manifest_hash
config_hash
matching_model_id/version
fee_model_id/version
latency_model_id/version
risk_model_id/version
random_seed
start/end exchange time
start/end delivery time
wall/CPU time
warning/capability downgrade list
```

并提供可选的有界/流式 AuditRecorder：

- orders；
- execution reports；
- fills；
- positions；
- account deltas；
- funding；
- liquidation；
- model diagnostics。

默认关闭详细日志，避免影响 HFT 性能；开启后写 chunk，不在内存无界增长。

## 14. Reset contract 需要覆盖更多组件

当前 reset 清单已经较完整，还应增加：

- VenueAccount/Portfolio；
- LocalRisk/ExchangeRisk/PostTradeRisk；
- Funding source 和 settlement cursor；
- TimerQueue；
- RNG seed/state；
- liquidity consumption；
- execution quality model；
- simulation hooks；
- streaming data chunk/cache position；
- Instrument/Market status；
- audit recorder flush/rotate。

必须区分：

- `reset()`：回到同一配置的初始状态；
- `rewind_data()`：只重置数据源；
- `clear_results()`：只清统计输出；
- `dispose()`：不可恢复地释放资源。

## 15. 高层平台能力不应塞进共享执行核心

功能清单还包含 Catalog、RunConfig、CustomData、Actor、ExecutionAlgorithm 和动态订阅。它们确实是
Titan 与 Nautilus 的差距，但不应全部进入 SharedExecutionCore。

建议边界：

| 能力 | 所属层 | 当前动作 |
|---|---|---|
| Catalog/Parquet query | Data/Orchestration | 后置，但先统一 DataManifest |
| RunConfig/Batch Node | Orchestration | PreparedRunner 稳定后实现 |
| CustomData | DataSource/Scheduler | 预留 custom event envelope |
| Actor | Strategy runtime | 暂不实现 |
| ExecutionAlgorithm | Command producer | 预留独立 command producer，不进入 matching |
| Dynamic subscription | Data runtime | 实盘优先，回测后置 |
| SimulationModule | Venue hooks | 预留 pre_market/post_settlement/reset hooks |

共享核心只需要定义扩展接口，不应承担 Catalog 查询、策略注册或批量任务管理。

## 16. 建议修订后的目标架构

```text
Python/Rust Strategy
        │
        ▼
RuntimeEventProjector / CommandAdapter
        │
        ▼
LocalGateway
├── LocalOrderManager
├── LocalPreTradeRisk
└── LocalPortfolioView
        │ entry latency
        ▼
GlobalScheduler ───────── Timer/Funding/MarketStatus/DataSources
        │
        ▼
VenueExecutionCore
├── ExchangeRisk
├── ExchangeAccountState
├── OrderTransport
└── InstrumentMatchingCore[]
    ├── Tick L2/L3 Matching + Queue + LiquidityConsumption
    └── Bar Matching + BarFillModel
        │
        ▼
ExecutionReport + response latency
        │
        ▼
LocalAccount/Portfolio update
        │
        ▼
on_order -> on_filled -> on_position/on_funding
        │
        ▼
BacktestResult / AuditRecorder
```

## 17. 建议调整后的实施优先级

### P0-A：编码前修正抽象

- [ ] Account 从 instrument core 提升到 VenueAccount/Portfolio；
- [ ] 明确 ExchangeAccountState 与 LocalAccountView；
- [ ] GlobalScheduler 覆盖 venue/asset/source priority；
- [ ] InstrumentSpec；
- [ ] Risk 三阶段空实现接口；
- [ ] Backtest/live 共用 ExecutionEventProjector。

### P0-B：完成 Tick 无行为迁移

- [ ] OrderStateMachine/Report；
- [ ] Transport；
- [ ] LocalOrderManager；
- [ ] VenueAccount 基础 StateValues；
- [ ] Tick adapters；
- [ ] golden differential 和性能验收。

### P0-C：Bar 专业基础

- [ ] NextOpen 接共享执行层；
- [ ] fee/latency/account；
- [ ] order/fill/position callbacks；
- [ ] cancel/reject/expire reports；
- [ ] Streaming BarFeed 接口；
- [ ] PreparedRunner/reset/result。

### P0-D：永续与统一运行时

- [ ] Funding source/settlement/callback；
- [ ] margin/leverage/liquidation；
- [ ] reduce-only；
- [ ] Hybrid；
- [ ] Timer；
- [ ] live event adapter；
- [ ] Canonical/native live Bar source。

### P1：执行真实性

- [ ] Historical liquidity consumption；
- [ ] ExecutionQuality/Slippage model；
- [ ] ConservativeOhlc/Touch/VolumeLimited；
- [ ] stop-market/stop-limit/GTD；
- [ ] 标准 audit reports；
- [ ] Instrument/Market status。

### P2：平台化

- [ ] DataManifest/Catalog；
- [ ] RunConfig/BatchNode；
- [ ] CustomData；
- [ ] Simulation hooks；
- [ ] OCO/OTO/bracket；
- [ ] 多币种 Portfolio；
- [ ] ExecutionAlgorithm command producer。

## 18. 最终判断

现有共享执行层方案的方向正确，可以解决 Bar 绕过 Tick 执行管线的问题，但在真正编码前至少还要
完善：

1. 多资产账户所有权；
2. exchange/local 双状态；
3. 全局多源 scheduler；
4. 风险扩展点；
5. 回测/实盘统一事件投影；
6. Funding/Hybrid/Timer 的明确阶段；
7. liquidity consumption/slippage；
8. streaming Bar 和实盘 Bar；
9. InstrumentSpec；
10. 可复现 Result/Audit metadata。

其中前五项属于架构骨架，应该在 Phase 1 之前修正；后五项可以在共享骨架稳定后逐步接入。Catalog、
Actor 和通用条件单平台不应阻塞共享执行层落地，但需要在边界上预留扩展位置。
