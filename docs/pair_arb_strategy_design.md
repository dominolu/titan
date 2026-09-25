# `pair_arb`：可复用高频双腿执行内核设计

状态：ABI V13 实现基线 v1.0
目标运行时：Titan Strategy ABI V13
策略包建议名：`strategies/pair_arb`

## 1. 设计目标

`pair_arb` 的核心不是提供更多套利公式，而是提供一套可复用的高频 **Paired Execution Engine（成对执行引擎）**，安全处理：

- initiating/maker 腿挂单、撤单与 cancel-confirm-replace；
- maker partial fill 后按成交增量产生 hedge obligation；
- hedge IOC partial fill、拒单、超时和重试；
- Fill、Order、Position、CommandResult 任意先后顺序及重复事件；
- 两腿 tick、lot、contract multiplier 和 hedge ratio 不一致；
- 行情中断、账户重连、outcome unknown 和策略重启；
- 未对冲数量、净风险、gross exposure 和未对冲持续时间限制。

期现、资金费、统计配对、跨所价差只是不同的**信号来源**。它们不应进入执行状态机，也不应各自重新实现一套成对下单逻辑。

本文不使用 `XEMM` 指代整个内核。XEMM（Cross-Exchange Market Making）只表示跨交易所做市这一种业务场景；`maker -> hedge` 是可以被期现、资金费、统计配对和跨所策略共同使用的执行方式。

v1 聚焦 `maker -> hedge` 高频执行。`taker-taker` 可以共享账本、风险和恢复模块，但作为后续独立 execution policy 加入，不能用条件分支污染 maker-hedge 主状态机。

## 2. 核心边界

```text
Market/Account Facts
        |
        +---------------------> Position & Order Ledger
        |
        v
Signal Model ---- PairIntent ----> Pair Execution Engine ----> OrderCommand
  basis                               |                         Gateway
  funding                             |
  zscore                              +--> Hedge Obligation Ledger
  cross-venue                         +--> Risk Supervisor
```

### 2.1 Signal 负责什么

Signal 只负责回答：

- 哪一侧机会有效；
- initiating leg 应挂什么价格、最大多少风险单位；
- initiating fill 与 hedge qty 的映射关系；
- 该机会的有效期、开仓边界和期望收益；
- 当前是否允许增加组合仓位。

Signal 不得生成 order id、提交/撤销/重试订单、根据 fill 自行下 hedge 单，或保存订单生命周期。

### 2.2 Execution 负责什么

Execution 只消费不可变的 `PairIntent`，负责：

- 将 normalized qty 转换、取整到各腿 lots；
- reconcile desired quotes 与 live orders；
- 捕获每张 maker order 对应的 hedge recipe；
- 每个 maker fill 产生、合并和偿还 hedge obligation；
- 选择 hedge IOC 价格、重试节奏和紧急升级；
- 维护订单、成交、仓位的幂等账本；
- 在信号撤回后继续完成已经产生的 hedge debt；
- 根据风险状态覆盖信号，撤 maker 或只允许降风险。

Execution 不判断 basis、funding 或 z-score 是否有经济意义，也不重新计算信号阈值。

### 2.3 Risk 具有最终决定权

- Signal 可以申请增加风险；
- Risk 可以缩量、拒绝或强制目标归零；
- Execution 必须继续偿还已经存在的 hedge debt；
- Signal 不能放宽任何 risk limit。

开仓门与降险门必须分开。行情或账户 degraded 时应关闭 `increase_risk_gate`，但不能因此阻止 cancel、reconcile 和可证明能降低风险的 hedge 命令。

## 3. Signal/Execution 契约

仅输出 `desired_pair_units` 不足以表达高频 maker-hedge：被动 maker quote 是一个或有成交机会，并不等于当前就要持有该目标仓位。因此 v1 使用 quote-oriented `PairIntent`。

```text
PairIntent
  generation: u64
  created_at_ns: i64
  valid_until_ns: i64
  model_id: u32
  allow_position_increase: bool
  bid_opportunity: QuoteOpportunity
  ask_opportunity: QuoteOpportunity

QuoteOpportunity
  enabled: bool
  initiator_leg: u8
  initiator_side: BUY | SELL
  quote_price: normalized price
  max_initiator_base: f64
  hedge_leg: u8
  hedge_base_per_initiator_base: f64   # signed
  entry_hedge_price_bound: f64
  expected_net_edge_bps: f64
  reason_code: u32
```

例：maker 买入 1 BTC 后，应在 hedge venue 卖出 1 BTC：

```text
initiator_side = BUY
hedge_base_per_initiator_base = -1.0
```

统计配对若 maker 买入 A 后需卖出 `0.63` 单位 B，则 ratio 为 `-0.63`。ratio 在 maker order 创建时冻结，不能在订单存续或迟到 fill 到达时改用新模型的 beta。

### 3.1 Intent 的版本规则

- `generation` 对一个 signal instance 单调递增；
- execution 只接受更高 generation，同 generation 重放必须幂等；
- 新 intent 替代 desired quotes，但不修改旧订单已经冻结的 hedge recipe；
- intent 过期立即撤 initiating orders；
- intent 过期、信号反转或模型切换都不能删除已有 hedge obligation；
- model version 变化时默认 cancel-confirm，再使用新 ratio 创建订单。

### 3.2 价格责任

Signal 给出 maker quote target、正常开仓时可接受的 hedge price bound，以及 expected edge。Execution 负责 post-only/tick rounding、实时 hedge VWAP、IOC aggressive limit，以及风险紧急状态下独立的价格保护。

maker fill 已经发生后，是否对冲不再由信号盈利性决定。正常价格边界失效时，execution 进入 risk hedge 路径，而不是留下裸露仓位等待价差恢复。

## 4. 账本是执行正确性的核心

不能只维护一个 `unhedged_base` 浮点数。至少需要四个互相关联但语义不同的账本。

### 4.1 Position Ledger

每条腿维护：

```text
PositionAnchor
  account_epoch
  account_version
  signed_position_lots

FillOverlay[]
  fill_key
  account_epoch
  account_version
  signed_delta_lots

effective_position = anchor + sum(overlays newer than anchor)
```

Position 是权威绝对事实，Fill 是低延迟增量事实：

- fill 先到时加入 overlay；
- position 后到且 version 覆盖 fill 时，从 overlay 移除该 fill；
- position 先到时，旧版本 fill 不得再次叠加；
- account epoch 增加时清理旧 epoch overlay 并进入 reconcile；
- 不能在每个 position 到达时简单覆盖本地 fill estimate。

### 4.2 Order Ledger

```text
OwnedOrder
  local_order_id
  leg
  side
  generation
  model_id
  requested_price_ticks
  requested_qty_lots
  cumulative_fill_lots
  state
  hedge_recipe_id
  submit_ts
  cancel_ts
  last_account_version
```

终态订单要在固定容量 tombstone ring 中保留一段时间。否则 cancel 后迟到 fill 无法找到原 hedge recipe。

### 4.3 Hedge Obligation Ledger

每个 maker fill 产生一笔债务：

```text
HedgeObligation
  obligation_id
  source_fill_key
  source_order_id
  hedge_leg
  required_signed_base
  allocated_signed_base
  in_flight_signed_base
  first_seen_ts
  deadline_ts
  state: PENDING | IN_FLIGHT | PARTIAL | SATISFIED | FAILED
```

```text
outstanding = required - allocated - conservative_in_flight
```

同方向、同 hedge leg、同风险等级的 obligations 可以聚合成一张 hedge IOC；内部仍需按 FIFO 或明确规则把 hedge fills 分配回源 obligation，以便去重、审计与恢复。

### 4.4 Fill Dedupe Ledger

fill key 至少包含：

```text
(local_account_no, account_epoch, account_version,
 asset_no, order_id, side, cumulative_fill_qty)
```

优先使用 connector 提供的稳定 venue fill id；缺失时才使用上述复合键。只用 sequence 或 order id 都不够。

## 5. 状态机

### 5.1 Engine Mode

```text
WARMING_UP
  -> QUOTING
  -> DRAINING
  -> HEDGE_ONLY
  -> QUOTING/PAUSED

任意状态 -> RECONCILING
不可恢复 -> FAULTED
停止请求 -> STOPPING
```

- `WARMING_UP`：等待两腿 market snapshot、account reconcile、position/balance；
- `QUOTING`：允许 reconcile desired maker quotes；
- `DRAINING`：不新增 quote，等待 cancel terminal 与 hedge completion；
- `HEDGE_ONLY`：只偿还 obligations，不产生 initiating risk；
- `RECONCILING`：存在 outcome unknown、epoch 切换或账本不一致；
- `PAUSED`：无 hedge debt 且明确禁止开仓；
- `FAULTED`：自动动作已超过安全边界，等待受控处理。

### 5.2 Order State

```text
EMPTY
 -> SUBMIT_PENDING
 -> OPEN/PARTIAL
 -> CANCEL_PENDING
 -> TERMINAL

任意非终态 -> OUTCOME_UNKNOWN -> RECONCILING -> OPEN/TERMINAL
```

CommandResult accepted 只表示命令被接收，不表示订单已经 OPEN。OrderChanged/Fill 才是订单和成交事实。

### 5.3 Quote reconcile

```text
desired disabled/expired/risk denied
  -> cancel active order

desired enabled + slot empty
  -> quantize -> pre-trade risk -> submit post-only

desired 与 active 不同
  -> profitability/price guard
  -> requote threshold + min quote lifetime
  -> cancel
  -> 等待 terminal confirmation
  -> submit replacement
```

默认不允许 old/new quote overlap。若未来为降低 latency 允许 overlap，必须把两张订单的最坏同时成交全部计入 gross 和 hedge capacity，作为显式 execution mode。

## 6. Maker fill 到 Hedge 的执行路径

```text
Maker Fill
  -> fill dedupe
  -> 查 source order 冻结的 hedge recipe
  -> 更新 position overlay
  -> 创建 HedgeObligation
  -> 若超过 soft exposure：撤全部 maker quotes
  -> 聚合同方向 outstanding debt
  -> 检查 hedge market/account 可执行性
  -> 计算 depth-aware aggressive IOC limit
  -> quantize，保留不可交易 dust
  -> submit hedge
  -> 根据 Fill/Order/CommandResult 更新
  -> 未偿还部分 retry 或升级
```

### 6.1 对冲数量

```text
maker_fill_base = fill_lots × maker_base_per_lot
required_hedge_base = maker_fill_base × frozen_hedge_ratio
hedge_lots = round_toward_risk_reduction(required_hedge_base / hedge_base_per_lot)
```

不能总是 `floor`。取整方向应最小化交易后绝对 residual exposure，并受 `max_overhedge_base` 限制。小于 hedge lot 的 dust 累积到下一笔；dust notional 或持续时间超过阈值时可选择最小 lot 越界对冲。

### 6.2 对冲价格

普通状态使用完整且连续的 depth 计算目标量 VWAP 和 worst price，IOC limit 使用方向性 slippage buffer。禁止无价格保护的 market order。depth 不足时先撤 maker，再按风险等级决定缩量分片、fallback venue 或紧急价格带。

紧急状态下，signal 的 entry edge 不再是必要条件。每次重试必须重新读取 outstanding debt 和最新 effective positions，绝不重发原始委托量。达到 retry/time/notional 硬限制后进入 `FAULTED`，并保持 maker 全撤。

### 6.3 对冲并发

v1 每个 hedge route 同时最多一张 active hedge order。新 maker fills 在 active hedge 期间继续形成 obligation，但不直接重复下单；待现有 hedge 获得事实结果后，以最新 outstanding debt 再计划。

这样牺牲少量并发，换取清晰的 in-flight 上界和幂等恢复。后续若需 pipeline，多 slot 必须为每笔 obligation 建立明确 allocation。

## 7. 持仓风险模型

至少同时计算：

```text
net_delta_quote       = sum(effective_position_base[i] × delta[i] × mark[i] × fx[i])
gross_notional        = sum(abs(position_notional[i]))
hedge_debt_base       = sum(outstanding obligations in common risk unit)
worst_open_order_risk = maker open qty + uncertain/in-flight command exposure
unhedged_age_ns       = now - oldest unsatisfied obligation first_seen_ts
```

| 级别 | 条件示例 | 动作 |
|---|---|---|
| Normal | 小于 soft limits | 正常 quoting/hedging |
| Soft breach | debt、delta 或 age 超 soft | 撤 initiating quotes，立即 hedge |
| Hard breach | 超 hard notional/age | emergency hedge，禁止恢复 quoting |
| Unknown | account invalidated、command unknown | cancel + reconcile，保守计算最坏敞口 |
| Fatal | reconcile 失败或超过 emergency boundary | fault，停止自动增险 |

特别约束：

- `net_delta` 接近零不代表安全，gross、venue、currency 和 margin 风险仍可能很大；
- 两腿 position ready 前不能 quote；
- hedge account 不 ready 或 hedge depth stale 时，maker quote 必须撤销；
- initiating order 最坏全部成交后的 hedge qty 必须小于当前可执行 hedge capacity；
- balance 增加不能自动扩大配置风险预算。

## 8. Execution Policy 接口

```text
ExecutionPolicy
  on_intent(intent, facts) -> DesiredOrders
  on_initiator_fill(fill, frozen_recipe) -> HedgeObligation[]
  plan_hedge(obligations, facts, risk_level) -> OrderIntent[]
  on_timeout(facts, ledger) -> Action[]
```

实现顺序：

1. `MakerHedgePolicy`：v1 核心；
2. `TakerTakerPolicy`：共享 ledger/risk/reconcile，独立 first-leg/second-leg transaction；
3. `ReduceOnlyUnwindPolicy`：所有策略共用的退出路径。

Signal 不选择具体 TIF、retry 或 command sequencing。配置选择 policy；Risk Supervisor 随时可以把 policy 强制切到 `ReduceOnlyUnwind` 或 `HedgeOnly`。

## 9. Numba 固定内存组织

```text
strategies/pair_arb/
  strategy.py              # 唯一编译输入；Spec、typed state 与 callback wiring
  parameters.json          # 冷路径编译参数

deploy/pair_arb_v13/artifacts/
  pair_arb.titan           # native library + canonical signed manifest
```

Numba 热路径不做 Python 动态分派。`build(parameters)` 在受限编译 worker 中生成固定 typed state 与 module-level `@njit` callbacks；运行时只加载 `.titan`，不读取 Python manifest。

ABI V13 使用策略声明的 aligned structured dtype；私有状态按具名 nested record/fixed array 分组，
不再维护按基础类型拆分的全局 offset：

```text
ctx.state:
  engine | quote slots | private order refs | fill dedupe ring |
  obligations | timers/counters | risk/intent/execution metrics

ctx (runtime-owned readonly views):
  market | positions | balances | accounts | active orders | current event
```

每个私有 ring 必须有容量耗尽策略。order-ref/fill-dedupe/obligation ring 接近满时先停止 quoting
并 drain；不得覆盖尚未 terminal/satisfied 的项目。完整订单事实不进入任何私有 ring。

## 10. 配置边界

```json
{
  "signal": {"type": "executable_spread", "parameters": {}},
  "execution": {
    "policy": "maker_hedge",
    "maker_leg": 0,
    "hedge_leg": 1,
    "requote_threshold_ticks": 1,
    "min_quote_lifetime_ms": 50,
    "hedge_retry_backoff_ms": 20,
    "hedge_retry_limit": 5,
    "normal_slippage_bps": 5.0,
    "emergency_slippage_bps": 30.0,
    "max_active_hedges": 1
  },
  "risk": {
    "max_maker_order_base": 0.001,
    "max_hedge_debt_base_soft": 0.001,
    "max_hedge_debt_base_hard": 0.003,
    "max_unhedged_ms_soft": 200,
    "max_unhedged_ms_hard": 2000,
    "max_net_delta_notional": 100.0,
    "max_gross_notional": 5000.0,
    "max_overhedge_base": 0.0001
  },
  "data": {
    "maker_stale_ms": 1000,
    "hedge_stale_ms": 500,
    "conversion_rate": 1.0,
    "conversion_expiry_ns": 1893456000000000000
  }
}
```

tick、lot、contract multiplier、fee 和 instrument 类型应优先由 runtime metadata 注入。手工 override 必须有来源与有效期。

## 11. 现有跨所做市策略的复用与修正

V13 `strategies/pair_arb` 直接实现成对执行状态机，并保留 maker bid/ask slot、cancel-confirm-replace、fill delta、hedge obligation、position epoch/version 和 readiness/stale gate；不再依赖或保留旧 XEMM/V12 策略包。

通用内核不应直接复制以下耦合：

1. `calculate_targets` 同时计算信号、报价、费用、仓位限制和执行目标；
2. `unhedged_base = maker_position + hedge_position` 硬编码 1:1 同标的关系；
3. maker fill 没有引用订单创建时冻结的 hedge recipe；
4. 单一 `unhedged_base` 无法区分 position exposure、hedge debt 和 in-flight risk；
5. 单 hedge slot 没有 obligation allocation，恢复和多 fill 聚合的审计边界不足；
6. position snapshot 与 fill overlay 的合并规则未作为独立通用账本表达；
7. signal/risk failure 与 hedge permission 共用 gate，扩展时容易在最需要降险时阻断 hedge。

因此重构路线应以 ledger 和 invariants 为中心，而不是先把 `calculate_targets` 抽成多个 signal 函数。

## 12. 必须验证的不变量

```text
I1  每个 maker fill 最多产生一次 hedge obligation
I2  每个 hedge fill 最多偿还一次 obligation
I3  旧 generation 的迟到 fill 使用旧订单冻结的 hedge recipe
I4  effective position 不因 Fill/Position 到达顺序而重复计数
I5  有 hedge debt 时，信号撤回不能停止降险流程
I6  maker 最坏全部成交风险始终不超过 hedge capacity 与 hard limits
I7  outcome unknown 时不重复提交同一经济数量，先 reconcile
I8  replacement order 在旧单 terminal 前默认不得提交
I9  stale/degraded 时不增加 gross exposure
I10 emergency retry 数量始终来自最新 outstanding debt
```

测试矩阵至少覆盖：

- Fill -> Order -> Position、Position -> Fill -> Order 及所有重复组合；
- partial maker fills 合并为一个 hedge；
- hedge partial fill + expired/rejected + retry；
- cancel ack 前后出现 maker fill；
- signal generation/hedge ratio 更新后旧 fill 迟到；
- command accepted、rejected、outcome unknown；
- account epoch 切换与 open order reconcile；
- obligation/dedupe/order ring 容量耗尽；
- hedge depth stale、恢复和价格跳空；
- 不同 lot size 导致 dust 与有限 overhedge。

## 13. 实施顺序

### Phase 1：无行为变化地提取执行事实

- 建立 normalization、OrderLedger、FillDedupe、PositionAnchor；
- 用现有跨所做市策略的回放测试锁定当前行为；
- 把 quote target 改为 `PairIntent` 输入，但仍使用 executable spread signal。

### Phase 2：引入 HedgeObligation

- maker order 冻结 hedge recipe；
- maker fill 创建 obligation；
- hedge fill 做 allocation；
- 用 obligation debt 替代单一 `unhedged_base` 作为执行事实。

### Phase 3：风险与恢复

- 分离 increase-risk gate 和 risk-reducing gate；
- 加入 position fill-overlay、outcome-unknown reconcile；
- 补齐软/硬 debt、age、delta、gross limits；
- 支持 checkpoint 或从账户事实保守重建。

### Phase 4：接入不同信号

- `executable_spread` 作为基准；
- 再接 `basis`、`funding_carry`、`residual_zscore`；
- 新信号只实现 `PairIntent`，不得修改 execution ledger/state machine。

## 14. v1 验收定义

v1 的完成标准不是“四类信号都能启动”，而是：

- `MakerHedgePolicy` 在任意合法事件顺序下维持上述十条不变量；
- 信号可被替换而不改变 execution callbacks 和账本；
- 任意 maker fill 都能追溯到冻结的 hedge recipe、obligation 和最终 hedge fills；
- 超限、stale、reject、unknown 和重连时不会继续增加 initiating risk；
- 策略能明确说明当前风险来自 position、open order、in-flight hedge 还是 unsatisfied obligation；
- 与现有跨所做市策略做同场景回放时，正常路径性能不显著退化，异常路径风险边界更严格。

资金费和 MarkPrice live ABI 的缺口会阻塞对应 signal，但不阻塞先完成通用 Paired Execution Engine。执行内核不应等待所有信号数据能力完善后才开始实现。
