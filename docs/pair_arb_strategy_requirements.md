# `pair_arb`：Slot 化双腿执行内核需求文档

状态：ABI V13 实现基线 v3.0
目标运行时：Titan Strategy ABI V13
建议策略包：`strategies/pair_arb`

## 1. 设计定位

`pair_arb` 是一个完整、独立的双腿交易策略，不再引入信号层概念。Pair 本身就是策略描述，直接包含两个 symbol、两个 broker、价差、ratio、方向、开始时间、执行模式、最大持仓和当前执行状态。

运行时采用协程弱关系模型：

- `on_tick` 检查当前 Slot 和挂单价差；
- `risk_check` 检查全部风控指标；
- `on_fill` 处理全部成交事件，并判断部分成交或完全成交后执行对应动作；
- `on_cancel` 只处理撤单请求返回和撤单确认；
- Broker 直接调用交易所，不经过本地 Gateway 命令队列；
- Broker 接受或拒绝请求后，策略继续运行；
- 后续挂单、成交和撤单事实通过对应事件更新。

## 2. Pair 数据结构

Pair 是策略的根对象，也是所有执行状态的归属对象。

```text
Pair
  pair_id

  symbol_left
  symbol_right
  broker_left
  broker_right

  spread
  requote_distance
  cancel_timeout_ns
  hedge_ratio_abs
  direction
  start_time_ns
  mode
  max_position

  slot_unit
  ready
  status
  posture
  posture_latched

  current_slot
  order_refs[MAX_ORDER_REFS]
```

字段定义：

- `pair_id`：策略实例唯一 ID。
- `symbol_left`、`symbol_right`：双腿交易 symbol。
- `broker_left`、`broker_right`：两条腿所属 Broker。两个 symbol 可以属于同一 Broker，也可以属于不同 Broker。
- `spread`：该策略使用的目标价差或价差参数。当前市场实时价差从行情数据计算，不作为订单事实保存。
- `requote_distance`：maker 相对目标价差允许偏离的最大距离。当前可实现价差超过该距离时，先撤销旧 maker，待撤单确认后按最新盘口重新挂单。
- `cancel_timeout_ns`：撤单请求等待最终回执的最大时间。超时不代表撤单成功，必须进入查询或 reconcile。
- `hedge_ratio_abs`：Pair 唯一的对冲数量比例，必须为正数。bid/ask 或不同执行方向共用同一个 ratio。
- `direction`：策略方向，例如 `LONG_SPREAD` 或 `SHORT_SPREAD`。它决定两个订单的买卖方向。
- `start_time_ns`：策略开始执行时间。开始时间前不创建订单。
- `mode`：`MAKER_TAKER` 或 `TAKER_TAKER`。
- `max_position`：Pair 允许的最大持仓或最大执行数量。
- `slot_unit`：每个 Slot 的 initiator 目标数量。
- `ready`：行情、账户和 Broker 条件是否满足执行要求。
- `status`：策略业务状态，例如 `CREATED`、`RUNNING`、`DRAINING`、`RECONCILING`、`STOPPED`、`ERROR`。
- `posture`：风险状态，取 `NORMAL`、`RESTRICTED`、`EMERGENCY`、`HALT`。
- `posture_latched`：是否锁定在 HALT。
- `current_slot`：Pair 唯一的 Slot 容器。没有活动任务时状态为 `INIT`；创建新 Slot 时直接将其中的 `slot_id` 在前一个值基础上递增，不另设 slot 计数器。
- `order_refs`：只保存当前 Slot 无法由平台推导的最小业务关联（`order_id/slot_id/role`）；不复制
  价格、数量、累计成交量或订单状态。
- 活动订单事实由 runtime 维护，策略只通过 ABI V13 的只读 `ctx.active_orders()` 访问。
- 历史订单由 runtime 侧的 append-only `OrdersList` 持久化管理。

## 3. Slot 数据结构

Slot 是一次分批双腿执行目标。Slot 不负责定义策略，只负责完成分配给它的数量。

```text
Slot
  slot_id
  initiator_order_id
  hedge_order_id
  initiator_filled_qty
  hedge_filled_qty
  target_initiator_qty

  created_ts_ns
  first_imbalance_ts_ns
  deadline_ts_ns
```

字段定义：

- `slot_id`：Pair 内唯一、单调递增的 Slot ID。
- `initiator_order_id`：当前 initiator 活动订单 ID，没有活动订单时为 0。
- `hedge_order_id`：当前 hedge 活动订单 ID，没有活动订单时为 0。
- `initiator_filled_qty`：当前 Slot 的 initiator 累计成交量。
- `hedge_filled_qty`：当前 Slot 的 hedge 累计成交量。
- `target_initiator_qty`：该 Slot 的 initiator 目标数量。
- 对冲目标数量由 `target_initiator_qty * Pair.hedge_ratio_abs` 实时计算。
- `created_ts_ns`：Slot 创建时间。
- `first_imbalance_ts_ns`：首次产生未对冲成交的时间。
- `deadline_ts_ns`：Slot 最晚处理时间。
- Slot 状态通过 `slot_state(current_slot)` 派生为 `INIT`、`ACTIVE`、`FILLED` 或 `ERROR`，不单独存储。

同一时间只允许一个活动 Slot。当前 Slot 完成或进入 ERROR 后，才允许在后续 `on_tick` 中更新 `current_slot` 的业务字段并将 `slot_id` 自增，开始下一个 Slot。不需要额外的槽位集合或槽位索引管理；历史执行信息由 OrdersList 保存。

## 4. OrdersList 数据结构

`OrdersList` 是历史订单表，统一保存已经结束或已经从活动订单中移出的订单请求、交易所返回结果和成交明细。OrdersList 位于 runtime 持久化层，不占用 Numba 的固定热路径内存。

```text
OrdersListItem
  order_id
  slot_id
  role
  symbol
  broker

  side
  price
  qty
  order_type
  time_in_force

  status
  venue_order_id

  filled_qty
  cumulative_filled_qty
  last_fill_price
  last_fill_ts_ns
  fill_count

  submit_ts_ns
  last_event_ts_ns
  error_code
```

字段定义：

- `order_id`：本地订单 ID，也是所有后续订单事件的索引。
- `slot_id`：该订单所属 Slot。订单创建时确定，不能改变。
- `role`：该订单属于 initiator 还是 hedge。
- `symbol`、`broker`：订单实际所属交易对象。
- `side`、`price`、`qty`：订单请求参数。
- `order_type`、`time_in_force`：订单类型和有效期。
- `status`：订单当前状态，取 `REQUESTING`、`UNKNOWN`、`WORKING`、`PARTIAL`、`FILLED`、`CANCEL_REQUESTED`、`CANCELED`、`REJECTED`。
- `venue_order_id`：交易所订单 ID。
- `filled_qty`：该订单新增成交累计量。
- `cumulative_filled_qty`：交易所报告的累计成交量。
- `last_fill_price`：最近成交价格。
- `last_fill_ts_ns`：最近成交时间。
- `fill_count`：该订单成交次数。
- `submit_ts_ns`、`last_event_ts_ns`：请求和事件时间。
- `error_code`：交易所或 Broker 错误码。

订单活动期间，Numba 只在 `order_refs` 中保存 Slot/role 关联；价格、数量、状态和累计成交量始终
以 runtime 的 `ctx.active_orders()` 与当前事件 view 为准。订单结束、撤单确认或不再属于当前 Slot 后，
runtime 将完整记录追加到 OrdersList 持久化。策略不得维护第二份完整活动订单事实。

## 5. Broker 调用结果

V13 handler 不等待 Broker。`ctx.submit_order()` 只在本地 command staging 接受后返回稳定 order ID，
`ctx.cancel_order()` 返回 command ID；runtime 在 callback 成功后原子提交整批命令。交易所接受、拒绝、
成交与撤单结果以后续 `on_order/on_fill/on_cancel` 事件返回同一策略 lane。runtime 先更新公共订单事实，
再调用 handler；策略只更新 `current_slot` 与 `order_refs` 的业务关系：

```python
order_id = ctx.submit_order(...)
ctx.state["slot"]["initiator_order_id"] = order_id
```

撤单同理：

```python
command_id = ctx.cancel_order(account_no, asset_no, order_id)
# 最终结果由 on_cancel/on_order 处理
```

返回结果只确认本次请求被交易所接受或拒绝。接受后订单可能挂单、部分成交或成交，
不阻塞策略继续运行；策略只在明确的事件入口中更新实时状态，runtime 负责把策略已经
确认的订单事实持久化到 OrdersList。

## 6. 四个事件入口

策略只保留四个主要处理入口：

```python
async def on_tick(ctx, tick): ...
async def risk_check(ctx): ...
async def on_fill(ctx, fill_event): ...
async def on_cancel(ctx, cancel_result): ...
```

### 6.1 `on_tick`

`on_tick` 是状态检查和补单入口，不是成交事实入口。每次 tick 按以下顺序执行：

```text
接收并校验 tick
  -> 执行 risk_check()，得到当前 posture
  -> posture=HALT：不创建新订单，只执行明确允许的撤单，然后结束
  -> 读取 current_slot 和 active_orders
  -> 检查两腿成交量和活动订单关系
  -> 当前 Slot 已完成：在允许开仓且无残留活动订单时创建下一个 Slot
  -> initiator 已撤单且仍有剩余：按最新盘口重新挂 maker
  -> hedge 已撤单且仍有缺口：按缺口立即重新提交 taker
  -> maker 仍活动：比较 maker 价格与 taker 腿买一/卖一的实时价差
  -> 价差在允许范围：保持原 maker
  -> 价差超过 requote_distance：只发起一次撤单，等待 on_cancel
  -> 撤单超时无回执：标记未知，停止重挂，查询/reconcile
  -> 撤单明确失败：恢复撤单前的 Slot 执行状态，按重试规则再次撤单
  -> 其他状态冲突：撤销可确认的活动订单，恢复 Slot 安全初始状态，进入 reconcile
```

`on_tick` 不直接推断或修改成交数量。两腿完全成交必须先由 `on_fill` 更新
`initiator_filled_qty`、`hedge_filled_qty` 和 Slot 状态；`on_tick` 只读取这些状态，
确认当前 Slot 是否已经完成，并在后续 tick 创建新的 Slot。

#### 6.1.1 活动订单状态分支

- `order_id == 0`：该角色没有当前活动订单。只有存在剩余数量、风险允许且没有待确认未知订单时，才允许创建订单。
- `WORKING` 或 `PARTIAL`：订单仍可能成交。initiator 进入价差检查；hedge 不因为普通 tick 重复提交。
- `CANCEL_REQUESTED`：撤单请求已经接受但尚未确认最终撤单。保留原订单 ID，不重挂、不重复发送撤单。
- `UNKNOWN`：Broker 请求或查询结果未知。不得自动重发；等待订单事件或 reconcile。
- `CANCELED`：正常情况下应已由 `on_cancel` 清除 Slot 中的对应订单 ID。若仍存在关系，`on_tick` 只记录异常并等待 reconcile，不直接覆盖。
- `FILLED`：正常情况下应已由 `on_fill` 清除对应订单关系或完成 Slot。若活动数组仍保留该订单，先进入异常审计，不重复下单。

只有 `on_cancel` 确认撤单成功后，订单才会从当前订单关系中清除；因此，
`on_tick` 看到撤单请求已接受时不能提前重挂。两腿完全成交后也不重挂原订单，
只在当前 Slot 完成、活动订单清空且风险允许时创建下一个 Slot。

#### 6.1.2 Maker 价差和重新挂单

maker 价差检查必须使用当前盘口，而不是下单时保存的旧价格。根据 taker 腿的订单方向，
从 taker 腿读取对应的对手价：买入使用卖一，卖出使用买一，再与当前 maker 订单价格计算可实现价差：

```text
maker_edge = direction_normalize(maker_order_price, taker_best_bid_or_ask)
```

其中 `taker_best_bid_or_ask` 的选择由 taker 订单方向决定：买入使用卖一，卖出使用买一。
价差偏离量定义为：

```text
spread_deviation = abs(maker_edge - Pair.spread)
```

`spread_deviation <= Pair.requote_distance` 时保留当前 maker。如果重新计算后无法达到
Pair 要求的最小可接受价差，则不新增订单。`spread_deviation > Pair.requote_distance`
时，对当前 maker 订单调用 `cancel_order()`；在撤单确认前不能创建替代 maker 订单。
撤单请求 accepted 不代表已经撤单，最终重挂必须等待 `on_cancel` 的 `CANCELED` 确认，
并使用新的盘口和剩余数量重新计算价格与数量。

一次 tick 中对同一个活动 maker 最多发送一次撤单请求。订单处于 `CANCEL_REQUESTED`
或 `UNKNOWN` 时，后续 tick 只等待回报或 reconcile，不重复撤单。

#### 6.1.3 撤单无回执、失败和异常状态

`on_tick` 必须对每个 `CANCEL_REQUESTED` 订单执行超时检查：

```text
now - cancel_request_ts_ns <= Pair.cancel_timeout_ns
  -> 保留 CANCEL_REQUESTED，等待 on_cancel

now - cancel_request_ts_ns > Pair.cancel_timeout_ns
  -> 订单设为 UNKNOWN
  -> 停止重挂和重复 cancel_order
  -> 查询 Broker；查询仍不能确认时进入 reconcile
```

撤单没有回执不能当作撤单成功。只有 `on_cancel` 收到最终 `CANCELED`，或查询/reconcile
确认交易所已经撤单，才允许清除当前订单 ID 并由后续 `on_tick` 补单。

如果收到明确的撤单失败或拒绝：

1. 保留原订单 ID、已成交量和当前 Slot 的成交事实；
2. 将订单从 `CANCEL_REQUESTED` 恢复为撤单前的 `WORKING` 或 `PARTIAL`；
3. 不创建替代订单，避免原订单仍在市场时出现重复挂单；
4. 如果当前价差仍超过阈值且重试时间允许，下一次 `on_tick` 再发起撤单；
5. 连续失败或达到超时上限时，停止自动重试并进入 reconcile。

其他异常状态包括活动订单 ID 与 Slot 关系不一致、订单状态未知、同时存在两个同角色活动
订单、订单已终态但 Slot 仍认为其活动、盘口缺失或价格不可用。处理顺序为：

```text
发现异常
  -> 停止该 Slot 的新增和替代下单
  -> 对仍能确认存在的活动订单发起撤单
  -> 等待 on_cancel 或 Broker 查询结果
  -> 所有可确认订单都结束后，恢复 Slot 的安全初始状态
  -> 进入 reconcile，核对订单、成交和账户持仓
```

这里的“恢复到 Slot 初始状态”是恢复执行关系，不是清零成交事实：

```text
initiator_order_id = 0
hedge_order_id = 0
保留 initiator_filled_qty
保留 hedge_filled_qty
重新计算 imbalance 和剩余数量
Slot 状态恢复为等待恢复/`INIT`
```

不得因为撤单失败或异常恢复而清零已确认成交量、改变 `slot_id` 或直接创建新 Slot。
对账确认事实一致后，后续 `on_tick` 才能按剩余数量重新挂 initiator 或重新提交 hedge；
若存在未解释持仓或订单，Pair 保持 `ERROR/HALT` 并等待人工审核。

如果 initiator 已确认撤单且存在剩余数量，`on_tick` 重新挂 maker；如果 hedge 已确认撤单
且 `initiator_filled_qty * Pair.hedge_ratio_abs - hedge_filled_qty > 0`，`on_tick` 按缺口
数量重新提交 taker。`on_tick` 不处理成交，成交结果统一由 `on_fill` 处理，撤单结果只由
`on_cancel` 确认。

重挂或重提数量：

```text
initiator_reorder_qty = target_initiator_qty - initiator_filled_qty
hedge_reorder_qty = initiator_filled_qty * Pair.hedge_ratio_abs
                    - hedge_filled_qty
```

数量小于或等于零时不创建对应订单。

#### 6.1.4 `on_tick` 的动作互斥规则

同一个 tick 对同一个订单只能选择一种动作，优先级如下：

```text
UNKNOWN / CANCEL_REQUESTED
  -> 等待，不发送新请求

已确认 CANCELED 且有剩余量
  -> 只创建一笔替代订单

WORKING / PARTIAL 的 maker
  -> 只做价差检查；超阈值时只发送 cancel_order()

当前 Slot 完成
  -> 不恢复旧订单，只在条件满足时创建一个新 Slot
```

新 Slot 的创建步骤为：确认前一 Slot 两腿累计成交量已经满足目标、两个当前订单 ID
均为 0、没有 UNKNOWN 订单、Pair posture 允许开仓且未超过 `max_position`；然后将
`current_slot.slot_id` 自增，重置该 Slot 的订单 ID、成交量和时间字段，创建新的
initiator 订单并把订单 ID 写回当前 Slot。一个 tick 不得重复创建 Slot，也不得在
旧订单仍可能产生晚到成交时提前推进 current_slot。

### 6.2 `risk_check`

`risk_check` 只检查风控指标并更新 Pair posture，包括：

- 未对冲数量和 gross imbalance；
- 单 Slot 最大 imbalance；
- 未对冲持续时间；
- 已成交 gross notional；
- 当前未成交订单风险；
- Broker 调用结果未知的订单风险；
- 当前活动 Slot 是否存在；
- 活动订单内存是否有容量；
- ready、账户 reconcile 和行情有效性。

```text
NORMAL      允许创建 Slot、追平和撤单
RESTRICTED  禁止创建 Slot，允许追平和撤单
EMERGENCY   禁止创建 Slot，使用紧急价格带追平和撤单
HALT        禁止自动新增命令，只执行明确允许的撤单和人工恢复
```

### 6.3 `on_fill`

`on_fill` 接收全部成交事件，包括部分成交和完全成交，不调用 `risk_check`。runtime 在投递前先把
成交应用到公共 active-order view；handler 不修改公共订单，只更新 `current_slot` 和最小私有关系：

部分成交的触发源是 Broker/交易所的成交回报事件，而不是 `on_tick` 推导或轮询猜测。
Broker 适配层必须先把交易所的不同回报格式转换为统一的策略成交事件。策略正常情况下
只接收本次新增成交量 `fill_qty`，而不是同时处理不同交易所的累计量/增量语义：

```text
NormalizedFill
  order_id
  fill_qty              # 本次确认新增的成交量，必须 > 0
  fill_price
  event_ts_ns
  status                # PARTIAL / FILLED，或等价交易所状态
```

如果交易所只返回累计成交量，由 connector 适配层转换为 `fill_qty`；如果只能确认状态而不能
确认数量，则 connector 不投递 `on_fill`，直接进入 reconcile。未知订单、旧 Slot 成交、
累计量回退或跳变、成交量超过订单数量，也都由 connector 适配层拦截并进入 reconcile。
`on_fill` 不负责猜测、去重或修正原始回报。

#### 6.3.1 成交事件的完整处理步骤

`on_fill` 按下面顺序处理一条成交事件：

1. **接收规范化事件**：`on_fill` 只接收 connector 已确认归属、数量和顺序合法的 `NormalizedFill`，不再执行幂等检查。
2. **定位当前订单**：通过 `order_id` 定位 `active_orders` 中的当前订单。connector 已保证该订单属于当前可处理 Slot；找不到订单属于 connector/reconcile 异常，不在策略中兜底查找和修正。
3. **确认角色**：读取订单的 `role`。`initiator` 只更新 `initiator_filled_qty`，`hedge` 只更新 `hedge_filled_qty`。
4. **更新订单事实**：直接使用本次 `fill_qty` 更新订单的 `filled_qty` 和 `cumulative_filled_qty`，同时写入成交价格、成交时间和 `fill_count`。`PARTIAL` 设置订单为部分成交，`FILLED` 设置订单为完全成交。
5. **更新当前 Slot**：将同一个 `fill_qty` 加到当前 Slot 对应的累计成交量，并重新计算 imbalance。已结束 Slot、未知订单或无法绑定当前 Slot 的成交不会进入此步骤，而由 connector 转入 reconcile。部分成交保留当前 `order_id`，不创建新 Slot。
6. **处理 initiator 完全成交**：先完成 `initiator_filled_qty` 更新并清理 initiator 的当前订单关系，再计算：

   ```text
   required_hedge_qty = initiator_filled_qty * Pair.hedge_ratio_abs
   hedge_submit_qty = required_hedge_qty - hedge_filled_qty
   ```

   若 `hedge_submit_qty > dust_threshold`，立即提交 taker，并将新订单 ID 写入 `current_slot.hedge_order_id`。
7. **处理 hedge 完全成交**：先完成 `hedge_filled_qty` 更新，确认 imbalance 在 dust 范围内，再清除 `current_slot.hedge_order_id`，将当前 Slot 标记为完成。`on_fill` 不创建下一个 Slot。
8. **归档订单**：完全成交订单从活动订单集合移入 OrdersList；部分成交订单继续保留在活动集合中。
9. **持久化结果**：runtime 只持久化策略已经更新的订单和 Slot 事实，不在回调之外再次推导或修改策略状态。

处理规则：

- 部分成交不得创建新 Slot；部分成交后的剩余订单数量由订单继续成交或下一次撤单确认后的 `on_tick` 重挂处理。
- 下一个 Slot 只能由 `on_tick` 在当前 Slot 完成且开仓条件满足时创建。
- 已结束 Slot、未知订单或无法绑定当前 Slot 的成交由 connector 转入 reconcile，不进入正常 `on_fill`。

部分成交的公共订单累计量由 runtime 更新，`on_fill` 只更新 current_slot 的业务累计量。部分成交撤单后，
剩余数量必须按当前 Slot 累计成交量重新计算，不能按原始订单数量重挂。

### 6.4 `on_cancel`

`on_cancel` 只处理撤单请求返回和最终撤单确认，不处理成交、不计算 hedge、不创建 Slot：

撤单事件至少应携带 `order_id`、请求结果或交易所订单状态、事件时间和错误信息。一次撤单
通常有两个阶段：本地 `cancel_order()` 的请求返回，以及交易所随后发送的最终订单状态确认。
前者只表示请求接受/拒绝，后者才表示订单是否真的结束。

#### 6.4.1 撤单事件的完整处理步骤

1. **接收规范化事件**：`on_cancel` 只接收 connector 已确认 `order_id`、订单归属和状态转换合法的撤单事件。未知订单、非法状态转换和重复回报由 connector 过滤或送入 reconcile。
2. **定位当前订单**：通过 `order_id` 定位 `active_orders` 中的当前订单。找不到当前订单属于 connector/reconcile 异常，不在策略中查历史记录并自行修正。
3. **记录结果**：保存 Broker 返回状态、交易所状态、错误码和事件时间；`on_cancel` 只应用 connector 交付的这一条规范化状态变化。
4. **处理请求接受**：将活动订单状态设为 `CANCEL_REQUESTED`，保留当前订单 ID 和 Slot 关系。此时订单可能仍然成交，不能重挂，也不能把订单当成已撤单。
5. **处理请求拒绝**：保留原订单状态和当前订单关系，记录拒绝原因。后续由 `on_tick` 根据价差、风险和重试规则决定是否再次撤单；`on_cancel` 不直接重挂。
6. **处理请求超时或未知**：将订单状态设为 `UNKNOWN`，保留订单 ID，不自动重发撤单或创建替代订单，等待订单查询、交易所事件或 reconcile。
7. **处理最终 `CANCELED`**：确认该订单没有未处理的成交事实后，将订单归档到 OrdersList，并清除 `current_slot` 中对应的 `initiator_order_id` 或 `hedge_order_id`。不能清除另一条腿的订单关系，也不能修改成交累计量。
8. **处理最终 `FILLED`**：`on_cancel` 不处理成交数量，保留订单关系并将该事实转交 `on_fill`。只有 `on_fill` 完成最终成交量、hedge 和 Slot 状态更新。
9. **处理其他终态**：`REJECTED`、`EXPIRED` 等终态按照“订单已结束”归档，但是否需要补单由后续 `on_tick` 根据订单角色和剩余数量决定；不能把它们误当作 `CANCELED`。
10. **等待下一步调度**：撤单确认成功后，`on_cancel` 只完成订单关系清理。initiator 重挂或 hedge 重新提交由下一次 `on_tick` 执行。

```text
收到 cancel_order() 返回
  -> accepted：订单标记为 CANCEL_REQUESTED，但保留当前 order_id
  -> rejected：保留原订单状态，等待下一次 on_tick
  -> timeout：订单标记为 UNKNOWN，保留当前 order_id
  -> 最终确认 CANCELED：归档订单并清除对应 order_id
  -> 最终确认 FILLED：不在 on_cancel 处理成交，等待/转入 on_fill
```

撤单请求 accepted 不等于订单已经 CANCELED。accepted 后订单仍可能挂单、部分成交或完整成交；这些情况不阻塞其他策略动作。只有最终确认 CANCELED 时，`on_cancel` 才清除当前订单 ID；清除后由下一次 `on_tick` 根据订单角色决定重新挂 initiator 或立即重新提交 taker hedge。`on_cancel` 不能覆盖 `on_fill` 已经记录的成交事实。

重复撤单回报、状态回退和撤单/成交顺序冲突由 connector 统一过滤或转入 reconcile；
`on_cancel` 不实现独立的幂等表，也不负责推断撤单回报是否重复。撤单确认与成交回报并发时，
connector 分别投递合法的成交事实和撤单事实，两个入口不能互相覆盖。

### 6.5 `on_fill` 与 `on_cancel` 的互补边界

两者处理同一个订单的不同事实，不能互相代替：

| 事实 | 处理入口 | 允许修改 | 不允许处理 |
|---|---|---|---|
| 部分或完整成交数量、成交价格、累计成交 | `on_fill` | 订单成交量、Slot 两腿成交量、imbalance，以及完整 initiator 成交后的 hedge | 撤单确认 |
| 撤单请求接受/拒绝/超时 | `on_cancel` | 订单撤单状态、当前订单关系 | 成交数量、hedge、创建 Slot |
| 撤单后晚到部分或完整成交 | connector/reconcile；确认仍属于当前活动订单时才进入 `on_fill` | 对账或当前 Slot 的成交事实 | 在策略入口中自行判断旧订单归属 |
| 撤单最终确认成功 | `on_cancel` | 归档订单、清除对应当前订单 ID | 修改已记录成交 |

订单事件交错时按以下规则处理：

1. `cancel accepted -> partial/full fill`：`on_cancel` 标记撤单请求并保留 order ID；随后 `on_fill` 按规范化 `fill_qty` 更新成交事实。
2. `partial/full fill -> cancel accepted`：`on_fill` 先处理成交；随后 `on_cancel` 只能更新撤单结果，不能覆盖成交事实。
3. `partial/full fill -> cancel confirmed`：`on_fill` 保留已记录成交；`on_cancel` 归档订单并清除当前订单 ID。
4. `cancel confirmed -> partial/full fill`：connector 先判断该成交是否仍能绑定当前活动订单；不能绑定或原 Slot 已推进时进入 reconcile，不投递正常 `on_fill`。
5. `cancel rejected`：订单仍可能继续部分或完整成交，保留当前 order ID，下一次 `on_tick` 再决定是否重试撤单。
6. `cancel timeout`：订单状态设为 `UNKNOWN`，保留当前 order ID，等待查询或 reconcile。

只有当以下条件全部满足时，当前 Slot 才能结束并允许下一次 `on_tick` 创建新 Slot：

```text
initiator_filled_qty >= target_initiator_qty
abs(initiator_filled_qty * Pair.hedge_ratio_abs - hedge_filled_qty) <= dust_threshold
initiator_order_id == 0
hedge_order_id == 0
```

`on_cancel` 永远不创建下一个 Slot，也不重新提交 hedge；撤单确认后由 `on_tick` 按订单角色恢复 initiator 或 hedge。当前 Slot 的完成由 hedge 完整成交触发，下一 Slot 由后续 `on_tick` 创建。

### 6.6 `reconcile`：异常事实对账与审核

`reconcile` 不是第五个策略事件入口，也不参与正常的成交或撤单状态机。它由 connector/runtime
用于处理本地事实不可信、事件缺失或事件冲突的恢复流程。成交回报进入策略前，connector
先完成格式转换、订单归属、累计量校验和重复/乱序过滤；异常不投递 `on_fill`，而是直接进入
reconcile。`reconcile` 的目标是先
恢复“交易所实际状态”，再决定是否允许 `on_tick` 继续自动下单。

#### 6.6.1 触发时机

`reconcile` 按 Pair 隔离执行，不阻塞其他 Pair 或事件引擎。触发源分为四类：

| 触发时机 | 触发者 | 典型条件 | 处理优先级 |
|---|---|---|---|
| 成交回报进入 connector 时 | connector | 重复/乱序、累计量回退或跳变、成交量超订单量、未知订单、旧 Slot 成交、无法确认成交量 | 立即冻结该订单补单并启动 reconcile |
| 撤单等待超时 | `on_tick`/runtime watchdog | `CANCEL_REQUESTED` 超过 `Pair.cancel_timeout_ns` 仍没有最终回执 | 立即标记 `UNKNOWN`，禁止重挂并启动 reconcile |
| Broker 请求或订单状态冲突 | connector/runtime | create/cancel 返回与后续订单查询、成交回报不一致 | 立即冻结相关订单；必要时冻结整个 Pair |
| 启动、重连、恢复或定时校验 | runtime | 进程启动、Broker 重连、断线恢复、周期性账户对账 | 在恢复交易前完成 Pair 级全量 reconcile |

明确的撤单拒绝本身不一定立即启动 reconcile：如果原订单仍可确认处于 `WORKING` 或
`PARTIAL`，先恢复撤单前状态，由后续 `on_tick` 按重试规则处理；只有连续拒绝、超时、
状态无法确认或与查询结果冲突时才进入 reconcile。`risk_check` 发现风险统计异常时，
只负责把 Pair 设置为 `RESTRICTED`/`HALT` 并发出 reconcile 请求，不在风控函数中执行对账。

#### 6.6.2 需要进入 reconcile 的事件

以下情况必须由 connector 拦截并进入 reconcile，而不是投递给 `on_fill`，也不是在策略事件
入口中强行修正：

| 事件或异常 | 进入原因 | 自动处理限制 |
|---|---|---|
| `order_id` 在活动订单和 OrdersList 都不存在 | 无法确认订单归属 | 不创建替代订单，查询 Broker |
| 成交事件没有可靠数量 | 无法计算实际仓位变化 | 不增加 Slot 成交量，不提交 hedge |
| 累计成交量回退、跳变或与本地累计量不一致 | 可能乱序、丢事件或语义错误 | 冻结该订单自动补单 |
| 成交量超过订单数量或超过 Pair 可接受范围 | 本地或交易所事实冲突 | 不继续推进 Slot |
| `FILLED`、`CANCELED` 等终态与本地状态冲突 | 事件顺序或本地状态落后 | 以查询快照确认，不直接覆盖 |
| `CANCEL_REQUESTED` / `UNKNOWN` 长时间没有最终结果 | 无法确认订单是否仍在市场 | 不重发未知请求，不重挂 |
| 撤单成功但仍收到成交，或撤单前后成交顺序无法确定 | 可能存在晚到成交 | 对账订单成交和账户持仓 |
| 交易所存在本地没有的活动订单或成交 | 外部下单、重启恢复不完整或订单 ID 丢失 | 进入人工审核或建立受控绑定 |
| current Slot 与订单的 `slot_id`、role、成交量不一致 | 策略状态无法安全推进 | Pair 进入 `ERROR`/`HALT` |
| 账户持仓、余额或成交汇总与策略统计不一致 | 可能已有真实风险敞口 | 禁止新 Slot，只允许受控平风险 |

#### 6.6.3 reconcile 处理流程

```text
触发 reconcile
  -> 标记 Pair=RECONCILING，暂停新 Slot、重挂和重复请求
  -> 对仍可确认存在的活动订单，只允许风险降低方向的撤单
  -> 保存本地 Pair、current_slot、active_orders、OrdersList 和账户统计快照
  -> 查询 Broker 的订单详情、成交明细、活动订单、账户持仓和余额
  -> connector 统一查询结果格式并按 order_id/venue_order_id 关联订单
  -> 以交易所订单和成交事实重建订单累计成交量、终态和未成交量
  -> 对缺失成交执行一次受控状态恢复，并更新 OrdersList/current_slot
  -> 重新计算 hedge 缺口、Slot 状态、Pair 持仓和风险统计
  -> 检查是否还有 UNKNOWN 订单、未绑定订单或无法解释的成交
  -> 无冲突：Pair 恢复原业务状态，清除 reconcile 标记
  -> 有冲突：Pair=ERROR/HALT，生成异常审核记录并等待人工处理
  -> reconcile 完成后，由下一次 on_tick 决定是否补单
```

对账期间不执行普通补单流程。对账查询可以重试，但同一个未知订单不能因为重试而重复
创建或撤销请求。查询结果分为三种：

- **已确认结束**：确认订单为 `FILLED`、`CANCELED`、`REJECTED` 或 `EXPIRED`，写入最终事实，清理当前订单关系。
- **已确认活动**：恢复为 `WORKING` 或 `PARTIAL`，保留当前订单关系；后续由 `on_tick` 继续检查价差或风险。
- **仍无法确认**：保留 `UNKNOWN`，Pair 继续保持 `RECONCILING`/`HALT`，不能自动重挂。

对账过程中 runtime 可以更新 OrdersList 和审计记录，但不能在普通事件分发之外悄悄修改
`current_slot` 的业务状态。需要恢复当前 Slot 时，只能使用专门的 reconcile 恢复事务，
一次性写入交易所确认的最终事实；该路径不得再重复调用普通 `on_fill`，避免产生第二次累加。

#### 6.6.4 异常审核记录

每次 reconcile 至少记录：异常时间、Pair ID、order ID、slot ID、事件原文、本地订单快照、
Broker 查询结果、账户持仓快照、差异数量、采取的冻结/恢复动作、最终结论和审核人/审核时间。
审核结论只能是 `RESOLVED`、`RETRY_REQUIRED` 或 `MANUAL_REQUIRED`。在 `MANUAL_REQUIRED`
状态下，Pair 保持 `ERROR` 或 `HALT`，不得由普通 `on_tick` 自动创建新 Slot。

## 7. 完整处理流程

### 7.1 初始化

1. 创建 Pair 及两个 symbol/broker 配置。
2. 设置 `hedge_ratio_abs`、spread、direction、start time、mode 和 max position。
3. 初始化 Pair、唯一 current_slot 和私有 order_refs；runtime 初始化 active-order view 与 append-only OrdersList。
4. 设置 `ready = false`、`status = CREATED`、`posture = NORMAL`。
5. 完成账户 reconcile、行情检查和 Broker 可用性检查。
6. 条件满足后设置 `ready = true`、`status = RUNNING`。

### 7.2 创建首个 Slot

首个 Slot 由 Pair 启动条件触发；后续 Slot 由 `on_tick` 在当前 Slot 完成后，根据剩余最大持仓和风险状态创建。

```text
检查 start_time / ready / posture / capacity
  -> 从 current_slot.slot_id 递增生成新的 slot_id
  -> 使用 Pair.hedge_ratio_abs
  -> 设置 initiator/hedge 两个订单的目标数量和方向
  -> 增加 reservation
  -> 由策略创建 initiator 活动订单记录并写入 current_slot.initiator_order_id
  -> 直接调用 broker.create_order()
  -> 由策略处理本次调用的接受/拒绝结果；后续成交或撤单仍分别进入 on_fill/on_cancel
  -> runtime 只持久化策略确认后的历史 OrdersListItem
```

### 7.3 Maker-Taker

1. 直接为 initiator 提交 maker 订单。
2. `on_tick` 检查当前挂单价差。
3. 价差超过阈值时调用 `cancel_order()`。
4. `on_cancel` 只处理撤单返回和确认；确认 initiator 撤单后由 `on_tick` 重新挂 maker。
5. initiator 部分或完整成交都进入 `on_fill`。
6. 部分成交只累计 `initiator_filled_qty`；完整成交时根据统一 Pair ratio 计算 hedge 数量。
7. initiator 完整成交后由 `on_fill` 直接提交 hedge IOC，并写入 `current_slot.hedge_order_id`。
8. hedge 部分或完整成交都进入 `on_fill`；完整成交时当前 Slot 完成，下一 Slot 由后续 `on_tick` 创建。

### 7.4 Taker-Taker

1. 创建唯一的当前 Slot，并使用 Pair.hedge_ratio_abs。
2. 在同一策略调度周期内直接提交两腿 IOC。
3. 分别处理 Broker 接受/拒绝结果。
4. 两腿成交统一进入 `on_fill`。
5. 任一腿部分或完整成交都进入 `on_fill`；只有 initiator 完整成交才由 `on_fill` 立即提交缺口对应的 hedge。
6. hedge 撤单确认后由 `on_tick` 按剩余 hedge 数量立即重新提交 taker。

### 7.5 异常与未知结果

Broker 调用超时或传输异常时：

```text
ctx.state["pair"]["reconcile_required"] = 1
  -> 不重复提交同一个未知请求
  -> 保留活动订单与 slot_id 关系
  -> 等待订单事件或 reconcile
  -> 确认后继续 on_fill / on_cancel / risk_check
```

### 7.6 重连与重启

```text
ready = false
  -> 停止创建新 Slot
  -> 查询账户、活动订单和成交
  -> 通过 order_id 绑定活动订单或历史 OrdersListItem
  -> 恢复 current_slot 两腿累计成交量
  -> 重算所有 Slot imbalance
  -> 调用 risk_check()
  -> 事实完整后恢复 ready
```

未查询到的订单不得直接视为撤单成功。

## 8. 固定内存要求

Numba 层只保存一个当前 Slot 和有界的最小订单关系；活动订单与历史订单均由 runtime 持有：

```text
Pair             1 个
current_slot     1 个
order_refs       MAX_ORDER_REFS 个（仅 order_id/slot_id/role）
active_orders    runtime 公共只读 view
OrdersList       runtime append-only store
```

订单索引关系：

```text
order_id -> runtime active_orders / OrdersListItem
order_id -> private order_refs -> slot_id/role -> current_slot
```

私有关系数组容量耗尽时：

- 不覆盖历史记录；
- 禁止创建新的订单；
- 保留已有 hedge、cancel 和 risk 处理；
- Pair 至少进入 `RESTRICTED`；
- 产生容量告警。

## 9. 不变量和验收

```text
I1  Pair 只有一个 hedge_ratio_abs，bid/ask 和所有 Slot 共用它。
I2  Slot 的 hedge 数量根据当前 Pair.hedge_ratio_abs 计算。
I3  一个 order_id 在 OrdersList 中只有一条记录。
I4  每个订单记录只属于一个 Slot 和一个角色（initiator 或 hedge）。
I5  Slot 只保存当前 initiator_order_id 和 hedge_order_id；撤单确认后清除对应关系。
I6  历史订单仍保存在 OrdersList，可通过 order_id 找回所属 Slot。
I7  OrdersList 同时保存订单请求、Broker 返回和成交明细，不单独维护独立成交记录。
I8  Broker timeout/transport error 后不得自动重发未知请求。
I9  重复、乱序或非法成交回报必须由 connector 拦截，不得进入正常 on_fill。
I10 on_tick 只做当前 Slot 状态、活动订单价差/重挂检查和 risk_check；不直接处理成交事实。
I11 on_fill 负责部分/完整成交后的 Slot 更新；只有 initiator 完整成交才立即提交 hedge，后继 Slot 由 on_tick 创建。
I12 on_cancel 只负责撤单返回和确认，不处理成交。
I13 posture >= RESTRICTED 时不得创建新 Slot。
I14 活动订单数组满时不得覆盖已有记录或创建新订单。
I15 风险统计不能因不同 Slot 的相反方向抵消而低估 gross imbalance。
I16 撤单无回执、撤单失败或订单状态冲突时，不得提前重挂，必须等待确认或进入 reconcile。
```

测试至少覆盖：connector 对重复/乱序/非法成交的拦截、两腿订单接受/拒绝/超时、接受后挂单或成交、撤单接受后的晚到成交、部分成交的 Slot 累计、剩余量重挂、Slot 完成后的后继 Slot、Pair ratio、on_tick/on_fill/on_cancel 职责边界、容量耗尽、重连恢复以及单策略等待 Broker 时其他策略继续运行。

## 10. 实施顺序

1. 在 `abi_v13.py` 统一定义 typed-state runtime context、订单、成交、市场和公开状态视图。
2. 在 `context.py` 提供 Pair、Slot、OrdersList 的访问和 Broker facade。
3. 在 `callbacks.py` 连接 Broker 请求结果和订单事件回调。
4. 实现 Pair、current Slot 和 active orders 的固定内存布局，OrdersList 由 runtime 持久化。
5. 实现 `on_tick` 和 `risk_check`。
6. 实现 `on_fill`、maker-taker 和后继 Slot。
7. 实现 `on_cancel`、撤单确认和重挂。
8. 实现 taker-taker、重连 reconcile 和审计回放。
