# Numba 策略三阶段技术方案：编写、静态编译与运行

状态：设计稿（待实现）
目标接口：Strategy ABI V13
核心方案：Typed State Blob + NumPy Structured Dtype + 离线 Numba AOT + Rust Native Runtime

## 1. 文档目的

本文只解决一件事：建立一套易读、易写、开发成本低、与 runtime 松耦合的 Numba 策略开发与执行体系。

整个生命周期严格分成三个阶段：

```text
阶段一：策略编写
  开发者只面对 Strategy SDK、具名状态和交易领域接口

阶段二：静态编译
  编译服务器把 Python/Numba 策略编译为平台原生策略 artifact

阶段三：策略运行
  Rust runtime 只加载原生 artifact，不启动 Python，不现场 JIT
```

V13 在本文中作为一套独立、完整的新策略契约设计。

## 2. 核心决策

### 2.1 策略开发者看到的接口

策略开发者只需要理解：

- `ctx`：行情、成交、订单、时间和交易命令；
- `ctx.state`：具名、强类型、固定结构的策略私有状态；
- handler：`on_tick(ctx)`、`on_fill(ctx)` 等单参数事件函数；
- `build(parameters)`：校验参数并生成初始状态与策略定义。

开发者不应接触 `state_f64/state_i64`、数字 offset、Rust 内存布局、connector 类型、runtime
内部对象、callback 地址或 Python/C/Rust 指针转换。

### 2.2 静态编译的定义

本文的“静态编译”是指：

- Python 源码和 Numba 只存在于编译服务器；
- compiler 将策略 handler 和 bridge 编译为目标平台原生共享库；
- 产物包含 native library、manifest、state schema 和 initial state blob；
- 生产 runtime 通过稳定 C ABI 加载共享库；
- 生产进程不导入策略 Python 模块，不运行 Numba JIT，也不依赖 Python对象存活。

共享库在进程启动时动态加载，但策略代码本身已经离线编译，因此属于 AOT/static strategy
compilation，而不是 runtime JIT。

### 2.3 状态模型

策略状态是一个 NumPy aligned structured dtype。编译后它变成一块固定大小、固定对齐、带
schema identity 的 opaque bytes：

```text
Python/Numba：知道字段名、类型、shape 和 offset
Rust runtime：只知道 address、byte length、alignment 和 schema hash
```

字段访问在编译期解析为固定 offset，不在热路径执行字符串查找。

## 3. 三阶段总览

```text
┌──────────────────────────────────────────────────────────────────┐
│ 阶段一：策略编写                                                  │
│ 单个 strategy.py                                                 │
│ build(parameters) -> StrategyDefinition                          │
│ handler(ctx)，typed state 通过 ctx.state 访问                    │
└──────────────────────────────┬───────────────────────────────────┘
                               │ source file
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│ 阶段二：静态编译                                                  │
│ validate -> type -> Numba lower -> object -> native link         │
│ -> schema hash -> initial state -> immutable artifact            │
└──────────────────────────────┬───────────────────────────────────┘
                               │ native artifact
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│ 阶段三：策略运行                                                  │
│ Rust load artifact + instance binding -> allocate state          │
│ -> bind callbacks/events/accounts                                │
│ -> dispatch event -> native callback -> host command sink        │
└──────────────────────────────────────────────────────────────────┘
```

三个阶段只通过明确合同连接：编写阶段输出单一 source file，编译阶段输出 immutable artifact；
运行阶段输入 artifact 和独立的实例部署绑定。账户、市场、routing key、lane capacity 和 handler duration
属于部署配置，不进入策略源码，也不改变 artifact。

# 第一部分：策略编写

## 4. 策略包结构

```text
strategies/pair_arb/
  strategy.py          # 唯一策略源文件
```

测试放在仓库统一测试目录，通过 strategy ID 关联，不进入策略发布包。README 属于项目文档而非
编译输入。参数、能力、state dtype、初始状态和 handler 全部在 `strategy.py` 中声明。

公共内容归入 `titan_strategy` SDK：

```text
titan_strategy/
  types.py       # Side、OrderStatus、EventKind、EventQos、TimeInForce 等公共枚举
  state.py       # record/array/new_state、dtype 验证、schema/hash 工具
  parameters.py  # FloatParam/IntParam/EnumParam 等参数声明
  definition.py  # StrategySpec、StrategyDefinition、EventSubscription、Capability
  context.py     # ctx 只读公共 view、当前事件 view 和交易命令 facade
  abi_v13.py     # 唯一跨 Rust/Python ABI 定义
```

公共模块统一的是类型系统、声明方法和能力词表，不是所有策略共享同一组状态字段。Pair、Slot、
indicator 等字段仍由各策略在自己的 `strategy.py` 中声明，否则公共 state 模块会反向依赖具体
策略，破坏松耦合。

策略包只能依赖 `numpy`、`numba` 和 `titan_strategy` public API。禁止依赖 connector、runtime
implementation、账户服务内部类型或 CLI 实现。

## 5. 类型化状态

### 5.1 状态声明示例

```python
from titan_strategy.state import (
    float64, int32, int64, new_state, record, uint8, uint64,
)

pair_dtype = record(
    status=int32,
    posture=int32,
    ready=uint8,
    reconcile_required=uint8,
    hedge_ratio=float64,
    max_position_lots=int64,
    filled_gross_notional=float64,
)

slot_dtype = record(
    id=uint64,
    created_ts=int64,
    deadline_ts=int64,
    initiator_order_id=uint64,
    hedge_order_id=uint64,
    target_lots=int64,
    initiator_filled_lots=int64,
    hedge_filled_lots=int64,
)

state_dtype = record(
    pair=pair_dtype,
    slot=slot_dtype,
)
```

`titan_strategy.state.record()` 统一生成 little-endian、`align=True` 的 NumPy dtype；`array()`
只允许正整数固定长度。策略开发者根据领域含义选字段类型，不选择底层数组，也不需要直接处理
NumPy layout 参数。

### 5.2 允许的状态类型

| 类型 | 用途 |
|---|---|
| `i1/u1` | flag、窄枚举、side、role |
| `i2/u2` | 明确需要 16 位的整数 |
| `i4/u4` | 状态枚举、本地编号、计数 |
| `i8/u8` | 时间戳、order ID、sequence、大计数 |
| `f4/f8` | 比例、统计值、指标；不可用于 ABI 中以 ticks/lots 表示的精确交易量 |
| nested dtype | Pair、Slot、Risk 等策略私有固定结构 |
| fixed subarray | 指标窗口、history ring 等有界策略数据 |

禁止 object、dict、list、字符串、变长数组、指针和平台宽度不确定的类型。V13 首版最大
alignment 为 8，统一 little-endian，这些规则由公共 `state` 模块自动保证。

`nested dtype` 只是把同一块 state blob 中的字段按业务对象分组，例如
`ctx.state["pair"]["status"]` 或 `ctx.state["slot"]["deadline_ts"]`；它不会创建 Python 对象或额外
分配内存。`fixed subarray` 用于编译时已知上限的策略私有集合，例如 64 项指标环形窗口、8 个候选
spread 或有限 Slot。所谓“原地修改”是 handler 对这些字段/数组元素的写入直接落到 Rust 持有的
state blob，callback 返回后仍保留，不复制整个 record。它不用于活动订单等 runtime 公共集合。

平台订单事实不属于策略私有 state。公共 `order_dtype`、活动订单集合和订单生命周期由 Rust
runtime/account service 维护，通过只读的 `ctx.active_orders()` 和当前事件 view 提供。策略不得在
`ctx.state` 中复制一份完整公共 Order 对象，否则容易与平台订单真相产生双写和状态漂移。

策略 state 只保存平台无法推导的业务关联，例如当前 Slot 的 `initiator_order_id`、
`hedge_order_id`、目标数量、累计成交量和策略角色。`pair_dtype`、`slot_dtype` 和根 `state_dtype`
仍由策略声明，因为这些字段是该策略独有的状态机；SDK 只提供 `record()`、标量类型和公共订单
view，不把 Pair/Slot 固化成所有策略共享的 schema。

### 5.3 私有状态与公共数据的访问规则

统一遵循以下边界：

```text
策略私有、需要持久化的数据   -> ctx.state
平台公共、由 runtime 维护的数据 -> ctx
```

| 数据 | 推荐访问方式 | 所有者与可变性 |
|---|---|---|
| Pair 状态机 | `ctx.state["pair"]` | 策略私有，可读写 |
| 当前执行 Slot | `ctx.state["slot"]` | 策略私有，可读写 |
| 私有指标、计数器、窗口 | `ctx.state["indicators"]` | 策略私有，可读写 |
| 行情 | `ctx.market(asset_no)` | runtime 公共数据，只读 |
| 当前仓位 | `ctx.position(account_no, asset_no)` | runtime 公共数据，只读 |
| 当前余额 | `ctx.balance(account_no, currency_no)` | runtime 公共数据，只读 |
| 当前账户状态 | `ctx.account(account_no)` | runtime 公共数据，只读 |
| 活动订单 | `ctx.active_orders()` | runtime 公共数据，只读 |
| 当前成交事件 | `ctx.fills()` | 当前 callback 借用视图，只读 |
| 当前订单事件 | `ctx.order_events()` | 当前 callback 借用视图，只读 |
| 当前时间 | `ctx.now` | runtime 公共数据，只读 |
| 下单、撤单 | `ctx.submit_order()` / `ctx.cancel_order()` | 受控命令入口 |

`ctx.state` 的根字段集合完全由当前策略的 `state_dtype` 决定，不会自动注入公共字段。若策略未声明
`orders`，则 `ctx.state["orders"]` 必须在编译时失败。即使 SDK 提供公共 `order_dtype`，策略把它
嵌入自己的 `state_dtype` 后得到的也只是私有副本，不会与 runtime 的活动订单自动同步。

策略不得把行情、仓位、活动订单、余额等平台事实复制进 `ctx.state` 形成第二事实源。策略只保存
平台无法推导的业务关联和决策状态；对公共数据的修改只能通过 `ctx` 暴露的受控交易命令完成。

```python
@njit
def on_tick(ctx):
    pair = ctx.state["pair"]
    slot = ctx.state["slot"]

    market = ctx.market(0)
    position = ctx.position(0, 0)
    active_orders = ctx.active_orders()

    if pair["status"] == PAIR_READY and position["qty_lots"] == 0:
        ctx.submit_order(...)
```

### 5.4 公共 view 的一致性和只读性

`ctx.market()`、`ctx.position()`、`ctx.active_orders()` 不是现场查询 connector，也不在 callback 中
分配或复制集合。StrategyRuntime 在事件进入 handler 前先把 canonical event 应用到本实例的公共
runtime view，再把 Rust 持有的连续内存以借用视图放入 context。因此当前 callback 读取到的公共
view 至少包含触发本次 callback 的事实。

`ctx.active_orders()` 只包含当前 `strategy_instance_id + generation` 通过 command sink 创建、且尚未归档的订单；
按 `order_id` 唯一，元素类型为 SDK 公共 `ActiveOrderView`。它包含公共订单事实，如 `order_id`、
`asset_no`、`account_no`、side、price、qty、cumulative filled qty、status 和时间戳，但不包含 Pair、
Slot 或 initiator/hedge role 等策略语义。终态订单在相关事件提交后追加到 runtime `OrdersList`，并按
明确的归档边界从 active view 移除。

策略需要保存平台无法推导的关联时，只保存最小私有引用。例如单 Slot pair-arb 使用
`slot.initiator_order_id/slot.hedge_order_id`；需要同时跟踪多个业务关联的策略可以在 `ctx.state` 中
声明有界 `order_refs`，元素只包含 `order_id/strategy_role/business_id`，不得复制公共订单价格、数量
或状态。

该所有权规则取代早期 pair-arb 文档中“Numba 自己维护完整 active_orders 数组”的技术实现，但不
改变其业务不变量：同一订单仍只能属于一个 Slot/role，未知或撤单中的订单仍阻止提前重挂，历史
OrdersList 仍由 runtime append-only 持久化。实现 V13 pair-arb 前必须同步更新旧需求文档的内存归属
表述，不能同时保留两套订单事实源。

公共 view 在 Numba 类型系统中是 read-only borrowed view。compiler 不为其注册 setitem、可写 record
field 或 writable pointer lowering，以下代码必须编译失败：

```python
ctx.active_orders()[0]["status"] = ORDER_FILLED
ctx.market(0)["best_bid"] = 0.0
```

只有 `ctx.state` 返回 mutable record。所有公共 view、当前事件 view 和 `ctx` 本身都只在本次 callback
内有效，不能返回、缓存或写入 state。

## 6. `StrategySpec` 与 `StrategyDefinition`

### 6.1 `StrategySpec`

策略不再提供 `strategy.json`。参数 schema 和能力由 `strategy.py` 内的类型化 spec 生成：

```python
from titan_strategy.definition import (
    Capability, EventSubscription, StrategyDefinition, StrategySpec,
)
from titan_strategy.parameters import EnumParam, FloatParam
from titan_strategy.types import EventKind, EventQos

SPEC = StrategySpec(
    strategy_id="pair_arb",
    strategy_version="1.0.0",
    state_schema_version=1,
    parameters=(
        FloatParam("hedge_ratio_abs", minimum=0.0, exclusive_minimum=True),
        FloatParam("max_position_lots", minimum=0.0, exclusive_minimum=True),
        EnumParam("mode", values=("MAKER_TAKER", "TAKER_TAKER")),
    ),
    subscriptions=(
        EventSubscription(EventKind.BBO, "on_tick", schema_version=1,
                          qos=EventQos.LATEST),
        EventSubscription(EventKind.FILL, "on_fill", schema_version=1,
                          qos=EventQos.RELIABLE_ORDERED),
        EventSubscription(EventKind.ORDER, "on_order", schema_version=1,
                          qos=EventQos.RELIABLE_ORDERED),
        EventSubscription(EventKind.CANCEL, "on_cancel", schema_version=1,
                          qos=EventQos.RELIABLE_ORDERED),
    ),
    capabilities=Capability.MARKET_DATA | Capability.ORDER_EXECUTION,
)
```

参数：

- `strategy_id`：稳定策略标识；
- `strategy_version`：业务版本；
- `state_schema_version`：状态语义版本；
- `parameters`：公共参数声明对象的固定 tuple，compiler 由此生成 JSON schema；
- `subscriptions`：策略需要的 canonical event、handler、schema version 和 QoS；
- `capabilities`：显式权限声明，compiler 只做一致性检查，不能通过代码扫描自动授予权限。

ABI version 不属于策略属性，由当前 SDK 与静态 compiler 决定，因此不在每个策略中重复声明。
V13 不提供 state migration。`state_schema_version` 只用于诊断和显式语义标识；加载与恢复始终要求
完整 schema hash、长度和 alignment 完全相同，版本不同或 hash 不同都直接拒绝。

公共参数类型：

```python
FloatParam(name, *, required=True, default=None, minimum=None,
           maximum=None, exclusive_minimum=False, exclusive_maximum=False)
IntParam(name, *, required=True, default=None, minimum=None, maximum=None)
EnumParam(name, *, values, required=True, default=None)
BoolParam(name, *, required=True, default=None)
```

- `name`：参数键；
- `required/default`：是否必填及默认值，两者组合由 SDK 校验；
- `minimum/maximum`：数值范围；
- `exclusive_minimum/exclusive_maximum`：边界是否排除；
- `values`：Enum 的有限字符串集合。

compiler 从这些声明生成 JSON schema 和命令行帮助；`build` 仍负责跨字段约束。

`EventSubscription(event_kind, handler, *, schema_version, qos)` 的参数：

- `event_kind`：SDK 公共 `EventKind`，不允许使用 connector 私有事件名；
- `handler`：本文件 `handlers` 中的标准 handler 名；
- `schema_version`：canonical event payload 版本；
- `qos`：`LATEST/RELIABLE_ORDERED/BEST_EFFORT`。

平台强制 fill/order/cancel/position/balance/account state 使用 `RELIABLE_ORDERED`，策略不能降级。
策略只声明事件需求，不声明具体账户、市场或 routing key。部署实例配置负责把逻辑绑定转换为
`AssetId/AccountId/routing_key`，runtime 再为当前实例生成连续的 `asset_no/account_no`。同一 artifact
可在不同实例绑定不同市场和账户，而无需重新编译。

### 6.2 `StrategyDefinition`

```python
@dataclass(frozen=True)
class StrategyDefinition:
    spec: StrategySpec
    state: np.ndarray
    handlers: dict[str, object]
    metadata: dict[str, object]
```

参数：

- `spec`：同一文件中的类型化策略、参数和能力声明；
- `state`：长度为 1 的 structured ndarray，包含初始状态；
- `handlers`：事件名到模块级 `@njit` 函数的映射；
- `metadata`：JSON-safe 诊断信息，不进入交易热路径。

## 7. `build(parameters)`

```python
def build(parameters: dict[str, object]) -> StrategyDefinition:
```

`parameters` 是经过 JSON schema 基础校验的参数副本。函数负责交叉参数校验、创建初始 state、
写入配置并返回定义。

禁止在 `build` 中发起网络请求、连接 connector、读取账户/市场、启动线程/协程、动态生成
handler、定义 `@njit` 闭包，或根据参数改变 dtype 和状态长度。

```python
def build(parameters):
    hedge_ratio = float(parameters["hedge_ratio_abs"])
    if hedge_ratio <= 0.0:
        raise ValueError("hedge_ratio_abs must be positive")

    state = new_state(state_dtype)
    state[0]["pair"]["status"] = PAIR_CREATED
    state[0]["pair"]["hedge_ratio"] = hedge_ratio

    return StrategyDefinition(
        spec=SPEC,
        state=state,
        handlers={
            "on_start": on_start,
            "on_tick": on_tick,
            "on_fill": on_fill,
            "on_order": on_order,
            "on_cancel": on_cancel,
            "on_stop": on_stop,
        },
        metadata={},
    )
```

## 8. Handler 编写接口

统一签名：

```python
@njit
def handler(ctx):
    ...
```

- `ctx`：当前 callback 的统一入口，包含只读事件视图、交易命令 facade 和可变 typed state；
- `ctx.state`：当前策略实例的可变 typed record，其具体 dtype 在静态编译时确定；
- 返回值：`None`。

标准 handler 包括 `on_start/on_tick/on_bar/on_depth/on_fill/on_order/on_cancel/on_position/
on_balance/on_account_state/on_timer/on_stop`，签名均为 `(ctx)`。

| Handler | 当前事件 accessor | 典型 QoS | 说明 |
|---|---|---|---|
| `on_tick` | `ctx.ticks()` | `LATEST` 或显式声明 | BBO/trade batch |
| `on_bar` | `ctx.bars()` | `RELIABLE_ORDERED` | 已关闭 Bar batch |
| `on_depth` | `ctx.depth()` | `LATEST` | 可覆盖盘口 snapshot/delta batch |
| `on_fill` | `ctx.fills()` | `RELIABLE_ORDERED` | 新增成交事实 |
| `on_order` | `ctx.order_events()` | `RELIABLE_ORDERED` | 接受、拒绝及普通状态转换 |
| `on_cancel` | `ctx.cancel_events()` | `RELIABLE_ORDERED` | 撤单请求结果与最终确认 |
| `on_position` | `ctx.position_events()` | `RELIABLE_ORDERED` | 仓位事实 |
| `on_balance` | `ctx.balance_events()` | `RELIABLE_ORDERED` | 余额事实 |
| `on_account_state` | `ctx.account_state_events()` | `RELIABLE_ORDERED` | 账户可用性与 epoch |
| `on_timer` | `ctx.timer()` | lane safe point | runtime timer |

`on_start/on_stop` 没有业务 event payload。表中的名字是 V13 唯一标准命名；不再使用
`on_filled`、`ctx.orders()` 等兼容别名。

Strategy SDK 向 handler 暴露的核心签名固定为：

```python
ctx.now -> int64
ctx.state -> MutableStateRecord

ctx.market(asset_no: uint32) -> readonly MarketView
ctx.position(account_no: uint32, asset_no: uint32) -> readonly PositionView
ctx.balance(account_no: uint32, currency_no: uint32) -> readonly BalanceView
ctx.account(account_no: uint32) -> readonly AccountView
ctx.active_orders() -> readonly ActiveOrderView[:]

ctx.best_bid_ticks(asset_no: uint32) -> int64
ctx.best_ask_ticks(asset_no: uint32) -> int64

ctx.submit_order(
    account_no: uint32,
    asset_no: uint32,
    side: uint8,
    order_type: uint8,
    qty_lots: int64,
    price_ticks: int64,
    time_in_force: uint8,
    reduce_only: bool = False,
    trigger_price_ticks: int64 = 0,
    gtd_expiry_ns: int64 = 0,
) -> uint64

ctx.cancel_order(
    account_no: uint32,
    asset_no: uint32,
    order_id: uint64,
) -> uint64
```

- `asset_no/account_no/currency_no`：实例部署绑定中的连续本地编号，不是 connector 对象；
- `*_ticks/*_lots/*_ns`：已经按公共市场元数据离散化的精确整数单位；
- `side/order_type/time_in_force`：`types.py` 固定数值的窄整数枚举；`qty_lots` 必须大于 0；
- `price_ticks`：限价类订单的价格；无需价格的订单类型必须传 0；
- `reduce_only`：是否只允许减少现有仓位；
- `trigger_price_ticks`：触发类订单的价格，非触发类传 0；
- `gtd_expiry_ns`：GTD 到期时间，非 GTD 传 0；
- `ctx.submit_order()`：本地 command sink 接受后返回 runtime 分配的稳定 `order_id`，策略可立即保存到
  私有 Slot；同步拒绝使本次 callback 以 `COMMAND_ERROR` 失败，不产生 order ID；后续交易所拒绝仍
  使用该 order ID 发送 `on_order`；
- `ctx.cancel_order()`：本地 command sink 接受后返回 `command_id`，最终结果仍以后续 `on_cancel` 或
  `on_order` 事件为准；
- `ctx.cancel_order()` 的三个输入必须与当前实例拥有的活动订单完全匹配，否则同步拒绝；
- 查询编号越界、命令字段非法或 command gate 关闭都产生稳定错误码，不允许返回悬空 view。

命令在 callback 返回时提交。为保持借用 view 的地址和长度稳定，本次 callback 预先取得的
`ctx.active_orders()` 不会因本次 submit/cancel 原地扩缩；命令造成的公共订单变化从下一个 callback
boundary 起可见。策略若需在同一 callback 内关联新订单，应使用 `submit_order()` 返回的 order ID，
不能再次扫描 active view。

事件 accessor（`ticks/bars/depth/fills/order_events/cancel_events/position_events/balance_events/
account_state_events`）统一返回当前 callback 的只读连续 view；`timer()` 返回当前只读 TimerView。它们
不接受筛选参数，避免热路径分配。策略需要筛选时使用整数编号在 Numba 循环中完成。

```python
@njit
def on_tick(ctx):
    pair = ctx.state["pair"]
    slot = ctx.state["slot"]

    if pair["posture"] == POSTURE_HALT:
        return
    if ctx.best_bid_ticks(0) <= 0 or ctx.best_ask_ticks(1) <= 0:
        return
    if slot["id"] == 0:
        slot["id"] = 1
        slot["created_ts"] = ctx.now
```

`ctx.state["pair"]` 中的字符串不是运行时字典查询。compiler 已知本策略的 `state_dtype`，
Numba 会把字段访问降低为 `state_ptr + 固定 offset`。读取和写入都直接作用于 Rust runtime
为该实例持有的 aligned state blob，不复制整块状态。

不使用 `ctx.stats` 作为该属性名。`stats` 通常表示可观测统计指标，无法准确表达订单、槽位、
状态机和风控标记等可变策略状态；该名称保留给未来的只读运行统计接口。策略私有状态统一使用
`ctx.state`，不再同时提供 `state` 参数或别名，避免同一概念出现两套写法。

字段名应使用完整业务语义并带单位后缀，如 `_ticks/_lots/_ns/_bps`。handler 只做事件编排，
复杂事实更新进入 `apply_fill` 等模块级具名函数。

## 9. SDK 状态函数

### 9.1 `record` / `array` / `new_state`

```python
def record(**fields: StateType) -> np.dtype
def array(element: StateType, length: int) -> FixedArrayType
def new_state(dtype: np.dtype) -> np.ndarray
```

- `record` 按参数声明顺序生成 little-endian aligned dtype，字段名重复或非法时失败；
- `array` 描述固定长度子数组，`length` 必须为正整数；
- `new_state` 返回 `np.zeros(1, dtype=dtype)`，并再次验证 dtype 来自公共状态类型系统。

### 9.2 `validate_state_dtype`

```python
def validate_state_dtype(
    dtype: np.dtype,
    *,
    max_state_bytes: int,
    max_alignment: int,
) -> StateLayout:
```

- `dtype`：根 structured dtype；
- `max_state_bytes`：平台最大状态字节数；
- `max_alignment`：平台最大对齐；
- 返回验证后的 `StateLayout`；
- 非 structured、object、非法 endian、字段重叠、超限或不对齐时抛出 `StateSchemaError`。

### 9.3 `canonical_state_schema`

```python
def canonical_state_schema(
    layout: StateLayout,
    *,
    schema_version: int,
) -> bytes:
```

编码字段名、类型、offset、shape、itemsize、alignment、endian 和嵌套关系，返回稳定 bytes。

### 9.4 `state_schema_hash`

```python
def state_schema_hash(canonical_schema: bytes) -> bytes:
```

返回完整 32 字节 SHA-256。初始状态值不参与 hash。

### 9.5 `describe_state_schema`

```python
def describe_state_schema(layout: StateLayout) -> dict[str, object]:
```

返回 JSON-safe 字段树，用于编译报告和 artifact inspect，不代替 canonical hash。

# 第二部分：静态编译

## 10. 编译输入与输出

静态编译阶段把单个 `strategy.py` 转成与 Python 源码解耦的原生 artifact。

输入：策略源文件、参数 JSON、ABI descriptor、target triple、CPU baseline 和编译器版本锁。

默认输出严格控制为两个文件：

```text
pair_arb.so                 # 或 .dylib/.dll：AOT 机器码与 native descriptor
pair_arb.manifest.cbor      # 参数 schema、能力、state schema、初始 state、digest、签名、build 信息
```

CBOR 支持二进制字段，因此 `initial_state` 不需要 base64，也不需要额外 `.bin` 文件。schema、
build report 和 checksums 都是 manifest 中的命名 section。

可选发布格式为单个 `pair_arb.titan` 容器，内部仍是上述 library + manifest。runtime 校验容器后
将 native library 解包到只读的 content-addressed cache 再加载。默认推荐两文件模式，因为
`dlopen` 可直接加载、实现简单、排障方便；单文件用于传输和制品仓库。

不建议把签名 manifest 完全嵌入共享库：签名需要覆盖 library digest，嵌入自身会产生循环签名，
且 inspect 必须先加载未验证的 native code。生产 runtime 不读取 `.py` 文件。

## 11. 编译命令

```text
titan strategy compile \
  --strategy <strategy.py> \
  --parameters <parameters.json> \
  --target <target-triple> \
  --cpu-baseline <baseline> \
  --artifact-format <pair|bundle> \
  --output <artifact-path>
```

- `--strategy`：唯一策略源文件，compiler 固定调用其中的 `build(parameters)`；
- `--parameters`：初始状态参数；
- `--target`：例如 `x86_64-unknown-linux-gnu`；V13 首版必须等于编译服务器 host target；
- `--cpu-baseline`：最低 CPU feature 集；
- `--artifact-format pair`：输出 native library + manifest 两个文件，默认值；
- `--artifact-format bundle`：输出一个 `.titan` 容器；
- `--output`：输出基名或 bundle 路径，失败不得留下可加载半成品。

## 12. 编译流水线

```text
读取 strategy.py
  -> 计算 source digest
  -> 隔离进程导入并调用 build
  -> build(parameters)
  -> validate StrategyDefinition/state/handlers
  -> canonical schema + hash
  -> Numba type inference
  -> 生成 C ABI bridge
  -> LLVM lowering/object emission
  -> native linker 生成共享库
  -> 导出符号检查
  -> 写 initial state/schema/manifest
  -> native smoke test
  -> 原子发布 artifact
```

## 13. 编译器函数

### 13.1 `CompileRequest`

```python
@dataclass(frozen=True)
class CompileRequest:
    source_file: Path
    parameters: dict[str, object]
    target_triple: str
    cpu_baseline: str
    artifact_format: str
    output_path: Path
    runtime_abi: dict[str, object]
```

字段依次表示唯一策略文件、参数、目标平台、CPU基线、`pair/bundle` 输出格式、输出路径和 ABI
descriptor。入口固定为 `build`，不需要每个策略重复配置 entrypoint。

### 13.2 `compile_package`

```python
def compile_package(request: CompileRequest) -> CompileResult:
```

协调完整编译事务。任何步骤失败都删除 staging directory，保留已有正式 artifact。

### 13.3 `load_strategy_definition`

```python
def load_strategy_definition(
    source_file: Path,
    parameters: dict[str, object],
) -> StrategyDefinition:
```

在隔离 compiler worker 中以唯一模块名加载 `source_file` 并调用固定入口 `build`；`parameters`
传入副本。策略不能通过相对文件导入隐藏额外策略源码，公共依赖只能来自已安装 SDK。

worker 使用只读 source/SDK mount、清理后的环境变量、禁用网络和子进程，并设置 CPU、内存和时间
上限。限制同时覆盖模块 top-level import 和 `build()`，避免把“build 禁止副作用”仅作为代码约定。

### 13.4 `validate_strategy_definition`

```python
def validate_strategy_definition(
    definition: StrategyDefinition,
    runtime_abi: dict[str, object],
) -> ValidatedStrategy:
```

校验 ID/version/state/handlers/metadata、dtype/schema、subscriptions、handler 对应关系、强制 QoS 和
capability 一致性，返回不可变编译模型。

### 13.5 `validate_handler`

```python
def validate_handler(name: str, handler: object) -> ValidatedHandler:
```

要求 handler 是模块级 Numba dispatcher，签名严格为 `(ctx)`；拒绝闭包、动态 dtype、
`*args/**kwargs` 和不支持的 Python操作。

### 13.6 `make_strategy_context_type`

```python
def make_strategy_context_type(
    state_dtype: np.dtype,
    abi: dict[str, object],
) -> StrategyContextType:
```

- `state_dtype`：当前策略唯一的、已经验证过的根 structured dtype；
- `abi`：runtime context 字段、函数表和版本描述；
- 返回值：仅用于本次策略编译的 Numba context 类型。

`StrategyContextType` 是 compiler/SDK 内部的 Numba extension type，类型身份包含规范化后的
`state_dtype`。它的 native data model 只保存 ABI context pointer 和 typed state pointer；构造发生
在 bridge 的 native IR 中，不创建 Python 对象，不拥有 state 内存，也不延长 state 生命周期。

公共 `ctx.now`、`ctx.best_bid_ticks()`、`ctx.submit_order()` 等属性和方法由 SDK 为这个 extension type
注册 typing/lowering；`ctx.state` 的 lowering 返回 `state_dtype` 对应的可变 record view。因此：

- 对策略作者只有一个稳定的 `ctx` 类型概念；
- 对 Numba 而言，每个策略的 `ctx.state` 仍是完全静态的具体类型；
- 不需要在公共 `Strategy` jitclass 中容纳动态 dtype；
- 不需要 AST 改写，也不需要在运行时根据 dtype 分支；
- state field load/store 直接使用固定 offset，热路径不做对象分配或字符串查找。

首版不得用“公共 jitclass + object 类型 state 字段”实现，因为它会退出 nopython 或丢失字段
类型；也不得为每次 callback 动态生成 Python 类。context 类型只在静态编译阶段按 schema 生成
一次，所有 handler 共享。

### 13.7 `NumbaAotBackend.compile`

```python
def compile(
    self,
    strategy: ValidatedStrategy,
    *,
    target_triple: str,
    cpu_baseline: str,
    work_dir: Path,
) -> NativeBuild:
```

- `strategy`：已验证策略；
- `target_triple`：目标平台；
- `cpu_baseline`：指令集下限；
- `work_dir`：临时目录；
- 返回 object files、导出符号、工具链和 linker 信息。

该 backend 是唯一允许依赖 Numba/llvmlite 编译内部 API 的模块，使 SDK 和 runtime 不随 AOT
实现变化。

### 13.8 `emit_callback_bridge`

```python
def emit_callback_bridge(
    handler: ValidatedHandler,
    state_dtype: np.dtype,
    abi: dict[str, object],
) -> NativeSymbol:
```

为 handler 生成 `i32(void*)` wrapper，从 context 读取 state pointer，以编译期 dtype 构造
`StrategyContextType` value，调用单参数 handler，并把异常映射为稳定错误码。

### 13.9 `link_native_library`

```python
def link_native_library(
    objects: tuple[Path, ...],
    *,
    target_triple: str,
    output: Path,
    exported_symbols: tuple[str, ...],
) -> Path:
```

把 bridge/handler/shim objects 链接为共享库，只导出白名单符号。

### 13.10 `write_artifact`

```python
def write_artifact(
    strategy: ValidatedStrategy,
    native: NativeBuild,
    request: CompileRequest,
    staging_dir: Path,
) -> CompiledArtifact:
```

构造 deterministic CBOR manifest，把 initial state、state schema、参数 schema、subscriptions、
能力、build report、library digest 和可选签名写入 manifest。`pair` 模式原子发布两个文件；`bundle` 模式再把两者封装成
单一 `.titan`。`CompiledArtifact` 返回最终文件路径和 digest。

### 13.11 `verify_artifact`

```python
def verify_artifact(
    artifact_path: Path,
    runtime_abi: dict[str, object],
) -> ArtifactManifest:
```

自动识别 pair/bundle，校验文件数、digest、ABI fingerprint、target、导出符号、state schema 和
initial state 长度；签名是否必需由部署 trust policy 决定。验证 bundle 时必须先验证容器目录、
entry 名称和大小上限，再提取 native library；不得加载未验证代码。

## 14. 原生导出符号

```text
titan_strategy_abi_version() -> u32
titan_strategy_descriptor() -> *const NativeStrategyDescriptor
titan_strategy_on_start(void*) -> i32       # 若声明
titan_strategy_on_tick(void*) -> i32        # 若声明
titan_strategy_on_fill(void*) -> i32        # 若声明
titan_strategy_on_order(void*) -> i32       # 若声明
titan_strategy_on_cancel(void*) -> i32      # 若声明
...
titan_strategy_on_stop(void*) -> i32        # 若声明
```

```c
struct NativeStrategyDescriptor {
    uint32_t struct_size;
    uint32_t abi_version;
    uint8_t  abi_fingerprint[32];
    uint32_t state_schema_version;
    uint32_t state_alignment;
    uint64_t state_len;
    uint8_t  state_schema_hash[32];
    uint64_t callback_mask;
};
```

`callback_mask` 的 bit 顺序固定为：`0 on_start`、`1 on_tick`、`2 on_bar`、`3 on_depth`、
`4 on_fill`、`5 on_order`、`6 on_cancel`、`7 on_position`、`8 on_balance`、
`9 on_account_state`、`10 on_timer`、`11 on_stop`。因此本文件示例导出的
`on_start/on_tick/on_fill/on_order/on_cancel/on_stop` 对应十进制 `2163`。保留 bit 必须为 0；manifest、
native descriptor 和实际导出符号必须三方一致。

共享库不导出 Python对象或 Numba dispatcher，runtime 不根据 Python 名称反射 handler。

## 15. Artifact 合同

`pair_arb.manifest.cbor` 的逻辑内容如下；示例使用 JSON 仅为了可读性：

```json
{
  "artifact_format_version": 1,
  "strategy_id": "pair_arb",
  "strategy_version": "1.0.0",
  "abi_version": 13,
  "abi_fingerprint": "...",
  "target_triple": "x86_64-unknown-linux-gnu",
  "cpu_baseline": "x86-64-v2",
  "native_library": "pair_arb.so",
  "callback_mask": 2163,
  "state_schema_version": 1,
  "state_schema_hash": "sha256:...",
  "state_len": 104,
  "state_alignment": 8,
  "initial_state": "<CBOR byte string>",
  "state_schema": {"fields": []},
  "parameter_schema": {"type": "object"},
  "subscriptions": [
    {"event": "bbo", "handler": "on_tick", "schema_version": 1, "qos": "latest"},
    {"event": "fill", "handler": "on_fill", "schema_version": 1, "qos": "reliable_ordered"},
    {"event": "order", "handler": "on_order", "schema_version": 1, "qos": "reliable_ordered"},
    {"event": "cancel", "handler": "on_cancel", "schema_version": 1, "qos": "reliable_ordered"}
  ],
  "capabilities": ["market_data", "order_execution"],
  "source_digest": "sha256:...",
  "parameters_digest": "sha256:...",
  "build": {"compiler": "...", "numba": "...", "llvm": "..."},
  "native_digest": "sha256:...",
  "signature": null
}
```

manifest 使用 RFC 8949 deterministic CBOR。`source_digest` 覆盖唯一 `strategy.py` bytes，
`parameters_digest` 覆盖 canonical 参数值；initial state 仍在签名 payload 内。artifact 必须不可变、
可复现并绑定目标平台，不能跨 target triple 使用。

首版签名是可选部署能力而不是编译前提。开发环境可接受 `signature=null`，生产策略仓库可配置为
必须签名。启用时使用结构体 `{algorithm: "ed25519", key_id: "...", value: <bytes>}`，对去除
`signature` 字段后的 deterministic CBOR payload（其中已包含 native digest）签名；runtime 从部署
trust store 按 `key_id` 验证。SDK 和策略源码不接触密钥。

“可复现”指相同 source、参数、SDK/compiler/toolchain lock、target 和 CPU baseline 产生逐字节相同的
unsigned manifest、initial state 和 native library。compiler 必须固定随机种子，移除绝对路径、时间戳
和非确定 build ID；签名不参与 unsigned reproducibility 比较。

runtime 所称 `artifact_digest` 是“移除 `signature` 字段后的 deterministic CBOR manifest bytes”的
SHA-256；该 manifest 已包含 `native_digest`，因此 identity 同时绑定 native library。digest 不写回
manifest 本身，避免自引用；loader 验证完成后计算并保存在 `StrategyArtifact`，checkpoint 直接记录它。

## 16. AOT 编译约束

- handler 和热路径函数必须模块级定义；
- dtype 必须是模块级常量；
- handler 不捕获闭包变量；
- 配置写入 state，不通过闭包捕获；
- 不使用 Python对象、反射、动态 import；
- 不依赖编译机路径和环境变量；
- native library 只调用 ABI 允许的 host function pointer；
- CPU 指令集由 `cpu_baseline` 控制。

V13 首版不承诺 Numba/llvmlite cross-target 编译。compiler 必须拒绝与 host OS、architecture、pointer
width 或 endian 不同的 target；不同生产 target 使用对应平台的编译服务器生成 artifact。

build report 必须记录 Python、Numba、llvmlite、LLVM、linker 和 SDK 版本。

# 第三部分：策略运行

## 17. 运行阶段职责

生产 runtime 只负责校验 artifact、加载共享库、解析符号、分配状态、构造 context、投递事件、
接收交易命令、停止和快照。生产进程不需要 Python、Numba、llvmlite 或策略源码。

### 17.1 `StrategyInstanceConfig`

```rust
pub struct StrategyInstanceConfig {
    pub strategy_instance_id: u64,
    pub artifact_path: PathBuf,
    pub markets: Vec<(u32, AssetId)>,
    pub accounts: Vec<(u32, AccountId)>,
    pub lane_capacity: usize,
    pub critical_reserve: usize,
    pub reliable_pending_capacity: usize,
    pub max_commands_per_callback: usize,
    pub max_handler_duration: Duration,
    pub cpu_affinity: Option<usize>,
}
```

- `strategy_instance_id`：部署系统分配的稳定非零实例 ID；同一实例重启后保持不变，不同实例不得复用；
- tuple 第一项分别是连续的 `asset_no/account_no`，必须从 0 开始且不得重复；
- `AssetId/AccountId` 来自部署环境，不暴露给策略代码；
- runtime 根据 artifact subscriptions 和这些绑定生成 EventEngine routing keys；
- lane/command capacity/timeout/affinity 是运行策略，不参与 artifact digest；
- `max_commands_per_callback` 必须大于 0，runtime 据此预分配每个实例的 callback command staging；
- artifact 内 initial state 已包含编译参数，运行阶段不得用部署配置改变 dtype 或初始业务参数。

## 18. ABI V13 完整 context 合同

V13 是独立 ABI，不引用 V12 布局。以下所有结构在 Rust 使用 `#[repr(C)]`，在 compiler 使用完全相同
的自然对齐 C layout；整数宽度固定，length/count 一律为 `uint64_t`，不在 ABI 中使用 `usize`。
首版只支持 little-endian 64-bit target，其他 target 在编译时拒绝。

```c
typedef int32_t (*TitanSubmitOrderFn)(
    void *command_context,
    const struct TitanSubmitOrderRequest *request,
    uint64_t *order_id_out);

typedef int32_t (*TitanCancelOrderFn)(
    void *command_context,
    const struct TitanCancelOrderRequest *request,
    uint64_t *command_id_out);

struct StrategyRuntimeContext {
    uint32_t struct_size;
    uint32_t abi_version;
    uint32_t event_kind;
    uint32_t event_schema_version;
    uint32_t flags;
    uint32_t reserved0;

    int64_t  now_ns;
    uint64_t generation;
    uint64_t strategy_instance_id;

    uint8_t *state_ptr;
    uint64_t state_len;
    uint32_t state_alignment;
    uint32_t state_schema_version;
    uint8_t  state_schema_hash[32];

    const struct TitanTickView *ticks_ptr;
    uint64_t ticks_len;
    const struct TitanBarView *bars_ptr;
    uint64_t bars_len;
    const struct TitanDepthView *depth_ptr;
    uint64_t depth_len;
    const struct TitanFillView *fills_ptr;
    uint64_t fills_len;
    const struct TitanOrderEventView *order_events_ptr;
    uint64_t order_events_len;
    const struct TitanCancelEventView *cancel_events_ptr;
    uint64_t cancel_events_len;
    const struct TitanPositionEventView *position_events_ptr;
    uint64_t position_events_len;
    const struct TitanBalanceEventView *balance_events_ptr;
    uint64_t balance_events_len;
    const struct TitanAccountStateEventView *account_state_events_ptr;
    uint64_t account_state_events_len;
    const struct TitanTimerView *timer_ptr;
    uint64_t timer_len;

    const struct TitanMarketView *markets_ptr;
    uint64_t markets_len;
    const struct TitanPositionView *positions_ptr;
    uint64_t positions_len;
    const struct TitanBalanceView *balances_ptr;
    uint64_t balances_len;
    const struct TitanAccountView *accounts_ptr;
    uint64_t accounts_len;
    const struct TitanActiveOrderView *active_orders_ptr;
    uint64_t active_orders_len;

    const void *event_payload_ptr;
    uint64_t event_payload_len;

    void *command_context;
    TitanSubmitOrderFn submit_order;
    TitanCancelOrderFn cancel_order;
    int32_t last_error_code;
    uint32_t reserved;
};
```

`ticks`、`bars`、`depth`、`fills`、`order_events`、`cancel_events`、`position_events`、
`balance_events`、`account_state_events`、`timer` 和 `event_payload` 是当前事件借用视图；其他公共
view 是在当前 lane committed
boundary 上的实例快照。`event_schema_version` 必须等于 artifact subscription 声明的版本。除当前
`event_kind` 对应的字段外，其他当前事件 pointer 必须为 null 且 length
为 0；`on_start/on_stop` 全部为 null/0。`event_payload` 仅供 ABI 后续扩展，标准 V13 handler 不通过它
绕过上述强类型 view。所有 `const` view 在 Numba 中也必须表现为只读类型。

公共 view 的最小字段合同如下；所有价格使用整数 `_ticks`，数量使用整数 `_lots`，时间使用
`_ns`，避免跨语言浮点舍入歧义：

```c
struct TitanTickView {
    uint32_t asset_no; uint8_t kind; uint8_t side; uint8_t reserved[2];
    int64_t exchange_ts_ns; int64_t receive_ts_ns;
    int64_t price_ticks; int64_t qty_lots; uint64_t source_sequence;
};

struct TitanBarView {
    uint32_t asset_no; uint32_t reserved;
    int64_t timeframe_ns; int64_t open_ts_ns; int64_t close_ts_ns;
    int64_t open_ticks; int64_t high_ticks; int64_t low_ticks; int64_t close_ticks;
    int64_t volume_lots;
};

struct TitanDepthView {
    uint32_t asset_no; uint32_t level;
    int64_t exchange_ts_ns; int64_t receive_ts_ns;
    int64_t price_ticks; int64_t qty_lots; uint64_t source_sequence;
    uint8_t side; uint8_t action; uint8_t is_snapshot; uint8_t reserved[5];
};

struct TitanFillView {
    uint64_t order_id; uint32_t asset_no; uint32_t account_no;
    int64_t fill_price_ticks; int64_t fill_qty_lots; int64_t cumulative_filled_lots;
    int64_t exchange_ts_ns; int64_t receive_ts_ns; uint64_t account_sequence;
    uint8_t side; uint8_t liquidity; uint8_t final_fill; uint8_t reserved[5];
};

struct TitanOrderEventView {
    uint64_t order_id; uint32_t asset_no; uint32_t account_no;
    int64_t price_ticks; int64_t qty_lots; int64_t cumulative_filled_lots;
    int64_t event_ts_ns; uint64_t account_sequence;
    uint8_t status; uint8_t reason; uint8_t reserved[6];
};

struct TitanCancelEventView {
    uint64_t order_id; uint32_t asset_no; uint32_t account_no;
    int64_t event_ts_ns; uint64_t account_sequence;
    uint8_t request_result; uint8_t final_status; uint8_t reserved[6];
};

struct TitanPositionEventView {
    uint32_t asset_no; uint32_t account_no;
    int64_t qty_lots; int64_t average_price_ticks; int64_t realized_pnl_ticks;
    int64_t event_ts_ns; uint64_t account_sequence;
};

struct TitanBalanceEventView {
    uint32_t account_no; uint32_t currency_no;
    int64_t total_units; int64_t available_units; int64_t event_ts_ns;
    uint64_t account_sequence;
};

struct TitanAccountStateEventView {
    uint32_t account_no; uint32_t reserved0;
    uint64_t account_epoch; int64_t event_ts_ns; uint64_t account_sequence;
    uint8_t state; uint8_t reason; uint8_t reserved[6];
};

struct TitanTimerView {
    uint64_t timer_id; int64_t scheduled_ts_ns; int64_t fired_ts_ns;
};

struct TitanMarketView {
    uint32_t asset_no; uint32_t flags;
    int64_t best_bid_ticks; int64_t best_bid_qty_lots;
    int64_t best_ask_ticks; int64_t best_ask_qty_lots;
    int64_t tick_size; int64_t lot_size; uint64_t source_sequence;
};

struct TitanPositionView {
    uint32_t asset_no; uint32_t account_no;
    int64_t qty_lots; int64_t average_price_ticks;
    int64_t realized_pnl_ticks; uint64_t account_sequence;
};

struct TitanBalanceView {
    uint32_t account_no; uint32_t currency_no;
    int64_t total_units; int64_t available_units; uint64_t account_sequence;
};

struct TitanAccountView {
    uint32_t account_no; uint32_t reserved;
    uint64_t account_epoch; uint64_t account_sequence;
    uint8_t state; uint8_t reason; uint8_t reserved2[6];
};

struct TitanActiveOrderView {
    uint64_t order_id; uint32_t asset_no; uint32_t account_no;
    int64_t price_ticks; int64_t qty_lots; int64_t cumulative_filled_lots;
    int64_t created_ts_ns; int64_t updated_ts_ns; uint64_t account_sequence;
    uint8_t side; uint8_t order_type; uint8_t time_in_force; uint8_t status;
    uint8_t reduce_only; uint8_t reserved[3];
};
```

具体枚举数值由 `types.py/abi_v13.py` 固定，不能使用 Python Enum object 进入热路径。新增、删除或
重排任何字段都会改变 ABI fingerprint；V13 内不得在保持 fingerprint 不变的情况下修改布局。

命令请求使用固定布局：

```c
struct TitanSubmitOrderRequest {
    uint32_t asset_no;
    uint32_t account_no;
    int64_t  price_ticks;
    int64_t  qty_lots;
    int64_t  trigger_price_ticks;
    int64_t  gtd_expiry_ns;
    uint8_t  side;
    uint8_t  order_type;
    uint8_t  time_in_force;
    uint8_t  reduce_only;
    uint8_t  trigger_kind;
    uint8_t  reserved[3];
};

struct TitanCancelOrderRequest {
    uint64_t order_id;
    uint32_t asset_no;
    uint32_t account_no;
};
```

- `command_context`：Rust-owned command sink 实例，不向策略暴露内部类型；
- `request`：callback 栈上固定布局请求，只在 host function 调用期间有效；
- `order_id_out`：submit 成功时写入 runtime 分配的稳定本地 order ID；
- `command_id_out`：cancel 成功时写入异步撤单命令 ID；
- 返回 `0` 表示命令已被本次 callback 的本地 staging sink 接受，负数表示稳定的同步拒绝码；不表示
  已提交到 execution dispatcher，更不表示交易所接受。

回测和实盘使用相同 host function ABI：回测 sink 把命令写入本地匹配引擎，实盘 sink 投递 execution
dispatcher。handler 永远不等待网络。ABI 只存在一个 command sink 模型，不再同时暴露另一套
command buffer API。

staging 是 runtime 内部实现，不是第二套策略 API。runtime 按 `max_commands_per_callback` 预分配空间；
callback 返回 `0` 时按调用顺序原子提交该批命令，callback 返回错误时整批丢弃并停止实例。容量耗尽
使当前 host call 返回同步拒绝并最终映射为 `COMMAND_ERROR`。因此“callback committed”同时表示
state 修改已结束且本批命令已交给对应的回测或实盘 sink；不代表外部执行结果已完成。

上述全部 `Titan*View` 的每个字段、offset、size、alignment 和语义都在 `abi_v13.py` 与 Rust
`titan-runtime-abi` 的同一 canonical descriptor 中列出。
descriptor 同时编码上述 context、request、view 和 function pointer 签名，SHA-256 结果为
`abi_fingerprint`。compiler、manifest、native descriptor 和 runtime 四方必须完全相同。

callback 返回码固定为：`0=OK`、`-1=HANDLER_ERROR`、`-2=INVALID_CONTEXT`、
`-3=STATE_SCHEMA_MISMATCH`、`-4=COMMAND_ERROR`。native callback 不允许异常或 unwind 穿过 C ABI；
bridge 必须在边界内转换为稳定错误码。

## 19. 对齐状态内存

```rust
pub struct AlignedStateMemory {
    words: Vec<u64>,
    byte_len: usize,
    schema: StateSchemaIdentity,
}
```

`Vec<u64>` 保证 8 字节对齐；分配 word 数为 `(byte_len + 7) / 8`，尾部 padding 必须清零且不属于
有效状态、schema hash 或 snapshot bytes。`byte_len` 必须大于 0，声明 alignment 超过 8 时拒绝。

### 19.1 `from_bytes`

```rust
pub fn from_bytes(
    schema: StateSchemaIdentity,
    initial: &[u8],
) -> Result<Self, StateMemoryError>
```

`schema` 提供 hash/version/len/alignment；`initial` 来自 artifact。要求长度完全一致，内存分配后
不得 resize。

### 19.2 `as_ptr` / `as_mut_ptr`

```rust
pub fn as_ptr(&self) -> *const u8
pub fn as_mut_ptr(&mut self) -> *mut u8
```

返回稳定首地址，供 context 和只读诊断使用。

### 19.3 `as_bytes` / `as_bytes_mut`

```rust
pub fn as_bytes(&self) -> &[u8]
pub fn as_bytes_mut(&mut self) -> &mut [u8]
```

只返回有效字节；可变访问仅允许在实例未运行或 primary lane 安全点。

### 19.4 `clone_for_instance`

```rust
pub fn clone_for_instance(&self) -> Self
```

创建独立内存并复制初始状态。native code/library handle 可以共享，状态不能共享。

## 20. Native Artifact Loader

### 20.1 `inspect`

```rust
pub fn inspect(
    &self,
    artifact_path: &Path,
) -> Result<ArtifactManifest, StrategyLoadError>
```

`artifact_path` 可以是 pair 模式的 manifest 路径或 bundle 路径。读取并校验 native digest、target、
CPU baseline 和 ABI fingerprint；部署策略要求签名时再按 trust store 校验签名。inspect 不执行策略
代码，且在验证 bundle 目录、大小上限和路径安全之前不提取文件。

### 20.2 `load`

```rust
pub fn load(
    &self,
    artifact_path: &Path,
) -> Result<StrategyArtifact, StrategyLoadError>
```

依次 inspect；bundle 模式把 native library 解包到以 digest 命名的只读 cache；加载 library；
解析 descriptor/callback symbols；交叉校验 manifest；从 manifest 读取 initial state；创建
`AlignedStateMemory` 并返回 artifact。

### 20.3 `StrategyArtifact::instantiate`

```rust
pub fn instantiate(
    &self,
    config: &StrategyInstanceConfig,
) -> Result<StrategyInstance, StrategyLoadError>
```

校验 `config.artifact_path` 指向当前 artifact、实例 ID 非零且未占用、编号绑定连续且无重复；计算
binding digest，clone 初始状态，共享只读 callback table 和 library lease，并创建首个实例 generation。

## 21. Callback 调用

### 21.1 `build_callback_context`

```rust
fn build_callback_context(
    instance: &mut StrategyInstance,
    event: RuntimeEventView<'_>,
    commands: &HostCommandSinkBinding,
) -> StrategyRuntimeContext
```

- `instance`：状态和实例元数据；
- `event`：当前只读事件；
- `commands`：回测或实盘实现的统一 host command sink binding；
- 返回栈上 context，不复制 state、不分配 heap。

### 21.2 `CallbackRegistry::invoke`

```rust
pub fn invoke(
    &self,
    kind: StrategyEventKind,
    context: &mut StrategyRuntimeContext,
) -> Result<(), CallbackError>
```

按固定 slot 调用 native function pointer；缺失可选 callback 时成功，非零返回码转为稳定错误。

### 21.3 Native bridge

```text
检查 context ABI/state len/hash
  -> 从 runtime context 读取 state_ptr
  -> 构造策略专用的 native StrategyContext value
  -> ctx.state 绑定为 typed record view
  -> 调用 handler(ctx)
  -> 返回 0 或稳定错误码
```

`ctx` 只在本次 callback 内有效。handler 不得保存 `ctx`、`ctx.state` 或其子 record/array view；
compiler 对逃逸进行拒绝，runtime 则保证 callback 返回前 state pointer 始终有效且对齐。

### 21.4 EventEngine 与策略 lane

策略 callback 不在 EventEngine 的 publisher/EventLoop 线程中执行。每个策略实例注册一个独立的
PRIMARY async lane；EventEngine 只完成路由、保留 `EventLease` 并把事件写入该 lane 的有界队列，
随后立即继续分发其他事件。lane 的专属 worker 才执行策略：

```text
connector/service publish
  -> EventEngine 路由与分配 per-lane admitted_sequence
  -> enqueue 到策略实例的 PRIMARY async lane
  -> publisher 立即返回并继续推送

策略 lane worker
  -> dequeue 一个 EventLease
  -> StrategyEventAdapter 构造当前事件 ABI view
  -> CallbackRegistry 选择 on_tick/on_bar/on_fill/...
  -> native bridge 构造 ctx 并同步调用 handler(ctx)
  -> 提交命令、清空临时 view
  -> committed_sequence 前进并释放 EventLease
```

因此慢策略只会积压自己的 lane，不会占用 EventEngine 发布线程，也不会阻塞其他策略的 lane。
`EventLease` 一直保留到 callback 返回，所以 `ctx` 中的事件指针在 callback 内有效；返回后立即失效。

### 21.5 不把 Numba handler 设计为协程

`on_tick/on_bar/on_fill` 都是 CPU-bound、同步、必须在有限时间内返回的 native 函数，不是
`async def` coroutine。异步函数本身不会让 CPU 计算并行，反而会破坏以下约束：

- 一个策略实例的 `ctx.state` 必须只有一个 writer；
- fill/order/market 事实必须按 lane 的确定顺序生效；
- `EventView` 和 `ctx` 借用的指针只在当前 callback 生命周期有效；
- 回测与实盘必须使用相同的事件次序和状态结果；
- snapshot、pause、stop 和 replace 必须发生在 callback 之间的 safe point。

因此一个策略实例不并发执行多个 handler，也不为 `on_tick/on_bar/on_fill` 分别创建 coroutine。
所有事件和控制操作在同一 lane worker 上串行化。这是 actor/single-writer 模型，不是“每事件一个
异步任务”模型。

当前 PRIMARY async lane 使用隔离的 Rust worker thread。即使未来为了大量策略改成共享的协程/
分片 executor，也只能在两个 callback 之间按 event count 或 time quantum yield；同步 native callback
一旦开始就必须运行到返回，不能在其中挂起或迁移线程。CPU-bound callback 不得直接运行在 Tokio
I/O executor 上，应使用专用计算线程、blocking pool 或独立进程。

下单、撤单等外部 I/O 不在 handler 内等待网络结果。`ctx.submit_order()`/`ctx.cancel_order()` 只调用
Rust host command sink；submit 同步取得本地 order ID，cancel 同步取得 command ID，handler 随即
返回；订单接受、拒绝、成交和撤单结果以后续
`on_order/on_fill/on_cancel` 事件回到同一 lane。

### 21.6 事件类型与 QoS

不同事件不能统一采用“队列满了就丢弃”的策略。subscription 为每个 canonical event 声明 QoS：

| 事件 | 推荐 QoS | 队列压力下的语义 |
|---|---|---|
| BBO、可覆盖的 depth snapshot | `Latest` | 按 event type + routing key 只保留最新值 |
| 可容忍采样的行情/遥测 | `BestEffort` | 满时丢弃并计数，不占用关键事件保留容量 |
| 已关闭 Bar | `ReliableOrdered` | 保序排队；不能静默跳过 |
| fill/order/cancel/position/balance/account state | `ReliableOrdered` | 保序排队；不能丢弃或覆盖 |
| timer 和生命周期控制 | lane safe-point control | 与业务 callback 串行执行 |

Tick 是容器事件名而不是固定可靠性语义：BBO 型 tick 可使用 `Latest`，逐笔成交型 tick 是否可靠由
策略声明决定。高频原始事件应优先在 market service 中批处理或生成 BBO/Bar，再送入策略，而不是
让策略 worker 消费无界逐笔流。

lane 的 `critical_reserve` 为 `ReliableOrdered` 事实保留主队列容量；主队列满后还可进入有界 reliable
pending queue。若 reliable pending 也满，EventEngine 把 lane 标记为 `ResyncRequired` 并发出
subscriber fault；策略 supervisor 再关闭该实例的 command gate。`Latest` 事件发生积压时覆盖旧值；
`BestEffort` 事件可被丢弃。所有覆盖、丢弃和 gap 都由 EventEngine 记录。

### 21.7 EventEngine 的慢 handler 处理

异步 lane 隔离的是 publisher 和其他策略，不可能消除本策略自身的排队延迟。若平均 callback
吞吐低于事件到达率，队列仍会增长。该问题由 EventEngine 的 subscriber lane 统一处理，不属于
Strategy SDK、策略 state、native bridge 或策略业务逻辑。

EventEngine 的 `PrimaryAsyncLaneConfig` 首版只设置一个相关参数：

```text
max_handler_duration
```

该参数由平台部署配置提供，不写入 `strategy.py`，也不进入策略 artifact。EventEngine 在调用通用
`EventHandler::handle()` 前记录开始时间，返回后检查总耗时；这个时间自然包含 event adapter、ABI
context 构造、native callback 执行和本地 command batch commit，不需要 StrategyRuntime 重复计时。

只要出现下列任一情况，EventEngine 就把该 subscriber lane 标记为 `Failed`：

- `EventHandler::handle()` 返回错误或发生可捕获 panic；
- handler 实际耗时大于 `max_handler_duration`。

EventEngine 固定执行：记录 `handler_error` 或 `handler_timeout`、关闭 lane admission、释放该 lane
尚未处理的事件并发送 subscriber fault。策略 supervisor 收到 fault 后关闭该实例的 command gate，
并把实例置为 `Failed`。StrategyRuntime 不保存耗时、超限次数或 timeout 状态。

首版不设置 soft budget，不累计连续超限次数，不做降级、重试、自动恢复或多级熔断。异常停止后
不再调用其他策略 handler；`on_stop` 只用于正常的管理面停止流程。

该检查发生在 `EventHandler::handle()` 返回后。单进程内无法安全中断永不返回的 native 函数；V13
首版不为死循环增加额外 watchdog/协程设计。

策略 handler 仍应有界执行：禁止网络 I/O、文件 I/O、sleep、锁等待和无上限循环。该约束由编译器
静态检查与 EventEngine 外部监控共同保证，不在策略内部实现计时或停止逻辑。

### 21.8 各 callback 的执行关系

`on_tick/on_bar/on_fill/on_order/on_cancel` 共用同一状态和同一事件序列，没有彼此独立的线程：

```text
admitted:  tick#100 -> fill#101 -> bar#102 -> order#103
executed:  on_tick  -> on_fill   -> on_bar  -> on_order
state:     S0        -> S1        -> S2      -> S3       -> S4
```

前一个 callback 返回并提交状态/命令后，后一个 callback 才能观察结果。该规则保证 `on_fill` 写入
的成交事实不会与 `on_tick` 同时修改 Slot，也使 checkpoint 可以在任意两个 callback 之间取得一致
快照。若 `Latest` 在尚未执行前发生覆盖，被覆盖事件不会获得独立 callback；最终 callback 只看到
最新 snapshot，并由 sequence/时间戳识别其新鲜度。

## 22. 状态快照

```rust
pub struct StrategyStateSnapshot {
    pub checkpoint_id: u64,
    pub strategy_instance_id: u64,
    pub generation: u64,
    pub event_committed_sequence: u64,
    pub strategy_id: Arc<str>,
    pub strategy_version: Arc<str>,
    pub artifact_digest: [u8; 32],
    pub binding_digest: [u8; 32],
    pub abi_version: u32,
    pub state_schema_version: u32,
    pub state_schema_hash: [u8; 32],
    pub state_alignment: u32,
    pub state_bytes: Arc<[u8]>,
    pub public_state_identity: [u8; 32],
    pub checksum: [u8; 32],
}
```

### 22.1 `freeze_state`

```rust
pub fn freeze_state(
    &self,
    checkpoint_id: u64,
) -> Result<StrategyStateSnapshot, SnapshotError>
```

在 primary lane callback 边界、且前序 command sink 调用已经提交后复制有效 bytes。snapshot 同时记录
稳定实例 ID、artifact digest、部署绑定 digest、lane `committed_sequence`、实例 generation，以及当时
active orders/position/balance/account epoch 的 canonical identity；不把这些公共事实复制进策略 state
blob。`binding_digest` 覆盖按本地编号排序的真实 AssetId、AccountId 和 routing key，不覆盖 lane 容量、
耗时限制或 CPU affinity。最后计算全部 metadata + bytes 的 SHA-256 checksum。

`public_state_identity` 的输入必须是稳定的 canonical bytes：记录公共 view schema version；account
按 `account_no` 排序，active orders 按 `(account_no, order_id)` 排序，position 按
`(account_no, asset_no)` 排序，balance 按 `(account_no, currency_no)` 排序；所有字段采用 ABI V13
little-endian 固定宽度编码。输入不包含指针、容量、内存地址、owner generation 或 wall-clock 读取
时间。identity 为这些 canonical bytes 的 SHA-256。这样不同进程重建相同公共事实时会得到相同结果。

### 22.2 `restore_state`

```rust
pub fn restore_state(
    &mut self,
    snapshot: &StrategyStateSnapshot,
) -> Result<(), SnapshotError>
```

只允许新实例启动前或已停止实例的安全点；严格校验实例 ID、artifact digest、binding digest、ABI、
策略、schema、长度、alignment 和 checksum，全部通过后一次性复制 bytes。V13 不做 schema migration。

恢复 state bytes 不等于恢复完成。固定恢复流程为：

```text
load artifact + restore private state
  -> lifecycle = RESTORING，command gate 保持关闭
  -> account/runtime service 重建 active orders、position、balance 和 account epoch
  -> 校验 public_state_identity；无法精确重放时执行 reconcile
  -> 检查 pending/unknown command，不自动重发
  -> 分配新的 generation，把已核验的存量活动订单原子 rebind 到新 generation
  -> 建立 EventEngine snapshot barrier
  -> 公共事实与私有 Slot 一致后 lifecycle = READY
  -> 显式 start 后才重新开放 command gate
```

因此 `StrategyStateSnapshot` 是 checkpoint 的策略部分，不是可脱离账户/订单事实独立恢复的完整实盘
快照。identity 不一致、旧订单无法绑定、事件存在 gap 或 reconcile 失败时必须保持停止，不能仅凭
state blob 继续交易。

rebind 只改变 runtime 内部的实例所有权标签，不修改交易所订单，也不重新下单。runtime 以稳定的
`strategy_instance_id + account_no + order_id` 查找 snapshot generation 的未终态订单，逐一核对账户、品种、
side、价格、原始数量、累计成交量和状态后，才把其 owner generation 更新为新 generation，并写入
审计记录。任一订单缺失、重复、字段冲突或状态未知时，整个 rebind 事务失败；新实例仍处于
`RESTORING/FAULTED` 且 command gate 关闭。成功后 `ctx.active_orders()` 才按新的
`strategy_instance_id + generation` 暴露这些订单。

## 23. 松耦合边界

| 组件 | 负责 | 不负责 |
|---|---|---|
| Strategy SDK | Definition、dtype/schema、facade、测试 API | runtime lifecycle、connector |
| Static Compiler | Numba AOT、native bridge、两文件或单容器 artifact | 生产事件循环 |
| EventEngine | lane、事件顺序、QoS、handler 耗时限制、subscriber fault | Strategy ABI、策略状态 |
| Rust Runtime | artifact、状态、事件适配、执行、快照 | lane 调度、handler 耗时限制、策略字段、Python源码 |
| Connector | 标准事件和执行事实 | typed state、schema、策略 library |

编译 backend 隔离 Numba/llvmlite 内部 API，因此更换编译实现不影响策略和 runtime 合同。

## 24. 开发体验

```text
编辑 dtype 和 handler
  -> 快速行为测试
  -> titan strategy compile
  -> 查看结构化错误或 artifact
  -> 在回测/模拟 runtime 运行相同 artifact
```

编译错误必须指向策略字段或函数，例如：

```text
state.indicator_window[]: unsupported big-endian dtype
on_tick: argument count is 2; ABI V13 requires exactly (ctx)
apply_fill: use of Python dict is unsupported in AOT code
state size 81920 exceeds max_state_bytes 65536
```

不得只返回 LLVM/Numba 内部堆栈。

`NumbaHarness` 若保留，只是 Strategy SDK 提供的 Python 测试宿主：按 V13 语义创建测试 context、
注入事件和公共只读 view、调用 `@njit` handler，并检查 state 与 staged commands。它不生成生产
artifact，不让 Rust 编译 Python，也不是 ABI 定义来源。真正的静态编译由第二阶段 compiler/AOT
backend 完成；真正的运行由第三阶段 Rust runtime 加载 native artifact 完成。为了避免误解，公开
类名建议使用 `StrategyTestHarness`，`NumbaHarness` 不作为生产 API。

## 25. 测试体系

### 25.1 编写阶段

- dtype 字段和默认值；
- handler 状态机和命令；
- 非法参数；
- Numba nopython 编译。

测试 harness 只是模拟 V13 context 的测试宿主，不参与生产运行。

### 25.2 静态编译阶段

- schema hash 和 content digest 稳定；
- handler symbols 完整；
- library 无 Python解释器即可加载；
- initial state 逐字节一致；
- 写公共只读 view、保存 ctx/event view、动态 dtype 等非法操作在编译时失败；
- subscription 的 handler、schema、QoS 和 capability 一致性校验；
- ABI descriptor/fingerprint 在 compiler、manifest、native descriptor 和 runtime 四方一致；
- shared library undefined-symbol 和动态依赖白名单通过；
- target/CPU baseline 正确。

### 25.3 Runtime 阶段

- Rust 加载 native artifact；
- callback 修改 typed state；
- 多实例隔离；
- 同一 strategy 的不同 `strategy_instance_id` 不得看到彼此的 active orders；
- tick/fill/order/cancel 完整投递；
- host command sink 在回测/实盘使用相同 ABI；
- callback 成功时提交 staged commands，callback 失败或容量超限时不提交该批命令；
- active orders/market/position 等公共 view 在当前事件应用后可见且编译期只读；
- callback 错误隔离；
- stop、library lease 和 snapshot round trip；
- restore 后 command gate 保持关闭，直至公共事实重建和 reconcile 完成；
- instance/artifact/binding digest 任一不匹配时拒绝 restore。

### 25.4 EventEngine 阶段

- 每实例 PRIMARY lane 串行执行所有 handler；
- `max_handler_duration` 是唯一耗时限制参数；
- handler error、panic 或超时后关闭 admission、释放排队事件并发出 subscriber fault；
- supervisor 收到 fault 后关闭策略 command gate 并停止实例；
- 慢策略、失败策略和满队列不阻塞其他策略或 publisher；
- `LATEST/RELIABLE_ORDERED/BEST_EFFORT` 行为和 gap 记录符合声明。

### 25.5 性能

- 从 EventAdapter、公共 view/context 构造、native bridge、handler 到 command sink 本地接收的完整
  callback 热路径零 heap allocation；
- typed field 不进入 Python解释器；
- 相对同一事件负载下的 ABI V12 双数组 native callback baseline，steady-state 吞吐下降不超过 3%；
- 相对同一 baseline，EventEngine admitted 到 committed 的 p99 延迟退化不超过 5%；
- runtime 不加载 Python/Numba动态库。

## 26. 实施顺序

### A. AOT 可行性验证

1. 根据 `state_dtype` 构造策略专用的 `StrategyContextType`；
2. 编译单参数 `handler(ctx)` 为 object，并验证 `ctx.state` 的类型推导和 lowering；
3. 链接共享库；
4. Rust 无 Python 环境加载调用；
5. 验证 `ctx.state["pair"]`、nested record 和 fixed state array 的原地修改；
6. 验证 host function pointer 下单；
7. 审计 shared library undefined symbols，确认不依赖 `libpython`、NumPy、Numba 或 llvmlite；
8. 明确 Numba NRT 是完全消除还是静态链接，禁止生产环境额外加载 Numba runtime；
9. 验证异常全部在 bridge 内转成错误码，没有 unwind 穿过 C ABI；
10. 在未安装 Python 的干净子进程完成 `dlopen -> descriptor -> callback -> unload`；
11. 确认 event adapter、context/公共 view 构造、state 字段访问和 command sink 热路径零 heap allocation；
12. 测量性能。

上述第 1 至 11 项全部通过才允许继续完整实现；任一失败都必须先缩小 V13 能力范围或更换 AOT
backend，不能以生产 runtime 内嵌 Python/JIT 作为回退方案。

若 nested record 支持不足，可以把 Pair/Slot 字段扁平化，但仍保留具名字段；策略需要指标窗口时
继续使用 fixed numeric/record array，不能退回按基础类型拆分的双数组。

### B. Strategy SDK

实现 Definition、EventSubscription、dtype/schema/hash、单参数 `handler(ctx)` 合同、typed
`ctx.state`、公共只读 view、host command API、V13 harness 和 pair_arb 示例。

### C. Static Compiler

实现隔离 worker、subscription/capability 校验、AOT backend、只读 view lowering、native bridge、
linker、动态依赖审计、artifact writer/verifier 和结构化诊断。

### D. Rust Runtime

实现 ABI V13、native loader、aligned state、公共只读 view、host command sink、callback context、
实例生命周期、checkpoint identity、恢复与 reconcile gate。

### E. EventEngine

为 `PrimaryAsyncLaneConfig` 增加 `max_handler_duration`；实现 handler error/panic/超时后的 lane failure、
subscriber fault 和 supervisor 停止联动，并补齐 QoS/隔离测试。EventEngine 不解析 Strategy ABI。

### F. 验收

用 pair_arb 验证完整三阶段，再验证多个独立策略；在编译服务器执行全量测试和 benchmark，
输出 artifact、性能报告和未满足清单。

### 当前未满足清单

本文是待实现设计。在对应代码和编译服务器证据产出前，以下项目均视为未满足，而不是默认成立：

1. Numba extension type 能否把每个策略的 `ctx.state` 降低为具体 nested structured dtype，并对公共
   view 保持编译期只读；
2. handler、bridge 和所需 runtime 支持能否形成不依赖 `libpython`、NumPy、Numba、llvmlite 动态库的
   可加载 object/shared library；
3. 标准事件 view、host function pointer、nested record 和 fixed array 原地修改是否全部通过无 Python
   进程 smoke test；
4. Rust ABI canonical descriptor、artifact loader、aligned state、实例归属、command staging 和
   checkpoint/rebind 是否已按本文实现；
5. EventEngine 是否已提供每实例 PRIMARY lane、三类 QoS、唯一 `max_handler_duration` 参数及 fault 到
   supervisor 的停止联动；
6. runtime/account/event 服务是否能在同一 checkpoint barrier 提供 public identity、durable sequence、
   pending/unknown command ledger 和安全的 generation rebind；
7. 完整 callback 热路径是否达到零 heap allocation，并满足相对 ABI V12 baseline 的吞吐和 p99 阈值；
8. 旧 pair-arb 需求文档中由 Numba 私有 state 维护完整 active orders 的表述是否已同步改为 runtime
   公共只读 view，避免实现时出现两套事实源。

任何一项失败都必须记录实际证据、影响范围和收缩后的 V13 能力；不得以生产 runtime 内嵌 Python/JIT、
恢复双数组状态接口或让策略直接依赖 connector 作为临时绕过。

## 27. 验收标准

### 策略编写

- 每个策略的编译输入只有一个 `strategy.py`；
- handler 只接收 `ctx`，具名状态统一通过 `ctx.state` 访问；
- 私有可写状态只来自 `ctx.state`，公共行情/订单/仓位只通过 `ctx` 的只读 view 访问；
- subscription 使用公共 EventKind/schema/QoS 声明，不包含 connector 或具体账户对象；
- 无双数组和数字 offset；
- build 无闭包、无外部连接；
- 策略只依赖公开 SDK。

### 静态编译

- 在 `192.168.3.88` 产生平台 native artifact；
- pair 格式严格输出 native library + manifest 两个文件；
- bundle 格式严格输出一个 `.titan` 文件；
- artifact 含原生库、初始状态、schema、参数能力和签名 metadata；
- handler 全部 AOT；
- shared library 不依赖 Python/NumPy/Numba/llvmlite 动态库；
- ABI fingerprint、subscription 和公共只读类型均通过编译校验；
- 构建可复现、失败原子化、错误可读。

### 策略运行

- runtime 不启动 Python、不运行 Numba JIT；
- Rust 持有唯一对齐状态内存；
- callback 通过稳定 C ABI；
- 多实例隔离、快照和停止正确；
- restore 必须经过公共事实重建和 reconcile gate，不能只恢复 state bytes 后直接交易；
- EventEngine handler timeout/fault 能停止单个策略且不阻塞其他实例；
- 热路径零分配并满足性能阈值。

## 28. 结论

V13 建立清晰的三段式结构：

```text
策略编写：以可读性和低开发成本为中心
静态编译：吸收 Python/Numba/LLVM 的复杂度
策略运行：只保留稳定、轻量、高性能的 Rust + native ABI
```

复杂度集中在一次性实现的 SDK 和编译器基础设施中。后续策略只声明状态、实现 handler、执行
静态编译；Rust runtime 和 connector 不随每个策略变化，从而实现松耦合。
