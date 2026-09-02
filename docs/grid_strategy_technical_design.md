# Titan 通用实盘交易框架技术设计

版本：v3.3
状态：技术方案设计

## 1. 定位

本文设计一套单服务、插件化、性能优先的实盘交易框架。具体交易逻辑、交易场所接入、风险规则、存储、核算和观测能力都通过插件提供。

框架目标：

- 插件可以独立开发、组合、替换和测试；
- 插件之间不依赖具体实现，只依赖稳定Service接口和Typed Event；
- 行情触发到订单发送全部使用进程内预绑定调用；
- 订单、账户和风险状态具有明确的唯一写者；
- 存储、核算、指标和日志不阻塞交易热路径；
- 插件卸载时自动释放监听器、定时器、连接和其他资源；
- 配置决定应用由哪些插件组成，内核不包含具体交易业务。

核心可以概括为：

```text
PluginKernel
+ Context Service
+ Inject
+ Fiber
+ Effect
+ Typed Events
+ Profile
```

## 2. 设计原则

### 2.1 一切业务能力都是插件

框架内核只管理插件，不实现具体业务。常见能力可以由以下插件提供：

```text
StrategyPlugin
ExchangePlugin
OrderPlugin
RiskPlugin
StorePlugin
AccountingPlugin
ObservabilityPlugin
```

这些名称是推荐能力包，不是内核写死的插件分类。插件可以同时提供多个Service，也可以只订阅事件完成一个很小的扩展。

### 2.2 插件之间不直接引用

插件不能持有其他插件实例，也不能导入其他插件的具体实现。

插件之间只有两种交互方式：

```text
Service Call   调用明确能力并获得结果
Typed Event    发布已经发生的事实
```

共享状态通过Service提供的只读View读取，不再增加第三套通信机制。

### 2.3 性能优先

- 热路径Service在启动时完成解析和绑定；
- 每笔事件不做字符串查找、反射和动态依赖解析；
- 进程内不做JSON/Protobuf序列化；
- 交易线程不访问数据库、消息代理和阻塞日志；
- 使用有界队列、预分配对象和预热连接；
- 账户分区使用单写者事件循环，不加全局锁；
- 非关键消费者与交易线程隔离。

### 2.4 正确性优先于无限扩展

所有业务可以插件化，但以下约束不能由配置绕过：

- 订单只能通过 `ctx.orders` 提交；
- 每个订单发送前必须调用 `ctx.risk`；
- 活动订单和订单状态只有OrderPlugin可以写；
- 账户和风险投影出现缺口时禁止扩大风险；
- 有活动订单时禁止直接热替换交易关键插件。

## 3. PluginKernel

```text
PluginKernel
  Context
  ServiceRegistry
  PluginLoader
  FiberRuntime
  EffectManager
  EventDispatcher
  ProfileLoader
  Supervisor
```

### 3.1 Context

Context是插件使用框架能力的唯一入口：

```text
PluginContext
  identity
  config

  orders
  risk
  market
  account
  positions
  clock

  events
  effect()
  plugin()
  logger()
```

具体Context属性由Service接口扩展。插件只能访问其声明依赖的Service。

高频只读状态通过View提供：

```text
AccountView
PositionView
OrderView
MarketView
RiskView

revision
source_sequence
updated_at
stale_after
```

View不可修改。需要改变订单、风险预占或生命周期时必须调用Service方法。

### 3.2 ServiceRegistry

Provider向Context注册Service：

```text
ctx.provide("orders", orderService)
ctx.provide("risk", riskService)
ctx.provide("exchange.binance", exchangeService)
```

Consumer声明依赖：

```text
inject = ["orders", "risk", "market"]
```

规则：

- Service名称必须命名空间化；
- 单写Service在一个作用域内只能有一个Provider；
- 必需Service不存在时插件保持PENDING；
- Provider停止时依赖插件先进入QUIESCING；
- 可选依赖使用 `ctx.get(name)`，不能伪装成必需依赖；
- 启动完成后将Service解析为强类型调用句柄。

### 3.3 FiberRuntime

每个插件实例对应一个Fiber：

```text
PENDING
  -> LOADING
  -> ACTIVE
  -> QUIESCING
  -> UNLOADING
  -> DISPOSED

异常：FAILED
恢复：RECONCILING
```

Fiber负责：

- 插件配置和依赖快照；
- 插件私有Context；
- 子插件树；
- Effect集合；
- 生命周期状态；
- 异常和诊断信息。

### 3.4 EffectManager

插件创建的外部资源必须注册为Effect：

```text
ctx.effect(() => {
  resource = acquire()
  return () => release(resource)
})
```

以下操作默认返回可逆Effect：

- `ctx.events.on()`；
- `ctx.provide()`；
- `ctx.plugin()`；
- `ctx.schedule()`；
- 连接和文件监听注册。

卸载时按注册逆序释放。有关联顺序的异步清理应放在同一个Effect中顺序执行。

### 3.5 ProfileLoader

Profile描述一个完整应用由哪些插件组成：

```text
Profile
  Bundle[]
  ProfilePatch
  LocalPatch
  StartupPatch
```

Patch根据稳定的entry ID：

- 插入插件；
- 替换Provider；
- 修改配置；
- 禁用插件；
- 隔离Service作用域。

交易关键插件的配置变更只生成待应用版本，不能自动热更新正在运行的实例。

### 3.6 Supervisor

Supervisor负责：

- 捕获插件异常；
- 记录回调耗时和连续失败；
- 暂停故障实例；
- 隔离慢插件；
- 处理Mailbox高水位；
- 发布插件健康状态；
- 触发安全停止和恢复对账。

StrategyPlugin异常只暂停对应实例；Order、Risk或Exchange Service异常禁止相关账户扩大风险。

## 4. 插件定义

### 4.1 PluginManifest

```text
PluginManifest
  id
  name
  version
  api_version
  entrypoint
  config_schema

  provides[]
    service
    version
    scope

  inject[]
    service
    version_range
    required

  publishes[]
    event
    schema_version

  subscribes[]
    event
    schema_version
    qos
    partition_by
    mailbox_capacity

  runtime
    execution_group
    max_callback_time
    failure_policy
```

Manifest用于启动校验和权限控制，不参与逐笔交易。

### 4.2 插件接口

```text
Plugin
  validate(config)
  apply(ctx, config)
```

`apply()`完成Service注册、事件订阅和资源Effect创建。它返回后Fiber进入ACTIVE。

策略插件通常实现：

```text
on_start(ctx)
on_market(ctx, event)
on_order(ctx, event)
on_filled(ctx, event)
on_account(ctx, event)
on_risk(ctx, event)
on_timer(ctx, event)
on_stop(ctx, reason)
```

回调只能修改插件私有状态并调用已注入Service，不能直接获得其他插件实例。

### 4.3 推荐Service

| Service | 唯一职责 |
|---|---|
| `ctx.orders` | 订单提交、撤单、改单、状态机和幂等 |
| `ctx.risk` | 检查、风险预占、释放和风险模式 |
| `ctx.market` | 行情订阅和只读市场视图 |
| `ctx.account` | 账户、余额、持仓和保证金视图 |
| `ctx.exchange.*` | 交易场所协议和连接 |
| `ctx.store` | Journal、Snapshot和恢复材料 |
| `ctx.accounting` | 成本、费用、收益和权益投影 |
| `ctx.metrics` | 非阻塞指标发布 |

业务插件可以增加新Service，但不能覆盖这些Service的权威状态边界。

## 5. 插件调用与消息推送

### 5.1 Service Call

需要立即结果或修改唯一权威状态时使用Service：

```text
result = ctx.orders.submit(orderIntent)
result = ctx.orders.cancel(cancelIntent)
view   = ctx.account.view()
mode   = ctx.risk.mode()
```

Service Call规则：

- 调用的是稳定接口，不是Provider具体类型；
- 热路径调用必须是本地、同步、有界和非阻塞；
- 启动时预绑定Provider和方法句柄；
- 返回不可变结果，不暴露内部可变对象；
- 调用失败返回明确错误，不无限等待；
- 同步Service依赖图不能成环；
- 反向通知必须发布事件，不能在同一调用栈回调Consumer。

### 5.2 Typed Event

已经发生的事实使用事件推送：

```text
ctx.events.publish("order.accepted", event)
ctx.events.publish("order.filled", event)
ctx.events.publish("account.updated", event)
ctx.events.publish("risk.mode.changed", event)
```

订阅：

```text
ctx.events.on("order.filled", handler)
```

事件规则：

- Publisher不知道Subscriber；
- 发布成功只表示Kernel接管事件；
- 订阅回调不在发布者当前调用栈执行；
- 每个有状态插件实例拥有独立Mailbox；
- 同一分区内保持顺序；
- 监听器随所属Fiber自动注销；
- 事件失败不能回滚已经发生的交易事实。

### 5.3 消息QoS

只保留三种消息语义：

| QoS | 用途 | 队列满处理 |
|---|---|---|
| `LATEST` | 高频行情和非关键最新状态 | 同一键覆盖旧值 |
| `RELIABLE_ORDERED` | 订单、成交、账户和风险事实 | 背压并停止扩大风险 |
| `BEST_EFFORT` | 指标、追踪和调试日志 | 采样或丢弃并计数 |

不使用一个通用队列承载全部消息。

### 5.4 事件结构

进程内事件使用紧凑Header和强类型Payload：

```text
EventHeader
  event_id
  event_type
  partition_key
  aggregate_id
  aggregate_sequence
  correlation_id
  causation_id
  occurred_at
  observed_at
```

`RELIABLE_ORDERED`消费者按event ID幂等，并根据aggregate sequence检测重复、乱序和缺口。

### 5.5 可选拦截事件

非热路径可以提供可拦截事件：

```text
config/validate
plugin/before-start
control/before-command
```

监听器通过 `next()` 继续或返回错误阻止操作。以下链路禁止使用动态waterfall：

```text
订单提交
风险预占
订单发送
成交状态更新
```

这些链路使用固定Service调用顺序，确保性能和可审计性。

## 6. 下单热路径

```text
Exchange WS
  -> ExchangePlugin.decode()
  -> StrategyPlugin.on_market()
  -> ctx.orders.submit(intent)
       -> ctx.risk.check_and_reserve(intent)
       -> OrderPlugin.prepare(intent)
       -> ctx.exchange.send(command)
  -> Exchange
```

这是固定调用链，不允许插件动态调整顺序。

热路径完成后异步发布：

```text
OrderEvent
RiskReservationEvent
AuditEvent
MetricEvent
```

存储、核算、查询、指标和日志只消费这些事件。

### 6.1 订单状态机

```text
CREATED
  -> RISK_ACCEPTED
  -> PREPARED
  -> SENT
  -> ACKNOWLEDGED
  -> PARTIALLY_FILLED
  -> FILLED

终态：REJECTED / CANCELED / EXPIRED
异常态：SEND_UNKNOWN / RECONCILING
```

规则：

- 状态迁移单调且幂等；
- 成交事实独立于订单终态处理；
- 发送超时先查询交易场所，不能直接重发；
- SEND_UNKNOWN继续占用风险额度；
- 只有拒单或撤单确认后才能释放剩余预占。

### 6.2 风险预占

```text
准备订单       -> 预占
订单已发送     -> 保持预占
部分成交       -> 成交占用 + 剩余预占
完全成交       -> 转为持仓占用
拒单           -> 释放全部预占
撤单确认       -> 释放剩余预占
发送结果未知   -> 保持预占并查询
```

## 7. 并发与隔离

### 7.1 单写者

账户订单和风险状态按account ID分区，每个分区只有一个事件循环可以写。

```text
AccountPartition
  OrderPlugin
  RiskPlugin
  AccountView
  ExchangeSession
```

网络I/O可以使用独立线程，但规范化事件必须回到所属分区更新状态。

### 7.2 Mailbox

- 每个策略实例拥有独立Mailbox；
- 同一实例回调串行执行；
- 行情按venue和instrument分区；
- 订单、成交和账户事件按account ID分区；
- Mailbox有固定容量和高水位；
- 慢实例只能阻塞自身，不能阻塞其他实例；
- 回调超过预算由Supervisor记录并触发降级。

### 7.3 Context隔离

不同账户或运行实例可以拥有独立Context：

```text
rootContext
  accountContext[A]
    strategyContext[A1]
    strategyContext[A2]
  accountContext[B]
    strategyContext[B1]
```

子Context继承公共Service，但账户、订单、风险和配置使用隔离作用域。

## 8. 持久化与恢复

### 8.1 异步持久化

以下事实最终必须持久化：

- 插件版本和配置摘要；
- 实例生命周期；
- 订单意图、状态和归属；
- 逐笔成交；
- 账户、费用和资金事件；
- 风险预占和风险模式；
- Fiber状态快照和消费水位；
- 恢复与对账结果。

除订单发送可靠性需要的最小记录外，都由StorePlugin异步处理。

### 8.2 订单持久化模式

| 模式 | 顺序 | 特点 |
|---|---|---|
| `strict` | 每笔WAL fsync后发送 | 故障窗口最小，尾延迟最高 |
| `group_commit` | 微批WAL fsync后发送 | 可靠性与延迟平衡，默认模式 |
| `async_reconcile` | 内存登记后发送，后台落盘 | 延迟最低，依赖交易场所查询恢复 |

### 8.3 恢复

```text
1. 启动PluginKernel和基础Service
2. 加载插件版本、配置、快照和消费水位
3. 关键Fiber进入RECONCILING
4. 查询交易场所订单、成交、余额和持仓
5. 补放缺失事件并重建Context View
6. 核对订单、账户和风险预占
7. 隔离未知订单和无法解释的差异
8. 对账成功后激活策略Fiber
```

恢复期间禁止扩大风险，不能根据当前持仓猜测遗漏成交，也不能自动认领未知订单。

## 9. 性能设计

### 9.1 热路径约束

- Service方法句柄启动时预绑定；
- 不使用动态Proxy完成逐单Service查找；
- 不进行序列化和对象深拷贝；
- 订单、事件和协议缓冲预分配或池化；
- 不使用全局锁和无界容器；
- Gateway连接预热并复用；
- 交易线程不运行异步消费者；
- 指标写入无阻塞Ring Buffer；
- 回调中禁止磁盘、网络和阻塞日志操作。

### 9.2 延迟测量

```text
T0  WS bytes received
T1  event decoded
T2  strategy callback completed
T3  risk accepted
T4  order prepared
T5  bytes handed to socket
T6  exchange acknowledgement received
```

```text
internal_order_latency = T5 - T0
strategy_latency       = T2 - T1
risk_order_latency     = T4 - T2
gateway_latency        = T5 - T4
exchange_round_trip    = T6 - T5
```

分别验收p50、p99和p99.9。压测必须覆盖突发行情、多策略共享账户、慢消费者、回报乱序和队列高水位。

## 10. Profile配置

```yaml
profile: live-default

plugins:
  - id: store
    module: titan-store-local

  - id: risk
    module: titan-risk-default

  - id: orders
    module: titan-orders
    inject: [risk, store]

  - id: exchange-main
    module: titan-exchange-example
    inject: [orders]

  - id: strategy-example
    module: user-strategy-example
    inject: [orders, market, account]

  - id: accounting
    module: titan-accounting
    inject: [store]

  - id: metrics
    module: titan-metrics
```

启动前输出最终插件树、Service依赖图、事件订阅表和Fiber状态，便于诊断PENDING依赖和配置覆盖。

## 11. 测试与验收

### 11.1 Kernel测试

- Service注册、替换、隔离和唯一性；
- Inject依赖、PENDING、启动和逆序停止；
- Fiber状态迁移；
- Effect自动释放和异常清理；
- Profile、Bundle和Patch组合；
- Typed Event类型、顺序和自动退订；
- Mailbox背压和Supervisor隔离。

### 11.2 插件契约测试

- Manifest和配置schema；
- Service API版本兼容；
- ExchangePlugin订单能力和恢复查询；
- OrderPlugin状态机和幂等；
- RiskPlugin预占、转换和释放；
- StorePlugin快照和事件重放；
- 异步插件故障不阻塞交易线程。

### 11.3 验收门槛

- 插件只依赖Service接口，不依赖Provider实现；
- 插件卸载后不残留监听器、定时器、连接和Service；
- 同步Service依赖不存在环；
- 下单固定链路不能被配置绕过；
- 逐笔下单不访问数据库、消息代理和阻塞日志；
- 重复事件不产生重复订单、持仓和账务；
- 任意重启点都能与交易场所完成对账；
- 慢异步插件不增加交易线程尾延迟；
- 各持久化模式达到配置的延迟和恢复目标。

## 12. 实施顺序

1. 实现Context、ServiceRegistry、Inject和Service隔离；
2. 实现Fiber、EffectManager和插件生命周期；
3. 实现ProfileLoader、Bundle、Patch和配置校验；
4. 实现Typed Event、QoS、Mailbox和Supervisor；
5. 实现Order、Risk和Exchange基础Service契约；
6. 实现固定下单链路、订单状态机和风险预占；
7. 实现Store、Accounting和Observability参考插件；
8. 实现持久化模式、恢复和交易场所对账；
9. 提供模拟交易、回放、插件示例和契约测试工具；
10. 完成性能基准、故障注入和小资金实盘验收。
