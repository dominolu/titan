# Titan Core Runtime交互契约

版本：v1.0

状态：核心组件公共契约

适用范围：Titan main、EventEngine、PluginEngine

## 1. 文档目标

本文是EventEngine与PluginEngine之间唯一权威的交互契约，只定义跨组件接口、状态机、内存生命周期和启动停止顺序，不定义组件内部实现。

PluginEngine与EventEngine文档不得分别复制并修改本契约语义。接口变更必须先升级`core_runtime_api_version`并更新兼容矩阵和集成测试。

## 2. 契约版本

```text
CoreRuntimeApiVersion
    major
    minor
```

- major不一致时拒绝启动；
- minor新增只能增加向后兼容能力；
- 删除字段、改变状态语义或改变内存所有权必须升级major；
- Titan main在构造两个核心组件时完成版本协商。

## 3. 权威职责

```text
EventEngine拥有：
    EventArena
    EventHandle/EventLease语义
    Ingress队列
    SubscriberChannel生产端
    RouteTable及版本切换
    EventControlApi

PluginEngine拥有：
    PluginPlan
    PluginSlot生命周期
    ServiceRegistry/EndpointSlot
    ResourceScope
    Plugin ActivationGate

Titan main拥有：
    两个核心组件的进程级启动和停止顺序
    核心版本协商
    核心故障后的进程级退出
```

## 4. 公共类型

```text
EventControlApi
RouteTransaction
RouteVersion
PublisherCapability
ScopedRouteCapability
SubscriptionToken
SubscriberChannel
EventReceiver
EventHandle
EventLease
TraceContext
```

插件只能持有受限的`PublisherCapability`、`ScopedRouteCapability`和`EventReceiver`，不能获得EventEngine内部注册表或RouteTable可变引用。

## 5. 路由事务

### 5.1 状态机

```text
CREATED
    -> STAGED
    -> COMMITTING
    -> ACTIVE
    -> RETIRING
    -> RETIRED

失败：ABORTED
```

### 5.2 接口语义

```text
begin_route_update(base_version)
    -> RouteTransaction

stage_subscription(txn, owner, spec)
    -> SubscriptionTokenCandidate

commit_at_safe_point(txn)
    -> (RouteVersion, CommittedSubscription[])

CommittedSubscription
    token: SubscriptionToken
    receiver: EventReceiver

abort(txn)

retire_subscription(token)
    -> RetireTicket
```

- STAGED路由对EventLoop不可见；
- `commit_at_safe_point()`只在EventLoop批次边界切换RouteTable版本；
- commit成功前不能释放旧RouteTable引用的SubscriberSender；
- abort必须释放候选Channel、引用和未激活Token；
- EventEngine不得持有Plugin Handler，也不得创建或管理插件Subscriber Runtime；
- EventReceiver只在调用者拥有的Runtime线程上同步驱动一次投递，EventEngine线程不执行Handler；
- RouteVersion单调递增，不允许复用；
- 动态订阅和插件固定订阅使用同一事务语义。

## 6. EventHandle与EventLease

```text
EventEngine向SubscriberChannel写入EventHandle
    -> EventReceiver.try_recv()创建EventLease
    -> Handler通过EventLease读取只读EventView
    -> EventLease释放引用
    -> 最后一个引用归还EventArena
```

- EventHandle只是固定大小的跨队列句柄；
- EventLease是消费者侧生命周期守卫；
- Handler不得保存超出回调生命周期的裸EventView引用；
- 冷路径消费者不得长期持有交易热路径EventLease；
- 删除订阅前必须停止新投递并回收或释放所有已投递Lease。

## 7. 插件装配与激活

Service Endpoint和RouteTable属于两个不同组件，不能声称由一个CPU原子操作同时切换。框架使用ActivationGate和可回滚事务实现业务不可见的逻辑提交：

```text
ConfigurationAdapter生成PluginSpec[]
    -> PluginEngine编译PluginPlan
    -> PluginFactory创建PluginBundle
    -> RuntimeHost校验Bundle授权范围
    -> 创建PluginSlot、ResourceScope和关闭状态ActivationGate
    -> ServiceRegistry暂存EndpointSlot
    -> EventEngine暂存RouteTransaction
    -> validate全部PluginBundle
    -> Titan main启动EventEngine
    -> 按依赖顺序Plugin.start()，执行任务保持SUSPENDED
    -> ServiceRegistry发布引用关闭ActivationGate的GATED Endpoint generation
    -> EventEngine在安全点commit RouteTransaction
    -> PluginSlot生命周期准备进入RUNNING
    -> 对ActivationGate执行一次Release写入ACTIVE
    -> Service、Publisher与执行任务同时获得业务可见性
```

提交期间：

- 插件Publisher在ActivationGate打开前返回`RuntimeNotActive`；
- SubscriberChannel可以在Gate打开前接收少量事件，但Handler不能执行；
- ServiceHandle在Endpoint发布前返回`ServiceUnavailable`，Endpoint已发布但Gate未打开时返回`RuntimeNotActive`；
- EndpointVersion、EventPublisher和Subscriber Runtime必须引用同一个ActivationGate；
- 任一步失败都必须保持Gate关闭、撤销路由候选、将Endpoint置为UNAVAILABLE并释放ResourceScope；
- 该流程是控制面事务，不承诺跨组件线性化原子点。

## 8. 动态路由

PluginEngine在装配期向获授权插件签发`ScopedRouteCapability`。插件内部Runtime可以通过该能力提交路由事务：

```text
ScopedEventRouter.subscribe()
    -> 校验事件类型、QoS、容量和数字路由键
    -> EventEngine创建RouteTransaction
    -> safe point提交
    -> 返回EventReceiver与SubscriptionToken
```

动态路由不重新编译PluginPlan，也不经过PluginEngine Control Thread。SubscriptionToken必须登记到对应Child ResourceScope。

## 9. 停止顺序

```text
PluginSlot进入QUIESCING
    -> EndpointSlot切换为UNAVAILABLE
    -> quiesce期间继续接收完成收敛所需的事实事件
    -> Plugin返回READY_TO_STOP
    -> EventEngine停止向该订阅投递新事件
    -> 等待当前Handler返回
    -> 回收队列中的EventLease
    -> retire SubscriptionToken和旧RouteVersion
    -> Plugin.stop()
    -> 关闭ResourceScope
    -> PluginSlot进入STOPPED
```

停止EventEngine前必须保证所有关键订阅已经完成上述流程并验证EventArena引用归零。

## 10. TraceContext

跨Event和Service Command统一传播：

```text
TraceContext
    trace_id: u64
    causation_id: u64
```

`process_run_id + trace_id`形成跨重启唯一标识。TraceContext只负责技术链路关联，订单号、策略号和账户号仍作为业务关联字段。

## 11. 必需集成测试

- 两个核心组件API major不兼容时拒绝启动；
- RouteTransaction提交、失败回滚和旧版本退休；
- RouteTable切换与EventLease回收竞争；
- ActivationGate打开前Handler和Publisher均不可运行；
- Endpoint激活失败时路由候选完整回滚；
- Subscriber停止时不存在悬空Sender；
- EventEngine停止时EventArena引用归零；
- TraceContext跨事件、Service Command和结果事件保持关联。
