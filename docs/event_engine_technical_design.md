# Titan EventEngine 独立技术实现设计

版本：v1.3

状态：已实现（含 Inline/Async FastLane）

适用范围：单进程、多线程、插件化实盘交易框架

关联文档：[Core Runtime交互契约](core_runtime_contract.md)

实现与验收：[EventEngine实现任务分解与验收记录](event_engine_implementation_plan.md)

## 1. 文档目标

本文定义 Titan 单进程实盘框架中的统一事件引擎，包括：

- 多个行情、账户和系统插件如何发布事件；
- EventEngine 如何完成事件汇聚、编号、分类和订阅路由；
- 多个Publisher如何向一个Subscriber Runtime推送事件；
- 一个Publisher的事件如何被多个Subscriber订阅；
- 如何隔离慢Subscriber，避免阻塞I/O和其他Runtime；
- 如何使用预分配内存和 EventHandle 减少内存复制；
- 如何处理队列容量、事件积压、关键事件可靠性和行情降级；
- EventEngine 如何在独占 CPU 和共享 CPU 两种环境下运行。

本文只设计通用事件基础设施，不包含任何具体策略逻辑，也不将 EventEngine 设计成通用事务协调器。

## 2. 核心结论

系统只保留一个对外可见的 `EventEngine` 概念：

```text
数据和事实：

Publisher Plugin
    -> EventEngine.publish()
    -> SubscriberChannel
    -> Subscriber Runtime回调


操作和命令：

Command Consumer
    -> OrderService.submit()
    -> Account Order Channel
    -> AccountPlugin


操作结果：

AccountPlugin
    -> EventEngine.publish()
    -> SubscriberChannel
    -> on_order/on_filled
```

其中：

- MPSC 是 EventEngine 内部的事件汇聚数据结构；
- SPSC 是 EventEngine 向单个Subscriber Runtime投递事件的内部数据结构；
- Publisher和Subscriber不直接依赖MPSC、SPSC的具体实现；
- 正常路由下 EventEngine 不直接执行策略或其他业务插件回调；只有显式注册的 Inline FastLane
  是 publisher 线程同步回调例外，Async FastLane 则在独立 worker 执行；
- 下单、撤单和查询通过 Service Call 完成，不包装成事件请求；
- ACK、Reject、Fill 等已经发生的结果通过 EventEngine 发布。

## 3. 设计原则

### 3.1 命令与事实分离

```text
Command / Query -> Service Call
Fact            -> EventEngine
```

示例：

```text
submit order     -> OrderService.submit()
cancel order     -> OrderService.cancel()
query snapshot   -> MarketService.snapshot()

order accepted   -> EventEngine.publish(OrderAccepted)
order filled     -> EventEngine.publish(OrderFilled)
risk changed     -> EventEngine.publish(RiskChanged)
```

EventEngine 不承担请求响应匹配，也不承担跨服务分布式事务。

### 3.2 发布不执行订阅者代码

`EventEngine.publish()` 只完成非阻塞事件提交：

```text
写入预分配事件内存
    -> 将EventHandle写入内部MPSC
    -> 立即返回
```

以下操作禁止发生在发布线程：

- 查找并同步调用策略；
- 写数据库；
- 计算指标；
- 等待订阅者；
- 执行外部网络调用；
- 等待交易所 ACK。

### 3.3 有界、预分配、可降级

- 所有内部队列必须有界；
- 启动阶段完成内存池和队列分配；
- 热路径不创建 `String`、无界 `Vec` 或临时序列化对象；
- 队列满必须执行明确策略，不能自动扩容；
- 行情事件和关键事件采用不同的背压规则。

“关键事件可靠”定义为不得静默丢失，并且失败范围可识别、可通过权威状态恢复；不表示在无限持续过载下阻塞EventLoop或使用无界内存维持每个Subscriber连续可用。

### 3.4 一个逻辑EventEngine

架构层只暴露一个 EventEngine。内部可以使用多个优先级队列、事件池和投递通道，但这些都是实现细节：

```text
EventEngine
    EventArena
    CriticalIngress MPSC
    MarketIngress MPSC
    EventLoop
    SequenceGenerator
    SubscriptionRegistry
    RouteTable
    SubscriberChannel[]
    PendingDispatchPool
    SubscriberHealthTable
    FaultSignalRing
    BackpressureController
    Metrics
```

## 4. 总体架构

```text
MarketPlugin Runtime ───┐
AccountPlugin Runtime ──┤
RiskPlugin Runtime ─────┼──> EventEngine.publish()
System Publisher ───────┘            │
                                     v
                                EventEngine
                                     │
                        ┌────────────┼────────────┐
                        v            v            v
                 StrategyRuntime  OMSRuntime  MetricsRuntime
                 SubscriberChannel SubscriberChannel SubscriberChannel
```

下单方向不经过 EventEngine：

```text
Strategy A ──┐
Strategy B ──┼──> OrderService -> Account Order MPSC -> AccountPlugin
Strategy C ──┘
```

多个Publisher向同一个策略发布时，由EventEngine完成汇聚：

```text
MarketPlugin ──┐
AccountPlugin ─┼──> EventEngine -> Strategy A SubscriberChannel
RiskPlugin ────┘
```

同一个Publisher被多个Subscriber订阅时，由EventEngine完成扇出：

```text
                                  ┌──> Strategy A SubscriberChannel
MarketPlugin -> EventEngine ──────┼──> Strategy B SubscriberChannel
                                  └──> Metrics SubscriberChannel
```

## 5. 模块职责

### 5.1 EventPublisher

Publisher Plugin只持有轻量、受权限约束的`EventPublisher`，不持有EventEngine实例：

```rust
pub trait EventPublisher {
    fn try_publish(&self, event: EventEnvelope)
        -> Result<(), PublishError>;

    fn reserve_market_batch(&self)
        -> Result<MarketBatchReservation, PublishError>;
}
```

职责：

- 校验事件头部；
- 根据事件类型选择内部入口；
- 小事件直接内联提交；
- 大型行情通过 reserve/commit 写入 EventArena；
- 队列满时立即返回错误；
- 不执行订阅路由和业务回调。

### 5.2 EventEngine

EventEngine 专属线程负责：

- 从内部 MPSC 读取 EventHandle；
- 分配单调本地序号；
- 校验来源序号；
- 识别事件优先级和路由键；
- 查找预编译订阅表；
- 向SubscriberChannel投递；
- 管理事件引用和回收；
- 处理慢订阅者和背压；
- 维护事件延迟、队列深度和丢弃统计；
- 处理 EventEngine 自身的轻量定时任务。

EventEngine 不负责：

- 更新策略内部状态；
- 调用 `on_market`、`on_order` 或 `on_filled`；
- 下单和撤单；
- 复杂风险计算；
- 数据库存储；
- 日志格式化；
- 收益计算；
- 跨步骤业务事务。

### 5.3 SubscriberRuntime

每个需要隔离的Subscriber拥有独立Runtime；冷路径Subscriber可以使用后台执行器。StrategyRuntime只是其中一种业务实现：

```text
StrategyRuntime
    EventReceiver
    Strategy实例
    MarketView
    OrderView
    PositionView
    LocalRiskView
    CallbackDispatcher
```

StrategyRuntime从SubscriberChannel读取事件后：

```text
更新本地View
    -> 调用策略回调
    -> 收集OrderIntent
    -> 调用OrderService
```

慢Subscriber只积压自己的Channel，不阻塞Publisher I/O、EventEngine和其他Subscriber。

### 5.4 OrderService

OrderService 提供同步非阻塞命令接口：

```rust
pub trait OrderService {
    fn submit(&self, request: OrderRequest)
        -> Result<OrderTicket, SubmitError>;

    fn cancel(&self, request: CancelRequest)
        -> Result<CancelTicket, CancelError>;
}
```

`submit()` 成功只表示：

- 本地参数校验通过；
- 本地风险检查和风险预占完成；
- 本地 OMS 已登记订单；
- 对应Account Connector Order MPSC已接受命令。

它不表示交易所已经接受或成交。交易所结果通过 EventEngine 返回。

## 6. 事件模型

### 6.1 EventEnvelope

事件头部使用固定布局，不包含动态字符串：

```rust
pub struct EventEnvelope {
    pub event_id: u64,
    pub source_id: u16,
    pub event_type: u16,
    pub priority: u8,
    pub flags: u8,

    pub broker_id: u16,
    pub account_id: u32,
    pub asset_no: u32,
    pub strategy_id: u32,

    pub source_sequence: u64,
    pub local_sequence: u64,
    pub trace_id: u64,
    pub causation_id: u64,
    pub exchange_ts: i64,
    pub local_ts: i64,

    pub payload: EventPayload,
}
```

不是所有事件都同时使用全部路由字段。未使用字段采用约定的无效值。

PluginEngine暴露的发布接口必须允许Publisher同时提交`source_id`、`source_sequence`、时间戳、`routing_key`和`flags`；仅携带Topic、Payload和Trace的便捷接口只能作为全部元数据为默认值的简写，不能成为Connector发布关键事实的唯一入口。

`trace_id`与`causation_id`遵循Core Runtime交互契约。EventEngine只原样传播并记录链路时间点，不在热路径构造字符串Span或同步导出追踪数据。

### 6.2 事件分类

关键事件：

```text
OrderAccepted
OrderRejected
OrderPartiallyFilled
OrderFilled
OrderCancelled
PositionChanged
BalanceChanged
RiskChanged
BrokerConnected
BrokerDisconnected
StrategyControl
```

行情事件：

```text
DepthSnapshot
DepthUpdate
Trade
BBO
Ticker
MarkPrice
FundingRate
```

### 6.3 EventHandle

大型 Payload 不在队列之间复制，只传递固定大小句柄：

```rust
pub struct EventHandle {
    pub block_id: u32,
    pub generation: u32,
    pub offset: u16,
    pub length: u16,
    pub event_type: u16,
    pub flags: u16,
}
```

`generation` 用于发现已经回收并重新使用的 Slot，避免悬空句柄被误读。

小型 POD 事件可以直接内联在队列 Slot 中，避免对象池间接访问。是否内联由事件类型在编译期确定。

## 7. EventArena与内存生命周期

### 7.1 预分配

启动时创建固定大小的 EventArena：

```text
EventArena
    SmallEventPool
    MarketBatchPool
    SnapshotPool
```

MarketPlugin行情解码流程：

```text
reserve MarketBatch
    -> 将协议消息直接标准化到Batch
    -> commit
    -> 向EventEngine内部MPSC发布EventHandle
```

标准化完成后到Subscriber消费结束之间不再复制行情Payload。

### 7.2 多订阅者

同一个不可变EventBlock可以由多个Subscriber读取：

```text
EventBlock
    ├── EventHandle -> Strategy A
    ├── EventHandle -> Strategy B
    └── EventHandle -> Strategy C
```

EventEngine 在投递前设置引用数，订阅者释放后递减，最后一个订阅者释放时归还对象池。

### 7.3 慢订阅者限制

共享 EventBlock 不能被慢订阅者无限持有。系统必须监控：

- EventChannel 深度；
- 最老事件年龄；
- EventBlock 持有时间；
- EventArena 可用 Slot；
- 每个Subscriber的未释放Handle数量。

超过阈值时，按事件类型执行合并、丢弃、暂停或重新同步，不能等待对象池耗尽后再处理。

### 7.4 冷路径消费者

Metrics、Logging、Persistence、Reporting 不允许长期持有交易热路径 EventBlock。

推荐方式：

```text
EventEngine
    -> 提取必要字段或复制到冷路径独立对象池
    -> BackgroundExecutor
```

热路径的零复制不能以慢消费者绑架内存生命周期为代价。

### 7.5 EventBlock所有权与内存语义

EventBlock从预留、发布、扇出到回收必须遵守单一、可审计的所有权协议：

```text
reserve成功
    -> Publisher持有1个初始引用
commit进入Ingress
    -> 初始引用转移给Ingress
EventEngine向目标Subscriber投递
    -> 在发布EventHandle前建立1个目标引用
投递成功
    -> 目标引用转移给SubscriberChannel
投递进入pending
    -> 目标引用转移给PendingDispatchEntry
投递失败且不进入pending
    -> 撤销目标引用
完成全部路由
    -> EventEngine释放Ingress引用
Subscriber或pending释放最后一个引用
    -> 清理Block并归还对应Pool Slot
```

引用计数和Pool回收的最小内存语义为：

- EventBlock Payload在`commit`前完整初始化，发布后不可变；
- EventHandle写入Ingress或Subscriber SPSC前必须先建立对应引用；
- 队列生产者使用Release发布Slot，消费者使用Acquire观察Slot和Payload；
- 普通引用递增可以使用Relaxed，但必须发生在承载该引用的EventHandle被Release发布之前；
- 释放引用使用`fetch_sub(1, Release)`；观察到返回值为1的最后释放者执行Acquire fence后才能清理Block；
- Pool归还端以Release发布可复用Slot，分配端以Acquire观察该Slot；
- 引用计数归零后禁止复活，并必须检查引用计数溢出；
- `generation`在Slot重新发布前使用checked increment单调增加，用于发现陈旧Handle，但不替代引用计数和Acquire/Release同步；若当前宽度可能在进程最长运行期内回绕，必须扩大字段宽度或在回绕前退休Slot并报告核心故障，禁止静默回到旧generation；
- enqueue失败、pending取消、Subscriber注销和停止流程都必须显式释放各自持有的引用，任何路径不得重复归还Slot。

第一版优先采用经过验证的引用计数和有界Pool原语；若自行实现裸原子引用计数或freelist，必须通过§19.3定义的并发模型测试。

## 8. 内部MPSC设计

### 8.1 为什么使用MPSC

EventEngine 有多个生产者和一个消费者：

```text
Multiple Producer:
    MarketPlugin Threads
    AccountPlugin Threads
    RiskPlugin
    ControlPlane

Single Consumer:
    EventEngine Thread
```

因此入口天然适合有界 MPSC。

### 8.2 双优先级入口

对外仍然只有一个 EventEngine，内部使用两个 MPSC：

```text
EventEngine
    CriticalIngress: MPSC<EventHandle>
    MarketIngress:   MPSC<EventHandle>
```

目的不是增加架构概念，而是防止行情洪峰阻塞订单、成交、连接和风险事件。

`EventPublisher.try_publish()`根据事件分类自动选择入口，Publisher Plugin不感知具体队列。

### 8.3 MPSC约束

- 固定容量；
- 固定 Slot 布局；
- 每个 Slot 使用 sequence 判断发布和回收轮次；
- 多生产者竞争写序号，单消费者顺序读取；
- 禁止无界扩容；
- 禁止在热路径中进行阻塞等待；
- 大型行情按自然批次发布，降低全局生产者竞争。

### 8.4 MPSC发布、回收与内存序

第一版优先采用经过验证、支持固定容量和非阻塞`try_push`的有界MPSC实现。若自行实现基于Slot sequence的环形队列，必须明确以下语义：

```text
Producer
    -> 通过CAS或等价机制取得确认可写的position
    -> Acquire读取slot.sequence并确认当前发布轮次
    -> 普通写入payload
    -> Release写slot.sequence发布payload

Consumer
    -> Acquire读取slot.sequence并确认当前消费轮次
    -> 普通读取payload
    -> Release写slot.sequence发布Slot已回收

下一轮Producer
    -> Acquire观察Consumer的回收Release后复用Slot
```

实现不得先用不可回滚的`fetch_add`取得ticket，再在发现队列已满时直接返回；这种实现可能留下消费者永远无法跨越的空洞。`try_publish()`必须在无法取得可用Slot时不修改队列的可消费序列，并立即返回`IngressFull`。

sequence宽度和容量必须保证在进程最长运行时间内不会产生不可区分的ABA轮次；回绕边界、多个Producer竞争同一尾部位置以及满队列失败都必须进入§19.3的模型测试。

### 8.5 自然批次

Market Connector以一次网络读取或协议帧解析结果形成批次：

```text
100个行情增量
    -> 1个EventBlock
    -> 1次MPSC发布
```

禁止为了凑满 Batch 主动等待。批处理用于降低队列竞争，不能增加人为延迟。

## 9. EventLoop

### 9.1 处理顺序

```rust
fn drain_once(&mut self) -> usize {
    let mut processed = 0;

    processed += self.drain_critical(CRITICAL_BUDGET);
    processed += self.retry_pending(PENDING_BUDGET);
    processed += self.drain_market(MARKET_BUDGET);
    processed += self.process_due_timers(TIMER_BUDGET);

    processed
}
```

其中每个Budget同时包含条数和墙钟时间上限：

```rust
struct DrainBudget {
    max_items: usize,
    max_elapsed_ns: u64,
}
```

基础处理顺序为：

```text
Critical事件
    -> 待重试投递
    -> Market事件
    -> EventEngine轻量Timer
```

pending retry排在Market之前，因为pending entry会持有EventBlock，及时重试可以降低Arena压力。该顺序表示同一调度周期内的基础优先级，不允许任一类别无界运行。

调度必须满足：

- 每类处理同时受`max_items`和`max_elapsed_ns`限制；
- 单个事件的路由扇出受`max_fanout_per_step`限制，超出后保存路由游标并在后续步骤继续，避免一个大扇出事件垄断EventLoop；
- 在单个处理步骤成本有界的前提下，每个持续非空类别至少每个调度周期获得一次服务；
- `drain_once`记录实际耗时和超预算次数；`max_drain_once_ns`用于告警和PerformanceEnvelope验证，不能作为跳过后续类别的简单硬截断；
- Timer记录`timer_lateness_ns`，超过`timer_max_lateness_ns`时在下一处理边界获得临时优先级提升；
- pending最老年龄超过`max_age`时不再无限重试，按§12.4转入`RESYNC_REQUIRED`；
- Critical长期满载时仍必须给pending、Market和Timer保留有界时间片，不能仅依靠条数budget推导墙钟时间公平性。

条数budget控制吞吐分配，时间budget和扇出步长控制尾延迟；两者缺一不可。

### 9.2 不执行回调

EventLoop 的最后一步是：

```text
subscriber_channel.try_publish(EventHandle)
```

不是：

```text
subscriber.on_event(event)
```

策略回调只能在 StrategyRuntime 线程中执行。

### 9.3 两种运行模式

EventEngine 只提供两种模式，不实现 Park/Notify。

#### Dedicated

```text
持续Drain
    -> 队列为空时cpu_relax
    -> 不sleep
```

```rust
loop {
    let work = event_engine.drain_once();

    if work == 0 {
        cpu_relax();
    }
}
```

特点：

- 独占一个逻辑 CPU；
- CPU 使用率通常接近一个完整核心；
- 不发生休眠和线程唤醒；
- 空闲后第一条事件的处理延迟最低；
- 适合跨所套利、做市和其他低延迟策略。

启用条件：

- 必须配置 CPU affinity；
- 部署层应通过 cpuset、cgroup 或同类机制预留 CPU；
- 避免在超线程兄弟核心运行高负载任务；
- 避免将网卡中断和普通后台服务放在同一核心。

#### SpinSleep

```text
短暂Spin
    -> 持续空闲
    -> sleep约10μs
    -> 再次检查
```

```rust
loop {
    let work = event_engine.drain_once();

    if work > 0 {
        idle_count = 0;
        continue;
    }

    if idle_count < spin_iterations {
        cpu_relax();
        idle_count += 1;
    } else {
        sleep(idle_sleep);
    }
}
```

特点：

- 不需要 Waker、Park状态机和防丢失唤醒逻辑；
- 持续有事件时不会进入 sleep，不影响连续吞吐；
- CPU 使用率低于 Dedicated；
- 空闲后第一条事件可能增加一个 sleep 周期及操作系统调度延迟；
- `sleep_us = 10` 不代表操作系统一定在10μs后准时调度。

适合网格、中低频策略、开发环境和共享 CPU 部署。

### 9.4 IdlePolicy

两种模式必须复用同一套事件处理逻辑，只替换空闲策略：

```rust
pub trait IdlePolicy {
    fn on_active(&mut self);
    fn on_idle(&mut self);
}
```

```text
DedicatedIdlePolicy
SpinSleepIdlePolicy
```

## 10. 订阅与路由

### 10.1 订阅声明

Subscriber在激活前提交数字化SubscriptionSpec：

```text
SubscriptionSpec
    subscriber_id
    event_types[]
    routing_keys[]
    qos
    capacity
```

订阅关系通过Core Runtime契约定义的RouteTransaction暂存，并在EventLoop安全点切换RouteTable版本。固定订阅与动态ScopedEventRouter使用同一事务语义。

### 10.2 预编译路由表

```text
market_routes[event_type][asset_no]
    -> SubscriberSender[]

account_routes[event_type][account_id]
    -> SubscriberSender[]

strategy_routes[strategy_id]
    -> SubscriberSender

order_owner[client_order_id]
    -> strategy_id
```

运行时禁止使用字符串 Topic、反射或动态过滤表达式完成热路径路由。

### 10.3 路由规则

- 行情按`asset_no`路由给全部订阅Subscriber；
- 订单和成交优先按订单所有者路由；
- 账户事件按 `account_id` 路由；
- 风险事件按风险作用域路由；
- 连接事件路由给依赖对应数据源的Subscriber；
- 未找到订单所有者的订单事件进入异常处理和对账流程。

## 11. Subscriber Channel

### 11.1 物理结构

EventEngine是唯一生产者，单个SubscriberRuntime是唯一消费者，因此每个Subscriber使用一条有界SPSC：

```text
EventEngine -> Subscriber SPSC -> SubscriberRuntime
```

Subscriber不感知其底层为SPSC，只持有：

```rust
pub trait EventReceiver {
    fn try_recv(&mut self) -> Option<EventLease>;
}
```

### 11.2 关键事件容量保护

第一版保持单一Subscriber SPSC，并按SubscriptionSpec为关键事件预留容量：

```text
capacity = total_capacity
critical_reserve = reserved_slots

Market事件只允许使用：
    total_capacity - critical_reserve

Critical事件可以使用：
    total_capacity
```

这样避免市场事件完全占满Subscriber队列。关键事件仍保持与此前已投递事件一致的FIFO顺序。

`critical_reserve`是第一层容量隔离，不是独立队列，也不是pending容量：

```text
Market达到可用上限(total_capacity - critical_reserve)
    -> 合并、覆盖或丢弃Market
    -> 不得侵占critical_reserve

Critical到达
    -> 可以使用SPSC全部剩余容量
    -> SPSC无法写入时才进入§12.4的有界pending dispatch
```

SubscriberChannel进入pending状态后，为保持该Subscriber的FIFO顺序，后续Critical不能绕过旧pending事件直接写入SPSC；Market也不能进入关键pending队列，只能按QoS合并、覆盖、丢弃或触发重同步。

`Latest`覆盖槽属于同一逻辑FIFO的一部分：只要槽内还有早于当前Critical的Market，Critical就必须进入pending，直到主队列和该Latest前驱均已消费。禁止让后到Critical越过Latest后又向Subscriber交付更旧行情。

如果基准测试证明单一FIFO无法满足关键事件延迟要求，后续可以在EventEngine内部升级为双优先级SubscriberChannel，但不改变EventReceiver接口。

### 11.3 Subscriber事件顺序

SubscriberRuntime按EventEngine投递顺序消费。对于StrategyRuntime中的订单和成交：

```text
收到EventLease
    -> OMS/OrderView更新
    -> PositionView更新
    -> AccountingView更新
    -> 调用on_order/on_filled
```

策略回调读取到的必须是事件应用后的最新本地状态。

## 12. 背压与降级

### 12.1 总体原则

队列满是系统状态，不是偶发异常。每种事件必须声明：

- 是否允许丢弃；
- 是否允许合并；
- 是否必须重试；
- 是否必须发布背压或数据失效事实；
- 由哪个业务插件负责快照恢复、对账或风险处置。

### 12.2 行情事件

| 事件类型 | 队列满处理 |
|---|---|
| BBO、Ticker、MarkPrice | 合并或用最新值覆盖旧值 |
| DepthSnapshot | 保留最新完整快照 |
| DepthUpdate | 发现缺口后标记失效并重新获取快照 |
| Trade | 按策略订阅要求保留、合并或降级 |
| FundingRate | 保留最新值 |

事件合并必须发生在EventEngine或专用最新值槽中，不能覆盖仍被Subscriber持有的不可变EventBlock。快照恢复由MarketPlugin负责，EventEngine只报告缺口和数据流失效事实。

### 12.3 订单和成交事件

订单、成交、持仓和余额事件不能静默丢弃：

```text
NORMAL
    -> SubscriberChannel达到高水位
LAGGING
    -> Market受限于critical_reserve边界并开始降级
    -> Critical继续使用SPSC全部剩余容量
    -> Critical写入SPSC失败
PENDING
    -> 当前及后续Critical进入该Subscriber的有界pending FIFO
    -> EventLoop按时间和条数budget重试最老pending
    -> SPSC和pending均降到恢复低水位
RECOVERING
    -> 先排空pending，再恢复普通Market投递
    -> 发布SubscriberRecovered
NORMAL
```

进入`LAGGING`时发布`SubscriberLagging/SubscriberBackpressure`事实，RiskPlugin或AccountPlugin据此执行风险限制与对账。状态转换本身必须写入预分配的`SubscriberHealthTable`，不能依赖普通EventArena分配成功；背压事实是状态通知，不是唯一真相来源。

### 12.4 有界PendingDispatch

每个关键Subscriber拥有固定容量的pending FIFO，存储来自启动阶段预分配的`PendingDispatchPool`：

```rust
struct PendingDispatchEntry {
    subscriber_id: SubscriberId,
    event: EventHandle,
    local_sequence: u64,
    enqueued_at_ns: u64,
}
```

PendingDispatch必须满足：

- 同时配置`per_subscriber_capacity`和`global_capacity`，禁止运行期扩容；
- 每个entry持有一个明确的EventBlock目标引用；重试成功时把引用转移给SubscriberChannel，取消或恢复重建时释放引用；
- 同一Subscriber按`local_sequence`保持FIFO，不能让新Critical绕过已有pending；
- pending只接收必须重试的Critical，Market不得占用该容量；
- 重试受`max_items`和`max_elapsed_ns`限制，不得阻塞EventLoop；
- 多个Subscriber共享重试budget时使用跨轮次持久化游标进行round-robin，每个Subscriber每轮至多重试一个entry后让出机会；
- 记录pending深度、最老年龄、重试次数和占用的EventBlock数量；
- `global_capacity`若小于全部关键Subscriber保证配额之和，必须显式声明为共享池并定义配额，不能声称每个Subscriber都获得完整保证。

当pending达到容量上限或最老事件超过`max_age`时：

```text
PENDING
    -> 将SubscriberHealthTable状态原子更新为RESYNC_REQUIRED
    -> 停止向该Subscriber投递Market
    -> 记录未投递local_sequence的起止范围和原因
    -> 不阻塞EventLoop，不扩容pending
    -> RiskPlugin禁止该Subscriber继续扩大风险
    -> 后续Critical只扩展缺失sequence范围，不再进入SPSC或pending
    -> OMS/AccountPlugin通过WAL、账户快照和交易所查询完成重放或对账
    -> 清空pending并释放全部目标引用
    -> 重建本地View并重新激活订阅
```

重新激活必须建立明确的恢复截点：Subscriber先加载截至某个`recovery_sequence`的权威状态，再从该截点之后重新接收事件，不能把旧pending与新快照任意混合。EventEngine只有在旧epoch的队列、pending、正在执行的Handler和全部EventLease均已收敛后，才能从`RECOVERING`切回`NORMAL`并重新打开投递入口。

Subscriber投递入口使用“关闭位+活动Producer计数”的原子admission gate。失败端先原子关闭入口，等待已进入的Producer退出临界区，再清理Channel；`FAILED`和`STOPPED`是不可被`PENDING`、`RECOVERING`或`RESYNC_REQUIRED`覆盖的终态。

EventEngine的可靠性承诺是“关键事件不得静默丢失”，而不是在无限持续过载下保证每个Subscriber的内存内无损连续运行。所有缓冲有界且EventLoop不阻塞时，终局策略必须牺牲受影响Subscriber的连续可用性，并依靠权威状态恢复。

`SubscriberHealthTable`使用预分配Slot和Release/Acquire状态发布。可选的`FaultSignalRing`只用于通知Titan main、RiskPlugin和诊断线程；Ring满时更新丢弃指标，但不能覆盖HealthTable中的权威故障状态，也不能为了报告普通Arena耗尽而再次从普通Arena分配。

### 12.5 非关键订阅者

Metrics、UI、普通日志和统计消费者积压时：

- 允许降采样；
- 允许覆盖最新值；
- 允许丢弃；
- 不允许阻塞交易策略事件投递。

### 12.6 MPSC满载

`try_publish()` 必须立即返回明确错误：

```text
MarketIngressFull
CriticalIngressFull
EventArenaExhausted
InvalidEvent
```

处理原则：

- 行情入口满：更新对应数据流权威Health状态为失效，并尽力发布MarketStreamInvalid通知；
- 关键入口满：更新RuntimeHealth并尽力发布CriticalIngressBackpressure通知，由RiskPlugin和AccountPlugin处置；
- EventArena耗尽：以O(1)成本更新Pool和Subscriber权威Health状态，慢Subscriber Top-K诊断按§12.7异步或增量完成；
- 不允许AccountPlugin或其他Publisher悄悄丢弃关键事件。

关键入口满同样不能通过普通CriticalIngress递归发布背压事实。Publisher必须立即返回错误，同时更新预分配的RuntimeHealth状态；业务插件根据返回值和Health状态执行WAL保留、停止扩大风险或对账。若来源本身没有可重放的权威记录，则必须将该能力缺口作为部署前校验错误，而不能声称关键事件可恢复。

### 12.7 EventArena压力诊断

EventArena接近低水位或耗尽时，检测到压力的Publisher或EventLoop只执行固定成本动作：

```text
更新对应Pool的压力状态和计数
    -> 执行当前事件类型的确定性降级
    -> 更新SubscriberHealthTable或写入有界FaultSignalRing
    -> 继续EventLoop
```

每个Subscriber维护`outstanding_handle_count`、保守的`oldest_handle_timestamp`、`channel_depth`、`pending_depth`和`last_progress_timestamp`，这些值在enqueue、release和消费进度更新时以O(1)成本维护。任意顺序释放Lease时，热路径的oldest时间可以保守地保持更早值，直到未释放数归零后清除；需要精确Top-K或精确最老Lease时由后台诊断完成，不能为维护精确最小值在热路径扫描全部Lease。

“识别占用最多的慢Subscriber”属于诊断动作，不得成为Arena耗尽处理的前置条件。Top-K扫描由后台诊断线程读取上述计数完成；若部署不使用后台诊断线程，则EventLoop采用持久化游标增量扫描，每轮最多检查`pressure_scan_budget`个Subscriber，禁止在压力路径中一次O(N)遍历全部Subscriber。

## 13. 顺序、一致性与去重

### 13.1 顺序保证

EventEngine 保证：

- 同一来源连接内按照 `source_sequence` 检查顺序；
- 按 EventEngine 实际消费顺序分配 `local_sequence`；
- 同一SubscriberChannel保持投递FIFO；
- 同一订单状态不能逆向推进。

EventEngine 不保证：

- 不同交易所 `exchange_ts` 的全局有序；
- 不同网络连接的真实发生顺序；
- 多个订阅者在同一时刻完成处理。

跨 Broker 策略按照本地可见顺序处理，不为了交易所时间戳排序而等待其他 Broker。

### 13.2 重复事件

Broker 重连、查询补偿和私有流可能产生重复事件。去重键优先使用：

```text
broker_id
account_id
exchange_order_id
exchange_trade_id
event_type
```

EventEngine只负责基础重复检测；订单状态幂等和成交去重的最终责任属于 OMS 和账户状态模块。

### 13.3 发布不是多订阅者事务

向多个订阅者投递默认不是原子事务：

```text
Strategy投递成功
Risk投递成功
Metrics队列已满
```

关键Subscriber投递失败时，若有容量则进入有界pending dispatch并发布关键背压事实；pending无法接收或超龄时转入`RESYNC_REQUIRED`并记录缺失范围。非关键Subscriber可以降级。EventEngine不直接执行策略暂停或风险处置，也不能为了保证Metrics接收而阻塞关键事件。

## 14. EventEngine与插件事务边界

EventEngine 支持高效事实传播，不提供通用 ACID 或分布式事务。

### 14.1 单次操作

由 Service 在本地完成：

```text
OrderService.submit()
    -> 参数校验
    -> 风险预占
    -> OMS登记
    -> Account Connector命令队列接收
```

### 14.2 多步骤交易流程

由策略或执行状态机负责：

```text
提交订单
    -> 等待ACK/Fill事件
    -> 计算敞口
    -> 撤单或补偿下单
    -> 完成/失败/恢复
```

EventEngine只负责把状态变化及时送达状态机。

### 14.3 崩溃恢复

跨进程或外部系统一致性依靠：

- 幂等 `client_order_id`；
- WAL/Outbox；
- OMS状态机；
- 交易所订单、成交和持仓对账；
- 重启恢复流程。

内存 EventEngine 不能替代持久化恢复机制。

## 15. 生命周期

### 15.1 启动

```text
Titan main读取Core配置
    -> 创建EventArena、Ingress MPSC、PendingDispatchPool、SubscriberHealthTable和FaultSignalRing
    -> 构造EventEngine但暂不接收业务事件
    -> 构造PluginEngine并注入EventEngine控制句柄
    -> PluginEngine通过EventControlApi暂存RouteTransaction
    -> 创建SubscriberChannel并返回EventReceiver候选
    -> PluginEngine validate全部PluginBundle
    -> Titan main启动EventEngine线程
    -> PluginEngine启动处于ActivationGate保护下的Subscriber
    -> PluginEngine发布引用关闭Gate的GATED Service Endpoint
    -> EventEngine在安全点提交RouteTable版本
    -> PluginEngine对共享ActivationGate执行Release写入ACTIVE
    -> Titan main标记应用RUNNING
```

EventEngine与PluginEngine由Titan main直接启动，是同级核心组件。详细顺序、失败回滚和版本协商以Core Runtime交互契约为准。Publisher开始推送前，EventEngine、必要路由和Subscriber必须就绪。

### 15.2 停止

```text
PluginEngine将Provider Endpoint切换为UNAVAILABLE
    -> Plugin quiesce期间继续接收收敛所需事实
    -> Plugin达到READY_TO_STOP
    -> EventEngine停止向对应订阅投递
    -> 等待Handler并释放未消费EventLease
    -> 退休SubscriptionToken和旧RouteVersion
    -> 停止EventEngine
    -> 验证PendingDispatchPool为空且所有Subscriber Health状态已收敛
    -> 验证EventArena全部归还
    -> Titan main停止PluginEngine并退出
```

停止时不能直接丢弃尚未处理的订单和成交事件。

### 15.3 订阅变更

- 使用RouteTransaction离线构建新订阅表；
- 在EventLoop批次安全点提交路由版本；
- 删除订阅前停止向旧通道投递；
- 等待旧通道已投递事件处理完成或显式释放；
- 不能释放仍被路由表引用的 SubscriberSender。

## 16. 配置建议

### 16.1 共享容量与调度配置

```yaml
event_engine:
  arena:
    small_event:
      slots: 32768
      block_bytes: 256
      low_watermark: 4096
    market_batch:
      slots: 8192
      block_bytes: 16384
      low_watermark: 1024
    snapshot:
      slots: 512
      block_bytes: 262144
      low_watermark: 64

  ingress:
    critical_capacity: 8192
    market_capacity: 65536
    max_sources: 1024

  subscribers:
    max_count: 64
    default_capacity: 16384
    critical_reserve: 2048
    lagging_high_watermark_ratio: 0.80
    recovery_low_watermark_ratio: 0.50
    idle_sleep_us: 10

  pending_dispatch:
    per_subscriber_capacity: 1024
    global_capacity: 8192
    allocation: shared
    guaranteed_per_critical_subscriber: 128
    max_age_ms: 100
    high_watermark_ratio: 0.80

  fault_signal_ring:
    capacity: 256

  dispatch:
    critical:
      max_items: 256
      max_elapsed_ns: 20000
    pending:
      max_items: 64
      max_elapsed_ns: 10000
    market:
      max_items: 256
      max_elapsed_ns: 30000
    timer:
      max_items: 64
      max_elapsed_ns: 10000
    max_fanout_per_step: 64
    max_drain_once_ns: 100000
    timer_max_lateness_ns: 50000
    timer_capacity: 1024

  diagnostics:
    pressure_scan_budget: 8
    trace_ring_capacity: 4096
```

以上数值只是配置结构示例，不是生产默认安全值。`block_bytes`必须满足对应事件类型的最大编码尺寸，启动时校验所有事件布局，禁止运行期截断或退化为堆分配。

`allocation: shared`表示`per_subscriber_capacity`是单Subscriber上限而非完整保证；启动时必须验证`关键Subscriber数量 * guaranteed_per_critical_subscriber <= global_capacity`。`SubscriberHealthTable`按`subscribers.max_count`预分配，超过最大Subscriber数量的RouteTransaction必须拒绝提交。

EventArena各Pool容量按Block而不是业务事件条数估算：

```text
pool_slots >=
    concurrent_reservations
  + ingress_burst_blocks
  + ceil(peak_block_rate * target_hold_time)
  + recovery_reserve
```

容量确定方法：

- `peak_block_rate`使用目标机器在行情和账户峰值下的Block生成速率；MarketBatch按批次数而不是批内行情条数计算；
- `target_hold_time`至少覆盖Ingress排队、EventLoop路由、Subscriber排队和消费的P99.9持有时间；
- 多Subscriber扇出不会直接按订阅者数量复制Block，但最慢Subscriber会决定共享Block持有时间；
- `SmallEventPool`覆盖生产者未提交reservation、CriticalIngress、SubscriberChannel关键事件和pending共同持有的峰值；
- `MarketBatchPool`覆盖Connector并发reservation、MarketIngress积压和Subscriber在途批次；
- `SnapshotPool`按同时恢复的数据流数量、每流最大在途快照数和重建期间保留代际计算；
- 第一版Pool之间不自动借用容量，避免MarketBatch或Snapshot压力侵占关键SmallEvent容量；
- 在50%、80%、95%目标负载和故障注入下测量后加入安全余量，并冻结为版本化PerformanceEnvelope。

`critical_reserve`按关键事件峰值率和风险处置反应时间估算：

```text
critical_reserve >=
    peak_critical_rate
  * (lag_detection_time + risk_action_time)
  + burst_margin
```

### 16.2 Dedicated模式

在§16.1共享配置基础上增加：

```yaml
event_engine:

  runtime:
    mode: dedicated
    cpu_affinity: 4
```

### 16.3 SpinSleep模式

在§16.1共享配置基础上增加：

```yaml
event_engine:

  runtime:
    mode: spin_sleep
    spin_iterations: 10000
    sleep_us: 10
```

## 17. 可观测性

必须提供以下指标：

```text
event_publish_total{source,event_type}
event_publish_rejected_total{reason}
event_dispatch_total{subscriber,event_type}
event_drop_total{subscriber,event_type,reason}
event_delivery_gap_total{subscriber,event_type,reason}
subscriber_resync_total{subscriber,reason}

critical_ingress_depth
market_ingress_depth
subscriber_channel_depth{subscriber}
subscriber_pending_depth{subscriber}
subscriber_pending_oldest_age_ns{subscriber}
subscriber_pending_retry_total{subscriber,result}
subscriber_health_state{subscriber}

event_ingress_to_dispatch_ns
event_ingress_to_subscriber_ns
oldest_event_age_ns{subscriber}
event_block_hold_ns{subscriber}
subscriber_outstanding_handles{subscriber}

event_arena_used_blocks{pool}
event_arena_free_blocks{pool}
event_arena_exhausted_total{pool}
event_arena_pressure_total{pool}

event_loop_drain_count
event_loop_drain_duration_ns
event_loop_budget_exhausted_total{class,limit}
event_loop_service_gap_ns{class}
event_loop_fanout_continuation_total
timer_lateness_ns
event_loop_idle_spin_count
event_loop_sleep_count
event_loop_sleep_overshoot_ns

fault_signal_ring_depth
fault_signal_ring_drop_total
trace_ring_depth{thread}
trace_ring_drop_total{thread}
```

延迟必须统计 P50、P99、P99.9 和最大值。仅统计平均值无法发现调度和队列积压造成的尾延迟。

EventEngine将`trace_id`、`causation_id`和关键时间点写入线程本地有界Trace Ring。后台导出器负责格式化和OpenTelemetry转换，EventLoop不执行字符串格式化、磁盘写入或同步导出。

## 18. 异常处理

| 异常 | 处理 |
|---|---|
| MarketIngress满 | 更新数据流权威Health状态为失效，并尽力发布MarketStreamInvalid通知 |
| CriticalIngress满 | 更新RuntimeHealth，来源保留可重放记录并触发风险处置 |
| SubscriberChannel高水位 | 标记Subscriber为LAGGING并发布事实 |
| Critical无法写入SubscriberChannel | 转入有界pending并保持该Subscriber FIFO |
| PendingDispatch满或超龄 | 标记RESYNC_REQUIRED，记录缺失范围，停止Market投递并触发权威状态恢复 |
| EventArena接近低水位 | 执行当前Pool的确定性降级，后台或增量识别慢Subscriber |
| EventArena耗尽 | 更新权威Health状态，不在EventLoop同步O(N)扫描或递归分配告警事件 |
| 来源sequence跳跃 | 标记数据投影失效并发布SequenceGap |
| 未知订单事件 | 发布UnknownOrderEvent，由AccountPlugin处理 |
| EventEngine线程异常 | 向Titan main报告核心故障并退出EventLoop |
| SubscriberRuntime异常 | 发布SubscriberRuntimeFailed，不执行交易业务处置 |

EventEngine只执行基础设施级标记、背压和事实上报。暂停策略、禁止扩大风险、撤单、账户对账和恢复启用由RiskPlugin或相应业务插件决定。

## 19. 测试方案

### 19.1 正确性测试

- 多生产者并发发布不丢事件；
- MPSC容量回绕正确；
- EventHandle generation检查有效；
- 多订阅者引用计数最终归零；
- 订阅增加、删除和路由版本切换正确；
- Core Runtime API major不兼容时拒绝启动；
- RouteTransaction暂存、提交、失败回滚和旧版本退休；
- ActivationGate打开前不执行Handler；
- 同一Subscriber接收多个Publisher事件；
- 同一Publisher事件扇出到多个Subscriber；
- 关键容量预留生效；
- Market不能侵占`critical_reserve`；
- Critical仅在SPSC写入失败后进入pending；
- 同一Subscriber存在pending时新Critical不能绕过旧事件；
- pending重试成功、取消和转入恢复时EventBlock引用正确转移或释放；
- pending满和超龄时进入RESYNC_REQUIRED并记录完整缺失sequence范围；
- Subscriber按`recovery_sequence`完成权威状态重建后恢复，旧pending不能与新快照混合；
- Fill应用状态后才调用 `on_filled`；
- 重复订单和成交事件保持幂等；
- 停止时所有EventLease均被回收。

### 19.2 压力测试

- 多Market/Account Publisher同时发布峰值事件；
- 行情批量大小变化；
- 单个Subscriber持续慢消费；
- 多个Subscriber订阅同一行情；
- Critical与Market同时达到峰值；
- EventArena接近耗尽；
- 三个Arena Pool分别耗尽且互不侵占保留容量；
- pending达到每Subscriber上限和全局上限；
- Critical持续满载时Market、pending和Timer的最大service gap；
- 大扇出事件分步路由时其他类别仍获得时间片；
- 压力诊断后台扫描或增量扫描不造成EventLoop延迟尖峰；
- Connector断线、恢复和快照重建；
- Dedicated与SpinSleep模式的CPU和延迟对比。

### 19.3 并发模型测试

MPSC、SPSC、EventBlock引用计数、Pool回收和生命周期竞争必须使用`loom`或等价并发模型工具覆盖，不能只依赖长时间压力测试：

- Producer完成Payload写入后以Release发布，Consumer以Acquire读取完整Payload；
- MPSC多个Producer竞争、队列满失败、容量回绕和sequence轮次切换不产生空洞；
- EventEngine建立目标引用发生在SubscriberChannel或pending发布之前；
- enqueue失败与目标引用撤销竞争不导致泄漏或提前归还；
- 最后两个Subscriber并发释放时恰好一个线程归还Pool Slot；
- pending重试成功、Subscriber注销和停止流程竞争时引用只转移或释放一次；
- Pool Slot归还、generation增加和重新分配之间的Release/Acquire关系成立；
- 陈旧generation的EventHandle不能读取已复用Payload；
- RouteTable版本切换、Subscriber注销和旧EventLease释放竞争安全；
- SubscriberHealthTable的Release状态更新能被Titan main和RiskPlugin的Acquire读取观察；
- FaultSignalRing满时不会覆盖权威Health状态或递归占用普通EventArena。

模型测试必须使用足够小的容量强制探索满队列、回绕、最后引用和停止边界，并在CI中固定运行。

### 19.4 端到端基准

关键时间点：

```text
t0 Market Connector收到网络消息
t1 Market Connector完成交易所语义处理、标准化并publish
t2 EventEngine取得事件
t3 EventEngine写入SubscriberChannel
t4 StrategyRuntime取得EventLease
t5 策略回调开始
t6 OrderService接受订单
t7 Account Connector I/O开始发送
t8 Socket send完成
```

核心指标：

```text
t3 - t1  EventEngine路由延迟
t5 - t0  行情到策略延迟
t7 - t5  策略到Account Connector发送线程延迟
t8 - t0  行情到Socket发送端到端延迟
```

实验室初始门槛与PluginEngine PerformanceEnvelope保持一致：

| 路径 | 初始参考目标 |
|---|---:|
| EventEngine已入队到SubscriberChannel可见 | P99不高于5us |
| DEDICATED Subscriber取事件到Handler入口 | P99不高于2us |
| 热路径堆分配 | 0 |
| 关键事件静默丢失 | 0 |

基准必须记录硬件、NUMA、CPU affinity、编译参数、50%/80%/95%负载以及P50、P99、P99.9和最大值。正式目标在目标硬件完成原型测试后冻结为版本化PerformanceEnvelope。

### 19.5 FastLane低延迟扩展

FastLane 是 EventEngine 的显式 opt-in 交付能力，不是默认路由，也不改变 Connector 对 Snapshot、
sequence、epoch、checksum 和恢复的所有权。发布到 FastLane 的同一事件仍进入正常 EventEngine route，
供兼容、审计和恢复消费者使用。

#### Inline FastLane

```text
Publisher -> descriptor/routing-key match -> handler -> normal ingress
```

Inline 模式在 publisher 线程直接执行 handler，适用于执行时间有严格上界、无阻塞 I/O、无高竞争锁
的内存内策略。handler error/panic 只停用对应 FastLane route，并写入 `SubscriberFailed` fault signal；
正常 ingress publication 继续进行。

#### Async FastLane

```text
Publisher
    -> clone OwnedEvent handle（EventArena retain）
    -> bounded priority/normal ArrayQueue
    -> active wake / dedicated worker
    -> handler

Publisher -> normal ingress -> EventLoop -> audit Subscriber
```

一个 Async FastLane group 可匹配多个 `(event_type, schema_version)` 和 routing key，并共享一个 worker。
队列元素只包含 descriptor、header 和引用计数 arena handle，不复制 payload。实现要求：

- queue capacity 必须为正且有界；publisher 使用非阻塞 push，不等待 handler；
- 当前 `capacity` 是每个 priority class 的容量；priority/normal 两个 queue 的理论合计上限为
  `2 * capacity`，`fast_lane_depth_max` 记录合计深度；
- `Dedicated` 必须配置 CPU affinity；`SpinSleep` 先自旋再 park timeout；`Park` 由 producer 主动
  `unpark`；
- priority 和 normal 各自保持 FIFO；priority 可绕过 normal backlog，但正在执行的 handler 不可抢占；
- 同一要求全序的业务流必须映射到同一 lane 和同一 priority class；跨 priority 不提供全序；
- handler error/panic 原子关闭 admission、清空未执行队列并记录 `SubscriberFailed`；
- QueueFull 表示有序 lane 已产生不可修复 gap：增加 `fast_lane_drop_total`，写入
  `SubscriberBackpressure`，关闭 lane admission，排空 gap 前队列后退出；禁止丢弃一条后继续交付；
- unregister/stop 关闭 admission、唤醒 worker、排空已接收事件并 join，随后才能验证 EventArena
  outstanding block 为零；
- 普通 EventEngine route 与 FastLane 故障隔离，FastLane 过载不得拒绝正常 ingress。

严禁使用每事件 `tokio::spawn`、无界 channel，或在 publisher 上等待 worker 释放队列空间。

新增可观测指标：

```text
fast_lane_enqueue_total
fast_lane_drop_total
fast_lane_depth_max
fast_lane_enqueue_latency
fast_lane_latency            # handler执行时间
```

2026-09-01 Binance Futures production stream 验证环境为 2 个逻辑 CPU、1 个物理核。Inline 模式
Depth steady 三轮 p50 为 `18.219/19.669/18.211us`。Async 模式在测试 handler 包含时间戳计算、
payload copy 和 channel send 时，publisher enqueue p50 为 `0.127-0.255us`、p99 约 `1.023us`，
实盘轮次 `fast_lane_drop_total=0`、`publish_rejected_total=0`。Async consumer 端 Depth steady p50
约 `29-31us`，并会随 handler 成本产生尾延迟；该模式的首要保证是 publisher 隔离而非保持 Inline
端到端延迟。正式生产数据必须在 Connector 和 worker 分属不同物理核的机器重新冻结。

## 20. 实施顺序

### 第一阶段：基础事件链路

- EventEnvelope和事件分类；
- 有界Critical/Market MPSC；
- EventEngine线程和RouteTable；
- 每Subscriber有界SPSC Channel；
- Core Runtime API版本协商；
- RouteTransaction和SubscriptionToken；
- Dedicated和SpinSleep两种IdlePolicy；
- 基础指标。

### 第二阶段：内存与扇出

- 分池EventArena及容量配置；
- reserve/commit接口；
- EventHandle和generation；
- 多订阅者引用计数及Acquire/Release语义；
- MPSC、引用计数和Pool回收的loom模型测试；
- 慢消费者检测和回收保护。

### 第三阶段：可靠性

- 行情合并和MarketStreamInvalid事实；
- 有界PendingDispatchPool及每Subscriber配额；
- Subscriber `NORMAL/LAGGING/PENDING/RECOVERING/RESYNC_REQUIRED`状态机；
- SubscriberHealthTable、FaultSignalRing和缺失sequence范围；
- RiskPlugin和AccountPlugin处置事件集成；
- Core Runtime契约生命周期与回滚测试。

### 第四阶段：性能验证

- 多Broker峰值压测；
- CPU affinity和物理核心隔离验证；
- Arena、Ingress、Subscriber和pending容量调优；
- 条数budget、时间budget和最大扇出步长调优；
- EventArena压力诊断开销验证；
- P99/P99.9尾延迟优化；
- 与现有实盘事件链路进行端到端对比。

## 21. 最终边界

EventEngine 的稳定职责为：

```text
接收事实
    -> 管理事件内存
    -> 编号与分类
    -> 查找订阅者
    -> 非阻塞投递
    -> 背压与降级
    -> 生命周期与观测
```

以下职责明确排除：

```text
策略执行
下单命令处理
业务风险计算
收益计算
数据库事务
跨交易所事务
订阅者同步回调
```

最终核心模型保持为：

```text
事实：Plugin -> EventEngine -> Subscriber
命令：Plugin -> Service -> Target Plugin
流程：StateMachine + Service + Event
```

这套模型在不引入StrategyShard、全局回调链和通用事务总线的前提下，覆盖多Publisher、多Subscriber和多插件的主要实盘事件场景，并为后续内部性能优化保留稳定接口。
