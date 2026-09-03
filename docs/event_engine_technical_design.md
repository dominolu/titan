# Titan EventEngine 独立技术实现设计

版本：v1.4

状态：v1.4 内部主体能力已实现；跨组件 SnapshotBarrier、Primary 迁移与生产性能验收待完成

适用范围：单进程、多线程、插件化实盘交易框架

关联文档：[Core Runtime交互契约](core_runtime_contract.md)

当前实现与验收（v1.3）：[EventEngine实现任务分解与验收记录](event_engine_implementation_plan.md)

## 1. 文档目标

本文定义 Titan 单进程实盘框架中的统一事件引擎，包括：

- 多个行情、账户和系统插件如何发布事件；
- EventEngine 如何完成事件汇聚、编号、分类和订阅路由；
- 多个Publisher如何向一个 Plugin AsyncLaneWorker 推送事件；
- 一个Publisher的事件如何被多个Subscriber订阅；
- 所有 Plugin 如何通过统一的 Async FastLane Primary 模式异步消费事件；
- 如何隔离慢Subscriber，避免阻塞I/O和其他Runtime；
- 如何使用预分配内存和 EventHandle 减少内存复制；
- 如何处理队列容量、事件积压、关键事件可靠性和行情降级；
- EventEngine 如何在独占 CPU 和共享 CPU 两种环境下运行。

本文只设计通用事件基础设施，不包含任何具体策略逻辑，也不将 EventEngine 设计成通用事务协调器。

### 1.1 v1.4 变更摘要

- 将 Async FastLane 从 v1.3 的可选 mirror 优化升级为所有 Plugin 的默认 Primary 事件交付方式；
- EventEngine 统一拥有 consumer queue、worker、EventLease、QoS 和生命周期，Plugin 只提供 EventHandler；
- 为 `RELIABLE_ORDERED` 增加有界 PendingDispatch，不以阻塞 publisher 换取可靠性；
- 增加 admitted/dispatched/committed 三段提交水位；
- 将 SubscriberHealthTable 定义为故障与恢复的权威状态；
- 正式定义 SnapshotBarrier、StreamBoundary、staging 和 recovery generation；
- Inline FastLane 降为受 capability 限制的固定成本内部投影，不承载普通 Plugin handler。

该版本改变 SubscriptionSpec、EventEnvelope metadata 和 consumer 生命周期，必须通过新的 Core Runtime
Event API major/capability 协商启用；v1.3 Plugin 只能在迁移期继续使用旧 normal consumer，不能被静默
切换到 v1.4 Primary lane。

### 1.2 当前实施边界

仓库已实现 Primary Async lane、可靠 pending、admitted/dispatched/committed 水位、SubscriberHealth、
SnapshotBarrier staging/replay 以及显式 v1.3 compatibility adapter。SnapshotBarrierRegistry 现按配置统一
限制活动 barrier 数、每 barrier staging 和全局 staging 总量；deadline 覆盖 staging/replay 全过程，容量耗尽、
超时、boundary 缺失、abort 和 lane stop 都会确定性释放 staging lease 与全局额度并保持
`RESYNC_REQUIRED`。尚未完成的是 Provider 和消费者的
跨组件迁移：Market `request_snapshot`、Account `reconcile` 仍需携带 barrier/boundary，关键 consumer
仍需完成独立 Mirror 对比后切换 Primary，目标硬件 P99/P99.9 也尚未冻结。因此内部类型存在不等于
端到端 v1.4 已验收，旧 SubscriberChannel 业务路径暂不能退休。

## 2. 核心结论

系统只保留一个对外可见的 `EventEngine` 概念：

```text
数据和事实：

Publisher Plugin
    -> EventEngine.publish()
    -> per-consumer Async FastLane
    -> EventEngine-managed worker
    -> Plugin EventHandler


操作和命令：

Command Consumer
    -> OrderService.submit()
    -> Account Order Channel
    -> AccountPlugin


操作结果：

AccountPlugin
    -> EventEngine.publish()
    -> Async FastLane
    -> on_order/on_filled
```

其中：

- MPSC 是 EventEngine 内部的事件汇聚数据结构；
- 每个 Plugin/Strategy 实例默认拥有独立 Async FastLane 和 worker，Publisher 与 Consumer 不依赖队列
  具体实现；
- EventEngine 管理 worker 并在 worker 上调用 Plugin 提供的 `EventHandler`，永远不在 publisher 或
  EventLoop 线程执行业务回调；
- Inline FastLane 不再是 Plugin 的标准消费方式，仅保留给经过 capability 授权、固定成本的内部投影；
- `LATEST/RELIABLE_ORDERED/BEST_EFFORT` 是 QoS，Async FastLane 是交付方式，两者正交；
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

`EventEngine.publish()` 只完成有界、非阻塞的事件提交和预编译路由入队：

```text
写入预分配事件内存
    -> 将EventHandle写入内部Ingress和目标Async FastLane
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
    AsyncFastLane[]
    AsyncLaneWorker[]
    PendingDispatchPool
    SubscriberHealthTable
    SnapshotBarrierRegistry
    SnapshotStagingPool
    FaultSignalRing
    BackpressureController
    Metrics
```

### 3.5 统一交付模型

v1.4 将 `AsyncFastLane` 设为所有 Plugin 事件消费的标准交付方式。这里的 “FastLane” 表示 publisher
到独立 consumer worker 的直接、有界、异步交付路径，不表示放宽可靠性：

```text
DeliveryMode = ASYNC_FAST_LANE
QoS          = LATEST | RELIABLE_ORDERED | BEST_EFFORT
Role         = PRIMARY | MIRROR
```

- `PRIMARY` 是 Plugin 的唯一业务交付路径，同一 Plugin 不再同时消费 normal route；
- `MIRROR` 仅用于审计、性能比较或迁移验证，必须使用不同 SubscriberId；
- 同一事件可以投递给多个 Primary lane，每个 lane 的队列、worker、health、pending 和恢复代际相互隔离；
- Publisher 对每个目标只执行固定成本的 retain 和非阻塞 admission，不等待 worker、pending 重试或恢复；
- 每个事件的 Primary direct fanout 必须有配置上限；RouteTransaction 超过上限时拒绝提交，不能让动态订阅
  把 publisher 路径退化为无界 O(N) 扇出；
- 低频 Plugin 仍使用同一交付模型，可以选择 Park/SpinSleep，而不是再实现私有消费线程。

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
                 Async Lane A   Async Lane B   Async Lane C
                    Worker A       Worker B       Worker C
                        │            │            │
                 StrategyHandler  OMSHandler  MetricsHandler
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
AccountPlugin ─┼──> EventEngine -> Strategy A Async FastLane -> Worker -> Handler
RiskPlugin ────┘
```

同一个Publisher被多个Subscriber订阅时，由EventEngine完成扇出：

```text
                                  ┌──> Strategy A Async FastLane
MarketPlugin -> EventEngine ──────┼──> Strategy B Async FastLane
                                  └──> Metrics Async FastLane
```

## 5. 模块职责

### 5.1 EventPublisher

Publisher Plugin只持有轻量、受权限约束的`EventPublisher`，不持有EventEngine实例：

```rust
pub trait EventPublisher {
    fn try_publish(&self, event: EventDraft)
        -> Result<(), PublishError>;

    fn reserve_market_batch(&self)
        -> Result<MarketBatchReservation, PublishError>;
}
```

`EventDraft` 不包含 `source_sequence/local_sequence`；受限 SourcePublicationLane 在成功接受 publication 时
生成它们并形成内部 `EventEnvelope`，避免 Provider 伪造或重复使用交付坐标。

职责：

- 校验事件头部；
- 根据事件类型选择内部入口；
- 小事件直接内联提交；
- 大型行情通过 reserve/commit 写入 EventArena；
- 队列满时立即返回错误；
- 只读取不可变的预编译 RouteSnapshot，并对目标 Async lane 执行固定成本非阻塞 admission；
- 不构建动态路由，不执行 pending 重试、恢复或业务回调。

`try_publish()` 的成功表示事实已被 EventEngine 接受，不表示每个 Primary lane 都已成功 admission。单个
lane 的 queue/pending 失败通过 SubscriberHealth 进入 `RESYNC_REQUIRED`，不能反向阻塞或回滚其他 lane；
只有 EventArena/Ingress/SourcePublicationLane 自身无法接受事实时才向 Publisher 返回错误。

实现顺序必须先保留所需 EventArena/Core Ingress 资源；若该步骤失败，不得向任何 Primary lane 暴露本次
事实。核心接受成功后再执行目标 fanout，个别 consumer 失败按上一段降级。这样 Publisher 不会收到
“返回失败但部分 Plugin 已经处理”的不确定结果。

### 5.2 EventEngine

EventEngine 核心、publication lane、EventLoop 和 AsyncLaneWorker 共同负责：

- publication lane 分配 source/local sequence，并按预编译 RouteSnapshot 直投 Primary Async lane；
- EventLoop 从内部 MPSC 读取审计、恢复和控制 EventHandle；
- 校验来源序号；
- 识别事件优先级和路由键；
- 查找预编译订阅表；
- 向 Async FastLane 执行非阻塞投递；
- 管理 lane worker、提交水位、SubscriberHealth、可靠 pending 和 SnapshotBarrier；
- 管理事件引用和回收；
- 处理慢订阅者和背压；
- 维护事件延迟、队列深度和丢弃统计；
- 处理 EventEngine 自身的轻量定时任务。

EventEngine 不负责：

- 更新策略内部状态；
- 解释事件后选择 `on_market`、`on_order` 或 `on_filled` 等业务回调；AsyncLaneWorker 只调用预注册的
  opaque `EventHandler`，业务分派属于 Plugin；
- 下单和撤单；
- 复杂风险计算；
- 数据库存储；
- 日志格式化；
- 收益计算；
- 跨步骤业务事务。

### 5.3 AsyncLaneRuntime

每个需要隔离的 Plugin/Strategy 实例默认拥有独立 Async lane 和 worker。EventEngine 统一管理消费循环，
Plugin 只注册 handler 和生命周期 gate：

```text
AsyncLaneRuntime
    bounded admission queue
    optional reliable pending
    SubscriberHealth slot
    progress watermarks
    dedicated/shared worker policy
    EventHandler
```

worker 取得 EventLease 后：

```text
推进 dispatched_sequence
    -> 调用 Plugin EventHandler
    -> handler 成功返回并释放 EventLease
    -> 推进 committed_sequence
```

StrategyRuntime、AccountRuntime 和 RiskRuntime 等业务对象不再各自实现 `dispatch_next` 线程。慢
Subscriber 只积压自己的 lane；每实例独立 worker 时，不阻塞 Publisher I/O、EventLoop 和其他
Subscriber。显式共享 worker 的冷路径 lane 只在该 group 内共享故障和延迟预算。

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
    pub source_lane_id: u16,
    pub source_generation: u32,
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

pub struct SourceStreamId {
    pub source_id: u16,
    pub publication_lane_id: u16,
    pub source_generation: u32,
}

#[repr(transparent)]
pub struct OrderingDomainId(pub u64);
```

不是所有事件都同时使用全部路由字段。未使用字段采用约定的无效值。

`SourceStreamId` 标识可独立排序和恢复的 publication lane；source generation 变化表示旧 cursor 失效。
`OrderingDomainId` 在订阅编译期生成，用于声明必须 FIFO 的事件集合，不能依靠 priority 猜测。

PluginEngine 为每个 Provider 注入受限 `SourcePublicationLane`。Publisher 提交 `source_id`、时间戳、
`routing_key`、flags 和 payload；publication lane 在成功接受事件时串行分配 `source_sequence`，失败时不
提交序号。Provider 的交易所 sequence、market update sequence 或 account version 保存在规范 payload
中，不得冒充 EventEngine `source_sequence`。仅携带 Topic、Payload 和 Trace 的无来源便捷接口不能成为
Connector 发布关键事实的入口。

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
    -> SourcePublicationLane向Primary Async lane和内部Ingress发布EventHandle
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

- Async lane queue/pending 深度；
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
commit进入SourcePublicationLane
    -> 为内部Ingress和每个目标Async lane分别建立引用
目标lane投递成功
    -> 对应目标引用转移给Async lane queue
投递进入pending
    -> 目标引用转移给PendingDispatchEntry
投递进入recovery staging
    -> 目标引用转移给SnapshotStagingEntry
投递失败且不进入pending
    -> 撤销目标引用
完成全部Primary路由和Ingress admission
    -> SourcePublicationLane释放Publisher初始引用
EventLoop完成内部Ingress处理
    -> 释放Ingress引用
AsyncLaneWorker、pending或staging释放最后一个引用
    -> 清理Block并归还对应Pool Slot
```

引用计数和Pool回收的最小内存语义为：

- EventBlock Payload在`commit`前完整初始化，发布后不可变；
- EventHandle写入Ingress、Async lane queue、pending或Snapshot staging前必须先建立对应引用；
- 队列生产者使用Release发布Slot，消费者使用Acquire观察Slot和Payload；
- 普通引用递增可以使用Relaxed，但必须发生在承载该引用的EventHandle被Release发布之前；
- 释放引用使用`fetch_sub(1, Release)`；观察到返回值为1的最后释放者执行Acquire fence后才能清理Block；
- Pool归还端以Release发布可复用Slot，分配端以Acquire观察该Slot；
- 引用计数归零后禁止复活，并必须检查引用计数溢出；
- `generation`在Slot重新发布前使用checked increment单调增加，用于发现陈旧Handle，但不替代引用计数和Acquire/Release同步；若当前宽度可能在进程最长运行期内回绕，必须扩大字段宽度或在回绕前退休Slot并报告核心故障，禁止静默回到旧generation；
- enqueue失败、pending取消、Subscriber注销和停止流程都必须显式释放各自持有的引用，任何路径不得重复归还Slot。

实现优先采用经过验证的引用计数和有界Pool原语；若自行实现裸原子引用计数或freelist，必须通过§19.3定义的并发模型测试。

## 8. 内部MPSC设计

### 8.1 为什么使用MPSC

EventEngine 的审计、恢复和控制 Ingress 有多个生产者和一个 EventLoop 消费者：

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

Primary Async lane 同样是多 Publisher、单 AsyncLaneWorker 的有界 MPSC，但拥有独立 QoS、pending、health
和提交水位；它不需要先由 EventLoop 转发。

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

实现优先采用经过验证、支持固定容量和非阻塞`try_push`的有界MPSC实现。若自行实现基于Slot sequence的环形队列，必须明确以下语义：

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

### 9.2 Publisher 与 EventLoop 不执行业务回调

Primary Async lane 的发布步骤是：

```text
route_snapshot.lookup(event)
    -> async_lane.try_admit(EventHandle)
    -> 立即返回
```

不是：

```text
subscriber.on_event(event)
```

Plugin handler 只在 EventEngine 管理的 AsyncLaneWorker 上执行。Inline FastLane 是经过单独 capability
授权的内部例外，不得承载普通 Plugin handler、Numba 回调或可能阻塞的业务逻辑。

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
    delivery_mode = async_fast_lane
    delivery_role = primary | mirror
    capacity
    critical_reserve
    pending_capacity
    worker_policy
```

订阅关系通过Core Runtime契约定义的RouteTransaction暂存，并在EventLoop安全点切换RouteTable版本。固定订阅与动态ScopedEventRouter使用同一事务语义。

每个 Plugin 实例默认注册一个 `PRIMARY` lane。`subscriber_id + subscription_generation` 唯一标识一次
lane 代际；replace、resync 和重新订阅必须创建新 generation，旧 generation 的 EventLease、handler 和
health 不能泄漏到新 lane。

### 10.2 预编译路由表

```text
market_routes[event_type][asset_no]
    -> AsyncLaneSender[]

account_routes[event_type][account_id]
    -> AsyncLaneSender[]

strategy_routes[strategy_id]
    -> AsyncLaneSender

order_owner[client_order_id]
    -> strategy_id
```

运行时禁止使用字符串 Topic、反射或动态过滤表达式完成热路径路由。

Primary direct delivery 要求 publication lane 能只读访问不可变 RouteSnapshot。RouteTransaction 在控制
安全点构建新版本，并用 Release 原子切换 snapshot 指针；publisher 用 Acquire 取得一次版本并在本次
publication 内固定使用。旧版本只有在所有进入该版本的 publisher、lane admission 和 EventLease 收敛后
才能退休，不能在 publisher 路径获取全局写锁。

### 10.3 路由规则

- 行情按`asset_no`路由给全部订阅Subscriber；
- 订单和成交优先按订单所有者路由；
- 账户事件按 `account_id` 路由；
- 风险事件按风险作用域路由；
- 连接事件路由给依赖对应数据源的Subscriber；
- 未找到订单所有者的订单事件进入异常处理和对账流程。

## 11. Async FastLane Primary 交付

### 11.1 物理结构与接口

多个 Publisher 可以直接向同一 consumer lane 投递，因此 Primary lane 使用有界 MPSC admission queue；
单个 AsyncLaneWorker 是唯一消费者：

```text
Publisher A ─┐
Publisher B ─┼──> bounded AsyncFastLane MPSC -> AsyncLaneWorker -> EventHandler
Publisher C ─┘
```

队列元素只保存固定布局 delivery header 和引用计数 EventHandle，不复制 payload。Plugin 注册：

```rust
pub trait EventHandler: Send + Sync + 'static {
    fn on_event(&self, event: &EventView<'_>) -> Result<(), HandlerError>;
    fn on_safe_point(&self, token: LaneControlToken) -> Result<(), HandlerError>;
}

pub struct AsyncLaneRegistration {
    pub subscription: SubscriptionSpec,
    pub handler: Arc<dyn EventHandler>,
    pub activation: ActivationGate,
}
```

AsyncLaneWorker 负责消费循环、EventLease、panic/error boundary、idle policy、提交水位和停止清理。Plugin
不得保存跨回调 EventView，也不再创建第二条私有消息队列或消费线程。

`AsyncLaneHandle::try_schedule_control(token)` 使用每 lane 预分配的有界 control slot，并主动唤醒 worker。
worker 在当前 handler 返回后、取得下一 EventLease 前调用 `on_safe_point`；control token 不占用 EventArena，
不改变事件 admitted/committed sequence，也不能携带动态 payload。pause、checkpoint、replace 和 stop 等
生命周期操作用 token 查询 Plugin 自己的有界 operation table，EventEngine 不解释业务控制语义。

EventEngine 不自动重试已经进入 handler 的事件，因为 handler 可能已经产生不可逆副作用。对于
`RELIABLE_ORDERED`，handler 返回错误时不推进 committed，并进入 `RESYNC_REQUIRED`；panic、ABI 违约
或明确 fatal error 进入 `FAILED`。`LATEST/BEST_EFFORT` 的 handler error 由 Plugin supervisor 按声明的
failure policy 决定 pause 或 fail，但同样不能推进 committed。

### 11.2 QoS 与容量保护

Async FastLane 是交付方式，QoS 决定 lane 满载时的语义：

| QoS | admission queue 满时 | 连续性 |
|---|---|---|
| `LATEST` | 原子替换同一 `(event_type, routing_key)` 的 latest slot | 允许跳过旧值 |
| `BEST_EFFORT` | 丢弃本次目标引用并计数 | 不保证连续 |
| `RELIABLE_ORDERED` | 转入该 lane 的有界 PendingDispatch | 不得静默丢失；失败后必须 resync |

每个 lane 按 SubscriptionSpec 预留关键容量：

```text
capacity = total_capacity
critical_reserve = reserved_slots

LATEST / BEST_EFFORT 只能使用 total_capacity - critical_reserve
RELIABLE_ORDERED 可以使用全部剩余容量
```

`critical_reserve` 不是独立队列，也不能替代 pending。lane 进入 `PENDING` 后，同一 ordering domain 的
后续 `RELIABLE_ORDERED` 事件必须跟随旧事件进入 pending，禁止绕过；LATEST/BEST_EFFORT 不得占用可靠
pending。

### 11.3 三段提交水位

每个 lane 使用 cache-line 隔离的预分配 slot 维护权威进度：

```rust
pub struct AsyncLaneProgress {
    pub subscription_generation: u64,
    pub admitted_sequence: u64,
    pub dispatched_sequence: u64,
    pub committed_sequence: u64,
    pub last_progress_at_ns: u64,
}
```

- `admitted_sequence`：事件目标引用已成功进入 admission queue 或可靠 pending 后推进；
- `dispatched_sequence`：worker 取得 EventLease、即将调用 handler 时推进；
- `committed_sequence`：handler 成功返回且 EventLease 已释放后推进；
- handler error、panic、进程退出或强制停止不能推进 committed；
- 三者必须满足 `committed <= dispatched <= admitted`，并且只能单调增加；
- health、checkpoint 和恢复只使用 committed，不能把“已入队”误认为“已处理”。
- admitted sequence 必须在 queue slot 或 pending entry 成功保留的线性化点分配；禁止先取号再竞争入队，
  否则多个 Publisher 会制造无法区分的空洞或乱序；
- lane admission gate 必须用有界原子状态协调 `NORMAL -> PENDING`，一旦 pending 位可见，同一 ordering
  domain 的后续可靠事件全部进入 pending。

这些 sequence 描述该 lane 的交付进度；Provider 的 market epoch/update sequence、account epoch/version
仍是业务事实坐标，两类坐标不能互相替代。

### 11.4 顺序与故障隔离

- 每个 ordering domain 必须映射到单一 source publication lane，并在一个 Async lane 内保持 FIFO；
- 同一订单的 Order、Fill、CommandResult 不得跨 priority class；
- 要求严格顺序的 lane 使用单 FIFO；priority/normal 双队列只允许承载互不要求相对顺序的 domain；
- EventEngine 不制造不同 source 之间的全局顺序；worker 按本地可见顺序串行调用 handler；
- 默认每个 Plugin/Strategy 实例独立 lane、worker、health slot 和 pending 配额；
- 共享 worker 只允许用于显式声明的冷路径 group，一个 handler 超时会影响该 group，因此不能声称实例级
  故障隔离。

实例级隔离只保证 queue、调度、health 和未开始 EventLease 相互独立。进程内 native handler 若永久
卡死，EventEngine 可以关闭该 lane admission、释放 queue/pending/staging 中尚未执行的引用并关闭业务
CommandGate，但不能安全强杀线程或回收正在执行的 EventLease；stop deadline 必须报告失败。内存破坏或
不可终止代码需要独立 worker 进程，不能把线程隔离描述成安全边界。

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
    -> Async lane admission queue达到高水位
LAGGING
    -> Market受限于critical_reserve边界并开始降级
    -> RELIABLE_ORDERED继续使用queue全部剩余容量
    -> RELIABLE_ORDERED写入queue失败
PENDING
    -> 当前及同一ordering domain后续可靠事件进入该lane的有界pending FIFO
    -> PendingDispatcher按时间和条数budget重试最老pending
    -> queue和pending均降到恢复低水位
    -> 发布SubscriberRecovered
NORMAL
```

只有未发生 delivery gap 时才能从 `PENDING` 回到 `NORMAL`。一旦 pending 满、超龄、handler 失败或
committed sequence 无法证明连续，必须进入 `RESYNC_REQUIRED -> RECOVERING`，不能因队列水位下降自行
恢复。进入 `LAGGING` 时发布 `SubscriberLagging/SubscriberBackpressure` 事实，业务 Plugin 据此关闭
命令 gate 或降低数据依赖。

### 12.4 有界PendingDispatch

每个包含 `RELIABLE_ORDERED` 订阅的 lane 拥有固定容量 pending FIFO，entry 来自启动阶段预分配的
`PendingDispatchPool`：

```rust
struct PendingDispatchEntry {
    subscriber_id: SubscriberId,
    subscription_generation: u64,
    ordering_domain: OrderingDomainId,
    event: EventHandle,
    admitted_sequence: u64,
    enqueued_at_ns: u64,
}
```

PendingDispatch必须满足：

- 同时配置`per_subscriber_capacity`和`global_capacity`，禁止运行期扩容；
- publisher 只做一次非阻塞 pending admission，不等待空位、重试或 worker；
- 每个 entry 持有一个明确的 EventBlock 目标引用；重试成功时把引用转移给 Async lane queue，取消或
  恢复重建时释放引用；
- 同一 lane/ordering domain 按 `admitted_sequence` 保持 FIFO，不能让新可靠事件绕过旧 pending；
- pending只接收必须重试的Critical，Market不得占用该容量；
- 重试由 EventEngine 的 PendingDispatcher 执行，并受 `max_items` 和 `max_elapsed_ns` 限制，不得阻塞
  publisher 或其他 lane worker；
- 多个Subscriber共享重试budget时使用跨轮次持久化游标进行round-robin，每个Subscriber每轮至多重试一个entry后让出机会；
- 记录pending深度、最老年龄、重试次数和占用的EventBlock数量；
- `global_capacity`若小于全部关键Subscriber保证配额之和，必须显式声明为共享池并定义配额，不能声称每个Subscriber都获得完整保证。

当pending达到容量上限或最老事件超过`max_age`时：

```text
PENDING
    -> 将SubscriberHealthTable状态原子更新为RESYNC_REQUIRED
    -> 原子关闭该lane Primary admission
    -> 记录未投递admitted/source sequence范围和原因
    -> 不阻塞publisher，不扩容pending
    -> 依赖该lane的Plugin关闭命令gate
    -> 后续可靠事件只扩展缺失范围，不再进入queue或pending
    -> 启动SnapshotBarrier恢复
    -> 清空pending并释放全部目标引用
    -> 在新subscription generation重建状态并重新激活lane
```

重新激活必须通过 §12.6 `SnapshotBarrier` 建立恢复截点，不能把旧 pending 与新快照任意混合。
EventEngine 只有在旧 generation 的 queue、pending、正在执行的 handler 和全部 EventLease 均已收敛，
且新 generation 已提交 barrier 后，才能从 `RECOVERING` 切回 `NORMAL`。

Subscriber投递入口使用“关闭位+活动Producer计数”的原子admission gate。失败端先原子关闭入口，等待已进入的Producer退出临界区，再清理Channel；`FAILED`和`STOPPED`是不可被`PENDING`、`RECOVERING`或`RESYNC_REQUIRED`覆盖的终态。

EventEngine的可靠性承诺是“关键事件不得静默丢失”，而不是在无限持续过载下保证每个Subscriber的内存内无损连续运行。所有缓冲有界且EventLoop不阻塞时，终局策略必须牺牲受影响Subscriber的连续可用性，并依靠权威状态恢复。

`SubscriberHealthTable`使用预分配Slot和Release/Acquire状态发布。可选的`FaultSignalRing`只用于通知Titan main、RiskPlugin和诊断线程；Ring满时更新丢弃指标，但不能覆盖HealthTable中的权威故障状态，也不能为了报告普通Arena耗尽而再次从普通Arena分配。

### 12.5 SubscriberHealth 权威状态

`SubscriberHealthTable` 是 lane 可用性的唯一权威来源；普通事件通知和 metrics 只是旁路观测：

```rust
pub enum SubscriberState {
    Starting,
    Normal,
    Lagging,
    Pending,
    ResyncRequired,
    Recovering,
    Failed,
    Stopping,
    Stopped,
}

pub struct SubscriberHealthSnapshot {
    pub subscriber_id: SubscriberId,
    pub subscription_generation: u64,
    pub state: SubscriberState,
    pub reason: SubscriberReason,
    pub progress: AsyncLaneProgress,
    pub missing_ranges: MissingRangeSet,
    pub active_barrier_id: Option<SnapshotBarrierId>,
    pub queue_depth: u32,
    pub pending_depth: u32,
    pub outstanding_leases: u32,
    pub changed_at_ns: u64,
}

pub struct DeliveryGap {
    pub stream: SourceStreamId,
    pub from_source_sequence: u64,
    pub to_source_sequence: u64,
}
```

`MissingRangeSet` 是按部署上限预分配的定长集合，不允许在故障路径扩容。超过可记录 stream 数量时将
reason 提升为 `GapSummaryOverflow` 并要求完整 barrier recovery，不能截断后声称精确恢复。

状态 slot 在启动时预分配，写端用 Release 发布完整快照，监督者、RiskPlugin 和 Plugin runtime 用
Acquire 读取。状态转换必须满足：

```text
STARTING -> NORMAL
NORMAL -> LAGGING -> PENDING -> NORMAL
NORMAL/LAGGING/PENDING -> RESYNC_REQUIRED -> RECOVERING -> NORMAL
任意非终态 -> FAILED
任意可运行态 -> STOPPING -> STOPPED
```

`FAILED/STOPPED` 是终态；旧 generation 不能覆盖新 generation 的 health。进入 `RESYNC_REQUIRED` 时
EventEngine 先更新 HealthTable 和关闭 admission，再尽力写 FaultSignalRing，不能依赖故障事件成功投递。
只有 SnapshotBarrier staging route 已原子安装且 `active_barrier_id` 可见后才能进入 `RECOVERING`；只有
consumer 确认所有 boundary 已应用、barrier replay tail 已推进 committed progress，并且 complete 校验
成功后才能回到 `NORMAL`。

### 12.6 SnapshotBarrier

SnapshotBarrier 是 Provider 权威快照与 EventEngine 增量投递之间的正式恢复契约：

```rust
pub struct SnapshotBarrierId(pub u64);

pub struct SnapshotBarrierRequest {
    pub barrier_id: SnapshotBarrierId,
    pub subscriber_id: SubscriberId,
    pub recovery_generation: u64,
    pub streams: Arc<[SourceStreamId]>,
    pub deadline_ns: u64,
}

pub struct StreamBoundary {
    pub stream: SourceStreamId,
    pub source_sequence: u64,
    pub provider_epoch: u64,
    pub provider_version: u64,
}

pub struct SnapshotBarrierCompleted {
    pub barrier_id: SnapshotBarrierId,
    pub boundaries: Arc<[StreamBoundary]>,
}

pub struct SnapshotBarrierCommit {
    pub barrier_id: SnapshotBarrierId,
    pub recovery_generation: u64,
    pub applied_boundaries: Arc<[StreamBoundary]>,
    pub committed_lane_sequence: u64,
}

pub trait SnapshotBarrierControl: Send + Sync {
    fn begin(
        &self,
        subscriber: SubscriberId,
        streams: Arc<[SourceStreamId]>,
        deadline: Instant,
    ) -> LocalResult<SnapshotBarrierRequest>;

    fn complete(&self, commit: SnapshotBarrierCommit)
        -> LocalResult<OperationId>;

    fn abort(&self, barrier: SnapshotBarrierId, reason: BarrierAbortReason)
        -> LocalResult<OperationId>;
}
```

协议流程：

```text
EventEngine.begin_snapshot_barrier(subscriber, streams)
    -> 在同一RouteTransaction安全点原子安装新generation staging route并关闭旧admission
    -> 等待切换前已进入旧admission的Publisher退出临界区
    -> 收敛或释放旧generation queue/pending/EventLease
    -> 返回SnapshotBarrierRequest

RecoveryCoordinator向各Provider请求带barrier_id的snapshot/reconcile
    -> Provider在同一SourcePublicationLane发布Snapshot facts
    -> Provider发布SnapshotBarrierCompleted
    -> Provider随后发布boundary之后的增量

EventEngine
    -> 在有界SnapshotStagingPool缓存新generation事件
    -> 保留带相同barrier_id的Snapshot facts
    -> 根据每个stream的source_sequence丢弃不新于boundary的普通增量
    -> 先交付Snapshot facts，再按stream顺序交付boundary之后的增量

Plugin handler构建candidate view并提交barrier
    -> SnapshotBarrierControl.complete(commit)
    -> EventEngine校验所有stream boundary和committed progress
    -> 原子切换candidate view与Primary lane generation
    -> SubscriberHealth = NORMAL
```

必须满足：

- Provider 负责交易所 Snapshot/Delta、epoch/version 和业务 sequence 的正确性；EventEngine 不解释交易所
  协议；
- `source_sequence` 是同一 SourcePublicationLane 的 EventEngine 投递坐标，completion marker 与其后的
  增量必须在同一 lane 串行发布；
- Market 的 `stream_epoch/update_sequence`、Account 的 `account_epoch/account_version` 进入
  `provider_epoch/provider_version`，用于消费者验证，不能被 EventEngine source sequence 替代；
- 多 source barrier 保存 boundary vector，不制造跨 source 全局顺序；
- SnapshotStagingPool、每 barrier 容量和 deadline 全部有界；满载、超时、Provider generation 改变、
  boundary 缺失或 candidate 提交失败时，本次恢复失败并保持 `RESYNC_REQUIRED`；
- SnapshotBarrier 不保证跨进程持久化；进程崩溃仍依赖 WAL、checkpoint 和 Provider reconciliation。

只返回裸 `OperationId`、但不提供 `barrier_id + StreamBoundary` 的 snapshot/reconcile 接口，不能宣称支持
无窗口恢复。

### 12.7 非关键订阅者

Metrics、UI、普通日志和统计消费者积压时：

- 允许降采样；
- 允许覆盖最新值；
- 允许丢弃；
- 不允许阻塞交易策略事件投递。

### 12.8 MPSC满载

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
- EventArena耗尽：以O(1)成本更新Pool和Subscriber权威Health状态，慢Subscriber Top-K诊断按§12.9异步或增量完成；
- 不允许AccountPlugin或其他Publisher悄悄丢弃关键事件。

关键入口满同样不能通过普通CriticalIngress递归发布背压事实。Publisher必须立即返回错误，同时更新预分配的RuntimeHealth状态；业务插件根据返回值和Health状态执行WAL保留、停止扩大风险或对账。若来源本身没有可重放的权威记录，则必须将该能力缺口作为部署前校验错误，而不能声称关键事件可恢复。

### 12.9 EventArena压力诊断

EventArena接近低水位或耗尽时，检测到压力的Publisher或EventLoop只执行固定成本动作：

```text
更新对应Pool的压力状态和计数
    -> 执行当前事件类型的确定性降级
    -> 更新SubscriberHealthTable或写入有界FaultSignalRing
    -> 继续EventLoop
```

每个 Subscriber 维护 `outstanding_handle_count`、保守的 `oldest_handle_timestamp`、`lane_depth`、
`pending_depth` 和 `last_progress_timestamp`，这些值在 enqueue、release 和消费进度更新时以 O(1) 成本
维护。任意顺序释放 Lease 时，热路径的 oldest 时间可以保守地保持更早值，直到未释放数归零后清除；
需要精确 Top-K 或精确最老 Lease 时由后台诊断完成，不能为维护精确最小值在热路径扫描全部 Lease。

“识别占用最多的慢Subscriber”属于诊断动作，不得成为Arena耗尽处理的前置条件。Top-K扫描由后台诊断线程读取上述计数完成；若部署不使用后台诊断线程，则EventLoop采用持久化游标增量扫描，每轮最多检查`pressure_scan_budget`个Subscriber，禁止在压力路径中一次O(N)遍历全部Subscriber。

## 13. 顺序、一致性与去重

### 13.1 顺序保证

EventEngine 保证：

- 同一 SourcePublicationLane 串行分配并检查 `source_sequence`；
- `local_sequence` 只表示 EventEngine 接受事实的本地次序，不声明交易所发生顺序；
- 同一 Async lane/ordering domain 按 `admitted_sequence` FIFO 投递；
- handler 成功和 EventLease 释放后才推进 `committed_sequence`；
- `RELIABLE_ORDERED` 事件不能绕过该 ordering domain 已存在的 pending。

EventEngine 不保证：

- 不同交易所 `exchange_ts` 的全局有序；
- 不同网络连接的真实发生顺序；
- 多个订阅者在同一时刻完成处理。

跨 Broker 策略按照本地可见顺序处理，不为了交易所时间戳排序而等待其他 Broker。

### 13.2 重复事件

Broker 重连、查询补偿和私有流可能产生重复业务事实。交易所订单和成交去重键通常包含：

```text
broker_id
account_id
exchange_order_id
exchange_trade_id
event_type
```

业务去重最终责任属于 Provider、OMS 和账户状态模块，EventEngine 不从 payload 推断重复 Fill 或订单
状态。EventEngine 只保证同一 `(subscriber_id, subscription_generation, admitted_sequence)` 不被
worker 正常提交两次；SnapshotBarrier 重放可能再次出现相同 `event_id`，Plugin handler 必须对规范事实
保持幂等。

### 13.3 发布不是多订阅者事务

向多个订阅者投递默认不是原子事务：

```text
Strategy投递成功
Risk投递成功
Metrics队列已满
```

关键 Primary lane 投递失败时，若有容量则进入有界 pending dispatch 并更新权威 health；pending 无法
接收或超龄时转入 `RESYNC_REQUIRED` 并记录缺失范围。非关键 lane 可以降级。EventEngine 不直接执行
策略暂停或风险处置，也不能为了保证任一 consumer 接收而阻塞 publisher。

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
    -> 创建EventArena、Ingress MPSC、PendingDispatchPool、SnapshotStagingPool、SubscriberHealthTable和FaultSignalRing
    -> 构造EventEngine但暂不接收业务事件
    -> 构造PluginEngine并注入EventEngine控制句柄
    -> PluginEngine通过EventControlApi暂存RouteTransaction
    -> 注册Plugin EventHandler并创建候选Async FastLane/Worker
    -> PluginEngine validate全部PluginBundle
    -> Titan main启动EventEngine线程
    -> EventEngine启动处于ActivationGate保护下的AsyncLaneWorker
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
    -> EventEngine关闭对应lane admission
    -> 唤醒并有界排空worker，等待当前Handler返回
    -> 释放未消费EventLease并冻结committed_sequence
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
- 删除订阅前关闭旧 Async lane admission；
- 等待旧 lane 已投递事件提交完成或显式释放；
- 不能释放仍被路由表引用的 AsyncLaneSender、仍在执行的 handler 或尚未归还的 EventLease。

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

  async_lanes:
    max_count: 64
    max_primary_fanout_per_event: 32
    default_capacity: 16384
    control_capacity: 64
    critical_reserve: 2048
    lagging_high_watermark_ratio: 0.80
    recovery_low_watermark_ratio: 0.50
    max_missing_ranges_per_lane: 64
    default_worker_mode: park
    spin_iterations: 10000
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

  snapshot_barriers:
    max_active: 16
    per_barrier_staging_capacity: 8192
    global_staging_capacity: 65536
    timeout_ms: 5000

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

`allocation: shared`表示`per_subscriber_capacity`是单Subscriber上限而非完整保证；启动时必须验证
`关键Primary lane数量 * guaranteed_per_critical_subscriber <= global_capacity`。`SubscriberHealthTable`
按 `async_lanes.max_count` 预分配，超过最大 lane 数量的 RouteTransaction 必须拒绝提交。

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
- `SmallEventPool`覆盖生产者未提交reservation、CriticalIngress、Async lane关键事件、pending和staging共同持有的峰值；
- `MarketBatchPool`覆盖Connector并发reservation、MarketIngress积压和Subscriber在途批次；
- `SnapshotPool`按同时恢复的数据流数量、每流最大在途快照数和重建期间保留代际计算；
- v1.4 各 Pool 之间不自动借用容量，避免 MarketBatch 或 Snapshot 压力侵占关键 SmallEvent 容量；
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

### 16.4 Async lane worker policy

每个 Primary lane 显式选择 worker policy：

- `Dedicated`：每 lane 独占线程并必须配置 CPU affinity，适合关键策略和风险消费者；
- `SpinSleep`：每 lane 独立线程，短暂自旋后 sleep，适合中低频但仍要求实例隔离的 Plugin；
- `Park`：producer 仅执行非阻塞 enqueue 后 `unpark`，适合低频控制和观测消费者；
- `SharedColdPath`：多个 lane 共享后台 worker，只允许 `BEST_EFFORT/LATEST` 冷路径，不允许关键策略、
  Account 或 Risk consumer 使用。

worker policy 只影响调度，不改变 QoS、pending、health、提交水位和 SnapshotBarrier 语义。

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
async_lane_depth{subscriber}
async_lane_admitted_sequence{subscriber}
async_lane_dispatched_sequence{subscriber}
async_lane_committed_sequence{subscriber}
async_lane_commit_lag{subscriber}
async_lane_worker_latency_ns{subscriber}
subscriber_pending_depth{subscriber}
subscriber_pending_oldest_age_ns{subscriber}
subscriber_pending_retry_total{subscriber,result}
subscriber_health_state{subscriber}

snapshot_barrier_active
snapshot_barrier_duration_ns{subscriber}
snapshot_barrier_staging_depth{subscriber}
snapshot_barrier_failed_total{subscriber,reason}

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
| Async lane高水位 | 标记Subscriber为LAGGING并发布事实 |
| RELIABLE_ORDERED无法写入Async lane | 转入有界pending并保持该ordering domain FIFO |
| PendingDispatch满或超龄 | 标记RESYNC_REQUIRED，记录缺失范围，停止Market投递并触发权威状态恢复 |
| handler error/panic | 不推进committed sequence，关闭lane admission并标记RESYNC_REQUIRED或FAILED |
| Snapshot staging满或barrier超时 | 中止恢复，释放staging lease并保持RESYNC_REQUIRED |
| EventArena接近低水位 | 执行当前Pool的确定性降级，后台或增量识别慢Subscriber |
| EventArena耗尽 | 更新权威Health状态，不在EventLoop同步O(N)扫描或递归分配告警事件 |
| 来源sequence跳跃 | 标记数据投影失效并发布SequenceGap |
| 未知订单事件 | 发布UnknownOrderEvent，由AccountPlugin处理 |
| EventEngine线程异常 | 向Titan main报告核心故障并退出EventLoop |
| AsyncLaneWorker异常 | 更新SubscriberHealth为FAILED并尽力发布故障通知，不执行交易业务处置 |

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
- lane control token 在当前 event handler 返回后、下一 EventLease 前执行，且不推进事件水位；
- control slot QueueFull 立即返回，不阻塞控制调用方；
- Primary Async lane 是 Plugin 唯一业务交付路径，不与同一 Subscriber 的 normal route 重复投递；
- Mirror lane 使用独立 SubscriberId，故障不影响 Primary；
- 同一Subscriber接收多个Publisher事件；
- 同一Publisher事件扇出到多个Subscriber；
- 关键容量预留生效；
- Market不能侵占`critical_reserve`；
- RELIABLE_ORDERED仅在 Async lane queue 写入失败后进入pending；
- 同一Subscriber存在pending时新Critical不能绕过旧事件；
- pending重试成功、取消和转入恢复时EventBlock引用正确转移或释放；
- pending满和超龄时进入RESYNC_REQUIRED并记录完整缺失sequence范围；
- admitted/dispatched/committed 三段水位单调且满足包含关系；
- handler error、panic 和强制停止不推进 committed sequence；
- SubscriberHealth 状态、reason、generation 和水位以 Release/Acquire 正确发布；
- SnapshotBarrier 在 route 切换前开始 staging，快照与增量之间无窗口；
- 多 source barrier 保存独立 boundary，不产生跨 source 全局顺序；
- staging 满、barrier 超时、旧 generation completion 和 boundary 缺失均保持 RESYNC_REQUIRED；
- Subscriber按 barrier boundary 完成权威状态重建后恢复，旧pending不能与新快照混合；
- AsyncLaneWorker 只调用 Plugin 的 opaque EventHandler，不解释 Fill 或选择业务 callback；
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
- 多个 Primary Async lane 同时达到峰值时 publisher enqueue 保持非阻塞；
- 单个 lane handler 永久阻塞时其他独立 worker 和 publisher 延迟不受影响；
- SnapshotStagingPool 达到每 barrier 和全局容量；
- Critical持续满载时Market、pending和Timer的最大service gap；
- 大扇出事件分步路由时其他类别仍获得时间片；
- 压力诊断后台扫描或增量扫描不造成EventLoop延迟尖峰；
- Connector断线、恢复和快照重建；
- Dedicated与SpinSleep模式的CPU和延迟对比。

### 19.3 并发模型测试

Ingress/Async lane MPSC、EventBlock引用计数、Pool回收、水位和生命周期竞争必须使用`loom`或等价并发模型工具覆盖，不能只依赖长时间压力测试：

- Producer完成Payload写入后以Release发布，Consumer以Acquire读取完整Payload；
- MPSC多个Producer竞争、队列满失败、容量回绕和sequence轮次切换不产生空洞；
- EventEngine建立目标引用发生在 Async lane queue 或 pending 发布之前；
- enqueue失败与目标引用撤销竞争不导致泄漏或提前归还；
- 最后两个Subscriber并发释放时恰好一个线程归还Pool Slot；
- pending重试成功、Subscriber注销和停止流程竞争时引用只转移或释放一次；
- Pool Slot归还、generation增加和重新分配之间的Release/Acquire关系成立；
- 陈旧generation的EventHandle不能读取已复用Payload；
- RouteTable版本切换、Subscriber注销和旧EventLease释放竞争安全；
- admitted/dispatched/committed 的 Release/Acquire 顺序不允许监督者观察到逆向水位；
- barrier staging route 安装、Provider completion、timeout 和 stop 并发时只完成或回滚一次；
- SubscriberHealthTable的Release状态更新能被Titan main和RiskPlugin的Acquire读取观察；
- FaultSignalRing满时不会覆盖权威Health状态或递归占用普通EventArena。

模型测试必须使用足够小的容量强制探索满队列、回绕、最后引用和停止边界，并在CI中固定运行。

### 19.4 端到端基准

关键时间点：

```text
t0 Market Connector收到网络消息
t1 Market Connector完成交易所语义处理、标准化并publish
t2 EventEngine完成Primary Async lane admission
t3 AsyncLaneWorker取得EventLease
t4 推进dispatched_sequence并进入Strategy handler
t5 策略回调开始
t6 OrderService接受订单
t7 Account Connector I/O开始发送
t8 Socket send完成
```

核心指标：

```text
t2 - t1  Publisher到Primary lane admission延迟
t4 - t2  Async lane排队与调度延迟
t5 - t0  行情到策略延迟
t7 - t5  策略到Account Connector发送线程延迟
t8 - t0  行情到Socket发送端到端延迟
```

实验室初始门槛与PluginEngine PerformanceEnvelope保持一致：

| 路径 | 初始参考目标 |
|---|---:|
| Publisher完成Primary Async lane admission | P99不高于5us |
| Dedicated AsyncLaneWorker取事件到Handler入口 | P99不高于2us |
| 热路径堆分配 | 0 |
| 关键事件静默丢失 | 0 |

基准必须记录硬件、NUMA、CPU affinity、编译参数、50%/80%/95%负载以及P50、P99、P99.9和最大值。正式目标在目标硬件完成原型测试后冻结为版本化PerformanceEnvelope。

### 19.5 Async FastLane 统一交付验收

v1.4 的 Async FastLane 是默认 Primary 交付方式，不再只是 normal route 的优化镜像：

```text
业务消费者：Publisher -> Primary Async FastLane -> isolated worker -> Plugin handler
审计消费者：Publisher -> Mirror Async FastLane  -> audit worker
```

Primary 和 Mirror 必须使用不同 SubscriberId、队列、watermark 与 health slot。一个 Plugin 不得同时通过
Primary 和 Mirror 处理同一业务事实。验收必须覆盖：

- `LATEST/RELIABLE_ORDERED/BEST_EFFORT` 在 Async lane 上执行各自语义，FastLane 不成为第四种 QoS；
- publisher enqueue 固定成本、非阻塞，不等待 worker、pending 或 SnapshotBarrier；
- 每实例独立 worker 时，一个 handler 阻塞、panic、QueueFull 或 resync 不影响其他实例；
- `RELIABLE_ORDERED` 先使用 queue，再进入有界 pending，只有 pending 满/超龄才产生明确 delivery gap；
- admitted/dispatched/committed 水位与 EventLease 生命周期一致；
- health 状态即使 FaultSignalRing 满也不会丢失；
- SnapshotBarrier 能从权威快照和每 source boundary 无窗口恢复；
- unregister/stop 关闭 admission、唤醒 worker、处理在途 handler、释放 queue/pending/staging lease 并
  join，随后 EventArena outstanding block 归零。

严禁使用每事件 `tokio::spawn`、无界 channel、publisher 等待 worker，或让多个关键 Plugin 默认共享
同一 worker。

#### Inline FastLane 边界

Inline FastLane 仅作为受限内部能力保留：

```text
Publisher -> fixed-cost internal projection -> Primary Async FastLane / ingress
```

它不能注册普通 Plugin handler、策略/Numba callback、Service 调用或任意用户代码。若无法在启动时证明
固定执行上界、无阻塞、无高竞争锁，则必须使用 Async FastLane。

#### v1.3 基准参考

2026-09-01 Binance Futures production stream 验证环境为 2 个逻辑 CPU、1 个物理核。v1.3 Inline 模式
Depth steady 三轮 p50 为 `18.219/19.669/18.211us`；原 Async mirror 模式的 publisher enqueue p50 为
`0.127-0.255us`、p99 约 `1.023us`，consumer 端 Depth steady p50 约 `29-31us`。

这些结果只证明旧 Async mirror enqueue 成本，不证明 v1.4 Primary 模式的 pending、watermark、health 和
SnapshotBarrier 已达标。v1.4 必须在 Connector 与 worker 位于不同物理核的目标机器重新冻结完整
PerformanceEnvelope。

## 20. 实施顺序

v1.3 的 Arena、Ingress、RouteTable、旧 SubscriberChannel 和 Async mirror 实现作为迁移基础。v1.4 必须
先完成可靠性机制，再允许任何关键 Plugin 切换到 Primary Async FastLane。

### 第一阶段：冻结可靠性交付契约

- 扩展 SubscriptionSpec：delivery role、QoS、capacity、critical reserve、pending 和 worker policy；
- 发布新的 Core Runtime Event API major/capability，并保留显式 v1.3 compatibility adapter；
- 实现有界 lane control slot 和 `on_safe_point`，供 Plugin 生命周期操作与事件 handler 串行；
- 实现每 lane `admitted/dispatched/committed` 三段水位；
- 实现带 generation、reason、missing range 和水位的 SubscriberHealthTable；
- 将 Async lane、pending、health 和 EventLease 生命周期纳入 loom 模型测试；
- 保持现有 normal route 为业务主路径，Async lane 暂时只以 Mirror 方式验证。

### 第二阶段：补齐 RELIABLE_ORDERED pending

- 将 PendingDispatchPool 接入 Async Primary lane；
- 保证同一 ordering domain 的新可靠事件不能绕过旧 pending；
- 完成 queue/pending 配额、重试 budget、max age 和公平性；
- pending 满、超龄和 handler failure 统一进入 `RESYNC_REQUIRED`；
- 验证 publisher 在 queue/pending/worker 全部过载时仍不等待 consumer。

### 第三阶段：实现 SnapshotBarrier

- [x] 实现 SnapshotBarrierRegistry、SnapshotStagingPool 和 recovery generation；
- 扩展 Market `request_snapshot` 与 Account `reconcile`，传递 barrier ID 并发布 StreamBoundary；
- [x] 完成 EventEngine 内部多 source boundary、staging 过滤、candidate commit、超时与失败回滚；
- 接入 Strategy、Risk、Account 等消费者的 CommandGate 和恢复状态机；
- [x] 完成 EventEngine 内部旧 generation 隔离、每 barrier/全局 staging 容量和 timeout 故障注入测试；
- 完成 Provider/consumer 接入后的跨组件无窗口验收。

### 第四阶段：逐 Plugin 切换 Primary Async FastLane

- 先迁移 Metrics/只读观测消费者；
- 再迁移 Market projection 和非交易关键消费者；
- 在 pending、health 和 SnapshotBarrier 验收后迁移 Account/Risk/Strategy 关键消费者；
- 每次迁移先运行独立 Mirror 对比，核对 event ID、顺序、最终状态和 committed watermark；
- Primary 切换后停止同一 Plugin 的旧 normal consumer，避免重复业务处理；
- 删除各 Plugin 私有消费线程和重复队列，只保留 EventHandler 与生命周期 gate。

### 第五阶段：性能冻结与旧路径退休

- 多 Broker、多 Primary lane 峰值压测和单 handler 卡死隔离测试；
- CPU affinity、物理核心隔离和 Dedicated/SpinSleep/Park 策略验证；
- Arena、Ingress、Async lane、pending 和 staging 容量调优；
- 冻结 publisher admission、worker dispatch、handler commit 的 P99/P99.9 PerformanceEnvelope；
- 所有关键 Plugin 完成恢复演练后，退休旧 SubscriberChannel 业务交付路径；
- 保留独立 Mirror/Audit lane，不保留同一 Subscriber 的双重业务消费。

## 21. 最终边界

EventEngine 的稳定职责为：

```text
接收事实
    -> 管理事件内存
    -> 编号与分类
    -> 查找订阅者
    -> 非阻塞投递到Primary Async FastLane
    -> 管理隔离worker与EventLease
    -> 执行QoS与RELIABLE_ORDERED pending
    -> 维护提交水位与SubscriberHealth
    -> 协调SnapshotBarrier
    -> 背压、降级与恢复代际
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
publisher/EventLoop上的业务回调
Provider内部Snapshot/Delta算法
交易所sequence解释
```

最终核心模型保持为：

```text
事实：Plugin -> EventEngine -> Primary Async FastLane -> isolated worker -> Plugin Handler
命令：Plugin -> Service -> Target Plugin
流程：StateMachine + Service + Event
```

这套模型在不引入StrategyShard、全局回调链和通用事务总线的前提下，覆盖多Publisher、多Subscriber和多插件的主要实盘事件场景，并为后续内部性能优化保留稳定接口。
