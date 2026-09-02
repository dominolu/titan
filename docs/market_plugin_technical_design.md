# Titan MarketPlugin 技术实现设计

版本：v0.4

状态：已实现并完成 Binance Futures 实盘验证

关联文档：

- [Core Runtime交互契约](core_runtime_contract.md)
- [EventEngine独立技术实现设计](event_engine_technical_design.md)
- [PluginEngine独立技术实现设计](plugin_engine_technical_design.md)

## 1. 设计目标

MarketPlugin 是完整 MarketConnector 实例的创建器、注册表和 Service 门面。

Connector 已经负责公共行情的全部实现，包括连接、订阅、重连、交易所协议、深度恢复、数据
标准化、状态维护和 EventEngine 发布。MarketPlugin 不重复实现这些能力。

目标数据路径：

```text
Exchange
    -> MarketConnector
    -> EventPublisher
    -> EventEngine
    -> Subscriber
```

MarketPlugin 不位于行情 payload 数据路径中。

## 2. 最终职责边界

### 2.1 MarketPlugin 负责

- 注册 ConnectorFactory；
- 根据 MarketSourceDefinition 创建 Connector 实例；
- 保存 SourceHandle 到 Connector 实例的映射；
- 启动、停止、替换和删除 Connector 实例；
- 将 PluginContext 提供的受限 EventPublisher 和 ChildResourceScope 传给 Connector；
- 通过 Service 暴露 Connector 的订阅、查询和管理接口；
- 汇总 Connector 自己提供的状态快照；
- 在插件停止时确保所有 Connector 已停止并释放。

### 2.2 Connector 负责

- REST、WebSocket、认证、代理、心跳和重连；
- 订阅、退订、订阅共享及交易所 ACK；
- Snapshot、Delta、sequence、checksum 和缺口恢复；
- symbol、价格、数量、时间戳和方向标准化；
- Instrument 数据和健康状态；
- L2、Trade、BBO、Ticker、MarkPrice、FundingRate 事件编码；
- 通过 EventPublisher 直接向 EventEngine 发布；
- 发布失败、QueueFull、过载和恢复处置；
- 自身线程、Task、Timer、队列和网络资源的停止与回收。

### 2.3 MarketPlugin 不负责

- MarketRuntime 或 per-instrument Runtime；
- BookBuilder、MarketView 或共享订单簿；
- 交易所 sequence/checksum 校验；
- Snapshot/Delta 衔接；
- 订阅需求聚合或重连策略；
- 行情标准化；
- 接收、复制或转发 L2 payload；
- 解释不同交易所的深度实现方式；
- 直接操作 EventEngine 路由表或 SubscriberChannel。

## 3. 架构

```text
PluginEngine
    -> MarketPlugin
        -> ConnectorFactoryRegistry
        -> ConnectorRegistry
            -> BinanceMarketConnector
            -> OkxMarketConnector
            -> HyperliquidMarketConnector
        -> MarketAdminService
        -> MarketService

Connector
    -> EventPublisher
    -> EventEngine
```

MarketPlugin 只有控制路径调用：

```text
create / get / start / stop / remove / list
subscribe / unsubscribe / request_snapshot
instruments / health / diagnostics
```

所有高频行情都从 Connector 直接进入 EventEngine。

## 4. 公共类型

```rust
#[repr(transparent)]
pub struct MarketSourceId(pub u32);

#[repr(transparent)]
pub struct AssetId(pub u32);

pub struct MarketSourceHandle {
    pub source_id: MarketSourceId,
    pub generation: u64,
}

pub struct MarketInstrumentBinding {
    pub native_symbol: Arc<str>,
    pub asset_id: AssetId,
    pub price_tick: f64,
    pub quantity_lot: f64,
}

pub struct MarketSourceDefinition {
    pub source_key: Arc<str>,
    pub connector_type: Arc<str>,
    pub connector_config: Arc<[u8]>,
    pub instruments: Arc<[MarketInstrumentBinding]>,
    pub enabled: bool,
    pub definition_version: u64,
}
```

`connector_config` 对 MarketPlugin 不透明，由具体 ConnectorFactory 校验和解析。V1 的 instrument
binding 在创建 Source 前确定，避免在行情热路径分配 AssetId。运行期动态发现 instrument 可以以后
扩展，不属于 V1 必需能力。

`price_tick` 和 `quantity_lot` 定义标准 ABI 整数单位，由 Connector 用于把交易所价格和数量转换为
`price_ticks`/`quantity_lots`；不得使用与 instrument 无关的固定小数倍率。

删除并重新创建同一 source_key 时必须增加 generation。旧 Handle 返回 `StaleHandle`，不能指向新
Connector 实例。

## 5. Connector 接口

### 5.1 Factory

```rust
pub trait MarketConnectorFactory: Send + Sync {
    fn connector_type(&self) -> &str;

    fn create(
        &self,
        definition: &MarketSourceDefinition,
        context: MarketConnectorContext,
    ) -> Result<Arc<dyn MarketConnector>, ConnectorError>;
}
```

Factory 完成具体配置校验。MarketPlugin 只检查 connector_type 是否注册、Source/AssetId 是否冲突和
插件级实例容量。

### 5.2 Connector

```rust
pub trait MarketConnector: Send + Sync {
    fn start(&self) -> Result<(), ConnectorError>;
    fn stop(&self, deadline: Instant) -> Result<(), ConnectorError>;

    fn subscribe(&self, request: MarketSubscribeRequest)
        -> LocalResult<MarketSubscription>;

    fn unsubscribe(&self, subscription: MarketSubscription)
        -> LocalResult<OperationId>;

    fn request_snapshot(&self, asset_id: AssetId)
        -> LocalResult<OperationId>;

    fn instruments(&self) -> Arc<[InstrumentSnapshot]>;
    fn health(&self) -> ConnectorHealthSnapshot;
    fn diagnostics(&self) -> ConnectorDiagnosticSnapshot;
    fn operation(&self, id: OperationId) -> ConnectorOperationSnapshot;
}
```

接口只定义公共操作，不规定 Connector 内部如何实现。subscribe/unsubscribe/request_snapshot 必须
立即返回本地结果或 OperationId，不能等待网络、交易所 ACK 或 Snapshot。start/stop 只由
MarketPlugin 控制 owner 调用；start 在任务成功启动后返回，stop 最多等待到 deadline。

`MarketSubscription` 的共享、引用计数和 Drop 行为由 Connector 实现。MarketPlugin 不再维护第二套
LeaseAggregator。

### 5.3 Connector Context

```rust
pub struct MarketConnectorContext {
    pub source: MarketSourceHandle,
    pub instruments: Arc<[MarketInstrumentBinding]>,
    pub market_source_stream: SourceStreamId,
    pub control_source_stream: SourceStreamId,
    pub event_publisher: EventPublisher,
    pub resources: ChildResourceScope,
}
```

EventPublisher 是 PluginContext 提供的受限发布能力：

- 只能发布 MarketPlugin Manifest 声明的标准事件；
- 不能注册 EventType、修改路由或枚举其他插件；
- Market 和 Critical 使用不同 SourceStreamId；
- Connector 不能获得原始 EventEngineHandle；
- Publisher 在插件未 ACTIVE 时拒绝发布；quiesce 期间保持有效直到 Connector.stop 返回，随后拒绝
  新发布并等待已有 PublishPermit 归还。

Connector 内部可以缓存预解析的 Event descriptor 和 AssetId，避免每批使用字符串查找。是否使用
复制式 publish 或 EventArena reserve/commit 是 EventPublisher/Core Runtime 的实现能力，不改变
MarketPlugin 与 Connector 的边界。

## 6. Service

### 6.1 MarketAdminService

```rust
pub trait MarketAdminService {
    fn create(&self, definition: MarketSourceDefinition)
        -> LocalResult<MarketSourceHandle>;

    fn start(&self, source: MarketSourceHandle)
        -> LocalResult<OperationId>;

    fn stop(&self, source: MarketSourceHandle, deadline: Instant)
        -> LocalResult<OperationId>;

    fn remove(&self, source: MarketSourceHandle)
        -> LocalResult<OperationId>;

    fn replace(
        &self,
        source: MarketSourceHandle,
        definition: MarketSourceDefinition,
    ) -> LocalResult<MarketSourceHandle>;

    fn list(&self) -> Arc<[MarketSourceSnapshot]>;

    fn operation(&self, id: OperationId) -> MarketOperationSnapshot;
}
```

MarketAdminService 只操作 ConnectorRegistry 和 Connector 生命周期。

### 6.2 MarketService

```rust
pub trait MarketService {
    fn resolve(&self, source_key: &str)
        -> Result<MarketSourceHandle, MarketError>;

    fn subscribe(
        &self,
        source: MarketSourceHandle,
        request: MarketSubscribeRequest,
    ) -> LocalResult<MarketSubscription>;

    fn unsubscribe(
        &self,
        source: MarketSourceHandle,
        subscription: MarketSubscription,
    ) -> LocalResult<OperationId>;

    fn request_snapshot(
        &self,
        source: MarketSourceHandle,
        asset_id: AssetId,
    ) -> LocalResult<OperationId>;

    fn instruments(&self, source: MarketSourceHandle)
        -> Result<Arc<[InstrumentSnapshot]>, MarketError>;

    fn health(&self, source: MarketSourceHandle)
        -> Result<ConnectorHealthSnapshot, MarketError>;

    fn operation(
        &self,
        source: MarketSourceHandle,
        id: OperationId,
    ) -> Result<ConnectorOperationSnapshot, MarketError>;
}
```

MarketService 查找 Connector 后直接委托同名方法，不改变参数、状态或结果。它不返回完整
`Arc<dyn MarketConnector>`，避免普通 Consumer 调用 `start/stop` 绕过 MarketAdminService。

Service Endpoint 只短暂读取 ConnectorRegistry 并进行一次接口调用。它不实现网络或行情逻辑，也
不持有 Connector 私有锁。

## 7. ConnectorRegistry

```rust
struct ConnectorEntry {
    handle: MarketSourceHandle,
    definition_version: u64,
    connector: Arc<dyn MarketConnector>,
    state: AtomicConnectorLifecycle,
}

struct ConnectorRegistry {
    state: RwLock<ConnectorRegistryState>,
}

struct ConnectorRegistryState {
    by_id: HashMap<MarketSourceId, Arc<ConnectorEntry>>,
    by_key: HashMap<Arc<str>, MarketSourceHandle>,
}
```

Registry 只保存实例和生命周期元数据，不保存订单簿、sequence、订阅引用或行情状态机。

Registry 不在行情热路径，V1 直接使用 `RwLock<HashMap>`。create/remove/replace 由 MarketPlugin 控制
owner 串行提交；没有基准证明确有必要时，不引入 RCU、ArcSwap 或自定义无锁 Registry。

## 8. 标准事件

MarketPlugin Manifest 授权 Connector 发布：

```text
titan.market.DepthBatch@1
titan.market.TradeBatch@1
titan.market.Bbo@1
titan.market.Ticker@1
titan.market.MarkPrice@1
titan.market.FundingRate@1
titan.market.StreamStateChanged@1
titan.market.StreamInvalidated@1
titan.market.InstrumentChanged@1
```

统一 L2 payload 至少包含：

```rust
pub struct MarketBatchHeaderV1 {
    pub asset_id: u32,
    pub kind: u16,
    pub flags: u16,
    pub item_count: u16,
    pub reserved: u16,
    pub stream_epoch: u64,
    pub first_update_sequence: u64,
    pub last_update_sequence: u64,
    pub exchange_ts: i64,
    pub receive_ts: i64,
}

pub struct DepthItemV1 {
    pub price_ticks: i64,
    pub quantity_lots: i64,
    pub side: u8,
    pub action: u8,
    pub reserved: [u8; 6],
}
```

价格和数量使用整数 tick/lot。交易所原始 sequence、checksum 和恢复过程属于 Connector 私有状态，
不进入 MarketPlugin Service。

具体 Connector 在产生标准化 feed batch 时同时附带 `stream_epoch`、Snapshot 边界以及 update
sequence。MarketPlugin 及发布桥接层只透传这些坐标，不读取 `Event.ival/order_id` 推断交易所序列，
也不执行通用 gap 检测。Binance Futures 在 REST Snapshot 与增量对齐后生成 epoch，OKX 在校验
`prevSeqId/seqId/checksum` 后生成坐标，Hyperliquid 则为每个完整 L2 image 建立新的 epoch。

EventEngine metadata 中的 `source_sequence` 不属于交易所协议序列，由受限 `MarketEventPublisher`
在每个 source publication lane 内串行分配，并且只在发布成功后提交。Connector 不维护或传入该值。

Connector 必须保证其公开事件语义正确：Snapshot 是完整替换边界；同一 epoch 的 Delta
update_sequence 连续；无法保证连续时发布 invalidation，并在恢复后发布新 epoch Snapshot。

## 9. 订阅与 Consumer 启动

```text
Strategy创建EventEngine asset_id路由
    -> MarketService.subscribe(source, request)
    -> MarketService直接委托Connector.subscribe(request)
    -> Connector向EventEngine发布Snapshot/Delta
    -> Strategy从Snapshot建立本地订单簿
```

路由必须先于 Connector subscribe 创建，避免首个 Snapshot 在 Subscriber 可见前发布。
该顺序由 Strategy/Consumer 调用方负责：调用方必须先完成 EventEngine route commit，再调用
`MarketService.subscribe`。MarketService 既不创建路由，也不额外查询或校验路由是否存在。

当订阅已经被其他 Consumer 共享时，Connector 的 `subscribe` 必须确保新 Consumer 最终能看到一个
完整 Snapshot。具体是重新发布内存 Snapshot、请求交易所 Snapshot 还是重新订阅，由 Connector
决定。

MarketPlugin 及通用兼容桥接层不缓存、合并、重编号或重放 Snapshot。Snapshot/Delta 状态、缓存和
恢复全部属于具体 Connector；`request_snapshot` 只委托 Connector 发起刷新。

Subscriber 进入 `RESYNC_REQUIRED` 后调用 Connector `request_snapshot(asset_id)`。MarketPlugin 不
生成或返回 Snapshot 内容。

## 10. 启动和停止

### 10.1 创建

```text
MarketAdminService.create
    -> 查找ConnectorFactory
    -> 校验Source/AssetId容量和冲突
    -> 分配SourceStreamId
    -> 创建ChildResourceScope和受限EventPublisher
    -> Factory.create(definition, context)
    -> 写入ConnectorRegistry
    -> 返回MarketSourceHandle
```

创建失败按相反顺序释放资源，不留下 Registry 条目或 publication lane。

### 10.2 启动

```text
MarketAdminService.start
    -> 返回OperationId
    -> MarketPlugin控制owner调用connector.start()
    -> Connector自己连接、订阅和更新状态
```

MarketPlugin 不等待网络连接成功。调用方通过 Connector `health()` 或 Operation 查询状态。
`Connector.start()` 的“成功”只表示内部任务和必要本地资源已成功启动，不包含 DNS、网络握手、
交易所登录、订阅 ACK 或 Snapshot 完成；它预期是 O(1) 的有界任务提交。因此控制 owner 可以对多个
Source 连续串行调用 start，不需要为此再设计并发启动调度器。

### 10.3 停止

```text
MarketPlugin.quiesce
    -> 拒绝create/start/subscribe入口
    -> 对每个Connector调用stop(deadline)
    -> Connector停止读取、恢复任务和新发布
    -> 等待Connector注册到ChildResourceScope的任务退出
    -> ActivationGate等待已有PublishPermit归还

MarketPlugin.stop
    -> 使所有SourceHandle失效
    -> 清空ConnectorRegistry
    -> 释放ChildResourceScope和publication lane
```

`Connector.stop()` 返回成功后不能再调用 EventPublisher。停止失败必须被记录并继续执行有界资源
回收，不能无限等待。

## 11. 配置

```yaml
market_plugin:
  max_sources: 16
  max_instruments: 4096

market_sources:
  - source_key: binance-futures-public
    connector_type: binance-futures
    enabled: true
    instruments:
      - native_symbol: btcusdt
        asset_id: 1001
        price_tick: 0.1
        quantity_lot: 0.001
    connector_config:
      stream_url: wss://fstream.binance.com/ws
      api_url: https://fapi.binance.com
```

MarketPlugin 只解析 source_key、connector_type、enabled、instrument binding 和插件容量。
connector_config 由 ConnectorFactory 解析。

公共 Market Connector 不得与 AccountPlugin 共享同一个包含私有账户状态的 Connector 实例。

## 12. 错误与状态

```rust
pub enum MarketErrorKind {
    FactoryNotFound,
    InvalidDefinition,
    CapacityExceeded,
    SourceNotFound,
    StaleHandle,
    AlreadyExists,
    QueueFull,
    ConnectorRejected,
    DeadlineExceeded,
    ResourceReleaseFailed,
}
```

交易所连接、sequence、checksum、Snapshot 和数据错误由 Connector 自己定义，并通过
ConnectorHealthSnapshot、DiagnosticSnapshot 或 Operation 结果暴露。MarketPlugin 不复制具体错误
枚举。

## 13. 性能要求

行情热路径必须是：

```text
Connector -> EventPublisher -> EventEngine
```

必须满足：

- MarketPlugin owner 不接收行情消息；
- 不经过 ConnectorRegistry 或 ServiceRegistry；
- 不为每个价位创建独立消息；
- 不使用无界队列；
- 不使用字符串 symbol 做 EventEngine 路由；
- EventPublisher QueueFull 直接返回 Connector；
- 多 Consumer 由 EventEngine 共享 EventBlock，不由 MarketPlugin 广播。

### 13.1 已落地的行情热路径

当前 Binance Futures 高频路径为：

```text
WebSocket frame
    -> Connector直接解析交易所消息
    -> Connector校验Snapshot/Delta、epoch和sequence
    -> reserve_market_batch
    -> 在EventArena内原位编码标准ABI
    -> EventEngine publish
```

实现约束如下：

- Connector 使用 direct publisher，正常行情不再经过 connector event MPSC 和 bridge task 唤醒；
- `reserve_market_batch` 直接在 EventArena block 中编码 header 和 items，不创建中间 payload `Vec`，
  也不执行一次完整 payload copy；
- Binance `depthUpdate` 直接反序列化为 Depth 结构，绕过通用 untagged/tagged enum 的两层缓冲；
- Depth price/quantity 从 WebSocket JSON 借用 `&str`，steady 路径不为每档创建 `String`；
- symbol 通过预计算 canonical mapping 查找，steady Depth 不 lowercase、clone 或替换 HashMap key；
- tick/lot 在 instrument 注册时预计算 decimal unit，交易所数字字符串直接转换为 scaled `i64`，
  不经过 `String -> f64 -> i64` 往返；
- Snapshot、sequence、epoch、checksum 和恢复仍完全由具体 Connector 处理，发布桥和 FastLane 不解释
  交易所业务语义。

### 13.2 FastLane 交付模式

EventEngine 提供两种显式 FastLane；正常 EventEngine route 在两种模式下都继续投递，作为兼容和
审计镜像：

```text
                              -> Inline FastLane handler
Connector -> EventEngine publish
                              -> normal EventEngine route -> Subscriber
```

`Inline` 在 publisher 线程同步执行严格有界的 handler，适合只更新内存状态的极低延迟策略。它消除
subscriber mailbox、线程唤醒和调度等待，但 handler 的锁竞争、I/O 或复杂计算会直接阻塞后续行情。

```text
Connector -> EventEngine publish -> retain arena lease -> bounded Async FastLane queue
                                                       -> dedicated worker -> handler
                         \-> normal EventEngine route -> audit Subscriber
```

`Async` 只在 publisher 上 retain EventArena handle 并写入有界 `ArrayQueue`，payload 不复制；一个
FastLane group 可以包含多个 EventType，并由一个 worker 处理，避免每类行情创建线程。支持：

- `Dedicated`、`SpinSleep` 和 `Park` worker 模式以及可选 CPU affinity；
- 短暂自旋后主动唤醒；
- priority/normal 两级队列；Depth/Trade 可以绕过已排队的 BBO/Funding；
- 每个优先级内部 FIFO；同一 asset 的有序事件必须配置到同一 lane 和同一优先级；
- handler panic/error 只停用对应 lane，不拒绝正常镜像发布；
- unregister 和 EventEngine stop 先停止新 admission、排空 gap 前事件、join worker，再检查
  EventArena outstanding blocks；
- 队列满时记录 `fast_lane_drop_total` 和 `SubscriberBackpressure`，停用对应 lane，禁止丢失一条后
  继续消费造成静默状态不一致；正常 EventEngine 镜像不受影响，调用方必须从权威 Snapshot 恢复后
  重新注册 lane。

当前配置入口：

```rust
pub struct AsyncFastLaneConfig {
    pub capacity: usize,                    // 每个priority class的有界容量
    pub priority_event_types: Vec<Arc<str>>,
    pub runtime_mode: SubscriberRuntimeMode,
    pub spin_iterations: usize,
    pub idle_sleep: Duration,
    pub cpu_affinity: Option<usize>,
}
```

当前实现有 priority/normal 两个 queue，因此 lane 的理论总上限是 `2 * capacity`；
`fast_lane_depth_max` 记录两个 queue 的合计深度。

Async FastLane 必须使用有界队列；禁止每事件 `tokio::spawn`、无界 channel 或 publisher 等待 worker
释放容量。

### 13.3 性能指标与实盘结果

测量起点是 tungstenite 返回 WebSocket frame 后、JSON decode 前记录的 `receive_ts`；终点是
FastLane handler 入口。Snapshot 是恢复冷路径，不计入 steady-state 指标。

2026-09-01，同步 Inline FastLane 在服务器三轮 Binance Futures production stream 的 p50：

| 事件 | Run 1 | Run 2 | Run 3 |
|---|---:|---:|---:|
| Depth Delta steady | 18.219us | 19.669us | 18.211us |
| Trade | 2.610us | 2.840us | 2.709us |
| BBO | 3.020us | 3.110us | 3.020us |

Depth 三轮平均 p50 为约 `18.7us`。三轮 `drops=0`、`resync=0`、`rejected=0`。

异步 FastLane 的重点指标是 publisher 隔离成本：

| 指标 | 实盘结果 |
|---|---:|
| Async enqueue p50 | 0.127-0.255us |
| Async enqueue p99 | 约1.023us |
| FastLane drop | 0 |
| publish rejected | 0 |
| resync | 0 |
| 最大观测合计队列深度 | 137（每个priority class容量16384） |

Async 测试 handler 故意执行时间戳计算、event type/payload 复制和 channel send。在该负载下 Depth
steady p50 约 `29-31us`，BBO p50 约 `12-13us`；Trade 和尾延迟会随 handler 成本与 BBO 突发产生
排队抖动，但 publisher enqueue 仍稳定在亚微秒级。它证明 Async 模式解决的是“复杂 callback 不阻塞
行情接收和发布”，不承诺复杂 callback 自身仍具有 Inline 模式的端到端延迟。

测试服务器为 2 个逻辑 CPU、1 个物理核。生产环境应把 Connector/publisher 与 Async FastLane
worker 放在不同物理核；同一物理核的 SMT sibling 结果不能作为最终生产 PerformanceEnvelope。

2026-09-01 后续三轮 20 秒主网复测修正了压测自身的干扰：FastLane handler 不再逐事件复制
payload 并唤醒 `std::mpsc` consumer；普通镜像 subscriber 使用主动唤醒；EventEngine 空闲时不再执行
10,000 次无效自旋。Depth 使用借用解析、常见档位 SmallVec、整数 decimal 快路径，并直接写入 arena
中的 ABI buffer。三轮 steady-state 结果如下（单位 `us`）：

| 事件 | p50（三轮） | p90（三轮） | p99（三轮） |
|---|---:|---:|---:|
| Depth Delta | 27.240 / 28.429 / 27.610 | 36.208 / 37.959 / 35.550 | 92.358 / 101.079 / 98.217 |
| Trade | 2.870 / 2.920 / 2.669 | 7.659 / 7.609 / 7.070 | 22.430 / 15.840 / 20.170 |
| BBO | 3.180 / 3.410 / 3.211 | 13.380 / 12.809 / 18.470 | 33.948 / 33.899 / 35.479 |

三类消息的 p50 均稳定低于 `30us`，Trade 的 p99 也低于 `30us`。BBO p99 和 Depth p90/p99
仍受突发队列、Depth 单帧档位数以及单物理核抢占影响，不能声明为 `<30us`。同期原始 Binance
Depth frame 抽样为平均 59 档、p90 93 档、p99 463 档；因此 Depth batch 数量相同并不代表每批
工作量相同。若验收门槛是三类 p99 全部 `<30us`，必须使用至少两个独立物理核并隔离 CPU，或改变
Depth ABI/交付语义；不能通过隐藏大 batch 或丢弃尾延迟样本达标。

## 14. 测试

### 14.1 MarketPlugin 单元测试

- Factory 注册和 connector_type 查找；
- create/get/resolve/list/remove；
- SourceHandle generation 和 stale handle；
- Source/AssetId 重复与容量限制；
- create/start/stop/replace 错误传播；
- Registry 并发读取和 create/remove 互斥；
- create 失败完整回滚。

### 14.2 PluginEngine + EventEngine 集成测试

- 0 Source 时 MarketPlugin 正常 RUNNING；
- Factory 创建的 Connector 获得受限真实 EventPublisher；
- FakeConnector 直接发布真实 L2 Batch 并按 AssetId 路由；
- MarketPlugin owner 未收到 L2 payload；
- 未授权事件和错误 SourceStreamId 被拒绝；
- Connector stop 与 EventPublisher publish 竞争无泄漏；
- ResourceScope 关闭后无 Connector、Task、Timer 或 EventBlock 泄漏；
- Inline FastLane 执行后正常镜像路由仍可收到同一事件；
- Async FastLane handler 阻塞时 publisher 仍可继续提交；
- Async FastLane 同优先级 FIFO、priority 绕过 normal backlog；
- Async FastLane QueueFull 可观测且停用 lane，不继续交付 gap 后事件；
- Async handler error/panic 被隔离，unregister/stop 排空并归还 arena lease。

### 14.3 Connector 合约测试

Binance Futures、OKX 和 Hyperliquid 分别负责验证自己的：

- 连接、订阅、退订和重连；
- Snapshot/Delta 或完整 L2 语义；
- sequence/checksum/缺口恢复；
- 整数 tick/lot 标准化；
- QueueFull 和 EventPublisher 失败处置；
- 新 Consumer 订阅和 request_snapshot；
- stop deadline 和资源释放。

这些测试属于 Connector，不属于 MarketPlugin 内部测试。

## 15. 实施状态与顺序

1. 定义最小 `MarketConnectorFactory`、`MarketConnector` 和标准 Market Event ABI。
2. 实现 ConnectorRegistry、MarketAdminService 和 MarketService。
3. 使用 FakeConnector 完成真实 PluginEngine/EventEngine 集成测试。
4. 为现有 Binance Futures Connector 接入 MarketConnector 接口。
5. 为现有 OKX Connector 接入 MarketConnector 接口。
6. 为现有 Hyperliquid Connector 接入 MarketConnector 接口。
7. 完成停止、压力和泄漏测试后替换旧入口。

上述 V1 工作已完成。随后完成的性能改进顺序为：direct connector publication、EventArena 原位 ABI
编码、Depth 借用式专用解析与整数 decimal scaling、Inline FastLane 基线验证、Async FastLane
隔离与优先级队列验证。Async FastLane 是 EventEngine 能力，不改变 MarketPlugin/Connector 职责边界。

三个 Connector 可以共享一个仅供 Connector 实现使用的内部 utility crate，例如重连退避、有界
Snapshot/Delta 缓存和通用 sequence gap 检测算法。该 crate 不属于 MarketPlugin 接口或标准事件
ABI；交易所专属规则仍由各 Connector 负责，MarketPlugin 不依赖该 utility crate。

## 16. 验收标准

- MarketPlugin 只包含 Factory、Registry、Service 和生命周期管理；
- Connector 拥有全部行情实现和订阅状态；
- Connector 拥有 Snapshot/Delta、epoch、update sequence、checksum/gap 检测及 QueueFull 恢复策略；
- L2 payload 不经过 MarketPlugin；
- Connector 使用受限 EventPublisher 直接发布 EventEngine；
- MarketPlugin 不包含 MarketRuntime、BookBuilder、MarketView、SnapshotService、LeaseAggregator、
  DesiredStateStore 或 ConnectorControlSink；
- 新增 Connector 只需注册 Factory，不修改 MarketPlugin；
- 三个现有 Connector 能通过相同最小接口工作；
- 启动、停止、替换和失败回滚不存在资源泄漏；
- Inline 和 Async FastLane 均保留正常 EventEngine 镜像路由；
- Async FastLane publisher enqueue 有界、不等待 handler，队列、drop、最大深度和 handler 延迟可观测；
- Async FastLane QueueFull 或 handler failure 不影响 Connector 继续发布到正常 EventEngine 路由。

## 17. 最终边界

```text
MarketPlugin = Connector Factory + Registry + Service Facade + Lifecycle

Connector = 完整公共行情实现 + EventEngine Publisher

EventEngine = 事件内存、路由、QoS和Subscriber交付

Strategy = 订阅事件、维护本地状态和交易决策
```

任何交易所协议或行情正确性逻辑进入 MarketPlugin，都视为职责越界。
