# Titan StrategyPlugin 技术实现设计

版本：v0.2

状态：设计基线，待 EventEngine v1.4 可靠交付契约实现并验收

> 当前实施范围（2026-09-03）：主链采用 Fresh-only profile。StrategyPlugin 只绑定 MarketService、
> AccountService 和 AccountExecutionService，命令直接进入 AccountExecutionService；不导出或依赖
> Risk、checkpoint/store、recovery、StreamBoundaryProvider。本文中这些能力的章节作为未来插件化扩展
> 的设计素材，不属于当前闭环的实现或验收条件。非 Fresh recovery 配置会被明确拒绝。

关联文档：

- [AccountPlugin 技术实现设计](account_plugin_technical_design.md)
- [MarketPlugin 技术实现设计](market_plugin_technical_design.md)
- [Core Runtime交互契约](core_runtime_contract.md)
- [EventEngine独立技术实现设计](event_engine_technical_design.md)
- [PluginEngine独立技术实现设计](plugin_engine_technical_design.md)
- [Bar/Tick 回测与实盘统一策略接口](bar_tick_numba_strategy.md)
- [Strategy ABI v8 migration](strategy_abi_v8_migration.md)

## 1. 设计目标

StrategyPlugin 是策略代码制品和 StrategyRuntime 实例的创建器、注册表、生命周期管理器及 Service
门面。它允许 Titan 在 PluginEngine 已经运行后动态加载、启动、暂停、替换和删除策略实例，而不重启
MarketPlugin、AccountPlugin、RiskPlugin 或 EventEngine。

目标路径：

```text
加载路径：
StrategyDefinition
    -> StrategyAdminService
    -> StrategyPackageLoader
    -> StrategyArtifact
    -> StrategyRuntimeFactory
    -> StrategyRuntime

事实路径：
MarketConnector / AccountConnector / RiskPlugin
    -> EventEngine
    -> per-strategy Primary Async FastLane
    -> isolated AsyncLaneWorker
    -> StrategyRuntime EventHandler
    -> Numba callback

命令路径：
Numba callback
    -> 预分配 CommandBuffer
    -> StrategyRuntime host bridge
    -> StrategyCommandGateway
    -> RiskService -> AccountExecutionService
    -> AccountConnector
```

StrategyPlugin 不进入逐条事件的 payload 转发路径。EventEngine v1.4 将 EventHandle 非阻塞写入策略
实例的 Primary Async FastLane，并在该实例的隔离 AsyncLaneWorker 上调用 StrategyRuntime 提供的 opaque
EventHandler。StrategyRuntime 不再创建消费者线程；策略代码仍只感知 ABI callback。

V1 目标：

- 一个 StrategyPlugin 管理多个相互隔离的 StrategyRuntime；
- 同一策略代码制品可以创建多个参数、账户和市场绑定不同的实例；
- 动态创建实例不重新编译 PluginPlan；
- 每个实例具有独立 generation、Primary Async lane、worker、SubscriberHealth、状态和故障域；
- 策略启动前等待所需 Market、Account 和 Risk 能力 READY；
- 策略回调串行执行，任何时刻只有一个线程写该实例状态；
- Numba 回调只接收一个稳定 `StrategyRuntimeContext*`，事件交付循环始终由 EventEngine 的 Rust worker
  拥有；
- 热路径只使用启动期预绑定的数字 ID、Service Handle、函数地址和预分配内存；
- pause、stop、replace 和失败处理不会产生两个可同时下单的 generation；
- 回测与实盘使用同一 Strategy ABI、回调语义和命令模型。

### 1.1 v0.2 变更摘要

- 采用 EventEngine v1.4 `PRIMARY + ASYNC_FAST_LANE` 作为策略唯一业务事件交付路径；
- QoS 仍仅为 `LATEST/RELIABLE_ORDERED/BEST_EFFORT`，FastLane 不再出现在 QoS 取值中；
- 禁止策略和 Numba callback 使用 Inline FastLane；
- StrategyRuntime 从主动 `EventReceiver::dispatch_next` 消费者改为 EventEngine worker 调用的 handler；
- READY、恢复和 replace 统一引用 EventEngine SnapshotBarrier，不在 StrategyRuntime 自建 staging、去重或
  watermark 算法；
- 补全 `StrategyAdminService -> CheckpointCoordinator -> Runtime/EventEngine/Provider/StoreService` 架构。

### 1.2 当前基础与前置缺口

当前仓库已经具备：

- PluginEngine 的稳定 ServiceHandle、ChildResourceScope、ActivationGate 和 `ScopedEventRouter`；
- EventEngine v1.3 的动态 RouteTransaction、EventLease 和 Async FastLane mirror 基础；
- MarketPlugin、AccountPlugin 的管理、查询和执行 Service；
- `titan-runtime` 的 callback dispatcher、回测 event loop、CallbackRegistry 和回调顺序；
- `titan-runtime-abi` v8 的单参数 `StrategyRuntimeContext`；
- `titan-python-host` 的 Numba 编译和进程内 keepalive；
- Bar、Tick、Hybrid、Timer、Funding、Order 和 Fill 的回测运行时。

实现 StrategyPlugin 前仍需补齐：

- `crates/titan-strategy-plugin` 公共契约和实现；
- EventEngine v1.4 Primary Async FastLane、可靠 pending、三段提交水位、SubscriberHealth 和
  SnapshotBarrier；
- StrategyRuntime opaque EventHandler 和共享 `StrategyEventAdapter`；
- 共享 `StrategyCommandGateway`；
- RiskService 的稳定 Service 契约，早期测试可先使用 FakeRiskService；
- 可选 StoreService 的 checkpoint 契约；
- Account Fill@2 的本次/累计成交双数量语义；
- Strategy ABI v9 的 Fill 双数量及多账户命令路由字段；
- 将当前仅供 `titan run-worker` 使用的 Python Host 重构为可注入 loader。

EventEngine v1.4 的 `RELIABLE_ORDERED pending`、admitted/dispatched/committed 水位、权威
SubscriberHealth 和 SnapshotBarrier 是 StrategyPlugin 进入实现前的阻塞依赖。四项能力未通过上游验收
前，StrategyPlugin 不得把 Async FastLane 作为账户、风险和策略关键事实的唯一交付路径。

## 2. 最终职责边界

### 2.1 StrategyPlugin 负责

- 注册 `StrategyPackageLoaderFactory` 和 `StrategyRuntimeFactory`；
- 校验并保存 `StrategyDefinition`；
- 解析策略代码制品引用，校验摘要、签名、ABI、参数 Schema 和能力声明；
- 创建、启动、暂停、恢复、停止、替换和删除 StrategyRuntime；
- 保存 `StrategyHandle -> StrategyRuntime` 映射；
- 通过 `ScopedEventRouter` 为每个实例创建受限动态订阅；
- 将 StrategyRuntime 注册为 EventEngine opaque EventHandler，为每个实例创建并持有 Primary Async lane
  handle、ChildResourceScope 和 StrategyActivationGate；
- 在创建阶段解析 MarketSource、Account、Asset、Currency 和 Risk Scope，并固化数字绑定；
- 向 Runtime 注入受限 StrategyEventAdapter、StrategyCommandGateway、Clock、Metrics 和只写 snapshot sink；
- 监督回调预算、SubscriberHealth、三段提交水位、心跳、故障和停止 deadline；
- 汇总 Runtime、EventEngine 和 checkpoint coordinator 提供的状态与诊断；
- 在插件停止时拒绝新管理操作，并有界停止全部 Runtime；
- 发布低频策略生命周期、故障和 checkpoint 事件。

### 2.2 StrategyRuntime 负责

- 持有已加载 StrategyArtifact 及其代码 keepalive；
- 实现 EventEngine 注册的 opaque `EventHandler`；
- 在该实例唯一 AsyncLaneWorker 上保证生命周期回调和业务回调串行、不可重入；
- 通过共享 `StrategyEventAdapter` 把规范事件机械映射为仅在本次回调内有效的 ABI 只读视图；
- 持有策略私有状态，并在回调安全点执行 pause、resume、stop 和状态冻结/恢复；
- 把回调生成的有界 `OrderCommandBuffer` 交给共享 `StrategyCommandGateway`；
- 响应 StrategyPlugin 根据权威 SubscriberHealth 下发的 `Lagging/ResyncRequired` 状态，关闭 CommandGate
  并使实例失效；
- pause 期间继续消费必要的账户和风险事实，但不调用决策回调，也不允许生成新命令；
- 执行 `on_start`、业务回调、`on_error` 和恰好一次 `on_stop`；
- 在 handler 返回前清空临时 ABI 视图；在 EventEngine 完成 lane drain/join 后释放代码 lease。

StrategyRuntime **不再实现第二套事件基础设施**：它不创建消费线程、Mailbox 或 EventReceiver，不决定
QoS，不检测 QueueFull，不维护交付水位，不重排事件，也不自行实现 trade ID 去重、source
sequence/version 缺口、Snapshot staging 或 checkpoint 持久化。

### 2.3 共享运行组件负责

- **EventEngine**：创建并拥有每实例 Primary Async lane 和隔离 worker，负责路由、EventLease、QoS、
  `RELIABLE_ORDERED pending`、admitted/dispatched/committed 水位、SubscriberHealth 和 SnapshotBarrier；
  只在 AsyncLaneWorker 调用 opaque handler，publisher/EventLoop 不调用策略代码；
- **Market/Account/Risk Provider**：只生成一次规范事件，负责各自来源域内的去重、快照、epoch、sequence
  和 reconciliation；
- **StrategyEventAdapter**：仅做数字 binding、单位和内存布局转换，不创造新事实、不推断 Fill、不重排或
  去重事件；
- **StrategyCommandGateway**：统一执行 capability、账户/品种 binding、owner namespace 和 CommandGate
  校验，并固定调用 `Risk -> AccountExecution`；
- **titan-runtime**：提供 ABI callback 调用器和 Timer 调度；Timer 到期后通过 lane-local control item 回到
  该实例 AsyncLaneWorker 串行处理，不从 Timer 线程直接调用策略；
- **CheckpointCoordinator + StoreService**：协调 snapshot 元数据和异步持久化；Runtime 只在安全点复制或
  恢复策略私有状态。

### 2.4 StrategyPackageLoader 负责

- 从受信任的 ArtifactStore 或本地只读目录解析代码制品；
- 验证内容摘要、可选签名、入口点和 package manifest；
- 按语言/运行形式编译或加载策略；
- 返回进程内不可序列化的 `StrategyArtifact`；
- 保持动态代码、CPython/Numba 对象或静态注册实现的生命周期；
- 将编译错误转换为结构化错误，且不泄漏源码中的 Secret；
- 缓存相同内容摘要与 Runtime ABI 的编译结果；
- 遵守有界并发和 deadline，不在 StrategyPlugin 控制 owner 上执行编译。

### 2.5 StrategyPlugin 不负责

- 交易所协议、行情重连、订单状态合流、Fill 去重或账户 reconciliation；
- 维护共享 MarketView、AccountView、OrderBook 或账户权威状态；
- 计算交易所手续费、保证金、清算价格或 realized PnL；
- 绕过 RiskPlugin 直接发送策略订单；
- 根据当前持仓猜测遗漏的 Fill；
- 在多个账户或市场流之间制造不存在的全局交易所顺序；
- 将 Python 对象、Rust trait object 或函数指针跨进程序列化；
- 允许策略代码枚举全局 Service、其他策略实例或其他账户；
- 在回调线程执行文件、网络、数据库、Python 解释器或阻塞日志调用；
- 将 Strategy Definition 当作 PluginSpec，或为每个策略实例创建 PluginSlot；
- 负责研究任务调度、参数搜索或批量回测编排。

MarketPlugin、AccountPlugin 和 RiskPlugin 提供规范事实；EventEngine 负责 Primary Async 交付与恢复；
StrategyRuntime 只实现单实例 handler、策略回调和私有状态；StrategyPlugin 管理这些 Runtime。

## 3. 架构

```text
PluginEngine
    -> StrategyPlugin (PluginSlot, DEDICATED control runtime)
        -> StrategyPackageLoaderRegistry
        -> StrategyRuntimeFactoryRegistry
        -> StrategyRegistry
            -> StrategyRuntime[strategy-A, generation-1]
            -> StrategyRuntime[strategy-B, generation-3]
        -> StrategyAdminService
        -> StrategyService

StrategyRuntime
    -> ChildResourceScope
    -> StrategyEventAdapter
    -> titan-runtime callback dispatcher / Timer
    -> StrategyArtifact keepalive
    -> StrategyCommandGateway

EventEngine
    -> per-strategy Primary Async FastLane
    -> isolated AsyncLaneWorker
    -> StrategyRuntime EventHandler
    -> QoS / reliable pending / progress watermarks / SubscriberHealth / SnapshotBarrier

StrategyCommandGateway
    -> RiskService
    -> AccountExecutionService

StrategyAdminService.checkpoint
    -> CheckpointCoordinator
        -> StrategyRuntime
            -> callback safe-point private-state snapshot
        -> EventEngine / Provider
            -> committed progress / StreamBoundary metadata
        -> StrategyCommandGateway
            -> owned-order / pending-command metadata
        -> StoreService
            -> persist / load
```

StrategyPlugin 本身只有控制路径调用：

```text
create / prepare / start / pause / resume / stop / replace / remove / list
resolve / state / health / diagnostics / checkpoint / operation
```

逐笔行情、Order、Fill 和风险事实从 EventEngine 的 Primary Async lane 直接进入对应 AsyncLaneWorker，并
调用 Runtime 的 handler；不经过 StrategyPlugin Registry 或管理 Service。

## 4. 公共类型

### 4.1 标识和 Handle

```rust
#[repr(transparent)]
pub struct StrategyId(pub u32);

pub struct StrategyHandle {
    pub strategy_id: StrategyId,
    pub generation: u64,
}

pub struct StrategyArtifactId {
    pub digest: [u8; 32],
}

pub struct StrategyOperationId(pub u64);
```

`StrategyId` 是进程内稳定路由标识。删除并重新创建相同 `strategy_key` 或 replace 时必须增加
generation。旧 Handle 返回 `StaleHandle`，不能命中新实例。

一个 StrategyId 同一时刻最多只有一个 generation 拥有 `CommandGate=OPEN`。候选 generation 可以加载
代码、建立路由并同步状态，但不能调用 Risk 或 AccountExecution Service。

### 4.2 StrategyDefinition

```rust
pub struct StrategyDefinition {
    pub strategy_key: Arc<str>,
    pub strategy_id: StrategyId,
    pub package: StrategyPackageRef,
    pub entrypoint: Arc<str>,
    pub parameters: Arc<[u8]>,
    pub parameter_schema_version: u32,
    pub markets: Arc<[StrategyMarketBinding]>,
    pub accounts: Arc<[StrategyAccountBinding]>,
    pub subscriptions: Arc<[StrategySubscriptionSpec]>,
    pub risk_scope: RiskScopeRef,
    pub runtime: StrategyRuntimeSpec,
    pub recovery: StrategyRecoveryPolicy,
    pub shutdown: StrategyShutdownPolicy,
    pub enabled: bool,
    pub definition_version: u64,
}

pub struct StrategyPackageRef {
    pub loader_type: Arc<str>,
    pub uri: Arc<str>,
    pub expected_digest: [u8; 32],
    pub signature_ref: Option<Arc<str>>,
}
```

`parameters` 是冷路径配置，由对应 package manifest 的 Schema 校验。进入 Runtime 前，字符串市场、账户
和品种绑定全部解析为连续数字索引；回调不得解析 URI、JSON、Symbol 或 account key。

`uri` 只允许命中 StrategyPlugin 配置授权的 ArtifactStore 或只读目录。生产环境必须固定
`expected_digest`，不能用可变 branch、tag 或未固定远端 URL 作为可复现代码身份。

### 4.3 市场与账户绑定

```rust
pub struct StrategyMarketBinding {
    pub local_market_no: u32,
    pub local_asset_no: u32,
    pub source_key: Arc<str>,
    pub asset_id: u32,
    pub data_mode: StrategyDataMode,
}

pub struct StrategyAccountBinding {
    pub local_account_no: u32,
    pub account_key: Arc<str>,
    pub tradable_assets: Arc<[StrategyTradableAsset]>,
}

pub struct StrategyTradableAsset {
    pub local_asset_no: u32,
    pub asset_id: u32,
}

pub enum StrategyDataMode {
    Tick,
    Bar { timeframe_ns: i64 },
    Hybrid { signal_timeframe_ns: i64 },
}
```

`local_asset_no` 是 Strategy ABI 数组下标，必须形成 `0..N` 的连续集合。同一个 local asset 必须唯一
映射到一个行情 binding；交易 binding 可以显式选择账户。禁止根据 symbol 字符串在热路径猜测路由。

V1 不允许同一个 local order 同时路由到多个账户。跨账户拆单必须由策略显式生成多个 command，且每个
command 带 `local_account_no`。

### 4.4 Runtime 配置

```rust
pub struct StrategyRuntimeSpec {
    pub async_lane_capacity: usize,
    pub critical_reserve: usize,
    pub reliable_pending_capacity: usize,
    pub worker_policy: AsyncLaneWorkerPolicy,
    pub command_capacity: usize,
    pub timer_capacity: usize,
    pub state_f64_capacity: usize,
    pub state_i64_capacity: usize,
    pub callback_budget: CallbackBudget,
    pub cpu_affinity: Option<usize>,
    pub startup_timeout: Duration,
    pub stop_timeout: Duration,
}

pub enum StrategyRecoveryPolicy {
    Fresh,
    RestoreLatestCheckpoint,
    RequireCheckpoint,
}

pub enum StrategyShutdownPolicy {
    LeaveOwnedOrders,
    CancelOwnedOrders,
}
```

StrategyPlugin v0.2 固定使用 `delivery_mode=ASYNC_FAST_LANE`、`delivery_role=PRIMARY`。Definition 不允许
选择 Inline FastLane；worker policy 只能在 PluginSpec 授权范围内选择 Dedicated、SpinSleep 或 Park。

V1 不提供隐式 `CancelAll` 或自动平仓策略。账户可能被多个策略共享，StrategyRuntime 只能操作其
`strategy_id + generation` 命名空间拥有的订单。自动平仓属于独立、明确授权的风险收缩流程。

## 5. Strategy package 与加载接口

### 5.1 Package Manifest

每个策略制品必须包含不可变 manifest：

```rust
pub struct StrategyPackageManifest {
    pub strategy_type: Arc<str>,
    pub package_version: semver::Version,
    pub runtime_abi: ApiVersion,
    pub parameter_schema: Arc<serde_json::Value>,
    pub state_schema_version: u32,
    pub callbacks: StrategyCallbackMask,
    pub capabilities: StrategyCapabilities,
    pub artifact_digest: [u8; 32],
}
```

能力至少区分：

```text
READ_TICK
READ_BAR
READ_DEPTH
READ_ACCOUNT
READ_RISK
SUBMIT_ORDER
CANCEL_ORDER
AMEND_ORDER
SCHEDULE_TIMER
CHECKPOINT_STATE
```

Definition 请求的订阅、命令和状态容量必须是 package manifest 能力的子集，同时也是 StrategyPlugin
PluginSpec 授权能力的子集。

### 5.2 Loader Factory

```rust
pub trait StrategyPackageLoaderFactory: Send + Sync {
    fn loader_type(&self) -> &str;

    fn create(
        &self,
        context: StrategyLoaderContext,
    ) -> Result<Arc<dyn StrategyPackageLoader>, StrategyError>;
}

pub trait StrategyPackageLoader: Send + Sync {
    fn inspect(
        &self,
        package: &StrategyPackageRef,
    ) -> Result<StrategyPackageManifest, StrategyError>;

    fn load(
        &self,
        request: StrategyLoadRequest,
        deadline: Instant,
    ) -> Result<StrategyArtifact, StrategyError>;
}
```

`inspect` 和 `load` 都是冷路径。调用必须经过有界 BlockingExecutor 或 ColdAsyncRuntime，不能占用
StrategyPlugin control owner、EventEngine publisher/EventLoop 或 AsyncLaneWorker。

### 5.3 StrategyArtifact

```rust
pub struct StrategyArtifact {
    pub id: StrategyArtifactId,
    pub manifest: StrategyPackageManifest,
    pub callbacks: CallbackRegistry,
    pub state: StrategyStateMemory,
    pub code_lease: StrategyCodeLease,
}
```

`StrategyArtifact` 是进程内资源，不能 Clone、Serialize 或跨进程传递。`code_lease` 必须活到最后一次
回调返回之后，防止函数地址所指代码或 Numba keepalive 被提前释放。

第一版加载形式：

```text
numba-python
    -> 同一进程调用 titan-python-host
    -> 校验 RuntimeAbiDescriptor
    -> 保留 LoadedNumbaStrategy keepalive
    -> CallbackRegistry::from_addresses()

rust-static
    -> 启动期注册 factory
    -> 构建相同 CallbackRegistry 或内部安全 adapter
```

现有 `titan-python-host` 声明只供 `titan run-worker` 使用。StrategyPlugin 接入前必须将其职责重构为可
注入的“进程内编译 Host”，或确保 StrategyPlugin 本身运行在 worker 进程。Numba 函数地址绝不能由
controller 编译后传给另一个进程。

不可信 Python、原生动态库和需要 `panic=abort` 的策略必须部署到独立进程。该模式需要额外的共享内存
事件/命令协议，不属于 V1；不能假装 ChildResourceScope 构成安全沙箱。

### 5.4 缓存

Artifact cache key：

```text
(artifact_digest, entrypoint, normalized_parameters_digest, runtime_abi_fingerprint, target_cpu)
```

缓存只共享不可变代码制品。可写 state、CommandBuffer、Timer 和 Runtime Context 永远按实例创建。
缓存淘汰前必须确认所有 `StrategyCodeLease` 已释放。

## 6. StrategyRuntime 接口

### 6.1 Factory

```rust
pub trait StrategyRuntimeFactory: Send + Sync {
    fn strategy_type(&self) -> &str;

    fn create(
        &self,
        definition: &StrategyDefinition,
        artifact: StrategyArtifact,
        context: StrategyRuntimeBuildContext,
    ) -> Result<Arc<dyn StrategyRuntime>, StrategyError>;
}
```

### 6.2 Runtime

```rust
pub trait StrategyRuntime: EventHandler + Send + Sync {
    fn prepare(&self) -> LocalResult<StrategyOperationId>;
    fn start(&self) -> LocalResult<StrategyOperationId>;
    fn pause(&self, reason: PauseReason) -> LocalResult<StrategyOperationId>;
    fn resume(&self) -> LocalResult<StrategyOperationId>;
    fn stop(&self, deadline: Instant) -> LocalResult<StrategyOperationId>;
    fn freeze_state(&self, request: StrategyStateSnapshotRequest)
        -> LocalResult<StrategyOperationId>;

    fn state(&self) -> StrategyRuntimeStateSnapshot;
    fn health(&self) -> StrategyRuntimeHealthSnapshot;
    fn diagnostics(&self) -> StrategyRuntimeDiagnosticSnapshot;
    fn operation(&self, id: StrategyOperationId) -> StrategyOperationSnapshot;
}
```

接口方法只允许完成本地校验和向 Async lane safe-point control slot 提交有界操作。编译、订阅提交、
checkpoint 写入、worker join 或等待账户 READY 都不能在调用方线程执行。QueueFull 立即返回。业务事件
入口只有 `EventHandler::on_event`，不能再暴露第二套 `EventReceiver` 消费入口。

`freeze_state` 只在 lane safe point 复制策略私有数组并写入 `StrategyStateSnapshotSink`；它不读取 Provider、
不查询 EventEngine 水位，也不访问 StoreService。`StrategyAdminService::checkpoint` 由
CheckpointCoordinator 负责组合这些结果。

### 6.3 Runtime Context

```rust
pub struct StrategyRuntimeBuildContext {
    pub strategy: StrategyHandle,
    pub artifact_id: StrategyArtifactId,
    pub markets: Arc<[ResolvedMarketBinding]>,
    pub accounts: Arc<[ResolvedAccountBinding]>,
    pub event_adapter: Arc<dyn StrategyEventAdapter>,
    pub command_gateway: Arc<dyn StrategyCommandGateway>,
    pub state_snapshot_sink: StrategyStateSnapshotSink,
    pub clock: ClockHandle,
    pub metrics: MetricsHandle,
    pub events: StrategyEventPublisher,
    pub resources: ChildResourceScope,
    pub activation: StrategyActivationGate,
    pub command_gate: StrategyCommandGate,
}
```

`ScopedEventRouter`、Market/Account/Risk/Execution 原始 Service 和 StoreService 不交给 Runtime。
StrategyPlugin 在创建 Runtime 之前完成 binding 和 readiness，并封装 `StrategyEventAdapter`、
`StrategyCommandGateway` 和只写的 `StrategyStateSnapshotSink`；创建 Runtime 后，将其作为 handler 注册
到 EventEngine Primary Async lane。Snapshot sink 只把安全点复制结果交回
CheckpointCoordinator，不向 Runtime 暴露 StoreService。

Gateway 和 adapter 只能使用 Definition 解析得到的账户、市场和 routing key，并必须拒绝访问未绑定的
账户/AssetId；策略不能保存或获得原始 PluginEngine、EventControlApi 或全局 ServiceRegistry。

### 6.4 Handler 与 callback safe point

EventEngine 的实例专属 AsyncLaneWorker 调用 `StrategyRuntime::on_event`：

```text
AsyncLaneWorker取得EventLease并推进dispatched_sequence
    -> StrategyRuntime::on_event(EventView)
    -> StrategyEventAdapter机械生成ABI view
    -> titan-runtime调用对应callback
    -> 校验并提交CommandBuffer
    -> 清空ABI pointer/length
    -> handler成功返回
    -> EventEngine释放EventLease并推进committed_sequence
```

EventEngine 不解释 Strategy ABI，也不选择 `on_tick/on_filled`；StrategyRuntime 不操作 lane queue、pending
或水位。pause、stop、replace 和 checkpoint 通过 EventEngine lane-local safe-point control 提交，与业务
handler 在同一个 worker 上串行执行。publisher/EventLoop 永远不调用策略 handler。

单个慢策略只积压自己的 Primary lane。EventEngine 可以关闭该 lane admission、释放未开始事件并关闭
CommandGate，但进程内永久卡死的 native callback 仍不能安全强杀；stop deadline 必须报告失败，不可信
策略需要独立 worker 进程。

## 7. Service

### 7.1 StrategyAdminService

```rust
pub trait StrategyAdminService: Send + Sync {
    fn create(&self, definition: StrategyDefinition)
        -> LocalResult<StrategyHandle>;
    fn prepare(&self, strategy: StrategyHandle)
        -> LocalResult<StrategyOperationId>;
    fn start(&self, strategy: StrategyHandle)
        -> LocalResult<StrategyOperationId>;
    fn pause(&self, strategy: StrategyHandle, reason: PauseReason)
        -> LocalResult<StrategyOperationId>;
    fn resume(&self, strategy: StrategyHandle)
        -> LocalResult<StrategyOperationId>;
    fn stop(&self, strategy: StrategyHandle, deadline: Instant)
        -> LocalResult<StrategyOperationId>;
    fn replace(&self, strategy: StrategyHandle, definition: StrategyDefinition)
        -> LocalResult<StrategyHandle>;
    fn remove(&self, strategy: StrategyHandle)
        -> LocalResult<StrategyOperationId>;
    fn checkpoint(&self, strategy: StrategyHandle)
        -> LocalResult<StrategyOperationId>;
    fn list(&self) -> Arc<[StrategyInstanceSnapshot]>;
    fn operation(&self, id: StrategyOperationId) -> StrategyOperationSnapshot;
}
```

Operation ID 由 StrategyPlugin 全局分配，不能使用每个 Runtime 从 1 开始的本地 ID。查询 operation 时
必须刷新 Runtime 的当前状态，不能只保存首次 Pending 快照。

### 7.2 StrategyService

```rust
pub trait StrategyService: Send + Sync {
    fn resolve(&self, strategy_key: &str) -> LocalResult<StrategyHandle>;
    fn state(&self, strategy: StrategyHandle)
        -> LocalResult<StrategyRuntimeStateSnapshot>;
    fn health(&self, strategy: StrategyHandle)
        -> LocalResult<StrategyRuntimeHealthSnapshot>;
    fn diagnostics(&self, strategy: StrategyHandle)
        -> LocalResult<StrategyRuntimeDiagnosticSnapshot>;
}
```

Service 只用于控制面查询，不能逐 tick 调用。连续策略状态通过事件和 Runtime 本地视图维护，不暴露
StrategyRuntime trait object、callback address 或可写 state 指针。

### 7.3 Manifest

StrategyPlugin 建议提供：

```text
titan.strategy.admin@1      COMMAND
titan.strategy.query@1      INLINE
```

必需依赖：

```text
titan.market.market@1
titan.account.query@1
titan.account.execution@1
titan.risk.check@1
```

可选依赖：

```text
titan.store.snapshot@1
titan.metrics.sink@1
```

StrategyPlugin 使用 `ExecutionModel::Dedicated` 管理控制命令和监督任务；每个 StrategyRuntime 的事件与
safe-point control 由 EventEngine 的实例专属 AsyncLaneWorker 执行，不复用 PluginEngine Control Thread。

## 8. StrategyRegistry

```rust
struct StrategyEntry {
    handle: StrategyHandle,
    definition_version: u64,
    artifact_id: StrategyArtifactId,
    runtime: Arc<dyn StrategyRuntime>,
    lane: AsyncLaneHandle,
    subscriber_health: SubscriberHealthView,
    state: AtomicStrategyLifecycle,
    command_gate: StrategyCommandGate,
}

struct StrategyRegistry {
    state: RwLock<StrategyRegistryState>,
}

struct StrategyRegistryState {
    by_id: HashMap<StrategyId, Arc<StrategyEntry>>,
    by_key: HashMap<Arc<str>, StrategyHandle>,
}
```

Registry 不保存策略业务 state、订单、MarketView、AccountView、EventLease、Async lane 内容或 callback
函数地址。管理操作由 StrategyPlugin control owner 串行提交；事件热路径永远不访问 Registry。

## 9. 动态事件订阅与 Async FastLane

### 9.1 订阅建立

创建实例时按 Definition 生成数字化订阅：

```text
StrategyPlugin.create()
    -> 创建候选 StrategyEntry 和 ChildResourceScope
    -> 解析 MarketSourceHandle / AccountHandle / RiskScope
    -> ScopedEventRouter.begin candidate routes
    -> 创建 StrategyRuntime，并作为 opaque EventHandler 注册
    -> 请求 EventEngine 创建 per-strategy PRIMARY Async FastLane/isolated worker
    -> 配置 QoS、capacity、critical reserve、reliable pending 和 worker policy
    -> SubscriptionToken 登记到 ChildResourceScope
    -> safe point 提交 RouteTable
    -> EventEngine打开LaneActivationGate，允许SnapshotBarrier facts进入handler
    -> Runtime 保持 PREPARED，StrategyActivationGate/CommandGate 均关闭
```

订阅只能选择 StrategyPlugin Manifest 预授权的事件类型、QoS、容量和 worker policy。每个策略实例必须
有独立、由 EventEngine 拥有的 Primary Async lane、worker、pending 配额和 SubscriberHealth slot。
Publisher 只执行固定成本非阻塞 admission；慢策略只影响自己的 lane。策略不能配置 Inline FastLane，也
不能同时注册 normal consumer 或 Mirror lane 处理同一业务事实。

### 9.2 QoS

建议基线：

| 事件 | QoS | EventEngine 过载处理 / Runtime 响应 |
|---|---|---|
| L2 delta / trade / BBO | 按策略声明 `LATEST` 或 `BEST_EFFORT` | 标记 market view stale，等待 SnapshotBarrier |
| Order / Fill / CommandResult | `RELIABLE_ORDERED` | 关闭 CommandGate，实例进入 INVALIDATED |
| Position / Balance / Account stream state | `RELIABLE_ORDERED` | 关闭 CommandGate，触发账户快照恢复 |
| Risk mode / reservation result | `RELIABLE_ORDERED` | 立即停止扩大风险 |
| Metrics / debug | `BEST_EFFORT` | 丢弃并计数 |

`ASYNC_FAST_LANE` 是固定 DeliveryMode，不是第四种 QoS。替换/丢弃、可靠 pending、QueueFull、三段水位
和 SubscriberHealth 全部由 EventEngine 实现。StrategyPlugin 观察权威 health：行情失效时停止使用旧
视图；账户关键事实失效时关闭 CommandGate 并启动 SnapshotBarrier，不能靠当前持仓恢复遗漏 Fill。

### 9.3 顺序语义

- 单个 SourceStreamId 内遵循 EventEngine 的 `source_sequence`；
- AccountId 路由内 Order、Fill 和 CommandResult 保持 connector 发布顺序；
- 不承诺不同市场、不同账户或不同 SourceStreamId 之间存在交易所全局顺序；
- EventEngine 的实例专属 worker 按 Async lane admitted 顺序串行调用 Runtime handler；
- 回测使用 scheduler 的确定性 EventKey；实盘不能伪造不存在的跨交易所时间顺序；
- Connector/Provider 负责来源域的 trade ID 去重、epoch/version 和 sequence 连续性；
- EventEngine 负责 subscriber delivery 连续性、三段水位和 `ResyncRequired`；
- Runtime 不再执行一套重复的 gap/去重算法；如需要框架只读状态，公共 adapter 在回调前按规范事件更新。

### 9.4 EventLease

AsyncLaneWorker 调用 handler 时持有 EventLease。共享 adapter 可以在一次回调内聚合多个 EventView，但
不得跨回调保存裸引用；进入 Numba 前只做预分配 ABI 布局转换。Runtime 在 handler 返回前清空
`StrategyRuntimeContext` 中的 pointer/length view，随后 EventEngine 释放 EventLease；只有 handler 成功
返回并释放 lease 后才能推进 committed sequence。

## 10. READY 与启动屏障

### 10.1 依赖条件

StrategyRuntime 只有同时满足以下条件才能 READY：

```text
artifact ABI + capabilities validated
dynamic routes committed
Primary Async lane/worker registered
Strategy ActivationGate and CommandGate remain closed
all Market bindings READY and initial snapshot available
all Account bindings READY with committed epoch/version
Risk scope READY and permits observation
checkpoint restored or recovery policy satisfied
EventEngine subscriber health is Normal and Provider streams are READY
```

READY 只表示可以安全调用 `on_start`，不表示 Risk 一定允许下单。Risk mode 可以在 RUNNING 中动态拒绝
命令。Strategy ActivationGate 只控制用户业务 callback；PREPARED/RECOVERING 期间 opaque handler 仍可
接收 SnapshotBarrier facts 并构建 candidate view，但不能产生命令。

### 10.2 EventEngine SnapshotBarrier

启动、SubscriberResyncRequired、checkpoint 恢复和 replace candidate 都复用 EventEngine v1.4 §12.6 的
`SnapshotBarrierControl`、`SnapshotBarrierId` 和 `StreamBoundary`，StrategyPlugin 不实现第二套
subscribe-before-snapshot 算法：

```text
StrategyPlugin请求EventEngine.begin(subscriber, streams, deadline)
    -> EventEngine原子安装recovery generation staging route
    -> RecoveryCoordinator向Market/Account Provider请求带barrier_id的snapshot/reconcile
    -> Provider发布Snapshot facts + SnapshotBarrierCompleted(boundaries)
    -> EventEngine保留snapshot facts，过滤不新于boundary的普通增量
    -> Primary AsyncLaneWorker调用Runtime handler构建candidate read-only view
    -> Runtime向RecoveryCoordinator确认已应用boundaries
    -> RecoveryCoordinator携带committed lane sequence调用SnapshotBarrierControl.complete
    -> EventEngine完成barrier并将SubscriberHealth切回Normal
    -> StrategyPlugin声明依赖READY
```

职责约束：

- Market Provider 使用 `stream_epoch/update_sequence`，Account Provider 使用
  `account_epoch/account_version` 表达业务边界；
- EventEngine 使用 `source_sequence` 完成 staging 和 replay，但不解释交易所 sequence；
- StrategyRuntime 不删除重复增量、不补洞、不比较 venue sequence；它只处理 EventEngine 为当前 recovery
  generation 选择的 Snapshot facts 和 replay tail；
- 多 source 使用 boundary vector，不制造全局顺序；
- staging 满、deadline、Provider generation 改变、boundary 缺失或 candidate commit 失败时保持
  `INVALIDATED/RESYNC_REQUIRED`，CommandGate 不得打开；
- Provider 只返回裸 OperationId、不能关联 barrier 和 StreamBoundary 时，该绑定不支持无窗口启动或恢复，
  必须 fail-fast。

### 10.3 启动

```text
DEFINED
    -> PREPARING
    -> WAITING_DEPENDENCIES
    -> READY
    -> 在AsyncLaneWorker safe point调用 on_start(s) 一次
    -> 打开 StrategyActivationGate
    -> 打开 CommandGate
    -> RUNNING
```

`on_start` 失败时 CommandGate 始终关闭；Runtime 依次调用 `on_error` 和恰好一次 `on_stop`，然后进入
FAILED。

## 11. 回调与命令语义

### 11.1 回调顺序

Strategy ABI 继续使用固定事件槽：

```text
Start / Order / Filled / Position / Funding / Bar / Tick / Timer / Error / Stop
```

对于一次账户成交，Connector 和 AccountPlugin 必须先形成唯一的规范化事实。Runtime 不重新解释成交
语义，其消费顺序为：

```text
EventEngine AsyncLaneWorker -> StrategyRuntime::on_event
    -> StrategyEventAdapter 填充 FillEvent ABI view
    -> on_filled(s)
    -> StrategyCommandGateway 校验 CommandBuffer
    -> RiskService -> AccountExecutionService
```

用户回调不能重入，同一实例任何时刻最多执行一个 callback。

### 11.2 Fill 双数量语义

规范化 Fill 同时携带：

```rust
pub struct FillEventV9 {
    // ... identity, price, fee and pnl fields
    pub last_fill_qty: f64,
    pub cumulative_filled_qty: f64,
}
```

- `last_fill_qty` 是本次增量，用于仓位、成交额、费用和 PnL 计算；
- `cumulative_filled_qty` 是本次成交完成后的订单累计量，用于订单一致性校验；
- 同一订单累计量不得回退；重复 trade ID 不得再次应用 `last_fill_qty`；
- StrategyRuntime 不负责从累计量推导 Fill，Connector 必须已经完成该工作；
- `OrderChanged.filled_quantity` 仍表示订单状态中的累计成交量。

当前 `titan-runtime-abi` v8 的 `FillEvent.qty` 只有一个数量字段。接入 Account Fill 双数量字段时必须发布
Strategy ABI v9 或新增带版本的尾部字段，不能在 v8 原偏移上静默改语义。

### 11.3 Command Buffer

Numba callback 只向预分配数组写 `OrderCommand`。callback 返回后 Rust Host 必须检查：

- `num_commands <= command_capacity`；
- local account/asset binding 有效；
- order ID 属于当前 strategy generation 的命名空间；
- price、qty、TIF、order type 和时间戳可表示；
- stop/pause/invalidated 状态下没有新 submit；
- 同一批命令不存在本地 ID 冲突；
- 所需 capability 已声明。

当前 Strategy ABI v8 的 `OrderCommand` 没有账户字段，只能依赖单账户隐式路由。支持 Definition 中的多
账户绑定时，Strategy ABI v9 必须增加 `local_account_no`；不能根据 asset 或当前持仓猜测账户。

命令由共享 gateway 采用固定调用链：

```text
StrategyRuntime
    -> StrategyCommandGateway
        -> RiskService.reserve/check
        -> AccountExecutionService.submit/amend/cancel
    -> 保存 receipt 和 TraceContext
```

任何环节 QueueFull 都立即失败并形成策略可见错误，gateway 和 Runtime 都不自动重试结果不确定的 submit。
最终交易所结果只通过 Account 事件返回。

## 12. Pause、Stop 与 Replace

### 12.1 Pause

```text
RUNNING
    -> 关闭 CommandGate
    -> 关闭 StrategyActivationGate
    -> PAUSING
    -> 等待当前 callback 返回
    -> 调用可选 on_pause host hook（不属于稳定用户 ABI）
    -> PAUSED
```

PAUSED 期间：

- 不调用 Tick/Bar/Timer 决策回调；
- Primary AsyncLaneWorker 继续把 Order、Fill、Position、Balance、Risk 和 StreamState 关键事实交给
  Runtime handler，由公共 adapter 维护恢复所需的框架只读状态；
- 行情可以按配置降级为 latest snapshot，但恢复前必须重新建立 Market view；
- 不自动撤单或平仓；如策略定义为 `CancelOwnedOrders`，由明确的 pause/stop operation 执行；
- Primary Async lane 始终有界，pause 不改变 EventEngine 的 QoS、pending、水位和过载规则。

resume 必须重新检查 Account/Market/Risk READY 和 EventEngine SubscriberHealth；若已进入
ResyncRequired，必须先完成 SnapshotBarrier，再在 lane safe point 执行恢复 hook、依次打开
StrategyActivationGate 和 CommandGate。

### 12.2 Stop

```text
关闭 CommandGate
    -> 关闭 StrategyActivationGate
    -> QUIESCING
    -> 等待当前 callback 返回
    -> 按 shutdown policy 处理 owned orders
    -> 继续消费收敛所需账户事实直到 deadline
    -> EventEngine 关闭 Primary lane admission
    -> EventEngine 有界排空/释放 queue、pending、staging EventLease
    -> 在lane safe point调用 on_stop(s) 恰好一次
    -> retire SubscriptionToken和路由
    -> join AsyncLaneWorker并冻结committed_sequence
    -> 释放 StrategyCodeLease 和 ChildResourceScope
    -> STOPPED
```

deadline 到达仍有活动订单或未决命令时，operation 返回明确失败并保留诊断；不得假装成功，也不得释放
仍可能执行回调的代码。

### 12.3 Replace

replace 使用新 generation，并采用 shadow candidate：

```text
加载并验证新 Artifact
    -> 创建 candidate ChildResourceScope，CommandGate=CLOSED
    -> 建立 candidate Primary Async lane，StrategyActivationGate/CommandGate=CLOSED
    -> 恢复/迁移 candidate state
    -> 通过 SnapshotBarrier 和公共 adapter 同步候选只读状态
    -> candidate 达到 READY
    -> 关闭 old CommandGate
    -> 关闭 old StrategyActivationGate
    -> 等待 old callback safe point
    -> old 执行 quiesce/checkpoint
    -> 原子切换 Registry generation
    -> 在candidate lane safe point调用 on_start/on_replace
    -> 打开 candidate StrategyActivationGate
    -> 打开 candidate CommandGate
    -> 停止 old lane/routes/runtime/code lease
```

candidate 在切换前可以观察事件，但不能产生外部命令或发布业务决策。候选失败时完整释放候选资源，旧
generation 保持不变。

状态迁移仅在以下条件全部满足时允许：

- package manifest 声明相同或可迁移的 `state_schema_version`；
- 迁移函数在冷路径、候选私有内存上执行；
- 迁移输入来自已完成 checkpoint，不读取运行中的可变裸指针；
- 迁移失败不会修改旧 generation；
- artifact digest、参数摘要和迁移结果进入审计记录。

V1 可以只支持 `Fresh` 和“相同 schema 原样恢复”；任意 Python 对象 pickle 不能作为稳定状态协议。

## 13. 状态机与错误模型

### 13.1 生命周期

```text
DEFINED
    -> PREPARING
    -> WAITING_DEPENDENCIES
    -> READY
    -> RUNNING
    -> PAUSING
    -> PAUSED
    -> QUIESCING
    -> STOPPING
    -> STOPPED

异常：INVALIDATED / FAILED
恢复：RECOVERING -> WAITING_DEPENDENCIES -> READY/RUNNING
```

StrategyPlugin 为 RUNNING 不代表所有 StrategyRuntime 都 RUNNING。单实例失败只更新插件健康摘要，不
使其他实例或整个 StrategyPlugin FAILED。

### 13.2 错误分类

```rust
pub enum StrategyErrorKind {
    InvalidDefinition,
    PackageNotFound,
    DigestMismatch,
    SignatureInvalid,
    ParameterInvalid,
    UnsupportedCapability,
    AbiMismatch,
    CompileFailed,
    LoadFailed,
    DependencyUnavailable,
    StaleHandle,
    InvalidState,
    RouteFailed,
    AsyncLaneQueueFull,
    SubscriberResyncRequired,
    CallbackFailed,
    CallbackTimeout,
    RiskRejected,
    ExecutionQueueFull,
    CheckpointFailed,
    StopTimeout,
    Internal,
}
```

错误必须携带 strategy ID、generation、operation ID、阶段和稳定 reason code。源码、参数中的 Secret、
完整路径、Python traceback 本文和账户凭据不得进入普通事件或 metrics；详细诊断只进入受限日志。

## 14. Checkpoint 与恢复

### 14.1 Checkpoint 内容

```text
strategy_id + generation
artifact_digest
entrypoint
normalized_parameters_digest
state_schema_version
state_f64[]
state_i64[]
owned_order identities
async lane subscription generation
committed lane sequence
per-source StreamBoundary[]
created_at
checksum
```

CheckpointCoordinator 通过 lane-local control 请求 Runtime 在 AsyncLaneWorker callback safe point 把策略
私有 state 复制到预分配 snapshot buffer，并从 EventEngine 读取 committed lane sequence、从 Provider
读取已确认 StreamBoundary、从 StrategyCommandGateway 读取 owned-order/pending-command 元数据；Runtime
不负责持久化。Coordinator 通过 StoreService 异步写入，在写入完成前不能把 operation 标记为
Succeeded。普通 checkpoint 不复制 EventLease、ABI view 或指针。

### 14.2 恢复顺序

```text
加载并验证相同 Artifact/ABI
    -> 加载 checkpoint
    -> 校验 digest、state schema 和 bindings
    -> 创建新recovery generation Primary Async lane
    -> Runtime在lane safe point恢复策略私有state，StrategyActivationGate/CommandGate保持关闭
    -> EventEngine.begin SnapshotBarrier(checkpoint StreamBoundary streams)
    -> Market/Account Provider发布权威snapshot/reconcile和新boundary
    -> EventEngine staging并重放boundary之后的规范事实
    -> Runtime handler构建candidate view
    -> RecoveryCoordinator调用SnapshotBarrierControl.complete
    -> 对 owned orders、position 和 risk reservation 对账
    -> 无法解释的差异进入 INVALIDATED
    -> 成功后 READY
```

恢复不能重放已经得到最终结果的 submit，也不能根据持仓差额合成 Fill。缺失成交必须由 AccountConnector
用 venue trade ID 补齐。

### 14.3 Store 不可用

- `Fresh` 可以不依赖 StoreService；
- `RestoreLatestCheckpoint` 在 Store 不可用时保持 WAITING_DEPENDENCIES；
- `RequireCheckpoint` 没有有效 checkpoint 时 fail-fast；
- 正在 RUNNING 的实例 checkpoint 失败不立即停止交易，但必须进入 degraded health，并依据配置达到连续
  失败阈值后关闭 CommandGate。

## 15. 标准事件与 ABI

StrategyPlugin Manifest 建议授权发布：

```text
titan.strategy.StateChanged@1
titan.strategy.HealthChanged@1
titan.strategy.CallbackFault@1
titan.strategy.CheckpointCompleted@1
titan.strategy.OperationCompleted@1
```

所有事件是低频控制事实，routing key 使用 StrategyId。StrategyRuntime 的普通交易决策不额外发布一份
“StrategyOrder”事件；Account `CommandResult/OrderChanged/Fill` 和 TraceContext 已经是权威事实。

公共 header：

```rust
pub struct StrategyEventHeaderV1 {
    pub strategy_id: u32,
    pub kind: u16,
    pub flags: u16,
    pub strategy_generation: u64,
    pub strategy_version: u64,
    pub occurred_at: i64,
    pub observed_at: i64,
    pub operation_id: u64,
}
```

事件 payload 使用固定 little-endian 编码，不暴露 Rust enum padding、字符串、Python 对象、函数地址或
指针。策略参数和 checkpoint 只发布摘要及引用，不发布完整内容。

## 16. 安全与权限

- StrategyPlugin 只能加载 PluginSpec 授权的 loader type 和 ArtifactStore；
- production package 必须固定 digest，可选要求签名和发布者 allowlist；
- package capability、Definition 请求和 PluginSpec 权限执行三层交集；
- StrategyRuntime 只能访问显式绑定的账户、市场、品种和 risk scope；
- Order ID/client ID 必须包含不可伪造的 strategy owner 映射；
- candidate、paused、invalidated 和 stopping generation 的 CommandGate 必须关闭；
- Numba callback address、state pointer 和 user_data 只在该实例 AsyncLaneWorker 及代码 lease 生命周期内
  有效；
- Python 编译发生在冷路径；回调热路径不得重新进入 CPython；
- 动态 native code 的 Panic 不得跨 ABI；SIGSEGV、死循环和内存破坏无法被进程内 ResourceScope 隔离；
- 不可信策略必须使用进程隔离，并配套有界 IPC、心跳和 kill deadline；
- 日志、错误和 metrics 不得包含源码全文、Secret、签名材料或账户凭据。

## 17. 性能与可靠性

### 17.1 热路径

- 每个实例一个由 EventEngine 管理的隔离 AsyncLaneWorker，作为 callback 单写者；
- 启动期预绑定 callback、Service endpoint、account/asset route 和 ABI 数组；
- callback 前后不进行 Registry 查找、JSON、字符串解析或 heap allocation；
- StrategyEventAdapter 的机械 ABI 转换使用预分配批次内存；
- CommandBuffer、Timer、state 和框架只读 view 均有固定容量；
- 不持有跨 callback EventLease；
- 不在 callback 中 await、文件 I/O、网络 I/O、编译或阻塞日志；
- 指标写入无阻塞 recorder；
- EventEngine SubscriberHealth 报告关键 lane 过载或 ResyncRequired 时立即关闭 CommandGate。

### 17.2 预算和隔离

每个 Runtime 独立记录：

```text
callback_duration_ns{kind}
callback_budget_violation_total
async_lane_depth / async_lane_high_watermark
async_lane_admitted_sequence
async_lane_dispatched_sequence
async_lane_committed_sequence
event_lag_ns{source}
command_buffer_depth
risk_reject_total
execution_queue_full_total
subscriber_resync_required_total
checkpoint_duration_ms
runtime_heartbeat_age_ms
```

soft budget 超限只记录；连续超限达到阈值可以 pause；stall threshold 超限由 Supervisor 关闭 CommandGate
并隔离实例。进程内线程无法安全终止正在执行的 native callback，因此硬卡死的最终处置是退出 worker
进程；这也是不可信策略需要进程隔离的原因。

### 17.3 容量

- 创建时配置 EventEngine Primary Async lane、critical reserve、reliable pending 和 worker policy，并预留
  命令、Timer、state 和 ABI view 容量；
- Definition 超过 PluginSpec 上限时拒绝创建；
- artifact cache、operation history、diagnostic recorder 和 checkpoint 队列必须有界；
- 每个实例独立过载，不能因为一个策略慢而停止其他策略；
- StrategyPlugin control queue 满时立即返回，不阻塞调用方。

## 18. 配置示例

PluginSpec 只加载 StrategyPlugin 能力：

```yaml
plugins:
  - instance_id: strategy-provider
    plugin: strategy.standard
    execution:
      model: dedicated
      cpu_affinity: 8
      callback_budget:
        soft_budget_us: 100
        stall_threshold_us: 5000
        max_consecutive_violations: 3
    config:
      max_strategy_runtimes: 128
      max_artifact_cache_entries: 64
      control_queue_capacity: 1024
      allowed_loader_types: [numba-python, rust-static]
      allowed_artifact_roots:
        - /opt/titan/strategies
```

StrategyDefinition 是运行期定义，不是 PluginSpec：

```yaml
strategies:
  - strategy_key: btc-grid-main
    strategy_id: 1001
    package:
      loader_type: numba-python
      uri: file:///opt/titan/strategies/grid-1.4.2
      expected_digest: "sha256:..."
    entrypoint: grid_strategy:build
    parameter_schema_version: 3
    parameters:
      lower_price: 55000
      upper_price: 75000
      levels: 40

    delivery:
      mode: async_fast_lane
      role: primary
      worker: dedicated
      cpu_affinity: 9
      capacity: 16384
      critical_reserve: 2048
      reliable_pending_capacity: 1024

    markets:
      - local_market_no: 0
        local_asset_no: 0
        source_key: binance-futures-market
        asset_id: 1
        data_mode: tick

    accounts:
      - local_account_no: 0
        account_key: binance-futures-main
        tradable_assets:
          - local_asset_no: 0
            asset_id: 1

    subscriptions:
      - event: titan.market.Trade@1
        routing_key: 1
        qos: latest
      - event: titan.account.OrderChanged@1
        routing_key: 10
        qos: reliable_ordered
      - event: titan.account.Fill@2
        routing_key: 10
        qos: reliable_ordered

    risk_scope: grid-main
    recovery: require_checkpoint
    shutdown: cancel_owned_orders
    enabled: true
    definition_version: 7
```

参数默认值必须由实现、基准测试和生产容量测试确定，本示例不构成生产推荐值。

## 19. 测试方案

### 19.1 StrategyPlugin 单元测试

- StrategyDefinition ID、binding、capacity 和 capability 校验；
- 重复 key/ID、旧 generation 和 definition version 冲突；
- Loader 注册、digest/signature/ABI/parameter schema 失败；
- create/start/pause/resume/stop/remove/replace 状态机；
- Operation ID 全局唯一及状态刷新；
- candidate replace 失败保留旧 generation；
- candidate 在切换前不能发命令；
- artifact cache lease 与淘汰；
- stop 后代码 keepalive 才释放；
- ResourceScope 逆序释放和重复停止幂等。

### 19.2 FakeRuntime 合约测试

- `on_start` 一次、`on_stop` 恰好一次；
- EventEngine AsyncLaneWorker 通过 opaque `EventHandler` 驱动 Runtime，策略代码不感知 lane 和调度；
- 单实例 callback 永不并发和重入；
- 公共 adapter 在 callback 前机械生成只读 ABI view，不改变规范事件语义；
- Order、Fill、Position、Risk 事件顺序；
- Fill 的 last/cumulative quantity 原样进入 ABI，Runtime 不推导增量或执行 trade ID 去重；
- callback command 交给 StrategyCommandGateway，并经过 Risk 后才到 AccountExecution；
- pause/invalidated/replace candidate 下 CommandGate 关闭；
- callback error 触发 on_error、on_stop 和 FAILED；
- callback/command/timer/state capacity 越界 fail-fast；
- handler 返回前裸 pointer view 被清空，成功返回并释放 EventLease 后 EventEngine 才推进 committed 水位。

### 19.3 PluginEngine + EventEngine 集成测试

- StrategyPlugin 可在零 StrategyRuntime 时 RUNNING；
- Primary Async lane 动态注册、RouteTransaction 提交、失败回滚和 retire；
- Strategy 不能注册 Inline FastLane，也不能同时通过 Primary/Mirror 处理同一业务事实；
- SnapshotBarrier staging route 在请求 Provider snapshot 前生效，快照与增量之间无窗口；
- 多 source StreamBoundary 不制造全局顺序；staging 满、超时和旧 generation completion 保持 INVALIDATED；
- Provider 报告 epoch/version/sequence 失效或 EventEngine 报告 ResyncRequired 时实例 INVALIDATED；
- EventEngine 的关键 Async lane/pending 过载状态关闭 CommandGate；
- admitted/dispatched/committed 水位单调，checkpoint 只使用 committed；
- 一个慢 Runtime 不影响其他 Primary lane、publisher 或 EventLoop；
- handler 只在对应 AsyncLaneWorker 执行，publisher/EventLoop 从不调用策略 handler；
- Service Endpoint replacement 时旧 Handle 返回 StaleHandle/Unavailable；
- 插件 quiesce 时 Runtime 按 deadline 有界停止；
- EventEngine 停止前全部 EventLease、Async lane、pending 和 staging 引用归零。

### 19.4 Numba 与 ABI 测试

- RuntimeAbiDescriptor layout/fingerprint 与 Python dtype 一致；
- ABI major/minor 不兼容在 callback 前拒绝；
- 32 个 callback slot 数量和地址校验；
- state 数组必须一维、C-contiguous 且 dtype 正确；
- callback pointer 只在 keepalive 生命周期内调用；
- Python 只参与编译，事件循环后不进入解释器；
- Strategy ABI v9 Fill 双数量字段迁移；
- 同一输入的回测与实盘投影 callback 序列一致。

### 19.5 故障注入和长稳

- Loader 超时、编译崩溃、ArtifactStore 不可用；
- Market/Account/Risk READY 抖动；
- route commit 失败、Async lane/pending/staging 过载、checkpoint 写失败；
- callback 连续超预算、返回错误和永久卡死；
- replace、pause、stop 与事件到达并发；
- 断线期间 Fill 补放和重复去重；
- 24 小时以上运行的内存、EventArena lease、线程和 artifact cache 稳定性；
- 小额实盘 submit/cancel/partial-fill/reconnect/recover 全链路验收。

## 20. 实施任务拆分

### Phase 0：EventEngine v1.4 阻塞依赖

1. 实现 Primary Async FastLane 和实例级 worker 隔离；
2. 实现 `RELIABLE_ORDERED pending` 与有界公平重试；
3. 实现 admitted/dispatched/committed 三段水位和权威 SubscriberHealth；
4. 实现 SnapshotBarrierControl、StreamBoundary、SnapshotStagingPool 和 recovery generation；
5. 实现 lane-local control slot/on_safe_point；
6. 完成 publisher 非阻塞、单 handler 卡死隔离、无窗口恢复和 EventLease 回收验收。

Phase 0 未完成前不得进入 StrategyPlugin 业务实现；可以冻结接口和编写 Fake，但不能把 v1.3 Async mirror
当作可靠 Primary 交付投入实盘。

### Phase 1：公共契约与骨架

1. 新建 `crates/titan-strategy-plugin`；
2. 定义 ID、Handle、Definition、Lifecycle、Error、Snapshot 和 Operation；
3. 定义 StrategyAdminService、StrategyService、Loader 和 Runtime trait；
4. 定义 Manifest、标准策略控制事件 ABI 和配置 Schema；
5. 使用 FakeLoader/FakeRuntime 固定公共契约。

### Phase 2：Registry 与生命周期

1. 实现 Factory Registry 和 StrategyRegistry；
2. 实现 create/prepare/start/pause/resume/stop/remove；
3. 实现 generation、全局 Operation ID 和状态快照；
4. 实现 ChildResourceScope、StrategyActivationGate 和 CommandGate；
5. 实现 shadow candidate replace 和失败回滚。

### Phase 3：动态路由与 READY

1. 在 EventEngine v1.4 四项可靠性能力通过验收后，接入 ScopedEventRouter 和 per-runtime Primary Async
   FastLane；
2. 实现数字 routing key、三种 QoS、容量、critical reserve、reliable pending 和 worker policy 校验；
3. 接入上游 SnapshotBarrierControl 和 RecoveryCoordinator，不在 StrategyPlugin 实现 staging；
4. 接入 MarketService、AccountService 和 RiskService readiness；
5. 接入 Provider readiness 与 EventEngine subscriber health，实现 INVALIDATED 和恢复状态机，不在
   Runtime 重复实现 gap 算法。

### Phase 4：Runtime 与 Strategy ABI

1. 让 StrategyRuntime 实现 EventEngine opaque EventHandler，并接入 lane-local callback safe point；
2. 实现共享 StrategyEventAdapter，机械转换 EventView 到 Tick/Bar/Order/Fill/Position/Funding ABI view；
3. 接入 `titan-runtime` callback dispatcher/Timer，实现预分配 CommandBuffer 和策略私有 state；
4. 实现共享 StrategyCommandGateway，固定执行 Risk -> AccountExecution 命令链；
5. 升级 Strategy ABI v9，支持 Fill 本次量和累计量；
6. 完成回测/实盘回调序列一致性测试。

### Phase 5：Numba loader 与 checkpoint

1. 重构 `titan-python-host` 为可注入、进程内 loader；
2. 实现 digest/manifest/schema/ABI 校验和有界编译；
3. 实现 artifact cache 和 StrategyCodeLease；
4. 接入 StoreService checkpoint 和恢复；
5. 实现 state schema 校验和相同 schema 恢复。

### Phase 6：生产可靠性

1. 完成 callback watchdog、SubscriberHealth、三段提交水位、flight recorder 和 metrics；
2. 完成 pause/stop owned-order policy；
3. 完成故障注入、压力、长稳和资源泄漏测试；
4. 完成小额实盘 shadow/reconnect/replace 验收；
5. 设计不可信策略的独立 worker 和共享内存协议；
6. 删除旧实盘策略启动入口和重复 Runtime 管理逻辑。

## 21. 验收标准

- EventEngine v1.4 的 reliable pending、三段水位、SubscriberHealth 和 SnapshotBarrier 已先通过上游验收；
- StrategyPlugin 固定使用 `PRIMARY ASYNC_FAST_LANE`，FastLane 不作为 QoS，Inline FastLane 被拒绝；
- StrategyPlugin 只包含 Loader/Runtime Factory、Registry、Service、路由装配、监督和生命周期；
- 每个 StrategyRuntime 具有独立私有状态和 ResourceScope；每个实例的 Primary Async lane、worker、
  pending、health 和路由由 EventEngine 拥有；
- EventEngine 只在实例专属 AsyncLaneWorker 调用 Runtime opaque handler，publisher/EventLoop 不执行策略
  callback，StrategyPlugin Registry 不在事件热路径；
- 策略启动前完成 artifact、route、Market、Account、Risk、checkpoint、Provider readiness 和 subscriber
  health 屏障；
- 同一 StrategyId 同时最多一个 generation 可以调用外部执行 Service；
- pause、invalidated、replace candidate 和 stopping 状态均拒绝新 submit；
- callback 串行、不可重入，共享 adapter 在回调前生成规范 ABI view；
- 策略订单固定交给 StrategyCommandGateway，并依次经过 RiskService 和 AccountExecutionService；
- Order/Fill/Position 等权威事实只由 AccountPlugin 规范化并经 EventEngine 投递；
- Fill 同时表达本次成交量和订单累计成交量，Connector/AccountPlugin 的 venue 去重保证重复成交不会二次
  应用；
- Numba pointer 与 keepalive 同进程同生命周期，ABI 不匹配在 callback 前失败；
- stop 在 deadline 内完成 lane admission 关闭、路由退休、EventLease 回收、worker join 和代码 lease
  释放；
- replace 候选失败不影响旧 generation，成功切换无双写命令窗口；
- checkpoint 可验证、可恢复且不包含裸指针或 Python 对象；
- Async lane/pending/staging 过载、ResyncRequired、callback failure 和 dependency loss 均有明确状态、
  指标和 SnapshotBarrier 恢复路径；
- 回测与实盘共享同一 Strategy ABI 和 callback 语义；
- 新增策略类型只需注册 Loader/Runtime Factory，不修改 StrategyPlugin 核心。

## 22. 最终边界

```text
StrategyPlugin
    = Strategy Package Loader
    + Runtime Factory
    + Registry
    + Dynamic Route Assembly
    + Service Facade
    + Lifecycle / Supervision

StrategyRuntime
    = Opaque EventHandler
    + Callback Safe-point / Lifecycle State
    + Strategy ABI Callback Driver
    + Strategy Private State / Artifact Keepalive

StrategyEventAdapter
    = Canonical Event -> Strategy ABI View（机械转换）

StrategyCommandGateway
    = Command Validation / Ownership
    + Risk -> AccountExecution

titan-runtime
    = Callback Invocation / Timer Scheduling

CheckpointCoordinator + StoreService
    = Snapshot Metadata / Persistence / Recovery Coordination

StrategyArtifact
    = Verified immutable code
    + Callback Registry
    + Code Keepalive Lease

MarketPlugin / AccountPlugin / RiskPlugin
    = Authoritative market, account, execution and risk capabilities

EventEngine
    = Primary Async FastLane / Isolated Worker
    + Event memory / routing / EventLease / QoS
    + Reliable Pending / Progress Watermarks / SubscriberHealth
    + SnapshotBarrier / Recovery Generation
```

任何交易所协议、共享账户权威状态、Fill 推导、风险审批、重复的 Mailbox/QoS/gap 实现或逐事件 Plugin
Registry 分发进入 StrategyRuntime/StrategyPlugin，都视为职责越界。
