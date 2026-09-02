# Titan PluginEngine 独立技术实现设计

版本：v1.6

状态：技术方案基线

适用范围：单进程、多线程、插件化实盘交易框架

关联文档：

- [Core Runtime交互契约](core_runtime_contract.md)
- [EventEngine 独立技术实现设计](event_engine_technical_design.md)
- [MarketPlugin 技术实现设计](market_plugin_technical_design.md)
- [AccountPlugin 技术实现设计](account_plugin_technical_design.md)

## 1. 文档目标

本文在 EventEngine 技术方案基础上，定义 Titan 的 PluginEngine，包括：

- 插件如何定义、注册、实例化和配置；
- 一个插件如何提供一个或多个 Service；
- 插件之间如何调用能力和发布事实；
- Service 如何在启动阶段完成依赖解析和预绑定；
- 插件线程、EventEngine 和后台执行器如何协作；
- 插件生命周期、资源回收和运行异常上报；
- 如何保证 PluginEngine 不进入逐笔行情和下单热路径；
- 如何在保持插件松耦合的同时避免动态调用开销。

本文不包含具体策略、Broker、风险规则、核算规则和存储实现。

## 2. 核心结论

PluginEngine 是 Titan Core 中的插件装配和生命周期内核，不是普通插件、业务事件引擎或通用任务调度器。

```text
Titan main
    └── Titan Core
          ├── EventEngine
          └── PluginEngine
                ├── PluginRegistry
                ├── ServiceRegistry
                └── RuntimeHost
                       │
                       ├── MarketPlugin
                       ├── AccountPlugin
                       ├── StrategyPlugin[]
                       ├── OrderPlugin
                       ├── RiskPlugin
                       └── BackgroundPlugin[]
```

`Titan main` 直接构造、配置、启动和停止 EventEngine 与 PluginEngine。二者是同级核心组件，不注册到 PluginRegistry，也不出现在普通插件 Profile 中。

插件之间只保留两种业务交互方式：

```text
操作和查询：Service Call
已经发生的事实：EventEngine.publish()
```

PluginEngine 负责：

- 接收配置适配层产生的标准化 `PluginSpec`；
- 校验 Manifest、配置和插件兼容性；
- 编译不可变 `PluginPlan`；
- 创建插件实例和私有 Context；
- 绑定 Service；
- 注册 EventEngine 订阅；
- 分配执行资源；
- 启动、停止插件并维护生命周期状态；
- 释放插件拥有的资源。

PluginEngine 不负责：

- 转发行情和订单事件；
- 执行策略回调；
- 逐笔查找 Service；
- 参与每次下单；
- 执行业务事务；
- 替代 EventEngine、OMS 或 RiskPlugin。

## 3. 与EventEngine的关系

### 3.1 Titan main负责核心启动

启动时：

```text
Titan main
    -> 加载Core配置
    -> 创建EventEngine
    -> 创建PluginEngine并注入EventEngine控制句柄
    -> ConfigurationAdapter生成PluginSpec[]
    -> compile_plugin_plan()生成PluginPlan
    -> PluginFactory创建PluginBundle
    -> RuntimeHost校验PluginBundle授权范围
    -> RuntimeHost创建PluginSlot和ResourceScope
    -> 暂存Service Endpoint和EventEngine路由
    -> validate全部PluginBundle
    -> 启动EventEngine
    -> 按依赖顺序start PluginBundle
    -> 按Core Runtime契约提交RouteTable、Endpoint generation和ActivationGate
    -> 所有必需插件进入RUNNING
```

PluginEngine只在插件装配事务中协调固定路由的注册和注销。运行期动态业务路由由预授权ScopedEventRouter直接提交给EventEngine。PluginEngine不拥有EventEngine的进程级生命周期，EventEngine的最终启动和停止由Titan main协调。

运行时通用关系：

```text
Publisher Plugin
    -> EventEngine.publish()
    -> SubscriberChannel
    -> Subscriber Plugin Runtime
```

实盘业务示例：

```text
MarketPlugin / AccountPlugin
    -> EventEngine.publish()
    -> StrategyRuntime EventChannel
    -> Strategy回调
```

逐笔事件经过 EventEngine，不经过 PluginEngine。

### 3.2 EventEngine负责事实传播

EventEngine 只负责：

- 接收事实；
- 管理事件内存；
- 编号和分类；
- 查找订阅者；
- 非阻塞投递；
- 背压和降级。

EventEngine 不管理插件依赖，也不创建或销毁插件。

### 3.3 运行期控制事件

PluginEngine 可以通过 EventEngine 发布低频控制事实：

```text
PluginStarted
PluginStopping
PluginStopped
PluginFailed
ConfigVersionChanged
ServiceAvailabilityChanged
```

PluginEngine 的启动、停止命令本身仍通过控制 Service 执行，不通过事件请求实现。

### 3.4 跨核心契约

PluginEngine不得自行定义RouteTable安全点、EventLease回收或EventControlApi兼容语义。这些内容以[Core Runtime交互契约](core_runtime_contract.md)为唯一依据。

PluginEngine启动时必须校验`core_runtime_api_version`。Service Endpoint和RouteTable属于不同核心组件，不存在跨组件CPU原子切换；文档中的“提交”统一表示ActivationGate保护下、可回滚的事务式时序。

以下变更必须同时升级公共契约和跨组件集成测试：

- RouteTransaction状态机；
- SubscriptionToken和EventLease所有权；
- Endpoint、RouteTable和ActivationGate的提交顺序；
- EventEngine与PluginEngine的启动停止顺序；
- TraceContext字段和传播规则。

## 4. 总体运行架构

以下业务插件仅用于展示PluginEngine与EventEngine的组合方式，不属于Titan Core：

```text
Titan main
    ├── PluginEngine Control Thread
    │     ├── PluginSpec / Lifecycle
    │     ├── ServiceRegistry
    │     └── 管理 Plugin Runtime
    │
    └── EventEngine Dedicated Thread


MarketPlugin ─────────── publish() ──> EventEngine
AccountPlugin ───────── publish() ──>     │
                                           v
                                  Subscriber EventChannel[]
                                           │
                                           v
                                   StrategyPlugin[]
                                           │
                                   OrderService.submit()
                                           │
                                           v
                                AccountExecutionService
                                           │
                                           v
                                Account Connector MPSC[]

BackgroundExecutor
    ├── MetricsPlugin
    ├── LoggingPlugin
    ├── StorePlugin
    └── ReportingPlugin
```

PluginEngine Control Thread 不持续轮询业务事件。没有生命周期或控制任务时，它可以阻塞等待，不需要占用专用 CPU。

## 5. 核心模块

PluginEngine内部只保留三个长期组件：

```text
PluginRegistry
ServiceRegistry
RuntimeHost
```

`PluginContext`和`ResourceScope`是实例级普通对象，`compile_plugin_plan()`是启动期纯函数，都不是长期组件。

```rust
pub struct PluginEngine {
    registry: PluginRegistry,
    services: ServiceRegistry,
    runtimes: RuntimeHost,
    event_control: EventEngineControl,
}
```

PluginEngine Control Thread是三个组件唯一的结构修改者。插件线程只持有预绑定Handle和私有Context，不反向访问这些组件。

### 5.1 PluginRegistry

PluginRegistry 保存可用插件类型：

```text
plugin_type
factory
manifest
package_version
source
```

职责：

- 注册内置插件；
- 注册可选动态插件；
- 根据 `plugin_type` 查找 PluginFactory；
- 阻止重复或不兼容插件类型；
- 校验框架 API 版本和插件 ABI 版本。

PluginRegistry只保存插件定义，不保存运行实例、线程、配置来源或生命周期状态。

### 5.2 ServiceRegistry

ServiceRegistry保存Service Provider及其可用状态：

```text
ServiceKey
    namespace
    name
    version
    scope
```

职责：

- 注册和注销Provider；
- 校验唯一Provider和Service版本；
- 按作用域解析Provider；
- 创建强类型ServiceHandle；
- Provider不可用时将相关EndpointSlot切换为UNAVAILABLE；
- 报告Service可用性变化。

核心只内置通用作用域：

```text
GLOBAL
PLUGIN_INSTANCE
CUSTOM(namespace, key)
```

`account`、`market`、`strategy`等业务作用域由插件使用`CUSTOM`定义，PluginEngine只比较命名空间和键，不解释业务含义。

### 5.3 RuntimeHost

RuntimeHost是插件运行状态的唯一权威来源。每个实例对应一个PluginSlot：

```text
PluginSlot
    identity
    descriptor
    bundle
    context
    lifecycle_state
    health
    resource_scope
```

RuntimeHost负责：

- 创建、保存和销毁插件实例；
- 串行维护插件生命周期状态；
- 按执行模型创建线程或后台任务；
- 应用线程名称和CPU affinity；
- 在装配提交后释放PluginSlot的ActivationGate；
- 将执行Handle、Service注册Token和EventEngine订阅Token登记到ResourceScope；
- 捕获线程退出、Panic及回调预算超限；
- 将运行异常报告给PluginEngine控制线程；
- 通过PluginSlot统一释放实例资源。

运行实例、线程状态和资源归属只保存在PluginSlot中，避免多个组件分别维护同一实例状态。

### 5.4 PluginContext

PluginContext是直接构造的实例能力视图：

```text
PluginContext
    Identity
    ConfigView
    BoundServices
    EventPublisher
    ScopedEventRouter?
    ResourceScopeHandle
    Clock
    Logger
    Metrics
```

插件只能访问Manifest中声明并成功注入的能力，不能读取PluginEngine内部注册表，也不能持有其他插件实例。Context在启动完成后只保留不可变配置快照和预绑定Handle。

PluginSlot唯一拥有ResourceScope并负责关闭；PluginContext只持有受限的ResourceScopeHandle，可登记资源但不能关闭或接管Scope。

### 5.5 ResourceScope

每个PluginSlot直接拥有一个ResourceScope：

```text
ResourceScope
    ThreadHandle[]
    EventSubscriptionToken[]
    ServiceRegistrationToken[]
    TimerHandle[]
    SocketHandle[]
    FileHandle[]
    ChildResourceScope[]
```

资源注册返回可撤销Token，并尽量采用RAII。停止或启动回滚时按逆序关闭ResourceScope。PluginEngine不理解Socket、Timer、Connector等业务资源。

### 5.6 compile_plugin_plan()

依赖解析是启动和配置切换阶段的纯函数：

```text
compile_plugin_plan(PluginSpec[], PluginRegistry)
    -> 校验Manifest、配置、API与ABI版本
    -> 查找必需和可选Service
    -> 校验Service版本和作用域
    -> 检测缺失依赖和同步调用环
    -> 生成实例创建顺序
    -> 生成Service绑定关系
    -> 生成事件权限和资源约束
    -> 生成启动和反向停止顺序
    -> 生成不可变PluginPlan
```

PluginPlan是一次装配版本的唯一编译结果，不参与逐笔事件和下单热路径。PluginFactory创建PluginBundle后，RuntimeHost还必须校验其ServiceExport和SubscriptionBinding没有超出PluginPlan与Manifest的授权范围。

### 5.7 配置输入边界

插件装配配置的文件解析、数据库读取、远程配置和Schema合并属于Titan应用层的ConfigurationAdapter，不属于PluginEngine：

```text
文件 / 数据库 / 配置服务 / 管理API
                 │
                 v
        ConfigurationAdapter
                 │
                 v
            PluginSpec[]
                 │
                 v
         PluginEngine.apply()
```

标准输入只包含：

```text
PluginSpec
    instance_id
    plugin_type
    config_snapshot
    enabled
    execution
        model
        cpu_affinity
        callback_budget
```

PluginEngine不关心配置来源。账户、行情源和策略实例仍属于各业务插件管理的Runtime Definitions，不进入PluginPlan，也不要求重新加载插件。

## 6. 插件定义

### 6.1 PluginManifest

Manifest描述插件类型的静态契约和能力上限：

```text
PluginManifest
    plugin_type
    name
    version
    engine_api_version
    abi_version
    config_schema

    provides[]
        service
        version
        scope_kind
        call_mode

    requires[]
        service
        version_range
        scope_selector_schema
        required

    publishes[]
        event_type
        schema_version

    subscribes[]
        event_type
        schema_version
        allowed_qos[]

    runtime_capabilities
        supported_execution_models[]
        reload_policy
```

Manifest不保存`cpu_affinity`、`callback_budget`、具体作用域键和`routing_keys`等实例级参数。这些参数属于PluginSpec或SubscriptionSpec。

插件可选择的执行模型只有`DEDICATED`、`BACKGROUND`和`PASSIVE`。Service的`INLINE`或`COMMAND`由每个ServiceExport的call_mode独立声明，不属于插件执行模型。

Manifest只用于注册、校验、依赖编译、权限和资源规划，不参与热路径动态查找。

### 6.2 PluginFactory与PluginBundle

PluginFactory创建一个完整的装配结果，而不是只返回生命周期对象：

```rust
pub trait PluginFactory: Send + Sync {
    fn manifest(&self) -> &'static PluginManifest;

    fn create(
        &self,
        init: PluginInit,
    ) -> Result<PluginBundle, PluginCreateError>;
}
```

```text
PluginInit
    identity
    config_view

PluginBundle
    lifecycle: Plugin
    service_exports[]
    subscription_bindings[]

ServiceExport
    service_key
    endpoint

SubscriptionBinding
    subscription_spec
    handler
```

Service Endpoint和Event Handler由插件实现创建，可以通过直接引用、共享状态或Command Channel访问插件内部状态，但不能向Consumer暴露插件具体类型。

PluginBundle只是创建阶段的普通返回对象，由PluginSlot持有，不是新的长期核心组件。

### 6.3 Plugin生命周期

```rust
pub trait Plugin: Send {
    fn validate(&self, ctx: &ValidationContext)
        -> Result<(), PluginError>;

    fn start(&mut self, ctx: &mut PluginContext)
        -> Result<(), PluginError>;

    fn quiesce(&mut self, reason: StopReason)
        -> Result<(), PluginError>;

    fn stop(&mut self)
        -> Result<(), PluginError>;
}
```

校验分为两层：

```text
compile_plugin_plan()
    校验Manifest、Schema、版本、依赖和执行域

Plugin.validate()
    校验实例配置、运行环境和已绑定依赖的元数据
```

`Plugin.validate()`发生在EventEngine启动以及Service和路由激活之前，不能调用尚未激活的Provider。`Plugin.start()`只完成资源就绪；插件自有Publisher和执行线程受RuntimeHost的ActivationGate约束，在PluginSlot原子进入RUNNING前不能发布业务事件或执行回调。

插件通过ResourceScopeHandle创建的执行任务应先处于SUSPENDED状态。RuntimeHost在Service Endpoint和订阅路由提交成功后释放ActivationGate。运行期业务能力通过Service和EventHandler提供，不继续扩展通用Plugin生命周期接口。

### 6.4 插件提供多个Service

一个插件可以提供一组生命周期、状态所有者、配置和故障域一致的Service。Consumer只注入所需接口，不获得整个插件实例，也不感知多个Service是否来自同一Provider。

同一插件提供多个Service必须同时满足：

- 生命周期一致；
- 状态所有者一致；
- 配置和故障域一致；
- 拆分只会增加无意义的跨模块跳转。

不满足这些条件时应拆为独立插件。实盘业务组合示例见附录A。

## 7. PluginContext

### 7.1 运行期能力视图

```rust
pub struct PluginContext {
    identity: PluginIdentity,
    config: ConfigView,
    services: BoundServices,
    events: EventPublisher,
    event_routes: Option<ScopedEventRouter>,
    resources: ResourceScopeHandle,
    clock: ClockHandle,
    logger: LoggerHandle,
    metrics: MetricsHandle,
}
```

ConfigSnapshot由PluginSlot唯一拥有，PluginInit和PluginContext只持有不可变ConfigView。ResourceScope同样由PluginSlot唯一拥有，Context只持有可登记资源、不能关闭Scope的ResourceScopeHandle。

Context不提供通用的运行期订阅构建器。插件级固定订阅由PluginBundle交付并在装配阶段注册。

### 7.2 BoundServices

BoundServices只用于启动阶段提取强类型ServiceHandle：

```rust
let orders = ctx.services.require::<OrderService>()?;
```

插件应将所需Handle保存到自己的Runtime状态。逐笔行情、订单命令和事件回调中不得反复访问BoundServices容器。

### 7.3 ScopedEventRouter

仅管理动态内部Runtime的插件才注入ScopedEventRouter，例如StrategyPlugin在运行期创建StrategyRuntime。它只能：

- 订阅Manifest授权的事件类型；
- 使用PluginSpec允许的QoS和容量；
- 提交已经数字化的路由键；
- 创建EventEngine新路由版本；
- 返回SubscriptionToken并登记到Child ResourceScope。

ScopedEventRouter直接使用EventEngine授予的受限控制能力，不重新编译PluginPlan，也不经过PluginEngine Control Thread。

### 7.4 不暴露PluginEngine

普通插件不能：

- 枚举全部PluginSlot；
- 直接启动或停止其他插件；
- 修改PluginRegistry或ServiceRegistry；
- 修改其他插件配置；
- 获取其他插件的具体类型；
- 绕过EventEngine直接调用订阅者。

PluginControlService只提供给显式授权的运维入口或管理插件，不作为普通业务插件的默认依赖。

### 7.5 只读View

高频共享状态通过Service返回只读View。View必须携带版本、来源序号和时效信息，不允许修改Provider权威状态；修改操作必须调用对应Service。

## 8. Service调用机制

### 8.1 调用语义

有明确目标和本地结果的操作或查询使用Service：

```text
TypedServiceHandle.operation(request)
    -> LocalResult
```

规则：

- 调用稳定接口，不调用Provider具体类型；
- 装配阶段完成Provider解析和强类型Handle绑定；
- 热路径不查ServiceRegistry；
- 返回本地明确结果，不通过事件模拟同步响应；
- 外部系统产生的ACK、Reject、Fill等事实通过EventEngine返回；
- 同步Service依赖图禁止形成环。

ServiceExport的call_mode至少包含：

```text
INLINE
    在调用者线程执行有界Provider方法

COMMAND
    非阻塞提交到Provider的有界Command Channel

ASYNC
    仅用于允许等待的冷路径长流程
```

### 8.2 同线程Service

只有ServiceExport声明`INLINE`且Consumer与Provider属于兼容的execution domain时，`compile_plugin_plan()`才能生成直接调用Handle：

```text
Consumer
    -> BoundServiceHandle
    -> Provider Endpoint
```

要求：

- 调用有界；
- 不阻塞网络和磁盘；
- 不等待其他线程；
- 不在返回前反向调用Consumer；
- 不持有跨回调锁。

执行域不同或无法静态证明相同时，必须使用跨线程Service。

### 8.3 跨线程Service

跨线程Service对Consumer仍表现为普通接口，Endpoint内部使用有界Command Channel：

```text
Consumer
    -> BoundServiceHandle
    -> CommandChannel.try_send()
    -> Provider Runtime
```

同步非阻塞调用只等待本地校验、必要的本地状态登记和命令队列接收，必须明确返回`Accepted`、`QueueFull`或`ServiceUnavailable`等本地结果，不能等待外部系统ACK。

实盘下单链路示例：

```text
Strategy
    -> OrderService.submit()
    -> AccountExecutionService.try_submit(account_route)
    -> Account Connector Order MPSC
    -> Connector I/O Thread
```

字符串`account_id`、`MarketSymbol`等标识在对应业务Runtime创建或绑定阶段解析一次，生成数字RouteHandle。逐笔和下单热路径只使用RouteHandle，不进行字符串或HashMap查找。

### 8.4 协程Service

协程接口只用于非热路径或明确的长流程，例如初始化查询、账户快照同步、运维命令和低频配置工作流。延迟敏感的策略回调下单接口必须保持同步非阻塞。

### 8.5 ServiceHandle可用性与版本

ServiceHandle是稳定的预绑定句柄，内部持有版本化EndpointSlot：

```text
EndpointSlot
    availability
    generation
    endpoint
    activation_gate
```

Provider进入QUIESCING或FAILED后，ServiceRegistry在控制路径将EndpointSlot切换为UNAVAILABLE，新调用立即返回`ServiceUnavailable`，不能等待Provider恢复。

新Endpoint generation可以在RouteTable提交前发布，但必须引用同一PluginSlot的关闭ActivationGate。ServiceHandle读取到GATED Endpoint时返回`RuntimeNotActive`；只有RuntimeHost对该Gate执行Release写入ACTIVE后，Service、Publisher和Handler才同时获得业务可见性。

Provider恢复时：

```text
RuntimeHost完成创建、validate和start
    -> ServiceRegistry校验Service版本与作用域
    -> 原子发布新Endpoint generation
    -> 已有ServiceHandle读取新generation
```

这只是Handle内部的一次原子读取，不是ServiceRegistry动态查找。调用中的旧Endpoint由EndpointLease保护，最后一个调用退出后才能释放。ABI或Service版本不兼容时禁止原子替换，必须生成新PluginPlan并重启相关Consumer。

第一版不得自行使用裸`AtomicPtr`实现Endpoint回收，优先采用经过验证的`ArcSwap<EndpointVersion>`或等价机制：

```rust
struct EndpointVersion {
    generation: u64,
    endpoint: Arc<dyn ServiceEndpoint>,
}
```

内存语义：

- 控制线程先完整构造不可变EndpointVersion，再以Release语义发布；
- 调用线程以Acquire语义读取并形成EndpointLease；
- Lease存续期间旧Endpoint和其动态库代码Lease都不能释放；
- generation单调递增且不复用，不能仅凭地址判断版本；
- Endpoint切换和Lease回收必须通过loom或等价并发模型测试覆盖。

## 9. 事件订阅机制

### 9.1 插件级固定订阅

插件级订阅在装配阶段完成：

```text
PluginBundle.subscription_bindings[]
    -> RuntimeHost按PluginPlan和Manifest校验绑定
    -> RuntimeHost创建PluginSlot和ResourceScope
    -> EventEngine创建SubscriberChannel
    -> SubscriptionToken登记到ResourceScope
    -> EventEngine暂存RouteTable版本
    -> Plugin.validate()
    -> EventEngine就绪后Plugin.start()
    -> EventEngine在安全点提交路由版本
    -> RuntimeHost释放ActivationGate
```

固定订阅的Filter、QoS、容量和Handler来自SubscriptionBinding。PluginEngine只协调装配事务，不在插件RUNNING后参与事件投递。

### 9.2 动态内部Runtime订阅

插件运行后创建的业务Runtime不进入PluginPlan：

```text
Plugin内部管理Service
    -> 创建Child ResourceScope
    -> ScopedEventRouter.subscribe(SubscriptionSpec, handler)
    -> EventEngine校验预授权能力并生成新RouteTable版本
    -> 返回EventReceiver和SubscriptionToken
    -> Token登记到Child ResourceScope
```

关闭该业务Runtime时释放Child ResourceScope即可注销其路由和通道。动态订阅不允许提升Manifest和PluginSpec授予的权限。

### 9.3 事件投递与执行线程

EventEngine不调用插件Handler：

```text
EventEngine
    -> 将EventHandle写入SubscriberChannel
    -> Runtime通过EventReceiver取得EventLease
    -> handler读取只读EventView
    -> 回调结束释放EventLease
```

EventEngine拥有RouteTable和SubscriberChannel生产端；PluginSlot的ResourceScope拥有SubscriptionToken和通道释放责任；RuntimeHost驱动插件级Subscriber Runtime。插件内部业务Runtime由插件自行驱动并登记到Child ResourceScope。

### 9.4 订阅QoS

```text
LATEST
    可覆盖的最新状态

RELIABLE_ORDERED
    需要可靠有序处理的事实

BEST_EFFORT
    指标、追踪和调试信息
```

Manifest声明允许的QoS集合和权限边界，PluginSpec或SubscriptionSpec选择实际QoS、容量和背压参数。EventEngine负责执行策略，插件不能在运行期自行提升权限或可靠性等级。

行情、订单、成交和风险只是QoS在实盘业务中的使用示例，不是PluginEngine内置事件类型。

### 9.5 订阅注销

```text
标记订阅QUIESCING
    -> EventEngine停止新投递
    -> Runtime停止进入新handler
    -> 等待当前handler返回
    -> 消费或释放队列中的EventLease
    -> 原子删除RouteTable条目
    -> 释放SubscriberChannel
    -> SubscriptionToken失效
```

不能先销毁PluginBundle或PluginSlot，再删除EventEngine路由和释放EventLease。

## 10. 执行模型

### 10.1 CONTROL

CONTROL只表示PluginEngine自身的控制路径：

- 生命周期；
- 配置切换；
- 健康管理；
- 接收运维控制命令。

PluginSpec不能选择CONTROL作为插件执行模型。PluginEngine Control Thread不执行事件Handler、Service Provider方法或插件后台任务。

### 10.2 DEDICATED

插件拥有独立线程。通用示例：

```text
LatencySensitiveSubscriber
StatefulRuntime
ExternalIoRuntime
```

EventEngine是Titan Core独立线程，不由RuntimeHost创建或管理。

适合：

- 状态私有且需要串行执行；
- 回调延迟敏感；
- 慢实例必须与其他实例隔离；
- 需要 CPU affinity。

### 10.3 BACKGROUND

多个冷路径插件共享后台执行器：

```text
Metrics
Logging
Persistence
Reporting
Notification
```

要求：

- 使用独立事件内存和队列；
- 不能长期持有交易热路径 EventLease；
- 队列满时按 BEST_EFFORT 或各自持久化策略处理；
- 不反向阻塞 EventEngine。

### 10.4 PASSIVE

PASSIVE插件没有自己的事件回调线程，适用于只提供Service Endpoint的插件：

```text
参数校验
本地只读View
轻量状态转换
```

PASSIVE不代表Service一定在调用者线程执行。每个ServiceExport仍通过`INLINE`、`COMMAND`或`ASYNC` call_mode独立决定调用方式。

### 10.5 ActivationGate实现语义

ActivationGate属于一次性冷路径同步，不使用Busy Spin：

```text
PREPARED
    -> ACTIVE
    -> QUIESCING
    -> STOPPED
```

- PluginSlot状态使用原子值发布，控制线程使用Release写入，运行线程使用Acquire读取；
- DEDICATED线程在PREPARED阶段通过Mutex/Condvar阻塞，必须用循环处理虚假唤醒；
- BACKGROUND任务只在ACTIVE后提交执行器；
- PASSIVE插件不创建等待线程；
- EventPublisher在ACTIVE前返回`RuntimeNotActive`；
- Gate打开后正常热路径不再访问Condvar。

为避免丢失通知，等待线程先持有Gate内部Mutex，再循环执行Acquire读取和Condvar wait；激活线程持有同一Mutex完成Release写入，然后`notify_all()`。Atomic状态用于ACTIVE后的快速读取，Mutex/Condvar只参与一次性启动和停止协调。

ActivationGate只解决插件任务何时可运行，不替代RouteTransaction和EndpointSlot。完整提交顺序遵循Core Runtime交互契约。

### 10.6 PluginControl命令协议

外部控制请求通过单写者Control Thread的有界MPSC提交：

```text
ControlCommand
    request_id
    idempotency_key
    deadline
    operation
    response_slot?
```

控制接口提供：

```text
try_submit(command) -> ControlTicket
submit_and_wait(command, deadline) -> Result
query(request_id) -> ControlOperationState
```

- 热路径和普通插件只能使用非阻塞`try_submit()`；
- 运维线程可以在明确deadline内等待；
- Control Thread不得等待由自己完成的ResponseSlot；
- 队列满立即返回`ControlQueueFull`；
- 等待超时不代表操作未执行，调用者必须使用request_id查询；
- 重试复用idempotency_key，Control Thread返回原操作状态而不是重复执行；
- 长流程由控制状态机分阶段推进，不在Control Thread中执行外部网络或磁盘等待。

### 10.7 异步运行时

第一版统一采用以下基线：

```text
DEDICATED
    原生OS线程，不运行通用异步任务

BACKGROUND / ASYNC
    RuntimeHost拥有的ColdAsyncRuntime
    基于Tokio多线程Runtime

PASSIVE
    无插件事件线程
```

约束：

- EventEngine和延迟敏感Subscriber Runtime不执行Tokio任务，也不在回调中`await`；
- ASYNC只用于配置、初始化、查询、对账和运维等冷路径；
- 所有异步任务先经过有界队列或Semaphore准入，禁止无界spawn；
- 阻塞文件、数据库和第三方同步调用进入独立有界BlockingExecutor；
- Connector可以在自身I/O线程使用Tokio current-thread runtime，但属于插件内部实现；
- 动态C ABI不暴露Rust Future，使用ControlTicket、回调或完成事件；
- ColdAsyncRuntime拥堵不得反向阻塞EventEngine、DEDICATED Runtime或下单Command Channel。

## 11. 生命周期

### 11.1 状态机

```text
DISCOVERED
    -> VALIDATED
    -> RESOLVED
    -> STARTING
    -> RUNNING
    -> QUIESCING
    -> STOPPING
    -> STOPPED

异常：FAILED
恢复：RECOVERING
```

状态迁移由 PluginEngine Control Thread 串行执行。

### 11.2 启动顺序

第一阶段，Titan main启动框架和插件能力：

```text
Titan main读取Core配置
    -> 创建EventEngine但暂不接收业务事件
    -> 创建PluginEngine
    -> ConfigurationAdapter生成PluginSpec[]
    -> PluginEngine调用compile_plugin_plan()
    -> 校验Manifest、配置、依赖和环
    -> 生成不可变PluginPlan
    -> RuntimeHost.prepare(PluginPlan)
    -> PluginFactory创建PluginBundle
    -> 校验PluginBundle没有超出PluginPlan授权范围
    -> 创建PluginSlot、PluginContext和ResourceScope
    -> ServiceRegistry暂存Service Endpoint并预绑定ServiceHandle
    -> EventEngine创建SubscriberChannel并暂存RouteTable版本
    -> RuntimeHost validate全部PluginBundle
    -> Titan main启动EventEngine
    -> RuntimeHost按PluginPlan顺序start PluginBundle
    -> 每个插件启动成功后发布引用关闭Gate的GATED Endpoint generation
    -> 提交RouteTable版本
    -> RuntimeHost对共享ActivationGate执行一次Release写入ACTIVE
    -> 所有插件进入RUNNING
    -> Titan服务进入FRAMEWORK_READY
```

此时允许不存在任何Connector、账户、行情订阅和策略实例。

以下是PluginEngine之外的实盘应用启动示例。Runtime Definitions可以从任意配置源加载，也可以事后通过管理API提交：

```text
加载MarketSourceDefinition
    -> MarketAdminService.upsert_source()
    -> MarketPlugin创建或更新MarketConnector实例

加载AccountDefinition
    -> AccountAdminService.upsert_account()
    -> AccountPlugin创建或更新AccountConnector实例

加载StrategyDefinition
    -> StrategyControlService.create()
    -> AccountService.acquire(account_id)
    -> MarketService.subscribe(MarketSymbol)
    -> 创建并激活策略事件路由
    -> AccountPlugin启动或复用对应AccountConnector实例
    -> MarketPlugin启动或复用对应MarketConnector实例
    -> 激活相应Connector事件源
    -> 等待AccountReady、MarketReady和RiskReady
    -> 启动对应StrategyRuntime回调循环
```

Connector创建、停止或失败不会改变 MarketPlugin和AccountPlugin的RUNNING状态。Connector只有在 EventEngine 和必要路由就绪后才能发布业务事件；策略实例只有在其声明的全部 `account_id`、`MarketSymbol` 和风险状态进入READY后才能进入RUNNING。

### 11.3 Plugin状态与Runtime状态

插件生命周期与其管理的Runtime生命周期必须分开。核心语义如下：

```text
ProviderPlugin = RUNNING
    ManagedRuntime[source-a] = READY
    ManagedRuntime[source-b] = RECONNECTING
    ManagedRuntime[source-c] = FAILED
```

单个Runtime失败只更新插件健康摘要，不触发插件卸载。只有插件的注册表、统一Service、控制线程或全部管理能力失效时，插件本身才进入FAILED。

在实盘业务插件组合中，Runtime Definition的增删改可由各插件提供的管理Service自行串行化。例如：

```text
MarketAdminService
    create / update / start / stop / remove

AccountAdminService
    create / update / start / stop / remove / reconcile

StrategyControlService
    create / update / start / stop / remove
```

### 11.4 启动失败回滚

任一必需插件启动失败：

```text
停止后续启动
    -> 将已启动实例按逆序QUIESCE
    -> 删除暂存或已激活的订阅
    -> 将EndpointSlot切换为UNAVAILABLE并注销Provider
    -> 释放ResourceScope
    -> 返回完整失败原因
```

该回滚只保证本地启动资源一致性，不回滚已经发送到交易所的外部事实。

### 11.5 通用停止流程

PluginEngine只执行通用生命周期停止，不理解活动订单、风险、账户或行情：

```text
Titan main请求PluginEngine.quiesce_all()
    -> PluginEngine拒绝新的插件装配和配置操作
    -> 按反向依赖顺序将PluginSlot标记为QUIESCING
    -> 将该插件提供的Service Endpoint切换为UNAVAILABLE
    -> 调用Plugin.quiesce(reason)
    -> quiesce期间继续投递插件完成收敛所需的事实事件
    -> 插件自行达到READY_TO_STOP或返回BUSY/FAILED
    -> 停止向该插件固定订阅投递新事件
    -> 按订阅注销协议等待handler并回收EventLease
    -> 到达停止条件后调用Plugin.stop()
    -> 释放ResourceScope
    -> 所有PluginSlot进入STOPPED
    -> Titan main停止EventEngine
    -> Titan main停止PluginEngine
```

每个业务插件在 `quiesce()` 中负责自己的安全条件。PluginEngine只处理通用结果和超时，不写死业务停止顺序。RiskPlugin负责后续实盘风险收缩和交易安全决策。

### 11.6 热更新边界

PluginEngine不按插件名称写死热更新规则。每个PluginManifest声明：

```text
reload_policy
    NEVER
    WHEN_QUIESCENT
    RESTART_REQUIRED
    LIVE
```

PluginEngine只校验并执行声明的生命周期策略。业务插件是否已经满足可更新条件，由插件自己的 `quiesce()` 返回结果决定。

## 12. 依赖与作用域

### 12.1 必需依赖

必需 Service 不存在时插件不能进入 RUNNING。以下仅为业务插件示例：

```text
StrategyPlugin requires:
    orders@v1
    market@v1
    account@v1
```

### 12.2 可选依赖

可选 Service 缺失时允许启动，但插件必须显式处理能力缺失：

```text
metrics: optional
reporting: optional
```

可选依赖不能用于绕过关键安全能力。

### 12.3 作用域解析

每个Service依赖必须显式声明作用域选择器，不能让ServiceRegistry根据注册或启动顺序随机选择Provider。以下为业务插件示例：

```text
account / okx-main   -> AccountPlugin.AccountConnector[okx-main]
account / bybit-main -> AccountPlugin.AccountConnector[bybit-main]
```

PluginEngine只处理 `CUSTOM(namespace, key)`，不理解 `account` 的含义。

### 12.4 依赖环

同步 Service 调用图禁止成环：

```text
A Service -> B Service -> A Service
```

反向状态通知使用 EventEngine。以下为业务调用示例：

```text
Strategy -> OrderService.submit()
AccountPlugin中的Connector -> EventEngine -> Strategy.on_order()
```

## 13. 运行异常上报

PluginEngine不执行交易安全决策。RuntimeHost发现异常后只执行通用处理：

```text
发现线程退出、Panic、回调预算超限或资源释放失败
    -> 更新PluginSlot.lifecycle_state/health
    -> 将不可用Provider的EndpointSlot切换为UNAVAILABLE
    -> 保存结构化错误
    -> 发布PluginRuntimeFailed/PluginHealthChanged
    -> 等待PluginControlService显式控制或授权业务插件响应
```

PluginEngine不会因为异常而自动：

- 禁止或允许下单；
- 暂停策略；
- 撤销活动订单；
- 调整风险额度；
- 发起账户对账；
- 重启Connector或业务Runtime。

这些行为由后续RiskPlugin或相关业务插件订阅通用健康事实后决定。PluginEngine只保证失效Service不会继续伪装成可用Provider。

EventEngine或PluginEngine自身出现核心故障时，由Titan main负责进程级退出和外部恢复，PluginEngine不尝试内部自恢复。

## 14. 配置更新

### 14.1 配置快照

每个PluginSlot持有不可变配置快照：

```text
config_version
config_hash
loaded_at
source
```

热路径读取本地快照，不访问全局配置中心。

每个动态业务资源持有独立定义版本：

```text
definition_id
definition_version
definition_hash
loaded_at
source
runtime_state
```

Plugin配置版本和Runtime Definition版本互不覆盖。

### 14.2 更新流程

```text
ConfigurationAdapter生成新PluginSpec[]
    -> compile_plugin_plan()
    -> 生成新PluginPlan
    -> 与当前PluginPlan比较并生成ChangePlan
    -> 判断是否允许在线应用
    -> RuntimeHost.prepare()
    -> PluginFactory创建新PluginBundle
    -> 校验新PluginBundle没有超出PluginPlan授权范围
    -> 暂存新Endpoint和RouteTable版本
    -> Plugin.validate()
    -> 在安全点按ChangePlan执行quiesce/start顺序
    -> 发布引用关闭Gate的新GATED Endpoint generation
    -> 按Core Runtime契约提交RouteTable版本
    -> 对新PluginSlot共享ActivationGate执行Release写入ACTIVE
    -> 释放旧ResourceScope
    -> 发布ConfigVersionChanged
```

### 14.3 更新分类

```text
LIVE
    日志等级、非关键阈值

RESTART_PLUGIN
    插件线程模型、静态容量上限、插件级故障策略

RESTART_ENGINE
    EventArena容量、EventEngine队列容量、核心ABI
```

插件必须在 Manifest 或配置 Schema 中声明字段更新等级，不能运行时猜测。

Connector、账户、行情源和策略实例不属于以上 Plugin配置更新：

```text
MarketSourceDefinition
    -> MarketAdminService

AccountDefinition
    -> AccountAdminService

StrategyDefinition
    -> StrategyControlService
```

每类Runtime Definition拥有自己的版本、校验、状态和增删改流程，不重建Plugin依赖图，也不重启对应插件。

## 15. 插件包与加载方式

### 15.1 第一版建议

第一版优先采用静态注册或启动期注册：

```text
PluginRegistry.register(factory)
```

优点：

- Rust类型和生命周期更安全；
- Service强类型绑定简单；
- 没有动态库卸载风险；
- 性能路径容易内联和优化；
- 部署和诊断更简单。

### 15.2 动态插件

动态插件属于第二阶段能力。启用前必须提供单一版本化C ABI入口：

```text
titan_plugin_entry_v1()
    -> PluginApiV1*
```

```text
PluginApiV1
    magic
    struct_size
    abi_major
    abi_minor
    manifest_schema_major
    manifest_schema_minor
    feature_bits
    function_pointers[]
```

动态库边界不能暴露Rust trait object、Rust引用、Rust Future或标准库集合。跨ABI对象必须使用：

- 固定布局结构；
- 显式长度和容量；
- 版本字段；
- 明确的所有权和释放函数；
- Host和Plugin各自提供的分配与释放函数；
- 整数状态码和显式错误缓冲区。

### 15.3 ABI与Schema兼容

- `abi_major`必须完全匹配；
- Host的abi_minor必须覆盖插件声明的最低minor；
- 结构体通过`struct_size`支持尾部新增字段，未知可选字段忽略；
- 未支持的必需feature bit导致加载失败；
- Manifest Schema major不兼容时拒绝加载；
- Manifest Schema minor通过ConfigurationAdapter迁移到当前标准格式；
- 插件业务配置携带独立`config_schema_version`，迁移必须在`PluginFactory.create()`前完成；
- Service ABI或语义不兼容时生成新PluginPlan并重启Consumer，不允许EndpointSlot热替换。

### 15.4 Panic与动态库生命周期

每个Rust插件导出函数必须在插件内部捕获Panic并转换为错误码：

```rust
catch_unwind(AssertUnwindSafe(|| plugin_call()))
    -> TitanStatus
```

- Panic禁止跨越`extern "C"`边界；
- Host回调同样禁止向插件传播Panic；
- 只有采用unwind构建策略的Rust Panic可以捕获；
- `panic=abort`、SIGSEGV、死循环和内存破坏无法在进程内恢复；
- 不可信插件必须部署到独立进程；
- EndpointLease除持有Endpoint外，还必须持有动态库代码Lease。

第一版不支持运行中卸载含活动线程、ServiceHandle或EventLease的动态库。

### 15.5 插件身份

```text
plugin_type     插件类型，例如market.standard
instance_id     运行实例，例如market
package_version 代码包版本
config_version  配置版本
```

日志、指标、事件和恢复材料必须同时保留 `plugin_type` 与 `instance_id`。

## 16. 权限与安全边界

Manifest 声明插件需要的能力：

```text
services.require[]
events.publish[]
events.subscribe[]
resources.network[]
resources.filesystem[]
```

单进程原生插件不能形成真正的安全沙箱，但 PluginEngine仍应：

- 只注入声明过的 Service；
- 拒绝未声明的事件发布和订阅；
- 记录配置和能力变更；
- 不在 Context 中暴露全局可变状态；
- 对不可信插件建议使用独立进程，而不是依赖单进程权限模型。

## 17. 性能设计

### 17.1 PluginEngine不在热路径

逐笔行情链路：

```text
MarketPlugin / AccountPlugin Connector
    -> EventEngine
    -> StrategyRuntime
```

下单链路：

```text
Strategy
    -> Bound OrderServiceHandle
    -> AccountExecutionService
    -> Account Connector Order MPSC
```

两条链路都不访问：

- PluginRegistry；
- ServiceRegistry动态解析；
- RuntimeHost控制状态；
- ConfigurationAdapter；
- `compile_plugin_plan()`；
- PluginEngine Control Thread。

### 17.2 启动期预绑定

启动完成后：

- Service名称解析为数组索引或强类型Handle；
- Event订阅解析为 EventEngine RouteTable；
- 配置解析为不可变本地结构；
- 回调解析为具体 Runtime Handler；
- 权限检查转换为已验证的发布能力。

账户、行情等业务路由由相应Provider插件自行预编译，不属于PluginEngine的数据结构。

逐笔路径不进行字符串查找、反射、动态依赖解析和配置反序列化。

### 17.3 插件回调隔离

EventEngine不执行插件回调。需要隔离的Subscriber Plugin使用独立线程，后台插件由独立执行器处理。

RuntimeHost和可观测性模块记录：

```text
callback_duration_ns
event_channel_depth
oldest_event_age_ns
service_call_duration_ns
thread_heartbeat_age_ns
```

慢回调只影响自身Runtime；同步INLINE call_mode超预算发生在调用者线程，不能被框架安全抢占。

### 17.4 回调预算与卡死检测

`callback_budget`是性能SLO和熔断依据，不是抢占式超时。单进程Rust无法安全终止正在执行的任意INLINE函数、死循环或阻塞线程。

```text
callback_budget
    soft_budget_us
    stall_threshold_us
    max_consecutive_violations
    violation_action
```

调用边界记录：

```text
callback_state
    running
    callback_id
    started_at
    owner_thread
```

- 调用返回后统计duration和连续超预算次数；
- 独立Watchdog通过started_at与线程心跳发现尚未返回的疑似卡死调用；
- Watchdog扫描周期必须不高于最小stall_threshold的四分之一；
- 达到stall阈值后将相关EndpointSlot置为UNAVAILABLE，阻止新调用进入；
- Runtime标记为STALLED并发布`PluginCallbackStalled`；
- RiskPlugin订阅该事实并决定暂停策略、限制风险或执行外部安全处置；
- 已经卡住的调用不能被线程强杀，也不能依赖`catch_unwind`恢复；
- 核心关键线程无法恢复时由Titan main执行进程级退出。

INLINE只允许受信任、经过基准测试、无I/O、无锁等待、无协程等待的Provider。需要硬隔离或真正deadline的调用必须使用COMMAND跨线程模型或独立进程插件。

### 17.5 锁约束

- 热路径Service禁止全局互斥锁；
- PluginEngine控制状态由Control Thread单写，外部控制请求通过有界队列提交；
- EventEngine和跨Runtime命令通道使用有界队列；
- 插件内部可变状态由插件声明明确所有者；
- 只读View使用不可变快照、版本化视图或明确的读协议。

## 18. 可观测性

### 18.1 指标

PluginEngine必须提供：

```text
plugin_state{instance_id}
plugin_start_total{instance_id,result}
plugin_stop_total{instance_id,result}
plugin_restart_total{instance_id,reason}
plugin_failure_total{instance_id,reason}

plugin_callback_duration_ns{instance_id,callback}
plugin_thread_heartbeat_age_ns{instance_id}
plugin_event_channel_depth{instance_id}
plugin_callback_budget_exceeded_total{instance_id,callback}
plugin_callback_stalled_total{instance_id,callback}

service_call_total{service,provider,result}
service_call_duration_ns{service,provider}
service_unavailable_total{service,consumer}

resource_count{instance_id,type}
resource_release_failure_total{instance_id,type}

plugin_profile_version
config_version{instance_id}
runtime_definition_version{type,id}
runtime_state{type,id}
route_table_version
```

### 18.2 TraceContext与Flight Recorder

Event、Service Command和结果事件统一传播Core Runtime契约定义的紧凑上下文：

```text
TraceContext
    trace_id: u64
    causation_id: u64
```

- `process_run_id + trace_id`形成全局链路标识；
- ServiceHandle自动将当前TraceContext写入Command；
- EventPublisher默认继承trace_id，并把直接原因写入causation_id；
- `client_order_id`、`strategy_id`、`account_id`继续作为业务关联字段；
- 热路径不创建字符串，不同步导出Span；
- 每个线程写入有界Trace Ring，由后台线程格式化并导出；
- 正常链路按trace_id确定性采样，错误、拒绝、超预算和卡死链路强制保留；
- 保留可配置时间窗口的内存Flight Recorder，异常发生后异步冻结和导出；
- OpenTelemetry只作为后台导出格式，不进入EventEngine或下单热路径。

示例链路：

```text
MarketEvent(event_id=100, trace=100)
    -> StrategyCallback(trace=100, causation=100)
    -> OrderCommand(trace=100, causation=100)
    -> OrderAccepted(trace=100, causation=command_id)
    -> Fill(trace=100, causation=order_event_id)
```

### 18.3 控制与诊断接口

控制接口至少支持：

- 查询插件类型和实例；
- 查询生命周期和健康状态；
- 查询提供和依赖的 Service；
- 查询事件订阅；
- 查看线程和队列状态；
- 启动、quiesce、停止和重启实例；
- 查看配置版本和最近错误。

## 19. 错误模型

```text
PluginError
    ManifestInvalid
    ConfigInvalid
    ApiVersionMismatch
    AbiVersionMismatch
    ManifestSchemaMismatch
    UnsupportedAbiFeature
    DependencyMissing
    DependencyCycle
    ServiceConflict
    ServiceUnavailable
    RuntimeNotActive
    ControlQueueFull
    ControlDeadlineExceeded
    SubscriptionRejected
    RuntimeStartFailed
    StartTimeout
    StopTimeout
    ResourceReleaseFailed
    CallbackBudgetExceeded
    CallbackStalled
    PluginFailed
```

错误必须携带：

```text
plugin_type
instance_id
lifecycle_state
operation
cause_chain
occurred_at
recoverable
request_id?
trace_context?
```

禁止只返回无上下文字符串。

## 20. 测试方案

### 20.1 Manifest与依赖测试

- Manifest Schema校验；
- 必需Service缺失；
- 可选Service缺失；
- Service版本不兼容；
- 同作用域Provider冲突；
- 同步依赖环检测；
- 多实例作用域解析；
- 一个插件提供多个Service。

### 20.2 生命周期测试

- 正常启动和逆序停止；
- 中间插件启动失败后的完整回滚；
- `quiesce()`超时；
- 插件线程意外退出；
- 资源释放失败；
- EventLease未释放；
- Provider停止时EndpointSlot进入UNAVAILABLE；
- Provider兼容恢复时ServiceHandle原子切换generation；
- 不兼容Service版本禁止原子替换；
- 不同execution domain禁止生成直接调用Handle；
- PluginBundle中的Service和订阅声明完整登记；
- 固定订阅只在插件启动成功后激活；
- ActivationGate打开前Publisher和Handler不可运行；
- INLINE卡死时阻断后续调用但不尝试强杀线程；
- ScopedEventRouter不能越权提升事件类型或QoS；
- 运行异常只更新状态和发布事件，不执行自动业务动作；
- `reload_policy`约束生效；
- PluginContext和ResourceScope完整释放。

### 20.3 并发测试

- Core Runtime API major不兼容时拒绝启动；
- RouteTransaction与Endpoint提交任一步失败时完整回滚；
- PluginEngine控制操作串行化；
- EventEngine路由切换时订阅注销；
- ServiceHandle与Provider生命周期竞争；
- Endpoint generation切换与旧EndpointLease回收竞争；
- EndpointSlot的Acquire/Release和Arc回收通过loom模型测试；
- 动态路由切换与EventLease回收竞争；
- ActivationGate通知无丢失且正确处理虚假唤醒；
- ControlCommand队列满、等待超时、幂等重试和结果查询；
- 配置更新和插件停止同时发生；
- Subscriber Runtime失败不影响其他实例；
- BackgroundExecutor拥堵不影响独立Runtime。

### 20.4 性能测试

原型阶段先采用以下实验室参考门槛，完成目标硬件基准后冻结为版本化`PerformanceEnvelope`。这些数值是框架开销目标，不是交易所端到端SLA：

| 路径 | 初始参考目标 |
|---|---:|
| 预绑定INLINE Handle框架开销 | P99不高于250ns |
| EndpointSlot generation读取 | P99不高于100ns |
| 有界MPSC `try_send()` | P99不高于1us |
| EventEngine已入队到SubscriberChannel可见 | P99不高于5us |
| DEDICATED Subscriber取事件到Handler入口 | P99不高于2us |
| 热路径堆分配 | 0 |
| 关键事件静默丢失 | 0 |

端到端延迟必须分段测量：

```text
T_ws_to_order =
    T_decode
  + T_event_publish
  + T_event_dispatch
  + T_subscriber_receive
  + T_strategy_callback
  + T_inline_services
  + T_order_enqueue
```

策略计算时间必须与框架开销分开。基准报告必须记录：

- CPU型号、频率策略、NUMA、内核和编译参数；
- Release、LTO、CPU affinity和预热条件；
- 50%、80%、95%容量负载及突发流量；
- P50、P99、P99.9、最大值、CPU占用和队列深度；
- 插件数量、订阅数量和消息大小；
- RuntimeHost指标与Trace采集开启前后的差值；
- 静态插件与动态ABI边界差值。

冻结基线后，关键路径相对回退超过10%或违反绝对门槛时CI性能测试失败。不同硬件使用独立PerformanceEnvelope，不能混用结果。

### 20.5 实盘插件组合集成测试

- 无Connector时MarketPlugin和AccountPlugin正常进入RUNNING；
- 插件RUNNING后从文件、数据库和API加载Runtime Definition；
- 动态新增或删除Connector不重启插件；
- 单个Connector失败不改变插件生命周期状态；
- AccountPlugin存在活动订单期间的停止流程；
- MarketPlugin停止不影响AccountPlugin订单连接；
- AccountPlugin停止不影响MarketPlugin公共行情连接；
- 跨线程OrderService提交延迟；
- `account_id -> OrderSender`预绑定路由开销；

## 21. 配置示例

### 21.1 Titan Core配置

Core配置只决定 EventEngine 和 PluginEngine 本身如何运行：

```yaml
core:
  event_engine:
    runtime:
      mode: dedicated
      cpu_affinity: 4

  plugin_engine:
    control:
      start_timeout_ms: 30000
      stop_timeout_ms: 30000
      queue_capacity: 1024
      result_retention_ms: 300000

    runtime:
      heartbeat_timeout_ms: 5000
      watchdog_interval_ms: 1

    background:
      worker_threads: 2
      queue_capacity: 8192

    cold_async:
      runtime: tokio_multi_thread
      worker_threads: 2
      admission_capacity: 4096
      blocking_worker_threads: 2
      blocking_queue_capacity: 512
```

### 21.2 PluginSpec输入示例

以下外部配置由ConfigurationAdapter转换为标准化`PluginSpec[]`，只决定加载哪些能力提供者：

```yaml
plugins:
  - instance_id: market-provider
    plugin: market.standard
    execution:
      model: dedicated
      cpu_affinity: 6
      callback_budget:
        soft_budget_us: 100
        stall_threshold_us: 5000
        max_consecutive_violations: 3
        violation_action: quarantine
    config:
      max_market_runtimes: 1024
      default_event_capacity: 65536

  - instance_id: account-provider
    plugin: account.standard
    execution:
      model: dedicated
      cpu_affinity: 7
      callback_budget:
        soft_budget_us: 100
        stall_threshold_us: 5000
        max_consecutive_violations: 3
        violation_action: quarantine
    config:
      max_account_runtimes: 256
      default_order_capacity: 8192

  - instance_id: order-provider
    plugin: order.standard

  - instance_id: risk-provider
    plugin: risk.standard

  - instance_id: cross-exchange-strategy-provider
    plugin: strategy.cross-exchange
    config:
      default_event_channel_capacity: 16384
```

加载完成后，MarketPlugin和AccountPlugin可以在没有任何Connector实例的情况下保持RUNNING并提供管理Service。

### 21.3 Runtime Definitions

以下内容不是PluginSpec。它可以来自独立文件、数据库或运行期管理API，并可在插件加载完成后的任意时间提交。

```yaml
market_sources:
  - source_id: okx-market
    connector: okx
    capabilities: [market_data]
    symbols:
      - BTC-USDT-SWAP

  - source_id: bybit-market
    connector: bybit
    capabilities: [market_data]
    symbols:
      - BTCUSDT

accounts:
  - account_id: okx-main
    connector: okx
    credentials: secret://okx/main
    capabilities:
      - account_data
      - order_entry
      - reconciliation

  - account_id: bybit-main
    connector: bybit
    credentials: secret://bybit/main
    capabilities:
      - account_data
      - order_entry
      - reconciliation

strategies:
  - strategy_id: btc-arbitrage
    strategy_type: cross-exchange
    account_ids:
      - okx-main
      - bybit-main

    market_symbols:
      - source_id: okx-market
        symbol: BTC-USDT-SWAP
      - source_id: bybit-market
        symbol: BTCUSDT

    event_channel_capacity: 16384
```

对应控制调用：

```text
market_sources[] -> MarketAdminService.upsert_source()
accounts[]       -> AccountAdminService.upsert_account()
strategies[]     -> StrategyControlService.create()
```

参数默认值必须由实现和基准测试确定，示例不构成生产推荐值。

## 22. 实施顺序

### 第一阶段：最小PluginEngine

- Core Runtime API版本协商；
- PluginManifest；
- PluginFactory、PluginBundle和Plugin接口；
- PluginRegistry；
- ServiceRegistry和EndpointSlot；
- `compile_plugin_plan()`和不可变PluginPlan；
- RuntimeHost和PluginSlot；
- ActivationGate；
- PluginContext；
- ResourceScope；
- 基础生命周期。

### 第二阶段：EventEngine集成

- EventPublisher注入；
- PluginBundle订阅绑定；
- ScopedEventRouter动态路由；
- SubscriberChannel创建；
- RouteTransaction暂存、提交、回滚和退休；
- Subscriber Runtime线程；
- 订阅注销和EventLease回收。

### 第三阶段：MarketPlugin与AccountPlugin集成

- 统一MarketService和AccountService；
- MarketAdminService和AccountAdminService；
- StrategyControlService；
- 文件、数据库和管理API的Runtime Definition适配；
- AccountExecutionService；
- 完整Connector能力配置；
- `account_id -> AccountConnector`路由；
- `MarketSourceHandle -> MarketConnector`路由；
- Connector自身的MarketSubscription引用管理；
- AccountReady、MarketReady与策略启动门控；

### 第四阶段：执行与异常上报

- DEDICATED、BACKGROUND、PASSIVE和ColdAsyncRuntime；
- CPU affinity；
- 回调预算、Watchdog和STALLED状态；
- 线程心跳；
- 结构化运行异常；
- PluginHealthChanged事件；
- EndpointSlot不可用和generation切换处理。

### 第五阶段：配置与插件包

- 配置版本和ChangePlan；
- 动态插件ABI原型与兼容矩阵；
- 插件包验证；
- 有界ControlCommand协议；
- TraceContext、Flight Recorder和后台导出；
- PerformanceEnvelope与CI回归门槛；
- 完整指标和诊断工具。

## 23. 最终边界

PluginEngine 的稳定职责：

```text
接收PluginSpec[]
    -> compile_plugin_plan()
    -> 生成不可变PluginPlan
    -> PluginFactory创建PluginBundle
    -> RuntimeHost创建PluginSlot、PluginContext和ResourceScope
    -> 暂存Service Endpoint和EventEngine路由
    -> validate/start PluginBundle
    -> 按Core Runtime契约事务式提交RouteTable、Endpoint和ActivationGate
    -> 管理PluginSlot生命周期
    -> 记录并发布运行状态
```

运行期核心关系：

```text
命令和查询：Plugin -> Bound Service -> Provider

事实传播：Plugin -> EventEngine -> Subscriber Runtime

生命周期：Operator -> PluginControlService -> PluginEngine
```

明确排除：

```text
逐笔事件转发
策略业务执行
订单状态机
风险业务规则
收益计算
数据库事务
跨交易所事务
动态回调瀑布链
```

PluginEngine 只负责把插件正确、稳定并高效地装配起来。应用进入 RUNNING 后，逐笔行情与下单热路径不再访问 PluginEngine，从而同时满足插件松耦合、调用简单和性能优先三个目标。

## 附录A：实盘业务插件组合示例

本附录用于说明核心插件机制如何承载实盘业务，不属于PluginEngine内置规范。

### A.1 MarketPlugin与AccountPlugin

```text
MarketPlugin
    ├── MarketService
    ├── MarketConnectorRegistry
    └── Connector实例[]

AccountPlugin
    ├── AccountService
    ├── AccountExecutionService
    ├── AccountAdminService
    ├── AccountConnectorRegistry
    └── Connector实例[]
```

MarketPlugin和AccountPlugin只负责Factory、Registry、Service门面和Connector生命周期。公共行情实现、
缺口恢复和市场事实发布属于MarketConnector；认证、订单状态合流、账户事实发布和对账属于
AccountConnector。插件本身不维护第二套MarketView、AccountView或交易所状态机。

二者是普通业务插件，不属于Titan Core。它们可以在没有Connector和业务Runtime时进入RUNNING并提供管理Service。

### A.2 Connector模型

保留能力完整的交易所Connector类型：

```text
OkxConnector
BybitConnector
BinanceConnector
```

同一Connector实现可以支持`MARKET_DATA`、`ACCOUNT_DATA`、`ORDER_ENTRY`和`RECONCILIATION`。MarketPlugin和AccountPlugin分别创建独立实例并只启用所需能力：

```text
MarketPlugin
    -> OkxConnector(MARKET_DATA)

AccountPlugin
    -> OkxConnector(ACCOUNT_DATA + ORDER_ENTRY + RECONCILIATION)
```

两个实例共享协议实现代码，但不共享连接、可变状态、队列和生命周期。Connector是插件管理的业务
实例，不是PluginSlot或Service，也不向Consumer暴露。

### A.3 统一Service与Connector实例

Service不按账户或市场重复创建，统一Service通过稳定标识路由：

```text
AccountService
    account_id -> AccountConnectorRegistry entry

MarketService
    source_handle -> MarketConnectorRegistry entry
```

字符串标识在策略或其他业务Runtime创建阶段解析为连续数字RouteHandle，热路径只使用数字Handle。

多个策略可以共享Connector实例，但订单归属、风险额度及业务订阅必须分别保存。Market订阅的共享和
引用计数由具体MarketConnector实现；账户实例生命周期由AccountAdminService显式控制，不由策略引用数
隐式启停：

```text
Strategy启动
    -> AccountService.resolve(account_id)
    -> MarketService.subscribe(MarketSymbol)

Strategy停止
    -> 释放MarketSubscription
```

MarketConnector没有订阅者后可以按自身策略延迟退订。AccountConnector只能通过显式管理操作关闭，
并且必须按shutdown policy处理活动订单、待确认命令和最终账户对账。
