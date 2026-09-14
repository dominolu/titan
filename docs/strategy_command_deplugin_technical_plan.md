# Titan 去插件化与最简策略执行技术方案

## 1. 架构决策

当前阶段不建设 Gateway、轻量 OMS 或账户执行状态机。所有原 Plugin 形态都被移除：具有独立领域职责的
组件改为由 `TradingRuntime` 直接构造、持有和管理的普通进程内核心服务；Connector 作为底层实现归入
Account/Market 服务，不单独抽象成服务：

- `AccountPlugin` → `AccountService`；
- `ConnectorPlugin` / venue plugin → 静态 venue connector factory/实现，分别归属 `AccountService` 和
  `MarketService`，不再形成单独服务；
- `StrategyPlugin` → `StrategyService`；
- `MarketPlugin` → `MarketService`；
- 其他 Plugin 也按同一规则迁移为显式依赖的核心服务。

这里的“去插件”是删除 PluginEngine、PluginFactory、manifest、动态装载、service export/lookup 和通用
Plugin 生命周期适配层，不是删除 Account、Connector、Strategy、Market 的业务能力。目标架构是：

```text
Static venue connectors
   ├─ market WS  ─> MarketService  ─┐
   └─ private WS ─> AccountService ─┤
                                    ▼
                                EventEngine
                                    │ 事实事件
                                    ▼
                              StrategyService
                                    │ Strategy 直接调用
                                    ▼
                              ExecutionHandle
                                    │ 直接异步提交
                                    ▼
                     Static account connector / REST API
```

EventEngine 只负责行情、private WS 账户事实和 connector health 的推送，以及策略事件 lane 的路由、串行
回调和隔离。核心服务之间通过构造参数和强类型句柄直接依赖，不通过 `ServiceRegistry`、字符串服务名或
Plugin service endpoint 查找。

当前阶段移除：

- PluginEngine、PluginFactory、Plugin manifest、Plugin service registry/export 及通用 Plugin 生命周期；
- Strategy ABI Command Outbox/Batch；
- StrategyCommandGateway；
- Plugin 形式的 AccountExecutionService/ServiceHandle/ExecutionEndpoint 命令适配层；
- Gateway 中的权限、路由、ID、幂等和订单归属；
- per-account command MPSC 和 command journal；
- outcome-unknown 查询、自动 reconcile 和账户命令隔离；
- 动态 Connector JSON C ABI。

这些交易治理能力以后由独立 OMS 统一实现，当前 Runtime 不再提前实现“半个 OMS”。

## 2. 不可回避的执行约束

去掉 Command Outbox 和 per-account MPSC 后，策略回调不能直接 `await` REST。EventEngine 当前调用同步
handler；如果在 handler 内阻塞 HTTP，该策略 lane 的后续行情、订单和成交都会停止。

因此当前最简实现采用：

```text
Strategy callback
  -> ExecutionHandle::submit/cancel
  -> shared Tokio execution runtime 立即 spawn Future
  -> callback 返回
  -> REST completion 由 execution task 本地处理
```

`ExecutionHandle` 只是预绑定 connector 与 executor 的薄句柄，不是 Gateway 或 OMS。它不做路由、权限、
风控、幂等、owned/pending order、重试、reconcile、排序或账户级排队。

必须接受的临时限制：

- 同一账户 submit/cancel 可以并发，完成顺序不保证；
- cancel 可能先于 submit 到达交易所；
- 网络超时只记录 Unknown，不自动查单；
- REST reject/unknown 暂不直接回调策略，策略订单状态以 private WS 事实为准；
- 不自动恢复账户状态；
- 策略提供 client order ID，Runtime 不保证幂等；
- 交易所变慢时 active task 可能增长；
- 在 OMS 落地前，这不是完整的生产交易安全模型。

## 3. 目标组件

### 3.1 核心服务模型

所有服务都是普通 Rust 类型，编译进主程序，由 `TradingRuntime` 显式注入依赖和管理生命周期：

```rust
pub struct TradingRuntime {
    event_engine: EventEngine,
    market_service: MarketService,
    account_service: AccountService,
    strategy_service: StrategyService,
    execution_runtime: tokio::runtime::Runtime,
}
```

核心服务不实现 `Plugin`，不返回 `PluginBundle`，不导出命名 endpoint，也不在运行期查询
`ServiceRegistry`。服务之间只传递具体类型或窄的强类型 trait/handle：

```text
TradingRuntime
 ├─ MarketService     持有 market connector，管理行情订阅并发布行情事实
 ├─ AccountService    持有 account connector，管理账户、private WS 与 REST 执行
 ├─ StrategyService   管理策略实例、事件 lane 与回调
 └─ EventEngine       只传递事实事件
```

每个核心服务可以保留自己的领域生命周期状态，但生命周期由 `TradingRuntime` 按明确顺序调用，而不是由
PluginEngine 驱动。原插件中的业务 core、registry、connector 实现和必要状态应迁移/改名后保留；只删除
插件包装、动态发现和跨插件服务调用机制。

### 3.2 TradingRuntime

```rust
pub struct TradingRuntime {
    event_engine: EventEngine,
    execution_runtime: tokio::runtime::Runtime,
    market_service: MarketService,
    account_service: AccountService,
    strategy_service: StrategyService,
}
```

它显式管理启停顺序，不持有 PluginPlan、ServiceRegistry 或动态 package session。

启动：

```text
构造 EventEngine
  -> 由静态 catalog/factory 构造 account/market connector
  -> 将 connector 分别注入 AccountService、MarketService
  -> 构造 StrategyService
  -> 注册事件类型和策略路由
  -> 启动 AccountService / MarketService
  -> 启动 StrategyService
```

停止：

```text
停止 StrategyService 新回调
  -> ExecutionHandle 停止接受新 task
  -> deadline 内等待/取消未完成 task
  -> 停止 AccountService / MarketService
  -> 停止 EventEngine
```

### 3.3 静态 Connector Catalog

不引入单独的 `ConnectorService`。Connector 编译进主程序，Catalog 只是 `TradingRuntime` 构造阶段使用的
factory 集合：

```rust
pub struct ConnectorCatalog {
    market: HashMap<ConnectorType, Arc<dyn MarketConnectorFactory>>,
    account: HashMap<ConnectorType, Arc<dyn AccountConnectorFactory>>,
}
```

Catalog 不参与运行期请求路径，也不拥有独立生命周期。factory 创建出的 market connector 直接归
`MarketService` 持有，account connector/BrokerApi 直接归 `AccountService` 持有；如果二者需要共享 venue
transport/session，则在构造时注入同一个 `Arc<VenueClient>`，仍不增加服务层。

热路径不经过字符串查询、C ABI、JSON 或全局动态 handle 表。删除 `connector_plugin_packages`、
`titan-connector-loader`、venue cdylib wrapper、Dynamic Connector adapter 和
`connector/src/dynamic_plugin.rs`，保留各交易所真正的 BrokerApi、REST 和 WS 实现。

### 3.4 AccountService 与 MarketService

`AccountServiceCore` 的账户连接、private WS、账户快照查询和账户事实发布能力迁移到普通
`AccountService`；`MarketPlugin` 的行情连接、订阅和行情事实发布能力迁移到普通 `MarketService`。

二者直接持有静态 factory 创建的强类型 connector handle，并直接持有 EventEngine publisher。不再通过
Plugin identity、service export、endpoint 或运行时 service lookup 连接。原
`AccountExecutionService` 仅作为旧命令总线入口删除；账户领域服务本身保留。

```text
Static market connector  ──> MarketService  ──> EventEngine
Static account connector ──> AccountService ──> EventEngine
```

### 3.5 ExecutionHandle

策略创建时，每个账户绑定一个薄句柄：

```rust
pub struct ExecutionHandle {
    account_id: AccountId,
    connector: Arc<dyn DirectExecutionConnector>,
    executor: tokio::runtime::Handle,
    observer: Arc<dyn ExecutionObserver>,
    accepting: Arc<AtomicBool>,
}

pub trait DirectExecutionConnector: Send + Sync {
    fn submit(&self, request: NewOrderRequest) -> BoxFuture<'static, Result<OrderInfo, ApiError>>;
    fn cancel(&self, request: CancelOrderRequest) -> BoxFuture<'static, Result<OrderInfo, ApiError>>;
}
```

策略 API：

```rust
impl ExecutionHandle {
    pub fn submit(&self, request: NewOrderRequest) -> Result<ExecutionTaskId, SpawnError>;
    pub fn cancel(&self, request: CancelOrderRequest) -> Result<ExecutionTaskId, SpawnError>;
}
```

调用只检查 Runtime 是否停止，分配观测用 task ID，然后 spawn connector Future。REST 返回由 execution task
交给只负责日志、指标和 trace 的 observer：

```rust
pub enum ObservedExecutionOutcome {
    Accepted(OrderInfo),
    Rejected(ApiError),
    Unknown(ApiError),
}

pub struct ObservedExecutionResult {
    pub task_id: ExecutionTaskId,
    pub account_id: AccountId,
    pub client_order_id: Option<String>,
    pub outcome: ObservedExecutionOutcome,
}
```

`ExecutionObserver` 不能调用策略，也不能发布策略业务事件。Runtime 只记录 Unknown，不查询订单、不重试、
不 reconcile。订单的权威状态来自 connector private WS，经 EventEngine 交付策略。

### 3.6 StrategyService

`StrategyPlugin` 改为普通 `StrategyService`，保留策略实例管理、事件 lane、启停和故障隔离；删除
Plugin lifecycle/factory/bundle、service 注入以及 ABI command outbox。原生 Rust 策略直接调用
`ExecutionHandle`：

原生 Rust 策略直接调用 `ExecutionHandle`：

```rust
pub trait Strategy: Send + 'static {
    fn on_event(
        &mut self,
        event: StrategyEvent<'_>,
        context: &StrategyContext,
    ) -> Result<(), StrategyError>;
}

pub struct StrategyContext {
    pub now_ns: i64,
    pub execution: Box<[ExecutionHandle]>,
}
```

```rust
context.execution[0].submit(NewOrderRequest { /* ... */ })?;
```

不再写 `OrderCommand[]`，也没有 callback 返回后的 drain/gateway。

如果保留现有 Numba 策略，语言 ABI 仍然存在，但改用 host function table，而不是 command buffer：

```rust
#[repr(C)]
pub struct StrategyExecutionApiV1 {
    pub context: *mut c_void,
    pub submit: extern "C" fn(
        context: *mut c_void,
        account_no: u32,
        request: *const AbiNewOrderRequest,
        task_id_out: *mut u64,
    ) -> i32,
    pub cancel: extern "C" fn(
        context: *mut c_void,
        account_no: u32,
        request: *const AbiCancelOrderRequest,
        task_id_out: *mut u64,
    ) -> i32,
}
```

Host function 只解析固定布局 request 并调用预绑定 ExecutionHandle。如果不再支持 Python/Numba，则这层
ABI 也一并删除。

## 4. 新流程

```text
EventEngine
  -> StrategyService lane
  -> Strategy::on_event(event, context)
  -> context.execution[account_no].submit(request)
  -> tokio::spawn(connector.submit(request))
  -> callback 返回

异步：
Connector REST Future
  -> OrderInfo / ApiError
  -> ExecutionObserver(log/metrics/trace)

账户事实：
Connector private WS
  -> AccountService
  -> Order/Fill/Position/Balance
  -> EventEngine
  -> StrategyService
  -> Strategy::on_event
```

Numba 路径只多一层固定 ABI host call：

```text
EventEngine -> ABI event view -> Numba callback
  -> host_execution.submit -> ExecutionHandle -> spawn Connector Future
```

移除的中间层：

```text
OrderCommandBuffer
StrategyRuntime drain
StandardStrategyCommandGateway
PluginStrategyServices
PluginAccountExecutionService
ServiceHandle<AccountExecutionApi>
AccountPlugin ExecutionEndpoint
AccountServiceCore command admission
Dynamic Connector JSON proxy
per-account MPSC
CommandJournal
```

被替换而非删除的组件：

| 原插件组件 | 新核心服务 | 保留的主要能力 | 删除的机制 |
|---|---|---|---|
| AccountPlugin | AccountService | 账户连接、快照、private WS、账户事实 | Plugin lifecycle/export、命令 endpoint |
| ConnectorPlugin / venue cdylib | 静态 connector factory/实现，归入 AccountService/MarketService | REST、market WS、private WS | 独立服务层、动态装载、JSON/C ABI、manifest |
| StrategyPlugin | StrategyService | 策略实例、lane、回调、隔离 | Plugin lifecycle、command outbox/drain |
| MarketPlugin | MarketService | 行情订阅、标准化、行情事实 | Plugin lifecycle/export、动态 service lookup |

## 5. EventEngine 边界

EventEngine 继续处理：

- depth/trade/BBO/bar；
- private WS 的 order/fill/position/balance；
- connector connection/health 事实；
- strategy timer；
- 每策略 lane 串行、QoS 和 backpressure。

EventEngine 不处理下单命令、REST completion、命令排队、账户路由、风控、订单所有权、幂等、
retry/reconcile 或 OMS 状态。只有交易所 private WS 产生的 Order/Fill/Position/Balance 等账户事实进入
EventEngine。

## 6. 最小错误语义

| 阶段 | 错误 | 处理 |
|---|---|---|
| spawn 前 | RuntimeStopping/InvalidBinding | 同步返回，不自动停止策略 |
| REST 完成 | 明确拒绝 | ExecutionObserver 记录 Rejected |
| REST 完成 | Transport/408/425/429/5xx | ExecutionObserver 记录 Unknown |

统一规则：不自动重试、不自动 get_order、不改变 account ready、不 reconcile，也不从 executor 线程直接
重入策略。策略状态只由 EventEngine 交付的行情、private WS 账户事实和 connector health 驱动。Unknown 必须
在运维观测中明确标识，不能伪装成 Rejected。

## 7. 未来 OMS 接入点

```text
当前：Strategy -> ExecutionHandle -> DirectExecutionConnector
未来：Strategy -> OmsClient       -> OMS -> Connector
```

冻结 `ExecutionRequest` 和 account event 契约。未来 OMS 接入时，再定义稳定的 OmsCommandResult 事实并通过
EventEngine 交付策略；当前临时的 ObservedExecutionResult 不是策略业务契约。
OMS 接管：

- 权限、账户/资产/venue route 和 risk；
- command/client order ID 与幂等 journal；
- owned/pending order；
- per-account serialization、bounded queue、rate limit 和优先级；
- submit/cancel ordering；
- outcome unknown 查询和 reconcile；
- shutdown cancel policy；
- 账户隔离和故障恢复。

禁止向当前 ExecutionHandle 逐步加入这些功能，避免 Runtime 再次演化成 OMS。

## 8. 代码改动范围

### Core 和配置

- `crates/titan-event-engine/src/core.rs`：移除 PluginEngine ownership；
- 将 `TraceContext` 等必要小类型迁移到 `titan-core-types`；
- `crates/titan-cli/src/core_runtime.rs`：直接构造 TradingRuntime、各核心服务和静态 connector；
- 删除 `plugins`、`connector_plugin_packages` 配置；
- 增加 execution runtime 线程数、shutdown deadline 和观测配置。

### Strategy

- `titan-strategy-plugin` 改为 `titan-strategy-runtime`，其业务主体改为 `StrategyService`；
- 保留策略实例管理、事件 lane、回调隔离和显式启停；
- 删除 StrategyCommandGateway、CommandGate、owned/pending metadata；
- 删除 command buffer 和 callback 后 drain；
- 原生策略直接持有 ExecutionHandle；
- Numba 如保留则增加 host execution function table；
- execution 错误不再统一转成 Strategy Failed。

### Account 和 Connector

- `titan-account-plugin` 改为 `titan-account-service`（或过渡期保留 crate 名但移除 Plugin 依赖）；
- `AccountServiceCore` 改为普通 `AccountService`，保留账户连接、查询、快照和 private WS 事实发布；
- 删除旧命令入口 AccountExecutionService、AccountExecutionRequest/Response 和 ExecutionEndpoint；
- 删除 AccountRuntime command sender/receiver、journal、reconcile scheduler，但不删除账户领域服务；
- Connector plugin factory/loader 改为构造期静态 factory/catalog；factory 产物由 AccountService 或
  MarketService 直接持有；
- BrokerApi Future 直接由共享 execution runtime 驱动；
- private WS 继续发布账户事实，REST completion 只进入 ExecutionObserver；
- 静态 connector factory 取代 dylib factory。

### Market

- `titan-market-plugin` 改为 `titan-market-service`（或过渡期保留 crate 名但移除 Plugin 依赖）；
- 保留行情连接、订阅管理、标准化和向 EventEngine 发布行情事实；
- 删除 Market PluginFactory、manifest、Plugin lifecycle wrapper 和 service export/lookup。

### 删除/退出运行时依赖

- `crates/titan-plugin-engine`；
- `crates/titan-connector-loader`；
- `crates/titan-connector-*-plugin`；
- Account/Market dynamic ABI；
- `connector/src/dynamic_plugin.rs`；
- Strategy/Account/Market/Connector 的 PluginFactory、manifest、Plugin lifecycle wrapper 和 service adapter。

先从 runtime dependency graph 移除，确认无使用方后再物理删除 crate，降低一次性变更风险。

## 9. 迁移阶段

1. **冻结基线**：记录事件、REST request、账户事件 golden trace；测量 event→callback、callback→REST begin。
2. **核心服务骨架**：建立 TradingRuntime，将 Account/Market/Strategy Plugin 迁移为普通核心服务，并把
   Connector Plugin 迁移为由 AccountService/MarketService 持有的静态实现，保持业务行为不变。
3. **静态 Connector**：移除动态 JSON C ABI，保持旧命令流以验证 request/event 等价。
4. **Direct ExecutionHandle**：实现共享 executor、直接绑定 connector 和只观测 REST 结果的 observer。
5. **删除旧命令流**：迁移 Rust/Numba 策略，删除 Outbox、Gateway、ExecutionEndpoint、MPSC、journal、reconcile。
6. **删除 Plugin 基础设施**：移除 PluginEngine、ServiceRegistry 和各 Plugin adapter，EventEngine 与
   PluginEngine 解耦。
7. **清理和 OMS 契约**：冻结执行与事件 contract，删除旧 crate、配置、部署和 ABI 测试。

每阶段用 feature flag 保留新旧路径对照，正确性和故障测试通过后再删除旧路径。

### 9.1 实施任务分解与完成状态

本次迁移按可独立验证的工作包执行，最终代码不保留双路径 feature flag：

| 工作包 | 交付物 | 状态 |
|---|---|---|
| A. 静态装配 | `TradingRuntime`、构造期 `ConnectorCatalog`、显式启停顺序 | 完成 |
| B. Connector 静态化 | 删除 loader、venue cdylib wrapper、dynamic connector ABI 与部署配置 | 完成 |
| C. 直接执行 | 共享 Tokio runtime、`ExecutionHandle`、observer、active-task 上限与 shutdown gate | 完成 |
| D. Strategy ABI | ABI v12 host submit/cancel；回测命令缓冲独立为 `BacktestCommandBuffer`，live callback 不再暴露扁平 command 字段 | 完成 |
| E. 旧命令流删除 | 删除 Gateway、AccountExecution endpoint、command MPSC/journal/reconcile 与命令模型 | 完成 |
| F. 核心服务化 | crate 改名为 `titan-account-service`、`titan-market-service`、`titan-strategy-runtime` | 完成 |
| G. 插件基础设施删除 | `titan-plugin-engine` 替换为精简 `titan-core-types`，删除 factory/manifest/registry/lifecycle | 完成 |
| H. 事实事件收口 | 保留 Order/Fill/Position/Balance 与 stream health；删除 CommandResult/Reconcile 事实 | 完成 |
| I. 可观测与故障语义 | Rejected/Unknown 分流，spawn P50/P99/P99.9/max、分配和 active task 指标 | 完成 |
| J. 验证与文档 | workspace check、测试目标构建、全量测试、依赖图与禁用符号扫描 | 完成 |
| K. Review 修复 | 账户 bootstrap/ready gate、事实缓存、execution permit RAII、聚合关停错误、迁移别名清理 | 完成 |

Backtest runtime 为保持历史回测语义，仍可通过 ABI v12 的独立 `BacktestCommandBuffer` 使用内部
`OrderCommand` 缓冲；live `TradingRuntime`/`StrategyService` 将该指针保持为空，只提供异步 host
submit/cancel，因此两种执行语义不再共享扁平字段。账户启动现在必须依次完成配置校验、REST
Order/Position/Balance 快照装载和 private stream ready，之后才进入 Ready 并允许策略启动；启动期间到达的
private facts 会先缓存并在快照后重放，避免快照覆盖更新。

### 9.2 验证记录

2026-09-14 在编译服务器 `192.168.3.88` 的非实盘工作区完成：

- `cargo test --workspace`：435 passed、13 ignored、0 failed（包含 doctest）；
- Python SDK/策略 `unittest`：9 passed、0 failed；
- Numba ABI v12：固定布局 host submit/cancel 与独立回测 command buffer 验证通过；
- Account bootstrap 单测：配置校验、快照缓存和 Ready 最后发布通过；
- Execution future 构造 panic 的 active permit 释放测试通过；
- `titan-cli` 依赖树中无 plugin engine、connector loader 或 venue plugin crate；
- 生产 Rust 源码禁用符号扫描无 PluginEngine、PluginFactory、PluginBundle、ServiceRegistry、Gateway、
  AccountExecution endpoint、CommandJournal、ReconcileScope 或 CommandResult；
- private WS Position publication 到 Account fact 的编码测试通过；
- 未执行实盘账户连接、下单、撤单或长期策略运行。

## 10. 验收标准

- 运行时依赖图不存在 PluginEngine；
- Account、Strategy、Market 由 TradingRuntime 作为普通核心服务直接管理；Connector 是
  AccountService/MarketService 内部持有的静态实现，不是独立服务；
- 运行时不存在 PluginFactory、PluginBundle、manifest、service export/lookup；
- 实盘命令路径不存在 OrderCommand buffer、Gateway、AccountExecution Service、per-account MPSC 或 JSON/C ABI；
- EventEngine 只接收事实事件，不接收命令；
- submit 不等待 REST，REST completion 不重入策略；
- Rejected 和 Unknown 在日志、指标和 trace 中明确区分；
- 单个 execution task 失败不自动关闭策略；
- shutdown 后拒绝新 task，并在 deadline 内处理未完成 task；
- private WS 账户事件继续到达策略；
- 旧/新路径的交易所 request 字段和事件 payload 等价；
- callback→Future spawn 报告 P50/P99/P99.9、分配次数和 active task 数。

## 11. 最低安全边界

完全不设限制地 `spawn` 会在交易所变慢时耗尽内存，因此即使不实现 OMS，也必须保留两项进程安全措施：

1. shutdown admission gate，停机时禁止创建新 task；
2. 进程级 active-task 硬上限，超过后返回 `ExecutorSaturated`。

它们不是 per-account 队列，不提供公平、顺序、重试或账户隔离，只防止进程失控。

该方案是有意的阶段性简化。在真实资金环境启用前，独立 OMS 必须补齐风险、幂等、排序、背压、恢复和订单
生命周期管理。
