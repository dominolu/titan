# Titan AccountPlugin 技术实现设计

版本：v0.1

状态：设计基线，待实现

关联文档：

- [MarketPlugin 技术实现设计](market_plugin_technical_design.md)
- [Core Runtime交互契约](core_runtime_contract.md)
- [EventEngine独立技术实现设计](event_engine_technical_design.md)
- [PluginEngine独立技术实现设计](plugin_engine_technical_design.md)

## 1. 设计目标

AccountPlugin 是完整 AccountConnector 实例的创建器、注册表和 Service 门面，用于管理交易账户实例，
并把账户事实可靠、低延迟地交付给 EventEngine。

AccountConnector 已经负责私有账户的全部交易所实现，包括认证、私有流、REST、下单、撤单、改单、
订单状态合流、成交去重、余额/持仓恢复、重连和标准化。AccountPlugin 不重复实现这些能力。

目标数据路径：

```text
命令路径：Strategy/Risk -> AccountExecutionService -> AccountConnector -> Exchange

事实路径：Exchange -> AccountConnector -> EventPublisher -> EventEngine -> Strategy/Risk/Store
```

AccountPlugin 不位于订单回报、成交、余额或持仓 payload 的数据路径中。

V1 目标：

- 同一进程管理多个交易所、主账户和子账户实例；
- 账户定义、凭据引用和 Connector 生命周期彼此隔离；
- 订单命令立即得到本地接收结果，最终结果通过事件异步返回；
- 私有 WebSocket 与 REST 响应由具体 Connector 合并成单一账户事实流；
- 重连后先完成权威 reconciliation，再发布 READY；
- 标准 ABI 使用稳定 ID 和整数单位，不在热路径传递字符串或 `f64`；
- AccountPlugin 停止时有界停止 Connector，并确保事件块、任务和凭据资源全部释放。

## 2. 最终职责边界

### 2.1 AccountPlugin 负责

- 注册 `AccountConnectorFactory`；
- 根据 `AccountDefinition` 创建 AccountConnector 实例；
- 保存 `AccountHandle` 到 Connector 实例的映射；
- 启动、停止、替换和删除账户实例；
- 为账户实例分配隔离的 account event lane 和 control lane；
- 将 PluginContext 提供的受限 EventPublisher、ChildResourceScope 和 SecretResolver 传给 Connector；
- 通过 Service 暴露执行命令、快照查询、reconcile 和管理接口；
- 汇总 Connector 自己提供的状态和诊断快照；
- 校验 account key、AccountId、AssetId/CurrencyId binding 和插件级容量冲突；
- 在插件停止时拒绝新命令，并确保所有 Connector 已停止和释放。

### 2.2 AccountConnector 负责

- API key、签名、nonce、时钟偏差、代理、REST、私有 WebSocket 和 listen key；
- 下单、撤单、改单、批量命令和交易所 ACK；
- client order id 生成/映射、幂等、订单状态机和 REST/WS 到达乱序合并；
- 过滤不属于本账户命名空间的订单，处理外部订单的显式纳管策略；
- Fill 去重、累计成交量校验和 fee/realized PnL 标准化；
- 余额、持仓、订单的 Snapshot/Delta、epoch、sequence/version、缺口和恢复；
- 启动与重连时的 REST Snapshot 和私有流增量衔接；
- 交易所 symbol、currency、价格、数量、金额、时间戳和枚举标准化；
- 维护权威的本地 open orders、positions、balances 查询快照；
- 直接通过 EventPublisher 向 EventEngine 发布账户事实；
- QueueFull、发布失败、账户流失效和 reconciliation 处置；
- 自身线程、Task、Timer、队列、网络连接和敏感凭据的停止与清理。

### 2.3 AccountPlugin 不负责

- 交易所签名、nonce、listen key 和重连协议；
- 通用 OMS、订单状态机或订单 ID 映射；
- 合并 REST 下单响应与私有 WebSocket 回报；
- Fill 去重或累计成交量计算；
- 余额、持仓、订单的 Snapshot/Delta 拼接；
- account epoch、交易所 sequence、version 或 gap 检测；
- 仓位、保证金、PnL、手续费或清算价格的交易所计算；
- 策略风险、组合风险、限额审批或资金分配；
- 自动重试不具有明确幂等语义的下单命令；
- 缓存或转发高频账户 payload；
- 直接操作 EventEngine 路由表或 SubscriberChannel；
- 将公共 MarketConnector 与私有 AccountConnector 合并为同一实例。

RiskPlugin 决定“是否允许发送”，AccountConnector 决定“如何正确发送并解释交易所结果”。
AccountPlugin 只负责找到正确的账户实例并委托命令。

## 3. 架构

```text
PluginEngine
    -> AccountPlugin
        -> AccountConnectorFactoryRegistry
        -> AccountRegistry
            -> BinanceFuturesAccountConnector
            -> OkxAccountConnector
            -> HyperliquidAccountConnector
        -> AccountAdminService
        -> AccountService
        -> AccountExecutionService

Strategy/Risk -> AccountExecutionService -> AccountConnector -> Exchange
Exchange -> AccountConnector -> AccountEventPublisher -> EventEngine
```

AccountPlugin 只有控制和命令路径调用：

```text
create / get / start / stop / remove / replace / list
submit / amend / cancel / cancel_all / cancel_all_after
reconcile / orders / positions / balances / health / diagnostics
```

订单回报、成交、余额和持仓事件均从 Connector 直接进入 EventEngine。

## 4. 公共类型

```rust
#[repr(transparent)]
pub struct AccountId(pub u32);

#[repr(transparent)]
pub struct AssetId(pub u32);

#[repr(transparent)]
pub struct CurrencyId(pub u32);

pub struct AccountHandle {
    pub account_id: AccountId,
    pub generation: u64,
}

pub struct AccountInstrumentBinding {
    pub native_symbol: Arc<str>,
    pub asset_id: AssetId,
    pub price_tick: DecimalUnit,
    pub quantity_lot: DecimalUnit,
    pub contract_multiplier: DecimalUnit,
}

pub struct AccountCurrencyBinding {
    pub native_currency: Arc<str>,
    pub currency_id: CurrencyId,
    pub amount_unit: DecimalUnit,
}

pub struct AccountDefinition {
    pub account_key: Arc<str>,
    pub connector_type: Arc<str>,
    pub credential_ref: SecretRef,
    pub connector_config: Arc<[u8]>,
    pub instruments: Arc<[AccountInstrumentBinding]>,
    pub currencies: Arc<[AccountCurrencyBinding]>,
    pub ownership: OrderOwnershipPolicy,
    pub enabled: bool,
    pub definition_version: u64,
}
```

`connector_config` 对 AccountPlugin 不透明，由具体 Factory 校验。`credential_ref` 只引用外部 Secret，
账户定义、日志、诊断和 Snapshot 中不得出现 secret 内容。SecretResolver 返回的 secret 只在 Connector
私有资源域内存在，停止时清理；SecretResolver 不允许 Connector 枚举其他账户凭据。

`DecimalUnit` 是经过校验的十进制定点单位。标准事件中的价格、数量和金额均使用 `i64` 整数单位，
不得在账户热路径使用 `f64`，也不得使用与 instrument/currency 无关的固定倍率。

删除并重新创建同一 `account_key` 时必须增加 generation。旧 Handle 返回 `StaleHandle`，不能命中新
账户实例。`AccountId` 是 EventEngine 的稳定路由键；一个 AccountId 同一时刻只能属于一个 generation。

`OrderOwnershipPolicy` 至少区分：

```rust
pub enum OrderOwnershipPolicy {
    ManagedOnly { client_id_prefix: Arc<str> },
    ObserveAll,
}
```

V1 默认 `ManagedOnly`。外部订单不能被静默丢弃后仍声明账户完整；Connector 必须将其计入诊断，或在
`ObserveAll` 下发布为 `EXTERNAL` 订单。

## 5. Connector 接口

### 5.1 Factory

```rust
pub trait AccountConnectorFactory: Send + Sync {
    fn connector_type(&self) -> &str;

    fn create(
        &self,
        definition: &AccountDefinition,
        context: AccountConnectorContext,
    ) -> Result<Arc<dyn AccountConnector>, AccountConnectorError>;
}
```

Factory 负责解析 connector_config、校验交易所能力和 binding，并通过受限 SecretResolver 获取凭据。
AccountPlugin 只检查 factory 是否注册、稳定 ID 冲突和插件级容量。

### 5.2 Connector

```rust
pub trait AccountConnector: Send + Sync {
    fn start(&self) -> Result<(), AccountConnectorError>;
    fn stop(&self, deadline: Instant) -> Result<(), AccountConnectorError>;

    fn submit(&self, command: SubmitOrderCommand)
        -> LocalResult<AccountCommandReceipt>;
    fn amend(&self, command: AmendOrderCommand)
        -> LocalResult<AccountCommandReceipt>;
    fn cancel(&self, command: CancelOrderCommand)
        -> LocalResult<AccountCommandReceipt>;
    fn cancel_all(&self, command: CancelAllCommand)
        -> LocalResult<AccountCommandReceipt>;
    fn cancel_all_after(&self, command: CancelAllAfterCommand)
        -> LocalResult<AccountCommandReceipt>;

    fn reconcile(&self, scope: ReconcileScope)
        -> LocalResult<OperationId>;

    fn orders(&self, filter: OrderFilter)
        -> Result<AccountStateSnapshot<OrderSnapshot>, AccountConnectorError>;
    fn positions(&self, filter: PositionFilter)
        -> Result<AccountStateSnapshot<PositionSnapshot>, AccountConnectorError>;
    fn balances(&self)
        -> Result<AccountStateSnapshot<BalanceSnapshot>, AccountConnectorError>;
    fn health(&self) -> AccountConnectorHealthSnapshot;
    fn diagnostics(&self) -> AccountConnectorDiagnosticSnapshot;
    fn operation(&self, id: OperationId) -> AccountConnectorOperationSnapshot;
}
```

所有执行方法只完成本地校验和有界 command queue admission，不能等待 DNS、网络或交易所响应。
成功返回 `AccountCommandReceipt` 只表示 Connector 已接收命令，不表示交易所接受订单。最终接受、拒绝、
成交或撤单结果必须通过标准账户事件发布。

```rust
pub struct AccountCommandReceipt {
    pub account: AccountHandle,
    pub command_id: CommandId,
    pub client_order_id: Option<ClientOrderId>,
    pub accepted_at: i64,
}
```

`command_id` 必须由调用方提供或在进入 Connector 前确定，用于端到端关联和重复 admission 检测。
Connector 负责将其映射为交易所 client order id。对于网络结果不确定的 submit，禁止 AccountPlugin
自动重试；Connector 只能依据交易所幂等能力和 client order id 查询/恢复。

查询接口返回 Connector 持有的不可变已提交快照，不触发同步 REST 调用。需要刷新时使用 `reconcile`
并通过 Operation/事件观察结果。快照封装显式携带一致性状态：

```rust
pub enum AccountSnapshotState {
    Ready,
    Reconciling,
    Invalidated,
    Stopped,
}

pub struct AccountStateSnapshot<T> {
    pub account: AccountHandle,
    pub state: AccountSnapshotState,
    pub committed_epoch: Option<u64>,
    pub committed_version: Option<u64>,
    pub captured_at: i64,
    pub items: Arc<[T]>,
}
```

Connector 在 reconcile 期间维护 candidate view，但查询只返回上一份 committed view，并标记
`Reconciling`；首次 reconcile 尚无 committed view 时返回空 items、`None` epoch/version 和
`Reconciling`。调用方不需要额外调用 `health()` 才能判断快照能否用于交易决策。只有
`ReconcileCompleted` 后 Connector 才原子替换 committed view 并返回 `Ready`。

### 5.3 Connector Context

```rust
pub struct AccountConnectorContext {
    pub account: AccountHandle,
    pub instruments: Arc<[AccountInstrumentBinding]>,
    pub currencies: Arc<[AccountCurrencyBinding]>,
    pub account_stream: SourceStreamId,
    pub control_stream: SourceStreamId,
    pub event_publisher: AccountEventPublisher,
    pub resources: ChildResourceScope,
    pub secrets: ScopedSecretResolver,
}
```

受限能力要求：

- Publisher 只能发布 AccountPlugin Manifest 声明的标准账户事件；
- Connector 不能注册 EventType、修改路由或获得原始 EventEngineHandle；
- account 和 control 使用独立 SourceStreamId；`CommandResult` 与 Order/Fill 使用同一 account lane，
  避免人为引入跨 lane 重排；
- 所有账户事实的 routing key 统一使用 AccountId，使 Consumer 一次路由即可得到该账户的完整事实流；
  instrument 级事件把 AssetId 放在 payload 中，V1 不用复合 key 拆散账户流；
- Publisher 在插件未 ACTIVE 时拒绝发布；quiesce 期间保持有效直到 Connector.stop 返回；
- Publisher 还具有 per-account admission gate，并从 Context 固定注入/校验 AccountId、generation 和
  routing key；Connector 不能伪造另一个账户或 generation；
- SecretResolver 只能解析当前定义中的 credential_ref，返回内容不能进入错误 Display、日志或事件；
- Connector 的所有 Task、Timer、线程和队列必须注册到 ChildResourceScope。

EventEngine metadata `source_sequence` 由受限 Publisher 在各 publication lane 内串行分配，只在发布成功
后提交。它不等于交易所 sequence，也不能用于替代账户 reconciliation version。

## 6. Service

### 6.1 AccountAdminService

```rust
pub trait AccountAdminService: Send + Sync {
    fn create(&self, definition: AccountDefinition)
        -> LocalResult<AccountHandle>;
    fn start(&self, account: AccountHandle)
        -> LocalResult<OperationId>;
    fn stop(&self, account: AccountHandle, deadline: Instant)
        -> LocalResult<OperationId>;
    fn remove(&self, account: AccountHandle)
        -> LocalResult<OperationId>;
    fn replace(&self, account: AccountHandle, definition: AccountDefinition)
        -> LocalResult<AccountHandle>;
    fn reconcile(&self, account: AccountHandle, scope: ReconcileScope)
        -> LocalResult<OperationId>;
    fn list(&self) -> Arc<[AccountInstanceSnapshot]>;
    fn operation(&self, id: OperationId) -> AccountOperationSnapshot;
}
```

`replace` 必须使用新 generation。涉及凭据、交易所账户或订单 ownership 变化时，V1 先 prepare 新
generation，再 quiesce/stop 旧 generation，随后 reconcile 并激活新 generation；不允许两个 generation
同时向同一 AccountId 发布。

### 6.2 AccountService

```rust
pub trait AccountService: Send + Sync {
    fn resolve(&self, account_key: &str) -> LocalResult<AccountHandle>;
    fn orders(&self, account: AccountHandle, filter: OrderFilter)
        -> LocalResult<AccountStateSnapshot<OrderSnapshot>>;
    fn positions(&self, account: AccountHandle, filter: PositionFilter)
        -> LocalResult<AccountStateSnapshot<PositionSnapshot>>;
    fn balances(&self, account: AccountHandle)
        -> LocalResult<AccountStateSnapshot<BalanceSnapshot>>;
    fn health(&self, account: AccountHandle)
        -> LocalResult<AccountConnectorHealthSnapshot>;
    fn diagnostics(&self, account: AccountHandle)
        -> LocalResult<AccountConnectorDiagnosticSnapshot>;
}
```

Service 查找 Connector 后直接委托，不修改、聚合或重新计算账户数据，也不返回完整 Connector trait
object。查询是控制面观察能力；策略的连续状态应由 EventEngine 事件建立，不应逐 tick 调 Service。

### 6.3 AccountExecutionService

```rust
pub trait AccountExecutionService: Send + Sync {
    fn submit(&self, account: AccountHandle, command: SubmitOrderCommand)
        -> LocalResult<AccountCommandReceipt>;
    fn amend(&self, account: AccountHandle, command: AmendOrderCommand)
        -> LocalResult<AccountCommandReceipt>;
    fn cancel(&self, account: AccountHandle, command: CancelOrderCommand)
        -> LocalResult<AccountCommandReceipt>;
    fn cancel_all(&self, account: AccountHandle, command: CancelAllCommand)
        -> LocalResult<AccountCommandReceipt>;
    fn cancel_all_after(
        &self,
        account: AccountHandle,
        command: CancelAllAfterCommand,
    ) -> LocalResult<AccountCommandReceipt>;
}
```

Endpoint 只做 generation/状态校验和一次 Connector 调用。不得经过 PluginEngine owner、异步 executor、
通用 JSON RPC 或无界 MPSC。Connector 的 command queue 必须有界；QueueFull 立即返回调用方。

RiskPlugin 应在调用 ExecutionService 前完成业务风险检查。AccountPlugin 只执行不可绕过的结构校验：
有效 Handle、已绑定 AssetId、整数单位可表示、command_id 格式、Connector 当前是否接收命令。

## 7. AccountRegistry

```rust
struct AccountEntry {
    handle: AccountHandle,
    definition_version: u64,
    connector: Arc<dyn AccountConnector>,
    state: AtomicAccountLifecycle,
}

struct AccountRegistry {
    state: RwLock<AccountRegistryState>,
}

struct AccountRegistryState {
    by_id: HashMap<AccountId, Arc<AccountEntry>>,
    by_key: HashMap<Arc<str>, AccountHandle>,
}
```

Registry 只保存实例和生命周期元数据，不保存订单、成交、持仓、余额、交易所 sequence、command queue
或 reconciliation 状态机。

Registry 不在账户事件热路径。执行命令路径只短暂读锁并 clone `Arc<AccountEntry>`，释放 Registry 锁后
调用 Connector。create/remove/replace 由 AccountPlugin 控制 owner 串行提交。

## 8. 标准事件与 ABI

AccountPlugin Manifest 授权 Connector 发布：

```text
titan.account.OrderChanged@1
titan.account.Fill@1
titan.account.PositionChanged@1
titan.account.BalanceChanged@1
titan.account.CommandResult@1
titan.account.ReconcileStarted@1
titan.account.ReconcileCompleted@1
titan.account.StreamStateChanged@1
titan.account.StreamInvalidated@1
```

所有 ABI 明确 little-endian 编码，不依赖 Rust struct padding。公共 header：

```rust
pub struct AccountEventHeaderV1 {
    pub account_id: u32,
    pub kind: u16,
    pub flags: u16,
    pub account_generation: u64,
    pub account_epoch: u64,
    pub account_version: u64,
    pub exchange_ts: i64,
    pub receive_ts: i64,
}
```

`account_generation` 来自 `AccountHandle`，使 Consumer 能识别 replace 前已经进入 SubscriberChannel 的
旧事件；它不由 Connector 自行递增。`flags` 至少包含 `SNAPSHOT`、`UPSERT`、`DELETE`、`EXTERNAL`、
`FINAL` 和 `SYNTHETIC`。

核心 payload 字段：

```rust
pub struct OrderChangedV1 {
    pub header: AccountEventHeaderV1,
    pub asset_id: u32,
    pub side: u8,
    pub order_type: u8,
    pub time_in_force: u8,
    pub status: u8,
    pub price_ticks: i64,
    pub quantity_lots: i64,
    pub filled_quantity_lots: i64,
    pub average_price_ticks: i64,
    pub client_order_id: Id128,
    pub venue_order_id: Id128,
    pub command_id: Id128,
}

pub struct FillV1 {
    pub header: AccountEventHeaderV1,
    pub asset_id: u32,
    pub side: u8,
    pub liquidity: u8,
    pub price_ticks: i64,
    pub quantity_lots: i64,
    pub fee_amount_units: i64,
    pub fee_currency_id: u32,
    pub realized_pnl_units: i64,
    pub trade_id: Id128,
    pub venue_order_id: Id128,
    pub client_order_id: Id128,
    pub command_id: Id128,
}

pub struct PositionChangedV1 {
    pub header: AccountEventHeaderV1,
    pub asset_id: u32,
    pub position_side: u8,
    pub margin_type: u8,
    pub quantity_lots: i64,
    pub entry_price_ticks: i64,
    pub liquidation_price_ticks: i64,
    pub realized_pnl_units: i64,
    pub unrealized_pnl_units: i64,
    pub margin_currency_id: u32,
}

pub struct BalanceChangedV1 {
    pub header: AccountEventHeaderV1,
    pub currency_id: u32,
    pub wallet_units: i64,
    pub available_units: i64,
    pub margin_units: i64,
    pub unrealized_pnl_units: i64,
}
```

交易所字符串 ID 必须在 Connector 边界解析为稳定 `Id128`。V1 可以采用“长度 + 120 bit 原始字节”
或受控 interner；禁止仅发布无碰撞处理的 64 bit hash。超长或不能无损表示的 ID 由 Connector 的
per-account ID interner 映射，并提供诊断查询。

订单状态只能单调推进到 Connector 定义的更新 version；迟到的 REST/WS 回报不能使 `FILLED` 回退为
`NEW`。该单调性由 Connector 的 OrderManager 保证，不由 AccountPlugin 或 Consumer 修复。

## 9. Reconciliation 与 READY 语义

账户数据不是普通无状态消息。Connector 必须把启动和重连恢复建模为显式 epoch：

```text
CONNECTING
    -> AUTHENTICATING
    -> RECONCILING
         1. 建立/缓冲私有流
         2. 获取权威 orders/positions/balances Snapshot
         3. 按交易所规则合并 Snapshot 窗口内增量
         4. 发布 ReconcileStarted(epoch)
         5. 发布各类 SNAPSHOT UPSERT/DELETE
         6. 发布 ReconcileCompleted(epoch, terminal_version)
    -> READY
```

不同交易所的正确步骤可能是“先 WS 后 REST”“先 REST 后 WS”或依赖特定 sequence token；顺序完全由
具体 Connector 实现，AccountPlugin 不提供通用 Snapshot/Delta 算法。

`account_epoch` 在每次无法证明连续性时增加。`account_version` 是 Connector 在一个 epoch 内为标准账户
事实分配的严格单调版本。若交易所提供可靠 sequence，Connector 将其纳入验证；若未提供，则 Connector
使用订单/成交唯一键、更新时间、累计量和权威 Snapshot 建立本地版本。AccountPlugin 不生成这两个值。

Consumer 规则：

- 收到 `StreamInvalidated` 后停止把本地账户视为 READY；
- 丢弃旧 generation 或旧 epoch 的事件；
- 在 `ReconcileCompleted` 前只构建候选状态，不允许发起依赖完整账户状态的新风险决策；
- `ReconcileCompleted` 后原子替换本地 orders/positions/balances view；
- Subscriber gap 或 QueueFull 后调用 `reconcile(account, Full)`，不能请求 AccountPlugin 返回事件 payload。

账户 READY 只表示私有流连续、Snapshot 已合并且 Connector 可接受命令，不表示余额充足、风险通过或
交易所一定接受下一笔订单。

## 10. 命令一致性与订单生命周期

### 10.1 Submit 时序

```text
Strategy生成command_id/client_order_id
    -> RiskPlugin批准
    -> AccountExecutionService.submit
    -> Connector有界队列admission
    -> 返回AccountCommandReceipt
    -> Connector签名并发送
    -> REST ACK和私有WS任意顺序到达
    -> Connector OrderManager去重、合并、单调推进
    -> 发布CommandResult和OrderChanged/Fill
```

`CommandResult` 表示命令处理结果，`OrderChanged` 表示订单事实，两者不能互相替代。可能出现 REST 超时
但私有流确认订单已创建；Connector 必须继续按 client order id reconcile，不能立即发布最终 Reject。

### 10.2 幂等

- 同一 AccountId 内 `command_id` 唯一；
- 相同 command_id 和相同内容重复调用返回相同 receipt；
- 相同 command_id 但内容不同返回 `CommandConflict`；
- client order id 必须在交易所允许范围内稳定映射；
- amend/cancel 必须携带可解析的 venue order id 或 client order id；
- 网络结果不确定时进入 `UNKNOWN/RECONCILING`，不自动生成新 client order id 重发。

Connector 可以维护有界 command journal 处理短期重试；持久化恢复属于可选 StorePlugin 集成，不放入
AccountPlugin Registry。若系统要求进程崩溃后的 exactly-once，下单前必须引入持久化 command journal，
不属于 V1 的内存级保证。

### 10.3 背压

- command queue 满：同步返回 `QueueFull`，命令未被接收；
- EventPublisher QueueFull：Connector 将账户流置为 INVALIDATED，停止声明 READY，触发 reconcile；
- 禁止阻塞 publisher 等待慢 Consumer；
- Critical 账户事件不能“丢一条后继续”形成静默状态偏差；
- 审计 Store 可以独立订阅正常 EventEngine route，不能阻塞交易 Connector。

## 11. 启动、停止与替换

### 11.1 创建

```text
AccountAdminService.create
    -> 查找AccountConnectorFactory
    -> 校验AccountId/key和binding容量/冲突
    -> 校验credential_ref可解析但不读取明文到Plugin状态
    -> 分配account/control SourceStreamId
    -> 创建ChildResourceScope和受限Publisher/SecretResolver
    -> Factory.create(definition, context)
    -> 写入AccountRegistry
    -> 返回AccountHandle
```

失败时按相反顺序回滚，不留下 Registry 条目、secret lease 或 publication lane。

### 11.2 启动

`start()` 成功只表示 Connector 本地任务已创建。账户实例经过 CONNECTING、RECONCILING 到 READY 的
状态通过 health 和标准控制事件观察。AccountPlugin 不等待登录、REST Snapshot 或私有流 ACK。

事件路由必须在 start 前提交，避免初始 Snapshot 对 Consumer 不可见。调用方顺序：

```text
创建账户事件路由 -> AccountAdminService.start -> 等待ReconcileCompleted/READY -> 启动策略
```

### 11.3 停止

```text
AccountPlugin.quiesce
    -> 拒绝create/start/submit/amend入口
    -> 保留cancel/cancel_all安全入口直到stop policy决定关闭
    -> 对每个Connector调用stop(deadline)
    -> Connector停止接收普通命令
    -> 可选执行cancel_all_after或cancel_all shutdown policy
    -> 停止私有流、REST任务和新事件发布
    -> 等待ChildResourceScope任务退出
    -> ActivationGate等待已有PublishPermit归还

AccountPlugin.stop
    -> 使所有AccountHandle失效
    -> 清空Registry
    -> 释放secret lease、ResourceScope和publication lane
```

是否在停止时撤单必须由账户定义中的 `ShutdownOrderPolicy` 明确指定，不能由 AccountPlugin 隐式决定。
默认建议 `CancelAllAfter(deadline)`，但实盘配置必须显式确认。停止失败需记录并继续有界回收，不能无限
等待；仍可能存在交易所活动订单时必须产生高优先级诊断。

### 11.4 替换

账户替换不能像无状态配置一样直接热切：

1. 校验新 Definition，创建 publisher gate 关闭的新 generation candidate，并暂存所需新路由；
2. 旧 generation 拒绝新 submit/amend，但继续收敛已接收命令和账户事实；
3. 依据 shutdown policy 处理旧活动订单，停止旧 Connector 并关闭旧 publisher admission；
4. 启动新 Connector，在 candidate view 中完成全量 reconcile，此时仍禁止对外发布；
5. 若路由发生变化，在 EventLoop safe point commit RouteTransaction；
6. 原子更新 Registry handle，打开新 publisher admission，发布新 generation 的完整 reconcile 序列和
   READY；
7. 退休被替换的旧 SubscriptionToken（若有）、旧 SourceStreamId 和旧 Connector，释放暂存事务资源；
   Consumer 只接受新 `account_generation`。

路由的暂存、安全点提交、旧版本退休和失败回滚完全遵循
[Core Runtime交互契约 §5“路由事务”](core_runtime_contract.md#5-路由事务)以及
[§7“插件装配与激活”](core_runtime_contract.md#7-插件装配与激活)，本文不重复定义 RouteTransaction。
动态账户路由由持有 ScopedEventRouter 的 Consumer Runtime 提交，AccountPlugin 不直接修改
EventEngine RouteTable。

同一 AccountId replace 前后 routing key 不变时，Consumer 的 SubscriptionToken 可以继续复用；切换
动作是关闭旧 publisher admission，再启用新 generation publisher。已经进入队列的旧事件由 ABI 中的
`account_generation` 识别并丢弃。若 Consumer、QoS、容量或 routing key 发生变化，则必须先 stage 新
订阅并在 EventLoop safe point commit，成功后才能退休旧 SubscriptionToken。

在旧 Connector 停止前发生 prepare/stage 失败时，旧 generation 保持有效并回滚 candidate。旧 Connector
停止后新 Connector 启动、reconcile 或 route commit 失败时，账户进入 `INVALIDATED`，新 publisher gate
保持关闭，并按原 Definition 尝试有界恢复旧 generation；恢复也失败则必须显式报告 replace Operation
失败，不能把半初始化的新 generation 暴露为 READY。

V1 不实现同一 AccountId 的双活 Connector。

## 12. 安全与权限

- Secret 只通过引用配置，禁止写入 YAML 明文字段、事件、错误链、Debug、metrics label 和 core dump；
- 每个 Connector 获得独立 ScopedSecretResolver 和 ResourceScope；
- AccountExecutionService 的调用方能力应由 Plugin Manifest 预授权，普通查询插件不能下单；
- `cancel_all`、`cancel_all_after`、设置杠杆等高影响操作使用独立 capability；
- AccountPlugin 不向普通 Consumer 暴露 AccountConnector 或 BrokerApi trait object；
- metrics label 使用 AccountId/connector_type，不使用 API key、用户 ID 或敏感账户别名；
- connector_config 的可打印诊断必须经过 Factory 提供的 redaction；
- 所有命令保留 TraceContext、command_id、AccountId 和调用方 plugin identity，供审计事件关联。

## 13. 配置

```yaml
account_plugin:
  max_accounts: 32
  max_instruments_per_account: 4096
  command_queue_capacity: 8192

accounts:
  - account_key: binance-futures-main
    account_id: 2001
    connector_type: binance-futures-account
    credential_ref: secret://titan/binance-futures/main
    enabled: true
    ownership:
      mode: managed_only
      client_id_prefix: titan-main-
    shutdown_order_policy:
      mode: cancel_all_after
      timeout_ms: 5000
    instruments:
      - native_symbol: BTCUSDT
        asset_id: 1001
        price_tick: "0.1"
        quantity_lot: "0.001"
        contract_multiplier: "1"
    currencies:
      - native_currency: USDT
        currency_id: 10
        amount_unit: "0.00000001"
    connector_config:
      api_url: https://fapi.binance.com
      private_stream_url: wss://fstream.binance.com/ws
```

AccountPlugin 解析账户稳定字段和容量；`connector_config` 由 Factory 解析。配置展示的 URL 仅为格式
示例，不应成为 Connector 内置业务默认值。

## 14. 错误与状态

```rust
pub enum AccountErrorKind {
    FactoryNotFound,
    InvalidDefinition,
    CredentialUnavailable,
    CapacityExceeded,
    AccountNotFound,
    StaleHandle,
    AlreadyExists,
    NotReady,
    CommandConflict,
    QueueFull,
    ConnectorRejected,
    DeadlineExceeded,
    ResourceReleaseFailed,
}
```

交易所错误码、认证、订单状态、sequence 和 reconciliation 错误由 Connector 自己定义，通过 health、
diagnostics、Operation 或 `CommandResult` 暴露。AccountPlugin 不复制交易所错误枚举，也不得把原始
错误文本未经 redaction 放进公共事件。

实例状态：

```text
CREATED -> STARTING -> CONNECTING -> RECONCILING -> READY
                              \-> DEGRADED
                              \-> INVALIDATED -> RECONCILING
READY -> STOPPING -> STOPPED
任意非终态 -> FAILED
```

AccountPlugin 本身 RUNNING 与单个账户 READY 分离。一个账户失败不能导致其他账户或插件整体 FAILED。

## 15. 性能与可靠性要求

账户事实热路径必须是：

```text
Private WS/REST completion -> AccountConnector -> EventPublisher -> EventEngine
```

执行命令热路径必须是：

```text
Strategy/Risk -> prebound AccountExecutionService -> AccountConnector bounded queue
```

必须满足：

- AccountPlugin owner 不接收逐笔账户事件或订单命令；
- 事实发布不经过 AccountRegistry、ServiceRegistry 或二次 bridge MPSC；
- execution endpoint 不做网络 I/O、不等待交易所 ACK；
- Connector 在单一 per-account serialization lane 合并 REST 与 WS 结果；
- payload 在 EventArena 中原位编码，避免中间 JSON/ABI `Vec` 完整复制；
- ID、AssetId、CurrencyId 和 decimal scale 在实例创建时预解析；
- command/event queue 均有界并提供 depth、drop/reject、latency 指标；
- 订单/Fill 使用 Critical QoS；余额/持仓/control 使用 Reliable QoS；
- 同一账户影响顺序的事件进入同一有序 publication lane；
- 所有账户事实只有 EventPublisher -> EventEngine 一个发布出口，禁止旁路分发。

建议测量指标：

```text
command_admission_latency_ns
command_queue_depth / command_queue_reject_total
private_frame_to_publish_latency_ns
account_event_publish_latency_ns
rest_ws_merge_latency_ns
reconcile_duration_ms
reconcile_failure_total
account_epoch_total
account_stream_invalidation_total
unknown_command_total
external_order_total
```

## 16. 测试方案

### 16.1 AccountPlugin 单元测试

- Factory 注册和 connector_type 查找；
- create/resolve/list/remove/replace；
- AccountHandle generation 和 stale handle；
- AccountId、key、AssetId/CurrencyId binding 重复和容量限制；
- credential_ref scope、redaction 和 create 失败回滚；
- Registry 并发读取与控制 owner 串行修改；
- start/stop/replace deadline 和资源释放；
- execution endpoint 只委托一次且不持有 Registry 锁；
- QueueFull、NotReady 和 ConnectorRejected 原样传播。

### 16.2 PluginEngine + EventEngine 集成测试

- 0 Account 时 AccountPlugin 正常 RUNNING；
- FakeConnector 获得受限真实 Publisher 和 SecretResolver；
- FakeConnector 直接发布 Order/Fill/Position/Balance 并按账户路由；
- AccountPlugin owner 未收到 payload；
- 未授权事件、错误 AccountId/SourceStreamId 被拒绝；
- command receipt 与异步 CommandResult/OrderChanged 可关联；
- stop 与 publish/submit 竞争不存在新命令越过 quiesce；
- ResourceScope 关闭后无 Connector、Task、secret lease 或 EventBlock 泄漏；
- Subscriber gap 导致账户失效和 reconcile；
- replace 的 RouteTransaction 安全点切换、旧 SubscriptionToken 退休和旧 generation 队列事件过滤；
- RECONCILING 查询只返回带状态的上一份 committed snapshot，不暴露 candidate view。

### 16.3 Connector 合约测试

Binance Futures、OKX 和 Hyperliquid 分别验证：

- 认证、listen key、私有流重连和凭据 redaction；
- REST Snapshot 与私有流增量衔接；
- account epoch/version 和 gap 恢复；
- REST/WS 响应任意顺序下订单状态单调；
- submit 超时但 WS 成功、REST reject、重复 ACK 和迟到回报；
- partial fill、多 fill、重复 fill 和累计量校验；
- ManagedOnly/ObserveAll 外部订单策略；
- orders/positions/balances Snapshot 的 UPSERT/DELETE 完整性；
- 数字定点转换、溢出和无法表示值；
- EventPublisher QueueFull 后 invalidation/reconcile；
- cancel_all_after 和 shutdown order policy；
- stop deadline、任务、网络、队列与 secret 释放。

### 16.4 故障注入与实盘 Shadow 测试

- 私有 WS 断开、乱序、重复和丢包；
- REST 超时、429、5xx、时钟偏差和 nonce 冲突；
- EventArena/command queue 满；
- Snapshot 期间持续成交；
- Connector 重启时存在活动订单和持仓；
- 使用只读或最小权限账户进行 Shadow reconcile，对比交易所 REST 权威状态；
- 开启极小限额后完成 submit/cancel/partial-fill 全链路；
- 验证事件状态与交易所最终 orders/positions/balances 一致。

## 17. 实施任务拆分

### Phase 1：公共契约与骨架

1. 新建 `titan-account-plugin` crate；
2. 定义 AccountId/Handle/Definition/binding/command/snapshot/error；
3. 定义标准 Account Event ABI、显式 encode/decode 和 schema compatibility tests；
4. 定义 Factory、Connector、Admin/Query/Execution Service trait；
5. 在 Plugin Manifest 声明事件、Service、Secret 和 Resource capability。

### Phase 2：Registry 与生命周期

1. 实现 FactoryRegistry 和 AccountRegistry；
2. 实现 generation、容量、冲突和 definition version 校验；
3. 实现 create/start/stop/remove/replace/reconcile Operation；
4. 实现 ChildResourceScope、SourceStreamId、Publisher 和 SecretResolver 回滚；
5. 完成 quiesce、shutdown policy 和泄漏测试。

### Phase 3：服务与 FakeConnector 闭环

1. 实现 AccountAdminService/AccountService/AccountExecutionService endpoint；
2. 实现有界 command admission 和 receipt/idempotency 合约测试；
3. FakeConnector 发布完整 reconcile Snapshot 和增量；
4. 接入真实 EventEngine 路由和正常 Subscriber；
5. 验证 QueueFull、gap、invalidation 和恢复。

### Phase 4：现有 Connector 拆分适配

1. 将现有 `BrokerApi` 中公共行情职责留给 MarketConnector，私有交易职责迁入 AccountConnector adapter；
2. Binance Futures：复用其 UserDataStream、REST client 和 OrderManager；
3. OKX：复用 private stream、REST client 和 OrderManager；
4. Hyperliquid：复用 user stream、REST client 和 OrderManager；
5. 消除旧 `PublishEvent::LiveEvent` bridge，使账户事实直接原位编码进入 EventEngine；
6. 保留交易所专属 OrderManager，不把合流逻辑上移到 AccountPlugin。

### Phase 5：恢复、性能与实盘验收

1. 为三家 Connector 实现完整 epoch/reconcile 状态机；
2. 完成 command journal、未知结果查询和外部订单策略；
3. 增加 arena 原位 ABI 编码、预解析 binding 和有界指标；
4. 故障注入、长稳、stop/restart 和资源泄漏测试；
5. 只读 Shadow 对账；
6. 小额实盘 submit/cancel/fill/position/balance 全链路验收；
7. 删除旧账户发布入口和重复账户缓存。

任务执行顺序不能把 Phase 4 的交易所逻辑提前抽象进 AccountPlugin。先用 FakeConnector 固定公共契约，
再逐个适配真实 Connector。

## 18. 验收标准

- AccountPlugin 只包含 Factory、Registry、Service、生命周期和受限能力装配；
- AccountConnector 拥有认证、私有流、REST、OrderManager、Snapshot/Delta 和 reconciliation；
- REST/WS 合流、Fill 去重、epoch/version 和交易所错误不进入 AccountPlugin；
- 账户 payload 不经过 AccountPlugin owner 或二次 bridge；
- 下单 endpoint 有界、非阻塞，最终结果通过事件返回；
- 同一 command_id 幂等，未知网络结果不会盲目重复下单；
- READY 前必须完成 orders/positions/balances 全量 reconcile；
- QueueFull/gap 后账户失效，不继续静默消费；
- AccountId/AssetId/CurrencyId 和金额单位稳定、无热路径字符串/f64 路由；
- Secret 不进入配置快照、日志、事件、错误或 metrics；
- 新增 AccountConnector 只需注册 Factory，不修改 AccountPlugin；
- 三个现有交易所通过相同最小接口；
- 启动、停止、替换和失败回滚无任务、secret lease、队列或 EventBlock 泄漏；
- 实盘最终状态与交易所权威 orders/positions/balances 一致。

## 19. 最终边界

```text
AccountPlugin = AccountConnector Factory + Registry + Service Facade + Lifecycle

AccountConnector = 完整私有账户实现 + OrderManager + Reconciliation + EventEngine Publisher

RiskPlugin = 下单前风险决策和限额

EventEngine = 账户事实内存、路由、QoS和Subscriber交付

Strategy/Store = 消费事实、维护各自View、交易决策和审计持久化
```

任何交易所协议、订单状态合流、账户 Snapshot/Delta 或余额/持仓业务计算进入 AccountPlugin，均视为
职责越界。
