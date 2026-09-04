# Titan 项目重构剩余任务清单

状态：当前闭环已完成；外部实盘验收、目标硬件调优及未来插件仍待执行

更新时间：2026-09-03

## 1. 范围

本清单只整理以下现有设计文档已经明确提出、但尚未完成或尚未完成全链路验收的工作：

- [AccountPlugin 技术实现设计](account_plugin_technical_design.md)
- [MarketPlugin 技术实现设计](market_plugin_technical_design.md)
- [Core Runtime 交互契约](core_runtime_contract.md)
- [EventEngine 独立技术实现设计](event_engine_technical_design.md)
- [PluginEngine 独立技术实现设计](plugin_engine_technical_design.md)
- [Bar/Tick 回测与实盘统一策略接口](bar_tick_numba_strategy.md)
- [Strategy ABI v8 migration](strategy_abi_v8_migration.md)

当前闭环明确采用 `Fresh` 启动，不提供 Risk、checkpoint/store、recovery 或
`StreamBoundaryProvider` 能力，也不以 no-op 或内存占位实现冒充这些能力。它们只记录在本文末尾的
“未来插件待办”中；独立设计完成前，不进入当前闭环的实现或验收条件。

ConnectorFactory 不由 Titan main 或 CLI 静态逐个注册。Binance Futures、OKX、Hyperliquid
对应的 ConnectorFactory 应由各自插件提供，并通过 PluginEngine 的动态插件和插件包机制加载。

## 2. 当前判断

当前状态是“新的 Core Runtime 主链已经完成装配和切换”：

- EventEngine、PluginEngine、MarketPlugin、AccountPlugin 的主要类型、生命周期和单元测试已经存在；
- Binance Futures、OKX、Hyperliquid 的 MarketConnector/AccountConnector 适配代码已经存在；
- EventEngine 已实现 Primary Async lane、可靠 pending、水位、SubscriberHealth 和 SnapshotBarrier
  的主要内部能力；
- Bar/Tick/Hybrid 回测运行时、Numba 单参数回调和 ABI 布局校验已经存在；
- `titan` CLI 实盘入口、前台/后台 worker、stop/status 和信号关闭均已进入
  `TitanCoreRuntime -> PluginEngine -> Plugin -> EventEngine` 链路；
- PluginEngine 已形成动态 ABI、校验、代码 Lease 和三家交易所 ConnectorFactory 插件包链路；CLI 能在
  Core 配置中加载 Numba strategy package，并通过类型化的 Market、Account 与 Execution Service 完成绑定。

当前未勾选项的执行前提：

- 目标机已确定为 `43.165.184.116`：Ubuntu 24.04.4 LTS、2 vCPU（AMD EPYC 9754）、约
  1.9 GiB RAM 和 1.9 GiB swap。Rust/Cargo 1.94.0 位于 `/home/ubuntu/.cargo/bin`；非交互 SSH
  必须显式补充该 PATH。主机上已有三套 Titan Cargo 工作区/构建目录，但截至 2026-09-03 没有
  Titan systemd/user service、容器或常驻进程在运行。
- Binance Futures 主网已完成 58 项公开/私有 REST 接口验收，覆盖行情与合约元数据、账户/余额/仓位、
  普通订单与批量订单、订单修改、条件单、listenKey、账户模式与杠杆/保证金设置、持仓保证金、成交/收益及
  异步导出。5.10 USDT 硬上限的小额真实订单闭环已通过：批量限价单 submit/amend/cancel、普通 GTX 限价单
  submit/query/cancel、市价开仓、reduce-only 平仓和 fills 均成功；可逆配置操作均恢复原值。真实动态插件库
  还连续完成 generation 1/2 的 listenKey、私有 WebSocket、READY、Full reconcile、事件发布和干净 stop。
  最终全账户复核为零仓位、零普通挂单、零条件单、零保证金占用；XRPUSDT 恢复为 CROSSED、5x，账户恢复
  单向持仓与 Multi-Assets 模式。目标机 production 公共流已通过：2026-09-03 按 Binance 实际路由拆分为
  高频 `wss://fstream.binance.com/public/ws`（Depth/Trade/BBO）与常规
  `wss://fstream.binance.com/market/ws`（markPrice/Funding）两条连接后，XRPUSDT/BTCUSDT 均实时收到
  Depth snapshot/delta、Trade、BBO、MarkPrice 与 Funding 事件；旧组合端点 `/ws` 上
  `@markPrice@1s` 仅 ACK 无推送的问题不再出现。
  2026-09-04 进一步按 Binance 官方 WebSocket 拆分迁移私有用户流：旧
  `wss://fstream.binance.com/ws/{listenKey}` 已退役（TCP 可连但不再推送 executionReport），生产私有流
  必须使用 `wss://fstream.binance.com/private/ws?listenKey=<key>&events=ORDER_TRADE_UPDATE/ACCOUNT_UPDATE`。
  迁移后在目标机真实主网用 XRPUSDT GTX 卖单 + 撤单验证 REST→私有流字段语义，探针自校验通过，账户最终
  零挂单零仓位。
  OKX、Hyperliquid 凭据和资金环境仍待提供。
- 容量调优和 P99/P99.9 冻结已有目标硬件，但目标负载仍需通过该机器的可持续峰值探测后确定
  50%/80%/95% 档位；在测量完成前不虚构生产门槛。

## 3. P0：跑通完整链路

以下任务完成后，才能认为新架构已经形成一条可执行的闭环。

### 3.1 Core Runtime API 与启动装配

- [x] 按 EventEngine v1.4 的 Subscription、Primary lane 和消费者生命周期语义升级
  `core_runtime_api_version`。
- [x] 增加对应的 capability/兼容矩阵；保留文档要求的显式 v1.3 compatibility adapter，禁止旧
  consumer 被静默接入新语义。
- [x] 在 Titan main/CLI 中创建并启动 `TitanCoreRuntime`，由它统一持有 EventEngine 和
  PluginEngine。
- [x] 将运行配置通过 ConfigurationAdapter 转成 `PluginSpec[]`、MarketSourceDefinition 和
  AccountDefinition，并在插件创建前完成配置版本迁移和校验。当前已定义的应用/插件配置版本均为
  v1，因此迁移为 v1 标准化；未知版本在创建 Factory 前显式拒绝。
- [x] 按 Core Runtime 契约完成版本协商、PluginPlan 编译、route/endpoint 暂存、ActivationGate
  激活、失败回滚和反向停止。
- [x] 验证停止 EventEngine 前全部订阅已退休、handler 已退出且 EventArena 引用归零。

### 3.2 动态插件和 ConnectorFactory 加载

- [x] 完成 PluginEngine 设计中的动态插件加载入口 `titan_plugin_entry_v1()`，不只停留在 ABI
  descriptor 校验。
- [x] 完成插件包 Manifest、ABI、Schema、feature bits、配置版本和 capability 校验。底层
  `load_package` 会在建立进程级 code lease 前统一核对包版本与动态库内嵌 Manifest 版本；所有通用及
  Market/Account Connector C ABI 输入范围在构造 Rust slice 前拒绝空指针和超过 `isize::MAX` 的长度，
  producer-owned 异常输出仍调用匹配的释放函数，避免损坏描述符造成越界读取或泄漏。插件包与动态库
  SHA-256 校验使用固定 64 KiB 缓冲流式读取，不再按文件大小分配内存；三家 venue 导出的 root create
  均经共享 FFI panic boundary 完成输入校验、JSON 解码和实例创建。
- [x] 实现动态库代码 Lease；活动 EndpointLease、线程或 EventLease 存在时不得卸载动态库。
- [x] 将 Binance Futures ConnectorFactory 放入对应交易所插件，由该插件加载和注册。
- [x] 将 OKX ConnectorFactory 放入对应交易所插件，由该插件加载和注册。
- [x] 将 Hyperliquid ConnectorFactory 放入对应交易所插件，由该插件加载和注册。
- [x] Titan main/CLI 只加载配置指定的插件包，不包含三个交易所 ConnectorFactory 的静态注册逻辑。
- [x] 验证新增 Connector 只需新增并加载插件包，不需要修改 MarketPlugin、AccountPlugin 或 Titan
  main。

### 3.3 切换实盘运行入口

- [x] 将 CLI 实盘入口从旧 `LiveBotBuilder/IceoryxUnifiedChannel` 切换到 `TitanCoreRuntime`；前台和 detached
  worker 共用相同 Core 装配。
- [x] 接通行情事实路径：Exchange -> MarketConnector -> EventPublisher -> EventEngine -> Strategy
  consumer。
- [x] 接通命令路径：Strategy -> AccountExecutionService -> AccountConnector -> Exchange。
- [x] 接通账户事实返回路径：Exchange -> AccountConnector -> EventPublisher -> EventEngine -> Strategy
  consumer。
- [x] 接通 AccountReady、MarketReady 与策略启动门控；Gate 打开前不得执行 handler、发布业务事件或
  接受下单命令。
- [x] 正确传播 `TraceContext`，保证事件、Service Command 和账户结果可关联；Fake 全链路已验证
  Market Event -> Strategy callback -> AccountExecutionService command -> account result 保留同一 trace，
  causation 按直接原因推进。
- [x] 统一处理 SIGINT、SIGTERM、正常停止、插件启动失败和 Connector 停止超时；CLI registry 只管理
  Core worker 进程，插件和 Connector 生命周期统一由 `TitanCoreRuntime` 反向关闭。

### 3.4 EventEngine v1.4 恢复扩展边界

EventEngine 内部 SnapshotBarrier、staging、boundary 校验、candidate commit、超时/失败回滚和 generation
隔离能力及测试已经完成。当前 Fresh-only 主链不调用 SnapshotBarrier recovery，也不要求 Market/Account
提供跨组件 `StreamBoundary`。该扩展不会阻塞当前主链，等独立恢复插件设计后再接入。

### 3.5 Numba 与 Bar/Tick/Hybrid 接入新链路

- [x] 在新的生产链路中由 StrategyPlugin 的 `numba-python` loader 加载 Numba `nopython` 单参数回调。
- [x] 保证回测和实盘加载同一份 `on_tick(s)` / `on_bar(s)` 策略文件和同一 ABI descriptor。
- [x] 接通实盘 Tick 模式。
- [x] 接通设计中已有的 VenueNative、Canonical 和 REST recovery Bar 输入及去重规则。
- [x] 冻结实盘 Bar/Hybrid 入口（2026-09-03）：live 模式 Bar/Hybrid、`titan.market.BarBatch`
  订阅和非 Tick data mode 在 CLI 配置适配与 run-spec 解析阶段显式拒绝，避免无生产者的
  Bar 订阅在运行时静默等待。
- [ ] 接通实盘 Bar 和 Hybrid 模式：需要生产级本地聚合器或交易所 Candle 适配器发布
  `BarBatchV1`，此前保持冻结；Fake connector 的 Bar/Hybrid 链路验收仍由测试覆盖。
- [x] 验证稳态事件热路径不进入 Python 解释器。

### 3.6 最小端到端自动化验收

- [x] 使用测试插件包验证动态插件发现、ABI/Manifest 校验和 ConnectorFactory 注册。
- [x] 使用 Fake MarketConnector 发布 Tick，经 EventEngine Primary Async lane 到达 Numba 策略。
- [x] 使用设计规定的 Bar 输入发布 Bar，经 EventEngine Primary Async lane 到达 Numba 策略；
  `BarBatchV1` 使用稳定 little-endian 编码并校验 complete/timeframe/close_ts，Strategy adapter 将同周期
  批次映射为 ABI `bars_ptr/num_bars`，Fake MarketConnector 的 Hybrid 测试已验证同一 Numba 策略依次收到
  Tick 与 Bar。
- [x] 验证策略命令经 AccountExecutionService 到达 Fake AccountConnector。
- [x] 验证 Fake AccountConnector 发布 Order、Fill 并由策略 consumer 收到；Position/Balance 等
  尚无类型化 Strategy ABI view 的事实现在被订阅校验拒绝、在 adapter 层 fail-fast，不再静默
  映射为错误回调。
- [x] 覆盖 route commit 失败、Endpoint 激活失败和 Connector 启动失败的完整回滚。
- [x] 覆盖正常 shutdown：停止新命令、收敛账户事实、退休订阅、释放 Connector 和 EventBlock。

## 4. P1：完成现有设计的验收与旧路径退休

### 4.1 AccountPlugin 与三家 AccountConnector

- [x] 为 Binance Futures、OKX、Hyperliquid 完成完整 epoch/reconcile 状态机验收。
  公共真实 Connector adapter 已覆盖 Full reconcile 的单 epoch/version 提交、READY 门控、失败保持
  invalidated、ManagedOnly/ObserveAll；三家私有流断线或不可解析消息均进入 `AccountPublication::Error ->
  StreamInvalidated -> Full reconcile`。OKX 与 Hyperliquid 的私有订阅 ACK 按频道去重，公共或重复 ACK 不会
  提前打开 READY；Binance Futures 以交易所时间拒绝旧 WS 状态，并验证 REST/WS 终态乱序只发布一次。
  对不提供可靠账户 sequence 的流，任何无法证明连续性的连接错误都会建立新 reconcile epoch 并由权威 REST
  orders/positions/balances 补齐后重新 READY。
- [x] 完成有界内存 command journal、稳定 client order id、未知网络结果按订单 ID 查询，以及相同
  command 的重复 admission/冲突处理；未引入超出 V1 范围的持久化 journal。
- [x] 完成外部订单策略和 ManagedOnly 诊断：前缀内订单与 journal 已知订单进入托管视图，其他订单计入
  `external_order_count`；`ObserveAll` 不过滤外部订单。
- [x] READY 前完成 orders、positions、balances 全量 reconcile；私有流 ready 只触发 `Full` reconcile，
  三类快照和 `ReconcileCompleted` 发布成功后才发布 READY 并打开命令条件。
- [x] QueueFull、私有流 error/gap 或账户事实发布失败后关闭 READY、发布 `StreamInvalidated`，并向有界
  per-account lane 调度 `Full` reconcile；reconcile 失败保持失效并继续有界恢复。
- [x] 消除新 AccountPlugin 路径的 `PublishEvent::LiveEvent` bridge：三家私有流通过
  `AccountPublication` 同步直达 per-account encoder 并编码进入 EventEngine；旧 CLI 队列所需兼容转换仅保留
  在 `PublishSender` 边界，随旧 CLI 一并退休。
- [x] 删除旧账户发布入口和重复账户缓存（2026-09-04）：queued `PublishSender`
  的 `send_account -> into_legacy` bridge 与旧连接器进程一并删除，账户事实只经
  `AccountPublication -> AccountEventEncoder` 单条 direct 路径编码。
- [x] 完成故障注入、长稳、stop/restart、shutdown policy 和资源泄漏测试。
  shutdown policy 的 LeaveOpen/CancelAll/CancelAllAfter 选择、venue reject、deadline 传播，以及 Market
  runtime 非协作 shutdown 的有界返回与后续 JoinHandle 回收已覆盖；Account 本地长稳测试已连续执行
  2,000 轮 Full reconcile（每十轮注入一次 REST transport failure），验证 READY 反复关闭/恢复且订单、余额、
  ID interner 不增长；10,000 次 operation churn 也验证终态历史严格受 1,024 上限约束。真实 per-account
  runtime 线程另行验证正常 stop 幂等、停止后 restart 拒绝、deadline 超时后保留并二次回收 JoinHandle，及
  runtime/ResourceScope 释放后 Weak 无法升级。真实网络故障与交易对账归入下方独立实盘验收项。
- [x] 完成只读 Shadow reconcile：replacement candidate 使用关闭的 publisher admission 完成私有流与
  全量 reconcile，旧 generation 停止并原子 swap 后才打开新 publisher，再发布新 generation 全量事实。
- [ ] 完成小额 submit/cancel/partial-fill/reconnect 以及最终 orders/positions/balances 对账。
  Binance Futures 已在目标机完成真实主网订单闭环：XRPUSDT、单笔名义金额不超过 5.10 USDT；被动限价单
  `NEW -> query -> CANCELED`，批量限价单 `NEW -> amend -> CANCELED`，3.7 XRP 市价开仓与 reduce-only
  平仓均为 `FILLED`，成交明细、手续费和 realized PnL 可查询。58 项主网接口检查还真实覆盖历史成交、
  聚合成交、Kline、资金费率、持仓量与合约统计、账户与交易状态、普通/条件单查询及撤销、listenKey、
  leverage/margin type/position mode/multi-assets mode、countdown cancel、isolated position margin 和
  income async download；所有可逆设置都已恢复。真实动态插件的两代连接均完成私有流 READY 和两轮 Full
  reconcile。实测同时修复了 serverTime/单对象响应解析、聚合成交短字段、布尔字段、浮点参数精度、
  API-key-only 公共请求、批量请求编码与 HTTP method、修改订单必填字段与可见性窗口、条件单新参数、
  symbol 大小写过滤、错误对象误解析为空订单、未绑定 symbol 触发 reconcile 风暴，以及私有流连接时误执行
  cancel-all 等问题。最终独立原始接口全账户复核为零仓位、零普通挂单、零条件单和零保证金占用；XRPUSDT
  为 CROSSED/5x，账户为单向持仓并开启 Multi-Assets。该总项继续保持未完成：5 USDT 最小订单无法
  确定性制造部分成交，且 OKX、Hyperliquid 的真实账户验收尚未执行。

### 4.2 MarketPlugin 与三家 MarketConnector

- [x] 分别完成 Binance Futures、OKX、Hyperliquid 的统一 Connector 合约测试。
  三家真实 Factory 已分别通过不联网的统一合约层测试：公开行情配置创建、instrument view、空 kind/未知
  asset 拒绝、重复 kind 归一、未启动 snapshot 拒绝、unsubscribe operation 终态、幂等 stop 和 ResourceScope
  释放；各 venue 的 sequence/checksum/recovery 解析测试也已覆盖。三家 production public stream 均已真实
  连接；此外使用真实本地 TCP/WebSocket 握手逐家强制 peer-side disconnect，验证指数退避重连后从共享
  desired state 完整重放订阅（Hyperliquid 的 l2Book/trades 双帧也逐连接完整验证）。统一 runtime 的 stop
  deadline、JoinHandle 保留/回收及资源释放测试共同覆盖关闭契约。
- [x] 覆盖 subscribe/unsubscribe/request_snapshot、订阅共享和新 consumer Snapshot：相同 asset/kind
  只建立一次 venue subscription，最后一个引用释放才退订；纯共享或部分共享的新 consumer 会在其
  EventEngine route 已建立后向具体 Connector 请求完整 Snapshot，适配层不缓存或伪造 stream 坐标。
- [x] 覆盖 Snapshot/Delta、epoch、sequence、checksum、gap 和 QueueFull 恢复：Binance Futures 验证
  REST snapshot、连续 delta、gap invalidation 与恢复 epoch；OKX 验证 snapshot/delta、sequence gap、checksum
  失败及恢复 epoch（并修复失效后 epoch 被重置的问题）；Hyperliquid 的 l2Book 全量图像验证每次均为 snapshot
  且 epoch 单调递增；共享适配器验证 EventEngine 发布失败后降级并请求 venue snapshot/recovery。
- [x] 覆盖 stop deadline、失败回滚、压力和资源泄漏。统一 runtime 已验证非协作 shutdown 在 deadline
  内返回并保留 JoinHandle 供后续回收、venue shutdown error 不误报 Stopped、停止后 restart 拒绝不残留
  admission；阻塞 snapshot 下 command queue 明确暴露有界压力，stop 将 pending operation 全部终态化，
  ResourceScope 关闭后 runtime 的 Weak 引用无法升级。
- [x] 三家 Connector 均通过真实 PluginEngine/EventEngine 路径验证，不能只验证旧 Connector 入口。
  当前动态 Core smoke 已实际加载三家插件包、创建 disabled MarketConnector、通过 `titan.market` Service
  resolve 并跨 ABI 读取 instrument view，再验证统一 shutdown。Binance Futures 已用 release 构建连接真实
  production public stream，经 MarketPlugin、PluginEngine 和 EventEngine FastLane 收到 snapshot/delta/trade/
  BBO/funding，连续性正确且 drop/resync/rejected 均为 0。Hyperliquid 也已连接 production public stream，
  真实 Depth/Trade 均通过相同三层路径到达 FastLane 并完成统一关闭。OKX 主站在当前网络不可达后，通过
  测试支持的 `OKX_TEST_PUBLIC_WS_URL` 切换到官方美国区域 public endpoint；Depth/Trade 在 7.42 秒内到达
  EventEngine，并完成 Connector、PluginEngine、FastLane 和 EventEngine 的统一关闭。测试同时支持
  `OKX_TEST_PROXY`，网络失败时输出完整 health/counts。
  2026-09-03 在目标机复验 Binance production 公共流时发现：旧组合端点 `/ws` 上 Depth/Trade/BBO
  可达，但 `@markPrice@1s` 订阅 ACK/LIST_SUBSCRIPTIONS 可见后仍无推送。将生产连接按 Binance 路由拆分为
  `public/ws`（Depth/Trade/BBO）与 `market/ws`（markPrice/Funding）后复测：XRPUSDT、BTCUSDT 均实时
  收到 `titan.market.MarkPrice` 与 Funding 事件，Depth snapshot/delta、Trade、BBO 保持通过，Mark
  Price/Funding 实时流因此更新为通过；独立 Python WebSocket 与 connector 探针结论一致。
- [x] 完成新入口切换后删除旧行情发布入口和重复 bridge（2026-09-04）：queued
  `PublishTransport/PublishReceiver` 与 QueueOverflow/BatchStart/BatchEnd/
  RegisterInstrument 变体删除；行情与账户统一走 `direct_publish_sender`。
  `MarketEventBridge` 保留为连接器内部 FeedBatch/NativeMarket 到 Market ABI
  的单一翻译点，不再存在可双写的旧队列入口。

### 4.3 PluginEngine 剩余设计项

- [x] 完成已有契约可落地的文件与管理 API 到 Runtime Definition 适配：`ConfigurationAdapter` 从 TOML
  生成 Market/Account Definition，Core 在插件 RUNNING 后通过各自 Admin Service 串行创建/启动。
  设计只声明配置“可来自数据库/配置服务”，未定义数据库或远程管理协议，因此不在本清单虚构对应适配器。
- [x] 完成配置版本、ChangePlan 和插件包替换流程的集成验收；包版本/来源已进入不可变
  `PluginPlan`，稳定 `ServiceHandle` 可观察新 Endpoint generation，候选包启动失败会恢复旧包和旧配置。
- [x] 完成 DEDICATED、BACKGROUND、PASSIVE、ColdAsyncRuntime 的组合测试：三种声明模型进入
  PluginPlan；PASSIVE 拒绝订阅任务；DEDICATED 使用独立线程并执行 Gate/CPU affinity；BACKGROUND
  subscriber 进入 RuntimeHost 共享的有界 ColdAsyncRuntime，异步等待 Gate，停止时等待任务退出并退休
  SubscriptionToken；BlockingExecutor 同样验证有界准入。
- [x] 完成 CPU affinity、callback budget、Watchdog、线程心跳和 STALLED 状态验收。
  CPU 可用性/Gate 已在真实 DEDICATED 创建边界校验；CallbackMonitor 覆盖 soft budget、连续超限阈值
  和运行中 stall 扫描，ThreadHeartbeat 提供 age/stale 判定。RuntimeHost 为声明 callback budget 的订阅运行时
  启动独立 Watchdog，以不大于 stall threshold 四分之一的周期扫描；卡死后关闭共享 Gate、使 Endpoint
  不可用、发布 `PluginCallbackStalled`/`PluginHealthChanged`，并在诊断中呈现 `FAILED/STALLED`。
- [x] 完成结构化运行异常、`PluginHealthChanged`、Endpoint generation 切换和不可用处理；Core 目录注册
  固定 schema 事件，故障事件保留 TraceContext，失败实例 Gate 与服务立即关闭。
- [x] 完成 TraceContext、Flight Recorder、后台导出、完整指标和诊断工具的生产接入。
  TraceContext 已贯穿 Event/Service；PluginEngine 现以固定槽位、原子覆盖且写冲突不等待的有界
  FlightRecorder 记录 callback、Service 调用及 lifecycle fault，错误、拒绝、超预算和卡死记录强制保留，
  异常快照通过有界队列交给独立后台 exporter，热路径不取得全局锁、不执行格式化或同步 I/O；并发测试验证
  多写者快照不会出现字段撕裂，写冲突通过 `dropped_records` 显式计数。已增加无锁热路径的 per-plugin/
  per-service 原子指标、provider/
  consumer 标签，以及聚合 profile/route/plugin/service 的冷路径快照；实例诊断已包含依赖、订阅、execution、
  heartbeat、callback budget、queue/pending/outstanding、按类型资源计数和最近错误；failure reason 与
  resource type 失败计数也作为独立标签快照提供。Runtime Definition 的版本/状态继续由对应 Market/Account
  插件诊断负责，PluginEngine 不复制插件内部权威状态。

### 4.4 EventEngine 性能和可靠性验收

- [x] 消除 `pending_retry_preserves_fifo_after_gate_opens` 在全工作区并行测试中的偶发失败。
- [x] 固定运行 Async lane、pending、health、EventLease 和生命周期竞争的 loom/等价模型测试。
- [x] 完成多 Broker、多 Primary lane 峰值压测和单 handler 卡死隔离测试。四个独立 source/routing
  domain 各突发发布 1,024 条可靠事件到四个 Primary lane，验证无跨路由、三水位全部提交、lane health
  保持 NORMAL 且 shutdown 后 EventArena 无残留；独立阻塞 handler 的测试验证其 lane 进入
  RESYNC_REQUIRED 时健康 lane 仍完整提交全部事件。
- [x] 验证 Dedicated、SpinSleep、Park、CPU affinity 和物理核心隔离。三种 policy 均在独立命名
  worker 上完成实际投递；Dedicated 强制配置 affinity。启动配置会拒绝不存在的 core，以及 EventLoop、
  普通 subscriber core pool 间的重复绑定；动态 Primary lane 对显式 affinity 执行独占预约，重复绑定失败，
  lane 退休后才释放 core 供下一 generation 使用。Worker 启动通过握手确认操作系统 affinity 调用成功，
  不支持线程绑核的平台（如当前 macOS runner）会明确返回 `CpuAffinityFailed`，不会伪报隔离成功。
- [ ] 调优 Arena、Ingress、Async lane、pending 和 staging 的有界容量。
  机制层已经具备配置化边界：`snapshot_barriers` 统一限制活动 barrier 数、每 barrier staging、全局
  staging 总量和最大 deadline；跨 lane 的容量耗尽、staging/replay 超时、abort、unregister/stop 均已通过
  故障注入验证额度和 EventLease 会释放。这里保留未完成，仅指仍需依据目标硬件、Broker 峰值与恢复窗口
  计算并冻结生产容量，不把设计示例值误报为已调优结果。
- [ ] 在目标硬件冻结 publisher admission、worker dispatch、handler commit 的 P99/P99.9
  PerformanceEnvelope，并建立 CI 回归门槛。

### 4.5 Bar/Tick/Hybrid 最终验收

- [x] 同一份逐笔数据经过离线和流式 Builder 后生成完全一致的 Bar。
- [x] 验证 Bar-only 不进入 1 ms 帧循环，且 `on_bar(s)` 订单不能在刚关闭的 Bar 中成交。
- [x] 覆盖整点成交、空周期、行情断档和多周期同时关闭。
- [x] 验证实盘迟到成交不修改已发布 Bar，REST 补齐不重复触发。
- [x] 提供 Numba 策略与纯 Rust 策略的稳态性能对比。Release 基准使用相同 ABI、相同状态增量和相同
  1,000,000 次预热，分别执行 10,000 × 1,000 次调用并校验最终状态；Apple M1 Pro 实测 Rust
  P50/P99/P99.9 为 3.375/3.750/4.458 ns，Numba 为 28.750/31.375/35.416 ns。工具与 JSON 报告均已入库，
  并明确该数字只代表回调 ABI 边界，不替代目标生产硬件的全链路 PerformanceEnvelope。

### 4.6 构建、回归和旧代码清理

- [x] 固化 Python 动态库的构建/运行环境，使工作区测试和 `titan` 命令无需人工临时设置
  `DYLD_LIBRARY_PATH`。
- [x] 将完整 workspace、全 feature Connector、动态插件和端到端链路测试纳入 CI。
- [x] 删除 CLI 的旧 LiveBot/Iceoryx 启动路径。
- [x] 删除 Connector 中旧（queued）PublishSender/PublishEvent 和重复 Runtime 管理代码
  （2026-09-04）：旧连接器进程、Iceoryx、binancespot/bybit、LiveBot/Iceoryx 示例、
  `PublishReceiver`、queued transport 与旧事件变体均已删除；保留 direct-only
  `PublishSender/PublishEvent` 作为 Market/AccountPlugin 新链路的事件载体。
- [x] 确认同一业务事实不存在新旧路径双重发布或双重消费（2026-09-04）：
  `PublishSender` 为单 transport direct 枚举，事件只投递一次；connector 测试新增
  `publish_event_is_delivered_once_through_the_direct_sender`，账户侧保留
  `account_publication_is_delivered_directly_without_a_live_event_bridge`。

### 4.7 断流状态与健康可见性（2026-09-03 增量）

- [x] 实例 supervisor 在 `PrimaryAsyncLane` 进入 `ResyncRequired`/`Failed` 时，把 runtime
  生命周期显式切换为 `Invalidated`、关闭 command/activation gate，并记录 degraded 原因；
  lane 非 Normal 后队列中剩余事件不再调用用户回调，而是安全丢弃/收敛。
- [x] `StrategyRuntimeHealthSnapshot` 直接反映 lane 权威状态：ResyncRequired/Failed 时
  `healthy=false`，不再出现 “Running + healthy 但实际已停止交易” 的假健康。
- [ ] 自动恢复仍依赖未来 Recovery/SnapshotBarrier Provider；当前恢复路径是
  stop/replace（或后续接入 barrier 后 resume）。

### 4.8 单位一致性校验（2026-09-03 增量）

- [x] CLI 配置适配阶段对策略实际绑定的同一 asset id 做 Market f64 tick/lot 与
  Account `DecimalUnit` 的一致性校验；不匹配会在启动前显式拒绝，并报告两侧数值。
- [x] 校验只检查 enabled strategy 真正绑定且两端都定义的单位对，避免无关定义误伤。
- [ ] 深度统一仍待完成：把 Market/Account 各自的 instrument unit 收敛为一个权威共享模型，
  而不是由 CLI 做事后一致性检查。

### 4.9 REST→私有流身份链路（2026-09-03 增量）

- [x] `AccountPublication::Order` 携带 client order id，并在 `OrderChangedV1`/`FillV2`
  中回填，策略侧订单/成交身份不再为零。
- [x] AccountPlugin REST 提交前把订单登记进 venue `OrderManager`（三家 venue），使私有流
  Order/Fill 更新不再因“未知 client id”被丢弃。
- [x] Binance/OKX 私有流对已登记 client id 不再强制旧前缀；Hyperliquid 统一
  32-hex ↔ `0x` cloid 编码（REST 提交与 OrderManager 键一致）。
- [x] CommandJournal 终态释放：私有流/encoder 的终态 OrderChanged 按 client id 回写释放
  submit/amend/cancel 条目；REST 已知终态、明确失败和 CancelAll/CancelAllAfter 释放对应
  command；unknown 结果与仍开放的订单命令保留用于幂等，容量仍由 command queue 上限约束。
  2026-09-04 在 Binance 主网探针路径复验未回归。

### 4.10 Binance REST→私有流主网字段语义验收（2026-09-04）

2026-09-04 在目标机 `43.165.184.116` 用 Binance USD-M 主网真实完成
`submit(GTX, XRPUSDT, 100 USDT) → private-stream NEW → REST cancel → private-stream CANCELED →
Full reconcile` 全链路，只读不成交、最终零挂单零仓位。实测语义与修复：

- [x] 私有用户流连接改为官方 `/private/ws?listenKey=…&events=ORDER_TRADE_UPDATE/ACCOUNT_UPDATE`；
  本地 `user_data_stream.rs` 日志可确认收到 `executionReport`（此前 90s 收不到任何帧）。
- [x] `AccountPublication::Order` 新增显式 `venue_order_id`：Binance WS 直接回填交易所 orderId；
  `OrderChangedV1`/`FillV2` 的 venue id 不再使用内部合成的 command-id 数值。
- [x] REST/查询快照时间戳在 ABI 边界由 BrokerApi 毫秒换算为纳秒；REST 与私有流
  `OrderChangedV1.exchange_ts` 实测同一笔订单完全相等，且与 `receive_ts` 同数量级。
- [x] REST 与私有流回传的 `client_order_id`（32-hex 确定性 ID）、`venue_order_id`、`status`
  （1=NEW/4=CANCELED）、side/type/price/quantity 实测一致；即使 WS 帧先于 REST 回包到达也一致。
- [x] 探针 `connector/examples/binance_futures_account_rest_ws_probe.rs` 增加 REST↔WS 一致性
  自校验（client id、venue id、exchange_ts、状态），可作为后续 CI/验收样例。
- [ ] OKX/Hyperliquid 私有流尚未实盘接入：其 cancel-all 聚合路径没有交易所 orderId 回填、WS
  exchange_ts 与 REST reconcile 的换算需在各自主网逐一实测；接入顺序先 Binance（已通）再其余两家。

### 4.11 EventEngine 容量扫描与旧路径清理审计（2026-09-04）

#### 容量扫描（目标机 `43.165.184.116`，2 vCPU / 1.9 GiB RAM，release）

把 `titan-event-engine` 基准改为可复现的容量扫描工具：
`TITAN_EVENT_BENCH_DEFAULT_CONFIG=1` 使用 `EventEngineConfig::default()` 基线；
`TITAN_EVENT_BENCH_SMALL_SLOTS/…_CRITICAL_CAPACITY/…_SUBSCRIBER_CAPACITY/…_PENDING_GLOBAL`
等环境变量可逐项缩小容量，结果行同时打印 drop/resync/arena 耗尽计数，便于找到拐点而非只记吞吐。

同一 synthetic 单 critical lane、64 B payload、单 ReliableOrdered subscriber 的结果：

| 配置 | 负载 | 吞吐 | dispatch P99 桶 | subscriber P99 桶 | drop/resync/arena | RSS |
|---|---|---:|---:|---:|---:|---:|
| tuned（原有基准档） | 500k burst | ~1.49M/s | 67,108,863 ns | 67,108,863 ns | 0/0/0 | ~46 MB |
| tuned | 500k @ 300k/s | 299,994/s | 2,097,151 ns | 4,194,303 ns | 0/0/0 | ~46 MB |
| default | 1M burst | ~1.06M/s | 16,777,215 ns | 16,777,215 ns | 0/0/0 | ~152 MB |
| default | 1M @ 500k/s | 499,984/s | 2,097,151 ns | 4,194,303 ns | 0/0/0 | ~152 MB |
| default | 1M @ 800k/s | 799,237/s | 4,194,303 ns | 8,388,607 ns | 0/0/0 | ~152 MB |
| 过小样本（8,192 slots / 4,096 sub cap） | 500k burst | 未完成 | — | — | ReliableOrdered 转 RESYNC，投递超时 | — |

结论：默认有界容量在该 synthetic critical lane 基准上无丢单且有余量；过小容量会明确进入重同步/超时，
证明配置边界是可承载的约束而非装饰。此结果只用于容量机制验收与目标机复现基线，不冻结为生产 SLA：
正式 P99/P99.9 仍需按真实 Market/Account 双源负载（含 MarketBatch 长度分布与多 Primary lane）
在目标部署形态下测得后再设 CI 门槛。

#### 旧路径清理审计

经依赖面梳理，connector 内待退休的“旧发布/旧桥/重复缓存”实际是一整簇，而不是独立死代码：

```text
connector/src/main.rs（iceoryx2 连接器进程）
  └─ PublishTransport::Queued / PublishReceiver / publish_channel / PublishEvent
       ├─ venue 测试用 queued 通道（可改为 direct 断言，删除成本低）
       ├─ binancespot、bybit 旧 venue 模块（仅被旧连接器进程使用）
       ├─ hftbacktest examples/src/bin/live.rs（研究用 LiveBot + Iceoryx IPC）
       └─ 旧 LiveEvent::Feed/Funding/Order/Position 兼容转换
```

新 Market/Account 直接路径只使用 `PublishTransport::Direct` 与 `DirectPublication`；但三家新 venue
的市场流仍通过 `PublishEvent`（FeedBatch/MarkPrice/StreamInvalidated 或少量 LiveEvent 格式）进入
`MarketEventBridge`，账户流直接进入 `AccountEventEncoder`，因此不能只删 `PublishEvent` 而不迁移 venue
发布格式。完整退休顺序建议：

1. 把 OKX/Hyperliquid/BinanceFutures 残余 `LiveEvent::Feed/Funding` 发布迁移到 `FeedBatch`/专属事件；
2. venue 单测改走 direct sender 断言，删除 queued 兼容测试依赖；
3. 删除 `PublishTransport::Queued/PublishReceiver/publish_channel` 及 `send_account` 的 legacy 分支；
4. 删除 `connector/src/main.rs` 与 `examples/src/bin/live.rs`（旧 Iceoryx 连接器进程/研究 LiveBot）；
5. 依据是否仍保留 hftbacktest 研究型 live 能力，决定 binancespot/bybit 模块与 `hftbacktest/src/live`
   去留，并同步 README/rust_strategy 文档。

该顺序涉及 README 宣称的 iceoryx IPC 研究能力和旧 venue 模块的产品取舍，未在本次未经确认直接删除；
代码层“单一事实无双重发布”通过 direct/direct-exclusive 设计约束与 EventEngine 路由隔离测试已覆盖，
待上述第 3 步完成后在 connector 边界再补一条断言即可收尾。

### 4.12 方案 B：删除旧 LiveBot/Iceoryx 与旧 venue 模块（2026-09-04）

经产品确认采取彻底删除方案，已落地：

- [x] 删除 `hftbacktest/src/live`（LiveBot、Iceoryx IPC、recorder）与 `live` feature；
  hftbacktest 保留回测/行情/策略接口，`default` 仅含 `backtest`。
- [x] 删除 `titan-runtime` 的 `live` feature 与 `RuntimeBotEvents for LiveBot` 兼容实现，
  `titan-cli` 不再启用该 feature。
- [x] 删除旧连接器进程 `connector/src/main.rs`、`connector` 的 iceoryx2/clap 依赖，
  以及 `binancespot`、`bybit` 模块与对应 feature/config/示例。
- [x] 删除 `examples/src/bin/live.rs`、`examples/binance_ws_to_numba_on_tick_latency.py`、
  `connector/scripts/run_binance_ws_to_on_tick_latency.sh`。
- [x] 更新根 README、`connector/README.md`、`hftbacktest/README.md`、`docs/rust_strategy.md`
  与 PluginEngine 示例，旧“连接器进程 + iceoryx IPC”不再作为产品入口。
- [x] 回测/新链路回归：`hftbacktest --lib` 106 项、`connector --lib` 220 项、
  `titan-runtime`/`titan-strategy-plugin` 测试均通过；全 workspace `--all-targets` 编译通过。

connector 已删除 queued transport 并改为 direct-only，`PublishEvent::LiveEvent` 亦已移除：
venue 行情统一以 `FeedBatch`/`MarkPrice`/`Funding`/`ConnectorError`/`StreamInvalidated`
发布，`MarketEventBridge` 只做 FeedBatch/NativeMarket → Market ABI 的单一翻译。
- [x] 删除 `Connector::submit/cancel` 旧订单接口及其三家实现/测试入口；订单执行只走
  `AccountConnector` 命令 + `BrokerApi` 路径。
- [x] 移除 `PublishEvent::LiveEvent`：行情 `Feed/Funding` 迁移为 `FeedBatch/Funding`，
  Hyperliquid `userFundings` 旧资金流订阅/发布删除，错误统一为 `ConnectorError`。

## 5. ABI 与文档同步

- [x] Strategy ABI v8 Funding 配置字段、布局和冲突校验已实现。
- [x] Rust 和 Python SDK 已执行严格 ABI 版本校验，不存在结构体尺寸回退。
- [x] 将 Strategy ABI v8 文档标记为历史迁移文档。
- [x] 补充当前 ABI v9 的迁移说明；当前 Rust 和 Python SDK 实际版本均为 9。
- [x] 更新 AccountPlugin 文档的“待实现”状态，使其反映主体代码已经落地、Phase 5 和实盘验收仍待完成。
- [x] 更新 EventEngine 实施记录，区分内部能力已实现与 v1.4 跨组件迁移尚未完成。

## 6. 剩余任务执行顺序与前置条件

Core API、动态插件、三家交易所插件包、Core 装配、Fresh-only StrategyPlugin、Numba 和 Fake 端到端闭环
已经完成。剩余工作不阻塞当前软件闭环，按外部条件推进：

1. 在明确提供的测试账户、凭据和小额资金环境中完成三家交易所 submit/cancel/partial-fill/reconnect 与
   最终对账；凭据不得写入仓库或测试输出。
2. 在指定目标硬件和目标负载上调优有界容量，冻结 publisher admission、worker dispatch、handler commit
   的 P99/P99.9，并据实设置 CI 回归门槛。
3. 删除 Connector 内部仅供旧 Connector trait 兼容的 `PublishSender/PublishEvent`、重复账户缓存和 Runtime
   管理代码，最后验证不存在事实双重发布或消费；CLI 的旧 LiveBot/Iceoryx 路径已经删除。

## 7. 未来插件待办（不阻塞当前闭环）

- [ ] Risk 插件：独立设计完成后提供风险决策 Service，并由 StrategyPlugin 选择性绑定。
- [ ] Checkpoint/Store 插件：独立设计持久化、版本和失败语义后提供状态快照存储。
- [ ] Recovery 插件：独立设计恢复 generation、协调和启动策略后扩展当前 Fresh-only profile。
- [ ] StreamBoundary Provider 插件：独立定义 Market/Account boundary 取得与发布契约后接入
  EventEngine SnapshotBarrier。

上述插件未安装时，当前实现不会导出对应 Service，不会声明对应依赖；非 Fresh recovery 配置会在创建策略
时明确拒绝。
