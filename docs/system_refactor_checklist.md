# Titan 系统重构与验证清单

基线提交：`741d2de56f652855a3e5848e523f312b014dc15f`。开始日期：2026-10-12。

本轮统一事件发布、账户事实、恢复、服务生命周期、策略执行和结果协议，减少跨模块重复状态与分支。所有构建、格式检查、静态检查、测试、AOT 制品生成和性能验证均在编译服务器 `192.168.3.88` 执行。本轮不接入真实账户，不部署或启用实盘策略。

## 执行顺序与完成条件

### 1 事件发布与路由

- [x] E1 将复制发布和 reservation commit 汇入同一提交入口。
- [x] E2 在任何 PRIMARY/FastLane 消费者可见前完成全局发布准入；拒绝的事件不得部分投递。
- [x] E3 统一各消费路径的 source sequence 检查、故障计数和发布元数据；保留可靠有序与行情 Latest 的不同 QoS。
- [x] E4 停机关闭发布准入并等待已进入的发布操作结束，防止尾部事件越过关闭边界。
- 验收要求：复制与 reservation 两条 API 均覆盖 ingress 满、重复序号、并发发布、停止竞态、内存 lease 回收；现有 PRIMARY、FastLane、可靠重试和 SnapshotBarrier 回归通过。

### 2 账户投影与快照

- [x] A1 集中管理账户订单、持仓、余额、epoch 与各投影提交版本，快照内容和版本在同一个提交边界读取。
- [x] A2 REST bootstrap 和私有流事实复用投影更新入口；消除重复 OrderChanged 构造与整数组反复复制。
- [x] A3 保留订单字段与明确的终态规则，规范累计成交去重和快照缺失订单的处理。
- [x] A4 增加余额事实入口，更新交易所私有流中的余额；保留各 venue 的字段解析和授权方式。
- 验收要求：快照与更新并发、不同投影版本、重复成交、非零初始仓位、余额更新和终态订单均有回归测试。

### 3 账户恢复与候选替换

- [x] R1 首次启动、重连和重新对账使用同一恢复流程：暂停准入、暂存增量、查询 REST、合并、提交、READY。
- [x] R2 暂存事实使用有界容量，超时或溢出保持 Invalidated，不能凭 socket 连接成功恢复 READY。
- [x] R3 处理断线期间终态订单、成交和余额变化；推进恢复 epoch，明确必须重新创建策略的恢复边界。
- [x] R4 候选账户可以在对外 publisher 关闭时完成本地 bootstrap；提交替换后统一发布快照与 READY。
- 验收要求：模拟断线遗漏事实、恢复失败、暂存溢出、候选替换失败回滚与成功切换；不得调用真实交易所。

### 4 生命周期与公共组件

- [x] L1 复用有界操作历史、deadline 等待、错误汇总与资源关闭；行情、账户、策略保留各自领域生命周期。
- [x] L2 全局停机 deadline 贯穿 Strategy、Execution、Account、Market、EventEngine；停止失败不得伪报成功。
- [x] L3 状态查询不承担停止超时推进；生命周期推进发生在执行或调度入口。
- [x] L4 限制运行中依赖组件替换，防止策略继续持有旧 generation 的市场或执行绑定。
- 验收要求：超时、重复停机、失败清理、过期 handle、操作历史容量和依赖替换覆盖完整。

### 5 V13 运行与双腿执行

- [x] V1 区分快照 generation 与新运行 generation，覆盖 Service → Factory → Instance 完整恢复。
- [x] V2 合并订单事实更新入口，活动视图与持久化历史保持一致；启动合并状态结束后释放。
- [x] V3 命令批次先校验、预留本地容量和登记，再执行；逐腿请求结果继续收敛，不能因第二腿本地拒绝遗漏第一腿事实。
- [x] V4 停机等待订单与成交事实收敛；策略风险缺口无法处理时返回 ABI 错误，最终 checkpoint 必须获得写入确认。
- [x] V5 checkpoint 使用独立周期和 dirty 标记；定义历史保留边界，限制全量复制成本。
- [x] V6 实盘与离线适配复用 callback/context/staging 入口，保持不同执行后端的撮合语义。
- [x] V7 pair_arb 容纳 Taker-Taker 双腿任意成交顺序；集中终态清理，保持 Maker-Taker、撤单确认后重挂和恢复校验。
- 验收要求：完整重启、Order→Fill 停机窗口、执行器饱和、双腿任意顺序、撤单拒绝、checkpoint 写失败、AOT 回调 smoke。

### 6 连接器转换与调度

- [x] C1 共用定点单位与完整 client namespace 的订单元数据 ID；清理拒绝请求的跟踪状态，保留交易所 wire、签名和枚举解析差异。
- [x] C2 清理发布端永远成功的 Result 兼容语义，集中 native 与 FeedBatch 的公共编码。
- [x] C3 策略 timer 和 supervisor 共用调度线程，保留独立 PRIMARY lane；统一线程取消与 join。
- [x] C4 `timer_capacity` 用于控制队列，`max_commands_per_callback` 独立设置；`checkpoint_interval` 独立于 timer 周期。账户、行情和执行 I/O runtime 继续按服务隔离，由各自资源 scope 关闭。
- 验收要求：三家 venue 的解析、wire、重连及共享调度取消测试；变更热路径时在编译服务器重测延迟基线。

### 7 验证后运行计划与监控

- [x] P1 controller 与 worker 使用同一份验证后配置；启动前核对配置摘要、artifact 摘要和策略身份。
- [x] P2 两个实盘入口复用信号处理、运行监控和停机流程。
- [x] P3 区分进程存活与组件健康，记录 Failed/Invalidated/Paused，不让 worker 存活掩盖交易链路故障。
- 验收要求：校验后配置变化、制品替换、组件故障、SIGTERM、owner token 与 worker 退出状态。

### 8 结果与报告

- [x] B1 所有后端共用 ResultBundle writer，最后提交 manifest，记录真实的制品与配置身份。
- [x] B2 Rust/Python reader 使用同一 v1 协议；returns 等缺失数据使用明确空值，不重算交易结果。
- [x] B3 CLI trace 回放及实盘停止结果均能进入 report，校验文件大小、摘要和路径。
- 验收要求：run → report 完整链路、输出损坏、未提交 manifest、空收益序列和 Python reporting 回归。

### 9 数据采集

- [x] D1 无界队列改为有界写入队列，将同步压缩移出 async 接收循环。
- [x] D2 停机先终止采集再排空写入；明确返回写盘和 gzip finish 错误。
- [x] D3 文件轮转和同日重启不覆盖已有采集数据。
- 验收要求：慢写盘、队列背压、写入失败、轮转、同日重启、退出排空和压缩文件可读性。

## 编译服务器验证门禁

- [x] 确认远端路径、版本或源码摘要、工作区状态；在独立目录验证，不覆盖已有服务器代码。
- [x] 固化本次源码摘要、工具链、测试命令与原始日志。
- [x] `cargo fmt --all -- --check`。
- [x] `cargo check --workspace --all-targets --all-features`。
- [x] `cargo clippy --workspace --all-targets --all-features`；区分既有 warning 与新增 warning。
- [x] `cargo test --workspace --all-targets --all-features`；真实账户测试保持 ignored。
- [x] Python Strategy SDK 与 reporting 完整测试。
- [x] V13 策略 AOT 构建、artifact smoke、pair_arb smoke 和 CLI 回放报告集成验证。
- [x] `scripts/check_single_runtime.sh`。
- [x] 更新验证结果和所有未完成项，不能将实现完成与验证通过混为同一状态。

## 验证记录

初始本地工作区干净。编译服务器原有 `/home/dominolu/dev/titan` 不含 `.git`，未覆盖或修改。本轮源码及验证均在 `/home/dominolu/dev/titan-refactor-20261012-741d2de5`；完整日志位于其 `verification-artifacts/final/`。

复验入口：`bash scripts/verify_system_refactor.sh`。脚本检查服务器 IP，拒绝在本地执行。验证顺序是格式 → check → clippy → Rust 测试及 doctest → SDK/reporting → 单一运行时检查 → AOT 制品 → 四个原生场景 → CLI 回放、报告、损坏文件拒绝和 SIGTERM。最终结果、工具链和源码摘要见 [验证报告](verification/2026-10-12/refactor_validation_report.md)。

## 实现边界与兼容性

- 新的 `max_commands_per_callback` 默认 64、`checkpoint_interval` 默认 1 秒；已有配置无需补字段。SDK 回调可返回 `None` 或字面量 ABI 状态码 `0/-1/-2/-3/-4`，C ABI V13 布局和 fingerprint 不变。
- 账户投影使用每个资产的有符号净持仓；同一资产返回多条 position side 时会拒绝 bootstrap，避免覆盖或遗漏敞口。本轮没有更改账户交易模式。稀疏持仓/余额增量保留已知的 REST 字段。
- 恢复仍严格验证 artifact、binding、state checksum 与 `public_state_identity`。公共事实或序列边界不一致时保持失败，不自动迁移旧私有状态；重连不会自动恢复原策略交易准入。
- 订单终态历史保留 1,024 条；策略历史保留约 4,096 条终态加活动订单，辅助 ID 表跟随保留集合回收。运行中的订单和未收敛成交不因历史清理丢弃。
- 替换候选账户在提交前禁止下单，也不执行取消旧账户订单的停机策略。绑定中的账户/行情替换须先释放策略资源。提交前失败保留旧 generation；提交后失败明确进入 Failed。
- 验证未接入真实账户。12 项实盘测试保持 ignored；实盘部署、真实账户对账和最小风险下单/撤单不属于本轮执行范围。


2026-10-12 验证：Rust workspace 427 项通过，12 项真实账户测试 ignored；doctest 2 项通过、1 项 ignored；Python SDK 27 项及 reporting 4 项通过。格式、check、clippy、全部原生 smoke 和 CLI 集成均通过。Clippy 的非阻断提示保留在原始日志中，未宣称 `-D warnings` 清零。


## 2026-10-13 Clippy 后续改善

- [x] 关闭接口返回拥有独立状态的 `ShutdownFuture`，通过共享 `shutdown_connector` 在普通 Mutex 释放后等待网络请求；Binance、OKX、Hyperliquid 均捕获自己的请求状态。
- [x] `CoreError` 将完整诊断上下文移入 Box，保持 `Result<_, CoreError>` 返回类型及字段读写方式；移除 core-types 原先整包的 `result_large_err` 豁免。错误创建路径新增一次分配，正常成功路径不分配。
- [x] Binance REST 成功响应使用 Box 缩小枚举；深度行情保留有界内联缓冲区，避免每条行情额外分配。
- [x] WebSocket、离线 V13、CLI 编译/运行记录、行情批次和 OKX 下单改用命名参数对象；回测订单请求归组，Tick/Bar 复用 `AuditEvent`。
- [x] 复杂队列/事务/运行状态类型提取别名；`StrategyArtifactCache` 和 `NativeDepthLevels` 补充 `is_empty()`。
- [x] 清理冗余克隆、嵌套 if、无效字段更新、索引循环、初始化后赋值、冗余模式匹配、集合转换和测试模块位置。
- [x] 六种交易所枚举实现标准 `FromStr`，原有宽容转换命名为 `from_venue_str`；未知输入继续转换为 `Unknown`。
- [x] 新增关闭 future 挂起时锁可获取、错误对象大小及诊断上下文保留两项回归。
- [x] 验证脚本将全工作区 Clippy 升级为 `-- -D warnings`，阻止新的未处理警告进入验证流程。

本轮只为无分配队列的所有权返回、内联深度行情缓冲区，以及与现有 public processor 接口一致的 `reject_order` 保留有明确说明的局部 lint 例外。没有使用整包关闭 lint 的方式清零。

Rust 源码接口变更：`Connector::shutdown` 返回独立 future；初始化/请求 API 改为参数对象；枚举宽容转换使用 `from_venue_str`。`CoreError` 通过构造器创建，不再支持直接结构体字面量或按字段解构；字段访问由 Deref 保留。仓库内调用点已全部迁移。C ABI V13 布局、交易语义、实盘测试启用状态及严格恢复校验保持原有约定。


2026-10-13 验证已在 `192.168.3.88` 完成：格式检查及无 warning 的 cargo check 通过；全工作区、全目标、全 feature Clippy 在 `-- -D warnings` 下通过、无未处理警告。Rust workspace 429 项通过、0 失败、12 项实盘测试 ignored；doctest 2 项通过、1 项 ignored；Python SDK 27 项、reporting 4 项通过。单一运行时检查、两种 AOT 制品、原生制品/恢复/排空场景及 CLI 集成全部通过。

证据：[完整脚本完成记录](verification/2026-10-13/logs/full-verification.log)、[严格 Clippy](verification/2026-10-13/logs/clippy.log)、[Rust 测试](verification/2026-10-13/logs/rust-test.log)、[SDK 测试](verification/2026-10-13/logs/sdk-test.log)、[reporting 测试](verification/2026-10-13/logs/reporting-test.log)、[CLI 集成](verification/2026-10-13/logs/cli-integration.log)。完整原始日志保存于 `docs/verification/2026-10-13/logs/`。

275 个最终源文件及配置文件摘要见 [source-files.sha256](verification/2026-10-13/source-files.sha256)，清单自身 SHA-256 为 `3604dcec89d04b93b19290bc21ce86895a96417a6a6a43c43dcd01f0b686029e`。服务器逐文件核对全部通过，见 [source-check.log](verification/2026-10-13/logs/source-check.log)。本轮未部署、未连接真实账户。


## 2026-10-13 通用恢复与策略职责简化

- [x] 新增通用 `RecoveryBaseline`，将公共事实基线与私有快照、订单历史校验和绑定；未完成成交交付的检查点在所有恢复模式中均拒绝。
- [x] 账户服务提供同一 committed epoch/version 的完整订单、持仓、余额图像；有界重试后仍不一致则返回 NotReady。
- [x] 通用运行时按 publisher generation 与 epoch 隔离旧账户会话事件；初始化账户不再伪装 READY，启动/resume 要求已提交的非零 epoch。
- [x] 通用恢复确认账户就绪、执行路由、订单归属、参数和累计成交；新检查点落盘及启动前无事实漂移后才开放下单入口。任何恢复错误将实例标记 Failed，不能绕过失败直接 start。
- [x] 补全绑定摘要：行情源、模式、时间框架和账户本地品种 → 实际资产映射；逻辑绑定不变时不把新会话 generation 当作资产迁移。
- [x] 新增可选 `on_recover` 生命周期回调，仅承担策略私有业务状态重建；恢复期间 submit/cancel 被宿主 gate 拒绝。
- [x] 外部执行前持久化完整命令批次及私有状态；意图 flush 失败时不发送订单。该同步持久化路径有额外 I/O 成本，本轮没有实盘延迟 SLA 结论。
- [x] `pair_arb` 移除账户 epoch/sequence 四个私有字段及重复会话处理；恢复回调只处理 Slot/ref/成交序列业务状态。制品版本 3.1.0、state schema 4。
- [x] `v13_smoke` 也接入通用恢复，验证协议可复用于其他策略。
- [x] 新增账户一致性、事实基线防篡改、已确认撤单、未知订单、缺失终态、离线成交变化、未完成成交、绑定变更等回归；原生测试覆盖恢复落盘失败、启动漂移及运行会话变化后的关闭行为。

可自动处理账户会话/序列变化、余额变化及有确证且累计成交不变的订单状态变化。缺失终态、无法证明的成交历史、持仓变化或未知活动订单会明确拒绝；没有推导或伪造成交，也没有自动跨制品/state schema 迁移。账户图像中的终态受保留范围限制，跨进程重启缺失的订单结果仍需补齐历史事实后才能恢复。

恢复回调扩展保持 C 结构布局，更新 ABI fingerprint 为 `6b7e11cc113413aecdaeaa27dc14549b00c6b50c8d5c5fe5e0ed599ac0269dfb`；此前 Clippy 阶段的 fingerprint 不变描述属于那一阶段。现有制品须在编译服务器重新构建；旧指纹兼容方案取消，保持严格一致性校验。SDK 契约见 [SDK README](../python/titan-strategy-sdk/README.md)。


通用恢复交付验证：编译服务器 `192.168.3.88` 上格式、check、全工作区/全目标/全 feature 的 Clippy `-D warnings` 全部通过；Rust workspace 440 通过、0 失败、12 实盘测试 ignored；doctest 2 通过、1 ignored；Python SDK 30、reporting 4 全部通过。AOT 制品、pair_arb 原生恢复回调、Service→Factory→Instance 恢复、通用恢复故障/启动漂移/publisher generation/epoch 隔离、订单排空/意图落盘失败和 CLI 集成全部通过。

证据保存在 `docs/verification/2026-10-13/recovery/logs/`：[完整脚本](verification/2026-10-13/recovery/logs/recovery-delivery.log)、[严格 Clippy](verification/2026-10-13/recovery/logs/clippy.log)、[Rust 测试](verification/2026-10-13/recovery/logs/rust-test.log)、[通用原生恢复](verification/2026-10-13/recovery/logs/common-recovery-smoke.log)、[排空与意图持久化](verification/2026-10-13/recovery/logs/drain-smoke.log)、[SDK](verification/2026-10-13/recovery/logs/sdk-test.log)、[CLI 集成](verification/2026-10-13/recovery/logs/cli-integration.log)。

277 个最终源码及配置文件的 [摘要清单](verification/2026-10-13/recovery/source-files.sha256) 已逐文件与服务器验证版本核对，全部匹配。清单自身 SHA-256：`e4aee2f4965a814165504d3bb4d7effd0c524a27982f64f8e873a15eea47ab0f`；[核对日志](verification/2026-10-13/recovery/logs/recovery-source-check.log)。本轮未提交、未部署、未连接真实账户。

## 2026-10-13 通用双向持仓修复

- [x] Bootstrap 以资产和方向为唯一键；允许 LONG、SHORT 两条记录，包括两条零仓位；拒绝重复方向、未知方向及 NET/hedge 混合记录。
- [x] 账户缓存及更新去重以 `(asset_id, position_side)` 为键，单侧清零不会覆盖另一侧；单侧 REST 元数据分别保留，私有 WS 更新保留交易所方向及可用价格/PnL。
- [x] Binance BOTH 统一映射 NET；OKX SHORT 数量统一为负；Hyperliquid 单向持仓保持 NET，不以数量正负改变持仓身份。
- [x] 策略 PositionView、PositionEventView、OrderEventView、ActiveOrderView 和 SubmitOrderRequest 贯通方向；订单历史和恢复基线保存方向，公共状态摘要按账户、资产、方向排序。
- [x] SDK 支持 `ctx.position(account, asset, PositionSide.LONG/SHORT)` 和显式方向下单；默认 NET 不自动合并双侧持仓。缺失方向返回稳定错误。
- [x] 下单接口不再固定丢弃方向；Binance NET 转 BOTH，双向平仓省略 reduceOnly；Hyperliquid 不支持的方向明确拒绝。没有调用任何账户模式修改接口。
- [x] 账户快照安装/单侧清零、方向时间戳隔离、双侧零仓、REST/Wire 参数、原生 SDK 双侧查询/平空命令及通用恢复覆盖相应回归场景。

本轮改变 ABI 布局与字段语义，V13 fingerprint 为 `7864d0be8e1b83b0683747bf4fdfd4cce1f87377cb3fdb97c56a9a270c7a50ed`；所有策略制品必须重新编译，旧快照不自动迁移。策略决定操作哪一侧；当前 `pair_arb` 默认按 NET 交易，不自动改为双向策略。此项修复支持账户的双向状态保存、查询、恢复及明确方向的交易请求，不宣称已有策略或净持仓回测引擎自动具有双向交易业务规则。

最终验证在 `192.168.3.88` 完成：格式、全目标/全 feature 构建检查、严格 Clippy（`-D warnings`）通过；Rust 444 项、doctest 2 项、Python SDK 31 项、reporting 4 项通过，0 失败。12 项实盘测试及 1 项示例 doctest 保持 ignored。单一运行时检查、两种 AOT 制品、原生策略、双侧持仓的通用恢复、排空/检查点失败、CLI run/report/损坏制品/SIGTERM 全部通过。完整脚本通过后，仅新增一项下单方向测试；补跑格式检查、严格 Clippy 与完整 Rust 回归再次通过，生产代码没有新增修改。

证据：[完整验证](verification/2026-10-13/positions/logs/titan-position-delivery.log)、[最后测试补跑](verification/2026-10-13/positions/logs/titan-position-last-test.log)、[Rust](verification/2026-10-13/positions/logs/rust-test.log)、[严格 Clippy](verification/2026-10-13/positions/logs/clippy.log)、[原生 SDK](verification/2026-10-13/positions/logs/sdk-test.log)、[双侧恢复](verification/2026-10-13/positions/logs/common-recovery-smoke.log)、[CLI](verification/2026-10-13/positions/logs/cli-integration.log)。280 个源码、配置及脚本文件的[摘要清单](verification/2026-10-13/positions/source-files.sha256)与编译服务器逐文件核对；清单 SHA-256 为 `993f88808e703db63d49a7b3b0db0dd5c611c4f14769fe68e2f7c75b472aefd2`。本轮未提交、未部署、未连接真实账户。

## 2026-10-13 完整 review：初始发现（已按下节修复）

审查范围为 `741d2de5` 之后的系统重构提交 `586f0f47`，以及当前未提交的 Clippy、通用恢复和双向持仓修复：99 个已跟踪源码/脚本/配置差异，加 3 个新增生产/测试源码，共 102 个路径。重点检查账户准备及快照边界、REST/WS 对账、订单意图持久化、下单准入、事件发布并发、生命周期/关闭、策略 SDK/ABI、pair_arb、回测、采集和 CLI。生产源码未在本次 review 中修改。

结论：发现 7 项需要处理的问题，其中 6 项 P1、1 项 P2。之前的 444 项 Rust 和 35 项 Python 回归通过不构成实盘就绪结论。四项新缺陷探针已在 `192.168.3.88` 的独立目录 `/home/dominolu/dev/titan-review-20261013` 复现；另三项为源码确认，未接真实账户验证。本次未执行实盘测试、未部署、未修复或提交这些发现。

### R1 — P1：复合快照把分类版本误当成统一提交边界

位置：[recovery_snapshot](../crates/titan-account-service/src/service.rs#L50)、[账户快照返回](../connector/src/account_runtime.rs#L297)、[缓存版本](../connector/src/account_projection.rs#L149)、[策略启动捕获](../crates/titan-strategy-runtime/src/service_core.rs#L889)。

订单、持仓和余额各自保存最后一次更新的 `account_version`；编码器为每一条事件分配不同版本。新 `recovery_snapshot` 要求三类版本完全相等，即使所有数据都处于同一个稳定 epoch，也永远不能满足。启动快照包含订单 version=1、持仓=2、余额=3 的真实 AccountProjection，经同样快照接口包装后，8 次重试仍返回 NotReady。没有订单时订单版本为 0，问题同样存在。ServiceCore 在首次创建/准备策略时也调用该接口，因此影响首次启动，不只影响旧快照恢复。

修复建议：提供在同一账户投影锁下捕获三类数据的复合快照接口，把共同提交边界与各类最后更新版本分开；不要直接删除一致性校验。回归应使用真实 AccountProjection，而不是让 mock 固定返回同一个版本。

证据：[版本及空仓探针日志](verification/2026-10-13/review/logs/titan-review-projection.log)、[探针源码](verification/2026-10-13/review/probes/titan_review_projection_probe.rs)。

### R2 — P1：定时回调命令 flush 失败后仍保持 Running、下单入口开启

位置：[命令意图 flush](../crates/titan-strategy-runtime/src/runtime_v13.rs#L813)、[定时回调错误处理](../crates/titan-strategy-runtime/src/runtime_v13.rs#L1787)、[控制队列只返回错误](../crates/titan-event-engine/src/primary.rs#L483)。

普通事件路径由 handle_event 调用 fail_runtime，但 fire_timer 把 invoke/commit_commands 的错误映射成 SafePointPanicked 后只发送给控制 ticket；fire_timer 丢弃该 ticket，后台调度器只能看到排队成功。已有 submit 成功、随后意图 flush 失败时，本批命令未发出，但已经写入的 PendingSubmit、私有订单角色和 staging 状态没有完成回滚或统一失效，策略仍能接收后续回调。原生 AOT 定时下单探针实测 `lifecycle=Running, gate_open=true`。

修复建议：所有回调入口共享失败处理；commit/flush/队列错误都必须统一 fail_runtime 并关闭 activation，保存具体错误。补充定时 submit/cancel 的意图提交、flush 超时和暂时失败测试。普通 checkpoint submit 失败已有明确关门逻辑；对照探针实测 Failed/closed，此路径不列为缺陷。

证据：[flush 失败日志](verification/2026-10-13/review/logs/titan-review-timer-flush.log)、[原生探针](verification/2026-10-13/review/probes/v13_review_timer_flush.rs)、[submit 失败正常关门对照](verification/2026-10-13/review/logs/titan-review-timer.log)。

### R3 — P1：同步回调跨来源发布会形成来源锁死锁

位置：[来源锁和回调派发](../crates/titan-event-engine/src/engine.rs#L416)、[仅阻止同来源重入](../crates/titan-event-engine/src/engine.rs#L631)。

publish_record 持有 source_sequences[source] 的普通 Mutex，直到 dispatch_fast_lanes 完成。线程 A 发布来源 1，inline 回调再发布来源 2；线程 B 发布来源 2，回调再发布来源 1，就形成锁环。同来源递归保护按 `(engine, source)` 匹配，不阻止此情况。两个并发发布者的有界探针实测 0/2 完成，双方均被阻塞，关闭也无法正常等待 active_publications 清零。

修复建议：在来源顺序保证与同步回调之间建立明确的重入规则；可拒绝同一引擎的嵌套 sequenced 发布，或通过有序队列派发，避免在外部回调中持有可能跨来源互锁的锁。不要简单释放锁而破坏来源顺序。回归必须覆盖同来源重入与跨来源互相发布两种情况。

证据：[死锁探针日志](verification/2026-10-13/review/logs/titan-review-engine.log)。探针在独立进程中采用有界等待，未阻塞生产运行或访问账户。

### R4 — P1：下单准入只在构造 future 时校验，旧会话请求可延迟发送

位置：[future 构造前检查](../connector/src/account_runtime.rs#L692)、[实际 REST 发送](../connector/src/account_runtime.rs#L791)、[执行线程延后 poll](../crates/titan-account-service/src/execution.rs#L444)。

direct_submit 检查 ready/active/readiness_fence 后返回 future，但 future 只捕获 api、connector 和 binding，没有捕获或再次校验会话 revision/epoch。ExecutionRuntime 可延迟 poll。若请求排队期间发生失效、停止或私有连接重建，即使策略已关门，尚未真正发送的旧请求仍能调用 submit_order；也可能在新 epoch 已 Ready 后才发出。当前检查只能阻止构造新 future，无法隔离已构造但未发送的旧会话意图。

修复建议：明确下单发送的准入线性化点，为请求携带会话 fencing token，并在执行任务开始及提交交易所请求前核对 token；会话失效后撤销未发送意图。测试应人为延迟 future poll，再触发 invalidate/stop/新 epoch，验证没有调用 mock broker。已经发送到交易所的请求继续按未知结果对账，不能当作未发送请求处理。

证据等级：源码确认，尚未运行账户级模拟 future 延迟探针；无实盘操作。

### R5 — P1：允许下单但未声明账户回调的策略缺少宿主必需的私有事实订阅

位置：[订阅自动补齐条件](../crates/titan-strategy-runtime/src/service_core.rs#L705)、[REST 终态等待私有事实](../crates/titan-strategy-runtime/src/runtime_v13.rs#L1233)、[仓库 smoke 策略](../strategies/v13_smoke/strategy.py#L19)。

ServiceCore 只在 manifest 已声明至少一个 titan.account 订阅时补齐内部私有事件。SDK 允许 BBO-only + ORDER_EXECUTION，仓库 v13_smoke 就是该配置。这类策略可以下单，但宿主接收不到 Order/Fill/Position/账户失效事件。新增 REST 处理又明确保留已成交订单为非终态，等待私有事实，因此订单不能正常退役、停止排空可超时，公共持仓和账户会话也不能更新。业务回调是否订阅与宿主是否需要维护账户状态是两回事。

修复建议：只要绑定账户并具有账户/执行能力，就独立补齐宿主所需事实订阅；策略可以不声明 on_order/on_fill，宿主仍必须更新自己的状态。用 ServiceCore 实际创建 BBO-only 执行策略，再推送私有 Fill/终态/失效事件验证排空，不能在测试中手工给运行时附加全部订阅而绕过此路径。

证据等级：源码及合法 SDK 策略配置确认；该条件原本已存在，本轮新增 REST 终态等待及通用恢复扩大了它的影响，属于本轮集成缺口。

### R6 — P1：REST 无持仓记录时仍伪造 NET 空仓，双向模式支持不完整

位置：[缺失记录的 NET 占位](../connector/src/account_runtime.rs#L1016)、[方向模式检查](../connector/src/account_projection.rs#L41)。

get_positions(None) 没有匹配记录时，本项目仍生成 NET=0，且用本地 now_ns 当作交易所时间戳。对于空仓的双向账户，后续真实 LONG/SHORT 私有更新会被判定为 mode changed，触发失效；策略按 LONG/SHORT 查询也拿不到正确的零仓位视图。探针确认 NET 占位拒绝首次 LONG。OKX 全局 positions 查询可能省略零仓位记录，不能从缺失行推断账户为 NET：[官方 Get positions 说明](https://www.okx.com/docs-v5/en/#trading-account-rest-api-get-positions)。本地生成的时间戳也不能当成 REST 已覆盖历史 WS 更新的证据。

修复建议：读交易所持仓模式/能力，或显式保留“空仓、模式尚未知”的状态；模式与经济持仓事实分开。双向模式零仓视图由已确认模式建立，不能自动推断为 NET。补充 REST 空列表 + LONG/SHORT 缓冲更新，以及 REST/WS 时间戳边界的初始化测试。

证据：[版本及空仓探针日志](verification/2026-10-13/review/logs/titan-review-projection.log)。

### R7 — P2：Hyperliquid 成交后的持仓价格元数据不会更新，正常重启也可被拒绝

位置：[成交后只发布数量](../connector/src/hyperliquid/ws.rs#L614)、[恢复比较持仓价格](../crates/titan-strategy-runtime/src/recovery.rs#L108)。

Hyperliquid fill 更新 quantity，但 entry_price/unrealized_pnl/liquidation_price 都为 None，账户投影因此沿用 bootstrap 时的价格。首次从零开仓，typed position 可变成 qty>0、average_price=0；加仓也会留下旧开仓价。检查点把这些旧字段保存为恢复基线；停机期间即使无成交，当前 REST 返回正确均价，通用恢复仍会报 recovery_position_change_requires_history。其背景是新恢复协议把平均价作为经济状态一致性条件，不能由不完整的 WS 投影稳定重建。

修复建议：在私有流中使用完整、权威的持仓快照更新价格/PnL；若 fill 本身不足以证明这些字段，应触发受版本边界保护的 REST 持仓刷新或明确标记字段未就绪，不应把旧值当作当前事实落盘。补充从零开仓、加仓、无离线成交重启的 Hyperliquid 模拟测试。

证据等级：源码确认；未连接 Hyperliquid 账户复验。

### 修复顺序及回归要求

1. 先修 R1，使真实账户服务的首次启动和原子复合快照成立。
2. 修 R2、R3、R4，统一失败关门与请求 fencing，消除死锁和旧会话延迟下单。
3. 修 R5、R6、R7，补齐宿主订阅、空仓模式及完整持仓事实。
4. 将本次四项负向复现迁移为正式回归；补齐三项尚未动态复现的模拟场景，再在编译服务器运行完整非实盘验证。

普通 Clippy 的接口/类型整理、共享 lifecycle 工具、关闭 future 释放 Mutex、CLI ResultBundle 路径校验及采集/回测参数提取，未发现同等置信度的新增阻断问题；这不表示其所有运行场景均已验收。同步 checkpoint+fsync 位于 PRIMARY 下单路径，是另一个已知性能取舍，需单独测量延迟/背压预算，不用未经授权地放松持久化来消除等待。

原验证目录 280 个源码/配置/脚本的摘要在审查前后均匹配：[审查前核对](verification/2026-10-13/review/logs/titan-review-precheck.log)、[审查后核对](verification/2026-10-13/review/logs/titan-review-original-source-check.log)。本次只增加 review 文档及证据，原生产源码、严格 ABI 校验和实盘测试忽略状态未修改。

## 2026-10-13 Review R1–R7 修复完成

上节保留初始审查记录；后续依据该记录修复 R1–R7。通用恢复、订单准入、事件并发和账户模式逻辑均在宿主/账户/连接器层处理，未向 pair_arb 添加平台恢复逻辑。Binance/OKX 原先拒绝双向模式的配置入口也一并修正；模式由交易所决定，本项目只读取和验证。

- [x] R1：同一投影锁下的复合快照和共同提交版本。
- [x] R2：定时及其他回调的命令提交失败统一关门。
- [x] R3：拒绝同步回调嵌套 sequenced 发布，消除跨来源锁环。
- [x] R4：submit future 捕获会话 token，首次执行和原生 broker 请求准备后复查；拒绝未发送订单并清理 pending 映射。
- [x] R5：按账户绑定补齐宿主私有事实订阅，覆盖 BBO-only 实际 ServiceCore 创建/恢复/账户失效流程。
- [x] R6：读取真实模式建立 Net/Hedge 空仓视图，未知模式保持保守；合成 exchange_ts=0，缓冲 LONG/SHORT 更新不被本地时间误丢弃。
- [x] R7：Hyperliquid 使用 clearinghouseState 完整持仓图片，更新数量、均价、盈亏和清算价。
- [x] 编译服务器全量非实盘验证：Rust 446、doctest 2、Python 35；严格 Clippy、AOT、恢复、排空、定时失败和 CLI 集成全部通过。

详细变更、回归范围、验证日志及 282 个源码校验和见 [修复验证报告](verification/2026-10-13/review-fixes/README.md)。12 项实盘测试仍未启用，未部署或操作真实账户。严格 ABI/制品/state schema 校验继续保留。本轮修复与验证证据一并提交 GitHub。

## 2026-10-21 编译与制品职责收敛到 Rust

- [x] Rust 统一调度 describe/Numba 对象生成、资源/超时限制、SIGINT/SIGTERM 取消及子进程组清理。
- [x] Rust 校验对象协议和符号、调度 nm/objcopy/cc、生成 ABI descriptor 并检查动态依赖与导出。
- [x] Rust 统一 canonical CBOR、manifest、摘要、Ed25519 签名、pair/bundle 发布和失败回滚；复用现有 runtime loader 校验。
- [x] 精简 Python SDK：保留策略、必要 Numba/ABI 桥接及相关测试，删除打包/签名/制品校验和进程调度；报告代码不变。
- [x] 完整编译服务器验证通过：Rust 453、doctest 2、Python SDK 30、报告 4；严格 Clippy、AOT、恢复、排空、CLI、重复构建、签名及实际编译中断验证通过。

测试同步竞态的修复仅限 EventEngine 测试接收线程 join，生产事件引擎不变。新旧制品继续采用相同严格 ABI 校验；未部署或执行实盘测试。详细边界、环境、源码校验和及日志见 [本轮验证报告](verification/2026-10-21/native-compiler/README.md)。
