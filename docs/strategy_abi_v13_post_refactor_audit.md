# Strategy ABI V13 重构后适配审计

状态：P0/P1/P2 整改完成；仅待另行授权的受控实盘验证

审计日期：2026-09-17；P0 复审修订：2026-09-24

审计范围：Strategy ABI、Strategy Runtime、Account/Execution、Connector、`pair_arb`、CLI、回测、部署、测试与文档。

## 1. 结论

当前仓库已经完成生产策略 loader 向 `native-v13` 的硬切换。本轮已修复会直接改变订单语义、
仓位判断和重启行为的全部 P0 问题；项目级 P1/P2 迁移项仍按本文后续清单推进。

P0 整改已在编译服务器 `192.168.3.88` 通过 Rust workspace 全目标检查、相关 Rust crate 测试和
Python Strategy SDK 测试。实盘启用仍需完成部署前风险参数确认和受控最小风险验证：

- `deploy/pair_arb_v13` 必须继续保持禁用；
- 未完成受控实盘验证前不得启用 `pair_arb`；
- 不得将非实盘测试视为交易所下单链路验收；
- 回测与实盘已经统一到同一种 V13 制品和 callback/context 契约，但不得据此宣称实盘链路已验收。

本次整改未在本地执行构建或测试；所有构建和非实盘测试均在规定的编译服务器完成。

## 2. 风险分级

- **P0**：可能造成错误下单、漏对冲、错误仓位判断、遗漏订单或遗漏成交，必须在任何实盘验证前修复。
- **P1**：阻止仓库完成全量 V13 迁移，或导致公开命令与实际能力不一致。
- **P2**：文档、测试和开发工具不一致，会降低后续维护与验收可信度。

## 3. P0：实盘安全阻断项

### P0-1 订单 TIF 映射错误

V13 SDK 定义：

| V13 TIF | 值 |
|---|---:|
| GTC | 1 |
| IOC | 2 |
| FOK | 3 |
| POST_ONLY | 4 |
| GTD | 5 |

账户执行层使用：

| Direct TIF | 值 |
|---|---:|
| GTC | 0 |
| GTX/PostOnly | 1 |
| FOK | 2 |
| IOC | 3 |

当前 `runtime_v13.rs` 使用顺序映射 `1→0、2→1、3→2、4→3`，导致：

- V13 `IOC` 被发送为 `GTX/PostOnly`；
- V13 `POST_ONLY` 被发送为 `IOC`。

这会直接影响 `pair_arb` 的 hedge IOC：需要立即成交的对冲单可能被转换为只挂单。

证据：

- `python/titan-strategy-sdk/titan_strategy/types.py`
- `crates/titan-strategy-runtime/src/runtime_v13.rs`
- `connector/src/account_runtime.rs`

整改清单：

- [x] 建立显式的 V13 TIF 到 Direct TIF 转换函数；
- [x] 禁止依赖枚举数值碰巧一致；
- [x] 增加转换边界测试，并复用已有 Connector wire 测试；
- [x] `pair_arb` hedge IOC 显式转换为 Direct IOC。

### P0-2 订单状态、订单类型和 TIF 事件未经转换

Account/Connector 当前订单状态编码为：

| 账户状态 | 值 |
|---|---:|
| New | 1 |
| Expired | 2 |
| Filled | 3 |
| Canceled | 4 |
| PartiallyFilled | 5 |
| Rejected | 6 |

V13 SDK 状态编码为：

| V13 状态 | 值 |
|---|---:|
| Pending | 1 |
| Accepted | 2 |
| PartiallyFilled | 3 |
| Filled | 4 |
| CancelPending | 5 |
| Canceled | 6 |
| Rejected | 7 |
| Expired | 8 |

当前 runtime 将账户状态原值直接写入 `TitanOrderEventView.status`。因此可能出现：

- Filled 被策略解释为 PartiallyFilled；
- Canceled 被策略解释为 Filled；
- PartiallyFilled 被策略解释为 CancelPending；
- Expired 和 Filled 未按 V13 terminal 规则从 active orders 移除；
- cancel callback 的触发条件与最终状态错误。

启动快照中的 `order_type`、`time_in_force` 和 `status` 也存在相同问题。

整改清单：

- [x] 在 Strategy Runtime 边界建立唯一 canonical 转换层；
- [x] 对事件流与启动快照复用同一套转换；
- [x] 对未知枚举 fail closed，不得直接透传；
- [x] 覆盖 terminal/non-terminal 账户状态转换；
- [x] 修正 active order 插入、更新、移除及 terminal callback 顺序。

### P0-3 `reduce_only` 和高级订单字段被丢弃

V13 submit request 已公开：

- `reduce_only`；
- `trigger_price_ticks`；
- `trigger_kind`；
- `gtd_expiry_ns`；
- STOP_LIMIT / STOP_MARKET / GTD。

但 `DirectNewOrderRequest` 不包含这些字段，Connector 最终还硬编码
`reduce_only = false`、`stop_price = None`。

这属于“SDK 接受、实盘静默忽略”，必须消除。

整改方案二选一：

1. 将字段完整贯通到 AccountService、Connector 和各交易所 wire；或
2. 从当前 V13 SDK 删除未支持能力，并在静态编译/command staging 阶段明确拒绝。

整改清单：

- [x] `reduce_only` 不再丢失；
- [x] 从公开 SDK 收窄 STOP/GTD，并在 command staging fail closed；
- [x] ActiveOrder/OrderEvent 能准确返回 `reduce_only`；
- [x] 复用并通过 OKX、Hyperliquid、Binance 现有 wire 级测试。

### P0-4 `pair_arb` 启动时未读取初始仓位和账户状态

`pair_arb.on_start` 当前只核对活动订单，随后直接设置：

- `READY_ACCOUNTS`；
- `READY_POSITIONS`；
- `READY_RECONCILED`。

它没有从 `ctx.position()` 读取两腿初始仓位，也没有从 `ctx.account()` 检查账户状态、epoch
和 sequence。若账户启动时已有仓位，策略私有状态仍可能认为仓位为零并开始新 Slot。

整改清单：

- [x] `on_start` 读取并保存两腿初始仓位；
- [x] 检查两个账户均为 Ready；
- [x] 保存 account epoch/sequence；
- [x] 未被 checkpoint 解释的非零仓位保守 HALT；
- [x] 公共状态与私有状态未完成对账时禁止 Quote；
- [x] 增加非零仓位和非 Ready 账户测试，epoch 冲突走 fail-closed 分支。

### P0-5 重启恢复与跨 generation 订单接管缺失

当前服务只允许 `recovery = fresh`。client order prefix 同时包含 strategy id 和 runtime generation，
导致重启后：

- 旧 generation 活动订单不会进入新实例的 active order view；
- 旧订单迟到 Fill 会被忽略；
- 新实例可能在未处理旧订单/旧成交时重新 Quote。

虽然 V13 instance 已具备 `freeze_state`/`restore_state` 基础结构，但 service 使用禁用的 snapshot
sink，CLI 也明确拒绝非 Fresh recovery。

整改清单：

- [x] 实现异步、原子提交的 durable checkpoint sink/store；
- [x] 接通 `RestoreLatestCheckpoint` 与 `RequireCheckpoint`；
- [x] 定义稳定的策略订单 ownership namespace；
- [x] 启动快照扫描旧 generation 订单，无法与私有状态对账时保守 HALT；
- [x] 旧 generation late Fill 可按稳定 namespace 路由，策略历史表负责去重；
- [x] 校验 artifact/binding/state schema/public-state identity；
- [x] 增加 checkpoint 落盘回读、跨 generation ownership 与 ID 不重用测试。

### P0-6 REST execution 结果没有返回策略 lane

Direct execution 的 Accepted/Rejected/Unknown 目前只进入日志 observer。策略无法及时区分：

- 请求已接受；
- 明确拒绝；
- outcome unknown；
- 最终订单事实尚未到达。

同时 `TitanCancelEventView.request_result` 当前固定为 `0`，不能表达真实撤单请求结果。

整改清单：

- [x] 将 execution observer 结果转换为策略事件；
- [x] 结果进入对应策略的 PRIMARY lane safe point；
- [x] 关联 task id、request kind、asset、client order id、strategy generation 和 account；
- [x] 明确区分 request result 与最终订单状态；
- [x] reject/unknown 显式反馈，迟到 WS 回报仍由 canonical order fact 收敛。

### P0-7 P0 修复复审发现的启动窗口与持久化缺口

复审确认初版 P0 修复仍有四个实盘阻断缺口：PRIMARY lane 在 seed/restore/start 前已接收事件但
runtime 会静默丢弃；暂停期间账户事实不再推进；checkpoint 后台写失败只记录日志且 generation
可能在崩溃后复用；execution result 队列失败会丢失结果。此外，订单、仓位、余额启动快照来自
独立查询，旧实现没有校验 epoch，也错误地把 position version 用作订单 sequence。

整改清单：

- [x] Defined/Ready 阶段持续更新公开视图但不调用策略回调，seed 按投影版本合并，避免覆盖较新事实；
- [x] Paused 阶段继续处理账户回调，同时保持命令门关闭；账户流非 Ready 时自动进入 Paused；
- [x] 启动快照校验三类投影 epoch，并分别保存 orders/positions/balances 的 committed version；
- [x] order snapshot 增加 per-account boundary，正确处理快照中缺失的终态订单与启动窗口 Fill；
- [x] generation 在实例创建和 replace 前同步原子持久化，文件与父目录均执行 fsync；
- [x] checkpoint writer 错误通过 sink health 传播，下一事件或 timer 立即关闭激活门并置 Failed；
- [x] execution result 入队或派发失败立即 fail-closed，暂停期间结果仍进入状态机；
- [x] REST Accepted 按 DirectOrderInfo.status 映射为 V13 Order 事件，且不覆盖更高权威的私有流事实；
- [x] 有账户执行绑定的策略强制配置 durable checkpoint root；
- [x] 增加 generation watermark 跨 store restart 单元测试。

2026-09-24 最终复验：旧 `titan-python-host` 已从 workspace 删除；编译服务器完成
`cargo fmt --all -- --check`、`cargo check --workspace --all-targets`、
`cargo clippy --workspace --all-targets` 和 `cargo test --workspace --all-targets`。Rust 全 workspace
共 407 项测试通过、12 项需实盘凭证的测试按设计 ignored；Python Strategy SDK 18 项测试及
5 项 subtest 全部通过。Clippy 命令成功，但仓库仍有本次迁移范围外的既有 warning，未将
`-D warnings` 列为本轮完成门禁。


## 4. P1：项目级 V13 迁移项（已完成）

### P1-1 回测链路与旧 ABI 清理

- [x] 通用结构迁入无策略版本语义的 crates/titan-domain-types；
- [x] 删除可执行的 titan-runtime、titan-python-host 与旧 Rust examples；
- [x] 删除 V12 StrategyRuntimeContext、双数组 state、BacktestCommandBuffer 和 ABI 12 descriptor；
- [x] HftBacktest 暴露 backtest::strategy_v13，复用 canonical OfflineV13Adapter；
- [x] CLI V13 trace backtest 与 Core Live 加载同一 .titan artifact；
- [x] scripts/check_single_runtime.sh 对 V12 符号回归 fail-fast。

### P1-2 CLI V13 artifact 化

- [x] strategy ls/show/validate 读取 .titan canonical manifest；
- [x] run/validate -e backtest 只接收 V13 artifact 与版本化 V13 trace；
- [x] 删除旧 StrategyManifest、Python paths、部署参数和动态 entrypoint；
- [x] compile/inspect/validate/run 共享 ArtifactManifestV13；
- [x] help、RunSpec 和 JSON 输出只描述真实可运行的 Tick profile。

编译服务器已用 pair_arb.titan 完成真实 CLI 回放：3 个事件、1 次 submit、1 次 cancel，状态 COMPLETED。

### P1-3 Bar/Hybrid 能力收缩

本次选择“未完整前不发布”，而不是保留半实现：

- [x] SDK 拒绝 BAR subscription，防止生成可运行 Bar artifact；
- [x] CLI 与 Core Live 配置拒绝 Bar/Hybrid；
- [x] Strategy Service 路由边界不接受 BarBatch；
- [x] dual_ma/event_counter 改为 Tick 示例；
- [x] 增加拒绝行为测试和文档说明。

Bar/Hybrid 不属于当前已发布 V13 profile；重新开放时必须另行完成 producer、同周期顺序、空 bar、
timeframe 和 live/offline 端到端测试。

### P1-4 artifact 信任策略

- [x] runtime 配置 trusted_ed25519_keys 并严格解析 32-byte public key；
- [x] 有策略的生产配置默认 require_artifact_signature = true；
- [x] unsigned artifact 仅能通过显式 allow_unsigned_artifacts = true 用于开发/shadow；
- [x] key map 可同时配置 current/next key，支持轮换；
- [x] loader 联合校验 signature、artifact digest、native digest、target 和 descriptor；
- [x] Python/Rust CLI 编译支持 --signing-key 与 --key-id，并有签名编译测试。
- [x] CLI live 预检复用 Core 配置的同一信任策略，backtest RunSpec 固化并在 worker 重验同一策略。

### P1-5 单一权威配置

- [x] 从 StrategyDefinition 删除 entrypoint、parameters、parameter_schema_version 和部署侧 subscriptions；
- [x] artifact manifest 成为参数 schema、订阅、callback 与 capability 的唯一权威；
- [x] 部署配置只保留 artifact、binding、risk scope、recovery 和 runtime policy；
- [x] runtime 固定 timer callback 与策略 capability 的命名已分离。

## 5. P2：文档、测试与工具

### P2-1 文档和示例配置

- [x] README 与 CLI 文档改为离线 AOT、V13 artifact、Tick trace/Core Live；
- [x] pair-arb 需求/设计升级为 ABI V13，并删除 strategy.json；
- [x] V13 技术设计标记已实现并写明 Tick-only 边界；
- [x] dual_ma 配置改为 V13 trace，删除失效的旧 live 配置；
- [x] 新增 pair-arb/dual-ma V13 trace 示例；
- [x] 仍含 V8/V12 内容的旧方案文档加上显著历史归档标记并链接当前 V13 权威文档。

### P2-2 contract tests

- [x] OrderType/TIF/Status runtime 显式转换；
- [x] IOC hedge 与 reduce-only 的 staging → Account → venue wire；
- [x] REST Accepted/Rejected/Unknown 回送策略 lane；
- [x] partial fill、hedge obligation、cancel-confirm-replace 和重复 fill；
- [x] 非零初始仓位、非 Ready account、恢复时缺失活动订单；
- [x] generation order id 隔离、旧 generation ownership 与迟到事实；
- [x] checkpoint round-trip、writer health 与 generation watermark restart；
- [x] Bar/Hybrid 未发布路径的 SDK/CLI/Core 三层拒绝；
- [x] Binance、OKX、Hyperliquid wire/status/partial-fill/reduce-only；
- [x] CLI 真实 pair-arb artifact V13 trace smoke。

### P2-3 CodeGraph

- [x] 代码完成后重新执行 codegraph index；
- [x] 索引不再返回已删除的 V12 runtime/python-host 符号。

强制全量重建后索引包含 211 个文件；`StrategyRuntimeContext` 查询仅返回
`StrategyRuntimeContextV13`，`BacktestCommandBuffer` 无结果。由于删除尚未提交时 CodeGraph
仍会扫描 Git tracked 路径，日志保留 11 条已删除文件的 `ENOENT`，但这些文件和符号均未进入新索引。

## 6. 最终能力边界

1. 开发输入是 strategy.py + parameters.json，部署和运行输入是 .titan；
2. 回测通过 V13 offline/HftBacktest adapter，实盘通过 Strategy Service，共享 callback/context/typed state；
3. 生产默认强制签名，checked-in pair-arb 模板仅作为 disabled development/shadow 示例允许 unsigned；
4. 当前只发布 Tick；Bar/Hybrid 稳定拒绝；
5. 实盘账户验证必须在独立授权后按最小风险流程执行。

## 7. 完成门禁

- [x] 仓库中不再存在可执行的 V12 strategy context/loader/host；
- [x] V13 enum 和订单字段跨 SDK、Runtime、Account、Connector 一致；
- [x] 同一 .titan artifact 可运行于回测和实盘；
- [x] pair-arb 启动以账户真实仓位、活动订单和 epoch 为准；
- [x] reject、outcome unknown、迟到事实和重复事实均有确定行为；
- [x] checkpoint/restart 保留旧订单、旧成交与 generation watermark；
- [x] Bar/Hybrid 未公开支持且在所有公开入口 fail-fast；
- [x] 生产 artifact 默认强制签名；
- [x] CLI、README、技术设计和部署模板与实现一致；
- [x] 编译服务器完成格式、编译、单元测试、Python AOT 测试和 V13 CLI smoke；
- [ ] 受控实盘最小下单/撤单验证：需要用户另行明确授权，不是本次离线重构的一部分。

最终 V13 CLI smoke 使用 checked-in `pair_arb.titan` 与 Tick trace 完成 3 个事件，产生 1 次
submit、1 次 cancel，最终状态 `COMPLETED`；制品 digest、trace digest 与 typed-state digest
均写入结果。`scripts/check_single_runtime.sh` 在编译服务器没有 `rg` 的环境下通过 portable
`grep` fallback 完成同一门禁。

## 8. 主要证据文件

- `python/titan-strategy-sdk/titan_strategy/types.py`
- `python/titan-strategy-sdk/titan_strategy/abi_v13.py`
- `crates/titan-strategy-runtime/src/runtime_v13.rs`
- `crates/titan-strategy-runtime/src/service_core.rs`
- `crates/titan-strategy-runtime/src/artifact.rs`
- `crates/titan-account-service/src/execution.rs`
- `connector/src/account_runtime.rs`
- `strategies/pair_arb/strategy.py`
- `crates/titan-domain-types/src/lib.rs`
- `crates/titan-strategy-runtime/src/offline_v13.rs`
- `hftbacktest/src/backtest/strategy_v13.rs`
- `crates/titan-cli/src/main.rs`
- `crates/titan-cli/src/core_runtime.rs`
