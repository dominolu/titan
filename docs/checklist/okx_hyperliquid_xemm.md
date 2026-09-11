# OKX–Hyperliquid XEMM 模块调试问题与单元测试 Checklist

## 1. 文档目的

本文记录 2026-09-09 OKX–Hyperliquid 主网跨所做市联调中实际暴露的问题，并将每个问题转换为可执行的测试检查项。后续新增测试时，应优先覆盖真实生产调用链，而不是只测试单个结构体的编码或单个函数的正常分支。

状态约定：

- `[x]`：仓库中已经存在直接覆盖该行为的测试。
- `[ ]`：仍需补充，或现有测试没有经过真实故障边界。
- `P0`：可能造成裸露仓位、错误方向交易或策略失效。
- `P1`：可能造成行情失真、恢复失败或难以定位生产问题。
- `P2`：主要影响部署效率和可运维性。

## 2. 本次调试结果基线

最终成功的最小主网闭环：

- OKX maker：卖出 `0.02` 张 `BTC-USDT-SWAP`，合约乘数换算为 `0.0002 BTC`，成交价 `79120.1`。
- Hyperliquid hedge：买入 `0.0002 BTC`，成交价 `79135.0`。
- 两边交易所成交时间戳相差约 `555 ms`。
- 对冲后组合 BTC 净敞口为 0，双方无挂单。
- 测试结束后使用 reduce-only 清理，双方最终仓位为 0。

该结果应作为后续端到端回归测试的最小验收样例。

## 3. 问题总表

| ID | 优先级 | 现象 | 根因 | 已采取修复 |
|---|---|---|---|---|
| XEMM-001 | P0 | OKX Fill 后立即 `StreamInvalidated`，策略没有对冲 | 动态账户 ABI 未携带 schema version，宿主把 `FillV2` 按 Fill v1 长度校验 | 宿主根据授权事件类型和固定 payload 长度识别 Fill v1/v2，并按正确 schema 发布 |
| XEMM-002 | P0 | Runtime 报 `subscriber_failed` | 策略对处于 `CANCELING` 的订单重复撤单，确定性 command ID 冲突，返回 `duplicate_command_id` | `cancel_bid`、`cancel_ask` 在 `LEG_CANCELING` 时直接返回 |
| XEMM-003 | P0 | maker 成交后没有 Hyperliquid Fill | 诊断订单 `0.0001 BTC` 名义价值低于 Hyperliquid 最低 10 美元 | 实盘 canary 使用满足最小名义价值的 `0.0002 BTC` |
| XEMM-004 | P0 | Hyperliquid Fill 重放后仓位可能被重复累加 | `userEvents` 恢复链路依赖 `startPosition` 重建历史增量，存在重放与重复计数风险 | 先做 `snapshot-first` 仓位快照，再仅按增量 Fill 执行后续仓位更新 |
| XEMM-005 | P0 | Fill 与 Position 同时到达可能重复计算敞口 | Fill 与 Position 使用了不同口径但逻辑没有明确边界 | Fill 使用增量（last fill）更新 `F_UNHEDGED_BASE`，Position 使用绝对快照/序列化持续追踪持仓，两者不做增量叠加 |
| XEMM-006 | P0 | 部分成交可能使用累计量重复对冲 | 账户事实没有清晰区分 last fill 与 cumulative fill | 使用 `FillV2.last_fill_quantity_lots` 驱动增量对冲，并保留累计量用于校验 |
| XEMM-007 | P0 | OKX/Hyperliquid 仓位方向曾出现难以判断的偏差 | side、signed position、contract multiplier 分属多个转换层 | 统一 Buy/Sell 编码、signed position 语义和 OKX 合约乘数换算 |
| XEMM-008 | P1 | Hyperliquid 完整 reconcile 失败 | 清算价、入场价和 PnL 等观察值未必严格落在配置单位网格上 | 成交数量保持严格 lot 校验；估值字段按最近单位取整 |
| XEMM-009 | P1 | Hyperliquid `l2Book` 看起来 2–5 秒才更新 | 单独依赖 L2 快照，未利用快速 BBO | Connector 对外仍暴露单一 Depth，内部订阅 `l2Book + bbo` 并合并 |
| XEMM-010 | P1 | OKX WebSocket 大约在心跳附近断开 | 发送了 JSON ping，而 OKX 要求文本 `ping`/`pong` | 公私有流使用文本心跳，首个心跳延迟 20 秒 |
| XEMM-011 | P1 | 只看到通用 `StreamInvalidated`，无法定位错误 | 动态插件 tracing 没有进入宿主日志，错误被多层折叠 | 增加失效来源码、宿主发布错误和交易所命令错误日志 |
| XEMM-012 | P1 | Runtime 未收到仓位、余额、命令结果、重连和定时器 | Runtime 与策略 ABI 的账户状态和控制事件通路不完整 | ABI v10 增加类型化事件并由 Runtime 自动补充账户订阅 |
| XEMM-013 | P1 | 深度序列号、快照/增量语义丢失 | Adapter 只传订单簿条目，没有保留元数据 | Depth ABI 保留 epoch、sequence、first sequence、flags 和 action |
| XEMM-014 | P1 | binding 已配置但没有自动建立行情订阅 | Runtime 没有根据策略 market binding 建立 Connector 订阅 | Runtime 启动阶段自动解析 binding 并建立行情订阅 |
| XEMM-015 | P1 | 停机后可能遗留策略订单 | shutdown 策略没有强制撤销策略拥有的订单 | 配置并校验 `shutdown = "cancel_owned_orders"` |
| XEMM-016 | P2 | Numba 只返回通用 `CompileFailed` | 服务器磁盘只剩约 398 MB，无法生成编译缓存 | 删除可重建 Debug target，恢复可用空间 |
| XEMM-017 | P2 | 服务器增量构建出现 ABI 字段缺失 | 服务器源码旧于本地，但共享 target 中保留了较新的缓存产物 | 同步完整源码后重新构建；发布目录绑定明确 commit |
| XEMM-018 | P2 | 每轮诊断需要多次重新编译和实盘成交 | 缺少动态 ABI 端到端 fixture、一键 canary 和结构化错误链 | 本文列出待补测试与工具化验收项 |

## 4. P0：动态 ABI 与账户事件

### 4.1 FillV2 跨动态 ABI

- [x] `FillV2` 编码/解码保留 `last_fill_quantity_lots` 和 `cumulative_filled_quantity_lots`。
- [x] 使用真实动态 Connector dylib，通过 `TitanAccountHostApiV1.publish_account` 发布 `FillV2`，断言宿主注册并投递 schema v2，而不是默认 v1。
- [x] 同一 fixture 发布 FillV1，断言仍以 schema v1 投递，保证向后兼容。
- [x] Fill event type 搭配未知 payload 长度时必须被拒绝，不能根据相近长度误判版本。
- [x] payload 的 event kind 与 event type 不一致时必须被拒绝。
- [x] FillV2 成功发布后不得产生 `StreamInvalidated` 或自动 reconcile。
- [x] FillV2 发布失败时，日志必须包含 account ID、event type、schema、payload length 和底层错误。

建议测试名称：

- `dynamic_account_fill_v2_crosses_host_abi_with_schema_v2`
- `dynamic_account_fill_v1_remains_backward_compatible`
- `dynamic_account_rejects_unknown_fill_payload_layout`

### 4.2 账户事实顺序和版本

- [x] 按 `OrderChanged → FillV2 → PositionChanged` 投递同一成交，断言策略对同一事件链只产生一次增量对冲；`PositionChanged` 提供绝对持仓锚点，不追加 fill 口径增量。
- [x] 按 `FillV2 → OrderChanged → PositionChanged` 乱序投递，断言 active order 状态正确，`unhedged exposure` 主要由 Fill 增量推进；`PositionChanged` 允许持续覆盖绝对持仓基线（含 active 模式）用于修正与回放重建。
- [x] 相同 FillV2 重放两次，断言第二次被去重。
- [x] 不同账户使用相同 account version 时不得互相去重。
- [x] 同一账户相同 version、不同 asset 或 side 的事实不得错误碰撞。
- [x] account epoch 增加后允许 version 从较小值重新开始，并正确重建基线。

## 5. P0：部分成交、仓位与方向

### 5.1 增量 Fill

- [x] OKX OrderManager 对累计部分成交只发布新增 delta。
- [x] Hyperliquid OrderManager 对累计部分成交只发布新增 delta。
- [ ] maker 依次收到累计成交 `1 → 2 → 5 lots` 时，对冲增量必须为 `1、1、3 lots`，总计 5 lots。
- [ ] cumulative 不变的重复订单更新不得生成 Fill。
- [ ] cumulative 回退的过期事件不得生成负数 Fill 或反向对冲。
- [ ] 最终 `Filled` order event 与最后一个 Fill 同时到达时不得双算。

- [ ] 同一会话在 `QUOTING/HEDGE_ONLY/ACTIVE` 中，`PositionChanged` 可持续更新 `maker/hedge position estimate`；`F_UNHEDGED_BASE` 使用绝对基线刷新（仅替换，不叠加增量）。
- [ ] 同一会话在 `WARMING_UP/PAUSED` 中，`PositionChanged` 按序列号重建 `maker/hedge position estimate` 并用绝对基线刷新 `F_UNHEDGED_BASE`。

### 5.2 方向矩阵

- [ ] OKX maker Buy Fill 必须生成 Hyperliquid Sell hedge。
- [ ] OKX maker Sell Fill 必须生成 Hyperliquid Buy hedge。
- [ ] Hyperliquid Buy Fill 必须减少负的 unhedged exposure。
- [ ] Hyperliquid Sell Fill 必须减少正的 unhedged exposure。
- [ ] OKX `pos=+0.02` 张按 `0.01 BTC/张` 和 `0.01 张/lot` 换算为 `+0.0002 BTC`。
- [ ] OKX `pos=-0.02` 张换算为 `-0.0002 BTC`。
- [ ] Hyperliquid `szi=+0.0002` 和 `szi=-0.0002` 保留正确正负号。
- [ ] `position_side=short` 搭配负 quantity 时只应用一次符号，不得负负得正。

### 5.3 Hyperliquid Fill 恢复（snapshot-first）

- [ ] `private` 连接后先刷新仓位快照并清空本地仓位缓冲，随后仅按 `userEvents` 增量更新。
- [ ] 多个 Fill 按时间正序重放，最终仓位等于最后一个事件推导值。
- [ ] 多个 Fill 逆序重放时，旧事件不得覆盖更新后的绝对仓位。
- [ ] 私有流重连、REST reconcile、`userEvents` 组合后不得重复累计。

## 6. P0：策略订单状态机

### 6.1 防止重复撤单

- [ ] `LEG_EMPTY` 调用 `cancel_bid/cancel_ask` 不生成命令。
- [ ] `LEG_CANCELING` 调用 `cancel_bid/cancel_ask` 不生成第二条 cancel。
- [ ] `LEG_OPEN` 首次 cancel 成功后状态立即转为 `LEG_CANCELING`。
- [ ] `LEG_SUBMITTING` 收到失效事件时最多生成一条 cancel。
- [ ] 连续 BBO、Timer、StreamInvalidated 事件到达时，同一个 order ID 只产生一个 cancel command ID。
- [ ] cancel CommandResult 失败后，订单状态恢复为 `LEG_OPEN`，允许下一次有新 command ID 的重试策略。
- [ ] terminal OrderChanged 到达后清空 active ID，迟到事件不得清除后来创建的新订单。

建议测试名称：

- `canceling_leg_does_not_emit_duplicate_cancel_command`
- `stream_invalidation_and_bbo_share_one_cancel_transition`
- `late_terminal_order_cannot_clear_replacement_order`

### 6.2 Hedge 生命周期

- [ ] maker Fill 触发时，先冻结新报价，再生成一个反向 IOC hedge。
- [ ] 一个未对冲敞口只能有一个 active hedge order。
- [ ] hedge 为部分成交时，仅对 remainder 重试。
- [ ] hedge IOC canceled 且仍有 remainder 时执行有界退避。
- [ ] 达到 `hedge_retry_limit` 后进入 `MODE_FAULTED` 并保持停止报价。
- [ ] hedge 完全成交后清零 unhedged exposure、retry counter 和 active hedge ID。
- [ ] Position snapshot 在 `WARMING_UP/PAUSED` 与运行态均可建立/更新基线；在 `HEDGE_ONLY/QUOTING` 阶段不能把 Fill 增量再次叠加到未对冲敞口中。

## 7. P0：交易所规则与下单校验

### 7.1 Hyperliquid 最小名义价值

- [ ] 下单前以限价和数量计算 notional，低于交易所最小值时不发送请求。
- [ ] BTC `0.0001`、价格约 79,000 时应判定低于 10 美元。
- [ ] BTC `0.0002`、价格约 79,000 时应允许发送。
- [ ] 最小 notional 应来自 Connector instrument metadata，不应硬编码在策略中。
- [ ] 因最小 notional 无法立即对冲时，策略必须停止报价并报告明确的阻碍原因。

### 7.2 价格与数量 wire 格式

- [x] Hyperliquid 价格限制为最多 5 位有效数字，并隐藏 f64 尾差。
- [x] Hyperliquid 数量序列化隐藏二进制浮点尾差。
- [ ] Buy IOC 价格取整不得降低穿价能力；Sell IOC 价格取整不得提高穿价门槛。
- [ ] OKX 数量必须严格落在 lot 网格，不能把不可表示数量静默取整后下单。
- [ ] Hyperliquid 数量必须符合 `szDecimals`。
- [ ] reduce-only 清理单必须校验方向与当前仓位相反，并拒绝扩大仓位。

## 8. P1：行情 Connector

### 8.1 Hyperliquid 合并 Depth

- [x] 策略只订阅一个 Depth 时，Connector 内部建立 `l2Book` 和 `bbo` 两个订阅。
- [x] 快速 BBO 更新能够替换顶档，同时保留新鲜 L2 尾部。
- [x] 旧 L2 尾部不得合并到更新 epoch 的 BBO。
- [x] 取消 Depth 时正确处理共享底层频道，不影响仍被其他 market kind 使用的订阅。
- [x] L2 snapshot 使用单调 epoch。
- [ ] 首个 BBO 早于 L2 到达时仍发布合法的两边顶档，不构造虚假尾部。
- [ ] L2 早于 BBO 到达时发布完整 snapshot；后续 BBO 只替换顶档。
- [ ] 任一底层频道断线时，对外 Depth 的 invalidation/恢复语义必须一致。
- [ ] 合并后 `first_update_sequence <= update_sequence`，epoch/flags/action 全部保留。

### 8.2 OKX 心跳

- [ ] 公有流连接后 20 秒发送文本 `ping`，不得立即发送第一次心跳。
- [ ] 私有流连接后 20 秒发送文本 `ping`。
- [ ] 文本 `pong` 不进入 JSON 反序列化。
- [ ] WebSocket Ping frame 返回 Pong frame，但不与应用层文本心跳混淆。
- [ ] JSON `{"op":"ping"}` 不再出现在发送路径。
- [ ] 连续多个心跳周期内不触发 reconnect 或 StreamInvalidated。

## 9. P1：账户 reconcile 与数值转换

- [x] 成交数量使用严格 decimal unit，拒绝不可精确表示的 lot。
- [x] 入场价、清算价和 PnL 等观察字段允许按最近配置单位取整。
- [x] Hyperliquid 主网完整 reconcile probe 能处理真实清算价小数尾部。
- [ ] 观察值取整必须有最大误差边界，超过边界时拒绝而非静默截断。
- [ ] 空仓 reconcile 必须为每个绑定资产发布显式零仓位 snapshot。
- [ ] reconcile 期间私有事实进入有界 staging queue，完成后按顺序重放。
- [ ] staging queue 满时进入可观察的 invalidation，不能静默丢 Fill。
- [ ] reconcile 失败不得打开 Ready gate；成功后才允许策略命令。
- [ ] 私有流重连后不得取消非本策略拥有的外部订单。

## 10. P1：Runtime、订阅和故障分类

### 10.1 自动订阅与 ABI 视图

- [x] Depth adapter 保留 source epoch、sequence、flags 和 actions。
- [x] Runtime 能向策略传递 Order、Fill、Position、Balance、CommandResult 和 AccountState。
- [x] 策略声明任一账户事件后，Runtime 自动补齐所绑定账户需要的账户事件订阅。
- [ ] Runtime 根据 market binding 自动建立且只建立一次行情订阅。
- [ ] 策略只声明 Hyperliquid Depth，不应感知内部 BBO 订阅。
- [ ] Timer 按配置周期触发，并在暂停、停止或 invalidated 后不再发出交易命令。

### 10.2 故障分类

- [ ] `duplicate_command_id` 应记录为策略/命令状态错误，不能伪装成 EventEngine 数据缺口。
- [ ] 同步 command gateway rejection 应使策略 fail-closed，但不得错误污染 subscriber lane 的可靠性指标。
- [ ] 真正的 handler panic 才标记 `SubscriberFailed`，并记录 event type 和 panic/错误来源。
- [ ] `StreamInvalidated` 日志包含来源码：私有流、直接事实发布、staged replay、reconcile、初始 reconcile 或命令结果发布。
- [ ] 一个账户失效时禁止继续 maker 报价，但已确认的 maker Fill 仍必须进入受控风险恢复流程。
- [ ] 账户恢复 Ready 前不得自动恢复新报价。

## 11. P1：日志与可观测性

- [ ] 动态 Connector 的错误能够进入宿主统一日志，不依赖 dylib 自己的 tracing subscriber。
- [ ] 账户事实发布失败包含 account ID、event type、schema version、payload length 和根因。
- [ ] 账户命令失败包含 account ID、command type、exchange、code、message 和 outcome certainty。
- [ ] 日志不得包含 API key、secret、passphrase、私钥或签名。
- [ ] 每个 maker Fill 和 hedge command 共享可追踪的 trace/causation ID。
- [ ] 可计算以下时间点：maker exchange fill、Fill publish、strategy callback、hedge admission、HTTP send、exchange accept、hedge fill。
- [ ] `subscriber_failed` 日志必须能区分 callback error、panic、backpressure 和 queue overflow。

## 12. P2：构建、发布与服务器预检

- [ ] 启动前检查磁盘可用空间；低于 8 GB 时拒绝构建或给出明确错误。
- [ ] Numba 编译失败保留 Python stderr，不能只返回通用 `CompileFailed`。
- [ ] 发布前确认服务器源码 commit 与目标 Git commit 一致。
- [ ] 二进制、OKX plugin、Hyperliquid plugin 和策略 artifact 分别校验 SHA256/digest。
- [ ] 不允许“旧源码 + 新共享 target 缓存”组合通过部署校验。
- [ ] 每个 release 使用不可变产物；若共享 target，至少记录真实构建 commit 并禁止回写旧 release。
- [ ] 自动保留最近有限数量 release，并只删除明确可重建的构建缓存。
- [ ] 启动服务时显式设置正确的 `VIRTUAL_ENV` 和 PATH。
- [ ] secret 文件保持 `0600`，发布同步不得覆盖、打印或打包 secrets。
- [ ] `shutdown = "cancel_owned_orders"` 缺失或值不正确时配置校验失败。

## 13. 一键 Canary 集成测试 Checklist

以下属于受控集成/主网 canary，不应放入默认单元测试套件：

- [ ] Preflight：两边余额足够，BTC 仓位为 0，挂单为 0，服务未运行。
- [ ] 根据实时价格计算同时满足 OKX lot 和 Hyperliquid 最小 notional 的最小共同数量。
- [ ] Canary 最多允许一个 maker 方向和一个 maker 活跃订单。
- [ ] 第一笔 maker Fill 后立即禁止新报价。
- [ ] maker Buy 对应 hedge Sell；maker Sell 对应 hedge Buy。
- [ ] maker quantity 经合约乘数换算后与 hedge base quantity 完全一致。
- [ ] 在限定时间内收到 hedge Fill；超时立即停止并进入风险恢复。
- [ ] 对冲后组合净敞口小于半个最小 hedge lot。
- [ ] 停止时撤销所有策略订单。
- [ ] 使用 reduce-only 清理双边测试仓位。
- [ ] Postflight：两边仓位 0、挂单 0、正式服务和 canary 服务均 inactive。
- [ ] 输出一份机器可读报告，包含价格、数量、方向、时间戳、延迟、错误和清理结果。

## 14. 建议实施顺序

### 第一批：必须先补的 P0 测试

- [ ] 动态 dylib `FillV2 → host → EventEngine → Strategy` 端到端测试。
- [ ] `LEG_CANCELING` 不重复发送 cancel 的状态机测试。
- [ ] maker Buy/Sell 与 hedge Sell/Buy 的完整方向矩阵。
- [ ] 部分成交 last/cumulative 增量对冲测试。
- [ ] Fill + Position + reconnect replay 不重复计算敞口。
- [ ] Hyperliquid 最小 notional 下单前校验测试。

### 第二批：恢复与可靠性

- [ ] StreamInvalidated、reconcile、staged replay 顺序测试。
- [ ] command rejection 与 subscriber failure 分类测试。
- [ ] Hyperliquid BBO+L2 合并断线恢复测试。
- [ ] OKX 多周期文本心跳测试。

### 第三批：部署和自动化

- [ ] 不可变 release 与 commit/digest 一致性测试。
- [ ] 磁盘和 Python 环境 preflight。
- [ ] 一键 canary 与自动 reduce-only 清理。

## 15. 完成定义

只有同时满足以下条件，才能认为本轮问题已经形成完整回归保护：

- [ ] 所有 P0 项拥有确定性测试，默认 CI 不依赖公网或真实资金。
- [ ] 动态 ABI 测试实际加载 dylib，而不是用同进程 mock 绕过 ABI。
- [ ] 测试使用真实 Numba 编译产物和 ABI v10 context。
- [ ] 任意 maker 部分成交都能证明只产生等量、反向、一次性的 hedge。
- [ ] 任意错误路径都能证明策略停止报价、订单有界、敞口可恢复。
- [ ] 主网 canary 默认关闭，必须显式确认，并在结束后自动验证零仓零挂单。

## 16. 建议测试落点

| 测试范围 | 建议文件/目录 | 测试类型 |
|---|---|---|
| FillV1/FillV2 编解码与 schema 识别 | `crates/titan-account-plugin/src/tests.rs` | Rust 单元测试 |
| 动态 dylib 到宿主的账户事实发布 | `crates/titan-account-plugin/tests/` | Rust 动态加载集成测试 |
| 账户事实到策略 ABI v10 view | `crates/titan-strategy-plugin/src/tests.rs` | Rust 单元/集成测试 |
| Runtime 自动订阅、事件顺序与故障分类 | `crates/titan-strategy-plugin/src/tests.rs`、`crates/titan-plugin-engine/src/tests.rs` | Rust 集成测试 |
| OKX 累计成交转增量成交 | `connector/src/okx/ordermanager.rs` | Rust 单元测试 |
| Hyperliquid 累计成交与重放 | `connector/src/hyperliquid/ordermanager.rs`、`connector/src/hyperliquid/ws.rs` | Rust 单元测试 |
| Hyperliquid L2+BBO 合并 | `connector/src/hyperliquid/ws.rs` | Rust 异步单元测试 |
| OKX 文本心跳 | `connector/src/okx/public_stream.rs`、`connector/src/okx/private_stream.rs` | 使用 mock WebSocket 的异步测试 |
| 策略方向、撤单和 hedge 状态机 | `strategies/okx_hyperliquid_xemm/` 下新增 `tests/` | Numba 策略 fixture 测试 |
| 构建与发布预检 | `deploy/okx_hyperliquid_xemm_testnet/` 下新增测试脚本 | 无网络部署测试 |
| 完整受控实盘闭环 | 独立 canary runner，不进入默认 CI | 显式启用的主网集成测试 |

测试实现时，每个用例应引用一个 `XEMM-xxx` 问题 ID；同一 ID 可以有多个用例，但任何 P0 ID 不得只由主网 canary 覆盖。
