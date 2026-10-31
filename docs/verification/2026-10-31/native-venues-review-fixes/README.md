# 交易所接入审查修复

本轮对应用户要求修复的四项审查发现，改动限于 Backpack 原生签名、Lighter 流处理、回归测试及测试时钟功能。没有修改平台对真实账户失效的暂停和对账规则。

## 修复内容

1. Lighter 去掉健康连接的固定 500 秒寿命。订阅等待仍有 15 秒上限，收发仍受心跳检查约束；真实断线继续交给连接器发布失效事件并重新认证。确认帧经过内容校验后才计入初始化屏障，重复确认不再次发布 ready。
2. `account_all_positions` 缺少或明确为 null 的 `transaction_time` 表示未知执行时间，保持零时间语义，不使用 API 发布时钟冒充交易所执行时钟。存在的时间字段仍校验格式和非负范围。
3. Backpack 签名保留查询参数的全部重复值，按参数名稳定排序，不再通过单值 JSON 对象丢弃 `marketType` 的早先值。
4. Backpack 提现原样发送 `recipientInformation`，但按官方明确要求将它排除在签名串之外；兼容官方默认/示例中的 null，不放宽其他嵌套参数的校验。

协议依据：[Backpack 提现](https://docs.backpack.exchange/#operation/request_withdrawal)、[Backpack 签名](https://docs.backpack.exchange/#section/Authentication)、[Lighter WebSocket](https://docs.lighter.xyz/api/websocket)、[Lighter 时间字段](https://docs.lighter.xyz/developers/reference/timestamps)。本轮核对的 Backpack 文档明确说明 `recipientInformation` 不属于签名串；仅允许嵌套对象参与签名不能修复该接口。

## 回归场景

- 三类 Backpack 历史查询同时使用 SPOT/PERP 过滤器，检查实际重复参数，并用独立固定向量验证 Ed25519 签名。
- 提现收款信息使用对象和 null 两种形式，检查请求体完整保留，并验证签名排除了该字段。端点均为回环模拟服务。
- Lighter 空账户初始持仓快照，以及有持仓但缺少/null/零/有效/非法执行时间的消息。
- 公共和私有流分别模拟持续运行 720 秒，每 30 秒继续处理真实回环 WebSocket 帧。私有流重复确认只发布一次 ready；服务端真实关闭仍终止会话。

时钟加速只用于测试，网络握手和每一帧收发使用真实回环连接；不将模拟服务视为交易所私有验收。

## 验证记录

编译与非实盘测试均在约定的 `192.168.3.88` 执行。[最终状态](final/status.tsv)全部为零；[工具链与基线](final/environment.txt)、[检查前源码](final/sources-before.json)和[检查后源码](final/sources-after.json)均已归档，651 个输入文件前后及本地一致。

| 检查 | 结果 |
| --- | --- |
| [格式检查](final/format-check.log) | 通过 |
| [官方接口/字段/签名覆盖审计](final/official-coverage.log) | Backpack 75、Lighter 104、signer 23，全部通过 |
| [Clippy](final/clippy.log) | workspace / all-targets / all-features，`-D warnings` 通过 |
| [整库默认并行回归](final/workspace.log) | 710 项 Rust、2 项 doctest 通过；26 项 Rust、1 项 doctest 按既有规则忽略 |
| [官方离线签名](final/official-signer.log) | 单独执行 1 项，覆盖 23 类交易及原有工具验证；无真实账户请求 |

[测试计数](final/test-counts.json)来自整库日志。新增的五项测试均在最终整库运行中通过，包含两个 720 秒模拟会话；[单独流回归](diagnostics/stream-regression.log)也保留。没有为得到通过结果改用串行模式或放宽告警。

工作期间仓库新增独立提交 `82b9bef9f20271551cf799fab256adfc44780912`，将 orderflow_events 策略与部署移到 hft_quant，并更新 CLI 测试。本轮保留该提交，最终验证以此为基线；输入清单相应从历史 660 项变为 651 项，不能直接套用上一轮的测试数量。

诊断记录保留两次早期失败：[新增公开流夹具缺少官方 `/stream` 路径](diagnostics/connector-initial.log)造成握手失败；[新增测试模块位于产品函数之前](diagnostics/clippy-module-order.log)触发 Clippy 排列检查。分别修正夹具路径及模块位置，没有放宽检查或超时断言。

本轮未部署，未读取真实账户、下撤单或执行资金操作；Backpack 公网连接和三家私有实盘验收仍沿用[接入说明](../../../exchange_onboarding.md)中的未完成状态。
