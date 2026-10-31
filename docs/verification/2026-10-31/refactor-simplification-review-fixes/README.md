# 精简重构 Review 修复与提交验收

2026-10-31。修复前一轮 review 确认的 P2 版本列表响应上限回归和 P3 心跳测试真实调度依赖；本记录取代[首次验收](../refactor-simplification/README.md)对最终合入版本的结论。实际待提交源码先整合 GitHub `master` 的上游 Hyperliquid TLS 修复，基线为 `e31594fda0facacfce3807f9a4147e7e20eeb0b0`。本地仅编辑、审查、同步和归档；全部格式、构建、静态检查和非实盘测试在 `192.168.3.88:/home/dominolu/dev/titan-simplify-review-fixes-20261031-e31594fd/src` 运行。

## 修复

- P2：handler 在 JSON 序列化前调用 `public_resource`，它直接复用 HTTP/SSE 的 `public_response`，保留单一字段规则。原 8MiB 响应体限制保持；不把私有 initial_state 放入该限长读体路径。只投影查询副本，不写回数据库或重算身份/摘要。
- 大列表回归通过真实 manager_router、SQLite、会话和授权查询：65 个合法 64KiB 全零初始状态版本的未投影体积超过 8MiB；列表及详情必须返回 200、移除四类恢复字段和 projection、保留公开 schema 和原 digest，并回读原初始状态长度和摘要。测试不是授权负例或任意 offset 矩阵，也不声称逐字节对比 SQL data。
- 下载回归用带排版空白的 JSON 原始字节，逐字节核对文件路径旁路，能检出重序列化或过滤。
- P3：心跳使用 `tokio::time::Instant`，重连耗时仍用标准库时钟。测试暂停时间，唯一 Ping/Pong 载荷确认处理完成后才推进；Pong 后 Text 构成接收顺序屏障，验证两者不续期。Socket pending 时保持测试任务可运行，阻止 Tokio 空闲自动推进；标准库 5s 只检测挂起，不决定心跳行为。市场端点和正式心跳期限保持原值。仅开发依赖启用 test-util，不改 lockfile。

## 输入与提交范围

[初始清单](source-inputs.json)取自实际 Git 暂存树，包含 759 个输入；不包含用户未提交的跨所策略、相关测试和研究文档，也不包含本地构建产物、node_modules 或运行目录。SDK 测试数量须使用此精确提交范围，不能沿用首次包含跨所测试的 155 项。

远端格式化只改动采集器共享传输和 HTTP 测试的排版，已同步回本地并暂存。[冻结清单](source-frozen.json)记录待测字节。验证完成后只允许修改本次验收说明、清单和工程记忆；最终提交的所有实现文件必须与服务器记录一致。源码清单中的 staged_tree 是证据归档前的输入树身份，不是最终提交 ID。

## 验证

最终完整门禁全部通过。[退出码](logs/status.tsv)、[机器汇总](summary.json)、[工具链](logs/environment.txt)、[验收后输入](source-after.json)已归档。冻结与验收后 759 项字节一致，所有实现及脚本与最终暂存内容一致；验收后仅更新说明、清单和工程记忆。入口为[完整脚本](../../../../scripts/verify_refactor_simplification.sh)。

| 门禁 | 结果 |
| --- | --- |
| 真实版本列表/详情定向回归 | 1 passed |
| 采集器共享传输定向回归 | 5 passed |
| 可控心跳测试重复 | 连续 10 次通过 |
| rustfmt、全特性 Clippy `-D warnings` | 通过 |
| 全 workspace/all-targets/all-features Rust | 736 passed，0 failed，26 ignored |
| doctest | 2 passed，0 failed，1 ignored |
| CLI 构建 | 通过；既有 6 项单文件集成包含于 Rust 数量 |
| SDK / 报告 | 120 / 5 passed |
| 前端类型/格式/lint/缓存/API/页面/生产构建 | 全部通过；请求捕获 40 条 |

SDK 从首次 155 项变为 120 项，是因为本次严格使用待提交树，排除了用户未提交的 35 项跨所策略测试，不是删除已有测试。上游新增 TLS 独立进程回归已包含于最终全工作区测试，未跳过。

## Review 与诊断

[修复前定向复现](review-reproduction.log)使用真实响应中间件：未投影 8,530,981 字节返回 500，投影后 10,196 字节返回 200。修复后用真实 manager 路由测试覆盖该边界。

首次整合验证中，两个定向回归、十次心跳与 Clippy 通过，全工作区在上游新增 Hyperliquid provider 断言处失败。源码归档的默认 TarInfo 时间戳为 0，而复用缓存中存在旧 connector 产物。已核对源码摘要不变，刷新隔离工作区源文件时间戳以要求重新编译；最终结果以完整重跑为准，不放宽断言、不跳过 TLS 测试。[失败日志](logs-attempt-1/rust.log)与[重新编译的 TLS 回归](tls-fresh-source.log)保留；源码摘要不变，重新编译后 TLS 独立进程测试通过，随后完整重跑全部通过。后续从 Git tree 打包源码时应以当前时间戳物化或使用独立缓存，不能把默认零时间戳文件直接交给已有 Cargo 缓存而信赖其增量判断。

本轮无真实账户访问、订单操作或实盘部署。
