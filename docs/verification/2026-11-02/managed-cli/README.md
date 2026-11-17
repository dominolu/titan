# 管理 CLI 实施与非实盘验收

## 范围与输入身份

本轮对应 GitHub #8–#15：统一连接与机器输出、Operation 找回/等待/取消、账户与绑定查询、策略/参数/构建回测、部署与生命周期、状态/日志/事实及有界导出。命令与兼容语义见[管理 CLI 手册](../../../managed_cli.md)。

基线为 `e07630a76d95b0b692c638aa5f1d591dd91b4191` 加任务开始时已有未提交修改，再叠加本批变更；不能用该 Git 提交单独代表验收输入。隔离源码的逐文件 SHA256 清单、完整命令、退出码、原始日志、合成配置和程序身份均私有保存。最终清单与二进制身份在验收收尾时核对。

所有格式化、编译、静态检查、Rust 测试、Python 驱动及真实二进制执行均在指定 `titan-build` 隔离目录完成，本机仅编辑与人工审查。复用服务器既有依赖镜像与锁定缓存，Cargo.lock 未修改。没有连接真实账户，没有实盘部署、下单、占用释放或 Issue/PR 对外更新。

## 实现要点

- 每进程使用同一 Cookie/CSRF 客户端登录 v2；profile 只保存凭据引用，显式参数覆盖环境和 profile。响应、输入、观察周期、日志分页和导出均有容量边界。
- 写请求先留存稳定幂等键，不自动重新提交。HTTP 受理、等待成功、失败、取消、unknown、观察超时分别输出；中断观察不延长 Runtime 期限或隐式取消。
- 资源与策略目录先授权、按节点/账户/环境/策略过滤再分页；参数写入检查修订与选定范围。部署与控制提交前读取授权目标并给出固定字段预览，不复制业务准入状态机。
- 事实时间按 `received_at_ns` 包含边界过滤，`run_generation` 只匹配有归属证据的记录；SQLite/PostgreSQL 都在查询阶段筛选。过滤条件、方向、固定上界进入游标，旧无新增过滤的游标摘要保留兼容。
- 观察采用有限周期轮询，保留 epoch/修订/代次语义，断线给出 stale；日志来源变化丢弃在途页。有界多页事实导出使用 NDJSON，不累积事实列表，也不将达到页数上限写成完整导出。

真实 CLI 首轮发现服务端原有登录限流累计成功登录，导致每进程认证第 11 个命令被拒绝；修复为认证成功后清除失败计数，仍保留 10 次失败/60 秒的预占与限流。新增回归同时验证正确密码不能绕过已触发的失败上限。

任务开始时已有执行所有权修改引用未定义字段，阻断工作区构建；最小修复使用已解析账户数量判断多账户租约独占。保留原有执行所有权实现，不引入新的恢复或交易控制入口。

## 验证结果

最终输入包含 826 个源码、文档与夹具文件，逐文件清单私有保存；已核对本机最终字节与服务器隔离目录一致。最终同一清单经仓库内容隐私检查无命中，任务所属 Titan 进程剩余数为零。登录回归的合成密码使用运行时随机值，最后一次针对该回归的格式检查和复验通过。验收 Titan 的 SHA256 为 `33c1759b64f44c6b5012c9d6b2f3b6e386f49eaf6ef133908a50eb6bd60d3c8c`。所有下表场景使用该服务器构建的程序，观察故障也在最终程序上重新执行。

| 门禁/场景 | 结果 |
| --- | --- |
| 明确修改文件的服务器 rustfmt | 通过；格式化后回传本机，再核对源码 |
| titan-cli / titan-persistence all-targets Clippy，`-D warnings` | 通过，零告警 |
| CLI 与历史回放程序构建 | 通过 |
| CLI/persistence/account/runtime Rust 回归 | **233 passed，12 ignored，0 failed** |
| 上述四个 crate 的 doctest | 命令通过，当前均为 0 个 doctest |
| 完整真实 CLI 主矩阵 | **14 组通过**，包含原键找回、范围/权限、两页有界导出及下述完整流程 |
| 回环 A/N/E 生命周期夹具 | **26 项通过**，Operation 写入经真实专用 CLI 发起 |
| 历史数据准备/回测/报告夹具 | **8 项通过**，prepare/backtest/report 经真实专用 CLI 发起 |
| scripted 回环观察故障 | **9 项通过**，含旧修订/断线/换 epoch/代次及 64 KiB 日志页 |

14 组主矩阵包含生命周期与历史夹具，不应与其 26/8 项相加当作互不重叠的测试总量。Rust 中的 12 项 ignored 保留：1 项需要独立 PostgreSQL/S3 服务、3 项原生策略/历史恢复专用夹具、8 项观察边界专用夹具；本轮没有直接开启这些 ignored 测试。生命周期夹具另行验证了原生停止、恢复和公开观测，但不能据此改写 ignored 的结果。

主矩阵中 Describe、compile 首次等待均实际经历 `unknown`；客户端以退出 7 保留原回执，随后只读按原键找回、核对相同 Operation ID/input digest，最终对同一 ID 等待成功。没有换键、重发写请求或隐藏首次未知结果。26 项生命周期检查还包括 A/N 重启、丢失控制回执、配置漂移、错误恢复点/订单身份拒绝、原期限恢复及引擎停止证明；模拟场所最终 6 笔委托均撤销、零残留。

历史场景使用一段已登记的历史行情，验证真实引擎、持久结果和报告，不是预设成交轨迹；本次回测 30 笔模拟成交、最终仓位为零。该结果只证明本轮 CLI 数据与报告链路，不宣称策略收益或其他历史区间验收。

可复用的门禁命令（在指定服务器执行，镜像参数从私有运行配置注入）：

```sh
cargo clippy --locked --no-deps -p titan-cli -p titan-persistence --all-targets -- -D warnings
cargo test --locked -p titan-cli -p titan-persistence -p titan-account-service -p titan-strategy-runtime --lib --tests
cargo test --locked -p titan-cli -p titan-persistence -p titan-account-service -p titan-strategy-runtime --doc
python3 scripts/management_cli_smoke.py --repo /workspace/titan --binary /workspace/bin/titan --output /private/cli-evidence --build --lifecycle --offline --history-binary /workspace/bin/v13_grid_history --data /private/history --parameters /private/parameters
python3 scripts/management_cli_watch_smoke.py --repo /workspace/titan --binary /workspace/bin/titan --output /private/watch-evidence
```

旧命令的 CLI golden、单文件策略、原生编译与不可变发布相关测试已包含在 233 项回归中；低层 `console request` 的旧参数/raw JSON 与新机器 envelope 另由主矩阵验证。

验证入口：[主 CLI 矩阵](../../../../scripts/management_cli_smoke.py)、[观察故障矩阵](../../../../scripts/management_cli_watch_smoke.py)。主矩阵驱动服务器产出的真实 Titan 二进制，生命周期与历史场景复用原有模拟服务及真实引擎；HTTP 仅用于夹具准备和独立事实对照。观察故障夹具仅证明协议行为，不替代管理器授权与真实 Runtime 验收。

## 范围与限制

- 所有交易生命周期场景使用回环模拟场所；历史回放使用已登记的离线输入，不连接交易账户。不将合成通过扩展为实盘验收。
- watch 采用有界轮询；没有声称实现 SSE 客户端或对 SSE 帧协议完成验收。
- 账户档案与实际账户之间没有可证明引用时不猜测对应；没有实时观测的账户只显示 unavailable。缺失金额、损益或风险不补零，不在客户端计算交易准入。
- 事实的时间/代次过滤使用已有持久载荷字段，无数据库迁移或新增索引；SQLite 查询与游标已执行回归，PostgreSQL 改动通过编译/Clippy，本轮未连接独立 PostgreSQL/S3 服务执行真实驱动回归。性能边界不扩展为全量生产归档吞吐保证。
- 本轮通过未派发取消和历史执行路径的取消确认；未单独故障注入覆盖所有已派发子进程取消时序，也未将旧 CLI 的所有工作流逐条重新实跑。适用旧入口回归范围以上述测试与矩阵为准。
- 参数草稿 SaveVersion 仍无 Operation 幂等键；创建/复制回执丢失后先查目录核对。unknown 与失败仍保留既有占用和原业务期限。
- 本摘要只包含方法、版本、结果与完整性限制。真实主机、个人目录、凭据、原始账户/订单事实和运行配置均不公开，原始证据私有保存。
