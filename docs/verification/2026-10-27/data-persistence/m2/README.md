# M2 本地数据持久化验收

日期：2026-10-27。M2（P05–P09）已完成，并在 `192.168.3.88` 对最终源码完成本轮非实盘验收。

## 来源与范围

Git 基线为 `5997364864861fdd5d8c649b93e54161018f703d`，加工作区内 M1/M2 修改。隔离源码目录为 `/home/dominolu/dev/titan-persistence-m2-20261027-59973648`，最终门禁目录为 `/home/dominolu/dev/titan-persistence-m2-20261027-59973648/evidence/m2-final-08`。没有覆盖服务器原工作区、访问真实账户或部署实盘。

[源码清单](source-identity.json)固定 540 个代码、配置及依赖文件；[本地字节对照](local-source-comparison.json)确认本地交付源码与服务器验证源码一致。本地只编辑、审查及比较文件摘要，编译、格式、静态检查、代码生成和测试全部在编译服务器执行。环境和固定依赖见[环境记录](environment.json)、[工具链](toolchain.txt)及[SQLite 检查](sqlite-inspect.json)。

## 交付内容

| 工作包 | 已验证的交付 |
| --- | --- |
| P05 | `titan-persistence` 提供 FactArchive/HistoryQuery；账户原 writer 与财务投影同事务追加 envelope、业务去重、冲突及水位。来源在第一条事实前登记，崩溃/丢弃不伪装清洁关闭；关键审计不可用拒绝新单，撤单保留。 |
| P06 | CLI 和原生历史执行器自动记录，Tick/Bar 共同审计边界提供平台持久化入口；完整模式有界背压输出 SQLite/真实 Parquet。保留 NPY/CSV/二进制输入，新增精确 Parquet reader。策略和 SDK 不增加存储调用。 |
| P07 | 本地内容对象、校验读取、条件登记、清单最后提交和原字节物化。保留 v1 原清单，发布失败保留 pending proof；维护重试不重跑引擎或重算财务指标。数据准备发布原输入绑定与两种 prepared 格式。 |
| P08 | 模拟运行和回测共用授权后分页的 `/facts`；独立只读连接、固定上界及条件/方向绑定游标。页面复用查询，权益图读取最新采样；公开业务帧保留 schema/hash，不解析恢复 BLOB。旧 `/history` 保留显式兼容。 |
| P09 | M1 回归与 M2 原生、浏览器、对象/报告闭环在同一最终源码通过；冻结产物、源码摘要、资源和提交延迟。 |

实际配置和维护命令见[本地存储手册](../../../../local_persistence_operations.md)；恢复点继续使用节点本地 SQLite，参见[设计](../../../../data_persistence_design.md)及[实施计划](../../../../data_persistence_implementation_plan.md)。

## 最终门禁

[统一入口](../../../../../scripts/verify_data_persistence.sh)使用 `TITAN_PERSISTENCE_STAGE=M2`；34 个步骤均退出 0。完整命令、退出码及输出见[运行记录](verification-run.json)、[驱动日志](verification-driver.log)和[精确统计](test-summary.json)。

| 门禁 | 结果 |
| --- | --- |
| Rust fmt / check / Clippy | 全工作区、全部 target/feature，Clippy `-D warnings`；[格式](rust-format.log)、[check](rust-check.log)、[Clippy](rust-clippy.log) |
| Rust 单元与集成 | 636 passed、0 failed、22 ignored；其中持久化契约 20 项；[日志](rust-tests.log) |
| doctest | 2 passed、1 ignored；[日志](rust-doctests.log) |
| SDK / 报告 | 115 / 5 项；[SDK](sdk-tests.log)、[报告](report-tests.log) |
| 旧策略与 SQLite 恢复 | 原旧签名文件直接加载与观测边界探针、原生服务/排空/失败门禁重新通过；[兼容结果](original-native-sqlite-recovery.json)、[服务](native-service.log)、[失败门禁](timer-failure.log) |
| 恢复维护 | 9 项；[结果](maintenance/results.json) |
| 管理回环模拟 | 32 项、浏览器 10 项；[流程](loopback/results.json)、[浏览器](loopback/browser/browser.json) |
| 只读管理 | 6 项、浏览器 8 项；[流程](readonly/results.json)、[浏览器](readonly/browser.json) |
| 历史回测 | 18 组，成交历史分页逐组匹配引擎，累计 5468 笔模拟成交；[矩阵](historical/results.json) |
| 历史页面 | 浏览器 23 项；[结果](historical/browser/browser.json)、[持久化事实截图](historical/browser/persisted-facts.png) |
| 本地完整闭环 | 24 项；18 个结果包共 42743 条事实，SQLite 与 Parquet 有序载荷摘要一致；[结果](local-persistence/results.json)、[命令及退出码](local-persistence/commands.json) |
| 前端 | DTO、typecheck、格式、lint、缓存测试与 build；[类型](web-types.log)、[缓存](web-cache.log)、[构建](web-build.log) |
| 管理目录查询隔离 | 12,000 条大元数据的原有并发查询/受理回归；[结果](history-query/results.json) |

默认 ignored 项未批量放开。旧原生 fixture 的显式恢复/边界探针单独执行，其通过项不混入工作区数量；恢复提交探针为同一单测的顺序重复测量，不增加独立测试数量。

## 完整性与兼容证据

- [归档历史](loopback/archive-history.json)确认 `simulation` 来源、公开 schema/hash、账户事实未知策略归属、固定上界及崩溃后旧/新运行隔离；[派单证据](loopback/durable-intents.json)逐笔核对到模拟 venue 的 POST 已有 SQLite 恢复意图提交。
- 持久化契约验证来源重传与原生业务去重、矛盾内容保留、事务失败不推进水位、零 chunk 来源、慢 sink/失败背压、所有者和封存边界、未知 schema、数据库角色隔离、u64/负时间/细分价格及负零往返。最新查询超过 20 条后刷新可见新记录，升降序游标不能互用。
- 维护 CLI 的 `facts --newest-first` 从完整单日结果取最新 20 条权益采样，[结果](latest-cli-proof.json)与[实际命令](latest-cli-proof.command.txt)确认首行等于该过滤范围的上界且按行号降序。
- 18 组使用横盘/上涨/下跌、三种方向和普通/压力模型，费用、资金费、风险和结束规则均来自原引擎。逐组核对 v1 文件摘要、SQLite/Parquet 记录和完整性，不用财务结果重算替代事实采集。
- 同一原生策略文件分别读取二进制及 Parquet prepared 输入，除物理输入路径、录制 ID 和运行 ID 外，引擎结果与逐条事实相同；没有重编译策略。[本地闭环](local-persistence/results.json)保存该文件及输入摘要。
- [历史基线对照](baseline-engine-comparison.json)比较改造前横盘中性基线的财务、风险、计数、模拟时间与模型；物理路径、构建身份和新增持久化字段不作引擎数量比较。此对照与同一文件的格式切换证明分别记录。
- 对象重复登记、数据集引用重登记、v1 原字节物化、固定上界 Parquet 导出、不覆盖、坏摘要和缺 manifest 拒绝均通过。无效 trace 的引擎失败仍保存 `FAILED` 诊断、`final_state_available=false` 和独立的持久化状态。

`received_segment_complete` 只证明平台收到的该段事实排空提交，`venue_history_complete` 始终为 false。权益及业务帧的采样标识独立于完整录制；恢复点不等于整个模拟器快照，本轮不承诺任意中断回测续跑。

## 资源与产物

[性能记录](performance.json)的恢复探针顺序提交 64 个 8 字节状态的恢复点：P50 10.383 ms、P95 10.742 ms、P99/最大 13.107 ms，包含 JSON、FULL 提交、等待和固定引用读取。它可能超过默认 5 ms handler 预算；本轮模拟沿既有 5 秒配置验证，没有修改生产默认预算或原恢复/停止期限。

单日完整审计的原生 Parquet 回放峰值 RSS 为 20248 KiB，输出 history.db 1667072 字节、facts.parquet 1014263 字节。该测量包含执行和落盘，不包含既有 CSV 准备排序的峰值内存。[原始 time 输出](local-persistence/parquet-replay.time.txt)保留用户/系统 CPU、墙钟和资源信息。测试 CLI 为 debug 构建，历史入口使用 `-C opt-level=2`，不作为生产吞吐或尾延迟 SLA。

账户队列 4096 条、每次最多取 256 条；回测 chunk 64 条/4 MiB，单事实 1 MiB；查询 200 条/约 4 MiB，busy timeout 200 ms；标准行情 Parquet reader 的单片上限为 1,048,576 行。自动配额、保留窗口、GC、长读/WAL/备份争用及生产预算在 P13 完整验收。

冻结产物在 `/home/dominolu/dev/titan-persistence-m2-20261027-59973648/evidence/m2-final-08/artifacts`，CLI SHA256 为 `8a1f6b283905f6f04341dd5767c753d08bfbb01bdf9f921a6ece3a7218c96f0c`；[产物清单](artifact-sha256.json)覆盖 CLI、历史程序和完整 Web dist。[进程清理](process-cleanup.json)确认本证据目录无遗留测试进程。归档不含真实凭据、私钥、数据库、私有恢复 BLOB 或二进制。

PostgreSQL/S3 真实驱动和双向切换、物理设备故障/整机断电、全部发布中断边界与并发备份压测仍属 M3/P13。既有 v1 结果和早期事实继续兼容；M2 不自动迁移旧历史、不抹掉未知覆盖，也不将模拟记录当真实交易事实。

## 复验

在相同源码的编译服务器目录运行，证据目录必须是新路径：

```sh
TITAN_SOURCE_BASELINE=5997364864861fdd5d8c649b93e54161018f703d \
TITAN_PERSISTENCE_STAGE=M2 \
TITAN_HISTORY_DATA=/home/dominolu/dev/titan-grid-history-20261022-2aafc598/data/grid-history \
TITAN_HISTORY_PARAMETERS=/home/dominolu/dev/titan-grid-history-20261022-2aafc598/verification-artifacts/grid-history/final/artifacts \
TITAN_PREVIOUS_RECOVERY_FIXTURE=/home/dominolu/dev/titan-observation-audit-20261027-638bae69/evidence/final-04/loopback \
TITAN_BROWSER_EXECUTABLE=/home/dominolu/.cache/ms-playwright/chromium-1248/chrome-linux64/chrome \
CARGO_TARGET_DIR=/tmp/titan-deplugin-target \
bash scripts/verify_data_persistence.sh evidence/m2-next
```
