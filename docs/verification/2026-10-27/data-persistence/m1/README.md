# M1 SQLite 恢复闭环验收

2026-10-27，M1（P01–P04）已在 `192.168.3.88` 完成非实盘验收。恢复点与运行代次统一进入 SQLite；正常文件 writer 和 `.generation` 写入已移除。策略源码、Numba SDK、ABI V13 和私有状态布局不变。历史/统一回测归档、对象存储及第二后端仍属于 M2/M3，本轮未访问真实账户或部署到实盘节点。

对应[设计](../../../../data_persistence_design.md)、[实施计划](../../../../data_persistence_implementation_plan.md)和[维护手册](../../../../recovery_storage_operations.md)。

## 源码与构建身份

| 项目 | 实际值 |
| --- | --- |
| 源码基线 | `5997364864861fdd5d8c649b93e54161018f703d` 加本次工作区修改；未创建提交 |
| 编译服务器 | `192.168.3.88` |
| 隔离源码 | `/home/dominolu/dev/titan-recovery-m1-20261027-59973648` |
| 最终证据 | 上述目录的 `evidence/m1-final-09`，本轮完整重验退出码 0 |
| Rust / Cargo | 1.94.1 / 1.94.1 |
| Python / Node / pnpm | 3.11.14 / v22.22.0 / 10.32.1 |
| 驱动及引擎 | `rusqlite=0.40.2`、`libsqlite3-sys=0.38.2`、bundled SQLite 3.53.2 |
| 恢复 schema | `user_version=1`；WAL、`synchronous=FULL`、外键 |
| 最终 CLI SHA256 | `c402d1e84ac49ba854a4c48b46a8e376d0b8849966e06b630dbf65dc6c719bae` |

[源码清单](source-identity.json)固定 531 个源码/配置/lockfile 的 SHA256；[本地字节对照](local-source-comparison.json)核对回传的格式化源码和生成契约。本地仅编辑、审查及字节比较，所有构建、格式检查、静态检查、代码生成和测试均在编译服务器执行。隔离目录通过显式文件清单同步，没有覆盖服务器原目录或其修改。

已验证的 debug CLI 和配套 Web 资源冻结于服务器 `evidence/m1-final-09/artifacts/titan`、`artifacts/console-dist`；摘要见[构建清单](build-sha256.txt)、[冻结产物](artifact-sha256.txt)及[Web 清单](web-build-sha256.json)。这些是非实盘验证产物，本轮未做生产发布。归档不含私钥、账户凭据、数据库、私有恢复载荷或二进制。

## 交付结果

| 工作包 | 实现与验收 |
| --- | --- |
| P01 | `StrategyRecoveryStore` 冻结恢复领域契约，增加 `RecoverySubmitToken`、`RecoveryPointRef`、代次预留、固定读取、pin、健康及关闭；SQLite 配置严格校验，拒绝未知 driver、互斥配置、角色/schema 及恢复/历史同路径。其他历史/对象端口仍待对应工作包实现。 |
| P02 | 不可变 BLOB/head/提交索引同事务；独立事务预留代次；单 writer 有界 FIFO 和指定 token 等待；代次/会话隔离、head 条件更新、所有权锁与身份见证；故障关闭、显式排空关闭、离线保留清理。 |
| P03 | service core 注入 `Arc<dyn StrategyRecoveryStore>`；派单、恢复、手动恢复点及停止等待自身提交；管理恢复核验 SHA256、pin 并冻结完整引用到 managed spec，保留原 Operation、账户占用和控制期限。 |
| P04 | `titan recovery` 提供检查、旧载荷/仅代次导入、原字节导出、隔离备份、pin/unpin 和清理；迁移回执幂等，原文件只读保留，活动旧宿主锁阻止迁移，备份不能直接激活。 |

当前配置使用 `[strategy_service.recovery_storage]`，旧 `checkpoint_root` 只映射到目录内 `runtime.db`。旧文件没有相符的显式迁移回执时拒绝启动，不自动创建空状态继续交易。恢复端口支持宿主注入替换实现；当前可信配置只开放已经验收的 SQLite，未宣称 PostgreSQL/S3 或其他恢复驱动可用。

代次隔离要求与已预留最大代次精确相等。新代次预留后，旧代次即失效，不等新 head 出现；head 更新未影响一行时回滚整个提交。服务关闭在全部运行停止清理后显式 join writer，再释放执行锁，避免 EventEngine 保留 Arc 导致锁滞留或旧写入越过交接。

## 最终门禁

统一入口为 [verify_data_persistence.sh](../../../../../scripts/verify_data_persistence.sh)，本轮 24 个步骤全部退出 0。原始命令保存在相邻 `*.command.txt`，阶段结果见[完整运行记录](verification-run.json)、[驱动输出](verification-driver.log)和[精确统计](test-summary.json)。使用锁定依赖及隔离 Cargo 镜像配置，最终检查运行于离线缓存，没有放宽 Clippy 或同步持久化条件。

| 门禁 | 最终结果与证据 |
| --- | --- |
| Rust fmt / check / Clippy | 全工作区、全部 target/feature；Clippy `-D warnings`；[格式](rust-format.log)、[check](rust-check.log)、[Clippy](rust-clippy.log) |
| Rust 单元/集成 | 614 passed、0 failed、22 ignored；其中恢复存储 24 项通过；[日志](rust-tests.log) |
| doctest | 2 passed、0 failed、1 ignored；[日志](rust-doctests.log) |
| SDK / 报告 | 115 / 5 项通过；[SDK](sdk-tests.log)、[报告](report-tests.log) |
| 原版本策略及恢复点 | 显式执行旧签名原生文件恢复 1 项与观测边界 8 项；[结果](original-native-sqlite-recovery.json)、[日志](original-native-sqlite-recovery.log) |
| SQLite 维护 | 9 项通过；[结果](maintenance/results.json) |
| 原生服务生命周期 | Service → Factory → Instance 启停恢复到代次 2；[日志](native-service.log) |
| 原生故障/排空 | 账户基线、启动漂移、代次隔离、Stop/Order/Fill 排空及持久化失败禁止派单；timer flush 失败后 `Failed`、交易门关闭；[恢复](common-recovery.log)、[排空](order-drain.log)、[timer](timer-failure.log) |
| DTO / Web | 配套类型生成、typecheck、build 通过；[类型](web-types.log)、[构建](web-build.log) |
| 管理模拟流程 | 27 项通过，10 次模拟下单、最终 0 残单；[结果](loopback/results.json) |
| 浏览器 | 10 项通过、页面错误为空；[结果](loopback/browser/browser.json) |
| 历史查询回归 | 12,000 条、每条 32 KiB 元数据；大查询并发的轻量读取/受理最大 0.08793 秒；[结果](history-query/results.json) |

全工作区默认 ignored 项没有批量放开；需要原生 fixture 的上述 1+8 项通过单独命令显式执行，不并入 614 的计数。`crash_child` 是由正常测试启动、杀死并核验的子进程 fixture，本身不作为独立通过项。其余 ignored 项和 1 项 ignored doctest 保留原状态。

## 恢复与故障证据

下表列出本轮实际执行的子场景；编号对应计划矩阵，不表示该编号跨阶段的所有负载或故障注入已经完成。

| 计划范围 | 本轮证明 |
| --- | --- |
| A01、A02 | 同一旧签名策略未重编译，原载荷先校验字节再导入/读取；错误 store、SHA256、元数据、私有状态及非 head 记录损坏拒绝；原 ABI/schema/绑定校验继续生效。 |
| A03、A06 | 失败启动消耗代次不复用，范围耗尽拒绝；竞争所有者拒绝；新代次尚无新 head 时旧代次已被隔离；FIFO、队列满不伪造 token、关闭超时保留锁及 Arc 尚存时显式交接通过。 |
| A04 | 实际子进程在 COMMIT 前与 COMMIT 后被 SIGKILL：重启分别无 head/有完整 head，已预留代次均推进；head 写入失败或被忽略时记录及提交索引不部分提交。 |
| A05 | 队列满、真实 SQLite busy、`max_page_count` 注入 SQLITE_FULL、只读连接写入失败和 flush 超时；故障后健康失败，迟到提交不能重开交易门。磁盘满测试限制 SQLite 页数，没有耗尽服务器磁盘。 |
| A07、A12 | 原生宿主恢复/风险复查及管理只读对账继续通过；每次模拟 POST 到达 venue 时，由独立连接核验对应 SQLite 状态和订单意图已提交，10 次均有[记录及摘要证明](loopback/durable-intents.json)。 |
| A08、A09 | head 变化不改变原引用；pin/保留窗口保护固定记录；原字节导入导出、重复导入/重复 pin、冲突拒绝、原始代次水位不变、活动旧宿主拒绝迁移；仅代次迁移不伪造恢复状态。 |
| A10、A26 | Backup API 一致副本、输出不覆盖、备份隔离拒绝代次预留；数据库复制到另一位置、硬链接及同位置删除重建被拒绝激活。 |
| A21、A25、A27 | 恢复 BLOB 不变期间公开业务帧独立更新；恢复序号 `u64::MAX` 字节往返；管理历史查询使用独立连接的回归通过。 |

旧模拟 fixture 的恢复载荷 SHA256 为 `6deb086a605d42fa8fc7cd004fe48c1fc60278d0085f2bb30247c6a19eb8f9ab`，签名策略 SHA256 为 `91dc1c38e925ed1d5ee77c8fa01446fb7a2263740559e1831a89d529ae38eb12`。本轮直接加载原策略，源文件及代次水位未改写。[inspect](sqlite-inspect.json)记录实际数据库身份、schema、版本和完整性。

公开业务观测保持 60 秒恢复周期，三次新帧等待最长 1.2283 秒，期间恢复 BLOB SHA256 始终为 `a5903deb5817564d3f4929ac7871899f79fcbd88b12762eff82f40dcf98d3b62`。见[采样证据](loopback/realtime-observation.json)及[浏览器截图](loopback/browser/business-observation.png)。业务帧不参与恢复校验，也不从 SQLite 私有载荷恢复页面值。

## SQLite 版本修复

原依赖 `rusqlite 0.37` 的 bundled SQLite 3.50.2 未包含官方 WAL-reset 修复。本方案使用多连接，符合官方列出的风险条件，因此将三个 SQLite 使用方统一固定到 `rusqlite=0.40.2` / SQLite 3.53.2，并在恢复连接加入运行时最低 3.51.3 校验。依据为 [SQLite WAL-reset 官方说明](https://www.sqlite.org/wal.html)和[驱动发布记录](https://github.com/rusqlite/rusqlite/releases/tag/v0.40.2)。

该变更只更新所需驱动及其依赖，没有以关闭 WAL、降低 FULL 或放开版本约束规避问题。最终第 09 轮在该依赖及最新代次/head 修复之后重跑全部门禁，包含账户观测、管理数据库和历史查询回归；早期通过记录不替代本轮结果。

## 性能与剩余范围

[最终提交探针](recovery-latency.log)在相同源码与服务器上顺序提交 64 个恢复点，私有状态 8 字节：P50 **9.512 ms**、P95 **10.105 ms**、P99/最大 **10.317 ms**。它包含提交到等待成功的路径、JSON 编码、SQLite FULL 提交和测试读取固定引用，属于合成小载荷原语测量，不是生产回调或尾延迟 SLA。默认队列 16、busy timeout 1000 ms；探针采用 3 秒等待期限。

该耗时可能超过现有默认 5 ms handler 预算；模拟网格沿既有测试配置使用 5 秒 handler 预算，并未修改生产默认值、原恢复或停止期限。M1 验收证明提交超时会故障关闭，不证明默认低延迟 profile 已能运行所有策略。P09/P13 须对实际载荷、设备、并发备份/历史负载和完整命令链冻结生产预算，不能关闭 fsync 来达标。

本轮没有执行整机断电、设备掉盘、任意指令级迁移杀进程、强制 writer panic、所有命名空间的持续高负载、所有 COMMIT 确认丢失时序及并发备份压测；这些场景不以事务单测或进程 SIGKILL 冒充设备耐久证明，完整门禁在 P13 执行。PostgreSQL/S3、统一事实/回测采集、任意中断模拟器续跑均未实施。

备份始终不可直接激活，M1 不提供一键灾备。绕过工具在原位置覆盖同一存储身份的旧数据库，无法只靠库内数据证明代次未回退；证据不足须保持停止并核对管理/节点回执及权威账户事实。pin 保守保留，自动回收和容量编排待后续工作，当前显式离线清理、满盘故障关闭。

## 复验入口

以下命令仅在编译服务器执行，输出必须使用新目录；fixture 指向上一版本生成的模拟材料：

```sh
cd /home/dominolu/dev/titan-recovery-m1-20261027-59973648
TITAN_SOURCE_BASELINE=5997364864861fdd5d8c649b93e54161018f703d \
TITAN_PREVIOUS_RECOVERY_FIXTURE=/home/dominolu/dev/titan-observation-audit-20261027-638bae69/evidence/final-04/loopback \
TITAN_BROWSER_EXECUTABLE=/home/dominolu/.cache/ms-playwright/chromium-1248/chrome-linux64/chrome \
TITAN_VERIFY_PYTHON=/home/dominolu/dev/titan/.venv/bin/python \
CARGO_TARGET_DIR=/tmp/titan-deplugin-target \
bash scripts/verify_data_persistence.sh evidence/m1-next
```

源码改变后须重新同步、冻结清单并运行对应门禁；生产迁移和实盘检查另需明确目标及授权。
