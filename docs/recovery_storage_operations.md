# SQLite 恢复存储维护（M1）

日期：2026-10-27。范围：P01–P04；验收状态及实际版本见[M1 验证记录](verification/2026-10-27/data-persistence/m1/README.md)。完整边界见[持久化设计](data_persistence_design.md)，后续工作见[实施计划](data_persistence_implementation_plan.md)。本页示例路径为占位路径，不表示已部署或已迁移生产数据。

## 1. 当前配置与策略边界

M1 实现 `StrategyRecoveryStore` 领域端口和 `SqliteRecoveryStore`。配置和依赖注入由 Rust 平台处理；策略源码、Numba SDK、原生 ABI V13 与策略私有状态布局不变。正常运行只写 SQLite，不写 JSON 恢复点或 `.generation` 文件，不在数据库失败时自动回退。

当前配置入口为：

```toml
[strategy_service.recovery_storage]
driver = "sqlite"
path = "/srv/titan/recovery/runtime.db"
queue_capacity = 16
busy_timeout_ms = 1000
```

相对路径按 runtime 配置目录解析。`driver` 当前只接受 `sqlite`；队列上限 4096，忙等待上限 5000 ms，零值拒绝。库连接启用并核对 WAL、`synchronous=FULL` 及外键。单个恢复载荷上限 32 MiB，恢复与观测数据库路径不能相同。

当前固定 `rusqlite=0.40.2`，bundled SQLite 为 3.53.2；打开恢复连接前拒绝低于 3.51.3 的运行时。该版本门槛确保包含 [SQLite 官方 WAL-reset 修复](https://www.sqlite.org/wal.html)，`inspect` 输出实际版本。不要仅替换系统 SQLite 或降级 lockfile 绕过门槛。

旧 `strategy_service.checkpoint_root` 仅作配置兼容，映射到其目录下的 `runtime.db`；不能与 `recovery_storage` 同时配置。目录仍有旧恢复文件时，必须存在与原字节匹配的显式迁移回执，才能正常启动。设计文档中的通用 `[storage.*]` profile 属于后续阶段，不能当作 M1 已支持的配置。

管理 worker 使用原有账户/运行编号命名空间下的 `runtime.db`。管理 Operation 继续提交 `recovery_point_sha256`；节点先核验并 pin 不可变记录，再把完整引用写入冻结的 `managed.json` 并传入 Runtime。受理后的恢复不再读取变化中的最新 head。重复操作沿原有幂等协议处理。

## 2. 提交与生命周期

代次通过独立 SQLite 事务预留，失败启动也不回退。完整业务状态、订单记录、未决意图和账户恢复基线作为同一原始 JSON BLOB 提交，同时推进 head。派单、恢复屏障、手动恢复点与最终停止等待自身 `RecoverySubmitToken`，不等待其他来源无限排空。

普通回调的 writer 健康检查只读原子状态；JSON 编码、摘要和数据库写入由独立有界 writer 执行。存储失败或超时保持本运行交易门关闭，迟到提交不能重开门。提交成功不表示交易所已受理；结果未知仍按原 client order ID 对账。

宿主保留原有管理占用及所有权锁，存储另按数据库/运行编号持有执行所有权。锁文件仅保存数据库身份见证，不保存状态或代次；它用于拒绝同一路径下数据库被删除重建后的静默身份重置。不得清空该见证来绕过检查。

服务 quiesce 必须先停止和清理全部运行，再调用存储 `close(deadline)` 排空并 join writer 后释放锁。若排空超时，锁仍保留；不能仅依赖 EventEngine 或 Arc 恰好析构来决定何时允许下一宿主启动。旧运行仍可能被事件引擎引用时，也不能在新运行后补写旧代次。

## 3. 冷路径检查

所有维护命令不创建账户 connector，不执行策略，不访问交易所，成功时输出 JSON：

```sh
titan recovery --database /srv/titan/recovery/runtime.db inspect
```

检查包括 SQLite 完整性、外键、全部恢复载荷的 SHA256/内部 checksum、检索元数据与载荷的一致性及 head 身份。输出仅含存储身份、SQLite/schema 版本、代次水位、恢复引用、记录/pin 数和可激活标记，不包含私有状态 BLOB。

恢复引用包含 `store_id`、`runtime_strategy_no`、`record_id`、`payload_sha256`。`record_id` 不使用墙钟或恢复序号推导；原始 `u64` 恢复序号在 SQL 中保存为十进制文本，载荷保留原有数值。命名空间在 M1 中由数据库身份和固定运行编号共同确定，不另为策略分配业务身份。

## 4. 从旧文件迁移

先冻结目标节点、账户及原命名空间，停止旧宿主并核对其退出，保留原管理占用、停止期限和账户证据。下面命令仅处理磁盘数据，不能替代上述业务核对。

原始 JSON 与 8 字节 little-endian 代次文件必须来自同一目录。`--expected-sha256` 使用已冻结迁移清单中的摘要：

```sh
titan recovery --database /srv/titan/recovery/runtime.db import-legacy \
  --point /srv/titan/legacy/strategy-77.json \
  --generation-watermark /srv/titan/legacy/strategy-77.generation \
  --expected-sha256 '<冻结清单中的原始 JSON SHA256>'
```

工具取得旧目录 `owner.lock` 和目标执行命名空间的独占锁；仍有旧宿主持有锁时拒绝迁移。原始字节先核验、再解码；原字节及 SHA256 不重新编码。先独立提交代次下界及其来源回执，再在事务中提交载荷、head 和导入回执。同一导入幂等；目标已有其他 head 或同一记录的迁移要求冲突时拒绝，不覆盖有效状态。原文件及代次文件不改写，归档或只读保留由维护流程处理。

只有代次文件、没有恢复点时，仍须保留已经消耗的代次：

```sh
titan recovery --database /srv/titan/recovery/runtime.db import-generation \
  --strategy-no 77 \
  --generation-watermark /srv/titan/legacy/strategy-77.generation \
  --expected-sha256 '<冻结清单中的代次文件 SHA256>'
```

此操作不创建恢复点，不能满足 `RequireCheckpoint`，也不自动选择 Fresh 或开放交易。禁止仅凭一份较老恢复点的代次猜测丢失的代次水位。

迁移完毕后通过 `inspect` 核对，固定所选恢复引用，再走原有管理恢复与账户对账。原始文件可保留为迁移证据；不要继续运行旧 writer 或维持双写。旧请求和签名文件按原字节保留，不重算旧幂等摘要。

## 5. 固定引用、导出与保留

以下 `'<引用 JSON>'` 指检查结果中的完整 `RecoveryPointRef` 对象；不要只复制最新序号或路径：

```sh
titan recovery --database /srv/titan/recovery/runtime.db pin \
  --reference '<引用 JSON>' --operation-id maintenance-001

titan recovery --database /srv/titan/recovery/runtime.db export \
  --reference '<引用 JSON>' --output /srv/titan/export/recovery.json

titan recovery --database /srv/titan/recovery/runtime.db unpin \
  --reference '<引用 JSON>' --operation-id maintenance-001
```

导出保持原始字节，输出路径必须不存在。错误数据库身份、记录或摘要被拒绝，不能自动替换成最新恢复点。管理恢复的 pin 在目标记录核验后建立，保守保留不确定操作的引用；当前不凭超时自动回收孤立 pin。释放前必须核对同一 Operation 的实际回执。pin 总量上限 4096，重复 pin 同一引用/操作幂等。

M1 提供显式维护清理，不在交易线程自动回收：

```sh
titan recovery --database /srv/titan/recovery/runtime.db prune \
  --strategy-no 77 --retain 64
```

清理需要命名空间独占，活动宿主持有锁时拒绝。保留 head、pin、迁移回执依赖以及指定滚动窗口，仅删除不再需要的记录和内部提交索引；不在此命令中 VACUUM。长期运行的容量及归档自动化在后续阶段实施，磁盘满当前按失败关闭处理。

## 6. 备份与灾备边界

```sh
titan recovery --database /srv/titan/recovery/runtime.db backup \
  --output /srv/titan/backup/runtime-20261027.db

titan recovery --database /srv/titan/backup/runtime-20261027.db inspect
```

备份使用 SQLite Backup API，在暂存文件中完成一致复制与激活隔离，关闭并同步后才以不覆盖方式发布。目标文件必须不存在；发布后用 `inspect` 核验全部载荷。副本中的 `activation_allowed=false`，可检查或导出，不能直接配置为交易恢复库。

备份只给出该数据库的一致边界，不代表管理库、历史库、对象、外部账户和单调时钟共同回到了同一时刻。恢复前仍须核对旧宿主退出、命名空间身份、原期限、未回退的代次证据和权威账户事实。M1 不提供跳过这些证据的“一键激活备份”。

数据库绑定规范化位置，复制到另一位置和硬链接不能直接激活。任意绕开工具、在原位置覆盖同一存储身份的旧字节，不能仅凭数据库自身证明未发生历史回退；这不属于支持的自动灾备流程。应保留原目录身份见证和管理/节点回执，证据不足时保持停止，不清锁、不换编号、不选择更老状态继续交易。

## 7. 错误与验收

冷路径和 writer 诊断可区分数据库 busy、full、read-only、corrupt、角色/版本不符、引用改变、所有权冲突及代次溢出；异步提交失败对调用者统一关闭为 `checkpoint_writer_failed`，提交等待超时另有明确错误。writer 日志只记录诊断码，不输出私有载荷或凭据。健康失败是粘性的，不能用一次后续成功写入清除。单独的 `close` 等待超时不释放锁，排空后可重试关闭；它不能清除已经发生的提交失败。

验证入口为 [verify_data_persistence.sh](../scripts/verify_data_persistence.sh)，仅允许在 `192.168.3.88` 执行。它要求显式提供源码基线、旧签名策略/恢复点的模拟 fixture 及已有 Chromium 路径，并记录代码摘要、命令、退出码、原生/SQLite/维护/浏览器结果。真实账户和实盘部署另走受控授权流程，本次 M1 不执行。

## M3 新运行预算准入

新运行的 SQLite 提交使用已验收的容量和时间类别：编码载荷最多 2 MiB，默认队列 16；宿主 `runtime.max_handler_duration` 必须显式不少于 750 ms。通用 5 ms 默认值保持原值，但在 SQLite 运行注册时返回 `recovery_handler_budget_too_small`。此检查不会修改既有运行的回调、恢复、行情新鲜度或停止期限。超大新提交返回 `recovery_runtime_payload_budget` 并关闭该存储实例的后续提交准入，迟到提交不能重开执行门。

旧载荷导入及兼容读取仍保留 32 MiB 上限；能够导入不等于满足新运行载荷预算。维护恢复时同时核对原策略、账户证据、载荷大小和既有期限，不通过放宽停止期限绕过预算。PostgreSQL/S3 配置、切换和故障均不改变恢复后端，恢复点继续使用节点本地 `runtime.db`。

## 候选 05 故障恢复的已验证范围

[非实盘故障补验](verification/2026-10-28/data-persistence/m4-faults/README.md)已验证部分成交及 bootstrap 期间原生成交重放，均使用原 60 s SQLite 点、固定 BLOB/pin，跨进程代次 1→2 且不延长原总期限。重建的是已确认的消费边界；不能把未收到的独立成交、未决执行或已失效账户连接补成可信状态。

Stop 有安全平仓和失败关闭两种结果。私有断线或事实不足时，本轮 E 退出而 A 最终 FAILED，安全证明 false，模拟残仓和占用保留；不能删除 pin、回退代次、刷新原期限或创建新 namespace 让运行重新激活。原 10 s 引擎超时回执的客户端观察可能晚于 10 s，本轮 11.702 s 明确记为失败。真实两条保留占用继续保持，非实盘证据不授权处置。

## 补查事实后的固定点新代次恢复

[候选 07 验收](verification/2026-10-28/data-persistence/m4-safe-recovery/README.md)支持对已知订单补查完整原生成交，以原固定点为基础在闭门阶段重放未消费增量。原始 SHA/checksum、schema、绑定和无未决执行门禁仍先验证；回调尝试发单/撤单、财务提交失败或身份冲突都会拒绝激活。当前范围仅限可精确证明的净持仓增加及零已实现盈亏，不能用历史接口返回“若干行”冒充完整恢复证据。

缺成交异常可能让后续周期 head 变成 pending；原 head 和私有业务错误继续保留，不能修改 SQL 或 BLOB 清除错误。验收选择派单前自然提交的无未决固定记录，由原生成交解释后续库存，并提交新代次记录；没有提前触发周期、改写旧点或延长原总期限。私有断线正例使用断线前已消费成交的原 60 s 健康点。

激活只发生在新 E 和新代次。旧 E 需先退出，旧运行仍 FAILED、双门关闭、安全证明 false；新旧 BOOTTIME 截止值及到期值逐项相同。补查已知订单不清除 `ACCOUNT_STREAM_GAP` 或旧冲突，不生成旧实盘 E 的安全释放证明。实盘及两条真实占用继续不动。
