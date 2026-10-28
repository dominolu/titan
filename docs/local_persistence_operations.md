# Titan 本地历史与结果存储手册

适用范围：M2–M3 的 SQLite 历史、Parquet、本地/S3 内容对象、PostgreSQL 投影及结果目录。恢复点维护继续使用[恢复存储手册](recovery_storage_operations.md)。所有命令在持有相应目录权限的节点运维环境执行，不加载交易连接器，不运行策略。

## 1. 目录和配置

| 用途 | 实际位置及规则 |
| --- | --- |
| 执行恢复 | `strategy_service.recovery_storage.path` 指定的 `runtime.db`；与历史库分开 |
| 直接启动的账户事实 | `[account_service].history_path`；未指定时使用恢复目录的 `history.db`，无恢复目录则使用工作目录的 `history.db` |
| 管理节点账户事实 | 节点 `work_root/account-facts.sqlite3`；保留旧事实表，同时增加统一归档表 |
| 单次回测 | `operations/<operation_id>/history/{result.json,history.db,facts.parquet,manifest.json}`；M3 登记后另有可裁剪的 `history-source.db` |
| 数据准备 | 准备 Operation 目录中的 `prepared.bin`、`prepared.parquet`、`prepared.json` 与发布凭证 |
| 管理节点内容对象及目录 | `work_root/storage/objects/<sha前两位>/<sha256>`、`work_root/storage/catalog.db` |

直接启动的配置示例：

```toml
[account_service]
history_path = "storage/history.db"
recording_environment = "simulation"
```

`recording_environment` 只接受账户运行的 `live` 或 `simulation`，默认 `live`；`backtest` 专用于回测执行器。此字段标记数据来源，不会将真实连接器变成模拟连接器。相对历史路径按运行配置所在目录解析。恢复/历史同路径、错误数据库角色和同库环境混用均拒绝；归档 reader/writer 都拒绝未知 schema，不在读操作中自动迁移。

## 2. 本地历史查询

```sh
titan storage facts \
  --database /node/operations/RUN/history/history.db \
  --environment backtest --account simulation --run-id RUN \
  --kind fill --limit 20
```

账户历史将环境和账户换成实际登记值。`--strategy-id` 仅用于已证明归属的数据；原始账户成交、余额、持仓不因某运行观察过就变成策略私有事实。录制 run ID 代表来源，不证明订单归属。

下一页传入响应 `next_cursor` 的完整 JSON：

```sh
titan storage facts \
  --database /node/operations/RUN/history/history.db \
  --environment backtest --account simulation --run-id RUN \
  --kind fill --limit 20 --cursor '<next_cursor JSON>'
```

游标绑定存储身份、完整过滤条件、读取方向和固定行号上界。新增行留到刷新后读取；修改过滤、方向或换库必须从第一页开始。每页最多 200 条、最多约 4 MiB；单条事实最多 1 MiB。序号、行号和纳秒时间以字符串返回。CLI `facts --newest-first` 和 HTTP `newest_first=true` 从最新记录往前读取；权益图使用此选项，避免长期运行后仍显示最早 20 条。流式导出保持归档行号升序，不重排迟到事件的交易所时间。

管理 API 复用同一查询端口：

```text
GET /api/v2/strategies/{strategy}/runs/{run}/facts
GET /api/v2/strategies/{strategy}/backtests/{backtest}/facts
GET /api/v2/resources/{resource}/facts
```

管理端先做节点、账户、环境和策略归属授权，再由节点固定数据路径与过滤条件。HTTP 不接受任意数据库路径。使用既有登录会话；游标失效返回 409，页面刷新重取。页面的“早期记录”开关仅访问原 `/history` 兼容接口，新归档默认使用 `/facts`。

## 3. 完整性和故障处理

`recording` 描述来源覆盖，包括 `source_count`、`unclosed_sources`、`gap_count`、`received_segment_complete`。没有来源不能解释为完整的零条记录。clean/complete 只证明本平台收到的该段事实排空提交；`venue_history_complete` 始终为 false，不能据此宣称交易所历史已补全。分页固定数据上界，覆盖诊断反映查询时来源健康。

账户采集在启动时先登记来源，再异步写入。事实、业务去重、冲突证据和水位同事务提交，COMMIT 后才更新 durable row。进程被杀或必需队列丢弃会保留不完整标记；重启创建新来源，不抹掉旧缺口。重连同笔原生成交保留来源证据，默认历史页只显示第一次有效业务事实；矛盾内容保留首笔及冲突证据。

必需事实丢弃、writer 错误或身份冲突使 critical audit 不健康，执行层在构造新单传输请求前拒绝，错误为 `CRITICAL_AUDIT_UNAVAILABLE`；撤单仍可用。已进入传输的未知请求仍按原订单身份核对。公开业务帧按既有冷采样保存，重复读同一帧不新增记录，采样丢弃单独计数和标记缺口。业务帧不读取恢复 BLOB。

修复磁盘或锁问题后，先保全原库和诊断、停止旧来源，核对账户事实与占用，再按正常生命周期重启。不要编辑 clean marker、水位、业务去重表或原事实来消除告警。不会自动补全交易所历史或自动 GC；M3 后端及切换命令见下文。

## 4. Parquet 导出和核验

```sh
titan storage export-facts \
  --database /node/operations/RUN/history/history.db \
  --environment backtest --account simulation --run-id RUN \
  --output /exports/RUN.parquet

titan storage inspect-parquet --input /exports/RUN.parquet
```

外部历史生效或本地已裁剪时，追加 `--storage-root /node/storage`，从当前路由读取并向当前对象后端归档。导出通过固定上界分批读取，保留原 envelope，最后提交 Parquet footer 和 `RUN.manifest.json`。旁边的 `archive-objects` 保存摘要对象。现有输出不覆盖；中断留下无 manifest 的文件不能视为提交完成，保留诊断后选新路径重试。

`inspect-parquet` 校验完整列定义、每条 envelope 和列值一致性，返回记录数及有序载荷摘要；该摘要按每条 JSON 的 8 字节大端长度加原始 JSON 顺序计算。它不是交易所完整历史证明。行情 Parquet 和六列整数 prepared tape 使用各自 schema，不能传给事实检查器。

## 5. 结果发布和登记重试

运行器先同步原结果文件，验证 v1 清单中每个相对路径、长度与 SHA256，再写不可变内容对象和发布清单，登记目录，最后发布本地 `manifest.json`。引擎失败也可以提交完整诊断包，但其业务状态仍为失败；存储失败不会被标成业务成功。

登记失败时保留 `manifest.pending.json` 的原字节。修复存储后只重试发布：

```sh
titan storage register-bundle \
  --bundle /node/operations/RUN/history \
  --storage-root /node/storage
```

也可用 `--manifest /evidence/exact-v1-manifest.json` 指定原清单。不能根据当前目录重新拼一份清单规避不一致。相同逻辑 ID、摘要和字节数可重试；冲突拒绝。返回 `reference` 是发布清单的内容引用，不是运行成功证明。

已有发布引用但目录缺登记时：

```sh
titan storage register-publication \
  --storage-root /node/storage --reference '<reference JSON>'
```

数据准备保留 `dataset-publication.pending.json`，尚未发布时可以：

```sh
titan storage publish-manifest \
  --storage-root /node/storage \
  --manifest /node/operations/PREP/dataset-publication.pending.json
```

这些命令验证原对象和清单，不重新撮合、不编译策略，也不重新计算收益。准备的输入摘要和质量仍由原准备回执负责；发布工具不切换管理器当前数据集指针。

## 6. 原字节物化和报告

```sh
titan storage materialize-bundle \
  --storage-root /node/storage --reference '<reference JSON>' \
  --output /reports/materialized-RUN
```

目标必须是新目录。读取时校验内容对象和原 v1 清单，以复制方式物化原字节，最后发布原 `manifest.json`；不使用硬链接，报告编辑不会改写内容对象。报告继续使用既有 `titan-reporting` ResultBundle reader，只展示引擎结果。

孤立对象先保留；M2 不自动删除对象，也不提供恢复/历史/目录跨库一致备份。备份时分别记录各库边界和已发布清单，不把普通文件复制的活动 SQLite 当一致备份。预算和 PostgreSQL/S3 切换见第 7–8 节，本轮结果见实施计划的 M3 证据。

## 7. PostgreSQL/S3 配置和初始投影（M3）

配置样例：[storage-profile.postgres-s3.example.json](../configs/storage-profile.postgres-s3.example.json)。替换目的地，向宿主进程注入样例中指定的环境变量；不要把连接串或密钥写入命令参数、管理请求或文档。生产 PostgreSQL 强制 TLS 并校验证书，S3 使用 HTTPS。bucket 必须预先创建并授予该 prefix 的 List/Get/Put/Delete（探针清理）权限，驱动不自动创建生产 bucket。每个控制实例使用独立 prefix。

```sh
titan storage initialize --storage-root /node/storage \
  --profile /node/private/storage-profile.json

titan storage inspect --storage-root /node/storage

titan storage project --storage-root /node/storage \
  --database /node/account-facts.sqlite3
```

初始化配置不可原位修改，输出只含配置摘要和目标身份等诊断。初始化不会激活外部后端；失败时检查本地生效路由和目标身份，再用原配置重试。管理节点在配置存在时自动启动后台投影，结果登记时自动注册新的回测历史源。直接运行宿主可以单独执行 `project --database ... --follow`；SIGINT/SIGTERM 停止该冷路径导出，不停止策略。省略 `--database` 只对已登记来源执行一轮批次，不能据此假设全部积压已追平。

新建 SQLite 恢复运行的编码载荷上限 2 MiB，回调预算至少 750 ms；不满足时在启动准入阶段拒绝。通用 `max_handler_duration=5 ms` 默认值保持原值，不能直接作为 SQLite 持久化运行配置。本轮原生网格仍采用既有 5 s 回调、10 s 停止配置，不自动扩大已运行实例的恢复/停止期限。旧载荷的兼容读取/导入上限仍为 32 MiB。

## 8. 双向切换与回切

历史和对象独立切换。以下命令返回计划 ID、阶段和摘要证明，保存实际输出作为运维回执。

```sh
titan storage prepare-history --storage-root /node/storage \
  --database /node/account-facts.sqlite3 --target postgres

titan storage resume --storage-root /node/storage --plan PLAN_ID

titan storage activate --storage-root /node/storage --plan PLAN_ID

titan storage facts --storage-root /node/storage \
  --database /node/account-facts.sqlite3 \
  --environment simulation --account gate --kind order --limit 20
```

`prepare-history --target sqlite` 是反向计划。激活前会补传切换期间的新事实，并核验原始行号/字节范围摘要。计划停在 `copying/verified` 时原路由仍生效；进程中断后先 `inspect` 再 `resume/activate`。已提交计划再次激活幂等，路由修订改变后的旧计划拒绝；切换后页面须刷新游标。目标离线不自动切换，也不读取部分本地历史冒充完整结果。初始化锁文件同时作为配置存在的见证；控制库丢失时拒绝查询/新发布和重新初始化，先按保全回执恢复控制身份，不能删除控制库切回默认目录。

```sh
titan storage prepare-objects --storage-root /node/storage --target s3
titan storage activate --storage-root /node/storage --plan OBJECT_PLAN_ID
```

反向计划使用 `--target filesystem`。对象激活最后阶段持有发布排他锁，期间新的冷路径发布可能收到维护中错误，使用原 pending 清单重试。现有对象句柄在路由修订变化后拒绝继续操作，调用方重新取得适配器并重试原发布；业务引擎不得重跑。所有已登记 manifest、文件及原 v1 清单都核验后才激活。

只有外部路由已生效并通过摘要证明后，才可显式释放本地缓存：

```sh
titan storage prune-history --storage-root /node/storage \
  --database /node/operations/RUN/history/history.db --through-row ROW

titan storage evict-local-objects --storage-root /node/storage

titan storage clean-staging --storage-root /node/storage
```

`history.db` 是原 v1 包的不可变快照；M3 登记用 Backup API 创建 `history-source.db`。维护命令接受原 `history.db` 逻辑路径并解析到工作副本，后台查询也使用副本。不得删除工作副本、改写原快照或覆盖为另一个库身份。历史裁剪只接受已关闭来源的已投影前缀，保留去重、缺口和单调序号。当前一次裁剪后须先回填才能再次裁剪；回切需要足够本地容量，不能跳过回填。驱逐内容对象前逐个核验 S3，回切时从 S3 回填。`clean-staging` 仅清理无人持有 owner 锁的中断 S3 会话，不删除内容对象、待发布清单或恢复点。

默认 DB+WAL 配额 20 GiB/库、最低余量 1 GiB、本地对象 100 GiB、单对象 5 GB；在批次边界检查，保留一个有界批次增长余量。历史和内容对象默认永久保留至显式维护或容量门槛，不自动按日期删除。容量或余量不足时关键采集失败并关闭新单准入，故障修复后核对旧来源与占用，再按正常生命周期重启，不能通过清除完整性标记放行。

配置摘要、控制 UUID、目的地 UUID、路由修订、维护回执、各历史源边界及本地恢复库备份应分别保存。恢复库仍使用 SQLite Backup API；控制/历史备份也须使用 SQLite 一致备份或停写后取副本。恢复旧控制库前必须按保全回执核对生效后端和修订，不能在存储有新写入时覆盖旧备份。不存在跨 SQLite/PostgreSQL/S3 的单一原子灾备快照。
