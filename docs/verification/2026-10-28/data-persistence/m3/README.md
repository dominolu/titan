# M3 PostgreSQL/S3 与切换验收

验收日期：2026-10-28（Asia/Shanghai）。

状态：M3（P10–P13）全部非实盘门禁通过。37 个步骤完成，产品源码和产物在本轮全流程保持一致；验收脚本等待上限修订及续验单独留证。

最终汇总：[summary.json](summary.json)；执行步骤：[最终步骤](steps-effective.jsonl)（[首次执行](steps.jsonl)、[续验](steps-resumed.jsonl)均保留）；源码身份：[最终核对](source-identity.json)、[原冻结清单](source-frozen-original.json)、[最终清单](source-frozen-final.json)、[本地字节对照](source-comparison.json)；产物身份：[artifact-sha256.json](artifact-sha256.json)；环境：[environment.json](environment.json)；清理：[cleanup.json](cleanup.json)。

18 组共 5,468 笔模拟成交、18 次 PostgreSQL 裁剪后分页和 18 次 SQLite 回填通过；9 个策略版本均从 S3 冷取原文件。[3,033,096,067 字节、195 个对象、31 份发布清单](bulk-switch.json)完成目录→S3→目录；原生 34 项流程/10 项浏览器、只读 6 项/8 项浏览器及历史 22 项浏览器通过，本地 24 项闭环通过。详细结果见[历史矩阵](historical/results.json)、[本地持久化](local-persistence/results.json)、[原生切换](loopback/backend-switch.json)。

## 代码与环境

- 基线：`5997364864861fdd5d8c649b93e54161018f703d` 加本工作区 M1–M3 修改，没有创建提交。
- 编译及全部非实盘验证：`192.168.3.88`，独立目录 `/home/dominolu/dev/titan-persistence-m3-20261027-59973648`。该目录是隔离源码快照，不是 Git checkout；不能把声明的基线当成远端 Git HEAD。
- 最终候选证据目录：上述目录的 `evidence/m3-final-05`。前序失败轮次保留在服务器；本轮历史矩阵末尾回切因脚本 300 s 等待超时，仅修订 2 份验收脚本后对同一 verified 计划续验，不重跑 18 组引擎。原始失败及续验均保留，[脚本调整](verification-harness-amendment.json)、[进度恢复来源](historical-progress-recovery.json)。
- 冻结源码清单含 551 个文件，覆盖 Rust、SDK、脚本、配置、策略、生成类型及前端；最终清单与结束时及本地逐项核对；与最初清单相比只变更上述 2 份验收脚本，产品源码及产物保持一致。
- SQLite 3.53.2 / rusqlite 0.40.2；postgres 0.19.14 / tokio-postgres 0.7.18 / postgres-native-tls 0.5.3；AWS S3 SDK 1.116.0。PostgreSQL 测试服务 15.12，`synchronous_commit/fsync/full_page_writes=on`。
- S3 服务为 Alpine 封装的 MinIO 构建，程序报告 `DEVELOPMENT.GOGET`；以 OCI digest `sha256:cf23643a6cf9ce159c57643ceb88279e431262282428c9e0bf3a7ef1a97e84b4` 固定身份，不根据开发版本字符串推定官方发布号。测试服务只绑定 127.0.0.1，使用本任务标签及独立卷。

## 本轮检查结果

| 范围 | 本轮结果 |
| --- | --- |
| Rust 工作区所有 target/feature | 644 passed，24 ignored；doctest 2 passed、1 ignored |
| 真实驱动契约 | [14 个场景](external/results.json)；显式执行 normally ignored 的服务集成入口 |
| 传输/进程故障 | [10 个场景](external-faults/results.json)；包括 PostgreSQL COMMIT 完成后丢确认 |
| 身份/过期计划负例 | [3 项补充](supplemental-identity.json)（[当次探针源码](supplemental-identity-probe.py)），在独立测试控制库注入不匹配预期 ID 后恢复；不改变共享实际目的地身份 |
| SQLite 争用预算 | [288 次提交及 3 组突发](recovery-contention.json)，FULL 同步 |
| SDK/报告及前端 | SDK 115 项、报告 5 项；类型、格式、lint、缓存隔离和构建门禁通过 |
| 原生兼容/恢复 | 旧签名策略与恢复字节兼容、维护、真实原生 service、停止和计时器负例通过 |

Rust 原始日志：[工作区测试](rust-tests.log)、[doctest](rust-doctests.log)。默认 ignored 测试未被批量解禁；真实驱动及性能探针由专用阶段显式执行，缺测试服务配置时失败，不静默跳过。

## 后端协议与故障范围

PostgreSQL 批次与源游标同事务提交；真实 SQL trigger、并发 exporter、提交前 SIGKILL、提交后截断 `CommandComplete(COMMIT)`、重复补传和网络黑洞均有独立检查。黑洞调用在约 4 s 客户端总期限内返回；本地恢复 writer 不持有远端连接。

S3 检查条件创建、并发相同内容、错误摘要、跨控制实例 owner、无 TLS 策略、错误凭据和秘密不出日志、PUT 已提交但确认丢失、下载 body 中断、错误条件成功响应及无人持有的 staging 清理。SDK 只在必需时启用附加传输 CRC；平台始终读取并核验大小/SHA256，ETag 不作摘要。所有文件落地还校验目标原字节，失败不发布成功 manifest。

历史和对象均实际往返；裁剪或驱逐后从目标回填。缺历史、缺清单引用、目标身份变化、源路径换库、旧修订计划和旧游标拒绝。配置库丢失不能重建一个默认目录后端。原结果包 `history.db` 保持不可变，`history-source.db` 工作副本承担投影、裁剪和回填；不删除原 v1 文件来掩盖摘要不符。

## 冻结预算及测量边界

每个场景 96 个编码载荷样本，最大 2,000,904 字节；同时测量 16 个小载荷突发。下表来自本候选的原始 JSON，不沿用 M2 或早期候选数字。

| 场景 | P50 / P95 / P99（ms） | 16 个突发（ms） |
| --- | --- | --- |
| 空闲 | 66.624 / 130.024 / 131.405 | 148.334 |
| 历史写入及长读 | 68.837 / 130.604 / 131.758 | 291.471 |
| 对象写入及在线备份 | 68.408 / 129.887 / 132.230 | 314.449 |

历史压力 714 次提交，长读 WAL 峰值 55,055,592 字节；对象场景 364 次压力提交，WAL 峰值 27,657,592 字节，在线备份 218.224 ms。单次提交预算 750 ms、突发预算 5 s 均满足。原生模拟继续使用既有回调 5 s、停止 10 s，未靠延长原恢复/停止期限达标。

新 SQLite 运行载荷上限 2 MiB、默认队列 16，启动准入要求回调预算不少于 750 ms；通用 5 ms 默认值保持原值但拒绝 SQLite 新运行。旧载荷读/导入仍为 32 MiB。事实 DB+WAL 默认 20 GiB/库、最低余量 1 GiB、本地内容对象 100 GiB、单对象 5 GB；账户队列同时受 4,096 条和估算 16 MiB 限制，单条估算最多 3 MiB。对象流按 1 MiB 复查余量，缓存物化另预留目标大小。保留窗口默认持续至显式维护或容量门槛，达到门槛故障关闭，不静默删事实。

这些是指定硬件、debug 候选和冻结负载的测量，不是跨机器/生产 SLA。CSV 准备排序、轻量 trace JSON 及既有报告 reader 继续保留原整批内存行为。内容对象不自动 GC，`clean-staging` 只清理无人持有锁的临时会话。

SQLite FULL 通过真实 `max_page_count` 限制注入，只读通过真实只读连接注入；进程/事务/传输故障不能表述为已切断设备或整机断电。没有执行物理断电、真实账户访问或实盘部署。P14 仍待明确目标与授权。

## 复现

在指定编译服务器隔离目录执行。测试服务的 `client-env.json`、服务 env 文件、签名种子、登录令牌及节点配置保留在私有证据目录，不复制到文档。

```sh
python3 scripts/persistence_test_services.py start --root evidence/services-new
python3 scripts/run_m3_verification.py \
  --services evidence/services-new --output evidence/m3-new
python3 scripts/persistence_test_services.py stop --root evidence/services-new
```

这些入口使用本轮冻结数据/参数、旧恢复 fixture 及 Chromium 绝对路径；更换基线时先更新来源和环境，再重新验证。所有服务命令核对任务标签，只清理对应容器和卷，不操作服务器既有 PostgreSQL 或其他服务。

批量对象 prepare/activate 的验收脚本等待上限为 1,800 s，覆盖多次分别有期限的网络请求，不改变驱动单次请求或策略生命周期预算。同一 fixture 的 `checks-progress.json` 每项原子保存；若最终对象激活中断，可在原历史脚本参数上追加 `--resume-after-matrix --resume-object-plan PLAN_ID`，只续验同一计划及余下检查。必须保留原配置、进度和回执，不重跑 18 组矩阵。该参数只适用于专用测试 fixture。
