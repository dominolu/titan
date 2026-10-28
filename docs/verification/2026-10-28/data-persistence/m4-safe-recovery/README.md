# 私有断线与缺成交异常后的安全恢复验收

日期：2026-10-28（Asia/Shanghai）。状态：**候选 07 的 5 个新增场景与 5 个既有故障回归按契约通过。两个恢复正例完成原期限内有仓位 Stop，三个补查负例拒绝激活并保留模拟占用。实盘继续停止，两条真实保留占用未动。**

全部构建、编译和非实盘验收在 `192.168.3.88` 的 `/home/dominolu/dev/titan-persistence-m3-20261027-59973648` 执行，使用真实 A/N/E 和 `127.0.0.1` Gate REST/WebSocket 模拟器。账户凭据为合成值，归档环境为 `simulation`。没有访问 `43.130.234.54`、真实交易所账户或生产管理库。

## 本轮平台改动

候选 05 只允许累计成交和持仓未变化的恢复；单靠现有查单/持仓镜像无法恢复缺失成交。本轮在平台增加以下闭环，策略源码、原签名文件、ABI 和恢复 schema 均不变：

1. 账户领域增加可选的订单原生成交证据端口。Gate 适配器先查单，再有界读取原生成交，再次查单并复核连接身份、订单内容和更新时间。原生 ID 相同但内容不同、扫描达到容量、数量/币种不可解释或成交总量不等于稳定订单累计量时拒绝。稳定累计量为零的订单沿用原有查单恢复路径；非零订单的历史请求失败不会回退到推测成交。
2. REST 补查事实通过原生 `FillV2` 进入原账户采集和财务账本，复用现有原生身份、费用指纹与归档去重，不增加第二套账本。新 E 在准备/激活前等待补查事实提交并核对审计健康；已有账本冲突会拒绝激活。
3. 新代次加载原始 SQLite 固定记录，先验证 SHA、内部 checksum、编译/绑定身份和 `pending_execution=false`。只有完整原生成交能够解释已消费累计量与当前持仓的差额时，才在交易门关闭的恢复阶段重放尚未消费的 Fill 回调。旧成交前缀不再消费；回调即使忽略发单 API 的拒绝返回值，只要尝试派单或撤单，也会使恢复失败。
4. 泛型库存证明和策略原有 `on_recover` 均通过后，提交新代次恢复点，再由显式 Restore 激活新 E。原 BLOB 不重写，原 pin、代次下界及失败回执保留。失败路径同时关闭外层激活门和原生实例命令门；旧运行不会因为重新连上或补齐事实而恢复交易。

实现入口：[账户端口](../../../../../crates/titan-account-service/src/model.rs)、[Gate 证据适配](../../../../../connector/src/account_runtime.rs)、[恢复计划验证](../../../../../crates/titan-strategy-runtime/src/recovery.rs)、[闭门重放与双门关闭](../../../../../crates/titan-strategy-runtime/src/runtime_v13.rs)、[激活前耐久屏障](../../../../../crates/titan-cli/src/management/control.rs)。恢复仍使用节点本地 SQLite，历史 PostgreSQL/S3 切换能力及策略存储无感知边界保持原设计。

当前历史增量恢复限定为**净持仓增加、已知订单、零已实现盈亏且平均价格可精确证明**。本轮 Gate 证据适配只接受净持仓模式，其他模式保守拒绝。订单补查最多 128 个，每个成交扫描上限 4096 条，达到上限拒绝；重放增量总量上限 4096 条。未知订单、平仓/反转历史、不能对齐的已消费边界、无法证明顺序的相同时间戳和非整数平均价格均拒绝。没有新增策略参数来开启存储或恢复行为。

## 新增验收矩阵

| 场景 | 注入与结果 | 实际证据 |
| --- | --- | --- |
| 缺成交异常后恢复 | 只送持仓与累计订单，扣留 native fill，确认 RECONCILING / 205。等待原 60 s 周期产生不可恢复的 pending head，保留该 head；选用派单前已自然提交、无未决执行的固定点。旧 Stop 失败后退出 E、重启 A/N，只有 REST 补查提供缺失成交。新代次消费一次、费用一次，再完成 reduce-only / IOC 平仓 | 总耗时 **89.283 s**，新 Stop 连同 E 退出/A 释放 **3.566 s**；[固定点](safe/missing-fill-recovery/selected-point-proof.json)、[未选用的异常 head](safe/missing-fill-recovery/unsafe-head-proof.json)、[补查](safe/missing-fill-recovery/history-backfill-proof.json)、[审计](safe/missing-fill-recovery/audit.json) |
| 私有断线后恢复 | 先消费原成交并等到原 60 s 健康点，再断开私有流；确认实际重连，旧运行仍失败、双门关闭、仓位/占用保留。重启 A/N 后显式新代次恢复，REST 返回原成交的三份副本，随后 WS 再重放三次；原成交与费用 digest 不变 | 总耗时 **77.525 s**，新 Stop 连同退出/释放 **3.615 s**；[旧运行关闭](safe/private-recovery/old-closed-proof.json)、[补查](safe/private-recovery/history-backfill-proof.json)、[终态](safe/private-recovery/terminal-proof.json)、[审计](safe/private-recovery/audit.json) |
| 补查不完整 | 稳定订单显示已成交 1 lot，历史成交返回空；新代次拒绝激活，无独立成交或费用入账，模拟持仓 0.0001 BTC、占用 2 条保留 | [结果](safe/missing-history-incomplete/results.json)、[补查失败](safe/missing-history-incomplete/history-backfill-proof.json)、[审计](safe/missing-history-incomplete/audit.json) |
| 同一原生 ID 在补查内矛盾 | 同次补查返回相同 transaction ID、不同费用；在入账前拒绝，无补造成交、无新委托、无释放 | [结果](safe/missing-history-conflict/results.json)、[审计](safe/missing-history-conflict/audit.json) |
| 补查与已有财务账本矛盾 | 私有断线后，补查内部一致，但原成交 ID 的费用与旧账本不同。旧费用 **404001 单位**不被覆盖；记录 **1 个归档冲突**和 `TRADE_ID_CONFLICT`，耐久审计门禁拒绝新 E 激活，模拟持仓/占用保留 | [结果](safe/private-history-fee-conflict/results.json)、[审计](safe/private-history-fee-conflict/audit.json) |

两个成功恢复均为代次 **1→2**；独立 SQLite 审计逐项比较新旧 `managed.json` 的 **boot ID、`deadline_boottime_ns`、`expires_at_ns` 完全相同**，同时核对 A 中各运行的原到期值。原固定 BLOB 字节不变，pin=1；最终两个订单各 `fill_count=1`、累计成交 1 lot，独立成交 2 笔、费用总计 **808001 × 1e-8 USDT = 0.00808001 USDT**，最终模拟仓位/占用为零。

所有场景保持 **handler 1 s、启动 30 s、恢复周期 60 s、Stop 10 s、总期限 120 s、100 USDT 风险上限及 50 bps 价格保护**。最小单量仍为 0.0001 BTC。缺成交正例使用派单前已有的自然提交点，没有提前触发周期恢复点、修改私有业务状态或将异常 head 改写为健康点。

旧 Stop 的失败操作和原期限保持失败；新代次的 Stop 是原总期限内的显式新操作，仍使用 10 s 预算，不延长旧操作。原失败运行始终 FAILED、`release_proof.safe=false`，不能把新 E 的安全释放证明改写到旧运行。费用为冻结模拟规则（名义额 × 0.0005，按 `1e-8 USDT / ROUND_HALF_UP`），不是实际交易所费率声明。

## 既有故障回归

同一候选 07 重新通过[上一轮矩阵](../m4-faults/README.md)的全部五项：

| 场景 | 本轮结论 |
| --- | --- |
| 部分成交与跨进程恢复 | 双 lot 签名参数 fixture 原字节复用，4 笔独立成交、费用 1616002 单位；Stop 退出/释放 **3.289 s**；[审计](regression/partial/audit.json) |
| bootstrap 期间重放 | Fill 仍先于 READY，库存/费用不重复；Stop 退出/释放 **3.451 s**；[审计](regression/bootstrap-replay/audit.json) |
| 行情断线重连 | 实际重连后可信平仓，Stop 退出/释放 **3.642 s**；[审计](regression/market-reconnect/audit.json) |
| 私有断线 | 保持失败关闭，模拟残仓和占用保留；[审计](regression/private-disconnect/audit.json) |
| 缺成交异常态 Stop | 原 10 s 引擎期限失败，未补造成交、未释放模拟占用；[审计](regression/abnormal-stop/audit.json) |

新增矩阵与既有回归分别使用独立的模拟器、端口和数据库，部分轮次并行。上述耗时是本轮实际观测值，包含当时资源竞争，不构成新生产性能 SLA。

## 开发负例与最终产物

初始开发门禁暴露了零成交兼容性缺口：零成交夹具没有历史成交接口，非零补查被无条件应用到零成交订单，见[日志](initial-zero-fill-compatibility-failure.log)。改为稳定零累计量保留旧路径后，完整门禁重新通过；没有为非零成交放宽补查要求。

冻结候选 06 的首个完整故障流程在进入恢复前被验收拒绝：旧 E 的外层激活门已失效、生命周期 Failed 且进程退出，但原生命令门标志仍为 true。没有继续派单或产生真实风险；[失败证明](candidate06/failure-proof.json)和[原发布清单](candidate06/release-manifest.json)保留。候选 07 在通用失败路径同时关闭原生命令门，不改变私有策略状态、不制造安全释放证明；随后完整门禁与本页 10 场景重新通过。

| 最终门禁 | 本轮结果 |
| --- | --- |
| Rust 格式、check、Clippy `-D warnings`、CLI 构建 | 全部通过；[步骤与命令](build/steps.jsonl) |
| 全工作区 / doctest | **655 passed / 24 ignored**；**2 passed / 1 ignored**；[工作区](build/workspace-tests.log)、[doctest](build/doctests.log) |
| 原生/守卫 / SDK | **9 / 115** 项通过；[原生](build/native-policy.log)、[SDK](build/sdk.log) |
| 原生管理 / 浏览器 | **32 / 10** 项通过；[管理](build/loopback-results.json)、[页面](build/browser.json) |
| Web | DTO 重新生成，类型、格式、lint、缓存与构建全部通过；[Web](build/web-build.log) |
| 源码与产物 | **563 项源码**与本地字节一致，**64 项冻结文件**验证；[源码证明](source-proof.json)、[发布清单](release-manifest.json)、[打包证明](package-proof.json) |

基线 `5997364864861fdd5d8c649b93e54161018f703d` 加工作区修改，逐文件身份见[清单](build/source-identity.json)。

| 产物 | SHA256 |
| --- | --- |
| 候选 07 程序 | `a74369e2d45a980e99a391b3f04318932a7825340438ec4e720e82b544a31035` |
| 原签名策略，来源于候选 04/05 | `dcc71c4a2594641a1d8368b5f248ffcede8b622230eee491f23e453dba616d8a` |
| 候选 07 发布包 | `71748abe744418a6d7d2f36d3926ca6b6c70bddc2e82a27c7385674c7e20fd5b` |

服务器目录：`evidence/m4-safe-recovery-release-07`、`evidence/m4-safe-recovery-gates-07-final`、`evidence/m4-safe-recovery-07-01`、`evidence/m4-safe-recovery-regression-07-01`。候选 05、候选 06 及全部失败现场均保留，候选 07 未部署到实盘。

## 复现、清理与限制

入口为 [management_fill_recovery_smoke.py](../../../../../scripts/management_fill_recovery_smoke.py)，新场景实现为 [management_safe_recovery_smoke.py](../../../../../scripts/management_safe_recovery_smoke.py)，独立审计为 [verify_safe_recovery_evidence.py](../../../../../scripts/verify_safe_recovery_evidence.py)。在编译服务器仓库根使用全新输出目录，例如：

```sh
/home/dominolu/dev/titan/.venv/bin/python scripts/management_fill_recovery_smoke.py \
  --repo . --release evidence/m4-safe-recovery-release-07 \
  --expected-binary-sha256 a74369e2d45a980e99a391b3f04318932a7825340438ec4e720e82b544a31035 \
  --expected-strategy-sha256 dcc71c4a2594641a1d8368b5f248ffcede8b622230eee491f23e453dba616d8a \
  --candidate-label candidate07 --scenario missing-fill-recovery \
  --output evidence/NEW-UNUSED-DIRECTORY
```

其他新场景为 `private-recovery`、`missing-history-incomplete`、`missing-history-conflict`、`private-history-fee-conflict`。审计脚本接受场景目录列表，仅只读打开 SQLite。每轮原始命令、退出码和日志均随场景保存。

[矩阵汇总](matrix-summary.json)、[进程与临时密钥清理](all-processes-cleanup.json)确认所有本轮自有 A/N/E 已退出，合成账户与临时签名私钥已清理。各负例模拟数据库及占用保留在服务器，没有手工解占。仓库仅收录白名单 JSON/日志，无数据库、恢复私有 BLOB、签名私钥或账户凭据；[导出摘要清单](evidence-manifest.json)固定 143 个证据文件。

`ACCOUNT_STREAM_GAP`、旧缺口和费用冲突均保留；干净关闭或已知订单补齐不证明交易所全账户历史连续，也不恢复完整收益覆盖。未验收任意平仓/反转历史恢复、多腿历史重放、长时间重连、物理断电或新增生产性能 SLA。

本轮不访问实盘，也不核对或修改两条真实占用；之前的真实成交补验仍未通过且按用户要求停止。非实盘新代次恢复成功不生成旧实盘 E 的安全释放证明，不授权实盘部署、重启、解占或新 namespace。实施状态见[实施计划](../../../../data_persistence_implementation_plan.md)。
