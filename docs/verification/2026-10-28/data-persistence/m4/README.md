# M4 / P14 受控实盘验收

日期：2026-10-28（Asia/Shanghai）。状态：**本轮受控实盘验收通过，P14 完成**。

使用 `192.168.3.88` 冻结并验证的原 M3 程序，在用户明确授权的实盘节点完成只读准入、签名部署、最小下撤单、暂停/恢复运行、固定 SQLite 记录恢复和原期限停止。主流程耗时 **22.02 秒**，13 项检查、7 个管理操作全部通过；六笔最小委托全部撤销，无成交。独立后置核对和清理也通过。

汇总：[summary.json](summary.json)；主流程：[results.json](results.json)、[日志](acceptance.log)；持久化审计：[audit-summary.json](audit-summary.json)；独立后置核对：[postflight-independent.json](postflight-independent.json)；清理：[cleanup.json](cleanup.json)。

## 现场约束和结果

| 项目 | 本轮实际核对 / 结果 |
| --- | --- |
| 实盘节点 | `43.130.234.54` / `VM-0-33-ubuntu` / x86_64 |
| 运维入口 | 用户明确授权的 ubuntu SSH，指定密钥，既有 sudo 权限；root 直接 SSH 公钥认证失败 |
| 账户 / 品种 | CrossEx gate，后端 ID 1，`GATE_FUTURE_BTC_USDT` |
| 规格 / 模式 | FUTURE / live；tick 0.1 USDT、lot 与最小量 0.0001 BTC；CROSS_EXCHANGE / SINGLE |
| 初始目标风险 | BTC 挂单 0、非零持仓 0，旧 BTC 控制器 0，其他 Titan 进程 0 |
| 限额与价格保护 | 含在途上限 100 USDT、新鲜 BBO 价格偏离最多 50 bps |
| 原期限 | 总期限 120 s、handler 1 s、Stop 10 s；恢复未改变原 expires_at_ns |
| 最小下撤单 | 6 笔，每笔 1 lot / 0.0001 BTC，最终均为 CANCELED；18 条订单事实 |
| 真实成交 | 累计成交量和独立成交记录均为 0 |
| 风险采样 | 最大实际名义敞口 0；最大含在途上界 **16.20044 USDT**，采样覆盖不完整 |
| 最终状态 | 目标挂单/非零持仓 0，本轮账户占用 0、Titan 进程 0 |

前置为 GET-only，账户后端以 SQLite `mode=ro` 读取，不初始化旧服务注册表；准入没有输出余额或凭据。见[前置报告](preflight.json)。规则及接口依据 [Gate CrossEx 官方文档](https://www.gate.com/docs/developers/crossex/)，数量、模式和当前风险均以本轮节点实时查询为准。

## 下撤单、停止及恢复

七个操作依次为 deploy、start、pause、resume、pause、restore、stop，全部 succeeded。A/N 重启复用同一暂停 worker，Resume 保持原代次；暂停等待交易所撤单终态和目标零持仓后，才对已核验 PID/启动时间的本轮 E 执行受控 SIGKILL。

本轮实际读取并保留了含 2 个非终态订单意图的旧 checkpoint，恢复经原订单身份的权威历史查询核对终态。恢复点始终在本地 SQLite，原始载荷 SHA256 为 `cd40211b5807aca137a6b37b32eb0f03d767bfa67c2d66363380298af599ec17`。旧记录 ID、store ID 和 payload SHA 组成固定引用，恢复 Operation 对该记录的 pin 数为 1，冻结 managed spec 使用同一引用。代次从 1 增至 2，两次 `expires_at_ns` 均为 `1793151526681151096`。

见[固定引用与原期限](recovery-proof.json)、[SQLite 检查](recovery-inspect.json)、[审计](audit-summary.json)。旧崩溃代次的 UNKNOWN 与未清洁来源保留，不将其改写为正常退出；新运行成功停止、给出安全释放证明，并清空本轮管理账户占用。

统一历史查询保留旧运行 12 条、新运行 6 条订单事实，来源/运行隔离。旧源明确显示 1 个崩溃缺口，新源干净封存，收到的片段完整；两者均不声明交易所完整历史，见[事实证明](facts-proof.json)。事实、风险和独立业务采样来自平台，不从恢复私有 BLOB 构造页面指标。实盘历史采用本地 SQLite；PostgreSQL/S3 往返仍以 M3 非实盘证据为准，本轮没有迁移外部生产后端。

## 发布及源码身份

[部署核对](deployment.json)验证 46 个文件的原字节、原生摘要及签名。实盘主机未编译；程序与 M3 验收程序完全一致。M4 仅修改验收脚本、增加 GET-only 准入脚本及测试，策略、SDK 和平台实现均未改变。

| 产物 | SHA256 |
| --- | --- |
| 原 M3 验证程序（未 strip） | `40ebe284c1c26ec66bcdc53942803033b521263f44670f752897ace26e01e8d3` |
| 签名最小数量策略 | `9accd2b4ad4cbe67904c7b0e149249369f4e984803e673eabe8edcdcc99e4f51` |
| `m4-release-01.tar.gz`（161,188,509 字节） | `6650ee2409dff2012309bc65994b3bbf2bb0e728a3f8a718ee19535e355b05d1` |

编译服务器目录：`/home/dominolu/dev/titan-persistence-m3-20261027-59973648/evidence/m4-release-01`。实盘独立目录：`/data/titan-persistence-m4-20261028-59973648`，安装目录为其下 `m4-release-01`，私有运行证据在 `run-01`；未替换既有生产程序或接管旧运行。

准备证据：[prepare-only 日志](m4-artifact-01.log)、[签名/ABI 校验](m4-native-validation.json)、[6 项准入单测](m4-policy-tests.log)、[冻结包证明](m4-package-proof.json)、[逐文件发布清单](release-manifest.json)。M3 工作区完整门禁不重复计为 M4 新测试；M4 使用相同程序和未改变的平台源码。

## 清理及范围

主流程[后置检查](postflight.json)和其后的独立 GET 复核均为零目标挂单/持仓、零验收进程；[持久化审计](audit-summary.json)确认全部六笔委托已撤销、累计和独立成交为零、本轮占用清空。

临时明文凭据文件及两个传输中转文件已删除，未注册长期服务。完整账户快照、账本和原始恢复载荷保留在实盘机私有目录；本目录只取经凭据值检查的脱敏白名单，见[清理](cleanup.json)和[节点导出文件摘要](evidence-sha256.json)。

最大敞口是已记录快照的最大值，`coverage_complete=false`，不能表述为连续行情最大值或完整策略净收益。本轮无真实成交，不声称已验证真实成交后的恢复、有仓位平仓；未执行物理断电、长期策略运行或既有生产数据迁移。本轮 P14 通过范围为授权的最小下撤单、零风险后的固定记录恢复和停止。

## 授权历史与复现

root 入口、指定密钥以及后续 ubuntu 入口均由用户分别明确授权；最终使用 ubuntu 加既有 sudo 权限。最初审批拒绝及 root 认证失败保留在[历史阻断](approval-block.json)、[连接及解决记录](ssh-access.json)。发布清单的授权字段和[准备汇总](preparation-summary.json)是打包/准备时快照，当前状态以本文和 `summary.json` 为准。

后续复现先明确本轮节点、账户、品种和风险授权，在实盘节点运行 [GET-only 准入](../../../../../scripts/persistence_live_preflight.py)；凭据和私有输出仍留在该节点。再用冻结程序和签名制品，通过 [management_live_control.py](../../../../../scripts/management_live_control.py) 的 `--release`、`--expected-sha256`、新私有 `--root` 和 10 分钟内的 `--preflight` 报告执行。主机、规格、模式、已有风险或控制器不符时拒绝；恢复和清理始终按原记录、代次、期限和进程身份处理，不重派未知操作或清除缺口放行。

## 后续真实成交补验

同日的[真实成交补验](../m4-filled/README.md)未通过，用户明确选择保留占用、停止本轮复验。该记录单独保留失败过程、独立安全平仓和非实盘修复候选；不改变本文首轮零成交的历史通过范围。
