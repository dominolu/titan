# P14 真实成交恢复补验记录

日期：2026-10-28（Asia/Shanghai）。状态：**未通过；用户明确要求保留占用，停止本轮复验**。首轮[零成交 M4 验收](../m4/README.md)的历史通过范围保持原样，不能据此认定本次补验完成。

本次沿用 `ubuntu@43.130.234.54`、gate / ID 1、`GATE_FUTURE_BTC_USDT`，最小数量 0.0001 BTC、含在途风险上限 100 USDT、价格偏离 50 bps。总期限 120 s、handler 1 s、启动 30 s、Stop 10 s、周期恢复点 60 s、策略平仓 8 s 均未延长。全部程序和原生策略在 `192.168.3.88` 构建；实盘机没有编译。

## 当前处置

- 用户选择“保留占用，停止本轮复验”；没有执行人工解占，没有启动下一轮实盘运行。
- 最近一次[独立 GET 核对](postflight-03-independent.json)为目标零挂单、零非零持仓、旧 BTC 控制器零、Titan 进程零。之后没有交易；最终[清理回执](final-cleanup.json)再次确认无 Titan 进程。
- 保留原崩溃运行 `UNKNOWN`、两条账户/命名空间占用、失败恢复回执、原期限、原始恢复记录与 pin、全部账本及安全平仓意图。没有清除诊断或更换 namespace。
- 临时明文凭据已删除，七个任务传输文件已删除；私有数据库、完整账户快照与 BLOB 留在实盘节点。对外仅保存经实际凭据值扫描的白名单，见[摘要](evidence-sha256.json)及[中间清理](cleanup-interim.json)。

## 现场过程与失败边界

| 尝试 | 已核对事实 | 未通过原因与退出 |
| --- | --- | --- |
| 01 | 签名部署通过；策略未激活；无委托、无成交、无代次预留 | 目录读取遇到 SQLite 建表窗口，报告 schema 不支持；E 另记录账户 bootstrap 连续性失败。两者分别保留，不能仅凭同时发生断言因果。[日志](acceptance-01.log) |
| 02 | 最小买入和平仓各一笔，交易所均确认 FILLED；正常 Stop 后零目标风险 | 仅接收订单累计量合成的 Fill，独立成交 ID 和费用缺失，独立账本为零；等待独立成交超时。[日志](acceptance-02.log)、[安全停止对账](safe-stop-resolution-02.json) |
| 03 | 原生独立买入成交一笔及费用入账；持仓 1 lot；暂停无开仓单；60 s 周期 SQLite 点保存确认持仓及 `pending_execution=false`；64.167 s 后执行受控 E 崩溃 | 恢复候选预留代次 3，但恢复回调拒绝，未尝试激活。原私有业务状态已因消息顺序进入 `RECONCILING / 205`。所有自有进程退出后，单独执行最小量 reduce-only 安全平仓，交易所确认 FILLED；本轮明确失败。[日志](acceptance-03.log)、[后置结果](postflight.json) |

前两轮的预占分别通过正常目录接收匹配 E 证明处理，没有直接改库：[未激活证明](nonactivation-resolution.json)只释放对应失败 Fresh Start；[正常 Stop 证明](safe-stop-resolution-02.json)释放其对应运行。第三轮是原崩溃持仓的恢复失败，不能套用“候选未激活”证明释放前驱占用。

四笔本轮实际订单（两买、两卖）均已由交易所逐笔 GET 核对为 FILLED，每笔 0.0001 BTC；其中第三轮的卖出属于独立安全清理，不能计为策略 Stop 验收。核对结果及两条待处置占用见[人工处置方案](operator-release-plan.json)。该方案的 `operator_authorized=false`，**没有执行**，用户已选择保留占用。

## SQLite 与期限

[checkpoint-proof.json](checkpoint-proof.json)证明周期点含一笔已消费独立成交、确认持仓 1 lot，以及公共执行屏障无未决量。该文件的 `passed` 仅对应这些条件；它没有证明策略业务状态允许恢复。后续现场诊断确认私有状态为 `RECONCILING / 205`，因此不能将该点或五项已通过步骤解释为恢复闭环通过。

| 字段 | 保留值 |
| --- | --- |
| RuntimeStrategyNo | `1793153499`，本次始终使用同一命名空间 |
| store ID | `fc33061510493847569029084cd5559f` |
| record ID | `115d300d642e13a87005e7c91e7c50d2` |
| payload SHA256 | `6331a1207de344b7f3b2ad67d7115cbc1860f63268b623afd2028bd194f0f519` |
| 崩溃点 / 失败候选 | 代次 2 / 已预留代次 3；代次下界未回退 |
| 原 expires_at_ns | `1793155635534956658`，未修改，现已到期 |

恢复继续存本地 SQLite，历史继续使用本地事实库。没有生产 PostgreSQL/S3 切换或既有数据迁移。没有复写恢复 BLOB、清零代次、删除账本或将崩溃来源改为清洁完成。

## 修复与非实盘验证

平台修复了目录在已确认正代次前读取未完成 SQLite schema 的窗口，并仅在匹配 E 的迟到未激活证明、原 Fresh Start 已失败/取消、同节点且占用仍属该操作时释放预占；失败 Restore 的前驱占用继续保留。

CrossEx 现在要求 `usertrades` 订阅确认，按原生 `transaction_id`、数量与费用产生独立 Fill。订单状态流保留 reduce-only，不再合成一笔缺成交身份/费用的独立记录。长交易所 ID 采用稳定映射，避免不同到达顺序和进程重启改变财务身份；重放去重、矛盾拒绝及 8,192 条证明容量均有回归覆盖。协议依据 [Gate 官方 WebSocket 文档](https://www.gate.com/docs/developers/crossex/ws/en/)。

实盘先推持仓、后推成交时，网格原先要求持仓 publication sequence 不小于成交 sequence，导致数量已一致仍超时为 205。修复只调整业务确认条件：先拒绝尚未消费的订单累计量，再核对实际与期望数量；平台的库存屏障、数量不一致的期限与拒绝条件保留。策略没有新增存储 API，SDK、ABI 和状态 schema 不变；本次业务修复需要重新编译签名策略，不能称策略源码未变。

| 验证 | 结果与证据 |
| --- | --- |
| 平台 Rust 格式、check、Clippy `-D warnings`、构建 | 通过；[Clippy](build/clippy.log)、[工作区日志](build/workspace-tests.log) |
| 工作区 / doctest | 647 passed / 24 ignored；2 passed / 1 ignored；[doctest](build/doctests.log) |
| 配套 Web | 类型、格式、lint、缓存、构建通过；[浏览器 10 项](build/browser.json) |
| 平台原生模拟闭环 | 32 项通过，使用该平台程序及业务修复前的策略；[结果](build/loopback-results.json)，不冒充新策略实盘证明 |
| 精确先持仓后成交对照 | 旧原生策略复现 `205 != 0`；[对照日志](build/position-first-reproduction.log) |
| 新策略与守卫 | 9 项通过，包括原生单 lot 持仓先到、60 s 后恢复与 reduce-only 退出、人工回执必须匹配审计及保留存储；[日志](build/native-policy-tests.log) |
| 修复后 SDK | 115 项通过；[日志](build/sdk.log) |
| 源码 | 557 文件与本地字节一致；[身份](build/source-identity.json) |

工作区首次运行暴露一个既有异步测试的读取竞态：屏障先发布 Failed，再发布健康状态。测试改为等待两个原完成条件，原断言与期限保持；没有改变引擎生产逻辑。另有缺少编译 Python 环境变量的失败准备运行，纠正显式工具链后重验；未放宽测试或策略屏障。

## 发布候选

实盘第三轮使用候选 03 的程序及旧业务策略，部署 60 文件核验通过，见[部署证明](deployment-03.json)。修复后的候选 04 已在编译服务器冻结，**未部署、未复验**，见[包证明](m4-fill-package-proof-04.json)及[逐文件清单](release-manifest.json)。

| 产物 | SHA256 |
| --- | --- |
| 候选 03/04 共用已验证程序 | `9649e59d4cb97d1b8e4be25b2081960d2b728573bf5fd25507fff7bc827d6929` |
| 候选 04 新签名策略 | `dcc71c4a2594641a1d8368b5f248ffcede8b622230eee491f23e453dba616d8a` |
| 候选 04 发布包 | `1d9254564725a7691bfa87b721a50305d6a3cbadc48c297a7ae12ec4b60e2680` |

编译证据位于 `192.168.3.88:/home/dominolu/dev/titan-persistence-m3-20261027-59973648/evidence/`；私有现场证据位于 `43.130.234.54:/data/titan-persistence-m4-fill-20261028-59973648/run-01`。候选 04 为 `evidence/m4-fill-release-04`，实盘已有的 01–03 产物及失败证据保留。

真实成交后的完整恢复、恢复后成交/费用不重复、有仓位的策略 Stop 平仓及其 10 s 完成均未通过本次实盘验收。没有长期运行、物理断电或后端生产迁移结论；后续须重新取得明确授权，不能自动解占或因非实盘通过而继续交易。
