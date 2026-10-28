# P14 完整 A/N/E 非实盘集成验收

日期：2026-10-28（Asia/Shanghai）。状态：**候选 04 的跨进程重放验收失败；修复为候选 05 后，完整非实盘链路通过。真实实盘补验仍停止，两条保留占用不变。**

本轮仅在 `192.168.3.88` 执行。使用真实管理服务 A、构建/执行节点 N、原生执行进程 E，以及绑定 `127.0.0.1` 的 Gate REST/WebSocket 模拟器；合成凭据、`recording_environment=simulation`。控制入口走相同的受控部署、启动、暂停、恢复与 Stop 路径，未访问实盘服务器或真实交易所账户。

入口：[management_fill_recovery_smoke.py](../../../../../scripts/management_fill_recovery_smoke.py)。它直接验证并部署冻结候选中的签名文件，不在测试中重新编译策略。模拟器在接受每个 POST 前，独立读取 SQLite，核对对应订单意图已提交且原始载荷 SHA256 正确。

## 候选 04 的负例与修复

[候选 04 日志](m4-fill-ane-02.log)证明签名部署、先持仓后成交、原周期 SQLite 提交、E 崩溃和 A/N 重启、固定记录恢复及原期限保持均已走通。但恢复后重放原成交时，账户风险屏障出现 `execution_inventory_or_intents_unproven`，见[失败证明](candidate04/failure-proof.json)。该候选不能记为通过。

根因在平台的已消费成交边界：恢复时重建了 `orders_list` 与持仓，但 `settled_fills` 只从新连接的活动订单初始化。已成交终态订单不会出现在新连接的 open-order seed 中；重放该订单成交时，平台从零计算增量，重复推进预期库存。财务账本已按原生成交身份去重，策略库存却再次变化，导致屏障正确关闭。该失败发生在接受 Stop 验收之前；退出清理不算通过。

候选 05 在通过原恢复载荷、无未决执行及交易所对账后，从恢复点已有的订单记录重建已消费累计量。重复累计成交继续保留账户归档中的 replay 来源，但不再推进策略库存、调用策略回调或增加消费次数。没有放宽校验、恢复/停止期限或库存屏障，没有增加新的恢复字段、修改策略 ABI/schema，也没有改动原生策略文件。

候选 04 的原程序、签名策略、发布清单与失败现场保留；候选 05 单独冻结，不覆盖原候选。

## 通过场景与实际结果

正式通过轮次：[主流程日志](m4-fill-ane-03.log)、[结果](candidate05/results.json)、[SQLite/账本审计](candidate05/audit.json)、[清理](candidate05/cleanup.json)。共 10 项检查、5 个管理操作通过：deploy、start、pause、restore、stop。

| 验收内容 | 实际证据 |
| --- | --- |
| 冻结产物 | 程序、配套 Web、原生文件摘要及签名核验；跨 N 部署后原生文件 SHA 不变，没有重新编译 |
| 先持仓后成交 | E 已消费持仓版本 **6** 时，独立成交仍为零；随后收到成交版本 **8**；[顺序证明](candidate05/ordering-proof.json) |
| 模拟成交与费用 | 最小买入 0.0001 BTC，原生成交 ID 与 USDT 费用进入同一事实账本；初始成交发送三次，只记一次 |
| 原周期恢复点 | 保持 **60 s**，等到已消费成交、确认 1 lot 持仓及 `pending_execution=false`；策略错误码为零；[恢复点证明](candidate05/checkpoint-proof.json) |
| 跨进程恢复 | 核对 PID/启动身份后 SIGKILL 原 E，保留 UNKNOWN 和模拟占用；重启 A/N；启动不同 PID 的新 E 读取 SQLite 固定记录；旧 BLOB 未变化，pin 数为 1 |
| 代次与期限 | 代次 **1→2**；两次 `expires_at_ns` 均为 **1793160418612141461**，保持原 120 s 总期限 |
| 跨进程成交重放 | 新 E 激活后再发送原买入成交三次；原成交/费用 digest 不变，确认库存保持 1 lot，没有重复开单 |
| 有仓位 Stop | 新 E 发出 0.0001 BTC 的 reduce-only / IOC 卖单并成交；Stop 回执 **1.607 s**，连同 E 退出 **3.690 s**，均小于原 **10 s**；未使用外部安全平仓 |
| 去重与最终状态 | 独立成交 **2** 笔；归档非重放 fill 2 条、跨进程 replay 1 条、冲突 0；恢复后两个订单的 `fill_count` 均为 1；最终 SQLite 持仓 0、模拟占用 0 |
| 来源与清理 | 旧崩溃源保留 1 个缺口；新源干净封存；管理进程和本轮 worker 均退出，合成凭据已删除；[清理证明](candidate05/cleanup.json) |

完整流程耗时 **78.876 s**。handler 1 s、启动 30 s、周期 60 s、Stop 10 s、总期限 120 s 均保持原值。模拟下单数量均为 0.0001 BTC，风险上限 100 USDT、价格偏离上限 50 bps。

模拟手续费规则明确为成交名义金额的 `0.0005`，按 `1e-8 USDT / ROUND_HALF_UP` 舍入；买入/卖出费用分别为 404001 / 404000 单位，总计 **808001 单位，即 0.00808001 USDT**。这些是冻结模拟输入，不是当前交易所费率声明。首次夹具默认半偶舍入与预期不符，修正为明确的模拟规则后重新执行；[首次日志](m4-fill-ane-01.log)保留，不计为产品缺陷或通过。

持久化风险采样最大实际名义额 8.08001 USDT、最大含在途上界 8.08006 USDT，低于 100；`coverage_complete=false`，不声明连续市场最大值。新旧源均不声明真实交易所完整历史。

## 本轮构建与源码

| 门禁 | 结果 |
| --- | --- |
| Rust 格式、check、Clippy `-D warnings`、CLI 构建 | 通过；[Clippy](build/clippy.log)、[构建](build/cli-build.log) |
| 全工作区 / doctest | **648 passed / 24 ignored**；**2 passed / 1 ignored**；[工作区](build/workspace-tests.log)、[doctest](build/doctests.log) |
| 原生与守卫测试 / SDK | **9 / 115** 项通过；[原生](build/native-policy-tests.log)、[SDK](build/sdk.log) |
| 既有原生管理矩阵 / 浏览器 | **32 / 10** 项通过；[管理矩阵](build/loopback-results.json)、[浏览器](build/browser.json) |
| Web | 配套 DTO 重新生成，类型、格式、lint、缓存和构建通过；[构建日志](build/web-build.log) |
| 源码身份 | 基线 `5997364864861fdd5d8c649b93e54161018f703d` 加工作区修改，**558 文件**与本地字节一致；[清单](build/source-identity.json) |

候选 05 的[发布清单](release-manifest.json)含 65 个文件，见[冻结证明](m4-fill-package-proof-05.json)。编译与完整集成均使用相同冻结程序；新产物没有部署到实盘服务器。

| 产物 | SHA256 |
| --- | --- |
| 候选 05 程序 | `38495ebcd5671901cc19ee07837c20fdaa3710a437e69d7b14b0cc17082f0b89` |
| 候选 04/05 共用原签名策略 | `dcc71c4a2594641a1d8368b5f248ffcede8b622230eee491f23e453dba616d8a` |
| 候选 05 发布包 | `87fa7b3eae697826f381a0d2a1093cfd5e6b52fde28a837aa5f4e18516d848b0` |

服务器目录：`/home/dominolu/dev/titan-persistence-m3-20261027-59973648`。通过场景为 `evidence/m4-fill-ane-03`；负例为 `evidence/m4-fill-ane-02`；门禁为 `evidence/m4-fill-replay-fix-05`；冻结产物为 `evidence/m4-fill-release-05`。

复现仅在编译服务器执行，使用新的 `--output`，指定该冻结 `--release`、上述 `--expected-binary-sha256`、`--expected-strategy-sha256` 及 `--candidate-label candidate05`。脚本校验服务器身份、全部发布摘要和 loopback URL，不能用来连接真实账户。

## 实盘状态及限制

本轮没有连接 `43.130.234.54`，没有读写实盘账户或管理库，没有解除两条保留占用，也没有创建人工释放回执。此前[真实成交补验](../m4-filled/README.md)仍是未通过、按用户要求停止；原 UNKNOWN、原到期期限、恢复点、代次下界和 pin 的保留决定不变。

本轮补齐的是完整模拟链路。真实成交后的恢复、真实跨连接历史兼容、真实有仓位 Stop 及人工占用处置仍需各自授权和验收；不能把本次模拟通过改记为实盘通过。尚未覆盖任意部分成交组合、bootstrap 中重放、长时间重连、异常态自动平仓或物理断电，亦不新增生产性能 SLA。
