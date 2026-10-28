# 候选 05 非实盘故障验收

日期：2026-10-28（Asia/Shanghai）。状态：**5 个故障场景按各自契约通过：3 个安全平仓停止，2 个失败关闭并保留模拟残仓和占用。实盘继续停止，两条真实保留占用未动。**

全部验证在 `192.168.3.88` 的隔离目录 `/home/dominolu/dev/titan-persistence-m3-20261027-59973648` 执行。使用真实管理服务 A、节点 N 和原生执行进程 E，Gate REST/WebSocket 模拟器仅绑定 `127.0.0.1`，凭据为合成值，账户事实环境为 `simulation`。本轮没有连接 `43.130.234.54`、真实交易所或生产数据库。

## 产物与变更边界

直接使用上一轮冻结的 `evidence/m4-fill-release-05`。本轮 **65 项发布文件全部原字节通过核对，Rust 平台、策略源码、ABI、恢复 schema 和配套 Web 均未修改**。558 项原源码清单中，只有验收入口脚本变化；新增故障模拟器、独立证据审计和双 lot 模拟参数文件。见[矩阵与源码审计](matrix-audit.json)。

| 产物 | SHA256 |
| --- | --- |
| 候选 05 程序 | `38495ebcd5671901cc19ee07837c20fdaa3710a437e69d7b14b0cc17082f0b89` |
| 原签名策略，4 个单 lot 场景复用 | `dcc71c4a2594641a1d8368b5f248ffcede8b622230eee491f23e453dba616d8a` |
| 部分成交专用双 lot 签名参数包 | `b506c62e1372ced88229f4de2661458305aa3504102f32e3c7ea99b210f9130e` |
| 原策略源码 | `8a7c8169cf72ca17466a5bbfb5079e365220af8f3959ccc96a8a757bd5db6bc1` |

最小整数数量为 1 lot = 0.0001 BTC，原单 lot 委托无法合法拆分。因此仅在编译服务器将 `order_qty_lots`、`max_position_lots` 从 1 改为 2，编译同一策略源码形成独立模拟 fixture；其余参数逐项与冻结候选相等。原签名文件未覆盖。A→N 部署每个场景的固定签名文件，验收过程中不再重编译。见[参数与公开元数据](partial-artifact-metadata.json)、[签名校验](partial/native-validation.json)。策略没有存储调用，也没有为故障注入添加分支。

部分成交输入表示一个 2 lot IOC 的两笔独立执行，分开发送累计订单、持仓和成交事实；这是冻结的合成协议输入，不声明真实交易所一定采用同样的中间消息和时序。

## 故障矩阵与实测结果

| 场景 | 必须成立的结果 | 本轮结果与证据 |
| --- | --- | --- |
| 部分成交、跨进程恢复 | 首笔 1 lot 被消费时，2 lot 订单仍有未成交量；第二笔推进到 2 lot；恢复及重放不能重复库存/费用 | 先持仓后成交；首笔后没有额外开仓。等待原 60 s 点，SIGKILL E、重启 A/N、固定引用恢复；代次 1→2。两笔原成交各重放三次。买卖两单各 `fill_count=2`，4 笔独立成交，费用 **0.01616002 USDT**，0 冲突。Stop 回执 **2.448 s**，连同 E 退出及 A 释放占用 **3.354 s**；[结果](partial/results.json)、[审计](partial/audit.json)、[终态](partial/terminal-proof.json) |
| bootstrap 期间重放 | 新 E 的 REST bootstrap 尚未完成时到达原成交，先入缓冲再 READY；原库存、费用不重复 | 在恢复 bootstrap 的账户 REST 响应返回前注入三次原生成交；持久化 fill 版本 **3**、READY 版本 **4**。随后再重放三次，库存仍为 1 lot。Stop 连同退出/释放 **3.447 s**，最终平仓；[注入证明](bootstrap-replay/bootstrap-proof.json)、[结果](bootstrap-replay/results.json)、[审计](bootstrap-replay/audit.json) |
| 行情断线重连后 Stop | 实际断开已建立 socket，发生新连接；可信账户事实下按原期限平仓 | 本轮 public 会话计数 **2→4**；Stop 平仓 REST 响应等待新行情连接建立。reduce-only / IOC 平仓，Stop 连同退出/释放 **3.550 s**；[连接证明](market-reconnect/fault-events.json)、[结果](market-reconnect/results.json)、[审计](market-reconnect/audit.json) |
| 私有账户流断线重连 | 新私有连接不能重新打开旧运行的失效门；没有安全证明不得释放占用 | private 会话 **1→2**，旧运行失效，Stop 失败回执 **1.967 s**；E 退出，A 最终 **FAILED**、`release_proof.safe=false`。仅原买入 1 笔，费用不变；模拟仓位保留 0.0001 BTC、模拟占用保留 2 条，`ACCOUNT_STREAM_GAP` 保留；[连接证明](private-disconnect/fault-events.json)、[终态](private-disconnect/manager-terminal-proof.json)、[审计](private-disconnect/audit.json) |
| 异常态 Stop | 已有持仓/累计订单但独立成交缺失，不能补造成交或盲平仓；Stop 在原引擎期限失败并保留占用 | 故意扣留独立 native fill，进入 **RECONCILING / 205**，平台库存证明关闭。重复 Stop 请求复用同一 Operation；引擎返回 **`stop_drain_deadline_exceeded`**。A 最终 FAILED、安全证明 false，0 笔独立成交/费用、模拟仓位 0.0001 BTC、占用 2 条；[异常前状态](abnormal-stop/abnormal-before-stop.json)、[超时回执](abnormal-stop/stop-timeout-proof.json)、[终态](abnormal-stop/manager-terminal-proof.json)、[审计](abnormal-stop/audit.json) |

前两项分别耗时 **75.825 / 76.918 s**；固定 SQLite BLOB 的 SHA 和 pin=1 均独立核对，A 管理库中的新旧运行到期值完全相同。全部场景保持 **handler 1 s、启动 30 s、恢复周期 60 s、Stop 10 s、运行总期限 120 s、100 USDT 风险上限及 50 bps 价格保护**。没有外部平仓兜底、提前写恢复点、清空库存屏障或延长运行期限。

异常 Stop 的 **引擎排空期限仍为 10 s**；经 A/N 调度、E 失败收尾和轮询，客户端实际收到失败回执为 **11.702 s**。该观测值不符合“客户端 10 s 内成功平仓”，也没有计作平仓通过。脚本的 15 s 观察等待只用于收集原超时失败回执，不修改 E 的固定期限；重复请求不新建停止操作或刷新期限。三个平仓正例均将 E 退出和 A 释放一起计入 10 s 验收窗口。

负例保留的是各自隔离模拟数据库中的两条占用，未人工删除。模拟器退出后不再产生订单，全部自有进程已退出。它们与实盘保留的两条占用没有共享目录或数据库。

## 持久化和完整性复核

[独立审计脚本](../../../../../scripts/verify_fill_fault_evidence.py)只读打开 SQLite，逐条校验归档 payload SHA、独立成交/费用、重放数量、冲突、固定引用/pin、管理库最终状态及原期限。部分成交的归档 fill 为非重放 4、跨进程 replay 2；bootstrap 为非重放 2、replay 1。重放保留来源记录，但不增加策略消费次数或财务汇总。

崩溃的旧来源缺口保留，新来源清洁排空；私有流断线的 `ACCOUNT_STREAM_GAP` 不因重新连上或 writer 清洁关闭而清除。`archive_sources` 的 clean/complete 表达本地录制边界，不能替代交易所连续性证明。全部风险最大值仍标为 `coverage_complete=false`；本轮持久化采样的最大实际名义额 16.16002 USDT、最大含在途上界 16.16012 USDT，不能推为连续市场最大值或生产性能 SLA。

模拟费用规则延续上一轮：名义额 × 0.0005，`1e-8 USDT / ROUND_HALF_UP`。单笔买入/卖出分别 404001 / 404000 单位；部分成交共 1616002 单位，其余两个平仓正例各 808001 单位。这不是实际交易所费率声明。

本轮在服务器执行脚本语法检查、原生参数包编译/签名、5 场景集成和独立审计。Rust/SDK/Web 未改，未重复其全量门禁；候选 05 既有 648 Rust、2 doctest、115 SDK 等属于[上一轮构建记录](../m4-ane/README.md)，不计为本轮重跑结果。

## 夹具修正、证据与清理

两次初始脚本失败均保留，且不计入通过：

1. [部分成交首轮](fixture-failures/partial-early-occupancy.log)在 E 已安全平仓后，立即读取尚未异步更新的 A 占用。改为在原 10 s 窗口内等待 A 的安全释放，再全流程重跑通过。
2. [异常 Stop 首轮](fixture-failures/abnormal-receipt-path.log)到错误目录寻找控制回执。实际 N 回执已包含 E 的超时失败；改为核对 A 返回的 `result.runtime` 原回执，再重跑通过。随后又补验两个负例，等到 A 最终 FAILED 后才退出管理服务。

没有为这些修正改动候选程序、策略或期限，见[夹具记录](fixture-failures/notes.json)。正例来自 `evidence/m4-fill-fault-05-02`，最终两个负例来自 `evidence/m4-fill-fault-05-04`，中间 `01/02/03` 原始现场仍在服务器，未覆盖。

[全部轮次清理](all-runs-cleanup.json)证明成功、预期失败及夹具失败各轮的自有 E/A/N 均退出，合成账户凭据已删除。双 lot fixture 的临时签名私钥和 DER 文件也已[清理](partial-artifact-cleanup.json)，公开签名包保留以供复现。仓库只收录白名单 JSON/日志，没有数据库、恢复私有 BLOB、签名私钥或账户凭据；[证据摘要清单](evidence-manifest.json)固定导出文件字节。

[文档核验](documentation-verification.json)记录服务器执行的相对链接检查、58 项导出证据摘要核对，以及本地/远端最终脚本、参数和文档的字节一致性。

## 复现与实施状态

入口为 [management_fill_recovery_smoke.py](../../../../../scripts/management_fill_recovery_smoke.py)，故障模拟与断言位于 [management_fill_fault_smoke.py](../../../../../scripts/management_fill_fault_smoke.py)。只在编译服务器、使用新的输出目录执行，例如：

```sh
/home/dominolu/dev/titan/.venv/bin/python scripts/management_fill_recovery_smoke.py \
  --repo /home/dominolu/dev/titan-persistence-m3-20261027-59973648 \
  --release evidence/m4-fill-release-05 \
  --expected-binary-sha256 38495ebcd5671901cc19ee07837c20fdaa3710a437e69d7b14b0cc17082f0b89 \
  --expected-strategy-sha256 dcc71c4a2594641a1d8368b5f248ffcede8b622230eee491f23e453dba616d8a \
  --candidate-label candidate05 --scenario bootstrap-replay \
  --output evidence/NEW-UNUSED-DIRECTORY
```

其他场景名为 `partial`、`market-reconnect`、`private-disconnect`、`abnormal-stop`。部分成交额外指定 `--strategy-source evidence/m4-fault-partial-artifact-01/strategy_version-source` 及表中的双 lot SHA。复现应从服务器仓库根执行；脚本核对服务器地址、发布摘要、签名及 loopback URL。独立审计可按 `matrix-audit.json` 的实际目录运行，`--private-case`、`--abnormal-case` 指向负例所在轮次。

P14 的这组非实盘故障补验已完成，见[实施状态](../../../../data_persistence_implementation_plan.md)。此前真实成交补验仍未通过且已停止；不据此创建真实 E 安全释放证明、不解除真实占用、不启动实盘或新 namespace。真实账户重连/恢复及有仓位 Stop、长时间重连、任意部分成交组合、异常事实补齐后自动恢复平仓和物理断电仍未验收。
