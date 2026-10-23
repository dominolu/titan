# M1/M2/M3 复核修复

复核日期：2026-10-23；修复验证：2026-10-23～24。审查基线为 `dd555c3b28e0e954f8d9dff2934c7fbbfba28b7e`，八项问题见 [修复前复现记录](before/review-results.json)。本次修复源码以 [SHA256 清单](source-sha256.txt) 固定；全部编译、格式、静态检查、代码生成和非实盘测试在 `192.168.3.88` 的独立目录 `titan-console-fixes-20261023-dd555c3b` 执行。本地仅编辑、审查和保存证据。

## 修复范围

| 问题 | 修复及责任边界 | 新回归 |
| --- | --- | --- |
| 成交后、持仓投影前提前释放风险 | E 从一致种子维护逐持仓方向预期库存；独立成交推进累计量，持仓确认前拒绝新增开仓；保护性退出仍可用，checkpoint、停止及安全释放保留屏障 | 原生库存/准入测试、停止与恢复探针 |
| E 控制队列满后 Stop 永久挂起 | 只缓存成功入队的 Stop；失败受理可重试，重复成功受理保持同一操作和原期限 | `v13_stop_queue_smoke` 实际填满控制队列后重试成功 |
| A 普通操作占满容量后拒绝 Stop | 普通操作和 Stop 分别保留有界受理预算；Stop 优先进入核对列表 | 256 个 unknown 普通操作后 Stop 仍成功 |
| catalog 故障阻断控制 | 目录、普通操作、新 Stop 和已有 Stop 回执分别推进，不共享故障前置条件 | catalog 503 时 Stop succeeded，节点仅接收一次 POST |
| 死任务永久占住进程槽位 | N 核对 PID/启动时间释放已死亡任务的计算槽位；A 不用 unknown 作为存活证明；原操作与实盘占用继续保留 | 死任务后新任务成功；原操作重试仍为 unknown，未重派 |
| 并行健康 writer 产生永久缺口 | 记录源持有 OS 文件锁至最终 clean marker；仅未持锁且未干净退出的来源产生按账户限定的缺口 | 并行 writer、崩溃来源范围和干净重启回归 |
| Web 历史活动淘汰模板、绑定及选择 | 目录、活动对象、各类历史和当前选择分别限额；缺失选中详情从有权限的资源接口取回 | 600 次资源更新、类型限额及旧 revision 回归 |
| M2 金额及最大敞口一直缺失 | E 负责精确线性计价、合并在途意图及成交预留；与交易准入共用 tick/lot 算式；持久化逐品种原币采样最大值 | 去重/成交投影缺口/过期价格/缺单位/溢出/大整数重启及实际 E 金额输出 |
| 验收新增：非终态 checkpoint 的订单缺少跨进程查询 | E 经 AccountService/connector 只读补查未决订单，核对品种及 client ID，再交给原有恢复对账；原启动期限内最多查询 128 笔 | 固定保留撤单中 checkpoint；错误历史身份拒绝后，正确权威终态恢复成功 |
| 验收新增：未激活的失败候选占用阻断原运行重试 | 只向已确认代次转移 owner；E 证明从未尝试激活时，原子恢复 predecessor 的重试资格，保留账户锁；旧候选的迟到回执不能覆盖新 owner | SQLite 交错与既有错误映射修复测试；错误历史身份拒绝后重新恢复 |

账户快照读取前后比较 E 意图修订及库存证据。期间变化时保留不完整标记，避免把两个独立投影的组合当成一致事实。金额计算、风控及恢复均留在 E；A/N/U 没有新增交易账本或另一套估值规则。

最终验收曾在 `recovery_order_result_missing` 安全拒绝，见 [失败记录](before/recovery-missing-result.log)：旧进程 checkpoint 停留在撤单中，新进程 bootstrap 只有当前活动订单，无法取得已撤订单的终态。修复通过权威只读查询补齐证据，不放宽原有身份、累计成交、持仓或 checkpoint 校验；未知、超时和成交变化仍拒绝恢复。测试保留真正的非终态 checkpoint，避免依赖 sleep 恰好等到新快照而掩盖该缺口。

负向身份测试随后暴露了失败候选占用的归属问题，见 [失败记录](before/failed-recovery-occupancy.log)。修复保留账户及订单命名空间锁，同时允许已证明从未激活的候选返回原运行重试；不会把未知结果当作未激活。新增计价配置也不改变旧成交的单位去重指纹，重复成交仍只计一次；真正的 tick/lot 变化继续报告冲突。完整性缺失原因取自实际风险证据，不保留与当前估值矛盾的固定提示。

## 验证证据

- Rust 全工作区及 doctest：**565 passed、0 failed、13 ignored**，见 [完整日志](checks/workspace-tests-final.log)。包括旧的不确定回执不能覆盖较新受理前拒绝、未激活候选保留账户锁并可重试的交错测试。忽略的真实账户/环境测试没有统一开启。
- 四个受影响包 all-targets/all-features Clippy `--no-deps -- -D warnings`、格式及正式 CLI 构建通过，见 [Clippy](checks/clippy-affected-final.log)、[格式](checks/fmt-final.log)、[构建](checks/build-final.log)。未声明全工作区 Clippy 无警告：[既有 hftbacktest 依赖诊断](before/dependency-clippy.log)仍有两处 `needless_return`；ts-rs 的 Serde 属性解析提示保持原状。
- Python SDK **113 项通过**，见 [日志](checks/sdk-tests-final.log)；实际原生 Stop 队列和网格停止/恢复探针见 [队列](checks/stop-queue-final.log)、[网格](checks/grid-probe-final.log)。
- 工具链见 [版本记录](checks/toolchain.txt)，原生探针使用的固定服务器制品见 [摘要](checks/native-fixture-sha256.txt)。
- 真实 A/N 故障注入见 [结果](checks/control-faults.json)。Web 类型、格式、静态检查、缓存压力测试和生产构建见 [类型/格式](checks/frontend-check-final.log)、[静态检查](checks/frontend-lint.log)、[缓存](checks/web-cache.log)、[构建](checks/frontend-build-final.log)。
- M3 实际 A/N/E、签名制品和模拟交易所 **21 项通过**，见 [结果](checks/control-results.json)、[金额敞口](checks/risk-evidence.json)、[页面](browser/control/browser.json)。覆盖错误历史身份拒绝、原运行重试、原期限恢复及停止释放，未残留委托。
- 最终配套程序重新通过 M2 [协议/权限/失联测试](checks/observation-results.json)及[页面](browser/observation/browser.json)，以及 M1 [离线工作流](checks/offline-results.json)、[故障注入](checks/offline-faults-results.json)及[页面](browser/offline/browser.json)。页面异常均为零。M1 复用已校验的原始历史数据及缓存，重新执行页面冻结、编译、回放、报告等流程；本次不声称重新执行十八组历史矩阵。

编译机曾测得单次 checkpoint 持久化 1.13 秒，超过原夹具的一秒 handler 预算，E 按规则失效关闭，见 [慢盘记录](before/slow-disk-handler.log)。非实盘夹具按实测改为五秒 handler，保留 fsync、五秒意图 flush、两秒行情新鲜度及十秒停止期限。故障脚本启动/传输预算与既有测试分别对齐为 40/15 秒，不通过去掉持久化或放宽交易限额取得通过。

## 指定实盘账户复验

仅在 `43.130.234.54` 使用编译服务器产物执行。沿用用户授权的 CrossEx gate（ID 1）、`GATE_FUTURE_BTC_USDT`、最大敞口 **100 USDT**，价格保护 **50 bps**，原总期限 **120 秒**，单笔最小数量 **0.0001 BTC**，停止方式 flatten。实盘机没有执行构建。

[成功结果](live/results.json)：**20.829 秒**完成签名部署、最小挂单、E 金额敞口、暂停撤单、A/N 重启、恢复运行、确认零仓位后的受控崩溃、原期限恢复、真实历史订单补查及停止。测试显式保留非终态 checkpoint，恢复从代次 1 到 2，未补充原运行预算。

[持久化审计](live/audit-summary.json)：四笔最小委托、12 条订单事实，累计成交及独立成交均为零；最大实际敞口为 0，最大含在途上界采样为 **16.3664 USDT**。账户占用和 Titan 活动进程均为零；[后置检查](live/postflight.json)确认 BTC 挂单 0、持仓 0。无真实成交，本轮不声称真实成交后的平仓或完整策略收益已经验收；相关成交竞态由原生及模拟测试覆盖。

首次实盘轮次在新账户 bootstrap 时未通过私有流连续性检查，策略恢复尚未进入，见 [失败关闭摘要](live-failure/failure-summary.json)。E 证明候选从未激活、结果非 unknown，A 保留原运行的占用归属；[当轮后置检查](live-failure/postflight.json)同样为零挂单、零持仓。完整清理后使用相同产物重新执行独立的有限验收，通过上面的成功轮次；未跳过账户就绪或恢复校验。凭据、账户余额、完整快照及账本留在实盘机私有目录。

## 配套发布与限制

先冻结并 strip 服务器构建的程序，再从该程序导出 DTO、构建配套 Web；发布物摘要见 [清单](release-sha256.txt)。不能单独替换程序而沿用不匹配的前端。

| 产物 | SHA256 |
| --- | --- |
| `bin/titan` | `fee02111897d57b7919655d654b22558fd4788890afd9f517254d37d362c043e` |
| `bin/v13_grid_history` | `1ce9e761426844f2cfb235518eec0c62b1f22af449ebc9867264af81d57b0acd` |
| 签名实盘网格 bundle | `6e2541eea72505c60f1b9d1083f73ed71787261b243170a4243ac65e91dfc8eb` |
| 编译服务器发布包 | `cbb94072aa661f1a870e799b36a6df6a26bef7f650bed399844532cc20c0c718` |

部署端再次核对程序和制品摘要，见 [部署身份](live/deployment-sha256.txt)。服务器冻结目录为 `verification-artifacts/review-fixes/release-accepted`；发布包不含签名私钥或账户凭据。

账户事实库升级到 `user_version=2`。历史缺口不被自动抹除；旧程序不能原地读取 v2 库。线性计价须显式配置 `linear_quote_currency`，并具备匹配绑定、最多两秒前的 BBO；缺失时保留空值和原因。最大敞口来自约 500ms 采样，不宣称连续行情最大值、完整历史覆盖或完整策略净收益。配置、备份及回滚见 [运行说明](../../../../management_console_operations.md)。
