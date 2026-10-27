# 业务观测发布全面复核

状态：复核、修复、编译及全部非实盘验收通过（2026-10-27）。

## 范围与环境

- Git 基线：`638bae69324114e8a2447f648c8b7ced832196bb`，保留前两阶段尚未提交的改造及既有文档修改。
- 编译与非实盘验证：`192.168.3.88`，新隔离目录 `/home/dominolu/dev/titan-observation-audit-20261027-638bae69`。服务器原有目录没有覆盖；此目录是源码快照，不是 Git checkout。
- 本地只编辑、审查及核对同步字节。格式化、构建、静态检查、测试、类型生成与性能探针全部在指定服务器执行。
- 验证对象为真实 Rust 原生运行时、原始签名网格策略、旧恢复点和回环模拟交易所；没有接入真实账户或部署实盘。

## 发现及修复

| 问题 | 修复后的行为 |
| --- | --- |
| 手动恢复点提交失败时，只更新生命周期，旧业务观测仍可用 | 统一进入失败处理，撤销观测、永久关闭本运行的交易门，并结束未完成停止操作；其他手动冻结错误也走相同失败边界 |
| 执行结果入队失败，而回调持有状态锁时，失效可能延至下一回调 | 在尝试获取状态锁之前撤销观测和交易准入；冷读取也检查异步失败与执行通道失败 |
| 失效操作受队列拒绝，或与已经在执行的 Start 交错，交易门可能被迟到操作重开 | 交易门以同一原子状态表达关闭、打开和本代次终止失效。失效不依赖控制操作受理；Start、Resume、Stop 和回滚路径均不能重开终止失效的门 |
| 普通控制请求被队列拒绝后，操作记录永久 Pending | 立即结束为 Failed，使历史保留上限可以回收记录；健康策略不因普通控制请求拒绝而被误标失败 |
| 未启动、恢复就绪或未启动即停止，可能发布水位为 0 的快照 | 限制可发布生命周期，拒绝零成功回调水位；没有运行回调时保持业务观测不可用 |
| 成功停止并关闭执行通道后，迟到执行回执可能把终态改成失败 | 用运行级原子停止标志识别已排空边界；迟到回执不再撤销终态帧，停止完成前仍保留拒绝时失败关闭 |
| 快照接口只按策略运行编号匹配，页面以候选数据自身代次校验 | 接口同时匹配编号和查询运行的代次，页面以运行记录代次校验业务来源，拒绝跨代次混入 |
| 在帧标记写入中之后调用外部时钟，异常可能留下永久奇数版本 | 在进入原子帧写入区之前取时；时钟异常不会留下未完成帧 |

对应源码：[原子发布](../../../../crates/titan-strategy-runtime/src/observation.rs)、[运行时边界](../../../../crates/titan-strategy-runtime/src/runtime_v13.rs)、[交易门](../../../../crates/titan-strategy-runtime/src/runtime.rs)、[快照接口](../../../../crates/titan-cli/src/management/v2.rs)、[页面身份校验](../../../../web/console/src/views/console/run-view.ts)。

## 并发一致性和请求合并

发布方继续由运行时已有的回调所有权串行化。值、有效条件、回调水位、时间和帧版本全部使用顺序一致原子操作；读取前后相同的偶数版本才接受数据。版本达到上限时撤销观测，避免回绕。冷路径确认完整后，只格式化自己的本地副本。上述规则保证业务帧内部一致，不代表账户、生命周期和业务观测属于同一个全局事务。

请求认领的原子交换是合并边界：此前请求合并进本次采集，此后请求保留到下一成功边界。重复回调水位不认领请求，最终停止强制请求发布。读取持续冲突时最多尝试 8 次，随后明确报告忙，不阻塞写入或伪造进度。

新增测试覆盖认领后请求到达、无回调、持续忙读、写入中失效、取时异常，以及多个读取方下的值、有效条件和来源水位一致性。并发压力测试保留连续覆盖写入，并加入周期性稳定窗口，确保单核调度下也能接受实际新帧；它不把“忙”错误地当作一致性失败，也不允许全部断言只检查初始帧。本轮 3 个读取方和 19,998 次写入下记录 937 次成功读取，其中 3 次是初始可用性检查，其余 934 次逐项核对 64 个值、条件和来源时间。读取次数包含重复帧，不表示不同发布帧的数量。

## 缺陷复现与运行时边界

在同一服务器隔离目录临时换回上一轮原始源码，运行新增的八类网格边界用例，结果为 **0 passed、8 failed**；页面跨代次回归也失败。随后恢复修复源码再验证，未修改上一轮保存的目录。

- [原实现失败摘要](checks/before-fix.json)、[八类运行时复现日志](checks/before-fix-boundaries.log)、[页面复现日志](checks/before-fix-run-view.log)。
- [运行时回归源码](../../../../crates/titan-strategy-runtime/tests/observation_boundaries.rs)：未启动及正常停止、恢复提交与停止 flush 失败、普通控制拒绝、失效队列拒绝、执行结果拒绝且状态锁被占用、启动中失效受理、启动中失效拒绝、关闭通道后迟到执行回执。
- [发布并发及故障测试](checks/publication-tests.log)。测试计数与最终全套结果见下方验收记录。

## 恢复兼容和热路径

业务观测包络保持 v2，公开 `titan.grid/v1`、恢复结构 schema 3、原生 ABI V13 均保持不变。原始签名网格与原始恢复点直接加载，不重新签名或改写输入。验证继续核对状态字节、绑定及 checksum，未绑定或篡改输入仍被拒绝，恢复加载探针的交易门保持关闭。

无发布请求时仍只读取一个原子请求标志。有请求时最多发布 64 个值和 64 个有效条件，共 1 KiB；原子发布不增加堆分配、JSON、文件、checksum、网络、锁或重试。取时发生在帧写入前。新停止标志用于异步回执边界，终止交易门的 CAS 用于控制切换，未给每次观测空请求路径增加原子操作。

优化探针只测预热标量发布原语和受控时间值，不覆盖所有私有字段布局及缓存冷热，不包含完整策略回调、真实时钟、账户处理、调度或传输；其数值不代表端到端或生产延迟保证。

本轮[优化发布探针](checks/publication-cost.log)：

| 载荷 | 空请求每次调用均值 | 有请求 P50 | P99 | 堆分配 |
| --- | --- | --- | --- | --- |
| 8 个值及 8 个条件，128 B | 1.251 ns | 275 ns | 322 ns | 0 |
| 64 个值及 64 个条件，1 KiB | 1.768 ns | 2270 ns | 3305 ns | 0 |

## 最终验收

最终轮次：服务器 `evidence/final-04`。

| 验证 | 本轮结果 |
| --- | --- |
| Rust 格式及全工作区、全部目标、全部 feature 静态检查 | [fmt](checks/rust-format.log)、[Clippy `-D warnings`](checks/rust-clippy.log)通过 |
| Rust 测试 | [591 passed、21 ignored](checks/rust-tests.log)；[doctest 2 passed、1 ignored](checks/rust-doc-tests.log)。其中 8 项边界与 1 项旧文件探针另外显式执行；12 项真实交易所探针未执行 |
| 旧恢复文件与故障边界 | [1 项原始文件兼容及 8 项原生运行时回归](checks/recovery-compat.log)、[输入原文件 SHA256 未变化](checks/recovery-compat.json)通过 |
| 编译 | [CLI](checks/cli-build.log)、[优化历史引擎](checks/history-build.log)通过 |
| Python | [SDK 115 项](checks/sdk-tests.log)、[报告 5 项](checks/reporting-tests.log)通过 |
| 前端 | [DTO 生成](checks/dto-export.log)、[类型](checks/web-types.log)、[格式](checks/web-format.log)、[ESLint](checks/web-lint.log)、[构建](checks/web-build.log)通过 |
| 前端行为 | [缓存](checks/web-cache.log)、[运行视图](checks/run-view.log)、[策略视图](checks/strategy-view.log)通过 |
| 网格完整链路 | [26 项流程](checks/loopback/results.json)、[10 项浏览器检查](checks/loopback/browser/browser.json)通过，模拟订单最终为 0 |

[本轮汇总](checks/verification-summary.json)和[进程收尾](checks/final-cleanup.json)确认模拟服务均退出、服务端口均关闭。真实交易所探针没有执行。

## 网格实时更新与恢复

[实时验证](checks/loopback/realtime-observation.json)保持恢复周期为 60 秒，连续等待三次带有新行情处理时间的管理器快照：

| 发布序号 | 成功回调水位 | 等待下一条新业务值 |
| --- | --- | --- |
| 6 | 38 | 1.101 秒 |
| 9 | 69 | 1.161 秒 |
| 11 | 89 | 1.107 秒 |

期间恢复文件逐字节相同，前后 SHA256 均为 `d6e3834711935806e84726d6fad8403c3ca7f94bd54e830b3d93e08ebcfef0b5`。[SSE](checks/loopback/business-observation-sse.json)也接收到后续发布；这些等待时间是本次回环采样实测，不是逐 tick 延迟。

[实际业务快照](checks/loopback/business-observation.json)、[公开 schema](checks/loopback/business-observation-schema.json)、[恢复后新代次观测](checks/loopback/restored-observation.json)、[页面截图](checks/loopback/browser/business-observation.png)均来自本轮真实宿主链路。公开 schema hash 保持 `8097dfc757165adf6c6f267c2c3506ff50d0d357316541ea8f5a6b9fbca55428`。平台恢复继续经过未决订单只读补查、错误历史订单身份拒绝、原风险期限保持、新代次持续发布与停止释放；没有把观测帧当作恢复状态恢复。

## 交付身份与复现

[构建 SHA256](checks/build-sha256.txt)：

```text
titan = afa29604b3e2456c2bf27abe673a8eeea3c8805edf421d2d144ec5d45a23fb2a
v13_grid_history = d02f1114f794b22744647263edcc1955a92965abc4f89d208b803deefd706436
```

CLI 为服务器 debug 构建，历史引擎启用优化，发布探针单独使用 release 构建。服务器导出的页面 `EXPECTED_BUILD` 等于本轮 CLI SHA256。使用既有锁文件与缓存依赖，没有新增第三方依赖。

[最终 677 文件清单](checks/source-sha256.txt)、[服务器源码身份](checks/source-identity.json)、[本地逐字节核对](checks/local-source-identity.json)、[本轮差异清单](checks/audit-delta.json)及[具体差异](checks/audit-source.diff)绑定源码和本轮证据。源码清单 SHA256 为 `0fb71f5a8d733af38f3309c13dfdeb1b8e7ba469c24c89183b067bc9201bfcc3`，本地不匹配文件数为 0。该清单不包含验证证据本身、依赖及服务器镜像配置。工具链见[记录](checks/toolchain.txt)，每项门禁的精确命令保存为 `checks/*.command.txt`。

整理 Git 提交时，独立的持久化设计、实施计划、目录链接及工程记忆中的后续 SQLite 恢复迁移规划留在工作区。[提交范围记录](checks/commit-scope.json)说明拆分边界：运行源码与最终验证版本相同，工程记忆仅提交命名与观测相关内容。上述 677 文件清单及原始差异清单保留验证当时的完整工作区身份，不等同于本次 Git 文件清单。`audit-source.diff` 是剔除无关规划后的提交范围视图；未筛选原始差异仍保存在服务器 `evidence/final-04/audit-source.diff`，其 SHA256 另行记录。本次整理未修改运行代码，也未重新执行构建或测试。

复现命令：

```bash
export CARGO_TARGET_DIR=/tmp/titan-deplugin-target
export CARGO_BUILD_JOBS=8
export TITAN_BROWSER_EXECUTABLE=/home/dominolu/.cache/ms-playwright/chromium-1248/chrome-linux64/chrome
export TITAN_PREVIOUS_OBSERVATION_FIXTURE=/home/dominolu/dev/titan-observation-20261027-638bae69/evidence/final-09/loopback
bash scripts/verify_strategy_observation.sh evidence/new-verification
```

详细约束与数据流见[业务观测协议](../../../strategy_observation_protocol.md)。当前数据持久化设计中的 SQLite 迁移仍是独立待实施计划，本轮未将其描述为现有能力。
