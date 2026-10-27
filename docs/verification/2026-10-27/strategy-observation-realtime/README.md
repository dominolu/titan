# 业务观测独立发布与恢复兼容验证

状态：实现、编译和全部非实盘验收通过（2026-10-27）。

## 版本与环境

- Git 基线：`638bae69324114e8a2447f648c8b7ced832196bb`，继承前一阶段尚未提交的策略命名和协议改造。
- 前一阶段修改源码清单 SHA256：`4319e8208a49e30accc551773634433103f2e0a76d8bf9872dfd0263539a7e3c`。旧结果不作为本轮通过证明。
- 编译服务器：`192.168.3.88`；本轮新隔离目录：`/home/dominolu/dev/titan-observation-realtime-20261027-638bae69`。既有目录未覆盖，新目录是源码快照，未假定为 Git checkout。
- 本地仅编辑、审查和核对同步字节。编译、格式化、类型生成、静态检查、测试和性能探针全部在编译服务器执行。
- 使用原 Cargo.lock、pnpm-lock.yaml 和服务器既有依赖；未新增第三方依赖。
- 本轮不访问真实账户、不连接真实交易所、不部署实盘；链路验收使用真实 Rust 运行宿主及回环模拟交易所。

## 最终行为

观测从成功回调边界发布到该运行的预分配原子标量帧，宿主冷读取后再格式化。观测不读恢复文件、不调用恢复提交或 flush，也不写入恢复点。请求合并，无请求时只检查一个原子标志；有请求时最多复制 64 个值及其有效条件，共 1 KiB 原始载荷。

所有载荷字和帧版本使用顺序一致原子操作。冷读取仅接受前后版本相同的完整帧，最多尝试 8 次；冲突持续时报告忙并在下一次采样重试。写方没有锁、重试、堆分配、JSON、SHA256、文件或网络操作；暂停读取不能阻塞写方或丢失最终 Stop 发布。失败回调使当前运行的观测不可用。

公开 `titan.grid/v1` schema 和 hash 保持不变，业务包络单独升级到 v2。source 使用 `runtime_publication`、发布序号、成功回调水位、实际采集时间及运行代次。它不表示持久化完成，不能作为恢复或交易准入证据。页面仍兼容 v1 历史包络并明确标记来源。

恢复结构 schema 3、原生 ABI V13、签名、绑定、checksum、指令持久化和停止屏障保持原有校验语义。恢复后的观测以新运行代次重新发布，不恢复旧观测帧。完整协议及数据流见[实现说明](../../../strategy_observation_protocol.md)。

## 网格实时链路

[实际观测数据](checks/loopback/business-observation.json)、[SSE 更新](checks/loopback/business-observation-sse.json)、[恢复后快照](checks/loopback/restored-observation.json)和[页面截图](checks/loopback/browser/business-observation.png)均来自本轮真实运行宿主。

[实时验证](checks/loopback/realtime-observation.json)保持恢复周期为 **60 秒**，连续等待三条带有新网格行情处理时间的管理器快照：

| 发布序号 | 成功回调水位 | 等待下一条新业务值的时间 |
| --- | --- | --- |
| 8 | 57 | 1.129 秒 |
| 10 | 78 | 1.166 秒 |
| 13 | 109 | 1.181 秒 |

期间恢复文件字节完全相同，前后 SHA256 均为：

```text
2e9fe5880e503c706216a0dfbd8275ae65638a46f9177dc72c423c071f3dedf1
```

SSE 也收到更高发布序号的更新，恢复文件仍未变化。这证明更新来自独立观测发布，既没有等待恢复周期，也没有通过缩短周期或额外保存恢复点完成更新。上述时间是本次回环环境等待新管理器快照的实测值，不是逐 tick 端到端延迟或生产 SLA。

## 热路径验证

[优化构建微基准](checks/publication-cost.log)与零分配检查：

| 原始载荷 | 有发布请求 P50 | P99 | 堆分配 |
| --- | --- | --- | --- |
| 8 个值 + 8 个条件，128 B | 128 ns | 137 ns | 0 |
| 64 个值 + 64 个条件，1 KiB | 885 ns | 1346 ns | 0 |

微基准只测预热原始标量发布原语，使用受控时间值，不包含完整策略回调、真实时钟、账户服务、JSON、网络或调度成本，也不提供延迟保证。常态无发布请求的分支只做原子标志检查。

并发回归验证多个读取方不接受跨帧字段；暂停读取方时写方仍完成最终发布；请求合并、重复读取不推进序号、失败失效及精确大整数均有覆盖。

## 恢复兼容与门禁

| 验证 | 本轮结果与证据 |
| --- | --- |
| Rust 格式与静态检查 | [cargo fmt](checks/rust-format.log)、[全工作区/全部目标/全部 feature 的 Clippy `-D warnings`](checks/rust-clippy.log)通过 |
| Rust 测试 | [585 passed、13 ignored](checks/rust-tests.log)；[doctest 2 passed、1 ignored](checks/rust-doc-tests.log)。其中需要旧文件的兼容探针另外显式执行通过；真实交易所探针未执行 |
| 优化发布探针 | [发布成本及零分配](checks/publication-cost.log)通过 |
| 原始旧文件兼容 | [执行日志](checks/recovery-compat.log)、[输入 SHA256 与原文件未变证明](checks/recovery-compat.json)通过 |
| 构建 | [CLI](checks/cli-build.log)、[优化历史引擎](checks/history-build.log)通过 |
| Python | [SDK 115 项](checks/sdk-tests.log)、[报告 5 项](checks/reporting-tests.log)通过 |
| 前端 | [DTO 导出](checks/dto-export.log)、[类型](checks/web-types.log)、[格式](checks/web-format.log)、[ESLint](checks/web-lint.log)、[构建](checks/web-build.log)通过 |
| 前端行为 | [缓存](checks/web-cache.log)、[v1/v2 协议、精度和代次](checks/run-view.log)、[Vue/Pinia 回归](checks/strategy-view.log)通过 |
| 网格完整链路 | [26 项流程](checks/loopback/results.json)、[10 项浏览器检查](checks/loopback/browser/browser.json)通过，含实时更新、SSE、控制确认丢失、管理器/节点重启、崩溃、错误恢复身份拒绝、原期限恢复及停止释放占用 |

兼容探针直接读取前一阶段保存的原始签名策略和原始恢复点，用新代码完成原生库加载、恢复身份校验及状态字节还原；状态字节相同，ABI 为 13、恢复 schema 为 3，交易门保持关闭。未绑定实例及被篡改状态仍拒绝。原始文件没有改写，也没有重新签名或生成“兼容”文件。

本轮平台恢复还实际经过模拟交易所的未决订单只读补查，保持原风险期限，随后确认新代次持续发布新业务数据。它与旧文件原生加载探针分别验证平台恢复流程和旧字节兼容，不宣称已验证真实账户恢复。

## 复现与交付身份

在指定服务器、本轮隔离源码目录执行：

```bash
export CARGO_TARGET_DIR=/tmp/titan-deplugin-target
export CARGO_BUILD_JOBS=8
export TITAN_BROWSER_EXECUTABLE=/home/dominolu/.cache/ms-playwright/chromium-1248/chrome-linux64/chrome
export TITAN_PREVIOUS_OBSERVATION_FIXTURE=/home/dominolu/dev/titan-observation-20261027-638bae69/evidence/final-09/loopback
bash scripts/verify_strategy_observation.sh evidence/new-verification
```

各门禁的精确命令保存在 `checks/*.command.txt`。[工具链](checks/toolchain.txt)及[构建 SHA256](checks/build-sha256.txt)：

```text
titan = 1037f026bc48f08499e3b917f1ccc28f7b1bb7591e1206d7948ffed80edf6e70
v13_grid_history = 85c8caf609aea4d817a918f67dc31b4c3713b4942b21fdce40420f7f4754c5b1
```

CLI 为本轮非实盘验证的 debug 构建；发布微基准单独使用优化构建。前端 `EXPECTED_BUILD` 匹配本轮 CLI。未使用本地构建结果。

[最终源码清单](checks/source-sha256.txt)、[本轮相对前一阶段的修改](checks/realtime-delta.json)、[服务器源码身份](checks/source-identity.json)、[本地字节核对](checks/local-source-identity.json)及[进程收尾](checks/final-cleanup.json)绑定本轮交付和证据。历史报告不替代本轮结果。
