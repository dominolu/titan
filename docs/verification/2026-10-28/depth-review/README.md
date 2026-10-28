# 深度审查三项修复验收

2026-10-28。修复 [初轮审查](../depth/review.md) 的两项 P1、一项 P2；基线为本地提交 `4e911025` 加本轮修复。所有编译、格式化、静态检查和测试均在 `192.168.3.88` 的独立目录 `/home/dominolu/dev/titan-depth-review-20261028-4e911025` 执行；没有实盘账户访问或部署。

## 修复与证据

| 问题 | 修复 | 验证 |
| --- | --- | --- |
| E04 删空补回漏记 | 同一行情代次、窗口内已知删除且处于上一 Top100 可证明范围的价位，允许 `0→正数量` 进入 refill 跟踪；未知新价位和旧代次证据仍拒绝 | [原生 SDK 测试](sdk-test.log)：连续三次删空/10 ms 补回后真实触发 E04；范围外价位不误算，重同步清除删除证据 |
| 失效期间拒绝保护性减仓 | 共享 `validate_depth_commands`：深度有效按原执行流程；不可用时保留撤单，reduce-only 必须证明账户/品种/持仓侧、退出方向和可用数量，扣除活动订单及本回调的预占；待确认撤单不释放数量 | [Rust 单测](rust-test.log)：合法减仓、无持仓、错误方向、超量、同批叠加、撤单未确认与 NET/LONG/SHORT 身份 |
| 在线/离线准入分叉 | 两个宿主调用同一准入函数；离线违规后保持失败，禁止后续回调重新放行。在线原有激活门、执行绑定、数量/价格保护和意图耐久流程保留 | [真实原生六场景探针](depth-execution.log)：在线及回放分别覆盖未初始化、有效、失效、恢复；失效时撤单+合法减仓成功，开仓与超量减仓拒绝 |

执行探针使用合成持仓、内存 broker 和账户编号，不连接网络账户。它观察完整在线运行时是否实际派发 mock 撤单/减仓，以及同一签名能力与编译内容在离线适配器是否产生一致准入结果。

## 最终结果

- [全工作区 Clippy](clippy.log)：all-targets、all-features、`-D warnings` 通过；[Rust 格式检查](fmt.log)通过。
- [全工作区 Rust 测试](rust-test.log)：**666 passed、0 failed、24 ignored**；[doctest](doctest.log)：**2 passed、0 failed、1 ignored**。没有执行 ignored 网络或实盘测试。
- [SDK 全量](sdk-test.log)：**123 passed**，包括真实 AOT/原生策略回调。
- [原 48052 B 完整深度链路](depth-pipeline.log)、[CLI 回放正负例](trace-final/summary.json)复验通过。
- [最终策略清单](orderflow-manifest.json)：`orderflow_events 0.2.1`，恢复 schema **2**，状态 **62904 B**。布局未变化，编译策略身份仍必须重新核验；不隐式接受旧产物的恢复点。
- [622 文件摘要](source.sha256)与[逐项校验](source-check.log)证明本地修改与服务器验证输入字节一致。`.cargo/config.toml` 仅在隔离目录追加既有 rsproxy 缓存配置，不修改本地配置或 Cargo.lock，环境配置不计入源码清单。
- [工具链](toolchain.log)：Rust/Cargo 1.94.1、Python 3.11.14；[CLI 与 bundle 摘要](artifact-checksums.txt)。最终策略编译结果摘要为 `027e2a71799ef4aa828867c6f030e849a9b1ec42430123179f1f4d5db87c7c0b`。

复验使用全工作区 `cargo test --workspace --all-targets --all-features`、doctest、Clippy 和 SDK unittest discover。新增入口为 `v13_depth_execution_smoke`，输入 `python/titan-strategy-sdk/tests/fixtures/v13_depth_execution.py` 以空参数编译的 bundle；同一编译服务器环境、SDK 指向本轮源码目录。原深度探针和 `scripts/verify_depth_trace.py` 保持原用法。

本轮关闭上述三项审查问题；仍不承诺任意深度分片、公网长期运行或生产延迟/积压预算。默认 64 KiB 超限仍失败关闭并重同步，原价格保护、账户门禁及生产审批不因本次修复放宽。
