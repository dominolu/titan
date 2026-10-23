# M2 实盘只读验收

日期：2026-10-23。基线 `6952500e` 加 [源文件 SHA256 清单](source-sha256.txt)。M2 T10–T13 完成；M3 不在本次结论内。

## 构建与非实盘验证

所有构建、格式、静态检查和测试均在 `192.168.3.88` 的隔离目录 `/home/dominolu/dev/titan-management-20261023-5f424533` 执行。本地仅编辑和保存证据。工具链沿用 M1：Rust/Cargo 1.94.1、Node 22.22、pnpm 10.32.1、Python 3.11.14；PureAdmin 上游固定 `f0ff132561ab684bb78379239adf29f9a38ac7f1`。

- `cargo test --workspace --all-features`：547 passed、0 failed、13 ignored（包含 doctest）；见 [完整日志](checks/final-workspace-tests.log)。忽略的真实账户/环境测试没有统一开启。
- 受影响五个 Rust 包的 all-targets Clippy `-D warnings`、格式检查、二进制构建通过；见 [Clippy](checks/final-owner-clippy.log)、[构建](checks/final-owner-build.log)。ts-rs 对 `deny_unknown_fields` 的生成提示不影响 Rust 输入校验。
- 前端格式、类型、静态检查及生产构建通过；见 [类型](checks/final-web-types.log)、[静态检查](checks/final-web-lint.log)、[构建](checks/final-web-build.log)。
- [浏览器及协议结果](browser/results.json)、[页面结果](browser/browser.json)：使用 E 测试输出的合成账户事实，覆盖数值无改写、慢节点隔离、旧代次、只读能力、账户权限、历史、日志、诊断确认、过期/重连和只读用户。
- 最终配套二进制重新执行 M1 实际页面流程（真实 describe/build/prepare/backtest/report、失败与取消），[结果](checks/offline-ui.json)通过。十八组历史结果仍参见 M1；本次未把旧结果冒充新十八组回放。

编译服务器冻结目录 `verification-artifacts/management/m2/release-v4a`；发布前复制并 strip，再导出 DTO 和构建配套 Web：

| 产物 | SHA256 |
| --- | --- |
| `bin/titan` | `0d37ae6e8c8c2a0f54dbb8a81050face56ac102c1ef8cfb1a6f32dd42fa9a626` |
| `bin/v13_grid_history` | `300edb4b29d9fc7fbb2a9d13e314bb57437767eab82289082f9e042bb2009177` |
| `live-release-v4a.tar.gz`（titan、Web、只读验收脚本） | `e2ea638058a09d3183a87dbc1b03e797a20d989274ef80ed1505404306f974af` |

## 指定实盘节点只读验收

仅在 `43.130.234.54` 执行，使用以上编译服务器产物。用户确认 CrossEx gate（ID 1）、GATE_FUTURE_BTC_USDT、最大敞口 100 USDT。本轮观察期限 90 秒，交易控制在 A/N 双端关闭，不执行策略或交易写动作。

[脱敏结果](live/results.json)：全部通过，运行 91.676 秒。实际读取账户 ID 1 的余额、订单、持仓、账户流水及历史事实；直接 Pause 被拒绝。重启 A/N 后 worker ID、代次、PID/启动身份保持一致，Registry 仅一个 worker，最终正常 STOPPED。验收前后 BTC 均无挂单、无持仓。凭据、完整账户快照、SQLite 账本和配置保留在实盘机的私有目录，不进入仓库。

首次验收发现执行器在异步上下文析构 panic，见 [失败证据](live-failure/failure-postflight.json)、[诊断日志](live-failure/worker.log)。已修复为同一同步 owner 持有完整生命周期，并增加异步入口回归。第二次脚本遇到启动期空 observation，worker 本身正常停止；修复 readiness 的 null 处理后，第三次完整验收通过。失败轮次未进行交易写操作，未清除原证据。

## 范围与限制

E 留存独立成交、原始帧/单位、原币权益、费用、资金费和资金流；去重冲突、溢出、写盘失败、未清洁退出和来源缺口有明确诊断。费用摘要分批有界计算，未追平时返回缺失。业务快照使用已提交 checkpoint 的类型布局解码。

账户权益是采样值，账户流水与账户快照缺少共同水位，策略归属仍不足；输出明确不完整，不提供伪造策略净收益、连续最大回撤或无行情证明的名义风险上界。实盘轮次没有成交，真实成交及控制/停止/恢复属于 M3 验收。

运行及回滚步骤见 [运行说明](../../../../management_console_operations.md)。保留账本与 Registry，前后端配套部署；回滚页面不清空账户事实、不终止交易 worker。
