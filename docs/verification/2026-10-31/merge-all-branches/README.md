# 全部分支合并到 master：检查与验证记录

日期：2026-10-31。初始 master / origin/master：`5808b4b1`。

## 分支检查

执行 `git fetch --all --prune`，检查全部本地及 origin 分支和两个 worktree。

- 已在 master：`codex/depth-top100-recovery`、`codex/fix-issue-5-tls-provider`、`codex/gate-crossex-broker`、`codex/hyperliquid-live-acceptance`、`codex/management-console`、`codex/openctp-connector-design`、`codex/single-file-strategies`，包括各自远端引用。
- 待合入：`codex/evm-solana-connectors` 和 `dex-connectos`，本地及远端均指向 `71324f27de9ea5b994c96f5d1f4c2957695033cc`，只需合并一次。
- 待合入的四个提交：`2a907f36`（Rust 工具链）、`0795d614`（EVM）、`40b733c9`（Solana）、`71324f27`（接入说明）。
- 另一个 TLS 修复 worktree 干净。主 worktree 原有未提交的策略、研究及文档修改保留在工作区，不纳入合并提交。

## 冲突处理与兼容边界

旧分支基于已删除的动态插件架构。合并保留 master 静态接入方式，在 `market_runtime` / `account_runtime` 目录注册两类工厂；不恢复动态插件引擎、插件 crate 或已删除的旧实盘 probe。

两份 Cargo.lock 在编译服务器协调：同一 semver 兼容版本优先沿用 master 原有 pin，新增链上依赖沿用分支 pin，再由 Cargo 校验和规范化；不以直接更新全部依赖解决冲突。

新接入遵守 owned shutdown future、NET 账户事件、统一 TLS provider、发送前 fence 及未知交易保留规则。旧实现存在确认超时伪造撤销、按请求数量伪造成交、跨币种账户金额混加等边界问题。因此保留行情和原生 RPC/签名构造工具，统一账户激活、下单、成交查询及账户总额明确返回 Unsupported，等待真实成交、费用、spot 库存与共同币种估值适配。此合并不表示链上统一账户交易或实盘已验收。

## 验证

编译服务器：`192.168.3.88`。使用新建隔离目录 `/tmp/titan-merge-all-20261031-nH6qpB`，未覆盖服务器现有源码。该服务器原 `/home/dominolu/dev/titan` 无 Git 元数据，因此不将它作为本次版本身份；本次身份由本地基线、合入提交及暂存树确定。

最终验证全部通过，汇总见 [summary.json](summary.json)，命令见 [verify.sh](verify.sh)：

| 检查 | 结果 |
| --- | --- |
| `cargo fmt --all -- --check` | 通过 |
| `cargo check --offline --locked --workspace --all-targets --all-features` | 通过 |
| `cargo clippy --offline --locked --workspace --all-targets --all-features -- -D warnings` | 零警告通过 |
| `cargo test --offline --locked --workspace --all-targets --all-features` | 788 passed、0 failed、26 ignored |
| `cargo test --offline --locked --workspace --all-features --doc` | 2 passed、0 failed、1 ignored |

工具链为 Rust / Cargo 1.94.1，Linux x86_64。测试使用默认并发；ignored 项保持忽略，未以实盘或外部服务验证替代本次非实盘门禁。

验证源码暂存树为 `06b157a9a5ef44bc9ad15f3c6492e38908929ba9`。验证结束后将远端文件逐字节与该暂存树的 archive 核对，2576 个文件一致；[SHA256 清单](source-sha256.json) 排除本报告目录，因为日志和最终报告在验证后补入，不影响运行代码。服务器格式化结果已同步回本地并暂存。

初次 Clippy 子进程未继承命令行镜像配置，找不到缓存依赖。后将 [cargo-mirror.toml](cargo-mirror.toml) 放入隔离目录父级 `.cargo/config.toml`，使子进程继承相同 registry。随后修复新增链上代码的 13 项 Clippy 告警（条件简化、类型别名及文档缩进），再完成以上全部门禁。`status.tsv` 保留重试历史，`summary.json` 记录每项最终退出码；日志为最终成功轮次。

完整日志：[check](check.log)、[Clippy](clippy.log)、[Rust tests](rust.log)、[doctest](doctest.log)。

未连接实盘账户，未运行链上交易 probe，未部署。
