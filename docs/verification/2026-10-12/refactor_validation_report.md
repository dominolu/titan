# 系统重构验证报告

日期：2026-10-12。基线：`741d2de56f652855a3e5848e523f312b014dc15f`。变更范围及逐项记录见 [系统重构清单](../../system_refactor_checklist.md)。

全部构建、格式检查、静态检查、测试和 AOT 生成均在编译服务器 `192.168.3.88` 执行。验证目录为 `/home/dominolu/dev/titan-refactor-20261012-741d2de5`，原有服务器工作区未覆盖。工具链为 Cargo/Rust 1.94.1、Python 3.11.14。

最终源码覆盖 275 个源文件及配置文件，逐文件摘要见 [source-files.sha256](source-files.sha256)。该清单自身 SHA-256：`9e99911795ea02218852fb96f83b4d4cb84999b8d730f0ff7ea326873abc23d4`。Cargo.lock 与两个最终 `.titan` 制品摘要见 [artifact-checksums.txt](logs/artifact-checksums.txt)。本轮验证未部署、未连接真实账户；代码提交信息以 Git 历史为准。

## 验证结果

| 检查 | 结果 | 原始证据 |
| --- | --- | --- |
| `cargo fmt --all -- --check` | 通过 | 服务器 `final/fmt.log`，空输出、退出码 0 |
| `cargo check --workspace --all-targets --all-features` | 通过，无编译 warning | [check.log](logs/check.log) |
| `cargo clippy --workspace --all-targets --all-features` | 通过，保留非阻断 lint 提示 | [clippy.log](logs/clippy.log) |
| Rust workspace 全目标、全 feature | 427 通过，0 失败，12 ignored | [rust-test.log](logs/rust-test.log) |
| Rust doctest | 2 通过，0 失败，1 ignored | [rust-doc-test.log](logs/rust-doc-test.log) |
| Python Strategy SDK | 27 通过 | [sdk-test.log](logs/sdk-test.log) |
| Python reporting | 4 通过 | [reporting-test.log](logs/reporting-test.log) |
| 单一 Runtime/ABI 检查 | 通过 | 服务器 `final/single-runtime.log`，退出码 0 |
| SDK 示例及 pair_arb AOT 编译、签名制品测试 | 通过 | SDK 日志及服务器 `final/*-build.log` |
| 原生 ABI 调用及 successor generation 恢复 | 通过 | 服务器 `final/artifact-smoke.log` |
| pair_arb 原生 Maker → Fill → Hedge | 通过 | 服务器 `final/pair-smoke.log` |
| Service → Factory → Instance 完整重启 | 通过 | [recovery-smoke.log](logs/recovery-smoke.log) |
| REST 终态 → Stop → Order → Fill | 通过 | [drain-smoke.log](logs/drain-smoke.log) |
| 最终 checkpoint 确认失败 | 正确进入 Failed，停止操作失败 | [drain-smoke.log](logs/drain-smoke.log) |
| executor 饱和且结果控制队列满 | 正确失败，未出现同步拒绝路径死锁 | [drain-smoke.log](logs/drain-smoke.log) |
| CLI run → ResultBundle → native/QuantStats 报告 | 通过 | [cli-integration.log](logs/cli-integration.log) |
| 损坏/未提交 Bundle 拒绝、非实盘 SIGTERM | 通过 | [cli-integration.log](logs/cli-integration.log) |

427 项 Rust 测试和 31 项 Python 测试是完整仓库测试结果，包含原有测试，不是本轮新增测试数量。12 项 Rust ignored 测试会接入真实账户，按执行环境约定未启用。

Clippy 仍提示既有 API 形状和代码风格问题，例如 `result_large_err`、`too_many_arguments`、`should_implement_trait`；本轮新增的同步拒绝死锁、发布前缓存提交、迟到 READY、候选取消旧订单等行为问题已经修复。此结果表示命令通过，不表示仓库通过 `clippy -D warnings`。

## 关键回归证据

账户测试验证快照版本与内容同锁读取、并发读取一致、稀疏字段保留、过期暂存事实过滤、暂存容量上限、遗漏终态 REST 对账、候选关闭 publisher 时可完成准备、旧会话 READY 不能恢复新会话准入，以及未提交候选停机不执行取消旧订单策略。辅助 ID 表跟随活动及保留终态集合回收。

事件测试覆盖复制与 reservation 的统一准入、拒绝后不投递、序号去重、失败重试、停机 reservation lease 释放及同 source 递归发布拒绝。已有并发发布、PRIMARY 隔离、Latest、可靠重试和 SnapshotBarrier 回归均通过。

策略测试覆盖 Taker-Taker 两腿任意成交顺序、不可收敛风险缺口通过现有 ABI 错误码失败、私有回调错误不再被进程存活掩盖、重复全局停机、checkpoint 确认，以及本地命令全批校验和登记后再执行。

采集器测试使用容量 1 的队列验证排空、跨日轮转、同日追加 gzip member、压缩文件可读性及写盘失败返回；压缩写盘位于独立 blocking worker，异步接收路径采用有界队列和背压。

## 性能样本

全量测试中的事件引擎基准处理 1,000,000 个事件：约 825,558 events/s，dispatch p99 为 4,095 ns，p999 为 32,767 ns；drop、resync、arena exhaustion 和 FastLane drop 均为 0。该样本是编译服务器的 debug profile 回归观测，不是生产延迟 SLA，也没有用于宣称性能提升。

## 使用边界

本轮没有启用实盘策略、下单、撤单或修改账户交易模式。账户采用单资产净持仓投影，检测到多 position side 会拒绝 bootstrap。恢复保留现有严格的 public-state、artifact、binding 和 checksum 校验，不会自动迁移不一致的历史私有状态。

复验入口为 [verify_system_refactor.sh](../../../scripts/verify_system_refactor.sh)，CLI 集成场景为 [verify_cli_bundle.py](../../../scripts/verify_cli_bundle.py)。脚本仅允许在指定编译服务器运行。
