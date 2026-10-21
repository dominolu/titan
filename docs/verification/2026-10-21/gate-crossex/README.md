# Gate CrossEx broker 验证

后续状态：四项 P1 与原项目接口缺口已进入补齐版本；此文件保留当时结论及原始证据。当前状态和新测试结果见[补齐验证记录](../gate-crossex-complete/README.md)。


日期：2026-10-21。实现说明：[分析、能力与接入](../../../gate_crossex_broker.md)。

后续[完整性复审](../gate-crossex-review/README.md)发现四项 P1 问题，并以新增服务器用例复现。下方通过数量保留为当次覆盖结果，不代表完整实现或当前已可进行私有实盘验收。

## 版本与环境

- 本地基线：`39f1267b6412165fc7adc005719bc9e09dcef9a8`，包含任务开始时已有的未提交工作；未覆盖或提交这些改动。
- 编译服务器：`192.168.3.88`。
- 隔离源码：`/home/dominolu/dev/titan-gatecrossex-20261021-39f1267b`，新建目录，无 `.git`；未覆盖服务器其他工作区。
- Rust/Cargo 1.94.1。既有编译链测试使用服务器已有 Python 3.11.14 虚拟环境，SDK 路径显式指向本次隔离源码。
- 本地只编辑、审查、传输和计算源码校验和；格式化、编译、Clippy、所有测试及公开行情探测均在指定服务器执行。
- 当次验证时，[源码清单](source-files.sha256) 的 362 个源码/配置/文档路径与本地逐项一致，清单 SHA256：`7f70813ec4b8e0d72f22fdc45166fe3f9cd0e83a04103c6497dba6b5e614c3c6`。不包含本报告、日志及二进制 fixture；后续复审增加的状态说明不属于该历史快照。
- [环境与 fixture 身份](logs/titan-gatecrossex-environment.txt)。没有部署或运行真实账户策略。

两个二进制 fixture 使用编译服务器此前验证过的版本：

| fixture | SHA256 |
| --- | --- |
| `deploy/pair_arb_v13/artifacts/pair_arb.titan` | `5f532d44ed3b14725e8aa212de9c30ee0e4dbb81378bfcd202d8221cf660af63` |
| `crates/titan-cli/tests/fixtures/artifacts/event_counter.titan` | `e0cbe122bea8c0e3a24d932cbadb5b2945205d9b3258482b0684a40be85bdb65` |

本地既有 `pair_arb.titan` 的 SHA256 为 `39bd8b286866c12fdb56c218199bd0c810bc299ab435f5053e3983c90641225e`，与上述服务器 fixture 不同；本任务没有覆盖本地文件。完整回归使用表中服务器版本，不能据此声称本地不同 fixture 也已通过验收。

## 结果

| 检查 | 结果 | 原始日志 |
| --- | --- | --- |
| `cargo fmt --all -- --check` | 通过 | [格式](logs/titan-gatecrossex-fmt.txt) |
| workspace/all-targets/all-features Clippy `-D warnings` | 通过 | [Clippy](logs/titan-gatecrossex-clippy-workspace.txt) |
| `connector` 独立 `gatecrossex` feature，关闭默认 features | 编译通过 | [独立 feature](logs/titan-gatecrossex-feature-check.txt) |
| connector 全量单元测试 | 255 passed，0 failed，12 ignored | [connector](logs/titan-gatecrossex-connector-all.txt) |
| workspace/all-targets/all-features | 477 passed，0 failed，12 ignored | [workspace](logs/titan-gatecrossex-workspace-test.txt) |
| workspace doctest | 2 passed，0 failed，1 ignored | [doctest](logs/titan-gatecrossex-doc-test.txt) |
| 主网公开行情，无凭据、无账户流 | Bbo/Depth/Trade/Mark/Funding 全部收到 | [公开行情](logs/titan-gatecrossex-public-probe.txt) |

新增 23 项 CrossEx 模拟/协议测试和 1 项 MarketConnector 工厂契约测试，覆盖签名与真实 HTTP body 字节、精度/模式拒绝、异步准备后的提交检查、写请求不重试、受理后查单失败的未知结果、撤单/改单不伪造终态、基础币数量、余额跨所聚合、折扣估值与钱包的区别、双向及零仓位、累计与迟到成交、空客户端身份、私有 payload、全部订阅确认及重连重放。

独立 Python HMAC-SHA512 实现生成固定测试向量，再与 Rust 签名结果比较；测试凭据仅为固定虚构字符串。主网探测使用服务器编译的 `gate_crossex_market_stream_probe`，只订阅 `BINANCE_FUTURE_BTC_USDT` 的五个公开频道，没有读取凭据、连接私有账户或发出订单。该观测确认该品种的实际 symbol 可保留 CrossEx 原生身份，不能推广为全部底层所和品种均已验收。

新增公开行情 example 在完整回归后添加，单独编译运行通过；最终全 targets Clippy 和格式检查覆盖该 example。生产模块的最终账户估值修正已经包含在 477 项全量回归中。

初次定向 Clippy 命令触发了既有 `hftbacktest` 未启用 S3 分支的 `needless_return`；采用项目既有 workspace 全 features 组合后通过，未修改该无关代码。初次完整测试使用系统 Python，因缺少 llvmlite 失败；显式指定服务器既有虚拟环境后全部通过。未放宽断言或启用 ignored 测试。

## 复现命令

仅在上述隔离源码目录执行：

```sh
export CARGO_TARGET_DIR=/home/dominolu/dev/titan-compiler-20261021-39f1267b/target
export TITAN_STRATEGY_PYTHON=/home/dominolu/dev/titan-compiler-20261021-39f1267b/.venv/bin/python
export TITAN_STRATEGY_SDK="$PWD/python/titan-strategy-sdk"
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo check --locked -p connector --no-default-features --features gatecrossex --lib
cargo test --locked -p connector --all-features
cargo test --locked --workspace --all-targets --all-features
cargo test --locked --workspace --all-features --doc
cargo run --locked -p connector --example gate_crossex_market_stream_probe
```

## 验收边界

本次支持 CROSS_EXCHANGE 的 FUTURE 业务和 USDT 资产估值；私有账户、下撤改单、杠杆操作和恢复均经模拟验证，尚未进行真实账户验收。没有转账、修改账户模式、扩大持仓或启动长期实盘策略。其他能力的明确不支持项见接入文档。
