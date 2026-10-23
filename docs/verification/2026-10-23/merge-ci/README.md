# 主分支合并前 CI 修复验证

2026-10-23。将 `codex/gate-crossex-broker` 合入 `master` 前，发现提交 `a055af9182f1c9787ceb3f6dd27e12e9da590c2d` 的 [GitHub CI](https://github.com/dominolu/titan/actions/runs/39889587193) 有三项失败。主分支基线为 `39f1267b6412165fc7adc005719bc9e09dcef9a8`，是功能分支的祖先，没有需要解决的产品代码冲突。

## 原因与修复

- Python SDK 测试已经使用 Rust 编译入口，但 Python CI job 没有构建 `target/debug/titan`，原生网格及 AOT 测试因此报 `FileNotFoundError`。新增 Rust 工具链、缓存和显式 CLI 构建，绑定当前 SDK、Python 和测试程序路径；Rust 版本统一为仓库的 1.94.1。
- 动态插件架构已移除，CI 仍调用不存在的 `scripts/test_dynamic_connector_plugins.sh`。改为执行现有 `scripts/check_single_runtime.sh`，保持单运行时架构检查。
- `pending_retry_round_robins_across_subscribers` 提前丢弃 `RecordingHandler` 的接收端，消费线程可能 panic；启动两个消费线程的先后顺序也影响前两次重试结果。测试改为保留真实订阅 receiver，分别显式释放一个槽位，核对两个订阅各获得一次重试。等待实际 trace，避免累计指标先于 trace 发布的竞态。没有增加 sleep、放宽超时或删掉公平性断言。

本轮只修改工作流和测试，网格、编译器及生产执行逻辑保持 `a055af91` 的版本；此前 [18 组历史回测](../../2026-10-22/grid-history/README.md) 仍属于该产品代码版本。

## 编译服务器验证

使用 `192.168.3.88` 新建隔离目录 `/home/dominolu/dev/titan-merge-20261023-a055af91`，由 `git archive a055af91` 展开，再同步两处修复。未覆盖原工作区。复用服务器 target 与 Python 环境；Rust 1.94.1、Python 3.11.14、Numba 0.63.1、NumPy 2.2.6。显式指定本轮 SDK/Python 路径。

[完整源码摘要](source-files.sha256) 共 751 个文件，远端 [逐文件核对](logs/source-check.txt) 全部通过。摘要包含当时已有的验证文档，不包含本证据目录。以下命令由 [实际验证脚本](verify.sh) 在服务器执行；本地没有运行构建、格式化、静态检查或测试。

| 检查 | 结果 | 证据 |
| --- | --- | --- |
| Rust fmt check | 通过 | [fmt](logs/fmt.txt) |
| 工作流 YAML 解析、单运行时检查 | 通过 | [YAML](logs/workflow-yaml.txt)、[架构](logs/single-runtime.txt) |
| Rust CLI 构建 | 通过 | [构建](logs/cli-build.txt) |
| 事件引擎 | 53 passed | [事件引擎](logs/event-engine.txt) |
| 公平性回归重复运行 | 100/100 passed | [重复回归](logs/fairness-repeat.txt) |
| `cargo test --locked --workspace` | 513 passed、0 failed、13 ignored，含 doctest | [默认配置](logs/rust-default.txt) |
| `cargo test --locked --workspace --all-targets --all-features` | 517 passed、0 failed、12 ignored | [全功能](logs/rust-all.txt) |
| Clippy 全 target / feature，`-D warnings` | 通过，无警告 | [Clippy](logs/clippy.txt) |
| SDK 全量 | 112 passed | [SDK](logs/sdk.txt) |
| 报告测试 | 4 passed | [报告](logs/reporting.txt) |
| Python compileall | 通过 | [语法](logs/python-syntax.txt) |

[机器汇总](logs/results.json)。没有运行真实账户交易，也没有开启 ignored 测试。本地仅在上述验证完成后新增本证据目录，产品代码与测试未再修改。
