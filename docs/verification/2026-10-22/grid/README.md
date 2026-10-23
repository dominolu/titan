# Numba 网格与停止屏障验证

日期：2026-10-22。全部编译、格式检查、静态检查、代码生成及测试在 `192.168.3.88` 完成；没有读取真实账户、连接交易所交易接口或部署实盘。

## 源码与环境

- 本地起点 HEAD：`2aafc598b0ac43d40a2852cae26a16aed09d9408`。工作区已有未提交的 Rust 编译链等改动，本次保留这些改动，以当前工作区快照验证。
- 服务器隔离目录：`/home/dominolu/dev/titan-grid-20261022-2aafc598`，新建且不覆盖既有源码。服务器旧目录没有 Git 信息，故用基线标识与源码 SHA256 清单确认身份。
- Rust/Cargo：1.94.1；Python：3.11.14；Numba：0.63.1；NumPy：2.2.6。
- 复用服务器 `.venv` 和 Cargo 构建缓存，显式设置 `TITAN_STRATEGY_PYTHON`、`TITAN_STRATEGY_SDK`、`TITAN_TEST_BINARY`，避免 editable install 或系统 Python 指向旧源码/缺少 llvmlite。
- [完整输入 SHA256 清单](source-sha256.txt)，[关键源码及制品校验和](artifact-checksums.txt)，[工具链](toolchain.txt)。证据目录自身不纳入输入清单，避免递归摘要。

## 实际结果

| 检查 | 结果 | 证据 |
| --- | --- | --- |
| `cargo +1.94.1 fmt --all -- --check` | 通过 | [格式日志](fmt.txt) |
| `cargo +1.94.1 clippy --workspace --all-targets --all-features -- -D warnings` | 通过，无 warning | [Clippy](clippy.txt) |
| `cargo +1.94.1 test --workspace --all-targets --all-features` | 512 passed、0 failed、12 ignored | [Rust 回归](rust-tests.txt) |
| SDK `unittest discover` 全量 | 77 passed | [SDK 回归](sdk.txt) |
| Rust CLI → Numba AOT → Linux bundle → loader 校验 | 通过 | [编译日志](grid-build.txt)、[制品校验](artifact-validation.json)、[manifest](artifact-manifest.json) |
| 原生 ctypes 回调 | 通过 | SDK 日志中的 `TestGridNative` |
| 离线 adapter 停止屏障 | 通过 | [停止探针](stop-smoke.txt) |
| 真实 NativeV13Runtime 停止屏障 | 通过 | 同上：撤单→reduce-only 平仓→等待权威零持仓 |
| CLI 完整轨迹 | COMPLETED，7 事件、3 个下单意图、1 个撤单意图 | [CLI 运行](cli-run.txt) |
| CLI 截断轨迹 | 明确失败，不生成成功结果 | SDK 日志中的 `test_cli_trace_drains_and_rejects_incomplete_stop` |

SDK 新增的 43 项网格业务回归涵盖：等差/等比、三种规模模式、方向性初始化、部分 IOC、均价/最后成交锚点、分数 tick、保留旧单、撤对侧确认、撤单与成交竞态、重复成交、终态先于成交、已知成交持仓先更新、长时间挂单的终态等待起点、待确认预占、最小交易规则、上下边界、行情过期、账户 epoch、未知订单、退避、留仓重启、严格恢复、毛收益归属和停止平仓。另有 2 项原生/CLI 测试、2 项停止能力契约测试；其余为已有 SDK 回归。

## 复现

在上述服务器目录执行，日志中的路径是本次隔离目录，不应作为未来默认工作区：

```bash
export TITAN_STRATEGY_PYTHON="$PWD/.venv/bin/python"
export TITAN_STRATEGY_SDK="$PWD/python/titan-strategy-sdk"
export TITAN_TEST_BINARY="$PWD/target/debug/titan"
export CARGO_BUILD_JOBS=8
cargo +1.94.1 fmt --all -- --check
cargo +1.94.1 clippy --workspace --all-targets --all-features -- -D warnings
cargo +1.94.1 test --workspace --all-targets --all-features
PYTHONPATH=.:python/titan-strategy-sdk .venv/bin/python -m unittest discover -s python/titan-strategy-sdk/tests -v
target/debug/titan strategy compile --strategy strategies/grid/strategy.py \
  --parameters strategies/grid/parameters.json --target x86_64-unknown-linux-gnu \
  --cpu-baseline x86-64-v2 --artifact-format bundle \
  --output verification-artifacts/grid/grid.titan
target/debug/titan strategy validate verification-artifacts/grid/grid.titan --json
cargo +1.94.1 run -p titan-strategy-runtime --example v13_grid_stop_smoke -- verification-artifacts/grid/grid.titan
TITAN_HOME="$PWD/verification-artifacts/grid/cli-fresh" target/debug/titan run \
  verification-artifacts/grid/grid.titan -e backtest -m tick \
  -c configs/grid_v13_backtest.toml --json
```

## 验收范围

这是原生策略功能和执行一致性验收，不是历史收益回测或实盘验收。制品采用示例小整数参数且未签名，供离线验证；真实部署需使用目标品种精度、货币换算、规则及经确认的账户配置重新编译并走独立实盘流程。12 项 ignored 测试保持忽略，没有统一开启。

策略支持线性合约 NET 模式。现货库存分配、双向持仓、Web 管理、热调参及自动设置杠杆未纳入本次交付。实际费用、资金费和净权益继续由平台账本处理，策略配对指标明确为毛收益。接口及限制见 [使用说明](../../../../strategies/grid/README.md)，来源差异见 [分析](../../../gate_arb_grid_analysis.md)。
