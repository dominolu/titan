# Titan

Titan 是面向加密货币永续合约的 Rust 高频交易框架，支持逐笔回测、Bar 回测、Hybrid 执行和实盘交易。策略使用 Numba 编写，同一份策略可运行于回测与实盘。

## 架构

```text
titan CLI controller
└── spawn titan run-worker
    ├── 加载并编译 Numba 策略
    ├── 运行唯一的 Rust Runtime
    ├── 驱动 Bar / Tick / Hybrid / Live backend
    └── 原子写入 ResultBundle
```

- `titan` 是唯一用户入口；controller 不初始化 Python。
- 每次运行使用独立 worker，负责冷路径的 CPython/Numba 编译。
- 事件热路径为 `Rust Runtime → C ABI → Numba nopython callback`，不经过 Python、PyO3 或 GIL。
- 撮合、账户、手续费、资金费和结果均以 Rust Runtime 为唯一权威来源。
- Python reporting 只读取并渲染 ResultBundle，不重新计算交易结果。

## 环境

- Rust 1.94.0
- Python 3.11+

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -e ./python/titan-strategy-sdk
pip install -e './python/titan-reporting[quantstats]'

PYO3_PYTHON="$PWD/.venv/bin/python" cargo build --release -p titan-cli
```

## 快速开始

```bash
# 仅做静态校验，不启动 Python
target/release/titan validate crates/titan-cli/tests/fixtures/dual_ma_run.json

# 前台运行
target/release/titan run crates/titan-cli/tests/fixtures/dual_ma_run.json

# 后台运行与管理
run_id=$(target/release/titan run run.json --detach)
target/release/titan ls
target/release/titan show "$run_id"
target/release/titan logs "$run_id"
target/release/titan stop "$run_id"
target/release/titan report "$run_id"
```

Numba 策略位于 [`strategies/`](strategies/)，入口格式为 `module:function`。示例见 [`strategies/dual_ma`](strategies/dual_ma)。

```python
from numba import njit

@njit
def on_bar(s):
    # 读取行情、更新状态并提交订单
    pass
```

## 核心能力

- Level-2 / Level-3 订单簿回放与队列位置模拟
- 行情延迟、订单延迟、手续费和资金费建模
- Bar、Tick、Hybrid 和 Live 统一事件生命周期
- 多资产、多交易所回测
- 独立交易所连接器与 iceoryx2 共享内存 IPC
- 带 SHA-256 manifest 的权威 ResultBundle
- worker 状态、日志、停止和异常恢复

## Broker 支持

| Broker | 市场 | 状态 | 说明 |
|---|---|---|---|
| Binance Futures | USD-M 永续合约 | ✅ 生产可用 | 主网/测试网，已接入统一 Broker API |
| OKX | V5 SWAP | ✅ 生产可用 | 实盘/模拟盘，已接入统一 Broker API |
| Hyperliquid | 永续合约 | ✅ 生产可用 | 主网/测试网，支持 EIP-712 签名 |
| Binance Spot | 现货 | 🚧 开发中 | 连接器框架已具备，Broker API 尚未完整接入 |
| Bybit | 线性合约 | 🚧 开发中 | 连接器框架已具备，Broker API 尚未完整接入 |

## 数据与结果

Tick 数据使用统一 NumPy 结构化数组；Bar 数据支持 Parquet 和扁平 NPY。原始 WebSocket 数据可通过 `collector` 采集并归一化。

worker 完成后原子提交 ResultBundle。`titan report` 会先验证 schema、文件大小和 SHA-256，再调用只读报告进程。格式说明见 [`docs/result_bundle_schema.md`](docs/result_bundle_schema.md)。

## 测试

```bash
PYO3_PYTHON="$PWD/.venv/bin/python" cargo test --workspace --all-features
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features
```

## 文档

- [重构后的 CLI 与 Runtime 设计](docs/rust_cli_numba_strategy_refactoring.md)
- [ResultBundle schema](docs/result_bundle_schema.md)
- [连接器说明](connector/README.md)

## License

MIT。项目源自 [hftbacktest](https://github.com/nkaz001/hftbacktest)。
