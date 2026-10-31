# Titan

**用 Python 编写策略，以 Rust 执行回测与实盘。**

[![Rust](https://img.shields.io/badge/Rust-1.94.1-orange?logo=rust)](rust-toolchain.toml)
[![Python](https://img.shields.io/badge/Python-3.11%2B-blue?logo=python)](python/titan-strategy-sdk/README.md)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

Titan 是面向加密货币永续合约的事件驱动交易框架，连接 CEX 与 DEX，覆盖策略开发、订单簿回测和实盘运行。
Python/Numba 策略离线编译为 `.titan` 原生策略文件；同一份文件可用于回测与实盘，策略执行热路径由 Rust 与原生回调完成。

[快速开始](#快速开始) · [编写策略](#编写策略) · [交易所接入](#交易所接入) · [实盘部署](#实盘部署) · [文档](#文档)

## 为什么选择 Titan

- **从研究走向实盘**：复用同一套策略逻辑和编译产物，减少回测代码与实盘代码分开维护的成本。
- **Python 开发，原生执行**：使用 Numba 编写策略回调，运行预编译策略无需 Python 解释器，热路径不经过 GIL。
- **贴近市场微观结构的回测**：底层 HftBacktest 提供 L2/L3 订单簿、队列位置、部分成交、延迟、手续费与资金费建模。
- **统一 CEX 与 DEX 接口**：通过一致的行情、账户和订单接口接入多个交易场所，并保留各所的原生 API 能力。
- **实盘运行可管理**：平台负责账户对账、订单事实、风险门禁、停止流程与检查点恢复；每次运行保留独立日志和结果。
- **适合自动化与 Agent**：使用统一 `titan` CLI 编译、校验、运行、查询和停止策略，主要命令支持 `--json`。

## 交易所接入

| 名称 | 标识 | 类型 | 支持范围 | 状态 | 文档 |
| :--- | :--- | :--- | :--- | :--- | :--- |
| [Binance Futures](https://binance.com) | `binance-futures` | CEX | USD-M 永续；主网 / 测试网 | ![主网验证](https://img.shields.io/badge/live-verified-green) | [配置](connector/examples/binancefutures.toml) |
| [OKX](https://okx.com) | `okx` | CEX | V5 SWAP；实盘 / 模拟盘 | ![主网验证](https://img.shields.io/badge/live-verified-green) | [配置](connector/examples/okx.toml) |
| [Hyperliquid](https://hyperliquid.xyz) | `hyperliquid` | DEX | 永续合约；主网 / 测试网 | ![主网验证](https://img.shields.io/badge/live-verified-green) | [配置](connector/examples/hyperliquid.toml) |
| [Gate CrossEx](https://www.gate.com/docs/developers/crossex/en/) | `gate-crossex` | 跨所交易网关 | FUTURE / SPOT / MARGIN 与原生扩展 | ![限定范围验证](https://img.shields.io/badge/live-scoped-blue) | [指南](docs/gate_crossex_broker.md) |
| [Backpack](https://backpack.exchange) | `backpack` | CEX | 统一 USDC 永续；原生现货、RFQ、借贷和钱包等 API | ![非实盘验证](https://img.shields.io/badge/beta-yellow) | [指南](docs/exchange_onboarding.md) |
| [Lighter Core](https://lighter.xyz) | `lighter` | DEX | 统一 Simple / cross USDC 永续；原生 API 与官方 signer | ![非实盘验证](https://img.shields.io/badge/beta-yellow) | [指南](docs/exchange_onboarding.md) |
| [Variational Omni](https://docs.variational.io/technical-documentation/api) | — | DEX 统计数据 | 公开 v1 / v2 统计 API | ![仅统计数据](https://img.shields.io/badge/data-only-blue) | [指南](docs/exchange_onboarding.md) |

标识对应行情源的 `connector_type`，账户适配器使用 `<标识>-account`。Variational 使用独立 Rust 统计客户端，官方交易 API 尚未开放。

`live verified` 表示已有主网验收记录；`live scoped` 表示限定账户与品种的验收，Gate CrossEx 当前为 Gate BTC 被动下单、改单和撤单；`beta` 表示已通过非实盘验证，私有账户和实盘交易验收待完成。各所配置、能力边界和原生接口见[接入指南](docs/exchange_onboarding.md)与 [API 目录](docs/exchange_api_catalog.md)。

## 快速开始

当前原生策略编译与运行面向 **Linux x86_64**。Rust 工具链由 [`rust-toolchain.toml`](rust-toolchain.toml) 固定；Python 3.11+ 用于策略编译和可选 HTML 报告，运行已编译策略无需 Python。

### 1. 准备编译环境

在仓库根目录执行：

```bash
cargo build --release -p titan-cli --bin titan
export PATH="$PWD/target/release:$PATH"

python3.11 -m venv .venv
source .venv/bin/activate
pip install -e ./python/titan-strategy-sdk
export TITAN_STRATEGY_PYTHON="$PWD/.venv/bin/python"
```

策略编译还需要 `cc`、`nm`、`objcopy` 等原生工具。CLI 命令可通过 `titan --help` 查询。

### 2. 编译并运行示例

使用仓库中的只读事件计数策略与 Tick trace，体验从策略源码到事件回放的完整流程，无需交易所账户：

```bash
mkdir -p target/strategies
titan strategy compile \
  --strategy strategies/event_counter/strategy.py \
  --parameters strategies/event_counter/parameters.json \
  --strategy-format bundle \
  --output target/strategies/event_counter.titan

titan validate target/strategies/event_counter.titan \
  -e backtest -m tick -c configs/pair_arb_v13_backtest.toml

titan run target/strategies/event_counter.titan \
  -e backtest -m tick -c configs/pair_arb_v13_backtest.toml --json
```

CLI 当前支持 Tick 模式。订单簿历史回测的撮合、延迟和队列模型由底层 HftBacktest 提供，使用方式见[回测接口](docs/titan_cli.md#v13-tick-trace-回测)和[历史回测示例](docs/verification/2026-10-22/grid-history/README.md)。

### 3. 查看结果

`run` 返回 `run_id`，运行记录默认保存在 `.titan/`。将下面的 `RUN_ID` 替换为返回值：

```bash
titan ls --json
titan show RUN_ID --json
titan logs RUN_ID
titan report RUN_ID --json
```

运行结果写入带摘要校验的 ResultBundle，运行状态与日志通过 CLI 查询。需要 HTML 报告时安装可选的报告模块：

```bash
pip install -e ./python/titan-reporting

titan report RUN_ID --renderer native --output report.html
```

[ResultBundle 格式](docs/result_bundle_schema.md)记录了可供分析和归档的结果字段；使用 QuantStats 报告时可安装 `./python/titan-reporting[quantstats]`。

## 编写策略

策略通过 `StrategySpec` 声明事件订阅和参数，通过 `@njit` 回调处理行情、账户事件与定时器。策略状态、盘口和订单接口由 SDK 提供。

下面是[事件计数示例](strategies/event_counter/strategy.py)中的行情回调：

```python
from numba import njit

@njit
def on_tick(ctx):
    ctx.state["tick_items"] += len(ctx.ticks())
    ctx.state["tick_callbacks"] += 1
```

每份策略提供 `StrategySpec`、`state_dtype` 和 `build(parameters)`，将订阅、类型化状态与回调组合起来。参数在编译时写入策略文件；修改参数或策略逻辑后，使用快速开始中的 `titan strategy compile` 重新编译。签名与可信公钥配置见 [CLI 指南](docs/titan_cli.md)。

| 示例 | 用途 | 入口 |
| :--- | :--- | :--- |
| `event_counter` | 了解事件回调与策略状态 | [源码](strategies/event_counter/strategy.py) |
| `pair_arb` | Maker-Taker / Taker-Taker 配对套利 | [指南](strategies/pair_arb/README.md) |
| `grid` | 等差 / 等比、多空 / 中性合约网格 | [指南](strategies/grid/README.md) |

完整策略结构、参数类型和回调接口见 [Python/Numba SDK](python/titan-strategy-sdk/README.md)。

## 实盘部署

将编译后的策略文件部署到目标节点，在部署配置中更新文件路径与摘要，并绑定行情源、账户、交易品种和风险域。[配对套利部署模板](deploy/pair_arb_v13/README.md)提供了配置与服务示例；完成凭据引用、品种单位、风险配置及所需组件的启用后，通过同一 CLI 启动：

```bash
titan validate pair-arb-mainnet \
  -e live -m tick -c deploy/pair_arb_v13/runtime.toml

titan run pair-arb-mainnet \
  -e live -m tick -c deploy/pair_arb_v13/runtime.toml --detach --json
```

实盘以部署文件中的 `strategy_key` 为入口。使用 `titan show RUN_ID` 和 `titan logs RUN_ID` 查询状态，使用 `titan stop RUN_ID` 发起策略的停止流程。

## 工作原理

```text
Python / Numba 策略
        │ titan strategy compile
        ▼
   .titan 原生策略文件
        ├── 回测：历史事件 → 策略回调 → 撮合与账户模型
        └── 实盘：交易所事件 → 策略回调 → 订单执行
                         │
                         ▼
                运行记录与 ResultBundle
```

Rust 运行时统一管理行情、账户、策略与执行；EventEngine 在服务之间传递事件，Python 回调在编译后以原生机器码运行。架构和扩展方式见 [EventEngine](crates/titan-event-engine/README.md)与[策略运行时](docs/strategy_abi_v13_implementation_status.md)。

## 文档

| 主题 | 文档 |
| :--- | :--- |
| CLI 与自动化 | [命令、回测与运行管理](docs/titan_cli.md) |
| 策略开发 | [Python/Numba SDK](python/titan-strategy-sdk/README.md) · [策略接口](docs/bar_tick_numba_strategy.md) |
| 交易所接入 | [接入指南](docs/exchange_onboarding.md) · [原生 API 目录](docs/exchange_api_catalog.md) |
| 部署与恢复 | [部署模板](deploy/pair_arb_v13/README.md) · [恢复存储](docs/recovery_storage_operations.md) |
| 结果与报告 | [ResultBundle](docs/result_bundle_schema.md) |
| 管理平台 | [管理平台说明](docs/management_console_requirements.md) |
| 开发与验证 | [工程约定](AGENTS.md) · [完整文档索引](docs/README.md) |

## License

[MIT](LICENSE)。项目源自 [hftbacktest](https://github.com/nkaz001/hftbacktest)。
