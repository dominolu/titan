# Runtime 符号迁移清单

> 该清单对应 `M0.1/M0.2`。目标不是保留两份实现，而是为当前符号指定唯一归宿。

## `hftbacktest/src/runtime.rs`

| 符号组 | 目标 | 处理 |
|---|---|---|
| ABI version、event ID、slot count | `titan-runtime-abi` | 已迁移并由 `hftbacktest::runtime` 临时 re-export |
| `FillEvent`、`OrderEvent`、`MarketState` | `titan-runtime-abi` | 迁移纯 `repr(C)` 定义；执行报告转换留在 engine adapter |
| `OrderCommand` | `titan-runtime-abi` | 迁移 wire struct；decode 留在 `hftbacktest` adapter |
| `BarItem`、`TimedBarItem`、`RuntimeTimer`、`RuntimeFunding` | `titan-runtime-abi` | 迁移 wire struct；funding config 转换留在 engine adapter |
| `BarHistoryView`、`TickItem`、`StrategyRuntimeContext` | `titan-runtime-abi` | 在 Bar/Event ABI 类型解耦后迁移 |
| `StrategyCallback`、`CallbackRegistry` | `titan-runtime` | 已迁移唯一 callback dispatch |
| `RuntimePayload/Event/EventSource` | `titan-runtime` | 已迁移唯一 source contract |
| `MaterializedBarSource` 和生命周期逻辑 | `titan-runtime` | 已迁移；撮合调用通过 `hftbacktest` adapter |
| `RuntimeError/RunStats/run_event_runtime*` | `titan-runtime` | 已迁移唯一事件循环和错误语义 |
| `PreparedBarRuntime` | `titan-runtime` | 已迁移，后续由 worker orchestration 评估是否保留 |

## `py-hftbacktest/src/runtime.rs`

| 符号组 | 目标 | 处理 |
|---|---|---|
| `RuntimeExecutionReport`、thread-local snapshot | ResultBundle writer | 不迁移 thread-local API；由 worker 持有运行结果并写 Bundle |
| Backtest/Live `RuntimeBotEvents` adapter | `hftbacktest` | 仅迁移仍缺失的 engine adapter；不得复制事件循环 |
| callback address 转换 | `titan-python-host` | 封装进 `LoadedNumbaStrategy`，不公开裸地址数组 |
| `*_run_tick_runtime` | 删除 | worker 直接调用 Rust API |
| `*_run_hybrid_runtime*` | 删除 | worker 直接调用 Rust API |
| `run_*materialized_bar_runtime*` | 删除 | worker 直接调用 Rust API |
| `strategy_runtime_layout` | 删除 | 由版本化 ABI descriptor/fingerprint 替代 |

## `py-hftbacktest/src/lib.rs` 和 Python package

| 符号组 | 目标 | 处理 |
|---|---|---|
| `BacktestAsset`/`LiveInstrument` pyclass | RunSpec + `hftbacktest` builder | 删除 Python 对象 |
| `build_*backtest`/`build_*livebot` | worker Backend factory | 删除 pyfunction |
| `_hftbacktest` pymodule | 删除 | 不提供 replacement extension |
| `eventbot.py` compiler/bridge | `titan-strategy-sdk` | 提取纯 Python compiler；删除 ctypes driver |
| `hftbacktest.reporting` | `titan-reporting` | 保留并冻结 schema，删除账户重算后迁包 |
| 示例策略 | `strategies/` | 迁移为 Manifest + build(parameters) |

## 依赖环拆除顺序

1. ABI metadata/event IDs 进入 `titan-runtime-abi`。
2. Bar/Event 的 wire 表示与 engine helper 分离。
3. 其余纯 `repr(C)` payload 和 Context 进入 ABI crate。
4. `hftbacktest::backtest::bar` 只依赖 ABI payload，不依赖 Runtime 事件循环。
5. 唯一 Runtime 事件循环移动到 `titan-runtime`，通过 adapter trait 调用 engine。
6. worker 接通后删除所有 Python C ABI Runtime symbols。
