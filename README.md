# Titan

Titan 是面向加密货币永续合约的 Rust 高频交易框架，支持逐笔回测、Bar 回测、Hybrid 执行和实盘交易。策略使用 Numba 编写，同一份策略可运行于回测与实盘。

## 架构

```text
titan CLI controller
└── spawn titan run-worker
    ├── 加载并编译 Numba 策略
    ├── TitanCoreRuntime
    │   ├── EventEngine
    │   │   ├── Primary / Async lane（可靠账户事实）
    │   │   └── FastLane（低延迟行情）
    │   └── PluginEngine
    │       ├── StrategyPlugin
    │       ├── MarketPlugin → 动态 ConnectorFactory
    │       └── AccountPlugin → 动态 ConnectorFactory
    ├── 驱动 Bar / Tick / Hybrid / Live backend
    └── 原子写入 ResultBundle
```

- `titan` 是唯一用户入口；`run`/`validate` controller 不初始化 Python，只有显式执行 `strategy compile` 时例外。
- 每次运行使用独立 worker，负责冷路径的 CPython/Numba 编译。
- 事件热路径为 `Rust Runtime → C ABI → Numba nopython callback`，不经过 Python、PyO3 或 GIL。
- 撮合、账户、手续费、资金费和结果均以 Rust Runtime 为唯一权威来源。
- Python reporting 只读取并渲染 ResultBundle，不重新计算交易结果。

## 近期重要重构

### EventEngine：统一的有界事件运行时

- 使用预分配的多级 `EventArena`、带 generation 校验的事件句柄和最后引用回收，限制实盘热路径的内存增长。
- 账户等可靠事实通过 Primary/Async lane 投递，支持有界 pending、FIFO 重试、sequence gap 检测、
  `RESYNC_REQUIRED` 截止和订阅者健康状态。
- 高频行情通过 zero-copy MarketBatch reservation 和 FastLane 投递；支持 inline handler 或有界异步 worker，
  保留事件顺序并统计 queue drop、最大队列深度和 P50/P99/P99.9/max 延迟。
- 路由变更由 `EventControl` 在 EventLoop safe point 原子提交；启动、订阅退休、handler 退出和 Arena lease
  释放纳入 `TitanCoreRuntime` 的统一生命周期。
- 目标机 release 基准已冻结：默认容量、100 万事件、30 万 events/s 连续三轮无 drop、resync 或 Arena
  exhaustion；dispatch/subscriber P99.9 门槛分别为 8.39 ms 和 16.78 ms。

详细设计与运行方法见 [Titan EventEngine](crates/titan-event-engine/README.md)。

### PluginEngine：交易所与策略的动态插件化

- Binance Futures、OKX、Hyperliquid 的 ConnectorFactory 已从 CLI/Core 静态注册迁移到独立动态插件包；
  新增交易所无需修改 EventEngine、MarketPlugin、AccountPlugin 或 Titan main。
- 插件加载会校验 package manifest、SHA-256、ABI、schema/config version、capability 和 Core Runtime API
  兼容性，校验完成后才创建服务端点。
- `PluginPlan`、预绑定 endpoint generation、ActivationGate 和事务化 route commit 保证插件整体激活；创建、
  绑定或激活失败会按相反顺序回滚。
- `ResourceScope`、endpoint/event lease 和动态库 code lease 防止仍有线程、服务句柄或事件引用时卸载代码；
  配置替换支持 quiesce、stop 和 generation 隔离。
- 热路径只持有预绑定的 `ServiceHandle`、`EventPublisher` 或路由句柄，不查询 PluginRegistry/ServiceRegistry。

详细控制面契约见 [Titan PluginEngine](crates/titan-plugin-engine/README.md)。

### Hyperliquid Connector：主网闭环完成

- 已接入统一 MarketConnector、AccountConnector 和 Broker API，覆盖公共 REST/WS 行情、EIP-712 签名、
  私有 `orderUpdates`/`userEvents`、orders/positions/balances reconcile 及 scheduled-cancel heartbeat。
- 完成真实 socket 断线重连与订阅重放，以及 `submit → amend → cancel` 的 REST/私有 WS 逐字段对账；
  cloid、oid、status、price、qty、executed/leaves 和 exchange timestamp 均一致。
- 完成 0.01 ETH 开仓/reduce-only 平仓，以及 20 USDC 风险上限内 GAS 10.2/16.1 的真实部分成交；
  REST、私有 WS、fills 对账一致，测试结束后均为零挂单、零仓位。
- 实盘探针推动修复了 amend 新旧 oid 乱序、WS amend 字段刷新、价格/数量浮点 wire 格式、公共行情关闭
  错误要求签名，以及 rustls provider 初始化冲突。
- Hyperliquid 主网 MarketPlugin → EventEngine 60 秒验证零 drop/resync；FastLane enqueue/handler P99.9
  分别为 8.19 µs 和 4.10 µs。该 connector 的外部实盘验收门禁已全部解除。

完整记录与原始证据见 [Hyperliquid 主网验收报告](docs/validation/hyperliquid_2026-09-07/README.md)。

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

## 支持 Agent 调用的 Titan CLI

`titan` 以策略 ID、环境、事件模式和 TOML 配置启动任务。controller 先完成静态校验并生成内部 RunSpec，再启动隔离 worker；用户和 Agent 不直接传递内部 JSON。

```bash
# 仅做静态校验，不启动 Python
target/release/titan validate dual_ma \
  -e backtest -m bar -c configs/dual_ma_aapl.toml

# 回测
target/release/titan run dual_ma \
  -e backtest -m bar -c configs/dual_ma_aapl.toml

# 实盘
target/release/titan run dual_ma \
  -e live -m tick -c configs/dual_ma_live.toml
```

`-e/--env` 支持 `backtest`、`live`；`-m/--mode` 支持 `bar`、`tick`、`hybrid`。当前 Live backend 使用 Tick 模式。

公开配置使用 TOML：

```toml
schema_version = 1
history_capacity = 16

[strategy.parameters]
fast = 20
slow = 50

[backtest]
data = "../data/bars.json"
```

Hybrid 使用 `tick_data` 和 `bar_data`。Tick/Hybrid 还可通过 `[backtest.execution]` 配置订单延迟、费率、队列模型、撮合方式及线性/反向资产；Live 使用 `[live]` 和 `[[live.instruments]]`，配置中只保存 connector 引用。

前台运行等待 worker 结束并返回 ResultBundle 中的 `result.json`。后台运行立即返回 `run_id`，之后通过管理命令查询：

```bash
run_id=$(target/release/titan run dual_ma \
  -e backtest -m bar -c configs/dual_ma_aapl.toml --detach)

target/release/titan ls --active
target/release/titan show "$run_id" --json
target/release/titan logs "$run_id"
target/release/titan stop "$run_id"
target/release/titan report "$run_id" --output report.html
```

`run`、`validate`、`ls`、`show`、`logs`、`stop`、`report` 和策略目录命令均支持 `--json`。机器模式下 stdout 只包含 JSON，worker、Python 和策略输出写入运行日志；失败信息从 stderr 返回。

策略目录命令用于发现和预编译策略：

```bash
target/release/titan strategy ls --json
target/release/titan strategy show dual_ma --json
target/release/titan strategy validate dual_ma --json
target/release/titan strategy compile dual_ma --parameters '{"fast":20,"slow":50}' --json
```

详细命令和配置格式见 [Titan CLI](docs/titan_cli.md)。

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
- EventEngine/PluginEngine 驱动的动态交易所插件连接器
- 带 SHA-256 manifest 的权威 ResultBundle
- worker 状态、日志、停止和异常恢复

## Broker 支持

| Broker | 市场 | 状态 | 说明 |
|---|---|---|---|
| Binance Futures | USD-M 永续合约 | ✅ 生产可用 | 主网/测试网，已接入统一 Broker API |
| OKX | V5 SWAP | ✅ 生产可用 | 实盘/模拟盘，已接入统一 Broker API |
| Hyperliquid | 永续合约 | ✅ 生产可用 | 主网/测试网，支持 EIP-712 签名 |

## 数据与结果

当前 CLI 从 TOML 引用规范化的 Bar/Tick JSON 数据；底层引擎仍使用统一事件和 NumPy 数据布局。原始 WebSocket 数据可通过 `collector` 采集并归一化。

worker 完成后原子提交带 SHA-256 manifest 的 ResultBundle。`titan report <run-id>` 读取权威结果；指定 `--output` 时才启动 Python renderer。报告只能写到 ResultBundle 目录之外，且不会重新计算成交、费用、资金费或 PnL；没有 canonical returns 时，QuantStats 输出 no-data 页面。格式说明见 [`docs/result_bundle_schema.md`](docs/result_bundle_schema.md)。

## 测试

```bash
PYO3_PYTHON="$PWD/.venv/bin/python" cargo test --workspace --all-features
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features
```

## 文档

- [公开文档索引](docs/README.md)
- [Titan CLI 与 Agent 接口](docs/titan_cli.md)
- [Bar/Tick 与 Numba 策略接口](docs/bar_tick_numba_strategy.md)
- [ResultBundle schema](docs/result_bundle_schema.md)
- [连接器说明](connector/README.md)

## License

MIT。项目源自 [hftbacktest](https://github.com/nkaz001/hftbacktest)。
