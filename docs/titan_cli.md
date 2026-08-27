# Titan CLI 与 Agent 接口

`titan` 是回测和实盘的统一入口。公开命令接收策略 ID 和 TOML 配置；controller 完成静态校验并生成内部 RunSpec，`run-worker` 仅作为隐藏的进程边界使用。

## 运行

```bash
titan run <strategy> -e <backtest|live> -m <bar|tick|hybrid> -c <config.toml>
```

长选项为 `--env`、`--mode` 和 `--config`。前台与 `--detach` 使用同一条 spawn-worker 路径。

```bash
# Bar 回测
titan run dual_ma -e backtest -m bar -c configs/dual_ma_aapl.toml

# Tick 实盘
titan run dual_ma -e live -m tick -c configs/dual_ma_live.toml

# 后台运行，返回 run_id
titan run dual_ma -e backtest -m bar -c configs/dual_ma_aapl.toml --detach
```

当前 live backend 支持 Tick 模式；不支持的 environment、mode、backend 或策略能力组合会在 worker 启动前失败。

## 配置

环境和事件模式只由 CLI 指定，TOML 保存策略参数和对应 backend 配置，避免重复配置产生覆盖优先级。

```toml
schema_version = 1
history_capacity = 16

[strategy.parameters]
fast = 20
slow = 50

[backtest]
data = "../data/bars.json"
```

Hybrid 使用 `backtest.tick_data` 与 `backtest.bar_data`。Live 使用 `[live]` 和 `[[live.instruments]]`。相对路径以配置文件所在目录为基准；未知字段和无关 mode 的字段会被拒绝。Live 配置只保存 connector 引用，不保存凭证。

Tick/Hybrid 回测可显式配置执行模型：

```toml
[backtest.execution]
entry_latency_ns = 100000
response_latency_ns = 200000
maker_fee = -0.0001
taker_fee = 0.0005
queue_power = 3.0
queue = "power_probability" # 或 risk_averse
exchange = "partial_fill" # 或 no_partial_fill
asset = "linear"          # 或 inverse
contract_size = 1.0
latency_offset_ns = 0
last_trades_capacity = 1024
```

controller 将解析结果保存到 `.titan/runs/<run-id>/run.json`。该文件是 controller 与 worker 之间的版本化内部协议，不是公开输入格式。

## 运行管理

```bash
titan ls
titan ls --active
titan ls --env live --strategy dual_ma
titan show <run-id>
titan logs <run-id>
titan stop <run-id>
```

运行记录包含策略版本、environment、event mode、进程身份、健康状态、执行计数、ResultBundle 和报告状态。状态覆盖 `STARTING`、`LOADING`、`COMPILING`、`READY`、`RUNNING`、`STOP_REQUESTED`、`CANCELLED` 及其他终态。

## 策略目录

```bash
titan strategy ls
titan strategy show dual_ma
titan strategy validate dual_ma
titan strategy compile dual_ma --parameters '{"fast":20,"slow":50}'
```

`ls/show/validate` 只读取静态 Manifest；只有 `compile` 初始化 Python 和 Numba。Manifest 声明策略支持的 environment、event mode 和参数 schema。

## ResultBundle 与报告

```bash
titan report <run-id>
titan report <run-id> --renderer native --output report.html
titan report <run-id> --renderer quantstats --output quantstats.html
```

Rust Runtime 是执行和账户事实的唯一来源。Python reporting 会重新校验 ResultBundle，只负责渲染，不重新计算成交、费用、资金费、PnL 或收益率。报告失败不会改变已完成任务的状态。
报告输出必须位于 ResultBundle 目录之外；同一个 run 同一时间只允许一个 renderer 写入。
若 Runtime 没有记录 canonical returns，QuantStats 会生成明确的 no-data 页面，不推导或伪造收益率。

## Agent 调用

主要命令均支持 `--json`：

```bash
titan run dual_ma -e backtest -m bar -c configs/dual_ma_aapl.toml --detach --json
titan ls --active --json
titan show <run-id> --json
titan logs <run-id> --json
titan stop <run-id> --json
titan report <run-id> --output report.html --json
```

JSON 对象包含 `schema_version`。失败时 stderr 返回稳定的 `error.code` 和 `error.message`，退出码区分配置、编译、Runtime、报告、registry 和系统错误。worker、Python 和策略日志不会混入 stdout。
