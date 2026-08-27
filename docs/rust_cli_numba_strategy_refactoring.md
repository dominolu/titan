# Rust CLI + Numba 策略统一运行重构方案

> 状态：设计方案，尚未实施
> 决策：Runtime 只保留一份 Rust 实现；删除 `py-hftbacktest` Runtime、ctypes 主入口和 PyO3 extension。仅由 `titan run-worker` 在冷路径使用最小 PyO3 embedding 加载和编译 Numba 策略。
> 前置约束：所有用户策略均使用 Numba；Rust CLI 是回测和实盘的唯一入口；Bar、Tick、Hybrid 只属于运行参数差异。

## 1. 重构目标

本次重构需要建立一套统一、可观测、可扩展的策略运行入口：

- 所有用户策略只实现 Numba `nopython` 回调，不增加 Rust 版策略；
- 回测和实盘统一通过 `titan run` 启动；
- `backtest`、`live` 是运行环境，`bar`、`tick`、`hybrid` 是事件模式；
- `titan` controller 只负责解析命令、创建运行记录和启动 worker，不初始化 CPython；
- 每个运行实例由独立 `titan run-worker` 进程直接构造 Backtest、LiveBot 和唯一 Runtime；
- Rust Runtime 直接调用 Numba 编译后的 C ABI 函数地址，热路径不经过 PyO3、CPython 或 GIL；
- Rust 输出权威 ResultBundle，Python 不重复计算撮合、账户、手续费、持仓和净值；
- Python 报告作为独立后处理阶段，由 Rust CLI 统一调度；
- CLI 支持查询策略目录、运行实例、状态、环境类型和事件模式。

现有 Bar/Tick 事件语义、单参数回调和 Runtime Context 约束继续遵循
[`bar_tick_numba_strategy.md`](bar_tick_numba_strategy.md)。

## 2. 当前实现判断与收敛原则

当前仓库没有“Rust 内嵌 CPython”的模式，但存在 PyO3 extension：

- `py-hftbacktest` 使用 `crate-type = ["cdylib"]`；
- PyO3 使用 `extension-module` feature；
- `_hftbacktest` 通过 `#[pymodule]` 暴露给 Python；
- `BacktestAsset`、`LiveInstrument` 使用 `#[pyclass]`；
- Tick Backtest 和 LiveBot 主要通过 `#[pyfunction]` 构造；
- Python 再通过 `ctypes.CDLL(_hftbacktest.__file__)` 调用导出的 Runtime C ABI。
- `hftbacktest/src/runtime.rs` 已存在核心 Runtime；`py-hftbacktest/src/runtime.rs` 还存在一套 C ABI、结果快照和 Backend 适配逻辑。

当前各路径的实际边界如下：

| 功能 | 当前机制 |
|---|---|
| 导入 `_hftbacktest` | PyO3 extension |
| 构造 BacktestAsset / LiveInstrument | PyO3 `#[pyclass]` |
| 构造 Tick Backtest / LiveBot | PyO3 `#[pyfunction]` |
| 编译用户策略 | Python + Numba |
| 生成 callback bridge | Numba `cfunc` |
| 调用 Rust Runtime | Python ctypes + C ABI |
| Rust 调用策略 | C ABI 函数指针 |
| `on_bar(s)` / `on_tick(s)` 中的 `s` | Numba 对 Rust Runtime Context 的结构化内存包装 |
| 报告生成 | Python |

`s` 不是 PyO3 对象。它由 Numba callback bridge 将 `StrategyRuntimeContext*` 转成结构化数组视图后构造。事件热路径已经不依赖 PyO3；本次重构需要进一步去掉 Python extension 对 Backend 构造和 Runtime 启动的所有权。

本次重构采用以下收敛原则：

- 全仓库只能存在一份事件生命周期、事件顺序、callback dispatch 和 Context 填充实现；
- 唯一 Runtime 最终位于 `titan-runtime`，底层撮合、账户、Backtest 和 LiveBot 继续位于 `hftbacktest`；
- `py-hftbacktest/src/runtime.rs` 不作为兼容 Runtime 保留，也不复制到新 crate；其中仍有价值的通用逻辑逐项合并进唯一 Runtime，纯 ctypes/PyO3 适配直接删除；
- 不再提供 Python 创建 Rust Backend、持有 Rust 指针或驱动 Runtime 的任何公共接口；
- 迁移期间允许用冻结的旧版本或 golden artifacts 做结果对照，但当前代码树中不长期维护新旧两套 Runtime。

## 3. 目标架构

```text
titan CLI controller（唯一用户入口，不初始化 CPython）
        │
        ├── 读取并静态校验 StrategyManifest 和 RunSpec
        ├── 创建 run_id 和运行注册记录
        └── spawn titan run-worker
                │
                ├── 取得运行所有权并更新 heartbeat
                ├── 直接构造 Rust Backtest 或 LiveBot
                ├── titan-python-host
                │       └── 最小 PyO3 embedding
                │           ├── 初始化 CPython
                │           ├── import 策略入口
                │           ├── 调用 compile_strategy()
                │           ├── Numba 编译 callbacks
                │           └── 返回进程内 callback、state 和 keepalive
                ├── 唯一 Rust Runtime
                │       ├── Bar / Tick / Hybrid 事件调度
                │       ├── 直接调用 Numba native callback
                │       ├── 撮合、账户、手续费和资金处理
                │       └── 输出 ResultBundle 或一致性快照
                └── 可选 spawn Python Report Process
                        ├── Native Renderer
                        ├── QuantStats Adapter
                        └── 后续其他 Renderer
```

每个 run 对应一个 worker 进程和一个内嵌解释器。禁止先初始化 CPython 再 `fork`；前台和
`--detach` 都必须走相同的 spawn-worker 路径，区别只在 controller 是否等待 worker 结束。

PyO3 不是 Runtime 形态，也不暴露 Backend/Runtime API；它只存在于 worker 启动阶段的策略
加载冷路径。Runtime 启动后，调用关系必须保持为：

```text
Rust Runtime -> C ABI function pointer -> Numba nopython callback
```

禁止变成逐事件调用普通 Python 函数或 PyO3 方法。

## 4. PyO3 边界决策

### 4.1 删除 Python Runtime/Backend 接口和 PyO3 extension

迁移完成后删除以下接口：

```rust
#[pymodule]
fn _hftbacktest(...)

#[pyclass]
struct BacktestAsset

#[pyclass]
struct LiveInstrument

#[pyfunction]
fn build_hashmap_backtest(...)

#[pyfunction]
fn build_roivec_backtest(...)

#[pyfunction]
fn build_hashmap_livebot(...)

#[pyfunction]
fn build_roivec_livebot(...)
```

同时取消：

```toml
[lib]
crate-type = ["cdylib"]

pyo3 = { features = ["extension-module"] }
```

不再生成或导入 `_hftbacktest.so`，也不再使用其文件路径作为 Runtime 动态库路径。
`py-hftbacktest/src/runtime.rs` 中的 C ABI Runtime 入口、thread-local 结果快照和 Backend 指针
适配也全部删除。最终 Python SDK 不链接、加载或持有任何 Rust Backend/Runtime 对象。

### 4.2 保留最小 PyO3 embedding

新增独立 crate。它由 `titan run-worker` 私有使用，不是面向用户的 Python extension：

```text
crates/titan-python-host/
  Cargo.toml
  src/
    lib.rs
    interpreter.rs
    compiler.rs
    descriptor.rs
    error.rs
```

依赖使用 embedding 模式：

```toml
[dependencies]
pyo3 = { version = "0.27.2", features = ["auto-initialize"] }
```

该 crate 的职责严格限制为：

- 初始化和配置 CPython；
- 配置策略 SDK 和策略目录的 `sys.path`；
- 调用 Python `compile_strategy()`；
- 提取 callback 地址、state buffer、能力和元数据；
- 保持 Numba bridge、Dispatcher、闭包数组和 NumPy state 存活；
- 将 Python、Numba 和 ABI 错误转换为结构化 Rust 错误。

它不得负责：

- 构造 Backtest 或 LiveBot；
- 读取和推进市场数据；
- 撮合、账户和手续费计算；
- 驱动事件循环；
- 生成分析报告。

建议 Rust 接口：

```rust
pub trait StrategyCompiler {
    fn compile(
        &self,
        spec: &StrategySpec,
        abi: &RuntimeAbi,
    ) -> Result<LoadedNumbaStrategy>;
}
```

`LoadedNumbaStrategy` 至少保存：

```rust
pub struct LoadedNumbaStrategy {
    pub metadata: StrategyMetadata,
    callbacks: CallbackRegistry,
    state: StrategyStateBuffers,
    abi_fingerprint: AbiFingerprint,
    python_keepalive: Py<PyAny>,
}
```

`StrategyStateBuffers` 和 `CallbackRegistry` 封装裸指针及长度，不对 CLI 或其他 crate 公开
可变裸指针。`LoadedNumbaStrategy` 不允许 `Clone`，不允许跨运行复用；是否允许跨线程必须由
显式线程模型决定。`python_keepalive` 的生命周期必须覆盖整个 Runtime，并在 Runtime 完全停止后
持有 GIL 释放。callback 地址、NumPy 指针和其他进程内地址禁止持久化到配置、文件或运行注册表。

worker 完成编译后可以释放 GIL，但不得 finalize CPython；Runtime 运行期间不得调用 Python C API、
PyO3 方法或需要 GIL 的 Numba 路径。

## 5. Numba 策略编译接口

新增 Python SDK：

```text
python/titan-strategy-sdk/
  titan_strategy/
    compiler.py
    descriptor.py
    context.py
    manifest.py
```

统一编译入口：

```python
def compile_strategy(
    entrypoint: str,
    parameters: dict,
    runtime_abi: dict,
) -> CompiledStrategy:
    ...
```

`compile_strategy()` 必须：

1. 根据 entrypoint 导入策略模块；
2. 调用策略 `build(parameters)`；
3. 校验 handler 是 Numba `Dispatcher` 且只能接收一个参数；
4. 为每个事件槽生成 `cfunc(int32(voidptr))` bridge；
5. 强制完成 JIT 编译，在 Runtime 启动前暴露编译错误；
6. 校验 Runtime ABI 版本、Context 布局和 callback 槽位；
7. 校验 state 数组是一维、连续且 dtype 正确；
8. 返回 callback 地址、state、能力、元数据和 keepalive 对象；
9. 拒绝 object mode、`objmode` 和任何需要 Python API/GIL 的 handler 路径；
10. 为每个失败保留 handler 名、编译签名、事件槽和稳定错误码。

建议返回结构：

```python
@dataclass
class CompiledStrategy:
    strategy_id: str
    strategy_version: str
    abi_version: int
    callback_addresses: tuple[int, ...]
    state_f64: np.ndarray
    state_i64: np.ndarray
    capabilities: tuple[str, ...]
    metadata: dict
    keepalive: tuple[object, ...]
```

### 5.1 Compile 与 Load 的区别

`titan strategy compile` 的强保证是：

- 校验策略和依赖；
- 生成策略指纹和编译诊断；
- 验证 Runtime ABI。

它可以尽力预热声明了稳定磁盘缓存的用户 Dispatcher，但不保证下一次运行无 JIT 延迟。
动态生成的 `cfunc` bridge 仍必须在每次 worker 启动时构建。

它不能保存并复用 callback 地址。每次 `titan run` 都必须在当前进程中重新加载策略，并取得当前进程有效的地址。

策略指纹至少包含策略源码及本地依赖、参数、Python/Numba/llvmlite 版本、CPU target/features、
策略 SDK 版本和 Runtime ABI fingerprint。

### 5.2 策略必须与未来数据解耦

同一份策略需要同时支持回测和实盘，因此 `build()` 不得接收完整未来行情，也不得在初始化阶段预计算整段回测指标。指标必须增量更新，或者只使用 Runtime 显式提供的历史窗口。

当前双均线策略接收完整 `closes` 并预计算 SMA，不满足统一回测/实盘要求。迁移时需要改为 O(1) 或固定窗口的增量 SMA，并保证 warm-up 行为在回测和实盘中一致。

## 6. Rust Runtime 与 ABI 拆分

当前两份 Runtime 必须收敛为一份。目标拆分：

```text
crates/
  hftbacktest/              # 撮合、账户、模型、Backtest、LiveBot
  titan-runtime/            # Bar/Tick/Hybrid统一运行时
  titan-runtime-abi/        # Rust/Numba共享ABI
  titan-python-host/        # 最小PyO3 embedding
  titan-cli/                # controller及run-worker二进制
```

`titan-runtime` 是唯一的事件 Runtime 实现。`hftbacktest` 只提供撮合、账户、市场模型、
Backtest、LiveBot 和必要适配 trait，不再包含另一套事件循环；`py-hftbacktest` 不在目标结构中。

`titan-runtime-abi` 负责：

- `StrategyRuntimeContext`；
- callback 函数签名和事件槽；
- Tick、Bar、Fill、Order 和 Command 的 ABI 结构；
- state buffer 规则；
- size、alignment、offset 和 ABI version 校验。

ABI descriptor 必须由 Rust 定义生成，并覆盖所有共享 struct 的字段类型、size、alignment、offset，
以及事件 ID、Command ID、错误码、callback 槽位数、pointer width 和 endianness。Python SDK 根据该
descriptor 构建并校验 NumPy dtype，双方计算稳定 ABI fingerprint；只比较 `abi_version` 或
`struct_size` 不足以放行 Runtime。

Numba callback 边界继续使用：

```rust
type StrategyCallback =
    unsafe extern "C" fn(*mut StrategyRuntimeContext) -> i32;
```

`titan-runtime` 负责：

- 统一生命周期和事件顺序；
- Bar、Tick、Hybrid source；
- callback registry；
- Runtime Context 和预分配 buffer；
- callback 错误、`on_error`、`on_stop`；
- ResultBundle 所需的权威执行和账户快照。

## 7. Rust CLI 命令设计

目标命令：

```text
titan run
titan ls
titan show
titan stop
titan logs
titan report
titan strategy ls
titan strategy show
titan strategy validate
titan strategy compile
```

### 7.1 统一运行命令

回测：

```bash
titan run dual_ma \
  --environment backtest \
  --event-mode bar \
  --config configs/dual_ma_aapl.toml
```

实盘：

```bash
titan run dual_ma \
  --environment live \
  --event-mode tick \
  --config configs/dual_ma_live.toml
```

不再把回测和实盘设计为两个独立策略入口。支持前台运行和：

```bash
titan run dual_ma --environment live --detach
```

### 7.2 运行实例列表

`titan ls` 展示已经加载、正在运行和历史运行实例：

```text
ID            STRATEGY  TYPE      EVENT  STATUS     PID    STARTED               REPORT
dual-ma-01    dual_ma   backtest  bar    completed  -      2026-08-26 10:02:15   ready
dual-ma-live  dual_ma   live      tick   running    23841  2026-08-26 10:15:08   -
maker-test    maker     backtest  tick   failed     -      2026-08-26 09:41:22   failed
```

支持过滤和机器可读输出：

```bash
titan ls --active
titan ls --type live
titan ls --type backtest
titan ls --strategy dual_ma
titan ls --status failed
titan ls --all
titan ls --json
```

### 7.3 策略目录列表

运行实例和可发现策略需要分开：

```bash
titan strategy ls
```

```text
STRATEGY  VERSION  EVENTS         ENVIRONMENTS    STATUS   SOURCE
dual_ma   1.0.0    bar            backtest,live   valid    strategies/dual_ma
maker     2.1.0    tick,bar       backtest,live   valid    strategies/maker
arb       0.4.0    tick,hybrid    backtest        invalid  strategies/arb
```

策略目录列表只读取静态 Manifest，不应为了列出策略而导入或编译全部 Python 模块。

## 8. Strategy Manifest

策略目录：

```text
strategies/
  dual_ma/
    strategy.toml
    strategy.py
    tests/
```

Manifest 示例：

```toml
id = "dual_ma"
name = "Dual Moving Average"
version = "1.0.0"
entrypoint = "strategy:build"
runtime_abi = 1
events = ["bar"]
environments = ["backtest", "live"]

[parameters.short_period]
type = "integer"
default = 20
minimum = 1

[parameters.long_period]
type = "integer"
default = 50
minimum = 2
```

运行前必须同时校验：

- Manifest schema 和参数；
- Python entrypoint；
- 实际 callback 与声明事件；
- Runtime ABI；
- environment 和 event-mode 能力；
- 策略 ID、版本和编译指纹。

StrategyManifest 只描述策略自身，不描述一次具体运行，也不保存交易账户、数据路径或凭证。

## 9. RunSpec 与 Backend 配置

Rust worker 要直接构造 Backend，必须有版本化、可完整反序列化的 RunSpec。RunSpec 至少包含：

- environment、event_mode 和 Backend 类型；
- instruments、market/venue、asset model、tick/lot size；
- Backtest 数据源、日期范围、撮合、queue、latency、fee 和 funding 模型；
- Live connector、IPC、订阅、恢复和风控配置；
- 策略参数、state 容量、历史窗口和 timer；
- Bundle、日志、报告和资源限制；
- schema_version 及配置来源摘要。

示意：

```toml
schema_version = 1
environment = "backtest"
event_mode = "bar"

[backend]
kind = "hashmap"

[execution]
exchange_model = "partial_fill"
queue_model = "risk_adverse"
bar_matching = "next_open"

[[instruments]]
symbol = "AAPL"
asset_type = "linear"
tick_size = 0.01
lot_size = 1.0

[instruments.fee]
kind = "trading_qty"
maker = 0.0001
taker = 0.0003

[strategy.parameters]
short_period = 20
long_period = 50
```

所有 model 使用带 `kind` 的判别联合，未知字段默认 fail-fast。相对路径统一相对于 RunSpec 文件
所在目录解析，再保存 canonical path 或内容摘要。Live secret 只能使用凭证引用，不得写入
RunSpec 快照、registry、日志或 ResultBundle。

controller 负责无需 Python 的静态校验；worker 在构造 Backend 和编译策略后执行动态能力校验。
必须维护 environment × event_mode × backend × data source 的显式能力矩阵，不根据行情字段猜测模式。

## 10. 状态与注册表

### 10.1 运行状态

策略目录的 `DISCOVERED/VALID/INVALID` 属于 strategy catalog，不进入运行实例状态机。运行状态为：

```text
CREATED -> LOADING -> COMPILING -> READY -> RUNNING -> STOPPING
                                                    ├-> COMPLETED
                                                    ├-> STOPPED
                                                    ├-> CANCELLED
                                                    └-> FAILED
```

健康状态独立记录：

```text
HEALTHY
UNRESPONSIVE
STALE
```

任一加载、编译或运行阶段都可以进入 `FAILED` 或接受 stop/cancel。正常完成回测使用
`COMPLETED`，操作员正常停止 live 使用 `STOPPED`，尚未开始执行就取消使用 `CANCELLED`。
heartbeat 超时但 PID 仍存在标记 `UNRESPONSIVE`；heartbeat 超时且 worker 身份无法确认标记
`STALE`。不能只依靠 PID，必须同时校验进程启动时间或随机 worker token，避免 PID 复用误判。

### 10.2 报告状态

报告状态独立于运行状态：

```text
NONE -> QUEUED -> GENERATING -> READY
                          \-> FAILED
```

报告失败不能将成功完成的回测改为 `FAILED`。

### 10.3 持久化

建议使用 SQLite，而不是单一 JSON 文件：

```text
.titan/
  registry.db
  runs/
    <run_id>/
      run.toml
      status.json
      logs/
      bundle/
      reports/
```

注册表至少包含：

- strategies；
- run_instances；
- run_heartbeats；
- artifacts；
- reports。

运行实例周期性更新 PID、状态、heartbeat、当前事件时间、事件数、订单数、成交数和最后错误。注册表禁止保存 callback 地址、NumPy 指针等进程内数据。

SQLite 使用 WAL、busy timeout 和短事务。controller 创建 run；worker 通过不可猜测的 owner token
取得该 run 的写入权，之后只有 owner worker 更新运行状态和 heartbeat。controller 启动时执行
reconciliation，修复上次崩溃留下的活跃记录。每次状态迁移和关键 artifact 注册必须在事务中完成。

## 11. ResultBundle 与报告

本次重构演进现有 `hftbacktest.reporting.ReportBundle` 和 `export_report_bundle()` schema，不新建
第二套同名 Bundle。P0 先冻结现有表、主键、排序、时间语义、checksum 和兼容读取规则，再让
Rust writer 生成同一 schema。表名以冻结后的 schema 为准，至少覆盖：

```text
manifest.json
run_metadata.json
execution_reports.parquet
order_events.parquet
portfolio_snapshots.parquet
account_snapshots.parquet
position_snapshots.parquet
market_marks.parquet
risk_events.parquet
fx_marks.parquet
```

若现有 `fill_events` 与拟议 `execution_reports` 语义重叠，P0 必须选择一个 canonical 名称并提供
schema migration，不能长期同时保留。manifest 记录 schema_version、文件行数、SHA-256、source/
derived 分类和生成器版本；Bundle 只有在所有文件 fsync 并原子提交 manifest 后才视为可见。

职责边界：

- Rust：成交、费用、余额、持仓、PnL、净值和风险状态；
- Python analytics：周期收益、回撤、统计指标和报告数据集；
- Renderer：只消费 Bundle 和派生分析数据，不重新解释撮合结果。

Rust 权威范围必须同时冻结估值规则：mark price 来源、快照时钟、FX 转换、realized/unrealized PnL、
费用/返佣/资金费率以及多账户聚合。Python 可以计算收益率、回撤和统计指标，但不得重新推导上述
权威字段。

报告不放入内嵌 Python解释器中运行。Rust完成 Bundle 原子落盘并将运行状态更新为 `COMPLETED` 后，再启动独立 Python Report Process：

```text
Rust Runtime -> ResultBundle -> Python Report Process -> HTML
```

用户可以单命令执行：

```bash
titan run dual_ma ... --report native
```

也可以不重新回测直接重新渲染：

```bash
titan report --run dual-ma-01 --renderer quantstats
```

实盘支持停止后报告或基于一致性快照生成阶段性报告：

```bash
titan report --run dual-ma-live --snapshot latest
```

运行中的 live Bundle 不使用“整体目录一次性原子落盘”假设。worker 在单一事件序号/账户快照
边界生成不可变 snapshot，原子提交独立 snapshot manifest；报告进程只能读取已经提交的 snapshot。

## 12. Python 环境与信任边界

仅设置 `sys.path` 不足以保证可复现。worker 启动前必须解析并记录 Python runtime、venv/conda/uv
环境、Python/Numba/llvmlite 版本、依赖锁摘要、CPU target/features 和 Numba cache 目录。依赖缺失、
libpython 不匹配或 cache 不可写必须在 `LOADING/COMPILING` 阶段给出结构化诊断。

策略模块在 import/build 阶段可以执行任意 Python 代码，Numba callback 也是进程内 native code；
因此 v1 明确把策略视为与 worker 等权的可信代码，而不是安全沙箱。Live 凭证采用最小权限并只注入
需要该连接器的 worker。以后若需要运行不可信策略，必须采用独立沙箱设计，不能把 PyO3 embedding
描述成隔离边界。

## 13. 目标目录结构

```text
crates/
  hftbacktest/
  titan-runtime/
  titan-runtime-abi/
  titan-python-host/
  titan-cli/                # titan controller + titan run-worker

python/
  titan-strategy-sdk/
    titan_strategy/
      compiler.py
      descriptor.py
      context.py
      manifest.py

  titan-reporting/
    titan_reporting/
      worker.py
      bundle.py
      analytics/
      adapters/
      renderers/

strategies/
  dual_ma/
    strategy.toml
    strategy.py
    tests/

configs/
  backtest/
  live/
```

`py-hftbacktest` 不作为 Runtime 或 Backend 兼容层保留。迁移开始时先冻结旧版本作为 golden 对照，
随后把仍需要的纯 Python 策略 SDK/报告代码分别迁入目标 package；C ABI Runtime、PyO3 extension、
Backend pyclass 和 ctypes 主入口直接进入删除清单。

## 14. 分阶段实施计划

### P0：冻结接口和基线

- 盘点 `hftbacktest/src/runtime.rs` 与 `py-hftbacktest/src/runtime.rs`，为每个符号指定唯一目标或删除结论；
- 定义完整 StrategyManifest、RunSpec、Backend 判别联合、参数 schema 和能力矩阵；
- 定义 RuntimeAbi descriptor、ABI fingerprint、callback 槽位和稳定错误码；
- 定义 CompiledStrategy 和 LoadedNumbaStrategy；
- 冻结现有 ResultBundle schema、估值规则、版本和 migration 规则；
- 定义 controller/worker 协议、进程所有权、停止语义和状态机；
- 冻结现有 Bar/Tick golden results、性能基线和 ABI 布局；
- 将旧 Python 运行结果保存为不可变 golden artifacts；后续不以长期双 Runtime 作为迁移手段。

### P1：提取 ABI 与 Numba compiler

- 将共享 ABI 移入 `titan-runtime-abi`，由 Rust生成 descriptor；
- 从 `eventbot.py` 提取 handler 校验和 callback bridge；
- 实现独立 `compile_strategy()`；
- 增加 callback、state、Context 和 ABI 校验；
- 增加编译错误分类和策略指纹；
- 使用 P0 golden artifacts 进行结果一致性对照；
- 将双均线改为增量指标，移除对完整未来 closes 的依赖。

### P2：建立最小 PyO3 host 和 Bar 垂直切片

- 新建 `titan-python-host`；
- 使用 `auto-initialize`，禁止 `extension-module`；
- Rust调用 `compile_strategy()` 并提取 descriptor；
- 实现 `titan` controller spawn `titan run-worker`，controller 不初始化 CPython；
- worker 直接构造 Bar Backtest，运行唯一 Runtime 和增量双均线；
- 保证 Python对象生命周期覆盖 Runtime，并验证 callback 运行期间不需要 GIL；
- 增加 Python环境、SDK路径和依赖诊断。

### P3：收敛唯一 Runtime 并覆盖 Tick/Hybrid

- 建立 `titan-runtime`，从两份现有实现中逐项合并唯一事件循环、source 和 callback registry；
- Rust直接构造 Tick Backtest，并接入 Tick/Hybrid source；
- 按 Bar、Tick、Hybrid 顺序通过 golden、失败注入和性能验收；
- 删除 `py-hftbacktest/src/runtime.rs`、ctypes Runtime 主入口和 thread-local 结果通道；
- 从 `hftbacktest` 删除已经迁入 `titan-runtime` 的重复事件循环，仅保留底层引擎和适配 trait；
- CI 增加检查，防止再次出现第二套 Runtime dispatch/lifecycle 实现。

### P4：完善 CLI controller、worker 和运行注册表

- 实现 `run`、`ls`、`show`、`stop`、`logs`；
- 建立 SQLite WAL registry 和 worker owner token；
- 实现严格状态迁移、PID/start-time、心跳、UNRESPONSIVE、STALE 和启动 reconciliation；
- 前台和 `--detach` 统一使用 spawn-worker；
- 实现信号转发、正常 stop、cancel、强杀和崩溃恢复测试；
- 完成 RunSpec 全量解析、secret redaction 和能力诊断。

### P5：建立策略目录并迁移 Live

- 实现 Manifest discovery；
- 实现 `strategy ls/show/validate/compile`；
- 将双均线移入 `strategies/dual_ma`；
- 移除 `hftbacktest.strategies` 中的示例策略公共入口；
- Rust worker 直接构造 LiveBot、connector、IPC 和恢复组件；
- 增加同一策略在 backtest/live 下的生命周期、warm-up 和停止一致性测试；
- 验证 Live 凭证只进入对应 worker，且不会进入日志、registry 或 Bundle。

### P6：ResultBundle 和报告解耦

- Rust按照冻结 schema 输出权威 ResultBundle；
- 实现 live 不可变一致性 snapshot 和原子 snapshot manifest；
- 删除 Python `_portfolio_tables()` 等账户重算逻辑；
- 实现独立 `titan report`；
- 接入 Native、QuantStats 和后续 Adapter；
- 实现历史 Bundle 重渲染；
- 确保报告失败不改变运行完成状态。

### P7：删除旧 Python binding 并完成切换

在 Rust CLI、Numba策略、回测、实盘和报告全部迁移并通过验收后：

- 删除 `#[pymodule]`、`#[pyclass]`、`#[pyfunction]`；
- 删除 `extension-module` feature；
- 删除 `_hftbacktest.so` import；
- 确认不存在 Python ctypes 对 Runtime 的主调用入口；
- 删除 `py-hftbacktest` 剩余 binding；纯 Python SDK/报告代码已经迁入独立 package；
- README、示例和运维脚本全部切换到 `titan` CLI。

## 15. 验收标准

- 用户不再直接执行 Python回测或实盘脚本；
- 所有用户策略只使用 Numba 单参数回调；
- 回测和实盘使用同一个 `titan run`；
- Bar、Tick、Hybrid 只体现为参数和事件能力差异；
- Rust直接拥有 Backend、Runtime 和运行生命周期；
- controller 不初始化 CPython，每个运行实例由独立 worker 拥有解释器和 Runtime；
- 全仓库只有一份 Runtime lifecycle、dispatch 和 Context 填充实现；
- Numba callback 热路径不经过 PyO3、CPython 或 GIL；
- `titan ls` 能准确显示策略实例、环境、事件模式、状态、PID和报告状态；
- `titan strategy ls` 无需导入 Python 即可列出策略；
- 同一 Numba策略可以在回测和实盘中运行，且不依赖未来数据；
- Rust是撮合、账户、手续费、持仓和净值的唯一权威实现；
- Rust Bundle 与冻结 schema 一致，历史 Bundle 可以按兼容规则读取；
- 报告可以基于历史 Bundle 重复生成和切换 Renderer；
- 报告失败不会破坏或否定已完成运行；
- `py-hftbacktest`、PyO3 extension、Backend pyclass 和 ctypes Runtime 主入口被删除；
- 最终只有 `titan-python-host` 依赖 PyO3，并且只用于策略加载与 Numba JIT 冷路径。

## 16. 非目标

本次重构不包括：

- 开发 Rust版用户策略；
- 将 Native/QuantStats 报告重写为 Rust；
- 第一阶段实现 Numba AOT `.so` 策略发布；
- 在策略热路径调用普通 Python函数；
- 将 callback 地址跨进程或跨运行持久化；
- 为 Bar、Tick、回测和实盘维护不同策略源码。
