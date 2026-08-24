# Bar/Tick 回测与实盘统一策略接口

> 状态：统一运行时已落地。Rust 事件循环、Numba 单参数回调、全局 TickBatch、显式 Bar、
> 历史环、NextOpen/SignalClose/Touch/ConservativeOhlc/VolumeLimited、Hybrid、Timer、Funding，以及
> native/canonical/recovery Live Bar 的去重与恢复组件均已实现；不受支持的能力组合 fail-fast。
>
> 当前仓库只保留 Rust `Strategy` trait；最终面向策略作者的接口必须是 Numba Python
> 单参数回调。Rust trait 可以作为核心内部接口、测试接口或迁移期兼容层，但不是最终的
> Python 策略 API。

## 目标

框架必须同时支持以下三种显式数据模式，并让同一份策略代码直接用于回测和实盘：

| 模式 | 策略输入 | 回测撮合 | 适用场景 |
| --- | --- | --- | --- |
| `Bar` | 预处理 OHLCV | 独立 Bar 撮合模型 | 中低频、因子、趋势策略 |
| `Tick` | 成交、BBO、深度 | Tick/L2 撮合模型 | HFT、做市、微观结构 |
| `Hybrid` | Bar 信号与 Tick/Depth | Tick/L2 撮合模型 | Bar 产生信号、逐笔数据模拟执行 |

数据模式必须由数据文件元信息和运行配置显式声明。运行时不得根据字段猜测模式，不得用
虚构的盘口让 Bar 数据伪装成 Tick/L2 数据，也不得默认在每次回测的策略循环中从逐笔成交
重新聚合 Bar。

## 唯一的用户策略调用形式

最终策略事件接口必须是 Numba `nopython` 回调，并且每个回调只接收一个参数 `s`：

```python
from numba import njit

@njit
def on_tick(s):
    # 读取当前 Tick 批次、盘口、持仓和策略状态；通过 s 下单或撤单。
    pass

@njit
def on_bar(s):
    # 读取同一 close_ts/timeframe 的 Bar 批次；通过 s 下单或撤单。
    pass
```

禁止把下列 Rust 形式作为最终用户 API：

```rust
fn on_bar(&mut self, hbt: &mut impl Bot<MD>, ctx: &mut StrategyCtx, bars: &BarBatch)
```

`s` 是一个 Numba `jitclass` 策略对象，统一封装：

- Rust Bot 的下单、撤单、订单、持仓和盘口操作；
- 全局、市场和品种上下文；
- 当前 Tick 批次或 Bar 批次的零拷贝视图；
- 全局、市场和品种级预分配策略状态；
- 当前运行时钟、事件类型、Bar 周期和关闭时间。

典型用法：

```python
@njit
def on_tick(s):
    for m in range(s.num_markets):
        for i in range(s.num_instruments(m)):
            inst = s.instrument(m, i)
            trades = s.trades(m, i)
            if inst.n_trades > 0:
                s.instrument_state(inst.asset_no)[0] = trades[inst.n_trades - 1].px

@njit
def on_bar(s):
    for i in range(s.num_bars):
        item = s.bar(i)
        if s.bar_timeframe == 60_000_000_000:
            signal = item.bar.close / item.bar.open - 1.0
            s.instrument_state(item.asset_no)[1] = signal
```

普通 Python 只负责配置、创建 Bot、声明订阅、触发首次编译和启动/停止。事件循环开始后，
不得在热路径返回 Python 解释器。事件队列、时钟、行情状态、撮合及 `_run_loop` 的所有权
全部在 Rust；Numba 只执行事件回调，策略模块不得定义或驱动 `_run_loop`。

`on_tick(s)` 每次接收一个全局 TickBatch，而不是每品种分别回调。批次中的每条记录携带
整数 `asset_no`，同一批次可以包含多个市场和品种。Rust 使用 `wait_next_feed` 推进到下一
批同时间行情，批次包含成交、BBO、深度快照和深度增量，而不只是 `last_trades`。

生命周期和交易事件使用稳定的数值事件 ID。当前已接线接口包括 `on_start(s)`、
`on_stop(s)`、`on_order(s)`、`on_filled(s)`、`on_position(s)`、`on_funding(s)`、
`on_timer(s)` 和 `on_error(s)`。预留固定事件槽位允许以后增加回调而不改变单参数 ABI。
Rust 必须保证
`on_stop(s)` 恰好调用一次，包括启动或中途回调失败的情况。

## Bar 数据模型与时间语义

Bar 只描述市场时间和数值，不包含 `available_ts`：

```rust
#[repr(C)]
pub struct Bar {
    pub open_ts: i64,
    pub close_ts: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub quote_volume: f64,
    pub buy_volume: f64,
    pub trade_count: u64,
    pub flags: u64,
}
```

所有周期采用半开区间 `[open_ts, close_ts)`。时间恰好等于 `close_ts` 的成交属于下一根
Bar。策略何时看见 Bar 属于 Feed/Runtime 的投递规则，不属于 `Bar` 数据结构：

- 普通 Bar 回测默认在 `close_ts` 投递；
- 延迟回测由独立的 Feed latency model 决定投递时间；
- 实盘在本地 Builder 最终关闭 Bar，或交易所确认 Candle 消息实际抵达时投递；
- 精确实盘回放可以在事件信封中保存 `recv_ts`，但不得写入 `Bar`；
- 策略可读取 `s.now` 与 `bar.close_ts`，但不能读取 `bar.available_ts`。

## 回测模式

### Bar-only

输入可以直接是已经完成、排序并带周期元信息的 OHLCV 文件。引擎直接跳到下一根
`close_ts`，填充 Bar 批次并调用 `on_bar(s)`，不运行固定 1 ms 帧轮询，也不扫描逐笔成交。

默认 `bar_matching="next_open"` 的调用顺序为：

```text
Bar N 在 T 关闭
  -> Bar N 对策略可见
  -> on_bar(s)
  -> 接受策略在 T 发出的订单
  -> 订单最早由 Bar N+1 撮合
```

除显式选择 `signal_close` 外，禁止使用刚关闭的 Bar N 判断 `on_bar(s)` 中新订单是否成交。
Bar-only 必须使用显式撮合模型，例如 `NextOpen`、`SignalClose`、`Touch`、
`ConservativeOhlc` 或 `VolumeLimited`，不能复用 L2 队列位置模型。

首个实现采用保守 `NextOpen`：Bar N 回调提交的订单最早在 Bar N+1 的 open 处理；市价单
按 open 成交，限价买单仅在 `open <= limit`、限价卖单仅在 `open >= limit` 时成交。该模型
不读取下一根 Bar 的 high/low，未成交 GTC 保留，IOC/FOK 在第一个可执行 open 后失效。
多周期输入只使用全局最小周期作为执行时钟，其他周期只产生信号；订单记录提交时的
`eligible_after`，不得使用开始时间早于该值的 Bar 撮合。`BAR_EMPTY`（包括 NaN 或合成
空 Bar）不参与撮合。

### `bar_matching` 参数

`run_event_bot(data_mode="bar", ...)` 通过 `bar_matching` 显式选择 Bar 撮合和成交价语义：

| 参数值 | 市价单成交价 | 限价单判定与成交价 | 典型用途 |
| --- | --- | --- | --- |
| `next_open` | 下一根可执行 Bar 的 `open` | 下一根 open 穿过限价时按 open 成交 | 默认、保守且避免同 Bar 前视 |
| `signal_close` | 产生信号 Bar 的 `close` | close 穿过限价时按 close 成交 | 与 same-close Bar 引擎进行结果对齐 |
| `touch` | 下一根符合条件 Bar 的 `open` | high/low 触及后按订单限价成交 | 乐观 OHLC 限价撮合 |
| `conservative_ohlc` | 下一根符合条件 Bar 的 `open` | 既触及且 close 穿过限价后按订单限价成交 | 保守 OHLC 限价撮合 |

```python
run_event_bot(
    data_mode="bar",
    bars=bars,
    bar_matching="signal_close",
    on_bar=on_bar,
    on_filled=on_filled,
)
```

`signal_close` 是明确的 same-close 回测假设：策略先读取完整收盘 Bar，再以该 close 成交。
除非执行场景真实提供收盘集合竞价或等价保证，否则它包含普通连续交易无法实现的同 Bar
成交假设，不应当作为实盘可成交性证明。为防止时间倒流，该模式要求 `feed_latency=0` 且
`entry_latency=0`；`response_latency` 可以非零，只影响策略收到成交回报的时间。成交价会
进入统一执行层，用于订单事件、持仓、现金、手续费和账户结算，而不是仅在导出结果时替换。

`volume_participation` 只控制 OHLC 撮合的成交量参与上限。默认值仍为
`bar_matching="next_open"`；终止平仓是独立且固定的生命周期规则。

### 固定的终止平仓规则

Bar 回测在数据结束、调用 `on_stop(s)` 之前，固定将每个非零持仓按该资产最后一根可执行
Bar 的 `close` 全量平仓。该生命周期规则不可关闭，也不提供策略参数：

```python
run_event_bot(
    data_mode="bar",
    bars=bars,
    bar_matching="signal_close",
)
```

终止平仓不是 `on_stop` 中的策略下单。引擎生成 reduce-only market order，并依次经过交易所
账户、手续费模型、ExecutionEventProjector 以及 `on_order`、`on_filled`、`on_position` 回调；
这些事件全部完成后才调用 `on_stop`，所以 `on_stop` 看到的持仓为零。平仓成交时间和价格均
使用最后一根可执行 Bar 的 `close`。无最后有效 Bar 的非零持仓必须 fail-fast，不得使用
NaN、空 Bar 或虚构价格。无行情延迟时成交时间为该 Bar 的 `close_ts`；存在 feed latency
时，成交时间取 `close_ts` 与最后一次 Bar 投递时间的较大值。

若最后一根 Bar 存在 feed latency，成交投递时间不得早于该 Bar 的实际投递时间，避免时钟
倒退。固定平仓规则用于与 Nautilus 的结束生命周期对齐；Bar 撮合价格仍由
`bar_matching` 独立控制。

### Tick-only

输入为成交、BBO、深度快照或深度增量，继续使用订单簿和 Tick/L2 撮合模型。策略订阅
Bar 时必须显式附加预处理 Bar Feed 或 `CanonicalBarBuilder`，不得由策略上下文隐式聚合。

### Hybrid

预处理 Bar 负责信号，Tick/Depth 负责盘口和订单撮合。两种数据由 Rust 事件调度器合并，
同一时间 `T` 的确定性顺序为：

1. 处理 `event_ts < T` 的市场事件；
2. 处理并投递逻辑时间恰为 `T` 的既有 Tick/订单回报；
3. 发布 `[T-period, T)` 的 Bar 并调用 `on_bar(s)`；
4. 接受策略在 `T` 发出的订单；
5. 后续成交只由 Tick execution source 决定。

多品种同一周期、同一 `close_ts` 应组成一个批次，只调用一次 `on_bar(s)`。行情缺失不能
把多个周期合并成一根 Bar。空 Bar 是生成显式空记录还是省略，必须由订阅配置确定。

## 实盘模式

### Tick

连接器通过共享内存 IPC 将归一化成交、BBO 和深度事件交给 Rust `LiveBot`；Rust 填充
Tick 上下文后，由已经编译的 Numba 循环直接调用 `on_tick(s)`。

### 本地 Canonical Bar

Rust `StreamingBarBuilder` 从归一化逐笔成交增量更新 OHLCV。每笔成交只执行常数次运算，
不保存整根 Bar 的所有成交，也不由 Python 聚合。Builder 可以按交易所时间加 watermark，
或按本地接收时间归桶。已经关闭并交给策略的 Bar 不得因迟到成交而再次触发或静默修改。

同一个 Rust Builder 必须同时支持：

- 离线物化：把历史 Tick 一次性转换成回测 Bar 文件；
- 在线增量：实盘逐笔生成完全相同定义的 Bar。

### 交易所原生 Candle

连接器把交易所 Candle 归一化为 Bar。未确认更新只刷新暂存状态；只有 confirmed/closed
消息触发 `on_bar(s)`。断线恢复必须按 `(asset, timeframe, open_ts)` 去重，使用 REST 补齐
缺失的已关闭 Bar，并且不能重复触发已经交给策略的 Bar。

`CanonicalLocal` 与 `VenueNative` 必须是显式的 Bar 来源模式。默认不得让回测使用交易所
历史 Candle、实盘却使用本地逐笔聚合，而不向用户说明语义差异。

## Numba 与 Rust 性能约束

采用以下执行方向：

```text
Python 启动层
  -> Rust 数据源、事件队列、时钟和撮合循环
  -> 稳定 C ABI 单指针回调
  -> Numba on_tick(s) / on_bar(s) / on_filled(s)
```

要求：

- 回调必须是 `@njit` Dispatcher，且只能有一个参数；
- 禁止 object mode、`objmode` 和 Python closure；
- Rust 上下文使用 `#[repr(C)]`，Python dtype 在导入时校验 size、alignment 和 field offset；
- Tick、Bar、订单和状态通过指针加长度暴露为 Numba `carray` 零拷贝视图；
- 缓冲区启动时预分配，回调内不得创建 Python list/dict 或临时对象；
- symbol 在初始化时映射为整数 `asset_no`，热路径不得查字符串；
- JSON/Serde 只允许出现在采集和转换边界；
- Bar-only 回测采用事件跳转，不得按固定 frame 空转；
- Tick、Bar、Hybrid 分别编译专用事件循环，模式选择只在启动时发生一次；
- 同一时间戳的事件批量处理，减少 Rust/Numba 边界调用次数。
- Numba 回调异常必须转换为非零 ABI 错误，依次触发 `on_error(s)` 和 `on_stop(s)`；禁止
  打印 `Exception ignored` 后继续运行。

策略订单操作写入预分配 POD command buffer，回调返回后由 Rust 同步消费。第一阶段支持
submit 与 cancel；不支持 modify，修改订单必须显式 cancel/replace。`wait=True` 不允许用于
事件回调，避免 Numba 回调重新进入或阻塞 Rust 调度器。
`on_error` 与 `on_stop` 只允许撤单，不允许提交新订单。

全局 TickBatch 使用显式 `max_tick_batch` 上限；达到上限时 Rust 终止运行并报告 overflow，
不得无界扩容、静默丢 Tick 或拆批改变同一帧语义。生产环境应结合连接器环形队列容量、
`frame_interval` 与监控指标设置该值。

## 历史数据访问

每个 `(asset_no, timeframe_ns)` 使用固定容量、启动时分配的 Rust 环形缓冲。策略通过
`s.open(asset_no, timeframe_ns)`、`s.high(...)`、`s.low(...)`、`s.close(...)` 和
`s.volume(...)` 获得零拷贝序列：

```python
@njit
def on_bar(s):
    opens = s.open(0, 60_000_000_000)
    if len(opens) >= 2:
        previous_open = opens[-1]
        two_bars_ago = opens[-2]
```

当前 `on_bar` 的 Bar 在回调返回后才提交历史，因此回调内 `[-1]` 严格表示前一根已关闭
Bar，杜绝当前 Bar 重复出现。负索引采用 Python 语义；非负索引从当前保留窗口中最老的
Bar 开始计数。正式的 `s` 便捷属性语法可后续优化，但这套索引语义不得改变。

## 数据文件要求

Bar/Tick 文件必须自描述，至少包含：

```text
schema_version
data_kind = bar | tick
symbol
venue
timeframe_ns       # Bar 文件必需
timestamp_unit = ns
interval_semantics = [open, close)
bar_source = canonical-local | venue-native
builder_version    # canonical-local 必需
```

NPZ 可以作为交换和归档格式；大规模回测应允许未压缩 NPY 或专用二进制格式通过 mmap
读取。加载阶段完成 schema、排序、周期、重叠、品种和撮合能力检查。

## 兼容和迁移

仓库历史提交 `2c10c3a`、`dd23908` 和 `c8f1ee7` 曾实现单参数 Numba 回调、两级 Rust
上下文和内存布局校验，可以作为恢复绑定层的参考。提交 `7490201` 删除了 Python binding，
因此不能把历史代码当作当前可用功能。

已完成的迁移顺序：

1. 恢复一个最小 Python/Numba binding crate，并恢复 Rust/Python ABI 布局校验；
2. 定义显式 `Bar`、`Tick`、`Hybrid` 数据源和能力校验；
3. 从当前 `StrategyCtx::fill` 与帧循环中移除隐式 Bar 聚合；
4. 实现 Bar-only 事件跳转和独立 Bar 撮合模型；
5. 实现 Hybrid 事件合并，并在 Tick 数据提前结束时 fail-fast；
6. 实现 Rust canonical live Bar builder 的 watermark/迟到/空 Bar 规则；
7. 将交易所 Candle、canonical Bar 与 REST recovery 归一化并按显式优先级去重；
8. 保留 Rust `Strategy` trait 作为内部测试或兼容入口，但文档默认示例改为 Numba
   `on_tick(s)` / `on_bar(s)`。

## 验收标准

- 同一份逐笔数据经过离线和流式 Builder，产生完全相同的 Bar；
- Bar-only 回测不进入 1 ms 帧循环；
- `on_bar(s)` 中产生的订单不能在刚关闭的 Bar 中成交；
- 整点成交、空周期、行情断档、多周期同时关闭均有确定性结果；
- 多品种同周期同时间只调用一次 `on_bar(s)`；
- 实盘迟到成交不修改已发布 Bar；
- 断线补齐不重复触发；
- 回测与实盘使用同一个 `@njit def on_tick(s)` / `@njit def on_bar(s)` 策略文件；
- 稳态热路径不进入 Python 解释器，并提供与纯 Rust 策略的基准对比。
