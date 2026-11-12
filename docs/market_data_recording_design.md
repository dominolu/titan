# 行情录制存储格式 v1：可调网格 Top-N CSR + tick 偏移

状态：**设计基线 rev.4，已实现并完成编译服务器验证**（2026-11-09）。本地 Rust `collector` 实现涵盖 book、trade、aux、恢复、回放、质量报告与 FAIL 回测门禁。性能和容量实测口径及未覆盖范围见[验收摘要](verification/2026-11-09/market-data-recording/README.md)；吞吐/体积章节中的容量预算仍是预算值，不能替代该摘要中的合成负载结果。rev.4 在 rev.3 基础上补充 collector 数据范围：合约 = 多档深度 + 逐笔成交 + openInterest + 资金费 + 爆仓明细；现货 = 多档深度 + 逐笔成交。book 格式仍只承载深度，trade/aux 独立存储。本文与仓库现有 Rust `collector`（gzip 文本 → hftbacktest NPZ）是并行的两套格式，见第 18 节。

---

## 1. 目标与非目标

### 目标

1. 以 **可调时间网格**（默认 100 ms）和 **可调深度窗口**（默认 Top-100）录制 L2 盘口，供 100 ms 级回测使用。
2. 只存窗口净变化，块内自包含，支持按块随机定位；单块损坏只影响一个块。
3. 价格与数量全程整数化，不经过 f64 还原价格。
4. 明确断档、重同步、序号代次语义；任何缺口都显式记录，不静默补造。
5. 写入崩溃安全：每块校验、可扫描恢复、原子发布；中间坏块不拖累后续好块。
6. 网格锚定 UTC 墙钟，跨市场、跨重启保持同一相位，便于跨市场回测。
7. 回测端可直接在稠密阶梯上增量推进，不需要逐槽解析文本或物化快照对象。
8. 合约额外记录 openInterest、资金费、爆仓明细；现货只记录深度 + 逐笔成交；trade/aux 独立事件流。

### 非目标

1. 不保存进入窗口之前的完整无限深度，也不保存逐消息深度原始流；逐笔成交单独记录（见 5.3），不进入 CSR 块。
2. 不统一不同交易所的原生序号空间；只在同一 `session_epoch` 内做连续性校验。
3. 不保证可重建队列位置或逐笔撮合所需的微观顺序；本格式的净差分语义见第 10 节。
4. 不承诺从净差分数据离线证明“槽内多条消息之间”的无缺口；该层由录制端在线校验并写入质量标记（见 9.2、14 节）。
5. 不把 openInterest、资金费、爆仓明细塞进 book 格式；它们走独立 aux 事件流，见 5.4。

---

## 2. 术语

| 术语 | 含义 |
| --- | --- |
| 网格 `grid_ms` | 盘口状态推进的最小时间格，默认 100 ms；槽边界锚定 UTC 墙钟 |
| 窗口 `depth_levels` | 每侧保留的档位数，默认 100；实际记录取 `min(depth_levels, depth_available)` |
| `feed_mode` | 行情形态：`diff`（增量流）或 `snapshot`（整包快照流，如 Hyperliquid l2Book） |
| tick 偏移 `off` | `abs_tick - base_tick + 32768`，`u16`，`0..65535` |
| `abs_tick` | 绝对 tick 序号（价格 / 最小变动价，整数） |
| 段 segment | 一个 `.tnmd` 文件；默认 1 小时；任何 gap / epoch / 元数据变化都切新段 |
| 块 block | 段内独立可解压单元，默认 60 s；块首带全量快照 |
| 槽 slot | UTC 墙钟网格区间 `[k*grid, (k+1)*grid)`，所有市场共用同一相位 |
| 净差分 | 槽末窗口与上一槽末窗口逐档比较后只写差异，不做“逐消息最后一条”折叠 |
| `session_epoch` | 录制代次；每个段唯一，gap/重启/重同步/元数据变化时递增 |
| `trade` | 逐笔成交事件流；NDJSON gzip，按小时切分，不进入 CSR |
| `aux` | 合约 openInterest/资金费/爆仓事件流；NDJSON gzip，现货不产生 |
| 槽关闭水位 | 已确定不会再写入的槽边界；迟到消息只能落入当前开放槽 |

---

## 3. 设计原则

1. **块内自包含**：块首快照 + 块内增量。任意块可独立重建，无需从段头重放。
2. **槽末净差分**：每槽只比较“槽初窗口”与“槽末窗口”，不逐消息推导窗口进出，避免删除不存在档位和同一档位反复进出造成的非法记录。
3. **整数精度**：价格用 `tick_units` 与 `price_decimals`；数量用 `qty_step_units` 与 `qty_decimals`。禁止 `f64` 价格参与 tick 对齐与比较。
4. **可调参数进段头**：`grid_ms`、`depth_levels`、`feed_mode` 等写入段头，回放端按实际值工作。
5. **断档有记录且切段**：gap 控制块固定是新段首块，段头记录新 epoch，避免“段内 gap 与段级 epoch 唯一”的冲突。
6. **校验即格式**：每块 CRC、每段头 CRC、索引可重建；坏块显式标记，不静默跳过，也不因中间坏块丢弃后续好块。
7. **压缩率让位于正确性**：本格式每天只有 0.3–1 GB 量级，不为省 20% 体积牺牲窗口/断档语义；压缩优化（列内差分、字节平面）后置到 v2。

---

## 4. 可调参数（核心）

### 4.1 主参数

| 参数 | 默认 | 允许范围 | 作用 |
| --- | --- | --- | --- |
| `grid_ms` | **100** | 10–1000（可到 60000，受槽数上限约束） | 时间网格；决定槽数、记录数、回测推进粒度 |
| `depth_levels` | **100** | 1–500 | 目标窗口档数；实际记录 `min(depth_levels, depth_available)` |
| `feed_mode` | `diff` | `diff` / `snapshot` | 按市场覆盖；Hyperliquid l2Book 用 `snapshot` |
| `block_duration_ms` | 60000 | 5000–65000 | 块时长；须为 `grid_ms` 整数倍，且 `block_duration/grid ≤ 65535` |
| `segment_duration_ms` | 3600000 | 60000–86400000 | 段时长；gap/epoch/tick 变化时提前切段 |
| `rebase_threshold_ticks` | 30000 | 1024–32767 | 价格偏离 base 超过该值则提前结束块并换 base；必须 `< 32768` |
| `qty_encoding` | `u16_escape` | `u16_escape` / `u32` | 变化数量的列编码；快照恒为 `u32` |
| `best_columns` | `false` | `false` / `true` | 是否额外存每条记录的最优 offset；仅用于加速/校验 |
| `compression` | `zstd` | `zstd` / `none` | 块压缩 |
| `compression_level` | 3 | 1–19 | ZSTD 级别 |
| `compression_dictionary` | `none` | `none` / `per_market` | 同一市场的块共享字典（可选） |
| `fsync_interval_blocks` | 1 | 1–60 | 每 N 块 fsync 一次数据与索引 |
| `idle_flush_ms` | 2000 | 500–10000 | 无消息时按本地时钟关闭空闲槽的间隔 |
| `external_snapshot_interval_ms` | 0（关） | 0 / ≥60000 | 独立的 60 s 全量快照（对账用） |
| `validation_interval_ms` | 60000 | 0 或 ≥1000 | 回放/录制对账周期；0 关闭 |
| `rest_validation_log_messages` | 2048 | 0 或 256–65536 | 最近 delta 消息日志条数，用于 REST 影子盘口对账；0 关闭 |
| `rest_validation_min_samples` | 5 | 0–1000 | 每市场每日有效 REST 影子对账样本下限；低于此值质量报告记 WARN |
| `gap_policy` | `skip` | `error` / `skip` / `fast_forward` | 回测遇到 gap 的默认行为；`error` 仅用于严格校验 |
| `emit_mode` | `changes` | `changes` / `grid` / `snapshot_copy` | 回测输出模式，影响吞吐与分配 |
| `oi_poll_ms` | 60000 | 1000–3600000 | 合约 openInterest 轮询间隔；仅交易所无 OI 推送时使用 |
| `event_segment_hours` | 1 | 1–24 | trade/aux 事件文件切分粒度（UTC 小时） |
| `include_bbo` | `false` | `false` / `true` | 原始采集是否保留独立 BBO 流；存储格式不单独存 BBO |
| `retention_hot_days` | 30 | 7–365 | 热数据保留 |
| `retention_cold_days` | 365 | 30–3650 | 冷数据保留 |
| `legacy_npz_bridge` | `false` | `false` / `true` | 是否额外导出 hftbacktest Event NPZ |
| `daily_archive` | `false` | `false` / `true` | 录制进程在 UTC 日切后自动把前一天数据打包为单个 zip |
| `daily_archive_prune` | `false` | `false` / `true` | 归档校验通过后删除已归档的数据文件（quality 报告保留） |
| `daily_archive_grace_secs` | 120 | 10–3600 | UTC 00:00 后等待多久再归档，留给最后一小时段/事件文件封口 |

### 4.2 参数约束与交互

- `block_duration_ms % grid_ms == 0`；`block_duration_ms / grid_ms ≤ 65535`。
- `rebase_threshold_ticks < 32768`，否则 `u16` 偏移会溢出。
- 命名统一：`depth_cap` 是**配置**里的交易所/订阅能力上限；`depth_available` 是**段头**记录的该段行情源实际覆盖档数；`depth_requested` 是段头记录的目标档数；`depth_recorded = min(depth_requested, depth_available)`。配置里不写 `depth_available`，段头里不写 `depth_cap`。
- `depth_available < depth_requested` 时记录质量标记，不宣称已覆盖目标深度。
- `grid_ms`、`depth_levels`、`feed_mode`、`qty_encoding` 是**段级常量**；段头记录实际值。跨段改变允许，但数据集 manifest 必须声明；回放端遇到参数变化发 `SegmentStart` 事件。
- 修改 `grid_ms`/`depth_levels`/`feed_mode` 不得在旧段尾部追加；必须切新段。
- `external_snapshot_interval_ms` 必须是 `validation_interval_ms` 的整数倍（或 0）。
- `gap_policy` 只影响回放；无论取值，gap 事件都必须进入回放事件流和结果 manifest，不允许静默丢弃。

### 4.3 交易所深度、形态与订阅

| 市场类型 | `feed_mode` | `depth_cap` / 订阅 | 说明 |
| --- | --- | --- | --- |
| Bybit V5 线性/反向永续 | `diff` | 1 / 50 / 200 / 500（无 100） | 按 [Bybit V5 Orderbook](https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook)，目标 100 档应订阅 200 或 500，记录前 100；`depth_available` 记 200/500 |
| Binance USD-M / COIN-M | `diff` | 1000（REST 快照上限） | WS 为 diff，需本地重建 + REST 快照；100 ms 批量推送 |
| Binance 现货 | `diff` | 1000 | `@depth@100ms`，同上 |
| OKX 永续/现货 | `diff` | 400 | 按订阅 books 档数；100 ms 批量推送 |
| Hyperliquid `l2Book` | `snapshot` | 20 | 整包快照、无原生序号、更新间隔通常大于 100 ms；100 ms 网格多数为空槽，不得声明 100 档 |

- 订阅档数取“≥ `depth_levels` 的最小受支持档位”；实际记录档数仍为 `depth_levels`。
- REST 初始快照是有限档盘口；后续 diff 对快照中不存在的价位发送零数量删除时，录制端按幂等删除忽略。槽末净差分仍只写已知窗口的变化，回放遇到文件内删除缺档仍判为数据错误。
- Binance/OKX 的强制定时推送与 UTC 墙钟网格不保证同相；一个槽可能落入 0 或 2 个批次，由净差分吸收，不影响正确性，只影响每槽变化数分布。
- 净差分格式省略无盘口变化的槽，因此落盘槽序号只能证明单调性；每条原生更新之间的连续性由行情适配器在线校验，断链时必须切分 gap 段并重新取快照。
- Binance diff 首次接入以 `U <= snapshot_seq + 1 <= u` 桥接 REST 快照；USDⓈ-M 首条桥接后按 `pu == previous_u` 校验后续更新，不能对每条更新重复套用首次 `[U,u]` 桥接条件。
- `feed_mode=snapshot` 的市场不做序号连续性校验，质量标记中必须写明。

---

## 5. 数据范围与目录布局

### 5.1 采集范围（合约 vs 现货）

| 数据 | 合约（永续/期货） | 现货 | 存储 |
| --- | --- | --- | --- |
| 多档深度 | 必需 | 必需 | `book/`（CSR 块） |
| 逐笔成交 | 必需 | 必需 | `trade/`（NDJSON gzip） |
| openInterest | 必需 | 不采集 | `aux/`（`kind=oi`） |
| 资金费 | 必需 | 不采集 | `aux/`（`kind=funding`） |
| 爆仓明细 | 必需 | 不采集 | `aux/`（`kind=liquidation`） |
| BBO | 不单独存储 | 不单独存储 | 由 `book/` Top-N 推导；原始采集如保留仅用于校验 |

规则：

- 合约/现货由市场元数据 `instrument_type` 决定；同一 collector 可混合运行，按市场配置。
- 逐笔成交与 aux **不进入 CSR 块**，独立事件流；book 表只用于深度。
- 交易所不提供公开爆仓流时，该项标记 `unsupported`，不得伪造空事件，质量报告必须显式声明。
- 合约的 OI/资金费/爆仓缺失或过期属于质量问题（WARN/FAIL），现货不要求 aux。
- `include_bbo=false` 时原始采集不订阅独立 BBO；需要 BBO 时从 book 顶部推导。保留 BBO 流时只作为校验/延迟研究，不作为第二套权威盘口。

### 5.2 目录布局

```text
<root>/
  book/
    venue=bybit/instrument=perpetual/symbol=BTCUSDT/date=2026-11-08/
      segment-20261108T000000Z.tnmd
      segment-20261108T000000Z.idx        # 可选加速索引，可由 .tnmd 重建
  trade/
    venue=bybit/instrument=perpetual/symbol=BTCUSDT/date=2026-11-08/
      trades-20261108T00.ndjson.gz
  aux/
    venue=bybit/instrument=perpetual/symbol=BTCUSDT/date=2026-11-08/
      aux-20261108T00.ndjson.gz
  snapshot/                               # 可选，独立对账快照，按市场/天聚合
  dataset.json                            # 导出时生成的输入 manifest（可选）
```

规则：

- 一个 book 段文件只属于一个 `market_id` + 一个 `session_epoch`；epoch 在每个段唯一。
- book 段默认按 UTC 整点切分；**任何 gap、重同步、writer 重启、epoch/精度变化都立即封段**，新段从切换时刻开始，不要求与整点对齐。
- **gap 控制块固定是新段的第一个块**；不存在“段内中部 gap 块”。
- 文件名使用 UTC 起始时间；`date` 目录用起始 UTC 日期。
- `.idx` 只是加速缓存，不是唯一真源；缺失或损坏时扫描 `.tnmd` 重建。
- trade/aux 按 `event_segment_hours`（默认 1 小时）切分，gzip 追加；顺序读，不做块索引/CRC。
- trade/aux 文件被强杀时可能留下不完整 gzip 尾；读取端只接受到最后一个完整换行，截断尾部记质量 WARN，不静默补造。
- trade/aux 每次轮转写同名 `.summary.json`：`records`、`first_exch_ts_ns`、`last_exch_ts_ns`、`truncated_tail`、`sha256`；丢失时由 quality 扫描 gzip 重建，summary 不是唯一真源。

### 5.3 trade 格式（逐笔成交）

每行一个 JSON，UTF-8，gzip 追加；价格/数量用**十进制字符串**，不经过 f64：

```json
{"exch_ts_ns":172...,"local_ts_ns":172...,"side":"buy|sell|unknown","price":"123.45","qty":"0.001","trade_id":"123456"}
```

- `exch_ts_ns`：交易所成交时间（ns，录制端归一化）；`local_ts_ns`：本地接收时间。
- `side`：主动方（aggressor）；交易所不提供时填 `unknown`，不猜测。
- `price`/`qty`：十进制字符串，单位按市场元数据（base/quote）；`trade_id` 为交易所原生成交 ID，缺失填 `null`。
- 不保存交易所原始 JSON；字段已覆盖回测需要。若需法证保留，另开原始归档，不默认双写。
- 现货与合约都写 trade；合约的爆仓成交不混入普通 trade，走 `aux/liquidation`。

### 5.4 aux 格式（合约 OI / 资金费 / 爆仓）

每行一个 JSON，含 `kind`：

```json
{"kind":"oi","exch_ts_ns":172...,"local_ts_ns":172...,"value":"12345","unit":"contracts|base|quote","source":"stream|rest"}
{"kind":"funding","exch_ts_ns":172...,"local_ts_ns":172...,"rate":"0.0001","next_funding_ts_ns":172...,"mark_px":"...","index_px":"...","source":"stream|rest"}
{"kind":"liquidation","exch_ts_ns":172...,"local_ts_ns":172...,"side":"buy|sell","price":"...","qty":"...","notional":"...","order_id":"...","status":"...","source":"stream"}
```

- OI 的 `unit` 按交易所实际口径标注；不得把 contracts、base、quote 混为同一字段。
- 资金费 `rate` 为十进制字符串；`mark_px`/`index_px` 仅在交易所同时提供时写，不自行计算。
- 爆仓明细保留方向、价格、数量、名义价值（可得时）、订单 ID/状态；交易所只给聚合爆仓时，字段按实际提供，缺失填 `null`，不伪造逐笔。
- OI/资金费默认**仅在值变化或超过 `oi_poll_ms` 时写**；爆仓逐事件写。去重不改变质量报告中的源更新计数。
- `source=rest` 的记录必须带交易所响应时间与本地接收时间；轮询失败不写空记录，计入 aux 缺口。

### 5.5 各交易所数据源（实现前按当前公开文档核对）

| 交易所 | openInterest | 资金费 | 爆仓明细 |
| --- | --- | --- | --- |
| Binance USD-M | REST `/fapi/v1/openInterest` 轮询 | `@markPrice@1s`（`r`、`T`，走 `/market` WebSocket 路由） | `<symbol>@forceOrder`（走 `/market` WebSocket 路由） |
| Binance COIN-M | REST `/dapi/v1/openInterest` 轮询 | `@markPrice@1s` | `!forceOrder@arr` |
| Bybit | `tickers.{symbol}`（若含 OI）或 REST `/v5/market/open-interest` 轮询 | `tickers.{symbol}`（`fundingRate`、`nextFundingTime`） | `allLiquidation.{symbol}`（订阅名待核对） |
| OKX | WS `open-interest` 或 REST `/api/v5/public/open-interest` | WS `funding-rate` | WS `liquidation-orders` |
| Hyperliquid | `activeAssetCtx` / `webData2`（待核对） | `activeAssetCtx`（`funding`） | 公开爆仓流待确认；无则标 `unsupported` |

- OI 轮询是 REST 调用，必须计入限频与退避；默认 `oi_poll_ms=60000`，按交易所权重调整。
- 合约专属；现货不订阅上述任何通道。
- 具体字段名/频道名在实现阶段按交易所版本核对并写进配置/映射表，不在格式层硬编码。

## 6. 精度与坐标系

### 6.1 价格

段头存：

- `price_decimals: u8`：价格十进制小数位（交易所元数据）。
- `tick_units: u64`：最小变动价，单位 `10^-price_decimals`。

换算：

```text
price_scaled = price_decimal × 10^price_decimals      // 用十进制字符串解析，禁止浮点乘
abs_tick     = price_scaled / tick_units              // 必须整除；否则拒绝该消息并计数
price_decimal = abs_tick × tick_units / 10^price_decimals   // 输出时按整数格式化
```

块内偏移：

```text
off = abs_tick - base_tick + 32768        // u16，合法 0..65535
abs_tick = base_tick + off - 32768
```

- 除 32768 的 bias 是**格式常量**，不是可调参数。
- `base_tick` 由块首快照选取，见 12.2；所有快照档位与块内变化必须落在相对 base 的 `[-32768, +32767]` 内，`rebase_threshold_ticks` 提供提前切块的余量。

### 6.2 数量

段头存：

- `qty_decimals: u8`：数量十进制小数位。
- `qty_step_units: u64`：数量步长，单位 `10^-qty_decimals`（这是**步长**，不是某个档位的数量）。

换算：

```text
qty_scaled = size_decimal × 10^qty_decimals     // 十进制精确
qty_steps  = qty_scaled / qty_step_units        // 必须整除；否则判精度错误；存储的就是这个“步数”
size_decimal = qty_steps × qty_step_units / 10^qty_decimals
```

- 精度错误（不整除、非法字符）计数并触发重同步，不四舍五入。
- 变化列 `val` 与快照列存的是 `qty_steps`（数量步数），不是十进制数量。
- 变化列 `val` 语义：`0` = 删除；`1..65534` = 步数；`65535` = 转义，真实值在 `escape_qty: u32[]` 中按出现顺序读取。
- 快照列用 `u32` 步数，不转义；超出 `u32` 视为异常并重同步。

---

## 7. 段格式

### 7.1 段头（固定 128 字节，小端）

| 偏移 | 大小 | 字段 | 类型 | 说明 |
| --- | --- | --- | --- | --- |
| 0 | 4 | `magic` | `[u8;4]` | `"TMR1"` |
| 4 | 2 | `version` | `u16` | 1 |
| 6 | 2 | `header_len` | `u16` | 128 |
| 8 | 4 | `market_id` | `u32` | catalog 内部编号 |
| 12 | 4 | `flags` | `u32` | bit0 已封段；bit1 有外部快照引用；bit2 录制端在线序号连续性已通过；其余保留 |
| 16 | 8 | `segment_start_local_ns` | `i64` | 本段开始（含 gap 起点）的本地接收时钟 |
| 24 | 8 | `segment_start_exch_ns` | `i64` | 本段开始（含 gap 起点）的交易所时钟 |
| 32 | 8 | `segment_end_local_ns` | `i64` | 打开中为 0 |
| 40 | 8 | `segment_end_exch_ns` | `i64` | 打开中为 0 |
| 48 | 8 | `tick_units` | `u64` | 价格步长 |
| 56 | 1 | `price_decimals` | `u8` | 价格小数位 |
| 57 | 1 | `qty_decimals` | `u8` | 数量小数位 |
| 58 | 2 | `grid_ms` | `u16` | 时间网格 |
| 60 | 2 | `block_duration_ms` | `u16` | 块时长 |
| 62 | 2 | `depth_requested` | `u16` | 配置目标档数 |
| 64 | 2 | `depth_available` | `u16` | 行情源实际覆盖档数 |
| 66 | 2 | `venue_id` | `u16` | 交易所编号 |
| 68 | 4 | `session_epoch` | `u32` | 本段代次；全库单调递增 |
| 72 | 8 | `qty_step_units` | `u64` | 数量步长（不是档位数量） |
| 80 | 4 | `encoding_flags` | `u32` | bit0：1 = `u16_escape`，0 = `u32` 数量列；bit1 `best_columns`；bit2 字典；bit3 及以上保留 |
| 84 | 1 | `feed_mode` | `u8` | 0 = diff，1 = snapshot |
| 85 | 3 | `reserved0` | `[u8;3]` | 0 |
| 88 | 32 | `symbol_utf8` | `[u8;32]` | NUL 填充；超长用 catalog 别名 |
| 120 | 4 | `header_crc32c` | `u32` | 覆盖字节 0–119 |
| 124 | 4 | `reserved1` | `u32` | 0 |

### 7.2 封段与索引

段关闭时：

1. 写入每块索引表；表头 `u32 entry_count` + `u32 crc32c`。
2. 每块索引 64 字节：

| 偏移 | 大小 | 字段 |
| --- | --- | --- |
| 0 | 8 | `block_off` |
| 8 | 4 | `block_len` |
| 12 | 4 | `payload_crc32c` |
| 16 | 8 | `start_local_ns` |
| 24 | 8 | `end_local_ns` |
| 32 | 8 | `start_exch_ns` |
| 40 | 8 | `first_seq` |
| 48 | 2 | `n_records` |
| 50 | 2 | `flags`（bit0 块不可读/CRC 失败；其余保留；gap 由 `block_kind` 区分） |
| 52 | 4 | `n_changes` |
| 56 | 1 | `block_kind` |
| 57 | 3 | `reserved` |
| 60 | 4 | `reserved2` |

3. 写段尾：`u32 index_crc` + `u64 index_off` + `u32 index_len` + `u32 magic "TMID"` + 8 字节保留（共 28 字节，补 4 字节对齐到 32）。
4. 回写段头 `flags.sealed`、`segment_end_*`，fsync 文件与目录。
5. 导出阶段计算整段 SHA-256 写入 `dataset.json`，不放进段内。

索引定位：按 `start_exch_ns` 二分找到块；块内按记录时间线性推进。**坏块在索引中置 flags.bit0，读取端跳过并产生 gap 事件，不阻断后续块。**

打开中的段使用同名 `.idx` 侧车文件加速启动：32 字节头（`magic "TMDX"`、`version u16`、`header_len u16`、`entry_count u32`、`reserved u32`、`header_crc32c u32`、`reserved2 u64`、`reserved3 u32`）后跟与封段索引相同的 64 字节条目。每写一块追加条目并按 `fsync_interval_blocks` 刷盘。`.idx` 可由 `.tnmd` 重建，不作为权威。

### 7.3 崩溃与坏块恢复

- 写入使用 `segment-*.tnmd.tmp`，封段后 rename；打开中的段允许尾部不完整。
- **尾部不完整块**：从偏移 128 顺序扫描，遇到第一个不完整/越界块时，只截断该块及其后的尾部；截断区间记为 gap（`truncated_tail`），由新段 gap 块记录。
- **中间坏块**：若块头 `TMB1`、`block_len` 合理但 `payload_crc32c` 失败，标记该块不可用，**按 `block_len` 跳到下一块继续扫描**；后续好块保留。
- **块头损坏**：从当前偏移按 8 字节对齐向后搜索下一个合法 `TMB1`（header CRC 通过、block_len 合理），中间区间标记为坏块/gap；只有搜索到文件末尾仍无合法块时才截断。
- 坏块区间在读取时产生显式 gap 事件；因为每个数据块自带快照，坏块之后的好块仍可独立重建，丢失范围仅限坏块自身。
- 尾部截断的 gap 区间由段头 `segment_end_*` 与最后一个完整块 `block_end_*` 的差推导；若段头未封（end=0），以最后完整块 end 为界并标记 `open/truncated`，不猜测缺失内容。
- `.idx` 缺失/截断/条目指向坏块时，均可扫描 `.tnmd` 重建；索引永远可由段内容重建。

---

## 8. 块格式

### 8.1 块头（固定 128 字节，小端）

| 偏移 | 大小 | 字段 | 类型 | 说明 |
| --- | --- | --- | --- | --- |
| 0 | 4 | `magic` | `[u8;4]` | `"TMB1"` |
| 4 | 1 | `kind` | `u8` | 0 数据块，1 gap 控制块 |
| 5 | 1 | `block_flags` | `u8` | bit0 块首快照，bit1 发生 rebase，bit2 gap 边界，bit3 迟到事件被钳制，bit4 存在饱和的 `n_msgs`，bit5 payload 是 ZSTD 帧；其余保留 |
| 6 | 2 | `reserved0` | `u16` | 0 |
| 8 | 4 | `block_len` | `u32` | 含块头、控制说明与尾部填充，8 字节对齐 |
| 12 | 4 | `market_id` | `u32` | 与段头一致 |
| 16 | 8 | `block_start_local_ns` | `i64` | 槽边界或 gap 起点 |
| 24 | 8 | `block_start_exch_ns` | `i64` | 槽边界或 gap 起点 |
| 32 | 8 | `block_end_local_ns` | `i64` | 提前切块时小于 start+block_duration |
| 40 | 8 | `block_end_exch_ns` | `i64` | 同上 |
| 48 | 8 | `base_tick` | `i64` | 本块偏移基准 |
| 56 | 8 | `seq_base` | `u64` | 块打开时已处理的最后原生序号（快照点）；diff feed 有效；snapshot feed 为 0 |
| 64 | 2 | `n_slots` | `u16` | 本块覆盖的网格槽数 |
| 66 | 2 | `n_records` | `u16` | 有变化的记录数（≤ n_slots） |
| 68 | 4 | `n_changes` | `u32` | 变化总数 |
| 72 | 4 | `n_escapes` | `u32` | 数量转义值个数 |
| 76 | 2 | `n_bid` | `u16` | 块首快照买档数 |
| 78 | 2 | `n_ask` | `u16` | 块首快照卖档数 |
| 80 | 4 | `snapshot_off` | `u32` | 相对块起点 |
| 84 | 4 | `records_off` | `u32` | 相对块起点 |
| 88 | 4 | `changes_off` | `u32` | 相对块起点 |
| 92 | 4 | `escapes_off` | `u32` | 相对块起点 |
| 96 | 4 | `payload_crc32c` | `u32` | 覆盖 `[128, block_len)` |
| 100 | 4 | `seq_last_processed_delta` | `u32` | 块关闭时最后处理序号 − `seq_base`；diff feed 有效；snapshot feed 为 0 |
| 104 | 2 | `control_reason` | `u16` | 仅 gap 块 |
| 106 | 2 | `control_len` | `u16` | gap 说明字节数 |
| 108 | 8 | `control_last_seq` | `u64` | 断档前最后序号（未知 0） |
| 116 | 8 | `control_next_seq` | `u64` | 重同步后首序号（未知 0） |
| 124 | 4 | `header_crc32c` | `u32` | 覆盖字节 0–123 |

约束：

- 数据块起止落在**UTC 墙钟网格**上：`block_start_exch_ns % (grid_ms × 1e6) == 0`；`block_end = start + n_slots × grid`。
- 数据块 `n_slots ≥ 1`；gap 块 `n_slots = 0`。
- `block_len` 8 字节对齐；各 section 偏移也 8 字节对齐。
- 块之间允许存在无消息区间：块 i+1 的 start 可以大于块 i 的 end（交易所无消息，状态延续）；这不是 gap，gap 只由 11.1 触发并有控制块。
- gap 块允许出现在段首；数据块不得早于其前的 gap 块结束时间。

### 8.2 数据块布局

```text
[0..128)             块头
[snapshot_off..)     快照段（见 9.1），8 字节对齐
[records_off..)      记录列（见 9.2），8 字节对齐
[changes_off..)      变化列（见 9.3），8 字节对齐
[escapes_off..)      转义数组，8 字节对齐
padding              到 block_len
```

### 8.3 gap 控制块

- `kind = 1`，`n_slots = 0`，`n_bid = n_ask = n_records = n_changes = 0`。
- `block_start/end` = 断档区间；`block_start` 与段头 `segment_start_*` 一致（即新段从断档起点开始）；`control_last_seq`/`control_next_seq` 记录断档前后原生序号。
- **说明文本位置**：紧跟在 128 字节块头之后，占 `control_len` 字节；`block_len = align8(128 + control_len)`，不足补零；`payload_crc32c` 覆盖 `[128, block_len)`。文本只写错误类别、区间和脱敏摘要，不写内部地址、凭据或账户标识。
- gap 块固定是新段第一个（或前几个）块；数据块从 gap 结束后的下一个 UTC 网格边界开始。

---

## 9. 数据块内部布局

### 9.1 块首快照（每块一次）

顺序：

| 顺序 | 内容 | 类型 | 说明 |
| --- | --- | --- | --- |
| 1 | `bid_off` | `u16[n_bid]` | 买档，按 `abs_tick` 从高到低（最优在前） |
| 2 | `ask_off` | `u16[n_ask]` | 卖档，按 `abs_tick` 从低到高（最优在前） |
| 3 | `bid_qty` | `u32[n_bid]` | `qty_steps`，均 > 0 |
| 4 | `ask_qty` | `u32[n_ask]` | `qty_steps`，均 > 0 |

- 各部分之间补零到 8 字节；`u32` 数量数组前补齐到 4 字节对齐；快照档数 = 当前窗口实际档数，可小于 `depth_available`（行情初期）。
- **快照语义按序号，不按到达时间**：快照 = 处理完所有原生序号 `≤ seq_base` 的消息后的窗口状态；`seq_base` 写入块头，为块打开时已处理的最后序号。这样迟到/乱序消息不会与“交易所时间 < 块起点”的定义冲突（见 10.4）；块内空槽的消息进度由 `seq_last_processed_delta` 记录，供跨块离线校验。
- 对 `feed_mode=snapshot`，快照 = 块起点时最近一次整包快照状态；`seq_base = 0`。
- 快照用于块内重建起点和跨块一致性校验，**不替代**第 14 节的外部对账。

### 9.2 记录列

顺序：

| 顺序 | 内容 | 类型 | 说明 |
| --- | --- | --- | --- |
| 1 | `row_ptr` | `u32[n_records+1]` | CSR 行指针，指向变化索引 |
| 2 | `local_lag_us` | `i32[n_records]` | 该槽最后一条消息的本地接收 − **钳制后**交易所时间（微秒）；迟到事件不保留原始交易所时间差 |
| 3 | `seq_last_delta` | `u32[n_records]` | 该槽最后一条消息的原生结束序号 − `seq_base` |
| 4 | `seq_first_prev_delta` | `u32[n_records]` | 该槽第一条消息的原生前驱序号 − `seq_base`；snapshot feed 填 `0xFFFFFFFF` |
| 5 | `exch_delta_ms` | `u16[n_records]` | 该槽最后一条消息的钳制交易所时间相对块起点的毫秒差；严格递增 |
| 6 | `n_msgs` | `u16[n_records]` | 该槽折叠的原生消息条数（snapshot feed 为快照条数）；饱和为 `0xFFFF` 表示 ≥65535，置 `block_flags.bit4`，不截断为假计数 |
| 7 | `best_bid_off` | `u16[n_records]` | 可选，`encoding_flags.bit1=1` 时存在 |
| 8 | `best_ask_off` | `u16[n_records]` | 可选 |

- 列顺序按“4 字节列在前、2 字节列在后”排列，保证 4 字节列在 8 字节对齐的 section 内按 4 字节对齐；section 末尾补齐到 8 字节。
- `slot = exch_delta_ms / grid_ms`，由时间差推导；同一块内记录按 `slot` 严格递增，每个槽最多一条记录。
- **离线连续性**：净差分会省略无变化槽，因此相邻落盘记录的 `seq_first_prev_delta` 可以大于上一条记录的 `seq_last_delta`；回放按单调性校验记录序号，并验证块尾 `seq_last_processed_delta` 不早于块内最后记录。每条原生更新之间的连续性由录制端在线校验，断链时写入 gap 并重新取快照。对严格“每条消息序号 +1”的交易所，可用 `n_msgs` 辅助验证槽内消息数（`n_msgs=0xFFFF` 的饱和记录跳过该验证）。段头 `flags.bit2` 表示在线校验是否通过，质量标记说明离线可证边界，不宣称完全离线可证。
- snapshot feed 无原生序号：`seq_last_delta = 0`、`seq_first_prev_delta = 0xFFFFFFFF`；回放跳过序号校验。
- 若某条记录的 `seq_last_delta`/`seq_first_prev_delta` 或块尾 `seq_last_processed_delta` 将超过 `u32` 或等于哨兵 `0xFFFFFFFF`，录制端必须结束当前块；下一块重新取 `seq_base`，不得截断或回绕。
- 无显式前驱字段的场所（例如 Bybit 的 `u`）用上一已处理消息的结束序号作为 `seq_first_prev`，此时录制端在线连续性检查也退化为单调性检查，不等同于原生链证明。由于落盘格式省略无变化槽，质量报告的稀疏记录序号检查标记为 `monotonic_only`；原生链检查结果由段头 `flags.bit2` 表示。
- 槽内无净变化则不写记录；回放按网格推进时状态自动延续。
- 迟到事件的归槽规则见 10.4，不再使用“早于块起点钳制到槽 0”的旧规则。迟到事件的 `local_lag_us` 基于钳制时间，原始时间差只计入 `late_events` 指标，不进入回放时钟。

### 9.3 变化列（CSR 的 col/val）

| 顺序 | 内容 | 类型 | 说明 |
| --- | --- | --- | --- |
| 1 | `col` | `u16[n_changes]` | tick 偏移（含 32768 bias） |
| 2 | `val` | `u16[n_changes]` | 0 删除；1–65534 数量；65535 转义 |
| 3 | `side_bitmap` | `u8[ceil(n_changes/8)]` | bit i：0 买，1 卖 |
| 4 | `escape_qty` | `u32[n_escapes]` | 按 `val == 65535` 出现顺序取值 |

- 当 `encoding_flags.bit0 = 0`（`qty_encoding = u32`）时，第 2 列改为 `val: u32[n_changes]`（0 删除，其余为数量）；`col` 后补齐到 4 字节对齐（0 或 2 字节），不写 `escape_qty`，`n_escapes = 0`；其余列不变。两种编码由段头唯一决定，同一段内不得混用。
- 同一记录内变化按 `(side, abs_tick 升序)` 排序后写入，提升列压缩率并保证确定性。
- 记录 i 的变化区间 = `[row_ptr[i], row_ptr[i+1])`。
- **净差分保证不会删除不存在的档位**：录制端只对“上一槽末窗口存在、当前槽末窗口不存在”的档位写 `val=0`。若回放遇到删除不存在档位，仍视为数据错误。

---

## 10. 槽、净差分与时间语义

### 10.1 墙钟网格

- 槽边界锚定 UTC 墙钟：`slot_global = floor(clamped_exch_ts_ns / (grid_ms × 1_000_000))`。
- 所有市场、所有段、所有重启共用同一相位；跨市场回测可以直接按 `slot_global` 对齐。
- 块起点取 `slot_global × grid_ms` 的墙面时间；段起点不必对齐，但数据块必须对齐。
- Binance/OKX 的 100 ms 定时推送相位可能与墙钟相差半个槽，这不影响正确性；净差分仍然只比较槽末状态。

### 10.2 槽末净差分（替代逐消息折叠）

录制端维护：

- 完整盘口 `F`（由 session 维护，含序号校验）；
- 上一槽末窗口 `W_prev`（每侧 Top-N，初始为块首快照）。

槽关闭时：

1. 从 `F` 取当前窗口 `W_now`（每侧 Top-N，按价格排序）。
2. 对每侧做差：`W_prev` 有、`W_now` 无 → 删除；`W_prev` 无、`W_now` 有 → 插入；两边都有且数量不同 → 更新；数量相同 → 不写。
3. 输出记录（若差异非空），`W_prev = W_now`。

这样：

- 同一档位在槽内“先进后出”不会产生非法的删除记录，因为槽末比较的是窗口状态；
- 同一档位多次变化只写最终净变化；
- 不再需要“按本地接收顺序最后一条为准”的折叠规则，CPU 也更省（每槽一次窗口计算，而不是逐消息推导窗口事件）。

### 10.3 块边界

- 块首快照在 UTC 网格边界采集；块内变化只属于本块。
- 块打开时确定 `seq_base` = 已处理消息的最后序号；块首快照 = 该序号对应的窗口状态。块关闭时把最后处理序号与 `seq_base` 的差写入 `seq_last_processed_delta`。
- 任何 rebase、gap、epoch 变化都先结束当前块；跨块不携带 delta 状态。
- 块之间允许无消息区间（见 8.1），回放按时间跳跃并保持状态。

### 10.4 迟到、乱序与槽关闭水位

消息按**原生序号顺序**处理；交易所时间只用于归槽，不用于重排状态。

1. 对每条消息计算 `clamped_exch_ts = max(prev_clamped_exch_ts, raw_exch_ts)`，保证槽序号非递减。
2. `raw_slot = floor(raw_exch_ts / grid)`；`slot = floor(clamped_exch_ts / grid)`。
3. 若 `slot` 小于 `current_open_slot`，钳制到 `current_open_slot`，并置 `block_flags.bit3`、累加 `late_events`。
4. 若 `slot` 大于 `current_open_slot`：在应用该消息前，按顺序关闭 `[current_open_slot, slot)` 的槽（关闭过程见 10.2），每个槽使用当时已处理消息的窗口状态；然后打开 `slot` 并应用消息。
5. 空闲刷新：若 `idle_flush_ms` 内无消息，按本地时钟关闭已过期的开放槽（截止 `floor((local_now - idle_flush_ms) / grid)`）；此后到达的迟到消息钳制到新的开放槽。
6. 块关闭时先关闭其覆盖的所有槽，再写记录；块首快照在开块时已确定，不随后续消息改写。已关闭槽不会被迟到消息改写。

**槽关闭水位**：`current_open_slot` 之前的槽视为已关闭（本设计不使用 grace，水位即当前开放槽）。消息 `raw_slot < current_open_slot` 即为迟到，只能进入 `current_open_slot` 及之后；其交易所时间取钳制值，避免回退。

### 10.5 不可恢复信息

- 槽末净差分丢失槽内中间状态、同价位删后重插的队列优先级、逐笔成交/订单流推断所需信息。
- 需要这些语义的回测必须另用原始逐消息归档，不能使用本格式冒充。

---

## 11. 断档、重同步与代次

### 11.1 触发条件

以下任一发生：

1. 交易所 WebSocket 断开/重连；
2. 原生序号断裂（Binance `pu != prev.u`、Bybit `u` 跳变、OKX `seqId` 断裂等，按交易所规则）；
3. 录制端主动重同步（REST/WS 快照）；
4. 窗口异常（无法解释的档位消失、窗口跨度溢出）；
5. 精度异常（tick/qty 不整除）或数量超出 `u32`；
6. 段文件尾部截断或中间坏块后恢复；
7. 写入端进程重启；
8. `feed_mode=snapshot` 的市场出现无法解析/非法整包快照。

**任何 gap 都结束当前段，并开启新段**；不写段内中部 gap 块。

### 11.2 gap 控制块与切段

- 触发 gap 时：先关闭已到期的槽并封存当前块（若已有覆盖槽）；丢弃未完成槽并计入 gap 区间；封当前段 → 打开新段，`session_epoch += 1` → 新段第一个块写 gap 控制块。
- gap 块 `block_start` = 新段起点 = 最后一个已封存槽的边界（无数据时为检测时刻对齐到的上一网格边界），`block_end` = 重同步完成、准备开数据块的时刻；数据块从 `block_end` 之后的第一个 UTC 网格边界开始。
- 允许新段开头连续多个 gap 块（例如重同步失败重试）；第一个数据块之前的所有控制块都按顺序处理。
- `control_reason` 枚举：

| 值 | 原因 |
| --- | --- |
| 1 | `disconnect` |
| 2 | `sequence_gap` |
| 3 | `resync` |
| 4 | `window_anomaly` |
| 5 | `precision_error` |
| 6 | `truncated_tail` |
| 7 | `writer_restart` |
| 8 | `bad_block` |
| 9 | `snapshot_invalid` |

- `control_last_seq` / `control_next_seq` 记录断档前后的原生序号（未知填 0）；`control_len` 说明文本紧跟块头（见 8.3）。
- 回放对 gap 的处理由 `gap_policy` 决定；默认 `skip`（跳过断档区间并在结果中标注），严格校验模式可设 `error`，研究回放可设 `fast_forward`（用重同步快照快进）。**任何模式下 gap 都必须出现在事件流和结果 manifest 中。**

### 11.3 session_epoch

- 每个段唯一 `session_epoch`，全库单调递增；gap、重同步、writer 重启、tick/qty/`feed_mode` 变化都 `+1` 并切新段。
- 同一段内 `seq_last_delta` 非递减；跨段不做连续性假设，只发 `SegmentStart(epoch)` 事件。
- 不同交易所的序号不能横向比较，也不要求跨市场统一。
- `feed_mode=snapshot` 的段不进行序号连续性声明，`flags.bit2=0`。

---

## 12. 录制端流程

### 12.1 总流程

```text
订阅深度流（venue 原生）
  → 归一化（交易所时间 ns、价格/数量十进制）
  → 按原生序号顺序维护完整盘口 + 在线连续性校验
  → 每 grid_ms 槽关闭：计算槽末窗口与上一槽末窗口的净差分
  → 追加到当前块（变化列）
  → 块满 / rebase / 块边界：封块（写块头、快照、CRC）
  → 块封存后追加 .idx 条目并 fsync
  → gap / epoch / 元数据变化：封段；新段先写 gap 块
  → 段满：封段（索引表、段尾、回写段头、rename）
```

### 12.2 网格锚定、块首快照与 base_tick

- 槽与块起点使用 UTC 墙钟网格（10.1）；段起点不必对齐。
- 开块时对当前窗口做快照：
  - `base_tick` 取当前 mid 的整数 tick（无 mid 时取 last trade tick；仍无则取最优侧）；
  - 快照 = 处理完所有序号 `≤ seq_base` 消息后的窗口状态；
  - 若最优价差本身超过偏移范围，说明市场异常，结束块并写 gap（`window_anomaly`），不写失真快照。
- **窗口跨度保护**：若快照最外档与 base 的距离超过 32767 tick（窗口跨度 > 65535 tick，常见于低单价、极小 tick 市场），`u16` 偏移无法表达。处理顺序：先按市场降低 `depth_levels`；仍超限则记录 `window_span_overflow` 质量标记并写 gap，不得截断档位后仍宣称 Top-N 完整。
- **槽内极端跳变**：若槽初窗口 `W_prev` 与槽末窗口 `W_now` 合并跨度 > 65535 tick，二者无法用同一 `base_tick` 表达。处理顺序：封旧块，以两状态跨度中点选 `base_tick` 开新块；若中点 base 仍无法同时覆盖 `W_prev`（新块首快照）与槽末变化，则丢弃该槽、写 gap（`window_anomaly`），从重同步快照重新开始。单槽跳变超过 ±32767 tick 通常意味着行情异常或数据错误，不能静默压缩/截断档位。
- 快照档位按最优到最差排序；`n_bid`/`n_ask` 可为 0（单边市场），但回测端需按市场状态处理。

### 12.3 槽末净差分实现

1. 维护每侧有序窗口（例如对 `F` 做 Top-N 选择，或维护有序索引）；每槽关闭时取一次 `W_now`。
2. 与 `W_prev` 做集合/数量差分（10.2）；只输出净变化。
3. 记录元数据取该槽最后一条已处理消息：`exch_delta_ms`（钳制值）、`local_lag_us`、`seq_last_delta`、`n_msgs`；`seq_first_prev_delta` 取该槽第一条消息的前驱序号相对 `seq_base` 的差。
4. 若某槽没有净变化，不写记录。
5. `W_prev` 在块首用快照初始化；块内跨槽延续；块边界重置为下一块首快照。

### 12.4 快照型 feed（Hyperliquid 等）

- `feed_mode=snapshot` 时，每条消息是完整窗口（如 HL `l2Book` 每侧 20 档），不维护增量序号。
- 每槽关闭时取该槽最后一次整包快照作为 `W_now`，与 `W_prev` 做同样的净差分；无快照的槽为空槽。
- `seq_base=0`；记录中 `seq_last_delta=0`、`seq_first_prev_delta=0xFFFFFFFF`；`n_msgs` = 该槽快照条数。
- 段头 `flags.bit2=0`、`feed_mode=1`；数据集质量标记写明“无原生序号、快照型、实际深度 20 档、原生更新间隔大于网格”。
- 若整包快照非法（缺档、价格不整除、单边缺失），按 11.1 触发 gap（`snapshot_invalid`），不写部分快照。

### 12.5 切块与切段条件

| 条件 | 动作 |
| --- | --- |
| 槽数达到 `block_duration_ms/grid_ms` | 封块，下一 UTC 网格边界开新块 |
| 新状态任一档超出 `base_tick ± rebase_threshold_ticks` | 在**当前槽开始处**封块（当前槽归入新块），新块用新 base 重算快照后再应用当前槽；若新 base 仍无法覆盖槽初与槽末状态，按 12.2 的极端跳变规则转 gap |
| gap 触发 | 先关闭已到期槽并封存当前块；丢弃未完成槽并计入 gap；封当前段；新段 `epoch+1`，首块写 gap 控制块；重同步后从下一个网格边界开数据块 |
| `feed_mode`/tick/qty 元数据变化 | 封块 + 封段，新段写新元数据 |
| 段达到时长/大小上限 | 封段，下一段立即开（可不在整点） |
| 进程重启 | 旧段按恢复流程收尾；新段首块写 `writer_restart` gap |
| 长时间无消息 | 空闲刷新关闭过期槽；块覆盖的槽全部关闭即封块（可只有快照、无记录）；段结束时强制封段 |

### 12.6 写盘与崩溃安全

- 每块写完后计算 `payload_crc32c`，追加到段；`fsync_interval_blocks` 为 1 时逐块 fsync（默认）。
- `.idx` 条目与块数据分文件；先写块再写索引；索引截断可由段扫描重建。
- 段文件先写 `.tmp`，封段后 rename；目录 fsync。
- 崩溃后恢复：尾部截断、中间坏块标记/重同步、gap 通过新段控制块记录；不静默接受坏块。

---

## 13. 回放端与回测接口

### 13.1 每市场阶梯

```text
bid_size: u32[65536]     // 索引 = off；qty_steps
ask_size: u32[65536]
bid_occ:  u32[2048]      // 占用位图，1 bit/tick，用于 O(1) 附近找最优
ask_occ:  u32[2048]
best_bid: u16
best_ask: u16
```

- 应用变化：写 `bid_size[off]`/`ask_size[off]`，同步更新占用位图与 best；删除 best 时用位图向下/向上找下一个非空档，不能只更新“变得更好”的情况。
- 内存：每市场约 2 × 65536 × 4 B + 2 × 8 KiB ≈ 528 KiB；23 市场约 12 MiB。
- 读取 Top-N 时从 best 沿位图/数组扫描，输出到调用方缓冲区，避免每次分配对象。

### 13.2 块重建

```text
mmap 段 → 解压块 → 校验 CRC
if gap 块: 记录 gap；按 gap_policy 处理；继续
if 坏块（索引 flags.bit0 或 CRC 失败）: 记录 gap；跳到下一块
清空阶梯 → 载入块首快照（绝对 tick 用 base_tick 还原为 off）
for each record:
    t_exch = block_start_exch + exch_delta_ms × 1e6
    t_local = t_exch + local_lag_us × 1000
    应用 row_ptr 区间内的变化
    if emit_mode == grid: 在槽边界发出状态视图（不复制数组）
    else if emit_mode == changes: 仅在有变化时发出
    else if emit_mode == snapshot_copy: 物化 Top-N（明确计入成本）
block 结束: 校验下一块快照与本块末状态逐档一致（跨 gap/坏块跳过）
```

- 主时间轴 = 钳制后的交易所时间；延迟建模用 `local_lag_us`。两种时钟都暴露给回测。
- 多市场回放：按段/块的 `start_exch_ns` 做 k 路归并；块内按记录推进；块之间允许时间跳跃（无消息区间）。
- **不物化快照**是速度前提：策略通过只读视图/游标读取阶梯；如果引擎只能吃“每 100 ms 一份 200 档快照对象”，必须显式选择 `snapshot_copy` 并接受分配成本，否则 10–50 倍加速不成立。

### 13.3 回测接口建议

```text
trait BookSource {
    fn next(&mut self) -> Event;             // SegmentStart / Record / Gap / End
    fn view(&self, market: MarketId) -> BookView;  // 只读窗口视图
}
struct BookView { best_bid(), best_ask(), size_at(side, off), top_n(side, n, out: &mut [Level]) }
```

- `Record` 事件携带 `market_id`、`exch_ts`、`local_ts`、`slot`、`gap` 标记。
- `Gap` 事件携带原因、区间与坏块/序号信息；不允许静默继续；结果 manifest 汇总所有 gap。
- `gap_policy=skip` 只影响是否中止回测，不影响 gap 事件的存在。

### 13.4 实现前置验证（必须先做）

在完整实现前，先用 **1 个市场、1 天数据** 做原型，验证：

1. 目标回测引擎能否接受增量视图（`BookView`/游标），而不是每槽一份快照对象；
2. 若只能接受快照对象，`snapshot_copy` 的真实成本是否仍满足 5× 目标；
3. 解压 + CRC + 应用变化的实际吞吐；
4. 多市场归并是否成为新瓶颈。

**该验证不通过时，本格式的“回放快 10–50×”前提不成立，应先调整回测接口，而不是继续扩大录制端实现。**

---

## 14. 校验与对账

| 层级 | 方法 | 频率 | 失败处理 |
| --- | --- | --- | --- |
| 块 | `payload_crc32c` + `header_crc32c` | 每次读取 | 标记坏块；产生 gap；继续后续块 |
| 块间 | 块 i 末状态 vs 块 i+1 首快照逐档比较（跨 gap/坏块跳过） | 每块 | 报错；定位块/档位；默认中止回测 |
| 段间 | 段边界状态与外部快照/下一段首快照比较 | 每段 | 报错；标记 gap |
| 序号（离线） | 跨记录 `seq_first_prev_delta[i] == seq_last_delta[i-1]`（i≥1）；同 epoch 相邻数据块 `next.seq_base == prev.seq_base + prev.seq_last_processed_delta`；严格 +1 场所再用 `n_msgs` 验证槽内条数 | 每次回放 | 报错；标记序号不连续；不能静默修复 |
| 序号（在线） | 录制端逐消息校验原生链（Binance `pu`、OKX `prevSeqId`、Bybit `u`） | 每条消息 | 触发 gap / 重同步；写入质量标记 |
| 外部快照（启用 `external_snapshot_interval_ms > 0` 时） | 独立全量快照与重建阶梯逐档比较 | `validation_interval_ms` | 报错；记录快照序号/时间 |
| 交易所 REST | 影子盘口：REST 快照 + 近期 delta 日志重建到主盘口当前序号，再逐档比较 | 抽检/每 5 min | 报错；样本不可用时丢弃并计数 |
| 精度 | tick/qty 整除、数量/价格范围 | 每条消息 | 计数；超阈值触发重同步 |

对账要点：

- **块间快照一致性只能验证编码/解码和窗口差分**，因为两者来自同一本地盘口；它抓不到录制端维护盘口本身的错误。**外部 REST 抽检不能省。**
- **REST 对账不使用“序号恰好等于某条消息结束”的严格匹配**：Binance 的 `lastUpdateId` 常落在某条事件的 `[U,u]` 区间内，严格相等命中率低。改用**影子盘口**：
  1. 录制端保留最近 `rest_validation_log_messages` 条 delta 日志（每条含原生 `prev`/`seq` 与变化）。
  2. 收到 REST 快照（序号 S）后，用快照重建影子窗口；日志中找覆盖 S 的第一条消息（要求 `prev ≤ S < seq`），按序应用后续消息，直到主盘口当前已处理序号 L（若 S 领先于本地，等待本地处理到 S）。
  3. 在 L 处把影子窗口与主盘口窗口逐档比较；L 是确定存在的共同序号，不需要 REST 序号恰好落在消息边界。
  4. 日志中找不到覆盖 S 的消息、期间跨 gap/epoch、S 早于日志起点或等待超时，丢弃样本并计数。
- 影子盘口只保留 REST 覆盖的窗口，不需要为每条消息保存完整状态环；内存由 `rest_validation_log_messages` 与每条 delta 数决定，按市场压测后再定容量。
- Bybit `u` 逐条递增，适用该流程；**OKX REST 是否带可对齐序号列入待确认项 6**；若无序号，OKX 只做 BBO/单边抽检或跳过，不伪装成逐档对账。
- 对账只统计**实际完成比较的样本数、通过数、失败数**；不可比样本丢弃并计数，不设“接受率”指标。每日有效样本少于 `rest_validation_min_samples` 时，在质量报告中记为“证据不足”警告，而不是判通过。
- 对账要求保留可复现输入（块文件、快照文件、REST 响应摘要、配置、版本号）；结果只输出类别与定位信息，不回显私有账户/地址数据。

---

## 15. 采集质量评估

目标：每天、每市场给出一份**可复核的结论**，不做评分、不做加权、不新增文件格式字段。质量是数据可用性的汇总，不是另一套存储设计。

### 15.1 产出

- 每市场每天一个 `quality.json`，由汇总任务读取段头、块索引、gap 控制块和对账日志生成。
- 生成质量报告时必须传入对应的录制配置，`rest_validation_min_samples` 按配置生效。
- 数据集导出时把各市场状态汇总进 `dataset.json`；不回写 `.tnmd` 段文件。
- 状态只有三档：`PASS` / `PASS_WITH_WARNINGS` / `FAIL`。

### 15.2 判定规则（只保留一套，避免阈值蔓延）

| 结论 | 条件 |
| --- | --- |
| `FAIL` | 块间快照 diff ≠ 0；diff feed 序号连续性校验失败；REST 影子盘口比较失败；合约必需数据（trade/OI/资金费/爆仓）整日缺失且交易所明确支持该数据 |
| `PASS_WITH_WARNINGS` | 有 gap（无论长短）；有坏块或尾部截断；`late_ratio` > 0.1%；有效 REST 样本 < `rest_validation_min_samples`；`depth_available < depth_requested`；`feed_mode=snapshot`；精度拒绝 > 0；trade/aux 部分缺失、尾部截断或过期；`unsupported` 的合约辅助数据 |
| `PASS` | 无 FAIL、无 WARN |

说明：

- gap 是显式、可解释的，但仍然不是 PASS：有缺口的数据不能宣称完整。质量报告必须给出 `gap_ms_ratio = gap_ms / recorded_ms` 和按 `gap_ms` 降序的 `top_gaps[]`；Binance 每 24 小时强制断连会造成日常 WARN，报告要能区分“5 秒重连”和“2 小时丢数据”，不能只给一个标签。
- snapshot feed 不是错误，只是深度/序号能力有限；默认 WARN 提醒使用者按回测目标取舍。
- WARN 不是坏数据；FAIL 数据不得进入正式回测。精度拒绝单次只记 WARN（拒绝后触发 gap/重同步，数据可用）；连续或高比例拒绝由运维告警处理，不升级为单日数据 FAIL。
- 现货不要求 OI/资金费/爆仓；合约的 `unsupported`（交易所无公开数据）记 WARN 并在报告中列出能力缺口，不伪造记录，也不与“采集失败”混为一谈。
- 不引入百分制、权重、SLA 评分或阈值矩阵。

### 15.3 报告字段

每市场每天至少输出：

| 维度 | 字段 |
| --- | --- |
| 完整性 | `segment_count`、`gap_count`、`gap_ms`、`gap_ms_ratio`、`top_gaps[]`、`bad_blocks`、`truncated_segments`、`seq_check(native_chain/monotonic_only/none)`；`trade_count`、`trade_truncated_tail`、`aux_oi_samples`、`aux_funding_samples`、`aux_liquidation_count`、`aux_unsupported[]`、`oi_max_age_ms`、`funding_last_age_ms` |
| 准确性 | `block_diff_mismatches`、`rest_samples_compared`、`rest_mismatches`、`precision_rejects`、`precision_reject_ratio` |
| 时效 | `late_events`、`late_ratio`、`lag_p50_us`、`lag_p95_us`、`lag_p99_us` |
| 覆盖 | `instrument_type`、`feed_mode`、`depth_requested/available/recorded`、`changes_per_slot_p50/p95`；`aux_sources`（stream/rest） |
| 结果 | `status`、`warnings[]`、`failures[]` |

字段只做计数与分布，不做综合评分；异常只保留类别、市场和定位，不写私有数据。

### 15.4 最小实现

- 汇总任务只读索引、段头、gap 块和校验日志，不重放全量数据；
- 块间 diff、REST 比较、序号校验在录制/回放任务中写校验日志，质量脚本只做聚合；
- `quality.json` 与数据同生命周期，保留期随数据；不做实时大屏或告警风暴。

---

## 16. 体积与性能预算

### 16.1 体积公式

```text
slots_per_day_market = 86_400_000 / grid_ms
changes_per_day      = markets × slots_per_day_market × c        // c = 每槽平均净变化数
bytes_per_day        ≈ changes_per_day × B                       // B = 待测压缩后 B/变化
blocks_per_day       = markets × 86_400_000 / block_duration_ms
depth_recorded       = min(depth_requested, depth_available)
snapshot_bytes_day   ≈ blocks_per_day × 12 × depth_recorded      // off u16 + qty u32，两侧
```

默认 `grid_ms=100`、23 市场、`block_duration_ms=60000`、`depth=100`；表中 B=3/4/5 是**待测假设值**，不是实测：

| 每槽净变化 c | B=3 B | B=4 B | B=5 B |
| --- | --- | --- | --- |
| 2 | 0.12 GB/天 | 0.16 GB/天 | 0.20 GB/天 |
| 5 | 0.30 GB/天 | 0.40 GB/天 | 0.50 GB/天 |
| 10 | 0.60 GB/天 | 0.79 GB/天 | 0.99 GB/天 |
| 20 | 1.19 GB/天 | 1.59 GB/天 | 1.99 GB/天 |
| 30 | 1.79 GB/天 | 2.38 GB/天 | 2.98 GB/天 |
| 50 | 2.98 GB/天 | 3.97 GB/天 | 4.97 GB/天 |

- 每记录固定开销（`row_ptr 4` + `lag 4` + `seq_last 4` + `seq_first_prev 4` + `exch_delta 2` + `n_msgs 2` = **20 B**，可选 best 再 +4 B）；摊到每个变化是 `20 / c`。加入 `seq_first_prev`/`n_msgs` 后，c=5 时固定开销约为旧版的 1.4 倍。
- 表中的 B=3–5 B 是**待测假设值**，不是实测：c=5 时原始约 `20/5 + 4.125 ≈ 8.1 B/变化`，压到 3–5 B 对应 ZSTD 约 1.6–2.7×，量级合理但必须实现后实测；在此之前不得把表值当容量承诺。
- **空槽不会写入记录**：公式按“每槽都有 c 个变化”线性外推，对小币种偏保守（实际 c 更低）。真正的风险是主流合约（BTC/ETH 在 100 ms 内 Top-100 净变化可能超过 10–20），不能按全局平均值规划。
- 本表**不含 trade/aux**：trade 体积 ≈ 成交笔数 × 每笔约 100–200 B 原始 JSON / gzip 4–8×；aux 为低频事件（OI/资金费按轮询或变化，爆仓按事件），需按市场实测；两者单独做容量评估，不并入 book 预算。
- 容量规划必须用**每市场、每小时的 c 分布上四分位或 p95 × 2 倍余量**，并单独压测 `grid_ms=10` 与 `depth=500` 的组合。
- 块首快照按**记录档数**计：记录 100 档约 39.7 MB/天原始（压缩后约 20–30 MB/天，+5–8%）；记录 20 档约 8 MB/天；记录 500 档约 199 MB/天原始（压缩后 +15–25%）。订阅 200 档但只记录 100 档时，按 100 档计。
- 后续压缩优化（v2，不阻塞 v1 正确性）：`col` 在记录内已排序，可存相邻差值或“距最优档距离”，再做字节平面拆分；通常比直接压 u16 更好。v1 先保证正确性与可读性。

### 16.2 性能口径

| 操作 | 目标 | 说明 |
| --- | --- | --- |
| 应用变化 | ≥ 5M changes/s（单线程 JS/TS 参考） | 数组写 + 位图更新 |
| 单市场回放 | ≥ 1M records/s | 含 CRC、解压、应用 |
| 多市场并行 | 23 市场线性扩展 | 每市场独立阶梯/解压任务 |
| 解压 | ZSTD ≥ 300 MB/s | 块级解压 |
| 相对现有解析基线 | ≥ 5× 端到端 | 同引擎、同精度、同市场；基线为项目当前加载/回放实现 |

速度收益必须标明基准与硬件；若引擎仍需物化快照，收益会明显下降（约 2–5×），见 13.4 的前置验证。

---

## 17. 保留期与归档

- 热数据：默认 30 天，可调 7–365 天；本地 SSD/NVMe。
- 冷数据：默认 365 天，可调 30–3650 天；按天合并段文件（同格式），或转 Parquet/对象存储。
- 合并不得改变语义：段头、块头、索引、`dataset.json` 的 `sha256` 和参数必须保留；合并前后做逐块 CRC/抽样回放校验。
- **每日归档（可选）**：`daily_archive = true` 时，录制进程在 UTC 日切后（默认 00:00:00 + 120 s 宽限）
  把前一天所有市场的数据打包为 `<root>/archives/<YYYY-MM-DD>.zip`。zip 内含 `manifest.json`，
  逐文件记录相对路径、字节数与 sha256；写入使用临时文件，回读校验通过后才原子重命名。
  `daily_archive_prune = true` 时，仅在归档校验通过后删除已归档的数据文件（`quality/` 保留）。
  也可用 `book archive --root <root> --date <YYYY-MM-DD> [--prune] [--overwrite]` 手动补归档。
  zip 只是传输/冷存封装：内部 `.tnmd`/`.gz` 保持原样，解压即可恢复原目录结构。
- 删除只按容量预算与显式策略执行；不得按“运行失败/不完整”静默删除，数据质量标记随文件保留。
- 目标量级：0.5 GB/天 × 365 天 ≈ 183 GB，加快照与冗余约 250–300 GB；实际按第 16.1 节 p95×2 预算复核。14 天仅适合短期验证，不适合研究型回测。

---

## 18. 兼容与版本演进

### 18.1 与现有 Rust collector

- 现有 `collector` 产出的是 **原始 gzip 文本 + `normalize` → hftbacktest NPZ**，与本格式并存。
- 新格式的 `legacy_npz_bridge = true` 时，提供离线导出器：读块 → 块首快照事件 + 块内变化事件 → hftbacktest `Event`（64 字节记录）→ NPZ，供旧引擎使用。
- 该桥接是有损的：窗口外档位、快照元数据、gap 控制信息、序号链不会全部进入 Event 流；导出器必须在 manifest 中声明“由 CSR 归档导出”。
- rev.4 的 `trade/`、`aux/` 是新增独立事件流，不与 book 段混写；旧 `normalize` 只处理旧 raw gzip，不读取新 trade/aux。需要旧 NPZ 时，trade 由独立转换器读取 NDJSON 生成 `TRADE_EVENT`；aux 不进 NPZ。

### 18.2 版本演进

- `version=1`；读取端必须拒绝未知 `major`，对未知 `encoding_flags` 位显式报错。
- 新增列/编码只能通过新 `version` 或仅追加的段级 flag；不得改变既有字段偏移。
- 数据集每次导出写 `dataset.json`：格式版本、市场列表与精度、`grid_ms`、`depth_requested/available`、`feed_mode`、时间范围、book 段/`trade`/`aux` 文件引用与 `sha256`、质量标记（gap 数/时长/占比、top gaps、坏块数、迟到事件、精度拒绝及比例、rebase 次数、序号校验级别、窗口跨度溢出、n_msgs 饱和次数、trade 完整性、aux 覆盖/过期/unsupported）、生成器版本。

---

## 19. 运维与可观测性

指标（按市场与段聚合，不上报私密内容）：

| 指标 | 用途 |
| --- | --- |
| `records_written`, `changes_written`, `bytes_written` | 体积与吞吐 |
| `changes_per_slot_p50/p95/p99` | 容量规划与编码选择 |
| `qty_escape_ratio` | 是否需要改 `u32` 编码 |
| `precision_rejects` | meta 与交易所不一致 |
| `seq_gaps`, `resyncs`, `gap_duration_ms`, `gap_ms_ratio` | 数据质量 |
| `late_events`, `late_ratio` | 槽水位/时钟质量 |
| `rebase_count`, `window_span_overflow` | 窗口质量 |
| `snapshot_diff_count`, `rest_stale_samples`, `rest_samples_compared` | 对账健康与有效样本数 |
| `bad_blocks`, `crc_failures`, `truncated_segments` | 存储健康 |
| `trade_records`, `trade_bytes`, `trade_truncated_tail` | 逐笔成交完整性/体积 |
| `aux_oi_samples`, `aux_funding_samples`, `aux_liquidation_count`, `oi_max_age_ms`, `aux_unsupported` | 合约辅助数据覆盖与过期 |
| `n_msgs_saturated`, `fsync_latency_p99`, `writer_queue_depth` | 写入压力/计数饱和 |

告警：CRC 失败、坏块、对账不一致、gap 超阈值、`late_ratio` 异常、磁盘余量低于最低值、持续精度拒绝。日志不回显凭据、真实地址或账户标识。

---

## 20. 验收门槛

实现完成后至少通过：

| 类别 | 用例/标准 |
| --- | --- |
| 正确性 | 每块末状态 = 下一块首快照（逐档 diff = 0）；抽检 REST 影子盘口 diff = 0 |
| 净差分 | 构造同槽内“进入后离开”“离开后重进”“数量多次变化”序列，输出恰为槽末净差分，不出现删除不存在档位 |
| 网格 | 23 市场同时录制，`slot_global` 对齐；重启/切段后相位不变 |
| 参数矩阵 | `grid_ms ∈ {10, 100, 1000}` × `depth ∈ {20, 100, 200, 500}` × `feed_mode ∈ {diff, snapshot}` 的格式/回放正确 |
| 精度 | tick/qty 边界、`u16` 转义（65534/65535）、`u32` 数量上限；不整除必须拒绝 |
| 漂移 | 合成极端价格移动，验证 rebase 切块与偏移范围；窗口跨度 > 65535 tick 触发 gap |
| 迟到/乱序 | 构造交易所时间回退、乱序到达、空闲刷新，验证钳制/水位/`late_events`，不回头改写已关闭槽 |
| 断档 | 断连、序号跳变、重同步、writer 重启；gap 块是新段首块，epoch 递增，默认 `skip` 不中止且结果有标注 |
| 坏块 | 人工破坏中间块（CRC/块头），验证跳到后续好块、只丢坏块区间；尾部部分块只截断尾部 |
| 序号 | 离线跨记录连续性校验通过；`feed_mode=snapshot` 正确标记为无原生序号 |
| 快照型 | Hyperliquid 20 档整包快照录制/回放；空槽、非法快照触发 gap |
| 崩溃 | `kill -9`、磁盘满、断电模拟；尾部截断/索引缺失可恢复，坏块不被接受 |
| 对账 | 连续 14 天块间 diff = 0；REST 影子盘口抽检 0 失败，且每日有效样本数 ≥ `rest_validation_min_samples` |
| 性能 | 单市场 ≥ 1M records/s、应用 ≥ 5M changes/s；端到端相对现有解析基线 ≥ 5×；先通过 13.4 前置验证 |
| 体积 | 每市场待测 `c`/`B` 与预算偏差 < 30%，容量按 p95×2 余量可容纳 |
| 数据范围 | 合约市场同时产生 book/trade/aux；现货只有 book/trade，不因缺 aux 误报 WARN |
| 逐笔成交 | 与交易所样本逐笔一致（side/price/qty/trade_id）；断尾可识别；现货与合约都覆盖 |
| OI/资金费 | 轮询/流更新、单位（contracts/base/quote）正确；仅变化或间隔写入；过期能触发 WARN |
| 爆仓明细 | 方向/价格/数量/名义/订单 ID 按交易所实际提供；无公开流时标 `unsupported`，不伪造 |
| aux 恢复 | 截断 gzip 尾、缺失 summary、轮询失败；可识别、可重扫，不静默补造 |
| 质量报告 | 每市场每日生成 `quality.json`，状态按第 15.2 节判定；FAIL 数据不得进入正式回测 |
| 保留 | 合并/冷归档后抽样逐块 CRC/回放一致；删除与质量标记审计 |

所有验证在 `titan-build` 或目标录制端专用环境执行，原始证据按仓库隐私约定私有保存；`docs/verification/` 只放脱敏摘要。

---

## 21. 风险与开放问题

| 风险 | 影响 | 缓解 |
| --- | --- | --- |
| 窗口净差分实现错误 | 回放状态漂移 | 块首快照 + 每块对账；净差分单测；外部 REST 抽检 |
| 交易所深度能力不足（HL 20 档、Bybit 无 100 档） | 无法满足 100 档回测 | `depth_available` 记录真实覆盖；按市场声明质量；目标档数取订阅档位 ≥ 请求值 |
| HL 无序号、更新间隔大于网格 | 序号校验失效、空槽多 | `feed_mode=snapshot`；质量标记写明；不宣称 100 ms 完整 |
| 窗口跨度 > 65535 tick（低单价/极小 tick） | `u16` 偏移无法表达 | 降低 `depth_levels`；仍超限则 gap + 质量标记 |
| `u16` 数量溢出 | 数据失真 | 转义 + 待测溢出率；必要时切换 `u32` 编码 |
| 净差分丢失微观顺序 | 队列位置/订单流研究不可用 | 明确非目标；另存原始逐消息归档 |
| 槽内序号链无法离线证明 | 数据完整性证明弱于逐消息归档 | 跨记录离线校验 + `n_msgs`；在线逐消息校验；质量标记声明边界 |
| 时钟异常/迟到 | 槽错位 | 序号顺序处理 + 交易所时间单调钳制 + 槽水位 + `late_events` |
| REST 影子对账依赖 delta 日志与交易所序号 | OKX 等无 REST 序号的市场无法逐档对账 | 日志容量可配；OKX 若确认无序号则只做 BBO/单边抽检或跳过，并在质量报告声明 |
| 目标回测引擎不支持增量视图 | 加速目标不成立 | 13.4 实现前置验证；不通过先改接口 |
| OI 轮询触发交易所限频 | aux 数据缺失/延迟 | 按交易所权重设置 `oi_poll_ms`，失败退避并记 WARN；优先使用 WS OI（若有） |
| 爆仓公开流缺失或字段不统一 | 合约爆仓明细不完整 | 按市场声明 `unsupported`/字段能力；不强造逐笔；质量报告显式列出 |
| OI 单位/资金费口径差异 | 跨所比较错误 | aux 记录 `unit`/`source`；按市场元数据解释，不跨所直接相加 |
| trade/aux 体积未测 | 容量规划偏差 | 影子录制后按成交笔数/事件数实测，单独预算；不并入 book 表 |

待确认：

1. 录制端已确认 Rust（Titan `collector`）；回放端是否复用同一 crate 仍取决于回测引擎形态。
2. 目标回测引擎是否支持增量视图；若只支持快照对象，速度目标需重估。
3. 各市场 `depth_cap` 的实际值（Bybit 无 100、HL 20，需按当前交易所版本复核）。
4. 是否需要队列位置/逐笔撮合级保真；若需要，本格式不能作为唯一数据源。
5. 磁盘预算与保留期最终值。
6. REST 抽检可用的交易所端点、限频与序号字段（需按市场配置）。
7. 各交易所 OI/资金费/爆仓的精确频道名与字段：Bybit `tickers` 是否含 OI、`allLiquidation` 订阅名、OKX `open-interest`/`liquidation-orders`、HL `activeAssetCtx` 与公开爆仓覆盖，均需按当前文档核对。
8. OI 轮询权重/限频与 `oi_poll_ms` 的最终值；爆仓流突发时的背压/丢弃策略。

---

## 附录 A：配置示例（占位路径，实际由部署配置提供）

```toml
[recorder]
format_version = 1
root = "/data/market"              # 占位
grid_ms = 100
depth_levels = 100
oi_poll_ms = 60000
event_segment_hours = 1
include_bbo = false
feed_mode = "diff"
block_duration_ms = 60000
segment_duration_ms = 3600000
rebase_threshold_ticks = 30000
qty_encoding = "u16_escape"
best_columns = false
compression = "zstd"
compression_level = 3
fsync_interval_blocks = 1
idle_flush_ms = 2000
validation_interval_ms = 60000
rest_validation_log_messages = 2048
rest_validation_min_samples = 5
external_snapshot_interval_ms = 0
gap_policy = "skip"
emit_mode = "changes"
retention_hot_days = 30
retention_cold_days = 365
legacy_npz_bridge = false

[recorder.venues.bybit]
depth_cap = 200                    # 订阅 orderbook.200；目标 100 档
[recorder.venues.binance_futures]
depth_cap = 1000
[recorder.venues.okx]
depth_cap = 400
[recorder.venues.hyperliquid]
depth_cap = 20
feed_mode = "snapshot"
```

按市场覆盖示例（可选）：

```toml
[recorder.markets.BTCUSDT]
depth_levels = 100
grid_ms = 100

[recorder.markets.SOLUSDT]
depth_levels = 50
```

## 附录 B：常量速查

| 常量 | 值 |
| --- | --- |
| 文件魔数 | 段 `TMR1`，块 `TMB1`，段尾索引 `TMID`，侧车 `.idx` `TMDX` |
| 段头大小 | 128 B |
| 块头大小 | 128 B |
| tick 偏移 bias | 32768（`u16`） |
| 偏移合法范围 | `[-32768, +32767]` 相对 `base_tick` |
| 数量转义值 | `65535`（`u16_escape`） |
| 序号不可用哨兵 | `0xFFFFFFFF`（`seq_first_prev_delta`，snapshot feed） |
| `feed_mode` | 0 = diff，1 = snapshot |
| 块/段对齐 | 8 B |
| 校验 | CRC32C（Castagnoli） |
| 字节序 | 小端 |
| `block_kind` | 0 数据，1 gap，其余保留 |
| gap 原因 | 1 断连，2 序号断裂，3 重同步，4 窗口异常，5 精度错误，6 尾部截断，7 写入端重启，8 坏块，9 快照非法 |
| gap 位置 | 固定在新段首部；段内无中部 gap 块 |

## 附录 C：三轮评审/需求处置记录

| 编号 | 评审问题 | 处置 | 落点 |
| --- | --- | --- | --- |
| 1 | 逐消息折叠 + 窗口进出会产生非法删除 | **采纳**：改为槽末净差分 | 10.2、12.3 |
| 2 | gap 段内写入与 epoch 切段自相矛盾 | **采纳**：gap 块固定为新段首块，epoch 每段唯一 | 5、11.2、11.3、12.5 |
| 3 | 网格锚定段起点导致跨市场错位 | **采纳**：锚定 UTC 墙钟 | 10.1、12.2 |
| 4 | 中间坏块导致后续好块被丢弃 | **采纳**：尾部截断、中间坏块跳过并向后重同步 | 7.3 |
| 5 | 折叠后无法离线验证序号连续性 | **部分采纳**：加 `seq_first_prev_delta` + `n_msgs` + 块尾 `seq_last_processed_delta`，跨记录/跨块可离线验证；槽内多消息链仍只能在线校验并显式标记 | 8.1、9.2、14、21 |
| 6 | 迟到/乱序归槽规则不完整 | **采纳**：序号顺序处理 + 交易所时间单调钳制 + 槽关闭水位 + 空闲刷新 | 10.4、11.1 |
| 7 | REST 抽检“diff=0”不可行 | **采纳（rev.3 修订）**：REST 快照 + delta 日志重建影子盘口，在共同后续序号比较，不再要求 REST 序号等于消息边界 | 14 |
| 8 | Hyperliquid 无序号、低频率、20 档 | **采纳**：新增 `feed_mode=snapshot` | 4.3、12.4 |
| 9 | Bybit 无 `orderbook.100` | **采纳**：改为 1/50/200/500，目标 100 档订阅 200 或 500 | 4.3 |
| 10 | gap 说明文本存放位置未定义 | **采纳**：紧跟 128 B 块头，`control_len` 字节，计入 `block_len` 与 CRC | 8.3 |
| 11 | 默认 `gap_policy=error` 会频繁中止研究回放 | **采纳**：默认 `skip`，gap 仍强制记录与标注；严格模式可设 `error` | 4.1、11.2、13.3 |
| 12 | 体积表未覆盖主流合约高 c 与空槽 | **采纳**：增加 c=30/50 行，明确按 p95×2、空槽不写 | 16.1 |
| 13 | 列内差分/字节平面可进一步压缩 | **后置**：v2 优化，不阻塞 v1 | 16.1 |
| 14 | 5×/10–50× 依赖增量引擎 | **采纳**：新增实现前置验证门禁 | 13.4、20 |
| 15 | REST 同序号严格匹配命中率低 | **采纳**：改为影子盘口（REST + delta 日志重建到当前序号） | 14 |
| 16 | 状态环内存与持续拷贝 | **采纳**：改为 delta 日志，按需重建；不保存每消息完整窗口 | 14、4.1 |
| 17 | 体积表 B 被称为“实测” | **采纳**：改为“待测”；补 c=5 原始约 8 B/变化与 ZSTD 1.6–2.7× 说明 | 16.1、20 |
| 18 | 槽内极端跳变无处理规则 | **采纳**：先折中 base，仍超限则丢弃该槽并写 gap | 12.2、12.5 |
| 19 | 任何 gap 都 WARN，日常断连噪音大 | **采纳**：保留三档，报告增加 `gap_ms_ratio` 与按 `gap_ms` 降序的 `top_gaps[]` | 15.2、15.3 |
| 20 | 精度拒绝一次即 FAIL 与 gap WARN 不一致 | **采纳**：精度拒绝降为 WARN，增加 `precision_reject_ratio`，连续/高比例交给运维告警 | 15.2、15.3 |
| 21 | n_msgs 溢出、索引 flags 重复、depth 命名不统一 | **采纳**：`n_msgs` 饱和 + `block_flags.bit4`；索引 flags 仅保留坏块位；统一 `depth_cap/depth_available/depth_requested/depth_recorded` 定义 | 8.1、9.2、7.2、4.2 |
| 22 | collector 范围扩展：合约深度+成交+OI+资金费+爆仓，现货深度+成交 | **采纳（rev.4）**：book/trade/aux 分工；trade/aux 独立 NDJSON；现货不产生 aux | 5、15、16 |
| 23 | trade/aux 是否塞进 CSR 块 | **采纳**：不塞入；book 只承载深度，trade/aux 顺序事件流 | 5.1、5.3、5.4 |
| 24 | 各交易所 OI/资金费/爆仓频道与限频 | **列入待核对**：按当前公开文档核对；OI 优先 WS，无 WS 才 REST 轮询；无公开爆仓流标 `unsupported` | 5.5、21 |
