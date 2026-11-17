# 行情录制格式 v1 实施技术方案（Rust / Titan collector）

状态：**实施设计稿 rev.4，已在 `collector` 落地并完成编译服务器验证**（2026-11-09）。实现和验证边界见[行情录制验收摘要](verification/2026-11-09/market-data-recording/README.md)。对应格式设计见 [行情录制存储格式 v1](market_data_recording_design.md)。本文按 **Rust + Titan 仓库现有 `collector` crate** 落地；数据范围按 rev.4：合约 = 深度 + 逐笔成交 + OI + 资金费 + 爆仓，现货 = 深度 + 逐笔成交。

---

## 1. 目标与约束

### 1.1 目标

- 在现有 `collector` crate 内新增行情归档能力：book（深度）、trade（逐笔成交）、aux（合约 OI/资金费/爆仓）的录制、恢复、回放、校验、质量汇总。
- 不新建 crate、不新建服务、不引入数据库/HTTP/页面。
- 单进程跑 23 个市场；每市场一个异步任务，book 块写入很小，不做 writer 线程池；trade/aux 复用有界队列与现有 gzip 写入。
- 代码按“写侧 / 读侧”两个方向组织，避免按 DDD 分层拆文件。

### 1.2 运行环境

- Rust 2024 edition（现有 `collector/Cargo.toml` 已是 2024）。
- Linux 目标；读取使用 `std::os::unix::fs::FileExt::read_at`，**不引入 mmap**，与格式文档的“按块随机定位”语义一致。
- 新增依赖只有：`zstd`（块压缩）、`crc32c`（Castagnoli 校验）、`toml`（配置）。其余复用 `tokio`、`clap`、`serde`、`serde_json`、`anyhow`、`thiserror`、`tracing`。`Feed` 用原生 `async fn` + 泛型静态分发，不引入 `async_trait`。

`collector/Cargo.toml` 只追加：

```toml
[dependencies]
zstd = "0.13"
crc32c = "0.6"
toml = "0.8"
```

### 1.3 文件数量控制

**7 个源文件**，不再按职责细拆 `validate.rs`、`quality.rs`、`types.rs`、`feed.rs`：

```text
collector/src/lib.rs                  # pub mod book;（约 10 行）
collector/src/book/mod.rs             # 共享类型、常量、错误（约 160 行）
collector/src/book/format.rs          # 固定偏移编解码 + CRC32C（约 360 行）
collector/src/book/segment.rs         # 段写入/读取/恢复/索引（约 500 行）
collector/src/book/recorder.rs        # 槽时钟/净差分/块组装/gap/校验（约 620 行）
collector/src/book/replay.rs          # 阶梯/回放/verify/quality（约 540 行）
collector/src/bin/book.rs             # CLI 入口（约 200 行）

collector/tests/book.rs               # 集成测试（约 600 行，含 fixture 生成）
```

说明：

- `validate` 并入 `recorder.rs`（写侧校验与录制共享 delta 日志）；
- `quality` 并入 `replay.rs`（读侧汇总，避免独立文件）；
- `feed` 不新增文件：diff 型复用现有 collector 行情任务，HL snapshot 适配放在 `bin/book.rs` 的组装层；
- `types` 并入 `book/mod.rs`；
- 不建 `book/format/`、`book/segment/` 等子目录。

若后续某文件超过约 800 行，再按“写侧/读侧”拆，不在第一版预拆。

### 1.4 现有 collector 文件改造（不新增源文件）

trade/aux 不进入 book 格式，复用现有 collector 的行情任务和写入设施：

| 现有文件 | 改造 |
| --- | --- |
| `collector/src/main.rs` | 按市场启停：深度 → book；trade → trade writer；合约再加 OI/资金费/爆仓 → aux writer；现货只开深度/成交 |
| `collector/src/{binance,binancefuturesum,binancefuturescm,bybit,hyperliquid}/*` | 复用现有 JSON 解析/订阅，增加 trade/aux 事件路由；不新建连接、不重复解析 |
| `collector/src/file.rs` | 增加 JSONL gzip 写入（trade/aux 每小时文件 + `.summary.json`），约 80 行；原有 raw gzip writer 保留 |
| `collector/src/pipeline.rs` | 复用有界队列/背压；depth/trade/aux 分别送入对应 sink |
| `collector/src/bin/book.rs` | 合约 OI REST 轮询、HL snapshot/trade/aux 适配；启动/停止整体任务 |

因此**新增源文件仍是 7 个 book 文件**；trade/aux 的代码落在现有文件里。若现有 collector 无法复用，才考虑新增 `events.rs`（+1 文件），不在第一版默认。

---

## 2. 模块职责、功能范围与代码预算

| 文件 | 职责 | 功能范围 | 不负责 | 估算 |
| --- | --- | --- | --- | --- |
| `src/lib.rs` | 库入口 | `pub mod book;` | 任何逻辑 | ~10 |
| `book/mod.rs` | 共享契约 | `Side`、`Level`、`Change`、`MarketMeta`、`SlotDiff`、`BlockMeta`、`GapReason`、常量、`BookError` | I/O、编码细节 | ~160 |
| `book/format.rs` | 固定偏移编解码 | 段头/块头/快照/记录列/变化列/索引条目；`crc32c`；tick/qty 与 offset 换算；字段校验；`n_msgs` 饱和 | 文件、压缩、时间 | ~360 |
| `book/segment.rs` | 段文件生命周期 | 临时文件、append 块、`.idx` 追加、fsync、封段回写、rename；`read_at` 读取 + 索引二分；尾部截断；中间坏块扫描/标记；索引重建 | 块内容语义、窗口差分 | ~500 |
| `book/recorder.rs` | 录制状态机 | UTC 墙钟槽、单调钳制、迟到/空闲刷新、`W_prev→W_now` 净差分、块组装（snapshot/base/seq/rebase）、gap/epoch 切段、块/段 rotate、REST 影子校验、`validation.jsonl` | 网络连接/重连（复用现有）、质量标签 | ~620 |
| `book/replay.rs` | 读侧 | 稠密阶梯（`Vec<u32>` + 占用位图）、块迭代、gap 策略、单/多市场归并、`BookView`；`verify`（块间 diff/序号）；`quality`（`quality.json`、三档、`gap_ms_ratio`/`top_gaps`） | 写文件、录制 | ~540 |
| `bin/book.rs` | CLI 入口 | `record/verify/repair/quality/replay`；配置加载；启动现有行情任务或 HL snapshot 任务、OI 轮询；退出码 | 业务逻辑 | ~200 |
| `collector/src/main.rs`、venue 模块（**修改**） | 数据路由 | 深度 → book；trade → trade writer；合约 OI/资金费/爆仓 → aux writer；现货只开深度/成交 | 盘口/编码 | ~150（改造） |
| `collector/src/file.rs`（**修改**） | trade/aux 写入 | JSONL gzip 每小时文件、`.summary.json`、fsync/轮转 | book 编码 | ~80（改造） |

实现约 **2,300 行**（book）；trade/aux 约 **300–400 行**，主要落在现有 `file.rs`/venue 任务/`bin/book.rs`。单元测试写在各自文件 `#[cfg(test)]` 中（约 1,200 行），集成测试 1 个文件（约 600 行）。不追求行数最少，而是避免无意义抽象。

---

## 3. 关键类型与接口

### 3.1 共享类型（`book/mod.rs`）

```rust
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Side { Bid, Ask }

#[derive(Clone, Copy)]
pub struct Level { pub tick: i64, pub qty: u64 } // tick=绝对 tick 序号; qty=qty_steps

pub struct MarketMeta {
    pub market_id: u32,
    pub venue_id: u16,
    pub symbol: String,
    pub feed_mode: FeedMode,        // Diff | Snapshot
    pub tick_units: u64,
    pub price_decimals: u8,
    pub qty_step_units: u64,
    pub qty_decimals: u8,
    pub depth_requested: u16,
    pub depth_available: u16,
}

pub struct Change { pub side: Side, pub tick: i64, pub qty: u64 } // qty=0 删除

pub struct SlotDiff {
    pub slot: u32,
    pub exch_ts_ns: i64,            // 钳制后的交易所时间
    pub local_lag_us: i32,
    pub seq_first_prev_delta: u32,  // 相对块头 seq_base
    pub seq_last_delta: u32,
    pub n_msgs: u16,
    pub changes: Vec<Change>,       // 已按 (side, tick asc) 排序
}

pub enum BookError { /* 20 个以内变体，thiserror */ }

pub enum NormalizedMessage {
    Depth(DepthMsg),
    Trade(TradeMsg),
    Aux(AuxRecord),
}

pub enum AuxRecord {
    Oi { exch_ts_ns: i64, local_ts_ns: i64, value: String, unit: OiUnit, source: AuxSource },
    Funding { exch_ts_ns: i64, local_ts_ns: i64, rate: String, next_funding_ts_ns: Option<i64>, mark_px: Option<String>, index_px: Option<String>, source: AuxSource },
    Liquidation { exch_ts_ns: i64, local_ts_ns: i64, side: Side, price: String, qty: String, notional: Option<String>, order_id: Option<String>, status: Option<String> },
}
```

约定：

- 原生 u64 序号在内存用 `u64`；只有相对 `seq_base` 的 u32 delta 进入文件。
- `tick` 用 `i64` 绝对 tick；编码时才转 `u16` offset。
- 不建 `Book`/`Quote`/`Snapshot` 的继承层次；`Level` 是纯数据。

### 3.2 接口

只有 1 个 trait，避免插件化：

```rust
pub trait Feed {
    async fn next(&mut self) -> Option<NormalizedMessage>;
}
```

- diff 型：现有 collector 的 Binance/Bybit/OKX 任务产生 `NormalizedMessage`；
- snapshot 型：`bin/book.rs` 内的 HL 适配器实现同一 trait；
- 若现有任务无法直接复用，只在 `bin/book.rs` 加薄适配，不改原有采集逻辑。

`SegmentWriter`/`SegmentReader`、`Ladder`、`Replay` 都是具体类型，不做 trait。

### 3.3 核心函数

```rust
// format.rs
pub fn encode_segment_header(h: &SegmentHeader) -> Result<[u8; 128]>;
pub fn decode_segment_header(b: &[u8]) -> Result<SegmentHeader>;
pub fn encode_block(input: &BlockInput) -> Result<Vec<u8>>;
pub fn decode_block(b: &[u8]) -> Result<DecodedBlock<'_>>;
pub fn encode_index_entry(e: &IndexEntry) -> [u8; 64];
pub fn crc32c(bytes: &[u8]) -> u32;

// segment.rs
impl SegmentWriter {
    pub fn create(dir: &Path, h: &SegmentHeader) -> Result<Self>;
    pub fn append(&mut self, block: &[u8], meta: &BlockMeta) -> Result<()>;
    pub fn seal(self) -> Result<()>;
    pub fn abort(self);
}
impl SegmentReader {
    pub fn open(path: &Path) -> Result<Self>;
    pub fn find_block(&self, ts_ns: i64) -> Option<usize>;
    pub fn read_block(&self, i: usize) -> Result<Vec<u8>>;
}
pub fn recover_segment(path: &Path) -> Result<RecoveryReport>;

// recorder.rs
pub async fn run_market(meta: MarketMeta, cfg: RecorderConfig, feed: impl Feed) -> Result<()>;

// replay.rs
impl Ladder { pub fn apply(&mut self, changes: &[Change]); pub fn top_n(&self, side: Side, n: usize, out: &mut Vec<Level>); }
pub fn replay_segment(...) -> impl Iterator<Item = Result<ReplayEvent>>;
pub fn verify(root: &Path, filter: &Filter) -> Result<VerifyReport>;
pub fn quality(root: &Path, date: &str) -> Result<QualityReport>;
```

不写 `AbstractCodec`、`Visitor`、`Repository`、`Service`。

---

## 4. 数据流

### 4.1 录制（写侧）

```text
现有行情任务（Binance/Bybit/OKX）或 HL snapshot 适配
  → NormalizedMessage（按 kind 路由）：
      Depth  (绝对 tick、qty_steps、原生 seq)
      Trade  (exch_ts、side、price、qty、trade_id)
      Aux    (OI / Funding / Liquidation；仅合约)

Depth → MarketRecorder：
  更新钳制时间/当前槽 → 维护现有盘口
  槽关闭：W_now = topN(book)；diff(W_prev, W_now)
  有差异：SlotDiff → encode_block
  SegmentWriter.append（块 + .idx + fsync）
  块满 / rebase / gap：封块；gap 时封段、epoch+1、新段首块 gap

Trade → trade JSONL gzip writer（每小时文件 + summary）
Aux   → aux   JSONL gzip writer（每小时文件 + summary）
  合约 OI 默认 REST 轮询；资金费/爆仓优先 WS；现货不启动 aux
```

合约与现货的差异只在路由：现货只把 Depth/Trade 送入 writer；合约额外把 Aux 送入 writer。不会为现货创建 aux 文件。

### 4.2 校验（写侧 + 读侧）

```text
写侧：REST 定时请求 → 快照序号 S
      → delta 日志重建影子窗口 → 应用至主盘口当前序号 L
      → 比较 → validation.jsonl（sample/ok/fail/stale）

读侧 verify：逐块 replay → 块间快照 diff + 序号离线校验 → VerifyReport
```

### 4.3 回放与质量（读侧）

```text
SegmentReader（索引二分）→ 逐块解压/CRC → Ladder 载入快照 → 逐记录 apply
  → ReplayEvent（SegmentStart/Record/Gap/End）
  → verify/quality 使用同一读取路径，不重复实现
  → trade/aux 顺序读 gzip JSONL + summary；不进入 Ladder
```

---

## 5. 配置与 CLI

### 5.1 配置

TOML，与格式文档附录 A 一致；只保留实际实现的键。

```toml
[recorder]
root = "/data/market"
grid_ms = 100
depth_levels = 100
block_duration_ms = 60000
compression_level = 3
fsync_interval_blocks = 1
rest_validation_log_messages = 2048
rest_validation_min_samples = 5
gap_policy = "skip"
oi_poll_ms = 60000
event_segment_hours = 1
include_bbo = false

[venues.bybit]
depth_cap = 200
[venues.hyperliquid]
depth_cap = 20
feed_mode = "snapshot"
```

### 5.2 CLI（`src/bin/book.rs`）

```text
book record  --config recorder.toml
book verify  --root /data/market --date 2026-11-08 [--market BTCUSDT]  # book + trade/aux 完整性
book repair  --segment /data/.../segment-....tnmd
book quality --root /data/market --date 2026-11-08 --config /workspace/recorder.toml
book replay  --root /data/market --market BTCUSDT --from ... --to ...   # 调试/基准
```

退出码：0 成功；2 配置/参数；3 数据校验失败；4 I/O/恢复失败。无 HTTP。

---

## 6. 错误处理与不变量

### 6.1 不变量

1. 块首快照 = `seq_base` 时刻窗口；`W_prev` 初始等于该快照。
2. 块内记录 `exch_delta_ms` 严格递增；净差分不产生“删除不存在档位”。
3. offset 在 `base_tick ± 32767` 内；超出则 rebase 或 gap。
4. 每块 CRC 正确；索引可重建；坏块不阻断后续块。
5. gap 块只出现在段首；段头 `epoch` 唯一递增。
6. `seq_base`/`seq_last_processed_delta` 单调；u32 溢出前封块。
7. 同一市场只有一个 writer；段文件只属于一个 epoch。

### 6.2 错误策略

| 情况 | 行为 |
| --- | --- |
| 配置非法/精度不可用 | 启动失败，不写半截数据 |
| 序号断裂/断连/重同步失败 | gap：封段、epoch+1、新段首块 gap，继续录制 |
| tick/qty 不整除 | 计数、触发重同步；该消息不写；质量 WARN |
| rebase 失败/窗口跨度超限 | 丢弃该槽，写 gap；不截断档位 |
| 写入/fsync 失败 | 停止该市场，进程非零退出 |
| 读取 CRC/坏块 | 产生 gap；继续后续块；verify/quality 记录 |
| 索引缺失/损坏 | 扫描段重建 `.idx` |
| 回放 gap | `skip` 默认；`error` 中止；`fast_forward` 用快照跳过 |

日志只写计数与错误类别；不写地址、凭据、账户。每段一行 summary（块数/记录数/gap/fsync 延迟）。

---

## 7. 测试要求

### 7.1 单元测试（模块内 `#[cfg(test)]`）

| 模块 | 必须覆盖 |
| --- | --- |
| `format` | 段头/块头偏移与大小；roundtrip；CRC 损坏；快照/记录/变化列布局；`u16_escape` 边界（0/65534/65535）；`u32` 分支解码拒绝；`n_msgs=0xFFFF` 饱和；snapshot 哨兵；tick/qty 整除；对齐 |
| `segment` | 写/读 roundtrip；封段索引；`.idx` 缺失重建；尾部截断；中间坏块跳过；块头损坏向后搜索；原子 rename；重复封段拒绝 |
| `recorder` | 墙钟槽/跨槽跳变/迟到/空闲刷新；净差分四类（进入、离开、重进、数量变化）；进入后离开不产生删除；正常/极端 rebase；gap/epoch；seq 溢出封块；`n_msgs` 饱和；快照型 feed；REST 影子日志缺失/跨 epoch/超时 |
| `replay` | Ladder apply vs 朴素 `BTreeMap`；删除 best 回退；`topN` 顺序；块间 diff；gap 三策略；多市场归并；坏块/截断；quality 三档/排序/空数据 |
| `trade/aux` | trade 字段（side/price/qty/trade_id）与十进制字符串；OI 单位 contracts/base/quote；资金费 rate/next time/mark/index；爆仓字段缺失填 null；OI/资金费去重；现货不启动 aux；gzip 尾截断与 summary 计数 |

单元测试不依赖真实时间、网络、真实交易所数据。

### 7.2 集成测试（`collector/tests/book.rs`）

1. 合成 1 分钟/1 市场 diff 消息 → 录制 → 回放 → 与朴素盘口逐槽 diff=0。
2. 参数矩阵 pairwise：`grid_ms ∈ {10,100,1000}` × `depth ∈ {20,100,200}` × `feed_mode ∈ {Diff,Snapshot}`；不跑全量笛卡尔。
3. 崩溃恢复：截断尾部、损坏中间块、删除 `.idx`、损坏块头；`repair` 后可读，坏块产生 gap，后续块可读。
4. gap/epoch：断连/序号跳变/重同步失败；gap 在段首、epoch 递增、三种 gap 策略。
5. REST 影子：mock 快照 + delta 日志，覆盖对齐、S 领先、S 过旧、跨 epoch；不访问网络。
6. 质量：合成段/gap/校验日志 → `QualityReport` 三档与 `top_gaps` 排序。
7. 多市场：2–3 个合成市场并发录制，回放归并时间序正确。
8. 数据范围：合约 fixture 同时产生 book/trade/aux；现货 fixture 只有 book/trade，**不创建 aux 文件**，quality 不误报。
9. trade/aux 恢复：截断 gzip 尾、删除 summary、OI 轮询失败、爆仓突发；可识别并记 WARN，不补造。
10. aux 能力：mock 某交易所无公开爆仓流，报告 `unsupported`，不生成空记录。

fixture 生成函数放在 `tests/book.rs` 内，不建额外 support crate。

### 7.3 性能与验收

- 基准用 `book replay --bench` 或独立 `benches/` 二进制，手工/构建服务器运行，不进常规 CI。
- 门槛沿用格式文档第 20 节：应用 ≥ 5M changes/s（单线程）、读取 ≥ 1M records/s、23 市场内存有界、24h 无丢块/半块。
- 测试命令：`cargo test -p collector`、`cargo test -p collector --test book`；格式/静态检查按仓库约定在 `titan-build` 执行。

### 7.4 验收映射

与格式文档第 20 节逐项对应：正确性/净差分/网格/参数矩阵/精度/rebase/迟到/断档/坏块/序号/快照型/崩溃/对账/性能/质量。映射表放 `tests/book.rs` 顶部注释，不单独维护文档。

---

## 8. 实施阶段

| 阶段 | 内容 | 退出条件 |
| --- | --- | --- |
| P0 | `lib.rs` + `book/mod.rs` + `format.rs` + `segment.rs` | 合成块 roundtrip、CRC、截断/坏块恢复通过 |
| P1 | `recorder.rs` + `bin/book.rs record` + 一个 diff feed + trade 路径 | 1 市场 1 小时录制（book+trade）；verify 块 diff=0；crash 恢复 |
| P2 | `replay.rs` + `verify` | 合成/录制数据回放；多市场归并；性能基线 |
| P3 | 23 市场、HL snapshot、合约 aux（OI/资金费/爆仓）、REST 影子、`quality` | 24h soak；合约 scope 完整、现货无 aux；quality 三档正确 |
| P4 | 参数矩阵、坏块/崩溃硬化、trade/aux 断尾/限频、运维脚本 | 格式文档第 20 节验收通过（含合约/现货数据范围） |

每阶段只合入能跑通的代码；未实现的可选项不保留空分支。

---

## 9. 最小代码约束（反过度设计）

1. 复用现有 WS/重连/快照同步/盘口/元数据/配置/日志；新增代码只在写侧与读侧。
2. 7 个源文件；不建 `core/domain/service/port` 层，不建 DI 容器。
3. 只有 1 个 `Feed` trait；其余用具体类型。
4. 状态对象只保留 `MarketRecorder`、`Ladder`、`SegmentWriter/Reader`；禁止继承/宏生成。
5. 固定偏移手写编解码；不引入 schema/IDL/prost/平化框架。
6. 不引入数据库/HTTP/前端/消息队列；quality 是 CLI 生成的 JSON。
7. 不建 writer 线程池：块写入很小，`write + fsync` 在录制任务内完成；性能不够再评估。
8. 不用 mmap/unsafe：`read_at` 足够；后续需要再单独评估 `memmap2`。
9. 不写 zstd/CRC32C 算法：用 `zstd`、`crc32c` crate。
10. 可选功能默认不实现：`u32` 数量编码、字典、`best_columns`、外部快照、NPZ 桥接；实测需要时单独小提交。
11. trade/aux 复用现有 gzip/JSONL 写入与行情任务，不为 aux 另建格式/索引/DB。
12. 一个集成测试文件；不提交真实行情；不引入 mock 框架。
13. 不优化未测量的路径：先按第 7.3 节测，不做 buffer 池/零拷贝。

---

## 10. 风险与待确认

1. **现有 collector 行情任务是否可直接复用**：若不提供稳定的 topN/原生序号，`bin/book.rs` 的适配层补 100–200 行；不重写采集。
2. **OKX REST 是否带可对齐序号**：若无，OKX 逐档 REST 对账跳过，quality 记 WARN。
3. **HL snapshot**：按 20 档、更新间隔大于网格实现；联调确认。
4. **Bybit 订阅档位**：目标 100 档订阅 200/500，`depth_cap` 按当前文档核对。
5. **回测引擎增量视图**：格式文档 §13.4 前置验证不通过，回放速度目标不成立，先改接口。
6. **B/c 体积参数**：仍为待测假设；首周影子录制后回填。
7. **REST 影子日志内存**：按 `rest_validation_log_messages` 与实际 delta 数压测后定值。
8. **OI/资金费/爆仓频道**：各交易所频道名、字段与限频需按当前文档核对；OI 优先用 WS，无 WS 才 REST 轮询；爆仓无公开流时标 `unsupported`。
9. **trade/aux 体积**：按成交笔数/事件数实测；与 book 分开预算，不混入第 16.1 节表。

---

## 11. 交付清单

- 7 个新增源文件（`lib.rs`、`book/mod.rs`、`book/format.rs`、`book/segment.rs`、`book/recorder.rs`、`book/replay.rs`、`bin/book.rs`）；trade/aux 改造现有 `main.rs`/venue/`file.rs`/`pipeline.rs`，不新增文件；
- 1 个集成测试文件 `collector/tests/book.rs`；
- 4 个业务命令 + `replay` 调试命令（共 5 个入口）；
- 新增依赖 3 个：`zstd`、`crc32c`、`toml`；OI REST 复用现有 `reqwest`；
- 不新增 crate、服务、数据库、页面、消息队列；不新增 aux 存储格式。
