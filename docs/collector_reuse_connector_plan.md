# 采集复用统一 broker 实现：优化方案

状态：**方案待批准，未实施**。本文只描述设计，不含代码改动。

## 1. 问题

采集侧 (`collector`) 与实盘侧 (`connector`) 各有一份 Binance 行情实现，互不依赖：

```
$ grep -n connector collector/Cargo.toml   → 无
$ grep -n collector connector/Cargo.toml   → 无
```

| 链路 | Binance 实现 | 序列对齐 | 状态 |
| --- | --- | --- | --- |
| 实盘 | `connector/src/binancefutures/` | `depth_pipeline.md:18` 契约，正确 | ✅ |
| 采集 | `collector/src/binancefuturesum/` | hftbacktest fork 遗留（`8dbc0489` 原样导入，从未改过） | ❌ |

采集侧 `binancefuturesum/mod.rs:72` 在**任何情况下**都推进序列基准：

```rust
*prev_u_map.entry(symbol.to_string()).or_insert(0) = u;
```

而第 45 行的缺口判断会为**每一帧**触发一次 REST 快照 `tokio::spawn`。快照返回后，`prev_u_map` 并不知道快照的 `lastUpdateId` 对齐到了哪里，基准因此永远落后于 WS 流，每个 `depthUpdate` 都判定缺口。

后果（2026-11-22 在干净出口 IP 的采集节点实测）：启动 20 秒内合约接口权重从 8 冲到 **11160**（上限 2400）；一分钟内 `session_epoch` 增长到 126，绝大多数段为 392 字节空壳。同一现象在旧节点上曾被误判为"IP 被封"，换到权重归零的干净 IP 后依旧复现，据此排除封禁假设。

对照实盘实现 `connector/src/binancefutures/market_data_stream.rs:441`，文档要求的语义是完整的：连续则发布；缺口则发 `StreamInvalidated`、**移除** `prev_u`、缓冲当前帧、拉快照；未对齐时只缓冲不触发。

`docs/depth_pipeline.md` 第 16–26 行「分层职责」早已规定：**`pu` 与 `lastUpdateId` 的对齐属于 connector，record 只负责差分存储。**

## 2. 目标与非目标

### 目标

1. 采集与实盘共用**一份** Binance 行情实现，序列对齐只有一处。
2. `record` 层职责收窄为差分存储：接收已对齐的批次，不再纠正序列。
3. 复用现有 `PublishEvent` 契约，不新建交易账本或第二套行情抽象。
4. 消除 429 风暴与空段碎片，`session_epoch` 恢复为"仅真实缺口/重启时递增"。
5. **删除 `collector/src/binancefuturesum/`**，不保留过渡实现。
6. **其他 broker（Bybit/OKX/Hyperliquid）的采集侧实现按同一契约收敛**：它们同样必须遵守
   `docs/depth_pipeline.md:18` 的序列对齐与失效语义，只是采集端本轮不接入。

### 非目标

1. 采集端本轮只接入 Binance；其他 broker 只做**代码层面的契约收敛**，不配置、不录制。
2. 不改动 `.tnmd` 存储格式、`book` 段/块语义。
3. 不引入实盘账户、下单或部署相关能力到采集机。

## 3. 依赖方向

```
现在:
  collector ──→ hftbacktest, titan-l2-book, （自己的 binance/ bybit/ okx/ hyperliquid 解析）
  connector ──→ titan-market-service, titan-account-service, titan-strategy-runtime, ...

目标:
  collector ──→ connector (default-features = false, features = ["binancefutures"])
                 └── 提供: MarketDataStream, PublishEvent, MarketStreamMetadata
  collector ──→ titan-l2-book, hftbacktest(backtest only)
```

采集侧不再保留任何交易所的原生 JSON 解析、`prev_u` 维护或快照候选管理。

### 依赖体量处理

`connector` 默认启用全部交易所：

```toml
default = ["binancefutures", "okx", "hyperliquid", "gatecrossex",
           "backpack", "lighter", "variational", "evm", "solana"]
```

采集机只需要 `binancefutures`。`collector` 以 `default-features = false, features = ["binancefutures"]`
依赖 `connector`。其他交易所 feature（含 `lighter` 的 `libloading`、`solana`/`evm` 的链上依赖）
必须能在编译期被完全排除——这是编译期约束，不是可选项：若排除不干净（例如
`titan-account-service` / `titan-strategy-runtime` 等重依赖被无条件链接），**就必须在
`connector` 内补齐 feature 隔离**，直到 `binancefutures` 单独编译不引入无关依赖为止。
不采用抽取轻量 crate 的替代路线。

**验收线**：`cargo tree -p collector` 中不出现非 Binance 交易所的依赖；`collector` 仅
`binancefutures` feature 下编译通过。

## 4. 数据流

```
Binance WS ──→ connector::MarketDataStream
                 ├─ 校验 pu == prev_u，维护 lastUpdateId 对齐（depth_pipeline.md:18）
                 ├─ 缺口 → StreamInvalidated + buffer + snapshot（私有候选）
                 └─ PublishEvent::FeedBatch { symbol, events, stream: Some(meta) }
                                │
                                ▼
                     collector 适配层（新增）
                 ├─ FeedBatch.events → DepthMessage / TradeMessage
                 ├─ StreamInvalidated → GapMessage（写入 gap 控制块，切新段、epoch+1）
                 └─ MarketStreamMetadata → 段头元数据（epoch、覆盖档数）
                                │
                                ▼
                book/recorder（现有，职责收窄）
                 └─ 槽末净差分 → .tnmd 段/块
```

采集侧**不再解析 Binance 原始 JSON**，不再维护 `prev_u_map`、`snapshot_candidates`、`pending_depth_messages`。这些全部由 `MarketDataStream` 承担。

## 5. 分层职责（对齐 `docs/depth_pipeline.md`）

| 层 | 职责 | 归属 |
| --- | --- | --- |
| 连接与重连 | 帧读取、ping、心跳、指数退避 | `collector::transport`（保留） |
| 序列对齐 | `U <= lastUpdateId <= u`、`pu == prev.u`、快照候选、缓冲 | **`connector`**（唯一） |
| 失效语义 | `StreamInvalidated`、epoch 单调递增 | **`connector`** |
| 差分存储 | 槽末净差分、块首快照、CRC、gap 块 | `collector::book`（现有） |

`collector::transport` 与 `connector` 内部的连接处理存在重复。**本轮不合并**（`titan-record` 已在使用 collector transport，改动面过大）；在文档中标注为后续项，避免同一条规则维护两份。

### 其他 broker 的同类收敛

采集端本轮只接 Binance，但 `collector/src/{bybit,okx,hyperliquid,binance}/` 下同样存在与
`binancefuturesum` 同源的旧逻辑（各自维护序列、各自拉快照）。这些必须在同一轮内处理掉，
否则删掉 Binance 那一份之后，其余几份仍是同样的隐患，只是暂时没被启用：

1. **逐一比对**各 broker 在采集侧的实现与 `connector` 对应实现（`depth_pipeline.md:18-20`）；
2. 差异之处（缺 `lastUpdateId` 区间校验、未在缺口时清空 `prev_u`、逐帧 `spawn` 拉快照）
   **在 connector 侧修正**，使 connector 成为唯一正确实现；
3. 采集侧对应的解析代码**同步删除**，不保留"暂时不用"的死代码；
4. 每个 broker 补充针对性的 connector 单元测试（构造快照落后于流、序列断裂、重复帧），
   保证契约在 connector 内被真正验证，而不是依赖采集端是否启用。

## 6. 实施阶段

### 阶段 0：依赖与契约盘点（不写生产代码）

1. 在 `titan-build` 上验证 `connector` 仅 `binancefutures` 时的编译与依赖树：
   `cargo tree -p connector --no-default-features --features binancefutures`
2. 若仍有非 Binance 交易所或被无条件链接的重依赖，在 `connector` 内补齐 feature 隔离，
   直到 `cargo tree -p collector` 干净为止——**解决，不退回**。
3. 盘点四个 broker 在采集侧与 connector 侧的序列处理差异，形成逐条对照表。

### 阶段 1：适配层与协议收敛

1. 新增 `collector/src/feed/connector.rs`：把 `PublishEvent` 转为 `book` 的 `DepthMessage` / `TradeMessage` / `GapMessage`。
2. `book.rs` 的 feed 构造改为通过适配层调用 `MarketDataStream`。
3. 按第 5 节修正各 broker 在 connector 内的序列与失效语义，补齐单元测试。

### 阶段 2：验证

1. 单元：适配层对 `FeedBatch` / `StreamInvalidated` / 元数据的映射，含缺口场景。
2. 集成：`titan-build` 上一次短时真实录制（只读公开行情，不接账户），断言：
   - 合约接口 `x-mbx-used-weight-1m` 稳定在 100 以内；
   - `session_epoch` 只在实际断流时递增；
   - 无 392 字节空壳段堆积。
3. 回归：`book` 现有 72 项测试、`connector` 全量测试（含其他 broker 新增用例）通过。
4. 全工作区 fmt / clippy `-D warnings` / 测试通过。

### 阶段 3：部署与观察

1. 构建产物按现有流程部署到采集机（`titan-build` 出产物、hash 全程校验）。
2. 启动后观察不少于 1 小时，记录权重、epoch、段大小分布。
3. 稳定后再将试跑数据纳入正式序列。

### 阶段 4：移除遗留（阶段 1 完成后即执行，不等部署）

1. 删除 `collector/src/binancefuturesum/` 及 `binance/`、`bybit/`、`okx/`、`hyperliquid/`
   中与 connector 重复的解析与序列代码，只保留 `book` 所需的类型转换。
2. 更新 `docs/depth_pipeline.md` 与 `docs/market_data_recording_design.md`，明确采集复用 connector。

## 7. 风险与回退

| 风险 | 影响 | 应对 |
| --- | --- | --- |
| `connector` 重依赖拖垮采集机构建 | 阶段 0 暴露 | 在 connector 内补 feature 隔离，直到依赖树干净；不退回轻量 crate |
| `PublishEvent` 语义为实盘设计，录制需要的字段缺失 | 适配层信息不足 | 阶段 1 先做字段盘点，缺字段时扩展事件而非绕开 |
| `connector` 只发规范化事件，不保证原始 diff 顺序 | 净差分可能丢细节 | 按 `market_data_recording_design.md:23` 语义验收：只在 `session_epoch` 内校验连续性 |
| 其他 broker 修正影响实盘链路 | 实盘行为变化 | 每处修正附单元测试；实盘链路不增加请求频率，仅补齐缺失校验 |
| 停机窗口 | 录制中断 | 阶段 3 选择在 UTC 日切后执行 |

每阶段的失败都不得进入下一阶段。

## 8. 当前现场状态

- `titan-record.service`：`inactive/dead`（采集节点已部署 `1b73ad86…8504`，未运行）
- 采集机 `recorder.toml`：`depth_available` 仍为测试值 `100`，**实施前需恢复为 `1000`**
- 新机磁盘：`9.6G`，可用 `7.8G`，长期录制前需评估容量或挂载数据盘

## 9. 已确认事项

1. **不设回退分支**：依赖问题在 `connector` 内通过 feature 隔离解决，不退回轻量 crate。
2. **采集端近期只接 Binance**，但 Bybit/OKX/Hyperliquid 的采集侧逻辑在同一轮内一并收敛，
   不在采集端保留任何"暂时不用"的旧实现。
3. **不保留 `binancefuturesum`**，阶段 1 完成后立即删除，不做过渡保留。
