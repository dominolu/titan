# 采集端 Bybit 回归 connector：行情实现单一来源（2026-11-25）

对应 [Issue #34](https://github.com/dominolu/titan/issues/34)。本轮只涉及公开行情与录制：未连接任何真实账户，未下单，未部署到采集机，未访问实盘服务器。

## 1. 范围

`book record` 现在对 Binance、OKX、Bybit、Hyperliquid 四个交易所都通过 `connector::market_stream` 取行情。采集侧不再包含任何交易所专有的帧解析、序号校验或快照对齐。

| 之前 | 之后 |
| --- | --- |
| `OkxFeed`、`HyperliquidFeed` 走 connector，`BybitFeed` 自带 270 行实现 | 一个 `MarketStreamFeed`，四家都走 connector |
| connector 没有 Bybit 行情模块 | `connector/src/bybit/`：`msg`、`rest`、`market_stream`、`mod` |
| 采集侧自校验 Bybit `u` 与断连重建 | connector 校验 `u`，断裂只重取镜像、不断连接 |
| `book.rs` 约 502 行交易所相关代码 | `book.rs` 1400 行，只剩 venue→配置的分发与 REST OI 轮询 |

`book.rs` 删除的采集侧实现：`BybitFeed`（270 行）、`ConnectorVenueFeed`、`BinanceConnectorFeed` 的重复外壳、`parse_changes`/`parse_levels`、`transport::keep_connection` 直连与其 import。

## 2. Bybit 模块的设计

### 2.1 分层

| 层 | 位置 | 职责 |
| --- | --- | --- |
| 连接 | `connector::transport`（既有） | 帧读取、ping、重连退避、订阅随连接重放 |
| 帧解析 | `bybit::msg` | V5 信封、订单簿、成交流、行情概要、REST 镜像 |
| REST | `bybit::rest` | 镜像（1/50/200/500 档）与 OI；`retCode != 0` 视为拒绝；429/503 走既有的 `VenueBanned` |
| 序列与失效 | `bybit::market_stream::BookState` | 纯状态机，无 I/O，可精确单测 |
| 连接驱动 | `bybit::market_stream::MarketStream` | 读帧、发布、取镜像、OI 轮询 |
| 对外入口 | `connector::market_stream::start_bybit_market_stream` | 与其它交易所共用 `run_stream_loop` 的重连、退避与错误上报 |

### 2.2 序号语义

- 镜像（订阅应答的 `snapshot` 或 REST 镜像）提供起始 `u`；之后每条 `delta` 的 `u` 必须等于上一条已发布 `u` + 1。
- `u` 跳变 → 发布 `StreamInvalidated`，把触发断裂的那一帧与随后的帧按 `MAX_BUFFERED_DELTAS = 256` 缓冲，并请求一次新镜像；**连接保持**。
- 新镜像只有在 `u` 大于已发布序号时才被采用；发布镜像前先发 `StreamInvalidated`（若旧镜像无法被续接），再把能连续续接的缓冲帧按序重放。
- 镜像 `u` ≤ 已发布序号 → 丢弃，绝不让盘口倒退。
- 重复帧（`u` 等于已发布序号）→ 忽略，不重复发布。
- 连接结束（`u` 会在新连接上重新计数）→ 若已发布过镜像则发一次失效，进入 `Disconnected`；新连接的镜像无条件作为新盘口发布。同一次中断不重复上报。

### 2.3 与其它交易所的共用

- 重连、指数退避、`ConnectorError` 上报：`run_stream_loop`（与 Binance/OKX/Hyperliquid 同一实现）。
- 深度发布：`NativeMarketBatch::Depth` 优先，`FeedBatch` 回退，与其它交易所一致。
- 失效发布：`PublishEvent::StreamInvalidated`，`epoch = 0`，与 OKX/Hyperliquid 一致。
- 订阅：`WsTarget::subscribing`，订阅随连接发送，重连自动重放。
- 拒绝处理：`transport::VenueBanned`；`retCode != 0` 在 REST 层报为 `Rejected`。

### 2.4 新增的事件

Bybit 的 OI 只能经 REST 获取，因此新增 `PublishEvent::OpenInterest { symbol, open_interest, exch_ts }`：值原样是场所文本，单位是场所约定（Bybit 报合约张数），由录制端记为 `aux/kind=oi`，`source=rest`。

## 3. 测试

### 3.1 connector 侧单测（`connector/src/bybit`）

| 用例 | 覆盖 |
| --- | --- |
| `the_first_image_is_published_before_anything_else` | 首镜像 |
| `contiguous_deltas_are_published_and_a_gap_invalidates_without_losing_the_connection` | 断裂、缓冲、镜像续接、恢复后连续性 |
| `a_snapshot_older_than_the_stream_does_not_move_the_book_backwards` | 快照落后于流 |
| `a_repeated_frame_is_not_published_twice` | 重复帧 |
| `a_reconnect_starts_a_fresh_book_even_below_the_previous_update_id` | 重连后 `u` 重置 |
| `the_buffer_is_bounded_and_keeps_the_newest_frames` | 缓冲边界 |
| `a_negative_update_id_is_refused_rather_than_wrapped` | 非法 `u` |
| `connection_tests::a_sequence_break_publishes_an_invalidation_and_keeps_the_connection` | **真实 socket**：断裂后连接不断，且只发布已证明连续的帧 |
| `connection_tests::a_repeated_delta_is_published_once` | 真实 socket：重复帧只发布一次 |
| `connection_tests::an_image_behind_the_published_book_does_not_move_it_backwards` | 真实 socket：落后镜像被丢弃 |
| `connection_tests::the_subscription_is_sent_when_the_socket_opens` | 订阅随连接发出 |
| `msg::*`、`rest::*`、`tests::*` | 报文解码、档位映射、类别→路径、REST `retCode` 拒绝 |

## 4. 验证结果（titan-build）

工具链：仓库固定 `rust-toolchain.toml` 1.94.1。

| 门禁 | 结果 |
| --- | --- |
| `cargo fmt --all -- --check` | 通过 |
| `cargo clippy -p connector -p collector --all-targets --all-features -- -D warnings` | 通过（0 warning） |
| `cargo test -p connector -p collector --all-targets --all-features` | 通过：connector lib 420 passed / 14 ignored，collector 与集成测试全部通过 |
| connector Bybit 专项 `cargo test -p connector --features bybit bybit` | 25 passed / 0 failed |

### 4.1 验证限制

- 全工作区测试在构建服务器上**未能完整执行**：`crates/titan-strategy-runtime/examples/{v13_artifact_smoke,v13_pair_arb_smoke}.rs` 引用的 `*V13` 符号在该服务器工作树中不存在。差异来自构建服务器 `~/dev/titan` 是一份非 git、内容不一致的同步副本，而不是本轮改动；本轮验证因此在独立目录中执行，只覆盖 `connector` 与 `collector`。
- 构建服务器磁盘一度耗尽（`/home` 100%，剩余 0）。释放冷测缓存后恢复；该清理与代码无关。
- **未做真实交易所录制**。本轮没有连向 Bybit 生产端点，也没有部署到采集机；`u` 的语义按 V5 文档与注入帧验证，生产行为需在受控录制中复核后才能宣称端到端可用。

## 5. 未包含

- Bybit 下单、私有流与订单管理器未实现；该模块只有公开行情。
- `allLiquidation` 爆仓流未订阅，录制侧仍标记为 `unsupported`。
- 采集侧 `book record` 的 venue→类别映射（`bybit_spot` / `bybit_linear` / `bybit_inverse` / 裸 `bybit`→linear）沿用原实现。
