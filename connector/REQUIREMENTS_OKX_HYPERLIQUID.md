# OKX / Hyperliquid 连接器需求文档

> 版本：v0.1（草案）  
> 日期：2026-08-19  
> 范围：`connector` crate 内新增 `okx` 与 `hyperliquid` 两个连接器，均实现现有
> [`Connector`](src/connector.rs) / [`ConnectorBuilder`](src/connector.rs) trait，策略侧与引擎侧无感知。

---

## 1. 背景与目标

当前 `connector` 已内置 binancefutures / binancespot / bybit 三个连接器，统一抽象为：

- `ConnectorBuilder::build_from(&str)`：从 TOML 配置构建连接器。
- `Connector::register(symbol)`：注册要交易的标的，驱动公共/私有流订阅。
- `Connector::order_manager()`：向引擎暴露当前在途订单（`GetOrders`）。
- `Connector::run(tx)`：启动各 WebSocket 流（不阻塞）。
- `Connector::submit / cancel`：提交新订单 / 撤单，通过 `PublishEvent` 回报。

目标：

1. 新增 OKX V5（合约）与 Hyperliquid（永续）两个连接器，遵循上述 trait，`main.rs`
   按 `"okx"` / `"hyperliquid"` 路由，与现有连接器并存。
2. 行情与成交回报统一转为 `LiveEvent`（`Feed` / `Order` / `Position` / `Error`），
   供 `FusedHashMapMarketDepth` 融合。
3. 订单状态管理复用「REST + WS 双通道确认」模式，防止幽灵订单。
4. 产出示例配置 `connector/examples/okx.toml`、`connector/examples/hyperliquid.toml`。

---

## 2. 通用设计原则

### 2.1 目录结构（与现有连接器一致）

```
connector/src/okx/
  mod.rs            # Config / OkxError / Connector 实现
  rest.rs           # reqwest 客户端（HMAC 签名、下单、撤单、快照、仓位）
  ordermanager.rs   # clOrdId 维度订单管理（双通道确认）
  public_stream.rs  # 公共 WS：books + trades
  private_stream.rs # 私有 WS：login + orders + positions
  msg/mod.rs
  msg/rest.rs       # REST 数据模型
  msg/stream.rs     # WS 数据模型

connector/src/hyperliquid/
  mod.rs            # Config / HyperliquidError / Connector 实现
  signing.rs        # EIP-712 phantom-agent 签名（msgpack + keccak256 + secp256k1）
  client.rs         # POST /info 与 POST /exchange
  ordermanager.rs   # cloid ↔ oid 维度订单管理
  ws.rs             # WS：l2Book + trades + orderUpdates + userEvents
  msg.rs            # 数据模型
```

### 2.2 注册与配置

- `connector/Cargo.toml` 新增 feature：`okx = []`、`hyperliquid = []`（默认不开启，与现有 feature 风格一致）。
- `main.rs` 的 `match args.connector.as_str()` 新增分支；`Args.connector` 帮助文本补充说明。
- 模块声明使用 `#[cfg(feature = "okx")]` / `#[cfg(feature = "hyperliquid")]` 门控。

### 2.3 错误处理

每个连接器定义 `thiserror` 枚举，实现 `to_value() -> hftbacktest::types::Value`，
错误经 `LiveEvent::Error(LiveError::with(kind, value))` 上报：

- 连接中断/重连失败 → `ErrorKind::ConnectionInterrupted`
- 登录/鉴权失败 → `ErrorKind::CriticalConnectionError`
- 下单/撤单被拒 → `ErrorKind::OrderError`

---

## 3. OKX V5 连接器

### 3.1 端点

| 用途 | 地址 |
| --- | --- |
| REST | `https://www.okx.com`（模拟盘 `https://www.okx.com` 演示账号） |
| 公共 WS | `wss://ws.okx.com:8443/ws/v5/public` |
| 私有 WS | `wss://ws.okx.com:8443/ws/v5/private` |

### 3.2 配置字段（`examples/okx.toml`）

```toml
rest_url = "https://www.okx.com"
public_ws_url = "wss://ws.okx.com:8443/ws/v5/public"
private_ws_url = "wss://ws.okx.com:8443/ws/v5/private"
api_key = ""
secret = ""
passphrase = ""
td_mode = "cross"       # cross | isolated
order_prefix = ""       # clOrdId 前缀，≤ 16 字符，不含特殊字符
```

### 3.3 认证

所有私有 REST / WS 请求携带 4 个 header / 参数：

- `OK-ACCESS-KEY`：api_key
- `OK-ACCESS-SIGN`：`Base64(HMAC_SHA256(timestamp + method + requestPath + body))`
  - GET：body 为空串，`requestPath` 含 query（如 `/api/v5/account/positions?instId=BTC-USDT-SWAP`）
  - POST：body 为 JSON 原文
- `OK-ACCESS-TIMESTAMP`：ISO 8601 UTC，毫秒精度，如 `2026-08-19T03:00:00.123Z`
- `OK-ACCESS-PASSPHRASE`：passphrase

私有 WS `login` 消息的签名体为 `timestamp + "GET" + "/users/self/verify"`（无 body）。

### 3.4 REST 接口

| 操作 | 端点 | 说明 |
| --- | --- | --- |
| 下单 | `POST /api/v5/trade/order` | body 见下；`code == "0"` 且 `data[0].sCode == "0"` 成功 |
| 撤单 | `POST /api/v5/trade/cancel-order` | `{"instId", "clOrdId"}` |
| 全撤 | `POST /api/v5/trade/cancel-all-orders` | `{"instId", "tdMode"}`，新标的上线时清场 |
| 订单薄快照 | `GET /api/v5/market/books?instId=&sz=400` | 公共，无需签名 |
| 持仓 | `GET /api/v5/account/positions?instId=` | 新标的上线时同步初始仓位 |

下单 body：

```json
{
  "instId": "BTC-USDT-SWAP",
  "tdMode": "cross",
  "clOrdId": "<prefix><16位随机>",
  "side": "buy",
  "ordType": "limit",
  "px": "50000.0",
  "sz": "0.01"
}
```

常见错误码：`50111` 签名无效、`50113` 时间戳过期、`51001` 合约不存在、
`51401` 撤单时订单不存在（按「已成交/已撤」处理）、`51008`/`51009` 订单数量/价格非法。

### 3.5 WS 通道

#### 公共流（books + trades）

订阅：`{"op":"subscribe","args":[{"channel":"books","instId":"BTC-USDT-SWAP"},{"channel":"trades","instId":"..."}]}`

- `books`：首条推送为全量快照（`action == "snapshot"`），随后为增量（`action == "update"`）；
  每个 level 为 `[px, sz, ...]`，`sz == "0"` 表示删除该档位。
- `trades`：`data[]` 中 `side == "buy"` → `LOCAL_BUY_TRADE_EVENT`，`"sell"` → `LOCAL_SELL_TRADE_EVENT`。
- 心跳：`{"op":"ping"}`，服务端回 `{"op":"pong"}`。

#### 私有流（orders + positions）

1. `{"op":"login","args":[{"apiKey","passphrase","timestamp","sign"}]}`；
   `event == "login"` 且 `code == "0"` 后订阅：
   `{"op":"subscribe","args":[{"channel":"orders","instType":"SWAP"},{"channel":"positions","instType":"SWAP"}]}`。
2. `orders` 通道：`data[]` 含 `clOrdId / ordId / state / px / sz / accFillSz / fillPx / avgPx / side / posSide / uTime`。
3. `positions` 通道：`data[]` 含 `instId / posSide / pos / uTime`；仅仓位变化时推送，
   初始值由 REST `GET /api/v5/account/positions` 补齐。

### 3.6 订单映射

#### 状态映射（OKX → hftbacktest `Status`）

| OKX `state` | `Status` |
| --- | --- |
| `live` | `New` |
| `partially_filled` | `PartiallyFilled` |
| `filled` | `Filled` |
| `canceled` / `mmp_canceled` | `Canceled` |
| `rejected` | `Rejected` |
| `order_failed` | `Expired` |

#### 方向 / 类型 / TIF

| hftbacktest | OKX |
| --- | --- |
| `Side::Buy` | `side = "buy"` |
| `Side::Sell` | `side = "sell"` |
| `OrdType::Limit` | `ordType = "limit"` |
| `OrdType::Market` | `ordType = "market"` |
| `TimeInForce::GTC` | 默认（limit 省略 tif） |
| `TimeInForce::GTX` | `ordType = "post_only"` |
| `TimeInForce::IOC` | `ordType = "ioc"` |
| `TimeInForce::FOK` | `ordType = "fok"` |

`px` 保留 `tick_size` 精度（`get_precision`），`sz` 保留 5 位小数。

### 3.7 订单管理（双通道确认）

复刻 binancefutures 的 `OrderManager`：

- `clOrdId = order_prefix + 16 位随机`，`order_id_map: (symbol, bot_order_id) -> clOrdId`。
- REST 通道：下单 ack / 撤单 ack / 撤单失败 → `removed_by_rest`。
- WS 通道：`orders` 通道终态更新 → `removed_by_ws`。
- 仅当两个通道都确认终态后才从内存删除订单，避免 WS 慢于 REST 造成幽灵订单。
- 新标的注册时：REST 全撤 + REST 拉取初始持仓并发布 `LiveEvent::Position`。

### 3.8 验收标准

- `cargo check -p connector --features okx` 通过。
- `main.rs` 支持 `connector <name> okx <config>` 启动。
- 模拟盘下单/撤单全链路可用：订阅 → 快照 → 下单 → orderUpdates → 撤单。

---

## 4. Hyperliquid 连接器

### 4.1 端点

| 用途 | 地址 |
| --- | --- |
| Info REST | `https://api.hyperliquid.xyz/info` |
| Exchange REST | `https://api.hyperliquid.xyz/exchange` |
| WS | `wss://api.hyperliquid.xyz/ws` |

### 4.2 配置字段（`examples/hyperliquid.toml`）

```toml
info_url = "https://api.hyperliquid.xyz/info"
exchange_url = "https://api.hyperliquid.xyz/exchange"
ws_url = "wss://api.hyperliquid.xyz/ws"
private_key = "0x..."        # API wallet 的 secp256k1 私钥（32 字节 hex）
account_address = "0x..."    # API wallet 地址（可由私钥推导，留空则自动推导）
order_prefix = ""            # 仅用于 cloid 前缀（可选，非必须为 hex）
is_mainnet = true            # false 表示测试网（签名 source = "b"）
```

> 注意：Hyperliquid 的 API wallet 私钥是 **secp256k1**（Ethereum 风格）私钥，
> 与项目内 `utils::sign_ed25519` 无关；所有 exchange 操作均为 EIP-712 签名。

### 4.3 签名方案（L1 action，phantom agent）

当前官方 SDK（Python v0.18+、rhyperliquid、hl-signing）统一采用 phantom agent 方案：

1. 将 action 以 **msgpack map（named）格式** 序列化（字段顺序必须与发送体一致）：
   - order：`{"type":"order","orders":[...],"grouping":"na"}`
   - cancel：`{"type":"cancel","cancels":[{"a":<asset>,"o":<oid>}]}`
   - cancelByCloid：`{"type":"cancelByCloid","cancels":[{"asset":<asset>,"cloid":"<hex>"}]}`
2. `connection_id = keccak256(msgpack(action) || nonce(8B BE) || vault_flag)`
   - 无 vault：`vault_flag = [0x00]`
   - 有 vault：`vault_flag = [0x01] + 地址 20 字节`
3. EIP-712：
   - domain：`{name: "Exchange", version: "1", chainId: 1337, verifyingContract: 0x0000...0000}`
     （L1 action 固定 chainId 1337，**不是** Arbitrum 42161，也不是 0x66eee）
   - primaryType：`Agent(string source,bytes32 connectionId)`
   - message：`{source: "a"(主网) / "b"(测试网), connectionId: "0x"+hex}`
4. 用 secp256k1 私钥对 `keccak256(0x19 0x01 || domainHash || structHash)` 做 ECDSA，
   得到 `{r, s, v}`（v ∈ {27, 28}）。

发送体：

```json
{
  "action": {"type": "order", "orders": [...], "grouping": "na"},
  "nonce": 1723456789012,
  "signature": {"r": "0x...", "s": "0x...", "v": 27}
}
```

订单元素（wire）：

```json
{
  "a": 0,                    // 资产索引（由 /info meta universe 下标决定）
  "b": true,                 // buy=true / sell=false
  "p": "50000",              // 价格字符串（tick 精度）
  "s": "0.01",               // 数量字符串（szDecimals 精度）
  "r": false,                // reduceOnly
  "t": {"limit": {"tif": "Gtc"}},
  "c": "a1b2..."             // cloid：32 位 hex（128bit），可选
}
```

### 4.4 REST（/info 与 /exchange）

`POST /info`（无需签名）：

| body | 用途 |
| --- | --- |
| `{"type":"meta"}` | universe：`[{name, szDecimals, ...}]`，资产索引 = 数组下标 |
| `{"type":"clearinghouseState","user":"0x..."}` | `assetPositions[].position.{coin,szi}`，初始持仓 |
| `{"type":"openOrders","user":"0x..."}` | 未成交订单（`oid / cloid / side / px / sz`） |

`POST /exchange`（签名见 4.3）响应：

- 下单：`{"status":"ok","response":{"type":"order","data":{"statuses":[
  {"resting":{"oid":...}} | {"filled":{"totalSz","avgPx","oid"}} | {"error":"..."}]}}}`
- 撤单：`{"status":"ok","response":{"type":"cancel","data":{"statuses":["success" | {"error":...}]}}}`

### 4.5 WS 通道

订阅消息：`{"method":"subscribe","subscription":{...}}`

| 订阅 | 用途 |
| --- | --- |
| `{"type":"l2Book","coin":"BTC"}` | 订单簿：`levels = [bids, asks]`，`sz==0` 或空数组表示删档 |
| `{"type":"trades","coin":"BTC"}` | 成交：`side == "B"` → buy taker |
| `{"type":"orderUpdates","user":"0x..."}` | 订单状态：`data[].{order, status}`（`open/filled/canceled/rejected`） |
| `{"type":"userEvents","user":"0x..."}` | 成交/资金费/强平事件，用于增量更新持仓 |

坑：

- 返回的 `channel` 字段是小写 coin 前缀，如 `l2Book:btc`、`trades:btc`、`orderUpdates`、`user`；
  处理时必须用 `starts_with("l2Book")` / `starts_with("trades")` 而非全等匹配。
- `l2Book` 第一层数组是 bids、第二层是 asks。
- `userEvents` 的 channel 是 `"user"`，`data.type == "fill"` 时 `fill.{coin,px,sz,side,startPosition,dir}`。

### 4.6 订单管理

- `cloid = 32 位 hex`（随机 u128），`order_id_map: (symbol, bot_order_id) -> cloid`。
- `orderUpdates` 是权威状态通道：`status` 映射
  `open→New`、`filled→Filled`、`canceled→Canceled`、`rejected→Rejected`；
  同时记录 `oid`，供撤单使用。
- 下单响应先到：`resting` 记 oid 并置 `New`；`filled` 置 `Filled` 并带 `avgPx`；
  `error` 置 `Expired` 并上报 `OrderError`。
- 撤单：优先按 oid（`{"type":"cancel"}`），若 oid 未知则按 cloid（`{"type":"cancelByCloid"}`）。
- 新标的注册时：`clearinghouseState` 拉初始持仓并发布；`userEvents.fill` 增量更新持仓。

### 4.7 订单映射

| hftbacktest | Hyperliquid |
| --- | --- |
| `Side::Buy` | `b = true` |
| `Side::Sell` | `b = false` |
| `OrdType::Limit` | `t = {"limit":{"tif":...}}` |
| `OrdType::Market` | v1 暂不支持（返回 `InvalidArg`，见风险） |
| `TimeInForce::GTC` | `tif = "Gtc"` |
| `TimeInForce::GTX` | `tif = "Alo"`（Add liquidity only） |
| `TimeInForce::IOC` | `tif = "Ioc"` |
| `TimeInForce::FOK` | v1 暂不支持（HL 无 FOK，返回 `InvalidArg`） |

### 4.8 依赖新增

- `rmp-serde`：msgpack named 序列化 action（用于 connection_id 计算）。
- `k256`（`ecdsa` feature）：secp256k1 可恢复签名。
- `sha3`：keccak256。
- `hex`：hex 编解码。

### 4.9 验收标准

- `cargo check -p connector --features hyperliquid` 通过。
- `main.rs` 支持 `connector <name> hyperliquid <config>` 启动。
- 测试网可用：`{"type":"noop"}` 签名验证 → 下单 → orderUpdates → 撤单全链路。

---

## 5. 风险与待办

1. **Hyperliquid market / FOK 订单**：v1 仅支持 limit（Gtc/Alo/Ioc），
   后续可按 `{"trigger":{"isMarket":true,...}}` 扩展市场单。
2. **OKX 全撤**：`cancel-all-orders` 参数以官方文档为准，上线前需在模拟盘验证。
3. **订单簿融合**：OKX/Hyperliquid 仅订阅单一深度（OKX `books` 全量档位、HL `l2Book`），
   不做多档融合；后续如需更细粒度可订阅 `books5/books-l2-tbt` 等多通道。
4. **重连状态**：断线重连后 Hyperliquid 需重新订阅全部标的与 orderUpdates；
   OKX 私有流重连需重新 login + subscribe，并补拉 REST 快照。
5. **限频**：未实现令牌桶/滑动窗口限频，高频下单可能触发交易所风控，后续统一抽象。
