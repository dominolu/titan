# Connector API 覆盖清单

记录 Binance USD-M / OKX V5 / Hyperliquid 三个连接器**已实现**的 REST 接口与 WebSocket
频道，以及统一 API 层（`src/api.rs`）的覆盖情况。
状态图例：✅ 已验证（实测/单测）｜⚠️ 已实现，端到端待验证 ｜🔧 已实现未接线

> 最后更新：2026-08-19
>
> 官方全量对照见 [API_GAP_ANALYSIS.md](API_GAP_ANALYSIS.md)

## 统一 API 层（src/api.rs）

新增 [`BrokerApi`](src/api.rs) trait 与统一数据结构（`OrderInfo`/`PositionInfo`/`AccountInfo`/
`Ticker`/`OrderBook`/`FundingRate`/`Fill`/`FeeRate`/`LeverageInfo`/`IncomeRecord` 等），
三个交易所的 REST 客户端均实现该 trait。策略只需持有 `Box<dyn BrokerApi>` 即可自由切换 broker：

| 统一接口 | 说明 | Binance | OKX | Hyperliquid |
|---|---|---|---|---|
| ping / get_server_time | 连通性 / 服务器时间 | ✅ | ✅ | ✅ exchangeStatus |
| get_instruments | 合约元数据 | ✅ | ✅ | ✅ meta |
| get_ticker / get_tickers | 行情快照 | ✅ | ✅ | ✅ metaAndAssetCtxs |
| get_order_book | 订单簿 | ✅ | ✅ | ✅ l2Book |
| get_trades | 最近成交 | ✅ | ✅ | ✅ recentTrades |
| get_klines | K 线 | ✅ | ✅ | ✅ candleSnapshot |
| get_funding_rate(_history) | 资金费 | ✅ | ✅ | ✅ |
| get_open_interest | 持仓量 | ✅ | ✅ | ✅ |
| submit_order / submit_orders | 下单/批量 | ✅ | ✅ | ✅（EIP-712 签名） |
| cancel_order / cancel_orders | 撤单/批量 | ✅ | ✅ | ✅ oid/cloid |
| cancel_all_orders / cancel_all_after | 全撤 / 防失控安全网 | ✅ countdownCancelAll | ✅ cancel-all-after | ✅ scheduleCancel |
| amend_order | 改单 | ✅ | ✅ | ✅ modify |
| get_order / get_open_orders / get_order_history | 查单 | ✅ | ✅ | ✅ |
| get_fills | 成交明细 | ✅ | ✅ | ✅ userFills |
| get_account / get_positions | 账户/持仓 | ✅ | ✅ | ✅ clearinghouseState |
| set_leverage / get_leverage | 杠杆 | ✅ | ✅ | ✅ updateLeverage |
| get_fee_rates / get_income_history | 费率/流水 | ✅ | ✅ | ✅ userFees/ledger |

## Binance USD-M Futures（binancefutures）

| 类型 | 接口 / 频道 | 实现位置 | 说明 | 状态 |
|---|---|---|---|---|
| REST | `start_user_data_stream` / `keepalive_user_data_stream` | rest.rs | listenKey 创建/保活 | ✅ |
| REST | `close_user_data_stream` | brokerapi.rs | 关闭 listenKey（DELETE） | ✅ |
| REST | `submit_order` | rest.rs / brokerapi.rs | 单笔下单（统一接口） | ✅ |
| REST | `submit_orders` | brokerapi.rs | 批量下单（≤5） | ✅ 统一接口接入 |
| REST | `modify_order` / `amend_order` | rest.rs / brokerapi.rs | 改单 | ✅ 统一接口接入 |
| REST | `cancel_order` / `cancel_orders` / `cancel_all_orders` | rest.rs / brokerapi.rs | 撤单 / 批量 / 全撤 | ✅ |
| REST | `countdown_cancel_all` | brokerapi.rs | 倒计时全撤（安全网） | ✅ |
| REST | `get_position_information` / `get_balance` / `get_account` | rest.rs / brokerapi.rs | 持仓 / 余额 / 账户 | ✅ |
| REST | `get_exchange_info` | brokerapi.rs | 合约规则/精度 | ✅ |
| REST | 行情全量（ticker/24hr、premiumIndex、bookTicker、trades、aggTrades、klines、fundingRate、openInterest、insuranceBalance 等） | brokerapi.rs | 对照官方文档 | ✅ |
| REST | 查单全量（order、openOrder、openOrders、allOrders、userTrades、forceOrders、orderAmendment、order/test） | brokerapi.rs | 对照官方文档 | ✅ |
| REST | 账户全量（marginType、leverage、positionMode、multiAssetsMargin、positionMargin、income、commissionRate、leverageBracket、adlQuantile、accountConfig 等） | brokerapi.rs | 对照官方文档 | ✅ |
| REST | algoOrder 系列（下单/撤/查） | brokerapi.rs | 条件单/算法单 | ✅ |
| REST | `get_depth` | rest.rs | 深度快照（WS 断档补快照） | ✅ |
| WS 公共 | `{symbol}@trade` / `@depth@0ms` / `@markPrice` / `@bookTicker` | market_data_stream.rs | 成交/深度/资金费/BBO | ⚠️ 当前环境 WS 不可达，格式按官方文档 + 单测 |
| WS 私有 | listenKey 用户数据流 | user_data_stream.rs | AccountUpdate / OrderTradeUpdate / ListenKeyExpired | ⚠️ 依赖 WS 连通性 |

## OKX V5 SWAP（okx）

| 类型 | 接口 / 频道 | 实现位置 | 说明 | 状态 |
|---|---|---|---|---|
| REST | `submit_order` / `batch_orders` | rest.rs / brokerapi.rs | 下单 / 批量 | ✅ 实盘 e2e 验证 |
| REST | `cancel_order` / `cancel_batch_orders` / `cancel_all_orders` | rest.rs / brokerapi.rs | 撤单 / 批量 / 全撤 | ✅ 实盘 e2e 验证 |
| REST | `cancel_all_after` | brokerapi.rs | 断线自动撤单（安全网） | ✅ |
| REST | `amend_order` / `amend_batch_orders` / `close_position` | brokerapi.rs | 改单 / 平仓 | ✅ |
| REST | 查单全量（order、orders-pending、orders-history、fills、mass-cancel、order-precheck） | brokerapi.rs | 对照官方文档 | ✅ |
| REST | 算法单 order-algo（下单/撤/查） | brokerapi.rs | 条件单 | ✅ |
| REST | 账户全量（balance、positions、config、set-leverage、leverage-info、max-size、trade-fee、bills、risk-state、max-withdrawal 等） | rest.rs / brokerapi.rs | 对照官方文档 | ✅ |
| REST | 行情全量（tickers、ticker、books、books-full、candles、trades、history-*） | rest.rs / brokerapi.rs | 对照官方文档 | ✅（get_books 原为 dead_code，统一接口已接入） |
| REST | 公共数据全量（funding-rate、funding-rate-history、open-interest、price-limit、time、mark-price、position-tiers、system/status） | brokerapi.rs | 对照官方文档 | ✅ |
| WS 公共 | `books` / `trades` / `funding-rate` | public_stream.rs | 核心行情 | ✅ 实测 |
| WS 公共 | `books5` / `bbo-tbt` | public_stream.rs | 精简盘口 / 最优价 → BBO | ✅ 解析单测 |
| WS 公共 | `tickers`/`open-interest`/`mark-price`/`index-tickers`/`estimated-price`/`liquidation-orders`/`adl-warning`/`status`/`candle*` | public_stream.rs | 补充频道解析 | ✅ 解析单测 |
| WS 私有 | `orders` / `positions`（instType=SWAP） | private_stream.rs | 登录后订阅订单 + 持仓 | ⚠️ 登录签名实测通过 |
| WS 私有 | `account`/`balance-and-position`/`orders-algo`/`algo-advance` | private_stream.rs | 补充频道解析 | ✅ 解析单测 |

## Hyperliquid（hyperliquid）

| 类型 | 接口 / 频道 | 实现位置 | 说明 | 状态 |
|---|---|---|---|---|
| REST | `post_info` / `get_meta` / `get_clearinghouse_state` / `get_open_orders` | client.rs | 基础 info | ✅ 实测 |
| REST | info 全量（allMids、userFills、userFillsByTime、orderStatus、l2Book、candleSnapshot、historicalOrders、fundingHistory、predictedFundings、userFunding、subAccounts、vaultDetails、userFees、portfolio、referral、spotMeta、spotMetaAndAssetCtxs、spotClearinghouseState、tokenDetails 等 ~50 种） | brokerapi.rs | 对照官方 gitbook 补齐 | ✅ 解析单测 |
| REST | `post_exchange`（order/cancel/cancelByCloid） | client.rs | 签名交易（EIP-712 phantom agent） | ✅ 测试网 e2e 验证 |
| REST | exchange 全量（modify、batchModify、scheduleCancel、updateLeverage、updateIsolatedMargin、twapOrder、twapCancel、approveAgent、approveBuilderFee、usdClassTransfer、spotSend、withdraw3 等） | brokerapi.rs | 签名后提交 | ✅ 签名器单测 |
| WS | `l2Book` / `trades` | ws.rs | 订单簿 / 成交 | ✅ 实测 |
| WS | `orderUpdates` / `userEvents` | ws.rs | 订单状态 / 用户成交 | ⚠️ 状态机有单测 |
| WS | `allMids`/`bbo`/`candle`/`userFills`/`userFundings`/`activeAssetCtx`/`clearinghouseState`/`openOrders`/`notification`/`spotState`/`twapStates` 等 | ws.rs | 补充频道解析；bbo → BBO 事件 | ✅ 解析单测 |

## 跨交易所对比

| 维度 | Binance | OKX | Hyperliquid |
|---|---|---|---|
| REST 签名 | HMAC-SHA256（query/body） | HMAC-SHA256（ISO 时间戳） | EIP-712 phantom agent（secp256k1） |
| WS 认证 | listenKey 私有流 | login（Unix 秒时间戳） | 无需 WS 认证（user 地址订阅） |
| 资金费数据源 | WS `markPrice` 推流 | WS `funding-rate` 推流 | WS `userFundings` 推流（快照 + 每小时结算） |
| 统一 API 层 | `BrokerApi` trait（src/api.rs），三家全实现 | 同左 | 同左 |
| 测试 | 单测（解析/映射/请求体） | 单测 + 实盘冒烟 | 单测 + 实盘冒烟 |

## 测试

`cargo test --all-features`：162 个测试通过（lib + bin 各跑一遍）。

- 统一层：枚举映射、统一结构序列化往返
- Binance：请求体构建（限价/市价/条件单/批量）、exchangeInfo/ticker/order/position/account/fills 解析映射、bookTicker 流解析
- OKX：下单 body（limit/post_only/fok/ioc/market/stop）、算法单 body、balance/position/order/fills/ticker/instruments 解析、WS 补充频道解析
- Hyperliquid：wire 构建（limit/market/trigger）、metaAndAssetCtxs/clearinghouseState/orderStatus/l2Book/funding/candle/recentTrades/bbo 解析、exchange 响应解析

实盘冒烟（默认 `--ignored` 跳过，需网络）：

```bash
cargo test --all-features hyperliquid::brokerapi::tests::live -- --ignored --nocapture
HTTPS_PROXY=127.0.0.1:7897 cargo test --all-features okx::brokerapi::tests::live -- --ignored --nocapture
```
