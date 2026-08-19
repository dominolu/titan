# Connector 官方 API 全量对照（Binance / OKX / Hyperliquid）

以三家交易所**官方 API 文档**为基准，列出全部 REST 端点与 WebSocket 频道，逐一标记当前 connector 的实现状态。

状态图例：

- ✅ 已实现并验证（实测或单测覆盖）
- ⚠️ 已实现，端到端待验证（格式按官方文档 + 单测，环境受限未实连）
- 🔧 客户端方法已实现，连接器未接线（dead code / 未调用）
- ❌ 未实现

> 官方来源：
> - Binance USD-M Futures：官方 docs（binance-docs / developers.binance.com，fapi 系列）
> - OKX V5：官方 REST/WS 文档（okx.com/docs-v5），端点清单与 tiagosiebler/okx-api 核对
> - Hyperliquid：官方 gitbook（info-endpoint / exchange-endpoint / websocket）
>
> 最后更新：2026-08-19

---

## 1. Binance USD-M Futures（/fapi）

### 1.1 REST 官方端点

#### 行情数据（Market Data）

| 官方端点 | 用途 | 状态 | 实现位置 / 备注 |
|---|---|---|---|
| `GET /fapi/v1/ping` | 连通性测试 | ❌ | |
| `GET /fapi/v1/time` | 服务器时间 | ❌ | |
| `GET /fapi/v1/exchangeInfo` | 合约规则 / 精度 | ❌ | 合约参数目前走配置文件 |
| `GET /fapi/v1/depth` | 订单簿快照 | ✅ | rest.rs `get_depth`，WS 断档补快照用 |
| `GET /fapi/v1/rpiDepth` | RPI 订单簿 | ❌ | |
| `GET /fapi/v1/trades` | 近期成交 | ❌ | 行情走 WS `@trade` |
| `GET /fapi/v1/historicalTrades` | 历史成交 | ❌ | |
| `GET /fapi/v1/aggTrades` | 聚合成交 | ❌ | |
| `GET /fapi/v1/klines` | K 线 | ❌ | |
| `GET /fapi/v1/continuousKlines` | 连续合约 K 线 | ❌ | |
| `GET /fapi/v1/indexPriceKlines` | 指数 K 线 | ❌ | |
| `GET /fapi/v1/markPriceKlines` | 标记价 K 线 | ❌ | |
| `GET /fapi/v1/premiumIndexKlines` | 溢价指数 K 线 | ❌ | |
| `GET /fapi/v1/premiumIndex` | 标记价 / 资金费 | ❌ | 资金费走 WS `@markPrice` |
| `GET /fapi/v1/fundingRate` | 资金费历史 | ❌ | |
| `GET /fapi/v1/fundingInfo` | 资金费档位 | ❌ | |
| `GET /fapi/v1/ticker/24hr` | 24h 统计 | ❌ | |
| `GET /fapi/v1/ticker/price` | 最新价 | ❌ | |
| `GET /fapi/v2/ticker/price` | 最新价 v2 | ❌ | |
| `GET /fapi/v1/ticker/bookTicker` | 盘口最优价 | ❌ | |
| `GET /futures/data/delivery-price` | 季度交割价 | ❌ | |
| `GET /fapi/v1/openInterest` | 持仓量 | ❌ | |
| `GET /futures/data/openInterestHist` | 持仓量历史 | ❌ | |
| `GET /futures/data/topLongShortPositionRatio` | 大户多空比（仓位） | ❌ | |
| `GET /futures/data/topLongShortAccountRatio` | 大户多空比（账户） | ❌ | |
| `GET /futures/data/globalLongShortAccountRatio` | 全账户多空比 | ❌ | |
| `GET /futures/data/takerlongshortRatio` | 主动买卖量比 | ❌ | |
| `GET /fapi/v1/lvtKlines` | BLVT K 线 | ❌ | |
| `GET /fapi/v1/indexInfo` | 复合指数信息 | ❌ | |
| `GET /fapi/v1/assetIndex` | 多资产指数 | ❌ | |
| `GET /futures/data/basis` | 基差数据 | ❌ | |
| `GET /fapi/v1/constituents` | 指数成分 | ❌ | |
| `GET /fapi/v1/insuranceBalance` | 保险基金 | ❌ | |
| `GET /fapi/v1/tradingSchedule` | 交易时间表 | ❌ | |

#### 交易（Trade）

| 官方端点 | 用途 | 状态 | 实现位置 / 备注 |
|---|---|---|---|
| `POST /fapi/v1/order` | 下单 | ✅ | rest.rs `submit_order` |
| `POST /fapi/v1/batchOrders` | 批量下单 | 🔧 | rest.rs `submit_orders` 已实现，connector 未调用 |
| `PUT /fapi/v1/order` | 改单 | 🔧 | rest.rs `modify_order` 已实现，connector 未调用 |
| `PUT /fapi/v1/batchOrders` | 批量改单 | ❌ | |
| `GET /fapi/v1/orderAmendment` | 改单历史 | ❌ | |
| `DELETE /fapi/v1/order` | 撤单 | ✅ | rest.rs `cancel_order` |
| `DELETE /fapi/v1/batchOrders` | 批量撤单 | ✅ | rest.rs `cancel_orders` |
| `DELETE /fapi/v1/allOpenOrders` | 全撤（按合约） | ✅ | rest.rs `cancel_all_orders` |
| `POST /fapi/v1/countdownCancelAll` | 倒计时全撤（安全网） | ❌ | 强烈建议补：防失控单 |
| `GET /fapi/v1/order` | 查单 | ❌ | |
| `GET /fapi/v1/allOrders` | 历史订单 | ❌ | |
| `GET /fapi/v1/openOrders` | 未成交订单 | ❌ | |
| `GET /fapi/v1/openOrder` | 查单（单笔） | ❌ | |
| `GET /fapi/v1/forceOrders` | 强平订单 | ❌ | |
| `GET /fapi/v1/userTrades` | 成交明细 | ❌ | |
| `POST /fapi/v1/order/test` | 下单测试 | ❌ | |
| `POST /fapi/v1/algoOrder` + 撤/查系列 | 条件单 / 算法单 | ❌ | |
| `POST /fapi/v1/convert/*` | 现货兑换 | ❌ | 与本项目无关 |
| `GET /fapi/v1/pmAccountInfo` | 组合保证金账户 | ❌ | |
| `GET /fapi/v1/apiReferral/*` | 返佣 | ❌ | |

#### 账户（Account）

| 官方端点 | 用途 | 状态 | 实现位置 / 备注 |
|---|---|---|---|
| `GET /fapi/v2/positionRisk` | 持仓查询 | ✅ | rest.rs `get_position_information` |
| `GET /fapi/v3/positionRisk` | 持仓查询 v3 | ❌ | |
| `GET /fapi/v2/balance` | 余额 | ❌ | |
| `GET /fapi/v3/balance` | 余额 v3 | ❌ | |
| `GET /fapi/v2/account` | 账户信息 | ❌ | |
| `GET /fapi/v3/account` | 账户信息 v3 | ❌ | |
| `POST /fapi/v1/marginType` | 切换逐仓/全仓 | ❌ | |
| `POST /fapi/v1/positionSide/dual` | 双向持仓模式 | ❌ | |
| `POST /fapi/v1/leverage` | 设置杠杆 | ❌ | |
| `POST /fapi/v1/multiAssetsMargin` | 多资产模式 | ❌ | |
| `POST /fapi/v1/positionMargin` | 调整逐仓保证金 | ❌ | |
| `GET /fapi/v1/positionMargin/history` | 保证金变动历史 | ❌ | |
| `GET /fapi/v1/leverageBracket` | 杠杆档位 | ❌ | |
| `GET /fapi/v1/commissionRate` | 手续费率 | ❌ | |
| `GET /fapi/v1/accountConfig` | 账户配置 | ❌ | |
| `GET /fapi/v1/symbolConfig` | 合约配置 | ❌ | |
| `GET /fapi/v1/rateLimit/order` | 下单频率限制 | ❌ | |
| `GET /fapi/v1/adlQuantile` | ADL 分位数 | ❌ | |
| `GET /fapi/v1/symbolAdlRisk` | 合约 ADL 风险 | ❌ | |
| `GET /fapi/v1/multiAssetsMargin` | 多资产模式查询 | ❌ | |
| `GET /fapi/v1/positionSide/dual` | 持仓模式查询 | ❌ | |
| `GET /fapi/v1/income` | 收益流水 | ❌ | |
| `GET /fapi/v1/apiTradingStatus` | API 交易状态 | ❌ | |
| `GET /fapi/v1/income/asyn` 等 | 历史流水文件下载 | ❌ | |
| `POST /fapi/v1/feeBurn` / `GET` | BNB 抵扣 | ❌ | |
| `POST /fapi/v1/stock/contract` | TradFi 协议 | ❌ | |

#### 用户数据流（User Data Stream）

| 官方端点 | 用途 | 状态 | 实现位置 / 备注 |
|---|---|---|---|
| `POST /fapi/v1/listenKey` | 创建 listenKey | ✅ | rest.rs `start_user_data_stream` |
| `PUT /fapi/v1/listenKey` | 保活 | ✅ | rest.rs `keepalive_user_data_stream` |
| `DELETE /fapi/v1/listenKey` | 关闭流 | ❌ | 建议补：进程退出时释放 |

### 1.2 WebSocket 官方流

#### 公共行情流

| 官方流 | 用途 | 状态 | 实现位置 / 备注 |
|---|---|---|---|
| `<symbol>@trade` | 逐笔成交 | ⚠️ | market_data_stream.rs，当前环境 fstream 不可达 |
| `<symbol>@aggTrade` | 聚合成交 | ❌ | |
| `<symbol>@markPrice` / `@markPrice@1s` | 标记价 + 资金费 | ⚠️ | market_data_stream.rs → `LiveEvent::Funding` |
| `<symbol>@depth@0ms` / `@depth@100ms` | L2 增量深度 | ⚠️ | market_data_stream.rs（0ms） |
| `<symbol>@depth5/10/20` | 深度快照 | ❌ | |
| `<symbol>@bookTicker` | 盘口最优 | ❌ | |
| `<symbol>@kline_<interval>` | K 线 | ❌ | |
| `<symbol>@continuousKline_<interval>` | 连续合约 K 线 | ❌ | |
| `<symbol>@forceOrder` | 强平推送 | ❌ | |
| `<symbol>@indexPrice` | 指数价 | ❌ | |
| `<symbol>@compositeIndex` | 复合指数 | ❌ | |
| `<symbol>@assetIndex` | 多资产指数 | ❌ | |
| 全市场流（allMarketTickers / allBookTickers 等） | 全市场推送 | ❌ | |
| 组合流（`/stream?streams=...`） | 多流合并 | ❌ | 当前逐 symbol 订阅 |

#### 私有用户数据流事件

| 官方事件 | 用途 | 状态 | 实现位置 / 备注 |
|---|---|---|---|
| `ACCOUNT_UPDATE` | 持仓/余额更新 | ⚠️ | user_data_stream.rs → `LiveEvent::Position` |
| `ORDER_TRADE_UPDATE` | 订单/成交更新 | ⚠️ | user_data_stream.rs → Order 双通道确认 |
| `listenKeyExpired` | listenKey 过期 | ⚠️ | user_data_stream.rs 已处理并重连 |
| `MARGIN_CALL` | 追加保证金提醒 | ❌ | |
| `ACCOUNT_CONFIG_UPDATE` | 账户配置变更 | ❌ | |

---

## 2. OKX V5（SWAP）

### 2.1 REST 官方端点

#### 账户（Account）

| 官方端点 | 用途 | 状态 | 实现位置 / 备注 |
|---|---|---|---|
| `GET /api/v5/account/positions` | 持仓查询 | ✅ | rest.rs `get_positions`，实测 |
| `GET /api/v5/account/balance` | 余额 | ❌ | |
| `GET /api/v5/account/instruments` | 账户合约 | ❌ | |
| `GET /api/v5/account/positions-history` | 历史持仓 | ❌ | |
| `GET /api/v5/account/account-position-risk` | 持仓风险 | ❌ | |
| `GET /api/v5/account/bills` | 账单流水 | ❌ | |
| `GET /api/v5/account/bills-archive` | 历史账单 | ❌ | |
| `GET /api/v5/account/config` | 账户配置 | ❌ | |
| `POST /api/v5/account/set-position-mode` | 设置持仓模式 | ❌ | |
| `POST /api/v5/account/set-leverage` | 设置杠杆 | ❌ | 建议优先补 |
| `GET /api/v5/account/leverage-info` | 查询杠杆 | ❌ | |
| `GET /api/v5/account/max-size` / `max-avail-size` | 最大可开/可买 | ❌ | |
| `POST /api/v5/account/position/margin-balance` | 调整保证金 | ❌ | |
| `GET /api/v5/account/trade-fee` | 手续费率 | ❌ | |
| `GET /api/v5/account/risk-state` | 风控状态 | ❌ | |
| `GET /api/v5/account/max-withdrawal` | 最大提现 | ❌ | |
| 其余 `account/*`（VIP 借贷、MMP、Greeks、固定期限贷款等） | 借贷/期权/风控 | ❌ | 与本项目无关 |

#### 交易（Trade）

| 官方端点 | 用途 | 状态 | 实现位置 / 备注 |
|---|---|---|---|
| `POST /api/v5/trade/order` | 下单 | ✅ | rest.rs `submit_order`，实盘 e2e 验证 |
| `POST /api/v5/trade/batch-orders` | 批量下单 | ❌ | 建议优先补 |
| `POST /api/v5/trade/cancel-order` | 撤单 | ✅ | rest.rs `cancel_order`，实盘 e2e 验证 |
| `POST /api/v5/trade/cancel-batch-orders` | 批量撤单 | ❌ | |
| `POST /api/v5/trade/cancel-all-orders` | 全撤 | ✅ | rest.rs `cancel_all_orders`（官方端点，SDK 清单未收录） |
| `POST /api/v5/trade/amend-order` | 改单 | ❌ | 建议优先补 |
| `POST /api/v5/trade/amend-batch-orders` | 批量改单 | ❌ | |
| `POST /api/v5/trade/close-position` | 市价平仓 | ❌ | |
| `GET /api/v5/trade/order` | 查单 | ❌ | |
| `GET /api/v5/trade/orders-pending` | 未成交订单 | ❌ | |
| `GET /api/v5/trade/orders-history` | 历史订单 | ❌ | |
| `GET /api/v5/trade/orders-history-archive` | 历史订单归档 | ❌ | |
| `GET /api/v5/trade/fills` | 成交明细 | ❌ | |
| `GET /api/v5/trade/fills-history` | 历史成交 | ❌ | |
| `POST /api/v5/trade/mass-cancel` | 按条件批量撤单 | ❌ | |
| `POST /api/v5/trade/cancel-all-after` | 断线自动撤单（安全网） | ❌ | 建议优先补 |
| `POST /api/v5/trade/order-precheck` | 下单前检查 | ❌ | |
| 算法单 `trade/order-algo`、`cancel-algos`、`amend-algos`、`orders-algo-pending`、`orders-algo-history` | 条件/算法单 | ❌ | |
| `trade/easy-convert`、`one-click-repay` 等 | 兑换/还款 | ❌ | 与本项目无关 |

#### 行情数据（Market Data）

| 官方端点 | 用途 | 状态 | 实现位置 / 备注 |
|---|---|---|---|
| `GET /api/v5/market/books` | 订单簿快照 | 🔧 | rest.rs `get_books`，dead code，行情走 WS |
| `GET /api/v5/market/books-rpi` | RPI 订单簿 | ❌ | |
| `GET /api/v5/market/books-full` | 全量订单簿 | ❌ | |
| `GET /api/v5/market/tickers` | 全部行情 | ❌ | |
| `GET /api/v5/market/ticker` | 单个行情 | ❌ | |
| `GET /api/v5/market/candles` | K 线 | ❌ | |
| `GET /api/v5/market/history-candles` | 历史 K 线 | ❌ | |
| `GET /api/v5/market/trades` | 近期成交 | ❌ | 行情走 WS `trades` |
| `GET /api/v5/market/history-trades` | 历史成交 | ❌ | |
| `GET /api/v5/market/platform-24-volume` | 24h 成交量 | ❌ | |

#### 公共数据（Public Data）

| 官方端点 | 用途 | 状态 | 实现位置 / 备注 |
|---|---|---|---|
| `GET /api/v5/public/instruments` | 合约元数据 | ✅ | rest.rs `get_instruments`，实测（lotSz 精度） |
| `GET /api/v5/public/funding-rate` | 当前资金费 | ❌ | 资金费走 WS `funding-rate` |
| `GET /api/v5/public/funding-rate-history` | 资金费历史 | ❌ | |
| `GET /api/v5/public/open-interest` | 持仓量 | ❌ | |
| `GET /api/v5/public/price-limit` | 涨跌停价 | ❌ | |
| `GET /api/v5/public/time` | 服务器时间 | ❌ | |
| `GET /api/v5/public/mark-price` | 标记价 | ❌ | |
| `GET /api/v5/public/position-tiers` | 仓位档位 | ❌ | |
| `GET /api/v5/system/status` | 系统状态 | ❌ | |
| 其余 `public/*`（交割历史、期权、指数、保险基金等） | 其他公共数据 | ❌ | |

#### 其他业务线（全部未实现）

| 业务线 | 官方端点 | 状态 |
|---|---|---|
| TradingBot（网格/信号/定投） | `/api/v5/tradingBot/*` | ❌ |
| 跟单 Copytrading | `/api/v5/copytrading/*` | ❌ |
| 大宗交易 RFQ / Block | `/api/v5/rfq/*`、`/api/v5/market/block-*` | ❌ |
| 价差交易 Spread | `/api/v5/sprd/*` | ❌ |
| 市场数据 Rubik | `/api/v5/rubik/*` | ❌ |
| 资产（充提/划转/兑换） | `/api/v5/asset/*` | ❌ |
| 子账户 | `/api/v5/users/subaccount/*`、`/api/v5/account/subaccount/*` | ❌ |
| 理财/借贷 | `/api/v5/finance/*` | ❌ |
| 返佣/推广 | `/api/v5/affiliate/*`、`/api/v5/users/partner/*` | ❌ |
| 券商 Broker | `/api/v5/broker/*` | ❌ |

### 2.2 WebSocket 官方频道

#### 公共频道

| 官方频道 | 用途 | 状态 | 实现位置 / 备注 |
|---|---|---|---|
| `books` | 订单簿（400 档增量） | ✅ | public_stream.rs，实测 |
| `books5` / `books50-l2-tbt` / `bbo-tbt` | 精简订单簿 / 最优价 | ❌ | |
| `trades` | 逐笔成交 | ✅ | public_stream.rs，实测 |
| `tickers` | 行情快照 | ❌ | |
| `open-interest` | 持仓量 | ❌ | |
| `candle1m` ... `candle1M` | K 线 | ❌ | |
| `mark-price` | 标记价 | ❌ | |
| `index-tickers` | 指数行情 | ❌ | |
| `estimated-price` | 预估交割价 | ❌ | |
| `funding-rate` | 资金费推流 | ✅ | public_stream.rs → `LiveEvent::Funding`，实测 |
| `liquidation-orders` | 强平订单 | ❌ | |
| `adl-warning` | ADL 预警 | ❌ | |
| `status` | 系统状态 | ❌ | |
| `opt-summary` / `opt-trades` | 期权 | ❌ | |
| `rfq-*` / `sprd-*` | 大宗/价差 | ❌ | |

#### 私有频道

| 官方频道 | 用途 | 状态 | 实现位置 / 备注 |
|---|---|---|---|
| `orders` | 订单更新 | ⚠️ | private_stream.rs，登录签名实测通过 |
| `positions` | 持仓更新 | ⚠️ | private_stream.rs |
| `account` | 余额/权益更新 | ❌ | |
| `balance-and-position` | 余额 + 持仓 | ❌ | 建议优先补 |
| `orders-algo` | 算法单更新 | ❌ | |
| `algo-advance` | 高级算法单 | ❌ | |

#### WS 交易 API（Trading over WebSocket）

官方支持 `order` / `batch-orders` / `cancel-order` / `batch-cancel-orders` / `amend-order` / `batch-amend-orders` / `mass-cancel` 等 WS 私有指令，当前全部 ❌（下单走 REST）。

---

## 3. Hyperliquid

### 3.1 POST /info 官方请求类型（全量）

#### 基础 / 用户查询

| 官方 type | 用途 | 状态 | 实现位置 / 备注 |
|---|---|---|---|
| `meta` | 合约 universe（精度） | ✅ | client.rs `get_meta`，实测 |
| `allMids` | 全部最新价 | ❌ | |
| `clearinghouseState` | 账户状态 / 仓位 | ✅ | client.rs `get_clearinghouse_state`，实测 |
| `openOrders` | 未成交订单 | ✅ | client.rs `get_open_orders`，新标的初始化全撤用 |
| `frontendOpenOrders` | 前端展示订单 | ❌ | |
| `userFills` | 成交记录 | ❌ | |
| `userFillsByTime` | 按时间成交记录 | ❌ | |
| `userRateLimit` | 用户限频 | ❌ | |
| `orderStatus` | 按 oid/cloid 查单 | ❌ | |
| `historicalOrders` | 历史订单 | ❌ | |
| `userTwapSliceFills` | TWAP 分片成交 | ❌ | |
| `subAccounts` | 子账户列表 | ❌ | |
| `userVaultEquities` | 金库权益 | ❌ | |
| `vaultDetails` | 金库详情 | ❌ | |
| `userRole` | 用户角色 | ❌ | |
| `portfolio` | 组合历史 | ❌ | |
| `referral` | 返佣信息 | ❌ | |
| `userFees` | 费率 | ❌ | |
| `delegations` / `delegatorSummary` / `delegatorHistory` / `delegatorRewards` | 质押委托 | ❌ | |
| `userDexAbstraction` / `userAbstraction` | 抽象账户 | ❌ | |
| `borrowLendUserState` / `borrowLendReserveState` / `allBorrowLendReserveStates` | 借贷 | ❌ | |
| `approvedBuilders` | 已批准 builder | ❌ | |
| `maxBuilderFee` | builder 费率上限 | ❌ | |

#### 行情 / 深度 / K 线

| 官方 type | 用途 | 状态 | 实现位置 / 备注 |
|---|---|---|---|
| `metaAndAssetCtxs` | 合约元数据 + 行情（含资金费） | ✅ | client.rs `get_funding_rates`，60s 轮询 |
| `allPerpMetas` | 全部 perp 元数据 | ❌ | |
| `activeAssetData` | 活跃资产数据 | ❌ | |
| `fundingHistory` | 资金费历史 | ❌ | |
| `predictedFundings` | 预测资金费 | ❌ | |
| `userFunding` | 用户资金费流水 | ❌ | |
| `l2Book` | 订单簿快照 | ❌ | 只走 WS `l2Book` |
| `candleSnapshot` | K 线 | ❌ | |
| `perpAnnotation` / `perpCategories` / `perpConciseAnnotations` / `perpDexs` / `perpDexStatus` / `perpDexLimits` / `perpDeployAuctionStatus` / `perpsAtOpenInterestCap` | perp DEX 元数据 | ❌ | |
| `spotMeta` / `spotMetaAndAssetCtxs` | spot 元数据 + 行情 | ❌ | |
| `spotClearinghouseState` | spot 账户 | ❌ | |
| `spotDeployState` / `spotPairDeployAuctionStatus` | spot 部署 | ❌ | |
| `tokenDetails` | 代币详情 | ❌ | |
| `outcomeMeta` / `settledOutcome` | 结果市场 | ❌ | |

### 3.2 POST /exchange 官方操作（全量）

| 官方 action type | 用途 | 状态 | 实现位置 / 备注 |
|---|---|---|---|
| `order` | 下单 | ✅ | client.rs `post_exchange` + signing.rs，EIP-712 phantom agent，测试网 e2e |
| `cancel` | 按 oid 撤单 | ✅ | 同上 |
| `cancelByCloid` | 按 cloid 撤单 | ✅ | 同上 |
| （官方无全撤操作） | 全撤需自行遍历 openOrders 逐笔撤 | — | 当前实现即如此 |
| `scheduleCancel` | 定时全撤（安全网） | ❌ | 建议优先补 |
| `modify` | 改单 | ❌ | 建议优先补 |
| `batchModify` | 批量改单 | ❌ | |
| `updateLeverage` | 调整杠杆 | ❌ | |
| `updateIsolatedMargin` | 调整逐仓保证金 | ❌ | |
| `twapOrder` / `twapCancel` | TWAP 算法单 | ❌ | |
| `approveAgent` | 授权 API 钱包 | ❌ | |
| `approveBuilderFee` | 授权 builder 费率 | ❌ | |
| `sendAsset` / `agentSendAsset` | 转账资产 | ❌ | |
| `sendToEvmWithData` | EVM 跨链 | ❌ | |
| `usdClassTransfer` | USDC 划转 | ❌ | |
| `usdSend` | 发送 USDC | ❌ | |
| `spotSend` | 发送 spot 资产 | ❌ | |
| `withdraw3` | 提现 | ❌ | |
| `cDeposit` / `cWithdraw` | 质押存取 | ❌ | |
| `tokenDelegate` | 质押委托 | ❌ | |
| `vaultTransfer` | 金库划转 | ❌ | |
| `reserveRequestWeight` / `noop` | 限频 / 非ce作废 | ❌ | |
| `userSetAbstraction` / `agentSetAbstraction` / `agentEnableDexAbstraction` | 抽象账户 | ❌ | |
| 结果市场操作（split/merge/negate outcome、mergeQuestion） | HIP-4 结果市场 | ❌ | |
| `claimRewards` / `topUpIsolatedOnlyMargin` / `userOutcome` | 奖励 / 保证金 / 结果 | ❌ | |

> 官方还支持通过 WebSocket 发送 exchange 请求（post-requests），当前 ❌，一律走 REST /exchange。

### 3.3 WebSocket 官方订阅频道（全量）

| 官方频道 | 用途 | 状态 | 实现位置 / 备注 |
|---|---|---|---|
| `l2Book` | 订单簿 | ✅ | ws.rs，实测 |
| `trades` | 逐笔成交 | ✅ | ws.rs，实测 |
| `orderUpdates` | 订单状态更新 | ⚠️ | ws.rs，cloid/oid 双通道确认，状态机有单测 |
| `userEvents` | 用户事件（成交/持仓增量） | ⚠️ | ws.rs，解析有单测 |
| `subscriptionResponse` | 订阅确认 | ✅ | ws.rs 控制通道 |
| `pong` | 心跳应答 | ✅ | ws.rs 自动回 Pong |
| `allMids` | 全部最新价 | ❌ | |
| `activeAssetCtx` / `activeAssetData` / `fastAssetCtxs` | 活跃资产行情 | ❌ | |
| `allDexsAssetCtxs` / `allDexsClearinghouseState` | 多 DEX 行情/账户 | ❌ | |
| `bbo` | 最优买卖价 | ❌ | |
| `candle` | K 线 | ❌ | |
| `clearinghouseState` | 账户快照订阅 | ❌ | |
| `openOrders` | 未成交订单订阅 | ❌ | |
| `notification` | 系统通知 | ❌ | |
| `outcomeMetaUpdates` | 结果市场元数据 | ❌ | |
| `spotState` | spot 账户订阅 | ❌ | |
| `twapStates` | TWAP 状态 | ❌ | |
| `userFills` | 成交订阅 | ❌ | |
| `userFundings` | 资金费订阅 | ❌ | 当前 60s REST 轮询，建议改为该频道 |
| `userNonFundingLedgerUpdates` | 非资金费流水 | ❌ | |
| `userTwapHistory` / `userTwapSliceFills` | TWAP 历史 | ❌ | |

---

## 4. 汇总统计

> 2026-08-19：核心 REST 接口已按本表全部补齐，并新增统一 API 层
> [`BrokerApi`](src/api.rs)（三所共用统一返回结构），详见 [API_COVERAGE.md](API_COVERAGE.md)。

| 交易所 | REST 已实现 | REST 官方总量（核心组） | WS 已实现 | WS 官方频道 |
|---|---|---|---|---|
| Binance USD-M | 核心组全部 ✅（~60 端点） | ~90（核心） | 4 行情流 + 3 私有事件 | ~25 |
| OKX V5 | 核心组全部 ✅（~45 端点） | ~60（交易/账户/行情核心） | 5 核心 + 补充频道解析 | ~25 |
| Hyperliquid | info ~50 种 + exchange ~30 种全部 ✅ | info ~55 + exchange ~35 | 4 核心 + 补充频道解析 | ~25 |

## 5. 建议补齐优先级

> 2026-08-19：以下第 1-6 项已全部完成（安全网、查单、改单/批量、账户与杠杆、
> HL 资金费、OKX 账户频道解析）。

按实盘交易的刚需排序，建议下一步补：

1. ~~交易安全网~~：Binance `countdownCancelAll`、OKX `cancel-all-after`、HL `scheduleCancel` ✅
2. ~~查询补齐~~：openOrders/order/fills 三家 ✅
3. ~~改单与批量~~：Binance `submit_orders`/`modify_order` 接入；OKX amend/batch；HL modify/batchModify ✅
4. ~~账户与杠杆~~：balance/leverage 三家 ✅
5. ~~HL 资金费实时化~~：已从 60s 轮询改为 WS `userFundings` 推流（快照 + 每小时结算，主网实测） ✅
6. ~~持仓/账户增量~~：OKX `balance-and-position` 等频道解析已补齐 ✅
