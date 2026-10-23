# Gate CrossEx broker

2026-10-21。已将 `/Users/dominolu/dev/gate_arb` 的 CrossEx REST 包装和公私 WebSocket 调用迁移为 Rust broker，并修复[首轮完整性复审](verification/2026-10-21/gate-crossex-review/README.md)的四项 P1。实现：`connector/src/gatecrossex/`；默认 feature：`gatecrossex`。代码迁移的历史回归见[补齐验证记录](verification/2026-10-21/gate-crossex-complete/README.md)，当前修复与实盘验收见[实盘记录](verification/2026-10-21/gate-crossex-live/README.md)。

协议依据：[Gate CrossEx REST](https://www.gate.com/docs/developers/crossex/en/)、[WebSocket](https://www.gate.com/docs/developers/crossex/ws/en/)。公共交易规则是实际可用品种的依据；原生扩展保留服务端新增字段和十进制字符串。

## 原项目调用与迁移

| gate_arb 调用 | Titan 对应实现 |
| --- | --- |
| `request/_build_headers` | `crossex_request`；有序 query、API base path、实际 JSON 字节参与 APIv4 HMAC-SHA512 签名；所有写请求不自动重试 |
| `list_rule_symbols/list_rule_risk_limits` | 原生方法；`list_rule_risk_limits(&symbols)` 强制传入必填品种列表；统一 `get_instruments` 提供 FUTURE/SPOT/MARGIN 的规则、tick、lot、最小数量 |
| `get_account` | 原始 `get_account_raw`；统一 `get_account` 按币种汇总底层所资产，保留每币种原生数量 |
| `list_positions/list_margin_positions` | 同名原生方法；统一 `get_positions` 区分合约、杠杆持仓与现货库存，保留 LONG/SHORT 两条事实 |
| `get_position_leverages/set_position_leverage/get_fee` | 原生同名方法；统一杠杆按 FUTURE/MARGIN 路由，费率按底层所、业务及特殊品种覆盖 |
| `list_transfer_coins/list_transfers/create_transfer` | 原生同名方法；兼容 coin/asset、from/to 与 from/to_exchange/business_type，金额保持字符串；不会自行触发划转 |
| `create_order/get_order/cancel_order` | 原生 `create_order/query_order/cancel_order_raw`；统一 `submit_order/get_order/cancel_order`；补充 `update_order/amend_order` |
| `list_open_orders/list_history_orders/list_history_trades` | 原生同名查询；统一挂单、历史订单/成交，保留 order_id/text，历史查询分页 |
| `list_account_book` | 原生同名方法；统一 `get_income_history` 分页后按 symbol 本地过滤，不向服务器发送不存在的过滤参数 |
| `_login_message/subscribe_*/keepalive_loop` | 原生 `GateCrossExWebSocket`，及自动重连、订阅重放的 GateCrossEx 行情/账户工厂 |
| `private_ws_service` 的 order/position 和账户资产更新 | AccountPublication → AccountRuntime；通用恢复、提交隔离及账户一致性继续归平台层 |

另外提供账户模式更新、杠杆现货杠杆、历史持仓/利息、折扣率、利率、ADL、清仓、逐仓保证金、Convert、证券持仓和批量撤单等原生方法。它们仅在调用方显式调用时发送请求。`crossex_request` 支持未单独命名的 CrossEx 端点。

## 统一 broker 的语义

- symbol 保留 `EXCHANGE_BUSINESS_BASE_QUOTE`，包含中文币名及 `kPEPE` 等大小写；不推测底层所，不统一大小写。证券通过原生扩展访问，避免把本币报价误当成 USDT。
- FUTURE/SPOT/MARGIN 数量按基础币表达；`contract_multiplier = 1`，不使用已废弃的 `contract_size` 换算。限价单检查步长、数量、最小金额及最大数量；精度不符拒绝，不静默取整。验证对齐后按实际 tick/lot 精度序列化价格和数量，避免 `tick × 整数` 的浮点尾数进入请求；小于 1e-8 的步长也保留实际精度。
- 账户支持 CROSS_EXCHANGE；ISOLATED_EXCHANGE 必须配置 `exchange_type = "OKX"` 等底层所，并校验绑定范围。不会自动切换账户模式。
- FUTURE 使用账户 SINGLE/DUAL 模式；MARGIN 分 LONG/SHORT；SPOT 是 NET 库存。持仓损益按品种结算币种绑定，缺少相应 currency binding 时拒绝 bootstrap。现货库存来自对应底层所的 base 余额，成本价和损益未提供时不推算成本。
- LIMIT/MARKET、GTC/IOC/FOK、GTX→POC 可通过统一请求。**SPOT/MARGIN 市价买单必须使用原生 `create_order` 的 `quote_qty`**；统一请求只有基础币 qty，不能准确表达按金额购买，因此提前拒绝该组合。此类订单的原始生命周期用原生 WS/REST 跟踪，不混入基础币数量订单投影。RPI 和证券订单也通过原生请求表达。
- 下撤改单只返回身份时表示受理；统一接口补查实际订单，补查失败返回结果未知，交平台对账。改单最多用 3 秒只读轮询确认所请求的价格和数量；旧字段、冲突终态或超时不能表示改单成功，写请求不会重发。写操作不会自动重发。批量统一接口对多请求拒绝，避免丢失部分成功信息；使用 `submit_order_results/cancel_order_results` 获得每个输入的成功/失败，或使用服务端原生 `batch_cancel_orders` 保留每项 accepted/label/message。
- 累计均价与新增成交价格分别发布。累计数量差防止重复成交；累计金额差用于合并成交的新增均价；有匹配数量的 last_executed_price 时使用该价格。缺少必要基线时触发对账。REST 恢复更新成交基线，旧快照不能覆盖更晚 WS 事实。
- CancelAll 继续尝试所有初始挂单，最后以权威挂单查询确认结果；单笔成交/撤单竞态不会跳过其余订单。

`AccountInfo.balances` 保留原币数量。总钱包和总损益以新鲜的 CrossEx SPOT coin/USDT ticker 换算；不假定 USDC 等于 USDT。缺少可用汇率或行情过期时拒绝输出总估值。交易所返回的折扣保证金、可用保证金原样保留，不能反推钱包余额；这些总额只是同一账户事实与所取行情的估值，并非跨接口原子价格快照。原生 `get_account_raw` 始终保留未转换资产及负债明细。

私有流确认所有必要频道后发布 ready，由 AccountRuntime bootstrap 后开放提交。默认 order/position/asset；绑定 MARGIN 时再订阅 margin_position/margin_interest。事实位于 payload，即使 event=subscribe 也处理。资产/利息触发合并的后台权威刷新，接收循环继续处理订单和心跳；刷新失败、超时或会话失效进入重连对账，旧会话的任务被取消。现货资产变化刷新库存，杠杆利息刷新相关负债数量。

本次集成还修复 PositionChangedV1 的序列化重叠：未实现损益在字节 88..96，币种 ID 在 96..100。事件仍为 100 字节。以前按错误偏移 92 手工读字段的消费者应同步修正；历史错误帧无法可靠恢复被覆盖的损益。

## 行情与原生 WebSocket

MarketService 支持 Depth/Bbo/Trades/MarkPrice/FundingRate，独立公私连接，协议 Ping，心跳超时及订阅重放。有限档盘口为完整替换图像，没有增量序号时 sequence=0。默认 5 档；BYBIT 使用独立 1 档配置；KRAKEN 的增量书可经原生 WS 使用，但不能直接映射为有限档快照。不同品种/底层所的频道可用性以服务器响应为准。

统一 `get_ticker/get_tickers/get_order_book/get_funding_rate` 使用公开 WS 快照。Gate 的 `get_open_interest` 使用其官方公开 futures ticker 的 `total_size` 换算为基础币，时间戳为查询接收时间，避免依赖未必提供首条数据的 CrossEx OI 频道；其他支持的所使用 CrossEx OI 推送。全市场 ticker 分组读取并有完整性检查，可能较慢；需要低延迟时绑定目标品种并使用长连接。CrossEx 未提供 REST 历史公共成交、K 线、历史资金费率和服务器时钟接口，相关统一历史方法返回 UNSUPPORTED；不会拿当前数据充当历史。Binance 的 open_interest 频道也未提供。

实盘核对发现：GATE 和 OKX FUTURE 公开盘口、BBO 和逐笔成交保留底层所的合约数量，不能把它们直接当作基础币。统一输出对 Gate 使用原生 `quanto_multiplier`，对 OKX 线性合约使用以 base 计价的 `ctVal × ctMult`。这是独立的行情换算，**REST 订单数量仍是基础币，contract_multiplier 仍为 1**。盘口以精确十进制乘法输出，避免浮点尾数导致 MarketService 的精确 lot 解析失败。24 小时基础币成交量不重复换算。

Gate/OKX 行情启动及查询会读取对应官方公共合约元数据；读取失败、合约身份或单位不匹配时拒绝输出数量。可在配置中提供经核对的 `[market_quantity_multipliers]`，键为完整原生 symbol，值为一份推送数量对应的基础币量；不能凭 `lot_size` 或已废弃的 `contract_size` 猜测。尚未确认数量单位的其他所需显式配置；原生 WS 始终保留原始数据。实际日本服务器对 OKX 原生公共 REST 返回了 HTTP 错误，因此该路径未作成功验收，不能依靠默认 1 绕过。

原生 WS 可订阅 last_price/index_price/ticker/trade/kline_x/funding_rate/open_interest/order_book_update，及全部私有频道；`api(channel, payload)` 提供官方 WS 写接口。返回 CrossExMessage 包含 payload、result、时间和完整 raw，保留利息、手续费、ADL 等统一 ABI 未表达的信息。

`subscribe/unsubscribe` 只确认请求已发送；公共频道不保证确认帧，实际服务端消息通过 `next()` 获取。私有频道可使用 `subscribe_confirmed` 等待确认，数据 payload 不会误当确认。原生连接拥有有界队列和 Ping/Pong；断线/溢出会报错，调用方显式重连、重放订阅，**不自动重发 WS api 写请求**。工厂中的公共/账户流负责各自自动重连。

## 接入 Titan

静态工厂自动注册：行情 `gate-crossex`，账户 `gate-crossex-account`。可参考 `connector/examples/gatecrossex.toml`。在 runtime.toml 的 market_sources / accounts 中设置对应 connector_type 和 connector_config，绑定同一原生 symbol；策略使用现有 local_asset/local_account 绑定。

账户示例（初始关闭；价格和数量步长必须按 rule/symbols 的目标品种核对）：

```toml
[[accounts]]
account_key = "gate-crossex-mainnet"
account_id = 3001
connector_type = "gate-crossex-account"
credential_ref = "secret://file/gate-crossex.toml"
connector_config = '''
rest_url = "https://api.gateio.ws/api/v4"
public_ws_url = "wss://api.gateio.ws/ws/crossex/public"
private_ws_url = "wss://api.gateio.ws/ws/crossex"
'''
instruments = [{ native_symbol = "OKX_FUTURE_BTC_USDT", asset_id = 3001, price_tick = { coefficient = 1, scale = 1 }, quantity_lot = { coefficient = 1, scale = 3 }, contract_multiplier = { coefficient = 1, scale = 0 } }]
currencies = [{ native_currency = "USDT", currency_id = 1, amount_unit = { coefficient = 1, scale = 8 } }]
ownership = "ObserveAll"
shutdown_order_policy = "CancelAll"
enabled = false
definition_version = 1
```

凭据由已有 SecretProvider 读取，文件内容为 `api_key = "..."`、`secret = "..."`，不要放入公开配置。AccountRuntime 的 LeaveOpen/CancelAll 策略维持既有语义；CrossEx 无倒计时接口，不能配置 CancelAllAfter。CancelAll 会取消已绑定 symbol 的全部挂单，需为该账户配置与运行计划一致的 ownership 和品种。

直接 REST 调用：

```rust,ignore
use connector::{api::BrokerApi, gatecrossex::GateCrossExClient};
let api = GateCrossExClient::new("https://api.gateio.ws/api/v4", &key, &secret)?;
let instruments = api.get_instruments().await?;
```

## L2 本地热路径

默认 `borrowed_depth = true`：常见有限档 L2 直接借用 WS 帧内字符串，16 档/侧以内使用内联 SmallVec。每个会话预编译合约数量乘数，通过公共 `NativeDepthLevels::ScaledBorrowed` 交给 MarketEventBridge，直接精确转换成 ABI 整数 lot，避免逐档创建价格/数量 String、浮点乘积和中间格式化。更深盘口可能溢出内联缓冲并分配，但仍借用每档字符串。转义字符串及其他频道保留通用解码回退；无效或未对齐数量拒绝发布并进入现有恢复流程。

字符串借用只持续到同步发布返回；异步策略队列持有已编码的 EventArena 数据，不保存 WS 帧引用。Market ABI V1 字节布局不变。Rust `NativeDepthLevels` 新增带显式乘数的变体，使用该 Rust 接口的外部穷举匹配需处理它；原有 Owned/Borrowed 语义保留。REST 订单数量和私有流未参与该优化。

`receive_ts` 统一在 WS 帧返回后、JSON 解码前记录。`borrowed_depth = false` 可恢复通用解码路径，但仍保留正确的收帧打点。真实 WS 收帧到 V13 策略回调的对照方法、分配审计及其他 broker 适配分析见[本地处理优化报告](verification/2026-10-21/gate-crossex-local-optimization/README.md)。

## 原生调用示例

```rust,ignore
use connector::gatecrossex::{GateCrossExClient, GateCrossExWebSocket};
let api = GateCrossExClient::new("https://api.gateio.ws/api/v4", &key, &secret)?;
let margin = api.list_margin_positions(&vec![]).await?;
let book = api.list_account_book(&vec![("statement_type".into(), "FUNDING_FEE".into())]).await?;
let mut ws = GateCrossExWebSocket::connect(&api, true).await?;
ws.subscribe_confirmed("margin_interest", &["!all".into()]).await?;
let message = ws.next().await?;
// Explicit mutation examples are intentionally omitted from startup code.
```

无凭据公开探测：在编译服务器执行 `cargo run --locked -p connector --example gate_crossex_market_stream_probe` 检查五类长连接数据，`cargo run --locked -p connector --example gate_crossex_public_api_probe` 检查规则及查询式快照。2026-10-21 已在指定实盘服务器完成 Gate 账户的私有登录、快照、AccountRuntime bootstrap、主动重连及 BTC USDT 最小数量 post-only 下单/改单/撤单，REST 与私有 WS 均确认，0 成交、0 BTC 持仓、无测试挂单。详见[实盘报告及验收边界](verification/2026-10-21/gate-crossex-live/README.md)。没有转账、改模式或启动长期实盘策略；其他底层所及真实成交链路不由此次通过结果代替验收。
