# Lighter、Backpack、Variational 接入与全量原生 API

协议核对日期：2026-10-30～31。包含 Rust connector、统一行情与账户运行时，以及用户追加要求的全部已公开原生 API。实现接口不等于执行真实账户操作；本轮没有启动实盘策略、转账提现或修改账户模式。

## 官方来源和边界

| 交易所 | 官方接口 | 关键约束 |
| --- | --- | --- |
| Backpack | [REST/WS](https://docs.backpack.exchange/)、[官方 Rust SDK](https://github.com/backpack-exchange/bpx-api-client) | Ed25519/base64；请求参数字母排序，指令前缀，毫秒 timestamp/window；WS 时间为微秒；clientId 仅 u32；深度 REST 快照与 U/u 增量必须衔接 |
| Lighter Core | [开发文档](https://docs.lighter.xyz/developers)、[API](https://docs.lighter.xyz/api/readme)、[官方 signer](https://github.com/elliottech/lighter-go) | 独立 account_index/api_key_index/chain_id；每密钥 nonce；价格/数量整数缩放；签名交易受理不等于订单已生效；Core 与 Robinhood 部署不得混用 |
| Variational Omni | [API](https://docs.variational.io/technical-documentation/api) | Trading API 尚未向任何用户开放；只能 GET `/metadata/v2/stats`，10 请求/10 秒/IP；报价可缓存 600 秒；PERP 和 SWAP 的资金费率结构不同 |

Variational 只实现官方公开统计客户端。不得逆向网页会话或把 indicative quotes 当成可执行 BBO/深度，不伪造精度、成交量单位、交易时间或交易能力。完整交易接入等待官方开放。

## 任务和验收

1. 已完成协议分析：固定来源、请求/响应语义、鉴权、精度、订单身份、重连及能力边界。
2. 已完成公共基础：HTTP 超时/错误、数值校验、数字 clientId 的耐久双向映射、任务回收与失败关闭。
3. Backpack 已实现并通过模拟测试：REST 元数据/行情/账户/下撤查单，公共 WS 的快照衔接，私有订单/原生成交/持仓，工厂注册及配置；真实公开与私有验收尚待通过。
4. Lighter 已实现并通过模拟及公开测试：REST、Core 公共与私有 WS、官方 signer 的 Rust FFI、串行 nonce、下撤改单确认，工厂注册及配置；真实私有验收尚待执行。
5. Variational 已完成官方可用范围并通过公开测试：有类型的 v2 stats、独立 PERP/SWAP 费率、缓存时间与单位、429 重试信息；交易能力等待官方开放。
6. `192.168.3.88` 已通过源码摘要、fmt、Clippy、单元/模拟 HTTP/WS、断线/序号/错误响应/身份恢复、工作区回归；三家公开接口结果分别记录，不将部分成功写成全通过。
7. 已完成文档和记忆：配置、安全边界、命令与实际结果。实盘部署和最小风险订单另需账户与风险参数，不将缺少实盘凭据视为已验收。
8. 已完成用户追加的全量原生 API：181 个 HTTP 操作、全部文档 WS 流模式、Lighter 23 类签名与工具入口；逐路由协议测试和官方来源/头文件覆盖审计均通过。

## 当前状态

Rust 连接器及全部已公开原生 API 的代码与非实盘验收已完成；最新[审查修复验收](verification/2026-10-31/native-venues-review-fixes/README.md)通过格式检查、Clippy、710 项 Rust 测试、2 项 doctest 和独立官方 signer 测试，651 份输入在整轮检查前后及本地一致。新增五项回归覆盖长连接、可选时间字段、重复查询签名和提现收款信息；同期策略迁移提交减少一项既有 CLI 测试。[全量 API 验收](verification/2026-10-31/native-venues-all-api/README.md)中的 706/660 及[首次连接器验收](verification/2026-10-31/native-venues/README.md)作为历史证据保留。

Lighter 公开行情/历史及新增系统、资产、费率和 Explorer 探针通过；Variational v1/v2 公开统计均通过。Backpack 本轮公开请求仍被连接重置，不能记为公开接口验收通过。真实私有账户、资金/账户管理、下单/撤单及恢复尚未验收，等待目标账户、实盘节点及各操作参数和风险范围。Variational 交易接入继续受官方 Trading API 未开放阻塞，不能把三家全部记为可实盘交易。

## 全量原生 API

[逐接口目录](exchange_api_catalog.md)对应 [HTTP 来源清单](../connector/specs/manifest.json)和[签名 ABI 清单](../connector/specs/lighter-signer.json)。按当前官方 OpenAPI、固定版本官方 SDK 与公开文档核对：

| 原生接口 | 覆盖 |
| --- | --- |
| Backpack HTTP | 75 个命名操作：账户、资产/抵押、借贷、资金/钱包、现货及永续订单/批量、RFQ/报价、Vault、股票日历/报价/公司行动、全部已发布历史和行情路径 |
| Lighter HTTP | 104 个命名操作：91 个 exchange 操作和 13 个 Explorer 操作，包含账户/API key/token、交易与批量、桥/充值提现、RFQ、推荐/积分/租赁、二元期权、指标与历史 |
| Variational HTTP | 推荐 v2 stats 和仍提供的废弃 v1 stats，两条原生入口保留完整字段；Trading API 等待官方开放 |
| Backpack WS | 20 个流模式，含私有账户/资金费/RFQ、所有公开行情/股票流及不同深度聚合周期，支持 includeVerifyingKey |
| Lighter WS | 25 个频道模式、订阅/退订、JSON/二进制消息、应用层 ping/pong；另支持 jsonapi/sendtx 与 sendtxbatch |
| Lighter signer | 全部 23 个 Sign* 交易，含 grouped/条件订单、转账/提现、子账户、资金池/份额、质押、保证金/模式、integrator、二元期权 operator；另有生成 key、auth token 和显式 check_client |

原生接口不经过统一模型裁剪。各操作提供 `native_<operation>` 命名方法和 Operation 枚举；`NativeRequest` 分开携带 path/query/header/body，保留 bool、整数和十进制字符串。结构校验处理 required、原生类型、枚举和数值/长度边界；它不宣称实现全部 JSON Schema 或交易所业务校验。响应保留 JSON 字段，也可用 `bytes` / `into_response` 读取 CSV 和流式导出。错误不回显鉴权请求或任意响应正文，写请求不自动重试。

```rust
use connector::{native_api::NativeRequest, lighter::LighterNativeClient};

let native = LighterNativeClient::default();
let config = native.native_system_config(NativeRequest::default()).await?.json().await?;
let markets = native.native_explorer_get_markets(NativeRequest::default()).await?.json().await?;
// BackpackClient 同样提供 native_get_assets、native_get_balances 等逐操作方法。
```

`LighterNativeClient::new(core_url, explorer_url)` 显式区分部署和 Explorer。官方仅在特定部署提供的操作会核对其 servers 清单；Core 测试网不能静默读主网 Explorer。`with_authorization` 接受每次调用生成 token 的 provider，`LighterClient::native()` 可复用当前 signer 的授权。Explorer 不发送账户 token。

NativeTransaction 的字段与新官方 C 头文件逐项对应，不按市场 ID 区间推断 perp/spot/binary 类型。签名下标使用 i64，原生有效 market index 为 48 位、255 保留；元数据、WS 映射与签名全链路不再截断为 i32。需要 L1 钱包授权时保留 `message_to_sign`，通过外部钱包 EIP-191 签名后用 `with_l1_signature` 写入原生 `L1Sig`；客户端只校验签名编码，不替代链上所有权验证。

`execute_native_transaction` 对不需要 L1 补签的交易复用平台 nonce 串行状态及最终守卫，返回 sendTx 受理，不代表业务完成。`sign_at_nonce`、原生 sendTx/sendTxBatch 与 WS RPC 是显式 nonce 流程，调用方必须独占执行身份并按原身份对账；不可与平台账户 writer 或外部下单器独立混用。原生 clientId 同样由调用方明确管理。`check_client` 会读取真实账户，必须在实盘节点按授权范围执行，本轮未调用。

## 统一接口及运行时能力

本节描述可移植 BrokerApi 与自动事实投影的范围；上面的原生接口覆盖不受此表裁剪，也不会自动获得统一账户/策略的执行能力。

默认 Cargo features 已包含 `backpack`、`lighter`、`variational`。静态目录注册行情 `backpack` / `lighter` 和账户 `backpack-account` / `lighter-account`；由既有 AccountRuntime / MarketConnectorRuntime 负责生命周期、bootstrap、风控与事实投影。Variational 没有交易或可执行行情工厂。

| 能力 | Backpack | Lighter Core | Variational Omni |
| --- | --- | --- | --- |
| 市场规则、原生 symbol | 线性 USDC PERP；原生 `BTC_USDC_PERP` | Core USDC 永续；原生 `ETH`、`BTC`，API 市场 ID 独立保存 | v2 原生 ticker 区分 `BTC-PERP` / `XAU-SWAP`，没有下单精度事实 |
| 盘口、BBO、公共成交、标记价格、资金费 WS | 实现 | 实现；公开连接使用官方 `readonly=true` | 没有官方可执行 WS；不创建假盘口 |
| 深度连续性 | REST 快照 + `U/u` 首次覆盖、后续严格连续 | 订阅完整快照；`begin_nonce` 接续上次 `nonce`，不按 offset 判缺口 | 不适用 |
| 下单 | Limit / Market；GTC / IOC / FOK / post-only | 有价格保护的 Limit / IOC / post-only | 官方尚未开放 |
| 撤单、按品种全部撤单、查单 | 实现；404 后查真实终态历史 | 实现；写交易后只读确认，不重发写请求 | 不可用 |
| 原子改单 | 原生没有 modify 接口，显式 UNSUPPORTED | 实现；新总数量含已成交部分；等待价格和总数量同时确认 | 不可用 |
| 余额、持仓、原生独立成交与费用 | 实现；只有 USDC 抵押域 | 实现；当前要求 Simple、cross USDC；不自动切换模式 | 不可用 |
| 逐笔已实现盈亏 | 官方成交没有此字段，明确 unavailable | 原生成交字段；文档规定省略零值 | 不可用 |
| 历史数据 | 订单、成交、资金费、固定周期 K 线 | 有界订单历史、分页原生成交、K 线、逐小时资金费历史 | 当前统计，不伪造历史 |
| 账户级操作 | 不提供转账、提现、自动改模式/杠杆 | 不提供转账、提现、自动改模式/杠杆 | 不可用 |

没有静默把不支持的类型转换成另一种类型。Lighter 的统一无价格 Market 请求不提供最差成交价格，故拒绝，调用方使用显式价格的 IOC Limit。统一 Broker 的批量下单仅接受单条，避免丢失多笔写操作部分成功结果；批量撤单继续处理其余请求，但任何单笔失败都会返回失败。原生层另提供官方完整批量接口并保留逐项结果。统一层没有声明账户级 dead-man switch；不能配置 `CancelAllAfter`。

### 接口分解

| 范围 | Backpack | Lighter Core |
| --- | --- | --- |
| 公共 REST | `/api/v1/markets`、`ticker(s)`、`depth`、`trades`、`klines`、`markPrices`、`fundingRates`、`openInterest` | `/api/v1/orderBookDetails`、`trades`、`candles`、`fundings`；OI 使用市场详情 |
| 公共 WS | 原生 depth/trade/ticker/markPrice 订阅 | `order_book`、`trade`、`market_stats`；统一查盘口和当前资金费也从完整订阅快照读取 |
| 账户与历史 | `/api/v1/account`、`capital`、`capital/collateral`、`position`、`order(s)`；`/wapi/v1/history/orders`、`fills` | `/api/v1/account`、`accountActiveOrders`、`accountInactiveOrders`、带账户身份的 `trades` |
| 写请求与鉴权 | `POST/DELETE /api/v1/order`、`DELETE /api/v1/orders`；Ed25519 请求签名与签名私有订阅 | `nextNonce` + 官方 signer + `sendTx`；API key 授权令牌用于私有读取和订阅 |
| 私有 WS | `account.orderUpdate`（含独立成交字段）、`account.positionUpdate`、`account.balanceUpdate` | `account_all_orders`、`account_all_trades`、`account_all_positions`、`user_stats` |

实现只消费各所可证明的字段。Backpack 的订单累计成交不替代独立执行；Lighter 的 `sendTx` 受理不替代订单事实。历史查单和成交扫描均有上界，超出范围明确失败，不把有界未找到当作确定不存在。

### 协议细节

- Lighter 签名价格和数量按 **`price_decimals` / `size_decimals`** 放大；`supported_*_decimals` 只定义允许的 tick/lot。先校验步长，再按原生精度转整数，拒绝溢出和不对齐，不能静默四舍五入。原生价格 u32、数量/client index u48。
- Lighter 官方 `Trade.*_fee` 为 ppm 费率，以原生 `usd_amount` 换算实际 USDC 费用，包含独立 integrator 费率；不把费率当作已支付金额。Core 抵押账本保留 USDC 的 12 位小数，币种绑定应使用 `amount_unit = { coefficient = 1, scale = 12 }`，并核对账户总额在 i64 范围内。签名不附加 builder/integrator 收费。
- Lighter 当前市场资金费字符串使用百分数，统一输出除以 100；`funding_timestamp` 为上一小时结算的毫秒时间，按官方逐小时规则给出下一结算时间。完整 Order 的 `timestamp` 是秒；Trade 的 `timestamp` 是毫秒；`transaction_time` 和盘口 `last_updated_at` 是微秒。统一 REST 时间为毫秒，市场/账户 ABI 时间为纳秒。
- Lighter 的 candles/fundings 查询时间使用秒；真实 candles 响应的 `t` 为毫秒，fundings 的 `timestamp` 为秒。K 线支持 1m/5m/15m/30m/1h/4h/12h/1d，最多 500 条；资金费历史最多 750 条。历史费率以无符号百分数配合 `direction` 返回，按付款方 long 为正、short 为负转换；该方向映射由官方正负费率定义和本轮原生响应核对得到，接口 schema 本身只声明字符串类型。
- Backpack 签名按官方排序串实现；重复查询键必须保留全部值并按参数名稳定排序。提现的 `recipientInformation`（对象或 null）原样发送，但按官方要求排除在签名串外。JSON 请求保持 bool/整数/十进制字符串类型。WS `T/E` 是微秒，markPrice 的 `n` 是毫秒，不能统一乘同一个因子。新订单/撤单响应解析或身份核对失败保留未知结果，不自动重发。
- Backpack 的公共 Trade 时间与文档字符串示例不同，官方 Rust SDK 使用整数；解析兼容 ISO 字符串和当代 Unix 毫秒/微秒两个不重叠数值区间，拒绝秒值及模糊区间。此处是兼容规则，不能推广为其他字段的统一单位；公开 OI 响应时间为毫秒。
- Backpack 的 `capital.available` 是资产可用数量，不能代替可用保证金。平台币种投影使用 `capital/collateral.netEquityAvailable` / `netEquity` / `pnlUnrealized`；私有事件触发会话内、合并的后台刷新，REST 总超时和旧会话回收有界。刷新期间若有新私有事实则丢弃旧 REST 结果，失败关闭账户流。
- Backpack 缺少逐笔 realized PnL，原生成交使用新的 `REALIZED_PNL_UNAVAILABLE` 账户事件标记。序列化长度和 schema 不变，事实历史显示 null；旧交易所的已知 PnL 指纹保持原样。缺失字段不能证明零盈亏，原生成交历史恢复对此保持拒绝；账户余额和持仓 PnL 仍使用各自权威事实。部署必须包含此次对应的账户观察器及恢复代码。
- 私有订阅必须得到原生数据/确认才能发布 ready，静默连接不代表认证成功。Backpack 通过签名批量订阅的有效初始 position image 确认；空数组合法，若服务端空账户不发送初始 image，则保持未就绪，不以超时放行。Lighter 等待 orders/trades/positions/user_stats 四个频道的有效初始确认，每个会话只发布一次 ready；无执行时间的持仓快照允许省略 `transaction_time`，按零表示未知，不使用发布时钟冒充执行时间。
- Lighter 健康连接只做心跳保活，不因客户端固定 500 秒计时主动失效；真实重连重新生成订阅授权，初始化确认仍有 15 秒上限。真实断线或心跳超时仍触发原有账户失效和对账流程。

## 配置

公开配置见 [Backpack](../connector/examples/backpack.toml)、[Lighter](../connector/examples/lighter.toml)。市场源通过 `connector_config` 引用相应 TOML 文本，账户同样使用该文本并通过现有 SecretProvider 合并凭据。原生 symbol、tick/lot、minQty 均需从当次元数据确认；不直接套用别所的绑定。

Backpack 私有凭据 TOML（由 SecretProvider 管理，不提交仓库）：

```toml
api_key = "<base64 Ed25519 verifying key>"
secret = "<base64 32-byte Ed25519 seed>"
client_id_journal = "/var/lib/titan/backpack-main-client-ids.jsonl"
```

Lighter 私有配置/凭据 TOML：

```toml
account_index = 12345 # 实际目标账户，不使用本例数字
api_key_index = 2
api_private_key = "<Lighter API private key，非 L1 钱包私钥>"
signer_path = "/opt/titan/lib/lighter-signer.so"
signer_sha256 = "<本次编译服务器产物的 SHA256>"
client_id_journal = "/var/lib/titan/lighter-main-client-ids.jsonl"
```

统一 Broker 测试网需同时修改 Core REST/WS 地址和 `chain_id = 300`；主网为 304。统一层目前只自动投影 Core USDC perpetual / Simple / cross；原生层提供 Robinhood 部署、spot、binary options、Unified/isolated 配置等官方接口，由调用方显式选择，不自动切换。每进程使用同一版本/网络的 signer，可容纳多个不同 account/API key；同一 key 的平台写请求共享串行 nonce 状态，不在同一进程混用 Core/RH 网络。

### 数字订单 ID 与 nonce

Titan 的完整客户端身份保存在持有文件锁的追加日志中，分配递增数字 ID，写入并 fsync 后才能进入写请求。日志同时保存下单 reduce-only 语义，以补全 Backpack 历史接口省略的自有订单属性。文件头绑定交易所、网络与凭据/账户；不接受尾部截断、重复映射、身份冲突或容量耗尽。同一进程的账户替换候选与前代共享同一个日志句柄和映射；文件锁仍阻止其他进程进入。每账户/执行 key 独占该日志和执行所有权，不与其他进程或外部下单器独立分配同一数字域。不要删除日志后从 1 重新开始；换 key/迁移前须完成账户对账。

Lighter 每 key 的 nonce 在最后一次风控检查后进入传输；传输开始前就标记结果未确定，future 被取消也不能复用。受理后只增加一次；失败或未知结果冻结该 signer。须先对账并进行进程级重启；仅热替换账户不会清除冻结。ACK 后查不到订单不当作失败重下，原身份与平台预占保留。

## signer 构建与验证

只在编译服务器执行：

```sh
bash scripts/build_lighter_signer.sh /tmp/new-lighter-source /opt/titan/lib/lighter-signer.so
```

脚本固定官方 [lighter-go aefa968 提交](https://github.com/elliottech/lighter-go/tree/aefa96848a105d82a6bbb55d3d05663293a71713)，稀疏检出源码，使用 Go + CGO 构建 `.so`，输出头文件、许可证和构建身份。此版 market index 的 C 参数为 64 位；首次接入的 v1.0.10 / 32 位 ABI 已被替换，旧库会拒绝加载，需重建并更新 SHA256。23 类签名绑定在编译服务器按其实际 C 头文件生成，不任意替换同名库；加载前校验配置摘要，不在运行时下载库，也不把密钥传给 Python 或子进程。

`native_abi_serializes_nonce_freezes_unknown_and_checks_final_guard` 使用明确的 C 模拟库校验参数及 HTTP 行为；`official_signer_offline_abi_roundtrip` 另外使用编译服务器生成的真实官方库、随机未注册密钥、回环地址和显式 nonce 校验签名，**不执行 CheckClient、不连接账户、不发送交易**。

公开验证入口：

```sh
cargo run -p connector --example native_venues_public_probe -- all
# 可单独指定 lighter / backpack / variational
```

真实交易验收仍需实盘服务器、明确的账户/品种/数量/价格保护和风险上界。代码、模拟或公开行情通过均不等于真实账户通过。
