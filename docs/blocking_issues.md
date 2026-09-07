# 阻碍性 Bug / 阻塞项清单

状态：本文件只收录“当前无法仅在代码内闭环解决、需要外部环境或进一步现场验证”的问题。
可内部修复的问题不在本清单，直接按主清单执行。

更新时间：2026-09-07

## 汇总

| ID | 标题 | 严重度 | 当前状态 | 主要阻塞条件 |
|---|---|---|---|---|
| B-01 | Binance 私有流偶发连接后零 executionReport | 高（影响“静默断流”保证） | 观测中，未定位根因 | 需要持续主网运行 + 日志/抓包复现 |
| B-02 | OKX/Hyperliquid 实盘私有流与字段语义验收 | 高（阻断多交易所闭环） | 2026-09-07 已完成并解除 | 无 |
| B-03 | 生产 P99/P99.9 PerformanceEnvelope 冻结 | 中（不阻塞功能） | 2026-09-07 已按目标机 300k/s + HL 主网 60s 契约冻结 | 无 |
| B-04 | 多 venue e2e/roundtrip 覆盖在方案 B 删除后未补齐 | 中 | OKX/Hyperliquid venue 级闭环均已由探针补上 | Binance 静默断流仍归 B-01 观测 |

---

## B-01 Binance 私有流偶发连接后零 executionReport

现象（2026-09-04 主网探针）：

- WebSocket 握手成功，日志输出 `binance user data websocket connected`；
- 随后约 90 秒内未收到任何 `executionReport`，即使 REST 下单已成功、撤单已成功；
- 立即重跑同一探针则完全正常（NEW/CANCELED 均由私有流回传），账户终态零挂单零仓位。

已排除/已缓解：

- 连接地址已迁移到 `wss://fstream.binance.com/private/ws?listenKey=…&events=…`（旧 `/ws/{listenKey}` 路由在 2026-04-23 退役，这是此前 90s 无帧的主因，但本次复测在该迁移之后仍出现一次）；
- 连接层已有 300s 无服务端 Ping 超时保护；
- 探针失败分支会撤销挂单并最终 reconcile，不留脏状态。

未解决根因候选：

1. listenKey 在连接建立与订阅注册之间被服务端轮换/失效，且未推送 `listenKeyExpired`；
2. `/private/ws` 偶发建立“空订阅”连接（events 参数未生效）而 TCP/WS 层不报错；
3. 服务器或中间链路偶发丢弃 executionReport 帧，无应用层心跳佐证。

验收/解除条件：

- 在目标机持续主网观测（含真实订单活动或周期性事件注入）一段稳定时间，记录每次连接、收帧时间线；
- 对“连接成功但 N 秒无业务帧”给出应用层可观测信号（计数/日志/健康状态），并能在复现时提供 listenKey 生命周期证据；
- 复现一次即可：抓取 WS 帧时间线、listenKey keepalive 时间、REST 事件时间做三方对照。

当前缓解措施：探针在 90s 无私有流事实时 fail-fast 并自清理；真实 AccountRuntime 依赖 reconcile 权威状态，不会仅因私有流空转而漏报持仓/订单终态。

---

## B-02 OKX / Hyperliquid 实盘私有流与字段语义验收

### OKX（2026-09-05 已完成）

在目标机上用 OKX 主网真实完成验收，探针
`connector/examples/okx_account_rest_ws_probe.rs`（私有流）与
`connector/examples/okx_market_stream_probe.rs`（公共流）连续两代通过：

- 公共流：XRP-USDT-SWAP 的 Depth snapshot/delta（`books`）、Trade、BBO（`bbo-tbt`）、
  FundingRate 全部实时到达 direct publisher；
- 私有流：login → orders/positions 订阅 → READY → Full reconcile；REST 提交
  post-only 卖单 → 私有流 NEW → REST 撤单 → 私有流终态 CANCELED → Full reconcile；
- 字段一致性实测通过：32-hex `clOrdId` 三路径（REST 命令回执、私有流、reconcile 快照）
  一致；`ordId` 经 IdInterner 在 REST 与私有流两侧归一为同一 Id128；私有流
  `uTime`（ms→ns）与 reconcile 快照 `uTime` 完全相等且毫秒对齐；撤单响应 `ts`
  回填后 REST 撤单事实与私有流终态 exchange_ts 完全相等；
- 语义差异已确认：OKX 价格限制按品种配置（`/api/v5/public/price-limit` 的
  `buyLmt`/`sellLmt`，XRP-USDT-SWAP 当前约为 ±1%，非固定规则），不支持 Binance 式
  “远高于市场”的 post-only 挂单；探针启动时实时拉取该品种的 price-limit 与盘口最优卖价
  动态计算 resting 价格（`OKX_PROBE_PRICE_MARGIN_PCT` 可调），不做任何品种假设；OKX 下单/
  撤单响应无统一时间戳字段（撤单响应有 `ts`，下单响应没有），因此 REST *命令*事实的
  exchange_ts 以 reconcile 快照与撤单 `ts` 为准；partial-fill 无法以最小订单
  （0.01 张 = 1 XRP）确定性制造，与 Binance 同口径豁免；
- 最终独立原始接口复核：零挂单、零仓位、零条件单，余额无损。

验收过程中发现并修复的代码缺口（均由探针暴露）：

1. `bbo-tbt` 帧解析使用了不存在的扁平 `bidPx/bidSz` 字段，因 serde 全默认字段而
   静默丢帧且无任何错误（`connector/src/okx/msg/stream.rs`，改为解析
   `bids/asks` 顶档数组）；
2. `/api/v5/trade/cancel-all-orders` 端点不存在（HTTP 404，`code` 为整数导致
   `CancelResponse` 解码失败），改为 `orders-pending` 查询 + `cancel-batch-orders`
   逐单校验（`connector/src/okx/rest.rs`）；
3. `orders-pending` 的 `reduceOnly` 以字符串 `"false"` 返回导致整个订单记录解码
   失败、Full reconcile 无法完成（新增 `from_lenient_bool` 宽松布尔解析）；
4. 撤单响应 `ts` 未解析，REST 撤单事实 exchange_ts 恒为 0（已回填至
   `OrderInfo.update_time`）。

### Hyperliquid（2026-09-07 已完成）

范围（详见 [refactor_remaining_tasks.md](refactor_remaining_tasks.md) 4.10）：

- cancel-all 聚合路径已补齐交易所 orderId/状态时间戳回填逻辑（离线与 unit test 阶段）；
- WS 与 REST reconcile 的 exchange_ts 换算仍需主网逐字段实测；
- REST→私有流 client/venue id、状态与时间戳一致性需复刻 Binance/OKX 探针方式验证；
- 小额 submit/cancel/partial-fill/reconnect 与最终 orders/positions/balances 对账。

目标机主网已完成真实 socket 重连、私有订阅重放，以及 submit/amend/cancel 的 REST/WS
cloid、oid、status、price、qty、executed/leaves、status timestamp 逐字段断言；另完成 0.01 ETH
市价开仓与 reduce-only 平仓，最终零挂单、零仓位。验收中发现并修复 amend 乱序 oid、WS amend
价格未更新、动态浮点价格 wire 三个实盘问题。

最终通过自动扫描薄顶档并只跨第一档的 IOC，在 20 USDC 上限内取得 GAS 10.2/16.1 的真实
partial-fill；REST、私有 WS、fills 逐字段一致，reduce-only 平仓后零挂单零仓位。无需配套
对手账户或提高风险上限。证据见 `validation/hyperliquid_2026-09-07/README.md`。

解除条件已满足：小额凭据环境中的提交、修改、撤单、重连、完整成交、部分成交和最终对账均通过。

---

## B-03 生产 PerformanceEnvelope 冻结

已具备：

- EventEngine bench 容量扫描与目标机 synthetic 基线（默认档 500k/800k 定速、1M burst 零丢单/零 RESYNC，RSS ~152 MB）；
- 冻结档配置已记录在 [refactor_remaining_tasks.md](refactor_remaining_tasks.md) 4.11。

2026-09-07 已在目标机按当前部署容量契约冻结：默认容量、1M events、300k/s 连续三轮零
drop/resync/arena exhaustion，dispatch/subscriber P99.9 门槛分别为 8,388,607/16,777,215 ns；
Hyperliquid 主网 MarketPlugin→EventEngine 60 秒样本的 fast-lane enqueue/handler P99.9 分别为
8,191/4,095 ns，零 drop/resync。后续若生产品种数或目标事件率变化，需版本化重测而不是沿用本门槛。

解除条件：给出目标负载契约（品种数、订阅 kind、事件率、恢复频率、允许的 RESYNC/重放次数），在目标机测得
publisher admission / worker dispatch / handler commit 的 P50/P99/P99.9 并设置 CI 回归门槛。

---

## B-04 方案 B 后多 venue e2e 覆盖缺口

删除旧 `Connector::submit/cancel`、venue 级 roundtrip 与 LiveBot/Iceoryx 后，三家 venue 的真实主网
闭环均已由探针补上；Binance 偶发静默断流继续由 B-01 独立观测。

非外部部分（本地可做）：

- 在 brokerapi/account 集成层补 REST/WS 乱序合流回归（用本地 mock 而非实盘）；
- 在 EventEngine/AccountPlugin 层补 journal 终态释放的运行时级断言。

实盘部分已由 B-02 一并验收。

---

## 不列为阻塞项的内部问题（已修/已缓解）

- `PublishSender` direct-only 的 `Result` 返回值不再表达背压：方法文档已说明错误由 direct 回调负责，
  适配器闭环未受影响；后续可在独立 API 版本中改为非 Result 签名。
- connector 测试数量变化导致的文档口径：已更新为 201 项。
