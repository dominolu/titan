# 阻碍性 Bug / 阻塞项清单

状态：本文件只收录“当前无法仅在代码内闭环解决、需要外部环境或进一步现场验证”的问题。
可内部修复的问题不在本清单，直接按主清单执行。

更新时间：2026-09-05

## 汇总

| ID | 标题 | 严重度 | 当前状态 | 主要阻塞条件 |
|---|---|---|---|---|
| B-01 | Binance 私有流偶发连接后零 executionReport | 高（影响“静默断流”保证） | 观测中，未定位根因 | 需要持续主网运行 + 日志/抓包复现 |
| B-02 | OKX/Hyperliquid 实盘私有流与字段语义验收 | 高（阻断多交易所闭环） | OKX 已完成（2026-09-05）；Hyperliquid 未执行 | Hyperliquid 凭据与资金环境 |
| B-03 | 生产 P99/P99.9 PerformanceEnvelope 冻结 | 中（不阻塞功能） | 仅有 synthetic 基线 | 目标机上约定的真实 Market/Account 双源负载 |
| B-04 | 多 venue e2e/roundtrip 覆盖在方案 B 删除后未补齐 | 中 | OKX venue 级闭环已由探针补上；Hyperliquid 待补 | B-02 的实盘环境；本地可先补 brokerapi 合流测试 |

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

在目标机 `43.165.184.116` 用 OKX 主网真实完成验收，探针
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

### Hyperliquid（未执行）

范围（详见 [refactor_remaining_tasks.md](refactor_remaining_tasks.md) 4.10）：

- cancel-all 聚合路径尚未回填交易所 orderId；
- WS 与 REST reconcile 的 exchange_ts 换算需在主网逐字段实测；
- REST→私有流 client/venue id、状态与时间戳一致性需复刻 Binance/OKX 探针方式验证；
- 小额 submit/cancel/partial-fill/reconnect 与最终 orders/positions/balances 对账。

阻塞原因：Hyperliquid 的 API 凭据与小额资金环境尚未提供；凭据不得写入仓库。

解除条件：提供只读/小额交易凭据后，复用
`connector/examples/okx_account_rest_ws_probe.rs` 的结构建立 HL 对应探针，并跑通
`submit → 私有流事实 → cancel → 私有流终态 → Full reconcile`。

---

## B-03 生产 PerformanceEnvelope 冻结

已具备：

- EventEngine bench 容量扫描与目标机 synthetic 基线（默认档 500k/800k 定速、1M burst 零丢单/零 RESYNC，RSS ~152 MB）；
- 冻结档配置已记录在 [refactor_remaining_tasks.md](refactor_remaining_tasks.md) 4.11。

阻塞原因：真实生产负载（Market Batch 长度分布、多 Primary lane、账户事实混合、恢复窗口）尚未在目标部署形态下定义并测量。

解除条件：给出目标负载契约（品种数、订阅 kind、事件率、恢复频率、允许的 RESYNC/重放次数），在目标机测得
publisher admission / worker dispatch / handler commit 的 P50/P99/P99.9 并设置 CI 回归门槛。

---

## B-04 方案 B 后多 venue e2e 覆盖缺口

删除旧 `Connector::submit/cancel`、venue 级 roundtrip 与 LiveBot/Iceoryx 时，Binance 真实主网闭环已由
探针补上；OKX/Hyperliquid venue 级 e2e 与“双通道 REST/WS 乱序合并”回归覆盖暂时降低。

非外部部分（本地可做）：

- 在 brokerapi/account 集成层补 REST/WS 乱序合流回归（用本地 mock 而非实盘）；
- 在 EventEngine/AccountPlugin 层补 journal 终态释放的运行时级断言。

实盘部分由 B-02 一并验收。

---

## 不列为阻塞项的内部问题（已修/已缓解）

- `PublishSender` direct-only 的 `Result` 返回值不再表达背压：方法文档已说明错误由 direct 回调负责，
  适配器闭环未受影响；后续可在独立 API 版本中改为非 Result 签名。
- connector 测试数量变化导致的文档口径：已更新为 201 项。
