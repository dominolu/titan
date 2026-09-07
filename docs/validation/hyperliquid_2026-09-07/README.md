# Hyperliquid 主网验收记录（43.165.184.116）

日期：2026-09-07（Asia/Shanghai）

## 已通过

- 公共 REST 与 WS：BTC instruments/ticker/book/funding/OI/trades/klines，以及
  `l2Book`/`trades` 实时流均通过。
- 私有 REST：真实完成 ETH 深价 GTC `submit -> get -> amend -> cancel`，终态零挂单。
- 私有 WS 与 REST 逐字段一致性：注入一次真实 socket 断开并二次 READY 后，完成
  `submit -> amend -> cancel`；cloid、oid、status、price、qty、executed_qty、leaves_qty、
  status timestamp 全部断言一致，终态零挂单。
- 小额真实成交：0.01 ETH 市价开仓并 reduce-only 平仓，WS position `0 -> 0.01 -> 0`，
  REST 终态零挂单、零仓位。
- 真实部分成交：自动扫描薄顶档后以 20 USDC 上限执行单档 IOC；GAS 买单 16.1，
  成交 10.2、余量 5.9，REST/私有 WS/fills 的 cloid、oid、status、price、qty、
  executed_qty、leaves_qty、status timestamp 全部一致；随后 reduce-only 平仓，终态零挂单、零仓位。
- 目标机 release 性能冻结：默认容量、1,000,000 events、300k events/s，连续三轮
  299,993–299,999 events/s，drop/resync/arena exhaustion 均为 0。冻结阈值：
  dispatch P99.9 <= 8,388,607 ns，subscriber P99.9 <= 16,777,215 ns。
- Hyperliquid 主网 MarketPlugin -> EventEngine 60 秒：12 DepthBatch、101 TradeBatch；
  fast-lane enqueue P99/P99.9 = 8,191/8,191 ns，handler P99/P99.9 =
  2,047/4,095 ns；drop/resync = 0。

## 部分成交样本（已解除）

- IOC 薄顶档方案两次在下单前因顶档恢复到约 400 USDC 而触发 20 USDC 风险上限，未发单。
- PROVE 抢价被动单（约 20 USDC）在 0.8 秒内一次性全部成交，并已自动反向平仓。
- PROVE 最优卖价排队单（约 20 USDC）等待 180 秒无成交，并已自动撤单。
- 最终改为自动扫描候选合约，并在发现 10–20 USDC 薄顶档时立即发出总额不超过
  20 USDC、限价只覆盖第一档的 IOC。GAS 样本成功形成 10.2/16.1 的真实部分成交。
- 每次结束后独立原始 REST 查询均为零挂单、零仓位。

因此无需配套对手账户或提高风险上限，partial-fill 门禁已通过。

## 主要证据

- `private_ws_field_consistency_after_fix.log`
- `private_ws_reconnect.log`
- `small_fill_roundtrip.log`
- `performance_300k_run1.log` / `run2.log` / `run3.log`
- `hyperliquid_live_pipeline_release_60s_after_fix.log`
- `partial_fill_passive_prove_after_wire_fix.log`
- `partial_fill_queue_prove.log`
- `partial_fill_ioc_final_retry1.log`
- `post_partial_ioc_final_open_orders.json` / `post_partial_ioc_final_account.json`
- 对应 `post_*_open_orders.json` 与 `post_*_account.json`

日志未保存私钥；包含完整签名请求的失败日志已从仓库证据目录删除。
