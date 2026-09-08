# OKX–Hyperliquid Numba XEMM

这是一个完全位于单个策略包内的 Strategy ABI v10 实现：OKX 为 maker，Hyperliquid 为 IOC hedge。

实现包含：

- OKX BBO 采样、Post Only 双边报价和 cancel-confirm-replace；
- Hyperliquid 完整 L2 batch 的顺序无关 VWAP/最差成交价计算；
- maker/taker fee、USDC/USDT 固定换算率、合约乘数与定向量化；
- maker partial fill 到反向 IOC hedge、hedge remainder 重试与 fill 去重；
- 固定内存状态、最大订单/持仓/未对冲限制和行情陈旧撤单。

## Runtime 要求

- `runtime_abi = 10`；
- OKX market binding 订阅 `Bbo`，Hyperliquid market binding 订阅 `Depth`；
- 两个账户至少声明一类 account subscription，StrategyPlugin 会自动补齐 Order、Fill、Position、
  Balance、CommandResult、Reconcile 和 StreamState 的可靠有序路由；
- `runtime.timer_interval` 建议配置为 `{ secs = 0, nanos = 100000000 }`（100ms），用于安静市场中的
  stale 检查和 hedge retry；
- `shutdown = "cancel_owned_orders"`，停机操作会等待 owned orders 进入终态；
- Hyperliquid Depth 必须提供完整 snapshot batch（`kind=1`、snapshot flag）；增量 batch 会触发撤单并等待
  下一次完整快照。

`unsafe_assume_flat_start` 仅为旧配置兼容保留，ABI v10 策略不再读取该参数。策略在两个账户的仓位快照、
账户 READY、OKX BBO 和 Hyperliquid Depth 任一缺失时都不会报价。

## 本地编译示例

```bash
cargo run -p titan-cli -- strategy compile okx_hyperliquid_xemm --parameters '{
  "order_amount_base": 0.001,
  "maker_fee_bps": -1.0,
  "taker_fee_bps": 3.5,
  "maker_price_tick": 0.1,
  "maker_quantity_lot": 1.0,
  "maker_contract_base_multiplier": 0.001,
  "hedge_price_tick": 0.1,
  "hedge_quantity_lot": 0.00001,
  "max_order_notional": 100.0,
  "max_abs_position_base": 0.01,
  "max_unhedged_base": 0.003,
  "max_unhedged_notional": 300.0,
  "conversion_expiry_ns": 1893456000000000000
}'
```

示例 tick/lot/fee 仅用于展示参数格式，运行前必须使用交易所 instrument metadata 与账户费率校验。
