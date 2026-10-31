# Cross-exchange market making

[cross_exchange_mm.py](cross_exchange_mm.py) 是一个单文件 Numba / ABI V13 做市策略，替代原 `reference_mm`。
采用逐成交触发的 maker–taker 对冲：在交易所 A 双边 PostOnly 做市，在交易所 B 用有价格保护的 IOC 单对冲每笔已确认新增成交。

## 绑定与参数

沿用原有行情编号参数，交易所由 Titan 运行配置指定：

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `reference_asset_no` | 0 | 参考行情，同时是实际对冲品种 |
| `traded_asset_no` | 1 | 做市品种 |
| `account_no` | 0 | 做市账户，只交易 `traded_asset_no` |
| `hedge_account_no` | 1 | 对冲账户，只交易 `reference_asset_no` |
| `order_qty_lots` | 5 | 每笔做市订单数量，单位为 maker lot |
| `max_position_lots` | 10 | 做市账户净持仓上限，含未确认释放的挂单预占 |
| `hedge_lots_per_maker_lot` | 1 | 一个 maker lot 对应的 hedge lot 数，必须是精确正整数 |
| `max_hedge_position_lots` | 10 | 对冲账户独立净持仓上限 |
| `max_unhedged_lots` | 10 | 合计未对冲敞口上限，单位为 hedge lot |
| `reference_tick_ratio` | 1 | hedge tick / maker tick，以明确的共同报价单位换算 |
| `maker_fee_bps` / `hedge_fee_bps` | 0 / 4 | 报价成本模型中的两侧费率；maker 费率可配置返佣负值 |
| `min_profit_bps` | 2 | 报价使用的最低预估利润，不是成交后收益保证 |
| `hedge_slippage_bps` | 5 | IOC 价格保护；同方向连续欠量的重试不会突破首次参考价保护边界 |
| `hedge_timeout_ns` | 10 秒 | 一段连续未确认对冲敞口的总期限；停止和恢复均不重置 |
| `max_hedge_attempts` | 5 | 该段敞口内已确认终态后的最大提交次数 |
| `retry_backoff_ns` | 500 毫秒 | 已确认 IOC 终态后的重试等待 |
| `hedge_min_order_lots` / `hedge_min_notional_tick_lots` | 1 / 1 | 对冲场所最小量及最小金额约束 |

其他参数保留参考价模型：`spread_bps=10`、`skew_bps=5`、预热 60 秒、基差半衰期 300 秒、最小重报价间隔 500 毫秒。价格和数量采用原生 ticks/lots，不默认等币种、等合约或等 lot。最大做市仓位按比例换算后必须能被对冲账户上限覆盖。

本版面向同一经济资产的线性 NET 合约。固定 lot 比例不能直接套用于基础币 delta 随价格变化的反向合约；现货库存、借币及其他保证金模式需要独立适配和验收。两所余额/保证金准入仍须由平台证明，策略不会把另一所浮盈当作本所可用保证金。

例如 maker lot 为 0.01 SOL、hedge lot 为 0.001 SOL 且合约基础币 delta 一致时，比例为 10；同时需要相应调整对冲仓位和欠量预算。原生最小成交量必须能被绑定单位无损表达，不能为满足整数比例而随意放大 maker lot、丢掉真实部分成交。原生比例非整数的组合本版不支持，需要另行实现精确有理数和尾量风控；不向上取整制造过度对冲，也不把不可交易尾量记作完成。参考所与交易所支持范围取决于平台 connector。

## 执行与仓位

1. 读取两所新鲜 BBO，预热基差模型；根据参考价和 maker 库存偏移生成目标报价。
2. 再用对冲所可执行方向的买卖价、可见最优档数量、配置费用和滑点保护约束报价。预估无法覆盖成本的旧报价立即撤销，不等待重报价限频。
3. 每笔独立 maker 成交增量进入业务库存，并计算：

   `hedge_debt_lots = maker_position_lots × hedge_lots_per_maker_lot + hedge_position_lots`

4. 欠量为正则卖出对冲，为负则买入对冲。一个 IOC 在途期间合并后续需求，不重复提交。对冲依据已消费的原生成交，不等待迟到持仓，也不从持仓或累计订单量伪造成交。
5. 有欠量或对冲单时撤销 maker 挂单，保留其风险预占。IOC 部分成交只扣除实际增量；余量必须等待原 IOC 终态与成交证据收齐后，才退避重试。
6. 对手 maker 成交可能使欠量减小或反向；先取消已不适合的在途对冲单并等事实确认。取消与成交竞态产生的反向欠量按实际成交修正，不假设撤单已成功。
7. 未知结果、事实冲突、账户断线/换代次或确认超时保持失败关闭。记录保留原订单身份、欠量和期限，不重新发相同写请求来猜结果。

报价会检查两个账户各自的仓位/名义金额预算，不能用净 delta 接近零抵消单所风险。策略毛收益合并两腿现金流与盯市，按 tick/lot 比例换成 maker tick × maker lot；实际手续费、资金费和净权益保护仍由平台账本/风控负责。

## 停止与恢复

Stop 和毛亏损限制会撤 maker 单，继续处理已知对冲欠量。停止完成必须同时满足：无业务订单引用、欠量为零、两所实际持仓与已消费成交相符。最终可能是 A 多仓、B 空仓；这属于完成对冲，不是两个账户都平仓。超时、重试耗尽或结果未知不能报告完成。

本策略不声明 `PAUSE_SAFE`。当前宿主在 Pause 时只允许 reduce-only，而对冲通常需要在另一所新开反向仓位，不能将其错误标记为 reduce-only。管理入口应拒绝该策略的 Pause；使用已定义的 Stop 流程。

恢复回调不下单/撤单。宿主先提供原生事实与订单结果证明，策略只接受与逐订单已消费成交量一致的结果，保留原对冲期限/价格锚/尝试次数，再重新预热行情。缺少证明、未知订单或到期仍拒绝。

类型为 `cross_exchange_mm`，源码版本 `2.0.0`、恢复 schema 2；旧 `reference_mm` 类型、旧编译摘要和 schema 1 检查点不能直接迁入。

## 编译与验证

业务代码仅一个文件；默认参数在 `SPEC` 中。按仓库约定，在 `192.168.3.88` 编译：

```bash
printf '{}' | titan strategy compile \
  --strategy strategies/cross_exchange_mm.py \
  --parameters /dev/stdin --artifact-format bundle \
  --output cross_exchange_mm.titan
```

编译时可用输入 JSON 覆盖参数；连接、凭据、两所行情/账户绑定、周期 timer 和平台风险门禁仍由运行配置提供。

当前受控实盘 `execution_limits` 入口只支持单行情绑定，此策略需要双行情/双账户；本次不绕过或关闭该门禁。Bullet connector 也尚未提供。本次工作不包含真实账户连接、部署或实盘交易。
