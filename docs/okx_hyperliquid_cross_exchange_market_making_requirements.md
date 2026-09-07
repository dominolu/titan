# OKX–Hyperliquid 跨所做市策略需求文档

状态：Draft v0.1

目标版本：Titan XEMM v1

默认部署：OKX 永续合约做 maker，Hyperliquid 永续合约做 taker/hedge

参考实现：Hummingbot `cross_exchange_market_making`

## 1. 背景

跨所做市（Cross-Exchange Market Making，XEMM）在流动性较好的对冲场所读取真实可成交价格，
在另一场所提供被动流动性。Maker 订单成交后，策略立即在 taker 场所反向成交，将方向敞口恢复到
目标范围，并锁定两边价差。

Hummingbot 参考实现提供了以下核心行为：

- maker 场所每个交易对最多维持一笔买单和一笔卖单；
- maker 报价以 taker 场所按目标数量计算的 VWAP 为锚；
- 报价需要满足最低盈利率，并可贴近 maker 场所盘口；
- 存量 maker 订单因盈利不足、余额不足或报价漂移而撤换；
- maker 成交按成交明细去重，并在 taker 场所发出反向限价单；
- 未完成、失败、撤销或过期的 taker 对冲单继续重试；
- 使用盘口深度容忍、价格采样窗口和 anti-hysteresis 降低瞬时盘口与频繁撤挂影响。

本文将上述行为收敛为适合 OKX 与 Hyperliquid 永续合约、且可在 Titan 中验收的产品需求。

## 2. 范围与假设

### 2.1 v1 范围

- 单策略实例管理一个标的，例如 OKX `BTC-USDT-SWAP` 与 Hyperliquid `BTC` perpetual。
- OKX 固定为 maker，maker 单使用 Post Only；Hyperliquid 固定为 taker，对冲单使用可成交限价 IOC。
- 账户均使用单向净持仓模式。策略不支持 OKX long/short 双向持仓模式。
- 两边合约必须具有相同的基础资产经济敞口；合约乘数和数量换算由配置显式给出。
- 支持两边不同报价/保证金币种，例如 OKX 的 USDT 与 Hyperliquid 的 USDC。
- 支持部分成交、累计成交、撤单后迟到成交、对冲部分成交和对冲重试。
- 支持进程重启后的订单、成交、仓位与未对冲敞口对账。

### 2.2 非目标

- v1 不支持多标的共享风险预算、三角换汇、现货–永续组合、DEX/Gateway gas 费用或跨链结算。
- v1 不依据资金费率主动切换 maker/taker 方向，也不做方向性择时。
- v1 不保证理论无风险。盘口变化、延迟、拒单、限频、强平、交易所故障和结算币种脱锚仍可能造成亏损。
- v1 不自动转账或自动补充保证金。

### 2.3 默认业务假设

- OKX 为 maker、Hyperliquid 为 hedge 是本版本的固定角色，而不是仅凭配置字符串猜测。
- USDT/USDC 换算率由外部可靠价格源提供；若未接入价格源，可配置固定值，但必须设置有效期。
- 盈利判断使用实际可成交深度、两边费用、数量换算和报价币种换算，不能只比较 BBO。

## 3. 名词与方向

| 名词 | 定义 |
|---|---|
| Maker bid | 在 OKX 买入；成交后应在 Hyperliquid 卖出对冲 |
| Maker ask | 在 OKX 卖出；成交后应在 Hyperliquid 买入对冲 |
| Hedge sell VWAP | 在 Hyperliquid 卖出指定数量可得到的平均价格 |
| Hedge buy VWAP | 在 Hyperliquid 买入指定数量需支付的平均价格 |
| 未对冲敞口 | 已在 maker 成交、尚未被 taker 成交覆盖的基础资产 Delta |
| Quote generation | 一组同时计算的 bid/ask 目标价格与数量版本 |
| Freshness | 行情、账户或汇率数据距离当前时间的最大允许年龄 |

## 4. 业务目标与成功标准

### 4.1 目标

1. 只在预估净收益满足阈值时提供 maker 流动性。
2. Maker 成交后优先降低方向敞口，而不是继续追求挂单收益。
3. 对重复、乱序和迟到的订单事件保持幂等。
4. 任一关键数据源失效时撤销 maker 单并停止新增风险。
5. 重启后能够通过交易所事实恢复，而不是假设本地状态完整。

### 4.2 核心 SLO

| 指标 | v1 目标 |
|---|---|
| maker fill 到首次 hedge command 提交 | p99 ≤ 50 ms（不含交易所网络延迟） |
| 关键行情过期后的 maker 撤单命令 | ≤ 1 个策略调度周期，且不超过 500 ms |
| 重复 fill 导致的重复对冲 | 0 |
| 未经绑定账户/标的发单 | 0 |
| 正常停止后遗留的策略 maker 单 | 0 |
| 未对冲 Delta 超限后继续新挂 maker 单 | 0 |

## 5. 功能需求

### FR-01 启动前校验

策略进入 `QUOTING` 前必须确认：

- OKX 与 Hyperliquid 公共行情、私有账户流及交易接口均可用；
- 两边 instrument metadata、价格 tick、数量 lot、合约乘数已加载；
- 两边账户已完成 orders/positions/balances 全量对账；
- 报价币种换算率存在且未过期；
- 当前净持仓和未对冲敞口未超过启动阈值；
- 不存在无法归属的同 client-id 前缀活动订单；
- 安全撤单心跳已启用，除非运行配置明确允许关闭。

任一条件不满足时，策略保持 `WARMING_UP` 或进入 `PAUSED`，不得提交 maker 单。

### FR-02 行情与可成交价格

1. 策略必须维护两边有序盘口，并验证 snapshot、sequence/epoch 与增量连续性。
2. 报价计算必须使用 Hyperliquid 在目标对冲数量上的 VWAP；深度不足时该方向不得报价。
3. Maker 顶部价格可忽略累计数量不超过 `top_depth_tolerance` 的薄量档位。
4. OKX 顶部 bid/ask 每 `price_sample_interval_ms` 采样一次，保留最近
   `price_sample_window` 个样本：
   - 竞争性 bid 锚点取当前值和窗口内有效值的最大值；
   - 竞争性 ask 锚点取当前值和窗口内有效值的最小值。
5. 任一所盘口交叉、为空、序列失效或超过 `market_stale_ms` 时停止该方向报价；关键行情整体失效时撤销双边 maker 单。

### FR-03 净盈利报价

设：

- `Qm` 为 OKX 合约数量对应的基础资产数量；
- `Qh` 为 Hyperliquid 对冲数量；
- `Rh` 为 `Qm -> Qh` 的基础资产换算；
- `Rq` 为 Hyperliquid quote -> OKX quote 的换算率；
- `fm`、`ft` 分别为 maker 与 taker 费率，返佣可为负；
- `pmin` 为最低净盈利率；
- `Ph_sell(Qh)`、`Ph_buy(Qh)` 为 Hyperliquid 卖出/买入 VWAP。

数量满足 `Qh = Qm × Rh`。忽略资金费与固定成本时，maker bid 的最高允许价格为：

```text
bid_cap = Ph_sell(Qh) × Rh × Rq × (1 - ft)
          / ((1 + fm) × (1 + pmin))
```

maker ask 的最低允许价格为：

```text
ask_floor = Ph_buy(Qh) × Rh × Rq × (1 + ft)
            × (1 + pmin) / (1 - fm)
```

实现时还必须从预期收益中扣除配置的固定成本与安全缓冲。最终报价规则：

- bid 不高于 `bid_cap`，可在不破坏盈利约束的前提下设为 OKX 有效顶部 bid 上一 tick；
- ask 不低于 `ask_floor`，可在不破坏盈利约束的前提下设为 OKX 有效顶部 ask 下一 tick；
- bid 向下取整到 OKX price tick，ask 向上取整到 OKX price tick；
- Post Only 价格不得穿越 OKX 对手价；否则退一 tick 或取消该方向报价；
- 量化后重新计算净盈利，未达阈值则不得下单。

### FR-04 报价数量

每一方向的 maker 数量为以下约束的最小值，并向下量化到 OKX lot：

- `order_amount` 指定值；若为 0，则使用账户权益乘 `portfolio_ratio_limit` 推导；
- OKX 可用保证金和风险限额允许的最大下单数量；
- Hyperliquid 可用保证金和风险限额允许的最大反向对冲数量；
- Hyperliquid 在最大允许价格冲击内的可成交深度乘 `taker_volume_factor`；
- `max_order_notional`；
- 当前 inventory/headroom 所允许的数量。

量化后低于任一交易所最小数量或最小名义金额时，该方向不报价。

### FR-05 Maker 订单维护

- 每个实例最多存在一笔活动 maker bid 和一笔活动 maker ask。
- Maker 单必须使用唯一、可重建、带策略前缀的 client order id 和 Post Only TIF。
- 以下任一条件成立时撤销对应 maker 单：
  - 当前净盈利低于 `cancel_profitability`；
  - 新目标价格与活动价格相差至少 `requote_threshold_ticks`；
  - 可用保证金不足以支持 maker 成交及随后对冲；
  - 行情、账户、汇率或连接状态过期；
  - 未对冲敞口或总持仓达到限制；
  - 手工暂停、关闭或风险熔断。
- 主动刷新模式采用 cancel-confirm-replace，不允许同方向旧单未终态时直接补挂新单。
- 两次非紧急改价至少间隔 `anti_hysteresis_ms`；风险撤单不受此限制。
- 被撤单在终态到达前仍视为可能成交，订单映射至少保留 `late_fill_retention_ms`。

### FR-06 Maker 成交与对冲

1. 只使用增量 fill 数量计算新敞口；不得把 cumulative fill 重复计入。
2. Fill 幂等键至少包含 venue、account、instrument、venue order id 与 trade/fill id。
3. Maker buy fill 增加正 Delta，并触发 Hyperliquid sell hedge；maker sell fill 增加负 Delta，并触发 Hyperliquid buy hedge。
4. 多笔小 fill 可在 `hedge_batch_window_ms` 内聚合，但不得使 `max_unhedged_age_ms` 超限。
5. 对冲价格使用当前深度计算，并加入 `hedge_slippage_bps`：
   - sell hedge 的 IOC limit 不高于期望卖出底价；
   - buy hedge 的 IOC limit 不低于期望买入上限。
6. 对冲优先使用 IOC 限价单；IOC 未完全成交时只对剩余数量重试。
7. 未对冲量低于 Hyperliquid 最小 lot 时累计保留，达到 lot 后再提交；其风险仍计入未对冲敞口。
8. Hedge command 未确认、拒绝、过期或连接结果未知时，必须先通过订单/成交对账确定事实，再决定是否重试，禁止盲目重复下单。
9. 当未对冲量达到 `max_unhedged_base` 或名义金额达到 `max_unhedged_notional` 时，立即撤销双边 maker 单并进入 `HEDGE_ONLY`。

### FR-07 仓位与库存控制

- 策略以两所经合约乘数归一化后的净基础资产 Delta 作为风险事实。
- `inventory_target_base` 默认为 0。
- 报价数量必须受 `max_abs_position_base` 限制；已接近上限的一侧应缩量或停报。
- v1 可支持线性 inventory skew：正 Delta 时降低/关闭 maker bid 并提高卖出倾向，负 Delta 时反向处理。
- `reduce_only` 只能在确认该对冲方向会减少 Hyperliquid 现有仓位时使用；否则普通 IOC 对冲，以避免因 reduce-only 拒单留下风险。

### FR-08 汇率

- USDC/USDT 换算率必须带 source、observed timestamp 和有效期。
- `fixed` 模式要求显式配置 `quote_conversion_rate` 与 `conversion_stale_ms`；固定值同样会过期，除非显式设置长期有效。
- 汇率缺失、非正数或过期时撤销 maker 单并暂停新增报价。
- 基础资产换算、合约乘数和报价币种换算必须分别建模，不得用一个 conversion rate 混用。

### FR-09 状态机

策略至少包含以下状态：

```text
STOPPED -> WARMING_UP -> RECONCILING -> QUOTING
                                  QUOTING -> HEDGE_ONLY
                                  QUOTING -> PAUSED
                               HEDGE_ONLY -> QUOTING
                         any active state -> STOPPING -> STOPPED
                         any active state -> FAULTED
```

- `QUOTING`：允许维护 maker 双边报价和执行对冲。
- `HEDGE_ONLY`：禁止新增 maker 风险，仅撤单、对账和降低敞口。
- `PAUSED`：禁止新增订单；根据暂停原因决定是否继续紧急对冲。
- `FAULTED`：命令闸门关闭，必须告警并由人工或受控恢复流程处理。

### FR-10 恢复与停止

- 启动/重启先关闭报价闸门，再拉取两边 open orders、recent fills、positions 和 balances。
- 策略只管理 client-id 命名空间内的订单，外部订单只计入账户风险，不得擅自撤销。
- 本地 checkpoint 仅作为加速信息；交易所事实优先。
- 正常停止顺序为：关闭新报价 → 撤销 maker 单 → 等待撤单终态 → 完成或明确移交未对冲风险 → 保存 checkpoint → 停止连接器。
- 到达停止 deadline 仍有未对冲敞口时必须返回非成功终态并发出高优先级告警。

### FR-11 可观测性与审计

必须输出以下指标：

- 两所 BBO/VWAP、数据年龄、quote generation；
- 目标/实际 maker 价格与数量、预估毛利/费用/净利；
- maker fill、hedge submitted、hedge filled 的数量与延迟；
- 当前净 Delta、未对冲量、未对冲年龄；
- 撤单原因、拒单原因、重试次数、熔断状态；
- 订单映射与 command/fill 幂等键的审计记录。

日志不得包含 API key、私钥、签名原文或完整凭据引用内容。

## 6. 配置契约

| 参数 | 类型 | 建议默认值 | 约束/含义 |
|---|---:|---:|---|
| `maker_asset_no` | int | 0 | OKX 本地资产编号 |
| `hedge_asset_no` | int | 1 | Hyperliquid 本地资产编号 |
| `maker_account_no` | int | 0 | OKX 本地账户编号 |
| `hedge_account_no` | int | 1 | Hyperliquid 本地账户编号 |
| `order_amount_base` | number | 必填 | 每侧基础资产目标数量，> 0；0 仅在启用组合比例模式时允许 |
| `min_profitability_bps` | number | 10 | 新挂单最低净盈利 |
| `cancel_profitability_bps` | number | 0 | 存量单撤销阈值，不得高于新挂单阈值 |
| `maker_fee_bps` | number | 必填 | 可为负数表示返佣 |
| `taker_fee_bps` | number | 必填 | Hyperliquid taker 费率 |
| `hedge_slippage_bps` | number | 20 | IOC 对冲保护价缓冲 |
| `taker_volume_factor` | number | 0.25 | 可成交深度使用比例，(0, 1] |
| `taker_balance_factor` | number | 0.995 | 可用保证金使用比例，(0, 1] |
| `portfolio_ratio_limit` | number | 0.1667 | 组合权益下单比例，(0, 1] |
| `top_depth_tolerance_base` | number | 0 | maker 顶部薄量忽略阈值 |
| `requote_threshold_ticks` | int | 1 | 改价最小差异 |
| `anti_hysteresis_ms` | int | 60,000 | 普通改价冷却时间 |
| `price_sample_interval_ms` | int | 5,000 | 顶部价格采样间隔 |
| `price_sample_window` | int | 12 | 采样数量 |
| `market_stale_ms` | int | 2,000 | 行情最大年龄 |
| `account_stale_ms` | int | 5,000 | 账户事实最大年龄 |
| `quote_conversion_rate` | number | 1.0 | Hyperliquid quote 到 OKX quote |
| `conversion_stale_ms` | int | 必填 | 汇率最大年龄 |
| `okx_contract_base_multiplier` | number | 必填 | 每张 OKX 合约对应基础资产数量 |
| `hyperliquid_contract_base_multiplier` | number | 1.0 | HL 数量到基础资产的乘数 |
| `max_order_notional` | number | 必填 | 单个 maker 单最大名义金额 |
| `max_abs_position_base` | number | 必填 | 两所合计绝对 Delta 限制 |
| `max_unhedged_base` | number | 必填 | 未对冲基础资产硬限制 |
| `max_unhedged_notional` | number | 必填 | 未对冲名义金额硬限制 |
| `max_unhedged_age_ms` | int | 1,000 | 未对冲最长时间 |
| `hedge_batch_window_ms` | int | 0 | 0 表示逐 fill 立即对冲 |
| `hedge_retry_limit` | int | 5 | 单次风险事件自动重试上限 |
| `hedge_retry_backoff_ms` | int | 100 | 指数退避初值 |
| `late_fill_retention_ms` | int | 900,000 | 终态订单关联信息保留时间 |

生产配置必须显式给出 fee、contract multiplier、风险限额和数据 freshness，不能依赖代码默认值。

## 7. 验收场景

| 编号 | 场景 | 预期结果 |
|---|---|---|
| AC-01 | 双边行情、账户和汇率就绪 | OKX 恰好一 bid、一 ask，均为 Post Only 且量化正确 |
| AC-02 | Hyperliquid 深度不足 | 受影响方向不挂单，另一方向可独立工作 |
| AC-03 | 净盈利跌破撤单线 | 对应 maker 单被撤销，不等待 anti-hysteresis |
| AC-04 | 瞬时薄量插单后快速消失 | 采样窗口与 depth tolerance 阻止无意义来回改价 |
| AC-05 | Maker bid 部分成交三次 | 每个 fill 仅计一次；提交等量 Hyperliquid sell hedge |
| AC-06 | Maker ask 在撤单确认前成交 | 迟到 fill 被识别并提交 buy hedge |
| AC-07 | Hedge IOC 部分成交 | 只对剩余量重试，不重复已成交部分 |
| AC-08 | Hedge command 响应未知 | 先查询/对账，再决定重试 |
| AC-09 | 重复、乱序 fill | Delta 和 hedge 总量保持正确且幂等 |
| AC-10 | 任一行情超过 freshness | 双边 maker 单在 SLO 内撤销并进入 `PAUSED`/`HEDGE_ONLY` |
| AC-11 | 未对冲敞口超限 | 停止报价、撤销 maker 单、仅执行减险动作并告警 |
| AC-12 | USDC/USDT 汇率过期 | 停止新增报价并撤销活动 maker 单 |
| AC-13 | 进程在 maker fill 后、hedge 前崩溃 | 重启对账恢复未对冲量并补做一次对冲 |
| AC-14 | 正常停止 | maker 单全部终态；无静默遗留订单或未报告敞口 |
| AC-15 | OKX/HL 价格 tick 与 lot 不同 | 经济价格/数量换算正确，各自按本所单位发单 |

## 8. 与 Hummingbot 参考实现的差异

Titan v1 保留参考实现的报价锚、VWAP、双边单、价格采样、迟到成交跟踪和成交后对冲思想，
但有意强化以下内容：

- 盈利公式显式包含两边费用、合约乘数和 quote conversion；
- 永续合约按仓位/保证金建模，不套用现货 wallet balance 语义；
- 对冲采用可审计的 IOC 剩余量状态机，而不是失败后直接盲目重发；
- 明确行情/账户/汇率 freshness 与未对冲硬限额；
- 重启恢复以交易所对账为事实来源；
- fill 去重使用值相等的稳定幂等键，不沿用对象身份比较；
- 不沿用参考代码中订单停止跟踪的递归调用和对冲 task 跟踪不一致问题。

## 9. 需求追溯来源

- Hummingbot 报价与主循环：
  `/Users/dominolu/dev/hummingbot_latest/hummingbot/strategy/cross_exchange_market_making/cross_exchange_market_making.py`
- Hummingbot 参数模型：
  `/Users/dominolu/dev/hummingbot_latest/hummingbot/strategy/cross_exchange_market_making/cross_exchange_market_making_config_map_pydantic.py`
- Hummingbot 订单与市场对映射：
  `/Users/dominolu/dev/hummingbot_latest/hummingbot/strategy/cross_exchange_market_making/order_id_market_pair_tracker.pyx`
- Hummingbot 行为测试：
  `/Users/dominolu/dev/hummingbot_latest/test/hummingbot/strategy/cross_exchange_market_making/test_cross_exchange_market_making.py`
