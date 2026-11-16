# cross_exchange_mm：交易所配对、原生元数据与行情/回测数据方案

日期：2026-11-11。对应工作清单 D1-D3（[工作清单](cross_exchange_mm_live_worklist.md)）。
本文只整理**已核对的事实**与**待验证清单**；标"待验证"的结论不能作为准入证据。

## 1. 交易所能力矩阵（D1）

来源：`connector/` 工厂注册、`docs/exchange_onboarding.md`、`docs/exchange_api_catalog.md`、
`docs/verification/2026-10-31/native-venues*/README.md`、`connector/README.md`。

| 交易所 | 合约类型 | PostOnly | 带保护价 IOC | 独立成交事件 | 持仓/保证金模式 | 实盘私有流验收 |
| --- | --- | --- | --- | --- | --- | --- |
| Binance USD-M | 线性永续 | GTX | IOC | 是（listenKey 用户流 OrderTradeUpdate，主网已验） | 只读查询；统一层不下发切换 | 已验收 |
| OKX V5 SWAP | 线性永续 | `post_only` | `ioc` | 是（orders 频道 fillPx/tradeId） | 读取 posMode，但 net 或 long_short 均可通过（不拒绝 HEDGE） | 登录签名与公开流通过；私有 orders/positions 端到端仍有缺口 |
| Hyperliquid | 线性永续 | ALO | IOC | 是（userFills/orderUpdates，含部分成交） | 无模式校验；updateLeverage 内固定 is_cross | 已验收 |
| Backpack | USDC 线性永续 | postOnly+GTC | IOC | 是（account.orderUpdate 逐笔） | 统一层仅 Simple/cross USDC | 实现完成，真实私有验收待做 |
| Lighter Core | 线性永续 | GTX | IOC（拒绝无价市价单） | 是（account_all_trades） | 需 Simple + cross USDC，拒绝自动切换 | 实现完成，真实私有验收待做 |
| Gate CrossEx FUTURE | 线性永续（跨所） | POC | IOC | 弱：由累计成交量差推导，无原生 trade id | CROSS_EXCHANGE 或按 exchange_type 的 ISOLATED | 仅 BTC post-only 生命周期验证 |
| Variational | — | — | — | — | — | 不可交易（官方 Trading API 未开放） |
| EVM / Solana | AMM | 不支持挂单 | 仅 IOC/FOK | 交换成交 | 不适用 | 不适配本策略 |

共同限制：

- 最小数量/最小金额元数据只被部分适配器使用（Backpack、Lighter、CrossEx 有本地校验，
  Binance/OKX/HL 依赖交易所拒单），**绑定层没有统一的最小金额字段**。
- 账户就绪硬依赖私有流：bootstrap 与 activate 都要求私有流事实，缺私有流不能进入 READY。
- 抵押币分组决定能否无 FX 配对：USDT = Binance USD-M / OKX SWAP / Gate CrossEx；
  USDC = Hyperliquid / Backpack / Lighter。

## 2. 候选 A/B 配对（D2）

策略约束（`strategies/cross_exchange_mm.md`）：同一经济资产的线性 NET 合约；每 1 个原生 maker lot
的基础币 delta 必须恰好等于整数个 hedge lot；每个最小 maker 部分成交都能对冲；报价/抵押币相同，
不假设 USDT=USDC，也没有 FX 源。

设 maker 每 lot 基础币 `b_A`、hedge 每 lot 基础币 `b_B`，准入条件：

1. `b_A / b_B` 为正整数（否则最小 maker 成交无法整数对冲）；
2. `b_B / b_A` 也需可表示，保证原腿回退/tail 处置可下单；
3. hedge 所最小金额 <= 每个可能成交的最小 maker 名义；
4. 共同报价/抵押币、共同价格刻度换算关系明确。

候选（**全部待验证，本机没有任何原生元数据证据**）：

| 候选 | 理由 | 主要风险 |
| --- | --- | --- |
| OKX SWAP（maker）+ Binance USD-M（hedge） | 同为 USDT 线性永续；BTC/ETH/SOL 同资产 | OKX 数量单位是合约（ctVal），需整数比例与最小成交核对；BTC 0.01 BTC/张 vs Binance 0.001 BTC/lot 可能只在单一方向成整数 |
| Binance USD-M + Gate CrossEx FUTURE | 同 USDT；CrossEx 明确 contract_multiplier 与 min_notional | CrossEx 成交由累计量推导，非独立逐笔；两腿共用一个 broker/账户可能违背双账户前提 |
| Hyperliquid + Backpack / Lighter | 同 USDC；数量步长细，比例可能为整数 | 两所真实私有验收未完成；Lighter 需要 signer 与账户模式适配 |
| 任一 USDT 所 + 任一 USDC 所 | — | **直接拒绝**：无 FX 源，跨币抵押不可证明 |

**D2 核对清单（每对交易所必须逐项产出证据）**：

- `get_instruments` 原生返回的 `price_tick`、`quantity_lot`、`contract_multiplier`、最小数量、
  最小金额、流动性档位，固定时间点抓取并存档摘要。
- 用精确有理数（不用浮点）证明 `b_A/b_B ∈ ℤ⁺`，并写出 `hedge_lots_per_maker_lot`。
- 证明最小 maker 部分成交满足 hedge 所最小量/最小金额，且原腿 reduce-only 回退可执行。
- 两所报价/抵押币一致；价格刻度比 `reference_tick_ratio` 的精确来源与舍入规则。
- 把结论写入冻结的绑定清单，作为编译参数与运行配置的来源；不把交易所返回的撮合/深度结论
  外推到其他合约。

## 3. 行情录制与现有数据（D3）

录制能力（`collector/`，`book` 子命令 record/verify/repair/quality/replay）：

- 100 ms UTC 网格、Top-N 净额差分块 + `.idx`、小时分段、NDJSON.gz 成交流、OI/funding/清算辅助流；
  zstd + CRC32C；显式 gap 块；每日 `quality.json` 三态（PASS / PASS_WITH_WARNINGS / FAIL），
  replay 强制质量门禁。
- 支持 Binance spot/USD-M/COIN-M、Bybit、OKX、Hyperliquid；不支持 CrossEx/Backpack/Lighter。
- 格式限制：100 ms 净额差分**不能**还原队列位置与逐消息盘口顺序；`MarketConfig` 没有
  contract multiplier / base delta 字段，合约张数换算必须在回测层完成。

现有数据：

- 仓库内 `data/` 只有 AAPL 1 分钟 K 线，与本题无关；`artifacts/`、`backtest_reports/` 无跨所数据。
- 私有实盘机上有两次录制，均为 **同一交易所**（Binance spot BTCUSDT + USD-M BTCUSDT）：
  8h06m 与 8h15m，完整性 clean，但质量均为 FAIL/PASS_WITH_WARNINGS（spot REST shadow 不匹配）。
  **没有 PASS 日、没有 24 小时、没有第二个交易所。**
- 结论：当前没有任何可用于 R09 的跨所数据集。

录制计划（进入 D4 前必须完成）：

1. 先修复 spot REST shadow 不匹配（记录差异档位），否则每天都会 FAIL，无法进入回放门禁。
2. 固定 D2 选定的 A/B 两腿，同时录制：maker 所与 hedge 所、同经济资产、`grid_ms=100`、
   `depth_levels=100`、`block_duration_ms=60000`，另含 trades 与 funding/OI。
3. 时长：先满足项目自身 7 天门禁，再做 30 天样本外；每天产出 quality 结论，
   FAIL 日不得进入收益回测。
4. 数据清单需额外携带 `base_delta_per_lot`、`contract_multiplier`、`min_notional`、费率档、
   等价关系版本——录制格式当前都没有，需要在数据集清单层补齐。
5. 容量参考：60 s 聚合下约 0.2 GB/市场·日；两所两三资产约 1-1.5 GB/日。
