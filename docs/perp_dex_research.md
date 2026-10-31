# 永续 DEX 与跨所策略源码调研

日期：2026-10-27（Asia/Shanghai）  
状态：固定源码审查；未运行第三方程序，未连接真实账户，未验证各所当前实盘可用性。  
落地设计：[跨所交易技术设计](cross_venue_trading_design.md)；任务与验收：[实施计划](cross_venue_implementation_plan.md)。

## 1. 审查方法与证据范围

按“入口注册 → 适配器 → 策略调用 → 订单确认 → 故障路径”核对，而非只统计 README 中的名称。下面“已接入”只表示对应提交有实现及调用路径，不表示已通过 Titan 集成、真实成交、长期稳定性或完整恢复验收。

| 项目 | 固定提交 | 提交时间（原始时区） | 主要审查对象 |
| --- | --- | --- | --- |
| [perp-dex-tools][P] | `4679a339b8cdc9998707feeda3c5d8b84fb8681f` | 2026-03-18 20:19:47 +01:00 | factory、11 个适配器、对冲入口、GRVT v2、单所 bot |
| [grid-trading-bot][G] | `9e198c460f23f5df939bd48d0ecbea320f237c38` | 2026-04-07 22:46:36 +08:00 | 动态装载、两个适配器、网格及停止逻辑 |
| [perp-farming-bots][F] | `503fb0486b05f37528c0032181fda2148f4d7640` | 2026-10-16 09:25:14 +08:00 | provider 白名单、原生交易、持久化、跨账户配对、网格、费用和资金费 |
| [cross-exchange-arbitrage][C] | `3dd42c06c20458e77eb87c17b4c014df83260c23` | 2025-11-28 10:54:33 +01:00 | EdgeX–Lighter 策略、盘口、执行与持仓跟踪 |

Titan 审查基线为 `5997364864861fdd5d8c649b93e54161018f703d` 加阅读时的工作区；工作区已有恢复、SQLite 存储和观测等未提交修改。本调研不将它们宣称为已验收版本。外部仓库下载到临时研究目录，不作为 Titan 依赖或源码引入。

证据等级分开登记：E0 文档宣称；E1 固定源码实现；E2 可重复模拟验证；E3 官方公网只读验证；E4 指定账户受控交易验证；E5 限定配置的持续运行验证。本次独立建立 E1 审查证据；上游自己的测试和验收描述只作为上游陈述，不提升为 Titan 的 E2–E5。

## 2. 交易所与部署覆盖矩阵

P/G/F/C 对应上述四个项目。`✓` 表示本次确认有适配实现；`—` 表示该项目没有对应原生账户交易实现。合计 15 个 provider/部署身份，其中 Lighter Core 与 Robinhood 必须分开；ExtendedX 是 G 的类命名，不另算一家交易所。

| 交易所/部署身份 | P | G | F | C | 审查结论与源证据 |
| --- | --- | --- | --- | --- | --- |
| EdgeX | ✓ | — | — | ✓ | P 的 factory 注册；C 唯一主腿；[P 适配器][P-edgex]、[C 入口][C-entry] |
| Backpack | ✓ | — | — | — | 单所交易及 Lighter 对冲入口；[适配器][P-backpack]、[对冲入口][P-hedge] |
| Paradex | ✓ | — | — | — | 有单所适配，不在 P 对冲入口白名单；[适配器][P-paradex] |
| Aster | ✓ | — | — | — | 有单所适配，不在 P 对冲入口白名单；[适配器][P-aster] |
| Lighter Core | ✓ | ✓ | ✓ | ✓ | 四者共同交集；SDK/原生签名、market/account/key index；[P][P-lighter]、[G][G-lighter]、[F][F-trading]、[C][C-orders] |
| GRVT | ✓ | — | — | — | 常规对冲及 v2 并发价差版本；[适配器][P-grvt]、[v2][P-grvt-v2] |
| Extended | ✓ | ✓ | — | — | P 使用 Starknet SDK；G 的 `ExtendedXAdapter` 对应同所；[P][P-extended]、[G][G-extended] |
| ApeX Omni | ✓ | — | — | — | `apex` 注册及对冲入口；[适配器][P-apex] |
| Nado | ✓ | — | — | — | SDK 下单与查询；连接/回报部分采用轮询思路；[适配器][P-nado] |
| Ethereal | ✓ | — | — | — | SDK REST/WS 及 subaccount；不在 P 对冲入口；[适配器][P-ethereal] |
| StandX | ✓ | — | — | — | 钱包认证、JWT 与临时 Ed25519 请求签名；[适配器][P-standx] |
| Arcus | — | — | ✓ | — | 自实现原生 perp 协议；不能以 Arcus spot SDK 替代；[F trading][F-trading] |
| Lighter Robinhood | — | — | ✓ | — | 与 Core 不同 host、chain 与账户身份；[F providers][F-models]、[hosts][F-markets] |
| Entropy | — | — | ✓ | — | 使用 Hyperliquid 协议的 main/`io:*` 市场，带特定 builder 语义；[适配器][F-entropy] |
| Ondo Perps | — | — | ✓ | — | 原生 HMAC、认证发现 accountID、cross margin 范围；[适配器][F-ondo] |

`perp-dex-tools` 的 11 所以 [factory][P-factory] 为准，不能按 README 的较短名单漏计，也不能推断任意两所已有策略组合。P 的跨所入口严格为 Backpack/Extended/ApeX/GRVT/EdgeX/Nado/StandX 与 Lighter 的七类组合；GRVT v2 是同一组合的不同实现。C 的 `--exchange` 只接受 `edgex`，不是任意交易所套利框架。F 的五个可交易 provider 以 `TRADING_PROVIDERS` 为准，目录里的 Binance/直接 Hyperliquid 标识不是其已实现账户适配器。[P 入口][P-hedge]、[C 入口][C-entry]、[F 白名单][F-models]

### 2.1 鉴权与原生语义提取

本表记录固定源码采用的机制，不能作为未来实现时的完整官方 wire spec。所有数值编码、签名域、字段顺序和接口版本必须在接入阶段重新固定官方规范及测试向量。

| Venue | 源码采用的鉴权/依赖 | Titan 必须保留的差异 |
| --- | --- | --- |
| EdgeX | `edgex_sdk`；account ID、STARK 私钥 | contract ID、签名账户与 native client ID；受理与成交分开 |
| Backpack | `bpx` 包装；Ed25519 WS 签名 | timestamp/window、原生 symbol 与订单身份；不能只复用通用 HMAC |
| Paradex | Paradex SDK、L1 地址、L2 私钥/地址 | 环境与 Stark 账户、token/session 生命周期；依赖环境与其他所不同 |
| Aster | aiohttp/WS，HMAC-SHA256 | query/body 的精确签名字节、listen key；相似 Binance 不代表完全兼容 |
| Lighter | SignerClient/native signer | deployment、chain、account index、key index、nonce、client index、tx hash 与 order index 区分 |
| GRVT | `pysdk.grvt_ccxt*` | trading account、API key 和签名私钥，订单签名期限；CCXT 形状不意味着无差异 |
| Extended | `x10` Starknet SDK、vault/API/Stark key | subaccount vault、SNIP12 域、external ID；异步订单确认 |
| ApeX | `apexomni`、API 三元组和 Omni key seed | zk/key seed 派生与 API 身份共同校验；不挪用其他 ApeX 产品的协议 |
| Nado | `nado_protocol`、签名私钥、subaccount | owner/subaccount 编码、x18、expiration/appendix、nonce；轮询事实须有新鲜度 |
| Ethereal | `ethereal` SDK、私钥/subaccount UUID | linked signer、产品 ID、RPC/REST/WS 角色；签名权限需独立审核 |
| StandX | 钱包签登录，JWT、临时 Ed25519 body key | session 过期、请求 ID、临时公钥绑定；不能把解码 JWT 当本地验签 |
| Arcus | 自实现 Ed25519 canonical payload | 地址+subaccount、ticks/quantums、ns 时间与 expiry 的原生单位 |
| Lighter RH | 同族 native signer | Core/RH chain 不互换；F 快照采用 Core 304、RH 466324，接入时再核对 |
| Entropy | 官方 Hyperliquid SDK 的 action signing | 查询主交易账户而非 agent；DEX/asset namespace、共享抵押域、builder 权限 |
| Ondo | 标准库 HMAC-SHA256 | 保留 key/secret 前缀、method/path/body 签名字节、accountID 与原生 ticker |

前 11 行分别见上一表适配器；后四项见 [F 原生交易][F-trading]、[Entropy][F-entropy]、[Ondo][F-ondo]。本次可直接读取的官方补充包括 [Lighter WS][O-lighter]、[Extended API][O-extended] 与 [Hyperliquid nonce][O-hl-nonce]。Arcus/Ondo 官方鉴权页及尝试的 EdgeX 文档路径本次浏览未能读取，相关精确协议结论目前来自固定源码；这几项在实施前必须补官方规范，不能标为“官方复核完成”。

## 3. 策略逐项目分析

### 3.1 perp-dex-tools：单所获利挂单与 Lighter 对冲家族

单所 `trading_bot.py` 根据方向挂开仓单，成交后按 take-profit 参数挂反向平仓单，结合等待时间、最大挂单、grid-step、暂停价和停止价。它是具有库存风险的交易循环，不是资金费套利，也不能凭两次挂单称为无风险收益。[单所 bot][P-bot]

常规 hedge 入口把具体主所脚本与 Lighter 组合，主所挂单、Lighter 对手腿、库存检查及开平循环分别写在各脚本中，重复协议与业务逻辑较多。Maker/Taker、部分成交和取消细节必须逐脚本核对；不能将 README 的概括推广为七个版本行为完全一致。[入口][P-hedge]、[EdgeX hedge][P-hedge-edgex]

GRVT v2 明确采用双腿并发提交：维护最多 2000 个价差样本，超过 1000 个后，以中位数及约 2 bp 价格偏移形成阈值；条件使用两所可成交方向的 bid/ask。下单数量只对 GRVT 顶档数量取 min，随后 `asyncio.gather` 提交两腿。因此能参考“滚动基差中心+偏移”的思想，但它没有提供跨所原子性，也不能证明另一所全量深度足够。[GRVT v2 trading_loop][P-grvt-v2-loop]

吸收内容：适配器工厂、分离协议差异、post-only 主腿、双向价差门槛、滚动中心、库存检查。Titan 自行实现有界状态机、成本后价差、两腿深度、幂等证据、增量对冲及风控，不移植整套 Python 循环。

### 3.2 grid-trading-bot：单所等差/等比网格

配置动态导入适配器，实际文件只有 Lighter 与 ExtendedX。网格生成 `N+1` 价格线，按单格数量或总投资估算下单量，启动时布置当前价格两侧订单；确认成交后在相邻格挂反向单。整个策略只持有一个 adapter，没有跨所两腿关系。[入口][G-entry]、[网格][G-strategy]

可参考网格几何、价格/数量约束、启动及 teardown 的结构。其“清算价附近 1% 动态停止”只是启发式阈值：只在越界后查询；查仓失败会跳过；触发 stop 的 teardown 撤挂单，没有明确的库存平仓流程。因此不能作为 Titan 清算保护或完成平仓的证明。[风险与停止][G-risk]

### 3.3 perp-farming-bots：跨账户金额配对与耐久执行

`spread_hedging` 固定多、空两个账户/原生 ticker，用 `(short_sell/long_buy−1)×100` 作为入场价差。先执行多腿，按其确认成交金额计算空腿数量；各腿独立量化，空腿部分成交或未知后停止新增动作，并保留实际分配，不自动补腿。源码没有验证跨所 ticker 是否代表同一经济资产。[入场][F-open]、[参数][F-models]

这实际是“金额配对”策略：假设多腿 0.01 BTC×100000，空腿价格 100100，则空腿计划约 0.009990 BTC；即使两边金额相同，BTC 净数量仍不为零。不同资产（如 BTC/ETH）则还需要 beta/协方差模型，不能直接标为对冲。Titan 默认同资产 delta 配对，金额配对只能作为显式研究模式。

较有价值的工程机制是 `prepare → journal → single send → lookup original → apply evidence`；存储区分 prepared/submitting/submitted/unknown，写结果未知只查原操作。账户原生库存必须与分配总和相符。其 grid 为程序价格触发后的 IOC 目标库存模型，与 G 的交易所常驻挂单网格有本质区别。[执行][F-execute]、[持久化][F-store]、[grid][F-grid]

费用、实际资金费和积分采用不同证据口径；未知费用不补零。积分集成只有 RH 的账户官方总量，不能推导每个 bot 的积分收益。资金费展示不等于实现了资金费套利。[venue scope][F-scope]

吸收内容：耐久意图、只查原订单、不用 ACK 推仓位、账户身份防重、owned allocation、证据完整性、资金费单位与费用缺失处理、流动性不足的等待语义。保留 Titan Rust 执行模型，不迁入 NiceGUI/FastAPI/SQLAlchemy 或单个全局 asyncio 锁。

### 3.4 cross-exchange-arbitrage：EdgeX maker / Lighter 对手腿

多 EdgeX 方向判断为 `Lighter_bid−EdgeX_bid > long_threshold`；空 EdgeX 方向为 `EdgeX_ask−Lighter_ask > short_threshold`。这里 EdgeX 的 bid/ask 是候选 maker 价格，不是双腿立即成交收益；没有扣除费用和逆向选择。阈值单位是绝对价格差，不是 bp。[策略][C-loop]

EdgeX 发 post-only 并等待完成，再发 Lighter 对手单。名为 `place_lighter_market_order` 的方法实际签 LIMIT + GOOD_TILL_TIME，并给买卖价分别约 ±0.2% 的空间，`reduce_only=False`；不能称它为 IOC 或无条件市价成交。盘口管理分模块，但 offset 校验只保证变大，不证明连续无缺口。[订单管理][C-orders]、[盘口][C-book]

吸收内容：盘口、订单、库存、数据记录分责；maker 价差与 taker 价差应分别计算。其时钟、确认、库存与恢复弱点必须由 Titan 平台机制替代。

## 4. 已确认的拒绝移植项

| 问题 | 源码证据 | 对交易的影响 | Titan 约束 |
| --- | --- | --- | --- |
| 对冲超时后直接标记已成交 | [C `monitor_lighter_order`][C-unsafe] | 可能在裸露仓位仍存在时继续交易 | 超时进入 UNKNOWN；保留全量未决风险；按原身份查单/成交 |
| 网格查单失败时假定成交 | [G `_tick`][G-unsafe] | 网络故障也可能触发反向新订单 | 活动列表缺失只触发查证；失败阻止新格推进 |
| teardown 清理内存但不证明撤净/平仓 | [G `_teardown`][G-stop] | 停机界面与真实风险脱节 | Stop barrier；区分已撤、留仓、已平、未知 |
| 等美元金额、独立 ticker 无经济等价校验 | [F `try_open`][F-open] | 残余 delta 或完全不同资产风险 | InstrumentEquivalence 与 sizing policy 显式绑定 |
| 只核对单所顶档后双腿并发 | [P GRVT v2][P-grvt-v2-loop] | 对手腿深度不足、两边部分成交 | 两腿 VWAP/保护价/完整计划深度，先预占全部失败分支 |
| 将 offset 单调增误当连续性 | [C `validate_order_book_offset`][C-book] | 跳失增量后产生虚假价差 | 按交易所官方规则检测衔接，不臆造 `+1` 规则 |
| 通用查询重试返回默认值 | [P `query_retry`][P-base] | 0/空值可能掩盖失联 | 类型化 Unavailable/Stale/Unknown，与真实零区分 |
| 统一浮点精度或默认 market ID | [G Lighter][G-lighter]、[P base][P-base] | 错合约、错 lot、错误价位 | 原生目录+精确单位+整数倍对齐，不靠 quantize 猜 tick |

上述是固定位置的源码行为，不等于断言上游所有模式都会触发，也不扩展为未经阅读的其他版本结论。

## 5. 复用方式与许可证证据

| 项目 | 固定版本许可证证据 | 本次采用方式 |
| --- | --- | --- |
| P | [自定义 Non-Commercial LICENSE][P-license]，有明确商业使用限制 | 只分析机制和接口需求；不复制、翻译移植或引入其实现 |
| G | 根目录固定 tree 未见 LICENSE | 不把“公开仓库”当作可复制授权；独立实现几何及状态机 |
| F | [Apache-2.0][F-license]；另有 [UPSTREAMS][F-upstreams] 与 Hummingbot attribution | 本次仅作设计参考；未来复制须逐文件登记及保留相应 NOTICE |
| C | [AGPL-3.0][C-license] | 本次不复制实现；未来采用代码需先明确相应发布和使用义务 |

这张表是仓库文件的工程记录，不是对 Titan 未来商业分发的法律结论。设计、官方协议、开源代码、SDK 及 native signer 二进制分别登记来源和许可证。外部仓库内的推荐码、builder/integrator 默认值与依赖不能自动带入 Titan。

## 6. 对 Titan 的直接结论

1. 优先 Lighter Core 与 EdgeX，形成最小闭环；再扩展 Extended、GRVT、Backpack，最后按明确需求增加其余 venue。Lighter RH 必须作为独立部署验收。
2. 复用现有 Rust MarketService/AccountService/Runtime、ABI V13 和 `pair_arb`，不增加 Python 常驻交易网关。
3. 先补多腿风险、精确资产等价、增量成交对冲、未知订单恢复、逐所保证金，再增加策略种类。
4. 将单所 grid、跨所 maker/taker、跨所 taker/taker、金额配对、资金费 carry 分别定义；后两类与跨所库存网格是新增设计，不能宣称上游已完整提供。
5. 接入顺序不代表交易收益或实盘授权；每所/部署/账户模式的准入证据独立维护。

## 7. 固定源码与官方参考索引

以下链接固定到审查 SHA。行号只用于本次取证；升级后重新冻结并复查。

[P]: https://github.com/your-quantguy/perp-dex-tools/tree/4679a339b8cdc9998707feeda3c5d8b84fb8681f
[G]: https://github.com/caimantou9-cell/grid-trading-bot/tree/9e198c460f23f5df939bd48d0ecbea320f237c38
[F]: https://github.com/neal-zhu/perp-farming-bots/tree/503fb0486b05f37528c0032181fda2148f4d7640
[C]: https://github.com/your-quantguy/cross-exchange-arbitrage/tree/3dd42c06c20458e77eb87c17b4c014df83260c23
[P-factory]: https://github.com/your-quantguy/perp-dex-tools/blob/4679a339b8cdc9998707feeda3c5d8b84fb8681f/exchanges/factory.py
[P-hedge]: https://github.com/your-quantguy/perp-dex-tools/blob/4679a339b8cdc9998707feeda3c5d8b84fb8681f/hedge_mode.py
[P-edgex]: https://github.com/your-quantguy/perp-dex-tools/blob/4679a339b8cdc9998707feeda3c5d8b84fb8681f/exchanges/edgex.py
[P-backpack]: https://github.com/your-quantguy/perp-dex-tools/blob/4679a339b8cdc9998707feeda3c5d8b84fb8681f/exchanges/backpack.py
[P-paradex]: https://github.com/your-quantguy/perp-dex-tools/blob/4679a339b8cdc9998707feeda3c5d8b84fb8681f/exchanges/paradex.py
[P-aster]: https://github.com/your-quantguy/perp-dex-tools/blob/4679a339b8cdc9998707feeda3c5d8b84fb8681f/exchanges/aster.py
[P-lighter]: https://github.com/your-quantguy/perp-dex-tools/blob/4679a339b8cdc9998707feeda3c5d8b84fb8681f/exchanges/lighter.py
[P-grvt]: https://github.com/your-quantguy/perp-dex-tools/blob/4679a339b8cdc9998707feeda3c5d8b84fb8681f/exchanges/grvt.py
[P-extended]: https://github.com/your-quantguy/perp-dex-tools/blob/4679a339b8cdc9998707feeda3c5d8b84fb8681f/exchanges/extended.py
[P-apex]: https://github.com/your-quantguy/perp-dex-tools/blob/4679a339b8cdc9998707feeda3c5d8b84fb8681f/exchanges/apex.py
[P-nado]: https://github.com/your-quantguy/perp-dex-tools/blob/4679a339b8cdc9998707feeda3c5d8b84fb8681f/exchanges/nado.py
[P-ethereal]: https://github.com/your-quantguy/perp-dex-tools/blob/4679a339b8cdc9998707feeda3c5d8b84fb8681f/exchanges/ethereal.py
[P-standx]: https://github.com/your-quantguy/perp-dex-tools/blob/4679a339b8cdc9998707feeda3c5d8b84fb8681f/exchanges/standx.py
[P-base]: https://github.com/your-quantguy/perp-dex-tools/blob/4679a339b8cdc9998707feeda3c5d8b84fb8681f/exchanges/base.py
[P-bot]: https://github.com/your-quantguy/perp-dex-tools/blob/4679a339b8cdc9998707feeda3c5d8b84fb8681f/trading_bot.py
[P-hedge-edgex]: https://github.com/your-quantguy/perp-dex-tools/blob/4679a339b8cdc9998707feeda3c5d8b84fb8681f/hedge/hedge_mode_edgex.py
[P-grvt-v2]: https://github.com/your-quantguy/perp-dex-tools/blob/4679a339b8cdc9998707feeda3c5d8b84fb8681f/hedge/hedge_mode_grvt_v2.py
[P-grvt-v2-loop]: https://github.com/your-quantguy/perp-dex-tools/blob/4679a339b8cdc9998707feeda3c5d8b84fb8681f/hedge/hedge_mode_grvt_v2.py#L1194-L1343
[P-license]: https://github.com/your-quantguy/perp-dex-tools/blob/4679a339b8cdc9998707feeda3c5d8b84fb8681f/LICENSE
[G-entry]: https://github.com/caimantou9-cell/grid-trading-bot/blob/9e198c460f23f5df939bd48d0ecbea320f237c38/main.py
[G-strategy]: https://github.com/caimantou9-cell/grid-trading-bot/blob/9e198c460f23f5df939bd48d0ecbea320f237c38/grid/strategy.py
[G-lighter]: https://github.com/caimantou9-cell/grid-trading-bot/blob/9e198c460f23f5df939bd48d0ecbea320f237c38/exchanges/lighter.py
[G-extended]: https://github.com/caimantou9-cell/grid-trading-bot/blob/9e198c460f23f5df939bd48d0ecbea320f237c38/exchanges/extendedx.py
[G-risk]: https://github.com/caimantou9-cell/grid-trading-bot/blob/9e198c460f23f5df939bd48d0ecbea320f237c38/grid/strategy.py#L260-L332
[G-unsafe]: https://github.com/caimantou9-cell/grid-trading-bot/blob/9e198c460f23f5df939bd48d0ecbea320f237c38/grid/strategy.py#L214-L242
[G-stop]: https://github.com/caimantou9-cell/grid-trading-bot/blob/9e198c460f23f5df939bd48d0ecbea320f237c38/grid/strategy.py#L189-L212
[F-models]: https://github.com/neal-zhu/perp-farming-bots/blob/503fb0486b05f37528c0032181fda2148f4d7640/src/perp_farming_bots/models.py
[F-markets]: https://github.com/neal-zhu/perp-farming-bots/blob/503fb0486b05f37528c0032181fda2148f4d7640/src/perp_farming_bots/markets.py
[F-trading]: https://github.com/neal-zhu/perp-farming-bots/blob/503fb0486b05f37528c0032181fda2148f4d7640/src/perp_farming_bots/trading.py
[F-entropy]: https://github.com/neal-zhu/perp-farming-bots/blob/503fb0486b05f37528c0032181fda2148f4d7640/src/perp_farming_bots/entropy.py
[F-ondo]: https://github.com/neal-zhu/perp-farming-bots/blob/503fb0486b05f37528c0032181fda2148f4d7640/src/perp_farming_bots/ondo.py
[F-open]: https://github.com/neal-zhu/perp-farming-bots/blob/503fb0486b05f37528c0032181fda2148f4d7640/src/perp_farming_bots/runtime.py#L510-L609
[F-execute]: https://github.com/neal-zhu/perp-farming-bots/blob/503fb0486b05f37528c0032181fda2148f4d7640/src/perp_farming_bots/runtime.py#L402-L505
[F-store]: https://github.com/neal-zhu/perp-farming-bots/blob/503fb0486b05f37528c0032181fda2148f4d7640/src/perp_farming_bots/store.py
[F-grid]: https://github.com/neal-zhu/perp-farming-bots/blob/503fb0486b05f37528c0032181fda2148f4d7640/src/perp_farming_bots/grid.py
[F-scope]: https://github.com/neal-zhu/perp-farming-bots/blob/503fb0486b05f37528c0032181fda2148f4d7640/docs/exchanges.md
[F-license]: https://github.com/neal-zhu/perp-farming-bots/blob/503fb0486b05f37528c0032181fda2148f4d7640/LICENSE
[F-upstreams]: https://github.com/neal-zhu/perp-farming-bots/blob/503fb0486b05f37528c0032181fda2148f4d7640/UPSTREAMS.md
[C-entry]: https://github.com/your-quantguy/cross-exchange-arbitrage/blob/3dd42c06c20458e77eb87c17b4c014df83260c23/arbitrage.py
[C-loop]: https://github.com/your-quantguy/cross-exchange-arbitrage/blob/3dd42c06c20458e77eb87c17b4c014df83260c23/strategy/edgex_arb.py#L525-L564
[C-orders]: https://github.com/your-quantguy/cross-exchange-arbitrage/blob/3dd42c06c20458e77eb87c17b4c014df83260c23/strategy/order_manager.py
[C-unsafe]: https://github.com/your-quantguy/cross-exchange-arbitrage/blob/3dd42c06c20458e77eb87c17b4c014df83260c23/strategy/order_manager.py#L238-L251
[C-book]: https://github.com/your-quantguy/cross-exchange-arbitrage/blob/3dd42c06c20458e77eb87c17b4c014df83260c23/strategy/order_book_manager.py
[C-license]: https://github.com/your-quantguy/cross-exchange-arbitrage/blob/3dd42c06c20458e77eb87c17b4c014df83260c23/LICENSE
[O-lighter]: https://apidocs.lighter.xyz/docs/websocket-reference
[O-extended]: https://api.docs.extended.exchange/
[O-hl-nonce]: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/nonces-and-api-wallets
