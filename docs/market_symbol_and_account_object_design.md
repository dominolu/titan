# 市场标的与账户对象：从"策略硬编码单位"改为"策略读取对象属性"

日期：2026-11-11。状态：设计分析；P0（无 ABI 变更）按[实施计划](market_symbol_and_account_object_implementation_plan.md)实施中。
评审结论、需修订的断言与分阶段建议见[设计评审](market_symbol_and_account_object_design_review.md)（同日）。
P1 的落地契约见[策略观测协议](strategy_observation_protocol.md)与[实施计划](market_symbol_and_account_object_implementation_plan.md)。
本文回答一个问题：`tick_size`、`lot_size`、合约乘数这类**交易所事实**应该由市场数据里的
symbol 对象提供、账户侧同理，策略只引用它需要的对象和属性——按这个模型，整个项目哪些地方要改。
所有结论都对照当前代码给出位置；未经验证的推断会明确标注。

## 1. 目标模型

```
① 交易所元数据（connector.get_instruments）
        ↓ 精确十进制
② 市场服务：symbol 对象 = { native_symbol, asset_id, price_tick, quantity_lot,
                            contract_multiplier, min_quantity, min_notional_quote,
                            linear_quote_currency }
   账户服务：account 对象 = { 同一资产的单位, currencies[amount_unit], 持仓/保证金模式 }
        ↓ 策略绑定引用（按角色，不写数字）
③ 策略：ctx.symbol(asset_no).tick_size_lots_ratio / .contract_multiplier ...
         ctx.account_info(account_no).quote_currency / .amount_unit ...
```

三条原则：

1. **单位只有一个权威来源**（交易所元数据 → 绑定），不允许在每个消费点各写一份。
2. **策略不写交易所数字**：比例、最小量、最小金额一律从对象属性派生；除用于回归测试的显式覆盖。
3. **平台用权威单位做准入与证明**，不信任策略声明的数值。

## 2. 现状：链路逐层分析

| 层 | 现状 | 位置 |
| --- | --- | --- |
| 交易所元数据 | `InstrumentInfo` 有 `tick_size`/`lot_size`/`min_qty`/`contract_size`，但类型是 **f64** | `connector/src/api.rs:308-323` |
| 元数据 → 绑定 | **没有连线**：`runtime.toml` 里的单位是人手抄的，`get_instruments()` 只被探针/示例调用，生产绑定路径不读 | `configs/cross_exchange_mm.example.toml:62,74,91` |
| 市场 service | `MarketInstrumentBinding` 只有 `native_symbol`/`asset_id`/`price_tick`/`quantity_lot`，**缺 `contract_multiplier`、最小量、最小金额、报价币** | `crates/titan-market-service/src/model.rs:29-36` |
| 市场 service 对外 | `InstrumentSnapshot` 只有 `native_symbol`/`asset_id`/`available`，**没有任何单位** | `model.rs:316-321` |
| 账户 service | `AccountInstrumentBinding` 有 `price_tick`/`quantity_lot`/`contract_multiplier`/`linear_quote_currency`，**比市场侧更完整** | `crates/titan-account-service/src/model.rs:44-53` |
| 策略绑定 | `StrategyMarketBinding` 只有 `local_market_no`/`local_asset_no`/`source_key`/`asset_id`/`data_mode`；`ResolvedMarketBinding` 同样无单位 | `crates/titan-strategy-runtime/src/model.rs:49-55,224-231` |
| 策略 ABI：行情 | `TitanMarketView` 有 `tick_size`/`lot_size` 字段，但运行时**恒置为 1**，且视图里**没有 `contract_multiplier`** | `runtime_v13.rs:170-175`；`python/titan-strategy-sdk/titan_strategy/abi_v13.py:82-87` |
| 策略 ABI：账户 | `TitanAccountView` 只有 `state`/`account_epoch`/`account_sequence`/`reason`，**没有账户属性**；`TitanBalanceView` 只有 `total_units`/`available_units` | `crates/titan-strategy-runtime/src/v13.rs:347-361` |
| SDK 访问器 | 只有 `market/position/balance/account/active_orders/...`，**没有 `symbol`/`account_info`** | `python/titan-strategy-sdk/titan_strategy/context_v13.py:440-520` |
| 策略源码 | 把两个交易所事实写成编译期参数：`hedge_lots_per_maker_lot`、`reference_tick_ratio`（还有 `hedge_min_order_lots`、`hedge_min_notional_tick_lots`） | `strategies/cross_exchange_mm.py:129,134` 等 |
| 平台校验 | **完全不读** `reference_tick_ratio`（`grep` 零结果）；`hedge_lots_per_maker_lot` 只与配置里的 `base_delta_per_lot` 比，而那也是人填的 | `execution_binding.rs` 的 `LotRatio::exact` |

## 3. 问题清单

| 编号 | 问题 | 严重度 | 证据 |
| --- | --- | --- | --- |
| D1 | 策略看不到真实单位，只能硬编码比例；换交易所对必须重编译策略 | 高 | `runtime_v13.rs:173-174` 恒置 1 |
| D2 | 平台的安全证明（"对冲只缩小已证明敞口"）建立在**用户填的** `base_delta_per_lot` 上，从不与权威单位交叉核对 → 填错只会静默错对冲 | 高 | 修方案 §3.2 要求平台验证，尚未实现 |
| D3 | 绑定单位靠人手抄，与交易所元数据之间没有任何核对；`get_instruments()` 在绑定路径上没有消费者 | 高 | 见 2 节 |
| D4 | 市场侧绑定比账户侧少 `contract_multiplier`/最小量/最小金额/报价币，同一资产在两处各写一份 | 中 | `model.rs:29-36` vs `model.rs:44-53` |
| D5 | 交易所元数据是 f64，转十进制单位若不精确会引入量化误差，而绑定要求精确十进制 | 中 | `connector/src/api.rs:312-315` |
| D6 | 策略侧"最小量/最小金额"也是参数（`hedge_min_order_lots`、`hedge_min_notional_tick_lots`），属于交易所事实 | 中 | `cross_exchange_mm.py:130,131` |
| D7 | 账户属性（报价币、`amount_unit`、保证金货币）策略完全不可见，只能靠编译期常量推断 | 中 | `v13.rs:347-361` |
| D8 | 风险上限（`execution_limits.max_notional_tick_lots`）用原生 tick×lot 表达，要求配置者知道每腿单位 | 中 | `model.rs:140-144` |

## 4. 修改清单（按层，含依赖）

### 4.1 connector

| # | 改动 | 说明 |
| --- | --- | --- |
| C1 | `InstrumentInfo` 增补精确十进制的 `tick_size`/`lot_size`/`min_qty`/`contract_size`（或提供严格的 f64→`DecimalUnit` 转换并给出精度上限） | 解决 D5；否则绑定单位来源本身不可靠 |
| C2 | 提供把 `get_instruments()` 结果导出/校验绑定的入口（例如 `titan instruments verify --config runtime.toml`） | 解决 D3，让手抄单位可被核对 |

### 4.2 titan-market-service

| # | 改动 | 说明 |
| --- | --- | --- |
| M1 | `MarketInstrumentBinding` 增加 `contract_multiplier`、`min_quantity`、`min_notional_quote`、`linear_quote_currency` | 解决 D4，与账户侧对齐 |
| M2 | `InstrumentSnapshot` 返回完整 symbol 对象（含上述单位），或新增 `SymbolSnapshot` | 让"symbol 对象"成为可查询的一等概念 |
| M3 | 同一 `asset_id` 的市场侧与账户侧单位一致性校验（启动期） | 去掉两处各写一份的隐患 |

### 4.3 titan-account-service

| # | 改动 | 说明 |
| --- | --- | --- |
| A1 | 账户侧 symbol/currency 属性已有；补充最小量/最小金额（若从交易所元数据可得） | 解决 D6 的账户侧来源 |
| A2 | 暴露账户属性查询（`account_attributes(handle, asset_id)`），供预检与策略视图共用 | 解决 D7 |

### 4.4 titan-strategy-runtime

| # | 改动 | 说明 |
| --- | --- | --- |
| R1 | `StrategyMarketBinding` / `ResolvedMarketBinding` 携带单位；创建期从市场/账户服务读取，不由配置手抄 | 解决 D1/D3 的运行时侧 |
| R2 | 填充 `TitanMarketView.tick_size`/`lot_size` 为真实值 | 解决 D1 |
| R3 | 新增 `TitanSymbolView`（含 `contract_multiplier`、`min_quantity`、`min_notional_quote`、`quote_currency`）并挂到 context | 解决 D1/D6，**ABI 变更** |
| R4 | 新增 `TitanAccountInfoView`（`amount_unit`、保证金货币、持仓模式）并挂到 context | 解决 D7，**ABI 变更** |
| R5 | 创建期交叉校验：策略声明的 lot 比例 / tick 比例 vs 权威单位，不一致拒绝启动 | 解决 D2，**不需要 ABI 变更** |
| R6 | 风险上限支持以报价币种表达，由平台用权威单位换算成 tick×lot | 解决 D8 |

### 4.5 SDK

| # | 改动 | 说明 |
| --- | --- | --- |
| S1 | 新增 `ctx.symbol(asset_no)`、`ctx.account_info(account_no)` 访问器与 dtype | 配合 R3/R4 |
| S2 | `_PUBLIC_ACCESSORS` 增加两个名字；静态校验同步 | `static_compiler.py:62-66` |
| S3 | `ABI_VERSION` / `ABI_FINGERPRINT` 更新；`abi_v13.py` 增加新 dtype | 见第 5 节代价 |

### 4.6 策略

| # | 改动 | 说明 |
| --- | --- | --- |
| P1 | `hedge_lots_per_maker_lot` 改为从两条腿的 `quantity_lot × contract_multiplier` 派生，非整数拒绝启动 | 解决 D1 |
| P2 | `reference_tick_ratio` 改为从两条腿 `price_tick` 派生 | 解决 D1 |
| P3 | `hedge_min_order_lots` / `hedge_min_notional_tick_lots` 改为从 symbol 对象读取 | 解决 D6 |
| P4 | 保留可选覆盖参数，仅用于回归测试与极端场景 | 保持测试可复现 |

### 4.7 配置与文档

| # | 改动 | 说明 |
| --- | --- | --- |
| G1 | `runtime.toml` 的单位改为"可由元数据生成/校验"，手写保留但标记为待校验 | 解决 D3 |
| G2 | `scripts/cross_exchange_mm_preflight.py` 增加单位与交易所元数据一致性检查 | 现只做配置内部一致性 |
| G3 | 示例配置、部署模板、策略文档、工程记忆同步 | — |

## 5. ABI 代价（必须先说清楚）

`ABI_FINGERPRINT` 是**覆盖全部 dtype 的规范描述哈希**（`abi_v13.py:197`），Rust 侧是硬编码常量
（`v13.rs:23`、`v13.rs:1587-1589` 严格相等比较）。因此：

- **新增视图（R3/R4）会改变 `runtime_context_dtype` → 指纹改变 → 所有已编译产物被拒绝加载**。
  这不是"静默兼容"的追加：必须重新编译并重新签名全部策略产物。
- 反过来，**不改 ABI 的部分（R5、R6、M1-M3、C2、G2）可以先行**，且能立刻消掉最危险的问题 D2。

因此有三个可选路径：

| 路径 | 内容 | 代价 | 风险 |
| --- | --- | --- | --- |
| **P0：只做校验（推荐先做）** | R5 交叉校验 + M1-M3 单位单一来源 + C2 元数据校验 + G2 预检 | 无 ABI 变更，可独立验证 | 策略仍写死比例，换交易所仍需重编译 |
| **P1：暴露对象（ABI v14）** | R1-R4 + S1-S3 + P1-P4 | 指纹变更，全部产物重编译与重新签名 | 需要一次协调发布；旧 checkpoint 不能迁入 |
| **P2：上限币种化** | R6 | 依赖 P1 的换算能力 | 低 |

## 6. 分阶段实施与验收

| 阶段 | 交付 | 验收 |
| --- | --- | --- |
| P0-1 | `titan instruments verify`：交易所元数据 vs 绑定单位 | 单位不一致、缺资产、精度超限均拒绝；示例配置通过 |
| P0-2 | M1-M3：市场侧绑定补齐 + 双源一致性校验 | 同一资产两处不一致时启动失败 |
| P0-3 | R5：策略声明比例 vs 权威单位校验 | 比例非整数或与元数据不符时拒绝启动；正确配置通过 |
| P1-1 | R3/S1：symbol 视图与访问器 | 新样例策略能打印真实 tick/lot/multiplier；旧产物被明确拒绝并提示重编译 |
| P1-2 | R4/S2：账户属性视图 | 策略可读报价币与 `amount_unit` |
| P1-3 | P1-P4：`cross_exchange_mm` 去除硬编码比例 | 换一对 lot 不同的交易所，同一产物无需重编译即可启动（或明确拒绝非整数） |
| P2 | R6：上限按报价币种 | 配置里不再出现原生 tick×lot 数字 |

P0 全程不触碰 ABI，可完全在 `titan-build` 用现有产物验证；P1 需要一次协调的 ABI 版本发布。

## 7. 未决问题

1. **元数据从哪来**：启动期向交易所拉 `get_instruments()`，还是离线生成配置后冻结？前者需要网络与重试策略，后者需要可复现的导出工具。
2. **最小金额的计价币种**：交易所的 `min_notional` 通常以报价币计，与本项目 `linear_quote_currency` 是否总是一致，需要逐所核对。
3. **非整数 lot 比例的交易对**：派生后依然可能是非整数（例如 maker lot 更细），首版继续拒绝，还是引入有理数与尾量风控？这会影响 P1-P3 的验收口径。
4. **ABI 版本策略**：v14 是保持"新宿主拒绝旧产物"的一致性，还是允许双版本并存一段时间？当前 `validate_descriptor` 严格比较，双版本需要额外分支。
5. **账户侧费率/保证金模式**：是否属于"账户对象属性"需要暴露给策略？当前策略用编译期费率常量近似。

## 8. 与其他待办的关系

- 本文 P0-3（R5）就是修复方案 §3.2 未完成的那条，也是上一轮 B8 讨论里"平台信任用户填的比例"的根因。
- 本文 P1 之后，`multi_binding` 的字段可以进一步缩减（`base_delta_per_lot`、`quote_currency`、`margin_domain` 全部派生），与工作清单 B8 剩余的瘦身方向一致。
- 本文不改变任何实盘授权；未访问真实账户或凭据。
