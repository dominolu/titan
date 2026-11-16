# 市场标的与账户对象设计评审：现状核对、结构性缺口与改造清单

日期：2026-11-11。评审对象：[市场标的与账户对象设计](market_symbol_and_account_object_design.md)（下称"设计文档"）。
状态：**设计评审，未实施。** 本文只给出核对结论、修订建议与改造清单；不改变任何实盘授权，未连接真实账户或凭据。

方法：只读核对当前工作区代码与既有文档，不编译、不跑测试、不联网。文中 `file:line` 是评审时的工作区位置，代码移动后需要重新核对；标"待验证"的交易所事实（如 OKX `ctVal`、张数语义）不能作为准入证据。

## 0. 结论

1. **方向成立。** 三条原则——单位只有一个权威来源、策略不写交易所数字、平台用权威单位做准入与证明——与[工程记忆](memory/engineering.md)第 3/4/14/15/19 节的既有边界一致，值得作为路线图。
2. **现状核对大部分准确。** 设计文档对字段缺失、`tick_size/lot_size` 恒 1、ABI 指纹严格比较、平台不读策略参数等断言逐条成立（见第 1 节）。
3. **但按当前文字实施 P0/P1 会在三处走偏**：R2 在 i64 字段上不可实现；R5 缺少"策略声明值"的来源；M1 用"再补一份单位"代替"统一量纲与单一权威"。另有四类设计缺口必须先补：对象分层、ABI 十进制表达、元数据生命周期/恢复身份、一次 ABI 变更的身份清单与失败可见性。
4. **建议**：先修订设计文档第 2/3/4/5 节，再按第 5 节修订后的阶段 A 开工；P0 的校验工作与既有工作清单 D2/B8 合并，不另立第四套校验入口。

## 1. 核对成立的部分

| # | 设计文档断言 | 核对结果与证据 |
| --- | --- | --- |
| 1 | 市场绑定只有 `native_symbol`/`asset_id`/`price_tick`/`quantity_lot` | 成立，缺 `contract_multiplier`、最小量、最小金额、报价币：`crates/titan-market-service/src/model.rs:29-36` |
| 2 | `InstrumentSnapshot` 没有任何单位 | 成立：`crates/titan-market-service/src/model.rs:315-320`；市场服务对外只有这一个快照出口：`crates/titan-market-service/src/service.rs:24-47` |
| 3 | 账户绑定比市场侧完整 | 成立，含 `contract_multiplier`、`linear_quote_currency` 与 `AccountCurrencyBinding.amount_unit`：`crates/titan-account-service/src/model.rs:44-60` |
| 4 | 账户单位进入账户事实/风控 | 成立：`crates/titan-account-service/src/risk.rs:129-240` 的 `notional_units`/`collateral_units`/保证金投影 |
| 5 | 策略绑定与解析绑定没有单位 | 成立：`crates/titan-strategy-runtime/src/model.rs:49-55`、`model.rs:224-231` |
| 6 | `TitanMarketView.tick_size/lot_size` 运行时恒置 1 | 成立：`crates/titan-strategy-runtime/src/runtime_v13.rs:166-175` |
| 7 | 行情视图没有 `contract_multiplier`；账户视图没有账户属性 | 成立：`crates/titan-strategy-runtime/src/v13.rs:326-362`；ABI 设计文档只定义布局、未定义 `tick_size/lot_size` 语义：`docs/strategy_abi_v13_typed_state_blob_technical_design.md:1108-1145` |
| 8 | SDK 没有 `symbol`/`account_info` 访问器 | 成立：`python/titan-strategy-sdk/titan_strategy/context_v13.py:440-520`，现有访问器为 market/position/balance/account/active_orders 等 |
| 9 | 策略把交易所事实写成编译期参数 | 成立：`strategies/cross_exchange_mm.py:129`、`:134`、`:139`；平台侧对 `hedge_lots_per_maker_lot`/`reference_tick_ratio` 零引用（`crates/` 内 grep 零结果） |
| 10 | ABI 指纹覆盖全部 dtype、Rust 侧严格相等比较 | 成立；拒绝加载只由 `abi_fingerprint` 相等保证：`crates/titan-strategy-runtime/src/v13.rs:1583-1600`、`:1630-1634`；Python 侧 `python/titan-strategy-sdk/titan_strategy/abi_v13.py:178-197` |
| 11 | `get_instruments()` 在生产绑定路径没有消费者 | 成立：唯一调用点在 examples/probe 与 connector 会话内部；绑定单位由手写配置进入，`configs/cross_exchange_mm.example.toml:62,74,91,108` |
| 12 | 平台"完全不读 `reference_tick_ratio`" | 成立：该参数只存在于策略源码与测试 |
| 13 | 风险上限用原生 tick×lot 表达 | 成立：`crates/titan-strategy-runtime/src/model.rs:140-144`；但管理受控路径已有"报价币上限 → tick×lot"换算先例：`crates/titan-cli/src/management/control.rs:189-215` |

## 2. 必须修正的判断

### 2.1 R2「把 `tick_size/lot_size` 填真实值」不可实现

两个字段是 `i64`（`crates/titan-strategy-runtime/src/v13.rs:326-336`，Python 侧 `abi_v13.py:82-87` 同为 `<i8`），而真实 tick/lot 是小数（例如 `0.0001`）。`tick_size=1` 在"1 个 ABI 价格单位 = 该腿 `price_tick`"的解释下本来就是自洽的；问题不是"1 是错的"，而是**跨腿量纲与十进制事实没有承载位置**。

**修订建议**：删除 R2，改为两步——先在文档中明确 ABI 单位语义（1 个 ABI price tick / quantity lot 代表什么），再由一次 ABI 变更新增符号视图承载十进制事实。不要试图往 `i64` 里塞小数。

### 2.2 §5「必须升 ABI 版本」「必须重新签名」表述不准确

- 拒绝加载由 fingerprint 相等决定，版本号只是附加错误码。仓库已有**同一 v13 内两次只改指纹不升版本**的先例（`6b7e11cc…`、`7864d0be…`，见[SDK README](../python/titan-strategy-sdk/README.md)、[system_refactor_checklist.md](system_refactor_checklist.md)）。升 14 是命名决策，与拒绝机制无关。
- 签名是否必需取决于发布策略（编译端签名可选，只有受控实盘强制）：`crates/titan-strategy-runtime/src/v13/compiler.rs:375-384`。

**修订建议**：改写为"新增视图会改变 fingerprint → 全部产物必须重编译；是否升 14 单独决策；受控实盘才强制重签"。

### 2.3 R5「创建期校验策略声明的比例」没有数据来源

运行时只有 `parameters_digest` 与 `initial_state`（`crates/titan-strategy-runtime/src/v13.rs:791-800`），参数值在编译/发布侧，部署期明确禁止覆盖（`crates/titan-strategy-runtime/src/service_core.rs:2018-2019`），`runtime.toml` 的 strategies 段也没有参数。

**修订建议**：P0 改为"编译期从运行配置派生参数 + 部署侧校验 `parameters_digest`"（无 ABI、无新通道）；或明确新增一个非 ABI 参数通道并纳入签名与发布校验。二选一并写进文档，不要保留"创建期校验、不需要 ABI 变更"这种两头都不成立的表述。

### 2.4 D2 的机制描述错误，真实风险更大

平台不读 `hedge_lots_per_maker_lot`，也不与账户单位交叉核对；它只在**两条配置腿**之间计算 `base_delta_per_lot` 的整数比：`crates/titan-strategy-runtime/src/execution_binding.rs:276`、`crates/titan-strategy-runtime/src/execution_reservation.rs:418`。因此实际存在**三份互不校验的人填值**：

1. 运行配置里的 `base_delta_per_lot`；
2. 策略参数里的 `hedge_lots_per_maker_lot`；
3. 账户绑定里的 `quantity_lot/contract_multiplier`。

**修订建议**：R5 重新表述为"用权威单位校验**配置**的 `base_delta_per_lot`"，并说明策略参数在 P1 之后如何改为读取平台校验过的整数事实（避免策略重算一遍）。

### 2.5 M1 不是"单一权威"，而是补齐一份副本；量纲尚未统一

市场侧 `quantity_lot` 的实际量纲随 venue/connector 实现不同：

- OKX 公开行情是合约张数，直接除以 `quantity_lot`（`connector/src/okx/brokerapi.rs:240`、`connector/src/market_event.rs:291-304`）；
- Gate CrossEx 公开行情先经 connector 内部乘数换成基础币再除（[工程记忆 §8/§9](memory/engineering.md)）；
- 市场绑定没有 `contract_multiplier`，也没有量纲声明，因此无法从绑定判断"每 ABI lot 的基础币 delta"。

直接给市场绑定补 `contract_multiplier` 而不先统一量纲，会让同名字段在不同 venue 表示不同事实。

**修订建议**：先引入显式量纲（原生数量单位 / 合约张数 / 基础币）与"每 ABI lot 的基础币 delta"字段，再把市场与账户两侧收敛到同一 `InstrumentSpec` 引用。

### 2.6 D3/D4 低估了现状：已有一条可复现的假通过链路

- Rust 侧已有启动期市场/账户一致性校验：`crates/titan-cli/src/core_runtime.rs:1290-1365`（调用点 `:309`），但只比较 `price_tick` 与 `quantity_lot`，**不含 `contract_multiplier`**；`crates/titan-account-service/src/observation.rs:508-527` 在风控行情绑定不一致时会**静默跳过**。
- 示例配置在 **market_sources** 里写了 `contract_multiplier`（`configs/cross_exchange_mm.example.toml:62,74`），但 `MarketInstrumentBinding` 没有该字段且父结构之外没有 `serde(deny_unknown_fields)`（`crates/titan-market-service/src/model.rs:28-36`）→ **该键今天被静默丢弃**。
- 而 `scripts/cross_exchange_mm_preflight.py:52-57,156-190` 恰恰读取这个被丢弃的键来推导 `base_delta_per_lot` 的整数比。

后果（条件：OKX `sz` 为合约张数、`ctVal` 为每张基础币，属待验证的交易所事实）：示例中 OKX 腿的 `quantity_lot=0.01`、`contract_multiplier=1` 与"下单数量按张、风险按基础币"两种口径至少有一个是错的；preflight 却可能给出"整数比 10"的假 PASS，而真实比例可能是 0.1（不可对冲）。这正是设计文档要消除的风险，已经以配置模板的形式存在。

**修订建议**：把"配置严格化（拒绝未知字段）+ 校验单一入口 + 示例配置用原生元数据重新核对"列为 P0 的第一批交付，而不是等 P1。

### 2.7 热路径担忧方向对，但现状没有"每 tick 读 symbol"

单位在冷启动被编译进 `AssetId → InstrumentUnits`（`connector/src/market_event.rs:113-165`），热路径只做 HashMap 查表，无锁、无分配（`market_event.rs:236-245,379-385`）。真正的约束应当写成："禁止在发布路径回查 service、加锁或做十进制解析；符号属性在创建期冻结、回调期静态绑定"。

### 2.8 R6 不依赖 P1；多绑定准入本身另有缺陷

- 换算能力已存在：`crates/titan-account-service/src/risk.rs:129-240`，以及管理端"报价币上限 → tick×lot"先例 `crates/titan-cli/src/management/control.rs:189-215`。§5 表中"P2 依赖 P1 的换算能力"应删掉。
- 反过来，多绑定里有三个字段目前没有运行时消费者：`min_notional_quote`、`max_market_age_ns`、`margin_domain`（`crates/titan-strategy-runtime/src/execution_binding.rs:30-48`），且准入没有新鲜 BBO 门（`crates/titan-strategy-runtime/src/runtime_v13.rs:1338-1390`）。这比"上限怎么表达"更应先补。

## 3. 结构性设计缺口

### 3.1 对象分层不清（最根本）

设计文档把三类不同事实都塞进"symbol 对象 / account 对象"：

- **交易所事实**：`price_tick`、`quantity_step`、`contract_size`、`min_qty`、`min_notional`、线性/反向、报价币与结算币；
- **每腿 ABI 单位**：行情 ABI lot 与账户 ABI lot 的量纲、每 lot 基础币 delta；
- **账户属性**：`amount_unit`、保证金币、持仓模式、费率。

建议改为三层：

```
InstrumentSpec（唯一权威，按 (source_key, native_symbol)）
  └─ 绑定层派生（创建期冻结、进入身份）
       market_abi_unit / account_abi_unit（含 base_delta_per_lot）
  └─ AccountAttributes（只放账户级属性，不复制 instrument 事实）
```

账户侧不应再抄一份 instrument 事实——这正是 D4 想解决、但 M1 用"复制副本"回应的原因。

### 3.2 ABI 表达力未定义

`DecimalUnit` 是 `coefficient: u64 + scale: u8`（`crates/titan-domain-types/src/lib.rs:75-127`），ABI 只有 i64/u32。设计文档只说新增 `TitanSymbolView`（含 `contract_multiplier`、`min_quantity`、`min_notional_quote`、`quote_currency`），没有说明编码方式。

建议**不要让策略用 float64 做十进制换算**（现状 `reference_tick_ratio` 就是 float64 参数，`strategies/cross_exchange_mm.py:66`、`:134`）。平台在创建期算好并暴露"策略可直接使用的整数事实"：

- `base_delta_per_lot`；
- `min_qty_lots`；
- `min_notional_quote_units`；
- tick 比的分子/分母（或已约分的整数比）；
- `quote_currency_no`；
- 原生 decimal 身份作为观测量单独放，不参与准入算术。

这样既保留精确性，也避免"平台证明"与"策略计算"变成两套实现。

### 3.3 元数据生命周期与恢复身份缺失

设计文档 §7 把"启动期拉取还是离线冻结"留成未决，但这条决定多个下游：

- 元数据摘要必须进入 `definition_version`、`binding_digest` 与恢复身份（`crates/titan-strategy-runtime/src/v13.rs:1201-1260`、`crates/titan-strategy-runtime/src/runtime.rs:79-128`）；
- 否则 R1 只加字段、不进摘要，旧 checkpoint 会**用新单位重新解释旧 tick/lot** 静默恢复；
- 运行中交易所改步长必须 fail-closed，不能静默采用新单位。

推荐离线冻结 + 摘要（与现有"配置即身份"的口径一致），启动期只校验、不联网。若选择启动期拉取，需同时写清重试、超时、降级与"配置 vs 元数据不一致即拒绝"的语义。

### 3.4 一次 ABI 变更要跟着变的身份清单 + 失败可见性

设计文档只提"产物重编译/重新签名"，至少还漏了：

- `V13_ABI_FINGERPRINT` 常量（`crates/titan-strategy-runtime/src/v13.rs:23-26`）、Python/Rust 两侧 dtype、offset、const 断言、编译端 descriptor；
- context `struct_size` 与接收端 `>=` 语义；
- checkpoint/recovery 身份、观测 schema、管理库中的目录与模板、trace schema、`deploy/` 摘要；
- 已编译产物清单（当前仓库内 `artifacts/` 4 个、`deploy/pair_arb` 1 个、CLI fixture 1 个；**历史回测报告里的产物属于冻结证据，不应重建**）。

另外，ABI 不匹配目前会被 `map_err` 吞成泛化错误（`crates/titan-strategy-runtime/src/compiled_strategy.rs:352-392`），设计文档 §6 的 P1-1 验收（"旧产物被明确拒绝并提示重编译"）**当前不满足**，需要先让 `AbiMismatch` 原因透传到 `StrategyError`。

### 3.5 校验要收敛成一处

现在已经有三套单位/配置校验：Rust CLI（`core_runtime.rs`）、account observation（`observation.rs`）、Python preflight（`scripts/cross_exchange_mm_preflight.py`）。再新增 `titan instruments verify` 就是第四套。

建议：Rust 单一 validator（读绑定 + 元数据快照 + 派生参数），CLI/preflight/管理端都调用它；否则"单一权威"会退化成"单一权威 + 多份解释"。

## 4. 模块改造清单

| 层 | 模块/位置 | 改动 | 优先级 |
| --- | --- | --- | --- |
| domain-types | `crates/titan-domain-types/src/lib.rs:75-127` | `DecimalUnit` 自定义 Deserialize 走 `new()`（当前可绕过校验写入 0 或 scale>18）；补 checked 乘除与有理数比；如需 ABI 定标，先定"10^-k 定点"或 `{coefficient, scale}` 二元组 | P0 |
| connector | `connector/src/api.rs:308-323` 与 8 个 `BrokerApi` 实现 | 精确十进制（`Option` 化）、量纲字段、`min_notional` + 币种、`synthetic` 标记；修 Binance `minNotional` 解析后丢弃、OKX `price/qty_precision` 失真、Gate `contract_size` 强制 1.0、HL tick 不可得、EVM/Solana 综合常量、Lighter scale=18 边界。**热路径不改** | P0-P1 |
| connector | 冷路径元数据探针 | 新增 service 级 `get_instruments` 出口（当前 connector 不向上暴露，`InstrumentChanged` 是死事件），供 verify/启动校验；不进入热路径 | P1 |
| market-service | `model.rs:29-36`、`registry.rs:148-182`、`service.rs:24-47` | 绑定改为 `InstrumentSpec` 引用 + 每 ABI lot 基础币 delta；registry 强校验；新增冷路径 `instrument_units()`；`InstrumentSnapshot` 与 `SymbolSnapshot` 分开（f64 观测值不得进权威路径）；配置拒绝未知字段 | P0-P1 |
| account-service | `model.rs:44-60`、`risk.rs:129-240`、`observation.rs:508-527` | 保留 `risk.rs` 作为唯一 notional/collateral 算式；账户属性查询（`amount_unit`/保证金币/持仓模式）；一致性检查从"静默跳过"改为"拒绝"；事实记录单位摘要 | P1 |
| strategy-runtime | `model.rs:49-55`、`model.rs:224-231`、`runtime_v13.rs:166-175` | 绑定创建期从 market/account service 取权威单位；R5 校验配置 `base_delta_per_lot` 与权威 base delta；单位信息进 `binding_digest`；恢复期重建校验；离线/回测从绑定注入 | P0-P1 |
| ABI | `v13.rs:326-362`、`v13.rs:598-656`、`offline_v13.rs:38-60` | context **尾部追加** `symbols_ptr/len`、`account_infos_ptr/len`；新增独立数组视图（绝不动既有 view 大小）；回调期静态绑定、零分配；ABI 不匹配原因透传 | P2（一次协调发布） |
| SDK/编译链 | `abi_v13.py:82-197`、`context_v13.py:440-520`、`static_compiler.py:62-66`、`testing.py:106-141` | 新 dtype + borrow 式 accessor + 只读污点根 + 内联视图；context 保持 `struct_size >=` 兼容判定；常量/指纹收敛为单一来源 | P2 |
| 策略 | `strategies/cross_exchange_mm.py:129-139` | 比例/最小量改为读符号视图或平台派生整数事实；整数比较、不用 float 定比分；保留显式覆盖参数并标注"仅回归"；删参会改 `parameter_schema`/`parameters_digest`，需升版本 | P2 |
| CLI/管理端 | `core_runtime.rs:1290-1365`、`management/control.rs:189-215`、`scripts/cross_exchange_mm_preflight.py` | 校验收敛到单一 validator（含 `contract_multiplier`/base delta/报价币/元数据摘要）；新增 `titan instruments verify`（离线快照优先）；受管上限换算复用 `notional_units`；多绑定进管理入口（当前明确拒绝） | P0-P1 |
| 持久化/恢复 | `v13.rs:1201-1260`、`crates/titan-strategy-runtime/src/runtime.rs:79-128`、observation | 单位/元数据摘要进绑定与恢复身份；不一致拒绝恢复（当前恢复期用当前配置重建预占，改单位会重解释存活订单）；历史事实记录单位 | P1-P2 |
| 配置/文档/部署 | `configs/cross_exchange_mm.example.toml`、[venue_pair_and_data.md](cross_exchange_mm_venue_pair_and_data.md)、[工作清单](cross_exchange_mm_live_worklist.md) | 修正示例（market 侧 `contract_multiplier` 被丢弃）；schema 版本与迁移指引；与既有 D2/B8 清单合并；同步工程记忆 | P0 |

## 5. 建议的实施顺序（对设计文档 P0/P1/P2 的修订）

### 阶段 A：无 ABI，先挡住"错数"

1. `DecimalUnit` 严格反序列化；配置拒绝未知字段。
2. Rust 单一 validator：base delta（`quantity_lot × contract_multiplier`）、报价币、市场/账户一致性、策略派生参数；CLI/preflight 调用同一实现。
3. 编译期派生参数 + 部署侧校验 `parameters_digest`。
4. 修正示例配置，preflight 改用 validator 的结论；用原生元数据核对 OKX/Gate 腿。

验收要点：非法单位（0、scale>18）、未知字段、双源单位不一致、派生比例非整数都拒绝启动；示例配置在离线元数据快照下通过；现有产物与 ABI 不变。全部在 `titan-build` 执行。

### 阶段 B：无 ABI，建立单一权威

1. connector 精确元数据 + 离线快照 + `titan instruments verify`。
2. `InstrumentSpec` 进入 market/account 绑定（含量纲、base delta、最小量、最小金额、报价币）。
3. 单位/元数据摘要进入绑定与恢复身份。
4. 修正 Gate/OKX 语义与全部示例/部署模板。

验收要点：元数据缺失、精度超限、量纲未知、摘要不一致均拒绝；同一资产在两侧的 base delta 不一致时启动失败；恢复期单位摘要不匹配时明确拒绝而不是重解释。

### 阶段 C：一次 ABI 变更

1. context 尾部新增 `symbols`/`account_infos` 指针，新增独立数组视图。
2. SDK accessor、静态校验、内联测试视图。
3. 策略改用符号对象/平台派生整数事实。
4. 全量重编译当前在跑的产物（`artifacts/` 4 个 + `deploy/pair_arb` + CLI fixture），按发布策略重签；ABI 不匹配错误需可读。

验收要点：旧产物被拒绝且错误信息提示重编译；新样例策略能读到真实单位；换一对 lot 不同的交易所，同一产物无需重编译即可启动（或明确拒绝非整数）；checkpoint 身份校验拒绝跨版本恢复。

### 阶段 D：上限币种化与多绑定补齐

R6 报价币上限；补新鲜 BBO 门；把 `min_notional_quote`/`max_market_age_ns`/`margin_domain` 接上消费者；多绑定进管理受控路径或明确长期边界。

## 6. 需要设计作者拍板的问题

1. **量纲**：`quantity_lot` 统一为"原生数量单位"还是"基础币 delta"？合约型 venue（OKX 张 / Gate 公开张数 vs REST 基础币）如何标记？
2. **ABI 表示**：`{coefficient, scale}` 双字段、10^-k 定点，还是平台派生整数事实？未定则 R3/P1 无法落地。
3. **元数据来源**：离线冻结 + 摘要（推荐）还是启动拉取？摘要进入哪个身份字段？
4. **R5 数据来源**：编译期派生参数 + 部署摘要校验，还是新增非 ABI 参数通道？
5. **ABI 版本策略**：沿用"v13 内只改 fingerprint"先例，还是正式升 14（并重命名 `native-v13`/`V13_*`）？不建议双版本并存，违背既有"不放宽 ABI/Schema/摘要校验"边界。
6. **多绑定是否进管理受控路径**：不进的话 R6 只在 core-run 生效，管理端仍走单腿 tick×lot 限额，两套口径长期并存。

## 7. 与既有工作的关系

- 设计文档 P0-1/P0-3 与[实盘准备工作清单](cross_exchange_mm_live_worklist.md)的 D2（原生元数据核对）与 B8（启动预检）高度重叠；应与 D2/B8 合并验收，而不是新增并行入口。
- [多绑定受控执行设计](cross_exchange_mm_multi_binding_design.md)已冻结 `base_delta_per_lot`、`quote_currency`、`margin_domain` 等字段；本设计应引用并收敛到同一 `InstrumentSpec`，避免同一事实出现第三、第四份拷贝。
- [工程记忆](memory/engineering.md)第 3 节（恢复与执行一致性）、第 4 节（持仓模型尊重交易所语义）、第 14/15 节（管理控制与金额敞口口径）、第 19 节（SQLite 恢复身份）是本文档的硬约束；实现后需按记忆文件的更新规则补充结论与证据链接。

### 7.1 设计文档需要修订的章节

- §2 现状表：D2 的机制描述（"hedge_lots_per_maker_lot 只与 base_delta_per_lot 比"不成立）；补充已有一致性校验与"示例配置字段被静默丢弃"。
- §3 问题清单：D3/D4 补充量纲问题；D5 补充"原始十进制串在映射点被丢弃"与 0.0 哨兵问题。
- §4.1 C1/C2：C1 范围扩展到量纲、`min_notional`、合成常量标记；C2 改为"先有 connector 元数据出口，再加 verify 入口"。
- §4.2 M1/M3：改为量纲统一与 `InstrumentSpec` 引用，而不是给市场绑定再补一份单位。
- §4.4 R1/R2/R5：R2 删除或改写；R5 改写数据来源；R1 明确"创建期读取 + 摘要进身份"。
- §5：修正"必须升版本/必须重签"；补充一次 ABI 变更的身份清单与错误可见性。
- §6：P0-1 的验收应构建在单一 validator 与示例修正上；阶段划分按第 5 节调整。
- §7：把本文第 6 节的问题并入未决问题清单，并标注推荐选项。

## 8. 验证与维护边界

- 本文为只读设计评审：**未编译、未运行测试、未联网、未连接真实账户或凭据**；引用的代码位置与断言不构成新的功能验证。
- 后续任何实现改动都须按 [AGENTS.md](../AGENTS.md) 在 `titan-build` 完成编译、格式/静态检查与测试；历史验收记录不能替代新修改的验证证据。
- 交易所事实（OKX `ctVal`/张数、Gate 公开与 REST 乘数、HL tick 分档）仍需按[配对与原生元数据方案](cross_exchange_mm_venue_pair_and_data.md)的 D2 清单逐项取证，标"待验证"的结论不得作为准入依据。
- 本文结论随代码演进复核；结论过期时直接修订并注明日期，不追加互相矛盾的段落。
