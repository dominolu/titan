# 市场标的与账户对象：实施计划与任务分解

日期：2026-11-13（2026-11-16 更新）。状态：**P0 完成**（P0-01…P0-15 已验收）；**P1 主体已切换**（P1-01…P1-05、P1-08、P1-09 完成）；P1-06 已完成（第 46 轮）；P1-10（矩阵全绿）达成：✅23 / ➖2（第 7、13 为用户决策删除的功能）/ 0 未完成。依据：

- [市场标的与账户对象设计](market_symbol_and_account_object_design.md)（问题清单 D1–D8、修改清单 C/M/A/R/S/P/G）；
- [设计评审](market_symbol_and_account_object_design_review.md)（对 R2/R5/M1 的修订、结构性缺口、阶段 A–D）；
- [策略观测协议](strategy_observation_protocol.md) 与[验收矩阵](verification/2026-11-13/symbol-units-p0/v14-acceptance-matrix.md)。

本文只负责**把设计拆成可执行任务并记录状态**；接口语义以上述文档为准。任何阶段都必须按
[AGENTS.md](../AGENTS.md) 在 `titan-build` 完成编译、格式检查、Clippy 与测试；本机只做编辑与审查。
本文不改变任何实盘授权，不访问真实账户或凭据。

## 0. 结论与边界

1. **顺序不可交换**：ABI V14 会让全部已编译产物因 `ABI_FINGERPRINT` 变化被拒绝，因此先完成 P0
   （无 ABI 变更、可独立验收），再一次性做 P1。P0 期间**不新增** `TitanSymbolView`/`ctx.symbol`。
2. **先挡错数，再给对象**：评审证明当前最危险的不是"策略看不到单位"，而是三份互不校验的人填值
   （配置 `base_delta_per_lot`、策略参数、账户绑定单位）。P0 用权威单位校验配置与绑定，能在
   不改 ABI 的前提下消掉 D2/D3 的主要风险。
3. **单一校验入口**：新增 Rust 校验器一处实现，`core-run`、`titan instruments verify`、
   管理端与 `scripts/cross_exchange_mm_preflight.py` 都调用它；不再新增第四套解释。
4. **与既有清单合并**：P0 与[实盘准备工作清单](cross_exchange_mm_live_worklist.md)的 D2（原生元数据核对）
   与 B8（启动预检）同批验收，不另立并行入口；B9 的整体验证包含本计划 P0 的验证。
5. 本计划**不改变**事件视图、typed state、恢复/持久化、管理平台边界；这些在 P1 只做身份字段追加。

## 1. 现状与本轮目标（2026-11-13）

| 事实 | 位置 | 影响 |
| --- | --- | --- |
| `MarketInstrumentBinding` 只有 `native_symbol`/`asset_id`/`price_tick`/`quantity_lot`，且无 `deny_unknown_fields` | `crates/titan-market-service/src/model.rs` | 示例配置的 `contract_multiplier` 被静默丢弃（D3/D4、评审 §2.6） |
| 市场与账户一致性只比较 `price_tick`/`quantity_lot`，不含乘数 | `crates/titan-cli/src/core_runtime.rs` 的 `validate_strategy_unit_consistency` | 乘数写错不会被发现 |
| `DecimalUnit` 用派生 `Deserialize`，可绕过 `new()` 写入 0 或 scale>18 | `crates/titan-domain-types/src/lib.rs` | 单位可非法，身份摘要不可复现（P0-8） |
| 加载失败被 `map_err` 吞成泛化错误 | `crates/titan-strategy-runtime/src/compiled_strategy.rs` | "旧产物被拒绝并提示重编译"无法验收 |
| preflight 自行推导 `base_delta_per_lot` 整数比 | `scripts/cross_exchange_mm_preflight.py` | 与平台算法各写一份，可能假 PASS |

本轮（P0 第一批）交付：`P0-01`、`P0-02`、`P0-03`、`P0-04`；随后按顺序做 `P0-05`–`P0-11`。

## 2. 任务分解总表

状态口径：`待做` / `进行中` / `已完成（本机）`（只表示代码完成） / `已验收（titan-build）`（附验证记录链接）。
"本机"通过不算验收。

### 2.1 P0：无 ABI 变更，先消除"错数"

| # | 任务 | 主要位置 | 验收要点 | 依赖 | 状态 |
| --- | --- | --- | --- | --- | --- |
| P0-01 | `DecimalUnit` 严格化：自定义 `Deserialize`（走 `new()`，拒绝 0/scale>18/未知字段）、checked 乘除、既约有理数 helper | `crates/titan-domain-types/src/lib.rs` | 非法单位反序列化失败；`{10,1}` 与 `{1,0}` 归一化相等；乘除溢出返回 `None` | — | 已验收（2026-11-13） |
| P0-02 | 市场绑定补齐单位并严格化：`contract_multiplier`、`min_quantity`、`min_notional_quote`、`linear_quote_currency`、`deny_unknown_fields` | `crates/titan-market-service/src/model.rs`、`configs/`、`deploy/` | 示例配置的 `contract_multiplier` 进入绑定；未知字段拒绝启动；缺乘数拒绝 | P0-01 | 已验收（2026-11-13） |
| P0-03 | 统一单位校验器：`base_delta_per_lot` 派生、市场/账户双源一致性（含乘数与报价币）、多绑定腿与配置比例核对、整数 lot 比与有理 tick 比 | 新模块 `crates/titan-strategy-runtime/src/instrument_validation.rs` | 任一不一致以稳定原因码拒绝；正确配置通过；单测覆盖每类失败 | P0-01/P0-02 | 已验收（2026-11-13，11 项单测） |
| P0-04 | `titan instruments verify --config` 入口：复用 P0-03，输出 JSON 报告与原因码 | `crates/titan-cli/src/main.rs`、`core_runtime.rs` | 示例配置通过；构造不一致配置被拒并给出原因码 | P0-03 | 已验收（2026-11-13，5 项 CLI 集成测试） |
| P0-05 | preflight 收敛：删除本地比例推导，改为调用 `titan instruments verify --json` | `scripts/cross_exchange_mm_preflight.py` | 同一不一致输入，CLI 与 preflight 结论与原因码一致 | P0-04 | 已验收（2026-11-13，7 项 preflight 测试） |
| P0-06 | 加载错误可见性：`V13LoadError` 原因码透传到 `StrategyError`，含 domain/major/minor/len/field 类别与"重新编译"提示 | `compiled_strategy.rs`、`v13.rs` | 原因码单测；旧产物 CLI 端到端待 P1-07 重编产物后补 | — | 已验收（结构原因码；旧产物 CLI 端到端待 P1-07） |
| P0-07 | 跨语言指纹/布局双向校验：Rust 从布局重算 descriptor 与 `V13_ABI_FINGERPRINT`、Python 常量比对 | `v13.rs`、`fixtures/abi_v13_descriptor.json`、SDK 测试 | 单侧改布局未改常量 → 测试失败 | — | 已验收（fixture 哈希 + 逐字段 offset 断言） |
| P0-08 | offline 恢复语义补齐：`offline_v13.rs` 补 `on_recover`、恢复订单注入、检查点冻结与代次完成边界 | `offline_v13.rs`、`crates/titan-cli/tests/offline_recovery.rs` | 快照身份/代次/恢复回调失败关闭；3 项原生回调测试 | P0-06 | 已验收（2026-11-13） |
| P0-09 | 编译期参数身份进入部署包 + 部署期 `parameters_digest` 校验（P1 前过渡） | `model.rs`、`service_core.rs`、`strategy_workspace.rs` | 可选 pin 不一致拒绝激活；`strategy build --runtime` 自动注入稳定摘要 | P0-03 | 已验收（2026-11-13） |
| P0-10 | connector 精确元数据出口：`InstrumentInfo` 精确十进制、量纲、`min_notional`+币种、`synthetic` 标记；`InstrumentSnapshot` 带完整单位 | `connector/src/api.rs` 与 8 个 venue 适配器、`titan-market-service` | 快照含绑定单位；OKX/Binance/Gate/HL/Backpack/Lighter 精确字段有单测；EVM/Solana 标 `synthetic`（无实机数据） | — | 已验收（2026-11-13） |
| P0-11 | 离线元数据快照格式 + trace 升版（V14 前移部分） | `titan-market-service` 元数据快照、CLI trace v2、`fixtures/*_v13_trace.json` | 快照 digest 可跨语言复现；缺快照/digest 不符/未覆盖资产 → 回测拒绝 | P0-10 | 已验收（2026-11-13） |

**P0-12…P0-15（2026-11-14 四十一轮立项）**：第 40 轮结项审计发现 P0 阶段留下四处"断言未真正生效"的缺口。
它们都不涉及 ABI 布局，属于 P0 性质的补漏，按"先挡错数"原则插在 P1 尾部任务之前。

| # | 任务 | 主要位置 | 验收要点 | 依赖 | 状态 |
| --- | --- | --- | --- | --- | --- |
| P0-12 | 守卫与验证脚本对齐 trace schema 4 与 `native-v14`：`verify_cli_bundle.py`/`verify_depth_trace.py` 仍内联 trace `schema_version=1`（现必被拒绝）；删除 `crates/titan-cli/src/*_v13_trace.json`（schema 3 死文件） | `scripts/`、`crates/titan-cli/src/` | 两个脚本对新宿主跑通；死文件删除后无引用 | — | 已验收（2026-11-14，第 41 轮） |
| P0-13 | 结果包 ABI 身份校验：`BundleManifest.abi_fingerprint` 目前**只写不校**，"旧结果包被拒绝"只覆盖 schema 版本 | `crates/titan-cli/src/main.rs` | 指纹不符以稳定原因码拒绝 + 单测 | — | 已验收（2026-11-14，第 41 轮） |
| P0-14 | 弃用单位占位单一来源：离线回放视图未写 `tick_size/lot_size` 占位（仅在线写），离线与在线视图事实不一致 | `runtime_v13.rs`、`offline_v13.rs`、`v13.rs` | 离线/在线同一常量；不变量测试覆盖两处 | — | 已验收（2026-11-14，第 41 轮） |
| P0-15 | 矩阵第 3 项（跨场所同产物）与第 6 项（元数据拉取错误路径）的离线可验证部分：同一产物在两套场所单位下派生同一比例、非整数比被拒；在线拉取的超时/429/缺字段以可注入 transport 覆盖 | 测试 + provider 骨架 | 两套快照 → 同一比例；错误路径稳定码 | P0-11 | 已验收（2026-11-14，第 41 轮；在线真实拉取待授权） |

### 2.2 P1：ABI V14 一次切换（不可拆分发布）

| # | 任务 | 主要位置 | 验收要点 | 依赖 |
| --- | --- | --- | --- | --- |
| P1-01 | 新 `v14` 模块：root 尾部追加、`TitanObjectRef`、payloads（symbol/account/currency/pair/status/fee）、`object_query` 预留、`object_schema_hash`、status 生命周期 | `crates/titan-strategy-runtime/src/v14.rs` | 尺寸/对齐 const 断言；`size_of(root)==440`、ref 32B；payload offset 断言（已完成并切换：440B root、32B ref、payload offset 断言均在测试中） | P0 全部 |
| P1-02 | `object_table.rs`：表构建器（在线/离线共用）、checked 派生整数、`object_identity_digest`、`metadata_snapshot_digest`、指针稳定性与 `Send` 契约 | `object_table.rs`、`object_sources.rs`、`service_core.rs`、`runtime_v13.rs` | 写入端不变量断言；摘要可复现；容量/准入拒绝（完成：构建器+适配器+服务层按实例建表/挂载/门禁；服务级端到端用例待元数据快照入口） | P1-01 |
| P1-03 | manifest/能力/两层门禁：`object_schema_major/minor/hash`、`object_requirements`、`requires_fee`、`object_identity_version`、`object_context` 能力 | `object_gate.rs`、`v13.rs` manifest 解析、`model.rs`/`compiled_strategy.rs` 能力映射 | 已验收（2026-11-14 前序轮次：解析/门禁/能力位与运行时 manifest 接线全部就绪） | P1-02 |
| P1-04 | Python `abi_v14.py`/`context_v14.py`：6 个 accessor、intrinsic 扫描、静态校验、`_PUBLIC_ACCESSORS`、testing 构造 helper | `python/titan-strategy-sdk/`、fixtures | 已验收（`cli.py` 已按 `runtime_abi.abi_version` 选择 `abi_v14`/`abi_v13`，端到端场景通过） | P1-03 |
| P1-05 | 编译端常量/const 断言/错误透传/`titan.lock` 同步 | `crates/titan-cli/src/compiler.rs`、`strategy_workspace.rs` | 已验收（compiler.rs 注入 `runtime_abi{abi_version:14, fingerprint, object_schema_hash}`；双向指纹校验通过） | P1-04 |
| P1-06 | 启动期元数据拉取接入启动流程（在线）与离线快照；失败即拒绝启动 | `core_runtime.rs`、`metadata_transport.rs`、market/account service | 超时/缺字段/不一致负例拒绝 | P0-10 | **已验收（2026-11-16，第 46 轮）**：`metadata_source = "hyperliquid"` 走公开 `/info`，真实拉取 235 个资产；错误路径 9 项单测；HL 不发布 tick，故以 `metadata_unit_unrepresentable` 拒绝进入多腿。OKX/Binance 出网被阻断，未取真实证据 |
| P1-07 | 全量重编译/重签 6 个产物，更新 `titan.lock` | `artifacts/`、`deploy/pair_arb/`、CLI fixture | 旧产物混入拒绝 | P1-05 | **已验收（2026-11-15，第 42 轮）**：6 个产物全部重编为 ABI 14 且 `validate` 通过；`runtime.toml` 的 `expected_digest` 已同步；ABI 13 负例返回 `artifact 13, host 14; recompile`。生产签名未做（需用户私钥） |
| P1-08 | 周边身份与脚本同步：result bundle `abi_fingerprint`、trace schema v2、`verify_*.sh` 清单 | `main.rs`、`scripts/` | 旧结果包/trace 拒绝 | P1-07 |
| P1-09 | `runtime_type`/`loader_type` 迁移（`native-v14`）与配置迁移命令 | `core_runtime.rs`、`compiled_strategy.rs`、`deploy/runtime.toml` | 旧 `native-v13` 配置明确报错/提示（完成：重命名+稳定原因码+迁移提示；未提供自动迁移命令） | P1-03 |
| P1-10 | 验收矩阵 1–25 全绿 | 全部 | 新证据，不复用历史记录（进行中：见 `docs/verification/2026-11-13/symbol-units-p0/v14-acceptance-matrix.md`：✅22 / 🟡2 / ⬜1） | P1-01..09 |

### 2.3 P2：策略迁移

| # | 任务 | 验收要点 |
| --- | --- | --- |
| P2-01 | `cross_exchange_mm` 读 `ctx.pair(0)`/`ctx.fee()`，删除 4 个交易所事实参数与 2 个费率参数，升 `source_version`/`parameter_schema` | 完成：6 个参数已删、on_start 读对象、关系校验依赖对象（正例/负例通过）；跨场所同产物启动验证待第 3 项 |
| P2-02 | `grid`/`pair_arb` 同类参数清理；`pair_arb` 比例改为 pair 对象或标注"禁止用于交易事实" | 完成：pair_arb 比例读 ctx.pair；grid `quote_units_per_tick_lot` 删除并改由 ctx.symbol 派生 |
| P2-03 | 管理端/`runtime_host` 同步：绑定指纹展示、quote→tick×lot 换算改调统一校验器、摘要脱敏输出 | 完成：诊断快照暴露 `object_identity_digest`/`metadata_snapshot_digest`，单位校验单一实现（verify/core-run/preflight 共用）；HTTP 专用端点未新增 |

### 2.4 P3：清理与文档

| # | 任务 | 验收要点 |
| --- | --- | --- |
| P3-01 | 删除 hftbacktest `strategy` 模块、导出、6 个单测、文档引用 | 限定 grep + workspace/examples 构建通过 |
| P3-02 | 清理 V13 兼容占位（`tick_size/lot_size`、`reserved` 语义、旧能力位重复）并改名/删除 `v13` 模块 | **已完成可做部分（2026-11-15，第 42 轮）**：`tick_size/lot_size` 弃用占位单一写入点；`reserved` 语义集中说明 + 不变量测试（唯一活槽是 `TitanSubmitOrderRequest.reserved[0]` 承载 `CommandPurpose`）；`OBJECT_CONTEXT` 能力位三处表达收敛到 `OBJECT_CONTEXT_CAPABILITY` 单一来源 + 跨位集语义测试。**未做**：删除 `reserved` 字段、`v13` 模块改名/删除（需另一次 ABI major；改名会打翻 `check_single_runtime.sh` 路径白名单，收益低于风险） |
| P3-03 | 同步工程记忆、设计/评审/观测协议文档与发布说明 | 完成：记忆 §28、观测协议改 V14 口径、V13 设计与状态文档加历史横幅、SDK README 补对象 accessor 与 `native-v14`；发布说明待生产签名后补（产物已重编为 ABI 14） |

## 3. P0 详细规格（本阶段实现契约）

### 3.1 P0-01 DecimalUnit

```rust
impl DecimalUnit {
    pub fn checked_mul(self, rhs: Self) -> Option<Self>;   // 精确乘积；scale>18 或 coefficient 溢出 → None
    pub fn checked_div(self, rhs: Self) -> Option<Ratio>;  // 精确有理数；除零 → None
    pub fn ratio_to(self, rhs: Self) -> Option<Ratio>;     // self / rhs，既约
    pub fn ceil_div_units(numerator: Self, denominator: Self) -> Option<u64>; // ceil(num/den) 整数商
}
pub struct Ratio { pub numerator: u64, pub denominator: u64 } // 既约；denominator>0
```

- 反序列化接受 `{ coefficient = 1, scale = 2 }` 与字符串 `"0.01"`（等价于 `FromStr`），未知字段拒绝；
  序列化格式不变（仍为 `{coefficient, scale}`），避免配置/wire 变更。
- 归一化：`{10,1}` 与 `{1,0}` 反序列化后相等；`0` 与 `scale>18` 拒绝。
- 不改 f64 转换路径；`as_f64` 仍仅用于展示/观测。

### 3.2 P0-02 市场绑定

```rust
#[serde(deny_unknown_fields)]
pub struct MarketInstrumentBinding {
    pub native_symbol: Arc<str>,
    pub asset_id: AssetId,
    pub price_tick: DecimalUnit,
    pub quantity_lot: DecimalUnit,              // 一个原生数量 lot（合约张数或基础币，由元数据量纲声明）
    pub contract_multiplier: DecimalUnit,       // 一个原生 lot 对应的基础币面值
    #[serde(default, skip_serializing_if = "Option::is_none")] pub min_quantity: Option<DecimalUnit>,
    #[serde(default, skip_serializing_if = "Option::is_none")] pub min_notional_quote: Option<DecimalUnit>,
    #[serde(default, skip_serializing_if = "Option::is_none")] pub linear_quote_currency: Option<Arc<str>>,
}
```

- 单位字段**仍然必填**（`contract_multiplier` 无 default）：读不到就拒绝启动，不退回默认值。
- 可选字段用 `Option` 表达"交易所不可得"，不得用 0 冒充。
- 示例/部署配置按原生元数据填写并标注核对状态；`deny_unknown_fields` 让手误键直接失败。
- 同步更新 `validate_runtime_definitions`：同一 `source_key` 内 `asset_id`/`native_symbol` 唯一。

### 3.3 P0-03 统一单位校验器

输入：`&[MarketSourceDefinition]`、`&[AccountDefinition]`、`&[StrategyDefinition]`（含 `ExecutionLimits`/
`MultiBindingExecutionLimits`）、可选元数据快照。输出：`InstrumentValidationReport { instruments: [...], errors: [...] }`，
错误为 `{ code, subject, detail }`，`code` 稳定：

| 原因码 | 条件 |
| --- | --- |
| `instrument_units_invalid` | 单位不可用（0/scale>18/派生溢出/除不尽） |
| `instrument_units_market_account_mismatch` | 同一 `asset_id` 在市场与账户两侧 tick/lot/乘数/报价币不一致 |
| `instrument_quote_currency_mismatch` | 两条腿共同报价币不同，或市场侧报价币与账户 `linear_quote_currency` 解析结果不同 |
| `instrument_base_delta_mismatch` | 配置 `base_delta_per_lot` ≠ `quantity_lot × contract_multiplier`（多绑定腿） |
| `instrument_lot_ratio_not_integral` | maker 最小成交不能整数对冲（`LotRatio` 非整数） |
| `instrument_min_fill_not_hedgeable` | `maker.min_quantity_lots × ratio` 小于 hedge 最小可下单量 |
| `instrument_metadata_missing` | 提供了快照但缺少被绑定品种 |
| `instrument_metadata_mismatch` | 快照与配置单位/报价币不一致 |

- 校验覆盖**所有**定义（不只策略绑定）：同 `asset_id` 的市场侧与账户侧必须一致（含乘数）。
- 保留现有"策略绑定必须存在于市场/账户定义"的检查，错误文案改为原因码 + 细节。
- 校验器不读网络、不读时钟、不做 f64；全部 checked 整数/十进制运算。

### 3.4 P0-04 `titan instruments verify`

```
titan instruments verify --config RUNTIME.toml [--metadata-snapshot FILE] [--json]
```

- 退出码：0 = 通过；非 0 = 有错误，stdout 输出 JSON（`--json`）或人类可读摘要。
- 复用 P0-03；`core-run` 启动路径调用同一函数（`ApplicationConfig::validate` 内）。
- 报告包含每个绑定品种的派生事实（`base_delta_per_lot`、`min_qty_lots`）与"元数据未校验"标记，
  避免把"配置内部一致"误读为"已与交易所核对"。

### 3.5 P0-05 preflight 收敛

- Python preflight 只做：读取配置 → 调 `titan instruments verify --json` → 按其 `errors` 决定 PASS/FAIL；
  删除本地 `hedge_lots_per_maker_lot`/`reference_tick_ratio` 推导。
- 保留 preflight 的只读/离线性质（不联网、不触发交易所调用）。

## 4. 验收与证据

**2026-11-13 P0 第一批证据**：[标的单位校验基础](verification/2026-11-13/symbol-units-p0/README.md)
（fmt/check/clippy 通过，915 Rust + 162 SDK + 7 preflight 通过；ABI 指纹未变）。

- P0 每项完成后在 `titan-build` 隔离目录执行：`cargo fmt --check`、`cargo clippy --workspace --all-targets
  --all-features -- -D warnings`、`cargo test --workspace --all-targets`、Python SDK 测试、CLI smoke。
- 证据写成 `docs/verification/2026-11-1X/<task>/README.md` 脱敏摘要，原始日志留在私有档案；
  历史验收记录不能替代本轮结果。
- 交易所原生事实（OKX `ctVal`/张数、Gate 公开与 REST 乘数、HL tick 分档）仍需按
  [配对与原生元数据方案](cross_exchange_mm_venue_pair_and_data.md) 的 D2 清单取证；`待验证` 结论不得作为准入依据。

## 5. 风险与未决

1. **配置严格化会拒绝现有配置**：缺 `contract_multiplier` 的 `runtime.toml` 会在加载期失败。这是
   有意的 fail-closed；迁移方式是按原生元数据补字段（示例配置已给出形态），不提供静默默认。
2. **`quantity_lot` 量纲历史债**：P0-02 只要求显式乘数，量纲统一（原生/张数/基础币）由 P0-10 的
   元数据字段给出；在量纲未取证前，跨所比较只用 `base_delta_per_lot`。
3. **校验器归属**：放在 `titan-strategy-runtime`（已依赖 market/account service 且已有多绑定校验）；
   若后续 market/account service 需要独立校验，只复用 `titan-domain-types` 的算术 helper，不复制逻辑。
4. **P1 不可拆分**：一次 ABI 变更必须同时更新 root/refs、payload、manifest、SDK、产物、脚本；
   任何"只改一半"都会让已编译产物被拒而无法运行。
5. **`min_notional` 计价币**：不同交易所可能以基础币或报价币计；P0-10 必须在元数据字段上显式标注币种，
   不得默认与 `linear_quote_currency` 一致。
6. **非整数 lot 比例**：首版继续拒绝（P0-03 `instrument_lot_ratio_not_integral`），不引入尾量风控。

## 6. 维护

- 任务状态变化时直接更新第 2 节表格与本节日期，不追加矛盾段落。
- 架构决定变化时同步更新[工程记忆](memory/engineering.md)与本计划引用的三份设计文档。
