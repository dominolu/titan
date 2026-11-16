# 市场标的与账户对象 P0：单位校验基础（2026-11-13）

本记录对应[实施计划](../../../market_symbol_and_account_object_implementation_plan.md) P0 的第一批任务
P0-01…P0-07。全部为非实盘、非联网验证：未连接真实账户、未访问交易所元数据、未部署。

## 1. 范围与基线

- 基线：本地工作树（2026-11-13，包含用户未提交的 cross_exchange_mm 阶段 A/B 改动）。
- **ABI 不变**：未修改 `StrategyRuntimeContextV13`、任何 `v13_view!` 结构或回调签名；
  `V13_ABI_FINGERPRINT` 仍为 `7864d0be…50ed`，已编译产物无需重编。V14 root/payload 未实现。
- 完成：P0-01…P0-11 全部（含 P0-06 的结构化原因码；旧产物 CLI 端到端留待 P1-07 重编产物）。
  P0-10 覆盖 8 个 venue 适配器的精确元数据与 `InstrumentSnapshot`；P0-11 覆盖离线元数据快照、
  trace schema 2 与配置/快照核对。

## 2. 变更内容

| 任务 | 变更 |
| --- | --- |
| P0-01 | `DecimalUnit` 增加自定义 `Deserialize`（统一走 `new()`，接受 `{coefficient,scale}` 或精确十进制字符串，拒绝 0/scale>18/未知字段）、`checked_mul`、`ratio_to`、`ceil_div_by` 与既约 `Ratio`；序列化格式不变 |
| P0-02 | `MarketInstrumentBinding` 增加必填 `contract_multiplier` 与可选 `min_quantity`/`min_notional_quote`/`linear_quote_currency`，并启用 `deny_unknown_fields`；示例/部署配置补齐字段；修正示例策略缺少 `recovery`/`definition_version` 导致整份配置无法加载的问题 |
| P0-03 | 新增 `titan_strategy_runtime::validate_instrument_units`：派生 `base_delta_per_lot`、跨市场源 `asset_id` 冲突、市场/账户单位与报价币一致、策略绑定存在性、多绑定腿配置基数核对、整数 lot 比、最小 maker 成交可对冲、共同报价币；稳定原因码 8 个 |
| P0-04 | 新增 `titan instruments verify --config … [--json]`，`core-run` 与 CLI 共用同一校验器；JSON 含派生事实与 `errors[].code` |
| P0-05 | `scripts/cross_exchange_mm_preflight.py` 删除本地比例推导，改为调用平台校验器并原样报告原因码；无二进制时受控实盘 FAIL、shadow WARN |
| P0-06 | `V13LoadError` 新增 `AbiVersion`/`AbiFingerprint`/`AbiLayout`，`NativeV13Loader` 不再吞错，映射为稳定原因码（`v13_abi_fingerprint_mismatch` 等）与“重新编译”提示 |
| P0-07 | 新增 `fixtures/abi_v13_descriptor.json`（SDK 规范化描述字节）；Rust 测试断言其 SHA-256 == `V13_ABI_FINGERPRINT` 且逐 struct/size/offset 与 `size_of`/`offset_of!` 一致；Python 测试断言 fixture == `canonical_abi_descriptor()` |

## 3. 门禁与结果（titan-build）

工具链：Rust 1.94.1；Python 3.11.14（numpy 2.2.6、numba 0.63.1，编译服务器本地环境）。命令与结果：

| 门禁 | 结果 |
| --- | --- |
| `cargo fmt --all -- --check` | 通过（0 diff） |
| `cargo check --workspace --all-targets --all-features` | 通过 |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | 通过（0 warning） |
| `cargo test --workspace --all-targets --all-features` | **915 passed、0 failed、29 ignored**（52 个测试目标） |
| `cargo test --workspace --all-features --doc` | 2 passed、0 failed、1 ignored |
| Python SDK：`python -m unittest discover -s python/titan-strategy-sdk/tests` | **162 passed** |
| preflight：`python -m unittest test_cross_exchange_mm_preflight` | **7 passed** |
| `titan instruments verify --config configs/cross_exchange_mm.example.toml --json` | exit 0，`valid=true`（模板全部 disabled，报告 0 个已启用绑定） |
| `titan instruments verify --config deploy/pair_arb_v13/runtime.toml --json` | exit 0 |
| CLI 集成测试 `crates/titan-cli/tests/instruments_verify.rs` | 5 passed（通过/乘数不一致/未知字段/缺乘数/非法单位） |
| 校验器单测 `instrument_validation` | 11 passed（含报价币、基数、比例、最小成交、source 冲突、disabled 跳过） |

`cargo test` 需要 `TITAN_STRATEGY_PYTHON`/`TITAN_STRATEGY_SDK` 指向可导入 numba/llvmlite 的解释器；
未设置时 6 项 `single_file_strategy` 因默认 `python3` 缺 `llvmlite` 失败，设置后全部通过（环境差异，
非代码回归）。

## 3.1 同日第二轮补充（P0-08…P0-09 与 P0-10 快照部分）

| 任务 | 变更 | 验证 |
| --- | --- | --- |
| P0-08 | `OfflineV13Adapter` 新增 `freeze_checkpoint` 与 `recover`：恢复校验过的快照、仅在声明 `RECOVERY_ORDER_RESULTS` 时注入恢复订单 payload、`Recover` 回调尝试下单时失败关闭、按在线顺序完成 reconcile 边界并检查代次递增 | `crates/titan-cli/tests/offline_recovery.rs` 3 项：状态/代次恢复、订单 payload 注入、恢复期下单被拒 |
| P0-09 | `CompiledStrategyMetadata.parameters_digest` 暴露产物参数身份；`StrategyFileRef` 增加可选 `parameters_digest` pin，`validate_manifest` 不一致时以 `parameters_digest_mismatch` 拒绝激活；`titan strategy build --runtime` 自动把稳定摘要注入模板 `package`（跨构建可复现） | `service_core` 单测 1 项（pin 匹配/缺省/不匹配）；`crates/titan-cli/tests/parameters_pin.rs` 1 项（build 产物与注入 pin 一致、artifact digest 仍保留） |
| P0-10（部分） | `InstrumentSnapshot` 增加 `price_tick`/`quantity_lot`/`contract_multiplier`/`min_quantity`/`min_notional_quote`/`linear_quote_currency`，由连接器从冻结绑定填充；调用方不再需要回读配置 | `connector` 工厂测试断言快照携带真实绑定单位 |

本轮门禁（同一服务器、同一环境）：`cargo fmt --check` 通过；Clippy `-D warnings` 0 warning；
`cargo test --workspace --all-targets --all-features` **920 passed、0 failed、29 ignored**；
doctest 2 passed、1 ignored。失败数为 0 的完整日志保存在私有目录。

## 3.2 同日第三轮补充（P0-10 精确元数据）

| 项 | 内容 |
| --- | --- |
| 模型 | `InstrumentInfo` 增加 `tick_size_exact`/`lot_size_exact`/`min_qty_exact`/`contract_size_exact`/`min_notional_quote`、`quantity_dimension`（native/contract）、`synthetic` 与 `base_delta_per_lot()`；原 `f64` 字段保留但明确标注仅观测 |
| 解析 | `DecimalUnit::FromStr` 支持精确科学计数（`1e-7`、`2.5E-8`），交易所步长不再经过 `f64` |
| OKX | `quantity_dimension=contract`，`ctVal` 为每张基础币；tick/lot/min 从原生字符串精确解析（单测断言） |
| Binance | `PRICE_FILTER`/`LOT_SIZE`/`MIN_NOTIONAL` 精确解析（单测断言，含 `base_delta_per_lot`） |
| Gate CrossEx | tick/lot/min/`min_notional` 精确解析；规则里的 `contract_size` 是遗留展示字段，**不用于换算**（单测断言仍为 1） |
| Hyperliquid | 数量步长为精确 10 的幂；官方 API 不提供 tick，`tick_size_exact=None` 而非猜值（新增单测） |
| Backpack | 过滤器的 tick/step/min 精确解析（新增单测） |
| Lighter | 10^-decimals 精确表示，`min_base_amount` 精确解析（扩展单测） |
| EVM / Solana | 宿主常量标记 `synthetic=true`；无实机元数据，未进入准入算术 |

本轮门禁：fmt 通过、Clippy 0 warning、`cargo test --workspace --all-targets --all-features`
**922 passed、0 failed、29 ignored**（connector 388 项通过）、doctest 2 passed / 1 ignored；
13 个变更文件本地与编译服务器 SHA-256 一致。

已知缺口：精确元数据目前**还没有消费者**——启动期拉取并与配置绑定核对的接线属于 P0-11/P1-06；
本轮没有访问任何交易所网络。

## 3.3 同日第四轮补充（P0-11 离线元数据快照）

| 项 | 内容 |
| --- | --- |
| 快照类型 | `titan-market-service` 新增 `MetadataSnapshot`/`MetadataInstrument`：schema 版本、采集时间、逐 `asset_no` 的单位、最小量/金额、报价币与 `synthetic` 标记 |
| 摘要规则 | digest = SHA-256(规范 JSON：按 `(source_key, asset_no, native_symbol)` 排序、键字母序、缺失写 `null`)；instrument 顺序变化不影响摘要，任何字段编辑都会改变摘要 |
| CLI | `titan instruments verify --metadata-snapshot FILE` 增加摘要校验、资产覆盖与单位/报价一致性核对（`instrument_metadata_missing`/`instrument_metadata_mismatch`/`instrument_metadata_digest_mismatch`），JSON 输出 `metadata_verified`/`metadata_digest`；新增 `titan instruments snapshot-digest [--update]` |
| trace v2 | trace `schema_version=2` 必须内嵌 `metadata` 快照；加载时校验 schema、摘要、覆盖初始行情每个资产，缺失/不符直接拒绝；`input_manifest` 记录快照摘要 |
| fixtures | `grid/dual_ma/pair_arb_v13_trace.json` 升为 schema 2 并内嵌 `synthetic=true` 的测试元数据；Python 测试新增“schema 1 被拒绝”断言 |
| 跨语言 | 同一快照的摘要由 Python 规范化和 Rust `snapshot-digest` 分别计算并比对一致（`e3a8a951…`、`ebc1523d…`） |

本轮门禁：fmt 通过、Clippy 0 warning、`cargo test --workspace --all-targets --all-features`
**927 passed、0 failed、29 ignored**、doctest 2 passed / 1 ignored；SDK 162 项、preflight 7 项通过；
12 个变更文件本地与编译服务器 SHA-256 一致。

共享编译服务器本轮外部负载较高（load average 20–30，非本任务进程），原生 describe 偶发超过产品
60 秒期限；测试只对该类环境性超时重试，并在同一测试二进制内串行化编译，未放宽产品期限。

## 3.5 第五轮补充（P1-01 布局与 P1-02 对象表构建器）

> 标题仍为 P0 是因为本目录是同一阶段的主记录；本节内容属于 P1 的前置实现，**尚未切换 ABI**。

| 项 | 内容 |
| --- | --- |
| V14 root | `StrategyRuntimeContextV14` = V13 前缀（392B，字段与类型不变）+ 48B 尾部（`objects_ptr/len`、`object_schema_minor`、`object_query=NUL`、`object_context=NUL`、保留字段），`size_of == 440`、`objects_ptr` 偏移 392、尾部 48B 全部 const 断言 |
| object ref | `TitanObjectRef` 32B/8 对齐；`flags` 定义 valid/volatile/synthetic；domain 编号 ABI 固定 |
| payloads | `TitanDecimalV1`(16B)、Symbol(152B)、Account(48B)、Currency(32B)、Pair(80B)、Status(40B)、Fee(32B)，逐字段偏移断言；`object_query` 预留为 NULL |
| schema | `object_payload_schemas_v14()` 生成规范描述与 `object_schema_hash_v14()`；payload 不进 root 指纹，后续以跨语言哈希/golden bytes 校验 |
| 对象表 | `ObjectTable::build`：从冻结绑定/元数据快照构建 symbol/account/currency/pair/status/fee；checked 派生 `base_delta_per_lot`、`min_qty_lots`、`min_notional_tick_lots`、整数 lot 比、有理 tick 比、`min_hedge_lots`；拒绝 synthetic/inverse/未知币种/币种编号冲突/非整数比例/缺最小量或最小金额/重复对象/超容量 |
| 身份 | `object_identity_digest` 只覆盖准入字段（单位、最小量、报价/保证金币、保证金域、持仓模式、pair 比例）；fee/status/审计字段与未参与配对的 synthetic 标记不改变摘要；`metadata_snapshot_digest` 单独保存 |
| 内存/线程 | payload arena 使用 `Box<[u64]>`（构建后不再扩容，8B 对齐）；status 为唯一可变对象且仅经 `&mut` 写入；`ObjectTable` 显式 `unsafe impl Send` 并写明单写者前提；Debug 不打印指针 |
| 测试 | v14 布局/schema 4 项、对象表 8 项（派生/顺序/身份覆盖/可变性/拒绝/容量/重复），已在 titan-build 通过 |

本轮门禁：fmt 通过、Clippy `-D warnings` 0 warning；`cargo test --workspace --all-targets --all-features`
**939 passed、0 failed、29 ignored**；doctest 2 passed / 1 ignored。

说明：一次全量运行中 3 项**既有** `recovery_storage` 时序测试在高负载下失败（`recovery_namespace_owned`）；
同一测试二进制单独连续 3 次通过（138 passed），随后全量复跑通过。该现象与本轮改动无关，未修改其时序断言。

**本轮没有切换 ABI**：`STRATEGY_ABI_V13`、`V13_ABI_FINGERPRINT`、loader、SDK 与所有已编译产物均未改变；
`v14`/`ObjectTable` 目前没有运行时消费者，接线属于 P1-03 之后的协调切换。

## 3.6 第六轮补充（P1-03 manifest 解析与两层门禁）

| 项 | 内容 |
| --- | --- |
| 类型 | `ObjectManifest`/`ObjectRequirement`（domain、major、min_minor、min_payload_len、min_count、required_fields）与 `ObjectGateError` 稳定原因码 |
| artifact 层 | `validate_artifact()` 对照宿主编译期常量：schema major/minor、identity version、domain 合法性、payload 长度上限、min_count、字段位图（仅 symbol 定义 12 位，其余必须为 0）、重复 domain |
| instance 层 | `validate_instance(table)`：domain/数量、每个 ref 的 major、minor 与 payload_len、symbol 必需字段位图按实际 payload 判定、`requires_fee` 要求每个账户都有 `valid=1` 的 fee 对象 |
| manifest 解析 | `CompiledStrategyMetadataV13` 增加可选 `object_manifest`（`object_schema_major` 存在才解析，V13 旧产物保持 `None`）；解析 `object_schema_hash`/`object_identity_version`/`requires_fee`/`object_requirements` |
| 能力位 | `object_context` 同时在原生能力位（`1<<11`）与运行时能力位（`1<<13`）定义，并在 manifest 解析与 `manifest_v13` 映射中同步；宿主默认授权集合自动包含该位 |
| 测试 | 门禁 2 项（artifact 拒绝/ instance 证明）与 manifest 解析 2 项（缺失可选、含要求解析并通过/被拒） |
| 未接线 | 运行时 `CompiledStrategyMetadata` 尚未携带 object manifest，`validate_manifest` 尚未调用门禁；ABI、loader、SDK、产物仍未切换 |

本轮门禁：fmt 通过、Clippy `-D warnings` 0 warning、`cargo test --workspace --all-targets --all-features`
**943 passed、0 failed、29 ignored**、doctest 2 passed / 1 ignored；5 个变更文件本地与服务器 SHA-256 一致。

## 3.7 第七轮补充（P1-04 前置：V14 dtype 与跨语言 schema）

| 项 | 内容 |
| --- | --- |
| `abi_v14.py` | 复用 V13 的视图/请求 dtype，追加 `objects_ptr/len`、`object_schema_minor`、`object_query`、`object_context`、`reserved_ext`，context dtype **440B**、object ref **32B** |
| payload dtypes | decimal 16、symbol 152、account 48、currency 32、pair 80、status 40、fee 32，与 Rust `size_of` 一致；`OBJECT_PAYLOADS` 按 domain 1…6 排序并携带各自 major/minor |
| 指纹 | `canonical_abi_descriptor()` 生成 V14 指纹 **187e84ba…f64d**，Rust 侧以 `V14_ABI_FINGERPRINT` 常量固定（切换前 loader 仍用 V13 指纹） |
| 对象 schema | `object_schema_descriptor()/object_schema_bytes()` 与 Rust `object_schema_descriptor_v14()` **逐字节一致**，哈希 **0b7e3d2e…fb14** |
| 双向保护 | `fixtures/abi_v14_descriptor.json`、`fixtures/object_schema_v14.json` 由 Python 生成；Rust 测试断言指纹/描述符相等且 context/object ref 的尾部偏移正确，Python 测试断言 fixture 与当前 dtype 一致 |
| 未实现 | 6 个 accessor、intrinsic 扫描、`_PUBLIC_ACCESSORS`/静态校验、testing 构造 helper 仍是下一步；ABI/loader/SDK 编译入口未切换 |

本轮门禁：fmt 通过、Clippy `-D warnings` 0 warning、`cargo test --workspace --all-targets --all-features`
**945 passed、0 failed、29 ignored**、doctest 2 passed / 1 ignored、SDK **166 项**通过；
5 个变更文件（含 2 个新 fixture）本地与服务器 SHA-256 一致。

## 3.8 第八轮补充（P1-04 accessor 与能力门禁）

| 项 | 内容 |
| --- | --- |
| accessor | `context_v14.py`：`symbol/account_info/currency/pair/status/fee` 六个只读 accessor；读取 refs 数组（长度用 select 保护、校验 flags/major/minor/payload_len）、按 domain/object_id 扫描并返回 payload 记录 |
| 失败语义 | 必需对象缺失/版本过旧/长度不足：返回零记录并写 `last_error_code=-2`；fee 缺失或 `valid=0` 返回零记录且**不置错**，由策略自行处理 |
| 能力门禁 | Python `Capability.OBJECT_CONTEXT`；静态编译器要求使用对象 accessor 的策略声明该能力；V13 宿主在 `validate_manifest` 显式拒绝 `object_context`（原因码 `object_context_abi_mismatch`），避免旧宿主执行读对象表的产物 |
| 原生验证 | `test_v14_objects.py` 用 Numba 直接编译 probe，读取手工构造的 V14 context/refs/payload：符号/账户/币种/pair/status/fee 数值、缺失与版本拒绝、payload 长度、fee 可选性、能力门禁 7 项全部通过 |
| 未完成 | `testing.py` 的场景构造 helper、经 Rust 编译产物的端到端回调测试需在协调切换后完成（当前编译仍用 V13 描述符） |

本轮门禁：fmt 通过、Clippy `-D warnings` 0 warning、`cargo test --workspace --all-targets --all-features`
**946 passed、0 failed、29 ignored**、doctest 2 passed / 1 ignored、SDK **173 项**通过；
5 个变更文件本地与服务器 SHA-256 一致。

## 3.9 第九轮补充（P1-04 TESTS 场景对象构造）

| 项 | 内容 |
| --- | --- |
| `testing.objects(...)` | 场景内声明 V14 对象表：symbols/accounts/currencies/pairs/fees/status；数量上限 64/16/128/16/16；返回 JSON 兼容描述 |
| 构造器 | `_build_object_table` 物化六个 payload 数组、按 `(domain, object_id)` 排序的 refs、状态缓冲；fee 以账户号寻址并允许 `valid=0`，重复账户拒绝 |
| 场景接线 | TESTS scene 新增 `objects`，step 新增 `object_status`（在回调前改写 status 生命周期/标志）；refs/payload 由 keepalive 持有，防止回调期间被回收 |
| 安全门禁 | 带 `objects` 的场景在 V13 harness 下直接报错（要求 V14 ABI），不会静默忽略对象 |
| 测试 | `test_v14_testing.py` 5 项：描述校验/上限、refs 排序与指针有效、状态覆盖、V13 拒绝、重复 fee 拒绝 |
| 未接线 | `cli.py` 仍以 V13 运行 harness；切换时改为传入 `abi_v14` 并补经 Rust 产物的端到端场景 |

本轮门禁：fmt 通过、Clippy `-D warnings` 0 warning、`cargo test --workspace --all-targets --all-features`
**946 passed、0 failed、29 ignored**、doctest 2 passed / 1 ignored、SDK **178 项**通过；
2 个变更文件本地与服务器 SHA-256 一致。

## 3.10 第十轮补充（对象表运行时适配器）

| 项 | 内容 |
| --- | --- |
| 元数据身份修正 | 快照查找新增 `instrument_by_symbol`；`instruments verify` 改用 `(source_key, native_symbol)` 匹配。原先按 `asset_id` 查 `asset_no` 混用了配置与离线 trace 两套编号 |
| 适配器 | `object_sources.rs`：从冻结的市场源/账户定义 + 策略绑定 + multi-binding 生成 `ObjectTableInput`；报价币由账户 `linear_quote_currency`（回退该账户所交易符号的报价币）证明，保证金币暂按报价币并标记待账户事实接入 |
| 权威单位 | 有元数据快照时以快照为准（synthetic=false），无快照时使用已校验绑定并标记 synthetic；缺失源/合约/币种/腿一律 fail-closed |
| pair | 由 multi-binding 的 maker/hedge 腿映射到本地资产号，`pair_revision` 必须落入 u32；比率方向为 maker lots → hedge lots |
| 测试 | 4 项：完整定义可构建（pair 比率 100、revision 3）、无快照的 pair 被拒（`object_admission_unprovable`）、快照权威且覆盖缺失即拒、缺失定义/未知腿 fail-closed |

本轮门禁：fmt 通过、Clippy `-D warnings` 0 warning、`cargo test --workspace --all-targets --all-features`
**950 passed、0 failed、29 ignored**、doctest 2 passed / 1 ignored、SDK **178 项**通过；
4 个变更文件本地与服务器 SHA-256 一致。

## 3.11 第十一轮补充（V14 回调上下文构建器与切换清单）

| 项 | 内容 |
| --- | --- |
| 上下文构建器 | `v14::callback_context_v14(now, schema, CallbackViewsV14, &ObjectTable)`：只写指针/长度，填 `objects_ptr/len`、`object_schema_minor`；`object_query` 保持 None、`reserved_*` 为 0；V13 前缀偏移与语义不变 |
| 视图参数 | `CallbackViewsV14` 聚合五个视图切片，避免长参数列表并保证回调期零拷贝 |
| 切换清单 | 新增 `docs/strategy_abi_v14_switch_checklist.md`：原子性/回滚、同批次变更清单、执行顺序、五个固定同步点、25 项矩阵证据落点、禁止事项 |
| 约束 | 构建器标注 dead-code 允许并注明由切换接线；门禁常量改为引用 `OBJECT_SCHEMA_MAJOR/MINOR`，避免两处漂移 |
| 测试 | `v14_callback_context_carries_the_frozen_table_pointer`：指针/长度/schema minor/保留字段/440B 全部断言 |

本轮门禁：fmt 通过、Clippy `-D warnings` 0 warning、`cargo test --workspace --all-targets --all-features`
**951 passed、0 failed、29 ignored**、doctest 2 passed / 1 ignored、SDK **178 项**通过；
2 个变更文件本地与服务器 SHA-256 一致。

## 3.12 第十二轮补充（V14 describe 就绪）

| 项 | 内容 |
| --- | --- |
| 描述符校验 | `abi_v14.validate_abi_descriptor`：校验 ABI 14、指纹，以及可选的 `object_schema_hash`（与 Python canonical 值一致，防止两侧 payload 布局漂移） |
| ABI 分派 | `static_compiler.runtime_abi_module`：注入 14 → 用 v14 校验/布局；否则保持 v13，现有编译路径与产物不受影响 |
| describe 输出 | 传入 v14 描述符时输出 `object_schema_major/minor/hash`、`object_identity_version`、`requires_fee`（由 `fee` accessor 使用决定）、`object_requirements`（symbol 核心字段位图 0xF0F、account、currency 必有，pair 仅在策略使用 pair accessor 时声明） |
| 兼容性 | 默认（v13）describe 输出**完全不变**，测试断言无对象字段；`cli.py` 把注入 ABI 同时传给 describe 与原生测试 harness |
| 测试 | `test_v14_descriptor_reports_object_requirements`：v14 字段/域名/位图/长度，以及 v13 输出无对象键 |

本轮门禁：fmt 通过、Clippy `-D warnings` 0 warning、`cargo test --workspace --all-targets --all-features`
**951 passed、0 failed、29 ignored**、SDK **179 项**（+1）通过；4 个变更文件本地与服务器 SHA-256 一致。

## 3.13 第十三轮补充（对象 manifest 数据通路）

| 项 | 内容 |
| --- | --- |
| 桥接结构 | `NumbaMetadataV13` 增加可选 `object_schema_major/minor/hash`、`object_identity_version`、`requires_fee`、`object_requirements`；缺省时 JSON/CBOR 输出与旧版一致 |
| 校验 | 对象字段“全有或全无”；schema major/minor 必须受支持，requirement 的 major/minor/min_payload_len/min_count 必须自洽；孤立 requirements/requires_fee 被拒 |
| manifest | `write_compiled_strategy_v13` 在签名前写入对象字段；`parse_manifest` 已能解析为 `ObjectManifest` |
| 运行时元数据 | `CompiledStrategyMetadata` 增加 `object_manifest`；loader 映射；`validate_manifest` 先跑 artifact 层门禁，再对“V13 root + 对象要求”显式拒绝（`object_context_abi_mismatch`） |
| 测试 | manifest 字段完整性/缺字段拒绝；V13 root 拒绝对象 manifest；未知 domain 先被 artifact 门禁拒绝（`object_requirement_invalid`） |
| 未切换 | 注入仍为 ABI 13，Python 默认不输出对象字段；常量/root/loader 仍未切 V14 |

本轮门禁：fmt 通过、Clippy `-D warnings` 0 warning、`cargo test --workspace --all-targets --all-features`
**953 passed、0 failed、29 ignored**、doctest 2 passed / 1 ignored、SDK **179 项**通过；
6 个变更文件（含 connector 示例）本地与服务器 SHA-256 一致。

## 3.14 第十四轮补充（ABI V14 切换核心）

> 本轮完成切换的“核心”部分：live ABI、loader、descriptor、manifest、Python 桥接与运行时上下文同时到 V14，
> 全量测试在同一源码版本上通过。对象表的服务层接线、`runtime_type` 迁移、产物/脚本与 25 项矩阵仍待完成。

| 项 | 内容 |
| --- | --- |
| 常量 | `STRATEGY_ABI = 14`、`ABI_FINGERPRINT = V14`；V13 常量与指纹保留为历史/负例测试常量 |
| loader/descriptor | manifest 与 native descriptor 写 ABI 14 + V14 指纹；`NumbaMetadataV13::validate`/loader 全部按 14 校验；metadata 暴露 `abi_version` 供 `strategy show` 报告 |
| 运行时 root | `CallbackRegistry`/`StrategyInstance`/staging/`invoke` 全部改用 440B `StrategyRuntimeContextV14`；`struct_size` 写 440；实例新增 `ObjectTable` 槽位（暂为空表，服务层接线待做） |
| manifest | 对象字段随签名写入；`object_context` 能力要求对象 requirements（`object_context_without_requirements`）；artifact 层门禁在加载期执行 |
| Python 桥接 | `_bridge` 的 context size/offset/ABI 版本改由实际 ABI 模块决定；`cli.py` 的 describe/test-native 元数据统一注入 V14；V13 校验路径仍保留供历史测试 |
| SDK 测试 | 原生回调测试改用 V14 context；`strategy show` 的 ABI 由 manifest 实际值报告 |
| 边界 | 对象表尚未由服务构建/挂载（accessor 在真实运行中会返回零记录并置 -2）；`runtime_type` 仍为 `native-v13`；仓库产物/fixtures 未重编 |

本轮门禁：fmt 通过、Clippy `-D warnings` 0 warning、`cargo test --workspace --all-targets --all-features`
**953 passed、0 failed、29 ignored**、doctest 2 passed / 1 ignored、SDK **179 项**通过；
13 个核心变更文件本地与服务器 SHA-256 一致。

## 3.15 第十五轮补充（P1-09 loader_type 迁移）

| 项 | 内容 |
| --- | --- |
| 重命名 | `native-v13` → `native-v14`：loader 工厂、manifest `runtime_type`、workspace 写入、示例/测试配置、`configs/cross_exchange_mm.example.toml`、`deploy/pair_arb_v13/runtime.toml`（28 个文件） |
| 旧配置拒绝 | 运行时 `validate_definition` 对 `native-v13` 返回稳定原因码 `loader_type_migrated`，消息包含“重新编译并设置 native-v14”；配置层错误同样附迁移提示 |
| 默认授权 | `StrategyCoreConfig::default().allowed_loader_types` 现为 `native-v14` |
| 测试 | 迁移拒绝与默认授权断言；全量既有测试同步新名称 |
| 边界 | 未提供自动迁移命令（只报错+提示）；发现 `crates/titan-cli/src/service_core.rs` 为未被任何模块引用的历史副本，其门禁不生效，真实门禁在 `titan-strategy-runtime` 的 `service_core` |

本轮门禁：fmt 通过、Clippy `-D warnings` 0 warning、`cargo test --workspace --all-targets --all-features`
**954 passed、0 failed、29 ignored**、doctest 2 passed / 1 ignored、SDK **179 项**通过；
6 个变更文件本地与服务器 SHA-256 一致。

## 3.16 第十六轮补充（对象表服务层接线）

| 项 | 内容 |
| --- | --- |
| 运行时接线 | `StrategyRuntimeBuildContext` 新增 `object_table: Option<ObjectTable>`；工厂创建实例时挂载并在**提供表**时执行 manifest requirements 的 instance 层门禁 |
| 无快照策略 | 未配置元数据快照时实例使用空表且跳过 instance 门禁：accessor 返回零记录并置 -2；这是发布前必须消除的临时态，已记录为剩余阻塞 |
| 定义访问 | `MarketService::definition(handle)`、`AccountService::definition(handle)` 默认拒绝，Core 实现从注册表返回冻结定义 |
| 服务构建 | `StrategyServiceDependencies.metadata_snapshot` 存在时，按实例绑定收集定义/账户，走 `object_sources` + `ObjectTable::build` 构建表；错误映射为稳定原因码 |
| 线程安全 | `ObjectTable` 增加 `unsafe impl Sync`，依据：构建后 arena/payload 不可变，唯一可变的 status 仅经 `&mut` 访问 |
| 构造器 | 17 处 build-context、5 处依赖字面量与 connector 示例同步新字段 |
| 验证边界 | 服务层端到端（配置快照→构建→accessor 读值）尚无自动化用例；现有证据为 object_sources 单测 + 全量编译/测试 |

本轮门禁：fmt 通过、Clippy `-D warnings` 0 warning、`cargo test --workspace --all-targets --all-features`
**954 passed、0 failed、29 ignored**、doctest 2 passed / 1 ignored、SDK **179 项**通过；
9 个变更文件本地与服务器 SHA-256 一致。

## 3.17 第十七轮补充（启动元数据快照入口）

| 项 | 内容 |
| --- | --- |
| 配置 | `[strategy_service] metadata_snapshot = "path"`：相对路径按运行配置目录解析；未配置时维持空表临时态 |
| 加载校验 | 读取 JSON → 反序列化 `MetadataSnapshot` → 校验 `digest_matches()` 与 `schema_version == 1`；任一不满足拒绝启动 |
| 传递 | `StrategyBootstrap.metadata_snapshot` → `StrategyServiceDependencies.metadata_snapshot` → 逐实例建表（第 16 轮接线） |
| 测试 | 合法快照加载；篡改内容（不改 digest）被拒；未来 schema 被拒 |
| 边界 | 交易所启动期拉取（D5 的在线路径）尚未实现；当前入口接受离线/受控快照文件，未配置快照时为空表临时态 |

本轮门禁：fmt 通过、Clippy `-D warnings` 0 warning、`cargo test --workspace --all-targets --all-features`
**955 passed、0 failed、29 ignored**（+1）、doctest 2 passed / 1 ignored、SDK **179 项**通过；
`core_runtime.rs` 本地与服务器 SHA-256 一致。

## 3.18 第十八轮补充（真实产物端到端读取对象）

| 项 | 内容 |
| --- | --- |
| 场景 | 新 fixture `v14_object_reader.py`：真实编译的策略在 `on_tick` 中读取 `symbol/account_info/currency/status`，写入 state |
| 编译链 | `titan strategy compile` 在编译期执行内联 TESTS（`testing.objects` 构造对象表），断言 price tick、base delta、min qty、min notional、报价币、账户、amount_unit、lifecycle 全部等于对象表值 |
| 修复 | `static_compiler` 未导入 `context_v14`，导致 V14 accessor 未注册（`Unknown attribute 'symbol'`）；改为显式副作用导入 |
| 证据 | 编译返回 `abi_version=14`、`tests.declared>=1` 且 `tests.passed==declared`；对应矩阵第 2 项（对象可读）正例 |
| 边界 | 负例（required_fields 缺失仍加载→拒绝）由门禁单测覆盖，尚未用真实产物跑 |

本轮门禁：fmt 通过、Clippy `-D warnings` 0 warning、`cargo test --workspace --all-targets --all-features`
**955 passed、0 failed、29 ignored**、doctest 2 / 1、SDK **180 项**（+1）通过；
3 个变更文件本地与服务器 SHA-256 一致。

## 3.19 第十九轮补充（元数据 provider 注入与失败关闭）

| 项 | 内容 |
| --- | --- |
| 抽象 | 运行时新增 `MetadataSnapshotProvider { fn snapshot() -> Result<Arc<MetadataSnapshot>, String> }`；`StrategyServiceDependencies.metadata_provider` 优先于直接快照 |
| 失败语义 | provider 失败在创建实例时映射为 `metadata_snapshot_unavailable`，启动拒绝；不回退到猜测单位 |
| CLI 实现 | `FileMetadataSnapshotProvider`：每次读取都重新校验 JSON/digest/schema；配置适配期先校验一次以尽早失败，运行期由 provider 再读 |
| 测试 | provider 合法快照成功；内容损坏与文件缺失均报错；配置期 digest/schema 负例保持 |
| 边界 | 交易所在线拉取实现仍缺位；provider 是后续 connector 元数据源的注入点，当前只有文件实现 |

本轮门禁：fmt 通过、Clippy `-D warnings` 0 warning、`cargo test --workspace --all-targets --all-features`
**956 passed、0 failed、29 ignored**（+1）、doctest 2 / 1、SDK **180 项**通过；
2 个变更文件本地与服务器 SHA-256 一致。

## 3.20 第二十轮补充（对象能力失败关闭与矩阵执行表）

| 项 | 内容 |
| --- | --- |
| 失败关闭 | 声明 `object_context` 能力但实例没有元数据快照/对象表时，运行时创建直接拒绝（`object_context_requires_metadata`），不再静默用空表运行 |
| 清理 | 删除未被任何模块引用的历史副本 `crates/titan-cli/src/service_core.rs`（其门禁从未生效，真实实现位于 `titan-strategy-runtime`） |
| 矩阵 | 新增 `v14-acceptance-matrix.md`：25 项逐项标注 ✅/🟡/⬜ 与证据/缺口，当前 ✅8、🟡11、⬜6，明确不可全绿发布 |
| 边界 | 矩阵 6 项未执行（跨场所、在线拉取、回放一致、零分配、hftbacktest 退役、发布重签） |

本轮门禁：fmt 通过、Clippy `-D warnings` 0 warning、`cargo test --workspace --all-targets --all-features`
**956 passed、0 failed、29 ignored**、doctest 2 / 1、SDK **180 项**通过；
运行时改动文件本地与服务器 SHA-256 一致；删除文件两侧均确认不存在。

## 3.21 第二十一轮补充（矩阵 1/8/19/24 关闭）

| 项 | 内容 |
| --- | --- |
| 第 1 项 | 新增 ABI 13 manifest 拒绝用例：返回 `AbiVersion{13,14}` 且文案含 recompile |
| 第 8 项 | payload minor：低于要求/缺字段拒绝；高于要求按追加式前缀读取（SDK accessor 测试） |
| 第 19 项 | accessor 增加 NULL/8B 对齐校验；截断、低版本、NULL、未对齐四类负例均返回零记录并置 -2 |
| 第 24 项 | 钉住 `direct-order` 前缀/ID、`orders-list` 校验和、`public-state` 身份固定向量 |
| 矩阵 | 更新为 ✅11 / 🟡8 / ⬜6 |

本轮门禁：fmt 通过、Clippy `-D warnings` 0 warning、`cargo test --workspace --all-targets --all-features`
**959 passed、0 failed、29 ignored**（+3）、doctest 2 / 1、SDK **182 项**（+2）通过；
4 个变更文件本地与服务器 SHA-256 一致。

## 3.22 第二十二轮补充（恢复身份与 fee 端到端）

| 项 | 内容 |
| --- | --- |
| 恢复身份 | `StrategyInstanceConfigV13.object_identity_digest`；`binding_digest` 覆盖对象准入身份；恢复对 `binding_digest` 的比较因此拒绝单位变化后的旧 checkpoint |
| 测试 | `binding_digest_covers_the_object_identity`：身份变化 → 摘要变化、同输入稳定、快照 checksum 有效 |
| fee 端到端 | `v14_object_reader` 真实产物回调读取 `fee["maker_fee_bps"]/flags`，编译期 TESTS 断言 -3/1 通过 |
| 矩阵 | 第 10、12 项转 ✅；当前 ✅13 / 🟡6 / ⬜6 |

本轮门禁：fmt 通过、Clippy `-D warnings` 0 warning、`cargo test --workspace --all-targets --all-features`
**960 passed、0 failed、29 ignored**（+1）、doctest 2 / 1、SDK **182 项**通过；
4 个变更文件本地与服务器 SHA-256 一致。

## 3.23 第二十三轮补充（trace schema 3 与 status 接线）

| 项 | 内容 |
| --- | --- |
| trace schema 3 | `V13Trace` 增加必需 `runtime_abi{abi_version, abi_fingerprint, object_schema_hash}`；加载时与宿主 ABI/指纹/对象 schema 哈希比对，不符即拒绝；`input_manifest` 记录 ABI 指纹与 schema 哈希 |
| fixtures | 三个 trace 升级为 schema 3，写入 V14 指纹与对象 schema 哈希；旧 schema 1/2 明确拒绝 |
| status | 每次回调前写 lifecycle/entry_flags（paused/gate/stopping），回调后写 last_return_code/last_error_code；空表用 `try_status_mut` 安全跳过 |
| 测试 | `status_lifecycle_mapping_is_frozen`；CLI 集成测试 + SDK 回放测试随 schema 3 更新 |
| 矩阵 | 第 17 项 ✅；第 11 项仍 🟡（缺逐回调滞后语义端到端）；当前 ✅14 / 🟡5 / ⬜6 |

本轮门禁：fmt 通过、Clippy `-D warnings` 0 warning、`cargo test --workspace --all-targets --all-features`
**961 passed、0 failed、29 ignored**（+1）、doctest 2 / 1、SDK **182 项**通过；
8 个变更文件本地与服务器 SHA-256 一致。

## 3.24 第二十四轮补充（P2-01：cross_exchange_mm 读对象表）

| 项 | 内容 |
| --- | --- |
| 参数删除 | `cross_exchange_mm` 移除 `hedge_lots_per_maker_lot`、`hedge_min_order_lots`、`reference_tick_ratio`、`maker_fee_bps`、`hedge_fee_bps`、`hedge_min_notional_tick_lots` 六个交易所事实参数；`source_version` 2.0.2 → 2.0.3 |
| 对象读取 | `on_start` 从 `ctx.pair(0)`、`ctx.symbol()`、`ctx.fee()` 读取并派生 ratio/min notional（quote/(tick·lot)）/费率；fee 缺失按 `valid=0` 取 0 |
| 能力 | 声明 `OBJECT_CONTEXT`，由静态编译器强制 |
| 关系校验 | 仓位/敞口上限检查从 `build` 参数移到 `on_start`（依赖对象比例），错误码 103/104/105 |
| 测试 | SDK Context 支持 `testing.objects` 构造 pair/symbol/fee/account/currency 并挂入原生上下文；`max_unhedged_lots` 负例改为断言 `error=105`；其余 41 项测试通过 |
| 已知缺口 | 离线 trace 不含账户/币种定义，`test_cli_trace_hedges_and_rejects_incomplete_stop` 以明确理由跳过（离线对象表接线待做） |

本轮门禁：fmt 通过、Clippy `-D warnings` 0 warning、全量 Rust **961 passed / 0 failed / 29 ignored**、
SDK **183 项**（含 1 项明确跳过）；2 个变更文件本地与服务器 SHA-256 一致。

## 3.25 第二十五轮补充（P2-02：pair_arb 比例改读对象）

| 项 | 内容 |
| --- | --- |
| 参数删除 | `pair_arb` 移除 `hedge_ratio_numerator`/`hedge_ratio_denominator`（全仓 grep 为 0） |
| 对象读取 | `on_start` 从 `ctx.pair(0)` 校验 资产/账户映射与比例，写入 `hedge_ratio_num`；不匹配或比例非正 → `fail_closed(308)` |
| 能力 | 声明 `OBJECT_CONTEXT` |
| 测试 | SDK 上下文提供 `ctx.pair()`；`fresh_state`/`Context` 预置派生比例；容量用例改为“对象比例 2 → 容量 5”；17 项全通过 |
| 过程修正 | 首次插入位置误落在 `refresh_readiness`，已移回 `on_start` |

本轮门禁：fmt 通过、Clippy `-D warnings` 0 warning、全量 Rust **961 passed / 0 failed / 29 ignored**、
SDK **183 项**（含 1 项明确跳过）；2 个变更文件本地与服务器 SHA-256 一致。

## 3.26 第二十六轮补充（P2-02：grid 报价单位改读对象）

| 项 | 内容 |
| --- | --- |
| 参数删除 | `grid` 移除 `quote_units_per_tick_lot`（策略内唯一的交易所事实参数） |
| 对象读取 | `on_start` 校验 `symbol` 的资产/报价币映射，按 `price_tick × quantity_lot × contract_multiplier` 用 Decimal 计算每 tick-lot 的报价单位 |
| 构建期 | `build` 只为参数中存在的字段赋值，派生字段留给 `on_start` |
| 能力 | 声明 `OBJECT_CONTEXT` |
| 测试 | SDK 上下文 `ctx.symbol()`、嵌套 decimal 行构造、内联 TESTS 携带对象表；75 项 grid 测试通过；离线 trace 回放以明确理由跳过 |
| 矩阵影响 | 第 4 项“策略无交易所数字”参数侧已达成（cross_exchange_mm + grid + pair_arb 均无交易所事实参数） |

本轮门禁：fmt 通过、Clippy `-D warnings` 0 warning、全量 Rust **961 passed / 0 failed / 29 ignored**、
SDK **183 项**（含 2 项明确跳过）；4 个变更文件本地与服务器 SHA-256 一致。

## 3.27 第二十七/二十八轮补充（离线对象表接线：部分完成）

| 项 | 内容 |
| --- | --- |
| 已完成 | `OfflineV13Config.object_table`：离线适配器与在线使用同一 `ObjectTable`，挂载到实例；`last_error_code()` 暴露 ABI 错误码用于诊断 |
| trace schema 4 | 新增必需 `accounts`（账户定义）与 `account_instruments`（账户级合约事实）；`offline_object_table()` 用线上同一构建器生成表；三个 fixtures 升级 |
| 夹具修正 | 账户 `ownership`/`shutdown_order_policy` 使用正确枚举名；账户 `instruments` 补 `linear_quote_currency`；账户/资产编号与策略绑定对齐 |
| 未解决 | 离线 `on_start` 仍返回 -4 且 `last_error_code=-2`，而同一对象表在原生 harness 中可正常启动；差异尚未定位，两个 CLI 回放用例保持显式跳过（skip 理由已更新） |
| 影响 | 发布阻断项“离线对象表接线”仍开放；矩阵第 7/13 项维持 🟡/⬜ |

本轮门禁：fmt 通过、Clippy `-D warnings` 0 warning、全量 Rust **961 passed / 0 failed / 29 ignored**、
doctest 2 / 1、SDK **183 项**（含 2 项显式跳过）；6 个变更文件本地与服务器 SHA-256 一致。

## 3.28 第二十九轮补充（离线 -2 诊断进展）

| 项 | 内容 |
| --- | --- |
| 已排除 | 逐字段导出离线上下文：`abi=14`、`struct_size=440`、state 长度/对齐/schema/hash 均正确、`objects_ptr` 非空且 4 个 ref；视图条数与 trace 一致 |
| 结论 | 上下文封装不是原因；差异在**策略侧读取视图**。离线 trace 的事件流没有 `start` 步，而 `grid` 在 `on_start` 内报价，因此缺失视图的 `-2` 来自未被补全的视图缓冲（账户/余额/持仓视图在首事件前为空） |
| 代码 | 离线 `invoke` 的视图借用显式别名（无行为变化），便于后续插入诊断；调试写文件代码已全部移除 |
| 测试 | 两个回放用例保持**显式跳过**并写明“需在 trace 中加入显式 start 步”的前置条件 |

本轮门禁：fmt 通过、Clippy `-D warnings` 0 warning、全量 Rust **961 passed / 0 failed / 29 ignored**、
doctest 2 / 1、SDK **183 项**（含 2 项显式跳过）；3 个变更文件本地与服务器 SHA-256 一致。

## 3.29 第三十轮补充（离线回放：验证与明确跳过）

| 项 | 内容 |
| --- | --- |
| 尝试 | 移除 grid 回放用例的跳过注解，确认失败仍复现（`Start returned -4`、`last_error_code=-2`），排除“跳过掩盖了已修复问题” |
| 结论不变 | 离线 `start()` 前后上下文字段全部正确（abi/size/state/objects/视图长度），因此 -2 来自策略侧对某视图的读取，但离线与原生 harness 的差异无法在两轮内定位 |
| 处理 | 回放用例恢复**显式跳过**，skip 理由更新为“需专门的离线 on_start 诊断”；未放松任何断言，未改动产品语义 |
| 遗留 | 发布阻断项“离线对象表端到端”仍开放（矩阵第 7/13 项） |

本轮门禁：fmt 通过、Clippy `-D warnings` 0 warning、全量 Rust **961 passed / 0 failed / 29 ignored**、
SDK **183 项**（含 2 项显式跳过）；2 个变更文件本地与服务器 SHA-256 一致。

## 3.30 第三十一轮补充（离线 -2 逐步定位）

| 步骤 | 结论 |
| --- | --- |
| 步骤标记 | 在 `grid.on_start` 逐步写入未使用字段并读取：31 进入、32 账户、33 持仓、34 余额、35/36 `symbol`、37 单位计算、**48 函数末尾** |
| 关键发现 | `on_start` **执行到末尾并返回 0**；`-2` 不在 `on_start` 内部，而在其后的桥接检查或后续回调 |
| 对象数值 | `symbol` 读取后写入的十进制向量为 0，说明离线下该 payload 内容与原生 harness 不一致（原生 harness 中同一读取得到 1/1/1） |
| 清理 | 策略与 CLI 的全部诊断代码已移除；仅保留 `OfflineV13Adapter::state_i64`（有文档、无副作用，供今后离线诊断使用） |
| 现状 | 两个回放用例仍显式跳过；矩阵第 7/13 项维持未完成 |

本轮门禁：fmt 通过、Clippy `-D warnings` 0 warning、全量 Rust **961 passed / 0 failed / 29 ignored**、
SDK **183 项**（2 项显式跳过）；3 个变更文件本地与服务器 SHA-256 一致。

## 3.31 第三十二轮补充（离线对象 payload 取证）

| 项 | 内容 |
| --- | --- |
| 取证 | 在离线运行前对四个对象 payload 取 SHA-256：symbol `ba7ffdeb…`、account `053ece4e…`、currency `84bf6f50…`、status `66d22b66…`，长度分别为 152/48/32/40 |
| 复现尝试 | 用 fixture 输入在本地逐字段重建 symbol payload，枚举 valuation/flags/currency 编号/instrument_ref/min 字段等组合，未能复现同一摘要 |
| 结论 | 离线构建的 payload 内容与按 fixture 推导的值不一致——即**离线 side 的输入或派生与在线/fixture 不一致**，这正是策略读到 0 的原因 |
| 现状 | 取证代码已移除；两个回放用例仍显式跳过；矩阵第 7/13 项维持未完成 |
| 下一步 | 离线 `payload()` 不再被策略消费：优先让离线用**与在线相同的适配器输入**（直接把 trace 的 `accounts`/`account_instruments` 转成 `ObjectTableSources` 而不重新推导），并以此为唯一构建路径 |

本轮门禁：fmt 通过、Clippy `-D warnings` 0 warning、全量 Rust **961 passed / 0 failed / 29 ignored**、
SDK **183 项**（2 项显式跳过）；2 个文件本地与服务器 SHA-256 一致。

## 3.32 第三十三轮补充（矩阵第 4 项 + 离线回放降级）

| 项 | 内容 |
| --- | --- |
| 决策 | 依用户 2026-11-14 决定，命令行离线回放**降级为已知限制**，不再阻塞交付；STATUS 与矩阵同步标注 |
| 新增守卫 | `scripts/check_strategy_no_exchange_facts.py`：名称级检查策略参数不得含交易所事实（合约乘数/tick/lot/min notional/费率/腿间比/报价单位等），仓库模式只看参数块，显式文件全量扫描 |
| 测试 | `test_strategy_exchange_facts.py` 2 项：仓库 3 个策略通过；合成反例 `maker_fee_bps` 被拒（退出码 1 且报出参数名） |
| 矩阵 | 第 4 项转 ✅；当前 ✅15 / 🟡5 / ⬜4（第 7/13 为已知限制） |

本轮门禁：fmt 通过、Clippy `-D warnings` 0 warning、全量 Rust **961 passed / 0 failed / 29 ignored**、
SDK **185 项**（2 项显式跳过）；2 个变更文件本地与服务器 SHA-256 一致。

## 3.33 第三十四轮补充（P3-01：hftbacktest strategy 模块退役）

| 项 | 内容 |
| --- | --- |
| 删除 | `hftbacktest/src/strategy.rs`（854 行、6 个单测）；`lib.rs` 的 `pub mod strategy` 与 `prelude.rs` 的 `strategy::*` 导出、遗留 doc 注释一并移除 |
| 守卫 | 新增 `scripts/check_no_retired_hftbacktest_strategy.sh`：限定 grep `hftbacktest::strategy`/`prelude::run_strategy`/`run_strategy_for`/`StrategyCtx`，并拒绝 `strategy.rs` 复现 |
| 单运行守卫 | `scripts/check_single_runtime.sh` 改为“V13 前缀 + V14 root 恰好两个定义”，legacy 正则不再误报文档字符串 |
| 文档 | `docs/rust_strategy.md` 改写为退役说明与迁移指引；`docs/bar_tick_numba_strategy.md` 删除过时的 `StrategyCtx` 现行描述 |
| 矩阵 | 第 15 项 ✅；当前 ✅16 / 🟡5 / ⬜3（第 7/13 为已知限制） |

本轮门禁：三个守卫脚本全部通过；fmt 通过、Clippy `-D warnings` 0 warning、workspace+examples 构建通过、
全量 Rust **955 passed / 0 failed / 29 ignored**（删除 6 个被退役的单测）、SDK **185 项**（2 项显式跳过）。

## 3.34 第三十五轮补充（P3-02：弃用占位单一写入点）

| 项 | 内容 |
| --- | --- |
| 常量 | 新增 `DEPRECATED_UNIT_PLACEHOLDER: i64 = 1`，`TitanMarketView.tick_size/lot_size` 的**唯一写入点**使用它；注释明确“非交易所事实” |
| 文档 | Rust 视图定义与 `abi_v13.market_dtype` 均标注 deprecated：真实单位只能来自 `ctx.symbol` |
| 不变量测试 | `deprecated_market_unit_placeholders_stay_constant`：常量恒为 1、默认视图为 0、`TitanMarketView` 偏移 40/48/56 与总长 64 冻结 |
| 边界 | 删除字段需要另一次 major（设计 §5 已声明），本轮只消除“双重解释”风险；`reserved` 语义与 `v13` 模块改名/删除仍未做 |

本轮门禁：fmt 通过、Clippy `-D warnings` 0 warning、全量 Rust **956 passed / 0 failed / 29 ignored**、
SDK **185 项**（2 项显式跳过）、单运行与退役守卫通过；3 个变更文件本地与服务器 SHA-256 一致。

## 3.35 第三十六轮补充（矩阵第 14 项：对象访问零分配）

| 项 | 内容 |
| --- | --- |
| 分配计数 | 把 `observation.rs` 内的测试专用 `CountingAllocator` 与线程局部计数器提取到 `crates/titan-strategy-runtime/src/test_allocations.rs`（整个 crate 只能有一个 `#[global_allocator]`），业务观测探针改为引用它 |
| 对象探针 | `object_table::hot_path_probe`：镜像生成代码的 `ctx.symbol(0)` 扫描（含 valid/major/minor/非空/8B 对齐/长度全部守卫）与 `table.fee(0)` 读取 |
| 结果 | 预热后 1000 轮（每轮 1 次扫描 + 2 次 fee 读）分配计数恒为 **0**；price tick 读回 1/1、fee 读回 -2 |
| 矩阵 | 第 14 项 ✅；当前 ✅17 / 🟡5 / ⬜2 |

本轮门禁：fmt 通过、Clippy `-D warnings` 0 warning、全量 Rust **957 passed / 0 failed / 29 ignored**、
SDK **185 项**（2 项显式跳过）；4 个变更文件本地与服务器 SHA-256 一致。

## 3.36 第三十七轮补充（P3-03：文档与记忆同步）

| 项 | 内容 |
| --- | --- |
| 工程记忆 | 新增 §28“V14 切换与离线回放的工程教训”：同批切换、单一 `#[global_allocator]`、离线/在线差异需单独验证、删模块要连带改守卫、参数守卫按用途分类；移除末尾过期的“仍待 P0-08…P0-11”旧结论 |
| 观测协议 | `strategy_observation_protocol.md` 改为 V14 口径（392B 前缀 + 440B root、payload 同 major 追加、`OBJECT_SCHEMA_HASH` 双向校验），并更正“旧 ABI 文件仍可读”的过时描述 |
| 历史文档 | `strategy_abi_v13_typed_state_blob_technical_design.md`、`strategy_abi_v13_implementation_status.md`、`strategy_abi_v13_post_refactor_audit.md` 均加“历史记录（V13 时期）”横幅并指向 V14 契约与状态页 |
| SDK README | 补对象 accessor 表格、`OBJECT_CONTEXT` 能力要求、`-2` 失败语义、`native-v14` loader 与交易所事实参数禁令 |
| 未做 | 发布说明（release notes）依赖产物重签，暂缓 |

本轮门禁：fmt 通过、Clippy `-D warnings` 0 warning、全量 Rust **957 passed / 0 failed / 29 ignored**、
SDK **185 项**（2 项显式跳过）、隐私检查通过；6 个文档文件本地与服务器 SHA-256 一致。

## 3.37 第三十八轮补充（P2-03：运行诊断暴露对象身份）

| 项 | 内容 |
| --- | --- |
| 实例访问器 | `StrategyInstanceV13` 新增 `object_identity_digest()` 与 `metadata_snapshot_digest()`，直接取自挂载的对象表 |
| 诊断快照 | `StrategyRuntimeDiagnosticSnapshot` 增加 `object_identity_digest`/`metadata_snapshot_digest` 两个字段，summary 更正为 `native V14 strategy runtime` |
| 口径一致 | 管理端与 `core-run` 通过同一诊断快照看到同一组摘要；单位校验仍只走 `validate_instrument_units*` 单一实现（`instruments verify`/core-run/preflight 共用） |
| 测试 | `operator_digests_track_units_but_not_the_snapshot_digest`：等价小数归一化后身份相同、tick 变化则身份变化、快照摘要单独变化不影响身份 |
| 边界 | 管理端 HTTP 层的字段透传未新增专用端点；脱敏输出沿用既有封装（UUID/摘要） |

本轮门禁：fmt 通过、Clippy `-D warnings` 0 warning、全量 Rust **958 passed / 0 failed / 29 ignored**、
SDK **185 项**（2 项显式跳过）；4 个变更文件本地与服务器 SHA-256 一致。

## 3.38 第三十九轮补充（矩阵第 2 项：requirement 负例）

| 项 | 内容 |
| --- | --- |
| 新增 | `test_v14_requirement_negative.py` 5 项：core 位集不含未定义位且不强制最小量/最小金额；manifest 报告的 requirement 与编译器一致；跨 ABI 描述符被拒；requirement 覆盖 symbol/account/currency；实例层 `object_field_missing` 契约两侧对齐 |
| 口径修正 | 验证发现 `CORE_SYMBOL_FIELDS` **不含**最小量/最小金额位（保守集合：单位 + 报价/保证金币 + valuation + tradable），因此测试不再假设“最小量必选”，改为断言“位集合法且最小量不强制” |
| 矩阵 | 第 2 项 ✅；当前 ✅18 / 🟡4 / ⬜2（第 7/13 为已知限制） |
| 环境 | 期间构建服务器短暂不可达一次（`Host is down`），重试后恢复，未影响结果 |

本轮门禁：fmt 通过、Clippy `-D warnings` 0 warning、全量 Rust **958 passed / 0 failed / 29 ignored**、
SDK **190 项**（+5，2 项显式跳过）；1 个变更文件本地与服务器 SHA-256 一致。

## 3.39 第四十轮补充（矩阵第 11 项 + 空表读取崩溃修复）

| 项 | 内容 |
| --- | --- |
| **缺陷** | 新增探针时发现真实缺陷：`ObjectTable::status()` 对**空表**（未挂载对象表之前的默认实例）读零长度 arena，触发 SIGSEGV；整个 `titan-strategy-runtime` 测试二进制因此崩溃 |
| 修复 | `status()` 在 `status` arena 为空时返回 `TitanStatusPayloadV1::default()`，与 `try_status_mut()` 的语义对齐 |
| 探针 | `status_publication_probe` 2 项：入口态（lifecycle/entry_flags）在回调前置、结果（last_return/last_error）在回调后置，第二次回调期间 `last_*` 仍描述上一次；空表无 status 对象且读取返回零值 |
| 矩阵 | 第 11 项 ✅；当前 ✅19 / 🟡3 / ⬜2（第 7/13 为已知限制） |
| 未采纳 | 曾尝试新增探测构造器与 `write_probe_bytes`，因引入 `Library` 依赖与未使用告警而回退，只保留必要的最小改动 |

本轮门禁：fmt 通过、Clippy `-D warnings` 0 warning、全量 Rust **960 passed / 0 failed / 29 ignored**、
SDK **190 项**（2 项显式跳过）；变更文件本地与服务器 SHA-256 一致。

## 3.40 第四十一轮补充（P0-12…P0-15：结项审计发现的四处缺口）

| 项 | 内容 |
| --- | --- |
| P0-12 守卫对齐 trace schema 4 | `scripts/verify_cli_bundle.py` 内联 trace 由 `schema_version=1` 改为 `schema_version=4`，新增 `trace_schema_4()`（内嵌 `runtime_abi`，取自 `fixtures/grid_v13_trace.json`；metadata 快照含按 `titan-market-service` canonical 规则现算的 digest；`accounts`、`account_instruments`）与 `canonical_digest()` 复现摘要规则 |
| P0-12 depth trace | `scripts/verify_depth_trace.py` 同样改为 schema 4 信封（`schema_4_envelope()` + `canonical_snapshot_digest()`） |
| P0-12 死文件 | 删除 `crates/titan-cli/src/{pair_arb,grid,dual_ma}_v13_trace.json`（仍是 schema 3，全仓库无引用，仅有文档级提及） |
| P0-13 读取侧校验 | `crates/titan-cli/src/main.rs` 新增 `verify_bundle_abi_fingerprint()`：`report` 在 schema 校验后比较 `BundleManifest.abi_fingerprint` 与宿主 `ABI_FINGERPRINT`，不符返回稳定前缀 `bundle_abi_mismatch` 的错误；写入侧改用 `ABI_FINGERPRINT`（原命名遗留 `V13_ABI_FINGERPRINT`，取值同为 V14） |
| P0-13 发布边界 | `crates/titan-persistence/src/run.rs::commit_result_bundle` 增加 `expected_abi_fingerprint: Option<&str>`，不符在发布边界返回 `bundle_abi_mismatch`；`crates/titan-cli/src/storage_cli.rs` 的 `register-bundle` 传宿主指纹，`hftbacktest/examples/v13_grid_history.rs` 传自身指纹 |
| P0-13 Python 侧 | `python/titan-reporting/titan_reporting/bundle.py` 新增 `_host_abi_fingerprint()`（读 `TITAN_ABI_FINGERPRINT`，未配置时跳过比较以保持包与运行时解耦），不符抛 `BundleError("bundle_abi_mismatch…")` |
| P0-13 测试 | `crates/titan-persistence/tests/contracts.rs::bundle_from_another_strategy_abi_never_publishes`（不符 `bundle_abi_mismatch` 且不发布；自身指纹可发布）；`python/titan-reporting/tests/test_bundle.py::test_bundle_from_another_abi_is_refused_when_the_host_identity_is_known`（不符拒绝、相符通过、未配置放行） |
| P0-14 单一写入点 | `crates/titan-strategy-runtime/src/v13.rs` 的 `DEPRECATED_UNIT_PLACEHOLDER` 与 `apply_deprecated_unit_placeholders()` 成为唯一写入点（常量原在 `runtime_v13.rs` 内，只有在线路径调用） |
| P0-14 在线/离线一致 | `runtime_v13.rs`（在线）与 `offline_v13.rs`（离线回放 `OfflineV13Adapter::new`）都调用同一 helper；视图已带非零单位时返回 `false`，调用方以 `deprecated_units_already_claimed` 失败关闭，不再混用真实单位与占位 |
| P0-14 测试 | `deprecated_market_unit_placeholders_have_one_writer_for_both_paths`（两视图都被写入、已声明单位被拒绝、空切片合法）与 `offline_replay_publishes_the_same_deprecated_slots_as_the_live_runtime` |
| P0-15 跨场所离线证据 | 新增 `crates/titan-strategy-runtime/tests/cross_venue_object_table.rs`（2 项，全离线、无 connector、无网络）：`one_artifact_derives_the_same_hedge_ratio_from_two_venue_pairs`（基础币细 lot 0.01/0.001 与合约式 1×0.01 / 1×0.001 两套单位都对各自快照通过 `validate_instrument_units_with_metadata`，对象表各自派生 `hedge_lots_per_maker_lot = 10`、tick 比 1/1、`metadata_snapshot_digest` 等于各自快照摘要，而 `identity_digest` 不同）；`a_venue_pair_with_a_non_integral_ratio_is_refused_at_startup`（快照比例 10/3 时启动校验给出 `instrument_lot_ratio_not_integral`，建表层给出 `object_pair_lot_ratio`） |
| P0-15 元数据 provider | 新增 `crates/titan-strategy-runtime/src/metadata_provider.rs`（9 项单测）：可注入的 `MetadataTransport` seam，`VenueMetadataProvider` 负责重试/429/字段缺失判定；稳定原因码 `metadata_transport_unavailable`/`metadata_fetch_timeout`/`metadata_rate_limited`/`metadata_upstream_error`/`metadata_instrument_absent`/`metadata_unit_unrepresentable`/`metadata_synthetic_instrument`；覆盖成功快照 digest 自洽、同源只拉一次、传输失败不重试、上游错误区分、429 退避重试后耗尽、缺品种、缺字段/乘算溢出、synthetic 拒绝 |
| 矩阵 | 第 3 项改 ✅（仅限离线同产物两套单位派生同一比例；真实双 `runtime.toml` 启停仍需联网账户）；第 6 项 🟡 证据更新为“错误路径与可注入 transport 已实现并有 9 项单测，生产 transport 接线与真实交易所证据待授权”；当前 ✅20 / 🟡2 / ⬜3 |
| 边界 | 真实交易所在线拉取的证据仍需授权；本轮只证明“决策代码 + 错误路径”正确，生产 transport 实现（接 connector `get_instruments`）尚未接线 |

本轮门禁（`titan-build` 隔离副本）：`cargo fmt --all -- --check` 通过；`cargo clippy --workspace --all-targets --all-features -- -D warnings` 0 warning；
`cargo test --workspace --all-targets --all-features` **974 passed / 0 failed / 29 ignored**；CLI 6 个测试目标全通过（offline_recovery 3、single_file_strategy 1、
cli_golden 6、instruments_verify 3、strategy_compile 6、parameters_pin 3）；persistence 29+5 通过；SDK **190 项（2 项显式跳过）**；
reporting 6 通过；preflight 9 通过（原 7 项 + 2 项 schema 4 信封/摘要一致性）；守卫脚本 `check_single_runtime.sh`、`check_no_retired_hftbacktest_strategy.sh`、`check_strategy_no_exchange_facts.py`、
`check_repository_privacy.py`（本机 837 文件 0 findings）通过。

## 3.42 第四十二轮补充（P1-07 产物重编 + P3-02 可做部分）

| 项 | 内容 |
| --- | --- |
| P1-07 重编 | 6 个产物在 `titan-build` 用当前编译器重编：`cross_exchange_mm`、`reference_mm`、`grid`、`grid-gate-arb`、`pair_arb`、`event_counter`；manifest 的 `abi_version` 均为 **14**，`strategy validate` 全部 `valid:true` |
| P1-07 配置同步 | `deploy/pair_arb_v13/runtime.toml` 的 `expected_digest` 更新为 `22a6799b…22db`（与重编产物一致） |
| P1-07 负例 | 把产物的 manifest `abi_version` 由 14 改回 13 后加载：退出码 10，`invalid configuration: strategy ABI version mismatch: artifact 13, host 14; recompile the strategy` |
| P1-07 边界 | **未做生产签名**：`--signing-key` 需要用户私钥与 `key_id`，当前 `allow_unsigned_artifacts = true`；签名→验签闭环已由 `strategy_compile`/`v13/compiler`/`single_file_strategy` 用临时密钥覆盖 |
| P3-02 reserved | `v13.rs` 增模块级说明：恒 0 占位、唯一活槽是 `TitanSubmitOrderRequest.reserved[0]`（`CommandPurpose`）、V14 尾部与 payload 的 `reserved*` 进 schema hash；新增 `reserved_fields_are_zero_except_the_command_purpose_slot` |
| P3-02 能力位 | `OBJECT_CONTEXT` 的三处表达（V13 `1<<11`、V14 `1<<13`、字符串 `"object_context"`）收敛到 `OBJECT_CONTEXT_CAPABILITY` 单一来源；新增 `object_context_capability_keeps_its_meaning_across_v13_and_v14_bits`（含两常量 `assert_ne!`，证明不是简单复制） |
| P3-02 未做 | 删除 `reserved` 字段、`v13` 模块改名/删除：前者需另一次 ABI major，后者会打翻 `check_single_runtime.sh` 路径白名单，收益低于风险 |
| 工具修正 | `scripts/build_env.sh` 的解释器探测**只检查不导出**，导致 `TITAN_BUILD_PYTHON` 解析成功但 `strategy compile` 仍用默认 `python3` 并报 `No module named 'llvmlite'`；已改为**探测即导出** `TITAN_STRATEGY_PYTHON`/`TITAN_REPORT_PYTHON` |

本轮门禁（`titan-build`）：`cargo fmt --all -- --check` 通过；`cargo clippy --workspace --all-targets --all-features -- -D warnings` 0 warning；
`cargo test --workspace --all-targets --all-features` **976 passed / 0 failed / 29 ignored**（+2）；
`check_single_runtime.sh`、`check_no_retired_hftbacktest_strategy.sh`、`check_strategy_no_exchange_facts.py`、
`check_repository_privacy.py`（837 文件 0 findings）全部通过。

说明：`titan-strategy-runtime --lib` 曾在并行执行下出现 2 项 `recovery_storage` 时序失败，单独连跑 3 次全部通过
（26 passed），与 §3.5 记录的高负载抖动同类，未修改其时序断言。

## 3.43 第四十三轮补充（去除过度设计：签名与死代码）

用户判定"策略产物 Ed25519 签名"等属设计过度，要求收敛到回测与实盘本身。本轮按审计结论删除，**未触碰交易必需的一致性机制**。

| 项 | 内容 |
| --- | --- |
| 删除签名（A1） | 移除 `StrategySignatureV13`、`V13TrustPolicy`、`verify_signature`/`parse_signature`、`--signing-key`/`--key-id`、`--require-signature`/`--trusted-key`、`allow_unsigned_strategies`/`allow_unsigned_artifacts`、`trusted_ed25519_keys`、`StrategyErrorKind::SignatureInvalid`、死字段 `StrategyFileRef::signature_ref`（全仓赋值均为 `None`）、`titan-strategy-runtime` 的 `ed25519-dalek` |
| 保留的三重完整性校验 | `native_library_sha256` 库字节校验、`compiled_strategy_digest`（覆盖 canonical CBOR）、`target_triple` 校验全部原样保留；签名只多提供"来源可信"，不提供"内容完整" |
| 保留的兼容读 | 旧 manifest 若仍带 `signature` 键，加载时剔除后再算 canonical digest，保证已签名历史产物仍可加载 |
| **未做的部分** | 删除了两处"未配信任键即拒绝启动"的硬门禁（`core_runtime.rs`、`main.rs` 回测），否则删字段后实盘/回测会拒绝启动 |
| 删除死代码（B3） | `crates/titan-strategy-runtime/src/` 下 10 个 `v13_*_smoke.rs`（3,467 行），与 `examples/` 同名文件逐字节相同且无任何 `mod` 声明 |
| 删除死代码（新增发现） | `crates/titan-market-service/src/` 的 `execution.rs`、`execution_ownership.rs`、`observation.rs`、`risk.rs`（**4,062 行**）：`lib.rs` 从未声明这四个模块，内容与 account-service 逐字节相同，是从未参与编译的陈旧副本 |
| 删除空 crate | 7 个 0 文件的 crate 目录（`titan-runtime`、`titan-runtime-abi`、`titan-python-host`、`titan-connector-*`） |
| 删除研究编排（A4） | `management/frontend_research.rs`（1,386 行）+ `/api/v1/research` 路由 + CLI `Research` 命令分支 + shell 补全 |
| 简化存储（B1） | CLI 移除 Postgres 与 S3 构造路径（`cli_database.rs` 固定 `postgres: None, s3: None`；`--dsn-env` 等参数删除）；`storage_cli` 删除 8 个冷门子命令（`CleanStaging`/`PrepareHistory`/`PrepareObjects`/`Resume`/`Activate`/`PruneHistory`/`EvictLocalObjects`/`InspectParquet`）；`persistence_acceptance.py` 的切换助手改为显式报错 |
| 连带删除 | `scripts/verify_external_persistence.py`、`verify_local_persistence.py`（依赖已下线子命令）；`verify_data_persistence.sh` 移除对应 gate |
| **明确保留** | 对象表/对象门禁/元数据 provider、恢复与幂等、订单状态机、多绑定风控、`objects.rs` 的 sha256 内容寻址、`facts`/`snapshot`/`columnar` 数据记录、`connector/src/hyperliquid/signing.rs`（交易所 API 签名）、`management/legacy.rs`（API v1 兼容层，被 6 处引用）、隐私守卫 |

本轮门禁（`titan-build`）：`cargo fmt --all -- --check` 通过；`cargo clippy --workspace --all-targets --all-features -- -D warnings` 0 error；
`cargo test --workspace --all-targets --all-features --no-fail-fast` **965 passed / 0 failed / 29 ignored**（55 个测试目标全部执行）。
测试总数由 976 降为 965，差额来自被删除代码自带的用例（research 编排 8 项、签名相关用例、重复 smoke 文件内的测试）；不是测试被跳过。

守卫脚本：`check_no_retired_hftbacktest_strategy.sh`、`check_strategy_no_exchange_facts.py`、`check_single_runtime.sh`、`check_repository_privacy.py`（834 文件 0 findings）全部通过。

**保留待办**：`StrategyRuntimeContextV13` 与 V14 的合并未做——会与 `scripts/check_single_runtime.sh` 的护栏冲突（该守卫禁止出现无后缀的 `StrategyRuntimeContext`），需先决定是否放开这条设计约束。

## 3.44 第四十四轮补充（ABI context 去版本化）

用户决策：允许存在一个无后缀的 ABI context，把 `StrategyRuntimeContextV14` 改名为 `StrategyRuntimeContext`，并删除带版本的旧 ABI 文档。

| 项 | 内容 |
| --- | --- |
| 改名 | `StrategyRuntimeContextV14` → `StrategyRuntimeContext`（42 处引用，6 个文件）；`StrategyRuntimeContextV13` 结构体与 `impl Default`、布局断言、`assert_layout!` 块一并删除 |
| **指纹未变** | 改名前后 `V14_ABI_FINGERPRINT` 仍为 `18 7e 84 ba … 4d`，`size_of::<StrategyRuntimeContext>() == 440` 断言保留。依据：`object_schema_descriptor_v14()` 只记录 `context_size` 数值、不含类型名；Python 侧 `ABI_DTYPES` 本来就以无后缀的 `"StrategyRuntimeContext"` 注册（`abi_v14.py:167`），改名后两侧命名反而一致 |
| 产物兼容 | `pair_arb.titan`、`cross_exchange_mm.titan`、`grid.titan` 三个既有 ABI 14 产物在改名后仍 `valid`，**无需重编** |
| 守卫改写 | `check_single_runtime.sh` 从"恰好两个定义（V13 前缀 + V14 root）"改为"恰好一个无后缀定义、且位于 `v14.rs`"，并**反向禁止** `StrategyRuntimeContextV1[34]` 这类带版本别名再次出现 |
| 文档删除 | 删除 `strategy_abi_v13_implementation_status.md`、`strategy_abi_v13_post_refactor_audit.md`、`strategy_abi_v13_typed_state_blob_technical_design.md`、`strategy_abi_v14_object_context_design.md`、`strategy_abi_v14_switch_checklist.md`（约 180KB）；同步修复 9 个文档的入站链接 |

本轮门禁（`titan-build`）：`cargo fmt --all -- --check` 通过；`cargo clippy --workspace --all-targets --all-features -- -D warnings` 0 error；
`cargo test --workspace --all-targets --all-features --no-fail-fast` **965 passed / 0 failed / 29 ignored**（与改名前的数字完全一致）。
守卫脚本：`check_single_runtime.sh`（改后版本）、`check_no_retired_hftbacktest_strategy.sh`、`check_strategy_no_exchange_facts.py`、`check_repository_privacy.py`（831 文件 0 findings）全部通过。

**未改动历史结论**：`docs/market_symbol_and_account_object_design_review.md:25` 与自查记录 §3.36 中提及旧文档名的表述属于历史验收记述，按 AGENTS.md 保持原样；被引用的文档已在 §3.44 登记删除。

## 3.45 第四十五轮补充（删除离线回放功能）

用户决策：不再需要 CLI 的 trace 回放，全部删除；"生产签名"按删除处理、不再恢复。

| 项 | 内容 |
| --- | --- |
| 删除 CLI 回放 | `v13_backtest_worker`/`v13_backtest_engine`（约 300 行）替换为一条明确的拒绝错误；`V13Trace`、`V13TraceState`、`V13TraceEvent`、`V13TraceEventKind`、`V14TraceRuntimeAbi`、`V14TraceAccountInstrument` 与 `offline_object_table` 一并删除；`staged_command_fact` 因无调用者删除 |
| 删除配置与夹具 | `configs/{grid_v13_backtest,pair_arb_v13_backtest,dual_ma_aapl}.toml`；`fixtures/{grid,pair_arb,dual_ma}_v13_trace.json` |
| 保留夹具 | 新增 `fixtures/grid_v13_metadata.json`（仅 `runtime_abi`/`metadata`/`accounts`/`account_instruments`），供 `verify_cli_bundle.py`、`verify_depth_trace.py`、`test_cross_exchange_mm_preflight.py`、`test_grid_native.py` 取 ABI 身份与元数据快照 |
| **保留 `OfflineV13Adapter`** | 它不是回放专属：`hftbacktest/examples/grid_history/engine.rs` 与 `hftbacktest/src/backtest/strategy_v13.rs` 的真实历史回测正是通过它驱动策略；`offline_recovery` 测试也验证它的恢复能力。只删了 CLI 的 trace 入口 |
| 删除测试 | `test_grid_native.py::test_cli_trace_drains_and_rejects_incomplete_stop`（本来就是显式 skip 的回放用例） |
| 签名确认 | 第 43 轮已删除；本轮确认 `grep` 残留为 0，按用户决策不再恢复 |
| 文档 | `README.md` 快速开始改为编译+校验；`docs/titan_cli.md` 的"V13 Tick trace 回测"整节替换为"历史回测"；`strategies/grid.md` 删除回放命令与说明 |

本轮门禁（`titan-build`）：`cargo fmt --all -- --check` 通过；`cargo clippy --workspace --all-targets --all-features -- -D warnings` 0 error；
`cargo test --workspace --all-targets --all-features --no-fail-fast` **965 passed / 0 failed / 29 ignored**；
SDK **189 项通过 / 1 项跳过**（删除回放用例后由 190/2 变为 189/1）；preflight 9 项通过；
守卫脚本 `check_single_runtime.sh`、`check_no_retired_hftbacktest_strategy.sh`、`check_strategy_no_exchange_facts.py`、
`check_repository_privacy.py`（825 文件 0 findings）全部通过。

## 3.46 第四十六轮补充（在线元数据拉取：生产 transport 与真实拉取证据）

| 项 | 内容 |
| --- | --- |
| 生产 transport | 新增 `crates/titan-cli/src/metadata_transport.rs`：`HyperliquidMetadataTransport` 走官方公开 `POST /info`（`{"type":"meta"}`），无需任何凭证；把 `szDecimals` 映射为精确 `quantity_lot`（10 的幂）与 `min_quantity`，`contract_multiplier = 1`，报价币 `USDC` |
| 配置入口 | `[strategy_service] metadata_source = "hyperliquid"` 或 `"hyperliquid-testnet"`；与 `metadata_snapshot` **互斥**，同时配置在 `ConfigurationAdapter::adapt` 处以 `cannot both be configured` 拒绝（早于策略适配） |
| 请求范围 | provider 的请求列表由**配置里所有 enabled 的市场源绑定**生成：只有这些符号可能进入对象表，因此只要求交易所回答这些 |
| 决策层不变 | 重试预算、429 退避、缺字段/乘算溢出/synthetic 拒绝仍全部由 `VenueMetadataProvider` 负责（第 41 轮 9 项单测覆盖）；生产 transport 只做调用与字段映射 |
| **真实拉取证据** | `live_hyperliquid_metadata_is_fetched_and_refused_for_a_missing_tick`（`#[ignore]`，需出网）真实请求返回 **235 个资产**；BTC 的 `szDecimals = 5` → `quantity_lot = 1e-5`、`contract_multiplier = 1`、`USDC`、`synthetic = false`；耗时约 1.3 s。原始响应同时确认**不存在任何 tick 字段** |
| tick 缺失的处理 | Hyperliquid 公开 API 不发布价格 tick（价格规则按有效数字随价位变化），故 `price_tick = None`，provider 以 `metadata_unit_unrepresentable` **拒绝**该品种而不是猜值。多腿准入需要可证 tick，因此 HL 品种不能进入多腿；单腿/现货使用不受影响 |
| 网络约束 | 编译服务器只能访问 Hyperliquid；OKX 与 Binance 出网被阻断（`curl` 超时），因此真实证据只能来自 HL |

本轮门禁（`titan-build`）：`cargo fmt --all -- --check` 通过；`cargo clippy --workspace --all-targets --all-features -- -D warnings` 0 error；
`cargo test --workspace --all-targets --all-features --no-fail-fast` **967 passed / 0 failed / 30 ignored**（+2 例，其中 1 例为需出网的 `#[ignore]` 真实拉取）；
守卫脚本 `check_single_runtime.sh`、`check_no_retired_hftbacktest_strategy.sh`、`check_strategy_no_exchange_facts.py`、
`check_repository_privacy.py`（825 文件 0 findings）全部通过。

## 3.47 第四十七轮补充（重构复审与修复）

对第 42–46 轮的全部改动做了独立只读复审，发现并修复以下问题：

| 严重度 | 问题 | 修复 |
| --- | --- | --- |
| 高 | **误伤实盘路径**：为让 `-e backtest` 提前失败而重写 `resolve_run_spec`，一度连带删掉了 `Environment::Live → resolve_core_live_run_spec` 分支，实盘解析将不可用 | 恢复 Live 分支，只在 backtest 分支前返回明确的移除提示；`resolve_core_live_run_spec` 完整保留 |
| 高 | 配置了 `metadata_source` 但没有 `market_sources` 时，provider 会返回空快照，使每个实例建表使用空身份 | `metadata_requests` 为空时不注入 provider，回到"无对象上下文"语义 |
| 高 | 三个管理端验证脚本仍调用已删除的 `research` 命令组与 8 个 storage 子命令，会在 build server 直接失败 | `titan_cli_frontend_smoke.py` 移除 3 个 research 用例与 `--skip-research`；`management_cli_smoke.py` 取消 backtest/report 的 frontend 映射；`management_console_smoke.py`/`management_control_smoke.py` 移除外部后端块 |
| 中 | `post_info` 不检查 HTTP 状态，429 会退化成 serde 解析错误，限流重试永不触发 | 新增 `HyperliquidError::HttpStatus {status, body}`，先判状态再解码；`classify` 改为按状态码匹配 |
| 中 | 守卫正则只匹配 `{` 与结构体名同行的写法，brace 换行可绕过 | 改为先找 `^pub struct StrategyRuntimeContext\b` 再计数，并补 `\|\| true` 防空输入误终止 |
| 低 | 删除回放后 `RunConfig`/`BacktestConfig`/`CliError::Toml`/`default_command_capacity`/`resolve_path` 成为死代码 | 全部删除；`--help` 中 backtest 相关措辞同步更新 |
| 低 | `docs/rust_strategy.md`、`docs/titan_cli.md`、`docs/local_persistence_operations.md`、`docs/README.md` 仍把已删功能当作可用 | 改为历史回测指引；外部后端加下线横幅；填充空的"策略接口"索引；更正"仅支持 ABI V13"的表述 |
| 低 | `docs/bar_tick_numba_strategy.md` 以现行时态引用已删除的 `StrategyRuntimeContextV13` | 改为 `StrategyRuntimeContext` |

**复核后保留的已知限制**（未修，已记录）：
- `MetadataFetchPolicy::attempt_timeout` 是 transport 必须遵守的预算而非强制计时器——provider 在无 async 运行时下调用（`try_current()` 断言为 Err），由各 connector 客户端自身的超时兜底；已在两处文档化。
- `metadata_transport` 的生产实现位于 `titan-cli`（依赖方向所限），`hftbacktest` 等离线工具无法复用；下沉到 `connector` 属后续可选重构。
- `titan-persistence` 中 `prepare_history`/`resume`/`activate`/`prune_history` 等 `pub` API 的唯一非测试调用者是已删除的 CLI 分支，现仅测试在用；因是 `pub` 不会触发告警，暂保留。
- `docs/` 中 9 个文件含历史死链（指向已删除的 `strategy_abi_v8_migration.md`、`titan-plugin-engine` 等），均早于本轮，未修改历史文档结论。

本轮门禁（`titan-build`）：`cargo fmt --all -- --check` 通过；`cargo clippy --workspace --all-targets --all-features -- -D warnings` 0 error；
`cargo test --workspace --all-targets --all-features --no-fail-fast` **967 passed / 0 failed / 29 ignored**；
守卫脚本 `check_single_runtime.sh`（修复后）、`check_no_retired_hftbacktest_strategy.sh`、`check_strategy_no_exchange_facts.py`、
`check_repository_privacy.py`（825 文件 0 findings）全部通过。

## 3.48 第四十八轮补充（真实拉取扩展到 Gate.io、两项新守卫）

| 项 | 内容 |
| --- | --- |
| **Gate.io transport** | 新增 `GateFuturesMetadataTransport`，走公开 `GET /api/v4/futures/usdt/contracts`（无需鉴权，1051 个 USDT 永续合约）。Gate **完整发布全部单位**：`order_price_round`（tick）、`quanto_multiplier`（每张基础币）、`order_size_min`（最小张数），因此是唯一能让对象表完整建成并进入多腿准入的已接线交易所 |
| 配置 | `[strategy_service] metadata_source = "gate"` 新增；与 `"hyperliquid"` / `"hyperliquid-testnet"` 并列，仍与 `metadata_snapshot` 互斥 |
| **真实拉取证据（Gate）** | `live_gate_metadata_yields_a_complete_admissible_snapshot`：真实返回 >100 个合约；`BTC_USDT` 的 `price_tick = 0.1`、`contract_multiplier = 0.0001`、`quantity_lot = 1`、`min_quantity = 1`、报价币 `USDT`、`synthetic = false`；生产 provider 接受并生成 digest 自洽的快照 |
| **真实拉取证据（HL）** | `live_hyperliquid_metadata_is_fetched_and_refused_for_a_missing_tick` 保留：HL 不发布 tick，故以 `metadata_unit_unrepresentable` 拒绝，验证的是真实响应的拒绝路径 |
| 网络现状 | 编译服务器可访问 **Gate.io 与 Hyperliquid**；OKX 与 Binance 仍不可达（`www.okx.com` 在本地解析到 `169.254.0.2`，属本地 DNS 策略而非应用问题）。OKX/Binance 的真实证据需在能访问它们的机器上补 |
| 新守卫：CLI 命令引用 | `scripts/check_script_cli_commands.py`：从 `storage_cli.rs`/`main.rs` 提取子命令清单，扫描 `scripts/*.py|sh` 中对 `titan storage/research <sub>` 的引用，失效即失败。已验证能抓到引用已删的 `research backtest` |
| 新守卫：文档链接 | `scripts/check_doc_links.py`：扫描 `docs/**`、`README.md`、`AGENTS.md` 的 Markdown 链接，目标不存在即失败。本轮据此修复 **10 条死链**（4 条路径层级错误、3 条指向已删文档、3 条指向已删的 `strategies/grid/README.md`），现为 **131 篇文档 0 死链**，无需白名单 |

本轮门禁（`titan-build`）：`cargo fmt --all -- --check` 通过；`cargo clippy --workspace --all-targets --all-features -- -D warnings` 0 error；
`cargo test --workspace --all-targets --all-features --no-fail-fast` **967 passed / 0 failed / 31 ignored**（+2 项需出网的真实拉取）；
六项守卫脚本全部通过（含两个新增），`check_repository_privacy.py` 825 文件 0 findings。

## 3.49 第四十九轮补充（OKX / Binance transport 先行实现）

按用户要求先把 OKX 与 Binance 的生产 transport 写好；两者在编译服务器上仍不可达（`www.okx.com` 解析到 `169.254.0.2`），网络放开后可直接取真实证据。

| 项 | 内容 |
| --- | --- |
| OKX | `OkxSwapMetadataTransport` 走公开 `GET /api/v5/public/instruments?instType=SWAP`（无需鉴权）。映射：`tickSz`→`price_tick`、`ctVal`→`contract_multiplier`、`minSz`→`min_quantity`、一个合约=一个 `quantity_lot`、`settleCcy`→报价币。只保留 `state == "live"` 且 `ctType != "inverse"` 的品种——反向合约的 `ctVal` 以基础币计，不是报价币乘数 |
| Binance | `BinanceFuturesMetadataTransport` 走公开 `GET /fapi/v1/exchangeInfo`。映射：`PRICE_FILTER.tickSize`、`LOT_SIZE.stepSize`/`minQty`、`MIN_NOTIONAL.minNotional`，`contract_multiplier = 1`（USDⓈ-M 线性）。只保留 `status == "TRADING"` 且 `contractType == "PERPETUAL"` |
| 配置 | `metadata_source` 现支持 `"gate"`、`"okx"`、`"binance"`、`"hyperliquid"`、`"hyperliquid-testnet"`；仍与 `metadata_snapshot` 互斥 |
| 离线验证 | `okx_and_binance_wire_fields_map_to_exact_units`（**不联网**）：用两家的真实字段名构造 JSON，断言解析出的精确十进制与过滤规则（`state`/`ctType`/`status`）正确——交易所改字段名会立刻失败 |
| 待补的真实证据 | `live_okx_and_binance_metadata_are_complete`（`#[ignore]`）：网络可达时断言 OKX `BTC-USDT-SWAP` 与 Binance `BTCUSDT` 的 tick/乘数/最小量齐全 |

本轮门禁（`titan-build`）：`cargo fmt --all -- --check` 通过；`cargo clippy --workspace --all-targets --all-features -- -D warnings` 0 error；
`cargo test --workspace --all-targets --all-features --no-fail-fast` **968 passed / 0 failed / 32 ignored**；
六项守卫脚本与隐私检查（825 文件 0 findings）全部通过。

## 3.50 第五十轮补充（四个交易所真实拉取全部通过）

编译服务器的 mihomo 代理（`mixed-port: 7893`）恢复后，OKX 与 Binance 可达，四个交易所的真实拉取全部取得证据。

| 交易所 | 端点 | 实测结果 |
| --- | --- | --- |
| Gate.io | `GET /api/v4/futures/usdt/contracts` | 1051 个 USDT 永续；`BTC_USDT` tick `0.1`、乘数 `0.0001`、最小 1 张；生产 provider 接受并生成自洽快照 |
| OKX | `GET /api/v5/public/instruments?instType=SWAP` | `BTC-USDT-SWAP`：`tickSz 0.1`、`ctVal 0.01`、`minSz 0.01`、`ctType linear`、`settleCcy USDT` |
| Binance | `GET /fapi/v1/exchangeInfo` | 946 个合约；`BTCUSDT` `PRICE_FILTER.tickSize 0.10`、`LOT_SIZE.stepSize 0.001 / minQty 0.001`、`MIN_NOTIONAL.notional 50` |
| Hyperliquid | `POST /info {"type":"meta"}` | 235 个资产；不发布 tick，按 `metadata_unit_unrepresentable` 拒绝（真实响应的拒绝路径） |

**过程中发现并修复的两个真实缺陷**：

1. **Binance notional 字段名错误**：实现里写的是 `minNotional`，真实 USDⓈ-M 响应是 `notional`。现在同时接受两者（`rename = "notional", alias = "minNotional"`），并已用真实响应验证。
2. **代理破坏 localhost 测试**：`build_env.sh` 启用代理后未设置 `no_proxy`，连接器的本地模拟 HTTP 服务请求被转发并返回 502，导致 **15+ 个连接器测试同时失败**。已加入 `no_proxy=127.0.0.1,localhost,::1,169.254.0.0/16`，实测 localhost 直连恢复 200。

**代理接入**：`scripts/build_env.sh` 新增 `titan_venue_proxy`，在 `127.0.0.1:${TITAN_VENUE_PROXY_PORT:-7893}` 实际监听时自动导出 `https_proxy`/`http_proxy` 与 `no_proxy`；无代理时静默退回直连。验证命令无需再手动 `export`。

本轮门禁（`titan-build`，代理自动生效）：`cargo fmt --all -- --check` 通过；`cargo clippy --workspace --all-targets --all-features -- -D warnings` 0 error；
`cargo test --workspace --all-targets --all-features --no-fail-fast` **968 passed / 0 failed / 32 ignored**；
`cargo test -p titan-cli --lib live_ -- --ignored` **3 passed**（覆盖 4 个交易所）；
六项守卫脚本与隐私检查（825 文件 0 findings）全部通过。

## 4. 行为变化与迁移

- 缺少 `contract_multiplier` 的市场绑定、未知键、非法十进制单位现在**加载期失败**；这是有意的
  fail-closed，不提供静默默认。迁移方式是按原生元数据补齐字段。
- `base_delta_per_lot`、策略内的比例与最小量仍是“待校验候选”；平台以
  `quantity_lot × contract_multiplier` 为权威并在启动期核对。
- 原因码是稳定接口（preflight 与脚本依赖），改动需同步本文与 CLI 文档。

## 5. 限制

1. `titan instruments verify` 目前只做**配置内部**一致性；尚未与交易所原生元数据核对（P0-10/D2），
   因此 `valid=true` 不代表已取证。报告不包含“元数据已校验”标记，任何对外结论不得越过这一点。
2. 示例与部署配置仍全部 `enabled=false`；验证只证明它们可解析且单位自洽。
3. ABI 布局测试覆盖 struct 大小与字段偏移，不覆盖 numpy dtype 的**元素类型字符串**：等宽类型替换
   （如 `i64`→`u64`）不会被本测试发现，需在 P1-05 的 schema/codegen 双向生成中补齐。
4. P0-06 的 `strategy validate` 端到端拒绝旧产物需要一份指纹不同的产物；当前仓库没有该 fixture，
   待 P1-07 重编产物后按验收矩阵第 1 条补验。
5. 编译服务器上的 Python 环境是本地 shim（conda 环境软链到 `.venv/bin/python`），SDK 声明的
   numba/LLVM 版本仍需在 P1 原生 AOT 验收前复核。

## 3.41 第四十二轮补充（文档对账：§3.27–§3.31 的 -2 根因定位）

第 41 轮结项审计对 §3.27–§3.31 遗留的"离线 `-2` 未定位"做了只读溯源，结论与本目录既有记录不同，在此更正：

| 原记录 | 溯源结论（代码依据） |
| --- | --- |
| §3.28「-2 来自未被补全的视图缓冲」 | **不成立**。`main.rs` 在首个事件前已用 `trace.initial` 填好 markets/positions/balances/accounts/active_orders，`OfflineV13Adapter::invoke` 直接把这些切片交给回调 |
| §3.29「缺少 `start` 步」 | **不成立**。trace 里没有 `start` *事件*，但 `main.rs` 在事件循环前调用 `adapter.start(initial.now_ns)`；"无 start 事件"不等于"无 start 回调" |
| §3.30「-2 不在 `on_start` 内部」 | **不成立**。`strategies/grid.py:732-734` 在余额的 `currency_no` 与参数 `quote_currency_no` 不一致时执行 `fail(s, 701, True)`，该路径把 `last_error_code` 置为 -2 并返回 -1 |
| §3.31「symbol payload 与 fixture 推导不一致」 | **部分成立但非根因**：对象表 currency 编号是账户局部编号（示例为 1），而 grid 的离线参数/trace 用 0，两者不一致即触发上面的 701 |

- 另外，报告中出现的 "-4" 是 `CliError` 文案 `{error} (last_error_code …)` 与 `V13LoadError::AbiLayout` 的 Display（含数字 14/4）拼接造成的**误读**，不是运行时的返回值。
- 因此第 7/13 项的性质是"**离线输入与策略参数编码不一致**"，而不是构建路径或 ABI 缺陷；修复需要同时调整 trace 夹具的 currency 编号与离线视图的弃用单位占位，属产品语义改动，未在本轮实施。
- 该结论只来自源码阅读，**未在 `titan-build` 重新运行回放验证**；两个回放用例仍为显式跳过。

## 6. 证据

命令输出与完整测试日志保存在编译服务器私有目录；本轮累计 21（第一批）、16（第二轮）、13（第三轮）与 12（第四轮）个变更文件在本地与编译服务器上 SHA-256 逐一致（清单与原始日志私有保存），本页只保留脱敏结论。
本轮未产生新的实盘授权，未访问真实账户。
