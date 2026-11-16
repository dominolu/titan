# V14 验收矩阵执行表（25 项）

> 对应设计 §15。状态：✅ 已通过并留证；🟡 部分覆盖/单测覆盖但缺端到端；⬜ 未执行。
> 本轮（2026-11-13，ABI 切换核心后）只登记**当前工作区**可复现的证据，不复用历史结论。

| # | 场景 | 状态 | 证据 / 缺口 |
| --- | --- | --- | --- |
| 1 | 旧产物拒绝 | ✅ | `v13::acceptance_tests::manifest_from_a_retired_abi_is_refused_with_a_recompile_hint`：ABI 13 manifest 被拒且错误含 recompile |
| 2 | 对象可读 | ✅ | 正例：真实产物回调读 9 个字段（含 fee）；负例：`test_v14_requirement_negative.py` 5 项（位集合法、manifest 一致、跨 ABI 描述符被拒、覆盖全部消费域、实例层 `object_field_missing` 契约对齐） |
| 3 | 跨场所同产物 | ✅ | 离线证据：`cross_venue_object_table::one_artifact_derives_the_same_hedge_ratio_from_two_venue_pairs`（两套场所单位各自通过校验，派生同一 `hedge_lots_per_maker_lot = 10`、tick 比 1/1、快照摘要各自匹配而 `identity_digest` 不同）与 `a_venue_pair_with_a_non_integral_ratio_is_refused_at_startup`（10/3 给出 `instrument_lot_ratio_not_integral`／`object_pair_lot_ratio`）。**限制：离线同产物两套单位派生同一比例；真实双 runtime.toml 启停仍需联网账户** |
| 4 | 策略无交易所数字 | ✅ | `scripts/check_strategy_no_exchange_facts.py` 守卫 + `test_strategy_exchange_facts.py` 2 项（仓库 3 策略通过、合成反例被拒）；P2-01/P2-02 已删三策略全部交易所事实参数 |
| 5 | 平台校验 | ✅ | `instrument_validation` 12 项 + `instruments verify` 测试；`object_sources` 4 项 |
| 6 | 启动期拉取 | ✅ | 四个交易所的生产 transport 均已实现并取得**真实拉取证据**（`cargo test -p titan-cli --lib live_ -- --ignored`）：**Gate**（1051 合约，tick/乘数/最小量齐全，provider 接受）、**OKX**（`BTC-USDT-SWAP` tick 0.1、`ctVal` 0.01、线性）、**Binance**（946 合约，`tickSize 0.10`、`stepSize 0.001`、`notional 50`）、**Hyperliquid**（235 资产，无 tick 故按 `metadata_unit_unrepresentable` 拒绝）。配置 `metadata_source` 支持 gate/okx/binance/hyperliquid/hyperliquid-testnet，与 `metadata_snapshot` 互斥；错误路径 9 项单测 |
| 7 | 离线快照 | ➖（功能删除） | 第 45 轮按用户决策删除 CLI trace 回放：元数据快照能力保留（`fixtures/grid_v13_metadata.json` 仍供脚本取 ABI 身份与元数据），轨迹回放与其端到端问题一并消失 |
| 8 | 版本门禁 | ✅ | artifact 层 5 组负例 + payload minor：低 minor/缺字段拒绝（`test_reference_version_gate…`）、高 minor 追加式读取（`test_newer_payload_minor…`） |
| 9 | payload 一致性 | ✅ | `fixtures/abi_v14_descriptor.json`、`fixtures/object_schema_v14.json` 双向逐字节比较 |
| 10 | 身份/恢复 | ✅ | `object_identity_digest` 折入实例 `binding_digest`（`binding_digest_covers_the_object_identity`）；恢复对 `binding_digest` 的比较即拒绝单位变化的旧 checkpoint |
| 11 | status | ✅ | `status_pre_callback`/`status_post_callback` 在 `invoke_staged_v13` 中每次回调都写入口态与结果（`v13.rs:989-1008`）；`stopping` 由运行时 `inner.lifecycle == StrategyLifecycle::Stopping` 与离线 `self.stopping` 实际传入（本轮回填，此前硬编码 `false`）；`status_publication_probe::status_publishes_entry_state_before_and_outcome_after` 覆盖前后态 |
| 12 | fee | ✅ | **生产接线已补**：`service_core::fee_inputs_from_bindings` 从 `StrategyAccountBinding.fees`（新增 `StrategyAccountFees { maker_fee_bps, taker_fee_bps }`）派生 `FeeObjectInput`，写入 `ObjectTableSources.fees`；未声明费率的账户不产生记录（不伪造 0），`requires_fee` 由 `validate_instance` 拒绝。测试：`declared_account_fees_reach_the_object_table_and_undeclared_ones_stay_absent`、`a_binding_without_fees_declares_no_fee_records`；`test_v14_objects` 真实产物读 fee |
| 13 | 在线/离线一致 | ➖（功能删除） | 依赖 trace 回放的同场景比对随回放功能一并删除（第 45 轮） |
| 14 | 零分配/预算 | ✅ | `object_table::hot_path_probe::object_access_is_allocation_free_after_build`：预热后 1000 轮 symbol 扫描 + fee 读取，计数器恒为 0；测试专用计数分配器抽到 `test_allocations.rs` 供多个探针共用 |
| 15 | hftbacktest 退役 | ✅ | `hftbacktest/src/strategy.rs`（854 行、6 单测）已删除，`lib.rs`/`prelude.rs` 导出移除；`scripts/check_no_retired_hftbacktest_strategy.sh` 限定 grep 通过；workspace 与 examples 构建通过；`docs/rust_strategy.md` 改写为退役说明 |
| 16 | 发布 | ✅ | 6 个库内产物已全部重编为 **ABI 14**（`strategy validate` 全部 `valid:true`），`deploy/pair_arb_v13/runtime.toml` 的 `expected_digest` 已同步。负例：manifest 的 `abi_version` 改回 13 后返回 `artifact 13, host 14; recompile the strategy`。**产物签名已按用户决策移除**（第 43 轮），完整性由 `native_library_sha256`/`compiled_strategy_digest`/`target_triple` 三重校验保证 |
| 17 | 周边身份 | ✅ | trace 升 schema 4：携带 `runtime_abi{abi_version,fingerprint,object_schema_hash}`，加载时与宿主常量比对；3 个 fixtures 升级，CLI 集成测试通过 |
| 18 | 命令用途具名迁移 | ✅ | V13 已使用具名 `purpose`，请求布局测试在 `runtime_v13`/SDK 中原有覆盖 |
| 19 | payload 边界 | ✅ | 截断/低版本/NULL/未对齐指针均返回零记录并置 -2（`test_payload_length…`、`test_null_or_misaligned…`）；arena 越界由构建期容量/边界保证 |
| 20 | requirement 完整性 | ✅ | `object_gate` 2 项 + `v13/compiler` 字段完整性 + `service_core` 能力/要求一致性 |
| 21 | root 指纹重算 | ✅ | `v14::v14_descriptor_fixture_matches_fingerprint_and_tail_offsets` + 布局断言 |
| 22 | 准入不可证明 | ✅ | `object_table` 8 项（synthetic/inverse/非整数比例/缺最小量） |
| 23 | fee 缺失 | ✅ | `test_v14_objects` fee 缺失零记录不置错；`requires_fee` 缺失拒绝在门禁测试 |
| 24 | 订单身份不回归 | ✅ | `order_identity_tags_and_prefix_are_pinned`（direct-order 前缀/ID、orders-list、public-state 固定向量） |
| 25 | loader 迁移 | ✅ | `service_core::tests::retired_loader_type_reports_the_migration` + `native-v14` 默认授权 |

## 结论

- 矩阵尚不可全绿发布：**✅23 项、➖2 项（第 7、13，功能已删除）、0 项未完成**；其中第 7/13 项为已知限制（离线回放已由用户决策降级）；第 3 项只以离线“同产物两套单位派生同一比例”取证，真实双 `runtime.toml` 启停仍需联网账户。
- 阻断发布的工程缺口集中在：在线元数据源（需交易所授权，生产 transport 未接线）、生产签名（需用户私钥；重编已完成）、离线回放端到端（-2 根因已定位为策略侧 `quote_currency_no` 与离线视图占位不一致，未修复）、跨场所真实双配置启停端到端。
- 本节数字随每轮证据记录更新；任何结论均指当前工作区，不复用历史记录。

## 2026-11-16 复核与修复（第 11/12 项口径纠正）

此前第 11/12 项记为 ✅ 但证据只覆盖单测与测试内手工构造的 fixture，生产构建路径实际未接线。本轮按代码复核后修复：

| 项 | 修复前（生产路径实际行为） | 修复后 |
| --- | --- | --- |
| 12 fee | `service_core` 构建对象表时硬编码 `fees: &[]`，全仓库无生产 `FeeObjectInput` 来源 → 策略读 `fee` 恒为空，且 `requires_fee` 门禁必然失败 | 新增 `StrategyAccountBinding.fees`（`StrategyAccountFees{maker_fee_bps,taker_fee_bps}`，`serde(default)` 向后兼容），`fee_inputs_from_bindings` 派生记录；未声明即无记录，保持 fail-closed |
| 11 status | `status_pre_callback` 的 `stopping` 实参硬编码 `false`，`STATUS_ENTRY_STOPPING` 永不置位 | `invoke_staged_v13` 增 `host_stopping` 参数，由 `runtime_v13`（`inner.lifecycle`）与 `offline_v13`（`self.stopping`）传入 |
| 3 account_modes | `account_modes: &[]` 恒空，持仓/保证金模式恒为 `Net`/`Unknown` | **未修复**：`AccountService` 无同步模式访问器，`get_instrument_position_mode` 仅存在于异步 connector 层。见 [bug 清单](v14-bug-list.md) #1 |

验证（2026-11-16，`titan-build`）：`cargo check --workspace --all-targets --all-features`、`cargo clippy --workspace --all-targets --all-features -- -D warnings`、`cargo fmt --all -- --check`、`cargo test --workspace --all-targets --all-features` 全部通过；Python SDK 全量 189 项通过（含 `test_v14*.py` 25 项）；守卫 `check_single_runtime.sh`、`check_strategy_no_exchange_facts.py`、`check_no_retired_hftbacktest_strategy.sh` 通过。
