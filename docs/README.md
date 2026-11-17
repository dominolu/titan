# Titan 文档

`docs/` 只保存面向使用者和集成方的公开说明。

公开记录的脱敏边界、私有证据与提交检查见[仓库隐私约定](repository_privacy.md)。

## 项目状态

- [项目精简重构清单：重复保护、冗余实现与闲置入口](refactor_simplification_checklist.md)
- [Titan 系统重构与验证清单](system_refactor_checklist.md)
- [Titan 项目重构剩余任务清单](refactor_remaining_tasks.md)
- [市场标的单位校验 P0 第一批（2026-11-13）](verification/2026-11-13/symbol-units-p0/README.md)
- [管理平台实施计划](management_console_implementation_plan.md)
- [数据持久化实施计划](data_persistence_implementation_plan.md)
- [M1 SQLite 恢复闭环验证](verification/2026-10-27/data-persistence/m1/README.md)
- [M2 本地数据持久化验证](verification/2026-10-27/data-persistence/m2/README.md)
- [M3 PostgreSQL/S3 与双向切换验证](verification/2026-10-28/data-persistence/m3/README.md)
- [M4 受控实盘：SQLite 固定恢复与最小下撤单](verification/2026-10-28/data-persistence/m4/README.md)
- [P14 真实成交补验：未通过，按用户要求停止并保留占用](verification/2026-10-28/data-persistence/m4-filled/README.md)
- [P14 完整 A/N/E 非实盘：候选 04 负例与候选 05 通过](verification/2026-10-28/data-persistence/m4-ane/README.md)
- [P14 候选 05 故障补验：部分成交、bootstrap、重连及异常态 Stop](verification/2026-10-28/data-persistence/m4-faults/README.md)
- [P14 故障后安全恢复：候选 07 补查、新代次恢复与原期限 Stop](verification/2026-10-28/data-persistence/m4-safe-recovery/README.md)
- [永续 DEX 与跨所交易实施及验收计划](cross_venue_implementation_plan.md)
- [Lighter、Backpack、Variational 接入与能力边界](exchange_onboarding.md)：[全量 API 验证和剩余条件](verification/2026-10-31/native-venues-all-api/README.md)。
- [三家交易所全部已公开原生 API 目录](exchange_api_catalog.md)

## 架构与发布

- [Titan 新一代实盘内核：EventEngine 与 PluginEngine 重构](event_plugin_engine_refactor_story.md)
- [管理平台需求文档](management_console_requirements.md)
- [Web 管理模块技术方案](web_console_technical_plan.md)
- [数据持久化设计：SQLite 恢复与可替换后端](data_persistence_design.md)
- [行情录制存储格式 v1：可调网格 Top-N CSR + tick 偏移](market_data_recording_design.md)
- [行情录制格式 v1 实施技术方案（最小实现）](market_data_recording_implementation_plan.md)
- [永续 DEX 与跨所交易技术设计](cross_venue_trading_design.md)
- [四个永续 DEX 项目的交易所与策略源码调研](perp_dex_research.md)

## CLI

- [Titan CLI 与 Agent 接口](titan_cli.md)
- [单文件策略、工具链与发布](single_file_strategies.md)
- [管理模块运行与验证](management_console_operations.md)
- [SQLite 恢复存储维护](recovery_storage_operations.md)
- [本地历史与结果存储维护](local_persistence_operations.md)（外部后端已下线，见文首说明）

## 策略接口

- [策略观测协议](strategy_observation_protocol.md)：ABI context 布局、对象 payload 版本与 `-2` 失败语义。
- [Rust 策略与历史回测](rust_strategy.md)：编译产物、`hftbacktest` 历史回测入口。
- [Bar/Tick Numba 策略](bar_tick_numba_strategy.md)：Python 策略编写与编译约束。

## 策略设计

- [pair_arb：Slot 化双腿执行内核 V3 需求](pair_arb_strategy_requirements.md)
- [cross_exchange_mm 修复与上线准备方案](cross_exchange_mm_repair_plan.md)
- [cross_exchange_mm 实盘准备工作清单](cross_exchange_mm_live_worklist.md)
- [cross_exchange_mm 多绑定受控执行设计（B1 冻结稿）](cross_exchange_mm_multi_binding_design.md)
- [cross_exchange_mm 交易所配对、原生元数据与行情/数据方案](cross_exchange_mm_venue_pair_and_data.md)
- [cross_exchange_mm 停止与风控预算表](cross_exchange_mm_stop_budget.md)
- [cross_exchange_mm 策略自身待办清单](cross_exchange_mm_strategy_tasks.md)
- [市场标的与账户对象：策略读取属性而非硬编码单位](market_symbol_and_account_object_design.md)
- [市场标的与账户对象：设计评审](market_symbol_and_account_object_design_review.md)
- [市场标的与账户对象：实施计划与任务分解](market_symbol_and_account_object_implementation_plan.md)
- [市场标的与账户对象设计评审：现状核对、结构性缺口与改造清单](market_symbol_and_account_object_design_review.md)
- [策略观测协议](strategy_observation_protocol.md)
- 验收：[cross_exchange_mm 阶段 A 修复与验收（2.0.1）](verification/2026-11-11/cross-exchange-mm-phase-a/README.md)、[B4 提交用途与成交先于持仓的合法对冲（2.0.2）](verification/2026-11-11/cross-exchange-mm-b4-command-purpose/README.md)、[B5 逐腿预占与救援额度（部分）](verification/2026-11-11/cross-exchange-mm-b5-reservation/README.md)、[B5-剩余：保证金投影与崩溃矩阵](verification/2026-11-11/cross-exchange-mm-b5-reservation-rest/README.md)、[B8：保证金准入（部分）](verification/2026-11-11/cross-exchange-mm-b8-margin-admission/README.md)

## 数据协议

- [ResultBundle schema v1](result_bundle_schema.md)

## ABI

仓库策略系统使用单一 ABI context（`StrategyRuntimeContext`，无版本后缀）；版本身份由 `STRATEGY_ABI` 与 `ABI_FINGERPRINT` 承载，旧 ABI 的 loader、SDK 与迁移文档均已移除。

- [深度快照、可信 Top100 与重同步契约](depth_pipeline.md)：ABI V13 可选能力、容量、策略预热及离线输入；[2026-10-28 验收](verification/2026-10-28/depth/README.md)。
