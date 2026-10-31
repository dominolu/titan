# Titan 文档

`docs/` 只保存面向使用者和集成方的公开说明。

## 项目状态

- [Titan 系统重构与验证清单](system_refactor_checklist.md)
- [Titan 项目重构剩余任务清单](refactor_remaining_tasks.md)
- [Strategy ABI V13 重构后适配审计](strategy_abi_v13_post_refactor_audit.md)
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
- [Lighter、Backpack、Variational 接入与能力边界](exchange_onboarding.md)：[全量 API 验证和剩余条件](verification/2026-10-31/native-venues-all-api/README.md)。
- [三家交易所全部已公开原生 API 目录](exchange_api_catalog.md)

## 架构与发布

- [Titan 新一代实盘内核：EventEngine 与 PluginEngine 重构](event_plugin_engine_refactor_story.md)
- [管理平台需求文档](management_console_requirements.md)
- [Web 管理模块技术方案](web_console_technical_plan.md)
- [数据持久化设计：SQLite 恢复与可替换后端](data_persistence_design.md)

## CLI

- [Titan CLI 与 Agent 接口](titan_cli.md)
- [管理模块运行与验证](management_console_operations.md)
- [SQLite 恢复存储维护](recovery_storage_operations.md)
- [历史、PostgreSQL/S3 及切换维护](local_persistence_operations.md)

## 策略接口

- [Strategy ABI V13 typed-state blob 技术设计](strategy_abi_v13_typed_state_blob_technical_design.md)
- [Strategy ABI V13 实施状态](strategy_abi_v13_implementation_status.md)

## 策略设计

- [pair_arb：Slot 化双腿执行内核 V3 需求](pair_arb_strategy_requirements.md)

## 数据协议

- [ResultBundle schema v1](result_bundle_schema.md)

## ABI

仓库策略系统仅支持 Strategy ABI V13；旧 ABI 的运行时 loader、SDK 和迁移文档均已移除。

- [深度快照、可信 Top100 与重同步契约](depth_pipeline.md)：ABI V13 可选能力、容量、策略预热及离线输入；[2026-10-28 验收](verification/2026-10-28/depth/README.md)。
