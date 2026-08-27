# Rust CLI + Numba 重构任务分解

> 来源：[`rust_cli_numba_strategy_refactoring.md`](rust_cli_numba_strategy_refactoring.md)
> 执行原则：任何时刻只维护一份可执行 Runtime；旧实现只生成冻结 golden artifacts，不作为长期兼容路径。

## 里程碑和依赖

| ID | 工作包 | 依赖 | 完成条件 |
|---|---|---|---|
| M0 | 基线与契约冻结 | - | Runtime 符号清单、RunSpec、ABI descriptor、Bundle schema、状态机均有版本化定义 |
| M1 | ABI 与 compiler | M0 | Rust/Python ABI fingerprint 一致；策略可在启动前完成 nopython 编译和错误诊断 |
| M2 | Bar 垂直切片 | M1 | `titan run` 经 controller/worker 完成双均线 Bar 回测，与 golden 一致 |
| M3 | 唯一 Runtime | M2 | Bar/Tick/Hybrid 全部运行于 `titan-runtime`；旧 ctypes Runtime 已删除 |
| M4 | 运行管理 | M3 | registry、detach、stop、heartbeat、崩溃恢复通过故障注入测试 |
| M5 | Live 与策略目录 | M4 | 同一策略可用于 backtest/live；Live secret 不泄漏 |
| M6 | Bundle 与报告 | M3 | Rust 输出冻结 Bundle schema；历史重渲染和 live snapshot 可用 |
| M7 | 删除旧 binding | M5、M6 | `py-hftbacktest`、PyO3 extension、Backend pyclass、ctypes 入口全部删除 |

## M0：基线与契约冻结

- [x] `M0.1` 枚举 `hftbacktest/src/runtime.rs` 与 `py-hftbacktest/src/runtime.rs` 的公开符号和调用方，见 [`runtime_symbol_migration_inventory.md`](runtime_symbol_migration_inventory.md)。
- [x] `M0.2` 对每个符号标注 `move-to-abi`、`move-to-runtime`、`keep-in-engine` 或 `delete`。
- [x] `M0.3` 冻结 Bar/Tick/Hybrid golden 数据、结果和 callback 基线。
- [x] `M0.4` 定义版本化 StrategyManifest 和参数校验错误。
- [x] `M0.5` 定义完整 RunSpec、Backend 判别联合和能力矩阵。
- [x] `M0.6` 定义 controller/worker 协议、owner token、signal 和退出码。
- [x] `M0.7` 冻结 ResultBundle v1 的提交、摘要、排序和时间语义。

## M1：ABI 与 Numba compiler

- [x] `M1.1` 新建 `titan-runtime-abi`，先承载 ABI version、事件槽和 descriptor/fingerprint。
- [x] `M1.2` 将纯 `repr(C)` payload 和 Context 迁入 ABI crate，消除与执行逻辑的耦合。
- [x] `M1.3` 由 Rust 生成包含字段类型、size、alignment、offset、事件 ID 和平台信息的 descriptor。
- [x] `M1.4` Python SDK 根据 descriptor 校验 NumPy dtype，并拒绝 fingerprint/offset 不一致。
- [x] `M1.5` 从 `eventbot.py` 提取 `compile_strategy()`、handler 校验和 `cfunc` bridge。
- [x] `M1.6` 实现结构化编译错误、state 校验和 keepalive descriptor。
- [x] `M1.7` 把双均线改成固定窗口增量 SMA，移除完整未来 closes。

## M2：Bar 垂直切片

- [x] `M2.1` 新建 `titan-python-host`，只提供 embedding compiler adapter。
- [x] `M2.2` 新建 `titan-cli` controller 和隐藏的 `run-worker` 子命令/二进制。
- [x] `M2.3` controller 静态校验 manifest/RunSpec 后 spawn worker，且不初始化 CPython。
- [x] `M2.4` worker 初始化 Python、编译策略、直接构造 Bar Backtest 并运行 callback。
- [x] `M2.5` 明确 GIL 释放、keepalive drop 顺序和 worker 退出码。
- [x] `M2.6` 双均线结果、事件顺序和失败语义与 golden 对齐。

## M3：唯一 Runtime 与 Tick/Hybrid

- [x] `M3.1` 新建 `titan-runtime`，迁移唯一 lifecycle、dispatch、source 和 Context 填充实现。
- [x] `M3.2` 解除 `hftbacktest::backtest::bar` 对 Runtime 执行逻辑的反向依赖。
- [x] `M3.3` Rust worker 直接构造 Tick Backtest 并接入 Tick source。
- [x] `M3.4` 接入 Hybrid source，验证同时间事件确定性顺序。
- [x] `M3.5` 迁移 timer、funding、order/fill/position/error/stop 生命周期。
- [x] `M3.6` 删除 `py-hftbacktest/src/runtime.rs`、ctypes symbols 和 thread-local 结果通道。
- [x] `M3.7` 删除 `hftbacktest` 中已迁出的重复事件循环，并增加单 Runtime CI 检查。

## M4：CLI 与运行管理

- [x] `M4.1` 实现 `run/ls/show/stop/logs` 和 JSON 输出。
- [x] `M4.2` 建立 SQLite WAL registry、事务化状态迁移和 artifact 注册。
- [x] `M4.3` 实现 PID、start-time、owner token 和 heartbeat。
- [x] `M4.4` 前台和 detach 统一使用 spawn-worker；实现 SIGTERM 正常停止。
- [x] `M4.5` 实现 STALE 检测和查询时 reconciliation。
- [x] `M4.6` 覆盖编译失败、owner token、dead worker reconciliation 和 PID 复用测试。

## M5：策略目录与 Live

- [x] `M5.1` 实现 `strategy ls/show/validate/compile`，其中 `ls` 不导入 Python。
- [x] `M5.2` 将固定窗口增量双均线迁入 `strategies/dual_ma`。
- [x] `M5.3` worker 直接构造 LiveBot、connector IPC 和 instrument。
- [x] `M5.4` Live 与回测共用正常 stop 和 Runtime lifecycle。
- [x] `M5.5` RunSpec 只保存 connector 引用，不保存 secret。

## M6：ResultBundle 与报告

- [x] `M6.1` Rust writer 输出冻结 result、manifest 和 SHA-256。
- [x] `M6.2` manifest-last 原子提交完成态 Bundle。
- [x] `M6.3` 新报告包不包含账户、费用、持仓、PnL 和净值重算。
- [x] `M6.4` `titan report --output` spawn 独立只读 Python report process。
- [x] `M6.5` 支持 Native/QuantStats、历史重渲染和 renderer 失败隔离。
- [x] `M6.6` 未知旧 schema 明确拒绝，禁止隐式 partial fallback。

## M7：删除旧入口并验收

- [x] `M7.1` 删除 `_hftbacktest` module、所有 pyclass/pyfunction 和 extension-module feature。
- [x] `M7.2` 删除 Python 创建 Backend、持有 Rust 指针或调用 Runtime 的 API。
- [x] `M7.3` 将纯 Python SDK/报告代码移入独立 package，删除 `py-hftbacktest`。
- [x] `M7.4` 更新 README 和示例，只保留 `titan` 用户入口。
- [x] `M7.5` 运行 Rust/Python 全量测试、golden、故障注入和 live RunSpec dry-run 验收。

## 每个工作包的提交门槛

- 代码和 schema 都有版本化测试；不得依赖人工检查 ABI offset。
- 新路径通过后立即删除被替代入口，不长期维护 feature flag 双路径。
- 不提交 callback 地址、裸指针、凭证或含敏感字段的完整环境变量。
- 每个运行失败都能映射到稳定错误码，并在 CLI 中保留可操作诊断。
- 用户已有未提交修改不属于本重构时必须保持原样。
