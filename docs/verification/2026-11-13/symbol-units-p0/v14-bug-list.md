# V14 ABI 遗留缺陷清单

> 2026-11-16 复核结论。已修复项见[验收矩阵](v14-acceptance-matrix.md)末尾“复核与修复”一节；
> 本文件只登记**当前代码与本地能力范围内无法修复**或**需外部条件才能验证**的缺陷。
> 不改写历史验收结论，每项注明阻塞原因与解除条件。

## 阻断级别定义

- **P1** — 影响策略正确性/可用性，无外部条件无法闭环。
- **P2** — 功能已按决策降级或删除，留有文档/互操作缺口。
- **P3** — 文档、命名或覆盖度问题，不阻断运行。

## 一、需要外部条件才能修复或验证

| # | 级别 | 缺陷 | 当前代码事实 | 阻塞原因 | 解除条件 |
| --- | --- | --- | --- | --- | --- |
| 1 | P1 | `account_modes` 恒为空，账户持仓/保证金模式永远是 `Net`/`Unknown` | `service_core::build_object_table` 传 `account_modes: &[]`；`object_sources.rs:135-139` 在无条目时回落 `(PositionModeV14::Net, MarginModeV14::Unknown)` | `AccountService` trait 无**同步**模式访问器；唯一数据源 `get_account_position_mode`/`get_instrument_position_mode` 是 connector 层**异步**方法，而对象表在同步的 `StrategyServiceCore` 路径构建 | 要么给 `AccountService` 增加同步模式快照（需连接器在握手阶段缓存模式），要么把对象表构建改为异步。两者都是跨 crate 的架构改动，不在本轮范围内 |
| 2 | P1 | 在线元数据源未接线（OKX/Binance 真实证据缺失） | `metadata_source` 已有 Gate/Hyperliquid 的可用 transport；OKX/Binance 在该构建机上 DNS 解析失败 | 构建机无外网到这些交易所 | 在可访问的网络机器上补真实拉取证据 |
| 3 | P1 | 跨场所真实双 `runtime.toml` 启停端到端缺失 | 仅离线证得“同产物两套单位派生同一比例”（`cross_venue_object_table.rs`） | 需要联网账户与实盘服务器授权 | 在远程实盘服务器执行受控启停；未经授权不得进行 |
| 4 | P2 | 离线回放端到端 -2 未修复 | 根因已定位为策略侧 `quote_currency_no` 与离线视图占位不一致 | 回放功能已按用户决策删除（矩阵第 7/13 项 ➖） | 仅当重新引入回放时才有意义 |
| 5 | P2 | 生产签名已移除 | 完整性改由 `native_library_sha256`/`compiled_strategy_digest`/`target_triple` 三重校验 | 需用户私钥，且已有明确决策移除 | 用户提供受控签名密钥并重新决策 |

## 二、代码事实与文档/约定不一致（可修但不属于本轮范围）

| # | 级别 | 缺陷 | 说明 |
| --- | --- | --- | --- |
| 6 | P3 | `status` payload 的 `last_flags`、`error_category`、`stop_deadline_ns` 从未被宿主写入 | `status_post_callback` 只写 `last_return_code`/`last_error_code`；这三个字段恒为 0。要填 `error_category` 需把 `StrategyErrorKind` 映射为 ABI 稳定枚举（新增跨语言契约），需单独设计 |
| 7 | P3 | `StrategyAccountFees` 未同步到 Python SDK 的编译/校验侧 | Rust 侧新增的 TOML 字段只在运行时消费；`static_compiler.py`/`definition.py` 不感知费率声明。当前不影响编译产物（费率不是策略参数），但若将来要在编译期做费率健全性检查需要补 |
| 8 | P3 | `StrategyRuntimeDiagnosticSnapshot` 未展示 fee/account-mode 来源 | 运维无法从诊断快照区分“未声明费率”与“声明了但被拒”；仅靠 `requires_fee` 的拒绝原因码判断 |

## 三、本轮未触碰（既有直传存在，与 V14 无关）

- `internal-docs/` 下的历史审查副本与工作区不完全同步；按隐私约定不得回写公开文件。

## 复现与验证入口

- 全量验证：`scripts/verify.sh cargo test --workspace --all-targets --all-features`
- V14 专项：`cargo test -p titan-strategy-runtime --lib fee_`、`python -m unittest discover -s python/titan-strategy-sdk/tests -p "test_v14*.py"`
- Python 需要 `TITAN_STRATEGY_PYTHON` 指向装有 numba/llvmlite 的解释器；`TITAN_STRATEGY_SDK` 指向 SDK 根。注意：用 `PYTHONPATH` 覆盖解释器搜索路径会遮蔽该解释器自身的站点包（表现为 `ModuleNotFoundError: llvmlite`），跑编译路径的用例时改为把仓库根追加在站点包之后。
