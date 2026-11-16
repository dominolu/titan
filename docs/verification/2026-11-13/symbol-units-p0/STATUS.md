# 符号/账户对象（V14）实施状态

> 快照时间：2026-11-16，第 49 轮（OKX/Binance transport 先行实现）。本文件只陈述**当前工作区**的事实，
> 不引用历史验收结论。逐轮证据见同目录 `README.md` §3.x，逐项矩阵见 `v14-acceptance-matrix.md`。

## 1. 门禁现状（titan-build，当前工作区）

| 门禁 | 结果 |
| --- | --- |
| `cargo fmt --all -- --check` | 通过 |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | 0 warning |
| `cargo test --workspace --all-targets --all-features` | **968 passed / 0 failed / 32 ignored**（ignored 含 3 项需出网的真实拉取） |
| `cargo test --workspace --all-features --doc` | 2 passed / 1 ignored |
| SDK `unittest discover` | **189 项通过，1 项显式跳过**（删除 trace 回放用例后） |
| 守卫脚本 | `check_single_runtime.sh`、`check_no_retired_hftbacktest_strategy.sh`、`check_strategy_no_exchange_facts.py`、`check_repository_privacy.py`、**`check_script_cli_commands.py`**、**`check_doc_links.py`** 全部通过 |

## 2. 已完成

**P0（11/11）**：DecimalUnit 严格化、绑定单位、统一校验器、`instruments verify`、preflight 收敛、
loader 原因码、跨语言布局守卫、离线恢复、`parameters_digest` pin、connector 精确元数据、离线快照 + trace 升版。

**P1（主体）**：V14 布局/指纹/跨语言 schema；`ObjectTable` + 适配器 + 服务层按实例建表/挂载/两层门禁；
ABI 14 一次性切换（常量/loader/descriptor/manifest/运行时 440B root/编译器/SDK 同批）；`native-v14` 迁移；
trace schema 4；快照 provider；status 生命周期接线；恢复身份折入 `binding_digest`。

**P2（3/3）**：`cross_exchange_mm`/`pair_arb`/`grid` 的交易所事实参数全部删除并改读对象表；
运行诊断暴露 `object_identity_digest`/`metadata_snapshot_digest`。

**P3**：hftbacktest `strategy` 模块退役（P3-01）；文档与记忆同步（P3-03）；`tick_size/lot_size`
弃用占位单一写入点（P3-02 部分）。

**矩阵**：✅22 / 🟡2 / ⬜1（第 6、7 为 🟡，第 13 为 ⬜；第 7/13 属用户确认的已知限制；第 3 项已以离线“同产物两套单位派生同一比例”取证，真实双 `runtime.toml` 启停待联网账户）。

**过程中发现并修复的真实缺陷**：
`ObjectTable::status()` 在空表上读取零长度 arena 导致 SIGSEGV（第 40 轮）；
`instrument_by_symbol` 身份键错配（第 10 轮）；
`context_v14` refs 长度分支赋值不传播到 packed shape（第 8 轮）；
`static_compiler` 未注册 V14 accessor（第 18 轮）。

## 3. 未完成（发布阻断，全部需外部条件）

| # | 阻断项 | 缺什么 |
| --- | --- | --- |
| A | ~~矩阵第 16 项：产物仍是 ABI 13~~ | **已完成（第 42 轮）**：6 个产物重编为 ABI 14，`validate` 全部通过，`expected_digest` 已同步；ABI 13 负例返回 `artifact 13, host 14; recompile`。生产签名另需用户私钥与 `key_id` |
| B | ~~矩阵第 6 项：在线元数据拉取~~ | **已完成（第 46 轮）**：HL 公开 `/info` 生产 transport + 配置入口 + 真实拉取证据（235 资产、约 1.3 s）。仅剩 OKX/Binance 因出网阻断无真实证据 |
| C | ~~矩阵第 3 项：跨场所同产物启动验证~~ | 离线部分已取证；真实双配置启停依赖交易所，随第 6 项一并搁置 |
| D | ~~矩阵第 7/13 项：离线回放端到端~~ | **已按用户决策删除功能**（第 45 轮）：CLI trace 回放、配置与轨迹夹具一并移除；`OfflineV13Adapter` 保留供真实历史回测使用 |
| E | ~~P3-02 剩余~~ | **已完成**：第 42 轮收敛 `reserved` 语义与能力位表达；第 44 轮删除 `StrategyRuntimeContextV13` 并把 V14 改名为无后缀的 `StrategyRuntimeContext`，守卫改为"恰好一个定义、禁止版本别名"；带版本的旧 ABI 文档已删除 |

## 4. 目标完成度判定

**目标尚未完成。** 依据：

1. 计划 P1-06/P1-07/P1-10 与矩阵第 6/16 项未通过：第 16 项是**工程待办**（重编为 ABI 14，可离线完成），
   第 6 项与第 3 项的真实启停部分缺授权；本轮已把第 3 项转为离线取证（✅）、第 6 项错误路径补齐；
2. 计划 P3-02 要求“清理 V13 兼容占位”：删除类改动需另一次 major，但语义注释、能力位合并、模块改名属可做项，
   当前只完成常量收敛部分；
3. 离线回放虽经用户决策降级，但仍是矩阵中的已知限制，不构成“已验收”。

上述任何一项都不能通过修改文档或放宽断言消除，因此本轮不标记目标完成。

## 5. 解除阻断所需的最小条件

| 条件 | 解锁项 |
| --- | --- |
| 在 `titan-build` 重编 6 个产物为 ABI 14（无需密钥）；生产签名另需用户私钥与 `key_id` | 矩阵第 16 项、`titan.lock` 更新、`expected_digest` 同步 |
| 授权访问交易所公开元数据端点（或提供一份权威快照） | 生产 transport 接线、矩阵第 6 项与第 3 项的真实双配置启停（错误路径已有 9 项单测） |
| 新的 major 决策 | P3-02 剩余清理 |
| 单独立项 | 离线回放重建 |
