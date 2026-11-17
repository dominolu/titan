# cross_exchange_mm 实盘准备工作清单

日期：2026-11-11。基线：`master` `80189496` + 当前工作区未提交改动（策略 `2.0.1`，schema 2）。
依据：[修复与上线准备方案](cross_exchange_mm_repair_plan.md)、[2026-11-01 审查复现](verification/2026-11-01/cross-exchange-mm-review/README.md)、[2026-10-31 发布验收](verification/2026-10-31/cross-exchange-mm/README.md)、[跨所实施计划](cross_venue_implementation_plan.md)。

## 结论

**现在不能直接实盘跑，也不是缺一个配置文件的问题。** 策略侧阶段 A 的修复已落在工作区、尚未在构建服务器验收；真正的硬阻断是平台阶段 B：受控实盘 `execution_limits` 路径仍写死单市场/单账户，双市场双账户准入、逐所保证金预占、`FlattenOwned` 停止目标都不存在。按仓库约定，不得通过关闭 `execution_limits` 绕过门禁上线。

因此本清单分为：可在本机/构建服务器推进的 A、C、D，以及必须改 Rust 平台的 B。B 是实盘的前置条件，A/C/D 可以并行先行，但任何一项通过都不代表可以实盘。

## 与其他清单的分工

本清单覆盖框架与交付流程；**策略自身**的代码、观测、内嵌场景与版本决策见
[cross_exchange_mm 策略自身待办清单](cross_exchange_mm_strategy_tasks.md)；
单位/对象建模见[市场标的与账户对象设计](market_symbol_and_account_object_design.md)。
三份清单的任务编号互不复用。

## 状态口径

- `待做`：未开始。
- `进行中`：已开始、未验收。
- `本机可完成`：只改源码/文档/模板，验收在构建服务器。
- `构建服务器依赖`：必须在 `titan-build` 编译或跑测试，本地不出证据。
- 任何项完成都必须有新的验证证据；历史测试数量不作为新修改已通过的证明。

## A 策略侧（单文件策略、专项测试、内嵌场景、产物冻结）

| 编号 | 工作 | 交付物 | 验收标准 | 依赖 | 状态 |
| --- | --- | --- | --- | --- | --- |
| A1 | 复核工作区 `2.0.1` 改动：持续安全检查 `quote_safe`、`business_result` 正故障码、`BusinessObservationSpec`、能力位 `BUSINESS_FAULT_DRAIN`/`REQUIRES_PERIODIC_TIMER` 与四个审查探针一一对应 | 本文件"审查结论"小节 + 问题清单 | 四个探针各有对应测试名；无新的状态/截止期漏洞 | 无 | 已完成（本机审查） |
| A2 | 补内嵌 `TESTS` 关键原生场景：限频内金额超限撤单、最小对冲不可执行撤单、业务故障正码、撤单竞态尾量 | `strategies/cross_exchange_mm.py` | `titan strategy test` 全部场景通过，且不与专项测试重复计算 | A1 | 已完成（待 A3 验收） |
| A3 | 构建服务器验收：SDK 回归、专项 40 项、原生编译/回放、bundle 编译与校验 | [脱敏摘要](verification/2026-11-11/cross-exchange-mm-phase-a/README.md) + 私有原始日志 | 全绿；源码/测试摘要固定 | A1,A2 | 已完成（2026-11-11） |
| A4 | 冻结 `2.0.1` 产物：`titan strategy compile` 生成 bundle，记录 manifest 与原生库摘要 | `artifacts/cross_exchange_mm/*.titan`（不入 Git）+ 私有摘要清单 | `validate` 通过；`source_digest`/`parameters_digest` 稳定 | A3 | 已完成（未签名）；注意产物不可字节复现，见验收页 |

## B Rust 平台（实盘硬阻断，按修复方案 R04–R07）

| 编号 | 工作 | 主要位置 | 验收标准 | 依赖 | 状态 |
| --- | --- | --- | --- | --- | --- |
| B1 | 设计冻结：多绑定受控执行配置 schema（逐 `(account, asset, position_side)` 限额、maker/hedge 角色、delta 比例、共同报价单位、保证金域、行情年龄、命令用途、停止预算），与旧 `execution_limits` 互斥 | 文档 + schema 定义 | 与修复方案 §4/§6 一致，能力协商与版本规则明确 | 无 | 已完成（设计冻结） |
| B2 | 多绑定配置类型 + 互斥/能力强制 + 旧宿主拒绝新能力 | `crates/titan-strategy-runtime/src/execution_binding.rs` | 8 项单测覆盖互斥、能力缺失、整数比例、停止预算、未绑定腿、重复腿 | B1 | 已完成（待 B9 整体验收） |
| B3 | 逐市场行情状态（`last_bbo_ns` 等按市场）+ 共用风险计算器（Q_min/Q_max、逐所最坏持仓/金额/保证金） | runtime_v13 | 行情年龄按资产分别记录并有回归测试 | B2 | 部分完成：逐资产行情年龄已实现；共用 Q_min/Q_max 计算器与离线执行器接入未做 |
| B4 | 命令用途证明：成交先于持仓时可合法 hedge，替换全局 `validate_opening` 阻断 | `execution_binding.rs` + `runtime_v13.rs` + SDK `submit_order(purpose)` + 策略 `2.0.2` | 未知用途/角色错配/超量/反向/缺独立成交全部拒绝；仅缩小已证明敞口放行 | B3 | 已完成（2026-11-11，见 [B4 验收](verification/2026-11-11/cross-exchange-mm-b4-command-purpose/README.md)） |
| B5 | 逐所保证金/救援预占：maker 提交前预留 hedge 资源，持久化与恢复不重复/不回退 | `execution_reservation.rs` + 运行时准入/对账/重建 + account-service 保证金投影 | 崩溃点矩阵通过 | B3,B4 | 部分完成：双方向预占、救援上限、恢复重建、跨进程崩溃矩阵、保证金投影函数均已实现；**运行时准入尚未调用保证金投影**（与 B8 同批构造点改动），见 [B5 首轮](verification/2026-11-11/cross-exchange-mm-b5-reservation/README.md)、[B5-剩余](verification/2026-11-11/cross-exchange-mm-b5-reservation-rest/README.md) |
| B6 | 业务故障排空收尾：宿主区分正业务故障码与回调失败，先保证安全撤单提交再进入失败态 | runtime_v13/service_core | 故障撤单不丢失，最终 `FAILED` 保留原因 | 工作区已有初版 | 部分完成（撤单保留已改，待 B9 验收） |
| B7 | `FlattenOwned` 停止目标 + 留仓占用移交 + 停止期限预算 | runtime + CLI 管理入口 | 留仓与零仓分别证明；失败不释放占用 | B5,B6 | 待做 |
| B8 | 管理入口双账户/双市场绑定预检与执行模板 | `titan-cli` management/control + 运行时保证金准入 | 非法绑定拒绝；模板与策略参数一致 | B2,B3 | 部分完成：保证金投影已接进派单前准入、管理入口对双绑给出明确拒绝并禁止配置覆盖包络；**多腿绑定 schema 与执行模板未做**，见 [B8 验收](verification/2026-11-11/cross-exchange-mm-b8-margin-admission/README.md) |
| B9 | 构建服务器验证：编译/Clippy/单测 + 离线执行器一致性 + 双所模拟与故障矩阵 | `titan-build` 隔离目录 | 证据包完整 | B2–B8 | 待做 |

## C 部署与运维

| 编号 | 工作 | 交付物 | 验收标准 | 依赖 | 状态 |
| --- | --- | --- | --- | --- | --- |
| C1 | 双市场双账户运行配置样例：账户/行情源/绑定/风险/`timer_interval` 必填、凭据只引用不内联、默认禁用 | `configs/cross_exchange_mm.example.toml` | 配置可解析；缺 timer 或单市场风险配置被拒 | B8 | 已完成（模板，实盘仍待 B8） |
| C2 | 部署模板：`runtime.toml`、systemd unit、`prepare_server.sh`、README，默认禁用并标注只在 shadow 使用 | `deploy/cross_exchange_mm/` | 部署路径/摘要校验/日志目录与现有模板一致 | C1 | 已完成（模板，shadow 用） |
| C3 | 启动预检：账户/市场身份、线性 NET、PostOnly/IOC 能力、tick/lot/乘数/最小约束、共同报价币、timer、期限预算 | `scripts/cross_exchange_mm_preflight.py` + 6 项单测 | 非法配置拒绝启动；缺 FX 拒绝 | C1,B8 | 静态部分已完成；原生元数据/资金项待 B8 |
| C4 | 实盘只读验收适配：双账户余额/持仓/挂单/费率只读核对 | 脚本 | 只读、无下单；证据脱敏 | C3 | 待做 |
| C5 | 停止与风控参数表：撤单/hedge/退出/确认/持久化预算 <= 宿主停止预算 | [停止与风控预算表](cross_exchange_mm_stop_budget.md) | 期限预算可核对；不超过管理路径 10 秒约束 | B7 | 已完成推导（当前默认参数被判不可行；数值待 B7/B9 验证） |

## D 交易所与数据/回测

| 编号 | 工作 | 交付物 | 验收标准 | 依赖 | 状态 |
| --- | --- | --- | --- | --- | --- |
| D1 | 交易所能力矩阵：okx / hyperliquid / binancefutures / backpack / lighter / variational / gatecrossex × PostOnly、IOC、独立成交、持仓模式、保证金模式、最小量/最小金额 | 文档矩阵，标注证据等级 | 每格有来源；未验收明确标未验收 | 无 | 已完成（文档矩阵） |
| D2 | 候选 A/B 配对与原生元数据核对：同经济资产、线性 NET、整数 lot 比例、共同报价/抵押币、最小成交可对冲 | 配对结论 + 核对清单 | 非法组合明确拒绝；比例用精确整数证明 | D1 | 部分完成（候选与核对清单；原生元数据待实机验证） |
| D3 | 行情录制计划：两所同资产、时长/市场数、质量门禁、现有数据盘点 | 计划 + 数据清单 | 能支撑 D4/D5；不把合成回放当收益证据 | D1 | 已完成（计划与盘点） |
| D4 | 双所独立撮合回测：逐所时延/队列/IOC 部分成交/费用/funding，结果可重复 | 回测 harness 改造 | 无未来数据；可复现 | D3 | 待做 |
| D5 | 成本敏感性与样本外报告：净/毛收益、费用、funding、成交率、回撤、单所保证金缓冲、裸露峰值/时长、救援次数、残留尾量 | 报告 | 与平台账本独立核对 | D4 | 待做 |

## 审查结论（A1，2026-11-11）

对工作区 `2.0.1` 改动逐行复核后，发现**三个会直接阻断编译/运行的问题**和一个测试标定问题，
已在本轮修复，待构建服务器验收：

| 编号 | 问题 | 证据 | 处理 |
| --- | --- | --- | --- |
| A1-a | SDK 静态校验只允许字面量返回 `0/-1..-4`，策略的 `return drive(ctx)` / `business_result()` 一律编译失败 | `python/titan-strategy-sdk/titan_strategy/static_compiler.py` 的 `_validate_handler_ast` | 声明 `BUSINESS_FAULT_DRAIN` 时允许计算返回；未声明的策略仍拒绝 |
| A1-b | `MM_OBSERVATION` 5 个字段声明 `uint64`，但状态字段是 `int64`，Rust 观测校验会拒绝整个观测定义 | `strategies/cross_exchange_mm.py` 的 `MM_OBSERVATION`；Rust `observation.rs` 的 kind 匹配 | 改为 `int64`（不改状态布局，仍 `schema 2`） |
| A1-c | 业务故障回调中只要有非 reduce-only 提交，宿主整批丢弃：同回调暂存的撤单一起丢失，直接 `Failed` | `runtime_v13.rs` 故障分支在 `commit_commands` 前返回；`offline_v13.rs` 同构 | 改为只丢弃新风险提交，保留撤单/reduce-only 并正常校验提交；故障原因仍锁存 |
| A1-d | 新增对冲预算测试用 `max_hedge_notional_tick_lots=50050`，初始报价就会被新检查拒绝（5×10015=50075），测试期望 2 笔提交不成立 | `python/titan-strategy-sdk/tests/test_cross_exchange_mm.py` | 标定改为 50080（初始通过、行情 +10 后越界触发撤单） |
| A1-f | 同一源码连续两次编译得到不同 bundle 字节与 `compiled_strategy_digest`，产物不可复现 | 构建服务器两次编译对比 | 已在验收页与工程记忆中记录；源码身份改用 `source_digest`+`parameters_digest` |
| A1-e | 内嵌 TESTS 的唯一场景断言 `context.last_error_code == -2`，而该字段每步被清零，且 `on_start` 只返回 -1 | `strategies/cross_exchange_mm.py` TESTS | 改为断言 `mm.error == 101`，并新增预热报价与限频内预算撤单两个原生场景 |

其余确认（未修改）：

- 持续安全检查 `quote_safe()` 同时用于新报价（`quote_side`）与已有挂单（`drive` 每次事件），
  使用未释放剩余量、两笔挂单合并最坏仓；`max_unhedged_lots` 仍只在 `drive_hedge` 事后锁存。
- 尾量处置仍缺：没有策略侧 reduce-only 回退、没有残留/人工处置观测字段（`S5/S6`）。
- `on_start`/`on_recover` 仍返回 -1，宿主明确把 Start/Recover 排除在业务故障排空之外（`S8`）。
- 平台侧 P0-P2（单市场门禁、逐市场行情状态、`validate_opening`、预占、`FlattenOwned`）见 B 节，
  本次未改，属于阶段 B。

## 里程碑

1. **M1（本机/构建服务器，可立即推进）**：A1–A4、C1–C2、D1–D3。
   - 已完成：A1 审查 + A1-a/A1-b/A1-c 修复、A2 内嵌场景重写、C1 配置样例、C2 部署模板、
     C3 静态预检脚本、D1–D3 文档。
   - 待完成：A3 构建服务器验收、A4 产物冻结。
2. **M2（平台设计）**：B1 冻结，B2–B3 起步，C3–C4 初版。
   - 已完成：B1 设计冻结（[多绑定执行设计](cross_exchange_mm_multi_binding_design.md)）。
3. **M3（平台实现与验证）**：B4–B6 + B9 首轮证据。
4. **M4（停止与准入收口）**：B7–B8 + 双所模拟/故障矩阵。
5. **M5（收益证据）**：D4–D5。
6. **M6（受控实盘）**：冻结产物 + 明确授权 + 两所账户/品种/数量/保护价/敞口/期限/停止目标，最小风险验收。

## 本轮进展与阻塞（2026-11-11）

已完成并在 `titan-build` 出证据：A1-A4、B1、B2、B3（行情年龄部分）、B4、C1、C2、C3、C5、D1-D3；
B5 完成双方向预占、恢复重建与跨进程崩溃矩阵；B8 前半把账户级保证金投影接进了运行时派单前准入。
验收页：[阶段 A（2.0.1）](verification/2026-11-11/cross-exchange-mm-phase-a/README.md)、
[B4 提交用途（2.0.2）](verification/2026-11-11/cross-exchange-mm-b4-command-purpose/README.md)、
[B5 预占（部分）](verification/2026-11-11/cross-exchange-mm-b5-reservation/README.md)、
[B5-剩余：保证金投影与崩溃矩阵](verification/2026-11-11/cross-exchange-mm-b5-reservation-rest/README.md)、
[B8：保证金准入（部分）](verification/2026-11-11/cross-exchange-mm-b8-margin-admission/README.md)。

仍在清单上、且**不能在本轮诚实标记完成**的项，按阻塞类型分三类：

| 阻塞类型 | 项目 | 说明 |
| --- | --- | --- |
| 平台实现（可在构建服务器验证，但属于成体系的平台改造，需独立实现+验收轮次） | B8（管理侧一半）、B7、B9 | B8 还差多腿绑定受控配置（约 12 个新校验字段）与管理执行模板，会改动当前唯一可用的单腿受控路径；B7 `FlattenOwned` 全仓库不存在；B9 是上述整体验收（含离线执行器接入）。这些不是配置或文档工作，半成品会给出错误的安全印象。 |
| 依赖跨所实盘数据与回测改造 | D4、D5 | 需要先按 D3 在两所同时录制（目前只有同一交易所 8 小时的私有数据，且质量 FAIL），再改造 `hftbacktest` 的多资产 V13 驱动。 |
| 需要实盘只读授权 | C4 | 双账户余额/持仓/挂单只读核对必须在实盘服务器执行，需要明确的账号与授权；本轮未获授权。 |

**因此当前结论不变：不能实盘运行 cross_exchange_mm。** 硬阻断是 B4/B5/B7/B8 与单市场门禁，
而不是缺配置或文档。

## 执行顺序说明

A 与 D1/D3 不依赖平台，先行；C1/C2 模板可先写但需标注"在 B8 前不可用于实盘"；B 是唯一能把"不能实盘"变成"能实盘"的路径，按 B1→B9 顺序推进，每一步在构建服务器出证据。

