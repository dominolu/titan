# cross_exchange_mm 多绑定受控执行设计（B1 冻结稿）

日期：2026-11-11。对应工作清单 B1（[工作清单](cross_exchange_mm_live_worklist.md)），
依赖修复方案 §4/§6/§7（[修复方案](cross_exchange_mm_repair_plan.md)）。
状态：**设计冻结；B2、B4、B3 的行情年龄部分与 B5 的数量/名义预占已实现并通过单测，其余未实现**。
实现 B5-B9 必须按本文与修复方案验收矩阵产出证据；本文不授予任何实盘操作权限。

已实现（`crates/titan-strategy-runtime/src/execution_binding.rs`）：

- `MultiBindingExecutionLimits` / `ExecutionLegLimit` / `RescueBudget` / `StopBudget` 类型，
  与旧 `ExecutionLimits` 通过 `execution_binding_ambiguous` 互斥。
- 精确整数 lot 比例：`LotRatio::exact` 用十进制 coefficient/scale 做整数约分，
  非整数比例拒绝为 `execution_binding_lot_ratio`，不使用浮点近似。
- 定义加载期（`validate_definition`）校验形状；运行时创建期（带 manifest 能力）再校验
  `MULTI_VENUE_EXECUTION`，缺能力返回 `execution_binding_capability_missing`。
- 逐市场行情年龄：`NativeV13Inner::last_bbo_ns` 由单个 `i64` 改为按 `local_asset_no` 的
  `BTreeMap`，估值包络按请求资产取自己的时间戳；另一条腿的新鲜行情不能证明本腿新鲜。
- 停止预算总和必须不超过宿主 `stop_timeout`，否则 `execution_stop_budget_exceeds_host`。

已实现（B4，`runtime_v13.rs` + `execution_binding.rs` + SDK `submit_order(purpose)`）：

- `CommandPurpose` 复用 `TitanSubmitOrderRequest.reserved[0]`（0 未声明、1 maker 报价、
  2 已知成交对冲、3 救援退出、4 自有持仓退出），ABI 布局不变。
- `commit_commands` 每次派单前校验：未知用途拒绝；有包络时按绑定角色复核
  （maker 腿只收 1、hedge 腿只收 2、退出用途必须 reduce-only、未绑定腿拒绝）；
  无包络（旧单市场路径）时用途只是声明，不授予任何放宽。
- `validate_opening` 改为用途感知：只有 `KnownFillHedge` 能在持仓未确认时放行，且必须
  同时满足"没有仍缺独立成交事件的填充"和"缩小已证明的未对冲敞口"（`hedge_reduces_proven_exposure`）。
  超量、反向、未声明用途仍返回 `execution_inventory_unproven`。

已实现（B5，`execution_reservation.rs` + 运行时准入/对账/恢复重建）：

- maker 派单前按 `剩余手数 × 整数lot比例` 预占 hedge 手数与最坏保护价名义，超额整批不派发。
- 撤单不释放；`CANCEL_PENDING`、非终态、以及终态但仍有待补独立成交的订单继续占用。
- 结算幂等（`settle(order_id, cumulative, price)`），已成交部分的 hedge 义务仍占用，
  只有 hedge 单自身终态证明才释放。
- 救援额度受数量与名义双上限约束，运行期不重置。
- 恢复时按非终态、非 reduce-only 订单重建预占；救援消耗未持久化，故存在未终结订单时
  按"已用尽"fail-closed 处理，宁可拒绝救援也不退款。

补充（B5-剩余）：

- 预占改为**双方向**：maker 腿按 `手数 × 比例`、hedge 腿按自身手数占用同一份 hedge 预算，
  报价到对冲交接过程中总预占不会塌陷；恢复时两条腿的非终态、非 reduce-only 订单都重建。
- account-service 提供逐账户**保证金投影**：`collateral_units` 按显式 `margin_rate_bps`
  （配置项，1..=100000，默认全额）换算抵押单位并向上取整；`margin_projection` 用
  `available_units` 给出 headroom。余额缺失/为零/为负一律拒绝，不同账户的盈余互不抵扣。
- **跨进程崩溃矩阵**已覆盖 4 个崩溃点（未派发 / 首单持久化 / 全部持久化 / 成交仅在内存），
  重建后预算精确等于已持久化义务，救援额度只会被判为已用尽。

已实现（B8 前半，保证金接入运行时准入）：

- `StrategyRuntimeBuildContext.accounts_service` 注入账户服务；创建期按腿取 `(instrument, currency)`
  单位，缺服务/账户/资产/货币直接拒绝创建（`execution_margin_unproven`）。
- 派单前逐腿用「已预占 own 名义 + 本次提交名义」对本账户 `available_units` 做投影，
  不覆盖 `execution_margin_insufficient`，余额未知 `execution_margin_unproven`；不跨账户抵扣。
- 管理入口对双市场/双账户策略给出明确拒绝，并禁止运行配置自带执行包络。
- 澄清：`execution_binding_unproven` 只在配置旧 `execution_limits` 时触发；配置 `multi_binding`
  时运行时已不再被单市场门禁挡住，剩余阻断在受控管理准入。

尚未实现：管理入口的多腿绑定受控配置与执行模板（约 12 个新字段，会改动当前唯一可用的单腿路径）、
把 `rescue` 接到实际退出路径、`FlattenOwned`、离线执行器接入（B9）。

## 0. 目标与不变量

把受控实盘执行从"单市场 + 单账户"扩展为"每策略 N 条腿（每腿 = 一个 (账户, 资产) 绑定）"，
同时保留现有安全性质：

1. 平台保留最终准入权；策略只做业务快速检查。
2. 任何可能成交的单腿都必须被预占覆盖；跨所盈利不得抵消另一所保证金或清算缓冲。
3. 在线宿主、离线执行器、恢复路径共用同一套计算与规则。
4. 旧配置与旧产物继续走旧路径；旧宿主必须拒绝自己不理解的新能力。
5. 未知结果、在途订单、CancelPending、等待成交确认的终态订单都占用预算。
6. 业务故障不丢弃同回调内的合法安全命令（撤单/reduce-only）。

## 1. 配置 schema（B2）

新增独立结构，与旧 `ExecutionLimits` **互斥**（`StrategyRuntimeSpec` 中二者只能有一个）：

```rust
pub struct MultiBindingExecutionLimits {
    pub revision: u64,                    // 风险配置修订，进入恢复身份
    pub quote_currency: u32,              // 共同报价/抵押币，必须有平台可证的同币关系
    pub legs: Vec<ExecutionLegLimit>,     // 至少 2 条：maker 与 hedge
    pub rescue: RescueBudget,             // 事先授权的有界救援预算
    pub stop: StopBudget,                 // 各阶段期限，<= 宿主总停止预算
}

pub struct ExecutionLegLimit {
    pub local_account_no: u32,
    pub local_asset_no: u32,
    pub role: LegRole,                    // Maker | Hedge
    pub base_delta_per_lot: DecimalUnit,  // 精确十进制/整数，不用浮点
    pub min_quantity_lots: i64,
    pub min_notional_quote: DecimalUnit,
    pub max_position_lots: i64,
    pub max_notional_tick_lots: i128,
    pub max_market_age_ns: i64,
    pub margin_domain: u32,               // 不可跨域抵扣
}

pub enum LegRole { Maker, Hedge }
pub struct RescueBudget { pub max_quantity_lots: i64, pub max_notional_tick_lots: i128,
                          pub max_cost_bps: u32, pub deadline_ns: i64 }
pub struct StopBudget { pub cancel_ns: i64, pub hedge_ns: i64, pub exit_ns: i64,
                        pub confirm_ns: i64, pub slack_ns: i64 }
```

准入规则（加载期校验，失败拒绝启动）：

- `legs.len() >= 2` 且 maker/hedge 角色各至少一条；同 `(account, asset)` 不得重复。
- 两条腿的 `quote_currency` 满足同一经济资产与同一抵押币；`base_delta_per_lot` 比例必须为
  正有理数且与策略声明的整数 lot 比例一致（首版只接受整数比例）。
- 与旧 `execution_limits` 同时出现 -> `execution_binding_ambiguous`。
- 多市场绑定必须声明能力 `MULTI_VENUE_EXECUTION`；未声明却绑定 2 条以上腿 ->
  `execution_binding_unproven`（保留现有错误码语义）。
- 停止预算求和不超过宿主 `stop_timeout`；管理路径总上限 10 秒仍优先，超出拒绝。

能力与版本：

- `MULTI_VENUE_EXECUTION` 现在是死位（`runtime_v13.rs` 无行为）。B2 起它必须**门禁**多腿配置，
  并由 `compiled_strategy.rs` 的 native->runtime 映射强制；旧宿主遇到未知能力字符串已会拒绝加载。
- `BUSINESS_FAULT_DRAIN`、`REQUIRES_PERIODIC_TIMER` 已在工作区实现（未验收）。
- 状态布局新增故障阶段/用途/预算字段时，策略发布 `2.1.0 / schema 3`；仅修 quote 分支的阶段 A
  保持 `2.0.1 / schema 2`。

## 2. 逐市场行情状态（B3）

- `NativeV13Core`/`NativeV13Inner` 的 `last_bbo_ns: i64` 改为按 `local_asset_no` 索引的
  `BTreeMap<u32, i64>`（或固定长度数组），由对应的 market source 更新；禁止用 maker 行情证明
  hedge 行情新鲜。
- 价格视图已按 `asset_no` 索引（`runtime_v13.rs`），需核对 `local_market_no` 与 `asset_no` 的
  映射，避免 leg 配置用 `local_market_no`、状态用 `asset_no` 时错位。
- 行情年龄检查从"全球一个年龄"改为逐腿 `max_market_age_ns`。

## 3. 共用风险计算器（B3/B4）

在 `titan-account-service` 提供唯一实现，供宿主准入、账户监控、离线执行器与恢复共用：

```rust
pub struct LegRisk { pub q_min_lots: i128, pub q_max_lots: i128,
                     pub worst_notional_tick_lots: i128, pub margin_domain: u32 }
pub fn leg_intervals(positions: &[...], active: &[...], pending_fills: &[...]) -> Vec<LegRisk>;
```

- `Q_min = P - 未释放卖单剩余量`，`Q_max = P + 未释放买单剩余量`；
- 未释放量包含本回调暂存命令、未知单、CancelPending、终态等待成交确认；
- 按精确基础币比例求双腿 delta 区间与逐所最坏持仓/名义/保证金；
- 跨账户按确定的账户锁顺序一次核对并预占，再持久化、派单；
- 离线执行器 `offline_v13.rs` 目前完全没有风险路径，必须调用同一计算器。

## 4. 命令用途与"成交先于持仓"的合法对冲（B4）

`TitanSubmitOrderRequest` 保留 2 字节 `reserved`，不改变 ABI 布局：

- `reserved[0] = CommandPurpose`：`MakerQuote=1, KnownFillHedge=2, RescueExit=3, OwnedExit=4`；
- 用途由**预先绑定的执行角色**复核：只有 role=Hedge 的腿才接受 `KnownFillHedge`；
  maker 腿只接受 `MakerQuote`；退出类用途要求持仓/活动预占证据并置 `reduce_only=1`。
- 用途不能由策略自由声明绕过风控；未知用途拒绝。
- 替换 `InventoryProof::validate_opening` 的全局阻断：已知成交的 hedge 允许在持仓推送滞后时
  提交，条件是独立成交已证明、原意图与剩余义务匹配、使用未耗尽预占、全部硬限制通过。
- 每个原生最小成交都要可处置：平台从原生元数据核对
  `1 maker lot 基础币 delta = r × 1 hedge lot 基础币 delta`，首版 r 为精确正整数。

## 5. 预占与救援（B5）

- maker 提交前由平台预占该订单可能触发的对冲数量、费用与逐所保证金；
- 预占进入恢复身份（含 revision、期限、代次），跨进程恢复不得重置或回退消耗水位；
- 撤单请求不释放预占；只有已证明终态 + 成交/持仓事实一致才释放；
- 救援额度是事先授权的硬上限，不是亏损后自动加码；超出预算转有界退出或人工处置；
- 尾量处置顺序：停止新增 maker -> 撤 maker -> 用预留额度对冲已知欠量 -> 合并自然到达成交 ->
  原腿 reduce-only 回退 -> 原腿也不可交易或 hedge 结果未知时暴露残留并请求人工处置。

## 6. 业务故障排空（B6，部分已实现）

工作区已实现：策略返回 1..=65535 时宿主锁存 `business_fault`、进入 `Stopping`、发布观测、
排空后置 `Failed`；本次同时修复"故障回调批次整批丢弃导致撤单丢失"的缺陷，改为**只丢弃
非 reduce-only 新风险提交，保留撤单/reduce-only 并正常校验提交**。

剩余：

- `CommandPurpose` 落地后，排空期允许已授权的 `KnownFillHedge`；
- 故障原因写入检查点并在重启后保留；
- 负数/超范围返回仍按平台硬故障失败关闭，不进入业务排空。

## 7. 停止目标与留仓（B7）

- 新增 `StopTarget { DrainHedge, FlattenOwned }`；普通人工 Stop 默认 `DrainHedge`（撤 maker、
  补齐已知欠量、允许留仓），风险触发自动停止默认 `FlattenOwned`。
- `FlattenOwned`：撤 maker、核清在途、按双腿独立预算逐步平掉本策略持仓，等待两腿订单终态、
  原生成交与持仓归零后才完成；需要独立退出引用与状态。
- 停止成功需要分别证明：Operation 成功、进程退出、订单/持仓事实、占用释放；未知不等于安全释放。
- 期限预算：`撤单 + 必要退出 + 确认/持久化 + 安全余量 <= 宿主总停止预算`，受管理路径 10 秒约束。

## 8. 启动预检（B8）

`BindingPreflight` 在启动前拒绝：

- 账户/市场身份不满足线性 NET、PostOnly/IOC、独立成交、查询能力；
- 原生 tick/lot/乘数/最小量/最小金额与绑定配置不一致；
- 共同经济资产、共同报价/抵押币或 FX 证据缺失；
- 两腿初始事实非 Fresh 且不符合恢复规则；资金/救援资源不可预占；
- 多绑定风险配置缺失、timer 未配置、停止预算不成立。

管理入口：双账户/双市场执行模板与绑定准入；单市场旧模板保持兼容。
本地静态部分已提供 `scripts/cross_exchange_mm_preflight.py`，可先行检查配置形状与整数比例；
原生元数据与资金项仍需 B8 + 实盘只读验收。

## 9. 实现顺序与验收

| 步骤 | 内容 | 验收 |
| --- | --- | --- |
| B2 | 配置类型 + 互斥 + 能力门禁 + 单测 | 互斥/缺能力/旧宿主拒绝负例 |
| B3 | 逐市场行情状态 + 共用计算器 + 离线执行器接入 | 双市场不混时钟/单位；离线与在线同结果 |
| B4 | CommandPurpose + 成交先于持仓准入（**已完成**） | 未知意图不可绕过；fill-before-position 合法 hedge |
| B5 | 预占/救援/恢复（**部分完成**：数量/名义预占 + 恢复重建；保证金与崩溃矩阵未做） | 崩溃点矩阵不重复/不回退 |
| B6 | 故障排空收尾（含本回调撤单保留） | 故障撤单真实提交；最终 FAILED 保留原因 |
| B7 | FlattenOwned + 留仓占用 + 期限预算 | 留仓与零仓分别证明 |
| B8 | 预检 + 管理双绑模板 | 非法配置拒绝启动 |
| B9 | 构建服务器编译/Clippy/测试 + 双所模拟/故障矩阵 | 完整非实盘证据包 |

验收矩阵沿用修复方案 §9（限频内超限、最小成交不可对冲、fill 先于 position、撤单竞态、
单位/币种不同、资金不足、故障与撤单同回调、无行情/timer 迟到、崩溃恢复、停止目标、
磁盘/队列故障、价格跳跃）。策略正确性与收益可行性分别判定，不互相替代。
