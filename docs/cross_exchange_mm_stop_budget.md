# cross_exchange_mm 停止与风控预算表（C5）

日期：2026-11-11。对应工作清单 C5。本文把策略期限参数、平台停止预算和管理入口上限
放在一张表里推导，供实盘准入核对。**多数行依赖阶段 B（`FlattenOwned`、逐所预占）才能生效**，
未实现的部分明确标注，不作为已可用的预算。

## 1. 输入：策略默认期限参数

来源 `strategies/cross_exchange_mm.py` 的 `SPEC` 默认值（编译期固化，部署不可覆盖）：

| 参数 | 默认 | 含义 |
| --- | --- | --- |
| `order_timeout_ns` | 10 s | 单笔订单事实缺失/撤单/成交确认的总期限 |
| `hedge_timeout_ns` | 10 s | 一段连续未确认对冲敞口的总期限；停止与恢复都不重置 |
| `retry_backoff_ns` | 0.5 s | 已确认 IOC 终态后的重试等待 |
| `max_hedge_attempts` | 5 | 该段敞口内已确认终态后的最大提交次数 |
| `min_requote_ns` | 0.5 s | 仅限制普通价格更新，不限制安全撤单 |
| `reference_stale_ns` / `venue_stale_ns` | 5 s / 10 s | 两腿行情年龄上限 |
| `max_unhedged_lots` | 10 hedge lot | 合计未对冲敞口上限（事后锁存） |

## 2. 输入：平台上限

| 约束 | 值 | 来源 |
| --- | --- | --- |
| 受控管理路径 Stop 上限 | 10 s | `crates/titan-cli/src/management/control.rs`（`stop_timeout` 超过 10 秒被拒） |
| 宿主默认 `stop_timeout` | 10 s | `crates/titan-strategy-runtime/src/model.rs` |
| 多绑定停止预算校验 | 各阶段之和 <= 宿主 `stop_timeout` | `crates/titan-strategy-runtime/src/execution_binding.rs`（`execution_stop_budget_exceeds_host`） |
| 行情新鲜度上限（受控执行） | `max_market_age_ns`，管理模板给 2 s | `management/control.rs` |
| 价格偏离上限 | 1..=500 bps | 同上 |

## 3. 预算推导（当前可核对的部分）

停止必须依次完成：撤 maker → 补齐已知欠量 → 确认订单终态/独立成交/持仓 → 持久化。
按阶段计最小需要：

| 阶段 | 下界 | 依据 | 现状 |
| --- | --- | --- | --- |
| 撤 maker | 一次撤单往返 + 成交确认 | `order_timeout_ns` 覆盖撤单与迟到成交；不能等于 0 | 已实现（`pull` + `cancel_sent_ns`） |
| 补齐已知欠量 | 至多 `max_hedge_attempts` 次 IOC，每次间隔 `retry_backoff_ns` | `hedge_timeout_ns` 是这段敞口的总期限 | 已实现（`drive_hedge`） |
| 确认终态 | 等待独立成交与持仓投影 | 未知结果不得当作完成 | 已实现（`audit` 的 `execution_ready` / 105 期限） |
| 留仓处置 | 0 或 `FlattenOwned` 退出 | 阶段 B | **未实现**，见第 4 节 |
| 持久化 | 检查点提交 | `checkpoint_interval` 默认 1 s | 已实现 |

关键结论（当前配置下**不可行**，必须改参数才能实盘）：

- 策略 `order_timeout_ns = hedge_timeout_ns = 10 s`，而管理路径 Stop 上限也是 10 s。
  两个相同数字不等于"停止能在期限内完成"：撤单、对冲、确认、持久化要串行占用时间。
- 因此首个生产配置必须**同时**下调策略期限或放宽宿主预算，且放宽宿主预算受 10 s 硬上限约束，
  实际上只能下调策略期限。建议方向（需在阶段 B 的 `StopBudget` 上验证）：
  `order_timeout_ns` ≤ 3 s、`hedge_timeout_ns` ≤ 4 s、`retry_backoff_ns` ≤ 0.3 s，
  留 ≥ 2 s 给确认与持久化。**这些数值尚未经验证，不得作为准入证据**。

## 4. 阶段 B 生效后的预算形状

`MultiBindingExecutionLimits.stop` 的五个字段必须满足：

```
cancel_ns + hedge_ns + exit_ns + confirm_ns + slack_ns <= runtime.stop_timeout
```

映射关系（设计已冻结，实现待 B7）：

| 字段 | 应覆盖 | 关联参数 |
| --- | --- | --- |
| `cancel_ns` | 两腿撤单请求 + 撤单确认上限 | `order_timeout_ns` |
| `hedge_ns` | 已知欠量的对冲窗口 | `hedge_timeout_ns` |
| `exit_ns` | `FlattenOwned` 逐步退出时限 | 新增，需与 `rescue.deadline_ns` 一致 |
| `confirm_ns` | 终态/成交/持仓确认 | `order_timeout_ns` |
| `slack_ns` | 安全余量，不允许为 0 | — |

每所保证金/救援额度另由 `rescue`（`max_quantity_lots`、`max_notional_tick_lots`、
`max_cost_bps`、`deadline_ns`）约束，是事先授权上限，不是亏损后自动加码。

## 5. 核对清单（准入前逐项填数并留证据）

- [ ] 策略实际编译参数（不是源码默认值）已从 `titan strategy show` 的 `normalized_parameters` 读出。
- [ ] `runtime.stop_timeout` 已显式配置，且满足第 3/4 节的不等式。
- [ ] 管理路径 Stop 上限 10 s 未被突破（超过直接拒绝）。
- [ ] 两腿 `max_market_age_ns` 与策略 `reference_stale_ns`/`venue_stale_ns` 不矛盾。
- [ ] `rescue.deadline_ns` 与 `stop.exit_ns` 一致；救援数量上限 >= 一个最小 maker 成交。
- [ ] 停止验收要分别证明：Operation 成功、进程退出、订单/持仓事实、占用释放；未知不等于安全。
- [ ] `FlattenOwned` 的留仓与零仓两条路径都通过（阶段 B7 未完成前该项无法勾选）。

## 6. 边界

- 本表只做预算推导与约束核对，不构成参数批准；所有数值须在 `titan-build` 的故障矩阵
  与双所模拟中验证后才能进入受控实盘（阶段 B9）。
- 未启动实盘、未连接真实账户、未读取任何凭据。
