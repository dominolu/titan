# `pair_arb`：Slot 化双腿执行内核

本目录实现 [docs/pair_arb_strategy_requirements.md](../../docs/pair_arb_strategy_requirements.md)
（需求稿 v2.2）定义的执行内核：一个 Pair、一个 current Slot、固定容量的活动订单数组、
append-only 的 OrdersList，以及四个策略入口。

目录里有两份实现，状态机语义相同，用途不同：

| 实现 | 文件 | 用途 |
|---|---|---|
| **ABI v12 Numba 策略**（运行时加载） | [strategy.py](strategy.py) + [state_layout.py](state_layout.py) | `pair_arb.strategy:build`，固定内存 `state_f64`/`state_i64`，回调 `on_start`/`on_tick`/`on_filled`/`on_order`/`on_stop` |
| **协程参考内核**（规格/回归验证） | [reference.py](reference.py) + engine/risk/connector/callbacks/context/sim | `pair_arb.reference:build`，保留 `on_tick`/`risk_check`/`on_fill`/`on_cancel` 四个 async 入口、reconcile 与审计记录 |

两者共享同一份需求与同一套测试语言：Numba 版负责“能被运行时加载并下单”，参考版负责
“完整表达需求里 ABI 还表达不了的部分”（对账、审计、connector 拦截）。

```text
on_tick     状态检查、maker 价差检查、撤单、重挂、Slot 推进
risk_check  风控指标计算与 posture 迁移（不做对账、不下单）
on_fill     唯一累加成交的入口；initiator 完全成交后立即提交 hedge
on_cancel   只处理撤单返回与最终撤单确认，清除对应订单关系
reconcile   非入口的恢复路径，由 runtime/connector 在事实不可信时调用
```

## 模块划分

| 文件 | 需求章节 | 职责 |
|---|---|---|
| `abi_v10.py` | §4 §5 §6.3 §6.4 | 订单、成交、行情、命令结果、状态词表（与 connector 的 `api_status`/`api_tif` 编码一致） |
| `context.py` | §2 §3 §4 §8 | `Pair` / `Slot` / `ActiveOrder` 固定内存布局、`OrdersList`、`PairContext`、`BrokerFacade` |
| `risk.py` | §6.2 | 风险快照、posture 迁移、gross imbalance（不平滑相反方向） |
| `engine.py` | §6.1 §6.3 §6.4 §6.5 §6.6 §7 | 四个入口、Slot 生命周期、定价、对账与审计记录 |
| `connector.py` | §6.3 §6.4 §6.6.2 | 累计量→增量转换、订单归属校验、重复/乱序/非法回报拦截 |
| `callbacks.py` | §5 §6.6.1 | broker 返回与订单事件的投递路由（不可信事实直接进 reconcile） |
| `sim.py` | — | 确定性模拟 broker，供单元测试与回放使用 |
| `strategy.py` | §7.1 §10 | `build(parameters)` 冷路径构造与参数校验 |
| `reference.py` | §7.1 §10 | 参考内核的 `build(parameters, broker=None)`（原 `strategy.py`） |
| `state_layout.py` | §2 §3 §8 | Numba 版策略私有 offset 布局（Pair/Slot/活动订单/终态 ring），不属于公共 ABI |

## 不变量覆盖

| 不变量 | 覆盖测试 |
|---|---|
| I1 单一 `hedge_ratio_abs` | `test_pair_arb_build.py::test_non_positive_ratio_or_slot_unit_is_rejected` |
| I2 Slot hedge 数量按当前 ratio 计算 | `test_pair_arb_fill.py::test_pair_ratio_is_applied_for_fractional_hedge_sizes` |
| I3 一个 order_id 只有一条 OrdersList 记录 | `test_pair_arb_reconcile.py::test_orders_list_keeps_one_record_per_order_id` |
| I4 订单只属于一个 Slot/角色 | `test_pair_arb_fill.py::test_fill_after_slot_completion_still_belongs_to_its_own_slot` |
| I5 撤单确认后清除对应关系 | `test_pair_arb_cancel.py::test_cancel_confirmation_archives_the_order_and_clears_the_relation` |
| I6 历史订单可按 order_id 找回 | `test_pair_arb_reconcile.py::test_restore_from_venue_rebinds_orders_to_the_current_slot` |
| I7 OrdersList 同时保存请求与成交明细 | `test_pair_arb_fill.py::test_full_initiator_fill_archives_the_order_and_hedges_immediately` |
| I8 timeout/transport error 不重发 | `test_pair_arb_tick.py::test_create_timeout_marks_the_order_unknown_and_stops_replacement` |
| I9 重复/乱序/非法成交由 connector 拦截 | `test_pair_arb_connector.py`（整个文件） |
| I10 `on_tick` 不处理成交 | `test_pair_arb_fill.py::test_on_tick_never_invents_or_changes_fill_quantities` |
| I11 只有 initiator 完全成交才立即 hedge | `test_pair_arb_fill.py::test_partial_initiator_fill_only_accumulates_the_slot` |
| I12 `on_cancel` 不处理成交 | `test_pair_arb_fill.py::test_on_cancel_never_changes_fill_quantities` |
| I13 `posture >= RESTRICTED` 不创建新 Slot | `test_pair_arb_tick.py::test_restricted_posture_blocks_new_slots_until_facts_recover` |
| I14 活动数组满时不覆盖、不下单 | `test_pair_arb_tick.py::test_active_order_array_never_overwrites_an_existing_record` |
| I15 风险统计不平滑相反方向的 gross | `test_pair_arb_risk.py::test_gross_imbalance_never_nets_opposite_slots` |
| I16 未确认撤单前不提前重挂 | `test_pair_arb_tick.py::test_no_replacement_maker_is_created_before_cancel_confirmation` |

补充覆盖：异常关系冻结与恢复（`TestPairArbAbnormalRelations`）、撤单超时/拒绝/连续拒绝、
`on_cancel` 四类返回值、`on_fill`/`on_cancel` 六种交错顺序、connector 对累计量回退与跳变的
拦截、reconcile 审计字段与三类结论、以及一对 Pair 阻塞在 broker 时另一对 Pair 继续推进。

需求 §9 测试清单中的其余场景（部分成交累计、剩余量重挂、后继 Slot、撤单接受后的晚到成交、
两腿接受/拒绝/超时、容量耗尽、重连恢复、单 Pair 等待 broker 时其他 Pair 继续运行）分别由
`test_pair_arb_fill.py`、`test_pair_arb_cancel.py`、`test_pair_arb_tick.py`、
`test_pair_arb_reconcile.py` 覆盖；异常关系（终态订单仍被 Slot 引用、同角色两个活动订单、
Slot 引用不存在的订单）由 `test_pair_arb_tick.py::TestPairArbAbnormalRelations` 覆盖，
冷路径参数校验与入口装配由 `test_pair_arb_build.py` 覆盖，posture 迁移由
`test_pair_arb_risk.py` 覆盖。

## 运行单元测试

按仓库约定，单元测试在编译服务器 `192.168.3.88` 上执行，本地只编写代码：

```bash
rsync -az --exclude '__pycache__' strategies/pair_arb/ <build-host>:~/dev/titan/strategies/pair_arb/
rsync -az --exclude '__pycache__' python/titan-strategy-sdk/tests/ <build-host>:~/dev/titan/python/titan-strategy-sdk/tests/
ssh <build-host> 'cd ~/dev/titan && PYTHONPATH=$PWD/python/titan-strategy-sdk \
  ~/miniconda3/envs/hft/bin/python -m unittest discover -s python/titan-strategy-sdk/tests -v'
```

当前结果：137 个用例全部通过（107 参考内核 + 24 ABI v12 Numba + 8 SDK 门面/分层 -
见 `Ran 148 tests ... OK`，含 9 个既有 xemm 用例）。

运行时加载检查（用真实 ABI descriptor 编译策略包）：

```bash
ssh <build-host> 'cd ~/dev/titan && PYTHONHOME=$HOME/miniconda3/envs/hft \
  LD_LIBRARY_PATH=$HOME/miniconda3/envs/hft/lib TITAN_HOME=/tmp/pair-arb \
  TITAN_STRATEGIES=$PWD/strategies PYTHONPATH=$PWD/python/titan-strategy-sdk \
  ./target/debug/titan strategy compile pair_arb --json'
# {"capabilities":["start","order","filled","tick","stop"], ... "strategy_id":"pair_arb"}
```

## ABI v12 Numba 实现说明

- **单位**：行情视图是价格单位；下单价格换算成整数 tick、数量换算成整数 lot 后交给门面
  （ABI 的 host 会拒绝带小数的请求）。`hedge_ratio_abs`、`spread`、`requote_distance`、
  `dust_lots` 等参数只影响策略内部计算。
- **完成判定**：ABI 的 fill 记录没有 status 字段，订单是否结束由“累计成交量达到委托量”判定；
  终态（CANCELED/REJECTED/EXPIRED/FILLED）来自 `on_order` 的订单事件。
- **职责边界**：`on_tick` 只做风控、Slot 推进、maker 价差检查、撤单超时与重挂；
  `on_filled` 是唯一累加成交并提交 hedge 的入口；`on_order` 只应用撤单确认/拒绝/终态。
- **容量**：`MAX_ACTIVE_ORDERS = 4` 固定数组，满了就拒绝新订单并进入 `RESTRICTED`；
  `HISTORY_RING = 64` 保存终态订单事实（runtime 目前没有 append-only `OrdersList`，
  环形写满会在 `I_HISTORY_WRAPPED` 标记）。
- **posture**：全部由 `risk_check`（`on_tick` 内调用，撤单超时扫描之后）计算；`HALT` 由关系冲突、
  over-fill 等异常触发并 latch，只允许撤单。
- **已知缺口**：ABI 目前没有 order/position query host call，也没有独立 `risk_check` 回调槽位，
  因此 reconcile 仍由 runtime 负责（参考内核保留完整实现）。

`strategy-manifest.json` 的 `artifact_digest` 由内容文件按 Rust
`digest_content_files` 规则计算；任何 `.py` 改动后必须重算（xemm 包的 digest 已用作算法校验样本）。

## 运行期接线状态

内核所需的 broker 能力被抽象为 `BrokerFacade`（`create_order` / `cancel_order` /
`query_order` / `query_open_orders` / `query_position`），事件事实由 `ConnectorNormalizer`
投递。仓库现有的策略运行时尚只加载 Numba 同步回调包（`strategy-manifest.json` 中的
`numba-python` loader，ABI major 12），没有协程 + broker facade 的宿主机路径，因此本包
**暂不提供** `strategy-manifest.json`：声明一个无法加载的 manifest（含伪造 digest）会掩盖
真实缺口。接入方式有两种，均不需要改动本包的入口语义：

1. 在 runtime 侧新增协程策略宿主，按 `BrokerFacade` 协议注入 broker，并直接使用
   `strategy.build(parameters, broker=...)` 返回的 `entrypoints`；
2. 或把 `engine.py` 的状态机移植到 ABI 固定内存布局（`state_f64`/`state_i64`），
   用 host 回调实现 `create_order`/`cancel_order`。

在宿主路径落地前，`sim.SimulatedBroker` 可以驱动完整的回放与回归测试。
