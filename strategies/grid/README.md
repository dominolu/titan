# Numba 合约网格（ABI V13）

`grid` 是单账户、单品种、线性合约、NET 持仓模式的原生策略。分析来源及差异见 [迁移分析](../../docs/gate_arb_grid_analysis.md)。这是交易策略组件；账户杠杆、保证金模式、交易所规则和最终风险审批仍由 Titan 平台管理。策略不会修改账户设置。

当前版本 `1.2.1`，state schema `3`。全面复核见 [网格审查](../../docs/verification/2026-10-22/grid-review/README.md)，真实历史行情、保护性退出修复及新验证见 [历史回测](../../docs/verification/2026-10-22/grid-history/README.md)。旧版本 checkpoint 不能直接恢复到本版；需要先用原制品完成停止，再按平台流程启动新版本。

## 行为

- 默认 `reset_pair`：一个网格订单完全成交后更新锚点，撤销剩余网格单，收到订单终态及相应独立成交事实后才挂下一轮。部分成交只更新库存和收益。
- `retain`：保留旧锚点的活动订单，按新锚点补足空位；同价不重复挂单。两侧各支持 0～16 层，不能同时为 0。
- `arithmetic` 间距为 `(上限－下限)/grid_levels`；`geometric` 使用 `(上限/下限)^(1/grid_levels)－1`。买价向下、卖价向上取整。几何多层使用买侧 `(1-r)^层数`、卖侧 `(1+r)^层数`，与源策略单层公式一致。
- 默认 `order_average` 按完全成交订单的累计成交均价更新锚点；`last_fill` 按最后一笔成交价更新。行情移动不会改变锚点。
- `neutral` 默认从零开始，可建立多头和空头；`neutral_mode=inventory_only` 则与源项目实际中性模式一致，只卖已累积库存。`long_bias` 只允许非负持仓；`short_bias` 只允许非正持仓。方向性初始仓位按源策略的上下剩余格数计算，使用有滑点保护的 IOC 限价单。全部确认完成后才开始网格；部分 IOC 必须先收到终态，再退避补余量，无进展重试有上限。
- 多头接近上边界时停止加买，只在上边界以内减仓；空头在下边界对称处理。边界平仓后停止该策略的报价。区间本身约束订单价，价格止盈止损由独立参数控制。
- 网格单全部使用 Post-only。保护单和平仓单为 IOC 限价单；平仓带 `reduce_only`。初始化的无进展重试有次数上限；保护性退出使用独立总时限，瞬时零成交或明确拒单不会耗尽开仓重试计数后永久放弃库存。最小数量、最小名义金额不满足时不会向上扩单或静默缩减初始目标。
- 完全成交后立即撤对侧，不等待持仓推送；下一轮仍须等待权威事实完整。保留订单模式同时检查本策略尚未释放的反向订单价格，避免新旧锚点之间自成交。
- 已确认库存的减仓优先处理，使用 `reduce_only`，不占用开仓保证金。中性订单原本需要反向开仓、但开仓预算不足时，只提交满足最小规则的减仓部分。不可交易的方向性尾仓会阻止继续增加新风险。
- 有权威账户序号时，迟到的旧订单成交不会回退较新的锚点。

## 编译与验证

以下命令仅在 `192.168.3.88` 执行。参数文件是小整数的离线示例，实际品种须先转换真实 tick、lot、货币单位及账户绑定。

```bash
export TITAN_STRATEGY_PYTHON="$PWD/.venv/bin/python"
export TITAN_STRATEGY_SDK="$PWD/python/titan-strategy-sdk"
target/debug/titan strategy compile \
  --strategy strategies/grid/strategy.py \
  --parameters strategies/grid/parameters.json \
  --target x86_64-unknown-linux-gnu --cpu-baseline x86-64-v2 \
  --artifact-format bundle --output verification-artifacts/grid/grid.titan

TITAN_TEST_BINARY="$PWD/target/debug/titan" PYTHONPATH=.:python/titan-strategy-sdk \
  .venv/bin/python -m unittest discover -s python/titan-strategy-sdk/tests -p 'test_grid*.py' -v

cargo +1.94.1 run -p titan-strategy-runtime --example v13_grid_stop_smoke -- \
  verification-artifacts/grid/grid.titan

target/debug/titan run verification-artifacts/grid/grid.titan \
  -e backtest -m tick -c configs/grid_v13_backtest.toml --json
```

策略文件自身包含完整 `SPEC` 参数 schema。参数省略时采用声明的默认值，未知参数、无效枚举和超出整数安全范围的配置会被拒绝。

示例 [回放配置](../../configs/grid_v13_backtest.toml) 和 [确定性事件轨迹](../../fixtures/grid_v13_trace.json) 走通买入成交、撤对侧、停止平仓、等待持仓确认的完整流程，预期产生 3 个下单意图、1 个撤单意图。它不是行情收益回测。轨迹中的 `stop` 事件之后可以继续提供撤单、成交和持仓事实；若轨迹结束时停止屏障尚未释放，CLI 明确失败，不生成成功结果。

真实行情回测使用 Rust [历史入口](../../hftbacktest/examples/v13_grid_history.rs)，通过同一原生制品和共享账户/费用协调器运行；不使用 Python 策略替身。以下仍只在编译服务器执行：

```bash
cargo +1.94.1 rustc -p hftbacktest --example v13_grid_history -- -C opt-level=2
bash scripts/download_grid_history.sh data/grid-history
bash scripts/run_grid_history.sh data/grid-history verification-artifacts/grid-history/final
.venv/bin/python scripts/report_grid_history.py \
  --input verification-artifacts/grid-history/final --data data/grid-history \
  --output backtest_reports/grid_history_20261022
```

下载脚本获取固定 UTC 日期的官方 BBO、聚合成交与资金费归档并检查官方 SHA256；运行脚本执行 3 种行情 × 3 种方向 × 2 种费用/延迟/参与率模型。全部盘口用于撮合和逐更新回撤/敞口，只有策略 BBO 回调限制为最短 100ms。最小订单规则、费用和流动性假设见 [报告](../../docs/verification/2026-10-22/grid-history/README.md)，不能把模型结果当作实际交易所执行保证。

## 参数和单位

| 参数 | 含义 |
| --- | --- |
| `account_no` / `asset_no` / `quote_currency_no` | 本实例的 ABI 绑定编号，不是交易所 ID |
| `lower_price_ticks` / `upper_price_ticks` | 严格递增的网格价界；新建锚点必须在界内 |
| `grid_levels` | 完整区间格数，不是活动单数 |
| `neutral_mode` | `two_sided` 或与源项目一致的 `inventory_only`；仅对 neutral 生效 |
| `buy_levels` / `sell_levels` | 两侧最大活动单数 |
| `size_mode` | `fixed_qty`、`fixed_notional`、`allocated` 或 `allocated_fixed_qty` |
| `order_qty_lots` | 固定数量，已按品种 lot 转为整数 |
| `order_notional_tick_lots` | 固定名义规模；每笔数量向下取整为 `金额/实际订单价` |
| `allocated_capital_tick_lots` | 分配的保证金额度；allocated 模式每笔按实际报价换算数量；allocated_fixed_qty 在创建盘口计算一次数量并冻结，与源项目一致 |
| `leverage` | 本地保证金预算使用的杠杆；必须与平台已验证的账户实际设置一致 |
| `max_position_lots` | 0 时按 `grid_levels×区间最大单笔数量` 推导，非零时显式限制 |
| `max_notional_tick_lots` | 按区间上限、当前卖一及候选价的最大值保守估值；当前仓位超限触发风险停止 |
| `min_order_lots` / `min_notional_tick_lots` | 由部署流程提供的交易所最小交易规则 |
| `quote_units_per_tick_lot` | `价格 tick 的报价币价值×一 lot 的基础币数量×线性乘数/一个余额 unit 的报价币价值` |
| `margin_reserve_pct` | 保证金预算额外预留百分比，默认 5%；不是手续费率 |
| `stop_below_ticks` / `stop_above_ticks` | 非零时按新鲜盘口中间价触发停止；上下限分别对应跌破/涨破，不按方向暗换含义 |
| `max_loss_tick_lots` | 不含手续费、资金费的毛损失保护；0 关闭 |
| `max_slippage_ticks` | 初始化/平仓 IOC 相对当前盘口的最大价格偏移 |
| `market_stale_after_ns` | 行情过期阈值，默认 5 秒 |
| `order_timeout_ns` | 提交、IOC、撤单、已报告但未消费的部分/完整成交及持仓确认的最长等待；行情过期不能延长等待 |
| `flatten_timeout_ns` | 从首次进入 STOPPING 起计算的保护性平仓总时限，默认 120 秒；行情刷新、重试及恢复不延长。未知订单结果仍先受 order_timeout_ns 约束 |
| `retry_backoff_ns` | 明确终态后的补单/拒单/撤单重试间隔，默认 0.5 秒 |
| `cancel_retry_limit` / `max_rejections` | 撤单尝试上限 / 新增风险的连续明确拒单及初始化 IOC 无进展终态上限；STOPPING 中的退出按独立期限处理 |
| `start_mode` | `strict` 拒绝已有持仓；`takeover` 显式接管权威数量和成本，但不认领未知订单 |
| `stop_action` | 人工停止时 `flatten` 默认平仓，`cancel` 留仓；风险停止始终平仓 |

`tick_lots` 是内部整数货币量：`price_ticks×qty_lots`。换成报价币需乘 `实际 tick 价值×实际 lot 数量×线性乘数`。不得直接把 USDT 金额、合约张数或交易所价格填入这些字段。浮点仅用于几何价格、平均成本和保证金预算，订单价量为整数；创建时限制中间乘积不超过 `2^52`。

保证金检查同时限制分配额度和可用余额，预留本策略所有尚未确认释放的订单。可用余额可能已经冻结部分订单资金，因此本地预算有意保守；交易所阶梯保证金、动态规则和账户组合风险由平台执行器再次验证。单实例需要独占其账户品种的 NET 仓位；其他来源的持仓变化会触发对账。

要沿用源项目的创建时资金分配和固定数量，可使用 [兼容参数示例](parameters.gate_arb.json)。其中的价格、数量单位仍是离线示例；`neutral_mode` 用于切换到中性方向时保持库存约束。

## 停止、恢复与收益

行情过期或账户未就绪时撤销本策略订单并等待；重新获得新鲜行情和一致持仓后补单。账户 epoch 改变、未知订单结果、订单身份冲突、成交缺口、确认超时或撤单重试耗尽时锁定为 `ERROR` / `RECONCILING`，不自动新增风险。失败状态不能被普通 tick 清除。

默认停止过程为 `STOPPING → 撤单确认 → reduce-only IOC → 独立成交确认 → 权威零持仓确认 → STOPPED`。部分平仓可以在明确终态后重试；行情过期时等待，不使用旧价强平。宿主停止超时则保留失败状态。`STOP_BARRIER` 能力要求策略显式调用 `ctx.complete_stop()`；旧运行时会拒绝含该未知能力的制品，不能把旧版本宿主用于运行此策略。

保护性退出总时限到期时进入 `RECONCILING`，保留未平库存事实且不释放停止屏障；不会自动放宽价格保护、虚报平仓或无限重试。恢复不重置该期限，时钟回退或期限已过的保护性退出拒绝恢复。平台宿主的停止 deadline 可更严格；部署时应与所需退出预算明确对齐。损失触发阈值是发起退出的条件，价格跳空、执行延迟、费用和资金费可能使实际亏损超过触发数值。

`cancel` 可用于人工留仓结束本次运行；使用同制品和平台 checkpoint 恢复后，等待新行情再恢复网格，不重复初始化。风险停止、边界完成与人工停止分别记录原因，前两者不会被当作人工留仓自动恢复。普通宿主 pause/resume 是执行门控制，不等同于“撤单留仓”；需要撤单留仓时使用此流程。

恢复回调不发任何命令，只接受平台校验后的 checkpoint。`RECOVERY_ORDER_RESULTS` 能力使 `ctx.recovery_orders()` 提供公共层已对账的订单结果。活动订单必须与保存的意图、已消费成交量、方向和价格一致，持仓必须与业务账本一致；已确认撤单且累计成交未变化的订单可以释放引用。缺少结果、身份变化或新增但未消费的成交仍拒绝恢复，不会把“订单不在挂单中”推断为撤单。旧宿主会拒绝新能力。重新编译或改参数会改变制品身份，不能拿新制品强行恢复旧状态。

成交去重按订单累计量判断，保留最近 128 个终结订单的消费水位。账户全局序号只用于确认水位，不用于忽略未知成交；超出业务缓存且无法证明归属/已消费的成交交给平台对账。订单累计量不得回退，终态与独立成交的缺口继续预占风险，并在期限内等待真实成交价。

业务账本按来源分开记录初始库存和网格库存，使用各自的加权平均成本：先匹配反向网格库存，再匹配初始库存。`grid_profit_tick_lots` 为网格配对毛收益，`inventory_profit_tick_lots` 为初始库存及退出的已实现毛收益，`unrealized_tick_lots` 为剩余库存浮动毛收益。`paired_lots` 记录配对数量；`completed_pairs` 只统计完全成交且全部数量都配对网格库存的订单，部分成交不会虚增完整次数。

V13 FillView 不含实际手续费和资金费，故 `gross_total_tick_lots` 明确为毛收益。净收益、费用、资金费和权益核对使用平台账本；本策略不会估造这些数据。需要按净亏损触发的账户级限制应配置在平台风控层。

此实现不包含现货库存分配、双向持仓模式、Web 管理界面、运行中热修改参数或自动调整账户杠杆。运行中调参应先完成停止，然后以新参数编译并新建实例。离线回放和实盘使用同一份原生制品；行情撮合、队列、延迟和费用模型由回测环境提供，不能把回调测试当作收益回测或实盘验收。

### 管理暂停

1.2.1 制品声明 `pause_safe`，保持 schema 3。暂停时通过 V13 `ctx.is_paused` 消费订单、独立成交、持仓和定时器，撤销现有报价并等待终态，不创建新的开仓报价；已触发的保护性退出继续执行。恢复运行沿用同一状态和代次。平台同时限制暂停期间仅可撤单和 venue reduce-only 委托。旧制品未声明此能力时，管理端不提供 Pause；新增能力位会被不支持的旧 host 拒绝，不应直接回滚 host 后加载新制品。
