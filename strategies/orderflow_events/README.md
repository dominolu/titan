# orderflow_events：L0 订单流事件采集策略（Numba / ABI V13）

只用 Titan 现有能力：订阅 BBO + DEPTH，使用 Rust 宿主重建的完整 Top100 盘口，**每 100 ms 在 `on_timer` 里重算一遍
L0 事件字典**（E01/E02/E03/E04/E07/E08/E11/E13/E14）。每个事件一个 `@njit` 检测函数、一组独立
阈值；不下单，只产事件与观测。原始行情仍由 Titan `collector` 采集（`collector/` crate）。

## 1. 事件与阈值

| 事件 | 触发条件（全部为 AND，除非写明 ∨） | 方向 | 阈值参数 |
| --- | --- | --- | --- |
| E01 推土机 | `run_len ≥ run_min` ∧ `\|dp(W)\| ≥ dp_bp` ∧ `V(dir,W) ≥ vol_btc` ∧ `ρ ≤ rho` | `sign(dp)` | `e01_*` |
| E02 加速 | E01 的 `run` 仍在 ∧ 持续 `≥ min_ms` ∧ `z(slope V) ≥ z` ∧ `z(slope \|dp\|) ≥ z` | `sign(dp)` | `e02_*` |
| E03 吸收 | `\|F(W)\| ≥ vol_btc` ∧ `\|dp(W)\| ≤ impact_bp` ∧ `κ ≤ kappa` ∧ `refill ≥ refill` | `−sign(F)` | `e03_*` |
| E04 冰山 | 同价位被吃穿 `≥ fills` 次 ∧ 补回时间 `≤ refill_ms`（要求该档有已知显示量；`cum/initial` 只作为强度因子，不做硬门限） | 买/卖侧 | `e04_*` |
| E07 扫单 | (`消耗档数 ≥ levels` ∨ `单侧量 ≥ vol_btc`) ∧ `\|dp(W)\| ≥ dp_bp` ∧ `事前Top5深度 < 单侧量` | `sign(dp)` | `e07_*` |
| E08 OFI/撤单 | `\|z(OFI(W))\| ≥ z` ∧ 净撤单代理 `≤ −h_ratio × 五档深度` ∧ 连续 `≥ persist` 次 | `sign(OFI)` | `e08_*` |
| E11 扫损 | 破前 `lookback` 高/低 `≥ break_bp` ∧ 破位后同向成交 `≥ vol_btc` ∧ 持续 `≥ min_ms` | 破位反向 | `e11_*` |
| E13 冲击 | `\|dp(W)\|/σ_W ≥ impact_z` ∧ (`z(量) ≥ vol_z` ∨ `z(最大单笔) ≥ maxq_z`) ∧ 同向笔数 `≥ min_prints` | `sign(dp)` | `e13_*` |
| E14 强平代理 | `\|mid−P0\|/P0/σ_W ≥ disp_z` ∧ 极端点后已过 `≥ prints` 笔 ∧ `z(量) ≥ vol_z` | 极端点方向 | `e14_*` |

所有 `*_enabled=false` 可直接关某个事件；阈值改动只改 `parameters.json`，不需要改代码。

## 2. 数据来源与平台缺口（重要）

1. **逐笔成交当前进不来**。V13 只把 BBO 和深度批次路由进策略（`compiled_strategy.rs` 里
   `Tick → titan.market.Bbo`），没有 trade 事件类型。`on_tick` 已按 `kind == 3` 预留 trade 分支，
   `ingest_trade()` 就是切换点。在平台补上之前，策略用 **深度触价减量推断成交**
   （`flow_source=depth_proxy`）：买单方向 = 卖一价档位被减量，卖单方向 = 买一价档位被减量。
   这是代理，会把手动撤单误计为成交；事件按 `proxy` 处理。
2. **爆仓标记不可得**。连接器没有订阅 `forceOrder`，E14 只能是价格+量能代理（`proxy`）。
3. **深度采用 HOST_DEPTH_100 能力**。Rust 校验快照语义、维护较深盘口、补回未变化的第 101 档，
   再提供完整 Top100 替换视图。策略读取 `ctx.depth_batch()` 的代次、有效性和 reset，
   不再通过批次大小猜快照。失效、恢复和超时后的首批会清理窗口并重新预热。详见
   [深度契约](../../docs/depth_pipeline.md)。

要把 1/2 补齐（另一个分支的平台改造，**本策略不改**）：

- SDK：`EventKind.TRADE = 13`，`_EVENT_HANDLER[EventKind.TRADE] = "on_tick"`；
- Rust：`V13EventKind::Trade = 13` → `TRADE_BATCH_EVENT`，`decode_market_batch` 里
  trade 批次设 `kind = 3`；
- Connector：Binance UM 订阅 `!forceOrder@arr`，按 symbol 过滤后以 `action=1` 发布为 trade 批次
  （`TitanTickView.kind=3` 且 action 位标记爆仓）；
- 然后加订阅 `EventSubscription(EventKind.TRADE, "on_tick", 1, EventQos.LATEST)`，并把
  `flow_source` 改成 `platform_trades`。**现在选 `platform_trades` 会在 `build()` 直接报错**，
  以免策略静默地永远不出事件。

## 3. 计算节拍与状态布局

- **节拍**：`timer_interval = 100 ms`（部署 runtime TOML 配置），`on_timer` 内再按
  `compute_interval_ns` 门控；`compute_interval_ms` 必须是 100，因为窗口环按 100 ms 桶组织。
- **窗口**：所有窗口都是 100 ms 桶的滚动和，粒度即 100 ms（与 v2.1 U5 对短窗的 ±100 ms 容差一致）。
- **状态（小于 64,000 B / 65,536 B 上限）**：100 档 × 2 侧的价格与量、640 个 100 ms 桶（64 s）、
  901 个 1 s 样本（15 分钟加当前秒）（15 min）、8 个触价槽（refill/冰山）、事件计数与 EWMA 基线。
- **z 统计**：用 EWMA 均值/方差代替离线研究的 trailing-1h median/MAD（状态有界；方向与阈值同构，
  数值不可与离线事件表直接对齐）。`sigma_c` 为 1 s 收益 EWMA(hl=15 s)，下限用参数
  `sigma_floor_bp`（而不是 trailing-1h p10）。

## 4. 观测字段（管理页/快照可见）

`mid_ticks`、`spread_bp`、`sigma_bp`、`ofi_2s`、`depth1_btc`、`depth5_btc`、`flow_source`、
`book_ready`（enum：unavailable/ready，完成预热后就绪）、`prints_seen`、`dropped`、`asset_no`、`computes`、
`events_total`、`last_type`、`last_direction`、`last_strength`、`last_ts_ns`，以及
`e01_count`…`e14_count` 九个计数器与 `e03_strength`、`e13_strength`。schema：
`titan.orderflow_events/v2`；另有 `book_valid`、`stream_epoch`、`book_resets`、`warmup_remaining_ms`。

排查最常用的三个：`asset_no`（必须等于部署绑定里的本地品种号，否则三个 handler 会过滤掉全部
行情且 `book_ready=0`）、`dropped`（跨越 1 s 时钟容差被丢弃的条目）、`book_ready`。

## 5. 构建与验证（必须在编译服务器 192.168.3.88 执行）

本地只写代码，不编译、不测试、不跑 Numba（见仓库 `AGENTS.md`）。

```bash
# 1) 编译原生策略
titan strategy compile \
  --strategy strategies/orderflow_events/strategy.py \
  --parameters strategies/orderflow_events/parameters.json \
  --target x86_64-unknown-linux-gnu \
  --cpu-baseline x86-64-v2 \
  --artifact-format bundle \
  --output orderflow_events.titan

# 2) 描述/类型检查（不产出制品，快速定位 Numba 类型错误）
titan strategy compile --describe-only ...   # 以 CLI 实际支持为准

# 3) SDK 回归（含新策略的导入与 njit 编译）
export TITAN_STRATEGY_PYTHON="$PWD/.venv/bin/python"
export TITAN_STRATEGY_SDK="$PWD/python/titan-strategy-sdk"
PYTHONPATH=python/titan-strategy-sdk:. "$TITAN_STRATEGY_PYTHON" -m unittest discover -s python/titan-strategy-sdk/tests -v
```

部署时把 runtime 的 `timer_interval` 设为 100 ms，例如：

```toml
runtime = { timer_capacity = 64, timer_interval = { secs = 0, nanos = 100000000 }, ... }
```

## 6. 已知限制

- 逐笔/爆仓为代理或不支持（见 §2）；E14 只有 `proxy`。
- 窗口粒度为 100 ms（E13 的 200 ms 窗口只有 2 个桶）。
- `ρ`（E01 的最大逆行）用「运行起点的 mid 到当前 mid」逐笔更新，未做窗口内 MAE 路径。
- **预热**：`sigma_c` 需要约 15 s 才有效；E11 的 15 min 高/低与 E14 的 5 min 基准在启动后
  头 15 min 内只有已采集的样本，早期的破位/位移会偏短视。`z` 统计用 EWMA，需要 ≥8 次采样才
  输出非零 z（约 0.8 s）。
- `asset_no` 默认 0，必须与部署绑定一致（见 §4 排查项）。
- 事件不落盘：策略只做实时消费/观测；研究数据由 `collector` 录原始行情后离线重算。
- 无 `on_recover`：重启依赖平台恢复 typed state；`on_start` 会用 `ctx.market()` 的 BBO 重建
  一档，其余档位等深度批次补齐。
- 深度快照识别是启发式（见 §2 第 3 条）。


## 6. 深度运行与升级要求

- 策略 0.2.0 / 恢复 schema 2，需重新编译；旧 checkpoint 不兼容。
- 删除旧参数 `snapshot_items`，默认 `depth_levels=100`。Binance 建议保留 `depth_snapshot_limit=1000`；
  快照少于两侧各 100 个真实价位时宿主明确拒绝，避免产生不完整的 Top100。
- 默认行情 arena 为 64 KiB 块 / 2048 槽，总块内存仍 128 MiB。已有显式小块配置须相应调整。
- 默认 E11 回看 15 分钟，恢复后先重新积累该窗口，期间 `book_valid=1` 但 `book_ready=0`。
  BBO 不会覆盖深度盘口或掩盖深度过期。
- 75 天 25 档历史不能验证深部事件。回放必须提供显式 `depth_batch` 元数据及可证明连续的原始快照/diff。
