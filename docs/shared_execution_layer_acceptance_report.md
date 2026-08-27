# 共享执行层最终验收与需求追踪报告

> 验收日期：2026-08-24
> 需求基线：[`shared_execution_layer_requirements.md`](shared_execution_layer_requirements.md)
> 结论：P0-A～P0-D、P1 以及需求基线列出的 P2 接口范围全部实现；无静默降级项。

## 1. 验收口径

本报告只把同时满足“生产代码路径、自动化测试、完整构建门禁”的项目标为通过。架构占位类型不作为
完成证据。Tick、Bar 和 Hybrid 保留各自的市场事实/撮合器，但统一进入 Rust 拥有的 command、risk、
transport、order state machine、Venue account、report、projector、result 与 audit 路径。

## 2. 需求追踪矩阵

| 需求 | 实现证据 | 验收证据 | 状态 |
|---|---|---|---|
| REQ-GEN-001～007 | `execution/`、`runtime.rs`、ABI v8 单参数 callback、全局 TickBatch/BarBatch | `rust_owns_loop_and_dispatches_extensible_callbacks`、Python lifecycle/global batch tests | 通过 |
| REQ-PERF-001～007 | 单态 Tick matcher、POD buffer、事件跳转、Chunked NPY、可关闭/有界审计 | ABI v8 复验 release A/B 中位 +1.095%；分块 feed/audit tests | 通过 |
| REQ-DET-001～004 | `GlobalScheduler<EventKey>`、稳定 source/sequence、显式 RNG seed | scheduler ordering、seeded quality replay、100 次 replay | 通过 |
| REQ-OWN-001～022 | `SharedExecutionEngine`、`VenueExecutionCore`、Instrument matcher adapter；账户只归 Venue | `ownership_keeps_account_at_venue_not_instrument`、多品种共享账户 tests | 通过 |
| REQ-BOUND-001～005 | `platform.rs` 的 Data/Actor/Algorithm/Simulation 边界和共享有界 command producers；Bar/Tick/Hybrid/live Tick 接同一生产入口 | producer 容量/reset 测试及 `algorithms_and_hooks_enter_the_real_bar_execution_path` | 通过 |
| REQ-INST-001～004 | 版本化 `InstrumentSpec`、统一精度/数量/名义校验、timestamped update/status | instrument validation、scheduled status/update reset tests | 通过 |
| REQ-ORD-001～015 | ABI command decode、统一状态机、GTD/trigger/reduce-only、cancel/replace、IOC/FOK | transition/race/partial/FOK/GTD、invalid/duplicate local reject tests | 通过 |
| REQ-REP-001～004 | matcher 只输出 `MatchOutcome`；coordinator 唯一计算 fee/account/report | independent partial fills、single fee/delta、sink tests | 通过 |
| REQ-ACC-001～014 | 权威 `ExchangePortfolio`、延迟后的 `PortfolioLedger`、CurrencyId/venue/instrument 明细 | exchange-before-local、cross-instrument collateral、funding attribution tests | 通过 |
| REQ-TIME-001～004 | Bar 仅有 open/close；feed latency 位于 scheduler envelope；订单四时间字段 | Rust feed-latency/entry/response tests；Python latency ABI test | 通过 |
| REQ-SCH-001～012 | v2 集中 phase contract、全排序键、matching 后 Funding phase、数据结束后继续 drain | same-timestamp Bar/Funding/Timer、Before/After 仓位差异、order race、StopAtDataEnd tests | 通过 |
| REQ-BAR-001～004 | NextOpen、正 entry latency、callback 后提交历史、empty Bar 无流动性 | Rust Bar tests及 Python history/empty/current-Bar tests | 通过 |
| REQ-HYB-001～004 | Bar 仅发信号、Tick 唯一 execution source；缺 Tick fail-fast | `bar_signal_and_tick_execution_never_double_match`、Python Hybrid test | 通过 |
| REQ-RISK-001～007 | local/exchange/post 三阶段、稳定 reason、RiskActionSink、Venue cross margin | three-stage、local/exchange visibility、liquidation/reduce-only tests | 通过 |
| REQ-FUND-001～008 | 独立 Funding event/config/engine；四时间；Before/After 真正跨 matching；逐 report 元数据；live projector | explicit config hash、同时间边界仓位、multi-report metadata、live dedupe、result/reset tests | 通过 |
| REQ-TMR-001～006 | 一等 `TimerQueue`，Replace/cancel/drain/reset；统一 POD payload | timer ordering、无行情下单、数据结束后推进 tests | 通过 |
| REQ-TICK-001～014 | 可选历史流动性账本和独立 ExecutionQualityModel，默认 identity/disabled | consumption reset、seeded quality/limit/liquidity tests | 通过 |
| REQ-DATA-001～006 | InMemory、Chunked feed、流式 NPY provider；manifest/排序/完整性验证 | chunk-boundary、NPY rewind/no materialization、manifest tests | 通过 |
| REQ-LIVEBAR-001～005 | native/canonical/recovery source、watermark、gap、REST key dedupe、禁止回写 | canonical builder/recovery tests | 通过 |
| REQ-PROJ-001～008 | Backtest/live 共用 `ExecutionEventProjector`，稳定事件 ID、ABI layout startup check | byte-equivalence、reconnect dedupe、合法 partial preservation tests | 通过 |
| REQ-FEE-001～006 | amount/currency/liquidity、负 rebate、rounding、收费时点、contract cash flow | fixed fee hash/reset、multiple fill fee、derivative cash-flow tests | 通过 |
| REQ-CAP-001～005 | mode-specific capability/model descriptor、参数 hash、fail-closed startup | capability composition及 invalid Margin startup test | 通过 |
| REQ-RES-001～004 | 统一 `BacktestResult`、三种 EndPolicy、termination、直接采集统计 | prepared runner、StopAtDataEnd、Tick result/reset tests | 通过 |
| REQ-AUD-001～004 | 九类 AuditKind、完整 EventKey、TITAUDIT header、bounded/streaming sink | audit coverage、binary schema/chunk tests | 通过 |
| REQ-RST-001～005 | 数据、时钟、账户、风险、Funding、Timer、RNG、历史、结果全 reset；finish hook | Tick/Bar 100 次 replay、dispose、Start→Error→Stop tests | 通过 |
| REQ-ERR-001～005 | `RuntimeError` 含 run/component/key/code/context；错误即停；finish/flush | invalid transition diagnostic、callback fatal lifecycle、source finish tests | 通过 |

## 3. 验收项结果

- **AC-TICK-001～003**：L2/L3、Partial/NoPartial、Queue、Latency、Fee golden tests 通过；phase v2
  及跨模式 P2 修复后的 release A/B 三次回归为 +1.899%、+0.182%、+1.370%，中位 +1.370%，满足不超过 3%。完整方法与
  原始样本见 [`tick_shared_execution_release_benchmark.md`](tick_shared_execution_release_benchmark.md)。
- **AC-BAR-001～004**：NextOpen、OHLC/volume partial/FOK、fee、两段 latency、双状态和 chunk boundary
  测试通过。真实 `AAPL_1m_all_sources.parquet` 与转换后 NPY 均读到 264,190 根；首尾时间、总成交量、
  callback/batch 数完全一致。Parquet/NPY replay 分别为 0.033432s/0.032079s（该数据样本，仅作一致性证据）。
- **AC-ACC-001～003**：同 Venue 多 Instrument collateral、exchange/local 延迟窗口、多币种明细守恒通过。
- **AC-SCH/HYB/TMR/FUND**：v2 同时间 phase、唯一 Hybrid matcher、无行情 Timer 下单、Funding
  Before/After matching 仓位和逐 report 元数据测试通过。
- **AC-PROJ-001～003**：等价 backtest/live report 产生 ABI v8 兼容字节；reconnect event 去重而 partial fill
  不折叠；策略 callback 无运行模式分支。
- **AC-RST-001～002、AC-RES-001**：Tick 和 Prepared Bar 均连续 100 次一致；fee/model/seed/phase/data
  进入 fingerprint，变更会改变 hash。

## 4. 最终门禁

```console
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo test --workspace --all-targets
cargo build --release -p py-hftbacktest
.venv/bin/python -m unittest discover -s py-hftbacktest/tests -v
```

最终结果：Rust 核心共享执行库 125 项测试通过；Python 37 项通过、1 项依赖外部行情 fixture 的测试按定义
跳过。Connector 两个测试目标各 31 项通过、各 1 项真实网络测试 ignored，不计为本地共享执行验收失败。

## 5. 兼容与迁移

- 公共 modify API 保持禁用；修改使用可审计 cancel/replace。
- ABI 当前为 v8；Funding 配置迁移见 [`strategy_abi_v8_migration.md`](strategy_abi_v8_migration.md)，
  order/fill 字段的 v7 变更仍见 [`strategy_abi_v7_migration.md`](strategy_abi_v7_migration.md)。
- 旧 `run_configured_materialized_bar_runtime` 符号保留零 latency 兼容；Python 使用 v2 符号显式传递
  feed/entry/response latency。
- Bar payload 不含 `available_ts`。策略收到 Bar 时的 `s.now` 是调度器 delivery time，Bar 的
  `close_ts` 仍是不可变市场事实。
