# 共享执行层实施任务清单

> 需求基线：[`shared_execution_layer_requirements.md`](shared_execution_layer_requirements.md)
> 状态标记：`[ ]` 未开始、`[-]` 进行中、`[x]` 已通过验收。

## P0-A：架构骨架

- [x] **A01 InstrumentSpec 与强类型 ID**
  需求：REQ-INST-001～004、REQ-CAP-001～005。
  交付：统一规格、精度/数量/名义金额校验、结构化错误、单元测试。
- [x] **A02 全局 Scheduler**
  需求：REQ-DET-001～004、REQ-SCH-001～012。
  交付：EventKey、phase contract、稳定 sequence、reset、顺序测试。
- [x] **A03 Engine/Venue/Instrument 所有权类型**
  需求：REQ-OWN-001～022。
  交付：不接管旧路径的骨架及所有权编译测试。
- [x] **A04 VenueAccount + PortfolioLedger**
  需求：REQ-ACC-001～014。
  交付：多 Instrument/Currency 账本和守恒测试。
- [x] **A05 Exchange/Local 双状态**
  需求：REQ-ACC-001～006。
  交付：exchange apply、延迟 report、local apply 和可见性测试。
- [x] **A06 三阶段 Risk**
  需求：REQ-RISK-001～007。
  交付：traits、AllowAll、稳定拒单原因和调用顺序测试。
- [x] **A07 ExecutionEventProjector 接口**
  需求：REQ-PROJ-001～008。
  交付：回测/live 规范化输入、稳定输出顺序和去重测试。
- [x] **A08 P0-A 集成验收**
  交付：全 workspace 测试、rustfmt、`cargo check --workspace` 和需求追踪更新。
  结果：workspace 测试全部通过；当前 Rust 1.94.0 Apple ARM 工具链不提供适用的
  `cargo-clippy`，已记录环境限制，未通过改换工具链绕过。

## P0-B：Tick 无行为迁移

- [x] **B01 现有 Tick characterization/golden 基线**
  - [x] L2 NoPartial 市价单：entry/response latency、taker price、fee、账户和时间戳；
  - [x] L2 NoPartial 被动 Limit、GTX 和同时间 fill-before-cancel race；
  - [x] L2 Partial 的独立 partial fill、IOC terminal expiry 和跨档 FOK；
  - [x] L2 RiskAdverseQueueModel 同价前方数量推进；
  - [x] L3 FIFO market add/fill 与 backtest order accepted/fill 生命周期；
  - [x] 多资产同时间 EventSet 的 `(asset_no, event_kind)` 顺序；
  - [x] 规范化 exchange report 的稳定 FNV-1a golden hash；
  - [x] release 性能基线：100 万事件、30 轮 A/B 的中位回归 0.429%，低于 3% 门槛，见
    [`tick_shared_execution_release_benchmark.md`](tick_shared_execution_release_benchmark.md)。
- [x] B02 共享内部 ExecutionCommand 与 OrderStateMachine。
- [x] B03 共享 MatchOutcome、独立 Fill、ExecutionReport 与账户协调器。
- [x] B04 共享 entry/response Transport。
- [x] **B05 Tick L2/L3 matcher adapter**
  - [x] L2/L3 legacy response 转换为共享 `MatchOutcome`；
  - [x] 回测/live 共用 Rust 核心 ABI order/fill projector；
  - [x] exchange-time outcome 通过无 Python/字符串/逐事件分配的 `OutcomeBus` 直接接入
    `TickOutcomeCoordinator`；立即成交会规范化为同时间 `Accepted -> Fill`，撤单结果会先进入
    `PendingCancel`；
  - [x] legacy L2/L3 matcher 不再调用 `State::apply_fill`，matcher 仅维护盘口/订单并输出
    `MatchOutcome`；matcher 不直接调用策略 callback。
- [x] **B06 旧 State/Fee 迁移到 VenueAccount adapter**
  Rust builder 与 Python 构建宏默认创建 `InstrumentSpec + LegacyExecutionFeeAdapter`；exchange-time
  fill 由共享协调器计算费用并更新 exchange account，真实 response delivery 才用同一不可变
  delta 更新 `PortfolioLedger`。内置 Local 以 external-accounting 模式运行，`Bot::position` 和
  `state_values` 由共享 local view 的兼容投影提供；自定义 `Asset::new` 仍保留旧 State fallback。
- [x] **B07 逐事件 differential 与 release 性能验收**
  L2 NoPartial、L2 Partial IOC/FOK、cancel/fill race 与 L3 FIFO 均校验 legacy/shared 的独立
  fill、终态、position、fee、trade count、volume/value 和 cash 语义；release 中位回归
  0.429%，通过 3% 门槛。

已修复的迁移前缺陷：旧 `PartialFillExchange` 会把主动单的跨档 fill 折叠到最终快照，且 IOC
余量过期时本地账户完全遗漏已成交数量。当前已改为逐 fill 响应，并为 IOC 余量单独发送 terminal
Expired report。

## P0-C：Bar 专业基础

- [x] C01 拆分 MaterializedBarSource 的 feed/matching/execution 职责：
  `MaterializedBarFeed` 仅负责校验/分批，`NextOpenBarMatcher` 仅维护订单和生成结果，
  `BarExecutionState` 独立消费结果；原有 runtime 行为测试保持通过。
- [x] C02 NextOpen 接入共享订单和账户链：Bar matcher 输出规范化结果，由
  `TickOutcomeCoordinator` 共用的状态机/账户/projector 链处理。
- [x] C03 Bar latency、fee、完整订单事件与部分成交接口：支持每资产
  `InstrumentSpec + ExecutionFeeModel`、任意固定 response latency、Order/Filled/Position
  完整回调，以及不折叠的独立 partial fill；跨后续 Bar close 的延迟无时间倒退测试通过。
- [x] C04 Streaming BarFeed 与一个端到端分块实现：`BarChunkProvider + ChunkedBarFeed`
  只持有当前 chunk/lookahead/batch，batch 可跨 chunk 边界；分块 feed → NextOpen matcher →
  shared execution 的端到端测试通过，并支持 reset。
- [x] C05 PreparedRunner、BacktestResult、Audit 与完整 reset：结果包含复现元数据、运行计数、
  exchange/local 终态快照；审计支持关闭或有界分块 drain；Bar feed、matcher、订单、双账户、
  projector、历史环、命令/回调 buffer 均进入 reset contract，dispose 后 fail-fast。
- [x] C06 现有双均线兼容及 100 次复现验收：AAPL `polygon_s3` 264,190 根 1m Bar 的
  Numba 双均线连续 100 轮核心结果完全一致（3,012 次金叉、3,012 次死叉、6,023 成交量、
  最终持仓 1）；中位 510.964 ms。Rust release 原生 Bar replay 100 轮中位 8.843 ms，
  约 2,993 万 Bar/s。Rust `PreparedRunner` 另有 100 次 reset 等价单元测试。

## P0-D：统一专业运行时

- [x] **D01 Funding 完整事件链**：Bar、Tick 与 Hybrid 均支持 publication/effective/
  settlement/delivery 四时间；exchange settlement 与 local callback 分离；Funding 可跨数据末尾推进，
  `BeforeSettlementEvents` 在同时间 matching 前结算，`AfterSettlementEvents` 在 matching 后结算，而不是
  仅改变 callback 顺序；每份 report 保留自身 event/config 元数据。live Funding 通过同一 projector；
  结果按 Venue/Instrument/Currency 归集，reset 清理 cursor、报告和累计值。
- [x] **D02 Margin/leverage/liquidation/reduce-only**：完成 Venue 级 `CrossMarginRisk`、共享 collateral、
  exchange-position reduce-only、mark/unrealized PnL、初始/维持保证金和 post-trade liquidation action。Bar
  运行时接入 local/exchange/post 三阶段；Tick 在每笔订单实际到达时使用权威账户逐笔检查，同时间订单之间
  先落账再检查下一笔，Numba ABI 的 reduce-only 即使未配置额外 margin model 也强制使用 exchange position；
  venue 风控撤单直接作用于 exchange order store 并经过 response latency，强平则转成正常的 reduce-only
  Market IOC 命令重新进入 transport/matcher/report 链；每个 Venue/Instrument 同时只允许一笔 pending liquidation。
- [x] **D03 Tick + Bar Hybrid 调度与唯一 execution source**：Python/Rust Hybrid 以 Bar 产生信号、Tick
  唯一撮合；Tick 结束而 Bar 尚有数据时 fail-fast，禁止隐式 Bar fallback，已验证不会双重成交。
- [x] **D04 TimerQueue 和无行情推进**：Timer 支持 Replace、cancel、稳定排序、drain/reset；Bar/Tick/
  Hybrid 在无行情与数据结束后继续推进，Timer callback 下单仍经过正常 transport。
- [x] **D05 Live connector 事件 adapter 与统一 Projector**：完成 ABI 校验、状态规范化、稳定事件 ID
  去重、partial-fill 保留和 projector 等价测试；Backtest/LiveBot 的 legacy response 也共用 projector
  callback 分类。费率公告与账户 Funding settlement 已分型，Hyperliquid 实际结算进入 LiveBot 的统一
  Funding 队列并投影为 `on_funding(s)`；只提供未来费率而不提供账户结算的公共行情不会伪造现金变化。
- [x] **D06 native/canonical live Bar、恢复与去重**：支持 native/canonical/recovery 显式优先级、
  watermark/迟到窗口、空 Bar、乱序诊断、REST 补齐键去重和已投递 Bar 禁止回写。

## P1：执行真实性

- [x] Historical liquidity consumption：可 reset 的 event/side/price 消耗账本已接入可配置
  `PartialFillExchange` 热路径，同一历史深度不可重复消费；`NoPartialFillExchange` 对该配置 fail-fast，避免
  静默伪造 partial liquidity 语义。
- [x] ExecutionQuality/Slippage model：Identity 和显式 seed 的成交概率/滑点模型已接入 Tick partial matcher，
  在修改订单状态前调整成交，保证不突破限价和历史流动性；默认关闭时保留单态 matcher 和可预测空分支，
  FOK 保持原子撮合语义。
- [x] ConservativeOhlc/Touch/VolumeLimited：Rust matcher 与 Python `bar_matching`/
  `volume_participation` 显式配置已接通，FOK 原子性及独立 partial fill 已测试。
- [x] SignalClose：Python `bar_matching="signal_close"` 显式启用信号 Bar close 成交；默认
  NextOpen 不变，零 feed/entry latency 限制和 same-close 前视风险均已 fail-fast/文档化。
- [x] Stop Market/Stop Limit/GTD：Bar 与 Tick runtime 均已接通触发与到期，公共 ABI v8 fail-fast
  校验无效组合。
- [x] Instrument/MarketStatus 与标准审计报告：版本化 InstrumentSpec/status 调度 cursor、market-status
  risk gate、reset，以及 `TITAUDIT` 版本化 little-endian 有界分块格式已完成。

## P2：平台能力

- [x] DataManifest/Catalog、RunConfig/BatchNode、CustomData：清单 hash 与输入枚举顺序无关，Batch 每轮
  强制 reset，CustomData 保持 scheduler envelope。
- [x] Simulation hooks、OCO/OTO/Bracket、完整多币种 Portfolio：能力位于 execution core 外，通过
  canonical command bus 输出；Bar 与 Tick 执行源共用 `ContingencyManager`，Hybrid 继承 Tick 唯一执行源，
  live Bot 复用 Tick 泛型运行时。父单只有完整成交才激活 OTO/Bracket 子单，父单终态失败会撤销 held
  子单，OCO/Bracket 子单首次部分或完整成交即撤销兄弟单，迟到提交被确定性拒绝；所有已激活动作均
  重新进入正常 transport/report 链。VenueAccount/Portfolio 以 CurrencyId 聚合并保留 Instrument 明细。
- [x] ExecutionAlgorithm command producer：策略、execution algorithm 和 simulation hook 共用有界、
  可 reset 的 `PlatformCommandProducers`/`PlatformCommandBus`；Bar、Tick、Hybrid 与 live Tick 泛型路径均在
  市场投递点生成 canonical command，再进入各模式原有 risk/transport/matcher；origin 和容量错误保持显式。

## 最终验收（2026-08-24）

- [x] `cargo test --workspace --all-targets`：最终核心共享执行库 125 项测试通过，connector 两个目标
  各 31 项通过、各 1 项真实网络测试按定义 ignored。
- [x] `cargo check --workspace --all-targets` 与 `cargo fmt --all -- --check` 通过。
- [x] Python release 扩展重建后，`python -m unittest discover -s tests -v`：37 项通过，1 项外部行情 fixture 测试跳过。
- [x] Tick release A/B：100 万事件、30 轮、5 轮 warmup，ABI v8 与复审修复后的三次回归分别为
  +1.899%、+0.182%、+1.370%，中位 +1.370%，通过不超过 3% 的门槛；详见
  [`tick_shared_execution_release_benchmark.md`](tick_shared_execution_release_benchmark.md)。
