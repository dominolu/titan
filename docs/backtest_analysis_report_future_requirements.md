# Titan/HFTBacktest 回测分析与图表 P1/P2 后续需求

> 文档状态：后续阶段需求池（2026-08-26）
> 适用范围：P1 完整 HFT 研究与交易归因，以及 P2 高级研究、容量评估和纯 Rust 部署
> P0 基线：[`backtest_analysis_report_requirements.md`](backtest_analysis_report_requirements.md)

## 1. 文档目的

本文承接从 P0 需求基线中拆出的 P1/P2 能力。P1/P2 必须继续使用 P0 定义的
`ReportBundle`、canonical metric、ReportingCalendar、估值币种、adapter/renderer
能力协商和报告状态语义，不得建立第二套不兼容事实模型。

| 优先级 | 含义 |
|---|---|
| **P1** | 完整 HFT 研究、执行质量和交易归因所必需 |
| **P2** | 高级研究、容量评估或纯 Rust 部署能力 |

## 2. 后续架构要求

- **REQ-ARCH-006（P1）**：原生 HFT 报告必须能够与通用收益报告组合为同一 HTML，也必须能够独立生成。
- **REQ-ARCH-007（P2）**：未来 Rust renderer 必须消费同一 `ReportBundle`，不得建立第二套不兼容结果格式。
- **REQ-RPT-005（P1）**：支持从摘要、回撤、异常成交跳转或筛选到对应时间区间和明细数据。
- **REQ-SUM-004（P1）**：HFT 摘要增加 maker ratio、fill ratio、平均 spread capture、短周期 adverse selection 和延迟分位数。

## 3. 通用收益与风险增强

### 3.1 收益图增强

| 图表 | 优先级 | 最低输入 |
|---|---:|---|
| Log-scale Cumulative Returns | P1 | canonical returns |
| Volatility-matched Benchmark | P2 | strategy + benchmark returns |

- **REQ-RET-004（P1）**：支持线性和对数坐标，并明确是否复利。

### 3.2 滚动风险增强

| 图表 | 优先级 |
|---|---:|
| Rolling Sortino | P1 |
| Rolling Beta/Alpha | P1 |
| Rolling Correlation | P1 |

- **REQ-ROLL-003（P1）**：支持按报告频率配置窗口，并在图表元数据中记录配置。

### 3.3 收益分布增强

- **REQ-TAIL-002（P1）**：历史法、参数法或其他方法必须使用不同 method ID，不得混用同名结果。
- **REQ-TAIL-003（P1）**：收益分布必须允许排除或单独标识资金流、强平和数据异常事件。

### 3.4 回撤联动

- **REQ-DD-004（P1）**：支持将回撤区间与 Benchmark、Gross Exposure、Fee/Funding 和 HFT 执行指标联动展示。

### 3.5 日历增强

- **REQ-CAL-003（P1）**：跨夜 session 必须按 trading session 归属，不能仅按本地日期截断。
- **REQ-CAL-004（P1）**：Hour-of-day 图必须明确使用 exchange time、delivery time 或报告时区。

### 3.6 Benchmark 增强

- **REQ-BMK-004（P1）**：支持 buy-and-hold、策略旧版本和零费用运行作为 baseline。

## 4. 持仓、敞口和风险占用增强

P1 图表：

- Position quantity over time；
- Long、Short、Gross、Net Exposure；
- Leverage；
- Margin utilization；
- Concentration by Venue/Instrument/Currency；
- Top positions；
- 持仓持续时间分布；
- 风险限额使用率和 breach 时间线。

- **REQ-EXP-003（P1）**：仓位和账户图必须能选择 exchange-final 事实或 local-delivered 可见视图，并明确标注。
- **REQ-EXP-004（P1）**：支持按 Venue、Instrument、Currency 和 strategy tag 聚合。
- **REQ-EXP-005（P1）**：保证金和杠杆指标必须使用账户模型输出，不得由报告层猜测产品保证金规则。

## 5. PnL 与可扩展归因

归因维度：

- Venue；
- Instrument；
- Currency；
- Side；
- Liquidity role；
- Order type/TIF；
- Strategy/signal tag；
- 时间段和市场 regime。

- **REQ-PNL-003（P1）**：每个归因维度的总和必须与组合结果在允许误差内一致。
- **REQ-PNL-004（P1）**：报告必须区分 mark-to-market PnL 与 execution PnL，禁止把中间价漂移全部解释为成交质量。
- **REQ-PNL-005（P2）**：支持用户提供 factor/regime 标签进行可扩展归因。

## 6. 交易与成本增强

P1 指标和图表：

- Trade PnL 分布；
- Win rate、Profit factor；
- 持有期；
- Round trips；
- Fee、spread、slippage 和 market-impact proxy 分解。

- **REQ-TRD-003（P1）**：Round-trip 配对算法必须版本化并声明 FIFO、LIFO 或 average-cost 口径。
- **REQ-TRD-004（P1）**：对于做市和持续库存策略，round-trip 指标必须标记为辅助指标，不能替代账户级 PnL。
- **REQ-TRD-005（P1）**：交易成本必须支持 fee、spread、slippage、market impact proxy 的独立分解。

## 7. HFT 执行质量分析

### 7.1 Spread capture 与滑点

每个 fill 应尽可能计算：

- 相对 decision mid 的 implementation shortfall；
- 相对 arrival/ack mid 的 slippage；
- fill mid 和 quoted spread；
- effective spread；
- realized spread；
- price improvement；
- maker/taker 分类。

- **REQ-EXE-001（P1）**：每个价格基准必须包含价格、时间戳和来源；缺失时不得用 fill 时 mid 冒充 decision mid。
- **REQ-EXE-002（P1）**：买卖方向符号必须统一，使正值/负值含义在全部成本指标中一致。
- **REQ-EXE-003（P1）**：按 Venue、Instrument、Side、Liquidity Role 和 notional bucket 展示分布和分位数。

### 7.2 Adverse selection

默认 horizon 建议为 100ms、1s、5s、10s、60s，并允许配置。

- Markout curve；
- fill 后 mid-price 变化；
- maker 与 taker markout 对比；
- bid/ask 和不同 Venue 对比；
- 按波动率、spread、队列深度分桶。

- **REQ-ADV-001（P1）**：Markout 必须基于市场事件时间，并声明使用 exchange feed 还是 local-visible feed。
- **REQ-ADV-002（P1）**：数据结束前不足完整 horizon 的样本必须剔除或单独标记为 censored。
- **REQ-ADV-003（P1）**：成交方向必须标准化为“正值对策略有利”或明确相反定义。

### 7.3 Fill 与队列表现

- Fill ratio；
- Partial-fill ratio；
- Time-to-first-fill、time-to-complete；
- Queue waiting time；
- Quote lifetime；
- Cancel-to-fill ratio；
- Fill probability by distance-to-mid/spread/depth；
- Missed opportunity proxy。

- **REQ-FILL-001（P1）**：Fill ratio 的分母必须明确是 accepted quantity、eligible quantity、orders 或 quotes，不同口径必须使用不同名称。
- **REQ-FILL-002（P1）**：Post-only 被拒、主动撤单、过期和数据结束未完成必须分开统计。
- **REQ-FILL-003（P2）**：队列指标只有在模型输出可解释 queue state 时才可生成，禁止从普通 L2 快照伪造精确排队位置。

## 8. 订单生命周期与可靠性

### 8.1 订单漏斗

```text
Submitted
  ├── Local rejected
  └── Sent
       ├── Exchange rejected
       └── Accepted
            ├── Filled / Partially filled
            ├── Canceled
            └── Expired
```

要求展示：

- 各状态数量、比例和 notional；
- Reject reason 分布；
- Cancel reason 分布；
- Order type/TIF 分布；
- 活跃订单数量时间线；
- 重复 ID、非法状态转换和审计异常。

- **REQ-ORDRPT-001（P1）**：每次状态转换必须保留 exchange timestamp、delivery timestamp、sequence 和 reason code。
- **REQ-ORDRPT-002（P1）**：订单漏斗必须可按 Venue、Instrument 和策略标签筛选。
- **REQ-ORDRPT-003（P1）**：cancel/fill race 和数据结束 pending orders 必须作为独立诊断项。

## 9. 延迟分析

延迟必须根据存在的时间戳拆分，而不是只提供一个总延迟：

- Feed latency：exchange market event → local delivery；
- Strategy decision latency：callback delivery → command submit；
- Request transport：submit → exchange arrival；
- Exchange processing/queue：arrival → execution/ack；
- Response transport：exchange report → local delivery；
- End-to-end：decision/submit → local fill notification。

图表包括：

- p50/p90/p95/p99/p99.9；
- Histogram、CDF 和 tail plot；
- 随时间变化；
- 按 Venue/Instrument/operation/result 分桶；
- 延迟与 fill/slippage/adverse selection 的关系。

- **REQ-LAT-001（P1）**：所有延迟必须由同一 clock domain 或已知转换关系的时间戳计算。
- **REQ-LAT-002（P1）**：负延迟必须视为数据/时钟异常并进入 diagnostics，禁止取绝对值修复。
- **REQ-LAT-003（P1）**：均值不能替代尾部分位数；摘要至少包含 p50、p95、p99。
- **REQ-LAT-004（P2）**：支持对模型延迟和历史观测延迟分别标记来源。

## 10. 容量、流动性与市场影响代理

P2 可选能力：

- Participation rate；
- 成交量占市场成交量比例；
- 下单数量占可见深度比例；
- 按订单规模的 slippage/markout；
- PnL 对 fee、latency、spread、fill probability 的敏感度；
- 参数扫描中的 capacity frontier。

- **REQ-CAP-001（P2）**：所有容量结果必须声明是否启用了真实 market-impact 模型。
- **REQ-CAP-002（P2）**：未建模永久/临时冲击时，只能称为 capacity proxy，不得声称是可交易容量。
- **REQ-CAP-003（P2）**：敏感度分析必须保留每个 run fingerprint 和变更参数。

## 11. 多 Run、策略版本和引擎对比

支持比较：

- 策略参数组；
- 当前版本与 baseline；
- 有费/无费、有延迟/无延迟；
- Tick 与 Bar；
- Titan 与外部引擎；
- 不同 Venue/Instrument 组合。

P1 图表：

- Equity overlay；
- Return/Drawdown/Sharpe 对比；
- 指标矩阵和排名；
- 参数热力图；
- Pareto frontier；
- 差异时间线；
- Fill/account reconciliation。

- **REQ-CMP-001（P1）**：比较报告必须验证估值币种、时间范围、资本、日历和收益口径兼容性。
- **REQ-CMP-002（P1）**：非完全重叠区间必须明确显示，不得默认裁剪后隐藏差异。
- **REQ-CMP-003（P1）**：指标排名必须保留原始值和方向，禁止仅输出综合色分数。
- **REQ-CMP-004（P1）**：逐笔比较必须使用稳定 fill/order key 或明确的匹配算法版本。

## 12. 后续 ReportBundle 数据表

P1/P2 在 P0 bundle 基础上增加以下逻辑表。

### 12.1 `fills`

```text
fill_id, order_id, client_order_id
venue_id, instrument_id
side, order_type, tif, liquidity_role
exchange_ts, delivery_ts
price, qty, notional
fee, rebate, fee_currency
decision_ts, decision_price
arrival_ts, arrival_price
ack_ts, ack_price
queue_ahead_qty, queue_wait_ns
strategy_tag, signal_tag
```

### 12.2 `order_events`

```text
event_id, sequence, order_id, client_order_id
venue_id, instrument_id
event_kind, status, reason_code
exchange_ts, delivery_ts
side, order_type, tif
price, requested_qty, filled_qty, leaves_qty
strategy_tag, signal_tag
```

### 12.3 `market_marks`

```text
exchange_ts, delivery_ts
venue_id, instrument_id
bid, ask, mid, microprice
bid_qty, ask_qty, reference_source
```

- **REQ-DATA-006（P1）**：fill 和 order event ID 必须在单次 run 内稳定唯一。
- **REQ-DATA-007（P1）**：记录 exchange 与 local-delivered 两个视图时必须使用 `view_kind`，禁止混成同一条未标识序列。
- **REQ-DATA-008（P1）**：报告导出必须支持分表/分块，禁止要求将 tick 级 marks 全部复制进单个 Python 对象。

## 13. 指标、adapter 与 renderer 增强

- `money_weighted_return` 为 P2 可选收益类型。
- **REQ-DQ-005（P1）**：HTML 视觉变化可以独立于 metric schema 版本，但必须记录 renderer version。
- **REQ-CALC-004（P1）**：Rust 可以实现少量关键 canonical metrics 用于 CI 和 CLI，但无需承担全部报告指标。
- **REQ-ADP-005（P1）**：允许注册内部或用户自定义 adapter/renderer，不要求修改核心包源码。
- **REQ-ADP-006（P1）**：第三方 renderer 失败时，必须保留 canonical metrics、诊断和原始导出。

后续能力和后端：

```text
TRANSACTIONS, ROUND_TRIPS, PNL_ATTRIBUTION
ORDERS, HFT_EXECUTION, LATENCY, QUEUE_ANALYTICS
MULTI_RUN_COMPARISON
```

| 后端 | 优先级 | 预期能力 |
|---|---:|---|
| `native` HFT sections | P1 | PnL、positions、execution、diagnostics |
| `pyfolio-reloaded` | P1 可选 | returns、positions、transactions、round trips |
| `rust` | P2 | 无 Python的 canonical summary/HTML |

## 14. 输出、交互和性能增强

P1/P2 输出：

- PDF 打印版；
- PNG/SVG 单图导出；
- 交互式筛选和区间缩放；
- 多 run dashboard。

- **REQ-OUT-005（P1）**：交互筛选不得重新计算或改变 canonical metrics；筛选结果必须标记为 view-level analytics。
- **REQ-OUT-006（P1）**：应符合基本无障碍要求，包括可辨识对比度和非颜色编码。
- **REQ-PERF-005（P1）**：百万级 fills/orders 的报告必须先聚合后绘制，禁止把每个点直接嵌入浏览器 DOM。
- **REQ-PERF-006（P1）**：降采样算法必须保留首尾值、局部 extrema、回撤 peak/valley 和显式异常点。
- **REQ-PERF-007（P2）**：批量研究必须支持只计算结构化 metrics 而不渲染 HTML。
- **REQ-SEC-004（P1）**：外部模板和第三方 renderer 必须明确是否允许脚本；默认产物不得执行远程脚本。

## 15. P1 验收标准

- **AC-P1-001**：多资产组合净值、费用、Funding 和归因可 reconcile。
- **AC-P1-002**：fill/order ledger 支持 spread capture、markout、fill ratio、订单漏斗和延迟分位图。
- **AC-P1-003**：多 run 比较能验证口径兼容并显示非重叠区间。
- **AC-P1-004**：百万级明细不会导致浏览器逐点渲染或无界内存增长。

## 16. 后续交付阶段

### Phase 2：HFT 执行报告

- 标准 fills 和 order lifecycle ledger；
- Fee/Funding/PnL attribution；
- Spread capture、slippage、markout；
- Fill/Cancel/Reject、queue 和 latency 分析；
- 原生 HFT renderer。

### Phase 3：高级对比和容量

- 多 run 比较和参数热力图；
- regime/factor attribution；
- capacity proxies 和敏感度；
- PDF/交互式 dashboard；
- 评估 Rust-only renderer。

## 17. 后续待定决策

1. fills/order events 的默认记录级别和存储预算；
2. market marks 的 retention/downsampling 规则；
3. Round-trip 使用 FIFO、LIFO 还是 average-cost；
4. Markout 默认 horizon；
5. Fill ratio 的标准分母集合和 metric IDs；
6. Queue analytics 所需模型输出契约；
7. 多 run 对齐和逐笔匹配算法；
8. Pyfolio Reloaded 是否列为官方支持后端；
9. Rust-only renderer 的启动条件和目标产物；
10. PDF、交互式 Dashboard 和最终视觉主题。
