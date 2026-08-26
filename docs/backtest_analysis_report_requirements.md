# Titan/HFTBacktest 回测分析与图表 P0 需求规格

> 文档状态：需求基线草案（2026-08-26）
> 适用范围：Titan/HFTBacktest 的 Tick/L2/L3、Bar、Hybrid 和多资产回测结果分析 P0 基线
> 相关文档：
>
> - [`shared_execution_layer_requirements.md`](shared_execution_layer_requirements.md)
> - [`shared_execution_layer_acceptance_report.md`](shared_execution_layer_acceptance_report.md)
> - [`titan_nautilus_backtest_engine_feature_checklist.md`](titan_nautilus_backtest_engine_feature_checklist.md)
> - [`backtest_analysis_report_future_requirements.md`](backtest_analysis_report_future_requirements.md)（后续阶段需求）

## 1. 文档目的

本文定义 Titan/HFTBacktest 第一版统一回测分析、图表和报告能力，并作为
`ReportBundle`、第三方 adapter、原生摘要和验收测试的 P0 需求基线。

本文解决以下问题：

1. 一份完整回测报告必须回答哪些投资表现、风险和执行质量问题；
2. Rust 回测核心必须提供哪些可信原始数据；
3. 通用报告、原生摘要和第三方工具之间如何划分边界；
4. QuantStats、原生 Python renderer 和结构化导出如何切换；
5. 各项指标的时间、收益、年化、Benchmark 和多资产口径；
6. 第一阶段的交付范围和验收标准。

本文不固定最终 UI 样式，也不要求第一阶段实现全部图表。需求编号在实现、测试和评审中保持稳定。

## 2. 规范用语与优先级

| 用语 | 含义 |
|---|---|
| **必须（MUST）** | 目标阶段的硬性要求 |
| **禁止（MUST NOT）** | 实现不得出现的行为 |
| **应该（SHOULD）** | 默认应实现；偏离时必须记录理由 |
| **可以（MAY）** | 可选能力，不阻塞当前阶段验收 |

| 优先级 | 含义 |
|---|---|
| **P0** | 第一版可用报告和可信口径所必需 |

## 3. 总体结论与系统边界

目标架构采用“可信数据核心 + 统一报告模型 + 可插拔 adapter/renderer”：

```text
Rust Backtest Engine
├── Portfolio/Account snapshots
├── Position snapshots
├── Fill ledger
├── Order lifecycle
├── Fee/Funding/Execution metadata
└── Reproducibility metadata
            │
            ▼
      ReportBundle V1
            │
            ▼
     Canonical ReportData
      ┌─────┼──────────────┐
      ▼     ▼              ▼
 QuantStats Native       Export
  Adapter   Renderer    JSON/Parquet
```

- **REQ-ARCH-001（P0）**：Rust 回测核心必须拥有会影响账务和执行语义的事实数据，报告层禁止从图表或最终汇总反推成交、费用、Funding 或账户变化。
- **REQ-ARCH-002（P0）**：必须定义独立于 QuantStats、pandas 和具体绘图库的版本化 `ReportBundle`。
- **REQ-ARCH-003（P0）**：第三方工具必须通过 adapter 接入，第三方类型和参数禁止渗透到 Rust 核心公共领域模型。
- **REQ-ARCH-004（P0）**：renderer 只负责指标呈现和产物生成，不得改变净值、成交或账务事实。
- **REQ-ARCH-005（P0）**：QuantStats 必须是可选依赖，不得成为 hftbacktest 核心安装和 Rust-only 使用的前置条件。

### 3.1 非目标

第一阶段不要求：

- 在 Rust 中完整重写 QuantStats；
- 把 tick 级全部行情嵌入 HTML；
- 在线下载 Benchmark、无风险利率或市场数据；
- 提供实盘交易终端或实时监控 Dashboard；
- 在报告层修复无效回测数据或静默填补关键字段；
- 进行因果性容量/市场冲击仿真；仅可报告已有模型产生的结果和代理指标。

## 4. 用户与核心使用场景

### 4.1 策略研究员

- 快速判断策略收益是否稳定、风险是否可接受；
- 查看收益是否由少数月份、品种或极端行情贡献；
- 比较手续费前后、Funding 前后表现；
- 定位回撤区间并回到对应市场事件。

### 4.2 风控与模型验证

- 验证杠杆、敞口、保证金、风险限额和尾部损失；
- 检查账户恒等式、数据完整性和运行警告；
- 对两个引擎、模型版本或参数组做可复现比较。

### 4.3 批量研究与 CI

- 无交互生成 JSON/CSV/Parquet/HTML；
- 从大量 runs 中提取结构化指标并排序；
- 对关键指标和图表数据运行回归测试；
- 报告生成失败不得使已完成的回测结果丢失。

## 5. 报告层级与默认结构

一份完整报告由以下层级组成：

1. **Executive Summary**：一页判断表现、风险和数据可信度；
2. **Returns & Risk**：QuantStats 风格通用投资组合分析；
3. **Drawdown & Tail Risk**：回撤和尾部风险；
4. **Exposure & Attribution**：持仓、敞口和基础 PnL 分解；
5. **Trading & Costs**：成交、换手和交易成本；
6. **Reproducibility & Diagnostics**：配置、模型、数据和警告。

- **REQ-RPT-001（P0）**：每个 section 必须声明 `available`、`partial`、`unavailable` 或 `failed` 状态。
- **REQ-RPT-002（P0）**：字段不足时必须明确标记跳过原因，禁止用零值代替未知值。
- **REQ-RPT-003（P0）**：摘要必须显示报告计算口径，包括时区、日切、估值币种、年化周期和收益类型。
- **REQ-RPT-004（P0）**：所有表格和图表必须能追溯到 `ReportBundle` 字段和计算版本。

## 6. Executive Summary 需求

### 6.1 运行身份和可信度

- Run ID、run fingerprint；
- 引擎版本、Git revision；
- 策略 ID、版本和参数摘要；
- 数据集/manifest 哈希；
- Matching、Fee、Latency、Risk、Execution Quality 模型版本；
- 起止 exchange/delivery 时间；
- termination、end policy；
- warnings 和 capability downgrades；
- 报告 adapter、renderer 和计算版本。

### 6.2 核心指标卡

P0 默认指标：

- Net Return、Annualized Return/CAGR；
- Sharpe、Sortino、Calmar；
- Maximum Drawdown、最长回撤时长；
- Annualized Volatility；
- Profit Factor 或 Gain-to-Pain；
- 总费用、返佣、Funding、净交易成本；
- 总成交次数、成交额、日均换手；
- 最大 Gross/Net Exposure、最大 Leverage；
- Benchmark Return、Alpha/Beta/Information Ratio（有 Benchmark 时）；
- 回测耗时、市场事件数、订单和成交计数。

- **REQ-SUM-001（P0）**：核心指标必须区分 gross 与 net，不得只展示手续费前收益。
- **REQ-SUM-002（P0）**：不适用的指标显示 `N/A` 及原因，例如无 Benchmark 时不得显示伪造的 Alpha。
- **REQ-SUM-003（P0）**：摘要必须显示初始资本或 book size；缺失时所有百分比资本指标必须标记口径受限。

## 7. 通用收益与风险图表

### 7.1 净值与累计收益

| 图表 | 优先级 | 最低输入 |
|---|---:|---|
| Net/Gross Equity Curve | P0 | portfolio snapshots |
| Cumulative Returns | P0 | canonical returns |
| Strategy vs Benchmark | P0 | strategy + benchmark returns |

- **REQ-RET-001（P0）**：净值图必须至少提供 net equity，并可选择叠加 gross equity。
- **REQ-RET-002（P0）**：Benchmark 必须与策略收益使用相同时间边界和缺失值策略。
- **REQ-RET-003（P0）**：外部资金流必须与投资收益分离，不得由简单 `equity.pct_change()` 错计为收益。

### 7.2 滚动风险指标

| 图表 | 优先级 |
|---|---:|
| Rolling Return | P0 |
| Rolling Volatility | P0 |
| Rolling Sharpe | P0 |

- **REQ-ROLL-001（P0）**：窗口必须显示真实跨度和样本数，例如 `30D/30 samples`，禁止只显示含糊的“rolling”。
- **REQ-ROLL-002（P0）**：样本不足的窗口必须为空，不得隐式缩短窗口。

### 7.3 收益分布与尾部风险

- 日/周/月收益分布直方图；
- QQ 图；
- 正负收益箱线图；
- VaR、CVaR/Expected Shortfall；
- Best/Worst day、week、month；
- Skew、Kurtosis；
- 连续盈利/亏损区间。

- **REQ-TAIL-001（P0）**：VaR/CVaR 必须声明置信度、计算方法和样本频率。

## 8. 回撤分析

P0 图表和表格：

- Underwater Drawdown；
- Top N Drawdown Periods；
- 每个回撤的 peak、valley、recovery；
- 深度、持续时间、恢复时间；
- 当前是否仍在回撤；
- 回撤区间的收益、波动、成交和敞口摘要。

- **REQ-DD-001（P0）**：回撤必须从 net equity 或 net cumulative wealth 计算，默认不得从逐期 PnL 绝对值直接计算。
- **REQ-DD-002（P0）**：未恢复回撤的 recovery 必须为空并标记 ongoing。
- **REQ-DD-003（P0）**：Top drawdowns 的区间不得重叠，排序规则必须稳定。

## 9. 日历与周期表现

- 月度收益热力图；
- 年度收益柱状图；
- 月度/季度/年度收益表；
- Day-of-week、Hour-of-day 表现；
- 月内、周内和交易时段分布；
- 盈利月份比例和最差月份。

- **REQ-CAL-001（P0）**：月度热力图必须基于复合周期收益，而不是周期内简单求和，除非输入明确为 additive PnL return。
- **REQ-CAL-002（P0）**：日切必须由 `ReportingCalendar` 定义；Crypto 默认 UTC 自然日，传统市场使用配置的交易日历。

## 10. Benchmark 与相对表现

Benchmark 可以来自调用方提供的 return/equity 序列、静态文件或同一批次的 baseline run。报告生成默认禁止联网下载。

P0 指标：

- Excess Return；
- Tracking Error；
- Information Ratio；
- Beta、Alpha；
- Correlation；
- Up/Down Capture；
- Strategy/Benchmark drawdown 对比。

- **REQ-BMK-001（P0）**：Benchmark 必须携带 ID、数据来源、币种、时区和频率。
- **REQ-BMK-002（P0）**：必须显示对齐前后样本范围和丢弃样本数量。
- **REQ-BMK-003（P0）**：禁止对不同估值币种的序列直接比较；必须先提供显式 FX 转换。

## 11. 持仓、敞口和风险占用

P0 图表：

- Position quantity over time；
- Long、Short、Gross、Net Exposure；
- Leverage；
- Margin utilization；
- Concentration by Venue/Instrument/Currency；
- Top positions；
- 持仓持续时间分布；
- 风险限额使用率和 breach 时间线。

- **REQ-EXP-001（P0）**：多资产敞口必须统一转换到报告估值币种。
- **REQ-EXP-002（P0）**：Gross Exposure 必须是绝对名义价值之和；Net Exposure 必须保留方向。

## 12. PnL、费用与归因

### 12.1 PnL 分解

- Gross trading PnL；
- Realized/Unrealized PnL；
- Fee、commission、rebate；
- Funding；
- FX translation；
- Liquidation/settlement；
- Net PnL。

- **REQ-PNL-001（P0）**：必须满足并验证账户恒等式，且报告展示 reconciliation 状态。
- **REQ-PNL-002（P0）**：Fee、rebate 和 Funding 必须分别保存和展示，不得只保留净额。

## 13. 交易与成本分析

P0 指标和图表：

- Number of fills/trades；
- Trading volume/value；
- Turnover；
- 平均/中位成交数量；
- Fee per traded value；
- 成本占 Gross PnL 比例；

- **REQ-TRD-001（P0）**：必须区分 fill、order 和 round trip，禁止把三者统称为 trade。
- **REQ-TRD-002（P0）**：部分成交必须作为独立 fill 保存，同时保留所属 order ID。
## 14. 数据质量、诊断与可复现性

报告必须包含以下检查：

- 时间戳是否单调；
- 重复、缺失和逆序记录；
- NaN、Inf、非法价格/数量；
- 账户恒等式和累计值单调性；
- 组合净值是否可重建；
- Benchmark 对齐覆盖率；
- 不完整 session；
- 数据结束时 pending orders/undelivered reports；
- capability downgrade；
- 被跳过图表及原因。

- **REQ-DQ-001（P0）**：严重账务不一致必须使报告状态为 invalid，即使仍生成诊断产物。
- **REQ-DQ-002（P0）**：报告必须保留引擎 warnings，不得只写日志后丢弃。
- **REQ-DQ-003（P0）**：所有自动清洗、resample、对齐和样本剔除必须计数并进入 metadata。
- **REQ-DQ-004（P0）**：相同 `ReportBundle`、报告配置和 renderer 版本必须得到数值一致的结构化指标。

## 15. Canonical ReportBundle 数据契约

`ReportBundle` 必须版本化，并以逻辑表而非第三方对象定义。物理格式可以是内存结构、Arrow、Parquet、NPZ 或稳定 FFI view。

### 15.1 `run_metadata`

最低字段：

```text
schema_version, run_id, run_fingerprint
engine_version, git_revision
strategy_id, strategy_version, strategy_parameters
runtime_abi_version, phase_contract_version
data_manifest_hash, config_hash, random_seed
model identities and config hashes
start/end exchange_ts, start/end delivery_ts
termination, end_policy, reporting_currency
timezone, reporting_calendar, initial_capital
warnings, capability_downgrades
```

### 15.2 `portfolio_snapshots`

```text
timestamp, timestamp_kind
equity_gross, equity_net, cash
realized_pnl, unrealized_pnl
fee, rebate, funding, external_flow
gross_exposure, net_exposure, margin, leverage
reporting_currency
```

### 15.3 `account_snapshots`

```text
timestamp, view_kind
venue_id, currency_id
balance, fee, rebate, funding
realized_pnl, unrealized_pnl, margin
```

### 15.4 `position_snapshots`

```text
timestamp, view_kind
venue_id, instrument_id, currency_id
quantity, mark_price, notional
realized_pnl, unrealized_pnl, margin
```

### 15.5 `benchmark`

```text
timestamp, benchmark_id
equity_or_return, value_kind
currency, timezone, source
```

- **REQ-DATA-001（P0）**：所有表必须有 schema version 和稳定字段语义。
- **REQ-DATA-002（P0）**：累计字段和区间增量字段必须在名称或 schema 中明确区分。
- **REQ-DATA-003（P0）**：组合级净值必须由账户/组合账本产生；禁止简单相加每资产收益率。
- **REQ-DATA-004（P0）**：所有货币金额必须携带币种或已明确转换为 reporting currency。
- **REQ-DATA-005（P0）**：缺失值必须用 null/option 表达，禁止使用零、NaN 或 magic number 表示未知。

## 16. 指标计算口径

### 16.1 收益类型

必须显式区分：

- `simple_return`；
- `log_return`；
- `additive_pnl_return`；
- `time_weighted_return`；

默认通用报告使用 net equity 的 time-weighted simple return；无外部资金流时等价于净值百分比变化。

### 16.2 频率和年化

- Crypto 默认 `365` reporting days/year；
- 传统交易日默认由 calendar 推导，常见为 `252`；
- 年化值必须记录 `periods_per_year`；
- intraday 报告不得直接套用日频常数；
- 低于最小有效样本数时必须返回 N/A。

### 16.3 无风险利率

- 默认显式为 0，而不是隐式省略；
- 非零值必须声明年化/周期化口径和来源；
- 报告生成不得默认联网获取。

### 16.4 估值和日切

- 估值价格来源必须配置：mid、last、settlement 或模型 mark；
- 跨币种必须提供 FX marks；
- day/session cutoff 必须由 `ReportingCalendar` 处理；
- 无效或 stale mark 必须进入 diagnostics。

- **REQ-CALC-001（P0）**：每个核心指标必须具有稳定 metric ID、版本、输入频率和参数记录。
- **REQ-CALC-002（P0）**：adapter 输出的同名指标必须标注 provider；不得把 QuantStats 数值冒充 Titan canonical metric。
- **REQ-CALC-003（P0）**：canonical metrics 与第三方 metrics 不一致时，摘要默认使用 canonical 值并显示差异诊断。

## 17. Adapter、Renderer 与能力协商

### 17.1 抽象接口

概念接口：

```python
class ReportAdapter(Protocol):
    name: str
    capabilities: frozenset[ReportCapability]

    def prepare(self, data: ReportData, config: ReportConfig) -> PreparedReport:
        ...

class ReportRenderer(Protocol):
    name: str
    capabilities: frozenset[ReportCapability]

    def render(self, report: PreparedReport, output: Path) -> ReportArtifact:
        ...
```

用户默认 API：

```python
report = BacktestReport.from_result(result)
report.generate("report.html", renderer="quantstats")
report.generate("report.html", renderer="native")
```

### 17.2 能力枚举

最低能力集合：

```text
RETURNS, BENCHMARK, DRAWDOWNS, CALENDAR_RETURNS
POSITIONS, EXPOSURES, FEES, FUNDING
SELF_CONTAINED_HTML
```

### 17.3 后端目标矩阵

| 后端 | 定位 | 预期能力 |
|---|---|---|
| `quantstats` | P0 | returns、benchmark、drawdowns、calendar、HTML |
| `native` | P0 | canonical summary、PnL、positions、diagnostics |
| `json/parquet` | P0 | 结构化数据交换和批量研究 |

- **REQ-ADP-001（P0）**：adapter 必须在生成前执行能力协商并返回缺失 section 列表。
- **REQ-ADP-002（P0）**：不支持的能力必须跳过并记录，禁止静默生成不完整图表。
- **REQ-ADP-003（P0）**：QuantStats adapter 只能消费已规范化收益和 Benchmark，禁止由其自行决定组合账务。
- **REQ-ADP-004（P0）**：第三方依赖必须位于 optional dependency group，并锁定经过验收的版本范围。

## 18. 输出格式、交互和视觉要求

### 18.1 输出格式

P0：

- 单文件 HTML 或明确的 HTML 资源目录；
- JSON metrics；
- CSV/Parquet 数据表；
- Notebook 中可显示的对象。

### 18.2 视觉规范

- 收益默认绿色、损失/回撤默认红色，但不得只依赖颜色表达；
- 同类图表保持一致单位、小数位和方向语义；
- 币种、百分比、数量和时间单位必须显示；
- 图例不得遮挡关键数据；
- 大样本图必须降采样，但 extrema、回撤峰谷和异常点必须保留；
- 打印和深色/浅色主题下保持可读。

- **REQ-OUT-001（P0）**：报告默认必须可离线打开，不得依赖运行时网络请求或 CDN。
- **REQ-OUT-002（P0）**：HTML 顶部必须显示报告状态：valid、partial、invalid 或 failed。
- **REQ-OUT-003（P0）**：所有图表必须有标题、单位、时间范围和口径提示。
- **REQ-OUT-004（P0）**：结构化 JSON 必须使用稳定 metric IDs，不能使用本地化显示名称作为 key。

## 19. 性能、资源和稳定性

- **REQ-PERF-001（P0）**：默认 recorder/report 能力不得进入撮合热路径执行复杂指标或图表逻辑。
- **REQ-PERF-002（P0）**：详细 fills/orders/marks 必须支持有界 buffer、分块写出或流式 sink。
- **REQ-PERF-003（P0）**：报告生成不得修改或重新运行策略。
- **REQ-PERF-004（P0）**：报告失败不得影响已经完成并持久化的 `BacktestResult/ReportBundle`。

## 20. 安全与隐私

- **REQ-SEC-001（P0）**：报告禁止包含 API key、secret、账户凭据和原始连接配置中的敏感值。
- **REQ-SEC-002（P0）**：策略参数和 metadata 必须支持字段级 redact。
- **REQ-SEC-003（P0）**：HTML 中的用户文本、symbol、strategy tag 和 reason 必须正确转义。

## 21. 测试与验收

### 21.1 单元测试

- 收益、复利、年化、Sharpe、Sortino、Calmar；
- 回撤 peak/valley/recovery；
- 月度收益；
- 外部资金流调整；
- 多币种换算；
- 时间窗口、日切和 Benchmark 对齐；
- capability negotiation。

### 21.2 不变量测试

- Portfolio/account reconciliation；
- Gross/Net PnL reconciliation；
- 归因之和等于组合值；
- fill 数量与引擎 `fill_count` 一致；
- order 终态计数与 result counters 一致；
- 费用只入账一次；
- 相同 bundle/config 产生相同 canonical metrics。

### 21.3 Golden 与交叉验证

- 小型人工可计算 fixture；
- 有手续费、返佣、Funding 的 fixture；
- 多资产、多 Venue、多币种 fixture；
- partial fill、cancel/fill race、ongoing drawdown；
- QuantStats adapter 输入 golden；
- HTML 关键 section 和结构 snapshot；
- 与现有 `hftbacktest.stats` 指标对照。

- **AC-P0-001**：单资产回测能生成离线 HTML、JSON metrics 和 canonical 数据导出。
- **AC-P0-002**：HTML 至少包含摘要、净值、累计收益、回撤、月度收益、滚动风险、交易量/费用和 diagnostics。
- **AC-P0-003**：QuantStats adapter 可替换为 native renderer，Rust 结果和用户回测调用无需变化。
- **AC-P0-004**：缺失 Benchmark、positions 或 fills 时报告可降级生成，并逐项显示跳过原因。
- **AC-P0-005**：核心 metrics 在人工 fixture 上与预期值一致，且 provider 差异可解释。
- **AC-P0-006**：非法账务或非单调关键记录使报告标记 invalid，而不是输出误导性 valid 报告。

## 22. 分阶段交付建议

### Phase 0：需求和口径冻结

- 冻结本文 P0 需求；
- 确定 canonical metric IDs；
- 确定 ReportingCalendar、return type 和 reporting currency 规则；
- 确定 `ReportBundle V1` 逻辑 schema。

### Phase 1：通用报告 MVP

- 组合级 portfolio snapshots；
- `ReportData`、validation 和 canonical metrics；
- JSON/Parquet 导出；
- QuantStats optional adapter；
- Native summary/diagnostics；
- 离线 HTML 合并产物。

## 23. 待定决策

以下事项必须在 Phase 0 结束前决定：

1. P0 canonical metrics 的精确公式和版本策略；
2. `ReportBundle V1` 的首选物理格式：Arrow/Parquet、NPZ 或混合；
3. Portfolio snapshot 的默认采样触发和频率；
4. 初始资本、外部资金流和收益口径 API；
5. Crypto 与传统市场的 ReportingCalendar 配置方式；
6. Benchmark 是 bundle 内表、外部输入还是同时支持；
7. QuantStats 输出是嵌入原生 HTML，还是作为独立 appendix；
8. canonical metrics 在 Rust 与 Python 之间的所有权边界。

## 24. 推荐的第一版用户体验

```python
from hftbacktest.reporting import BacktestReport, ReportConfig

report = BacktestReport.from_result(
    result,
    config=ReportConfig(
        reporting_currency="USDT",
        calendar="crypto_utc",
        periods_per_year=365,
        initial_capital=100_000,
    ),
)

artifact = report.generate(
    "backtest-report.html",
    renderer="quantstats",
    include_native_sections=True,
)
```

批量研究只计算结构化指标：

```python
metrics = report.metrics()
report.export("report-bundle", format="parquet")
```

后端切换：

```python
report.generate("native-report.html", renderer="native")
```

上述调用必须建立在同一个 `ReportBundle` 和 canonical 口径上；切换后端只能改变可用 section 和呈现方式，不能改变回测事实。
