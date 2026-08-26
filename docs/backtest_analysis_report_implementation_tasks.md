# Titan/HFTBacktest P0 回测分析报告实现任务

> 依据：[`backtest_analysis_report_requirements.md`](backtest_analysis_report_requirements.md)
> 状态：P0 报告侧核心链路及审查修复已完成；引擎 round-trip/risk facts 与真实 QuantStats 环境验收尚未关闭（2026-08-26）

## 实施原则

- 回测事实、canonical 指标、第三方适配和视觉渲染分层；
- QuantStats、pandas 只作为可选报告依赖；
- 现有 `hftbacktest.stats` API 保持兼容；
- 缺少可选数据时显式降级，账务不合法时标记 invalid；
- 每个阶段具备独立测试和可验收产物。

## 任务清单

### TASK-RPT-001：P0 数据契约

- [x] `ReportConfig`、`RunMetadata`、`ReportBundle`；
- [x] portfolio/account/position/benchmark 逻辑表；
- [x] 单资产 legacy recorder 转换入口；
- [x] canonical portfolio snapshots 直接构造入口；
- [x] schema version 和输入防御性复制。

### TASK-RPT-002：校验和报告状态

- [x] `valid/partial/invalid` 数据状态及 section-level `failed`；
- [x] Native renderer 致命失败时生成最小 `failed` 诊断产物；
- [x] schema、时间戳、有限数值、净值和累计字段校验；
- [x] gross/net/fee/rebate/funding reconciliation；
- [x] Benchmark 覆盖和可选 section 诊断；
- [x] validation issues 结构化输出。

### TASK-RPT-003：日历、收益和 canonical metrics

- [x] Crypto UTC 和可配置时区/日切；
- [x] 外部资金流调整后的 TWR simple return；
- [x] 日/月/年复合收益；
- [x] Return、CAGR、Volatility、Sharpe、Sortino、Calmar；
- [x] Drawdown、VaR/CVaR、Skew/Kurtosis；
- [x] 费用、Funding、成交、换手、敞口和 Benchmark 指标；
- [x] 稳定 metric ID、版本、单位、provider。

### TASK-RPT-004：Adapter/Renderer 能力层

- [x] adapter/renderer protocol 和 registry；
- [x] capability negotiation；
- [x] Native adapter/renderer；
- [x] QuantStats optional adapter/renderer；
- [x] 后端不可用时的显式降级。

### TASK-RPT-005：P0 离线报告

- [x] Executive Summary；
- [x] Equity/Cumulative Returns；
- [x] Underwater/Top Drawdowns；
- [x] Monthly/Annual Returns；
- [x] Rolling Risk 和 Return Distribution；
- [x] Position/Exposure、Fee/Funding/Trading；
- [x] Diagnostics 和 reproducibility metadata；
- [x] 无 CDN 的离线 HTML。

### TASK-RPT-006：结构化导出

- [x] metrics JSON；
- [x] bundle manifest JSON；
- [x] Parquet/CSV 分表导出；
- [x] 原子目录替换和失败不损坏旧产物；
- [x] metadata 敏感字段 redact。

### TASK-RPT-007：公共 API 与兼容

- [x] `hftbacktest.reporting` 公共 API；
- [x] `BacktestReport.from_record/from_bundle`；
- [x] `metrics/generate/export`；
- [x] `LinearAssetRecord.report()` 兼容入口；
- [x] optional dependency 和使用文档。

### TASK-RPT-008：测试与验收

- [x] 人工可计算收益/回撤 fixture；
- [x] 外部资金流和 fee/rebate/funding fixture；
- [x] invalid/partial/backend fallback；
- [x] Benchmark 对齐；
- [x] Parquet/JSON/CSV 导出；
- [x] Native HTML 端到端；
- [x] 旧 stats 回归测试。

## 审查修复（2026-08-26）

- [x] 首期亏损从初始 high-water mark 计算回撤；
- [x] integer epoch 先按 UTC 解释，再转换报告时区；
- [x] 年化收益执行最小样本门槛；
- [x] account/position 完整 schema、币种、cash/exposure reconciliation；
- [x] 缺失账务字段保留 null 并使报告 invalid，不再静默补零；
- [x] Benchmark 声明时区转换及对齐前后样本审计；
- [x] section 独立状态和 renderer section failure 回传；
- [x] `num_trades` 不再冒充 fill count，Gain-to-Pain 不再冒充 Profit Factor；
- [x] 配置化 weekday/holiday 传统交易日历；
- [x] 扩展 metadata 敏感字段匹配；
- [x] 唯一 backup 路径和导出失败回滚测试；
- [x] Position、Benchmark drawdown、Trading Activity 展示；
- [x] metric version、parameters、frequency 和报告口径展示。
- [x] Rust `BacktestResult` 保留 execution reports，Python `from_result` 转换独立 fill/order facts；
- [x] `run_event_bot(return_result=True)` 从 Rust 运行时复制 canonical execution reports，默认 state-array 返回保持兼容；
- [x] 部分成交保持独立 fill，并校验 fill/order engine counters；
- [x] Rust 状态机与集成测试覆盖 pending-cancel/fill race、同时间 fill-before-cancel 和 partial IOC 终态；
- [x] 原币金额保留、as-of FX marks 显式派生 `*_reporting`，完成多 Venue cash/exposure reconciliation；
- [x] `risk_events` 限额使用率/breach 和 `market_marks` age/stale 诊断事实及时间线；
- [x] Notebook self-contained rich display 和 extrema-preserving 有界降采样；
- [x] QuantStats adapter/renderer mocked contract golden 测试。
- [x] QuantStats 0.0.81 optional 环境真实 standalone HTML 集成测试（未安装依赖时自动 skip）。

## P0 关闭前仍需上游能力

- [ ] Rust/recorder 原生提供 round-trip 事实表；fill/order 已由 execution reports 接入并生成稳定逻辑 ID；
- [ ] 引擎/recorder 直接产生 FX、risk-limit 和 mark-age facts；报告侧契约、换算、校验与图表已完成；
