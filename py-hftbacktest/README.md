# Py-HftBacktest

## P0 backtest report

The reporting API keeps the canonical portfolio data and metrics independent from any third-party
tear-sheet library. Native offline HTML is always available; QuantStats is optional.

```python
from hftbacktest.reporting import BacktestReport, ReportConfig

config = ReportConfig(
    reporting_currency="USDT",
    initial_capital=100_000,
    calendar="crypto_utc",
)
report = BacktestReport.from_record(recorder.get(0), config)

report.metrics()
report.generate("backtest-report.html", renderer="native")
report.export("report-bundle", format="parquet")
```

Install the optional QuantStats backend with:

```shell
pip install "hftbacktest[reports]"
```

Then generate a native report with a linked QuantStats appendix:

```python
report.generate("backtest-report.html", renderer="quantstats", include_native_sections=True)
```

For a multi-asset strategy, pass authoritative portfolio snapshots through
`BacktestReport.from_portfolio(...)`; per-asset legacy recorder returns are deliberately not summed.
Canonical portfolio input must explicitly provide cumulative `fee`, `rebate`, `funding` and
interval `external_flow`; missing accounting facts remain null and make the report invalid instead
of being treated as zero.

`BacktestReport.from_result(...)` converts retained engine execution reports into separate order
and fill tables, preserving partial fills. `from_portfolio(...)` also accepts optional
`round_trip_events`, `fx_marks`, `risk_events`, and `market_marks` tables. Foreign-currency source
amounts remain intact while explicit as-of FX conversion creates `*_reporting` columns used for
portfolio reconciliation and charts. Set `max_mark_age_ns` to audit the supplied stale-mark flags.

The Rust-owned event runtime preserves its state-array return by default. Request a copied result
snapshot when the report needs authoritative execution facts:

```python
from hftbacktest import run_event_bot

result = run_event_bot(..., return_result=True)
report = BacktestReport.from_result(
    result,
    config,
    portfolio_snapshots=portfolio_snapshots,
    currency_map={0: "USDT"},
)
```

`EventBotResult` exposes canonical execution reports and Venue/Order composite counters without
transferring Rust-owned memory to Python.

Traditional sessions can configure a timezone, cutoff, weekdays and holidays:

```python
config = ReportConfig(
    reporting_currency="USD",
    initial_capital=1_000_000,
    calendar="weekday",
    timezone="America/New_York",
    day_cutoff_hour=17,
    trading_weekdays=(0, 1, 2, 3, 4),
    calendar_holidays=frozenset({"2026-01-01"}),
)
```

To build the shared library for testing the Python bindings, run the command below.

```
maturin develop
```
