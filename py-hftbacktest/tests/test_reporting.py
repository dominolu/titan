import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import numpy as np
import polars as pl

from hftbacktest.reporting import (
    BackendUnavailableError,
    BacktestReport,
    ReportConfig,
    ReportStatus,
    RunMetadata,
    SectionAvailability,
)
from hftbacktest.reporting.bundle import ensure_timestamp
from hftbacktest.reporting.backends import QuantStatsAdapter
from hftbacktest.reporting.renderers import _extrema_indices
from hftbacktest.stats import LinearAssetRecord
from hftbacktest.types import record_dtype


def timestamps(count: int) -> list[datetime]:
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    return [start + timedelta(days=index) for index in range(count)]


def complete_report(
    equity: list[float] | None = None,
    flows: list[float] | None = None,
    *,
    benchmark: pl.DataFrame | None = None,
    metadata: dict | None = None,
) -> BacktestReport:
    equity = equity or [100.0, 110.0, 165.0, 181.5]
    flows = flows or [0.0, 0.0, 55.0, 0.0]
    ts = timestamps(len(equity))
    portfolio = pl.DataFrame(
        {
            "timestamp": ts,
            "equity_gross": equity,
            "equity_net": equity,
            "external_flow": flows,
            "fee": [0.0] * len(equity),
            "rebate": [0.0] * len(equity),
            "funding": [0.0] * len(equity),
            "num_trades": list(range(len(equity))),
            "trading_volume": [float(index) for index in range(len(equity))],
            "trading_value": [index * 10.0 for index in range(len(equity))],
            "gross_exposure": [10.0] * len(equity),
            "net_exposure": [10.0] * len(equity),
            "leverage": [0.1] * len(equity),
        }
    )
    accounts = pl.DataFrame(
        {
            "timestamp": ts,
            "view_kind": ["local_delivered"] * len(equity),
            "venue_id": ["test"] * len(equity),
            "currency_id": ["USD"] * len(equity),
            "balance": equity,
            "fee": [0.0] * len(equity),
            "rebate": [0.0] * len(equity),
            "funding": [0.0] * len(equity),
            "realized_pnl": [None] * len(equity),
            "unrealized_pnl": [None] * len(equity),
            "margin": [None] * len(equity),
        }
    )
    positions = pl.DataFrame(
        {
            "timestamp": ts,
            "view_kind": ["local_delivered"] * len(equity),
            "venue_id": ["test"] * len(equity),
            "instrument_id": ["TEST"] * len(equity),
            "currency_id": ["USD"] * len(equity),
            "quantity": [1.0] * len(equity),
            "mark_price": [10.0] * len(equity),
            "notional": [10.0] * len(equity),
            "realized_pnl": [None] * len(equity),
            "unrealized_pnl": [None] * len(equity),
            "margin": [None] * len(equity),
        }
    )
    fill_count = max(len(equity) - 1, 0)
    fills = pl.DataFrame(
        {
            "timestamp": ts[1:],
            "exchange_timestamp": ts[1:],
            "fill_id": [f"fill-{index}" for index in range(fill_count)],
            "order_id": list(range(1, fill_count + 1)),
            "venue_id": ["test"] * fill_count,
            "instrument_id": ["TEST"] * fill_count,
            "side": ["buy"] * fill_count,
            "quantity": [1.0] * fill_count,
            "price": [10.0] * fill_count,
            "notional": [10.0] * fill_count,
            "fee": [0.0] * fill_count,
            "rebate": [0.0] * fill_count,
            "currency": ["USD"] * fill_count,
            "liquidity": ["maker"] * fill_count,
        }
    )
    orders = pl.DataFrame(
        {
            "timestamp": ts[1:],
            "exchange_timestamp": ts[1:],
            "event_sequence": list(range(fill_count)),
            "order_id": list(range(1, fill_count + 1)),
            "venue_id": ["test"] * fill_count,
            "instrument_id": ["TEST"] * fill_count,
            "side": ["buy"] * fill_count,
            "status": ["filled"] * fill_count,
            "request": ["new"] * fill_count,
            "quantity": [1.0] * fill_count,
            "price": [10.0] * fill_count,
            "executed_quantity": [1.0] * fill_count,
            "reason": ["none"] * fill_count,
        }
    )
    config = ReportConfig(reporting_currency="USD", initial_capital=100.0)
    report_metadata = dict(metadata or {})
    report_metadata.setdefault("fill_count", fill_count)
    report_metadata.setdefault("order_count", fill_count)
    return BacktestReport.from_portfolio(
        portfolio,
        config,
        account_snapshots=accounts,
        position_snapshots=positions,
        benchmark=benchmark,
        fill_events=fills,
        order_events=orders,
        metadata=report_metadata,
    )


class TestReportConfig(unittest.TestCase):
    def test_rejects_invalid_accounting_configuration(self):
        with self.assertRaises(ValueError):
            ReportConfig(reporting_currency="USD", initial_capital=0.0)
        with self.assertRaises(ValueError):
            ReportConfig(reporting_currency="USD", initial_capital=1.0, day_cutoff_hour=24)
        with self.assertRaises(ValueError):
            ReportConfig(reporting_currency="USD", initial_capital=1.0, asset_type="option")
        with self.assertRaises(ValueError):
            ReportConfig(
                reporting_currency="USD",
                initial_capital=1.0,
                minimum_annualization_samples=1,
            )
        with self.assertRaises(ValueError):
            ReportConfig(
                reporting_currency="USD",
                initial_capital=1.0,
                trading_weekdays=(0, 7),
            )
        with self.assertRaises(ValueError):
            ReportConfig(
                reporting_currency="USD", initial_capital=1.0, max_plot_points=99
            )
        with self.assertRaises(ValueError):
            ReportConfig(
                reporting_currency="USD", initial_capital=1.0, max_mark_age_ns=-1
            )


class TestCanonicalAnalytics(unittest.TestCase):
    def test_reconciliation_uses_local_delivered_view_without_double_counting(self):
        source = complete_report(equity=[100.0, 101.0], flows=[0.0, 0.0])
        portfolio = source.bundle.portfolio_snapshots.with_columns(
            pl.Series("cash", [100.0, 101.0])
        )
        accounts = pl.concat(
            [
                source.bundle.account_snapshots,
                source.bundle.account_snapshots.with_columns(
                    pl.lit("exchange_final").alias("view_kind")
                ),
            ]
        ).sort("timestamp")
        positions = pl.concat(
            [
                source.bundle.position_snapshots,
                source.bundle.position_snapshots.with_columns(
                    pl.lit("exchange_final").alias("view_kind")
                ),
            ]
        ).sort("timestamp")
        report = BacktestReport.from_portfolio(
            portfolio,
            source.config,
            metadata={"fill_count": 1, "order_count": 1},
            account_snapshots=accounts,
            position_snapshots=positions,
            fill_events=source.bundle.fill_events,
            order_events=source.bundle.order_events,
        )
        self.assertEqual(report.data().validation.status, ReportStatus.VALID)

    def test_round_trip_schema_fx_and_pnl_reconciliation(self):
        source = complete_report(equity=[100.0, 101.0], flows=[0.0, 0.0])
        ts = timestamps(2)
        round_trips = pl.DataFrame(
            {
                "round_trip_id": ["rt-1"],
                "venue_id": ["eu"],
                "instrument_id": ["EUR-ASSET"],
                "entry_timestamp": [ts[0]],
                "exit_timestamp": [ts[1]],
                "side": ["long"],
                "quantity": [2.0],
                "entry_price": [10.0],
                "exit_price": [11.0],
                "gross_pnl": [2.0],
                "fee": [0.2],
                "rebate": [0.05],
                "funding": [-0.1],
                "net_pnl": [1.75],
                "currency": ["EUR"],
            }
        )
        fx_marks = pl.DataFrame(
            {
                "timestamp": ts,
                "currency": ["EUR", "EUR"],
                "reporting_currency": ["USD", "USD"],
                "rate": [2.0, 2.0],
            }
        )
        report = BacktestReport.from_portfolio(
            source.bundle.portfolio_snapshots,
            source.config,
            metadata={"fill_count": 1, "order_count": 1, "round_trip_count": 1},
            account_snapshots=source.bundle.account_snapshots,
            position_snapshots=source.bundle.position_snapshots,
            fill_events=source.bundle.fill_events,
            order_events=source.bundle.order_events,
            round_trip_events=round_trips,
            fx_marks=fx_marks,
        )
        self.assertEqual(report.data().validation.status, ReportStatus.VALID)
        self.assertEqual(report.bundle.round_trip_events["net_pnl_reporting"][0], 3.5)

        broken = round_trips.with_columns(
            pl.lit(99.0).alias("net_pnl"),
            pl.lit(ts[0] - timedelta(days=1)).alias("exit_timestamp"),
        )
        invalid = BacktestReport.from_portfolio(
            source.bundle.portfolio_snapshots,
            source.config,
            metadata={"fill_count": 1, "order_count": 1, "round_trip_count": 1},
            account_snapshots=source.bundle.account_snapshots,
            position_snapshots=source.bundle.position_snapshots,
            fill_events=source.bundle.fill_events,
            order_events=source.bundle.order_events,
            round_trip_events=broken,
            fx_marks=fx_marks,
        )
        codes = {issue.code for issue in invalid.data().validation.issues}
        self.assertIn("round_trip_events.invalid_interval", codes)
        self.assertIn("round_trip_events.pnl_reconciliation", codes)

    def test_risk_breaches_and_stale_marks_are_structured_diagnostics(self):
        source = complete_report(equity=[100.0, 101.0], flows=[0.0, 0.0])
        ts = timestamps(2)
        risk_events = pl.DataFrame(
            {
                "timestamp": ts,
                "event_id": ["risk-1", "risk-2"],
                "limit_id": ["gross", "gross"],
                "scope": ["portfolio", "portfolio"],
                "metric": ["gross_exposure", "gross_exposure"],
                "observed_value": [90.0, 110.0],
                "limit_value": [100.0, 100.0],
                "utilization": [0.9, 1.1],
                "breached": [False, True],
            }
        )
        market_marks = pl.DataFrame(
            {
                "timestamp": ts,
                "exchange_timestamp": ts,
                "venue_id": ["test", "test"],
                "instrument_id": ["TEST", "TEST"],
                "mark_price": [10.0, 10.1],
                "source": ["mid", "mid"],
                "age_ns": [50, 150],
                "stale": [False, True],
            }
        )
        config = replace(source.config, max_mark_age_ns=100)
        report = BacktestReport.from_portfolio(
            source.bundle.portfolio_snapshots,
            config,
            metadata={"fill_count": 1, "order_count": 1},
            account_snapshots=source.bundle.account_snapshots,
            position_snapshots=source.bundle.position_snapshots,
            fill_events=source.bundle.fill_events,
            order_events=source.bundle.order_events,
            risk_events=risk_events,
            market_marks=market_marks,
        )
        validation = report.data().validation
        self.assertEqual(validation.status, ReportStatus.PARTIAL)
        codes = {issue.code for issue in validation.issues}
        self.assertIn("risk_events.limit_breach", codes)
        self.assertIn("market_marks.stale", codes)
        self.assertIn("risk_events", report.bundle.tables())
        self.assertIn("market_marks", report.bundle.tables())
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "risk-report.html"
            artifact = report.generate(output)
            self.assertEqual(artifact.status, ReportStatus.PARTIAL)
            self.assertIn("Risk Diagnostics", output.read_text(encoding="utf-8"))

    def test_multi_venue_fx_conversion_drives_reporting_currency_reconciliation(self):
        ts = timestamps(2)
        portfolio = pl.DataFrame(
            {
                "timestamp": ts,
                "equity_gross": [100.0, 101.0],
                "equity_net": [100.0, 101.0],
                "cash": [100.0, 101.0],
                "fee": [0.0, 0.0],
                "rebate": [0.0, 0.0],
                "funding": [0.0, 0.0],
                "external_flow": [0.0, 0.0],
                "gross_exposure": [20.0, 20.0],
                "net_exposure": [20.0, 20.0],
            }
        )
        accounts = pl.DataFrame(
            {
                "timestamp": [ts[0], ts[0], ts[1], ts[1]],
                "view_kind": ["local_delivered"] * 4,
                "venue_id": ["us", "eu", "us", "eu"],
                "currency_id": ["USD", "EUR", "USD", "EUR"],
                "balance": [60.0, 20.0, 61.0, 20.0],
                "fee": [0.0] * 4,
                "rebate": [0.0] * 4,
                "funding": [0.0] * 4,
                "realized_pnl": [None] * 4,
                "unrealized_pnl": [None] * 4,
                "margin": [None] * 4,
            }
        )
        positions = pl.DataFrame(
            {
                "timestamp": [ts[0], ts[0], ts[1], ts[1]],
                "view_kind": ["local_delivered"] * 4,
                "venue_id": ["us", "eu", "us", "eu"],
                "instrument_id": ["A", "B", "A", "B"],
                "currency_id": ["USD", "EUR", "USD", "EUR"],
                "quantity": [1.0] * 4,
                "mark_price": [10.0, 5.0, 10.0, 5.0],
                "notional": [10.0, 5.0, 10.0, 5.0],
                "realized_pnl": [None] * 4,
                "unrealized_pnl": [None] * 4,
                "margin": [None] * 4,
            }
        )
        fx_marks = pl.DataFrame(
            {
                "timestamp": ts,
                "currency": ["EUR", "EUR"],
                "reporting_currency": ["USD", "USD"],
                "rate": [2.0, 2.0],
            }
        )
        config = ReportConfig(reporting_currency="USD", initial_capital=100.0)
        report = BacktestReport.from_portfolio(
            portfolio,
            config,
            account_snapshots=accounts,
            position_snapshots=positions,
            fx_marks=fx_marks,
        )
        self.assertNotEqual(report.data().validation.status, ReportStatus.INVALID)
        self.assertEqual(
            report.bundle.account_snapshots["balance_reporting"].to_list(),
            [60.0, 40.0, 61.0, 40.0],
        )
        self.assertEqual(
            report.bundle.position_snapshots["notional_reporting"].to_list(),
            [10.0, 10.0, 10.0, 10.0],
        )

    def test_foreign_currency_without_fx_coverage_is_invalid(self):
        source = complete_report(equity=[100.0, 101.0], flows=[0.0, 0.0])
        accounts = source.bundle.account_snapshots.with_columns(
            pl.lit("EUR").alias("currency_id")
        )
        report = BacktestReport.from_portfolio(
            source.bundle.portfolio_snapshots,
            source.config,
            account_snapshots=accounts,
            position_snapshots=source.bundle.position_snapshots,
            fill_events=source.bundle.fill_events,
            order_events=source.bundle.order_events,
            fx_marks=pl.DataFrame({"timestamp": timestamps(1), "rate": [2.0]}),
            metadata={"fill_count": 1, "order_count": 1},
        )
        self.assertEqual(report.data().validation.status, ReportStatus.INVALID)
        codes = {issue.code for issue in report.data().validation.issues}
        self.assertIn("account_snapshots.fx_rate_unavailable", codes)
        self.assertIn("fx_marks.required_fields", codes)

    def test_partial_fills_remain_distinct_from_orders_and_round_trips(self):
        source = complete_report(equity=[100.0, 101.0, 102.0], flows=[0.0] * 3)
        fills = source.bundle.fill_events.with_columns(
            pl.lit(7).alias("order_id"),
            pl.Series("quantity", [0.4, 0.6]),
        )
        orders = source.bundle.order_events.with_columns(
            pl.lit(7).alias("order_id"),
            pl.Series("status", ["partially_filled", "filled"]),
        )
        report = BacktestReport.from_portfolio(
            source.bundle.portfolio_snapshots,
            source.config,
            metadata={"fill_count": 2, "order_count": 1},
            account_snapshots=source.bundle.account_snapshots,
            position_snapshots=source.bundle.position_snapshots,
            fill_events=fills,
            order_events=orders,
        )
        self.assertEqual(report.data().validation.status, ReportStatus.VALID)
        self.assertEqual(report.metrics()["trading.number_of_fills"].value, 2)
        self.assertEqual(report.metrics()["trading.number_of_orders"].value, 1)
        self.assertNotIn("trading.number_of_round_trips", report.metrics())

    def test_epoch_timestamp_is_converted_from_utc_not_relabelled(self):
        frame = ensure_timestamp(
            pl.DataFrame({"timestamp": [0]}), "ns", "Asia/Shanghai"
        )
        value = frame["timestamp"][0]
        self.assertEqual(value.hour, 8)
        self.assertEqual(value.utcoffset(), timedelta(hours=8))

    def test_traditional_calendar_maps_weekend_to_previous_session(self):
        ts = [
            datetime(2025, 1, 3, tzinfo=timezone.utc),  # Friday
            datetime(2025, 1, 4, tzinfo=timezone.utc),  # Saturday
            datetime(2025, 1, 6, tzinfo=timezone.utc),  # Monday
        ]
        portfolio = pl.DataFrame(
            {
                "timestamp": ts,
                "equity_gross": [100.0, 101.0, 103.0],
                "equity_net": [100.0, 101.0, 103.0],
                "fee": [0.0] * 3,
                "rebate": [0.0] * 3,
                "funding": [0.0] * 3,
                "external_flow": [0.0] * 3,
            }
        )
        config = ReportConfig(
            reporting_currency="USD", initial_capital=100.0, calendar="weekday"
        )
        periodic = BacktestReport.from_portfolio(portfolio, config).data().periodic_returns
        self.assertEqual(
            periodic["session"].to_list(),
            [datetime(2025, 1, 3).date(), datetime(2025, 1, 6).date()],
        )

    def test_timezone_and_day_cutoff_define_reporting_session(self):
        ts = [
            datetime(2025, 1, 1, 22, tzinfo=timezone.utc),
            datetime(2025, 1, 1, 23, tzinfo=timezone.utc),
            datetime(2025, 1, 2, 1, tzinfo=timezone.utc),
            datetime(2025, 1, 2, 3, tzinfo=timezone.utc),
        ]
        portfolio = pl.DataFrame(
            {
                "timestamp": ts,
                "equity_gross": [100.0, 101.0, 102.0, 103.0],
                "equity_net": [100.0, 101.0, 102.0, 103.0],
                "external_flow": [0.0] * 4,
                "fee": [0.0] * 4,
                "rebate": [0.0] * 4,
                "funding": [0.0] * 4,
            }
        )
        config = ReportConfig(
            reporting_currency="USD",
            initial_capital=100.0,
            timezone="UTC",
            day_cutoff_hour=2,
        )
        report = BacktestReport.from_portfolio(portfolio, config)
        periodic = report.data().periodic_returns
        self.assertEqual(periodic["session"].to_list(), [datetime(2025, 1, 1).date(), datetime(2025, 1, 2).date()])
        self.assertAlmostEqual(periodic["return"][0], 0.02)
        self.assertAlmostEqual(periodic["return"][1], 103.0 / 102.0 - 1.0)

    def test_external_flow_is_not_counted_as_return(self):
        report = complete_report()
        data = report.data()
        self.assertEqual(data.validation.status, ReportStatus.VALID)
        for actual, expected in zip(data.periodic_returns["return"].to_list(), [0.1, 0.0, 0.1]):
            self.assertAlmostEqual(actual, expected)
        self.assertAlmostEqual(report.metrics()["return.net"].value, 0.21)
        self.assertAlmostEqual(report.metrics()["risk.max_drawdown"].value, 0.0)

    def test_fee_rebate_and_funding_reconcile_and_remain_separate(self):
        source = complete_report(equity=[100.0, 109.0, 111.5], flows=[0.0] * 3)
        portfolio = source.bundle.portfolio_snapshots.with_columns(
            pl.Series("equity_gross", [100.0, 111.0, 111.0]),
            pl.Series("fee", [0.0, 2.0, 3.0]),
            pl.Series("rebate", [0.0, 1.0, 1.5]),
            pl.Series("funding", [0.0, -1.0, 2.0]),
        )
        report = BacktestReport.from_portfolio(
            portfolio,
            source.config,
            account_snapshots=source.bundle.account_snapshots,
            position_snapshots=source.bundle.position_snapshots,
            fill_events=source.bundle.fill_events,
            order_events=source.bundle.order_events,
        )
        metrics = report.metrics()
        self.assertEqual(report.data().validation.status, ReportStatus.VALID)
        self.assertEqual(metrics["cost.fee"].value, 3.0)
        self.assertEqual(metrics["cost.rebate"].value, 1.5)
        self.assertEqual(metrics["pnl.funding"].value, 2.0)
        self.assertEqual(metrics["cost.net"].value, 1.5)

    def test_drawdown_and_tail_metrics(self):
        report = complete_report(
            equity=[100.0, 110.0, 88.0, 96.8, 121.0],
            flows=[0.0] * 5,
        )
        metrics = report.metrics()
        self.assertAlmostEqual(metrics["return.net"].value, 0.21)
        self.assertAlmostEqual(metrics["risk.max_drawdown"].value, -0.2)
        self.assertEqual(metrics["risk.max_drawdown_duration"].value, 2)
        self.assertEqual(metrics["risk.sharpe"].provider, "titan")
        self.assertEqual(metrics["risk.sharpe"].version, 1)

    def test_first_period_loss_is_measured_from_initial_high_water_mark(self):
        report = complete_report(
            equity=[100.0, 90.0, 100.0],
            flows=[0.0] * 3,
        )
        self.assertAlmostEqual(report.metrics()["risk.max_drawdown"].value, -0.1)

    def test_annualized_return_requires_minimum_samples(self):
        report = complete_report(equity=[100.0, 110.0], flows=[0.0, 0.0])
        metric = report.metrics()["return.annualized"]
        self.assertIsNone(metric.value)
        self.assertEqual(metric.parameters["samples"], 1)
        self.assertEqual(metric.parameters["minimum_samples"], 2)

    def test_benchmark_is_aligned_by_reporting_session(self):
        benchmark = pl.DataFrame(
            {
                "timestamp": timestamps(4),
                "equity_or_return": [0.0, 0.05, 0.0, 0.05],
                "value_kind": ["return"] * 4,
                "benchmark_id": ["TEST"] * 4,
                "currency": ["USD"] * 4,
                "timezone": ["UTC"] * 4,
                "source": ["fixture"] * 4,
                "frequency": ["1d"] * 4,
            }
        )
        report = complete_report(benchmark=benchmark)
        metrics = report.metrics()
        self.assertIn("benchmark.return", metrics)
        self.assertAlmostEqual(metrics["benchmark.return"].value, 0.1025)
        self.assertIn("benchmark.information_ratio", metrics)
        self.assertEqual(metrics["benchmark.input_samples"].value, 4)
        self.assertEqual(metrics["benchmark.aligned_samples"].value, 3)
        self.assertIn("benchmark.aligned_start", metrics)

    def test_naive_benchmark_uses_its_declared_timezone(self):
        benchmark = pl.DataFrame(
            {
                "timestamp": [datetime(2025, 1, 1, 20), datetime(2025, 1, 2, 20)],
                "equity_or_return": [0.01, 0.02],
                "value_kind": ["return"] * 2,
                "benchmark_id": ["TEST"] * 2,
                "currency": ["USD"] * 2,
                "timezone": ["America/New_York"] * 2,
                "source": ["fixture"] * 2,
                "frequency": ["1d"] * 2,
            }
        )
        report = complete_report(benchmark=benchmark)
        converted = report.bundle.benchmark["timestamp"][0]
        self.assertEqual(converted.hour, 1)
        self.assertEqual(str(report.bundle.benchmark.schema["timestamp"].time_zone), "UTC")


class TestLegacyAdapter(unittest.TestCase):
    def test_engine_result_execution_reports_feed_canonical_fact_tables(self):
        source = complete_report(equity=[100.0, 101.0, 102.0], flows=[0.0] * 3)
        reports = [
            {
                "kind": "accepted",
                "reason": "none",
                "venue_id": "test",
                "instrument_id": "TEST",
                "order_id": 42,
                "exchange_ts": timestamps(3)[1],
                "delivery_ts": timestamps(3)[1],
                "sequence": 0,
                "status": "new",
                "side": "buy",
                "order_price": 10.0,
                "order_qty": 1.0,
                "exec_price": 0.0,
                "exec_qty": 0.0,
                "maker": False,
            },
            {
                "kind": "fill",
                "reason": "none",
                "venue_id": "test",
                "instrument_id": "TEST",
                "order_id": 42,
                "exchange_ts": timestamps(3)[1],
                "delivery_ts": timestamps(3)[1],
                "sequence": 1,
                "status": "partially_filled",
                "side": "buy",
                "order_price": 10.0,
                "order_qty": 1.0,
                "exec_price": 10.0,
                "exec_qty": 0.4,
                "maker": True,
                "account_delta": {"fee": 0.01, "trade_value": 4.0, "currency": 7},
            },
            {
                "kind": "fill",
                "reason": "none",
                "venue_id": "test",
                "instrument_id": "TEST",
                "order_id": 42,
                "exchange_ts": timestamps(3)[2],
                "delivery_ts": timestamps(3)[2],
                "sequence": 2,
                "status": "filled",
                "side": "buy",
                "order_price": 10.0,
                "order_qty": 1.0,
                "exec_price": 10.0,
                "exec_qty": 0.6,
                "maker": True,
                "account_delta": {"fee": -0.005, "trade_value": 6.0, "currency": 7},
            },
        ]
        result = {
            "run_id": 9,
            "order_count": 1,
            "fill_count": 2,
            "execution_reports": reports,
            "metadata": {"engine_version": "test"},
        }
        report = BacktestReport.from_result(
            result,
            source.config,
            portfolio_snapshots=source.bundle.portfolio_snapshots,
            account_snapshots=source.bundle.account_snapshots,
            position_snapshots=source.bundle.position_snapshots,
            currency_map={7: "USD"},
        )
        self.assertEqual(report.data().validation.status, ReportStatus.VALID)
        self.assertEqual(len(report.bundle.fill_events), 2)
        self.assertEqual(len(report.bundle.order_events), 3)
        self.assertEqual(report.bundle.fill_events["order_id"].unique().to_list(), [42])
        self.assertEqual(report.bundle.fill_events["fee"].to_list(), [0.01, 0.0])
        self.assertEqual(report.bundle.fill_events["rebate"].to_list(), [0.0, 0.005])
        self.assertEqual(report.bundle.fill_events["currency"].unique().to_list(), ["USD"])

    def test_execution_adapter_accepts_terminal_u64_order_id(self):
        source = complete_report(equity=[100.0, 101.0], flows=[0.0, 0.0])
        result = {
            "order_count": 1,
            "fill_count": 1,
            "execution_reports": [
                {
                    "kind": "fill",
                    "sequence": 1,
                    "venue_id": 1,
                    "instrument_id": 2,
                    "order_id": (1 << 64) - 1,
                    "delivery_ts": 1_000,
                    "exchange_ts": 900,
                    "exec_price": 10.0,
                    "exec_qty": 1.0,
                    "order_price": 10.0,
                    "order_qty": 1.0,
                    "side": "sell",
                    "status": "filled",
                    "account_delta": {
                        "currency": 7,
                        "trade_value": 10.0,
                        "fee": 0.0,
                    },
                }
            ],
        }
        report = BacktestReport.from_result(
            result,
            source.config,
            portfolio_snapshots=source.bundle.portfolio_snapshots,
            account_snapshots=source.bundle.account_snapshots,
            position_snapshots=source.bundle.position_snapshots,
            currency_map={7: "USD"},
        )
        self.assertEqual(report.bundle.fill_events.schema["order_id"], pl.UInt64)
        self.assertEqual(report.bundle.fill_events["order_id"].item(), (1 << 64) - 1)

    def test_result_without_execution_capability_does_not_report_zero_facts(self):
        source = complete_report(equity=[100.0, 101.0], flows=[0.0, 0.0])
        report = BacktestReport.from_result(
            {"metadata": {"engine_version": "legacy"}},
            source.config,
            portfolio_snapshots=source.bundle.portfolio_snapshots,
            account_snapshots=source.bundle.account_snapshots,
            position_snapshots=source.bundle.position_snapshots,
        )
        self.assertIsNone(report.bundle.fill_events)
        self.assertIsNone(report.bundle.order_events)
        self.assertNotIn("trading.number_of_fills", report.metrics())
        codes = {issue.code for issue in report.data().validation.issues}
        self.assertIn("fill_events.unavailable", codes)
        self.assertIn("order_events.unavailable", codes)

    def test_legacy_signed_fee_is_split_without_changing_net_equity(self):
        record = np.zeros(4, dtype=record_dtype)
        record["timestamp"] = np.arange(4, dtype=np.int64) * 86_400_000_000_000
        record["price"] = 10.0
        record["balance"] = 0.0
        record["fee"] = [0.0, 1.0, 0.5, 2.0]
        config = ReportConfig(reporting_currency="USD", initial_capital=100.0)
        report = BacktestReport.from_record(record, config)
        portfolio = report.bundle.portfolio_snapshots
        self.assertEqual(portfolio["fee"].to_list(), [0.0, 1.0, 1.0, 2.5])
        self.assertEqual(portfolio["rebate"].to_list(), [0.0, 0.0, 0.5, 0.5])
        self.assertEqual(portfolio["equity_net"].to_list(), [100.0, 99.0, 99.5, 98.0])
        self.assertEqual(report.data().validation.status, ReportStatus.PARTIAL)

    def test_existing_record_api_builds_report_without_mutating_stats_api(self):
        record = np.zeros(3, dtype=record_dtype)
        record["timestamp"] = np.arange(3, dtype=np.int64) * 86_400_000_000_000
        record["price"] = 10.0
        config = ReportConfig(reporting_currency="USD", initial_capital=100.0)
        legacy = LinearAssetRecord(record).contract_size(2.0)
        report = legacy.report(config)
        self.assertIsInstance(report, BacktestReport)
        self.assertEqual(report.config.contract_size, 2.0)
        self.assertEqual(report.config.asset_type, "linear")


class TestValidation(unittest.TestCase):
    def test_duplicate_fill_id_and_engine_counter_mismatch_are_invalid(self):
        source = complete_report()
        duplicate_fills = source.bundle.fill_events.with_columns(
            pl.lit("duplicate").alias("fill_id")
        )
        report = BacktestReport.from_portfolio(
            source.bundle.portfolio_snapshots,
            source.config,
            metadata={"fill_count": len(duplicate_fills) + 1},
            account_snapshots=source.bundle.account_snapshots,
            position_snapshots=source.bundle.position_snapshots,
            fill_events=duplicate_fills,
            order_events=source.bundle.order_events,
        )
        codes = {issue.code for issue in report.data().validation.issues}
        self.assertEqual(report.data().validation.status, ReportStatus.INVALID)
        self.assertIn("fill_events.duplicate_fill_id", codes)
        self.assertIn("fill_events.counter_mismatch", codes)

    def test_missing_accounting_fields_are_not_silently_zero_filled(self):
        portfolio = pl.DataFrame(
            {
                "timestamp": timestamps(2),
                "equity_gross": [100.0, 101.0],
                "equity_net": [100.0, 101.0],
            }
        )
        report = BacktestReport.from_portfolio(
            portfolio,
            ReportConfig(reporting_currency="USD", initial_capital=100.0),
        )
        self.assertEqual(report.data().validation.status, ReportStatus.INVALID)
        self.assertIn(
            "portfolio.required_value_null",
            {issue.code for issue in report.data().validation.issues},
        )

    def test_account_cash_is_reconciled_when_portfolio_cash_is_available(self):
        source = complete_report()
        portfolio = source.bundle.portfolio_snapshots.with_columns(
            pl.col("equity_net").alias("cash")
        )
        broken_accounts = source.bundle.account_snapshots.with_columns(
            (pl.col("balance") - 1.0).alias("balance")
        )
        report = BacktestReport.from_portfolio(
            portfolio,
            source.config,
            account_snapshots=broken_accounts,
            position_snapshots=source.bundle.position_snapshots,
        )
        self.assertEqual(report.data().validation.status, ReportStatus.INVALID)
        self.assertIn(
            "account_snapshots.cash_reconciliation",
            {issue.code for issue in report.data().validation.issues},
        )

    def test_snapshot_tables_must_satisfy_their_schema(self):
        source = complete_report()
        malformed = pl.DataFrame(
            {"timestamp": [timestamps(1)[0]], "junk": [1.0]}
        )
        report = BacktestReport.from_portfolio(
            source.bundle.portfolio_snapshots,
            source.config,
            account_snapshots=malformed,
            position_snapshots=malformed,
        )
        result = report.data().validation
        self.assertEqual(result.status, ReportStatus.INVALID)
        codes = {issue.code for issue in result.issues}
        self.assertIn("account_snapshots.required_fields", codes)
        self.assertIn("position_snapshots.required_fields", codes)

    def test_position_exposure_is_reconciled(self):
        source = complete_report()
        broken_positions = source.bundle.position_snapshots.with_columns(
            pl.lit(20.0).alias("notional")
        )
        report = BacktestReport.from_portfolio(
            source.bundle.portfolio_snapshots,
            source.config,
            account_snapshots=source.bundle.account_snapshots,
            position_snapshots=broken_positions,
        )
        self.assertEqual(report.data().validation.status, ReportStatus.INVALID)
        self.assertIn(
            "position_snapshots.exposure_reconciliation",
            {issue.code for issue in report.data().validation.issues},
        )

    def test_out_of_order_and_duplicate_timestamps_are_reported(self):
        source = complete_report()
        rows = source.bundle.portfolio_snapshots[[0, 2, 1, 1]]
        report = BacktestReport.from_portfolio(rows, source.config)
        result = report.data().validation
        self.assertEqual(result.status, ReportStatus.INVALID)
        codes = {issue.code for issue in result.issues}
        self.assertIn("data.timestamp_not_monotonic", codes)
        self.assertIn("data.timestamp_duplicate", codes)

    def test_incomplete_benchmark_coverage_is_partial(self):
        benchmark = pl.DataFrame(
            {
                "timestamp": timestamps(2),
                "equity_or_return": [0.0, 0.05],
                "value_kind": ["return"] * 2,
                "benchmark_id": ["TEST"] * 2,
                "currency": ["USD"] * 2,
                "timezone": ["UTC"] * 2,
                "source": ["fixture"] * 2,
                "frequency": ["1d"] * 2,
            }
        )
        result = complete_report(benchmark=benchmark).data().validation
        self.assertEqual(result.status, ReportStatus.PARTIAL)
        self.assertIn(
            "benchmark.incomplete_coverage", {issue.code for issue in result.issues}
        )

    def test_reconciliation_failure_is_invalid(self):
        report = complete_report()
        broken = report.bundle.portfolio_snapshots.with_columns(
            (pl.col("equity_net") - 1.0).alias("equity_net")
        )
        invalid = BacktestReport.from_portfolio(
            broken,
            report.config,
            account_snapshots=report.bundle.account_snapshots,
            position_snapshots=report.bundle.position_snapshots,
        )
        self.assertEqual(invalid.data().validation.status, ReportStatus.INVALID)
        self.assertIn(
            "portfolio.reconciliation",
            {issue.code for issue in invalid.data().validation.issues},
        )

    def test_missing_optional_tables_is_partial_not_invalid(self):
        source = complete_report()
        report = BacktestReport.from_portfolio(source.bundle.portfolio_snapshots, source.config)
        self.assertEqual(report.data().validation.status, ReportStatus.PARTIAL)
        codes = {issue.code for issue in report.data().validation.issues}
        self.assertIn("account_snapshots.unavailable", codes)
        self.assertIn("position_snapshots.unavailable", codes)
        self.assertEqual(
            report.data().sections["returns_risk"].status,
            SectionAvailability.AVAILABLE,
        )


class TestOutputs(unittest.TestCase):
    def test_extrema_downsampling_is_bounded_and_preserves_spikes(self):
        values = np.zeros(10_000)
        values[1_234] = -100.0
        values[8_765] = 200.0
        frame = pl.DataFrame({"timestamp": range(len(values)), "value": values})
        indices = _extrema_indices(frame, ("value",), 200)
        self.assertLessEqual(len(indices), 200)
        self.assertEqual(indices[0], 0)
        self.assertEqual(indices[-1], len(values) - 1)
        self.assertIn(1_234, indices)
        self.assertIn(8_765, indices)

    def test_notebook_repr_is_self_contained_summary(self):
        document = complete_report()._repr_html_()
        self.assertIn("Titan/HFTBacktest Report", document)
        self.assertIn("return.net", document)
        self.assertNotIn("<script", document)

    def test_native_fatal_failure_emits_minimal_failed_report(self):
        report = complete_report()
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "failed.html"
            with patch(
                "hftbacktest.reporting.renderers.NativeRenderer.render",
                side_effect=RuntimeError("fatal render error"),
            ):
                artifact = report.generate(output)
            self.assertEqual(artifact.status, ReportStatus.FAILED)
            self.assertIn("renderer.failed", {issue.code for issue in artifact.issues})
            self.assertIn('<p class="status">failed</p>', output.read_text(encoding="utf-8"))

    def test_quantstats_adapter_consumes_only_canonical_returns(self):
        report = complete_report()
        prepared = QuantStatsAdapter().prepare(report.data(), report.config)
        self.assertEqual(prepared.provider, "quantstats")
        self.assertEqual(
            prepared.payload["returns"].to_list(),
            report.data().periodic_returns["return"].to_list(),
        )
        self.assertIsNone(prepared.payload["benchmark"])

    def test_quantstats_renderer_contract_golden(self):
        report = complete_report()
        captured = {}

        def render_html(returns, **kwargs):
            captured["returns"] = returns.to_list()
            captured.update(kwargs)
            Path(kwargs["output"]).write_text(
                "<html><body>quantstats-golden</body></html>", encoding="utf-8"
            )

        fake_quantstats = SimpleNamespace(
            reports=SimpleNamespace(html=render_html)
        )
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "quantstats.html"
            with patch.dict(sys.modules, {"quantstats": fake_quantstats}):
                artifact = report.generate(
                    output,
                    renderer="quantstats",
                    include_native_sections=False,
                )
            self.assertEqual(output.read_text(encoding="utf-8"), "<html><body>quantstats-golden</body></html>")
            self.assertEqual(captured["returns"], report.data().periodic_returns["return"].to_list())
            self.assertEqual(captured["rf"], report.config.risk_free_rate)
            self.assertEqual(captured["periods_per_year"], report.config.annualization)
            self.assertEqual(artifact.provider, "quantstats")

    def test_installed_quantstats_generates_standalone_html(self):
        try:
            import quantstats  # noqa: F401
        except ImportError:
            self.skipTest("optional reports dependency is not installed")
        report = complete_report()
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "quantstats.html"
            artifact = report.generate(
                output,
                renderer="quantstats",
                include_native_sections=False,
            )
            document = output.read_text(encoding="utf-8")
            self.assertEqual(artifact.provider, "quantstats")
            self.assertGreater(len(document), 100_000)
            self.assertIn("generated by quantstats for python", document.casefold())
            self.assertIn("Backtest Report", document)
            self.assertIn("<html", document.casefold())

    def test_renderer_section_failure_is_propagated_to_artifact(self):
        report = complete_report()
        with tempfile.TemporaryDirectory() as directory:
            with patch(
                "hftbacktest.reporting.renderers._plot_drawdown",
                side_effect=RuntimeError("plot failed"),
            ):
                artifact = report.generate(Path(directory) / "report.html")
            self.assertEqual(artifact.status, ReportStatus.PARTIAL)
            self.assertIn(
                "renderer.section_failed", {issue.code for issue in artifact.issues}
            )
            document = artifact.path.read_text(encoding="utf-8")
            self.assertIn("drawdown_tail</td><td>failed", document)
            self.assertIn('<p class="status">partial</p>', document)

    def test_invalid_bundle_still_generates_diagnostic_html(self):
        report = complete_report()
        broken = report.bundle.portfolio_snapshots.with_columns(
            (pl.col("equity_net") - 1.0).alias("equity_net")
        )
        invalid = BacktestReport.from_portfolio(broken, report.config)
        with tempfile.TemporaryDirectory() as directory:
            artifact = invalid.generate(Path(directory) / "invalid.html")
            self.assertEqual(artifact.status, ReportStatus.INVALID)
            document = artifact.path.read_text(encoding="utf-8")
            self.assertIn("portfolio.reconciliation", document)
            self.assertIn('<p class="status">invalid</p>', document)

    def test_native_html_and_structured_exports(self):
        report = complete_report(
            metadata={
                "strategy_parameters": {
                    "api_key": "secret",
                    "apiSecret": "also-secret",
                    "access_token_value": "token-secret",
                }
            }
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifact = report.generate(root / "report.html")
            self.assertEqual(artifact.status, ReportStatus.VALID)
            document = artifact.path.read_text(encoding="utf-8")
            self.assertIn("Titan/HFTBacktest Report", document)
            self.assertIn("Underwater Drawdown", document)
            self.assertIn("data:image/png;base64", document)

            parquet = report.export(root / "parquet-bundle", format="parquet")
            csv = report.export(root / "csv-bundle", format="csv")
            self.assertTrue((parquet / "portfolio_snapshots.parquet").exists())
            self.assertTrue((parquet / "canonical_periodic_returns.parquet").exists())
            self.assertTrue((csv / "portfolio_snapshots.csv").exists())
            self.assertTrue((csv / "canonical_drawdowns.csv").exists())
            manifest = json.loads((parquet / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(
                manifest["metadata"]["strategy_parameters"]["api_key"], "[REDACTED]"
            )
            self.assertEqual(
                manifest["metadata"]["strategy_parameters"]["apiSecret"], "[REDACTED]"
            )
            self.assertEqual(
                manifest["metadata"]["strategy_parameters"]["access_token_value"],
                "[REDACTED]",
            )
            self.assertIn("sha256", manifest["tables"]["portfolio_snapshots"])
            self.assertEqual(
                manifest["tables"]["canonical_periodic_returns"]["kind"],
                "canonical_derived",
            )

    def test_failed_export_restores_previous_directory(self):
        report = complete_report()
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "bundle"
            report.export(destination, format="csv")
            original = (destination / "manifest.json").read_bytes()
            real_replace = os.replace

            def fail_new_install(source, target):
                source_path = Path(source)
                target_path = Path(target)
                if (
                    target_path.name == destination.name
                    and ".backup." not in source_path.name
                ):
                    raise OSError("simulated install failure")
                return real_replace(source, target)

            with patch(
                "hftbacktest.reporting.export.os.replace",
                side_effect=fail_new_install,
            ):
                with self.assertRaises(OSError):
                    report.export(destination, format="csv")
            self.assertEqual((destination / "manifest.json").read_bytes(), original)

    def test_quantstats_unavailable_falls_back_explicitly(self):
        report = complete_report()
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "report.html"
            with patch.dict(sys.modules, {"quantstats": None}):
                artifact = report.generate(output, renderer="quantstats")
            self.assertEqual(artifact.provider, "native")
            self.assertEqual(artifact.status, ReportStatus.PARTIAL)
            self.assertIn("backend.unavailable", {issue.code for issue in artifact.issues})

    def test_strict_backend_does_not_hide_missing_dependency(self):
        source = complete_report()
        strict = BacktestReport.from_bundle(
            source.bundle,
            replace(source.config, strict_backend=True),
        )
        with tempfile.TemporaryDirectory() as directory:
            with patch.dict(sys.modules, {"quantstats": None}):
                with self.assertRaises(BackendUnavailableError):
                    strict.generate(Path(directory) / "report.html", renderer="quantstats")


if __name__ == "__main__":
    unittest.main()
