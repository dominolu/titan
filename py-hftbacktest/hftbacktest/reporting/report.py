from __future__ import annotations

from dataclasses import replace
import html
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import polars as pl

from .analytics import (
    build_calendar_returns,
    build_drawdowns,
    build_periodic_returns,
    build_return_profiles,
    compute_metrics,
)
from .backends import (
    BackendRegistry,
    BackendUnavailableError,
    NativeAdapter,
    QuantStatsAdapter,
    ReportAdapter,
    ReportRenderer,
)
from .bundle import (
    bundle_from_legacy_record,
    bundle_from_portfolio,
    execution_reports_to_tables,
)
from .calendar import ReportingCalendar
from .export import export_report_bundle
from .models import (
    IssueSeverity,
    MetricValue,
    ReportArtifact,
    ReportBundle,
    ReportCapability,
    ReportConfig,
    ReportData,
    ReportStatus,
    RenderOutcome,
    RunMetadata,
    SectionAvailability,
    SectionState,
    ValidationIssue,
    ValidationResult,
    status_from_issues,
)
from .renderers import NativeRenderer, QuantStatsRenderer, atomic_write_text
from .validation import validate_bundle


def default_registry() -> BackendRegistry:
    registry = BackendRegistry()
    registry.register("native", NativeAdapter(), NativeRenderer())
    registry.register("quantstats", QuantStatsAdapter(), QuantStatsRenderer())
    return registry


class BacktestReport:
    def __init__(
        self,
        bundle: ReportBundle,
        config: ReportConfig,
        *,
        registry: BackendRegistry | None = None,
    ) -> None:
        self.bundle = bundle
        self.config = config
        self.registry = registry if registry is not None else default_registry()
        self._data_cache: ReportData | None = None

    @classmethod
    def from_bundle(
        cls,
        bundle: ReportBundle,
        config: ReportConfig,
        *,
        registry: BackendRegistry | None = None,
    ) -> "BacktestReport":
        return cls(bundle, config, registry=registry)

    @classmethod
    def from_record(
        cls,
        record: np.ndarray[Any, Any] | pl.DataFrame,
        config: ReportConfig,
        *,
        metadata: RunMetadata | Mapping[str, Any] | None = None,
        registry: BackendRegistry | None = None,
    ) -> "BacktestReport":
        bundle = bundle_from_legacy_record(record, config, metadata=metadata)
        return cls(bundle, config, registry=registry)

    @classmethod
    def from_portfolio(
        cls,
        portfolio_snapshots: pl.DataFrame | Mapping[str, Any],
        config: ReportConfig,
        *,
        metadata: RunMetadata | Mapping[str, Any] | None = None,
        account_snapshots: pl.DataFrame | None = None,
        position_snapshots: pl.DataFrame | None = None,
        benchmark: pl.DataFrame | None = None,
        fill_events: pl.DataFrame | None = None,
        order_events: pl.DataFrame | None = None,
        round_trip_events: pl.DataFrame | None = None,
        fx_marks: pl.DataFrame | None = None,
        risk_events: pl.DataFrame | None = None,
        market_marks: pl.DataFrame | None = None,
        registry: BackendRegistry | None = None,
    ) -> "BacktestReport":
        bundle = bundle_from_portfolio(
            portfolio_snapshots,
            config,
            metadata=metadata,
            account_snapshots=account_snapshots,
            position_snapshots=position_snapshots,
            benchmark=benchmark,
            fill_events=fill_events,
            order_events=order_events,
            round_trip_events=round_trip_events,
            fx_marks=fx_marks,
            risk_events=risk_events,
            market_marks=market_marks,
        )
        return cls(bundle, config, registry=registry)

    @classmethod
    def from_result(
        cls,
        result: Any,
        config: ReportConfig,
        *,
        portfolio_snapshots: pl.DataFrame | Mapping[str, Any] | None = None,
        account_snapshots: pl.DataFrame | None = None,
        position_snapshots: pl.DataFrame | None = None,
        benchmark: pl.DataFrame | None = None,
        round_trip_events: pl.DataFrame | None = None,
        fx_marks: pl.DataFrame | None = None,
        risk_events: pl.DataFrame | None = None,
        market_marks: pl.DataFrame | None = None,
        currency_map: Mapping[Any, str] | None = None,
        registry: BackendRegistry | None = None,
    ) -> "BacktestReport":
        if isinstance(result, ReportBundle):
            return cls.from_bundle(result, config, registry=registry)
        embedded_bundle = (
            result.get("report_bundle")
            if isinstance(result, Mapping)
            else getattr(result, "report_bundle", None)
        )
        if isinstance(embedded_bundle, ReportBundle):
            return cls.from_bundle(embedded_bundle, config, registry=registry)
        if portfolio_snapshots is None:
            raise ValueError(
                "portfolio_snapshots is required until the engine result carries a full ReportBundle"
            )

        def result_value(name: str, default: Any = None) -> Any:
            if isinstance(result, Mapping):
                return result.get(name, default)
            return getattr(result, name, default)

        metadata_source = result_value("metadata", {})
        if not isinstance(metadata_source, Mapping):
            metadata_source = {
                name: getattr(metadata_source, name)
                for name in RunMetadata.__dataclass_fields__
                if hasattr(metadata_source, name)
            }
        metadata = dict(metadata_source)
        for name in (
            "run_id",
            "order_count",
            "fill_count",
            "round_trip_count",
            "reject_count",
            "cancel_count",
            "expire_count",
            "market_event_count",
            "wall_time_ns",
            "cpu_time_ns",
            "start_exchange_ts",
            "end_exchange_ts",
            "start_delivery_ts",
            "end_delivery_ts",
            "warnings",
            "capability_downgrades",
        ):
            value = result_value(name)
            if value is not None:
                metadata[name] = value
        for name in ("termination", "end_policy"):
            value = result_value(name)
            if value is not None:
                metadata[name] = getattr(value, "name", str(value)).casefold()
        missing_reports = object()
        reports = result_value("execution_reports", missing_reports)
        if reports is missing_reports:
            fills = orders = None
        else:
            fills, orders = execution_reports_to_tables(
                reports,
                config,
                currency_map=currency_map,
            )
        return cls.from_portfolio(
            portfolio_snapshots,
            config,
            metadata=metadata,
            account_snapshots=account_snapshots,
            position_snapshots=position_snapshots,
            benchmark=benchmark,
            fill_events=fills,
            order_events=orders,
            round_trip_events=round_trip_events,
            fx_marks=fx_marks,
            risk_events=risk_events,
            market_marks=market_marks,
            registry=registry,
        )

    def register_backend(
        self,
        name: str,
        adapter: ReportAdapter,
        renderer: ReportRenderer,
        *,
        replace_existing: bool = False,
    ) -> None:
        self.registry.register(name, adapter, renderer, replace=replace_existing)

    def available_backends(self) -> tuple[str, ...]:
        return self.registry.names()

    def _build_data(self) -> ReportData:
        validation = validate_bundle(self.bundle, self.config)
        required = {
            "timestamp",
            "equity_net",
            "equity_gross",
            "external_flow",
            "fee",
            "rebate",
            "funding",
        }
        if not required.issubset(self.bundle.portfolio_snapshots.columns):
            empty_returns = pl.DataFrame(
                schema={"session": pl.Date, "return": pl.Float64}
            )
            return ReportData(
                self.bundle,
                validation,
                empty_returns,
                build_drawdowns(empty_returns),
                pl.DataFrame(schema={"period": pl.String, "return": pl.Float64}),
                pl.DataFrame(schema={"period": pl.String, "return": pl.Float64}),
                pl.DataFrame(schema={"bucket": pl.Int8, "return": pl.Float64}),
                pl.DataFrame(schema={"bucket": pl.Int8, "return": pl.Float64}),
                {},
                self._section_states(validation, metrics_available=False),
            )
        try:
            calendar = ReportingCalendar.from_config(self.config)
            periodic = build_periodic_returns(self.bundle, self.config, calendar)
            if self.bundle.benchmark is not None and "benchmark_return" in periodic.columns:
                missing_benchmark = periodic["benchmark_return"].null_count()
                if missing_benchmark:
                    issue = ValidationIssue(
                        "benchmark.incomplete_coverage",
                        f"benchmark is missing {missing_benchmark} reporting sessions",
                        IssueSeverity.WARNING,
                        table="benchmark",
                        count=missing_benchmark,
                    )
                    issues = validation.issues + (issue,)
                    if validation.status != ReportStatus.INVALID:
                        validation = ValidationResult(status_from_issues(issues), issues)
            drawdowns = build_drawdowns(periodic)
            monthly, annual = build_calendar_returns(periodic)
            weekday, hourly = build_return_profiles(self.bundle, calendar, periodic)
            metrics = compute_metrics(self.bundle, self.config, periodic, drawdowns)
        except Exception as exc:
            issue = ValidationIssue(
                "analytics.failed",
                f"canonical analytics failed: {exc}",
                IssueSeverity.ERROR,
            )
            issues = validation.issues + (issue,)
            validation = ValidationResult(status_from_issues(issues), issues)
            periodic = pl.DataFrame(schema={"session": pl.Date, "return": pl.Float64})
            drawdowns = build_drawdowns(periodic)
            monthly, annual = build_calendar_returns(periodic)
            weekday = pl.DataFrame(schema={"bucket": pl.Int8, "return": pl.Float64})
            hourly = pl.DataFrame(schema={"bucket": pl.Int8, "return": pl.Float64})
            metrics = {}
        return ReportData(
            self.bundle,
            validation,
            periodic,
            drawdowns,
            monthly,
            annual,
            weekday,
            hourly,
            metrics,
            self._section_states(validation, metrics_available=bool(metrics)),
        )

    def _section_states(
        self,
        validation: ValidationResult,
        *,
        metrics_available: bool,
    ) -> Mapping[str, SectionState]:
        issue_codes = {item.code for item in validation.issues}
        analytics_failed = "analytics.failed" in issue_codes or not metrics_available
        portfolio_errors = any(
            item.severity == IssueSeverity.ERROR and item.table == "portfolio_snapshots"
            for item in validation.issues
        )
        analytical_status = (
            SectionAvailability.FAILED if analytics_failed
            else SectionAvailability.PARTIAL if portfolio_errors
            else SectionAvailability.AVAILABLE
        )
        portfolio = self.bundle.portfolio_snapshots

        exposure_fields = ("gross_exposure", "net_exposure", "leverage")
        exposure_available = any(
            field in portfolio.columns and portfolio[field].null_count() < len(portfolio)
            for field in exposure_fields
        )
        trading_fields = ("num_trades", "trading_volume", "trading_value")
        cumulative_trading_available = any(
            field in portfolio.columns and portfolio[field].null_count() < len(portfolio)
            for field in trading_fields
        )
        fills_available = self.bundle.fill_events is not None
        orders_available = self.bundle.order_events is not None
        round_trips_available = self.bundle.round_trip_events is not None
        position_missing = self.bundle.position_snapshots is None
        position_errors = any(
            item.severity == IssueSeverity.ERROR and item.table == "position_snapshots"
            for item in validation.issues
        )
        metadata_degraded = any(
            item.table == "run_metadata" and item.severity != IssueSeverity.INFO
            for item in validation.issues
        )
        return {
            "executive_summary": SectionState(
                "executive_summary",
                SectionAvailability.PARTIAL if metadata_degraded else SectionAvailability.AVAILABLE,
                "run metadata contains warnings or errors" if metadata_degraded else None,
                source_fields=("run_metadata", "canonical_metrics"),
            ),
            "returns_risk": SectionState(
                "returns_risk",
                analytical_status,
                "canonical analytics unavailable" if analytics_failed else None,
                ("portfolio_snapshots.equity_net", "portfolio_snapshots.external_flow"),
            ),
            "drawdown_tail": SectionState(
                "drawdown_tail",
                analytical_status,
                "canonical analytics unavailable" if analytics_failed else None,
                ("canonical_periodic_returns",),
            ),
            "exposure_attribution": SectionState(
                "exposure_attribution",
                SectionAvailability.FAILED
                if position_errors
                else SectionAvailability.PARTIAL
                if exposure_available and position_missing
                else analytical_status
                if exposure_available
                else SectionAvailability.UNAVAILABLE,
                "position snapshots failed validation"
                if position_errors
                else "portfolio exposure is available but position attribution is unavailable"
                if exposure_available and position_missing
                else None
                if exposure_available
                else "exposure fields are unavailable",
                tuple(f"portfolio_snapshots.{item}" for item in exposure_fields),
            ),
            "trading_costs": SectionState(
                "trading_costs",
                analytical_status
                if fills_available and orders_available and round_trips_available
                else SectionAvailability.PARTIAL,
                "round-trip facts are unavailable"
                if fills_available and orders_available and not round_trips_available
                else "only cumulative trade counters are available; fill/order/round-trip facts are unavailable"
                if cumulative_trading_available
                else "fill/trading counters are unavailable; costs only",
                tuple(f"portfolio_snapshots.{item}" for item in trading_fields)
                + ("portfolio_snapshots.fee", "portfolio_snapshots.rebate", "portfolio_snapshots.funding"),
            ),
            "diagnostics": SectionState(
                "diagnostics",
                SectionAvailability.AVAILABLE,
                source_fields=(
                    "validation_issues",
                    "run_metadata",
                    "risk_events",
                    "market_marks",
                ),
            ),
        }

    def data(self, *, refresh: bool = False) -> ReportData:
        if refresh or self._data_cache is None:
            self._data_cache = self._build_data()
        return self._data_cache

    def metrics(self, *, refresh: bool = False) -> Mapping[str, MetricValue]:
        return dict(self.data(refresh=refresh).metrics)

    def _repr_html_(self) -> str:
        data = self.data()
        metric_ids = (
            "return.net",
            "return.annualized",
            "risk.sharpe",
            "risk.max_drawdown",
            "risk.volatility.annualized",
            "cost.net",
        )
        rows = []
        for metric_id in metric_ids:
            metric = data.metrics.get(metric_id)
            if metric is None:
                continue
            value = "N/A" if metric.value is None else str(metric.value)
            rows.append(
                f"<tr><td>{html.escape(metric_id)}</td>"
                f"<td>{html.escape(value)}</td><td>{html.escape(metric.unit)}</td></tr>"
            )
        issues = sum(
            1 for issue in data.validation.issues if issue.severity != IssueSeverity.INFO
        )
        return (
            '<div class="hftbacktest-report-summary">'
            f"<h3>Titan/HFTBacktest Report — {html.escape(data.validation.status.value)}</h3>"
            f"<p>{html.escape(self.config.reporting_currency)} · "
            f"{html.escape(self.config.calendar)} · {issues} diagnostic issue(s)</p>"
            "<table><thead><tr><th>Metric</th><th>Value</th><th>Unit</th></tr></thead>"
            f"<tbody>{''.join(rows)}</tbody></table></div>"
        )

    @staticmethod
    def _with_issue(data: ReportData, issue: ValidationIssue) -> ReportData:
        issues = data.validation.issues + (issue,)
        status = data.validation.status
        if status != ReportStatus.INVALID:
            status = status_from_issues(issues)
        return replace(data, validation=ValidationResult(status, issues))

    @staticmethod
    def _append_link(path: Path, label: str, href: str) -> None:
        document = path.read_text(encoding="utf-8")
        link = f'<section><h2>Third-party Appendix</h2><p><a href="{href}">{label}</a></p></section>'
        atomic_write_text(path, document.replace("</main>", f"{link}</main>"))

    @staticmethod
    def _write_failed_report(path: Path, issue: ValidationIssue) -> None:
        document = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>Titan Backtest Report Failed</title></head>
<body><main><h1>Titan/HFTBacktest Report</h1><p class="status">failed</p>
<section><h2>Diagnostics</h2><p><strong>{html.escape(issue.code)}</strong>: {html.escape(issue.message)}</p></section>
</main></body></html>"""
        atomic_write_text(path, document)

    @staticmethod
    def _render_and_collect(
        renderer: ReportRenderer,
        prepared: Any,
        destination: Path,
        config: ReportConfig,
    ) -> tuple[ValidationIssue, ...]:
        outcome = renderer.render(prepared, destination, config)
        issues = tuple(prepared.issues)
        if isinstance(outcome, RenderOutcome):
            issues += outcome.issues
        return issues

    def generate(
        self,
        output: str | Path,
        *,
        renderer: str = "native",
        include_native_sections: bool = True,
    ) -> ReportArtifact:
        destination = Path(output).expanduser().resolve()
        data = self.data()
        requested = renderer
        definition = self.registry.get(renderer)

        if renderer != "native" and not include_native_sections:
            available = {
                ReportCapability.RETURNS,
                ReportCapability.DRAWDOWNS,
                ReportCapability.CALENDAR_RETURNS,
                ReportCapability.FEES,
                ReportCapability.FUNDING,
                ReportCapability.EXPOSURES,
            }
            if self.bundle.benchmark is not None:
                available.add(ReportCapability.BENCHMARK)
            if self.bundle.position_snapshots is not None:
                available.add(ReportCapability.POSITIONS)
            skipped = sorted(item.value for item in available.difference(definition.capabilities))
            if skipped:
                data = self._with_issue(
                    data,
                    ValidationIssue(
                        "backend.capability_skipped",
                        f"{renderer} does not render available sections: {', '.join(skipped)}",
                        IssueSeverity.WARNING,
                    ),
                )

        try:
            prepared = definition.adapter.prepare(data, self.config)
            if renderer == "quantstats" and include_native_sections:
                appendix = destination.with_name(f"{destination.stem}.quantstats{destination.suffix or '.html'}")
                for issue in self._render_and_collect(
                    definition.renderer, prepared, appendix, self.config
                ):
                    data = self._with_issue(data, issue)
                native = self.registry.get("native")
                native_prepared = native.adapter.prepare(data, self.config)
                for issue in self._render_and_collect(
                    native.renderer, native_prepared, destination, self.config
                ):
                    data = self._with_issue(data, issue)
                self._append_link(destination, "Open QuantStats report", appendix.name)
                provider = "native+quantstats"
            else:
                for issue in self._render_and_collect(
                    definition.renderer, prepared, destination, self.config
                ):
                    data = self._with_issue(data, issue)
                provider = renderer
        except (BackendUnavailableError, ImportError) as exc:
            if self.config.strict_backend:
                raise
            issue = ValidationIssue(
                "backend.unavailable",
                str(exc),
                IssueSeverity.WARNING,
            )
            data = self._with_issue(data, issue)
            native = self.registry.get("native")
            native_prepared = native.adapter.prepare(data, self.config)
            for render_issue in self._render_and_collect(
                native.renderer, native_prepared, destination, self.config
            ):
                data = self._with_issue(data, render_issue)
            provider = "native"
        except Exception as exc:
            if self.config.strict_backend:
                raise
            if renderer == "native":
                issue = ValidationIssue(
                    "renderer.failed",
                    f"native report generation failed: {exc}",
                    IssueSeverity.ERROR,
                    table="renderer",
                )
                self._write_failed_report(destination, issue)
                return ReportArtifact(
                    path=destination,
                    status=ReportStatus.FAILED,
                    provider="native",
                    metrics=dict(data.metrics),
                    issues=data.validation.issues + (issue,),
                    requested_provider=requested,
                )
            issue = ValidationIssue(
                "backend.failed",
                f"{renderer} failed and native fallback was used: {exc}",
                IssueSeverity.WARNING,
            )
            data = self._with_issue(data, issue)
            native = self.registry.get("native")
            native_prepared = native.adapter.prepare(data, self.config)
            for render_issue in self._render_and_collect(
                native.renderer, native_prepared, destination, self.config
            ):
                data = self._with_issue(data, render_issue)
            provider = "native"
        return ReportArtifact(
            path=destination,
            status=data.validation.status,
            provider=provider,
            metrics=dict(data.metrics),
            issues=data.validation.issues,
            requested_provider=requested,
        )

    def export(self, output: str | Path, *, format: str = "parquet") -> Path:
        data = self.data()
        return export_report_bundle(
            self.bundle,
            data.metrics,
            data.validation,
            output,
            format=format,
            config=self.config,
            derived_tables={
                "canonical_periodic_returns": data.periodic_returns,
                "canonical_drawdowns": data.drawdowns,
                "canonical_monthly_returns": data.monthly_returns,
                "canonical_annual_returns": data.annual_returns,
                "canonical_weekday_returns": data.weekday_returns,
                "canonical_hourly_returns": data.hourly_returns,
            },
        )
