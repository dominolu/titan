from __future__ import annotations

import base64
from dataclasses import replace
import html
import io
import json
import os
from pathlib import Path
import tempfile
from typing import Any

import numpy as np
import polars as pl

from .backends import BackendUnavailableError, ReportRenderer
from .models import (
    MetricValue,
    PreparedReport,
    RenderOutcome,
    ReportCapability,
    ReportConfig,
    ReportData,
    ReportStatus,
    SectionAvailability,
    ValidationIssue,
    IssueSeverity,
)


def _matplotlib():
    import matplotlib

    matplotlib.use("Agg")
    from matplotlib import pyplot as plt

    return plt


def _figure_uri(fig: Any) -> str:
    stream = io.BytesIO()
    fig.savefig(stream, format="png", dpi=135, bbox_inches="tight")
    _matplotlib().close(fig)
    return "data:image/png;base64," + base64.b64encode(stream.getvalue()).decode("ascii")


def _extrema_indices(
    frame: pl.DataFrame,
    fields: tuple[str, ...],
    max_points: int,
) -> list[int]:
    """Select bounded plot points while retaining per-bucket extrema for each series."""
    length = len(frame)
    if length <= max_points:
        return list(range(length))
    usable_fields = [
        name
        for name in fields
        if name in frame.columns and frame.schema[name].is_numeric()
    ]
    if not usable_fields:
        step = max(1, int(np.ceil(length / max_points)))
        sampled = sorted(set([0, *range(0, length, step)]))[: max_points - 1]
        return sorted(set([*sampled, length - 1]))
    bucket_count = max(1, (max_points - 2) // (2 * len(usable_fields)))
    boundaries = np.linspace(1, length - 1, bucket_count + 1, dtype=int)
    arrays = {
        field: frame[field].cast(pl.Float64).to_numpy() for field in usable_fields
    }
    selected = {0, length - 1}
    for start, end in zip(boundaries[:-1], boundaries[1:]):
        if end <= start:
            continue
        for field in usable_fields:
            values = arrays[field][start:end]
            finite = np.flatnonzero(np.isfinite(values))
            if not len(finite):
                continue
            finite_values = values[finite]
            selected.add(start + int(finite[np.argmin(finite_values)]))
            selected.add(start + int(finite[np.argmax(finite_values)]))
    return sorted(selected)


def _downsample(
    frame: pl.DataFrame,
    fields: tuple[str, ...],
    max_points: int,
) -> pl.DataFrame:
    return frame[_extrema_indices(frame, fields, max_points)]


def _reporting_view(frame: pl.DataFrame) -> pl.DataFrame:
    if "view_kind" not in frame.columns:
        return frame
    kinds = {str(item) for item in frame["view_kind"].drop_nulls().unique().to_list()}
    for preferred in ("local_delivered", "local_delivery"):
        if preferred in kinds:
            return frame.filter(pl.col("view_kind") == preferred)
    return frame


def atomic_write_text(path: Path, document: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(handle, "w", encoding="utf-8") as stream:
            stream.write(document)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except Exception:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def _format_metric(metric: MetricValue) -> str:
    value = metric.value
    if value is None:
        return "N/A"
    if metric.unit == "ratio":
        if metric.metric_id.startswith(("return.", "risk.max_drawdown", "risk.volatility", "risk.var", "risk.cvar", "benchmark.return", "benchmark.tracking")):
            return f"{float(value) * 100:.2f}%"
        return f"{float(value):.3f}"
    if metric.unit in {"count", "sessions"}:
        return f"{int(value):,}"
    if isinstance(value, (float, np.floating)):
        return f"{float(value):,.2f} {html.escape(metric.unit)}"
    return html.escape(str(value))


def _plot_equity(data: ReportData, config: ReportConfig) -> str:
    plt = _matplotlib()
    frame = _downsample(
        data.bundle.portfolio_snapshots,
        ("equity_net", "equity_gross"),
        config.max_plot_points,
    )
    fig, axes = plt.subplots(2, 1, figsize=(11, 7), sharex=False)
    axes[0].plot(frame["timestamp"].to_list(), frame["equity_net"].to_numpy(), label="Net")
    axes[0].plot(
        frame["timestamp"].to_list(),
        frame["equity_gross"].to_numpy(),
        label="Gross",
        alpha=0.7,
    )
    axes[0].set_title("Portfolio Equity")
    axes[0].set_ylabel(data.bundle.metadata.reporting_currency)
    axes[0].grid(alpha=0.25)
    axes[0].legend()

    periodic = _downsample(
        data.periodic_returns,
        ("return", "benchmark_return"),
        config.max_plot_points,
    )
    if len(periodic):
        wealth = np.cumprod(1.0 + periodic["return"].to_numpy())
        axes[1].plot(periodic["session"].to_list(), wealth - 1.0, label="Strategy")
        if "benchmark_return" in periodic.columns:
            # Missing benchmark observations are not zero returns. Plot only the
            # explicitly aligned sample range so the chart matches canonical metrics.
            aligned = periodic.drop_nulls(["benchmark_return"])
            if len(aligned):
                benchmark = aligned["benchmark_return"].to_numpy()
                axes[1].plot(
                    aligned["session"].to_list(),
                    np.cumprod(1.0 + benchmark) - 1.0,
                    label="Benchmark",
                    alpha=0.75,
                )
    axes[1].set_title("Cumulative Returns")
    axes[1].set_ylabel("Return")
    axes[1].grid(alpha=0.25)
    axes[1].legend()
    fig.tight_layout()
    return _figure_uri(fig)


def _plot_drawdown(data: ReportData, config: ReportConfig) -> str:
    plt = _matplotlib()
    fig, ax = plt.subplots(figsize=(11, 3.6))
    drawdowns = _downsample(
        data.drawdowns, ("drawdown",), config.max_plot_points
    )
    if len(drawdowns):
        sessions = drawdowns["session"].to_list()
        values = drawdowns["drawdown"].to_numpy()
        ax.fill_between(sessions, values, 0.0, color="#c0392b", alpha=0.65)
        if "benchmark_return" in data.periodic_returns.columns:
            aligned = data.periodic_returns.drop_nulls(["benchmark_return"])
            if len(aligned):
                benchmark_wealth = np.cumprod(
                    1.0 + aligned["benchmark_return"].to_numpy()
                )
                benchmark_peak = np.maximum.accumulate(
                    np.concatenate(([1.0], benchmark_wealth))
                )[1:]
                benchmark_frame = pl.DataFrame(
                    {
                        "session": aligned["session"],
                        "drawdown": benchmark_wealth / benchmark_peak - 1.0,
                    }
                )
                benchmark_frame = _downsample(
                    benchmark_frame, ("drawdown",), config.max_plot_points
                )
                ax.plot(
                    benchmark_frame["session"].to_list(),
                    benchmark_frame["drawdown"].to_numpy(),
                    label="Benchmark",
                    color="#336699",
                    linewidth=1.2,
                )
                ax.legend()
    ax.set_title("Underwater Drawdown")
    ax.set_ylabel("Drawdown")
    ax.grid(alpha=0.25)
    fig.tight_layout()
    return _figure_uri(fig)


def _plot_calendar(data: ReportData) -> str:
    plt = _matplotlib()
    monthly = data.monthly_returns
    annual = data.annual_returns
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.2))
    if len(monthly):
        years = sorted({period[:4] for period in monthly["period"].to_list()})
        matrix = np.full((len(years), 12), np.nan)
        positions = {year: index for index, year in enumerate(years)}
        for period, value in monthly.iter_rows():
            matrix[positions[period[:4]], int(period[5:7]) - 1] = value * 100.0
        image = axes[0].imshow(matrix, aspect="auto", cmap="RdYlGn")
        axes[0].set_yticks(range(len(years)), years)
        axes[0].set_xticks(range(12), [str(month) for month in range(1, 13)])
        axes[0].set_title("Monthly Returns (%)")
        fig.colorbar(image, ax=axes[0], fraction=0.046)
    else:
        axes[0].text(0.5, 0.5, "Insufficient data", ha="center", va="center")
        axes[0].set_axis_off()
    if len(annual):
        values = annual["return"].to_numpy() * 100.0
        axes[1].bar(annual["period"].to_list(), values, color=np.where(values >= 0, "#2e8b57", "#c0392b"))
        axes[1].axhline(0.0, color="black", linewidth=0.8)
        axes[1].set_title("Annual Returns (%)")
        axes[1].tick_params(axis="x", rotation=45)
    else:
        axes[1].text(0.5, 0.5, "Insufficient data", ha="center", va="center")
        axes[1].set_axis_off()
    fig.tight_layout()
    return _figure_uri(fig)


def _rolling(values: np.ndarray, window: int, function: str) -> np.ndarray:
    output = np.full(len(values), np.nan)
    for index in range(window - 1, len(values)):
        sample = values[index - window + 1 : index + 1]
        if function == "return":
            output[index] = np.prod(1.0 + sample) - 1.0
        elif function == "volatility":
            output[index] = np.std(sample, ddof=1)
        else:
            deviation = np.std(sample, ddof=1)
            output[index] = np.mean(sample) / deviation if deviation > 0.0 else np.nan
    return output


def _plot_rolling_and_distribution(data: ReportData, config: ReportConfig) -> str:
    plt = _matplotlib()
    periodic = data.periodic_returns
    values = periodic["return"].to_numpy() if len(periodic) else np.array([])
    fig, axes = plt.subplots(2, 1, figsize=(11, 7))
    if len(values) >= 5:
        window = min(30, max(5, len(values) // 4))
        dates = periodic["session"].to_list()
        rolling_frame = pl.DataFrame(
            {
                "session": dates,
                "rolling_return": _rolling(values, window, "return"),
                "rolling_volatility": _rolling(values, window, "volatility")
                * np.sqrt(config.annualization),
                "rolling_sharpe": _rolling(values, window, "sharpe")
                * np.sqrt(config.annualization),
            }
        )
        rolling_frame = _downsample(
            rolling_frame,
            ("rolling_return", "rolling_volatility", "rolling_sharpe"),
            config.max_plot_points,
        )
        sampled_dates = rolling_frame["session"].to_list()
        axes[0].plot(
            sampled_dates,
            rolling_frame["rolling_return"].to_numpy(),
            label="Rolling Return",
        )
        axes[0].plot(
            sampled_dates,
            rolling_frame["rolling_volatility"].to_numpy(),
            label="Annualized Volatility",
        )
        axes[0].plot(
            sampled_dates,
            rolling_frame["rolling_sharpe"].to_numpy(),
            label="Sharpe",
        )
        span_days = (dates[-1] - dates[-window]).days if len(dates) > window else 0
        axes[0].set_title(
            f"Rolling Risk ({window} samples; latest window spans {span_days}D)"
        )
        axes[0].legend()
        axes[0].grid(alpha=0.25)
        axes[1].hist(values * 100.0, bins=min(40, max(5, len(values) // 2)), color="#336699", alpha=0.8)
        axes[1].axvline(0.0, color="black", linewidth=0.8)
        axes[1].set_title("Periodic Return Distribution")
        axes[1].set_xlabel("Return (%)")
    else:
        for ax in axes:
            ax.text(0.5, 0.5, "Insufficient data", ha="center", va="center")
            ax.set_axis_off()
    fig.tight_layout()
    return _figure_uri(fig)


def _plot_exposure_and_costs(data: ReportData, config: ReportConfig) -> str:
    plt = _matplotlib()
    frame = _downsample(
        data.bundle.portfolio_snapshots,
        (
            "gross_exposure",
            "net_exposure",
            "fee",
            "rebate",
            "funding",
            "trading_value",
            "trading_volume",
        ),
        config.max_plot_points,
    )
    fig, axes = plt.subplots(3, 1, figsize=(11, 9), sharex=True)
    timestamps = frame["timestamp"].to_list()
    if frame["gross_exposure"].null_count() < len(frame):
        axes[0].plot(timestamps, frame["gross_exposure"].to_numpy(), label="Gross Exposure")
    if frame["net_exposure"].null_count() < len(frame):
        axes[0].plot(timestamps, frame["net_exposure"].to_numpy(), label="Net Exposure")
    axes[0].set_title("Exposure")
    axes[0].grid(alpha=0.25)
    axes[0].legend()
    for name, label in (("fee", "Fee"), ("rebate", "Rebate"), ("funding", "Funding")):
        axes[1].plot(timestamps, frame[name].to_numpy(), label=label)
    axes[1].set_title("Cumulative Costs and Funding")
    axes[1].grid(alpha=0.25)
    axes[1].legend()
    trading_plotted = False
    for name, label in (
        ("trading_value", "Trading Value"),
        ("trading_volume", "Trading Volume"),
        ("num_trades", "Trade Counter"),
    ):
        if name in frame.columns and frame[name].null_count() < len(frame):
            axes[2].plot(timestamps, frame[name].to_numpy(), label=label)
            trading_plotted = True
    axes[2].set_title("Cumulative Trading Activity")
    axes[2].grid(alpha=0.25)
    if trading_plotted:
        axes[2].legend()
    else:
        axes[2].text(0.5, 0.5, "Trading counters unavailable", ha="center", va="center")
    fig.tight_layout()
    return _figure_uri(fig)


def _plot_positions(data: ReportData, config: ReportConfig) -> str:
    plt = _matplotlib()
    frame = data.bundle.position_snapshots
    if frame is None or frame.is_empty():
        raise ValueError("position snapshots are unavailable")
    frame = _reporting_view(frame)
    required = {"timestamp", "instrument_id", "quantity", "notional"}
    if not required.issubset(frame.columns):
        raise ValueError("position snapshots do not satisfy the canonical schema")
    notional_field = (
        "notional_reporting" if "notional_reporting" in frame.columns else "notional"
    )
    grouping = ["venue_id", "instrument_id"] if "venue_id" in frame.columns else ["instrument_id"]
    rankings = (
        frame.group_by(grouping)
        .agg(pl.col(notional_field).abs().max().alias("max_notional"))
        .sort("max_notional", descending=True)
        .head(10)
    )
    fig, axes = plt.subplots(2, 1, figsize=(11, 7), sharex=False)
    for key in rankings.select(grouping).iter_rows():
        predicate = pl.col("instrument_id") == key[-1]
        if len(grouping) == 2:
            predicate &= pl.col("venue_id") == key[0]
        item = frame.filter(predicate).sort("timestamp")
        item = _downsample(item, ("quantity", notional_field), config.max_plot_points)
        axes[0].plot(
            item["timestamp"].to_list(),
            item["quantity"].to_numpy(),
            label="/".join(str(part) for part in key),
        )
    axes[0].set_title("Position Quantity Over Time (Top 10 by Notional)")
    axes[0].set_ylabel("Quantity")
    axes[0].grid(alpha=0.25)
    if not rankings.is_empty():
        axes[0].legend(ncol=2)
    latest = frame.sort("timestamp").group_by(grouping).agg(
        pl.col(notional_field).last().alias("notional")
    ).sort(pl.col("notional").abs(), descending=True).head(10)
    values = latest["notional"].to_numpy()
    axes[1].bar(
        [
            "/".join(str(part) for part in key)
            for key in latest.select(grouping).iter_rows()
        ],
        values,
        color=np.where(values >= 0.0, "#2e8b57", "#c0392b"),
    )
    axes[1].set_title("Latest Position Notional Concentration")
    axes[1].set_ylabel(data.bundle.metadata.reporting_currency)
    axes[1].tick_params(axis="x", rotation=45)
    axes[1].grid(axis="y", alpha=0.25)
    fig.tight_layout()
    return _figure_uri(fig)


def _plot_execution_facts(data: ReportData, config: ReportConfig) -> str:
    plt = _matplotlib()
    fills = data.bundle.fill_events
    orders = data.bundle.order_events
    if fills is None or orders is None:
        raise ValueError("fill and order facts are unavailable")
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.2))
    if not fills.is_empty():
        price_field = "price_reporting" if "price_reporting" in fills.columns else "price"
        sampled = _downsample(
            fills.sort("timestamp"), (price_field, "quantity"), config.max_plot_points
        )
        sizes = np.maximum(sampled["quantity"].to_numpy(), 0.0)
        size_scale = 20.0 + 80.0 * sizes / max(float(np.max(sizes)), 1e-12)
        axes[0].scatter(
            sampled["timestamp"].to_list(),
            sampled[price_field].to_numpy(),
            s=size_scale,
            alpha=0.65,
        )
        axes[0].set_title("Fill Price and Quantity")
        axes[0].set_ylabel("Price")
        axes[0].grid(alpha=0.25)
    else:
        axes[0].text(0.5, 0.5, "No fills", ha="center", va="center")
    if not orders.is_empty():
        statuses = orders.group_by("status").agg(pl.len().alias("count")).sort("status")
        axes[1].bar(
            [str(item) for item in statuses["status"].to_list()],
            statuses["count"].to_numpy(),
            color="#336699",
        )
        axes[1].set_title("Order Event Status Counts")
        axes[1].set_ylabel("Events")
        axes[1].tick_params(axis="x", rotation=45)
        axes[1].grid(axis="y", alpha=0.25)
    else:
        axes[1].text(0.5, 0.5, "No order events", ha="center", va="center")
    fig.tight_layout()
    return _figure_uri(fig)


def _plot_risk_diagnostics(data: ReportData, config: ReportConfig) -> str:
    plt = _matplotlib()
    risk = data.bundle.risk_events
    marks = data.bundle.market_marks
    if (risk is None or risk.is_empty()) and (marks is None or marks.is_empty()):
        raise ValueError("risk and mark diagnostics are unavailable")
    fig, axes = plt.subplots(2, 1, figsize=(11, 7), sharex=False)
    if risk is not None and not risk.is_empty():
        for limit_id in risk["limit_id"].unique().to_list():
            item = risk.filter(pl.col("limit_id") == limit_id).sort("timestamp")
            item = _downsample(item, ("utilization",), config.max_plot_points)
            axes[0].plot(
                item["timestamp"].to_list(),
                item["utilization"].to_numpy() * 100.0,
                label=str(limit_id),
            )
        breaches = risk.filter(pl.col("breached"))
        if not breaches.is_empty():
            axes[0].scatter(
                breaches["timestamp"].to_list(),
                breaches["utilization"].to_numpy() * 100.0,
                color="#c0392b",
                marker="x",
                label="breach",
                zorder=4,
            )
        axes[0].axhline(100.0, color="#c0392b", linestyle="--", alpha=0.6)
        axes[0].set_title("Risk Limit Utilization and Breaches")
        axes[0].set_ylabel("Utilization (%)")
        axes[0].legend(ncol=2)
        axes[0].grid(alpha=0.25)
    else:
        axes[0].text(0.5, 0.5, "Risk-limit facts unavailable", ha="center", va="center")
    if marks is not None and not marks.is_empty():
        for instrument in marks["instrument_id"].unique().to_list():
            item = marks.filter(pl.col("instrument_id") == instrument).sort("timestamp")
            item = _downsample(item, ("age_ns",), config.max_plot_points)
            axes[1].plot(
                item["timestamp"].to_list(),
                item["age_ns"].to_numpy() / 1_000_000.0,
                label=str(instrument),
            )
        stale = marks.filter(pl.col("stale"))
        if not stale.is_empty():
            axes[1].scatter(
                stale["timestamp"].to_list(),
                stale["age_ns"].to_numpy() / 1_000_000.0,
                color="#c0392b",
                marker="x",
                label="stale",
                zorder=4,
            )
        if config.max_mark_age_ns is not None:
            axes[1].axhline(
                config.max_mark_age_ns / 1_000_000.0,
                color="#c0392b",
                linestyle="--",
                alpha=0.6,
            )
        axes[1].set_title("Valuation Mark Age and Stale Marks")
        axes[1].set_ylabel("Age (ms)")
        axes[1].legend(ncol=2)
        axes[1].grid(alpha=0.25)
    else:
        axes[1].text(0.5, 0.5, "Mark-age facts unavailable", ha="center", va="center")
    fig.tight_layout()
    return _figure_uri(fig)


def _plot_return_profiles(data: ReportData) -> str:
    plt = _matplotlib()
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.2))
    weekday = data.weekday_returns
    weekday_labels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    if len(weekday):
        buckets = weekday["bucket"].to_list()
        values = weekday["return"].to_numpy() * 100.0
        labels = [weekday_labels[int(item) - 1] for item in buckets]
        axes[0].bar(labels, values, color=np.where(values >= 0.0, "#2e8b57", "#c0392b"))
        axes[0].axhline(0.0, color="black", linewidth=0.8)
        axes[0].set_title("Average Return by Weekday (%)")
    else:
        axes[0].text(0.5, 0.5, "Insufficient data", ha="center", va="center")
        axes[0].set_axis_off()
    hourly = data.hourly_returns
    if len(hourly):
        buckets = hourly["bucket"].to_numpy()
        values = hourly["return"].to_numpy() * 100.0
        axes[1].bar(buckets, values, color=np.where(values >= 0.0, "#2e8b57", "#c0392b"))
        axes[1].axhline(0.0, color="black", linewidth=0.8)
        axes[1].set_title("Average Interval Return by Hour (%)")
        axes[1].set_xlabel("Reporting timezone hour")
    else:
        axes[1].text(0.5, 0.5, "Insufficient data", ha="center", va="center")
        axes[1].set_axis_off()
    fig.tight_layout()
    return _figure_uri(fig)


def _top_drawdowns(data: ReportData, limit: int = 5) -> list[dict[str, Any]]:
    frame = data.drawdowns
    if frame.is_empty():
        return []
    sessions = frame["session"].to_list()
    values = frame["drawdown"].to_numpy()
    episodes: list[dict[str, Any]] = []
    start: int | None = None
    for index, value in enumerate(values):
        if value < 0.0 and start is None:
            start = max(0, index - 1)
        recovered = value >= 0.0 and start is not None
        at_end = index == len(values) - 1 and start is not None
        if recovered or at_end:
            end = index
            segment = values[start : end + 1]
            valley_offset = int(np.argmin(segment))
            valley = start + valley_offset
            episodes.append(
                {
                    "peak": sessions[start],
                    "valley": sessions[valley],
                    "recovery": sessions[end] if recovered else None,
                    "drawdown": float(values[valley]),
                    "duration": end - start,
                    "ongoing": not recovered,
                }
            )
            start = None
    return sorted(episodes, key=lambda item: item["drawdown"])[:limit]


class NativeRenderer(ReportRenderer):
    name = "native"
    capabilities = frozenset(ReportCapability)

    def render(self, report: PreparedReport, output: Path, config: ReportConfig) -> RenderOutcome:
        if not isinstance(report.payload, ReportData):
            raise TypeError("NativeRenderer expects ReportData")
        data = report.payload
        output.parent.mkdir(parents=True, exist_ok=True)
        metrics = data.metrics
        cards = [
            ("Net Return", "return.net"),
            ("Annualized Return", "return.annualized"),
            ("Sharpe", "risk.sharpe"),
            ("Sortino", "risk.sortino"),
            ("Max Drawdown", "risk.max_drawdown"),
            ("Calmar", "risk.calmar"),
            ("Volatility", "risk.volatility.annualized"),
            ("Gain/Pain", "risk.gain_to_pain"),
            ("Total Fee", "cost.fee"),
            ("Funding", "pnl.funding"),
            ("Daily Turnover", "trading.daily_turnover"),
            ("Max Leverage", "exposure.leverage.max"),
        ]
        card_html = "".join(
            f'<div class="card"><span>{html.escape(label)}</span><strong>{_format_metric(metrics[key])}</strong></div>'
            for label, key in cards
            if key in metrics
        )
        render_issues: list[str] = []
        issue_rows = "".join(
            "<tr>"
            f"<td>{html.escape(issue.severity.value)}</td>"
            f"<td>{html.escape(issue.code)}</td>"
            f"<td>{html.escape(issue.message)}</td>"
            f"<td>{html.escape(issue.table or '')}</td>"
            "</tr>"
            for issue in data.validation.issues
        ) or '<tr><td colspan="4">No validation issues</td></tr>'
        metadata = data.bundle.metadata.to_dict(config.redact_fields)
        metadata_html = html.escape(json.dumps(metadata, ensure_ascii=False, indent=2, default=str))
        drawdown_rows = "".join(
            "<tr>"
            f"<td>{item['peak']}</td><td>{item['valley']}</td>"
            f"<td>{item['recovery'] or 'Ongoing'}</td>"
            f"<td>{item['drawdown'] * 100:.2f}%</td><td>{item['duration']}</td>"
            "</tr>"
            for item in _top_drawdowns(data)
        ) or '<tr><td colspan="5">No drawdown periods</td></tr>'
        metric_rows = "".join(
            "<tr>"
            f"<td>{html.escape(metric.metric_id)}</td>"
            f"<td>{_format_metric(metric)}</td>"
            f"<td>{html.escape(metric.unit)}</td>"
            f"<td>{html.escape(metric.provider)}</td>"
            f"<td>{metric.version}</td>"
            f"<td>{html.escape(json.dumps(dict(metric.parameters), sort_keys=True, default=str))}</td>"
            "</tr>"
            for metric in sorted(metrics.values(), key=lambda item: item.metric_id)
        ) or '<tr><td colspan="6">Canonical metrics unavailable</td></tr>'
        images: dict[str, str] = {}
        portfolio_columns = set(data.bundle.portfolio_snapshots.columns)
        plotters: list[tuple[str, Any]] = []
        if {"timestamp", "equity_net", "equity_gross"}.issubset(portfolio_columns):
            plotters.append(("Equity and Returns", lambda: _plot_equity(data, config)))
        plotters.extend(
            [
                ("Underwater Drawdown", lambda: _plot_drawdown(data, config)),
                ("Calendar Returns", lambda: _plot_calendar(data)),
                ("Return Profiles", lambda: _plot_return_profiles(data)),
                (
                    "Rolling Risk and Distribution",
                    lambda: _plot_rolling_and_distribution(data, config),
                ),
            ]
        )
        if {
            "timestamp",
            "gross_exposure",
            "net_exposure",
            "fee",
            "rebate",
            "funding",
        }.issubset(portfolio_columns):
            plotters.append(("Exposure and Costs", lambda: _plot_exposure_and_costs(data, config)))
        if data.bundle.position_snapshots is not None:
            plotters.append(("Positions", lambda: _plot_positions(data, config)))
        if data.bundle.fill_events is not None and data.bundle.order_events is not None:
            plotters.append(
                ("Execution Facts", lambda: _plot_execution_facts(data, config))
            )
        if data.bundle.risk_events is not None or data.bundle.market_marks is not None:
            plotters.append(
                ("Risk Diagnostics", lambda: _plot_risk_diagnostics(data, config))
            )
        plot_sections = {
            "Equity and Returns": "returns_risk",
            "Underwater Drawdown": "drawdown_tail",
            "Calendar Returns": "returns_risk",
            "Return Profiles": "returns_risk",
            "Rolling Risk and Distribution": "returns_risk",
            "Exposure and Costs": "exposure_attribution",
            "Positions": "exposure_attribution",
            "Execution Facts": "trading_costs",
            "Risk Diagnostics": "diagnostics",
        }
        failed_sections: dict[str, str] = {}
        for title, plotter in plotters:
            try:
                images[title] = plotter()
            except Exception as exc:
                render_issues.append(f"{title}: {exc}")
                failed_sections[plot_sections[title]] = str(exc)
        if render_issues:
            if not data.validation.issues:
                issue_rows = ""
            issue_rows += "".join(
                "<tr><td>warning</td><td>renderer.section_failed</td>"
                f"<td>{html.escape(message)}</td><td>renderer</td></tr>"
                for message in render_issues
            )
        sections = dict(data.sections)
        for section_id, reason in failed_sections.items():
            if section_id in sections:
                sections[section_id] = replace(
                    sections[section_id],
                    status=SectionAvailability.FAILED,
                    reason=f"renderer failed: {reason}",
                )
        section_rows = "".join(
            "<tr>"
            f"<td>{html.escape(section.section_id)}</td>"
            f"<td>{html.escape(section.status.value)}</td>"
            f"<td>{html.escape(section.reason or '')}</td>"
            f"<td>{html.escape(', '.join(section.source_fields))}</td>"
            "</tr>"
            for section in sections.values()
        )
        document_status = data.validation.status
        if render_issues and document_status == ReportStatus.VALID:
            document_status = ReportStatus.PARTIAL
        image_html = "".join(
            f'<section><h2>{html.escape(title)}</h2><img src="{uri}" alt="{html.escape(title)}"></section>'
            for title, uri in images.items()
        )
        document = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Titan Backtest Report</title>
<style>
body{{font-family:Inter,system-ui,sans-serif;margin:0;background:#f4f6f8;color:#18222d}}
main{{max-width:1180px;margin:auto;padding:28px}} h1,h2{{color:#12263a}}
.status{{display:inline-block;padding:6px 12px;border-radius:14px;background:#dce6ef}}
.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;margin:20px 0}}
.card{{background:white;border-radius:8px;padding:14px;box-shadow:0 1px 4px #0002}}
.card span{{display:block;color:#667788;font-size:13px}} .card strong{{font-size:22px}}
section{{background:white;margin:18px 0;padding:18px;border-radius:8px;box-shadow:0 1px 4px #0002}}
img{{display:block;width:100%;height:auto}} table{{width:100%;border-collapse:collapse}}
th,td{{text-align:left;border-bottom:1px solid #dde3e8;padding:8px;font-size:13px}}
pre{{white-space:pre-wrap;overflow-wrap:anywhere;background:#f7f8fa;padding:12px}}
</style></head><body><main>
<h1>Titan/HFTBacktest Report</h1>
<p class="status">{html.escape(document_status.value)}</p>
<p>Provider: titan · Calendar: {html.escape(config.calendar)} · Timezone: {html.escape(config.timezone)} · Day cutoff: {config.day_cutoff_hour:02d}:00 · Currency: {html.escape(config.reporting_currency)} · Initial capital: {config.initial_capital:,.2f} · Annualization: {config.annualization} · Return type: time-weighted simple return</p>
<div class="cards">{card_html}</div>
<section><h2>Section Availability</h2><table><thead><tr><th>Section</th><th>Status</th><th>Reason</th><th>Canonical sources</th></tr></thead><tbody>{section_rows}</tbody></table></section>
{image_html}
<section><h2>Canonical Metrics</h2><table><thead><tr><th>Metric ID</th><th>Value</th><th>Unit</th><th>Provider</th><th>Version</th><th>Parameters</th></tr></thead><tbody>{metric_rows}</tbody></table></section>
<section><h2>Top Drawdowns</h2><table><thead><tr><th>Peak</th><th>Valley</th><th>Recovery</th><th>Depth</th><th>Sessions</th></tr></thead><tbody>{drawdown_rows}</tbody></table></section>
<section><h2>Diagnostics</h2><table><thead><tr><th>Severity</th><th>Code</th><th>Message</th><th>Table</th></tr></thead><tbody>{issue_rows}</tbody></table></section>
<section><h2>Reproducibility Metadata</h2><pre>{metadata_html}</pre></section>
</main></body></html>"""
        atomic_write_text(output, document)
        return RenderOutcome(
            output,
            tuple(
                ValidationIssue(
                    "renderer.section_failed",
                    message,
                    IssueSeverity.WARNING,
                    table="renderer",
                )
                for message in render_issues
            ),
        )


class QuantStatsRenderer(ReportRenderer):
    name = "quantstats"
    capabilities = frozenset(
        {
            ReportCapability.RETURNS,
            ReportCapability.BENCHMARK,
            ReportCapability.DRAWDOWNS,
            ReportCapability.CALENDAR_RETURNS,
            ReportCapability.SELF_CONTAINED_HTML,
        }
    )

    def render(self, report: PreparedReport, output: Path, config: ReportConfig) -> Path:
        try:
            import quantstats as qs
        except ImportError as exc:
            raise BackendUnavailableError(
                "QuantStats reporting requires: pip install 'hftbacktest[reports]'"
            ) from exc
        output.parent.mkdir(parents=True, exist_ok=True)
        handle, temporary_name = tempfile.mkstemp(
            prefix=f".{output.stem}.", suffix=".html", dir=output.parent
        )
        os.close(handle)
        temporary = Path(temporary_name)
        payload = report.payload
        try:
            qs.reports.html(
                payload["returns"],
                benchmark=payload["benchmark"],
                rf=config.risk_free_rate,
                title=payload["title"],
                output=str(temporary),
                periods_per_year=config.annualization,
            )
            os.replace(temporary, output)
        finally:
            if temporary.exists():
                temporary.unlink()
        return output
