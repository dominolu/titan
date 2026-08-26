from __future__ import annotations

from datetime import date
from typing import Any

import numpy as np
import polars as pl

from .calendar import ReportingCalendar
from .models import MetricValue, ReportBundle, ReportConfig


def _compound(values: list[float] | np.ndarray) -> float:
    if len(values) == 0:
        return 0.0
    return float(np.prod(1.0 + np.asarray(values, dtype=float)) - 1.0)


def _sessions(frame: pl.DataFrame, calendar: ReportingCalendar) -> list[date]:
    return [
        calendar.session_for_timestamp(value)
        for value in frame["timestamp"].to_list()
    ]


def _canonical_snapshots(bundle: ReportBundle) -> pl.DataFrame:
    portfolio = bundle.portfolio_snapshots.sort("timestamp")
    aggregations: list[pl.Expr] = []
    for name in portfolio.columns:
        if name == "timestamp":
            continue
        if name == "external_flow":
            aggregations.append(pl.col(name).fill_null(0.0).sum())
        else:
            aggregations.append(pl.col(name).last())
    return portfolio.group_by("timestamp", maintain_order=True).agg(aggregations)


def build_periodic_returns(
    bundle: ReportBundle,
    config: ReportConfig,
    calendar: ReportingCalendar,
) -> pl.DataFrame:
    portfolio = _canonical_snapshots(bundle)
    if len(portfolio) < 2:
        return pl.DataFrame(
            schema={
                "session": pl.Date,
                "return": pl.Float64,
                "equity_net": pl.Float64,
                "equity_gross": pl.Float64,
                "external_flow": pl.Float64,
            }
        )
    sessions = _sessions(portfolio, calendar)
    equity_net = portfolio["equity_net"].cast(pl.Float64).to_numpy()
    equity_gross = portfolio["equity_gross"].cast(pl.Float64).to_numpy()
    flows = portfolio["external_flow"].fill_null(0.0).cast(pl.Float64).to_numpy()

    by_session: dict[date, dict[str, Any]] = {}
    for index in range(1, len(portfolio)):
        previous = equity_net[index - 1]
        current = equity_net[index]
        flow = flows[index]
        value = (current - flow) / previous - 1.0
        session = sessions[index]
        entry = by_session.setdefault(
            session,
            {
                "returns": [],
                "gross_returns": [],
                "equity_net": current,
                "equity_gross": equity_gross[index],
                "flow": 0.0,
            },
        )
        entry["returns"].append(float(value))
        entry["gross_returns"].append(
            float((equity_gross[index] - flow) / equity_gross[index - 1] - 1.0)
        )
        entry["equity_net"] = float(current)
        entry["equity_gross"] = float(equity_gross[index])
        entry["flow"] += float(flow)

    periodic = pl.DataFrame(
        {
            "session": list(by_session),
            "return": [_compound(entry["returns"]) for entry in by_session.values()],
            "gross_return": [_compound(entry["gross_returns"]) for entry in by_session.values()],
            "equity_net": [entry["equity_net"] for entry in by_session.values()],
            "equity_gross": [entry["equity_gross"] for entry in by_session.values()],
            "external_flow": [entry["flow"] for entry in by_session.values()],
        },
        schema_overrides={"session": pl.Date},
    )
    benchmark = build_benchmark_returns(bundle, calendar)
    if benchmark is not None:
        periodic = periodic.join(benchmark, on="session", how="left")
    return periodic.sort("session")


def build_benchmark_returns(
    bundle: ReportBundle,
    calendar: ReportingCalendar,
) -> pl.DataFrame | None:
    if bundle.benchmark is None or bundle.benchmark.is_empty():
        return None
    value = bundle.benchmark.sort("timestamp")
    required = {"timestamp", "equity_or_return", "value_kind"}
    if not required.issubset(value.columns):
        return None
    kinds = value["value_kind"].drop_nulls().unique().to_list()
    if len(kinds) != 1 or kinds[0] not in {"return", "equity"}:
        return None
    sessions = _sessions(value, calendar)
    raw = value["equity_or_return"].cast(pl.Float64).to_numpy()
    if kinds[0] == "equity":
        if len(raw) < 2:
            return None
        returns = raw[1:] / raw[:-1] - 1.0
        sessions = sessions[1:]
    else:
        returns = raw
    grouped: dict[date, list[float]] = {}
    for session, item in zip(sessions, returns):
        if np.isfinite(item):
            grouped.setdefault(session, []).append(float(item))
    return pl.DataFrame(
        {
            "session": list(grouped),
            "benchmark_return": [_compound(items) for items in grouped.values()],
        },
        schema_overrides={"session": pl.Date},
    ).sort("session")


def build_drawdowns(periodic_returns: pl.DataFrame) -> pl.DataFrame:
    if periodic_returns.is_empty():
        return pl.DataFrame(
            schema={
                "session": pl.Date,
                "wealth": pl.Float64,
                "peak": pl.Float64,
                "drawdown": pl.Float64,
            }
        )
    returns = periodic_returns["return"].to_numpy()
    wealth = np.cumprod(1.0 + returns)
    # The initial portfolio wealth is a real high-water mark. Without this
    # baseline, a loss in the first reporting period becomes its own peak.
    peak = np.maximum.accumulate(np.concatenate(([1.0], wealth)))[1:]
    drawdown = wealth / peak - 1.0
    return pl.DataFrame(
        {
            "session": periodic_returns["session"],
            "wealth": wealth,
            "peak": peak,
            "drawdown": drawdown,
        }
    )


def build_calendar_returns(
    periodic_returns: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    if periodic_returns.is_empty():
        monthly = pl.DataFrame(schema={"period": pl.String, "return": pl.Float64})
        annual = pl.DataFrame(schema={"period": pl.String, "return": pl.Float64})
        return monthly, annual
    value = periodic_returns.with_columns(
        pl.col("session").dt.strftime("%Y-%m").alias("month"),
        pl.col("session").dt.strftime("%Y").alias("year"),
    )
    monthly = value.group_by("month", maintain_order=True).agg(
        ((pl.col("return") + 1.0).product() - 1.0).alias("return")
    ).rename({"month": "period"})
    annual = value.group_by("year", maintain_order=True).agg(
        ((pl.col("return") + 1.0).product() - 1.0).alias("return")
    ).rename({"year": "period"})
    return monthly, annual


def build_return_profiles(
    bundle: ReportBundle,
    calendar: ReportingCalendar,
    periodic_returns: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    if periodic_returns.is_empty():
        weekday = pl.DataFrame(schema={"bucket": pl.Int8, "return": pl.Float64, "samples": pl.UInt32})
    else:
        weekday = periodic_returns.with_columns(
            pl.col("session").dt.weekday().alias("bucket")
        ).group_by("bucket").agg(
            pl.col("return").mean().alias("return"),
            pl.len().cast(pl.UInt32).alias("samples"),
        ).sort("bucket")

    portfolio = _canonical_snapshots(bundle)
    if len(portfolio) < 2:
        hourly = pl.DataFrame(schema={"bucket": pl.Int8, "return": pl.Float64, "samples": pl.UInt32})
        return weekday, hourly
    equity = portfolio["equity_net"].cast(pl.Float64).to_numpy()
    flows = portfolio["external_flow"].fill_null(0.0).cast(pl.Float64).to_numpy()
    hours = portfolio.select(
        pl.col("timestamp").dt.convert_time_zone(calendar.timezone).dt.hour().alias("hour")
    )["hour"].to_list()
    values: dict[int, list[float]] = {}
    for index in range(1, len(portfolio)):
        item = (equity[index] - flows[index]) / equity[index - 1] - 1.0
        if np.isfinite(item):
            values.setdefault(int(hours[index]), []).append(float(item))
    hourly = pl.DataFrame(
        {
            "bucket": list(values),
            "return": [float(np.mean(items)) for items in values.values()],
            "samples": [len(items) for items in values.values()],
        },
        schema_overrides={"bucket": pl.Int8, "samples": pl.UInt32},
    ).sort("bucket")
    return weekday, hourly


def _metric(
    metric_id: str,
    value: float | int | str | None,
    unit: str,
    **parameters: Any,
) -> MetricValue:
    if isinstance(value, (float, np.floating)) and not np.isfinite(value):
        value = None
    parameters.setdefault("input_frequency", "reporting_session")
    return MetricValue(metric_id, value, unit, parameters=parameters)


def _max_underwater_duration(drawdowns: np.ndarray) -> int:
    longest = 0
    current = 0
    for value in drawdowns:
        if value < 0.0:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest


def compute_metrics(
    bundle: ReportBundle,
    config: ReportConfig,
    periodic_returns: pl.DataFrame,
    drawdowns: pl.DataFrame,
) -> dict[str, MetricValue]:
    portfolio = _canonical_snapshots(bundle)
    returns = periodic_returns["return"].drop_nulls().to_numpy()
    periods = config.annualization
    metrics: dict[str, MetricValue] = {}

    total_return = _compound(returns)
    gross_return = (
        _compound(periodic_returns["gross_return"].drop_nulls().to_numpy())
        if "gross_return" in periodic_returns.columns
        else None
    )
    annual_return: float | None = None
    volatility: float | None = None
    sharpe: float | None = None
    sortino: float | None = None
    if len(returns) >= config.minimum_annualization_samples:
        annual_return = (1.0 + total_return) ** (periods / len(returns)) - 1.0
    if len(returns) >= 2:
        volatility = float(np.std(returns, ddof=1) * np.sqrt(periods))
        risk_free_period = (1.0 + config.risk_free_rate) ** (1.0 / periods) - 1.0
        excess = returns - risk_free_period
        standard_deviation = float(np.std(excess, ddof=1))
        if standard_deviation > 0.0:
            sharpe = float(np.mean(excess) / standard_deviation * np.sqrt(periods))
        downside = float(np.sqrt(np.mean(np.minimum(excess, 0.0) ** 2)))
        if downside > 0.0:
            sortino = float(np.mean(excess) / downside * np.sqrt(periods))

    drawdown_values = drawdowns["drawdown"].to_numpy() if len(drawdowns) else np.array([])
    max_drawdown = float(np.min(drawdown_values)) if len(drawdown_values) else 0.0
    max_duration = _max_underwater_duration(drawdown_values)
    calmar = (
        annual_return / abs(max_drawdown)
        if annual_return is not None and max_drawdown < 0.0
        else None
    )
    positive = float(returns[returns > 0.0].sum()) if len(returns) else 0.0
    negative = abs(float(returns[returns < 0.0].sum())) if len(returns) else 0.0
    gain_to_pain = positive / negative if negative > 0.0 else None

    var_95 = float(np.quantile(returns, 0.05)) if len(returns) else None
    cvar_95 = (
        float(np.mean(returns[returns <= var_95]))
        if var_95 is not None and np.any(returns <= var_95)
        else None
    )
    skew = None
    kurtosis = None
    if len(returns) >= 3:
        centered = returns - np.mean(returns)
        sigma = float(np.std(returns))
        if sigma > 0.0:
            skew = float(np.mean((centered / sigma) ** 3))
            kurtosis = float(np.mean((centered / sigma) ** 4) - 3.0)

    metrics.update(
        {
            "return.net": _metric("return.net", total_return, "ratio"),
            "return.gross": _metric("return.gross", gross_return, "ratio"),
            "return.annualized": _metric(
                "return.annualized",
                annual_return,
                "ratio",
                periods_per_year=periods,
                minimum_samples=config.minimum_annualization_samples,
                samples=len(returns),
            ),
            "risk.volatility.annualized": _metric(
                "risk.volatility.annualized", volatility, "ratio", periods_per_year=periods
            ),
            "risk.sharpe": _metric(
                "risk.sharpe",
                sharpe,
                "ratio",
                periods_per_year=periods,
                risk_free_rate=config.risk_free_rate,
            ),
            "risk.sortino": _metric(
                "risk.sortino",
                sortino,
                "ratio",
                periods_per_year=periods,
                risk_free_rate=config.risk_free_rate,
            ),
            "risk.calmar": _metric("risk.calmar", calmar, "ratio"),
            "risk.max_drawdown": _metric("risk.max_drawdown", max_drawdown, "ratio"),
            "risk.max_drawdown_duration": _metric(
                "risk.max_drawdown_duration", max_duration, "sessions"
            ),
            "risk.gain_to_pain": _metric("risk.gain_to_pain", gain_to_pain, "ratio"),
            "risk.var_95": _metric("risk.var_95", var_95, "ratio", confidence=0.95),
            "risk.cvar_95": _metric("risk.cvar_95", cvar_95, "ratio", confidence=0.95),
            "distribution.skew": _metric("distribution.skew", skew, "ratio"),
            "distribution.excess_kurtosis": _metric(
                "distribution.excess_kurtosis", kurtosis, "ratio"
            ),
        }
    )
    fills = bundle.fill_events
    orders = bundle.order_events
    round_trips = bundle.round_trip_events
    if fills is not None:
        metrics["trading.number_of_fills"] = _metric(
            "trading.number_of_fills",
            len(fills),
            "count",
            input_frequency="execution_event",
        )
        if not fills.is_empty() and "quantity" in fills.columns:
            quantities = fills["quantity"].drop_nulls()
            metrics["trading.fill_quantity.mean"] = _metric(
                "trading.fill_quantity.mean",
                float(quantities.mean()) if len(quantities) else None,
                "quantity",
                input_frequency="execution_event",
            )
            metrics["trading.fill_quantity.median"] = _metric(
                "trading.fill_quantity.median",
                float(quantities.median()) if len(quantities) else None,
                "quantity",
                input_frequency="execution_event",
            )
    if orders is not None:
        order_count = (
            orders.select(pl.struct("venue_id", "order_id").n_unique()).item()
            if not orders.is_empty() and {"venue_id", "order_id"}.issubset(orders.columns)
            else 0
        )
        metrics["trading.number_of_orders"] = _metric(
            "trading.number_of_orders",
            order_count,
            "count",
            input_frequency="order_event",
        )
    if round_trips is not None:
        metrics["trading.number_of_round_trips"] = _metric(
            "trading.number_of_round_trips",
            len(round_trips),
            "count",
            input_frequency="round_trip",
        )
        pnl_field = (
            "net_pnl_reporting"
            if "net_pnl_reporting" in round_trips.columns
            else "net_pnl"
        )
        if not round_trips.is_empty() and pnl_field in round_trips.columns:
            pnl = round_trips[pnl_field].drop_nulls().to_numpy()
            profits = float(pnl[pnl > 0.0].sum()) if len(pnl) else 0.0
            losses = abs(float(pnl[pnl < 0.0].sum())) if len(pnl) else 0.0
            metrics["trading.profit_factor"] = _metric(
                "trading.profit_factor",
                profits / losses if losses > 0.0 else None,
                "ratio",
                input_frequency="round_trip",
            )

    def final(name: str) -> float | int | None:
        if name not in portfolio.columns or portfolio[name].null_count() == len(portfolio):
            return None
        return portfolio[name].drop_nulls()[-1]

    elapsed_days = max(
        (portfolio["timestamp"][-1] - portfolio["timestamp"][0]).total_seconds() / 86_400,
        1.0 / periods,
    )
    trading_value = final("trading_value")
    daily_turnover = (
        float(trading_value) / config.initial_capital / elapsed_days
        if trading_value is not None
        else None
    )
    metrics.update(
        {
            "cost.fee": _metric(
                "cost.fee", final("fee"), config.reporting_currency,
                input_frequency="snapshot_cumulative",
            ),
            "cost.rebate": _metric(
                "cost.rebate", final("rebate"), config.reporting_currency,
                input_frequency="snapshot_cumulative",
            ),
            "cost.net": _metric(
                "cost.net",
                float(final("fee")) - float(final("rebate"))
                if final("fee") is not None and final("rebate") is not None
                else None,
                config.reporting_currency,
                input_frequency="snapshot_cumulative",
            ),
            "pnl.funding": _metric(
                "pnl.funding", final("funding"), config.reporting_currency,
                input_frequency="snapshot_cumulative",
            ),
            "trading.number_of_trades": _metric(
                "trading.number_of_trades", final("num_trades"), "count"
            ),
            "trading.volume": _metric("trading.volume", final("trading_volume"), "quantity"),
            "trading.value": _metric(
                "trading.value", trading_value, config.reporting_currency
            ),
            "trading.daily_turnover": _metric(
                "trading.daily_turnover", daily_turnover, "ratio"
            ),
            "trading.daily_number_of_trades": _metric(
                "trading.daily_number_of_trades",
                float(final("num_trades")) / elapsed_days
                if final("num_trades") is not None
                else None,
                "count",
            ),
            "cost.fee_per_trading_value": _metric(
                "cost.fee_per_trading_value",
                float(final("fee")) / float(trading_value)
                if final("fee") is not None and trading_value not in (None, 0)
                else None,
                "ratio",
            ),
            "exposure.gross.max": _metric(
                "exposure.gross.max",
                float(portfolio["gross_exposure"].drop_nulls().max())
                if portfolio["gross_exposure"].drop_nulls().len()
                else None,
                config.reporting_currency,
            ),
            "exposure.net.max_abs": _metric(
                "exposure.net.max_abs",
                float(portfolio["net_exposure"].drop_nulls().abs().max())
                if portfolio["net_exposure"].drop_nulls().len()
                else None,
                config.reporting_currency,
            ),
            "exposure.leverage.max": _metric(
                "exposure.leverage.max",
                float(portfolio["leverage"].drop_nulls().max())
                if portfolio["leverage"].drop_nulls().len()
                else None,
                "ratio",
            ),
        }
    )
    gross_values = portfolio["gross_exposure"].drop_nulls().to_numpy()
    net_values = portfolio["net_exposure"].drop_nulls().to_numpy()
    if len(gross_values) and len(net_values):
        long_values = (gross_values + net_values) / 2.0
        short_values = (gross_values - net_values) / 2.0
        metrics["exposure.long.max"] = _metric(
            "exposure.long.max", float(np.max(long_values)), config.reporting_currency
        )
        metrics["exposure.short.max"] = _metric(
            "exposure.short.max", float(np.max(short_values)), config.reporting_currency
        )

    if len(returns):
        metrics["return.best_session"] = _metric(
            "return.best_session", float(np.max(returns)), "ratio"
        )
        metrics["return.worst_session"] = _metric(
            "return.worst_session", float(np.min(returns)), "ratio"
        )
        metrics["return.winning_session_rate"] = _metric(
            "return.winning_session_rate", float(np.mean(returns > 0.0)), "ratio"
        )
        longest_wins = longest_losses = current_wins = current_losses = 0
        for item in returns:
            if item > 0.0:
                current_wins += 1
                current_losses = 0
            elif item < 0.0:
                current_losses += 1
                current_wins = 0
            else:
                current_wins = current_losses = 0
            longest_wins = max(longest_wins, current_wins)
            longest_losses = max(longest_losses, current_losses)
        metrics["return.max_consecutive_wins"] = _metric(
            "return.max_consecutive_wins", longest_wins, "sessions"
        )
        metrics["return.max_consecutive_losses"] = _metric(
            "return.max_consecutive_losses", longest_losses, "sessions"
        )
        calendar_value = periodic_returns.with_columns(
            pl.col("session").dt.strftime("%G-%V").alias("week"),
            pl.col("session").dt.strftime("%Y-%m").alias("month"),
        )
        for bucket in ("week", "month"):
            grouped = calendar_value.group_by(bucket).agg(
                ((pl.col("return") + 1.0).product() - 1.0).alias("return")
            )
            metrics[f"return.best_{bucket}"] = _metric(
                f"return.best_{bucket}", float(grouped["return"].max()), "ratio"
            )
            metrics[f"return.worst_{bucket}"] = _metric(
                f"return.worst_{bucket}", float(grouped["return"].min()), "ratio"
            )

    if "benchmark_return" in periodic_returns.columns:
        aligned = periodic_returns.drop_nulls(["return", "benchmark_return"])
        if len(aligned):
            strategy = aligned["return"].to_numpy()
            benchmark = aligned["benchmark_return"].to_numpy()
            benchmark_total = _compound(benchmark)
            active = strategy - benchmark
            benchmark_input = bundle.benchmark
            input_samples = len(benchmark_input) if benchmark_input is not None else 0
            expected_input_returns = max(
                input_samples
                - (
                    1
                    if benchmark_input is not None
                    and not benchmark_input.is_empty()
                    and benchmark_input["value_kind"][0] == "equity"
                    else 0
                ),
                0,
            )
            aligned_sessions = aligned["session"].to_list()
            tracking_error = (
                float(np.std(active, ddof=1) * np.sqrt(periods)) if len(active) >= 2 else None
            )
            information_ratio = (
                float(np.mean(active) * periods / tracking_error)
                if tracking_error is not None and tracking_error > 0.0
                else None
            )
            beta = None
            alpha = None
            if len(active) >= 2 and float(np.var(benchmark, ddof=1)) > 0.0:
                beta = float(np.cov(strategy, benchmark, ddof=1)[0, 1] / np.var(benchmark, ddof=1))
                alpha = float((np.mean(strategy) - beta * np.mean(benchmark)) * periods)
            metrics.update(
                {
                    "benchmark.return": _metric("benchmark.return", benchmark_total, "ratio"),
                    "benchmark.tracking_error": _metric(
                        "benchmark.tracking_error", tracking_error, "ratio"
                    ),
                    "benchmark.information_ratio": _metric(
                        "benchmark.information_ratio", information_ratio, "ratio"
                    ),
                    "benchmark.beta": _metric("benchmark.beta", beta, "ratio"),
                    "benchmark.alpha": _metric("benchmark.alpha", alpha, "ratio"),
                    "benchmark.excess_return": _metric(
                        "benchmark.excess_return", total_return - benchmark_total, "ratio"
                    ),
                    "benchmark.correlation": _metric(
                        "benchmark.correlation",
                        float(np.corrcoef(strategy, benchmark)[0, 1])
                        if len(strategy) >= 2
                        and np.std(strategy) > 0.0
                        and np.std(benchmark) > 0.0
                        else None,
                        "ratio",
                    ),
                    "benchmark.up_capture": _metric(
                        "benchmark.up_capture",
                        float(np.mean(strategy[benchmark > 0.0]) / np.mean(benchmark[benchmark > 0.0]))
                        if np.any(benchmark > 0.0) and np.mean(benchmark[benchmark > 0.0]) != 0.0
                        else None,
                        "ratio",
                    ),
                    "benchmark.down_capture": _metric(
                        "benchmark.down_capture",
                        float(np.mean(strategy[benchmark < 0.0]) / np.mean(benchmark[benchmark < 0.0]))
                        if np.any(benchmark < 0.0) and np.mean(benchmark[benchmark < 0.0]) != 0.0
                        else None,
                        "ratio",
                    ),
                    "benchmark.aligned_samples": _metric(
                        "benchmark.aligned_samples", len(aligned), "count"
                    ),
                    "benchmark.input_samples": _metric(
                        "benchmark.input_samples", input_samples, "count"
                    ),
                    "benchmark.strategy_samples": _metric(
                        "benchmark.strategy_samples", len(periodic_returns), "count"
                    ),
                    "benchmark.dropped_strategy_samples": _metric(
                        "benchmark.dropped_strategy_samples",
                        len(periodic_returns) - len(aligned),
                        "count",
                    ),
                    "benchmark.dropped_input_samples": _metric(
                        "benchmark.dropped_input_samples",
                        max(expected_input_returns - len(aligned), 0),
                        "count",
                    ),
                    "benchmark.aligned_start": _metric(
                        "benchmark.aligned_start",
                        aligned_sessions[0].isoformat(),
                        "date",
                    ),
                    "benchmark.aligned_end": _metric(
                        "benchmark.aligned_end",
                        aligned_sessions[-1].isoformat(),
                        "date",
                    ),
                    "benchmark.input_start": _metric(
                        "benchmark.input_start",
                        str(benchmark_input["timestamp"].min())
                        if benchmark_input is not None and not benchmark_input.is_empty()
                        else None,
                        "datetime",
                    ),
                    "benchmark.input_end": _metric(
                        "benchmark.input_end",
                        str(benchmark_input["timestamp"].max())
                        if benchmark_input is not None and not benchmark_input.is_empty()
                        else None,
                        "datetime",
                    ),
                    "benchmark.coverage": _metric(
                        "benchmark.coverage",
                        len(aligned) / len(periodic_returns) if len(periodic_returns) else None,
                        "ratio",
                    ),
                }
            )
    return metrics
