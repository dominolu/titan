from __future__ import annotations

from collections.abc import Mapping
from bisect import bisect_right
from typing import Any

import numpy as np
import polars as pl

from .models import ReportBundle, ReportConfig, RunMetadata


PORTFOLIO_DEFAULTS: dict[str, Any] = {
    "cash": None,
    "realized_pnl": None,
    "unrealized_pnl": None,
    "fee": None,
    "rebate": None,
    "funding": None,
    "external_flow": None,
    "gross_exposure": None,
    "net_exposure": None,
    "margin": None,
    "leverage": None,
    "num_trades": None,
    "trading_volume": None,
    "trading_value": None,
}


def _value(item: Any, name: str, default: Any = None) -> Any:
    if isinstance(item, Mapping):
        return item.get(name, default)
    return getattr(item, name, default)


def _label(value: Any) -> str:
    if value is None:
        return "unknown"
    name = getattr(value, "name", None)
    if name is not None:
        return str(name).casefold()
    raw = getattr(value, "value", value)
    return str(raw).casefold()


def execution_reports_to_tables(
    reports: Any,
    config: ReportConfig,
    *,
    currency_map: Mapping[Any, str] | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Convert canonical Rust execution reports or mapping equivalents into logical tables."""
    fill_rows: list[dict[str, Any]] = []
    order_rows: list[dict[str, Any]] = []
    for report in reports:
        kind = _label(_value(report, "kind"))
        sequence = int(_value(report, "sequence", 0))
        venue = _value(report, "venue_id", config.venue_id)
        instrument = _value(report, "instrument_id", config.instrument_id)
        venue = getattr(venue, "value", getattr(venue, "0", venue))
        instrument = getattr(instrument, "value", getattr(instrument, "0", instrument))
        order_id = int(_value(report, "order_id", 0))
        delivery_ts = _value(report, "delivery_ts", _value(report, "timestamp"))
        exchange_ts = _value(report, "exchange_ts", delivery_ts)
        exec_price = float(_value(report, "exec_price", 0.0))
        exec_qty = float(_value(report, "exec_qty", 0.0))
        delta = _value(report, "account_delta")
        signed_fee = float(_value(delta, "fee", 0.0)) if delta is not None else 0.0
        order_rows.append(
            {
                "timestamp": delivery_ts,
                "exchange_timestamp": exchange_ts,
                "event_sequence": sequence,
                "order_id": order_id,
                "venue_id": str(venue),
                "instrument_id": str(instrument),
                "side": _label(_value(report, "side")),
                "status": _label(_value(report, "status", kind)),
                "request": _label(_value(report, "request", "unknown")),
                "quantity": float(_value(report, "order_qty", 0.0)),
                "price": float(_value(report, "order_price", 0.0)),
                "executed_quantity": exec_qty,
                "reason": _label(_value(report, "reason")),
            }
        )
        if kind == "fill":
            raw_currency = _value(delta, "currency") if delta is not None else None
            raw_currency = getattr(raw_currency, "value", raw_currency)
            currency = None
            if currency_map is not None:
                try:
                    currency = currency_map.get(raw_currency)
                except TypeError:
                    currency = None
                if currency is None:
                    currency = currency_map.get(str(raw_currency))
            if currency is None and isinstance(raw_currency, str):
                currency = raw_currency
            if not currency:
                currency = "unknown" if raw_currency is None else str(raw_currency)
            trade_value = (
                float(_value(delta, "trade_value", abs(exec_price * exec_qty)))
                if delta is not None
                else abs(exec_price * exec_qty)
            )
            fill_rows.append(
                {
                    "timestamp": delivery_ts,
                    "exchange_timestamp": exchange_ts,
                    "fill_id": f"{venue}:{order_id}:{sequence}",
                    "order_id": order_id,
                    "venue_id": str(venue),
                    "instrument_id": str(instrument),
                    "side": _label(_value(report, "side")),
                    "quantity": exec_qty,
                    "price": exec_price,
                    "notional": trade_value,
                    "fee": max(signed_fee, 0.0),
                    "rebate": max(-signed_fee, 0.0),
                    "currency": currency,
                    "liquidity": "maker" if bool(_value(report, "maker", False)) else "taker",
                }
            )
    fills = (
        pl.DataFrame(fill_rows, schema_overrides={"order_id": pl.UInt64})
        if fill_rows
        else pl.DataFrame()
    )
    orders = (
        pl.DataFrame(order_rows, schema_overrides={"order_id": pl.UInt64})
        if order_rows
        else pl.DataFrame()
    )
    if not fills.is_empty():
        fills = ensure_timestamp(fills, config.timestamp_unit, config.timezone)
        fills = ensure_timestamp(
            fills,
            config.timestamp_unit,
            config.timezone,
            timestamp_field="exchange_timestamp",
        )
    if not orders.is_empty():
        orders = ensure_timestamp(orders, config.timestamp_unit, config.timezone)
        orders = ensure_timestamp(
            orders,
            config.timestamp_unit,
            config.timezone,
            timestamp_field="exchange_timestamp",
        )
    return fills, orders


def ensure_timestamp(
    df: pl.DataFrame,
    unit: str,
    timezone: str,
    *,
    naive_timezone: str | None = None,
    timestamp_field: str = "timestamp",
) -> pl.DataFrame:
    if timestamp_field not in df.columns:
        return df
    dtype = df.schema[timestamp_field]
    if isinstance(dtype, pl.Datetime):
        if dtype.time_zone is None:
            source_timezone = naive_timezone or timezone
            value = pl.col(timestamp_field).dt.replace_time_zone(source_timezone)
            if source_timezone != timezone:
                value = value.dt.convert_time_zone(timezone)
            return df.with_columns(value)
        if dtype.time_zone != timezone:
            return df.with_columns(pl.col(timestamp_field).dt.convert_time_zone(timezone))
        return df
    if dtype.is_integer():
        return df.with_columns(
            pl.from_epoch(timestamp_field, time_unit=unit)
            .dt.replace_time_zone("UTC")
            .dt.convert_time_zone(timezone)
        )
    return df


def normalize_portfolio_snapshots(df: pl.DataFrame, config: ReportConfig) -> pl.DataFrame:
    value = ensure_timestamp(df.clone(), config.timestamp_unit, config.timezone)
    for name, default in PORTFOLIO_DEFAULTS.items():
        if name not in value.columns:
            value = value.with_columns(pl.lit(default, dtype=pl.Float64).alias(name))
    if "reporting_currency" not in value.columns:
        value = value.with_columns(pl.lit(config.reporting_currency).alias("reporting_currency"))
    if "timestamp_kind" not in value.columns:
        value = value.with_columns(pl.lit("local_delivery").alias("timestamp_kind"))
    return value


def _apply_fx_marks(
    frame: pl.DataFrame | None,
    fx_marks: pl.DataFrame | None,
    config: ReportConfig,
    *,
    currency_field: str,
    amount_fields: tuple[str, ...],
    timestamp_field: str = "timestamp",
) -> pl.DataFrame | None:
    if (
        frame is None
        or frame.is_empty()
        or currency_field not in frame.columns
        or timestamp_field not in frame.columns
    ):
        return frame
    marks: dict[str, tuple[list[Any], list[float]]] = {}
    required_fx = {"timestamp", "currency", "reporting_currency", "rate"}
    if (
        fx_marks is not None
        and not fx_marks.is_empty()
        and required_fx.issubset(fx_marks.columns)
    ):
        for currency in fx_marks["currency"].drop_nulls().unique().to_list():
            values = fx_marks.filter(
                (pl.col("currency") == currency)
                & (pl.col("reporting_currency") == config.reporting_currency)
            ).sort("timestamp")
            marks[str(currency)] = (
                values["timestamp"].to_list(),
                values["rate"].cast(pl.Float64, strict=False).to_list(),
            )
    rates: list[float | None] = []
    for timestamp, currency in frame.select(timestamp_field, currency_field).iter_rows():
        currency = str(currency)
        if currency == config.reporting_currency:
            rates.append(1.0)
            continue
        timestamps, values = marks.get(currency, ([], []))
        index = bisect_right(timestamps, timestamp) - 1
        rates.append(values[index] if index >= 0 else None)
    value = frame.with_columns(
        pl.Series("fx_rate_to_reporting", rates, dtype=pl.Float64)
    )
    expressions = []
    for field in amount_fields:
        if field in value.columns:
            expressions.append(
                (pl.col(field) * pl.col("fx_rate_to_reporting")).alias(
                    f"{field}_reporting"
                )
            )
    return value.with_columns(expressions) if expressions else value


def bundle_from_portfolio(
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
) -> ReportBundle:
    portfolio = (
        portfolio_snapshots
        if isinstance(portfolio_snapshots, pl.DataFrame)
        else pl.DataFrame(portfolio_snapshots)
    )
    portfolio = normalize_portfolio_snapshots(portfolio, config)
    if metadata is None:
        run_metadata = RunMetadata.from_mapping({}, config)
    elif isinstance(metadata, RunMetadata):
        run_metadata = metadata
    else:
        run_metadata = RunMetadata.from_mapping(metadata, config)

    def normalize_optional(
        value: pl.DataFrame | None,
        *,
        naive_timezone: str | None = None,
        timestamp_fields: tuple[str, ...] = ("timestamp",),
    ) -> pl.DataFrame | None:
        if value is None:
            return None
        normalized = value.clone()
        for timestamp_field in timestamp_fields:
            normalized = ensure_timestamp(
                normalized,
                config.timestamp_unit,
                config.timezone,
                naive_timezone=naive_timezone,
                timestamp_field=timestamp_field,
            )
        return normalized

    benchmark_timezone: str | None = None
    if benchmark is not None and "timezone" in benchmark.columns:
        zones = benchmark["timezone"].drop_nulls().unique().to_list()
        if len(zones) == 1:
            benchmark_timezone = str(zones[0])

    normalized_fx = normalize_optional(fx_marks)
    normalized_accounts = normalize_optional(account_snapshots)
    normalized_positions = normalize_optional(position_snapshots)
    normalized_fills = normalize_optional(
        fill_events, timestamp_fields=("timestamp", "exchange_timestamp")
    )
    normalized_orders = normalize_optional(
        order_events, timestamp_fields=("timestamp", "exchange_timestamp")
    )
    normalized_round_trips = normalize_optional(
        round_trip_events,
        timestamp_fields=("entry_timestamp", "exit_timestamp"),
    )
    normalized_accounts = _apply_fx_marks(
        normalized_accounts,
        normalized_fx,
        config,
        currency_field="currency_id",
        amount_fields=("balance", "fee", "rebate", "funding", "realized_pnl", "unrealized_pnl", "margin"),
    )
    normalized_positions = _apply_fx_marks(
        normalized_positions,
        normalized_fx,
        config,
        currency_field="currency_id",
        amount_fields=("mark_price", "notional", "realized_pnl", "unrealized_pnl", "margin"),
    )
    normalized_fills = _apply_fx_marks(
        normalized_fills,
        normalized_fx,
        config,
        currency_field="currency",
        amount_fields=("price", "notional", "fee", "rebate"),
    )
    normalized_round_trips = _apply_fx_marks(
        normalized_round_trips,
        normalized_fx,
        config,
        currency_field="currency",
        amount_fields=("gross_pnl", "fee", "rebate", "funding", "net_pnl"),
        timestamp_field="exit_timestamp",
    )

    return ReportBundle(
        metadata=run_metadata,
        portfolio_snapshots=portfolio,
        account_snapshots=normalized_accounts,
        position_snapshots=normalized_positions,
        benchmark=normalize_optional(benchmark, naive_timezone=benchmark_timezone),
        fill_events=normalized_fills,
        order_events=normalized_orders,
        round_trip_events=normalized_round_trips,
        fx_marks=normalized_fx,
        risk_events=normalize_optional(risk_events),
        market_marks=normalize_optional(
            market_marks, timestamp_fields=("timestamp", "exchange_timestamp")
        ),
    )


def _split_cumulative_fee(raw: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    if len(raw) == 0:
        return raw.copy(), raw.copy()
    increments = np.diff(raw, prepend=0.0)
    fee = np.cumsum(np.clip(increments, 0.0, None))
    rebate = np.cumsum(np.clip(-increments, 0.0, None))
    return fee, rebate


def bundle_from_legacy_record(
    record: np.ndarray[Any, Any] | pl.DataFrame,
    config: ReportConfig,
    *,
    metadata: RunMetadata | Mapping[str, Any] | None = None,
) -> ReportBundle:
    """Convert one legacy linear/inverse asset recorder into a canonical bundle.

    Legacy records do not carry a portfolio currency ledger. This constructor is deliberately
    single-asset; callers with multiple assets must build authoritative portfolio snapshots rather
    than summing per-asset returns.
    """
    frame = record.clone() if isinstance(record, pl.DataFrame) else pl.DataFrame(record)
    required = {
        "timestamp",
        "price",
        "position",
        "balance",
        "fee",
        "num_trades",
        "trading_volume",
        "trading_value",
    }
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"legacy record is missing columns: {sorted(missing)}")
    if frame.is_empty():
        raise ValueError("legacy record must contain at least one row")

    timestamp = frame["timestamp"].to_numpy()
    price = frame["price"].cast(pl.Float64).to_numpy()
    position = frame["position"].cast(pl.Float64).to_numpy()
    balance = frame["balance"].cast(pl.Float64).to_numpy()
    raw_fee = frame["fee"].cast(pl.Float64).to_numpy()
    fee, rebate = _split_cumulative_fee(raw_fee)

    if config.asset_type == "linear":
        position_value = position * price * config.contract_size
        pnl_gross = balance + position_value
    else:
        with np.errstate(divide="ignore", invalid="ignore"):
            position_value = position / price * config.contract_size
            pnl_gross = -balance - position_value

    equity_gross = config.initial_capital + pnl_gross
    equity_net = equity_gross - fee + rebate
    gross_exposure = np.abs(position_value)
    net_exposure = position_value
    leverage = gross_exposure / config.initial_capital

    portfolio = pl.DataFrame(
        {
            "timestamp": timestamp,
            "timestamp_kind": ["local_delivery"] * len(frame),
            "equity_gross": equity_gross,
            "equity_net": equity_net,
            "cash": config.initial_capital + balance,
            "realized_pnl": [None] * len(frame),
            "unrealized_pnl": [None] * len(frame),
            "fee": fee,
            "rebate": rebate,
            "funding": np.zeros(len(frame)),
            "external_flow": np.zeros(len(frame)),
            "gross_exposure": gross_exposure,
            "net_exposure": net_exposure,
            "margin": [None] * len(frame),
            "leverage": leverage,
            "num_trades": frame["num_trades"].to_numpy(),
            "trading_volume": frame["trading_volume"].to_numpy(),
            "trading_value": frame["trading_value"].to_numpy(),
            "reporting_currency": [config.reporting_currency] * len(frame),
        }
    )
    portfolio = ensure_timestamp(portfolio, config.timestamp_unit, config.timezone)

    account = portfolio.select(
        "timestamp",
        pl.lit("local_delivered").alias("view_kind"),
        pl.lit(config.venue_id).alias("venue_id"),
        pl.lit(config.reporting_currency).alias("currency_id"),
        pl.col("cash").alias("balance"),
        "fee",
        "rebate",
        "funding",
        "realized_pnl",
        "unrealized_pnl",
        "margin",
    )
    positions = pl.DataFrame(
        {
            "timestamp": portfolio["timestamp"],
            "view_kind": ["local_delivered"] * len(frame),
            "venue_id": [config.venue_id] * len(frame),
            "instrument_id": [config.instrument_id] * len(frame),
            "currency_id": [config.reporting_currency] * len(frame),
            "quantity": position,
            "mark_price": price,
            "notional": position_value,
            "realized_pnl": [None] * len(frame),
            "unrealized_pnl": [None] * len(frame),
            "margin": [None] * len(frame),
        }
    )

    legacy_warning = (
        "constructed from a legacy single-asset recorder; realized/unrealized PnL and "
        "multi-currency reconciliation are unavailable"
    )
    if metadata is None:
        metadata_value: Mapping[str, Any] = {"warnings": (legacy_warning,)}
    elif isinstance(metadata, RunMetadata):
        metadata_value = metadata.to_dict() | {
            "warnings": tuple(metadata.warnings) + (legacy_warning,)
        }
    else:
        metadata_value = dict(metadata) | {
            "warnings": tuple(metadata.get("warnings", ())) + (legacy_warning,)
        }
    run_metadata = RunMetadata.from_mapping(metadata_value, config)
    return ReportBundle(
        metadata=run_metadata,
        portfolio_snapshots=portfolio,
        account_snapshots=account,
        position_snapshots=positions,
    )
