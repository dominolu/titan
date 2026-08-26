"""Runs the single-argument Numba dual-MA strategy on canonical Bar Parquet."""

import argparse
import time

import numpy as np
import polars as pl

from hftbacktest.eventbot import (
    BAR_COMPLETE,
    BAR_NATIVE,
    run_event_bot,
    timed_bar_dtype,
)
from hftbacktest.strategies import create_dual_ma_strategy


AUTO_SOURCE_PRIORITY = ("polygon_s3", "databento", "s3")
EXPECTED_US_EQUITY_MINUTE_COUNTS = (211, 390)


def _select_daily_sources(frame, source_priority):
    """Select one provider per trading date and reject incomplete merged coverage."""
    priorities = {source: rank for rank, source in enumerate(source_priority)}
    candidates = (
        frame.with_columns(pl.col("ts").dt.date().alias("_session"))
        .group_by("_session", "source")
        .agg(
            pl.len().alias("bars"),
            pl.col("ts").n_unique().alias("unique_timestamps"),
            pl.col("ts").min().alias("start"),
            pl.col("ts").max().alias("end"),
        )
        .with_columns(
            pl.col("source")
            .replace_strict(
                priorities,
                default=len(priorities),
                return_dtype=pl.Int32,
            )
            .alias("priority")
        )
    )
    duplicates = candidates.filter(pl.col("bars") != pl.col("unique_timestamps"))
    if not duplicates.is_empty():
        raise ValueError(
            "duplicate Bar timestamps within a source/session: "
            f"{duplicates.select('_session', 'source').to_dicts()}"
        )
    selected = (
        candidates.sort("_session", "priority", "source")
        .unique("_session", keep="first", maintain_order=True)
        .sort("_session")
    )
    unexpected_counts = selected.filter(
        ~pl.col("bars").is_in(EXPECTED_US_EQUITY_MINUTE_COUNTS)
    )
    if not unexpected_counts.is_empty():
        raise ValueError(
            "auto-merged US equity data contains incomplete sessions: "
            f"{unexpected_counts.select('_session', 'source', 'bars').to_dicts()[:10]}"
        )
    selected = selected.with_columns(
        (pl.col("_session") - pl.col("_session").shift(1))
        .dt.total_days()
        .alias("calendar_gap_days")
    )
    long_gaps = selected.filter(pl.col("calendar_gap_days") > 4)
    if not long_gaps.is_empty():
        raise ValueError(
            "auto-merged US equity data contains unexpected calendar gaps: "
            f"{long_gaps.select('_session', 'calendar_gap_days').to_dicts()[:10]}"
        )
    selection = selected.select("_session", "source")
    merged = (
        frame.with_columns(pl.col("ts").dt.date().alias("_session"))
        .join(selection, on=["_session", "source"], how="inner")
        .drop("_session")
        .sort("ts")
    )
    coverage = (
        selected.group_by("source")
        .agg(
            pl.len().alias("sessions"),
            pl.col("bars").sum().alias("bars"),
            pl.col("_session").min().alias("first_session"),
            pl.col("_session").max().alias("last_session"),
        )
        .sort("source")
    )
    audit = {
        "mode": "auto_daily_source",
        "source_priority": list(source_priority),
        "sessions": selected.height,
        "bars": merged.height,
        "maximum_calendar_gap_days": int(
            selected["calendar_gap_days"].drop_nulls().max() or 0
        ),
        "daily_bar_counts": {
            str(row[0]): row[1]
            for row in selected.group_by("bars").len().sort("bars").iter_rows()
        },
        "selected_source_coverage": [
            {
                **row,
                "first_session": str(row["first_session"]),
                "last_session": str(row["last_session"]),
            }
            for row in coverage.to_dicts()
        ],
    }
    return merged, audit


def _normalize_auto_us_equity_timestamps(frame, timezone):
    sessions = (
        frame.with_columns(pl.col("ts").dt.date().alias("_session"))
        .group_by("_session")
        .agg(pl.col("ts").min().alias("session_start"))
        .with_columns(pl.col("session_start").dt.hour().alias("start_hour"))
    )
    invalid = sessions.filter(~pl.col("start_hour").is_in((9, 13, 14)))
    if not invalid.is_empty():
        raise ValueError(
            "cannot infer US equity timestamp encoding for sessions: "
            f"{invalid.select('_session', 'session_start').to_dicts()[:10]}"
        )
    sessions = sessions.with_columns(
        (pl.col("start_hour") == 9).alias("wall_clock_encoded")
    )
    value = frame.with_columns(pl.col("ts").dt.date().alias("_session")).join(
        sessions.select("_session", "wall_clock_encoded"),
        on="_session",
        how="left",
    )
    localized = (
        pl.col("ts")
        .dt.replace_time_zone(None)
        .dt.replace_time_zone(timezone)
        .dt.convert_time_zone("UTC")
    )
    value = value.with_columns(
        pl.when(pl.col("wall_clock_encoded"))
        .then(localized)
        .otherwise(pl.col("ts"))
        .alias("_normalized_ts")
    )
    audit = {
        "mode": "auto_us_equity",
        "wall_clock_timezone": timezone,
        "wall_clock_encoded_sessions": sessions.filter(
            pl.col("wall_clock_encoded")
        ).height,
        "utc_encoded_sessions": sessions.filter(
            ~pl.col("wall_clock_encoded")
        ).height,
    }
    return value, audit


def load_bars(
    path,
    source,
    timeframe_ns,
    include_unfinalized=False,
    *,
    return_audit=False,
    source_priority=AUTO_SOURCE_PRIORITY,
    wall_clock_timezone=None,
):
    frame = pl.read_parquet(
        path,
        columns=[
            "ts",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "vwap",
            "transaction_count",
            "source",
            "is_final",
        ],
    )
    if not include_unfinalized:
        frame = frame.filter(pl.col("is_final"))
    if source == "auto":
        frame, audit = _select_daily_sources(frame, source_priority)
    else:
        frame = frame.filter(pl.col("source") == source)
        audit = {
            "mode": "single_source",
            "source": source,
            "bars": frame.height,
        }
    frame = frame.sort("ts")
    if frame.height == 0:
        raise ValueError(f"no Bar rows matched source={source!r}")
    if frame.select(pl.col("ts").is_duplicated().any()).item():
        raise ValueError(f"duplicate Bar timestamps for source={source!r}")

    timestamp = pl.col("ts")
    if wall_clock_timezone == "auto":
        frame, timestamp_audit = _normalize_auto_us_equity_timestamps(
            frame, "America/New_York"
        )
        timestamp_field = "_normalized_ts"
        audit["timestamp_normalization"] = timestamp_audit
    elif wall_clock_timezone is not None:
        timestamp = (
            timestamp.dt.replace_time_zone(None)
            .dt.replace_time_zone(wall_clock_timezone)
            .dt.convert_time_zone("UTC")
        )
        frame = frame.with_columns(timestamp.alias("_normalized_ts"))
        timestamp_field = "_normalized_ts"
        audit["timestamp_normalization"] = {
            "mode": "fixed_wall_clock",
            "wall_clock_timezone": wall_clock_timezone,
        }
    else:
        timestamp_field = "ts"
    open_ts = frame[timestamp_field].cast(pl.Int64).to_numpy() * 1_000
    bars = np.zeros(frame.height, dtype=timed_bar_dtype)
    bars["asset_no"] = 0
    bars["timeframe_ns"] = timeframe_ns
    bars["bar"]["open_ts"] = open_ts
    bars["bar"]["close_ts"] = open_ts + timeframe_ns
    for field in ("open", "high", "low", "close", "volume"):
        bars["bar"][field] = frame[field].to_numpy()
    vwap = frame["vwap"].fill_null(0.0).to_numpy()
    bars["bar"]["quote_volume"] = vwap * bars["bar"]["volume"]
    bars["bar"]["buy_volume"] = 0.0
    bars["bar"]["trade_count"] = (
        frame["transaction_count"].fill_null(0).clip(0, None).to_numpy()
    )
    bars["bar"]["flags"] = BAR_COMPLETE | BAR_NATIVE
    if return_audit:
        return bars, audit
    return bars


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data", default="data/AAPL_1m_all_sources.parquet", help="Bar Parquet path"
    )
    parser.add_argument("--source", default="polygon_s3")
    parser.add_argument("--short-period", type=int, default=20)
    parser.add_argument("--long-period", type=int, default=50)
    parser.add_argument("--quantity", type=float, default=1.0)
    parser.add_argument("--timeframe-ns", type=int, default=60_000_000_000)
    parser.add_argument("--include-unfinalized", action="store_true")
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument(
        "--bar-matching",
        choices=("next_open", "signal_close", "touch", "conservative_ohlc"),
        default="next_open",
    )
    args = parser.parse_args()

    bars = load_bars(
        args.data, args.source, args.timeframe_ns, args.include_unfinalized
    )
    strategy = create_dual_ma_strategy(
        closes=bars["bar"]["close"],
        short_period=args.short_period,
        long_period=args.long_period,
        timeframe_ns=args.timeframe_ns,
        quantity=args.quantity,
    )
    if args.runs <= 0:
        raise ValueError("runs must be positive")
    baseline = None
    durations = []
    state = strategy.state
    for _ in range(args.runs):
        strategy.state.fill(0.0)
        strategy.state_i64.fill(0)
        strategy.state_i64[0] = 1
        started = time.perf_counter_ns()
        state = run_event_bot(
            data_mode="bar",
            bars=bars,
            history_capacity=args.long_period + 1,
            on_bar=strategy.on_bar,
            on_filled=strategy.on_filled,
            on_stop=strategy.on_stop,
            state=strategy.state,
            state_i64=strategy.state_i64,
            bar_matching=args.bar_matching,
        )
        durations.append(time.perf_counter_ns() - started)
        core = (tuple(state), tuple(strategy.state_i64))
        if baseline is None:
            baseline = core
        elif core != baseline:
            raise RuntimeError("dual-MA result changed between repeated runs")
    print(
        f"bars={len(bars)} short_ma={state[0]:.6f} long_ma={state[1]:.6f} "
        f"golden_crosses={int(state[2])} death_crosses={int(state[3])} "
        f"filled_qty={state[4]:.4f} final_position={state[6]:.4f} "
        f"buy_orders={strategy.state_i64[2]} sell_orders={strategy.state_i64[3]} "
        f"submit_errors={strategy.state_i64[1]}"
    )
    durations.sort()
    median = durations[len(durations) // 2] / 1_000_000
    print(
        f"reproducibility_runs={args.runs} identical=true "
        f"median_ms={median:.3f} total_seconds={sum(durations) / 1_000_000_000:.6f}"
    )


if __name__ == "__main__":
    main()
