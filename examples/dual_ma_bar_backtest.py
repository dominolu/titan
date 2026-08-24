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


def load_bars(path, source, timeframe_ns, include_unfinalized=False):
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
    ).filter(pl.col("source") == source)
    if not include_unfinalized:
        frame = frame.filter(pl.col("is_final"))
    frame = frame.sort("ts")
    if frame.height == 0:
        raise ValueError(f"no Bar rows matched source={source!r}")
    if frame.select(pl.col("ts").is_duplicated().any()).item():
        raise ValueError(f"duplicate Bar timestamps for source={source!r}")

    open_ts = frame["ts"].cast(pl.Int64).to_numpy() * 1_000
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
    parser.add_argument(
        "--close-positions-on-stop",
        action=argparse.BooleanOptionalAction,
        default=False,
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
            close_positions_on_stop=args.close_positions_on_stop,
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
