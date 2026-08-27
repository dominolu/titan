#!/usr/bin/env python3
"""Measure Binance Futures WS-ingress -> Numba ``on_tick(s)`` latency.

The hot path is the production strategy ABI documented in
``docs/bar_tick_numba_strategy.md``: Rust owns the live event loop and calls a single-argument
Numba callback without returning to the Python interpreter. Python only configures the run and
summarizes the preallocated sample buffer after ``on_stop``.
"""

import argparse
import json

import numpy as np
from numba import njit

from hftbacktest import HashMapMarketDepthLiveBot, LiveInstrument, run_event_bot


START_NS = 0
LAST_WS_RECV_NS = 1
SAMPLE_COUNT = 2
CLOCK_ANOMALIES = 3
CALLBACK_COUNT = 4
WARMUP_NS = 5
STOP_AFTER_NS = 6
SAMPLE_CAPACITY = 7
HEADER_LEN = 8


@njit
def on_tick(s):
    """Record one latency per distinct WS-ingress timestamp, entirely in Numba."""
    s.state_i64[CALLBACK_COUNT] += 1
    callback_ts = s.now
    if s.state_i64[START_NS] == 0:
        s.state_i64[START_NS] = callback_ts

    ticks = s.ticks()
    for index in range(s.num_ticks):
        ws_recv_ts = ticks[index]["event"]["local_ts"]
        if ws_recv_ts <= 0 or ws_recv_ts == s.state_i64[LAST_WS_RECV_NS]:
            continue
        s.state_i64[LAST_WS_RECV_NS] = ws_recv_ts

        if callback_ts - s.state_i64[START_NS] < s.state_i64[WARMUP_NS]:
            continue
        latency = callback_ts - ws_recv_ts
        if latency < 0:
            s.state_i64[CLOCK_ANOMALIES] += 1
            continue

        sample_index = s.state_i64[SAMPLE_COUNT]
        if sample_index >= s.state_i64[SAMPLE_CAPACITY]:
            s.stop()
            return
        s.state_i64[HEADER_LEN + sample_index] = latency
        s.state_i64[SAMPLE_COUNT] = sample_index + 1

    if callback_ts - s.state_i64[START_NS] >= s.state_i64[STOP_AFTER_NS]:
        s.stop()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Measure Binance Futures WS ingress to Numba on_tick latency"
    )
    parser.add_argument("--connector-name", default="binance-latency")
    parser.add_argument("--symbol", default="btcusdt")
    parser.add_argument("--tick-size", type=float, default=0.1)
    parser.add_argument("--lot-size", type=float, default=0.001)
    parser.add_argument("--frame-us", type=int, default=1_000)
    parser.add_argument("--warmup-seconds", type=int, default=5)
    parser.add_argument("--measure-seconds", type=int, default=60)
    parser.add_argument("--max-samples", type=int, default=1_000_000)
    return parser.parse_args()


def main():
    args = parse_args()
    if args.frame_us <= 0:
        raise ValueError("--frame-us must be greater than zero")
    if args.warmup_seconds < 0 or args.measure_seconds <= 0:
        raise ValueError("warm-up must be non-negative and measurement must be positive")
    if args.max_samples <= 0:
        raise ValueError("--max-samples must be greater than zero")

    state_i64 = np.zeros(HEADER_LEN + args.max_samples, dtype=np.int64)
    state_i64[WARMUP_NS] = args.warmup_seconds * 1_000_000_000
    state_i64[STOP_AFTER_NS] = (
        args.warmup_seconds + args.measure_seconds
    ) * 1_000_000_000
    state_i64[SAMPLE_CAPACITY] = args.max_samples

    instrument = (
        LiveInstrument()
        .connector(args.connector_name)
        .symbol(args.symbol)
        .tick_size(args.tick_size)
        .lot_size(args.lot_size)
        .last_trades_capacity(0)
    )
    hbt = HashMapMarketDepthLiveBot([instrument])
    try:
        run_event_bot(
            hbt=hbt,
            data_mode="tick",
            on_tick=on_tick,
            frame_interval=args.frame_us * 1_000,
            max_tick_batch=65_536,
            state_i64=state_i64,
        )
    finally:
        hbt.close()

    count = int(state_i64[SAMPLE_COUNT])
    if count == 0:
        raise RuntimeError(
            "no Binance WS pushes reached on_tick during the measurement window; "
            "check connector logs and symbol"
        )
    samples_us = state_i64[HEADER_LEN : HEADER_LEN + count].astype(np.float64) / 1_000.0
    result = {
        "connector": "binancefutures",
        "strategy_api": "numba_on_tick",
        "symbol": args.symbol,
        "samples": count,
        "callbacks": int(state_i64[CALLBACK_COUNT]),
        "frame_us": args.frame_us,
        "warmup_seconds": args.warmup_seconds,
        "measure_seconds": args.measure_seconds,
        "min_us": round(float(np.min(samples_us)), 3),
        "p50_us": round(float(np.percentile(samples_us, 50)), 3),
        "p90_us": round(float(np.percentile(samples_us, 90)), 3),
        "p99_us": round(float(np.percentile(samples_us, 99)), 3),
        "p99_9_us": round(float(np.percentile(samples_us, 99.9)), 3),
        "max_us": round(float(np.max(samples_us)), 3),
        "mean_us": round(float(np.mean(samples_us)), 3),
        "stddev_us": round(float(np.std(samples_us)), 3),
        "clock_anomalies": int(state_i64[CLOCK_ANOMALIES]),
    }
    print(
        " ".join(
            f"{key}={value}"
            for key, value in result.items()
            if key not in ("connector", "strategy_api", "symbol")
        )
    )
    print(f"RESULT_JSON={json.dumps(result, separators=(',', ':'))}")


if __name__ == "__main__":
    main()
