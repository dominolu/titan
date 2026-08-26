"""Export one Titan dual-MA Bar backtest as auditable CSV execution records."""

import argparse
import json
from pathlib import Path

import numpy as np
import polars as pl
from numba import njit

from dual_ma_bar_backtest import load_bars
from hftbacktest.eventbot import run_event_bot
from hftbacktest.strategies import create_dual_ma_strategy


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default="data/AAPL_1m_all_sources.parquet")
    parser.add_argument("--source", default="polygon_s3")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--short-period", type=int, default=20)
    parser.add_argument("--long-period", type=int, default=50)
    parser.add_argument("--quantity", type=float, default=100.0)
    parser.add_argument("--initial-cash", type=float, default=1_000_000.0)
    parser.add_argument("--timeframe-ns", type=int, default=60_000_000_000)
    parser.add_argument(
        "--bar-matching",
        choices=("next_open", "signal_close", "touch", "conservative_ohlc"),
        default="next_open",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    bars = load_bars(args.data, args.source, args.timeframe_ns)
    strategy = create_dual_ma_strategy(
        closes=bars["bar"]["close"],
        short_period=args.short_period,
        long_period=args.long_period,
        timeframe_ns=args.timeframe_ns,
        quantity=args.quantity,
    )

    capacity = len(bars) + 1
    state_f64_prefix = 8
    state_i64_prefix = 9
    fill_count_index = 8
    runtime_state = np.zeros(state_f64_prefix + capacity * 2, dtype=np.float64)
    runtime_state_i64 = np.zeros(state_i64_prefix + capacity * 8, dtype=np.int64)
    runtime_state_i64[0] = 1
    asset_no = strategy.asset_no

    @njit
    def on_filled(s):
        for fill in s.fills():
            if fill["asset_no"] != asset_no:
                continue
            index = s.state_i64[fill_count_index]
            if index >= capacity:
                s.stop()
                return
            i64_base = state_i64_prefix + index * 8
            f64_base = state_f64_prefix + index * 2
            s.state_i64[i64_base] = fill["order_id"]
            s.state_i64[i64_base + 1] = fill["venue_order_id"]
            s.state_i64[i64_base + 2] = fill["exch_ts"]
            s.state_i64[i64_base + 3] = fill["local_ts"]
            s.state_i64[i64_base + 4] = fill["sequence"]
            s.state_i64[i64_base + 5] = fill["side"]
            s.state_i64[i64_base + 6] = fill["maker"]
            s.state_i64[i64_base + 7] = fill["instrument_id"]
            s.state[f64_base] = fill["qty"]
            s.state[f64_base + 1] = fill["price"]
            s.state_i64[fill_count_index] = index + 1
            s.state[4] += fill["qty"]
            s.state[5] = fill["price"]

    result = run_event_bot(
        data_mode="bar",
        bars=bars,
        history_capacity=args.long_period + 1,
        on_bar=strategy.on_bar,
        on_filled=on_filled,
        on_stop=strategy.on_stop,
        state=runtime_state,
        state_i64=runtime_state_i64,
        bar_matching=args.bar_matching,
        return_result=True,
    )

    count = int(runtime_state_i64[fill_count_index])
    fill_i64 = runtime_state_i64[state_i64_prefix : state_i64_prefix + count * 8].reshape(
        count, 8
    )
    fill_f64 = runtime_state[state_f64_prefix : state_f64_prefix + count * 2].reshape(
        count, 2
    )
    sides = fill_i64[:, 5]
    quantities = fill_f64[:, 0]
    prices = fill_f64[:, 1]
    fee_by_fill = {
        (report["order_id"], report["sequence"]): report["account_delta"]["fee"]
        for report in result.execution_reports
        if report["kind"] == "fill" and report["account_delta"] is not None
    }
    commissions = np.asarray(
        [
            fee_by_fill[(int(order_id) & ((1 << 64) - 1), int(sequence))]
            for order_id, sequence in zip(fill_i64[:, 0], fill_i64[:, 4])
        ],
        dtype=np.float64,
    )
    cash_deltas = -sides * quantities * prices - commissions
    positions = np.cumsum(sides * quantities)
    cash = args.initial_cash + np.cumsum(cash_deltas)
    ts_event = pl.Series(fill_i64[:, 2]).cast(pl.Datetime("ns", "UTC"))

    fills = pl.DataFrame(
        {
            "fill_index": np.arange(1, count + 1),
            "client_order_id": fill_i64[:, 0],
            "venue_order_id": fill_i64[:, 1],
            "instrument_id": ["AAPL.XNAS"] * count,
            "order_side": np.where(sides == 1, "BUY", "SELL"),
            "order_type": ["MARKET"] * count,
            "last_qty": quantities,
            "last_px": prices,
            "currency": ["USD"] * count,
            "commission": commissions,
            "liquidity_side": np.where(fill_i64[:, 6] == 1, "MAKER", "TAKER"),
            "ts_event": ts_event,
            "ts_init": pl.Series(fill_i64[:, 3]).cast(pl.Datetime("ns", "UTC")),
            "sequence": fill_i64[:, 4],
            "cash_delta": cash_deltas,
            "position_after": positions,
            "cash_after": cash,
        }
    )
    fills.write_csv(output_dir / "fills.csv", datetime_format="%Y-%m-%d %H:%M:%S%:z")

    initial_ts = int(bars[0]["bar"]["close_ts"])
    account_states = pl.DataFrame(
        {
            "ts_event": pl.concat(
                [
                    pl.Series([initial_ts]).cast(pl.Datetime("ns", "UTC")),
                    ts_event,
                ]
            ),
            "total": np.concatenate(([args.initial_cash], cash)),
            "locked": np.zeros(count + 1),
            "free": np.concatenate(([args.initial_cash], cash)),
            "currency": ["USD"] * (count + 1),
            "account_id": ["TITAN-001"] * (count + 1),
            "account_type": ["CASH"] * (count + 1),
            "base_currency": ["USD"] * (count + 1),
            "position": np.concatenate(([0.0], positions)),
            "cash_delta": np.concatenate(([0.0], cash_deltas)),
            "commission": np.concatenate(([0.0], commissions)),
            "source_order_id": np.concatenate(([0], fill_i64[:, 0])),
        }
    )
    account_states.write_csv(
        output_dir / "account_states.csv", datetime_format="%Y-%m-%d %H:%M:%S%:z"
    )

    commission_records = pl.DataFrame(
        {
            "fill_index": np.arange(1, count + 1),
            "client_order_id": fill_i64[:, 0],
            "ts_event": ts_event,
            "instrument_id": ["AAPL.XNAS"] * count,
            "order_side": np.where(sides == 1, "BUY", "SELL"),
            "last_qty": quantities,
            "last_px": prices,
            "liquidity_side": np.where(fill_i64[:, 6] == 1, "MAKER", "TAKER"),
            "commission_amount": commissions,
            "commission_currency": ["USD"] * count,
            "cumulative_commission": np.cumsum(commissions),
        }
    )
    commission_records.write_csv(
        output_dir / "commissions.csv", datetime_format="%Y-%m-%d %H:%M:%S%:z"
    )

    summary = {
        "engine": "Titan MaterializedBarSource shared execution runtime",
        "data": args.data,
        "source": args.source,
        "bar_matching": args.bar_matching,
        "terminal_flatten": "last_executable_bar_close",
        "bars": len(bars),
        "fills": count,
        "account_states": count + 1,
        "commission_records": count,
        "initial_cash": args.initial_cash,
        "ending_cash": float(cash[-1]) if count else args.initial_cash,
        "final_position": float(positions[-1]) if count else 0.0,
        "total_commission": float(commissions.sum()),
        "golden_crosses": int(runtime_state[2]),
        "death_crosses": int(runtime_state[3]),
        "buy_orders": int(runtime_state_i64[2]),
        "sell_orders": int(runtime_state_i64[3]),
    }
    (output_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print("TITAN_DUAL_MA_EXPORT=" + json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main()
