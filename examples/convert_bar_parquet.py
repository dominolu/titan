"""Converts normalized Bar Parquet into the flat NPY format consumed by Rust."""

import argparse
import json
from pathlib import Path

import numpy as np

from dual_ma_bar_backtest import load_bars


flat_timed_bar_dtype = np.dtype(
    [
        ("asset_no", "u8"),
        ("timeframe_ns", "i8"),
        ("open_ts", "i8"),
        ("close_ts", "i8"),
        ("open", "f8"),
        ("high", "f8"),
        ("low", "f8"),
        ("close", "f8"),
        ("volume", "f8"),
        ("quote_volume", "f8"),
        ("buy_volume", "f8"),
        ("trade_count", "u8"),
        ("flags", "u8"),
    ]
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument("--timeframe-ns", type=int, default=60_000_000_000)
    parser.add_argument("--include-unfinalized", action="store_true")
    args = parser.parse_args()

    bars = load_bars(
        args.input, args.source, args.timeframe_ns, args.include_unfinalized
    )
    rows = np.empty(len(bars), dtype=flat_timed_bar_dtype)
    rows["asset_no"] = bars["asset_no"]
    rows["timeframe_ns"] = bars["timeframe_ns"]
    for field in (
        "open_ts",
        "close_ts",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_volume",
        "buy_volume",
        "trade_count",
        "flags",
    ):
        rows[field] = bars["bar"][field]

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    np.save(output, rows, allow_pickle=False)
    manifest = {
        "schema_version": 1,
        "data_kind": "bar",
        "symbol": "AAPL",
        "venue": args.source,
        "timestamp_unit": "ns",
        "interval_semantics": "[open, close)",
        "timeframe_ns": args.timeframe_ns,
        "bar_source": "venue-native",
        "rows": len(rows),
        "dtype_itemsize": rows.dtype.itemsize,
        "input": str(Path(args.input)),
    }
    output.with_suffix(".manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(
        f"converted rows={len(rows)} bytes={output.stat().st_size} "
        f"output={output}"
    )


if __name__ == "__main__":
    main()
