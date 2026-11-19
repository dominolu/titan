"""Liquidity-mapper style statistics over Titan recordings (Rust fast path, NumPy fallback).

The rules follow the reference project (``liquiditymapperfast``, read 2026-11-17); the
exact constants and quotations live in ``lm_common``, which also reads trades and aux
directly and is what the NumPy side uses for its tests. Two phases:

1. **extract** (Rust): trades and the perpetual archive's aux stream become flat
   columns; the book segments become a per-segment survey of the 100 ms slot grid on a
   coarse price grid: crossing quotes, mid, imbalance within bands, best-5/10/20/50
   depth, the largest resting level (with its distance), resting size by distance in
   basis points, and the resting size available in front of each fill inside the
   absorption window. The survey is written as shards; raw decoded books are removed.
2. **detect** (Rust): the reference absorption rules per instrument and side (rolling
   credit of every fill, candidate groups, a per-minute cap) and the liquidation
   groups, written as shards.

Python then aggregates shards into hourly panels and event tables.

Nothing here is a Titan strategy or platform component; it is offline research tooling
for one recording set (see README.md next to this file for the case-specific facts).
"""

from __future__ import annotations

import glob
import json
import os
import shutil
import subprocess
import sys

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
LMSTAT_DIR = os.path.join(HERE, "lmstat")
DEFAULT_BIN = os.path.join(LMSTAT_DIR, "target", "release", "lmstat")

# Coarse grid of the book survey: 5 USD a bin on BTC around 64,000 is 0.8 bp.
BIN_USD = 5.0
# Imbalance bands, in basis points from the touch.
IMB_BANDS_BP = (1.0, 2.0, 5.0, 10.0, 20.0, 50.0)
# Depth in front of a fill, best N levels of the side being consumed.
FRONT_DEPTHS = (1, 3, 5, 10)


def lmstat_bin(explicit: str | None = None) -> str:
    path = explicit or os.environ.get("LMSTAT_BIN") or DEFAULT_BIN
    if not os.path.exists(path):
        raise SystemExit(
            f"lmstat binary not found at {path}; build it on the compile server "
            f"(scripts/verify.sh cargo build --release --manifest-path {LMSTAT_DIR}/Cargo.toml)"
        )
    return path


def _run(args: list[str]) -> None:
    proc = subprocess.run(args, capture_output=True, text=True)
    if proc.returncode != 0:
        sys.stderr.write(proc.stderr[-4000:])
        raise SystemExit(f"lmstat failed: {' '.join(args)}")


# --------------------------------------------------------------------------------------
# phase 1


def extract(zip_path: str, work_dir: str, binary: str | None = None, threads: int = 0) -> dict:
    """Extracts trades, aux and the book survey of one archive; returns what was written."""
    binary = lmstat_bin(binary)
    os.makedirs(work_dir, exist_ok=True)
    run_shard = os.path.join(work_dir, "survey")
    shutil.rmtree(run_shard, ignore_errors=True)
    books = os.path.join(work_dir, "books")
    args = [
        "extract",
        "--zip",
        zip_path,
        "--book-work",
        books,
        "--survey",
        run_shard,
        "--trades",
        os.path.join(work_dir, "trades"),
        "--aux",
        os.path.join(work_dir, "aux"),
        "--bin-usd",
        str(BIN_USD),
        "--imb-bands-bp",
        ",".join(f"{b:g}" for b in IMB_BANDS_BP),
        "--front-depths",
        ",".join(str(n) for n in FRONT_DEPTHS),
    ]
    if threads:
        args += ["--threads", str(threads)]
    _run([binary] + args)
    # The decoded books are at least 100x the survey; nothing later needs them.
    shutil.rmtree(books, ignore_errors=True)
    return {"survey": run_shard, "trades": os.path.join(work_dir, "trades"), "aux": os.path.join(work_dir, "aux")}


def detect(work: dict, binary: str | None = None) -> dict:
    """Runs the absorption and liquidation detectors over the extracted trades/aux."""
    binary = lmstat_bin(binary)
    out = os.path.join(os.path.dirname(work["trades"]), "detect")
    _run(
        [
            binary,
            "detect",
            "--trades",
            work["trades"],
            "--aux",
            work["aux"],
            "--out",
            out,
            "--window-ms",
            "10",
            "--floor-usd",
            "25000",
            "--per-minute",
            "10",
            "--spot-instrument",
            "binance_spot_BTCUSDT",
            "--perp-instrument",
            "binance_perpetual_BTCUSDT",
        ]
    )
    return {"dir": out}


# --------------------------------------------------------------------------------------
# reading shards


def read_index(shard_dir: str, kind: str) -> list[dict]:
    """The shards of one kind: ``shard.json`` lists their metadata; each column is
    ``<kind>_<instrument>_<hh>_<name>.{f32|f64|i64|i32|u32}``.

    A column recorded as ``"<n>_cols"`` holds ``n`` float32 values per row.
    """
    out = []
    for meta_path in sorted(glob.glob(os.path.join(shard_dir, f"{kind}_*.json"))):
        with open(meta_path) as fh:
            meta = json.load(fh)
        if "instrument" not in meta:
            continue
        base = os.path.join(shard_dir, f"{kind}_{meta['instrument']}_{meta['hour']:02d}_")
        rows = {}
        for name, spec in meta.get("columns", {}).items():
            if isinstance(spec, str) and spec.endswith("_cols"):
                stride = int(spec.split("_")[0])
                rows[name] = np.fromfile(base + name + ".f32", dtype=np.float32).reshape(-1, stride)
            else:
                dtype = {"f64": np.float64, "f32": np.float32, "i64": np.int64, "i32": np.int32, "u32": np.uint32}[spec]
                rows[name] = np.fromfile(base + name + "." + spec, dtype=dtype)
        out.append({"meta": meta, "rows": rows})
    return out
