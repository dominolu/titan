"""Shared helpers for the liquidity-mapper style studies of Titan recordings.

Reference rules (liquiditymapperfast/liquiditymapperfast.github.io, read 2026-11-17):

* Absorption (``src/shared/absorption.ts``): per instrument and aggressor side, every
  fill looks back over the fills of its side at its price within ``ABSORPTION_WINDOW_MS``
  of its own time (itself included) and sums their USD; each fill in that set keeps the
  largest window it ever belonged to. A fill is marked when that credit reaches the
  threshold, which is a fixed USD amount or ``mean + k * sd`` of the *start-anchored*
  window sums of the recent minutes (a window opens at a fill and takes the fills up to
  its opening + the window; the next fill after that opens the next one).
* Large market orders (``src/shared/orders.ts``): fills are rebuilt into orders by the
  venue's order key, else by the exchange millisecond; size buckets are
  ``SIZE_EDGES = [0, 25K, 50K, 100K, 250K, 500K, 1M, 5M]`` USD; "whales" start at 100K.
* Liquidations (``src/shared/liquidations.ts``): Binance reports at most one per market
  per second (the latest of that second), so a cascade is undercounted; each is drawn at
  the price the market was at; the smallest kept is 100 USD.
* Depth imbalance (``src/app/lt.ts``, ``src/shared/lt-sample.ts``): a level ``d`` basis
  points from its own side's touch weighs ``2 ** (-d / half_life)``, the default half-life
  being 10 bp.

Recording facts this module relies on (``docs/market_data_recording_design.md``):

* The book window keeps the top 100 levels per side; deletions of levels pushed out by
  better ones follow from the window rule. The window includes crossing levels.
* Blocks of 60 s each carry a snapshot plus 100 ms-grid net differences; a slot's record
  is the book state at the slot's close. The grid is anchored to UTC wall clock, so the
  slot index of a block equals the wall-clock hour of its start (the recorder's block base
  time); ``slot_base`` recovers that hour even when the local clock lags the exchange by
  seconds.
* Trade and aux files are named by the UTC wall clock too, with no local-lag slack.

The data kinds are 2026-11 synthetic data (Binance-shaped, liquidations mostly at the
support around 64,000-67,000 on 2026-11-14/15), so the studies here are analyses of the
local recording set, not of live BTC history.
"""

from __future__ import annotations

import datetime as _dt
import gzip
import io
import json
import os
import struct
import zipfile
from dataclasses import dataclass
from typing import Iterable, Iterator

import numpy as np

# --------------------------------------------------------------------------------------
# constants from the reference project

ABSORPTION_WINDOW_MS = 10
GROUP_FLOOR_USD = 25_000
GROUPS_PER_MINUTE = 10
LATE_MS = 250
ACCEPT_MS = 2 * 3_600_000
AUTO_K = 10
AUTO_SD_MINUTES = 30
FIXED_THRESHOLD_USD = 250_000
SIZE_EDGES = np.array([0, 25_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 5_000_000], dtype=np.float64)
WHALE_USD = 100_000
LIQUIDATION_FLOOR_USD = 100
LT_HALF_LIFE_BP = 10.0
ORDER_QUIET_MS = 150

SEC_NS = 1_000_000_000
MIN_NS = 60 * SEC_NS
HOUR_NS = 3_600 * SEC_NS
DAY_NS = 24 * HOUR_NS
GRID_NS = 100 * 1_000_000


def utc(ns: int) -> str:
    return _dt.datetime.fromtimestamp(ns / 1e9, _dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]


# --------------------------------------------------------------------------------------
# trade / aux event streams


@dataclass
class Trades:
    """Fills of one instrument, time-ordered: exchange ns, price (USD), coin qty, taker side (+1 buy, -1 sell)."""

    ts: np.ndarray
    px: np.ndarray
    qty: np.ndarray
    side: np.ndarray  # int8: +1 taker buy, -1 taker sell

    def __len__(self) -> int:
        return len(self.ts)

    @property
    def notional(self) -> np.ndarray:
        return self.px * self.qty


def _read_ndjson_gz(blob: bytes) -> Iterator[dict]:
    with gzip.GzipFile(fileobj=io.BytesIO(blob)) as g:
        for line in g:
            if line.strip():
                yield json.loads(line)


def load_trades(zip_path: str, day: str) -> Trades:
    """Every trade file of `day` in the archive (``trade/.../trades-<day>T<hh>.ndjson.gz``)."""
    ts, px, qty, side = [], [], [], []
    with zipfile.ZipFile(zip_path) as z:
        for name in sorted(z.namelist()):
            if not (name.startswith("trade/") and name.endswith(".ndjson.gz")):
                continue
            if f"trades-{day.replace('-', '')}T" not in name:
                continue
            for r in _read_ndjson_gz(z.read(name)):
                ts.append(r["exch_ts_ns"])
                px.append(float(r["price"]))
                qty.append(float(r["qty"]))
                side.append(1 if r["side"] == "buy" else -1)
    t = np.asarray(ts, dtype=np.int64)
    order = np.argsort(t, kind="stable")
    return Trades(
        ts=t[order],
        px=np.asarray(px, dtype=np.float64)[order],
        qty=np.asarray(qty, dtype=np.float64)[order],
        side=np.asarray(side, dtype=np.int8)[order],
    )


@dataclass
class Liquidation:
    ts: np.ndarray
    px: np.ndarray
    qty: np.ndarray
    usd: np.ndarray
    liquidated: np.ndarray  # int8: +1 long liquidated (forced sell), -1 short liquidated (forced buy)

    def __len__(self) -> int:
        return len(self.ts)


@dataclass
class Aux:
    liq: Liquidation
    oi_ts: np.ndarray
    oi: np.ndarray  # contracts (BTC)
    funding_ts: np.ndarray
    funding_rate: np.ndarray
    mark_ts: np.ndarray
    mark: np.ndarray
    index_px: np.ndarray


def load_aux(zip_path: str, day: str) -> Aux:
    lts, lpx, lqty, llg = [], [], [], []
    ots, oi, fts, fr, mts, mark, idx = [], [], [], [], [], [], []
    with zipfile.ZipFile(zip_path) as z:
        for name in sorted(z.namelist()):
            if not (name.startswith("aux/") and name.endswith(".ndjson.gz")):
                continue
            if f"aux-{day.replace('-', '')}T" not in name:
                continue
            for r in _read_ndjson_gz(z.read(name)):
                kind = r["kind"]
                if kind == "liquidation":
                    q = float(r["qty"])
                    p = float(r["price"])
                    lts.append(r["exch_ts_ns"])
                    lpx.append(p)
                    lqty.append(q)
                    # Binance order side: a forced sell closes a long.
                    llg.append(1 if r["side"] == "sell" else -1)
                elif kind == "oi":
                    ots.append(r["exch_ts_ns"])
                    oi.append(float(r["value"]))
                elif kind == "funding":
                    fts.append(r["exch_ts_ns"])
                    fr.append(float(r["rate"]))
                    mts.append(r["exch_ts_ns"])
                    mark.append(float(r["mark_px"]))
                    idx.append(float(r["index_px"]))
    order = np.argsort(np.asarray(lts, dtype=np.int64), kind="stable") if lts else np.array([], dtype=np.int64)
    lts_a = np.asarray(lts, dtype=np.int64)[order]
    lpx_a = np.asarray(lpx, dtype=np.float64)[order]
    lqty_a = np.asarray(lqty, dtype=np.float64)[order]
    return Aux(
        liq=Liquidation(lts_a, lpx_a, lqty_a, lpx_a * lqty_a, np.asarray(llg, dtype=np.int8)[order]),
        oi_ts=np.asarray(ots, dtype=np.int64),
        oi=np.asarray(oi, dtype=np.float64),
        funding_ts=np.asarray(fts, dtype=np.int64),
        funding_rate=np.asarray(fr, dtype=np.float64),
        mark_ts=np.asarray(mts, dtype=np.int64),
        mark=np.asarray(mark, dtype=np.float64),
        index_px=np.asarray(idx, dtype=np.float64),
    )


def load_validation(zip_path: str, day: str) -> dict:
    """Summaries the recorder wrote for book validation (status counts, differing levels)."""
    status: dict[str, int] = {}
    samples = []
    with zipfile.ZipFile(zip_path) as z:
        for name in sorted(z.namelist()):
            if not (name.startswith("validation/") and name.endswith(".ndjson.gz")):
                continue
            if f"validation-{day.replace('-', '')}T" not in name:
                continue
            for r in _read_ndjson_gz(z.read(name)):
                status[r.get("status", "?")] = status.get(r.get("status", "?"), 0) + 1
                if r.get("differing_levels", 0) > 0:
                    samples.append(r)
    return {"status": status, "differing": samples}


# --------------------------------------------------------------------------------------
# book segments


@dataclass
class SegmentInfo:
    path: str
    grid_ms: int = 100
    tick_units: int = 10
    qty_decimals: int = 3
    depth_available: int = 1000
    first_block_ns: int = 0
    last_block_ns: int = 0
    blocks: int = 0
    data_blocks: int = 0
    gap_blocks: int = 0
    valid_slots: int = 0
    out_of_window_blocks: int = 0
    block_errors: int = 0
    center_tick: int = 0
    lo_tick: int = 0
    width: int = 0
    slots: int = 0
    day_start_ns: int = 0


def extract_book(zip_path: str, out_dir: str, day: str) -> list[str]:
    """Extracts ``book/.../segment-*.tnmd`` of `day` and returns the paths."""
    paths = []
    with zipfile.ZipFile(zip_path) as z:
        for name in z.namelist():
            if name.startswith("book/") and name.endswith(".tnmd") and f"date={day}" in name:
                target = os.path.join(out_dir, os.path.basename(name))
                os.makedirs(out_dir, exist_ok=True)
                with z.open(name) as src, open(target, "wb") as dst:
                    dst.write(src.read())
                paths.append(target)
    return sorted(paths)


@dataclass
class HourBook:
    """One segment decoded around its own window, with the slot-hour resolution."""

    info: SegmentInfo
    hour_start_ns: int  # the grid hour the slot indices belong to (start)
    mids: np.ndarray  # int64 ticks, 0 where no book
    quote_units: np.ndarray  # int64: best_bid + best_ask in ticks (best side doubled when one side is empty)
    ts: np.ndarray  # int64 ns
    depth: np.ndarray  # int32 [slot, 2, width]; side 0 = bids, 1 = asks
    valid: np.ndarray  # bool [slot]


def read_hour_book(decoder: str, segment: str, out_dir: str, day_start_ns: int,
                   half_ticks: int = 3000) -> HourBook:
    """Runs the decoder on one segment; results go to a private directory keyed by the segment name."""
    import shutil
    import subprocess

    stage = os.path.join(out_dir, os.path.basename(segment).replace(".tnmd", ""))
    shutil.rmtree(stage, ignore_errors=True)
    subprocess.run(
        [decoder, "--segment", segment, "--out", stage, "--instrument", "book",
         "--hour-start-ns", str(day_start_ns), "--window-half-ticks", str(half_ticks), "--center-ticks", "0"],
        check=True,
    )
    root = os.path.join(stage, "instrument=book")
    day_dir = os.path.join(root, os.listdir(root)[0])
    meta = json.load(open(os.path.join(day_dir, "meta.json")))
    width = meta["width"]
    slots = meta["slots"]
    info = SegmentInfo(
        path=segment,
        grid_ms=meta["grid_ms"],
        tick_units=meta["tick_units"],
        qty_decimals=meta["qty_decimals"],
        depth_available=meta["depth_available"],
        first_block_ns=meta["first_block_exch_ns"],
        last_block_ns=meta["last_block_exch_ns"],
        blocks=meta["blocks"],
        data_blocks=meta["data_blocks"],
        gap_blocks=meta["gap_blocks"],
        valid_slots=meta["valid_slots"],
        out_of_window_blocks=meta["out_of_window_blocks"],
        block_errors=meta["block_errors"],
        center_tick=meta["center_tick"],
        lo_tick=meta["lo_tick"],
        width=width,
        slots=slots,
        day_start_ns=day_start_ns,
    )
    mids = np.fromfile(os.path.join(day_dir, "mids"), dtype=np.int64)
    quote = np.fromfile(os.path.join(day_dir, "quote_units"), dtype=np.int64)
    ts = np.fromfile(os.path.join(day_dir, "ts"), dtype=np.int64)
    valid = np.fromfile(os.path.join(day_dir, "valid"), dtype=np.uint8).astype(bool)
    depth = np.memmap(os.path.join(day_dir, "depth"), dtype=np.int32, mode="r", shape=(slots, 2, width))
    # The grid hour of the slots: the day grid's at most one hour before (and including) the first block.
    first = int(meta["first_block_exch_ns"])
    hour_start = first - (first - day_start_ns) % HOUR_NS
    return HourBook(info, hour_start, mids, quote, ts, depth, valid)


# --------------------------------------------------------------------------------------
# absorption (client-side marks of the recorded fills)


def rolling_target_credits(ts_ms: np.ndarray, notional: np.ndarray, fill_ids: np.ndarray,
                           window_ms: int = ABSORPTION_WINDOW_MS) -> np.ndarray:
    """Credit of every fill: the largest window (fills of the same price and side within
    `window_ms` up to its own time) it ever belonged to, as ``absorption.ts`` #take does."""
    n = len(ts_ms)
    credit = np.zeros(n)
    start = 0
    for i in range(n):
        if i and fill_ids[i] != fill_ids[i - 1]:
            start = i
        lo = start
        # window ending at fill i: fills with t >= t_i - W
        bound = ts_ms[i] - window_ms
        while lo < i and ts_ms[lo] < bound:
            lo += 1
        s = float(notional[lo:i + 1].sum())
        credit[lo:i + 1] = np.maximum(credit[lo:i + 1], s)
    return credit


def size_bucket(usd: np.ndarray) -> np.ndarray:
    return np.searchsorted(SIZE_EDGES, usd, side="right") - 1
