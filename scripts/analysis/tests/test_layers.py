"""Tests for the reference mechanics re-implemented on the NumPy side.

``tests/`` holds a fixture taken from the Rust decoder on one real segment
(``kat.npz``): the CSR row pointers and record times of its first block, the first
book state, and the Fenwick layer tables the reference builds for that many rows.

Run locally (no build needed):  python3 -m unittest scripts/analysis/tests/test_layers.py
"""

from __future__ import annotations

import os
import sys
import unittest

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import lmstats  # noqa: E402

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")


def cross_profit(events: list[tuple[float, float]]) -> float:
    """``shared/crossing.ts`` ``crossProfit``: two-aggressor trades with their sizes."""
    x = 0.0
    q = 0.0
    for p, v in events:
        x += p * v
        q += v
    return x / q if q > 0 else 0.0


def absorption_credit(times_ms: np.ndarray, usd: np.ndarray, window_ms: int = 10) -> np.ndarray:
    """``shared/absorption.ts`` ``#take``: every fill keeps the largest window (same side and price,
    time at or after its own minus the window, itself included) it ever belonged to."""
    n = len(times_ms)
    credit = np.zeros(n)
    for i in range(n):
        lo = i
        while lo > 0 and times_ms[lo - 1] >= times_ms[i] - window_ms:
            lo -= 1
        s = float(usd[lo : i + 1].sum())
        credit[lo : i + 1] = np.maximum(credit[lo : i + 1], s)
    return credit


def start_anchored_sums(times_ms: np.ndarray, usd: np.ndarray, window_ms: int = 10) -> list[float]:
    """``#take``'s window sums for the automatic threshold: a window opens at a fill and takes the
    fills up to its opening plus the window; the next fill after that opens the next one."""
    out = []
    i = 0
    while i < len(times_ms):
        start = times_ms[i]
        s = 0.0
        while i < len(times_ms) and times_ms[i] <= start + window_ms:
            s += usd[i]
            i += 1
        out.append(s)
    return out


def lt_weight(bp: float, half_life_bp: float) -> float:
    """``shared/lt-sample.ts`` ``ltWeight``."""
    return 2.0 ** (-max(0.0, bp) / half_life_bp)


def auto_threshold(minutes: list[dict], k: float) -> float | None:
    """``autoThreshold``: mean + k SD over every window sum recorded in the minutes."""
    n = sum(m["n"] for m in minutes)
    if not n:
        return None
    mean = sum(m["n"] * m["mean"] for m in minutes) / n
    m2 = sum(m["m2"] + m["n"] * (m["mean"] - mean) ** 2 for m in minutes)
    return mean + k * (m2 / n) ** 0.5


def size_bucket(usd: float) -> int:
    """``sizeBucket`` over ``SIZE_EDGES``: the last edge it reaches (0 under 25K)."""
    bucket = 0
    for i, edge in enumerate([0, 25_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 5_000_000]):
        if i and usd >= edge:
            bucket = i
    return bucket


class LadderWindowTest(unittest.TestCase):
    def setUp(self):
        data = np.load(os.path.join(FIXTURES, "kat.npz"))
        self.ptrs = data["ptrs"]
        self.times = data["times"]
        self.first_keys = data["first_keys"]  # (side, tick): bids above the touch, asks below
        self.first_vals = data["first_vals"]

    def test_fixture_shape(self):
        self.assertEqual(len(self.ptrs), len(self.times) + 1)
        self.assertEqual(int(self.ptrs[0]), 0)
        self.assertTrue(np.all(np.diff(self.times) > 0))
        self.assertTrue(np.all(np.diff(self.ptrs) >= 0))
        self.assertEqual(len(self.first_keys), 200)  # top 100 levels a side at the block's snapshot
        self.assertEqual(int((self.first_keys[:, 0] == 0).sum()), 100)
        self.assertEqual(int((self.first_keys[:, 0] == 1).sum()), 100)
        self.assertEqual(int(self.ptrs[-1]), 16029)  # changes the first block applies

class AbsorptionWindowTest(unittest.TestCase):
    def test_credit_is_the_largest_window_a_fill_belongs_to(self):
        times = np.array([0, 3, 7, 13, 20], dtype=np.int64)
        usd = np.array([10.0, 20.0, 30.0, 40.0, 50.0])
        credit = absorption_credit(times, usd)
        # Each fill keeps the largest window it ever belonged to, not the one it would see at its own
        # arrival: fill 0 is in the 60 USD window [0, 7], fill 1 in the 90 USD window [3, 13].
        self.assertEqual(credit[0], 60.0)
        self.assertEqual(credit[1], 90.0)
        self.assertEqual(credit[2], 90.0)
        self.assertEqual(credit[3], 90.0)
        self.assertEqual(credit[4], 90.0)
        # Fill 3 at 13 ms: itself and fill 2 at 7 ms (90 USD); fill 4 at 20 ms: itself and fill 3.
        self.assertEqual(credit[4], 90.0)

    def test_start_anchored_sums(self):
        times = np.array([0, 5, 11, 12, 30], dtype=np.int64)
        usd = np.array([1.0, 2.0, 4.0, 8.0, 16.0])
        # Window at 0 covers 0, 5 (<= 10); the next fill at 11 opens, covering 11, 12 (<= 21); 30 opens.
        self.assertEqual(start_anchored_sums(times, usd), [3.0, 12.0, 16.0])

    def test_auto_threshold_merges_minutes(self):
        # Two minutes of two window sums each: {10, 10} and {20, 24} -> n 4, mean 16, population
        # variance ((10-16)^2 * 2 + (20-16)^2 + (24-16)^2) / 4 = 38, SD 6.1644... -> 16 + 10 SD.
        minutes = [{"n": 2, "mean": 10.0, "m2": 0.0}, {"n": 2, "mean": 22.0, "m2": 8.0}]
        self.assertAlmostEqual(auto_threshold(minutes, 10), 77.64414002968977, places=9)
        self.assertIsNone(auto_threshold([], 10))


class ReferenceScalarRulesTest(unittest.TestCase):
    def test_lt_weight_halves_every_half_life(self):
        self.assertEqual(lt_weight(0, 10), 1.0)
        self.assertAlmostEqual(lt_weight(10, 10), 0.5)
        self.assertAlmostEqual(lt_weight(20, 10), 0.25)
        self.assertEqual(lt_weight(-5, 10), 1.0)

    def test_size_buckets(self):
        self.assertEqual(size_bucket(10_000), 0)
        self.assertEqual(size_bucket(25_000), 1)
        self.assertEqual(size_bucket(49_999), 1)
        self.assertEqual(size_bucket(50_000), 2)
        self.assertEqual(size_bucket(100_000), 3)
        self.assertEqual(size_bucket(7_000_000), 7)

    def test_cross_profit(self):
        self.assertAlmostEqual(cross_profit([(100.0, 1.0), (102.0, 1.0)]), 101.0)
        self.assertEqual(cross_profit([]), 0.0)


if __name__ == "__main__":
    unittest.main()
