"""Tests for the reference mechanics re-implemented on the NumPy side.

The checks call the modules themselves, not stand-ins for them: a regression in ``lm_common``
has to fail a test.

``fixtures/`` holds a vector taken from the Rust decoder on one real segment (``kat.npz``): the
CSR row pointers and record times of its first block, the first book state, and the Fenwick layer
tables the reference builds for that many rows. The slot and escape rules of the decoder itself
are unit-tested where the decoding happens, in ``tnmd_decoder/src/main.rs``.

Run locally (no build needed):  python3 -m unittest scripts/analysis/tests/test_layers.py
"""

from __future__ import annotations

import os
import sys
import unittest

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import lm_common  # noqa: E402

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")


def start_anchored_sums(times_ms: np.ndarray, usd: np.ndarray, window_ms: int = 10) -> list[float]:
    """``#take``'s window sums for the automatic threshold (``lm_common`` cites the rule but has no
    function for it): a window opens at a fill and takes the fills up to its opening plus the
    window; the next fill after that opens the next one."""
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


def auto_threshold(minutes: list[dict], k: float) -> float | None:
    """``autoThreshold``: mean + k SD over every window sum recorded in the minutes. Also not in
    ``lm_common``; the rule is what ``lm_common``'s constants describe."""
    n = sum(m["n"] for m in minutes)
    if not n:
        return None
    mean = sum(m["n"] * m["mean"] for m in minutes) / n
    m2 = sum(m["m2"] + m["n"] * (m["mean"] - mean) ** 2 for m in minutes)
    return mean + k * (m2 / n) ** 0.5


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
        # One price and side: every fill is in the same group.
        credit = lm_common.rolling_target_credits(times, usd, np.zeros(5, dtype=np.int64))
        # Each fill keeps the largest window it ever belonged to, not the one it would see at its own
        # arrival: fill 0 is in the 60 USD window [0, 7], fill 1 in the 90 USD window [3, 13].
        self.assertEqual(credit[0], 60.0)
        self.assertEqual(credit[1], 90.0)
        self.assertEqual(credit[2], 90.0)
        self.assertEqual(credit[3], 90.0)
        self.assertEqual(credit[4], 90.0)

    def test_credit_restarts_at_the_next_group(self):
        # The fill at index 2 is the first of another price (or side): it may not credit a window
        # that reaches back into the previous group.
        times = np.array([0, 3, 7, 9, 20], dtype=np.int64)
        usd = np.array([10.0, 20.0, 30.0, 40.0, 50.0])
        credit = lm_common.rolling_target_credits(times, usd, np.array([0, 0, 1, 1, 1], dtype=np.int64))
        self.assertEqual(credit[0], 30.0)  # fills 0 and 1 within 10 ms of fill 1
        self.assertEqual(credit[1], 30.0)
        self.assertEqual(credit[2], 70.0)  # fills 2 and 3
        self.assertEqual(credit[3], 70.0)
        self.assertEqual(credit[4], 50.0)  # alone

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
    def test_size_buckets(self):
        # `sizeBucket` over SIZE_EDGES: the last edge a notional reaches (0 under 25K, 7 from 5M).
        edges = [10_000, 25_000, 49_999, 50_000, 100_000, 7_000_000]
        self.assertEqual(lm_common.size_bucket(np.array(edges)).tolist(), [0, 1, 1, 2, 3, 7])


if __name__ == "__main__":
    unittest.main()
