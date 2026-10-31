"""Scene protocol validation; real callbacks/publication are covered by Rust integration."""
import unittest

from titan_strategy.definition import Capability, EventSubscription, StrategySpec
from titan_strategy.testing import _assert, depth_book, validate_tests
from titan_strategy.types import EventKind, EventQos


class TestInlineScenes(unittest.TestCase):
    def test_protocol_rejects_empty_assertions_duplicate_names_unknown_steps(self):
        for cases in ([{"name": "empty"}],
                      [{"name": "same", "expect": {"x": 1}}] * 2,
                      [{"name": "unknown", "steps": [{"callback": "trade"}], "expect": {"x": 1}}],
                      [{"name": "unknown", "steps": [{"callback": "on_tick", "unknown": True}], "expect": {"x": 1}}]):
            with self.subTest(cases=cases), self.assertRaises(ValueError):
                validate_tests(cases)

    def test_depth_fixture_has_complete_sides_and_explicit_timestamps(self):
        rows = depth_book(bid=1000, ask=1001, now_ns=42)
        self.assertEqual(len(rows), 200)
        self.assertEqual((rows[0]["price_ticks"], rows[99]["price_ticks"]), (1000, 901))
        self.assertEqual((rows[100]["price_ticks"], rows[-1]["price_ticks"]), (1001, 1100))
        self.assertTrue(all(row["receive_ts_ns"] == 42 for row in rows))

    def test_assertion_paths_and_operators_fail_closed(self):
        _assert({"nested": {"values": [1, 2]}}, {"nested.values.1": 2})
        for checks in ({"nested.values.1": 3}, {"nested.values.1": {"unknown": 2}},
                       {"nested..values.1": 2}, {"nested.values.1": {}}):
            with self.subTest(checks=checks), self.assertRaises((ValueError, AssertionError)):
                _assert({"nested": {"values": [1, 2]}}, checks)

    def test_host_depth_projection_still_requires_reliable_ordering(self):
        with self.assertRaisesRegex(ValueError, "reliable ordered"):
            StrategySpec("depth_test", "1.0.0", 1,
                         subscriptions=(EventSubscription(EventKind.DEPTH, "on_depth", 1, EventQos.LATEST),),
                         capabilities=Capability.MARKET_DATA | Capability.HOST_DEPTH_100)
