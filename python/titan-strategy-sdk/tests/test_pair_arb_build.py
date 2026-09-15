"""Cold-path construction: parameter validation and entrypoint wiring."""

import unittest

from pair_arb_harness import default_parameters, pair_arb  # noqa: E402


class TestPairArbBuild(unittest.TestCase):
    def test_build_wires_the_four_entrypoints(self):
        strategy = pair_arb.build(default_parameters())
        self.assertEqual(
            sorted(strategy.entrypoints),
            ["on_cancel", "on_fill", "on_tick", "risk_check"],
        )
        for name, handler in strategy.entrypoints.items():
            self.assertTrue(callable(handler), name)

    def test_build_creates_one_pair_one_slot_and_a_fixed_active_array(self):
        strategy = pair_arb.build(default_parameters())
        self.assertEqual(strategy.pair.current_slot.slot_id, 0)
        self.assertEqual(len(strategy.pair.active_orders), pair_arb.MAX_ACTIVE_ORDERS)
        self.assertEqual(strategy.pair.status, pair_arb.PairStatus.CREATED)
        self.assertFalse(strategy.pair.ready)

    def test_missing_required_parameters_are_rejected(self):
        parameters = default_parameters()
        del parameters["hedge_ratio_abs"]
        with self.assertRaises(ValueError) as error:
            pair_arb.build(parameters)
        self.assertIn("hedge_ratio_abs", str(error.exception))

    def test_non_positive_ratio_or_slot_unit_is_rejected(self):
        with self.assertRaises(ValueError):
            pair_arb.build(default_parameters(hedge_ratio_abs=0.0))
        with self.assertRaises(ValueError):
            pair_arb.build(default_parameters(slot_unit=0.0))
        with self.assertRaises(ValueError):
            pair_arb.build(default_parameters(max_position=-1.0))

    def test_identical_symbols_are_rejected(self):
        with self.assertRaises(ValueError):
            pair_arb.build(default_parameters(symbol_right="AAA"))

    def test_invalid_direction_and_mode_are_rejected(self):
        with self.assertRaises(KeyError):
            pair_arb.build(default_parameters(direction="SIDEWAYS"))
        with self.assertRaises(KeyError):
            pair_arb.build(default_parameters(mode="MAKER_MAKER"))

    def test_parameters_are_applied_to_the_pair(self):
        strategy = pair_arb.build(
            default_parameters(direction="SHORT_SPREAD", mode="TAKER_TAKER",
                               hedge_ratio_abs=0.6, spread=2.0, requote_distance=0.25)
        )
        pair = strategy.pair
        self.assertEqual(pair.direction, pair_arb.Direction.SHORT_SPREAD)
        self.assertEqual(pair.mode, pair_arb.ExecutionMode.TAKER_TAKER)
        self.assertEqual(pair.direction.initiator_side, pair_arb.Side.SELL)
        self.assertEqual(pair.direction.hedge_side, pair_arb.Side.BUY)
        self.assertAlmostEqual(pair.hedge_ratio_abs, 0.6)
        self.assertEqual(pair.spread, 2.0)
        self.assertEqual(pair.requote_distance, 0.25)

    def test_invalid_limits_are_rejected_at_build_time(self):
        with self.assertRaises(ValueError):
            pair_arb.build(default_parameters(max_unhedged_qty_soft=5.0,
                                              max_unhedged_qty_hard=1.0))


if __name__ == "__main__":
    unittest.main()
