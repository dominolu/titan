import sys
import unittest
from pathlib import Path

import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))
sys.path.append(str(PROJECT_ROOT / "python" / "titan-strategy-sdk"))

from titan_strategy.context import (  # noqa: E402
    Strategy,
    fill_dtype,
    order_event_dtype,
    position_event_dtype,
    runtime_ctx_dtype,
)
from strategies.okx_hyperliquid_xemm import strategy as xemm  # noqa: E402


def _default_parameters():
    return {
        "maker_asset_no": 0,
        "hedge_asset_no": 1,
        "maker_account_no": 0,
        "hedge_account_no": 1,
        "order_amount_base": 10.0,
        "min_profitability_bps": 10.0,
        "cancel_profitability_bps": 0.0,
        "maker_fee_bps": 10.0,
        "taker_fee_bps": 10.0,
        "taker_volume_factor": 0.25,
        "quote_conversion_rate": 1.0,
        "maker_price_tick": 1.0,
        "maker_quantity_lot": 1.0,
        "maker_contract_base_multiplier": 1.0,
        "hedge_price_tick": 1.0,
        "hedge_quantity_lot": 1.0,
        "hedge_contract_base_multiplier": 1.0,
        "max_order_notional": 1_000.0,
        "max_abs_position_base": 10_000.0,
        "max_unhedged_base": 10_000.0,
        "max_unhedged_notional": 10_000_000.0,
        "market_stale_ms": 2_000,
        "conversion_expiry_ns": 1_000_000_000,
        "anti_hysteresis_ms": 60_000,
        "price_sample_interval_ms": 5_000,
        "price_sample_window": 12,
        "hedge_retry_limit": 5,
        "hedge_retry_backoff_ms": 100,
        "top_depth_tolerance_base": 0.0,
        "hedge_slippage_bps": 20.0,
    }


class _Harness:
    def __init__(self):
        self.s = xemm.build(_default_parameters())
        self.runtime_ctx = np.zeros(1, dtype=runtime_ctx_dtype)
        self.runtime_ctx[0]["state_f64_ptr"] = self.s.state.ctypes.data
        self.runtime_ctx[0]["state_f64_len"] = self.s.state.size
        self.runtime_ctx[0]["state_i64_ptr"] = self.s.state_i64.ctypes.data
        self.runtime_ctx[0]["state_i64_len"] = self.s.state_i64.size
        self.runtime_ctx[0]["num_commands"] = 0
        self.runtime_ctx[0]["command_capacity"] = 16
        self.runtime_ctx[0]["payload_ptr"] = 0
        self.runtime_ctx[0]["fills_ptr"] = 0

        # Make PositionChanged baseline path active and allow both accounts to be accepted.
        self.s.state_i64[xemm.I_MAKER_POSITION_READY] = 1
        self.s.state_i64[xemm.I_HEDGE_POSITION_READY] = 1

        self.ctx = Strategy(self.runtime_ctx)

    def set_fill(
        self, *, sequence, side, asset_no, local_account_no, last_fill_qty, account_epoch=1
    ):
        fill = np.zeros(1, dtype=fill_dtype)
        fill[0]["asset_no"] = asset_no
        fill[0]["local_account_no"] = local_account_no
        fill[0]["account_epoch"] = account_epoch
        fill[0]["sequence"] = sequence
        fill[0]["side"] = side
        fill[0]["last_fill_qty"] = float(last_fill_qty)
        fill[0]["cumulative_filled_qty"] = float(last_fill_qty)
        self.runtime_ctx[0]["fills_ptr"] = fill.ctypes.data
        self.runtime_ctx[0]["num_fills"] = 1
        self.s.on_filled(self.ctx)

    def set_position(
        self,
        *,
        sequence,
        local_account_no,
        asset_no,
        quantity,
        position_side=xemm.BUY_SIDE,
        account_epoch=1,
    ):
        position = np.zeros(1, dtype=position_event_dtype)
        position[0]["local_account_no"] = local_account_no
        position[0]["asset_no"] = asset_no
        position[0]["account_epoch"] = account_epoch
        position[0]["sequence"] = sequence
        position[0]["quantity"] = float(abs(quantity))
        position[0]["position_side"] = position_side
        self.runtime_ctx[0]["payload_ptr"] = position.ctypes.data
        self.s.on_position(self.ctx)

    def set_order(self, *, order_id, status, side, asset_no):
        order = np.zeros(1, dtype=order_event_dtype)
        order[0]["order_id"] = order_id
        order[0]["status"] = status
        order[0]["side"] = side
        order[0]["asset_no"] = asset_no
        self.runtime_ctx[0]["orders_ptr"] = order.ctypes.data
        self.runtime_ctx[0]["num_orders"] = 1
        self.s.on_order(self.ctx)


class TestXemm005FillAndPosition(unittest.TestCase):
    def test_fill_order_position_out_of_order_keeps_terminal_order_and_absolute_anchor(self):
        harness = _Harness()
        harness.s.state_i64[xemm.I_BID_ORDER_ID] = 42
        harness.s.state_i64[xemm.I_BID_STATE] = xemm.LEG_OPEN

        harness.set_fill(
            sequence=2,
            side=xemm.BUY_SIDE,
            asset_no=0,
            local_account_no=0,
            last_fill_qty=1.0,
        )
        harness.set_order(
            order_id=42,
            status=xemm.ORDER_FILLED,
            side=xemm.BUY_SIDE,
            asset_no=0,
        )
        harness.set_position(
            sequence=3,
            local_account_no=0,
            asset_no=0,
            quantity=1.0,
        )

        self.assertEqual(harness.s.state_i64[xemm.I_BID_STATE], xemm.LEG_EMPTY)
        self.assertEqual(harness.s.state_i64[xemm.I_BID_ORDER_ID], 0)
        self.assertEqual(harness.s.state[xemm.F_UNHEDGED_BASE], 1.0)

    def test_fill_dedupe_identity_isolated_by_account_asset_and_side(self):
        accounts = _Harness()
        accounts.set_fill(
            sequence=7, side=xemm.BUY_SIDE, asset_no=0,
            local_account_no=0, last_fill_qty=1.0,
        )
        accounts.set_fill(
            sequence=7, side=xemm.BUY_SIDE, asset_no=0,
            local_account_no=1, last_fill_qty=1.0,
        )
        self.assertEqual(accounts.s.state_i64[xemm.I_MAKER_FILL_COUNT], 2)

        assets = _Harness()
        assets.set_fill(
            sequence=7, side=xemm.BUY_SIDE, asset_no=0,
            local_account_no=0, last_fill_qty=1.0,
        )
        assets.set_fill(
            sequence=7, side=xemm.BUY_SIDE, asset_no=1,
            local_account_no=0, last_fill_qty=1.0,
        )
        self.assertEqual(assets.s.state[xemm.F_UNHEDGED_BASE], 2.0)

        sides = _Harness()
        sides.set_fill(
            sequence=7, side=xemm.BUY_SIDE, asset_no=0,
            local_account_no=0, last_fill_qty=1.0,
        )
        sides.set_fill(
            sequence=7, side=xemm.SELL_SIDE, asset_no=0,
            local_account_no=0, last_fill_qty=1.0,
        )
        self.assertEqual(sides.s.state_i64[xemm.I_MAKER_FILL_COUNT], 2)
        self.assertEqual(sides.s.state[xemm.F_UNHEDGED_BASE], 0.0)

    def test_new_account_epoch_accepts_restarted_fill_and_position_versions(self):
        harness = _Harness()
        harness.set_fill(
            sequence=10, account_epoch=2, side=xemm.BUY_SIDE,
            asset_no=0, local_account_no=0, last_fill_qty=1.0,
        )
        harness.set_fill(
            sequence=1, account_epoch=3, side=xemm.BUY_SIDE,
            asset_no=0, local_account_no=0, last_fill_qty=1.0,
        )
        harness.set_fill(
            sequence=1, account_epoch=3, side=xemm.BUY_SIDE,
            asset_no=0, local_account_no=0, last_fill_qty=1.0,
        )
        self.assertEqual(harness.s.state_i64[xemm.I_MAKER_FILL_COUNT], 2)

        harness.set_position(
            sequence=10, account_epoch=2, local_account_no=0,
            asset_no=0, quantity=5.0,
        )
        harness.set_position(
            sequence=1, account_epoch=3, local_account_no=0,
            asset_no=0, quantity=7.0,
        )
        harness.set_position(
            sequence=11, account_epoch=2, local_account_no=0,
            asset_no=0, quantity=9.0,
        )
        self.assertEqual(harness.s.state[xemm.F_MAKER_POSITION_ESTIMATE], 7.0)
        self.assertEqual(harness.s.state_i64[xemm.I_MAKER_POSITION_EPOCH], 3)
        self.assertEqual(harness.s.state_i64[xemm.I_MAKER_POSITION_SEQUENCE], 1)

    def test_fill_and_position_mixed_event_keeps_unhedged_only_by_position_anchor(self):
        harness = _Harness()

        # Position anchor (maker=+2, hedge=0): unhedged = 2.
        harness.set_position(
            sequence=1,
            local_account_no=0,
            asset_no=0,
            quantity=2.0,
            position_side=xemm.BUY_SIDE,
        )
        self.assertEqual(harness.s.state[xemm.F_UNHEDGED_BASE], 2.0)

        # Fill indicates +1 base maker fill (maker buy). on_filled applies +1 increment.
        harness.set_fill(
            sequence=1,
            side=xemm.BUY_SIDE,
            asset_no=0,
            local_account_no=0,
            last_fill_qty=1.0,
        )
        self.assertEqual(harness.s.state[xemm.F_UNHEDGED_BASE], 3.0)
        self.assertEqual(harness.s.state[xemm.F_TOTAL_MAKER_FILL_BASE], 1.0)

        # Position snapshot arrives with maker=+3; PositionChanged is absolute anchor.
        harness.set_position(
            sequence=2,
            local_account_no=0,
            asset_no=0,
            quantity=3.0,
            position_side=xemm.BUY_SIDE,
        )
        self.assertEqual(harness.s.state[xemm.F_UNHEDGED_BASE], 3.0)
        self.assertEqual(harness.s.state[xemm.F_MAKER_POSITION_ESTIMATE], 3.0)

    def test_maker_fill_direction_matrix_uses_incremental_quantity(self):
        buy = _Harness()
        buy.set_fill(
            sequence=1,
            side=xemm.BUY_SIDE,
            asset_no=0,
            local_account_no=0,
            last_fill_qty=2.0,
        )
        self.assertEqual(buy.s.state[xemm.F_UNHEDGED_BASE], 2.0)

        sell = _Harness()
        sell.set_fill(
            sequence=1,
            side=xemm.SELL_SIDE,
            asset_no=0,
            local_account_no=0,
            last_fill_qty=2.0,
        )
        self.assertEqual(sell.s.state[xemm.F_UNHEDGED_BASE], -2.0)

    def test_hedge_fill_direction_matrix_reduces_existing_exposure(self):
        positive = _Harness()
        positive.s.state[xemm.F_UNHEDGED_BASE] = 3.0
        positive.set_fill(
            sequence=1,
            side=xemm.SELL_SIDE,
            asset_no=1,
            local_account_no=1,
            last_fill_qty=2.0,
        )
        self.assertEqual(positive.s.state[xemm.F_UNHEDGED_BASE], 1.0)

        negative = _Harness()
        negative.s.state[xemm.F_UNHEDGED_BASE] = -3.0
        negative.set_fill(
            sequence=1,
            side=xemm.BUY_SIDE,
            asset_no=1,
            local_account_no=1,
            last_fill_qty=2.0,
        )
        self.assertEqual(negative.s.state[xemm.F_UNHEDGED_BASE], -1.0)

    def test_partial_fill_deltas_one_one_three_total_five(self):
        harness = _Harness()
        for sequence, delta, expected in [(1, 1.0, 1.0), (2, 1.0, 2.0), (3, 3.0, 5.0)]:
            harness.set_fill(
                sequence=sequence,
                side=xemm.BUY_SIDE,
                asset_no=0,
                local_account_no=0,
                last_fill_qty=delta,
            )
            self.assertEqual(harness.s.state[xemm.F_UNHEDGED_BASE], expected)
        self.assertEqual(harness.s.state[xemm.F_TOTAL_MAKER_FILL_BASE], 5.0)
        self.assertEqual(harness.s.state_i64[xemm.I_MAKER_FILL_COUNT], 3)

    def test_fill_then_position_still_single_count_when_hedge_and_maker_anchor_arrives(self):
        harness = _Harness()

        # Initial absolute anchor: maker +2, hedge -1, so unhedged=+1.
        harness.set_position(
            sequence=1,
            local_account_no=0,
            asset_no=0,
            quantity=2.0,
            position_side=xemm.BUY_SIDE,
        )
        harness.set_position(
            sequence=1,
            local_account_no=1,
            asset_no=1,
            quantity=1.0,
            position_side=xemm.SELL_SIDE,
        )
        self.assertEqual(harness.s.state[xemm.F_UNHEDGED_BASE], 1.0)
        self.assertEqual(harness.s.state[xemm.F_MAKER_POSITION_ESTIMATE], 2.0)
        self.assertEqual(harness.s.state[xemm.F_HEDGE_POSITION_ESTIMATE], -1.0)

        # Fill before the latest position baseline refresh: maker +1.
        harness.set_fill(
            sequence=2,
            side=xemm.BUY_SIDE,
            asset_no=0,
            local_account_no=0,
            last_fill_qty=1.0,
        )
        self.assertEqual(harness.s.state[xemm.F_UNHEDGED_BASE], 2.0)
        self.assertEqual(harness.s.state[xemm.F_TOTAL_MAKER_FILL_BASE], 1.0)

        # Anchor position then arrives with maker +3 and hedge -1 again, still absolute.
        harness.set_position(
            sequence=2,
            local_account_no=0,
            asset_no=0,
            quantity=3.0,
            position_side=xemm.BUY_SIDE,
        )
        harness.set_position(
            sequence=2,
            local_account_no=1,
            asset_no=1,
            quantity=1.0,
            position_side=xemm.SELL_SIDE,
        )
        self.assertEqual(harness.s.state[xemm.F_UNHEDGED_BASE], 2.0)
        self.assertEqual(harness.s.state[xemm.F_MAKER_POSITION_ESTIMATE], 3.0)
        self.assertEqual(harness.s.state[xemm.F_HEDGE_POSITION_ESTIMATE], -1.0)

    def test_duplicate_fill_is_idempotent_under_position_anchor_updates(self):
        harness = _Harness()

        harness.set_position(
            sequence=1,
            local_account_no=0,
            asset_no=0,
            quantity=2.0,
            position_side=xemm.BUY_SIDE,
        )
        self.assertEqual(harness.s.state_i64[xemm.I_MAKER_FILL_COUNT], 0)

        # First replay of this fill.
        harness.set_fill(
            sequence=3,
            side=xemm.BUY_SIDE,
            asset_no=0,
            local_account_no=0,
            last_fill_qty=1.0,
        )
        self.assertEqual(harness.s.state[xemm.F_UNHEDGED_BASE], 3.0)
        self.assertEqual(harness.s.state_i64[xemm.I_MAKER_FILL_COUNT], 1)

        # Same fill replay should be deduplicated and never re-apply.
        harness.set_fill(
            sequence=3,
            side=xemm.BUY_SIDE,
            asset_no=0,
            local_account_no=0,
            last_fill_qty=1.0,
        )
        self.assertEqual(harness.s.state[xemm.F_UNHEDGED_BASE], 3.0)
        self.assertEqual(harness.s.state_i64[xemm.I_MAKER_FILL_COUNT], 1)

        # Position snapshot with same anchor keeps absolute baseline and also prevents any hidden double count.
        harness.set_position(
            sequence=2,
            local_account_no=0,
            asset_no=0,
            quantity=3.0,
            position_side=xemm.BUY_SIDE,
        )
        self.assertEqual(harness.s.state[xemm.F_UNHEDGED_BASE], 3.0)
        self.assertEqual(harness.s.state[xemm.F_MAKER_POSITION_ESTIMATE], 3.0)


if __name__ == "__main__":
    unittest.main()
