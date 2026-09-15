"""Harness that drives the ABI v12 Numba ``pair_arb`` strategy with a synthetic runtime context."""

from __future__ import annotations

import ctypes
import sys
from pathlib import Path
from typing import Optional

import numpy as np

SDK_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = Path(__file__).resolve().parents[3]
for root in (SDK_ROOT, REPO_ROOT):
    if str(root) not in sys.path:
        sys.path.append(str(root))

from titan_strategy.abi_v10 import (  # noqa: E402
    EVENT_TICK,
    backtest_command_buffer_dtype,
    fill_dtype,
    order_command_dtype,
    order_event_dtype,
    runtime_ctx_dtype,
)
from titan_strategy.context import callback_bridge  # noqa: E402
from strategies.pair_arb import state_layout as L  # noqa: E402
from strategies.pair_arb import strategy as pair_arb_numba  # noqa: E402

NS = 1_000_000


def default_parameters(**overrides):
    parameters = {
        "left_asset_no": 0,
        "right_asset_no": 1,
        "left_account_no": 0,
        "right_account_no": 1,
        "direction": "LONG_SPREAD",
        "mode": "MAKER_TAKER",
        "hedge_ratio_abs": 1.0,
        "left_price_tick": 0.1,
        "left_lot_size": 1.0,
        "right_price_tick": 0.1,
        "right_lot_size": 1.0,
        "spread": 1.0,
        "requote_distance": 0.5,
        "cancel_timeout_ns": 1_000 * NS,
        "max_position_lots": 30.0,
        "slot_unit_lots": 10.0,
        "dust_lots": 0.0,
        "cancel_retry_limit": 2,
        "taker_slippage_bps": 0.0,
        "emergency_slippage_bps": 0.0,
        "max_unhedged_lots_soft": 10.0,
        "max_unhedged_lots_hard": 20.0,
        "max_gross_notional": 1_000_000.0,
    }
    parameters.update(overrides)
    return parameters


class NumbaHarness:
    """Minimal runtime context: two legs, one strategy, one backtest command buffer."""

    def __init__(self, **overrides) -> None:
        self.strategy = pair_arb_numba.build(default_parameters(**overrides))
        self.state = self.strategy.state
        self.state_i64 = self.strategy.state_i64
        self.now_ns = 1_000 * NS

        self.commands = np.zeros(32, dtype=order_command_dtype)
        self.command_buffer = np.zeros(1, dtype=backtest_command_buffer_dtype)
        self.command_buffer[0]["commands_ptr"] = self.commands.ctypes.data
        self.command_buffer[0]["num_commands"] = 0
        self.command_buffer[0]["command_capacity"] = self.commands.size

        self.markets = np.zeros(2, dtype=np.float64)
        self.best_bid = np.zeros(2, dtype=np.float64)
        self.best_ask = np.zeros(2, dtype=np.float64)
        self.positions = np.zeros(2, dtype=np.float64)
        self.fills = np.zeros(8, dtype=fill_dtype)
        self.orders = np.zeros(8, dtype=order_event_dtype)

        self.ctx = np.zeros(1, dtype=runtime_ctx_dtype)
        self.ctx[0]["event_kind"] = EVENT_TICK
        self.ctx[0]["now"] = self.now_ns
        self.ctx[0]["state_f64_ptr"] = self.state.ctypes.data
        self.ctx[0]["state_f64_len"] = self.state.size
        self.ctx[0]["state_i64_ptr"] = self.state_i64.ctypes.data
        self.ctx[0]["state_i64_len"] = self.state_i64.size
        self.ctx[0]["positions_ptr"] = self.positions.ctypes.data
        self.ctx[0]["num_positions"] = self.positions.size
        self.ctx[0]["backtest_commands"] = self.command_buffer.ctypes.data
        self._install_markets()
        self._bridges = {
            name: callback_bridge(getattr(self.strategy, name))
            for name in ("on_start", "on_tick", "on_filled", "on_order", "on_stop")
        }
        self.invoke("on_start")

    # -- market ---------------------------------------------------------------------------

    def _install_markets(self) -> None:
        # The facade reads markets through market_state_dtype; expose one record per leg.
        from titan_strategy.abi_v10 import market_state_dtype

        self.markets = np.zeros(2, dtype=market_state_dtype)
        self.ctx[0]["markets_ptr"] = self.markets.ctypes.data
        self.ctx[0]["num_markets"] = self.markets.size
        self.set_market()

    def set_market(self, left_bid=50.0, left_ask=50.2, right_bid=100.0, right_ask=100.2,
                   tick=0.1) -> None:
        self.markets[0]["best_bid"] = left_bid
        self.markets[0]["best_ask"] = left_ask
        self.markets[0]["best_bid_qty"] = 100.0
        self.markets[0]["best_ask_qty"] = 100.0
        self.markets[0]["tick_size"] = tick
        self.markets[0]["lot_size"] = 1.0
        self.markets[1]["best_bid"] = right_bid
        self.markets[1]["best_ask"] = right_ask
        self.markets[1]["best_bid_qty"] = 100.0
        self.markets[1]["best_ask_qty"] = 100.0
        self.markets[1]["tick_size"] = tick
        self.markets[1]["lot_size"] = 1.0

    def advance(self, steps: int = 1) -> int:
        self.now_ns += steps * NS
        self.ctx[0]["now"] = self.now_ns
        return self.now_ns

    # -- events ---------------------------------------------------------------------------

    def invoke(self, name: str) -> int:
        """Drive one handler through the real callback bridge the runtime uses."""

        return self._bridges[name](ctypes.c_void_p(self.ctx.ctypes.data))

    def tick(self):
        self.advance()
        self.invoke("on_tick")

    def fill(self, order_id, qty, price=None, status=None, cumulative=None):
        """Feed one fill record.

        ``status`` is accepted for readability only: the ABI fill record has no status field, so
        the strategy derives completion from the cumulative quantity reaching the order quantity.
        ``cumulative`` defaults to "local filled + qty"; pass it explicitly to replay duplicate or
        regressing venue reports.
        """

        self.advance()
        index = self.order_index(order_id)
        if cumulative is None:
            cumulative = qty if index < 0 else self.order_filled(index) + qty
        record = self.fills[0]
        record["order_id"] = order_id
        record["last_fill_qty"] = float(qty)
        record["cumulative_filled_qty"] = float(cumulative)
        record["price"] = float(price if price is not None else 0.0)
        record["exch_ts"] = self.now_ns
        record["side"] = self.leg_side(order_id)
        self.ctx[0]["fills_ptr"] = self.fills.ctypes.data
        self.ctx[0]["num_fills"] = 1
        self.invoke("on_filled")

    def order_event(self, order_id, status):
        self.advance()
        record = self.orders[0]
        record["order_id"] = order_id
        record["status"] = int(status)
        record["exch_ts"] = self.now_ns
        self.ctx[0]["orders_ptr"] = self.orders.ctypes.data
        self.ctx[0]["num_orders"] = 1
        self.invoke("on_order")

    def stop(self):
        self.invoke("on_stop")

    # -- inspection -----------------------------------------------------------------------

    @property
    def slot_id(self):
        return int(self.state_i64[L.I_SLOT_ID])

    @property
    def posture(self):
        return int(self.state_i64[L.I_POSTURE])

    @property
    def status(self):
        return int(self.state_i64[L.I_PAIR_STATUS])

    @property
    def initiator_order_id(self):
        return int(self.state_i64[L.I_SLOT_INIT_ORDER_ID])

    @property
    def hedge_order_id(self):
        return int(self.state_i64[L.I_SLOT_HEDGE_ORDER_ID])

    @property
    def init_filled(self):
        return float(self.state[L.F_SLOT_INIT_FILLED_LOTS])

    @property
    def hedge_filled(self):
        return float(self.state[L.F_SLOT_HEDGE_FILLED_LOTS])

    @property
    def command_count(self):
        return int(self.command_buffer[0]["num_commands"])

    def command(self, index):
        return self.commands[index]

    def order_index(self, order_id):
        for index in range(L.MAX_ACTIVE_ORDERS):
            if self.state_i64[L.i_order_field(index, L.I_ORDER_ID)] == order_id:
                return index
        return -1

    def order_status(self, order_id):
        index = self.order_index(order_id)
        return 0 if index < 0 else int(self.state_i64[L.i_order_field(index, L.I_ORDER_STATUS)])

    def order_filled(self, index):
        return float(self.state[L.f_order_filled(index)])

    def leg_side(self, order_id):
        index = self.order_index(order_id)
        if index < 0:
            return 0
        return int(self.state_i64[L.i_order_field(index, L.I_ORDER_SIDE)])

    def last_new_order(self, asset_no: Optional[int] = None):
        found = None
        for index in range(self.command_count):
            command = self.commands[index]
            if command["kind"] != 1:
                continue
            if asset_no is not None and command["asset_no"] != asset_no:
                continue
            found = command
        return found

    def history_entries(self):
        entries = []
        for slot in range(int(self.state_i64[L.I_HISTORY_COUNT])):
            entries.append({
                "order_id": int(self.state_i64[L.i_history_field(slot, L.I_HIST_ORDER_ID)]),
                "slot_id": int(self.state_i64[L.i_history_field(slot, L.I_HIST_SLOT_ID)]),
                "role": int(self.state_i64[L.i_history_field(slot, L.I_HIST_ROLE)]),
                "status": int(self.state_i64[L.i_history_field(slot, L.I_HIST_STATUS)]),
            })
        return entries


__all__ = ["L", "NS", "NumbaHarness", "default_parameters", "pair_arb_numba"]
