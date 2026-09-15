"""Shared deterministic harness for the ``pair_arb`` unit tests."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

def _repo_root(path: Path) -> Path:
    for parent in path.parents:
        if (parent / "strategies").is_dir() and (parent / "python").is_dir():
            return parent
    return path.parents[3]


PROJECT_ROOT = _repo_root(Path(__file__).resolve())
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))
if str(PROJECT_ROOT / "python" / "titan-strategy-sdk") not in sys.path:
    sys.path.append(str(PROJECT_ROOT / "python" / "titan-strategy-sdk"))

from strategies import pair_arb  # noqa: E402
from strategies.pair_arb.abi_v10 import (  # noqa: E402
    Direction,
    ExecutionMode,
    MarketView,
    OrderStatus,
    Role,
    Side,
    TickSnapshot,
    VenueOrderStatus,
)
from strategies.pair_arb.connector import ConnectorNormalizer, VenueFillReport  # noqa: E402
from strategies.pair_arb.context import slot_state  # noqa: E402
from strategies.pair_arb.sim import SimulatedBroker  # noqa: E402

NS = 1_000_000


def default_parameters(**overrides):
    parameters = {
        "pair_id": "pair-1",
        "symbol_left": "AAA",
        "symbol_right": "BBB",
        "broker_left": "venue_a",
        "broker_right": "venue_b",
        "direction": "LONG_SPREAD",
        "mode": "MAKER_TAKER",
        "hedge_ratio_abs": 1.0,
        "spread": 1.0,
        "requote_distance": 0.5,
        "cancel_timeout_ns": 1_000 * NS,
        "max_position": 30.0,
        "slot_unit": 10.0,
        "dust_threshold": 0.0,
        "cancel_retry_limit": 2,
        "taker_slippage_bps": 0.0,
        "emergency_slippage_bps": 0.0,
        "max_unhedged_qty_soft": 10.0,
        "max_unhedged_qty_hard": 20.0,
        "max_unhedged_ms_soft": 200,
        "max_unhedged_ms_hard": 2_000,
        "maker_stale_ms": 1_000,
        "taker_stale_ms": 500,
    }
    parameters.update(overrides)
    return parameters


class Harness:
    """Drives one ``pair_arb`` instance against a scripted broker and market."""

    def __init__(self, **overrides) -> None:
        self.now_ns = 1_000 * NS
        self.broker = overrides.pop("broker", None) or SimulatedBroker()
        self.strategy = pair_arb.build(default_parameters(**overrides), broker=self.broker)
        self.ctx = self.strategy.context
        self.engine = self.strategy.engine
        self.callbacks = self.strategy.callbacks
        self.normalizer = self.strategy.normalizer
        self.pair = self.strategy.pair
        self.ctx.tick_clock(self.now_ns)
        self.set_market()
        self.engine.mark_ready(self.ctx)

    # -- market ---------------------------------------------------------------------------

    def set_market(self, left_bid=50.0, left_ask=50.2, right_bid=100.0, right_ask=100.2,
                   tick=0.1, ts_ns: Optional[int] = None) -> None:
        ts = self.now_ns if ts_ns is None else ts_ns
        self.ctx.market_left = MarketView(
            best_bid=left_bid, best_ask=left_ask, tick_size=tick, lot_size=0.1, ts_ns=ts
        )
        self.ctx.market_right = MarketView(
            best_bid=right_bid, best_ask=right_ask, tick_size=tick, lot_size=0.1, ts_ns=ts
        )

    def advance(self, steps: int = 1) -> int:
        self.now_ns += steps * NS
        self.ctx.tick_clock(self.now_ns)
        return self.now_ns

    # -- entrypoints ----------------------------------------------------------------------

    async def tick(self, refresh_market: bool = True):
        self.advance()
        if refresh_market:
            self.set_market(
                left_bid=self.ctx.market_left.best_bid,
                left_ask=self.ctx.market_left.best_ask,
                right_bid=self.ctx.market_right.best_bid,
                right_ask=self.ctx.market_right.best_ask,
                ts_ns=self.now_ns,
            )
        return await self.callbacks.on_tick(
            TickSnapshot(
                left=self.ctx.market_left,
                right=self.ctx.market_right,
                ts_ns=self.now_ns,
            )
        )

    async def fill(self, order_id: int, qty: float, price: Optional[float] = None,
                   status: VenueOrderStatus = VenueOrderStatus.PARTIALLY_FILLED):
        order = self.pair.get_active(order_id)
        cumulative = qty if order is None else order.filled_qty + qty
        report = VenueFillReport(
            order_id=order_id,
            ts_ns=self.advance(),
            fill_qty=qty,
            cumulative_filled_qty=cumulative,
            fill_price=price if price is not None else (order.price if order else 0.0),
            status=status,
            sequence=self.advance(0),
            symbol=order.symbol if order else "",
            side=order.side if order else None,
        )
        outcomes = await self.callbacks.on_venue_fill(report)
        return outcomes[0]

    # -- inspection -----------------------------------------------------------------------

    def active(self, role: Role):
        return self.pair.get_active_for_role(self.pair.current_slot.slot_id, role)

    def initiator(self):
        return self.active(Role.INITIATOR)

    def hedge(self):
        return self.active(Role.HEDGE)

    @property
    def slot(self):
        return self.pair.current_slot

    def slot_state(self):
        return slot_state(self.pair)

    def orders_list(self):
        return self.ctx.orders_list

    def audit(self):
        return self.ctx.audit_records

    def reconcile_reasons(self):
        return [request.reason for request in self.ctx.reconcile_requests]

    async def start_slot(self):
        """Run one tick that opens the first slot and return the initiator order."""

        result = await self.tick()
        assert result.has("slot_started"), result
        return self.initiator()


__all__ = [
    "Harness",
    "NS",
    "ConnectorNormalizer",
    "Direction",
    "ExecutionMode",
    "MarketView",
    "OrderStatus",
    "Role",
    "Side",
    "TickSnapshot",
    "VenueOrderStatus",
    "default_parameters",
    "pair_arb",
]
