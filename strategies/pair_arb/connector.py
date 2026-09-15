"""Connector-side normalization and interception for ``pair_arb``.

The kernel must never see a fact it cannot trust. Before a venue report is turned into a kernel
call, this module proves order ownership, converts cumulative quantities into increments and
rejects duplicates, regressions, jumps and over-sized fills (requirements §6.3, §6.6.2).
Everything it refuses goes to the host reconcile path instead of the kernel.

The checks read the kernel's fixed memory through :mod:`strategies.pair_arb.state_layout`; there is
no second copy of Pair/Slot state anywhere in the package.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from typing import Dict, Optional, Tuple

import numpy as np

from .events import (
    NormalizedFill,
    ReconcileReason,
    VenueFillReport,
    VenueOrderReport,
    VenueOrderStatus,
)
from .state_layout import (
    HISTORY_RING,
    I_HIST_ORDER_ID,
    I_HIST_STATUS,
    I_LEFT_ASSET_NO,
    I_ORDER_FILL_COUNT,
    I_ORDER_ID,
    I_ORDER_LAST_EVENT_TS,
    I_ORDER_QTY_INDEX,
    I_ORDER_ROLE,
    I_ORDER_SIDE,
    I_PAIR_DUST_LOTS_INDEX,
    I_RIGHT_ASSET_NO,
    MAX_ACTIVE_ORDERS,
    ROLE_INITIATOR,
    f_order_filled,
    f_order_qty,
    i_history_field,
    i_order_field,
)


class IntakeAction(IntEnum):
    """What the connector decided to do with one venue fact."""

    DELIVER = 1
    DROP = 2
    RECONCILE = 3


@dataclass
class IntakeDecision:
    """Outcome of normalizing one raw venue fact."""

    action: IntakeAction
    fill: Optional[NormalizedFill] = None
    order: Optional[VenueOrderReport] = None
    reason: Optional[ReconcileReason] = None
    text: str = ""

    @property
    def delivered(self) -> bool:
        return self.action is IntakeAction.DELIVER


class ConnectorNormalizer:
    """Validates venue facts against the kernel's fixed-memory order facts."""

    def __init__(self, state: np.ndarray, state_i64: np.ndarray, dust_lots: float = 0.0) -> None:
        self.state = state
        self.state_i64 = state_i64
        self.dust_lots = float(dust_lots)
        self._last_sequence: Dict[int, int] = {}

    # -- kernel views -----------------------------------------------------------------------

    def active_index(self, order_id: int) -> int:
        if order_id <= 0:
            return -1
        for index in range(MAX_ACTIVE_ORDERS):
            if self.state_i64[i_order_field(index, I_ORDER_ID)] == order_id:
                return index
        return -1

    def order_filled(self, index: int) -> float:
        return float(self.state[f_order_filled(index)])

    def order_qty(self, index: int) -> float:
        return float(self.state[f_order_qty(index)])

    def archived_status(self, order_id: int) -> int:
        for slot in range(HISTORY_RING):
            if self.state_i64[i_history_field(slot, I_HIST_ORDER_ID)] == order_id:
                return int(self.state_i64[i_history_field(slot, I_HIST_STATUS)])
        return 0

    def order_belongs_to_leg(self, index: int, asset_no: int) -> bool:
        if asset_no < 0:
            return True
        role = int(self.state_i64[i_order_field(index, I_ORDER_ROLE)])
        expected = int(self.state_i64[I_LEFT_ASSET_NO if role == ROLE_INITIATOR
                                       else I_RIGHT_ASSET_NO])
        return asset_no == expected

    # -- fills ------------------------------------------------------------------------------

    def normalize_fill(self, report: VenueFillReport) -> IntakeDecision:
        index = self.active_index(report.order_id)
        if index < 0:
            if self.archived_status(report.order_id):
                return self._reconcile(ReconcileReason.STALE_SLOT_FILL,
                                       f"fill for archived order {report.order_id}", report)
            return self._reconcile(ReconcileReason.UNKNOWN_ORDER,
                                   f"fill for unknown order {report.order_id}", report)

        if not self.order_belongs_to_leg(index, getattr(report, "asset_no", -1)):
            return self._reconcile(ReconcileReason.UNKNOWN_ORDER,
                                   f"fill asset does not match order {report.order_id}", report)
        if report.side and report.side != int(self.state_i64[i_order_field(index, I_ORDER_SIDE)]):
            return self._reconcile(ReconcileReason.UNKNOWN_ORDER,
                                   f"fill side does not match order {report.order_id}", report)

        filled = self.order_filled(index)
        qty = self.order_qty(index)
        delta = self._fill_delta(filled, report)
        if isinstance(delta, ReconcileReason):
            return self._reconcile(delta, "fill quantity cannot be confirmed", report)
        if delta is None:
            return IntakeDecision(action=IntakeAction.DROP,
                                  text=f"duplicate fill for order {report.order_id}")
        if delta <= 0.0:
            return self._reconcile(ReconcileReason.CUMULATIVE_REGRESSION,
                                   f"non-positive fill delta {delta}", report)
        if filled + delta > qty + self.dust_lots:
            return self._reconcile(ReconcileReason.FILL_EXCEEDS_ORDER,
                                   f"fills {filled + delta} exceed order quantity {qty}", report)

        last_sequence = self._last_sequence.get(report.order_id, -1)
        if report.sequence and report.sequence < last_sequence:
            return self._reconcile(ReconcileReason.DUPLICATE_OR_OUT_OF_ORDER,
                                   f"sequence {report.sequence} is older than {last_sequence}",
                                   report)
        self._last_sequence[report.order_id] = max(last_sequence, report.sequence)
        return IntakeDecision(
            action=IntakeAction.DELIVER,
            fill=NormalizedFill(
                order_id=report.order_id,
                fill_qty=delta,
                fill_price=report.fill_price or float(
                    self.state_i64[i_order_field(index, I_ORDER_LAST_EVENT_TS)] and 0.0
                ),
                event_ts_ns=report.ts_ns,
                cumulative_filled_qty=filled + delta,
                venue_order_id=report.venue_order_id,
                sequence=report.sequence,
            ),
        )

    @staticmethod
    def _fill_delta(filled: float, report: VenueFillReport):
        """Return the incremental quantity, ``None`` for a duplicate, or a reconcile reason."""

        cumulative = report.cumulative_filled_qty
        reported = report.fill_qty
        if cumulative is None and reported is None:
            return ReconcileReason.FILL_WITHOUT_QUANTITY
        if reported is not None and reported <= 0.0 and cumulative is None:
            return ReconcileReason.FILL_WITHOUT_QUANTITY
        if cumulative is not None:
            derived = float(cumulative) - filled
            if derived < 0.0:
                return ReconcileReason.CUMULATIVE_REGRESSION
            if derived == 0.0:
                return None
            if reported is not None and abs(float(reported) - derived) > 1e-9 * max(1.0, derived):
                return ReconcileReason.CUMULATIVE_JUMP
            return derived
        return float(reported)

    # -- order events -----------------------------------------------------------------------

    def normalize_order_event(self, report: VenueOrderReport) -> IntakeDecision:
        index = self.active_index(report.order_id)
        if index < 0:
            if self.archived_status(report.order_id):
                return IntakeDecision(
                    action=IntakeAction.DROP,
                    text=f"duplicate terminal report for archived order {report.order_id}",
                )
            return self._reconcile(ReconcileReason.UNKNOWN_ORDER,
                                   f"order event for unknown order {report.order_id}", report)
        return IntakeDecision(action=IntakeAction.DELIVER, order=report)

    # -- helpers ----------------------------------------------------------------------------

    def _reconcile(self, reason: ReconcileReason, text: str, report) -> IntakeDecision:
        return IntakeDecision(action=IntakeAction.RECONCILE, reason=reason, text=text)


_ = (I_ORDER_FILL_COUNT, I_ORDER_QTY_INDEX, I_PAIR_DUST_LOTS_INDEX, Tuple)
