"""Test strategy that deliberately writes to stdout while compiling."""

from types import SimpleNamespace

import numpy as np
from numba import njit


print("noise from strategy import")


def build(parameters):
    print("noise from strategy build")
    state = np.zeros(1, dtype=np.float64)
    state_i64 = np.zeros(1, dtype=np.int64)

    @njit
    def on_bar(s):
        s.state[0] += 1.0

    return SimpleNamespace(
        strategy_id="noisy",
        strategy_version="1.0.0",
        on_bar=on_bar,
        state=state,
        state_i64=state_i64,
        metadata={},
    )
