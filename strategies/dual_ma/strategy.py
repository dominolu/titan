"""Fixed-memory dual moving average strategy for the Titan callback ABI."""

from types import SimpleNamespace

import numpy as np
from numba import njit


def build(parameters):
    fast = int(parameters.get("fast", 2))
    slow = int(parameters.get("slow", 4))
    if fast <= 0 or slow <= fast:
        raise ValueError("dual_ma requires 0 < fast < slow")

    state = np.zeros(4 + slow, dtype=np.float64)
    state_i64 = np.zeros(2, dtype=np.int64)  # ring cursor, observed count

    @njit
    def on_bar(s):
        bars = s.bars()
        if len(bars) == 0:
            return
        close = bars[0]["bar"]["close"]
        cursor = s.state_i64[0]
        count = s.state_i64[1]
        if count >= slow:
            s.state[1] -= s.state[4 + cursor]
        if count >= fast:
            fast_out = (cursor + slow - fast) % slow
            s.state[0] -= s.state[4 + fast_out]
        s.state[4 + cursor] = close
        s.state[0] += close
        s.state[1] += close
        next_count = count + 1
        fast_count = min(next_count, fast)
        slow_count = min(next_count, slow)
        s.state[2] = s.state[0] / fast_count
        s.state[3] = s.state[1] / slow_count
        s.state_i64[0] = (cursor + 1) % slow
        s.state_i64[1] = next_count

    return SimpleNamespace(
        strategy_id="dual_ma",
        strategy_version="1.0.0",
        on_bar=on_bar,
        state=state,
        state_i64=state_i64,
        metadata={"fast": fast, "slow": slow},
    )
