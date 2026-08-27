from types import SimpleNamespace

import numpy as np
from numba import njit


def build(_parameters):
    state = np.zeros(4, dtype=np.float64)

    @njit
    def on_tick(s):
        s.state[0] += len(s.ticks())
        s.state[1] += 1

    @njit
    def on_bar(s):
        s.state[2] += len(s.bars())
        s.state[3] += 1

    return SimpleNamespace(
        strategy_id="event_counter",
        strategy_version="1.0.0",
        on_tick=on_tick,
        on_bar=on_bar,
        state=state,
        state_i64=np.zeros(1, dtype=np.int64),
    )
