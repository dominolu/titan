"""ABI v12 runtime entry: ``pair_arb.strategy:build``.

Thin host adapter over :mod:`strategies.pair_arb.kernel`. It only reshapes the shared kernel into
what ``titan_strategy.compiler`` expects for a Numba strategy package:

* ``@njit`` handlers that accept the ``Strategy`` facade, one per ABI callback slot;
* ``state`` / ``state_i64`` one-dimensional C-contiguous arrays;
* ``strategy_id`` / ``strategy_version`` / ``metadata``.

The coroutine host (:mod:`strategies.pair_arb.coroutine`) builds the *same* kernel and turns the
emitted commands into awaited broker calls, so both runtimes share one state machine.
"""

from __future__ import annotations

from types import SimpleNamespace

from .kernel import build_kernel


def build(parameters):
    """Build the ABI v12 strategy object from the shared kernel."""

    kernel = build_kernel(parameters)
    handlers = kernel.handlers
    return SimpleNamespace(
        strategy_id=kernel.strategy_id,
        strategy_version=kernel.strategy_version,
        on_start=handlers["on_start"],
        on_tick=handlers["on_tick"],
        on_filled=handlers["on_filled"],
        on_order=handlers["on_order"],
        on_stop=handlers["on_stop"],
        state=kernel.state,
        state_i64=kernel.state_i64,
        metadata=kernel.metadata,
    )
