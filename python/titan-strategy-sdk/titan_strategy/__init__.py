"""Titan strategy SDK: one ABI source, one facade, one callback bridge.

Module layering (imports only point downwards)::

    intrinsic.py  ->  abi_v10.py  ->  callbacks.py  ->  context.py  ->  strategies/*

* :mod:`titan_strategy.abi_v10` — the only definition of ABI dtypes, constants and layout checks.
* :mod:`titan_strategy.callbacks` — execution host calls, command encoding, callback bridges.
* :mod:`titan_strategy.context` — the ``Strategy`` facade strategies program against.

``intrinsic.py`` is an implementation leaf.  Its pointer helper is re-exported by ``abi_v10``
so the public dependency surface remains the three ABI/bridge/facade modules above.
"""

from . import abi_v10, callbacks, context
from .compiler import CompiledStrategy, compile_strategy
from .context import Strategy

__all__ = [
    "CompiledStrategy",
    "Strategy",
    "abi_v10",
    "callbacks",
    "compile_strategy",
    "context",
]
