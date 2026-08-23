"""Reusable single-argument Numba event strategies."""

from .dual_ma import DualMaStrategy, create_dual_ma_strategy, init

__all__ = ["DualMaStrategy", "create_dual_ma_strategy", "init"]
