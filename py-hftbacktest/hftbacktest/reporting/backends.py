from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .models import PreparedReport, RenderOutcome, ReportCapability, ReportConfig, ReportData


class BackendUnavailableError(RuntimeError):
    pass


class ReportAdapter(ABC):
    name: str
    capabilities: frozenset[ReportCapability]

    @abstractmethod
    def prepare(self, data: ReportData, config: ReportConfig) -> PreparedReport:
        raise NotImplementedError


class ReportRenderer(ABC):
    name: str
    capabilities: frozenset[ReportCapability]

    @abstractmethod
    def render(
        self,
        report: PreparedReport,
        output: Path,
        config: ReportConfig,
    ) -> Path | RenderOutcome:
        raise NotImplementedError


@dataclass(frozen=True)
class BackendDefinition:
    adapter: ReportAdapter
    renderer: ReportRenderer

    @property
    def capabilities(self) -> frozenset[ReportCapability]:
        return self.adapter.capabilities.intersection(self.renderer.capabilities)


class BackendRegistry:
    def __init__(self) -> None:
        self._backends: dict[str, BackendDefinition] = {}

    def register(
        self,
        name: str,
        adapter: ReportAdapter,
        renderer: ReportRenderer,
        *,
        replace: bool = False,
    ) -> None:
        if not name:
            raise ValueError("backend name must not be empty")
        if name in self._backends and not replace:
            raise ValueError(f"backend {name!r} is already registered")
        if adapter.name != name or renderer.name != name:
            raise ValueError("backend, adapter, and renderer names must match")
        self._backends[name] = BackendDefinition(adapter, renderer)

    def get(self, name: str) -> BackendDefinition:
        try:
            return self._backends[name]
        except KeyError as exc:
            raise ValueError(
                f"unknown report backend {name!r}; available={sorted(self._backends)}"
            ) from exc

    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self._backends))


class NativeAdapter(ReportAdapter):
    name = "native"
    capabilities = frozenset(
        {
            ReportCapability.RETURNS,
            ReportCapability.BENCHMARK,
            ReportCapability.DRAWDOWNS,
            ReportCapability.CALENDAR_RETURNS,
            ReportCapability.POSITIONS,
            ReportCapability.EXPOSURES,
            ReportCapability.FEES,
            ReportCapability.FUNDING,
            ReportCapability.SELF_CONTAINED_HTML,
        }
    )

    def prepare(self, data: ReportData, config: ReportConfig) -> PreparedReport:
        capabilities = set(self.capabilities)
        if data.bundle.benchmark is None:
            capabilities.discard(ReportCapability.BENCHMARK)
        if data.bundle.position_snapshots is None:
            capabilities.discard(ReportCapability.POSITIONS)
        return PreparedReport(self.name, frozenset(capabilities), data)


class QuantStatsAdapter(ReportAdapter):
    name = "quantstats"
    capabilities = frozenset(
        {
            ReportCapability.RETURNS,
            ReportCapability.BENCHMARK,
            ReportCapability.DRAWDOWNS,
            ReportCapability.CALENDAR_RETURNS,
            ReportCapability.SELF_CONTAINED_HTML,
        }
    )

    def prepare(self, data: ReportData, config: ReportConfig) -> PreparedReport:
        try:
            import pandas as pd
        except ImportError as exc:
            raise BackendUnavailableError(
                "QuantStats reporting requires the 'reports' optional dependency"
            ) from exc
        periodic = data.periodic_returns
        if periodic.is_empty():
            raise ValueError("QuantStats requires at least one periodic return")
        index = pd.DatetimeIndex(periodic["session"].to_list(), tz=config.timezone)
        returns = pd.Series(periodic["return"].to_numpy(), index=index, name="Strategy")
        benchmark = None
        capabilities = set(self.capabilities)
        if "benchmark_return" in periodic.columns:
            benchmark = pd.Series(
                periodic["benchmark_return"].to_numpy(),
                index=index,
                name="Benchmark",
            ).dropna()
        else:
            capabilities.discard(ReportCapability.BENCHMARK)
        payload: dict[str, Any] = {
            "returns": returns,
            "benchmark": benchmark,
            "title": f"{data.bundle.metadata.strategy_id} Backtest Report",
        }
        return PreparedReport(self.name, frozenset(capabilities), payload)
