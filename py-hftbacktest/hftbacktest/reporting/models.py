from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
import re
from datetime import date
from typing import Any, Mapping

import polars as pl


SCHEMA_VERSION = 1
METRIC_SCHEMA_VERSION = 1


class ReportStatus(str, Enum):
    VALID = "valid"
    PARTIAL = "partial"
    INVALID = "invalid"
    FAILED = "failed"


class IssueSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class ReportCapability(str, Enum):
    RETURNS = "returns"
    BENCHMARK = "benchmark"
    DRAWDOWNS = "drawdowns"
    CALENDAR_RETURNS = "calendar_returns"
    POSITIONS = "positions"
    EXPOSURES = "exposures"
    FEES = "fees"
    FUNDING = "funding"
    SELF_CONTAINED_HTML = "self_contained_html"


class SectionAvailability(str, Enum):
    AVAILABLE = "available"
    PARTIAL = "partial"
    UNAVAILABLE = "unavailable"
    FAILED = "failed"


@dataclass(frozen=True)
class SectionState:
    section_id: str
    status: SectionAvailability
    reason: str | None = None
    source_fields: tuple[str, ...] = ()


@dataclass(frozen=True)
class ValidationIssue:
    code: str
    message: str
    severity: IssueSeverity
    table: str | None = None
    field: str | None = None
    count: int | None = None

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["severity"] = self.severity.value
        return value


@dataclass(frozen=True)
class ValidationResult:
    status: ReportStatus
    issues: tuple[ValidationIssue, ...] = ()

    @property
    def is_usable(self) -> bool:
        return self.status in (ReportStatus.VALID, ReportStatus.PARTIAL)

    def with_issue(self, issue: ValidationIssue) -> "ValidationResult":
        issues = self.issues + (issue,)
        return ValidationResult(status=status_from_issues(issues), issues=issues)


def status_from_issues(issues: tuple[ValidationIssue, ...] | list[ValidationIssue]) -> ReportStatus:
    if any(issue.severity == IssueSeverity.ERROR for issue in issues):
        return ReportStatus.INVALID
    if any(issue.severity == IssueSeverity.WARNING for issue in issues):
        return ReportStatus.PARTIAL
    return ReportStatus.VALID


@dataclass(frozen=True)
class ReportConfig:
    reporting_currency: str
    initial_capital: float
    calendar: str = "crypto_utc"
    timezone: str = "UTC"
    periods_per_year: int | None = None
    minimum_annualization_samples: int = 2
    day_cutoff_hour: int = 0
    trading_weekdays: tuple[int, ...] | None = None
    calendar_holidays: frozenset[str] = field(default_factory=frozenset)
    risk_free_rate: float = 0.0
    timestamp_unit: str = "ns"
    contract_size: float = 1.0
    asset_type: str = "linear"
    venue_id: str = "unknown"
    instrument_id: str = "asset-0"
    strict_backend: bool = False
    max_plot_points: int = 5_000
    max_mark_age_ns: int | None = None
    redact_fields: frozenset[str] = field(
        default_factory=lambda: frozenset({"api_key", "secret", "password", "token"})
    )

    def __post_init__(self) -> None:
        if not self.reporting_currency:
            raise ValueError("reporting_currency must not be empty")
        if not (self.initial_capital > 0.0):
            raise ValueError("initial_capital must be positive")
        if self.periods_per_year is not None and self.periods_per_year <= 0:
            raise ValueError("periods_per_year must be positive")
        if self.minimum_annualization_samples < 2:
            raise ValueError("minimum_annualization_samples must be at least 2")
        if not 0 <= self.day_cutoff_hour <= 23:
            raise ValueError("day_cutoff_hour must be between 0 and 23")
        if self.trading_weekdays is not None:
            if not self.trading_weekdays or any(
                day < 0 or day > 6 for day in self.trading_weekdays
            ):
                raise ValueError("trading_weekdays must contain weekday numbers from 0 to 6")
            if len(set(self.trading_weekdays)) != len(self.trading_weekdays):
                raise ValueError("trading_weekdays must not contain duplicates")
        for holiday in self.calendar_holidays:
            try:
                date.fromisoformat(holiday)
            except ValueError as exc:
                raise ValueError(
                    f"calendar_holidays contains invalid ISO date: {holiday!r}"
                ) from exc
        if self.contract_size <= 0.0:
            raise ValueError("contract_size must be positive")
        if self.asset_type not in {"linear", "inverse"}:
            raise ValueError("asset_type must be 'linear' or 'inverse'")
        if self.max_mark_age_ns is not None and self.max_mark_age_ns < 0:
            raise ValueError("max_mark_age_ns must be non-negative")
        if self.max_plot_points < 100:
            raise ValueError("max_plot_points must be at least 100")

    @property
    def annualization(self) -> int:
        if self.periods_per_year is not None:
            return self.periods_per_year
        return 365 if self.calendar == "crypto_utc" else 252


@dataclass(frozen=True)
class RunMetadata:
    schema_version: int = SCHEMA_VERSION
    run_id: str = "unknown"
    run_fingerprint: str = "unknown"
    engine_version: str = "unknown"
    git_revision: str = "unknown"
    strategy_id: str = "unknown"
    strategy_version: str = "unknown"
    runtime_abi_version: int | None = None
    phase_contract_version: int | None = None
    data_manifest_hash: str = "unknown"
    config_hash: str = "unknown"
    random_seed: int | None = None
    start_exchange_ts: int | None = None
    end_exchange_ts: int | None = None
    start_delivery_ts: int | None = None
    end_delivery_ts: int | None = None
    reporting_currency: str = ""
    timezone: str = "UTC"
    reporting_calendar: str = "crypto_utc"
    initial_capital: float = 0.0
    termination: str = "unknown"
    end_policy: str = "unknown"
    strategy_parameters: Mapping[str, Any] = field(default_factory=dict)
    model_identities: Mapping[str, Any] = field(default_factory=dict)
    warnings: tuple[str, ...] = ()
    capability_downgrades: tuple[str, ...] = ()
    order_count: int | None = None
    fill_count: int | None = None
    round_trip_count: int | None = None
    reject_count: int | None = None
    cancel_count: int | None = None
    expire_count: int | None = None
    market_event_count: int | None = None
    wall_time_ns: int | None = None
    cpu_time_ns: int | None = None
    extras: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any], config: ReportConfig) -> "RunMetadata":
        known = {item.name for item in cls.__dataclass_fields__.values()}
        kwargs = {key: item for key, item in value.items() if key in known and key != "extras"}
        kwargs.setdefault("reporting_currency", config.reporting_currency)
        kwargs.setdefault("timezone", config.timezone)
        kwargs.setdefault("reporting_calendar", config.calendar)
        kwargs.setdefault("initial_capital", config.initial_capital)
        kwargs["extras"] = {
            key: item for key, item in value.items() if key not in known
        } | dict(value.get("extras", {}))
        return cls(**kwargs)

    def to_dict(self, redact_fields: frozenset[str] = frozenset()) -> dict[str, Any]:
        value = asdict(self)

        def normalized_key(key: str) -> str:
            value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", key).casefold()
            return re.sub(r"[^a-z0-9]+", "", value)

        sensitive_patterns = {
            normalized_key(item) for item in redact_fields if item
        }

        def is_sensitive(key: str) -> bool:
            normalized = normalized_key(key)
            return any(pattern in normalized for pattern in sensitive_patterns)

        def redact(item: Any) -> Any:
            if isinstance(item, dict):
                return {
                    key: "[REDACTED]" if is_sensitive(str(key)) else redact(child)
                    for key, child in item.items()
                }
            if isinstance(item, (list, tuple)):
                return [redact(child) for child in item]
            return item

        return redact(value)


@dataclass(frozen=True)
class ReportBundle:
    metadata: RunMetadata
    portfolio_snapshots: pl.DataFrame
    account_snapshots: pl.DataFrame | None = None
    position_snapshots: pl.DataFrame | None = None
    benchmark: pl.DataFrame | None = None
    fill_events: pl.DataFrame | None = None
    order_events: pl.DataFrame | None = None
    round_trip_events: pl.DataFrame | None = None
    fx_marks: pl.DataFrame | None = None
    risk_events: pl.DataFrame | None = None
    market_marks: pl.DataFrame | None = None
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != SCHEMA_VERSION:
            raise ValueError(
                f"unsupported ReportBundle schema_version={self.schema_version}; "
                f"expected {SCHEMA_VERSION}"
            )
        object.__setattr__(self, "portfolio_snapshots", self.portfolio_snapshots.clone())
        for name in (
            "account_snapshots",
            "position_snapshots",
            "benchmark",
            "fill_events",
            "order_events",
            "round_trip_events",
            "fx_marks",
            "risk_events",
            "market_marks",
        ):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, value.clone())

    def tables(self) -> dict[str, pl.DataFrame]:
        values = {"portfolio_snapshots": self.portfolio_snapshots.clone()}
        for name in (
            "account_snapshots",
            "position_snapshots",
            "benchmark",
            "fill_events",
            "order_events",
            "round_trip_events",
            "fx_marks",
            "risk_events",
            "market_marks",
        ):
            value = getattr(self, name)
            if value is not None:
                values[name] = value.clone()
        return values


@dataclass(frozen=True)
class MetricValue:
    metric_id: str
    value: float | int | str | None
    unit: str
    provider: str = "titan"
    version: int = METRIC_SCHEMA_VERSION
    parameters: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ReportData:
    bundle: ReportBundle
    validation: ValidationResult
    periodic_returns: pl.DataFrame
    drawdowns: pl.DataFrame
    monthly_returns: pl.DataFrame
    annual_returns: pl.DataFrame
    weekday_returns: pl.DataFrame
    hourly_returns: pl.DataFrame
    metrics: Mapping[str, MetricValue]
    sections: Mapping[str, SectionState] = field(default_factory=dict)


@dataclass(frozen=True)
class PreparedReport:
    provider: str
    capabilities: frozenset[ReportCapability]
    payload: Any
    issues: tuple[ValidationIssue, ...] = ()


@dataclass(frozen=True)
class ReportArtifact:
    path: Path
    status: ReportStatus
    provider: str
    metrics: Mapping[str, MetricValue]
    issues: tuple[ValidationIssue, ...] = ()
    requested_provider: str | None = None


@dataclass(frozen=True)
class RenderOutcome:
    path: Path
    issues: tuple[ValidationIssue, ...] = ()
