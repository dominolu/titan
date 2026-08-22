"""Cold-path validation for self-describing Bar and Tick data files."""

from dataclasses import dataclass
import json
from pathlib import Path


SCHEMA_VERSION = 1
DATA_KINDS = frozenset(("bar", "tick"))
BAR_SOURCES = frozenset(("canonical-local", "venue-native"))


@dataclass(frozen=True, slots=True)
class DataManifest:
    schema_version: int
    data_kind: str
    symbol: str
    venue: str
    timestamp_unit: str = "ns"
    interval_semantics: str | None = None
    timeframe_ns: int | None = None
    bar_source: str | None = None
    builder_version: str | None = None

    def validate(self) -> "DataManifest":
        if self.schema_version != SCHEMA_VERSION:
            raise ValueError(
                f"unsupported schema_version={self.schema_version}; expected {SCHEMA_VERSION}"
            )
        if self.data_kind not in DATA_KINDS:
            raise ValueError("data_kind must be explicitly 'bar' or 'tick'")
        if not self.symbol or not self.venue:
            raise ValueError("symbol and venue are required")
        if self.timestamp_unit != "ns":
            raise ValueError("timestamp_unit must be 'ns'")
        if self.data_kind == "bar":
            if self.interval_semantics != "[open, close)":
                raise ValueError("bar interval_semantics must be '[open, close)'")
            if self.timeframe_ns is None or self.timeframe_ns <= 0:
                raise ValueError("bar timeframe_ns must be positive")
            if self.bar_source not in BAR_SOURCES:
                raise ValueError(
                    "bar_source must be 'canonical-local' or 'venue-native'"
                )
            if self.bar_source == "canonical-local" and not self.builder_version:
                raise ValueError("canonical-local bars require builder_version")
        else:
            if self.timeframe_ns is not None or self.bar_source is not None:
                raise ValueError("tick manifests cannot declare timeframe_ns or bar_source")
        return self

    @classmethod
    def from_dict(cls, value: dict) -> "DataManifest":
        known = {field.name for field in cls.__dataclass_fields__.values()}
        unknown = set(value) - known
        if unknown:
            raise ValueError(f"unknown manifest fields: {sorted(unknown)}")
        try:
            return cls(**value).validate()
        except TypeError as error:
            raise ValueError(f"invalid data manifest: {error}") from error


def load_data_manifest(path) -> DataManifest:
    """Loads and validates a JSON sidecar before any hot-path data mapping occurs."""

    with Path(path).open("r", encoding="utf-8") as file:
        value = json.load(file)
    if not isinstance(value, dict):
        raise ValueError("data manifest root must be an object")
    return DataManifest.from_dict(value)
