"""Public business observations and private, compile-time state projections.

These declarations never run in a trading callback. Rust validates the bindings
against the authenticated recovery layout and reads committed recovery points.
"""

from __future__ import annotations

from dataclasses import dataclass
import re


@dataclass(frozen=True)
class Equals:
    source: str
    value: int


@dataclass(frozen=True)
class ObservationField:
    name: str
    kind: str
    unit: str
    source: str
    label: str = ""
    description: str = ""
    enum_mapping: tuple[tuple[int, str], ...] = ()
    valid_when: Equals | None = None

    def __post_init__(self):
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", self.name):
            raise ValueError("observation field name must be lowercase snake_case")
        if self.kind not in {"uint64", "int64", "decimal", "boolean", "enum"}:
            raise ValueError("unknown observation field kind")
        if not self.unit or len(self.unit) > 64:
            raise ValueError("observation field needs a bounded explicit unit")
        if not re.fullmatch(r"[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)*", self.source):
            raise ValueError("observation source must be a scalar state path")
        mapping = tuple(tuple(item) for item in self.enum_mapping)
        if (self.kind == "enum") != bool(mapping):
            raise ValueError("enum fields require an explicit enum mapping")
        if len({key for key, _ in mapping}) != len(mapping):
            raise ValueError("duplicate observation enum source value")
        if any(isinstance(key, bool) or not isinstance(key, int) or not re.fullmatch(
            r"[a-z][a-z0-9_]{0,63}", value
        ) for key, value in mapping):
            raise ValueError("invalid observation enum mapping")
        if self.valid_when is not None and not isinstance(self.valid_when, Equals):
            raise TypeError("observation validity condition must be Equals")
        object.__setattr__(self, "enum_mapping", mapping)


@dataclass(frozen=True)
class BusinessObservationSpec:
    schema_id: str
    schema_version: int
    fields: tuple[ObservationField, ...]

    def __post_init__(self):
        if not re.fullmatch(r"[a-z][a-z0-9_.]{0,127}", self.schema_id):
            raise ValueError("invalid business observation schema id")
        if isinstance(self.schema_version, bool) or not 0 < self.schema_version <= 2**32 - 1:
            raise ValueError("business observation schema version must be positive u32")
        fields = tuple(self.fields)
        if not 1 <= len(fields) <= 64 or len({field.name for field in fields}) != len(fields):
            raise ValueError("business observation requires 1..64 unique fields")
        object.__setattr__(self, "fields", fields)

    def describe(self) -> dict[str, object]:
        # Source paths and validity bindings deliberately do not enter the public schema.
        return {
            "schema": {
                "schema_id": self.schema_id,
                "schema_version": self.schema_version,
                "fields": [{
                    "name": item.name, "kind": item.kind, "unit": item.unit,
                    "label": item.label, "description": item.description,
                    "nullable": item.valid_when is not None,
                    "enum_values": sorted({value for _, value in item.enum_mapping}),
                } for item in self.fields],
            },
            "projection": [{
                "name": item.name, "source": item.source,
                "enum_mapping": {str(key): value for key, value in item.enum_mapping},
                "valid_when": None if item.valid_when is None else {
                    "source": item.valid_when.source, "equals": str(item.valid_when.value),
                },
            } for item in self.fields],
        }


__all__ = ["BusinessObservationSpec", "Equals", "ObservationField"]
