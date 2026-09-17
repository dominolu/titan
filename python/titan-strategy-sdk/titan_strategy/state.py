"""Typed state declarations and canonical schema support for Strategy ABI V13."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import re
from typing import Any

import numpy as np


int8 = np.dtype("i1")
uint8 = np.dtype("u1")
int16 = np.dtype("<i2")
uint16 = np.dtype("<u2")
int32 = np.dtype("<i4")
uint32 = np.dtype("<u4")
int64 = np.dtype("<i8")
uint64 = np.dtype("<u8")
float32 = np.dtype("<f4")
float64 = np.dtype("<f8")

_SCALARS = {
    ("i", 1), ("u", 1), ("i", 2), ("u", 2), ("i", 4), ("u", 4),
    ("i", 8), ("u", 8), ("f", 4), ("f", 8),
}
_FIELD_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")


class StateSchemaError(ValueError):
    """Raised when a dtype cannot safely be used as a V13 state blob."""


@dataclass(frozen=True)
class FixedArrayType:
    element: object
    length: int


@dataclass(frozen=True)
class StateFieldLayout:
    name: str
    offset: int
    dtype: np.dtype
    shape: tuple[int, ...]
    fields: tuple["StateFieldLayout", ...]


@dataclass(frozen=True)
class StateLayout:
    dtype: np.dtype
    itemsize: int
    alignment: int
    fields: tuple[StateFieldLayout, ...]


def _little_scalar(dtype: np.dtype, path: str) -> np.dtype:
    dtype = np.dtype(dtype)
    if dtype.fields is not None or dtype.subdtype is not None:
        raise StateSchemaError(f"{path}: expected a scalar state type")
    if dtype.hasobject or (dtype.kind, dtype.itemsize) not in _SCALARS:
        raise StateSchemaError(f"{path}: unsupported state scalar {dtype.str}")
    if dtype.itemsize > 1 and dtype.byteorder not in ("<", "=", "|"):
        raise StateSchemaError(f"{path}: state scalars must be little-endian")
    if dtype.itemsize > 1 and not np.little_endian and dtype.byteorder == "=":
        raise StateSchemaError(f"{path}: native-endian dtype is not little-endian")
    return dtype.newbyteorder("<") if dtype.itemsize > 1 else dtype


def _as_dtype(value: object, path: str) -> np.dtype:
    if isinstance(value, FixedArrayType):
        element = _as_dtype(value.element, f"{path}[]")
        return np.dtype((element, (value.length,)))
    try:
        dtype = np.dtype(value)
    except TypeError as exc:
        raise StateSchemaError(f"{path}: unsupported state type {value!r}") from exc
    if dtype.fields is None:
        return _little_scalar(dtype, path)
    _validate_dtype_tree(dtype, path)
    return dtype


def array(element: object, length: int) -> FixedArrayType:
    if isinstance(length, bool) or not isinstance(length, int) or length <= 0:
        raise StateSchemaError("array length must be a positive integer")
    _as_dtype(element, "array.element")
    return FixedArrayType(element=element, length=length)


def record(**fields: object) -> np.dtype:
    if not fields:
        raise StateSchemaError("record must contain at least one field")
    declarations: list[tuple[str, object]] = []
    for name, value in fields.items():
        if not _FIELD_NAME.fullmatch(name):
            raise StateSchemaError(f"invalid state field name {name!r}")
        declarations.append((name, _as_dtype(value, f"state.{name}")))
    dtype = np.dtype(declarations, align=True)
    _validate_dtype_tree(dtype, "state")
    return dtype


def _validate_dtype_tree(dtype: np.dtype, path: str) -> None:
    dtype = np.dtype(dtype)
    if dtype.hasobject:
        raise StateSchemaError(f"{path}: object fields are forbidden")
    if dtype.subdtype is not None:
        base, shape = dtype.subdtype
        if not shape or any(isinstance(n, bool) or int(n) <= 0 for n in shape):
            raise StateSchemaError(f"{path}: array shape must be fixed and positive")
        _validate_dtype_tree(base, f"{path}[]")
        return
    if dtype.fields is None:
        _little_scalar(dtype, path)
        return
    if dtype.names is None or not dtype.names:
        raise StateSchemaError(f"{path}: structured dtype must contain fields")
    occupied: list[tuple[int, int, str]] = []
    for name in dtype.names:
        if not _FIELD_NAME.fullmatch(name):
            raise StateSchemaError(f"{path}: invalid field name {name!r}")
        entry = dtype.fields[name]
        if len(entry) != 2:
            raise StateSchemaError(f"{path}.{name}: dtype titles are forbidden")
        child, offset = np.dtype(entry[0]), int(entry[1])
        if offset < 0 or offset + child.itemsize > dtype.itemsize:
            raise StateSchemaError(f"{path}.{name}: field is outside its record")
        for start, end, other in occupied:
            if offset < end and start < offset + child.itemsize:
                raise StateSchemaError(f"{path}.{name}: overlaps field {other}")
        occupied.append((offset, offset + child.itemsize, name))
        required_alignment = max(1, child.alignment)
        if offset % required_alignment:
            raise StateSchemaError(f"{path}.{name}: offset is not naturally aligned")
        _validate_dtype_tree(child, f"{path}.{name}")
    if not dtype.isalignedstruct:
        raise StateSchemaError(f"{path}: structured dtype must use align=True")


def _field_layout(name: str, dtype: np.dtype, offset: int) -> StateFieldLayout:
    shape: tuple[int, ...] = ()
    base = dtype
    if dtype.subdtype is not None:
        base, raw_shape = dtype.subdtype
        shape = tuple(int(value) for value in raw_shape)
    nested = ()
    if base.fields is not None and base.names is not None:
        nested = tuple(
            _field_layout(child, np.dtype(base.fields[child][0]), int(base.fields[child][1]))
            for child in base.names
        )
    return StateFieldLayout(name=name, offset=offset, dtype=base, shape=shape, fields=nested)


def validate_state_dtype(
    dtype: np.dtype,
    *,
    max_state_bytes: int,
    max_alignment: int,
) -> StateLayout:
    dtype = np.dtype(dtype)
    if dtype.fields is None or dtype.names is None:
        raise StateSchemaError("state: root dtype must be structured")
    if max_state_bytes <= 0 or max_alignment <= 0:
        raise ValueError("state limits must be positive")
    _validate_dtype_tree(dtype, "state")
    if dtype.itemsize <= 0 or dtype.itemsize > max_state_bytes:
        raise StateSchemaError(
            f"state size {dtype.itemsize} exceeds max_state_bytes {max_state_bytes}"
        )
    if dtype.alignment > max_alignment:
        raise StateSchemaError(
            f"state alignment {dtype.alignment} exceeds max_alignment {max_alignment}"
        )
    fields = tuple(
        _field_layout(name, np.dtype(dtype.fields[name][0]), int(dtype.fields[name][1]))
        for name in dtype.names
    )
    return StateLayout(dtype=dtype, itemsize=dtype.itemsize, alignment=dtype.alignment, fields=fields)


def new_state(dtype: np.dtype) -> np.ndarray:
    layout = validate_state_dtype(dtype, max_state_bytes=2**63 - 1, max_alignment=8)
    return np.zeros(1, dtype=layout.dtype)


def _scalar_name(dtype: np.dtype) -> str:
    dtype = _little_scalar(dtype, "schema")
    endian = "little" if dtype.itemsize > 1 else "not_applicable"
    return f"{dtype.kind}{dtype.itemsize}:{endian}"


def _describe_field(field: StateFieldLayout) -> dict[str, Any]:
    value: dict[str, Any] = {
        "name": field.name,
        "offset": field.offset,
        "itemsize": field.dtype.itemsize * int(np.prod(field.shape or (1,))),
        "alignment": field.dtype.alignment,
        "shape": list(field.shape),
    }
    if field.fields:
        value["kind"] = "record"
        value["fields"] = [_describe_field(child) for child in field.fields]
    else:
        value["kind"] = "scalar"
        value["type"] = _scalar_name(field.dtype)
    return value


def describe_state_schema(layout: StateLayout) -> dict[str, object]:
    return {
        "itemsize": layout.itemsize,
        "alignment": layout.alignment,
        "endian": "little",
        "fields": [_describe_field(field) for field in layout.fields],
    }


def canonical_state_schema(layout: StateLayout, *, schema_version: int) -> bytes:
    if isinstance(schema_version, bool) or not isinstance(schema_version, int) or schema_version <= 0:
        raise StateSchemaError("state schema version must be a positive integer")
    payload = {"schema_version": schema_version, **describe_state_schema(layout)}
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def state_schema_hash(canonical_schema: bytes) -> bytes:
    if not isinstance(canonical_schema, bytes):
        raise TypeError("canonical_schema must be bytes")
    return hashlib.sha256(canonical_schema).digest()


__all__ = [
    "FixedArrayType", "StateFieldLayout", "StateLayout", "StateSchemaError", "array",
    "canonical_state_schema", "describe_state_schema", "float32", "float64", "int8",
    "int16", "int32", "int64", "new_state", "record", "state_schema_hash", "uint8",
    "uint16", "uint32", "uint64", "validate_state_dtype",
]
