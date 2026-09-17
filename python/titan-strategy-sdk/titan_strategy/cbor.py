"""Small RFC 8949 deterministic-CBOR codec used by strategy artifacts.

Only the closed set of manifest value types is supported.  Keeping the codec local avoids making
the production artifact verifier depend on a Python package that is unrelated to compilation.
"""

from __future__ import annotations

import struct
from typing import Any


class CborError(ValueError):
    pass


def _head(major: int, value: int) -> bytes:
    if value < 0:
        raise CborError("negative CBOR argument")
    if value < 24:
        return bytes([(major << 5) | value])
    if value <= 0xFF:
        return bytes([(major << 5) | 24, value])
    if value <= 0xFFFF:
        return bytes([(major << 5) | 25]) + value.to_bytes(2, "big")
    if value <= 0xFFFFFFFF:
        return bytes([(major << 5) | 26]) + value.to_bytes(4, "big")
    if value <= 0xFFFFFFFFFFFFFFFF:
        return bytes([(major << 5) | 27]) + value.to_bytes(8, "big")
    raise CborError("integer is outside CBOR uint64 range")


def dumps(value: object) -> bytes:
    if value is None:
        return b"\xf6"
    if value is False:
        return b"\xf4"
    if value is True:
        return b"\xf5"
    if isinstance(value, int):
        return _head(0, value) if value >= 0 else _head(1, -1 - value)
    if isinstance(value, float):
        return b"\xfb" + struct.pack(">d", value)
    if isinstance(value, bytes):
        return _head(2, len(value)) + value
    if isinstance(value, str):
        encoded = value.encode("utf-8")
        return _head(3, len(encoded)) + encoded
    if isinstance(value, (list, tuple)):
        return _head(4, len(value)) + b"".join(dumps(item) for item in value)
    if isinstance(value, dict):
        encoded = [(dumps(key), dumps(item)) for key, item in value.items()]
        encoded.sort(key=lambda pair: (len(pair[0]), pair[0]))
        return _head(5, len(encoded)) + b"".join(key + item for key, item in encoded)
    raise CborError(f"unsupported CBOR value {type(value).__name__}")


def _argument(data: bytes, offset: int, additional: int) -> tuple[int, int]:
    if additional < 24:
        return additional, offset
    widths = {24: 1, 25: 2, 26: 4, 27: 8}
    width = widths.get(additional)
    if width is None or offset + width > len(data):
        raise CborError("invalid or truncated CBOR argument")
    return int.from_bytes(data[offset:offset + width], "big"), offset + width


def _loads(data: bytes, offset: int) -> tuple[Any, int]:
    if offset >= len(data):
        raise CborError("truncated CBOR item")
    initial = data[offset]
    offset += 1
    major, additional = initial >> 5, initial & 31
    if major == 7:
        if additional == 20:
            return False, offset
        if additional == 21:
            return True, offset
        if additional == 22:
            return None, offset
        if additional == 27:
            if offset + 8 > len(data):
                raise CborError("truncated float64")
            return struct.unpack(">d", data[offset:offset + 8])[0], offset + 8
        raise CborError("unsupported CBOR simple value")
    size, offset = _argument(data, offset, additional)
    if major == 0:
        return size, offset
    if major == 1:
        return -1 - size, offset
    if major in (2, 3):
        if offset + size > len(data):
            raise CborError("truncated string")
        raw = data[offset:offset + size]
        return (raw if major == 2 else raw.decode("utf-8")), offset + size
    if major == 4:
        values = []
        for _ in range(size):
            item, offset = _loads(data, offset)
            values.append(item)
        return values, offset
    if major == 5:
        values = {}
        previous: tuple[int, bytes] | None = None
        for _ in range(size):
            key_start = offset
            key, offset = _loads(data, offset)
            encoded_key = data[key_start:offset]
            order = (len(encoded_key), encoded_key)
            if previous is not None and order <= previous:
                raise CborError("map keys are not in deterministic order")
            previous = order
            if key in values:
                raise CborError("duplicate map key")
            item, offset = _loads(data, offset)
            values[key] = item
        return values, offset
    raise CborError("unsupported CBOR major type")


def loads(data: bytes) -> object:
    if not isinstance(data, bytes):
        raise TypeError("CBOR input must be bytes")
    value, offset = _loads(data, 0)
    if offset != len(data):
        raise CborError("trailing bytes after CBOR value")
    return value


__all__ = ["CborError", "dumps", "loads"]
