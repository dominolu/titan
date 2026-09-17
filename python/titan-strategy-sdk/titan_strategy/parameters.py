"""Typed strategy parameter declarations for Strategy ABI V13."""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any


_MISSING = object()


class ParameterError(ValueError):
    pass


def _check_name(name: str) -> None:
    if not isinstance(name, str) or not name or not name.isidentifier() or name.startswith("_"):
        raise ParameterError(f"invalid parameter name {name!r}")


@dataclass(frozen=True, init=False)
class Parameter:
    name: str
    required: bool
    default: object
    has_default: bool

    def _init(self, name: str, required: bool, default: object) -> None:
        _check_name(name)
        if not isinstance(required, bool):
            raise ParameterError("required must be bool")
        has_default = default is not _MISSING
        if required and has_default:
            raise ParameterError(f"{name}: a required parameter cannot have a default")
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "required", required)
        object.__setattr__(self, "default", None if not has_default else default)
        object.__setattr__(self, "has_default", has_default)

    def validate(self, value: object) -> object:
        raise NotImplementedError

    def json_schema(self) -> dict[str, object]:
        raise NotImplementedError

    def _with_default(self, schema: dict[str, object]) -> dict[str, object]:
        if self.has_default:
            schema["default"] = self.default
        return schema


@dataclass(frozen=True, init=False)
class FloatParam(Parameter):
    minimum: float | None
    maximum: float | None
    exclusive_minimum: bool
    exclusive_maximum: bool

    def __init__(
        self,
        name: str,
        *,
        required: bool = True,
        default: object = _MISSING,
        minimum: float | None = None,
        maximum: float | None = None,
        exclusive_minimum: bool = False,
        exclusive_maximum: bool = False,
    ) -> None:
        self._init(name, required, default)
        for label, bound in (("minimum", minimum), ("maximum", maximum)):
            if bound is not None and (isinstance(bound, bool) or not math.isfinite(float(bound))):
                raise ParameterError(f"{name}: {label} must be finite")
        if minimum is not None and maximum is not None and float(minimum) > float(maximum):
            raise ParameterError(f"{name}: minimum exceeds maximum")
        object.__setattr__(self, "minimum", None if minimum is None else float(minimum))
        object.__setattr__(self, "maximum", None if maximum is None else float(maximum))
        object.__setattr__(self, "exclusive_minimum", bool(exclusive_minimum))
        object.__setattr__(self, "exclusive_maximum", bool(exclusive_maximum))
        if self.has_default:
            self.validate(self.default)

    def validate(self, value: object) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ParameterError(f"{self.name}: expected number")
        result = float(value)
        if not math.isfinite(result):
            raise ParameterError(f"{self.name}: expected finite number")
        if self.minimum is not None and (
            result < self.minimum or (self.exclusive_minimum and result == self.minimum)
        ):
            raise ParameterError(f"{self.name}: below minimum")
        if self.maximum is not None and (
            result > self.maximum or (self.exclusive_maximum and result == self.maximum)
        ):
            raise ParameterError(f"{self.name}: above maximum")
        return result

    def json_schema(self) -> dict[str, object]:
        result: dict[str, object] = {"type": "number"}
        if self.minimum is not None:
            result["exclusiveMinimum" if self.exclusive_minimum else "minimum"] = self.minimum
        if self.maximum is not None:
            result["exclusiveMaximum" if self.exclusive_maximum else "maximum"] = self.maximum
        return self._with_default(result)


@dataclass(frozen=True, init=False)
class IntParam(Parameter):
    minimum: int | None
    maximum: int | None

    def __init__(self, name: str, *, required: bool = True, default: object = _MISSING,
                 minimum: int | None = None, maximum: int | None = None) -> None:
        self._init(name, required, default)
        if minimum is not None and (isinstance(minimum, bool) or not isinstance(minimum, int)):
            raise ParameterError(f"{name}: minimum must be int")
        if maximum is not None and (isinstance(maximum, bool) or not isinstance(maximum, int)):
            raise ParameterError(f"{name}: maximum must be int")
        if minimum is not None and maximum is not None and minimum > maximum:
            raise ParameterError(f"{name}: minimum exceeds maximum")
        object.__setattr__(self, "minimum", minimum)
        object.__setattr__(self, "maximum", maximum)
        if self.has_default:
            self.validate(self.default)

    def validate(self, value: object) -> int:
        if isinstance(value, bool) or not isinstance(value, int):
            raise ParameterError(f"{self.name}: expected integer")
        if self.minimum is not None and value < self.minimum:
            raise ParameterError(f"{self.name}: below minimum")
        if self.maximum is not None and value > self.maximum:
            raise ParameterError(f"{self.name}: above maximum")
        return value

    def json_schema(self) -> dict[str, object]:
        result: dict[str, object] = {"type": "integer"}
        if self.minimum is not None:
            result["minimum"] = self.minimum
        if self.maximum is not None:
            result["maximum"] = self.maximum
        return self._with_default(result)


@dataclass(frozen=True, init=False)
class EnumParam(Parameter):
    values: tuple[str, ...]

    def __init__(self, name: str, *, values: tuple[str, ...], required: bool = True,
                 default: object = _MISSING) -> None:
        self._init(name, required, default)
        normalized = tuple(values)
        if not normalized or any(not isinstance(value, str) or not value for value in normalized):
            raise ParameterError(f"{name}: enum values must be non-empty strings")
        if len(set(normalized)) != len(normalized):
            raise ParameterError(f"{name}: enum values must be unique")
        object.__setattr__(self, "values", normalized)
        if self.has_default:
            self.validate(self.default)

    def validate(self, value: object) -> str:
        if not isinstance(value, str) or value not in self.values:
            raise ParameterError(f"{self.name}: expected one of {self.values!r}")
        return value

    def json_schema(self) -> dict[str, object]:
        return self._with_default({"type": "string", "enum": list(self.values)})


@dataclass(frozen=True, init=False)
class BoolParam(Parameter):
    def __init__(self, name: str, *, required: bool = True, default: object = _MISSING) -> None:
        self._init(name, required, default)
        if self.has_default:
            self.validate(self.default)

    def validate(self, value: object) -> bool:
        if not isinstance(value, bool):
            raise ParameterError(f"{self.name}: expected boolean")
        return value

    def json_schema(self) -> dict[str, object]:
        return self._with_default({"type": "boolean"})


def parameter_json_schema(parameters: tuple[Parameter, ...]) -> dict[str, object]:
    properties = {parameter.name: parameter.json_schema() for parameter in parameters}
    required = [parameter.name for parameter in parameters if parameter.required]
    result: dict[str, object] = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "additionalProperties": False,
        "properties": properties,
    }
    if required:
        result["required"] = required
    return result


def validate_parameters(parameters: tuple[Parameter, ...], values: dict[str, object]) -> dict[str, object]:
    if not isinstance(values, dict):
        raise ParameterError("parameters must be an object")
    declarations = {parameter.name: parameter for parameter in parameters}
    unknown = sorted(set(values) - set(declarations))
    if unknown:
        raise ParameterError(f"unknown parameters: {', '.join(unknown)}")
    result: dict[str, object] = {}
    for parameter in parameters:
        if parameter.name in values:
            result[parameter.name] = parameter.validate(values[parameter.name])
        elif parameter.has_default:
            result[parameter.name] = parameter.validate(parameter.default)
        elif parameter.required:
            raise ParameterError(f"missing required parameter {parameter.name!r}")
    return result


__all__ = [
    "BoolParam", "EnumParam", "FloatParam", "IntParam", "Parameter", "ParameterError",
    "parameter_json_schema", "validate_parameters",
]
