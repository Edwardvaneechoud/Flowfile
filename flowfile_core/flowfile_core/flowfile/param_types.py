"""Shared typed-parameter primitives for flow parameters and the flow-API layer.

Lowest layer of the parameter stack — imports nothing from flowfile_core so
both ``schemas.schemas`` and ``schemas.input_schema`` can depend on it.
"""

import json
from typing import Literal

from pydantic import BaseModel, model_validator

ParamType = Literal["string", "integer", "float", "boolean", "enum"]
ParamValue = str | int | float | bool

_TRUE_VALUES = frozenset({"true", "1", "yes", "on"})
_FALSE_VALUES = frozenset({"false", "0", "no", "off"})


def coerce_param_value(param_type: ParamType, raw: str, enum_values: list[str] | None = None) -> ParamValue:
    """Coerce *raw* to the Python value for *param_type*.

    Raises ValueError with a parameter-name-free message; callers add context.
    """
    if param_type == "integer":
        try:
            return int(raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"'{raw}' is not a valid integer") from exc
    if param_type == "float":
        try:
            return float(raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"'{raw}' is not a valid number") from exc
    if param_type == "boolean":
        low = str(raw).strip().lower()
        if low in _TRUE_VALUES:
            return True
        if low in _FALSE_VALUES:
            return False
        raise ValueError(f"'{raw}' is not a valid boolean (true/false)")
    if param_type == "enum":
        if raw not in (enum_values or []):
            raise ValueError(f"'{raw}' is not one of {enum_values}")
        return raw
    return raw


def stringify_param_value(value: ParamValue) -> str:
    """Render a typed parameter value for string interpolation (bool -> true/false)."""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def render_param_as_expr_literal(value: ParamValue) -> str:
    """Render a typed value as a polars_expr_transformer literal for expression fields.

    Booleans -> bare ``true``/``false``; ints/floats -> bare number; strings and
    enum values -> a double-quoted, escaped literal (the parser ``eval``s string
    tokens, so ``json.dumps`` output is both valid and round-trip safe). Type is
    inferred from the runtime value, so a bare ``${param}`` resolves correctly for
    every parameter type without manual quoting.
    """
    if isinstance(value, bool):  # bool subclasses int; must be checked first
        return "true" if value else "false"
    if isinstance(value, (int | float)):
        return repr(value)
    return json.dumps(str(value))


class FlowParameter(BaseModel):
    """A single flow-level parameter that can be referenced via ${name} syntax.

    ``default_value`` stays a string for file-format stability; ``typed_default``
    yields the coerced Python value used for whole-field ``${name}`` injection.
    """

    name: str
    default_value: str = ""
    description: str = ""
    type: ParamType = "string"
    enum_values: list[str] | None = None

    @model_validator(mode="after")
    def _validate_typed_default(self) -> "FlowParameter":
        if self.type == "enum" and not self.enum_values:
            raise ValueError(f"parameter '{self.name}' is type 'enum' but has no enum_values")
        if self.default_value != "":
            try:
                coerce_param_value(self.type, self.default_value, self.enum_values)
            except ValueError as exc:
                raise ValueError(f"parameter '{self.name}': {exc}") from exc
        return self

    def typed_default(self) -> ParamValue:
        # TODO: empty default returns "" for any type; an int/float param then exports a wrong-typed default.
        if self.default_value == "":
            return ""
        try:
            return coerce_param_value(self.type, self.default_value, self.enum_values)
        except ValueError:
            return self.default_value
