"""Pydantic models for flow-level arguments.

Flow arguments allow parameterizing flows so that values like file paths,
column names, or filter thresholds can be set at run time — either from the
CLI, the API, or from a parent flow when the flow is used as a subflow node.
"""

import re
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

FlowArgType = Literal["string", "number", "boolean", "list"]

# Pattern that matches {{arg_name}} placeholders in code/expressions.
TEMPLATE_PATTERN = re.compile(r"\{\{(\w+)\}\}")


class FlowArgument(BaseModel):
    """A single typed argument for a flow."""

    name: str = Field(..., description="Unique argument name (snake_case recommended).")
    arg_type: FlowArgType = Field(default="string", description="The value type of the argument.")
    default: Any = Field(default=None, description="Default value used when no value is provided at run time.")
    description: str = Field(default="", description="Human-readable description shown in the UI.")
    required: bool = Field(default=False, description="Whether a value must be supplied at run time.")
    options: list[str] | None = Field(default=None, description="Optional fixed choices for dropdowns.")

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        if not re.match(r"^[a-zA-Z_]\w*$", v):
            raise ValueError(f"Argument name must be a valid identifier, got: {v!r}")
        return v


def coerce_arg_value(value: Any, arg_type: FlowArgType) -> Any:
    """Coerce a raw value to the expected type.

    Args:
        value: The raw value (often a string from CLI or frontend).
        arg_type: The expected FlowArgType.

    Returns:
        The coerced value.

    Raises:
        ValueError: If the value cannot be coerced.
    """
    if value is None:
        return None

    if arg_type == "string":
        return str(value)
    elif arg_type == "number":
        try:
            # Prefer int if the value looks integral
            if isinstance(value, str) and "." not in value:
                return int(value)
            return float(value)
        except (ValueError, TypeError) as e:
            raise ValueError(f"Cannot convert {value!r} to number") from e
    elif arg_type == "boolean":
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            if value.lower() in ("true", "1", "yes"):
                return True
            if value.lower() in ("false", "0", "no"):
                return False
        raise ValueError(f"Cannot convert {value!r} to boolean")
    elif arg_type == "list":
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        raise ValueError(f"Cannot convert {value!r} to list")
    else:
        return value


def resolve_flow_arguments(
    arg_definitions: list[FlowArgument],
    provided_values: dict[str, Any],
) -> dict[str, Any]:
    """Validate and resolve argument values against their definitions.

    Args:
        arg_definitions: The list of FlowArgument definitions from the flow.
        provided_values: User-supplied values (name → raw value).

    Returns:
        A dict mapping argument names to their resolved (type-coerced) values.

    Raises:
        ValueError: If a required argument is missing or type coercion fails.
    """
    resolved: dict[str, Any] = {}
    defined_names = {arg.name for arg in arg_definitions}

    # Warn about unknown arguments
    unknown = set(provided_values.keys()) - defined_names
    if unknown:
        raise ValueError(f"Unknown flow arguments: {', '.join(sorted(unknown))}")

    for arg in arg_definitions:
        if arg.name in provided_values:
            raw = provided_values[arg.name]
            resolved[arg.name] = coerce_arg_value(raw, arg.arg_type)
        elif arg.default is not None:
            resolved[arg.name] = coerce_arg_value(arg.default, arg.arg_type)
        elif arg.required:
            raise ValueError(f"Required flow argument '{arg.name}' was not provided")
        else:
            resolved[arg.name] = None

    return resolved


def substitute_template(text: str, arguments: dict[str, Any]) -> str:
    """Replace ``{{arg_name}}`` placeholders in *text* with resolved values.

    Unrecognised placeholders are left as-is.

    Args:
        text: The template string (e.g. code, expression, file path).
        arguments: Resolved argument name → value mapping.

    Returns:
        The string with placeholders replaced.
    """
    if not arguments:
        return text

    def _replacer(match: re.Match) -> str:
        name = match.group(1)
        if name in arguments and arguments[name] is not None:
            return str(arguments[name])
        return match.group(0)  # leave unresolved

    return TEMPLATE_PATTERN.sub(_replacer, text)


def apply_field_binding(obj: Any, field_path: str, value: Any) -> None:
    """Set a nested attribute on *obj* using a dot-separated path.

    For example, ``apply_field_binding(node, "received_file.path", "/new/path")``
    is equivalent to ``node.received_file.path = "/new/path"``.

    Args:
        obj: The root object.
        field_path: Dot-separated path to the target attribute.
        value: The value to set.

    Raises:
        AttributeError: If an intermediate attribute does not exist.
    """
    parts = field_path.split(".")
    target = obj
    for part in parts[:-1]:
        target = getattr(target, part)
    setattr(target, parts[-1], value)
