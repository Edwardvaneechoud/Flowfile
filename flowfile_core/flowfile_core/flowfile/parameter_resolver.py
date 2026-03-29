"""Parameter resolution engine for Flowfile flows.

Resolves ${param_name} references in node settings at execution time.
"""

import re
from typing import Any

from pydantic import BaseModel

_PARAM_PATTERN = re.compile(r"\$\{([a-zA-Z_][a-zA-Z0-9_]*)\}")

# Type alias: list of (object, field_name_or_key_or_index, original_value) triples
# used to restore mutated fields after node execution.
_Restorations = list[tuple[Any, str | int, Any]]


def resolve_parameters(text: str, params: dict[str, str]) -> str:
    """Replace ${name} patterns in *text* with values from *params*.

    Unknown references are left unchanged.
    """
    if not params or "${" not in text:
        return text
    return _PARAM_PATTERN.sub(lambda m: params.get(m.group(1), m.group(0)), text)


# ---------------------------------------------------------------------------
# In-place mutation (used by _execute_single_node)
# ---------------------------------------------------------------------------


def apply_parameters_in_place(obj: Any, params: dict[str, str]) -> _Restorations:
    """Mutate *obj*'s string fields in place, substituting ${name} patterns.

    Returns a list of (target, field, original_value) triples so the caller
    can restore the originals after execution.  This preserves the identity of
    the settings object so that node closures that captured it at registration
    time automatically see the resolved values during execution.

    Raises:
        ValueError: If any ${...} references remain after substitution
                    (i.e. a parameter name not present in *params*).
    """
    if not params or obj is None:
        return []

    restorations: _Restorations = []
    _apply_recursive(obj, params, restorations)

    # Validate: no unresolved refs should remain
    unresolved = find_unresolved_in_model(obj)
    if unresolved:
        # Roll back before raising so the node is left in a clean state
        restore_parameters(restorations)
        raise ValueError(
            f"Unresolved parameter references in node settings: {sorted(unresolved)}. "
            "Check that all referenced parameters are defined on the flow."
        )

    return restorations


def restore_parameters(restorations: _Restorations) -> None:
    """Restore original field values from the list returned by *apply_parameters_in_place*."""
    for obj, field, original in restorations:
        if isinstance(obj, BaseModel):
            object.__setattr__(obj, field, original)
        elif isinstance(obj, (dict | list)):
            obj[field] = original


def _apply_recursive(obj: Any, params: dict[str, str], restorations: _Restorations) -> None:
    if isinstance(obj, BaseModel):
        for field_name in obj.model_fields:
            value = getattr(obj, field_name, None)
            if isinstance(value, str):
                if "${" in value:
                    resolved = resolve_parameters(value, params)
                    if resolved != value:
                        restorations.append((obj, field_name, value))
                        object.__setattr__(obj, field_name, resolved)
            elif isinstance(value, BaseModel):
                _apply_recursive(value, params, restorations)
            elif isinstance(value, dict):
                _apply_recursive(value, params, restorations)
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    if isinstance(item, BaseModel):
                        _apply_recursive(item, params, restorations)
                    elif isinstance(item, str) and "${" in item:
                        resolved = resolve_parameters(item, params)
                        if resolved != item:
                            restorations.append((value, i, item))
                            value[i] = resolved
    elif isinstance(obj, dict):
        for key, value in obj.items():
            if isinstance(value, str) and "${" in value:
                resolved = resolve_parameters(value, params)
                if resolved != value:
                    restorations.append((obj, key, value))
                    obj[key] = resolved
            elif isinstance(value, (BaseModel | dict)):
                _apply_recursive(value, params, restorations)


def find_unresolved_in_model(obj: Any) -> set[str]:
    """Return parameter names that still appear as ${...} in *obj* after substitution."""
    found: set[str] = set()
    if isinstance(obj, BaseModel):
        for field_name in obj.model_fields:
            found |= find_unresolved_in_model(getattr(obj, field_name, None))
    elif isinstance(obj, str):
        for m in _PARAM_PATTERN.finditer(obj):
            found.add(m.group(1))
    elif isinstance(obj, dict):
        for v in obj.values():
            found |= find_unresolved_in_model(v)
    elif isinstance(obj, list):
        for item in obj:
            found |= find_unresolved_in_model(item)
    return found


# ---------------------------------------------------------------------------
# Legacy helper kept for tests / external callers
# ---------------------------------------------------------------------------


def _walk_and_resolve(obj: Any, params: dict[str, str]) -> Any:
    if isinstance(obj, str):
        return resolve_parameters(obj, params)
    if isinstance(obj, dict):
        return {k: _walk_and_resolve(v, params) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_walk_and_resolve(item, params) for item in obj]
    return obj


def _find_unresolved(obj: Any) -> set[str]:
    found: set[str] = set()
    if isinstance(obj, str):
        for m in _PARAM_PATTERN.finditer(obj):
            found.add(m.group(1))
    elif isinstance(obj, dict):
        for v in obj.values():
            found |= _find_unresolved(v)
    elif isinstance(obj, list):
        for item in obj:
            found |= _find_unresolved(item)
    return found


def resolve_node_settings(setting_input: Any, params: dict[str, str]) -> Any:
    """Return a NEW instance of *setting_input* with all ${...} references resolved.

    Kept for backwards compatibility and unit tests.  The execution engine
    now uses *apply_parameters_in_place* / *restore_parameters* instead so
    that node closures which captured the original settings object see the
    resolved values.
    """
    if not params or setting_input is None:
        return setting_input
    if not isinstance(setting_input, BaseModel):
        return setting_input

    raw = setting_input.model_dump()
    resolved = _walk_and_resolve(raw, params)

    unresolved = _find_unresolved(resolved)
    if unresolved:
        raise ValueError(
            f"Unresolved parameter references in node settings: {sorted(unresolved)}. "
            "Check that all referenced parameters are defined on the flow."
        )

    return type(setting_input).model_validate(resolved)
