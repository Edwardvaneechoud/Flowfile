"""Parameter resolution engine for Flowfile flows.

Resolves ${param_name} references in node settings at execution time.
"""

import re
from typing import Any

from pydantic import BaseModel

_PARAM_PATTERN = re.compile(r"\$\{([a-zA-Z_][a-zA-Z0-9_]*)\}")


def resolve_parameters(text: str, params: dict[str, str]) -> str:
    """Replace ${name} patterns in *text* with values from *params*.

    Unknown references are left unchanged.
    """
    if not params or "${" not in text:
        return text
    return _PARAM_PATTERN.sub(lambda m: params.get(m.group(1), m.group(0)), text)


def _walk_and_resolve(obj: Any, params: dict[str, str]) -> Any:
    """Recursively walk a plain-Python object and resolve parameter references."""
    if isinstance(obj, str):
        return resolve_parameters(obj, params)
    if isinstance(obj, dict):
        return {k: _walk_and_resolve(v, params) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_walk_and_resolve(item, params) for item in obj]
    return obj


def resolve_node_settings(setting_input: Any, params: dict[str, str]) -> Any:
    """Return a new instance of *setting_input* with all ${...} references resolved.

    If *setting_input* is a Pydantic model, it is serialized to dict, all string
    values are substituted, and a new model instance is reconstructed.
    For non-Pydantic objects the original is returned unchanged.

    The original model is never mutated.

    Raises:
        ValueError: If any ${...} references remain unresolved after substitution.
    """
    if not params or setting_input is None:
        return setting_input

    if not isinstance(setting_input, BaseModel):
        return setting_input

    raw = setting_input.model_dump()
    resolved = _walk_and_resolve(raw, params)

    # Check for unresolved references
    unresolved = _find_unresolved(resolved)
    if unresolved:
        raise ValueError(
            f"Unresolved parameter references in node settings: {sorted(unresolved)}. "
            "Check that all referenced parameters are defined on the flow."
        )

    return type(setting_input).model_validate(resolved)


def _find_unresolved(obj: Any) -> set[str]:
    """Return the set of parameter names that remain unresolved."""
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
