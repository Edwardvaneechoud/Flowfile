"""Parameter resolution engine for Flowfile flows.

Resolves ${param_name} references in node settings at execution time.
"""

import ast
import io
import re
import tokenize
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from pydantic import BaseModel

from flowfile_core.flowfile.param_types import (
    ParamValue,
    render_param_as_expr_literal,
    stringify_param_value,
)
from flowfile_core.schemas.transform_schema import PolarsCodeInput

_PARAM_PATTERN = re.compile(r"\$\{([a-zA-Z_][a-zA-Z0-9_]*)\}")
_STRING_PREFIX = re.compile(r"[a-zA-Z]*")
# Python 3.12+ splits f-strings into tokens; a string nested in one stays raw like the f-string.
_FSTRING_START = getattr(tokenize, "FSTRING_START", None)
_FSTRING_END = getattr(tokenize, "FSTRING_END", None)

# Type alias: list of (object, field_name_or_key_or_index, original_value) triples
# used to restore mutated fields after node execution.
_Restorations = list[tuple[Any, str | int, Any]]


def resolve_parameters(text: str, params: dict[str, ParamValue]) -> str:
    """Replace ${name} patterns in *text* with values from *params*.

    Unknown references are left unchanged. Typed values are rendered with
    ``stringify_param_value`` (bool -> ``true``/``false``).
    """
    if not params or "${" not in text:
        return text
    return _PARAM_PATTERN.sub(
        lambda m: stringify_param_value(params[m.group(1)]) if m.group(1) in params else m.group(0),
        text,
    )


def resolve_expression_parameters(text: str, params: dict[str, ParamValue]) -> str:
    """Replace ${name} patterns with type-correct polars_expr_transformer literals.

    Used for expression fields (formula / advanced filter / dynamic-rename formula):
    strings and enums are rendered as double-quoted literals, numbers bare, bools as
    ``true``/``false`` (see ``render_param_as_expr_literal``). Unlike ``_substitute``
    this never does whole-field typed injection — the result is always a valid
    expression string, so a bare ``${name}`` works for any type and can be parsed.
    """
    if not params or "${" not in text:
        return text
    return _PARAM_PATTERN.sub(
        lambda m: render_param_as_expr_literal(params[m.group(1)]) if m.group(1) in params else m.group(0),
        text,
    )


def _render_code_string_literal(token: str, params: dict[str, ParamValue]) -> str | None:
    """The substituted ``repr`` of a plain string-literal token, or ``None`` to leave the token as it is."""
    if {"f", "b"} & set(_STRING_PREFIX.match(token).group(0).lower()):
        return None
    try:
        content = ast.literal_eval(token)
    except (ValueError, SyntaxError):
        return None
    if not isinstance(content, str):
        return None
    resolved = resolve_parameters(content, params)
    return repr(resolved) if resolved != content else None


def resolve_code_parameters(code: str, params: dict[str, ParamValue]) -> str:
    """Replace ${name} patterns in Python source (the Polars-code node) without letting a value become code.

    A reference inside a plain string literal (any quoting, no ``f``/``b`` prefix) re-renders that
    literal as the ``repr`` of its substituted content, so quotes, backslashes and newlines in a
    value stay part of the string. References anywhere else (bare code, comments, f-strings, byte
    strings) are substituted as raw text, as is all of *code* when it does not tokenize.
    """
    if not params or "${" not in code:
        return code
    try:
        tokens = list(tokenize.generate_tokens(io.StringIO(code).readline))
    except (tokenize.TokenError, SyntaxError):
        return resolve_parameters(code, params)

    line_starts = [0]
    for line in io.StringIO(code).readlines():
        line_starts.append(line_starts[-1] + len(line))

    pieces: list[str] = []
    cursor = 0
    fstring_depth = 0
    for token in tokens:
        if token.type == tokenize.ERRORTOKEN and token.string.startswith(("'", '"')):
            return resolve_parameters(code, params)
        if token.type == _FSTRING_START:
            fstring_depth += 1
        elif token.type == _FSTRING_END:
            fstring_depth -= 1
        if token.type != tokenize.STRING or fstring_depth or "${" not in token.string:
            continue
        literal = _render_code_string_literal(token.string, params)
        if literal is None:
            continue
        start = line_starts[token.start[0] - 1] + token.start[1]
        end = line_starts[token.end[0] - 1] + token.end[1]
        if code[start:end] != token.string:
            return resolve_parameters(code, params)
        pieces.append(resolve_parameters(code[cursor:start], params))
        pieces.append(literal)
        cursor = end
    pieces.append(resolve_parameters(code[cursor:], params))
    return "".join(pieces)


def _is_expression_field(model: BaseModel, field_name: str) -> bool:
    """Whether *field_name* on *model* is tagged as a raw-expression field."""
    extra = type(model).model_fields[field_name].json_schema_extra
    return isinstance(extra, dict) and extra.get("expression") is True


def _is_python_code_field(model: BaseModel, field_name: str) -> bool:
    """Whether *field_name* on *model* holds the Python source of a Polars-code node."""
    return isinstance(model, PolarsCodeInput) and field_name == "polars_code"


def _substitute(value: str, params: dict[str, ParamValue]) -> Any:
    """Resolve *value*: a whole-field ``${name}`` ref yields the typed value; embedded refs stringify."""
    m = _PARAM_PATTERN.fullmatch(value)
    if m and m.group(1) in params:
        return params[m.group(1)]
    return resolve_parameters(value, params)


# In-place mutation (used by _execute_single_node)


def apply_parameters_in_place(obj: Any, params: dict[str, ParamValue]) -> _Restorations:
    """Mutate *obj*'s string fields in place, substituting ${name} patterns.

    A field whose entire value is a single ``${name}`` reference receives the
    parameter's *typed* value (int/float/bool for typed parameters — assigned via
    ``object.__setattr__``, bypassing field validation, and restored afterwards);
    embedded references are stringified.

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


@contextmanager
def node_parameters_resolved(node: Any) -> Iterator[None]:
    """Substitute a node's flow ``${name}`` refs into its settings for the block.

    The same substitute/restore contract the run loop uses, with parameters taken
    from the node's own ``_params_getter``. ``_hash`` is pinned across the mutation
    so the restored settings don't look changed and trigger a spurious reset.

    Raises:
        ValueError: If a referenced parameter is not defined on the flow.
    """
    params_getter = getattr(node, "_params_getter", None)
    params = params_getter() if params_getter is not None else {}
    if not params:
        yield
        return

    saved_hash = node._hash
    restorations = apply_parameters_in_place(node.setting_input, params)
    try:
        yield
    finally:
        restore_parameters(restorations)
        node._hash = saved_hash


def _apply_recursive(
    obj: Any, params: dict[str, ParamValue], restorations: _Restorations, render_expressions: bool = True
) -> None:
    """Substitute ``${name}`` refs in *obj* in place.

    When *render_expressions* is True (runtime resolution) expression fields render
    typed literals via ``resolve_expression_parameters`` and Polars code renders its
    string literals via ``resolve_code_parameters``. When False (e.g. code-gen
    sentinel substitution) every field uses the raw ``_substitute`` path so the
    replacement text is inserted verbatim.
    """
    if isinstance(obj, BaseModel):
        for field_name in obj.model_fields:
            value = getattr(obj, field_name, None)
            if isinstance(value, str):
                if "${" in value:
                    if render_expressions and _is_expression_field(obj, field_name):
                        resolved = resolve_expression_parameters(value, params)
                    elif render_expressions and _is_python_code_field(obj, field_name):
                        resolved = resolve_code_parameters(value, params)
                    else:
                        resolved = _substitute(value, params)
                    if resolved != value:
                        restorations.append((obj, field_name, value))
                        object.__setattr__(obj, field_name, resolved)
            elif isinstance(value, BaseModel):
                _apply_recursive(value, params, restorations, render_expressions)
            elif isinstance(value, dict):
                _apply_recursive(value, params, restorations, render_expressions)
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    if isinstance(item, BaseModel):
                        _apply_recursive(item, params, restorations, render_expressions)
                    elif isinstance(item, str) and "${" in item:
                        resolved = _substitute(item, params)
                        if resolved != item:
                            restorations.append((value, i, item))
                            value[i] = resolved
    elif isinstance(obj, dict):
        for key, value in obj.items():
            if isinstance(value, str) and "${" in value:
                resolved = _substitute(value, params)
                if resolved != value:
                    restorations.append((obj, key, value))
                    obj[key] = resolved
            elif isinstance(value, (BaseModel | dict)):
                _apply_recursive(value, params, restorations, render_expressions)


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


# Legacy helper kept for tests / external callers


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
