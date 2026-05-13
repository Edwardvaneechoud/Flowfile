"""Tool-arg coercions applied at the executor seam.

These coercions fix the recoverable wrong-shape patterns smaller
open-weights models routinely emit. Each function preserves the
``tool_args`` shape on no-op so the caller can detect changes and so
the strict refusal path stays for genuinely-bad payloads.
"""

from __future__ import annotations

import json
import re
from typing import Any, Final

_CONNECTION_ID_ARROW_RE: Final = re.compile(
    r"^\s*(\d+)\s*(?:->|→|:)\s*(\d+)\s*$"
)


def _coerce_to_int_or_none(value: Any) -> int | None:
    """Best-effort coercion of a ``node_id``-shaped value to ``int``.

    The Pydantic validator at the ``add_*`` and ``connect`` paths runs in
    lax mode and silently coerces ``"5"`` → ``5``; the manually-validated
    paths (``delete_node`` / ``update_node_settings`` / ``read_node_*``)
    historically refused outright on any non-``int`` shape, which made the
    cross-tool experience inconsistent — the LLM would correct on one
    tool and immediately repeat the same mistake on another. This helper
    closes that gap: numeric-looking strings get coerced; truly bogus
    inputs (``"node_5"``, ``None``, lists) return ``None`` so the caller
    can refuse with a structured detail. ``bool`` is rejected explicitly
    because Python treats it as ``int`` subclass and ``True`` / ``False``
    would otherwise sneak through as ``1`` / ``0``.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _coerce_to_int_or_self(value: Any) -> Any:
    """Small helper for lenient ``int | null`` parsing.

    Returns the parsed int when ``value`` is a stringified int (``"4"``)
    or None when it's an empty string. Otherwise returns the original
    ``value`` unchanged so the caller's type check fires on the
    structurally-wrong shape rather than on a recoverable string. Booleans
    are passed through (caller rejects them — Python's ``isinstance(True, int)``
    is True so we explicitly avoid swallowing booleans here).
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped or stripped.lower() in ("null", "none"):
            return None
        try:
            return int(stripped)
        except (TypeError, ValueError):
            return value
    return value


def _coerce_to_int_list_or_self(value: Any) -> Any:
    """Coerce common llama-70b mis-shapes back to ``list[int]``.

    Accepts:

    * Native ``list`` of ints — returned unchanged (each element passes
      through ``_coerce_to_int_or_self`` so stringified ints inside the
      list are also recovered).
    * Single ``int`` — wrapped as ``[int]`` (the model emitted a scalar
      where the schema expected an array).
    * ``str`` parseable as JSON to a list or int — parsed and wrapped.
    * ``str`` of comma-separated ints (``"4, 5"``) — parsed.
    * Anything else — returned unchanged so the caller's type check
      surfaces the actual shape problem in the refusal_detail.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return [value]
    if isinstance(value, list):
        return [_coerce_to_int_or_self(item) for item in value]
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return []
        # Try JSON first — covers '[4]', '[4, 5]', '4', and '"4"'.
        try:
            parsed = json.loads(stripped)
        except (TypeError, ValueError):
            parsed = None
        if isinstance(parsed, list):
            return [_coerce_to_int_or_self(item) for item in parsed]
        if isinstance(parsed, int) and not isinstance(parsed, bool):
            return [parsed]
        # Fallback: comma-separated integers.
        try:
            return [int(part.strip()) for part in stripped.split(",") if part.strip()]
        except (TypeError, ValueError):
            return value
    return value


def _unwrap_json_string_values(value: Any) -> Any:
    """Recursively unwrap JSON-encoded string CONTAINERS in tool args.

    Smaller open-weights models routinely emit structured tool args as
    JSON-encoded strings rather than native objects / arrays. Two
    common failure modes:

    * ``upstream_node_ids: "[3, 4]"`` (the field is array<int>; model
      emits a string).
    * ``groupby_input: "{\\"agg_cols\\": [...]}"`` (an object field
      delivered as a JSON-string).

    Pydantic cannot reverse-coerce a JSON-string into a dict / list at
    validation time, so without this the planner spends its retry
    budget re-asking the model to fix shape. Pre-unwrapping at the
    executor seam means the rest of the pipeline (Pydantic validation,
    custom handlers) sees the native types it expects.

    The heuristic is intentionally narrow: only attempt to JSON-parse
    strings that **start with ``{`` or ``[``**. This protects free-form
    code bodies (``polars_code``, ``python_script``, ``sql_query``),
    Polars expressions ("``pl.col('x') > 5``"), SQL queries ("``SELECT
    …``"), AND — critically — string-typed fields with numeric content
    (``BasicFilter.value = "1"``, column names like ``"123"``) from
    being parsed accidentally.

    Bare scalar parse-results — int, float, bool, null — are NOT
    returned. Eagerly unwrapping digit-prefixed strings into ints
    corrupts str-typed fields: when the LLM correctly emits
    ``BasicFilter.value = "1"``, turning that into ``1`` makes Pydantic
    reject the int for a str field and the rejection blames the LLM
    for the wrong shape. Pydantic v2 lax mode handles ``str → int``
    coercion at the model layer when a field actually wants an int, so
    leaving scalars as strings is both safe and correct.

    Walks dicts and lists recursively so partially-encoded payloads
    (e.g. ``{"join_input": {"join_mapping": "[{...}]"}}``) get fully
    unwrapped in one pass.
    """
    if isinstance(value, dict):
        return {k: _unwrap_json_string_values(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_unwrap_json_string_values(item) for item in value]
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return value
        first = stripped[0]
        # Only unwrap strings that look like JSON containers ({} or []).
        # Bare scalar strings (digits, quoted strings, "true", code
        # bodies, identifiers) pass through unchanged so Pydantic's
        # lax-mode coercion at the model layer can decide what to do.
        if first not in "{[":
            return value
        try:
            parsed = json.loads(stripped)
        except (TypeError, ValueError):
            return value
        # Recurse so nested JSON-strings inside the parsed container
        # also get unwrapped.
        if isinstance(parsed, dict):
            return _unwrap_json_string_values(parsed)
        if isinstance(parsed, list):
            return _unwrap_json_string_values(parsed)
        return value
    return value


def _coerce_connection_id_to_flat(tool_args: dict[str, Any]) -> dict[str, Any]:
    """Accept the LLM's natural ``connection_id`` shape.

    The LLM occasionally emits ``{"connection_id": "1→2"}`` for a
    delete_connection call. Without this coercion, that gets rejected
    with *"missing required field: from_node_id"* — burning a retry
    round before the LLM re-emits the structured
    ``{from_node_id, to_node_id}`` shape. Same posture as the universal
    JSON-string unwrap: accept the LLM's natural emission rather than
    forcing the function-calling API to teach it the strict shape via
    refusal-loop attrition.

    Recognises three arrow-style separators commonly seen in LLM
    outputs (``->``, ``→``, and ``:``) so we don't have to enumerate
    every possible whitespace / Unicode variant. Returns ``tool_args``
    unchanged when ``connection_id`` is absent or can't be parsed —
    defensive for the strict-shape path.
    """
    raw = tool_args.get("connection_id")
    if raw is None:
        return tool_args
    if not isinstance(raw, str):
        return tool_args
    m = _CONNECTION_ID_ARROW_RE.match(raw)
    if m is None:
        return tool_args
    rebuilt = dict(tool_args)
    rebuilt.pop("connection_id", None)
    rebuilt.setdefault("from_node_id", int(m.group(1)))
    rebuilt.setdefault("to_node_id", int(m.group(2)))
    return rebuilt
