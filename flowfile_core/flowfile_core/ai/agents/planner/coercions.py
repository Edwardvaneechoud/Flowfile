"""Tool-arg coercions applied at the planner seam.

These coercions sit between the LLM's emitted ``tool_args`` and the
executor's strict Pydantic validation. They handle the recoverable
wrong-shape cases that smaller open-weights models routinely emit, so
the executor's refusal pipeline only sees genuinely-bad payloads.

Per the project rule ("normalize at the executor seam, not in the
prompt"), each helper here corresponds to a recoverable-shape pattern
the LLM consistently produces. Adding more prompt text to fix these
cases hits diminishing returns; coercion is faster, cheaper, and
deterministic.
"""

from __future__ import annotations

import json
import re
from typing import Any

_FORMULA_NAME_PATTERNS: tuple[tuple[str, int], ...] = (
    # Phrase forms the user typically uses to name a derived column.
    # Each pattern is (regex, group index) — group 1 captures the name.
    (r"\bas\s+([A-Za-z_][A-Za-z0-9_]*)\b", 1),
    (r"\ba\s+([A-Za-z_][A-Za-z0-9_]*)\s+column\b", 1),
    (r"\bcolumn\s+(?:called|named)\s+([A-Za-z_][A-Za-z0-9_]*)\b", 1),
    (r"\bnew\s+column\s+([A-Za-z_][A-Za-z0-9_]*)\b", 1),
)


def _derive_formula_output_column_name(user_prompt: str) -> str:
    """Best-effort extraction of the user's intended output-column name
    for the formula bare-string coercion. Falls back to ``"derived"``
    when no recognizable pattern fires.
    """
    text = (user_prompt or "").strip()
    if not text:
        return "derived"
    for pattern, group in _FORMULA_NAME_PATTERNS:
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if m is not None:
            candidate = m.group(group)
            if candidate and candidate.lower() not in {"new", "column", "data"}:
                return candidate
    return "derived"


def _coerce_formula_bare_string_args(
    tool_args: dict[str, Any],
    *,
    user_prompt: str,
) -> dict[str, Any]:
    """Auto-coerce the LLM's *bare-expression-string* shape for
    ``add_formula`` at agent_staged ``fill_settings``.

    Smaller models routinely confuse the FunctionInput envelope's
    inner ``function`` field (a string expression) with the OUTER
    ``function`` parameter (a FunctionInput object). They emit
    ``{"function": "[first] + ' ' + [last]"}`` — only the expression
    string keyed under the wrapper-field name, no ``field``
    descriptor. The downstream Pydantic validation then refuses with
    *"function expects FunctionInput, got str"*; the cleanest UX is
    to recognise the intent and fill in the missing ``field`` rather
    than refuse.

    Coercion rule (narrow):

    * ``tool_args`` is a single-key dict ``{"function": <str>}`` AND
      the value is a non-empty string (the formula text).
    * Synthesize ``{"field": {"name": <derived>, "data_type":
      "String"}, "function": <the string>}``.
    * ``field.name`` is derived from the user prompt (best-effort
      extraction of *"as <name>"* / *"a <name> column"*); falls
      back to ``"derived"`` so the user can rename in the diff
      review.
    * The caller still wraps the result under the inner-input
      ``function`` field name; this helper only fixes the SHAPE
      under the wrapper.

    Returns ``tool_args`` unchanged when the coercion doesn't apply.
    Defensive: a future LLM that does emit the right shape sails
    through unchanged.
    """
    if not isinstance(tool_args, dict):
        return tool_args
    candidate = tool_args.get("function")
    if not isinstance(candidate, str) or not candidate.strip():
        return tool_args
    # Only fire when the bare-string shape is the *complete* inner
    # payload — i.e. the LLM didn't already supply ``field``. If
    # ``field`` is present we leave it for normal validation to
    # surface the more specific error.
    if "field" in tool_args:
        return tool_args

    derived_name = _derive_formula_output_column_name(user_prompt)
    return {
        "field": {"name": derived_name, "data_type": "String"},
        "function": candidate,
    }


def _looks_like_outer_envelope_value(value: Any) -> bool:
    """Does ``value`` look like an outer-envelope's inner-class value
    (a dict, possibly JSON-encoded)?

    Returns True for ``{...}`` and for ``"{\\"...\\": ...}"`` (JSON
    string that parses to a dict). The universal unwrap converts the
    JSON-string variant to a dict at the executor seam, so both cases
    mean *"the LLM provided the outer envelope"* — no planner-side
    wrap needed. Returns False for scalars (bare expression strings,
    ints, lists), which are inner-schema collision noise that DOES
    need the wrap.
    """
    if isinstance(value, dict):
        return True
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped or stripped[0] != "{":
            return False
        try:
            parsed = json.loads(stripped)
        except (TypeError, ValueError):
            return False
        return isinstance(parsed, dict)
    return False
