"""Render-side helpers for the user-facing chat trail.

Captures the assistant preamble (``_capture_rationale``), classifies
clarifying-question turns (``_looks_like_question``), and renders
human-readable summaries for the per-tool-call event payload
(``_arg_summary*``). Used by the loop to populate the ``rationale`` /
``arg_summary`` fields on the SSE events the frontend renders.
"""

from __future__ import annotations

from typing import Any

from ._internal import _ADD_PREFIX, _classify_op_kind  # noqa: F401  (re-export for tests)

# Capture a single short rationale (~280 chars) and trim trailing
# whitespace. Rationale longer than this is almost always the model
# writing a paragraph — clipping keeps the chat scannable.
_RATIONALE_MAX_CHARS: int = 280


def _capture_rationale(text: str | None) -> str | None:
    """Trim and bound the model's preamble for use as a tool_step rationale.

    The planner prompt instructs the model to emit a single short
    natural-language sentence immediately before each tool call. We capture
    that assistant ``content`` and clip it; if the model didn't produce a
    preamble (or emitted only whitespace), we return ``None`` so the
    frontend / executor falls back to the server-generated arg_summary.
    """
    if not isinstance(text, str):
        return None
    trimmed = text.strip()
    if not trimmed:
        return None
    if len(trimmed) > _RATIONALE_MAX_CHARS:
        # Cut on the last sentence boundary inside the cap if we can find one,
        # else hard-truncate. Keeps the UI from showing a half-sentence with
        # an awkward dangling "and".
        head = trimmed[:_RATIONALE_MAX_CHARS]
        for boundary in (". ", "! ", "? ", "\n"):
            idx = head.rfind(boundary)
            if idx > _RATIONALE_MAX_CHARS // 2:
                return head[: idx + 1].strip()
        return head.rstrip() + "…"
    return trimmed


_QUESTION_TOKENS: tuple[str, ...] = (
    "which",
    "what",
    "where",
    "when",
    "who",
    "why",
    "how",
    "do you",
    "would you",
    "should i",
    "should we",
    "can you",
    "could you",
    "shall i",
    "shall we",
    "did you mean",
)


def _looks_like_question(text: str | None) -> bool:
    """True if ``text`` reads like a clarifying question to the user.

    Two cheap signals:

    * **Ends with ``?``** — wins regardless of length / wording.
    * **Lower-cased substring contains an interrogative token** from
      :data:`_QUESTION_TOKENS`. Catches *"Should I drop nulls first?"*-style
      preambles even when the trailing ``?`` is missing, and *"Do you want
      me to ..."* when the model gets verbose.

    Whitespace-only or ``None`` returns ``False``. No NLP — a token list
    keeps the planner free of model-driven classification cost on a path
    that fires on every loop exit. False positives on declarative
    sentences containing ``"how"`` or ``"which"`` (e.g. *"This is how the
    join works."*) are tolerable: the only consequence is the frontend
    renders *"Agent waiting for your reply…"* instead of *"Agent finished
    — nothing to stage"*, both still resumable via ``/followup``.
    """
    if not isinstance(text, str):
        return False
    trimmed = text.strip()
    if not trimmed:
        return False
    if trimmed.rstrip().endswith("?"):
        return True
    lowered = trimmed.lower()
    return any(token in lowered for token in _QUESTION_TOKENS)


def _format_columns(value: Any) -> str | None:
    """Render a list-of-strings / list-of-dicts as a comma-separated column list."""
    if isinstance(value, list):
        names: list[str] = []
        for item in value:
            if isinstance(item, str) and item:
                names.append(item)
            elif isinstance(item, dict):
                nm = item.get("name") or item.get("column")
                if isinstance(nm, str) and nm:
                    names.append(nm)
        if names:
            return ", ".join(names)
    return None


def _arg_summary_for_add(node_type: str, settings: dict[str, Any]) -> str:
    """Render a one-line natural-language summary for an ``add_<node_type>`` call.

    Used by the planner as the frontend's secondary line (raw args reference)
    and by the frontend as the headline when the model failed to emit a
    preamble. Each branch covers the load-bearing fields for the most common
    node types; everything else falls back to the generic ``"Adding <type>"``
    shape so we never crash on a settings shape we didn't anticipate.

    The LLM's tool call ``arguments`` follow the per-node Pydantic settings
    schema directly (e.g. ``NodeFilter`` has ``filter_input`` at the top
    level), so the load-bearing fields live at the root of ``settings``.
    Some surfaces (e.g. ``add_node`` with explicit envelope) wrap them
    under ``settings_input`` — we handle both shapes.
    """
    if not isinstance(settings, dict):
        settings = {}
    nested = settings.get("settings_input")
    settings_dict: dict[str, Any] = nested if isinstance(nested, dict) else settings

    pretty_type = node_type.replace("_", " ")

    if node_type == "filter":
        predicate = settings_dict.get("filter_input", {})
        if isinstance(predicate, dict):
            expr = predicate.get("advanced_filter") or predicate.get("basic_filter")
            if isinstance(expr, str) and expr.strip():
                return f"Filter on `{expr.strip()}`"
        return "Adding filter"

    if node_type == "sort":
        cols = settings_dict.get("sort_by")
        if isinstance(cols, list) and cols:
            names = []
            for item in cols:
                if isinstance(item, dict):
                    nm = item.get("column")
                    direction = item.get("how") or item.get("direction") or "asc"
                    if isinstance(nm, str) and nm:
                        names.append(f"{nm} {direction}")
            if names:
                return f"Sort by {', '.join(names)}"
        return "Adding sort"

    if node_type == "join":
        join_input = settings_dict.get("join_input", {})
        if isinstance(join_input, dict):
            keys = join_input.get("join_mapping") or join_input.get("join_keys")
            how = join_input.get("how") or "inner"
            if isinstance(keys, list) and keys:
                key_strs: list[str] = []
                for k in keys:
                    if isinstance(k, dict):
                        left = k.get("left_col") or k.get("left")
                        right = k.get("right_col") or k.get("right")
                        if isinstance(left, str) and isinstance(right, str):
                            key_strs.append(f"{left}={right}")
                    elif isinstance(k, str):
                        key_strs.append(k)
                if key_strs:
                    return f"{how.capitalize()} join on {', '.join(key_strs)}"
        return "Adding join"

    if node_type == "select":
        cols = _format_columns(settings_dict.get("select_input"))
        if cols:
            return f"Select columns: {cols}"
        return "Adding select"

    if node_type in {"formula", "polars_code", "python_script", "sql_query"}:
        # These either have a code/expression body or a target column; show
        # the target name when present so the user sees "Adding amount_usd"
        # rather than the raw expression.
        target = settings_dict.get("function") or settings_dict.get("output_column")
        if isinstance(target, dict):
            field = target.get("field") or target.get("column")
            if isinstance(field, str) and field:
                return f"Adding {pretty_type} → `{field}`"
        return f"Adding {pretty_type}"

    if node_type == "group_by":
        group_cols = _format_columns(settings_dict.get("group_by_input"))
        if group_cols:
            return f"Group by {group_cols}"
        return "Adding group_by"

    if node_type == "unique":
        cols = _format_columns(settings_dict.get("unique_input"))
        if cols:
            return f"Unique on {cols}"
        return "Adding unique"

    if node_type == "union":
        return "Adding union"

    if node_type.startswith("read_") or node_type.endswith("_source") or node_type in {"manual_input"}:
        # Sources don't have an upstream; a path / table is the right hint.
        path = settings_dict.get("path") or settings_dict.get("file_path")
        table = settings_dict.get("table_name")
        if isinstance(path, str) and path:
            return f"Reading from `{path}`"
        if isinstance(table, str) and table:
            return f"Reading from `{table}`"
        return f"Adding {pretty_type}"

    return f"Adding {pretty_type}"


def _arg_summary(tool_name: str, tool_args: dict[str, Any]) -> str | None:
    """Server-side fallback summary for a tool call.

    Used when the model didn't emit a rationale preamble. Returns ``None``
    for meta ops (the UI hides them anyway). For ``flowfile.graph.add_*``
    we render a settings-aware one-liner; for ``connect`` / ``delete_*`` /
    ``schema.*`` we render a generic but still informative line.
    """
    if not tool_args:
        tool_args = {}

    if tool_name.startswith(_ADD_PREFIX):
        node_type = tool_name.removeprefix(_ADD_PREFIX)
        return _arg_summary_for_add(node_type, tool_args)

    if tool_name == "flowfile.graph.connect":
        upstream = tool_args.get("upstream_node_id") or tool_args.get("from_node_id")
        downstream = tool_args.get("downstream_node_id") or tool_args.get("to_node_id")
        if isinstance(upstream, int) and isinstance(downstream, int):
            return f"Connecting node {upstream} → node {downstream}"
        return "Connecting nodes"

    if tool_name == "flowfile.graph.delete_node":
        nid = tool_args.get("node_id")
        if isinstance(nid, int):
            return f"Removing node {nid}"
        return "Removing a node"

    if tool_name == "flowfile.graph.update_node_settings":
        nid = tool_args.get("node_id")
        if isinstance(nid, int):
            return f"Updating settings on node {nid}"
        return "Updating node settings"

    if tool_name == "flowfile.graph.delete_connection":
        upstream = tool_args.get("upstream_node_id")
        downstream = tool_args.get("downstream_node_id")
        if isinstance(upstream, int) and isinstance(downstream, int):
            return f"Disconnecting node {upstream} ↛ node {downstream}"
        return "Removing a connection"

    if tool_name == "flowfile.schema.read_node_schema":
        nid = tool_args.get("node_id")
        if isinstance(nid, int):
            return f"Reading schema for node {nid}"
        return "Reading node schema"

    if tool_name == "flowfile.schema.read_node_preview":
        nid = tool_args.get("node_id")
        if isinstance(nid, int):
            return f"Reading preview for node {nid}"
        return "Reading node preview"

    if tool_name.startswith("flowfile.codegen."):
        return f"Generating code ({tool_name.removeprefix('flowfile.codegen.')})"

    if tool_name.startswith("flowfile.meta."):
        return None

    return None
