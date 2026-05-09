"""Tool-message reply rendering for the LLM.

These helpers compose the ``role="tool"`` content the planner appends
back into the conversation after each ``execute_tool_call``. Echoing
the just-staged settings closes the loop on subsequent
``update_node_settings`` calls — without this echo the LLM has to
hallucinate the full settings dict, including fields it can't know
(``pos_x``, ``user_id``, ``depending_on_id``).
"""

from __future__ import annotations

import json
from typing import Any

from flowfile_core.ai.tools.executor import ToolExecutionResult


_SETTINGS_REPLY_MAX_CHARS = 2000
"""Cap on the JSON-serialised settings echo in the LLM tool reply.

Empirically, simple node settings (filter, group_by, select) serialise to
200-500 chars; complex ones (multi-column joins, formulas with long
expressions) can exceed 1500. The cap keeps the tool reply bounded so a
pathological staging doesn't blow the context budget. When clipped, we
append ``... [truncated]`` so the LLM knows the echo is partial and can
``read_node_schema`` if needed.
"""


def _extract_staged_settings_for_reply(result: ToolExecutionResult) -> str | None:
    """Return a compact JSON of the just-staged settings, for the LLM tool reply.

    Without this, the only post-staging signal the LLM gets back is
    ``status: staged | predicted columns: ...``. When the user later
    asks to modify the same node, the LLM has to invent the full
    settings dict (the update contract requires the *full* settings) —
    including fields it can't know like ``pos_x`` / ``pos_y`` /
    ``user_id`` / ``depending_on_id``, which it then hallucinates with
    random-looking values that overwrite the real ones. Echoing the
    staged settings closes that loop.

    Handles both ``add_*`` payloads (settings under ``"settings"``) and
    modification payloads (settings under ``"new_settings"``). Returns
    ``None`` for op shapes without a settings dict (connect /
    delete_node / delete_connection).
    """
    payload = result.staged_node_payload
    if not isinstance(payload, dict):
        return None
    settings = payload.get("settings")
    if not isinstance(settings, dict):
        settings = payload.get("new_settings")
    if not isinstance(settings, dict):
        return None
    try:
        rendered = json.dumps(settings, separators=(",", ":"))
    except (TypeError, ValueError):
        return None
    if len(rendered) > _SETTINGS_REPLY_MAX_CHARS:
        rendered = rendered[:_SETTINGS_REPLY_MAX_CHARS] + "... [truncated]"
    return rendered


def _summarise_result_for_llm(result: ToolExecutionResult) -> str:
    """Compact summary of a :class:`ToolExecutionResult` for the LLM tool message.

    The LLM sees this as the ``role="tool"`` reply that closes the loop on
    its prior tool call. Keep it terse but include enough signal for the
    LLM to correct on retry: refusal reason / detail, predicted columns,
    warnings, and the just-staged settings dict so subsequent
    ``update_node_settings`` calls can copy current values rather than
    hallucinate them.
    """
    parts: list[str] = [f"status: {result.status}"]
    if result.status == "rejected":
        if result.refusal_reason:
            parts.append(f"refusal: {result.refusal_reason}")
        if result.refusal_detail:
            parts.append(f"detail: {result.refusal_detail}")
    if result.warnings:
        parts.append("warnings: " + "; ".join(result.warnings))
    if result.predicted_output_schema:
        cols = ", ".join(
            str(col.get("name", ""))
            for col in result.predicted_output_schema
            if isinstance(col, dict) and col.get("name")
        )
        if cols:
            parts.append(f"predicted columns: {cols}")
    if result.status in {"staged", "applied", "warned"}:
        settings_json = _extract_staged_settings_for_reply(result)
        if settings_json is not None:
            parts.append(f"settings: {settings_json}")
    if result.extra:
        parts.append(f"extra: {result.extra}")
    return " | ".join(parts)


def _payload_node_id(payload: dict[str, Any] | None) -> int | None:
    """Extract the node id a staged tool call targets.

    For ``add_*`` payloads the id lives on the validated settings dict
    (``payload["settings"]["node_id"]``); the planner emits this for
    ``staged_node_ids`` tracking and for the ``tool_call_staged`` event.
    For ``update_node_settings`` modification payloads, the id is a
    top-level field — modifications target an existing node, so the
    inner ``new_settings.node_id`` is not authoritative (and might be
    omitted by the LLM). We return the top-level ``node_id`` in that
    case for the event payload, but the planner does NOT add modification
    targets to ``staged_node_ids`` (that list is for net-new nodes; the
    drift detector treats modification targets as unchanged).
    """
    if not isinstance(payload, dict):
        return None
    if payload.get("kind") == "modification":
        nid = payload.get("node_id")
        return nid if isinstance(nid, int) else None
    settings = payload.get("settings")
    if not isinstance(settings, dict):
        return None
    nid = settings.get("node_id")
    return nid if isinstance(nid, int) else None
