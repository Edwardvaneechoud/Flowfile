"""Level 3 — Planner (multi-turn, plan-then-execute). Owned by W40.

Per plan §2 / §6.4 / §5.6, the planner:

* opens a session via ``POST /ai/agent/start``;
* surfaces a narrowed tool catalog using D002's two-stage pattern (default
  ``surface="agent"`` calls ``flowfile.meta.pick_category`` first, then the
  matching ``CATEGORY_PRESETS`` for the rest of the loop; ``surface=
  "agent_complex"`` exposes the full catalog in one shot);
* dispatches each LLM tool call through W31's :func:`execute_tool_call` with
  ``mode="stage"`` — the live graph is never mutated mid-run (per §9.2
  "Level 3 agent never auto-applies");
* runs D006's snapshot+warn-and-pause check before every dispatch — if the
  user mutated the canvas mid-run, the loop yields ``drift_detected`` +
  ``paused`` and exits cleanly so the route can return JSON / SSE-close
  while the session waits for ``POST /ai/agent/{session_id}/resume``;
* retries a rejected step up to ``max_retries_per_step`` times by feeding
  the executor's ``refusal_detail`` back as a ``role="tool"`` message and
  asking the LLM to correct;
* on completion, bundles the per-step :class:`StagedToolEntry` list into a
  single :class:`flowfile_core.ai.diff.GraphDiff` via
  :func:`flowfile_core.ai.diff.bundle_staged_results` and registers it via
  W41's :func:`flowfile_core.ai.diff.register_diff` — the user reviews the
  diff via the W35 ``AiDiffPreview`` and accepts atomically.

The function is a **pure async generator** that never raises — every
failure mode becomes a :class:`PlannerEvent` of type ``"error"`` /
``"tool_call_rejected"`` / ``"drift_detected"`` / ``"abort"`` so the SSE
wrapper can stream the failure to the client without a structured
exception escaping the generator boundary.

W42 swaps the in-memory session store for a disk-backed sidecar; W40 only
needs the in-memory shape. The ``PlannerEvent`` ``id:`` headers carry
``f"{session_id}.{step_count}"`` so EventSource clients *can* re-attach via
``Last-Event-ID`` once W42 lands the replay buffer — W40's resume route is
exclusively for D006 drift-pause, not connection drop.

System prompt: ``prompts/base.md`` + ``prompts/planner.md`` (D008) — the
``copilot`` level wouldn't fit; planner-level prompt language is
intentionally distinct.

The W11 lazy-litellm contract is preserved — this module must not import
``litellm`` at load time. The provider call goes through the W11 seam,
which lazy-loads litellm in its own subclass.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from flowfile_core.ai import audit as audit_module
from flowfile_core.ai import diff as diff_module
from flowfile_core.ai import safety, sessions
from flowfile_core.ai.context.builder import render_prompt_context
from flowfile_core.ai.providers.base import Message, Provider, ToolCall
from flowfile_core.ai.scheduler import RateLimitScheduler, default_scheduler
from flowfile_core.ai.tools.dry_run import DryRunCache
from flowfile_core.ai.tools.executor import (
    InsertionContext,
    ToolExecutionResult,
    execute_tool_call,
)
from flowfile_core.ai.tools.registry import build_tool_catalog

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_graph import FlowGraph

logger = logging.getLogger(__name__)


DEFAULT_MAX_STEPS: int = 12
DEFAULT_MAX_RETRIES_PER_STEP: int = 3
DEFAULT_MAX_TOKENS: int = 2_048
RATIONALE_MAX_LEN: int = 500


PlannerEventName = Literal[
    "thinking",
    "tool_call_proposed",
    "tool_call_staged",
    "tool_call_warned",
    "tool_call_rejected",
    "drift_detected",
    "paused",
    "retry",
    "abort",
    "complete",
    "error",
    "info",
]


class PlannerEvent(BaseModel):
    """One event yielded by :func:`run_planner_session`.

    The ``payload`` shape is event-specific — see the caller-side handler
    contract in ``stores/ai-agent-store.ts``. Keeping it a dict rather than
    a discriminated union keeps wire-shape evolution cheap; consumers
    branch on ``event``.
    """

    model_config = ConfigDict(frozen=True)

    event: PlannerEventName
    payload: dict[str, Any] = Field(default_factory=dict)


# --------------------------------------------------------------------------- #
# Helpers                                                                      #
# --------------------------------------------------------------------------- #


_ADD_PREFIX = "flowfile.graph.add_"
_PICK_CATEGORY_NAME = "flowfile.meta.pick_category"

# W38 — single short sentence per step, ≤ 20 words per the prompt instruction;
# we capture up to ~280 chars (a generous cap that tolerates the model running
# slightly long) and trim trailing whitespace. Rationale longer than this is
# almost always the model writing a paragraph — clipping keeps the chat scannable.
_RATIONALE_MAX_CHARS: int = 280


OpKind = Literal["meta", "graph", "schema", "codegen", "unknown"]


def _classify_op_kind(tool_name: str) -> OpKind:
    """Map a fully-qualified tool name to its op_kind for the W38 UI gating.

    ``flowfile.meta.*`` are LLM-internal routing decisions (D002 two-stage
    selection) and the frontend hides them. ``flowfile.graph.*`` are the
    user-facing canvas mutations that need a rationale. ``flowfile.schema.*``
    are read-only introspection calls — we still show them so the user
    knows the agent is "looking at" something. ``flowfile.codegen.*`` are
    code generation helpers that only show up under ``agent_complex``.
    """
    if tool_name.startswith("flowfile.meta."):
        return "meta"
    if tool_name.startswith("flowfile.graph."):
        return "graph"
    if tool_name.startswith("flowfile.schema."):
        return "schema"
    if tool_name.startswith("flowfile.codegen."):
        return "codegen"
    return "unknown"


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


def _allocate_node_id(flow: FlowGraph, session: sessions.AgentSession) -> int:
    """Pick the next free node_id, considering live nodes + in-batch additions.

    The flow's ``_node_db`` is keyed by id; every add_* tool dispatch
    reserves a slot in the staged session as well. Allocating here keeps
    the LLM out of the id-management business — it just emits settings.
    """
    used: set[int] = set()
    for node in flow.nodes:
        try:
            used.add(int(node.node_id))
        except (TypeError, ValueError, AttributeError):
            continue
    for entry in session.staged_results:
        if not isinstance(entry.staged_node_payload, dict):
            continue
        settings = entry.staged_node_payload.get("settings")
        if isinstance(settings, dict):
            nid = settings.get("node_id")
            if isinstance(nid, int):
                used.add(nid)
    return (max(used) + 1) if used else 1


def _resolve_insertion_context(session: sessions.AgentSession, tc: ToolCall, flow: FlowGraph) -> InsertionContext:
    """Build an :class:`InsertionContext` for a tool call.

    Order of preference:

    1. ``upstream_node_ids`` / ``right_input_node_id`` / ``pos_x`` / ``pos_y``
       in the LLM's tool args (it can override when chaining is needed —
       e.g. a join that targets two upstreams).
    2. The most-recent in-batch staged addition's ``node_id`` (chained
       transformations: add_filter → add_sort).
    3. The most-recent live node in the flow (cold-flow first step;
       attaches to whatever's already there).
    4. Empty (truly cold flow with no nodes) — the executor handles this
       (most node types refuse, sources don't need an upstream).
    """
    args: dict[str, Any] = tc.arguments or {}

    upstream_ids: list[int] = []
    raw_upstream = args.get("upstream_node_ids")
    if isinstance(raw_upstream, list):
        for uid in raw_upstream:
            if isinstance(uid, int):
                upstream_ids.append(uid)

    if not upstream_ids:
        for entry in reversed(session.staged_results):
            if not entry.tool_name.startswith(_ADD_PREFIX):
                continue
            payload = entry.staged_node_payload if isinstance(entry.staged_node_payload, dict) else {}
            settings = payload.get("settings") if isinstance(payload.get("settings"), dict) else {}
            nid = settings.get("node_id") if isinstance(settings, dict) else None
            if isinstance(nid, int):
                upstream_ids = [nid]
                break

    if not upstream_ids:
        live_nodes = flow.nodes
        if live_nodes:
            try:
                upstream_ids = [int(live_nodes[-1].node_id)]
            except (TypeError, ValueError, AttributeError):
                upstream_ids = []

    raw_right = args.get("right_input_node_id")
    right_input_node_id = raw_right if isinstance(raw_right, int) else None

    pos_x = args.get("pos_x")
    pos_y = args.get("pos_y")
    pos_x_val = float(pos_x) if isinstance(pos_x, int | float) else 0.0
    pos_y_val = float(pos_y) if isinstance(pos_y, int | float) else 0.0

    return InsertionContext(
        upstream_node_ids=upstream_ids,
        right_input_node_id=right_input_node_id,
        pos_x=pos_x_val,
        pos_y=pos_y_val,
    )


def _summarise_result_for_llm(result: ToolExecutionResult) -> str:
    """Compact summary of a :class:`ToolExecutionResult` for the LLM tool message.

    The LLM sees this as the ``role="tool"`` reply that closes the loop on
    its prior tool call. Keep it terse but include enough signal for the
    LLM to correct on retry: refusal reason / detail, predicted columns,
    warnings.
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
    if result.extra:
        parts.append(f"extra: {result.extra}")
    return " | ".join(parts)


def _payload_node_id(payload: dict[str, Any] | None) -> int | None:
    if not isinstance(payload, dict):
        return None
    settings = payload.get("settings")
    if not isinstance(settings, dict):
        return None
    nid = settings.get("node_id")
    return nid if isinstance(nid, int) else None


def _collect_live_node_ids(flow: FlowGraph) -> list[int]:
    out: list[int] = []
    for node in flow.nodes:
        try:
            out.append(int(node.node_id))
        except (TypeError, ValueError, AttributeError):
            continue
    return out


def _check_self_loop(
    proposed_node_id: int,
    insertion_context: InsertionContext,
) -> str | None:
    """Return a refusal_detail string if ``proposed_node_id`` would self-loop, else None.

    W54 universal invariant: a new node's ``node_id`` may never equal any of
    its own ``upstream_node_ids`` or its ``right_input_node_id``. Catches all
    three plausible upstream causes (LLM-provided collision, stale
    staged_results post-resume, live-graph drift) regardless of which one
    fired — the apply_diff cycle error is the same.
    """
    upstream = list(insertion_context.upstream_node_ids or [])
    right_input = insertion_context.right_input_node_id
    if proposed_node_id in upstream:
        return (
            f"proposed node_id {proposed_node_id} collides with upstream_node_ids "
            f"{upstream} — would create a self-loop on apply"
        )
    if right_input is not None and proposed_node_id == right_input:
        return (
            f"proposed node_id {proposed_node_id} equals right_input_node_id "
            f"{right_input} — would create a self-loop on apply"
        )
    return None


def _op_count(graph_diff: diff_module.GraphDiff) -> int:
    return (
        len(graph_diff.additions)
        + len(graph_diff.connections_added)
        + len(graph_diff.deletions)
        + len(graph_diff.connections_removed)
    )


def _resolve_current_surface(session: sessions.AgentSession, narrowed_category: str | None) -> str:
    """Map the session's surface + the heuristic ``pick_category`` outcome to a tool surface.

    ``surface="agent"`` starts with just ``flowfile.meta.pick_category``;
    once the LLM picks a category we narrow to ``CATEGORY_PRESETS[chosen]``
    for the rest of the loop. ``surface="agent_complex"`` is one-shot full
    catalog and never narrows.
    """
    if session.surface == "agent_complex":
        return "agent_complex"
    if narrowed_category is None:
        return "agent"
    return narrowed_category


def _build_initial_messages(flow: FlowGraph, session: sessions.AgentSession) -> list[Message]:
    """Build ``[system, user]`` from W22 + the user's goal.

    The system block comes from ``assemble_system_prompt(surface)`` (via
    ``render_prompt_context``) — D008's ``base.md`` + ``planner.md`` for
    both ``agent`` and ``agent_complex``. The user block is W22's
    deterministic subgraph snapshot followed by a ``## Goal`` block.

    **Context bug fix (D1 from W40 diagnostic 2026-05-04):** previously
    called with ``pinned_node_ids=[]`` and no ``mentions``, so the user
    saw ``## Subgraph (empty)`` regardless of canvas state — the agent
    was context-blind and refused every cold-flow request even when nodes
    existed. Pass ``mentions="@flow"`` so the resolver expands to all
    current nodes (mirrors W28's chat-route fix and W23's "Fix with AI"
    pattern).
    """
    ctx = render_prompt_context(
        flow,
        [],
        surface=session.surface,
        samples_mode=session.samples_mode,
        mentions="@flow",
    )
    user_text = f"{ctx.user}\n\n## Goal\n\n{session.user_prompt}".strip()
    return [
        Message(role="system", content=ctx.system),
        Message(role="user", content=user_text),
    ]


# --------------------------------------------------------------------------- #
# Main loop                                                                    #
# --------------------------------------------------------------------------- #


async def run_planner_session(
    *,
    session: sessions.AgentSession,
    flow: FlowGraph,
    provider: Provider,
    scheduler: RateLimitScheduler | None = None,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    max_retries_per_step: int = DEFAULT_MAX_RETRIES_PER_STEP,
) -> AsyncIterator[PlannerEvent]:
    """Drive a planner session to completion / pause / failure.

    Pure async generator. **Never raises** — every failure becomes an
    ``error`` / ``tool_call_rejected`` / ``drift_detected`` / ``abort``
    event with a stable shape. The caller (``agent_routes.py``) wraps
    the generator in SSE; tests consume the raw events.

    Mutations to the in-memory ``session`` (status, step_count,
    staged_results, messages, drift_detail, diff_id, rationale) are
    in-place — callers can inspect the session after the generator
    exhausts. W42 disk-persists these fields verbatim.
    """
    try:
        async for event in _run_planner_loop(
            session=session,
            flow=flow,
            provider=provider,
            scheduler=scheduler,
            max_tokens=max_tokens,
            max_retries_per_step=max_retries_per_step,
        ):
            yield event
    except Exception as exc:  # noqa: BLE001 — last-resort safety net
        logger.exception("run_planner_session crashed unexpectedly")
        try:
            session.status = "failed"
            session.touch()
        except Exception:
            pass
        yield PlannerEvent(event="error", payload={"message": f"planner crashed: {exc}"})


async def _run_planner_loop(
    *,
    session: sessions.AgentSession,
    flow: FlowGraph,
    provider: Provider,
    scheduler: RateLimitScheduler | None,
    max_tokens: int,
    max_retries_per_step: int,
) -> AsyncIterator[PlannerEvent]:
    # ``aborted`` is honored as an early-exit (the abort route flips status
    # to ``aborted`` between iterations). Same for ``running`` (normal start)
    # and ``paused_drift`` (resume after D006 drift).
    if session.status == "aborted":
        yield PlannerEvent(event="abort", payload={"session_id": session.session_id})
        return
    if session.status not in ("running", "paused_drift"):
        yield PlannerEvent(
            event="error",
            payload={"message": f"cannot run session in status {session.status!r}"},
        )
        return

    if session.status == "paused_drift":
        # Resume after drift — re-snapshot so subsequent drift_detect compares against fresh state.
        session.snapshot = sessions.capture_graph_snapshot(flow)
        session.drift_detail = None
        session.pause_reason = None
        session.status = "running"

        # W54 — staged_results hygiene. Drop entries whose node_id now
        # collides with a live node (user manually added one mid-pause)
        # or whose upstream references an id that no longer exists (user
        # deleted the upstream). One audit row per drop so the cause is
        # diagnosable post-hoc. ``awaiting_user`` is reserved-but-unused
        # today, so we only thread hygiene through the paused_drift path.
        _, dropped = sessions.revalidate_staged_results_against_live(session, flow)
        for entry, reason in dropped:
            try:
                audit_module.record_event(
                    audit_module.AuditEvent(
                        session_id=session.session_id,
                        user_id=session.user_id,
                        tool_name="internal.staged_drop_on_resume",
                        flow_id=session.flow_id,
                        result_status="error",
                        error=f"staged_drop_on_resume:{reason}",
                        tool_args={
                            "__planner_meta__": {
                                "dropped_tool_name": entry.tool_name,
                                "dropped_payload": entry.staged_node_payload,
                                "dropped_audit_id": entry.audit_id,
                                "reason": reason,
                                "live_node_ids_at_resume": sorted(_collect_live_node_ids(flow)),
                            }
                        },
                    )
                )
            except Exception:  # noqa: BLE001 — audit-side errors must not crash the loop
                logger.warning("audit.record_event failed for staged_drop_on_resume", exc_info=False)
        if dropped:
            yield PlannerEvent(
                event="info",
                payload={
                    "message": "resumed; dropped stale staged entries",
                    "dropped_count": len(dropped),
                    "drop_reasons": [reason for _, reason in dropped],
                },
            )

        session.touch()
        yield PlannerEvent(
            event="info",
            payload={"message": "resumed; re-snapshotted graph"},
        )

    sched = scheduler or default_scheduler()
    dry_run_cache = DryRunCache()
    narrowed_category: str | None = None
    retries_for_step = 0

    if not session.messages:
        session.messages = _build_initial_messages(flow, session)

    while True:
        if session.step_count >= session.max_steps:
            session.status = "failed"
            session.touch()
            yield PlannerEvent(
                event="error",
                payload={"message": "max_steps reached", "max_steps": session.max_steps},
            )
            return

        if session.status == "aborted":
            yield PlannerEvent(
                event="abort",
                payload={"session_id": session.session_id},
            )
            return

        # D006 — drift check before every dispatch. ``staged_node_ids``
        # excludes the agent's own staged additions from the external-added
        # bucket so the planner doesn't self-pause on its own work (W45 Q1).
        drift = sessions.detect_drift(
            flow,
            session.snapshot,
            agent_staged_node_ids=set(session.staged_node_ids),
        )
        if drift is not None:
            session.status = "paused_drift"
            session.drift_detail = drift
            session.pause_reason = "graph_changed"
            session.touch()
            yield PlannerEvent(
                event="drift_detected",
                payload={"drift": drift.model_dump(), "session_id": session.session_id},
            )
            yield PlannerEvent(
                event="paused",
                payload={"reason": "graph_changed", "session_id": session.session_id},
            )
            return

        current_surface = _resolve_current_surface(session, narrowed_category)
        try:
            tool_catalog = build_tool_catalog(surface=current_surface)
        except KeyError as exc:
            session.status = "failed"
            session.touch()
            yield PlannerEvent(event="error", payload={"message": f"unknown surface: {exc}"})
            return

        # --- Provider call ---
        try:
            async with sched.acquire(provider.name, surface=current_surface):
                response = await provider.chat(
                    messages=session.messages,
                    tools=tool_catalog,
                    max_tokens=max_tokens,
                )
        except Exception as exc:  # noqa: BLE001 — collapse to a stable reason
            logger.warning("planner provider call failed: %s", exc)
            session.status = "failed"
            session.touch()
            yield PlannerEvent(event="error", payload={"message": f"provider error: {exc}"})
            return

        assistant_text = response.content or ""
        if assistant_text:
            session.last_assistant_text = assistant_text
        assistant_msg = Message(
            role="assistant",
            content=assistant_text or None,
            tool_calls=list(response.tool_calls) if response.tool_calls else None,
        )
        session.messages.append(assistant_msg)

        tool_calls = list(response.tool_calls or [])

        # W38 — when the assistant turn is pure prose (no tool calls), surface
        # it as a ``thinking`` event so the user sees what the model said.
        # When tool calls follow, the same text rides on each ``tool_call_*``
        # event as ``rationale`` (the W38 contract), so emitting both would
        # render the same sentence twice in the chat trail.
        if assistant_text and not tool_calls:
            yield PlannerEvent(event="thinking", payload={"text": assistant_text})

        if not tool_calls:
            # LLM has nothing more to do.
            break

        any_succeeded_this_round = False

        # W38 — capture the assistant preamble that landed alongside this turn's
        # tool calls; it's the natural-language "what this step does" that the
        # planner.md prompt asks the model to emit. Shared across every tool
        # call in this round (the model writes one preamble per turn, even when
        # it ends up emitting multiple calls). Falls back to ``None`` when the
        # model skipped the preamble — the per-call ``arg_summary`` covers the
        # rendering gap.
        rationale_for_round = _capture_rationale(assistant_text)

        for tc in tool_calls:
            op_kind = _classify_op_kind(tc.name)
            # Rationale only attaches to user-facing ops. Meta ops are LLM-internal
            # routing and the UI hides them; if the model wrote a preamble for a
            # round whose only call is meta, that preamble belongs to whatever
            # *next* round of work the meta call is selecting for, not to the
            # meta call itself.
            rationale = rationale_for_round if op_kind != "meta" else None
            arg_summary = _arg_summary(tc.name, tc.arguments or {})

            yield PlannerEvent(
                event="tool_call_proposed",
                payload={
                    "id": tc.id,
                    "name": tc.name,
                    "arguments": tc.arguments,
                    "op_kind": op_kind,
                    "rationale": rationale,
                    "arg_summary": arg_summary,
                },
            )

            # Inject planner-managed args for add_* dispatches
            tool_args: dict[str, Any] = dict(tc.arguments) if tc.arguments else {}
            # W54 — capture provenance: did the LLM emit ``node_id`` itself,
            # or did the planner allocate? Both values flow into audit_meta
            # so the audit row alone shows whether a self-loop traced back
            # to an LLM hallucination or a planner allocation collision.
            llm_provided_node_id: int | None = None
            allocated_node_id: int | None = None
            if tc.name.startswith(_ADD_PREFIX):
                raw_llm_id = tool_args.get("node_id")
                if isinstance(raw_llm_id, int):
                    llm_provided_node_id = raw_llm_id
                tool_args.setdefault("flow_id", session.flow_id)
                if "node_id" not in tool_args:
                    tool_args["node_id"] = _allocate_node_id(flow, session)
                    raw_allocated = tool_args.get("node_id")
                    if isinstance(raw_allocated, int):
                        allocated_node_id = raw_allocated

            insertion_context = _resolve_insertion_context(session, tc, flow)

            # W54 — build audit_meta for instrumentation. Rides on
            # tool_args["__planner_meta__"] in the persisted audit row.
            # Always populated for add_* calls (success or rejected) so
            # any future self-loop is diagnosable from the audit row alone.
            audit_meta: dict[str, Any] | None = None
            if tc.name.startswith(_ADD_PREFIX):
                audit_meta = {
                    "allocated_node_id": allocated_node_id,
                    "llm_provided_node_id": llm_provided_node_id,
                    "resolved_upstream_node_ids": list(insertion_context.upstream_node_ids or []),
                    "right_input_node_id": insertion_context.right_input_node_id,
                    "live_node_ids_at_stage": sorted(_collect_live_node_ids(flow)),
                    "staged_node_ids_at_stage": list(session.staged_node_ids),
                }

            # W54 — universal self-loop invariant guard. Catches all three
            # plausible upstream causes (LLM-provided collision, stale
            # staged_results post-resume, live-graph drift). When it fires,
            # treat as ``tool_call_rejected`` with refusal_reason
            # ``self_loop_prevented``; counts toward W53's retry budget;
            # writes its own audit row (we never reach execute_tool_call,
            # which is what would otherwise persist the audit).
            if tc.name.startswith(_ADD_PREFIX):
                proposed = tool_args.get("node_id")
                if isinstance(proposed, int):
                    self_loop_detail = _check_self_loop(proposed, insertion_context)
                    if self_loop_detail is not None:
                        audit_id_for_event: int | None = None
                        try:
                            audit_row = audit_module.record_event(
                                audit_module.AuditEvent(
                                    session_id=session.session_id,
                                    user_id=session.user_id,
                                    tool_name=tc.name,
                                    flow_id=session.flow_id,
                                    result_status="rejected",
                                    error=self_loop_detail,
                                    tool_args={
                                        **(safety.redact_secrets(tool_args) if tool_args else {}),
                                        "__planner_meta__": audit_meta,
                                    },
                                )
                            )
                            audit_id_for_event = audit_row.id if audit_row is not None else None
                        except Exception:  # noqa: BLE001 — audit must not crash the loop
                            logger.warning("audit.record_event failed for self_loop_prevented", exc_info=False)

                        # Feed the rejection back to the LLM so it can correct on retry.
                        session.messages.append(
                            Message(
                                role="tool",
                                tool_call_id=tc.id,
                                name=tc.name,
                                content=f"status: rejected | refusal: self_loop_prevented | detail: {self_loop_detail}",
                            )
                        )
                        yield PlannerEvent(
                            event="tool_call_rejected",
                            payload={
                                "id": tc.id,
                                "name": tc.name,
                                "reason": "self_loop_prevented",
                                "detail": self_loop_detail,
                                "op_kind": op_kind,
                                "rationale": rationale,
                                "arg_summary": arg_summary,
                                "audit_id": audit_id_for_event,
                            },
                        )
                        continue

            # Dispatch — execute_tool_call is meant to never raise (returns rejected
            # result instead) but we wrap defensively.
            try:
                result = execute_tool_call(
                    flow_id=session.flow_id,
                    tool_name=tc.name,
                    tool_args=tool_args,
                    insertion_context=insertion_context,
                    session_id=session.session_id,
                    user_id=session.user_id,
                    mode="stage",
                    flow=flow,
                    dry_run_cache=dry_run_cache,
                    llm_provided_node_id=llm_provided_node_id,
                    audit_meta=audit_meta,
                )
            except Exception as exc:  # noqa: BLE001 — defence in depth
                logger.exception("tool dispatch raised; treating as rejected")
                tool_msg = Message(
                    role="tool",
                    tool_call_id=tc.id,
                    name=tc.name,
                    content=f"dispatch raised: {exc}",
                )
                session.messages.append(tool_msg)
                yield PlannerEvent(
                    event="tool_call_rejected",
                    payload={
                        "id": tc.id,
                        "name": tc.name,
                        "reason": "exception",
                        "detail": str(exc),
                        "op_kind": op_kind,
                        "rationale": rationale,
                        "arg_summary": arg_summary,
                    },
                )
                continue

            # Feed the result back into the conversation so the LLM can correct on retry
            tool_msg = Message(
                role="tool",
                tool_call_id=tc.id,
                name=tc.name,
                content=_summarise_result_for_llm(result),
            )
            session.messages.append(tool_msg)

            if result.status == "rejected":
                yield PlannerEvent(
                    event="tool_call_rejected",
                    payload={
                        "id": tc.id,
                        "name": tc.name,
                        "reason": result.refusal_reason or "rejected",
                        "detail": result.refusal_detail or "",
                        "op_kind": op_kind,
                        "rationale": rationale,
                        "arg_summary": arg_summary,
                    },
                )
                continue

            # pick_category is meta — surface narrows for the remaining loop
            if tc.name == _PICK_CATEGORY_NAME:
                chosen = result.extra.get("category") if isinstance(result.extra, dict) else None
                if isinstance(chosen, str) and chosen:
                    narrowed_category = chosen
                    # ``op_kind: "meta"`` lets the frontend suppress this from the
                    # user-visible chat trail (D002 internal routing — not a step
                    # the user needs to see). The audit log still captures the
                    # underlying tool call via execute_tool_call().
                    yield PlannerEvent(
                        event="info",
                        payload={
                            "message": "category narrowed",
                            "category": chosen,
                            "op_kind": "meta",
                        },
                    )
                any_succeeded_this_round = True
                continue

            # Real staging path
            if result.status in ("staged", "warned", "applied"):
                if result.staged_node_payload is not None:
                    session.staged_results.append(
                        diff_module.StagedToolEntry(
                            tool_name=tc.name,
                            audit_id=result.audit_id,
                            staged_node_payload=result.staged_node_payload,
                        )
                    )
                    # Track ids the agent has staged so subsequent drift
                    # checks can exclude them from external-added detection
                    # (W45 Q1). Only ``add_<node_type>`` calls produce a
                    # node_id — connection / delete payloads don't.
                    if tc.name.startswith(_ADD_PREFIX):
                        staged_id = _payload_node_id(result.staged_node_payload)
                        if staged_id is not None and staged_id not in session.staged_node_ids:
                            session.staged_node_ids.append(staged_id)
                event_name: PlannerEventName = "tool_call_warned" if result.status == "warned" else "tool_call_staged"
                yield PlannerEvent(
                    event=event_name,
                    payload={
                        "id": tc.id,
                        "name": tc.name,
                        "node_id": _payload_node_id(result.staged_node_payload),
                        "predicted_output_schema": result.predicted_output_schema,
                        "warnings": list(result.warnings),
                        "op_kind": op_kind,
                        "rationale": rationale,
                        "arg_summary": arg_summary,
                    },
                )
                any_succeeded_this_round = True

        # End of per-tool-call loop.
        session.step_count += 1
        session.touch()

        if any_succeeded_this_round:
            retries_for_step = 0
        else:
            retries_for_step += 1
            if retries_for_step >= max_retries_per_step:
                session.status = "failed"
                session.touch()
                yield PlannerEvent(
                    event="error",
                    payload={
                        "message": (
                            f"all {max_retries_per_step} consecutive attempts at step "
                            f"{session.step_count} were rejected"
                        ),
                    },
                )
                return
            yield PlannerEvent(
                event="retry",
                payload={"attempt": retries_for_step, "max": max_retries_per_step},
            )
        # Loop continues — accumulated tool messages are in session.messages.

    # --- Loop ended (no more tool calls) ---
    if not session.staged_results:
        session.status = "completed"
        session.touch()
        yield PlannerEvent(
            event="complete",
            payload={
                "session_id": session.session_id,
                "diff_id": None,
                "op_count": 0,
                "rationale": session.last_assistant_text,
                "diff_payload": None,
            },
        )
        return

    try:
        graph_diff = diff_module.bundle_staged_results(session.staged_results)
    except ValueError as exc:
        session.status = "failed"
        session.touch()
        yield PlannerEvent(event="error", payload={"message": f"bundle failed: {exc}"})
        return

    rationale = (session.last_assistant_text or "")[:RATIONALE_MAX_LEN] or None
    graph_diff = graph_diff.model_copy(
        update={
            "session_id": session.session_id,
            "flow_id": session.flow_id,
            "rationale": rationale,
        }
    )
    diff_id = diff_module.register_diff(graph_diff)
    session.diff_id = diff_id
    session.rationale = rationale
    session.status = "completed"
    session.touch()
    yield PlannerEvent(
        event="complete",
        payload={
            "session_id": session.session_id,
            "diff_id": diff_id,
            "op_count": _op_count(graph_diff),
            "rationale": rationale,
            "diff_payload": graph_diff.model_dump(mode="json"),
        },
    )


__all__ = [
    "DEFAULT_MAX_RETRIES_PER_STEP",
    "DEFAULT_MAX_STEPS",
    "DEFAULT_MAX_TOKENS",
    "OpKind",
    "PlannerEvent",
    "PlannerEventName",
    "RATIONALE_MAX_LEN",
    "run_planner_session",
]
