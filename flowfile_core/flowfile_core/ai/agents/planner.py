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

from flowfile_core.ai import diff as diff_module
from flowfile_core.ai import sessions
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

        # D006 — drift check before every dispatch
        drift = sessions.detect_drift(flow, session.snapshot)
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

        if assistant_text:
            yield PlannerEvent(event="thinking", payload={"text": assistant_text})

        tool_calls = list(response.tool_calls or [])
        if not tool_calls:
            # LLM has nothing more to do.
            break

        any_succeeded_this_round = False

        for tc in tool_calls:
            yield PlannerEvent(
                event="tool_call_proposed",
                payload={"id": tc.id, "name": tc.name, "arguments": tc.arguments},
            )

            # Inject planner-managed args for add_* dispatches
            tool_args: dict[str, Any] = dict(tc.arguments) if tc.arguments else {}
            if tc.name.startswith(_ADD_PREFIX):
                tool_args.setdefault("flow_id", session.flow_id)
                if "node_id" not in tool_args:
                    tool_args["node_id"] = _allocate_node_id(flow, session)

            insertion_context = _resolve_insertion_context(session, tc, flow)

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
                    },
                )
                continue

            # pick_category is meta — surface narrows for the remaining loop
            if tc.name == _PICK_CATEGORY_NAME:
                chosen = result.extra.get("category") if isinstance(result.extra, dict) else None
                if isinstance(chosen, str) and chosen:
                    narrowed_category = chosen
                    yield PlannerEvent(
                        event="info",
                        payload={"message": "category narrowed", "category": chosen},
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
                event_name: PlannerEventName = "tool_call_warned" if result.status == "warned" else "tool_call_staged"
                yield PlannerEvent(
                    event=event_name,
                    payload={
                        "id": tc.id,
                        "name": tc.name,
                        "node_id": _payload_node_id(result.staged_node_payload),
                        "predicted_output_schema": result.predicted_output_schema,
                        "warnings": list(result.warnings),
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
    "PlannerEvent",
    "PlannerEventName",
    "RATIONALE_MAX_LEN",
    "run_planner_session",
]
