"""Planner main loop + followup-message injection.

The async generator :func:`run_planner_session` is the public entry
point. It snapshots the graph (drift detection), narrows the tool
catalog per stage, calls the LLM, dispatches each tool call through
the executor in ``mode="stage"`` (or ``mode="apply"`` for ``agent_live``),
and yields :class:`PlannerEvent`s that the SSE wrapper streams.

Every failure mode becomes a typed ``PlannerEvent`` — the generator
never raises so the route handler can close the SSE cleanly without a
structured exception escaping the boundary.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any, Literal

from flowfile_core.ai import audit as audit_module
from flowfile_core.ai import diff as diff_module
from flowfile_core.ai import safety, sessions
from flowfile_core.ai.providers.base import Message, Provider, ToolCall
from flowfile_core.ai.scheduler import RateLimitScheduler, default_scheduler
from flowfile_core.ai.tools.classification import classify_node_type
from flowfile_core.ai.tools.dry_run import DryRunCache
from flowfile_core.ai.tools.executor import (
    InsertionContext,
    ToolExecutionResult,  # noqa: F401  (re-exported via __init__ for tests)
    execute_tool_call,
)
from flowfile_core.ai.tools.meta_ops import (
    CLASSIFY_INTENT_TOOL_NAME,
    EMIT_PLAN_TOOL_NAME,
    PICK_NODE_TYPE_TOOL_NAME,
    PICK_UPSTREAM_TOOL_NAME,
    VERIFY_COMPLETION_TOOL_NAME,
)
from flowfile_core.ai.tools.registry import (
    build_tool_catalog,
    get_staged_fill_inner_field_name,
)

from ._internal import (
    _ADD_PREFIX,
    _MANDATORY_TOOL_CALL_STAGES,
    _STAGED_SINGLE_OP_TOOL_NAMES,
    _STAGED_STATE_MACHINE_SURFACES,
    DEFAULT_MAX_RETRIES_PER_STEP,
    DEFAULT_MAX_TOKENS,
    PlannerEvent,
    PlannerEventName,
    RATIONALE_MAX_LEN,
    _check_self_loop,
    _classify_op_kind,
    _collect_live_node_ids,
    _op_count,
)
from .catalog import (
    _build_staged_tool_catalog,
    _log_stage_transition,
    _resolve_current_surface,
)
from .coercions import _coerce_formula_bare_string_args, _looks_like_outer_envelope_value
from .insertion import (
    _allocate_node_id,
    _count_prior_staged_with_same_upstream,
    _collect_staged_upstream_positions,
    _resolve_insertion_context,
)
from .llm_replies import _payload_node_id, _summarise_result_for_llm
from .messages import _build_initial_messages, _refresh_system_prompt_for_stage
from .rationale import _arg_summary, _capture_rationale, _looks_like_question
from .recovery import _recover_textual_tool_call
from .staged_schemas import _collect_staged_upstream_schemas

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_graph import FlowGraph

logger = logging.getLogger(__name__)


# Staged-flow tool name lookups. Mirrored from ``meta_ops`` so the loop
# can branch on tool name without re-importing the constants in the loop
# hot path.
_EMIT_PLAN_NAME = EMIT_PLAN_TOOL_NAME
_CLASSIFY_INTENT_NAME = CLASSIFY_INTENT_TOOL_NAME
_PICK_NODE_TYPE_NAME = PICK_NODE_TYPE_TOOL_NAME
_VERIFY_COMPLETION_NAME = VERIFY_COMPLETION_TOOL_NAME
_PICK_UPSTREAM_NAME = PICK_UPSTREAM_TOOL_NAME


# --------------------------------------------------------------------------- #
# Followup-message injection                                                   #
# --------------------------------------------------------------------------- #


FollowupAction = Literal["rejected_diff", "user_message"]


_REJECTED_DIFF_DEFAULT_NOTE: str = "no specific reason provided"
"""Generic rejection reason used by :func:`inject_followup_message` when the
user clicks Reject without typing an explanation. Surfaces in the synthetic
followup turn so the model sees *something* signalling the rejection rather
than just an empty user message."""


def inject_followup_message(
    session: sessions.AgentSession,
    *,
    action: FollowupAction,
    message: str | None = None,
    rejected_diff_id: str | None = None,
) -> Message:
    """Append the synthetic followup turn to ``session.messages``.

    Two action shapes:

    * ``"rejected_diff"`` — the user clicked Reject on a staged diff. The
      synthesised content carries the optional user-supplied note (or a
      generic *"no specific reason provided"* fallback) plus the rejected
      ``diff_id`` for diagnostics so the model sees an explicit "course
      correct" signal rather than re-emitting the same plan.
    * ``"user_message"`` — the user typed a new instruction after a
      ``complete`` / ``awaiting_user_input``. The text is appended verbatim
      as the next user turn.

    **Why ``role="user"`` instead of ``role="tool"``** — a ``role="tool"``
    message must be paired with a preceding assistant turn whose
    ``tool_calls`` carries the matching ``tool_call_id``. By the time the
    planner has completed, the last assistant turn has no tool calls (that
    is what triggered the completion); injecting an unpaired ``tool``
    message would be rejected by litellm / Anthropic / OpenAI on the next
    chat call. ``role="user"`` is semantically equivalent to "the human
    sent feedback" and works across every provider.

    The function mutates ``session.messages`` in place and returns the
    appended :class:`Message` for the caller's bookkeeping. Callers
    (``agent_routes.followup``) typically follow this with
    :func:`run_planner_session` so the planner's followup-resume entry path
    re-snapshots the graph + drops staged bookkeeping before the next chat
    call.
    """
    if action == "rejected_diff":
        note = (message or "").strip() or _REJECTED_DIFF_DEFAULT_NOTE
        diff_ref = rejected_diff_id or session.diff_id or "unknown"
        content = (
            "[The user rejected the previously staged diff "
            f"(diff_id={diff_ref}).]\n"
            f"User's reason: {note}\n\n"
            "Treat this as authoritative feedback: do not re-emit the same plan. "
            "Re-plan based on the user's reason; if the reason names a different "
            "upstream node or a different transformation, follow that lead."
        )
    elif action == "user_message":
        content = (message or "").strip()
        if not content:
            raise ValueError("user_message followup requires a non-empty message")
    else:
        raise ValueError(f"unknown followup action: {action!r}")

    msg = Message(role="user", content=content)
    session.messages.append(msg)
    return msg


# --------------------------------------------------------------------------- #
# Public entry + loop                                                          #
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
    exhausts. The disk-persistence layer mirrors these fields verbatim.
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
    # ``aborted`` is honored as an early-exit (the abort route flips
    # status to ``aborted`` between iterations). Same for ``running``
    # (normal start), ``paused_drift`` (resume after drift detection),
    # ``paused_user_action`` (cold-start re-attach), and ``completed`` /
    # ``awaiting_user_input`` (post-completion followup re-entry).
    if session.status == "aborted":
        yield PlannerEvent(event="abort", payload={"session_id": session.session_id})
        return
    if session.status not in (
        "running",
        "paused_drift",
        "paused_user_action",
        "completed",
        "awaiting_user_input",
    ):
        yield PlannerEvent(
            event="error",
            payload={"message": f"cannot run session in status {session.status!r}"},
        )
        return

    if session.status in ("completed", "awaiting_user_input"):
        # Post-completion followup re-entry. The route already appended
        # the synthetic ``user`` / ``tool`` message that drives the next
        # planner turn; everything we do here is housekeeping so the
        # resumed run starts from a clean slate:
        #
        # * **Re-snapshot the graph.** The user may have mutated the
        #   canvas between completion and the followup, so we capture a
        #   fresh baseline. The very next ``detect_drift`` round compares
        #   against this; if drift fires, the loop pauses exactly like a
        #   mid-run mutation would.
        # * **Drop the prior diff bookkeeping.** ``staged_results`` /
        #   ``staged_node_ids`` / ``diff_id`` reflect the *previous* round
        #   (rejected, abandoned, or never-bundled). Keeping them would
        #   double-count node ids in ``_allocate_node_id`` and re-bundle
        #   already-rejected ops on the next ``complete``.
        # * **Reset retry / drift bookkeeping.** ``rationale`` is rebuilt
        #   from the next assistant turn; ``last_assistant_text`` was
        #   used by question-detection on the prior completion and is no
        #   longer authoritative.
        was_completion = session.status
        session.snapshot = sessions.capture_graph_snapshot(flow)
        session.staged_results = []
        session.staged_node_ids = []
        session.diff_id = None
        session.rationale = None
        session.drift_detail = None
        session.pause_reason = None
        session.last_assistant_text = None
        session.status = "running"
        session.touch()
        yield PlannerEvent(
            event="info",
            payload={
                "message": "followup; re-snapshotted graph",
                "previous_status": was_completion,
            },
        )

    if session.status == "paused_user_action":
        # Cold-start resume. The previous SSE stream is dead and an
        # arbitrary amount of time has passed. Re-snapshot, clear the
        # pause reason, flip to running. We don't revalidate
        # staged_results eagerly here — the subsequent drift_detect run
        # will surface any missing upstream as a paused_drift event (and
        # that path already owns
        # ``revalidate_staged_results_against_live``).
        session.snapshot = sessions.capture_graph_snapshot(flow)
        session.pause_reason = None
        session.status = "running"

    if session.status == "paused_drift":
        # Resume after drift — re-snapshot so subsequent drift_detect compares against fresh state.
        session.snapshot = sessions.capture_graph_snapshot(flow)
        session.drift_detail = None
        session.pause_reason = None
        session.status = "running"

        # Staged_results hygiene. Drop entries whose node_id now
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

        # Drift check before every dispatch. ``staged_node_ids``
        # excludes the agent's own staged additions from the
        # external-added bucket so the planner doesn't self-pause on its
        # own work.
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

        current_surface = _resolve_current_surface(session)

        # Refresh the system prompt for the current stage. The first
        # iteration's system prompt was set in ``_build_initial_messages``;
        # subsequent stage advances need a re-render so the LLM sees the
        # right per-stage suffix and (at fill_settings) the picked type's
        # single-node block. The helper also slims the user message at
        # fill_settings to drop the full subgraph noise — passing
        # ``flow`` lets it resolve the picked upstream's predicted
        # schema for the focused mini-prompt.
        _refresh_system_prompt_for_stage(session, flow)

        try:
            if session.surface in _STAGED_STATE_MACHINE_SURFACES and session.stage in ("pick_upstream", "fill_settings"):
                tool_catalog, dyn_err = _build_staged_tool_catalog(session, flow)
                if dyn_err is not None:
                    session.status = "failed"
                    session.touch()
                    yield PlannerEvent(event="error", payload={"message": dyn_err})
                    return
            else:
                tool_catalog = build_tool_catalog(surface=current_surface)
        except KeyError as exc:
            session.status = "failed"
            session.touch()
            yield PlannerEvent(event="error", payload={"message": f"unknown surface: {exc}"})
            return

        # --- Provider call ---
        # Pass surface/session_id/user_id through so the prompt log
        # (FLOWFILE_AI_LOG_PROMPTS=true) tags each entry with the stage
        # the call came from. Without this, every planner entry lands as
        # ``surface=null`` and you can't grep by stage post-hoc.
        try:
            async with sched.acquire(provider.name, surface=current_surface):
                response = await provider.chat(
                    messages=session.messages,
                    tools=tool_catalog,
                    max_tokens=max_tokens,
                    surface=current_surface,
                    session_id=session.session_id,
                    user_id=session.user_id,
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

        # Last-resort text-JSON recovery for small models that emit the
        # function call as content prose instead of via the
        # function-calling API. Applied at ALL agent_staged stages —
        # llama-3.3-8b can fail this way at stage 0 too, not just stage
        # 3. The "exactly one match" + "name must be in
        # expected_tool_names" rules in ``_recover_textual_tool_call``
        # are sufficient to defend against the after-add summary pattern
        # (multiple distinct tool names → declines).
        if (
            not tool_calls
            and assistant_text
            and session.surface in _STAGED_STATE_MACHINE_SURFACES
        ):
            expected_names = {t.name for t in tool_catalog}
            recovered = _recover_textual_tool_call(assistant_text, expected_names)
            if recovered is not None:
                tool_calls = [recovered]
                # Patch the assistant message we just appended so the
                # provider sees a coherent history on subsequent rounds
                # — the recovered tool_call gets paired with a
                # ``role="tool"`` reply downstream, which requires the
                # corresponding tool_calls to have appeared on the
                # prior assistant turn.
                session.messages[-1] = Message(
                    role="assistant",
                    content=assistant_text or None,
                    tool_calls=[recovered],
                )
                logger.info(
                    "planner.staged session=%s recovered_text_json_tool_call tool=%s stage=%s",
                    session.session_id[:8],
                    recovered.name,
                    session.stage,
                )

        # When the assistant turn is pure prose (no tool calls), surface
        # it as a ``thinking`` event so the user sees what the model
        # said. When tool calls follow, the same text rides on each
        # ``tool_call_*`` event as ``rationale``, so emitting both would
        # render the same sentence twice in the chat trail.
        if assistant_text and not tool_calls:
            yield PlannerEvent(event="thinking", payload={"text": assistant_text})

        if not tool_calls:
            # At mandatory-tool-call stages, an empty ``tool_calls``
            # (even after text-JSON recovery tried) means the LLM emitted
            # unparseable prose — e.g. llama-70b's token-corruption case
            # ("altimoreFiltering to rows ..."). Treat it as a
            # no-progress retry instead of a terminal "nothing to
            # stage": append a synthetic reminder so the next round
            # explicitly tells the LLM what's expected, and route
            # through the existing ``max_retries_per_step`` budget.
            # Classify stage stays on the legacy break path — there an
            # empty ``tool_calls`` is a valid termination signal
            # (op_kind="other" handled, or LLM has nothing more to add).
            if (
                session.surface in _STAGED_STATE_MACHINE_SURFACES
                and session.stage in _MANDATORY_TOOL_CALL_STAGES
            ):
                retries_for_step += 1
                if retries_for_step >= max_retries_per_step:
                    session.status = "failed"
                    session.touch()
                    yield PlannerEvent(
                        event="error",
                        payload={
                            "message": (
                                f"agent_staged stage={session.stage}: LLM emitted "
                                f"prose with no tool_call across "
                                f"{max_retries_per_step} consecutive rounds; check "
                                "the prompt log for the model's response shape"
                            ),
                        },
                    )
                    return
                expected_tool = next(
                    iter(t.name for t in tool_catalog), "the available tool"
                )
                session.messages.append(
                    Message(
                        role="user",
                        content=(
                            f"Your previous response was prose only — no function "
                            f"call. You MUST call ``{expected_tool}`` via the "
                            f"function-calling API to advance. Do not write the "
                            f"call as text in your response; emit a real tool "
                            f"call with the correct arguments."
                        ),
                    )
                )
                yield PlannerEvent(
                    event="retry",
                    payload={
                        "attempt": retries_for_step,
                        "max": max_retries_per_step,
                    },
                )
                continue
            # All other stages / surfaces — preserve the existing
            # termination path so legacy ``agent`` / ``agent_complex``
            # and the ``classify`` stage of ``agent_staged`` keep
            # their current "LLM is done" semantics.
            break

        any_succeeded_this_round = False

        # Capture the assistant preamble that landed alongside this
        # turn's tool calls; it's the natural-language "what this step
        # does" that the planner.md prompt asks the model to emit.
        # Shared across every tool call in this round (the model writes
        # one preamble per turn, even when it ends up emitting multiple
        # calls). Falls back to ``None`` when the model skipped the
        # preamble — the per-call ``arg_summary`` covers the rendering
        # gap.
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
            # At fill_settings on agent_staged, the LLM-facing tool
            # spec exposes only the inner-input shape (e.g.
            # ``GroupByInput``: top-level ``agg_cols``) for single-input
            # node types. The executor's settings validation expects the
            # full Pydantic envelope
            # (``NodeGroupBy.groupby_input.agg_cols``), so wrap the
            # LLM's args under the resolved field name before the
            # existing flow_id / node_id injection runs. Multi-field
            # types fall back to the flat-stripped spec (no wrap needed).
            if (
                session.surface in _STAGED_STATE_MACHINE_SURFACES
                and session.stage == "fill_settings"
                and tc.name.startswith(_ADD_PREFIX)
            ):
                picked_type = tc.name.removeprefix(_ADD_PREFIX)
                # Auto-coerce the formula bare-string shape before the
                # inner-input wrap runs. The LLM emits
                # ``{"function": "<expression>"}`` (one key, value is
                # the formula text) when it confuses the inner
                # ``function`` STRING field with the outer ``function``
                # OBJECT wrapper. Coerce to the proper FunctionInput
                # envelope so the wrap can proceed normally.
                if picked_type == "formula":
                    tool_args = _coerce_formula_bare_string_args(
                        tool_args, user_prompt=session.user_prompt
                    )
                inner_field = get_staged_fill_inner_field_name(picked_type)
                # Wrap inner-shape args under the canonical envelope
                # field name. Stronger outer-envelope detection than the
                # naive ``inner_field in tool_args``: the value at
                # ``inner_field`` must be a dict OR a JSON-encoded string
                # that parses to a dict (the universal unwrap will
                # handle the latter at the executor seam). A bare scalar
                # (e.g. FunctionInput's inner ``function`` STRING that
                # the LLM mistakenly placed at the wrapper level for
                # formula) is collision noise and SHOULD be wrapped.
                already_wrapped = (
                    inner_field is not None
                    and inner_field in tool_args
                    and _looks_like_outer_envelope_value(tool_args.get(inner_field))
                )
                if inner_field is not None and not already_wrapped:
                    inner_args = {
                        k: v
                        for k, v in tool_args.items()
                        if k not in {"flow_id", "node_id", "upstream_node_ids", "right_input_node_id"}
                    }
                    tool_args = {
                        k: v
                        for k, v in tool_args.items()
                        if k in {"flow_id", "node_id", "upstream_node_ids", "right_input_node_id"}
                    }
                    tool_args[inner_field] = inner_args
            # Capture provenance: did the LLM emit ``node_id`` itself,
            # or did the planner allocate? Both values flow into
            # audit_meta so the audit row alone shows whether a
            # self-loop traced back to an LLM hallucination or a planner
            # allocation collision.
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

            insertion_context, ambiguous_detail = _resolve_insertion_context(session, tc, flow)

            # Build audit_meta for instrumentation. Rides on
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
            elif tc.name == "flowfile.graph.connect":
                # W71 v2.14 — pre-filter source-only staged ids so the
                # executor's ``unrequested_wire_to_live`` backstop can
                # refuse wires from freshly-staged source nodes into
                # pre-existing live nodes. The list is snapshot-at-stage
                # so any later acceptance of the diff (which transitions
                # ids from staged → live) naturally disables the guard
                # on subsequent rounds.
                staged_source_node_ids: list[int] = []
                for entry in session.staged_results:
                    if not entry.tool_name.startswith(_ADD_PREFIX):
                        continue
                    entry_node_type = entry.tool_name.removeprefix(_ADD_PREFIX)
                    if classify_node_type(entry_node_type) != "source":
                        continue
                    entry_nid = sessions._entry_node_id(entry)
                    if isinstance(entry_nid, int):
                        staged_source_node_ids.append(entry_nid)
                audit_meta = {
                    "live_node_ids_at_stage": sorted(_collect_live_node_ids(flow)),
                    "staged_node_ids_at_stage": list(session.staged_node_ids),
                    "staged_source_node_ids_at_stage": staged_source_node_ids,
                }

            # Universal self-loop invariant guard. Catches all three
            # plausible upstream causes (LLM-provided collision, stale
            # staged_results post-resume, live-graph drift). When it
            # fires, treat as ``tool_call_rejected`` with refusal_reason
            # ``self_loop_prevented``; counts toward the retry budget;
            # writes its own audit row (we never reach
            # execute_tool_call, which is what would otherwise persist
            # the audit).
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

            # Count prior staged adds anchored at the same upstream so
            # fan-outs stack vertically rather than overlap. ``add_*``
            # calls only; non-add ops aren't laid out. Also build a
            # position map for in-batch staged-but-unapplied upstreams so
            # the executor's resolver can anchor chained adds (filter →
            # sort) onto the prior staged add — which by definition
            # isn't in ``flow.nodes`` yet.
            if tc.name.startswith(_ADD_PREFIX):
                staged_offset_index = _count_prior_staged_with_same_upstream(
                    session, insertion_context.upstream_node_ids
                )
                extra_upstream_positions: dict[int, tuple[float, float]] | None = (
                    _collect_staged_upstream_positions(session) or None
                )
            else:
                staged_offset_index = 0
                extra_upstream_positions = None
            # Staged-but-not-yet-applied upstream schemas. Threaded
            # through every dispatch (add_* AND update_node_settings) so
            # the predictor's Tier 0a sees them and stops emitting
            # *"upstream not found"* warnings for nodes the agent staged
            # earlier in the same session.
            extra_upstream_schemas: dict[int, Any] | None = (
                _collect_staged_upstream_schemas(session) or None
            )

            # ``agent_live`` applies LIVE to the canvas
            # (mode="apply"), then runs a post-apply observation (real
            # subgraph run in Performance / schema-eval in Development)
            # and feeds the runtime outcome back to the LLM.
            # ``agent_staged`` + ``agent_complex`` stay on mode="stage" —
            # they bundle the staged ops into a diff for batch user
            # review at the end.
            dispatch_mode: str = (
                "apply" if session.surface == "agent_live" else "stage"
            )
            # Dispatch — execute_tool_call is meant to never raise (returns rejected
            # result instead) but we wrap defensively. Offload to a worker
            # thread: the apply path runs ``flow.add_<node_type>(settings)``
            # which can fall through ``_predicted_data_getter`` into a
            # synchronous worker request (e.g. explore_data's
            # ``analysis_preparation`` calls ``get_number_of_records`` on a
            # predicted-empty frame, which dispatches to ``ExternalDfFetcher``
            # with ``wait_on_completion=True``). That sync HTTP/websocket
            # call must not run on the FastAPI event loop or the worker's
            # completion frames pile up against an unread socket and the
            # whole core process freezes.
            try:
                result = await asyncio.to_thread(
                    execute_tool_call,
                    flow_id=session.flow_id,
                    tool_name=tc.name,
                    tool_args=tool_args,
                    insertion_context=insertion_context,
                    session_id=session.session_id,
                    user_id=session.user_id,
                    mode=dispatch_mode,  # type: ignore[arg-type]
                    flow=flow,
                    dry_run_cache=dry_run_cache,
                    llm_provided_node_id=llm_provided_node_id,
                    audit_meta=audit_meta,
                    staged_offset_index=staged_offset_index,
                    extra_upstream_positions=extra_upstream_positions,
                    extra_upstream_schemas=extra_upstream_schemas,
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

            # ``agent_live`` post-apply observation. The node is
            # already in ``flow.nodes`` (mode="apply"); now observe the
            # runtime outcome and decide whether to commit or auto-undo.
            # On observation failure we delete the just-added node and
            # feed the runtime error back to the LLM as the next round's
            # tool reply, counting toward the per-step retry budget.
            if (
                session.surface == "agent_live"
                and tc.name.startswith(_ADD_PREFIX)
                and result.status in ("applied", "warned")
                and isinstance(result.node_id, int)
            ):
                from flowfile_core.ai.agents.live_observation import (
                    format_observation_block,
                    observe_after_apply,
                )

                live_node_id = result.node_id
                # observe_after_apply is async: Performance mode runs
                # ``flow.trigger_fetch_node`` under the per-flow
                # ``flow_run_lock`` (matching ``routes.trigger_fetch_node_data``)
                # so AI observations are serialised with concurrent UI runs.
                # Both modes offload the synchronous polars / worker IO via
                # ``asyncio.to_thread`` so the SSE generator's event loop
                # stays free.
                obs = await observe_after_apply(flow, live_node_id)
                obs_block = format_observation_block(obs)

                # Append the observation block to the existing tool
                # reply so the LLM sees it on its next round.
                tool_msg.content = (tool_msg.content or "").rstrip() + "\n\n" + obs_block

                if not obs.success:
                    # Auto-undo: delete the just-added node so the
                    # canvas returns to the last successful state.
                    try:
                        await asyncio.to_thread(flow.delete_node, live_node_id)
                    except Exception:
                        logger.exception(
                            "agent_live: undo (delete_node=%s) failed",
                            live_node_id,
                        )

                    retries_for_step += 1
                    if retries_for_step >= max_retries_per_step:
                        session.status = "failed"
                        session.touch()
                        yield PlannerEvent(
                            event="error",
                            payload={
                                "message": (
                                    f"agent_live: {max_retries_per_step} consecutive "
                                    "runtime failures on the same step "
                                    f"(node_type={tc.name.removeprefix(_ADD_PREFIX)}); "
                                    "giving up and leaving the canvas at the last "
                                    "successful state."
                                ),
                                "session_id": session.session_id,
                            },
                        )
                        return
                    yield PlannerEvent(
                        event="tool_call_rejected",
                        payload={
                            "id": tc.id,
                            "name": tc.name,
                            "reason": "runtime_error",
                            "detail": obs.error_message,
                            "op_kind": op_kind,
                            "rationale": rationale,
                            "arg_summary": arg_summary,
                        },
                    )
                    continue

                # Observation succeeded — record the applied node
                # and reset the per-step retry budget.
                session.applied_results.append(
                    diff_module.AppliedNodeRecord(
                        tool_name=tc.name,
                        audit_id=result.audit_id,
                        node_id=live_node_id,
                        node_type=tc.name.removeprefix(_ADD_PREFIX),
                        rationale=rationale or "",
                        output_schema=obs.output_schema,
                    )
                )
                retries_for_step = 0
                if live_node_id not in session.staged_node_ids:
                    session.staged_node_ids.append(live_node_id)

                # Reset stage so the next round starts a fresh
                # classify→pick→fill cycle (multi-node turns
                # serialize as N×4 rounds, same as agent_staged).
                prev_stage = session.stage
                sessions.reset_stage_state(session)
                _log_stage_transition(
                    session,
                    from_stage=prev_stage,
                    to_stage=session.stage,
                    tool_name=tc.name,
                    completed_op=tc.name,
                )
                yield PlannerEvent(
                    event="tool_call_applied",
                    payload={
                        "id": tc.id,
                        "name": tc.name,
                        "node_id": live_node_id,
                        "output_schema": obs.output_schema,
                        "sample_rows": obs.sample_rows,
                        "op_kind": op_kind,
                        "rationale": rationale,
                        "arg_summary": arg_summary,
                    },
                )
                yield PlannerEvent(
                    event="stage_advanced",
                    payload={
                        "from": prev_stage,
                        "to": session.stage,
                        "completed_op": tc.name,
                        "session_id": session.session_id,
                        "op_kind_meta": "live_apply",
                    },
                )
                continue

            # Stage transitions on meta tool success. Each meta tool
            # sets a piece of session state and advances the stage; the
            # next loop iteration re-renders the system prompt and
            # exposes the next stage's tool. ``stage_advanced`` is
            # surfaced to the frontend so the agent panel can render the
            # current step ("Step 2/4: picking upstream").

            # Plan stage runs once at session start. The LLM emits a
            # brief markdown plan via ``flowfile.meta.emit_plan``; the
            # planner records the plan, surfaces it in the chat trail,
            # and advances to ``classify`` so the normal state machine
            # takes over.
            if (
                session.surface in _STAGED_STATE_MACHINE_SURFACES
                and tc.name == _EMIT_PLAN_NAME
                and isinstance(result.extra, dict)
            ):
                plan_text = str(result.extra.get("plan") or "").strip()
                rationale = str(result.extra.get("rationale") or "")
                prev_stage = session.stage
                session.stage = "classify"
                _log_stage_transition(
                    session,
                    from_stage=prev_stage,
                    to_stage=session.stage,
                    tool_name=tc.name,
                )
                yield PlannerEvent(
                    event="stage_advanced",
                    payload={
                        "from": prev_stage,
                        "to": session.stage,
                        "plan": plan_text,
                        "rationale": rationale,
                        "session_id": session.session_id,
                        "op_kind_meta": "plan",
                    },
                )
                any_succeeded_this_round = True
                continue

            if (
                session.surface in _STAGED_STATE_MACHINE_SURFACES
                and tc.name == _CLASSIFY_INTENT_NAME
                and isinstance(result.extra, dict)
            ):
                op_kind = result.extra.get("op_kind")
                rationale = str(result.extra.get("rationale") or "")
                prev_stage = session.stage
                if isinstance(op_kind, str):
                    session.picked_op_kind = op_kind  # type: ignore[assignment]
                    if op_kind == "add":
                        session.stage = "pick_type"
                    elif op_kind in ("modify", "delete", "connect", "disconnect"):
                        session.stage = "single_stage_op"
                    elif (
                        op_kind == "other"
                        and session.verify_plan_completion
                        and not session.verify_round_consumed
                    ):
                        # Opt-in verify-completion gate. One extra LLM
                        # round at ``verify_completion`` confirms the
                        # plan is done before the loop terminates. The
                        # one-shot ``verify_round_consumed`` guard (set
                        # in the verify-result handler below) prevents
                        # ping-pong if a follow-up classify also picks
                        # ``op_kind="other"``.
                        session.stage = "verify_completion"
                    # ``other`` (no verify gate / already consumed) leaves
                    # stage at ``classify`` — the loop will call the LLM
                    # again, which sees the rationale already in history
                    # and will most likely emit no tool call, ending the
                    # loop with the rationale as the final assistant
                    # message (question detection still routes to
                    # ``awaiting_user_input`` if it ends in a question).
                _log_stage_transition(
                    session,
                    from_stage=prev_stage,
                    to_stage=session.stage,
                    tool_name=tc.name,
                    op_kind=op_kind if isinstance(op_kind, str) else None,
                )
                yield PlannerEvent(
                    event="stage_advanced",
                    payload={
                        "from": prev_stage,
                        "to": session.stage,
                        "op_kind": op_kind,
                        "rationale": rationale,
                        "session_id": session.session_id,
                        "op_kind_meta": "meta",
                    },
                )
                any_succeeded_this_round = True
                continue

            if (
                session.surface in _STAGED_STATE_MACHINE_SURFACES
                and tc.name == _VERIFY_COMPLETION_NAME
                and isinstance(result.extra, dict)
            ):
                # Opt-in verify-completion gate result.
                is_complete_raw = result.extra.get("is_complete")
                is_complete = is_complete_raw is True
                rationale = str(result.extra.get("rationale") or "")
                prev_stage = session.stage
                # One-shot guard: mark consumed regardless of the LLM's
                # answer so a stubborn ``is_complete=false`` followed by
                # another classify→other can't ping-pong back into
                # verify. The cap is per-session, not per-stage.
                session.verify_round_consumed = True
                if is_complete:
                    # Plan is done. Route stage back to ``classify``;
                    # the LLM sees the verify rationale + the prior
                    # classify rationale in history and emits no tool
                    # call on the next round, breaking the loop via
                    # the standard non-mandatory-stage termination
                    # path. Same posture as the no-verify
                    # ``op_kind="other"`` path above.
                    session.stage = "classify"
                else:
                    # is_complete=False (or non-true). Reset to a
                    # fresh classify cycle so the LLM picks the next
                    # op_kind for the remaining work; the rationale
                    # in history names the missing step(s).
                    sessions.reset_stage_state(session)
                _log_stage_transition(
                    session,
                    from_stage=prev_stage,
                    to_stage=session.stage,
                    tool_name=tc.name,
                )
                yield PlannerEvent(
                    event="stage_advanced",
                    payload={
                        "from": prev_stage,
                        "to": session.stage,
                        "is_complete": is_complete,
                        "rationale": rationale,
                        "session_id": session.session_id,
                        "op_kind_meta": "meta",
                    },
                )
                any_succeeded_this_round = True
                continue

            if (
                session.surface in _STAGED_STATE_MACHINE_SURFACES
                and tc.name == _PICK_NODE_TYPE_NAME
                and isinstance(result.extra, dict)
            ):
                node_type = result.extra.get("node_type")
                rationale = str(result.extra.get("rationale") or "")
                prev_stage = session.stage
                if isinstance(node_type, str):
                    session.picked_node_type = node_type
                    session.stage = "pick_upstream"
                _log_stage_transition(
                    session,
                    from_stage=prev_stage,
                    to_stage=session.stage,
                    tool_name=tc.name,
                    node_type=node_type if isinstance(node_type, str) else None,
                )
                yield PlannerEvent(
                    event="stage_advanced",
                    payload={
                        "from": prev_stage,
                        "to": session.stage,
                        "picked_node_type": node_type,
                        "rationale": rationale,
                        "session_id": session.session_id,
                        "op_kind_meta": "meta",
                    },
                )
                any_succeeded_this_round = True
                continue

            if (
                session.surface in _STAGED_STATE_MACHINE_SURFACES
                and tc.name == _PICK_UPSTREAM_NAME
                and isinstance(result.extra, dict)
            ):
                upstream_ids_raw = result.extra.get("upstream_node_ids")
                right_input_raw = result.extra.get("right_input_node_id")
                rationale = str(result.extra.get("rationale") or "")
                prev_stage = session.stage
                if isinstance(upstream_ids_raw, list):
                    session.picked_upstream_ids = [
                        u for u in upstream_ids_raw if isinstance(u, int)
                    ]
                    session.picked_right_input_id = (
                        right_input_raw if isinstance(right_input_raw, int) else None
                    )
                    session.stage = "fill_settings"
                _log_stage_transition(
                    session,
                    from_stage=prev_stage,
                    to_stage=session.stage,
                    tool_name=tc.name,
                    upstream_node_ids=list(session.picked_upstream_ids),
                )
                yield PlannerEvent(
                    event="stage_advanced",
                    payload={
                        "from": prev_stage,
                        "to": session.stage,
                        "picked_upstream_ids": session.picked_upstream_ids,
                        "right_input_node_id": session.picked_right_input_id,
                        "rationale": rationale,
                        "session_id": session.session_id,
                        "op_kind_meta": "meta",
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
                    # checks can exclude them from external-added
                    # detection. Only ``add_<node_type>`` calls produce
                    # a node_id — connection / delete payloads don't.
                    if tc.name.startswith(_ADD_PREFIX):
                        staged_id = _payload_node_id(result.staged_node_payload)
                        if staged_id is not None and staged_id not in session.staged_node_ids:
                            session.staged_node_ids.append(staged_id)
                event_name: PlannerEventName = "tool_call_warned" if result.status == "warned" else "tool_call_staged"
                # Re-persist the session after each staging so a process
                # crash mid-loop can resume against the up-to-date
                # ``staged_results`` / ``staged_node_ids``. Best-effort:
                # a checkpoint failure must not stall the planner.
                try:
                    session.touch()
                    # FileLock + JSON write inside register_session is sync;
                    # offload so the planner SSE generator doesn't stall
                    # while the lock is contended.
                    await asyncio.to_thread(sessions.register_session, session)
                except Exception:
                    logger.exception(
                        "planner: session checkpoint after tool_call_staged failed (session=%s)",
                        session.session_id,
                    )
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
                # On agent_staged, reset the state machine after each
                # successful add_* (stage 3) or single-stage non-add op
                # so the next round starts a fresh classify→pick→fill
                # cycle. Multi-node turns serialize naturally as N×4
                # rounds without any history pruning.
                if session.surface in _STAGED_STATE_MACHINE_SURFACES and (
                    tc.name.startswith(_ADD_PREFIX)
                    or tc.name in _STAGED_SINGLE_OP_TOOL_NAMES
                ):
                    prev_stage = session.stage
                    sessions.reset_stage_state(session)
                    _log_stage_transition(
                        session,
                        from_stage=prev_stage,
                        to_stage=session.stage,
                        tool_name=tc.name,
                        completed_op=tc.name,
                    )
                    yield PlannerEvent(
                        event="stage_advanced",
                        payload={
                            "from": prev_stage,
                            "to": session.stage,
                            "session_id": session.session_id,
                            "completed_op": tc.name,
                            "op_kind_meta": "meta",
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
        # If the agent's last assistant message reads like a clarifying
        # question, this is *not* "agent finished — nothing to stage";
        # the agent is waiting on the user. Flip to
        # ``awaiting_user_input`` and emit a distinct SSE event so the
        # frontend renders the right state. Both ``awaiting_user_input``
        # and ``completed`` are resumable via the ``/followup``
        # endpoint, so the only difference is UX framing and the SSE
        # event name.
        if _looks_like_question(session.last_assistant_text):
            session.status = "awaiting_user_input"
            session.touch()
            yield PlannerEvent(
                event="awaiting_user_input",
                payload={
                    "session_id": session.session_id,
                    "question": session.last_assistant_text,
                },
            )
            return

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
