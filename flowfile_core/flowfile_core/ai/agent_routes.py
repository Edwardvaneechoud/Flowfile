"""HTTP routes for the W40 multi-turn planner agent.

Mounted under ``/ai`` from :mod:`flowfile_core.ai.routes`. Auth via
``Depends(get_current_active_user)``; W17's feature-flag gate covers all
endpoints through the parent ``ai_router``. Sessions are namespaced by
``user_id`` so cross-user access on ``{session_id}/*`` returns ``404`` —
**not** ``403`` — to avoid leaking the existence of someone else's session.

Four endpoints:

* ``POST /ai/agent/start`` — open a session, stream :class:`PlannerEvent`s
  as SSE. The session is registered before the first byte goes out so
  abort/resume routes can find it; the SSE stream itself is the planner's
  primary output.
* ``POST /ai/agent/{session_id}/resume`` — body ``{action: "continue" |
  "discard"}``. ``"continue"`` re-snapshots the graph and resumes the
  generator (SSE response). ``"discard"`` pops the session and frees any
  staged diff (JSON response).
* ``POST /ai/agent/{session_id}/abort`` — flip status to ``aborted`` so
  the in-flight generator's next iteration exits cleanly. JSON response.
  Idempotent: aborting an already-completed/aborted session is a no-op.
* ``GET /ai/agent/{session_id}`` — status snapshot for re-attach (the
  frontend uses this on page-reload to re-render the in-progress state).

Error mapping mirrors W12 / W20 / W23 / W34 / W50 / W51:

* ``404`` — unknown provider, missing flow, unknown session.
* ``409`` — provider not configured; ``resume`` against a non-paused
  session.
* ``422`` — Pydantic validation; ``surface="agent_complex"`` against a
  ``supports_tools=False`` provider.
* ``503`` — ``FEATURE_FLAG_AI`` off (inherited).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.orm import Session

from flowfile_core import flow_file_handler
from flowfile_core.ai import sessions
from flowfile_core.ai.agents.planner import (
    DEFAULT_MAX_RETRIES_PER_STEP,
    DEFAULT_MAX_STEPS,
    DEFAULT_MAX_TOKENS,
    FollowupAction,
    inject_followup_message,
    run_planner_session,
)
from flowfile_core.ai.byok import ProviderNotConfiguredError, get_configured_provider
from flowfile_core.ai.providers.registry import PROVIDERS, UnknownProviderError
from flowfile_core.ai.replay_buffer import default_replay_buffer
from flowfile_core.ai.streaming import (
    make_streaming_response,
    planner_events_sse,
)
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.database.connection import get_db

logger = logging.getLogger(__name__)

router = APIRouter()


# --------------------------------------------------------------------------- #
# Request / response shapes                                                    #
# --------------------------------------------------------------------------- #


class AgentStartRequest(BaseModel):
    """Body for ``POST /ai/agent/start``."""

    model_config = ConfigDict(extra="ignore")

    flow_id: int = Field(ge=0)
    prompt: str = Field(min_length=1, max_length=5_000)
    surface: Literal["agent_complex", "agent_staged", "agent_live"] = "agent_staged"
    """W71 v1.10 — defaults to ``agent_staged`` (multi-stage state
    machine). Big-model power users can opt into ``agent_complex``
    (single-shot full catalog) by passing it explicitly. The legacy
    two-stage ``agent`` surface was removed in v1.10 — it was the
    failure mode that motivated W71 in the first place.
    W71 v2.0 — third option ``agent_live`` mirrors the
    ``agent_staged`` state machine but applies each step LIVE to
    the canvas (mode=apply, not stage), runs the affected subgraph
    (Performance) or evaluates a sample (Development), and feeds
    the runtime observation back to the LLM. Auto-undoes the just-
    added node on runtime failure and retries up to 3×."""
    samples_mode: Literal["off", "regex"] = "off"
    provider: str = Field(default="anthropic", min_length=1)
    model: str | None = None
    max_steps: int = Field(default=DEFAULT_MAX_STEPS, ge=1, le=64)
    max_tokens: int = Field(default=DEFAULT_MAX_TOKENS, ge=64, le=16_384)
    max_retries_per_step: int = Field(default=DEFAULT_MAX_RETRIES_PER_STEP, ge=1, le=8)
    session_id: str | None = None
    """If supplied, the session reuses this id instead of auto-generating one
    — useful for clients that want to pre-allocate session ids for routing.
    Must not collide with an existing session for any user."""
    selected_node_ids: list[int] | None = None
    """W57 — node ids the user has selected on the canvas at start time.
    Mirrors W28's chat-route wire shape. Read by the planner's
    ``_resolve_insertion_context`` as a fallback upstream signal when the
    LLM does not emit an explicit ``upstream_node_ids`` arg. ``None`` and
    empty list both resolve to "no selection" — the resolver will refuse
    on ambiguity rather than guess. Frontend reads this from the live
    flow store at start time."""
    skip_plan: bool = False
    """W71 v2.7 — when True, the agent_staged / agent_live state
    machine starts at ``stage="classify"`` instead of the v2.4
    default ``stage="plan"``. The auto-promote-from-chat path
    (W58 / ``_dispatchPromotedAgent`` in the frontend) sets this
    to True because the chat-mode response that preceded the
    promotion already produced a plan-shaped narrative; emitting
    a fresh plan would cost an extra round and duplicate the
    chat-mode output. Direct agent runs (user typed with the
    agent toggle on) leave this False so the plan stage fires."""
    verify_plan_completion: bool = False
    """W71 v2.12 — opt-in: when True, after classify picks
    ``op_kind="other"`` (intending to terminate), the planner
    runs ONE extra round at ``stage="verify_completion"``. The
    LLM sees a single tool ``flowfile.meta.verify_completion``,
    walks the chat-mode plan as an authoritative checklist, and
    either terminates the loop (``is_complete=true``) or sends
    control back to ``classify`` for another op
    (``is_complete=false``). Default off because of the extra
    LLM round per run; users opt in via a frontend toggle when
    they've seen the agent terminate prematurely on multi-step
    plans (e.g. add a node mid-flow but skip the rewires)."""


class AgentResumeRequest(BaseModel):
    action: Literal["continue", "discard"]


class AgentFollowupRequest(BaseModel):
    """W49 — body for ``POST /ai/agent/{session_id}/followup``.

    Two action shapes feed the same re-entry path:

    * ``"rejected_diff"`` — the user clicked Reject on the staged diff.
      ``message`` is an optional user-supplied rejection note ("the
      upstream is wrong, use the read node directly"); ``rejected_diff_id``
      is the diff the user just rejected (passed through to the synthetic
      tool-message for diagnostics — can be omitted if the session's own
      ``diff_id`` matches).
    * ``"user_message"`` — the user typed a follow-up message after a
      ``complete`` / ``awaiting_user_input``. ``message`` is required and
      becomes the next user turn in the planner conversation.
    """

    model_config = ConfigDict(extra="ignore")

    action: FollowupAction
    message: str | None = Field(default=None, max_length=5_000)
    rejected_diff_id: str | None = None


class AgentAbortResponse(BaseModel):
    status: Literal["aborted"] = "aborted"
    session_id: str
    partial_diff_id: str | None = None


class AgentDiscardResponse(BaseModel):
    status: Literal["discarded"] = "discarded"
    session_id: str


class AgentStateResponse(BaseModel):
    """Public snapshot of an :class:`AgentSession` for re-attach.

    Excludes the internal ``messages`` list (could contain redactable
    content) and the ``snapshot`` (large, internal). Frontend renders
    only the public surface.
    """

    session_id: str
    flow_id: int
    status: sessions.SessionStatus
    surface: sessions.PlannerSurface
    samples_mode: sessions.SamplesMode
    step_count: int
    max_steps: int
    staged_count: int
    diff_id: str | None
    rationale: str | None
    pause_reason: str | None
    drift_detail: sessions.DriftDetail | None
    stage: sessions.PlannerStage
    """W71 — current state-machine stage for ``agent_staged`` sessions.
    Stays at ``"classify"`` for legacy ``agent`` / ``agent_complex``
    surfaces (those don't drive transitions). The frontend reads this
    to render *"Step 1/4: classifying intent"* etc. on the agent panel."""
    picked_op_kind: sessions.PlannerOpKind | None
    """W71 — op kind chosen by the stage-0 ``classify_intent`` call,
    surfaced so the UI can show the in-flight intent (*"Adding a node…"*
    vs *"Modifying node 3…"*). ``None`` until classify resolves."""
    picked_node_type: str | None
    """W71 — node type chosen by stage 1's ``pick_node_type`` call.
    Surfaced so the UI can show the in-flight node type. ``None`` until
    pick_type resolves; reset to ``None`` after each successful add."""
    created_at: str
    updated_at: str


# --------------------------------------------------------------------------- #
# Helpers                                                                      #
# --------------------------------------------------------------------------- #


def _ensure_known_provider(name: str) -> None:
    if name not in PROVIDERS:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown provider {name!r}. Configured providers: {sorted(PROVIDERS)}",
        )


def _resolve_flow(flow_id: int):
    flow = flow_file_handler.get_flow(flow_id)
    if flow is None:
        raise HTTPException(status_code=404, detail=f"Flow {flow_id} not found")
    return flow


def _to_state_response(session: sessions.AgentSession) -> AgentStateResponse:
    return AgentStateResponse(
        session_id=session.session_id,
        flow_id=session.flow_id,
        status=session.status,
        surface=session.surface,
        samples_mode=session.samples_mode,
        step_count=session.step_count,
        max_steps=session.max_steps,
        staged_count=len(session.staged_results),
        diff_id=session.diff_id,
        rationale=session.rationale,
        pause_reason=session.pause_reason,
        drift_detail=session.drift_detail,
        stage=session.stage,
        picked_op_kind=session.picked_op_kind,
        picked_node_type=session.picked_node_type,
        created_at=session.created_at.isoformat(),
        updated_at=session.updated_at.isoformat(),
    )


def _looks_cold_started(session: sessions.AgentSession) -> bool:
    """Heuristic: a ``running`` session whose disk file is older than its
    in-process LRU mirror would be is cold.

    The disk repo's ``get`` fills the LRU on read, so by the time we get
    here the LRU is warm. The signal we actually care about is *"was this
    LRU entry hydrated from disk on this same call?"* — which we approximate
    by inspecting the disk repo type. If the repo is in-memory, an entry
    being absent triggers ``get_session`` to return ``None``; we never get
    here. If the repo is disk-backed and ``get_session`` returned a session,
    we know it came from disk OR cache. There's no cheap way to disambiguate
    without instrumenting the repo, so we conservatively flip on every read
    of a ``running`` session whose ``updated_at`` is more than the
    keepalive interval old (15s). Live planner runs touch ``updated_at``
    much more often than that.
    """
    repo = sessions.get_session_repo()
    repo_kind = type(repo).__name__
    if repo_kind != "DiskSessionRepository":
        return False

    now = datetime.now(timezone.utc)
    delta = (now - session.updated_at).total_seconds()
    # KEEPALIVE_INTERVAL_SECONDS is 15s; any live session would have been
    # touched at least once in that window (planner step boundaries +
    # checkpoint hook). Three-window grace = 45s is generous enough that
    # a tiny in-flight delay never trips the flip during a real run.
    return delta > 45.0


# --------------------------------------------------------------------------- #
# Routes                                                                       #
# --------------------------------------------------------------------------- #


@router.post("/agent/start", tags=["ai"])
async def agent_start(
    body: AgentStartRequest,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Open a planner session and stream events as SSE.

    Errors before the stream opens:

    * ``404`` — unknown provider; missing flow.
    * ``409`` — provider not configured.
    * ``422`` — ``surface="agent_complex"`` on a ``supports_tools=False``
      provider; session_id collision.
    """

    _ensure_known_provider(body.provider)
    flow = _resolve_flow(body.flow_id)

    # Resolve the model with surface-aware routing **before** byok runs so the
    # explicit ``model=`` arg wins. Default behaviour (cred.default_model
    # wins over surface) is right for chat — your chat default is whatever
    # you configured — but wrong for agent: the agent surface needs a tool-
    # capable model. Free-tier defaults like ``minimax-m2.5:free`` can't
    # reliably tool-call AND get rate-limited within seconds. Forcing
    # ``surface_models[surface]`` here means OpenRouter / Groq users on a
    # cheap chat default automatically get routed to a sonnet-class model
    # for agent runs.
    resolved_model = body.model
    if resolved_model is None:
        cls = PROVIDERS.get(body.provider)
        if cls is not None:
            resolved_model = cls.surface_models.get(body.surface)

    try:
        provider = get_configured_provider(
            db,
            current_user.id,
            body.provider,
            surface=body.surface,
            model=resolved_model,
        )
    except ProviderNotConfiguredError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except UnknownProviderError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    if not getattr(provider, "supports_tools", False):
        raise HTTPException(
            status_code=422,
            detail=(
                f"Provider {body.provider!r} does not support tool-calling, which the "
                "planner requires. Switch to a tool-capable provider/model."
            ),
        )

    if body.session_id is not None and sessions.get_session(body.session_id) is not None:
        raise HTTPException(
            status_code=422,
            detail=f"session_id {body.session_id!r} already in use",
        )

    snapshot = sessions.capture_graph_snapshot(flow)
    session_kwargs = {
        "flow_id": body.flow_id,
        "user_id": current_user.id,
        "user_prompt": body.prompt,
        "surface": body.surface,
        "samples_mode": body.samples_mode,
        "provider_name": body.provider,
        "model_name": body.model,
        "snapshot": snapshot,
        "max_steps": body.max_steps,
        "selected_node_ids": list(body.selected_node_ids or []),
    }
    if body.session_id is not None:
        session_kwargs["session_id"] = body.session_id
    # W71 v2.7 — auto-promote-from-chat sets ``skip_plan=True`` so the
    # session jumps straight into ``classify``; the plan stage is
    # redundant when a chat-mode plan already preceded the agent run.
    if body.skip_plan:
        session_kwargs["stage"] = "classify"
    # W71 v2.12 — opt-in verify-completion mode. Default off; users opt
    # in via the frontend toggle when they've seen the agent terminate
    # prematurely on multi-step plans.
    if body.verify_plan_completion:
        session_kwargs["verify_plan_completion"] = True
    session = sessions.AgentSession(**session_kwargs)
    sessions.register_session(session)

    # W71 v1.1 — one structured INFO line per session start. Pairs with the
    # ``planner.staged session=…`` lines emitted on each stage transition,
    # so a single ``grep agent\\.`` against the core's stderr/log file
    # reconstructs every session's surface + provider + model + the full
    # state-machine trajectory without parsing the JSONL prompt log.
    logger.info(
        "agent.start session=%s flow=%s surface=%s provider=%s model=%s",
        session.session_id[:8],
        body.flow_id,
        body.surface,
        body.provider,
        resolved_model,
    )
    # W71 v1.10 — the legacy ``surface=agent`` warning is gone because
    # the surface itself was removed: the literal type rejects "agent"
    # at request validation. The remaining surfaces (agent_staged,
    # agent_complex) are both viable on their respective model tiers.

    events = run_planner_session(
        session=session,
        flow=flow,
        provider=provider,
        max_tokens=body.max_tokens,
        max_retries_per_step=body.max_retries_per_step,
    )
    sse = planner_events_sse(
        events,
        session_id=session.session_id,
        step_count_getter=lambda: session.step_count,
        replay_buffer=default_replay_buffer(),
        flow_id=session.flow_id,
    )
    return make_streaming_response(sse)


@router.post("/agent/{session_id}/resume", tags=["ai"])
async def agent_resume(
    session_id: str,
    body: AgentResumeRequest,
    request: Request,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Resume a paused session (drift OR cold-start ``paused_user_action``).

    * ``action="continue"`` re-snapshots the graph and streams the rest of
      the planner's events as SSE (mirrors ``/start``). When the request
      carries a ``Last-Event-ID`` header (W42), buffered SSE frames newer
      than the cursor are flushed to the client *before* the live planner
      stream resumes.
    * ``action="discard"`` pops the session, returns JSON. Any staged diff
      that was registered before the pause stays — the user can still
      reject it via the W41 ``/ai/diff/{id}/reject`` route.

    Resumable statuses: ``paused_drift`` (D006) and ``paused_user_action``
    (W42 cold-start). Other statuses → 409.

    Errors:

    * ``404`` — unknown session (or owned by a different user).
    * ``409`` — session not in a resumable paused state.
    """
    session = sessions.get_session(session_id, user_id=current_user.id)
    if session is None:
        raise HTTPException(status_code=404, detail=f"Unknown session_id {session_id!r}")
    if session.status not in ("paused_drift", "paused_user_action"):
        raise HTTPException(
            status_code=409,
            detail=f"session {session_id} is not paused (status={session.status!r})",
        )

    if body.action == "discard":
        sessions.pop_session(session_id, user_id=current_user.id)
        return AgentDiscardResponse(session_id=session_id)

    # continue
    flow = _resolve_flow(session.flow_id)
    # Mirror /start's surface-aware model resolution: if the session was
    # opened without an explicit model, force ``surface_models[surface]`` so
    # the resumed run uses the same tool-capable model the original /start
    # picked (rather than falling back to cred.default_model on resume).
    resume_model = session.model_name
    if resume_model is None:
        cls = PROVIDERS.get(session.provider_name)
        if cls is not None:
            resume_model = cls.surface_models.get(session.surface)
    try:
        provider = get_configured_provider(
            db,
            current_user.id,
            session.provider_name,
            surface=session.surface,
            model=resume_model,
        )
    except ProviderNotConfiguredError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except UnknownProviderError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    last_event_id = request.headers.get("last-event-id")

    events = run_planner_session(session=session, flow=flow, provider=provider)
    sse = planner_events_sse(
        events,
        session_id=session.session_id,
        step_count_getter=lambda: session.step_count,
        replay_buffer=default_replay_buffer(),
        flow_id=session.flow_id,
        replay_after_event_id=last_event_id,
    )
    return make_streaming_response(sse)


@router.post("/agent/{session_id}/followup", tags=["ai"])
async def agent_followup(
    session_id: str,
    body: AgentFollowupRequest,
    request: Request,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """W49 — re-enter a completed planner session with a synthetic followup.

    Distinct from ``/resume`` (which targets ``paused_drift`` /
    ``paused_user_action``): ``/followup`` targets ``completed`` and
    ``awaiting_user_input`` so the user can keep the conversation going
    after a diff reject, a clarifying-question turn, or a no-ops
    completion. The route:

    1. Validates the session is in a followup-resumable state.
    2. Re-resolves the original provider / model the session was opened
       with (so a token-budget bump or surface-flip won't change the
       resumed run's model).
    3. Calls :func:`inject_followup_message` to append the synthetic
       ``role="user"`` turn (rejection note or user message text).
    4. Calls :func:`run_planner_session` — the planner's followup-resume
       entry path drops the previous diff bookkeeping, re-snapshots the
       graph (D006), and re-enters the loop with the appended message.

    Reuses the same SSE event types as ``/start`` / ``/resume`` so frontend
    consumers need no new handlers.

    Errors:

    * ``404`` — unknown session (or owned by a different user).
    * ``409`` — session not in a followup-resumable state OR provider
      not configured.
    * ``422`` — Pydantic validation; ``user_message`` action with empty /
      missing ``message``.
    """
    session = sessions.get_session(session_id, user_id=current_user.id)
    if session is None:
        raise HTTPException(status_code=404, detail=f"Unknown session_id {session_id!r}")
    if session.status not in ("completed", "awaiting_user_input"):
        raise HTTPException(
            status_code=409,
            detail=(
                f"session {session_id} is not followup-resumable "
                f"(status={session.status!r}); only completed / awaiting_user_input accepted"
            ),
        )

    flow = _resolve_flow(session.flow_id)

    # Mirror /resume's surface-aware model resolution: if the session was
    # opened without an explicit model, force ``surface_models[surface]`` so
    # the resumed run uses the same tool-capable model the original /start
    # picked.
    resume_model = session.model_name
    if resume_model is None:
        cls = PROVIDERS.get(session.provider_name)
        if cls is not None:
            resume_model = cls.surface_models.get(session.surface)
    try:
        provider = get_configured_provider(
            db,
            current_user.id,
            session.provider_name,
            surface=session.surface,
            model=resume_model,
        )
    except ProviderNotConfiguredError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except UnknownProviderError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    try:
        inject_followup_message(
            session,
            action=body.action,
            message=body.message,
            rejected_diff_id=body.rejected_diff_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    # Persist the updated message history before the SSE generator opens —
    # if the SSE client disconnects mid-stream, the followup turn is
    # already on disk so a subsequent /resume sees it.
    try:
        sessions.register_session(session)
    except Exception:
        logger.exception("agent_followup: session checkpoint failed for session=%s", session_id)

    last_event_id = request.headers.get("last-event-id")

    events = run_planner_session(session=session, flow=flow, provider=provider)
    sse = planner_events_sse(
        events,
        session_id=session.session_id,
        step_count_getter=lambda: session.step_count,
        replay_buffer=default_replay_buffer(),
        flow_id=session.flow_id,
        replay_after_event_id=last_event_id,
    )
    return make_streaming_response(sse)


@router.post("/agent/{session_id}/abort", tags=["ai"])
async def agent_abort(
    session_id: str,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),  # noqa: ARG001 — gate / consistency with sibling routes
):
    """Flip a running session to ``aborted``.

    Idempotent: aborting an already-completed/aborted session returns 200
    with the recorded final state. The in-flight generator's next loop
    iteration sees ``status == "aborted"`` and yields an ``abort`` event
    before exiting cleanly.

    Errors:

    * ``404`` — unknown session (or owned by a different user).
    """
    session = sessions.get_session(session_id, user_id=current_user.id)
    if session is None:
        raise HTTPException(status_code=404, detail=f"Unknown session_id {session_id!r}")

    if session.status not in ("completed", "aborted", "failed"):
        session.status = "aborted"
        session.touch()

    return AgentAbortResponse(
        session_id=session_id,
        partial_diff_id=session.diff_id,
    )


@router.get("/agent/{session_id}", tags=["ai"])
async def agent_state(
    session_id: str,
    current_user=Depends(get_current_active_user),
) -> AgentStateResponse:
    """Public snapshot of an in-flight or terminal session.

    Excludes the internal ``messages`` list (potentially redactable
    content) and the ``GraphSnapshot`` (large, internal). Returns 404 for
    unknown / cross-user sessions to avoid existence-leak.

    W42 cold-start hydration: when the read pulls a ``running`` session
    from disk (no in-memory mirror in the disk repo's LRU cache), the
    previous SSE generator is dead — there's no live process serving it.
    We flip the status to ``paused_user_action`` and re-persist before
    returning so the frontend renders the resume-prompt UI rather than
    "still streaming…".
    """
    session = sessions.get_session(session_id, user_id=current_user.id)
    if session is None:
        raise HTTPException(status_code=404, detail=f"Unknown session_id {session_id!r}")

    if session.status == "running" and _looks_cold_started(session):
        session.status = "paused_user_action"
        session.pause_reason = "cold_start"
        session.touch()
        try:
            sessions.register_session(session)
        except Exception:
            logger.exception(
                "agent_state: cold-start status flip persistence failed for session=%s",
                session_id,
            )

    return _to_state_response(session)


__all__ = [
    "AgentAbortResponse",
    "AgentDiscardResponse",
    "AgentFollowupRequest",
    "AgentResumeRequest",
    "AgentStartRequest",
    "AgentStateResponse",
    "router",
]
