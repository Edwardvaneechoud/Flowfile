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
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.orm import Session

from flowfile_core import flow_file_handler
from flowfile_core.ai import sessions
from flowfile_core.ai.agents.planner import (
    DEFAULT_MAX_RETRIES_PER_STEP,
    DEFAULT_MAX_STEPS,
    DEFAULT_MAX_TOKENS,
    run_planner_session,
)
from flowfile_core.ai.byok import ProviderNotConfiguredError, get_configured_provider
from flowfile_core.ai.providers.registry import PROVIDERS, UnknownProviderError
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
    surface: Literal["agent", "agent_complex"] = "agent"
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


class AgentResumeRequest(BaseModel):
    action: Literal["continue", "discard"]


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
        created_at=session.created_at.isoformat(),
        updated_at=session.updated_at.isoformat(),
    )


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
    }
    if body.session_id is not None:
        session_kwargs["session_id"] = body.session_id
    session = sessions.AgentSession(**session_kwargs)
    sessions.register_session(session)

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
    )
    return make_streaming_response(sse)


@router.post("/agent/{session_id}/resume", tags=["ai"])
async def agent_resume(
    session_id: str,
    body: AgentResumeRequest,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Resume a paused-on-drift session.

    * ``action="continue"`` re-snapshots the graph and streams the rest of
      the planner's events as SSE (mirrors ``/start``).
    * ``action="discard"`` pops the session, returns JSON. Any staged diff
      that was registered before the pause stays — the user can still
      reject it via the W41 ``/ai/diff/{id}/reject`` route.

    Errors:

    * ``404`` — unknown session (or owned by a different user).
    * ``409`` — session not in ``paused_drift`` state.
    """
    session = sessions.get_session(session_id, user_id=current_user.id)
    if session is None:
        raise HTTPException(status_code=404, detail=f"Unknown session_id {session_id!r}")
    if session.status != "paused_drift":
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

    events = run_planner_session(session=session, flow=flow, provider=provider)
    sse = planner_events_sse(
        events,
        session_id=session.session_id,
        step_count_getter=lambda: session.step_count,
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
    """
    session = sessions.get_session(session_id, user_id=current_user.id)
    if session is None:
        raise HTTPException(status_code=404, detail=f"Unknown session_id {session_id!r}")
    return _to_state_response(session)


__all__ = [
    "AgentAbortResponse",
    "AgentDiscardResponse",
    "AgentResumeRequest",
    "AgentStartRequest",
    "AgentStateResponse",
    "router",
]
