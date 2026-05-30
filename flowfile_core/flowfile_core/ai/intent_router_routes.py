"""HTTP route for chat → agent auto-promotion.

Mounted under ``/ai`` from :mod:`flowfile_core.ai.routes`. Auth via
``Depends(get_current_active_user)``; the feature-flag gate covers
this through the parent ``ai_router``.

The frontend's chat ``sendMessage`` calls ``POST /ai/route`` *before*
dispatching to ``/ai/chat/stream`` whenever the manual Agent toggle
is off and auto-promote is on. The verdict in the response
(``"chat"`` or ``"agent"``) tells the frontend which backend surface
to dispatch to.

Provider resolution flows through
:func:`flowfile_core.ai.byok.get_configured_provider` so BYOK rows +
env-var fallback + surface-keyed model defaults are honoured the same
way the chat / agent surfaces do. The classifier runs at the
``intent_classifier`` surface (Haiku-class on every provider that maps
it) — see :data:`flowfile_core.ai.intent_router.SURFACE`. Dedicated
surface key (not borrowing ``settings_autocomplete``) so audit log
filtering and future model tuning treat the two as independent.

Failure modes:

* ``404`` — unknown provider name.
* ``409`` — provider not configured (no BYOK row + no env-var
  fallback + not Ollama).
* ``422`` — Pydantic validation (empty ``message``, missing
  ``provider``, etc.).
* ``503`` — ``FEATURE_FLAG_AI`` off (inherited from the parent
  router-level dependency).

Crucially, the *classifier itself* never raises out of the route —
:func:`classify_intent` collapses every internal failure to a
``kind="chat"`` fallback so the chat dispatch path always works. The
route therefore returns a 200 with ``verdict="chat"`` even when the
classifier timed out / parse-failed; the audit log records the failure
mode for post-launch tuning.
"""

from __future__ import annotations

import logging
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from flowfile_core.ai import audit
from flowfile_core.ai.byok import ProviderNotConfiguredError, get_configured_provider
from flowfile_core.ai.intent_router import (
    SURFACE,
    IntentClassification,
    RouteVerdict,
    classify_intent,
    message_preview,
    now_ms,
    verdict_for,
)
from flowfile_core.ai.providers import (
    Message,
    UnknownProviderError,
    is_resolvable_provider,
    resolvable_provider_names,
)
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.database.connection import get_db

logger = logging.getLogger(__name__)

router = APIRouter()


# Tool-name marker for the audit row. ``internal.*`` mirrors the
# planner's ``internal.staged_drop_on_resume`` convention — it's
# not a user-callable tool, just a typed audit row.
_AUDIT_TOOL_NAME = "internal.intent_classification"

# Synthetic session_id prefix for the audit row. Each classify call
# mints a fresh id (the chat surface doesn't have a long-lived
# session yet). Using ``intent_router/`` as a static prefix keeps
# audit-log queries filterable by tool_name *or* prefix.
_AUDIT_SESSION_PREFIX = "intent_router"


class ChatHistoryEntry(BaseModel):
    """One prior turn the classifier sees as conversation context.

    System messages are intentionally excluded — the classifier injects its
    own system prompt, and a stale chat-side system message would just
    compete with it.
    """

    role: Literal["user", "assistant"]
    content: str = Field(min_length=1)


class RouteRequest(BaseModel):
    """Body for ``POST /ai/route``."""

    message: str = Field(min_length=1, max_length=10_000)
    provider: str = Field(min_length=1)
    model: str | None = None
    history: list[ChatHistoryEntry] | None = None
    """Recent chat turns (oldest first), excluding the current message. The
    classifier uses this to disambiguate short follow-ups like *"can you
    implement?"* — the answer depends entirely on whether the prior assistant
    turn proposed concrete nodes / steps. Capped server-side at
    :data:`flowfile_core.ai.intent_router.DEFAULT_HISTORY_TURNS` turns."""


class RouteResponse(BaseModel):
    """Verdict for the frontend to dispatch on.

    The frontend dispatches to ``/ai/agent/start`` on
    ``verdict="agent"`` and to ``/ai/chat/stream`` on
    ``verdict="chat"``. ``kind`` / ``confidence`` are surfaced for
    telemetry and the optional banner copy; ``reason`` is the
    one-sentence rationale shown in the *"Switched to Agent mode —
    \\<reason\\>"* banner.
    """

    verdict: RouteVerdict
    kind: str
    confidence: float
    reason: str
    latency_ms: int


def _ensure_known_provider(name: str) -> None:
    if not is_resolvable_provider(name):
        raise HTTPException(
            status_code=404,
            detail=f"Unknown provider {name!r}; supported: {resolvable_provider_names()}",
        )


def _emit_audit(
    *,
    user_id: int,
    classification: IntentClassification,
    verdict: RouteVerdict,
    message: str,
    latency_ms: int,
    provider_name: str,
    model_name: str | None,
) -> None:
    """Persist a single ``auto_promotion_classified`` audit row.

    Audit emission is best-effort — the route must keep working even if
    the audit DB is briefly unavailable. Mirrors the posture in
    :mod:`flowfile_core.ai.agents.planner` (``audit.record_event``
    failures are logged-and-swallowed there too).
    """
    session_id = f"{_AUDIT_SESSION_PREFIX}/{now_ms()}"
    try:
        audit.record_event(
            audit.AuditEvent(
                session_id=session_id,
                user_id=user_id,
                tool_name=_AUDIT_TOOL_NAME,
                result_status="success",
                provider=provider_name,
                model=model_name,
                tool_args={
                    "event": "auto_promotion_classified",
                    "surface": SURFACE,
                    "message_preview": message_preview(message),
                    "kind": classification.kind,
                    "confidence": classification.confidence,
                    "reason": classification.reason,
                    "verdict": verdict,
                    "latency_ms": latency_ms,
                },
            )
        )
    except Exception as exc:  # noqa: BLE001 — audit failures must never break the route
        logger.warning("intent_router audit emit failed: %s", exc, exc_info=False)


@router.post("/route", tags=["ai"])
async def route_message(
    body: RouteRequest,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> RouteResponse:
    """Classify a chat-drawer message and return the dispatch verdict.

    The frontend hits this **before** ``/ai/chat/stream`` whenever
    the manual Agent toggle is off and auto-promote is on. On
    ``verdict="agent"`` the frontend retargets the dispatch to
    ``/ai/agent/start`` (and shows a promotion banner with
    ``reason`` + an undo affordance).
    """

    _ensure_known_provider(body.provider)

    try:
        provider = get_configured_provider(
            db,
            current_user.id,
            body.provider,
            surface="intent_classifier",
            model=body.model,
        )
    except ProviderNotConfiguredError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except UnknownProviderError as exc:
        # Defence-in-depth — _ensure_known_provider already covers the
        # PROVIDERS-side mapping, but provider_factory has its own check.
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    history_messages: list[Message] | None = None
    if body.history:
        history_messages = [Message(role=h.role, content=h.content) for h in body.history]

    started_at = now_ms()
    classification = await classify_intent(
        body.message,
        history=history_messages,
        provider=provider,
    )
    latency_ms = max(0, now_ms() - started_at)
    verdict = verdict_for(classification)

    _emit_audit(
        user_id=current_user.id,
        classification=classification,
        verdict=verdict,
        message=body.message,
        latency_ms=latency_ms,
        provider_name=body.provider,
        model_name=getattr(provider, "model", None) or body.model,
    )

    return RouteResponse(
        verdict=verdict,
        kind=classification.kind,
        confidence=classification.confidence,
        reason=classification.reason,
        latency_ms=latency_ms,
    )


__all__ = ["router", "ChatHistoryEntry", "RouteRequest", "RouteResponse"]
