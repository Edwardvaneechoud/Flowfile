"""HTTP route for edge ghost-node suggestions.

Mounted under ``/ai`` from :mod:`flowfile_core.ai.routes`. Auth via
``Depends(get_current_active_user)``; the feature-flag gate covers
this through the parent ``ai_router``.

Single endpoint:

* ``POST /ai/suggest_next_node`` — JSON-in, JSON-out
  (non-streaming). The ghost-node UX wants a fast synchronous
  result; SSE handshake overhead isn't worth it for a sub-4s call.

The provider call lives in :mod:`flowfile_core.ai.suggest_next_node`;
this module is the thin route surface that resolves the flow + BYOK
provider and maps errors onto HTTP status codes.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from flowfile_core import flow_file_handler
from flowfile_core.ai.byok import ProviderNotConfiguredError, get_configured_provider
from flowfile_core.ai.providers import (
    PROVIDERS,
    UnknownProviderError,
    list_supported_providers,
)
from flowfile_core.ai.suggest_next_node import (
    DEFAULT_TIMEOUT_SECONDS,
    MAX_SUGGESTIONS,
    SURFACE,
    NextNodeSuggestionsResponse,
    suggest_next_node,
)
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.database.connection import get_db

router = APIRouter()


_DEFAULT_PROVIDER = "anthropic"
"""Ghost-node defaults to the same provider AnthropicProvider routes
``ghost_node`` to (Haiku 4.5) — fastest TTFB on tool-aware traffic."""


class SuggestNextNodeRequest(BaseModel):
    """Body for ``POST /ai/suggest_next_node``."""

    flow_id: int = Field(ge=0)
    upstream_node_id: int | str
    """The node whose outgoing edge stub is being hovered."""
    provider: str = Field(default=_DEFAULT_PROVIDER, min_length=1)
    model: str | None = None
    intent: str | None = Field(default=None, max_length=500)
    """Optional caller-supplied hint (e.g. ``"filter to last 30 days"``).
    The LLM uses this to bias which suggestions it returns."""
    max_suggestions: int = Field(default=MAX_SUGGESTIONS, ge=1, le=10)
    timeout: float = Field(default=DEFAULT_TIMEOUT_SECONDS, gt=0.0, le=10.0)


def _ensure_known_provider(name: str) -> None:
    if name not in PROVIDERS:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown provider {name!r}; supported: {list_supported_providers()}",
        )


def _resolve_flow(flow_id: int):
    flow = flow_file_handler.get_flow(flow_id)
    if flow is None:
        raise HTTPException(status_code=404, detail=f"Flow {flow_id} not found")
    return flow


def _resolve_provider(
    db: Session,
    user_id: int,
    name: str,
    *,
    model: str | None,
):
    try:
        return get_configured_provider(
            db,
            user_id,
            name,
            surface=SURFACE,
            model=model,
        )
    except ProviderNotConfiguredError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except UnknownProviderError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post(
    "/suggest_next_node",
    response_model=NextNodeSuggestionsResponse,
    tags=["ai"],
)
async def suggest_next_node_route(
    body: SuggestNextNodeRequest,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> NextNodeSuggestionsResponse:
    """Return top-N schema-grounded next-node suggestions for an edge stub.

    Errors:

    * ``404`` — flow not found or provider name unknown.
    * ``409`` — provider not configured (no BYOK row, no env-var fallback).
    * ``422`` — Pydantic validation (negative ``flow_id``, oversized
      ``intent``, ``max_suggestions`` out of range, etc.).
    * ``503`` — ``FEATURE_FLAG_AI`` is off (inherited from the parent router).

    Soft-failures (LLM timeout, parse error, hallucinated columns, cold
    upstream, all candidates filtered) become a ``200`` with
    ``degraded=true`` and a stable ``reason`` string — the frontend silently
    hides the popover when ``degraded`` is set.
    """
    _ensure_known_provider(body.provider)
    flow = _resolve_flow(body.flow_id)
    provider = _resolve_provider(db, current_user.id, body.provider, model=body.model)
    return await suggest_next_node(
        flow,
        body.upstream_node_id,
        provider=provider,
        intent=body.intent,
        max_suggestions=body.max_suggestions,
        timeout=body.timeout,
    )


__all__ = ["router"]
