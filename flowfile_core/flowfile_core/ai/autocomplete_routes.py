"""HTTP routes for settings autocomplete (W34).

Mounted under ``/ai`` from :mod:`flowfile_core.ai.routes`. Auth via
``Depends(get_current_active_user)``; W17's feature-flag gate covers these
through the parent ``ai_router``.

Two endpoints:

* ``POST /ai/autocomplete/formula`` — schema-aware completions for a
  ``formula`` settings expression.
* ``POST /ai/autocomplete/join_keys`` — proposed ``(left_col, right_col)``
  pairs for a ``join`` settings panel.

Both routes are non-streaming JSON — the autocomplete UX wants a fast
synchronous result, not a stream. SSE is used by the chat (W20) and agent
(W40) surfaces; autocomplete fires on keystrokes and a per-keystroke SSE
handshake would burn the latency budget.

The provider call paths are pure functions in
:mod:`flowfile_core.ai.autocomplete`; this module is the thin route surface
that resolves the flow + BYOK provider and maps errors onto HTTP status codes.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from flowfile_core import flow_file_handler
from flowfile_core.ai.autocomplete import (
    DEFAULT_TIMEOUT_SECONDS,
    MAX_FORMULA_SUGGESTIONS,
    MAX_JOIN_KEY_PAIRS,
    SURFACE,
    FormulaSuggestionsResponse,
    JoinKeySuggestionsResponse,
    suggest_formula_completions,
    suggest_join_keys,
)
from flowfile_core.ai.byok import ProviderNotConfiguredError, get_configured_provider
from flowfile_core.ai.providers import (
    PROVIDERS,
    UnknownProviderError,
    list_supported_providers,
)
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.database.connection import get_db

router = APIRouter()


_DEFAULT_PROVIDER = "google"


class FormulaAutocompleteRequest(BaseModel):
    """Body for ``POST /ai/autocomplete/formula``."""

    flow_id: int = Field(ge=0)
    node_id: int | str
    partial_text: str = Field(default="", max_length=2_000)
    intent: str | None = Field(default=None, max_length=500)
    provider: str = Field(default=_DEFAULT_PROVIDER, min_length=1)
    model: str | None = None
    max_suggestions: int = Field(
        default=MAX_FORMULA_SUGGESTIONS,
        ge=1,
        le=20,
    )
    timeout: float = Field(
        default=DEFAULT_TIMEOUT_SECONDS,
        gt=0.0,
        le=10.0,
    )


class JoinKeyAutocompleteRequest(BaseModel):
    """Body for ``POST /ai/autocomplete/join_keys``."""

    flow_id: int = Field(ge=0)
    left_node_id: int | str
    right_node_id: int | str
    how: str = Field(default="inner", min_length=1, max_length=32)
    provider: str = Field(default=_DEFAULT_PROVIDER, min_length=1)
    model: str | None = None
    max_pairs: int = Field(
        default=MAX_JOIN_KEY_PAIRS,
        ge=1,
        le=20,
    )
    timeout: float = Field(
        default=DEFAULT_TIMEOUT_SECONDS,
        gt=0.0,
        le=10.0,
    )


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
    "/autocomplete/formula",
    response_model=FormulaSuggestionsResponse,
    tags=["ai"],
)
async def autocomplete_formula(
    body: FormulaAutocompleteRequest,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> FormulaSuggestionsResponse:
    """Return schema-aware formula completions.

    Errors:

    * ``404`` — flow or provider unknown.
    * ``409`` — provider not configured (no BYOK row, no env-var fallback).
    * ``422`` — Pydantic validation (negative ``flow_id``, oversized
      ``partial_text``, etc.).
    * ``503`` — ``FEATURE_FLAG_AI`` is off (inherited from the parent router).

    Soft-failures (LLM timeout, parse error, hallucinated columns,
    unknown upstream schema) become a ``200`` with ``degraded=true`` and a
    stable ``reason`` string — the frontend silently falls back to static
    completions.
    """
    _ensure_known_provider(body.provider)
    flow = _resolve_flow(body.flow_id)
    provider = _resolve_provider(db, current_user.id, body.provider, model=body.model)
    return await suggest_formula_completions(
        flow,
        body.node_id,
        body.partial_text,
        body.intent,
        provider=provider,
        max_suggestions=body.max_suggestions,
        timeout=body.timeout,
    )


@router.post(
    "/autocomplete/join_keys",
    response_model=JoinKeySuggestionsResponse,
    tags=["ai"],
)
async def autocomplete_join_keys(
    body: JoinKeyAutocompleteRequest,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> JoinKeySuggestionsResponse:
    """Return proposed ``(left_col, right_col)`` join-key pairs.

    Error mapping mirrors :func:`autocomplete_formula`. Pairs that cite
    columns missing from either upstream are filtered server-side; the
    response is always grounded against the actual upstream schemas.
    """
    _ensure_known_provider(body.provider)
    flow = _resolve_flow(body.flow_id)
    provider = _resolve_provider(db, current_user.id, body.provider, model=body.model)
    return await suggest_join_keys(
        flow,
        body.left_node_id,
        body.right_node_id,
        body.how,
        provider=provider,
        max_pairs=body.max_pairs,
        timeout=body.timeout,
    )


__all__ = ["router"]
