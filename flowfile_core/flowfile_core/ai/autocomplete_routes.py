"""HTTP routes for settings autocomplete.

Mounted under ``/ai`` from :mod:`flowfile_core.ai.routes`. Auth via
``Depends(get_current_active_user)``; the feature-flag gate covers
these through the parent ``ai_router``.

Two endpoints:

* ``POST /ai/autocomplete/formula`` — schema-aware completions for a
  ``formula`` settings expression.
* ``POST /ai/autocomplete/join_keys`` — proposed
  ``(left_col, right_col)`` pairs for a ``join`` settings panel.

Both routes are non-streaming JSON — the autocomplete UX wants a
fast synchronous result, not a stream. SSE is used by the chat and
agent surfaces; autocomplete fires on keystrokes and a per-keystroke
SSE handshake would burn the latency budget.

The provider call paths are pure functions in
:mod:`flowfile_core.ai.autocomplete`; this module is the thin route
surface that resolves the flow + BYOK provider and maps errors onto
HTTP status codes.
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
    is_resolvable_provider,
    list_supported_providers,
    resolvable_provider_names,
)
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.database.connection import get_db

router = APIRouter()


class FormulaAutocompleteRequest(BaseModel):
    """Body for ``POST /ai/autocomplete/formula``."""

    flow_id: int = Field(ge=0)
    node_id: int | str
    partial_text: str = Field(default="", max_length=2_000)
    intent: str | None = Field(default=None, max_length=500)
    provider: str | None = Field(default=None, min_length=1)
    """When omitted, :func:`_resolve_provider` walks ``PROVIDERS`` in
    registration order and picks the first one configured for the
    user. The historical hardcoded default ``"google"`` was bitrot —
    the typical user runs on Anthropic / OpenAI / OpenRouter / Groq,
    so requests that didn't override ``provider`` 409'd."""
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
    provider: str | None = Field(default=None, min_length=1)
    """See :class:`FormulaAutocompleteRequest.provider`. Falls back
    to the user's first configured provider in
    :func:`_resolve_provider` when omitted."""
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


def _ensure_known_provider(name: str | None) -> None:
    # ``name=None`` signals "no explicit pin" — the walk-and-pick
    # fallback in :func:`_resolve_provider` handles selection.
    if name is None:
        return
    if not is_resolvable_provider(name):
        raise HTTPException(
            status_code=404,
            detail=f"Unknown provider {name!r}; supported: {resolvable_provider_names()}",
        )


def _resolve_flow(flow_id: int):
    flow = flow_file_handler.get_flow(flow_id)
    if flow is None:
        raise HTTPException(status_code=404, detail=f"Flow {flow_id} not found")
    return flow


def _resolve_provider(
    db: Session,
    user_id: int,
    name: str | None,
    *,
    model: str | None,
):
    # Walk-and-pick fallback when the caller doesn't pin a provider —
    # try each provider in ``PROVIDERS`` registration order; return
    # the first one that resolves without raising
    # ``ProviderNotConfiguredError``. Replaces the historical
    # hardcoded ``"google"`` default that 409'd for users running on
    # any other configured provider.
    if name is None:
        for candidate in PROVIDERS:
            try:
                return get_configured_provider(
                    db,
                    user_id,
                    candidate,
                    surface=SURFACE,
                    model=model,
                )
            except (ProviderNotConfiguredError, UnknownProviderError):
                continue
        raise HTTPException(
            status_code=409,
            detail=(
                "No AI provider configured. Save an API key for any "
                "supported provider via POST /ai/providers/{name} or "
                "set the appropriate env var. Supported: "
                f"{list_supported_providers()}"
            ),
        )
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
