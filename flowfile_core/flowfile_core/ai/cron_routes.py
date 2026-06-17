"""``POST /ai/generate_cron`` — natural language → cron expression.

Mounted under ``/ai``; auth + feature-flag gating come from the parent router.
Non-streaming JSON, and flow-independent (no ``flow_id``). The work happens in
:mod:`flowfile_core.ai.cron_nl`; this is the thin route surface that resolves a
BYOK provider and maps errors to HTTP status codes.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from flowfile_core.ai.byok import ProviderNotConfiguredError, get_configured_provider
from flowfile_core.ai.cron_nl import (
    DEFAULT_TIMEOUT_SECONDS,
    MAX_DESCRIPTION_LEN,
    SURFACE,
    CronGenerationResponse,
    generate_cron_expression,
)
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


class GenerateCronRequest(BaseModel):
    description: str = Field(min_length=1, max_length=MAX_DESCRIPTION_LEN)
    provider: str | None = Field(default=None, min_length=1)  # None → first configured
    model: str | None = None
    timeout: float = Field(default=DEFAULT_TIMEOUT_SECONDS, gt=0.0, le=15.0)


def _ensure_known_provider(name: str | None) -> None:
    if name is not None and not is_resolvable_provider(name):
        raise HTTPException(
            status_code=404,
            detail=f"Unknown provider {name!r}; supported: {resolvable_provider_names()}",
        )


def _resolve_provider(db: Session, user_id: int, name: str | None, *, model: str | None):
    if name is None:
        for candidate in PROVIDERS:
            try:
                return get_configured_provider(db, user_id, candidate, surface=SURFACE, model=model)
            except (ProviderNotConfiguredError, UnknownProviderError):
                continue
        raise HTTPException(
            status_code=409,
            detail=(
                "No AI provider configured. Save an API key via "
                "POST /ai/providers/{name} or set the relevant env var. "
                f"Supported: {list_supported_providers()}"
            ),
        )
    try:
        return get_configured_provider(db, user_id, name, surface=SURFACE, model=model)
    except ProviderNotConfiguredError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except UnknownProviderError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/generate_cron", response_model=CronGenerationResponse, tags=["ai"])
async def generate_cron(
    body: GenerateCronRequest,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> CronGenerationResponse:
    """404 unknown provider · 409 not configured · 422 bad body · 503 AI off.
    Soft failures (timeout, parse error, not a schedule) return 200 + degraded."""
    _ensure_known_provider(body.provider)
    provider = _resolve_provider(db, current_user.id, body.provider, model=body.model)
    return await generate_cron_expression(body.description, provider=provider, timeout=body.timeout)


__all__ = ["router"]
