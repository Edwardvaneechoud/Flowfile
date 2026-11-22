"""``POST /ai/generate_chart``: natural language → a chart request for a catalog table.

Mounted under ``/ai`` (feature flag from the parent router). The table is read through a
``CatalogService`` built with the caller's ``AccessResolver``, so a table the caller may
not read is refused (403/404, as the catalog refuses it) before any model call. Provider
resolution is ``node_codegen_routes``'s, with its 404 and 409. Both read the database in one
thread, on a session closed before the model call, so the call holds no connection and the
event loop does no database work. Everything after that is a 200: a provider failure, a
timeout or an answer that still fails after the retry comes back ``degraded`` with a stable
``reason`` (``chart_request.REASONS``). Never 401.
"""

from __future__ import annotations

import asyncio
from typing import Any, TypeVar

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from flowfile_core.ai.chart_profile import profile_table, render_profile
from flowfile_core.ai.chart_request import (
    DEFAULT_TIMEOUT_SECONDS,
    MAX_PROMPT_LEN,
    MAX_TIMEOUT_SECONDS,
    SURFACE,
    ChartGenerationResponse,
    generate_chart,
)
from flowfile_core.ai.feature_flag import send_data_allowed
from flowfile_core.ai.node_codegen_routes import _ensure_known_provider, _resolve_provider
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.database.connection import get_db_context

router = APIRouter()

AnswerT = TypeVar("AnswerT", bound=BaseModel)


class GenerateChartRequest(BaseModel):
    """Body for ``POST /ai/generate_chart``.

    ``current_chart`` is the chart on screen as a chart request; it is checked like a model
    answer, and one that fails is dropped with a warning rather than refused. ``send_values``
    is the user's choice; the server's ``send_data_allowed`` can still withhold them.
    """

    table_id: int
    prompt: str = Field(min_length=1, max_length=MAX_PROMPT_LEN)
    current_chart: dict[str, Any] | None = None
    send_values: bool = True
    provider: str | None = Field(default=None, min_length=1)
    model: str | None = None
    timeout: float = Field(default=DEFAULT_TIMEOUT_SECONDS, gt=0.0, le=MAX_TIMEOUT_SECONDS)


def _readable_table(db: Session, user: Any, table_id: int):
    """The table as the catalog shows it to ``user``; 404 or 403 the way the catalog answers."""
    from flowfile_core.catalog import (
        CatalogService,
        NotAuthorizedError,
        SQLAlchemyCatalogRepository,
        TableNotFoundError,
    )
    from flowfile_core.catalog.access import AccessResolver

    service = CatalogService(SQLAlchemyCatalogRepository(db), access=AccessResolver(db, user))
    try:
        return service.get_table(table_id, getattr(user, "id", None))
    except TableNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Catalog table not found") from exc
    except NotAuthorizedError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc


def _table_and_provider(user: Any, body: GenerateChartRequest):
    """The readable table and the provider, read on one session that is closed before the model call."""
    with get_db_context() as db:
        table = _readable_table(db, user, body.table_id)
        provider = _resolve_provider(db, user.id, body.provider, model=body.model, surface=SURFACE)
    return table, provider


def answered_by(result: AnswerT, provider: Any) -> AnswerT:
    """The result stamped with the provider and model that answered it, for the builder's log."""
    return result.model_copy(update={"provider": provider.name, "model": getattr(provider, "model", None)})


def _profile_warning(problem: str) -> str:
    return (
        f"Column statistics and example values were not available ({problem}); "
        "the model saw the column names, types and roles only."
    )


@router.post("/generate_chart", response_model=ChartGenerationResponse, tags=["ai"])
async def generate_chart_route(
    body: GenerateChartRequest,
    current_user=Depends(get_current_active_user),
) -> ChartGenerationResponse:
    """404 unknown provider or table · 403 table not readable · 409 no provider · 503 AI off.

    Soft failures (timeout, provider error, unusable answer) return 200 + ``degraded``.
    """
    _ensure_known_provider(body.provider)
    table, provider = await asyncio.to_thread(_table_and_provider, current_user, body)

    share_values = body.send_values and send_data_allowed()
    profile = await profile_table(table, current_user, share_values=share_values)
    warnings = [] if profile.complete else [_profile_warning(profile.problem or "unknown error")]
    result = await generate_chart(
        prompt=body.prompt,
        columns=profile.column_infos(),
        profile_text=render_profile(profile),
        provider=provider,
        user_id=current_user.id,
        current_chart=body.current_chart,
        warnings=warnings,
        timeout=body.timeout,
    )
    return answered_by(result, provider)


__all__ = ["router", "GenerateChartRequest", "answered_by"]
