"""The AI dashboard builder's two routes, around the existing ``POST /ai/generate_chart``.

``POST /ai/plan_dashboard`` turns a question over several catalog tables into a plan (a title,
KPIs, and the charts to build with what each tests). The browser has each chart built through
``/ai/generate_chart``, then ``POST /ai/dashboard_story`` reads what the built charts return
and writes the page from it.

Mounted under ``/ai`` (feature flag from the parent router). Every selected table is read
through a ``CatalogService`` built with the caller's ``AccessResolver`` before any model call,
so one the caller may not read refuses the request (403/404, as the catalog refuses it).
Provider resolution is ``node_codegen_routes``'s, with its 404 and 409. Everything after that
is a 200: a failure comes back ``degraded`` with a stable ``reason``. Never 401.

Data values reach the model only when the request's ``send_values`` and the server's
``send_data_allowed`` both allow it; otherwise the story route runs no evidence query at all.
"""

from __future__ import annotations

import asyncio
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from flowfile_core.ai.chart_profile import TableProfile, profile_table, render_profile
from flowfile_core.ai.chart_request import MAX_PROMPT_LEN
from flowfile_core.ai.chart_routes import _readable_table, answered_by, profile_warnings
from flowfile_core.ai.dashboard_plan import (
    DEFAULT_TIMEOUT_SECONDS,
    MAX_CHECKS,
    MAX_KPIS,
    MAX_SOURCES,
    MAX_TIMEOUT_SECONDS,
    DashboardPlanResponse,
    PlannedKpi,
    PlanSource,
    plan_dashboard,
    source_keys,
)
from flowfile_core.ai.dashboard_story import (
    DashboardStoryResponse,
    StoryChart,
    StoryFailure,
    gather_evidence,
    keep_private,
    withheld_evidence,
    write_story,
)
from flowfile_core.ai.feature_flag import send_data_allowed
from flowfile_core.ai.node_codegen_routes import _ensure_known_provider, _resolve_provider
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.database.connection import get_db

router = APIRouter()


class PlanDashboardRequest(BaseModel):
    """Body for ``POST /ai/plan_dashboard``. ``send_values`` is the user's choice; the server can still withhold."""

    table_ids: list[int] = Field(min_length=1, max_length=MAX_SOURCES)
    prompt: str = Field(min_length=1, max_length=MAX_PROMPT_LEN)
    send_values: bool = True
    provider: str | None = Field(default=None, min_length=1)
    model: str | None = None
    timeout: float = Field(default=DEFAULT_TIMEOUT_SECONDS, gt=0.0, le=MAX_TIMEOUT_SECONDS)


def _readable_tables(db: Session, user: Any, table_ids: list[int]) -> list[Any]:
    """Each distinct table as the catalog shows it to ``user``, in request order; 404 or 403 on the first refused."""
    return [_readable_table(db, user, table_id) for table_id in dict.fromkeys(table_ids)]


async def _profiles(tables: list[Any], user: Any, share_values: bool) -> tuple[list[TableProfile], list[str]]:
    profiles = await asyncio.gather(*(profile_table(t, user, share_values=share_values) for t in tables))
    warnings = [f'"{p.table_name}": ' + w for p in profiles for w in profile_warnings(p)]
    return list(profiles), warnings


def plan_sources(profiles: list[TableProfile]) -> list[PlanSource]:
    keys = source_keys([p.table_name for p in profiles])
    return [
        PlanSource(key=key, table_id=p.table_id, columns=tuple(p.column_infos()), profile_text=render_profile(p))
        for key, p in zip(keys, profiles, strict=True)
    ]


@router.post("/plan_dashboard", response_model=DashboardPlanResponse, tags=["ai"])
async def plan_dashboard_route(
    body: PlanDashboardRequest,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> DashboardPlanResponse:
    """404 unknown provider or table · 403 table not readable · 409 no provider · 503 AI off.

    Soft failures (timeout, provider error, unusable answer) return 200 + ``degraded``.
    """
    _ensure_known_provider(body.provider)
    tables = await asyncio.to_thread(_readable_tables, db, current_user, body.table_ids)
    provider = _resolve_provider(db, current_user.id, body.provider, model=body.model)

    share_values = body.send_values and send_data_allowed()
    profiles, warnings = await _profiles(tables, current_user, share_values)
    result = await plan_dashboard(
        prompt=body.prompt,
        sources=plan_sources(profiles),
        provider=provider,
        user_id=current_user.id,
        warnings=warnings,
        timeout=body.timeout,
    )
    return answered_by(result, provider)


class DashboardStoryRequest(BaseModel):
    """Body for ``POST /ai/dashboard_story``: the question, the plan's KPIs and the charts that were built.

    Each chart carries the query its tile runs (``payload``); with ``send_values`` off the browser
    sends none and core reads nothing.
    """

    table_ids: list[int] = Field(min_length=1, max_length=MAX_SOURCES)
    prompt: str = Field(min_length=1, max_length=MAX_PROMPT_LEN)
    title: str | None = Field(default=None, max_length=120)
    kpis: list[PlannedKpi] = Field(default_factory=list, max_length=MAX_KPIS)
    charts: list[StoryChart] = Field(default_factory=list, max_length=MAX_CHECKS)
    failed: list[StoryFailure] = Field(default_factory=list, max_length=MAX_CHECKS)
    send_values: bool = True
    provider: str | None = Field(default=None, min_length=1)
    model: str | None = None
    timeout: float = Field(default=DEFAULT_TIMEOUT_SECONDS, gt=0.0, le=MAX_TIMEOUT_SECONDS)


def _require_consistent(body: DashboardStoryRequest) -> None:
    """422 when an item names a table outside ``table_ids`` or two charts share an id."""
    selected = set(body.table_ids)
    strays = sorted({item.table_id for item in (*body.kpis, *body.charts)} - selected)
    if strays:
        raise HTTPException(status_code=422, detail=f"Items name tables that are not selected: {strays}")
    for items, what in ((body.charts, "charts"), (body.kpis, "kpis")):
        ids = [item.id for item in items]
        if len(set(ids)) != len(ids):
            raise HTTPException(status_code=422, detail=f"Two {what} share an id.")


@router.post("/dashboard_story", response_model=DashboardStoryResponse, tags=["ai"])
async def dashboard_story_route(
    body: DashboardStoryRequest,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> DashboardStoryResponse:
    """404 unknown provider or table · 403 table not readable · 409 no provider · 422 · 503 AI off.

    Soft failures (timeout, provider error, unusable answer) return 200 + ``degraded``.
    """
    _ensure_known_provider(body.provider)
    _require_consistent(body)
    tables = await asyncio.to_thread(_readable_tables, db, current_user, body.table_ids)
    provider = _resolve_provider(db, current_user.id, body.provider, model=body.model)

    by_id = {t.id: t for t in tables}
    keys = dict(zip(by_id, source_keys([t.name for t in tables]), strict=True))
    withheld = await asyncio.to_thread(_read_withheld, db, list(by_id))
    measuring = body.send_values and send_data_allowed() and bool(body.charts)
    private = keep_private(tables=by_id, withheld=withheld, kpis=body.kpis, charts=body.charts, measuring=measuring)
    if measuring:
        evidence = await gather_evidence(tables=by_id, user=current_user, kpis=private.kpis, charts=private.charts)
        evidence = [*evidence, *private.evidence]
    else:
        evidence = withheld_evidence(body.kpis, body.charts)
    result = await write_story(
        prompt=body.prompt,
        title=body.title,
        kpis=body.kpis,
        charts=private.shown_charts,
        failed=body.failed,
        sources=keys,
        evidence=evidence,
        provider=provider,
        user_id=current_user.id,
        warnings=private.warnings,
        timeout=body.timeout,
    )
    return answered_by(result, provider)


def _read_withheld(db: Session, table_ids: list[int]) -> Any:
    from flowfile_core.catalog.semantic.confidentiality import read_withheld

    return read_withheld(db, table_ids)


__all__ = ["router", "DashboardStoryRequest", "PlanDashboardRequest"]
