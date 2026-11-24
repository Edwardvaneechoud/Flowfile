"""``POST /ai/semantic/draft`` and ``POST /ai/semantic/ask``: local-first AI over a catalog semantic model.

Mounted under ``/ai`` (``FEATURE_FLAG_AI`` from the parent router). The provider rule is these routes' own, not
``node_codegen_routes._resolve_provider`` (whose no-provider path falls through to an Ollama that is usually not
running): the provider the request names, then the local model when it is installed, else a **409** with body
``{"error_code": "AI_NOT_SET_UP", "message", "local_available"}`` that the UI turns into the one-click local offer.
A user's per-task model pick lives in the browser (``aiStore.surfaceModels``), which sends it as ``provider`` and
``model``; the backend stores none. Never 401.

Database work runs in threads on sessions closed before any model call. After resolution every outcome is a 200,
degraded ones with a ``reason``. Every draft and every answered question is written to the AI audit log with the
rows sent to the model (always 0: statistics go only when ``send_data_allowed()``, result rows never).
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse

from flowfile_core.ai.feature_flag import send_data_allowed
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.schemas.semantic_schema import (
    AiNotSetUpOut,
    SemanticAnsweredWith,
    SemanticAskIn,
    SemanticAskOut,
    SemanticDraftIn,
    SemanticDraftOut,
    SemanticDraftTable,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/semantic")

_NOT_SET_UP = (
    "No AI is set up for this. Install the local model (it runs on this computer and nothing leaves it), "
    "or add a provider in Settings → AI."
)


class _NotSetUp(Exception):
    def __init__(self, local_available: bool, message: str = _NOT_SET_UP) -> None:
        super().__init__(message)
        self.local_available = local_available
        self.message = message


def _local_status() -> tuple[bool, bool]:
    """``(installed, available)`` for the local model, read from disk only (no server is started)."""
    try:
        from flowfile_core.ai.local_model import manager

        return manager.is_installed(), manager.is_available()
    except Exception:  # noqa: BLE001
        logger.warning("local model status could not be read", exc_info=True)
        return False, False


def resolve_semantic_provider(db: Any, user_id: int, name: str | None, model: str | None) -> Any:
    """Named provider → the installed local model → ``_NotSetUp``; an unknown name is a 404."""
    from flowfile_core.ai.byok import ProviderNotConfiguredError, get_configured_provider
    from flowfile_core.ai.node_codegen_routes import _ensure_known_provider
    from flowfile_core.ai.providers import LOCAL_PROVIDER_ID

    _ensure_known_provider(name)
    installed, available = _local_status()
    if name is not None:
        if name == LOCAL_PROVIDER_ID and not installed:
            raise _NotSetUp(available, "The local model is not installed yet. " + _NOT_SET_UP)
        try:
            return get_configured_provider(db, user_id, name, surface="semantic", model=model)
        except ProviderNotConfiguredError as exc:
            raise _NotSetUp(installed or available, f"{exc} {_NOT_SET_UP}") from None
    if installed:
        return get_configured_provider(db, user_id, LOCAL_PROVIDER_ID, model=model)
    raise _NotSetUp(available)


def _not_set_up(exc: _NotSetUp) -> JSONResponse:
    body = AiNotSetUpOut(message=exc.message, local_available=exc.local_available)
    return JSONResponse(status_code=409, content=body.model_dump())


def _service(db: Any, user: Any) -> Any:
    from flowfile_core.catalog.access import AccessResolver
    from flowfile_core.catalog.semantic.service import SemanticModelService

    return SemanticModelService(db, user, AccessResolver(db, user))


def _http(exc: Exception) -> HTTPException | None:
    """A catalog or semantic error as the semantic router maps it (403/404/409/422), else ``None``."""
    from flowfile_core.routes.catalog_semantic import _EXCEPTION_MAP

    for cls, code in _EXCEPTION_MAP:
        if isinstance(exc, cls):
            return HTTPException(code, detail=str(exc))
    return None


async def _in_thread(fn, *args):
    try:
        return await asyncio.to_thread(fn, *args)
    except (_NotSetUp, HTTPException):
        raise
    except Exception as exc:
        mapped = _http(exc)
        if mapped is None:
            raise
        raise mapped from None


def _source(provider: Any) -> tuple[str, str | None]:
    model = getattr(provider, "model", None)
    return f"ai:{provider.name}/{model}" if model else f"ai:{provider.name}", model


def _audit(user_id: int, tool: str, provider: Any, status: str, args: dict[str, Any], error: str | None) -> None:
    """Best-effort audit row; a failure to record is logged, never raised into the answer."""
    try:
        from flowfile_core.ai.audit import AuditEvent, record_event

        record_event(
            AuditEvent(
                session_id=f"semantic-{uuid.uuid4().hex[:12]}",
                user_id=user_id,
                tool_name=tool,
                result_status=status,
                tool_args=args,
                error=error,
                provider=getattr(provider, "name", None),
                model=getattr(provider, "model", None),
            )
        )
    except Exception:  # noqa: BLE001
        logger.warning("semantic AI audit record failed", exc_info=True)


@router.post("/draft", response_model=SemanticDraftOut, tags=["ai"], responses={409: {"model": AiNotSetUpOut}})
async def draft_semantic_model(body: SemanticDraftIn, current_user=Depends(get_current_active_user)):
    """Rule candidates plus AI words (descriptions, metric names), stored as proposals that queue for review.

    404 unknown provider or namespace · 403 not writable · 409 ``AI_NOT_SET_UP`` · 503 AI off; the rest is a 200.
    """
    from flowfile_core.ai.semantic_draft import draft_words, words_for_store
    from flowfile_core.database.connection import get_db_context

    def prepare():
        with get_db_context() as db:
            provider = resolve_semantic_provider(db, current_user.id, body.provider, body.model)
            service = _service(db, current_user)
            sugg, model_tables, problem = service.suggest_candidates(body.namespace_id, body.table_ids)
            return provider, sugg, model_tables, problem

    try:
        provider, sugg, model_tables, problem = await _in_thread(prepare)
    except _NotSetUp as exc:
        return _not_set_up(exc)
    table_ids = body.table_ids or sorted(model_tables)
    source, model = _source(provider)
    outcomes = await draft_words(
        provider, sugg, table_ids, user_id=current_user.id, share_values=send_data_allowed() and sugg.stats_available
    )
    words = words_for_store(outcomes)

    def store():
        with get_db_context() as db:
            return _service(db, current_user).store_suggestions(body.namespace_id, sugg, words=words, ai_source=source)

    stored = await _in_thread(store)
    tables = [
        SemanticDraftTable(
            table_id=o.table_id, table=o.table, calls=o.calls, ok=o.ok, reason=o.reason, message=o.message
        )
        for o in outcomes
    ]
    failed = [t for t in tables if not t.ok]
    reason = None
    if failed:
        reason = "all_tables_failed" if len(failed) == len(tables) else "some_tables_failed"
    elif problem:
        reason = "no_statistics"
    out = SemanticDraftOut(
        **stored.model_dump(),
        provider=provider.name,
        model=model,
        table_outcomes=tables,
        degraded=reason is not None,
        reason=reason,
    )
    out.tables = len(table_ids)
    out.stats_available = sugg.stats_available and problem is None
    out.stats_message = problem
    _audit(
        current_user.id,
        "semantic_draft",
        provider,
        "success" if not failed else "error",
        {
            "namespace_id": body.namespace_id,
            "tables": [o.table for o in outcomes],
            "calls": sum(o.calls for o in outcomes),
            "added": stored.added.model_dump(),
            "statistics_sent": send_data_allowed() and sugg.stats_available,
            "rows_sent_to_model": 0,
        },
        reason,
    )
    return out


def _menu(service: Any, namespace_id: int, question: str) -> list[Any]:
    """The accepted metrics as menu items, narrowed to ``MAX_MENU`` by glosslayer's keyword search."""
    from flowfile_core.ai.semantic_ask import MAX_MENU, MenuItem

    snap = service.snapshot(namespace_id, accepted_only=True)
    metrics = snap.metrics
    if len(metrics) > MAX_MENU:
        from glosslayer.ids import SubjectId

        hits = service.view.search(question, kinds=["metric"], limit=MAX_MENU, for_ai=True)
        wanted = []
        for h in hits:
            try:
                wanted.append(SubjectId.parse(h["subject"]).name)
            except (KeyError, ValueError):
                continue
        picked = [m for m in metrics if m.name in wanted]
        metrics = (picked or metrics)[:MAX_MENU]
    items = []
    for m in metrics:
        table = snap.table(m.table)
        columns = [c.name for c in table.columns] if table else []
        items.append(
            MenuItem(
                name=m.name,
                meaning=m.value("description"),
                time_column=m.value("time_column"),
                dimensions=[d for d in (m.value("dimensions") or []) if isinstance(d, str)],
                filter_fields=columns,
            )
        )
    return items


@router.post("/ask", response_model=SemanticAskOut, tags=["ai"], responses={409: {"model": AiNotSetUpOut}})
async def ask_semantic_model(body: SemanticAskIn, current_user=Depends(get_current_active_user)):
    """A question answered with one accepted metric, run under the caller's rights; rows never reach the model.

    404 unknown provider or namespace · 403 not readable · 409 ``AI_NOT_SET_UP`` · 503 AI off; the rest is a 200.
    """
    from flowfile_core.ai.semantic_ask import ask
    from flowfile_core.database.connection import get_db_context

    def prepare():
        with get_db_context() as db:
            provider = resolve_semantic_provider(db, current_user.id, body.provider, body.model)
            return provider, _menu(_service(db, current_user), body.namespace_id, body.question)

    try:
        provider, menu = await _in_thread(prepare)
    except _NotSetUp as exc:
        return _not_set_up(exc)
    _source_str, model = _source(provider)
    out = SemanticAskOut(question=body.question, provider=provider.name, model=model)
    if not menu:
        out.no_metric_fits, out.degraded, out.reason = True, True, "no_accepted_metrics"
        out.notes.append("This model has no approved metrics yet. Approve one in Review, then ask again.")
        return out

    def run_query(query):
        from flowfile_core.catalog.exceptions import SemanticValueError

        with get_db_context() as db:
            try:
                return _service(db, current_user).query_metric(body.namespace_id, query)
            except SemanticValueError as exc:
                raise ValueError(str(exc)) from None

    async def run(query):
        return await asyncio.to_thread(run_query, query)

    answer, attempts, result = await ask(provider, menu, body.question, user_id=current_user.id, run=run)
    out.attempts = attempts
    out.notes += answer.notes
    out.query = answer.query
    if answer.no_metric:
        out.no_metric_fits = True
        out.notes.append(answer.why or "No approved metric answers this question.")
    elif result is None:
        out.degraded, out.reason = True, answer.reason or "invalid_query"
        out.notes += answer.problems
    else:
        out.result = result
        out.answered_with = SemanticAnsweredWith(**result.metric.model_dump())
        if result.error:
            out.degraded, out.reason = True, "query_failed"
    _audit(
        current_user.id,
        "semantic_ask",
        provider,
        "success" if out.result is not None and not out.degraded else ("rejected" if out.no_metric_fits else "error"),
        {
            "namespace_id": body.namespace_id,
            "question_chars": len(body.question),
            "metric": out.answered_with.name if out.answered_with else None,
            "metric_version": out.answered_with.version if out.answered_with else None,
            "sql": result.sql if result is not None else None,
            "rows_returned": len(result.rows) if result is not None else 0,
            "rows_sent_to_model": 0,
            "attempts": attempts,
        },
        out.reason,
    )
    return out


__all__ = ["router", "resolve_semantic_provider"]
