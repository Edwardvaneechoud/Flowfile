"""``POST /ai/semantic/draft``, ``/ask`` and ``/suggest``: local-first AI over a catalog semantic model.

Mounted under ``/ai`` (``FEATURE_FLAG_AI`` from the parent router). The provider rule is these routes' own, not
``node_codegen_routes._resolve_provider``: the provider the request names, then the local model when it is installed,
else a **409** ``AI_NOT_SET_UP`` body that the UI turns into the one-click local offer. Never 401.

Database work runs in threads on sessions closed before any model call. After resolution every outcome is a 200,
degraded ones with a ``reason``. Every call is written to the AI audit log with the rows sent to the model (always 0:
statistics go only when ``send_data_allowed()``, result rows never).
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections import Counter
from collections.abc import Callable, Coroutine
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse

from flowfile_core.ai.feature_flag import send_data_allowed
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.schemas import semantic_schema as s

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/semantic")

_NOT_SET_UP = (
    "No AI is set up for this. Install the local model (it runs on this computer and nothing leaves it), "
    "or add a provider in Settings → AI."
)
_CONFLICTS = {409: {"model": s.AiNotSetUpOut, "description": "AI_NOT_SET_UP, or SUGGEST_RUNNING (SuggestRunningOut)"}}


class _NotSetUp(Exception):
    def __init__(self, local_available: bool, why: str = "") -> None:
        self.body = s.AiNotSetUpOut(message=why + _NOT_SET_UP, local_available=local_available)


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
    if name is None:
        if not installed:
            raise _NotSetUp(available)
        return get_configured_provider(db, user_id, LOCAL_PROVIDER_ID, model=model)
    if name == LOCAL_PROVIDER_ID and not installed:
        raise _NotSetUp(available, "The local model is not installed yet. ")
    try:
        return get_configured_provider(db, user_id, name, surface="semantic", model=model)
    except ProviderNotConfiguredError as exc:
        raise _NotSetUp(installed or available, f"{exc} ") from None


async def _with_service(user: Any, work: Callable[[Any], Any]) -> Any:
    """``work(service)`` in a thread, on a session of its own; its errors answer as the semantic router's do."""
    from flowfile_core.catalog.access import AccessResolver
    from flowfile_core.catalog.semantic.service import SemanticModelService
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.routes.catalog import handle_catalog_exceptions

    @handle_catalog_exceptions()
    def run() -> Any:
        with get_db_context() as db:
            return work(SemanticModelService(db, user, AccessResolver(db, user)))

    return await asyncio.to_thread(run)


async def _or_conflict(work: Coroutine[Any, Any, Any]) -> Any:
    """The route's answer, or the 409 bodies of no AI set up and of a suggestion already running."""
    from flowfile_core.catalog.semantic.activity import ActivityRunning
    from flowfile_core.routes.catalog_semantic import suggest_running

    try:
        return await work
    except _NotSetUp as exc:
        return JSONResponse(status_code=409, content=exc.body.model_dump())
    except ActivityRunning as exc:
        return suggest_running(exc)


def _source(provider: Any, model: str | None) -> str:
    return f"ai:{provider.name}/{model}" if model else f"ai:{provider.name}"


def _model_label(provider: Any) -> str | None:
    """The local provider's label is a fixed default; the local manager knows which model actually runs."""
    from flowfile_core.ai.providers import LOCAL_PROVIDER_ID

    model = getattr(provider, "model", None)
    if getattr(provider, "name", None) != LOCAL_PROVIDER_ID:
        return model
    try:
        from flowfile_core.ai.local_model import manager

        return manager.get_selected_model_id() or model
    except Exception:  # noqa: BLE001
        return model


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
                tool_args=args | {"rows_sent_to_model": 0},
                error=error,
                provider=getattr(provider, "name", None),
                model=getattr(provider, "model", None),
            )
        )
    except Exception:  # noqa: BLE001
        logger.warning("semantic AI audit record failed", exc_info=True)


@router.post("/draft", response_model=s.SemanticDraftOut, tags=["ai"], responses=_CONFLICTS)
async def draft_semantic_model(body: s.SemanticDraftIn, current_user=Depends(get_current_active_user)):
    """Rule candidates plus AI words (descriptions, metric names), stored as proposals that queue for review.

    404 unknown provider or namespace · 403 not writable · 409 ``AI_NOT_SET_UP`` or ``SUGGEST_RUNNING`` · 503 AI off;
    the rest is a 200. The run is recorded in ``catalog/semantic/activity.py`` with its table-by-table step.
    """
    return await _or_conflict(_draft(body, current_user))


async def _draft(body: s.SemanticDraftIn, user: Any) -> s.SemanticDraftOut:
    from flowfile_core.ai.semantic_draft import draft_words, words_for_store
    from flowfile_core.catalog.semantic import activity
    from flowfile_core.catalog.semantic.confidentiality import read_withheld

    def resolve(service: Any) -> Any:
        provider = resolve_semantic_provider(service.db, user.id, body.provider, body.model)
        service.namespace(body.namespace_id, writable=True)
        return provider

    def prepare(service: Any) -> Any:
        sugg, model_tables, problem = service.suggest_candidates(body.namespace_id, body.table_ids)
        table_ids = body.table_ids or sorted(model_tables)
        return sugg, problem, table_ids, read_withheld(service.db, table_ids)

    provider = await _with_service(user, resolve)
    model = getattr(provider, "model", None)
    with activity.tracking(body.namespace_id, "draft", "Suggesting a model…", user.id) as run:
        run.step = "Reading your tables"
        sugg, problem, table_ids, withheld = await _with_service(user, prepare)
        share_values = send_data_allowed() and sugg.stats_available

        def progress(done: int, total: int) -> None:
            run.step = f"{done} of {total} tables"

        outcomes = await draft_words(
            provider,
            sugg,
            table_ids,
            user_id=user.id,
            share_values=share_values,
            withheld=withheld,
            progress=progress,
        )
        run.step = "Storing the proposals"
        words, source = words_for_store(outcomes), _source(provider, model)
        stored = await _with_service(
            user, lambda service: service.store_suggestions(body.namespace_id, sugg, words=words, ai_source=source)
        )
        failed = [o.table for o in outcomes if not o.ok]
        reason = "no_statistics" if problem else None
        if failed:
            reason = "all_tables_failed" if len(failed) == len(outcomes) else "some_tables_failed"
        stored.tables = len(table_ids)
        stored.stats_available, stored.stats_message = sugg.stats_available and problem is None, problem
        _audit(
            user.id,
            "semantic_draft",
            provider,
            "error" if failed else "success",
            {
                "namespace_id": body.namespace_id,
                "tables": [o.table for o in outcomes],
                "calls": sum(o.calls for o in outcomes),
                "added": stored.added.model_dump(),
                "statistics_sent": share_values,
                "columns_withheld": sum(len(o.withheld_columns) for o in outcomes),
                "tables_withheld": sum(o.reason == "kept_private" for o in outcomes),
                "ai_descriptions_dropped": sum(len(o.words.dropped) for o in outcomes),
            },
            reason,
        )
        run.counts = activity.counts_of(stored.added)
        run.summary = activity.summary_of(run.counts)
        if failed:
            run.summary += f" The model could not describe {', '.join(failed)}."
        return s.SemanticDraftOut(
            **stored.model_dump(),
            provider=provider.name,
            model=model,
            table_outcomes=outcomes,
            degraded=reason is not None,
            reason=reason,
        )


_KNOWN_TTL = 300.0
_known_cache: dict[tuple[int | None, int], tuple[float, dict[tuple[str, str], list[str]]]] = {}
_TEXT_TYPES = ("str", "utf8", "bool", "cat", "enum")


class _Menu:
    """The accepted metrics as menu items, narrowed to ``MAX_MENU`` by glosslayer's keyword search.

    A column kept from AI stays on the menu by name only: no label or synonyms, no known values read.
    """

    def __init__(self, service: Any, namespace_id: int, question: str) -> None:
        from flowfile_core.ai.semantic_ask import MAX_MENU, MenuItem
        from flowfile_core.catalog.semantic.confidentiality import read_withheld

        self.snap, _scope = service.snapshot(namespace_id, accepted_only=True)
        self.withheld = read_withheld(service.db, [t.table_id for t in self.snap.tables])
        self.withheld_count = sum(
            len(self.withheld.names(t.table_id, [c.name for c in t.fields])) for t in self.snap.tables
        )
        told = self.snap.model_value("ai_instructions")
        self.instructions = told if isinstance(told, str) and told.strip() else None
        metrics = self.snap.metrics
        if len(metrics) > MAX_MENU:
            from glosslayer.ids import SubjectId

            hits = service.view.search(question, kinds=["metric"], limit=MAX_MENU, for_ai=True)
            wanted = {SubjectId.parse(h["subject"]).name for h in hits}
            metrics = ([m for m in metrics if m.name in wanted] or metrics)[:MAX_MENU]
        dims = {m.key: self.dimensions(m) for m in metrics}
        known = self.known_values(service, namespace_id, metrics, dims)
        self.items = []
        for m in metrics:
            table = self.snap.table(m.table)
            private = self.withheld.whole_table(table.table_id)
            fields = {f: self.ref(m, f) for f in [*dims[m.key], *(c.name for c in table.fields)]}
            self.items.append(
                MenuItem(
                    metric=m,
                    meaning=None if private else m.description,
                    synonyms=[] if private else m.synonyms,
                    dimensions=dims[m.key],
                    columns=[c.name for c in table.fields],
                    dimension_words={d: self.column_words(m, d) for d in dims[m.key]},
                    known_values={f: known[ref] for f, ref in fields.items() if ref in known},
                )
            )

    def ref(self, m: s.MetricOut, field: str) -> tuple[str, str] | None:
        """``(table key, column)`` of a menu field: a bare column of the metric's table, or ``<table>.<column>``."""
        owner, _, column = field.rpartition(".")
        t = self.snap.table(owner or m.table)
        return (t.key, column) if t is not None else None

    def column(self, ref: tuple[str, str] | None) -> s.FieldOut | None:
        """The column, unless it is unknown or kept from AI."""
        t = self.snap.table(ref[0]) if ref else None
        return t.column(ref[1]) if t is not None and not self.withheld.blocks(t.table_id, ref[1]) else None

    def column_words(self, m: s.MetricOut, dimension: str) -> list[str]:
        """A dimension's label and synonyms as the model holds them, for matching the question's words."""
        col = self.column(self.ref(m, dimension))
        found = [col.value("label"), *(col.value("synonyms") or [])] if col is not None else []
        return [x for x in found if isinstance(x, str)]

    def dimensions(self, m: s.MetricOut) -> list[str]:
        """What a metric can be grouped by on the menu: its allowed dimensions, else every Group-by column it reaches.

        A metric stored with no allowed dimensions may be broken down by any column it reaches; offering all of those
        would crowd a small model's menu. A reached table whose short name another one already took is left out, since
        that name would not say which.
        """
        from flowfile_core.catalog.semantic.compiler import reachable_tables

        if m.dimensions:
            return m.dimensions
        tables: dict[str, s.SemanticTableOut] = {}
        for t in map(self.snap.table, reachable_tables(self.snap, m.table)):
            if t is not None:
                tables.setdefault(t.name, t)
        return [
            c.name if t.key == m.table else f"{t.name}.{c.name}"
            for t in tables.values()
            for c in t.fields
            if c.value("role") == "category"
        ]

    def known_values(
        self, service: Any, namespace_id: int, metrics: list[s.MetricOut], dims: dict[str, list[str]]
    ) -> dict[tuple[str, str], list[str]]:
        """Distinct values of the menu's category columns, for matching the question's words to filter values.

        They stay in core, never in the prompt. One SQL statement per namespace, cached for ``_KNOWN_TTL`` seconds in
        this process per caller; a failed read is an empty map. A column kept from AI is never asked for, and filtered
        out of a cached answer too, so a level raised since counts at once.
        """
        key = (service.user_id, namespace_id)
        hit = _known_cache.get(key)
        if hit is None or time.monotonic() - hit[0] >= _KNOWN_TTL:
            wanted = []
            for m in metrics:
                own = [c.name for c in self.snap.table(m.table).fields if c.value("role") == "category"]
                for ref in (self.ref(m, f) for f in [*dims[m.key], *own]):
                    col = self.column(ref)
                    role, dtype = (col.value("role"), str(col.dtype or "").lower()) if col else (None, "")
                    if role == "category" or (role is None and any(t in dtype for t in _TEXT_TYPES)):
                        wanted.append(ref)
            hit = _known_cache[key] = (time.monotonic(), service.known_values(wanted))
        return {ref: values for ref, values in hit[1].items() if self.column(ref) is not None}


@router.post("/ask", response_model=s.SemanticAskOut, tags=["ai"], responses={409: {"model": s.AiNotSetUpOut}})
async def ask_semantic_model(body: s.SemanticAskIn, current_user=Depends(get_current_active_user)):
    """A question answered with one accepted metric, run under the caller's rights; rows never reach the model.

    404 unknown provider or namespace · 403 not readable · 409 ``AI_NOT_SET_UP`` · 503 AI off; the rest is a 200.
    """
    return await _or_conflict(_ask(body, current_user))


async def _ask(body: s.SemanticAskIn, user: Any) -> s.SemanticAskOut:
    from flowfile_core.ai.semantic_ask import ask

    def prepare(service: Any) -> Any:
        provider = resolve_semantic_provider(service.db, user.id, body.provider, body.model)
        return provider, _Menu(service, body.namespace_id, body.question)

    provider, menu = await _with_service(user, prepare)
    out = s.SemanticAskOut(question=body.question, provider=provider.name, model=_model_label(provider))
    if not menu.items:
        out.no_metric_fits, out.degraded, out.reason = True, True, "no_accepted_metrics"
        out.notes.append("This model has no approved metrics yet. Approve one in Review, then ask again.")
        return out

    async def run(q: s.MetricQuery) -> s.MetricQueryResult:
        try:
            return await _with_service(user, lambda service: service.query_metric(body.namespace_id, q))
        except HTTPException as exc:
            raise (ValueError(exc.detail) if exc.status_code == 422 else exc) from None

    answer, out.attempts, result = await ask(
        provider, menu.items, body.question, user_id=user.id, run=run, instructions=menu.instructions
    )
    out.notes, out.partial, out.query, out.result = answer.notes, answer.partial, answer.query, result
    if answer.no_metric:
        out.no_metric_fits = True
        out.notes.append(answer.why)
    elif result is None:
        out.degraded, out.reason = True, answer.reason or "invalid_query"
        out.notes += answer.problems
    else:
        out.answered_with = result.metric
        if result.error:
            out.degraded, out.reason = True, "query_failed"
        else:
            _answer_sentence(out, answer, menu)
    _audit(
        user.id,
        "semantic_ask",
        provider,
        "success" if result is not None and not out.degraded else ("rejected" if out.no_metric_fits else "error"),
        {
            "namespace_id": body.namespace_id,
            "question_chars": len(body.question),
            "metric": result.metric.name if result is not None else None,
            "metric_version": result.metric.version if result is not None else None,
            "sql": result.sql if result is not None else None,
            "rows_returned": len(result.rows) if result is not None else 0,
            "columns_withheld": menu.withheld_count,
            "attempts": out.attempts,
            "intent": out.intent,
            "answered_in_words": out.answer is not None,
            **{k: answer.dropped.get(k, 0) for k in ("dimensions_dropped", "filters_dropped", "grain_dropped")},
            "instructions_sent": menu.instructions is not None,
        },
        out.reason,
    )
    return out


def _answer_sentence(out: s.SemanticAskOut, answer: Any, menu: _Menu) -> None:
    """The answer in words, rendered in core from the result rows; a failure leaves the table to answer by itself."""
    from flowfile_core.ai.semantic_answer import render

    metric = next(m.metric for m in menu.items if m.name == out.query.metric)
    try:
        rendered = render(out.query, out.result, metric, answer.intent, answer.gaps)
    except Exception:  # noqa: BLE001
        logger.warning("semantic answer sentence failed", exc_info=True)
        return
    out.intent = answer.intent.kind
    out.answer, out.facts = rendered.answer, rendered.facts
    out.notes = [n for n in out.notes if n not in rendered.notes]


@router.post("/suggest", response_model=s.SemanticSuggestRequestOut, tags=["ai"], responses=_CONFLICTS)
async def suggest_on_request(body: s.SemanticSuggestRequestIn, current_user=Depends(get_current_active_user)):
    """Specific suggestions for one request (or more of ``kinds``), checked by code and stored as proposals.

    404 unknown provider or namespace · 403 not writable · 409 ``AI_NOT_SET_UP`` or ``SUGGEST_RUNNING`` · 422 neither a
    request nor kinds · 503 AI off; after provider resolution every outcome is a 200. The run is recorded in
    ``catalog/semantic/activity.py`` without the request's text.
    """
    return await _or_conflict(_suggest(body, current_user))


async def _suggest(body: s.SemanticSuggestRequestIn, user: Any) -> s.SemanticSuggestRequestOut:
    from flowfile_core.ai.semantic_suggest import Context, store_round, suggest_for_request
    from flowfile_core.catalog.semantic import activity

    def prepare(service: Any) -> Any:
        provider = resolve_semantic_provider(service.db, user.id, body.provider, body.model)
        service.namespace(body.namespace_id, writable=True)
        return provider, Context.read(service, body.namespace_id)

    provider, ctx = await _with_service(user, prepare)
    label = _model_label(provider)
    out = s.SemanticSuggestRequestOut(
        request=body.request,
        kinds=body.kinds or None,
        provider=provider.name,
        model=label,
        withheld_columns=ctx.withheld_columns,
    )
    source = _source(provider, label)
    what = "Asking the AI for specific suggestions…"
    with activity.tracking(body.namespace_id, "request", what, user.id) as run:

        async def apply(items: list[dict[str, Any]]) -> Any:
            run.step = "Storing the suggestions"
            return await _with_service(
                user, lambda service: store_round(service, body.namespace_id, items, out, ai_source=source)
            )

        run.step = "Waiting for the AI"
        await suggest_for_request(provider, ctx, out, user_id=user.id, apply=apply)
        if out.reason is None and not out.proposed and any(r.retry for r in out.skipped):
            out.reason = "all_refused"
        out.degraded = out.reason is not None
        out.notes = list(dict.fromkeys(out.notes))
        # One entry per suggestion, the latest refusal winning.
        out.skipped = [r for i, r in enumerate(out.skipped) if r.what not in {x.what for x in out.skipped[i + 1 :]}]
        _audit(
            user.id,
            "semantic_suggest",
            provider,
            "error" if out.degraded and not out.proposed else "success",
            {
                "namespace_id": body.namespace_id,
                "request_chars": len(body.request or ""),
                "kinds": out.kinds,
                "attempts": out.attempts,
                "proposed": len(out.proposed),
                "proposed_kinds": [p.kind for p in out.proposed],
                "skipped": len(out.skipped),
                "nothing_to_suggest": out.nothing_to_suggest,
                "statistics_sent": False,
                "columns_withheld": out.withheld_columns,
            },
            out.reason,
        )
        run.counts = dict(Counter(p.kind if p.kind == "synonyms" else f"{p.kind}s" for p in out.proposed))
        if run.counts:
            run.summary = activity.summary_of(run.counts)
        elif out.nothing_to_suggest:
            run.summary = "The AI found nothing to suggest."
        elif out.skipped:
            run.summary = "Nothing was proposed: no suggestion passed the checks."
        else:
            run.summary = "Nothing new to propose."
        return out
