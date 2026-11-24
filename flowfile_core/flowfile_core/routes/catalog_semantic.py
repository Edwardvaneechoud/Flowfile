"""Semantic-model routes under ``/catalog/namespaces/{id}/semantic-model`` (``catalog/semantic/service.py``).

Kept out of ``routes/catalog.py``: a router of its own, JWT-only (a person's facts are signed with their user id),
sync handlers and no trailing slashes. Domain errors map to 403 / 404 / 409 / 422, never 401, which the frontend's
axios interceptor reads as an expired session.
"""

from functools import wraps

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse, PlainTextResponse
from sqlalchemy.orm import Session

from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.catalog.access import AccessResolver
from flowfile_core.catalog.exceptions import (
    NamespaceNotFoundError,
    NotAuthorizedError,
    SemanticConflictError,
    SemanticNotFoundError,
    SemanticValueError,
)
from flowfile_core.catalog.semantic import activity
from flowfile_core.catalog.semantic.service import SemanticModelService
from flowfile_core.database.connection import get_db
from flowfile_core.schemas.semantic_schema import (
    FactOut,
    JoinCheckOut,
    JoinDependentOut,
    MetricQuery,
    MetricQueryResult,
    MetricQuerySaveAsFlow,
    MetricQuerySaveAsFlowOut,
    ProposalsIn,
    ReviewAction,
    ReviewItemOut,
    ReviewResultOut,
    SemanticActivityOut,
    SemanticEventOut,
    SemanticModelOut,
    SemanticModelUpdate,
    SemanticSyncOut,
    SuggestIn,
    SuggestOut,
    SuggestRunningOut,
)

router = APIRouter(prefix="/catalog", tags=["catalog"], dependencies=[Depends(get_current_active_user)])

_BASE = "/namespaces/{namespace_id}/semantic-model"

_EXCEPTION_MAP: tuple[tuple[type[Exception], int], ...] = (
    (NamespaceNotFoundError, 404),
    (SemanticNotFoundError, 404),
    (NotAuthorizedError, 403),
    (SemanticConflictError, 409),
    (SemanticValueError, 422),
    (ValueError, 422),
)


def get_semantic_service(
    db: Session = Depends(get_db),
    current_user=Depends(get_current_active_user),
) -> SemanticModelService:
    """The service for the signed-in user, with the same ``AccessResolver`` the catalog router uses."""
    return SemanticModelService(db, current_user, AccessResolver(db, current_user))


def _handle(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except tuple(exc for exc, _ in _EXCEPTION_MAP) as exc:
            status = next(code for cls, code in _EXCEPTION_MAP if isinstance(exc, cls))
            raise HTTPException(status, detail=str(exc)) from None

    return wrapper


@router.get(_BASE, response_model=SemanticModelOut)
@_handle
def get_semantic_model(namespace_id: int, service: SemanticModelService = Depends(get_semantic_service)):
    """The model of a schema (level 1) or of a catalog (level 0), with every fact's review status."""
    return service.get_model(namespace_id)


@router.put(_BASE, response_model=SemanticModelOut)
@_handle
def update_semantic_model(
    namespace_id: int, body: SemanticModelUpdate, service: SemanticModelService = Depends(get_semantic_service)
):
    """Propose the model's name, description and AI instructions (facts on ``model:<namespace path>``)."""
    return service.update_model(namespace_id, body)


@router.post(f"{_BASE}/proposals", response_model=list[FactOut])
@_handle
def propose_semantic_facts(
    namespace_id: int, body: ProposalsIn, service: SemanticModelService = Depends(get_semantic_service)
):
    """Store proposals after checking them against the catalog; answers the stored facts with their status."""
    return service.propose(namespace_id, body.proposals)


@router.post(f"{_BASE}/joins/check", response_model=list[JoinCheckOut])
@_handle
def check_semantic_joins(namespace_id: int, service: SemanticModelService = Depends(get_semantic_service)):
    """Measure every join of the model (accepted and pending) against the data: one aggregate query per side.

    Reading rights are enough. Each entry says ``ok``, ``reversed``, ``many_to_many`` or ``unchecked`` (no compute,
    an empty or unreadable table), with a sentence a person can act on.
    """
    return service.check_joins(namespace_id)


@router.get(f"{_BASE}/joins/dependents", response_model=list[JoinDependentOut])
@_handle
def get_semantic_join_dependents(
    namespace_id: int,
    key: str = Query(..., description="A join's key such as relationship:cat.sales::orders_customers"),
    service: SemanticModelService = Depends(get_semantic_service),
):
    """The accepted metrics that would stop working without this join, worked out by the compiler alone.

    The key goes in the query string because it holds dots and colons. Reading rights are enough.
    """
    return service.join_dependents(namespace_id, key)


@router.get(f"{_BASE}/review", response_model=list[ReviewItemOut])
@_handle
def get_semantic_review_queue(namespace_id: int, service: SemanticModelService = Depends(get_semantic_service)):
    return service.review_queue(namespace_id)


@router.post(f"{_BASE}/review", response_model=ReviewResultOut)
@_handle
def review_semantic_facts(
    namespace_id: int, body: ReviewAction, service: SemanticModelService = Depends(get_semantic_service)
):
    """Accept, reject, retract or reopen facts; ``subject`` with ``retract`` removes a whole metric or join."""
    return service.review(namespace_id, body)


@router.get(f"{_BASE}/history", response_model=list[SemanticEventOut])
@_handle
def get_semantic_history(
    namespace_id: int,
    subject: str = Query(..., description="A subject such as field:cat.sales.orders/amount"),
    service: SemanticModelService = Depends(get_semantic_service),
):
    return service.history(namespace_id, subject)


@router.post(f"{_BASE}/sync", response_model=SemanticSyncOut)
@_handle
def sync_semantic_model(namespace_id: int, service: SemanticModelService = Depends(get_semantic_service)):
    """Re-read the whole catalog into the ledger now (new and dropped tables and columns)."""
    return service.sync(namespace_id)


def suggest_running(exc: activity.ActivityRunning) -> JSONResponse:
    """The top-level 409 body every Suggest route answers while another run holds the namespace."""
    return JSONResponse(status_code=409, content=SuggestRunningOut(message=str(exc)).model_dump())


@router.post(f"{_BASE}/suggest", response_model=SuggestOut, responses={409: {"model": SuggestRunningOut}})
@_handle
def suggest_semantic_model(
    namespace_id: int,
    body: SuggestIn | None = None,
    service: SemanticModelService = Depends(get_semantic_service),
):
    """Rule-based proposals from one aggregate query per table (no AI): roles, joins and metrics, queued for review.

    A second call adds nothing. Without a reachable worker the rules use names and types, ``stats_available`` false.
    An optional body ``{"kinds": [...]}`` stores only those kinds; no body stores every kind, as before. The run is
    recorded in ``activity`` (409 ``SUGGEST_RUNNING`` while another runs for this namespace).
    """
    kinds = body.kinds if body is not None else None
    ns, _path = service._namespace(namespace_id)
    service._require_writable(ns)
    label = activity.label_for("suggest", list(kinds) if kinds else None)
    try:
        with activity.tracking(namespace_id, "suggest", label, service.user_id) as run:
            out = service.suggest(namespace_id, kinds=kinds)
            counts = activity.counts_of(out.added)
            run.done(activity.summary_of(counts), counts)
            return out
    except activity.ActivityRunning as exc:
        return suggest_running(exc)


@router.get(f"{_BASE}/activity", response_model=SemanticActivityOut)
@_handle
def get_semantic_activity(namespace_id: int, service: SemanticModelService = Depends(get_semantic_service)):
    """The Suggest run going on for this namespace now, and the last finished one; reading rights are enough.

    In-process state: a core restart forgets it. Holds no request text, only the label, step, summary and counts.
    """
    service._namespace(namespace_id)
    running, last = activity.read(namespace_id)
    viewer = service.user_id
    return SemanticActivityOut(
        running=running.as_dict(viewer) if running else None,
        last=last.as_dict(viewer) if last else None,
    )


@router.post(f"{_BASE}/query", response_model=MetricQueryResult)
@_handle
def query_semantic_metric(
    namespace_id: int, body: MetricQuery, service: SemanticModelService = Depends(get_semantic_service)
):
    """One metric by dimensions, compiled to one SQL statement and run like ``POST /catalog/sql/execute``.

    A query that cannot compile is a 422; an execution failure comes back in ``error``.
    """
    return service.query_metric(namespace_id, body)


@router.post(f"{_BASE}/query/save-as-flow", response_model=MetricQuerySaveAsFlowOut)
@_handle
def save_semantic_metric_query_as_flow(
    namespace_id: int, body: MetricQuerySaveAsFlow, service: SemanticModelService = Depends(get_semantic_service)
):
    """Register the compiled query as a flow of catalog readers into a SQL node."""
    return service.save_metric_query_as_flow(namespace_id, body)


@router.get(f"{_BASE}/export", response_class=PlainTextResponse)
@_handle
def export_semantic_model(namespace_id: int, service: SemanticModelService = Depends(get_semantic_service)):
    """The namespace's accepted model as Apache Ossie YAML."""
    return PlainTextResponse(service.export(namespace_id), media_type="application/yaml")
