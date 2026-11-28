"""Semantic-model routes under ``/catalog/namespaces/{id}/semantic-model`` (``catalog/semantic/service.py``).

Kept out of ``routes/catalog.py``: a router of its own, JWT-only (a person's facts are signed with their user id),
sync handlers and no trailing slashes. Domain errors map to 403 / 404 / 409 / 422, never 401, which the frontend's
axios interceptor reads as an expired session.
"""

from typing import Annotated

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse, PlainTextResponse
from sqlalchemy.orm import Session

from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.catalog.access import AccessResolver
from flowfile_core.catalog.semantic import activity
from flowfile_core.catalog.semantic.service import SemanticModelService
from flowfile_core.database.connection import get_db
from flowfile_core.routes.catalog import handle_catalog_exceptions
from flowfile_core.schemas import semantic_schema as s

router = APIRouter(prefix="/catalog", tags=["catalog"], dependencies=[Depends(get_current_active_user)])

_BASE = "/namespaces/{namespace_id}/semantic-model"


def get_semantic_service(
    db: Session = Depends(get_db), current_user=Depends(get_current_active_user)
) -> SemanticModelService:
    """The service for the signed-in user, with the same ``AccessResolver`` the catalog router uses."""
    return SemanticModelService(db, current_user, AccessResolver(db, current_user))


Service = Annotated[SemanticModelService, Depends(get_semantic_service)]


def suggest_running(exc: activity.ActivityRunning) -> JSONResponse:
    """The top-level 409 body every Suggest route answers while another run holds the namespace."""
    return JSONResponse(status_code=409, content=s.SuggestRunningOut(message=str(exc)).model_dump())


@router.get(_BASE, response_model=s.SemanticModelOut)
@handle_catalog_exceptions()
def get_semantic_model(namespace_id: int, service: Service):
    """The model of a schema (level 1) or of a catalog (level 0), with every fact's review status."""
    return service.get_model(namespace_id)


@router.put(_BASE, response_model=s.SemanticModelOut)
@handle_catalog_exceptions()
def update_semantic_model(namespace_id: int, body: s.SemanticModelUpdate, service: Service):
    """Propose the model's name, description and AI instructions (facts on ``model:<namespace path>``)."""
    return service.update_model(namespace_id, body)


@router.post(f"{_BASE}/proposals", response_model=list[s.FactOut])
@handle_catalog_exceptions()
def propose_semantic_facts(namespace_id: int, body: s.ProposalsIn, service: Service):
    """Store proposals after checking them against the catalog; answers the stored facts with their status."""
    return service.propose(namespace_id, body.proposals)


@router.post(f"{_BASE}/joins/check", response_model=list[s.JoinCheckOut])
@handle_catalog_exceptions()
def check_semantic_joins(namespace_id: int, service: Service):
    """Measure every join of the model (accepted and pending): one aggregate query per side. Reading rights suffice."""
    return service.check_joins(namespace_id)


@router.post(f"{_BASE}/data-rules/run", response_model=s.DataRulesRunOut)
@handle_catalog_exceptions()
def run_semantic_data_rules(namespace_id: int, service: Service, body: s.DataRulesRunIn | None = None):
    """Run the accepted data rules of the model's tables, or of the tables named, on the compute backend: one plan
    per table. Reading rights suffice. Answers 503 when compute cannot be reached."""
    return service.run_data_rules(namespace_id, body.table_ids if body else None)


@router.get(f"{_BASE}/data-rules/results", response_model=list[s.DataRuleTableOut])
@handle_catalog_exceptions()
def get_semantic_data_rule_results(namespace_id: int, service: Service):
    """Each table's newest run of its data rules, for the rules accepted now."""
    return service.data_rule_results(namespace_id)


@router.get(f"{_BASE}/joins/dependents", response_model=list[s.JoinDependentOut])
@handle_catalog_exceptions()
def get_semantic_join_dependents(
    namespace_id: int,
    service: Service,
    key: str = Query(..., description="A join's key such as relationship:cat.sales::orders_customers"),
):
    """The accepted metrics that would stop working without this join. The key holds dots and colons, hence a query."""
    return service.join_dependents(namespace_id, key)


@router.get(f"{_BASE}/review", response_model=list[s.ReviewItemOut])
@handle_catalog_exceptions()
def get_semantic_review_queue(namespace_id: int, service: Service):
    return service.review_queue(namespace_id)


@router.post(f"{_BASE}/review", response_model=s.ReviewResultOut)
@handle_catalog_exceptions()
def review_semantic_facts(namespace_id: int, body: s.ReviewAction, service: Service):
    """Accept, reject, retract or reopen facts; ``subject`` with ``retract`` removes a whole metric or join."""
    return service.review(namespace_id, body)


@router.get(f"{_BASE}/history", response_model=list[s.SemanticEventOut])
@handle_catalog_exceptions()
def get_semantic_history(
    namespace_id: int,
    service: Service,
    subject: str = Query(..., description="A subject such as field:cat.sales.orders/amount"),
):
    return service.history(namespace_id, subject)


@router.post(f"{_BASE}/sync", response_model=s.SemanticSyncOut)
@handle_catalog_exceptions()
def sync_semantic_model(namespace_id: int, service: Service):
    """Re-read the whole catalog into the ledger now (new and dropped tables and columns)."""
    return service.sync(namespace_id)


@router.post(f"{_BASE}/suggest", response_model=s.SuggestOut, responses={409: {"model": s.SuggestRunningOut}})
@handle_catalog_exceptions()
def suggest_semantic_model(namespace_id: int, service: Service, body: s.SuggestIn | None = None):
    """Rule-based proposals from one aggregate query per table (no AI): roles, joins and metrics, queued for review.

    A second call adds nothing. Without a reachable worker the rules use names and types, ``stats_available`` false.
    The run is recorded in ``activity`` (409 ``SUGGEST_RUNNING`` while another runs for this namespace).
    """
    kinds = body.kinds if body is not None else None
    service.namespace(namespace_id, writable=True)
    try:
        with activity.tracking(namespace_id, "suggest", activity.suggest_label(kinds), service.user_id) as run:
            out = service.suggest(namespace_id, kinds=kinds)
            run.counts = activity.counts_of(out.added)
            run.summary = activity.summary_of(run.counts)
            return out
    except activity.ActivityRunning as exc:
        return suggest_running(exc)


@router.get(f"{_BASE}/activity", response_model=s.SemanticActivityOut)
@handle_catalog_exceptions()
def get_semantic_activity(namespace_id: int, service: Service):
    """The Suggest run going on for this namespace now, and the last finished one; reading rights are enough."""
    service.namespace(namespace_id)
    running, last = activity.read(namespace_id, service.user_id)
    return s.SemanticActivityOut(running=running, last=last)


@router.post(f"{_BASE}/query", response_model=s.MetricQueryResult)
@handle_catalog_exceptions()
def query_semantic_metric(namespace_id: int, body: s.MetricQuery, service: Service):
    """One metric by dimensions, run like ``POST /catalog/sql/execute``; one that cannot compile is a 422."""
    return service.query_metric(namespace_id, body)


@router.post(f"{_BASE}/query/save-as-flow", response_model=s.MetricQuerySaveAsFlowOut)
@handle_catalog_exceptions()
def save_semantic_metric_query_as_flow(namespace_id: int, body: s.MetricQuerySaveAsFlow, service: Service):
    """Register the compiled query as a flow of catalog readers into a SQL node."""
    return service.save_metric_query_as_flow(namespace_id, body)


@router.get(f"{_BASE}/export", response_class=PlainTextResponse)
@handle_catalog_exceptions()
def export_semantic_model(namespace_id: int, service: Service):
    """The namespace's accepted model as Apache Ossie YAML."""
    return PlainTextResponse(service.export(namespace_id), media_type="application/yaml")
