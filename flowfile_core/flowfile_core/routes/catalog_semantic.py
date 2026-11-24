"""Semantic-model routes under ``/catalog/namespaces/{id}/semantic-model`` (``catalog/semantic/service.py``).

Kept out of ``routes/catalog.py``: a router of its own, JWT-only (a person's facts are signed with their user id),
sync handlers and no trailing slashes. Domain errors map to 403 / 404 / 409 / 422, never 401, which the frontend's
axios interceptor reads as an expired session.
"""

from functools import wraps

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import PlainTextResponse
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
from flowfile_core.catalog.semantic.service import SemanticModelService
from flowfile_core.database.connection import get_db
from flowfile_core.schemas.semantic_schema import (
    FactOut,
    ProposalsIn,
    ReviewAction,
    ReviewItemOut,
    ReviewResultOut,
    SemanticEventOut,
    SemanticModelOut,
    SemanticModelUpdate,
    SemanticSyncOut,
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


@router.get(f"{_BASE}/review", response_model=list[ReviewItemOut])
@_handle
def get_semantic_review_queue(namespace_id: int, service: SemanticModelService = Depends(get_semantic_service)):
    return service.review_queue(namespace_id)


@router.post(f"{_BASE}/review", response_model=ReviewResultOut)
@_handle
def review_semantic_facts(
    namespace_id: int, body: ReviewAction, service: SemanticModelService = Depends(get_semantic_service)
):
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


@router.get(f"{_BASE}/export", response_class=PlainTextResponse)
@_handle
def export_semantic_model(namespace_id: int, service: SemanticModelService = Depends(get_semantic_service)):
    """The namespace's accepted model as Apache Ossie YAML."""
    return PlainTextResponse(service.export(namespace_id), media_type="application/yaml")
