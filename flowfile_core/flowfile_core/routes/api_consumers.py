"""HTTP routes for managing API consumers (reusable API clients / service accounts).

A consumer holds rotatable API keys and is granted access to one or more published
flow endpoints, so a single key can call multiple flows. Managed from the frontend
"API Access" panel (available in both local/Electron and Docker modes).

Ownership: a consumer belongs to the user who created it and may be granted only
that owner's endpoints. Non-admins manage their own consumers; admins manage all.
In Electron mode the single ``local_user`` is an admin and owns everything, so the
panel works there with no special-casing.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db
from flowfile_core.flowfile import api_consumer_manager
from flowfile_core.routes.flow_api import _consumer_out, _endpoint_out, _key_out
from flowfile_core.schemas.flow_api_schema import (
    ApiConsumerCreate,
    ApiConsumerGrant,
    ApiConsumerOut,
    ApiConsumerUpdate,
    ApiEndpointOut,
    ApiKeyCreate,
    ApiKeyCreated,
    ApiKeyOut,
    ApiKeyUpdate,
)

router = APIRouter(prefix="/api-consumers", tags=["api_consumers"])


# Ownership helpers (mirror flow_api._get_owned_endpoint)


def _scope(current_user) -> int | None:
    """Owner filter for queries: None (all rows) for admins, else the user's own id."""
    return None if getattr(current_user, "is_admin", False) else current_user.id


def _get_consumer(db: Session, consumer_id: int, current_user) -> db_models.ApiConsumer:
    try:
        return api_consumer_manager.get_consumer(db, consumer_id, _scope(current_user))
    except LookupError as exc:
        raise HTTPException(status_code=404, detail="Consumer not found") from exc


def _owned_endpoint_for_consumer(
    db: Session, consumer: db_models.ApiConsumer, endpoint_id: int
) -> db_models.FlowApiEndpoint:
    """An endpoint the consumer is allowed to be granted: it must belong to the consumer's owner."""
    ep = db.get(db_models.FlowApiEndpoint, endpoint_id)
    if ep is None or ep.owner_id != consumer.owner_id:
        raise HTTPException(status_code=404, detail="Endpoint not found")
    return ep


def _endpoints_out(db: Session, endpoints: list[db_models.FlowApiEndpoint]) -> list[ApiEndpointOut]:
    reg_ids = {ep.registration_id for ep in endpoints}
    reg_by_id = (
        {
            reg.id: reg
            for reg in db.query(db_models.FlowRegistration).filter(db_models.FlowRegistration.id.in_(reg_ids)).all()
        }
        if reg_ids
        else {}
    )
    return [_endpoint_out(ep, reg_by_id.get(ep.registration_id)) for ep in endpoints]


# Consumer CRUD


@router.post("", response_model=ApiConsumerOut, status_code=status.HTTP_201_CREATED)
def create_consumer(
    body: ApiConsumerCreate,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    try:
        consumer = api_consumer_manager.create_consumer(
            db, current_user.id, name=body.name, description=body.description, enabled=body.enabled
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return _consumer_out(db, consumer)


@router.get("", response_model=list[ApiConsumerOut])
def list_consumers(
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    return [_consumer_out(db, c) for c in api_consumer_manager.list_consumers(db, _scope(current_user))]


@router.get("/{consumer_id}", response_model=ApiConsumerOut)
def get_consumer(
    consumer_id: int,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    return _consumer_out(db, _get_consumer(db, consumer_id, current_user))


@router.put("/{consumer_id}", response_model=ApiConsumerOut)
def update_consumer(
    consumer_id: int,
    body: ApiConsumerUpdate,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    consumer = _get_consumer(db, consumer_id, current_user)
    try:
        consumer = api_consumer_manager.update_consumer(
            db, consumer, name=body.name, description=body.description, enabled=body.enabled
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return _consumer_out(db, consumer)


@router.delete("/{consumer_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_consumer(
    consumer_id: int,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    consumer = _get_consumer(db, consumer_id, current_user)
    api_consumer_manager.delete_consumer(db, consumer)


# Grants (which flows a consumer may call)


@router.get("/{consumer_id}/endpoints", response_model=list[ApiEndpointOut])
def list_granted_endpoints(
    consumer_id: int,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    consumer = _get_consumer(db, consumer_id, current_user)
    return _endpoints_out(db, api_consumer_manager.list_granted_endpoints(db, consumer))


@router.get("/{consumer_id}/available-endpoints", response_model=list[ApiEndpointOut])
def list_available_endpoints(
    consumer_id: int,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """All published endpoints the consumer *could* be granted (its owner's endpoints).

    The frontend diffs this against the granted list to render the grant picker.
    """
    consumer = _get_consumer(db, consumer_id, current_user)
    endpoints = (
        db.query(db_models.FlowApiEndpoint).filter(db_models.FlowApiEndpoint.owner_id == consumer.owner_id).all()
    )
    return _endpoints_out(db, endpoints)


@router.post("/{consumer_id}/endpoints", response_model=ApiEndpointOut, status_code=status.HTTP_201_CREATED)
def grant_endpoint(
    consumer_id: int,
    body: ApiConsumerGrant,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    consumer = _get_consumer(db, consumer_id, current_user)
    ep = _owned_endpoint_for_consumer(db, consumer, body.endpoint_id)
    try:
        api_consumer_manager.grant_endpoint(db, consumer, ep)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    registration = db.get(db_models.FlowRegistration, ep.registration_id)
    return _endpoint_out(ep, registration)


@router.delete("/{consumer_id}/endpoints/{endpoint_id}", status_code=status.HTTP_204_NO_CONTENT)
def revoke_endpoint(
    consumer_id: int,
    endpoint_id: int,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    consumer = _get_consumer(db, consumer_id, current_user)
    api_consumer_manager.revoke_endpoint(db, consumer, endpoint_id)


# Keys


@router.get("/{consumer_id}/keys", response_model=list[ApiKeyOut])
def list_keys(
    consumer_id: int,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    consumer = _get_consumer(db, consumer_id, current_user)
    return [_key_out(k) for k in api_consumer_manager.list_keys(db, consumer)]


@router.post("/{consumer_id}/keys", response_model=ApiKeyCreated, status_code=status.HTTP_201_CREATED)
def create_key(
    consumer_id: int,
    body: ApiKeyCreate,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Mint a key for the consumer. The raw token is returned exactly once."""
    consumer = _get_consumer(db, consumer_id, current_user)
    key, raw_token = api_consumer_manager.create_key(db, consumer, name=body.name, expires_at=body.expires_at)
    return ApiKeyCreated(**_key_out(key).model_dump(), api_key=raw_token)


@router.patch("/{consumer_id}/keys/{key_id}", response_model=ApiKeyOut)
def update_key(
    consumer_id: int,
    key_id: int,
    body: ApiKeyUpdate,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    consumer = _get_consumer(db, consumer_id, current_user)
    try:
        key = api_consumer_manager.get_key(db, consumer, key_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail="Key not found") from exc
    key = api_consumer_manager.update_key(db, key, name=body.name, enabled=body.enabled)
    return _key_out(key)


@router.delete("/{consumer_id}/keys/{key_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_key(
    consumer_id: int,
    key_id: int,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    consumer = _get_consumer(db, consumer_id, current_user)
    try:
        key = api_consumer_manager.get_key(db, consumer, key_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail="Key not found") from exc
    api_consumer_manager.delete_key(db, key)
