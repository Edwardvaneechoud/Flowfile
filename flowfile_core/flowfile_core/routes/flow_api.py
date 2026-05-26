"""HTTP routes for hosting flows as data APIs.

Two routers:
- ``data_router`` (public, API-key authenticated): ``GET /api/data/{slug}`` runs a
  published flow synchronously and returns its result as JSON.
- ``management_router`` (JWT): publish/unpublish a registered flow and manage its
  per-endpoint API keys, under ``/flow-api``.
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import anyio
from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from flowfile_core.auth.api_key import generate_api_key, verify_api_key
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db
from flowfile_core.flowfile.api_runner import (
    ApiConfigError,
    ApiExecutionError,
    ApiParamError,
    run_flow_as_api,
)
from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.schemas.flow_api_schema import (
    ApiEndpointCreate,
    ApiEndpointOut,
    ApiEndpointUpdate,
    ApiKeyCreate,
    ApiKeyCreated,
    ApiKeyOut,
    ApiParamSpec,
    ApiTestRequest,
)

_API_RUN_TIMEOUT = float(os.environ.get("FLOWFILE_API_RUN_TIMEOUT_SECONDS", "120"))

data_router = APIRouter(tags=["flow_api"])
management_router = APIRouter(prefix="/flow-api", tags=["flow_api"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_param_schema(raw: str | None) -> list[ApiParamSpec]:
    if not raw:
        return []
    return [ApiParamSpec(**spec) for spec in json.loads(raw)]


def _dump_param_schema(specs: list[ApiParamSpec]) -> str:
    return json.dumps([spec.model_dump() for spec in specs])


def _endpoint_out(ep: db_models.FlowApiEndpoint, db: Session) -> ApiEndpointOut:
    registration = db.get(db_models.FlowRegistration, ep.registration_id)
    return ApiEndpointOut(
        id=ep.id,
        registration_id=ep.registration_id,
        owner_id=ep.owner_id,
        slug=ep.slug,
        enabled=ep.enabled,
        response_node_id=ep.response_node_id,
        parameters=_parse_param_schema(ep.param_schema_json),
        path=f"/api/data/{ep.slug}",
        flow_name=registration.name if registration else None,
        created_at=ep.created_at,
        updated_at=ep.updated_at,
    )


def _key_out(key: db_models.FlowApiKey) -> ApiKeyOut:
    return ApiKeyOut(
        id=key.id,
        endpoint_id=key.endpoint_id,
        name=key.name,
        key_prefix=key.key_prefix,
        enabled=key.enabled,
        last_used_at=key.last_used_at,
        expires_at=key.expires_at,
        created_at=key.created_at,
    )


def _get_owned_endpoint(db: Session, endpoint_id: int, user_id: int) -> db_models.FlowApiEndpoint:
    ep = db.get(db_models.FlowApiEndpoint, endpoint_id)
    if ep is None or ep.owner_id != user_id:
        raise HTTPException(status_code=404, detail="Endpoint not found")
    return ep


def _resolve_api_node_id(flow_path: str | None, user_id: int) -> int:
    """Open the flow and return the id of its single ``api_response`` node."""
    if not flow_path:
        raise HTTPException(status_code=400, detail="Flow has no saved file to publish")
    try:
        flow = open_flow(Path(flow_path), user_id=user_id)
    except Exception as exc:  # noqa: BLE001 - surface load failure to the user
        raise HTTPException(status_code=400, detail=f"Could not open flow: {exc}") from exc
    api_nodes = [n for n in flow.nodes if n.node_type == "api_response"]
    if len(api_nodes) == 0:
        raise HTTPException(status_code=400, detail="Flow must contain exactly one API response node (found none)")
    if len(api_nodes) > 1:
        raise HTTPException(status_code=400, detail="Flow must contain exactly one API response node (found multiple)")
    return api_nodes[0].node_id


# ---------------------------------------------------------------------------
# Public data endpoint
# ---------------------------------------------------------------------------


@data_router.get("/api/data/{slug}")
async def run_published_flow(
    slug: str,
    request: Request,
    endpoint: db_models.FlowApiEndpoint = Depends(verify_api_key),
    db: Session = Depends(get_db),
):
    """Run a published flow synchronously and return its data as JSON."""
    registration = db.get(db_models.FlowRegistration, endpoint.registration_id)
    if registration is None or not registration.flow_path:
        raise HTTPException(status_code=500, detail="Published flow is no longer available")

    specs = _parse_param_schema(endpoint.param_schema_json)
    query = dict(request.query_params)
    flow_path = registration.flow_path
    owner_id = endpoint.owner_id

    def _run():
        return run_flow_as_api(flow_path, owner_id, specs, query)

    try:
        return await asyncio.wait_for(anyio.to_thread.run_sync(_run), timeout=_API_RUN_TIMEOUT)
    except ApiParamError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except (ApiConfigError, ApiExecutionError) as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except asyncio.TimeoutError as exc:
        raise HTTPException(status_code=504, detail="Flow execution timed out") from exc


# ---------------------------------------------------------------------------
# Management endpoints (JWT)
# ---------------------------------------------------------------------------


@management_router.post("/endpoints", response_model=ApiEndpointOut, status_code=status.HTTP_201_CREATED)
def publish_endpoint(
    body: ApiEndpointCreate,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Publish a registered flow as an API endpoint."""
    registration = db.get(db_models.FlowRegistration, body.registration_id)
    if registration is None or registration.owner_id != current_user.id:
        raise HTTPException(status_code=404, detail="Flow not found")
    if db.query(db_models.FlowApiEndpoint).filter_by(registration_id=body.registration_id).first() is not None:
        raise HTTPException(status_code=409, detail="Flow is already published as an API")
    if db.query(db_models.FlowApiEndpoint).filter_by(slug=body.slug).first() is not None:
        raise HTTPException(status_code=409, detail="Slug already in use")

    response_node_id = _resolve_api_node_id(registration.flow_path, current_user.id)
    ep = db_models.FlowApiEndpoint(
        registration_id=body.registration_id,
        owner_id=current_user.id,
        slug=body.slug,
        enabled=body.enabled,
        response_node_id=response_node_id,
        param_schema_json=_dump_param_schema(body.parameters),
    )
    db.add(ep)
    db.commit()
    db.refresh(ep)
    return _endpoint_out(ep, db)


@management_router.get("/endpoints", response_model=list[ApiEndpointOut])
def list_endpoints(
    registration_id: int | None = None,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """List the current user's published endpoints, optionally filtered by flow."""
    query = db.query(db_models.FlowApiEndpoint).filter(db_models.FlowApiEndpoint.owner_id == current_user.id)
    if registration_id is not None:
        query = query.filter(db_models.FlowApiEndpoint.registration_id == registration_id)
    return [_endpoint_out(ep, db) for ep in query.all()]


@management_router.get("/endpoints/{endpoint_id}", response_model=ApiEndpointOut)
def get_endpoint(
    endpoint_id: int,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    return _endpoint_out(_get_owned_endpoint(db, endpoint_id, current_user.id), db)


@management_router.put("/endpoints/{endpoint_id}", response_model=ApiEndpointOut)
def update_endpoint(
    endpoint_id: int,
    body: ApiEndpointUpdate,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    ep = _get_owned_endpoint(db, endpoint_id, current_user.id)
    if body.slug is not None and body.slug != ep.slug:
        clash = (
            db.query(db_models.FlowApiEndpoint)
            .filter(db_models.FlowApiEndpoint.slug == body.slug, db_models.FlowApiEndpoint.id != ep.id)
            .first()
        )
        if clash is not None:
            raise HTTPException(status_code=409, detail="Slug already in use")
        ep.slug = body.slug
    if body.enabled is not None:
        ep.enabled = body.enabled
    if body.parameters is not None:
        ep.param_schema_json = _dump_param_schema(body.parameters)
    db.commit()
    db.refresh(ep)
    return _endpoint_out(ep, db)


@management_router.post("/endpoints/{endpoint_id}/test")
def test_endpoint(
    endpoint_id: int,
    body: ApiTestRequest | None = None,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Owner-only 'try it': run the published flow with the given params and return the JSON.

    Runs as the endpoint owner without requiring an API key, so the owner can test
    the endpoint from the catalog UI. Mirrors the public endpoint's behavior.
    """
    ep = _get_owned_endpoint(db, endpoint_id, current_user.id)
    registration = db.get(db_models.FlowRegistration, ep.registration_id)
    if registration is None or not registration.flow_path:
        raise HTTPException(status_code=404, detail="Flow not found")
    specs = _parse_param_schema(ep.param_schema_json)
    params = dict(body.params) if body else {}
    try:
        return run_flow_as_api(registration.flow_path, ep.owner_id, specs, params)
    except ApiParamError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except (ApiConfigError, ApiExecutionError) as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@management_router.delete("/endpoints/{endpoint_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_endpoint(
    endpoint_id: int,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    ep = _get_owned_endpoint(db, endpoint_id, current_user.id)
    db.query(db_models.FlowApiKey).filter(db_models.FlowApiKey.endpoint_id == ep.id).delete()
    db.delete(ep)
    db.commit()


@management_router.post(
    "/endpoints/{endpoint_id}/keys", response_model=ApiKeyCreated, status_code=status.HTTP_201_CREATED
)
def create_key(
    endpoint_id: int,
    body: ApiKeyCreate,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Mint a new API key. The raw token is returned exactly once."""
    ep = _get_owned_endpoint(db, endpoint_id, current_user.id)
    raw_token, key_hash, key_prefix = generate_api_key()
    key = db_models.FlowApiKey(
        endpoint_id=ep.id,
        owner_id=current_user.id,
        name=body.name,
        key_hash=key_hash,
        key_prefix=key_prefix,
        expires_at=body.expires_at,
    )
    db.add(key)
    db.commit()
    db.refresh(key)
    return ApiKeyCreated(**_key_out(key).model_dump(), api_key=raw_token)


@management_router.get("/endpoints/{endpoint_id}/keys", response_model=list[ApiKeyOut])
def list_keys(
    endpoint_id: int,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    ep = _get_owned_endpoint(db, endpoint_id, current_user.id)
    keys = db.query(db_models.FlowApiKey).filter(db_models.FlowApiKey.endpoint_id == ep.id).all()
    return [_key_out(k) for k in keys]


@management_router.delete("/endpoints/{endpoint_id}/keys/{key_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_key(
    endpoint_id: int,
    key_id: int,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    ep = _get_owned_endpoint(db, endpoint_id, current_user.id)
    key = db.get(db_models.FlowApiKey, key_id)
    if key is None or key.endpoint_id != ep.id:
        raise HTTPException(status_code=404, detail="Key not found")
    db.delete(key)
    db.commit()
