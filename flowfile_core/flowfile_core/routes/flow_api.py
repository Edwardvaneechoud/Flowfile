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
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from flowfile_core.auth.api_key import generate_api_key, verify_api_key
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.configs import logger
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db
from flowfile_core.flowfile import api_consumer_manager
from flowfile_core.flowfile.api_runner import (
    ApiConfigError,
    ApiExecutionError,
    ApiParamError,
    _effective_specs,
    run_flow_as_api,
)
from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.schemas.flow_api_schema import (
    ApiConsumerOut,
    ApiEndpointCreate,
    ApiEndpointOut,
    ApiEndpointUpdate,
    ApiKeyCreate,
    ApiKeyCreated,
    ApiKeyOut,
    ApiKeyUpdate,
    ApiParamSpec,
    ApiTestRequest,
    FlowParamInfo,
    PublishableFlow,
)

_API_RUN_TIMEOUT = float(os.environ.get("FLOWFILE_API_RUN_TIMEOUT_SECONDS", "120"))

# Global cap on concurrent public API runs. Each request executes a full graph
# synchronously on a worker thread, so an unbounded fan-in could exhaust the
# threadpool and memory. Requests beyond the cap get a fast 503 instead of
# queueing. (Per-flow serialization is handled separately, in the runner.)
_API_MAX_CONCURRENT_RUNS = int(os.environ.get("FLOWFILE_API_MAX_CONCURRENT_RUNS", "4"))
_API_RUN_SEMAPHORE = asyncio.Semaphore(_API_MAX_CONCURRENT_RUNS)

data_router = APIRouter(tags=["flow_api"])
management_router = APIRouter(prefix="/flow-api", tags=["flow_api"])


# Helpers


def _parse_param_schema(raw: str | None) -> list[ApiParamSpec]:
    if not raw:
        return []
    return [ApiParamSpec(**spec) for spec in json.loads(raw)]


def _dump_param_schema(specs: list[ApiParamSpec]) -> str:
    return json.dumps([spec.model_dump() for spec in specs])


def _endpoint_out(
    ep: db_models.FlowApiEndpoint,
    registration: db_models.FlowRegistration | None,
    parameters: list[ApiParamSpec] | None = None,
) -> ApiEndpointOut:
    """Serialize an endpoint for the API.

    ``registration`` supplies ``flow_name`` (pass the looked-up row, or None if it
    is gone); callers fetch it so a list response can batch the lookup. When
    ``parameters`` is omitted the stored ``param_schema_json`` is used verbatim;
    pass an explicit list to advertise the runtime-effective specs instead.
    """
    return ApiEndpointOut(
        id=ep.id,
        registration_id=ep.registration_id,
        owner_id=ep.owner_id,
        slug=ep.slug,
        enabled=ep.enabled,
        parameters=parameters if parameters is not None else _parse_param_schema(ep.param_schema_json),
        path=f"/api/data/{ep.slug}",
        flow_name=registration.name if registration else None,
        created_at=ep.created_at,
        updated_at=ep.updated_at,
    )


def _key_out(key: db_models.FlowApiKey) -> ApiKeyOut:
    return ApiKeyOut(
        id=key.id,
        consumer_id=key.consumer_id,
        endpoint_id=key.endpoint_id,
        name=key.name,
        key_prefix=key.key_prefix,
        enabled=key.enabled,
        last_used_at=key.last_used_at,
        expires_at=key.expires_at,
        created_at=key.created_at,
    )


def _consumer_out(db: Session, consumer: db_models.ApiConsumer) -> ApiConsumerOut:
    return ApiConsumerOut(
        id=consumer.id,
        name=consumer.name,
        description=consumer.description,
        owner_id=consumer.owner_id,
        enabled=consumer.enabled,
        is_implicit=consumer.is_implicit,
        endpoint_count=api_consumer_manager.count_endpoints(db, consumer.id),
        key_count=api_consumer_manager.count_keys(db, consumer.id),
        created_at=consumer.created_at,
        updated_at=consumer.updated_at,
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


# Public data endpoint


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

    # Reject fast with 503 when every run slot is busy rather than queueing
    # unbounded synchronous graph runs onto the threadpool. Checked-then-acquired
    # without an await in between, so the acquire never blocks (single event loop).
    if _API_RUN_SEMAPHORE.locked():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Server is busy handling other API requests; please retry shortly.",
        )

    def _run():
        return run_flow_as_api(flow_path, owner_id, specs, query)

    await _API_RUN_SEMAPHORE.acquire()
    try:
        return await asyncio.wait_for(anyio.to_thread.run_sync(_run), timeout=_API_RUN_TIMEOUT)
    except ApiParamError as exc:
        # Parameter errors describe the caller's own input and are safe to return.
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except (ApiConfigError, ApiExecutionError) as exc:
        # Don't leak node-level detail (file paths, SQL, column names) to the public
        # caller; log it server-side for the owner/operator to inspect instead.
        logger.error("Published flow '%s' failed: %s", slug, exc)
        raise HTTPException(status_code=500, detail="Flow execution failed") from exc
    except asyncio.TimeoutError as exc:
        raise HTTPException(status_code=504, detail="Flow execution timed out") from exc
    finally:
        _API_RUN_SEMAPHORE.release()


# Management endpoints (JWT)


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

    # Validate the flow has exactly one api_response node (raises 400 otherwise).
    # The node id is intentionally not persisted: the runner re-scans the flow at
    # request time, so a stored id could only go stale against an edited flow.
    _resolve_api_node_id(registration.flow_path, current_user.id)
    ep = db_models.FlowApiEndpoint(
        registration_id=body.registration_id,
        owner_id=current_user.id,
        slug=body.slug,
        enabled=body.enabled,
        param_schema_json=_dump_param_schema(body.parameters),
    )
    db.add(ep)
    # The pre-checks above aren't atomic with the UNIQUE constraints (slug,
    # registration_id); a concurrent publisher can still win the race to commit.
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status_code=409, detail="Flow is already published or the slug is in use") from exc
    db.refresh(ep)
    return _endpoint_out(ep, registration)


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
    endpoints = query.all()
    # Batch-fetch the registrations (for flow_name) in one query to avoid N+1.
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


@management_router.get("/publishable-flows", response_model=list[PublishableFlow])
def list_publishable_flows(
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """The current user's API-compatible flows that have no endpoint yet.

    Powers the APIs tab's "Create API" picker: a flow qualifies when it has exactly
    one ``api_response`` node (``is_api_compatible``) and isn't already published.
    """
    published = db.query(db_models.FlowApiEndpoint.registration_id)
    regs = (
        db.query(db_models.FlowRegistration)
        .filter(
            db_models.FlowRegistration.owner_id == current_user.id,
            db_models.FlowRegistration.is_api_compatible.is_(True),
            ~db_models.FlowRegistration.id.in_(published),
        )
        .order_by(db_models.FlowRegistration.name)
        .all()
    )
    return [
        PublishableFlow(
            registration_id=r.id,
            name=r.name,
            file_exists=bool(r.flow_path) and Path(r.flow_path).exists(),
        )
        for r in regs
    ]


@management_router.get("/flows/{registration_id}/parameters", response_model=list[FlowParamInfo])
def get_flow_parameters(
    registration_id: int,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Return the flow's ${name} parameters so the UI can pre-fill query params.

    Best-effort: returns an empty list if the flow file is missing or can't be opened.
    """
    registration = db.get(db_models.FlowRegistration, registration_id)
    if registration is None or registration.owner_id != current_user.id:
        raise HTTPException(status_code=404, detail="Flow not found")
    if not registration.flow_path:
        return []
    try:
        flow = open_flow(Path(registration.flow_path), user_id=current_user.id)
    except Exception:  # noqa: BLE001 - convenience lookup, degrade gracefully
        return []
    return [FlowParamInfo(name=p.name, default=p.default_value or "") for p in flow.flow_settings.parameters]


@management_router.get("/endpoints/{endpoint_id}", response_model=ApiEndpointOut)
def get_endpoint(
    endpoint_id: int,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    ep = _get_owned_endpoint(db, endpoint_id, current_user.id)
    registration = db.get(db_models.FlowRegistration, ep.registration_id)
    # Advertise the parameters the runner will actually enforce: the flow's own
    # ${name} params refined by the stored type overrides (api_runner._effective_specs).
    # Overrides for params no longer in the flow are dropped, matching runtime
    # behavior. Falls back to the stored specs if the flow file can't be opened.
    parameters = _parse_param_schema(ep.param_schema_json)
    if registration and registration.flow_path:
        try:
            flow = open_flow(Path(registration.flow_path), user_id=current_user.id)
            parameters = _effective_specs(flow, parameters)
        except Exception:  # noqa: BLE001 - degrade to stored specs on load failure
            pass
    return _endpoint_out(ep, registration, parameters=parameters)


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
    # The slug pre-check above isn't atomic with the UNIQUE constraint; surface a
    # racing committer as 409 rather than a raw 500.
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status_code=409, detail="Slug already in use") from exc
    db.refresh(ep)
    registration = db.get(db_models.FlowRegistration, ep.registration_id)
    return _endpoint_out(ep, registration)


@management_router.post("/endpoints/{endpoint_id}/test")
async def test_endpoint(
    endpoint_id: int,
    body: ApiTestRequest | None = None,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Owner-only 'try it': run the published flow with the given params and return the JSON.

    Runs as the endpoint owner without requiring an API key, so the owner can test
    the endpoint from the catalog UI. Mirrors the public endpoint's execution and
    timeout behavior; unlike the public route it returns verbose error detail, since
    the caller is the authenticated owner.
    """
    ep = _get_owned_endpoint(db, endpoint_id, current_user.id)
    registration = db.get(db_models.FlowRegistration, ep.registration_id)
    if registration is None or not registration.flow_path:
        raise HTTPException(status_code=404, detail="Flow not found")
    specs = _parse_param_schema(ep.param_schema_json)
    params = dict(body.params) if body else {}
    flow_path = registration.flow_path
    owner_id = ep.owner_id

    def _run():
        return run_flow_as_api(flow_path, owner_id, specs, params)

    try:
        return await asyncio.wait_for(anyio.to_thread.run_sync(_run), timeout=_API_RUN_TIMEOUT)
    except ApiParamError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except (ApiConfigError, ApiExecutionError) as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except asyncio.TimeoutError as exc:
        raise HTTPException(status_code=504, detail="Flow execution timed out") from exc


@management_router.delete("/endpoints/{endpoint_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_endpoint(
    endpoint_id: int,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    ep = _get_owned_endpoint(db, endpoint_id, current_user.id)
    # Implicit (per-endpoint) consumers granted only to this endpoint are garbage-
    # collected with it; shared consumers just lose this one grant. SQLite FK is off,
    # so delete grants/keys explicitly rather than relying on a cascade.
    granted = api_consumer_manager.list_consumers_for_endpoint(db, ep.id, include_implicit=True)
    implicit_ids = [c.id for c in granted if c.is_implicit]
    db.query(db_models.FlowApiKey).filter(db_models.FlowApiKey.endpoint_id == ep.id).delete(synchronize_session=False)
    db.query(db_models.ApiConsumerEndpoint).filter(db_models.ApiConsumerEndpoint.endpoint_id == ep.id).delete(
        synchronize_session=False
    )
    if implicit_ids:
        db.query(db_models.FlowApiKey).filter(db_models.FlowApiKey.consumer_id.in_(implicit_ids)).delete(
            synchronize_session=False
        )
        db.query(db_models.ApiConsumer).filter(db_models.ApiConsumer.id.in_(implicit_ids)).delete(
            synchronize_session=False
        )
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
    """Mint a new API key for this one endpoint. The raw token is returned exactly once.

    The key is attached to an implicit, single-endpoint consumer so it authenticates
    through the same consumer + grant path as keys created from the API Access panel.
    For access spanning multiple flows, create a consumer there instead.
    """
    ep = _get_owned_endpoint(db, endpoint_id, current_user.id)
    consumer = api_consumer_manager.get_or_create_implicit_consumer(db, ep)
    raw_token, key_hash, key_prefix = generate_api_key()
    key = db_models.FlowApiKey(
        consumer_id=consumer.id,
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


@management_router.patch("/endpoints/{endpoint_id}/keys/{key_id}", response_model=ApiKeyOut)
def update_key(
    endpoint_id: int,
    key_id: int,
    body: ApiKeyUpdate,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Rename or enable/disable an API key in place (revoke without deleting it)."""
    ep = _get_owned_endpoint(db, endpoint_id, current_user.id)
    key = db.get(db_models.FlowApiKey, key_id)
    if key is None or key.endpoint_id != ep.id:
        raise HTTPException(status_code=404, detail="Key not found")
    if body.name is not None:
        key.name = body.name
    if body.enabled is not None:
        key.enabled = body.enabled
    db.commit()
    db.refresh(key)
    return _key_out(key)


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


@management_router.get("/endpoints/{endpoint_id}/consumers", response_model=list[ApiConsumerOut])
def list_endpoint_consumers(
    endpoint_id: int,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """List the (non-implicit) API consumers granted access to this endpoint.

    Powers the flow API panel's read-only "Access" section; manage grants from the
    API Access panel. Implicit per-flow consumers are hidden — their keys show up in
    this endpoint's own key list instead.
    """
    ep = _get_owned_endpoint(db, endpoint_id, current_user.id)
    return [_consumer_out(db, c) for c in api_consumer_manager.list_consumers_for_endpoint(db, ep.id)]
