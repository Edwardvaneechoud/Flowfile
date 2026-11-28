"""Delta Sharing over HTTP: the open protocol for recipients, and the routes that manage shares.

``protocol_router`` (``/delta-sharing``) is what Delta Sharing clients call with the bearer token of a profile
file; its errors are the protocol's ``{errorCode, message}`` bodies (``errors.SharingError``, answered by the
handler ``main.py`` installs), 401 included, since no renderer calls it. ``manage_router``
(``/delta-sharing/manage``) is the designer's: JWT, no trailing slashes, 403/404/409/422 and never 401.
"""

from __future__ import annotations

import re
from typing import Any

from fastapi import APIRouter, Body, Depends, Header, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse, Response
from sqlalchemy.orm import Session

from flowfile_core.auth.api_key import consumer_for_token
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db
from flowfile_core.delta_sharing import config, server
from flowfile_core.delta_sharing.errors import SharingError, not_found, unauthenticated
from flowfile_core.delta_sharing.service import DeltaSharingService, ShareConflict
from flowfile_core.schemas import delta_sharing_schema as schema

protocol_router = APIRouter(prefix=config.PROTOCOL_PREFIX, tags=["delta_sharing"])
manage_router = APIRouter(prefix=f"{config.PROTOCOL_PREFIX}/manage", tags=["delta_sharing"])

_CAPABILITIES = {"delta-sharing-capabilities": "responseformat=parquet"}
_NDJSON = "application/x-ndjson"


def sharing_error_response(_request: Request, exc: SharingError) -> JSONResponse:
    headers = {"WWW-Authenticate": "Bearer"} if exc.status == 401 else None
    return JSONResponse(
        status_code=exc.status, content={"errorCode": exc.code, "message": exc.message}, headers=headers
    )


_FORWARDED_HOST = re.compile(r"^[A-Za-z0-9.\-]+(:\d{1,5})?$|^\[[0-9A-Fa-f:.]+\](:\d{1,5})?$")
_FORWARDED_PREFIX = re.compile(r"^(/[A-Za-z0-9._~-]+)*$")


def protocol_base_url(request: Request) -> str:
    """Where this server's protocol is reached from outside.

    The configured endpoint when set; else the address the request came in at, as a reverse proxy in front of core
    reports it (``X-Forwarded-Proto``/``-Host``/``-Prefix``, each taken only when well-formed; nginx and the dev
    proxy send ``/api`` as the prefix), else the request's own base URL.
    """
    configured = config.configured_endpoint()
    if configured:
        return configured
    headers = request.headers
    host = headers.get("x-forwarded-host", "").split(",")[0].strip()
    proto = headers.get("x-forwarded-proto", "").split(",")[0].strip().lower()
    prefix = headers.get("x-forwarded-prefix", "").split(",")[0].strip().rstrip("/")
    if not _FORWARDED_PREFIX.match(prefix):
        prefix = ""
    if host and _FORWARDED_HOST.match(host):
        scheme = proto if proto in ("http", "https") else request.url.scheme
        base = f"{scheme}://{host}"
    else:
        base = str(request.base_url).rstrip("/")
    return f"{base}{prefix}{config.PROTOCOL_PREFIX}"


def _require_enabled() -> None:
    if not config.enabled():
        raise SharingError(404, "RESOURCE_DOES_NOT_EXIST", "Delta Sharing is turned off on this server.")


def recipient(
    authorization: str | None = Header(None),
    db: Session = Depends(get_db),
) -> db_models.ApiConsumer:
    """The API consumer the request's bearer token belongs to; any failure is a 401."""
    _require_enabled()
    scheme, _, token = (authorization or "").partition(" ")
    if scheme.lower() != "bearer":
        raise unauthenticated()
    consumer = consumer_for_token(db, token.strip())
    if consumer is None:
        raise unauthenticated()
    return consumer


def _json(body: dict[str, Any]) -> JSONResponse:
    return JSONResponse(content=body, headers=_CAPABILITIES)


def _ndjson(version: int, lines: list[dict]) -> Response:
    return Response(
        content=b"".join(server.ndjson(lines)),
        media_type=_NDJSON,
        headers={"Delta-Table-Version": str(version), **_CAPABILITIES},
    )


@protocol_router.get("/shares")
def list_shares(
    maxResults: int | None = Query(None),  # noqa: N803
    pageToken: str | None = Query(None),  # noqa: N803
    consumer: db_models.ApiConsumer = Depends(recipient),
    db: Session = Depends(get_db),
):
    return _json(server.list_shares(db, consumer, maxResults, pageToken))


@protocol_router.get("/shares/{share}")
def get_share(share: str, consumer: db_models.ApiConsumer = Depends(recipient), db: Session = Depends(get_db)):
    return _json(server.get_share(db, consumer, share))


@protocol_router.get("/shares/{share}/schemas")
def list_schemas(
    share: str,
    maxResults: int | None = Query(None),  # noqa: N803
    pageToken: str | None = Query(None),  # noqa: N803
    consumer: db_models.ApiConsumer = Depends(recipient),
    db: Session = Depends(get_db),
):
    return _json(server.list_schemas(db, consumer, share, maxResults, pageToken))


@protocol_router.get("/shares/{share}/schemas/{schema_name}/tables")
def list_tables(
    share: str,
    schema_name: str,
    maxResults: int | None = Query(None),  # noqa: N803
    pageToken: str | None = Query(None),  # noqa: N803
    consumer: db_models.ApiConsumer = Depends(recipient),
    db: Session = Depends(get_db),
):
    return _json(server.list_tables(db, consumer, share, schema_name, maxResults, pageToken))


@protocol_router.get("/shares/{share}/all-tables")
def list_all_tables(
    share: str,
    maxResults: int | None = Query(None),  # noqa: N803
    pageToken: str | None = Query(None),  # noqa: N803
    consumer: db_models.ApiConsumer = Depends(recipient),
    db: Session = Depends(get_db),
):
    return _json(server.list_all_tables(db, consumer, share, maxResults, pageToken))


@protocol_router.get("/shares/{share}/schemas/{schema_name}/tables/{table}/version")
@protocol_router.head(
    "/shares/{share}/schemas/{schema_name}/tables/{table}/version", operation_id="delta_sharing_table_version_head"
)
def table_version(
    share: str,
    schema_name: str,
    table: str,
    startingTimestamp: str | None = Query(None),  # noqa: N803
    consumer: db_models.ApiConsumer = Depends(recipient),
    db: Session = Depends(get_db),
):
    version = server.table_version(db, consumer, share, schema_name, table, startingTimestamp)
    return Response(status_code=200, headers={"Delta-Table-Version": str(version), **_CAPABILITIES})


@protocol_router.get("/shares/{share}/schemas/{schema_name}/tables/{table}/metadata")
def table_metadata(
    share: str,
    schema_name: str,
    table: str,
    consumer: db_models.ApiConsumer = Depends(recipient),
    db: Session = Depends(get_db),
):
    version, lines = server.metadata_lines(db, consumer, share, schema_name, table)
    return _ndjson(version, lines)


@protocol_router.post("/shares/{share}/schemas/{schema_name}/tables/{table}/query")
def query_table(
    request: Request,
    share: str,
    schema_name: str,
    table: str,
    body: dict[str, Any] | None = Body(None),
    consumer: db_models.ApiConsumer = Depends(recipient),
    db: Session = Depends(get_db),
):
    version, lines = server.query_lines(db, consumer, share, schema_name, table, body or {}, protocol_base_url(request))
    return _ndjson(version, lines)


@protocol_router.get("/files/{token}")
def download_file(token: str, db: Session = Depends(get_db)):
    """One data file of a table on local disk, for the signed URL a query handed out; the token is the grant."""
    _require_enabled()
    path = server.local_file_for(db, token)
    if path is None:
        raise not_found("File")
    return FileResponse(path, media_type="application/octet-stream")


def _service(db: Session = Depends(get_db), current_user=Depends(get_current_active_user)) -> DeltaSharingService:
    if not config.enabled():
        raise HTTPException(status_code=404, detail="Delta Sharing is turned off on this server.")
    return DeltaSharingService(db, current_user)


def _call(fn, *args):
    try:
        return fn(*args)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc).strip("'\"")) from exc
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ShareConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@manage_router.get("/status", response_model=schema.DeltaSharingStatus)
def status(_user=Depends(get_current_active_user)):
    return schema.DeltaSharingStatus(enabled=config.enabled(), endpoint=config.configured_endpoint())


@manage_router.get("/shares", response_model=list[schema.DeltaShareOut])
def manage_list_shares(svc: DeltaSharingService = Depends(_service)):
    return svc.list_shares()


@manage_router.post("/shares", response_model=schema.DeltaShareOut, status_code=201)
def manage_create_share(body: schema.DeltaShareCreate, svc: DeltaSharingService = Depends(_service)):
    return _call(svc.create_share, body)


@manage_router.get("/shares/{share_id}", response_model=schema.DeltaShareOut)
def manage_get_share(share_id: int, svc: DeltaSharingService = Depends(_service)):
    return _call(svc.get_share, share_id)


@manage_router.patch("/shares/{share_id}", response_model=schema.DeltaShareOut)
def manage_update_share(share_id: int, body: schema.DeltaShareUpdate, svc: DeltaSharingService = Depends(_service)):
    return _call(svc.update_share, share_id, body)


@manage_router.delete("/shares/{share_id}", status_code=204)
def manage_delete_share(share_id: int, svc: DeltaSharingService = Depends(_service)):
    _call(svc.delete_share, share_id)
    return Response(status_code=204)


@manage_router.post("/shares/{share_id}/tables", response_model=schema.DeltaShareOut)
def manage_add_table(share_id: int, body: schema.DeltaShareTableAdd, svc: DeltaSharingService = Depends(_service)):
    return _call(svc.add_table, share_id, body)


@manage_router.patch("/shares/{share_id}/tables/{entry_id}", response_model=schema.DeltaShareOut)
def manage_update_table(
    share_id: int, entry_id: int, body: schema.DeltaShareTableUpdate, svc: DeltaSharingService = Depends(_service)
):
    return _call(svc.update_table, share_id, entry_id, body)


@manage_router.delete("/shares/{share_id}/tables/{entry_id}", response_model=schema.DeltaShareOut)
def manage_remove_table(share_id: int, entry_id: int, svc: DeltaSharingService = Depends(_service)):
    return _call(svc.remove_table, share_id, entry_id)


@manage_router.post("/shares/{share_id}/recipients", response_model=schema.DeltaShareOut)
def manage_grant(share_id: int, body: schema.DeltaShareGrant, svc: DeltaSharingService = Depends(_service)):
    return _call(svc.grant, share_id, body.consumer_id)


@manage_router.delete("/shares/{share_id}/recipients/{consumer_id}", response_model=schema.DeltaShareOut)
def manage_revoke(share_id: int, consumer_id: int, svc: DeltaSharingService = Depends(_service)):
    return _call(svc.revoke, share_id, consumer_id)


@manage_router.post("/shares/{share_id}/profile", response_model=schema.DeltaShareProfileOut)
def manage_create_profile(
    request: Request,
    share_id: int,
    body: schema.DeltaShareProfileCreate,
    svc: DeltaSharingService = Depends(_service),
):
    return _call(svc.create_profile, share_id, body, protocol_base_url(request))


@manage_router.get("/eligible-tables", response_model=list[schema.DeltaShareEligibleTableOut])
def manage_eligible_tables(svc: DeltaSharingService = Depends(_service)):
    return svc.eligible_tables()


@manage_router.get("/consumers/{consumer_id}/shares", response_model=list[schema.DeltaShareOut])
def manage_consumer_shares(consumer_id: int, svc: DeltaSharingService = Depends(_service)):
    return _call(svc.shares_of_consumer, consumer_id)
