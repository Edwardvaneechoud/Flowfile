"""Routes over core's compute backends table: the workers and Polars clusters core reaches by id, beside its built-in
backends.

Mounted at ``/compute_backends`` in :mod:`flowfile_core.main`. Every user may list the configured backends' ids and
kinds, each with the last handshake verdict of its backend in this process (``handshake.last_verdict``, ``None`` until
it was checked), which the node picker and the Flow Settings dialog show beside the id; the URL is answered to an
administrator only, and adding, removing and the handshake are admin-only like the ``/system`` routes. A row is one
configured backend of a kind (``registry.CONFIGURED_PREFIXES``):

- ``worker``, the default kind: its id ``worker:<name>``, its base URL, its bearer token, stored as secret-manager
  ciphertext and never answered, and, for a worker whose TLS certificate is not public, the PEM of the CA core trusts
  for it.
- ``polars-cloud``, a Polars cluster the operator runs: its id ``polars-cloud:<name>``, its scheduler's ``http(s)://``
  URL (answered as the row's ``url``) and, optionally, its dashboard's URL, both kept in the row's JSON settings, which
  are what the backend is made from. No token
  and no secret: a self-run Polars scheduler has no client authentication, and plans carry user code, so each address
  must resolve to loopback or a private range (``polars_cloud.address_refusal``), TLS or not; another is a 422 that says
  why. Adding the row connects to nothing.

``local`` and ``worker`` are built in and never rows. A self-hoster fills the table through the settings page, a
deployment's control plane over these routes; core manages no backend itself, and how one is hosted, registered or
scaled is the control plane's. The registry makes the backend from a row by its kind on the first lookup of its id
(``registry.backend``), and removing the row forgets that instance and the verdicts of the backends on its URL. An
unknown kind, a field the kind does not take, or a missing one is a 422.

The token travels as an HTTP header value, so the add route refuses one that is no header value
(``worker.is_header_value``) with a 422 whose detail states the rule and never the token. The route checks it itself:
a validator on the request model would fail with an error that echoes the field's input. A CA certificate the ``ssl``
module cannot load as PEM, or one given with an ``http://`` URL, is a 422 too, and an empty one is no CA.

The handshake checks the backend now as its first use would (``handshake.verify``): a worker's capabilities read with
the row's token, a Polars cluster's the constants of its image, compared with this process's, then the probe plans run
on it, and the verdict is kept as its last. The verdict is the answer, so a backend that cannot be reached, refuses the
protocol version or refuses the token is a 200 verdict with ``ok`` false and the error, not a 503 as from the
``/system/worker_pool`` proxy, which forwards a call.
"""

from __future__ import annotations

import json
import ssl

from fastapi import APIRouter, Depends, HTTPException, Response, status
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, Field, SecretStr, field_validator
from sqlalchemy.orm import Session

from flowfile_core.auth.jwt import get_current_active_user, get_current_admin_user
from flowfile_core.auth.models import User
from flowfile_core.database.connection import get_db
from flowfile_core.database.models import ConfiguredBackend
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends.polars_cloud import address_refusal
from flowfile_core.flowfile.execution.backends.worker import TOKEN_RULE, is_header_value
from flowfile_core.flowfile.execution.errors import ComputeError
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake
from flowfile_core.secret_manager.secret_manager import encrypt_secret

router = APIRouter(prefix="/compute_backends", dependencies=[Depends(get_current_active_user)])


_KIND_FIELDS = {
    registry.WORKER_KIND: (("url", "token"), ("ca_cert",)),
    registry.POLARS_CLOUD_KIND: (("scheduler_url",), ("dashboard_url",)),
}
"""Backend kind -> the fields its body requires and the ones it may carry, beside ``kind`` and ``name``."""


class ComputeBackendInput(BaseModel):
    """Request body for ``POST /compute_backends``: the backend's kind (``worker`` unless given), its name and the
    kind's fields.

    A worker takes its base URL, its bearer token and, optionally, the PEM of the CA to trust for its certificate; a
    Polars cluster its scheduler's URL and, optionally, its dashboard's. Which fields a kind requires and allows is
    the route's check (``_KIND_FIELDS``), so a worker body is validated as it was before kinds existed.
    """

    kind: str = registry.WORKER_KIND
    name: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$", max_length=64)
    url: str | None = None
    token: SecretStr | None = Field(default=None, min_length=1)
    ca_cert: str | None = None
    scheduler_url: str | None = None
    dashboard_url: str | None = None

    @field_validator("url", "scheduler_url", "dashboard_url")
    @classmethod
    def _http_url(cls, url: str | None, info) -> str | None:
        if url is None:
            return None
        if not url.startswith(("http://", "https://")):
            what = {"url": "worker URL", "scheduler_url": "scheduler URL", "dashboard_url": "dashboard URL"}
            raise ValueError(f"the {what[info.field_name]} must start with http:// or https://")
        return url.rstrip("/")


class ComputeBackendOut(BaseModel):
    """One configured backend as the routes answer it: its id, its kind, its URL (a Polars cluster's scheduler; ``None``
    to a user who is no administrator) and the last verdict of its handshake in this process (``None`` until it was
    checked), never its token."""

    id: str
    kind: str = registry.WORKER_KIND
    url: str | None
    verdict: handshake.Verdict | None = None


def _check_kind_fields(body: ComputeBackendInput) -> None:
    """Refuse a body of an unknown kind, one missing a field its kind requires or one carrying another kind's field.

    A missing field is answered as the request model would answer it, FastAPI's 422 list; the others as a string detail.
    """
    if body.kind not in _KIND_FIELDS:
        kinds = " or ".join(sorted(_KIND_FIELDS))
        raise HTTPException(status_code=422, detail=f"Unknown backend kind {body.kind!r}: the kinds are {kinds}")
    required, optional = _KIND_FIELDS[body.kind]
    missing = [field for field in required if getattr(body, field) is None]
    if missing:
        raise RequestValidationError(
            [{"type": "missing", "loc": ("body", field), "msg": "Field required"} for field in missing]
        )
    allowed = {"kind", "name", *required, *optional}
    foreign = [field for field in type(body).model_fields if field not in allowed and getattr(body, field)]
    if foreign:
        raise HTTPException(
            status_code=422, detail=f"A {body.kind} backend does not take {', '.join(foreign)}; nothing was stored"
        )


def _configured(db: Session, backend_id: str) -> ConfiguredBackend:
    row = db.get(ConfiguredBackend, backend_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"No compute backend {backend_id!r} is configured")
    return row


@router.get("", response_model=list[ComputeBackendOut])
def list_compute_backends(
    current_user: User = Depends(get_current_active_user), db: Session = Depends(get_db)
) -> list[ComputeBackendOut]:
    """The configured backends, ordered by id, each with its kind and last verdict; the URLs to an administrator
    only."""
    rows = db.query(ConfiguredBackend).order_by(ConfiguredBackend.id).all()
    return [
        ComputeBackendOut(
            id=row.id,
            kind=row.kind,
            url=row.url if current_user.is_admin else None,
            verdict=handshake.last_verdict(row.id),
        )
        for row in rows
    ]


@router.post("", response_model=ComputeBackendOut, status_code=status.HTTP_201_CREATED)
def add_compute_backend(
    body: ComputeBackendInput,
    current_user: User = Depends(get_current_admin_user),
    db: Session = Depends(get_db),
) -> ComputeBackendOut:
    """Configure the backend ``<kind>:<name>``; 409 when the id exists.

    A worker's token is encrypted for the admin who adds it. A token that is no header value is a 422 stating
    ``TOKEN_RULE``, and nothing is stored; so is a CA certificate that is no PEM certificate or that comes with an
    ``http://`` URL. A Polars cluster's scheduler or dashboard address that is neither loopback nor private is a 422
    naming why, and nothing is stored; nothing is connected to either way.
    """
    _check_kind_fields(body)
    if body.kind == registry.POLARS_CLOUD_KIND:
        return _add_polars_cluster(body, db)
    if not is_header_value(body.token.get_secret_value()):
        raise HTTPException(status_code=422, detail=f"The token was not stored: {TOKEN_RULE}")
    ca_cert = body.ca_cert or None
    if ca_cert is not None:
        if not body.url.startswith("https://"):
            raise HTTPException(
                status_code=422, detail="The CA certificate was not stored: it applies to an https:// worker URL only"
            )
        try:
            ssl.create_default_context(cadata=ca_cert)
        except ssl.SSLError:
            raise HTTPException(
                status_code=422,
                detail="The CA certificate was not stored: it must be PEM, -----BEGIN CERTIFICATE----- blocks",
            ) from None
    backend_id = registry.CONFIGURED_WORKER_PREFIX + body.name
    if db.get(ConfiguredBackend, backend_id) is not None:
        raise HTTPException(status_code=409, detail=f"Compute backend {backend_id!r} already exists")
    token = encrypt_secret(body.token.get_secret_value(), current_user.id)
    db.add(ConfiguredBackend(id=backend_id, kind=registry.WORKER_KIND, url=body.url, token=token, ca_cert=ca_cert))
    db.commit()
    return ComputeBackendOut(id=backend_id, kind=registry.WORKER_KIND, url=body.url)


def _add_polars_cluster(body: ComputeBackendInput, db: Session) -> ComputeBackendOut:
    """Store the ``polars-cloud:<name>`` row: the scheduler URL as its ``url``, both URLs in its settings."""
    for label, url in (("scheduler", body.scheduler_url), ("dashboard", body.dashboard_url)):
        refusal = None if url is None else address_refusal(url)
        if refusal is not None:
            raise HTTPException(status_code=422, detail=f"The Polars {label} address was not stored: {refusal}")
    backend_id = registry.POLARS_CLOUD_PREFIX + body.name
    if db.get(ConfiguredBackend, backend_id) is not None:
        raise HTTPException(status_code=409, detail=f"Compute backend {backend_id!r} already exists")
    connection = {"scheduler_url": body.scheduler_url}
    if body.dashboard_url:
        connection["dashboard_url"] = body.dashboard_url
    row_settings = json.dumps(connection)
    db.add(
        ConfiguredBackend(id=backend_id, kind=registry.POLARS_CLOUD_KIND, url=body.scheduler_url, settings=row_settings)
    )
    db.commit()
    return ComputeBackendOut(id=backend_id, kind=registry.POLARS_CLOUD_KIND, url=body.scheduler_url)


@router.delete("/{backend_id}", status_code=status.HTTP_204_NO_CONTENT, dependencies=[Depends(get_current_admin_user)])
def remove_compute_backend(backend_id: str, db: Session = Depends(get_db)) -> Response:
    """Remove a configured backend, forget the registry's instance of it and the verdicts of the backends on its URL;
    404 when no row has the id."""
    row = _configured(db, backend_id)
    url = row.url
    db.delete(row)
    db.commit()
    registry.forget(backend_id)
    handshake.reset_handshake(url)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post(
    "/{backend_id}/handshake", response_model=handshake.Verdict, dependencies=[Depends(get_current_admin_user)]
)
def handshake_compute_backend(backend_id: str, db: Session = Depends(get_db)) -> handshake.Verdict:
    """Check the backend now, the probe plans included, and answer the verdict; 404 when no row has the id.

    A ``ComputeError`` while checking is the verdict the check kept: ``ok`` false, the message as ``error``.
    """
    _configured(db, backend_id)
    try:
        return handshake.verify(registry.backend(backend_id))
    except ComputeError:
        return handshake.last_verdict(backend_id)
