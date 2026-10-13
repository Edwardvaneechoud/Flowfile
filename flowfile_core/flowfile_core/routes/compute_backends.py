"""Admin routes over core's compute backends table: the workers core reaches by id, beside its built-in backends.

Mounted at ``/compute_backends`` in :mod:`flowfile_core.main`, admin-only like the ``/system`` routes. A row is one
configured worker: its id ``worker:<name>``, its base URL and its bearer token, stored as secret-manager ciphertext and
never answered. ``local`` and ``worker`` are built in and never rows. A self-hoster fills the table through the
settings page, a deployment's control plane over these routes; core manages no worker itself, and how one is hosted,
registered or scaled is the control plane's. The registry makes a ``WorkerBackend`` from a row on the first lookup of
its id (``registry.backend``), and removing the row forgets that instance.

The handshake reads the worker's capabilities with the row's token and compares them with this process's, field by
field, each difference naming both sides (``handshake.capability_differences``). The verdict is the answer, so a worker
that cannot be reached, refuses the protocol version or refuses the token is a 200 verdict with ``ok`` false and the
error, not a 503 as from the ``/system/worker_pool`` proxy, which forwards a call.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Response, status
from pydantic import BaseModel, Field, SecretStr, field_validator
from sqlalchemy.orm import Session

from flowfile_core.auth.jwt import get_current_admin_user
from flowfile_core.auth.models import User
from flowfile_core.database.connection import get_db
from flowfile_core.database.models import ConfiguredBackend
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.errors import ComputeError
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.handshake import capability_differences
from flowfile_core.secret_manager.secret_manager import encrypt_secret
from shared.compute_protocol import Capabilities, local_capabilities

router = APIRouter(prefix="/compute_backends", dependencies=[Depends(get_current_admin_user)])


class ComputeBackendInput(BaseModel):
    """Request body for ``POST /compute_backends``: the worker's name, its base URL and its bearer token."""

    name: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$", max_length=64)
    url: str
    token: SecretStr = Field(min_length=1)

    @field_validator("url")
    @classmethod
    def _http_url(cls, url: str) -> str:
        if not url.startswith(("http://", "https://")):
            raise ValueError("the worker URL must start with http:// or https://")
        return url.rstrip("/")


class ComputeBackendOut(BaseModel):
    """One configured worker as the routes answer it: its id and URL, never its token."""

    id: str
    url: str


class HandshakeVerdict(BaseModel):
    """Whether core can use a configured worker: both sides' capabilities and every field where they differ.

    ``worker`` is ``None`` and ``error`` says why when the worker's capabilities could not be read.
    """

    id: str
    ok: bool
    core: Capabilities
    worker: Capabilities | None
    differences: list[str]
    error: str | None


def _configured(db: Session, backend_id: str) -> ConfiguredBackend:
    row = db.get(ConfiguredBackend, backend_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"No compute backend {backend_id!r} is configured")
    return row


@router.get("", response_model=list[ComputeBackendOut])
def list_compute_backends(db: Session = Depends(get_db)) -> list[ComputeBackendOut]:
    """The configured workers, ordered by id."""
    rows = db.query(ConfiguredBackend).order_by(ConfiguredBackend.id).all()
    return [ComputeBackendOut(id=row.id, url=row.url) for row in rows]


@router.post("", response_model=ComputeBackendOut, status_code=status.HTTP_201_CREATED)
def add_compute_backend(
    body: ComputeBackendInput,
    current_user: User = Depends(get_current_admin_user),
    db: Session = Depends(get_db),
) -> ComputeBackendOut:
    """Configure the worker ``worker:<name>``, its token encrypted for the admin who adds it; 409 when the id exists."""
    backend_id = registry.CONFIGURED_WORKER_PREFIX + body.name
    if db.get(ConfiguredBackend, backend_id) is not None:
        raise HTTPException(status_code=409, detail=f"Compute backend {backend_id!r} already exists")
    token = encrypt_secret(body.token.get_secret_value(), current_user.id)
    db.add(ConfiguredBackend(id=backend_id, url=body.url, token=token))
    db.commit()
    return ComputeBackendOut(id=backend_id, url=body.url)


@router.delete("/{backend_id}", status_code=status.HTTP_204_NO_CONTENT)
def remove_compute_backend(backend_id: str, db: Session = Depends(get_db)) -> Response:
    """Remove a configured worker and forget the registry's instance of it; 404 when no row has the id."""
    db.delete(_configured(db, backend_id))
    db.commit()
    registry.forget(backend_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/{backend_id}/handshake", response_model=HandshakeVerdict)
def handshake_compute_backend(backend_id: str, db: Session = Depends(get_db)) -> HandshakeVerdict:
    """Read the worker's capabilities with its own token and compare them with core's; 404 when no row has the id."""
    _configured(db, backend_id)
    core = local_capabilities()
    try:
        worker = registry.backend(backend_id).capabilities()
    except ComputeError as exc:
        return HandshakeVerdict(id=backend_id, ok=False, core=core, worker=None, differences=[], error=str(exc))
    differences = capability_differences(core, worker)
    return HandshakeVerdict(
        id=backend_id, ok=not differences, core=core, worker=worker, differences=differences, error=None
    )
