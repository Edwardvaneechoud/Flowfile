"""Routes over core's ``compute_backends`` table: the configured workers core reaches by id.

Mounted at ``/compute_backends`` in :mod:`flowfile_core.main`. Every user may list the configured ids, each with its
backend's last handshake verdict in this process (``handshake.last_verdict``, ``None`` until checked). Only an
administrator gets the URL and the whole verdict; anyone else gets the verdict without host details
(``_without_addresses``). Adding, removing and the handshake are admin-only.

A row is one worker: its id ``worker:<name>``, its base URL, its bearer token (secret-manager ciphertext, never
answered), an optional CA certificate PEM for a worker whose TLS certificate is not public, and ``shares_core_disk``
(the worker sees core's files, so core offers it ``file``). ``local`` and ``worker`` are built in and never rows.
Core manages no worker itself; the Backends page or a deployment's control plane fills the table. The registry makes
a ``WorkerBackend`` from a row on its id's first lookup.

The add route checks the token is a header value itself (``worker.is_header_value``), because a model validator's
422 would echo the input. A CA certificate ``ssl`` cannot load as PEM, or one given with an ``http://`` URL, is a
422 too; an empty one is no CA.

The handshake answers its verdict as a 200 even when it refuses (unreachable, other version, refused token, a token
that no longer decrypts): ``ok`` false with the error, not a 503.
"""

from __future__ import annotations

import ssl

from fastapi import APIRouter, Depends, HTTPException, Response, status
from pydantic import BaseModel, Field, SecretStr, field_validator
from sqlalchemy.orm import Session

from flowfile_core.auth.jwt import get_current_active_user, get_current_admin_user
from flowfile_core.auth.models import User
from flowfile_core.database.connection import get_db
from flowfile_core.database.models import ConfiguredBackend
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends.worker import TOKEN_RULE, is_header_value
from flowfile_core.flowfile.execution.errors import ComputeError
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake
from flowfile_core.secret_manager.secret_manager import encrypt_secret
from shared.compute_protocol import Capabilities, local_capabilities

router = APIRouter(prefix="/compute_backends", dependencies=[Depends(get_current_active_user)])


class ComputeBackendInput(BaseModel):
    """Request body for ``POST /compute_backends``: the worker's name, its base URL, its bearer token, optionally the
    PEM of the CA to trust for its certificate, and whether it sees core's files."""

    name: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$", max_length=64)
    url: str
    token: SecretStr = Field(min_length=1)
    ca_cert: str | None = None
    shares_core_disk: bool = False

    @field_validator("url")
    @classmethod
    def _http_url(cls, url: str) -> str:
        if not url.startswith(("http://", "https://")):
            raise ValueError("the worker URL must start with http:// or https://")
        return url.rstrip("/")


class ComputeBackendOut(BaseModel):
    """One configured worker as the routes answer it, never its token: its URL is ``None`` to a non-administrator,
    and its verdict is the last handshake verdict in this process (``None`` until it was checked)."""

    id: str
    url: str | None
    shares_core_disk: bool = False
    verdict: handshake.Verdict | None = None


_REFUSAL_REASONS = {"ComputeUnavailable": "unreachable", "VersionMismatch": "version mismatch"}


def _without_host(capabilities: Capabilities | None) -> Capabilities | None:
    """*capabilities* without the fields that describe a host: its ``platform`` and its plugins' library paths."""
    if capabilities is None:
        return None
    return capabilities.model_copy(update={"platform": "", "plugin_libs": {}})


def _without_addresses(verdict: handshake.Verdict | None) -> handshake.Verdict | None:
    """*verdict* as a non-administrator sees it: what the picker shows, nothing that can name a host.

    The error becomes a reason word for its kind (``unreachable``, ``version mismatch``, else ``refused``), since its
    text names the worker's URL. Each absence keeps only the words before its parenthesised detail, and ``platform``
    and plugin library paths are dropped. The version differences stay: they name versions only.
    """
    if verdict is None:
        return None
    return verdict.model_copy(
        update={
            "core": _without_host(verdict.core),
            "backend": _without_host(verdict.backend),
            "offered": _without_host(verdict.offered),
            "absent": {name: reason.split(" (", 1)[0] for name, reason in verdict.absent.items()},
            "error": None if verdict.error is None else _REFUSAL_REASONS.get(verdict.error_kind, "refused"),
        }
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
    """The configured workers, ordered by id, each with its last verdict; the URLs, and the verdicts whole, to an
    administrator only (``_without_addresses``)."""
    rows = db.query(ConfiguredBackend).order_by(ConfiguredBackend.id).all()
    return [
        ComputeBackendOut(
            id=row.id,
            url=row.url if current_user.is_admin else None,
            shares_core_disk=bool(row.shares_core_disk),
            verdict=(
                handshake.last_verdict(row.id)
                if current_user.is_admin
                else _without_addresses(handshake.last_verdict(row.id))
            ),
        )
        for row in rows
    ]


@router.post("", response_model=ComputeBackendOut, status_code=status.HTTP_201_CREATED)
def add_compute_backend(
    body: ComputeBackendInput,
    current_user: User = Depends(get_current_admin_user),
    db: Session = Depends(get_db),
) -> ComputeBackendOut:
    """Configure the worker ``worker:<name>``, its token encrypted for the admin who adds it; 409 when the id exists.

    A 422 stores nothing: for a token that is no header value (the detail states ``TOKEN_RULE``), a CA certificate that
    is not PEM, or a CA certificate with an ``http://`` URL.
    """
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
        except (ssl.SSLError, TypeError, ValueError):
            raise HTTPException(
                status_code=422,
                detail="The CA certificate was not stored: it must be PEM, -----BEGIN CERTIFICATE----- blocks",
            ) from None
    backend_id = registry.CONFIGURED_WORKER_PREFIX + body.name
    if db.get(ConfiguredBackend, backend_id) is not None:
        raise HTTPException(status_code=409, detail=f"Compute backend {backend_id!r} already exists")
    token = encrypt_secret(body.token.get_secret_value(), current_user.id)
    db.add(
        ConfiguredBackend(
            id=backend_id, url=body.url, token=token, ca_cert=ca_cert, shares_core_disk=body.shares_core_disk
        )
    )
    db.commit()
    return ComputeBackendOut(id=backend_id, url=body.url, shares_core_disk=body.shares_core_disk)


@router.delete("/{backend_id}", status_code=status.HTTP_204_NO_CONTENT, dependencies=[Depends(get_current_admin_user)])
def remove_compute_backend(backend_id: str, db: Session = Depends(get_db)) -> Response:
    """Remove a configured worker, forget the registry's instance of it and the verdicts of the backends on its URL;
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
    """Check the worker now, the probe plans included, and answer the verdict; 404 when no row has the id.

    A ``ComputeError`` while checking answers the verdict the check kept. A row the registry cannot make a backend of
    (``LookupError``: its token no longer decrypts) answers a refused verdict that is not kept, as there is no backend.
    """
    _configured(db, backend_id)
    try:
        backend = registry.backend(backend_id)
    except LookupError as e:
        return handshake.Verdict(
            id=backend_id, ok=False, core=local_capabilities(), error=str(e), error_kind=type(e).__name__
        )
    try:
        return handshake.verify(backend)
    except ComputeError:
        return handshake.last_verdict(backend_id)
