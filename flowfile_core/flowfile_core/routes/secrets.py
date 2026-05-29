"""
Manages CRUD (Create, Read, Update, Delete) operations for secrets.

This router provides secure endpoints for creating, retrieving, and deleting
sensitive credentials for the authenticated user. Secrets are encrypted before
being stored and are associated with the user's ID.

Every CRUD endpoint emits a row to ``secret_access_events`` so an operator can
later reconstruct who touched what, when, and from where — see
:mod:`flowfile_core.secret_manager.audit`. Failed attempts are recorded too,
which is the entire point of the audit trail.
"""

import os
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel
from sqlalchemy.orm import Session

from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.auth.models import Secret, SecretInput
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db
from flowfile_core.secret_manager import audit
from flowfile_core.secret_manager.secret_manager import delete_secret as delete_secret_action
from flowfile_core.secret_manager.secret_manager import store_secret

router = APIRouter(dependencies=[Depends(get_current_active_user)])


def _client_ip(request: Request) -> str | None:
    """Best-effort client IP extraction.

    ``X-Forwarded-For`` is honored only when ``FLOWFILE_TRUST_PROXY_HEADERS`` is
    set (the deployment sits behind a proxy that overwrites the header);
    otherwise the header is ignored and the direct connection IP is used, since
    a directly-exposed instance would let clients forge the recorded source IP.
    Returns ``None`` if nothing is available (e.g. in some test clients).
    """
    if os.environ.get("FLOWFILE_TRUST_PROXY_HEADERS", "").strip().lower() in ("1", "true", "yes", "on"):
        forwarded = request.headers.get("x-forwarded-for")
        if forwarded:
            return forwarded.split(",")[0].strip()
    client = request.client
    return client.host if client else None


class SecretAuditEntry(BaseModel):
    """Response shape for a single audit row.

    Mirrors the ORM columns minus the FK link — clients see the denormalized
    ``secret_name`` and ``secret_id`` directly without a join.
    """

    id: int
    user_id: int
    secret_id: int | None
    secret_name: str | None
    action: str
    result_status: str
    error: str | None
    source: str
    ip_address: str | None
    created_at: datetime


@router.get("/secrets", response_model=list[Secret])
async def get_secrets(
    request: Request,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Retrieves all secret names for the currently authenticated user.

    Note: This endpoint returns the secret names and metadata but does not
    expose the decrypted secret values.

    Args:
        request: The HTTP request (used to capture client IP for audit).
        current_user: The authenticated user object, injected by FastAPI.
        db: The database session, injected by FastAPI.

    Returns:
        A list of `Secret` objects, each containing the name and encrypted value.
    """
    user_id = current_user.id

    db_secrets = db.query(db_models.Secret).filter(db_models.Secret.user_id == user_id).all()
    secrets = [
        Secret(name=db_secret.name, value=db_secret.encrypted_value, user_id=str(db_secret.user_id))
        for db_secret in db_secrets
    ]

    audit.record_event(
        audit.SecretEvent(
            user_id=user_id,
            action="list",
            result_status="success",
            ip_address=_client_ip(request),
        ),
        db=db,
    )
    db.commit()

    return secrets


@router.post("/secrets", response_model=Secret)
async def create_secret(
    secret: SecretInput,
    request: Request,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> Secret:
    """Creates a new secret for the authenticated user.

    The secret value is encrypted before being stored in the database. A secret
    name must be unique for a given user.
    """
    user_id = 1 if os.environ.get("FLOWFILE_MODE") == "electron" else current_user.id
    ip = _client_ip(request)

    existing_secret = (
        db.query(db_models.Secret)
        .filter(db_models.Secret.user_id == user_id, db_models.Secret.name == secret.name)
        .first()
    )

    if existing_secret:
        audit.record_event(
            audit.SecretEvent(
                user_id=user_id,
                action="create",
                result_status="error",
                secret_name=secret.name,
                error="duplicate_name",
                ip_address=ip,
            ),
            db=db,
        )
        db.commit()
        raise HTTPException(status_code=400, detail="Secret with this name already exists")

    stored_secret = store_secret(db, secret, user_id)

    audit.record_event(
        audit.SecretEvent(
            user_id=user_id,
            action="create",
            result_status="success",
            secret_name=stored_secret.name,
            secret_id=stored_secret.id,
            ip_address=ip,
        ),
        db=db,
    )
    db.commit()

    return Secret(name=stored_secret.name, value=stored_secret.encrypted_value, user_id=str(user_id))


@router.get("/secrets/audit", response_model=list[SecretAuditEntry])
async def get_audit_log(
    request: Request,
    secret_name: str | None = Query(None, description="Filter by secret name (exact match)"),
    action: str | None = Query(None, description="Filter by action: create | list | read | delete"),
    limit: int = Query(100, ge=1, le=1000, description="Max rows returned (DESC by created_at)"),
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> list[SecretAuditEntry]:
    """Read the secret-access audit log.

    Non-admin users see only their own events; admins see every user's events
    (operators investigating an incident shouldn't have to swap accounts).
    """
    scope_user = None if current_user.is_admin else current_user.id

    rows = audit.query_events(
        user_id=scope_user,
        secret_name=secret_name,
        action=action,  # type: ignore[arg-type]
        limit=limit,
        db=db,
    )
    return [
        SecretAuditEntry(
            id=row.id,
            user_id=row.user_id,
            secret_id=row.secret_id,
            secret_name=row.secret_name,
            action=row.action,
            result_status=row.result_status,
            error=row.error,
            source=row.source,
            ip_address=row.ip_address,
            created_at=row.created_at,
        )
        for row in rows
    ]


@router.get("/secrets/{secret_name}", response_model=Secret)
async def get_secret(
    secret_name: str,
    request: Request,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> Secret:
    """Retrieves a specific secret by name for the authenticated user.

    Note: This endpoint returns the secret name and metadata but does not
    expose the decrypted secret value.
    """
    user_id = 1 if os.environ.get("FLOWFILE_MODE") == "electron" else current_user.id
    ip = _client_ip(request)

    db_secret = (
        db.query(db_models.Secret)
        .filter(db_models.Secret.user_id == user_id, db_models.Secret.name == secret_name)
        .first()
    )

    if not db_secret:
        audit.record_event(
            audit.SecretEvent(
                user_id=user_id,
                action="read",
                result_status="error",
                secret_name=secret_name,
                error="not_found",
                ip_address=ip,
            ),
            db=db,
        )
        db.commit()
        raise HTTPException(status_code=404, detail="Secret not found")

    audit.record_event(
        audit.SecretEvent(
            user_id=user_id,
            action="read",
            result_status="success",
            secret_name=db_secret.name,
            secret_id=db_secret.id,
            ip_address=ip,
        ),
        db=db,
    )
    db.commit()

    return Secret(name=db_secret.name, value=db_secret.encrypted_value, user_id=str(db_secret.user_id))


@router.delete("/secrets/{secret_name}", status_code=204)
async def delete_secret(
    secret_name: str,
    request: Request,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> None:
    """Deletes a secret by name for the authenticated user."""
    user_id = 1 if os.environ.get("FLOWFILE_MODE") == "electron" else current_user.id
    ip = _client_ip(request)

    # Look up before the delete so we can record the secret_id in the audit row;
    # the FK in ``secret_access_events`` is SET NULL on delete, but capturing
    # the id at the moment of the action makes joins against deleted rows easier.
    target = (
        db.query(db_models.Secret)
        .filter(db_models.Secret.user_id == user_id, db_models.Secret.name == secret_name)
        .first()
    )

    try:
        delete_secret_action(db, secret_name, user_id)
    except HTTPException as exc:
        # Map the 404 to a short error code so audit consumers can filter on it.
        err = "not_found" if exc.status_code == 404 else str(exc.detail)
        audit.record_event(
            audit.SecretEvent(
                user_id=user_id,
                action="delete",
                result_status="error",
                secret_name=secret_name,
                error=err,
                ip_address=ip,
            ),
            db=db,
        )
        db.commit()
        raise

    audit.record_event(
        audit.SecretEvent(
            user_id=user_id,
            action="delete",
            result_status="success",
            secret_name=secret_name,
            secret_id=target.id if target else None,
            ip_address=ip,
        ),
        db=db,
    )
    db.commit()

    return None
