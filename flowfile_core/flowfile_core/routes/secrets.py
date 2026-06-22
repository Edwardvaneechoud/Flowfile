"""
Manages CRUD (Create, Read, Update, Delete) operations for secrets.

This router provides secure endpoints for creating, retrieving, and deleting
sensitive credentials for the authenticated user. Secrets are encrypted before
being stored and are associated with the user's ID.
"""

import os

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from flowfile_core.auth import sharing
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.auth.models import Secret, SecretInput
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db
from flowfile_core.schemas.sharing_schema import AccessInfo
from flowfile_core.secret_manager.secret_manager import delete_secret as delete_secret_action
from flowfile_core.secret_manager.secret_manager import store_secret

router = APIRouter(dependencies=[Depends(get_current_active_user)])

_OWNER_ACCESS = AccessInfo(is_owner=True, access_level="owner")


def _project_sync_secret(user_id: int) -> None:
    """Refresh the project's secret manifest after a standalone-secret change (no-op when no project)."""
    from flowfile_core.project import project_sync

    project_sync.secret_changed(user_id)


def _shared_secret_rows(db: Session, user_id: int) -> list[Secret]:
    """Group-shared secrets as metadata-only rows: no value, not even masked ciphertext."""
    return [
        Secret(name=row.name, value=None, user_id=str(row.user_id), id=row.id, access=access)
        for row, access in sharing.shared_resource_rows(db, user_id, "secret")
    ]


@router.get("/secrets", response_model=list[Secret])
async def get_secrets(current_user=Depends(get_current_active_user), db: Session = Depends(get_db)):
    """Retrieves all secret names for the currently authenticated user.

    Note: This endpoint returns the secret names and metadata but does not
    expose the decrypted secret values.

    Args:
        current_user: The authenticated user object, injected by FastAPI.
        db: The database session, injected by FastAPI.

    Returns:
        A list of `Secret` objects, each containing the name and encrypted value.
    """
    user_id = current_user.id

    db_secrets = db.query(db_models.Secret).filter(db_models.Secret.user_id == user_id).all()

    secrets = []
    for db_secret in db_secrets:
        secrets.append(
            Secret(
                name=db_secret.name,
                value=db_secret.encrypted_value,
                user_id=str(db_secret.user_id),
                id=db_secret.id,
                access=_OWNER_ACCESS,
            )
        )
    secrets.extend(_shared_secret_rows(db, user_id))

    return secrets


@router.post("/secrets", response_model=Secret)
async def create_secret(
    secret: SecretInput, current_user=Depends(get_current_active_user), db: Session = Depends(get_db)
) -> Secret:
    """Creates a new secret for the authenticated user.

    The secret value is encrypted before being stored in the database. A secret
    name must be unique for a given user.

    Args:
        secret: A `SecretInput` object containing the name and plaintext value of the secret.
        current_user: The authenticated user object, injected by FastAPI.
        db: The database session, injected by FastAPI.

    Raises:
        HTTPException: 400 if a secret with the same name already exists for the user.

    Returns:
        A `Secret` object containing the name and the *encrypted* value.
    """
    user_id = 1 if os.environ.get("FLOWFILE_MODE") == "electron" else current_user.id

    existing_secret = (
        db.query(db_models.Secret)
        .filter(db_models.Secret.user_id == user_id, db_models.Secret.name == secret.name)
        .first()
    )

    if existing_secret:
        raise HTTPException(status_code=400, detail="Secret with this name already exists")

    stored_secret = store_secret(db, secret, user_id)
    _project_sync_secret(user_id)
    return Secret(
        name=stored_secret.name,
        value=stored_secret.encrypted_value,
        user_id=str(user_id),
        id=stored_secret.id,
        access=_OWNER_ACCESS,
    )


@router.get("/secrets/{secret_name}", response_model=Secret)
async def get_secret(
    secret_name: str, current_user=Depends(get_current_active_user), db: Session = Depends(get_db)
) -> Secret:
    """Retrieves a specific secret by name for the authenticated user.

    Note: This endpoint returns the secret name and metadata but does not
    expose the decrypted secret value.

    Args:
        secret_name: The name of the secret to retrieve.
        current_user: The authenticated user object, injected by FastAPI.
        db: The database session, injected by FastAPI.

    Raises:
        HTTPException: 404 if the secret is not found.

    Returns:
        A `Secret` object containing the name and encrypted value.
    """
    user_id = 1 if os.environ.get("FLOWFILE_MODE") == "electron" else current_user.id

    db_secret = (
        db.query(db_models.Secret)
        .filter(db_models.Secret.user_id == user_id, db_models.Secret.name == secret_name)
        .order_by(db_models.Secret.id.asc())
        .first()
    )

    if db_secret:
        return Secret(
            name=db_secret.name,
            value=db_secret.encrypted_value,
            user_id=str(db_secret.user_id),
            id=db_secret.id,
            access=_OWNER_ACCESS,
        )

    # Shared-only match: metadata without the value (mirrors get_encrypted_secret's
    # own-shadows-shared, lowest-id-wins resolution).
    shared = [row for row in _shared_secret_rows(db, user_id) if row.name == secret_name]
    if shared:
        return shared[0]

    raise HTTPException(status_code=404, detail="Secret not found")


@router.delete("/secrets/{secret_name}", status_code=204)
async def delete_secret(
    secret_name: str, current_user=Depends(get_current_active_user), db: Session = Depends(get_db)
) -> None:
    """Deletes a secret by name for the authenticated user.

    Args:
        secret_name: The name of the secret to delete.
        current_user: The authenticated user object, injected by FastAPI.
        db: The database session, injected by FastAPI.

    Returns:
        An empty response with a 204 No Content status code upon success.
    """
    user_id = 1 if os.environ.get("FLOWFILE_MODE") == "electron" else current_user.id
    delete_secret_action(db, secret_name, user_id)
    _project_sync_secret(user_id)
    return None
