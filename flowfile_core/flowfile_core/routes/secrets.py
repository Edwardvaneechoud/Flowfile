"""
Manages CRUD (Create, Read, Update, Delete) operations for secrets.

This router provides secure endpoints for creating, retrieving, and deleting
sensitive credentials for the authenticated user. Secrets are encrypted before
being stored and are associated with the user's ID.
"""
import os
from typing import List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.auth.models import Secret, SecretInput
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db
from flowfile_core.secret_manager.secret_manager import store_secret, delete_secret as delete_secret_action

router = APIRouter(dependencies=[Depends(get_current_active_user)])


@router.get("/secrets", response_model=List[Secret])
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

    # Get secrets from database
    db_secrets = db.query(db_models.Secret).filter(db_models.Secret.user_id == user_id).all()

    # Prepare response model (without decrypting)
    secrets = []
    for db_secret in db_secrets:
        secrets.append(Secret(
            name=db_secret.name,
            value=db_secret.encrypted_value,
            user_id=str(db_secret.user_id)
        ))

    return secrets


@router.post("/secrets", response_model=Secret)
async def create_secret(secret: SecretInput, current_user=Depends(get_current_active_user),
                        db: Session = Depends(get_db)) -> Secret:
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
    # Get user ID
    user_id = 1 if os.environ.get("FLOWFILE_MODE") == "electron" else current_user.id

    existing_secret = db.query(db_models.Secret).filter(
        db_models.Secret.user_id == user_id,
        db_models.Secret.name == secret.name
    ).first()

    if existing_secret:
        raise HTTPException(status_code=400, detail="Secret with this name already exists")

    # The store_secret function handles encryption and DB storage
    stored_secret = store_secret(db, secret, user_id)
    return Secret(name=stored_secret.name, value=stored_secret.encrypted_value, user_id=str(user_id))


@router.get("/secrets/{secret_name}", response_model=Secret)
async def get_secret(secret_name: str,
                     current_user=Depends(get_current_active_user), db: Session = Depends(get_db)) -> Secret:
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
    # Get user ID
    user_id = 1 if os.environ.get("FLOWFILE_MODE") == "electron" else current_user.id

    # Get secret from database
    db_secret = db.query(db_models.Secret).filter(
        db_models.Secret.user_id == user_id,
        db_models.Secret.name == secret_name
    ).first()

    if not db_secret:
        raise HTTPException(status_code=404, detail="Secret not found")

    return Secret(
        name=db_secret.name,
        value=db_secret.encrypted_value,
        user_id=str(db_secret.user_id)
    )


@router.delete("/secrets/{secret_name}", status_code=204)
async def delete_secret(secret_name: str, current_user=Depends(get_current_active_user),
                        db: Session = Depends(get_db)) -> None:
    """Deletes a secret by name for the authenticated user.

    Args:
        secret_name: The name of the secret to delete.
        current_user: The authenticated user object, injected by FastAPI.
        db: The database session, injected by FastAPI.

    Returns:
        An empty response with a 204 No Content status code upon success.
    """
    # Get user ID
    user_id = 1 if os.environ.get("FLOWFILE_MODE") == "electron" else current_user.id
    delete_secret_action(db, secret_name, user_id)
    return None
