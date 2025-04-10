# app_routes/secrets.py

import os
from typing import List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.auth.models import Secret, SecretInput
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db
from flowfile_core.secrets.secrets import encrypt_secret, store_secret, delete_secret as delete_secret_action

router = APIRouter(dependencies=[Depends(get_current_active_user)])


# Get all secrets for current user
@router.get("/secrets", response_model=List[Secret])
async def get_secrets(current_user=Depends(get_current_active_user), db: Session = Depends(get_db)):
    user_id = current_user.id

    # Get secrets from database
    db_secrets = db.query(db_models.Secret).filter(db_models.Secret.user_id == user_id).all()

    # Decrypt secrets
    secrets = []
    for db_secret in db_secrets:
        secrets.append(Secret(
            name=db_secret.name,
            value=db_secret.encrypted_value,
            user_id=str(db_secret.user_id)
        ))

    return secrets


# Create a new secret
@router.post("/secrets", response_model=Secret)
async def create_secret(secret: SecretInput, current_user=Depends(get_current_active_user),
                        db: Session = Depends(get_db)):
    print('current_user', current_user)
    # Get user ID
    user_id = 1 if os.environ.get("FLOWFILE_MODE") == "electron" or 1 == 1 else current_user.id

    existing_secret = db.query(db_models.Secret).filter(
        db_models.Secret.user_id == user_id,
        db_models.Secret.name == secret.name
    ).first()

    if existing_secret:
        raise HTTPException(status_code=400, detail="Secret with this name already exists")

    encrypted_value = store_secret(db, secret, user_id).encrypted_value
    return Secret(name=secret.name, value=encrypted_value, user_id=str(user_id))


# Get a specific secret by name
@router.get("/secrets/{secret_name}", response_model=Secret)
async def get_secret(secret_name: str, current_user=Depends(get_current_active_user), db: Session = Depends(get_db)):
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
async def delete_secret(secret_name: str, current_user=Depends(get_current_active_user), db: Session = Depends(get_db)):
    # Get user ID
    user_id = 1 if os.environ.get("FLOWFILE_MODE") == "electron" or 1 == 1 else current_user.id
    delete_secret_action(db, secret_name, user_id)
    return None
