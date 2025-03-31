# app_routes/secrets.py



import os
from typing import List

import keyring
from cryptography.fernet import Fernet
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.auth.models import Secret, SecretInput
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db

router = APIRouter(dependencies=[Depends(get_current_active_user)])


def get_master_key():
    # If in Electron mode, get or create from keyring
    if os.environ.get("FLOWFILE_MODE") == "electron" or 1 == 1:
        key = keyring.get_password("flowfile", "master_key")
        if not key:
            # Generate a new key (must be valid Fernet key)
            key = Fernet.generate_key().decode()
            keyring.set_password("flowfile", "master_key", key)
        return key
    else:
        # In Docker mode, get from environment
        key = os.environ.get("MASTER_KEY")
        if not key:
            raise Exception("MASTER_KEY environment variable must be set in Docker mode")
        return key


def encrypt_secret(secret_value):
    key = get_master_key().encode()
    f = Fernet(key)
    return f.encrypt(secret_value.encode()).decode()


def decrypt_secret(encrypted_value):
    key = get_master_key().encode()
    f = Fernet(key)
    return f.decrypt(encrypted_value.encode()).decode()


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

    # Encrypt secret
    encrypted_value = encrypt_secret(secret.value)

    # Store in database
    db_secret = db_models.Secret(
        name=secret.name,
        encrypted_value=encrypted_value,
        iv="",  # Not used with Fernet
        user_id=user_id
    )
    db.add(db_secret)
    db.commit()
    db.refresh(db_secret)

    return Secret(name=secret.name, value=encrypted_value, user_id=str(user_id))


# Get a specific secret by name
@router.get("/secrets/{secret_name}", response_model=Secret)
async def get_secret(secret_name: str, current_user=Depends(get_current_active_user), db: Session = Depends(get_db)):
    # Get user ID
    user_id = 1 if os.environ.get("FLOWFILE_MODE") == "electron" or 1 == 1 else current_user.id

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

    # Find secret
    db_secret = db.query(db_models.Secret).filter(
        db_models.Secret.user_id == user_id,
        db_models.Secret.name == secret_name
    ).first()

    if not db_secret:
        raise HTTPException(status_code=404, detail="Secret not found")

    # Delete secret
    db.delete(db_secret)
    db.commit()

    return None
