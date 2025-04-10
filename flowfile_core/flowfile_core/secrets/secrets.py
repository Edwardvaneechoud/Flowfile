
from cryptography.fernet import Fernet
from sqlalchemy import and_
from sqlalchemy.orm import Session
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.auth.secrets import get_master_key
from pydantic import SecretStr
from flowfile_core.auth.models import SecretInput
from fastapi.exceptions import HTTPException


def encrypt_secret(secret_value):
    """Encrypt a secret value using the master key."""
    key = get_master_key().encode()
    f = Fernet(key)
    return f.encrypt(secret_value.encode()).decode()


def decrypt_secret(encrypted_value) -> SecretStr:
    """Decrypt an encrypted value using the master key."""
    key = get_master_key().encode()
    f = Fernet(key)
    return SecretStr(f.decrypt(encrypted_value.encode()).decode())


def get_encrypted_secret(current_user_id: int, secret_name: str) -> str|None:
    with get_db_context() as db:
        user_id = current_user_id
        db_secret = db.query(db_models.Secret).filter(and_(db_models.Secret.user_id == user_id,
                                                      db_models.Secret.name == secret_name)).first()
        if db_secret:
            return db_secret.encrypted_value
        else:
            return None


def store_secret(db: Session, secret: SecretInput, user_id: int) -> db_models.Secret:
    encrypted_value = encrypt_secret(secret.value.get_secret_value())

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
    return db_secret


def delete_secret(db: Session, secret_name: str, user_id: int) -> None:
    db_secret = db.query(db_models.Secret).filter(
        db_models.Secret.user_id == user_id,
        db_models.Secret.name == secret_name
    ).first()

    if not db_secret:
        raise HTTPException(status_code=404, detail="Secret not found")

    db.delete(db_secret)
    db.commit()
