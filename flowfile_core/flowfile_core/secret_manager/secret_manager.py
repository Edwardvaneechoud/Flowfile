from cryptography.fernet import Fernet
from fastapi.exceptions import HTTPException
from pydantic import SecretStr
from sqlalchemy import and_
from sqlalchemy.orm import Session

from flowfile_core.auth.models import SecretInput
from flowfile_core.auth.secrets import get_master_key
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from shared.crypto.envelope import (
    KEY_DERIVATION_VERSION,
    SECRET_FORMAT_PREFIX,
    decrypt_secret_envelope,
    encrypt_secret_envelope,
)
from shared.crypto.envelope import (
    derive_user_key as _derive_user_key,
)

__all__ = [
    "KEY_DERIVATION_VERSION",
    "SECRET_FORMAT_PREFIX",
    "SecretInput",
    "_decrypt_with_master_key",
    "_encrypt_with_master_key",
    "decrypt_secret",
    "delete_secret",
    "derive_user_key",
    "encrypt_secret",
    "get_encrypted_secret",
    "store_secret",
]


def derive_user_key(user_id: int) -> bytes:
    """Derive a per-user Fernet key from the current master key."""
    return _derive_user_key(get_master_key().encode(), user_id)


def _encrypt_with_master_key(secret_value: str) -> str:
    """Legacy encryption with the master key directly (no per-user derivation).

    Still used by older callers (e.g. ``flow_graph._encrypt_with_master_key``).
    New code should use :func:`encrypt_secret` so the envelope embeds user_id.
    """
    key = get_master_key().encode()
    return Fernet(key).encrypt(secret_value.encode()).decode()


def _decrypt_with_master_key(encrypted_value: str) -> SecretStr:
    """Legacy counterpart to :func:`_encrypt_with_master_key`."""
    key = get_master_key().encode()
    return SecretStr(Fernet(key).decrypt(encrypted_value.encode()).decode())


def encrypt_secret(secret_value: str, user_id: int) -> str:
    """Encrypt ``secret_value`` as a v1 envelope bound to ``user_id``."""
    return encrypt_secret_envelope(get_master_key().encode(), secret_value, user_id)


def decrypt_secret(encrypted_value: str, user_id: int | None = None) -> SecretStr:
    """Decrypt a v1 envelope or a legacy raw token.

    For v1 envelopes, ``user_id`` is read from the envelope. For legacy raw
    tokens, the caller-supplied ``user_id`` selects the derived key; ``None``
    falls back to master-key decryption.
    """
    return decrypt_secret_envelope(get_master_key().encode(), encrypted_value, user_id)


def get_encrypted_secret(current_user_id: int, secret_name: str) -> str | None:
    with get_db_context() as db:
        db_secret = (
            db.query(db_models.Secret)
            .filter(and_(db_models.Secret.user_id == current_user_id, db_models.Secret.name == secret_name))
            .first()
        )
        if db_secret:
            return db_secret.encrypted_value
        return None


def store_secret(db: Session, secret: SecretInput, user_id: int) -> db_models.Secret:
    encrypted_value = encrypt_secret(secret.value.get_secret_value(), user_id)

    db_secret = db_models.Secret(
        name=secret.name,
        encrypted_value=encrypted_value,
        iv="",
        user_id=user_id,
    )
    db.add(db_secret)
    db.commit()
    db.refresh(db_secret)
    return db_secret


def delete_secret(db: Session, secret_name: str, user_id: int) -> None:
    db_secret = (
        db.query(db_models.Secret)
        .filter(db_models.Secret.user_id == user_id, db_models.Secret.name == secret_name)
        .first()
    )

    if not db_secret:
        raise HTTPException(status_code=404, detail="Secret not found")

    db.delete(db_secret)
    db.commit()
