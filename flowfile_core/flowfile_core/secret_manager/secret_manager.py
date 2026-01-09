import base64

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from fastapi.exceptions import HTTPException
from pydantic import SecretStr
from sqlalchemy import and_
from sqlalchemy.orm import Session

from flowfile_core.auth.models import SecretInput
from flowfile_core.auth.secrets import get_master_key
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context

# Version identifier for key derivation scheme (allows future migrations)
KEY_DERIVATION_VERSION = b"flowfile-secrets-v1"

# Encrypted secret format: $ffsec$1${user_id}${fernet_token}
# This embeds the user_id so the worker can derive the correct key
SECRET_FORMAT_PREFIX = "$ffsec$1$"


def derive_user_key(user_id: int) -> bytes:
    """
    Derive a user-specific encryption key from the master key using HKDF.

    This provides cryptographic isolation between users - each user's secrets
    are encrypted with a unique key derived from the master key.

    Args:
        user_id: The unique identifier for the user

    Returns:
        bytes: A 32-byte URL-safe base64-encoded key suitable for Fernet
    """
    master_key = get_master_key().encode()

    # Use HKDF to derive a user-specific key
    hkdf = HKDF(
        algorithm=hashes.SHA256(),
        length=32,  # Fernet requires 32 bytes
        salt=KEY_DERIVATION_VERSION,  # Static salt is fine for key derivation
        info=f"user-{user_id}".encode(),  # User-specific context
    )

    # Derive raw key material and encode for Fernet
    derived_key = hkdf.derive(master_key)
    return base64.urlsafe_b64encode(derived_key)


def _encrypt_with_master_key(secret_value: str) -> str:
    """Legacy encryption using master key directly (for backward compatibility)."""
    key = get_master_key().encode()
    f = Fernet(key)
    return f.encrypt(secret_value.encode()).decode()


def _decrypt_with_master_key(encrypted_value: str) -> SecretStr:
    """Legacy decryption using master key directly (for backward compatibility)."""
    key = get_master_key().encode()
    f = Fernet(key)
    return SecretStr(f.decrypt(encrypted_value.encode()).decode())


def encrypt_secret(secret_value: str, user_id: int) -> str:
    """
    Encrypt a secret value using a user-specific derived key.

    The encrypted format embeds the user_id so it can be decrypted
    without knowing the user context (e.g., by the worker service).

    Format: $ffsec$1${user_id}${fernet_token}

    Args:
        secret_value: The plaintext secret to encrypt
        user_id: The user ID to derive the encryption key for

    Returns:
        str: The encrypted value with embedded user_id
    """
    key = derive_user_key(user_id)
    f = Fernet(key)
    fernet_token = f.encrypt(secret_value.encode()).decode()
    return f"{SECRET_FORMAT_PREFIX}{user_id}${fernet_token}"


def decrypt_secret(encrypted_value: str, user_id: int | None = None) -> SecretStr:
    """
    Decrypt an encrypted value.

    Supports both new format (with embedded user_id) and legacy format.
    - New format: $ffsec$1${user_id}${fernet_token} - user_id extracted automatically
    - Legacy format: raw Fernet token - requires user_id parameter or uses master key

    Args:
        encrypted_value: The encrypted secret
        user_id: Optional user ID (required for legacy secrets, ignored for new format)

    Returns:
        SecretStr: The decrypted value wrapped in SecretStr
    """
    # Check for new versioned format with embedded user_id
    if encrypted_value.startswith(SECRET_FORMAT_PREFIX):
        # Parse: $ffsec$1${user_id}${fernet_token}
        remainder = encrypted_value[len(SECRET_FORMAT_PREFIX):]
        parts = remainder.split("$", 1)
        if len(parts) != 2:
            raise ValueError("Invalid encrypted secret format")

        embedded_user_id = int(parts[0])
        fernet_token = parts[1]

        key = derive_user_key(embedded_user_id)
        f = Fernet(key)
        return SecretStr(f.decrypt(fernet_token.encode()).decode())

    # Legacy format - use provided user_id or fall back to master key
    if user_id is not None:
        key = derive_user_key(user_id)
        f = Fernet(key)
        return SecretStr(f.decrypt(encrypted_value.encode()).decode())

    # Fall back to master key for legacy secrets without user context
    return _decrypt_with_master_key(encrypted_value)


def get_encrypted_secret(current_user_id: int, secret_name: str) -> str | None:
    with get_db_context() as db:
        user_id = current_user_id
        db_secret = (
            db.query(db_models.Secret)
            .filter(and_(db_models.Secret.user_id == user_id, db_models.Secret.name == secret_name))
            .first()
        )
        if db_secret:
            return db_secret.encrypted_value
        else:
            return None


def store_secret(db: Session, secret: SecretInput, user_id: int) -> db_models.Secret:
    encrypted_value = encrypt_secret(secret.value.get_secret_value(), user_id)

    # Store in database
    db_secret = db_models.Secret(
        name=secret.name,
        encrypted_value=encrypted_value,
        iv="",  # Legacy field, not used with current encryption
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
