"""Per-user OAuth client config for the Google Analytics connector.

Stored as three well-known rows in the per-user ``Secret`` table so they show
up in the Secrets UI next to the refresh tokens and can be deleted from the
same place. Env vars act as a fallback for bootstrap / Docker deployments.
"""

from __future__ import annotations

from pydantic import SecretStr
from sqlalchemy.orm import Session

from flowfile_core.auth.models import SecretInput
from flowfile_core.configs.settings import (
    GOOGLE_OAUTH_CLIENT_ID,
    GOOGLE_OAUTH_CLIENT_SECRET,
    GOOGLE_OAUTH_REDIRECT_URI,
)
from flowfile_core.database.models import Secret
from flowfile_core.secret_manager.secret_manager import decrypt_secret, store_secret

GOOGLE_OAUTH_CLIENT_ID_KEY = "ga_oauth_client_id"
GOOGLE_OAUTH_CLIENT_SECRET_KEY = "ga_oauth_client_secret"
GOOGLE_OAUTH_REDIRECT_URI_KEY = "ga_oauth_redirect_uri"

_OAUTH_KEYS = (
    GOOGLE_OAUTH_CLIENT_ID_KEY,
    GOOGLE_OAUTH_CLIENT_SECRET_KEY,
    GOOGLE_OAUTH_REDIRECT_URI_KEY,
)


def _get_secret_row(db: Session, name: str, user_id: int) -> Secret | None:
    return (
        db.query(Secret)
        .filter(Secret.name == name, Secret.user_id == user_id)
        .first()
    )


def get_user_secret(db: Session, name: str, user_id: int) -> str | None:
    row = _get_secret_row(db, name, user_id)
    if row is None:
        return None
    return decrypt_secret(row.encrypted_value, user_id).get_secret_value()


def set_user_secret(db: Session, name: str, value: str, user_id: int) -> None:
    row = _get_secret_row(db, name, user_id)
    if row is None:
        store_secret(db, SecretInput(name=name, value=SecretStr(value)), user_id)
    else:
        # Re-encrypt through store_secret to keep a single encryption code path.
        # Simpler to update the row in place via encrypt_secret, but SecretInput
        # guarantees the value never appears unencrypted on any intermediate row.
        from flowfile_core.secret_manager.secret_manager import encrypt_secret

        row.encrypted_value = encrypt_secret(value, user_id)
        db.commit()


def delete_user_secret(db: Session, name: str, user_id: int) -> None:
    db.query(Secret).filter(Secret.name == name, Secret.user_id == user_id).delete(
        synchronize_session=False
    )
    db.commit()


def get_google_oauth_config(db: Session, user_id: int) -> dict[str, str]:
    """Resolve the Google OAuth client config: user's Secret rows first, env vars as fallback.

    Returns keys ``client_id``, ``client_secret``, ``redirect_uri``. Any
    unresolved key is returned as an empty string (callers check for truthiness).
    """
    client_id = get_user_secret(db, GOOGLE_OAUTH_CLIENT_ID_KEY, user_id) or GOOGLE_OAUTH_CLIENT_ID
    client_secret = (
        get_user_secret(db, GOOGLE_OAUTH_CLIENT_SECRET_KEY, user_id) or GOOGLE_OAUTH_CLIENT_SECRET
    )
    redirect_uri = (
        get_user_secret(db, GOOGLE_OAUTH_REDIRECT_URI_KEY, user_id) or GOOGLE_OAUTH_REDIRECT_URI
    )
    return {
        "client_id": client_id or "",
        "client_secret": client_secret or "",
        "redirect_uri": redirect_uri or "",
    }


def set_google_oauth_config(
    db: Session,
    *,
    user_id: int,
    client_id: str,
    client_secret: str,
    redirect_uri: str,
) -> None:
    set_user_secret(db, GOOGLE_OAUTH_CLIENT_ID_KEY, client_id, user_id)
    set_user_secret(db, GOOGLE_OAUTH_CLIENT_SECRET_KEY, client_secret, user_id)
    set_user_secret(db, GOOGLE_OAUTH_REDIRECT_URI_KEY, redirect_uri, user_id)


def clear_google_oauth_config(db: Session, user_id: int) -> None:
    for name in _OAUTH_KEYS:
        delete_user_secret(db, name, user_id)
