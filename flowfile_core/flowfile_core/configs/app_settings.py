"""Instance-wide configuration storage, encrypted with the master key.

For values that aren't per-user — e.g. the Google OAuth client ID/secret the
server uses to talk to Google on any user's behalf. Keys used today:

- ``google_oauth_client_id``
- ``google_oauth_client_secret``
- ``google_oauth_redirect_uri``

All values round-trip as strings. Readers call ``get_app_setting`` or the
typed helper ``get_google_oauth_config`` (which also falls back to env vars).
"""

from __future__ import annotations

from sqlalchemy.orm import Session

from flowfile_core.configs.settings import (
    GOOGLE_OAUTH_CLIENT_ID,
    GOOGLE_OAUTH_CLIENT_SECRET,
    GOOGLE_OAUTH_REDIRECT_URI,
)
from flowfile_core.database.models import AppSetting
from flowfile_core.secret_manager.secret_manager import (
    _decrypt_with_master_key,
    _encrypt_with_master_key,
)

GOOGLE_OAUTH_CLIENT_ID_KEY = "google_oauth_client_id"
GOOGLE_OAUTH_CLIENT_SECRET_KEY = "google_oauth_client_secret"
GOOGLE_OAUTH_REDIRECT_URI_KEY = "google_oauth_redirect_uri"


def get_app_setting(db: Session, name: str) -> str | None:
    row = db.query(AppSetting).filter(AppSetting.name == name).first()
    if row is None:
        return None
    return _decrypt_with_master_key(row.encrypted_value).get_secret_value()


def set_app_setting(db: Session, name: str, value: str, user_id: int | None) -> None:
    encrypted = _encrypt_with_master_key(value)
    row = db.query(AppSetting).filter(AppSetting.name == name).first()
    if row is None:
        row = AppSetting(name=name, encrypted_value=encrypted, updated_by_user_id=user_id)
        db.add(row)
    else:
        row.encrypted_value = encrypted
        row.updated_by_user_id = user_id
    db.commit()


def delete_app_setting(db: Session, name: str) -> None:
    db.query(AppSetting).filter(AppSetting.name == name).delete(synchronize_session=False)
    db.commit()


def get_google_oauth_config(db: Session) -> dict[str, str]:
    """Resolve the Google OAuth client config: DB first, env vars as fallback.

    Returns keys ``client_id``, ``client_secret``, ``redirect_uri``. Any
    unresolved key is returned as an empty string (callers check for truthiness).
    """
    client_id = get_app_setting(db, GOOGLE_OAUTH_CLIENT_ID_KEY) or GOOGLE_OAUTH_CLIENT_ID
    client_secret = (
        get_app_setting(db, GOOGLE_OAUTH_CLIENT_SECRET_KEY) or GOOGLE_OAUTH_CLIENT_SECRET
    )
    redirect_uri = (
        get_app_setting(db, GOOGLE_OAUTH_REDIRECT_URI_KEY) or GOOGLE_OAUTH_REDIRECT_URI
    )
    return {
        "client_id": client_id or "",
        "client_secret": client_secret or "",
        "redirect_uri": redirect_uri or "",
    }


def set_google_oauth_config(
    db: Session,
    *,
    client_id: str,
    client_secret: str,
    redirect_uri: str,
    user_id: int | None,
) -> None:
    set_app_setting(db, GOOGLE_OAUTH_CLIENT_ID_KEY, client_id, user_id)
    set_app_setting(db, GOOGLE_OAUTH_CLIENT_SECRET_KEY, client_secret, user_id)
    set_app_setting(db, GOOGLE_OAUTH_REDIRECT_URI_KEY, redirect_uri, user_id)


def clear_google_oauth_config(db: Session) -> None:
    for name in (
        GOOGLE_OAUTH_CLIENT_ID_KEY,
        GOOGLE_OAUTH_CLIENT_SECRET_KEY,
        GOOGLE_OAUTH_REDIRECT_URI_KEY,
    ):
        delete_app_setting(db, name)


