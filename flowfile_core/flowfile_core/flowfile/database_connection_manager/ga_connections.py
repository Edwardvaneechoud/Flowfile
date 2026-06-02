"""CRUD helpers for Google Analytics 4 connections.

Each connection stores its credential as a single encrypted Secret (Fernet +
per-user HKDF key), referenced by foreign key. The credential is either an OAuth
refresh token (minted by the OAuth callback in ``routes/ga_connections.py``) or a
service-account JSON key (submitted to ``POST /ga_connection/service_account``),
selected by ``auth_method``.
"""

import json

from pydantic import SecretStr
from sqlalchemy.orm import Session

from flowfile_core.database.models import GoogleAnalyticsConnection as DBGoogleAnalyticsConnection
from flowfile_core.database.models import Secret
from flowfile_core.schemas.google_analytics_schemas import FullGoogleAnalyticsConnectionInterface
from flowfile_core.secret_manager.secret_manager import SecretInput, encrypt_secret, store_secret

_SECRET_SUFFIX = "_ga_oauth_refresh_token"
_SA_SECRET_SUFFIX = "_ga_service_account_key"


def _secret_name(connection_name: str) -> str:
    return connection_name + _SECRET_SUFFIX


def _sa_secret_name(connection_name: str) -> str:
    return connection_name + _SA_SECRET_SUFFIX


def get_ga_connection(db: Session, connection_name: str, user_id: int) -> DBGoogleAnalyticsConnection | None:
    return (
        db.query(DBGoogleAnalyticsConnection)
        .filter(
            DBGoogleAnalyticsConnection.connection_name == connection_name,
            DBGoogleAnalyticsConnection.user_id == user_id,
        )
        .first()
    )


def upsert_ga_connection_with_refresh_token(
    db: Session,
    *,
    connection_name: str,
    user_id: int,
    refresh_token: str,
    oauth_user_email: str | None,
    description: str | None = None,
    default_property_id: str | None = None,
) -> DBGoogleAnalyticsConnection:
    """Create or update a GA connection after a successful OAuth exchange.

    Existing rows keep their ``description`` and ``default_property_id`` if the
    caller passes ``None`` (so a *Reconnect* doesn't wipe metadata).
    """
    existing = get_ga_connection(db, connection_name, user_id)
    encrypted = encrypt_secret(refresh_token, user_id)

    if existing:
        secret_record = db.query(Secret).filter(Secret.id == existing.credential_secret_id).first()
        if secret_record:
            secret_record.encrypted_value = encrypted
        else:
            new_secret = store_secret(
                db,
                SecretInput(name=_secret_name(connection_name), value=SecretStr(refresh_token)),
                user_id,
            )
            existing.credential_secret_id = new_secret.id
        if description is not None:
            existing.description = description
        if default_property_id is not None:
            existing.default_property_id = default_property_id
        existing.auth_method = "oauth"
        existing.oauth_user_email = oauth_user_email
        db.commit()
        db.refresh(existing)
        return existing

    secret_id = store_secret(
        db,
        SecretInput(name=_secret_name(connection_name), value=SecretStr(refresh_token)),
        user_id,
    ).id
    db_conn = DBGoogleAnalyticsConnection(
        connection_name=connection_name,
        description=description,
        default_property_id=default_property_id,
        auth_method="oauth",
        oauth_user_email=oauth_user_email,
        credential_secret_id=secret_id,
        user_id=user_id,
    )
    db.add(db_conn)
    db.commit()
    db.refresh(db_conn)
    return db_conn


def _validate_service_account_key(service_account_key: str) -> dict:
    """Parse + validate a service-account JSON key, returning the parsed dict.

    Raises ``ValueError`` if the JSON is malformed, is not a service-account key,
    or is missing the ``client_email`` / ``private_key`` fields the GA4 client
    needs to authenticate.
    """
    try:
        info = json.loads(service_account_key)
    except json.JSONDecodeError as e:
        raise ValueError(f"Service account key is not valid JSON: {e}") from e
    if not isinstance(info, dict):
        raise ValueError("Service account key must be a JSON object.")
    if info.get("type") != "service_account":
        raise ValueError('Service account key must have "type": "service_account".')
    # ``client_email`` + ``private_key`` + ``token_uri`` are the fields the GA4
    # client (google.oauth2.service_account) requires to authenticate — validate
    # them at save time so an incomplete key is rejected here rather than failing
    # cryptically at flow-run time.
    for field in ("client_email", "private_key", "token_uri"):
        if not info.get(field):
            raise ValueError(f"Service account key is missing required field '{field}'.")
    return info


def upsert_ga_connection_with_service_account(
    db: Session,
    *,
    connection_name: str,
    user_id: int,
    service_account_key: str,
    description: str | None = None,
    default_property_id: str | None = None,
) -> DBGoogleAnalyticsConnection:
    """Create or update a GA connection that authenticates with a service-account
    JSON key. The key is validated, then stored as a single encrypted Secret —
    reusing ``credential_secret_id``, the same slot the OAuth refresh token uses.

    ``oauth_user_email`` is set to the key's ``client_email`` so the UI can show
    the principal identity. Existing rows keep their ``description`` and
    ``default_property_id`` when the caller passes ``None``.
    """
    info = _validate_service_account_key(service_account_key)
    client_email = info["client_email"]

    existing = get_ga_connection(db, connection_name, user_id)
    encrypted = encrypt_secret(service_account_key, user_id)

    if existing:
        secret_record = db.query(Secret).filter(Secret.id == existing.credential_secret_id).first()
        if secret_record:
            secret_record.encrypted_value = encrypted
        else:
            new_secret = store_secret(
                db,
                SecretInput(name=_sa_secret_name(connection_name), value=SecretStr(service_account_key)),
                user_id,
            )
            existing.credential_secret_id = new_secret.id
        if description is not None:
            existing.description = description
        if default_property_id is not None:
            existing.default_property_id = default_property_id
        existing.auth_method = "service_account"
        existing.oauth_user_email = client_email
        db.commit()
        db.refresh(existing)
        return existing

    secret_id = store_secret(
        db,
        SecretInput(name=_sa_secret_name(connection_name), value=SecretStr(service_account_key)),
        user_id,
    ).id
    db_conn = DBGoogleAnalyticsConnection(
        connection_name=connection_name,
        description=description,
        default_property_id=default_property_id,
        auth_method="service_account",
        oauth_user_email=client_email,
        credential_secret_id=secret_id,
        user_id=user_id,
    )
    db.add(db_conn)
    db.commit()
    db.refresh(db_conn)
    return db_conn


def update_ga_connection_metadata(
    db: Session,
    *,
    connection_name: str,
    user_id: int,
    description: str | None,
    default_property_id: str | None,
) -> DBGoogleAnalyticsConnection:
    """Update description + default property id only. The OAuth credential is
    never touched by this path — use the OAuth callback for that."""
    db_conn = get_ga_connection(db, connection_name, user_id)
    if db_conn is None:
        raise ValueError(f"Google Analytics connection '{connection_name}' not found for user {user_id}.")
    db_conn.description = description
    db_conn.default_property_id = default_property_id
    db.commit()
    db.refresh(db_conn)
    return db_conn


def delete_ga_connection(db: Session, connection_name: str, user_id: int) -> None:
    db_conn = get_ga_connection(db, connection_name, user_id)
    if not db_conn:
        return

    secret_id = db_conn.credential_secret_id
    db.delete(db_conn)
    if secret_id is not None:
        db.query(Secret).filter(Secret.id == secret_id).delete(synchronize_session=False)
    db.commit()


def get_encrypted_credential(db: Session, connection_name: str, user_id: int) -> str | None:
    """Return the connection's stored ``$ffsec$1$...`` credential blob — the
    encrypted refresh token or service-account JSON, depending on ``auth_method``
    — or ``None`` if the connection or its secret row is missing."""
    db_conn = get_ga_connection(db, connection_name, user_id)
    if not db_conn:
        return None
    secret_record = db.query(Secret).filter(Secret.id == db_conn.credential_secret_id).first()
    return secret_record.encrypted_value if secret_record else None


# Backwards-compatible alias: the function returns the stored credential blob
# regardless of credential type, so callers added before service-account support
# (which import ``get_encrypted_refresh_token``) keep working unchanged.
get_encrypted_refresh_token = get_encrypted_credential


def ga_connection_interface_from_db(
    db_conn: DBGoogleAnalyticsConnection,
) -> FullGoogleAnalyticsConnectionInterface:
    return FullGoogleAnalyticsConnectionInterface(
        connection_name=db_conn.connection_name,
        description=db_conn.description,
        default_property_id=db_conn.default_property_id,
        auth_method=db_conn.auth_method,
        oauth_user_email=db_conn.oauth_user_email,
    )


def get_all_ga_connections_interface(db: Session, user_id: int) -> list[FullGoogleAnalyticsConnectionInterface]:
    rows = db.query(DBGoogleAnalyticsConnection).filter(DBGoogleAnalyticsConnection.user_id == user_id).all()
    return [ga_connection_interface_from_db(r) for r in rows]
