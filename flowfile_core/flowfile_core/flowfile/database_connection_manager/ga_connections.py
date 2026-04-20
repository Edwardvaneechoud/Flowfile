"""CRUD helpers for Google Analytics 4 connections.

Each connection stores an OAuth refresh token as an encrypted Secret (Fernet +
per-user HKDF key), referenced by foreign key. The refresh token is minted by
the OAuth callback in ``routes/ga_connections.py`` — there is no code path that
accepts a raw credential over the public API.
"""

from pydantic import SecretStr
from sqlalchemy.orm import Session

from flowfile_core.database.models import GoogleAnalyticsConnection as DBGoogleAnalyticsConnection
from flowfile_core.database.models import Secret
from flowfile_core.schemas.google_analytics_schemas import FullGoogleAnalyticsConnectionInterface
from flowfile_core.secret_manager.secret_manager import SecretInput, encrypt_secret, store_secret

_SECRET_SUFFIX = "_ga_oauth_refresh_token"


def _secret_name(connection_name: str) -> str:
    return connection_name + _SECRET_SUFFIX


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
        oauth_user_email=oauth_user_email,
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


def get_encrypted_refresh_token(db: Session, connection_name: str, user_id: int) -> str | None:
    """Return the stored ``$ffsec$1$...`` token, or ``None`` if the connection
    or its secret row is missing."""
    db_conn = get_ga_connection(db, connection_name, user_id)
    if not db_conn:
        return None
    secret_record = db.query(Secret).filter(Secret.id == db_conn.credential_secret_id).first()
    return secret_record.encrypted_value if secret_record else None


def ga_connection_interface_from_db(
    db_conn: DBGoogleAnalyticsConnection,
) -> FullGoogleAnalyticsConnectionInterface:
    return FullGoogleAnalyticsConnectionInterface(
        connection_name=db_conn.connection_name,
        description=db_conn.description,
        default_property_id=db_conn.default_property_id,
        oauth_user_email=db_conn.oauth_user_email,
    )


def get_all_ga_connections_interface(db: Session, user_id: int) -> list[FullGoogleAnalyticsConnectionInterface]:
    rows = db.query(DBGoogleAnalyticsConnection).filter(DBGoogleAnalyticsConnection.user_id == user_id).all()
    return [ga_connection_interface_from_db(r) for r in rows]
