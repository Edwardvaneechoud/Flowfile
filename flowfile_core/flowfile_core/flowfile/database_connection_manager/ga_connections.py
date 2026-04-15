"""CRUD helpers for Google Analytics 4 connections.

The service-account JSON is stored as an encrypted Secret referenced by foreign
key, mirroring the pattern used for cloud-storage and database connection
credentials. See ``db_connections.py`` for the sibling implementation.
"""

from pydantic import SecretStr
from sqlalchemy.orm import Session

from flowfile_core.database.models import GoogleAnalyticsConnection as DBGoogleAnalyticsConnection
from flowfile_core.database.models import Secret
from flowfile_core.schemas.google_analytics_schemas import (
    FullGoogleAnalyticsConnection,
    FullGoogleAnalyticsConnectionInterface,
)
from flowfile_core.secret_manager.secret_manager import SecretInput, decrypt_secret, encrypt_secret, store_secret

_SECRET_SUFFIX = "_ga_service_account_key"


def _secret_name(connection_name: str) -> str:
    return connection_name + _SECRET_SUFFIX


def get_ga_connection(db: Session, connection_name: str, user_id: int) -> DBGoogleAnalyticsConnection | None:
    """Fetch a GA connection row by name + owner."""
    return (
        db.query(DBGoogleAnalyticsConnection)
        .filter(
            DBGoogleAnalyticsConnection.connection_name == connection_name,
            DBGoogleAnalyticsConnection.user_id == user_id,
        )
        .first()
    )


def store_ga_connection(
    db: Session, connection: FullGoogleAnalyticsConnection, user_id: int
) -> DBGoogleAnalyticsConnection:
    """Persist a new GA connection + its encrypted service-account key."""
    if connection.service_account_json is None:
        raise ValueError("service_account_json is required")

    existing = get_ga_connection(db, connection.connection_name, user_id)
    if existing:
        raise ValueError(
            f"Google Analytics connection '{connection.connection_name}' already exists for user {user_id}."
        )

    secret_id = store_secret(
        db,
        SecretInput(name=_secret_name(connection.connection_name), value=connection.service_account_json),
        user_id,
    ).id

    db_conn = DBGoogleAnalyticsConnection(
        connection_name=connection.connection_name,
        description=connection.description,
        default_property_id=connection.default_property_id,
        service_account_key_id=secret_id,
        user_id=user_id,
    )
    db.add(db_conn)
    db.commit()
    db.refresh(db_conn)
    return db_conn


def update_ga_connection(
    db: Session, connection: FullGoogleAnalyticsConnection, user_id: int
) -> DBGoogleAnalyticsConnection:
    """Update an existing GA connection. If ``service_account_json`` is empty,
    the existing key is preserved (same UX as cloud connection updates)."""
    db_conn = get_ga_connection(db, connection.connection_name, user_id)
    if db_conn is None:
        raise ValueError(f"Google Analytics connection '{connection.connection_name}' not found for user {user_id}.")

    db_conn.description = connection.description
    db_conn.default_property_id = connection.default_property_id

    new_key_value = connection.service_account_json.get_secret_value() if connection.service_account_json else ""
    if new_key_value:
        secret_record = db.query(Secret).filter(Secret.id == db_conn.service_account_key_id).first()
        if secret_record:
            secret_record.encrypted_value = encrypt_secret(new_key_value, user_id)
        else:
            new_secret = store_secret(
                db,
                SecretInput(name=_secret_name(connection.connection_name), value=SecretStr(new_key_value)),
                user_id,
            )
            db_conn.service_account_key_id = new_secret.id

    db.commit()
    db.refresh(db_conn)
    return db_conn


def delete_ga_connection(db: Session, connection_name: str, user_id: int) -> None:
    """Delete the connection row and its associated Secret."""
    db_conn = get_ga_connection(db, connection_name, user_id)
    if not db_conn:
        return

    secret_id = db_conn.service_account_key_id
    db.delete(db_conn)
    if secret_id is not None:
        db.query(Secret).filter(Secret.id == secret_id).delete(synchronize_session=False)
    db.commit()


def get_ga_connection_schema(db: Session, connection_name: str, user_id: int) -> FullGoogleAnalyticsConnection | None:
    """Return the full connection with the service-account JSON decrypted."""
    db_conn = get_ga_connection(db, connection_name, user_id)
    if not db_conn:
        return None

    secret_record = db.query(Secret).filter(Secret.id == db_conn.service_account_key_id).first()
    decrypted_json = decrypt_secret(secret_record.encrypted_value) if secret_record else None

    return FullGoogleAnalyticsConnection(
        connection_name=db_conn.connection_name,
        description=db_conn.description,
        default_property_id=db_conn.default_property_id,
        service_account_json=decrypted_json,
    )


def ga_connection_interface_from_db(
    db_conn: DBGoogleAnalyticsConnection,
) -> FullGoogleAnalyticsConnectionInterface:
    return FullGoogleAnalyticsConnectionInterface(
        connection_name=db_conn.connection_name,
        description=db_conn.description,
        default_property_id=db_conn.default_property_id,
    )


def get_all_ga_connections_interface(db: Session, user_id: int) -> list[FullGoogleAnalyticsConnectionInterface]:
    rows = db.query(DBGoogleAnalyticsConnection).filter(DBGoogleAnalyticsConnection.user_id == user_id).all()
    return [ga_connection_interface_from_db(r) for r in rows]
