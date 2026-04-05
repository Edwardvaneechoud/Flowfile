"""CRUD operations for stored API connections."""

from __future__ import annotations

import json

from pydantic import SecretStr
from sqlalchemy.orm import Session

from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import ApiConnection as DBApiConnection
from flowfile_core.database.models import Secret
from flowfile_core.schemas.api_schemas import (
    ApiAuth,
    ApiKeyAuth,
    BasicAuth,
    BearerAuth,
    CustomHeaderAuth,
    FullApiConnection,
    FullApiConnectionInterface,
    OAuth2ClientCredentials,
)
from flowfile_core.secret_manager.secret_manager import SecretInput, store_secret


def store_api_connection(db: Session, connection: FullApiConnection, user_id: int) -> DBApiConnection:
    """Store a new API connection in the database."""
    existing = get_api_connection(db, connection.connection_name, user_id)
    if existing:
        raise ValueError(
            f"API connection '{connection.connection_name}' already exists for user {user_id}."
            " Use a unique name or delete the existing connection first."
        )

    auth = connection.auth
    auth_type = auth.auth_type if auth else "none"
    auth_secret_id = None
    auth_key = None
    auth_key_location = None
    auth_username = None
    oauth2_token_url = None
    oauth2_client_id = None
    oauth2_client_secret_id = None
    oauth2_scope = None

    if isinstance(auth, ApiKeyAuth):
        auth_key = auth.key
        auth_key_location = auth.location
        auth_secret_id = store_secret(
            db, SecretInput(name=f"{connection.connection_name}_api_key", value=auth.value), user_id
        ).id
    elif isinstance(auth, BearerAuth):
        auth_secret_id = store_secret(
            db, SecretInput(name=f"{connection.connection_name}_token", value=auth.token), user_id
        ).id
    elif isinstance(auth, BasicAuth):
        auth_username = auth.username
        auth_secret_id = store_secret(
            db, SecretInput(name=f"{connection.connection_name}_password", value=auth.password), user_id
        ).id
    elif isinstance(auth, OAuth2ClientCredentials):
        oauth2_token_url = auth.token_url
        oauth2_client_id = auth.client_id
        oauth2_scope = auth.scope
        oauth2_client_secret_id = store_secret(
            db, SecretInput(name=f"{connection.connection_name}_client_secret", value=auth.client_secret), user_id
        ).id
    elif isinstance(auth, CustomHeaderAuth):
        # Store all header secrets as a single JSON-encoded encrypted secret
        header_values = {k: v.get_secret_value() for k, v in auth.headers.items()}
        secret_name = f"{connection.connection_name}_custom_headers"
        secret_value = SecretStr(json.dumps(header_values))
        auth_secret_id = store_secret(db, SecretInput(name=secret_name, value=secret_value), user_id).id

    db_conn = DBApiConnection(
        connection_name=connection.connection_name,
        base_url=connection.base_url,
        auth_type=auth_type,
        auth_key=auth_key,
        auth_key_location=auth_key_location,
        auth_secret_id=auth_secret_id,
        auth_username=auth_username,
        oauth2_token_url=oauth2_token_url,
        oauth2_client_id=oauth2_client_id,
        oauth2_client_secret_id=oauth2_client_secret_id,
        oauth2_scope=oauth2_scope,
        default_headers=json.dumps(connection.default_headers) if connection.default_headers else None,
        verify_ssl=connection.verify_ssl,
        user_id=user_id,
    )

    db.add(db_conn)
    db.commit()
    db.refresh(db_conn)
    return db_conn


def get_api_connection(db: Session, connection_name: str, user_id: int) -> DBApiConnection | None:
    """Get an API connection by name and user ID."""
    return (
        db.query(DBApiConnection)
        .filter(DBApiConnection.connection_name == connection_name, DBApiConnection.user_id == user_id)
        .first()
    )


def get_api_connection_schema(db: Session, connection_name: str, user_id: int) -> FullApiConnection | None:
    """Get a full API connection with decrypted secrets."""
    db_conn = get_api_connection(db, connection_name, user_id)
    if db_conn is None:
        return None

    auth = _rebuild_auth(db, db_conn)
    default_headers = json.loads(db_conn.default_headers) if db_conn.default_headers else None

    return FullApiConnection(
        connection_name=db_conn.connection_name,
        base_url=db_conn.base_url,
        auth=auth,
        default_headers=default_headers,
        verify_ssl=db_conn.verify_ssl,
    )


def get_local_api_connection(connection_name: str, user_id: int) -> FullApiConnection | None:
    """Get a full API connection (convenience wrapper with auto-managed session)."""
    with get_db_context() as db:
        return get_api_connection_schema(db, connection_name, user_id)


def delete_api_connection(db: Session, connection_name: str, user_id: int) -> None:
    """Delete an API connection and its associated secrets."""
    db_conn = get_api_connection(db, connection_name, user_id)
    if db_conn is None:
        return

    # Clean up associated secrets
    for secret_id in [db_conn.auth_secret_id, db_conn.oauth2_client_secret_id]:
        if secret_id:
            secret = db.query(Secret).filter(Secret.id == secret_id).first()
            if secret:
                db.delete(secret)

    db.delete(db_conn)
    db.commit()


def get_all_api_connections_interface(db: Session, user_id: int) -> list[FullApiConnectionInterface]:
    """Get all API connections for a user (no secrets exposed)."""
    results = db.query(DBApiConnection).filter(DBApiConnection.user_id == user_id).all()
    return [
        FullApiConnectionInterface(
            connection_name=c.connection_name,
            base_url=c.base_url,
            auth_type=c.auth_type,
            verify_ssl=c.verify_ssl,
        )
        for c in results
    ]


def _rebuild_auth(db: Session, db_conn: DBApiConnection) -> ApiAuth | None:
    """Rebuild the typed ApiAuth from DB columns."""
    auth_type = db_conn.auth_type
    if auth_type == "none":
        return None

    def _get_secret_value(secret_id: int | None) -> SecretStr:
        if secret_id is None:
            raise ValueError("Expected secret_id but got None")
        secret = db.query(Secret).filter(Secret.id == secret_id).first()
        if secret is None:
            raise ValueError(f"Secret with id {secret_id} not found")
        return SecretStr(secret.encrypted_value)

    if auth_type == "api_key":
        return ApiKeyAuth(
            key=db_conn.auth_key,
            value=_get_secret_value(db_conn.auth_secret_id),
            location=db_conn.auth_key_location or "header",
        )
    elif auth_type == "bearer":
        return BearerAuth(token=_get_secret_value(db_conn.auth_secret_id))
    elif auth_type == "basic":
        return BasicAuth(
            username=db_conn.auth_username,
            password=_get_secret_value(db_conn.auth_secret_id),
        )
    elif auth_type == "oauth2_client_credentials":
        return OAuth2ClientCredentials(
            token_url=db_conn.oauth2_token_url,
            client_id=db_conn.oauth2_client_id,
            client_secret=_get_secret_value(db_conn.oauth2_client_secret_id),
            scope=db_conn.oauth2_scope,
        )
    elif auth_type == "custom_headers":
        encrypted_json = _get_secret_value(db_conn.auth_secret_id)
        header_values = json.loads(encrypted_json.get_secret_value())
        return CustomHeaderAuth(headers={k: SecretStr(v) for k, v in header_values.items()})

    return None
