"""OAuth credential custody for ``auth_method="oauth"`` database connections.

Core owns the whole token lifecycle: the interactive callback stores the
refresh token (owner-keyed ``$ffsec$`` ciphertext), and at credential
resolution time :func:`resolve_oauth_access_token` silently exchanges it for
a short-lived access token — persisting a rotated refresh token when the IdP
returns one (Okta does under some policies). The worker never refreshes; it
only receives the encrypted access token and connects. This keeps rotation
races out of worker subprocesses and honors the worker-never-touches-the-DB
rule.

An expired or revoked refresh token raises :class:`ReconnectRequiredError`,
which the API layer maps to HTTP 422 with ``error_code="RECONNECT_REQUIRED"``
(never 401 — the frontend treats 401 as JWT expiry).
"""

from __future__ import annotations

import json
from typing import NamedTuple

from pydantic import SecretStr
from sqlalchemy.orm import Session

from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import DatabaseConnection as DBConnectionModel
from flowfile_core.database.models import Secret
from flowfile_core.flowfile.database_connection_manager.db_connections import get_database_connection
from flowfile_core.secret_manager.secret_manager import SecretInput, decrypt_secret, encrypt_secret, store_secret
from shared.snowflake_oauth import SnowflakeOAuthError, derive_snowflake_endpoints, refresh_access_token


class ReconnectRequiredError(Exception):
    """The stored refresh token is expired/revoked; the user must sign in again."""

    def __init__(self, connection_name: str, detail: str = ""):
        self.connection_name = connection_name
        suffix = f" ({detail})" if detail else ""
        super().__init__(
            f"Reconnect your '{connection_name}' connection: its sign-in has expired or was revoked{suffix}. "
            "Open the connection settings and sign in again."
        )


class OAuthEndpoints(NamedTuple):
    authorize_endpoint: str
    token_endpoint: str


def _connection_account(db_connection: DBConnectionModel) -> str | None:
    if not db_connection.extra_params:
        return None
    try:
        parsed = json.loads(db_connection.extra_params)
    except (TypeError, ValueError):
        return None
    account = (parsed or {}).get("account") or db_connection.host
    return account or None


def resolve_oauth_endpoints(db_connection: DBConnectionModel) -> OAuthEndpoints:
    """Explicit endpoint overrides (External OAuth) or Snowflake-derived defaults."""
    authorize = db_connection.oauth_authorize_endpoint
    token = db_connection.oauth_token_endpoint
    if authorize and token:
        return OAuthEndpoints(authorize, token)
    account = _connection_account(db_connection)
    if not account:
        raise ValueError(
            "Cannot derive Snowflake OAuth endpoints: the connection has no 'account' extra param. "
            "Set the account, or configure explicit authorize/token endpoints."
        )
    derived_authorize, derived_token = derive_snowflake_endpoints(str(account))
    return OAuthEndpoints(authorize or derived_authorize, token or derived_token)


def _secret_ciphertext(db: Session, secret_id: int | None) -> str | None:
    if secret_id is None:
        return None
    secret = db.query(Secret).filter(Secret.id == secret_id).first()
    return secret.encrypted_value if secret else None


def store_refresh_token(db: Session, db_connection: DBConnectionModel, refresh_token: str) -> None:
    """Persist a (new or rotated) refresh token encrypted under the OWNER's key."""
    owner_id = db_connection.user_id
    if db_connection.oauth_refresh_token_id is not None:
        secret = db.query(Secret).filter(Secret.id == db_connection.oauth_refresh_token_id).first()
        if secret:
            secret.encrypted_value = encrypt_secret(refresh_token, owner_id)
            db.commit()
            return
    new_secret = store_secret(
        db,
        SecretInput(name=f"{db_connection.connection_name}_oauth_refresh_token", value=SecretStr(refresh_token)),
        owner_id,
    )
    db_connection.oauth_refresh_token_id = new_secret.id
    db.commit()


def resolve_oauth_access_token(connection_name: str, user_id: int) -> str:
    """Mint a short-lived access token for a stored OAuth connection.

    Returns the access token as owner-keyed ``$ffsec$`` ciphertext, ready for
    the core→worker wire. Raises :class:`ReconnectRequiredError` when the
    refresh token is missing, expired, or revoked.
    """
    with get_db_context() as db:
        db_connection = get_database_connection(db, connection_name, user_id)
        if db_connection is None:
            raise ValueError(f"Database connection '{connection_name}' not found or not accessible for this user")
        if db_connection.oauth_refresh_token_id is None:
            raise ReconnectRequiredError(connection_name, "no sign-in on record")
        refresh_ciphertext = _secret_ciphertext(db, db_connection.oauth_refresh_token_id)
        client_secret_ciphertext = _secret_ciphertext(db, db_connection.oauth_client_secret_id)
        if refresh_ciphertext is None:
            raise ReconnectRequiredError(connection_name, "no sign-in on record")
        if not db_connection.oauth_client_id or client_secret_ciphertext is None:
            raise ValueError(f"Connection '{connection_name}' has no OAuth client configured")
        endpoints = resolve_oauth_endpoints(db_connection)

        try:
            token_response = refresh_access_token(
                endpoints.token_endpoint,
                db_connection.oauth_client_id,
                decrypt_secret(client_secret_ciphertext).get_secret_value(),
                decrypt_secret(refresh_ciphertext).get_secret_value(),
            )
        except SnowflakeOAuthError as e:
            if e.requires_reauthentication:
                raise ReconnectRequiredError(connection_name, str(e)) from e
            raise

        if token_response.refresh_token:
            store_refresh_token(db, db_connection, token_response.refresh_token)
        return encrypt_secret(token_response.access_token, db_connection.user_id)
