"""Snowflake connection helpers: a live look at an account before saving, and the browser sign-in.

``POST /snowflake/discover`` signs in with an access token or a key-pair that is not saved yet (or the stored
secret of a connection being edited) and answers the session's user, role, warehouse and database with the
warehouses, databases and roles on offer, which the connection form turns into dropdowns.

The browser sign-in is Snowflake's built-in OAuth client for local applications (authorization code + PKCE,
``shared.snowflake_auth``). ``/oauth/start`` opens a one-shot listener on ``127.0.0.1`` and a free port, records a
pending sign-in and answers the Snowflake page to open (``snowflake_callback``); Snowflake sends the browser back to
that listener, not to core's app, because the client accepts only ``http://127.0.0.1:<port>`` with no path. The
form polls ``/oauth/status/{flow_id}`` and then saves the connection with the sign-in's id, which claims the
refresh token. The sign-in exists only where core runs on the user's own machine (``sharing.browser_sign_in_allowed``):
the redirect must reach the user's own loopback address.

No route here answers 401 for a Snowflake problem: the frontend reads 401 as an expired Flowfile session.
Failures are 422 with a plain string ``detail``.
"""

from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, SecretStr
from sqlalchemy.orm import Session

from flowfile_core.auth import sharing
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.configs import logger
from flowfile_core.database.connection import get_db
from flowfile_core.database.models import Secret
from flowfile_core.flowfile.database_connection_manager import snowflake_signin
from flowfile_core.flowfile.database_connection_manager.db_connections import (
    SNOWFLAKE,
    get_database_connection,
    is_snowflake,
    prepare_snowflake_connection,
    snowflake_unavailable_message,
)
from flowfile_core.flowfile.database_connection_manager.snowflake_callback import begin_sign_in, session_out
from flowfile_core.schemas.input_schema import FullDatabaseConnection
from flowfile_core.secret_manager.secret_manager import decrypt_secret
from shared.db_dialects import get_dialect
from shared.snowflake_auth import AUTH_OAUTH, SnowflakeAuthError
from shared.snowflake_auth import normalize_account as _normalize_account

router = APIRouter()


class SnowflakeDiscoverRequest(BaseModel):
    account: str
    auth_method: Literal["access_token", "key_pair"]
    username: str = ""
    secret: SecretStr = SecretStr("")
    private_key_passphrase: SecretStr | None = None
    role: str | None = None
    connection_name: str | None = None


class SnowflakeSession(BaseModel):
    account_host: str | None = None
    username: str | None = None
    role: str | None = None
    warehouse: str | None = None
    database: str | None = None
    warehouses: list[str] = []
    databases: list[str] = []
    roles: list[str] = []


class SnowflakeOAuthStartRequest(BaseModel):
    account: str
    role: str | None = None


class SnowflakeOAuthStartResponse(BaseModel):
    auth_url: str
    flow_id: str


class SnowflakeOAuthStatus(BaseModel):
    status: Literal["pending", "ok", "error"]
    message: str | None = None
    account_host: str | None = None
    username: str | None = None
    role: str | None = None
    warehouse: str | None = None
    database: str | None = None
    warehouses: list[str] = []
    databases: list[str] = []


def _require_driver() -> None:
    message = snowflake_unavailable_message()
    if message:
        raise HTTPException(422, message)


def _blank_to_none(value: str | None) -> str | None:
    value = (value or "").strip()
    return value or None


def _discover(uri: str) -> dict[str, Any]:
    return get_dialect(SNOWFLAKE).discover(uri)


def _stored_target(db: Session, current_user, name: str | None) -> tuple[str, str, str, str | None, str | None] | None:
    """The stored Snowflake connection ``name`` the caller owns or may manage, as (host, method, secret, user, role).

    Only ever used with the stored host and method, so a stored credential is presented to no account but the one
    it was saved for.
    """
    if not name:
        return None
    row = get_database_connection(db, name, current_user.id)
    if row is None or not is_snowflake(row.database_type) or row.password_id is None:
        return None
    if row.user_id != current_user.id and not sharing.can_manage(db, current_user, "database_connection", row.id):
        return None
    secret = db.query(Secret).filter(Secret.id == row.password_id).first()
    if secret is None:
        return None
    value = decrypt_secret(secret.encrypted_value).get_secret_value()
    return row.host, row.auth_method, value, row.username, row.role


@router.post("/discover", response_model=SnowflakeSession)
def discover(
    body: SnowflakeDiscoverRequest,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """Sign in to an account with credentials not saved yet and describe the session and what it may use.

    A blank ``secret`` with the ``connection_name`` of a connection the caller owns or manages uses that
    connection's stored secret, always with its stored account and method.
    """
    _require_driver()
    username = body.username.strip()
    role = _blank_to_none(body.role)
    stored = _stored_target(db, current_user, body.connection_name) if not body.secret.get_secret_value() else None
    try:
        if stored is not None:
            host, method, secret, stored_user, stored_role = stored
            username = username or (stored_user or "")
            if method == AUTH_OAUTH:
                # A sign-in's token is bound to its user and role.
                username, role = stored_user or "", stored_role
        else:
            passphrase = body.private_key_passphrase.get_secret_value() if body.private_key_passphrase else None
            prepared = prepare_snowflake_connection(
                FullDatabaseConnection(
                    connection_name=body.connection_name or "discover",
                    database_type=SNOWFLAKE,
                    host=body.account,
                    username=username,
                    password=body.secret,
                    auth_method=body.auth_method,
                    role=role,
                ),
                user_id=current_user.id,
                private_key_passphrase=passphrase,
            )
            host, method, secret = prepared.host, prepared.auth_method, prepared.password.get_secret_value()
        uri = get_dialect(SNOWFLAKE).build_uri(
            host=host, username=username or None, password=secret, auth=method, role=role
        )
        return session_out(host, _discover(uri))
    except ImportError:
        raise HTTPException(
            422, snowflake_unavailable_message() or "The Snowflake driver could not be loaded."
        ) from None
    except ValueError as e:
        raise HTTPException(422, str(e)) from None
    except Exception as e:
        logger.warning("Snowflake discover failed: %s", type(e).__name__)
        raise HTTPException(422, f"Could not connect to Snowflake ({type(e).__name__}).") from None


@router.post("/oauth/start", response_model=SnowflakeOAuthStartResponse)
def oauth_start(body: SnowflakeOAuthStartRequest, current_user=Depends(get_current_active_user)) -> dict[str, str]:
    """Record a pending sign-in and answer the Snowflake page to open in the user's browser."""
    if not sharing.browser_sign_in_allowed():
        raise HTTPException(404, "Not Found")
    try:
        host = _normalize_account(body.account)
    except SnowflakeAuthError as e:
        raise HTTPException(422, str(e)) from None
    try:
        auth_url, flow_id = begin_sign_in(user_id=current_user.id, host=host, role=_blank_to_none(body.role))
    except SnowflakeAuthError as e:
        raise HTTPException(422, str(e)) from None
    return {"auth_url": auth_url, "flow_id": flow_id}


@router.get("/oauth/status/{flow_id}", response_model=SnowflakeOAuthStatus)
def oauth_status(flow_id: str, current_user=Depends(get_current_active_user)) -> dict[str, Any]:
    """How the caller's sign-in stands; 404 when it is unknown, expired or someone else's."""
    flow = snowflake_signin.get_for_user(flow_id, current_user.id)
    if flow is None:
        raise HTTPException(404, "This Snowflake sign-in has expired. Sign in again.")
    session = flow.session or {}
    return {
        "status": flow.public_status,
        "message": flow.message,
        "account_host": flow.host,
        "username": flow.username,
        "role": session.get("role") or flow.role,
        "warehouse": session.get("warehouse"),
        "database": session.get("database"),
        "warehouses": session.get("warehouses") or [],
        "databases": session.get("databases") or [],
    }
