"""Snowflake connection helpers: a live look at an account before saving, and the browser sign-in.

``POST /snowflake/discover`` signs in with an access token or a key-pair that is not saved yet (or the stored
secret of a connection being edited) and answers the session's user, role, warehouse and database with the
warehouses, databases and roles on offer, which the connection form turns into dropdowns.

The browser sign-in is Snowflake's built-in OAuth client for local applications (authorization code + PKCE,
``shared.snowflake_auth``). ``/oauth/start`` records a pending sign-in (``snowflake_signin``) and answers the
Snowflake page to open; Snowflake sends the browser to ``/oauth/callback`` on core's own loopback port, which has
no Flowfile session and trusts only its signed ``state``; the form polls ``/oauth/status/{flow_id}`` and then saves
the connection with the sign-in's id, which claims the refresh token. The sign-in exists only where core runs on
the user's own machine (``sharing.browser_sign_in_allowed``): the client's redirect must be a loopback address.

No route here answers 401 for a Snowflake problem: the frontend reads 401 as an expired Flowfile session.
Failures are 422 with a plain string ``detail``.
"""

from __future__ import annotations

import html
import time
from typing import Any, Literal

import jwt
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import HTMLResponse
from jwt import PyJWTError
from pydantic import BaseModel, SecretStr
from sqlalchemy.orm import Session

from flowfile_core.auth import sharing
from flowfile_core.auth.jwt import get_current_active_user, get_jwt_secret
from flowfile_core.configs import logger, settings
from flowfile_core.configs.settings import ALGORITHM
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
from flowfile_core.schemas.input_schema import FullDatabaseConnection
from flowfile_core.secret_manager.secret_manager import decrypt_secret
from shared.db_dialects import get_dialect
from shared.snowflake_auth import AUTH_OAUTH, SnowflakeAuthError, authorize_url, exchange_code, new_pkce_pair
from shared.snowflake_auth import normalize_account as _normalize_account

router = APIRouter()

_STATE_TYPE = "snowflake_oauth_state"
_NO_REFRESH_TOKEN = (
    "This Snowflake account does not issue refresh tokens for the built-in sign-in, so Flowfile cannot keep the "
    "connection. Use an access token or a key-pair."
)


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


def _redirect_uri() -> str:
    # The desktop shell port-scans, so the live port, never a literal.
    return f"http://127.0.0.1:{settings.SERVER_PORT}/snowflake/oauth/callback"


def _sign_state(flow_id: str) -> str:
    payload = {"type": _STATE_TYPE, "flow_id": flow_id, "exp": int(time.time()) + snowflake_signin.TTL_SECONDS}
    return jwt.encode(payload, get_jwt_secret(), algorithm=ALGORITHM)


def _verify_state(state: str) -> dict:
    try:
        payload = jwt.decode(state, get_jwt_secret(), algorithms=[ALGORITHM])
    except PyJWTError as e:
        raise ValueError(
            f"This sign-in link is not valid any more ({type(e).__name__}). Start again in Flowfile."
        ) from e
    if payload.get("type") != _STATE_TYPE or not payload.get("flow_id"):
        raise ValueError("This sign-in link was not made by Flowfile. Start again in Flowfile.")
    return payload


def _page(ok: bool, message: str, status_code: int = 200) -> HTMLResponse:
    title = "Signed in to Snowflake" if ok else "Snowflake sign-in failed"
    body = f"""<!doctype html>
<html><head><meta charset="utf-8"><title>{html.escape(title)}</title></head>
<body style="font-family: system-ui, sans-serif; padding: 2rem; max-width: 40rem; margin: 0 auto;">
<h3>{html.escape(title)}</h3>
<p>{html.escape(message)}</p>
</body></html>"""
    return HTMLResponse(body, status_code=status_code)


def _require_driver() -> None:
    message = snowflake_unavailable_message()
    if message:
        raise HTTPException(422, message)


def _blank_to_none(value: str | None) -> str | None:
    value = (value or "").strip()
    return value or None


def _discover(uri: str) -> dict[str, Any]:
    return get_dialect(SNOWFLAKE).discover(uri)


def _session_out(host: str, info: dict[str, Any]) -> dict[str, Any]:
    return {
        "account_host": host,
        "username": info.get("user") or None,
        "role": info.get("role") or None,
        "warehouse": info.get("warehouse") or None,
        "database": info.get("database") or None,
        "warehouses": list(info.get("warehouses") or []),
        "databases": list(info.get("databases") or []),
        "roles": list(info.get("roles") or []),
    }


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
        return _session_out(host, _discover(uri))
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
    role = _blank_to_none(body.role)
    pkce = new_pkce_pair()
    redirect_uri = _redirect_uri()
    flow = snowflake_signin.start(
        user_id=current_user.id, host=host, role=role, verifier=pkce.verifier, redirect_uri=redirect_uri
    )
    url = authorize_url(
        host, redirect_uri=redirect_uri, state=_sign_state(flow.flow_id), code_challenge=pkce.challenge, role=role
    )
    return {"auth_url": url, "flow_id": flow.flow_id}


@router.get("/oauth/callback", response_class=HTMLResponse)
def oauth_callback(
    code: str | None = Query(None),
    state: str | None = Query(None),
    error: str | None = Query(None),
) -> HTMLResponse:
    """Where Snowflake sends the browser after the sign-in; trusted only through the signed ``state``.

    Exchanges the code for the tokens, then describes the session (a failure there does not fail the sign-in) and
    answers a page saying the tab can be closed. A sign-in that already finished is never overwritten.
    """
    if not state:
        return _page(False, "The sign-in link is incomplete. Start again in Flowfile.", 400)
    try:
        flow_id = _verify_state(state)["flow_id"]
    except ValueError as e:
        return _page(False, str(e), 400)
    flow = snowflake_signin.begin_exchange(flow_id)
    if flow is None:
        current = snowflake_signin.get(flow_id)
        if current is None:
            return _page(False, "This sign-in has expired. Start again in Flowfile.", 400)
        if current.status == "ok":
            return _page(True, "Signed in to Snowflake. You can close this tab and return to Flowfile.")
        return _page(False, current.message or "This sign-in is already being completed.")
    if error:
        message = (
            "The sign-in was cancelled in Snowflake. Start again in Flowfile."
            if error == "access_denied"
            else f"Snowflake did not complete the sign-in ({error})."
        )
        snowflake_signin.finish(flow_id, status="error", message=message)
        return _page(False, message)
    if not code:
        message = "Snowflake returned no sign-in code. Start again in Flowfile."
        snowflake_signin.finish(flow_id, status="error", message=message)
        return _page(False, message)
    try:
        tokens = exchange_code(flow.host, code=code, verifier=flow.verifier, redirect_uri=flow.redirect_uri)
    except SnowflakeAuthError as e:
        snowflake_signin.finish(flow_id, status="error", message=str(e))
        return _page(False, str(e))
    if not tokens.refresh_token:
        snowflake_signin.finish(flow_id, status="error", message=_NO_REFRESH_TOKEN)
        return _page(False, _NO_REFRESH_TOKEN)

    session: dict[str, Any] = {}
    message = None
    try:
        uri = get_dialect(SNOWFLAKE).build_uri(
            host=flow.host,
            username=tokens.username or None,
            password=tokens.refresh_token,
            auth=AUTH_OAUTH,
            role=flow.role,
        )
        session = _session_out(flow.host, _discover(uri))
    except ImportError:
        message = snowflake_unavailable_message()
    except ValueError as e:
        message = f"Signed in, but the warehouses and databases could not be listed: {e}"
    except Exception as e:
        message = f"Signed in, but the warehouses and databases could not be listed ({type(e).__name__})."
    snowflake_signin.finish(
        flow_id,
        status="ok",
        message=message,
        refresh_token=tokens.refresh_token,
        access_token=tokens.access_token,
        username=session.get("username") or tokens.username,
        session=session,
    )
    return _page(True, "Signed in to Snowflake. You can close this tab and return to Flowfile.")


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
