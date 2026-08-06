"""FastAPI routes for the Snowflake SSO / OAuth sign-in flow on database connections.

Clones the Google Analytics OAuth machinery: ``/oauth/start`` builds the
authorize URL with an HMAC-signed state (the callback is unauthenticated, so
the state JWT is what lets it trust the user id + connection name), and
``/oauth/callback`` exchanges the code for tokens and stores the refresh
token encrypted against the connection. The API never accepts or returns raw
token material.
"""

from __future__ import annotations

import html
import time
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import HTMLResponse
from jose import JWTError, jwt
from pydantic import BaseModel
from sqlalchemy.orm import Session

from flowfile_core.auth.jwt import get_current_active_user, get_jwt_secret
from flowfile_core.configs import logger
from flowfile_core.configs.settings import ALGORITHM
from flowfile_core.database.connection import get_db
from flowfile_core.database.models import DatabaseConnection as DBConnectionModel
from flowfile_core.database.models import Secret
from flowfile_core.flowfile.database_connection_manager.db_connections import get_database_connection
from flowfile_core.flowfile.database_connection_manager.db_oauth import resolve_oauth_endpoints, store_refresh_token
from flowfile_core.secret_manager.secret_manager import decrypt_secret
from shared.snowflake_oauth import SnowflakeOAuthError, exchange_authorization_code

router = APIRouter()

_STATE_TTL_SECONDS = 600
_STATE_TYPE = "db_oauth_state"
_DEFAULT_REDIRECT_URI = "http://localhost:63578/db_connection_lib/oauth/callback"
# Snowflake's built-in OAuth server wants its proprietary scope; external IdPs
# use the standard OpenID offline-access scope for a refresh token.
_SNOWFLAKE_SCOPE = "refresh_token"
_EXTERNAL_IDP_SCOPE = "offline_access"


class OAuthStartResponse(BaseModel):
    auth_url: str


def _sign_oauth_state(*, user_id: int, connection_name: str) -> str:
    payload = {
        "type": _STATE_TYPE,
        "user_id": user_id,
        "connection_name": connection_name,
        "exp": int(time.time()) + _STATE_TTL_SECONDS,
    }
    return jwt.encode(payload, get_jwt_secret(), algorithm=ALGORITHM)


def _verify_oauth_state(state: str) -> dict:
    try:
        payload = jwt.decode(state, get_jwt_secret(), algorithms=[ALGORITHM])
    except JWTError as e:
        raise HTTPException(400, f"Invalid OAuth state: {e}") from e
    if payload.get("type") != _STATE_TYPE:
        raise HTTPException(400, "OAuth state has wrong type")
    return payload


def _redirect_uri(db_connection: DBConnectionModel) -> str:
    return db_connection.oauth_redirect_uri or _DEFAULT_REDIRECT_URI


def _client_config(db: Session, db_connection: DBConnectionModel) -> tuple[str, str]:
    """The connection's OAuth client id + decrypted client secret, or 422."""
    if not db_connection.oauth_client_id or db_connection.oauth_client_secret_id is None:
        raise HTTPException(
            422,
            f"Connection '{db_connection.connection_name}' has no OAuth client configured. "
            "Set the client id and client secret on the connection first.",
        )
    secret = db.query(Secret).filter(Secret.id == db_connection.oauth_client_secret_id).first()
    if secret is None:
        raise HTTPException(422, f"Connection '{db_connection.connection_name}' OAuth client secret is missing.")
    return db_connection.oauth_client_id, decrypt_secret(secret.encrypted_value).get_secret_value()


def _get_oauth_connection(db: Session, connection_name: str, user_id: int) -> DBConnectionModel:
    db_connection = get_database_connection(db, connection_name, user_id)
    if db_connection is None:
        raise HTTPException(404, "Database connection not found")
    if db_connection.auth_method != "oauth":
        raise HTTPException(422, f"Connection '{connection_name}' does not use OAuth authentication.")
    return db_connection


def _callback_html(status: str, message: str) -> str:
    safe_status = html.escape(status)
    safe_message = html.escape(message)
    return f"""<!doctype html>
<html><head><meta charset=\"utf-8\"><title>Database connection — {safe_status}</title></head>
<body style=\"font-family: system-ui; padding: 2rem; max-width: 40rem; margin: 0 auto;\">
<h3>{safe_status}</h3>
<p>{safe_message}</p>
<p>You can close this window.</p>
<script>
(function () {{
  try {{
    if (window.opener) {{
      window.opener.postMessage(
        {{ source: "flowfile-db-oauth", status: {safe_status!r}, message: {safe_message!r} }},
        "*"
      );
    }}
  }} catch (_) {{}}
  setTimeout(function () {{ try {{ window.close(); }} catch (_) {{}} }}, 500);
}})();
</script>
</body></html>"""


@router.get("/db_connection_lib/oauth/start", response_model=OAuthStartResponse, tags=["db_connections"])
def oauth_start(
    connection_name: str = Query(..., min_length=1),
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> OAuthStartResponse:
    """Return the IdP authorize URL to open in a popup for a stored OAuth connection."""
    db_connection = _get_oauth_connection(db, connection_name, current_user.id)
    client_id, _ = _client_config(db, db_connection)
    try:
        endpoints = resolve_oauth_endpoints(db_connection)
    except ValueError as e:
        raise HTTPException(422, str(e)) from e
    # Explicit endpoint overrides signal an external IdP (Okta / Entra / Ping).
    scope = _EXTERNAL_IDP_SCOPE if db_connection.oauth_token_endpoint else _SNOWFLAKE_SCOPE
    state = _sign_oauth_state(user_id=current_user.id, connection_name=connection_name)
    params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": _redirect_uri(db_connection),
        "scope": scope,
        "state": state,
    }
    return OAuthStartResponse(auth_url=f"{endpoints.authorize_endpoint}?{urlencode(params)}")


@router.get("/db_connection_lib/oauth/callback", tags=["db_connections"])
def oauth_callback(
    code: str | None = Query(None),
    state: str | None = Query(None),
    error: str | None = Query(None),
    error_description: str | None = Query(None),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    """The IdP redirects here with ``?code=...&state=...``; exchanges the code and
    stores the refresh token encrypted under the connection owner's key."""
    if error:
        logger.info("DB OAuth callback error: %s (%s)", error, error_description)
        detail = f"{error}: {error_description}" if error_description else error
        return HTMLResponse(_callback_html("error", f"The identity provider returned an error: {detail}"))
    if not code or not state:
        return HTMLResponse(_callback_html("error", "Missing code or state parameter"))
    try:
        state_payload = _verify_oauth_state(state)
    except HTTPException as e:
        return HTMLResponse(_callback_html("error", e.detail), status_code=400)

    connection_name = state_payload["connection_name"]
    user_id = int(state_payload["user_id"])
    try:
        db_connection = _get_oauth_connection(db, connection_name, user_id)
        client_id, client_secret = _client_config(db, db_connection)
        endpoints = resolve_oauth_endpoints(db_connection)
    except (HTTPException, ValueError) as e:
        detail = e.detail if isinstance(e, HTTPException) else str(e)
        return HTMLResponse(_callback_html("error", str(detail)), status_code=400)

    try:
        token_response = exchange_authorization_code(
            endpoints.token_endpoint, client_id, client_secret, code, _redirect_uri(db_connection)
        )
    except SnowflakeOAuthError as e:
        logger.error("DB OAuth token exchange failed for %s: %s", connection_name, e)
        return HTMLResponse(_callback_html("error", f"Token exchange failed: {e}"), status_code=400)

    if not token_response.refresh_token:
        return HTMLResponse(
            _callback_html(
                "error",
                "The identity provider did not return a refresh token. For Snowflake OAuth the "
                "security integration must allow the 'refresh_token' scope; for an external IdP "
                "request offline access.",
            ),
            status_code=400,
        )

    store_refresh_token(db, db_connection, token_response.refresh_token)
    return HTMLResponse(_callback_html("ok", f"Connection '{connection_name}' is signed in."))
