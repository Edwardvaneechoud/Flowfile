"""FastAPI routes for managing Google Analytics 4 connections.

Credentials are minted via the OAuth 2.0 authorisation-code flow — the user
signs in with Google, consents to the ``analytics.readonly`` scope, and the
callback stores the resulting refresh token encrypted. The API never accepts
a raw credential over the wire.
"""

from __future__ import annotations

import html
import time
from urllib.parse import urlencode

import requests
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import HTMLResponse
from jose import JWTError, jwt
from pydantic import BaseModel
from sqlalchemy.orm import Session

from flowfile_core.auth.jwt import get_current_active_user, get_jwt_secret
from flowfile_core.configs import logger
from flowfile_core.configs.app_settings import (
    GOOGLE_OAUTH_CLIENT_SECRET_KEY,
    clear_google_oauth_config,
    get_google_oauth_config,
    get_user_secret,
    set_google_oauth_config,
)
from flowfile_core.configs.settings import ALGORITHM
from flowfile_core.database.connection import get_db
from flowfile_core.flowfile.database_connection_manager.ga_connections import (
    delete_ga_connection,
    get_all_ga_connections_interface,
    get_encrypted_refresh_token,
    get_ga_connection,
    update_ga_connection_metadata,
    upsert_ga_connection_with_refresh_token,
)
from flowfile_core.schemas.google_analytics_schemas import (
    FullGoogleAnalyticsConnectionInterface,
    GoogleAnalyticsConnectionMetadata,
)
from flowfile_core.secret_manager.secret_manager import decrypt_secret

router = APIRouter()

_OAUTH_AUTHORIZE_URL = "https://accounts.google.com/o/oauth2/v2/auth"
_OAUTH_TOKEN_URL = "https://oauth2.googleapis.com/token"
_OAUTH_SCOPES = "openid email https://www.googleapis.com/auth/analytics.readonly"
_STATE_TTL_SECONDS = 600
_STATE_TYPE = "ga_oauth_state"


class GoogleAnalyticsConnectionTestRequest(BaseModel):
    connection_name: str


class GoogleAnalyticsConnectionTestResponse(BaseModel):
    success: bool
    message: str


class OAuthStartResponse(BaseModel):
    auth_url: str


class GoogleOAuthClientView(BaseModel):
    client_id: str
    redirect_uri: str
    is_configured: bool


class GoogleOAuthClientInput(BaseModel):
    client_id: str
    # Blank means "keep the existing stored secret" — matches the form UX.
    client_secret: str = ""
    redirect_uri: str


def _sign_oauth_state(
    *,
    user_id: int,
    connection_name: str,
    description: str | None,
    default_property_id: str | None,
) -> str:
    payload = {
        "type": _STATE_TYPE,
        "user_id": user_id,
        "connection_name": connection_name,
        "description": description,
        "default_property_id": default_property_id,
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


def _resolve_oauth_config(db: Session, user_id: int) -> dict[str, str]:
    """Return the resolved Google OAuth client config or raise 500 if missing.

    Looks in the user's own Secret rows first (set via the Google OAuth card on
    the GA Connections page) then falls back to env vars. Either way the caller
    gets a usable ``{client_id, client_secret, redirect_uri}`` dict or a clean
    error explaining how to configure it.
    """
    cfg = get_google_oauth_config(db, user_id)
    if not cfg["client_id"] or not cfg["client_secret"] or not cfg["redirect_uri"]:
        raise HTTPException(
            500,
            "Google OAuth is not configured. Open Connections → Google Analytics and "
            "paste your Web application OAuth client credentials from "
            "https://console.cloud.google.com/apis/credentials.",
        )
    return cfg


def _callback_html(status: str, message: str) -> str:
    safe_status = html.escape(status)
    safe_message = html.escape(message)
    return f"""<!doctype html>
<html><head><meta charset=\"utf-8\"><title>Google Analytics — {safe_status}</title></head>
<body style=\"font-family: system-ui; padding: 2rem; max-width: 40rem; margin: 0 auto;\">
<h3>{safe_status}</h3>
<p>{safe_message}</p>
<p>You can close this window.</p>
<script>
(function () {{
  try {{
    if (window.opener) {{
      window.opener.postMessage(
        {{ source: "flowfile-ga-oauth", status: {safe_status!r}, message: {safe_message!r} }},
        "*"
      );
    }}
  }} catch (_) {{}}
  setTimeout(function () {{ try {{ window.close(); }} catch (_) {{}} }}, 500);
}})();
</script>
</body></html>"""


@router.get("/oauth/start", response_model=OAuthStartResponse, tags=["ga_connections"])
def oauth_start(
    connection_name: str = Query(..., min_length=1),
    description: str | None = Query(None),
    default_property_id: str | None = Query(None),
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> OAuthStartResponse:
    """Return the Google auth URL to open in a popup. The state JWT carries
    the user id + form values, signed with Flowfile's JWT secret so the
    (unauthenticated) callback can trust them."""
    cfg = _resolve_oauth_config(db, current_user.id)
    state = _sign_oauth_state(
        user_id=current_user.id,
        connection_name=connection_name,
        description=description,
        default_property_id=default_property_id,
    )
    params = {
        "response_type": "code",
        "client_id": cfg["client_id"],
        "redirect_uri": cfg["redirect_uri"],
        "scope": _OAUTH_SCOPES,
        "access_type": "offline",
        "prompt": "consent",
        "include_granted_scopes": "true",
        "state": state,
    }
    return OAuthStartResponse(auth_url=f"{_OAUTH_AUTHORIZE_URL}?{urlencode(params)}")


@router.get("/oauth/callback", tags=["ga_connections"])
def oauth_callback(
    code: str | None = Query(None),
    state: str | None = Query(None),
    error: str | None = Query(None),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    """Google redirects here with ``?code=...&state=...``. Exchanges the code
    for tokens, stores the refresh token encrypted against the user_id in the
    state, and renders a self-closing HTML page that postMessages the outcome
    to ``window.opener``."""
    if error:
        logger.info("GA OAuth callback error: %s", error)
        return HTMLResponse(_callback_html("error", f"Google returned error: {error}"))
    if not code or not state:
        return HTMLResponse(_callback_html("error", "Missing code or state parameter"))
    try:
        state_payload = _verify_oauth_state(state)
    except HTTPException as e:
        return HTMLResponse(_callback_html("error", e.detail), status_code=400)

    cfg = _resolve_oauth_config(db, int(state_payload["user_id"]))

    token_resp = requests.post(
        _OAUTH_TOKEN_URL,
        data={
            "code": code,
            "client_id": cfg["client_id"],
            "client_secret": cfg["client_secret"],
            "redirect_uri": cfg["redirect_uri"],
            "grant_type": "authorization_code",
        },
        timeout=20,
    )
    if not token_resp.ok:
        logger.error("GA OAuth token exchange failed: %s", token_resp.text)
        return HTMLResponse(
            _callback_html("error", f"Token exchange failed: {token_resp.text[:300]}"),
            status_code=400,
        )

    token_data = token_resp.json()
    refresh_token = token_data.get("refresh_token")
    if not refresh_token:
        # Happens when the user had a prior grant and Google doesn't re-issue
        # a refresh token. prompt=consent should force one — if it's still
        # missing, surface that instead of silently losing the credential.
        return HTMLResponse(
            _callback_html(
                "error",
                "Google did not return a refresh token. Revoke the app at "
                "https://myaccount.google.com/permissions and try again.",
            ),
            status_code=400,
        )

    oauth_user_email: str | None = None
    id_token = token_data.get("id_token")
    if id_token:
        try:
            id_payload = jwt.get_unverified_claims(id_token)
            oauth_user_email = id_payload.get("email")
        except JWTError:
            oauth_user_email = None

    upsert_ga_connection_with_refresh_token(
        db,
        connection_name=state_payload["connection_name"],
        user_id=int(state_payload["user_id"]),
        refresh_token=refresh_token,
        oauth_user_email=oauth_user_email,
        description=state_payload.get("description"),
        default_property_id=state_payload.get("default_property_id"),
    )
    return HTMLResponse(
        _callback_html("ok", f"Connected as {oauth_user_email or 'Google user'}")
    )


@router.put("/ga_connection", tags=["ga_connections"])
def update_ga_connection_endpoint(
    input_connection: GoogleAnalyticsConnectionMetadata,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Update description + default property id. Does NOT touch the credential —
    use the OAuth flow for that."""
    logger.info("Update GA connection %s", input_connection.connection_name)
    try:
        update_ga_connection_metadata(
            db,
            connection_name=input_connection.connection_name,
            user_id=current_user.id,
            description=input_connection.description,
            default_property_id=input_connection.default_property_id,
        )
    except ValueError as e:
        raise HTTPException(404, str(e)) from e
    return {"message": "Google Analytics connection updated successfully"}


@router.delete("/ga_connection", tags=["ga_connections"])
def delete_ga_connection_endpoint(
    connection_name: str,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    logger.info("Delete GA connection %s", connection_name)
    db_conn = get_ga_connection(db, connection_name, current_user.id)
    if db_conn is None:
        raise HTTPException(404, "Google Analytics connection not found")
    delete_ga_connection(db, connection_name, current_user.id)
    return {"message": "Google Analytics connection deleted successfully"}


@router.get(
    "/ga_connections",
    tags=["ga_connections"],
    response_model=list[FullGoogleAnalyticsConnectionInterface],
)
def list_ga_connections(
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> list[FullGoogleAnalyticsConnectionInterface]:
    return get_all_ga_connections_interface(db, current_user.id)


@router.post(
    "/test",
    tags=["ga_connections"],
    response_model=GoogleAnalyticsConnectionTestResponse,
)
def test_ga_connection(
    request: GoogleAnalyticsConnectionTestRequest,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> GoogleAnalyticsConnectionTestResponse:
    """Refresh the stored OAuth token. Proves the refresh token is still valid
    without touching the Analytics Data API (so no property-level IAM needed)."""
    cfg = _resolve_oauth_config(db, current_user.id)
    encrypted = get_encrypted_refresh_token(db, request.connection_name, current_user.id)
    if encrypted is None:
        raise HTTPException(404, "Google Analytics connection not found")

    try:
        refresh_token = decrypt_secret(encrypted).get_secret_value()
    except Exception as e:
        return GoogleAnalyticsConnectionTestResponse(
            success=False, message=f"Could not decrypt stored credential: {e}"
        )

    try:
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
    except ImportError as e:
        raise HTTPException(
            500,
            "google-auth is not installed. Install the 'google_analytics' extras to use this feature.",
        ) from e

    try:
        creds = Credentials(
            token=None,
            refresh_token=refresh_token,
            token_uri=_OAUTH_TOKEN_URL,
            client_id=cfg["client_id"],
            client_secret=cfg["client_secret"],
            scopes=["https://www.googleapis.com/auth/analytics.readonly"],
        )
        creds.refresh(Request())
    except Exception as e:
        return GoogleAnalyticsConnectionTestResponse(
            success=False, message=f"Could not refresh token: {e}"
        )

    return GoogleAnalyticsConnectionTestResponse(
        success=True, message="Refresh token is valid"
    )


@router.get("/oauth/client_config", response_model=GoogleOAuthClientView, tags=["ga_connections"])
def get_oauth_client(
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> GoogleOAuthClientView:
    """Return the stored OAuth client config (client secret is never echoed)."""
    cfg = get_google_oauth_config(db, current_user.id)
    return GoogleOAuthClientView(
        client_id=cfg["client_id"],
        redirect_uri=cfg["redirect_uri"],
        is_configured=bool(cfg["client_id"] and cfg["client_secret"] and cfg["redirect_uri"]),
    )


@router.put("/oauth/client_config", tags=["ga_connections"])
def put_oauth_client(
    body: GoogleOAuthClientInput,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> dict[str, str]:
    """Persist the OAuth client config. A blank ``client_secret`` preserves the
    existing stored value so the form can round-trip without the user re-typing
    the secret on every edit."""
    if not body.client_id.strip() or not body.redirect_uri.strip():
        raise HTTPException(422, "client_id and redirect_uri are required")

    existing_secret = get_user_secret(db, GOOGLE_OAUTH_CLIENT_SECRET_KEY, current_user.id)
    effective_secret = body.client_secret or existing_secret
    if not effective_secret:
        raise HTTPException(422, "client_secret is required when no existing value is stored")

    set_google_oauth_config(
        db,
        user_id=current_user.id,
        client_id=body.client_id,
        client_secret=effective_secret,
        redirect_uri=body.redirect_uri,
    )
    return {"message": "OAuth client config saved"}


@router.delete("/oauth/client_config", tags=["ga_connections"])
def delete_oauth_client(
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> dict[str, str]:
    clear_google_oauth_config(db, current_user.id)
    return {"message": "OAuth client config cleared"}
