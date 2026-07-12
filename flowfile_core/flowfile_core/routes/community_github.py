"""Token lifecycle for GitHub device-flow / PAT publishing of community nodes.

All routes are JWT-gated (never admin). GitHub-domain states never return 401 —
the frontend axios interceptor treats 401 as JWT expiry — so failures carry typed
``error_code`` payloads with non-401 status codes. The per-user token + login are
stored as two well-known ``Secret`` rows (the GA OAuth pattern); the token never
reaches the renderer, and device codes are never logged.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.configs.app_settings import delete_user_secret, get_user_secret, set_user_secret
from flowfile_core.configs.settings import get_community_github_client_id
from flowfile_core.database.connection import get_db
from flowfile_core.flowfile.community_nodes import github_client
from flowfile_core.flowfile.community_nodes.github_client import (
    GithubApiError,
    GithubAuthError,
    GithubNotConfiguredError,
)

router = APIRouter()

GITHUB_PUBLISH_TOKEN_KEY = "github_publish_token"
GITHUB_PUBLISH_LOGIN_KEY = "github_publish_login"

_SCOPE_HINT = (
    "This token has no public_repo or repo scope, so it can't fork the registry or open a "
    "pull request. Use a classic personal access token with the public_repo scope."
)


class GithubStatus(BaseModel):
    configured: bool
    connected: bool
    login: str = ""


class DevicePollRequest(BaseModel):
    device_code: str


class PatRequest(BaseModel):
    token: str


class PatResponse(BaseModel):
    login: str
    scopes_warning: str = ""


def _not_configured(e: GithubNotConfiguredError) -> HTTPException:
    return HTTPException(status_code=503, detail={"error_code": "GITHUB_NOT_CONFIGURED", "message": str(e)})


def _auth_http(e: GithubAuthError) -> HTTPException:
    mapping = {
        "device_expired": (410, "DEVICE_EXPIRED"),
        "device_denied": (410, "DEVICE_DENIED"),
        "token_invalid": (400, "GITHUB_TOKEN_INVALID"),
    }
    status, code = mapping.get(e.reason, (400, "GITHUB_TOKEN_INVALID"))
    return HTTPException(status_code=status, detail={"error_code": code, "message": str(e)})


def _api_http(e: GithubApiError) -> HTTPException:
    code = "GITHUB_RATE_LIMITED" if e.reason == "rate_limited" else "GITHUB_API_ERROR"
    return HTTPException(status_code=502, detail={"error_code": code, "message": str(e)})


def _scopes_warning(token: str) -> str:
    """Best-effort classic-PAT scope check. Fine-grained PATs carry no scopes header (no warning)."""
    try:
        resp = github_client._http(None).get(
            f"{github_client.GITHUB_API}/user",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
        )
        header = resp.headers.get("X-OAuth-Scopes")
    except Exception:
        return ""
    if header is None:
        return ""
    scopes = {s.strip() for s in header.split(",") if s.strip()}
    return "" if scopes & {"public_repo", "repo"} else _SCOPE_HINT


@router.get("/status", summary="GitHub publishing connection status")
def github_status(current_user=Depends(get_current_active_user), db: Session = Depends(get_db)) -> GithubStatus:
    token = get_user_secret(db, GITHUB_PUBLISH_TOKEN_KEY, current_user.id)
    login = get_user_secret(db, GITHUB_PUBLISH_LOGIN_KEY, current_user.id) or ""
    return GithubStatus(configured=bool(get_community_github_client_id()), connected=token is not None, login=login)


@router.post("/device/start", summary="Begin the GitHub device flow")
def github_device_start(current_user=Depends(get_current_active_user)) -> github_client.DeviceStart:
    try:
        return github_client.device_start()
    except GithubNotConfiguredError as e:
        raise _not_configured(e) from e
    except GithubApiError as e:
        raise _api_http(e) from e


@router.post("/device/poll", summary="Poll the GitHub device flow once")
def github_device_poll(
    request: DevicePollRequest,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> dict:
    try:
        result = github_client.device_poll(request.device_code)
        if result.status != "success":
            return {"status": result.status, "interval": result.interval}
        login = github_client.fetch_login(result.access_token)
    except GithubNotConfiguredError as e:
        raise _not_configured(e) from e
    except GithubAuthError as e:
        raise _auth_http(e) from e
    except GithubApiError as e:
        raise _api_http(e) from e

    set_user_secret(db, GITHUB_PUBLISH_TOKEN_KEY, result.access_token, current_user.id)
    set_user_secret(db, GITHUB_PUBLISH_LOGIN_KEY, login, current_user.id)
    return {"status": "connected", "login": login}


@router.post("/pat", summary="Store a GitHub personal access token")
def github_save_pat(
    request: PatRequest,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> PatResponse:
    token = request.token.strip()
    if not token:
        raise HTTPException(
            status_code=400, detail={"error_code": "GITHUB_TOKEN_INVALID", "message": "Provide a token."}
        )
    try:
        login = github_client.fetch_login(token)
    except GithubAuthError as e:
        raise _auth_http(e) from e
    except GithubApiError as e:
        raise _api_http(e) from e

    warning = _scopes_warning(token)
    set_user_secret(db, GITHUB_PUBLISH_TOKEN_KEY, token, current_user.id)
    set_user_secret(db, GITHUB_PUBLISH_LOGIN_KEY, login, current_user.id)
    return PatResponse(login=login, scopes_warning=warning)


@router.delete("/token", summary="Disconnect GitHub publishing")
def github_disconnect(current_user=Depends(get_current_active_user), db: Session = Depends(get_db)) -> dict:
    delete_user_secret(db, GITHUB_PUBLISH_TOKEN_KEY, current_user.id)
    delete_user_secret(db, GITHUB_PUBLISH_LOGIN_KEY, current_user.id)
    return {"disconnected": True}
