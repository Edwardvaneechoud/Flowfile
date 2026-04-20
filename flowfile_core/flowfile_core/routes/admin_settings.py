"""Admin-only routes for instance-wide settings (OAuth client credentials, …).

Values are persisted via ``configs.app_settings`` (master-key Fernet). GET
returns a redacted view — the client secret is never echoed back to the
browser.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from flowfile_core.auth.jwt import get_current_admin_user
from flowfile_core.configs.app_settings import (
    GOOGLE_OAUTH_CLIENT_ID_KEY,
    GOOGLE_OAUTH_CLIENT_SECRET_KEY,
    GOOGLE_OAUTH_REDIRECT_URI_KEY,
    clear_google_oauth_config,
    get_app_setting,
    get_google_oauth_config,
    set_app_setting,
)
from flowfile_core.database.connection import get_db

router = APIRouter()


class GoogleOAuthConfigView(BaseModel):
    client_id: str
    redirect_uri: str
    is_configured: bool


class GoogleOAuthConfigInput(BaseModel):
    client_id: str = Field(..., min_length=1)
    # Blank means "keep the existing stored secret" — matches the form UX.
    client_secret: str = ""
    redirect_uri: str = Field(..., min_length=1)


@router.get(
    "/settings/google_oauth",
    tags=["admin_settings"],
    response_model=GoogleOAuthConfigView,
)
def get_google_oauth(
    current_user=Depends(get_current_admin_user),
    db: Session = Depends(get_db),
) -> GoogleOAuthConfigView:
    cfg = get_google_oauth_config(db)
    return GoogleOAuthConfigView(
        client_id=cfg["client_id"],
        redirect_uri=cfg["redirect_uri"],
        is_configured=bool(cfg["client_id"] and cfg["client_secret"] and cfg["redirect_uri"]),
    )


@router.put("/settings/google_oauth", tags=["admin_settings"])
def put_google_oauth(
    body: GoogleOAuthConfigInput,
    current_user=Depends(get_current_admin_user),
    db: Session = Depends(get_db),
) -> dict[str, str]:
    existing_secret = get_app_setting(db, GOOGLE_OAUTH_CLIENT_SECRET_KEY)
    effective_secret = body.client_secret or existing_secret
    if not effective_secret:
        raise HTTPException(422, "client_secret is required when no existing value is stored")

    set_app_setting(db, GOOGLE_OAUTH_CLIENT_ID_KEY, body.client_id, current_user.id)
    set_app_setting(db, GOOGLE_OAUTH_CLIENT_SECRET_KEY, effective_secret, current_user.id)
    set_app_setting(db, GOOGLE_OAUTH_REDIRECT_URI_KEY, body.redirect_uri, current_user.id)
    return {"message": "Google OAuth config saved"}


@router.delete("/settings/google_oauth", tags=["admin_settings"])
def delete_google_oauth(
    current_user=Depends(get_current_admin_user),
    db: Session = Depends(get_db),
) -> dict[str, str]:
    clear_google_oauth_config(db)
    return {"message": "Google OAuth config cleared"}
