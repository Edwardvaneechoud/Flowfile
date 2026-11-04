"""Consent endpoints for anonymous usage telemetry.

Two routes, no trailing slashes (the renderer matches these byte-for-byte;
a trailing slash would 307 and drop the POST body in Docker).

``can_manage`` is the only mode-dependent bit: on a single-user desktop or pip
install anyone may flip the toggle, while in a shared Docker deployment the
choice belongs to an admin. On a server joined to a hub (``hub_decides``) it
belongs to no one there: the server's environment says, as the hub's admin set
it, ``consent`` answers that decision, so nobody is asked, and every consent
POST is 403 with a sentence saying so. A non-manager POST is 403 and never 401 — the axios
interceptor logs the user out on 401. A consent write that does not stick
(read-only storage) is 503 ``TELEMETRY_PERSIST_FAILED`` rather than a 200 the
renderer would toast as success.

The install id is deliberately absent from every response: the renderer has no
use for it and it is the one field that is even weakly identifying.
"""

from __future__ import annotations

import os

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from flowfile_core import telemetry as core_telemetry
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.events import publish
from shared import telemetry

router = APIRouter(prefix="/telemetry", tags=["telemetry"])


class TelemetryConsentInput(BaseModel):
    """Request body for ``POST /telemetry/consent``."""

    enabled: bool


class TelemetryStatusOut(BaseModel):
    """Everything the privacy panel renders. Never carries the install id."""

    available: bool
    consent: bool | None
    env_kill_switch: bool
    endpoint_configured: bool
    can_manage: bool
    hub_decides: bool = False


def _can_manage(current_user) -> bool:
    # Read per call: configs.settings caches FLOWFILE_MODE at import.
    if os.environ.get("FLOWFILE_MODE", "electron") != "docker":
        return True
    return bool(getattr(current_user, "is_admin", False))


def _status(current_user) -> TelemetryStatusOut:
    status = telemetry.get_status().as_dict()
    decided = core_telemetry.hub_decision()
    if decided is not None:
        return TelemetryStatusOut(**{**status, "consent": decided}, can_manage=False, hub_decides=True)
    return TelemetryStatusOut(**status, can_manage=_can_manage(current_user))


@router.get("/status", response_model=TelemetryStatusOut)
def get_telemetry_status(current_user=Depends(get_current_active_user)) -> TelemetryStatusOut:
    """Current gate state and stored consent (``null`` when never asked)."""
    return _status(current_user)


@router.post("/consent", response_model=TelemetryStatusOut)
def set_telemetry_consent(
    body: TelemetryConsentInput,
    current_user=Depends(get_current_active_user),
) -> TelemetryStatusOut:
    """Grant or revoke consent. Declining also forgets the install id.

    A fresh grant publishes ``app_started``: the lifespan publish always happens
    before consent exists and is dropped, so an install that consents once and
    never relaunches would otherwise report zero launches and read as a failed
    startup rather than the churn it is. Re-granting publishes nothing (the
    install id is unchanged, so it would only duplicate), and neither does a
    revoke or a grant that failed to persist.
    """
    if core_telemetry.hub_decision() is not None:
        raise HTTPException(
            status_code=403,
            detail={"error_code": "TELEMETRY_HUB_DECIDES", "message": core_telemetry.HUB_DECIDES},
        )
    if not _can_manage(current_user):
        raise HTTPException(
            status_code=403,
            detail={"error_code": "TELEMETRY_ADMIN_ONLY", "message": "Only an admin can change telemetry consent."},
        )
    was_granted = telemetry.consent() is True
    result = telemetry.set_consent(body.enabled)
    if not result.persisted:
        raise HTTPException(
            status_code=503,
            detail={
                "error_code": "TELEMETRY_PERSIST_FAILED",
                "message": "Could not save the telemetry choice; it was not applied.",
            },
        )
    if body.enabled and not was_granted:
        publish("app_started")
    return _status(current_user)
