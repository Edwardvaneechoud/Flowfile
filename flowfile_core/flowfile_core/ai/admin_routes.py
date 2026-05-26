"""Admin endpoint for the AI feature flag.

Mounted under ``/system`` in :mod:`flowfile_core.main` — **outside** the
``/ai/*`` router. The whole point of this endpoint is to flip the AI gate
on; if it lived under the gated router it would be blocked by the very
flag it's supposed to set.

Mutates two pieces of state on every call:

* ``flowfile_core.configs.settings.FEATURE_FLAG_AI`` (the ``MutableBool``)
  so :func:`flowfile_core.ai.feature_flag.is_ai_enabled` sees the new value
  on its next call (live attribute lookup, no module reload required).
* ``os.environ["FEATURE_FLAG_AI"]`` so a hypothetical
  ``importlib.reload(settings)`` would re-resolve to the same value, and
  any future code that consults the env var directly stays in sync.

Persistence across process restarts is **not** part of this endpoint's
contract — that requires writing the value to a config file the user owns
(``.env``), which we surface as a UI hint instead. ``persisted`` is
therefore always ``false`` in the response so the caller can distinguish
"flag is on for this process" from "flag will boot on after restart".
"""

from __future__ import annotations

import os

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from flowfile_core.auth.jwt import get_current_admin_user
from flowfile_core.auth.models import User
from flowfile_core.configs import settings as _settings

router = APIRouter()


class FeatureFlagInput(BaseModel):
    """Request body for ``POST /system/feature_flags/ai``."""

    enabled: bool = Field(..., description="Target state for FEATURE_FLAG_AI in the running process.")


class FeatureFlagState(BaseModel):
    """Response shape for the AI feature-flag endpoints."""

    enabled: bool = Field(..., description="Live state of FEATURE_FLAG_AI in the running process.")
    persisted: bool = Field(
        ...,
        description=(
            "False when the value lives only in process memory; True would mean the change "
            "survives a restart. Always False here — restart-survival requires the user to "
            "set FEATURE_FLAG_AI in their .env."
        ),
    )


@router.post("/feature_flags/ai", response_model=FeatureFlagState, tags=["system"])
def set_ai_feature_flag(
    payload: FeatureFlagInput,
    _current_user: User = Depends(get_current_admin_user),
) -> FeatureFlagState:
    """Toggle the AI feature flag for the running process.

    Admin-only (``get_current_admin_user`` raises 403 for non-admin, 401
    for unauthenticated). The endpoint sits outside the gated ``/ai/*``
    router so it remains callable when the flag is off — that's the whole
    point: an admin uses this to turn AI on without touching the env / .env.

    Sets ``settings.FEATURE_FLAG_AI`` (the ``MutableBool``
    :func:`flowfile_core.ai.feature_flag.is_ai_enabled` reads on every
    call) and ``os.environ["FEATURE_FLAG_AI"]`` to the same boolean. The
    env-var write keeps a future ``importlib.reload(settings)`` consistent
    and unblocks any future code that consults the env var directly.
    """
    _settings.FEATURE_FLAG_AI.set(payload.enabled)
    os.environ["FEATURE_FLAG_AI"] = "true" if payload.enabled else "false"
    if payload.enabled:
        # Enabling at runtime (AI may have booted off): warm the litellm import now so
        # the first AI call doesn't pay the ~2-3s cold-start on the request hot path.
        from flowfile_core.ai.providers._litellm_base import start_prewarm

        start_prewarm()
    return FeatureFlagState(enabled=bool(_settings.FEATURE_FLAG_AI), persisted=False)


@router.get("/feature_flags/ai", response_model=FeatureFlagState, tags=["system"])
def get_ai_feature_flag(
    _current_user: User = Depends(get_current_admin_user),
) -> FeatureFlagState:
    """Read the live AI feature-flag state.

    Same admin-only auth as the POST so non-admin clients can't probe the
    flag through this surface. Returns ``persisted=False`` for the same
    reason :func:`set_ai_feature_flag` does — the value lives in process
    memory; cross-restart persistence is the env-var's job.
    """
    return FeatureFlagState(enabled=bool(_settings.FEATURE_FLAG_AI), persisted=False)
