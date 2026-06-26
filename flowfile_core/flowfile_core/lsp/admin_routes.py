"""Admin endpoint for the notebook LSP feature flag (mirrors ``ai/admin_routes.py``).

Mounted under ``/system`` in :mod:`flowfile_core.main`. Unlike the AI gate, the
``/lsp/*`` surfaces never 503 when off (they degrade to empty 200), so this endpoint
exists purely so an admin can flip ``FLOWFILE_LSP_ENABLED`` live without a restart.

Mutates the same two pieces of state the AI endpoint does: the ``MutableBool`` in
``settings`` (so :func:`flowfile_core.lsp.feature_flag.is_lsp_enabled` sees it on its
next call) and ``os.environ`` (so a hypothetical ``importlib.reload(settings)`` stays
consistent). ``persisted`` is always ``False`` — surviving a restart is the env-var's
job. Note: open editors cache ``/lsp/capabilities`` for the session, so a live flip
reaches them only on reload.
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
    """Request body for ``POST /system/feature_flags/lsp``."""

    enabled: bool = Field(..., description="Target state for FLOWFILE_LSP_ENABLED in the running process.")


class FeatureFlagState(BaseModel):
    """Response shape for the LSP feature-flag endpoints."""

    enabled: bool = Field(..., description="Live state of FLOWFILE_LSP_ENABLED in the running process.")
    persisted: bool = Field(
        ...,
        description=(
            "False when the value lives only in process memory; True would mean the change "
            "survives a restart. Always False here — restart-survival requires the user to "
            "set FLOWFILE_LSP_ENABLED in their .env."
        ),
    )


@router.post("/feature_flags/lsp", response_model=FeatureFlagState, tags=["system"])
def set_lsp_feature_flag(
    payload: FeatureFlagInput,
    _current_user: User = Depends(get_current_admin_user),
) -> FeatureFlagState:
    """Toggle the notebook LSP feature flag for the running process (admin-only)."""
    _settings.FLOWFILE_LSP_ENABLED.set(payload.enabled)
    os.environ["FLOWFILE_LSP_ENABLED"] = "true" if payload.enabled else "false"
    return FeatureFlagState(enabled=bool(_settings.FLOWFILE_LSP_ENABLED), persisted=False)


@router.get("/feature_flags/lsp", response_model=FeatureFlagState, tags=["system"])
def get_lsp_feature_flag(
    _current_user: User = Depends(get_current_admin_user),
) -> FeatureFlagState:
    """Read the live LSP feature-flag state (admin-only, same auth as the POST)."""
    return FeatureFlagState(enabled=bool(_settings.FLOWFILE_LSP_ENABLED), persisted=False)
