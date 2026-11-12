import os

from fastapi import APIRouter
from fastapi.responses import RedirectResponse
from pydantic import BaseModel

from flowfile_core.auth.secrets import generate_master_key, is_master_key_configured
from flowfile_core.configs import settings
from flowfile_core.project.git_ops import git_available
from shared import server_extension
from shared._version import get_version

router = APIRouter()


class SetupStatus(BaseModel):
    """Response model for the setup status endpoint, which answers without a token.

    ``version`` is the Flowfile release this core runs, so whatever watches the server can read it without signing in.
    ``node`` is the name this storage dir keeps as a controller's server (``server_extension.identity``), left out of
    the answer when it keeps none: a controller that looks for a server that moved tells it by that name from another
    machine at the same private address on another network.
    """

    setup_required: bool
    master_key_configured: bool
    mode: str
    projects_enabled: bool
    projects_confined: bool
    git_available: bool
    version: str
    node: str | None = None


class GeneratedKey(BaseModel):
    """Response model for the generate key endpoint."""

    key: str
    instructions: str


@router.get("/", tags=["admin"])
async def docs_redirect():
    """Redirects to the documentation page."""
    return RedirectResponse(url="/docs")


@router.get("/health/status", response_model=SetupStatus, response_model_exclude_none=True, tags=["health"])
async def get_setup_status():
    """Get the current setup status of the application, with the name this storage dir keeps as a server."""
    # Default to "tauri" — `flowfile run ui` (no env) and the Tauri desktop shell
    # both want desktop-mode auth (auto-auth, no setup wizard). The frontend
    # accepts "electron" | "tauri" | "desktop" all as desktop mode, so existing
    # deployments that hard-code FLOWFILE_MODE=electron keep working.
    mode = os.environ.get("FLOWFILE_MODE", "tauri")
    master_key_ok = is_master_key_configured()
    identity = server_extension.identity()
    return SetupStatus(
        setup_required=not master_key_ok,
        master_key_configured=master_key_ok,
        mode=mode,
        projects_enabled=(not settings.is_docker_mode()) or bool(settings.FLOWFILE_ENABLE_PROJECTS),
        projects_confined=not settings.is_electron_mode(),
        git_available=git_available(),
        version=get_version(),
        node=identity.recorded_name if identity is not None else None,
    )


@router.post("/setup/generate-key", response_model=GeneratedKey, tags=["setup"])
async def generate_key():
    """Generate a new master encryption key."""
    key = generate_master_key()
    instructions = (
        f'Add to your .env file:\n  FLOWFILE_MASTER_KEY="{key}"\n\n'
        "Then restart: docker-compose down && docker-compose up"
    )
    return GeneratedKey(key=key, instructions=instructions)
