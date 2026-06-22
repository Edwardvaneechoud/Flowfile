import os

from fastapi import APIRouter
from fastapi.responses import RedirectResponse
from pydantic import BaseModel

from flowfile_core.auth.secrets import generate_master_key, is_master_key_configured
from flowfile_core.configs import settings
from flowfile_core.project.git_ops import git_available

router = APIRouter()


class SetupStatus(BaseModel):
    """Response model for the setup status endpoint."""

    setup_required: bool
    master_key_configured: bool
    mode: str
    projects_enabled: bool
    projects_confined: bool
    git_available: bool


class GeneratedKey(BaseModel):
    """Response model for the generate key endpoint."""

    key: str
    instructions: str


@router.get("/", tags=["admin"])
async def docs_redirect():
    """Redirects to the documentation page."""
    return RedirectResponse(url="/docs")


@router.get("/health/status", response_model=SetupStatus, tags=["health"])
async def get_setup_status():
    """Get the current setup status of the application."""
    # Default to "tauri" — `flowfile run ui` (no env) and the Tauri desktop shell
    # both want desktop-mode auth (auto-auth, no setup wizard). The frontend
    # accepts "electron" | "tauri" | "desktop" all as desktop mode, so existing
    # deployments that hard-code FLOWFILE_MODE=electron keep working.
    mode = os.environ.get("FLOWFILE_MODE", "tauri")
    master_key_ok = is_master_key_configured()
    return SetupStatus(
        setup_required=not master_key_ok,
        master_key_configured=master_key_ok,
        mode=mode,
        projects_enabled=(not settings.is_docker_mode()) or bool(settings.FLOWFILE_ENABLE_PROJECTS),
        projects_confined=not settings.is_electron_mode(),
        git_available=git_available(),
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
