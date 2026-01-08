import os

from fastapi import APIRouter
from fastapi.responses import RedirectResponse
from pydantic import BaseModel

from flowfile_core.auth.secrets import generate_master_key, is_master_key_configured

# Router setup
router = APIRouter()


class SetupStatus(BaseModel):
    """Response model for the setup status endpoint."""

    setup_required: bool
    master_key_configured: bool
    mode: str


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
    """
    Get the current setup status of the application.

    Returns information about whether initial setup is required,
    including master key configuration status.
    """
    mode = os.environ.get("FLOWFILE_MODE", "electron")
    master_key_ok = is_master_key_configured()

    return SetupStatus(
        setup_required=not master_key_ok,
        master_key_configured=master_key_ok,
        mode=mode,
    )


@router.post("/setup/generate-key", response_model=GeneratedKey, tags=["setup"])
async def generate_key():
    """
    Generate a new master encryption key.

    This endpoint generates a valid Fernet key that can be used as the
    FLOWFILE_MASTER_KEY environment variable or saved to master_key.txt.

    Note: This does NOT automatically configure the key - the user must
    add it to their environment or Docker secrets configuration.
    """
    key = generate_master_key()
    instructions = (
        "Add this key to your configuration:\n\n"
        "Option 1 - Environment variable (add to .env or docker-compose.yml):\n"
        f"  FLOWFILE_MASTER_KEY={key}\n\n"
        "Option 2 - Docker secret (save to master_key.txt):\n"
        f"  echo '{key}' > master_key.txt\n\n"
        "After configuring, restart the application."
    )
    return GeneratedKey(key=key, instructions=instructions)
