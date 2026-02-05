"""Core API endpoints for listing and managing shared user files."""

from fastapi import APIRouter, HTTPException

from shared.storage_config import storage

router = APIRouter(prefix="/shared-files", tags=["shared-files"])


@router.get("/")
def list_shared_files(subdirectory: str = "") -> list[dict]:
    """List files in the shared user files directory.

    Args:
        subdirectory: Optional subdirectory to list (relative to user_files root).

    Returns:
        List of file info dicts with name, size, and is_dir fields.
    """
    base = storage.user_files_directory
    target = (base / subdirectory).resolve() if subdirectory else base.resolve()

    # Path traversal check
    if not str(target).startswith(str(base.resolve())):
        raise HTTPException(status_code=400, detail="Invalid subdirectory")

    if not target.exists():
        return []

    return [
        {"name": f.name, "size": f.stat().st_size, "is_dir": f.is_dir()}
        for f in sorted(target.iterdir())
    ]
