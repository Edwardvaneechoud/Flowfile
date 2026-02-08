"""
File Manager API endpoints for Docker mode.

Provides endpoints to list, upload, and delete files in the user data directory.
Only intended for use in Docker mode where users need a web-based way to manage
data files (CSV, Parquet, Excel, JSON, text) in the shared volume.
"""

import os
from pathlib import Path

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from fastapi.responses import JSONResponse

from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.fileExplorer.funcs import FileInfo, SecureFileExplorer
from shared.storage_config import storage

router = APIRouter(dependencies=[Depends(get_current_active_user)])

ALLOWED_EXTENSIONS = {
    "csv", "parquet", "xlsx", "xls", "json", "txt", "tsv",
}

MAX_FILE_SIZE = 500 * 1024 * 1024  # 500 MB


def _check_docker_mode() -> None:
    """Raise 403 if not running in Docker mode."""
    if os.environ.get("FLOWFILE_MODE") != "docker":
        raise HTTPException(403, "File manager is only available in Docker mode")


@router.get("/files", response_model=list[FileInfo])
async def list_files() -> list[FileInfo]:
    """List files in the user data uploads directory."""
    _check_docker_mode()
    uploads_dir = storage.uploads_directory
    uploads_dir.mkdir(parents=True, exist_ok=True)
    explorer = SecureFileExplorer(
        start_path=uploads_dir,
        sandbox_root=uploads_dir,
    )
    return explorer.list_contents(
        show_hidden=False,
        file_types=list(ALLOWED_EXTENSIONS),
        sort_by="name",
    )


@router.post("/upload")
async def upload_file(file: UploadFile = File(...)) -> JSONResponse:
    """Upload a file to the user data uploads directory.

    Only allows files with permitted extensions and enforces a size limit.
    """
    _check_docker_mode()

    if not file.filename:
        raise HTTPException(400, "No filename provided")

    # Sanitize filename - take only the basename and strip traversal attempts
    safe_name = Path(file.filename).name
    if not safe_name or ".." in safe_name:
        raise HTTPException(400, "Invalid filename")

    # Check extension
    suffix = Path(safe_name).suffix.lstrip(".").lower()
    if suffix not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            400,
            f"File type '.{suffix}' not allowed. "
            f"Allowed types: {', '.join(sorted(ALLOWED_EXTENSIONS))}",
        )

    uploads_dir = storage.uploads_directory
    uploads_dir.mkdir(parents=True, exist_ok=True)
    file_location = uploads_dir / safe_name

    # Read file content with size check
    content = await file.read()
    if len(content) > MAX_FILE_SIZE:
        raise HTTPException(400, f"File too large. Maximum size is {MAX_FILE_SIZE // (1024 * 1024)} MB")

    with open(file_location, "wb") as f:
        f.write(content)

    return JSONResponse(
        content={
            "filename": safe_name,
            "filepath": str(file_location),
            "size": len(content),
        }
    )


@router.delete("/files/{filename}")
async def delete_file(filename: str) -> JSONResponse:
    """Delete a file from the user data uploads directory."""
    _check_docker_mode()

    # Sanitize filename
    safe_name = Path(filename).name
    if not safe_name or ".." in safe_name or "/" in filename or "\\" in filename:
        raise HTTPException(400, "Invalid filename")

    uploads_dir = storage.uploads_directory
    file_path = uploads_dir / safe_name

    # Verify path stays within uploads directory
    resolved = file_path.resolve()
    if not str(resolved).startswith(str(uploads_dir.resolve())):
        raise HTTPException(403, "Access denied")

    if not file_path.exists():
        raise HTTPException(404, "File not found")

    if file_path.is_dir():
        raise HTTPException(400, "Cannot delete directories")

    file_path.unlink()
    return JSONResponse(content={"message": f"File '{safe_name}' deleted"})
