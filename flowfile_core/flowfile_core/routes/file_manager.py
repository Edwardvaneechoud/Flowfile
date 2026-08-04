"""
File Manager API endpoints.

Provides endpoints to list, upload, and delete files in the user data directory.
Listing and deletion serve the Docker-mode file manager UI, where users need a
web-based way to manage data files in the shared volume. Uploading is available
in every mode because the canvas file-drop relies on it.
"""

import os
from pathlib import Path

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.fileExplorer.funcs import FileInfo, SecureFileExplorer
from shared.storage_config import storage

router = APIRouter(dependencies=[Depends(get_current_active_user)])

ALLOWED_EXTENSIONS = {
    "csv",
    "parquet",
    "xlsx",
    "xls",
    "json",  # kept for the File Manager UI; the canvas drop never maps .json to a node
    "txt",
    "tsv",
    "ipc",
    "arrow",
    "feather",
    "ndjson",
    "jsonl",
    "avro",
}

MAX_FILE_SIZE = 500 * 1024 * 1024  # 500 MB
CHUNK_SIZE = 1024 * 1024
MAX_UNIQUE_ATTEMPTS = 1000


class UploadLimits(BaseModel):
    max_file_size_bytes: int
    allowed_extensions: list[str]


def _check_docker_mode() -> None:
    """Raise 403 if not running in Docker mode."""
    if os.environ.get("FLOWFILE_MODE") != "docker":
        raise HTTPException(403, "File manager is only available in Docker mode")


def _open_unique(uploads_dir: Path, safe_name: str) -> tuple[Path, int]:
    """Create and open the first free of "a.csv", "a (1).csv", "a (2).csv", ... returning (path, fd)."""
    stem, suffix = Path(safe_name).stem, Path(safe_name).suffix
    for attempt in range(MAX_UNIQUE_ATTEMPTS):
        candidate = uploads_dir / (safe_name if attempt == 0 else f"{stem} ({attempt}){suffix}")
        try:
            # O_EXCL so concurrent same-name uploads cannot win the same suffix.
            return candidate, os.open(candidate, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        except FileExistsError:
            continue
    raise HTTPException(409, f"Too many files named '{safe_name}'")


async def _write_upload(file: UploadFile, fd: int) -> int:
    """Stream the upload into an open fd, rejecting it the moment it grows past MAX_FILE_SIZE."""
    size = 0
    with os.fdopen(fd, "wb") as f:
        while chunk := await file.read(CHUNK_SIZE):
            size += len(chunk)
            if size > MAX_FILE_SIZE:
                raise HTTPException(400, f"File too large. Maximum size is {MAX_FILE_SIZE // (1024 * 1024)} MB")
            f.write(chunk)
    return size


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


@router.get("/limits", response_model=UploadLimits)
async def get_upload_limits() -> UploadLimits:
    """Report the upload constraints.

    Not mode-gated: the canvas file-drop shows these limits in every mode.
    """
    return UploadLimits(max_file_size_bytes=MAX_FILE_SIZE, allowed_extensions=sorted(ALLOWED_EXTENSIONS))


@router.post("/upload")
async def upload_file(file: UploadFile = File(...), unique: bool = False) -> JSONResponse:
    """Upload a file to the user data uploads directory.

    Available in every mode because the canvas file-drop uploads from package and
    electron modes too. Only permitted extensions are accepted and the size limit is
    enforced while streaming. With ``unique`` the destination is a non-colliding name
    instead of an overwrite; the response ``filename`` is always the final on-disk name.
    """
    if not file.filename:
        raise HTTPException(400, "No filename provided")

    # Sanitize filename - take only the basename and strip traversal attempts
    safe_name = Path(file.filename).name
    if not safe_name or ".." in safe_name:
        raise HTTPException(400, "Invalid filename")

    suffix = Path(safe_name).suffix.lstrip(".").lower()
    if suffix not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            400,
            f"File type '.{suffix}' not allowed. " f"Allowed types: {', '.join(sorted(ALLOWED_EXTENSIONS))}",
        )

    uploads_dir = storage.uploads_directory
    uploads_dir.mkdir(parents=True, exist_ok=True)

    if unique:
        file_location, fd = _open_unique(uploads_dir, safe_name)
        write_target = file_location
    else:
        # Stream into a hidden temp file and rename over the destination only on
        # success, so a rejected or failed upload never destroys an existing file.
        file_location = uploads_dir / safe_name
        write_target = uploads_dir / f".{safe_name[:40]}.{os.urandom(6).hex()}.part"
        fd = os.open(write_target, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)

    try:
        size = await _write_upload(file, fd)
        if not unique:
            os.replace(write_target, file_location)
    except BaseException:
        write_target.unlink(missing_ok=True)
        raise

    return JSONResponse(
        content={
            "filename": file_location.name,
            "filepath": str(file_location),
            "size": size,
        }
    )


@router.delete("/files/{filename}")
async def delete_file(filename: str) -> JSONResponse:
    """Delete a file from the user data uploads directory."""
    _check_docker_mode()

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
