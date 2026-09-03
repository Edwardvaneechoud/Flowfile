"""Admin endpoints for the catalog DB snapshots in ``db_backups/``.

Mounted under ``/system`` in :mod:`flowfile_core.main`, next to the worker-pool
proxy: admin-only, no trailing slashes (the renderer matches these byte-for-byte;
a trailing slash would 307 and drop the POST body in Docker). Errors carry a
typed ``{error_code, message}`` detail and never 401, which the axios interceptor
would treat as JWT expiry.

Listing is read-only: no restore, delete or download here — a restore is a
deliberate, app-stopped file copy documented for the user instead.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from flowfile_core.auth.jwt import get_current_admin_user
from flowfile_core.auth.models import User
from flowfile_core.database import backup
from flowfile_core.database.connection import get_database_path

router = APIRouter()


class DbBackupInput(BaseModel):
    """Request body for ``POST /system/db_backups``."""

    reason: Literal["manual", "pre_update"]


class DbBackupOut(BaseModel):
    """One snapshot file, described from its name and its ``db_info`` row."""

    file_name: str
    path: str
    size_bytes: int
    created_at: str
    kind: str
    from_revision: str | None = None
    to_revision: str | None = None
    app_version: str | None = None


class DbBackupsOut(BaseModel):
    """Everything the Backups tab renders."""

    directory: str
    keep: int
    enabled: bool
    backups: list[DbBackupOut]


def _db_path() -> Path:
    db_path = get_database_path()
    if db_path is None:
        raise HTTPException(
            status_code=503,
            detail={"error_code": "BACKUP_FAILED", "message": "This deployment does not use a file database."},
        )
    return db_path


@router.get("/db_backups", response_model=DbBackupsOut, tags=["system"])
def list_db_backups(_current_user: User = Depends(get_current_admin_user)) -> DbBackupsOut:
    """The snapshots of the catalog database, newest first."""
    db_path = _db_path()
    keep = backup.keep_count()
    return DbBackupsOut(
        directory=str(db_path.parent / backup.BACKUP_DIR_NAME),
        keep=keep,
        enabled=keep > 0,
        backups=[DbBackupOut(**snapshot) for snapshot in backup.list_snapshots(db_path)],
    )


@router.post("/db_backups", response_model=DbBackupOut, tags=["system"])
def create_db_backup(
    payload: DbBackupInput,
    _current_user: User = Depends(get_current_admin_user),
) -> DbBackupOut:
    """Take a snapshot now and return it. Subject to the same retention as migration snapshots."""
    db_path = _db_path()
    if backup.keep_count() <= 0:
        raise HTTPException(
            status_code=409,
            detail={
                "error_code": "BACKUPS_DISABLED",
                "message": "Database snapshots are disabled (FLOWFILE_DB_BACKUP_KEEP=0).",
            },
        )
    target = backup.create_snapshot(db_path, payload.reason)
    written = (
        next((s for s in backup.list_snapshots(db_path) if s["file_name"] == target.name), None)
        if target is not None
        else None
    )
    if written is None:
        raise HTTPException(
            status_code=503,
            detail={
                "error_code": "BACKUP_FAILED",
                "message": "Could not write a database snapshot; the database may be busy.",
            },
        )
    return DbBackupOut(**written)
