"""Browse object storage behind a saved cloud connection.

The local filesystem keeps its own ``/files/*`` endpoints; this router exists so the
same frontend browser component can also point at S3, ADLS, or GCS.

The connection is resolved for the *calling* user (own first, then group-granted)
while its secrets stay encrypted under the owner — the same shape the cloud reader
node already uses, so browsing never exposes a path the caller could not already
read through that connection.
"""

import os
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.configs import logger
from flowfile_core.database.connection import get_db
from flowfile_core.flowfile.database_connection_manager.db_connections import get_cloud_connection_schema
from flowfile_core.flowfile.flow_data_engine.cloud_storage_reader import CloudStorageReader
from flowfile_core.schemas.cloud_storage_schemas import CloudStorageType, FullCloudStorageConnection
from shared.cloud_storage.browse import (
    DEFAULT_PAGE_SIZE,
    MAX_PAGE_SIZE,
    BrowseEntry,
    BrowseError,
    EntryKind,
    browse_support,
    list_cloud_uri,
)
from shared.cloud_storage.uri import canonical_scheme, uri_is_root, uri_parent

router = APIRouter()

# AuthSettingsInput.connection_name defaults to the string "None"; treat it as unset.
_UNSET_CONNECTION_NAMES = (None, "", "None")


class CloudBrowseEntry(BaseModel):
    """One row, shaped like ``FileInfo`` so the existing browser can render it unchanged.

    ``last_modified`` and ``created_date`` are nullable rather than synthesized: object
    stores have no creation time, and prefixes have neither a size nor a modified time.
    Faking them would silently corrupt the browser's date sort.
    """

    name: str
    path: str
    is_directory: bool
    size: int = 0
    file_type: str = ""
    last_modified: datetime | None = None
    created_date: datetime | None = None
    is_hidden: bool = False
    exists: bool = True
    entry_kind: EntryKind


class CloudBrowseResponse(BaseModel):
    """A page of a listing.

    Entries are ordered within the page only: S3 and ADLS return prefixes and objects
    as separate per-page arrays, so a later page can hold directories that sort before
    an earlier page's files.
    """

    path: str
    parent_path: str | None = None
    is_root: bool = False
    entries: list[CloudBrowseEntry] = []
    truncated: bool = False
    next_token: str | None = None
    root_listing_denied: bool = False
    current_is_delta_table: bool = False


def ambient_credentials_allowed() -> bool:
    """Whether browsing may fall back to the process's own cloud credentials.

    In docker the credentials belong to the server, so letting any authenticated user
    enumerate with them would hand out reconnaissance over the whole deployment.
    Electron and package mode run on the caller's own machine, where they are already
    the caller's own credentials.

    Read live rather than through ``configs.settings``, which caches FLOWFILE_MODE at
    import time (cf. ``auth.sharing.sharing_enabled``).
    """
    return os.environ.get("FLOWFILE_MODE", "electron") != "docker"


def _error(status_code: int, error_code: str, message: str) -> HTTPException:
    """Build a typed error. Never 401 — the axios interceptor reads that as JWT expiry."""
    return HTTPException(status_code=status_code, detail={"error_code": error_code, "message": message})


def _resolve_connection(
    db: Session, user_id: int, connection_name: str | None, storage_type: CloudStorageType | None
) -> FullCloudStorageConnection:
    if connection_name not in _UNSET_CONNECTION_NAMES:
        connection = get_cloud_connection_schema(db, connection_name, user_id)
        if connection is None:
            # Same 404 whether it is missing or simply not shared with this user.
            raise _error(404, "CONNECTION_NOT_FOUND", "Cloud connection not found.")
        return connection

    if not ambient_credentials_allowed():
        raise _error(400, "CONNECTION_REQUIRED", "Select a cloud connection to browse object storage.")
    if storage_type not in (None, "s3"):
        raise _error(
            400,
            "CONNECTION_REQUIRED",
            f"Select a cloud connection to browse {storage_type} storage.",
        )
    return FullCloudStorageConnection(storage_type="s3", auth_method="env_vars", connection_name=None)


def _to_response_entry(entry: BrowseEntry) -> CloudBrowseEntry:
    file_type = ""
    if not entry.is_directory and "." in entry.name:
        file_type = entry.name.rsplit(".", 1)[-1].lower()
    return CloudBrowseEntry(
        name=entry.name,
        path=entry.uri,
        is_directory=entry.is_directory,
        size=entry.size or 0,
        file_type=file_type,
        last_modified=entry.last_modified,
        is_hidden=entry.is_hidden,
        entry_kind=entry.entry_kind,
    )


@router.get("/cloud", response_model=CloudBrowseResponse, tags=["storage_browser"])
def browse_cloud_storage(
    path: str = Query("", max_length=2048),
    connection_name: str | None = Query(None, max_length=255),
    storage_type: CloudStorageType | None = Query(None),
    page_token: str | None = Query(None, max_length=4096),
    page_size: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> CloudBrowseResponse:
    """List one level of an object-storage location. An empty *path* lists buckets."""
    connection = _resolve_connection(db, current_user.id, connection_name, storage_type)

    supported, reason = browse_support(connection.storage_type, connection.auth_method)
    if not supported:
        raise _error(409, "BROWSE_UNSUPPORTED_AUTH", reason)

    storage_options = CloudStorageReader.get_storage_options(connection)
    try:
        result = list_cloud_uri(
            connection.storage_type, path, storage_options, page_size=page_size, page_token=page_token
        )
    except BrowseError as exc:
        # exc carries an already-sanitized message; provider text can embed endpoints and SAS tokens.
        logger.warning(f"Cloud browse failed ({connection.storage_type}): {exc.error_code}")
        raise _error(exc.status_code, exc.error_code, str(exc)) from None
    except ValueError as exc:
        raise _error(400, "INVALID_PATH", str(exc)) from None

    current_path = result.path or canonical_scheme(connection.storage_type)
    at_root = uri_is_root(current_path)
    return CloudBrowseResponse(
        path=current_path,
        parent_path=None if at_root else uri_parent(current_path),
        is_root=at_root,
        entries=[_to_response_entry(entry) for entry in result.entries],
        truncated=result.truncated,
        next_token=result.next_token,
        root_listing_denied=result.root_listing_denied,
        current_is_delta_table=result.current_is_delta_table,
    )
