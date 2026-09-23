"""Inspect a Delta table at a bare object-storage path and turn on its change data feed.

The cloud storage reader and writer nodes address a Delta table by URI plus a saved cloud
connection instead of through the catalog, so these endpoints give their settings drawers
what the catalog routes give a catalog table: existence, head version, schema, partitioning,
change-tracking state with its enablement floor, and version history.

The connection resolves for the calling user exactly as in the storage browser (own first,
then group-granted; docker refuses ambient credentials). Every read is metadata only.
"""

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import polars as pl
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.configs import logger
from flowfile_core.database.connection import get_db
from flowfile_core.flowfile.flow_data_engine.cloud_storage_reader import CloudStorageReader
from flowfile_core.routes.storage_browser import _error, _resolve_connection
from flowfile_core.schemas.catalog_schema import ColumnSchema
from flowfile_core.schemas.cloud_storage_schemas import CloudStorageAuthMode, CloudStorageType
from shared.cloud_storage.uri import is_cloud_uri
from shared.cloud_storage.utils import normalize_delta_path
from shared.delta_models import DeltaVersionCommit
from shared.delta_utils import (
    delta_table_exists,
    enable_change_data_feed,
    format_delta_timestamp,
    get_change_data_feed_floor,
    get_delta_head_version,
    get_delta_partition_columns,
)

router = APIRouter()


class CloudDeltaTarget(BaseModel):
    """A Delta table location as the cloud storage nodes store it."""

    connection_name: str | None = Field(None, max_length=255)
    auth_mode: CloudStorageAuthMode = "auto"
    resource_path: str = Field(max_length=2048)


class CloudDeltaHistoryRequest(CloudDeltaTarget):
    limit: int = Field(100, ge=1, le=1000)


class CloudDeltaInfoOut(BaseModel):
    """State of the table at a path. ``exists=False`` leaves every other field empty.

    ``cdc_enabled_version`` is the change-data-feed floor derived from the Delta log: change
    reads must start at or above it.
    """

    exists: bool = False
    current_version: int | None = None
    partition_columns: list[str] = Field(default_factory=list)
    columns: list[ColumnSchema] = Field(default_factory=list)
    cdc_enabled: bool = False
    cdc_enabled_version: int | None = None


def _storage_type_of(resource_path: str) -> CloudStorageType:
    if resource_path.startswith(("s3://", "s3a://")):
        return "s3"
    if resource_path.startswith(("gs://", "gcs://")):
        return "gcs"
    return "adls"


def _resolve_target(db: Session, user_id: int, target: CloudDeltaTarget) -> tuple[str, dict[str, Any]]:
    """Validate *target* and resolve its connection for the caller -> (delta path, storage options).

    Only object-storage URIs are accepted: a local path would read the server's own disk.
    GCS is refused because delta-rs does not take Flowfile's gcsfs-style GCS options.
    """
    if not is_cloud_uri(target.resource_path):
        raise _error(400, "INVALID_PATH", "Enter an object-storage path such as s3://bucket/table.")
    storage_type = _storage_type_of(target.resource_path)
    connection = _resolve_connection(db, user_id, target.connection_name, storage_type)
    if connection.storage_type == "gcs" or storage_type == "gcs":
        raise _error(400, "GCS_UNSUPPORTED", "Delta table inspection is not supported on GCS yet.")
    if connection.connection_name is None and target.auth_mode == "aws-cli":
        connection = connection.model_copy(update={"auth_method": "aws-cli"})
    return normalize_delta_path(target.resource_path), CloudStorageReader.get_storage_options(connection)


def _scrub(message: str, storage_options: dict[str, Any]) -> str:
    """Mask connection values in provider error text: a grantee must never see the owner's secrets."""
    for value in storage_options.values():
        if isinstance(value, str) and len(value) >= 8:
            message = message.replace(value, "***")
    return message


@contextmanager
def _delta_errors(storage_options: dict[str, Any]) -> Iterator[None]:
    try:
        yield
    except HTTPException:
        raise
    except TableNotFoundError:
        raise _error(404, "NOT_A_DELTA_TABLE", "No Delta table exists at this path.") from None
    except Exception as exc:
        message = _scrub(str(exc), storage_options)
        logger.warning(f"Cloud Delta request failed: {message}")
        raise _error(400, "DELTA_ERROR", message) from None


def _read_info(path: str, storage_options: dict[str, Any]) -> CloudDeltaInfoOut:
    if not delta_table_exists(path, storage_options):
        return CloudDeltaInfoOut()
    schema = pl.scan_delta(path, storage_options=storage_options).collect_schema()
    floor = get_change_data_feed_floor(path, storage_options)
    return CloudDeltaInfoOut(
        exists=True,
        current_version=get_delta_head_version(path, storage_options),
        partition_columns=get_delta_partition_columns(path, storage_options),
        columns=[ColumnSchema(name=name, dtype=str(dtype)) for name, dtype in schema.items()],
        cdc_enabled=floor is not None,
        cdc_enabled_version=floor,
    )


@router.post("/info", response_model=CloudDeltaInfoOut)
def get_cloud_delta_info(
    target: CloudDeltaTarget,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> CloudDeltaInfoOut:
    """Existence, head version, schema, partitioning and change-tracking state of the table at a path."""
    path, storage_options = _resolve_target(db, current_user.id, target)
    with _delta_errors(storage_options):
        return _read_info(path, storage_options)


@router.post("/cdc/enable", response_model=CloudDeltaInfoOut)
def enable_cloud_delta_cdc(
    target: CloudDeltaTarget,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> CloudDeltaInfoOut:
    """Turn the change data feed on. Idempotent: one metadata commit, only when it was off."""
    path, storage_options = _resolve_target(db, current_user.id, target)
    with _delta_errors(storage_options):
        enable_change_data_feed(path, storage_options)
        return _read_info(path, storage_options)


@router.post("/history", response_model=list[DeltaVersionCommit])
def get_cloud_delta_history(
    body: CloudDeltaHistoryRequest,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> list[DeltaVersionCommit]:
    """The table's most recent commits, newest first."""
    path, storage_options = _resolve_target(db, current_user.id, body)
    with _delta_errors(storage_options):
        history = DeltaTable(path, without_files=True, storage_options=storage_options).history(body.limit)
    return [
        DeltaVersionCommit(
            version=entry.get("version"),
            timestamp=format_delta_timestamp(entry.get("timestamp")),
            operation=entry.get("operation"),
            parameters=entry.get("operationParameters"),
        )
        for entry in history
    ]
