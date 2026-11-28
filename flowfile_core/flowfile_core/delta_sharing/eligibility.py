"""Which catalog tables may be served over Delta Sharing, asked when a table is added and on every request."""

from __future__ import annotations

from sqlalchemy.orm import Session

from flowfile_core.auth.jwt import active_db_user
from flowfile_core.catalog.access import AccessResolver
from flowfile_core.catalog.storage_backend import CatalogStorageTarget, resolve_for_table
from flowfile_core.database.models import CatalogTable
from flowfile_core.delta_sharing import log_reader
from flowfile_core.delta_sharing.presign import signing_refusal


def row_refusal(table: CatalogTable) -> str | None:
    """Why the table's catalog row alone rules it out (no storage is read), or ``None``."""
    if table.table_type != "physical":
        return "Virtual tables cannot be shared; write the data to a table first."
    if table.storage_format != "delta":
        if table.storage_format == "iceberg":
            return "Tables kept in another catalog cannot be shared from Flowfile."
        return "Only Delta tables can be shared; this table is stored as legacy Parquet."
    if not table.file_path:
        return "The table has no stored data."
    return None


def storage_target(table: CatalogTable) -> CatalogStorageTarget:
    return resolve_for_table(table.file_path, table.namespace_id)


def full_refusal(table: CatalogTable) -> str | None:
    """:func:`row_refusal`, then what its storage and Delta log say, or ``None`` when the table can be served."""
    refusal = row_refusal(table)
    if refusal is not None:
        return refusal
    try:
        target = storage_target(table)
    except ValueError as exc:
        return str(exc)
    refusal = signing_refusal(target, table.file_path)
    if refusal is not None:
        return refusal
    try:
        delta = log_reader.open_table(table.file_path, target.storage_options)
    except Exception:
        return "The table's Delta log could not be read."
    return log_reader.reader_refusal(delta)


def owner_manages(db: Session, owner_id: int, table: CatalogTable) -> bool:
    """Whether the share's owner is active and still manages *table*: a share serves nothing its owner lost."""
    owner = active_db_user(db, owner_id)
    if owner is None:
        return False
    return AccessResolver(db, owner).can_manage("catalog_table", table.id, table.owner_id)
