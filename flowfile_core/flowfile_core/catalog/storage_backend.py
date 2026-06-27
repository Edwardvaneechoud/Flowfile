"""Resolve where catalog table data lives — local filesystem or object storage.

This is the single seam for catalog storage resolution. v1 reads one instance-wide
object-storage target from env config (``FLOWFILE_CATALOG_STORAGE_URI`` +
``FLOWFILE_CATALOG_STORAGE_CONNECTION``); when unset the catalog behaves exactly as
before (local filesystem). Per-namespace / per-table resolution is an additive change
to ``resolve_catalog_storage`` only — the ``namespace_id`` hook is accepted now and
ignored in v1, and no call site changes when it is wired up.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from flowfile_core.configs import settings
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager.db_connections import get_cloud_connection_schema
from flowfile_core.flowfile.flow_data_engine.cloud_storage_reader import CloudStorageReader
from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnectionWorkerInterface
from shared.storage_config import storage

_CLOUD_URI_SCHEMES = ("s3://", "s3a://", "az://", "abfs://", "abfss://", "adl://", "gs://", "gcs://")


def _is_cloud_uri(value: str) -> bool:
    """Return ``True`` when *value* is an object-storage URI rather than a local path."""
    return value.startswith(_CLOUD_URI_SCHEMES)


def join_catalog_uri(base: str, dir_name: str) -> str:
    """Join a catalog table directory name onto a base, for cloud or local roots.

    Mirrors how the worker resolves a table: cloud bases keep the URI scheme intact,
    local bases produce a filesystem path.
    """
    if _is_cloud_uri(base):
        return base.rstrip("/") + "/" + dir_name
    return str(Path(base) / dir_name)


@dataclass(frozen=True)
class CatalogStorageTarget:
    """Where a catalog table's bytes live, plus the credentials to reach them.

    ``storage_options`` are decrypted, for core's own in-process lazy reads (reader /
    SQL nodes). ``worker_interface`` carries the same connection with owner-encrypted
    secrets for the core→worker hand-off (the worker decrypts independently).
    """

    is_cloud: bool
    base: str
    storage_options: dict[str, str] = field(default_factory=dict)
    connection_name: str | None = None
    worker_interface: FullCloudStorageConnectionWorkerInterface | None = None

    def to_worker_payload(self) -> dict | None:
        """Serialize a ``CatalogStorageInterface`` for the worker, or ``None`` for local.

        The worker joins ``base_uri`` with the bare table directory name and decrypts
        ``connection`` itself, so secrets never cross the wire in plaintext.
        """
        if not self.is_cloud or self.worker_interface is None:
            return None
        return {"base_uri": self.base, "connection": self.worker_interface.model_dump()}


def resolve_catalog_storage(user_id: int, *, namespace_id: int | None = None) -> CatalogStorageTarget:
    """Resolve the catalog storage backend for *user_id*.

    Unset config ⇒ a local target rooted at ``storage.catalog_tables_directory`` (today's
    behavior, byte-for-byte). When ``FLOWFILE_CATALOG_STORAGE_URI`` is set, the named
    ``CloudStorageConnection`` supplies credentials; a missing URI-companion connection,
    or a connection name that does not resolve for the user, raises ``ValueError``.

    *namespace_id* is accepted for the future per-schema backend and ignored in v1.
    """
    uri = settings.get_catalog_storage_uri()
    if not uri:
        return CatalogStorageTarget(is_cloud=False, base=str(storage.catalog_tables_directory))

    connection_name = settings.get_catalog_storage_connection()
    if not connection_name:
        raise ValueError(
            "FLOWFILE_CATALOG_STORAGE_URI is set but FLOWFILE_CATALOG_STORAGE_CONNECTION is not; "
            "a cloud connection name is required to resolve catalog storage credentials."
        )

    with get_db_context() as db:
        conn = get_cloud_connection_schema(db, connection_name, user_id)
    if conn is None:
        raise ValueError(
            f"Catalog storage connection '{connection_name}' was not found or is not accessible " f"for user {user_id}."
        )

    return CatalogStorageTarget(
        is_cloud=True,
        base=uri.rstrip("/"),
        storage_options=CloudStorageReader.get_storage_options(conn),
        connection_name=connection_name,
        worker_interface=conn.get_worker_interface(user_id),
    )
