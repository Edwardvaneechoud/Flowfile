"""Resolve where catalog table data lives — local filesystem or object storage.

This is the single seam for catalog storage resolution. Storage is resolved **per catalog**:
each level-0 catalog namespace may carry ``storage_uri`` + ``storage_connection_name`` columns,
and every schema/table beneath it inherits them (one namespace <-> one storage). An unset
``storage_uri`` (or ``namespace_id is None``) ⇒ the local filesystem, byte-for-byte as before.
Credentials resolve as the catalog owner. ``FLOWFILE_CATALOG_STORAGE_URI`` /
``FLOWFILE_CATALOG_STORAGE_CONNECTION`` survive only as an optional creation-time default
snapshotted onto a new catalog, never a live override.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from sqlalchemy.orm import Session

from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager.db_connections import get_cloud_connection_schema
from flowfile_core.flowfile.flow_data_engine.cloud_storage_reader import CloudStorageReader
from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnectionWorkerInterface
from shared.storage_config import storage

_CLOUD_URI_SCHEMES = ("s3://", "s3a://", "az://", "abfs://", "abfss://", "adl://", "gs://", "gcs://")
_CLOUD_URI_SCHEME_BYTES = tuple(s.encode() for s in _CLOUD_URI_SCHEMES)


def _is_cloud_uri(value: str) -> bool:
    """Return ``True`` when *value* is an object-storage URI rather than a local path."""
    return value.startswith(_CLOUD_URI_SCHEMES)


def serialized_frame_uses_cloud(blob: bytes | None) -> bool:
    """Return ``True`` when a serialized Polars LazyFrame embeds an object-storage scan.

    Polars serializes a scan's ``storage_options`` inline, so a cloud scan in the plan means
    the blob also carries that source's decrypted credentials. Such a blob must never be
    deserialized/replayed — the producer flow is re-run instead, resolving credentials fresh.
    Gates the optimized virtual-table cache on both the write and read paths.
    """
    if not blob:
        return False
    return any(scheme in blob for scheme in _CLOUD_URI_SCHEME_BYTES)


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


def _local_target() -> CatalogStorageTarget:
    """The default local-filesystem target, rooted at the catalog tables directory."""
    return CatalogStorageTarget(is_cloud=False, base=str(storage.catalog_tables_directory))


def _resolve_in_session(db: Session, namespace_id: int) -> CatalogStorageTarget:
    """Resolve the storage target for *namespace_id* using an open session."""
    from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository

    root = SQLAlchemyCatalogRepository(db).get_root_namespace(namespace_id)
    if root is None or not root.storage_uri:
        return _local_target()

    connection_name = root.storage_connection_name
    if not connection_name:
        raise ValueError(
            f"Catalog '{root.name}' has storage_uri set but no storage_connection_name; "
            "a cloud connection is required to resolve catalog storage credentials."
        )
    owner_id = root.owner_id
    conn = get_cloud_connection_schema(db, connection_name, owner_id)
    if conn is None:
        raise ValueError(
            f"Catalog storage connection '{connection_name}' was not found or is not accessible "
            f"for the catalog owner (user {owner_id})."
        )
    return CatalogStorageTarget(
        is_cloud=True,
        base=str(root.storage_uri).rstrip("/"),
        storage_options=CloudStorageReader.get_storage_options(conn),
        connection_name=connection_name,
        worker_interface=conn.get_worker_interface(owner_id),
    )


def resolve_for_namespace(namespace_id: int | None, *, db: Session | None = None) -> CatalogStorageTarget:
    """Resolve catalog storage for a namespace, inheriting from its level-0 root catalog.

    The root catalog's ``storage_uri``/``storage_connection_name`` columns decide the backend; an unset
    ``storage_uri`` (or ``namespace_id is None``) ⇒ the local filesystem, byte-for-byte as before.
    Credentials always resolve as the **catalog owner** (owner-keyed secrets), never the calling user,
    so an authorized writer uses the catalog's connection. Pass *db* to reuse the caller's session.
    """
    if namespace_id is None:
        return _local_target()
    if db is not None:
        return _resolve_in_session(db, namespace_id)
    with get_db_context() as own_db:
        return _resolve_in_session(own_db, namespace_id)


def resolve_catalog_storage(user_id: int, *, namespace_id: int | None = None) -> CatalogStorageTarget:
    """Backward-compatible shim around :func:`resolve_for_namespace`.

    *user_id* is retained for call-compatibility but ignored — per-catalog credentials always resolve
    as the catalog owner. New code should call :func:`resolve_for_namespace` directly.
    """
    return resolve_for_namespace(namespace_id)
