"""Resolve where catalog table data lives — local filesystem or object storage.

Storage is resolved per catalog: a level-0 namespace carries ``storage_uri`` +
``storage_connection_name``, and every schema/table beneath it inherits them. An unset
``storage_uri`` (or ``namespace_id is None``) ⇒ local filesystem. Credentials resolve as the
catalog owner.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from flowfile_core.configs.settings import FLOWFILE_COMPUTE_PROTOCOL
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager.db_connections import get_cloud_connection_schema
from flowfile_core.flowfile.execution.protocol import CredentialRef
from flowfile_core.flowfile.flow_data_engine.cloud_storage_reader import CloudStorageReader
from flowfile_core.notebook.lookup import metadata_lookup
from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnectionWorkerInterface
from shared.cloud_credential_provider import CredentialRefProvider
from shared.cloud_storage.uri import CLOUD_URI_SCHEMES as _CLOUD_URI_SCHEMES
from shared.storage_config import storage

_CLOUD_URI_SCHEME_BYTES = tuple(s.encode() for s in _CLOUD_URI_SCHEMES)


def _is_cloud_uri(value: str) -> bool:
    """Return ``True`` when *value* is an object-storage URI rather than a local path."""
    return value.startswith(_CLOUD_URI_SCHEMES)


def serialized_frame_uses_cloud(blob: bytes | None) -> bool:
    """Return ``True`` when a serialized Polars LazyFrame embeds an object-storage scan.

    Such a blob carries the source's credentials frozen at build time, so it must never be replayed
    (re-run the producer instead).
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


def _catalog_table_dir_name(path_or_uri: str) -> str:
    """Last path segment (the table directory name) of a local path or object-storage URI.

    Avoids ``Path(...).name`` on URIs, where ``Path`` would corrupt the ``scheme://`` prefix.
    """
    if _is_cloud_uri(path_or_uri):
        return path_or_uri.rstrip("/").rsplit("/", 1)[-1]
    return Path(path_or_uri).name


@dataclass(frozen=True)
class CatalogStorageTarget:
    """Where a catalog table's bytes live, plus the credentials to reach them.

    ``storage_options`` are decrypted for core's own in-process reads; ``worker_interface``
    keeps the connection owner-encrypted for the core→worker hand-off. With
    ``FLOWFILE_COMPUTE_PROTOCOL`` on, ``credential_ref`` names the namespace connection for the
    owner and ``plan_storage_options`` are its options without a secret: what a scan's plan
    carries (:meth:`scan_kwargs`).
    """

    is_cloud: bool
    base: str
    storage_options: dict[str, str] = field(default_factory=dict)
    connection_name: str | None = None
    worker_interface: FullCloudStorageConnectionWorkerInterface | None = None
    credential_ref: CredentialRef | None = None
    plan_storage_options: dict[str, str] = field(default_factory=dict)

    def to_worker_payload(self) -> dict | None:
        """Serialize a ``CatalogStorageInterface`` for the worker, or ``None`` for local.

        The worker joins ``base_uri`` with the bare table directory name and decrypts
        ``connection`` itself, so secrets never cross the wire in plaintext.
        """
        if not self.is_cloud or self.worker_interface is None:
            return None
        return {"base_uri": self.base, "connection": self.worker_interface.model_dump()}

    def scan_kwargs(self, user_id: int | None) -> dict[str, Any]:
        """``storage_options``/``credential_provider`` kwargs for a scan of this target whose plan may be serialized.

        With a ``credential_ref``, a new ``CredentialRefProvider`` on every call, so each plan gets its own (Polars
        caches resolved credentials by the provider's pickled bytes); the process that runs the plan resolves it.
        Otherwise the resolved options' credentials travel encrypted for *user_id*.
        """
        if self.credential_ref is not None:
            provider = CredentialRefProvider(self.credential_ref.connection_id, self.credential_ref.user_id)
            return CloudStorageReader.get_scan_kwargs(self.plan_storage_options, provider)
        return CloudStorageReader.get_secure_scan_kwargs(self.storage_options, user_id)


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
    credential_ref = None
    plan_storage_options: dict[str, str] = {}
    if FLOWFILE_COMPUTE_PROTOCOL:
        options, provider = CloudStorageReader.get_read_credentials(conn, owner_id)
        if isinstance(provider, CredentialRefProvider):
            credential_ref = CredentialRef(connection_id=provider.connection_id, user_id=provider.user_id)
            plan_storage_options = options
    return CatalogStorageTarget(
        is_cloud=True,
        base=str(root.storage_uri).rstrip("/"),
        storage_options=CloudStorageReader.get_storage_options(conn),
        connection_name=connection_name,
        worker_interface=conn.get_worker_interface(owner_id),
        credential_ref=credential_ref,
        plan_storage_options=plan_storage_options,
    )


def locate_for_namespace(namespace_id: int | None) -> CatalogStorageTarget:
    """Where a namespace's tables live, without resolving the storage connection: no credential is read.

    What a notebook kernel is told (``notebook.lookup``): ``is_cloud``, ``base`` and ``connection_name`` only,
    by the rules of :func:`resolve_for_namespace` up to the connection.
    """
    if namespace_id is None:
        return _local_target()
    from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository

    with get_db_context() as db:
        root = SQLAlchemyCatalogRepository(db).get_root_namespace(namespace_id)
        if root is None or not root.storage_uri:
            return _local_target()
        if not root.storage_connection_name:
            raise ValueError(
                f"Catalog '{root.name}' has storage_uri set but no storage_connection_name; "
                "a cloud connection is required to resolve catalog storage credentials."
            )
        return CatalogStorageTarget(
            is_cloud=True, base=str(root.storage_uri).rstrip("/"), connection_name=root.storage_connection_name
        )


def resolve_for_namespace(namespace_id: int | None, *, db: Session | None = None) -> CatalogStorageTarget:
    """Resolve catalog storage for a namespace, inheriting from its level-0 root catalog.

    Credentials always resolve as the catalog owner, never the calling user. Pass *db* to
    reuse the caller's session. In a notebook kernel session (``metadata_lookup`` set, never
    with *db*) core answers where the data lives without any credential: a cloud-backed table
    is held there and read by core.
    """
    if namespace_id is None:
        return _local_target()
    if db is not None:
        return _resolve_in_session(db, namespace_id)
    remote = metadata_lookup.get()
    if remote is not None:
        answer = remote("catalog_storage", {"namespace_id": namespace_id})
        if "error" in answer:
            raise ValueError(answer["error"])
        return CatalogStorageTarget(
            is_cloud=answer["is_cloud"], base=answer["base"], connection_name=answer.get("connection_name")
        )
    with get_db_context() as own_db:
        return _resolve_in_session(own_db, namespace_id)


def resolve_catalog_storage(_user_id: int, *, namespace_id: int | None = None) -> CatalogStorageTarget:
    """Shim around :func:`resolve_for_namespace`; *_user_id* is ignored (credentials resolve as the owner)."""
    return resolve_for_namespace(namespace_id)
