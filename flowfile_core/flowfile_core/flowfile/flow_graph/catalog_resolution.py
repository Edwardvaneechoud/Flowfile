"""Catalog lookups while building and running: table and SQL-view resolution, access checks, virtual plans."""

import io as _io
import json
from pathlib import Path
from typing import Literal, NamedTuple

import polars as pl

from flowfile_core.auth import sharing
from flowfile_core.catalog import CatalogService
from flowfile_core.catalog.delta_utils import (
    check_source_versions_current,
    is_delta_table,
)
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.catalog.storage_backend import _is_cloud_uri, serialized_frame_uses_cloud
from flowfile_core.configs import logger
from flowfile_core.configs.flow_logger import NodeLogger
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.flow_graph._root import root
from flowfile_core.notebook.lookup import metadata_lookup
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.catalog_schema import scd2_system_columns_missing


def _resolve_virtual_table(
    is_optimized: bool,
    serialized_lf: bytes | None,
    catalog_table_id: int,
    run_location: Literal["remote", "local"] | None = None,
    node_logger: NodeLogger = None,
    source_table_versions: str | None = None,
    user_id: int | None = None,
) -> pl.LazyFrame:
    """Resolve a virtual table to a LazyFrame.

    Optimized tables deserialize a stored execution plan if source table
    versions are still current; otherwise falls back to re-executing
    the producer flow via CatalogService.

    ``user_id`` (the flow's executing principal) is threaded into
    ``resolve_virtual_flow_table`` so a query virtual table's referenced-table
    grants are enforced during flow execution, not just on the API path. ``None``
    means an internal/unscoped run (scheduler, CLI, electron) → unrestricted.
    """
    if (
        is_optimized
        and serialized_lf
        and not serialized_frame_uses_cloud(serialized_lf)
        and check_source_versions_current(source_table_versions)
    ):
        return pl.LazyFrame.deserialize(_io.BytesIO(serialized_lf))
    with root().get_db_context() as db:
        repo = SQLAlchemyCatalogRepository(db)
        svc = root().CatalogService(repo)
        return svc.resolve_virtual_flow_table(
            catalog_table_id, user_id=user_id, run_location=run_location, node_logger=node_logger
        )


class CatalogSqlTables(NamedTuple):
    """Resolved catalog tables for SQL execution."""

    table_paths: dict[str, str]
    virtual_tables: dict[str, tuple[bool, bytes | None, int, str | None]]
    # Each physical table's namespace_id, for per-catalog storage resolution.
    table_namespaces: dict[str, int | None]


def _accessible_catalog_table_ids(db, user_id: int | None) -> set[int] | None:
    """Catalog-table ids the executing principal may read, or ``None`` for unrestricted.

    ``None`` means no filtering: internal/scheduler/CLI runs (``user_id is None``),
    electron/single-user mode (sharing disabled), or an admin. A ``user_id`` that
    doesn't resolve to a ``User`` row denies everything (a stale/forged id must
    never widen access).
    """
    if user_id is None or not sharing.sharing_enabled():
        return None
    user = db.get(db_models.User, user_id)
    if user is None:
        return set()
    if getattr(user, "is_admin", False):
        return None
    from flowfile_core.catalog.access import AccessResolver

    return AccessResolver(db, user).accessible_ids("catalog_table")


def _authorize_catalog_write(db, user_id: int | None, *, existing, namespace_id: int | None) -> None:
    """Raise ``PermissionError`` when the executing principal may not write the target.

    Mirrors ``_accessible_catalog_table_ids`` semantics: ``user_id is None``
    (internal/scheduler/CLI runs) or sharing disabled means unrestricted; an id
    that doesn't resolve to a ``User`` fails closed; admins bypass. An existing
    table requires manage on the table (parity with ``overwrite_table_data``); a
    new table requires the target namespace to be writable — public, owned, or
    manage-granted — with ``namespace_id is None`` skipped exactly like
    ``_require_namespace_writable`` (the write then lands in the seeded public
    default namespace).
    """
    if user_id is None or not sharing.sharing_enabled():
        return
    user = db.get(db_models.User, user_id)
    if user is None:
        raise PermissionError("Executing user could not be resolved; refusing catalog write")
    if getattr(user, "is_admin", False):
        return
    from flowfile_core.catalog.access import AccessResolver

    resolver = AccessResolver(db, user)
    if existing is not None:
        if not resolver.can_manage("catalog_table", existing.id, owner_id=existing.owner_id):
            raise PermissionError(f"Not authorized to overwrite catalog table '{existing.name}'")
    elif namespace_id is not None and namespace_id not in resolver.writable_namespace_ids():
        raise PermissionError("Not authorized to create a table in the target catalog namespace")


def _resolve_catalog_sql_tables(node_id: int | str, user_id: int | None = None) -> CatalogSqlTables:
    """Resolve all catalog tables (physical Delta + virtual) for a SQL query node.

    In multi-user mode the registered tables are restricted to those the
    executing ``user_id`` may read, mirroring ``CatalogService.execute_sql_query``;
    a query referencing an inaccessible table simply finds it unregistered.
    """
    remote = metadata_lookup.get()
    if remote is not None:
        answer = remote("catalog_sql_tables", {"node_id": node_id})
        return CatalogSqlTables(
            table_paths=dict(answer["table_paths"]),
            virtual_tables={name: tuple(entry) for name, entry in answer["virtual_tables"].items()},
            table_namespaces=dict(answer["table_namespaces"]),
        )
    table_paths: dict[str, str] = {}
    virtual_tables: dict[str, tuple[bool, bytes | None, int, str | None]] = {}
    table_namespaces: dict[str, int | None] = {}
    try:
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            accessible = _accessible_catalog_table_ids(db, user_id)
            seen_names: set[str] = set()
            for t in repo.list_tables():
                if accessible is not None and t.id not in accessible:
                    continue
                if t.name in seen_names:
                    logger.warning(
                        "Duplicate table name %r in catalog SQL context for node %s; "
                        "later entry will overwrite the earlier one",
                        t.name,
                        node_id,
                    )
                seen_names.add(t.name)
                if t.table_type == "virtual":
                    virtual_tables[t.name] = (
                        t.is_optimized,
                        t.serialized_lazy_frame,
                        t.id,
                        t.source_table_versions,
                    )
                elif t.file_path and (_is_cloud_uri(t.file_path) or is_delta_table(Path(t.file_path))):
                    table_paths[t.name] = t.file_path
                    table_namespaces[t.name] = t.namespace_id

    except Exception:
        logger.warning(
            "Could not resolve catalog tables for SQL node %s",
            node_id,
            exc_info=True,
        )
    return CatalogSqlTables(table_paths=table_paths, virtual_tables=virtual_tables, table_namespaces=table_namespaces)


class CatalogTableInfo(NamedTuple):
    """Resolved catalog table info for a single-table reader."""

    file_path: str | None
    table_type: str
    serialized_lf: bytes | None
    is_optimized: bool
    source_table_versions: str | None = None
    # False when the principal may not read the table; the reader then fails closed.
    authorized: bool = True
    # Resolved identity, surfaced for back-filling name-only readers.
    table_id: int | None = None
    namespace_id: int | None = None
    table_name: str | None = None
    # SCD2 shape off the catalog record (source of truth for generated names); None => not SCD2.
    scd2_config: dict | None = None
    # Persisted column schema (JSON [{name, dtype}]) to predict a change reader without opening the feed.
    schema_json: str | None = None


_CDF_COLUMN_DTYPES = (
    ("_change_type", "String"),
    ("_commit_version", "UInt64"),
    ("_commit_timestamp", "Datetime(time_unit='ms', time_zone=None)"),
)
"""Dtypes ``load_cdf`` gives the three change columns, for schema prediction."""


def _scd2_config_is_stale(cfg: dict, table_record) -> bool:
    """True when a table's persisted SCD2 config names columns its stored schema no longer has.

    Defence in depth for a record that outlived its data (a kernel or ``/refresh`` write that
    replaced an SCD2 table wholesale). Degrading to an unfiltered read is far better than a
    ``ColumnNotFoundError`` at collect time; an unreadable or absent schema is left alone rather
    than guessed at.
    """
    raw_schema = getattr(table_record, "schema_json", None)
    if not raw_schema:
        return False
    try:
        columns = json.loads(raw_schema)
    except (TypeError, ValueError):
        return False
    if not columns:
        return False
    return scd2_system_columns_missing(cfg, columns)


def _resolve_catalog_table_info(node_catalog_reader: "input_schema.NodeCatalogReader") -> CatalogTableInfo:
    """Resolve a single catalog table (physical or virtual) for a table reader node.

    In a notebook kernel session (``metadata_lookup`` set) core answers from the same function, without the
    table's plan: the kernel opens no database connection and a virtual read is held there.
    """
    remote = metadata_lookup.get()
    if remote is not None:
        return CatalogTableInfo(
            **remote(
                "catalog_table",
                {
                    "node_id": node_catalog_reader.node_id,
                    "catalog_table_id": node_catalog_reader.catalog_table_id,
                    "catalog_full_table_name": node_catalog_reader.catalog_full_table_name,
                    "catalog_table_name": node_catalog_reader.catalog_table_name,
                    "catalog_namespace_id": node_catalog_reader.catalog_namespace_id,
                },
            )
        )
    file_path: str | None = None
    table_type: str = "physical"
    serialized_lf: bytes | None = None
    is_optimized: bool = False
    source_table_versions: str | None = None
    resolved_table_id: int | None = None
    resolved_namespace_id: int | None = None
    resolved_table_name: str | None = None
    resolved_scd2_config: dict | None = None
    resolved_schema_json: str | None = None
    try:
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)

            table_record = None
            if node_catalog_reader.catalog_table_id:
                table_record = repo.get_table(node_catalog_reader.catalog_table_id)
            else:
                reference = node_catalog_reader.catalog_full_table_name or node_catalog_reader.catalog_table_name
                if reference:
                    try:
                        table_record = svc.resolve_table(
                            reference,
                            default_namespace_id=node_catalog_reader.catalog_namespace_id,
                        )
                    except Exception:
                        logger.warning(
                            "Could not resolve catalog table reference %r (ns=%s) for node %s",
                            reference,
                            node_catalog_reader.catalog_namespace_id,
                            node_catalog_reader.node_id,
                            exc_info=True,
                        )

            # Authorize node.user_id (server-stamped) against the table; None => unrestricted internal run.
            effective_id = table_record.id if table_record is not None else node_catalog_reader.catalog_table_id
            if effective_id is not None:
                if not sharing.user_id_can_use(db, node_catalog_reader.user_id, "catalog_table", effective_id):
                    return CatalogTableInfo(None, "physical", None, False, None, authorized=False)
            elif sharing.sharing_enabled() and node_catalog_reader.user_id is not None:
                # Restricted run with no table id to authorize: fail closed rather than fall through to a path.
                return CatalogTableInfo(None, "physical", None, False, None, authorized=False)

            if table_record is not None:
                resolved_table_id = table_record.id
                resolved_namespace_id = table_record.namespace_id
                resolved_table_name = table_record.name
                table_type = table_record.table_type
                if table_type == "virtual":
                    is_optimized = table_record.is_optimized
                    serialized_lf = table_record.serialized_lazy_frame
                    source_table_versions = table_record.source_table_versions
                else:
                    file_path = table_record.file_path
                    resolved_schema_json = table_record.schema_json
                    raw_scd2_config = getattr(table_record, "scd2_config", None)
                    if raw_scd2_config:
                        try:
                            resolved_scd2_config = json.loads(raw_scd2_config)
                        except (TypeError, ValueError):
                            logger.warning("Unreadable scd2_config on catalog table %s", table_record.id)
                    if resolved_scd2_config and _scd2_config_is_stale(resolved_scd2_config, table_record):
                        logger.warning(
                            "Catalog table %s claims to be SCD2 but its schema no longer carries the "
                            "configured system columns; reading it unfiltered",
                            table_record.id,
                        )
                        resolved_scd2_config = None
            else:
                resolved_namespace_id = node_catalog_reader.catalog_namespace_id
                file_path = svc.resolve_table_file_path(
                    table_id=node_catalog_reader.catalog_table_id,
                    table_name=node_catalog_reader.catalog_table_name,
                    namespace_id=node_catalog_reader.catalog_namespace_id,
                )
    except Exception:
        logger.warning("Could not resolve catalog table for node %s", node_catalog_reader.node_id, exc_info=True)
    return CatalogTableInfo(
        file_path=file_path,
        table_type=table_type,
        serialized_lf=serialized_lf,
        is_optimized=is_optimized,
        source_table_versions=source_table_versions,
        table_id=resolved_table_id,
        namespace_id=resolved_namespace_id,
        table_name=resolved_table_name,
        scd2_config=resolved_scd2_config,
        schema_json=resolved_schema_json,
    )


def _effective_namespace_id(svc: CatalogService, settings) -> int | None:
    """Resolve a node's target namespace name-first (portable ``namespace_full_name``), falling back to
    the numeric ``namespace_id`` for flows saved before names were stored. The numeric id is install-local
    and goes stale when namespaces are recreated, so the name is preferred whenever present."""
    resolved = svc.resolve_namespace_id_by_full_name(getattr(settings, "namespace_full_name", None))
    return resolved if resolved is not None else settings.namespace_id
