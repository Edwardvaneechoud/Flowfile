"""Catalog lookups while building and running: table and SQL-view resolution, access checks, virtual plans."""

import hashlib
import json
import re
from pathlib import Path
from typing import Literal, NamedTuple

import polars as pl

from flowfile_core.auth import sharing
from flowfile_core.catalog import CatalogService, table_sources
from flowfile_core.catalog.delta_utils import is_delta_table
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.catalog.storage_backend import _is_cloud_uri
from flowfile_core.catalog.text_utils import is_table_reference
from flowfile_core.configs import logger
from flowfile_core.configs.flow_logger import NodeLogger
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.flow_graph._root import root
from flowfile_core.notebook.lookup import metadata_lookup
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.catalog_schema import scd2_system_columns_missing


def _resolve_virtual_table(
    catalog_table_id: int,
    run_location: Literal["remote", "local"] | None = None,
    node_logger: NodeLogger = None,
    user_id: int | None = None,
) -> pl.LazyFrame:
    """Resolve a virtual table to a LazyFrame by running its producer flow's writer node (CatalogService).

    ``user_id`` (the flow's executing principal) is threaded into
    ``resolve_virtual_flow_table`` so a query virtual table's referenced-table
    grants are enforced during flow execution, not just on the API path. ``None``
    means an internal/unscoped run (scheduler, CLI, electron) → unrestricted.
    """
    with root().get_db_context() as db:
        repo = SQLAlchemyCatalogRepository(db)
        svc = root().CatalogService(repo)
        return svc.resolve_virtual_flow_table(
            catalog_table_id, user_id=user_id, run_location=run_location, node_logger=node_logger
        )


def _producer_fingerprint(repo: SQLAlchemyCatalogRepository, table) -> str | None:
    """sha256 of a lazy virtual table's producer flow file, which its output follows; else None.

    Reads resolve the producer from its saved file, so an unsaved edit in an open editor is not seen
    here either.
    """
    if not table.is_optimized or not table.producer_registration_id:
        return None
    registration = repo.get_flow(table.producer_registration_id)
    if registration is None or not registration.flow_path:
        return None
    try:
        return hashlib.sha256(Path(registration.flow_path).read_bytes()).hexdigest()
    except OSError:
        logger.debug("Producer flow file of virtual table %s is unreadable; reruns it every run", table.id)
        return None


class CatalogSqlTables(NamedTuple):
    """Resolved catalog tables for SQL execution."""

    table_paths: dict[str, str]
    # name -> (is_optimized, producer_fingerprint, table_id, source_table_versions)
    virtual_tables: dict[str, tuple[bool, str | None, int, str | None]]
    # Each physical table's namespace_id, for per-catalog storage resolution.
    table_namespaces: dict[str, int | None]
    materialized_tables: dict[str, dict[str, str]] | None = None


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
    from flowfile_core.catalog.exceptions import KeptElsewhereError
    from flowfile_core.catalog.services.external_catalogs import refuse_kept_elsewhere

    try:
        refuse_kept_elsewhere(db, namespace_id, "save anything into it")
    except KeptElsewhereError as exc:
        raise PermissionError(str(exc)) from None
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


def _resolve_catalog_sql_tables(
    node_id: int | str, user_id: int | None = None, *, sql_query: str | None = None
) -> CatalogSqlTables:
    """Resolve all catalog tables (physical Delta + virtual) for a SQL query node.

    In multi-user mode the registered tables are restricted to those the
    executing ``user_id`` may read, mirroring ``CatalogService.execute_sql_query``;
    a query referencing an inaccessible table simply finds it unregistered.
    Given ``sql_query``, only the virtual tables it references are resolved, since
    each one is fingerprinted here and runs its producer when read.
    """
    remote = metadata_lookup.get()
    if remote is not None:
        args = {"node_id": node_id} if sql_query is None else {"node_id": node_id, "sql_query": sql_query}
        answer = remote("catalog_sql_tables", args)
        return CatalogSqlTables(
            table_paths=dict(answer["table_paths"]),
            virtual_tables={name: tuple(entry) for name, entry in answer["virtual_tables"].items()},
            table_namespaces=dict(answer["table_namespaces"]),
            materialized_tables=answer.get("materialized_tables"),
        )
    table_paths: dict[str, str] = {}
    virtual_tables: dict[str, tuple[bool, str | None, int, str | None]] = {}
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
                    if sql_query is not None and not is_table_reference(t.name, sql_query):
                        continue
                    virtual_tables[t.name] = (
                        t.is_optimized,
                        _producer_fingerprint(repo, t),
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
    producer_fingerprint: str | None
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
    materialized: dict[str, str] | None = None
    cdc_enabled: bool = False
    cdc_enabled_version: int | None = None
    # ``catalog.schema.table`` when the table is kept by another catalog; its columns are that catalog's to say.
    kept_elsewhere: str | None = None


_CDF_COLUMN_DTYPES = (
    ("_change_type", "String"),
    ("_commit_version", "UInt64"),
    ("_commit_timestamp", "Datetime(time_unit='ms', time_zone=None)"),
)
"""Dtypes ``load_cdf`` gives the three change columns, for schema prediction."""


def _scd2_config_is_stale(cfg: dict, raw_schema: str | None) -> bool:
    """True when a table's persisted SCD2 config names columns its stored schema no longer has.

    Defence in depth for a record that outlived its data (a kernel or ``/refresh`` write that
    replaced an SCD2 table wholesale). Degrading to an unfiltered read is far better than a
    ``ColumnNotFoundError`` at collect time; an unreadable or absent schema is left alone rather
    than guessed at.
    """
    if not raw_schema:
        return False
    try:
        columns = json.loads(raw_schema)
    except (TypeError, ValueError):
        return False
    if not columns:
        return False
    return scd2_system_columns_missing(cfg, columns)


_MATERIALIZE_FAILED = re.compile(r"Materializing catalog view '.*' failed: (?P<body>\{.*\})", re.DOTALL)


def _core_refusal(exc: Exception) -> str | None:
    """The sentence core refused a catalog page cell's lookup with, else ``None``.

    The kernel runtime asks core's ``GET /catalog/tables/resolve`` and raises a refusal as an HTTP status error
    carrying the response; core refuses a table kept elsewhere with a 422 whose ``detail`` is one sentence (a mistyped
    table, a schema not yet in Flowfile's catalog, no Use). A refused ``POST .../materialize`` (what the worker was
    refused while writing the snapshot) the runtime raises as ``RuntimeError("Materializing catalog view '<name>'
    failed: <core's answer>")``, whose string ``detail`` is the sentence. Any other error is left as it is.
    """
    if isinstance(exc, RuntimeError) and getattr(exc, "response", None) is None:
        matched = _MATERIALIZE_FAILED.fullmatch(str(exc))
        try:
            detail = json.loads(matched["body"]).get("detail") if matched else None
        except (ValueError, AttributeError):
            return None
        return detail if isinstance(detail, str) else None
    response = getattr(exc, "response", None)
    if getattr(response, "status_code", None) != 422:
        return None
    try:
        detail = response.json().get("detail")
    except (ValueError, AttributeError):
        return None
    return detail if isinstance(detail, str) else None


def _resolve_catalog_table_info(node_catalog_reader: "input_schema.NodeCatalogReader") -> CatalogTableInfo:
    """Resolve a single catalog table (physical or virtual) for a table reader node.

    In a notebook kernel session (``metadata_lookup`` set) core answers from the same function, without the
    table's plan: the kernel opens no database connection and a virtual read is held there.
    """
    remote = metadata_lookup.get()
    if remote is not None:
        args = {
            "node_id": node_catalog_reader.node_id,
            "catalog_table_id": node_catalog_reader.catalog_table_id,
            "catalog_full_table_name": node_catalog_reader.catalog_full_table_name,
            "catalog_table_name": node_catalog_reader.catalog_table_name,
            "catalog_namespace_id": node_catalog_reader.catalog_namespace_id,
        }
        try:
            answer = remote("catalog_table", args)
        except Exception as exc:
            sentence = _core_refusal(exc)
            if sentence is None:
                raise
            raise ValueError(sentence) from None
        info = CatalogTableInfo(**answer)
        if info.scd2_config and _scd2_config_is_stale(info.scd2_config, info.schema_json):
            return info._replace(scd2_config=None)
        return info
    file_path: str | None = None
    table_type: str = "physical"
    producer_fingerprint: str | None = None
    is_optimized: bool = False
    source_table_versions: str | None = None
    resolved_table_id: int | None = None
    resolved_namespace_id: int | None = None
    resolved_table_name: str | None = None
    resolved_scd2_config: dict | None = None
    resolved_schema_json: str | None = None
    cdc_enabled = False
    cdc_enabled_version = None
    try:
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)

            table_record = None
            if not node_catalog_reader.catalog_table_id:
                outside = _outside_table_of(db, node_catalog_reader)
                if outside is not None:
                    return _outside_table_info(db, node_catalog_reader.user_id, outside)
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
                cdc_enabled = table_record.cdc_enabled
                cdc_enabled_version = table_record.cdc_enabled_version
                if table_type == "virtual":
                    is_optimized = table_record.is_optimized
                    producer_fingerprint = _producer_fingerprint(repo, table_record)
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
                    if resolved_scd2_config and _scd2_config_is_stale(resolved_scd2_config, resolved_schema_json):
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
        producer_fingerprint=producer_fingerprint,
        is_optimized=is_optimized,
        source_table_versions=source_table_versions,
        table_id=resolved_table_id,
        namespace_id=resolved_namespace_id,
        table_name=resolved_table_name,
        scd2_config=resolved_scd2_config,
        schema_json=resolved_schema_json,
        cdc_enabled=cdc_enabled,
        cdc_enabled_version=cdc_enabled_version,
    )


def _outside_table_of(db, settings: "input_schema.NodeCatalogReader"):
    """The table a reader names in a catalog kept elsewhere (``external_catalogs.OutsideTable``), or ``None``."""
    from flowfile_core.catalog.services.external_catalogs import outside_table

    return outside_table(
        db,
        settings.catalog_full_table_name,
        settings.catalog_table_name,
        settings.catalog_namespace_id,
        source=settings.source,
    )


def _outside_table_info(db, user_id: int | None, outside) -> CatalogTableInfo:
    """A reader's resolution of a table kept elsewhere: its reference, and whether ``user_id`` has Use on its schema."""
    return CatalogTableInfo(
        file_path=None,
        table_type="physical",
        producer_fingerprint=None,
        is_optimized=False,
        authorized=sharing.user_id_can_use(db, user_id, "catalog_namespace", outside.namespace_id),
        namespace_id=outside.namespace_id,
        table_name=outside.table,
        kept_elsewhere=outside.reference,
    )


def _outside_table_ref(reference: str, user_id: int | None):
    """The ``IcebergTableRef`` of a table kept elsewhere for ``user_id``'s run: ciphertext only, Use checked.

    Raises the sentence ``external_catalogs.table_ref_for_run`` words when the catalog is gone or not shared.
    """
    from flowfile_core.catalog.services.external_catalogs import table_ref_for_run

    catalog, table = reference.split(".", 1)
    with get_db_context() as db:
        return table_ref_for_run(db, catalog, table, user_id, writes=False)


def _outside_table_columns(reference: str, user_id: int | None) -> dict[str, pl.DataType]:
    """The columns of a table kept elsewhere as its catalog describes them; no storage key is asked for.

    Once described, the table has its lineage row (``_record_outside_table``).
    """
    from shared.iceberg_utils import describe_table

    columns = dict(describe_table(_outside_table_ref(reference, user_id)).columns)
    _record_outside_table(reference)
    return columns


def _record_outside_table(reference: str) -> None:
    """Give a table kept elsewhere its ``catalog_tables`` row now that its catalog described it for a node.

    The row is lineage only (``external_catalogs.ensure_table_row``), so a failure to write it is logged and never
    fails the read.
    """
    from flowfile_core.catalog.services.external_catalogs import ensure_table_row, outside_table

    try:
        with get_db_context() as db:
            ensure_table_row(db, outside_table(db, reference, None, None, source="local"))
    except Exception:
        logger.warning("Could not record the catalog row of %s", reference, exc_info=True)


def unresolved_schema_refusal(svc: CatalogService, settings) -> str | None:
    """Why a writer names no schema it can write, or ``None``: its ``namespace_full_name`` resolves nowhere here and it
    holds no ``namespace_id`` to fall back to (a catalog removed or renamed, a flow from another install, a typo).

    Without this refusal such a writer would write a table into no namespace at all. A writer on the hub names the
    hub's schema, which resolves nowhere here by design: never refused.
    """
    if table_sources.is_remote(getattr(settings, "source", None)):
        return None
    name = getattr(settings, "namespace_full_name", None)
    if not name or settings.namespace_id is not None or svc.resolve_namespace_id_by_full_name(name) is not None:
        return None
    return f"There is no catalog schema called {name} here; pick the schema again."


def _effective_namespace_id(svc: CatalogService, settings) -> int | None:
    """Resolve a node's target namespace name-first (portable ``namespace_full_name``), falling back to
    the numeric ``namespace_id`` for flows saved before names were stored. The numeric id is install-local
    and goes stale when namespaces are recreated, so the name is preferred whenever present."""
    resolved = svc.resolve_namespace_id_by_full_name(getattr(settings, "namespace_full_name", None))
    return resolved if resolved is not None else settings.namespace_id
