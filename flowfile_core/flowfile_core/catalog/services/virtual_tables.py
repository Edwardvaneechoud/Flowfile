"""Virtual flow tables and query-based virtual tables: CRUD, resolution, schema derivation."""

from __future__ import annotations

import hashlib
import io
import json
import logging
import re
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import polars as pl
from requests.exceptions import RequestException

from flowfile_core.auth import sharing
from flowfile_core.catalog.constants import QUERY_VIRTUAL_TABLE_RECURSION_LIMIT
from flowfile_core.catalog.delta_utils import check_source_versions_current, is_delta_table
from flowfile_core.catalog.exceptions import (
    CatalogError,
    FlowNotFoundError,
    NotAuthorizedError,
    NotAVirtualTableError,
    TableNotFoundError,
    WorkerUnavailableError,
)
from flowfile_core.catalog.repository import CatalogRepository
from flowfile_core.catalog.services._resolve import resolve_or_log
from flowfile_core.catalog.services.namespaces import NamespaceService
from flowfile_core.catalog.services.schedules import ScheduleService
from flowfile_core.catalog.services.tables import TableService
from flowfile_core.catalog.storage_backend import (
    CatalogStorageTarget,
    _is_cloud_uri,
    resolve_for_table,
    serialized_frame_uses_cloud,
)
from flowfile_core.catalog.table_sources import remote_sql_tables, row_targets, scan_delta, table_row
from flowfile_core.catalog.text_utils import (
    hash_source_versions,
    is_table_reference,
    rewrite_qualified_references,
)
from flowfile_core.catalog.validators import format_full_name
from flowfile_core.configs.flow_logger import FlowLogger, NodeLogger
from flowfile_core.database.models import CatalogTable
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.context import ExecutionContext, current_context, use_context
from flowfile_core.flowfile.execution.errors import ComputeError, ComputeUnavailable
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import (
    trigger_resolve_virtual_table,
)
from flowfile_core.flowfile.parameter_resolver import node_parameters_resolved
from flowfile_core.schemas.catalog_schema import CatalogTableMaterializeResult, CatalogTableOut
from shared.delta_utils import validate_catalog_path
from shared.storage_config import require_flow_in_storage, storage

if TYPE_CHECKING:
    from flowfile_core.catalog.services.sql import SqlService
    from flowfile_core.flowfile.execution.protocol import ResultRef

logger = logging.getLogger(__name__)

# Sentinel default: omitted leaves source_table_versions untouched, None clears it.
_UNSET: str = "\x00unset\x00"

_ABS_PATH_RE = re.compile(r"(?:/[\w.\-]+){2,}")
MIXED_STORAGE = (
    "This query names tables on object storage and tables or views this Flowfile reads from its own disk; "
    "the SQL editor reads one or the other. Combine them in a flow."
)


def _scrub_abs_paths(text: str) -> str:
    """Replace absolute filesystem paths in error text with their basename so callers don't learn server layout."""
    return _ABS_PATH_RE.sub(lambda m: m.group(0).rsplit("/", 1)[-1], text)


def scan_object_storage_table(
    table: CatalogTable | CatalogTableOut, target: CatalogStorageTarget, user_id: int | None, display: str
) -> pl.LazyFrame:
    """A lazy scan of a physical table on object storage from its row and its storage target, its plan holding no
    secret. A table whose catalog names no storage has nothing to be read with and is refused, never read with
    whatever credentials this process has."""
    if not target.is_cloud:
        raise ValueError(
            f"Table '{display}' is stored in cloud/object storage, but its catalog has no storage connection to "
            "read it with."
        )
    return scan_delta(target, table.file_path, user_id)


class VirtualTableService:
    """Owns virtual flow tables and query-based virtual tables."""

    def __init__(
        self,
        repo: CatalogRepository,
        namespaces: NamespaceService,
        tables: TableService,
        schedules: ScheduleService,
    ) -> None:
        self.repo = repo
        self._namespaces = namespaces
        self._tables = tables
        self._schedules = schedules
        self._sql: SqlService | None = None

    def bind(self, *, sql: SqlService) -> None:
        """Late-bind SqlService to break the SqlService↔VirtualTableService cycle."""
        self._sql = sql

    def _require_sql(self) -> SqlService:
        if self._sql is None:
            raise RuntimeError("VirtualTableService.bind(sql=...) was not called")
        return self._sql

    def create_virtual_flow_table(
        self,
        name: str,
        owner_id: int,
        producer_registration_id: int,
        namespace_id: int | None = None,
        description: str | None = None,
        serialized_lazy_frame: bytes | None = None,
        is_optimized: bool = False,
        schema_json: str | None = None,
        polars_plan: str | None = None,
        source_table_versions: str | None = None,
    ) -> CatalogTableOut:
        """Create a virtual flow table (non-materialized catalog entry)."""
        producer = self.repo.get_flow(producer_registration_id)
        if producer is None:
            raise FlowNotFoundError(registration_id=producer_registration_id)

        self._tables.validate_table_registration(name, namespace_id)

        table = CatalogTable(
            name=name,
            namespace_id=namespace_id,
            description=description,
            owner_id=owner_id,
            file_path=None,
            storage_format="delta",
            table_type="virtual",
            producer_registration_id=producer_registration_id,
            serialized_lazy_frame=serialized_lazy_frame,
            is_optimized=is_optimized,
            schema_json=schema_json,
            polars_plan=polars_plan,
            source_table_versions=source_table_versions,
        )
        table = self.repo.create_table(table)
        return self._tables.table_to_out(table)

    def update_virtual_flow_table(
        self,
        table_id: int,
        name: str | None = None,
        description: str | None = None,
        namespace_id: int | None = None,
        producer_registration_id: int | None = None,
        serialized_lazy_frame: bytes | None = None,
        is_optimized: bool | None = None,
        schema_json: str | None = None,
        polars_plan: str | None = None,
        source_table_versions: str | None = _UNSET,
    ) -> CatalogTableOut:
        """Update a virtual flow table's metadata or producer.

        ``source_table_versions`` uses an explicit-clear sentinel: passing None
        clears the stored fingerprint (a write whose sources can no longer be
        fingerprinted must not leave a stale one behind); omitting the argument
        leaves it untouched.
        """
        table = self.repo.get_table(table_id)
        if table is None or getattr(table, "table_type", "physical") != "virtual":
            raise TableNotFoundError(table_id=table_id)

        if name is not None:
            table.name = name
        if description is not None:
            table.description = description
        if namespace_id is not None:
            table.namespace_id = namespace_id
        if producer_registration_id is not None:
            producer = self.repo.get_flow(producer_registration_id)
            if producer is None:
                raise FlowNotFoundError(registration_id=producer_registration_id)
            table.producer_registration_id = producer_registration_id
        if serialized_lazy_frame is not None:
            table.serialized_lazy_frame = serialized_lazy_frame
        if is_optimized is not None:
            table.is_optimized = is_optimized
        if schema_json is not None:
            table.schema_json = schema_json
        if polars_plan is not None:
            table.polars_plan = polars_plan
        if source_table_versions is not _UNSET:
            table.source_table_versions = source_table_versions

        table = self.repo.update_table(table)

        self._schedules.safely_fire_table_trigger_schedules(table.id, table.updated_at)

        return self._tables.table_to_out(table)

    def create_query_virtual_table(
        self,
        name: str,
        owner_id: int,
        sql_query: str,
        namespace_id: int | None = None,
        description: str | None = None,
    ) -> CatalogTableOut:
        """Create a query-based virtual table from a SQL expression."""
        from flowfile_core.flowfile.sources.external_sources.sql_source.sql_source import (
            UnsafeSQLError,
            validate_sql_query,
        )

        try:
            validate_sql_query(sql_query)
        except UnsafeSQLError as e:
            raise ValueError(str(e)) from e

        self._tables.validate_table_registration(name, namespace_id)

        sql = self._require_sql()
        result = sql.execute_sql_query(sql_query, max_rows=1)
        if result.error:
            raise ValueError(f"SQL query failed: {result.error}")

        schema_list = [{"name": c, "dtype": d} for c, d in zip(result.columns, result.dtypes, strict=False)]
        schema_json = json.dumps(schema_list) if schema_list else None

        table = CatalogTable(
            name=name,
            namespace_id=namespace_id,
            description=description,
            owner_id=owner_id,
            file_path=None,
            storage_format="delta",
            table_type="virtual",
            producer_registration_id=None,
            serialized_lazy_frame=None,
            is_optimized=False,
            sql_query=sql_query,
            schema_json=schema_json,
            column_count=len(schema_list),
        )
        table = self.repo.create_table(table)
        return self._tables.table_to_out(table)

    def update_query_virtual_table(
        self,
        table_id: int,
        name: str | None = None,
        description: str | None = None,
        namespace_id: int | None = None,
        sql_query: str | None = None,
    ) -> CatalogTableOut:
        """Update a query-based virtual table; re-derives schema if SQL changed."""
        table = self.repo.get_table(table_id)
        if table is None or getattr(table, "table_type", "physical") != "virtual":
            raise TableNotFoundError(table_id=table_id)
        if not getattr(table, "sql_query", None):
            raise TableNotFoundError(table_id=table_id)

        if name is not None:
            table.name = name
        if description is not None:
            table.description = description
        if namespace_id is not None:
            table.namespace_id = namespace_id
        if sql_query is not None:
            from flowfile_core.flowfile.sources.external_sources.sql_source.sql_source import (
                UnsafeSQLError,
                validate_sql_query,
            )

            try:
                validate_sql_query(sql_query)
            except UnsafeSQLError as e:
                raise ValueError(str(e)) from e

            sql = self._require_sql()
            result = sql.execute_sql_query(sql_query, max_rows=1)
            if result.error:
                raise ValueError(f"SQL query failed: {result.error}")

            schema_list = [{"name": c, "dtype": d} for c, d in zip(result.columns, result.dtypes, strict=False)]
            table.sql_query = sql_query
            table.schema_json = json.dumps(schema_list) if schema_list else None
            table.column_count = len(schema_list)

        table = self.repo.update_table(table)

        self._schedules.safely_fire_table_trigger_schedules(table.id, table.updated_at)

        return self._tables.table_to_out(table)

    def _table_query_aliases(self, table: CatalogTable, bare_unique: bool, path_cache: dict) -> set[str]:
        """SQL-registerable names for a table: fully-qualified ``catalog.schema.table``,
        the 2-part ``schema.table``, and the bare name when globally unique. ``path_cache``
        memoizes namespace-path lookups across the table set."""
        nid = table.namespace_id
        if nid not in path_cache:
            path_cache[nid] = self._namespaces.resolve_namespace_path(nid)
        ns_path = path_cache[nid]
        schema_name = ns_path.rsplit(".", 1)[-1] if ns_path else None
        names = {format_full_name(schema_name, table.name), format_full_name(ns_path, table.name)}
        if bare_unique:
            names.add(table.name)
        return names

    def resolve_query_virtual_table(
        self,
        table_id: int,
        user_id: int | None = None,
        _visited: set[int] | None = None,
        _depth: int = 0,
    ) -> pl.LazyFrame:
        """Resolve a query-based virtual table by executing its stored SQL."""
        if _depth > QUERY_VIRTUAL_TABLE_RECURSION_LIMIT:
            raise ValueError(
                f"Query virtual table recursion limit exceeded (depth > {QUERY_VIRTUAL_TABLE_RECURSION_LIMIT})"
            )
        if _visited is None:
            _visited = set()
        if table_id in _visited:
            raise ValueError(f"Circular reference detected in query virtual table {table_id}")
        _visited.add(table_id)

        table = self.repo.get_table(table_id)
        if table is None or not getattr(table, "sql_query", None):
            raise TableNotFoundError(table_id=table_id)

        from shared.sql_validation import validate_sql_query

        # A query stored before validation existed, or written straight to the DB, must not run unchecked.
        validate_sql_query(table.sql_query)

        all_tables = [t for t in self.repo.list_tables() if t.id != table_id]
        bare_counts: dict[str, int] = {}
        for t in all_tables:
            bare_counts[t.name] = bare_counts.get(t.name, 0) + 1

        path_cache: dict[int | None, str | None] = {}
        aliases_by_table: dict[int, list[str]] = {}
        alias_to_table: dict[str, CatalogTable] = {}
        for t in all_tables:
            aliases = self._table_query_aliases(t, bare_counts.get(t.name, 0) == 1, path_cache)
            aliases_by_table[t.id] = list(aliases)
            for alias in aliases:
                alias_to_table[alias] = t

        rewritten_query = rewrite_qualified_references(table.sql_query, alias_to_table.keys())
        referenced_ids = {tbl.id for alias, tbl in alias_to_table.items() if is_table_reference(alias, rewritten_query)}

        if sharing.sharing_enabled() and user_id is not None and referenced_ids:
            db = getattr(self.repo, "_db", None)
            if db is None:
                # No session to verify grants with: fail closed.
                raise NotAuthorizedError(user_id, "use a table referenced by this query")
            for ref_id in referenced_ids:
                if not sharing.user_id_can_use(db, user_id, "catalog_table", ref_id):
                    raise NotAuthorizedError(user_id, "use a table referenced by this query")

        sql_context = pl.SQLContext()
        for tbl_id in referenced_ids:
            t = next(tbl for tbl in all_tables if tbl.id == tbl_id)
            lazy_frame = self._resolve_table_for_sql_context(t, user_id=user_id, visited=_visited, depth=_depth + 1)
            if lazy_frame is None:
                continue
            for alias in aliases_by_table[tbl_id]:
                sql_context.register(alias, lazy_frame)

        return sql_context.execute(rewritten_query)

    def _resolve_table_for_sql_context(
        self,
        t: CatalogTable,
        user_id: int | None,
        visited: set[int] | None,
        depth: int,
    ) -> pl.LazyFrame | None:
        """Return a LazyFrame for a single referenced table when resolving a SQL context.

        ``resolve_or_log`` covers the broad set of errors that can come out of
        nested virtual-table resolution — corrupt serialized frames, missing
        producer files, polars eval errors, recursion bugs.
        """
        if t.table_type == "virtual":
            if getattr(t, "sql_query", None):
                return resolve_or_log(
                    lambda: self.resolve_query_virtual_table(t.id, user_id=user_id, _visited=visited, _depth=depth),
                    kind="nested query virtual table",
                    identifier=t.name,
                )
            if (
                t.is_optimized
                and t.serialized_lazy_frame
                and not serialized_frame_uses_cloud(t.serialized_lazy_frame)
                and check_source_versions_current(t.source_table_versions)
            ):
                return pl.LazyFrame.deserialize(io.BytesIO(t.serialized_lazy_frame))
            if t.producer_registration_id:
                return resolve_or_log(
                    lambda: self.resolve_virtual_flow_table(t.id, user_id=user_id),
                    kind="flow virtual table",
                    identifier=t.name,
                )
            return None
        if t.file_path and _is_cloud_uri(t.file_path):
            # The resolved frame is shipped to the worker, so the credentials must not sit in the plan.
            return pl.scan_delta(t.file_path, **resolve_for_table(t.file_path, t.namespace_id).scan_kwargs(user_id))
        if t.file_path and is_delta_table(Path(t.file_path)):
            return pl.scan_delta(t.file_path)
        return None

    def resolve_virtual_flow_table(
        self,
        table_id: int,
        user_id: int | None = None,
        run_location: Literal["remote", "local"] | None = None,
        node_logger: NodeLogger | None = None,
    ) -> pl.LazyFrame:
        """Resolve a virtual flow table to a LazyFrame.

        For optimized tables, deserializes the stored LazyFrame directly.
        For query-based virtual tables, delegates to resolve_query_virtual_table.
        For non-optimized tables, runs the producer flow's writer node with ``run_location`` as its location, else
        on the current execution context's backend. A writer that failed on ``ComputeUnavailable`` raises it.
        """
        with self.held_virtual_flow_table(table_id, user_id, run_location, node_logger) as (plan, _):
            return plan

    @contextmanager
    def held_virtual_flow_table(
        self,
        table_id: int,
        user_id: int | None = None,
        run_location: Literal["remote", "local"] | None = None,
        node_logger: NodeLogger | None = None,
    ) -> Iterator[tuple[pl.LazyFrame, ResultRef | None]]:
        """Resolve a virtual flow table as ``resolve_virtual_flow_table`` does; its plan and its producer run's ref.

        The ref is ``None`` for a table that resolves without a run (a query table, an optimized one whose sources
        are current). Otherwise it is the writer node's result, held for the block: a deferred one is released when
        the block ends, a stored one stays, since the plan scans it.
        """
        if run_location is None:
            run_location = current_context().backend_id
        if node_logger is None:
            node_logger = FlowLogger(-1).get_node_logger(-1)
        from flowfile_core.flowfile.manage.io_flowfile import open_flow

        table = self.repo.get_table(table_id)
        if table is None or table.table_type != "virtual":
            raise TableNotFoundError(table_id=table_id)
        if table.sql_query:
            yield self.resolve_query_virtual_table(table_id, user_id=user_id), None
            return

        if (
            table.is_optimized
            and table.serialized_lazy_frame
            and not serialized_frame_uses_cloud(table.serialized_lazy_frame)
        ):
            if check_source_versions_current(table.source_table_versions):
                yield pl.LazyFrame.deserialize(io.BytesIO(table.serialized_lazy_frame)), None
                return
            logger.info("Source table versions changed for virtual table %r, running its producer flow", table.name)

        if not table.producer_registration_id:
            raise ValueError(f"Virtual table {table.name} has no producer flow")

        producer = self.repo.get_flow(table.producer_registration_id)
        if producer is None:
            raise FlowNotFoundError(registration_id=table.producer_registration_id)

        flow = open_flow(require_flow_in_storage(Path(producer.flow_path)), user_id=user_id)
        selected_node = None
        for node in flow.nodes:
            if node.name == "catalog_writer" and node.setting_input.catalog_write_settings.table_name == table.name:
                selected_node = node

        if selected_node is None:
            raise ValueError(f"No catalog_writer node for table '{table.name}' in flow '{producer.name}'")
        # Outside the producer's run loop, the writer's and its upstream nodes' ${name} refs are unresolved.
        upstream = [flow.get_node(i) for i in flow._get_upstream_node_ids(selected_node.node_id)]
        producer_context = ExecutionContext.for_flow(
            flow.flow_settings.model_copy(update={"execution_location": run_location})
        )
        with ExitStack() as params:
            params.enter_context(use_context(producer_context))
            for node in [selected_node, *upstream]:
                params.enter_context(node_parameters_resolved(node))
            selected_node.execute_node(reset_cache=True, performance_mode=True, node_logger=node_logger)

        if selected_node.results.errors:
            if isinstance(selected_node._last_run_error, ComputeUnavailable):
                raise selected_node._last_run_error
            raise ValueError(f"Flow errors for table '{table.name}': {selected_node.results.errors}")

        flowframe = selected_node.get_resulting_data()
        try:
            if flowframe is None or flowframe.data_frame is None:
                raise ValueError(f"No data produced for table '{table.name}'")
            # .lazy() leaves the shared memoized engine untouched (identity on LazyFrames).
            yield flowframe.data_frame.lazy(), selected_node.results.result_ref
        finally:
            # The producer graph is dropped after the block; the yielded plan keeps what it reads.
            selected_node.release_deferred_result()

    def fresh_versions_hash(self, table_id: int) -> str:
        """Cache key for a virtual table's worker IPC snapshot, read post-resolution.

        Resolution may re-run the producer flow, whose write re-stamps
        ``source_table_versions`` in a separate session — so this bypasses the
        session identity map. Callers must invoke it AFTER resolution, or they
        key the cache on the pre-resolution fingerprint and the worker serves
        the old snapshot while the fresh compute is discarded.
        """
        table = self.repo.get_table_fresh(table_id)
        return hash_source_versions(table.source_table_versions if table is not None else None)

    def materialize_virtual_table(self, table_id: int, user_id: int | None = None) -> CatalogTableMaterializeResult:
        """Resolve a virtual table and have the worker materialise it to a kernel-readable IPC file. A physical
        table on object storage, this catalog's or a remote one's, is written the same way from a scan with its
        storage target: a kernel holds no credential for it.

        Every call rebuilds the snapshot: a serialised plan records table paths,
        not data, so its sha256 stays constant when referenced data changes.
        The plan hash therefore only names the snapshot file (the worker skips
        its idempotency cache for the kernel_shared target).
        """
        table = table_row(self.repo, table_id, user_id)
        on_storage = bool(table.table_type != "virtual" and table.file_path and _is_cloud_uri(table.file_path))
        if getattr(table, "table_type", "physical") != "virtual" and not on_storage:
            raise NotAVirtualTableError(table_id=table_id)

        try:
            if on_storage:
                [target] = row_targets([table], user_id, "notebook")
                plan_bytes = scan_object_storage_table(table, target, user_id, table.name).serialize()
            else:
                plan_bytes = self.resolve_virtual_flow_table(table_id, user_id=user_id).serialize()
        except (CatalogError, ValueError):
            raise
        except pl.exceptions.PolarsError as e:
            # e.g. a circular view leaves an unregistered relation behind.
            raise ValueError(f"Could not resolve virtual table '{table.name}': {_scrub_abs_paths(str(e))}") from e

        # A scan's encrypted credential differs on every build, so a table on object storage names its own file.
        key = hashlib.sha256(f"{table_id}:{user_id}".encode() if on_storage else plan_bytes).hexdigest()
        try:
            result = trigger_resolve_virtual_table(
                table_id, plan_bytes, key, target="kernel_shared", worker_url=registry.worker_url()
            )
        except (RequestException, RuntimeError, ComputeError) as e:
            raise WorkerUnavailableError(_scrub_abs_paths(str(e))) from e
        ipc_path = validate_catalog_path(result["ipc_path"], storage.shared_virtual_results_directory)
        return CatalogTableMaterializeResult(
            path=str(ipc_path),
            row_count=result["row_count"],
            mtime=result["mtime"],
        )

    def resolve_all_delta_tables(self) -> dict[str, str]:
        """Return a mapping of logical table name -> directory name for LOCAL Delta catalog tables.

        Object-storage tables are excluded — the dir-name + local-root model can't address them.
        """
        tables = self.repo.list_tables()
        return {
            table.name: Path(table.file_path).name
            for table in tables
            if table.file_path and is_delta_table(Path(table.file_path))
        }

    def resolve_all_queryable_tables(
        self, accessible_table_ids: set[int] | None = None
    ) -> tuple[dict[str, str], dict[str, int]]:
        """Return Delta + virtual name maps, keyed by qualified name and by bare name (when unique).

        ``accessible_table_ids`` (set in multi-user mode) restricts the registered
        tables to the ones the requesting user may read, so SQL cannot reach a
        table the user cannot see.

        Object-storage tables are excluded from this worker-offloaded SQL path; cloud catalog SQL
        runs through the flow_graph catalog-SQL node instead.
        """
        tables = self.repo.list_tables()
        if accessible_table_ids is not None:
            tables = [t for t in tables if t.id in accessible_table_ids]
        bare_counts: dict[str, int] = {}
        for t in tables:
            if t.table_type == "virtual" or (t.file_path and is_delta_table(Path(t.file_path))):
                bare_counts[t.name] = bare_counts.get(t.name, 0) + 1

        path_cache: dict[int | None, str | None] = {}
        delta_map: dict[str, str] = {}
        virtual_map: dict[str, int] = {}
        for table in tables:
            aliases = self._table_query_aliases(table, bare_counts.get(table.name, 0) == 1, path_cache)
            if table.table_type == "virtual":
                for alias in aliases:
                    virtual_map[alias] = table.id
            elif table.file_path and is_delta_table(Path(table.file_path)):
                dir_name = Path(table.file_path).name
                for alias in aliases:
                    delta_map[alias] = dir_name
        return delta_map, virtual_map

    def _object_storage_tables(
        self, accessible_table_ids: set[int] | None, exclude: set[str] | frozenset
    ) -> dict[str, tuple[CatalogTable, str]]:
        """Each SQL name of an object-storage-backed physical table with its row and display name. The worker's SQL
        route reads tables by directory name under the local root, so these are read as a plan
        (``object_storage_plan``)."""
        tables = self.repo.list_tables()
        if accessible_table_ids is not None:
            tables = [t for t in tables if t.id in accessible_table_ids]
        bare_counts: dict[str, int] = {}
        for t in tables:
            if t.table_type != "virtual" and t.file_path and _is_cloud_uri(t.file_path):
                bare_counts[t.name] = bare_counts.get(t.name, 0) + 1
        path_cache: dict[int | None, str | None] = {}
        aliases: dict[str, tuple[CatalogTable, str]] = {}
        for table in tables:
            if table.table_type == "virtual" or not table.file_path or not _is_cloud_uri(table.file_path):
                continue
            names = self._table_query_aliases(table, bare_counts.get(table.name, 0) == 1, path_cache)
            display = format_full_name(path_cache.get(table.namespace_id), table.name)
            for name in names:
                if name not in exclude:
                    aliases[name] = (table, display)
        return aliases

    def object_storage_plan(
        self,
        query: str,
        user_id: int | None,
        own: set[str],
        accessible_table_ids: set[int] | None = None,
        purpose: str = "sql",
    ) -> tuple[pl.LazyFrame, list[str]] | None:
        """``query`` as a lazy plan over the physical tables on object storage it names, and those names; ``None``
        when it names none.

        The tables are this catalog's and, under the names this catalog does not use, a remote catalog's
        (``remote_sql_tables``). Each is scanned with its storage target's ``scan_kwargs``, so the plan holds no
        secret, and a remote catalog is asked for its targets once, now. ``own`` are the names of this catalog's
        tables on disk and its views, which the worker's SQL route reads: a query that names both kinds is refused.

        Raises:
            ValueError: A sentence for the person: both kinds named, a table whose catalog has no storage
                connection, or a remote catalog's refusal.
            pl.exceptions.PolarsError: The SQL does not hold over the tables' columns.
        """
        local = self._object_storage_tables(accessible_table_ids, own)
        tables: dict[str, CatalogTable | CatalogTableOut] = {name: table for name, (table, _) in local.items()}
        tables.update(remote_sql_tables(query, user_id, own | set(tables)))
        collapsed = rewrite_qualified_references(query, set(tables) | own)
        tables = {name: table for name, table in tables.items() if is_table_reference(name, collapsed)}
        if not tables:
            return None
        if any(is_table_reference(name, collapsed) for name in own):
            raise ValueError(MIXED_STORAGE)
        rows = list({table.id: table for table in tables.values()}.values())
        targets = dict(zip((row.id for row in rows), row_targets(rows, user_id, purpose), strict=True))
        context = pl.SQLContext()
        for name, table in tables.items():
            display = local[name][1] if name in local else name
            context.register(name, scan_object_storage_table(table, targets[table.id], user_id, display))
        return context.execute(collapsed), sorted(tables)
