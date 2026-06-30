"""Virtual flow tables and query-based virtual tables: CRUD, resolution, schema derivation."""

from __future__ import annotations

import io
import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import polars as pl

from flowfile_core.auth import sharing
from flowfile_core.catalog.constants import QUERY_VIRTUAL_TABLE_RECURSION_LIMIT
from flowfile_core.catalog.delta_utils import check_source_versions_current, is_delta_table
from flowfile_core.catalog.exceptions import (
    FlowNotFoundError,
    NotAuthorizedError,
    TableNotFoundError,
)
from flowfile_core.catalog.repository import CatalogRepository
from flowfile_core.catalog.services._resolve import resolve_or_log
from flowfile_core.catalog.services.namespaces import NamespaceService
from flowfile_core.catalog.services.schedules import ScheduleService
from flowfile_core.catalog.services.tables import TableService
from flowfile_core.catalog.storage_backend import _is_cloud_uri, resolve_for_namespace, serialized_frame_uses_cloud
from flowfile_core.catalog.text_utils import (
    is_table_reference,
    rewrite_qualified_references,
)
from flowfile_core.catalog.validators import format_full_name
from flowfile_core.configs.flow_logger import FlowLogger, NodeLogger
from flowfile_core.database.models import CatalogTable
from flowfile_core.schemas.catalog_schema import CatalogTableOut

if TYPE_CHECKING:
    from flowfile_core.catalog.services.sql import SqlService

logger = logging.getLogger(__name__)


def _should_offload() -> bool:
    """Return True when heavy I/O should be delegated to the worker process."""
    from flowfile_core.configs.settings import OFFLOAD_TO_WORKER

    return OFFLOAD_TO_WORKER.value


def _project_sync_tables(owner_id: int) -> None:
    """Mirror a SQL-view create/update into the active project's tables.yaml (no-op when none active)."""
    from flowfile_core.project import project_sync

    project_sync.tables_changed(owner_id)


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

    # ---- Virtual flow table CRUD ---------------------------------------- #

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
        source_table_versions: str | None = None,
    ) -> CatalogTableOut:
        """Update a virtual flow table's metadata or producer."""
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
        if source_table_versions is not None:
            table.source_table_versions = source_table_versions

        table = self.repo.update_table(table)

        self._schedules.safely_fire_table_trigger_schedules(table.id, table.updated_at)

        return self._tables.table_to_out(table)

    # ---- Query-based virtual table CRUD --------------------------------- #

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
        _project_sync_tables(owner_id)
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
        _project_sync_tables(table.owner_id)

        return self._tables.table_to_out(table)

    # ---- Resolution ------------------------------------------------------ #

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

        all_tables = [t for t in self.repo.list_tables() if t.id != table_id]
        bare_counts: dict[str, int] = {}
        for t in all_tables:
            bare_counts[t.name] = bare_counts.get(t.name, 0) + 1

        aliases_by_table: dict[int, list[str]] = {}
        alias_to_table: dict[str, CatalogTable] = {}
        for t in all_tables:
            ns_name = self._namespaces.resolve_namespace_name(t.namespace_id)
            qualified = format_full_name(ns_name, t.name)
            aliases = [qualified]
            if bare_counts.get(t.name, 0) == 1 and qualified != t.name:
                aliases.append(t.name)
            aliases_by_table[t.id] = aliases
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
            target = resolve_for_namespace(t.namespace_id)
            return pl.scan_delta(t.file_path, storage_options=target.storage_options or None)
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
        For non-optimized tables, triggers flow execution via the worker.
        """
        if run_location is None:
            run_location = "remote" if _should_offload() else "local"
        if node_logger is None:
            node_logger = FlowLogger(-1).get_node_logger(-1)
        from flowfile_core.flowfile.manage.io_flowfile import open_flow

        table = self.repo.get_table(table_id)
        if table is None or table.table_type != "virtual":
            raise TableNotFoundError(table_id=table_id)
        if table.sql_query:
            return self.resolve_query_virtual_table(table_id, user_id=user_id)

        if (
            table.is_optimized
            and table.serialized_lazy_frame
            and not serialized_frame_uses_cloud(table.serialized_lazy_frame)
        ):
            if check_source_versions_current(table.source_table_versions):
                return pl.LazyFrame.deserialize(io.BytesIO(table.serialized_lazy_frame))
            logger.info(
                "Source table versions changed for virtual table %r, falling back to flow execution", table.name
            )

        if not table.producer_registration_id:
            raise ValueError(f"Virtual table {table.name} has no producer flow")

        producer = self.repo.get_flow(table.producer_registration_id)
        if producer is None:
            raise FlowNotFoundError(registration_id=table.producer_registration_id)

        flow = open_flow(Path(producer.flow_path), user_id=user_id)
        selected_node = None
        for node in flow.nodes:
            if node.name == "catalog_writer" and node.setting_input.catalog_write_settings.table_name == table.name:
                selected_node = node

        if selected_node is None:
            raise ValueError(f"No catalog_writer node for table '{table.name}' in flow '{producer.name}'")
        selected_node.execute_node(
            run_location=run_location,
            reset_cache=True,
            performance_mode=True,
            optimize_for_downstream=False,
            node_logger=node_logger,
        )

        if selected_node.results.errors:
            raise ValueError(f"Flow errors for table '{table.name}': {selected_node.results.errors}")

        flowframe = selected_node.get_resulting_data()
        if flowframe is None or flowframe.data_frame is None:
            raise ValueError(f"No data produced for table '{table.name}'")

        flowframe.lazy = True
        return flowframe.data_frame

    # ---- Discovery ------------------------------------------------------- #

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

        delta_map: dict[str, str] = {}
        virtual_map: dict[str, int] = {}
        for table in tables:
            ns_name = self._namespaces.resolve_namespace_name(table.namespace_id)
            qualified = format_full_name(ns_name, table.name)
            include_bare = bare_counts.get(table.name, 0) == 1
            if table.table_type == "virtual":
                virtual_map[qualified] = table.id
                if include_bare and qualified != table.name:
                    virtual_map[table.name] = table.id
            elif table.file_path and is_delta_table(Path(table.file_path)):
                dir_name = Path(table.file_path).name
                delta_map[qualified] = dir_name
                if include_bare and qualified != table.name:
                    delta_map[table.name] = dir_name
        return delta_map, virtual_map
