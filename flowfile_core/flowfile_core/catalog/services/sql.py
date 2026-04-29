"""SQL execution against catalog tables and ``save_sql_query_as_flow``."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

from flowfile_core.catalog.constants import (
    DEFAULT_SQL_MAX_ROWS,
    SAVED_FLOW_NODE_X,
    SAVED_FLOW_NODE_Y_STEP,
    SAVED_FLOW_SQL_NODE_X,
    SAVED_FLOW_SQL_NODE_Y,
)
from flowfile_core.catalog.repository import CatalogRepository
from flowfile_core.catalog.services._resolve import resolve_or_log
from flowfile_core.catalog.services.flows import FlowRegistrationService
from flowfile_core.catalog.text_utils import (
    hash_source_versions,
    is_table_reference,
    rewrite_qualified_references,
)
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import (
    trigger_resolve_virtual_table,
)
from flowfile_core.schemas.catalog_schema import SqlQueryResult
from shared.storage_config import storage

if TYPE_CHECKING:
    from flowfile_core.catalog.services.virtual_tables import VirtualTableService

logger = logging.getLogger(__name__)


class SqlService:
    """Owns ad-hoc SQL execution and ``save_sql_query_as_flow``."""

    def __init__(self, repo: CatalogRepository, flows: FlowRegistrationService) -> None:
        self.repo = repo
        self._flows = flows
        self._virtual_tables: VirtualTableService | None = None
        self._facade = None  # set by CatalogService.__init__ via bind_facade()

    def bind(self, *, virtual_tables: VirtualTableService) -> None:
        """Late-bind VirtualTableService to break the SqlService↔VirtualTableService cycle."""
        self._virtual_tables = virtual_tables

    def bind_facade(self, facade) -> None:
        """Late-bind the facade so test monkeypatches on
        ``CatalogService.resolve_virtual_flow_table`` flow through this service."""
        self._facade = facade

    def _require_virtual_tables(self) -> VirtualTableService:
        if self._virtual_tables is None:
            raise RuntimeError("SqlService.bind(virtual_tables=...) was not called")
        return self._virtual_tables

    def _resolve_virtual_flow_table_via_facade(self, table_id: int, *, user_id: int | None, run_location: str):
        """Route resolution through the facade if present so tests that
        patch ``CatalogService.resolve_virtual_flow_table`` (or set it
        on the instance) see the spy fire."""
        if self._facade is not None:
            return self._facade.resolve_virtual_flow_table(table_id, user_id=user_id, run_location=run_location)
        return self._require_virtual_tables().resolve_virtual_flow_table(
            table_id, user_id=user_id, run_location=run_location
        )

    def execute_sql_query(
        self, query: str, max_rows: int = DEFAULT_SQL_MAX_ROWS, user_id: int | None = None
    ) -> SqlQueryResult:
        """Execute a SQL query against all catalog tables (physical + virtual) via the worker."""
        from flowfile_core.flowfile.sources.external_sources.sql_source.sql_source import (
            UnsafeSQLError,
            validate_sql_query,
        )

        try:
            validate_sql_query(query)
        except UnsafeSQLError as e:
            return SqlQueryResult(error=str(e))

        virtual_tables = self._require_virtual_tables()
        delta_map, virtual_map = virtual_tables.resolve_all_queryable_tables()
        if not delta_map and not virtual_map:
            return SqlQueryResult(error="No catalog tables available")

        query = rewrite_qualified_references(query, {*delta_map, *virtual_map})
        referenced_virtuals = {vname for vname in virtual_map if is_table_reference(vname, query)}

        virtual_refs: dict[str, str] = {}
        ipc_path_by_id: dict[int, str] = {}
        for vname in referenced_virtuals:
            vid = virtual_map[vname]
            if vid not in ipc_path_by_id:
                ipc_path = resolve_or_log(
                    lambda vid=vid: self._materialise_virtual_for_sql(vid, user_id),
                    kind="virtual table for SQL",
                    identifier=vname,
                )
                if ipc_path is None:
                    continue
                ipc_path_by_id[vid] = ipc_path
            virtual_refs[vname] = ipc_path_by_id[vid]

        # Lazy module lookup so monkeypatches on ``catalog.service.trigger_sql_query``
        # — used by tests — flow through.
        from flowfile_core.catalog import service as _service_module

        try:
            result = _service_module.trigger_sql_query(query, delta_map, max_rows, virtual_refs=virtual_refs or None)
            return SqlQueryResult(**result)
        except RuntimeError as e:
            return SqlQueryResult(error=str(e))

    def _materialise_virtual_for_sql(self, virtual_id: int, user_id: int | None) -> str:
        """Resolve a virtual table to an IPC path for the SQL worker call."""
        lazy_frame = self._resolve_virtual_flow_table_via_facade(virtual_id, user_id=user_id, run_location="remote")
        versions_hash = hash_source_versions(self.repo.get_table(virtual_id).source_table_versions)
        result = trigger_resolve_virtual_table(virtual_id, lazy_frame.serialize(), versions_hash)
        return result["ipc_path"]

    def save_sql_query_as_flow(
        self,
        query: str,
        name: str,
        owner_id: int,
        namespace_id: int | None = None,
        description: str | None = None,
        used_tables: list[str] | None = None,
    ) -> int:
        """Create a registered flow from a SQL query.

        Builds a flow JSON with catalog_reader nodes (one per used table)
        connected to a single sql_query node, saves it, and registers it.
        Returns the registration ID.
        """
        from flowfile_core.flowfile.utils import create_unique_id

        used_tables = used_tables or []
        flow_id = create_unique_id()

        nodes = []
        reader_node_ids = []

        for i, table_name in enumerate(used_tables):
            table = self.repo.get_table_by_name(table_name, namespace_id)
            if table is None:
                continue
            node_id = i + 1
            reader_node_ids.append(node_id)
            nodes.append(
                {
                    "id": node_id,
                    "type": "catalog_reader",
                    "is_start_node": True,
                    "x_position": SAVED_FLOW_NODE_X,
                    "y_position": SAVED_FLOW_NODE_X + i * SAVED_FLOW_NODE_Y_STEP,
                    "input_ids": [],
                    "outputs": [len(used_tables) + 1],
                    "setting_input": {
                        "catalog_table_id": table.id,
                        "catalog_table_name": table.name,
                    },
                }
            )

        sql_node_id = len(used_tables) + 1
        nodes.append(
            {
                "id": sql_node_id,
                "type": "sql_query",
                "is_start_node": len(reader_node_ids) == 0,
                "x_position": SAVED_FLOW_SQL_NODE_X,
                "y_position": SAVED_FLOW_SQL_NODE_Y,
                "input_ids": reader_node_ids,
                "outputs": [],
                "setting_input": {
                    "sql_query_input": {"sql_code": query},
                },
            }
        )

        flow_data = {
            "flowfile_version": "0.6.3",
            "flowfile_id": flow_id,
            "flowfile_name": name,
            "flowfile_settings": {
                "flow_id": flow_id,
                "name": name,
                "description": description or "",
                "execution_mode": "Performance",
            },
            "nodes": nodes,
        }

        flows_dir = storage.user_data_directory / "flows"
        flows_dir.mkdir(parents=True, exist_ok=True)
        flow_path = flows_dir / f"{name.replace(' ', '_')}_{flow_id}.json"
        flow_path.write_text(json.dumps(flow_data, indent=2), encoding="utf-8")

        flow = self._flows.register_flow(
            name=name,
            flow_path=str(flow_path),
            owner_id=owner_id,
            namespace_id=namespace_id,
            description=description,
        )
        return flow.id
