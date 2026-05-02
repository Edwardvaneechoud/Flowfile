"""Business-logic layer for the Catalog system.

``CatalogService`` encapsulates all domain rules (validation, authorisation,
enrichment) and delegates persistence to a ``CatalogRepository``.  It never
raises ``HTTPException`` — only domain-specific exceptions from
``catalog.exceptions``.
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Literal

import polars as pl

from flowfile_core.catalog.constants import (
    DEFAULT_PREVIEW_LIMIT,
    DEFAULT_SQL_MAX_ROWS,
)
from flowfile_core.catalog.repository import CatalogRepository
from flowfile_core.catalog.serializers import (
    VizEnrichment,
    format_pyarrow_preview,
)
from flowfile_core.catalog.services.engagement import FlowEngagementService
from flowfile_core.catalog.services.flows import FlowRegistrationService
from flowfile_core.catalog.services.namespaces import NamespaceService
from flowfile_core.catalog.services.previews import TablePreviewService
from flowfile_core.catalog.services.runs import FlowRunService
from flowfile_core.catalog.services.schedules import ScheduleService
from flowfile_core.catalog.services.sql import SqlService
from flowfile_core.catalog.services.stats import StatsService
from flowfile_core.catalog.services.tables import CatalogMaterializationResult, TableService
from flowfile_core.catalog.services.virtual_tables import VirtualTableService
from flowfile_core.catalog.services.visualizations import VisualizationService

# Re-exports preserved so external callers / tests that still reach for the
# underscore-prefixed names continue to work.
from flowfile_core.catalog.text_utils import is_table_reference as _is_table_reference  # noqa: F401
from flowfile_core.catalog.text_utils import parse_delta_history as _parse_delta_history  # noqa: F401
from flowfile_core.catalog.text_utils import rewrite_qualified_references as _rewrite_qualified_references  # noqa: F401
from flowfile_core.catalog.validators import (
    format_full_name,
    reject_dot_in_name,
    validate_thumbnail,
    validate_viz_source,
)
from flowfile_core.configs.flow_logger import NodeLogger
from flowfile_core.database.models import (
    CatalogNamespace,
    CatalogTable,
    CatalogVisualization,
    FlowFavorite,
    FlowFollow,
    FlowRegistration,
    FlowRun,
    FlowSchedule,
    RunType,
    TableFavorite,
)

# Worker-trigger re-imports kept here because sub-services do
# ``from flowfile_core.catalog import service as _service_module`` and call
# ``_service_module.trigger_*`` so test monkeypatches against this module's
# attributes flow through to the sub-services.
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import (  # noqa: F401
    trigger_catalog_materialize,
    trigger_resolve_virtual_table,
    trigger_sql_query,
    trigger_visualize_column_stats,
    trigger_visualize_fields,
    trigger_visualize_query,
)
from flowfile_core.schemas.catalog_schema import (
    ActiveFlowRun,
    CatalogStats,
    CatalogTableOut,
    CatalogTablePreview,
    ColumnStatsResponse,
    DashboardCreate,
    DashboardOut,
    DashboardUpdate,
    DeltaTableHistory,
    FlowRegistrationOut,
    FlowRunDetail,
    FlowRunOut,
    FlowScheduleOut,
    GlobalArtifactOut,
    NamespaceTree,
    PaginatedFlowRuns,
    SqlQueryResult,
    VisualizationComputeResponse,
    VisualizationCreate,
    VisualizationFieldsResponse,
    VisualizationOut,
    VisualizationUpdate,
    VizSourceDescriptor,
)

logger = logging.getLogger(__name__)
viz_logger = logger.getChild("viz")


def _should_offload() -> bool:
    """Return True when heavy I/O should be delegated to the worker process."""
    from flowfile_core.configs.settings import OFFLOAD_TO_WORKER

    return OFFLOAD_TO_WORKER.value


# polars-gw workflow that returns rows un-aggregated (raw select-all).
_GW_RAW_SELECT_ALL_PAYLOAD: dict = {"workflow": [{"type": "view", "query": [{"op": "raw", "fields": ["*"]}]}]}


class CatalogService:
    """Coordinates all catalog business logic.

    Parameters
    ----------
    repo:
        Any object satisfying the ``CatalogRepository`` protocol.
    """

    # Re-exported on the class so tests can reference ``CatalogService.CatalogMaterializationResult``.
    CatalogMaterializationResult = CatalogMaterializationResult

    # Re-exported as a class-level static so tests can call
    # ``CatalogService._compute_laziness_blockers(flow_path)`` directly.
    _compute_laziness_blockers = staticmethod(TableService._compute_laziness_blockers)

    def __init__(self, repo: CatalogRepository) -> None:
        self.repo = repo
        self._namespaces = NamespaceService(repo)
        self._flows = FlowRegistrationService(repo, self._namespaces)
        self._runs = FlowRunService(repo)
        self._engagement = FlowEngagementService(repo, self._flows)
        self._schedules = ScheduleService(repo, self._runs, self._namespaces)
        self._tables = TableService(repo, self._namespaces, self._flows, self._schedules)

        # SqlService and VirtualTableService form a cycle: SqlService.execute_sql_query
        # uses VirtualTableService for resolution, and VirtualTableService.create_query_virtual_table
        # uses SqlService to derive the schema. Late-bind to break the cycle.
        self._sql = SqlService(repo, self._flows)
        self._virtual_tables = VirtualTableService(repo, self._namespaces, self._tables, self._schedules)
        self._sql.bind(virtual_tables=self._virtual_tables)
        self._virtual_tables.bind(sql=self._sql)

        self._previews = TablePreviewService(repo, self._tables)
        self._visualizations = VisualizationService(
            repo, self._namespaces, self._tables, self._virtual_tables, self._sql
        )
        # Tests monkeypatch ``CatalogService.resolve_virtual_flow_table`` and
        # ``CatalogService._fire_table_trigger_schedules``; binding the facade
        # on these sub-services lets those overrides flow through.
        self._visualizations.bind_facade(self)
        self._sql.bind_facade(self)
        self._schedules.bind_facade(self)

        self._stats = StatsService(repo, self._flows, self._runs, self._tables)

    # ------------------------------------------------------------------ #
    # Private helpers
    # ------------------------------------------------------------------ #

    _reject_dot_in_name = staticmethod(reject_dot_in_name)
    _format_full_name = staticmethod(format_full_name)

    def _resolve_namespace_name(self, namespace_id: int | None) -> str | None:
        """Look up a namespace's name by ID, returning ``None`` when absent."""
        return self._namespaces.resolve_namespace_name(namespace_id)

    def _resolve_viz_enrichment(self, viz: CatalogVisualization, table: CatalogTable | None) -> VizEnrichment:
        """Resolve table + namespace name fields attached to a visualization DTO."""
        return self._visualizations._resolve_viz_enrichment(viz, table)

    def _validate_table_registration(self, name: str, namespace_id: int | None) -> None:
        """Reject invalid table names and check the namespace + uniqueness pre-conditions."""
        self._tables.validate_table_registration(name, namespace_id)

    def resolve_table(
        self,
        reference: str,
        default_namespace_id: int | None = None,
        strict: bool = False,
    ) -> CatalogTable:
        """Resolve a ``"ns.table"`` or bare ``"table"`` reference to a single CatalogTable."""
        return self._tables.resolve_table(reference, default_namespace_id, strict)

    def _enrich_flow_registration(self, flow: FlowRegistration, user_id: int) -> FlowRegistrationOut:
        """Attach favourite/follow flags and run stats to a single registration."""
        return self._flows.enrich_flow_registration(flow, user_id)

    def _bulk_enrich_flows(self, flows: list[FlowRegistration], user_id: int) -> list[FlowRegistrationOut]:
        """Enrich many flows with favourites, follows, and run stats in bulk."""
        return self._flows.bulk_enrich_flows(flows, user_id)

    def _resolve_log_path(self, run_id: int, run_type: str) -> str | None:
        """Return the log file path for subprocess-spawned runs, if it exists."""
        return self._runs._resolve_log_path(run_id, run_type)

    def _run_to_out(self, run: FlowRun) -> FlowRunOut:
        """Convert a FlowRun ORM row to its FlowRunOut DTO."""
        return self._runs.run_to_out(run)

    # ------------------------------------------------------------------ #
    # Namespace operations
    # ------------------------------------------------------------------ #

    def create_namespace(
        self,
        name: str,
        owner_id: int,
        parent_id: int | None = None,
        description: str | None = None,
    ) -> CatalogNamespace:
        """Create a catalog (level 0) or schema (level 1) namespace."""
        return self._namespaces.create_namespace(name, owner_id, parent_id, description)

    def update_namespace(
        self,
        namespace_id: int,
        name: str | None = None,
        description: str | None = None,
    ) -> CatalogNamespace:
        """Update a namespace's name and/or description."""
        return self._namespaces.update_namespace(namespace_id, name, description)

    def delete_namespace(self, namespace_id: int) -> None:
        """Delete a namespace if it has no children, flows or tables."""
        self._namespaces.delete_namespace(namespace_id)

    def get_namespace(self, namespace_id: int) -> CatalogNamespace:
        """Retrieve a single namespace by ID."""
        return self._namespaces.get_namespace(namespace_id)

    def list_namespaces(self, parent_id: int | None = None) -> list[CatalogNamespace]:
        """List namespaces, optionally filtered by parent."""
        return self._namespaces.list_namespaces(parent_id)

    def get_namespace_tree(self, user_id: int) -> list[NamespaceTree]:
        """Build the full catalog tree with flows, tables and visualizations nested under schemas."""
        return self._namespaces.get_namespace_tree(
            user_id,
            list_visualizations=lambda uid: self._visualizations.list_visualization_library(uid),
            bulk_enrich_tables=self._tables.bulk_enrich_tables,
            bulk_enrich_flows=self._flows.bulk_enrich_flows,
        )

    def get_default_namespace_id(self) -> int | None:
        """Return the ID of the ``General > default`` schema."""
        return self._namespaces.get_default_namespace_id()

    # ------------------------------------------------------------------ #
    # Flow registration operations
    # ------------------------------------------------------------------ #

    def register_flow(
        self,
        name: str,
        flow_path: str,
        owner_id: int,
        namespace_id: int | None = None,
        description: str | None = None,
    ) -> FlowRegistrationOut:
        """Register a new flow in the catalog."""
        return self._flows.register_flow(name, flow_path, owner_id, namespace_id, description)

    def update_flow(
        self,
        registration_id: int,
        requesting_user_id: int,
        name: str | None = None,
        description: str | None = None,
        namespace_id: int | None = None,
    ) -> FlowRegistrationOut:
        """Update a flow registration."""
        return self._flows.update_flow(registration_id, requesting_user_id, name, description, namespace_id)

    def delete_flow(self, registration_id: int) -> None:
        """Delete a flow and its related favourites/follows."""
        self._flows.delete_flow(registration_id)

    def get_flow(self, registration_id: int, user_id: int) -> FlowRegistrationOut:
        """Get an enriched flow registration."""
        return self._flows.get_flow(registration_id, user_id)

    def list_flows(self, user_id: int, namespace_id: int | None = None) -> list[FlowRegistrationOut]:
        """List flows, optionally filtered by namespace, enriched with user context."""
        return self._flows.list_flows(user_id, namespace_id)

    def list_artifacts_for_flow(self, registration_id: int) -> list[GlobalArtifactOut]:
        """List all active artifacts produced by a registered flow."""
        return self._flows.list_artifacts_for_flow(registration_id)

    # ------------------------------------------------------------------ #
    # Run operations
    # ------------------------------------------------------------------ #

    def list_runs(
        self,
        registration_id: int | None = None,
        schedule_id: int | None = None,
        run_type: RunType | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> PaginatedFlowRuns:
        """List run summaries (without snapshots) with total count for pagination."""
        return self._runs.list_runs(registration_id, schedule_id, run_type, limit, offset)

    def get_run_detail(self, run_id: int) -> FlowRunDetail:
        """Get a single run including the YAML snapshot."""
        return self._runs.get_run_detail(run_id)

    def get_run(self, run_id: int) -> FlowRun:
        """Get a raw FlowRun model."""
        return self._runs.get_run(run_id)

    def start_run(
        self,
        registration_id: int | None,
        flow_name: str,
        flow_path: str | None,
        user_id: int,
        number_of_nodes: int,
        run_type: RunType,
        flow_snapshot: str | None = None,
    ) -> FlowRun:
        """Record a new flow run start."""
        return self._runs.start_run(
            registration_id, flow_name, flow_path, user_id, number_of_nodes, run_type, flow_snapshot
        )

    def complete_run(
        self,
        run_id: int,
        success: bool,
        nodes_completed: int,
        number_of_nodes: int | None = None,
        node_results_json: str | None = None,
    ) -> FlowRun:
        """Mark a run as completed."""
        return self._runs.complete_run(run_id, success, nodes_completed, number_of_nodes, node_results_json)

    def create_completed_run(
        self,
        registration_id: int | None,
        flow_name: str,
        flow_path: str | None,
        user_id: int,
        started_at: datetime | None,
        ended_at: datetime | None,
        success: bool,
        nodes_completed: int,
        number_of_nodes: int,
        run_type: RunType,
        node_results_json: str | None = None,
        flow_snapshot: str | None = None,
    ) -> FlowRun:
        """Record a fully completed run in one step (fallback when start_run was skipped)."""
        return self._runs.create_completed_run(
            registration_id,
            flow_name,
            flow_path,
            user_id,
            started_at,
            ended_at,
            success,
            nodes_completed,
            number_of_nodes,
            run_type,
            node_results_json,
            flow_snapshot,
        )

    def auto_register_flow(self, flow_path: str, name: str, user_id: int) -> FlowRegistration | None:
        """Auto-register a flow under ``General > {Unnamed Flows | Local Flows | default}``."""
        general = self.repo.get_namespace_by_name("General", parent_id=None)
        if general is None:
            logger.info("Auto-registration skipped: 'General' catalog namespace not found")
            return None

        is_unnamed = Path(flow_path).parent.name == "unnamed_flows"
        if is_unnamed:
            target_ns = self.repo.get_namespace_by_name("Unnamed Flows", parent_id=general.id)
            if target_ns is None:
                target_ns = self.ensure_unnamed_flows_namespace()
        else:
            target_ns = self.repo.get_namespace_by_name("Local Flows", parent_id=general.id)
            if target_ns is None:
                target_ns = self.ensure_local_flows_namespace()
            if target_ns is None:
                target_ns = self.repo.get_namespace_by_name("default", parent_id=general.id)
        if target_ns is None:
            logger.info("Auto-registration skipped: no suitable namespace found under 'General'")
            return None
        existing = self.repo.get_flow_by_path(flow_path)
        if existing:
            return None
        reg = FlowRegistration(
            name=name or Path(flow_path).stem,
            flow_path=flow_path,
            namespace_id=target_ns.id,
            owner_id=user_id,
        )
        return self.repo.create_flow(reg)

    def ensure_unnamed_flows_namespace(self) -> CatalogNamespace | None:
        """Ensure the ``General > Unnamed Flows`` namespace exists, creating it if missing."""
        return self._flows.ensure_unnamed_flows_namespace()

    def ensure_local_flows_namespace(self) -> CatalogNamespace | None:
        """Ensure the ``General > Local Flows`` namespace exists, creating it if missing."""
        return self._flows.ensure_local_flows_namespace()

    def resolve_registration_id(self, flow_path: str) -> int | None:
        """Look up the registration ID for a flow by its file path."""
        return self._flows.resolve_registration_id(flow_path)

    def get_run_snapshot(self, run_id: int) -> str:
        """Return the flow snapshot text for a run."""
        return self._runs.get_run_snapshot(run_id)

    # ------------------------------------------------------------------ #
    # Favorites
    # ------------------------------------------------------------------ #

    def add_favorite(self, user_id: int, registration_id: int) -> FlowFavorite:
        """Add a flow to the user's favourites (idempotent)."""
        return self._engagement.add_favorite(user_id, registration_id)

    def remove_favorite(self, user_id: int, registration_id: int) -> None:
        """Remove a flow from the user's favourites."""
        self._engagement.remove_favorite(user_id, registration_id)

    def list_favorites(self, user_id: int) -> list[FlowRegistrationOut]:
        """List all flows the user has favourited, enriched."""
        return self._engagement.list_favorites(user_id)

    def add_follow(self, user_id: int, registration_id: int) -> FlowFollow:
        """Follow a flow (idempotent)."""
        return self._engagement.add_follow(user_id, registration_id)

    def remove_follow(self, user_id: int, registration_id: int) -> None:
        """Unfollow a flow."""
        self._engagement.remove_follow(user_id, registration_id)

    def list_following(self, user_id: int) -> list[FlowRegistrationOut]:
        """List all flows the user is following, enriched."""
        return self._engagement.list_following(user_id)

    # ------------------------------------------------------------------ #
    # Catalog table operations
    # ------------------------------------------------------------------ #

    def _table_to_out(
        self,
        table: CatalogTable,
        user_id: int | None = None,
        compute_laziness: bool = False,
    ) -> CatalogTableOut:
        """Build a single-table DTO with namespace, schema, favourite and reader info."""
        return self._tables.table_to_out(table, user_id, compute_laziness)

    def _bulk_enrich_tables(self, tables: list[CatalogTable], user_id: int) -> list[CatalogTableOut]:
        """Enrich many tables with favourite status, namespace name and DTO fields."""
        return self._tables.bulk_enrich_tables(tables, user_id)

    def register_table(
        self,
        name: str,
        file_path: str,
        owner_id: int,
        namespace_id: int | None = None,
        description: str | None = None,
        source_registration_id: int | None = None,
        source_run_id: int | None = None,
    ) -> CatalogTableOut:
        """Register a new table by materialising it as a Delta table via the worker."""
        return self._tables.register_table(
            name, file_path, owner_id, namespace_id, description, source_registration_id, source_run_id
        )

    def register_table_from_data(
        self,
        name: str,
        table_path: str,
        owner_id: int,
        namespace_id: int | None = None,
        description: str | None = None,
        source_registration_id: int | None = None,
        source_run_id: int | None = None,
        storage_format: str = "delta",
        schema: list[dict[str, str]] | None = None,
        row_count: int | None = None,
        column_count: int | None = None,
        size_bytes: int | None = None,
    ) -> CatalogTableOut:
        """Register an already-materialized table (Delta or Parquet) without copying its data."""
        return self._tables.register_table_from_data(
            name,
            table_path,
            owner_id,
            namespace_id,
            description,
            source_registration_id,
            source_run_id,
            storage_format,
            schema,
            row_count,
            column_count,
            size_bytes,
        )

    def register_table_from_parquet(
        self,
        name: str,
        parquet_path: str,
        owner_id: int,
        namespace_id: int | None = None,
        description: str | None = None,
        source_registration_id: int | None = None,
        source_run_id: int | None = None,
    ) -> CatalogTableOut:
        """Backward-compatible alias for ``register_table_from_data`` with parquet storage."""
        # Body kept on the facade — a test monkeypatches
        # ``CatalogService.register_table_from_data`` and expects this
        # alias to route through it.
        return self.register_table_from_data(
            name=name,
            table_path=parquet_path,
            owner_id=owner_id,
            namespace_id=namespace_id,
            description=description,
            source_registration_id=source_registration_id,
            source_run_id=source_run_id,
            storage_format="parquet",
        )

    def overwrite_table_data(
        self,
        table_id: int,
        table_path: str | None = None,
        parquet_path: str | None = None,
        source_registration_id: int | None = None,
        source_run_id: int | None = None,
        description: str | None = None,
        storage_format: str | None = None,
        schema: list[dict[str, str]] | None = None,
        row_count: int | None = None,
        column_count: int | None = None,
        size_bytes: int | None = None,
    ) -> CatalogTableOut:
        """Replace the data of an existing catalog table in-place, preserving its ID."""
        return self._tables.overwrite_table_data(
            table_id,
            table_path,
            parquet_path,
            source_registration_id,
            source_run_id,
            description,
            storage_format,
            schema,
            row_count,
            column_count,
            size_bytes,
        )

    def _fire_table_trigger_schedules(self, table_id: int, table_updated_at: datetime) -> int:
        """Fire enabled ``table_trigger`` schedules watching *table_id* (push path)."""
        return self._schedules.fire_table_trigger_schedules(table_id, table_updated_at)

    def resolve_write_destination(
        self,
        table_name: str,
        namespace_id: int | None,
        write_mode: str,
        catalog_dir: Path,
    ) -> tuple[CatalogTable | None, Path, str]:
        """Resolve the destination path and Delta write mode for a catalog write."""
        return self._tables.resolve_write_destination(table_name, namespace_id, write_mode, catalog_dir)

    def resolve_table_file_path(
        self,
        table_id: int | None = None,
        table_name: str | None = None,
        namespace_id: int | None = None,
    ) -> str | None:
        """Resolve a catalog table's file path by ID or by name + namespace."""
        return self._tables.resolve_table_file_path(table_id, table_name, namespace_id)

    def get_table(self, table_id: int, user_id: int | None = None) -> CatalogTableOut:
        """Get a catalog table by ID."""
        return self._tables.get_table(table_id, user_id)

    def resolve_table_out(
        self,
        reference: str,
        default_namespace_id: int | None = None,
        strict: bool = False,
        user_id: int | None = None,
    ) -> tuple[CatalogTableOut, list[dict]]:
        """Resolve a reference and return its DTO plus ambiguity warnings (empty when unambiguous)."""
        return self._tables.resolve_table_out(reference, default_namespace_id, strict, user_id)

    def list_tables(self, namespace_id: int | None = None, user_id: int | None = None) -> list[CatalogTableOut]:
        """List tables, optionally filtered by namespace."""
        return self._tables.list_tables(namespace_id, user_id)

    def update_table(
        self,
        table_id: int,
        name: str | None = None,
        description: str | None = None,
        namespace_id: int | None = None,
    ) -> CatalogTableOut:
        """Update a catalog table's metadata."""
        return self._tables.update_table(table_id, name, description, namespace_id)

    def delete_table(self, table_id: int) -> None:
        """Delete a catalog table and its materialised storage (Delta dir or Parquet file)."""
        self._tables.delete_table(table_id)

    # ------------------------------------------------------------------ #
    # Virtual Flow Table operations
    # ------------------------------------------------------------------ #

    def create_virtual_flow_table(
        self,
        name: str,
        owner_id: int,
        producer_registration_id: int,
        namespace_id: int | None = None,
        description: str | None = None,
        is_optimized: bool = False,
        schema_json: str | None = None,
    ) -> CatalogTableOut:
        """Create a virtual flow table (non-materialised catalog entry)."""
        return self._virtual_tables.create_virtual_flow_table(
            name,
            owner_id,
            producer_registration_id,
            namespace_id,
            description,
            is_optimized,
            schema_json,
        )

    def update_virtual_flow_table(
        self,
        table_id: int,
        name: str | None = None,
        description: str | None = None,
        namespace_id: int | None = None,
        producer_registration_id: int | None = None,
        is_optimized: bool | None = None,
        schema_json: str | None = None,
    ) -> CatalogTableOut:
        """Update a virtual flow table's metadata or producer."""
        return self._virtual_tables.update_virtual_flow_table(
            table_id,
            name,
            description,
            namespace_id,
            producer_registration_id,
            is_optimized,
            schema_json,
        )

    def create_query_virtual_table(
        self,
        name: str,
        owner_id: int,
        sql_query: str,
        namespace_id: int | None = None,
        description: str | None = None,
    ) -> CatalogTableOut:
        """Create a query-based virtual table from a SQL expression."""
        return self._virtual_tables.create_query_virtual_table(name, owner_id, sql_query, namespace_id, description)

    def update_query_virtual_table(
        self,
        table_id: int,
        name: str | None = None,
        description: str | None = None,
        namespace_id: int | None = None,
        sql_query: str | None = None,
    ) -> CatalogTableOut:
        """Update a query-based virtual table; re-derives schema if SQL changed."""
        return self._virtual_tables.update_query_virtual_table(table_id, name, description, namespace_id, sql_query)

    def resolve_query_virtual_table(
        self,
        table_id: int,
        user_id: int | None = None,
        _visited: set[int] | None = None,
        _depth: int = 0,
    ) -> pl.LazyFrame:
        """Resolve a query-based virtual table by executing its stored SQL."""
        return self._virtual_tables.resolve_query_virtual_table(
            table_id, user_id=user_id, _visited=_visited, _depth=_depth
        )

    def _resolve_table_for_sql_context(
        self,
        t: CatalogTable,
        user_id: int | None,
        visited: set[int] | None,
        depth: int,
    ) -> pl.LazyFrame | None:
        """Return a LazyFrame for a single referenced table when resolving a SQL context."""
        return self._virtual_tables._resolve_table_for_sql_context(t, user_id, visited, depth)

    def resolve_virtual_flow_table(
        self,
        table_id: int,
        user_id: int | None = None,
        run_location: Literal["remote", "local"] | None = None,
        node_logger: NodeLogger | None = None,
    ) -> pl.LazyFrame:
        """Resolve a virtual flow table to a LazyFrame (deserialised, query-derived, or worker-executed)."""
        return self._virtual_tables.resolve_virtual_flow_table(
            table_id, user_id=user_id, run_location=run_location, node_logger=node_logger
        )

    def get_table_preview(
        self,
        table_id: int,
        limit: int = DEFAULT_PREVIEW_LIMIT,
        version: int | None = None,
        user_id: int | None = None,
    ) -> CatalogTablePreview:
        """Read the first N rows from a catalog table (physical, virtual or Delta-versioned)."""
        return self._previews.get_table_preview(table_id, limit, version, user_id)

    def get_table_history(self, table_id: int, limit: int | None = None) -> DeltaTableHistory:
        """Return the version history for a Delta catalog table."""
        return self._previews.get_table_history(table_id, limit)

    def add_table_favorite(self, user_id: int, table_id: int) -> TableFavorite:
        """Add a table to the user's favourites (idempotent)."""
        return self._tables.add_table_favorite(user_id, table_id)

    def remove_table_favorite(self, user_id: int, table_id: int) -> None:
        """Remove a table from the user's favourites."""
        self._tables.remove_table_favorite(user_id, table_id)

    def list_table_favorites(self, user_id: int) -> list[CatalogTableOut]:
        """List all tables the user has favourited, enriched."""
        return self._tables.list_table_favorites(user_id)

    def _schedule_to_out(self, schedule: FlowSchedule) -> FlowScheduleOut:
        """Convert a FlowSchedule ORM row to its DTO, populating trigger info."""
        return self._schedules._schedule_to_out(schedule)

    def create_schedule(
        self,
        registration_id: int,
        owner_id: int,
        schedule_type: str,
        interval_seconds: int | None = None,
        trigger_table_id: int | None = None,
        trigger_table_ids: list[int] | None = None,
        enabled: bool = True,
        name: str | None = None,
        description: str | None = None,
    ) -> FlowScheduleOut:
        """Create a new schedule (interval, table_trigger or table_set_trigger) for a flow."""
        return self._schedules.create_schedule(
            registration_id,
            owner_id,
            schedule_type,
            interval_seconds,
            trigger_table_id,
            trigger_table_ids,
            enabled,
            name,
            description,
        )

    def update_schedule(
        self,
        schedule_id: int,
        enabled: bool | None = None,
        interval_seconds: int | None = None,
        name: str | None = None,
        description: str | None = None,
    ) -> FlowScheduleOut:
        """Update a schedule's enabled flag, interval, name or description."""
        return self._schedules.update_schedule(schedule_id, enabled, interval_seconds, name, description)

    def delete_schedule(self, schedule_id: int) -> None:
        """Delete a schedule and its associated trigger table links."""
        self._schedules.delete_schedule(schedule_id)

    def get_schedule(self, schedule_id: int) -> FlowScheduleOut:
        """Get a schedule by ID."""
        return self._schedules.get_schedule(schedule_id)

    def list_schedules(self, registration_id: int | None = None) -> list[FlowScheduleOut]:
        """List schedules, optionally filtered by flow."""
        return self._schedules.list_schedules(registration_id)

    # ------------------------------------------------------------------ #
    # Trigger schedule now
    # ------------------------------------------------------------------ #

    def _spawn_flow_subprocess(self, flow_path: str, run_id: int) -> int | None:
        """Fire-and-forget a ``flowfile run flow`` subprocess; returns the child PID or None."""
        return self._runs._spawn_flow_subprocess(flow_path, run_id)

    def _spawn_flow_run(
        self,
        flow: FlowRegistration,
        user_id: int,
        run_type: RunType,
        schedule_id: int | None = None,
    ) -> FlowRun:
        """Create a FlowRun record and spawn the subprocess; mark failed on spawn error."""
        return self._runs.spawn_flow_run(flow, user_id, run_type, schedule_id)

    def run_flow_now(self, registration_id: int, user_id: int) -> FlowRunOut:
        """Trigger a registered flow immediately without a schedule."""
        return self._runs.run_flow_now(registration_id, user_id)

    def trigger_schedule_now(self, schedule_id: int, user_id: int) -> FlowRunOut:
        """Manually trigger a scheduled flow immediately."""
        return self._schedules.trigger_schedule_now(schedule_id, user_id)

    # ------------------------------------------------------------------ #
    # Active runs + cancel
    # ------------------------------------------------------------------ #

    def list_active_runs(self) -> list[ActiveFlowRun]:
        """List all currently running flows (``ended_at IS NULL``)."""
        return self._runs.list_active_runs()

    def cancel_run(self, run_id: int) -> None:
        """Cancel a running flow by terminating its subprocess and marking the run failed."""
        self._runs.cancel_run(run_id)

    # ------------------------------------------------------------------ #
    # Dashboard / Stats
    # ------------------------------------------------------------------ #

    def get_catalog_stats(self, user_id: int) -> CatalogStats:
        """Return an overview of the catalog for the dashboard."""
        return self._stats.get_catalog_stats(user_id)

    # ------------------------------------------------------------------ #
    # SQL Query
    # ------------------------------------------------------------------ #

    def resolve_all_delta_tables(self) -> dict[str, str]:
        """Return a mapping of logical table name → directory name for all Delta catalog tables."""
        return self._virtual_tables.resolve_all_delta_tables()

    def resolve_all_queryable_tables(self) -> tuple[dict[str, str], dict[str, int]]:
        """Return Delta + virtual name maps, keyed by qualified name and by bare name (when unique)."""
        return self._virtual_tables.resolve_all_queryable_tables()

    def execute_sql_query(
        self, query: str, max_rows: int = DEFAULT_SQL_MAX_ROWS, user_id: int | None = None
    ) -> SqlQueryResult:
        """Execute a SQL query against all catalog tables (physical + virtual) via the worker."""
        return self._sql.execute_sql_query(query, max_rows, user_id)

    def save_sql_query_as_flow(
        self,
        query: str,
        name: str,
        owner_id: int,
        namespace_id: int | None = None,
        description: str | None = None,
        used_tables: list[str] | None = None,
    ) -> int:
        """Create a registered flow from a SQL query and return the registration ID."""
        return self._sql.save_sql_query_as_flow(query, name, owner_id, namespace_id, description, used_tables)

    # ================== Visualizations =====================================

    def list_visualizations_for_table(self, table_id: int, user_id: int | None = None) -> list[VisualizationOut]:
        """List visualizations bound to a specific catalog table."""
        return self._visualizations.list_visualizations_for_table(table_id, user_id)

    def list_visualization_library(self, user_id: int | None = None) -> list[VisualizationOut]:
        """Return all saved visualizations as catalog library entries (specs omitted)."""
        return self._visualizations.list_visualization_library(user_id)

    def get_visualization(self, viz_id: int, user_id: int | None = None) -> VisualizationOut:
        """Get a single visualization by ID."""
        return self._visualizations.get_visualization(viz_id, user_id)

    _validate_thumbnail = staticmethod(validate_thumbnail)
    _validate_viz_source = staticmethod(validate_viz_source)

    def create_visualization(self, payload: VisualizationCreate, user_id: int) -> VisualizationOut:
        """Create a new visualization (table-source or sql-source)."""
        return self._visualizations.create_visualization(payload, user_id)

    def update_visualization(self, viz_id: int, payload: VisualizationUpdate, user_id: int) -> VisualizationOut:
        """Update a visualization's spec, name, namespace or thumbnail."""
        return self._visualizations.update_visualization(viz_id, payload, user_id)

    def delete_visualization(self, viz_id: int, user_id: int) -> None:
        """Delete a visualization by ID."""
        self._visualizations.delete_visualization(viz_id, user_id)

    # ================== Dashboards =========================================

    def list_dashboards(self, user_id: int | None = None) -> list[DashboardOut]:
        """List all dashboards."""
        return self._visualizations.list_dashboards(user_id)

    def get_dashboard(self, dashboard_id: int, user_id: int | None = None) -> DashboardOut:
        """Get a dashboard by ID."""
        return self._visualizations.get_dashboard(dashboard_id, user_id)

    def create_dashboard(self, payload: DashboardCreate, user_id: int) -> DashboardOut:
        """Create a new dashboard."""
        return self._visualizations.create_dashboard(payload, user_id)

    def update_dashboard(self, dashboard_id: int, payload: DashboardUpdate, user_id: int) -> DashboardOut:
        """Update a dashboard's name, layout, namespace or description."""
        return self._visualizations.update_dashboard(dashboard_id, payload, user_id)

    def delete_dashboard(self, dashboard_id: int, user_id: int) -> None:
        """Delete a dashboard by ID."""
        self._visualizations.delete_dashboard(dashboard_id, user_id)

    # ---- Compute ----------------------------------------------------------

    def compute_saved_visualization_rows(
        self,
        viz_id: int,
        max_rows: int | None,
        user_id: int,
        payload: dict | None = None,
    ) -> VisualizationComputeResponse:
        """Compute rows for a saved viz against its embedded source via the worker."""
        return self._visualizations.compute_saved_visualization_rows(viz_id, max_rows, user_id, payload)

    def get_visualization_fields_for_viz(self, viz_id: int, user_id: int) -> VisualizationFieldsResponse:
        """Return the list of fields available for a saved visualization's source."""
        return self._visualizations.get_visualization_fields_for_viz(viz_id, user_id)

    def compute_ad_hoc_visualization(
        self,
        source: VizSourceDescriptor,
        payload: dict,
        max_rows: int | None,
        user_id: int,
    ) -> VisualizationComputeResponse:
        """Compute rows for an ad-hoc viz source via the worker."""
        return self._visualizations.compute_ad_hoc_visualization(source, payload, max_rows, user_id)

    def get_visualization_fields(self, source: VizSourceDescriptor, user_id: int) -> VisualizationFieldsResponse:
        """Return the list of fields available for a viz source descriptor."""
        return self._visualizations.get_visualization_fields(source, user_id)

    def get_table_column_stats(
        self,
        table_id: int,
        column: str,
        limit: int,
        user_id: int,
    ) -> ColumnStatsResponse:
        """Return distinct values plus min/max for a single column on a catalog table."""
        return self._visualizations.get_table_column_stats(table_id, column, limit, user_id)


# Backward-compat re-export for external code that imported the
# underscore-prefixed module-level helper.
_format_pyarrow_preview = format_pyarrow_preview
