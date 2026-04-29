"""Business-logic layer for the Catalog system.

``CatalogService`` encapsulates all domain rules (validation, authorisation,
enrichment) and delegates persistence to a ``CatalogRepository``.  It never
raises ``HTTPException`` — only domain-specific exceptions from
``catalog.exceptions``.
"""

from __future__ import annotations

import hashlib
import io
import json
import logging
import os
import signal
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal
from uuid import uuid4

import polars as pl
from deltalake import DeltaTable
from pyarrow import dataset as ds
from sqlalchemy.exc import IntegrityError

from flowfile_core.catalog.constants import (
    DEFAULT_PREVIEW_LIMIT,
    DEFAULT_SQL_MAX_ROWS,
    DEFAULT_VISUALIZATION_ROWS,
    MAX_PREVIEW_LIMIT,
    MAX_VISUALIZATION_ROWS,
    QUERY_VIRTUAL_TABLE_RECURSION_LIMIT,
    SAVED_FLOW_NODE_X,
    SAVED_FLOW_NODE_Y_STEP,
    SAVED_FLOW_SQL_NODE_X,
    SAVED_FLOW_SQL_NODE_Y,
)
from flowfile_core.catalog.delta_utils import (
    check_source_versions_current,
    delete_table_storage,
    get_delta_table_size_bytes,
    is_delta_table,
    read_delta_preview,
    table_exists,
)
from flowfile_core.catalog.exceptions import (
    AmbiguousTableError,
    DashboardNotFoundError,
    FavoriteNotFoundError,
    FlowAlreadyRunningError,
    FlowHasArtifactsError,
    FlowNotFoundError,
    FollowNotFoundError,
    NamespaceExistsError,
    NamespaceNotEmptyError,
    NamespaceNotFoundError,
    NestingLimitError,
    NoSnapshotError,
    RunNotFoundError,
    ScheduleNotFoundError,
    TableExistsError,
    TableFavoriteNotFoundError,
    TableNotFoundError,
    VisualizationComputeError,
    VisualizationExistsError,
    VisualizationNotFoundError,
)
from flowfile_core.catalog.repository import CatalogRepository
from flowfile_core.catalog.services.engagement import FlowEngagementService
from flowfile_core.catalog.services.flows import FlowRegistrationService
from flowfile_core.catalog.services.namespaces import NamespaceService
from flowfile_core.catalog.services.previews import TablePreviewService
from flowfile_core.catalog.services.runs import FlowRunService
from flowfile_core.catalog.services.schedules import ScheduleService
from flowfile_core.catalog.services.sql import SqlService
from flowfile_core.catalog.services.tables import CatalogMaterializationResult, TableService
from flowfile_core.catalog.services.virtual_tables import VirtualTableService
from flowfile_core.catalog.serializers import (
    VizEnrichment,
    artifact_to_out,
    format_pyarrow_preview,
    run_to_out,
)
from flowfile_core.catalog.text_utils import (
    hash_source_versions as _hash_source_versions,
    is_table_reference as _is_table_reference,
    parse_delta_history as _parse_delta_history,
    rewrite_qualified_references as _rewrite_qualified_references,
)
from flowfile_core.catalog.validators import (
    format_full_name,
    reject_dot_in_name,
    validate_thumbnail,
    validate_viz_source,
)
from flowfile_core.configs.flow_logger import FlowLogger, NodeLogger
from flowfile_core.database.models import (
    CatalogDashboard,
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
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import (
    trigger_catalog_materialize,
    trigger_delta_history,
    trigger_delta_version_preview,
    trigger_read_table_metadata,
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
    CatalogTableSummary,
    ColumnSchema,
    ColumnStatsResponse,
    DashboardCreate,
    DashboardLayout,
    DashboardOut,
    DashboardUpdate,
    DeltaTableHistory,
    FlowRegistrationOut,
    FlowRunDetail,
    FlowRunOut,
    FlowScheduleOut,
    FlowSummary,
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
from flowfile_core.utils.arrow_reader import read_top_n
from shared.delta_utils import validate_catalog_path
from shared.storage_config import storage
from shared.subprocess_utils import spawn_flow_subprocess

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
        self._virtual_tables = VirtualTableService(
            repo, self._namespaces, self._tables, self._schedules
        )
        self._sql.bind(virtual_tables=self._virtual_tables)
        self._virtual_tables.bind(sql=self._sql)

        self._previews = TablePreviewService(repo, self._tables, self._virtual_tables)

    # ------------------------------------------------------------------ #
    # Private helpers
    # ------------------------------------------------------------------ #

    _reject_dot_in_name = staticmethod(reject_dot_in_name)
    _format_full_name = staticmethod(format_full_name)

    def _resolve_namespace_name(self, namespace_id: int | None) -> str | None:
        return self._namespaces.resolve_namespace_name(namespace_id)

    def _resolve_viz_enrichment(self, viz: CatalogVisualization, table: CatalogTable | None) -> VizEnrichment:
        table_name: str | None = None
        table_namespace_name: str | None = None
        table_full_name: str | None = None
        table_type: str | None = None
        if table is not None:
            table_name = table.name
            table_namespace_name = self._resolve_namespace_name(table.namespace_id)
            table_full_name = self._format_full_name(table_namespace_name, table.name)
            table_type = table.table_type or "physical"
        if viz.namespace_id is not None:
            namespace_name = self._resolve_namespace_name(viz.namespace_id)
        else:
            namespace_name = table_namespace_name
        return VizEnrichment(
            table_name=table_name,
            table_namespace_name=table_namespace_name,
            table_full_name=table_full_name,
            table_type=table_type,
            namespace_name=namespace_name,
        )

    def _validate_table_registration(self, name: str, namespace_id: int | None) -> None:
        self._tables._validate_table_registration(name, namespace_id)

    def resolve_table(
        self,
        reference: str,
        default_namespace_id: int | None = None,
        strict: bool = False,
    ) -> CatalogTable:
        return self._tables.resolve_table(reference, default_namespace_id, strict)

    def _enrich_flow_registration(self, flow: FlowRegistration, user_id: int) -> FlowRegistrationOut:
        return self._flows.enrich_flow_registration(flow, user_id)

    def _bulk_enrich_flows(self, flows: list[FlowRegistration], user_id: int) -> list[FlowRegistrationOut]:
        return self._flows.bulk_enrich_flows(flows, user_id)

    def _resolve_log_path(self, run_id: int, run_type: str) -> str | None:
        return self._runs._resolve_log_path(run_id, run_type)

    def _run_to_out(self, run: FlowRun) -> FlowRunOut:
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
        return self._namespaces.create_namespace(name, owner_id, parent_id, description)

    def update_namespace(
        self,
        namespace_id: int,
        name: str | None = None,
        description: str | None = None,
    ) -> CatalogNamespace:
        return self._namespaces.update_namespace(namespace_id, name, description)

    def delete_namespace(self, namespace_id: int) -> None:
        self._namespaces.delete_namespace(namespace_id)

    def get_namespace(self, namespace_id: int) -> CatalogNamespace:
        return self._namespaces.get_namespace(namespace_id)

    def list_namespaces(self, parent_id: int | None = None) -> list[CatalogNamespace]:
        return self._namespaces.list_namespaces(parent_id)

    def get_namespace_tree(self, user_id: int) -> list[NamespaceTree]:
        """Build the full catalog tree with flows nested under schemas.

        Uses bulk enrichment to avoid N+1 queries when there are many flows.
        """
        catalogs = self.repo.list_root_namespaces()

        # Collect all flows first, then bulk-enrich them
        all_flows: list[FlowRegistration] = []
        namespace_flow_map: dict[int, list[FlowRegistration]] = {}
        namespace_artifact_map: dict[int, list[GlobalArtifactOut]] = {}
        namespace_table_map: dict[int, list[CatalogTableOut]] = {}

        # Visualizations are surfaced as a peer of flows / tables / artifacts in
        # whatever namespace they were saved into (their own ``namespace_id``,
        # not the parent table's). Resolve once and bucket by namespace.
        viz_by_namespace: dict[int, list[VisualizationOut]] = {}
        for v in self.list_visualization_library(user_id=user_id):
            if v.namespace_id is None:
                continue
            viz_by_namespace.setdefault(v.namespace_id, []).append(v)

        for cat in catalogs:
            cat_flows = self.repo.list_flows(namespace_id=cat.id)
            namespace_flow_map[cat.id] = cat_flows
            all_flows.extend(cat_flows)
            namespace_artifact_map[cat.id] = [
                artifact_to_out(a) for a in self.repo.list_artifacts_for_namespace(cat.id)
            ]
            namespace_table_map[cat.id] = self._bulk_enrich_tables(self.repo.list_tables_for_namespace(cat.id), user_id)

            for schema in self.repo.list_child_namespaces(cat.id):
                schema_flows = self.repo.list_flows(namespace_id=schema.id)
                namespace_flow_map[schema.id] = schema_flows
                all_flows.extend(schema_flows)
                namespace_artifact_map[schema.id] = [
                    artifact_to_out(a) for a in self.repo.list_artifacts_for_namespace(schema.id)
                ]
                namespace_table_map[schema.id] = self._bulk_enrich_tables(
                    self.repo.list_tables_for_namespace(schema.id), user_id
                )

        # Bulk enrich all flows at once
        enriched = self._bulk_enrich_flows(all_flows, user_id)
        enriched_map = {e.id: e for e in enriched}

        # Build tree structure
        result: list[NamespaceTree] = []
        for cat in catalogs:
            schemas = self.repo.list_child_namespaces(cat.id)
            children: list[NamespaceTree] = []
            for schema in schemas:
                schema_flows = namespace_flow_map.get(schema.id, [])
                flow_outs = [enriched_map[f.id] for f in schema_flows if f.id in enriched_map]
                children.append(
                    NamespaceTree(
                        id=schema.id,
                        name=schema.name,
                        parent_id=schema.parent_id,
                        level=schema.level,
                        description=schema.description,
                        owner_id=schema.owner_id,
                        created_at=schema.created_at,
                        updated_at=schema.updated_at,
                        children=[],
                        flows=flow_outs,
                        artifacts=namespace_artifact_map.get(schema.id, []),
                        tables=namespace_table_map.get(schema.id, []),
                        visualizations=viz_by_namespace.get(schema.id, []),
                    )
                )
            cat_flows = namespace_flow_map.get(cat.id, [])
            root_flow_outs = [enriched_map[f.id] for f in cat_flows if f.id in enriched_map]
            result.append(
                NamespaceTree(
                    id=cat.id,
                    name=cat.name,
                    parent_id=cat.parent_id,
                    level=cat.level,
                    description=cat.description,
                    owner_id=cat.owner_id,
                    created_at=cat.created_at,
                    updated_at=cat.updated_at,
                    children=children,
                    flows=root_flow_outs,
                    artifacts=namespace_artifact_map.get(cat.id, []),
                    tables=namespace_table_map.get(cat.id, []),
                    visualizations=viz_by_namespace.get(cat.id, []),
                )
            )
        return result

    def get_default_namespace_id(self) -> int | None:
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
        return self._flows.register_flow(name, flow_path, owner_id, namespace_id, description)

    def update_flow(
        self,
        registration_id: int,
        requesting_user_id: int,
        name: str | None = None,
        description: str | None = None,
        namespace_id: int | None = None,
    ) -> FlowRegistrationOut:
        return self._flows.update_flow(
            registration_id, requesting_user_id, name, description, namespace_id
        )

    def delete_flow(self, registration_id: int) -> None:
        self._flows.delete_flow(registration_id)

    def get_flow(self, registration_id: int, user_id: int) -> FlowRegistrationOut:
        return self._flows.get_flow(registration_id, user_id)

    def list_flows(self, user_id: int, namespace_id: int | None = None) -> list[FlowRegistrationOut]:
        return self._flows.list_flows(user_id, namespace_id)

    def list_artifacts_for_flow(self, registration_id: int) -> list[GlobalArtifactOut]:
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
        return self._runs.list_runs(registration_id, schedule_id, run_type, limit, offset)

    def get_run_detail(self, run_id: int) -> FlowRunDetail:
        return self._runs.get_run_detail(run_id)

    def get_run(self, run_id: int) -> FlowRun:
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
        """Auto-register a flow under General > {Unnamed Flows | Local Flows | default}.

        Body lives on the facade — not FlowRegistrationService — because a test
        monkeypatches ``CatalogService.ensure_local_flows_namespace`` to simulate
        an older catalog where that namespace was never seeded. Routing the
        ``ensure_*`` calls through ``self`` keeps that contract working.
        """
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
        return self._flows.ensure_unnamed_flows_namespace()

    def ensure_local_flows_namespace(self) -> CatalogNamespace | None:
        return self._flows.ensure_local_flows_namespace()

    def resolve_registration_id(self, flow_path: str) -> int | None:
        return self._flows.resolve_registration_id(flow_path)

    def get_run_snapshot(self, run_id: int) -> str:
        return self._runs.get_run_snapshot(run_id)

    # ------------------------------------------------------------------ #
    # Favorites
    # ------------------------------------------------------------------ #

    def add_favorite(self, user_id: int, registration_id: int) -> FlowFavorite:
        return self._engagement.add_favorite(user_id, registration_id)

    def remove_favorite(self, user_id: int, registration_id: int) -> None:
        self._engagement.remove_favorite(user_id, registration_id)

    def list_favorites(self, user_id: int) -> list[FlowRegistrationOut]:
        return self._engagement.list_favorites(user_id)

    def add_follow(self, user_id: int, registration_id: int) -> FlowFollow:
        return self._engagement.add_follow(user_id, registration_id)

    def remove_follow(self, user_id: int, registration_id: int) -> None:
        self._engagement.remove_follow(user_id, registration_id)

    def list_following(self, user_id: int) -> list[FlowRegistrationOut]:
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
        return self._tables.table_to_out(table, user_id, compute_laziness)

    def _bulk_enrich_tables(self, tables: list[CatalogTable], user_id: int) -> list[CatalogTableOut]:
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
        return self._schedules.fire_table_trigger_schedules(table_id, table_updated_at)

    def resolve_write_destination(
        self,
        table_name: str,
        namespace_id: int | None,
        write_mode: str,
        catalog_dir: Path,
    ) -> tuple[CatalogTable | None, Path, str]:
        return self._tables.resolve_write_destination(table_name, namespace_id, write_mode, catalog_dir)

    def resolve_table_file_path(
        self,
        table_id: int | None = None,
        table_name: str | None = None,
        namespace_id: int | None = None,
    ) -> str | None:
        return self._tables.resolve_table_file_path(table_id, table_name, namespace_id)

    def get_table(self, table_id: int, user_id: int | None = None) -> CatalogTableOut:
        return self._tables.get_table(table_id, user_id)

    def resolve_table_out(
        self,
        reference: str,
        default_namespace_id: int | None = None,
        strict: bool = False,
        user_id: int | None = None,
    ) -> tuple[CatalogTableOut, list[dict]]:
        return self._tables.resolve_table_out(reference, default_namespace_id, strict, user_id)

    def list_tables(self, namespace_id: int | None = None, user_id: int | None = None) -> list[CatalogTableOut]:
        return self._tables.list_tables(namespace_id, user_id)

    def update_table(
        self,
        table_id: int,
        name: str | None = None,
        description: str | None = None,
        namespace_id: int | None = None,
    ) -> CatalogTableOut:
        return self._tables.update_table(table_id, name, description, namespace_id)

    def delete_table(self, table_id: int) -> None:
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
        serialized_lazy_frame: bytes | None = None,
        is_optimized: bool = False,
        schema_json: str | None = None,
        polars_plan: str | None = None,
        source_table_versions: str | None = None,
    ) -> CatalogTableOut:
        return self._virtual_tables.create_virtual_flow_table(
            name,
            owner_id,
            producer_registration_id,
            namespace_id,
            description,
            serialized_lazy_frame,
            is_optimized,
            schema_json,
            polars_plan,
            source_table_versions,
        )

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
        return self._virtual_tables.update_virtual_flow_table(
            table_id,
            name,
            description,
            namespace_id,
            producer_registration_id,
            serialized_lazy_frame,
            is_optimized,
            schema_json,
            polars_plan,
            source_table_versions,
        )

    def create_query_virtual_table(
        self,
        name: str,
        owner_id: int,
        sql_query: str,
        namespace_id: int | None = None,
        description: str | None = None,
    ) -> CatalogTableOut:
        return self._virtual_tables.create_query_virtual_table(
            name, owner_id, sql_query, namespace_id, description
        )

    def update_query_virtual_table(
        self,
        table_id: int,
        name: str | None = None,
        description: str | None = None,
        namespace_id: int | None = None,
        sql_query: str | None = None,
    ) -> CatalogTableOut:
        return self._virtual_tables.update_query_virtual_table(
            table_id, name, description, namespace_id, sql_query
        )

    def resolve_query_virtual_table(
        self,
        table_id: int,
        user_id: int | None = None,
        _visited: set[int] | None = None,
        _depth: int = 0,
    ) -> pl.LazyFrame:
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
        return self._virtual_tables._resolve_table_for_sql_context(t, user_id, visited, depth)

    def resolve_virtual_flow_table(
        self,
        table_id: int,
        user_id: int | None = None,
        run_location: Literal["remote", "local"] | None = None,
        node_logger: NodeLogger | None = None,
    ) -> pl.LazyFrame:
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
        return self._previews.get_table_preview(table_id, limit, version, user_id)

    def resolve_virtual_flow_table_preview(
        self,
        table_id: int,
        limit: int,
        user_id: int | None = None,
    ) -> CatalogTablePreview:
        return self._previews.resolve_virtual_flow_table_preview(table_id, limit, user_id)

    def get_table_history(self, table_id: int, limit: int | None = None) -> DeltaTableHistory:
        return self._previews.get_table_history(table_id, limit)

    def add_table_favorite(self, user_id: int, table_id: int) -> TableFavorite:
        return self._tables.add_table_favorite(user_id, table_id)

    def remove_table_favorite(self, user_id: int, table_id: int) -> None:
        self._tables.remove_table_favorite(user_id, table_id)

    def list_table_favorites(self, user_id: int) -> list[CatalogTableOut]:
        return self._tables.list_table_favorites(user_id)

    def _schedule_to_out(self, schedule: FlowSchedule) -> FlowScheduleOut:
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
        return self._schedules.update_schedule(schedule_id, enabled, interval_seconds, name, description)

    def delete_schedule(self, schedule_id: int) -> None:
        self._schedules.delete_schedule(schedule_id)

    def get_schedule(self, schedule_id: int) -> FlowScheduleOut:
        return self._schedules.get_schedule(schedule_id)

    def list_schedules(self, registration_id: int | None = None) -> list[FlowScheduleOut]:
        return self._schedules.list_schedules(registration_id)

    # ------------------------------------------------------------------ #
    # Trigger schedule now
    # ------------------------------------------------------------------ #

    def _spawn_flow_subprocess(self, flow_path: str, run_id: int) -> int | None:
        return self._runs._spawn_flow_subprocess(flow_path, run_id)

    def _spawn_flow_run(
        self,
        flow: FlowRegistration,
        user_id: int,
        run_type: RunType,
        schedule_id: int | None = None,
    ) -> FlowRun:
        return self._runs.spawn_flow_run(flow, user_id, run_type, schedule_id)

    def run_flow_now(self, registration_id: int, user_id: int) -> FlowRunOut:
        return self._runs.run_flow_now(registration_id, user_id)

    def trigger_schedule_now(self, schedule_id: int, user_id: int) -> FlowRunOut:
        return self._schedules.trigger_schedule_now(schedule_id, user_id)

    # ------------------------------------------------------------------ #
    # Active runs + cancel
    # ------------------------------------------------------------------ #

    def list_active_runs(self) -> list[ActiveFlowRun]:
        return self._runs.list_active_runs()

    def cancel_run(self, run_id: int) -> None:
        self._runs.cancel_run(run_id)

    # ------------------------------------------------------------------ #
    # Dashboard / Stats
    # ------------------------------------------------------------------ #

    def get_catalog_stats(self, user_id: int) -> CatalogStats:
        """Return an overview of the catalog for the dashboard.

        Uses bulk enrichment for favourite flows to avoid N+1 queries.
        """
        total_ns = self.repo.count_catalog_namespaces()
        total_flows = self.repo.count_all_flows()
        total_runs = self.repo.count_runs()
        total_favs = self.repo.count_favorites(user_id)
        total_table_favs = self.repo.count_table_favorites(user_id)
        total_artifacts = self.repo.count_all_active_artifacts()
        total_tables = self.repo.count_all_tables()
        total_virtual_tables = self.repo.count_virtual_tables()
        total_schedules = self.repo.count_schedules()

        recent_runs_raw = self.repo.list_runs(limit=10, offset=0)
        recent_out = [self._run_to_out(r) for r in recent_runs_raw]

        # Bulk enrich favourite flows
        favs = self.repo.list_favorites(user_id)
        flows: list[FlowRegistration] = []
        for fav in favs:
            flow = self.repo.get_flow(fav.registration_id)
            if flow is not None:
                flows.append(flow)
        fav_flows = self._bulk_enrich_flows(flows, user_id)

        # Bulk enrich favourite tables
        fav_tables = self.list_table_favorites(user_id)

        # Active runs
        active_runs = self.list_active_runs()

        return CatalogStats(
            total_namespaces=total_ns,
            total_flows=total_flows,
            total_runs=total_runs,
            total_favorites=total_favs,
            total_table_favorites=total_table_favs,
            total_artifacts=total_artifacts,
            total_tables=total_tables,
            total_virtual_tables=total_virtual_tables,
            total_schedules=total_schedules,
            recent_runs=recent_out,
            favorite_flows=fav_flows,
            favorite_tables=fav_tables,
            active_runs=active_runs,
        )

    # ------------------------------------------------------------------ #
    # SQL Query
    # ------------------------------------------------------------------ #

    def resolve_all_delta_tables(self) -> dict[str, str]:
        return self._virtual_tables.resolve_all_delta_tables()

    def resolve_all_queryable_tables(self) -> tuple[dict[str, str], dict[str, int]]:
        return self._virtual_tables.resolve_all_queryable_tables()

    def execute_sql_query(
        self, query: str, max_rows: int = DEFAULT_SQL_MAX_ROWS, user_id: int | None = None
    ) -> SqlQueryResult:
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
        return self._sql.save_sql_query_as_flow(query, name, owner_id, namespace_id, description, used_tables)

    # ================== Visualizations =====================================

    def _viz_to_out(self, viz: CatalogVisualization) -> VisualizationOut:
        # Specs are stored as JSON. The current shape is ``list[IChart]``
        # (one per chart tab). Older 008-era rows store a single ``IChart``
        # dict — coerce to a one-element list on read.
        try:
            raw = json.loads(viz.spec_json) if viz.spec_json else []
        except (TypeError, ValueError):
            raw = []
        if isinstance(raw, dict):
            spec_list = [raw]
        elif isinstance(raw, list):
            spec_list = [item for item in raw if isinstance(item, dict)]
        else:
            spec_list = []

        table = self.repo.get_table(viz.catalog_table_id) if viz.catalog_table_id is not None else None
        enrichment = self._resolve_viz_enrichment(viz, table)
        return VisualizationOut(
            id=viz.id,
            name=viz.name,
            description=viz.description,
            chart_type=viz.chart_type,
            spec=spec_list,
            spec_gw_version=viz.spec_gw_version,
            source_type=viz.source_type or "table",
            catalog_table_id=viz.catalog_table_id,
            sql_query=viz.sql_query,
            namespace_id=viz.namespace_id,
            thumbnail_data_url=viz.thumbnail_data_url,
            created_by=viz.created_by,
            created_at=viz.created_at,
            updated_at=viz.updated_at,
            table_name=enrichment.table_name,
            table_namespace_name=enrichment.table_namespace_name,
            table_full_name=enrichment.table_full_name,
            table_type=enrichment.table_type,
            namespace_name=enrichment.namespace_name,
        )

    def list_visualizations_for_table(self, table_id: int, user_id: int | None = None) -> list[VisualizationOut]:
        """Filtered listing — viz that reference a specific catalog table."""
        if self.repo.get_table(table_id) is None:
            raise TableNotFoundError(table_id=table_id)
        return [self._viz_to_out(v) for v in self.repo.list_visualizations(table_id)]

    def list_visualization_library(self, user_id: int | None = None) -> list[VisualizationOut]:
        """Return all saved visualizations as catalog library entries.

        SQL-source viz carry only their inline query and namespace; table-source
        viz also surface their parent table's name + namespace. Orphaned rows
        (parent table deleted) are returned without table info rather than
        being dropped, so users can still find/edit/delete them.

        ``spec`` is omitted (empty list) here — the library listing is for
        catalog browsing; full chart specs are fetched per-viz on demand.
        """
        rows = self.repo.list_all_visualizations()
        if not rows:
            return []
        table_ids = {r.catalog_table_id for r in rows if r.catalog_table_id is not None}
        tables_by_id: dict[int, CatalogTable] = {}
        for tid in table_ids:
            t = self.repo.get_table(tid)
            if t is not None:
                tables_by_id[tid] = t
        items: list[VisualizationOut] = []
        for viz in rows:
            table = tables_by_id.get(viz.catalog_table_id) if viz.catalog_table_id else None
            enrichment = self._resolve_viz_enrichment(viz, table)
            items.append(
                VisualizationOut(
                    id=viz.id,
                    name=viz.name,
                    description=viz.description,
                    chart_type=viz.chart_type,
                    spec_gw_version=viz.spec_gw_version,
                    source_type=viz.source_type or "table",
                    catalog_table_id=viz.catalog_table_id,
                    sql_query=viz.sql_query,
                    namespace_id=viz.namespace_id,
                    thumbnail_data_url=viz.thumbnail_data_url,
                    created_by=viz.created_by,
                    created_at=viz.created_at,
                    updated_at=viz.updated_at,
                    table_name=enrichment.table_name,
                    table_namespace_name=enrichment.table_namespace_name,
                    table_full_name=enrichment.table_full_name,
                    table_type=enrichment.table_type,
                    namespace_name=enrichment.namespace_name,
                )
            )
        return items

    def get_visualization(self, viz_id: int, user_id: int | None = None) -> VisualizationOut:
        viz = self.repo.get_visualization(viz_id)
        if viz is None:
            raise VisualizationNotFoundError(viz_id=viz_id)
        return self._viz_to_out(viz)

    # TODO(visual-editor task 12): replace ValueError → 422 with a typed
    # VisualizationThumbnailTooLargeError → 413, and replace the cosmetic
    # `data:image/*` prefix check with magic-byte sniffing of the decoded
    # base64 payload (PNG: 89 50 4E 47 0D 0A 1A 0A; JPEG: FF D8 FF). The
    # current prefix check passes garbage like `data:image/png;base64,XXXX`
    # which then silently fails to render in <img>.
    _validate_thumbnail = staticmethod(validate_thumbnail)
    _validate_viz_source = staticmethod(validate_viz_source)

    def create_visualization(self, payload: VisualizationCreate, user_id: int) -> VisualizationOut:
        self._validate_viz_source(payload)

        # Default the namespace to the parent table's namespace when this is
        # a table-source viz and the caller didn't pick one. SQL-source viz
        # without a namespace stay unscoped.
        namespace_id = payload.namespace_id
        if payload.source_type == "table":
            table = self.repo.get_table(payload.catalog_table_id) if payload.catalog_table_id else None
            if table is None:
                raise TableNotFoundError(table_id=payload.catalog_table_id or 0)
            if namespace_id is None:
                namespace_id = table.namespace_id

        viz = CatalogVisualization(
            name=payload.name,
            description=payload.description,
            chart_type=payload.chart_type,
            spec_json=json.dumps(payload.spec),
            spec_gw_version=payload.spec_gw_version,
            source_type=payload.source_type or "table",
            catalog_table_id=payload.catalog_table_id if payload.source_type == "table" else None,
            sql_query=payload.sql_query if payload.source_type == "sql" else None,
            thumbnail_data_url=self._validate_thumbnail(payload.thumbnail_data_url),
            namespace_id=namespace_id,
            created_by=user_id,
        )
        try:
            created = self.repo.create_visualization(viz)
        except IntegrityError as exc:
            raise VisualizationExistsError(payload.name, payload.catalog_table_id or 0) from exc
        return self._viz_to_out(created)

    def update_visualization(self, viz_id: int, payload: VisualizationUpdate, user_id: int) -> VisualizationOut:
        viz = self.repo.get_visualization(viz_id)
        if viz is None:
            raise VisualizationNotFoundError(viz_id=viz_id)
        provided = payload.model_fields_set
        if "name" in provided:
            if payload.name is None:
                raise ValueError("name cannot be cleared")
            viz.name = payload.name
        if "description" in provided:
            viz.description = payload.description
        if "chart_type" in provided:
            viz.chart_type = payload.chart_type
        if "spec" in provided:
            if payload.spec is None:
                raise ValueError("spec cannot be cleared")
            viz.spec_json = json.dumps(payload.spec)
        if "spec_gw_version" in provided:
            viz.spec_gw_version = payload.spec_gw_version
        if "namespace_id" in provided:
            viz.namespace_id = payload.namespace_id
        if "sql_query" in provided and viz.source_type == "sql":
            viz.sql_query = payload.sql_query
        if "catalog_table_id" in provided and viz.source_type == "table":
            if payload.catalog_table_id is None:
                raise ValueError("catalog_table_id cannot be cleared on a table-source viz")
            viz.catalog_table_id = payload.catalog_table_id
        if "thumbnail_data_url" in provided:
            viz.thumbnail_data_url = self._validate_thumbnail(payload.thumbnail_data_url)
        try:
            updated = self.repo.update_visualization(viz)
        except IntegrityError as exc:
            raise VisualizationExistsError(viz.name, viz.catalog_table_id or 0) from exc
        return self._viz_to_out(updated)

    def delete_visualization(self, viz_id: int, user_id: int) -> None:
        viz = self.repo.get_visualization(viz_id)
        if viz is None:
            raise VisualizationNotFoundError(viz_id=viz_id)
        self.repo.delete_visualization(viz_id)

    # ================== Dashboards =========================================

    def _validate_filter_datasources(self, layout: DashboardLayout) -> None:
        """Ensure filter datasource_ids and target_tile_ids refer to known entities."""
        seen: dict[int, bool] = {}
        tile_ids = {t.id for t in layout.tiles}
        for f in layout.filters:
            for tid in f.target_tile_ids:
                if tid not in tile_ids:
                    raise ValueError(f"filter '{f.id}' targets unknown tile_id={tid}")
            if f.datasource_id is None:
                continue
            if f.datasource_id in seen:
                if not seen[f.datasource_id]:
                    raise ValueError(f"filter '{f.id}' references unknown catalog_table_id={f.datasource_id}")
                continue
            exists = self.repo.get_table(f.datasource_id) is not None
            seen[f.datasource_id] = exists
            if not exists:
                raise ValueError(f"filter '{f.id}' references unknown catalog_table_id={f.datasource_id}")

    def _dashboard_to_out(self, dashboard: CatalogDashboard) -> DashboardOut:
        try:
            raw = json.loads(dashboard.layout_json) if dashboard.layout_json else {}
        except (TypeError, ValueError):
            raw = {}
        layout = DashboardLayout.model_validate(raw) if raw else DashboardLayout()
        ns_name: str | None = None
        if dashboard.namespace_id is not None:
            ns = self.repo.get_namespace(dashboard.namespace_id)
            ns_name = ns.name if ns is not None else None
        return DashboardOut(
            id=dashboard.id,
            name=dashboard.name,
            description=dashboard.description,
            layout=layout,
            layout_version=dashboard.layout_version or 1,
            namespace_id=dashboard.namespace_id,
            namespace_name=ns_name,
            created_by=dashboard.created_by,
            created_at=dashboard.created_at,
            updated_at=dashboard.updated_at,
        )

    def list_dashboards(self, user_id: int | None = None) -> list[DashboardOut]:
        return [self._dashboard_to_out(d) for d in self.repo.list_dashboards()]

    def get_dashboard(self, dashboard_id: int, user_id: int | None = None) -> DashboardOut:
        dashboard = self.repo.get_dashboard(dashboard_id)
        if dashboard is None:
            raise DashboardNotFoundError(dashboard_id=dashboard_id)
        return self._dashboard_to_out(dashboard)

    def create_dashboard(self, payload: DashboardCreate, user_id: int) -> DashboardOut:
        if payload.namespace_id is not None and self.repo.get_namespace(payload.namespace_id) is None:
            raise NamespaceNotFoundError(namespace_id=payload.namespace_id)
        self._validate_filter_datasources(payload.layout)
        dashboard = CatalogDashboard(
            name=payload.name,
            description=payload.description,
            layout_json=payload.layout.model_dump_json(),
            layout_version=payload.layout.grid.version,
            namespace_id=payload.namespace_id,
            created_by=user_id,
        )
        created = self.repo.create_dashboard(dashboard)
        return self._dashboard_to_out(created)

    def update_dashboard(self, dashboard_id: int, payload: DashboardUpdate, user_id: int) -> DashboardOut:
        dashboard = self.repo.get_dashboard(dashboard_id)
        if dashboard is None:
            raise DashboardNotFoundError(dashboard_id=dashboard_id)
        provided = payload.model_fields_set
        if "name" in provided:
            if payload.name is None:
                raise ValueError("name cannot be cleared")
            dashboard.name = payload.name
        if "description" in provided:
            dashboard.description = payload.description
        if "namespace_id" in provided:
            if payload.namespace_id is not None and self.repo.get_namespace(payload.namespace_id) is None:
                raise NamespaceNotFoundError(namespace_id=payload.namespace_id)
            dashboard.namespace_id = payload.namespace_id
        if "layout" in provided:
            if payload.layout is None:
                raise ValueError("layout cannot be cleared")
            self._validate_filter_datasources(payload.layout)
            dashboard.layout_json = payload.layout.model_dump_json()
            dashboard.layout_version = payload.layout.grid.version
        dashboard.updated_at = datetime.now(timezone.utc)
        updated = self.repo.update_dashboard(dashboard)
        return self._dashboard_to_out(updated)

    def delete_dashboard(self, dashboard_id: int, user_id: int) -> None:
        dashboard = self.repo.get_dashboard(dashboard_id)
        if dashboard is None:
            raise DashboardNotFoundError(dashboard_id=dashboard_id)
        self.repo.delete_dashboard(dashboard_id)

    # ---- Compute ----------------------------------------------------------

    def _clamp_max_rows(self, requested: int | None) -> int:
        if requested is None or requested <= 0:
            return min(DEFAULT_VISUALIZATION_ROWS, MAX_VISUALIZATION_ROWS)
        return min(requested, MAX_VISUALIZATION_ROWS)

    def _session_key_for_table(self, table_id: int) -> str:
        table = self.repo.get_table(table_id)
        if table is None:
            raise TableNotFoundError(table_id=table_id)
        ts = int(table.updated_at.timestamp()) if table.updated_at else 0
        return f"tbl:{table_id}:{ts}"

    def _resolve_source_for_worker(self, source: VizSourceDescriptor, user_id: int | None) -> dict:
        """Translate a frontend source descriptor into a worker source spec.

        Returns a dict whose shape matches ``VizWorkerSource`` on the worker side:

        - ``kind=physical`` for Delta/Parquet catalog tables (ships only the
          directory name, not the absolute path).
        - ``kind=sql`` for query-virtual tables and ad-hoc SQL queries; reuses
          the same delta_map / virtual_refs plumbing as ``execute_sql_query``.
        - ``kind=ipc_path`` for flow-produced virtual tables, materialised by
          the worker on first access and cached on disk.
        """
        if source.source_type == "table":
            if source.table_id is None:
                raise ValueError("table_id is required when source_type='table'")
            table = self.repo.get_table(source.table_id)
            if table is None:
                raise TableNotFoundError(table_id=source.table_id)

            if table.table_type != "virtual":
                if not table.file_path:
                    raise ValueError(f"Table {table.id} has no file_path")
                return {
                    "kind": "physical",
                    "session_key": self._session_key_for_table(table.id),
                    "table_path": Path(table.file_path).name,
                }

            if table.sql_query:
                spec = self._build_sql_worker_source(table.sql_query, user_id=user_id)
                spec["session_key"] = self._session_key_for_table(table.id)
                return spec

            lf = self.resolve_virtual_flow_table(table.id, user_id=user_id, run_location="remote")
            versions_hash = _hash_source_versions(table.source_table_versions)
            res = trigger_resolve_virtual_table(table.id, lf.serialize(), versions_hash)
            return {
                "kind": "ipc_path",
                "session_key": f"fvt:{table.id}:{int(res['mtime'])}",
                "ipc_path": res["ipc_path"],
                "mtime": res["mtime"],
            }

        # Ad-hoc SQL — used by the editor / SqlExplorePanel preview path.
        if not source.sql_query:
            raise ValueError("sql_query is required when source_type='sql'")
        return self._build_sql_worker_source(source.sql_query, user_id=user_id)

    def _build_sql_worker_source(self, sql_query: str, user_id: int | None) -> dict:
        """Build a worker SQL source spec mirroring ``execute_sql_query``'s setup.

        Resolves catalog references, asks the worker to materialise each
        referenced flow-virtual table to its IPC cache, and emits a
        deterministic session key.
        """
        from flowfile_core.flowfile.sources.external_sources.sql_source.sql_source import (
            UnsafeSQLError,
            validate_sql_query,
        )

        try:
            validate_sql_query(sql_query)
        except UnsafeSQLError as exc:
            raise ValueError(str(exc)) from exc

        delta_map, virtual_map = self.resolve_all_queryable_tables()
        rewritten = _rewrite_qualified_references(sql_query, {*delta_map, *virtual_map})
        referenced_virtuals = {n for n in virtual_map if _is_table_reference(n, rewritten)}

        virtual_refs: dict[str, str] = {}
        for vname in referenced_virtuals:
            try:
                vid = virtual_map[vname]
                lf = self.resolve_virtual_flow_table(vid, user_id=user_id, run_location="remote")
                versions_hash = _hash_source_versions(self.repo.get_table(vid).source_table_versions)
                res = trigger_resolve_virtual_table(vid, lf.serialize(), versions_hash)
                virtual_refs[vname] = res["ipc_path"]
            except Exception:
                logger.warning("Could not resolve virtual table %r for viz", vname)

        # Only ship the delta_map subset that's actually referenced — keeps the
        # session key compact and the worker's SQLContext small.
        referenced_delta = {n: d for n, d in delta_map.items() if _is_table_reference(n, rewritten)}

        key_material = json.dumps(
            {"q": rewritten, "d": sorted(referenced_delta.items()), "v": sorted(virtual_refs.items())},
            sort_keys=True,
        )
        digest = hashlib.sha256(key_material.encode()).hexdigest()
        return {
            "kind": "sql",
            "session_key": f"sql:{digest}",
            "sql_query": rewritten,
            "tables": referenced_delta,
            "virtual_refs": virtual_refs or None,
        }

    def _dispatch_visualize_query(
        self, worker_source: dict, payload: dict, max_rows: int
    ) -> VisualizationComputeResponse:
        try:
            data = trigger_visualize_query(worker_source, payload, max_rows)
        except RuntimeError as exc:
            raise VisualizationComputeError(str(exc)) from exc
        return VisualizationComputeResponse(**data)

    def compute_saved_visualization_rows(
        self,
        viz_id: int,
        max_rows: int | None,
        user_id: int,
        payload: dict | None = None,
    ) -> VisualizationComputeResponse:
        """Compute rows for a saved viz against its embedded source.

        When ``payload`` is provided, this is the GraphicWalker
        ``computation`` callback path — every chart aggregation the user
        builds becomes one POST and the worker runs polars-gw with the GW
        workflow against the cached LazyFrame. When ``payload`` is None
        the server falls back to a raw-select so the initial sample-fetch
        path (used by VisualizationCard's preview, etc.) keeps working.
        """
        viz = self.repo.get_visualization(viz_id)
        if viz is None:
            raise VisualizationNotFoundError(viz_id=viz_id)
        source = self._viz_source_descriptor(viz)
        worker_source = self._resolve_source_for_worker(source, user_id=user_id)
        effective_payload = payload or _GW_RAW_SELECT_ALL_PAYLOAD
        viz_logger.info(
            "dispatch saved compute viz_id=%s source_type=%s kind=%s " "session_key=%s max_rows=%s gw_workflow=%s",
            viz_id,
            viz.source_type,
            worker_source["kind"],
            worker_source["session_key"],
            max_rows,
            payload is not None,
        )
        return self._dispatch_visualize_query(worker_source, effective_payload, self._clamp_max_rows(max_rows))

    def get_visualization_fields_for_viz(self, viz_id: int, user_id: int) -> VisualizationFieldsResponse:
        viz = self.repo.get_visualization(viz_id)
        if viz is None:
            raise VisualizationNotFoundError(viz_id=viz_id)
        source = self._viz_source_descriptor(viz)
        return self.get_visualization_fields(source, user_id=user_id)

    @staticmethod
    def _viz_source_descriptor(viz: CatalogVisualization) -> VizSourceDescriptor:
        if viz.source_type == "sql":
            if not viz.sql_query:
                raise VisualizationComputeError(f"sql visualization {viz.id} has no sql_query")
            return VizSourceDescriptor(source_type="sql", sql_query=viz.sql_query)
        return VizSourceDescriptor(source_type="table", table_id=viz.catalog_table_id)

    def compute_ad_hoc_visualization(
        self,
        source: VizSourceDescriptor,
        payload: dict,
        max_rows: int | None,
        user_id: int,
    ) -> VisualizationComputeResponse:
        worker_source = self._resolve_source_for_worker(source, user_id=user_id)
        viz_logger.info(
            "dispatch ad-hoc compute source_type=%s kind=%s session_key=%s max_rows=%s",
            source.source_type,
            worker_source["kind"],
            worker_source["session_key"],
            max_rows,
        )
        return self._dispatch_visualize_query(worker_source, payload, self._clamp_max_rows(max_rows))

    def get_visualization_fields(self, source: VizSourceDescriptor, user_id: int) -> VisualizationFieldsResponse:
        worker_source = self._resolve_source_for_worker(source, user_id=user_id)
        viz_logger.info(
            "dispatch fields source_type=%s kind=%s session_key=%s",
            source.source_type,
            worker_source["kind"],
            worker_source["session_key"],
        )
        try:
            data = trigger_visualize_fields(worker_source)
        except RuntimeError as exc:
            raise VisualizationComputeError(str(exc)) from exc
        return VisualizationFieldsResponse(**data)

    def get_table_column_stats(
        self,
        table_id: int,
        column: str,
        limit: int,
        user_id: int,
    ) -> ColumnStatsResponse:
        """Distinct values + min/max for a single column on a catalog table.

        Used by the dashboard filter UI to pre-populate categorical
        dropdowns and pre-fill numeric range inputs. Reuses the
        viz-session worker's cached LazyFrame so subsequent calls on the
        same table skip the load step.
        """
        if self.repo.get_table(table_id) is None:
            raise TableNotFoundError(table_id=table_id)
        clamped_limit = max(1, min(limit, MAX_PREVIEW_LIMIT))
        source = VizSourceDescriptor(source_type="table", table_id=table_id)
        worker_source = self._resolve_source_for_worker(source, user_id=user_id)
        viz_logger.info(
            "dispatch column_stats kind=%s session_key=%s column=%s limit=%d",
            worker_source["kind"],
            worker_source["session_key"],
            column,
            clamped_limit,
        )
        try:
            data = trigger_visualize_column_stats(worker_source, column, clamped_limit)
        except RuntimeError as exc:
            raise VisualizationComputeError(str(exc)) from exc
        return ColumnStatsResponse(**data)


# Backward-compat re-export for external code that imported the
# underscore-prefixed module-level helper.
_format_pyarrow_preview = format_pyarrow_preview
