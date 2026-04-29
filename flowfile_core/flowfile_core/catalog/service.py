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
from dataclasses import dataclass
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
from flowfile_core.catalog.services.runs import FlowRunService
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
    validate_schedule_create,
    validate_schedule_update,
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

    def __init__(self, repo: CatalogRepository) -> None:
        self.repo = repo
        self._namespaces = NamespaceService(repo)
        self._flows = FlowRegistrationService(repo, self._namespaces)
        self._runs = FlowRunService(repo)
        self._engagement = FlowEngagementService(repo, self._flows)

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
        """Check that the namespace exists and the table name is unique.

        Raises NamespaceNotFoundError or TableExistsError on validation failure.
        """
        self._reject_dot_in_name(name, "Table")
        if namespace_id is not None:
            ns = self.repo.get_namespace(namespace_id)
            if ns is None:
                raise NamespaceNotFoundError(namespace_id=namespace_id)
        existing = self.repo.get_table_by_name(name, namespace_id)
        if existing is not None:
            raise TableExistsError(name=name, namespace_id=namespace_id)

    def resolve_table(
        self,
        reference: str,
        default_namespace_id: int | None = None,
        strict: bool = False,
    ) -> CatalogTable:
        """Resolve a ``"ns.table"`` or bare ``"table"`` reference to a single CatalogTable."""
        if not reference:
            raise TableNotFoundError(name=reference)

        if "." in reference:
            ns_name, _, table_name = reference.partition(".")
            if not ns_name or not table_name:
                raise TableNotFoundError(name=reference)
            return self._resolve_qualified(ns_name, table_name, strict=strict)

        return self._resolve_bare(reference, default_namespace_id, strict=strict)

    def _all_namespaces_named(self, ns_name: str) -> list[CatalogNamespace]:
        roots = self.repo.list_root_namespaces()
        out: list[CatalogNamespace] = list(roots)
        for root in roots:
            out.extend(self.repo.list_child_namespaces(root.id))
        return [ns for ns in out if ns.name == ns_name]

    def _resolve_qualified(self, ns_name: str, table_name: str, *, strict: bool) -> CatalogTable:
        candidates_ns = self._all_namespaces_named(ns_name)
        if not candidates_ns:
            raise NamespaceNotFoundError(name=ns_name)

        tables: list[CatalogTable] = []
        for ns in candidates_ns:
            t = self.repo.get_table_by_name(table_name, ns.id)
            if t is not None:
                tables.append(t)
        if not tables:
            raise TableNotFoundError(name=f"{ns_name}.{table_name}")
        if len(tables) == 1:
            return tables[0]
        return self._disambiguate(f"{ns_name}.{table_name}", tables, strict=strict)

    def _resolve_bare(self, name: str, default_namespace_id: int | None, *, strict: bool) -> CatalogTable:
        if default_namespace_id is not None:
            t = self.repo.get_table_by_name(name, default_namespace_id)
            if t is None:
                raise TableNotFoundError(name=name)
            return t
        matches = self.repo.list_tables_by_name(name)
        if not matches:
            raise TableNotFoundError(name=name)
        if len(matches) == 1:
            return matches[0]
        return self._disambiguate(name, matches, strict=strict)

    def _disambiguate(self, reference: str, matches: list[CatalogTable], *, strict: bool) -> CatalogTable:
        candidates = [
            {
                "id": t.id,
                "name": t.name,
                "namespace_id": t.namespace_id,
                "namespace_name": self._resolve_namespace_name(t.namespace_id),
            }
            for t in matches
        ]
        if strict:
            raise AmbiguousTableError(name=reference, candidates=candidates)
        picked_candidate, *other_candidates = candidates
        alternatives = ", ".join(
            f"{self._format_full_name(c['namespace_name'], c['name'])} (id={c['id']})" for c in other_candidates
        )
        logger.warning(
            "Ambiguous table reference '%s' resolved to id=%s (%s). Other candidates: %s",
            reference,
            picked_candidate["id"],
            self._format_full_name(picked_candidate["namespace_name"], picked_candidate["name"]),
            alternatives,
        )
        return matches[0]

    @dataclass(frozen=True)
    class CatalogMaterializationResult:
        table_path: str
        schema: list[dict[str, str]]
        row_count: int
        column_count: int
        size_bytes: int
        storage_format: str = "delta"

    @staticmethod
    def _read_table_metadata(table_path: str, storage_format: str) -> tuple[list[dict[str, str]], int, int, int]:
        """Read schema, row_count, column_count, size_bytes from a table.

        Offloads the work to the worker process when available.  Only falls
        back to local I/O when the worker is not running.
        """
        if _should_offload():
            try:
                data = trigger_read_table_metadata(Path(table_path).name)
                schema_list = [{"name": c["name"], "dtype": c["dtype"]} for c in data["column_schema"]]
                return schema_list, data["row_count"], data["column_count"], data["size_bytes"]
            except Exception:
                logger.warning("Worker metadata read failed, falling back to local read", exc_info=True)

        # Fallback: read locally (only when worker is unavailable)

        p = Path(table_path)

        if storage_format == "delta" or (storage_format is None and is_delta_table(p)):
            dt = DeltaTable(str(p))
            pa_schema = dt.schema().to_arrow()
            schema_list = [{"name": field.name, "dtype": str(field.type)} for field in pa_schema]

            # Leverage pyarrow dataset for a fast, metadata-only row count
            row_count = dt.to_pyarrow_dataset().count_rows()
            size_bytes = get_delta_table_size_bytes(p)

        else:
            # Handle legacy Parquet files
            dataset = ds.dataset(str(p), format="parquet")
            schema_list = [{"name": field.name, "dtype": str(field.type)} for field in dataset.schema]

            row_count = dataset.count_rows()
            size_bytes = p.stat().st_size

        return schema_list, row_count, len(schema_list), size_bytes

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

    @staticmethod
    def _parse_schema_columns(table: CatalogTable) -> list[ColumnSchema]:
        """Parse the JSON-encoded column schema from a catalog table."""
        if not table.schema_json:
            return []
        try:
            raw = json.loads(table.schema_json)
            return [ColumnSchema(name=c["name"], dtype=c["dtype"]) for c in raw]
        except (json.JSONDecodeError, KeyError, TypeError):
            return []

    def _resolve_flow_name(self, registration_id: int | None) -> str | None:
        """Look up a flow registration name by id, returning None when absent."""
        if not registration_id:
            return None
        reg = self.repo.get_flow(registration_id)
        return reg.name if reg else None

    def _check_file_exists(self, table: CatalogTable) -> bool:
        """Determine whether the backing data for a table is available."""
        is_virtual = getattr(table, "table_type", "physical") == "virtual"
        if is_virtual:
            return True
        fe = table_exists(table.file_path) if table.file_path else False
        if not fe:
            logger.warning(
                "Catalog table %s (id=%d) references missing file: %s",
                table.name,
                table.id,
                table.file_path,
            )
        return fe

    @staticmethod
    def _compute_laziness_blockers(producer_file_path: str | None) -> list[str] | None:
        """Compute laziness blockers for a virtual table from its producer flow."""
        if not producer_file_path:
            return None
        try:
            from flowfile_core.flowfile.handler import open_flow

            fg = open_flow(Path(producer_file_path))
        except Exception as e:
            logger.warning(f"Could not open the flow or calculate the laziness: \n {e}")
            return None
        try:
            _, reasons = fg.check_flow_laziness()
            return reasons
        except Exception as e:
            logger.warning(f"Could not open the flow or calculate the reasons:\n{e}")
            return None

    def _table_to_out(
        self,
        table: CatalogTable,
        user_id: int | None = None,
        compute_laziness: bool = False,
    ) -> CatalogTableOut:
        columns = self._parse_schema_columns(table)
        source_registration_name = self._resolve_flow_name(table.source_registration_id)
        producer_registration_name = self._resolve_flow_name(table.producer_registration_id)

        readers = self.repo.list_readers_for_table(table.id)
        read_by_flows = [FlowSummary(id=r.id, name=r.name) for r in readers]

        is_fav = False
        if user_id is not None:
            is_fav = self.repo.get_table_favorite(user_id, table.id) is not None

        fe = self._check_file_exists(table)

        is_virtual = getattr(table, "table_type", "physical") == "virtual"
        laziness_blockers: list[str] | None = None
        if compute_laziness and is_virtual and table.producer_registration_id:
            producer = self.repo.get_flow(table.producer_registration_id)
            laziness_blockers = self._compute_laziness_blockers(producer.flow_path if producer else None)

        namespace_name = self._resolve_namespace_name(table.namespace_id)
        full_table_name = self._format_full_name(namespace_name, table.name)

        return CatalogTableOut(
            id=table.id,
            name=table.name,
            namespace_id=table.namespace_id,
            namespace_name=namespace_name,
            full_table_name=full_table_name,
            description=table.description,
            owner_id=table.owner_id,
            file_exists=fe,
            is_favorite=is_fav,
            schema_columns=columns,
            row_count=table.row_count,
            column_count=table.column_count,
            size_bytes=table.size_bytes,
            source_registration_id=table.source_registration_id,
            source_registration_name=source_registration_name,
            source_run_id=table.source_run_id,
            read_by_flows=read_by_flows,
            table_type=getattr(table, "table_type", "physical"),
            producer_registration_id=table.producer_registration_id,
            producer_registration_name=producer_registration_name,
            is_optimized=getattr(table, "is_optimized", None),
            laziness_blockers=laziness_blockers,
            sql_query=getattr(table, "sql_query", None),
            polars_plan=getattr(table, "polars_plan", None),
            source_table_versions=getattr(table, "source_table_versions", None),
            created_at=table.created_at,
            updated_at=table.updated_at,
        )

    def _bulk_enrich_tables(self, tables: list[CatalogTable], user_id: int) -> list[CatalogTableOut]:
        """Enrich multiple tables with favorite status in bulk to avoid N+1 queries."""
        if not tables:
            return []

        table_ids = [t.id for t in tables]
        fav_ids = self.repo.bulk_get_favorite_table_ids(user_id, table_ids)

        ns_name_cache: dict[int, str | None] = {}
        for t in tables:
            if t.namespace_id is not None and t.namespace_id not in ns_name_cache:
                ns_name_cache[t.namespace_id] = self._resolve_namespace_name(t.namespace_id)

        result: list[CatalogTableOut] = []
        for table in tables:
            columns = self._parse_schema_columns(table)
            source_registration_name = self._resolve_flow_name(table.source_registration_id)
            producer_registration_name = self._resolve_flow_name(table.producer_registration_id)

            readers = self.repo.list_readers_for_table(table.id)
            read_by_flows = [FlowSummary(id=r.id, name=r.name) for r in readers]

            fe = self._check_file_exists(table)

            namespace_name = ns_name_cache.get(table.namespace_id) if table.namespace_id is not None else None
            full_table_name = self._format_full_name(namespace_name, table.name)

            result.append(
                CatalogTableOut(
                    id=table.id,
                    name=table.name,
                    namespace_id=table.namespace_id,
                    namespace_name=namespace_name,
                    full_table_name=full_table_name,
                    description=table.description,
                    owner_id=table.owner_id,
                    file_exists=fe,
                    is_favorite=table.id in fav_ids,
                    schema_columns=columns,
                    row_count=table.row_count,
                    column_count=table.column_count,
                    size_bytes=table.size_bytes,
                    source_registration_id=table.source_registration_id,
                    source_registration_name=source_registration_name,
                    source_run_id=table.source_run_id,
                    read_by_flows=read_by_flows,
                    table_type=getattr(table, "table_type", "physical"),
                    producer_registration_id=table.producer_registration_id,
                    producer_registration_name=producer_registration_name,
                    is_optimized=getattr(table, "is_optimized", None),
                    sql_query=getattr(table, "sql_query", None),
                    polars_plan=getattr(table, "polars_plan", None),
                    source_table_versions=getattr(table, "source_table_versions", None),
                    created_at=table.created_at,
                    updated_at=table.updated_at,
                )
            )
        return result

    def _materialize_table_with_worker(
        self,
        source_file_path: str,
        table_name: str | None = None,
    ) -> CatalogMaterializationResult:
        response = trigger_catalog_materialize(
            source_file_path=source_file_path,
            table_name=table_name,
        )
        if response.ok:
            data = response.json()
            schema = [
                {"name": col["name"], "dtype": col["dtype"]}
                for col in data.get("column_schema", [])
                if "name" in col and "dtype" in col
            ]
            return CatalogService.CatalogMaterializationResult(
                table_path=data["table_path"],
                schema=schema,
                row_count=data["row_count"],
                column_count=data["column_count"],
                size_bytes=data["size_bytes"],
                storage_format="delta",
            )

        detail = None
        try:
            detail = response.json().get("detail")
        except (ValueError, AttributeError):
            detail = None

        if response.status_code == 422:
            if isinstance(detail, dict) and detail.get("error_type") == "unsupported_file_type":
                raise ValueError(detail.get("message", "Unsupported file type"))
            raise ValueError(detail.get("message", "Unsupported file type") if isinstance(detail, dict) else "")

        if isinstance(detail, dict):
            message = detail.get("message", response.text)
        else:
            message = response.text
        raise RuntimeError(f"Worker catalog materialization failed: {message}")

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
        """Register a new table in the catalog by materializing it as Parquet.

        The caller must provide ``file_path`` pointing to a supported source
        file (CSV, Parquet, Excel). The service reads the file, writes a
        Parquet copy to the catalog tables directory, and records metadata.

        Raises
        ------
        NamespaceNotFoundError
            If ``namespace_id`` is given but doesn't exist.
        TableExistsError
            If a table with this name already exists in the namespace.
        """
        self._validate_table_registration(name, namespace_id)

        materialized = self._materialize_table_with_worker(
            source_file_path=file_path,
            table_name=name,
        )

        return self._create_table_record_from_metadata(
            name=name,
            table_path=materialized.table_path,
            schema=materialized.schema,
            row_count=materialized.row_count,
            column_count=materialized.column_count,
            size_bytes=materialized.size_bytes,
            owner_id=owner_id,
            namespace_id=namespace_id,
            description=description,
            source_registration_id=source_registration_id,
            source_run_id=source_run_id,
            storage_format=materialized.storage_format,
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
        """Register an already-materialized table (Delta or Parquet) in the catalog.

        Unlike ``register_table``, this does NOT copy the file — it records
        the given ``table_path`` directly. Use this when the caller has
        already written the table to the catalog tables directory
        (e.g. the catalog writer node in a flow graph).

        When *schema*, *row_count*, *column_count*, and *size_bytes* are all
        provided the service records them directly without reading the table.
        This is the preferred path — the worker that wrote the table should
        supply this metadata so the core process never touches the data files.

        Raises
        ------
        NamespaceNotFoundError
            If ``namespace_id`` is given but doesn't exist.
        TableExistsError
            If a table with this name already exists in the namespace.
        """
        self._validate_table_registration(name, namespace_id)

        if schema is not None and row_count is not None and size_bytes is not None:
            # Fast path: caller already computed metadata (from worker)
            schema_list = schema
            if column_count is None:
                column_count = len(schema_list)
        else:
            # Fallback: read metadata from the table on disk
            schema_list, row_count, column_count, size_bytes = self._read_table_metadata(table_path, storage_format)

        return self._create_table_record_from_metadata(
            name=name,
            table_path=table_path,
            schema=schema_list,
            row_count=row_count,
            column_count=column_count,
            size_bytes=size_bytes,
            owner_id=owner_id,
            namespace_id=namespace_id,
            description=description,
            source_registration_id=source_registration_id,
            source_run_id=source_run_id,
            storage_format=storage_format,
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
        """Backward-compatible alias for ``register_table_from_data``."""
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
        """Replace the data of an existing catalog table **in-place**.

        This preserves the table's primary-key ID so that all foreign-key
        references (schedules, read links, favourites, flow definitions)
        remain valid.

        When *schema*, *row_count*, *column_count*, and *size_bytes* are all
        provided the service records them directly without reading the table.
        This is the preferred path.

        Raises
        ------
        TableNotFoundError
            If the table doesn't exist.
        """
        table = self.repo.get_table(table_id)
        if table is None:
            raise TableNotFoundError(table_id=table_id)

        # Resolve path: prefer table_path, fall back to parquet_path
        resolved_path_str = table_path or parquet_path
        dest_path = Path(resolved_path_str)

        if storage_format is None:
            storage_format = "delta" if is_delta_table(dest_path) else "parquet"

        if schema is not None and row_count is not None and size_bytes is not None:
            schema_list = schema
            if column_count is None:
                column_count = len(schema_list)
        else:
            schema_list, row_count, column_count, size_bytes = self._read_table_metadata(str(dest_path), storage_format)

        # Remove old storage if it differs from the new one
        old_path = Path(table.file_path)
        if old_path != dest_path and old_path.exists():
            try:
                delete_table_storage(old_path)
            except OSError:
                logger.warning("Failed to delete old table storage %s", old_path, exc_info=True)

        # Update fields in-place
        table.file_path = str(dest_path)
        table.storage_format = storage_format
        table.schema_json = json.dumps(schema_list)
        table.row_count = row_count
        table.column_count = column_count
        table.size_bytes = size_bytes
        if source_registration_id is not None:
            table.source_registration_id = source_registration_id
        if source_run_id is not None:
            table.source_run_id = source_run_id
        if description is not None:
            table.description = description

        table = self.repo.update_table(table)

        try:
            self._fire_table_trigger_schedules(table.id, table.updated_at)
        except Exception:
            logger.exception("Failed to fire push triggers for table %s", table.id)

        return self._table_to_out(table)

    def _fire_table_trigger_schedules(self, table_id: int, table_updated_at: datetime) -> int:
        """Fire enabled table_trigger schedules watching *table_id* (push path).

        This is the **push path** of the dual trigger mechanism for
        ``table_trigger`` schedules.  It runs synchronously inside
        ``overwrite_table_data`` — i.e. immediately when a catalog table's
        data is replaced — so the downstream flow starts without waiting
        for the next scheduler poll tick.

        A parallel **poll path** exists in
        ``FlowScheduler._process_table_trigger_schedules`` (engine.py).
        The poll path runs every ~30 s and compares
        ``CatalogTable.updated_at`` against
        ``FlowSchedule.last_trigger_table_updated_at``.  It acts as a
        **safety net**: if the push path fails (exception, process crash,
        etc.) the poll path will still detect the table change on its next
        tick and launch the flow.

        Double-firing is prevented by two guards:

        1. ``has_active_run`` — if the push path already spawned a
           subprocess, the poll path (and any concurrent push) sees an
           active run and skips the schedule.
        2. ``last_trigger_table_updated_at`` — the push path commits
           this timestamp (equal to the table's ``updated_at``) via
           ``repo.update_schedule`` *before* returning.  When the poll
           path later compares ``table.updated_at`` against this value
           it finds them equal and skips the schedule.

        There is a small race window (~poll interval) where the poll path
        could run *after* the table update but *before* the push path
        commits the timestamp.  In that scenario ``has_active_run`` is the
        final safeguard — the subprocess spawned by the push path will
        already have created a ``FlowRun`` record.

        Returns the number of flows launched.
        """
        schedules = self.repo.list_table_trigger_schedules_for_table(table_id)
        launched = 0
        for schedule in schedules:
            flow = self.repo.get_flow(schedule.registration_id)
            if flow is None:
                logger.warning("Schedule %s references missing flow %s", schedule.id, schedule.registration_id)
                continue

            if self.repo.has_active_run(schedule.registration_id):
                logger.info("Skipping push trigger for flow %s — active run exists", schedule.registration_id)
                continue

            schedule.last_triggered_at = datetime.now(timezone.utc)
            schedule.last_trigger_table_updated_at = table_updated_at
            self.repo.update_schedule(schedule)

            run = self._spawn_flow_run(flow, user_id=schedule.owner_id, run_type="scheduled", schedule_id=schedule.id)
            if run.pid is not None:
                launched += 1

        return launched

    def _create_table_record_from_metadata(
        self,
        name: str,
        table_path: str,
        schema: list[dict[str, str]],
        row_count: int,
        column_count: int,
        size_bytes: int,
        owner_id: int,
        namespace_id: int | None,
        description: str | None,
        source_registration_id: int | None,
        source_run_id: int | None,
        storage_format: str = "delta",
    ) -> CatalogTableOut:
        table = CatalogTable(
            name=name,
            namespace_id=namespace_id,
            description=description,
            owner_id=owner_id,
            file_path=table_path,
            storage_format=storage_format,
            schema_json=json.dumps(schema),
            row_count=row_count,
            column_count=column_count,
            size_bytes=size_bytes,
            source_registration_id=source_registration_id,
            source_run_id=source_run_id,
        )
        table = self.repo.create_table(table)
        return self._table_to_out(table)

    def resolve_write_destination(
        self,
        table_name: str,
        namespace_id: int | None,
        write_mode: str,
        catalog_dir: Path,
    ) -> tuple[CatalogTable | None, Path, str]:
        """Resolve the destination path and Delta write mode for a catalog write.

        Returns ``(existing_table_or_None, dest_path, delta_mode)``.

        Raises
        ------
        TableExistsError
            If the table exists and *write_mode* is ``"error"``.
        """

        existing = self.repo.get_table_by_name(table_name, namespace_id)

        if existing is not None:
            if write_mode == "error":
                raise TableExistsError(name=table_name, namespace_id=namespace_id)

            old_path = Path(existing.file_path)
            if is_delta_table(old_path):
                return existing, old_path, write_mode

            # Legacy parquet file — compute new delta dir at same stem.
            new_dir = old_path.parent / old_path.stem
            return existing, new_dir, write_mode

        # New table — merge modes handled by the worker (it creates the table)
        dir_name = f"{table_name}_{uuid4().hex[:8]}"
        return None, catalog_dir / dir_name, write_mode

    def resolve_table_file_path(
        self,
        table_id: int | None = None,
        table_name: str | None = None,
        namespace_id: int | None = None,
    ) -> str | None:
        """Resolve a catalog table's file path by ID or by name + namespace.

        Returns ``None`` if the table cannot be found.
        """
        if table_id is not None:
            table = self.repo.get_table(table_id)
            if table is not None:
                return table.file_path
        elif table_name:
            table = self.repo.get_table_by_name(table_name, namespace_id)
            if table is not None:
                return table.file_path
        return None

    def get_table(self, table_id: int, user_id: int | None = None) -> CatalogTableOut:
        """Get a catalog table by ID.

        Raises
        ------
        TableNotFoundError
            If the table doesn't exist.
        """
        table = self.repo.get_table(table_id)
        if table is None:
            raise TableNotFoundError(table_id=table_id)
        return self._table_to_out(table, user_id=user_id)

    def resolve_table_out(
        self,
        reference: str,
        default_namespace_id: int | None = None,
        strict: bool = False,
        user_id: int | None = None,
    ) -> tuple[CatalogTableOut, list[dict]]:
        """Resolve a reference and return its DTO plus ambiguity warnings (empty when unambiguous).

        Warnings are populated only when the reference is a bare name matching multiple
        rows; qualified references (``"ns.name"``) and filtered bare references
        (``default_namespace_id`` set) never produce warnings.
        """
        warnings: list[dict] = []
        if not strict and "." not in reference and default_namespace_id is None:
            matches = self.repo.list_tables_by_name(reference)
            if len(matches) > 1:
                warnings = [
                    {
                        "id": t.id,
                        "name": t.name,
                        "namespace_id": t.namespace_id,
                        "namespace_name": self._resolve_namespace_name(t.namespace_id),
                    }
                    for t in matches
                ]
        table = self.resolve_table(reference, default_namespace_id=default_namespace_id, strict=strict)
        return self._table_to_out(table, user_id=user_id), warnings

    def list_tables(self, namespace_id: int | None = None, user_id: int | None = None) -> list[CatalogTableOut]:
        """List tables, optionally filtered by namespace."""
        tables = self.repo.list_tables(namespace_id=namespace_id)
        if user_id is not None:
            return self._bulk_enrich_tables(tables, user_id)
        return [self._table_to_out(t) for t in tables]

    def update_table(
        self,
        table_id: int,
        name: str | None = None,
        description: str | None = None,
        namespace_id: int | None = None,
    ) -> CatalogTableOut:
        """Update a catalog table's metadata.

        Raises
        ------
        TableNotFoundError
            If the table doesn't exist.
        """
        table = self.repo.get_table(table_id)
        if table is None:
            raise TableNotFoundError(table_id=table_id)
        if name is not None:
            table.name = name
        if description is not None:
            table.description = description
        if namespace_id is not None:
            table.namespace_id = namespace_id
        table = self.repo.update_table(table)
        return self._table_to_out(table)

    def delete_table(self, table_id: int) -> None:
        """Delete a catalog table and its materialized storage (Delta dir or Parquet file).

        Virtual tables have no physical storage for the optimized case, so
        only metadata is removed.

        Raises
        ------
        TableNotFoundError
            If the table doesn't exist.
        """
        table = self.repo.get_table(table_id)
        if table is None:
            raise TableNotFoundError(table_id=table_id)

        file_path = table.file_path
        self.repo.delete_table(table_id)

        # Only clean up physical storage for non-virtual tables that have a file_path
        if file_path:
            try:
                storage_path = Path(file_path)
                if storage_path.exists():
                    delete_table_storage(storage_path)
            except OSError:
                logger.warning("Failed to delete materialized storage %s", file_path, exc_info=True)

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
        """Create a virtual flow table (non-materialized catalog entry).

        Raises
        ------
        FlowNotFoundError
            If the producer flow doesn't exist.
        NamespaceNotFoundError
            If the namespace doesn't exist.
        TableExistsError
            If a table with this name already exists in the namespace.
        """
        # Validate producer flow
        producer = self.repo.get_flow(producer_registration_id)
        if producer is None:
            raise FlowNotFoundError(registration_id=producer_registration_id)

        # Validate namespace
        self._validate_table_registration(name, namespace_id)

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
        return self._table_to_out(table)

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
        """Update a virtual flow table's metadata or producer.

        Raises
        ------
        TableNotFoundError
            If the table doesn't exist or is not virtual.
        FlowNotFoundError
            If the new producer flow doesn't exist.
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
        if source_table_versions is not None:
            table.source_table_versions = source_table_versions

        table = self.repo.update_table(table)

        try:
            self._fire_table_trigger_schedules(table.id, table.updated_at)
        except Exception:
            logger.exception("Failed to fire push triggers for virtual table %s", table.id)

        return self._table_to_out(table)

    # ------------------------------------------------------------------ #
    # Query-based Virtual Table operations
    # ------------------------------------------------------------------ #

    def create_query_virtual_table(
        self,
        name: str,
        owner_id: int,
        sql_query: str,
        namespace_id: int | None = None,
        description: str | None = None,
    ) -> CatalogTableOut:
        """Create a query-based virtual table from a SQL expression.

        The SQL is validated and executed once to derive the output schema.

        Raises
        ------
        NamespaceNotFoundError
            If the namespace doesn't exist.
        TableExistsError
            If a table with this name already exists in the namespace.
        ValueError
            If the SQL query is invalid or fails to execute.
        """
        from flowfile_core.flowfile.sources.external_sources.sql_source.sql_source import (
            UnsafeSQLError,
            validate_sql_query,
        )

        try:
            validate_sql_query(sql_query)
        except UnsafeSQLError as e:
            raise ValueError(str(e)) from e

        self._validate_table_registration(name, namespace_id)

        # Execute the query once to derive schema
        result = self.execute_sql_query(sql_query, max_rows=1)
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
        return self._table_to_out(table)

    def update_query_virtual_table(
        self,
        table_id: int,
        name: str | None = None,
        description: str | None = None,
        namespace_id: int | None = None,
        sql_query: str | None = None,
    ) -> CatalogTableOut:
        """Update a query-based virtual table.

        If sql_query changes, re-derives the schema.

        Raises
        ------
        TableNotFoundError
            If the table doesn't exist or is not a query-based virtual table.
        ValueError
            If the new SQL query is invalid or fails.
        """
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

            result = self.execute_sql_query(sql_query, max_rows=1)
            if result.error:
                raise ValueError(f"SQL query failed: {result.error}")

            schema_list = [{"name": c, "dtype": d} for c, d in zip(result.columns, result.dtypes, strict=False)]
            table.sql_query = sql_query
            table.schema_json = json.dumps(schema_list) if schema_list else None
            table.column_count = len(schema_list)

        table = self.repo.update_table(table)

        try:
            self._fire_table_trigger_schedules(table.id, table.updated_at)
        except Exception:
            logger.exception("Failed to fire push triggers for query virtual table %s", table.id)

        return self._table_to_out(table)

    def resolve_query_virtual_table(
        self,
        table_id: int,
        user_id: int | None = None,
        _visited: set[int] | None = None,
        _depth: int = 0,
    ) -> pl.LazyFrame:
        """Resolve a query-based virtual table by executing its stored SQL.

        Builds a pl.SQLContext with all other catalog tables and executes the
        stored SQL query. Guards against circular references via _visited set.

        Raises
        ------
        TableNotFoundError
            If the table doesn't exist or has no sql_query.
        ValueError
            If circular reference or recursion limit is hit.
        """
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
            ns_name = self._resolve_namespace_name(t.namespace_id)
            qualified = self._format_full_name(ns_name, t.name)
            aliases = [qualified]
            if bare_counts.get(t.name, 0) == 1 and qualified != t.name:
                aliases.append(t.name)
            aliases_by_table[t.id] = aliases
            for alias in aliases:
                alias_to_table[alias] = t

        rewritten_query = _rewrite_qualified_references(table.sql_query, alias_to_table.keys())
        referenced_ids = {
            tbl.id for alias, tbl in alias_to_table.items() if _is_table_reference(alias, rewritten_query)
        }

        ctx = pl.SQLContext()
        for tbl_id in referenced_ids:
            t = next(tbl for tbl in all_tables if tbl.id == tbl_id)
            lf = self._resolve_table_for_sql_context(t, user_id=user_id, visited=_visited, depth=_depth + 1)
            if lf is None:
                continue
            for alias in aliases_by_table[tbl_id]:
                ctx.register(alias, lf)

        return ctx.execute(rewritten_query)

    def _resolve_table_for_sql_context(
        self,
        t: CatalogTable,
        user_id: int | None,
        visited: set[int] | None,
        depth: int,
    ) -> pl.LazyFrame | None:
        if t.table_type == "virtual":
            if getattr(t, "sql_query", None):
                try:
                    return self.resolve_query_virtual_table(t.id, user_id=user_id, _visited=visited, _depth=depth)
                except Exception:
                    logger.warning("Could not resolve nested query virtual table %r", t.name)
                    return None
            if t.is_optimized and t.serialized_lazy_frame and check_source_versions_current(t.source_table_versions):
                return pl.LazyFrame.deserialize(io.BytesIO(t.serialized_lazy_frame))
            if t.producer_registration_id:
                try:
                    return self.resolve_virtual_flow_table(t.id, user_id=user_id)
                except Exception:
                    logger.warning("Could not resolve flow virtual table %r", t.name)
                    return None
            return None
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
        For non-optimized tables, triggers flow execution via the worker
        and returns a LazyFrame reading the IPC result.

        Raises
        ------
        TableNotFoundError
            If the table doesn't exist or is not virtual.
        ValueError
            If the virtual table cannot be resolved.
        """
        if run_location is None:
            run_location: Literal["remote", "local"] = "remote" if _should_offload() else "local"
        if node_logger is None:
            node_logger = FlowLogger(-1).get_node_logger(-1)
        from flowfile_core.flowfile.manage.io_flowfile import open_flow

        table = self.repo.get_table(table_id)
        if table is None or table.table_type != "virtual":
            raise TableNotFoundError(table_id=table_id)
        # Query-based virtual table: delegate to SQL resolver
        if table.sql_query:
            return self.resolve_query_virtual_table(table_id, user_id=user_id)

        if table.is_optimized and table.serialized_lazy_frame:
            if check_source_versions_current(table.source_table_versions):
                return pl.LazyFrame.deserialize(io.BytesIO(table.serialized_lazy_frame))
            logger.info(
                "Source table versions changed for virtual table %r, falling back to flow execution", table.name
            )

        # Non-optimized path: load the producer flow and execute it
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

    def get_table_preview(
        self,
        table_id: int,
        limit: int = DEFAULT_PREVIEW_LIMIT,
        version: int | None = None,
        user_id: int | None = None,
    ) -> CatalogTablePreview:
        """Read the first N rows from a catalog table.

        Dispatches to the appropriate helper based on table type and version.

        Raises
        ------
        TableNotFoundError
            If the table doesn't exist.
        """
        table = self.repo.get_table(table_id)
        if table is None:
            raise TableNotFoundError(table_id=table_id)

        if getattr(table, "table_type", "physical") == "virtual":
            if getattr(table, "sql_query", None):
                return self._get_query_virtual_table_preview(table, limit, user_id)
            return self._get_virtual_table_preview(table, limit, user_id)

        return self._get_physical_table_preview(table, limit, version)

    def _format_virtual_preview(
        self,
        lf: pl.LazyFrame,
        table: CatalogTable,
        limit: int,
    ) -> CatalogTablePreview:
        """Materialise *lf* on the worker, read top-N rows back as PyArrow.

        Honours Rule A: core never collects the LazyFrame — the plan is shipped
        to the worker, written as IPC, then read back via ``read_top_n``.
        """
        versions_hash = _hash_source_versions(table.source_table_versions)
        res = trigger_resolve_virtual_table(table.id, lf.serialize(), versions_hash)
        ipc_path = validate_catalog_path(res["ipc_path"], storage.catalog_virtual_results_directory)
        pa_table = read_top_n(str(ipc_path), n=limit)
        return format_pyarrow_preview(pa_table, total_rows=res.get("row_count"))

    def _get_query_virtual_table_preview(
        self,
        table: CatalogTable,
        limit: int,
        user_id: int | None,
    ) -> CatalogTablePreview:
        """Resolve a query-based virtual table and return a preview."""
        try:
            lf = self.resolve_query_virtual_table(table.id, user_id=user_id)
            return self._format_virtual_preview(lf, table, limit)
        except Exception:
            logger.warning("Could not resolve query virtual table %d for preview", table.id, exc_info=True)
            return CatalogTablePreview(columns=[], dtypes=[], rows=[], total_rows=0)

    def _get_virtual_table_preview(self, table: CatalogTable, limit: int, user_id: int | None) -> CatalogTablePreview:
        """Resolve a virtual flow table and return a preview of the collected result."""
        try:
            lf = self.resolve_virtual_flow_table(table.id, user_id=user_id)
            return self._format_virtual_preview(lf, table, limit)
        except Exception:
            logger.warning("Could not resolve virtual table %d for preview", table.id, exc_info=True)
            return CatalogTablePreview(columns=[], dtypes=[], rows=[], total_rows=0)

    def resolve_virtual_flow_table_preview(
        self,
        table_id: int,
        limit: int,
        user_id: int | None = None,
    ) -> CatalogTablePreview:
        """Resolve a virtual flow table and return a preview (worker-backed)."""
        table = self.repo.get_table(table_id)
        if table is None:
            raise TableNotFoundError(table_id=table_id)
        lf = self.resolve_virtual_flow_table(table_id, user_id=user_id)
        return self._format_virtual_preview(lf, table, limit)

    def _get_physical_table_preview(self, table: CatalogTable, limit: int, version: int | None) -> CatalogTablePreview:
        """Read a preview from a physical (Delta or Parquet) catalog table."""
        if not table.file_path:
            return CatalogTablePreview(columns=[], dtypes=[], rows=[], total_rows=0)

        data_path = Path(table.file_path)
        if not table_exists(data_path):
            return CatalogTablePreview(columns=[], dtypes=[], rows=[], total_rows=0)

        if version is not None and is_delta_table(data_path):
            return self._get_delta_version_preview(data_path, version, limit)

        if is_delta_table(data_path):
            pa_table = read_delta_preview(str(data_path), n_rows=limit)
        else:
            pa_table = read_top_n(str(data_path), n=limit)
        return format_pyarrow_preview(pa_table, total_rows=table.row_count)

    def _get_delta_version_preview(self, data_path: Path, version: int, limit: int) -> CatalogTablePreview:
        """Read a Delta table preview at a specific version via the worker (or locally)."""
        table_path = str(data_path)
        if _should_offload():
            try:
                return trigger_delta_version_preview(data_path.name, version, limit)
            except Exception:
                logger.warning("Worker delta version preview failed, falling back to local", exc_info=True)

        dt = DeltaTable(table_path, version=version)
        dataset = dt.to_pyarrow_dataset()
        pa_table = dataset.head(limit)
        return format_pyarrow_preview(pa_table)

    def get_table_history(self, table_id: int, limit: int | None = None) -> DeltaTableHistory:
        """Return the version history for a Delta catalog table.

        Returns an empty history for non-Delta tables.

        Raises
        ------
        TableNotFoundError
            If the table doesn't exist.
        """
        table = self.repo.get_table(table_id)
        if table is None:
            raise TableNotFoundError(table_id=table_id)

        if not table.file_path:
            return DeltaTableHistory(current_version=0, history=[])

        data_path = Path(table.file_path)
        if not is_delta_table(data_path):
            return DeltaTableHistory(current_version=0, history=[])

        table_path = str(data_path)
        if _should_offload():
            try:
                return trigger_delta_history(data_path.name, limit)
            except Exception:
                logger.warning("Worker delta history read failed, falling back to local", exc_info=True)

        # Local fallback
        dt = DeltaTable(table_path, without_files=True)
        raw_history = dt.history(limit)
        current_version = dt.version()
        history = _parse_delta_history(raw_history)
        return DeltaTableHistory(current_version=current_version, history=history)

    def add_table_favorite(self, user_id: int, table_id: int) -> TableFavorite:
        """Add a table to user's favourites (idempotent).

        Raises
        ------
        TableNotFoundError
            If the table doesn't exist.
        """
        table = self.repo.get_table(table_id)
        if table is None:
            raise TableNotFoundError(table_id=table_id)
        existing = self.repo.get_table_favorite(user_id, table_id)
        if existing is not None:
            return existing
        fav = TableFavorite(user_id=user_id, table_id=table_id)
        return self.repo.add_table_favorite(fav)

    def remove_table_favorite(self, user_id: int, table_id: int) -> None:
        """Remove a table from user's favourites.

        Raises
        ------
        TableFavoriteNotFoundError
            If the favourite doesn't exist.
        """
        existing = self.repo.get_table_favorite(user_id, table_id)
        if existing is None:
            raise TableFavoriteNotFoundError(user_id=user_id, table_id=table_id)
        self.repo.remove_table_favorite(user_id, table_id)

    def list_table_favorites(self, user_id: int) -> list[CatalogTableOut]:
        """List all tables the user has favourited, enriched."""
        favs = self.repo.list_table_favorites(user_id)
        tables: list[CatalogTable] = []
        for fav in favs:
            table = self.repo.get_table(fav.table_id)
            if table is not None:
                tables.append(table)
        return self._bulk_enrich_tables(tables, user_id)

    def _schedule_to_out(self, schedule: FlowSchedule) -> FlowScheduleOut:
        """Convert a FlowSchedule ORM instance to its Pydantic output schema, populating trigger table info."""
        # Resolve single table trigger
        trigger_table_name: str | None = None
        trigger_namespace_id: int | None = None
        trigger_namespace_name: str | None = None
        trigger_full_table_name: str | None = None
        if schedule.trigger_table_id is not None:
            table = self.repo.get_table(schedule.trigger_table_id)
            if table is not None:
                trigger_table_name = table.name
                trigger_namespace_id = table.namespace_id
                trigger_namespace_name = self._resolve_namespace_name(table.namespace_id)
                trigger_full_table_name = self._format_full_name(trigger_namespace_name, table.name)

        # Resolve table set trigger IDs and names
        trigger_table_ids: list[int] = []
        trigger_table_names: list[str] = []
        trigger_full_table_names: list[str] = []
        if schedule.schedule_type == "table_set_trigger":
            trigger_table_ids = self.repo.get_trigger_table_ids(schedule.id)
            for tid in trigger_table_ids:
                table = self.repo.get_table(tid)
                if table is None:
                    trigger_table_names.append(f"#{tid}")
                    trigger_full_table_names.append(f"#{tid}")
                    continue
                trigger_table_names.append(table.name)
                ns_name = self._resolve_namespace_name(table.namespace_id)
                trigger_full_table_names.append(self._format_full_name(ns_name, table.name))

        return FlowScheduleOut(
            id=schedule.id,
            registration_id=schedule.registration_id,
            owner_id=schedule.owner_id,
            enabled=schedule.enabled,
            name=schedule.name,
            description=schedule.description,
            schedule_type=schedule.schedule_type,
            interval_seconds=schedule.interval_seconds,
            trigger_table_id=schedule.trigger_table_id,
            trigger_table_name=trigger_table_name,
            trigger_namespace_id=trigger_namespace_id,
            trigger_namespace_name=trigger_namespace_name,
            trigger_full_table_name=trigger_full_table_name,
            trigger_table_ids=trigger_table_ids,
            trigger_table_names=trigger_table_names,
            trigger_full_table_names=trigger_full_table_names,
            last_triggered_at=schedule.last_triggered_at,
            last_trigger_table_updated_at=schedule.last_trigger_table_updated_at,
            created_at=schedule.created_at,
            updated_at=schedule.updated_at,
        )

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
        """Create a new schedule for a registered flow.

        Raises
        ------
        FlowNotFoundError
            If the flow doesn't exist.
        ValueError
            If validation fails (bad type, interval too short, missing trigger table).
        TableNotFoundError
            If the trigger table doesn't exist.
        """
        flow = self.repo.get_flow(registration_id)
        if flow is None:
            raise FlowNotFoundError(registration_id=registration_id)

        validate_schedule_create(
            schedule_type=schedule_type,
            interval_seconds=interval_seconds,
            trigger_table_id=trigger_table_id,
            trigger_table_ids=trigger_table_ids,
            table_exists=lambda table_id: self.repo.get_table(table_id) is not None,
        )

        schedule = FlowSchedule(
            registration_id=registration_id,
            owner_id=owner_id,
            enabled=enabled,
            name=name,
            description=description,
            schedule_type=schedule_type,
            interval_seconds=interval_seconds,
            trigger_table_id=trigger_table_id,
        )
        schedule = self.repo.create_schedule(schedule)

        if schedule_type == "table_set_trigger" and trigger_table_ids:
            self.repo.set_trigger_table_ids(schedule.id, trigger_table_ids)

        return self._schedule_to_out(schedule)

    def update_schedule(
        self,
        schedule_id: int,
        enabled: bool | None = None,
        interval_seconds: int | None = None,
        name: str | None = None,
        description: str | None = None,
    ) -> FlowScheduleOut:
        """Update a schedule.

        Raises
        ------
        ScheduleNotFoundError
            If the schedule doesn't exist.
        """
        schedule = self.repo.get_schedule(schedule_id)
        if schedule is None:
            raise ScheduleNotFoundError(schedule_id=schedule_id)
        if enabled is not None:
            schedule.enabled = enabled
        if interval_seconds is not None:
            validate_schedule_update(interval_seconds)
            schedule.interval_seconds = interval_seconds
        if name is not None:
            schedule.name = name
        if description is not None:
            schedule.description = description
        schedule = self.repo.update_schedule(schedule)
        return self._schedule_to_out(schedule)

    def delete_schedule(self, schedule_id: int) -> None:
        """Delete a schedule and its associated trigger table links.

        Raises
        ------
        ScheduleNotFoundError
            If the schedule doesn't exist.
        """
        schedule = self.repo.get_schedule(schedule_id)
        if schedule is None:
            raise ScheduleNotFoundError(schedule_id=schedule_id)
        self.repo.delete_schedule(schedule_id)  # Also cleans up ScheduleTriggerTable rows

    def get_schedule(self, schedule_id: int) -> FlowScheduleOut:
        """Get a schedule by ID.

        Raises
        ------
        ScheduleNotFoundError
            If the schedule doesn't exist.
        """
        schedule = self.repo.get_schedule(schedule_id)
        if schedule is None:
            raise ScheduleNotFoundError(schedule_id=schedule_id)
        return self._schedule_to_out(schedule)

    def list_schedules(
        self,
        registration_id: int | None = None,
    ) -> list[FlowScheduleOut]:
        """List schedules, optionally filtered by flow."""
        schedules = self.repo.list_schedules(registration_id=registration_id)
        return [self._schedule_to_out(s) for s in schedules]

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
        """Manually trigger a scheduled flow immediately.

        Raises
        ------
        ScheduleNotFoundError
            If the schedule doesn't exist.
        FlowNotFoundError
            If the associated flow doesn't exist.
        FlowAlreadyRunningError
            If the flow already has an active (unfinished) run.
        """
        schedule = self.repo.get_schedule(schedule_id)
        if schedule is None:
            raise ScheduleNotFoundError(schedule_id=schedule_id)

        flow = self.repo.get_flow(schedule.registration_id)
        if flow is None:
            raise FlowNotFoundError(registration_id=schedule.registration_id)

        # Check for active runs
        if self.repo.has_active_run(schedule.registration_id):
            raise FlowAlreadyRunningError(registration_id=schedule.registration_id)

        run = self._spawn_flow_run(flow, user_id=user_id, run_type="on_demand", schedule_id=schedule.id)
        return self._run_to_out(run)

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
        """Return a mapping of logical table name -> directory name for all Delta catalog tables."""
        tables = self.repo.list_tables()
        return {
            table.name: Path(table.file_path).name
            for table in tables
            if table.file_path and is_delta_table(Path(table.file_path))
        }

    def resolve_all_queryable_tables(self) -> tuple[dict[str, str], dict[str, int]]:
        """Return Delta + virtual name maps, keyed by qualified name and by bare name (when unique)."""
        tables = self.repo.list_tables()
        bare_counts: dict[str, int] = {}
        for t in tables:
            if t.table_type == "virtual" or (t.file_path and is_delta_table(Path(t.file_path))):
                bare_counts[t.name] = bare_counts.get(t.name, 0) + 1

        delta_map: dict[str, str] = {}
        virtual_map: dict[str, int] = {}
        for table in tables:
            ns_name = self._resolve_namespace_name(table.namespace_id)
            qualified = self._format_full_name(ns_name, table.name)
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

        delta_map, virtual_map = self.resolve_all_queryable_tables()
        if not delta_map and not virtual_map:
            return SqlQueryResult(error="No catalog tables available")

        query = _rewrite_qualified_references(query, {*delta_map, *virtual_map})
        referenced_virtuals = {vname for vname in virtual_map if _is_table_reference(vname, query)}

        virtual_refs: dict[str, str] = {}
        ipc_path_by_id: dict[int, str] = {}
        for vname in referenced_virtuals:
            vid = virtual_map[vname]
            if vid not in ipc_path_by_id:
                try:
                    lf = self.resolve_virtual_flow_table(vid, user_id=user_id, run_location="remote")
                    versions_hash = _hash_source_versions(self.repo.get_table(vid).source_table_versions)
                    res = trigger_resolve_virtual_table(vid, lf.serialize(), versions_hash)
                    ipc_path_by_id[vid] = res["ipc_path"]
                except Exception:
                    logger.warning("Could not resolve virtual table %r for SQL", vname)
                    continue
            virtual_refs[vname] = ipc_path_by_id[vid]

        try:
            result = trigger_sql_query(query, delta_map, max_rows, virtual_refs=virtual_refs or None)
            return SqlQueryResult(**result)
        except RuntimeError as e:
            return SqlQueryResult(error=str(e))

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

        Builds a flow YAML with catalog_reader nodes (one per used table)
        connected to a single sql_query node, saves it, and registers it.

        Returns the registration ID.
        """
        import json

        from flowfile_core.flowfile.utils import create_unique_id

        used_tables = used_tables or []
        flow_id = create_unique_id()

        # Build nodes
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

        # SQL query node
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

        # Register in catalog
        flow = self.register_flow(
            name=name,
            flow_path=str(flow_path),
            owner_id=owner_id,
            namespace_id=namespace_id,
            description=description,
        )
        return flow.id

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
