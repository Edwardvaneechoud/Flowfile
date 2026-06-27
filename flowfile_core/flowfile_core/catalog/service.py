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
from typing import TYPE_CHECKING, Literal

import polars as pl

from flowfile_core.catalog.constants import (
    DEFAULT_PREVIEW_LIMIT,
    DEFAULT_SQL_MAX_ROWS,
)
from flowfile_core.catalog.exceptions import (
    DashboardNotFoundError,
    NotAuthorizedError,
    NotebookNotFoundError,
    VisualizationNotFoundError,
)
from flowfile_core.catalog.repository import CatalogRepository

if TYPE_CHECKING:
    from flowfile_core.catalog.access import AccessResolver
    from flowfile_core.catalog.storage_backend import CatalogStorageTarget
from flowfile_core.catalog.serializers import (
    VizEnrichment,
    format_pyarrow_preview,
)
from flowfile_core.catalog.services.engagement import FlowEngagementService
from flowfile_core.catalog.services.flows import FlowRegistrationService
from flowfile_core.catalog.services.namespaces import NamespaceService
from flowfile_core.catalog.services.notebooks import NotebookService
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
from flowfile_core.catalog.text_utils import hash_source_versions as _hash_source_versions  # noqa: F401
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
from flowfile_core.database import models as db_models
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
    NotebookCreate,
    NotebookOut,
    NotebookSummaryOut,
    NotebookUpdate,
    OptimizeTableResponse,
    PaginatedFlowRuns,
    SqlQueryResult,
    VacuumTableResponse,
    VisualizationComputeResponse,
    VisualizationCreate,
    VisualizationFieldsResponse,
    VisualizationOut,
    VisualizationUpdate,
    VizSourceDescriptor,
)
from flowfile_core.schemas.sharing_schema import AccessInfo

logger = logging.getLogger(__name__)
viz_logger = logger.getChild("viz")

_OWNER_ACCESS = AccessInfo(is_owner=True, access_level="owner")


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

    def __init__(self, repo: CatalogRepository, access: AccessResolver | None = None) -> None:
        self.repo = repo
        # None for internal callers (scheduler, kafka sync, flow execution),
        # electron mode, and tests → fully unrestricted, today's behavior.
        # Set by routes/catalog.py for per-request private-by-default filtering.
        self.access = access
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

        self._previews = TablePreviewService(repo, self._tables, self._virtual_tables)
        self._visualizations = VisualizationService(
            repo, self._namespaces, self._tables, self._virtual_tables, self._sql
        )
        # Tests monkeypatch ``CatalogService.resolve_virtual_flow_table`` and
        # ``CatalogService._fire_table_trigger_schedules``; binding the facade
        # on these sub-services lets those overrides flow through.
        self._visualizations.bind_facade(self)
        self._sql.bind_facade(self)
        self._schedules.bind_facade(self)

        self._notebooks = NotebookService(repo, self._namespaces)

        self._stats = StatsService(repo, self._flows, self._runs, self._tables)

    # ------------------------------------------------------------------ #
    # Authorization helpers (no-op when self.access is None / unrestricted)
    # ------------------------------------------------------------------ #

    @property
    def _restricted(self) -> bool:
        return self.access is not None and self.access.restricted

    def _require_use(self, resource_type: str, resource_id: int) -> None:
        if self._restricted:
            self.access.require_use(resource_type, resource_id)

    def _require_manage(self, resource_type: str, resource_id: int) -> None:
        if self._restricted:
            self.access.require_manage(resource_type, resource_id)

    def _require_namespace_writable(self, namespace_id: int | None) -> None:
        """Creating/moving items requires write access to the target namespace:
        public, owned, or manage-granted. A use-level grant is read-only."""
        if self._restricted and namespace_id is not None and namespace_id not in self.access.writable_namespace_ids():
            raise NotAuthorizedError(self.access.user_id or -1, "create items in this namespace")

    def _require_use_run(self, run_id: int) -> None:
        """A run is accessible to its actor or to anyone who can use its flow."""
        if not self._restricted:
            return
        run = self._runs.get_run(run_id)  # raises RunNotFoundError if missing
        if run.user_id == self.access.user_id:
            return
        if run.registration_id is not None and self.access.can_use("flow", run.registration_id):
            return
        raise NotAuthorizedError(self.access.user_id or -1, "access this run")

    def _require_manage_schedule(self, schedule_id: int) -> None:
        """Editing a schedule needs schedule ownership, manage on its flow, or admin."""
        if not self._restricted:
            return
        schedule = self._schedules.get_schedule(schedule_id)  # raises ScheduleNotFoundError if missing
        if schedule.owner_id == self.access.user_id:
            return
        if self.access.can_manage("flow", schedule.registration_id):
            return
        raise NotAuthorizedError(self.access.user_id or -1, "modify this schedule")

    def _require_use_visualization(self, viz_id: int) -> None:
        """Viz read: creator, a use/manage grant, or read on its parent table."""
        if not self._restricted:
            return
        viz = self.repo.get_visualization(viz_id)
        if viz is None:
            raise VisualizationNotFoundError(viz_id=viz_id)
        if viz.created_by == self.access.user_id:
            return
        if self.access.can_use("visualization", viz_id, owner_id=viz.created_by):
            return
        if viz.catalog_table_id is not None and self.access.can_use("catalog_table", viz.catalog_table_id):
            return
        raise NotAuthorizedError(self.access.user_id or -1, "access this visualization")

    def _require_manage_visualization(self, viz_id: int) -> None:
        if not self._restricted:
            return
        viz = self.repo.get_visualization(viz_id)
        if viz is None:
            raise VisualizationNotFoundError(viz_id=viz_id)
        if viz.created_by == self.access.user_id:
            return
        if not self.access.can_manage("visualization", viz_id, owner_id=viz.created_by):
            raise NotAuthorizedError(self.access.user_id or -1, "modify this visualization")

    def _require_use_dashboard(self, dashboard_id: int) -> None:
        if not self._restricted:
            return
        dashboard = self.repo.get_dashboard(dashboard_id)
        if dashboard is None:
            raise DashboardNotFoundError(dashboard_id=dashboard_id)
        if dashboard.created_by == self.access.user_id:
            return
        if not self.access.can_use("dashboard", dashboard_id, owner_id=dashboard.created_by):
            raise NotAuthorizedError(self.access.user_id or -1, "access this dashboard")

    def _require_manage_dashboard(self, dashboard_id: int) -> None:
        if not self._restricted:
            return
        dashboard = self.repo.get_dashboard(dashboard_id)
        if dashboard is None:
            raise DashboardNotFoundError(dashboard_id=dashboard_id)
        if dashboard.created_by == self.access.user_id:
            return
        if not self.access.can_manage("dashboard", dashboard_id, owner_id=dashboard.created_by):
            raise NotAuthorizedError(self.access.user_id or -1, "modify this dashboard")

    def _require_use_viz_source(self, source) -> None:
        """Guard an ad-hoc viz source. Table sources require read on that table."""
        if self._restricted and getattr(source, "source_type", None) == "table" and source.table_id is not None:
            self._require_use("catalog_table", source.table_id)

    def _require_use_notebook(self, notebook_id: int) -> None:
        """Notebook read: owner, a use/manage grant, or an inherited namespace grant."""
        if not self._restricted:
            return
        nb = self.repo.get_notebook(notebook_id)
        if nb is None:
            raise NotebookNotFoundError(notebook_id=notebook_id)
        if nb.owner_id == self.access.user_id:
            return
        if self.access.can_use("catalog_notebook", notebook_id, owner_id=nb.owner_id):
            return
        raise NotAuthorizedError(self.access.user_id or -1, "access this notebook")

    def _require_manage_notebook(self, notebook_id: int) -> None:
        if not self._restricted:
            return
        nb = self.repo.get_notebook(notebook_id)
        if nb is None:
            raise NotebookNotFoundError(notebook_id=notebook_id)
        if nb.owner_id == self.access.user_id:
            return
        if not self.access.can_manage("catalog_notebook", notebook_id, owner_id=nb.owner_id):
            raise NotAuthorizedError(self.access.user_id or -1, "modify this notebook")

    def _filter_by_access(self, items: list, resource_type: str) -> list:
        if not self._restricted:
            return items
        allowed = self.access.accessible_ids(resource_type)
        return [item for item in items if item.id in allowed]

    # -- access annotation (stamps the DTO .access field for the frontend) ---

    def _access_detail_map(self, resource_type: str) -> dict:
        """resource_id -> AccessInfo for own + group-granted items (restricted mode only)."""
        from flowfile_core.auth import sharing

        details = sharing.granted_access_details(
            self.access.db,
            self.access.user_id,
            resource_type,
            group_ids=self.access.group_ids(),
            ns_perms=self.access._ns_perms_for(resource_type),
        )
        granter_ids = {by for _perm, by in details.values() if by is not None}
        usernames = {}
        if granter_ids:
            usernames = dict(
                self.access.db.query(db_models.User.id, db_models.User.username).filter(
                    db_models.User.id.in_(granter_ids)
                )
            )
        out = {}
        for rid, (perm, by) in details.items():
            out[rid] = AccessInfo(is_owner=False, access_level=perm, shared_by=usernames.get(by))
        return out

    def _stamp_access(self, items: list, resource_type: str, detail_map: dict | None = None):
        """Set ``.access`` on each DTO: owner → owner; granted → use/manage; else None."""
        if not self._restricted:
            return items
        from flowfile_core.auth import sharing

        owner_attr = sharing.RESOURCE_REGISTRY[resource_type].owner_attr
        details = self._access_detail_map(resource_type) if detail_map is None else detail_map
        uid = self.access.user_id
        for item in items:
            if item is None:
                continue
            owner = getattr(item, owner_attr, None)
            if owner == uid:
                item.access = _OWNER_ACCESS
            elif item.id in details:
                item.access = details[item.id]
        return items

    # ------------------------------------------------------------------ #
    # Private helpers
    # ------------------------------------------------------------------ #

    _reject_dot_in_name = staticmethod(reject_dot_in_name)
    _format_full_name = staticmethod(format_full_name)

    def _resolve_namespace_name(self, namespace_id: int | None) -> str | None:
        """Look up a namespace's name by ID, returning ``None`` when absent."""
        return self._namespaces.resolve_namespace_name(namespace_id)

    def resolve_namespace_id_by_full_name(self, full_name: str | None) -> int | None:
        """Resolve a portable ``"catalog.schema"`` reference to an existing namespace id (resolve-only).

        Name-first resolution for catalog-writer / model nodes whose numeric ``namespace_id`` is
        install-local and goes stale when namespaces are recreated.
        """
        if not full_name:
            return None
        catalog_name, _, schema_name = full_name.partition(".")
        return self._namespaces.resolve_namespace_id_by_path(catalog_name, schema_name or None)

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
        self._require_namespace_writable(parent_id)
        return self._namespaces.create_namespace(name, owner_id, parent_id, description)

    def update_namespace(
        self,
        namespace_id: int,
        name: str | None = None,
        description: str | None = None,
    ) -> CatalogNamespace:
        """Update a namespace's name and/or description."""
        self._require_manage("catalog_namespace", namespace_id)
        return self._namespaces.update_namespace(namespace_id, name, description)

    def delete_namespace(self, namespace_id: int) -> None:
        """Delete a namespace if it has no children, flows or tables."""
        self._require_manage("catalog_namespace", namespace_id)
        self._namespaces.delete_namespace(namespace_id)

    def get_namespace(self, namespace_id: int) -> CatalogNamespace:
        """Retrieve a single namespace by ID."""
        return self._namespaces.get_namespace(namespace_id)

    def list_namespaces(self, parent_id: int | None = None) -> list[CatalogNamespace]:
        """List namespaces, optionally filtered by parent."""
        namespaces = self._namespaces.list_namespaces(parent_id)
        if self._restricted:
            visible = self.access.visible_namespace_ids()
            namespaces = [ns for ns in namespaces if ns.id in visible]
        return namespaces

    def get_namespace_tree(self, user_id: int) -> list[NamespaceTree]:
        """Build the full catalog tree with flows, tables and visualizations nested under schemas."""
        tree = self._namespaces.get_namespace_tree(
            user_id,
            list_visualizations=lambda uid: self._visualizations.list_visualization_library(uid),
            list_notebooks=lambda uid: self._notebooks.list_notebooks(uid),
            bulk_enrich_tables=self._tables.bulk_enrich_tables,
            bulk_enrich_flows=self._flows.bulk_enrich_flows,
        )
        if not self._restricted:
            return tree
        return self._filter_namespace_tree(tree)

    def _filter_namespace_tree(self, tree: list[NamespaceTree]) -> list[NamespaceTree]:
        """Private-by-default tree: per-namespace items filtered to accessible ones
        (own ∪ granted, incl. namespace-inherited), and each kept item + namespace
        stamped with its `.access`. A namespace is kept when it is visible
        (public/owned/granted) OR still has any visible child (context-only ancestor)."""
        visible_ns = self.access.visible_namespace_ids()
        accessible = {
            t: self.access.accessible_ids(t)
            for t in ("flow", "catalog_table", "visualization", "global_artifact", "catalog_notebook")
        }
        # Compute the per-type granted-detail maps once (reused for every node).
        details = {
            t: self._access_detail_map(t)
            for t in (
                "flow",
                "catalog_table",
                "visualization",
                "global_artifact",
                "catalog_notebook",
                "catalog_namespace",
            )
        }

        def _prune(node: NamespaceTree) -> NamespaceTree | None:
            node.flows = self._stamp_access(
                [f for f in node.flows if f.id in accessible["flow"]], "flow", details["flow"]
            )
            node.tables = self._stamp_access(
                [t for t in node.tables if t.id in accessible["catalog_table"]],
                "catalog_table",
                details["catalog_table"],
            )
            node.visualizations = self._stamp_access(
                [v for v in node.visualizations if v.id in accessible["visualization"]],
                "visualization",
                details["visualization"],
            )
            node.notebooks = self._stamp_access(
                [n for n in node.notebooks if n.id in accessible["catalog_notebook"]],
                "catalog_notebook",
                details["catalog_notebook"],
            )
            node.artifacts = self._stamp_access(
                [a for a in node.artifacts if a.id in accessible["global_artifact"]],
                "global_artifact",
                details["global_artifact"],
            )
            node.children = [c for c in (_prune(child) for child in node.children) if c is not None]
            self._stamp_access([node], "catalog_namespace", details["catalog_namespace"])
            has_items = bool(
                node.flows or node.tables or node.visualizations or node.notebooks or node.artifacts or node.children
            )
            if node.id in visible_ns:
                return node
            if has_items:
                # Context-only ancestor: kept so granted children can render, but its
                # own metadata is not the user's to read — redact the description (the
                # name stays: it is the breadcrumb to the granted item).
                node.description = None
                return node
            return None

        return [n for n in (_prune(node) for node in tree) if n is not None]

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
        self._require_namespace_writable(namespace_id)
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
        self._require_manage("flow", registration_id)
        if namespace_id is not None:
            self._require_namespace_writable(namespace_id)
        return self._flows.update_flow(registration_id, requesting_user_id, name, description, namespace_id)

    def delete_flow(self, registration_id: int, delete_file: bool = False) -> None:
        """Delete a flow and its related favourites/follows (optionally its file)."""
        self._require_manage("flow", registration_id)
        self._flows.delete_flow(registration_id, delete_file)

    def get_flow(self, registration_id: int, user_id: int) -> FlowRegistrationOut:
        """Get an enriched flow registration."""
        self._require_use("flow", registration_id)
        flow = self._flows.get_flow(registration_id, user_id)
        return self._stamp_access([flow], "flow")[0]

    def list_flows(self, user_id: int, namespace_id: int | None = None) -> list[FlowRegistrationOut]:
        """List flows, optionally filtered by namespace, enriched with user context."""
        flows = self._filter_by_access(self._flows.list_flows(user_id, namespace_id), "flow")
        return self._stamp_access(flows, "flow")

    def list_artifacts_for_flow(self, registration_id: int) -> list[GlobalArtifactOut]:
        """List all active artifacts produced by a registered flow."""
        self._require_use("flow", registration_id)
        return self._stamp_access(self._flows.list_artifacts_for_flow(registration_id), "global_artifact")

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
        search: str | None = None,
    ) -> PaginatedFlowRuns:
        """List run summaries (without snapshots) with total count for pagination."""
        if self._restricted and registration_id is not None:
            self._require_use("flow", registration_id)
        elif self._restricted and schedule_id is not None:
            schedule = self._schedules.get_schedule(schedule_id)
            self._require_use("flow", schedule.registration_id)
        result = self._runs.list_runs(registration_id, schedule_id, run_type, limit, offset, search)
        if self._restricted and registration_id is None and schedule_id is None:
            # Global list: best-effort page filter to own runs ∪ runs of accessible
            # flows (page totals may drift; by-id run reads are separately guarded).
            allowed_flows = self.access.accessible_ids("flow")
            user_id = self.access.user_id
            result.runs = [r for r in result.runs if r.user_id == user_id or (r.registration_id in allowed_flows)]
        return result

    def get_run_detail(self, run_id: int) -> FlowRunDetail:
        """Get a single run including the YAML snapshot."""
        self._require_use_run(run_id)
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

    def ensure_python_editor_flows_namespace(self) -> CatalogNamespace | None:
        """Ensure the ``General > Python Editor`` namespace exists, creating it if missing."""
        return self._flows.ensure_python_editor_flows_namespace()

    def resolve_registration_id(self, flow_path: str) -> int | None:
        """Look up the registration ID for a flow by its file path."""
        return self._flows.resolve_registration_id(flow_path)

    def get_run_snapshot(self, run_id: int) -> str:
        """Return the flow snapshot text for a run."""
        self._require_use_run(run_id)
        return self._runs.get_run_snapshot(run_id)

    # ------------------------------------------------------------------ #
    # Favorites
    # ------------------------------------------------------------------ #

    def add_favorite(self, user_id: int, registration_id: int) -> FlowFavorite:
        """Add a flow to the user's favourites (idempotent)."""
        self._require_use("flow", registration_id)
        return self._engagement.add_favorite(user_id, registration_id)

    def remove_favorite(self, user_id: int, registration_id: int) -> None:
        """Remove a flow from the user's favourites."""
        self._engagement.remove_favorite(user_id, registration_id)

    def list_favorites(self, user_id: int) -> list[FlowRegistrationOut]:
        """List all flows the user has favourited, enriched."""
        flows = self._filter_by_access(self._engagement.list_favorites(user_id), "flow")
        return self._stamp_access(flows, "flow")

    def add_follow(self, user_id: int, registration_id: int) -> FlowFollow:
        """Follow a flow (idempotent)."""
        self._require_use("flow", registration_id)
        return self._engagement.add_follow(user_id, registration_id)

    def remove_follow(self, user_id: int, registration_id: int) -> None:
        """Unfollow a flow."""
        self._engagement.remove_follow(user_id, registration_id)

    def list_following(self, user_id: int) -> list[FlowRegistrationOut]:
        """List all flows the user is following, enriched."""
        flows = self._filter_by_access(self._engagement.list_following(user_id), "flow")
        return self._stamp_access(flows, "flow")

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
        self._require_namespace_writable(namespace_id)
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
        partition_columns: list[str] | None = None,
    ) -> CatalogTableOut:
        """Register an already-materialized table (Delta or Parquet) without copying its data."""
        self._require_namespace_writable(namespace_id)
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
            partition_columns,
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
        partition_columns: list[str] | None = None,
    ) -> CatalogTableOut:
        """Replace the data of an existing catalog table in-place, preserving its ID."""
        self._require_manage("catalog_table", table_id)
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
            partition_columns,
        )

    def _fire_table_trigger_schedules(self, table_id: int, table_updated_at: datetime) -> int:
        """Fire enabled ``table_trigger`` schedules watching *table_id* (push path)."""
        return self._schedules.fire_table_trigger_schedules(table_id, table_updated_at)

    def resolve_write_destination(
        self,
        table_name: str,
        namespace_id: int | None,
        write_mode: str,
        target: CatalogStorageTarget,
    ) -> tuple[CatalogTable | None, str, str]:
        """Resolve the destination (local path or object-storage URI) and Delta write mode."""
        return self._tables.resolve_write_destination(table_name, namespace_id, write_mode, target)

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
        self._require_use("catalog_table", table_id)
        return self._stamp_access([self._tables.get_table(table_id, user_id)], "catalog_table")[0]

    def resolve_table_out(
        self,
        reference: str,
        default_namespace_id: int | None = None,
        strict: bool = False,
        user_id: int | None = None,
    ) -> tuple[CatalogTableOut, list[dict]]:
        """Resolve a reference and return its DTO plus ambiguity warnings (empty when unambiguous)."""
        result, warnings = self._tables.resolve_table_out(reference, default_namespace_id, strict, user_id)
        self._require_use("catalog_table", result.id)
        return self._stamp_access([result], "catalog_table")[0], warnings

    def list_tables(self, namespace_id: int | None = None, user_id: int | None = None) -> list[CatalogTableOut]:
        """List tables, optionally filtered by namespace."""
        tables = self._filter_by_access(self._tables.list_tables(namespace_id, user_id), "catalog_table")
        return self._stamp_access(tables, "catalog_table")

    def update_table(
        self,
        table_id: int,
        name: str | None = None,
        description: str | None = None,
        namespace_id: int | None = None,
    ) -> CatalogTableOut:
        """Update a catalog table's metadata."""
        self._require_manage("catalog_table", table_id)
        if namespace_id is not None:
            self._require_namespace_writable(namespace_id)
        return self._tables.update_table(table_id, name, description, namespace_id)

    def delete_table(self, table_id: int, delete_file: bool = False) -> None:
        """Delete a catalog table; optionally delete its managed storage (Delta dir / Parquet)."""
        self._require_manage("catalog_table", table_id)
        self._tables.delete_table(table_id, delete_file)

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
        """Create a virtual flow table (non-materialised catalog entry)."""
        self._require_namespace_writable(namespace_id)
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
        """Update a virtual flow table's metadata or producer."""
        self._require_manage("catalog_table", table_id)
        if namespace_id is not None:
            self._require_namespace_writable(namespace_id)
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
        """Create a query-based virtual table from a SQL expression."""
        self._require_namespace_writable(namespace_id)
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
        self._require_manage("catalog_table", table_id)
        if namespace_id is not None:
            self._require_namespace_writable(namespace_id)
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
        self._require_use("catalog_table", table_id)
        return self._previews.get_table_preview(table_id, limit, version, user_id)

    def resolve_virtual_flow_table_preview(
        self,
        table_id: int,
        limit: int,
        user_id: int | None = None,
    ) -> CatalogTablePreview:
        """Resolve a virtual flow table and return a preview (worker-backed)."""
        self._require_use("catalog_table", table_id)
        return self._previews.resolve_virtual_flow_table_preview(table_id, limit, user_id)

    def get_table_history(self, table_id: int, limit: int | None = None) -> DeltaTableHistory:
        """Return the version history for a Delta catalog table."""
        self._require_use("catalog_table", table_id)
        return self._previews.get_table_history(table_id, limit)

    def optimize_table(self, table_id: int, z_order_columns: list[str] | None = None) -> OptimizeTableResponse:
        """Compact (and optionally Z-order) a Delta catalog table."""
        return self._tables.optimize_table(table_id, z_order_columns)

    def vacuum_table(
        self,
        table_id: int,
        retention_hours: int = 168,
        dry_run: bool = True,
    ) -> VacuumTableResponse:
        """Vacuum tombstoned files from a Delta catalog table."""
        return self._tables.vacuum_table(table_id, retention_hours=retention_hours, dry_run=dry_run)

    def add_table_favorite(self, user_id: int, table_id: int) -> TableFavorite:
        """Add a table to the user's favourites (idempotent)."""
        self._require_use("catalog_table", table_id)
        return self._tables.add_table_favorite(user_id, table_id)

    def remove_table_favorite(self, user_id: int, table_id: int) -> None:
        """Remove a table from the user's favourites."""
        self._tables.remove_table_favorite(user_id, table_id)

    def list_table_favorites(self, user_id: int) -> list[CatalogTableOut]:
        """List all tables the user has favourited, enriched."""
        tables = self._filter_by_access(self._tables.list_table_favorites(user_id), "catalog_table")
        return self._stamp_access(tables, "catalog_table")

    def _schedule_to_out(self, schedule: FlowSchedule) -> FlowScheduleOut:
        """Convert a FlowSchedule ORM row to its DTO, populating trigger info."""
        return self._schedules._schedule_to_out(schedule)

    def create_schedule(
        self,
        registration_id: int,
        owner_id: int,
        schedule_type: str,
        interval_seconds: int | None = None,
        cron_expression: str | None = None,
        cron_timezone: str | None = None,
        trigger_table_id: int | None = None,
        trigger_table_ids: list[int] | None = None,
        enabled: bool = True,
        name: str | None = None,
        description: str | None = None,
    ) -> FlowScheduleOut:
        """Create a new schedule (interval, cron, table_trigger or table_set_trigger) for a flow."""
        # use-level on the flow is enough: the schedule runs as its creator
        # (FlowSchedule.owner_id), so secret/connection resolution stays self-consistent.
        self._require_use("flow", registration_id)
        return self._schedules.create_schedule(
            registration_id=registration_id,
            owner_id=owner_id,
            schedule_type=schedule_type,
            interval_seconds=interval_seconds,
            cron_expression=cron_expression,
            cron_timezone=cron_timezone,
            trigger_table_id=trigger_table_id,
            trigger_table_ids=trigger_table_ids,
            enabled=enabled,
            name=name,
            description=description,
        )

    def update_schedule(
        self,
        schedule_id: int,
        enabled: bool | None = None,
        interval_seconds: int | None = None,
        cron_expression: str | None = None,
        cron_timezone: str | None = None,
        name: str | None = None,
        description: str | None = None,
    ) -> FlowScheduleOut:
        """Update a schedule's enabled flag, interval, cron expression/timezone, name or description."""
        self._require_manage_schedule(schedule_id)
        return self._schedules.update_schedule(
            schedule_id=schedule_id,
            enabled=enabled,
            interval_seconds=interval_seconds,
            cron_expression=cron_expression,
            cron_timezone=cron_timezone,
            name=name,
            description=description,
        )

    def delete_schedule(self, schedule_id: int) -> None:
        """Delete a schedule and its associated trigger table links."""
        self._require_manage_schedule(schedule_id)
        self._schedules.delete_schedule(schedule_id)

    def get_schedule(self, schedule_id: int) -> FlowScheduleOut:
        """Get a schedule by ID."""
        schedule = self._schedules.get_schedule(schedule_id)
        if self._restricted and schedule.owner_id != self.access.user_id:
            self._require_use("flow", schedule.registration_id)
        return schedule

    def list_schedules(self, registration_id: int | None = None) -> list[FlowScheduleOut]:
        """List schedules, optionally filtered by flow."""
        if self._restricted and registration_id is not None:
            # Same fail-fast contract as list_runs: by-flow listing needs use on the flow.
            self._require_use("flow", registration_id)
        schedules = self._schedules.list_schedules(registration_id)
        if self._restricted and registration_id is None:
            allowed = self.access.accessible_ids("flow")
            user_id = self.access.user_id
            schedules = [s for s in schedules if s.owner_id == user_id or s.registration_id in allowed]
        return schedules

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
        self._require_use("flow", registration_id)
        return self._runs.run_flow_now(registration_id, user_id)

    def trigger_schedule_now(self, schedule_id: int, user_id: int) -> FlowRunOut:
        """Manually trigger a scheduled flow immediately."""
        if self._restricted:
            schedule = self._schedules.get_schedule(schedule_id)
            self._require_use("flow", schedule.registration_id)
        return self._schedules.trigger_schedule_now(schedule_id, user_id)

    # ------------------------------------------------------------------ #
    # Active runs + cancel
    # ------------------------------------------------------------------ #

    def list_active_runs(self) -> list[ActiveFlowRun]:
        """List all currently running flows (``ended_at IS NULL``)."""
        runs = self._runs.list_active_runs()
        if self._restricted:
            allowed = self.access.accessible_ids("flow")
            user_id = self.access.user_id
            runs = [r for r in runs if r.user_id == user_id or r.registration_id in allowed]
        return runs

    def cancel_run(self, run_id: int) -> None:
        """Cancel a running flow by terminating its subprocess and marking the run failed."""
        self._require_use_run(run_id)
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
        accessible = self.access.accessible_ids("catalog_table") if self._restricted else None
        return self._sql.execute_sql_query(query, max_rows, user_id, accessible_table_ids=accessible)

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
        # Only embed catalog_reader nodes for tables the caller may read, so the
        # generated flow can't be used to exfiltrate another user's table.
        accessible = self.access.accessible_ids("catalog_table") if self._restricted else None
        return self._sql.save_sql_query_as_flow(
            query, name, owner_id, namespace_id, description, used_tables, accessible_table_ids=accessible
        )

    # ================== Visualizations =====================================

    def list_visualizations_for_table(self, table_id: int, user_id: int | None = None) -> list[VisualizationOut]:
        """List visualizations bound to a specific catalog table."""
        self._require_use("catalog_table", table_id)
        return self._stamp_access(
            self._visualizations.list_visualizations_for_table(table_id, user_id), "visualization"
        )

    def list_visualization_library(self, user_id: int | None = None) -> list[VisualizationOut]:
        """Return all saved visualizations as catalog library entries (specs omitted)."""
        vizzes = self._visualizations.list_visualization_library(user_id)
        if self._restricted:
            viz_ids = self.access.accessible_ids("visualization")  # own ∪ granted (+ ns-inherited)
            table_ids = self.access.accessible_ids("catalog_table")
            vizzes = [v for v in vizzes if v.id in viz_ids or (v.catalog_table_id in table_ids)]
        return self._stamp_access(vizzes, "visualization")

    def get_visualization(self, viz_id: int, user_id: int | None = None) -> VisualizationOut:
        """Get a single visualization by ID."""
        self._require_use_visualization(viz_id)
        return self._stamp_access([self._visualizations.get_visualization(viz_id, user_id)], "visualization")[0]

    _validate_thumbnail = staticmethod(validate_thumbnail)
    _validate_viz_source = staticmethod(validate_viz_source)

    def create_visualization(self, payload: VisualizationCreate, user_id: int) -> VisualizationOut:
        """Create a new visualization (table-source or sql-source)."""
        if payload.catalog_table_id is not None:
            self._require_use("catalog_table", payload.catalog_table_id)
        # When namespace_id is None it defaults to the source table's namespace,
        # which the table-use check above already covers.
        self._require_namespace_writable(payload.namespace_id)
        return self._visualizations.create_visualization(payload, user_id)

    def update_visualization(self, viz_id: int, payload: VisualizationUpdate, user_id: int) -> VisualizationOut:
        """Update a visualization's spec, name, namespace or thumbnail."""
        self._require_manage_visualization(viz_id)
        if payload.catalog_table_id is not None:
            self._require_use("catalog_table", payload.catalog_table_id)
        if payload.namespace_id is not None:
            self._require_namespace_writable(payload.namespace_id)
        return self._visualizations.update_visualization(viz_id, payload, user_id)

    def delete_visualization(self, viz_id: int, user_id: int) -> None:
        """Delete a visualization by ID."""
        self._require_manage_visualization(viz_id)
        self._visualizations.delete_visualization(viz_id, user_id)

    def list_notebooks(self, user_id: int | None = None) -> list[NotebookSummaryOut]:
        """List saved notebooks (own ∪ granted in restricted mode)."""
        notebooks = self._notebooks.list_notebooks(user_id)
        notebooks = self._filter_by_access(notebooks, "catalog_notebook")
        return self._stamp_access(notebooks, "catalog_notebook")

    def get_notebook(self, notebook_id: int, user_id: int | None = None) -> NotebookOut:
        """Get a single notebook by ID."""
        self._require_use_notebook(notebook_id)
        return self._stamp_access([self._notebooks.get_notebook(notebook_id, user_id)], "catalog_notebook")[0]

    def create_notebook(self, payload: NotebookCreate, user_id: int) -> NotebookOut:
        """Create a new notebook in a writable namespace."""
        self._require_namespace_writable(payload.namespace_id)
        return self._notebooks.create_notebook(payload, user_id)

    def update_notebook(self, notebook_id: int, payload: NotebookUpdate, user_id: int) -> NotebookOut:
        """Update a notebook's name, cells, namespace or default kernel."""
        self._require_manage_notebook(notebook_id)
        if payload.namespace_id is not None:
            self._require_namespace_writable(payload.namespace_id)
        return self._notebooks.update_notebook(notebook_id, payload, user_id)

    def delete_notebook(self, notebook_id: int, user_id: int) -> None:
        """Delete a notebook by ID."""
        self._require_manage_notebook(notebook_id)
        self._notebooks.delete_notebook(notebook_id, user_id)

    # ================== Dashboards =========================================

    def list_dashboards(self, user_id: int | None = None) -> list[DashboardOut]:
        """List all dashboards."""
        dashboards = self._visualizations.list_dashboards(user_id)
        if self._restricted:
            allowed = self.access.accessible_ids("dashboard")  # own ∪ granted (+ ns-inherited)
            dashboards = [d for d in dashboards if d.id in allowed]
        return self._stamp_access(dashboards, "dashboard")

    def get_dashboard(self, dashboard_id: int, user_id: int | None = None) -> DashboardOut:
        """Get a dashboard by ID."""
        self._require_use_dashboard(dashboard_id)
        return self._stamp_access([self._visualizations.get_dashboard(dashboard_id, user_id)], "dashboard")[0]

    def create_dashboard(self, payload: DashboardCreate, user_id: int) -> DashboardOut:
        """Create a new dashboard."""
        self._require_namespace_writable(payload.namespace_id)
        return self._visualizations.create_dashboard(payload, user_id)

    def update_dashboard(self, dashboard_id: int, payload: DashboardUpdate, user_id: int) -> DashboardOut:
        """Update a dashboard's name, layout, namespace or description."""
        self._require_manage_dashboard(dashboard_id)
        if payload.namespace_id is not None:
            self._require_namespace_writable(payload.namespace_id)
        return self._visualizations.update_dashboard(dashboard_id, payload, user_id)

    def delete_dashboard(self, dashboard_id: int, user_id: int) -> None:
        """Delete a dashboard by ID."""
        self._require_manage_dashboard(dashboard_id)
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
        self._require_use_visualization(viz_id)
        return self._visualizations.compute_saved_visualization_rows(viz_id, max_rows, user_id, payload)

    def get_visualization_fields_for_viz(self, viz_id: int, user_id: int) -> VisualizationFieldsResponse:
        """Return the list of fields available for a saved visualization's source."""
        self._require_use_visualization(viz_id)
        return self._visualizations.get_visualization_fields_for_viz(viz_id, user_id)

    def compute_ad_hoc_visualization(
        self,
        source: VizSourceDescriptor,
        payload: dict,
        max_rows: int | None,
        user_id: int,
    ) -> VisualizationComputeResponse:
        """Compute rows for an ad-hoc viz source via the worker."""
        self._require_use_viz_source(source)
        return self._visualizations.compute_ad_hoc_visualization(source, payload, max_rows, user_id)

    def get_visualization_fields(self, source: VizSourceDescriptor, user_id: int) -> VisualizationFieldsResponse:
        """Return the list of fields available for a viz source descriptor."""
        self._require_use_viz_source(source)
        return self._visualizations.get_visualization_fields(source, user_id)

    def get_table_column_stats(
        self,
        table_id: int,
        column: str,
        limit: int,
        user_id: int,
    ) -> ColumnStatsResponse:
        """Return distinct values plus min/max for a single column on a catalog table."""
        self._require_use("catalog_table", table_id)
        return self._visualizations.get_table_column_stats(table_id, column, limit, user_id)


# Backward-compat re-export for external code that imported the
# underscore-prefixed module-level helper.
_format_pyarrow_preview = format_pyarrow_preview
