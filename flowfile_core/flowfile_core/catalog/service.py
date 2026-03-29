"""Business-logic layer for the Catalog system.

``CatalogService`` encapsulates all domain rules (validation, authorisation,
enrichment) and delegates persistence to a ``CatalogRepository``.  It never
raises ``HTTPException`` — only domain-specific exceptions from
``catalog.exceptions``.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from flowfile_core.catalog.delta_utils import delete_table_storage, is_delta_table, read_delta_preview, table_exists
from flowfile_core.catalog.exceptions import (
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
)
from flowfile_core.catalog.repository import CatalogRepository
from flowfile_core.database.models import (
    CatalogNamespace,
    CatalogTable,
    FlowFavorite,
    FlowFollow,
    FlowRegistration,
    FlowRun,
    FlowSchedule,
    GlobalArtifact,
    TableFavorite,
)
from flowfile_core.schemas.catalog_schema import (
    ActiveFlowRun,
    CatalogStats,
    CatalogTableOut,
    CatalogTablePreview,
    CatalogTableSummary,
    ColumnSchema,
    DeltaTableHistory,
    DeltaVersionCommit,
    FlowRegistrationOut,
    FlowRunDetail,
    FlowRunOut,
    FlowScheduleOut,
    FlowSummary,
    GlobalArtifactOut,
    NamespaceTree,
    PaginatedFlowRuns,
)

logger = logging.getLogger(__name__)


def _format_delta_timestamp(ts: object) -> str | None:
    """Convert a raw delta log timestamp to an ISO 8601 string."""
    if ts is None:
        return None
    if isinstance(ts, str):
        return ts
    if isinstance(ts, datetime):
        return ts.isoformat()
    if isinstance(ts, int | float):
        # Milliseconds since epoch
        return datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat()
    return str(ts)


def _parse_delta_history(raw_history: list[dict]) -> list[DeltaVersionCommit]:
    """Convert raw deltalake history dicts into typed ``DeltaVersionCommit`` models."""
    return [
        DeltaVersionCommit(
            version=h.get("version"),
            timestamp=_format_delta_timestamp(h.get("timestamp")),
            operation=h.get("operation"),
            parameters=h.get("operationParameters"),
        )
        for h in raw_history
    ]


class CatalogService:
    """Coordinates all catalog business logic.

    Parameters
    ----------
    repo:
        Any object satisfying the ``CatalogRepository`` protocol.
    """

    def __init__(self, repo: CatalogRepository) -> None:
        self.repo = repo

    # ------------------------------------------------------------------ #
    # Private helpers
    # ------------------------------------------------------------------ #

    @dataclass(frozen=True)
    class CatalogMaterializationResult:
        table_path: str
        schema: list[dict[str, str]]
        row_count: int
        column_count: int
        size_bytes: int
        storage_format: str = "delta"

    @staticmethod
    def _read_table_metadata(
        table_path: str, storage_format: str
    ) -> tuple[list[dict[str, str]], int, int, int]:
        """Read schema, row_count, column_count, size_bytes from a table.

        Offloads the work to the worker process when available.  Only falls
        back to local I/O when the worker is not running.
        """
        from flowfile_core.configs.settings import OFFLOAD_TO_WORKER

        if OFFLOAD_TO_WORKER:
            try:
                from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import (
                    trigger_read_table_metadata,
                )

                data = trigger_read_table_metadata(table_path, storage_format)
                schema_list = [{"name": c["name"], "dtype": c["dtype"]} for c in data["schema"]]
                return schema_list, data["row_count"], data["column_count"], data["size_bytes"]
            except Exception:
                logger.warning("Worker metadata read failed, falling back to local read", exc_info=True)

        # Fallback: read locally (only when worker is unavailable)
        import polars as pl

        from flowfile_core.catalog.delta_utils import get_delta_table_size_bytes, is_delta_table

        p = Path(table_path)
        if storage_format == "delta" or (storage_format is None and is_delta_table(p)):
            lf = pl.scan_delta(str(p))
            schema = lf.collect_schema()
            schema_list = [{"name": n, "dtype": str(d)} for n, d in schema.items()]
            row_count = lf.select(pl.len()).collect().item()
            size_bytes = get_delta_table_size_bytes(p)
        else:
            lf = pl.scan_parquet(p)
            schema = lf.collect_schema()
            schema_list = [{"name": n, "dtype": str(d)} for n, d in schema.items()]
            row_count = lf.select(pl.len()).collect().item()
            size_bytes = p.stat().st_size
        return schema_list, row_count, len(schema), size_bytes

    def _enrich_flow_registration(self, flow: FlowRegistration, user_id: int) -> FlowRegistrationOut:
        """Attach favourite/follow flags and run stats to a single registration.

        Note: For bulk operations, prefer ``_bulk_enrich_flows`` to avoid N+1 queries.
        """
        is_fav = self.repo.get_favorite(user_id, flow.id) is not None
        is_follow = self.repo.get_follow(user_id, flow.id) is not None
        run_count = self.repo.count_run_for_flow(flow.id)
        last_run = self.repo.last_run_for_flow(flow.id)
        artifact_count = self.repo.count_active_artifacts_for_flow(flow.id)
        produced_tables = self.repo.list_tables_for_flow(flow.id)
        read_tables = self.repo.list_read_tables_for_flow(flow.id)
        return FlowRegistrationOut(
            id=flow.id,
            name=flow.name,
            description=flow.description,
            flow_path=flow.flow_path,
            namespace_id=flow.namespace_id,
            owner_id=flow.owner_id,
            created_at=flow.created_at,
            updated_at=flow.updated_at,
            is_favorite=is_fav,
            is_following=is_follow,
            run_count=run_count,
            last_run_at=last_run.started_at if last_run else None,
            last_run_success=last_run.success if last_run else None,
            file_exists=os.path.exists(flow.flow_path) if flow.flow_path else False,
            artifact_count=artifact_count,
            tables_produced=[
                CatalogTableSummary(id=t.id, name=t.name, namespace_id=t.namespace_id) for t in produced_tables
            ],
            tables_read=[CatalogTableSummary(id=t.id, name=t.name, namespace_id=t.namespace_id) for t in read_tables],
        )

    def _bulk_enrich_flows(self, flows: list[FlowRegistration], user_id: int) -> list[FlowRegistrationOut]:
        """Enrich multiple flows with favourites, follows, and run stats in bulk.

        Uses 3 queries total instead of 4×N, dramatically improving performance
        when listing many flows.
        """
        if not flows:
            return []

        flow_ids = [f.id for f in flows]

        fav_ids = self.repo.bulk_get_favorite_flow_ids(user_id, flow_ids)
        follow_ids = self.repo.bulk_get_follow_flow_ids(user_id, flow_ids)
        run_stats = self.repo.bulk_get_run_stats(flow_ids)
        artifact_counts = self.repo.bulk_get_artifact_counts(flow_ids)
        tables_by_flow = self.repo.bulk_get_tables_for_flows(flow_ids)
        read_tables_by_flow = self.repo.bulk_get_read_tables_for_flows(flow_ids)

        result: list[FlowRegistrationOut] = []
        for flow in flows:
            run_count, last_run = run_stats.get(flow.id, (0, None))
            produced = tables_by_flow.get(flow.id, [])
            read = read_tables_by_flow.get(flow.id, [])
            result.append(
                FlowRegistrationOut(
                    id=flow.id,
                    name=flow.name,
                    description=flow.description,
                    flow_path=flow.flow_path,
                    namespace_id=flow.namespace_id,
                    owner_id=flow.owner_id,
                    created_at=flow.created_at,
                    updated_at=flow.updated_at,
                    is_favorite=flow.id in fav_ids,
                    is_following=flow.id in follow_ids,
                    run_count=run_count,
                    last_run_at=last_run.started_at if last_run else None,
                    last_run_success=last_run.success if last_run else None,
                    file_exists=os.path.exists(flow.flow_path) if flow.flow_path else False,
                    artifact_count=artifact_counts.get(flow.id, 0),
                    tables_produced=[
                        CatalogTableSummary(id=t.id, name=t.name, namespace_id=t.namespace_id) for t in produced
                    ],
                    tables_read=[CatalogTableSummary(id=t.id, name=t.name, namespace_id=t.namespace_id) for t in read],
                )
            )
        return result

    @staticmethod
    def _resolve_log_path(run_id: int, run_type: str) -> str | None:
        """Return the log file path if it exists for subprocess-spawned runs."""
        if run_type not in ("scheduled", "manual", "on_demand"):
            return None
        log_file = Path.home() / ".flowfile" / "logs" / f"scheduled_run_{run_id}.log"
        if log_file.exists():
            return str(log_file)
        return None

    @staticmethod
    def _run_to_out(run: FlowRun) -> FlowRunOut:
        return FlowRunOut(
            id=run.id,
            registration_id=run.registration_id,
            flow_name=run.flow_name,
            flow_path=run.flow_path,
            user_id=run.user_id,
            started_at=run.started_at,
            ended_at=run.ended_at,
            success=run.success,
            nodes_completed=run.nodes_completed,
            number_of_nodes=run.number_of_nodes,
            duration_seconds=run.duration_seconds,
            run_type=run.run_type,
            schedule_id=run.schedule_id,
            has_snapshot=run.flow_snapshot is not None,
            has_log=CatalogService._resolve_log_path(run.id, run.run_type) is not None,
        )

    @staticmethod
    def _artifact_to_out(artifact: GlobalArtifact) -> GlobalArtifactOut:
        """Convert a GlobalArtifact ORM instance to its Pydantic output schema."""
        tags: list[str] = []
        if hasattr(artifact, "tags") and artifact.tags:
            if isinstance(artifact.tags, list):
                tags = artifact.tags
            elif isinstance(artifact.tags, str):
                try:
                    tags = json.loads(artifact.tags)
                except (json.JSONDecodeError, TypeError):
                    tags = [t.strip() for t in artifact.tags.split(",") if t.strip()]

        return GlobalArtifactOut(
            id=artifact.id,
            name=artifact.name,
            version=artifact.version,
            status=artifact.status,
            description=getattr(artifact, "description", None),
            python_type=getattr(artifact, "python_type", None),
            python_module=getattr(artifact, "python_module", None),
            serialization_format=getattr(artifact, "serialization_format", None),
            size_bytes=getattr(artifact, "size_bytes", None),
            sha256=getattr(artifact, "sha256", None),
            tags=tags,
            namespace_id=artifact.namespace_id,
            source_registration_id=getattr(artifact, "source_registration_id", None),
            source_flow_id=getattr(artifact, "source_flow_id", None),
            source_node_id=getattr(artifact, "source_node_id", None),
            owner_id=getattr(artifact, "owner_id", None),
            created_at=getattr(artifact, "created_at", None),
            updated_at=getattr(artifact, "updated_at", None),
        )

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
        """Create a catalog (level 0) or schema (level 1) namespace.

        Raises
        ------
        NamespaceNotFoundError
            If ``parent_id`` is given but doesn't exist.
        NestingLimitError
            If the parent is already at level 1 (schema).
        NamespaceExistsError
            If a namespace with the same name already exists under the parent.
        """
        level = 0
        if parent_id is not None:
            parent = self.repo.get_namespace(parent_id)
            if parent is None:
                raise NamespaceNotFoundError(namespace_id=parent_id)
            if parent.level >= 1:
                raise NestingLimitError(parent_id=parent_id, parent_level=parent.level)
            level = parent.level + 1

        existing = self.repo.get_namespace_by_name(name, parent_id)
        if existing is not None:
            raise NamespaceExistsError(name=name, parent_id=parent_id)

        ns = CatalogNamespace(
            name=name,
            parent_id=parent_id,
            level=level,
            description=description,
            owner_id=owner_id,
        )
        return self.repo.create_namespace(ns)

    def update_namespace(
        self,
        namespace_id: int,
        name: str | None = None,
        description: str | None = None,
    ) -> CatalogNamespace:
        """Update a namespace's name and/or description.

        Raises
        ------
        NamespaceNotFoundError
            If the namespace doesn't exist.
        """
        ns = self.repo.get_namespace(namespace_id)
        if ns is None:
            raise NamespaceNotFoundError(namespace_id=namespace_id)
        if name is not None:
            ns.name = name
        if description is not None:
            ns.description = description
        return self.repo.update_namespace(ns)

    def delete_namespace(self, namespace_id: int) -> None:
        """Delete a namespace if it has no children or flows.

        Raises
        ------
        NamespaceNotFoundError
            If the namespace doesn't exist.
        NamespaceNotEmptyError
            If the namespace has child namespaces or flow registrations.
        """
        ns = self.repo.get_namespace(namespace_id)
        if ns is None:
            raise NamespaceNotFoundError(namespace_id=namespace_id)
        children = self.repo.count_children(namespace_id)
        flows = self.repo.count_flows_in_namespace(namespace_id)
        tables = self.repo.count_tables_in_namespace(namespace_id)
        if children > 0 or flows > 0 or tables > 0:
            raise NamespaceNotEmptyError(namespace_id=namespace_id, children=children, flows=flows, tables=tables)
        self.repo.delete_namespace(namespace_id)

    def get_namespace(self, namespace_id: int) -> CatalogNamespace:
        """Retrieve a single namespace by ID.

        Raises
        ------
        NamespaceNotFoundError
            If the namespace doesn't exist.
        """
        ns = self.repo.get_namespace(namespace_id)
        if ns is None:
            raise NamespaceNotFoundError(namespace_id=namespace_id)
        return ns

    def list_namespaces(self, parent_id: int | None = None) -> list[CatalogNamespace]:
        """List namespaces, optionally filtered by parent."""
        return self.repo.list_namespaces(parent_id)

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

        for cat in catalogs:
            cat_flows = self.repo.list_flows(namespace_id=cat.id)
            namespace_flow_map[cat.id] = cat_flows
            all_flows.extend(cat_flows)
            namespace_artifact_map[cat.id] = [
                self._artifact_to_out(a) for a in self.repo.list_artifacts_for_namespace(cat.id)
            ]
            namespace_table_map[cat.id] = self._bulk_enrich_tables(self.repo.list_tables_for_namespace(cat.id), user_id)

            for schema in self.repo.list_child_namespaces(cat.id):
                schema_flows = self.repo.list_flows(namespace_id=schema.id)
                namespace_flow_map[schema.id] = schema_flows
                all_flows.extend(schema_flows)
                namespace_artifact_map[schema.id] = [
                    self._artifact_to_out(a) for a in self.repo.list_artifacts_for_namespace(schema.id)
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
                )
            )
        return result

    def get_default_namespace_id(self) -> int | None:
        """Return the ID of the default 'default' schema under 'General'."""
        general = self.repo.get_namespace_by_name("General", parent_id=None)
        if general is None:
            return None
        default_schema = self.repo.get_namespace_by_name("default", parent_id=general.id)
        if default_schema is None:
            return None
        return default_schema.id

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
        """Register a new flow in the catalog.

        Raises
        ------
        NamespaceNotFoundError
            If ``namespace_id`` is given but doesn't exist.
        """
        if namespace_id is not None:
            ns = self.repo.get_namespace(namespace_id)
            if ns is None:
                raise NamespaceNotFoundError(namespace_id=namespace_id)
        flow = FlowRegistration(
            name=name,
            description=description,
            flow_path=flow_path,
            namespace_id=namespace_id,
            owner_id=owner_id,
        )
        flow = self.repo.create_flow(flow)
        return self._enrich_flow_registration(flow, owner_id)

    def update_flow(
        self,
        registration_id: int,
        requesting_user_id: int,
        name: str | None = None,
        description: str | None = None,
        namespace_id: int | None = None,
    ) -> FlowRegistrationOut:
        """Update a flow registration.

        Raises
        ------
        FlowNotFoundError
            If the flow doesn't exist.
        """
        flow = self.repo.get_flow(registration_id)
        if flow is None:
            raise FlowNotFoundError(registration_id=registration_id)
        if name is not None:
            flow.name = name
        if description is not None:
            flow.description = description
        if namespace_id is not None:
            flow.namespace_id = namespace_id
        flow = self.repo.update_flow(flow)
        return self._enrich_flow_registration(flow, requesting_user_id)

    def delete_flow(self, registration_id: int) -> None:
        """Delete a flow and its related favourites/follows.

        Raises
        ------
        FlowNotFoundError
            If the flow doesn't exist.
        FlowHasArtifactsError
            If the flow still has active (non-deleted) artifacts.
        """
        flow = self.repo.get_flow(registration_id)
        if flow is None:
            raise FlowNotFoundError(registration_id=registration_id)

        artifact_count = self.repo.count_active_artifacts_for_flow(registration_id)
        if artifact_count > 0:
            raise FlowHasArtifactsError(registration_id, artifact_count)

        self.repo.delete_flow(registration_id)

    def get_flow(self, registration_id: int, user_id: int) -> FlowRegistrationOut:
        """Get an enriched flow registration.

        Raises
        ------
        FlowNotFoundError
            If the flow doesn't exist.
        """
        flow = self.repo.get_flow(registration_id)
        if flow is None:
            raise FlowNotFoundError(registration_id=registration_id)
        return self._enrich_flow_registration(flow, user_id)

    def list_flows(self, user_id: int, namespace_id: int | None = None) -> list[FlowRegistrationOut]:
        """List flows, optionally filtered by namespace, enriched with user context.

        Uses bulk enrichment to avoid N+1 queries.
        """
        flows = self.repo.list_flows(namespace_id=namespace_id)
        return self._bulk_enrich_flows(flows, user_id)

    def list_artifacts_for_flow(self, registration_id: int) -> list[GlobalArtifactOut]:
        """List all active artifacts produced by a registered flow.

        Raises
        ------
        FlowNotFoundError
            If the flow doesn't exist.
        """
        flow = self.repo.get_flow(registration_id)
        if flow is None:
            raise FlowNotFoundError(registration_id=registration_id)
        artifacts = self.repo.list_artifacts_for_flow(registration_id)
        return [self._artifact_to_out(a) for a in artifacts]

    # ------------------------------------------------------------------ #
    # Run operations
    # ------------------------------------------------------------------ #

    def list_runs(
        self,
        registration_id: int | None = None,
        schedule_id: int | None = None,
        run_type: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> PaginatedFlowRuns:
        """List run summaries (without snapshots) with total count for pagination."""
        runs = self.repo.list_runs(
            registration_id=registration_id, schedule_id=schedule_id, run_type=run_type, limit=limit, offset=offset
        )
        counts = self.repo.count_runs_by_status(
            registration_id=registration_id, schedule_id=schedule_id, run_type=run_type
        )
        return PaginatedFlowRuns(
            items=[self._run_to_out(r) for r in runs],
            total=counts["total"],
            total_success=counts["success"],
            total_failed=counts["failed"],
            total_running=counts["running"],
        )

    def get_run_detail(self, run_id: int) -> FlowRunDetail:
        """Get a single run including the YAML snapshot.

        Raises
        ------
        RunNotFoundError
            If the run doesn't exist.
        """
        run = self.repo.get_run(run_id)
        if run is None:
            raise RunNotFoundError(run_id=run_id)
        return FlowRunDetail(
            id=run.id,
            registration_id=run.registration_id,
            flow_name=run.flow_name,
            flow_path=run.flow_path,
            user_id=run.user_id,
            started_at=run.started_at,
            ended_at=run.ended_at,
            success=run.success,
            nodes_completed=run.nodes_completed,
            number_of_nodes=run.number_of_nodes,
            duration_seconds=run.duration_seconds,
            run_type=run.run_type,
            schedule_id=run.schedule_id,
            has_snapshot=run.flow_snapshot is not None,
            has_log=self._resolve_log_path(run.id, run.run_type) is not None,
            flow_snapshot=run.flow_snapshot,
            node_results_json=run.node_results_json,
        )

    def get_run(self, run_id: int) -> FlowRun:
        """Get a raw FlowRun model.

        Raises
        ------
        RunNotFoundError
            If the run doesn't exist.
        """
        run = self.repo.get_run(run_id)
        if run is None:
            raise RunNotFoundError(run_id=run_id)
        return run

    def start_run(
        self,
        registration_id: int | None,
        flow_name: str,
        flow_path: str | None,
        user_id: int,
        number_of_nodes: int,
        run_type: str = "in_designer_run",
        flow_snapshot: str | None = None,
    ) -> FlowRun:
        """Record a new flow run start."""
        run = FlowRun(
            registration_id=registration_id,
            flow_name=flow_name,
            flow_path=flow_path,
            user_id=user_id,
            started_at=datetime.now(timezone.utc),
            number_of_nodes=number_of_nodes,
            run_type=run_type,
            flow_snapshot=flow_snapshot,
        )
        return self.repo.create_run(run)

    def complete_run(
        self,
        run_id: int,
        success: bool,
        nodes_completed: int,
        number_of_nodes: int | None = None,
        node_results_json: str | None = None,
    ) -> FlowRun:
        """Mark a run as completed.

        Raises
        ------
        RunNotFoundError
            If the run doesn't exist.
        """
        run = self.repo.get_run(run_id)
        if run is None:
            raise RunNotFoundError(run_id=run_id)
        now = datetime.now(timezone.utc)
        run.ended_at = now
        run.success = success
        run.nodes_completed = nodes_completed
        if number_of_nodes is not None and number_of_nodes > 0:
            run.number_of_nodes = number_of_nodes
        if run.started_at:
            # Normalize both sides to naive UTC for duration calculation.
            # SQLite stores naive datetimes, so started_at may lack tzinfo.
            started_utc = run.started_at.replace(tzinfo=None)
            now_utc = now.replace(tzinfo=None)
            run.duration_seconds = (now_utc - started_utc).total_seconds()
        if node_results_json is not None:
            run.node_results_json = node_results_json
        return self.repo.update_run(run)

    def get_run_snapshot(self, run_id: int) -> str:
        """Return the flow snapshot text for a run.

        Raises
        ------
        RunNotFoundError
            If the run doesn't exist.
        NoSnapshotError
            If the run has no snapshot.
        """
        run = self.repo.get_run(run_id)
        if run is None:
            raise RunNotFoundError(run_id=run_id)
        if not run.flow_snapshot:
            raise NoSnapshotError(run_id=run_id)
        return run.flow_snapshot

    # ------------------------------------------------------------------ #
    # Favorites
    # ------------------------------------------------------------------ #

    def add_favorite(self, user_id: int, registration_id: int) -> FlowFavorite:
        """Add a flow to user's favourites (idempotent).

        Raises
        ------
        FlowNotFoundError
            If the flow doesn't exist.
        """
        flow = self.repo.get_flow(registration_id)
        if flow is None:
            raise FlowNotFoundError(registration_id=registration_id)
        existing = self.repo.get_favorite(user_id, registration_id)
        if existing is not None:
            return existing
        fav = FlowFavorite(user_id=user_id, registration_id=registration_id)
        return self.repo.add_favorite(fav)

    def remove_favorite(self, user_id: int, registration_id: int) -> None:
        """Remove a flow from user's favourites.

        Raises
        ------
        FavoriteNotFoundError
            If the favourite doesn't exist.
        """
        existing = self.repo.get_favorite(user_id, registration_id)
        if existing is None:
            raise FavoriteNotFoundError(user_id=user_id, registration_id=registration_id)
        self.repo.remove_favorite(user_id, registration_id)

    def list_favorites(self, user_id: int) -> list[FlowRegistrationOut]:
        """List all flows the user has favourited, enriched.

        Uses bulk enrichment to avoid N+1 queries.
        """
        favs = self.repo.list_favorites(user_id)
        flows: list[FlowRegistration] = []
        for fav in favs:
            flow = self.repo.get_flow(fav.registration_id)
            if flow is not None:
                flows.append(flow)
        return self._bulk_enrich_flows(flows, user_id)

    # ------------------------------------------------------------------ #
    # Follows
    # ------------------------------------------------------------------ #

    def add_follow(self, user_id: int, registration_id: int) -> FlowFollow:
        """Follow a flow (idempotent).

        Raises
        ------
        FlowNotFoundError
            If the flow doesn't exist.
        """
        flow = self.repo.get_flow(registration_id)
        if flow is None:
            raise FlowNotFoundError(registration_id=registration_id)
        existing = self.repo.get_follow(user_id, registration_id)
        if existing is not None:
            return existing
        follow = FlowFollow(user_id=user_id, registration_id=registration_id)
        return self.repo.add_follow(follow)

    def remove_follow(self, user_id: int, registration_id: int) -> None:
        """Unfollow a flow.

        Raises
        ------
        FollowNotFoundError
            If the follow record doesn't exist.
        """
        existing = self.repo.get_follow(user_id, registration_id)
        if existing is None:
            raise FollowNotFoundError(user_id=user_id, registration_id=registration_id)
        self.repo.remove_follow(user_id, registration_id)

    def list_following(self, user_id: int) -> list[FlowRegistrationOut]:
        """List all flows the user is following, enriched.

        Uses bulk enrichment to avoid N+1 queries.
        """
        follows = self.repo.list_follows(user_id)
        flows: list[FlowRegistration] = []
        for follow in follows:
            flow = self.repo.get_flow(follow.registration_id)
            if flow is not None:
                flows.append(flow)
        return self._bulk_enrich_flows(flows, user_id)

    # ------------------------------------------------------------------ #
    # Catalog table operations
    # ------------------------------------------------------------------ #

    def _table_to_out(self, table: CatalogTable, user_id: int | None = None) -> CatalogTableOut:
        """Convert a CatalogTable ORM instance to its Pydantic output schema."""
        columns: list[ColumnSchema] = []
        if table.schema_json:
            try:
                raw = json.loads(table.schema_json)
                columns = [ColumnSchema(name=c["name"], dtype=c["dtype"]) for c in raw]
            except (json.JSONDecodeError, KeyError, TypeError):
                pass

        # Resolve source flow name if linked
        source_registration_name: str | None = None
        if table.source_registration_id:
            reg = self.repo.get_flow(table.source_registration_id)
            if reg is not None:
                source_registration_name = reg.name

        # Resolve flows that read from this table
        readers = self.repo.list_readers_for_table(table.id)
        read_by_flows = [FlowSummary(id=r.id, name=r.name) for r in readers]

        is_fav = False
        if user_id is not None:
            is_fav = self.repo.get_table_favorite(user_id, table.id) is not None

        return CatalogTableOut(
            id=table.id,
            name=table.name,
            namespace_id=table.namespace_id,
            description=table.description,
            owner_id=table.owner_id,
            file_exists=table_exists(table.file_path) if table.file_path else False,
            is_favorite=is_fav,
            schema_columns=columns,
            row_count=table.row_count,
            column_count=table.column_count,
            size_bytes=table.size_bytes,
            source_registration_id=table.source_registration_id,
            source_registration_name=source_registration_name,
            source_run_id=table.source_run_id,
            read_by_flows=read_by_flows,
            created_at=table.created_at,
            updated_at=table.updated_at,
        )

    def _bulk_enrich_tables(self, tables: list[CatalogTable], user_id: int) -> list[CatalogTableOut]:
        """Enrich multiple tables with favorite status in bulk to avoid N+1 queries."""
        if not tables:
            return []

        table_ids = [t.id for t in tables]
        fav_ids = self.repo.bulk_get_favorite_table_ids(user_id, table_ids)

        result: list[CatalogTableOut] = []
        for table in tables:
            columns: list[ColumnSchema] = []
            if table.schema_json:
                try:
                    raw = json.loads(table.schema_json)
                    columns = [ColumnSchema(name=c["name"], dtype=c["dtype"]) for c in raw]
                except (json.JSONDecodeError, KeyError, TypeError):
                    pass

            source_registration_name: str | None = None
            if table.source_registration_id:
                reg = self.repo.get_flow(table.source_registration_id)
                if reg is not None:
                    source_registration_name = reg.name

            readers = self.repo.list_readers_for_table(table.id)
            read_by_flows = [FlowSummary(id=r.id, name=r.name) for r in readers]

            result.append(
                CatalogTableOut(
                    id=table.id,
                    name=table.name,
                    namespace_id=table.namespace_id,
                    description=table.description,
                    owner_id=table.owner_id,
                    file_exists=table_exists(table.file_path) if table.file_path else False,
                    is_favorite=table.id in fav_ids,
                    schema_columns=columns,
                    row_count=table.row_count,
                    column_count=table.column_count,
                    size_bytes=table.size_bytes,
                    source_registration_id=table.source_registration_id,
                    source_registration_name=source_registration_name,
                    source_run_id=table.source_run_id,
                    read_by_flows=read_by_flows,
                    created_at=table.created_at,
                    updated_at=table.updated_at,
                )
            )
        return result

    def _materialize_table_with_worker(
        self,
        source_file_path: str,
        table_name: str | None = None,
        parquet_filename: str | None = None,
    ) -> CatalogMaterializationResult:
        from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import (
            trigger_catalog_materialize,
        )

        response = trigger_catalog_materialize(
            source_file_path=source_file_path,
            table_name=table_name,
            parquet_filename=parquet_filename,
        )
        if response.ok:
            data = response.json()
            schema = [
                {"name": col["name"], "dtype": col["dtype"]}
                for col in data.get("schema", [])
                if "name" in col and "dtype" in col
            ]
            # Accept both table_path (new delta) and parquet_path (legacy) from worker
            resolved_path = data.get("table_path") or data.get("parquet_path")
            storage_format = data.get("storage_format", "parquet")
            return CatalogService.CatalogMaterializationResult(
                table_path=resolved_path,
                schema=schema,
                row_count=data["row_count"],
                column_count=data["column_count"],
                size_bytes=data["size_bytes"],
                storage_format=storage_format,
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
        if namespace_id is not None:
            ns = self.repo.get_namespace(namespace_id)
            if ns is None:
                raise NamespaceNotFoundError(namespace_id=namespace_id)

        existing = self.repo.get_table_by_name(name, namespace_id)
        if existing is not None:
            raise TableExistsError(name=name, namespace_id=namespace_id)

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
        if namespace_id is not None:
            ns = self.repo.get_namespace(namespace_id)
            if ns is None:
                raise NamespaceNotFoundError(namespace_id=namespace_id)

        existing = self.repo.get_table_by_name(name, namespace_id)
        if existing is not None:
            raise TableExistsError(name=name, namespace_id=namespace_id)

        if schema is not None and row_count is not None and size_bytes is not None:
            # Fast path: caller already computed metadata (from worker)
            schema_list = schema
            if column_count is None:
                column_count = len(schema_list)
        else:
            # Fallback: read metadata from the table on disk
            schema_list, row_count, column_count, size_bytes = self._read_table_metadata(
                table_path, storage_format
            )

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
            schema_list, row_count, column_count, size_bytes = self._read_table_metadata(
                str(dest_path), storage_format
            )

        # Remove old storage if it differs from the new one
        old_path = Path(table.file_path)
        if old_path != dest_path and (old_path.exists() or old_path.is_dir()):
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

            now = datetime.now(timezone.utc)
            run = FlowRun(
                registration_id=flow.id,
                flow_name=flow.name,
                flow_path=flow.flow_path,
                user_id=schedule.owner_id,
                started_at=now,
                number_of_nodes=0,
                run_type="scheduled",
                schedule_id=schedule.id,
            )
            run = self.repo.create_run(run)

            schedule.last_triggered_at = now
            schedule.last_trigger_table_updated_at = table_updated_at
            self.repo.update_schedule(schedule)

            pid = self._spawn_flow_subprocess(flow.flow_path, run.id)
            if pid is not None:
                run.pid = pid
                self.repo.update_run(run)
                launched += 1
            else:
                logger.error("Failed to spawn subprocess for run %s — marking as failed", run.id)
                run.ended_at = datetime.now(timezone.utc)
                run.success = False
                self.repo.update_run(run)

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
            If the table exists and *write_mode* is not ``"overwrite"``.
        """
        from flowfile_core.catalog.delta_utils import is_delta_table, is_legacy_parquet

        existing = self.repo.get_table_by_name(table_name, namespace_id)

        if existing is not None:
            if write_mode != "overwrite":
                raise TableExistsError(name=table_name, namespace_id=namespace_id)

            old_path = Path(existing.file_path)
            if is_delta_table(old_path):
                return existing, old_path, "overwrite"

            # Legacy parquet file — remove it, create new delta dir at same stem
            new_dir = old_path.parent / old_path.stem
            if is_legacy_parquet(old_path):
                old_path.unlink()
            return existing, new_dir, "overwrite"

        from uuid import uuid4

        dir_name = f"{table_name}_{uuid4().hex[:8]}"
        return None, catalog_dir / dir_name, "error"

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
            tables = self.repo.list_tables(namespace_id=namespace_id)
            for t in tables:
                if t.name == table_name:
                    return t.file_path
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

        try:
            storage_path = Path(file_path)
            if storage_path.exists() or storage_path.is_dir():
                delete_table_storage(storage_path)
        except OSError:
            logger.warning("Failed to delete materialized storage %s", file_path, exc_info=True)

    def get_table_preview(self, table_id: int, limit: int = 100, version: int | None = None) -> CatalogTablePreview:
        """Read the first N rows from the materialized table (Delta or Parquet).

        When *version* is provided and the table is a Delta table, reads from
        that specific historical version via the worker.

        Raises
        ------
        TableNotFoundError
            If the table doesn't exist.
        """
        table = self.repo.get_table(table_id)
        if table is None:
            raise TableNotFoundError(table_id=table_id)

        data_path = Path(table.file_path)
        if not table_exists(data_path):
            return CatalogTablePreview(columns=[], dtypes=[], rows=[], total_rows=0)

        # Version-specific preview for Delta tables
        if version is not None and is_delta_table(data_path):
            return self._get_delta_version_preview(data_path, version, limit)

        import polars as pl

        if is_delta_table(data_path):
            df = read_delta_preview(data_path, n_rows=limit)
        else:
            df = pl.read_parquet(data_path, n_rows=limit)

        columns = df.columns
        dtypes = [str(df[col].dtype) for col in columns]

        def _make_json_safe(val: object) -> object:
            if val is None or isinstance(val, bool | int | float | str):
                return val
            return str(val)

        rows = [[_make_json_safe(v) for v in row] for row in df.rows()]

        return CatalogTablePreview(
            columns=columns,
            dtypes=dtypes,
            rows=rows,
            total_rows=table.row_count or len(df),
        )

    def _get_delta_version_preview(self, data_path: Path, version: int, limit: int) -> CatalogTablePreview:
        """Read a Delta table preview at a specific version via the worker (or locally)."""
        from flowfile_core.configs.settings import OFFLOAD_TO_WORKER

        table_path = str(data_path)
        if OFFLOAD_TO_WORKER:
            try:
                from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import (
                    trigger_delta_version_preview,
                )

                return trigger_delta_version_preview(table_path, version, limit)
            except Exception:
                logger.warning("Worker delta version preview failed, falling back to local", exc_info=True)

        # Local fallback using deltalake + PyArrow
        from deltalake import DeltaTable

        dt = DeltaTable(table_path, version=version)
        dataset = dt.to_pyarrow_dataset()
        pa_table = dataset.head(limit)
        columns = pa_table.column_names
        dtypes = [str(field.type) for field in pa_table.schema]
        rows_data = pa_table.to_pylist()

        def _make_json_safe(val: object) -> object:
            if val is None or isinstance(val, bool | int | float | str):
                return val
            return str(val)

        rows = [[_make_json_safe(row.get(c)) for c in columns] for row in rows_data]
        return CatalogTablePreview(columns=columns, dtypes=dtypes, rows=rows, total_rows=len(rows))

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

        data_path = Path(table.file_path)
        if not is_delta_table(data_path):
            return DeltaTableHistory(current_version=0, history=[])

        from flowfile_core.configs.settings import OFFLOAD_TO_WORKER

        table_path = str(data_path)
        if OFFLOAD_TO_WORKER:
            try:
                from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import (
                    trigger_delta_history,
                )

                return trigger_delta_history(table_path, limit)
            except Exception:
                logger.warning("Worker delta history read failed, falling back to local", exc_info=True)

        # Local fallback
        from deltalake import DeltaTable as DT

        dt = DT(table_path)
        raw_history = dt.history(limit)
        current_version = dt.version()
        history = _parse_delta_history(raw_history)
        return DeltaTableHistory(current_version=current_version, history=history)

    # ------------------------------------------------------------------ #
    # Table Favorites
    # ------------------------------------------------------------------ #

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

    # ------------------------------------------------------------------ #
    # Schedule operations
    # ------------------------------------------------------------------ #

    def _schedule_to_out(self, schedule: FlowSchedule) -> FlowScheduleOut:
        """Convert a FlowSchedule ORM instance to its Pydantic output schema, populating trigger table info."""
        # Resolve single table trigger name
        trigger_table_name: str | None = None
        if schedule.trigger_table_id is not None:
            table = self.repo.get_table(schedule.trigger_table_id)
            if table is not None:
                trigger_table_name = table.name

        # Resolve table set trigger IDs and names
        trigger_table_ids: list[int] = []
        trigger_table_names: list[str] = []
        if schedule.schedule_type == "table_set_trigger":
            trigger_table_ids = self.repo.get_trigger_table_ids(schedule.id)
            for tid in trigger_table_ids:
                table = self.repo.get_table(tid)
                trigger_table_names.append(table.name if table else f"#{tid}")

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
            trigger_table_ids=trigger_table_ids,
            trigger_table_names=trigger_table_names,
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

        if schedule_type not in ("interval", "table_trigger", "table_set_trigger"):
            raise ValueError(f"Invalid schedule_type: {schedule_type}")

        if schedule_type == "interval":
            if interval_seconds is None or interval_seconds < 60:
                raise ValueError("interval_seconds must be >= 60")
        elif schedule_type == "table_trigger":
            if trigger_table_id is None:
                raise ValueError("trigger_table_id is required for table_trigger schedules")
            table = self.repo.get_table(trigger_table_id)
            if table is None:
                raise TableNotFoundError(table_id=trigger_table_id)
        elif schedule_type == "table_set_trigger":
            if not trigger_table_ids or len(trigger_table_ids) < 2:
                raise ValueError("table_set_trigger requires at least 2 trigger_table_ids")
            for tid in trigger_table_ids:
                table = self.repo.get_table(tid)
                if table is None:
                    raise TableNotFoundError(table_id=tid)

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
            if interval_seconds < 60:
                raise ValueError("interval_seconds must be >= 60")
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

    @staticmethod
    def _spawn_flow_subprocess(flow_path: str, run_id: int) -> int | None:
        """Fire-and-forget a ``flowfile run flow`` subprocess.

        Returns the child PID on success, or ``None`` on failure.
        """
        from shared.subprocess_utils import spawn_flow_subprocess

        return spawn_flow_subprocess(flow_path, run_id)

    def run_flow_now(self, registration_id: int, user_id: int) -> FlowRunOut:
        """Trigger a registered flow immediately without a schedule.

        Raises
        ------
        FlowNotFoundError
            If the flow doesn't exist.
        FlowAlreadyRunningError
            If the flow already has an active (unfinished) run.
        """
        flow = self.repo.get_flow(registration_id)
        if flow is None:
            raise FlowNotFoundError(registration_id=registration_id)

        if self.repo.has_active_run(registration_id):
            raise FlowAlreadyRunningError(registration_id=registration_id)

        now = datetime.now(timezone.utc)
        run = FlowRun(
            registration_id=flow.id,
            flow_name=flow.name,
            flow_path=flow.flow_path,
            user_id=user_id,
            started_at=now,
            number_of_nodes=0,
            run_type="manual",
        )
        run = self.repo.create_run(run)

        pid = self._spawn_flow_subprocess(flow.flow_path, run.id)
        if pid is not None:
            run.pid = pid
            self.repo.update_run(run)
        else:
            logger.error("Failed to spawn subprocess for run %s — marking as failed", run.id)
            run.ended_at = datetime.now(timezone.utc)
            run.success = False
            self.repo.update_run(run)

        return self._run_to_out(run)

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

        # Create a run record before spawning
        now = datetime.now(timezone.utc)
        run = FlowRun(
            registration_id=flow.id,
            flow_name=flow.name,
            flow_path=flow.flow_path,
            user_id=user_id,
            started_at=now,
            number_of_nodes=0,
            run_type="on_demand",
            schedule_id=schedule.id,
        )
        run = self.repo.create_run(run)

        pid = self._spawn_flow_subprocess(flow.flow_path, run.id)
        if pid is not None:
            run.pid = pid
            self.repo.update_run(run)
        else:
            logger.error("Failed to spawn subprocess for run %s — marking as failed", run.id)
            run.ended_at = datetime.now(timezone.utc)
            run.success = False
            self.repo.update_run(run)

        return self._run_to_out(run)

    # ------------------------------------------------------------------ #
    # Active runs + cancel
    # ------------------------------------------------------------------ #

    def list_active_runs(self) -> list[ActiveFlowRun]:
        """List all currently running flows (ended_at IS NULL)."""
        runs = self.repo.list_active_runs()
        return [
            ActiveFlowRun(
                id=r.id,
                registration_id=r.registration_id,
                flow_name=r.flow_name,
                flow_path=r.flow_path,
                user_id=r.user_id,
                started_at=r.started_at,
                nodes_completed=r.nodes_completed,
                number_of_nodes=r.number_of_nodes,
                run_type=r.run_type,
            )
            for r in runs
        ]

    def cancel_run(self, run_id: int) -> None:
        """Cancel a running flow by terminating its subprocess and marking
        the database record as failed.

        If ``pid`` is stored on the run, sends ``SIGTERM`` to the process.
        ``ProcessLookupError`` is silently ignored (the process already
        exited).  If no PID is available the record is still marked as
        cancelled.

        Raises
        ------
        RunNotFoundError
            If the run doesn't exist.
        """
        import os
        import signal

        run = self.repo.get_run(run_id)
        if run is None:
            raise RunNotFoundError(run_id=run_id)

        if run.pid is not None:
            try:
                os.kill(run.pid, signal.SIGTERM)
                logger.info("Sent SIGTERM to pid %s for run %s", run.pid, run_id)
            except ProcessLookupError:
                logger.info("Process %s for run %s already exited", run.pid, run_id)
            except OSError:
                logger.warning("Failed to kill pid %s for run %s", run.pid, run_id, exc_info=True)

        now = datetime.now(timezone.utc)
        run.ended_at = now
        run.success = False
        if run.started_at:
            started_utc = run.started_at.replace(tzinfo=None)
            now_utc = now.replace(tzinfo=None)
            run.duration_seconds = (now_utc - started_utc).total_seconds()
        self.repo.update_run(run)

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
            total_schedules=total_schedules,
            recent_runs=recent_out,
            favorite_flows=fav_flows,
            favorite_tables=fav_tables,
            active_runs=active_runs,
        )
