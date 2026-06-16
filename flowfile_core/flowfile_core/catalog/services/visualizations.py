"""Visualizations and dashboards: CRUD, enrichment, worker compute, column stats."""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy.exc import IntegrityError

from flowfile_core.catalog.constants import (
    DEFAULT_VISUALIZATION_ROWS,
    MAX_PREVIEW_LIMIT,
    MAX_VISUALIZATION_ROWS,
)
from flowfile_core.catalog.exceptions import (
    DashboardNotFoundError,
    NamespaceNotFoundError,
    TableNotFoundError,
    VisualizationComputeError,
    VisualizationExistsError,
    VisualizationNotFoundError,
)
from flowfile_core.catalog.repository import CatalogRepository
from flowfile_core.catalog.serializers import VizEnrichment
from flowfile_core.catalog.services._resolve import resolve_or_log
from flowfile_core.catalog.services.namespaces import NamespaceService
from flowfile_core.catalog.services.sql import SqlService
from flowfile_core.catalog.services.tables import TableService
from flowfile_core.catalog.services.virtual_tables import VirtualTableService
from flowfile_core.catalog.text_utils import (
    hash_source_versions,
    is_table_reference,
    rewrite_qualified_references,
)
from flowfile_core.catalog.validators import (
    format_full_name,
    validate_thumbnail,
    validate_viz_source,
)
from flowfile_core.database.models import CatalogDashboard, CatalogTable, CatalogVisualization
from flowfile_core.schemas.catalog_schema import (
    ColumnStatsResponse,
    DashboardCreate,
    DashboardLayout,
    DashboardOut,
    DashboardUpdate,
    VisualizationComputeResponse,
    VisualizationCreate,
    VisualizationFieldsResponse,
    VisualizationOut,
    VisualizationUpdate,
    VizSourceDescriptor,
)

logger = logging.getLogger(__name__)
viz_logger = logger.getChild("viz")


def _project_sync_visualizations(user_id: int) -> None:
    """Mirror a viz create/update/delete into the active project's visualizations.yaml (no-op when
    no project is active; never raises)."""
    from flowfile_core.project import project_sync

    project_sync.visualizations_changed(user_id)


def _project_sync_dashboards(user_id: int) -> None:
    """Mirror a dashboard create/update/delete into the active project's dashboards.yaml (no-op when
    no project is active; never raises)."""
    from flowfile_core.project import project_sync

    project_sync.dashboards_changed(user_id)

# polars-gw workflow that returns rows un-aggregated (raw select-all).
_GW_RAW_SELECT_ALL_PAYLOAD: dict = {"workflow": [{"type": "view", "query": [{"op": "raw", "fields": ["*"]}]}]}


class VisualizationService:
    """Owns catalog visualizations + dashboards: CRUD, enrichment, worker compute."""

    def __init__(
        self,
        repo: CatalogRepository,
        namespaces: NamespaceService,
        tables: TableService,
        virtual_tables: VirtualTableService,
        sql: SqlService,
    ) -> None:
        self.repo = repo
        self._namespaces = namespaces
        self._tables = tables
        self._virtual_tables = virtual_tables
        self._sql = sql
        self._facade = None  # set by CatalogService.__init__ via bind_facade()

    def bind_facade(self, facade) -> None:
        """Late-bind the facade so callers that monkeypatch
        ``CatalogService.resolve_virtual_flow_table`` flow through this service."""
        self._facade = facade

    def _resolve_virtual_flow_table_via_facade(self, table_id: int, *, user_id: int | None, run_location: str):
        """Route resolution through the facade if present.

        Tests monkeypatch ``CatalogService.resolve_virtual_flow_table``;
        going through the facade lets that override fire instead of the
        underlying VirtualTableService implementation.
        """
        if self._facade is not None:
            return self._facade.resolve_virtual_flow_table(table_id, user_id=user_id, run_location=run_location)
        return self._virtual_tables.resolve_virtual_flow_table(table_id, user_id=user_id, run_location=run_location)

    # ---- Enrichment ------------------------------------------------------ #

    def _resolve_viz_enrichment(self, viz: CatalogVisualization, table: CatalogTable | None) -> VizEnrichment:
        table_name: str | None = None
        table_namespace_name: str | None = None
        table_full_name: str | None = None
        table_type: str | None = None
        if table is not None:
            table_name = table.name
            table_namespace_name = self._namespaces.resolve_namespace_name(table.namespace_id)
            table_full_name = format_full_name(table_namespace_name, table.name)
            table_type = table.table_type or "physical"
        if viz.namespace_id is not None:
            namespace_name = self._namespaces.resolve_namespace_name(viz.namespace_id)
        else:
            namespace_name = table_namespace_name
        return VizEnrichment(
            table_name=table_name,
            table_namespace_name=table_namespace_name,
            table_full_name=table_full_name,
            table_type=table_type,
            namespace_name=namespace_name,
        )

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

    # ---- Visualizations CRUD -------------------------------------------- #

    def list_visualizations_for_table(self, table_id: int, user_id: int | None = None) -> list[VisualizationOut]:
        """Filtered listing — viz that reference a specific catalog table."""
        if self.repo.get_table(table_id) is None:
            raise TableNotFoundError(table_id=table_id)
        return [self._viz_to_out(v) for v in self.repo.list_visualizations(table_id)]

    def list_visualization_library(self, user_id: int | None = None) -> list[VisualizationOut]:
        """Return all saved visualizations as catalog library entries.

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

    def create_visualization(self, payload: VisualizationCreate, user_id: int) -> VisualizationOut:
        validate_viz_source(payload)

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
            thumbnail_data_url=validate_thumbnail(payload.thumbnail_data_url),
            namespace_id=namespace_id,
            created_by=user_id,
        )
        try:
            created = self.repo.create_visualization(viz)
        except IntegrityError as exc:
            raise VisualizationExistsError(payload.name, payload.catalog_table_id or 0) from exc
        _project_sync_visualizations(user_id)
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
            viz.thumbnail_data_url = validate_thumbnail(payload.thumbnail_data_url)
        try:
            updated = self.repo.update_visualization(viz)
        except IntegrityError as exc:
            raise VisualizationExistsError(viz.name, viz.catalog_table_id or 0) from exc
        _project_sync_visualizations(user_id)
        return self._viz_to_out(updated)

    def delete_visualization(self, viz_id: int, user_id: int) -> None:
        viz = self.repo.get_visualization(viz_id)
        if viz is None:
            raise VisualizationNotFoundError(viz_id=viz_id)
        self.repo.delete_visualization(viz_id)
        _project_sync_visualizations(user_id)

    # ---- Dashboards CRUD ------------------------------------------------ #

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
            ns_name = self._namespaces.resolve_namespace_name(dashboard.namespace_id)
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
        _project_sync_dashboards(user_id)
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
        _project_sync_dashboards(user_id)
        return self._dashboard_to_out(updated)

    def delete_dashboard(self, dashboard_id: int, user_id: int) -> None:
        dashboard = self.repo.get_dashboard(dashboard_id)
        if dashboard is None:
            raise DashboardNotFoundError(dashboard_id=dashboard_id)
        self.repo.delete_dashboard(dashboard_id)
        _project_sync_dashboards(user_id)

    # ---- Compute -------------------------------------------------------- #

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
        """Translate a frontend source descriptor into a worker source spec."""
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

            from flowfile_core.catalog import service as _service_module

            lazy_frame = self._resolve_virtual_flow_table_via_facade(table.id, user_id=user_id, run_location="remote")
            versions_hash = hash_source_versions(table.source_table_versions)
            result = _service_module.trigger_resolve_virtual_table(table.id, lazy_frame.serialize(), versions_hash)
            return {
                "kind": "ipc_path",
                "session_key": f"fvt:{table.id}:{int(result['mtime'])}",
                "ipc_path": result["ipc_path"],
                "mtime": result["mtime"],
            }

        if not source.sql_query:
            raise ValueError("sql_query is required when source_type='sql'")
        return self._build_sql_worker_source(source.sql_query, user_id=user_id)

    def _build_sql_worker_source(self, sql_query: str, user_id: int | None) -> dict:
        """Build a worker SQL source spec mirroring ``execute_sql_query``'s setup."""
        from flowfile_core.flowfile.sources.external_sources.sql_source.sql_source import (
            UnsafeSQLError,
            validate_sql_query,
        )

        try:
            validate_sql_query(sql_query)
        except UnsafeSQLError as exc:
            raise ValueError(str(exc)) from exc

        delta_map, virtual_map = self._virtual_tables.resolve_all_queryable_tables()
        rewritten = rewrite_qualified_references(sql_query, {*delta_map, *virtual_map})
        referenced_virtuals = {n for n in virtual_map if is_table_reference(n, rewritten)}

        virtual_refs: dict[str, str] = {}
        for vname in referenced_virtuals:
            ipc_path = resolve_or_log(
                lambda vname=vname: self._materialise_virtual_for_viz(vname, virtual_map, user_id),
                kind="virtual table for viz",
                identifier=vname,
            )
            if ipc_path is not None:
                virtual_refs[vname] = ipc_path

        # Only ship the delta_map subset that's actually referenced — keeps the
        # session key compact and the worker's SQLContext small.
        referenced_delta = {n: d for n, d in delta_map.items() if is_table_reference(n, rewritten)}

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

    def _materialise_virtual_for_viz(self, vname: str, virtual_map: dict[str, int], user_id: int | None) -> str:
        from flowfile_core.catalog import service as _service_module

        vid = virtual_map[vname]
        lazy_frame = self._resolve_virtual_flow_table_via_facade(vid, user_id=user_id, run_location="remote")
        versions_hash = hash_source_versions(self.repo.get_table(vid).source_table_versions)
        result = _service_module.trigger_resolve_virtual_table(vid, lazy_frame.serialize(), versions_hash)
        return result["ipc_path"]

    def _dispatch_visualize_query(
        self, worker_source: dict, payload: dict, max_rows: int
    ) -> VisualizationComputeResponse:
        from flowfile_core.catalog import service as _service_module

        try:
            data = _service_module.trigger_visualize_query(worker_source, payload, max_rows)
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
        """Compute rows for a saved viz against its embedded source."""
        viz = self.repo.get_visualization(viz_id)
        if viz is None:
            raise VisualizationNotFoundError(viz_id=viz_id)
        source = self._viz_source_descriptor(viz)
        worker_source = self._resolve_source_for_worker(source, user_id=user_id)
        effective_payload = payload or _GW_RAW_SELECT_ALL_PAYLOAD
        viz_logger.info(
            "dispatch saved compute viz_id=%s source_type=%s kind=%s session_key=%s max_rows=%s gw_workflow=%s",
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
        from flowfile_core.catalog import service as _service_module

        worker_source = self._resolve_source_for_worker(source, user_id=user_id)
        viz_logger.info(
            "dispatch fields source_type=%s kind=%s session_key=%s",
            source.source_type,
            worker_source["kind"],
            worker_source["session_key"],
        )
        try:
            data = _service_module.trigger_visualize_fields(worker_source)
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
        """Distinct values + min/max for a single column on a catalog table."""
        from flowfile_core.catalog import service as _service_module

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
            data = _service_module.trigger_visualize_column_stats(worker_source, column, clamped_limit)
        except RuntimeError as exc:
            raise VisualizationComputeError(str(exc)) from exc
        return ColumnStatsResponse(**data)
