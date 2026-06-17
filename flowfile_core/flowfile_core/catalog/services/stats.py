"""Catalog dashboard statistics: counts, recent runs, favourite enrichment."""

from __future__ import annotations

import logging

from flowfile_core.catalog.repository import CatalogRepository
from flowfile_core.catalog.services.flows import FlowRegistrationService
from flowfile_core.catalog.services.runs import FlowRunService
from flowfile_core.catalog.services.tables import TableService
from flowfile_core.database.models import FlowRegistration
from flowfile_core.schemas.catalog_schema import CatalogStats

logger = logging.getLogger(__name__)


class StatsService:
    """Owns the dashboard overview statistics endpoint."""

    def __init__(
        self,
        repo: CatalogRepository,
        flows: FlowRegistrationService,
        runs: FlowRunService,
        tables: TableService,
    ) -> None:
        self.repo = repo
        self._flows = flows
        self._runs = runs
        self._tables = tables

    def get_catalog_stats(self, user_id: int) -> CatalogStats:
        """Return an overview of the catalog for the dashboard.

        Uses bulk enrichment for favourite flows to avoid N+1 queries.
        """
        total_namespaces = self.repo.count_catalog_namespaces()
        total_flows = self.repo.count_all_flows()
        total_runs = self.repo.count_runs()
        total_favorites = self.repo.count_favorites(user_id)
        total_table_favorites = self.repo.count_table_favorites(user_id)
        total_artifacts = self.repo.count_all_active_artifacts()
        total_tables = self.repo.count_all_tables()
        total_virtual_tables = self.repo.count_virtual_tables()
        total_schedules = self.repo.count_schedules()

        recent_runs_raw = self.repo.list_runs(limit=10, offset=0)
        recent_out = [self._runs.run_to_out(r) for r in recent_runs_raw]

        favorites = self.repo.list_favorites(user_id)
        flows: list[FlowRegistration] = []
        for favorite in favorites:
            flow = self.repo.get_flow(favorite.registration_id)
            if flow is not None:
                flows.append(flow)
        fav_flows = self._flows.bulk_enrich_flows(flows, user_id)

        fav_tables = self._tables.list_table_favorites(user_id)

        active_runs = self._runs.list_active_runs()

        return CatalogStats(
            total_namespaces=total_namespaces,
            total_flows=total_flows,
            total_runs=total_runs,
            total_favorites=total_favorites,
            total_table_favorites=total_table_favorites,
            total_artifacts=total_artifacts,
            total_tables=total_tables,
            total_virtual_tables=total_virtual_tables,
            total_schedules=total_schedules,
            recent_runs=recent_out,
            favorite_flows=fav_flows,
            favorite_tables=fav_tables,
            active_runs=active_runs,
        )
