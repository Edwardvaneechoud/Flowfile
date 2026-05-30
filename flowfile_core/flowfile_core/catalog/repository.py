"""Data-access abstraction for the Catalog system.

Defines a ``CatalogRepository`` :pep:`544` Protocol and provides a concrete
``SQLAlchemyCatalogRepository`` implementation backed by SQLAlchemy.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from sqlalchemy import func
from sqlalchemy.orm import Session

from flowfile_core.database.models import (
    ApiConsumer,
    ApiConsumerEndpoint,
    CatalogDashboard,
    CatalogNamespace,
    CatalogTable,
    CatalogTableReadLink,
    CatalogVisualization,
    FlowApiEndpoint,
    FlowApiKey,
    FlowFavorite,
    FlowFollow,
    FlowRegistration,
    FlowRun,
    FlowSchedule,
    GlobalArtifact,
    RunType,
    ScheduleTriggerTable,
    TableFavorite,
)

# ---------------------------------------------------------------------------
# Repository Protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class CatalogRepository(Protocol):
    """Abstract interface for catalog data access.

    Any class that satisfies this protocol can be used by ``CatalogService``,
    enabling easy unit-testing with mock implementations.
    """

    # -- Namespace operations ------------------------------------------------

    def get_namespace(self, namespace_id: int) -> CatalogNamespace | None: ...

    def get_namespace_by_name(self, name: str, parent_id: int | None) -> CatalogNamespace | None: ...

    def list_namespaces(self, parent_id: int | None = None) -> list[CatalogNamespace]: ...

    def list_root_namespaces(self) -> list[CatalogNamespace]: ...

    def list_child_namespaces(self, parent_id: int) -> list[CatalogNamespace]: ...

    def create_namespace(self, ns: CatalogNamespace) -> CatalogNamespace: ...

    def update_namespace(self, ns: CatalogNamespace) -> CatalogNamespace: ...

    def delete_namespace(self, namespace_id: int) -> None: ...

    def count_children(self, namespace_id: int) -> int: ...

    # -- Flow registration operations ----------------------------------------

    def get_flow(self, registration_id: int) -> FlowRegistration | None: ...

    def get_flow_by_name(self, name: str, namespace_id: int) -> FlowRegistration | None: ...

    def get_flow_by_path(self, flow_path: str) -> FlowRegistration | None: ...

    def list_flows(
        self,
        namespace_id: int | None = None,
        owner_id: int | None = None,
    ) -> list[FlowRegistration]: ...

    def list_flows_by_ids(self, registration_ids: list[int]) -> list[FlowRegistration]: ...

    def create_flow(self, reg: FlowRegistration) -> FlowRegistration: ...

    def update_flow(self, reg: FlowRegistration) -> FlowRegistration: ...

    def delete_flow(self, registration_id: int) -> None: ...

    def count_flows_in_namespace(self, namespace_id: int) -> int: ...

    def count_active_artifacts_for_flow(self, registration_id: int) -> int: ...

    # -- Artifact operations -------------------------------------------------

    def list_artifacts_for_namespace(self, namespace_id: int) -> list[GlobalArtifact]: ...

    def list_artifacts_for_flow(self, registration_id: int) -> list[GlobalArtifact]: ...

    def count_all_active_artifacts(self) -> int: ...

    def bulk_get_artifact_counts(self, flow_ids: list[int]) -> dict[int, int]: ...

    # -- Run operations ------------------------------------------------------

    def get_run(self, run_id: int) -> FlowRun | None: ...

    def list_runs(
        self,
        registration_id: int | None = None,
        schedule_id: int | None = None,
        run_type: RunType | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[FlowRun]: ...

    def create_run(self, run: FlowRun) -> FlowRun: ...

    def update_run(self, run: FlowRun) -> FlowRun: ...

    def count_runs(
        self,
        registration_id: int | None = None,
        schedule_id: int | None = None,
        run_type: RunType | None = None,
    ) -> int: ...

    def count_runs_by_status(
        self,
        registration_id: int | None = None,
        schedule_id: int | None = None,
        run_type: RunType | None = None,
    ) -> dict[str, int]: ...

    # -- Favorites -----------------------------------------------------------

    def get_favorite(self, user_id: int, registration_id: int) -> FlowFavorite | None: ...

    def add_favorite(self, fav: FlowFavorite) -> FlowFavorite: ...

    def remove_favorite(self, user_id: int, registration_id: int) -> None: ...

    def list_favorites(self, user_id: int) -> list[FlowFavorite]: ...

    def count_favorites(self, user_id: int) -> int: ...

    # -- Follows -------------------------------------------------------------

    def get_follow(self, user_id: int, registration_id: int) -> FlowFollow | None: ...

    def add_follow(self, follow: FlowFollow) -> FlowFollow: ...

    def remove_follow(self, user_id: int, registration_id: int) -> None: ...

    def list_follows(self, user_id: int) -> list[FlowFollow]: ...

    # -- Aggregate helpers ---------------------------------------------------

    def count_run_for_flow(self, registration_id: int) -> int: ...

    def last_run_for_flow(self, registration_id: int) -> FlowRun | None: ...

    def count_catalog_namespaces(self) -> int: ...

    def count_all_flows(self) -> int: ...

    # -- Catalog table operations -----------------------------------------------

    def get_table(self, table_id: int) -> CatalogTable | None: ...

    def get_table_by_name(self, name: str, namespace_id: int | None) -> CatalogTable | None: ...

    def list_tables_by_name(self, name: str) -> list[CatalogTable]: ...

    def list_tables(self, namespace_id: int | None = None) -> list[CatalogTable]: ...

    def list_tables_for_namespace(self, namespace_id: int) -> list[CatalogTable]: ...

    def create_table(self, table: CatalogTable) -> CatalogTable: ...

    def update_table(self, table: CatalogTable) -> CatalogTable: ...

    def delete_table(self, table_id: int) -> None: ...

    def count_tables_in_namespace(self, namespace_id: int) -> int: ...

    def count_all_tables(self) -> int: ...

    # -- Virtual table helpers -----------------------------------------------

    def list_virtual_tables(self, namespace_id: int | None = None) -> list[CatalogTable]: ...

    def get_virtual_table_by_producer(self, registration_id: int) -> CatalogTable | None: ...

    def get_virtual_table_by_producer_and_name(
        self, registration_id: int, name: str, namespace_id: int | None
    ) -> CatalogTable | None: ...

    def count_virtual_tables(self) -> int: ...

    # -- Table Favorites -----------------------------------------------------

    def get_table_favorite(self, user_id: int, table_id: int) -> TableFavorite | None: ...

    def add_table_favorite(self, fav: TableFavorite) -> TableFavorite: ...

    def remove_table_favorite(self, user_id: int, table_id: int) -> None: ...

    def list_table_favorites(self, user_id: int) -> list[TableFavorite]: ...

    def count_table_favorites(self, user_id: int) -> int: ...

    # -- Bulk enrichment helpers (for N+1 elimination) -----------------------

    def bulk_get_favorite_flow_ids(self, user_id: int, flow_ids: list[int]) -> set[int]: ...

    def bulk_get_follow_flow_ids(self, user_id: int, flow_ids: list[int]) -> set[int]: ...

    def bulk_get_run_stats(self, flow_ids: list[int]) -> dict[int, tuple[int, FlowRun | None]]: ...

    def bulk_get_favorite_table_ids(self, user_id: int, table_ids: list[int]) -> set[int]: ...

    def list_tables_for_flow(self, registration_id: int) -> list[CatalogTable]: ...

    def bulk_get_tables_for_flows(self, flow_ids: list[int]) -> dict[int, list[CatalogTable]]: ...

    def upsert_read_link(self, table_id: int, registration_id: int) -> None: ...

    def list_readers_for_table(self, table_id: int) -> list[FlowRegistration]: ...

    def list_read_tables_for_flow(self, registration_id: int) -> list[CatalogTable]: ...

    def bulk_get_read_tables_for_flows(self, flow_ids: list[int]) -> dict[int, list[CatalogTable]]: ...

    # -- Schedule operations -------------------------------------------------

    def get_schedule(self, schedule_id: int) -> FlowSchedule | None: ...

    def list_schedules(
        self,
        registration_id: int | None = None,
        enabled_only: bool = False,
    ) -> list[FlowSchedule]: ...

    def create_schedule(self, schedule: FlowSchedule) -> FlowSchedule: ...

    def update_schedule(self, schedule: FlowSchedule) -> FlowSchedule: ...

    def delete_schedule(self, schedule_id: int) -> None: ...

    def count_schedules(self) -> int: ...

    # -- Active run operations -----------------------------------------------

    def list_active_runs(self) -> list[FlowRun]: ...

    def has_active_run(self, registration_id: int) -> bool: ...

    def list_due_interval_schedules(self) -> list[FlowSchedule]: ...

    # -- Visualizations ------------------------------------------------------

    def list_visualizations(self, catalog_table_id: int) -> list[CatalogVisualization]: ...

    def list_all_visualizations(self) -> list[CatalogVisualization]: ...

    def get_visualization(self, viz_id: int) -> CatalogVisualization | None: ...

    def get_visualization_by_name(self, catalog_table_id: int, name: str) -> CatalogVisualization | None: ...

    def create_visualization(self, viz: CatalogVisualization) -> CatalogVisualization: ...

    def update_visualization(self, viz: CatalogVisualization) -> CatalogVisualization: ...

    def delete_visualization(self, viz_id: int) -> None: ...

    # -- Dashboards ----------------------------------------------------------

    def list_dashboards(self) -> list[CatalogDashboard]: ...

    def get_dashboard(self, dashboard_id: int) -> CatalogDashboard | None: ...

    def create_dashboard(self, dashboard: CatalogDashboard) -> CatalogDashboard: ...

    def update_dashboard(self, dashboard: CatalogDashboard) -> CatalogDashboard: ...

    def delete_dashboard(self, dashboard_id: int) -> None: ...

    def list_table_trigger_schedules(self) -> list[FlowSchedule]: ...

    def list_table_trigger_schedules_for_table(self, table_id: int) -> list[FlowSchedule]: ...

    def get_trigger_table_ids(self, schedule_id: int) -> list[int]: ...

    def set_trigger_table_ids(self, schedule_id: int, table_ids: list[int]) -> None: ...

    def delete_trigger_table_ids(self, schedule_id: int) -> None: ...


# ---------------------------------------------------------------------------
# SQLAlchemy implementation
# ---------------------------------------------------------------------------


class SQLAlchemyCatalogRepository:
    """Concrete ``CatalogRepository`` backed by a SQLAlchemy ``Session``."""

    def __init__(self, db: Session) -> None:
        self._db = db

    # -- Namespace operations ------------------------------------------------

    def get_namespace(self, namespace_id: int) -> CatalogNamespace | None:
        return self._db.get(CatalogNamespace, namespace_id)

    def get_namespace_by_name(self, name: str, parent_id: int | None) -> CatalogNamespace | None:
        return self._db.query(CatalogNamespace).filter_by(name=name, parent_id=parent_id).first()

    def list_namespaces(self, parent_id: int | None = None) -> list[CatalogNamespace]:
        q = self._db.query(CatalogNamespace)
        if parent_id is not None:
            q = q.filter(CatalogNamespace.parent_id == parent_id)
        else:
            q = q.filter(CatalogNamespace.parent_id.is_(None))
        return q.order_by(CatalogNamespace.name).all()

    def list_root_namespaces(self) -> list[CatalogNamespace]:
        return (
            self._db.query(CatalogNamespace)
            .filter(CatalogNamespace.parent_id.is_(None))
            .order_by(CatalogNamespace.name)
            .all()
        )

    def list_child_namespaces(self, parent_id: int) -> list[CatalogNamespace]:
        return self._db.query(CatalogNamespace).filter_by(parent_id=parent_id).order_by(CatalogNamespace.name).all()

    def create_namespace(self, ns: CatalogNamespace) -> CatalogNamespace:
        self._db.add(ns)
        self._db.commit()
        self._db.refresh(ns)
        return ns

    def update_namespace(self, ns: CatalogNamespace) -> CatalogNamespace:
        self._db.commit()
        self._db.refresh(ns)
        return ns

    def delete_namespace(self, namespace_id: int) -> None:
        ns = self._db.get(CatalogNamespace, namespace_id)
        if ns is not None:
            self._db.delete(ns)
            self._db.commit()

    def count_children(self, namespace_id: int) -> int:
        return self._db.query(CatalogNamespace).filter_by(parent_id=namespace_id).count()

    # -- Flow registration operations ----------------------------------------

    def get_flow(self, registration_id: int) -> FlowRegistration | None:
        return self._db.get(FlowRegistration, registration_id)

    def get_flow_by_name(self, name: str, namespace_id: int) -> FlowRegistration | None:
        return self._db.query(FlowRegistration).filter_by(name=name, namespace_id=namespace_id).first()

    def get_flow_by_path(self, flow_path: str) -> FlowRegistration | None:
        return self._db.query(FlowRegistration).filter_by(flow_path=flow_path).first()

    def list_flows(
        self,
        namespace_id: int | None = None,
        owner_id: int | None = None,
    ) -> list[FlowRegistration]:
        q = self._db.query(FlowRegistration)
        if namespace_id is not None:
            q = q.filter_by(namespace_id=namespace_id)
        if owner_id is not None:
            q = q.filter_by(owner_id=owner_id)
        return q.order_by(FlowRegistration.name).all()

    def list_flows_by_ids(self, registration_ids: list[int]) -> list[FlowRegistration]:
        if not registration_ids:
            return []
        return self._db.query(FlowRegistration).filter(FlowRegistration.id.in_(registration_ids)).all()

    def create_flow(self, reg: FlowRegistration) -> FlowRegistration:
        self._db.add(reg)
        self._db.commit()
        self._db.refresh(reg)
        return reg

    def update_flow(self, reg: FlowRegistration) -> FlowRegistration:
        self._db.commit()
        self._db.refresh(reg)
        return reg

    def delete_flow(self, registration_id: int) -> None:
        # Clean up related records first
        self._db.query(FlowFavorite).filter_by(registration_id=registration_id).delete()
        self._db.query(FlowFollow).filter_by(registration_id=registration_id).delete()
        # Remove published API endpoints and their keys. SQLite FK enforcement is
        # off so these don't cascade: a leftover key would stay enabled (a real
        # revocation gap) and the unique slug would stay occupied, blocking
        # republish. Delete keys first (they reference the endpoint), then endpoints.
        endpoint_ids = [
            row[0]
            for row in self._db.query(FlowApiEndpoint.id)
            .filter(FlowApiEndpoint.registration_id == registration_id)
            .all()
        ]
        if endpoint_ids:
            # Implicit (per-endpoint) consumers granted only to these endpoints are
            # garbage-collected; shared consumers just lose the grant (their keys have
            # a NULL endpoint_id and survive). Same explicit-delete reasoning: SQLite
            # FK enforcement is off, so grants/keys/consumers don't cascade on their own.
            granted_consumer_ids = [
                row[0]
                for row in self._db.query(ApiConsumerEndpoint.consumer_id)
                .filter(ApiConsumerEndpoint.endpoint_id.in_(endpoint_ids))
                .distinct()
                .all()
            ]
            self._db.query(ApiConsumerEndpoint).filter(ApiConsumerEndpoint.endpoint_id.in_(endpoint_ids)).delete(
                synchronize_session=False
            )
            self._db.query(FlowApiKey).filter(FlowApiKey.endpoint_id.in_(endpoint_ids)).delete(
                synchronize_session=False
            )
            self._db.query(FlowApiEndpoint).filter(FlowApiEndpoint.id.in_(endpoint_ids)).delete(
                synchronize_session=False
            )
            if granted_consumer_ids:
                orphan_implicit_ids = [
                    row[0]
                    for row in self._db.query(ApiConsumer.id)
                    .filter(
                        ApiConsumer.id.in_(granted_consumer_ids),
                        ApiConsumer.is_implicit.is_(True),
                        ~ApiConsumer.id.in_(self._db.query(ApiConsumerEndpoint.consumer_id)),
                    )
                    .all()
                ]
                if orphan_implicit_ids:
                    self._db.query(FlowApiKey).filter(FlowApiKey.consumer_id.in_(orphan_implicit_ids)).delete(
                        synchronize_session=False
                    )
                    self._db.query(ApiConsumer).filter(ApiConsumer.id.in_(orphan_implicit_ids)).delete(
                        synchronize_session=False
                    )
        # Hard-delete any soft-deleted artifacts referencing this flow
        self._db.query(GlobalArtifact).filter_by(
            source_registration_id=registration_id,
        ).filter(GlobalArtifact.status == "deleted").delete()
        # Detach historical runs from this registration so a future registration
        # that happens to reuse the same SQLite-assigned id cannot pull these
        # runs into its own per-flow history. The runs keep their flow_uuid for
        # global-history attribution.
        self._db.query(FlowRun).filter_by(registration_id=registration_id).update(
            {"registration_id": None}, synchronize_session=False
        )
        flow = self._db.get(FlowRegistration, registration_id)
        if flow is not None:
            self._db.delete(flow)
            self._db.commit()

    def _runs_of_registration(self, registration_id: int):
        """Return a filter clause matching FlowRuns belonging to a registration.

        Resolves to ``FlowRun.flow_uuid`` so a deleted+recreated registration with
        the same SQLite-assigned id can never surface the previous flow's runs.
        If the registration doesn't exist the scalar subquery yields NULL, which
        makes the equality unsatisfiable — no rows match, no explicit guard
        needed at call sites.
        """
        uuid_subq = (
            self._db.query(FlowRegistration.flow_uuid).filter_by(id=registration_id).scalar_subquery()
        )
        return FlowRun.flow_uuid == uuid_subq

    def _apply_run_filters(
        self,
        q,
        *,
        registration_id: int | None = None,
        schedule_id: int | None = None,
        run_type: RunType | None = None,
    ):
        """Apply the standard run filters to a FlowRun query."""
        if registration_id is not None:
            q = q.filter(self._runs_of_registration(registration_id))
        if schedule_id is not None:
            q = q.filter(FlowRun.schedule_id == schedule_id)
        if run_type is not None:
            q = q.filter(FlowRun.run_type == run_type)
        return q

    def count_flows_in_namespace(self, namespace_id: int) -> int:
        return self._db.query(FlowRegistration).filter_by(namespace_id=namespace_id).count()

    def count_active_artifacts_for_flow(self, registration_id: int) -> int:
        return (
            self._db.query(GlobalArtifact)
            .filter_by(source_registration_id=registration_id)
            .filter(GlobalArtifact.status != "deleted")
            .count()
        )

    # -- Artifact operations -------------------------------------------------

    def list_artifacts_for_namespace(self, namespace_id: int) -> list[GlobalArtifact]:
        """List active artifacts belonging to a namespace."""
        return (
            self._db.query(GlobalArtifact)
            .filter_by(namespace_id=namespace_id)
            .filter(GlobalArtifact.status != "deleted")
            .order_by(GlobalArtifact.name, GlobalArtifact.version.desc())
            .all()
        )

    def list_artifacts_for_flow(self, registration_id: int) -> list[GlobalArtifact]:
        """List active artifacts produced by a specific flow."""
        return (
            self._db.query(GlobalArtifact)
            .filter_by(source_registration_id=registration_id)
            .filter(GlobalArtifact.status != "deleted")
            .order_by(GlobalArtifact.name, GlobalArtifact.version.desc())
            .all()
        )

    def count_all_active_artifacts(self) -> int:
        """Count all non-deleted artifacts across all namespaces."""
        return self._db.query(GlobalArtifact).filter(GlobalArtifact.status != "deleted").count()

    def bulk_get_artifact_counts(self, flow_ids: list[int]) -> dict[int, int]:
        """Get artifact counts per flow in a single query.

        Returns a dict mapping flow registration ID -> active artifact count.
        """
        if not flow_ids:
            return {}

        rows = (
            self._db.query(
                GlobalArtifact.source_registration_id,
                func.count(GlobalArtifact.id),
            )
            .filter(
                GlobalArtifact.source_registration_id.in_(flow_ids),
                GlobalArtifact.status != "deleted",
            )
            .group_by(GlobalArtifact.source_registration_id)
            .all()
        )
        return {reg_id: count for reg_id, count in rows}

    # -- Run operations ------------------------------------------------------

    def get_run(self, run_id: int) -> FlowRun | None:
        return self._db.get(FlowRun, run_id)

    def list_runs(
        self,
        registration_id: int | None = None,
        schedule_id: int | None = None,
        run_type: RunType | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[FlowRun]:
        q = self._apply_run_filters(
            self._db.query(FlowRun),
            registration_id=registration_id,
            schedule_id=schedule_id,
            run_type=run_type,
        )
        return q.order_by(FlowRun.started_at.desc()).offset(offset).limit(limit).all()

    def create_run(self, run: FlowRun) -> FlowRun:
        self._db.add(run)
        self._db.commit()
        self._db.refresh(run)
        return run

    def update_run(self, run: FlowRun) -> FlowRun:
        self._db.commit()
        self._db.refresh(run)
        return run

    def count_runs(
        self,
        registration_id: int | None = None,
        schedule_id: int | None = None,
        run_type: RunType | None = None,
    ) -> int:
        return self._apply_run_filters(
            self._db.query(FlowRun),
            registration_id=registration_id,
            schedule_id=schedule_id,
            run_type=run_type,
        ).count()

    def count_runs_by_status(
        self,
        registration_id: int | None = None,
        schedule_id: int | None = None,
        run_type: RunType | None = None,
    ) -> dict[str, int]:
        from sqlalchemy import case, func

        q = self._apply_run_filters(
            self._db.query(
                func.count().label("total"),
                func.count(case((FlowRun.success.is_(True), 1))).label("success"),
                func.count(case((FlowRun.success.is_(False), 1))).label("failed"),
                func.count(case((FlowRun.success.is_(None), 1))).label("running"),
            ),
            registration_id=registration_id,
            schedule_id=schedule_id,
            run_type=run_type,
        )
        row = q.one()
        return {"total": row.total, "success": row.success, "failed": row.failed, "running": row.running}

    # -- Favorites -----------------------------------------------------------

    def get_favorite(self, user_id: int, registration_id: int) -> FlowFavorite | None:
        return self._db.query(FlowFavorite).filter_by(user_id=user_id, registration_id=registration_id).first()

    def add_favorite(self, fav: FlowFavorite) -> FlowFavorite:
        self._db.add(fav)
        self._db.commit()
        self._db.refresh(fav)
        return fav

    def remove_favorite(self, user_id: int, registration_id: int) -> None:
        fav = self._db.query(FlowFavorite).filter_by(user_id=user_id, registration_id=registration_id).first()
        if fav is not None:
            self._db.delete(fav)
            self._db.commit()

    def list_favorites(self, user_id: int) -> list[FlowFavorite]:
        return self._db.query(FlowFavorite).filter_by(user_id=user_id).order_by(FlowFavorite.created_at.desc()).all()

    def count_favorites(self, user_id: int) -> int:
        return self._db.query(FlowFavorite).filter_by(user_id=user_id).count()

    # -- Follows -------------------------------------------------------------

    def get_follow(self, user_id: int, registration_id: int) -> FlowFollow | None:
        return self._db.query(FlowFollow).filter_by(user_id=user_id, registration_id=registration_id).first()

    def add_follow(self, follow: FlowFollow) -> FlowFollow:
        self._db.add(follow)
        self._db.commit()
        self._db.refresh(follow)
        return follow

    def remove_follow(self, user_id: int, registration_id: int) -> None:
        follow = self._db.query(FlowFollow).filter_by(user_id=user_id, registration_id=registration_id).first()
        if follow is not None:
            self._db.delete(follow)
            self._db.commit()

    def list_follows(self, user_id: int) -> list[FlowFollow]:
        return self._db.query(FlowFollow).filter_by(user_id=user_id).order_by(FlowFollow.created_at.desc()).all()

    # -- Aggregate helpers ---------------------------------------------------

    def count_run_for_flow(self, registration_id: int) -> int:
        return self._db.query(FlowRun).filter(self._runs_of_registration(registration_id)).count()

    def last_run_for_flow(self, registration_id: int) -> FlowRun | None:
        return (
            self._db.query(FlowRun)
            .filter(self._runs_of_registration(registration_id))
            .order_by(FlowRun.started_at.desc())
            .first()
        )

    def count_catalog_namespaces(self) -> int:
        return self._db.query(CatalogNamespace).filter_by(level=0).count()

    def count_all_flows(self) -> int:
        return self._db.query(FlowRegistration).count()

    # -- Catalog table operations -----------------------------------------------

    def get_table(self, table_id: int) -> CatalogTable | None:
        return self._db.get(CatalogTable, table_id)

    def get_table_by_name(self, name: str, namespace_id: int | None) -> CatalogTable | None:
        return self._db.query(CatalogTable).filter_by(name=name, namespace_id=namespace_id).first()

    def list_tables_by_name(self, name: str) -> list[CatalogTable]:
        return (
            self._db.query(CatalogTable)
            .filter_by(name=name)
            .order_by(CatalogTable.namespace_id.asc(), CatalogTable.id.asc())
            .all()
        )

    def list_tables(self, namespace_id: int | None = None) -> list[CatalogTable]:
        q = self._db.query(CatalogTable)
        if namespace_id is not None:
            q = q.filter_by(namespace_id=namespace_id)
        return q.order_by(CatalogTable.name).all()

    def list_tables_for_namespace(self, namespace_id: int) -> list[CatalogTable]:
        return self._db.query(CatalogTable).filter_by(namespace_id=namespace_id).order_by(CatalogTable.name).all()

    def create_table(self, table: CatalogTable) -> CatalogTable:
        self._db.add(table)
        self._db.commit()
        self._db.refresh(table)
        return table

    def update_table(self, table: CatalogTable) -> CatalogTable:
        self._db.commit()
        self._db.refresh(table)
        return table

    def delete_table(self, table_id: int) -> None:
        self._db.query(CatalogTableReadLink).filter_by(table_id=table_id).delete()
        self._db.query(TableFavorite).filter_by(table_id=table_id).delete()
        self._db.query(CatalogVisualization).filter_by(catalog_table_id=table_id).delete()
        table = self._db.get(CatalogTable, table_id)
        if table is not None:
            self._db.delete(table)
            self._db.commit()

    def count_tables_in_namespace(self, namespace_id: int) -> int:
        return self._db.query(CatalogTable).filter_by(namespace_id=namespace_id).count()

    def count_all_tables(self) -> int:
        return self._db.query(CatalogTable).count()

    # -- Virtual table helpers -----------------------------------------------

    def list_virtual_tables(self, namespace_id: int | None = None) -> list[CatalogTable]:
        q = self._db.query(CatalogTable).filter(CatalogTable.table_type == "virtual")
        if namespace_id is not None:
            q = q.filter(CatalogTable.namespace_id == namespace_id)
        return q.order_by(CatalogTable.name).all()

    def get_virtual_table_by_producer(self, registration_id: int) -> CatalogTable | None:
        return (
            self._db.query(CatalogTable)
            .filter_by(table_type="virtual", producer_registration_id=registration_id)
            .first()
        )

    def count_virtual_tables(self) -> int:
        return self._db.query(CatalogTable).filter(CatalogTable.table_type == "virtual").count()

    # -- Table Favorites -----------------------------------------------------

    def get_table_favorite(self, user_id: int, table_id: int) -> TableFavorite | None:
        return self._db.query(TableFavorite).filter_by(user_id=user_id, table_id=table_id).first()

    def add_table_favorite(self, fav: TableFavorite) -> TableFavorite:
        self._db.add(fav)
        self._db.commit()
        self._db.refresh(fav)
        return fav

    def remove_table_favorite(self, user_id: int, table_id: int) -> None:
        fav = self._db.query(TableFavorite).filter_by(user_id=user_id, table_id=table_id).first()
        if fav is not None:
            self._db.delete(fav)
            self._db.commit()

    def list_table_favorites(self, user_id: int) -> list[TableFavorite]:
        return self._db.query(TableFavorite).filter_by(user_id=user_id).order_by(TableFavorite.created_at.desc()).all()

    def count_table_favorites(self, user_id: int) -> int:
        return self._db.query(TableFavorite).filter_by(user_id=user_id).count()

    # -- Bulk enrichment helpers (for N+1 elimination) -----------------------

    def bulk_get_favorite_flow_ids(self, user_id: int, flow_ids: list[int]) -> set[int]:
        """Return the subset of flow_ids that the user has favourited."""
        if not flow_ids:
            return set()
        rows = (
            self._db.query(FlowFavorite.registration_id)
            .filter(
                FlowFavorite.user_id == user_id,
                FlowFavorite.registration_id.in_(flow_ids),
            )
            .all()
        )
        return {r[0] for r in rows}

    def bulk_get_follow_flow_ids(self, user_id: int, flow_ids: list[int]) -> set[int]:
        """Return the subset of flow_ids that the user is following."""
        if not flow_ids:
            return set()
        rows = (
            self._db.query(FlowFollow.registration_id)
            .filter(
                FlowFollow.user_id == user_id,
                FlowFollow.registration_id.in_(flow_ids),
            )
            .all()
        )
        return {r[0] for r in rows}

    def bulk_get_favorite_table_ids(self, user_id: int, table_ids: list[int]) -> set[int]:
        """Return the subset of table_ids that the user has favourited."""
        if not table_ids:
            return set()
        rows = (
            self._db.query(TableFavorite.table_id)
            .filter(
                TableFavorite.user_id == user_id,
                TableFavorite.table_id.in_(table_ids),
            )
            .all()
        )
        return {r[0] for r in rows}

    def bulk_get_run_stats(self, flow_ids: list[int]) -> dict[int, tuple[int, FlowRun | None]]:
        """Return run_count and last_run for each flow_id in one query batch.

        Grouping is by ``flow_uuid`` (resolved from ``flow_registrations``) so
        history that survived a registration delete+recreate stays attached to
        the original flow only.
        """
        if not flow_ids:
            return {}

        # registration_id -> flow_uuid (skip ids that no longer exist)
        uuid_rows = (
            self._db.query(FlowRegistration.id, FlowRegistration.flow_uuid)
            .filter(FlowRegistration.id.in_(flow_ids))
            .all()
        )
        id_to_uuid = {rid: uuid for rid, uuid in uuid_rows}
        uuids = list(id_to_uuid.values())
        if not uuids:
            return {fid: (0, None) for fid in flow_ids}

        # Query 1: counts per flow_uuid
        count_rows = (
            self._db.query(FlowRun.flow_uuid, func.count(FlowRun.id).label("cnt"))
            .filter(FlowRun.flow_uuid.in_(uuids))
            .group_by(FlowRun.flow_uuid)
            .all()
        )
        counts = {uuid: cnt for uuid, cnt in count_rows}

        # Query 2: last run per flow_uuid using a subquery for max started_at
        subq = (
            self._db.query(
                FlowRun.flow_uuid,
                func.max(FlowRun.started_at).label("max_started"),
            )
            .filter(FlowRun.flow_uuid.in_(uuids))
            .group_by(FlowRun.flow_uuid)
            .subquery()
        )
        last_runs_rows = (
            self._db.query(FlowRun)
            .join(
                subq,
                (FlowRun.flow_uuid == subq.c.flow_uuid) & (FlowRun.started_at == subq.c.max_started),
            )
            .all()
        )
        last_runs = {r.flow_uuid: r for r in last_runs_rows}

        result: dict[int, tuple[int, FlowRun | None]] = {}
        for fid in flow_ids:
            uuid_ = id_to_uuid.get(fid)
            result[fid] = (counts.get(uuid_, 0), last_runs.get(uuid_)) if uuid_ else (0, None)
        return result

    def list_tables_for_flow(self, registration_id: int) -> list[CatalogTable]:
        """Return all catalog tables produced by a specific flow."""
        return (
            self._db.query(CatalogTable)
            .filter(CatalogTable.source_registration_id == registration_id)
            .order_by(CatalogTable.name)
            .all()
        )

    def bulk_get_tables_for_flows(self, flow_ids: list[int]) -> dict[int, list[CatalogTable]]:
        """Return tables produced by each flow_id in one query."""
        if not flow_ids:
            return {}
        rows = (
            self._db.query(CatalogTable)
            .filter(CatalogTable.source_registration_id.in_(flow_ids))
            .order_by(CatalogTable.name)
            .all()
        )
        result: dict[int, list[CatalogTable]] = {}
        for table in rows:
            result.setdefault(table.source_registration_id, []).append(table)
        return result

    def upsert_read_link(self, table_id: int, registration_id: int) -> None:
        """Record that a flow reads from a catalog table (idempotent)."""
        existing = (
            self._db.query(CatalogTableReadLink).filter_by(table_id=table_id, registration_id=registration_id).first()
        )
        if not existing:
            self._db.add(CatalogTableReadLink(table_id=table_id, registration_id=registration_id))
            self._db.commit()

    def list_readers_for_table(self, table_id: int) -> list[FlowRegistration]:
        """Return all flows that read from a given table."""
        link_rows = (
            self._db.query(CatalogTableReadLink.registration_id).filter(CatalogTableReadLink.table_id == table_id).all()
        )
        reg_ids = [r[0] for r in link_rows]
        if not reg_ids:
            return []
        return (
            self._db.query(FlowRegistration)
            .filter(FlowRegistration.id.in_(reg_ids))
            .order_by(FlowRegistration.name)
            .all()
        )

    def list_read_tables_for_flow(self, registration_id: int) -> list[CatalogTable]:
        """Return all tables that a given flow reads from."""
        link_rows = (
            self._db.query(CatalogTableReadLink.table_id)
            .filter(CatalogTableReadLink.registration_id == registration_id)
            .all()
        )
        table_ids = [r[0] for r in link_rows]
        if not table_ids:
            return []
        return self._db.query(CatalogTable).filter(CatalogTable.id.in_(table_ids)).order_by(CatalogTable.name).all()

    def bulk_get_read_tables_for_flows(self, flow_ids: list[int]) -> dict[int, list[CatalogTable]]:
        """Return tables read by each flow_id in one query."""
        if not flow_ids:
            return {}
        link_rows = (
            self._db.query(CatalogTableReadLink).filter(CatalogTableReadLink.registration_id.in_(flow_ids)).all()
        )
        table_ids = list({link.table_id for link in link_rows})
        if not table_ids:
            return {}
        tables_by_id = {
            t.id: t
            for t in self._db.query(CatalogTable)
            .filter(CatalogTable.id.in_(table_ids))
            .order_by(CatalogTable.name)
            .all()
        }
        result: dict[int, list[CatalogTable]] = {}
        for link in link_rows:
            table = tables_by_id.get(link.table_id)
            if table:
                result.setdefault(link.registration_id, []).append(table)
        return result

    # -- Schedule operations -------------------------------------------------

    def get_schedule(self, schedule_id: int) -> FlowSchedule | None:
        return self._db.get(FlowSchedule, schedule_id)

    def list_schedules(
        self,
        registration_id: int | None = None,
        enabled_only: bool = False,
    ) -> list[FlowSchedule]:
        q = self._db.query(FlowSchedule)
        if registration_id is not None:
            q = q.filter_by(registration_id=registration_id)
        if enabled_only:
            q = q.filter(FlowSchedule.enabled.is_(True))
        return q.order_by(FlowSchedule.created_at.desc()).all()

    def create_schedule(self, schedule: FlowSchedule) -> FlowSchedule:
        self._db.add(schedule)
        self._db.commit()
        self._db.refresh(schedule)
        return schedule

    def update_schedule(self, schedule: FlowSchedule) -> FlowSchedule:
        self._db.commit()
        self._db.refresh(schedule)
        return schedule

    def delete_schedule(self, schedule_id: int) -> None:
        self._db.query(ScheduleTriggerTable).filter_by(schedule_id=schedule_id).delete()
        schedule = self._db.get(FlowSchedule, schedule_id)
        if schedule is not None:
            self._db.delete(schedule)
            self._db.commit()

    def count_schedules(self) -> int:
        return self._db.query(FlowSchedule).count()

    # -- Active run operations -----------------------------------------------

    def list_active_runs(self) -> list[FlowRun]:
        """Return runs that have not yet ended (ended_at IS NULL)."""
        return self._db.query(FlowRun).filter(FlowRun.ended_at.is_(None)).order_by(FlowRun.started_at.desc()).all()

    def has_active_run(self, registration_id: int) -> bool:
        """Check if a flow already has an active (unfinished) run."""
        return (
            self._db.query(FlowRun)
            .filter(self._runs_of_registration(registration_id), FlowRun.ended_at.is_(None))
            .first()
            is not None
        )

    def list_due_interval_schedules(self) -> list[FlowSchedule]:
        """Return enabled interval schedules (filtering done in Python)."""
        return (
            self._db.query(FlowSchedule)
            .filter(FlowSchedule.enabled.is_(True), FlowSchedule.schedule_type == "interval")
            .all()
        )

    def list_table_trigger_schedules(self) -> list[FlowSchedule]:
        """Return enabled table-trigger schedules."""
        return (
            self._db.query(FlowSchedule)
            .filter(FlowSchedule.enabled.is_(True), FlowSchedule.schedule_type == "table_trigger")
            .all()
        )

    def list_table_trigger_schedules_for_table(self, table_id: int) -> list[FlowSchedule]:
        """Return enabled table-trigger schedules watching a specific table."""
        return (
            self._db.query(FlowSchedule)
            .filter(
                FlowSchedule.enabled.is_(True),
                FlowSchedule.schedule_type == "table_trigger",
                FlowSchedule.trigger_table_id == table_id,
            )
            .all()
        )

    def get_trigger_table_ids(self, schedule_id: int) -> list[int]:
        """Return table IDs linked to a table_set_trigger schedule."""
        rows = (
            self._db.query(ScheduleTriggerTable.table_id).filter(ScheduleTriggerTable.schedule_id == schedule_id).all()
        )
        return [r[0] for r in rows]

    def set_trigger_table_ids(self, schedule_id: int, table_ids: list[int]) -> None:
        """Replace all trigger table links for a schedule."""
        self._db.query(ScheduleTriggerTable).filter_by(schedule_id=schedule_id).delete()
        for tid in table_ids:
            self._db.add(ScheduleTriggerTable(schedule_id=schedule_id, table_id=tid))
        self._db.commit()

    def delete_trigger_table_ids(self, schedule_id: int) -> None:
        """Remove all trigger table links for a schedule."""
        self._db.query(ScheduleTriggerTable).filter_by(schedule_id=schedule_id).delete()
        self._db.commit()

    # -- Visualizations ------------------------------------------------------

    def list_visualizations(self, catalog_table_id: int) -> list[CatalogVisualization]:
        return (
            self._db.query(CatalogVisualization)
            .filter_by(catalog_table_id=catalog_table_id)
            .order_by(CatalogVisualization.created_at.desc())
            .all()
        )

    def list_all_visualizations(self) -> list[CatalogVisualization]:
        return self._db.query(CatalogVisualization).order_by(CatalogVisualization.created_at.desc()).all()

    def get_visualization(self, viz_id: int) -> CatalogVisualization | None:
        return self._db.get(CatalogVisualization, viz_id)

    def get_visualization_by_name(self, catalog_table_id: int, name: str) -> CatalogVisualization | None:
        return self._db.query(CatalogVisualization).filter_by(catalog_table_id=catalog_table_id, name=name).first()

    def create_visualization(self, viz: CatalogVisualization) -> CatalogVisualization:
        self._db.add(viz)
        self._db.commit()
        self._db.refresh(viz)
        return viz

    def update_visualization(self, viz: CatalogVisualization) -> CatalogVisualization:
        self._db.commit()
        self._db.refresh(viz)
        return viz

    def delete_visualization(self, viz_id: int) -> None:
        viz = self._db.get(CatalogVisualization, viz_id)
        if viz is not None:
            self._db.delete(viz)
            self._db.commit()

    # -- Dashboards ----------------------------------------------------------

    def list_dashboards(self) -> list[CatalogDashboard]:
        return self._db.query(CatalogDashboard).order_by(CatalogDashboard.updated_at.desc()).all()

    def get_dashboard(self, dashboard_id: int) -> CatalogDashboard | None:
        return self._db.get(CatalogDashboard, dashboard_id)

    def create_dashboard(self, dashboard: CatalogDashboard) -> CatalogDashboard:
        self._db.add(dashboard)
        self._db.commit()
        self._db.refresh(dashboard)
        return dashboard

    def update_dashboard(self, dashboard: CatalogDashboard) -> CatalogDashboard:
        self._db.commit()
        self._db.refresh(dashboard)
        return dashboard

    def delete_dashboard(self, dashboard_id: int) -> None:
        dashboard = self._db.get(CatalogDashboard, dashboard_id)
        if dashboard is not None:
            self._db.delete(dashboard)
            self._db.commit()
