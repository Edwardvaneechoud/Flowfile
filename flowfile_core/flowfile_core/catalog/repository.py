"""Data-access abstraction for the Flow Catalog system.

Defines a ``CatalogRepository`` :pep:`544` Protocol and provides a concrete
``SQLAlchemyCatalogRepository`` implementation backed by SQLAlchemy.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from sqlalchemy.orm import Session

from flowfile_core.database.models import (
    CatalogNamespace,
    CatalogTable,
    FlowFavorite,
    FlowFollow,
    FlowRegistration,
    FlowRun,
    GlobalArtifact,
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
        limit: int = 50,
        offset: int = 0,
    ) -> list[FlowRun]: ...

    def create_run(self, run: FlowRun) -> FlowRun: ...

    def update_run(self, run: FlowRun) -> FlowRun: ...

    def count_runs(self) -> int: ...

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

    def list_tables(self, namespace_id: int | None = None) -> list[CatalogTable]: ...

    def list_tables_for_namespace(self, namespace_id: int) -> list[CatalogTable]: ...

    def create_table(self, table: CatalogTable) -> CatalogTable: ...

    def update_table(self, table: CatalogTable) -> CatalogTable: ...

    def delete_table(self, table_id: int) -> None: ...

    def count_tables_in_namespace(self, namespace_id: int) -> int: ...

    def count_all_tables(self) -> int: ...

    def list_all_tables(self) -> list[CatalogTable]: ...

    # -- Bulk enrichment helpers (for N+1 elimination) -----------------------

    def bulk_get_favorite_flow_ids(self, user_id: int, flow_ids: list[int]) -> set[int]: ...

    def bulk_get_follow_flow_ids(self, user_id: int, flow_ids: list[int]) -> set[int]: ...

    def bulk_get_run_stats(self, flow_ids: list[int]) -> dict[int, tuple[int, FlowRun | None]]: ...


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
        # Hard-delete any soft-deleted artifacts referencing this flow
        self._db.query(GlobalArtifact).filter_by(
            source_registration_id=registration_id,
        ).filter(GlobalArtifact.status == "deleted").delete()
        flow = self._db.get(FlowRegistration, registration_id)
        if flow is not None:
            self._db.delete(flow)
            self._db.commit()

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
        from sqlalchemy import func

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
        limit: int = 50,
        offset: int = 0,
    ) -> list[FlowRun]:
        q = self._db.query(FlowRun)
        if registration_id is not None:
            q = q.filter_by(registration_id=registration_id)
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

    def count_runs(self) -> int:
        return self._db.query(FlowRun).count()

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
        return self._db.query(FlowRun).filter_by(registration_id=registration_id).count()

    def last_run_for_flow(self, registration_id: int) -> FlowRun | None:
        return (
            self._db.query(FlowRun)
            .filter_by(registration_id=registration_id)
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
        table = self._db.get(CatalogTable, table_id)
        if table is not None:
            self._db.delete(table)
            self._db.commit()

    def count_tables_in_namespace(self, namespace_id: int) -> int:
        return self._db.query(CatalogTable).filter_by(namespace_id=namespace_id).count()

    def count_all_tables(self) -> int:
        return self._db.query(CatalogTable).count()

    def list_all_tables(self) -> list[CatalogTable]:
        return self._db.query(CatalogTable).order_by(CatalogTable.name).all()

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

    def bulk_get_run_stats(self, flow_ids: list[int]) -> dict[int, tuple[int, FlowRun | None]]:
        """Return run_count and last_run for each flow_id in one query batch.

        Returns a dict: flow_id -> (run_count, last_run_or_none)
        """
        if not flow_ids:
            return {}

        from sqlalchemy import func

        # Query 1: counts per registration_id
        count_rows = (
            self._db.query(
                FlowRun.registration_id,
                func.count(FlowRun.id).label("cnt"),
            )
            .filter(FlowRun.registration_id.in_(flow_ids))
            .group_by(FlowRun.registration_id)
            .all()
        )
        counts = {r[0]: r[1] for r in count_rows}

        # Query 2: last run per registration_id using a subquery for max started_at
        subq = (
            self._db.query(
                FlowRun.registration_id,
                func.max(FlowRun.started_at).label("max_started"),
            )
            .filter(FlowRun.registration_id.in_(flow_ids))
            .group_by(FlowRun.registration_id)
            .subquery()
        )
        last_runs_rows = (
            self._db.query(FlowRun)
            .join(
                subq,
                (FlowRun.registration_id == subq.c.registration_id) & (FlowRun.started_at == subq.c.max_started),
            )
            .all()
        )
        last_runs = {r.registration_id: r for r in last_runs_rows}

        # Build result dict
        result: dict[int, tuple[int, FlowRun | None]] = {}
        for fid in flow_ids:
            result[fid] = (counts.get(fid, 0), last_runs.get(fid))
        return result
