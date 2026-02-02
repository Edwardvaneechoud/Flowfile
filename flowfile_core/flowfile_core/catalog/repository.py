"""Data-access abstraction for the Flow Catalog system.

Defines a ``CatalogRepository`` :pep:`544` Protocol and provides a concrete
``SQLAlchemyCatalogRepository`` implementation backed by SQLAlchemy.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from sqlalchemy.orm import Session

from flowfile_core.database.models import (
    CatalogNamespace,
    FlowFavorite,
    FlowFollow,
    FlowRegistration,
    FlowRun,
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

    def get_namespace_by_name(
        self, name: str, parent_id: int | None
    ) -> CatalogNamespace | None: ...

    def list_namespaces(self, parent_id: int | None = None) -> list[CatalogNamespace]: ...

    def list_root_namespaces(self) -> list[CatalogNamespace]: ...

    def list_child_namespaces(self, parent_id: int) -> list[CatalogNamespace]: ...

    def create_namespace(self, ns: CatalogNamespace) -> CatalogNamespace: ...

    def update_namespace(self, ns: CatalogNamespace) -> CatalogNamespace: ...

    def delete_namespace(self, namespace_id: int) -> None: ...

    def count_children(self, namespace_id: int) -> int: ...

    # -- Flow registration operations ----------------------------------------

    def get_flow(self, registration_id: int) -> FlowRegistration | None: ...

    def get_flow_by_name(
        self, name: str, namespace_id: int
    ) -> FlowRegistration | None: ...

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

    def get_favorite(
        self, user_id: int, registration_id: int
    ) -> FlowFavorite | None: ...

    def add_favorite(self, fav: FlowFavorite) -> FlowFavorite: ...

    def remove_favorite(self, user_id: int, registration_id: int) -> None: ...

    def list_favorites(self, user_id: int) -> list[FlowFavorite]: ...

    def count_favorites(self, user_id: int) -> int: ...

    # -- Follows -------------------------------------------------------------

    def get_follow(
        self, user_id: int, registration_id: int
    ) -> FlowFollow | None: ...

    def add_follow(self, follow: FlowFollow) -> FlowFollow: ...

    def remove_follow(self, user_id: int, registration_id: int) -> None: ...

    def list_follows(self, user_id: int) -> list[FlowFollow]: ...

    # -- Aggregate helpers ---------------------------------------------------

    def count_run_for_flow(self, registration_id: int) -> int: ...

    def last_run_for_flow(self, registration_id: int) -> FlowRun | None: ...

    def count_catalog_namespaces(self) -> int: ...

    def count_all_flows(self) -> int: ...


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

    def get_namespace_by_name(
        self, name: str, parent_id: int | None
    ) -> CatalogNamespace | None:
        return (
            self._db.query(CatalogNamespace)
            .filter_by(name=name, parent_id=parent_id)
            .first()
        )

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
        return (
            self._db.query(CatalogNamespace)
            .filter_by(parent_id=parent_id)
            .order_by(CatalogNamespace.name)
            .all()
        )

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
        return (
            self._db.query(CatalogNamespace)
            .filter_by(parent_id=namespace_id)
            .count()
        )

    # -- Flow registration operations ----------------------------------------

    def get_flow(self, registration_id: int) -> FlowRegistration | None:
        return self._db.get(FlowRegistration, registration_id)

    def get_flow_by_name(
        self, name: str, namespace_id: int
    ) -> FlowRegistration | None:
        return (
            self._db.query(FlowRegistration)
            .filter_by(name=name, namespace_id=namespace_id)
            .first()
        )

    def get_flow_by_path(self, flow_path: str) -> FlowRegistration | None:
        return (
            self._db.query(FlowRegistration)
            .filter_by(flow_path=flow_path)
            .first()
        )

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
        flow = self._db.get(FlowRegistration, registration_id)
        if flow is not None:
            self._db.delete(flow)
            self._db.commit()

    def count_flows_in_namespace(self, namespace_id: int) -> int:
        return (
            self._db.query(FlowRegistration)
            .filter_by(namespace_id=namespace_id)
            .count()
        )

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
        return (
            q.order_by(FlowRun.started_at.desc())
            .offset(offset)
            .limit(limit)
            .all()
        )

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

    def get_favorite(
        self, user_id: int, registration_id: int
    ) -> FlowFavorite | None:
        return (
            self._db.query(FlowFavorite)
            .filter_by(user_id=user_id, registration_id=registration_id)
            .first()
        )

    def add_favorite(self, fav: FlowFavorite) -> FlowFavorite:
        self._db.add(fav)
        self._db.commit()
        self._db.refresh(fav)
        return fav

    def remove_favorite(self, user_id: int, registration_id: int) -> None:
        fav = (
            self._db.query(FlowFavorite)
            .filter_by(user_id=user_id, registration_id=registration_id)
            .first()
        )
        if fav is not None:
            self._db.delete(fav)
            self._db.commit()

    def list_favorites(self, user_id: int) -> list[FlowFavorite]:
        return (
            self._db.query(FlowFavorite)
            .filter_by(user_id=user_id)
            .order_by(FlowFavorite.created_at.desc())
            .all()
        )

    def count_favorites(self, user_id: int) -> int:
        return (
            self._db.query(FlowFavorite)
            .filter_by(user_id=user_id)
            .count()
        )

    # -- Follows -------------------------------------------------------------

    def get_follow(
        self, user_id: int, registration_id: int
    ) -> FlowFollow | None:
        return (
            self._db.query(FlowFollow)
            .filter_by(user_id=user_id, registration_id=registration_id)
            .first()
        )

    def add_follow(self, follow: FlowFollow) -> FlowFollow:
        self._db.add(follow)
        self._db.commit()
        self._db.refresh(follow)
        return follow

    def remove_follow(self, user_id: int, registration_id: int) -> None:
        follow = (
            self._db.query(FlowFollow)
            .filter_by(user_id=user_id, registration_id=registration_id)
            .first()
        )
        if follow is not None:
            self._db.delete(follow)
            self._db.commit()

    def list_follows(self, user_id: int) -> list[FlowFollow]:
        return (
            self._db.query(FlowFollow)
            .filter_by(user_id=user_id)
            .order_by(FlowFollow.created_at.desc())
            .all()
        )

    # -- Aggregate helpers ---------------------------------------------------

    def count_run_for_flow(self, registration_id: int) -> int:
        return (
            self._db.query(FlowRun)
            .filter_by(registration_id=registration_id)
            .count()
        )

    def last_run_for_flow(self, registration_id: int) -> FlowRun | None:
        return (
            self._db.query(FlowRun)
            .filter_by(registration_id=registration_id)
            .order_by(FlowRun.started_at.desc())
            .first()
        )

    def count_catalog_namespaces(self) -> int:
        return (
            self._db.query(CatalogNamespace)
            .filter_by(level=0)
            .count()
        )

    def count_all_flows(self) -> int:
        return self._db.query(FlowRegistration).count()
