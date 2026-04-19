"""Catalog registration helpers for flow persistence.

These functions bridge the in-memory flow system with the catalog database.
They are intentionally kept as standalone functions (not on FlowfileHandler)
to avoid coupling the handler to the database layer.
"""

import logging
from dataclasses import dataclass
from pathlib import Path

from flowfile_core.catalog import CatalogService
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import FlowRegistration

logger = logging.getLogger(__name__)


class FlowPathNamespaceCollision(Exception):
    """Raised when a flow_path already exists under a different namespace."""


class FlowNameNamespaceCollision(Exception):
    """Raised when a new registration would share its display name with an
    existing, distinct flow in the same namespace.

    The catalog picker lists flows by ``name``; allowing two entries with the
    same name in one namespace forces users to guess which one they picked.
    The overwrite flow is the correct path for reusing an existing name.
    """


@dataclass(frozen=True)
class FlowRegistrationSnapshot:
    """Detached snapshot of a ``FlowRegistration`` row.

    Returned by lookup helpers so callers can access fields outside the DB session
    without triggering lazy loads.
    """

    id: int
    name: str
    flow_path: str
    namespace_id: int | None
    owner_id: int


def auto_register_flow(flow_path: str, name: str, user_id: int | None) -> None:
    """Register a flow in the default catalog namespace (General > default) if it exists.

    Failures are logged at info level since users may wonder why some flows
    don't appear in the catalog.
    """
    if user_id is None or flow_path is None:
        return
    try:
        with get_db_context() as db:
            service = CatalogService(SQLAlchemyCatalogRepository(db))
            reg = service.auto_register_flow(flow_path, name, user_id)
            if reg:
                logger.info(f"Auto-registered flow '{reg.name}' in default namespace")
    except Exception:
        logger.info(f"Auto-registration failed for '{flow_path}' (non-critical)", exc_info=True)


def register_flow_in_namespace(
    flow_path: str,
    name: str,
    user_id: int | None,
    namespace_id: int | None,
) -> None:
    """Register a flow in a specific namespace, or auto-register if namespace_id is None.

    If a registration already exists at ``flow_path`` in the same namespace, it is
    touched/updated.  If one exists in a **different** namespace, raise
    ``FlowPathNamespaceCollision`` — normal callers prefix filenames with the flow id
    so this signals a real bug rather than user error.  The route layer is
    responsible for translating this to an HTTP 409.

    Unlike :func:`auto_register_flow`, this is an explicit save-to-namespace path:
    unexpected errors (e.g. DB failures) propagate to the caller instead of being
    swallowed.
    """
    if namespace_id is None:
        auto_register_flow(flow_path, name, user_id)
        return
    if user_id is None or flow_path is None:
        return
    with get_db_context() as db:
        service = CatalogService(SQLAlchemyCatalogRepository(db))
        existing = service.repo.get_flow_by_path(flow_path)
        if existing:
            if existing.namespace_id != namespace_id:
                raise FlowPathNamespaceCollision(
                    f"flow_path '{flow_path}' is already registered under "
                    f"namespace {existing.namespace_id}; refusing to reassign to {namespace_id}"
                )
            service.update_flow(
                registration_id=existing.id,
                requesting_user_id=user_id,
                namespace_id=namespace_id,
            )
            return
        reg_name = name or Path(flow_path).stem
        name_clash = service.repo.get_flow_by_name(reg_name, namespace_id)
        if name_clash is not None:
            # New registration would share its display name with an existing,
            # distinct flow in the same namespace.  The caller should overwrite
            # instead — raise so the route can translate to 409 with guidance.
            raise FlowNameNamespaceCollision(
                f"A flow named '{reg_name}' already exists in this namespace. "
                "Select it in the catalog picker to overwrite, or choose a different name."
            )
        reg = FlowRegistration(
            name=reg_name,
            flow_path=flow_path,
            namespace_id=namespace_id,
            owner_id=user_id,
        )
        service.repo.create_flow(reg)


def _snapshot(reg: FlowRegistration | None) -> FlowRegistrationSnapshot | None:
    if reg is None:
        return None
    return FlowRegistrationSnapshot(
        id=reg.id,
        name=reg.name,
        flow_path=reg.flow_path,
        namespace_id=reg.namespace_id,
        owner_id=reg.owner_id,
    )


def find_registration_by_path(flow_path: str) -> FlowRegistrationSnapshot | None:
    """Return FlowRegistrationSnapshot for an existing flow_path, or None."""
    if not flow_path:
        return None
    with get_db_context() as db:
        service = CatalogService(SQLAlchemyCatalogRepository(db))
        return _snapshot(service.repo.get_flow_by_path(flow_path))


def find_registration_by_name(name: str, namespace_id: int) -> FlowRegistrationSnapshot | None:
    """Return the FlowRegistrationSnapshot for ``(name, namespace_id)``, or None.

    Used by the route layer to detect display-name collisions before writing
    any YAML — two registrations with the same name in one namespace would
    render ambiguously in the catalog picker.
    """
    if not name:
        return None
    with get_db_context() as db:
        service = CatalogService(SQLAlchemyCatalogRepository(db))
        return _snapshot(service.repo.get_flow_by_name(name, namespace_id))


def find_registration_by_registration_id(rid: int | None) -> FlowRegistrationSnapshot | None:
    """Return the FlowRegistrationSnapshot for the given registration id, or None.

    Used by the route layer to cross-check that a target catalog file belongs to
    the flow being saved.  Callers typically pass
    ``flow.flow_settings.source_registration_id`` (avoiding an import cycle with
    the in-memory flow handler).
    """
    if rid is None:
        return None
    with get_db_context() as db:
        service = CatalogService(SQLAlchemyCatalogRepository(db))
        return _snapshot(service.repo.get_flow(rid))


def resolve_source_registration_id(flow) -> None:
    """Resolve and set source_registration_id on a flow from the catalog registration.

    Looks up the flow_registrations table by flow_path and stamps the
    registration ID onto the in-memory flow settings so it is available
    for run tracking and kernel nodes without needing to re-resolve later.
    """
    if getattr(flow.flow_settings, "source_registration_id", None) is not None:
        return
    flow_path = flow.flow_settings.path or flow.flow_settings.save_location
    if not flow_path:
        return
    try:
        with get_db_context() as db:
            service = CatalogService(SQLAlchemyCatalogRepository(db))
            reg_id = service.resolve_registration_id(flow_path)
            if reg_id is not None:
                try:
                    flow.flow_settings.source_registration_id = reg_id
                except (AttributeError, ValueError):
                    object.__setattr__(flow.flow_settings, "source_registration_id", reg_id)
    except Exception:
        logger.info(f"Could not resolve source_registration_id for '{flow_path}' (non-critical)", exc_info=True)
