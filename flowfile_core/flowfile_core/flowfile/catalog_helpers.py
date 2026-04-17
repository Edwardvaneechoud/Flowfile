"""Catalog registration helpers for flow persistence.

These functions bridge the in-memory flow system with the catalog database.
They are intentionally kept as standalone functions (not on FlowfileHandler)
to avoid coupling the handler to the database layer.
"""

import logging
from pathlib import Path

from fastapi import HTTPException

from flowfile_core.catalog import CatalogService
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import FlowRegistration

logger = logging.getLogger(__name__)


class FlowPathNamespaceCollision(Exception):
    """Raised when a flow_path already exists under a different namespace."""


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
    ``HTTPException(409)`` — normal callers prefix filenames with the flow id so
    this signals a real bug rather than user error.
    """
    if namespace_id is None:
        auto_register_flow(flow_path, name, user_id)
        return
    if user_id is None or flow_path is None:
        return
    try:
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
            reg = FlowRegistration(
                name=name or Path(flow_path).stem,
                flow_path=flow_path,
                namespace_id=namespace_id,
                owner_id=user_id,
            )
            service.repo.create_flow(reg)
    except FlowPathNamespaceCollision as err:
        raise HTTPException(status_code=409, detail=str(err)) from err
    except Exception:
        logger.info(
            f"Registration in namespace {namespace_id} failed for '{flow_path}' (non-critical)",
            exc_info=True,
        )


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
