"""Catalog registration helpers for flow persistence.

These functions bridge the in-memory flow system with the catalog database.
They are intentionally kept as standalone functions (not on FlowfileHandler)
to avoid coupling the handler to the database layer.
"""

import logging
import re
from dataclasses import dataclass
from pathlib import Path

from flowfile_core.catalog import CatalogService
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import FlowApiEndpoint, FlowRegistration
from shared.storage_config import storage

logger = logging.getLogger(__name__)


_SAFE_FILENAME_RE = re.compile(r"[^A-Za-z0-9._-]+")


def _safe_filename_stem(name: str) -> str:
    """Slugify ``name`` for use as a flow filename stem."""
    cleaned = _SAFE_FILENAME_RE.sub("_", name).strip("._-")
    return cleaned or "flow"


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


def sync_api_compatibility(flow) -> None:
    """Refresh the flow's catalog ``is_api_compatible`` flag from its in-memory graph.

    A flow is API-compatible when it has exactly one ``api_response`` node. No-op if
    the flow isn't registered yet. Best-effort: failures are logged, never raised, so
    they cannot break a save.
    """
    if flow is None:
        return
    flow_path = getattr(flow.flow_settings, "path", None) or getattr(flow.flow_settings, "save_location", None)
    if not flow_path:
        return
    is_compatible = sum(1 for n in flow.nodes if n.node_type == "api_response") == 1
    try:
        with get_db_context() as db:
            reg = SQLAlchemyCatalogRepository(db).get_flow_by_path(flow_path)
            if reg is None:
                return
            changed = False
            if bool(reg.is_api_compatible) != is_compatible:
                reg.is_api_compatible = is_compatible
                changed = True
            # A flow that just lost (or duplicated) its api_response node can no
            # longer serve requests. Disable any already-published endpoint so the
            # public route returns a clean 403 instead of 500ing on an invalid graph.
            if not is_compatible:
                disabled = (
                    db.query(FlowApiEndpoint)
                    .filter(FlowApiEndpoint.registration_id == reg.id, FlowApiEndpoint.enabled.is_(True))
                    .update({"enabled": False}, synchronize_session=False)
                )
                changed = changed or bool(disabled)
            if changed:
                db.commit()
    except Exception:
        logger.info(f"Failed to sync API compatibility for '{flow_path}' (non-critical)", exc_info=True)


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


def register_python_editor_flow(
    flow,
    *,
    name: str | None = None,
    namespace_id: int | None = None,
    flow_path: str | None = None,
    user_id: int | None = None,
) -> int:
    """Save and register a Python-authored flow with the catalog.

    Mirrors what the canvas does for in-designer flows: persists the flow as
    YAML on disk, creates a ``FlowRegistration`` row, and stamps the
    resulting ``source_registration_id`` back onto the in-memory flow so
    downstream operations (notably ``write_mode='virtual'`` catalog writes)
    can succeed.

    By default the flow lands under
    ``~/.flowfile/flows/python_editor_flows/<flow_id>_<name>.yaml`` and is
    registered under the seeded ``General > Python Editor`` namespace.

    Args:
        flow: The :class:`FlowGraph` to register.
        name: Display name for the registration. Defaults to
            ``flow.flow_settings.name``.
        namespace_id: Optional explicit namespace. Defaults to
            ``General > Python Editor``.
        flow_path: Optional explicit YAML save path. When omitted, a path is
            chosen under :attr:`FlowfileStorage.python_editor_flows_directory`.
        user_id: Owner id for the registration. Defaults to ``1``
            (single-user mode).

    Returns:
        The ``id`` of the resulting ``FlowRegistration`` row.

    Raises:
        ValueError: If ``flow`` does not look like a ``FlowGraph``.
    """
    if not hasattr(flow, "save_flow") or not hasattr(flow, "flow_settings"):
        raise ValueError("register_python_editor_flow requires a FlowGraph instance")

    owner_id = user_id if user_id is not None else 1
    display_name = name or getattr(flow.flow_settings, "name", None) or f"Flow_{flow.flow_id}"

    if flow_path is None:
        flow_path = getattr(flow.flow_settings, "path", None) or getattr(flow.flow_settings, "save_location", None)
    if not flow_path:
        stem = _safe_filename_stem(f"{flow.flow_id}_{display_name}")
        flow_path = str(storage.python_editor_flows_directory / f"{stem}.yaml")

    flow.save_flow(flow_path)

    with get_db_context() as db:
        service = CatalogService(SQLAlchemyCatalogRepository(db))

        target_ns_id = namespace_id
        if target_ns_id is None:
            ns = service.ensure_python_editor_flows_namespace()
            if ns is None:
                # Fall back to default namespace if the seeding helper can't
                # create the Python Editor bucket (e.g. 'General' missing).
                target_ns_id = service.get_default_namespace_id()
            else:
                target_ns_id = ns.id

        existing = service.repo.get_flow_by_path(flow_path)
        if existing is not None:
            reg_id = existing.id
        else:
            reg = service.register_flow(
                name=display_name,
                flow_path=flow_path,
                owner_id=owner_id,
                namespace_id=target_ns_id,
            )
            reg_id = reg.id

    flow.flow_settings.source_registration_id = reg_id

    return reg_id


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
