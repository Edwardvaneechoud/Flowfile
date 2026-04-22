"""Catalog helper functions for FlowFrame operations.

This module provides functions for reading from and writing to the Flowfile
catalog, similar to how database/frame_helpers.py handles database operations.
"""

from __future__ import annotations

import ast
import os
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal, NamedTuple

from flowfile_frame.config import logger

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_graph import FlowGraph
    from flowfile_frame.flow_frame import FlowFrame


WriteMode = Literal["overwrite", "error", "append", "upsert", "update", "delete", "virtual"]

_UNHELPFUL_NAMES = frozenset({"df", "_", "tmp", "x", "y", "z", "frame", "result", "out"})


def get_current_user_id() -> int:
    """Get the current user ID for catalog operations.

    Returns:
        int: The current user ID (defaults to 1 for single-user mode).
    """
    env_user = os.environ.get("FLOWFILE_USER_ID")
    if env_user:
        try:
            return int(env_user)
        except ValueError:
            pass
    return 1


# ---------------------------------------------------------------------------
# Flow binding: decides which FlowRegistration a script's catalog writes
# roll up under. Priority (first wins):
#   1. catalog_context(name=...) ContextVar override
#   2. FLOWFILE_FLOW_NAME env var
#   3. Receiver var name inferred from the first catalog-write call site
#   4. __main__.__file__ / sys.argv[0]
#   5. Synthetic fallback
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FlowBinding:
    """How a standalone flowfile_frame script identifies itself to the catalog."""

    name: str
    flow_path: str
    user_id: int = 1


_flow_binding_override: ContextVar[FlowBinding | None] = ContextVar("_flow_binding_override", default=None)


@contextmanager
def catalog_context(
    *,
    name: str | None = None,
    flow_path: str | None = None,
    user_id: int | None = None,
) -> Iterator[FlowBinding]:
    """Override flow-registration parameters for all catalog writes in this block.

    Any catalog write inside the ``with`` block will auto-register under the given
    name/flow_path instead of the default (script filename / inferred var name).
    """
    resolved_name = name or _derive_default_name(inferred_name=None)
    resolved_path = flow_path or _derive_default_path(resolved_name)
    resolved_user = user_id if user_id is not None else get_current_user_id()
    binding = FlowBinding(name=resolved_name, flow_path=resolved_path, user_id=resolved_user)
    token = _flow_binding_override.set(binding)
    try:
        yield binding
    finally:
        _flow_binding_override.reset(token)


def _derive_default_name(inferred_name: str | None) -> str:
    env_name = os.environ.get("FLOWFILE_FLOW_NAME")
    if env_name:
        return env_name
    if inferred_name:
        return inferred_name
    entry = _entry_script_path()
    if entry:
        return Path(entry).stem
    return f"flowframe_script_{os.getpid()}"


def _derive_default_path(name: str) -> str:
    env_path = os.environ.get("FLOWFILE_FLOW_PATH")
    if env_path:
        return env_path
    entry = _entry_script_path()
    if entry:
        return entry
    return f"<flowframe>/{name}"


def _entry_script_path() -> str | None:
    """Resolve the entry-point script path, or None if running in a REPL/Jupyter."""
    main_mod = sys.modules.get("__main__")
    main_file = getattr(main_mod, "__file__", None)
    if main_file and not main_file.endswith("ipykernel_launcher.py"):
        return main_file
    argv0 = sys.argv[0] if sys.argv else ""
    if argv0 and argv0 != "-c" and not argv0.endswith("ipykernel_launcher.py"):
        return argv0
    return None


def _derive_flow_binding(inferred_name: str | None = None) -> FlowBinding:
    """Resolve the effective FlowBinding for the current call."""
    override = _flow_binding_override.get()
    if override is not None:
        return override
    name = _derive_default_name(inferred_name)
    path = _derive_default_path(name)
    return FlowBinding(name=name, flow_path=path, user_id=get_current_user_id())


def _ensure_flow_registered(
    flow_graph: FlowGraph,
    inferred_name: str | None = None,
) -> int | None:
    """Auto-register this flow (idempotent) and stamp source_registration_id on flow_graph.

    Returns the registration id, or None if registration failed (non-fatal).
    Only consults ``inferred_name`` on the very first write (when the flow_graph
    has no name yet) so subsequent writes don't rename the flow.
    """
    if flow_graph.flow_settings.source_registration_id is not None:
        return flow_graph.flow_settings.source_registration_id

    from flowfile_core.flowfile.catalog_helpers import (
        auto_register_flow,
        find_registration_by_path,
    )

    binding = _derive_flow_binding(inferred_name)

    try:
        auto_register_flow(binding.flow_path, binding.name, binding.user_id)
        snap = find_registration_by_path(binding.flow_path)
    except Exception:
        logger.info("Auto-registration of flowfile_frame script failed (non-critical)", exc_info=True)
        return None

    if snap is None:
        return None

    flow_graph.flow_settings.source_registration_id = snap.id
    # Stamp the binding name/path onto the graph so execute() reports correctly,
    # overriding any generic "Flow_{id}" default from create_flow_graph.
    flow_graph.flow_settings.name = binding.name
    flow_graph.flow_settings.path = binding.flow_path
    return snap.id


# ---------------------------------------------------------------------------
# Var-name inference via the `executing` library
# ---------------------------------------------------------------------------


def _infer_receiver_name(skip_frames: int = 2) -> str | None:
    """Return the var name of the method-call receiver at the caller's call site, or None.

    For example, ``customers.write_catalog_table()`` yields ``"customers"``. Returns
    None for chained calls, REPL/exec contexts where introspection fails, or if the
    ``executing`` library is unavailable.
    """
    try:
        import executing
    except ImportError:
        return None

    try:
        frame = sys._getframe(skip_frames)
    except ValueError:
        return None

    try:
        node = executing.Source.executing(frame).node
    except Exception:
        return None
    if node is None:
        return None

    # For `receiver.method(...)`, the Call node's func is an Attribute whose
    # .value is the receiver expression. We only accept plain Name receivers.
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        recv = node.func.value
        if isinstance(recv, ast.Name):
            return recv.id
    return None


def _accept_inferred_name(name: str | None) -> str | None:
    """Filter out unhelpful inferred names (too-short, blocklisted)."""
    if not name:
        return None
    if len(name) <= 2 or name.lower() in _UNHELPFUL_NAMES:
        return None
    return name


def _resolve_table_name(explicit: str | None, inferred: str | None) -> str:
    if explicit:
        return explicit
    accepted = _accept_inferred_name(inferred)
    if accepted:
        return accepted
    detail = f" (inferred '{inferred}' is too generic)" if inferred else ""
    raise ValueError("Could not determine a catalog table name" + detail + " — please pass table_name=... explicitly.")


# ---------------------------------------------------------------------------
# Namespace management
# ---------------------------------------------------------------------------


class NamespaceInfo(NamedTuple):
    id: int
    name: str
    parent_id: int | None
    level: int
    description: str | None


def _ns_info(ns) -> NamespaceInfo:
    return NamespaceInfo(
        id=ns.id,
        name=ns.name,
        parent_id=ns.parent_id,
        level=ns.level,
        description=ns.description,
    )


def create_namespace(
    name: str,
    *,
    parent: str | int | None = None,
    description: str | None = None,
    user_id: int | None = None,
) -> int:
    """Create a catalog namespace (level 0) or schema (level 1).

    Args:
        name: Namespace name. Must not contain '.'.
        parent: Parent namespace id or name. If given, creates a schema under it.
        description: Optional description.
        user_id: Owner user id (defaults to current user).

    Returns:
        The newly created namespace's id.
    """
    from flowfile_core.catalog import CatalogService
    from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
    from flowfile_core.database.connection import get_db_context

    parent_id = _resolve_namespace(parent).id if parent is not None else None
    owner = user_id if user_id is not None else get_current_user_id()
    with get_db_context() as db:
        svc = CatalogService(SQLAlchemyCatalogRepository(db))
        ns = svc.create_namespace(name=name, owner_id=owner, parent_id=parent_id, description=description)
        return ns.id


def get_namespace(name_or_path: str | int) -> NamespaceInfo:
    """Resolve a namespace by id, bare name, or qualified 'catalog.schema' path."""
    return _ns_info(_resolve_namespace(name_or_path))


def list_namespaces(*, parent: str | int | None = None) -> list[NamespaceInfo]:
    """List namespaces, optionally filtered by parent name or id."""
    from flowfile_core.catalog import CatalogService
    from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
    from flowfile_core.database.connection import get_db_context

    parent_id = _resolve_namespace(parent).id if parent is not None else None
    with get_db_context() as db:
        svc = CatalogService(SQLAlchemyCatalogRepository(db))
        return [_ns_info(ns) for ns in svc.list_namespaces(parent_id)]


class TableInfo(NamedTuple):
    id: int
    name: str
    namespace_name: str | None
    full_table_name: str | None
    table_type: str
    row_count: int | None
    source_registration_name: str | None


def _table_info(t) -> TableInfo:
    return TableInfo(
        id=t.id,
        name=t.name,
        namespace_name=t.namespace_name,
        full_table_name=t.full_table_name,
        table_type=t.table_type,
        row_count=t.row_count,
        source_registration_name=t.source_registration_name,
    )


def list_tables(*, schema: str | int | None = None) -> list[TableInfo]:
    """List catalog tables, optionally filtered by schema.

    Args:
        schema: Schema name ('stg'), qualified path ('warehouse.stg'), or id.
            When None, lists tables across all schemas.

    Returns:
        A list of TableInfo NamedTuples (id, name, namespace_name, full_table_name,
        table_type, row_count, source_registration_name).
    """
    from flowfile_core.catalog import CatalogService
    from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
    from flowfile_core.database.connection import get_db_context

    schema_id = _resolve_namespace(schema).id if schema is not None else None
    with get_db_context() as db:
        svc = CatalogService(SQLAlchemyCatalogRepository(db))
        return [_table_info(t) for t in svc.list_tables(namespace_id=schema_id)]


def list_catalogs() -> list[NamespaceInfo]:
    """List all catalogs (level-0 namespaces)."""
    return list_namespaces(parent=None)


def list_schemas(*, catalog: str | int | None = None) -> list[NamespaceInfo]:
    """List schemas (level-1 namespaces), optionally scoped to one catalog.

    Args:
        catalog: Catalog name or id. When None, lists schemas across all catalogs.
    """
    if catalog is not None:
        return list_namespaces(parent=catalog)
    all_schemas: list[NamespaceInfo] = []
    for cat in list_catalogs():
        all_schemas.extend(list_namespaces(parent=cat.id))
    return all_schemas


def create_catalog(name: str, *, description: str | None = None, user_id: int | None = None) -> int:
    """Create a catalog (level-0 namespace). Returns the new catalog id."""
    return create_namespace(name, description=description, user_id=user_id)


def create_schema(
    name: str,
    *,
    catalog: str | int,
    description: str | None = None,
    user_id: int | None = None,
) -> int:
    """Create a schema (level-1 namespace) under ``catalog``. Returns the new schema id."""
    return create_namespace(name, parent=catalog, description=description, user_id=user_id)


def get_schema(name_or_path: str | int) -> NamespaceInfo:
    """Resolve a schema by id, bare name, or qualified 'catalog.schema' path."""
    return get_namespace(name_or_path)


def _resolve_namespace(name_or_id: str | int):
    """Resolve a namespace reference to the underlying ORM object.

    Accepts an int id, a bare name (unique match required), or a 'catalog.schema'
    qualified path. Raises ValueError on no/ambiguous match.
    """
    from flowfile_core.catalog import CatalogService
    from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
    from flowfile_core.database.connection import get_db_context

    if isinstance(name_or_id, int):
        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            ns = svc.repo.get_namespace(name_or_id)
            if ns is None:
                raise ValueError(f"Namespace id {name_or_id} not found")
            return ns

    if not isinstance(name_or_id, str) or not name_or_id:
        raise ValueError(f"Invalid namespace reference: {name_or_id!r}")

    with get_db_context() as db:
        svc = CatalogService(SQLAlchemyCatalogRepository(db))
        if "." in name_or_id:
            catalog_name, schema_name = name_or_id.split(".", 1)
            root = svc.repo.get_namespace_by_name(catalog_name, None)
            if root is None:
                raise ValueError(f"Catalog '{catalog_name}' not found")
            child = svc.repo.get_namespace_by_name(schema_name, root.id)
            if child is None:
                raise ValueError(f"Schema '{schema_name}' not found under '{catalog_name}'")
            return child

        # Bare name: try root-level first, then search children for a unique match.
        root = svc.repo.get_namespace_by_name(name_or_id, None)
        if root is not None:
            return root
        matches = [ns for ns in svc.list_namespaces(None)]
        nested = []
        for cat in matches:
            child = svc.repo.get_namespace_by_name(name_or_id, cat.id)
            if child is not None:
                nested.append(child)
        if len(nested) == 1:
            return nested[0]
        if not nested:
            raise ValueError(f"Namespace '{name_or_id}' not found")
        raise ValueError(f"Namespace name '{name_or_id}' is ambiguous; use qualified 'catalog.schema' form")


def _resolve_namespace_arg(
    namespace: str | int | None,
    namespace_id: int | None,
    schema: str | int | None = None,
) -> int | None:
    """Collapse (schema, namespace, namespace_id) into a single int id.

    ``schema`` is the preferred kwarg (level-1 semantics). ``namespace`` is an
    alias kept for the original API surface. ``namespace_id`` is the legacy int form.
    Precedence: schema > namespace > namespace_id.
    """
    if schema is not None:
        return _resolve_namespace(schema).id
    if namespace is not None:
        return _resolve_namespace(namespace).id
    return namespace_id


# ---------------------------------------------------------------------------
# Catalog I/O (existing public API, extended)
# ---------------------------------------------------------------------------


def add_write_to_catalog(
    flow_graph: FlowGraph,
    depends_on_node_id: int,
    *,
    table_name: str,
    schema: str | int | None = None,
    namespace: str | int | None = None,
    namespace_id: int | None = None,
    write_mode: WriteMode = "overwrite",
    merge_keys: list[str] | None = None,
    description: str | None = None,
    inferred_flow_name: str | None = None,
) -> int:
    """Add a catalog writer node to the flow graph.

    Auto-registers the flow on first catalog write so ``source_registration_id``
    is set before the writer runs (required for virtual writes, needed for
    lineage on physical writes).
    """
    from flowfile_core.schemas import input_schema
    from flowfile_frame.utils import generate_node_id

    resolved_ns_id = _resolve_namespace_arg(namespace, namespace_id, schema)
    _ensure_flow_registered(flow_graph, inferred_name=inferred_flow_name)

    node_id = generate_node_id()
    flow_id = flow_graph.flow_id

    settings = input_schema.NodeCatalogWriter(
        flow_id=flow_id,
        node_id=node_id,
        user_id=get_current_user_id(),
        depending_on_id=depends_on_node_id,
        description=description,
        catalog_write_settings=input_schema.CatalogWriteSettings(
            table_name=table_name,
            namespace_id=resolved_ns_id,
            write_mode=write_mode,
            merge_keys=merge_keys or [],
        ),
    )

    flow_graph.add_catalog_writer(settings)
    return node_id


def read_catalog_table(
    table_name: str,
    *,
    schema: str | int | None = None,
    namespace: str | int | None = None,
    namespace_id: int | None = None,
    delta_version: int | None = None,
    flow_graph=None,
) -> FlowFrame:
    """Read a table from the Flowfile catalog.

    Resolves the table by name (and optionally schema) via the catalog
    service, then creates a catalog reader node in the flow graph.

    Args:
        table_name: Name of the catalog table to read.
        schema: Schema name ('stg') or qualified path ('warehouse.stg'), or id.
        namespace: Alias for ``schema`` (kept for the original API surface).
        namespace_id: Deprecated int form (kept for backwards compat).
        delta_version: Optional Delta version to read (for time-travel queries).
        flow_graph: Optional existing FlowGraph to add the node to.

    Returns:
        FlowFrame: A FlowFrame backed by a catalog reader node.
    """
    from flowfile_core.schemas import input_schema
    from flowfile_frame.flow_frame import FlowFrame
    from flowfile_frame.utils import create_flow_graph, generate_node_id

    node_id = generate_node_id()

    if flow_graph is None:
        flow_graph = create_flow_graph()

    resolved_ns_id = _resolve_namespace_arg(namespace, namespace_id, schema)
    flow_id = flow_graph.flow_id
    settings = input_schema.NodeCatalogReader(
        flow_id=flow_id,
        node_id=node_id,
        user_id=get_current_user_id(),
        catalog_table_name=table_name,
        catalog_namespace_id=resolved_ns_id,
        delta_version=delta_version,
    )
    flow_graph.add_catalog_reader(settings)
    return FlowFrame(
        data=flow_graph.get_node(node_id).get_resulting_data().data_frame,
        flow_graph=flow_graph,
        node_id=node_id,
    )


def read_catalog_sql(
    sql_query: str,
    *,
    flow_graph=None,
) -> FlowFrame:
    """Execute a SQL query against all catalog Delta tables.

    Registers every Delta table in the catalog into a Polars SQLContext
    (by table name) and executes the given SQL query.
    """
    from flowfile_core.schemas import input_schema
    from flowfile_frame.flow_frame import FlowFrame
    from flowfile_frame.utils import create_flow_graph, generate_node_id

    node_id = generate_node_id()

    if flow_graph is None:
        flow_graph = create_flow_graph()

    flow_id = flow_graph.flow_id
    settings = input_schema.NodeCatalogReader(
        flow_id=flow_id,
        node_id=node_id,
        user_id=get_current_user_id(),
        sql_query=sql_query,
    )
    flow_graph.add_catalog_reader(settings)
    return FlowFrame(
        data=flow_graph.get_node(node_id).get_resulting_data().data_frame,
        flow_graph=flow_graph,
        node_id=node_id,
    )


def write_catalog_table(
    df: FlowFrame,
    table_name: str | None = None,
    *,
    schema: str | int | None = None,
    namespace: str | int | None = None,
    namespace_id: int | None = None,
    write_mode: WriteMode = "overwrite",
    merge_keys: list[str] | None = None,
    description: str | None = None,
) -> None:
    """Write a FlowFrame to the Flowfile catalog.

    If ``table_name`` is omitted, the receiver variable name at the call site is
    used (e.g. ``write_catalog_table(customers)`` → table ``"customers"``). That
    same inferred name seeds the flow's auto-registration name.

    Args:
        df: The FlowFrame to write.
        table_name: Name of the catalog table. When None, inferred from the argument
            variable name at the call site.
        schema: Schema name ('stg') or qualified path ('warehouse.stg'), or id.
        namespace: Alias for ``schema`` (kept for the original API surface).
        namespace_id: Deprecated int form (kept for backwards compat).
        write_mode: How to handle existing data:
            - 'overwrite' / 'error' / 'append'
            - 'upsert' / 'update' / 'delete' (require merge_keys)
            - 'virtual' (register a non-materialized virtual table)
        merge_keys: Column names for merge operations.
        description: Optional description for the table.
    """
    inferred = _infer_receiver_name(skip_frames=2)
    # For module-level ``write_catalog_table(customers)`` the receiver of this call
    # is the module, but the first positional argument is the frame. Try the arg name.
    if inferred is None:
        inferred = _infer_first_arg_name(skip_frames=2)
    resolved_table = _resolve_table_name(table_name, inferred)
    df.write_catalog_table(
        table_name=resolved_table,
        schema=schema,
        namespace=namespace,
        namespace_id=namespace_id,
        write_mode=write_mode,
        merge_keys=merge_keys,
        description=description,
        _inferred_flow_name=inferred,
    )


def _infer_first_arg_name(skip_frames: int = 1) -> str | None:
    """Return the var name of the first positional argument at the caller's call site.

    For ``write_catalog_table(customers)`` yields ``"customers"``. Returns None for
    literals, expressions, or when ``executing`` is unavailable.
    """
    try:
        import executing
    except ImportError:
        return None
    try:
        frame = sys._getframe(skip_frames)
    except ValueError:
        return None
    try:
        node = executing.Source.executing(frame).node
    except Exception:
        return None
    if node is None or not isinstance(node, ast.Call) or not node.args:
        return None
    first = node.args[0]
    if isinstance(first, ast.Name):
        return first.id
    return None
