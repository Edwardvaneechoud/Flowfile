"""Catalog helper functions for FlowFrame operations.

This module provides functions for reading from and writing to the Flowfile
catalog, similar to how database/frame_helpers.py handles database operations.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import TYPE_CHECKING

from flowfile_frame.catalog_reference import WriteMode, _resolve_namespace_id

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_graph import FlowGraph
    from flowfile_frame.catalog_reference import SchemaReference
    from flowfile_frame.flow_frame import FlowFrame


_MANAGED_FLOW_STEM_RE = re.compile(r"^[A-Za-z0-9_\-]+$")


def get_current_user_id() -> int:
    """Get the current user ID for catalog operations.

    Returns:
        int: The current user ID (defaults to 1 for single-user mode).
    """
    return 1


def add_write_to_catalog(
    flow_graph: FlowGraph,
    depends_on_node_id: int,
    *,
    table_name: str,
    schema: SchemaReference | None = None,
    namespace_id: int | None = None,
    write_mode: str = "overwrite",
    merge_keys: list[str] | None = None,
    description: str | None = None,
) -> int:
    """Add a catalog writer node to the flow graph.

    Args:
        flow_graph: The flow graph to add the node to.
        depends_on_node_id: The node ID that this writer depends on.
        table_name: Name of the catalog table to write to.
        schema: Target :class:`SchemaReference`. Preferred over ``namespace_id``.
        namespace_id: Legacy. Raw namespace id; mutually exclusive with ``schema``.
        write_mode: How to handle existing data.
        merge_keys: Column names for merge operations.
        description: Optional description for the node.

    Returns:
        int: The node ID of the created catalog writer node.
    """
    from flowfile_core.schemas import input_schema
    from flowfile_frame.utils import generate_node_id

    resolved_namespace_id = _resolve_namespace_id(schema, namespace_id)
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
            namespace_id=resolved_namespace_id,
            write_mode=write_mode,
            merge_keys=merge_keys or [],
        ),
    )

    flow_graph.add_catalog_writer(settings)
    return node_id


def read_catalog_table(
    table_name: str,
    *,
    schema: SchemaReference | None = None,
    namespace_id: int | None = None,
    delta_version: int | None = None,
    flow_graph: FlowGraph | None = None,
) -> FlowFrame:
    """Read a table from the Flowfile catalog.

    Resolves the table by name (and optionally schema) via the catalog
    service, then creates a catalog reader node in the flow graph.

    Args:
        table_name: Name of the catalog table to read.
        schema: Target :class:`SchemaReference`. Preferred over ``namespace_id``.
        namespace_id: Legacy. Raw namespace id; mutually exclusive with ``schema``.
        delta_version: Optional Delta version to read (for time-travel queries).
        flow_graph: Optional existing FlowGraph to add the node to.

    Returns:
        FlowFrame: A FlowFrame backed by a catalog reader node.

    Raises:
        ValueError: If both ``schema`` and ``namespace_id`` are provided, or
            if the table cannot be found.
    """
    from flowfile_core.schemas import input_schema
    from flowfile_frame.flow_frame import FlowFrame
    from flowfile_frame.utils import create_flow_graph, generate_node_id

    resolved_namespace_id = _resolve_namespace_id(schema, namespace_id)
    node_id = generate_node_id()

    if flow_graph is None:
        flow_graph = create_flow_graph()

    flow_id = flow_graph.flow_id
    settings = input_schema.NodeCatalogReader(
        flow_id=flow_id,
        node_id=node_id,
        user_id=get_current_user_id(),
        catalog_table_name=table_name,
        catalog_namespace_id=resolved_namespace_id,
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
    flow_graph: FlowGraph | None = None,
) -> FlowFrame:
    """Execute a SQL query against all catalog Delta tables.

    Registers every Delta table in the catalog into a Polars SQLContext
    (by table name) and executes the given SQL query.

    Args:
        sql_query: SQL query string to execute.
        flow_graph: Optional existing FlowGraph to add the node to.

    Returns:
        FlowFrame: A FlowFrame backed by a catalog SQL reader node.

    Raises:
        ValueError: If no Delta catalog tables are available.
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
    table_name: str,
    *,
    schema: SchemaReference | None = None,
    namespace_id: int | None = None,
    write_mode: WriteMode = "overwrite",
    merge_keys: list[str] | None = None,
    description: str | None = None,
) -> None:
    """Write a LazyFrame to the Flowfile catalog as a Delta table.

    Args:
        df: The FlowFrame to write.
        table_name: Name of the catalog table to write to.
        schema: Target :class:`SchemaReference`. Preferred over ``namespace_id``.
        namespace_id: Legacy. Raw namespace id; mutually exclusive with ``schema``.
        write_mode: How to handle existing data:
            - 'overwrite': Replace the entire table
            - 'error': Fail if the table already exists
            - 'append': Add rows to the existing table
            - 'upsert': Insert new rows or update existing by merge_keys
            - 'update': Update only existing rows by merge_keys
            - 'delete': Delete rows matching merge_keys
            - 'virtual': Register the flow as a virtual table without materializing.
              Requires the flow to be catalog-registered first; call
              :func:`save_flow_to_catalog` (or :meth:`FlowFrame.save_to_catalog`).
        merge_keys: Column names to use as merge keys (required for upsert/update/delete).
        description: Optional description for the table.

    Raises:
        ValueError: If both ``schema`` and ``namespace_id`` are provided, or
            if merge_keys are required but not provided.
    """
    df.write_catalog_table(
        table_name=table_name,
        schema=schema,
        namespace_id=namespace_id,
        write_mode=write_mode,
        merge_keys=merge_keys,
        description=description,
    )


def _resolve_python_flows_schema_id() -> int:
    """Resolve (and create if needed) the ``General > Python Flows`` namespace id."""
    from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
    from flowfile_core.database.connection import get_db_context

    with get_db_context() as db:
        service = CatalogService(SQLAlchemyCatalogRepository(db))
        ns = service.ensure_python_flows_namespace()
        if ns is None:
            raise RuntimeError(
                "Could not resolve or create the 'General > Python Flows' namespace. "
                "Pass an explicit schema= argument."
            )
        return ns.id


def save_flow_to_catalog(
    flow_or_frame: FlowFrame | FlowGraph,
    name: str,
    *,
    schema: SchemaReference | None = None,
    description: str | None = None,
) -> int:
    """Save a flow to disk and register it in the catalog.

    This is the Python equivalent of the canvas "Save to Catalog" action: it
    writes the flow YAML to the managed flows directory and creates a
    ``FlowRegistration`` row, stamping ``source_registration_id`` onto the
    in-memory flow settings. After this call, ``write_catalog_table(...,
    write_mode="virtual")`` works against the same flow.

    Args:
        flow_or_frame: A :class:`FlowFrame` or :class:`FlowGraph` to register.
        name: Display name and filename stem (alphanumeric, underscores, and
            hyphens only). The on-disk filename is ``{flow_id}_{name}.yaml``.
        schema: Target :class:`SchemaReference`. Defaults to
            ``General > Python Flows`` (auto-created on first use).
        description: Unused for now; reserved for future catalog metadata.

    Returns:
        int: The ``source_registration_id`` of the registered flow.

    Raises:
        ValueError: If ``name`` contains invalid characters or the registered
            target collides with another flow.

    Note:
        Virtual writes serialize the flow's LazyFrame to bytes. User code
        containing un-picklable closures (e.g. ``map_batches(lambda …)``)
        falls back to non-optimized virtual mode.
    """
    from flowfile_core.flowfile.catalog_helpers import (
        FlowNameNamespaceCollision,
        FlowPathNamespaceCollision,
        find_registration_by_name,
        find_registration_by_path,
        register_flow_in_namespace,
        resolve_source_registration_id,
    )
    from flowfile_frame.flow_frame import FlowFrame
    from shared.storage_config import storage

    flow_graph = flow_or_frame.flow_graph if isinstance(flow_or_frame, FlowFrame) else flow_or_frame

    stem = name.strip() if name else ""
    if not stem:
        raise ValueError("name must not be empty")
    if "/" in stem or "\\" in stem or ".." in stem:
        raise ValueError(f"invalid flow name: {name!r}")
    if not _MANAGED_FLOW_STEM_RE.fullmatch(stem):
        raise ValueError(f"invalid flow name: {name!r}. Only letters, digits, underscores, and hyphens are allowed.")

    schema_id = schema.id if schema is not None else _resolve_python_flows_schema_id()

    flows_dir = Path(storage.flows_directory)
    flows_dir.mkdir(parents=True, exist_ok=True)
    base_path = os.path.normpath(str(flows_dir.resolve()))
    target = os.path.normpath(os.path.join(base_path, f"{flow_graph.flow_id}_{stem}.yaml"))
    if not target.startswith(base_path + os.sep):
        raise ValueError(f"resolved flow path escapes flows directory: {target}")

    user_id = get_current_user_id()
    source_registration_id = getattr(flow_graph._flow_settings, "source_registration_id", None)

    name_clash = find_registration_by_name(stem, schema_id)
    if name_clash is not None and name_clash.id != source_registration_id:
        raise ValueError(
            f"A flow named {stem!r} already exists in this namespace. "
            "Choose a different name, or open the existing flow from the catalog."
        )

    existing_reg = find_registration_by_path(target)
    if existing_reg is not None and existing_reg.id != source_registration_id:
        raise ValueError(f"target file {target} is already registered to another flow")
    if existing_reg is None and os.path.exists(target):
        raise ValueError(f"target file {target} exists but is not catalog-registered; refusing to overwrite")

    flow_graph.save_flow(target)
    try:
        register_flow_in_namespace(target, stem, user_id, schema_id)
    except (FlowPathNamespaceCollision, FlowNameNamespaceCollision) as err:
        raise ValueError(str(err)) from err
    resolve_source_registration_id(flow_graph)

    reg_id = getattr(flow_graph._flow_settings, "source_registration_id", None)
    if reg_id is None:
        raise RuntimeError(
            f"flow saved to {target} but registration could not be resolved. "
            "This usually indicates a database error; check logs."
        )
    return reg_id
