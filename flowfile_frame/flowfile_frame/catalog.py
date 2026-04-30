"""Catalog helper functions for FlowFrame operations.

This module provides functions for reading from and writing to the Flowfile
catalog, similar to how database/frame_helpers.py handles database operations.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from flowfile_frame.catalog_reference import WriteMode, _resolve_namespace_id

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_graph import FlowGraph
    from flowfile_frame.catalog_reference import SchemaReference
    from flowfile_frame.flow_frame import FlowFrame


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
