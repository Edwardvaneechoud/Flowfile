"""Catalog helper functions for FlowFrame operations.

This module provides functions for reading from and writing to the Flowfile
catalog, similar to how database/frame_helpers.py handles database operations.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Literal

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
    namespace_full_name: str | None = None,
    write_mode: str = "overwrite",
    merge_keys: list[str] | None = None,
    partition_by: list[str] | None = None,
    scd2_compare_columns: list[str] | None = None,
    scd2_full_snapshot: bool = False,
    scd2_surrogate_key_column: str = "sk",
    scd2_valid_from_column: str = "valid_from",
    scd2_valid_to_column: str = "valid_to",
    scd2_is_current_column: str = "is_current",
    scd2_partition_on_current: bool = True,
    description: str | None = None,
) -> int:
    """Add a catalog writer node to the flow graph.

    Args:
        flow_graph: The flow graph to add the node to.
        depends_on_node_id: The node ID that this writer depends on.
        table_name: Name of the catalog table to write to.
        schema: Target :class:`SchemaReference`. Preferred over ``namespace_id``.
        namespace_id: Legacy. Raw namespace id; mutually exclusive with ``schema``.
        namespace_full_name: Portable ``"catalog.schema"`` name, resolved at run time.
            Survives recreation of the catalog on another machine, unlike ``namespace_id``.
        write_mode: How to handle existing data.
        merge_keys: Column names for merge operations. Also the SCD2 business key.
        partition_by: Delta partition columns (applied at table creation).
        scd2_compare_columns: Columns compared for change detection when ``write_mode="scd2"``.
            Empty means every non-key, non-system column.
        scd2_full_snapshot: When ``write_mode="scd2"``, end-date current rows whose business
            key is absent from this run's input.
        scd2_surrogate_key_column: Name of the generated surrogate-key column (``write_mode="scd2"``).
        scd2_valid_from_column: Name of the generated valid-from column (``write_mode="scd2"``).
        scd2_valid_to_column: Name of the generated valid-to column (``write_mode="scd2"``).
        scd2_is_current_column: Name of the generated is-current column (``write_mode="scd2"``).
        scd2_partition_on_current: Partition new SCD2 tables by the is-current column
            (creation-time only; explicit ``partition_by`` columns nest above it).
        description: Optional description for the node.

    Returns:
        int: The node ID of the created catalog writer node.
    """
    from flowfile_core.schemas import input_schema
    from flowfile_frame.utils import generate_node_id

    resolved_namespace_id = _resolve_namespace_id(schema, namespace_id)
    node_id = generate_node_id()
    flow_id = flow_graph.flow_id

    if write_mode != "scd2":
        ignored = [
            name
            for name, value, default in (
                ("scd2_compare_columns", scd2_compare_columns or [], []),
                ("scd2_full_snapshot", scd2_full_snapshot, False),
                ("scd2_partition_on_current", scd2_partition_on_current, True),
                ("scd2_surrogate_key_column", scd2_surrogate_key_column, "sk"),
                ("scd2_valid_from_column", scd2_valid_from_column, "valid_from"),
                ("scd2_valid_to_column", scd2_valid_to_column, "valid_to"),
                ("scd2_is_current_column", scd2_is_current_column, "is_current"),
            )
            if value != default
        ]
        if ignored:
            raise ValueError(
                f"{', '.join(ignored)} only apply when write_mode='scd2', but write_mode is '{write_mode}'"
            )

    scd2 = None
    if write_mode == "scd2":
        scd2 = input_schema.Scd2Settings(
            compare_columns=scd2_compare_columns or [],
            full_snapshot=scd2_full_snapshot,
            surrogate_key_column=scd2_surrogate_key_column,
            valid_from_column=scd2_valid_from_column,
            valid_to_column=scd2_valid_to_column,
            is_current_column=scd2_is_current_column,
            partition_on_current=scd2_partition_on_current,
        )

    settings = input_schema.NodeCatalogWriter(
        flow_id=flow_id,
        node_id=node_id,
        user_id=get_current_user_id(),
        depending_on_id=depends_on_node_id,
        description=description,
        catalog_write_settings=input_schema.CatalogWriteSettings(
            table_name=table_name,
            namespace_id=resolved_namespace_id,
            namespace_full_name=namespace_full_name,
            write_mode=write_mode,
            merge_keys=merge_keys or [],
            partition_by=partition_by or [],
            scd2=scd2,
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
    scd2_view: Literal["active", "all", "active_at"] | None = None,
    scd2_as_of: str | datetime | None = None,
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
        scd2_view: History view for an SCD2-tracked table: ``"active"`` (current rows
            only), ``"all"`` (every version), or ``"active_at"`` (rows valid at
            ``scd2_as_of``). ``None`` (the default) means no filter — every version is
            returned, and the setting is ignored for non-SCD2 tables.
        scd2_as_of: Required when ``scd2_view="active_at"``. Accepts an ISO-8601 string
            or a ``datetime`` (converted via ``.isoformat()``).
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
        scd2_view=scd2_view,
        scd2_as_of=scd2_as_of.isoformat() if isinstance(scd2_as_of, datetime) else scd2_as_of,
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


def register_flow_with_catalog(
    flow_or_frame,
    *,
    name: str | None = None,
    schema: SchemaReference | None = None,
    namespace_id: int | None = None,
    flow_path: str | None = None,
    user_id: int | None = None,
) -> int:
    """Register a Python-authored flow with the Flowfile catalog.

    Persists the flow as a YAML file (under
    ``~/.flowfile/flows/python_editor_flows/`` by default) and creates a
    ``FlowRegistration`` row, mirroring how the canvas registers flows. The
    resulting registration id is stamped onto the flow so subsequent calls
    such as :func:`write_catalog_table` with ``write_mode='virtual'`` can
    succeed.

    Calling this explicitly is optional: ``write_catalog_table`` with
    ``write_mode='virtual'`` will auto-register the flow under
    ``General > Python Editor`` on first use. Use this helper when you want
    a specific name, namespace, or save path.

    Args:
        flow_or_frame: A :class:`FlowFrame` or the underlying ``FlowGraph``.
        name: Display name for the catalog entry. Defaults to the flow's
            current name.
        schema: Target :class:`SchemaReference`. Preferred over
            ``namespace_id``.
        namespace_id: Legacy. Raw namespace id; mutually exclusive with
            ``schema``. Defaults to ``General > Python Editor``.
        flow_path: Optional explicit YAML save path. When omitted, a path is
            chosen under ``~/.flowfile/flows/python_editor_flows/``.
        user_id: Owner id for the registration (defaults to ``1``).

    Returns:
        The id of the resulting flow registration.
    """
    from flowfile_core.flowfile.catalog_helpers import register_python_editor_flow

    if hasattr(flow_or_frame, "flow_graph"):
        graph = flow_or_frame.flow_graph
    else:
        graph = flow_or_frame

    resolved_namespace_id = _resolve_namespace_id(schema, namespace_id)

    return register_python_editor_flow(
        graph,
        name=name,
        namespace_id=resolved_namespace_id,
        flow_path=flow_path,
        user_id=user_id,
    )


def write_catalog_table(
    df: FlowFrame,
    table_name: str,
    *,
    schema: SchemaReference | None = None,
    namespace_id: int | None = None,
    namespace_full_name: str | None = None,
    write_mode: WriteMode = "overwrite",
    merge_keys: list[str] | None = None,
    partition_by: list[str] | None = None,
    scd2_compare_columns: list[str] | None = None,
    scd2_full_snapshot: bool = False,
    scd2_surrogate_key_column: str = "sk",
    scd2_valid_from_column: str = "valid_from",
    scd2_valid_to_column: str = "valid_to",
    scd2_is_current_column: str = "is_current",
    scd2_partition_on_current: bool = True,
    description: str | None = None,
) -> None:
    """Write a LazyFrame to the Flowfile catalog as a Delta table.

    Args:
        df: The FlowFrame to write.
        table_name: Name of the catalog table to write to.
        schema: Target :class:`SchemaReference`. Preferred over ``namespace_id``.
        namespace_id: Legacy. Raw namespace id; mutually exclusive with ``schema``.
        namespace_full_name: Portable ``"catalog.schema"`` name, resolved at run time.
        write_mode: How to handle existing data:
            - 'overwrite': Replace the entire table
            - 'error': Fail if the table already exists
            - 'append': Add rows to the existing table
            - 'upsert': Insert new rows or update existing by merge_keys
            - 'update': Update only existing rows by merge_keys
            - 'delete': Delete rows matching merge_keys
            - 'scd2': Slowly-changing-dimension type 2 write. Changed rows are end-dated
              and re-inserted as a new version; see the ``scd2_*`` arguments.
            - 'virtual': Register a virtual table backed by this flow
              (requires the flow to be registered with the catalog first;
              see :func:`flowfile_frame.register_flow_with_catalog`).
        merge_keys: Column names to use as merge keys (required for upsert/update/delete/scd2;
            also the SCD2 business key).
        partition_by: Delta partition columns (applied at table creation).
        scd2_compare_columns: Columns compared for change detection when ``write_mode="scd2"``.
            Empty means every non-key, non-system column.
        scd2_full_snapshot: When ``write_mode="scd2"``, end-date current rows whose business
            key is absent from this run's input.
        scd2_surrogate_key_column: Name of the generated surrogate-key column (``write_mode="scd2"``).
        scd2_valid_from_column: Name of the generated valid-from column (``write_mode="scd2"``).
        scd2_valid_to_column: Name of the generated valid-to column (``write_mode="scd2"``).
        scd2_is_current_column: Name of the generated is-current column (``write_mode="scd2"``).
        scd2_partition_on_current: Partition new SCD2 tables by the is-current column.
        description: Optional description for the table.

    Raises:
        ValueError: If both ``schema`` and ``namespace_id`` are provided, or
            if merge_keys are required but not provided.
    """
    df.write_catalog_table(
        table_name=table_name,
        schema=schema,
        namespace_id=namespace_id,
        namespace_full_name=namespace_full_name,
        write_mode=write_mode,
        merge_keys=merge_keys,
        partition_by=partition_by,
        scd2_compare_columns=scd2_compare_columns,
        scd2_full_snapshot=scd2_full_snapshot,
        scd2_surrogate_key_column=scd2_surrogate_key_column,
        scd2_valid_from_column=scd2_valid_from_column,
        scd2_valid_to_column=scd2_valid_to_column,
        scd2_is_current_column=scd2_is_current_column,
        scd2_partition_on_current=scd2_partition_on_current,
        description=description,
    )
