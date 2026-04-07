"""Catalog helper functions for FlowFrame operations.

This module provides functions for reading from and writing to the Flowfile
catalog, similar to how database/frame_helpers.py handles database operations.
"""

from __future__ import annotations

from typing import Literal

import polars as pl


def get_current_user_id() -> int:
    """Get the current user ID for catalog operations.

    Returns:
        int: The current user ID (defaults to 1 for single-user mode).
    """
    return 1


def add_write_to_catalog(
    flow_graph: "FlowGraph",
    depends_on_node_id: int,
    *,
    table_name: str,
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
        namespace_id: Optional namespace ID for the table.
        write_mode: How to handle existing data.
        merge_keys: Column names for merge operations.
        description: Optional description for the node.

    Returns:
        int: The node ID of the created catalog writer node.
    """
    from flowfile_core.schemas import input_schema
    from flowfile_frame.utils import generate_node_id

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
            namespace_id=namespace_id,
            write_mode=write_mode,
            merge_keys=merge_keys or [],
        ),
    )

    flow_graph.add_catalog_writer(settings)
    return node_id


def read_catalog_table(
    table_name: str,
    *,
    namespace_id: int | None = None,
    delta_version: int | None = None,
) -> pl.LazyFrame:
    """Read a table from the Flowfile catalog.

    Resolves the table by name (and optionally namespace) via the catalog
    service, then reads the underlying Delta or Parquet file directly.

    Args:
        table_name: Name of the catalog table to read.
        namespace_id: Optional namespace ID to scope the lookup.
        delta_version: Optional Delta version to read (for time-travel queries).

    Returns:
        pl.LazyFrame: The data read from the catalog table.

    Raises:
        ValueError: If the table cannot be found in the catalog.
    """
    from flowfile_core.catalog.delta_utils import is_delta_table
    from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
    from flowfile_core.catalog.service import CatalogService
    from flowfile_core.database.connection import get_db_context

    with get_db_context() as db:
        repo = SQLAlchemyCatalogRepository(db)
        svc = CatalogService(repo)
        file_path = svc.resolve_table_file_path(
            table_name=table_name,
            namespace_id=namespace_id,
        )

    if not file_path:
        raise ValueError(
            f"Catalog table '{table_name}' not found"
            + (f" in namespace {namespace_id}" if namespace_id is not None else "")
        )

    if is_delta_table(file_path):
        scan_kwargs = {}
        if delta_version is not None:
            scan_kwargs["version"] = delta_version
        return pl.scan_delta(file_path, **scan_kwargs)

    return pl.scan_parquet(file_path)


def write_catalog_table(
    df: pl.DataFrame | pl.LazyFrame,
    table_name: str,
    *,
    namespace_id: int | None = None,
    write_mode: Literal["overwrite", "error", "append", "upsert", "update", "delete"] = "overwrite",
    merge_keys: list[str] | None = None,
    description: str | None = None,
) -> None:
    """Write a DataFrame to the Flowfile catalog as a Delta table.

    Args:
        df: The DataFrame or LazyFrame to write.
        table_name: Name of the catalog table to write to.
        namespace_id: Optional namespace ID for the table.
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
        ValueError: If merge_keys are required but not provided.
    """
    from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
    from flowfile_core.catalog.service import CatalogService
    from flowfile_core.database.connection import get_db_context
    from shared.delta_utils import get_delta_size_bytes, merge_into_delta, write_delta
    from shared.storage_config import storage

    if write_mode in ("upsert", "update", "delete") and not merge_keys:
        raise ValueError(f"merge_keys are required for write_mode='{write_mode}'")

    # Collect LazyFrame if needed
    if isinstance(df, pl.LazyFrame):
        collected = df.collect()
    else:
        collected = df

    catalog_dir = storage.catalog_tables_directory
    catalog_dir.mkdir(parents=True, exist_ok=True)

    # Resolve destination path and check for existing table
    with get_db_context() as db:
        repo = SQLAlchemyCatalogRepository(db)
        svc = CatalogService(repo)
        existing, dest_path, delta_mode = svc.resolve_write_destination(
            table_name=table_name,
            namespace_id=namespace_id,
            write_mode=write_mode,
            catalog_dir=catalog_dir,
        )

    # Write the data
    dest = str(dest_path)
    if delta_mode in ("upsert", "update", "delete"):
        wrote = merge_into_delta(collected, dest, merge_mode=delta_mode, merge_keys=merge_keys)
    else:
        wrote = write_delta(collected.lazy(), dest, mode=delta_mode)

    if not wrote:
        return

    # Compute metadata
    schema_list = [{"name": col, "dtype": str(collected.schema[col])} for col in collected.columns]
    meta_kwargs = {
        "schema": schema_list,
        "row_count": len(collected),
        "column_count": len(collected.columns),
        "size_bytes": get_delta_size_bytes(dest_path),
    }

    # Register / update in catalog
    user_id = get_current_user_id()
    with get_db_context() as db:
        repo = SQLAlchemyCatalogRepository(db)
        svc = CatalogService(repo)
        if existing is not None:
            svc.overwrite_table_data(
                table_id=existing.id,
                table_path=dest,
                description=description,
                storage_format="delta",
                **meta_kwargs,
            )
        else:
            svc.register_table_from_data(
                name=table_name,
                table_path=dest,
                owner_id=user_id,
                namespace_id=namespace_id,
                description=description,
                storage_format="delta",
                **meta_kwargs,
            )
