from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Literal

from polars._typing import CsvEncoding

from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.schemas import cloud_storage_schemas, input_schema
from flowfile_frame.cloud_storage.secret_manager import get_current_user_id
from flowfile_frame.utils import generate_node_id

if TYPE_CHECKING:
    from flowfile_frame.flow_frame import FlowFrame


def _iceberg_catalog(
    file_format: str, uri: str | None, warehouse: str | None, token_secret: str | None
) -> cloud_storage_schemas.IcebergCatalogSettings | None:
    """The catalog block of an Iceberg node; the catalog keywords are refused with any other format."""
    if file_format != "iceberg":
        if uri or warehouse or token_secret:
            raise ValueError("catalog_uri, catalog_warehouse and catalog_token_secret need file_format='iceberg'")
        return None
    if not uri:
        raise ValueError("file_format='iceberg' needs catalog_uri, the address of the table's Iceberg REST catalog")
    return cloud_storage_schemas.IcebergCatalogSettings(uri=uri, warehouse=warehouse, token_secret=token_secret)


def read_from_cloud_storage(
    source: str,
    *,
    file_format: Literal["csv", "parquet", "json", "delta", "iceberg"] = "parquet",
    connection_name: str | None = None,
    scan_mode: Literal["single_file", "directory"] | None = None,
    delimiter: str = ";",
    has_header: bool = True,
    encoding: str = "utf8",
    delta_version: int | None = None,
    changes_since: int | str | datetime | None = None,
    include_change_preimage: bool = False,
    description: str | None = None,
    output_field_config: input_schema.OutputFieldConfig | None = None,
    catalog_uri: str | None = None,
    catalog_warehouse: str | None = None,
    catalog_token_secret: str | None = None,
) -> FlowFrame:
    """Read data from cloud storage.

    Unified function that supports all cloud storage formats (CSV, Parquet,
    JSON, Delta, Iceberg). Returns a FlowFrame for consistency with read_database()
    and read_catalog_table().

    With ``file_format="iceberg"``, ``source`` is a table's name (``namespace.table``) and ``catalog_uri`` the
    address of the Iceberg REST catalog that keeps it; the catalog hands out the storage keys each time the flow
    runs, so no connection is named.

    Args:
        source: Cloud storage path (e.g., 's3://bucket/path/file.parquet'), or an Iceberg table's name.
        file_format: File format to read.
        connection_name: Name of the stored cloud storage connection.
        scan_mode: 'single_file' or 'directory'. Auto-detected from path if None.
        delimiter: CSV delimiter (only used for CSV format).
        has_header: Whether CSV has headers (only used for CSV format).
        encoding: CSV encoding (only used for CSV format).
        delta_version: Delta table version for time-travel (only used for Delta format).
        changes_since: Read the Delta table's change feed instead of its current rows
            (only used for Delta format). See :func:`flowfile_frame.scan_delta`.
        include_change_preimage: Keep ``update_preimage`` rows in the change feed
            (only used for Delta format).
        description: Optional description for the node.
        output_field_config: Optional schema validation/transformation config.
            Set ``enabled=True`` and supply ``fields`` to enforce the expected
            output schema (matches the canvas-side ``output_field_config``
            knob). Construct via :class:`flowfile_frame.OutputFieldConfig`.
        catalog_uri: The Iceberg REST catalog's address (only used for Iceberg format).
        catalog_warehouse: The catalog's warehouse (only used for Iceberg format).
        catalog_token_secret: The name of a stored secret holding a bearer token for the catalog (only used for
            Iceberg format; not yet tested against a catalog that checks it).

    Returns:
        FlowFrame: A FlowFrame backed by a cloud storage reader node.
    """
    from flowfile_frame.flow_frame_methods import (
        _scan_iceberg_table,
        scan_csv_from_cloud_storage,
        scan_delta,
        scan_json_from_cloud_storage,
        scan_parquet_from_cloud_storage,
    )

    catalog = _iceberg_catalog(file_format, catalog_uri, catalog_warehouse, catalog_token_secret)
    if (changes_since is not None or include_change_preimage) and file_format != "delta":
        raise ValueError("changes_since is only supported for the 'delta' file format")

    if file_format == "csv":
        frame = scan_csv_from_cloud_storage(
            source,
            connection_name=connection_name,
            scan_mode=scan_mode,
            delimiter=delimiter,
            has_header=has_header,
            encoding=encoding,
            description=description,
            output_field_config=output_field_config,
        )
    elif file_format == "parquet":
        frame = scan_parquet_from_cloud_storage(
            source,
            connection_name=connection_name,
            scan_mode=scan_mode,
            description=description,
            output_field_config=output_field_config,
        )
    elif file_format == "json":
        frame = scan_json_from_cloud_storage(
            source,
            connection_name=connection_name,
            scan_mode=scan_mode,
            description=description,
            output_field_config=output_field_config,
        )
    elif file_format == "delta":
        frame = scan_delta(
            source,
            connection_name=connection_name,
            version=delta_version,
            changes_since=changes_since,
            include_change_preimage=include_change_preimage,
            description=description,
            output_field_config=output_field_config,
        )
    elif file_format == "iceberg":
        frame = _scan_iceberg_table(source, catalog, description=description, output_field_config=output_field_config)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")

    return frame


def write_to_cloud_storage(
    df: FlowFrame,
    path: str,
    *,
    file_format: Literal["csv", "parquet", "json", "delta", "iceberg"] = "parquet",
    connection_name: str | None = None,
    delimiter: str = ";",
    encoding: CsvEncoding = "utf8",
    compression: Literal["snappy", "gzip", "brotli", "lz4", "zstd"] = "snappy",
    write_mode: Literal["overwrite", "append", "error", "upsert", "update", "delete"] = "overwrite",
    partition_by: list[str] | None = None,
    merge_keys: list[str] | None = None,
    track_changes: bool = False,
    catalog_uri: str | None = None,
    catalog_warehouse: str | None = None,
    catalog_token_secret: str | None = None,
) -> None:
    """Write data to cloud storage.

    Unified function that supports all cloud storage formats (CSV, Parquet,
    JSON, Delta, Iceberg). Consistent with write_database() and write_catalog_table().

    With ``file_format="iceberg"``, ``path`` is a table's name (``namespace.table``) and ``catalog_uri`` the address
    of the Iceberg REST catalog that keeps it. A missing table is created; ``write_mode`` is ``"overwrite"`` or
    ``"append"``, and a table's columns and partitioning are never changed.

    Args:
        df: The flowframe to write.
        path: Cloud storage destination path, or an Iceberg table's name.
        file_format: File format to write.
        connection_name: Name of the stored cloud storage connection.
        delimiter: CSV delimiter (only used for CSV format).
        encoding: CSV encoding (only used for CSV format).
        compression: Parquet compression (only used for Parquet format).
        write_mode: How to handle existing data (only used for Delta format): 'overwrite',
            'append', 'error' (fail if the table exists), or 'upsert' / 'update' / 'delete'
            (merge on ``merge_keys``).
        partition_by: Delta partition columns, applied at table creation
            (only used for Delta format).
        merge_keys: Column names for merge operations, required for upsert/update/delete
            (only used for Delta format).
        track_changes: Turn the table's change feed on (only used for Delta format).
            Enable-only; not allowed with ``write_mode="overwrite"``.
        catalog_uri: The Iceberg REST catalog's address (only used for Iceberg format).
        catalog_warehouse: The catalog's warehouse (only used for Iceberg format).
        catalog_token_secret: The name of a stored secret holding a bearer token for the catalog (only used for
            Iceberg format; not yet tested against a catalog that checks it).
    """
    catalog = _iceberg_catalog(file_format, catalog_uri, catalog_warehouse, catalog_token_secret)
    if partition_by and file_format != "delta":
        raise ValueError("partition_by is only supported for the 'delta' file format")
    if (merge_keys or track_changes) and file_format != "delta":
        raise ValueError("merge_keys and track_changes are only supported for the 'delta' file format")

    if file_format == "csv":
        df.write_csv_to_cloud_storage(
            path=path,
            connection_name=connection_name,
            delimiter=delimiter,
            encoding=encoding,
        )
    elif file_format == "parquet":
        df.write_parquet_to_cloud_storage(
            path=path,
            connection_name=connection_name,
            compression=compression,
        )
    elif file_format == "json":
        df.write_json_to_cloud_storage(path=path, connection_name=connection_name)
    elif file_format == "delta":
        df.write_delta(
            path=path,
            connection_name=connection_name,
            write_mode=write_mode,
            partition_by=partition_by,
            merge_keys=merge_keys,
            track_changes=track_changes,
        )
    elif file_format == "iceberg":
        node_id = add_write_ff_to_cloud_storage(
            path, df.flow_graph, df.node_id, write_mode=write_mode, file_format="iceberg", iceberg_catalog=catalog
        )
        df._create_child_frame(node_id)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")


def add_write_ff_to_cloud_storage(
    path: str,
    flow_graph: FlowGraph | None,
    depends_on_node_id: int,
    *,
    connection_name: str | None = None,
    write_mode: Literal["overwrite", "append", "error", "upsert", "update", "delete"] = "overwrite",
    file_format: Literal["csv", "parquet", "json", "delta", "iceberg"] = "parquet",
    csv_delimiter: str = ";",
    csv_encoding: CsvEncoding = "utf8",
    parquet_compression: Literal["snappy", "gzip", "brotli", "lz4", "zstd"] = "snappy",
    partition_by: list[str] | None = None,
    merge_keys: list[str] | None = None,
    track_changes: bool = False,
    description: str | None = None,
    iceberg_catalog: cloud_storage_schemas.IcebergCatalogSettings | None = None,
) -> int:
    node_id = generate_node_id()
    flow_id = flow_graph.flow_id
    settings = input_schema.NodeCloudStorageWriter(
        flow_id=flow_id,
        node_id=node_id,
        cloud_storage_settings=cloud_storage_schemas.CloudStorageWriteSettings(
            resource_path=path,
            connection_name=connection_name,
            file_format=file_format,
            write_mode=write_mode,
            csv_delimiter=csv_delimiter,
            csv_encoding=csv_encoding,
            parquet_compression=parquet_compression,
            partition_by=partition_by,
            merge_keys=merge_keys or [],
            track_changes=track_changes,
            iceberg_catalog=iceberg_catalog,
        ),
        user_id=get_current_user_id(),
        depending_on_id=depends_on_node_id,
        description=description,
    )
    flow_graph.add_cloud_storage_writer(settings)
    return node_id
