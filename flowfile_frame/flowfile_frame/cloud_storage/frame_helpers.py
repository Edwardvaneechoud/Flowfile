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


def read_from_cloud_storage(
    source: str,
    *,
    file_format: Literal["csv", "parquet", "json", "delta"] = "parquet",
    connection_name: str | None = None,
    scan_mode: Literal["single_file", "directory"] | None = None,
    delimiter: str = ";",
    has_header: bool = True,
    encoding: str = "utf8",
    delta_version: int | None = None,
    changes_since: int | str | datetime | None = None,
    include_change_preimage: bool = False,
    output_field_config: input_schema.OutputFieldConfig | None = None,
) -> FlowFrame:
    """Read data from cloud storage.

    Unified function that supports all cloud storage formats (CSV, Parquet,
    JSON, Delta). Returns a FlowFrame for consistency with read_database()
    and read_catalog_table().

    Args:
        source: Cloud storage path (e.g., 's3://bucket/path/file.parquet').
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
        output_field_config: Optional schema validation/transformation config.
            Set ``enabled=True`` and supply ``fields`` to enforce the expected
            output schema (matches the canvas-side ``output_field_config``
            knob). Construct via :class:`flowfile_frame.OutputFieldConfig`.

    Returns:
        FlowFrame: A FlowFrame backed by a cloud storage reader node.
    """
    from flowfile_frame.flow_frame_methods import (
        scan_csv_from_cloud_storage,
        scan_delta,
        scan_json_from_cloud_storage,
        scan_parquet_from_cloud_storage,
    )

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
            output_field_config=output_field_config,
        )
    elif file_format == "parquet":
        frame = scan_parquet_from_cloud_storage(
            source,
            connection_name=connection_name,
            scan_mode=scan_mode,
            output_field_config=output_field_config,
        )
    elif file_format == "json":
        frame = scan_json_from_cloud_storage(
            source,
            connection_name=connection_name,
            scan_mode=scan_mode,
            output_field_config=output_field_config,
        )
    elif file_format == "delta":
        frame = scan_delta(
            source,
            connection_name=connection_name,
            version=delta_version,
            changes_since=changes_since,
            include_change_preimage=include_change_preimage,
            output_field_config=output_field_config,
        )
    else:
        raise ValueError(f"Unsupported file format: {file_format}")

    return frame


def write_to_cloud_storage(
    df: FlowFrame,
    path: str,
    *,
    file_format: Literal["csv", "parquet", "json", "delta"] = "parquet",
    connection_name: str | None = None,
    delimiter: str = ";",
    encoding: CsvEncoding = "utf8",
    compression: Literal["snappy", "gzip", "brotli", "lz4", "zstd"] = "snappy",
    write_mode: Literal["overwrite", "append", "error", "upsert", "update", "delete"] = "overwrite",
    partition_by: list[str] | None = None,
    merge_keys: list[str] | None = None,
    track_changes: bool = False,
) -> None:
    """Write data to cloud storage.

    Unified function that supports all cloud storage formats (CSV, Parquet,
    JSON, Delta). Consistent with write_database() and write_catalog_table().

    Args:
        df: The flowframe to write.
        path: Cloud storage destination path.
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
    """
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
    else:
        raise ValueError(f"Unsupported file format: {file_format}")


def add_write_ff_to_cloud_storage(
    path: str,
    flow_graph: FlowGraph | None,
    depends_on_node_id: int,
    *,
    connection_name: str | None = None,
    write_mode: Literal["overwrite", "append", "error", "upsert", "update", "delete"] = "overwrite",
    file_format: Literal["csv", "parquet", "json", "delta"] = "parquet",
    csv_delimiter: str = ";",
    csv_encoding: CsvEncoding = "utf8",
    parquet_compression: Literal["snappy", "gzip", "brotli", "lz4", "zstd"] = "snappy",
    partition_by: list[str] | None = None,
    merge_keys: list[str] | None = None,
    track_changes: bool = False,
    description: str | None = None,
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
        ),
        user_id=get_current_user_id(),
        depending_on_id=depends_on_node_id,
        description=description,
    )
    flow_graph.add_cloud_storage_writer(settings)
    return node_id
