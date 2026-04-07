from __future__ import annotations

from typing import Literal

import polars as pl
from polars._typing import CsvEncoding

from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.schemas import cloud_storage_schemas, input_schema
from flowfile_frame.cloud_storage.secret_manager import get_current_user_id
from flowfile_frame.utils import generate_node_id


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
) -> pl.LazyFrame:
    """Read data from cloud storage.

    Unified function that supports all cloud storage formats (CSV, Parquet,
    JSON, Delta). Returns a pl.LazyFrame for consistency with read_database()
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

    Returns:
        pl.LazyFrame: The data read from cloud storage.
    """
    from flowfile_frame.flow_frame_methods import (
        scan_csv_from_cloud_storage,
        scan_delta,
        scan_json_from_cloud_storage,
        scan_parquet_from_cloud_storage,
    )

    if file_format == "csv":
        frame = scan_csv_from_cloud_storage(
            source,
            connection_name=connection_name,
            scan_mode=scan_mode,
            delimiter=delimiter,
            has_header=has_header,
            encoding=encoding,
        )
    elif file_format == "parquet":
        frame = scan_parquet_from_cloud_storage(
            source,
            connection_name=connection_name,
            scan_mode=scan_mode,
        )
    elif file_format == "json":
        frame = scan_json_from_cloud_storage(
            source,
            connection_name=connection_name,
            scan_mode=scan_mode,
        )
    elif file_format == "delta":
        frame = scan_delta(
            source,
            connection_name=connection_name,
            version=delta_version,
        )
    else:
        raise ValueError(f"Unsupported file format: {file_format}")

    return frame.data


def write_to_cloud_storage(
    df: pl.DataFrame | pl.LazyFrame,
    path: str,
    *,
    file_format: Literal["csv", "parquet", "json", "delta"] = "parquet",
    connection_name: str | None = None,
    delimiter: str = ";",
    encoding: CsvEncoding = "utf8",
    compression: Literal["snappy", "gzip", "brotli", "lz4", "zstd"] = "snappy",
    write_mode: Literal["overwrite", "append"] = "overwrite",
) -> None:
    """Write data to cloud storage.

    Unified function that supports all cloud storage formats (CSV, Parquet,
    JSON, Delta). Consistent with write_database() and write_catalog_table().

    Args:
        df: The DataFrame or LazyFrame to write.
        path: Cloud storage destination path.
        file_format: File format to write.
        connection_name: Name of the stored cloud storage connection.
        delimiter: CSV delimiter (only used for CSV format).
        encoding: CSV encoding (only used for CSV format).
        compression: Parquet compression (only used for Parquet format).
        write_mode: 'overwrite' or 'append' (only used for Delta format).
    """
    from flowfile_frame.flow_frame import FlowFrame

    if isinstance(df, pl.DataFrame):
        df = df.lazy()

    frame = FlowFrame(data=df)

    if file_format == "csv":
        frame.write_csv_to_cloud_storage(
            path=path, connection_name=connection_name, delimiter=delimiter, encoding=encoding,
        )
    elif file_format == "parquet":
        frame.write_parquet_to_cloud_storage(
            path=path, connection_name=connection_name, compression=compression,
        )
    elif file_format == "json":
        frame.write_json_to_cloud_storage(path=path, connection_name=connection_name)
    elif file_format == "delta":
        frame.write_delta(path=path, connection_name=connection_name, write_mode=write_mode)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")


def add_write_ff_to_cloud_storage(
    path: str,
    flow_graph: FlowGraph | None,
    depends_on_node_id: int,
    *,
    connection_name: str | None = None,
    write_mode: Literal["overwrite", "append"] = "overwrite",
    file_format: Literal["csv", "parquet", "json", "delta"] = "parquet",
    csv_delimiter: str = ";",
    csv_encoding: CsvEncoding = "utf8",
    parquet_compression: Literal["snappy", "gzip", "brotli", "lz4", "zstd"] = "snappy",
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
        ),
        user_id=get_current_user_id(),
        depending_on_id=depends_on_node_id,
        description=description,
    )
    flow_graph.add_cloud_storage_writer(settings)
    return node_id
