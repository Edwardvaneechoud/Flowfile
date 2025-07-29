"""Cloud storage writer module for FlowFile Worker.

This module provides functionality to write Polars LazyFrames to various cloud storage
services (S3, Azure ADLS, Google Cloud Storage) in different file formats.
"""

import polars as pl
from typing import Dict, Any
from logging import Logger

from flowfile_worker.external_sources.s3_source.models import (
    CloudStorageWriteSettings,
    WriteSettings
)
from flowfile_worker.utils import collect_lazy_frame


def _write_parquet_to_cloud(
    df: pl.LazyFrame,
    resource_path: str,
    storage_options: Dict[str, Any],
    write_settings: WriteSettings,
    logger: Logger
) -> None:
    """Write LazyFrame to a Parquet file in cloud storage.

    Args:
        df: Polars LazyFrame to write.
        resource_path: Cloud storage path where the file will be written.
        storage_options: Storage-specific options for authentication and configuration.
        write_settings: Write configuration including compression settings.
        logger: Logger instance for logging operations.

    Raises:
        Exception: If writing fails, wrapped with a descriptive error message.
    """
    try:
        sink_kwargs = {
            "path": resource_path,
            "compression": write_settings.parquet_compression,
        }
        if storage_options:
            sink_kwargs["storage_options"] = storage_options

        try:
            # Try to use sink_parquet for lazy execution
            df.sink_parquet(**sink_kwargs)
        except Exception as e:
            # Fall back to collecting and writing if sink fails
            logger.warning(f"Failed to use sink_parquet, falling back to collect and write: {str(e)}")
            pl_df = collect_lazy_frame(df)
            sink_kwargs['file'] = sink_kwargs.pop("path")
            pl_df.write_parquet(**sink_kwargs)

    except Exception as e:
        logger.error(f"Failed to write Parquet to {resource_path}: {str(e)}")
        raise Exception(f"Failed to write Parquet to cloud storage: {str(e)}")


def _write_delta_to_cloud(
    df: pl.LazyFrame,
    resource_path: str,
    storage_options: Dict[str, Any],
    write_settings: WriteSettings,
    logger: Logger
) -> None:
    """Write LazyFrame to Delta Lake format in cloud storage.

    Args:
        df: Polars LazyFrame to write.
        resource_path: Cloud storage path where the Delta table will be written.
        storage_options: Storage-specific options for authentication and configuration.
        write_settings: Write configuration including write mode.
        logger: Logger instance for logging operations.
    """
    sink_kwargs = {
        "target": resource_path,
        "mode": write_settings.write_mode,
    }
    if storage_options:
        sink_kwargs["storage_options"] = storage_options

    # Delta format requires collecting the LazyFrame first
    collect_lazy_frame(df).write_delta(**sink_kwargs)


def _write_csv_to_cloud(
    df: pl.LazyFrame,
    resource_path: str,
    storage_options: Dict[str, Any],
    write_settings: WriteSettings,
    logger: Logger
) -> None:
    """Write LazyFrame to a CSV file in cloud storage.

    Args:
        df: Polars LazyFrame to write.
        resource_path: Cloud storage path where the CSV file will be written.
        storage_options: Storage-specific options for authentication and configuration.
        write_settings: Write configuration including delimiter settings.
        logger: Logger instance for logging operations.

    Raises:
        Exception: If writing fails, wrapped with a descriptive error message.
    """
    try:
        sink_kwargs = {
            "path": resource_path,
            "separator": write_settings.csv_delimiter,
        }
        if storage_options:
            sink_kwargs["storage_options"] = storage_options

        # sink_csv executes the lazy query and writes the result
        df.sink_csv(**sink_kwargs)

    except Exception as e:
        logger.error(f"Failed to write CSV to {resource_path}: {str(e)}")
        raise Exception(f"Failed to write CSV to cloud storage: {str(e)}")


def _write_json_to_cloud(
    df: pl.LazyFrame,
    resource_path: str,
    storage_options: Dict[str, Any],
    write_settings: WriteSettings,
    logger: Logger
) -> None:
    """Write LazyFrame to a line-delimited JSON (NDJSON) file in cloud storage.

    Args:
        df: Polars LazyFrame to write.
        resource_path: Cloud storage path where the NDJSON file will be written.
        storage_options: Storage-specific options for authentication and configuration.
        write_settings: Write configuration settings.
        logger: Logger instance for logging operations.

    Raises:
        Exception: If writing fails, wrapped with a descriptive error message.
    """
    try:
        sink_kwargs = {"path": resource_path}
        if storage_options:
            sink_kwargs["storage_options"] = storage_options

        try:
            # Try to use sink_ndjson for lazy execution
            df.sink_ndjson(**sink_kwargs)
        except Exception as e:
            # Fall back to collecting and writing if sink fails
            pl_df = collect_lazy_frame(df)
            sink_kwargs['file'] = sink_kwargs.pop("path")
            pl_df.write_ndjson(**sink_kwargs)
            logger.error(f"Failed to use sink_ndjson, falling back to collect and write: {str(e)}")

    except Exception as e:
        logger.error(f"Failed to write JSON to {resource_path}: {str(e)}")
        raise Exception(f"Failed to write JSON to cloud storage: {str(e)}")

writers = {
    "parquet": _write_parquet_to_cloud,
    "delta": _write_delta_to_cloud,
    "csv": _write_csv_to_cloud,
    "json": _write_json_to_cloud,
}


def write_df_to_cloud(
    df: pl.LazyFrame,
    settings: CloudStorageWriteSettings,
    logger: Logger
) -> None:
    """Write a Polars LazyFrame to an object in cloud storage.

    Supports writing to S3, Azure ADLS, and Google Cloud Storage. Currently supports
    'overwrite' write mode. The 'append' mode is not yet implemented for most formats.

    Args:
        df: Polars LazyFrame to write to cloud storage.
        settings: Cloud storage write settings containing connection details and write options.
        logger: Logger instance for logging operations.

    Raises:
        ValueError: If the specified file format is not supported.
        NotImplementedError: If 'append' write mode is used for non-delta formats.
        Exception: If writing to cloud storage fails.
    """
    connection = settings.connection
    write_settings = settings.write_settings
    logger.info(
        f"Writing to {connection.storage_type} storage: {write_settings.resource_path}"
    )
    # Validate write mode
    if write_settings.write_mode == 'append' and write_settings.file_format != "delta":
        raise NotImplementedError(
            "The 'append' write mode is not yet supported for this destination."
        )

    storage_options = connection.get_storage_options()

    # Dispatch to the appropriate writer
    writer_func = writers.get(write_settings.file_format)
    if not writer_func:
        raise ValueError(
            f"Unsupported file format for writing: {write_settings.file_format}"
        )

    writer_func(
        df,
        write_settings.resource_path,
        storage_options,
        write_settings,
        logger
    )

    logger.info(f"Successfully wrote data to {write_settings.resource_path}")
