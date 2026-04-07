"""Unified cloud write functions for all storage providers.

Used by both flowfile_core and flowfile_worker to write LazyFrames to
cloud storage in parquet, delta, csv, and json formats.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any, Literal

import polars as pl
from polars.exceptions import PanicException

from shared.cloud_storage.gcs import sink_to_gcs, write_delta_to_gcs
from shared.cloud_storage.utils import normalize_delta_path

_default_logger = logging.getLogger(__name__)


def _collect_lazy_frame(lf: pl.LazyFrame) -> pl.DataFrame:
    """Collect a LazyFrame, falling back to in-memory if streaming fails."""
    try:
        return lf.collect(engine="streaming")
    except PanicException:
        return lf.collect(engine="in-memory")


def write_parquet_to_cloud(
    df: pl.LazyFrame,
    resource_path: str,
    storage_options: dict[str, Any],
    *,
    compression: str = "snappy",
    credential_provider: Callable | None = None,
    use_pyarrow: bool = False,
    logger: logging.Logger | None = None,
) -> None:
    """Write LazyFrame to a Parquet file in cloud storage."""
    log = logger or _default_logger
    try:
        if use_pyarrow:
            sink_to_gcs(df, path=resource_path, storage_options=storage_options, file_format="parquet")
            return

        sink_kwargs: dict[str, Any] = {
            "path": resource_path,
            "compression": compression,
        }
        if storage_options:
            sink_kwargs["storage_options"] = storage_options
        if credential_provider:
            sink_kwargs["credential_provider"] = credential_provider

        try:
            df.sink_parquet(**sink_kwargs)
        except Exception as e:
            log.warning(f"Failed to use sink_parquet, falling back to collect and write: {e}")
            pl_df = _collect_lazy_frame(df)
            write_kwargs: dict[str, Any] = {
                "file": resource_path,
                "compression": compression,
            }
            if storage_options:
                write_kwargs["storage_options"] = storage_options
            pl_df.write_parquet(**write_kwargs)

    except Exception as e:
        log.error(f"Failed to write Parquet to {resource_path}: {e}")
        raise Exception(f"Failed to write Parquet to cloud storage: {e}") from e


def write_delta_to_cloud(
    df: pl.LazyFrame,
    resource_path: str,
    storage_options: dict[str, Any],
    *,
    mode: str = "overwrite",
    credential_provider: Callable | None = None,
    use_pyarrow: bool = False,
    logger: logging.Logger | None = None,
) -> None:
    """Write LazyFrame to Delta Lake format in cloud storage."""
    log = logger or _default_logger
    if use_pyarrow:
        write_delta_to_gcs(df, resource_path, storage_options, mode=mode)
        return

    sink_kwargs: dict[str, Any] = {
        "target": normalize_delta_path(resource_path),
        "mode": mode,
    }
    if storage_options:
        sink_kwargs["storage_options"] = storage_options
    if credential_provider:
        sink_kwargs["credential_provider"] = credential_provider

    try:
        df.sink_delta(**sink_kwargs)
    except Exception as e:
        log.warning(f"Failed to use sink_delta, falling back to collect and write_delta: {e}")
        write_kwargs: dict[str, Any] = {
            "target": normalize_delta_path(resource_path),
            "mode": mode,
        }
        if storage_options:
            write_kwargs["storage_options"] = storage_options
        _collect_lazy_frame(df).write_delta(**write_kwargs)


def write_csv_to_cloud(
    df: pl.LazyFrame,
    resource_path: str,
    storage_options: dict[str, Any],
    *,
    separator: str = ",",
    credential_provider: Callable | None = None,
    use_pyarrow: bool = False,
    logger: logging.Logger | None = None,
) -> None:
    """Write LazyFrame to a CSV file in cloud storage."""
    log = logger or _default_logger
    try:
        if use_pyarrow:
            sink_to_gcs(df, resource_path, storage_options, file_format="csv", separator=separator)
            return

        sink_kwargs: dict[str, Any] = {
            "path": resource_path,
            "separator": separator,
        }
        if storage_options:
            sink_kwargs["storage_options"] = storage_options
        if credential_provider:
            sink_kwargs["credential_provider"] = credential_provider

        df.sink_csv(**sink_kwargs)

    except Exception as e:
        log.error(f"Failed to write CSV to {resource_path}: {e}")
        raise Exception(f"Failed to write CSV to cloud storage: {e}") from e


def write_json_to_cloud(
    df: pl.LazyFrame,
    resource_path: str,
    storage_options: dict[str, Any],
    *,
    credential_provider: Callable | None = None,
    use_pyarrow: bool = False,
    logger: logging.Logger | None = None,
) -> None:
    """Write LazyFrame to a line-delimited JSON (NDJSON) file in cloud storage."""
    log = logger or _default_logger
    try:
        if use_pyarrow:
            sink_to_gcs(df, resource_path, storage_options, file_format="json")
            return

        sink_kwargs: dict[str, Any] = {"path": resource_path}
        if storage_options:
            sink_kwargs["storage_options"] = storage_options
        if credential_provider:
            sink_kwargs["credential_provider"] = credential_provider

        try:
            df.sink_ndjson(**sink_kwargs)
        except Exception as e:
            log.warning(f"Failed to use sink_ndjson, falling back to collect and write: {e}")
            pl_df = _collect_lazy_frame(df)
            write_kwargs: dict[str, Any] = {"file": resource_path}
            if storage_options:
                write_kwargs["storage_options"] = storage_options
            pl_df.write_ndjson(**write_kwargs)

    except Exception as e:
        log.error(f"Failed to write JSON to {resource_path}: {e}")
        raise Exception(f"Failed to write JSON to cloud storage: {e}") from e


def write_to_cloud(
    df: pl.LazyFrame,
    resource_path: str,
    storage_options: dict[str, Any],
    file_format: Literal["parquet", "delta", "csv", "json"],
    *,
    write_mode: str = "overwrite",
    compression: str = "snappy",
    separator: str = ",",
    credential_provider: Callable | None = None,
    use_pyarrow: bool = False,
    logger: logging.Logger | None = None,
) -> None:
    """Dispatch to the correct writer based on file format.

    This is the top-level entry point for cloud writing, used by both
    flowfile_core and flowfile_worker.
    """
    log = logger or _default_logger

    if write_mode == "append" and file_format != "delta":
        raise NotImplementedError("The 'append' write mode is not yet supported for this destination.")

    if file_format == "parquet":
        write_parquet_to_cloud(
            df, resource_path, storage_options,
            compression=compression, credential_provider=credential_provider,
            use_pyarrow=use_pyarrow, logger=log,
        )
    elif file_format == "delta":
        write_delta_to_cloud(
            df, resource_path, storage_options,
            mode=write_mode, credential_provider=credential_provider,
            use_pyarrow=use_pyarrow, logger=log,
        )
    elif file_format == "csv":
        write_csv_to_cloud(
            df, resource_path, storage_options,
            separator=separator, credential_provider=credential_provider,
            use_pyarrow=use_pyarrow, logger=log,
        )
    elif file_format == "json":
        write_json_to_cloud(
            df, resource_path, storage_options,
            credential_provider=credential_provider,
            use_pyarrow=use_pyarrow, logger=log,
        )
    else:
        raise ValueError(f"Unsupported file format for writing: {file_format}")

    log.info(f"Successfully wrote data to {resource_path}")
