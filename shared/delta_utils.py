"""Shared Delta Lake / catalog utility helpers.

Small, dependency-light functions used by both ``flowfile_core`` and
``flowfile_worker``.  Keeping them here avoids cross-package duplication.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl

logger = logging.getLogger(__name__)


# JSON serialisation helpers


def make_json_safe(val: object) -> object:
    """Coerce *val* to a JSON-native Python type."""
    if val is None or isinstance(val, bool | int | float | str):
        return val
    return str(val)


# Delta timestamp formatting


def format_delta_timestamp(ts: object) -> str | None:
    """Convert a raw delta-log timestamp to an ISO 8601 string.

    Handles ``None``, ``str``, ``datetime``, and epoch-millisecond
    ``int``/``float`` values.
    """
    if ts is None:
        return None
    if isinstance(ts, str):
        return ts
    if isinstance(ts, datetime):
        return ts.isoformat()
    if isinstance(ts, int | float):
        return datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat()
    return str(ts)


# Delta table size


def get_delta_size_bytes(path: str | Path, storage_options: dict[str, str] | None = None) -> int:
    """Sum the sizes of active data files from the Delta transaction log.

    Uses the Delta log metadata rather than filesystem scanning, which
    correctly excludes tombstoned files from previous versions.

    Falls back to filesystem scanning if the delta log can't be read (local only;
    when *storage_options* is set the table lives in object storage, so an
    unreadable log yields ``0`` rather than a local filesystem scan).
    """
    from deltalake import DeltaTable

    try:
        dt = DeltaTable(str(path), storage_options=storage_options)
        add_actions = dt.get_add_actions(flatten=True)
        size_col = add_actions.column("size_bytes")
        return sum(v for v in size_col.to_pylist() if v is not None)
    except Exception:
        if storage_options is not None:
            logger.warning("Failed to read size from delta log for %s", path, exc_info=True)
            return 0
        logger.warning("Failed to read size from delta log, falling back to filesystem scan", exc_info=True)
        return sum(f.stat().st_size for f in Path(path).rglob("*.parquet"))


def _delta_table_exists(output_path: str, storage_options: dict[str, str] | None) -> bool:
    """Return ``True`` if a Delta table exists at *output_path* (local or object storage).

    Locally this is a cheap ``_delta_log`` directory probe; for object storage it
    attempts to open the table, treating any failure as "does not exist".
    """
    if storage_options is None:
        import os

        return os.path.isdir(output_path) and os.path.isdir(os.path.join(output_path, "_delta_log"))

    from deltalake import DeltaTable

    try:
        DeltaTable(str(output_path), storage_options=storage_options)
        return True
    except Exception:
        return False


# Catalog path validation


def _frame_column_names(df: pl.LazyFrame | pl.DataFrame) -> set[str]:
    """Return the column names of a LazyFrame or DataFrame without materialising data."""
    import polars as pl_

    if isinstance(df, pl_.LazyFrame):
        return set(df.collect_schema().names())
    return set(df.columns)


def _validate_partition_columns(df: pl.LazyFrame | pl.DataFrame, partition_by: list[str]) -> None:
    """Raise ``ValueError`` if any partition column is absent from *df*."""
    missing = [c for c in partition_by if c not in _frame_column_names(df)]
    if missing:
        raise ValueError(f"partition_by columns not present in data: {missing}")


def write_delta(
    df: pl.LazyFrame | pl.DataFrame,
    output_path: str,
    mode: str = "overwrite",
    partition_by: list[str] | None = None,
    storage_options: dict[str, str] | None = None,
) -> bool:
    """Write a Polars DataFrame or LazyFrame to a Delta table.

    When *df* is a LazyFrame the write is streamed via ``sink_delta``,
    avoiding full materialisation in memory.  An eager DataFrame falls
    back to ``write_delta``.

    Handles schema_mode options for overwrite and append modes. *partition_by*
    is passed through to Delta for every mode: it defines partitioning when the
    table is created and, on writes to an existing table, Delta enforces that it
    matches the table's existing partitioning (raising on a mismatch).

    When *storage_options* is set, *output_path* is an object-storage URI
    (``s3://...``) and the write targets that backend; the same ``schema_mode`` /
    ``partition_by`` semantics apply.  When ``None`` the behavior is identical to
    a local filesystem write.
    """
    import os

    import polars as pl_

    if storage_options is None:
        os.makedirs(output_path, exist_ok=True)

    # Skip no-op append: empty data with unchanged schema on existing table
    if mode == "append" and _delta_table_exists(output_path, storage_options):
        from deltalake import DeltaTable

        existing_cols = {f.name for f in DeltaTable(str(output_path), storage_options=storage_options).schema().fields}
        incoming_cols = _frame_column_names(df)
        if incoming_cols == existing_cols:
            row_count = df.select(pl_.len()).collect().item() if isinstance(df, pl_.LazyFrame) else df.height
            if row_count == 0:
                return False

    delta_write_options: dict[str, object] = {}
    if mode == "overwrite":
        delta_write_options["schema_mode"] = "overwrite"
    elif mode == "append":
        delta_write_options["schema_mode"] = "merge"

    if partition_by:
        _validate_partition_columns(df, partition_by)
        delta_write_options["partition_by"] = partition_by

    write_kwargs: dict[str, object] = {"mode": mode, "delta_write_options": delta_write_options}
    if storage_options is not None:
        write_kwargs["storage_options"] = storage_options

    if isinstance(df, pl_.LazyFrame):
        df.sink_delta(output_path, **write_kwargs)
    else:
        df.write_delta(output_path, **write_kwargs)
    return True


def merge_into_delta(
    df: pl.DataFrame,
    output_path: str,
    merge_mode: str = "upsert",
    merge_keys: list[str] | None = None,
    partition_by: list[str] | None = None,
    storage_options: dict[str, str] | None = None,
) -> bool:
    """Merge a Polars DataFrame into a Delta table.

    Handles table creation when the target doesn't exist yet and supports
    three merge modes: ``upsert``, ``update``, and ``delete``.

    When *storage_options* is set, *output_path* is an object-storage URI and the
    merge runs against that backend; existence is probed by opening the table
    rather than via local filesystem checks. ``None`` is identical to today's
    local behavior.

    Returns ``True`` if data was written, ``False`` if the write was a no-op.
    """
    import os

    from deltalake import DeltaTable

    table_exists = _delta_table_exists(output_path, storage_options)

    # Skip no-op update: empty data with unchanged schema on existing table
    if merge_mode == "update" and df.height == 0 and table_exists:
        existing_cols = {f.name for f in DeltaTable(str(output_path), storage_options=storage_options).schema().fields}
        if set(df.columns) == existing_cols:
            return False

    create_kwargs: dict[str, object] = {}
    if storage_options is not None:
        create_kwargs["storage_options"] = storage_options

    if not table_exists:
        if storage_options is None:
            os.makedirs(output_path, exist_ok=True)
        create_opts: dict[str, object] = {}
        if partition_by:
            _validate_partition_columns(df, partition_by)
            create_opts["partition_by"] = partition_by
        if merge_mode in ("delete", "update"):
            df.clear().write_delta(output_path, mode="error", delta_write_options=create_opts, **create_kwargs)
        else:
            df.write_delta(output_path, mode="error", delta_write_options=create_opts, **create_kwargs)
    else:
        if partition_by:
            logger.warning("Ignoring partition_by on merge into existing table: Delta partitioning is immutable")
        if not merge_keys:
            raise ValueError("merge_keys is required for merge operations on existing tables")

        dt = DeltaTable(str(output_path), storage_options=storage_options)

        # Schema evolution: add new source columns to the target before merging
        if merge_mode in ("upsert", "update"):
            target_col_names = {field.name for field in dt.schema().fields}
            new_cols = [c for c in df.columns if c not in target_col_names]
            if new_cols:
                df.clear().write_delta(
                    output_path, mode="append", delta_write_options={"schema_mode": "merge"}, **create_kwargs
                )
                dt = DeltaTable(str(output_path), storage_options=storage_options)

        predicate = " AND ".join(f'target."{k}" = source."{k}"' for k in merge_keys)
        source_arrow = df.to_arrow()

        merger = dt.merge(
            source=source_arrow,
            predicate=predicate,
            source_alias="source",
            target_alias="target",
        )
        if merge_mode == "upsert":
            merger.when_matched_update_all().when_not_matched_insert_all().execute()
        elif merge_mode == "update":
            merger.when_matched_update_all().execute()
        elif merge_mode == "delete":
            merger.when_matched_delete().execute()
        else:
            raise ValueError(f"Unknown merge_mode: {merge_mode}")
    return True


# Delta maintenance: vacuum / optimize


def get_delta_partition_columns(path: str | Path, storage_options: dict[str, str] | None = None) -> list[str]:
    """Return the partition columns of a Delta table, or ``[]`` if unpartitioned/unreadable."""
    from deltalake import DeltaTable

    try:
        return list(DeltaTable(str(path), storage_options=storage_options).metadata().partition_columns)
    except Exception:
        logger.warning("Failed to read partition columns from %s", path, exc_info=True)
        return []


def vacuum_delta(
    path: str | Path,
    retention_hours: int = 168,
    dry_run: bool = True,
    storage_options: dict[str, str] | None = None,
) -> list[str]:
    """Vacuum tombstoned files from a Delta table, returning the affected file list.

    Delta enforces a minimum 168h (7-day) retention; a shorter window requires
    disabling that guard, which this does automatically for ``retention_hours < 168``.
    """
    from deltalake import DeltaTable

    dt = DeltaTable(str(path), storage_options=storage_options)
    return dt.vacuum(
        retention_hours=retention_hours,
        dry_run=dry_run,
        enforce_retention_duration=retention_hours >= 168,
    )


def optimize_delta(
    path: str | Path,
    z_order_columns: list[str] | None = None,
    storage_options: dict[str, str] | None = None,
) -> dict:
    """Optimize a Delta table, returning the metrics dict.

    Z-orders by *z_order_columns* when given, otherwise compacts small files.
    """
    from deltalake import DeltaTable

    dt = DeltaTable(str(path), storage_options=storage_options)
    if z_order_columns:
        return dt.optimize.z_order(z_order_columns)
    return dt.optimize.compact()


# Catalog path validation


def validate_catalog_path(table_name: str, catalog_dir: Path) -> Path:
    """Validate that *table_name* is a simple name and resolve it under *catalog_dir*.

    Only a bare name is accepted (no path separators, no ``..``, no null
    bytes).  The full path is constructed from the trusted *catalog_dir*.

    Raises ``ValueError`` when the input is invalid.
    """
    _guard_bare_table_name(table_name)
    return catalog_dir.resolve() / table_name


def _guard_bare_table_name(table_name: str) -> None:
    """Raise ``ValueError`` unless *table_name* is a simple, separator-free name."""
    if not table_name:
        raise ValueError("table_name must not be empty")
    if "\x00" in table_name:
        raise ValueError("table_name contains null bytes")
    if "/" in table_name or "\\" in table_name:
        raise ValueError("table_name must not contain path separators")
    if ".." in table_name:
        raise ValueError("table_name must not contain '..'")


def validate_catalog_uri(table_name: str, base_uri: str) -> str:
    """Validate *table_name* and join it onto an object-storage *base_uri*.

    The cloud analogue of :func:`validate_catalog_path`: applies the same
    bare-name guards (no separators, no ``..``, no null bytes) while producing a
    URI (e.g. ``s3://bucket/prefix/table``) instead of resolving a local path.
    """
    _guard_bare_table_name(table_name)
    return base_uri.rstrip("/") + "/" + table_name
