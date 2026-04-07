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


# ---------------------------------------------------------------------------
# JSON serialisation helpers
# ---------------------------------------------------------------------------


def make_json_safe(val: object) -> object:
    """Coerce *val* to a JSON-native Python type."""
    if val is None or isinstance(val, bool | int | float | str):
        return val
    return str(val)


# ---------------------------------------------------------------------------
# Delta timestamp formatting
# ---------------------------------------------------------------------------


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
        # Milliseconds since epoch
        return datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat()
    return str(ts)


# ---------------------------------------------------------------------------
# Delta table size
# ---------------------------------------------------------------------------


def get_delta_size_bytes(path: str | Path) -> int:
    """Sum the sizes of active data files from the Delta transaction log.

    Uses the Delta log metadata rather than filesystem scanning, which
    correctly excludes tombstoned files from previous versions.

    Falls back to filesystem scanning if the delta log can't be read.
    """
    from deltalake import DeltaTable

    try:
        dt = DeltaTable(str(path))
        add_actions = dt.get_add_actions(flatten=True)
        size_col = add_actions.column("size_bytes")
        return sum(v for v in size_col.to_pylist() if v is not None)
    except Exception:
        logger.warning("Failed to read size from delta log, falling back to filesystem scan", exc_info=True)
        return sum(f.stat().st_size for f in Path(path).rglob("*.parquet"))


# ---------------------------------------------------------------------------
# Catalog path validation
# ---------------------------------------------------------------------------


def write_delta(df: pl.LazyFrame | pl.DataFrame, output_path: str, mode: str = "overwrite") -> bool:
    """Write a Polars DataFrame or LazyFrame to a Delta table.

    When *df* is a LazyFrame the write is streamed via ``sink_delta``,
    avoiding full materialisation in memory.  An eager DataFrame falls
    back to ``write_delta``.

    Handles schema_mode options for overwrite and append modes.
    """
    import os

    import polars as pl_

    os.makedirs(output_path, exist_ok=True)

    # Skip no-op append: empty data with unchanged schema on existing table
    if mode == "append" and os.path.isdir(os.path.join(output_path, "_delta_log")):
        from deltalake import DeltaTable

        existing_cols = {f.name for f in DeltaTable(output_path).schema().fields}
        if isinstance(df, pl_.LazyFrame):
            incoming_cols = set(df.collect_schema().names())
        else:
            incoming_cols = set(df.columns)
        if incoming_cols == existing_cols:
            row_count = df.select(pl_.len()).collect().item() if isinstance(df, pl_.LazyFrame) else df.height
            if row_count == 0:
                return False

    delta_write_options: dict[str, str] = {}
    if mode == "overwrite":
        delta_write_options["schema_mode"] = "overwrite"
    elif mode == "append":
        delta_write_options["schema_mode"] = "merge"

    if isinstance(df, pl_.LazyFrame):
        df.sink_delta(output_path, mode=mode, delta_write_options=delta_write_options)
    else:
        df.write_delta(output_path, mode=mode, delta_write_options=delta_write_options)
    return True


def merge_into_delta(
    df: pl.DataFrame,
    output_path: str,
    merge_mode: str = "upsert",
    merge_keys: list[str] | None = None,
) -> bool:
    """Merge a Polars DataFrame into a Delta table.

    Handles table creation when the target doesn't exist yet and supports
    three merge modes: ``upsert``, ``update``, and ``delete``.

    Returns ``True`` if data was written, ``False`` if the write was a no-op.
    """
    import os

    from deltalake import DeltaTable

    table_exists = os.path.isdir(output_path) and os.path.isdir(os.path.join(output_path, "_delta_log"))

    # Skip no-op update: empty data with unchanged schema on existing table
    if merge_mode == "update" and df.height == 0 and table_exists:
        existing_cols = {f.name for f in DeltaTable(output_path).schema().fields}
        if set(df.columns) == existing_cols:
            return False

    if not table_exists:
        os.makedirs(output_path, exist_ok=True)
        if merge_mode in ("delete", "update"):
            df.clear().write_delta(output_path, mode="error")
        else:
            df.write_delta(output_path, mode="error")
    else:
        if not merge_keys:
            raise ValueError("merge_keys is required for merge operations on existing tables")

        dt = DeltaTable(output_path)

        # Schema evolution: add new source columns to the target before merging
        if merge_mode in ("upsert", "update"):
            target_col_names = {field.name for field in dt.schema().fields}
            new_cols = [c for c in df.columns if c not in target_col_names]
            if new_cols:
                df.clear().write_delta(output_path, mode="append", delta_write_options={"schema_mode": "merge"})
                dt = DeltaTable(output_path)

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


# ---------------------------------------------------------------------------
# Catalog path validation
# ---------------------------------------------------------------------------


def validate_catalog_path(table_name: str, catalog_dir: Path) -> Path:
    """Validate that *table_name* is a simple name and resolve it under *catalog_dir*.

    Only a bare name is accepted (no path separators, no ``..``, no null
    bytes).  The full path is constructed from the trusted *catalog_dir*.

    Raises ``ValueError`` when the input is invalid.
    """
    if not table_name:
        raise ValueError("table_name must not be empty")
    if "\x00" in table_name:
        raise ValueError("table_name contains null bytes")
    if "/" in table_name or "\\" in table_name:
        raise ValueError("table_name must not contain path separators")
    if ".." in table_name:
        raise ValueError("table_name must not contain '..'")

    return catalog_dir.resolve() / table_name
