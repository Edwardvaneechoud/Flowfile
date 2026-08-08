"""Shared Delta Lake / catalog utility helpers.

Small, dependency-light functions used by both ``flowfile_core`` and
``flowfile_worker``.  Keeping them here avoids cross-package duplication.
"""

from __future__ import annotations

import hashlib
import logging
import time
from dataclasses import dataclass, field
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

    Falls back to filesystem scanning if the delta log can't be read (local only; an
    unreadable object-storage log yields ``0``).
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


def _open_delta_or_none(output_path: str, storage_options: dict[str, str] | None):
    """Open the Delta table at *output_path*, or return ``None`` if it doesn't exist.

    Only a genuine "table not found" yields ``None``; any other failure (auth, network) propagates,
    so a transient error is never mistaken for a missing table (which would silently overwrite).
    """
    from deltalake import DeltaTable
    from deltalake.exceptions import TableNotFoundError

    if storage_options is None:
        import os

        if not (os.path.isdir(output_path) and os.path.isdir(os.path.join(output_path, "_delta_log"))):
            return None
        try:
            return DeltaTable(str(output_path))
        except TableNotFoundError:
            return None

    try:
        return DeltaTable(str(output_path), storage_options=storage_options)
    except TableNotFoundError:
        return None


def _delta_table_exists(output_path: str, storage_options: dict[str, str] | None) -> bool:
    """Return ``True`` if a Delta table exists at *output_path* (local or object storage).

    Locally a cheap ``_delta_log`` probe; for object storage it opens the table. Only a genuine
    "not found" returns ``False`` — any other error propagates rather than masquerading as missing.
    """
    if storage_options is None:
        import os

        return os.path.isdir(output_path) and os.path.isdir(os.path.join(output_path, "_delta_log"))

    return _open_delta_or_none(output_path, storage_options) is not None


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


def _lists_from_fixed_size_arrays(df: pl.LazyFrame | pl.DataFrame) -> pl.LazyFrame | pl.DataFrame:
    """Cast top-level fixed-size ``Array`` columns to ``List`` before a Delta write.

    Delta has no fixed-size-array type, so an ``Array(inner, N)`` column is recorded in the log
    as a plain list while the Parquet files keep the fixed-size layout. ``scan_delta`` then plans
    against the log's ``List`` and collects an ``Array``, so the mismatch surfaces as a dtype
    error on every later read instead of at write time. Normalising on the way in keeps the log
    and the data agreeing.

    Only top-level columns are rewritten: an ``Array`` nested inside a ``Struct`` or a ``List``
    still round-trips inconsistently.
    """
    import polars as pl_

    schema = df.collect_schema() if isinstance(df, pl_.LazyFrame) else df.schema
    casts = [
        pl_.col(name).cast(pl_.List(dtype.inner)) for name, dtype in schema.items() if isinstance(dtype, pl_.Array)
    ]
    return df.with_columns(casts) if casts else df


def _quote_ident(name: str) -> str:
    """Quote *name* as a SQL identifier for a DataFusion merge clause.

    Embedded double quotes are doubled, the standard SQL identifier-quoting rule. Column names
    reach these clauses from user settings, so splicing them raw would let a quote character break
    out of the identifier and be parsed as SQL.
    """
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


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

    When *storage_options* is set, *output_path* is an object-storage URI and the write
    targets that backend; ``None`` is a local filesystem write.
    """
    import os

    import polars as pl_

    df = _lists_from_fixed_size_arrays(df)

    if storage_options is None:
        os.makedirs(output_path, exist_ok=True)

    # Skip no-op append: empty data with unchanged schema on existing table
    if mode == "append":
        existing_dt = _open_delta_or_none(output_path, storage_options)
        if existing_dt is not None:
            existing_cols = {f.name for f in existing_dt.schema().fields}
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
    commit_metadata: dict[str, str] | None = None,
) -> bool:
    """Merge a Polars DataFrame into a Delta table.

    Handles table creation when the target doesn't exist yet and supports
    three merge modes: ``upsert``, ``update``, and ``delete``.

    When *storage_options* is set, *output_path* is an object-storage URI and existence is
    probed by opening the table rather than via local filesystem checks.

    *commit_metadata* (when set) is stamped onto the Delta commit as custom metadata
    (``userMetadata`` in the log), so table history can attribute the change.

    Returns ``True`` if data was written, ``False`` if the write was a no-op.
    """
    import os

    from deltalake import DeltaTable

    df = _lists_from_fixed_size_arrays(df)

    # Open the target once (re-used below) rather than probing then re-opening.
    dt = _open_delta_or_none(output_path, storage_options)
    table_exists = dt is not None

    # Skip no-op update: empty data with unchanged schema on existing table
    if merge_mode == "update" and df.height == 0 and table_exists:
        if set(df.columns) == {f.name for f in dt.schema().fields}:
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

        # Schema evolution: add new source columns to the target before merging
        if merge_mode in ("upsert", "update"):
            target_col_names = {field.name for field in dt.schema().fields}
            new_cols = [c for c in df.columns if c not in target_col_names]
            if new_cols:
                df.clear().write_delta(
                    output_path, mode="append", delta_write_options={"schema_mode": "merge"}, **create_kwargs
                )
                dt = DeltaTable(str(output_path), storage_options=storage_options)

        predicate = " AND ".join(f"target.{_quote_ident(k)} = source.{_quote_ident(k)}" for k in merge_keys)
        source_arrow = df.to_arrow()

        merge_kwargs: dict[str, object] = {}
        if commit_metadata:
            from deltalake import CommitProperties

            merge_kwargs["commit_properties"] = CommitProperties(custom_metadata=commit_metadata)

        merger = dt.merge(
            source=source_arrow,
            predicate=predicate,
            source_alias="source",
            target_alias="target",
            **merge_kwargs,
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


# In-place table edits (catalog "edit table data" surface)


class DeltaEditError(ValueError):
    """An edit payload failed validation (bad keys, uncastable values, …)."""


class DeltaEditStaleError(Exception):
    """Optimistic-concurrency failure: the table moved past the version the edits were based on."""

    def __init__(self, expected: int, current: int):
        self.expected = expected
        self.current = current
        super().__init__(f"Table is at Delta version {current}, edits were based on version {expected}")


EDIT_COLUMN_DTYPES = ("String", "Int64", "Float64", "Boolean", "Date", "Datetime")
"""Polars dtype names a user may pick for a column added through the edit surface."""


def _edit_dtype(name: str) -> pl.DataType:
    import polars as pl_

    mapping: dict[str, pl.DataType] = {
        "String": pl_.String(),
        "Int64": pl_.Int64(),
        "Float64": pl_.Float64(),
        "Boolean": pl_.Boolean(),
        "Date": pl_.Date(),
        "Datetime": pl_.Datetime("us"),
    }
    if name not in mapping:
        raise DeltaEditError(f"Unsupported dtype for a new column: {name!r} (allowed: {', '.join(EDIT_COLUMN_DTYPES)})")
    return mapping[name]


def _edit_rows_to_frame(
    columns: list[str],
    rows: list[list],
    target_schema: dict[str, pl.DataType],
) -> pl.DataFrame:
    """Build a typed DataFrame from row-major JSON cell values.

    Values arrive JSON-typed (str/int/float/bool/None); each column is cast to the
    target table's dtype, with string→temporal handled via the parsing helpers so
    both ``2024-01-02 10:11:12`` and ISO ``T`` forms round-trip.
    """
    import polars as pl_

    for i, row in enumerate(rows):
        if len(row) != len(columns):
            raise DeltaEditError(f"Row {i} has {len(row)} values, expected {len(columns)}")

    data = {col: [row[i] for row in rows] for i, col in enumerate(columns)}
    df = pl_.DataFrame(data, strict=False)

    exprs = []
    for col in columns:
        target = target_schema[col]
        current = df.schema[col]
        if current == target:
            continue
        if target.is_nested() or isinstance(target, pl_.Binary | pl_.Object):
            raise DeltaEditError(f"Column {col!r} has unsupported dtype {target} for editing")
        expr = pl_.col(col)
        if current == pl_.String:
            if target == pl_.Date:
                expr = expr.str.to_date()
            elif isinstance(target, pl_.Datetime):
                expr = expr.str.to_datetime(time_unit=target.time_unit, time_zone=target.time_zone)
            elif target == pl_.Time:
                expr = expr.str.to_time()
            else:
                expr = expr.cast(target)
        else:
            expr = expr.cast(target)
        exprs.append(expr)
    try:
        return df.with_columns(exprs) if exprs else df
    except Exception as exc:
        raise DeltaEditError(f"Edited values could not be converted to the table's column types: {exc}") from exc


def _require_unique_keys(df: pl.DataFrame, key_columns: list[str], what: str) -> None:
    keys = df.select(key_columns)
    if any(count > 0 for count in keys.null_count().row(0)):
        raise DeltaEditError(f"{what} contain null key values in {key_columns}")
    if keys.is_duplicated().any():
        raise DeltaEditError(f"{what} contain duplicate key values in {key_columns}")


def apply_delta_edits(
    table_path: str,
    key_columns: list[str],
    upsert_columns: list[str] | None = None,
    upsert_rows: list[list] | None = None,
    new_columns: list[dict] | None = None,
    delete_keys: list[list] | None = None,
    expected_version: int | None = None,
    storage_options: dict[str, str] | None = None,
    commit_metadata: dict[str, str] | None = None,
) -> dict:
    """Apply row-level edits (keyed upserts, deletes, new columns) to an existing Delta table.

    The payload is UI-sized: *upsert_rows* / *delete_keys* are row-major JSON cell
    values from an edit grid. Edits attach to rows via *key_columns*, which must
    uniquely identify rows in the target table — validated here, because a merge
    over duplicate keys would silently mis-attach values.

    *expected_version* enables optimistic concurrency: when set and the table's
    current Delta version differs, ``DeltaEditStaleError`` is raised before writing.

    *new_columns* entries are ``{"name", "dtype"}`` with dtype one of
    ``EDIT_COLUMN_DTYPES``; they are added via Delta schema evolution even when no
    rows reference them yet.

    Returns a dict with ``rows_upserted``, ``rows_deleted``, ``new_version`` and
    refreshed table metadata (``schema``, ``row_count``, ``column_count``, ``size_bytes``).
    """
    import polars as pl_
    from deltalake import DeltaTable

    dt = _open_delta_or_none(table_path, storage_options)
    if dt is None:
        raise DeltaEditError(f"No Delta table found at {table_path}")

    current_version = dt.version()
    if expected_version is not None and current_version != expected_version:
        raise DeltaEditStaleError(expected=expected_version, current=current_version)

    target_schema = dict(pl_.scan_delta(table_path, storage_options=storage_options).collect_schema())
    missing_keys = [k for k in key_columns if k not in target_schema]
    if missing_keys:
        raise DeltaEditError(f"Key columns not in table schema: {missing_keys}")

    new_columns = new_columns or []
    for spec in new_columns:
        if spec["name"] in target_schema:
            raise DeltaEditError(f"Column {spec['name']!r} already exists in the table")
        target_schema[spec["name"]] = _edit_dtype(spec["dtype"])

    # Aliased count so a real column named "len" can still serve as a key.
    count_col = "__ff_edit_group_count__"
    key_dupes = (
        pl_.scan_delta(table_path, storage_options=storage_options)
        .group_by(key_columns)
        .agg(pl_.len().alias(count_col))
        .filter(pl_.col(count_col) > 1)
        .select(pl_.len())
        .collect()
        .item()
    )
    if key_dupes:
        raise DeltaEditError(
            f"Key columns {key_columns} do not uniquely identify rows "
            f"({key_dupes} duplicated key groups); pick different key columns or add a record id column"
        )

    # Validate and build every frame before the first commit, so a rejected
    # payload never leaves a partially-applied sequence behind.
    source: pl.DataFrame | None = None
    if upsert_rows:
        if not upsert_columns:
            raise DeltaEditError("upsert_rows requires upsert_columns")
        unknown = [c for c in upsert_columns if c not in target_schema]
        if unknown:
            raise DeltaEditError(f"Unknown columns in edit payload: {unknown}")
        missing = [k for k in key_columns if k not in upsert_columns]
        if missing:
            raise DeltaEditError(f"Edit payload must include the key columns; missing: {missing}")
        source = _edit_rows_to_frame(upsert_columns, upsert_rows, target_schema)
        _require_unique_keys(source, key_columns, "Edited rows")

    deletes: pl.DataFrame | None = None
    if delete_keys:
        deletes = _edit_rows_to_frame(key_columns, delete_keys, target_schema)
        _require_unique_keys(deletes, key_columns, "Deleted rows")

    rows_upserted = 0
    rows_deleted = 0

    # Schema evolution for declared new columns the upsert payload does not carry:
    # merge_into_delta only evolves columns present in its source frame, so a
    # column added without any values yet needs an explicit 0-row append.
    covered = set(upsert_columns or [])
    uncovered = [spec for spec in new_columns if spec["name"] not in covered]
    if uncovered:
        empty = pl_.DataFrame(schema={spec["name"]: _edit_dtype(spec["dtype"]) for spec in uncovered})
        write_kwargs: dict[str, object] = {}
        if storage_options is not None:
            write_kwargs["storage_options"] = storage_options
        empty.write_delta(table_path, mode="append", delta_write_options={"schema_mode": "merge"}, **write_kwargs)

    if source is not None:
        merge_into_delta(
            source,
            table_path,
            merge_mode="upsert",
            merge_keys=key_columns,
            storage_options=storage_options,
            commit_metadata=commit_metadata,
        )
        rows_upserted = source.height

    if deletes is not None:
        merge_into_delta(
            deletes,
            table_path,
            merge_mode="delete",
            merge_keys=key_columns,
            storage_options=storage_options,
            commit_metadata=commit_metadata,
        )
        rows_deleted = deletes.height

    refreshed = pl_.scan_delta(table_path, storage_options=storage_options)
    schema = dict(refreshed.collect_schema())
    row_count = int(refreshed.select(pl_.len()).collect().item())
    return {
        "rows_upserted": rows_upserted,
        "rows_deleted": rows_deleted,
        "new_version": DeltaTable(table_path, without_files=True, storage_options=storage_options).version(),
        "schema": [{"name": name, "dtype": str(dtype)} for name, dtype in schema.items()],
        "row_count": row_count,
        "column_count": len(schema),
        "size_bytes": get_delta_size_bytes(table_path, storage_options=storage_options),
    }


def add_delta_row_id_column(
    table_path: str,
    column_name: str = "record_id",
    expected_version: int | None = None,
    storage_options: dict[str, str] | None = None,
) -> dict:
    """Rewrite a Delta table with a sequential ``Int64`` row-id column appended.

    The escape hatch for tables with no natural unique key: the edit surface needs
    key columns, so this mints them once. A full-table rewrite — the caller decides
    where it runs (worker child in offload mode, in-process otherwise).
    """
    import polars as pl_
    from deltalake import DeltaTable

    dt = _open_delta_or_none(table_path, storage_options)
    if dt is None:
        raise DeltaEditError(f"No Delta table found at {table_path}")
    current_version = dt.version()
    if expected_version is not None and current_version != expected_version:
        raise DeltaEditStaleError(expected=expected_version, current=current_version)

    existing = {f.name for f in dt.schema().fields}
    if column_name in existing:
        raise DeltaEditError(f"Column {column_name!r} already exists in the table")

    df = pl_.scan_delta(table_path, storage_options=storage_options).collect()
    df = df.with_row_index(column_name, offset=1).with_columns(pl_.col(column_name).cast(pl_.Int64))
    # Re-check right before the overwrite: a commit that landed while we collected
    # would otherwise be silently erased by the full rewrite.
    latest = DeltaTable(table_path, without_files=True, storage_options=storage_options).version()
    if latest != current_version:
        raise DeltaEditStaleError(expected=current_version, current=latest)
    write_delta(df, table_path, mode="overwrite", storage_options=storage_options)

    schema = df.schema
    return {
        "new_version": DeltaTable(table_path, without_files=True, storage_options=storage_options).version(),
        "schema": [{"name": name, "dtype": str(dtype)} for name, dtype in schema.items()],
        "row_count": df.height,
        "column_count": len(schema),
        "size_bytes": get_delta_size_bytes(table_path, storage_options=storage_options),
    }


# SCD2 (slowly changing dimension, type 2)

SCD2_SK_ALGORITHM = "sha256-v1"
"""Frozen surrogate-key algorithm id. Changing it changes every key ever generated."""

_SCD2_SK_DIGEST_CHARS = 32
_SCD2_FIELD_SEP = "\x1f"
_SCD2_OP_COLUMN = "__scd2_op"
_SCD2_HIT_COLUMN = "__scd2_hit"
_SCD2_CLASS_COLUMN = "__scd2_class"
_SCD2_TARGET_PREFIX = "__scd2_t"
_SCD2_RESERVED_PREFIX = "__scd2_"


@dataclass
class Scd2Result:
    """Outcome of one SCD2 write.

    ``skipped`` maps onto the existing catalog no-op protocol (no commit, no metadata refresh);
    ``created`` is set only by an initial load. ``rows_closed`` conflates matched closes with
    ``full_snapshot`` close-outs — delta-rs reports a single ``num_target_rows_updated``.
    """

    skipped: bool = False
    created: bool = False
    rows_inserted: int = 0
    rows_closed: int = 0
    rows_total: int = 0
    rows_current: int = 0
    metrics: dict = field(default_factory=dict)


def _scd2_integer_dtypes() -> tuple:
    """The integer dtypes accepted as business keys (``pl.INTEGER_DTYPES`` is deprecated)."""
    import polars as pl_

    return (
        pl_.Int8,
        pl_.Int16,
        pl_.Int32,
        pl_.Int64,
        pl_.UInt8,
        pl_.UInt16,
        pl_.UInt32,
        pl_.UInt64,
    )


def _scd2_canonical_expr(name: str, dtype) -> pl.Expr:
    """Render one business-key column as the frozen canonical string of ``SCD2_SK_ALGORITHM``.

    Deliberately avoids a plain ``cast(String)`` for temporal and boolean dtypes: Polars' default
    string rendering is a library-version decision, while a surrogate key must be stable forever.
    Floating-point and nested keys are rejected — they have no stable canonical form.
    """
    import polars as pl_

    col = pl_.col(name)
    if dtype == pl_.Boolean:
        return pl_.when(col).then(pl_.lit("true")).otherwise(pl_.lit("false"))
    if dtype in _scd2_integer_dtypes():
        return col.cast(pl_.Int64).cast(pl_.String)
    if dtype == pl_.Date:
        return col.dt.to_string("%Y-%m-%d")
    if isinstance(dtype, pl_.Datetime):
        base = col.dt.convert_time_zone("UTC") if dtype.time_zone else col
        return base.dt.cast_time_unit("us").dt.to_string("%Y-%m-%dT%H:%M:%S%.6f")
    if dtype == pl_.String:
        return col
    raise ValueError(
        f"SCD2 business key column '{name}' has unsupported dtype {dtype}. Business keys must be "
        f"string, integer, boolean, date or datetime — floating-point and nested keys have no "
        f"stable canonical form. Cast the column upstream of the catalog writer."
    )


def _scd2_key_payload_expr(schema, business_keys: list[str]) -> pl.Expr:
    """The canonical encoding of the business key alone, without the *valid_from* term.

    This — not the raw columns — is the identity the surrogate key and the stored row are built
    from, so it is also what the null/duplicate pre-flight guards must be evaluated on: two rows
    whose keys differ only below the canonical resolution (e.g. nanosecond datetimes truncated to
    microseconds) are the same key once written.
    """
    import polars as pl_

    parts: list[pl.Expr] = []
    for name in business_keys:
        canonical = _scd2_canonical_expr(name, schema[name])
        parts.append(pl_.concat_str([canonical.str.len_bytes().cast(pl_.String), pl_.lit(":"), canonical]))
    return pl_.concat_str(parts, separator=_SCD2_FIELD_SEP)


def _scd2_payload_expr(schema, business_keys: list[str], valid_from_iso: str) -> pl.Expr:
    """Build the length-prefixed, separator-joined canonical payload hashed into a surrogate key."""
    import polars as pl_

    return pl_.concat_str(
        [
            _scd2_key_payload_expr(schema, business_keys),
            pl_.lit(f"{len(valid_from_iso.encode('utf-8'))}:{valid_from_iso}"),
        ],
        separator=_SCD2_FIELD_SEP,
    )


def scd2_surrogate_keys(df: pl.DataFrame, business_keys: list[str], valid_from_iso: str) -> pl.Series:
    """Deterministic surrogate keys for *df* — ``sha256(payload).hexdigest()[:32]``.

    The payload is ``"\\x1f"``-joined ``"<utf8 byte length>:<canonical value>"`` items: one per
    business key in the given order, then *valid_from_iso* as the final item. Length-prefixing makes
    the encoding injection-proof, so no pair of distinct key tuples can share a payload.

    A pure function of its inputs and stable across Polars/deltalake versions by construction —
    ``pl.Expr.hash`` is deliberately never used (xxhash, not version-stable). Frozen as
    ``SCD2_SK_ALGORITHM`` and pinned by golden vectors in ``shared/tests/test_scd2.py``. Business
    keys are guaranteed non-null by the caller's pre-flight guards.
    """
    import polars as pl_

    if df.height == 0:
        return pl_.Series("sk", [], dtype=pl_.String)
    payload = df.select(_scd2_payload_expr(df.schema, business_keys, valid_from_iso).alias("p")).get_column("p")
    return pl_.Series(
        "sk",
        [hashlib.sha256(v.encode("utf-8")).hexdigest()[:_SCD2_SK_DIGEST_CHARS] for v in payload.to_list()],
        dtype=pl_.String,
    )


def _scd2_parse_iso_utc(value: str) -> datetime:
    """Parse an ISO-8601 instant into a tz-aware UTC datetime (naive input is assumed UTC)."""
    ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def _scd2_sql_timestamp_literal(ts: datetime) -> str:
    """SQL literal for a UTC-microsecond timestamp, for clauses that cannot reference the source."""
    return f"arrow_cast('{ts.strftime('%Y-%m-%dT%H:%M:%S.%f')}Z', 'Timestamp(Microsecond, Some(\"UTC\"))')"


def _scd2_stamp(
    df: pl.DataFrame,
    business_keys: list[str],
    ts: datetime,
    valid_from_iso: str,
    system_columns: list[str],
) -> pl.DataFrame:
    """Append the four SCD2 system columns to *df*, marking every row current."""
    import polars as pl_

    sk_column, valid_from_column, valid_to_column, is_current_column = system_columns
    return df.with_columns(
        scd2_surrogate_keys(df, business_keys, valid_from_iso).alias(sk_column),
        pl_.lit(ts).cast(pl_.Datetime("us", "UTC")).alias(valid_from_column),
        pl_.lit(None).cast(pl_.Datetime("us", "UTC")).alias(valid_to_column),
        pl_.lit(True).alias(is_current_column),
    )


def scd2_into_delta(
    df: pl.DataFrame,
    output_path: str,
    *,
    business_keys: list[str],
    valid_from_iso: str,
    compare_columns: list[str] | None,
    full_snapshot: bool,
    surrogate_key_column: str = "sk",
    valid_from_column: str = "valid_from",
    valid_to_column: str = "valid_to",
    is_current_column: str = "is_current",
    partition_by: list[str] | None = None,
    partition_on_current: bool = True,
    storage_options: dict[str, str] | None = None,
    max_commit_attempts: int = 4,
) -> Scd2Result:
    """Apply an SCD2 batch to a Delta table in one atomic commit.

    Changed rows are end-dated and re-inserted as a new version, new keys are inserted, and close
    and insert ride one ``MERGE`` so a reader never sees a torn state. A missing target is an
    initial load. Nothing to do ⇒ ``Scd2Result(skipped=True)`` with zero commits, which makes
    re-running an identical batch a true no-op. Validity is half-open, ``[valid_from, valid_to)``.

    Args:
        valid_from_iso: One processing instant for the whole write, supplied by the caller. The
            surrogate key is a function of (business key, valid_from), so re-deriving the clock
            here would mint different keys on the local and worker paths of the same write.
        compare_columns: Columns whose change opens a new version; empty means every non-key
            column. These and the business keys are type-checked against the target — any other
            input column still lands in it and widens its schema unchecked.
        full_snapshot: Also end-date current keys absent from *df*.
        partition_by: Extra partition columns, nested above the is-current column. Creation-time
            only; Delta partitioning is immutable.
        partition_on_current: Partition new tables by the is-current column. ``False`` is the
            deliberate opt-out.
        storage_options: Forwarded verbatim — ``None`` is the local filesystem, ``{}`` means
            ambient credentials, so it is never truthiness-tested.
        max_commit_attempts: Attempts before a commit conflict is re-raised. Only
            ``CommitFailedError`` is retried; each retry re-reads and re-classifies.

    Raises:
        ValueError: The batch or the target is unusable, or *valid_from_iso* is not strictly newer
            than every instant already recorded in the table.
    """
    df = _lists_from_fixed_size_arrays(df)

    last_error: Exception | None = None
    for attempt in range(max_commit_attempts):
        try:
            return _scd2_once(
                df,
                output_path,
                business_keys=business_keys,
                valid_from_iso=valid_from_iso,
                compare_columns=compare_columns,
                full_snapshot=full_snapshot,
                surrogate_key_column=surrogate_key_column,
                valid_from_column=valid_from_column,
                valid_to_column=valid_to_column,
                is_current_column=is_current_column,
                partition_by=partition_by,
                partition_on_current=partition_on_current,
                storage_options=storage_options,
            )
        except Exception as exc:
            from deltalake.exceptions import CommitFailedError

            if not isinstance(exc, CommitFailedError) or attempt == max_commit_attempts - 1:
                raise
            last_error = exc
            logger.warning(
                "SCD2 commit conflict on %s (attempt %d/%d), re-reading and retrying",
                output_path,
                attempt + 1,
                max_commit_attempts,
            )
            time.sleep(0.1 * (2**attempt))
    raise last_error  # pragma: no cover - unreachable, the last attempt re-raises


def _scd2_guard_input(
    df: pl.DataFrame,
    business_keys: list[str],
    system_columns: list[str],
    compare_columns: list[str] | None,
) -> list[str]:
    """Pre-flight the incoming batch and return the resolved compare-column set."""
    if not business_keys:
        raise ValueError("SCD2 requires at least one business-key column (merge_keys)")
    if len(set(system_columns)) != len(system_columns):
        raise ValueError(f"SCD2 column names must be distinct, got {system_columns}")

    missing = [c for c in business_keys if c not in df.columns]
    if missing:
        raise ValueError(f"SCD2 business-key column(s) not present in the input data: {missing}")
    collide = [c for c in system_columns if c in df.columns]
    if collide:
        raise ValueError(
            f"SCD2 system column name(s) {collide} collide with input columns. "
            f"Rename the input column(s) or change the SCD2 column names in the node settings."
        )
    # The whole prefix is reserved, not just the three staging columns: the classify join also
    # renames the target's tracked columns to ``__scd2_t{i}``, and an input column of that name
    # would silently be compared in their place.
    reserved = [c for c in df.columns if c.startswith(_SCD2_RESERVED_PREFIX)]
    if reserved:
        raise ValueError(f"Input uses reserved SCD2 internal column name(s): {reserved}")

    requested = list(compare_columns or []) or [c for c in df.columns if c not in business_keys]
    unknown = [c for c in requested if c not in df.columns]
    if unknown:
        raise ValueError(f"SCD2 compare_columns not present in the input data: {unknown}")

    null_keys = [c for c in business_keys if df.get_column(c).null_count() > 0]
    if null_keys:
        raise ValueError(
            f"SCD2 business-key column(s) {null_keys} contain NULL values. Business keys must be "
            f"non-null: a NULL key can never be matched, so its history could not be maintained."
        )
    key_payload = df.select(_scd2_key_payload_expr(df.schema, business_keys).alias("k")).get_column("k")
    duplicates = int(key_payload.is_duplicated().sum())
    if duplicates:
        raise ValueError(
            f"SCD2 input contains {duplicates} row(s) whose business key {business_keys} is not "
            f"unique. Deduplicate upstream (e.g. a Group By / Unique node) before the catalog writer."
        )
    return [c for c in requested if c not in business_keys]


def _scd2_initial_load(
    df: pl.DataFrame,
    output_path: str,
    business_keys: list[str],
    ts: datetime,
    valid_from_iso: str,
    system_columns: list[str],
    partition_by: list[str] | None,
    partition_on_current: bool,
    storage_options: dict[str, str] | None,
) -> Scd2Result:
    """Create the table from *df*, every row current. A 0-row batch still creates it."""
    import os

    staged = _scd2_stamp(df, business_keys, ts, valid_from_iso, system_columns)
    # The is-current flag partitions new SCD2 tables by default: closed rows are quarantined into
    # files later merges never rewrite, and current-state scans prune to a single partition per
    # user-partition. Explicit partition_by columns nest above it; *partition_on_current* is the
    # deliberate opt-out. Validated after stamping so the generated column is legal.
    effective_partition_by = list(partition_by or [])
    if partition_on_current and system_columns[3] not in effective_partition_by:
        effective_partition_by.append(system_columns[3])
    create_options: dict[str, object] = {}
    if effective_partition_by:
        _validate_partition_columns(staged, effective_partition_by)
        create_options["partition_by"] = effective_partition_by
    write_kwargs: dict[str, object] = {}
    if storage_options is not None:
        write_kwargs["storage_options"] = storage_options
    else:
        os.makedirs(output_path, exist_ok=True)
    staged.write_delta(output_path, mode="error", delta_write_options=create_options, **write_kwargs)
    return Scd2Result(
        created=True,
        rows_inserted=staged.height,
        rows_total=staged.height,
        rows_current=staged.height,
    )


def _scd2_supertype(a, b):
    """The Polars supertype of two dtypes, or ``None`` when they have none."""
    import polars as pl_

    try:
        merged = pl_.concat(
            [pl_.Series("x", [], dtype=a).to_frame(), pl_.Series("x", [], dtype=b).to_frame()],
            how="vertical_relaxed",
        )
    except Exception:
        return None
    return merged.schema["x"]


def _scd2_dtype_writes_into(target_dtype, incoming_dtype) -> bool:
    """True when *incoming_dtype* can be written into a *target_dtype* column without loss.

    Change detection happens in the supertype of the pair while the MERGE writes into the target's
    own Delta type, so anything that has to narrow on the way in re-detects the same difference on
    every run: the row is closed and re-inserted with the truncated value, forever. The gate is
    therefore "the supertype IS the target type" — the incoming values widen into the target rather
    than the target widening to meet them. The comparison itself must also be expressible, which
    rules out pairs Polars will supertype but not compare (String vs Int64).

    Temporal dtypes demand exact equality of time unit and time zone: a ``ns``/``us`` or
    naive/aware mismatch survives the supertype check but the classify join rejects it, and for a
    business key the extra precision is silently truncated on write.
    """
    import polars as pl_

    if target_dtype.is_temporal() or incoming_dtype.is_temporal():
        return False
    if _scd2_supertype(target_dtype, incoming_dtype) != target_dtype:
        return False
    try:
        pl_.DataFrame(
            {"a": pl_.Series([None], dtype=incoming_dtype), "b": pl_.Series([None], dtype=target_dtype)}
        ).select(pl_.col("a").ne_missing(pl_.col("b")))
    except Exception:
        return False
    return True


def _scd2_validate_target(
    dt,
    df: pl.DataFrame,
    output_path: str,
    business_keys: list[str],
    compare_columns: list[str],
    system_columns: list[str],
    partition_by: list[str] | None,
    storage_options: dict[str, str] | None,
) -> dict:
    """Check the target is SCD2-shaped and type-compatible; return its Polars schema."""
    import polars as pl_

    target_columns = [f.name for f in dt.schema().fields]
    missing_system = [c for c in system_columns if c not in target_columns]
    if missing_system:
        raise ValueError(
            f"Table at '{output_path}' is not an SCD2 table: column(s) {missing_system} are missing. "
            f"SCD2 mode never converts an existing table in place, because back-filling history that "
            f"was never recorded would be a fabrication. Write to a new table, or recreate this one "
            f"with an SCD2 write."
        )
    missing_keys = [c for c in business_keys if c not in target_columns]
    if missing_keys:
        raise ValueError(f"SCD2 business-key column(s) {missing_keys} are missing from the target table")
    if partition_by:
        logger.warning("Ignoring partition_by on an SCD2 write to an existing table: Delta partitioning is immutable")

    scan_kwargs = {"storage_options": storage_options} if storage_options is not None else {}
    target_schema = dict(pl_.scan_delta(output_path, **scan_kwargs).collect_schema())

    incompatible = []
    for name in business_keys + compare_columns:
        if name in target_schema and target_schema[name] != df.schema[name]:
            if not _scd2_dtype_writes_into(target_schema[name], df.schema[name]):
                incompatible.append((name, str(target_schema[name]), str(df.schema[name])))
    if incompatible:
        detail = ", ".join(f"{c}: table={t}, incoming={i}" for c, t, i in incompatible)
        raise ValueError(
            f"SCD2 write to '{output_path}' aborted: incompatible column type(s) — {detail}. "
            f"Cast the incoming column(s) to the table's types before the catalog writer."
        )
    return target_schema


def _scd2_classify(
    df: pl.DataFrame,
    output_path: str,
    business_keys: list[str],
    compare_columns: list[str],
    target_schema: dict,
    is_current_column: str,
    storage_options: dict[str, str] | None,
) -> tuple[pl.DataFrame, int]:
    """Label every input row new/changed/unchanged, and report the target's current row count.

    The target read is bounded — current slice only, projected to keys plus tracked columns. Change
    detection is null-safe (``ne_missing``), so NULL→value and value→NULL both open a new version
    while NULL→NULL does not. A tracked column the target does not have yet compares against a typed
    NULL, so adding a column to the compare set re-versions the rows it appears on.
    """
    import polars as pl_

    scan_kwargs = {"storage_options": storage_options} if storage_options is not None else {}
    tracked_in_target = [c for c in compare_columns if c in target_schema]
    current = (
        pl_.scan_delta(output_path, **scan_kwargs)
        .filter(pl_.col(is_current_column))
        .select(business_keys + tracked_in_target)
        .rename({c: f"{_SCD2_TARGET_PREFIX}{i}" for i, c in enumerate(tracked_in_target)})
        .with_columns(pl_.lit(True).alias(_SCD2_HIT_COLUMN))
        .collect()
    )
    joined = df.join(current, on=business_keys, how="left")

    diffs = [pl_.col(c).ne_missing(pl_.col(f"{_SCD2_TARGET_PREFIX}{i}")) for i, c in enumerate(tracked_in_target)]
    diffs += [
        pl_.col(c).ne_missing(pl_.lit(None, dtype=df.schema[c])) for c in compare_columns if c not in target_schema
    ]
    changed = pl_.any_horizontal(diffs) if diffs else pl_.lit(False)

    classified = joined.with_columns(
        pl_.when(pl_.col(_SCD2_HIT_COLUMN).is_null())
        .then(pl_.lit("new"))
        .when(changed)
        .then(pl_.lit("changed"))
        .otherwise(pl_.lit("unchanged"))
        .alias(_SCD2_CLASS_COLUMN)
    ).select(list(df.columns) + [_SCD2_CLASS_COLUMN])
    return classified, current.height


def _scd2_guard_clock(
    output_path: str,
    ts: datetime,
    valid_from_column: str,
    valid_to_column: str,
    storage_options: dict[str, str] | None,
) -> None:
    """Refuse an instant that is not strictly newer than every instant already in the table.

    The write stamps *ts* as one interval's end and the next one's start, so a stale instant emits
    a span overlapping one already written — two versions of a key active at once, which every
    ``active_at`` read then returns twice. Gets in via clock skew between machines sharing a
    catalog, or a retry whose conflict winner stamped a later instant than ours.

    Args:
        ts: The write's processing instant.
        valid_from_column: Interval-start column; its max covers every insert.
        valid_to_column: Interval-end column. Read too because a close does not always come with an
            insert: an all-unchanged ``full_snapshot`` end-dates absent keys and inserts nothing,
            stamping *ts* here alone, so a ``valid_from``-only watermark would go stale and wave
            through a write reopening one of those keys inside the interval it just closed.
    """
    import polars as pl_

    scan_kwargs = {"storage_options": storage_options} if storage_options is not None else {}
    stats = (
        pl_.scan_delta(output_path, **scan_kwargs)
        .select(
            pl_.col(valid_from_column).max().alias("vf"),
            pl_.col(valid_to_column).max().alias("vt"),
        )
        .collect()
    )
    stamped = [v for v in (stats.item(0, "vf"), stats.item(0, "vt")) if v is not None]
    if not stamped:
        return
    newest = max(v.replace(tzinfo=timezone.utc) if v.tzinfo is None else v for v in stamped)
    if ts > newest:
        return
    raise ValueError(
        f"SCD2 processing timestamp {ts.isoformat()} is not later than the table's newest version "
        f"({newest.isoformat()}). This usually means clock skew between machines or a concurrent "
        f"writer won a conflict - re-run the flow to write with a fresh timestamp."
    )


def _scd2_once(
    df: pl.DataFrame,
    output_path: str,
    *,
    business_keys: list[str],
    valid_from_iso: str,
    compare_columns: list[str] | None,
    full_snapshot: bool,
    surrogate_key_column: str,
    valid_from_column: str,
    valid_to_column: str,
    is_current_column: str,
    partition_by: list[str] | None,
    partition_on_current: bool,
    storage_options: dict[str, str] | None,
) -> Scd2Result:
    """One SCD2 attempt: guard, classify, and commit. Wrapped by the retry shell."""
    import polars as pl_

    system_columns = [surrogate_key_column, valid_from_column, valid_to_column, is_current_column]
    compare = _scd2_guard_input(df, business_keys, system_columns, compare_columns)
    ts = _scd2_parse_iso_utc(valid_from_iso)

    dt = _open_delta_or_none(output_path, storage_options)
    if dt is None:
        return _scd2_initial_load(
            df,
            output_path,
            business_keys,
            ts,
            valid_from_iso,
            system_columns,
            partition_by,
            partition_on_current,
            storage_options,
        )
    if df.height == 0:
        # An empty batch never end-dates anything, full_snapshot included: an upstream that
        # produced nothing is far more likely to be broken than to mean "everything is gone".
        return Scd2Result(skipped=True)

    target_schema = _scd2_validate_target(
        dt, df, output_path, business_keys, compare, system_columns, partition_by, storage_options
    )
    classified, current_rows = _scd2_classify(
        df, output_path, business_keys, compare, target_schema, is_current_column, storage_options
    )

    n_changed = int((classified[_SCD2_CLASS_COLUMN] == "changed").sum())
    n_new = int((classified[_SCD2_CLASS_COLUMN] == "new").sum())
    needs_snapshot_close = bool(full_snapshot and current_rows > classified.height - n_new)
    if n_changed == 0 and n_new == 0 and not needs_snapshot_close:
        return Scd2Result(skipped=True)
    _scd2_guard_clock(output_path, ts, valid_from_column, valid_to_column, storage_options)

    to_insert = classified.filter(pl_.col(_SCD2_CLASS_COLUMN) != "unchanged").drop(_SCD2_CLASS_COLUMN)
    insert_rows = _scd2_stamp(to_insert, business_keys, ts, valid_from_iso, system_columns).with_columns(
        pl_.lit("insert").alias(_SCD2_OP_COLUMN)
    )
    # 'keep' rows exist only to hold unchanged keys out of the not-matched-by-source close-out.
    match_source = classified if full_snapshot else classified.filter(pl_.col(_SCD2_CLASS_COLUMN) == "changed")
    close_rows = (
        match_source.select(business_keys + [_SCD2_CLASS_COLUMN])
        .with_columns(
            pl_.when(pl_.col(_SCD2_CLASS_COLUMN) == "changed")
            .then(pl_.lit("close"))
            .otherwise(pl_.lit("keep"))
            .alias(_SCD2_OP_COLUMN),
            pl_.lit(ts).cast(pl_.Datetime("us", "UTC")).alias(valid_from_column),
        )
        .drop(_SCD2_CLASS_COLUMN)
    )
    staged_columns = list(insert_rows.columns)
    close_padded = close_rows.with_columns(
        [pl_.lit(None).cast(insert_rows.schema[c]).alias(c) for c in staged_columns if c not in close_rows.columns]
    ).select(staged_columns)
    staged = pl_.concat([insert_rows.select(staged_columns), close_padded], how="vertical")

    predicate = (
        " AND ".join(f"target.{_quote_ident(k)} = source.{_quote_ident(k)}" for k in business_keys)
        + f" AND target.{_quote_ident(is_current_column)} = true"
        + f" AND source.{_quote_ident(_SCD2_OP_COLUMN)} <> 'insert'"
    )
    merger = dt.merge(
        source=staged.to_arrow(),
        predicate=predicate,
        source_alias="source",
        target_alias="target",
        merge_schema=True,
    )
    merger = merger.when_matched_update(
        updates={
            _quote_ident(valid_to_column): f"source.{_quote_ident(valid_from_column)}",
            _quote_ident(is_current_column): "false",
        },
        predicate=f"source.{_quote_ident(_SCD2_OP_COLUMN)} = 'close'",
    )
    merger = merger.when_not_matched_insert(
        updates={_quote_ident(c): f"source.{_quote_ident(c)}" for c in staged_columns if c != _SCD2_OP_COLUMN},
        predicate=f"source.{_quote_ident(_SCD2_OP_COLUMN)} = 'insert'",
    )
    if full_snapshot:
        merger = merger.when_not_matched_by_source_update(
            updates={
                _quote_ident(valid_to_column): _scd2_sql_timestamp_literal(ts),
                _quote_ident(is_current_column): "false",
            },
            predicate=f"target.{_quote_ident(is_current_column)} = true",
        )
    metrics = merger.execute()

    scan_kwargs = {"storage_options": storage_options} if storage_options is not None else {}
    after = pl_.scan_delta(output_path, **scan_kwargs)
    return Scd2Result(
        rows_inserted=int(metrics.get("num_target_rows_inserted", 0)),
        rows_closed=int(metrics.get("num_target_rows_updated", 0)),
        rows_total=int(after.select(pl_.len()).collect().item()),
        rows_current=int(after.filter(pl_.col(is_current_column)).select(pl_.len()).collect().item()),
        metrics=metrics,
    )


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

    The cloud analogue of :func:`validate_catalog_path`: same bare-name guards, producing a URI.
    """
    _guard_bare_table_name(table_name)
    return base_uri.rstrip("/") + "/" + table_name
