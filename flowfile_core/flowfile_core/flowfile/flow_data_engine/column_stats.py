"""On-demand single-column statistics for node result previews.

Runs one bounded, single-pass aggregate (len / null_count / n_unique / min /
max / mean) over a node's cached result engine on the compute backend it is
given, and reads back the one row it yields (``bounded.single_row``): through
``LocalBackend`` the aggregate runs in core, through ``WorkerBackend`` in a
worker child, over the scan of a stored result or the full upstream plan of a
deferred one. The results are written into the engine's ``FlowfileColumn``,
which stays the single source of truth for column statistics; the endpoint
just returns that column's existing ``FileColumn`` representation.
"""

from __future__ import annotations

import copy
import time
from typing import TYPE_CHECKING

import polars as pl

from flowfile_core.configs import logger
from flowfile_core.flowfile.execution.errors import ComputeError
from flowfile_core.flowfile.flow_data_engine.bounded import single_row
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.schemas.output_model import FileColumn

if TYPE_CHECKING:
    from flowfile_core.flowfile.execution.protocol import ComputeBackend
    from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine


class ColumnStatsUnavailable(Exception):
    """Raised when stats cannot be computed without executing or re-pulling data."""


def _supports_unique(dtype: pl.DataType) -> bool:
    return dtype not in (pl.Object, pl.Unknown)


def _supports_min_max(dtype: pl.DataType) -> bool:
    return not (dtype.is_nested() or dtype in (pl.Object, pl.Unknown, pl.Null))


def _supports_mean(dtype: pl.DataType) -> bool:
    return dtype.is_numeric()


def build_stats_exprs(column_name: str, dtype: pl.DataType) -> list[pl.Expr]:
    """Builds the single-pass aggregation expressions for one column."""
    col = pl.col(column_name)
    exprs = [pl.len().alias("total_rows"), col.null_count().alias("null_count")]
    if _supports_unique(dtype):
        # drop_nulls first: Polars counts null as a distinct value.
        exprs.append(col.drop_nulls().n_unique().alias("unique_count"))
    if _supports_min_max(dtype):
        # Cast categoricals to string so ordering doesn't depend on the string cache.
        ordered = col.cast(pl.String) if dtype in (pl.Categorical, pl.Enum) else col
        exprs.append(ordered.min().alias("min_value"))
        exprs.append(ordered.max().alias("max_value"))
    if _supports_mean(dtype):
        exprs.append(col.mean().alias("average_value"))
    return exprs


def _count_only_exprs(column_name: str) -> list[pl.Expr]:
    """The degraded expression set, valid for every dtype."""
    return [pl.len().alias("total_rows"), pl.col(column_name).null_count().alias("null_count")]


def run_stats_query(backend: ComputeBackend, engine: FlowDataEngine, exprs: list[pl.Expr]) -> dict:
    """Runs the stats aggregate on ``backend``, reads back its one row and logs the duration.

    The aggregate always yields one row; the backend computes it and only that
    row reaches core.
    """
    start = time.perf_counter()
    row = single_row(
        backend,
        engine.data_frame.lazy().select(exprs),
        empty_message="The column statistics aggregate returned no row.",
    )
    elapsed_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(f"column_stats on backend {backend.id!r} finished elapsed_ms={elapsed_ms}")
    return row


def _execute_stats_query(engine: FlowDataEngine, column_name: str, dtype: pl.DataType, backend: ComputeBackend) -> dict:
    """Runs the aggregate on ``backend``, degrading to counts only.

    The site is the backend and is never switched: a failed query on the
    worker must not re-run in core, where for a locally-run node the engine is
    the full upstream plan. A failed full query (a ``ComputeError`` from the
    backend, or a Polars error such as a dtype the aggregate cannot take)
    retries with the count-only expressions on the same backend: they are
    valid for every dtype and fail fast when the worker is down. When that
    retry fails with a ``ComputeError`` the statistics are unavailable; any
    other error of the retry propagates.
    """
    try:
        return run_stats_query(backend, engine, build_stats_exprs(column_name, dtype))
    except (ComputeError, pl.exceptions.PolarsError) as full_error:
        logger.warning(
            f"column_stats full query failed on backend {backend.id!r} ({full_error}); retrying with counts only"
        )
    try:
        return run_stats_query(backend, engine, _count_only_exprs(column_name))
    except ComputeError as retry_error:
        raise ColumnStatsUnavailable(
            f"Could not compute column statistics on the {backend.id!r} backend: {retry_error}"
        ) from retry_error


def _memoized_stats(engine: FlowDataEngine, column_name: str, dtype: pl.DataType) -> FileColumn | None:
    """Returns the already-applied stats when this engine's column holds them.

    A fresh run builds a fresh engine and schema, so the memo self-invalidates.
    A count-only degraded result (unique unknown on a dtype that supports it)
    is not memoized — the next click retries the full aggregate.
    """
    for column in engine.schema or []:
        if column.column_name == column_name:
            if not column.stats_applied:
                return None
            if _supports_unique(dtype) and FlowfileColumn._known_count(column.number_of_unique_values) is None:
                return None
            return FileColumn.model_validate(column.get_column_repr())
    return None


def _apply_stats_to_schema(engine: FlowDataEngine, column_name: str, dtype: pl.DataType, raw: dict) -> FlowfileColumn:
    """Writes the computed stats onto a private copy of the engine's column.

    Schema lists are shared by reference across engines (samples, splits,
    shallow copies), so the shared objects are never mutated: the enriched copy
    replaces the original in a fresh list bound to this engine only — sibling
    output handles and up/downstream nodes keep their own untouched columns.
    """
    columns = list(engine.schema or [])
    index = next((i for i, c in enumerate(columns) if c.column_name == column_name), None)
    if index is None:
        flow_column = FlowfileColumn.create_from_polars_dtype(column_name, dtype)
        columns.append(flow_column)
    else:
        flow_column = copy.copy(columns[index])
        columns[index] = flow_column
    flow_column.apply_statistics(
        total_rows=raw["total_rows"],
        null_count=raw["null_count"],
        n_unique=raw.get("unique_count"),
        min_value=raw.get("min_value"),
        max_value=raw.get("max_value"),
        average_value=raw.get("average_value"),
    )
    engine._schema = columns
    return flow_column


def compute_column_stats(engine: FlowDataEngine, column_name: str, backend: ComputeBackend) -> FileColumn:
    """Computes stats for one column on ``backend`` and applies them to the engine's schema.

    The engine's ``FlowfileColumn`` is the single source of truth: results are
    written onto it (so later previews of this run carry them too, and repeat
    requests answer from it without recomputing) and returned as its ordinary
    ``FileColumn`` representation. Raises
    ``pl.exceptions.ColumnNotFoundError`` for an unknown column and
    ``ColumnStatsUnavailable`` when the backend cannot compute even the
    counts. Also memoizes the exact row count onto the engine.
    """
    schema = engine.data_frame.lazy().collect_schema()
    if column_name not in schema:
        raise pl.exceptions.ColumnNotFoundError(column_name)
    dtype = schema[column_name]

    memoized = _memoized_stats(engine, column_name, dtype)
    if memoized is not None:
        return memoized

    raw = _execute_stats_query(engine, column_name, dtype, backend)
    flow_column = _apply_stats_to_schema(engine, column_name, dtype, raw)
    if engine.known_record_count() is None:
        engine.number_of_records = raw["total_rows"]
    return FileColumn.model_validate(flow_column.get_column_repr())
