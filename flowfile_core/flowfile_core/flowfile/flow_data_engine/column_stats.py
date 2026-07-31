"""On-demand single-column statistics for node result previews.

Runs one bounded, single-pass aggregate (len / null_count / n_unique / min /
max / mean) over a node's cached result engine — locally for remote-run
results (a scan over the worker's cache), in the worker for locally-run
results (the full upstream plan). The results are written into the engine's
``FlowfileColumn``, which stays the single source of truth for column
statistics; the endpoint just returns that column's existing ``FileColumn``
representation.
"""

from __future__ import annotations

import copy
import time
from typing import TYPE_CHECKING
from uuid import uuid4

import polars as pl

from flowfile_core.configs import logger
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import (
    ExternalDfFetcher,
    clear_task_from_worker,
)
from flowfile_core.schemas.output_model import FileColumn

if TYPE_CHECKING:
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


def run_stats_query(engine: FlowDataEngine, exprs: list[pl.Expr]) -> dict:
    """Runs the one collect for the stats expressions and logs its duration."""
    lf = engine.data_frame.lazy()
    start = time.perf_counter()
    row = lf.select(exprs).collect(engine="streaming" if engine._streamable else "auto").row(0, named=True)
    elapsed_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(f"column_stats collect finished elapsed_ms={elapsed_ms}")
    return row


def run_stats_query_in_worker(engine: FlowDataEngine, exprs: list[pl.Expr]) -> dict:
    """Ships the stats plan to the worker; core only reads back the 1-row result.

    Used for locally-run nodes, whose cached engine is the full upstream plan —
    collecting that in core would execute the whole pipeline in the API process.
    The worker's spawned child collects ``plan.select(exprs)`` into its cache;
    core then reads a single row from the cached file (bounded, like a preview),
    then best-effort clears the worker-side task so per-request refs don't
    accumulate in a long-lived worker.
    """
    start = time.perf_counter()
    fetcher = ExternalDfFetcher(
        flow_id=-1,
        node_id=-1,
        lf=engine.data_frame.lazy().select(exprs),
        file_ref=f"column_stats_{uuid4().hex}",
        wait_on_completion=True,
    )
    try:
        row = fetcher.get_result().collect().row(0, named=True)
    finally:
        try:
            clear_task_from_worker(fetcher.file_ref)
        except Exception:
            logger.debug("column_stats worker task cleanup failed", exc_info=True)
    elapsed_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(f"column_stats worker collect finished elapsed_ms={elapsed_ms}")
    return row


def _execute_stats_query(engine: FlowDataEngine, column_name: str, dtype: pl.DataType, offload_to_worker: bool) -> dict:
    """Runs the aggregate at one execution site, degrading to counts only.

    The site is chosen once and never switched: a failed worker offload must
    not re-run in core — for locally-run flows the engine is the full upstream
    plan, and collecting it in-process is exactly what the offload exists to
    prevent. Worker failures all surface as plain exceptions (a dead worker is
    indistinguishable from a dtype error), so the count-only retry settles it:
    it is valid for every dtype and fails fast when the worker is down.
    """
    full_exprs = build_stats_exprs(column_name, dtype)
    if offload_to_worker:
        try:
            return run_stats_query_in_worker(engine, full_exprs)
        except Exception as full_error:
            logger.warning(f"column_stats worker full query failed ({full_error}); retrying with counts only")
            try:
                return run_stats_query_in_worker(engine, _count_only_exprs(column_name))
            except Exception as retry_error:
                raise ColumnStatsUnavailable(
                    "Could not compute column statistics in the worker. Is the worker running?"
                ) from retry_error
    try:
        return run_stats_query(engine, full_exprs)
    except pl.exceptions.PolarsError:
        logger.warning(f"column_stats full query failed for {column_name!r}; retrying with counts only")
        return run_stats_query(engine, _count_only_exprs(column_name))


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


def compute_column_stats(engine: FlowDataEngine, column_name: str, offload_to_worker: bool = False) -> FileColumn:
    """Computes stats for one column and applies them to the engine's schema.

    The engine's ``FlowfileColumn`` is the single source of truth: results are
    written onto it (so later previews of this run carry them too, and repeat
    requests answer from it without recomputing) and returned as its ordinary
    ``FileColumn`` representation. Raises
    ``pl.exceptions.ColumnNotFoundError`` for an unknown column and
    ``ColumnStatsUnavailable`` when the worker offload fails. Also memoizes
    the exact row count onto the engine.
    """
    schema = engine.data_frame.lazy().collect_schema()
    if column_name not in schema:
        raise pl.exceptions.ColumnNotFoundError(column_name)
    dtype = schema[column_name]

    memoized = _memoized_stats(engine, column_name, dtype)
    if memoized is not None:
        return memoized

    raw = _execute_stats_query(engine, column_name, dtype, offload_to_worker)
    flow_column = _apply_stats_to_schema(engine, column_name, dtype, raw)
    if engine.known_record_count() is None:
        engine.number_of_records = raw["total_rows"]
    return FileColumn.model_validate(flow_column.get_column_repr())
