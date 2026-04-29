"""Spawned-child entry point for viz session compute.

Imported only inside ``mp_context.Process(target=...)`` — every import below
runs in the spawned child, never in the FastAPI process.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import polars as pl
import polars_gw

from flowfile_worker import models
from flowfile_worker.catalog_reader import open_catalog_table, open_virtual_result
from shared.delta_utils import make_json_safe

logger = logging.getLogger(__name__).getChild("viz")


def _build_sql_lazyframe_in_child(
    sql_query: str,
    tables: dict[str, str] | None,
    virtual_refs: dict[str, str] | None,
) -> pl.LazyFrame:
    ctx = pl.SQLContext()
    for name, dir_name in (tables or {}).items():
        ctx.register(name, open_catalog_table(dir_name))
    if virtual_refs:
        for name, ipc_name in virtual_refs.items():
            ctx.register(name, open_virtual_result(ipc_name))
    return ctx.execute(sql_query)


def _build_viz_loader_in_child(source: models.VizWorkerSource) -> pl.LazyFrame:
    if source.kind == "physical":
        if source.table_path is None:
            raise ValueError("table_path is required for physical source")
        return open_catalog_table(source.table_path)
    if source.kind == "sql":
        if not source.sql_query:
            raise ValueError("sql_query is required for sql source")
        return _build_sql_lazyframe_in_child(source.sql_query, source.tables, source.virtual_refs)
    if source.kind == "ipc_path":
        if not source.ipc_path:
            raise ValueError("ipc_path is required for ipc_path source")
        return open_virtual_result(source.ipc_path)
    raise ValueError(f"Unknown viz source kind: {source.kind}")


def _execute(lf: pl.LazyFrame, payload: dict, max_rows: int) -> dict:
    start = time.perf_counter()
    rows = polars_gw.execute_workflow(lf, payload, max_rows=max_rows)
    elapsed = (time.perf_counter() - start) * 1000
    safe_rows = [{k: make_json_safe(v) for k, v in row.items()} for row in rows]
    return {
        "rows": safe_rows,
        "total_rows": len(safe_rows),
        "truncated": len(safe_rows) >= max_rows,
        "elapsed_ms": round(elapsed, 1),
    }


_NUMERIC_DTYPES = (
    pl.Int8, pl.Int16, pl.Int32, pl.Int64,
    pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
    pl.Float32, pl.Float64, pl.Decimal,
)
_TEMPORAL_DTYPES = (pl.Date, pl.Datetime, pl.Time, pl.Duration)


def _column_stats(lf: pl.LazyFrame, column: str, limit: int) -> dict:
    """Distinct values + min/max for a single column.

    Fetches up to ``limit + 1`` distinct values to detect truncation, plus
    a separate ``min``/``max`` aggregation for numeric / temporal columns
    so the UI can pre-fill range inputs even when there are many distinct
    values to enumerate.
    """
    schema = lf.collect_schema()
    if column not in schema:
        raise ValueError(f"column {column!r} not found in source schema")
    dtype = schema[column]
    is_numeric = isinstance(dtype, _NUMERIC_DTYPES)
    is_temporal = isinstance(dtype, _TEMPORAL_DTYPES)

    fetch = max(1, limit) + 1
    distinct_df = (
        lf.select(pl.col(column).drop_nulls().unique()).sort(column).head(fetch).collect()
    )
    raw_values = distinct_df[column].to_list()
    truncated = len(raw_values) > limit
    values = [make_json_safe(v) for v in raw_values[:limit]]
    distinct_count = None if truncated else len(values)

    min_v: object | None = None
    max_v: object | None = None
    if is_numeric or is_temporal:
        agg = lf.select(
            pl.col(column).min().alias("__min"),
            pl.col(column).max().alias("__max"),
        ).collect()
        min_v = make_json_safe(agg["__min"][0]) if agg.height else None
        max_v = make_json_safe(agg["__max"][0]) if agg.height else None

    return {
        "dtype": str(dtype),
        "values": values,
        "truncated": truncated,
        "distinct_count": distinct_count,
        "min": min_v,
        "max": max_v,
    }


def viz_session_main(source: dict[str, Any], request_q, response_q) -> None:
    """Long-lived child loop. One instance per session_key.

    Bootstraps the LazyFrame, then services execute/fields/shutdown ops over
    the queue pair. On bootstrap failure, emits a fatal-load message and
    exits — the registry treats this child as dead.
    """
    try:
        viz_source = models.VizWorkerSource(**source)
    except Exception as exc:
        response_q.put({"ok": False, "fatal": True, "error": str(exc)[:1024], "type": "load"})
        return

    load_start = time.perf_counter()
    try:
        lf = _build_viz_loader_in_child(viz_source)
    except Exception as exc:
        response_q.put({"ok": False, "fatal": True, "error": str(exc)[:1024], "type": "load"})
        return
    load_ms = (time.perf_counter() - load_start) * 1000
    logger.info(
        "session ready key=%s polars_gw=%s load_ms=%.1f",
        viz_source.session_key,
        getattr(polars_gw, "__version__", "?"),
        load_ms,
    )

    fields_cache: list[dict] | None = None

    while True:
        msg = request_q.get()
        op = msg.get("op")
        if op == "shutdown":
            try:
                response_q.put({"ok": True, "result": "bye"})
            except Exception:
                pass
            return
        rid = msg.get("request_id")
        try:
            if op == "execute":
                result: Any = _execute(lf, msg["payload"], msg.get("max_rows", 100_000))
            elif op == "fields":
                if fields_cache is None:
                    fields_cache = polars_gw.get_fields(lf)
                result = {"fields": fields_cache}
            elif op == "column_stats":
                payload = msg.get("payload") or {}
                result = _column_stats(
                    lf, payload["column"], int(payload.get("limit") or 100)
                )
            else:
                raise ValueError(f"unknown op: {op!r}")
            response_q.put({"request_id": rid, "ok": True, "result": result})
        except ValueError as exc:
            response_q.put({"request_id": rid, "ok": False, "error": str(exc)[:1024], "type": "ValueError"})
        except Exception as exc:
            response_q.put({"request_id": rid, "ok": False, "error": str(exc)[:1024], "type": type(exc).__name__})
