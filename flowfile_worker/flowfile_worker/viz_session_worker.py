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
from flowfile_worker.funcs import _validate_catalog_path, _validate_virtual_results_path
from shared.delta_utils import make_json_safe

logger = logging.getLogger(__name__)


def _build_sql_lazyframe_in_child(
    sql_query: str,
    tables: dict[str, str] | None,
    virtual_refs: dict[str, str] | None,
) -> pl.LazyFrame:
    ctx = pl.SQLContext()
    for name, dir_name in (tables or {}).items():
        p = _validate_catalog_path(dir_name)
        if not p.is_dir() or not (p / "_delta_log").is_dir():
            raise ValueError(f"Table '{name}' is not a valid Delta table")
        ctx.register(name, pl.scan_delta(str(p)))
    if virtual_refs:
        for name, ipc_name in virtual_refs.items():
            p = _validate_virtual_results_path(ipc_name)
            ctx.register(name, pl.scan_ipc(str(p)))
    return ctx.execute(sql_query)


def _build_viz_loader_in_child(source: models.VizWorkerSource) -> pl.LazyFrame:
    if source.kind == "physical":
        if source.table_path is None:
            raise ValueError("table_path is required for physical source")
        p = _validate_catalog_path(source.table_path)
        storage_format = source.storage_format or "delta"
        if storage_format == "delta" or (p.is_dir() and (p / "_delta_log").is_dir()):
            return pl.scan_delta(str(p))
        return pl.scan_parquet(p)
    if source.kind == "sql":
        if not source.sql_query:
            raise ValueError("sql_query is required for sql source")
        return _build_sql_lazyframe_in_child(source.sql_query, source.tables, source.virtual_refs)
    if source.kind == "ipc_path":
        if not source.ipc_path:
            raise ValueError("ipc_path is required for ipc_path source")
        p = _validate_virtual_results_path(source.ipc_path)
        return pl.scan_ipc(str(p))
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
        "[viz-child] session ready key=%s polars_gw=%s load_ms=%.1f",
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
            else:
                raise ValueError(f"unknown op: {op!r}")
            response_q.put({"request_id": rid, "ok": True, "result": result})
        except ValueError as exc:
            response_q.put({"request_id": rid, "ok": False, "error": str(exc)[:1024], "type": "ValueError"})
        except Exception as exc:
            response_q.put({"request_id": rid, "ok": False, "error": str(exc)[:1024], "type": type(exc).__name__})
