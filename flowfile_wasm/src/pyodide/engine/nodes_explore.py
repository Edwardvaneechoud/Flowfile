import gc
from typing import Any

import polars as pl

from .errors import format_error_lf
from .log import log_node, logger
from .state import get_lazyframe, get_schema, store_lazyframe

GW_MAX_ROWS = 100_000


_GW_QUANTITATIVE = {
    "Int8",
    "Int16",
    "Int32",
    "Int64",
    "Int128",
    "UInt8",
    "UInt16",
    "UInt32",
    "UInt64",
    "Float32",
    "Float64",
    "Decimal",
}


_GW_TEMPORAL = {"Date", "Datetime", "Time", "Duration"}


def _gw_semantic_type(pl_dtype) -> str:
    """Map a Polars DataType to a Graphic Walker semanticType."""
    try:
        base = pl_dtype.base_type().__name__
    except Exception:
        base = str(pl_dtype)
    if base in _GW_QUANTITATIVE:
        return "quantitative"
    if base in _GW_TEMPORAL:
        return "temporal"
    return "nominal"


def _gw_analytic_type(semantic_type: str) -> str:
    return "measure" if semantic_type == "quantitative" else "dimension"


def _gw_build_fields(schema) -> list[dict[str, Any]]:
    """Build a MutField list from a Polars schema (OrderedDict[str, DataType])."""
    fields: list[dict[str, Any]] = []
    for name, dtype in schema.items():
        sem = _gw_semantic_type(dtype)
        fields.append(
            {
                "fid": name,
                "key": name,
                "name": name,
                "basename": name,
                "semanticType": sem,
                "analyticType": _gw_analytic_type(sem),
                "disable": False,
            }
        )
    return fields


def _gw_prepare_row(row_dict: dict[str, Any]) -> dict[str, Any]:
    """Make a row JSON-safe for the JS side.

    Polars temporal/bytes values don't survive Pyodide's default dict
    conversion; coerce them to primitive strings/numbers.
    """
    import datetime as _dt

    out: dict[str, Any] = {}
    for k, v in row_dict.items():
        if v is None:
            out[k] = None
        elif isinstance(v, _dt.datetime | _dt.date | _dt.time):
            out[k] = v.isoformat()
        elif isinstance(v, _dt.timedelta):
            out[k] = v.total_seconds()
        elif isinstance(v, bytes | bytearray):
            try:
                out[k] = v.decode("utf-8", errors="replace")
            except Exception:
                out[k] = str(v)
        else:
            out[k] = v
    return out


@log_node
def execute_explore_data(node_id: int, input_id: int, settings: dict) -> dict:
    """Execute explore_data node: materialise up to GW_MAX_ROWS rows from the
    upstream LazyFrame and return a Graphic Walker input payload plus any
    persisted specList from the node settings.
    """
    input_lf = get_lazyframe(input_id)
    if input_lf is None:
        return {
            "success": False,
            "error": (
                f"Explore error on node #{node_id}: No input data from "
                f"node #{input_id}. Make sure the upstream node executed successfully."
            ),
        }

    df = None
    try:
        # Build field metadata from the schema (cheap, no collect).
        schema = input_lf.collect_schema()
        fields = _gw_build_fields(schema)

        # Count rows cheaply for truncation metadata.
        try:
            total_rows = input_lf.select(pl.len()).collect().item()
        except Exception:
            total_rows = None

        truncated = bool(total_rows is not None and total_rows > GW_MAX_ROWS)
        df = input_lf.head(GW_MAX_ROWS).collect()

        # Convert rows to JSON-safe dicts (IRow[] for Graphic Walker).
        rows = [_gw_prepare_row(r) for r in df.to_dicts()]

        # Keep the upstream LazyFrame registered so downstream nodes still work.
        store_lazyframe(node_id, input_lf)

        # Rehydrate any saved chart specs from the node settings.
        spec_list: list[Any] = []
        try:
            gw_input = (settings or {}).get("graphic_walker_input") or {}
            spec_list = gw_input.get("specList") or []
            if not isinstance(spec_list, list):
                spec_list = []
        except Exception:
            spec_list = []

        is_initial = len(spec_list) == 0

        return {
            "success": True,
            "schema": get_schema(node_id),
            "has_data": True,
            "graphic_walker_input": {
                "is_initial": is_initial,
                "dataModel": {
                    "fields": fields,
                    "data": rows,
                },
                "specList": spec_list,
            },
            "row_info": {
                "total_rows": total_rows,
                "loaded_rows": len(rows),
                "truncated": truncated,
                "max_rows": GW_MAX_ROWS,
            },
        }
    except Exception as e:
        return {
            "success": False,
            "error": format_error_lf("explore_data", node_id, e, input_lf),
        }
    finally:
        if df is not None:
            del df
            gc.collect()


def prepare_visual_data(csv_content: str, max_rows: int = GW_MAX_ROWS) -> dict:
    """Build a Graphic Walker payload from a CSV string for the catalog Visuals
    feature. Standalone (no LazyFrame store): parse the dataset, derive fields
    from its schema, and materialise up to max_rows JSON-safe rows. Mirrors
    execute_explore_data's field/row extraction without the DAG plumbing.
    """
    df = None
    try:
        import io
        import time

        t0 = time.perf_counter()
        lf = pl.read_csv(io.StringIO(csv_content)).lazy()
        schema = lf.collect_schema()
        fields = _gw_build_fields(schema)

        try:
            total_rows = lf.select(pl.len()).collect().item()
        except Exception:
            total_rows = None

        truncated = bool(total_rows is not None and total_rows > max_rows)
        df = lf.head(max_rows).collect()
        rows = [_gw_prepare_row(r) for r in df.to_dicts()]
        logger.info(
            "prepare_visual_data rows=%d/%s ok (%.0fms)", len(rows), total_rows, (time.perf_counter() - t0) * 1000
        )

        return {
            "success": True,
            "fields": fields,
            "data": rows,
            "row_info": {
                "total_rows": total_rows,
                "loaded_rows": len(rows),
                "truncated": truncated,
                "max_rows": max_rows,
            },
        }
    except Exception as e:
        logger.warning("prepare_visual_data failed: %s", e)
        return {"success": False, "error": f"Failed to prepare visualization data: {e}"}
    finally:
        if df is not None:
            del df
            gc.collect()
