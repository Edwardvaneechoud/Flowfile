"""Table display payload for the notebook UI (``application/vnd.flowfile.table+json``).

Mirrors the table helpers in ``kernel_runtime/kernel_runtime/flowfile_client.py``. The kernel image
ships only ``kernel_runtime/``, so the kernel keeps its own copy; the two are pinned
equal by ``shared/tests/test_notebook_display.py``. The dtype -> field mapping mirrors
flowfile_wasm's explore node.
"""

from __future__ import annotations

import datetime
import json
import math
from decimal import Decimal
from typing import Any

import polars as pl

TABLE_MIME = "application/vnd.flowfile.table+json"
DISPLAY_MAX_ROWS = 2_000

_QUANTITATIVE = {
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
_TEMPORAL = {"Date", "Datetime", "Time", "Duration"}


def semantic_type(dtype: Any) -> str:
    """Map a Polars DataType to a Graphic Walker semanticType."""
    try:
        base = dtype.base_type().__name__
    except Exception:
        base = str(dtype)
    if base in _QUANTITATIVE:
        return "quantitative"
    if base in _TEMPORAL:
        return "temporal"
    return "nominal"


def build_fields(schema: Any) -> list[dict[str, Any]]:
    """Build a Graphic Walker IMutField list from a Polars schema."""
    fields: list[dict[str, Any]] = []
    for name, dtype in schema.items():
        sem = semantic_type(dtype)
        fields.append(
            {
                "fid": name,
                "key": name,
                "name": name,
                "basename": name,
                "semanticType": sem,
                "analyticType": "measure" if sem == "quantitative" else "dimension",
                "disable": False,
            }
        )
    return fields


def build_table_payload(df: pl.DataFrame, max_rows: int, total_rows: int | None = None) -> dict[str, Any]:
    """Build the table payload from the first ``max_rows`` rows of ``df``.

    Pure over an eager frame: a caller holding a lazy lineage collects the head itself
    and passes the full row count as ``total_rows`` when it knows it.
    """
    head = df.head(max_rows)
    loaded_rows = head.height
    if total_rows is None:
        total_rows = df.height
    return {
        "columns": head.columns,
        "fields": build_fields(head.schema),
        "data": head.to_dicts(),
        "total_rows": total_rows,
        "loaded_rows": loaded_rows,
        "truncated": bool(total_rows > loaded_rows),
        "max_rows": max_rows,
    }


def _json_default(value: Any) -> Any:
    """json.dumps fallback for non-serializable Polars cells (temporal/decimal/bytes)."""
    if isinstance(value, datetime.datetime | datetime.date | datetime.time):
        return value.isoformat()
    if isinstance(value, datetime.timedelta):
        return value.total_seconds()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, bytes | bytearray):
        return bytes(value).decode("utf-8", errors="replace")
    return str(value)


def _sanitize_non_finite(value: Any) -> Any:
    """Replace NaN/Inf floats with None (recurses) so the JSON parses in JS."""
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, list):
        return [_sanitize_non_finite(v) for v in value]
    if isinstance(value, dict):
        return {k: _sanitize_non_finite(v) for k, v in value.items()}
    return value


def dump_table_payload(payload: dict[str, Any]) -> str:
    """Serialize a table payload to a JSON string the frontend can ``JSON.parse``."""
    try:
        return json.dumps(payload, default=_json_default, allow_nan=False)
    except ValueError:
        payload["data"] = [_sanitize_non_finite(row) for row in payload["data"]]
        return json.dumps(payload, default=_json_default, allow_nan=False)
