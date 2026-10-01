"""Payload-equality guard: shared/notebook_display.py must match the kernel's copy.

The kernel builder is read from ``kernel_runtime/kernel_runtime/flowfile_client.py`` with
``ast`` and exec'd in isolation (only the payload helpers and ``_GW_*`` constants), because
``kernel_runtime`` is not importable on the core test path; same approach as
``test_dry_run_ctx_parity.py``.
"""

import ast
import datetime
import json
import math
from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest

from shared import notebook_display

_KERNEL_FUNCS = {
    "_gw_semantic_type",
    "_gw_analytic_type",
    "_gw_build_fields",
    "_json_default",
    "_sanitize_non_finite",
    "_build_table_payload",
    "_dump_table_payload",
}


def _kernel_namespace() -> dict:
    root = Path(__file__).resolve()
    while not (root / "kernel_runtime").is_dir():
        root = root.parent
        assert root != root.parent, "could not locate the repo root"
    src = (root / "kernel_runtime" / "kernel_runtime" / "flowfile_client.py").read_text(encoding="utf-8")
    body = []
    for stmt in ast.parse(src).body:
        if isinstance(stmt, ast.FunctionDef) and stmt.name in _KERNEL_FUNCS:
            body.append(stmt)
        elif isinstance(stmt, ast.Assign | ast.AnnAssign):
            targets = stmt.targets if isinstance(stmt, ast.Assign) else [stmt.target]
            if any(isinstance(t, ast.Name) and t.id.startswith("_GW_") for t in targets):
                body.append(stmt)
    found = {s.name for s in body if isinstance(s, ast.FunctionDef)}
    assert found == _KERNEL_FUNCS, f"kernel payload helpers moved or renamed: missing {_KERNEL_FUNCS - found}"
    ns = {"pl": pl, "json": json, "math": math, "datetime": datetime, "Decimal": Decimal, "Any": object}
    exec(compile(ast.Module(body=body, type_ignores=[]), "flowfile_client.py", "exec"), ns)
    return ns


KERNEL = _kernel_namespace()

FRAMES = {
    "ints": pl.DataFrame({"a": [1, 2, 3], "b": pl.Series([1, 2, 3], dtype=pl.UInt8)}),
    "floats": pl.DataFrame({"x": [1.5, float("nan"), float("inf"), None]}),
    "strings": pl.DataFrame({"s": ["a", "b", None], "cat": pl.Series(["x", "y", "x"], dtype=pl.Categorical)}),
    "nulls": pl.DataFrame({"n": [None, None], "i": [None, 1]}),
    "dates": pl.DataFrame(
        {
            "d": [datetime.date(2024, 1, 1), None],
            "dt": [datetime.datetime(2024, 1, 1, 12, 30), datetime.datetime(2025, 6, 1)],
            "dur": [datetime.timedelta(seconds=5), None],
            "dec": pl.Series([Decimal("1.25"), None], dtype=pl.Decimal(10, 2)),
        }
    ),
    "lists": pl.DataFrame({"l": [[1, 2], [], None], "st": [{"k": 1}, {"k": None}, None]}),
    "empty": pl.DataFrame(schema={"a": pl.Int64, "s": pl.String}),
    "above_cap": pl.DataFrame({"i": list(range(250)), "f": [i / 3 for i in range(250)]}),
}


@pytest.mark.parametrize("name", list(FRAMES))
@pytest.mark.parametrize("max_rows", [100, 2_000])
def test_shared_payload_equals_kernel_payload(name, max_rows):
    df = FRAMES[name]
    shared = notebook_display.build_table_payload(df, max_rows)
    kernel = KERNEL["_build_table_payload"](df, max_rows)
    assert {k: v for k, v in shared.items() if k != "data"} == {k: v for k, v in kernel.items() if k != "data"}
    assert notebook_display.dump_table_payload(shared) == KERNEL["_dump_table_payload"](kernel)


def test_lazy_head_with_known_total_matches_kernel_lazy_path():
    lf = FRAMES["above_cap"].lazy()
    shared = notebook_display.build_table_payload(lf.head(100).collect(), 100, total_rows=250)
    kernel = KERNEL["_build_table_payload"](lf, 100)
    assert notebook_display.dump_table_payload(shared) == KERNEL["_dump_table_payload"](kernel)


def test_above_cap_is_truncated():
    payload = notebook_display.build_table_payload(FRAMES["above_cap"], 100)
    assert payload["truncated"] is True
    assert (payload["loaded_rows"], payload["total_rows"]) == (100, 250)


def test_mime_matches_kernel():
    assert notebook_display.TABLE_MIME == KERNEL["_GW_TABLE_MIME"]
