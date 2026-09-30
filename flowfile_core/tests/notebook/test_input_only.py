"""Frame methods a cell may call although the render never writes them (``allowlist.INPUT_ONLY``).

Each builds the node the frame builds for it, through the interpreter exactly as through ``exec``, and the
edited cells together use every input-only entry. A sync of them reads and runs nothing beyond a literal
source's own rows, a call the frame cannot hold as code fails on its line as ``exec`` reports it, and the
render's own check accepts none of them. In a sync the Polars Code node the frame's ``lazy_methods`` wrapper
builds for such a call takes the columns Polars plans for it, so the calls below it build checked.
"""

from __future__ import annotations

import polars as pl
import pytest

from flowfile_core.notebook import allowlist
from flowfile_core.notebook.interpret import CellInterpreter, interprets_expression
from flowfile_frame.notebook_cells import clean_run
from tests.notebook.conftest import ExecRunner, masked_payload, no_kernel_manager
from tests.notebook.test_sync_io import (  # noqa: F401  (database_connection is a fixture)
    DATABASE_CONNECTION,
    LITERAL_COLLECTS,
    _Calls,
    _collects_beyond,
    _empty_flow,
    _names,
    _record_io,
    _snapshot_session,
    database_connection,
)
from tests.notebook.test_sync_io import _run as _run_in

IMPORTS = "import flowfile as fl"
SOURCE = (
    "src = fl.from_raw_data({'columns': [{'name': 'a', 'data_type': 'Integer'}, "
    "{'name': 'b', 'data_type': 'String'}], 'data': [[1, 2, 3], ['x', 'y', 'z']]})"
)
EDITED: dict[str, tuple[str, str]] = {
    "tail": ("src.tail(2)", "polars_code"),
    "limit": ("src.limit(2)", "sample"),
    "slice": ("src.slice(1, 2)", "polars_code"),
    "shift": ("src.shift(1, fill_value=0)", "polars_code"),
    "reverse": ("src.reverse()", "polars_code"),
    "first": ("src.first()", "polars_code"),
    "last": ("src.last()", "polars_code"),
    "drop_nulls": ("src.drop_nulls(subset=['a'])", "polars_code"),
    "drop_nans": ("src.drop_nans()", "polars_code"),
    "fill_null": ("src.fill_null(0)", "polars_code"),
    "fill_nan": ("src.fill_nan(0)", "polars_code"),
    "interpolate": ("src.interpolate()", "polars_code"),
    "max": ("src.max()", "polars_code"),
    "min": ("src.min()", "polars_code"),
    "sum": ("src.sum()", "polars_code"),
    "mean": ("src.mean()", "polars_code"),
    "median": ("src.median()", "polars_code"),
    "std": ("src.std(ddof=0)", "polars_code"),
    "var": ("src.var()", "polars_code"),
    "null_count": ("src.null_count()", "polars_code"),
    "quantile": ("src.quantile(0.5)", "polars_code"),
    "melt": ("src.melt(id_vars=['b'])", "polars_code"),
    "explode": ("src.explode('a')", "polars_code"),
    "cast": ("src.cast({'a': fl.Float64})", "polars_code"),
    "count": ("src.count()", "polars_code"),
    "unnest": ("src.unnest('a')", "polars_code"),
    "gather_every": ("src.gather_every(2)", "polars_code"),
    "top_k": ("src.top_k(2, by='a')", "polars_code"),
    "bottom_k": ("src.bottom_k(2, by=fl.col('a'))", "polars_code"),
    "sql": ("src.sql('SELECT a FROM self')", "sql_query"),
    "sink_csv": ("src.sink_csv('/nonexistent/input_only.csv')", "output"),
    "sink_ipc": ("src.sink_ipc('/nonexistent/input_only.arrow')", "output"),
    "sink_ndjson": ("src.sink_ndjson('/nonexistent/input_only.ndjson')", "output"),
    "write_ipc": ("src.write_ipc('/nonexistent/written.arrow')", "output"),
    "write_ndjson": ("src.write_ndjson('/nonexistent/written.ndjson')", "output"),
    "write_avro": ("src.write_avro('/nonexistent/written.avro')", "output"),
    "write_catalog_table": ("src.write_catalog_table('input_only_table')", "catalog_writer"),
    "write_delta": ("src.write_delta('s3://bucket/input_only')", "cloud_storage_writer"),
    "write_parquet_to_cloud_storage": (
        "src.write_parquet_to_cloud_storage('s3://bucket/input_only.parquet')",
        "cloud_storage_writer",
    ),
    "write_csv_to_cloud_storage": (
        "src.write_csv_to_cloud_storage('s3://bucket/input_only.csv')",
        "cloud_storage_writer",
    ),
    "write_json_to_cloud_storage": (
        "src.write_json_to_cloud_storage('s3://bucket/input_only.json')",
        "cloud_storage_writer",
    ),
    "write_database": (f"src.write_database({DATABASE_CONNECTION!r}, 'orders')", "database_writer"),
}
"""Each input-only frame entry -> a call over ``src`` using it, and the node type that call places."""


def edited_cell(names) -> str:
    """One cell binding ``out_<name>`` to the edited call of each of ``names``; it reads ``src`` from ``SOURCE``."""
    return "\n".join(f"out_{name} = {EDITED[name][0]}" for name in names)


def _run(cells: list[str], executor) -> dict:
    with no_kernel_manager():
        return clean_run([(f"cell-{i}", code) for i, code in enumerate(cells)], ceiling=0, user_id=1, executor=executor)


def _both(cells: list[str], interpreter: CellInterpreter | None = None) -> tuple[dict, dict]:
    return _run(cells, interpreter or CellInterpreter()), _run(cells, ExecRunner.executor())


def _types(result: dict) -> list[str]:
    return [node["type"] for node in sorted(result["flowfile_data"]["nodes"], key=lambda node: node["id"])]


def test_every_input_only_entry_has_an_edited_cell():
    assert set(EDITED) == set(allowlist.INPUT_ONLY["FlowFrame"])


def test_a_scan_unique_select_limit_chain_ends_in_a_sample_node(tmp_path):
    path = tmp_path / "rows.csv"
    pl.DataFrame({"a": [1, 1, 2], "b": ["x", "y", "z"]}).write_csv(path)
    cell = f"rows = fl.scan_csv({str(path)!r}).unique(['a']).select(['a', 'b']).limit(1)"
    interpreted, executed = _both([IMPORTS, cell])
    assert executed["ok"], executed.get("message")
    assert interpreted["ok"], (interpreted.get("line"), interpreted.get("message"))
    assert masked_payload(interpreted) == masked_payload(executed)
    assert _types(interpreted) == ["read", "unique", "select", "sample"]
    sample = next(node for node in interpreted["flowfile_data"]["nodes"] if node["type"] == "sample")
    assert sample["setting_input"]["sample_size"] == 1


@pytest.mark.parametrize("name", sorted(EDITED))
def test_an_input_only_call_builds_the_frames_node_as_exec_does(name, database_connection):
    call, node_type = EDITED[name]
    interpreter = CellInterpreter()
    interpreted, executed = _both([IMPORTS, SOURCE, f"out = {call}"], interpreter)
    assert executed["ok"], executed.get("message")
    assert interpreted["ok"], (interpreted.get("line"), interpreted.get("message"))
    assert masked_payload(interpreted) == masked_payload(executed)
    assert _types(interpreted) == ["manual_input", node_type]
    assert interpreter.used_input_only == {("FlowFrame", name)}


def test_a_sync_of_every_input_only_call_reads_connects_and_runs_nothing(database_connection, monkeypatch):
    calls = _Calls()
    held_ran = _record_io(monkeypatch, calls)
    result = _run([IMPORTS, SOURCE, edited_cell(EDITED)], CellInterpreter())
    assert result["ok"], (result.get("line"), result.get("message"))
    assert len(result["flowfile_data"]["nodes"]) == len(EDITED) + 1
    assert held_ran == []
    assert _collects_beyond(calls, set(LITERAL_COLLECTS)) == []
    assert calls.labels() <= {"collect"}, sorted(calls.labels())


UNPLANNED = {"explode": "it writes its own text", "unnest": "'a' holds no struct, so Polars refuses the plan"}


def test_every_input_only_polars_code_call_is_seeded_with_the_columns_polars_plans():
    names = [name for name, (_, node_type) in EDITED.items() if node_type == "polars_code"]
    with _snapshot_session({"flowfile_data": _empty_flow(), "schemas": {}}, {}) as (mode, namespace):
        _run_in(namespace, "cell-0", f"{IMPORTS}\n{SOURCE}\n{edited_cell(names)}")
        column_less = {name for name in names if namespace[f"out_{name}"].node_id in mode.column_less}
        assert _names(namespace["out_melt"]) == ["b", "variable", "value"]
    assert column_less == set(UNPLANNED)


def test_a_planned_node_lets_the_calls_below_it_build_checked_as_exec_does():
    cell = "dropped = src.tail(2).drop('a')\nkept = src.tail(2).filter(fl.col('a') > 1)"
    interpreted, executed = _both([IMPORTS, SOURCE, cell])
    assert interpreted["ok"] and executed["ok"], (interpreted.get("message"), executed.get("message"))
    assert masked_payload(interpreted) == masked_payload(executed)
    assert _types(interpreted) == ["manual_input", "polars_code", "select", "polars_code", "filter"]
    assert interpreted["warnings"] == executed["warnings"] == []


FAILURES = {
    "frame_argument": ("out = src.tail(src)", "tail takes no frame as an argument"),
    "frame_in_a_list": ("out = src.drop_nulls(subset=['a', src])", "drop_nulls takes no frame as an argument"),
    "frame_in_a_dict": ("out = src.cast({'a': src})", "cast takes no frame as an argument"),
    "frame_to_explode": ("out = src.explode(src)", "explode takes no frame as an argument"),
    "unknown_keyword": ("out = src.tail(nope=1)", "tail() got an unexpected keyword argument 'nope'"),
    "missing_argument": ("out = src.slice()", "slice() missing a required argument: 'offset'"),
    "expression_build_keyword": (
        "out = src.with_columns(fl.col('a').sqrt(convertable_to_code=False))",
        "sqrt() got an unexpected keyword argument 'convertable_to_code'",
    ),
    "expression_formula_keyword": (
        "out = src.with_columns(fl.col('a').sqrt(ff_repr='[b]'))",
        "sqrt() got an unexpected keyword argument 'ff_repr'",
    ),
}


@pytest.mark.parametrize("name", sorted(FAILURES))
def test_a_call_the_frame_cannot_hold_as_code_fails_on_its_line_as_exec_reports_it(name):
    code, message = FAILURES[name]
    interpreted, executed = _both([IMPORTS, SOURCE, code])
    assert (interpreted["ok"], interpreted["cell_id"], interpreted["line"]) == (False, "cell-2", 1)
    assert message in interpreted["message"], interpreted["message"]
    report = ("cell_id", "line", "kind", "message")
    assert {key: interpreted[key] for key in report} == {key: executed[key] for key in report}


def test_text_that_reads_like_a_lambda_stays_the_nodes_code():
    interpreted, executed = _both([IMPORTS, SOURCE, "out = src.fill_null('<lambda> at 0x0')"])
    assert interpreted["ok"] and executed["ok"]
    assert masked_payload(interpreted) == masked_payload(executed)
    code = next(n for n in interpreted["flowfile_data"]["nodes"] if n["type"] == "polars_code")["setting_input"]
    assert code["polars_code_input"]["polars_code"] == "output_df = input_df.fill_null('<lambda> at 0x0')"


def test_the_render_check_accepts_no_input_only_call():
    source = "fl.from_raw_data({'columns': [{'name': 'a', 'data_type': 'Integer'}], 'data': [[1]]})"
    assert interprets_expression(f"{source}.head(1)")
    assert not interprets_expression(f"{source}.limit(1)")
