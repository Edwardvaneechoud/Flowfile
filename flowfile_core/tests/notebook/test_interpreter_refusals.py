"""The interpreter refuses every construct outside the notebook dialect, on its line, before placing anything.

Each case runs one cell after a setup cell (``fl``, ``pl`` and a frame ``df``) in a notebook mode and
asserts the failing cell's id, its 1-based line and kind, that a dialect refusal says "this needs a
kernel", and that nothing was placed from the refused line on. The cases cover imports, names and attributes
starting with ``_``, every statement and expression outside the subset, the builtins, each refused
``fl`` name, the refused frame, expression and ``pl`` families, argument rules and the size bounds.
"""

from __future__ import annotations

import pytest

from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import CatalogNamespace
from flowfile_core.notebook import allowlist
from flowfile_core.notebook.interpret import CellInterpreter
from flowfile_frame import notebook
from flowfile_frame.notebook_cells import clean_run, execute_cell, new_namespace

SETUP = (
    "import flowfile as fl\n"
    "import polars as pl\n"
    "df = fl.from_raw_data({'columns': [{'name': 'a', 'data_type': 'Integer'}], 'data': [[1, 2, 3]]})\n"
    "p = fl.Parameter('p', default=1)"
)
CELL_ID = "cell-7"
REFUSED_FL = sorted(name for name, (verdict, _) in allowlist.FL_VERDICTS.items() if verdict == allowlist.REFUSE)


def _interpret(code: str):
    """``(result, nodes placed by the cell)`` for ``code`` run after :data:`SETUP`."""
    with notebook.notebook_mode(user_id=1) as mode:
        namespace = new_namespace()
        interpreter = CellInterpreter()
        setup = execute_cell("setup", SETUP, namespace, executor=interpreter)
        assert setup.ok, setup.error
        before = len(mode.graph.nodes)
        result = execute_cell(CELL_ID, code, namespace, executor=interpreter)
        return result, len(mode.graph.nodes) - before


NEEDS_KERNEL = {
    "import": ("import os", 1),
    "import_from": ("from os import path", 1),
    "import_without_alias": ("import flowfile", 1),
    "import_other_alias": ("import json as j", 1),
    "future_import": ("from __future__ import annotations", 1),
    "reader_alias": ("from flowfile_frame import scan_ipc as s", 1),
    "private_attribute": ("x = df._deferred", 1),
    "dunder_attribute": ("x = fl.col('a').__class__", 1),
    "private_fl_name": ("x = fl._open_graph_in_editor", 1),
    "dunder_name": ("x = __builtins__", 1),
    "private_name": ("_x = 1", 1),
    "lambda": ("f = lambda v: v", 1),
    "list_comprehension": ("xs = [v for v in [1, 2]]", 1),
    "dict_comprehension": ("xs = {v: v for v in [1]}", 1),
    "generator": ("xs = list(v for v in [1])", 1),
    "for": ("y = 1\nfor v in [1]:\n    y = v", 2),
    "while": ("y = 1\nwhile False:\n    pass", 2),
    "if": ("y = 1\nif y:\n    y = 2", 2),
    "with": ("y = 1\nwith y:\n    pass", 2),
    "try": ("y = 1\ntry:\n    y = 2\nexcept Exception:\n    pass", 2),
    "class": ("class A:\n    pass", 1),
    "async_def": ("async def f():\n    pass", 1),
    "plain_def": ("def f(x):\n    return x", 1),
    "helper_text_changed": (
        "def _flowfile_flow_parameter(frame, name, value, **declaration):\n"
        '    """The parameters cell declares every parameter, so a gate only names it"""\n'
        "    return name",
        1,
    ),
    "augmented_assignment": ("y = 1\ny += 1", 2),
    "annotated_assignment": ("y: int = 1", 1),
    "delete": ("y = 1\ndel y", 2),
    "global": ("global y", 1),
    "print": ("print(1)", 1),
    "display": ("display(df)", 1),
    "getattr": ("getattr(fl, 'col')", 1),
    "setattr": ("setattr(df, 'x', 1)", 1),
    "exec": ("exec('1')", 1),
    "eval": ("eval('1')", 1),
    "open": ("open('x')", 1),
    "dunder_import": ("__import__('os')", 1),
    "non_literal_subscript": ("k = 'x'\nfl.custom_nodes[k]", 2),
    "integer_subscript": ("y = [1]\nz = y[0]", 2),
    "attribute_target": ("df.x = 1", 1),
    "subscript_target": ("d = {'k': 1}\nd['k'] = 2", 2),
    "chained_assignment": ("a = b = 1", 1),
    "reserved_fl": ("fl = 1", 1),
    "reserved_flow": ("flow = 1", 1),
    "reserved_display": ("display = 1", 1),
    "helper_name": ("_flowfile_expr_literal = 1", 1),
    "starred_argument": ("x = fl.col(*['a'])", 1),
    "double_starred_argument": ("x = fl.lit(1, **{'dtype': None})", 1),
    "starred_target": ("a, *b = [1, 2]", 1),
    "private_keyword": ("x = df.head(1, _n=1)", 1),
    "fstring_conversion": ("x = f'{_flowfile_expr_literal(p)!r}'", 1),
    "fstring_format_spec": ("x = f'{_flowfile_expr_literal(p):>5}'", 1),
    "fstring_other_placeholder": ("x = f'{p}'", 1),
    "bytes": ("x = b'a'", 1),
    "chained_comparison": ("x = fl.col('a') < fl.col('a') < 3", 1),
    "not": ("x = not p", 1),
    "boolean_operator": ("x = p and p", 1),
    "conditional": ("x = 1 if p else 2", 1),
    "negated_expression": ("x = -fl.col('a')", 1),
    "inverted_expression": ("x = ~fl.col('a')", 1),
    "literal_arithmetic": ("x = 'a' * 10", 1),
    "power": ("x = fl.col('a') ** 2", 1),
    "membership": ("x = 1 in [1]", 1),
    "set": ("x = {1}", 1),
    "walrus": ("(x := 1)", 1),
    "bare_binary_statement": ("fl.col('a') + 1", 1),
    "str_method": ("x = 'abc'.upper()", 1),
    "str_method_on_name": ("x = 'abc'\ny = x.upper()", 2),
    "list_method": ("x = [1]\nx.append(2)", 2),
    "fl_call_read": ("x = fl.col", 1),
    "python_script_called": ("x = fl.python_script(kernel='k')", 1),
    "other_decorator": ("@fl.col\ndef f(x):\n    return x", 2),
    "two_decorators": ("@fl.python_script\n@fl.python_script\ndef f(x):\n    return x", 3),
    "unknown_fl_name": ("x = fl.os", 1),
    "fl_module_rebound": ("x = fl", 1),
    "collect": ("x = df.collect()", 1),
    "collect_async": ("x = df.collect_async()", 1),
    "describe": ("x = df.describe()", 1),
    "fetch": ("x = df.fetch(1)", 1),
    "profile": ("x = df.profile()", 1),
    "show_graph": ("df.show_graph()", 1),
    "explain": ("x = df.explain()", 1),
    "collect_schema": ("x = df.collect_schema()", 1),
    "schema": ("x = df.schema", 1),
    "columns": ("x = df.columns", 1),
    "data": ("x = df.data", 1),
    "flow_graph": ("x = df.flow_graph", 1),
    "node_id": ("x = df.node_id", 1),
    "save_graph": ("df.save_graph('x.flowfile')", 1),
    "to_graph": ("x = df.to_graph()", 1),
    "serialize": ("x = df.serialize()", 1),
    "cache": ("x = df.cache()", 1),
    "pipe": ("x = df.pipe(fl.col)", 1),
    "map_batches": ("x = df.map_batches(fl.col)", 1),
    "sink_csv": ("df.sink_csv('x.csv')", 1),
    "tail": ("x = df.tail(2)", 1),
    "lazy": ("x = df.lazy()", 1),
    "concat_method": ("x = df.concat(df)", 1),
    "map_elements": ("x = fl.col('a').map_elements(fl.col)", 1),
    "expr_map_batches": ("x = fl.col('a').map_batches(fl.col)", 1),
    "expr_pipe": ("x = fl.col('a').pipe(fl.col)", 1),
    "expr_meta": ("x = fl.col('a').meta", 1),
    "expr_name": ("x = fl.col('a').name", 1),
    "expr_list": ("x = fl.col('a').list", 1),
    "expr_attribute": ("x = fl.col('a').expr", 1),
    "str_namespace_other": ("x = fl.col('a').str.json_decode()", 1),
    "group_by_shortcut": ("x = df.group_by('a').sum()", 1),
    "gate_is_open": ("fl.add_flow_parameter(flow, p)\ng = fl.Gate(df, parameter='p', value=1)\nx = g.is_open", 3, 1),
    "parameter_ref": ("x = p.ref", 1),
    "parameter_to_expr": ("x = p.to_expr()", 1),
    "custom_nodes_install": ("fl.custom_nodes.install('x')", 1),
    "custom_nodes_list": ("x = fl.custom_nodes.list()", 1),
    "custom_nodes_get": ("x = fl.custom_nodes.get('x')", 1),
    "graph_attribute": ("flow.run_graph()", 1),
    "graph_save": ("flow.save_flow('x')", 1),
    "graph_elsewhere": ("x = fl.from_raw_data({'columns': [], 'data': []}, flow_graph=flow)", 1),
    "pl_read": ("x = pl.read_csv('x.csv')", 1),
    "pl_scan": ("x = pl.scan_parquet('x.parquet')", 1),
    "pl_lazyframe": ("x = pl.LazyFrame({'a': [1]})", 1),
    "pl_dataframe_not_literal": ("x = pl.DataFrame(df)", 1),
    "pl_dataframe_expression_values": ("x = pl.DataFrame({'a': [fl.col('a')]})", 1),
    "pl_dataframe_other_keyword": ("x = pl.DataFrame({'a': [1]}, orient='row')", 1),
    "pl_config": ("x = pl.Config", 1),
    "pl_string_cache": ("pl.enable_string_cache()", 1),
    "pl_string_cache_context": ("x = pl.StringCache()", 1),
    "pl_random_seed": ("pl.set_random_seed(1)", 1),
    "pl_thread_pool": ("x = pl.thread_pool_size()", 1),
    "pl_api": ("pl.api.register_expr_namespace('x')", 1),
    "pl_collect_all": ("x = pl.collect_all([])", 1),
    "pl_expression": ("x = pl.col('a')", 1),
    "pl_sql": ("x = pl.sql('select 1')", 1),
    "datetime_now": ("import datetime\nx = datetime.datetime.now()", 2),
    "datetime_today": ("import datetime\nx = datetime.date.today()", 2),
    "datetime_from_names": ("import datetime\ny = 2024\nx = datetime.date(y, 1, 1)", 3),
    "datetime_keywords": ("import datetime\nx = datetime.date(year=2024, month=1, day=1)", 2),
    "hashlib": ("import hashlib\nx = hashlib.md5", 2),
    "json": ("import json\nx = json.dumps(1)", 2),
    "prelude_module_outside_the_script": (
        "import numpy as np\nnp.zeros(1)\n\n\n@fl.python_script\ndef s(df):\n    return df",
        2,
    ),
    "prelude_alias_fl": ("import os as fl\n\n\n@fl.python_script\ndef s(df):\n    return df", 1),
    "prelude_alias_display": ("import os as display\n\n\n@fl.python_script\ndef s(df):\n    return df", 1),
    "prelude_constant_reserved": ("flow = {1}\n\n\n@fl.python_script\ndef s(df):\n    return df", 1),
    "prelude_set_after_the_script": ("@fl.python_script\ndef s(df):\n    return df\n\n\nx = {1}", 6),
    "untyped_formulas": ("x = df.with_columns(flowfile_formulas=['[a] + 1'], output_column_names=['b'])", 1),
    "run_flow_name": ("x = fl.RunFlow(1, name='child')", 1),
    "canvas_node_id_from_a_name": ("n = 3\nx = fl.canvas_node(n, df)", 2),
    "polars_code_def_elsewhere": (
        "def _polars_code_4(input_df):\n    return input_df\n\n\nx = fl.lit(_polars_code_4)",
        5,
    ),
    "polars_code_annotation": ("def _polars_code_4(input_df: pl.LazyFrame):\n    return input_df", 1),
    "polars_code_default": ("def _polars_code_4(input_df=None):\n    return input_df", 1),
    "multi_line_chain": ("x = (\n    df\n    .filter(fl.col('a') > 1)\n    .collect()\n)", 4, 1),
}


@pytest.mark.parametrize("name", sorted(NEEDS_KERNEL))
def test_a_construct_outside_the_dialect_needs_a_kernel_on_its_line(name):
    code, line, *before = NEEDS_KERNEL[name]
    result, placed = _interpret(code)
    assert (result.cell_id, result.line, result.kind) == (CELL_ID, line, "needs_kernel"), result.message
    assert result.message.endswith("this needs a kernel") and result.error == result.message
    assert placed == (before[0] if before else 0)


@pytest.mark.parametrize("name", REFUSED_FL)
def test_every_refused_fl_name_needs_a_kernel(name):
    result, placed = _interpret(f"\nx = fl.{name}")
    assert (result.cell_id, result.line, result.kind) == (CELL_ID, 2, "needs_kernel")
    assert f"`fl.{name}`" in result.message and result.message.endswith("this needs a kernel")
    assert placed == 0


def test_auto_create_never_reaches_the_catalog():
    with get_db_context() as db:
        before = db.query(CatalogNamespace).count()
    result, placed = _interpret("ref = fl.CatalogReference('notebook_made', auto_create=True)")
    with get_db_context() as db:
        after = db.query(CatalogNamespace).count()
    assert (result.line, result.kind) == (1, "needs_kernel")
    assert before == after and placed == 0


BOUNDED = {
    "depth": ("x = " + " + ".join(["1"] * 300), {}),
    "string_length": ("x = 'a'", {"string_length": 0}),
    "statements": ("x = 1\ny = 2", {"statements_per_cell": 1}),
    "ast_nodes": ("x = [1, 2, 3, 4, 5]", {"ast_nodes_per_cell": 5}),
    "literal_elements": ("x = [1, 2, 3, 4, 5, 6]", {"literal_elements_per_request": 5}),
    "steps": ("x = [1, 2, 3, 4, 5, 6]", {"steps_per_request": 5}),
    "nodes": ("x = df.head(1).head(1)", {"nodes_per_request": 2}),
    "call_nesting": ("x = " + "fl.lit(" * 150 + "1" + ")" * 150, {}),
}


@pytest.mark.parametrize("name", sorted(BOUNDED))
def test_a_cell_past_a_bound_is_refused(name, monkeypatch):
    code, bounds = BOUNDED[name]
    with notebook.notebook_mode(user_id=1):
        namespace = new_namespace()
        interpreter = CellInterpreter()
        assert execute_cell("setup", SETUP, namespace, executor=interpreter).ok
        interpreter.steps = interpreter.elements = 0
        for key, value in bounds.items():
            monkeypatch.setitem(allowlist.BOUNDS, key, value)
        result = execute_cell(CELL_ID, code, namespace, executor=interpreter)
    assert (result.cell_id, result.kind) == (CELL_ID, "refused")
    assert result.line == 1


def test_an_expression_doubled_line_by_line_is_refused_on_its_line_before_it_outgrows_memory():
    result, placed = _interpret("e = fl.col('a')\n" + "e = e + e\n" * 22)
    assert (result.cell_id, result.kind, placed) == (CELL_ID, "refused", 0)
    assert 1 < result.line <= 23
    assert "expressions too long" in result.message


def test_a_missing_prelude_module_fails_as_its_import_does():
    code = "import not_a_module_anywhere\n\n\n@fl.python_script\ndef s(df):\n    return df"
    result, placed = _interpret(code)
    assert (result.line, result.kind) == (1, "error")
    assert result.message == "ModuleNotFoundError: No module named 'not_a_module_anywhere'\n"
    assert placed == 0


def test_an_undefined_name_is_an_error_on_its_line():
    result, _ = _interpret("y = 1\nx = undefined_name")
    assert (result.line, result.kind) == (2, "error")
    assert result.message == "NameError: name 'undefined_name' is not defined\n"


def test_a_clean_run_reports_the_refused_cell_line_and_kind():
    cells = [("imports", "import flowfile as fl"), ("cell-3", "x = 1\nprint(x)"), ("cell-4", "y = 2")]
    result = clean_run(cells, ceiling=0, user_id=1, executor=CellInterpreter())
    assert result["ok"] is False
    assert (result["cell_id"], result["line"], result["kind"]) == ("cell-3", 2, "needs_kernel")
    assert result["message"] == result["error"] and "`print`" in result["message"]
    assert notebook.current() is None
