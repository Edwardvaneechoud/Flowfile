"""The cell interpreter: every rendered corpus cell interprets, into the flow ``exec`` builds from it.

The corpus clean runs are the session's shared ones (``corpus_runs``): every rendered cell must
interpret, and the round trip holds the interpreting runner's flows equal to the ``exec`` runner's.
Shape tests pin the forms the exporter emits that the corpus covers only once or not at all, each
built by both executors into the same payload up to what a run mints (the session graph's identity
and fresh Python Script cell ids); the error tests pin that both executors report a failure with the
same kind, line and message; and the allowlist tests pin that the corpus uses every entry not tied to
an exporter handler and no input-only one, and stays far below every bound.
"""

from __future__ import annotations

import json
import math
import re
import sys
import types

import polars as pl
import pytest

from flowfile_core.flowfile.flow_graph import add_connection
from flowfile_core.notebook import allowlist
from flowfile_core.notebook.interpret import CellInterpreter, _parse
from flowfile_core.notebook.push import seed_snapshot
from flowfile_core.notebook.render import _NOTEBOOK_HELPERS, render
from flowfile_core.schemas import input_schema, transform_schema
from flowfile_frame import notebook
from flowfile_frame.native import set_node_reference
from flowfile_frame.notebook_cells import clean_run, execute_cell, new_namespace, seed_session
from tests.notebook.conftest import (
    NOTEBOOK_OWNER_ID,
    RUNNERS,
    ExecRunner,
    clean_run_request,
    masked_payload,
    no_kernel_manager,
)


def _clean_run(graph, executor, cells=None):
    rendering = render(graph)
    cells = cells if cells is not None else [(cell.cell_id, cell.code) for cell in rendering.cells]
    provenance = {
        cell.cell_id: [(graph.get_node(node_id).node_type, node_id) for node_id in cell.node_ids]
        for cell in rendering.cells
        if cell.node_ids
    }
    ceiling = max((node.node_id for node in graph.nodes), default=0)
    with no_kernel_manager(), notebook.RUN_LOCK:
        try:
            seed_session(**seed_snapshot(graph), user_id=NOTEBOOK_OWNER_ID)
            return clean_run(cells, ceiling, provenance, executor=executor)
        finally:
            notebook.exit()


def _interpreting(corpus_runs):
    return corpus_runs["interpreting"].runs


def test_every_rendered_corpus_cell_interprets(corpus_runs):
    failed = {
        name: {
            "cell_id": run.result.cell_id,
            "line": run.result.line,
            "kind": run.result.kind,
            "message": run.result.error,
        }
        for name, run in _interpreting(corpus_runs).items()
        if run.result.error
    }
    assert not failed, json.dumps(failed, indent=1)


def test_every_allowed_entry_is_used_by_the_corpus_or_emitted_outside_it(corpus_runs):
    from tests.notebook.test_allowlist import EMITTED_OUTSIDE_THE_CORPUS, _allowed_entries

    used = set().union(*(run.interpreter.used for run in _interpreting(corpus_runs).values()))
    assert used <= _allowed_entries()
    assert not set().union(*(run.interpreter.used_input_only for run in _interpreting(corpus_runs).values()))
    dead = _allowed_entries() - used - set(EMITTED_OUTSIDE_THE_CORPUS)
    assert not dead, sorted(dead)


def test_the_corpus_stays_far_below_every_bound(notebook_corpus, corpus_runs, monkeypatch):
    runs = _interpreting(corpus_runs)
    bounds = dict(allowlist.BOUNDS)
    for key in ("ast_nodes_per_cell", "statements_per_cell", "depth", "string_length"):
        monkeypatch.setitem(allowlist.BOUNDS, key, bounds[key] // 10)
    for name, graph in notebook_corpus:
        cells = render(graph).cells
        assert len(cells) * 10 < bounds["cells_per_request"], name
        for cell in cells:
            assert len(cell.code.encode()) * 10 < bounds["bytes_per_cell"], name
            _parse(f"<{cell.cell_id}>", cell.code)
        interpreter = runs[name].interpreter
        assert interpreter.steps * 10 < bounds["steps_per_request"], name
        assert interpreter.elements * 10 < bounds["literal_elements_per_request"], name
        assert interpreter.expression_chars * 10 < bounds["expression_chars_per_request"], name
        assert len(graph.nodes) * 10 < bounds["nodes_per_request"], name


IMPORTS = "import flowfile as ff\nimport polars as pl"
SOURCE = "src = ff.from_raw_data({'columns': [{'name': 'a', 'data_type': 'Integer'}], 'data': [[1, 2, 3]]})"
HELPERS = "\n\n\n".join(_NOTEBOOK_HELPERS.values())
SHAPES = {
    "parameters_bound_and_bare": [
        IMPORTS,
        "min_a = ff.add_flow_parameter(flow, ff.Parameter('min_a', default=2, type='integer'))\n"
        "ff.add_flow_parameter(flow, ff.Parameter('label', default='x'))",
        SOURCE + "\nkept = src.filter(ff.col('a') >= min_a)",
    ],
    "flow_input_sample": [
        IMPORTS,
        "orders = ff.FlowInput(\n    'orders',\n    sample=pl.DataFrame({'id': [1, 2], 'amount': [5, 15]}, "
        "schema={'id': ff.Int64, 'amount': ff.Int64}, strict=False),\n    flow_graph=flow,\n)\n"
        "big = orders.filter(ff.col('amount') > 10)\nbig.to_flow_output('big')",
    ],
    "flow_input_signed_sample": [
        IMPORTS,
        "refunds = ff.FlowInput(\n    'refunds',\n    sample=pl.DataFrame({'amount': [-5, 15], 'delta': [-2.5, +1.0]}, "
        "schema={'amount': ff.Int64, 'delta': ff.Float64}, strict=False),\n    flow_graph=flow,\n)\n"
        "negative = refunds.filter(ff.col('amount') < 0)",
    ],
    "tuple_unpacking_and_bare_writer": [
        IMPORTS,
        SOURCE + "\nkept, dropped = src.filter_split(ff.col('a') > 1)\n"
        "kept.write_csv('/nonexistent/kept.csv', separator=',')\nlast = dropped.select('a')",
    ],
    "helpers_and_gates": [
        IMPORTS + "\nimport json\n\n\n" + HELPERS,
        "mode = ff.add_flow_parameter(flow, ff.Parameter('mode', default='a', type='enum', enum_values=['a', 'b']))\n"
        "cut = ff.add_flow_parameter(flow, ff.Parameter('cut', default=1, type='integer'))",
        SOURCE + "\ngate = ff.Gate(src, parameter=_flowfile_flow_parameter(src, 'mode', mode, type='enum', "
        "enum_values=['a', 'b']), value='a')\nopen_side = gate.then.select('a')\nother = gate.otherwise\n"
        "formula_gate = ff.Gate(src, f'[a] > {_flowfile_expr_literal(cut)}')\nlive = formula_gate.then",
    ],
    "temporal_literals": [
        IMPORTS + "\nimport datetime",
        "src = ff.from_raw_data({'columns': [{'name': 'd', 'data_type': 'Date'}], 'data': [['2024-01-01']]})\n"
        "late = src.filter(ff.col('d') > datetime.date(2024, 1, 2))\n"
        "later = src.filter(ff.col('d') < datetime.datetime(2025, 1, 2, 3, 4, 5))",
    ],
    "polars_code_defs": [
        IMPORTS,
        SOURCE + "\nother = src.select('a')",
        "def _polars_code_3(input_df_1: ff.FlowFrame, input_df_2: ff.FlowFrame):\n"
        "    output_df = input_df_1.join(input_df_2, how='cross')\n    return output_df\n\n\n"
        "joined = src.polars_code(_polars_code_3, other, description='Cross')",
        "def _polars_code_4():\n    output_df = pl.LazyFrame({'a': [1, 2]})\n    return output_df\n\n\n"
        "made = ff.polars_code(_polars_code_4)",
        "def _polars_code_5(input_df: ff.FlowFrame):\n    return input_df.with_columns(\n        \n"
        "        b=pl.lit(1),\n    )\n\n\nwidened = src.polars_code(_polars_code_5)",
        "texted = src.polars_code(\n    'output_df = input_df'\n)",
    ],
    "python_script_forms": [
        IMPORTS,
        SOURCE,
        "import json\nLIMIT = 2\n\n\n@ff.python_script(\n    kernel='lite',\n    returns={'a': ff.Int64},\n"
        '    description=\'Keep a few\',\n)\ndef _script_3(frame):\n    """Keep the first rows."""\n'
        "    rows = frame.collect().head(LIMIT)\n    # %% Publish\n    return rows.with_columns(\n"
        "        tag=pl.lit(json.dumps(1)))\n\n\nscripted = _script_3(src)",
        "@ff.python_script(outputs=['left', 'right'])\ndef split(frame):\n    return {'left': frame, 'right': frame}\n\n\n"
        "split_4 = split.node(src)\nleft = split_4['left']\nright = split_4['right']",
    ],
    "python_script_prelude_constants": [
        IMPORTS,
        SOURCE,
        "_STOPWORDS = {'a', 'the'}\nKEY = b'k'\nLOOKUP = {(1, 2): 'x'}\nEMPTY = set()\nFLOOR = -1\n\n\n"
        "@ff.python_script(kernel='lite')\ndef clean(frame):\n"
        "    keep = len(_STOPWORDS) + len(KEY) + len(LOOKUP) + len(EMPTY) > FLOOR\n"
        "    return frame if keep else frame\n\n\ncleaned = clean(src)",
    ],
    "join_temporaries": [
        IMPORTS,
        SOURCE,
        "_join_3_left = src.select('a')\n_join_3_right = src.select('a')\n"
        "joined_3 = _join_3_left.join(_join_3_right, on='a')",
        "df_right = src\nself_joined = src.join(df_right, on='a', how='inner')",
    ],
}


def _synced(graph):
    """``graph``'s unedited rendered cells clean-run by the production runner, then by its ``exec`` twin."""
    request = clean_run_request(graph, render(graph))
    with no_kernel_manager():
        return [runner().clean_run(NOTEBOOK_OWNER_ID, graph.flow_id, request) for runner in RUNNERS.values()]


def _assert_both_runners_build_it_alike(graph):
    interpreted, executed = _synced(graph)
    assert executed.error is None, executed.error
    assert interpreted.error is None, (interpreted.cell_id, interpreted.line, interpreted.error)
    assert masked_payload(interpreted.flowfile_data) == masked_payload(executed.flowfile_data)


def _run_cells(cells, executor):
    with no_kernel_manager():
        return clean_run([(f"cell-{i}", code) for i, code in enumerate(cells)], ceiling=0, user_id=1, executor=executor)


@pytest.mark.parametrize("name", sorted(SHAPES))
def test_an_exporter_shape_interprets_into_what_exec_builds(name):
    interpreted, executed = (
        _run_cells(SHAPES[name], executor) for executor in (CellInterpreter(), ExecRunner.executor())
    )
    assert executed["ok"], executed.get("error")
    assert interpreted["ok"], (interpreted.get("cell_id"), interpreted.get("line"), interpreted.get("message"))
    assert masked_payload(interpreted) == masked_payload(executed)


def test_frame_readers_imported_from_the_frame_interpret(tmp_path):
    path = tmp_path / "rows.arrow"
    pl.DataFrame({"a": [1, 2]}).write_ipc(path)
    cells = [IMPORTS + "\nfrom flowfile_frame import scan_ipc", f"rows = scan_ipc({str(path)!r})\nkept = rows.head(1)"]
    interpreted, executed = (_run_cells(cells, executor) for executor in (CellInterpreter(), ExecRunner.executor()))
    assert interpreted["ok"] and executed["ok"]
    assert masked_payload(interpreted) == masked_payload(executed)


def test_custom_node_attribute_subscript_and_node_forms_interpret(notebook_corpus):
    call = "(ordered, source_column='a', threshold_value=1, emoji_column_name='mood', add_random_sparkle=False)"
    cells = [
        IMPORTS,
        SOURCE.replace("'Integer'", "'Double'") + "\nordered = src.sort('a')",
        f"by_attribute = ff.custom_nodes.mood_emoji{call}",
        f"by_key = ff.custom_nodes['mood_emoji']{call}",
        f"placed = ff.custom_nodes.mood_emoji.node{call}\nby_node = placed.output",
    ]
    interpreted, executed = (_run_cells(cells, executor) for executor in (CellInterpreter(), ExecRunner.executor()))
    assert executed["ok"], executed.get("error")
    assert interpreted["ok"], interpreted.get("message")
    assert masked_payload(interpreted) == masked_payload(executed)


ERRORS = {
    "frame_error": "ff.Gate(df, parameter='undeclared', value=1)",
    "third_call_of_a_chain": (
        "out = (\n    df.filter(ff.col('a') > 1)\n    .with_columns(ff.col('a').alias('b'))\n    .join(df, on='nope')\n)"
    ),
    "method_name_above_its_arguments": "out = (df\n    .filter(ff.col('a') > 1)\n    .join(\n        df, on='nope'))",
    "polars_error": "out = df.unpivot(on=['nope'])",
    "python_script_decorator": "x = 1\n@ff.python_script(\n    kernel='k')\ndef s(df):\n    return helper(df)",
    "invalid_settings": "out = df.to_flow_output('')",
    "notebook_refusal": "ff.RunFlow(df)",
    "missing_custom_node": "x = ff.custom_nodes.nope_nope",
    "missing_custom_node_by_key": "x = ff.custom_nodes['nope_nope']",
    "syntax": "x = (",
    "scope_error": "nonlocal x",
}
ERROR_SETUP = (
    "import flowfile as ff\ndf = ff.from_raw_data({'columns': [{'name': 'a', 'data_type': 'Integer'}], 'data': [[1]]})"
)


@pytest.mark.parametrize("name", sorted(ERRORS))
def test_both_executors_report_a_failing_cell_the_same_way(name):
    reports = []
    for executor in (ExecRunner.executor(), CellInterpreter()):
        with notebook.notebook_mode(user_id=1):
            namespace = new_namespace()
            assert execute_cell("setup", ERROR_SETUP, namespace, executor=executor).ok
            result = execute_cell("failing", ERRORS[name], namespace, executor=executor)
        message = re.sub(r"<cell-failing-\d+>", "<cell>", result.message or "")
        reports.append((result.ok, result.kind, result.line, message))
    assert reports[0][0] is False
    assert reports[1] == reports[0]


def test_hash_clock_and_untranslated_formulas_render_as_formula_text_that_interprets():
    import flowfile_frame as ff

    graph = ff.create_flow_graph()
    source = ff.from_raw_data(
        {
            "columns": [{"name": "a", "data_type": "String"}, {"name": "d", "data_type": "Date"}],
            "data": [["x"], ["2024-01-01"]],
        },
        flow_graph=graph,
    )
    formulas = {"hashed": "md5([a])", "stamped": "now()", "joined": "concat([a], 'x')"}
    frame = source.filter(flowfile_formula="[d] < today()")
    for name, formula in formulas.items():
        frame = frame.with_columns(
            flowfile_formulas=[formula], output_column_names=[name], output_column_datatypes=["Auto"]
        )
    code = "\n".join(cell.code for cell in render(graph).cells)
    assert "lambda" not in code and "datetime" not in code
    assert code.count("output_column_datatypes=['Auto']") == len(formulas)
    interpreted, executed = (_clean_run(graph, executor) for executor in (CellInterpreter(), ExecRunner.executor()))
    assert interpreted["ok"], (interpreted.get("cell_id"), interpreted.get("line"), interpreted.get("message"))
    assert masked_payload(interpreted) == masked_payload(executed)
    canvas = {node.node_id: node.setting_input for node in graph.nodes}
    for node in interpreted["flowfile_data"]["nodes"]:
        if node["type"] in ("formula", "filter"):
            key = "function" if node["type"] == "formula" else "filter_input"
            assert node["setting_input"][key] == canvas[node["id"]].model_dump(mode="json")[key]


def _source_graph(**columns: list[int]):
    """A new graph holding one manual input of integer ``columns``, and that input's frame."""
    import flowfile_frame as ff

    graph = ff.create_flow_graph()
    raw = {"columns": [{"name": name, "data_type": "Integer"} for name in columns], "data": list(columns.values())}
    return graph, ff.from_raw_data(raw, flow_graph=graph)


def test_negations_render_as_expressions_and_a_translation_outside_the_allowlist_as_formula_text():
    graph, source = _source_graph(a=[1, -2], qty=[3, 4])
    frame = source.filter(flowfile_formula="[qty] * -1 < 0")
    formulas = {
        "negated": "-[a]",
        "above": "[a] > -1",
        "banded": "if [a] > 1 then 'x' elseif [a] > 0 then 'y' else 'z' endif",
    }
    for name, formula in formulas.items():
        frame = frame.with_columns(
            flowfile_formulas=[formula], output_column_names=[name], output_column_datatypes=["Auto"]
        )
    code = "\n".join(cell.code for cell in render(graph).cells)
    assert code.count(".neg()") == 3
    assert f"flowfile_formulas=[{formulas['banded']!r}]" in code
    _assert_both_runners_build_it_alike(graph)


def test_a_basic_filter_value_shaped_like_arithmetic_renders_as_text():
    graph, source = _source_graph(a=[1, -2])
    for node_id, value in ((10, "1-2"), (11, "-3")):
        graph.add_filter(
            input_schema.NodeFilter(
                flow_id=graph.flow_id,
                node_id=node_id,
                depending_on_id=source.node_id,
                filter_input=transform_schema.FilterInput(
                    mode="basic", basic_filter=transform_schema.BasicFilter(field="a", operator=">", value=value)
                ),
            )
        )
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(source.node_id, node_id))
    code = "\n".join(cell.code for cell in render(graph).cells)
    assert 'ff.col("a") > "1-2"' in code and 'ff.col("a") > -3' in code
    _assert_both_runners_build_it_alike(graph)


def test_a_node_reference_naming_a_builtin_renders_and_syncs():
    import flowfile_frame as ff

    graph, source = _source_graph(a=[1, 2])
    kept = source.filter(ff.col("a") > 1)
    counted = kept.select("a")
    set_node_reference(graph, kept.node_id, "sum")
    set_node_reference(graph, counted.node_id, "input")
    code = "\n".join(cell.code for cell in render(graph).cells)
    assert re.search(r"^sum = ", code, re.M) and re.search(r"^input = sum", code, re.M)
    _assert_both_runners_build_it_alike(graph)


def _json_then_math(orders):
    parts = [json.dumps(v) for v in math.modf(2.5)]
    return orders.head(len(parts))


def test_a_script_whose_stored_prelude_has_another_order_still_renders_decorated():
    import flowfile_frame as ff

    graph, source = _source_graph(a=[1, 2])
    scripted = ff.python_script(kernel="lite")(_json_then_math)(source)
    cells = graph.get_node(scripted.node_id).setting_input.python_script_input.cells
    assert cells[0].code == "import json\nimport math"
    cells[0].code = "import math\nimport json"
    code = "\n".join(cell.code for cell in render(graph).cells)
    assert "import math\nimport json\n\n\n@ff.python_script(" in code
    _assert_both_runners_build_it_alike(graph)


def test_a_prelude_import_of_a_module_without_a_spec_interprets_as_exec_imports_it(monkeypatch):
    monkeypatch.setitem(sys.modules, "spec_less_module", types.ModuleType("spec_less_module"))
    cells = [
        IMPORTS,
        SOURCE,
        "import spec_less_module\n\n\n@ff.python_script(kernel='lite')\ndef s(frame):\n"
        "    return frame if spec_less_module else frame\n\n\nkept = s(src)",
    ]
    interpreted, executed = (_run_cells(cells, executor) for executor in (CellInterpreter(), ExecRunner.executor()))
    assert executed["ok"], executed.get("error")
    assert interpreted["ok"], (interpreted.get("cell_id"), interpreted.get("line"), interpreted.get("message"))
    assert masked_payload(interpreted) == masked_payload(executed)
