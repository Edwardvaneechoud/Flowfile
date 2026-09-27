"""Native classes in the notebook dialect (plan section 3 item 3) and the per-type normaliser (section 2.5 step 3).

Every flow is built in process through ``flowfile_frame`` or core ``add_*``; where a round trip matters the
rendered cells run as a clean run in notebook mode and the rebuilt node is compared with its canvas twin.
"""

import polars as pl

import flowfile_frame as ff
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.notebook.compare import normalise, parameters_equal, settings_equal
from flowfile_core.notebook.render import render
from flowfile_core.schemas import input_schema, transform_schema
from flowfile_frame import notebook
from flowfile_frame.notebook_cells import clean_run


def double_amount(orders):
    """Doubles the amount."""
    doubled = orders.collect().with_columns(pl.col("amount") * 2)

    # %% Publish
    return doubled


def _cells_by_node(rendering) -> dict:
    return {cell.node_ids[0]: cell for cell in rendering.cells if cell.kind == "node"}


def _rebuild(graph: FlowGraph) -> tuple[dict, dict]:
    """Render ``graph``, clean-run its cells in notebook mode and return (canvas nodes, rebuilt nodes) by id."""
    rendering = render(graph)
    cells = [(cell.cell_id, cell.code) for cell in rendering.cells]
    provenance = {
        cell.cell_id: [(graph.get_node(node_id).node_type, node_id) for node_id in cell.node_ids]
        for cell in rendering.cells
        if cell.node_ids
    }
    result = clean_run(cells, max(node.node_id for node in graph.nodes), provenance)
    assert result["ok"], result.get("error")
    canvas = graph.get_flowfile_data().model_dump(mode="json")
    return {n["id"]: n for n in canvas["nodes"]}, {n["id"]: n for n in result["flowfile_data"]["nodes"]}


def _assert_exact(graph: FlowGraph, node_id: int) -> None:
    canvas, rebuilt = _rebuild(graph)
    assert rebuilt[node_id]["type"] == canvas[node_id]["type"]
    node_type = canvas[node_id]["type"]
    assert settings_equal(canvas[node_id]["setting_input"], rebuilt[node_id]["setting_input"], node_type)


def _parameter_gate():
    source = ff.from_dict({"region": ["N", "S"], "amount": [1.0, 2.0]})
    ff.add_flow_parameter(source, ff.Parameter("mode", default="full", type="enum", enum_values=["full", "quick"]))
    gate = ff.Gate(source, parameter="mode", operator="not_equals", value="quick", description="Which side?")
    full = gate.then.with_columns(ff.lit("full").alias("side"))
    quick = gate.otherwise.with_columns(ff.lit("quick").alias("side"))
    union = ff.concat([full, quick], how="diagonal_relaxed")
    return union.flow_graph, gate, full, quick


def test_parameter_gate_uses_the_parameter_variable_and_always_writes_else_output():
    graph, gate, full, quick = _parameter_gate()
    cells = _cells_by_node(render(graph))
    code = cells[gate.node_id].code
    assert code.startswith(f"gate_{gate.node_id} = fl.Gate(")
    assert "parameter=MODE" in code and 'operator="not_equals"' in code and 'value="quick"' in code
    assert "else_output=True" in code and 'description="Which side?"' in code
    assert f"gate_{gate.node_id}.then" in cells[full.node_id].code
    assert f"gate_{gate.node_id}.otherwise" in cells[quick.node_id].code
    _assert_exact(graph, gate.node_id)


def test_single_exit_gate_writes_else_output_false_and_round_trips():
    source = ff.from_dict({"a": [1, 2]})
    gate = ff.Gate(source, formula="[a] > 1", else_output=False)
    graph = gate.then.select("a").flow_graph
    code = _cells_by_node(render(graph))[gate.node_id].code
    assert code.endswith('"[a] > 1", else_output=False)')
    _assert_exact(graph, gate.node_id)


def test_formula_gate_with_control_passes_control_and_stale_parameter_fields_are_cosmetic():
    source = ff.from_dict({"a": [1, 2]})
    control = ff.from_dict({"flag": [True]}, flow_graph=source.flow_graph)
    gate = ff.Gate(source, formula="[flag]", control=control, else_output=False)
    graph = gate.then.select("a").flow_graph
    gate.node.setting_input.gate_input.parameter = "leftover"
    gate.node.setting_input.gate_input.value = "stale"
    code = _cells_by_node(render(graph))[gate.node_id].code
    assert f"control=source_{control.node_id}" in code
    _assert_exact(graph, gate.node_id)


def test_gates_fl_gate_cannot_express_are_placeholders():
    graph, gate, _, _ = _parameter_gate()
    gate.node.setting_input.gate_input.parameter = "undeclared"
    cell = _cells_by_node(render(graph))[gate.node_id]
    assert cell.status == "placeholder" and "does not declare" in cell.reason
    assert cell.code.startswith(f"gate_{gate.node_id} = fl.canvas_node({gate.node_id}, ")


def test_run_flow_whose_child_no_longer_resolves_is_a_placeholder():
    graph = FlowGraph()
    graph.add_run_flow(
        input_schema.NodeRunFlow(
            flow_id=graph.flow_id,
            node_id=1,
            flow_reference=input_schema.SubflowReference(registration_id=987654, flow_uuid="gone", name="Gone"),
            output_slots=["out"],
        )
    )
    cell = _cells_by_node(render(graph))[1]
    assert cell.status == "placeholder" and "does not resolve" in cell.reason


def test_the_demo_renders_its_native_nodes_as_the_demo_writes_them(notebook_corpus):
    graph = dict(notebook_corpus)["demo"]
    codes = {graph.get_node(nid).node_type: cell.code for nid, cell in _cells_by_node(render(graph)).items()}
    assert codes["run_flow"].startswith("run_flow_") and "fl.RunFlow(\n    fl.flow_ref(uuid=" in codes["run_flow"]
    assert "orders=source_" in codes["run_flow"] and 'params={"min_amount": 25}' in codes["run_flow"]
    assert "fl.custom_nodes.mood_emoji(" in codes["mood_emoji"] and "threshold_value=TARGET_PCT" in codes["mood_emoji"]
    assert "parameter=MODE" in codes["gate"] and "else_output=True" in codes["gate"]
    script = codes["python_script"]
    assert script.startswith("import polars as pl\n\n\n@fl.python_script(")
    assert 'outputs=["forecast"]' in script and 'returns={"month": fl.Int64' in script
    assert "    # %% [markdown]\n    # ## Fit" in script and "    # %% Forecast\n" in script


def _script_graph(**settings) -> tuple[FlowGraph, int]:
    graph = FlowGraph()
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=1,
            raw_data_format=input_schema.RawData(
                columns=[input_schema.MinimalFieldInfo(name="amount", data_type="Float64")], data=[[1.0, 2.0]]
            ),
        )
    )
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="python_script"))
    graph.add_python_script(
        input_schema.NodePythonScript(flow_id=graph.flow_id, node_id=2, depending_on_ids=[1], **settings)
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    return graph, 2


def test_code_only_python_script_is_a_placeholder():
    graph, node_id = _script_graph(python_script_input=input_schema.PythonScriptInput(code="x = 1"))
    cell = _cells_by_node(render(graph))[node_id]
    assert cell.status == "placeholder" and "code-only" in cell.reason


def test_python_script_class_form_keeps_cell_ids_outputs_and_consumers_index_by_name():
    cells = [input_schema.NotebookCell(id="a1", code="x = 1"), input_schema.NotebookCell(id="b2", code="y = 2\nz = 3")]
    graph, node_id = _script_graph(
        python_script_input=input_schema.PythonScriptInput(code="x = 1\n\ny = 2\nz = 3", kernel_id="k", cells=cells),
        output_names=["left", "right"],
    )
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=3, node_type="sort"))
    graph.add_sort(
        input_schema.NodeSort(
            flow_id=graph.flow_id,
            node_id=3,
            depending_on_id=2,
            sort_input=[transform_schema.SortByInput(column="amount", how="asc")],
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(2, 3, output_handle="output-1"))
    by_node = _cells_by_node(render(graph))
    code = by_node[node_id].code
    assert '("a1", "x = 1")' in code and '("b2", """y = 2\nz = 3""")' in code
    assert 'kernel="k"' in code and 'outputs=["left", "right"]' in code
    assert f'python_script_{node_id}["right"]' in by_node[3].code
    canvas, rebuilt = _rebuild(graph)
    assert rebuilt[node_id]["setting_input"]["python_script_input"]["cells"] == [
        {"id": "a1", "code": "x = 1"},
        {"id": "b2", "code": "y = 2\nz = 3"},
    ]
    assert settings_equal(canvas[node_id]["setting_input"], rebuilt[node_id]["setting_input"], "python_script")


def test_a_decorated_script_renders_as_a_decorator_and_rebuilds_the_same_cells():
    source = ff.from_dict({"amount": [1.0, 2.0]})
    script = ff.python_script(kernel="lite", description="Double it")(double_amount)
    out = script(source)
    graph = out.flow_graph
    node_id = out.node_id
    code = _cells_by_node(render(graph))[node_id].code
    assert code.startswith('import polars as pl\n\n\n@fl.python_script(kernel="lite", description="Double it")\n')
    assert '    """Doubles the amount."""' in code and "    # %% Publish\n" in code
    assert "\ndef double_amount(orders):\n" in code
    assert code.endswith(f"python_script_{node_id} = double_amount(source_{source.node_id})")
    _assert_exact(graph, node_id)


def test_a_prelude_module_that_does_not_import_falls_back_to_the_class_form():
    cells = [
        input_schema.NotebookCell(id="p", code="import not_a_module_anywhere_xyz as nm"),
        input_schema.NotebookCell(id="i", code='# flowfile: inputs\norders = flowfile_ctx.read_inputs()["main"][0]'),
        input_schema.NotebookCell(
            id="o", code='# flowfile: outputs\n_result = nm.go(orders)\nflowfile_ctx.publish_output(_result, "main")'
        ),
    ]
    graph, node_id = _script_graph(python_script_input=input_schema.PythonScriptInput(code="...", cells=cells))
    code = _cells_by_node(render(graph))[node_id].code
    assert code.startswith(f"python_script_{node_id} = fl.PythonScript(") and "@fl.python_script" not in code


def test_flow_input_and_output_render_as_the_port_calls(notebook_corpus):
    graph = dict(notebook_corpus)["native_flow_io"]
    codes = {graph.get_node(nid).node_type: cell.code for nid, cell in _cells_by_node(render(graph)).items()}
    assert (
        codes["flow_input"].startswith("flow_input_") and '"orders",\n    sample=pl.DataFrame(' in codes["flow_input"]
    )
    assert "flow_graph=flow" in codes["flow_input"]
    assert codes["flow_output"].endswith('.to_flow_output("big_orders")')


def test_a_schema_only_flow_input_renders_schema_and_round_trips():
    graph = ff.create_flow_graph()
    orders = ff.FlowInput("orders", schema={"id": pl.Int64, "when": pl.Date}, flow_graph=graph)
    code = _cells_by_node(render(graph))[orders.node_id].code
    assert 'schema={"id": fl.Int64, "when": fl.Date}' in code
    _assert_exact(graph, orders.node_id)


def test_custom_node_settings_drift_is_a_placeholder(notebook_corpus):
    source = ff.from_dict({"pct": [50.0, 120.0]})
    moods = ff.custom_nodes.mood_emoji(source, source_column="pct", emoji_column_name="mood")
    graph = moods.flow_graph
    code = _cells_by_node(render(graph))[moods.node_id].code
    assert code == (
        f'mood_emoji_{moods.node_id} = fl.custom_nodes.mood_emoji(source_{source.node_id}, source_column="pct", '
        'emoji_column_name="mood")'
    )
    moods.flow_graph.get_node(moods.node_id).setting_input.settings["mood_config"]["retired_option"] = 1
    cell = _cells_by_node(render(graph))[moods.node_id]
    assert cell.status == "placeholder" and "settings drift" in cell.reason


def test_outer_parentheses_do_not_change_the_rendered_formula():
    source = ff.from_dict({"a": [1, 2]})
    plain = source.filter(flowfile_formula="[a] * 2 > [a] + 1")
    wrapped = source.filter(flowfile_formula="(([a] * 2 > [a] + 1))")
    cells = _cells_by_node(render(wrapped.flow_graph))
    assert cells[plain.node_id].code.split(" = ", 1)[1] == cells[wrapped.node_id].code.split(" = ", 1)[1]


def test_every_native_cell_compiles_and_notebook_mode_is_left_clean(notebook_corpus):
    for _, graph in notebook_corpus:
        for cell in render(graph).cells:
            compile(cell.code, cell.cell_id, "exec")
    assert notebook.current() is None


def test_normalise_drops_record_fields_and_compares_formulas_through_the_translator():
    base = {"flow_id": 1, "node_id": 4, "pos_x": 1.0, "description": "x", "node_reference": "r", "user_id": 3}
    a = {**base, "functions": [{"field": {"name": "b", "data_type": "Auto"}, "function": "[a] * 2"}]}
    b = {"node_id": 4, "function": {"field": {"name": "b", "data_type": "Auto"}, "function": "([a] * 2)"}}
    assert settings_equal(a, b, "formula")
    c = {"node_id": 4, "functions": [{"field": {"name": "b", "data_type": "Auto"}, "function": "[a] * 3"}]}
    assert not settings_equal(a, c, "formula")
    assert "flow_id" not in normalise(a, "formula") and "description" not in normalise(a, "formula")


def test_a_basic_filter_equals_its_advanced_spelling():
    basic = {
        "filter_input": {"mode": "basic", "basic_filter": {"field": "q", "operator": "greater_than", "value": "7"}}
    }
    advanced = {"filter_input": {"mode": "advanced", "advanced_filter": "([q] > 7)", "basic_filter": None}}
    assert settings_equal(basic, advanced, "filter")
    other = {"filter_input": {"mode": "advanced", "advanced_filter": "[q] > 8"}}
    assert not settings_equal(basic, other, "filter")


def test_select_compares_the_ordered_projection():
    entry = {"old_name": "a", "new_name": "b", "keep": True, "data_type": "Int64", "data_type_change": False}
    a = {"keep_missing": False, "select_input": [{**entry, "position": 0, "original_position": 3, "is_altered": True}]}
    b = {"keep_missing": False, "select_input": [entry, {"old_name": "c", "new_name": "c", "keep": False}]}
    assert settings_equal(a, b, "select")
    assert not settings_equal({**a, "keep_missing": True}, {**b, "keep_missing": True}, "select")


def test_python_script_cells_compare_by_code_and_gate_by_its_active_source():
    script = {"python_script_input": {"code": "x", "cells": [{"id": "one", "code": "x"}]}}
    same = {"python_script_input": {"code": "x", "cells": [{"id": "two", "code": "x"}]}}
    assert settings_equal(script, same, "python_script")
    gate = {"gate_input": {"condition_source": "formula", "formula": "[a] > 1", "parameter": "p", "value": "v"}}
    fresh = {"gate_input": {"condition_source": "formula", "formula": "([a] > 1)", "parameter": "", "value": ""}}
    assert settings_equal(gate, fresh, "gate")


def test_output_directory_spelled_as_the_file_path_is_the_same_target():
    canvas = {"output_settings": {"name": "o.csv", "directory": "/tmp/x", "abs_file_path": "/tmp/x/o.csv"}}
    frame = {"output_settings": {"name": "o.csv", "directory": "/tmp/x/o.csv", "abs_file_path": "/tmp/x/o.csv"}}
    assert settings_equal(canvas, frame, "output")


def test_parameters_compare_as_models():
    model = ff.Parameter("n", default=3, type="integer")
    graph = ff.create_flow_graph()
    ff.add_flow_parameter(graph, model)
    stored = graph.flow_settings.parameters
    assert parameters_equal(stored, [p.model_dump(mode="json") for p in stored])
    assert not parameters_equal(stored, [])
