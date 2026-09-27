"""Tests for the notebook renderer: one fl-dialect cell per node, placeholders, ordering and fingerprint."""

import random

import polars as pl
import pytest

import flowfile_frame as ff
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.notebook.render import (
    IMPORTS_CELL_ID,
    PARAMETERS_CELL_ID,
    code_fingerprint,
    node_label,
    render,
    topological_order,
)
from flowfile_core.schemas import input_schema


def _connect(graph: FlowGraph, from_id: int, to_id: int, input_type: str = "input-0") -> None:
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id, to_id, input_type))


def _pipeline() -> dict:
    """A flow covering sources, filter, formula, join, group_by, sort, select, union and a ${param}."""
    orders = ff.from_dict({"id": [1, 2, 3], "amount": [10, 20, 30], "region": ["n", "s", "n"]})
    graph = orders.flow_graph
    ff.add_flow_parameter(graph, ff.Parameter("factor", default=2, type="integer"))
    ff.add_flow_parameter(graph, ff.Parameter("label", default="big"))
    regions = ff.from_dict({"region": ["n", "s"], "name": ["North", "South"]}, flow_graph=graph)
    big = orders.filter(ff.col("amount") > 10)
    scaled = big.with_columns(flowfile_formulas=["[amount] * ${factor}"], output_column_names=["scaled"])
    labelled = scaled.with_columns((ff.col("amount") * 2).alias("double"))
    joined = labelled.join(regions, on="region", how="left")
    grouped = joined.group_by("name").agg(ff.col("amount").sum())
    ordered = grouped.sort("name")
    selected = ordered.select("name")
    small = orders.filter(flowfile_formula="[region] = '${label}'")
    union = ff.concat([selected, small.select("region").rename({"region": "name"})], how="diagonal_relaxed")
    return {"graph": union.flow_graph, "orders": orders, "joined": joined, "union": union}


def _cells_by_node(rendering) -> dict:
    return {cell.node_ids[0]: cell for cell in rendering.cells if cell.kind == "node"}


def _live_dumps(graph: FlowGraph) -> dict:
    return {
        node.node_id: node.setting_input.model_dump(mode="json") if node.setting_input is not None else None
        for node in graph.nodes
        if node.node_type != "polars_lazy_frame"
    }


def test_one_cell_per_node_with_leading_imports_and_parameters():
    graph = _pipeline()["graph"]
    rendering = render(graph)
    assert rendering.cells[0].cell_id == IMPORTS_CELL_ID
    assert rendering.cells[0].code.splitlines()[0] == "import flowfile as fl"
    assert rendering.cells[1].cell_id == PARAMETERS_CELL_ID
    node_cells = [c for c in rendering.cells if c.kind == "node"]
    assert sorted(c.node_ids[0] for c in node_cells) == sorted(n.node_id for n in graph.nodes)
    assert [c.cell_id for c in node_cells] == [f"node-{c.node_ids[0]}" for c in node_cells]
    assert set(rendering.var_by_node) == {n.node_id for n in graph.nodes}


def test_code_cells_compile_and_use_the_fl_alias():
    rendering = render(_pipeline()["graph"])
    for cell in rendering.cells:
        compile(cell.code, cell.cell_id, "exec")
        assert "ff." not in cell.code
        assert "run_etl_pipeline" not in cell.code
    assert all(cell.status == "code" for cell in rendering.cells), [
        (c.cell_id, c.reason) for c in rendering.cells if c.status != "code"
    ]


def test_parameters_cell_declares_with_add_flow_parameter():
    rendering = render(_pipeline()["graph"])
    params = next(c for c in rendering.cells if c.cell_id == PARAMETERS_CELL_ID)
    assert params.code.splitlines() == [
        'FACTOR = fl.add_flow_parameter(flow, fl.Parameter("factor", default=2, type="integer"))',
        'LABEL = fl.add_flow_parameter(flow, fl.Parameter("label", default="big"))',
    ]
    assert params.defines == ["FACTOR", "LABEL"]
    assert "flow" in params.uses and "fl" in params.uses


def test_param_refs_stay_verbatim_and_no_sentinel_leaks():
    rendering = render(_pipeline()["graph"])
    text = "\n".join(cell.code for cell in rendering.cells)
    assert "${factor}" in text and "${label}" in text
    assert "__FF_PARAM" not in text


def test_labels_are_deterministic_type_label_and_id():
    pipeline = _pipeline()
    rendering = render(pipeline["graph"])
    joined_id = pipeline["joined"].node_id
    assert rendering.var_by_node[joined_id] == node_label("join", joined_id) == f"joined_{joined_id}"
    cell = _cells_by_node(rendering)[joined_id]
    assert cell.defines == [f"joined_{joined_id}"]
    assert pipeline["orders"].node_id in {n for n, v in rendering.var_by_node.items() if v.startswith("source_")}


def test_node_reference_names_the_variable_and_labels_are_uniquified_against_it():
    pipeline = _pipeline()
    graph = pipeline["graph"]
    joined_id = pipeline["joined"].node_id
    orders_id = pipeline["orders"].node_id
    graph.get_node(orders_id).setting_input.node_reference = f"joined_{joined_id}"
    rendering = render(graph)
    assert rendering.var_by_node[orders_id] == f"joined_{joined_id}"
    assert rendering.var_by_node[joined_id] == f"joined_{joined_id}_2"
    consumers = [c for c in rendering.cells if f"joined_{joined_id}" in c.uses]
    assert consumers and all(c.node_ids != [orders_id] for c in consumers)
    assert f"joined_{joined_id}_2" in _cells_by_node(rendering)[joined_id].defines


def test_cell_ids_and_text_are_stable_across_renders():
    graph = _pipeline()["graph"]
    first, second = render(graph), render(graph)
    assert [c.model_dump() for c in first.cells] == [c.model_dump() for c in second.cells]
    assert first.code_fingerprint == second.code_fingerprint


def test_order_is_independent_of_insertion_order():
    graph = _pipeline()["graph"]
    before = [c.cell_id for c in render(graph).cells]
    graph._node_db = dict(reversed(list(graph._node_db.items())))
    assert [c.cell_id for c in render(graph).cells] == before
    nodes = list(graph.nodes)
    for seed in range(5):
        random.Random(seed).shuffle(nodes)
        assert [n.node_id for n in topological_order(nodes)] == [int(c[5:]) for c in before if c.startswith("node-")]


def test_order_is_kahn_with_min_heap_on_node_id():
    graph = FlowGraph()
    for node_id in (5, 3, 9):
        graph.add_manual_input(
            input_schema.NodeManualInput(
                flow_id=graph.flow_id,
                node_id=node_id,
                raw_data_format=input_schema.RawData.from_pylist([{"a": 1}]),
            )
        )
    graph.add_union(input_schema.NodeUnion(flow_id=graph.flow_id, node_id=1, depending_on_ids=[5, 9]))
    _connect(graph, 5, 1)
    _connect(graph, 9, 1)
    order = [n.node_id for n in topological_order(list(graph.nodes))]
    assert order == [3, 5, 9, 1]


def test_render_does_not_mutate_live_settings():
    graph = _pipeline()["graph"]
    before = _live_dumps(graph)
    render(graph)
    assert _live_dumps(graph) == before


def test_fingerprint_ignores_moves_and_tracks_settings_edits():
    pipeline = _pipeline()
    graph = pipeline["graph"]
    original = code_fingerprint(graph)
    node = graph.get_node(pipeline["joined"].node_id)
    node.setting_input.pos_x = (node.setting_input.pos_x or 0) + 250
    node.setting_input.pos_y = (node.setting_input.pos_y or 0) - 40
    assert code_fingerprint(graph) == original
    assert render(graph).code_fingerprint == original
    node.setting_input.join_input.how = "inner"
    assert code_fingerprint(graph) != original


def test_fingerprint_tracks_parameters():
    graph = _pipeline()["graph"]
    original = code_fingerprint(graph)
    graph.flow_settings.parameters[0].default_value = "3"
    assert code_fingerprint(graph) != original


def test_user_description_renders_and_auto_text_does_not():
    graph = FlowGraph()
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist([{"a": 1}]),
            description='Raw "orders"',
        )
    )
    graph.add_sort(
        input_schema.NodeSort(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            sort_input=[input_schema.transform_schema.SortByInput(column="a", how="desc")],
        )
    )
    _connect(graph, 1, 2)
    cells = _cells_by_node(render(graph))
    assert cells[1].code.endswith('description="Raw \\"orders\\"")')
    assert "description" not in cells[2].code
    compile(cells[1].code, "c", "exec")


def test_unconfigured_node_and_its_downstream_are_placeholders():
    pipeline = _pipeline()
    graph = pipeline["graph"]
    joined_id = pipeline["joined"].node_id
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=900, node_type="select"))
    _connect(graph, joined_id, 900)
    graph.add_record_count(input_schema.NodeRecordCount(flow_id=graph.flow_id, node_id=901, depending_on_id=900))
    _connect(graph, 900, 901)
    rendering = render(graph)
    cells = _cells_by_node(rendering)
    promise, downstream = cells[900], cells[901]
    assert promise.status == "placeholder" and promise.reason == "not configured yet"
    assert promise.code.startswith(f"selected_900 = fl.canvas_node(900, joined_{joined_id})  # ")
    assert downstream.status == "placeholder" and "downstream of node 900" in downstream.reason
    assert downstream.code.startswith("counted_901 = fl.canvas_node(901, selected_900)")
    assert downstream.uses == ["fl", "selected_900"]
    for cell in (promise, downstream):
        compile(cell.code, cell.cell_id, "exec")
    assert cells[joined_id].status == "code"


def test_polars_lazy_frame_is_unsupported_and_blocks_downstream():
    lazy = ff.FlowFrame(pl.LazyFrame({"a": [1, 2]}))
    graph = lazy.flow_graph
    filtered = lazy.filter(ff.col("a") > 1)
    cells = _cells_by_node(render(graph))
    source = cells[lazy.node_id]
    assert source.status == "unsupported"
    var = f"polars_lazy_frame_{lazy.node_id}"
    assert source.code.startswith(f"{var} = fl.canvas_node({lazy.node_id})  # ")
    assert cells[filtered.node_id].status == "placeholder"
    assert var in cells[filtered.node_id].uses


def test_explore_data_rest_credentials_and_native_types_are_placeholders():
    graph = FlowGraph()
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id, node_id=1, raw_data_format=input_schema.RawData.from_pylist([{"a": 1}])
        )
    )
    graph.add_explore_data(input_schema.NodeExploreData(flow_id=graph.flow_id, node_id=2))
    _connect(graph, 1, 2)
    graph.add_rest_api_reader(
        input_schema.NodeRestApiReader(
            flow_id=graph.flow_id,
            node_id=3,
            rest_api_settings=input_schema.RestApiSettings(
                url="http://127.0.0.1:9/items", headers={"Authorization": "Bearer abc"}
            ),
        )
    )
    graph.add_rest_api_reader(
        input_schema.NodeRestApiReader(
            flow_id=graph.flow_id,
            node_id=4,
            rest_api_settings=input_schema.RestApiSettings(url="http://127.0.0.1:9/items", query_params={"page": "1"}),
        )
    )
    cells = _cells_by_node(render(graph))
    assert cells[2].status == "placeholder"
    assert cells[2].code.startswith("explore_data_2 = fl.canvas_node(2, source_1)  # ")
    assert cells[3].status == "placeholder" and "credential" in cells[3].reason
    assert "Bearer" not in cells[3].code
    assert cells[4].status == "code" and "fl.read_api(" in cells[4].code
    for cell in cells.values():
        compile(cell.code, cell.cell_id, "exec")


def test_gate_renders_as_fl_gate_and_consumers_bind_its_exit():
    source = ff.from_dict({"a": [1, 2]})
    gate = ff.Gate(source, formula="[a] > 1", else_output=False)
    after = gate.then.select("a")
    graph = after.flow_graph
    cells = _cells_by_node(render(graph))
    gate_cell = cells[gate.node_id]
    assert gate_cell.status == "code"
    assert gate_cell.code == f'gate_{gate.node_id} = fl.Gate(source_{source.node_id}, "[a] > 1", else_output=False)'
    assert f"gate_{gate.node_id}.then" in cells[after.node_id].code
    assert f"gate_{gate.node_id}" in cells[after.node_id].uses


def test_handler_failure_becomes_placeholder_without_raising(monkeypatch):
    from flowfile_core.flowfile.code_generator.code_generator import FlowGraphToFlowFrameConverter

    def boom(self, settings, var_name, input_vars):
        self._add_code(f"{var_name} = broken(")
        raise RuntimeError("handler exploded")

    monkeypatch.setattr(FlowGraphToFlowFrameConverter, "_handle_sort", boom, raising=False)
    pipeline = _pipeline()
    rendering = render(pipeline["graph"])
    sort_cell = next(c for c in rendering.cells if c.kind == "node" and "ordered_" in (c.defines or [""])[0])
    assert sort_cell.status == "placeholder" and "handler exploded" in sort_cell.reason
    assert "broken(" not in "\n".join(c.code for c in rendering.cells)
    for cell in rendering.cells:
        compile(cell.code, cell.cell_id, "exec")


@pytest.mark.parametrize("reverse", [False, True])
def test_every_cell_compiles_for_the_demo_style_mix(reverse):
    graph = _pipeline()["graph"]
    if reverse:
        graph._node_db = dict(reversed(list(graph._node_db.items())))
    for cell in render(graph).cells:
        compile(cell.code, cell.cell_id, "exec")


def test_parameters_cell_runs_against_the_frame_api():
    rendering = render(_pipeline()["graph"])
    params = next(c for c in rendering.cells if c.cell_id == PARAMETERS_CELL_ID)
    flow = FlowGraph()
    namespace = {"fl": ff, "flow": flow}
    exec(compile(params.code, "parameters", "exec"), namespace)
    assert [(p.name, p.type, p.default_value) for p in flow.flow_settings.parameters] == [
        ("factor", "integer", "2"),
        ("label", "string", "big"),
    ]


def test_no_parameters_cell_without_parameters_and_fingerprint_ignores_flow_id():
    graph = FlowGraph()
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id, node_id=1, raw_data_format=input_schema.RawData.from_pylist([{"a": 1}])
        )
    )
    rendering = render(graph)
    assert [c.cell_id for c in rendering.cells] == [IMPORTS_CELL_ID, "node-1"]
    original = rendering.code_fingerprint
    graph.get_node(1).setting_input.flow_id = graph.flow_id + 1
    assert code_fingerprint(graph) == original
