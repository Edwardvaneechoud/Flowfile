"""The notebook rendering: the FlowFrame export split into cells, each carrying the node ids of its statement."""

import pytest

import flowfile_frame as ff
from flowfile_core.flowfile.code_generator.code_generator import FlowGraphToFlowFrameConverter
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.notebook.render import code_fingerprint, render
from flowfile_core.schemas import input_schema, transform_schema
from flowfile_frame import notebook
from flowfile_frame.notebook_cells import clean_run
from tests.notebook.conftest import NOTEBOOK_OWNER_ID, RUNNERS
from tests.notebook.test_ledger import grade


def _pipeline() -> FlowGraph:
    """A fused chain, a join boundary, a described node and ``${param}`` references in text and whole fields."""
    orders = ff.from_dict({"id": [1, 2, 3], "amount": [10, 20, 30], "region": ["n", "s", "n"]})
    graph = orders.flow_graph
    ff.add_flow_parameter(graph, ff.Parameter("factor", default=2, type="integer"))
    ff.add_flow_parameter(graph, ff.Parameter("label", default="big"))
    regions = ff.from_dict({"region": ["n", "s"], "name": ["North", "South"]}, flow_graph=graph)
    scaled = orders.filter(ff.col("amount") > 10).with_columns(
        flowfile_formulas=["[amount] * ${factor}"], output_column_names=["scaled"]
    )
    joined = scaled.join(regions, on="region", how="left", description="Add region names")
    return joined.filter(flowfile_formula="[region] = '${label}'").flow_graph


def test_cells_cover_every_node_once_in_statement_order():
    graph = _pipeline()
    rendering = render(graph)
    assert [cell.kind for cell in rendering.cells[:2]] == ["imports", "parameters"]
    node_cells = [cell for cell in rendering.cells if cell.kind == "node"]
    assert sorted(n for cell in node_cells for n in cell.node_ids) == sorted(n.node_id for n in graph.nodes)
    assert any(len(cell.node_ids) > 1 for cell in node_cells)
    assert all(cell.cell_id == f"cell-{cell.node_ids[0]}" for cell in node_cells)
    for cell in rendering.cells:
        compile(cell.code, cell.cell_id, "exec")
    assert rendering.var_by_node[node_cells[-1].node_ids[-1]] in node_cells[-1].defines


def test_names_refs_and_descriptions_are_what_the_frame_stores_back():
    rendering = render(_pipeline())
    code = "\n".join(cell.code for cell in rendering.cells)
    assert 'factor = ff.add_flow_parameter(flow, ff.Parameter("factor", default=2, type="integer"))' in code
    assert "[amount] * ${factor}" in code and "[region] = '${label}'" in code and "__FF_PARAM" not in code
    assert 'description="Add region names"' in code
    joined = next(cell for cell in rendering.cells if 'description="Add region names"' in cell.code)
    assert joined.defines == [f"joined_{joined.node_ids[-1]}"]


def test_the_cells_rebuild_every_node_exactly(runner_kind):
    graph = _pipeline()
    rendering = render(graph)
    provenance = {c.cell_id: [(graph.get_node(n).node_type, n) for n in c.node_ids] for c in rendering.cells}
    try:
        cells = [(c.cell_id, c.code) for c in rendering.cells]
        ceiling = max(n.node_id for n in graph.nodes)
        executor = RUNNERS[runner_kind].executor()
        result = clean_run(cells, ceiling, provenance, user_id=NOTEBOOK_OWNER_ID, executor=executor)
    finally:
        if notebook.current() is not None:
            notebook.exit()
    assert result["ok"], result.get("error")
    assert set(grade(graph, result).values()) == {"EXACT"}


@pytest.mark.parametrize(
    "code",
    [
        pytest.param("def kept(rows: LazyFrame) -> LazyFrame:\n    return rows", id="bare_name_annotations"),
        pytest.param("def kept(rows):\n    return rows\n# trailing note", id="trailing_comment"),
        pytest.param("# heading\n\ndef kept(rows):\n    return rows", id="blank_line_under_comment"),
        pytest.param('"""About it."""\ndef kept(rows):\n    return rows', id="module_docstring"),
        pytest.param("def _polars_code_5(input_df):\n    return input_df", id="snippet_name"),
        pytest.param("@staticmethod\ndef kept(rows):\n    return rows", id="decorated"),
        pytest.param("def _kept(rows):\n    return rows", id="underscore_name"),
    ],
)
def test_an_unedited_polars_code_function_pushes_back_unchanged(code):
    source = ff.from_dict({"a": [1, 2]})
    graph = source.flow_graph
    graph.add_polars_code(
        input_schema.NodePolarsCode(
            flow_id=graph.flow_id,
            node_id=900,
            depending_on_ids=[source.node_id],
            polars_code_input=transform_schema.PolarsCodeInput(polars_code=code),
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(source.node_id, 900))
    rendering = render(graph)
    assert "__future__" not in rendering.cells[0].code
    provenance = {c.cell_id: [(graph.get_node(n).node_type, n) for n in c.node_ids] for c in rendering.cells}
    try:
        cells = [(c.cell_id, c.code) for c in rendering.cells]
        executor = RUNNERS["interpreting"].executor()
        result = clean_run(cells, 900, provenance, user_id=NOTEBOOK_OWNER_ID, executor=executor)
    finally:
        if notebook.current() is not None:
            notebook.exit()
    assert result["ok"], result.get("error")
    assert set(grade(graph, result).values()) == {"EXACT"}


def test_parameters_cell_runs_against_the_frame_api():
    params = next(cell for cell in render(_pipeline()).cells if cell.kind == "parameters")
    flow = FlowGraph()
    namespace = {"ff": ff, "flow": flow}
    exec(compile(params.code, "parameters", "exec"), namespace)
    assert isinstance(namespace["factor"], ff.Parameter)
    assert [(p.name, p.type, p.default_value) for p in flow.flow_settings.parameters] == [
        ("factor", "integer", "2"),
        ("label", "string", "big"),
    ]


def test_an_unconfigured_node_and_its_downstream_are_placeholders():
    graph = _pipeline()
    last = graph.nodes[-1].node_id
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=900, node_type="select"))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(last, 900))
    graph.add_record_count(input_schema.NodeRecordCount(flow_id=graph.flow_id, node_id=901, depending_on_id=900))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(900, 901))
    cells = {cell.node_ids[-1]: cell for cell in render(graph).cells if cell.node_ids}
    promise, downstream = cells[900], cells[901]
    assert (promise.status, promise.reason) == ("placeholder", "not configured yet")
    assert promise.code.startswith(f"selected_900 = ff.canvas_node(900, filtered_{last})  # ")
    assert downstream.reason == "downstream of node 900, which is not editable as code"
    assert downstream.code.startswith("counted_901 = ff.canvas_node(901, selected_900)")
    assert downstream.uses == ["ff", "selected_900"]


def test_render_does_not_mutate_live_settings():
    graph = _pipeline()
    before = {n.node_id: n.setting_input.model_dump(mode="json") for n in graph.nodes}
    render(graph)
    assert {n.node_id: n.setting_input.model_dump(mode="json") for n in graph.nodes} == before


def test_fingerprint_ignores_moves_and_flow_id_and_tracks_settings_and_parameters():
    graph = _pipeline()
    original = code_fingerprint(graph)
    node = graph.nodes[-1]
    node.setting_input.pos_x = (node.setting_input.pos_x or 0) + 250
    node.setting_input.flow_id = graph.flow_id + 1
    assert code_fingerprint(graph) == render(graph).code_fingerprint == original
    graph.flow_settings.parameters[0].default_value = "3"
    assert code_fingerprint(graph) != original


def test_an_edit_during_the_export_leaves_the_pre_edit_fingerprint(monkeypatch):
    graph = _pipeline()
    before = code_fingerprint(graph)
    convert = FlowGraphToFlowFrameConverter.convert

    def convert_then_edit(self):
        code = convert(self)
        graph.flow_settings.parameters[0].default_value = "3"
        return code

    monkeypatch.setattr(FlowGraphToFlowFrameConverter, "convert", convert_then_edit)
    assert render(graph).code_fingerprint == before != code_fingerprint(graph)


def test_an_export_failure_raises_instead_of_rendering_no_node_cells(monkeypatch):
    graph = _pipeline()

    def fail(self):
        raise RuntimeError("boom")

    monkeypatch.setattr(FlowGraphToFlowFrameConverter, "convert", fail)
    with pytest.raises(RuntimeError, match="boom"):
        render(graph)


def _grouped_pipeline() -> FlowGraph:
    outer = ff.FlowGroup("Clean", color="blue")
    inner = ff.FlowGroup("Inner", parent_group=outer)
    orders = ff.from_dict({"id": [1, 2, 3], "amount": [10, 20, 30]})
    kept = orders.filter(ff.col("amount") > 10).add_to_group(outer)
    return kept.with_columns((ff.col("amount") * 2).alias("double")).add_to_group(inner).flow_graph


def test_groups_render_as_their_own_cell_before_the_node_cells():
    graph = _grouped_pipeline()
    rendering = render(graph)
    assert [cell.kind for cell in rendering.cells] == ["imports", "groups", "node"]
    groups = rendering.cells[1]
    assert groups.cell_id == "groups" and groups.node_ids == []
    assert (
        groups.code == 'clean = ff.FlowGroup("Clean", color="blue")\ninner = ff.FlowGroup("Inner", parent_group=clean)'
    )
    assert groups.defines == ["clean", "inner"] and groups.uses == ["ff"]
    node_cell = rendering.cells[2]
    assert node_cell.uses == ["clean", "ff", "inner"]
    assert '.filter(ff.col("amount") > 10).add_to_group(clean)' in node_cell.code
    assert node_cell.code.endswith('.alias("double")).add_to_group(inner)\n)')


def test_the_grouped_cells_rebuild_the_groups(runner_kind):
    graph = _grouped_pipeline()
    rendering = render(graph)
    provenance = {c.cell_id: [(graph.get_node(n).node_type, n) for n in c.node_ids] for c in rendering.cells}
    cells = [(c.cell_id, c.code) for c in rendering.cells]
    with notebook.notebook_mode(user_id=NOTEBOOK_OWNER_ID):
        result = clean_run(
            cells, max(n.node_id for n in graph.nodes), provenance, executor=RUNNERS[runner_kind].executor()
        )
    assert result["ok"], result
    groups = {g["name"]: g for g in result["flowfile_data"]["groups"]}
    assert groups["Inner"]["parent_group_id"] == groups["Clean"]["id"] and groups["Clean"]["color"] == "blue"
    members = {n["id"]: n["group_id"] for n in result["flowfile_data"]["nodes"]}
    filtered, formula = (next(n.node_id for n in graph.nodes if n.node_type == t) for t in ("filter", "formula"))
    assert members[filtered] == groups["Clean"]["id"] and members[formula] == groups["Inner"]["id"]


def test_fingerprint_tracks_groups_but_not_their_boxes():
    graph = _grouped_pipeline()
    before = code_fingerprint(graph)
    inner = next(g for g in graph._groups.values() if g.name == "Inner")
    graph.update_group(inner.id, bounds=(1.0, 2.0, 300.0, 200.0), collapsed=True)
    assert code_fingerprint(graph) == before
    graph.update_group(inner.id, name="Renamed")
    renamed = code_fingerprint(graph)
    assert renamed != before
    node = next(n for n in graph.nodes if n.node_type == "formula")
    graph.remove_nodes_from_group([node.node_id])
    assert code_fingerprint(graph) != renamed
