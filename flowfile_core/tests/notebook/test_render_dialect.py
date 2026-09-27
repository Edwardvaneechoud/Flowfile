"""The notebook dialect's fluent forms (plan section 3 item 5): parameter comparisons, frame methods, cosmetics.

Canvas flows are built through core ``add_*`` or ``flowfile_frame``; where exactness matters the rendered
cells run as a clean run in notebook mode and the rebuilt node is compared with its canvas twin.
"""

import pytest

import flowfile_frame as ff
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.notebook.compare import param_comparison_filter, settings_equal
from flowfile_core.notebook.render import render
from flowfile_core.schemas import input_schema, transform_schema
from flowfile_frame import notebook
from flowfile_frame.notebook_cells import clean_run, seed_session
from tests.notebook.test_render_native import _assert_exact, _cells_by_node


def _people() -> ff.FlowFrame:
    return ff.from_dict({"id": [1, 2, 3], "name": ["a", "b", "c"], "salary": [10, 20, 30], "city": ["x", "y", "x"]})


def _add(graph: FlowGraph, settings, method: str, *inputs: int) -> int:
    getattr(graph, method)(settings)
    for source in inputs:
        connection = input_schema.NodeConnection.create_from_simple_input(source, settings.node_id)
        add_connection(graph, connection)
    return settings.node_id


def _basic_filter(source: ff.FlowFrame, basic: transform_schema.BasicFilter) -> tuple[FlowGraph, int]:
    graph = source.flow_graph
    ff.add_flow_parameter(graph, ff.Parameter("min_salary", default=15, type="integer"))
    ff.add_flow_parameter(graph, ff.Parameter("max_salary", default=25, type="integer"))
    node_id = max(node.node_id for node in graph.nodes) + 1
    settings = input_schema.NodeFilter(
        flow_id=graph.flow_id,
        node_id=node_id,
        depending_on_id=source.node_id,
        filter_input=transform_schema.FilterInput(mode="basic", basic_filter=basic),
    )
    return graph, _add(graph, settings, "add_filter", source.node_id)


def _rerendered(graph: FlowGraph, node_id: int) -> str:
    """The cell of ``node_id`` once the clean-run payload is rendered again."""
    rendering = render(graph)
    cells = [(cell.cell_id, cell.code) for cell in rendering.cells]
    provenance = {
        cell.cell_id: [(graph.get_node(n).node_type, n) for n in cell.node_ids] for cell in rendering.cells if cell.node_ids
    }
    payload = clean_run(cells, max(node.node_id for node in graph.nodes), provenance)["flowfile_data"]
    bound = seed_session(payload, payload["flowfile_settings"]["parameters"], {}, {})
    try:
        return _cells_by_node(render(bound["flow"]))[node_id].code
    finally:
        notebook.exit()


@pytest.mark.parametrize(
    "basic,expected",
    [
        (transform_schema.BasicFilter(field="salary", operator=">", value="${min_salary}"), '> MIN_SALARY'),
        (
            transform_schema.BasicFilter(field="salary", operator="between", value="${min_salary}", value2="${max_salary}"),
            '(fl.col("salary") >= MIN_SALARY) & (fl.col("salary") <= MAX_SALARY)',
        ),
    ],
    ids=["comparison", "between"],
)
def test_a_whole_field_parameter_value_renders_the_parameter_variable(basic, expected):
    graph, node_id = _basic_filter(_people(), basic)
    code = _cells_by_node(render(graph))[node_id].code
    assert expected in code and '"${' not in code
    _assert_exact(graph, node_id)
    assert _rerendered(graph, node_id) == code


def test_a_formula_holding_a_parameter_keeps_its_verbatim_form():
    source = _people()
    ff.add_flow_parameter(source, ff.Parameter("min_salary", default=15, type="integer"))
    filtered = source.filter(flowfile_formula="[salary] > ${min_salary} and [city] = 'x'")
    code = _cells_by_node(render(filtered.flow_graph))[filtered.node_id].code
    assert "flowfile_formula=" in code and "${min_salary}" in code


def test_an_undeclared_parameter_value_keeps_the_literal():
    graph, node_id = _basic_filter(_people(), transform_schema.BasicFilter(field="city", operator="=", value="${nope}"))
    assert '== "${nope}"' in _cells_by_node(render(graph))[node_id].code


@pytest.mark.parametrize(
    "code",
    [
        "input_df.with_columns(pl.col('salary') * 2)",
        "output_df = input_df.filter(pl.col('salary') > 15)\nreturn output_df",
        "doubled = input_df.with_columns(pl.col('salary') * 2)",
    ],
    ids=["expression", "explicit_return", "other_name"],
)
def test_polars_code_renders_as_the_frame_method_over_a_def(code):
    source = _people()
    graph = source.flow_graph
    settings = input_schema.NodePolarsCode(
        flow_id=graph.flow_id,
        node_id=source.node_id + 1,
        depending_on_ids=[source.node_id],
        polars_code_input=transform_schema.PolarsCodeInput(polars_code=code),
        description="Doubles salary",
    )
    node_id = _add(graph, settings, "add_polars_code", source.node_id)
    rendering = render(graph)
    cell = _cells_by_node(rendering)[node_id].code
    assert f"def _polars_code_{node_id}(input_df: fl.FlowFrame):" in cell
    assert f'.polars_code(_polars_code_{node_id}, description="Doubles salary")' in cell
    assert "pl.col('salary')" in cell and "import polars as pl" in rendering.cells[0].code
    _assert_exact(graph, node_id)


def test_record_id_renders_with_row_index_with_its_groups():
    source = _people()
    graph = source.flow_graph
    settings = input_schema.NodeRecordId(
        flow_id=graph.flow_id,
        node_id=source.node_id + 1,
        depending_on_id=source.node_id,
        record_id_input=transform_schema.RecordIdInput(
            output_column_name="n", offset=5, group_by=True, group_by_columns=["city"]
        ),
    )
    node_id = _add(graph, settings, "add_record_id", source.node_id)
    code = _cells_by_node(render(graph))[node_id].code
    assert code.endswith(f'source_{source.node_id}.with_row_index("n", offset=5, group_by=[\'city\'])')
    _assert_exact(graph, node_id)


def test_unpivot_of_several_columns_renders_the_native_method():
    source = _people()
    graph = source.flow_graph
    settings = input_schema.NodeUnpivot(
        flow_id=graph.flow_id,
        node_id=source.node_id + 1,
        depending_on_id=source.node_id,
        unpivot_input=transform_schema.UnpivotInput(index_columns=["id"], value_columns=["name", "city"]),
    )
    node_id = _add(graph, settings, "add_unpivot", source.node_id)
    code = _cells_by_node(render(graph))[node_id].code
    assert code.endswith(".unpivot(on=['name', 'city'], index=['id'])")
    _assert_exact(graph, node_id)


def test_a_keep_missing_select_lists_the_columns_it_passes_through():
    source = _people()
    graph = source.flow_graph
    settings = input_schema.NodeSelect(
        flow_id=graph.flow_id,
        node_id=source.node_id + 1,
        depending_on_id=source.node_id,
        keep_missing=True,
        select_input=[transform_schema.SelectInput("name", "full_name"), transform_schema.SelectInput("city", keep=False)],
    )
    node_id = _add(graph, settings, "add_select", source.node_id)
    code = _cells_by_node(render(graph))[node_id].code
    assert 'fl.col("id")' in code and 'fl.col("salary")' in code and 'fl.col("city")' not in code


def test_a_description_difference_never_makes_a_row_differ():
    sort = {"sort_input": [{"column": "a", "how": "asc"}]}
    canvas = {**sort, "description": "Sort by a", "description_is_auto_generated": True}
    assert settings_equal(canvas, sort, "sort")
    assert settings_equal({**sort, "description": "mine"}, {**sort, "description": ""}, "sort")


def test_a_parameter_comparison_equals_its_basic_filter():
    basic = {"filter_input": {"mode": "basic", "basic_filter": {"field": "x", "operator": ">", "value": "${p}"}}}
    advanced = {"filter_input": {"mode": "advanced", "advanced_filter": "([x] > ${p})"}}
    assert settings_equal(basic, advanced, "filter")
    between = param_comparison_filter("(([x] >= ${a}) and ([x] <= ${b}))")
    assert between["basic_filter"] == {"field": "x", "operator": "between", "value": "${a}", "value2": "${b}"}
    assert param_comparison_filter("([x] > ${p}) and ([y] < 2)") is None


def test_a_closing_return_output_df_and_stale_group_columns_are_cosmetic():
    code = {"polars_code_input": {"polars_code": "output_df = input_df\nreturn output_df"}}
    assert settings_equal(code, {"polars_code_input": {"polars_code": "  output_df = input_df"}}, "polars_code")
    grouped = {"record_id_input": {"output_column_name": "n", "group_by": False, "group_by_columns": ["a"]}}
    plain = {"record_id_input": {"output_column_name": "n", "group_by": False, "group_by_columns": []}}
    assert settings_equal(grouped, plain, "record_id")
