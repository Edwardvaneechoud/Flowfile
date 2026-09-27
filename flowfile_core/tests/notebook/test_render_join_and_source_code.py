"""The two rows the frame closed for the ledger: a join keeping its right keys and a Polars-code node with no input.

Each canvas node is built through core ``add_*``, rendered, clean-run in notebook mode and compared with
its rebuilt twin; the rebuilt payload must render back to the same cell.
"""

from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.notebook.render import render
from flowfile_core.schemas import input_schema, schemas, transform_schema
from tests.notebook.test_render_dialect import _rerendered
from tests.notebook.test_render_native import _assert_exact, _cells_by_node


def _manual(graph: FlowGraph, node_id: int, columns: dict[str, tuple[str, list]]) -> None:
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=1,
            node_id=node_id,
            raw_data_format=input_schema.RawData(
                columns=[input_schema.MinimalFieldInfo(name=n, data_type=t) for n, (t, _) in columns.items()],
                data=[values for _, values in columns.values()],
            ),
        )
    )


def _join_flow(right_select: list[transform_schema.SelectInput], how: str = "inner") -> FlowGraph:
    graph = FlowGraph(flow_settings=schemas.FlowSettings(flow_id=1, name="join_keeps_keys", path="."))
    _manual(graph, 1, {"id": ("Integer", [1, 2, 3]), "city": ("String", ["a", "b", "c"])})
    _manual(graph, 2, {"id": ("Integer", [1, 2, 4]), "city": ("String", ["x", "y", "z"]), "n": ("Integer", [1, 2, 3])})
    graph.add_join(
        input_schema.NodeJoin(
            flow_id=1,
            node_id=3,
            depending_on_ids=[1, 2],
            join_input=transform_schema.JoinInput(
                join_mapping=[transform_schema.JoinMap("id", "id")],
                left_select=[transform_schema.SelectInput("id"), transform_schema.SelectInput("city")],
                right_select=right_select,
                how=how,
            ),
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 3, "main"))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(2, 3, "right"))
    return graph


def test_a_join_keeping_its_right_key_renders_as_one_native_join():
    right = [transform_schema.SelectInput(name) for name in ("id", "city", "n")]
    graph = _join_flow(right, how="left")
    code = _cells_by_node(render(graph))[3].code
    assert "keep_right_keys=True" in code and "coalesce" not in code and "suffix" not in code
    assert code.count(".join(") == 1 and "__DROP__" not in code
    _assert_exact(graph, 3)
    assert _rerendered(graph, 3) == code


def test_a_join_renaming_its_right_clashes_with_one_suffix_renders_the_suffix():
    right = [
        transform_schema.SelectInput("id", "id_r"),
        transform_schema.SelectInput("city", "city_r"),
        transform_schema.SelectInput("n"),
    ]
    graph = _join_flow(right)
    code = _cells_by_node(render(graph))[3].code
    assert 'suffix="_r"' in code and "keep_right_keys=True" in code
    _assert_exact(graph, 3)
    assert _rerendered(graph, 3) == code


def test_a_polars_code_node_without_input_renders_as_fl_polars_code():
    graph = FlowGraph(flow_settings=schemas.FlowSettings(flow_id=1, name="source_code", path="."))
    graph.add_polars_code(
        input_schema.NodePolarsCode(
            flow_id=1,
            node_id=1,
            depending_on_ids=[],
            polars_code_input=transform_schema.PolarsCodeInput(
                polars_code="output_df = pl.LazyFrame({'a': [1, 2, 3]})\nreturn output_df"
            ),
            description="three rows",
        )
    )
    rendering = render(graph)
    code = _cells_by_node(rendering)[1].code
    assert "def _polars_code_1():" in code
    assert 'fl.polars_code(_polars_code_1, description="three rows")' in code
    assert "import polars as pl" in rendering.cells[0].code
    _assert_exact(graph, 1)
    assert _rerendered(graph, 1) == code
