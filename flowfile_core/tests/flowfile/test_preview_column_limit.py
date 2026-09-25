"""A data preview carries at most MAX_PREVIEW_COLUMNS columns; schemas used for prediction stay complete."""

import pytest

from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, schemas, transform_schema
from flowfile_core.schemas.output_model import MAX_PREVIEW_COLUMNS

WIDTH = MAX_PREVIEW_COLUMNS + 5
COLUMNS = [f"c{i}" for i in range(WIDTH)]
KEPT = COLUMNS[:MAX_PREVIEW_COLUMNS]


def _create_graph(execution_location: str) -> FlowGraph:
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=1, name="wide", path=".", execution_mode="Development", execution_location=execution_location
        )
    )
    graph = handler.get_flow(1)
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=1, node_type="manual_input"))
    rows = [{name: r * WIDTH + i for i, name in enumerate(COLUMNS)} for r in range(3)]
    graph.add_manual_input(
        input_schema.NodeManualInput(flow_id=1, node_id=1, raw_data_format=input_schema.RawData.from_pylist(rows))
    )
    return graph


def _add_downstream(graph: FlowGraph, node_type: str, node_id: int = 2) -> None:
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=node_id, node_type=node_type))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, node_id))


def _assert_capped(example) -> None:
    assert example.number_of_columns == WIDTH
    assert [c.name for c in example.table_schema] == KEPT
    assert example.columns == KEPT
    assert example.data, "preview should carry rows"
    assert all(list(row) == KEPT for row in example.data)


def test_preview_is_capped_after_run(execution_location):
    graph = _create_graph(execution_location)
    run_info = graph.run_graph()
    assert run_info.success

    example = graph.get_node(1).get_table_example(True)

    assert example.has_example_data
    _assert_capped(example)
    assert example.data[0]["c0"] == 0


def test_named_output_preview_is_capped(execution_location):
    graph = _create_graph(execution_location)
    _add_downstream(graph, "filter")
    graph.add_filter(
        input_schema.NodeFilter(
            flow_id=1,
            node_id=2,
            depending_on_id=1,
            split_mode=True,
            filter_input=transform_schema.FilterInput(
                mode="basic",
                basic_filter=transform_schema.BasicFilter(
                    field="c0", operator=transform_schema.FilterOperator.EQUALS, value="0"
                ),
            ),
        )
    )
    run_info = graph.run_graph()
    assert run_info.success

    _assert_capped(graph.get_node(2).get_table_example(True, output_handle="output-1"))


def test_not_yet_run_preview_caps_the_schema():
    graph = _create_graph("local")

    example = graph.get_node(1).get_table_example(True)

    assert example.number_of_columns == WIDTH
    assert [c.name for c in example.table_schema] == KEPT
    assert example.data == []


def test_schema_only_callers_keep_every_column():
    graph = _create_graph("local")
    _add_downstream(graph, "select")

    assert len(graph.get_node(1).get_table_example().table_schema) == WIDTH
    node_data = graph.get_node(2).get_node_data(flow_id=1, include_output=False)
    assert [c.name for c in node_data.main_input.table_schema] == COLUMNS


@pytest.mark.parametrize("width", [3, MAX_PREVIEW_COLUMNS])
def test_narrow_preview_is_untouched(width):
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(flow_id=1, name="narrow", path=".", execution_mode="Development", execution_location="local")
    )
    graph = handler.get_flow(1)
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=1, node_type="manual_input"))
    names = [f"c{i}" for i in range(width)]
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=1, node_id=1, raw_data_format=input_schema.RawData.from_pylist([dict.fromkeys(names, 1)])
        )
    )
    assert graph.run_graph().success

    example = graph.get_node(1).get_table_example(True)

    assert example.number_of_columns == width
    assert example.columns == names
    assert list(example.data[0]) == names
