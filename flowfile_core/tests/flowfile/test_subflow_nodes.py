"""Standalone behavior of the flow_input / flow_output nodes + YAML roundtrip."""

import pytest

from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.schemas import input_schema, schemas
from flowfile_core.schemas.schemas import FlowParameter


def make_graph(flow_id: int = 201) -> FlowGraph:
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id,
            name="subflow_nodes_test",
            path=".",
            execution_mode="Development",
            execution_location="local",
        )
    )
    return handler.get_flow(flow_id)


def sample_raw_data() -> input_schema.RawData:
    return input_schema.RawData.from_pylist(
        [
            {"name": "Alice", "city": "Amsterdam"},
            {"name": "Bob", "city": "Berlin"},
            {"name": "Cara", "city": "Copenhagen"},
        ]
    )


def add_promise(graph: FlowGraph, node_id: int, node_type: str) -> None:
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type=node_type))


def run_and_assert_ok(graph: FlowGraph) -> None:
    result = graph.run_graph()
    assert result is not None
    if not result.success:
        errors = "\n".join(f"  node {r.node_id}: {r.error}" for r in result.node_step_result if not r.success)
        raise AssertionError(f"Flow failed:\n{errors}")


def build_passthrough_subflow(graph: FlowGraph) -> None:
    add_promise(graph, 1, "flow_input")
    graph.add_flow_input(
        input_schema.NodeFlowInput(flow_id=graph.flow_id, node_id=1, input_name="customers", raw_data_format=sample_raw_data())
    )
    add_promise(graph, 2, "flow_output")
    graph.add_flow_output(
        input_schema.NodeFlowOutput(flow_id=graph.flow_id, node_id=2, output_name="result", depending_on_id=1)
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))


def test_flow_input_to_flow_output_standalone_run():
    graph = make_graph(201)
    build_passthrough_subflow(graph)
    run_and_assert_ok(graph)
    df = graph.get_node(2).get_resulting_data().collect()
    assert df.height == 3
    assert set(df.columns) == {"name", "city"}


def test_flow_input_without_sample_data_runs_empty():
    graph = make_graph(202)
    add_promise(graph, 1, "flow_input")
    graph.add_flow_input(input_schema.NodeFlowInput(flow_id=graph.flow_id, node_id=1, input_name="empty_in"))
    run_and_assert_ok(graph)
    assert graph.get_node(1).get_resulting_data().collect().height == 0


def test_duplicate_flow_input_name_rejected():
    graph = make_graph(203)
    add_promise(graph, 1, "flow_input")
    graph.add_flow_input(input_schema.NodeFlowInput(flow_id=graph.flow_id, node_id=1, input_name="same"))
    add_promise(graph, 2, "flow_input")
    with pytest.raises(ValueError, match="already used"):
        graph.add_flow_input(input_schema.NodeFlowInput(flow_id=graph.flow_id, node_id=2, input_name="same"))


def test_duplicate_flow_output_name_rejected():
    graph = make_graph(204)
    build_passthrough_subflow(graph)
    add_promise(graph, 3, "flow_output")
    with pytest.raises(ValueError, match="already used"):
        graph.add_flow_output(
            input_schema.NodeFlowOutput(flow_id=graph.flow_id, node_id=3, output_name="result", depending_on_id=1)
        )


def test_invalid_port_names_rejected():
    with pytest.raises(ValueError):
        input_schema.NodeFlowInput(flow_id=1, node_id=1, input_name="1starts_with_digit")
    with pytest.raises(ValueError):
        input_schema.NodeFlowOutput(flow_id=1, node_id=1, output_name="has space")


def test_yaml_roundtrip_preserves_subflow_nodes_and_typed_params(tmp_path):
    graph = make_graph(205)
    build_passthrough_subflow(graph)
    graph.flow_settings.parameters = [
        FlowParameter(name="limit", default_value="5", type="integer"),
        FlowParameter(name="mode", default_value="a", type="enum", enum_values=["a", "b"]),
    ]
    flow_path = tmp_path / "subflow_roundtrip.yaml"
    graph.save_flow(str(flow_path))

    reloaded = open_flow(flow_path)
    input_node = reloaded.get_node(1)
    output_node = reloaded.get_node(2)
    assert input_node.node_type == "flow_input"
    assert input_node.setting_input.input_name == "customers"
    assert input_node.setting_input.raw_data_format is not None
    assert len(input_node.setting_input.raw_data_format.columns) == 2
    assert output_node.node_type == "flow_output"
    assert output_node.setting_input.output_name == "result"

    params = {p.name: p for p in reloaded.flow_settings.parameters}
    assert params["limit"].type == "integer"
    assert params["limit"].typed_default() == 5
    assert params["mode"].type == "enum"
    assert params["mode"].enum_values == ["a", "b"]

    run_and_assert_ok(reloaded)
    assert reloaded.get_node(2).get_resulting_data().collect().height == 3
