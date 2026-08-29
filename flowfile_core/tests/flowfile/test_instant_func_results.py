"""Edit-time instant-result validation for the gate node's formula editor.

A formula gate evaluates its condition against the control input (input-1)
when one is connected, so get_instant_func_results must resolve columns from
that frame — and fall back to the data input when no control is wired.

Run with an isolated DB:
    FLOWFILE_DB_PATH=/tmp/ff_instant_func_tests.db SKIP_WORKER_TESTS=1 \
        poetry run pytest flowfile_core/tests/flowfile/test_instant_func_results.py -q
"""

from flowfile_core.flowfile.extensions import get_instant_func_results
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, schemas, transform_schema

DATA_ROWS = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
CONTROL_ROWS = [{"VALL": "1"}]


def create_graph(flow_id: int = 1) -> FlowGraph:
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id,
            name="instant_func_results_test",
            path=".",
            execution_mode="Development",
            execution_location="local",
        )
    )
    return handler.get_flow(flow_id)


def add_manual_input(graph: FlowGraph, data: list[dict], node_id: int) -> None:
    graph.add_node_promise(
        input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="manual_input")
    )
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=node_id,
            raw_data_format=input_schema.RawData.from_pylist(data),
        )
    )


def connect(graph: FlowGraph, from_id: int, to_id: int, input_type: str = "input-0") -> None:
    add_connection(
        graph,
        input_schema.NodeConnection.create_from_simple_input(from_id, to_id, input_type=input_type),
    )


def add_formula_gate(graph: FlowGraph, node_id: int, depending_on_id: int, control_id: int | None = None) -> None:
    graph.add_node_promise(
        input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="gate")
    )
    connect(graph, depending_on_id, node_id)
    if control_id is not None:
        connect(graph, control_id, node_id, input_type="right")
    graph.add_gate(
        input_schema.NodeGate(
            flow_id=graph.flow_id,
            node_id=node_id,
            depending_on_id=depending_on_id,
            is_setup=True,
            gate_input=transform_schema.GateInput(condition_source="formula", formula="[VALL] = '1'"),
        )
    )


def build_gate_graph(with_control: bool) -> FlowGraph:
    """1 data -> 2 gate, optionally 10 control -> 2 (input-1)."""
    graph = create_graph()
    add_manual_input(graph, DATA_ROWS, node_id=1)
    if with_control:
        add_manual_input(graph, CONTROL_ROWS, node_id=10)
    add_formula_gate(graph, node_id=2, depending_on_id=1, control_id=10 if with_control else None)
    return graph


class TestGateInstantFuncResults:
    def test_control_columns_resolve_when_control_is_connected(self):
        graph = build_gate_graph(with_control=True)
        result = get_instant_func_results(graph.get_node(2), "[VALL] = '1'")
        assert result.success is not False, result.result

    def test_data_columns_error_when_control_is_connected(self):
        graph = build_gate_graph(with_control=True)
        result = get_instant_func_results(graph.get_node(2), "[a] = 1")
        assert result.success is False

    def test_data_columns_resolve_without_a_control(self):
        graph = build_gate_graph(with_control=False)
        result = get_instant_func_results(graph.get_node(2), "[a] = 1")
        assert result.success is not False, result.result

    def test_control_columns_error_without_a_control(self):
        graph = build_gate_graph(with_control=False)
        result = get_instant_func_results(graph.get_node(2), "[VALL] = '1'")
        assert result.success is False

    def test_parameter_mode_gate_keeps_data_input_behavior(self):
        graph = build_gate_graph(with_control=True)
        node = graph.get_node(2)
        node.setting_input.gate_input = transform_schema.GateInput(
            condition_source="parameter", parameter="env", operator="equals", value="prod"
        )
        result = get_instant_func_results(node, "[a] = 1")
        assert result.success is not False, result.result

    def test_non_gate_nodes_keep_data_input_behavior(self):
        graph = create_graph()
        add_manual_input(graph, DATA_ROWS, node_id=1)
        graph.add_node_promise(
            input_schema.NodePromise(flow_id=graph.flow_id, node_id=3, node_type="formula")
        )
        connect(graph, 1, 3)
        result = get_instant_func_results(graph.get_node(3), "[a] + 1")
        assert result.success is not False, result.result
