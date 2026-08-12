"""Partially-connected flows must run and export: orphaned nodes are skipped, not
misreported as a cycle.

Repro for the bug where disconnecting an edge mid-flow left a chain of two or
more orphaned downstream nodes, and both ``run_graph`` and the code generator
raised "Cycle detected in the graph. Execution order cannot be determined."

Run with:
    pytest flowfile_core/tests/flowfile/test_disconnected_flow_execution.py -v
"""
from typing import Literal

import pytest

from flowfile_core.flowfile.code_generator.code_generator import (
    export_flow_to_flowframe,
    export_flow_to_polars,
)
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection, delete_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, schemas
from flowfile_core.schemas.transform_schema import BasicFilter, FilterInput, FilterOperator

SAMPLE_DATA = [
    {"name": "Alice", "age": 25, "city": "New York"},
    {"name": "Bob", "age": 30, "city": "Los Angeles"},
    {"name": "Charlie", "age": 35, "city": "New York"},
]


def create_graph(flow_id: int = 1, execution_mode: Literal["Development", "Performance"] = "Development") -> FlowGraph:
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(flow_id=flow_id, name="test_flow", path=".", execution_mode=execution_mode)
    )
    return handler.get_flow(flow_id)


def add_manual_input(graph: FlowGraph, data, node_id: int = 1):
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=node_id,
            raw_data_format=input_schema.RawData.from_pylist(data),
        )
    )


def add_filter(graph: FlowGraph, node_id: int, depending_on_id: int, field: str, value: str):
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="filter"))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(depending_on_id, node_id))
    graph.add_filter(
        input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=node_id,
            depending_on_id=depending_on_id,
            filter_input=FilterInput(
                mode="basic",
                basic_filter=BasicFilter(field=field, operator=FilterOperator.EQUALS, value=value),
            ),
        )
    )


def build_chain_graph() -> FlowGraph:
    """manual_input(1) → filter(2) → filter(3) → filter(4) → filter(5)."""
    graph = create_graph()
    add_manual_input(graph, SAMPLE_DATA, node_id=1)
    add_filter(graph, node_id=2, depending_on_id=1, field="city", value="New York")
    add_filter(graph, node_id=3, depending_on_id=2, field="name", value="ORPHANED_A")
    add_filter(graph, node_id=4, depending_on_id=3, field="name", value="ORPHANED_B")
    add_filter(graph, node_id=5, depending_on_id=4, field="name", value="ORPHANED_C")
    return graph


def disconnect(graph: FlowGraph, from_id: int, to_id: int):
    delete_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id, to_id))


class TestDisconnectedFlowRun:
    def test_fully_connected_control(self):
        """Sanity check: the intact chain runs all five nodes."""
        graph = build_chain_graph()
        run_info = graph.run_graph()
        assert run_info.success
        assert {step.node_id for step in run_info.node_step_result} == {1, 2, 3, 4, 5}

    def test_run_with_deep_orphaned_chain(self):
        """Disconnecting mid-flow leaves ≥2 orphans; the connected part still runs.

        Previously raised 'Cycle detected in the graph'.
        """
        graph = build_chain_graph()
        disconnect(graph, 2, 3)

        run_info = graph.run_graph()
        assert run_info.success
        assert {step.node_id for step in run_info.node_step_result} == {1, 2}

        result = graph.get_node(2).get_resulting_data().collect().to_dicts()
        assert len(result) == 2
        assert all(row["city"] == "New York" for row in result)

    def test_run_with_single_orphan_still_works(self):
        """Disconnecting the last edge (one orphan) keeps working as before."""
        graph = build_chain_graph()
        disconnect(graph, 4, 5)

        run_info = graph.run_graph()
        assert run_info.success
        assert {step.node_id for step in run_info.node_step_result} == {1, 2, 3, 4}


class TestDisconnectedFlowCodegen:
    @pytest.mark.parametrize(
        "export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"]
    )
    def test_export_with_deep_orphaned_chain(self, export_func):
        """Code export succeeds and contains only the connected nodes.

        Previously raised 'Cycle detected in the graph' (500 on /editor/code_to_flowframe).
        """
        graph = build_chain_graph()
        disconnect(graph, 2, 3)

        code = export_func(graph)
        assert "New York" in code
        assert "ORPHANED" not in code

    @pytest.mark.parametrize(
        "export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"]
    )
    def test_export_fully_connected_control(self, export_func):
        graph = build_chain_graph()
        code = export_func(graph)
        for marker in ("New York", "ORPHANED_A", "ORPHANED_B", "ORPHANED_C"):
            assert marker in code
