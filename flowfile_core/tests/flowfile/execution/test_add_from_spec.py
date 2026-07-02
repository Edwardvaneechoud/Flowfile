"""Behavior tests for the declarative _add_from_spec path (Phase 4 nodes)."""

import polars as pl
import pytest

from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, schemas, transform_schema

pytestmark = pytest.mark.usefixtures("flowfile_worker")

MIGRATED_TYPES = ["filter", "sort", "record_count", "sample", "union"]


def _graph(execution_location: str = "remote") -> FlowGraph:
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(flow_id=1, name="spec_flow", path=".", execution_location=execution_location)
    )
    graph = handler.get_flow(1)
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=1,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist(
                [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}, {"a": 3, "b": "z"}]
            ),
        )
    )
    return graph


def _add_node(graph: FlowGraph, node_id: int, node_type: str):
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=node_id, node_type=node_type))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, node_id))
    if node_type == "filter":
        graph.add_filter(
            input_schema.NodeFilter(
                flow_id=1,
                node_id=node_id,
                depending_on_id=1,
                filter_input=transform_schema.FilterInput(mode="advanced", advanced_filter="[a] > 1"),
            )
        )
    elif node_type == "sort":
        graph.add_sort(
            input_schema.NodeSort(
                flow_id=1,
                node_id=node_id,
                depending_on_id=1,
                sort_input=[transform_schema.SortByInput(column="a", how="desc")],
            )
        )
    elif node_type == "record_count":
        graph.add_record_count(input_schema.NodeRecordCount(flow_id=1, node_id=node_id, depending_on_id=1))
    elif node_type == "sample":
        graph.add_sample(input_schema.NodeSample(flow_id=1, node_id=node_id, depending_on_id=1, sample_size=2))
    elif node_type == "union":
        second_input_id = node_id + 100
        graph.add_node_promise(
            input_schema.NodePromise(flow_id=1, node_id=second_input_id, node_type="manual_input")
        )
        graph.add_manual_input(
            input_schema.NodeManualInput(
                flow_id=1,
                node_id=second_input_id,
                raw_data_format=input_schema.RawData.from_pylist([{"a": 4, "b": "w"}]),
            )
        )
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(second_input_id, node_id))
        graph.add_union(input_schema.NodeUnion(flow_id=1, node_id=node_id, depending_on_ids=[1, second_input_id]))


@pytest.mark.parametrize("node_type", MIGRATED_TYPES)
def test_spec_node_preserves_identity_conventions(node_type):
    graph = _graph()
    _add_node(graph, 2, node_type)
    node = graph.get_node(2)
    # add_node_step conventions the skip-logic and saved flows rely on.
    assert node.name == node_type
    assert node._function.__name__ == "_func"
    assert node.node_type == node_type
    assert node.setting_input.node_id == 2


@pytest.mark.parametrize("node_type", MIGRATED_TYPES)
def test_spec_node_runs_and_produces_expected_data(node_type):
    graph = _graph()
    _add_node(graph, 2, node_type)
    run_info = graph.run_graph()
    assert run_info.success, f"run failed: {[r.error for r in run_info.node_step_result if not r.success]}"
    result = graph.get_node(2).get_resulting_data().data_frame.lazy().collect()
    if node_type == "filter":
        assert result["a"].to_list() == [2, 3]
    elif node_type == "sort":
        assert result["a"].to_list() == [3, 2, 1]
    elif node_type == "record_count":
        assert result["number_of_records"].to_list() == [3]
    elif node_type == "sample":
        assert result.height == 2
    elif node_type == "union":
        assert result.height == 4


def test_spec_node_serialization_roundtrip(tmp_path):
    graph = _graph()
    for i, node_type in enumerate(MIGRATED_TYPES, start=2):
        _add_node(graph, i, node_type)
    storage = graph.get_flowfile_data()
    node_types = {n.id: n.type for n in storage.nodes}
    for i, node_type in enumerate(MIGRATED_TYPES, start=2):
        assert node_types[i] == node_type


def test_filter_split_mode_still_works():
    graph = _graph()
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=2, node_type="filter"))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    graph.add_filter(
        input_schema.NodeFilter(
            flow_id=1,
            node_id=2,
            depending_on_id=1,
            filter_input=transform_schema.FilterInput(mode="advanced", advanced_filter="[a] > 1"),
            split_mode=True,
        )
    )
    run_info = graph.run_graph()
    assert run_info.success
    named = graph.get_node(2)._named_outputs
    assert len(named) == 2
    heights = sorted(fde.data_frame.lazy().select(pl.len()).collect().item() for fde in named.values())
    assert heights == [1, 2]
