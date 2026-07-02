"""Parity tests: nodes migrated to the ExecutionBackend seam behave the same
in local and remote execution locations."""

from pathlib import Path

import polars as pl
import pytest

from flowfile_core.flowfile.flow_graph import add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, schemas

pytestmark = pytest.mark.usefixtures("flowfile_worker")


def _create_graph(execution_location: str, flow_id: int = 1):
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id,
            name="parity_flow",
            path=".",
            execution_mode="Development",
            execution_location=execution_location,
        )
    )
    return handler.get_flow(flow_id)


def _add_manual_input(graph, data, node_id: int = 1):
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=node_id, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=1, node_id=node_id, raw_data_format=input_schema.RawData.from_pylist(data)
        )
    )


def _connect(graph, from_id: int, to_id: int):
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id, to_id))


_DATA = [{"a": i, "b": f"row{i}"} for i in range(100)]


@pytest.mark.parametrize("execution_location", ["local", "remote"])
def test_output_node_writes_file_in_both_locations(execution_location, tmp_path: Path):
    graph = _create_graph(execution_location)
    _add_manual_input(graph, _DATA, node_id=1)
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=2, node_type="output"))
    _connect(graph, 1, 2)

    out_file = tmp_path / f"parity_{execution_location}.csv"
    output_settings = input_schema.OutputSettings(
        name=out_file.name,
        directory=str(tmp_path),
        file_type="csv",
        table_settings=input_schema.OutputCsvTable(),
    )
    graph.add_output(input_schema.NodeOutput(flow_id=1, node_id=2, output_settings=output_settings))

    run_info = graph.run_graph()
    assert run_info.success, f"run failed: {[r.error for r in run_info.node_step_result if not r.success]}"
    assert out_file.exists()
    written = pl.read_csv(out_file)
    assert written.height == len(_DATA)


@pytest.mark.parametrize("execution_location", ["local", "remote"])
def test_random_split_node_in_both_locations(execution_location):
    graph = _create_graph(execution_location)
    _add_manual_input(graph, _DATA, node_id=1)
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=2, node_type="random_split"))
    _connect(graph, 1, 2)
    graph.add_random_split(
        input_schema.NodeRandomSplit(
            flow_id=1,
            node_id=2,
            splits=[
                input_schema.RandomSplitGroup(name="train", percentage=70.0),
                input_schema.RandomSplitGroup(name="test", percentage=30.0),
            ],
            seed=42,
        )
    )

    run_info = graph.run_graph()
    assert run_info.success, f"run failed: {[r.error for r in run_info.node_step_result if not r.success]}"
    node = graph.get_node(2)
    named_outputs = node._named_outputs
    assert set(named_outputs) == {"output-0", "output-1"}
    total = sum(fde.data_frame.lazy().select(pl.len()).collect().item() for fde in named_outputs.values())
    assert total == len(_DATA)
