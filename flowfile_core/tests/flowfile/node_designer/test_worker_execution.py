"""E2E tests for custom-node execution offloaded to the worker.

Registry-backed nodes in a remote-execution flow must run process() in the
worker (killable subprocess owns dataset memory); in-core execution remains
only for local mode and inline (non-registry) classes.

Requires the session worker started by conftest.
"""
from pathlib import Path

import pytest

import requests

from flowfile_core.configs.settings import WORKER_URL
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.models import custom_node_task_id
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.user_defined.registry import registry as singleton_registry
from flowfile_core.schemas import input_schema, schemas

WORKER_NODE_SOURCE = '''
import polars as pl
from flowfile_core.flowfile.node_designer import CustomNodeBase, NodeSettings, Section, TextInput


class WorkerExecSettings(NodeSettings):
    main: Section = Section(title="Main", suffix=TextInput(label="Suffix", default="_x"))


class WorkerExecNode(CustomNodeBase):
    node_name: str = "Worker Exec Node"
    settings_schema: WorkerExecSettings = WorkerExecSettings()

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        suffix = self.settings_schema.main.suffix.value
        return inputs[0].with_columns((pl.col("name") + pl.lit(suffix)).alias("named"))
'''

SPLIT_NODE_SOURCE = '''
import polars as pl
from flowfile_core.flowfile.node_designer import CustomNodeBase


class WorkerSplitNode(CustomNodeBase):
    node_name: str = "Worker Split Node"
    number_of_outputs: int = 2
    output_names: list[str] = ["big", "small"]

    def process(self, *inputs: pl.LazyFrame):
        lf = inputs[0]
        return {"big": lf.filter(pl.col("value") > 10), "small": lf.filter(pl.col("value") <= 10)}
'''


@pytest.fixture
def registered_node(tmp_path):
    """Load a node file into the singleton registry; unregister afterwards."""

    loaded_entries = []

    def _register(source: str, file_name: str):
        path: Path = tmp_path / file_name
        path.write_text(source)
        entry = singleton_registry.load_file(path)
        assert entry.error is None, entry.error
        loaded_entries.append(entry)
        return entry

    yield _register

    for entry in loaded_entries:
        singleton_registry.remove_file(entry.file_path)


def create_graph(execution_location: str = "remote") -> FlowGraph:
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=1, name="flow_1", path=".", execution_mode="Development", execution_location=execution_location
        )
    )
    return handler.get_flow(1)


def add_manual_input(graph: FlowGraph, data: list[dict], node_id: int = 1) -> None:
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=node_id, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(flow_id=1, node_id=node_id, raw_data_format=input_schema.RawData.from_pylist(data))
    )


def add_registered_custom_node(graph: FlowGraph, entry, node_id: int, settings: dict) -> None:
    graph.add_node_promise(
        input_schema.NodePromise(flow_id=1, node_id=node_id, node_type=entry.node_key, is_user_defined=True)
    )
    node_settings = input_schema.UserDefinedNode(
        flow_id=1, node_id=node_id, settings=settings, is_user_defined=True, user_id=1
    )
    graph.add_user_defined_node(
        custom_node=entry.node_class.from_settings(settings), user_defined_node_settings=node_settings
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, node_id))


def run_and_check(graph: FlowGraph):
    run_info = graph.run_graph()
    assert run_info.success, [
        f"node {r.node_id}: {r.error}" for r in run_info.node_step_result if not r.success
    ]
    return run_info


def worker_executed_custom_node(node) -> bool:
    """The custom-node worker task is keyed by the node hash (suffixed to stay distinct
    from the result-store task); a Completed status proves offload."""
    response = requests.get(f"{WORKER_URL}/status/{custom_node_task_id(node.hash)}", timeout=10)
    if not response.ok:
        return False
    payload = response.json()
    return payload["status"] == "Completed" and payload["result_type"] == "other"


def test_registry_node_executes_on_worker(registered_node):
    entry = registered_node(WORKER_NODE_SOURCE, "worker_exec_node.py")
    graph = create_graph("remote")
    add_manual_input(graph, [{"name": "a"}, {"name": "b"}])
    add_registered_custom_node(graph, entry, node_id=2, settings={"main": {"suffix": "_w"}})

    run_and_check(graph)

    node = graph.get_node(2)
    assert worker_executed_custom_node(node), "expected worker offload path"
    result = node.get_resulting_data().collect()
    assert result["named"].to_list() == ["a_w", "b_w"]
    assert node.setting_input.node_source_hash == entry.source_hash


def test_registry_node_local_mode_runs_in_core(registered_node):
    entry = registered_node(WORKER_NODE_SOURCE, "worker_exec_node_local.py")
    graph = create_graph("local")
    add_manual_input(graph, [{"name": "a"}])
    add_registered_custom_node(graph, entry, node_id=2, settings={"main": {"suffix": "_l"}})

    run_and_check(graph)

    node = graph.get_node(2)
    assert not worker_executed_custom_node(node), "local mode must not offload to the worker"
    assert node.get_resulting_data().collect()["named"].to_list() == ["a_l"]


def test_multi_output_via_worker(registered_node):
    entry = registered_node(SPLIT_NODE_SOURCE, "worker_split_node.py")
    graph = create_graph("remote")
    add_manual_input(graph, [{"value": 5}, {"value": 15}, {"value": 25}])
    add_registered_custom_node(graph, entry, node_id=2, settings={})

    run_and_check(graph)

    node = graph.get_node(2)
    assert worker_executed_custom_node(node)
    assert node.get_resulting_data().collect()["value"].to_list() == [15, 25]
    assert node._named_outputs["output-1"].collect()["value"].to_list() == [5]


def test_source_edit_changes_node_hash(registered_node, tmp_path):
    entry = registered_node(WORKER_NODE_SOURCE, "worker_hash_node.py")

    def build():
        graph = create_graph("remote")
        add_manual_input(graph, [{"name": "a"}])
        add_registered_custom_node(graph, entry, node_id=2, settings={"main": {"suffix": "_h"}})
        return graph

    first_hash = build().get_node(2).hash

    edited = WORKER_NODE_SOURCE.replace('default="_x"', 'default="_edited"')
    (tmp_path / "worker_hash_node.py").write_text(edited)
    entry = singleton_registry.load_file(tmp_path / "worker_hash_node.py")
    assert entry.error is None

    assert build().get_node(2).hash != first_hash


def test_worker_down_gives_clean_node_error(registered_node, monkeypatch):
    from flowfile_core.flowfile.flow_data_engine.subprocess_operations import subprocess_operations

    entry = registered_node(WORKER_NODE_SOURCE, "worker_down_node.py")
    graph = create_graph("remote")
    add_manual_input(graph, [{"name": "a"}])
    add_registered_custom_node(graph, entry, node_id=2, settings={"main": {"suffix": "_d"}})

    monkeypatch.setattr(subprocess_operations, "WORKER_URL", "http://127.0.0.1:1")
    run_info = graph.run_graph()
    assert not run_info.success
    failed = [r for r in run_info.node_step_result if not r.success]
    assert any(r.node_id == 2 for r in failed)
