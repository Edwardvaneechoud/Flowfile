"""A node's held result is released when what held it goes: the node, its flow, or a run_flow child graph.

The local cases count what the worker embedded in this process holds beyond what it held before the test. The
subflow cases check that the child graph's deferred plans are released once its run ends while a result the worker
stored for the child is left alone, since the parent's result may scan its file.
"""

import uuid

import pytest

from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends.embedded import EmbeddedWorkerBackend
from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, schemas, transform_schema


@pytest.fixture
def held():
    """The result ids the worker embedded in this process holds that it did not hold when the test began."""
    from flowfile_worker.results import results

    registry.reset()
    before = set(results._records)
    yield lambda: set(results._records) - before
    registry.reset()


@pytest.fixture
def remote_worker():
    from flowfile_core.schemas.schemas import is_valid_execution_location_in_current_global_settings
    from tests.conftest import is_worker_running

    if not (is_worker_running() and is_valid_execution_location_in_current_global_settings("remote")):
        pytest.skip("Remote execution not active")


def rows(n: int) -> input_schema.RawData:
    return input_schema.RawData.from_pylist([{"name": f"r{i}", "rank": i} for i in range(n, 0, -1)])


def add_promise(graph: FlowGraph, node_id: int, node_type: str) -> None:
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type=node_type))


def input_then_sort(graph: FlowGraph, cache_sort: bool = False) -> FlowGraph:
    """manual_input(1) -> sort(2)."""
    add_promise(graph, 1, "manual_input")
    graph.add_manual_input(input_schema.NodeManualInput(flow_id=graph.flow_id, node_id=1, raw_data_format=rows(3)))
    add_promise(graph, 2, "sort")
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    graph.add_sort(
        input_schema.NodeSort(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            cache_results=cache_sort,
            sort_input=[transform_schema.SortByInput(column="rank", how="asc")],
        )
    )
    return graph


def test_deleting_a_node_releases_its_result(held):
    graph = input_then_sort(
        FlowGraph(schemas.FlowSettings(flow_id=9601, name="release_node", path=".", execution_location="local"))
    )
    assert graph.run_graph().success
    first, second = (graph.get_node(node_id).results.result_ref.result_id for node_id in (1, 2))
    assert held() == {first, second}

    graph.delete_node(2)
    assert held() == {first}
    graph.delete_node(1)

    assert held() == set()


def test_deleting_a_flow_through_the_handler_releases_its_results(held):
    handler = FlowfileHandler()
    graph = input_then_sort(
        handler.register_flow(
            schemas.FlowSettings(flow_id=9602, name="release_flow", path=".", execution_location="local")
        )
    )
    assert graph.run_graph().success
    assert len(held()) == 2

    handler.delete_flow(9602)

    assert held() == set()


def _register_subflow(tmp_path, cache_sort: bool) -> int:
    """Save flow_input ``rows`` -> sort -> flow_output ``result`` as a registered flow; returns its registration id."""
    child = FlowGraph(schemas.FlowSettings(flow_id=9603, name="child", path=".", execution_location="local"))
    add_promise(child, 1, "flow_input")
    child.add_flow_input(
        input_schema.NodeFlowInput(flow_id=child.flow_id, node_id=1, input_name="rows", raw_data_format=rows(2))
    )
    add_promise(child, 2, "sort")
    add_connection(child, input_schema.NodeConnection.create_from_simple_input(1, 2))
    child.add_sort(
        input_schema.NodeSort(
            flow_id=child.flow_id,
            node_id=2,
            depending_on_id=1,
            cache_results=cache_sort,
            sort_input=[transform_schema.SortByInput(column="rank", how="asc")],
        )
    )
    add_promise(child, 3, "flow_output")
    child.add_flow_output(
        input_schema.NodeFlowOutput(flow_id=child.flow_id, node_id=3, output_name="result", depending_on_id=2)
    )
    add_connection(child, input_schema.NodeConnection.create_from_simple_input(2, 3))
    path = tmp_path / f"child_{uuid.uuid4().hex}.yaml"
    child.save_flow(str(path))
    with get_db_context() as db:
        registration = db_models.FlowRegistration(
            flow_uuid=str(uuid.uuid4()), name=path.stem, flow_path=str(path), owner_id=1
        )
        db.add(registration)
        db.commit()
        db.refresh(registration)
        return registration.id


def _parent_running(registration_id: int, flow_id: int, execution_location: str) -> FlowGraph:
    """manual_input(1) -> run_flow(2) over the registered child."""
    parent = FlowGraph(
        schemas.FlowSettings(flow_id=flow_id, name="parent", path=".", execution_location=execution_location)
    )
    add_promise(parent, 1, "manual_input")
    parent.add_manual_input(input_schema.NodeManualInput(flow_id=flow_id, node_id=1, raw_data_format=rows(2)))
    add_promise(parent, 2, "run_flow")
    parent.add_run_flow(
        input_schema.NodeRunFlow(
            flow_id=flow_id,
            node_id=2,
            user_id=1,
            flow_reference=input_schema.SubflowReference(registration_id=registration_id),
            input_slots=["rows"],
            output_slots=["result"],
            parameter_bindings=[],
            iteration_mode="first_value",
            append_run_metadata=False,
        )
    )
    add_connection(parent, input_schema.NodeConnection.create_from_simple_input(1, 2))
    return parent


def _record_submits(monkeypatch, backend_class) -> list[tuple[int, object]]:
    """Every (flow_id, ResultRef) ``backend_class.submit`` mints from here on."""
    minted = []
    real_submit = backend_class.submit

    def recording_submit(self, *args, **kwargs):
        result = real_submit(self, *args, **kwargs)
        minted.append((kwargs["flow_id"], result.ref))
        return result

    monkeypatch.setattr(backend_class, "submit", recording_submit)
    return minted


def test_a_subflow_run_releases_the_child_graphs_deferred_results(held, tmp_path, monkeypatch):
    minted = _record_submits(monkeypatch, EmbeddedWorkerBackend)
    parent = _parent_running(_register_subflow(tmp_path, cache_sort=False), 9604, "local")

    assert parent.run_graph().success

    child_refs = [ref for flow_id, ref in minted if flow_id != parent.flow_id]
    assert child_refs and all(ref.state == "deferred" for ref in child_refs)
    assert held() == {parent.get_node(node_id).results.result_ref.result_id for node_id in (1, 2)}
    assert [row["rank"] for row in parent.get_node(2).get_output("output-0").collect().to_dicts()] == [1, 2]


def test_a_subflow_run_leaves_a_result_the_worker_stored_for_the_child(tmp_path, monkeypatch, remote_worker):
    minted = _record_submits(monkeypatch, WorkerBackend)
    parent = _parent_running(_register_subflow(tmp_path, cache_sort=True), 9605, "remote")

    assert parent.run_graph().success

    worker = registry.backend("worker")
    child_refs = [ref for flow_id, ref in minted if flow_id != parent.flow_id]
    stored = [ref for ref in child_refs if ref.state == "materialised"]
    deferred = [ref for ref in child_refs if ref.state == "deferred"]
    assert stored and deferred
    assert all(worker.exists(ref) for ref in stored)
    assert not any(worker.exists(ref) for ref in deferred)
    assert [row["rank"] for row in parent.get_node(2).get_output("output-0").collect().to_dicts()] == [1, 2]
    for ref in stored:
        worker.release(ref)
