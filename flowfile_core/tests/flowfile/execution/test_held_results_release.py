"""A node's held result is released when what held it goes: the node, its flow, or a run_flow child graph.

The local cases count what the worker embedded in this process holds beyond what it held before the test. The
subflow cases check that the child graph's deferred plans are released once its run ends while a result the worker
stored for the child is left alone, since the parent's result may scan its file.
"""

import uuid

import pytest

from flowfile_core import events
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends.embedded import EmbeddedWorkerBackend
from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
from flowfile_core.flowfile.execution.errors import TaskFailed
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


def test_a_subflow_runs_stored_results_go_with_the_run_flow_node(held, tmp_path):
    parent = _parent_running(_register_subflow(tmp_path, cache_sort=True), 9606, "local")

    assert parent.run_graph().success
    adopted = {ref.result_id for ref in parent.get_node(2)._adopted_refs}
    assert adopted and adopted <= held()
    assert [row["rank"] for row in parent.get_node(2).get_output("output-0").collect().to_dicts()] == [1, 2]

    parent.close_flow()

    assert held() == set()


def test_a_second_subflow_run_releases_the_child_results_the_first_one_adopted(held, tmp_path):
    parent = _parent_running(_register_subflow(tmp_path, cache_sort=True), 9607, "local")
    assert parent.run_graph().success
    first = {ref.result_id for ref in parent.get_node(2)._adopted_refs}

    assert parent.run_graph().success
    second = {ref.result_id for ref in parent.get_node(2)._adopted_refs}

    assert first and second and first.isdisjoint(second)
    assert not first & held()
    assert second <= held()
    assert [row["rank"] for row in parent.get_node(2).get_output("output-0").collect().to_dicts()] == [1, 2]
    parent.close_flow()
    assert held() == set()


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


def _with_csv_output(graph: FlowGraph, directory) -> FlowGraph:
    """Adds output(3), a CSV written to ``directory``, after node 2."""
    add_promise(graph, 3, "output")
    graph.add_output(
        input_schema.NodeOutput(
            flow_id=graph.flow_id,
            node_id=3,
            output_settings=input_schema.OutputSettings(
                name="out.csv",
                directory=str(directory),
                file_type="csv",
                write_mode="overwrite",
                table_settings=input_schema.OutputCsvTable(),
            ),
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(2, 3))
    return graph


def _call_before_node_runs(monkeypatch, node_id: int, call) -> None:
    """``call()`` once, just before node ``node_id`` executes."""
    from flowfile_core.flowfile.flow_node.flow_node import FlowNode

    real = FlowNode._do_execute
    fired = []

    def hooked(self, *args, **kwargs):
        if self.node_id == node_id and not fired:
            fired.append(node_id)
            call()
        return real(self, *args, **kwargs)

    monkeypatch.setattr(FlowNode, "_do_execute", hooked)


def test_closing_a_running_flow_waits_for_the_run_and_then_releases_what_it_minted(held, tmp_path, monkeypatch):
    graph = _with_csv_output(
        input_then_sort(
            FlowGraph(
                schemas.FlowSettings(
                    flow_id=9611, name="close_running", path=".", execution_location="local", execution_mode="Development"
                )
            )
        ),
        tmp_path,
    )
    _call_before_node_runs(monkeypatch, 3, graph.close_flow)

    run_info = graph.run_graph()

    assert all(result.success is not False for result in run_info.node_step_result), [
        (result.node_id, result.error) for result in run_info.node_step_result
    ]
    assert graph.flow_settings.is_running is False
    assert held() == set()


def test_deleting_a_running_flow_through_the_handler_leaves_nothing_held_once_the_run_ends(
    held, tmp_path, monkeypatch
):
    closed: list[int] = []
    events.subscribe("flow_closed", lambda flow_id: closed.append(flow_id))
    handler = FlowfileHandler()
    graph = _with_csv_output(
        input_then_sort(
            handler.register_flow(
                schemas.FlowSettings(
                    flow_id=9612, name="delete_running", path=".", execution_location="local", execution_mode="Development"
                )
            )
        ),
        tmp_path,
    )
    _call_before_node_runs(monkeypatch, 1, lambda: handler.delete_flow(9612))

    run_info = graph.run_graph()

    assert run_info.success, [(result.node_id, result.error) for result in run_info.node_step_result]
    assert (tmp_path / "out.csv").exists()
    assert handler.get_flow(9612) is None
    assert closed == [9612]
    assert held() == set()


@pytest.mark.parametrize("error", [TaskFailed("refused"), LookupError("no backend")], ids=["task-failed", "lookup"])
def test_a_release_that_fails_does_not_stop_the_close_and_the_flow_still_leaves(held, monkeypatch, error):
    closed: list[int] = []
    events.subscribe("flow_closed", lambda flow_id: closed.append(flow_id))
    handler = FlowfileHandler()
    graph = input_then_sort(
        handler.register_flow(
            schemas.FlowSettings(flow_id=9613, name="release_fails", path=".", execution_location="local")
        )
    )
    assert graph.run_graph().success

    def refuse(self, ref):
        raise error

    monkeypatch.setattr(EmbeddedWorkerBackend, "release", refuse)
    handler.delete_flow(9613)

    assert handler.get_flow(9613) is None
    assert closed == [9613]
    assert graph.get_node(2).results.result_ref is None


def _split_then_select(flow_id: int, execution_mode: str) -> FlowGraph:
    """manual_input(1) -> random_split(2), seeded, 80/20 -> select(3) on the split's second output."""
    graph = FlowGraph(
        schemas.FlowSettings(
            flow_id=flow_id, name="split", path=".", execution_location="local", execution_mode=execution_mode
        )
    )
    add_promise(graph, 1, "manual_input")
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=flow_id, node_id=1, raw_data_format=input_schema.RawData.from_pylist([{"id": i} for i in range(100)])
        )
    )
    add_promise(graph, 2, "random_split")
    graph.add_random_split(
        input_schema.NodeRandomSplit(
            flow_id=flow_id,
            node_id=2,
            depending_on_id=1,
            splits=[
                input_schema.RandomSplitGroup(name="big", percentage=80.0),
                input_schema.RandomSplitGroup(name="small", percentage=20.0),
            ],
            seed=42,
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    add_promise(graph, 3, "select")
    add_connection(
        graph,
        input_schema.NodeConnection(
            input_connection=input_schema.NodeInputConnection(node_id=3, connection_class="input-0"),
            output_connection=input_schema.NodeOutputConnection(node_id=2, connection_class="output-1"),
        ),
    )
    graph.add_select(
        input_schema.NodeSelect(
            flow_id=flow_id, node_id=3, depending_on_id=2, select_input=[transform_schema.SelectInput(old_name="id")]
        )
    )
    return graph


def test_a_preview_fetch_that_reruns_a_split_sends_the_nodes_reading_it_back_to_run(held):
    graph = _split_then_select(9614, "Development")
    try:
        assert graph.run_graph().success
        select = graph.get_node(3)
        assert select.get_resulting_data().collect().height == 20

        graph.trigger_fetch_node(2)

        assert select.node_stats.has_run_with_current_setup is False
        run_info = graph.run_graph()
        assert run_info.success, [(result.node_id, result.error) for result in run_info.node_step_result]
        assert select.get_resulting_data().collect().height == 20
    finally:
        graph.close_flow()
    assert held() == set()


def test_a_rerun_releases_the_outputs_its_operation_minted_last_time(held):
    graph = _split_then_select(9615, "Performance")
    split = graph.get_node(2)
    try:
        assert graph.run_graph().success
        first = {ref.result_id for ref in split._operation_refs.values()}
        assert first and first <= held()

        assert graph.run_graph().success
        second = {ref.result_id for ref in split._operation_refs.values()}
        assert second.isdisjoint(first)
        assert held().isdisjoint(first)
    finally:
        graph.close_flow()
    assert held() == set()
