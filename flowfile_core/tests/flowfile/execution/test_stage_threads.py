"""Every node of a run sees the run's execution context, on whichever thread executes it."""

import threading
import uuid
from typing import NamedTuple

import pytest

from flowfile_core.configs import settings
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends.local import LocalBackend
from flowfile_core.flowfile.execution.context import ExecutionContext, current_context, use_context
from flowfile_core.flowfile.execution.protocol import ComputeBackend
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.flowfile.manage import io_flowfile
from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.schemas import input_schema, schemas
from flowfile_core.schemas.schemas import get_global_execution_location


class CountingBackend(LocalBackend):
    """Core's own backend under another id, so a run on it needs no worker; ``touch``, on this class only, records
    a node id."""

    def __init__(self, backend_id: str):
        super().__init__()
        self.id = backend_id
        self.touched: list[int] = []
        self._touched_lock = threading.Lock()

    def touch(self, node_id: int) -> None:
        with self._touched_lock:
            self.touched.append(node_id)


class Execution(NamedTuple):
    graph_uuid: str
    node_id: int
    context: ExecutionContext
    thread: str


class Recorder:
    """Every ``FlowNode.execute_node`` call with the context it saw and its thread; ``hook`` runs before the node."""

    def __init__(self):
        self.calls: list[Execution] = []
        self.hook = None
        self._lock = threading.Lock()

    def record(self, node: FlowNode, context: ExecutionContext) -> None:
        with self._lock:
            self.calls.append(Execution(node.parent_uuid, node.node_id, context, threading.current_thread().name))


@pytest.fixture
def counting(monkeypatch) -> list[CountingBackend]:
    """``counting`` is registered and is the default backend; the list holds each instance its factory made."""
    made: list[CountingBackend] = []

    def make_counting() -> CountingBackend:
        made.append(CountingBackend("counting"))
        return made[-1]

    registry.reset()
    registry.register_backend("counting", make_counting)
    monkeypatch.setenv("FLOWFILE_COMPUTE_BACKEND", "counting")
    yield made
    registry.reset()


@pytest.fixture
def recorder(monkeypatch) -> Recorder:
    recorder = Recorder()
    real_execute_node = FlowNode.execute_node

    def recording_execute_node(node, *args, **kwargs):
        context = current_context()
        recorder.record(node, context)
        if recorder.hook is not None:
            recorder.hook(node, context)
        return real_execute_node(node, *args, **kwargs)

    monkeypatch.setattr(FlowNode, "execute_node", recording_execute_node)
    return recorder


def make_graph(flow_id: int, **settings_fields) -> FlowGraph:
    fields = {"execution_mode": "Performance", "max_parallel_workers": 4}
    fields.update(settings_fields)
    return FlowGraph(schemas.FlowSettings(flow_id=flow_id, name=f"stage_threads_{flow_id}", path=".", **fields))


def add_promise(graph: FlowGraph, node_id: int, node_type: str) -> None:
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type=node_type))


def rows(n: int) -> input_schema.RawData:
    return input_schema.RawData.from_pylist([{"name": f"r{i}", "rank": i} for i in range(1, n + 1)])


def add_manual_input(graph: FlowGraph, node_id: int) -> None:
    add_promise(graph, node_id, "manual_input")
    graph.add_manual_input(
        input_schema.NodeManualInput(flow_id=graph.flow_id, node_id=node_id, raw_data_format=rows(3))
    )


def test_parallel_stage_threads_see_the_run_context(counting, recorder):
    graph = make_graph(9201, execution_location="remote", max_parallel_workers=4)
    for node_id in (1, 2, 3, 4):
        add_manual_input(graph, node_id)
    all_four_running = threading.Barrier(4, timeout=10)

    def resolve_backend_together(node, context):
        all_four_running.wait()
        context.backend.touch(node.node_id)

    recorder.hook = resolve_backend_together

    run_info = graph.run_graph()

    assert run_info.success is True
    assert run_info.nodes_completed == 4
    assert graph.execution_context == ExecutionContext(backend_id="counting", parallelism=4, preview_rows=None)
    assert sorted(call.node_id for call in recorder.calls) == [1, 2, 3, 4]
    assert [call.context for call in recorder.calls] == [graph.execution_context] * 4
    threads = {call.thread for call in recorder.calls}
    assert len(threads) == 4
    assert threading.current_thread().name not in threads
    assert len(counting) == 1
    assert isinstance(counting[0], ComputeBackend)
    assert sorted(counting[0].touched) == [1, 2, 3, 4]
    assert current_context() == ExecutionContext(backend_id="counting", parallelism=1, preview_rows=100)


def test_a_sequential_stage_runs_on_the_run_thread_under_the_graph_context(counting, recorder):
    graph = make_graph(9202, execution_location="local", max_parallel_workers=4)
    add_manual_input(graph, 1)
    add_manual_input(graph, 2)

    run_info = graph.run_graph()

    assert run_info.success is True
    assert graph.execution_context.backend_id == "local"
    assert graph.execution_context.parallelism == 1
    assert [call.context for call in recorder.calls] == [graph.execution_context] * 2
    assert {call.thread for call in recorder.calls} == {threading.current_thread().name}


def test_a_stage_runs_as_many_nodes_at_once_as_the_run_context_allows(counting, recorder):
    graph = make_graph(9206, execution_location="remote", max_parallel_workers=4)
    for node_id in (1, 2, 3, 4):
        add_manual_input(graph, node_id)
    one_at_a_time = ExecutionContext(backend_id="counting", parallelism=1, preview_rows=None)

    with use_context(one_at_a_time):
        run_info = graph.run_graph()

    assert run_info.success is True
    assert graph.execution_context.parallelism == 4
    assert [call.context for call in recorder.calls] == [one_at_a_time] * 4
    assert {call.thread for call in recorder.calls} == {threading.current_thread().name}


@pytest.fixture
def local_subflow(tmp_path) -> dict:
    """A subflow saved with local execution: flow_input ``rows`` (two sample rows) -> flow_output ``result``."""
    child = make_graph(9203, execution_location="local")
    add_promise(child, 1, "flow_input")
    child.add_flow_input(
        input_schema.NodeFlowInput(flow_id=child.flow_id, node_id=1, input_name="rows", raw_data_format=rows(2))
    )
    add_promise(child, 2, "flow_output")
    child.add_flow_output(
        input_schema.NodeFlowOutput(flow_id=child.flow_id, node_id=2, output_name="result", depending_on_id=1)
    )
    add_connection(child, input_schema.NodeConnection.create_from_simple_input(1, 2))
    path = tmp_path / "local_subflow.yaml"
    child.save_flow(str(path))
    with get_db_context() as db:
        registration = db_models.FlowRegistration(
            flow_uuid=str(uuid.uuid4()), name="local_subflow", flow_path=str(path), owner_id=1
        )
        db.add(registration)
        db.commit()
        db.refresh(registration)
        return {"path": path, "registration_id": registration.id}


def test_a_subflow_runs_under_the_context_of_the_flow_that_runs_it(counting, recorder, local_subflow):
    parent = make_graph(9204, execution_location="remote", max_parallel_workers=2)
    add_manual_input(parent, 1)
    add_promise(parent, 2, "run_flow")
    parent.add_run_flow(
        input_schema.NodeRunFlow(
            flow_id=parent.flow_id,
            node_id=2,
            user_id=1,
            flow_reference=input_schema.SubflowReference(registration_id=local_subflow["registration_id"]),
            input_slots=["rows"],
            output_slots=["result"],
            parameter_bindings=[],
            iteration_mode="first_value",
            append_run_metadata=False,
        )
    )

    run_info = parent.run_graph()

    assert run_info.success is True
    assert parent.execution_context.backend_id == "counting"
    assert open_flow(local_subflow["path"]).execution_context.backend_id == "local"
    parent_calls = {call.node_id: call for call in recorder.calls if call.graph_uuid == parent.uuid}
    child_calls = [call for call in recorder.calls if call.graph_uuid != parent.uuid]
    assert sorted(parent_calls) == [1, 2]
    assert sorted(call.node_id for call in child_calls) == [1, 2]
    assert {call.context for call in recorder.calls} == {parent.execution_context}
    assert {call.thread for call in child_calls} == {parent_calls[2].thread}
    assert parent_calls[2].thread != threading.current_thread().name
    assert parent.get_node(2).get_output("output-0").collect().height == 2


@pytest.fixture
def opened_flows(monkeypatch) -> list[FlowGraph]:
    """Every graph ``open_flow`` returns from here on; a run_flow node opens its child through it."""
    opened: list[FlowGraph] = []
    real_open_flow = io_flowfile.open_flow

    def recording_open_flow(*args, **kwargs):
        opened.append(real_open_flow(*args, **kwargs))
        return opened[-1]

    monkeypatch.setattr(io_flowfile, "open_flow", recording_open_flow)
    return opened


@pytest.mark.parametrize("flag", [False, True], ids=["flag-off", "flag-on"])
@pytest.mark.parametrize("location", ["remote", "local"])
def test_a_subflow_child_takes_its_parent_s_location_with_the_protocol_on(
    monkeypatch, counting, local_subflow, opened_flows, location, flag
):
    monkeypatch.setattr(settings.FLOWFILE_COMPUTE_PROTOCOL, "value", flag)
    parent = make_graph(9207, execution_location=location)
    add_manual_input(parent, 1)
    add_promise(parent, 2, "run_flow")
    parent.add_run_flow(
        input_schema.NodeRunFlow(
            flow_id=parent.flow_id,
            node_id=2,
            user_id=1,
            flow_reference=input_schema.SubflowReference(registration_id=local_subflow["registration_id"]),
            input_slots=["rows"],
            output_slots=["result"],
            parameter_bindings=[],
            iteration_mode="first_value",
            append_run_metadata=False,
        )
    )

    run_info = parent.run_graph()

    assert run_info.success is True
    children = [graph for graph in opened_flows if graph._subflow_depth == 1]
    assert len(children) == 1
    child_location = location if flag else "local"
    assert children[0].execution_location == child_location
    assert children[0].execution_context.backend_id == ("counting" if child_location == "remote" else "local")


@pytest.mark.parametrize(
    ("backend_variable", "offload", "location", "backend_id"),
    [
        ("counting", False, "remote", "counting"),
        ("local", True, "local", "local"),
        (None, True, "remote", "worker"),
        (None, False, "local", "local"),
    ],
)
def test_a_call_site_without_a_graph_resolves_the_default_backend(
    monkeypatch, backend_variable, offload, location, backend_id
):
    if backend_variable is None:
        monkeypatch.delenv("FLOWFILE_COMPUTE_BACKEND", raising=False)
    else:
        monkeypatch.setenv("FLOWFILE_COMPUTE_BACKEND", backend_variable)
    monkeypatch.setattr(settings.OFFLOAD_TO_WORKER, "value", offload)

    assert get_global_execution_location() == location
    assert schemas.FlowGraphConfig().execution_location == location
    assert current_context().backend_id == backend_id


def test_a_flow_asking_for_remote_runs_locally_when_the_default_backend_is_local(monkeypatch):
    monkeypatch.setenv("FLOWFILE_COMPUTE_BACKEND", "local")

    flow_settings = schemas.FlowSettings(flow_id=9205, name="asks_remote", path=".", execution_location="remote")

    assert flow_settings.execution_location == "local"
    assert FlowGraph(flow_settings).execution_context.backend_id == "local"
