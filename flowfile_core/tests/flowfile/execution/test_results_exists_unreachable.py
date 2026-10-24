"""An unreachable worker is a typed node error, never a cache miss that reruns the node in core.

Covers the probe itself (``results_exists``), the executor's handling of a cached node in remote and
local mode, recovery once the worker is back, any other compute error raised by the probe, and schema
prediction during an outage. A cached node asks the worker about the result it holds from its last run,
so the remote cases run the flow once against the session worker before it goes away.
"""

import socket

import polars as pl
import pytest
import requests

from flowfile_core.configs import settings
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends import worker as worker_module
from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
from flowfile_core.flowfile.execution.errors import ComputeUnavailable, VersionMismatch
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import subprocess_operations as subops
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.flow_node import flow_node as flow_node_module
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, schemas, transform_schema

WORKER_DOWN_MESSAGE = "Could not reach the worker at http://worker.invalid"


def _closed_url() -> str:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    return f"http://127.0.0.1:{port}"


@pytest.fixture
def closed_worker_url(monkeypatch) -> str:
    """Point the registry's worker backend at a port nothing listens on."""
    url = _closed_url()
    monkeypatch.setattr(registry.backend(registry.WORKER_BACKEND_ID), "url", url)
    return url


@pytest.fixture(params=[False, True], ids=["flag_off", "flag_on"])
def protocol(request, monkeypatch) -> bool:
    """The protocol flag in each state, back to what it was when the test ends."""
    monkeypatch.setattr(settings.FLOWFILE_COMPUTE_PROTOCOL, "value", request.param)
    return request.param


@pytest.fixture
def task_routes(monkeypatch):
    """The protocol flag off: these tests reach the worker through the task routes' fetcher, which the flag replaces."""
    monkeypatch.setattr(settings.FLOWFILE_COMPUTE_PROTOCOL, "value", False)


@pytest.fixture
def running_worker():
    from tests.conftest import is_worker_running

    if not is_worker_running():
        pytest.skip("flowfile_worker is not running")


@pytest.fixture
def stop_worker(monkeypatch, running_worker):
    """Call to point the registry's worker backend at a port nothing listens on from then on; returns that URL."""

    def stop() -> str:
        url = _closed_url()
        monkeypatch.setattr(registry.backend(registry.WORKER_BACKEND_ID), "url", url)
        return url

    return stop


def _cached_input_graph(flow_id: int, execution_location: str, execution_mode: str = "Performance") -> FlowGraph:
    """A graph holding one manual input node (id 1) with ``cache_results`` on."""
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id,
            name="cached",
            path=".",
            execution_location=execution_location,
            execution_mode=execution_mode,
        )
    )
    graph = handler.get_flow(flow_id)
    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=flow_id,
            node_id=1,
            cache_results=True,
            raw_data_format=input_schema.RawData.from_pylist([{"a": 1}]),
        )
    )
    return graph


def _add_sort(graph: FlowGraph) -> FlowNode:
    """Wire an uncached sort node (id 2) below node 1."""
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="sort"))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    graph.add_sort(
        input_schema.NodeSort(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            sort_input=[transform_schema.SortByInput(column="a", how="asc")],
        )
    )
    return graph.get_node(2)


def _watch(monkeypatch, *nodes: FlowNode) -> dict[str, int]:
    """Count, from here on, calls to the nodes' functions, executions and LazyFrame collects.

    A node function call is the node being computed (or planned) in core; an execution is the executor
    running a node at all (``FlowNode._do_execute``); a collect is core materialising data.
    """
    counts = {"function": 0, "execution": 0, "collect": 0}

    for node in nodes:

        def counting_function(*args, _original=node._function, **kwargs):
            counts["function"] += 1
            return _original(*args, **kwargs)

        monkeypatch.setattr(node, "_function", counting_function)

    original_execute = FlowNode._do_execute

    def counting_execute(self, *args, **kwargs):
        counts["execution"] += 1
        return original_execute(self, *args, **kwargs)

    monkeypatch.setattr(FlowNode, "_do_execute", counting_execute)

    original_collect = pl.LazyFrame.collect

    def counting_collect(self, *args, **kwargs):
        counts["collect"] += 1
        return original_collect(self, *args, **kwargs)

    monkeypatch.setattr(pl.LazyFrame, "collect", counting_collect)
    return counts


def _reset(counts: dict[str, int]) -> None:
    for key in counts:
        counts[key] = 0


def _assert_failed_with(node: FlowNode, message_part: str, error_class: str = "ComputeUnavailable") -> None:
    """The node is in its error state with the typed message and class, and holds no result."""
    state = node._execution_state
    assert node.results.errors is not None and message_part in node.results.errors
    assert state.error == node.results.errors
    assert node.node_stats.error == node.results.errors
    assert state.has_run_with_current_setup is False
    assert state.has_completed_last_run is False
    assert node._last_exception_class == error_class
    assert node.results.resulting_data is None


def _assert_succeeded(node: FlowNode) -> None:
    """The node completed its last run and carries no error."""
    state = node._execution_state
    assert node.results.errors is None
    assert state.error is None
    assert node.node_stats.error is None
    assert state.has_completed_last_run is True
    assert node._last_exception_class is None
    assert node.results.resulting_data is not None


def test_unreachable_worker_raises_compute_unavailable(closed_worker_url):
    with pytest.raises(ComputeUnavailable) as excinfo:
        subops.results_exists("abc", worker_url=closed_worker_url)
    assert closed_worker_url in str(excinfo.value)
    assert isinstance(excinfo.value.__cause__, requests.ConnectionError)


def test_worker_answering_404_is_a_cache_miss(monkeypatch):
    not_found = requests.Response()
    not_found.status_code = 404
    monkeypatch.setattr(subops._worker_session, "get", lambda *args, **kwargs: not_found)
    assert subops.results_exists("abc", worker_url="http://worker.invalid") is False


def test_cached_node_fails_instead_of_running_in_core(stop_worker, monkeypatch):
    graph = _cached_input_graph(9101, "remote")
    cached = graph.get_node(1)
    assert graph.run_graph().success is True
    assert cached.results.result_ref.state == "materialised"
    downstream = _add_sort(graph)
    closed_worker_url = stop_worker()
    counts = _watch(monkeypatch, cached, downstream)

    run_info = graph.run_graph()

    assert graph.flow_settings.execution_location == "remote"
    assert run_info.success is False
    results = {r.node_id: r for r in run_info.node_step_result}
    assert results[1].success is False
    assert closed_worker_url in results[1].error
    _assert_failed_with(cached, closed_worker_url)
    # A failed upstream skips its dependents: the sort gets no result row and never runs.
    assert 2 not in results
    assert downstream.results.errors is None
    assert downstream.results.resulting_data is None
    assert counts == {"function": 0, "execution": 0, "collect": 0}


def test_other_compute_errors_fail_the_node_the_same_way(running_worker, task_routes, monkeypatch):
    graph = _cached_input_graph(9106, "remote")
    cached = graph.get_node(1)
    assert graph.run_graph().success is True
    downstream = _add_sort(graph)
    message = "The worker runs other versions than this process"

    def mismatched_results_exists(file_ref: str, *, worker_url: str) -> bool:
        raise VersionMismatch(message)

    monkeypatch.setattr(worker_module, "results_exists", mismatched_results_exists)
    counts = _watch(monkeypatch, cached, downstream)

    run_info = graph.run_graph()

    assert run_info.success is False
    results = {r.node_id: r for r in run_info.node_step_result}
    assert results[1].success is False
    assert results[1].error == message
    _assert_failed_with(cached, message, error_class="VersionMismatch")
    assert 2 not in results
    assert downstream.results.resulting_data is None
    assert counts == {"function": 0, "execution": 0, "collect": 0}


@pytest.mark.parametrize(("flow_id", "execution_mode"), [(9102, "Development"), (9105, "Performance")])
def test_local_cached_node_never_asks_the_worker(closed_worker_url, monkeypatch, protocol, flow_id, execution_mode):
    """A cached node in local mode is decided in core, the worker beside core never asked, in both modes and flag
    states.

    The worker embedded in this process stores a ``cache_results`` node's result in both flag states, so it is the
    node's cache as on the worker beside core and a second run skips it in either mode, keeping the stored result.
    """
    graph = _cached_input_graph(flow_id, "local", execution_mode)
    node = graph.get_node(1)
    probes = []
    monkeypatch.setattr(worker_module, "results_exists", lambda file_ref, **_: probes.append(file_ref) or False)
    monkeypatch.setattr(subops, "results_exists", lambda file_ref, **_: probes.append(file_ref) or False)
    counts = _watch(monkeypatch, node)

    run_info = graph.run_graph()

    assert run_info.success is True
    _assert_succeeded(node)
    held = node.results.result_ref
    assert (held.backend_id, held.state) == ("local", "materialised")
    assert probes == []
    assert counts["execution"] == 1
    assert counts["function"] == 1

    _reset(counts)
    assert graph.run_graph().success is True
    _assert_succeeded(node)
    assert probes == []
    assert counts["execution"] == 0
    assert counts["function"] == 0
    assert node.results.result_ref == held
    assert node.results.resulting_data.data_frame.collect().to_dicts() == [{"a": 1}]
    node.release_result()


def test_cached_node_recovers_after_the_worker_returns(running_worker, task_routes, monkeypatch):
    graph = _cached_input_graph(9103, "remote")
    node = graph.get_node(1)
    assert graph.run_graph().success is True
    _assert_succeeded(node)
    stored = node.results.result_ref
    assert stored.state == "materialised"

    worker = {"state": "down"}
    real_results_exists = worker_module.results_exists

    def fake_results_exists(file_ref: str, *, worker_url: str) -> bool:
        if worker["state"] == "down":
            raise ComputeUnavailable(WORKER_DOWN_MESSAGE)
        return real_results_exists(file_ref, worker_url=worker_url)

    submits = []
    real_submit = WorkerBackend.submit

    def counting_submit(self, *args, **kwargs):
        submits.append(args)
        return real_submit(self, *args, **kwargs)

    monkeypatch.setattr(worker_module, "results_exists", fake_results_exists)
    monkeypatch.setattr(WorkerBackend, "submit", counting_submit)
    counts = _watch(monkeypatch, node)

    down = graph.run_graph()
    assert down.success is False
    assert down.node_step_result[0].error == WORKER_DOWN_MESSAGE
    _assert_failed_with(node, WORKER_DOWN_MESSAGE)
    assert counts == {"function": 0, "execution": 0, "collect": 0}
    assert submits == []

    # The failed node reruns past its cache: the run clears the error and reads the stored result back.
    _reset(counts)
    worker["state"] = "present"
    back = graph.run_graph()
    assert back.success is True
    _assert_succeeded(node)
    assert submits == []
    assert node.results.result_ref == stored
    assert counts == {"function": 0, "execution": 1, "collect": 0}
    assert node.results.resulting_data.data_frame.collect().to_dicts() == [{"a": 1}]

    # Once healthy again, a cache hit skips the node.
    _reset(counts)
    assert graph.run_graph().success is True
    _assert_succeeded(node)
    assert submits == []
    assert counts == {"function": 0, "execution": 0, "collect": 0}


def test_prediction_survives_the_worker_down(stop_worker, monkeypatch):
    graph = _cached_input_graph(9104, "remote")
    cached = graph.get_node(1)
    assert graph.run_graph().success is True
    downstream = _add_sort(graph)
    closed_worker_url = stop_worker()
    warnings = []
    monkeypatch.setattr(flow_node_module.logger, "warning", lambda message, *args, **kwargs: warnings.append(message))
    counts = _watch(monkeypatch, cached, downstream)

    predicted = cached.get_predicted_resulting_data()

    assert predicted.columns == ["a"]
    assert [column.column_name for column in downstream.get_predicted_schema()] == ["a"]
    assert any("worker not usable" in str(message) and closed_worker_url in str(message) for message in warnings)
    for node in (cached, downstream):
        assert node.results.errors is None
        assert node._execution_state.error is None
        assert getattr(node, "_last_exception_class", None) is None
    assert counts["execution"] == 0
    assert counts["collect"] == 0
