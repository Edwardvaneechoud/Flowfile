"""An input whose result the worker no longer holds is recomputed once; an input that still holds one is not."""

import threading
import time
from types import SimpleNamespace

import pytest

from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends.embedded import EmbeddedWorkerBackend
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.schemas import input_schema
from tests.flowfile.execution.graph_helpers import add_manual_input, add_promise, add_sort_on, connect, make_graph, ranks

BACKEND_ID = "recompute"
EXPLORERS = (3, 4, 5)
A_DAY_AND_AN_HOUR = 25 * 3600


class RecordingBackend(EmbeddedWorkerBackend):
    """The embedded worker under ``BACKEND_ID``, recording each submit's node id and error."""

    def __init__(self):
        super().__init__()
        self.id = BACKEND_ID
        self.submits: list[tuple[int | str, str | None]] = []
        self.before_submit = None
        self._submits_lock = threading.Lock()

    def submit(self, plan, inputs, hints, result_id, *, flow_id, node_id):
        if self.before_submit is not None:
            self.before_submit(node_id)
        try:
            answer = super().submit(plan, inputs, hints, result_id, flow_id=flow_id, node_id=node_id)
        except Exception as e:
            self._record(node_id, f"{type(e).__name__}: {e}")
            raise
        self._record(node_id, None)
        return answer

    def _record(self, node_id: int | str, error: str | None) -> None:
        with self._submits_lock:
            self.submits.append((node_id, error))

    def succeeded(self, node_id: int) -> int:
        return sum(1 for submitted, error in self.submits if submitted == node_id and error is None)


@pytest.fixture
def backend(monkeypatch, protocol_on) -> RecordingBackend:
    """``RecordingBackend`` registered as the default backend, with the compute protocol on."""
    made = RecordingBackend()
    registry.reset()
    registry.register_backend(BACKEND_ID, lambda: made)
    monkeypatch.setenv("FLOWFILE_COMPUTE_BACKEND", BACKEND_ID)
    yield made
    registry.reset()


@pytest.fixture
def recomputes(monkeypatch) -> list[int]:
    """The id of every node run again with its cache reset, as the retry of a missing input runs it."""
    calls: list[int] = []
    lock = threading.Lock()
    real_execute_node = FlowNode.execute_node

    def recording_execute_node(node, *args, **kwargs):
        if kwargs.get("reset_cache"):
            with lock:
                calls.append(node.node_id)
        return real_execute_node(node, *args, **kwargs)

    monkeypatch.setattr(FlowNode, "execute_node", recording_execute_node)
    return calls


def add_explore(graph: FlowGraph, node_id: int, source_id: int) -> None:
    add_promise(graph, node_id, "explore_data")
    graph.add_explore_data(input_schema.NodeExploreData(flow_id=graph.flow_id, node_id=node_id))
    connect(graph, source_id, node_id)


def add_union(graph: FlowGraph, node_id: int, source_ids: list[int]) -> None:
    add_promise(graph, node_id, "union")
    for source_id in source_ids:
        connect(graph, source_id, node_id)
    graph.add_union(input_schema.NodeUnion(flow_id=graph.flow_id, node_id=node_id, depending_on_ids=source_ids))


def errors(run_info) -> list[tuple[int | str, str | None]]:
    return [(result.node_id, result.error) for result in run_info.node_step_result if not result.success]


def test_a_transform_whose_result_expired_under_three_explorers_is_recomputed_once(backend, recomputes, monkeypatch):
    from flowfile_worker import results as worker_results
    from flowfile_worker.results import ResultRegistry

    graph = make_graph(9301, "remote", max_parallel_workers=4)
    add_manual_input(graph, 1, ranks(5))
    add_sort_on(graph, 2, 1, "rank")
    for node_id in EXPLORERS:
        add_explore(graph, node_id, 2)
    try:
        first = graph.run_graph()
        assert first.success is True, errors(first)
        assert graph.execution_context.parallelism == 4

        # Without this the existence checks would reset the ages and nothing would expire.
        monkeypatch.setattr(ResultRegistry, "touch", lambda self, result_id: None, raising=False)
        monkeypatch.setattr(
            worker_results, "time", SimpleNamespace(monotonic=lambda: time.monotonic() + A_DAY_AND_AN_HOUR)
        )
        del backend.submits[:]

        second = graph.run_graph()

        assert second.success is True, errors(second)
        assert errors(second) == []
        assert sorted(recomputes) == [1, 2]
        assert backend.succeeded(1) == 1
        assert backend.succeeded(2) == 1
        assert all(backend.succeeded(node_id) == 1 for node_id in EXPLORERS), backend.submits
        assert not [error for _, error in backend.submits if error is not None and "Cancelled" in error]
        assert {node_id for node_id, error in backend.submits if error is not None} <= {2, *EXPLORERS}
        for node_id in (2, *EXPLORERS):
            assert backend.exists(graph.get_node(node_id).results.result_ref)
    finally:
        graph.close_flow()


def test_an_input_that_still_holds_its_result_is_not_run_again(backend, recomputes):
    graph = make_graph(9302, "remote", max_parallel_workers=4)
    add_manual_input(graph, 1, ranks(3))
    add_sort_on(graph, 2, 1, "rank")
    add_manual_input(graph, 3, ranks(2))
    add_union(graph, 4, [2, 3])
    lost: list[str] = []

    def lose_the_sort_once(node_id):
        if node_id == 4 and not lost:
            ref = graph.get_node(2).results.result_ref
            lost.append(ref.result_id)
            backend.release(ref)

    backend.before_submit = lose_the_sort_once
    try:
        run_info = graph.run_graph()

        assert run_info.success is True, errors(run_info)
        assert lost
        assert recomputes == [2]
        assert [backend.succeeded(node_id) for node_id in (1, 2, 3, 4)] == [1, 2, 1, 1]
        [(failed, error)] = [(node_id, error) for node_id, error in backend.submits if error is not None]
        assert failed == 4 and error.startswith("ResultMissing"), error
        assert backend.count(graph.get_node(4).results.result_ref) == 5
    finally:
        graph.close_flow()
