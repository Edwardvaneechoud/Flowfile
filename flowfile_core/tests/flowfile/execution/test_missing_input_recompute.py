"""An input whose result the worker no longer holds is recomputed once; an input that still holds one is not.

A read outside a run (a preview, column statistics, an export, a chart) of a result that is gone says so.
"""

import threading
import time
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from flowfile_core.flowfile.analytics import node_viz
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends.embedded import EmbeddedWorkerBackend
from flowfile_core.flowfile.execution.errors import RESULT_GONE, ComputeUnavailable
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.routes import routes
from flowfile_core.schemas import input_schema, transform_schema
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

    graph = make_graph(9301, "remote", max_parallel_workers=4)
    add_manual_input(graph, 1, ranks(5))
    add_sort_on(graph, 2, 1, "rank")
    for node_id in EXPLORERS:
        add_explore(graph, node_id, 2)
    try:
        first = graph.run_graph()
        assert first.success is True, errors(first)
        assert graph.execution_context.parallelism == 4

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


def add_split_with_two_readers(graph: FlowGraph) -> None:
    """manual_input(1) -> random_split(2), seeded 80/20 -> select(3) on output-0 and select(4) on output-1."""
    add_manual_input(graph, 1, [{"id": i} for i in range(100)])
    add_promise(graph, 2, "random_split")
    graph.add_random_split(
        input_schema.NodeRandomSplit(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            splits=[
                input_schema.RandomSplitGroup(name="big", percentage=80.0),
                input_schema.RandomSplitGroup(name="small", percentage=20.0),
            ],
            seed=42,
        )
    )
    connect(graph, 1, 2)
    for node_id, handle in ((3, "output-0"), (4, "output-1")):
        add_promise(graph, node_id, "select")
        add_connection(
            graph,
            input_schema.NodeConnection(
                input_connection=input_schema.NodeInputConnection(node_id=node_id, connection_class="input-0"),
                output_connection=input_schema.NodeOutputConnection(node_id=2, connection_class=handle),
            ),
        )
        graph.add_select(
            input_schema.NodeSelect(
                flow_id=graph.flow_id,
                node_id=node_id,
                depending_on_id=2,
                select_input=[transform_schema.SelectInput(old_name="id")],
            )
        )


def lose_the_small_split_once(graph: FlowGraph, backend: RecordingBackend) -> list[str]:
    """Releases the split's second output just before node 4 first submits; the ids of both outputs then."""
    before: list[str] = []

    def lose(node_id):
        if node_id == 4 and not before:
            split = graph.get_node(2)
            before.extend(split.held_output(handle).result_id for handle in ("output-0", "output-1"))
            backend.release(split.held_output("output-1"))

    backend.before_submit = lose
    return before


def test_a_recomputed_split_comes_back_under_new_ids_and_the_other_outputs_reader_is_read_again(backend, recomputes):
    graph = make_graph(9303, "remote", max_parallel_workers=1)
    add_split_with_two_readers(graph)
    before = lose_the_small_split_once(graph, backend)
    try:
        run_info = graph.run_graph()

        assert run_info.success is True, errors(run_info)
        assert recomputes == [2]
        split = graph.get_node(2)
        assert not {split.held_output(handle).result_id for handle in ("output-0", "output-1")} & set(before)
        assert all(backend.exists(ref) for ref in split.held_refs())
        assert backend.count(graph.get_node(4).results.result_ref) == 20
        # Its plan scanned the first output's old file, so it holds nothing until it runs again.
        reader = graph.get_node(3)
        assert reader.held_refs() == []
        assert not reader.node_stats.has_run_with_current_setup

        again = graph.run_graph()

        assert again.success is True, errors(again)
        assert recomputes == [2]
        assert backend.count(graph.get_node(3).results.result_ref) == 80
        assert backend.count(graph.get_node(4).results.result_ref) == 20
    finally:
        graph.close_flow()


def test_a_node_below_the_other_outputs_reader_runs_after_the_split_is_recomputed(backend, recomputes):
    graph = make_graph(9305, "remote", max_parallel_workers=1)
    add_split_with_two_readers(graph)
    add_sort_on(graph, 5, 3, "id")
    lose_the_small_split_once(graph, backend)
    try:
        run_info = graph.run_graph()

        assert run_info.success is True, errors(run_info)
        assert recomputes == [2]
        assert backend.count(graph.get_node(5).results.result_ref) == 80
    finally:
        graph.close_flow()


def test_a_rerun_that_cannot_reach_its_backend_fails_the_node_and_the_run_goes_on(backend, monkeypatch):
    graph = make_graph(9304, "remote", max_parallel_workers=4)
    add_manual_input(graph, 1, ranks(3))
    add_sort_on(graph, 2, 1, "rank")
    add_manual_input(graph, 3, ranks(2))
    add_union(graph, 4, [2, 3])
    real_execute_node = FlowNode.execute_node

    def unreachable_on_rerun(node, *args, **kwargs):
        if kwargs.get("reset_cache"):
            raise ComputeUnavailable("the backend went away", backend_id=BACKEND_ID)
        return real_execute_node(node, *args, **kwargs)

    monkeypatch.setattr(FlowNode, "execute_node", unreachable_on_rerun)
    backend.before_submit = lambda node_id: node_id == 4 and backend.release(graph.get_node(2).results.result_ref)
    try:
        run_info = graph.run_graph()

        assert run_info.success is False
        [(failed, error)] = errors(run_info)
        assert failed == 4
        assert "is not held" in error
    finally:
        graph.close_flow()


def sorted_ranks_under_an_explorer(flow_id: int) -> FlowGraph:
    """manual_input(1) -> sort(2) -> explore_data(3), not yet run."""
    graph = make_graph(flow_id, "remote", max_parallel_workers=1)
    add_manual_input(graph, 1, ranks(3))
    add_sort_on(graph, 2, 1, "rank")
    add_explore(graph, 3, 2)
    return graph


def test_a_gone_split_output_previews_without_rows_until_a_fetch_recomputes_it(backend):
    graph = make_graph(9306, "remote", max_parallel_workers=1)
    add_split_with_two_readers(graph)
    try:
        assert graph.run_graph().success is True
        split = graph.get_node(2)
        assert len(split.get_table_example(True, output_handle="output-1").data) == 20

        backend.release(split.held_output("output-1"))

        gone = split.get_table_example(True, output_handle="output-1")
        assert split.result_is_gone()
        assert (gone.has_example_data, gone.data, gone.columns) == (False, [], ["id"])

        graph.trigger_fetch_node(2)

        back = split.get_table_example(True, output_handle="output-1")
        assert not split.result_is_gone()
        assert back.has_example_data is True
        assert len(back.data) == 20
    finally:
        graph.close_flow()


def test_the_main_grid_keeps_its_rows_and_stats_and_export_say_the_result_is_gone(backend, monkeypatch):
    graph = sorted_ranks_under_an_explorer(9307)
    monkeypatch.setattr(routes, "get_flow_or_404", lambda *args, **kwargs: graph)
    try:
        assert graph.run_graph().success is True
        node = graph.get_node(2)
        backend.release(node.results.result_ref)

        assert len(node.get_table_example(True).data) == 3
        with pytest.raises(HTTPException) as stats:
            routes.get_node_column_stats(graph.flow_id, 2, "rank", output_handle="output-0", current_user=None)
        with pytest.raises(HTTPException) as export:
            routes.export_node_data(
                graph.flow_id, 2, output_handle="output-0", file_format="csv", limit=100, current_user=None
            )

        assert (stats.value.status_code, stats.value.detail) == (409, RESULT_GONE)
        assert (export.value.status_code, export.value.detail) == (409, RESULT_GONE)
    finally:
        graph.close_flow()


def test_a_chart_over_a_gone_result_reads_as_a_node_that_has_not_run(backend):
    graph = sorted_ranks_under_an_explorer(9308)
    try:
        assert graph.run_graph().success is True
        explorer = graph.get_node(3)
        # No chart request before the release: a session already open keeps answering from the plan it holds.
        backend.release(explorer.results.result_ref)

        with pytest.raises(node_viz.NodeNotRunError, match=RESULT_GONE):
            node_viz.get_node_fields(graph, explorer)
    finally:
        graph.close_flow()


def test_an_export_of_a_plan_over_a_gone_cached_input_says_the_result_is_gone(backend, monkeypatch):
    graph = make_graph(9309, "remote", max_parallel_workers=1, execution_mode="Performance")
    add_manual_input(graph, 1, ranks(3))
    add_sort_on(graph, 2, 1, "rank", cache_results=True)
    add_sort_on(graph, 3, 2, "name")
    monkeypatch.setattr(routes, "get_flow_or_404", lambda *args, **kwargs: graph)
    try:
        assert graph.run_graph().success is True
        cached, reader = graph.get_node(2), graph.get_node(3)
        assert (cached.results.result_ref.state, reader.results.result_ref.state) == ("materialised", "deferred")

        backend.release(cached.results.result_ref)

        assert backend.exists(reader.results.result_ref)
        with pytest.raises(HTTPException) as export:
            routes.export_node_data(
                graph.flow_id, 3, output_handle="output-0", file_format="csv", limit=100, current_user=None
            )
        assert (export.value.status_code, export.value.detail) == (409, RESULT_GONE)
    finally:
        graph.close_flow()
