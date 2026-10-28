"""An Explore Data node's visualisation session runs on the backend that holds its result when that backend declares
``viz_session``, else on the default worker.

With an exchange store configured and the compute protocol flag on, that is the backend the node's own
``execution_location`` names; with the flag off it is the default worker whatever holds the result. A backend that
does not declare ``viz_session`` refuses the node before any node starts, as any operation is refused; ``local``, the
worker embedded in this process, declares it and charts a node it holds itself with the flag on. The backends are the
embedded worker under other ids, answering a session themselves, so no worker process is needed.
"""

from flowfile_core.flowfile.analytics import node_viz
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.protocol import OperationKind, OpResult
from flowfile_core.schemas import input_schema
from tests.flowfile.execution import test_placement
from tests.flowfile.execution.test_placement import (
    BACKEND_ID,
    DeclaringBackend,
    add_manual_input,
    add_promise,
    assert_refused_before_any_node,
    connect,
    everything,
    make_graph,
    without,
)

# The fixtures these tests share with the placement tests, bound here.
declare = test_placement.declare
flag = test_placement.flag

SECOND = "worker:second"
LOCAL, WORKER = registry.LOCAL_BACKEND_ID, registry.WORKER_BACKEND_ID
ROWS = [{"g": "a", "v": 1}, {"g": "b", "v": 2}]


class ChartingBackend(DeclaringBackend):
    """Declares what it is given and answers a ``viz_session`` itself with no fields."""

    def run_op(self, op, payload):
        if op is not OperationKind.viz_session:
            return super().run_op(op, payload)
        with self._work_lock:
            self.work.append("run_op viz_session")
        return OpResult(refs={}, metadata={"fields": [], "cache_hit": False})


def chart_on(backend_id: str) -> ChartingBackend:
    backend = ChartingBackend(backend_id, everything())
    registry.register_backend(backend_id, lambda: backend)
    return backend


def explore(graph, location: str = "auto") -> None:
    """A manual input and an Explore Data node over it, both on ``location``."""
    add_manual_input(graph, 1, ROWS)
    graph.get_node(1).setting_input.execution_location = location
    add_promise(graph, 2, "explore_data")
    graph.add_explore_data(input_schema.NodeExploreData(flow_id=graph.flow_id, node_id=2, execution_location=location))
    connect(graph, 1, 2)


def test_a_chart_opens_on_the_second_worker_that_holds_the_explore_data_nodes_result(declare, monkeypatch, flag):
    monkeypatch.setenv("FLOWFILE_EXCHANGE_URI", "s3://placement-exchange/runs")
    default, second, worker = chart_on(BACKEND_ID), chart_on(SECOND), chart_on(WORKER)
    graph = make_graph(8650 + flag)
    explore(graph, SECOND)

    run_info = graph.run_graph()

    assert run_info.success is True, [result.error for result in run_info.node_step_result]
    assert node_viz.get_node_fields(graph, graph.get_node(2)).fields == []
    held, charted = (SECOND, SECOND) if flag else (BACKEND_ID, WORKER)
    assert graph.get_node(2).results.result_ref.backend_id == held
    assert [b.id for b in (default, second, worker) if "run_op viz_session" in b.work] == [charted]


def test_a_chart_on_a_result_the_embedded_worker_holds_opens_there_under_the_flag(declare, flag):
    local, worker = chart_on(LOCAL), chart_on(WORKER)
    graph = make_graph(8652 + flag, LOCAL)
    explore(graph)

    run_info = graph.run_graph()

    assert run_info.success is True, [result.error for result in run_info.node_step_result]
    assert run_info.nodes_completed == 2
    assert all(result.compute_error is None for result in run_info.node_step_result)
    assert graph.get_node(2).results.result_ref.backend_id == LOCAL
    assert node_viz.get_node_fields(graph, graph.get_node(2)).fields == []
    charted = LOCAL if flag else WORKER
    assert [b.id for b in (local, worker) if "run_op viz_session" in b.work] == [charted]


def test_an_explore_data_node_is_refused_on_a_configured_worker_without_the_viz_session_operation(
    declare, flag
):
    second = declare(without(everything(), operation="viz_session"), SECOND)
    graph = make_graph(8654 + flag, SECOND)
    explore(graph)

    run_info = graph.run_graph()

    if flag:
        assert_refused_before_any_node(
            graph, run_info, second, {2: ["it needs the viz_session operation"]}
        )
    else:
        assert run_info.success is True, [result.error for result in run_info.node_step_result]
        assert "submit 2" in second.work
