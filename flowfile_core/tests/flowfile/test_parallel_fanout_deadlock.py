"""Regression tests for the multi-worker fan-out deadlock in the core engine.

Background: ``FlowNode.get_resulting_data`` used to acquire *each input node's*
``_execution_lock`` (in per-input slot order) while holding its own lock, across
the node-function call. When a parallel stage ran two sibling nodes that consumed
the same two upstreams in opposite left/right order, they acquired the shared
upstream locks in opposite order — a classic AB-BA deadlock that wedged the run
forever and could not be cancelled. See
``flowfile_core/flowfile/flow_node/flow_node.py::get_resulting_data``.

These tests run under the real ``flowfile_worker`` (session-autouse fixture in
``tests/conftest.py``).
"""

import threading
import time

import pytest

from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, schemas, transform_schema


def _make_remote_graph(flow_id: int = 1, max_parallel_workers: int = 4) -> FlowGraph:
    """A fresh remote-execution graph with real parallelism enabled."""
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id,
            name="deadlock_repro",
            path=".",
            execution_mode="Development",
            execution_location="remote",
            max_parallel_workers=max_parallel_workers,
        )
    )
    return handler.get_flow(flow_id)


def _add_manual_input(graph: FlowGraph, data: list[dict], node_id: int) -> None:
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id, node_id=node_id, raw_data_format=input_schema.RawData.from_pylist(data)
        )
    )


def _add_formula(graph: FlowGraph, node_id: int, input_id: int, out_field: str, expression: str) -> None:
    """A narrow transform → runs LOCAL_WITH_SAMPLING (deferred) under remote execution."""
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="formula"))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(input_id, node_id))
    graph.add_formula(
        input_schema.NodeFormula(
            flow_id=graph.flow_id,
            node_id=node_id,
            function=transform_schema.FunctionInput(
                field=transform_schema.FieldInput(name=out_field), function=expression
            ),
        )
    )


def _add_join(graph: FlowGraph, node_id: int, left_id: int, right_id: int) -> None:
    """Inner join on ``id``. left_id is the main input, right_id the ``input-1`` input."""
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="join"))
    left_connection = input_schema.NodeConnection.create_from_simple_input(left_id, node_id)
    right_connection = input_schema.NodeConnection.create_from_simple_input(right_id, node_id)
    right_connection.input_connection.connection_class = "input-1"
    add_connection(graph, left_connection)
    add_connection(graph, right_connection)
    graph.add_join(
        input_schema.NodeJoin(
            flow_id=graph.flow_id,
            node_id=node_id,
            auto_generate_selection=True,
            auto_keep_all=True,
            auto_keep_left=True,
            auto_keep_right=True,
            join_input=transform_schema.JoinInput(
                join_mapping=[transform_schema.JoinMap(left_col="id", right_col="id")],
                left_select=[transform_schema.SelectInput(old_name="id", new_name="id", join_key=True, keep=True)],
                right_select=[transform_schema.SelectInput(old_name="id", new_name="id", join_key=True, keep=False)],
                how="inner",
            ),
        )
    )


def test_parallel_fanout_opposite_order_joins_do_not_deadlock(flowfile_worker, monkeypatch):
    """Two deferred upstreams fan out to joins that consume them in opposite order.

    This is the reduced shape of the reported hang (nodes 8 `[42,44]` and 6
    `[44,42]`). On the pre-fix engine the parallel stage deadlocks and the run
    never returns; here we run it in a background thread with a hard timeout so a
    regression fails cleanly instead of hanging the suite.

    To make the race deterministic, we inject a small delay into the per-input
    read helper. On the buggy code that read runs *while holding the upstream's
    lock* (acquired per-input in slot order), so the delay guarantees the two
    opposite-order joins overlap and deadlock. On the fixed code no upstream lock
    is held during the read, so the delay only slows the run slightly.
    """
    orig_resolve = FlowNode._resolve_input_result_for_handle

    def _slow_resolve(input_node, handle):
        result = orig_resolve(input_node, handle)
        time.sleep(0.25)
        return result

    monkeypatch.setattr(FlowNode, "_resolve_input_result_for_handle", staticmethod(_slow_resolve))

    graph = _make_remote_graph()

    # Two independent source→formula chains (A and B). The formulas are narrow
    # transforms, so they run LOCAL_WITH_SAMPLING (result deferred/memoized).
    _add_manual_input(graph, [{"id": 1, "a": "a1"}, {"id": 2, "a": "a2"}, {"id": 3, "a": "a3"}], node_id=1)
    _add_manual_input(graph, [{"id": 1, "b": "b1"}, {"id": 2, "b": "b2"}, {"id": 3, "b": "b3"}], node_id=2)
    _add_formula(graph, node_id=3, input_id=1, out_field="a_out", expression="[a] + '!'")  # A
    _add_formula(graph, node_id=4, input_id=2, out_field="b_out", expression="[b] + '?'")  # B

    # Fan out A(3) and B(4) to three joins in one stage, two of them in OPPOSITE
    # left/right order → the AB-BA cycle on the old code.
    _add_join(graph, node_id=5, left_id=3, right_id=4)  # (A, B)
    _add_join(graph, node_id=6, left_id=4, right_id=3)  # (B, A) — opposite order
    _add_join(graph, node_id=7, left_id=3, right_id=4)  # (A, B) — extra fan-out consumer

    result: dict = {}

    def _run():
        try:
            result["info"] = graph.run_graph()
        except Exception as exc:  # pragma: no cover - defensive
            result["error"] = exc

    runner = threading.Thread(target=_run, daemon=True)
    runner.start()
    runner.join(timeout=60)

    if runner.is_alive():
        try:
            graph.cancel()
        except Exception:
            pass
        pytest.fail("Flow run deadlocked: parallel fan-out to opposite-order joins did not complete within 60s")

    assert "error" not in result, f"Flow run raised: {result.get('error')}"
    run_info = result["info"]
    assert run_info.success, "Flow run reported failure"

    # All three fan-out joins produced their (matching) rows — no node was starved.
    for join_id in (5, 6, 7):
        out = graph.get_node(join_id).get_resulting_data()
        assert out.get_number_of_records() == 3, f"join {join_id} produced wrong row count"


def test_cancel_interrupts_a_blocked_collect(flowfile_worker):
    """A run blocked acquiring a node's execution lock must be cancellable.

    We hold a node's own ``_execution_lock`` from the test thread to simulate a
    slow in-flight materialization by another thread; the run then blocks trying
    to acquire it. ``flow.cancel()`` must break that wait promptly (the acquire is
    cancel-aware), so the run unwinds and ``is_running`` clears — instead of the
    pre-fix behaviour where the flag was only checked between node steps.
    """
    graph = _make_remote_graph()
    _add_manual_input(graph, [{"id": 1, "a": "a1"}, {"id": 2, "a": "a2"}], node_id=1)
    _add_formula(graph, node_id=2, input_id=1, out_field="a_out", expression="[a] + '!'")

    blocked_node = graph.get_node(2)
    # Hold the node's lock so the run thread blocks in the cancel-aware acquire.
    blocked_node._execution_lock.acquire()
    try:
        result: dict = {}

        def _run():
            try:
                result["info"] = graph.run_graph()
            except Exception as exc:  # pragma: no cover - defensive
                result["error"] = exc

        runner = threading.Thread(target=_run, daemon=True)
        runner.start()

        # Wait until the run is actually in flight and parked on the lock.
        for _ in range(100):
            if graph.flow_settings.is_running:
                break
            threading.Event().wait(0.05)
        assert graph.flow_settings.is_running, "run did not start"

        graph.cancel()

        runner.join(timeout=20)
        assert not runner.is_alive(), "cancel did not interrupt the blocked collect"
        assert graph.flow_settings.is_running is False, "is_running should clear after cancel"
    finally:
        blocked_node._execution_lock.release()
