"""A worker that cannot run a task fails it with a typed error; nothing continues in core."""

import time
from types import SimpleNamespace
from uuid import uuid4

import pytest

from flowfile_core.flowfile import api_runner
from flowfile_core.flowfile.execution.errors import (
    ComputeError,
    ComputeUnavailable,
    PlanRejected,
    TaskFailed,
    worker_failure,
)
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import subprocess_operations as subops
from flowfile_core.flowfile.flow_graph import add_connection
from flowfile_core.flowfile.flow_graph import execution as graph_execution
from flowfile_core.flowfile.flow_node import flow_node as flow_node_module
from flowfile_core.flowfile.flow_node.models import RemoteExecutionError
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.util.skip_rules import NodeRunStatus
from flowfile_core.schemas import input_schema, schemas, transform_schema


class _NotAPlan:
    """Stands in for a LazyFrame: its serialised plan is bytes the worker cannot load."""

    def serialize(self) -> bytes:
        return b"not a plan"


@pytest.fixture
def running_worker():
    from tests.conftest import is_worker_running

    if not is_worker_running():
        pytest.skip("flowfile_worker is not running")


def _force_rest(monkeypatch):
    def _no_websocket(**kwargs):
        raise ConnectionRefusedError("websocket disabled for this test")

    monkeypatch.setattr(subops, "streaming_start", _no_websocket)


def _task_id(label: str) -> str:
    return f"t-plan-rejected-{label}-{time.monotonic_ns()}"


@pytest.mark.parametrize(
    ("error_kind", "description", "expected"),
    [
        ("environment", "ComputeError: failed to read incoming DSL_VERSION", PlanRejected),
        ("environment", "ModuleNotFoundError: No module named 'core_only_udf'", PlanRejected),
        ("environment", "Task exceeded the 3600s time limit and was terminated", PlanRejected),
        ("connection", "received 1011 (internal error); then sent 1011 (internal error)", ComputeUnavailable),
        (None, "There was an unknown error with the process, and the process got killed by the server", TaskFailed),
        (None, "Process ended unexpectedly", TaskFailed),
        (None, "Error processing result: Arrow IPC stream is truncated", TaskFailed),
        ("task", "InvalidOperationError: conversion from `str` to `i64` failed", TaskFailed),
    ],
)
def test_worker_failure_types_each_kind_and_keeps_the_message(error_kind, description, expected):
    error = worker_failure(error_kind, description)

    assert type(error) is expected
    assert isinstance(error, ComputeError)
    assert error.args == (description,)


@pytest.mark.parametrize("transport", ["websocket", "rest"])
def test_worker_reports_an_unloadable_plan_as_an_environment_failure(running_worker, transport, monkeypatch):
    if transport == "rest":
        _force_rest(monkeypatch)
    fetcher = subops.ExternalDfFetcher(
        flow_id=1, node_id=1, lf=_NotAPlan(), file_ref=_task_id(transport), wait_on_completion=False
    )

    with pytest.raises(RemoteExecutionError) as excinfo:
        fetcher.get_result()

    assert fetcher.error_code == -1
    assert fetcher.error_kind == "environment"
    assert "DSL_VERSION" in fetcher.error_description
    assert str(excinfo.value) == fetcher.error_description
    assert excinfo.value.error_kind == "environment"


def test_blocking_websocket_fetch_carries_the_kind_on_the_raised_error(running_worker):
    with pytest.raises(RemoteExecutionError) as excinfo:
        subops.ExternalDfFetcher(
            flow_id=1, node_id=1, lf=_NotAPlan(), file_ref=_task_id("blocking"), wait_on_completion=True
        )

    assert excinfo.value.error_kind == "environment"
    assert "DSL_VERSION" in str(excinfo.value)


@pytest.mark.parametrize("transport", ["websocket", "rest"])
def test_materialize_raises_plan_rejected_without_collecting_in_core(running_worker, transport, monkeypatch):
    if transport == "rest":
        _force_rest(monkeypatch)
    core_collects = []

    def _collect_in_core(n_records=None):
        core_collects.append(n_records)
        raise AssertionError("the response was collected in core")

    data = SimpleNamespace(data_frame=_NotAPlan(), collect=_collect_in_core)
    flow = SimpleNamespace(flow_settings=SimpleNamespace(execution_location="remote"), flow_id=1)
    api_node = SimpleNamespace(hash=uuid4().hex, node_id=3)

    with pytest.raises(PlanRejected, match="DSL_VERSION") as excinfo:
        api_runner._materialize(data, None, flow, api_node)

    assert isinstance(excinfo.value.__cause__, RemoteExecutionError)
    assert excinfo.value.args == (str(excinfo.value.__cause__),)
    assert core_collects == []


def _manual_input_then_sort(flow_id: int):
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id,
            name="plan_rejected",
            path=".",
            execution_mode="Development",
            execution_location="remote",
        )
    )
    graph = handler.get_flow(flow_id)
    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=flow_id,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist([{"a": 2}, {"a": 1}]),
        )
    )
    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=2, node_type="sort"))
    graph.add_sort(
        input_schema.NodeSort(
            flow_id=flow_id,
            node_id=2,
            depending_on_id=1,
            sort_input=[transform_schema.SortByInput(column="a", how="asc")],
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    return graph


def test_node_fails_with_plan_rejected_and_downstream_is_skipped(running_worker, monkeypatch):
    graph = _manual_input_then_sort(flow_id=9301)
    real_fetcher = flow_node_module.ExternalDfFetcher
    shipped = []

    def _ship_not_a_plan(*, lf, **kwargs):
        fetcher = real_fetcher(lf=_NotAPlan(), **kwargs)
        shipped.append(fetcher)
        return fetcher

    real_classify = graph_execution.classify_from_inputs
    decisions = {}

    def _recording_classify(node, input_statuses):
        decision = real_classify(node, input_statuses)
        decisions[node.node_id] = decision
        return decision

    monkeypatch.setattr(flow_node_module, "ExternalDfFetcher", _ship_not_a_plan)
    monkeypatch.setattr(graph_execution, "classify_from_inputs", _recording_classify)

    run_info = graph.run_graph()

    assert run_info.success is False
    assert len(shipped) == 1
    worker_text = shipped[0].error_description
    assert "DSL_VERSION" in worker_text

    source = graph.get_node(1)
    source_result = next(r for r in run_info.node_step_result if r.node_id == 1)
    assert source_result.success is False
    assert source_result.error == worker_text
    assert source.results.errors == worker_text
    assert source._last_exception_class == "PlanRejected"
    assert source.node_stats.has_completed_last_run is False
    assert source.node_stats.has_run_with_current_setup is False
    assert source.results.example_data_generator is None
    assert source.results.warnings is None

    assert decisions[2] == NodeRunStatus.SKIPPED_ERROR
    assert 2 not in {r.node_id for r in run_info.node_step_result}
