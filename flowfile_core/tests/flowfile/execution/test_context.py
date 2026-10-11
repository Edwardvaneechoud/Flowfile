"""A run's execution context: resolution order, the graph's own context and when the graph rebuilds it."""

import pytest

from flowfile_core.configs import settings
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.context import ExecutionContext, current_context, use_context
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.schemas import schemas


@pytest.fixture(autouse=True)
def default_backend_probe(monkeypatch):
    """The default backend is ``probe``, so a remote flow is allowed and resolves to it."""
    monkeypatch.setenv("FLOWFILE_COMPUTE_BACKEND", "probe")
    registry.reset()
    yield
    registry.reset()


def make_graph(flow_id: int, **settings_fields) -> FlowGraph:
    fields = {"execution_location": "local", "execution_mode": "Performance", "max_parallel_workers": 4}
    fields.update(settings_fields)
    return FlowGraph(schemas.FlowSettings(flow_id=flow_id, name=f"context_{flow_id}", path=".", **fields))


def test_with_nothing_set_and_no_graph_the_default_backend_is_current():
    assert current_context() == ExecutionContext(backend_id="probe", parallelism=1, preview_rows=100)


def test_with_nothing_set_the_graph_passed_in_is_current():
    graph = make_graph(9101)

    assert current_context(graph) is graph.execution_context
    assert current_context(graph).backend_id == "local"


def test_use_context_wins_over_the_graph_and_restores_the_previous_context():
    graph = make_graph(9102)
    outer = ExecutionContext(backend_id="outer", parallelism=2, preview_rows=None)
    inner = ExecutionContext(backend_id="inner", parallelism=3, preview_rows=7)

    with use_context(outer) as entered:
        assert entered is outer
        assert current_context() is outer
        assert current_context(graph) is outer
        with use_context(inner):
            assert current_context(graph) is inner
        assert current_context(graph) is outer

    assert current_context(graph) is graph.execution_context
    assert current_context().backend_id == "probe"


def test_use_context_restores_the_previous_context_when_the_block_raises():
    with pytest.raises(RuntimeError), use_context(ExecutionContext("inner", 1, None)):
        raise RuntimeError("boom")

    assert current_context() == ExecutionContext(backend_id="probe", parallelism=1, preview_rows=100)


def test_a_context_resolves_its_backend_on_access_so_it_may_exist_before_registration():
    context = ExecutionContext(backend_id="late", parallelism=1, preview_rows=None)

    with pytest.raises(LookupError):
        _ = context.backend

    made = []

    def make_late():
        made.append(object())
        return made[-1]

    registry.register_backend("late", make_late)

    assert context.backend is made[0]
    assert context.backend is made[0]
    assert len(made) == 1


def test_a_local_graph_runs_on_the_local_backend():
    graph = make_graph(9103, execution_location="local", max_parallel_workers=3)

    assert graph.execution_context == ExecutionContext(backend_id="local", parallelism=3, preview_rows=None)


def test_a_remote_graph_runs_on_the_default_backend():
    graph = make_graph(9104, execution_location="remote", max_parallel_workers=6)

    assert graph.execution_context == ExecutionContext(backend_id="probe", parallelism=6, preview_rows=None)


def test_a_remote_graph_falls_back_to_the_worker_while_offload_is_on(monkeypatch):
    monkeypatch.delenv("FLOWFILE_COMPUTE_BACKEND")
    monkeypatch.setattr(settings.OFFLOAD_TO_WORKER, "value", True)

    graph = make_graph(9105, execution_location="remote")

    assert graph.execution_context.backend_id == "worker"


def test_development_mode_keeps_100_preview_rows_and_performance_mode_none():
    development = make_graph(9106, execution_mode="Development")
    performance = make_graph(9107, execution_mode="Performance")

    assert development.execution_context.preview_rows == 100
    assert performance.execution_context.preview_rows is None


def test_setting_the_location_rebuilds_the_context():
    graph = make_graph(9108, execution_location="remote")
    assert graph.execution_context.backend_id == "probe"

    graph.execution_location = "local"
    assert graph.execution_context == ExecutionContext(backend_id="local", parallelism=4, preview_rows=None)

    graph.execution_location = "remote"
    assert graph.execution_context == ExecutionContext(backend_id="probe", parallelism=4, preview_rows=None)


def test_setting_the_mode_rebuilds_the_context():
    graph = make_graph(9109, execution_mode="Performance")

    graph.execution_mode = "Development"
    assert graph.execution_context.preview_rows == 100

    graph.execution_mode = "Performance"
    assert graph.execution_context.preview_rows is None


def test_replacing_the_flow_settings_rebuilds_the_context():
    graph = make_graph(9110, execution_location="remote", execution_mode="Performance", max_parallel_workers=4)
    before = graph.execution_context

    graph.flow_settings = schemas.FlowSettings(
        flow_id=9110,
        name="context_9110",
        path=".",
        execution_location="local",
        execution_mode="Development",
        max_parallel_workers=2,
    )

    assert before == ExecutionContext(backend_id="probe", parallelism=4, preview_rows=None)
    assert graph.execution_context == ExecutionContext(backend_id="local", parallelism=2, preview_rows=100)
