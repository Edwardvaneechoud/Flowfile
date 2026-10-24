"""Bounded reads through a compute backend, and the engine sites that make them.

``bounded.py`` submits a plan deferred, makes one bounded read of it (a count or a sample) and releases it; the engine
reads a count for an evenly spaced sample, a first row, a null profile, a pivot's distinct values and a schema's
statistics through the current execution context's backend that way. The embedded worker is checked in this process,
the worker backend at its transport boundary with stand-in tasks (no worker needed) and against a running worker.
"""

from types import SimpleNamespace

import polars as pl
import pytest
import requests
from polars.io.plugins import register_io_source

from flowfile_core.configs import settings
from flowfile_core.configs.settings import WORKER_URL
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends import worker as worker_module
from flowfile_core.flowfile.execution.backends.embedded import EmbeddedWorkerBackend
from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
from flowfile_core.flowfile.execution.context import ExecutionContext, use_context
from flowfile_core.flowfile.execution.errors import ComputeError, ComputeUnavailable, PlanRejected, TaskFailed
from flowfile_core.flowfile.execution.protocol import JobHandle, ResultRef, SubmitResult
from flowfile_core.flowfile.flow_data_engine.bounded import (
    DEFERRED_HINTS,
    count_rows,
    head_rows,
    single_row,
    unique_values,
)
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_graph import add_connection
from flowfile_core.flowfile.schema_callbacks import pre_calculate_pivot_schema
from flowfile_core.schemas import input_schema, transform_schema
from shared.compute_protocol import PROTOCOL_VERSION
from tests.flowfile.conftest import add_test_manual_input, create_test_graph

pytestmark = pytest.mark.core

LARGE = 2_000_000
EMPTY = "nothing to read here"


@pytest.fixture(autouse=True)
def task_routes(monkeypatch):
    """The protocol flag off: these tests reach the worker through the task routes' fetcher, which the flag replaces."""
    monkeypatch.setattr(settings.FLOWFILE_COMPUTE_PROTOCOL, "value", False)


def large_plan(rows: int = LARGE) -> pl.LazyFrame:
    """A plan whose full collect is ``rows`` rows: ``n`` counts up from 0, ``g`` is ``n`` modulo 7."""
    n = pl.int_range(0, rows, dtype=pl.Int64)
    return pl.LazyFrame().select(n.alias("n"), (n % 7).alias("g"))


def counting_source(total: int) -> tuple[pl.LazyFrame, list[int]]:
    """A scan of ``total`` rows that keeps the size of every batch it produces, honouring a pushed-down limit."""
    produced: list[int] = []

    def source(with_columns, predicate, n_rows, batch_size):
        remaining = total if n_rows is None else min(n_rows, total)
        start = 0
        while remaining > 0:
            size = min(remaining, batch_size or 100_000)
            produced.append(size)
            batch = pl.DataFrame({"n": pl.int_range(start, start + size, dtype=pl.Int64, eager=True)})
            if predicate is not None:
                batch = batch.filter(predicate)
            if with_columns is not None:
                batch = batch.select(with_columns)
            start += size
            remaining -= size
            yield batch

    return register_io_source(io_source=source, schema={"n": pl.Int64}), produced


class Recording:
    """Passes every call to ``inner``; keeps the calls the helpers make and what ``submit`` returned."""

    def __init__(self, inner):
        self.inner = inner
        self.id = inner.id
        self.protocol_version = inner.protocol_version
        self.calls: list[tuple] = []
        self.submitted: list[SubmitResult] = []

    def submit(self, plan, inputs, hints, result_id, *, flow_id, node_id):
        self.calls.append(("submit", hints, flow_id, node_id))
        result = self.inner.submit(plan, inputs, hints, result_id, flow_id=flow_id, node_id=node_id)
        self.submitted.append(result)
        return result

    def sample(self, ref, n):
        self.calls.append(("sample", n))
        return self.inner.sample(ref, n)

    def count(self, ref):
        self.calls.append(("count",))
        return self.inner.count(ref)

    def release(self, ref):
        self.calls.append(("release",))
        self.inner.release(ref)

    def read_through(self, read: str, n: int | None = None) -> list[tuple]:
        """The calls of one bounded read: a deferred submit, the read and the release."""
        return [("submit", DEFERRED_HINTS, -1, -1), (read,) if n is None else (read, n), ("release",)]


class UnreachableBackend:
    """Holds plans without computing them; every read fails the way an unreachable worker does."""

    id = "unreachable"
    protocol_version = PROTOCOL_VERSION

    def __init__(self):
        self.released: list[str] = []

    def submit(self, plan, inputs, hints, result_id, *, flow_id, node_id):
        ref = ResultRef(self.id, result_id, [], None, "deferred", result_id.encode())
        return SubmitResult(ref=ref, sample=None, handle=JobHandle(self.id, result_id))

    def sample(self, ref, n):
        raise ComputeUnavailable("Could not reach the worker: connection refused")

    def count(self, ref):
        raise ComputeUnavailable("Could not reach the worker: connection refused")

    def release(self, ref):
        self.released.append(ref.result_id)


def count_collects(monkeypatch) -> list[int]:
    """One entry per ``LazyFrame.collect`` in this process from here on."""
    collects: list[int] = []
    real_collect = pl.LazyFrame.collect

    def counting_collect(self, *args, **kwargs):
        collects.append(1)
        return real_collect(self, *args, **kwargs)

    monkeypatch.setattr(pl.LazyFrame, "collect", counting_collect)
    return collects


@pytest.fixture
def running_worker():
    from tests.conftest import is_worker_running

    if not is_worker_running():
        pytest.skip("flowfile_worker is not running")


@pytest.fixture(params=["embedded", "worker"])
def backend(request):
    """A fresh backend of each kind; the worker's needs a running worker."""
    if request.param == "worker":
        request.getfixturevalue("running_worker")
        return WorkerBackend(url=WORKER_URL)
    return EmbeddedWorkerBackend()


@pytest.fixture
def fresh_registry():
    registry.reset()
    yield
    registry.reset()


@pytest.fixture(params=["embedded", "worker"])
def recorded_context(request, fresh_registry):
    """An execution context whose backend records what the engine asks of it; the embedded worker, then the worker."""
    if request.param == "worker":
        request.getfixturevalue("running_worker")
        recording = Recording(WorkerBackend(url=WORKER_URL))
    else:
        recording = Recording(EmbeddedWorkerBackend())
    registry.register_backend(recording.id, lambda: recording)
    with use_context(ExecutionContext(recording.id, parallelism=1, preview_rows=None)):
        yield recording


@pytest.fixture
def unreachable_context(fresh_registry):
    """An execution context whose backend cannot compute anything."""
    unreachable = UnreachableBackend()
    registry.register_backend(unreachable.id, lambda: unreachable)
    with use_context(ExecutionContext(unreachable.id, parallelism=1, preview_rows=None)):
        yield unreachable


# The helpers, on each backend


def test_the_hints_keep_a_plan_deferred_with_no_sample_on_every_core_backend():
    plan, produced = counting_source(LARGE)
    for backend in (EmbeddedWorkerBackend(), WorkerBackend(url=WORKER_URL)):
        result = backend.submit(plan, [], DEFERRED_HINTS, "bounded_hints", flow_id=-1, node_id=-1)
        try:
            assert (result.ref.state, result.sample) == ("deferred", None)
        finally:
            backend.release(result.ref)
    assert produced == [], "submitting computes nothing"


def test_count_rows_counts_on_the_backend(backend):
    recording = Recording(backend)

    assert count_rows(recording, large_plan()) == LARGE

    assert recording.calls == recording.read_through("count")
    (submitted,) = recording.submitted
    assert submitted.ref.result_id.startswith("bounded_")
    assert not backend.exists(submitted.ref), "released once read"


def test_head_rows_reads_the_first_n_rows(backend):
    recording = Recording(backend)

    rows = head_rows(recording, large_plan(), 5)

    assert rows.num_rows == 5
    assert rows.column("n").to_pylist() == [0, 1, 2, 3, 4]
    assert recording.calls == recording.read_through("sample", 5)
    assert not backend.exists(recording.submitted[0].ref)


def test_head_rows_of_a_shorter_plan_is_all_of_it(backend):
    rows = head_rows(backend, pl.DataFrame({"n": [1, 2, 3]}), 10)

    assert rows.column("n").to_pylist() == [1, 2, 3]


def test_single_row_is_the_first_row_as_python_values(backend):
    recording = Recording(backend)

    row = single_row(recording, large_plan().filter(pl.col("n") >= 42), EMPTY)

    assert row == {"n": 42, "g": 0}
    assert recording.calls == recording.read_through("sample", 1)


def test_single_row_of_an_empty_plan_raises_the_callers_message(backend):
    recording = Recording(backend)

    with pytest.raises(ValueError, match=f"^{EMPTY}$"):
        single_row(recording, large_plan().filter(pl.col("n") < 0), EMPTY)

    assert recording.calls[-1] == ("release",)
    assert not backend.exists(recording.submitted[0].ref)


def test_unique_values_are_the_first_distinct_values_up_to_the_limit(backend):
    recording = Recording(backend)
    keys = large_plan().select((pl.col("n") % 1000).alias("k"))

    values = unique_values(recording, keys, 200)

    assert values.dtype == pl.Int64
    assert values.to_list() == list(range(200))
    assert recording.calls == recording.read_through("sample", 200)


def test_unique_values_order_by_value_with_null_first(backend):
    values = unique_values(backend, pl.LazyFrame({"k": [10, 2, None, 2, 1, 10]}), 200)

    assert values.to_list() == [None, 1, 2, 10]
    assert values.cast(pl.String).to_list() == [None, "1", "2", "10"], "ordered by value, not by text"


def test_unique_values_read_a_plan_of_one_column():
    recording = Recording(EmbeddedWorkerBackend())

    with pytest.raises(ValueError, match="one column"):
        unique_values(recording, large_plan(), 10)

    assert recording.calls == [], "nothing is submitted"


# Only the bounded rows are computed


def test_the_embedded_worker_reads_only_the_rows_a_head_asks_for():
    """The plan crosses as bytes, so the source's count is the job's own; the rows read back are the bounded ones."""
    plan, _ = counting_source(10_000_000)

    rows = head_rows(EmbeddedWorkerBackend(), plan, 5)

    assert rows.column("n").to_pylist() == [0, 1, 2, 3, 4]


def test_the_embedded_worker_reads_only_the_row_a_single_row_asks_for():
    plan, _ = counting_source(10_000_000)

    assert single_row(EmbeddedWorkerBackend(), plan, EMPTY) == {"n": 0}


class StandInTask:
    """A worker task as its fetcher reports it: what it was built with, its outcome, where it wrote its rows."""

    def __init__(self, kwargs, *, status=None, result=None, failure=None):
        self.kwargs = kwargs
        self.file_ref = kwargs.get("file_ref")
        self.status = status
        self.result = result
        self.failure = failure
        self.error_code = 0 if failure is None else failure[0]
        self.error_kind = None if failure is None else failure[1]
        self.error_description = None if failure is None else failure[2]

    def get_result(self):
        if self.failure is not None:
            raise RuntimeError(self.error_description)
        return self.result

    def cancel(self):
        pass


@pytest.fixture
def worker_tasks(monkeypatch, tmp_path):
    """The worker backend's sampler, count and clear calls replaced by stand-ins; each keeps what it was asked.

    A sampler writes what the worker's child writes, the plan limited to ``sample_size``; a count task reports the
    plan's row count.
    """
    tasks = SimpleNamespace(samplers=[], counts=[], cleared=[])

    def sampler(**kwargs):
        path = tmp_path / f"{kwargs['file_ref']}.arrow"
        kwargs["lf"].limit(kwargs["sample_size"]).collect().write_ipc(path)
        task = StandInTask(kwargs, status=SimpleNamespace(file_ref=str(path)))
        tasks.samplers.append(task)
        return task

    def dataframe_task(**kwargs):
        task = StandInTask(kwargs, result=kwargs["lf"].select(pl.len()).collect().item())
        tasks.counts.append(task)
        return task

    monkeypatch.setattr(worker_module, "ExternalSampler", sampler)
    monkeypatch.setattr(worker_module, "ExternalDfFetcher", dataframe_task)
    monkeypatch.setattr(
        worker_module, "clear_task_from_worker", lambda task_id, *, worker_url: tasks.cleared.append(task_id)
    )
    return tasks


def test_a_head_on_the_worker_is_one_sampler_task_for_n_rows(worker_tasks):
    plan = large_plan()

    rows = head_rows(WorkerBackend(url=WORKER_URL), plan, 5)

    (task,) = worker_tasks.samplers
    assert task.kwargs["lf"] is plan, "the plan itself, serialised only by the fetcher"
    assert task.kwargs["sample_size"] == 5
    assert (task.kwargs["flow_id"], task.kwargs["node_id"]) == (-1, -1)
    assert task.file_ref.startswith("bounded_")
    assert worker_tasks.counts == [], "no store task: nothing is materialised"
    assert worker_tasks.cleared == [task.file_ref], "the release clears the sampler task"
    assert rows.column("n").to_pylist() == [0, 1, 2, 3, 4]


def test_a_count_on_the_worker_is_one_count_task(worker_tasks):
    plan = large_plan()

    assert count_rows(WorkerBackend(url=WORKER_URL), plan) == LARGE

    (task,) = worker_tasks.counts
    assert task.kwargs["operation_type"] == "calculate_number_of_records"
    assert task.kwargs["lf"] is plan
    assert worker_tasks.samplers == []
    assert worker_tasks.cleared == [], "a count leaves no task file to clear"


def test_distinct_values_on_the_worker_are_one_sampler_task_up_to_the_limit(worker_tasks):
    values = unique_values(WorkerBackend(url=WORKER_URL), pl.LazyFrame({"k": ["b", "a", "b", "c"]}), 2)

    (task,) = worker_tasks.samplers
    assert task.kwargs["sample_size"] == 2
    assert values.to_list() == ["a", "b"]


def test_a_running_worker_reads_only_the_bounded_rows_back(running_worker, monkeypatch):
    worker = WorkerBackend(url=WORKER_URL)
    collects = count_collects(monkeypatch)

    assert count_rows(worker, large_plan()) == LARGE
    assert head_rows(worker, large_plan(), 3).column("n").to_pylist() == [0, 1, 2]
    last = LARGE - 1
    assert single_row(worker, large_plan().filter(pl.col("n") == last), EMPTY) == {"n": last, "g": last % 7}
    assert unique_values(worker, large_plan().select("g"), 3).to_list() == [0, 1, 2]

    assert collects == [], "the worker computed every read; core only read the rows back"


# Failures are typed and never computed in core

HELPERS = {
    "count_rows": lambda backend: count_rows(backend, large_plan()),
    "head_rows": lambda backend: head_rows(backend, large_plan(), 5),
    "single_row": lambda backend: single_row(backend, large_plan(), EMPTY),
    "unique_values": lambda backend: unique_values(backend, large_plan().select("g"), 10),
}
FAILURES = {
    "unreachable": (ComputeUnavailable, None),
    "task_failed": (TaskFailed, (1, None, "the plan failed on the worker")),
    "plan_rejected": (PlanRejected, (-1, "environment", "the worker could not load the plan")),
}


@pytest.mark.parametrize("helper", list(HELPERS))
def test_a_backend_that_cannot_read_fails_each_helper_with_its_compute_error(helper, monkeypatch):
    unreachable = UnreachableBackend()
    collects = count_collects(monkeypatch)

    with pytest.raises(ComputeUnavailable, match="connection refused"):
        HELPERS[helper](unreachable)

    assert collects == [], "nothing is computed in core instead"
    assert len(unreachable.released) == 1, "the held plan is released all the same"


@pytest.mark.parametrize("failure", list(FAILURES))
@pytest.mark.parametrize("helper", list(HELPERS))
def test_a_failing_worker_fails_each_helper_typed_and_nothing_collects_in_core(helper, failure, monkeypatch):
    error_type, outcome = FAILURES[failure]

    def start(**kwargs):
        if outcome is None:
            raise requests.ConnectionError("connection refused")
        return StandInTask(kwargs, failure=outcome)

    monkeypatch.setattr(worker_module, "ExternalSampler", start)
    monkeypatch.setattr(worker_module, "ExternalDfFetcher", start)
    monkeypatch.setattr(worker_module, "clear_task_from_worker", lambda task_id, *, worker_url: None)
    recording = Recording(WorkerBackend(url=WORKER_URL))
    collects = count_collects(monkeypatch)

    with pytest.raises(error_type) as raised:
        HELPERS[helper](recording)

    assert isinstance(raised.value, ComputeError)
    assert collects == [], "nothing is computed in core instead"
    assert recording.calls[-1] == ("release",)
    assert not recording.inner.exists(recording.submitted[0].ref)


# The engine sites, on the execution context's backend


def test_an_evenly_spaced_sample_counts_on_the_context_backend(recorded_context):
    engine = FlowDataEngine(pl.LazyFrame({"n": list(range(100))}))

    sample = engine.get_sample(10, random=True)

    assert recorded_context.calls == recorded_context.read_through("count")
    assert engine.number_of_records == 100
    assert sample.collect()["n"].to_list() == list(range(0, 100, 10))


def test_the_first_row_peek_reads_one_row_on_the_context_backend(recorded_context):
    engine = FlowDataEngine(pl.LazyFrame({"a": ["H1", "v1"], "b": ["H2", "v2"]}))

    renamed = engine.apply_dynamic_rename(transform_schema.DynamicRenameInput(rename_mode="first_row"))

    assert recorded_context.calls == recorded_context.read_through("sample", 1)
    assert renamed.collect().to_dicts() == [{"H1": "v1", "H2": "v2"}]


def test_the_null_profile_aggregates_on_the_context_backend(recorded_context):
    engine = FlowDataEngine(pl.LazyFrame({"keep": ["x", None, "z"], "drop": [None, None, None]}))

    assert engine._fetch_null_profile() == (3, {"keep": 1, "drop": 3})
    assert recorded_context.calls == recorded_context.read_through("sample", 1)


def test_a_pivot_reads_up_to_200_distinct_values_on_the_context_backend(recorded_context):
    rows = pl.LazyFrame({"g": ["b", "a", "b", "c"], "k": [1, 1, 2, 2], "v": [1.0, 2.0, 3.0, 4.0]})
    pivot = transform_schema.PivotInput(pivot_column="g", value_col="v", index_columns=["k"], aggregations=["sum"])

    pivoted = FlowDataEngine(rows).do_pivot(pivot)

    assert recorded_context.calls == recorded_context.read_through("sample", 200)
    assert pivoted.collect().sort("k").to_dicts() == [
        {"k": 1, "a": 2.0, "b": 1.0, "c": 0.0},
        {"k": 2, "a": 0.0, "b": 3.0, "c": 4.0},
    ]


def test_a_pivots_predicted_columns_come_from_the_context_backend(recorded_context):
    rows = FlowDataEngine(pl.LazyFrame({"g": [10, 2, 1, 2], "k": [1, 1, 2, 2], "v": [1.0, 2.0, 3.0, 4.0]}))
    pivot = transform_schema.PivotInput(pivot_column="g", value_col="v", index_columns=["k"], aggregations=["sum"])

    predicted = pre_calculate_pivot_schema(rows.schema, pivot, input_lf=rows.data_frame)

    assert [column.column_name for column in predicted] == ["k", "1", "2", "10"], "ordered by value"
    assert recorded_context.calls == recorded_context.read_through("sample", 200)


def test_schema_statistics_are_one_row_on_the_context_backend(recorded_context):
    engine = FlowDataEngine(pl.LazyFrame({"a": [1, 2, 3], "s": ["x", "y", None], "d": [None, None, None]}))

    columns = {column.column_name: column for column in engine.calculate_schema()}

    assert recorded_context.calls == recorded_context.read_through("sample", 1)
    stats = {
        name: (
            c.data_type,
            c.size,
            c.number_of_empty_values,
            c.min_value,
            c.max_value,
            c.average_value,
            c.number_of_unique_values,
            c.example_values,
        )
        for name, c in columns.items()
    }
    assert stats == {
        "a": ("Int64", 3, 0, "1.0", "3.0", "2.0", 3, "1.0, 3.0"),
        "s": ("String", 1, 1, "x", "y", None, 3, "x, y"),
        "d": ("Null", -3, 3, None, None, None, 1, "None"),
    }


def test_schema_statistics_take_any_dtype(recorded_context):
    """Numbers only and dtypes with parameters, which the statistics read through dtype names could not take."""
    timestamps = pl.Series([1, 2, 3]).cast(pl.Datetime("us"))
    engine = FlowDataEngine(pl.LazyFrame({"x": [1.5, None, 2.0], "t": timestamps, "l": [[1], [2], None]}))

    columns = {column.column_name: column for column in engine.calculate_schema()}

    assert (columns["x"].min_value, columns["x"].max_value, columns["x"].average_value) == ("1.5", "2.0", "1.75")
    assert columns["t"].data_type.startswith("Datetime")
    assert columns["t"].min_value == "1970-01-01 00:00:00.000001"
    assert (columns["l"].min_value, columns["l"].number_of_empty_values) == (None, 1)
    assert columns["l"].number_of_unique_values is None, "a list column has no distinct count"


SITES = {
    "sample_count": lambda: FlowDataEngine(pl.LazyFrame({"n": [1, 2, 3]})).get_sample(2, random=True),
    "first_row": lambda: FlowDataEngine(pl.LazyFrame({"a": ["H1", "v1"]})).apply_dynamic_rename(
        transform_schema.DynamicRenameInput(rename_mode="first_row")
    ),
    "null_profile": lambda: FlowDataEngine(pl.LazyFrame({"a": [None, "x"]})).apply_data_cleansing(
        transform_schema.DataCleansingInput(remove_null_columns=True)
    ),
    "pivot": lambda: FlowDataEngine(pl.LazyFrame({"g": ["a"], "k": [1], "v": [1.0]})).do_pivot(
        transform_schema.PivotInput(pivot_column="g", value_col="v", index_columns=["k"], aggregations=["sum"])
    ),
    "schema_statistics": lambda: FlowDataEngine(pl.LazyFrame({"a": [1]})).calculate_schema(),
}


@pytest.mark.parametrize("site", list(SITES))
def test_a_site_whose_backend_cannot_compute_fails_typed_and_never_collects_in_core(
    site, unreachable_context, monkeypatch
):
    collects = count_collects(monkeypatch)

    with pytest.raises(ComputeUnavailable):
        SITES[site]()

    assert collects == [], "no in-core fallback"
    assert len(unreachable_context.released) == 1


# A node read outside a run


@pytest.fixture
def unreachable_default(fresh_registry, monkeypatch):
    """The default backend cannot compute anything; the embedded worker records what it is asked."""
    unreachable = UnreachableBackend()
    in_core = Recording(EmbeddedWorkerBackend())
    registry.register_backend(unreachable.id, lambda: unreachable)
    registry.register_backend(in_core.id, lambda: in_core)
    monkeypatch.setenv(registry.BACKEND_ENV_VAR, unreachable.id)
    return in_core


def pivot_flow(execution_location: str):
    """A manual input pivoted on ``g``, as a FlowFrame or the designer builds it, not run."""
    graph = create_test_graph(flow_id=91, execution_location=execution_location)
    add_test_manual_input(graph, [{"g": "a", "k": 1, "v": 1.0}, {"g": "b", "k": 1, "v": 2.0}])
    graph.add_node_promise(input_schema.NodePromise(flow_id=91, node_id=2, node_type="pivot"))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    pivot = transform_schema.PivotInput(pivot_column="g", value_col="v", index_columns=["k"], aggregations=["sum"])
    graph.add_pivot(input_schema.NodePivot(flow_id=91, node_id=2, pivot_input=pivot))
    return graph


def test_a_local_graphs_node_read_outside_a_run_reads_on_the_embedded_worker(unreachable_default):
    """A build-time read (a FlowFrame's) runs the node's function in its graph's context, not the default one."""
    graph = pivot_flow("local")

    rows = graph.get_node(2).get_resulting_data()

    assert rows.collect().to_dicts() == [{"k": 1, "a": 1.0, "b": 2.0}]
    assert unreachable_default.calls == unreachable_default.read_through("sample", 200)


def test_a_remote_graphs_node_read_outside_a_run_reads_on_the_default_backend(unreachable_default):
    graph = pivot_flow("remote")

    with pytest.raises(ComputeUnavailable):
        graph.get_node(2).get_resulting_data()

    assert unreachable_default.calls == [], "nothing ran in core instead"
