"""Each backend's materialisation policy: what ``submit`` holds for each combination of hints.

The worker's policy is the one policy: a plan is stored when it must be materialised, or when it is wide and a
preview is asked for, and the sample comes inline from the same job. ``EmbeddedWorkerBackend`` stores it in a file
under core's cache directory that ``release`` removes, in both flag states; ``WorkerBackend`` stores it on the worker,
and with the flag off every other plan stays deferred in core, its samples from a worker sampler task.
"""

import io
import threading
import uuid
from pathlib import Path

import polars as pl
import pyarrow as pa
import pytest

from flowfile_core.configs import settings
from flowfile_core.configs.settings import WORKER_URL
from flowfile_core.flowfile.execution.backends import worker as worker_module
from flowfile_core.flowfile.execution.backends.embedded import EmbeddedWorkerBackend
from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
from flowfile_core.flowfile.execution.errors import ResultMissing, TaskFailed
from flowfile_core.flowfile.execution.protocol import JobHandle, ResultRef, SubmitHints
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import subprocess_operations as subops
from flowfile_worker.results import result_path

PEOPLE = pl.DataFrame({"id": list(range(10)), "name": [f"n{i}" for i in range(10)]})
HINT_COMBINATIONS = [
    pytest.param(must_materialise, wide, preview_rows, id=f"materialise={must_materialise}-wide={wide}-{preview_rows}")
    for must_materialise in (True, False)
    for wide in (True, False)
    for preview_rows in (4, None)
]


def plan() -> pl.LazyFrame:
    return PEOPLE.lazy().filter(pl.col("id") >= 0)


def hints(must_materialise: bool, wide: bool, preview_rows: int | None, fanout: int = 1) -> SubmitHints:
    return SubmitHints(must_materialise=must_materialise, wide=wide, fanout=fanout, preview_rows=preview_rows)


def rows(sample: bytes) -> pa.Table:
    return pa.ipc.open_stream(io.BytesIO(sample)).read_all()


def new_result_id() -> str:
    return f"t-submit-policy-{uuid.uuid4().hex}"


@pytest.fixture(autouse=True)
def task_routes(monkeypatch):
    """The protocol flag off: these tests reach the worker through the task routes' fetcher, which the flag replaces."""
    monkeypatch.setattr(settings.FLOWFILE_COMPUTE_PROTOCOL, "value", False)


@pytest.fixture
def running_worker():
    from tests.conftest import is_worker_running

    if not is_worker_running():
        pytest.skip("flowfile_worker is not running")


@pytest.fixture
def worker(running_worker):
    """A worker backend already checked, so the handshake's probe jobs run before a test records anything."""
    backend = WorkerBackend(url=WORKER_URL)
    handshake.ensure_verdict(backend)
    return backend


@pytest.fixture
def kept(worker):
    """Worker results a test made, released when it ends."""
    refs: list[ResultRef] = []
    yield refs
    for ref in refs:
        worker.release(ref)


def submit_to_worker(worker: WorkerBackend, kept: list[ResultRef], submit_hints: SubmitHints):
    return submit_to_worker_plan(worker, kept, plan(), submit_hints)


def submit_to_worker_plan(worker: WorkerBackend, kept: list[ResultRef], lazy: pl.LazyFrame, submit_hints: SubmitHints):
    result = worker.submit(lazy, [], submit_hints, new_result_id(), flow_id=-1, node_id=-1)
    kept.append(result.ref)
    return result


@pytest.fixture
def sampler_tasks(monkeypatch) -> list[dict]:
    """The keyword arguments of every worker sampler task started from here on."""
    started = []
    real_sampler = worker_module.ExternalSampler

    def recording_sampler(**kwargs):
        started.append(kwargs)
        return real_sampler(**kwargs)

    monkeypatch.setattr(worker_module, "ExternalSampler", recording_sampler)
    return started


@pytest.mark.parametrize(("must_materialise", "wide", "preview_rows"), HINT_COMBINATIONS)
def test_the_embedded_worker_stores_what_must_be_materialised_or_is_wide_with_a_preview(
    must_materialise, wide, preview_rows
):
    backend = EmbeddedWorkerBackend()
    stored = must_materialise or (wide and preview_rows is not None)
    stored_file = result_path(1, "hash")

    result = backend.submit(plan(), [], hints(must_materialise, wide, preview_rows), "hash", flow_id=1, node_id=1)

    assert result.ref.state == ("materialised" if stored else "deferred")
    assert result.ref.result_id == "hash"
    assert result.handle == JobHandle("local", "hash")
    assert stored_file.is_file() is stored
    if preview_rows is None:
        assert result.sample is None
    else:
        sample = rows(result.sample)
        assert sample.num_rows == preview_rows
        assert sample.column("id").to_pylist() == [0, 1, 2, 3]
    assert rows(backend.sample(result.ref, 3)).num_rows == 3
    assert rows(backend.sample(result.ref, 50)).num_rows == 10
    assert backend.count(result.ref) == 10

    backend.release(result.ref)

    assert backend.exists(result.ref) is False
    assert not stored_file.exists()


@pytest.mark.parametrize(("must_materialise", "wide", "preview_rows"), HINT_COMBINATIONS)
def test_the_worker_stores_what_must_be_materialised_or_is_wide_with_a_preview(
    worker, kept, must_materialise, wide, preview_rows
):
    result = submit_to_worker(worker, kept, hints(must_materialise, wide, preview_rows))

    stored = must_materialise or (wide and preview_rows is not None)
    assert result.ref.state == ("materialised" if stored else "deferred")
    assert result.handle == JobHandle("worker", result.ref.result_id)
    if preview_rows is None:
        assert result.sample is None
    else:
        sample = rows(result.sample)
        assert sample.num_rows == preview_rows
        assert sample.column("id").to_pylist() == [0, 1, 2, 3]


def test_the_worker_policy_does_not_read_fanout(worker, kept):
    result = submit_to_worker(worker, kept, hints(False, False, 4, fanout=3))

    assert result.ref.state == "deferred"


def test_a_stored_result_and_its_job_carry_the_result_id_and_where_to_file_them(worker, kept, monkeypatch):
    stores = []
    real_fetcher = worker_module.ExternalDfFetcher

    def recording_fetcher(**kwargs):
        stores.append(kwargs)
        return real_fetcher(**kwargs)

    monkeypatch.setattr(worker_module, "ExternalDfFetcher", recording_fetcher)
    result_id = new_result_id()

    ref = worker.submit(plan(), [], hints(True, False, None), result_id, flow_id=-1, node_id=7).ref
    kept.append(ref)

    assert ref.result_id == result_id
    assert [(s["file_ref"], s["flow_id"], s["node_id"]) for s in stores] == [(result_id, -1, 7)]
    assert worker.result_file(ref) == ref.location.decode()
    assert Path(worker.result_file(ref)).is_file()


def test_a_deferred_plan_samples_through_a_worker_sampler_task(worker, kept, sampler_tasks):
    result = submit_to_worker(worker, kept, hints(False, False, 4))
    ref = result.ref

    assert [(task["file_ref"], task["sample_size"]) for task in sampler_tasks] == [(ref.result_id, 4)]
    assert Path(worker.result_file(ref)).is_file()

    assert rows(worker.sample(ref, 3)).num_rows == 3
    assert rows(worker.sample(ref, 50)).num_rows == 10
    assert [task["sample_size"] for task in sampler_tasks] == [4, 3, 50]


def test_a_deferred_plan_without_a_preview_runs_nothing_on_the_worker(worker, kept, sampler_tasks):
    ref = submit_to_worker(worker, kept, hints(False, False, None)).ref

    assert sampler_tasks == []
    assert worker.result_file(ref) is None
    assert ref.row_count is None
    assert [(column.column_name, column.data_type) for column in ref.schema] == [("id", "Int64"), ("name", "String")]


def test_a_deferred_plan_is_held_as_the_object_and_a_stored_one_is_serialised_once(worker, kept, monkeypatch):
    serialised = []
    real_serialize = pl.LazyFrame.serialize

    def counting_serialize(self, *args, **kwargs):
        serialised.append(1)
        return real_serialize(self, *args, **kwargs)

    monkeypatch.setattr(pl.LazyFrame, "serialize", counting_serialize)
    deferred_plan = plan()

    deferred = submit_to_worker_plan(worker, kept, deferred_plan, hints(False, False, None)).ref
    assert worker.plan_for(deferred) is deferred_plan
    assert deferred.location == deferred.result_id.encode()
    assert serialised == []

    submit_to_worker_plan(worker, kept, plan(), hints(True, False, None))
    assert serialised == [1]


def test_plan_for_a_deferred_result_is_the_plan_further_plans_compose_on(worker, kept):
    ref = submit_to_worker(worker, kept, hints(False, False, None)).ref

    composed = worker.plan_for(ref).filter(pl.col("id") > 6).select("name")

    assert composed.collect()["name"].to_list() == ["n7", "n8", "n9"]


def test_a_deferred_result_is_counted_by_the_worker(worker, kept):
    assert worker.count(submit_to_worker(worker, kept, hints(False, False, None)).ref) == 10


def test_a_released_deferred_result_is_gone_and_its_sampler_task_cleared(worker):
    ref = worker.submit(plan(), [], hints(False, False, 4), new_result_id(), flow_id=-1, node_id=-1).ref
    assert worker.exists(ref)
    assert subops.results_exists(ref.result_id, worker_url=WORKER_URL)

    worker.release(ref)

    assert not worker.exists(ref)
    assert not subops.results_exists(ref.result_id, worker_url=WORKER_URL)
    with pytest.raises(ResultMissing):
        worker.plan_for(ref)
    with pytest.raises(ResultMissing):
        worker.sample(ref, 1)


class _WaitingFetcher:
    """A fetcher whose task runs until it is cancelled, then fails the way a cancelled task does."""

    def __init__(self, **kwargs):
        self.file_ref = kwargs["file_ref"]
        self.cancelled = threading.Event()
        self.error_code = 0
        self.error_kind = None
        self.error_description = None
        self.status = None

    def get_result(self):
        assert self.cancelled.wait(timeout=10), "the task was never cancelled"
        self.error_code = -2
        self.error_description = "Task cancelled"
        raise RuntimeError(self.error_description)

    def cancel(self):
        self.cancelled.set()


def test_cancel_reaches_the_task_a_submit_waits_on(monkeypatch):
    started = []

    def waiting_fetcher(**kwargs):
        started.append(_WaitingFetcher(**kwargs))
        return started[-1]

    monkeypatch.setattr(worker_module, "ExternalDfFetcher", waiting_fetcher)
    worker = WorkerBackend(url=WORKER_URL)
    outcome = {}

    def submit():
        try:
            worker.submit(plan(), [], hints(True, False, None), "t-waiting", flow_id=-1, node_id=-1)
        except Exception as e:
            outcome["error"] = e

    submitting = threading.Thread(target=submit)
    submitting.start()
    for _ in range(100):
        if worker._in_flight.get("t-waiting") is not None:
            break
        threading.Event().wait(0.05)

    worker.cancel(JobHandle("worker", "t-waiting"))
    submitting.join(timeout=10)

    assert started[0].cancelled.is_set()
    assert isinstance(outcome["error"], TaskFailed)
    assert str(outcome["error"]) == "Task cancelled"
    assert worker._in_flight == {}
