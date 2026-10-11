"""The worker backend runs plans and operations as worker tasks and reads their results from the shared disk."""

import io
from pathlib import Path

import polars as pl
import pyarrow as pa
import pytest

from flowfile_core.configs.settings import WORKER_URL
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
from flowfile_core.flowfile.execution.errors import ForeignRef, OperationUnsupported, PlanRejected, ResultMissing
from flowfile_core.flowfile.execution.protocol import (
    ComputeBackend,
    JobHandle,
    OperationKind,
    OpResult,
    ResultRef,
    SubmitHints,
)
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import subprocess_operations as subops
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.handshake import fetch_worker_capabilities
from flowfile_core.flowfile.sources.external_sources.sql_source.models import (
    DatabaseExternalReadSettings,
    ExtDatabaseConnection,
)
from flowfile_core.schemas.input_schema import ReceivedTable
from flowfile_core.secret_manager.secret_manager import encrypt_secret

PEOPLE = pl.DataFrame(
    {"id": list(range(10)), "name": [f"n{i}" for i in range(10)], "score": [i * 1.5 for i in range(10)]}
)
FOREIGN_REF = ResultRef("local", "elsewhere", [], 0, "materialised", b"/elsewhere.arrow")


@pytest.fixture
def running_worker():
    from tests.conftest import is_worker_running

    if not is_worker_running():
        pytest.skip("flowfile_worker is not running")


@pytest.fixture
def backend(running_worker):
    return WorkerBackend()


@pytest.fixture
def kept(backend):
    """Results a test made, released from the worker when it ends."""
    refs: list[ResultRef] = []
    yield refs
    for ref in refs:
        backend.release(ref)


@pytest.fixture(params=["websocket", "rest"])
def transport(request, monkeypatch):
    """Runs a test over the WebSocket submit and again over the REST fallback a refused WebSocket falls back to."""
    if request.param == "rest":

        def _no_websocket(**kwargs):
            raise ConnectionRefusedError("websocket disabled for this test")

        monkeypatch.setattr(subops, "streaming_start", _no_websocket)
    return request.param


def hints(preview_rows: int | None) -> SubmitHints:
    return SubmitHints(must_materialise=True, wide=False, fanout=1, preview_rows=preview_rows)


def submit_people(backend: WorkerBackend, kept: list[ResultRef], preview_rows: int | None = None):
    result = backend.submit(PEOPLE.lazy().filter(pl.col("id") >= 0).serialize(), [], hints(preview_rows))
    kept.append(result.ref)
    return result


def read_stream(data: bytes) -> pa.Table:
    return pa.ipc.open_stream(io.BytesIO(data)).read_all()


def test_the_worker_backend_is_a_compute_backend():
    worker = WorkerBackend()

    assert isinstance(worker, ComputeBackend)
    assert worker.id == "worker"


def test_the_registry_builds_the_worker_backend_from_its_built_in_table():
    registry.reset()
    try:
        made = registry.backend("worker")

        assert isinstance(made, WorkerBackend)
        assert registry.backend("worker") is made
    finally:
        registry.reset()


def test_capabilities_are_the_workers_answer(running_worker):
    assert WorkerBackend().capabilities() == fetch_worker_capabilities(WORKER_URL)


def test_submit_holds_the_result_in_the_file_the_worker_wrote(backend, kept, transport):
    result = submit_people(backend, kept, preview_rows=4)

    ref = result.ref
    assert ref.backend_id == "worker"
    assert ref.state == "materialised"
    assert ref.row_count == 10
    assert [(column.column_name, column.data_type) for column in ref.schema] == [
        ("id", "Int64"),
        ("name", "String"),
        ("score", "Float64"),
    ]
    assert Path(ref.location.decode()).is_file()
    assert result.handle == JobHandle("worker", ref.result_id)

    sample = read_stream(result.sample)
    assert sample.num_rows == 4
    assert sample.column_names == ["id", "name", "score"]
    assert sample.column("id").to_pylist() == [0, 1, 2, 3]


def test_submit_without_preview_rows_returns_no_sample(backend, kept):
    assert submit_people(backend, kept).sample is None


def test_a_plan_the_worker_cannot_load_is_rejected(backend, transport):
    with pytest.raises(PlanRejected, match="DSL_VERSION"):
        backend.submit(b"not a plan", [], hints(None))


def test_sample_never_holds_more_than_n_rows(backend, kept):
    ref = submit_people(backend, kept).ref

    assert read_stream(backend.sample(ref, 3)).num_rows == 3
    assert read_stream(backend.sample(ref, 0)).num_rows == 0
    assert read_stream(backend.sample(ref, 50)).num_rows == 10


def test_count_is_the_rows_in_the_file(backend, kept):
    assert backend.count(submit_people(backend, kept).ref) == 10


def test_a_released_result_is_gone(backend, kept):
    ref = submit_people(backend, kept).ref
    assert backend.exists(ref)

    backend.release(ref)

    assert not backend.exists(ref)
    with pytest.raises(ResultMissing):
        backend.plan_for(ref)
    with pytest.raises(ResultMissing):
        backend.sample(ref, 1)


def test_plan_for_is_a_leaf_further_plans_build_on(backend, kept):
    ref = submit_people(backend, kept).ref

    rows = backend.plan_for(ref).filter(pl.col("score") > 9).select("id").collect()

    assert rows["id"].to_list() == [7, 8, 9]


def test_the_job_is_done_once_submit_returns(backend, kept):
    status = backend.status(submit_people(backend, kept).handle)

    assert status.state == "done"
    assert status.error is None


@pytest.mark.parametrize(
    "call",
    [
        lambda worker: worker.sample(FOREIGN_REF, 1),
        lambda worker: worker.count(FOREIGN_REF),
        lambda worker: worker.exists(FOREIGN_REF),
        lambda worker: worker.release(FOREIGN_REF),
        lambda worker: worker.plan_for(FOREIGN_REF),
        lambda worker: worker.submit(PEOPLE.lazy().serialize(), [FOREIGN_REF], hints(None)),
        lambda worker: worker.status(JobHandle("local", "elsewhere")),
        lambda worker: worker.cancel(JobHandle("local", "elsewhere")),
    ],
    ids=["sample", "count", "exists", "release", "plan_for", "submit_input", "status", "cancel"],
)
def test_a_reference_another_backend_minted_is_refused_before_the_worker_is_called(call):
    with pytest.raises(ForeignRef, match="'local'"):
        call(WorkerBackend())


def test_a_file_read_runs_through_the_create_table_route(backend, kept, tmp_path):
    csv_path = tmp_path / "people.csv"
    PEOPLE.write_csv(csv_path)
    received_table = ReceivedTable.create_from_path(str(csv_path), "csv")
    received_table.set_absolute_filepath()

    result = backend.run_op(OperationKind.file_read, {"received_table": received_table, "flow_id": 1, "node_id": 1})

    ref = result.refs["result"]
    kept.append(ref)
    assert list(result.refs) == ["result"]
    assert result.metadata == {}
    assert ref.row_count == 10
    assert [column.column_name for column in ref.schema] == ["id", "name", "score"]
    assert backend.plan_for(ref).collect().equals(PEOPLE)


def test_a_file_write_produces_no_result_and_writes_the_file(backend, tmp_path):
    target = tmp_path / "people.parquet"
    payload = {
        "lf": PEOPLE.lazy(),
        "data_type": "parquet",
        "path": str(target),
        "write_mode": "overwrite",
        "flow_id": 1,
        "node_id": 1,
    }

    result = backend.run_op(OperationKind.file_write, payload)

    assert result == OpResult(refs={})
    assert pl.read_parquet(target).equals(PEOPLE)


@pytest.fixture
def postgres():
    from test_utils.postgres.fixtures import can_connect_to_db

    if not can_connect_to_db():
        pytest.skip("the test PostgreSQL database is not running")


def test_a_database_read_returns_a_reference_to_the_rows_it_read(backend, kept, postgres):
    connection = ExtDatabaseConnection(
        database_type="postgresql",
        username="testuser",
        password=encrypt_secret("testpass", user_id=1),
        host="localhost",
        port=5433,
        database="testdb",
    )
    settings = DatabaseExternalReadSettings(
        connection=connection, node_id=1, flow_id=1, query="SELECT * FROM public.movies"
    )

    ref = backend.run_op(OperationKind.database_read, {"settings": settings}).refs["result"]
    kept.append(ref)

    assert ref.row_count > 0
    assert backend.plan_for(ref).head(5).collect().height == 5


@pytest.mark.parametrize("op", [OperationKind.random_split, OperationKind.viz_session])
def test_an_operation_without_a_worker_task_is_unsupported(op):
    with pytest.raises(OperationUnsupported, match=op.value):
        WorkerBackend().run_op(op, {})


def test_export_and_import_are_unsupported():
    worker = WorkerBackend()
    ref = ResultRef("worker", "task", [], 0, "materialised", b"/nowhere.arrow")

    with pytest.raises(OperationUnsupported):
        worker.export(ref, "s3://bucket/out.parquet", None)
    with pytest.raises(OperationUnsupported):
        worker.import_uri("s3://bucket/in.parquet", [], None, None)
