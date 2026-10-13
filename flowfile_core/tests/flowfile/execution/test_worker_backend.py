"""The worker backend runs plans and operations as worker tasks and reads their results from the shared disk.

It exports such a result in a worker task that sinks it, never in core, and holds an imported URI as a deferred plan
in process. A blob is held over the ``/v1`` routes only, so an import of one over the task routes is refused.
"""

import io
import re
import socket
import uuid
from pathlib import Path
from urllib.parse import urlsplit

import polars as pl
import pyarrow as pa
import pytest
import requests

from flowfile_core.configs import settings
from flowfile_core.configs.settings import WORKER_URL
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends import worker as worker_module
from flowfile_core.flowfile.execution.backends.local import LocalBackend
from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
from flowfile_core.flowfile.execution.errors import (
    ComputeUnavailable,
    ForeignRef,
    OperationUnsupported,
    PlanRejected,
    ResultMissing,
)
from flowfile_core.flowfile.execution.payloads import (
    op_payload,
    CustomNodePayload,
    FuzzyJoinPayload,
    MlApplyPayload,
    MlTrainPayload,
    RandomSplitPayload,
)
from flowfile_core.flowfile.execution.protocol import (
    ComputeBackend,
    CredentialRef,
    JobHandle,
    OperationKind,
    OpResult,
    ResultRef,
    SubmitHints,
)
from flowfile_core.flowfile.execution.payloads import FileWritePayload
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake, streaming
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import subprocess_operations as subops
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.handshake import fetch_worker_capabilities
from flowfile_core.flowfile.execution.payloads import DatabaseReadPayload, FileReadPayload, op_payload
from flowfile_core.flowfile.node_designer import CustomNodeBase
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.input_schema import (
    DatabaseConnection,
    DatabaseSettings,
    NodeDatabaseReader,
    NodeRead,
    OutputSettings,
    ReceivedTable,
)
from flowfile_core.secret_manager.secret_manager import encrypt_secret
from shared.compute_protocol import local_capabilities

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
    return WorkerBackend(url=WORKER_URL)


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


def new_result_id() -> str:
    return f"t-worker-backend-{uuid.uuid4().hex}"


def submit_people(backend: WorkerBackend, kept: list[ResultRef], preview_rows: int | None = None):
    plan = PEOPLE.lazy().filter(pl.col("id") >= 0)
    result = backend.submit(plan, [], hints(preview_rows), new_result_id(), flow_id=-1, node_id=-1)
    kept.append(result.ref)
    return result


def read_stream(data: bytes) -> pa.Table:
    return pa.ipc.open_stream(io.BytesIO(data)).read_all()


def test_the_worker_backend_is_a_compute_backend():
    worker = WorkerBackend(url=WORKER_URL)

    assert isinstance(worker, ComputeBackend)
    assert worker.id == "worker"


def test_the_registry_builds_the_worker_backend_from_its_built_in_table():
    registry.reset()
    try:
        made = registry.backend("worker")

        assert isinstance(made, WorkerBackend)
        assert made.url == WORKER_URL
        assert registry.backend("worker") is made
    finally:
        registry.reset()


def test_capabilities_are_the_workers_answer(running_worker):
    assert WorkerBackend(url=WORKER_URL).capabilities() == fetch_worker_capabilities(WORKER_URL)


def test_capabilities_pass_the_workers_declaration_through(monkeypatch):
    declared = local_capabilities().model_copy(
        update={
            "operations": frozenset({"fuzzy_join"}),
            "read_schemes": frozenset({"s3"}),
            "write_schemes": frozenset({"s3"}),
            "credential_resolution": "vended",
        }
    )
    answer = requests.Response()
    answer.status_code = 200
    answer._content = declared.model_dump_json().encode()
    monkeypatch.setattr(worker_module.requests, "get", lambda url, **kwargs: answer)

    assert WorkerBackend(url=WORKER_URL).capabilities() == declared


@pytest.fixture
def closed_port_url() -> str:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    return f"http://127.0.0.1:{port}"


def same_worker_by_another_name() -> str:
    """The session worker's URL with its host spelled another way: a call that arrives by this name was sent by a
    backend made on it, never by the URL the worker settings configure."""
    parts = urlsplit(WORKER_URL)
    host = "localhost" if parts.hostname == "127.0.0.1" else "127.0.0.1"
    return f"{parts.scheme}://{host}:{parts.port}{parts.path}"


def test_a_backend_made_on_another_url_sends_every_call_there(running_worker, monkeypatch):
    url = same_worker_by_another_name()
    assert url != WORKER_URL
    configured_answer = fetch_worker_capabilities(WORKER_URL)
    sent, connects = [], []
    real_send, real_connect = requests.Session.send, streaming.connect

    def recording_send(self, request, **kwargs):
        sent.append(request.url)
        return real_send(self, request, **kwargs)

    def recording_connect(ws_url, *args, **kwargs):
        connects.append(ws_url)
        return real_connect(ws_url, *args, **kwargs)

    monkeypatch.setattr(requests.Session, "send", recording_send)
    monkeypatch.setattr(streaming, "connect", recording_connect)
    worker = WorkerBackend(url=url)
    # This backend makes the process's first worker contact, so the handshake's own calls are recorded too.
    handshake.reset_handshake()
    try:
        result = worker.submit(PEOPLE.lazy(), [], hints(4), new_result_id(), flow_id=-1, node_id=-1)
        assert read_stream(result.sample).num_rows == 4
        assert worker.exists(result.ref) is True
        assert worker.capabilities() == configured_answer
        worker.release(result.ref)
        assert worker.exists(result.ref) is False
    finally:
        handshake.reset_handshake()

    assert sent and all(sent_to.startswith(f"{url}/") for sent_to in sent), sent
    websocket_url = url.replace("http://", "ws://", 1)
    assert connects and all(connected.startswith(f"{websocket_url}/") for connected in connects), connects


def test_a_backend_made_on_a_url_nothing_answers_fails_naming_it(running_worker, closed_port_url):
    dead = WorkerBackend(url=closed_port_url)

    with pytest.raises(ComputeUnavailable, match=re.escape(f"Could not reach the worker at {closed_port_url}")):
        dead.capabilities()
    with pytest.raises(ComputeUnavailable, match=re.escape(f"Could not reach the worker at {closed_port_url}")):
        dead.submit(PEOPLE.lazy(), [], hints(None), new_result_id(), flow_id=-1, node_id=-1)

    configured = registry.backend(registry.WORKER_BACKEND_ID)
    assert configured.url == WORKER_URL
    assert configured.capabilities() == fetch_worker_capabilities(WORKER_URL)


def test_submit_holds_the_result_in_the_file_the_worker_wrote(backend, kept, transport):
    result = submit_people(backend, kept, preview_rows=4)

    ref = result.ref
    assert ref.backend_id == "worker"
    assert ref.result_id.startswith("t-worker-backend-")
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


@pytest.fixture
def core_only_udf_plan(tmp_path, monkeypatch) -> pl.LazyFrame:
    """A plan whose UDF lives in a module only this (core) process can import, as with separate images."""
    module_name = f"core_only_udf_{tmp_path.name.replace('-', '_')}"
    (tmp_path / f"{module_name}.py").write_text("def plus_one(s):\n    return s + 1\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    module = __import__(module_name)
    return pl.LazyFrame({"a": [1, 2]}).select(pl.col("a").map_batches(module.plus_one, return_dtype=pl.Int64))


def test_a_plan_the_worker_cannot_load_is_rejected(backend, transport, core_only_udf_plan):
    with pytest.raises(PlanRejected, match="core_only_udf"):
        backend.submit(core_only_udf_plan, [], hints(None), new_result_id(), flow_id=-1, node_id=-1)


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
        lambda worker: worker.export(FOREIGN_REF, "file:///nowhere/out.parquet", None),
        lambda worker: worker.submit(PEOPLE.lazy(), [FOREIGN_REF], hints(None), "t-foreign", flow_id=-1, node_id=-1),
        lambda worker: worker.status(JobHandle("local", "elsewhere")),
        lambda worker: worker.cancel(JobHandle("local", "elsewhere")),
    ],
    ids=["sample", "count", "exists", "release", "plan_for", "export", "submit_input", "status", "cancel"],
)
def test_a_reference_another_backend_minted_is_refused_before_the_worker_is_called(call):
    with pytest.raises(ForeignRef, match="'local'"):
        call(WorkerBackend(url=WORKER_URL))


def test_a_file_read_runs_through_the_create_table_route(backend, kept, tmp_path):
    csv_path = tmp_path / "people.csv"
    PEOPLE.write_csv(csv_path)
    received_table = ReceivedTable.create_from_path(str(csv_path), "csv")
    received_table.set_absolute_filepath()
    settings = NodeRead(flow_id=1, node_id=1, received_file=received_table)

    read = FileReadPayload(flow_id=1, node_id=1, settings=settings, node_logger=None)
    result = backend.run_op(OperationKind.file_read, op_payload(read))

    ref = result.refs["result"]
    kept.append(ref)
    assert list(result.refs) == ["result"]
    assert result.metadata == {}
    assert ref.row_count == 10
    assert [column.column_name for column in ref.schema] == ["id", "name", "score"]
    assert backend.plan_for(ref).collect().equals(PEOPLE)


def test_a_file_write_produces_no_result_and_writes_the_file(backend, tmp_path):
    target = tmp_path / "people.parquet"
    settings = OutputSettings(
        name=target.name,
        directory=str(tmp_path),
        file_type="parquet",
        write_mode="overwrite",
        table_settings={"file_type": "parquet"},
    )
    write = FileWritePayload(plan=PEOPLE.lazy(), settings=settings, flow_id=1, node_id=1)

    result = backend.run_op(OperationKind.file_write, op_payload(write))

    assert result == OpResult(refs={})
    assert pl.read_parquet(target).equals(PEOPLE)


@pytest.fixture
def postgres():
    from test_utils.postgres.fixtures import can_connect_to_db

    if not can_connect_to_db():
        pytest.skip("the test PostgreSQL database is not running")


def test_a_database_read_returns_a_reference_to_the_rows_it_read(backend, kept, postgres):
    connection = DatabaseConnection(
        database_type="postgresql", username="testuser", host="localhost", port=5433, database="testdb"
    )
    settings = NodeDatabaseReader(
        flow_id=1,
        node_id=1,
        database_settings=DatabaseSettings(database_connection=connection, schema_name="public", table_name="movies"),
    )
    read = DatabaseReadPayload(
        flow_id=1,
        node_id=1,
        settings=settings,
        connection=connection,
        encrypted_password=encrypt_secret("testpass", user_id=1),
        cancel_check=lambda: False,
    )

    ref = backend.run_op(OperationKind.database_read, op_payload(read)).refs["result"]
    kept.append(ref)

    assert ref.row_count > 0
    assert backend.plan_for(ref).head(5).collect().height == 5


@pytest.mark.parametrize("kind", list(OperationKind), ids=lambda kind: kind.value)
def test_the_worker_backend_runs_every_operation_kind(kind):
    """Each kind reaches its method, which asks for its payload before it calls the worker."""
    with pytest.raises(ValueError, match="has no 'payload' key"):
        WorkerBackend(url=WORKER_URL).run_op(kind, {})


@pytest.fixture
def task_routes(monkeypatch):
    """The protocol flag off for the test, so its results are minted over the task routes."""
    monkeypatch.setattr(settings.FLOWFILE_COMPUTE_PROTOCOL, "value", False)


DEFERRED = SubmitHints(must_materialise=False, wide=False, fanout=1, preview_rows=None)


def test_export_of_a_task_route_result_is_a_worker_task_that_sinks_it(
    backend, kept, task_routes, transport, monkeypatch, tmp_path
):
    stored = submit_people(backend, kept).ref
    deferred = backend.submit(PEOPLE.lazy(), [], DEFERRED, new_result_id(), flow_id=-1, node_id=-1).ref
    kept.append(deferred)
    sunk, tasks = [], []
    real_sink, real_fetcher = pl.LazyFrame.sink_parquet, worker_module.ExternalDfFetcher

    def recording_sink(self, path, *args, **kwargs):
        sunk.append(path)
        return real_sink(self, path, *args, **kwargs)

    def recording_fetcher(*args, **kwargs):
        tasks.append((kwargs["operation_type"], kwargs["kwargs"]))
        return real_fetcher(*args, **kwargs)

    monkeypatch.setattr(pl.LazyFrame, "sink_parquet", recording_sink)
    monkeypatch.setattr(worker_module, "ExternalDfFetcher", recording_fetcher)
    from_file, from_plan = f"file://{tmp_path}/stored.parquet", f"file://{tmp_path}/deferred.parquet"

    assert backend.export(stored, from_file, CredentialRef(connection_id=7, user_id=3)) == from_file
    assert backend.export(deferred, from_plan, None) == from_plan

    assert (stored.state, deferred.state) == ("materialised", "deferred")
    assert sunk == []
    assert tasks == [
        ("export_result", {"uri": from_file, "credential": {"connection_id": 7, "user_id": 3}}),
        ("export_result", {"uri": from_plan, "credential": None}),
    ]
    assert pl.read_parquet(from_file).equals(PEOPLE)
    assert pl.read_parquet(from_plan).equals(PEOPLE)


def test_an_export_of_a_task_route_result_writes_nothing_when_the_worker_is_unreachable(
    task_routes, closed_port_url, tmp_path
):
    dead = WorkerBackend(url=closed_port_url)
    deferred = dead.submit(PEOPLE.lazy(), [], DEFERRED, new_result_id(), flow_id=-1, node_id=-1).ref
    target = tmp_path / "out.parquet"

    try:
        with pytest.raises(ComputeUnavailable):
            dead.export(deferred, f"file://{target}", None)
    finally:
        dead.release(deferred)

    assert not target.exists()


def test_import_uri_holds_a_deferred_plan_in_process(backend, task_routes, tmp_path):
    path = tmp_path / "in.parquet"

    ref = backend.import_uri(f"file://{path}", worker_module._flowfile_schema(PEOPLE.schema), PEOPLE.height, None)

    try:
        assert (ref.backend_id, ref.state, ref.row_count) == ("worker", "deferred", PEOPLE.height)
        assert ref.location == ref.result_id.encode()
        assert backend.exists(ref)
        assert isinstance(backend.plan_for(ref), pl.LazyFrame)
        assert backend.result_file(ref) is None

        PEOPLE.write_parquet(path)

        assert pl.from_arrow(read_stream(backend.sample(ref, 3))).equals(PEOPLE.head(3))
        assert backend.result_file(ref) is not None
        assert backend.count(ref) == PEOPLE.height
    finally:
        backend.release(ref)
    assert not backend.exists(ref)
    assert path.is_file()


def test_a_blob_import_over_the_task_routes_is_unsupported(task_routes, monkeypatch, tmp_path):
    sent = []
    monkeypatch.setattr(requests.Session, "send", lambda self, request, **kwargs: sent.append(request))
    monkeypatch.setattr(worker_module, "ExternalDfFetcher", lambda *args, **kwargs: sent.append(kwargs))

    with pytest.raises(OperationUnsupported, match="/v1 routes"):
        WorkerBackend(url=WORKER_URL).import_uri(f"file://{tmp_path}/model.bin", None, None, None, kind="blob")

    assert sent == []


SPLIT_NODE_SOURCE = """
import polars as pl
from flowfile_core.flowfile.node_designer import CustomNodeBase


class WorkerBackendSplit(CustomNodeBase):
    node_name: str = "Worker Backend Split"
    number_of_outputs: int = 2
    output_names: list[str] = ["big", "small"]

    def process(self, *inputs):
        rows = inputs[0]
        return {"big": rows.filter(pl.col("id") > 6), "small": rows.filter(pl.col("id") <= 6)}
"""


class PlacedSplit(CustomNodeBase):
    """The class the node was placed with in core; it selects no secret."""

    node_name: str = "Worker Backend Split"
    number_of_outputs: int = 2
    output_names: list[str] = ["big", "small"]


def fuzzy_settings() -> input_schema.NodeFuzzyMatch:
    return input_schema.NodeFuzzyMatch(
        flow_id=1,
        node_id=2,
        join_input={
            "join_mapping": [
                {"left_col": "name", "right_col": "name", "threshold_score": 75, "fuzzy_type": "levenshtein"}
            ],
            "left_select": {"renames": [{"old_name": "name", "new_name": "name", "join_key": True}]},
            "right_select": {"renames": [{"old_name": "name", "new_name": "name", "join_key": True}]},
            "how": "inner",
        },
    )


def test_a_fuzzy_join_matches_on_the_worker_as_it_does_in_core(backend, kept):
    names = pl.LazyFrame({"name": ["eduward", "edward", "courtney"]})
    join = FuzzyJoinPayload(
        left=names, right=names, settings=fuzzy_settings(), flow_id=1, node_id=2, job_id=f"{new_result_id()}_fm"
    )

    ref = backend.run_op(OperationKind.fuzzy_join, op_payload(join)).refs["result"]
    kept.append(ref)

    assert (ref.result_id, ref.state, ref.row_count) == (join.job_id, "materialised", 5)
    in_core = LocalBackend()
    local_ref = in_core.run_op(OperationKind.fuzzy_join, op_payload(join)).refs["result"]
    columns = ["name", "name_right", "name_vs_name_right_levenshtein"]
    on_worker = backend.plan_for(ref).collect().sort(columns)
    assert on_worker.equals(in_core.plan_for(local_ref).collect().sort(columns))


def test_a_random_split_on_the_worker_partitions_like_core(backend, kept):
    settings = input_schema.NodeRandomSplit(flow_id=1, node_id=3, seed=11)
    rows = pl.LazyFrame({"n": list(range(50))})
    split = RandomSplitPayload(plan=rows, settings=settings, flow_id=1, node_id=3, job_id=new_result_id())
    try:
        refs = backend.run_op(OperationKind.random_split, op_payload(split)).refs
        kept.extend(refs.values())

        assert [(ref.state, ref.row_count) for ref in refs.values()] == [("deferred", 40), ("deferred", 10)]
        on_worker = {name: backend.plan_for(ref).collect()["n"].to_list() for name, ref in refs.items()}
        in_core = LocalBackend()
        local_refs = in_core.run_op(OperationKind.random_split, op_payload(split)).refs
        assert on_worker == {name: in_core.plan_for(ref).collect()["n"].to_list() for name, ref in local_refs.items()}
        assert backend.status(JobHandle("worker", split.job_id)).state == "done"
    finally:
        subops.clear_task_from_worker(split.job_id, worker_url=WORKER_URL)


def test_a_custom_node_runs_on_the_worker_from_its_node_source(backend, kept):
    rows = pl.LazyFrame({"id": list(range(10))})
    job = CustomNodePayload(
        inputs=(rows,),
        custom_node=PlacedSplit(),
        node_source=SPLIT_NODE_SOURCE,
        class_name="WorkerBackendSplit",
        settings_values={},
        output_names=["big", "small"],
        user_id=None,
        flow_id=1,
        node_id=4,
        job_id=f"{new_result_id()}-custom-node",
    )

    refs = backend.run_op(OperationKind.custom_node, op_payload(job)).refs
    kept.extend(refs.values())

    assert list(refs) == ["output-0", "output-1"]
    assert [(ref.state, ref.row_count) for ref in refs.values()] == [("materialised", 3), ("materialised", 7)]
    assert backend.plan_for(refs["output-0"]).collect()["id"].to_list() == [7, 8, 9]
    assert backend.plan_for(refs["output-1"]).collect()["id"].to_list() == list(range(7))


def test_a_model_trained_on_the_worker_scores_on_the_worker(backend, kept, tmp_path):
    model_path = tmp_path / "model.json"
    training = pl.LazyFrame({"x": [1.0, 2.0, 3.0, 4.0], "y": [3.0, 5.0, 7.0, 9.0]})
    train = MlTrainPayload(
        plan=training,
        staging_path=str(model_path),
        model_type="linear_regression",
        target_column="y",
        feature_columns=["x"],
        params={"add_bias": True},
        flow_id=1,
        node_id=5,
        job_id=new_result_id(),
    )
    apply = MlApplyPayload(
        plan=pl.LazyFrame({"x": [10.0]}),
        model_path=str(model_path),
        output_column="pred",
        flow_id=1,
        node_id=6,
        job_id=new_result_id(),
    )
    try:
        report = backend.run_op(OperationKind.ml_train, op_payload(train))

        assert report.refs == {}
        assert report.metadata["model_type"] == "linear_regression"
        assert report.metadata["size_bytes"] == model_path.stat().st_size
        assert len(report.metadata["sha256"]) == 64

        ref = backend.run_op(OperationKind.ml_apply, op_payload(apply)).refs["result"]
        kept.append(ref)

        assert (ref.result_id, ref.state, ref.row_count) == (apply.job_id, "materialised", 1)
        assert backend.plan_for(ref).collect()["pred"].round(6).to_list() == [21.0]
    finally:
        # Clearing the training task deletes the file it names, the model.
        subops.clear_task_from_worker(train.job_id, worker_url=WORKER_URL)
