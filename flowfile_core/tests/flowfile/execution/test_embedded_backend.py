"""The embedded worker backend: the worker's ``/v1`` bodies called in this process, with no port and no wire.

Every case runs ``EmbeddedWorkerBackend`` with the worker's jobs in this process (``spawner.in_process``) and leaves
the worker port as it found it, whether the session's worker listens there or none does; a child started, a pool
member leased, a socket that listens, a submit socket dialled or a request through the worker session fails the case
(``in_this_process``, which ``test_local_backend.py`` shares). The error mapping is the adapter's: a child's failure
is a ``TaskFailed`` naming the error, a plan this process cannot load a ``PlanRejected``, a result the worker does not
hold a ``ResultMissing``, and what a body refuses before anything runs a ``PlanRejected``.
The handshake passes with its probes run as in-process jobs, the plan crosses as bytes, each operation the adapter
builds a body for holds its outputs, an import and an export cross a URI, and after a job a secret core encrypted still
decrypts through the resolver the worker registers. The adapter declares every kind the worker declares, with ``file``
among its schemes. What every backend guarantees is the conformance suite's, which runs this one too; the seven kinds
the worker serves on its task routes and a custom node with no node file are ``test_local_backend.py``'s.
"""

import hashlib
import importlib
import json
import shutil
import socket
import sys
import uuid
from dataclasses import replace
from pathlib import Path

import polars as pl
import pytest
import websockets.sync.client

from flowfile_core.configs import settings
from flowfile_core.flowfile.execution.backends import worker as worker_module
from flowfile_core.flowfile.execution.backends.embedded import EmbeddedWorkerBackend
from flowfile_core.flowfile.execution.errors import (
    ForeignRef,
    OperationUnsupported,
    PlanRejected,
    ResultMissing,
    TaskFailed,
)
from flowfile_core.flowfile.execution.payloads import (
    FuzzyJoinPayload,
    PolarsCodePayload,
    RandomSplitPayload,
    RestApiReadPayload,
    SqlQueryPayload,
    op_payload,
)
from flowfile_core.flowfile.execution.protocol import (
    ComputeBackend,
    CredentialRef,
    JobHandle,
    OperationKind,
    ResultRef,
    SubmitHints,
    SubmitResult,
)
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import subprocess_operations as subops
from flowfile_core.schemas import input_schema
from flowfile_core.secret_manager.secret_manager import encrypt_secret
from flowfile_worker import mp_context, pool, service, spawner
from shared.cloud_credential_provider import EncryptedCredentialProvider
from shared.compute_protocol import PROTOCOL_VERSION, WorkerLocation, file_uri, local_capabilities
from shared.ml.trainers import get_trainer
from shared.path_utils import DirectoryScanUnsupportedError
from shared.storage_config import storage
from tests.flowfile.execution.conftest import fuzzy_settings, serve
from tests.flowfile.execution.test_worker_backend_v1 import (
    PEOPLE,
    SPLIT_NODE_SOURCE,
    PlacedSplit,
    custom_node_payload,
    file_read_payload,
    ml_apply_payload,
    ml_train_payload,
)

BLOB = bytes(range(256)) * 64
STATES = pytest.mark.parametrize("must_materialise", [True, False], ids=["materialised", "deferred"])


class _Bytes:
    """A stand-in plan: the adapter only calls ``serialize()``, so these bytes cross as the plan."""

    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def serialize(self) -> bytes:
        return self._payload


@pytest.fixture(scope="module")
def json_api():
    """A JSON API on a free loopback port answering every GET with the rows of ``PEOPLE``.

    Module-scoped, so its socket listens before ``no_port`` refuses a listen for each case.
    """
    with serve(json.dumps(PEOPLE.to_dicts()).encode(), "application/json") as url:
        yield url


@pytest.fixture(autouse=True)
def in_process(monkeypatch):
    """The worker's jobs run in this process, as a library host sets it."""
    monkeypatch.setattr(spawner, "in_process", True)


def _worker_port_refused() -> bool:
    try:
        socket.create_connection(("127.0.0.1", settings.WORKER_PORT), timeout=1).close()
    except OSError:
        return True
    return False


@pytest.fixture(autouse=True)
def in_this_process(monkeypatch):
    """Every job runs here: a child started, a pool member leased, a socket listened, a submit socket dialled or a
    request through the worker session fails the case, and the worker port is as it was before the case after it."""

    def refused(what: str):
        def fail(*args, **kwargs):
            pytest.fail(f"{what} while the embedded worker ran")

        return fail

    port_refused_before = _worker_port_refused()
    monkeypatch.setattr(mp_context, "Process", refused("a child was started"))
    monkeypatch.setattr(pool.task_pool, "acquire", refused("a pool member was leased"))
    monkeypatch.setattr(socket.socket, "listen", refused("a socket listened"))
    monkeypatch.setattr(websockets.sync.client, "connect", refused("a submit socket was dialled"))
    monkeypatch.setattr(worker_module, "connect", refused("a submit socket was dialled"))
    monkeypatch.setattr(subops._worker_session, "request", refused("a request went through the worker session"))
    yield
    assert _worker_port_refused() == port_refused_before, f"the case opened or closed port {settings.WORKER_PORT}"


@pytest.fixture
def backend() -> EmbeddedWorkerBackend:
    return EmbeddedWorkerBackend()


@pytest.fixture
def kept(backend):
    """Results a case made, released when it ends."""
    refs: list[ResultRef] = []
    yield refs
    for ref in refs:
        backend.release(ref)


@pytest.fixture
def inline_jobs(monkeypatch) -> list[str]:
    """The task id of every job run in this process from here on, in order."""
    ran: list[str] = []
    real = spawner.run_inline

    def recording(task_id, target, kwargs):
        ran.append(task_id)
        return real(task_id, target, kwargs)

    monkeypatch.setattr(spawner, "run_inline", recording)
    return ran


@pytest.fixture
def checked(backend):
    """``backend`` with no verdict before the case and none left after it."""
    handshake.reset_handshake()
    yield backend
    handshake.reset_handshake()


@pytest.fixture
def blob_source(tmp_path) -> Path:
    source = tmp_path / "model.bin"
    source.write_bytes(BLOB)
    return source


def hints(*, must_materialise: bool = False, preview_rows: int | None = None, wide: bool = False) -> SubmitHints:
    return SubmitHints(must_materialise=must_materialise, wide=wide, fanout=1, preview_rows=preview_rows)


def new_id() -> str:
    return f"t-embedded-{uuid.uuid4().hex}"


def submit(
    backend: EmbeddedWorkerBackend,
    kept: list[ResultRef],
    *,
    plan=None,
    inputs: list[ResultRef] | None = None,
    result_id: str | None = None,
    **hint_values,
) -> SubmitResult:
    plan = PEOPLE.lazy().filter(pl.col("id") >= 0) if plan is None else plan
    result = backend.submit(plan, inputs or [], hints(**hint_values), result_id or new_id(), flow_id=-1, node_id=-1)
    kept.append(result.ref)
    return result


def rows(stream: bytes) -> pl.DataFrame:
    return pl.read_ipc_stream(stream)


def location(ref: ResultRef) -> WorkerLocation:
    return WorkerLocation.model_validate_json(ref.location)


def columns(ref: ResultRef) -> list[str]:
    return [column.column_name for column in ref.schema]


def dtypes(ref: ResultRef) -> list[tuple[str, str]]:
    return [(column.column_name, column.data_type) for column in ref.schema]


def test_the_embedded_worker_is_a_backend_that_declares_every_kind_the_worker_declares_and_file(backend):
    assert isinstance(backend, ComputeBackend)
    assert (backend.id, backend.protocol_version) == ("local", PROTOCOL_VERSION)

    declared = backend.capabilities()

    assert declared.operations == service.OPERATIONS == frozenset(kind.value for kind in OperationKind)
    worker = service.worker_capabilities()
    assert declared == worker.model_copy(
        update={"read_schemes": worker.read_schemes | {"file"}, "write_schemes": worker.write_schemes | {"file"}}
    )


@pytest.mark.parametrize(
    ("plan", "hint_values", "error"),
    [
        (pl.LazyFrame({"a": [1]}).select("missing"), {"must_materialise": True}, "ColumnNotFoundError"),
        (pl.LazyFrame({"a": [1]}).select("missing"), {"must_materialise": False}, "ColumnNotFoundError"),
        (
            pl.LazyFrame({"a": ["x"]}).select(pl.col("a").cast(pl.Int64, strict=True)),
            {"must_materialise": True, "preview_rows": 5},
            "InvalidOperationError",
        ),
    ],
    ids=["materialised", "deferred", "while-collected"],
)
def test_a_plan_that_fails_on_its_data_is_a_task_failure_naming_the_error(backend, plan, hint_values, error):
    """A plan whose schema fails, and one whose schema resolves but whose rows fail as they are collected."""
    result_id = new_id()

    with pytest.raises(TaskFailed) as failed:
        submit(backend, [], plan=plan, result_id=result_id, **hint_values)

    assert str(failed.value).startswith(error)
    job = backend.status(JobHandle(backend.id, result_id))
    assert (job.state, job.progress) == ("failed", -1)
    assert job.error.startswith(error)
    assert backend.exists(ResultRef(backend.id, result_id, [], None, "deferred", b"")) is False


def test_bytes_that_are_no_plan_are_rejected(backend):
    with pytest.raises(PlanRejected):
        backend.submit(_Bytes(b"not a plan"), [], hints(), new_id(), flow_id=-1, node_id=-1)


def test_a_plan_over_a_module_this_process_cannot_import_is_rejected(backend, tmp_path, monkeypatch):
    """The bytes are made while the module exists, as a plan from another image would be, and the module then goes."""
    module_name = f"core_only_udf_{tmp_path.name.replace('-', '_')}"
    (tmp_path / f"{module_name}.py").write_text("def plus_one(s):\n    return s + 1\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    module = importlib.import_module(module_name)
    plan = pl.LazyFrame({"a": [1, 2]}).select(pl.col("a").map_batches(module.plus_one, return_dtype=pl.Int64))
    payload = plan.serialize()
    (tmp_path / f"{module_name}.py").unlink()
    shutil.rmtree(tmp_path / "__pycache__", ignore_errors=True)
    sys.modules.pop(module_name, None)

    with pytest.raises(PlanRejected):
        backend.submit(_Bytes(payload), [], hints(), new_id(), flow_id=-1, node_id=-1)


@STATES
def test_a_released_result_is_missing_as_an_input_and_to_an_export(backend, must_materialise, tmp_path):
    """What the conformance suite's released result leaves out; releasing it again changes nothing."""
    ref = submit(backend, [], must_materialise=must_materialise).ref
    target = tmp_path / "released.parquet"

    backend.release(ref)

    with pytest.raises(ResultMissing, match=ref.result_id):
        submit(backend, [], inputs=[ref])
    with pytest.raises(ResultMissing, match=ref.result_id):
        backend.export(ref, file_uri(target), None)
    assert not target.exists()
    backend.release(ref)


def test_submitting_under_a_held_id_replaces_that_result(backend, kept):
    result_id = new_id()
    first = submit(backend, kept, result_id=result_id).ref

    second = submit(backend, kept, plan=pl.LazyFrame({"n": [1, 2, 3]}), result_id=result_id).ref

    assert first.result_id == second.result_id == result_id
    assert backend.exists(first) and backend.exists(second)
    assert backend.plan_for(second).collect()["n"].to_list() == [1, 2, 3]


def test_a_result_another_backend_minted_is_refused_as_an_input_and_exports_nothing(backend, tmp_path):
    """Every read, a submit and an export refuse a hand-made foreign reference, so no other backend needs to run."""
    foreign = ResultRef("worker", "abc", [], None, "deferred", b"abc")

    for read in (backend.plan_for, backend.exists, backend.release, backend.count, lambda ref: backend.sample(ref, 1)):
        with pytest.raises(ForeignRef):
            read(foreign)
    with pytest.raises(ForeignRef):
        submit(backend, [], inputs=[foreign])
    with pytest.raises(ForeignRef):
        backend.export(foreign, file_uri(tmp_path / "foreign.parquet"), None)
    assert not (tmp_path / "foreign.parquet").exists()


def test_a_sample_above_the_cap_is_refused_before_anything_runs(backend, kept, inline_jobs):
    ref = submit(backend, kept, must_materialise=True).ref
    inline_jobs.clear()

    with pytest.raises(PlanRejected, match=str(service.SAMPLE_ROWS_CAP)):
        backend.sample(ref, service.SAMPLE_ROWS_CAP + 1)

    assert inline_jobs == []
    assert rows(backend.sample(ref, service.SAMPLE_ROWS_CAP)).equals(PEOPLE)


def test_an_apply_with_a_table_as_its_model_is_refused_by_the_body(backend, kept):
    table = submit(backend, kept, must_materialise=True).ref

    with pytest.raises(PlanRejected, match="holds rows, not a model"):
        backend.run_op(OperationKind.ml_apply, op_payload(ml_apply_payload(table)))


def test_a_body_the_worker_model_refuses_is_rejected_before_anything_runs(backend, kept, inline_jobs, monkeypatch):
    """The body is checked as its route checks it: a field the worker's model forbids is the route's 422."""
    table = submit(backend, kept, must_materialise=True).ref
    spec = worker_module.V1_OPS[OperationKind.ml_apply]
    monkeypatch.setitem(
        worker_module.V1_OPS,
        OperationKind.ml_apply,
        replace(spec, request=lambda job: {**spec.request(job), "model_path": "/nowhere"}),
    )
    inline_jobs.clear()

    with pytest.raises(PlanRejected, match="model_path"):
        backend.run_op(OperationKind.ml_apply, op_payload(ml_apply_payload(table)))

    assert inline_jobs == []


def test_a_directory_scan_is_refused_before_the_call(backend, tmp_path, inline_jobs):
    read = file_read_payload(tmp_path)
    read.settings.received_file.scan_mode = "directory"

    with pytest.raises(DirectoryScanUnsupportedError):
        backend.run_op(OperationKind.file_read, op_payload(read))

    assert inline_jobs == []


@STATES
def test_status_follows_a_job_and_an_unknown_job_is_a_lookup_error(backend, kept, must_materialise):
    result = submit(backend, kept, must_materialise=must_materialise)

    assert backend.status(result.handle).state == "done"
    with pytest.raises(LookupError, match="no-such-job"):
        backend.status(JobHandle(backend.id, "no-such-job"))
    backend.cancel(JobHandle(backend.id, "no-such-job"))
    backend.cancel(result.handle)
    assert backend.status(result.handle).state == "done"
    with pytest.raises(ForeignRef):
        backend.status(JobHandle("worker", result.handle.job_id))


def test_a_task_s_reported_result_is_read_by_its_handle():
    backend = EmbeddedWorkerBackend()
    task_id = new_id()
    with service.status_dict_lock:
        service.status_dict[task_id] = service.models.Status(
            background_task_id=task_id, status="Completed", file_ref="", results={"outputs": {"main": 3}}
        )
    try:
        assert backend.task_result(JobHandle(backend.id, task_id)) == {"outputs": {"main": 3}}
        with pytest.raises(ForeignRef):
            backend.task_result(JobHandle("worker", task_id))
    finally:
        with service.status_dict_lock:
            service.status_dict.pop(task_id, None)
    with pytest.raises(LookupError, match=task_id):
        backend.task_result(JobHandle(backend.id, task_id))


def test_the_handshake_passes_with_its_probes_run_in_this_process(checked, inline_jobs):
    verdict = handshake.verify(checked)

    assert (verdict.ok, verdict.differences, verdict.absent, verdict.error) == (True, [], {}, None)
    assert verdict.offered.plugins == local_capabilities().plugins
    assert [job.rsplit("-", 1)[0] for job in inline_jobs] == [
        "handshake-polars_grouper",
        "handshake-polars_ds",
        "handshake-python",
    ]
    assert handshake.verdict_for(checked) is verdict
    assert handshake.last_verdict(checked.id) is verdict


def test_the_plan_crosses_as_bytes_and_plan_for_decodes_the_location(backend, kept):
    plan = PEOPLE.lazy().filter(pl.col("id") > 3)

    deferred = submit(backend, kept, plan=plan).ref
    materialised = submit(backend, kept, plan=plan, must_materialise=True).ref

    assert location(deferred).kind == "plan"
    assert backend.plan_for(deferred).explain() == plan.explain()
    held = location(materialised)
    assert held.kind == "file"
    assert Path(held.path).is_relative_to(storage.cache_directory)
    # Polars prints a Windows path with forward slashes.
    assert Path(held.path).as_posix() in backend.plan_for(materialised).explain()
    assert rows(backend.sample(materialised, 10)).equals(PEOPLE.filter(pl.col("id") > 3))


def test_a_file_read_holds_its_rows(backend, kept, tmp_path):
    read = file_read_payload(tmp_path)

    result = backend.run_op(OperationKind.file_read, op_payload(read))

    ref = result.refs["result"]
    kept.append(ref)
    assert (list(result.refs), result.metadata) == (["result"], {})
    assert (ref.result_id, ref.state, ref.row_count) == (f"{read.job_id}_0", "materialised", 10)
    assert columns(ref) == ["id", "name", "score"]
    assert rows(backend.sample(ref, 3)).equals(PEOPLE.head(3))
    assert backend.count(ref) == 10
    backend.release(ref)
    assert backend.exists(ref) is False


def test_a_rest_api_read_holds_what_it_fetched(backend, kept, json_api):
    node = input_schema.NodeRestApiReader(
        flow_id=1, node_id=1, user_id=1, rest_api_settings=input_schema.RestApiSettings(url=f"{json_api}/people")
    )
    read = RestApiReadPayload(flow_id=1, node_id=1, settings=node, encrypted_secret=None)

    ref = backend.run_op(OperationKind.rest_api_read, op_payload(read)).refs["result"]

    kept.append(ref)
    assert (location(ref).kind, ref.row_count) == ("file", 10)
    assert rows(backend.sample(ref, 1000))["name"].to_list() == PEOPLE["name"].to_list()
    assert backend.count(ref) == 10


def test_a_fuzzy_join_holds_the_matches(backend, kept):
    names = pl.LazyFrame({"name": ["eduward", "edward", "courtney"]})
    join = FuzzyJoinPayload(
        left=names, right=names, settings=fuzzy_settings(), flow_id=1, node_id=2, job_id=f"{new_id()}_fm"
    )
    settings_before = join.settings.join_input.model_dump()

    ref = backend.run_op(OperationKind.fuzzy_join, op_payload(join)).refs["result"]

    kept.append(ref)
    assert (ref.result_id, ref.state, ref.row_count) == (f"{join.job_id}_0", "materialised", 5)
    matched = rows(backend.sample(ref, 100)).sort("name", "name_right")
    assert matched.select("name", "name_right").rows() == [
        ("courtney", "courtney"),
        ("eduward", "eduward"),
        ("eduward", "edward"),
        ("edward", "eduward"),
        ("edward", "edward"),
    ]
    assert backend.count(ref) == 5
    assert backend.status(JobHandle(backend.id, join.job_id)).state == "done"
    assert join.settings.join_input.model_dump() == settings_before


def test_a_random_split_holds_each_split_keyed_and_ordered_as_the_settings_say(backend, kept):
    node = input_schema.NodeRandomSplit(flow_id=1, node_id=3, seed=11)
    split = RandomSplitPayload(
        plan=pl.LazyFrame({"n": list(range(50))}), settings=node, flow_id=1, node_id=3, job_id=new_id()
    )

    refs = backend.run_op(OperationKind.random_split, op_payload(split)).refs

    kept.extend(refs.values())
    assert list(refs) == [group.name for group in node.splits] == ["train", "test"]
    assert [(ref.result_id, ref.state, ref.row_count) for ref in refs.values()] == [
        (f"{split.job_id}_0", "materialised", 40),
        (f"{split.job_id}_1", "materialised", 10),
    ]
    held = {name: rows(backend.sample(ref, 100))["n"].to_list() for name, ref in refs.items()}
    assert sorted(held["train"] + held["test"]) == list(range(50))
    assert [backend.count(ref) for ref in refs.values()] == [40, 10]
    again = backend.run_op(OperationKind.random_split, op_payload(replace(split, job_id=new_id()))).refs
    kept.extend(again.values())
    assert rows(backend.sample(again["train"], 100))["n"].to_list() == held["train"], "the seed fixes the shuffle"


def test_a_custom_node_holds_each_declared_output(backend, kept, clean_import_aliases):
    job = custom_node_payload(SPLIT_NODE_SOURCE, "WorkerBackendSplit", PlacedSplit(), ["big", "small"])

    refs = backend.run_op(OperationKind.custom_node, op_payload(job)).refs

    kept.extend(refs.values())
    assert list(refs) == ["output-0", "output-1"]
    assert [(ref.result_id, ref.state, ref.row_count) for ref in refs.values()] == [
        (f"{job.job_id}_0", "materialised", 3),
        (f"{job.job_id}_1", "materialised", 7),
    ]
    assert rows(backend.sample(refs["output-0"], 10))["id"].to_list() == [7, 8, 9]
    assert rows(backend.sample(refs["output-1"], 10))["id"].to_list() == list(range(7))
    assert [backend.count(ref) for ref in refs.values()] == [3, 7]
    backend.release(refs["output-0"])
    assert (backend.exists(refs["output-0"]), backend.exists(refs["output-1"])) == (False, True)


def test_a_model_is_trained_held_as_a_blob_and_applied_by_reference(backend, kept, tmp_path):
    """A two-feature fit of ``y = 2·x1 + 3·x2 + 1`` scores its own rows, so a column that is no feature rides along."""
    x1 = [float(i) for i in range(8)]
    x2 = [float(i % 3) for i in range(8)]
    y = [2 * a + 3 * b + 1 for a, b in zip(x1, x2, strict=True)]
    training_rows = pl.LazyFrame({"x1": x1, "x2": x2, "y": y})
    train = replace(ml_train_payload(), plan=training_rows, feature_columns=["x1", "x2"])

    trained = backend.run_op(OperationKind.ml_train, op_payload(train))

    model = trained.refs["model"]
    kept.append(model)
    assert list(trained.refs) == ["model"]
    assert (model.backend_id, model.result_id, model.kind, model.state, model.schema, model.row_count) == (
        backend.id,
        f"{train.job_id}_0",
        "blob",
        "materialised",
        [],
        None,
    )
    assert location(model).kind == "blob"
    target = tmp_path / "model.json"
    assert backend.export(model, file_uri(target), None) == file_uri(target)
    exported = target.read_bytes()
    assert trained.metadata == {
        "sha256": hashlib.sha256(exported).hexdigest(),
        "size_bytes": len(exported),
        "model_type": "linear_regression",
    }
    for read in (backend.plan_for, backend.count, lambda blob: backend.sample(blob, 1)):
        with pytest.raises(OperationUnsupported):
            read(model)

    apply = replace(ml_apply_payload(model), plan=training_rows)
    scored_refs = backend.run_op(OperationKind.ml_apply, op_payload(apply)).refs

    scored = scored_refs["result"]
    kept.append(scored)
    assert list(scored_refs) == ["result"]
    assert (scored.result_id, scored.kind, scored.state, scored.row_count) == (
        f"{apply.job_id}_0",
        "table",
        "materialised",
        8,
    )
    assert columns(scored) == ["x1", "x2", "y", "pred"]
    assert rows(backend.sample(scored, 10))["pred"].round(6).to_list() == y
    expected = get_trainer("linear_regression").apply(training_rows, json.loads(exported), "pred").collect()
    assert backend.plan_for(scored).collect().equals(expected)
    assert backend.count(scored) == 8
    assert backend.exists(model) is True
    backend.release(model)
    assert backend.exists(model) is False
    assert not Path(location(model).path).exists()
    with pytest.raises(ResultMissing, match=model.result_id):
        backend.run_op(OperationKind.ml_apply, op_payload(ml_apply_payload(model)))
    foreign = ResultRef("worker", "elsewhere", [], None, "materialised", b"/elsewhere.blob", kind="blob")
    with pytest.raises(ForeignRef, match="elsewhere"):
        backend.run_op(OperationKind.ml_apply, op_payload(ml_apply_payload(foreign)))


def test_polars_code_holds_a_lazy_result_deferred_and_an_eager_one_as_a_file(backend, kept, tmp_path):
    source = tmp_path / "people.arrow"
    PEOPLE.write_ipc(source)
    scan = pl.scan_ipc(source)
    lazy = PolarsCodePayload(
        code="output_df = input_df.filter(pl.col('id') > 7).with_columns(double=pl.col('id') * 2)",
        inputs=(scan,),
        flow_id=1,
        node_id=1,
        job_id=new_id(),
    )
    eager = PolarsCodePayload(
        code="output_df = input_df.collect().head(2)", inputs=(scan,), flow_id=1, node_id=1, job_id=new_id()
    )

    refs = backend.run_op(OperationKind.polars_code, op_payload(lazy)).refs
    deferred = refs["result"]
    stored = backend.run_op(OperationKind.polars_code, op_payload(eager)).refs["result"]

    kept.extend([deferred, stored])
    assert list(refs) == ["result"]
    assert (deferred.result_id, deferred.state, location(deferred).kind) == (f"{lazy.job_id}_0", "deferred", "plan")
    assert dtypes(deferred) == [("id", "Int64"), ("name", "String"), ("score", "Float64"), ("double", "Int64")]
    assert rows(backend.sample(deferred, 10))["double"].to_list() == [16, 18]
    assert backend.count(deferred) == 2
    assert (stored.result_id, stored.state, stored.row_count, location(stored).kind) == (
        f"{eager.job_id}_0",
        "materialised",
        2,
        "file",
    )
    assert rows(backend.sample(stored, 10))["id"].to_list() == [0, 1]


def test_a_sql_query_holds_its_result_over_the_input_scan_and_an_unsafe_statement_fails_its_job(
    backend, kept, tmp_path
):
    source = tmp_path / "people.arrow"
    PEOPLE.write_ipc(source)
    query = SqlQueryPayload(
        sql_code="SELECT id, name, id * 2 AS double FROM input_1 WHERE id > 7",
        inputs=(pl.scan_ipc(source),),
        flow_id=1,
        node_id=1,
        job_id=new_id(),
    )

    refs = backend.run_op(OperationKind.sql_query, op_payload(query)).refs

    ref = refs["result"]
    kept.append(ref)
    assert list(refs) == ["result"]
    assert (ref.result_id, ref.state, location(ref).kind) == (f"{query.job_id}_0", "deferred", "plan")
    assert dtypes(ref) == [("id", "Int64"), ("name", "String"), ("double", "Int64")]
    assert rows(backend.sample(ref, 10))["double"].to_list() == [16, 18]
    assert backend.count(ref) == 2
    unsafe = SqlQueryPayload(sql_code="DROP TABLE input_1", inputs=(pl.scan_ipc(source),), flow_id=1, node_id=1)
    with pytest.raises(TaskFailed, match="UnsafeSQLError"):
        backend.run_op(OperationKind.sql_query, op_payload(unsafe))


def test_a_parquet_is_imported_counted_without_a_job_sampled_through_one_and_exported(backend, tmp_path, inline_jobs):
    """The file is written only after the import, so an import that read it would fail."""
    source = tmp_path / "in.parquet"
    schema = worker_module._flowfile_schema(PEOPLE.schema)
    target = tmp_path / "out.parquet"

    ref = backend.import_uri(file_uri(source), schema, PEOPLE.height, None)

    try:
        assert (ref.backend_id, ref.state, ref.row_count, location(ref).kind) == (backend.id, "deferred", 10, "plan")
        assert columns(ref) == ["id", "name", "score"]
        assert backend.exists(ref)
        assert backend.count(ref) == 10
        assert inline_jobs == []
        PEOPLE.write_parquet(source)
        assert rows(backend.sample(ref, 2)).equals(PEOPLE.head(2))
        assert len(inline_jobs) == 1
        assert isinstance(backend.plan_for(ref), pl.LazyFrame)
        assert backend.export(ref, file_uri(target), None) == file_uri(target)
        assert pl.read_parquet(target).equals(PEOPLE)
        with pytest.raises(ValueError, match="needs its schema"):
            backend.import_uri(file_uri(source), None, PEOPLE.height, None)
    finally:
        backend.release(ref)
    assert backend.exists(ref) is False
    assert source.is_file()


def test_a_blob_is_held_as_a_copy_in_the_cache_directory_and_refused_where_rows_are_read(
    backend, blob_source, tmp_path
):
    """What the conformance suite's blob leaves out: where the copy is held and every refusal."""
    refused = tmp_path / "refused.bin"

    ref = backend.import_uri(file_uri(blob_source), None, None, None, kind="blob")

    copy = Path(location(ref).path)
    try:
        assert location(ref).kind == "blob"
        assert copy.is_relative_to(storage.cache_directory)
        assert copy.read_bytes() == BLOB
        with pytest.raises(OperationUnsupported, match="is a blob"):
            submit(backend, [], inputs=[ref])
        for read in (backend.plan_for, backend.count, lambda blob: backend.sample(blob, 1)):
            with pytest.raises(OperationUnsupported, match="is a blob"):
                read(ref)
        with pytest.raises(ValueError, match="takes no credential"):
            backend.export(ref, file_uri(refused), CredentialRef(connection_id=1, user_id=1))
        assert not refused.exists()
    finally:
        backend.release(ref)
    assert not copy.exists()
    with pytest.raises(ResultMissing):
        backend.export(ref, file_uri(refused), None)
    assert not refused.exists()


@pytest.mark.parametrize(
    "given",
    [{"schema": []}, {"row_count": 0}, {"credential": CredentialRef(connection_id=7, user_id=3)}],
    ids=["schema", "row_count", "credential"],
)
def test_a_blob_import_takes_no_schema_row_count_or_credential(backend, blob_source, given):
    arguments = {"schema": None, "row_count": None, "credential": None, **given}

    with pytest.raises(ValueError, match="A blob import takes no"):
        backend.import_uri(file_uri(blob_source), **arguments, kind="blob")


def test_a_blob_uri_of_another_scheme_is_refused_by_the_body(backend, blob_source):
    with pytest.raises(PlanRejected, match="s3"):
        backend.import_uri("s3://bucket/model.bin", None, None, None, kind="blob")
    blob = backend.import_uri(file_uri(blob_source), None, None, None, kind="blob")
    try:
        with pytest.raises(PlanRejected, match="s3"):
            backend.export(blob, "s3://bucket/model.bin", None)
    finally:
        backend.release(blob)


def test_a_secret_core_encrypted_decrypts_through_the_resolver_the_worker_registers(backend, kept):
    submit(backend, kept)
    credentials = {"aws_access_key_id": "key", "aws_secret_access_key": "secret"}
    ciphertext = encrypt_secret(json.dumps(credentials), user_id=1)

    assert EncryptedCredentialProvider(ciphertext)() == (credentials, None)
