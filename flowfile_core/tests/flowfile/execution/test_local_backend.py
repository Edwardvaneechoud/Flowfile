"""What ``local`` is in a library host: the worker embedded in this process, every job run here.

The registry makes it without a registration, as an ``EmbeddedWorkerBackend`` under the id ``local``, and its factory
sets the worker's in-process mode, so a submit, a read, a write, a Delta operation, a visualisation request and a
custom node whose class has no node file all run in this process: no child is started, no pool member leased and no
port opened or dialled. The results are the worker's, held by its registry under core's storage directory: a deferred
submit as its plan, a required one as a file, a blob as a copy of its bytes, each read back through ``plan_for``,
``sample`` and ``count``, exported to a URI and imported from one. The seven kinds the worker serves on its task routes
run through the worker's own job runner and session registry. Both flag states run the same cases: the embedded
worker reads the flag nowhere.
"""

import datetime
import hashlib
import io
import json
import os
import socket
import sqlite3
import uuid
from pathlib import Path

import polars as pl
import pytest
import websockets.sync.client

from flowfile_core.configs import settings
from flowfile_core.flowfile.execution import registry
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
    CloudWritePayload,
    CustomNodePayload,
    DatabaseWritePayload,
    DeltaMergePayload,
    DeltaScd2Payload,
    DeltaWritePayload,
    FileReadPayload,
    FileWritePayload,
    FuzzyJoinPayload,
    MlApplyPayload,
    MlTrainPayload,
    PolarsCodePayload,
    RandomSplitPayload,
    SqlQueryPayload,
    VizSessionPayload,
    op_payload,
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
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import subprocess_operations as subops
from flowfile_core.flowfile.node_designer import CustomNodeBase
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.cloud_storage_schemas import CloudStorageWriteSettings
from flowfile_worker import mp_context, pool, spawner
from flowfile_worker.service import OPERATIONS
from shared.compute_protocol import PROTOCOL_VERSION, WorkerLocation, file_uri, local_capabilities
from shared.delta_utils import Scd2Result, get_delta_size_bytes
from shared.ml.trainers import get_trainer
from shared.storage_config import storage
from tests.flowfile.execution.test_write_payloads import SECRET, USER_ID, s3_connection

UTC = datetime.timezone.utc
SCD2_TABLE_SCHEMA = [
    {"name": "id", "dtype": "Int64"},
    {"name": "val", "dtype": "String"},
    {"name": "qty", "dtype": "Int64"},
    {"name": "sk", "dtype": "String"},
    {"name": "valid_from", "dtype": "Datetime(time_unit='us', time_zone='UTC')"},
    {"name": "valid_to", "dtype": "Datetime(time_unit='us', time_zone='UTC')"},
    {"name": "is_current", "dtype": "Boolean"},
]
BLOB = bytes(range(256)) * 64


class Splitter(CustomNodeBase):
    """A class with no node file: splits ``n`` at 89, the small side computed."""

    node_name: str = "Local Backend Splitter"
    number_of_outputs: int = 2
    output_names: list[str] = ["big", "small"]

    def process(self, *inputs: pl.LazyFrame):
        rows = inputs[0]
        return {"big": rows.filter(pl.col("n") > 89), "small": rows.filter(pl.col("n") <= 89).collect()}


class Doubler(CustomNodeBase):
    node_name: str = "Local Backend Doubler"

    def process(self, *inputs: pl.LazyFrame):
        return inputs[0].with_columns(pl.col("n") * 2)


class Silent(CustomNodeBase):
    node_name: str = "Local Backend Silent"

    def process(self, *inputs: pl.LazyFrame):
        return None


def hints(*, must_materialise: bool = False, preview_rows: int | None = None) -> SubmitHints:
    return SubmitHints(must_materialise=must_materialise, wide=False, fanout=1, preview_rows=preview_rows)


def new_id() -> str:
    return f"local-{uuid.uuid4().hex}"


def submit(backend, plan: pl.LazyFrame, inputs=(), submit_hints=None, result_id: str | None = None):
    return backend.submit(plan, list(inputs), submit_hints or hints(), result_id or new_id(), flow_id=1, node_id=1)


def rows(sample: bytes) -> pl.DataFrame:
    return pl.read_ipc_stream(sample)


def location(ref: ResultRef) -> WorkerLocation:
    return WorkerLocation.model_validate_json(ref.location)


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
    assert _worker_port_refused() == port_refused_before, "the case opened or closed the worker port"


@pytest.fixture
def fresh_registry():
    registry.reset()
    yield
    registry.reset()


@pytest.fixture
def backend(fresh_registry, monkeypatch) -> ComputeBackend:
    """The registry's ``local``, made by its factory, which sets the worker's in-process mode itself."""
    monkeypatch.setattr(spawner, "in_process", False)
    made = registry.backend(registry.LOCAL_BACKEND_ID)
    assert spawner.in_process is True
    return made


@pytest.fixture
def kept(backend):
    """Results a case made, released when it ends."""
    refs: list[ResultRef] = []
    yield refs
    for ref in refs:
        backend.release(ref)


@pytest.fixture
def ten_rows() -> pl.LazyFrame:
    return pl.LazyFrame({"n": list(range(10)), "label": [f"row {i}" for i in range(10)]})


@pytest.fixture
def foreign_ref() -> ResultRef:
    return ResultRef(backend_id="worker", result_id="abc", schema=[], row_count=None, state="deferred", location=b"abc")


# What local is


def test_the_registry_makes_the_embedded_worker_without_a_registration(fresh_registry):
    made = registry.backend(registry.LOCAL_BACKEND_ID)

    assert isinstance(made, EmbeddedWorkerBackend)
    assert registry.backend(registry.LOCAL_BACKEND_ID) is made


def test_a_registration_under_the_local_id_wins_over_the_built_in(fresh_registry):
    replacement = object()
    registry.register_backend(registry.LOCAL_BACKEND_ID, lambda: replacement)

    assert registry.backend(registry.LOCAL_BACKEND_ID) is replacement


def test_local_is_the_embedded_worker_declaring_what_this_process_runs(backend, monkeypatch):
    monkeypatch.delenv("FLOWFILE_WORKER_LOCAL_FILES", raising=False)

    assert isinstance(backend, ComputeBackend)
    assert (backend.id, backend.protocol_version) == ("local", PROTOCOL_VERSION)
    capabilities = backend.capabilities()
    undeclared = {"operations": frozenset(), "read_schemes": frozenset(), "write_schemes": frozenset()}
    assert capabilities.model_copy(update=undeclared) == local_capabilities()
    assert capabilities.operations == OPERATIONS == frozenset(kind.value for kind in OperationKind)
    assert capabilities.read_schemes == {"file", "s3", "az", "gs", "http", "https"}
    assert capabilities.write_schemes == {"file", "s3", "az", "gs", "https"}
    assert capabilities.credential_resolution == "master_key"


# Results, held by the worker in this process


def test_a_deferred_submit_is_held_as_its_plan_and_read_back(backend, kept, ten_rows):
    submitted = submit(backend, ten_rows, result_id="hash-1")
    ref = submitted.ref
    kept.append(ref)

    assert (ref.backend_id, ref.result_id, ref.state, ref.row_count) == ("local", "hash-1", "deferred", None)
    assert location(ref).kind == "plan"
    assert [(c.column_name, c.data_type) for c in ref.schema] == [("n", "Int64"), ("label", "String")]
    assert submitted.handle.job_id == "hash-1"
    assert backend.status(submitted.handle).state == "done"
    assert rows(backend.sample(ref, 3)).height == 3
    assert rows(backend.sample(ref, 50)).height == 10
    assert backend.count(ref) == 10
    assert backend.exists(ref)
    assert backend.plan_for(ref).collect()["n"].to_list() == list(range(10))


@pytest.mark.parametrize("preview_rows", [None, 4])
def test_a_required_submit_is_a_file_under_the_cache_directory_and_the_sample_comes_inline(
    backend, kept, ten_rows, preview_rows
):
    submitted = submit(backend, ten_rows, submit_hints=hints(must_materialise=True, preview_rows=preview_rows))
    ref = submitted.ref
    kept.append(ref)

    held = location(ref)
    assert (ref.state, held.kind) == ("materialised", "file")
    assert held.path.startswith(str(storage.cache_directory))
    if preview_rows is None:
        assert submitted.sample is None
    else:
        assert rows(submitted.sample)["n"].to_list() == [0, 1, 2, 3]
    # Polars prints a Windows path with forward slashes.
    assert Path(held.path).as_posix() in backend.plan_for(ref).explain()
    assert backend.count(ref) == 10


def test_submitting_under_a_held_id_replaces_that_result(backend, kept, ten_rows):
    three_rows = pl.LazyFrame({"n": [1, 2, 3]})
    first = submit(backend, ten_rows, result_id="same").ref

    second = submit(backend, three_rows, result_id="same").ref
    kept.append(second)

    assert backend.exists(first) and backend.exists(second)
    assert backend.plan_for(second).collect()["n"].to_list() == [1, 2, 3]


def test_a_released_result_is_gone(backend, ten_rows):
    ref = submit(backend, ten_rows).ref

    backend.release(ref)

    assert not backend.exists(ref)
    with pytest.raises(ResultMissing):
        backend.sample(ref, 1)
    with pytest.raises(ResultMissing):
        backend.count(ref)
    with pytest.raises(ResultMissing):
        submit(backend, ten_rows, [ref])
    backend.release(ref)


def test_a_ref_from_another_backend_is_refused(backend, ten_rows, foreign_ref):
    for read in (backend.plan_for, backend.exists, backend.release, backend.count):
        with pytest.raises(ForeignRef):
            read(foreign_ref)
    with pytest.raises(ForeignRef):
        backend.sample(foreign_ref, 1)
    with pytest.raises(ForeignRef):
        submit(backend, ten_rows, [foreign_ref])


def test_a_plan_that_fails_on_its_data_fails_its_submit_and_holds_nothing(backend):
    failing = pl.LazyFrame({"a": ["x"]}).select(pl.col("a").cast(pl.Int64, strict=True))
    result_id = new_id()

    with pytest.raises(TaskFailed, match="InvalidOperationError"):
        submit(backend, failing, submit_hints=hints(must_materialise=True, preview_rows=5), result_id=result_id)

    ref = ResultRef("local", result_id, [], None, "deferred", b"")
    assert backend.exists(ref) is False


@pytest.mark.parametrize("must_materialise", [False, True], ids=["deferred", "required"])
def test_export_writes_the_rows_to_the_uri(backend, kept, ten_rows, foreign_ref, must_materialise, tmp_path):
    ref = submit(backend, ten_rows, submit_hints=hints(must_materialise=must_materialise)).ref
    kept.append(ref)
    target = file_uri(tmp_path / "out.parquet")

    assert backend.export(ref, target, None) == target

    assert pl.read_parquet(tmp_path / "out.parquet").equals(ten_rows.collect())
    with pytest.raises(ForeignRef):
        backend.export(foreign_ref, file_uri(tmp_path / "foreign.parquet"), None)
    released = submit(backend, ten_rows).ref
    backend.release(released)
    with pytest.raises(ResultMissing):
        backend.export(released, file_uri(tmp_path / "released.parquet"), None)
    assert not (tmp_path / "foreign.parquet").exists()
    assert not (tmp_path / "released.parquet").exists()


def test_import_uri_holds_a_scan_with_the_given_schema_and_count_and_reads_nothing(backend, kept, ten_rows, tmp_path):
    """The file is written only after the import, so an import that read it would fail."""
    path = tmp_path / "in.parquet"
    schema = submit(backend, ten_rows).ref.schema

    ref = backend.import_uri(file_uri(path), schema, 10, None)

    assert (ref.backend_id, ref.state, ref.row_count) == ("local", "deferred", 10)
    assert [(c.column_name, c.data_type) for c in ref.schema] == [("n", "Int64"), ("label", "String")]
    assert backend.exists(ref)
    assert backend.count(ref) == 10

    ten_rows.collect().write_parquet(path)

    assert rows(backend.sample(ref, 3)).equals(ten_rows.head(3).collect())
    assert isinstance(backend.plan_for(ref), pl.LazyFrame)
    backend.release(ref)
    assert not backend.exists(ref)
    assert path.is_file()


def test_a_table_import_needs_its_schema(backend, tmp_path):
    with pytest.raises(ValueError, match="needs its schema"):
        backend.import_uri(file_uri(tmp_path / "in.parquet"), None, 10, None)


@pytest.fixture
def blob_source(tmp_path):
    source = tmp_path / "model.bin"
    source.write_bytes(BLOB)
    return source


def test_a_blob_is_copied_into_the_cache_directory_and_out_again_and_never_read_as_rows(
    backend, blob_source, ten_rows, tmp_path
):
    ref = backend.import_uri(file_uri(blob_source), None, None, None, kind="blob")

    held = location(ref)
    assert (ref.kind, ref.state, ref.schema, ref.row_count, held.kind) == ("blob", "materialised", [], None, "blob")
    assert held.path.startswith(str(storage.cache_directory))
    assert open(held.path, "rb").read() == BLOB
    assert backend.exists(ref)
    with pytest.raises(OperationUnsupported, match="is a blob"):
        submit(backend, ten_rows, [ref])
    for read in (backend.plan_for, backend.count, lambda blob: backend.sample(blob, 1)):
        with pytest.raises(OperationUnsupported, match="is a blob"):
            read(ref)
    target = file_uri(tmp_path / "copy.bin")
    assert backend.export(ref, target, None) == target
    assert (tmp_path / "copy.bin").read_bytes() == BLOB
    with pytest.raises(ValueError, match="takes no credential"):
        backend.export(ref, file_uri(tmp_path / "out.bin"), CredentialRef(connection_id=7, user_id=3))
    backend.release(ref)
    assert not backend.exists(ref)
    assert not (tmp_path / "out.bin").exists()
    with pytest.raises(ResultMissing):
        backend.export(ref, file_uri(tmp_path / "released.bin"), None)


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


# The operations, run as the worker's jobs in this process


def test_random_split_holds_one_result_per_split(backend, kept):
    node = input_schema.NodeRandomSplit(flow_id=1, node_id=1, seed=7)
    split = RandomSplitPayload(plan=pl.LazyFrame({"n": list(range(100))}), settings=node, flow_id=1, node_id=1)

    refs = backend.run_op(OperationKind.random_split, op_payload(split)).refs

    kept.extend(refs.values())
    assert list(refs) == ["train", "test"]
    assert [(ref.state, ref.row_count) for ref in refs.values()] == [("materialised", 80), ("materialised", 20)]
    assert [backend.count(ref) for ref in refs.values()] == [80, 20]
    held = pl.concat([backend.plan_for(ref).collect() for ref in refs.values()])
    assert sorted(held["n"].to_list()) == list(range(100))


def numbers_scan(tmp_path) -> pl.LazyFrame:
    """Five rows scanned from a file, so a lazy result over them is held as its plan, as over any source."""
    pl.DataFrame({"n": list(range(5))}).write_ipc(tmp_path / "numbers.arrow")
    return pl.scan_ipc(tmp_path / "numbers.arrow")


def test_polars_code_holds_a_lazy_result_deferred_and_an_eager_one_as_a_file(backend, kept, tmp_path):
    numbers = numbers_scan(tmp_path)
    code = "output_df = input_df.filter(pl.col('n') > 2).with_columns(double=pl.col('n') * 2)"
    lazy = PolarsCodePayload(code=code, inputs=(numbers,), flow_id=1, node_id=1)
    eager = PolarsCodePayload(code="output_df = input_df.collect().head(2)", inputs=(numbers,), flow_id=1, node_id=1)

    deferred = backend.run_op(OperationKind.polars_code, op_payload(lazy)).refs
    stored = backend.run_op(OperationKind.polars_code, op_payload(eager)).refs["result"]

    kept.extend([*deferred.values(), stored])
    ref = deferred["result"]
    assert deferred == {"result": ref}
    assert (ref.state, location(ref).kind) == ("deferred", "plan")
    assert [(column.column_name, column.data_type) for column in ref.schema] == [("n", "Int64"), ("double", "Int64")]
    assert backend.plan_for(ref).collect().to_dicts() == [{"n": 3, "double": 6}, {"n": 4, "double": 8}]
    assert (stored.state, stored.row_count) == ("materialised", 2)


def test_sql_query_holds_the_querys_result_and_an_unsafe_statement_fails_its_job(backend, kept, tmp_path):
    numbers = numbers_scan(tmp_path)
    query = SqlQueryPayload(
        sql_code="SELECT n, n * 2 AS double FROM input_1 WHERE n > 2", inputs=(numbers,), flow_id=1, node_id=1
    )

    refs = backend.run_op(OperationKind.sql_query, op_payload(query)).refs

    kept.extend(refs.values())
    ref = refs["result"]
    assert refs == {"result": ref}
    assert ref.state == "deferred"
    assert [(column.column_name, column.data_type) for column in ref.schema] == [("n", "Int64"), ("double", "Int64")]
    assert backend.plan_for(ref).collect().to_dicts() == [{"n": 3, "double": 6}, {"n": 4, "double": 8}]
    unsafe = SqlQueryPayload(sql_code="DROP TABLE input_1", inputs=(numbers,), flow_id=1, node_id=1)
    with pytest.raises(TaskFailed, match="UnsafeSQLError"):
        backend.run_op(OperationKind.sql_query, op_payload(unsafe))


def test_file_read_holds_the_rows_of_a_csv(backend, kept, tmp_path):
    path = tmp_path / "people.csv"
    path.write_text("name,age\nada,36\ngrace,85\n")
    received = input_schema.ReceivedTable(file_type="csv", name=path.name, path=str(path))
    node = input_schema.NodeRead(flow_id=1, node_id=1, cache_data=False, received_file=received)
    read = FileReadPayload(flow_id=1, node_id=1, settings=node, node_logger=None)

    refs = backend.run_op(OperationKind.file_read, op_payload(read)).refs

    kept.extend(refs.values())
    assert list(refs) == ["result"]
    assert (refs["result"].state, refs["result"].row_count) == ("materialised", 2)
    assert backend.plan_for(refs["result"]).collect().to_dicts() == [
        {"name": "ada", "age": 36},
        {"name": "grace", "age": 85},
    ]
    assert [c.column_name for c in refs["result"].schema] == ["name", "age"]


def test_fuzzy_join_holds_the_matches(backend, kept):
    names = pl.LazyFrame({"name": ["eduward", "edward", "courtney"]})
    node = input_schema.NodeFuzzyMatch(
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
    join = FuzzyJoinPayload(left=names, right=names, settings=node, flow_id=1, node_id=2)

    refs = backend.run_op(OperationKind.fuzzy_join, op_payload(join)).refs

    kept.extend(refs.values())
    assert (refs["result"].state, refs["result"].row_count) == ("materialised", 5)
    matches = backend.plan_for(refs["result"]).collect().sort("name", "name_right")
    assert matches.select("name", "name_right").rows() == [
        ("courtney", "courtney"),
        ("eduward", "eduward"),
        ("eduward", "edward"),
        ("edward", "eduward"),
        ("edward", "edward"),
    ]


@pytest.fixture
def training_rows() -> pl.LazyFrame:
    """Eight rows of ``y = 2·x1 + 3·x2 + 1``."""
    x1 = [float(i) for i in range(8)]
    x2 = [float(i % 3) for i in range(8)]
    return pl.LazyFrame({"x1": x1, "x2": x2, "y": [2 * a + 3 * b + 1 for a, b in zip(x1, x2, strict=True)]})


def training(plan: pl.LazyFrame) -> MlTrainPayload:
    return MlTrainPayload(
        plan=plan,
        model_type="linear_regression",
        target_column="y",
        feature_columns=["x1", "x2"],
        params={"add_bias": True},
        flow_id=1,
        node_id=1,
    )


def scoring(plan: pl.LazyFrame, model: ResultRef) -> MlApplyPayload:
    return MlApplyPayload(plan=plan, model=model, output_column="pred", flow_id=1, node_id=2)


def test_a_training_holds_the_model_as_a_blob_with_its_digest(backend, training_rows, tmp_path):
    train = training(training_rows)

    trained = backend.run_op(OperationKind.ml_train, op_payload(train))

    assert list(trained.refs) == ["model"]
    model = trained.refs["model"]
    assert (model.backend_id, model.result_id, model.kind, model.state, model.schema, model.row_count) == (
        "local",
        f"{train.job_id}_0",
        "blob",
        "materialised",
        [],
        None,
    )
    assert backend.exists(model)
    held = location(model).path
    target = tmp_path / "model.json"
    assert backend.export(model, file_uri(target), None) == file_uri(target)
    exported = target.read_bytes()
    assert trained.metadata == {
        "sha256": hashlib.sha256(exported).hexdigest(),
        "size_bytes": len(exported),
        "model_type": "linear_regression",
    }
    backend.release(model)
    assert not backend.exists(model)
    assert not os.path.exists(held)


def test_an_apply_scores_with_a_held_model_and_holds_the_rows(backend, kept, training_rows, tmp_path):
    model = backend.run_op(OperationKind.ml_train, op_payload(training(training_rows))).refs["model"]
    kept.append(model)
    backend.export(model, file_uri(tmp_path / "model.json"), None)
    fitted = json.loads((tmp_path / "model.json").read_bytes())

    scored = backend.run_op(OperationKind.ml_apply, op_payload(scoring(training_rows, model)))

    assert list(scored.refs) == ["result"]
    result = scored.refs["result"]
    kept.append(result)
    assert (result.kind, result.state, result.row_count) == ("table", "materialised", 8)
    assert [c.column_name for c in result.schema] == ["x1", "x2", "y", "pred"]
    expected = get_trainer("linear_regression").apply(training_rows, fitted, "pred").collect()
    assert backend.plan_for(result).collect().equals(expected)
    assert backend.exists(model)


def test_an_apply_with_a_model_this_backend_does_not_hold_is_refused(backend, training_rows):
    foreign = ResultRef("worker", "abc_0", [], None, "materialised", b"abc_0", kind="blob")
    released = backend.run_op(OperationKind.ml_train, op_payload(training(training_rows))).refs["model"]
    backend.release(released)

    for model, error in ((foreign, ForeignRef), (released, ResultMissing)):
        with pytest.raises(error):
            backend.run_op(OperationKind.ml_apply, op_payload(scoring(training_rows, model)))


# The seven kinds the worker serves on its task routes, run in this process


def test_a_file_write_holds_no_result_and_reports_no_metadata(backend, tmp_path):
    output = input_schema.OutputSettings(
        name="people.csv",
        directory=str(tmp_path),
        file_type="csv",
        write_mode="overwrite",
        table_settings={"file_type": "csv", "delimiter": ";", "encoding": "utf-8"},
    )
    write = FileWritePayload(plan=pl.LazyFrame({"name": ["ada", "grace"]}), settings=output, flow_id=1, node_id=2)

    result = backend.run_op(OperationKind.file_write, op_payload(write))

    assert result == OpResult(refs={})
    assert result.metadata == {}
    assert pl.read_csv(tmp_path / "people.csv", separator=";")["name"].to_list() == ["ada", "grace"]
    assert backend.status(JobHandle("local", write.job_id)).state == "done"


def test_a_file_write_that_fails_is_a_task_failure_naming_the_error(backend, tmp_path):
    output = input_schema.OutputSettings(
        name="people.csv", directory=str(tmp_path), file_type="csv", write_mode="create", table_settings={}
    )
    (tmp_path / "people.csv").write_text("taken\n")
    write = FileWritePayload(plan=pl.LazyFrame({"name": ["ada"]}), settings=output, flow_id=1, node_id=2)

    with pytest.raises(TaskFailed, match="File already exists"):
        backend.run_op(OperationKind.file_write, op_payload(write))


def test_a_database_write_writes_a_sqlite_table(backend, tmp_path):
    database = tmp_path / "people.db"
    connection = input_schema.DatabaseConnection(database_type="sqlite", database=f"sqlite:///{database}")
    node = input_schema.NodeDatabaseWriter(
        flow_id=1,
        node_id=2,
        user_id=USER_ID,
        database_write_settings=input_schema.DatabaseWriteSettings(
            database_connection=connection, table_name="people", if_exists="replace"
        ),
    )
    people = pl.DataFrame({"id": [1, 2, 3], "name": ["ada", "grace", "edsger"]})
    write = DatabaseWritePayload(
        plan=people.lazy(),
        settings=node,
        connection=connection,
        encrypted_password=None,
        table_name="people",
        flow_id=1,
        node_id=2,
    )

    result = backend.run_op(OperationKind.database_write, op_payload(write))

    assert result == OpResult(refs={})
    with sqlite3.connect(database) as db:
        assert db.execute("SELECT id, name FROM people ORDER BY id").fetchall() == list(people.iter_rows())


def test_a_cloud_write_hands_the_workers_writer_the_connection_with_its_secrets_decrypted_there(backend, monkeypatch):
    received = []
    monkeypatch.setattr(
        "flowfile_worker.external_sources.s3_source.main.write_df_to_cloud",
        lambda df, settings, logger: received.append((df, settings)),
    )
    write = CloudWritePayload(
        plan=pl.LazyFrame({"id": [1]}),
        settings=CloudStorageWriteSettings(resource_path="s3://bucket/people.parquet", file_format="parquet"),
        connection=s3_connection().get_worker_interface(USER_ID),
        flow_id=7,
        node_id=3,
    )

    result = backend.run_op(OperationKind.cloud_write, op_payload(write))

    assert result == OpResult(refs={})
    ((df, sent),) = received
    assert df.collect().to_dicts() == [{"id": 1}]
    assert sent.write_settings.resource_path == "s3://bucket/people.parquet"
    assert sent.connection.aws_access_key_id == "key-id"
    assert sent.connection.aws_secret_access_key.get_secret_value().startswith("$ffsec$")
    assert sent.connection.get_storage_options()["aws_secret_access_key"] == SECRET
    assert (sent.flowfile_flow_id, sent.flowfile_node_id) == (7, 3)


def scd2_write(path: str, plan: pl.LazyFrame, run_timestamp: str) -> DeltaScd2Payload:
    return DeltaScd2Payload(
        plan=plan,
        path=path,
        business_keys=["id"],
        compare_columns=["val"],
        full_snapshot=False,
        surrogate_key_column="sk",
        valid_from_column="valid_from",
        valid_to_column="valid_to",
        is_current_column="is_current",
        run_timestamp=run_timestamp,
        partition_by=[],
        partition_on_current=True,
        connection=None,
        flow_id=1,
        node_id=2,
    )


def test_an_scd2_write_loads_a_table_then_versions_the_rows_that_changed(backend, tmp_path):
    path = str(tmp_path / "dimension")
    first_batch = pl.LazyFrame({"id": [1, 2, 3], "val": ["a", "b", "c"], "qty": [10, 20, 30]})
    second_batch = pl.LazyFrame({"id": [1, 2, 4], "val": ["A", "b", "d"], "qty": [11, 20, 40]})

    first = backend.run_op(
        OperationKind.delta_scd2, op_payload(scd2_write(path, first_batch, "2024-01-01T00:00:00+00:00"))
    )
    first_size = get_delta_size_bytes(path)
    second = backend.run_op(
        OperationKind.delta_scd2, op_payload(scd2_write(path, second_batch, "2024-02-01T00:00:00+00:00"))
    )
    second_size = get_delta_size_bytes(path)

    assert first == OpResult(
        refs={},
        metadata={
            "table_path": path,
            "schema": SCD2_TABLE_SCHEMA,
            "row_count": 3,
            "column_count": 7,
            "size_bytes": first_size,
            "version": 0,
            "scd2_metrics": {"rows_inserted": 3, "rows_closed": 0, "rows_total": 3, "rows_current": 3, "created": True},
        },
    )
    assert second == OpResult(
        refs={},
        metadata={
            "table_path": path,
            "schema": SCD2_TABLE_SCHEMA,
            "row_count": 5,
            "column_count": 7,
            "size_bytes": second_size,
            "version": 1,
            "scd2_metrics": {
                "rows_inserted": 2,
                "rows_closed": 1,
                "rows_total": 5,
                "rows_current": 4,
                "created": False,
            },
        },
    )
    jan, feb = datetime.datetime(2024, 1, 1, tzinfo=UTC), datetime.datetime(2024, 2, 1, tzinfo=UTC)
    assert pl.read_delta(path).sort("id", "valid_from").rows() == [
        (1, "a", 10, "086436db5d6237d8f53e55ebf5bafe19", jan, feb, False),
        (1, "A", 11, "8a33fe6e8900cddb5e995de3f3912ec3", feb, None, True),
        (2, "b", 20, "a4d2b5f0652fef0de72c82802a95ade3", jan, None, True),
        (3, "c", 30, "ec4f31d51c39fec0ce47aecc38b5bf5c", jan, None, True),
        (4, "d", 40, "b45a6c225243cc60905eb98c9803620e", feb, None, True),
    ]


def test_an_scd2_write_that_changes_nothing_commits_nothing_and_names_its_version(backend, tmp_path):
    path = str(tmp_path / "dimension")
    batch = pl.LazyFrame({"id": [1, 2], "val": ["a", "b"], "qty": [10, 20]})
    backend.run_op(OperationKind.delta_scd2, op_payload(scd2_write(path, batch, "2024-01-01T00:00:00+00:00")))

    again = backend.run_op(OperationKind.delta_scd2, op_payload(scd2_write(path, batch, "2024-02-01T00:00:00+00:00")))

    assert again == OpResult(refs={}, metadata={"skipped": True, "version": 0})
    assert pl.read_delta(path).height == 2


def delta_write(path: str, plan: pl.LazyFrame, mode: str, connection=None) -> DeltaWritePayload:
    return DeltaWritePayload(
        plan=plan, path=path, mode=mode, partition_by=[], enable_cdf=False, connection=connection, flow_id=1, node_id=2
    )


def delta_merge(path: str, plan: pl.LazyFrame) -> DeltaMergePayload:
    return DeltaMergePayload(
        plan=plan,
        path=path,
        merge_mode="upsert",
        merge_keys=["id"],
        partition_by=[],
        enable_cdf=False,
        connection=None,
        flow_id=1,
        node_id=2,
    )


def test_a_delta_write_and_merge_report_the_rows_they_wrote(backend, tmp_path):
    path = str(tmp_path / "people")
    people = pl.LazyFrame({"id": [1, 2, 3], "name": ["ada", "grace", "edsger"]})
    changes = pl.LazyFrame({"id": [3, 4], "name": ["dijkstra", "barbara"]})
    schema = [{"name": "id", "dtype": "Int64"}, {"name": "name", "dtype": "String"}]

    written = backend.run_op(OperationKind.delta_write, op_payload(delta_write(path, people, "overwrite")))
    written_size = get_delta_size_bytes(path)
    merged = backend.run_op(OperationKind.delta_merge, op_payload(delta_merge(path, changes)))

    assert written == OpResult(
        refs={},
        metadata={"table_path": path, "schema": schema, "row_count": 3, "column_count": 2, "size_bytes": written_size},
    )
    assert merged == OpResult(
        refs={},
        metadata={
            "table_path": path,
            "schema": schema,
            "row_count": 4,
            "column_count": 2,
            "size_bytes": get_delta_size_bytes(path),
        },
    ), "a merge reports the table after it, as the worker always has"
    assert pl.read_delta(path).sort("id").rows() == [(1, "ada"), (2, "grace"), (3, "dijkstra"), (4, "barbara")]


def test_an_append_of_no_rows_reports_a_skip(backend, tmp_path):
    path = str(tmp_path / "people")
    backend.run_op(OperationKind.delta_write, op_payload(delta_write(path, pl.LazyFrame({"id": [1]}), "overwrite")))

    appended = backend.run_op(
        OperationKind.delta_write,
        op_payload(delta_write(path, pl.LazyFrame({"id": []}, schema={"id": pl.Int64}), "append")),
    )

    assert appended == OpResult(refs={}, metadata={"skipped": True})
    assert pl.read_delta(path).height == 1


def test_a_delta_write_on_a_connection_hands_the_workers_writer_the_options_decrypted_there(backend, monkeypatch):
    received = []

    def record(df, path, **kwargs):
        received.append((df, path, kwargs))
        return False

    monkeypatch.setattr("shared.delta_utils.write_delta", record)
    connection = s3_connection().get_worker_interface(USER_ID)
    write = delta_write("s3://bucket/t", pl.LazyFrame({"id": [1]}), "overwrite", connection)

    result = backend.run_op(OperationKind.delta_write, op_payload(write))

    assert result == OpResult(refs={}, metadata={"skipped": True})
    ((df, path, kwargs),) = received
    assert (df.to_dicts(), path, kwargs["mode"]) == ([{"id": 1}], "s3://bucket/t", "overwrite")
    assert kwargs["storage_options"]["aws_secret_access_key"] == SECRET


def test_an_scd2_write_on_a_connection_hands_the_workers_writer_the_options_decrypted_there(backend, monkeypatch):
    received = []

    def record(df, path, **kwargs):
        received.append((df, path, kwargs))
        return Scd2Result(skipped=True, version=2)

    monkeypatch.setattr("shared.delta_utils.scd2_into_delta", record)
    write = scd2_write("s3://bucket/dim", pl.LazyFrame({"id": [1], "val": ["a"]}), "2024-05-01T00:00:00+00:00")
    write = DeltaScd2Payload(**{**write.__dict__, "connection": s3_connection().get_worker_interface(USER_ID)})

    result = backend.run_op(OperationKind.delta_scd2, op_payload(write))

    assert result == OpResult(refs={}, metadata={"skipped": True, "version": 2})
    ((df, path, kwargs),) = received
    assert (df.to_dicts(), path) == ([{"id": 1, "val": "a"}], "s3://bucket/dim")
    assert kwargs["storage_options"]["aws_secret_access_key"] == SECRET
    assert kwargs["valid_from_iso"] == "2024-05-01T00:00:00+00:00"


def viz_payload(request: str = "fields", **overrides) -> VizSessionPayload:
    values = {
        "plan": pl.LazyFrame({"g": ["a", "b"], "v": [1, 2]}),
        "session_key": "node:abc:run",
        "request": request,
        "flow_id": 1,
        "node_id": 2,
        **overrides,
    }
    return VizSessionPayload(**values)


def test_a_viz_session_asks_the_workers_session_registry_and_answers_as_its_routes_do(backend, monkeypatch):
    from flowfile_worker import viz_sessions

    asked = []

    def execute(source, op, payload, max_rows):
        asked.append((source, op, payload, max_rows))
        if op == "fields":
            return {"fields": [{"fid": "g"}]}, False
        return {"rows": [{"g": "a"}], "total_rows": 1, "truncated": False, "elapsed_ms": 1.5, "cache_hit": True}, True

    monkeypatch.setattr(viz_sessions.viz_session_registry, "execute", execute)
    fields = viz_payload()
    query = viz_payload("query", query={"workflow": []}, max_rows=10)

    answered_fields = backend.run_op(OperationKind.viz_session, op_payload(fields))
    answered_query = backend.run_op(OperationKind.viz_session, op_payload(query))

    assert answered_fields == OpResult(refs={}, metadata={"fields": [{"fid": "g"}], "cache_hit": False, "error": None})
    assert answered_query == OpResult(
        refs={},
        metadata={
            "rows": [{"g": "a"}],
            "total_rows": 1,
            "truncated": False,
            "elapsed_ms": 1.5,
            "cache_hit": True,
            "error": None,
        },
    )
    (fields_source, fields_op, _, _), (query_source, query_op, query_payload, max_rows) = asked
    assert (fields_source.kind, fields_source.session_key, fields_op) == ("plan", "node:abc:run", "fields")
    assert pl.LazyFrame.deserialize(io.BytesIO(fields_source.plan_bytes)).collect().to_dicts() == [
        {"g": "a", "v": 1},
        {"g": "b", "v": 2},
    ]
    assert (query_source.session_key, query_op, query_payload, max_rows) == (
        "node:abc:run",
        "execute",
        {"workflow": []},
        10,
    )


def test_a_viz_session_that_cannot_answer_reports_its_error_or_fails_as_the_route_does(backend, monkeypatch):
    from fastapi import HTTPException

    from flowfile_worker import viz_sessions

    def value_error(source, op, payload, max_rows):
        raise ValueError("unknown field 'z'")

    monkeypatch.setattr(viz_sessions.viz_session_registry, "execute", value_error)
    answered = backend.run_op(OperationKind.viz_session, op_payload(viz_payload()))
    assert answered == OpResult(refs={}, metadata={"fields": [], "cache_hit": False, "error": "unknown field 'z'"})

    def busy(source, op, payload, max_rows):
        raise HTTPException(status_code=503, detail="viz session busy")

    monkeypatch.setattr(viz_sessions.viz_session_registry, "execute", busy)
    with pytest.raises(RuntimeError, match="visualize_fields failed: viz session busy"):
        backend.run_op(OperationKind.viz_session, op_payload(viz_payload()))


# A custom node whose class has no node file


def custom_node(node_class: type[CustomNodeBase] = Splitter, output_names=None) -> CustomNodePayload:
    instance = node_class.from_settings({})
    return CustomNodePayload(
        inputs=(pl.LazyFrame({"n": list(range(100))}),),
        custom_node=instance,
        node_source=None,
        class_name=None,
        settings_values={},
        output_names=output_names if output_names is not None else list(instance.output_names),
        user_id=None,
        flow_id=7,
        node_id=3,
    )


def test_a_class_with_no_node_file_runs_here_and_its_outputs_are_held_as_the_workers(backend, kept):
    job = custom_node()

    refs = backend.run_op(OperationKind.custom_node, op_payload(job)).refs

    kept.extend(refs.values())
    assert list(refs) == ["output-0", "output-1"]
    big, small = refs.values()
    assert [(ref.result_id, ref.state, ref.row_count) for ref in (big, small)] == [
        (f"{job.job_id}_0", "materialised", 10),
        (f"{job.job_id}_1", "materialised", 90),
    ]
    assert backend.plan_for(big).collect()["n"].to_list() == list(range(90, 100))
    assert rows(backend.sample(small, 100))["n"].to_list() == list(range(90))
    assert [backend.count(ref) for ref in (big, small)] == [10, 90]
    backend.release(big)
    assert (backend.exists(big), backend.exists(small)) == (False, True)


@pytest.mark.parametrize(
    ("node_class", "expected"), [(Doubler, ["result"]), (Silent, [])], ids=["one-frame", "nothing"]
)
def test_a_class_with_no_node_file_keys_its_result_by_what_process_returned(backend, kept, node_class, expected):
    refs = backend.run_op(OperationKind.custom_node, op_payload(custom_node(node_class))).refs

    kept.extend(refs.values())
    assert list(refs) == expected
    if refs:
        assert backend.plan_for(refs["result"]).collect()["n"].to_list()[:3] == [0, 2, 4]


def test_a_dict_without_a_declared_output_is_refused_and_nothing_is_held(backend):
    job = custom_node(output_names=["big", "small", "medium"])

    with pytest.raises(ValueError, match="did not return declared output 'medium'"):
        backend.run_op(OperationKind.custom_node, op_payload(job))

    for i in range(3):
        assert backend.exists(ResultRef("local", f"{job.job_id}_{i}", [], None, "materialised", b"")) is False
