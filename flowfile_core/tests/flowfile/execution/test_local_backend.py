"""Core's own process as a compute: plans held lazily in an in-process store, operations run in process.

With the protocol flag on, a plan that must be materialised is streamed to a file under core's storage directory
instead, and held as a scan of that file. A result is exported to a URI by a streaming sink of its plan, and a URI is
imported as a scan held deferred. A blob is copied into core's storage directory and out again, and never read as rows.
"""

import datetime
import io

import polars as pl
import pytest

from flowfile_core.configs import settings
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends.local import LocalBackend
from flowfile_core.flowfile.execution.errors import (
    ForeignRef,
    OperationUnsupported,
    ResultMissing,
)
from flowfile_core.flowfile.execution.payloads import (
    DeltaMergePayload,
    DeltaScd2Payload,
    DeltaWritePayload,
    FileReadPayload,
    FileWritePayload,
    FuzzyJoinPayload,
    RandomSplitPayload,
    op_payload,
)
from flowfile_core.flowfile.execution.protocol import (
    ComputeBackend,
    CredentialRef,
    JobStatus,
    OperationKind,
    OpResult,
    ResultRef,
    SubmitHints,
)
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.schemas import input_schema
from shared.compute_protocol import PROTOCOL_VERSION, local_capabilities
from shared.delta_utils import get_delta_size_bytes
from shared.storage_config import storage

UNSUPPORTED_KINDS = (
    OperationKind.google_analytics_read,
    OperationKind.ml_train,
    OperationKind.ml_apply,
    OperationKind.viz_session,
)
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


def hints(*, must_materialise: bool = False, preview_rows: int | None = None) -> SubmitHints:
    return SubmitHints(must_materialise=must_materialise, wide=False, fanout=1, preview_rows=preview_rows)


def submit(backend: LocalBackend, plan: pl.LazyFrame, inputs=(), submit_hints=None, result_id: str = "node-hash"):
    return backend.submit(plan, list(inputs), submit_hints or hints(), result_id, flow_id=1, node_id=1)


def count_collects(monkeypatch) -> list[int]:
    """The height of each frame ``LazyFrame.collect`` returns from here on (a streaming sink's own is empty)."""
    collects = []
    real_collect = pl.LazyFrame.collect

    def counting_collect(self, *args, **kwargs):
        collected = real_collect(self, *args, **kwargs)
        collects.append(collected.height)
        return collected

    monkeypatch.setattr(pl.LazyFrame, "collect", counting_collect)
    return collects


def ipc_rows(sample: bytes) -> pl.DataFrame:
    return pl.read_ipc_stream(io.BytesIO(sample))


@pytest.fixture
def fresh_registry():
    registry.reset()
    yield
    registry.reset()


@pytest.fixture
def backend() -> LocalBackend:
    return LocalBackend()


@pytest.fixture
def ten_rows() -> pl.LazyFrame:
    return pl.LazyFrame({"n": list(range(10)), "label": [f"row {i}" for i in range(10)]})


@pytest.fixture
def foreign_ref() -> ResultRef:
    return ResultRef(backend_id="worker", result_id="abc", schema=[], row_count=None, state="deferred", location=b"abc")


def test_the_local_backend_satisfies_the_backend_interface(backend):
    assert isinstance(backend, ComputeBackend)
    assert backend.id == "local"
    assert backend.protocol_version == PROTOCOL_VERSION
    capabilities = backend.capabilities()
    undeclared = {"operations": frozenset(), "read_schemes": frozenset(), "write_schemes": frozenset()}
    assert capabilities.model_copy(update=undeclared) == local_capabilities()
    assert capabilities.read_schemes == capabilities.write_schemes == {"file", "s3", "az", "gs", "https"}
    assert capabilities.credential_resolution == "master_key"


def test_the_registry_makes_the_local_backend_without_a_registration(fresh_registry):
    made = registry.backend(registry.LOCAL_BACKEND_ID)

    assert isinstance(made, LocalBackend)
    assert registry.backend(registry.LOCAL_BACKEND_ID) is made


def test_a_registration_under_the_local_id_wins_over_the_built_in(fresh_registry):
    replacement = object()
    registry.register_backend(registry.LOCAL_BACKEND_ID, lambda: replacement)

    assert registry.backend(registry.LOCAL_BACKEND_ID) is replacement


def test_a_submitted_plan_is_held_deferred_and_read_back(backend, ten_rows):
    submitted = submit(backend, ten_rows, result_id="hash-1")
    ref = submitted.ref

    assert ref.backend_id == "local"
    assert ref.result_id == "hash-1"
    assert ref.state == "deferred"
    assert ref.row_count is None
    assert ref.location == b"hash-1"
    assert [(c.column_name, c.data_type) for c in ref.schema] == [("n", "Int64"), ("label", "String")]
    assert submitted.handle.job_id == "hash-1"
    assert backend.status(submitted.handle) == JobStatus(state="done", progress=100, error=None)

    assert ipc_rows(backend.sample(ref, 3)).height == 3
    assert ipc_rows(backend.sample(ref, 50)).height == 10
    assert backend.count(ref) == 10
    assert backend.exists(ref)
    assert backend.plan_for(ref).collect()["n"].to_list() == list(range(10))


def test_submitting_under_a_held_id_replaces_that_result(backend, ten_rows):
    three_rows = pl.LazyFrame({"n": [1, 2, 3]})
    first = submit(backend, ten_rows, result_id="same").ref

    second = submit(backend, three_rows, result_id="same").ref

    assert backend.exists(first) and backend.exists(second)
    assert backend.plan_for(second).collect()["n"].to_list() == [1, 2, 3]


def test_a_released_result_is_gone(backend, ten_rows):
    ref = submit(backend, ten_rows).ref

    backend.release(ref)

    assert not backend.exists(ref)
    with pytest.raises(ResultMissing):
        backend.plan_for(ref)
    with pytest.raises(ResultMissing):
        backend.sample(ref, 1)
    backend.release(ref)


def test_must_materialise_does_not_collect(backend, ten_rows, monkeypatch):
    collects = count_collects(monkeypatch)

    ref = submit(backend, ten_rows, submit_hints=hints(must_materialise=True)).ref

    assert sum(collects) == 0
    assert ref.state == ("materialised" if settings.FLOWFILE_COMPUTE_PROTOCOL else "deferred")
    assert ref.row_count is None
    assert backend.count(ref) == 10
    backend.release(ref)


def test_a_plan_that_fails_to_collect_is_held_and_fails_where_it_is_read(backend, monkeypatch, tmp_path):
    failing = pl.LazyFrame({"a": ["x"]}).select(pl.col("a").cast(pl.Int64, strict=True))
    required = hints(must_materialise=True, preview_rows=5)

    if settings.FLOWFILE_COMPUTE_PROTOCOL:
        monkeypatch.setattr(storage, "_base_dir", tmp_path)  # Polars writes the failed sink's file after raising
        with pytest.raises(pl.exceptions.InvalidOperationError):
            submit(backend, failing, submit_hints=required)
        assert backend._results == {}
        assert not (tmp_path / "cache" / "local" / "1" / "node-hash.arrow").exists()
        return

    ref = submit(backend, failing, submit_hints=required).ref

    assert ref.state == "deferred"
    with pytest.raises(pl.exceptions.InvalidOperationError):
        backend.plan_for(ref).collect()
    with pytest.raises(pl.exceptions.InvalidOperationError):
        backend.sample(ref, 5)


@pytest.mark.parametrize("next_call", ["release", "submit"])
def test_a_failed_sink_never_writes_the_result_file_and_what_it_wrote_goes_with_the_ids_next_call(
    backend, ten_rows, next_call, monkeypatch, tmp_path
):
    """Polars can write a failed sink's file after raising; that file sits beside the result's and goes with the id."""
    if not settings.FLOWFILE_COMPUTE_PROTOCOL:
        pytest.skip("the local backend stores nothing with the protocol flag off")
    monkeypatch.setattr(storage, "_base_dir", tmp_path)
    result_file = tmp_path / "cache" / "local" / "1" / "node-hash.arrow"
    held = submit(backend, ten_rows, submit_hints=hints(must_materialise=True)).ref
    sinks = []

    def failing_sink(self, path, **kwargs):
        sinks.append(path)
        raise pl.exceptions.ComputeError("the sink failed")

    monkeypatch.setattr(pl.LazyFrame, "sink_ipc", failing_sink)
    with pytest.raises(pl.exceptions.ComputeError):
        submit(backend, ten_rows, submit_hints=hints(must_materialise=True))
    sinks[0].touch()  # the file Polars writes once the error is raised, made here so nothing waits for it

    assert sinks[0].parent == result_file.parent
    assert not result_file.exists()
    if next_call == "release":
        backend.release(held)
    else:
        submit(backend, ten_rows)
    assert not sinks[0].exists()


def test_the_plan_is_held_as_the_object_submitted(backend, monkeypatch):
    """No serialise, no copy: a deferred result is the submitted plan, so plans compose over one in-memory frame."""
    serialised = []
    monkeypatch.setattr(pl.LazyFrame, "serialize", lambda self, *a, **k: serialised.append(1))
    plan = pl.DataFrame({"n": [1, 2]}).lazy().filter(pl.col("n") > 1)

    ref = submit(backend, plan).ref

    assert backend.plan_for(ref) is plan
    assert serialised == []


@pytest.mark.parametrize("preview_rows", [None, 4])
def test_no_sample_comes_inline_and_sample_serves_it_when_read(backend, ten_rows, preview_rows, monkeypatch):
    collects = count_collects(monkeypatch)

    submitted = submit(backend, ten_rows, submit_hints=hints(preview_rows=preview_rows))

    assert submitted.sample is None
    assert collects == []
    assert ipc_rows(backend.sample(submitted.ref, 4)).height == 4


def test_inputs_are_validated_against_the_store(backend, ten_rows, foreign_ref):
    held = submit(backend, ten_rows, result_id="upstream").ref
    assert submit(backend, ten_rows, [held], result_id="downstream").ref.state == "deferred"

    with pytest.raises(ForeignRef):
        submit(backend, ten_rows, [foreign_ref])
    backend.release(held)
    with pytest.raises(ResultMissing):
        submit(backend, ten_rows, [held])


def test_a_ref_from_another_backend_is_refused(backend, foreign_ref):
    for read in (backend.plan_for, backend.exists, backend.release, backend.count):
        with pytest.raises(ForeignRef):
            read(foreign_ref)
    with pytest.raises(ForeignRef):
        backend.sample(foreign_ref, 1)


@pytest.mark.parametrize("must_materialise", [False, True], ids=["deferred", "required"])
def test_export_streams_the_rows_to_the_uri_without_collecting(
    backend, ten_rows, foreign_ref, must_materialise, monkeypatch, tmp_path
):
    """With the protocol flag on, a required result is stored, so its export sinks the scan of its file."""
    monkeypatch.setattr(storage, "_base_dir", tmp_path / "storage")
    ref = submit(backend, ten_rows, submit_hints=hints(must_materialise=must_materialise)).ref
    stored = must_materialise and bool(settings.FLOWFILE_COMPUTE_PROTOCOL)
    assert ref.state == ("materialised" if stored else "deferred")
    target = f"file://{tmp_path}/out.parquet"
    collects = count_collects(monkeypatch)

    assert backend.export(ref, target, None) == target

    assert sum(collects) == 0
    assert pl.read_parquet(target).equals(ten_rows.collect())
    with pytest.raises(ForeignRef):
        backend.export(foreign_ref, f"file://{tmp_path}/foreign.parquet", None)
    backend.release(ref)
    with pytest.raises(ResultMissing):
        backend.export(ref, f"file://{tmp_path}/released.parquet", None)
    assert not (tmp_path / "foreign.parquet").exists()
    assert not (tmp_path / "released.parquet").exists()


def test_import_uri_holds_a_scan_with_the_given_schema_and_count_and_reads_nothing(
    backend, ten_rows, monkeypatch, tmp_path
):
    """The file is written only after the import, so an import that read it would fail."""
    path = tmp_path / "in.parquet"
    schema = submit(backend, ten_rows, result_id="source").ref.schema
    collects = count_collects(monkeypatch)

    ref = backend.import_uri(f"file://{path}", schema, 10, None)

    assert (ref.backend_id, ref.state, ref.row_count) == ("local", "deferred", 10)
    assert [(c.column_name, c.data_type) for c in ref.schema] == [("n", "Int64"), ("label", "String")]
    assert backend.exists(ref)
    assert backend.count(ref) == 10
    assert collects == []

    ten_rows.collect().write_parquet(path)

    assert ipc_rows(backend.sample(ref, 3)).equals(ten_rows.head(3).collect())
    assert isinstance(backend.plan_for(ref), pl.LazyFrame)
    backend.release(ref)
    assert not backend.exists(ref)
    assert path.is_file()


@pytest.fixture
def blob_source(monkeypatch, tmp_path):
    """A file of ``BLOB`` to import, with core's storage directory in ``tmp_path / "storage"``."""
    monkeypatch.setattr(storage, "_base_dir", tmp_path / "storage")
    source = tmp_path / "model.bin"
    source.write_bytes(BLOB)
    return source


def test_a_blob_is_copied_into_cores_storage_and_out_again(backend, blob_source, tmp_path):
    ref = backend.import_uri(f"file://{blob_source}", None, None, None, kind="blob")

    (held,) = storage.cache_directory.rglob("*.blob")
    assert held.read_bytes() == BLOB
    assert (ref.kind, ref.state, ref.schema, ref.row_count) == ("blob", "materialised", [], None)
    assert backend.exists(ref)
    target = f"file://{tmp_path}/copy.bin"
    assert backend.export(ref, target, None) == target
    assert (tmp_path / "copy.bin").read_bytes() == BLOB
    backend.release(ref)
    assert not backend.exists(ref)
    assert not held.exists()
    assert blob_source.read_bytes() == (tmp_path / "copy.bin").read_bytes() == BLOB
    with pytest.raises(ResultMissing):
        backend.export(ref, f"file://{tmp_path}/released.bin", None)


def test_a_plan_cannot_read_a_blob(backend, blob_source, ten_rows):
    blob = backend.import_uri(f"file://{blob_source}", None, None, None, kind="blob")

    with pytest.raises(OperationUnsupported, match="is a blob"):
        submit(backend, ten_rows, [blob])
    for read in (backend.plan_for, backend.count, lambda ref: backend.sample(ref, 1)):
        with pytest.raises(OperationUnsupported, match="is a blob"):
            read(blob)
    assert backend._results == {}
    backend.release(blob)


@pytest.mark.parametrize(
    "given",
    [{"schema": []}, {"row_count": 0}, {"credential": CredentialRef(connection_id=7, user_id=3)}],
    ids=["schema", "row_count", "credential"],
)
def test_a_blob_takes_no_credential_schema_or_row_count(backend, blob_source, tmp_path, given):
    arguments = {"schema": None, "row_count": None, "credential": None, **given}

    with pytest.raises(ValueError, match="A blob import takes no"):
        backend.import_uri(f"file://{blob_source}", **arguments, kind="blob")

    assert backend._blobs == {}
    assert list(storage.cache_directory.rglob("*.blob")) == []
    blob = backend.import_uri(f"file://{blob_source}", None, None, None, kind="blob")
    with pytest.raises(ValueError, match="takes no credential"):
        backend.export(blob, f"file://{tmp_path}/out.bin", CredentialRef(connection_id=7, user_id=3))
    assert not (tmp_path / "out.bin").exists()
    backend.release(blob)


def test_a_table_import_needs_its_schema(backend, tmp_path):
    with pytest.raises(ValueError, match="needs its schema"):
        backend.import_uri(f"file://{tmp_path}/in.parquet", None, 10, None)

    assert backend._results == {}


def test_a_blob_uri_of_another_scheme_is_refused(backend, blob_source, tmp_path):
    with pytest.raises(ValueError, match="not s3://"):
        backend.import_uri("s3://bucket/model.bin", None, None, None, kind="blob")
    assert backend._blobs == {}
    blob = backend.import_uri(f"file://{blob_source}", None, None, None, kind="blob")
    with pytest.raises(ValueError, match="not s3://"):
        backend.export(blob, "s3://bucket/model.bin", None)
    backend.release(blob)


@pytest.mark.parametrize("kind", UNSUPPORTED_KINDS, ids=lambda kind: kind.value)
def test_an_operation_without_a_local_path_is_unsupported(backend, kind):
    with pytest.raises(OperationUnsupported, match=kind.value):
        backend.run_op(kind, {})


def test_random_split_holds_one_result_per_split(backend):
    settings = input_schema.NodeRandomSplit(flow_id=1, node_id=1, seed=7)
    split = RandomSplitPayload(plan=pl.LazyFrame({"n": list(range(100))}), settings=settings, flow_id=1, node_id=1)

    refs = backend.run_op(OperationKind.random_split, op_payload(split)).refs

    assert list(refs) == ["train", "test"]
    assert [backend.count(ref) for ref in refs.values()] == [80, 20]
    rows = pl.concat([backend.plan_for(ref).collect() for ref in refs.values()])
    assert sorted(rows["n"].to_list()) == list(range(100))


def test_file_read_holds_the_rows_of_a_csv(backend, tmp_path):
    path = tmp_path / "people.csv"
    path.write_text("name,age\nada,36\ngrace,85\n")
    received = input_schema.ReceivedTable(file_type="csv", name=path.name, path=str(path))
    settings = input_schema.NodeRead(flow_id=1, node_id=1, cache_data=False, received_file=received)

    read = FileReadPayload(flow_id=1, node_id=1, settings=settings, node_logger=None)
    refs = backend.run_op(OperationKind.file_read, op_payload(read)).refs

    assert list(refs) == ["result"]
    assert backend.plan_for(refs["result"]).collect().to_dicts() == [
        {"name": "ada", "age": 36},
        {"name": "grace", "age": 85},
    ]
    assert [c.column_name for c in refs["result"].schema] == ["name", "age"]


def test_fuzzy_join_holds_the_matches(backend):
    names = pl.LazyFrame({"name": ["eduward", "edward", "courtney"]})
    settings = input_schema.NodeFuzzyMatch(
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

    join = FuzzyJoinPayload(left=names, right=names, settings=settings, flow_id=1, node_id=2)

    refs = backend.run_op(OperationKind.fuzzy_join, op_payload(join)).refs

    matches = backend.plan_for(refs["result"]).collect().sort("name", "name_right")
    assert matches.select("name", "name_right").rows() == [
        ("courtney", "courtney"),
        ("eduward", "eduward"),
        ("eduward", "edward"),
        ("edward", "eduward"),
        ("edward", "edward"),
    ]


def test_a_write_holds_no_result_and_reports_no_metadata(backend, tmp_path):
    settings = input_schema.OutputSettings(
        name="people.csv",
        directory=str(tmp_path),
        file_type="csv",
        write_mode="overwrite",
        table_settings={"file_type": "csv", "delimiter": ",", "encoding": "utf-8"},
    )
    write = FileWritePayload(plan=pl.LazyFrame({"name": ["ada", "grace"]}), settings=settings, flow_id=1, node_id=2)

    result = backend.run_op(OperationKind.file_write, op_payload(write))

    assert isinstance(result, OpResult)
    assert result.refs == {}
    assert result.metadata == {}
    assert pl.read_csv(tmp_path / "people.csv")["name"].to_list() == ["ada", "grace"]


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


def delta_write(path: str, plan: pl.LazyFrame, mode: str) -> DeltaWritePayload:
    return DeltaWritePayload(
        plan=plan, path=path, mode=mode, partition_by=[], enable_cdf=False, connection=None, flow_id=1, node_id=2
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
            "row_count": 2,
            "column_count": 2,
            "size_bytes": get_delta_size_bytes(path),
        },
    )


def test_an_append_of_no_rows_reports_a_skip(backend, tmp_path):
    path = str(tmp_path / "people")
    backend.run_op(OperationKind.delta_write, op_payload(delta_write(path, pl.LazyFrame({"id": [1]}), "overwrite")))

    appended = backend.run_op(
        OperationKind.delta_write,
        op_payload(delta_write(path, pl.LazyFrame({"id": []}, schema={"id": pl.Int64}), "append")),
    )

    assert appended == OpResult(refs={}, metadata={"skipped": True})
    assert pl.read_delta(path).height == 1
