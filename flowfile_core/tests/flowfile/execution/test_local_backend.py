"""Core's own process as a compute: plans held lazily in an in-process store, operations run in process."""

import datetime
import io

import polars as pl
import pytest

from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends.local import LocalBackend
from flowfile_core.flowfile.execution.errors import (
    ForeignRef,
    OperationUnsupported,
    ResultMissing,
)
from flowfile_core.flowfile.execution.protocol import (
    ComputeBackend,
    JobStatus,
    OperationKind,
    OpResult,
    ResultRef,
    SubmitHints,
)
from flowfile_core.flowfile.execution.write_payloads import (
    PAYLOAD_KEY,
    DeltaMergePayload,
    DeltaScd2Payload,
    DeltaWritePayload,
    FileWritePayload,
)
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.schemas import input_schema
from shared.compute_protocol import PROTOCOL_VERSION, local_capabilities
from shared.delta_utils import get_delta_size_bytes

UNSUPPORTED_KINDS = (
    OperationKind.kafka_read,
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


def hints(*, must_materialise: bool = False, preview_rows: int | None = None) -> SubmitHints:
    return SubmitHints(must_materialise=must_materialise, wide=False, fanout=1, preview_rows=preview_rows)


def submit(backend: LocalBackend, plan: pl.LazyFrame, inputs=(), submit_hints=None, result_id: str = "node-hash"):
    return backend.submit(plan, list(inputs), submit_hints or hints(), result_id, flow_id=1, node_id=1)


def count_collects(monkeypatch) -> list[int]:
    """One entry per ``LazyFrame.collect`` from here on."""
    collects = []
    real_collect = pl.LazyFrame.collect

    def counting_collect(self, *args, **kwargs):
        collects.append(1)
        return real_collect(self, *args, **kwargs)

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
    assert backend.capabilities() == local_capabilities()


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

    assert collects == []
    assert ref.state == "deferred"
    assert ref.row_count is None
    assert backend.count(ref) == 10


def test_a_plan_that_fails_to_collect_is_held_and_fails_where_it_is_read(backend):
    failing = pl.LazyFrame({"a": ["x"]}).select(pl.col("a").cast(pl.Int64, strict=True))

    ref = submit(backend, failing, submit_hints=hints(must_materialise=True, preview_rows=5)).ref

    assert ref.state == "deferred"
    with pytest.raises(pl.exceptions.InvalidOperationError):
        backend.plan_for(ref).collect()
    with pytest.raises(pl.exceptions.InvalidOperationError):
        backend.sample(ref, 5)


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


def test_export_and_import_are_unsupported(backend, ten_rows):
    ref = submit(backend, ten_rows).ref

    with pytest.raises(OperationUnsupported, match="'local'"):
        backend.export(ref, "s3://bucket/key", None)
    with pytest.raises(OperationUnsupported, match="'local'"):
        backend.import_uri("s3://bucket/key", ref.schema, None, None)


@pytest.mark.parametrize("kind", UNSUPPORTED_KINDS, ids=lambda kind: kind.value)
def test_an_operation_without_a_local_path_is_unsupported(backend, kind):
    with pytest.raises(OperationUnsupported, match=kind.value):
        backend.run_op(kind, {})


def test_random_split_holds_one_result_per_split(backend):
    table = FlowDataEngine(pl.LazyFrame({"n": list(range(100))}))
    settings = input_schema.NodeRandomSplit(flow_id=1, node_id=1, seed=7)

    refs = backend.run_op(OperationKind.random_split, {"input": table, "settings": settings}).refs

    assert list(refs) == ["train", "test"]
    assert [backend.count(ref) for ref in refs.values()] == [80, 20]
    rows = pl.concat([backend.plan_for(ref).collect() for ref in refs.values()])
    assert sorted(rows["n"].to_list()) == list(range(100))


def test_file_read_holds_the_rows_of_a_csv(backend, tmp_path):
    path = tmp_path / "people.csv"
    path.write_text("name,age\nada,36\ngrace,85\n")
    received = input_schema.ReceivedTable(file_type="csv", name=path.name, path=str(path))
    settings = input_schema.NodeRead(flow_id=1, node_id=1, cache_data=False, received_file=received)

    refs = backend.run_op(OperationKind.file_read, {"settings": settings, "node_logger": None}).refs

    assert list(refs) == ["result"]
    assert backend.plan_for(refs["result"]).collect().to_dicts() == [
        {"name": "ada", "age": 36},
        {"name": "grace", "age": 85},
    ]
    assert [c.column_name for c in refs["result"].schema] == ["name", "age"]


def test_fuzzy_join_holds_the_matches(backend):
    names = FlowDataEngine([{"name": "eduward"}, {"name": "edward"}, {"name": "courtney"}])
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

    refs = backend.run_op(
        OperationKind.fuzzy_join, {"left": names, "right": names, "settings": settings, "node_logger": None}
    ).refs

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

    result = backend.run_op(OperationKind.file_write, {PAYLOAD_KEY: write})

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
        OperationKind.delta_scd2, {PAYLOAD_KEY: scd2_write(path, first_batch, "2024-01-01T00:00:00+00:00")}
    )
    first_size = get_delta_size_bytes(path)
    second = backend.run_op(
        OperationKind.delta_scd2, {PAYLOAD_KEY: scd2_write(path, second_batch, "2024-02-01T00:00:00+00:00")}
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
    backend.run_op(OperationKind.delta_scd2, {PAYLOAD_KEY: scd2_write(path, batch, "2024-01-01T00:00:00+00:00")})

    again = backend.run_op(
        OperationKind.delta_scd2, {PAYLOAD_KEY: scd2_write(path, batch, "2024-02-01T00:00:00+00:00")}
    )

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

    written = backend.run_op(OperationKind.delta_write, {PAYLOAD_KEY: delta_write(path, people, "overwrite")})
    written_size = get_delta_size_bytes(path)
    merged = backend.run_op(OperationKind.delta_merge, {PAYLOAD_KEY: delta_merge(path, changes)})

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
    backend.run_op(OperationKind.delta_write, {PAYLOAD_KEY: delta_write(path, pl.LazyFrame({"id": [1]}), "overwrite")})

    appended = backend.run_op(
        OperationKind.delta_write,
        {PAYLOAD_KEY: delta_write(path, pl.LazyFrame({"id": []}, schema={"id": pl.Int64}), "append")},
    )

    assert appended == OpResult(refs={}, metadata={"skipped": True})
    assert pl.read_delta(path).height == 1
