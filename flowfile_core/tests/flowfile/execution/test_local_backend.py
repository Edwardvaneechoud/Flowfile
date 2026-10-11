"""Core's own process as a compute: plans held lazily in an in-process store, operations run in process."""

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
from flowfile_core.flowfile.execution.payloads import FileReadPayload, op_payload
from flowfile_core.flowfile.execution.protocol import (
    ComputeBackend,
    JobStatus,
    OperationKind,
    OpResult,
    ResultRef,
    SubmitHints,
)
from flowfile_core.flowfile.execution.payloads import FileWritePayload
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.schemas import input_schema
from shared.compute_protocol import PROTOCOL_VERSION, local_capabilities

UNSUPPORTED_KINDS = (
    OperationKind.google_analytics_read,
    OperationKind.ml_train,
    OperationKind.ml_apply,
    OperationKind.delta_scd2,
    OperationKind.viz_session,
)


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

    read = FileReadPayload(flow_id=1, node_id=1, settings=settings, node_logger=None)
    refs = backend.run_op(OperationKind.file_read, op_payload(read)).refs

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

    result = backend.run_op(OperationKind.file_write, op_payload(write))

    assert isinstance(result, OpResult)
    assert result.refs == {}
    assert result.metadata == {}
    assert pl.read_csv(tmp_path / "people.csv")["name"].to_list() == ["ada", "grace"]
