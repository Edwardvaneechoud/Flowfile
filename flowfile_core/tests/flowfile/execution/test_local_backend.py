"""Core's own process as a compute: plans held lazily in an in-process store, operations run in process."""

import io

import polars as pl
import pytest

from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends.local import LocalBackend
from flowfile_core.flowfile.execution.errors import (
    ForeignRef,
    OperationUnsupported,
    PlanRejected,
    ResultMissing,
    TaskFailed,
)
from flowfile_core.flowfile.execution.protocol import (
    ComputeBackend,
    JobStatus,
    OperationKind,
    OpResult,
    ResultRef,
    SubmitHints,
)
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.schemas import input_schema
from shared.compute_protocol import PROTOCOL_VERSION, local_capabilities

UNSUPPORTED_KINDS = (
    OperationKind.kafka_read,
    OperationKind.google_analytics_read,
    OperationKind.ml_train,
    OperationKind.ml_apply,
    OperationKind.delta_scd2,
    OperationKind.viz_session,
)


def hints(*, must_materialise: bool = False, preview_rows: int | None = None) -> SubmitHints:
    return SubmitHints(must_materialise=must_materialise, wide=False, fanout=1, preview_rows=preview_rows)


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
def ten_rows() -> bytes:
    return pl.LazyFrame({"n": list(range(10)), "label": [f"row {i}" for i in range(10)]}).serialize()


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
    submitted = backend.submit(ten_rows, [], hints())
    ref = submitted.ref

    assert ref.backend_id == "local"
    assert ref.state == "deferred"
    assert ref.row_count is None
    assert ref.location == ref.result_id.encode("utf-8")
    assert [(c.column_name, c.data_type) for c in ref.schema] == [("n", "Int64"), ("label", "String")]
    assert submitted.handle.job_id == ref.result_id
    assert backend.status(submitted.handle) == JobStatus(state="done", progress=100, error=None)

    assert ipc_rows(backend.sample(ref, 3)).height == 3
    assert ipc_rows(backend.sample(ref, 50)).height == 10
    assert backend.count(ref) == 10
    assert backend.exists(ref)
    assert backend.plan_for(ref).collect()["n"].to_list() == list(range(10))


def test_a_released_result_is_gone(backend, ten_rows):
    ref = backend.submit(ten_rows, [], hints()).ref

    backend.release(ref)

    assert not backend.exists(ref)
    with pytest.raises(ResultMissing):
        backend.plan_for(ref)
    with pytest.raises(ResultMissing):
        backend.sample(ref, 1)
    backend.release(ref)


def test_must_materialise_collects_the_plan(backend, ten_rows):
    ref = backend.submit(ten_rows, [], hints(must_materialise=True)).ref

    assert ref.state == "materialised"
    assert ref.row_count == 10
    assert backend.count(ref) == 10
    assert backend.plan_for(ref).collect().height == 10


def test_a_plan_that_fails_to_collect_fails_the_submit(backend):
    failing = pl.LazyFrame({"a": ["x"]}).select(pl.col("a").cast(pl.Int64, strict=True)).serialize()

    with pytest.raises(TaskFailed):
        backend.submit(failing, [], hints(must_materialise=True))


def test_a_plan_that_does_not_deserialise_is_rejected(backend):
    with pytest.raises(PlanRejected):
        backend.submit(b"not a plan", [], hints())


def test_the_inline_sample_is_there_only_when_preview_rows_ask_for_it(backend, ten_rows):
    with_sample = backend.submit(ten_rows, [], hints(preview_rows=4))
    without_sample = backend.submit(ten_rows, [], hints())

    assert ipc_rows(with_sample.sample).height == 4
    assert without_sample.sample is None


def test_inputs_are_validated_against_the_store(backend, ten_rows, foreign_ref):
    held = backend.submit(ten_rows, [], hints()).ref
    assert backend.submit(ten_rows, [held], hints()).ref.state == "deferred"

    with pytest.raises(ForeignRef):
        backend.submit(ten_rows, [foreign_ref], hints())
    backend.release(held)
    with pytest.raises(ResultMissing):
        backend.submit(ten_rows, [held], hints())


def test_a_ref_from_another_backend_is_refused(backend, foreign_ref):
    for read in (backend.plan_for, backend.exists, backend.release, backend.count):
        with pytest.raises(ForeignRef):
            read(foreign_ref)
    with pytest.raises(ForeignRef):
        backend.sample(foreign_ref, 1)


def test_export_and_import_are_unsupported(backend, ten_rows):
    ref = backend.submit(ten_rows, [], hints()).ref

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
    table = FlowDataEngine(pl.LazyFrame({"name": ["ada", "grace"]}))
    settings = input_schema.NodeOutput(
        flow_id=1,
        node_id=2,
        output_settings={
            "name": "people.csv",
            "directory": str(tmp_path),
            "file_type": "csv",
            "write_mode": "overwrite",
            "table_settings": {"file_type": "csv", "delimiter": ",", "encoding": "utf-8"},
        },
    )

    result = backend.run_op(OperationKind.file_write, {"input": table, "settings": settings, "flow_id": 1})

    assert isinstance(result, OpResult)
    assert result.refs == {}
    assert result.metadata == {}
    assert pl.read_csv(tmp_path / "people.csv")["name"].to_list() == ["ada", "grace"]
