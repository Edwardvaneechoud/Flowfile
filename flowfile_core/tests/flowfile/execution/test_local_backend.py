"""What ``local`` is in a library host: the worker embedded in this process, every job run here.

The registry makes it without a registration, as an ``EmbeddedWorkerBackend`` under the id ``local``, and its factory
sets the worker's in-process mode, so a write, a Delta operation, a visualisation request and a custom node whose class
has no node file all run in this process: no child is started, no pool member leased and no port opened or dialled
(``in_this_process``, shared with ``test_embedded_backend.py``, which holds the adapter's results and the operations it
builds a body for). The seven kinds the worker serves on its task routes run through the worker's own job runner and
session registry. Both flag states run the same cases: the embedded worker reads the flag nowhere.
"""

import datetime
import io
import sqlite3

import polars as pl
import pytest

from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends.embedded import EmbeddedWorkerBackend
from flowfile_core.flowfile.execution.errors import TaskFailed
from flowfile_core.flowfile.execution.payloads import (
    CustomNodePayload,
    DeltaScd2Payload,
    FileWritePayload,
    VizSessionPayload,
    op_payload,
)
from flowfile_core.flowfile.execution.protocol import ComputeBackend, JobHandle, OperationKind, OpResult, ResultRef
from flowfile_core.flowfile.node_designer import CustomNodeBase
from flowfile_worker import spawner
from flowfile_worker.external_sources.s3_source.models import FullCloudStorageConnection as WorkerConnection
from flowfile_worker.service import OPERATIONS
from shared.compute_protocol import PROTOCOL_VERSION, local_capabilities
from shared.delta_utils import Scd2Result, get_delta_size_bytes
from tests.flowfile.execution.test_embedded_backend import in_this_process, rows  # noqa: F401
from tests.flowfile.execution.test_op_payloads import Splitter, custom_node
from tests.flowfile.execution.test_write_payloads import (
    ROWS,
    SECRET,
    USER_ID,
    cloud_write,
    csv_settings,
    delta_merge,
    delta_scd2,
    delta_write,
    file_write,
    s3_connection,
    sqlite_write,
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


class Doubler(CustomNodeBase):
    node_name: str = "Local Backend Doubler"

    def process(self, *inputs: pl.LazyFrame):
        return inputs[0].with_columns(pl.col("n") * 2)


class Silent(CustomNodeBase):
    node_name: str = "Local Backend Silent"

    def process(self, *inputs: pl.LazyFrame):
        return None


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


# The seven kinds the worker serves on its task routes, run in this process


def test_a_file_write_holds_no_result_and_reports_no_metadata(backend, tmp_path):
    write = file_write(tmp_path)

    result = backend.run_op(OperationKind.file_write, op_payload(write))

    assert result == OpResult(refs={})
    assert result.metadata == {}
    assert pl.read_csv(tmp_path / "people.csv", separator=";").equals(ROWS)
    assert backend.status(JobHandle("local", write.job_id)).state == "done"


def test_a_file_write_that_fails_is_a_task_failure_naming_the_error(backend, tmp_path):
    output = csv_settings(tmp_path).model_copy(update={"write_mode": "create"})
    (tmp_path / "people.csv").write_text("taken\n")
    write = FileWritePayload(plan=ROWS.lazy(), settings=output, flow_id=7, node_id=3)

    with pytest.raises(TaskFailed, match="File already exists"):
        backend.run_op(OperationKind.file_write, op_payload(write))


def test_a_database_write_writes_a_sqlite_table(backend, tmp_path):
    result = backend.run_op(OperationKind.database_write, op_payload(sqlite_write(tmp_path / "people.db")))

    assert result == OpResult(refs={})
    with sqlite3.connect(tmp_path / "people.db") as db:
        assert db.execute("SELECT id, name FROM people ORDER BY id").fetchall() == list(ROWS.iter_rows())


def test_a_cloud_write_hands_the_workers_writer_the_connection_with_its_secrets_decrypted_there(backend, monkeypatch):
    received = []
    monkeypatch.setattr(
        "flowfile_worker.external_sources.s3_source.main.write_df_to_cloud",
        lambda df, settings, logger: received.append((df, settings)),
    )

    result = backend.run_op(OperationKind.cloud_write, op_payload(cloud_write()))

    assert result == OpResult(refs={})
    ((df, sent),) = received
    assert df.collect().equals(ROWS)
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


def test_an_scd2_write_loads_a_table_versions_the_rows_that_changed_and_skips_a_rerun(backend, tmp_path):
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
    again = backend.run_op(
        OperationKind.delta_scd2, op_payload(scd2_write(path, second_batch, "2024-03-01T00:00:00+00:00"))
    )

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
    assert again == OpResult(refs={}, metadata={"skipped": True, "version": 1}), "a rerun names the version it kept"
    jan, feb = datetime.datetime(2024, 1, 1, tzinfo=UTC), datetime.datetime(2024, 2, 1, tzinfo=UTC)
    assert pl.read_delta(path).sort("id", "valid_from").rows() == [
        (1, "a", 10, "086436db5d6237d8f53e55ebf5bafe19", jan, feb, False),
        (1, "A", 11, "8a33fe6e8900cddb5e995de3f3912ec3", feb, None, True),
        (2, "b", 20, "a4d2b5f0652fef0de72c82802a95ade3", jan, None, True),
        (3, "c", 30, "ec4f31d51c39fec0ce47aecc38b5bf5c", jan, None, True),
        (4, "d", 40, "b45a6c225243cc60905eb98c9803620e", feb, None, True),
    ]


@pytest.mark.parametrize("enable_cdf", [True, False], ids=["tracked", "untracked"])
def test_a_delta_write_and_merge_report_the_rows_they_wrote(backend, tmp_path, enable_cdf):
    path = str(tmp_path / "people")
    changes = pl.LazyFrame({"id": [3, 4], "name": ["dijkstra", "barbara"]})
    schema = [{"name": "id", "dtype": "Int64"}, {"name": "name", "dtype": "String"}]

    written = backend.run_op(OperationKind.delta_write, op_payload(delta_write(path, enable_cdf=enable_cdf)))
    written_size = get_delta_size_bytes(path)
    merged = backend.run_op(OperationKind.delta_merge, op_payload(delta_merge(path, changes)))
    merged_nothing = backend.run_op(OperationKind.delta_merge, op_payload(delta_merge(path, changes.clear())))

    assert written == OpResult(
        refs={},
        metadata={"table_path": path, "schema": schema, "row_count": 3, "column_count": 2, "size_bytes": written_size},
    )
    table_after = {"table_path": path, "schema": schema, "row_count": 4, "column_count": 2}
    assert merged == merged_nothing == OpResult(
        refs={}, metadata={**table_after, "size_bytes": get_delta_size_bytes(path)}
    ), "a merge reports the table after it, as the worker always has"
    assert pl.read_delta(path).sort("id").rows() == [(1, "ada"), (2, "grace"), (3, "dijkstra"), (4, "barbara")]


def test_an_append_reports_the_rows_it_appended_and_an_append_of_none_a_skip(backend, tmp_path):
    path = str(tmp_path / "numbers")
    backend.run_op(
        OperationKind.delta_write, op_payload(delta_write(path, plan=pl.LazyFrame({"x": [1, 2]}), enable_cdf=False))
    )

    def append(plan: pl.LazyFrame) -> OpResult:
        write = delta_write(path, mode="append", plan=plan, enable_cdf=False)
        return backend.run_op(OperationKind.delta_write, op_payload(write))

    appended = append(pl.LazyFrame({"x": [3, 4, 5]}))
    appended_nothing = append(pl.LazyFrame({"x": []}, schema={"x": pl.Int64}))

    assert appended == OpResult(
        refs={},
        metadata={
            "table_path": path,
            "schema": [{"name": "x", "dtype": "Int64"}],
            "row_count": 3,
            "column_count": 1,
            "size_bytes": get_delta_size_bytes(path),
        },
    )
    assert appended_nothing == OpResult(refs={}, metadata={"skipped": True})
    assert pl.read_delta(path).height == 5


def test_a_delta_write_on_a_connection_hands_the_workers_writer_the_options_decrypted_there(backend, monkeypatch):
    received = []

    def record(df, path, **kwargs):
        received.append((df, path, kwargs))
        return False

    monkeypatch.setattr("shared.delta_utils.write_delta", record)
    connection = s3_connection().get_worker_interface(USER_ID)

    result = backend.run_op(OperationKind.delta_write, op_payload(delta_write("s3://bucket/t", connection)))

    assert result == OpResult(refs={}, metadata={"skipped": True})
    ((df, path, kwargs),) = received
    assert (df.equals(ROWS), path, kwargs["mode"]) == (True, "s3://bucket/t", "overwrite")
    assert kwargs["storage_options"] == WorkerConnection.model_validate(connection.model_dump()).get_storage_options()
    assert kwargs["storage_options"]["aws_secret_access_key"] == SECRET


def test_an_scd2_write_on_a_connection_hands_the_workers_writer_the_options_decrypted_there(backend, monkeypatch):
    received = []

    def record(df, path, **kwargs):
        received.append((df, path, kwargs))
        return Scd2Result(skipped=True, version=2)

    monkeypatch.setattr("shared.delta_utils.scd2_into_delta", record)
    write = delta_scd2("s3://bucket/dim", s3_connection().get_worker_interface(USER_ID))

    result = backend.run_op(OperationKind.delta_scd2, op_payload(write))

    assert result == OpResult(refs={}, metadata={"skipped": True, "version": 2})
    ((df, path, kwargs),) = received
    assert (df.equals(ROWS), path) == (True, "s3://bucket/dim")
    assert kwargs.pop("storage_options")["aws_secret_access_key"] == SECRET
    assert kwargs == {
        "business_keys": ["id"],
        "valid_from_iso": "2024-05-01T00:00:00+00:00",
        "compare_columns": ["name"],
        "full_snapshot": True,
        "surrogate_key_column": "row_key",
        "valid_from_column": "from_ts",
        "valid_to_column": "to_ts",
        "is_current_column": "live",
        "partition_by": ["name"],
        "partition_on_current": False,
    }


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


def without_a_node_file(node_class: type[CustomNodeBase] = Splitter, **overrides) -> CustomNodePayload:
    """A run of ``node_class`` over the numbers 0 to 99, ``Splitter`` set to split at 89 by its settings."""
    settings_values = None if node_class is Splitter else {}
    return custom_node(node_class, settings_values, node_source=None, class_name=None, **overrides)


def test_a_class_with_no_node_file_runs_here_and_its_outputs_are_held_as_the_workers(backend, kept):
    job = without_a_node_file()

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
    refs = backend.run_op(OperationKind.custom_node, op_payload(without_a_node_file(node_class))).refs

    kept.extend(refs.values())
    assert list(refs) == expected
    if refs:
        assert backend.plan_for(refs["result"]).collect()["n"].to_list()[:3] == [0, 2, 4]


def test_a_dict_without_a_declared_output_is_refused_and_nothing_is_held(backend):
    job = without_a_node_file(output_names=["big", "small", "medium"])

    with pytest.raises(ValueError, match="did not return declared output 'medium'"):
        backend.run_op(OperationKind.custom_node, op_payload(job))

    for i in range(3):
        assert backend.exists(ResultRef("local", f"{job.job_id}_{i}", [], None, "materialised", b"")) is False
