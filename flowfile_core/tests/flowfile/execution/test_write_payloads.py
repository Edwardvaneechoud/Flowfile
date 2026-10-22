"""The write operations take one payload dataclass under ``"payload"``, and both backends unpack the same one.

The worker side is checked at the fetcher boundary: the fetchers are replaced by recorders, so these tests show
what each payload becomes on its way to the worker without one running. The embedded worker's side writes for real
where the target is the local disk, and records what the worker's writer receives where it would need object storage.
"""

import base64
import dataclasses
import io
import shutil
import sqlite3
import uuid

import polars as pl
import pytest
from pydantic import SecretStr

from flowfile_core.configs.settings import WORKER_URL
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends import worker as worker_module
from flowfile_core.flowfile.execution.backends.embedded import EmbeddedWorkerBackend
from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
from flowfile_core.flowfile.execution.errors import PlanRejected, TaskFailed
from flowfile_core.flowfile.execution.protocol import JobHandle, OperationKind, OpResult
from flowfile_core.flowfile.execution.payloads import (
    op_payload,
    PAYLOAD_KEY,
    CatalogTableIdentity,
    CloudWritePayload,
    DatabaseWritePayload,
    DeltaMergePayload,
    DeltaScd2Payload,
    DeltaWritePayload,
    FileWritePayload,
    unpack,
)
from flowfile_core.flowfile.flow_node.models import RemoteExecutionError
from flowfile_core.flowfile.sources.external_sources.sql_source.models import DatabaseExternalWriteSettings
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.cloud_storage_schemas import (
    CloudStorageWriteSettings,
    CloudStorageWriteSettingsWorkerInterface,
    FullCloudStorageConnection,
)
from flowfile_core.secret_manager.secret_manager import encrypt_secret
from shared.delta_utils import Scd2Result
from shared.storage_config import storage
from tests.flowfile.conftest import add_test_manual_input, create_test_graph

pytestmark = pytest.mark.core


def make(backend_type: type):
    """An instance of ``backend_type``: the worker backend on the session worker, the embedded worker as it is."""
    return backend_type(url=WORKER_URL) if backend_type is WorkerBackend else backend_type()


USER_ID = 1
SECRET = "plain-s3-secret"
ROWS = pl.DataFrame({"id": [1, 2, 3], "name": ["ada", "grace", "edsger"]})
WRITE_KINDS = {
    OperationKind.file_write: FileWritePayload,
    OperationKind.database_write: DatabaseWritePayload,
    OperationKind.cloud_write: CloudWritePayload,
    OperationKind.delta_write: DeltaWritePayload,
    OperationKind.delta_merge: DeltaMergePayload,
    OperationKind.delta_scd2: DeltaScd2Payload,
}


def s3_connection() -> FullCloudStorageConnection:
    return FullCloudStorageConnection(
        connection_name="write-payloads",
        storage_type="s3",
        auth_method="access_key",
        aws_access_key_id="key-id",
        aws_secret_access_key=SecretStr(SECRET),
        aws_region="us-east-1",
        endpoint_url="http://localhost:9000",
    )


def csv_settings(directory, delimiter: str = ",") -> input_schema.OutputSettings:
    return input_schema.OutputSettings(
        name="people.csv",
        directory=str(directory),
        file_type="csv",
        write_mode="overwrite",
        table_settings=input_schema.OutputCsvTable(delimiter=delimiter),
    )


def delta_write(path: str, connection=None, mode: str = "overwrite") -> DeltaWritePayload:
    return DeltaWritePayload(
        plan=ROWS.lazy(),
        path=path,
        mode=mode,
        partition_by=None,
        enable_cdf=True,
        connection=connection,
        flow_id=7,
        node_id=3,
    )


def delta_merge(path: str, plan: pl.LazyFrame, connection=None) -> DeltaMergePayload:
    return DeltaMergePayload(
        plan=plan,
        path=path,
        merge_mode="upsert",
        merge_keys=["id"],
        partition_by=None,
        enable_cdf=False,
        connection=connection,
        flow_id=7,
        node_id=3,
    )


def delta_scd2(path: str, connection=None) -> DeltaScd2Payload:
    return DeltaScd2Payload(
        plan=ROWS.lazy(),
        path=path,
        business_keys=["id"],
        compare_columns=["name"],
        full_snapshot=True,
        surrogate_key_column="row_key",
        valid_from_column="from_ts",
        valid_to_column="to_ts",
        is_current_column="live",
        run_timestamp="2024-05-01T00:00:00+00:00",
        partition_by=["name"],
        partition_on_current=False,
        connection=connection,
        flow_id=7,
        node_id=3,
    )


class RecordingFetcher:
    """Stands in for a worker fetcher: keeps what it was built with and reports a finished task."""

    on_wait = None

    def __init__(self, built: list, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.error_code = 0
        self.error_description = None
        self.error_kind = None
        self.file_ref = "recorded-task"
        self.result = {"row_count": 3}
        self.cancelled = False
        built.append(self)

    def get_result(self):
        if self.on_wait is not None:
            self.on_wait(self)
        return self.result

    def cancel(self):
        self.cancelled = True


@pytest.fixture
def built(monkeypatch) -> list[RecordingFetcher]:
    """The fetchers the worker backend built, in order; no worker is contacted."""
    fetchers: list[RecordingFetcher] = []
    for name in ("ExternalOutputWriter", "ExternalDatabaseWriter", "ExternalCloudWriter", "ExternalDfFetcher"):
        monkeypatch.setattr(worker_module, name, lambda *a, **k: RecordingFetcher(fetchers, *a, **k))
    return fetchers


def test_the_worker_backend_sends_a_file_write_to_the_output_writer(built, tmp_path):
    settings = csv_settings(tmp_path, delimiter=";")
    write = FileWritePayload(plan=ROWS.lazy(), settings=settings, flow_id=7, node_id=3)

    result = WorkerBackend(url=WORKER_URL).run_op(OperationKind.file_write, op_payload(write))

    (fetcher,) = built
    assert fetcher.kwargs == {
        "lf": write.plan,
        "data_type": "csv",
        "path": settings.abs_file_path,
        "write_mode": "overwrite",
        "flow_id": 7,
        "node_id": 3,
        "sheet_name": None,
        "delimiter": ";",
        "compression": None,
        "wait_on_completion": False,
        "worker_url": WORKER_URL,
    }
    assert result == OpResult(refs={})


def test_the_worker_backend_sends_a_referenced_database_write_with_the_stored_ciphertext(built):
    ciphertext = encrypt_secret("db-password", USER_ID)
    stored = input_schema.FullDatabaseConnection(
        connection_name="warehouse",
        database_type="postgresql",
        username="loader",
        password=SecretStr(ciphertext),
        host="db.internal",
        port=5432,
        database="analytics",
    )
    node_settings = input_schema.NodeDatabaseWriter(
        flow_id=1,
        node_id=2,
        user_id=USER_ID,
        database_write_settings=input_schema.DatabaseWriteSettings(
            connection_mode="reference", database_connection_name="warehouse", table_name="people", if_exists="replace"
        ),
    )
    write = DatabaseWritePayload(
        plan=ROWS.lazy(),
        settings=node_settings,
        connection=stored,
        encrypted_password=ciphertext,
        table_name="public.people",
        flow_id=7,
        node_id=3,
    )

    WorkerBackend(url=WORKER_URL).run_op(OperationKind.database_write, op_payload(write))

    (fetcher,) = built
    (sent,) = fetcher.args
    assert isinstance(sent, DatabaseExternalWriteSettings)
    assert (sent.connection.host, sent.connection.username, sent.connection.database) == (
        "db.internal",
        "loader",
        "analytics",
    )
    assert sent.connection.password == ciphertext
    assert (sent.table_name, sent.if_exists) == ("public.people", "replace")
    assert (sent.flowfile_flow_id, sent.flowfile_node_id) == (7, 3)
    assert pl.LazyFrame.deserialize(io.BytesIO(sent.operation)).collect().equals(ROWS)


def test_the_worker_backend_sends_a_cloud_write_with_its_secrets_still_encrypted(built):
    write = CloudWritePayload(
        plan=ROWS.lazy(),
        settings=CloudStorageWriteSettings(resource_path="s3://bucket/people.parquet", file_format="parquet"),
        connection=s3_connection().get_worker_interface(USER_ID),
        flow_id=7,
        node_id=3,
    )

    WorkerBackend(url=WORKER_URL).run_op(OperationKind.cloud_write, op_payload(write))

    (fetcher,) = built
    (sent,) = fetcher.args
    assert isinstance(sent, CloudStorageWriteSettingsWorkerInterface)
    assert sent.connection == write.connection
    assert sent.connection.aws_secret_access_key.startswith("$ffsec$")
    assert sent.write_settings.resource_path == "s3://bucket/people.parquet"
    assert (sent.flowfile_flow_id, sent.flowfile_node_id) == (7, 3)
    assert pl.LazyFrame.deserialize(io.BytesIO(base64.b64decode(sent.operation))).collect().equals(ROWS)


@pytest.mark.parametrize("with_connection", [True, False], ids=["object-storage", "local-table"])
def test_the_worker_backend_runs_the_delta_operations_with_the_encrypted_storage_payload(built, with_connection):
    connection = s3_connection().get_worker_interface(USER_ID) if with_connection else None
    storage_payload = {"connection": connection.model_dump()} if with_connection else None
    backend = WorkerBackend(url=WORKER_URL)

    written = backend.run_op(OperationKind.delta_write, op_payload(delta_write("s3://bucket/t", connection)))
    merged = backend.run_op(
        OperationKind.delta_merge, op_payload(delta_merge("s3://bucket/t", ROWS.lazy(), connection))
    )

    write_task, merge_task = built
    assert write_task.kwargs["operation_type"] == "write_delta"
    assert write_task.kwargs["kwargs"] == {
        "output_path": "s3://bucket/t",
        "mode": "overwrite",
        "partition_by": None,
        "enable_cdf": True,
        "storage_payload": storage_payload,
    }
    assert merge_task.kwargs["operation_type"] == "merge_delta"
    assert merge_task.kwargs["kwargs"] == {
        "output_path": "s3://bucket/t",
        "merge_mode": "upsert",
        "merge_keys": ["id"],
        "partition_by": None,
        "enable_cdf": False,
        "storage_payload": storage_payload,
    }
    assert (write_task.kwargs["flow_id"], write_task.kwargs["node_id"]) == (7, 3)
    assert SECRET not in repr(write_task.kwargs)
    assert written == merged == OpResult(refs={}, metadata={"row_count": 3})


@pytest.mark.parametrize("with_connection", [True, False], ids=["object-storage", "local-table"])
def test_the_worker_backend_runs_an_scd2_write_with_the_encrypted_storage_payload(built, with_connection):
    connection = s3_connection().get_worker_interface(USER_ID) if with_connection else None
    write = delta_scd2("s3://bucket/dim", connection)

    result = WorkerBackend(url=WORKER_URL).run_op(OperationKind.delta_scd2, op_payload(write))

    (task,) = built
    assert task.kwargs["operation_type"] == "scd2_delta"
    assert task.kwargs["lf"] is write.plan
    assert (task.kwargs["flow_id"], task.kwargs["node_id"]) == (7, 3)
    assert task.kwargs["wait_on_completion"] is False
    assert task.kwargs["kwargs"] == {
        "output_path": "s3://bucket/dim",
        "run_timestamp": "2024-05-01T00:00:00+00:00",
        "business_keys": ["id"],
        "compare_columns": ["name"],
        "full_snapshot": True,
        "surrogate_key_column": "row_key",
        "valid_from_column": "from_ts",
        "valid_to_column": "to_ts",
        "is_current_column": "live",
        "partition_by": ["name"],
        "partition_on_current": False,
        "storage_payload": {"connection": connection.model_dump()} if with_connection else None,
    }
    if with_connection:
        sent = task.kwargs["kwargs"]["storage_payload"]["connection"]
        assert sent["aws_secret_access_key"].startswith("$ffsec$")
    assert SECRET not in repr(task.kwargs)
    assert result == OpResult(refs={}, metadata={"row_count": 3})


def test_the_worker_scd2_report_is_the_metadata_unchanged(built, monkeypatch):
    report = {"skipped": True, "version": 4}

    def finish_with_the_report(fetcher):
        fetcher.result = report

    monkeypatch.setattr(RecordingFetcher, "on_wait", staticmethod(finish_with_the_report))

    result = WorkerBackend(url=WORKER_URL).run_op(OperationKind.delta_scd2, op_payload(delta_scd2("/tables/dim")))

    assert result == OpResult(refs={}, metadata={"skipped": True, "version": 4})


def test_cancel_reaches_the_scd2_write_while_it_runs(built, monkeypatch):
    backend = WorkerBackend(url=WORKER_URL)
    write = delta_scd2("/tables/dim")

    def cancel_while_waiting(fetcher):
        backend.cancel(JobHandle(backend.id, write.job_id))

    monkeypatch.setattr(RecordingFetcher, "on_wait", staticmethod(cancel_while_waiting))

    backend.run_op(OperationKind.delta_scd2, op_payload(write))

    (fetcher,) = built
    assert fetcher.cancelled


def test_an_scd2_payload_round_trips_through_unpack():
    write = delta_scd2("/tables/dim")

    assert unpack(op_payload(write), DeltaScd2Payload) is write


def test_an_scd2_payload_takes_its_fields_by_keyword_only():
    with pytest.raises(TypeError, match="positional"):
        DeltaScd2Payload(ROWS.lazy(), "/tables/dim")


def test_an_scd2_payload_is_frozen():
    write = delta_scd2("/tables/dim")

    with pytest.raises(dataclasses.FrozenInstanceError):
        write.path = "/tables/other"


def test_unpack_names_the_scd2_payload_it_expected():
    with pytest.raises(TypeError, match=r"payload\['payload'\] must be a DeltaScd2Payload, not DeltaWritePayload"):
        unpack(op_payload(delta_write("/tables/dim")), DeltaScd2Payload)


def test_the_embedded_worker_writes_a_file_with_the_payload_settings(tmp_path):
    write = FileWritePayload(plan=ROWS.lazy(), settings=csv_settings(tmp_path, delimiter=";"), flow_id=7, node_id=3)

    result = EmbeddedWorkerBackend().run_op(OperationKind.file_write, op_payload(write))

    assert result == OpResult(refs={})
    assert pl.read_csv(tmp_path / "people.csv", separator=";").equals(ROWS)


def test_the_embedded_worker_writes_a_database_table(tmp_path):
    database = tmp_path / "people.db"
    connection = input_schema.DatabaseConnection(database_type="sqlite", database=f"sqlite:///{database}")
    node_settings = input_schema.NodeDatabaseWriter(
        flow_id=1,
        node_id=2,
        user_id=USER_ID,
        database_write_settings=input_schema.DatabaseWriteSettings(
            database_connection=connection, table_name="people", if_exists="replace"
        ),
    )
    write = DatabaseWritePayload(
        plan=ROWS.lazy(),
        settings=node_settings,
        connection=connection,
        encrypted_password=None,
        table_name="people",
        flow_id=1,
        node_id=2,
    )

    EmbeddedWorkerBackend().run_op(OperationKind.database_write, op_payload(write))

    with sqlite3.connect(database) as db:
        assert db.execute("SELECT id, name FROM people ORDER BY id").fetchall() == list(ROWS.iter_rows())


def test_the_embedded_worker_hands_the_workers_writer_a_cloud_write_connection_it_decrypts_there(monkeypatch):
    received = []
    monkeypatch.setattr(
        "flowfile_worker.external_sources.s3_source.main.write_df_to_cloud",
        lambda df, settings, logger: received.append((df, settings)),
    )
    write = CloudWritePayload(
        plan=ROWS.lazy(),
        settings=CloudStorageWriteSettings(resource_path="s3://bucket/people.parquet", file_format="parquet"),
        connection=s3_connection().get_worker_interface(USER_ID),
        flow_id=7,
        node_id=3,
    )

    EmbeddedWorkerBackend().run_op(OperationKind.cloud_write, op_payload(write))

    ((df, settings),) = received
    assert df.collect().equals(ROWS)
    assert settings.write_settings.resource_path == write.settings.resource_path
    assert settings.connection.aws_secret_access_key.get_secret_value().startswith("$ffsec$")
    assert settings.connection.get_storage_options()["aws_secret_access_key"] == SECRET
    assert settings.connection.aws_access_key_id == "key-id"


def test_the_embedded_worker_writes_and_merges_a_local_delta_table(tmp_path):
    path = str(tmp_path / "people")
    backend = EmbeddedWorkerBackend()

    backend.run_op(OperationKind.delta_write, op_payload(delta_write(path)))
    changes = pl.LazyFrame({"id": [3, 4], "name": ["dijkstra", "barbara"]})
    backend.run_op(OperationKind.delta_merge, op_payload(delta_merge(path, changes)))

    assert pl.read_delta(path).sort("id").rows() == [(1, "ada"), (2, "grace"), (3, "dijkstra"), (4, "barbara")]


def test_the_embedded_worker_hands_the_workers_delta_write_the_storage_options_it_decrypts_there(monkeypatch):
    received = []
    monkeypatch.setattr("shared.delta_utils.write_delta", lambda *args, **kwargs: received.append(kwargs) or False)
    connection = s3_connection().get_worker_interface(USER_ID)

    EmbeddedWorkerBackend().run_op(OperationKind.delta_write, op_payload(delta_write("s3://bucket/t", connection)))

    (kwargs,) = received
    assert kwargs["storage_options"]["aws_secret_access_key"] == SECRET


def test_the_embedded_worker_hands_the_workers_scd2_write_the_storage_options_it_decrypts_there(monkeypatch):
    received = []

    def record(rows, path, **kwargs):
        received.append((rows, path, kwargs))
        return Scd2Result(skipped=True, version=2)

    monkeypatch.setattr("shared.delta_utils.scd2_into_delta", record)
    write = delta_scd2("s3://bucket/dim", s3_connection().get_worker_interface(USER_ID))

    result = EmbeddedWorkerBackend().run_op(OperationKind.delta_scd2, op_payload(write))

    ((rows, path, kwargs),) = received
    assert rows.equals(ROWS)
    assert path == "s3://bucket/dim"
    assert kwargs["storage_options"]["aws_secret_access_key"] == SECRET
    assert {name: value for name, value in kwargs.items() if name != "storage_options"} == {
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
    assert result == OpResult(refs={}, metadata={"skipped": True, "version": 2})


def test_an_scd2_payload_carries_no_decrypted_secret():
    write = delta_scd2("s3://bucket/dim", s3_connection().get_worker_interface(USER_ID))

    assert SECRET not in write.connection.model_dump_json()


def test_a_cloud_payload_carries_no_decrypted_secret():
    write = CloudWritePayload(
        plan=ROWS.lazy(),
        settings=CloudStorageWriteSettings(resource_path="s3://bucket/people.parquet"),
        connection=s3_connection().get_worker_interface(USER_ID),
        flow_id=7,
        node_id=3,
    )

    assert SECRET not in write.connection.model_dump_json()


@pytest.mark.parametrize("backend_type", [EmbeddedWorkerBackend, WorkerBackend])
@pytest.mark.parametrize("kind", list(WRITE_KINDS), ids=lambda kind: kind.value)
def test_a_write_without_its_payload_key_names_the_key(backend_type, kind, built):
    with pytest.raises(ValueError, match=r"has no 'payload' key; it carries a \w+Payload"):
        make(backend_type).run_op(kind, {"input": ROWS.lazy()})
    assert built == []


@pytest.mark.parametrize("backend_type", [EmbeddedWorkerBackend, WorkerBackend])
def test_a_write_given_another_kind_of_payload_names_the_one_it_expects(backend_type, built, tmp_path):
    wrong = FileWritePayload(plan=ROWS.lazy(), settings=csv_settings(tmp_path), flow_id=1, node_id=1)

    with pytest.raises(TypeError, match=r"payload\['payload'\] must be a DeltaWritePayload, not FileWritePayload"):
        make(backend_type).run_op(OperationKind.delta_write, op_payload(wrong))
    assert built == []


@pytest.fixture
def node():
    graph = create_test_graph(flow_id=91, execution_location="remote")
    add_test_manual_input(graph, [{"id": 1}], node_id=1)
    return graph.get_node(1)


def test_cancelling_a_node_reaches_the_write_it_is_waiting_on(node, built, tmp_path, monkeypatch):
    backend = registry.backend(worker_module.WORKER_BACKEND_ID)
    write = FileWritePayload(plan=ROWS.lazy(), settings=csv_settings(tmp_path), flow_id=91, node_id=1)
    seen = []

    def cancel_while_waiting(fetcher):
        seen.append(node._submit_in_flight)
        node.cancel()

    monkeypatch.setattr(RecordingFetcher, "on_wait", staticmethod(cancel_while_waiting))

    node.run_op(backend, OperationKind.file_write, op_payload(write), job_id=write.job_id)

    (fetcher,) = built
    assert seen == [JobHandle(backend.id, write.job_id)]
    assert fetcher.cancelled
    assert node._submit_in_flight is None


class FailingBackend:
    id = "failing"

    def __init__(self, error: Exception):
        self.error = error

    def run_op(self, op, payload):
        raise self.error


def test_a_failed_write_that_names_its_class_reaches_the_node_as_that_class(node):
    with pytest.raises(RemoteExecutionError) as raised:
        node.run_op(FailingBackend(TaskFailed("ValueError: bad row")), OperationKind.file_write, {}, job_id="j")

    assert raised.value.original_class == "ValueError"
    assert str(raised.value) == "ValueError: bad row"
    assert node._submit_in_flight is None


@pytest.mark.parametrize("error", [TaskFailed("the task died"), PlanRejected("cannot load the plan")])
def test_other_write_failures_propagate_as_raised(node, error):
    with pytest.raises(type(error)) as raised:
        node.run_op(FailingBackend(error), OperationKind.file_write, {}, job_id="j")

    assert raised.value is error


# A catalog table named by identity


IDENTITY_KINDS = {
    "write": (OperationKind.delta_write, lambda connection: delta_write("unused", connection)),
    "merge": (OperationKind.delta_merge, lambda connection: delta_merge("unused", ROWS.lazy(), connection)),
    "scd2": (OperationKind.delta_scd2, lambda connection: delta_scd2("unused", connection)),
}


def by_identity(write, table: CatalogTableIdentity):
    return dataclasses.replace(write, path=None, table=table)


@pytest.mark.parametrize("kind", IDENTITY_KINDS)
@pytest.mark.parametrize("with_connection", [True, False], ids=["object-storage", "local-table"])
def test_the_worker_backend_sends_a_catalog_table_by_identity_and_no_path(built, kind, with_connection):
    operation, make_write = IDENTITY_KINDS[kind]
    connection = s3_connection().get_worker_interface(USER_ID) if with_connection else None
    base_uri = "s3://bucket/catalog" if with_connection else None
    write = by_identity(
        make_write(connection), CatalogTableIdentity(table_id=12, table_name="dim_ab12", base_uri=base_uri)
    )

    WorkerBackend(url=WORKER_URL).run_op(operation, op_payload(write))

    (task,) = built
    sent = task.kwargs["kwargs"]
    assert "output_path" not in sent
    assert (sent["table_id"], sent["table_name"]) == (12, "dim_ab12")
    expected = {"base_uri": base_uri, "connection": connection.model_dump()} if with_connection else None
    assert sent["storage_payload"] == expected
    assert not any(isinstance(value, str) and value.startswith("/") for value in sent.values())
    assert SECRET not in repr(task.kwargs)


@pytest.mark.parametrize("kind", IDENTITY_KINDS)
def test_the_embedded_worker_writes_a_catalog_table_by_identity_under_its_catalog_directory(kind):
    operation, make_write = IDENTITY_KINDS[kind]
    name = f"identity_{uuid.uuid4().hex[:8]}"
    expected = str(storage.catalog_tables_directory / name)
    write = by_identity(make_write(None), CatalogTableIdentity(table_id=None, table_name=name, base_uri=None))
    try:
        result = EmbeddedWorkerBackend().run_op(operation, op_payload(write))

        assert result.metadata["table_path"] == expected
        assert pl.read_delta(expected).height == 3
    finally:
        shutil.rmtree(expected, ignore_errors=True)


def test_the_embedded_worker_resolves_an_object_storage_identity_under_its_root(monkeypatch):
    received = []
    monkeypatch.setattr("shared.delta_utils.write_delta", lambda df, path, **kwargs: received.append(path) or False)
    write = by_identity(
        delta_write("unused", s3_connection().get_worker_interface(USER_ID)),
        CatalogTableIdentity(table_id=3, table_name="dim_ab12", base_uri="s3://bucket/catalog"),
    )

    EmbeddedWorkerBackend().run_op(OperationKind.delta_write, op_payload(write))

    assert received == ["s3://bucket/catalog/dim_ab12"]


def test_an_identity_names_a_directory_and_never_a_path():
    write = by_identity(
        delta_write("unused"), CatalogTableIdentity(table_id=None, table_name="../escape", base_uri=None)
    )

    with pytest.raises(TaskFailed, match="path separators"):
        EmbeddedWorkerBackend().run_op(OperationKind.delta_write, op_payload(write))
