"""The write operations take one payload dataclass under ``"payload"``, and both backends unpack the same one.

The worker side is checked at the fetcher boundary on the task routes and at the request boundary on ``/v1``: the
fetchers, or the backend's ``/v1`` request, are replaced by recorders, so these tests show what each payload becomes on
its way to the worker without one running; a file and a database write also run for real over ``/v1`` on the session
worker. The embedded worker's writes are ``test_local_backend.py``'s, which builds them here, and the payload contract
every kind shares is ``test_payloads.py``'s, which takes ``WRITE_PAYLOADS`` from here.
"""

import base64
import dataclasses
import io
import json
import shutil
import sqlite3
import uuid

import polars as pl
import pytest
import requests
from pydantic import SecretStr

from flowfile_core.configs.settings import WORKER_URL
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends import worker as worker_module
from flowfile_core.flowfile.execution.backends.embedded import EmbeddedWorkerBackend
from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
from flowfile_core.flowfile.execution.errors import (
    ComputeError,
    ComputeUnavailable,
    PlanRejected,
    TaskFailed,
    VersionMismatch,
)
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake
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
from shared.compute_protocol import local_capabilities
from shared.storage_config import storage
from tests.flowfile.conftest import add_test_manual_input, create_test_graph

pytestmark = pytest.mark.core


USER_ID = 1
SECRET = "plain-s3-secret"
ROWS = pl.DataFrame({"id": [1, 2, 3], "name": ["ada", "grace", "edsger"]})


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


def delta_write(
    path: str, connection=None, mode: str = "overwrite", plan: pl.LazyFrame | None = None, enable_cdf: bool = True
) -> DeltaWritePayload:
    return DeltaWritePayload(
        plan=ROWS.lazy() if plan is None else plan,
        path=path,
        mode=mode,
        partition_by=None,
        enable_cdf=enable_cdf,
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


class V1Recorder:
    """Stands in for the worker backend's ``/v1`` request: keeps each one and answers it as a write is answered."""

    def __init__(self):
        self.sent: list[dict] = []
        self.on_op = None

    def request(self, method: str, path: str, **kwargs):
        self.sent.append({"method": method, "path": path, **kwargs})
        if path.startswith("/ops/") and self.on_op is not None:
            self.on_op()
        return self

    def json(self) -> dict:
        return {"outputs": {}, "metadata": {}}


@pytest.fixture
def v1_sent(monkeypatch, protocol_on) -> V1Recorder:
    """The ``/v1`` requests the worker backend sent, in order; no worker is contacted."""
    recorder = V1Recorder()
    monkeypatch.setattr(
        WorkerBackend, "_v1", lambda backend, method, path, **kwargs: recorder.request(method, path, **kwargs)
    )
    return recorder


@pytest.mark.usefixtures("protocol_off")
def test_the_worker_backend_sends_a_file_write_to_the_output_writer(built, tmp_path):
    write = file_write(tmp_path)

    result = WorkerBackend(url=WORKER_URL).run_op(OperationKind.file_write, op_payload(write))

    (fetcher,) = built
    assert fetcher.kwargs == {
        "lf": write.plan,
        "data_type": "csv",
        "path": write.settings.abs_file_path,
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


def referenced_database_write() -> DatabaseWritePayload:
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
    return DatabaseWritePayload(
        plan=ROWS.lazy(),
        settings=node_settings,
        connection=stored,
        encrypted_password=ciphertext,
        table_name="public.people",
        flow_id=7,
        node_id=3,
    )


def file_write(directory=".") -> FileWritePayload:
    return FileWritePayload(plan=ROWS.lazy(), settings=csv_settings(directory, delimiter=";"), flow_id=7, node_id=3)


def cloud_write() -> CloudWritePayload:
    return CloudWritePayload(
        plan=ROWS.lazy(),
        settings=CloudStorageWriteSettings(resource_path="s3://bucket/people.parquet", file_format="parquet"),
        connection=s3_connection().get_worker_interface(USER_ID),
        flow_id=7,
        node_id=3,
    )


WRITE_PAYLOADS = {
    OperationKind.file_write: file_write,
    OperationKind.database_write: referenced_database_write,
    OperationKind.cloud_write: cloud_write,
    OperationKind.delta_write: lambda: delta_write("/tables/t"),
    OperationKind.delta_merge: lambda: delta_merge("/tables/t", ROWS.lazy()),
    OperationKind.delta_scd2: lambda: delta_scd2("/tables/dim"),
}


@pytest.mark.usefixtures("protocol_off")
def test_the_worker_backend_sends_a_referenced_database_write_with_the_stored_ciphertext(built):
    write = referenced_database_write()

    WorkerBackend(url=WORKER_URL).run_op(OperationKind.database_write, op_payload(write))

    (fetcher,) = built
    (sent,) = fetcher.args
    assert isinstance(sent, DatabaseExternalWriteSettings)
    assert (sent.connection.host, sent.connection.username, sent.connection.database) == (
        "db.internal",
        "loader",
        "analytics",
    )
    assert sent.connection.password == write.encrypted_password
    assert (sent.table_name, sent.if_exists) == ("public.people", "replace")
    assert (sent.flowfile_flow_id, sent.flowfile_node_id) == (7, 3)
    assert pl.LazyFrame.deserialize(io.BytesIO(sent.operation)).collect().equals(ROWS)


@pytest.mark.usefixtures("protocol_off")
def test_the_worker_backend_sends_a_cloud_write_with_its_secrets_still_encrypted(built):
    write = cloud_write()

    WorkerBackend(url=WORKER_URL).run_op(OperationKind.cloud_write, op_payload(write))

    (fetcher,) = built
    (sent,) = fetcher.args
    assert isinstance(sent, CloudStorageWriteSettingsWorkerInterface)
    assert sent.connection == write.connection
    assert sent.connection.aws_secret_access_key.startswith("$ffsec$")
    assert sent.write_settings.resource_path == "s3://bucket/people.parquet"
    assert (sent.flowfile_flow_id, sent.flowfile_node_id) == (7, 3)
    assert pl.LazyFrame.deserialize(io.BytesIO(base64.b64decode(sent.operation))).collect().equals(ROWS)


def test_the_worker_backend_posts_a_file_write_to_its_v1_route(v1_sent, built, tmp_path):
    write = file_write(tmp_path)

    result = WorkerBackend(url=WORKER_URL).run_op(OperationKind.file_write, op_payload(write))

    (request,) = v1_sent.sent
    assert (request["method"], request["path"], request["params"]) == (
        "POST",
        "/ops/file_write",
        {"job_id": write.job_id},
    )
    body = request["json"]
    assert pl.LazyFrame.deserialize(io.BytesIO(base64.b64decode(body.pop("operation")))).collect().equals(ROWS)
    assert body == {
        "data_type": "csv",
        "path": write.settings.abs_file_path,
        "write_mode": "overwrite",
        "sheet_name": None,
        "delimiter": ";",
        "compression": None,
        "flowfile_flow_id": 7,
        "flowfile_node_id": 3,
    }
    assert result == OpResult(refs={})
    assert built == []


def test_the_worker_backend_posts_a_referenced_database_write_with_the_stored_ciphertext(v1_sent, built):
    write = referenced_database_write()

    WorkerBackend(url=WORKER_URL).run_op(OperationKind.database_write, op_payload(write))

    (request,) = v1_sent.sent
    assert (request["path"], request["params"]) == ("/ops/database_write", {"job_id": write.job_id})
    sent = DatabaseExternalWriteSettings.model_validate_json(json.dumps(request["json"]))
    assert (sent.connection.host, sent.connection.username, sent.connection.database) == (
        "db.internal",
        "loader",
        "analytics",
    )
    assert sent.connection.password == write.encrypted_password
    assert (sent.table_name, sent.if_exists) == ("public.people", "replace")
    assert (sent.flowfile_flow_id, sent.flowfile_node_id) == (7, 3)
    assert pl.LazyFrame.deserialize(io.BytesIO(sent.operation)).collect().equals(ROWS)
    assert "db-password" not in json.dumps(request["json"])
    assert built == []


def test_the_worker_backend_posts_a_cloud_write_with_its_secrets_still_encrypted(v1_sent, built):
    write = cloud_write()

    WorkerBackend(url=WORKER_URL).run_op(OperationKind.cloud_write, op_payload(write))

    (request,) = v1_sent.sent
    assert (request["path"], request["params"]) == ("/ops/cloud_write", {"job_id": write.job_id})
    body = request["json"]
    assert body["connection"] == write.connection.model_dump(mode="json")
    assert body["connection"]["aws_secret_access_key"].startswith("$ffsec$")
    assert body["write_settings"]["resource_path"] == "s3://bucket/people.parquet"
    assert (body["flowfile_flow_id"], body["flowfile_node_id"]) == (7, 3)
    assert pl.LazyFrame.deserialize(io.BytesIO(base64.b64decode(body["operation"]))).collect().equals(ROWS)
    assert SECRET not in json.dumps(body)
    assert built == []


CREDENTIAL_FIELDS = (
    "aws_access_key_id",
    "aws_secret_access_key",
    "aws_session_token",
    "azure_account_key",
    "azure_client_secret",
    "azure_sas_token",
    "gcs_service_account_key",
)


@pytest.fixture
def backend_reporting(monkeypatch):
    """Make a worker backend on a URL of its own reporting core's capabilities with a ``credential_resolution``.

    Its check runs no probe and reaches no worker; the verdicts are dropped when the test ends.
    """
    made: list[WorkerBackend] = []
    monkeypatch.setattr(handshake, "_run_probe", lambda backend, name, plan: None)

    def make(backend_id: str, credential_resolution: str) -> WorkerBackend:
        backend = WorkerBackend(url=f"http://{backend_id.replace(':', '-')}.invalid", backend_id=backend_id)
        reported = local_capabilities().model_copy(update={"credential_resolution": credential_resolution})
        monkeypatch.setattr(backend, "capabilities", lambda: reported)
        made.append(backend)
        return backend

    yield make
    for backend in made:
        handshake.reset_handshake(backend.url)


def test_a_cloud_write_to_an_ambient_backend_carries_its_connection_without_credentials(
    protocol, backend_reporting, monkeypatch
):
    connection = cloud_write().connection.model_copy(
        update={field: encrypt_secret(f"{field}-value", USER_ID) for field in CREDENTIAL_FIELDS}
    )
    write = dataclasses.replace(cloud_write(), connection=connection)
    ambient = backend_reporting("worker:ambient", "ambient")
    keyed = backend_reporting("worker:keyed", "master_key")

    if not protocol:
        requests_sent = []
        real_send = requests.Session.send

        def recording_send(session, request, **kwargs):
            requests_sent.append(request.url)
            return real_send(session, request, **kwargs)

        monkeypatch.setattr(requests.Session, "send", recording_send)
        with pytest.raises(VersionMismatch) as refused:
            ambient.run_op(OperationKind.cloud_write, op_payload(write))
        assert str(refused.value) == handshake.ambient_refusal("worker:ambient")
        assert requests_sent == []

        fetchers: list[RecordingFetcher] = []
        monkeypatch.setattr(worker_module, "ExternalCloudWriter", lambda *a, **k: RecordingFetcher(fetchers, *a, **k))
        keyed.run_op(OperationKind.cloud_write, op_payload(write))
        (fetcher,) = fetchers
        (keyed_settings,) = fetcher.args
        assert keyed_settings.connection == connection
        return

    recorder = V1Recorder()
    monkeypatch.setattr(
        WorkerBackend,
        "_v1",
        lambda backend, method, path, **kwargs: recorder.request(method, path, backend=backend.id, **kwargs),
    )
    for backend in (ambient, keyed):
        assert handshake.verdict_for(backend).ok
        backend.run_op(OperationKind.cloud_write, op_payload(write))

    bodies = {request["backend"]: request["json"] for request in recorder.sent}
    assert "$ffsec$" not in json.dumps(bodies["worker:ambient"])
    stripped = bodies["worker:ambient"]["connection"]
    assert {field: stripped[field] for field in CREDENTIAL_FIELDS} == dict.fromkeys(CREDENTIAL_FIELDS)
    assert {field: value for field, value in stripped.items() if field not in CREDENTIAL_FIELDS} == {
        field: value for field, value in connection.model_dump(mode="json").items() if field not in CREDENTIAL_FIELDS
    }
    assert (stripped["storage_type"], stripped["auth_method"], stripped["aws_region"], stripped["endpoint_url"]) == (
        "s3",
        "access_key",
        "us-east-1",
        "http://localhost:9000",
    )
    assert bodies["worker:keyed"]["connection"] == connection.model_dump(mode="json")
    assert all(bodies["worker:keyed"]["connection"][field].startswith("$ffsec$") for field in CREDENTIAL_FIELDS)
    assert bodies["worker:ambient"]["write_settings"] == bodies["worker:keyed"]["write_settings"]


@pytest.mark.usefixtures("protocol_on")
def test_a_cloud_write_to_an_ambient_backend_whose_verdict_was_dropped_carries_no_credentials(
    backend_reporting, monkeypatch
):
    """A connection error re-arms the handshake mid-run; the write checks the backend again before building its body."""
    connection = cloud_write().connection.model_copy(
        update={field: encrypt_secret(f"{field}-value", USER_ID) for field in CREDENTIAL_FIELDS}
    )
    write = dataclasses.replace(cloud_write(), connection=connection)
    ambient = backend_reporting("worker:ambient", "ambient")
    assert handshake.verdict_for(ambient).ok
    handshake.reset_handshake(ambient.url)
    assert handshake.cached_verdict(ambient) is None
    recorder = V1Recorder()
    monkeypatch.setattr(
        WorkerBackend,
        "_v1",
        lambda backend, method, path, **kwargs: recorder.request(method, path, backend=backend.id, **kwargs),
    )

    ambient.run_op(OperationKind.cloud_write, op_payload(write))

    (request,) = recorder.sent
    assert "$ffsec$" not in json.dumps(request["json"])
    assert {field: request["json"]["connection"][field] for field in CREDENTIAL_FIELDS} == dict.fromkeys(
        CREDENTIAL_FIELDS
    )


def test_a_cloud_write_to_a_backend_with_no_report_is_refused_when_its_capabilities_cannot_be_read(
    backend_reporting, monkeypatch
):
    """With no cached verdict the write asks the backend; a backend that cannot answer gets nothing, not a guess."""
    write = cloud_write()
    backend = backend_reporting("worker:unreachable", "ambient")
    assert handshake.cached_verdict(backend) is None

    def unreachable():
        raise ComputeUnavailable("Connection refused", backend_id=backend.id)

    monkeypatch.setattr(backend, "capabilities", unreachable)
    recorder = V1Recorder()
    monkeypatch.setattr(
        WorkerBackend,
        "_v1",
        lambda backend, method, path, **kwargs: recorder.request(method, path, backend=backend.id, **kwargs),
    )

    with pytest.raises(ComputeError):
        backend._resolves_ambient()
    with pytest.raises(ComputeError):
        backend.run_op(OperationKind.cloud_write, op_payload(write))
    assert recorder.sent == []


@pytest.mark.usefixtures("protocol_on")
def test_the_session_worker_writes_a_file_over_its_v1_route(tmp_path):
    write = file_write(tmp_path)
    backend = WorkerBackend(url=WORKER_URL)

    result = backend.run_op(OperationKind.file_write, op_payload(write))

    assert result == OpResult(refs={})
    assert pl.read_csv(tmp_path / "people.csv", separator=";").equals(ROWS)
    assert backend.status(JobHandle(backend.id, write.job_id)).state == "done"


@pytest.mark.usefixtures("protocol_on")
def test_the_session_worker_writes_a_database_table_over_its_v1_route(tmp_path):
    write = sqlite_write(tmp_path / "people.db")

    result = WorkerBackend(url=WORKER_URL).run_op(OperationKind.database_write, op_payload(write))

    assert result == OpResult(refs={})
    with sqlite3.connect(tmp_path / "people.db") as db:
        assert db.execute("SELECT id, name FROM people ORDER BY id").fetchall() == list(ROWS.iter_rows())


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


def sqlite_write(database) -> DatabaseWritePayload:
    connection = input_schema.DatabaseConnection(database_type="sqlite", database=f"sqlite:///{database}")
    node_settings = input_schema.NodeDatabaseWriter(
        flow_id=1,
        node_id=2,
        user_id=USER_ID,
        database_write_settings=input_schema.DatabaseWriteSettings(
            database_connection=connection, table_name="people", if_exists="replace"
        ),
    )
    return DatabaseWritePayload(
        plan=ROWS.lazy(),
        settings=node_settings,
        connection=connection,
        encrypted_password=None,
        table_name="people",
        flow_id=1,
        node_id=2,
    )


def test_an_scd2_payload_carries_no_decrypted_secret():
    write = delta_scd2("s3://bucket/dim", s3_connection().get_worker_interface(USER_ID))

    assert SECRET not in write.connection.model_dump_json()


def test_a_cloud_payload_carries_no_decrypted_secret():
    assert SECRET not in cloud_write().connection.model_dump_json()


@pytest.fixture
def node():
    graph = create_test_graph(flow_id=91, execution_location="remote")
    add_test_manual_input(graph, [{"id": 1}], node_id=1)
    return graph.get_node(1)


@pytest.mark.usefixtures("protocol_off")
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


def test_cancelling_a_node_reaches_the_v1_write_job_it_is_waiting_on(node, v1_sent, tmp_path):
    backend = registry.backend(worker_module.WORKER_BACKEND_ID)
    write = FileWritePayload(plan=ROWS.lazy(), settings=csv_settings(tmp_path), flow_id=91, node_id=1)
    seen = []

    def cancel_while_waiting():
        seen.append(node._submit_in_flight)
        node.cancel()

    v1_sent.on_op = cancel_while_waiting

    node.run_op(backend, OperationKind.file_write, op_payload(write), job_id=write.job_id)

    write_request, cancel_request = v1_sent.sent
    assert write_request["path"] == "/ops/file_write"
    assert (cancel_request["method"], cancel_request["path"]) == ("POST", f"/jobs/{write.job_id}/cancel")
    assert seen == [JobHandle(backend.id, write.job_id)]
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
