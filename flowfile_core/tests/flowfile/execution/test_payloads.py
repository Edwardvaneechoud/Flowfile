"""Each read kind takes one payload dataclass, which both backends unpack into what their transport needs.

The worker side runs against stand-in fetchers that write the task's Arrow file, so nothing here needs a worker,
a database server, a broker or the network.
"""

import sqlite3
import threading
import uuid
from types import SimpleNamespace

import polars as pl
import pytest
from pydantic import SecretStr

from flowfile_core.flowfile.execution.backends import worker as worker_module
from flowfile_core.flowfile.execution.backends.local import LocalBackend
from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
from flowfile_core.flowfile.execution.errors import OperationUnsupported, TaskFailed
from flowfile_core.flowfile.execution.payloads import (
    PAYLOAD_KEY,
    DatabaseReadPayload,
    FileReadPayload,
    GoogleAnalyticsReadPayload,
    KafkaReadPayload,
    RestApiReadPayload,
    op_payload,
    unpack,
)
from flowfile_core.flowfile.execution.protocol import JobHandle, OperationKind
from flowfile_core.flowfile.sources.external_sources.sql_source.models import DatabaseExternalReadSettings
from flowfile_core.flowfile.sources.external_sources.sql_source.sql_source import BaseSqlSource
from flowfile_core.schemas import input_schema
from flowfile_core.secret_manager.secret_manager import decrypt_secret, encrypt_secret
from shared.google_analytics.models import GoogleAnalyticsReadSettings
from shared.kafka.models import KafkaReadResult, KafkaReadSettings
from shared.path_utils import CloudPathNotSupportedError, DirectoryScanUnsupportedError
from shared.rest_api.models import RestApiReadSettings

ROWS = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
FLOW_ID, NODE_ID = 7, 3


def encrypted(value: str) -> str:
    return encrypt_secret(value, user_id=1)


def database_payload(connection=None, connection_mode: str = "inline") -> DatabaseReadPayload:
    connection = connection or input_schema.DatabaseConnection(
        database_type="postgresql", username="reader", host="db.internal", port=5432, database="shop"
    )
    database_settings = input_schema.DatabaseSettings(
        connection_mode=connection_mode,
        database_connection=connection if connection_mode == "inline" else None,
        database_connection_name="stored" if connection_mode == "reference" else None,
        schema_name="public",
        table_name="movies",
    )
    return DatabaseReadPayload(
        flow_id=FLOW_ID,
        node_id=NODE_ID,
        settings=input_schema.NodeDatabaseReader(flow_id=1, node_id=1, database_settings=database_settings),
        connection=connection,
        encrypted_password=encrypted("db-pass"),
        cancel_check=lambda: False,
    )


def _never_consumed_in_core(*args, **kwargs):
    pytest.fail("a worker read never consumes in core")


def kafka_payload(read_kafka_source=None) -> KafkaReadPayload:
    return KafkaReadPayload(
        flow_id=FLOW_ID,
        node_id=NODE_ID,
        settings=KafkaReadSettings(
            bootstrap_servers="broker:9092", topic="orders", sasl_username="u", sasl_password=encrypted("kafka-pass")
        ),
        read_kafka_source=read_kafka_source or _never_consumed_in_core,
    )


def google_analytics_payload(auth_method: str = "service_account") -> GoogleAnalyticsReadPayload:
    settings = input_schema.NodeGoogleAnalyticsReader(
        flow_id=1,
        node_id=1,
        google_analytics_settings=input_schema.GoogleAnalyticsSettings(
            ga_connection_name="ga",
            property_id="123",
            metrics=["sessions"],
            dimensions=["date", "country"],
            limit=50,
            filters=[input_schema.GoogleAnalyticsFilter(field="country", operator="EXACT", value="NL")],
            order_bys=[input_schema.GoogleAnalyticsOrderBy(field="sessions", descending=True)],
        ),
    )
    if auth_method == "service_account":
        return GoogleAnalyticsReadPayload(
            flow_id=FLOW_ID,
            node_id=NODE_ID,
            settings=settings,
            auth_method="service_account",
            encrypted_credential=encrypted("{sa key}"),
        )
    return GoogleAnalyticsReadPayload(
        flow_id=FLOW_ID,
        node_id=NODE_ID,
        settings=settings,
        auth_method="oauth",
        encrypted_credential=encrypted("refresh-token"),
        oauth_client_id="client-id",
        oauth_client_secret_encrypted="client-secret-ciphertext",
    )


def rest_payload() -> RestApiReadPayload:
    settings = input_schema.NodeRestApiReader(
        flow_id=1,
        node_id=1,
        rest_api_settings=input_schema.RestApiSettings(
            url="https://api.example.com/items", auth=input_schema.RestApiAuthSettings(auth_type="bearer")
        ),
    )
    return RestApiReadPayload(flow_id=FLOW_ID, node_id=NODE_ID, settings=settings, encrypted_secret=encrypted("tok"))


def file_payload(path, file_type: str = "csv", **received) -> FileReadPayload:
    received_file = input_schema.ReceivedTable(file_type=file_type, name="people", path=str(path), **received)
    received_file.set_absolute_filepath()
    return FileReadPayload(
        flow_id=FLOW_ID,
        node_id=NODE_ID,
        settings=input_schema.NodeRead(flow_id=1, node_id=1, received_file=received_file),
        node_logger=None,
    )


READ_PAYLOADS = {
    OperationKind.database_read: database_payload,
    OperationKind.kafka_read: kafka_payload,
    OperationKind.google_analytics_read: google_analytics_payload,
    OperationKind.rest_api_read: rest_payload,
    OperationKind.file_read: lambda: file_payload("people.csv"),
}


@pytest.fixture
def fetchers(monkeypatch, tmp_path):
    """Stand-in worker fetchers: each records what it was handed and writes ``ROWS`` as the task's Arrow file."""
    made: list = []

    class Fetcher:
        error_code, error_description, error_kind = 0, None, None

        def __init__(self, *args, **kwargs):
            self.args, self.kwargs = args, kwargs
            self.file_ref = uuid.uuid4().hex
            path = tmp_path / f"{self.file_ref}.arrow"
            ROWS.write_ipc(path)
            self.status = SimpleNamespace(file_ref=str(path), number_of_records=ROWS.height)
            self.cancelled = False
            made.append(self)

        def get_result(self):
            return pl.scan_ipc(self.status.file_ref)

        def cancel(self):
            self.cancelled = True

    for name in (
        "ExternalDatabaseFetcher",
        "ExternalKafkaFetcher",
        "ExternalGoogleAnalyticsFetcher",
        "ExternalRestApiFetcher",
        "ExternalCreateFetcher",
    ):
        monkeypatch.setattr(worker_module, name, Fetcher)
    monkeypatch.setattr(worker_module, "fetch_kafka_offsets", lambda task_id: None)
    return SimpleNamespace(cls=Fetcher, made=made)


def test_an_op_payload_carries_the_dataclass_under_one_key():
    read = rest_payload()

    assert op_payload(read) == {PAYLOAD_KEY: read}
    assert unpack(op_payload(read), RestApiReadPayload) is read


def test_every_payload_gets_its_own_job_id():
    assert rest_payload().job_id != rest_payload().job_id


def test_the_worker_reads_a_database_with_the_password_still_encrypted(fetchers):
    read = database_payload()

    result = WorkerBackend().run_op(OperationKind.database_read, op_payload(read))

    [fetcher] = fetchers.made
    sent = fetcher.args[0]
    assert isinstance(sent, DatabaseExternalReadSettings)
    assert sent.connection.password == read.encrypted_password
    assert sent.connection.host == "db.internal"
    assert sent.query == BaseSqlSource(table_name="movies", schema_name="public").query
    assert (sent.flowfile_flow_id, sent.flowfile_node_id) == (FLOW_ID, NODE_ID)
    ref = result.refs["result"]
    assert ref.state == "materialised"
    assert ref.row_count == 3
    assert result.metadata == {}


def test_the_worker_reads_a_stored_connection_with_the_payloads_password():
    stored = input_schema.FullDatabaseConnection(
        connection_name="stored", username="reader", password=SecretStr("never sent"), host="db.internal", port=5432
    )
    read = database_payload(connection=stored, connection_mode="reference")

    sent = worker_module._database_read_settings(read)

    assert sent.connection.password == read.encrypted_password
    assert sent.connection.username == "reader"


def test_the_worker_reads_kafka_with_its_secrets_encrypted_and_reports_the_offsets(fetchers, monkeypatch):
    read = kafka_payload()
    offsets_asked = []

    def offsets(task_id):
        offsets_asked.append(task_id)
        return {"new_offsets": {"0": 12}, "messages_consumed": 4}

    monkeypatch.setattr(worker_module, "fetch_kafka_offsets", offsets)

    result = WorkerBackend().run_op(OperationKind.kafka_read, op_payload(read))

    [fetcher] = fetchers.made
    sent = fetcher.args[0]
    assert sent.sasl_password == read.settings.sasl_password
    assert sent.topic == "orders"
    assert (sent.flowfile_flow_id, sent.flowfile_node_id) == (FLOW_ID, NODE_ID)
    assert offsets_asked == [fetcher.file_ref]
    assert result.metadata == {"offsets": {"0": 12}}


@pytest.mark.parametrize("answer", [None, {"new_offsets": {}, "messages_consumed": 0}], ids=["none", "empty"])
def test_a_worker_kafka_read_that_consumed_nothing_reports_no_offsets(fetchers, monkeypatch, answer):
    monkeypatch.setattr(worker_module, "fetch_kafka_offsets", lambda task_id: answer)

    result = WorkerBackend().run_op(OperationKind.kafka_read, op_payload(kafka_payload()))

    assert result.metadata == {}


def test_the_worker_signs_in_to_google_analytics_with_a_service_account():
    read = google_analytics_payload("service_account")

    sent = worker_module._google_analytics_read_settings(read)

    assert isinstance(sent, GoogleAnalyticsReadSettings)
    assert sent.auth_method == "service_account"
    assert sent.service_account_key_encrypted == read.encrypted_credential
    assert sent.refresh_token_encrypted is None
    assert sent.oauth_client_id is None
    assert sent.oauth_client_secret_encrypted is None
    assert (sent.property_id, sent.metrics, sent.dimensions, sent.limit) == (
        "123",
        ["sessions"],
        ["date", "country"],
        50,
    )
    assert [(f.field, f.operator, f.value) for f in sent.filters] == [("country", "EXACT", "NL")]
    assert [(o.field, o.descending) for o in sent.order_bys] == [("sessions", True)]
    assert (sent.flowfile_flow_id, sent.flowfile_node_id) == (FLOW_ID, NODE_ID)


def test_the_worker_signs_in_to_google_analytics_with_oauth(fetchers):
    read = google_analytics_payload("oauth")

    WorkerBackend().run_op(OperationKind.google_analytics_read, op_payload(read))

    [fetcher] = fetchers.made
    sent = fetcher.args[0]
    assert sent.auth_method == "oauth"
    assert sent.refresh_token_encrypted == read.encrypted_credential
    assert (sent.oauth_client_id, sent.oauth_client_secret_encrypted) == ("client-id", "client-secret-ciphertext")
    assert sent.service_account_key_encrypted is None


def test_the_worker_routes_the_rest_secret_by_auth_type(fetchers):
    read = rest_payload()

    WorkerBackend().run_op(OperationKind.rest_api_read, op_payload(read))

    [fetcher] = fetchers.made
    sent = fetcher.args[0]
    assert isinstance(sent, RestApiReadSettings)
    assert sent.url == "https://api.example.com/items"
    assert sent.auth.bearer_token_encrypted == read.encrypted_secret
    assert (sent.flowfile_flow_id, sent.flowfile_node_id) == (FLOW_ID, NODE_ID)


def test_the_worker_reads_a_file_through_the_create_table_task(fetchers, tmp_path):
    read = file_payload(tmp_path / "people.csv")

    result = WorkerBackend().run_op(OperationKind.file_read, op_payload(read))

    [fetcher] = fetchers.made
    assert fetcher.kwargs["received_table"] is read.settings.received_file
    assert (fetcher.kwargs["flow_id"], fetcher.kwargs["node_id"], fetcher.kwargs["file_type"]) == (
        FLOW_ID,
        NODE_ID,
        "csv",
    )
    assert result.refs["result"].row_count == 3


def test_the_worker_refuses_a_cloud_path_and_a_directory_before_a_task_starts(fetchers, tmp_path):
    with pytest.raises(CloudPathNotSupportedError):
        WorkerBackend().run_op(OperationKind.file_read, op_payload(file_payload("s3://bucket/people.xlsx", "excel")))
    with pytest.raises(DirectoryScanUnsupportedError):
        WorkerBackend().run_op(
            OperationKind.file_read, op_payload(file_payload(tmp_path, "excel", scan_mode="directory"))
        )
    assert fetchers.made == []


def test_a_cancel_reaches_a_worker_read_by_its_job_id(fetchers, monkeypatch):
    waiting = threading.Event()

    class BlockingFetcher(fetchers.cls):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.stopped = threading.Event()

        def get_result(self):
            waiting.set()
            self.stopped.wait(10)
            raise RuntimeError("Task cancelled")

        def cancel(self):
            super().cancel()
            self.stopped.set()

    monkeypatch.setattr(worker_module, "ExternalRestApiFetcher", BlockingFetcher)
    backend, read = WorkerBackend(), rest_payload()
    raised = []

    def run():
        try:
            backend.run_op(OperationKind.rest_api_read, op_payload(read))
        except TaskFailed as e:
            raised.append(e)

    thread = threading.Thread(target=run)
    thread.start()
    assert waiting.wait(10)
    backend.cancel(JobHandle(backend.id, read.job_id))
    thread.join(10)

    [fetcher] = fetchers.made
    assert fetcher.cancelled
    assert [str(e) for e in raised] == ["Task cancelled"]


def test_the_local_backend_reads_a_database_and_holds_the_rows_as_a_plan(tmp_path):
    path = tmp_path / "shop.db"
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE movies (id INTEGER, title TEXT)")
        db.executemany("INSERT INTO movies VALUES (?, ?)", [(1, "a"), (2, "b")])
    connection = input_schema.DatabaseConnection(database_type="sqlite", database=f"sqlite:///{path}")
    read = DatabaseReadPayload(
        flow_id=FLOW_ID,
        node_id=NODE_ID,
        settings=input_schema.NodeDatabaseReader(
            flow_id=1,
            node_id=1,
            database_settings=input_schema.DatabaseSettings(database_connection=connection, table_name="movies"),
        ),
        connection=connection,
        encrypted_password=None,
        cancel_check=lambda: False,
    )
    backend = LocalBackend()

    ref = backend.run_op(OperationKind.database_read, op_payload(read)).refs["result"]
    plan = backend.plan_for(ref)
    backend.release(ref)

    assert ref.state == "deferred"
    assert ref.row_count == 2
    assert not backend.exists(ref)
    assert plan.collect().to_dicts() == [{"id": 1, "title": "a"}, {"id": 2, "title": "b"}]


@pytest.mark.parametrize("consumed", [3, 0])
def test_the_local_backend_consumes_kafka_through_the_payloads_hook(consumed):
    calls = []

    def read_kafka_source(settings, **kwargs):
        calls.append((settings, kwargs))
        return ROWS.lazy(), KafkaReadResult(new_offsets={0: 9}, messages_consumed=consumed, partitions_read=1)

    read = kafka_payload(read_kafka_source)
    backend = LocalBackend()

    result = backend.run_op(OperationKind.kafka_read, op_payload(read))

    [(settings, kwargs)] = calls
    assert settings is read.settings
    assert kwargs["commit"] is False
    assert kwargs["spill_path"].endswith(".arrow")
    assert kwargs["decrypt_fn"](read.settings.sasl_password) == "kafka-pass"
    assert result.metadata == ({"offsets": {0: 9}} if consumed else {})
    assert backend.plan_for(result.refs["result"]).collect().equals(ROWS)


def test_the_local_backend_fetches_rest_with_the_secret_decrypted_here(monkeypatch):
    captured = {}

    def fetch(settings, *, secret=None):
        captured.update(settings=settings, secret=secret)
        return ROWS

    monkeypatch.setattr("shared.rest_api.fetch.fetch_rest_api", fetch)
    read = rest_payload()

    ref = LocalBackend().run_op(OperationKind.rest_api_read, op_payload(read)).refs["result"]

    assert captured["secret"] == "tok"
    assert captured["settings"].auth.bearer_token_encrypted == read.encrypted_secret
    assert (ref.state, ref.row_count) == ("deferred", 3)


def test_the_local_backend_reads_a_file(tmp_path):
    path = tmp_path / "people.csv"
    ROWS.write_csv(path)
    backend = LocalBackend()

    ref = backend.run_op(OperationKind.file_read, op_payload(file_payload(path))).refs["result"]

    assert ref.state == "deferred"
    assert backend.plan_for(ref).collect().equals(ROWS)


def test_the_local_backend_has_no_google_analytics_reader():
    with pytest.raises(OperationUnsupported, match="google_analytics_read"):
        LocalBackend().run_op(OperationKind.google_analytics_read, op_payload(google_analytics_payload()))


BACKEND_KINDS = [
    *[(WorkerBackend, kind) for kind in READ_PAYLOADS],
    *[(LocalBackend, kind) for kind in READ_PAYLOADS if kind is not OperationKind.google_analytics_read],
]


@pytest.mark.parametrize(
    ("backend_class", "kind"), BACKEND_KINDS, ids=lambda value: getattr(value, "value", getattr(value, "__name__", ""))
)
def test_a_malformed_payload_is_refused_naming_the_key(fetchers, backend_class, kind):
    backend = backend_class()
    right = READ_PAYLOADS[kind]()
    wrong_kind = rest_payload() if kind is not OperationKind.rest_api_read else database_payload()

    with pytest.raises(ValueError, match="'payload'"):
        backend.run_op(kind, {})
    with pytest.raises(ValueError, match="'payload'"):
        backend.run_op(kind, {"settings": right.settings})
    with pytest.raises(ValueError, match="settings"):
        backend.run_op(kind, {PAYLOAD_KEY: right, "settings": right.settings})
    with pytest.raises(TypeError, match=f"payload\\['payload'\\] must be a {type(right).__name__}"):
        backend.run_op(kind, {PAYLOAD_KEY: wrong_kind})
    with pytest.raises(TypeError, match="'payload'"):
        backend.run_op(kind, [right])
    assert fetchers.made == []


def test_unpack_reads_the_password_back_as_stored():
    read = unpack(op_payload(database_payload()), DatabaseReadPayload)

    assert decrypt_secret(read.encrypted_password).get_secret_value() == "db-pass"
