"""A server joined to its hub with keys of its own refuses, before the run, a step on another server of the hub whose
stored database, cloud storage, Kafka or Google Analytics connection holds this server's secret.

Such a connection keeps its secret in this server's database under this server's master key, which no other server of
the hub has. The step is refused with one sentence that names it and the connection, nothing starts and nothing is
asked of the hub's vend; the placement preview shows the same refusal. The same step on this server's own worker, a
connection with no secret, and a desktop are placed as before.
"""

import sqlite3
import uuid

import pytest
from pydantic import SecretStr

from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import ConfiguredBackend
from flowfile_core.flowfile.database_connection_manager import ga_connections
from flowfile_core.flowfile.database_connection_manager.db_connections import (
    store_cloud_connection,
    store_database_connection,
)
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.placement import held_on_this_server
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake
from flowfile_core.hub_client import client, node_tokens, session
from flowfile_core.kafka.connection_manager import store_kafka_connection
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.cloud_storage_schemas import CloudStorageReadSettings, FullCloudStorageConnection
from flowfile_core.schemas.kafka_schemas import KafkaConnectionCreate
from shared import hub_node
from shared.tests import hub_join
from tests.conftest import closed_url
from tests.flowfile.execution.graph_helpers import add_promise, make_graph
from tests.hub_client.conftest import FakeHub, compute_node, serve_fake, sign_in_through_hub, store  # noqa: F401

pytestmark = pytest.mark.usefixtures("protocol_on")
join_hub = hub_join.join_hub

HUB_USER_ID = 47
NODE_ID = 3
TEAM = "worker:team"


def sentence(node_id: int, kind: str, name: str, backend_id: str = TEAM) -> str:
    return (
        f'cannot run on backend "{backend_id}": node {node_id} uses the {kind} connection "{name}", whose secret this '
        'server keeps and the hub\'s server "team" cannot open, so the step can run on this server, or the connection '
        "must be on the hub"
    )


@pytest.fixture(autouse=True)
def clean(monkeypatch):
    registry.reset()
    handshake.reset_handshake()
    monkeypatch.setattr(node_tokens, "_tokens", {})
    monkeypatch.setattr(client, "_module_http", None)
    yield
    with get_db_context() as db:
        for row in db.query(ConfiguredBackend).all():
            db.delete(row)
        db.commit()
    registry.reset()
    handshake.reset_handshake()


@pytest.fixture
def no_store(monkeypatch):
    from flowfile_core.configs import exchange_config

    for name in (exchange_config.URI_VAR, exchange_config.CONNECTION_VAR):
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def joined(tmp_path, monkeypatch, join_hub):
    """This storage dir joined ``join_hub`` with keys of its own."""
    for name in ("FLOWFILE_HUB_JOIN_TOKEN", "FLOWFILE_HUB_URL", "FLOWFILE_NODE_NAME"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(hub_node, "path", lambda: tmp_path / "storage" / hub_node.FILE_NAME)
    hub_node.join(join_hub.url, join_hub.mint(enable_on_join=True), name="core-1", worker_url="http://core-1:63579")


@pytest.fixture
def hub(serve_fake, store):  # noqa: F811
    """A hub listing ``team`` at an address nothing listens on, a person signed in to it, and the hub-filled row."""
    fake = FakeHub()
    fake.url = serve_fake(fake)
    address = closed_url()
    fake.nodes = [compute_node(NODE_ID, "team", worker_url=address)]
    tokens = client.sign_in(fake.url, "ana", "s3cret")
    stored = {"hub_url": fake.url, "refresh_token": tokens["refresh_token"], "device_id": 1, "device_name": "core-1"}
    session.save(HUB_USER_ID, stored)
    with get_db_context() as db:
        db.add(ConfiguredBackend(id=TEAM, url=address, hub_node_id=NODE_ID, hub_user_id=HUB_USER_ID))
        db.commit()
    fake.calls.clear()
    return fake


@pytest.fixture
def user_id() -> int:
    with get_db_context() as db:
        return db.query(db_models.User).filter(db_models.User.username == "local_user").one().id


@pytest.fixture
def connections(user_id):
    """A cloud storage connection with an access key, one with ``env_vars``, a Kafka connection with a SASL password
    and a Google Analytics connection, all of ``local_user``'s on this server; removed afterwards."""
    suffix = uuid.uuid4().hex[:8]
    names = {kind: f"{kind}_{suffix}" for kind in ("s3", "ambient", "kafka", "ga")}
    with get_db_context() as db:
        store_cloud_connection(
            db,
            FullCloudStorageConnection(
                connection_name=names["s3"],
                storage_type="s3",
                auth_method="access_key",
                aws_region="eu-west-1",
                aws_access_key_id="AKIAEXAMPLE",
                aws_secret_access_key=SecretStr("kept-here"),
            ),
            user_id,
        )
        store_cloud_connection(
            db,
            FullCloudStorageConnection(connection_name=names["ambient"], storage_type="s3", auth_method="env_vars"),
            user_id,
        )
        kafka = store_kafka_connection(
            db,
            KafkaConnectionCreate(
                connection_name=names["kafka"],
                bootstrap_servers="broker:9092",
                security_protocol="SASL_SSL",
                sasl_mechanism="PLAIN",
                sasl_username="svc",
                sasl_password=SecretStr("sasl-pw"),
            ),
            user_id,
        )
        ga_connections.upsert_ga_connection_with_refresh_token(
            db, connection_name=names["ga"], user_id=user_id, refresh_token="refresh-kept-here", oauth_user_email=None
        )
    names["kafka_id"] = kafka.id
    yield names
    with get_db_context() as db:
        rows = [
            *db.query(db_models.CloudStorageConnection).filter(
                db_models.CloudStorageConnection.connection_name.in_([names["s3"], names["ambient"]])
            ),
            *db.query(db_models.KafkaConnection).filter(db_models.KafkaConnection.connection_name == names["kafka"]),
            *db.query(db_models.GoogleAnalyticsConnection).filter(
                db_models.GoogleAnalyticsConnection.connection_name == names["ga"]
            ),
        ]
        secret_ids = [
            getattr(row, column)
            for row in rows
            for column in ("aws_secret_access_key_id", "sasl_password_id", "credential_secret_id")
            if getattr(row, column, None) is not None
        ]
        for row in rows:
            db.delete(row)
        db.flush()
        db.query(db_models.Secret).filter(db_models.Secret.id.in_(secret_ids)).delete()
        db.commit()


def add_cloud_reader(graph, node_id: int, user_id: int, connection: str) -> None:
    add_promise(graph, node_id, "cloud_storage_reader")
    graph.add_cloud_storage_reader(
        input_schema.NodeCloudStorageReader(
            flow_id=graph.flow_id,
            node_id=node_id,
            user_id=user_id,
            cloud_storage_settings=CloudStorageReadSettings(
                resource_path="s3://bucket/data.parquet", file_format="parquet", connection_name=connection
            ),
        )
    )


def add_kafka_source(graph, node_id: int, user_id: int, connection_id: int) -> None:
    add_promise(graph, node_id, "kafka_source")
    graph.add_kafka_source(
        input_schema.NodeKafkaSource(
            flow_id=graph.flow_id,
            node_id=node_id,
            user_id=user_id,
            kafka_settings=input_schema.KafkaSourceSettings(kafka_connection_id=connection_id, topic_name="events"),
        )
    )


def add_ga_reader(graph, node_id: int, user_id: int, connection: str) -> None:
    add_promise(graph, node_id, "google_analytics_reader")
    graph.add_google_analytics_reader(
        input_schema.NodeGoogleAnalyticsReader(
            flow_id=graph.flow_id,
            node_id=node_id,
            user_id=user_id,
            google_analytics_settings=input_schema.GoogleAnalyticsSettings(
                ga_connection_name=connection, property_id="123456", metrics=["activeUsers"]
            ),
        )
    )


@pytest.mark.parametrize("kind", ["cloud storage", "Kafka"])
def test_a_step_on_a_hub_server_whose_connection_holds_this_servers_secret_is_refused_before_the_run(
    joined, hub, no_store, user_id, connections, kind
):
    graph = make_graph(9871 if kind == "Kafka" else 9872, execution_location=TEAM)
    if kind == "Kafka":
        add_kafka_source(graph, 1, user_id, connections["kafka_id"])
        name = connections["kafka"]
    else:
        add_cloud_reader(graph, 1, user_id, connections["s3"])
        name = connections["s3"]
    sign_in_through_hub(hub, HUB_USER_ID)
    try:
        with node_tokens.acting_for(HUB_USER_ID):
            run_info = graph.run_graph()

        assert run_info.success is False
        (row,) = run_info.node_step_result
        assert (row.node_id, row.compute_error, row.error) == (1, "Refusal", sentence(1, kind, name))
        assert "InvalidToken" not in row.error
        assert graph.get_node(1).results.result_ref is None, "no node started"
        assert ("POST", "/hub/vend") not in hub.calls, "nothing was asked of the hub's vend"
        preview = graph.preview_placement()
        assert [(r.node_id, r.backend_id, r.reason) for r in preview.refusals] == [
            (1, TEAM, sentence(1, kind, name).split(": ", 1)[1])
        ]
    finally:
        graph.close_flow()


def test_a_google_analytics_step_on_a_hub_server_is_refused_and_the_run_keeps_it_on_this_servers_worker(
    joined, hub, no_store, user_id, connections
):
    graph = make_graph(9873, execution_location=TEAM)
    add_ga_reader(graph, 1, user_id, connections["ga"])
    try:
        node = graph.get_node(1)
        (refusal,) = held_on_this_server([node], {1: TEAM})
        assert f'cannot run on backend "{TEAM}": {refusal.reason}' == sentence(1, "Google Analytics", connections["ga"])

        preview = graph.preview_placement()
        assert preview.assignment[1] == registry.WORKER_BACKEND_ID
        assert not any("whose secret this server keeps" in r.reason for r in preview.refusals)
    finally:
        graph.close_flow()


@pytest.mark.parametrize("location", ["local", "worker"])
def test_the_same_steps_on_this_servers_own_worker_are_not_refused(
    joined, hub, no_store, user_id, connections, location
):
    graph = make_graph(9874, execution_location=location)
    add_cloud_reader(graph, 1, user_id, connections["s3"])
    add_kafka_source(graph, 2, user_id, connections["kafka_id"])
    add_ga_reader(graph, 3, user_id, connections["ga"])
    try:
        assert held_on_this_server(graph.nodes, {node.node_id: location for node in graph.nodes}) == []
        preview = graph.preview_placement()
        assert preview.refusals == []
        assert preview.assignment[1] == preview.assignment[2] == location
    finally:
        graph.close_flow()


def test_a_connection_with_no_secret_on_a_hub_server_is_not_refused(joined, hub, no_store, user_id, connections):
    graph = make_graph(9875, execution_location=TEAM)
    add_cloud_reader(graph, 1, user_id, connections["ambient"])
    try:
        assert held_on_this_server(graph.nodes, {1: TEAM}) == []
        preview = graph.preview_placement()
        assert not any("whose secret this server keeps" in r.reason for r in preview.refusals)
    finally:
        graph.close_flow()


def test_a_desktop_places_such_a_step_as_before(hub, no_store, user_id, connections):
    graph = make_graph(9876, execution_location=TEAM)
    add_cloud_reader(graph, 1, user_id, connections["s3"])
    add_kafka_source(graph, 2, user_id, connections["kafka_id"])
    try:
        preview = graph.preview_placement()
        assert not any("whose secret this server keeps" in r.reason for r in preview.refusals)
        assert preview.assignment[1] == preview.assignment[2] == TEAM
    finally:
        graph.close_flow()


@pytest.fixture
def databases(user_id, tmp_path):
    """A Postgres connection and a SQLite one over a two-row ``movies`` table, each stored with its password as a
    secret of this server's (``password_id``), both ``local_user``'s; removed afterwards."""
    suffix = uuid.uuid4().hex[:8]
    path = tmp_path / "shop.db"
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE movies (id INTEGER, title TEXT)")
        db.executemany("INSERT INTO movies VALUES (?, ?)", [(1, "a"), (2, "b")])
    names = {"postgres": f"pg_{suffix}", "sqlite": f"lite_{suffix}"}
    with get_db_context() as db:
        store_database_connection(
            db,
            input_schema.FullDatabaseConnection(
                connection_name=names["postgres"],
                host="127.0.0.1",
                port=1,
                database="shop",
                username="svc",
                password=SecretStr("kept-here"),
            ),
            user_id,
        )
        store_database_connection(
            db,
            input_schema.FullDatabaseConnection(
                connection_name=names["sqlite"],
                database_type="sqlite",
                database=f"sqlite:///{path}",
                username="",
                password=SecretStr("kept-here"),
            ),
            user_id,
        )
    yield names
    with get_db_context() as db:
        rows = db.query(db_models.DatabaseConnection).filter(
            db_models.DatabaseConnection.connection_name.in_(list(names.values()))
        )
        secret_ids = [row.password_id for row in rows if row.password_id is not None]
        for row in rows:
            db.delete(row)
        db.flush()
        db.query(db_models.Secret).filter(db_models.Secret.id.in_(secret_ids)).delete()
        db.commit()


def add_database_reader(graph, node_id: int, user_id: int, connection: str) -> None:
    add_promise(graph, node_id, "database_reader")
    graph.add_database_reader(
        input_schema.NodeDatabaseReader(
            flow_id=graph.flow_id,
            node_id=node_id,
            user_id=user_id,
            database_settings=input_schema.DatabaseSettings(
                connection_mode="reference", database_connection_name=connection, table_name="movies"
            ),
        )
    )


def database_writer(user_id: int, connection: str) -> input_schema.NodeDatabaseWriter:
    return input_schema.NodeDatabaseWriter(
        flow_id=1,
        node_id=2,
        user_id=user_id,
        database_write_settings=input_schema.DatabaseWriteSettings(
            connection_mode="reference", database_connection_name=connection, table_name="out"
        ),
    )


def test_a_database_step_on_a_hub_server_whose_password_this_server_keeps_is_refused_before_the_run(
    joined, hub, no_store, user_id, databases
):
    graph = make_graph(9877, execution_location=TEAM)
    add_database_reader(graph, 1, user_id, databases["postgres"])
    try:
        run_info = graph.run_graph()

        assert run_info.success is False
        (row,) = run_info.node_step_result
        expected = sentence(1, "database", databases["postgres"])
        assert (row.node_id, row.compute_error, row.error) == (1, "Refusal", expected)
        assert "InvalidToken" not in row.error
        assert graph.get_node(1).results.result_ref is None, "no node started"
        assert ("POST", "/hub/vend") not in hub.calls, "no vend asked for the connection's password"
        preview = graph.preview_placement()
        reasons = [(r.node_id, r.backend_id, r.reason) for r in preview.refusals]
        assert reasons == [(1, TEAM, expected.split(": ", 1)[1])]
    finally:
        graph.close_flow()


def test_a_database_writer_on_a_hub_server_whose_password_this_server_keeps_is_refused(
    joined, hub, no_store, user_id, databases
):
    class Step:
        node_id = 2
        setting_input = database_writer(user_id, databases["postgres"])

    (refusal,) = held_on_this_server([Step()], {2: TEAM})
    assert f'cannot run on backend "{TEAM}": {refusal.reason}' == sentence(2, "database", databases["postgres"])


def test_an_inline_database_connection_on_a_hub_server_is_not_refused(joined, hub, no_store, user_id):
    graph = make_graph(9878, execution_location=TEAM)
    add_promise(graph, 1, "database_reader")
    graph.add_database_reader(
        input_schema.NodeDatabaseReader(
            flow_id=graph.flow_id,
            node_id=1,
            user_id=user_id,
            database_settings=input_schema.DatabaseSettings(
                database_connection=input_schema.DatabaseConnection(
                    host="127.0.0.1", port=1, database="shop", username="svc", password_ref="on_the_hub"
                ),
                table_name="movies",
            ),
        )
    )
    try:
        assert held_on_this_server(graph.nodes, {1: TEAM}) == []
    finally:
        graph.close_flow()


def test_a_database_step_on_this_servers_own_worker_runs(joined, hub, no_store, user_id, databases, monkeypatch):
    from flowfile_worker import secrets as worker_secrets

    # The hub fixture's store moved core's secure store; the worker beside core reads the same one.
    monkeypatch.setattr(worker_secrets, "_storage", worker_secrets.SecureStorage())
    graph = make_graph(9879, execution_location="local")
    add_database_reader(graph, 1, user_id, databases["sqlite"])
    try:
        assert len(held_on_this_server(graph.nodes, {1: TEAM})) == 1, "the same step is refused on a hub server"
        assert held_on_this_server(graph.nodes, {1: "local"}) == held_on_this_server(graph.nodes, {1: "worker"}) == []

        run_info = graph.run_graph()

        assert run_info.success is True, [row.error for row in run_info.node_step_result]
        assert graph.get_node(1).get_resulting_data().to_dict() == {"id": [1, 2], "title": ["a", "b"]}
        assert ("POST", "/hub/vend") not in hub.calls
    finally:
        graph.close_flow()


def test_a_desktop_places_a_database_step_as_before(hub, no_store, user_id, databases):
    graph = make_graph(9880, execution_location=TEAM)
    add_database_reader(graph, 1, user_id, databases["postgres"])
    try:
        preview = graph.preview_placement()
        assert not any("whose secret this server keeps" in r.reason for r in preview.refusals)
        assert preview.assignment[1] == TEAM
    finally:
        graph.close_flow()
