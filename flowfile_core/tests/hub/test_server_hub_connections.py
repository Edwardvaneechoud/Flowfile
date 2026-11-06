"""On a server joined to its hub with keys of its own every stored connection lives on the hub; a desktop keeps its own.

Core runs in process in docker mode as the joined server ``team-1``, its ``hub_node.json``, keys and secure store
under the test's ``tmp_path`` (T45's fixtures); the hub is ``tests/hub/hub_connections.ConnectionsHub``, served from a
thread. The editor's cloud storage, Kafka and database connection pages reach the hub through core's own routes with
the person's delegated token, nothing of them is written to this server's database, and a connection another person
shared with them is listed with the hub's ``access``. A step resolves a connection by name through a vend sealed to
this server, never from a row this server held before it joined. A desktop that never joined a hub keeps its
connections in its own database, as before.
"""

import uuid
from collections.abc import Iterator

import pytest
from cryptography.fernet import Fernet
from fastapi import HTTPException
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager.db_connections import (
    delete_database_connection,
    get_local_cloud_connection,
    get_local_database_connection,
    store_database_connection,
)
from flowfile_core.flowfile.flow_graph.builders.connectors import _kafka_consumer_config
from flowfile_core.schemas import input_schema
from flowfile_core.secret_manager.secret_manager import decrypt_secret
from shared import hub_node
from shared.tests.hub_jwks import HubSigner
from tests import test_hub_sign_in
from tests.hub.hub_connections import ConnectionsHub
from tests.hub.test_server_hub_secrets import drop_accounts
from tests.hub.test_server_run_report import UID, sign_in
from tests.test_hub_sign_in import NODE, bearer, join

node_file = test_hub_sign_in.node_file
username = test_hub_sign_in.username

client = TestClient(main.app)

OTHER = UID + 1
KINDS = ("cloud", "kafka", "database")
MODELS = {
    "cloud": db_models.CloudStorageConnection,
    "kafka": db_models.KafkaConnection,
    "database": db_models.DatabaseConnection,
}
SECRET_FIELD = {"cloud": "aws_secret_access_key", "kafka": "sasl_password", "database": "password"}
CREATE = {
    "cloud": "/cloud_connections/cloud_connection",
    "kafka": "/kafka/connections",
    "database": "/db_connection_lib",
}
LIST = {
    "cloud": "/cloud_connections/cloud_connections",
    "kafka": "/kafka/connections",
    "database": "/db_connection_lib",
}


@pytest.fixture
def hub(node_file, monkeypatch) -> Iterator[ConnectionsHub]:
    """A ``ConnectionsHub`` this server joined as ``team-1`` with its own keys and master key, knowing a second
    person ``dee``; its people removed after."""
    monkeypatch.setenv("FLOWFILE_MASTER_KEY", Fernet.generate_key().decode())
    signer = HubSigner()
    fake = ConnectionsHub(signer)
    join(fake)
    fake.people[OTHER] = {"uid": OTHER, "username": "dee", "full_name": None, "node_admin": False}
    try:
        yield fake
    finally:
        fake.close()
        signer.close()
        drop_accounts(fake.issuer)


@pytest.fixture
def person(hub, username) -> tuple[int, str]:
    """A person signed in through the hub: their account here and core's access token."""
    return sign_in(hub, username)


@pytest.fixture
def name() -> str:
    return f"conn_{uuid.uuid4().hex[:8]}"


def cloud(name: str, secret: str = "minio-secret") -> dict:
    return {
        "storage_type": "s3",
        "auth_method": "access_key",
        "connection_name": name,
        "aws_access_key_id": "minio",
        "aws_secret_access_key": secret,
        "aws_region": "us-east-1",
        "endpoint_url": "http://localhost:9000",
    }


def kafka(name: str, secret: str = "kafka-secret") -> dict:
    return {
        "connection_name": name,
        "bootstrap_servers": "localhost:19092",
        "security_protocol": "SASL_PLAINTEXT",
        "sasl_mechanism": "PLAIN",
        "sasl_username": "ada",
        "sasl_password": secret,
    }


def database(name: str, secret: str = "pg-secret") -> dict:
    return {
        "connection_name": name,
        "database_type": "postgresql",
        "username": "ada",
        "password": secret,
        "host": "localhost",
        "port": 5433,
        "database": "warehouse",
    }


BODIES = {"cloud": cloud, "kafka": kafka, "database": database}


def rows_named(kind: str, connection_name: str) -> int:
    with get_db_context() as db:
        model = MODELS[kind]
        return db.query(model).filter(model.connection_name == connection_name).count()


@pytest.mark.parametrize("kind", KINDS)
def test_a_connection_added_in_the_editor_lands_on_the_hub_and_not_in_this_servers_database(hub, person, name, kind):
    _, access_token = person

    added = client.post(CREATE[kind], json=BODIES[kind](name), headers=bearer(access_token))

    assert added.status_code == 200, added.text
    assert "secret" not in added.text
    [row] = hub.connections
    assert (row["kind"], row["name"], row["owner"]) == (kind, name, UID)
    assert row["settings"][SECRET_FIELD[kind]] == BODIES[kind](name)[SECRET_FIELD[kind]]
    assert rows_named(kind, name) == 0
    [(method, route, token)] = hub.connection_calls
    assert (method, route) == ("POST", CREATE[kind]) and hub.issued[token] == UID and token != access_token


def test_the_pages_list_change_and_delete_each_kind_on_the_hub(hub, person, name):
    _, access_token = person
    headers = bearer(access_token)
    for kind in KINDS:
        assert client.post(CREATE[kind], json=BODIES[kind](name), headers=headers).status_code == 200

    listed = {kind: client.get(LIST[kind], headers=headers) for kind in KINDS}

    assert all(response.status_code == 200 for response in listed.values()), {k: r.text for k, r in listed.items()}
    assert all([row["connection_name"] for row in response.json()] == [name] for response in listed.values())
    assert not any("secret" in response.text for response in listed.values())
    assert all(response.json()[0]["access"]["is_owner"] for response in listed.values())
    kafka_id = listed["kafka"].json()[0]["id"]

    assert client.put(CREATE["cloud"], json=cloud(name, "rotated"), headers=headers).status_code == 200
    assert hub.resolve("cloud", UID, name)["settings"]["aws_secret_access_key"] == "rotated"
    assert client.put(CREATE["database"], json=database(name, "rotated"), headers=headers).status_code == 200
    assert hub.resolve("database", UID, name)["settings"]["password"] == "rotated"
    moved = client.put(f"/kafka/connections/{kafka_id}", json={"bootstrap_servers": "broker:9092"}, headers=headers)
    assert moved.status_code == 200, moved.text
    assert moved.json()["bootstrap_servers"] == "broker:9092" and "kafka-secret" not in moved.text
    assert hub.resolve("kafka", UID, name)["settings"]["sasl_password"] == "kafka-secret"

    assert client.delete(CREATE["cloud"], params={"connection_name": name}, headers=headers).status_code == 200
    assert client.delete(CREATE["database"], params={"connection_name": name}, headers=headers).status_code == 200
    assert client.delete(f"/kafka/connections/{kafka_id}", headers=headers).status_code == 200
    assert hub.connections == []
    gone = client.delete(CREATE["cloud"], params={"connection_name": name}, headers=headers)
    assert (gone.status_code, gone.json()["detail"]) == (404, "Cloud connection connection not found")
    assert client.put(CREATE["database"], json=database(name), headers=headers).status_code == 404
    assert client.delete(f"/kafka/connections/{kafka_id}", headers=headers).status_code == 404
    assert all(rows_named(kind, name) == 0 for kind in KINDS)
    assert all(hub.issued[token] == UID for _, _, token in hub.connection_calls)


def test_a_name_the_person_holds_on_the_hub_already_is_refused_with_the_hubs_sentence(hub, person, name):
    _, access_token = person
    headers = bearer(access_token)
    client.post(CREATE["database"], json=database(name), headers=headers)

    again = client.post(CREATE["database"], json=database(name, "other"), headers=headers)

    assert (again.status_code, again.json()["detail"]) == (422, "Connection name already exists")
    assert hub.resolve("database", UID, name)["settings"]["password"] == "pg-secret"


@pytest.mark.parametrize("kind", KINDS)
def test_a_connection_shared_with_the_person_is_listed_with_the_hubs_marking_and_no_secret(hub, person, name, kind):
    _, access_token = person
    hub.add(kind, OTHER, BODIES[kind](name))
    hub.share(kind, name, OTHER, UID)

    listed = client.get(LIST[kind], headers=bearer(access_token))

    assert listed.status_code == 200, listed.text
    [row] = listed.json()
    assert row["connection_name"] == name and "secret" not in listed.text
    assert row["access"] == {"is_owner": False, "access_level": "use", "shared_by": "dee"}


def test_a_step_resolves_each_kind_by_name_from_the_hub_sealed_to_this_server(hub, person, name):
    person_id, _ = person
    hub.add("database", OTHER, database(name))
    hub.share("database", name, OTHER, UID)
    hub.add("cloud", UID, cloud(name))
    hub.add("kafka", UID, kafka(name))

    found = get_local_database_connection(name, person_id)
    lake = get_local_cloud_connection(name, person_id)
    config = _kafka_consumer_config(input_schema.KafkaSourceSettings(kafka_connection_name=name), person_id)

    assert (found.host, found.port, found.username) == ("localhost", 5433, "ada")
    password = found.password.get_secret_value()
    assert password.startswith(f"$ffsec$1${person_id}$") and decrypt_secret(password).get_secret_value() == "pg-secret"
    assert lake.aws_secret_access_key.get_secret_value() == "minio-secret" and lake.endpoint_url.endswith(":9000")
    assert config["bootstrap.servers"] == "localhost:19092" and config["sasl.username"] == "ada"
    assert decrypt_secret(config["sasl.password"]).get_secret_value() == "kafka-secret"
    asked = [body for _, body in hub.connection_vends]
    assert [body["connections"] for body in asked] == [
        [{"kind": kind, "name": name}] for kind in ("database", "cloud", "kafka")
    ]
    assert all(body["targets"] == [NODE] and body["run_id"].startswith("core-lookup-") for body in asked)
    assert all(uid == UID for uid, _ in hub.connection_vends)
    assert all(rows_named(kind, name) == 0 for kind in KINDS)


def test_a_name_the_person_reaches_no_connection_of_on_the_hub_resolves_to_none_and_no_row_here_is_read(
    hub, person, name
):
    person_id, _ = person
    hub.add("database", OTHER, database(name))
    with get_db_context() as db:
        store_database_connection(db, input_schema.FullDatabaseConnection(**database(name, "pre-join")), person_id)
    try:
        assert get_local_database_connection(name, person_id) is None
        assert get_local_cloud_connection(name, person_id) is None
        with pytest.raises(HTTPException, match="Kafka connection not found"):
            _kafka_consumer_config(input_schema.KafkaSourceSettings(kafka_connection_name=name), person_id)
        assert [body["connections"][0]["kind"] for _, body in hub.connection_vends] == ["database", "cloud", "kafka"]
    finally:
        with get_db_context() as db:
            delete_database_connection(db, name, person_id)


@pytest.fixture
def desktop(tmp_path, monkeypatch) -> str:
    """A desktop that never joined a hub: electron mode, no ``hub_node.json``; its own access token."""
    monkeypatch.setenv("FLOWFILE_MODE", "electron")
    monkeypatch.setattr(hub_node, "path", lambda: tmp_path / "storage" / hub_node.FILE_NAME)
    return client.post("/auth/token").json()["access_token"]


def test_a_desktop_that_never_joined_a_hub_keeps_its_connections_in_its_own_database(desktop, name):
    headers = bearer(desktop)
    kafka_id = None
    try:
        for kind in KINDS:
            added = client.post(CREATE[kind], json=BODIES[kind](name), headers=headers)
            assert added.status_code == 200, added.text
            if kind == "kafka":
                kafka_id = added.json()["id"]
            assert rows_named(kind, name) == 1
            assert name in [row["connection_name"] for row in client.get(LIST[kind], headers=headers).json()]
        found = get_local_database_connection(name, 1)
        assert decrypt_secret(found.password.get_secret_value()).get_secret_value() == "pg-secret"
        assert get_local_cloud_connection(name, 1).aws_secret_access_key.get_secret_value() == "minio-secret"
        config = _kafka_consumer_config(input_schema.KafkaSourceSettings(kafka_connection_id=kafka_id), 1)
        assert decrypt_secret(config["sasl.password"]).get_secret_value() == "kafka-secret"
    finally:
        client.delete(CREATE["cloud"], params={"connection_name": name}, headers=headers)
        client.delete(CREATE["database"], params={"connection_name": name}, headers=headers)
        if kafka_id is not None:
            client.delete(f"/kafka/connections/{kafka_id}", headers=headers)
    assert all(rows_named(kind, name) == 0 for kind in KINDS)
