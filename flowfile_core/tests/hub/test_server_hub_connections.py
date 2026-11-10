"""On a server joined to its hub with keys of its own the connection pages keep this server's own connections and
show the hub's read-only; a desktop keeps its own.

Core runs in process in docker mode as the joined server ``team-1``, its ``hub_node.json``, keys and secure store
under the test's ``tmp_path`` (T45's fixtures); the hub is ``tests/hub/hub_connections.ConnectionsHub``, served from a
thread. The editor's cloud storage, Kafka and database connection routes add, change and delete in this server's
database only. Each list answers this server's own connections, then the ones the hub lists for the person, their own
and those shared with them, asked with their delegated token: no secret, marked ``source: "hub"``, ids negated, with
the hub's ``access``. A step that names a connection gets this server's own first, else the hub's list row, which
holds no secret; its secrets reach a step only sealed by a run's vend. A desktop that never joined a hub keeps its
connections in its own database, as before.
"""

import copy
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
    get_local_cloud_connection,
    get_local_database_connection,
)
from flowfile_core.flowfile.execution import run_secrets
from flowfile_core.flowfile.flow_graph.builders.connectors import (
    _kafka_consumer_config,
    _resolve_database_credentials,
    _step_database_credentials,
)
from flowfile_core.schemas import input_schema
from flowfile_core.secret_manager.secret_manager import decrypt_secret
from shared import hub_node
from shared.tests.hub_jwks import HubSigner
from tests import test_hub_sign_in
from tests.hub.hub_connections import ConnectionsHub
from tests.hub.test_server_hub_secrets import drop_accounts
from tests.hub.test_server_run_report import UID, sign_in
from tests.test_hub_sign_in import bearer, join

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


def owners_of(kind: str, connection_name: str) -> list[int]:
    """The owner of each ``kind`` connection named ``connection_name`` in this server's database."""
    with get_db_context() as db:
        model = MODELS[kind]
        return [row.user_id for row in db.query(model).filter(model.connection_name == connection_name)]


def rows_named(kind: str, connection_name: str) -> int:
    return len(owners_of(kind, connection_name))


def stored_secrets(user_id: int) -> list[str]:
    """The values of the secrets ``user_id`` holds in this server's database, opened with this server's key."""
    with get_db_context() as db:
        rows = db.query(db_models.Secret).filter(db_models.Secret.user_id == user_id).all()
        return [decrypt_secret(row.encrypted_value).get_secret_value() for row in rows]


@pytest.mark.parametrize("kind", KINDS)
def test_a_connection_added_in_the_editor_lands_in_this_servers_database_and_not_on_the_hub(hub, person, name, kind):
    person_id, access_token = person
    headers = bearer(access_token)

    added = client.post(CREATE[kind], json=BODIES[kind](name), headers=headers)
    listed = client.get(LIST[kind], headers=headers)

    assert added.status_code == 200, added.text
    assert "secret" not in added.text and "secret" not in listed.text
    [row] = listed.json()
    assert (row["connection_name"], row["source"], row["access"]["is_owner"]) == (name, "local", True)
    assert row["id"] > 0 and owners_of(kind, name) == [person_id]
    assert stored_secrets(person_id) == [BODIES[kind](name)[SECRET_FIELD[kind]]]
    assert hub.connections == []
    assert [(method, route) for method, route, _ in hub.connection_calls] == [("GET", LIST[kind])]


def test_each_page_lists_this_servers_own_connections_then_the_hubs_read_only_with_negated_ids(hub, person, name):
    _, access_token = person
    headers = bearer(access_token)
    on_the_hub = {kind: hub.add(kind, UID, BODIES[kind](f"hub_{name}")) for kind in KINDS}
    for kind in KINDS:
        assert client.post(CREATE[kind], json=BODIES[kind](name), headers=headers).status_code == 200

    listed = {kind: client.get(LIST[kind], headers=headers) for kind in KINDS}

    assert all(response.status_code == 200 for response in listed.values()), {k: r.text for k, r in listed.items()}
    for kind in KINDS:
        mine, the_hubs = listed[kind].json()
        assert (mine["connection_name"], mine["source"]) == (name, "local") and mine["id"] > 0
        assert (the_hubs["connection_name"], the_hubs["source"]) == (f"hub_{name}", "hub")
        assert the_hubs["id"] == -on_the_hub[kind]["id"] and the_hubs["access"]["is_owner"]
    assert not any("secret" in response.text for response in listed.values())
    assert [(method, route) for method, route, _ in hub.connection_calls] == [("GET", LIST[kind]) for kind in KINDS]
    assert all(hub.issued[token] == UID and token != access_token for _, _, token in hub.connection_calls)


def test_a_change_or_delete_of_a_hub_connection_is_not_found_here_and_the_hub_keeps_it(hub, person, name):
    _, access_token = person
    headers = bearer(access_token)
    on_the_hub = {kind: hub.add(kind, UID, BODIES[kind](name)) for kind in KINDS}
    kept = copy.deepcopy(hub.connections)
    kafka_id = client.get(LIST["kafka"], headers=headers).json()[0]["id"]

    answers = {
        "change cloud": client.put(CREATE["cloud"], json=cloud(name, "rotated"), headers=headers),
        "change database": client.put(CREATE["database"], json=database(name, "rotated"), headers=headers),
        "change kafka": client.put(
            f"/kafka/connections/{kafka_id}", json={"bootstrap_servers": "broker:9092"}, headers=headers
        ),
        "delete cloud": client.delete(CREATE["cloud"], params={"connection_name": name}, headers=headers),
        "delete database": client.delete(CREATE["database"], params={"connection_name": name}, headers=headers),
        "delete kafka": client.delete(f"/kafka/connections/{kafka_id}", headers=headers),
    }

    assert kafka_id == -on_the_hub["kafka"]["id"]
    assert {what: (answer.status_code, answer.json()["detail"]) for what, answer in answers.items()} == {
        "change cloud": (404, "Cloud connection not found"),
        "change database": (404, "Database connection not found"),
        "change kafka": (404, f"Kafka connection {kafka_id} not found"),
        "delete cloud": (404, "Cloud connection not found"),
        "delete database": (404, "Database connection not found"),
        "delete kafka": (404, f"Kafka connection {kafka_id} not found"),
    }
    assert hub.connections == kept
    assert {method for method, _, _ in hub.connection_calls} == {"GET"}
    assert all(rows_named(kind, name) == 0 for kind in KINDS)


@pytest.mark.parametrize("kind", KINDS)
def test_a_name_the_hub_also_holds_is_added_here_listed_once_as_this_servers_own_and_is_the_one_a_step_takes(
    hub, person, name, kind
):
    person_id, access_token = person
    headers = bearer(access_token)
    hub.add(kind, UID, BODIES[kind](name, "the-hubs"))
    before = run_secrets.hub_row(kind, name, person_id)

    added = client.post(CREATE[kind], json=BODIES[kind](name, "this-servers"), headers=headers)
    listed = client.get(LIST[kind], headers=headers)

    assert before["source"] == "hub"
    assert added.status_code == 200, added.text
    [row] = listed.json()
    assert (row["connection_name"], row["source"]) == (name, "local") and row["id"] > 0
    assert run_secrets.hub_row(kind, name, person_id) is None
    assert stored_secrets(person_id) == ["this-servers"]
    assert hub.resolve(kind, UID, name)["settings"][SECRET_FIELD[kind]] == "the-hubs"


@pytest.mark.parametrize("kind", KINDS)
def test_a_connection_shared_with_the_person_on_the_hub_is_listed_with_the_hubs_marking_and_no_secret(
    hub, person, name, kind
):
    _, access_token = person
    shared = hub.add(kind, OTHER, BODIES[kind](name))
    hub.share(kind, name, OTHER, UID)

    listed = client.get(LIST[kind], headers=bearer(access_token))

    assert listed.status_code == 200, listed.text
    [row] = listed.json()
    assert row["connection_name"] == name and "secret" not in listed.text
    assert (row["source"], row["id"]) == ("hub", -shared["id"])
    assert row["access"] == {"is_owner": False, "access_level": "use", "shared_by": "dee"}


def test_a_step_that_names_a_hub_connection_gets_its_list_row_with_no_secret_and_no_vend_is_asked_outside_a_run(
    hub, person, name
):
    person_id, _ = person
    hub.add("database", OTHER, database(name))
    hub.share("database", name, OTHER, UID)
    hub.add("cloud", UID, cloud(name))
    hub.add("kafka", UID, kafka(name))
    reader = input_schema.DatabaseSettings(
        connection_mode="reference", database_connection_name=name, table_name="movies"
    )

    found = get_local_database_connection(name, person_id)
    lake = get_local_cloud_connection(name, person_id)
    config = _kafka_consumer_config(input_schema.KafkaSourceSettings(kafka_connection_name=name), person_id)

    assert all(run_secrets.hub_row(kind, name, person_id)["source"] == "hub" for kind in KINDS)
    assert (found.host, found.port, found.username) == ("localhost", 5433, "ada")
    assert found.password.get_secret_value() == ""
    assert lake.aws_access_key_id == "minio" and lake.endpoint_url.endswith(":9000")
    assert lake.aws_secret_access_key is None
    assert config["bootstrap.servers"] == "localhost:19092" and config["sasl.username"] == "ada"
    assert "sasl.password" not in config
    refusal = f"{name} is a hub connection, which only a hub server opens: run this step on a hub server"
    with pytest.raises(ValueError, match=refusal):
        _step_database_credentials(reader, person_id, lambda: _resolve_database_credentials(reader, person_id))
    assert hub.connection_vends == [] and hub.catalog.vend_calls == []
    assert all(rows_named(kind, name) == 0 for kind in KINDS)


def test_a_name_only_another_person_holds_on_the_hub_is_not_listed_and_is_no_connection_for_a_step(hub, person, name):
    person_id, access_token = person
    for kind in KINDS:
        hub.add(kind, OTHER, BODIES[kind](name))

    listed = {kind: client.get(LIST[kind], headers=bearer(access_token)).json() for kind in KINDS}

    assert listed == {kind: [] for kind in KINDS}
    assert get_local_database_connection(name, person_id) is None
    assert get_local_cloud_connection(name, person_id) is None
    with pytest.raises(HTTPException, match="Kafka connection not found"):
        _kafka_consumer_config(input_schema.KafkaSourceSettings(kafka_connection_name=name), person_id)
    assert hub.connection_vends == []


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
