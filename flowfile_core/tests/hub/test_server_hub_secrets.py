"""On a server joined to its hub with keys of its own the Secrets page keeps this server's own secrets and shows the
hub's read-only; a desktop keeps its own.

Core runs in process in docker mode as the joined server ``team-1``, its ``hub_node.json``, keys and secure store
under the test's ``tmp_path`` (T45's fixtures); the hub is ``tests/hub/hub_secrets.SecretsHub``, served from a thread.
Core's ``/secrets`` routes add, read and delete in this server's database only. The list answers this server's own
secrets, then the ones the hub lists for the person, asked with their delegated token: names only, marked ``source:
"hub"``, ids negated. Nothing is written to the hub, and a hub that does not answer lists none. A desktop that never
joined a hub keeps its secrets in its own database, as before.
"""

import uuid
from collections.abc import Iterator

import pytest
from cryptography.fernet import Fernet
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.secret_manager.secret_manager import decrypt_secret, get_encrypted_secret
from shared import hub_node
from shared.tests.hub_jwks import HubSigner
from tests import test_hub_sign_in
from tests.hub.hub_secrets import SecretsHub
from tests.hub.test_server_run_report import UID, sign_in
from tests.test_hub_sign_in import bearer, join

node_file = test_hub_sign_in.node_file
username = test_hub_sign_in.username

client = TestClient(main.app)

NOT_FOUND = "Secret not found"
TAKEN = "Secret with this name already exists"
CONNECTIONS = (db_models.CloudStorageConnection, db_models.KafkaConnection, db_models.DatabaseConnection)


def drop_accounts(issuer: str) -> None:
    """Remove the accounts ``issuer``'s people got here, with the connections and secrets they stored."""
    with get_db_context() as db:
        mappings = db.query(db_models.IssuerAccount).filter(db_models.IssuerAccount.issuer == issuer)
        user_ids = [row.user_id for row in mappings]
        mappings.delete(synchronize_session=False)
        for model in CONNECTIONS:
            db.query(model).filter(model.user_id.in_(user_ids)).delete(synchronize_session=False)
        db.query(db_models.Secret).filter(db_models.Secret.user_id.in_(user_ids)).delete(synchronize_session=False)
        db.query(db_models.User).filter(db_models.User.id.in_(user_ids)).delete(synchronize_session=False)
        db.commit()


@pytest.fixture
def hub(node_file, monkeypatch) -> Iterator[SecretsHub]:
    """A ``SecretsHub`` this server joined as ``team-1`` with its own keys and master key; its people removed after."""
    monkeypatch.setenv("FLOWFILE_MASTER_KEY", Fernet.generate_key().decode())
    signer = HubSigner()
    fake = SecretsHub(signer)
    join(fake)
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
    return f"api_key_{uuid.uuid4().hex[:8]}"


def rows_named(secret_name: str) -> int:
    with get_db_context() as db:
        return db.query(db_models.Secret).filter(db_models.Secret.name == secret_name).count()


def test_a_secret_added_in_the_editor_lands_in_this_servers_database_and_not_on_the_hub(hub, person, name):
    person_id, access_token = person

    added = client.post("/secrets/secrets", json={"name": name, "value": "s3cret"}, headers=bearer(access_token))

    assert added.status_code == 200, added.text
    assert added.json()["name"] == name and "s3cret" not in added.text
    assert (added.json()["source"], added.json()["user_id"]) == ("local", str(person_id))
    encrypted = get_encrypted_secret(current_user_id=person_id, secret_name=name)
    assert encrypted.startswith(f"$ffsec$1${person_id}$") and decrypt_secret(encrypted).get_secret_value() == "s3cret"
    assert rows_named(name) == 1
    assert name not in hub.values and hub.secret_calls == []


def test_the_secrets_page_lists_this_servers_own_secrets_then_the_hubs_read_only_with_negated_ids(hub, person, name):
    _, access_token = person
    headers = bearer(access_token)
    on_the_hub = hub.keep(UID, f"hub_{name}", "hub-value")
    hub.keep(UID + 1, f"other_{name}", "another-persons")
    own = client.post("/secrets/secrets", json={"name": name, "value": "own-value"}, headers=headers).json()

    listed = client.get("/secrets/secrets", headers=headers)

    assert listed.status_code == 200, listed.text
    mine, the_hubs = listed.json()
    assert (mine["name"], mine["source"], mine["id"]) == (name, "local", own["id"]) and own["id"] > 0
    assert (the_hubs["name"], the_hubs["source"], the_hubs["id"]) == (f"hub_{name}", "hub", -on_the_hub["id"])
    assert the_hubs["value"] is None and "own-value" not in listed.text and "hub-value" not in listed.text
    [(method, route, token)] = hub.secret_calls
    assert (method, route) == ("GET", "/secrets/secrets") and hub.issued[token] == UID and token != access_token


def test_a_change_or_delete_of_a_hub_secret_is_not_found_here_and_the_hub_keeps_it(hub, person, name):
    _, access_token = person
    headers = bearer(access_token)
    hub.keep(UID, name, "hub-value")
    assert [row["name"] for row in client.get("/secrets/secrets", headers=headers).json()] == [name]

    deleted = client.delete(f"/secrets/secrets/{name}", headers=headers)
    changed = client.put(f"/secrets/secrets/{name}", json={"value": "x"}, headers=headers)
    read = client.get(f"/secrets/secrets/{name}", headers=headers)

    assert (deleted.status_code, deleted.json()["detail"]) == (404, NOT_FOUND)
    assert changed.status_code == 405
    assert (read.status_code, read.json()["detail"]) == (404, NOT_FOUND)
    assert hub.values[name] == "hub-value" and rows_named(name) == 0
    assert {method for method, _, _ in hub.secret_calls} == {"GET"}


def test_a_name_the_hub_also_holds_is_added_here_and_listed_once_as_this_servers_own(hub, person, name):
    _, access_token = person
    headers = bearer(access_token)
    hub.keep(UID, name, "hub-value")

    added = client.post("/secrets/secrets", json={"name": name, "value": "own-value"}, headers=headers)
    again = client.post("/secrets/secrets", json={"name": name, "value": "second"}, headers=headers)
    listed = client.get("/secrets/secrets", headers=headers)

    assert added.status_code == 200, added.text
    assert (again.status_code, again.json()["detail"]) == (400, TAKEN)
    [row] = listed.json()
    assert (row["name"], row["source"], row["id"]) == (name, "local", added.json()["id"])
    assert hub.values[name] == "hub-value" and rows_named(name) == 1


def test_a_hub_that_does_not_answer_leaves_the_page_this_servers_own_secrets_and_adding_one_works(hub, person, name):
    _, access_token = person
    headers = bearer(access_token)
    hub.keep(UID, f"hub_{name}", "hub-value")
    hub.close()

    added = client.post("/secrets/secrets", json={"name": name, "value": "s3cret"}, headers=headers)
    listed = client.get("/secrets/secrets", headers=headers)

    assert added.status_code == 200, added.text
    assert listed.status_code == 200, listed.text
    assert [(row["name"], row["source"]) for row in listed.json()] == [(name, "local")]
    assert rows_named(name) == 1


@pytest.fixture
def desktop(tmp_path, monkeypatch) -> str:
    """A desktop that never joined a hub: electron mode, no ``hub_node.json``; its own access token."""
    monkeypatch.setenv("FLOWFILE_MODE", "electron")
    monkeypatch.setattr(hub_node, "path", lambda: tmp_path / "storage" / hub_node.FILE_NAME)
    return client.post("/auth/token").json()["access_token"]


def test_a_desktop_that_never_joined_a_hub_keeps_its_secrets_in_its_own_database(desktop, name):
    headers = bearer(desktop)
    try:
        added = client.post("/secrets/secrets", json={"name": name, "value": "local"}, headers=headers)

        assert added.status_code == 200, added.text
        assert rows_named(name) == 1
        encrypted = get_encrypted_secret(current_user_id=1, secret_name=name)
        assert encrypted.startswith("$ffsec$1$1$") and decrypt_secret(encrypted).get_secret_value() == "local"
        assert name in [row["name"] for row in client.get("/secrets/secrets", headers=headers).json()]
        assert client.put(f"/secrets/secrets/{name}", json={"value": "x"}, headers=headers).status_code == 405
    finally:
        assert client.delete(f"/secrets/secrets/{name}", headers=headers).status_code == 204
    assert rows_named(name) == 0
