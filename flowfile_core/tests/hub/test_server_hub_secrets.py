"""On a server joined to its hub with keys of its own every secret lives on the hub; a desktop keeps its own.

Core runs in process in docker mode as the joined server ``team-1``, its ``hub_node.json``, keys and secure store
under the test's ``tmp_path`` (T45's fixtures); the hub is ``tests/hub/hub_secrets.SecretsHub``, served from a thread.
The editor's Secrets page reaches the hub through core's own ``/secrets`` routes with the person's delegated token,
and nothing of it is written to this server's database. A desktop that never joined a hub keeps its secrets in its
own database, as before.
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
from tests.hub.hub_secrets import NOT_FOUND, TAKEN, SecretsHub
from tests.hub.test_server_run_report import sign_in
from tests.test_hub_sign_in import bearer, join

node_file = test_hub_sign_in.node_file
username = test_hub_sign_in.username

client = TestClient(main.app)


def drop_accounts(issuer: str) -> None:
    with get_db_context() as db:
        mappings = db.query(db_models.IssuerAccount).filter(db_models.IssuerAccount.issuer == issuer)
        user_ids = [row.user_id for row in mappings]
        mappings.delete(synchronize_session=False)
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


def test_a_secret_added_in_the_editor_lands_on_the_hub_and_not_in_this_servers_database(hub, person, name):
    person_id, access_token = person

    added = client.post("/secrets/secrets", json={"name": name, "value": "s3cret"}, headers=bearer(access_token))

    assert added.status_code == 200, added.text
    assert added.json()["name"] == name and "s3cret" not in added.text
    assert hub.values[name] == "s3cret" and hub.owners[name] == hub.issued[hub.secret_calls[-1][2]]
    assert rows_named(name) == 0
    [(method, route, token)] = hub.secret_calls
    assert (method, route) == ("POST", "/secrets/secrets") and token in hub.issued and token != access_token


def test_the_secrets_page_lists_changes_and_removes_the_persons_secrets_on_the_hub(hub, person, name):
    _, access_token = person
    headers = bearer(access_token)
    client.post("/secrets/secrets", json={"name": name, "value": "first"}, headers=headers)

    listed = client.get("/secrets/secrets", headers=headers)
    assert listed.status_code == 200, listed.text
    assert [row["name"] for row in listed.json()] == [name] and "first" not in listed.text
    assert client.get(f"/secrets/secrets/{name}", headers=headers).json()["name"] == name

    changed = client.put(f"/secrets/secrets/{name}", json={"value": "second"}, headers=headers)
    assert changed.status_code == 200, changed.text
    assert hub.values[name] == "second" and "second" not in changed.text

    assert client.delete(f"/secrets/secrets/{name}", headers=headers).status_code == 204
    assert name not in hub.values
    gone = client.delete(f"/secrets/secrets/{name}", headers=headers)
    assert (gone.status_code, gone.json()["detail"]) == (404, NOT_FOUND)
    assert client.put(f"/secrets/secrets/{name}", json={"value": "x"}, headers=headers).status_code == 404
    assert rows_named(name) == 0
    assert {token for _, _, token in hub.secret_calls} <= set(hub.issued)


def test_a_name_the_person_holds_on_the_hub_already_is_refused_with_the_hubs_sentence(hub, person, name):
    _, access_token = person
    headers = bearer(access_token)
    client.post("/secrets/secrets", json={"name": name, "value": "first"}, headers=headers)

    again = client.post("/secrets/secrets", json={"name": name, "value": "second"}, headers=headers)

    assert (again.status_code, again.json()["detail"]) == (400, TAKEN)
    assert hub.values[name] == "first"


def test_a_hub_that_does_not_answer_is_a_502_naming_it_and_nothing_is_kept_here(hub, person, name):
    _, access_token = person
    hub.close()

    added = client.post("/secrets/secrets", json={"name": name, "value": "s3cret"}, headers=bearer(access_token))

    assert added.status_code == 502 and "Could not reach the hub" in added.json()["detail"]
    assert rows_named(name) == 0


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
        assert client.get("/secrets/kept", headers=headers).status_code == 404
    finally:
        assert client.delete(f"/secrets/secrets/{name}", headers=headers).status_code == 204
    assert rows_named(name) == 0
