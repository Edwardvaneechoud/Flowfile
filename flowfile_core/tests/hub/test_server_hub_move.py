"""At start, a server joined to its hub with keys of its own moves the secrets in its database to their owners there.

The setup is ``test_server_hub_secrets``'s. What the hub takes leaves this server's database; what it refuses stays,
with the hub's sentence, on the Secrets page (``GET /secrets/kept``) and in the log, never with its value; a secret a
stored connection holds stays with it; a server that never joined a hub moves nothing.
"""

import logging
import uuid

import pytest
from cryptography.fernet import Fernet
from pydantic import SecretStr

from flowfile_core.auth.models import SecretInput
from flowfile_core.configs import logger as core_logger
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.hub_client import secrets as hub_secrets
from flowfile_core.secret_manager.secret_manager import store_secret
from tests import test_hub_sign_in
from tests.hub import test_server_hub_secrets as page
from tests.hub.hub_secrets import NOT_ALLOWED, already_there, no_person
from tests.test_hub_sign_in import bearer

node_file = test_hub_sign_in.node_file
username = test_hub_sign_in.username
hub = page.hub
person = page.person
name = page.name
rows_named = page.rows_named
client = page.client


def account(hub, uid: int) -> int:
    """A local account mapped to the hub's person ``uid``, as their first sign-in makes it."""
    with get_db_context() as db:
        user = db_models.User(username=f"p{uid}-{uuid.uuid4().hex[:6]}", email=None, hashed_password="x")
        db.add(user)
        db.flush()
        db.add(db_models.IssuerAccount(issuer=hub.issuer, subject_id=uid, user_id=user.id))
        db.commit()
        return user.id


def secret_here(owner: int, secret_name: str, value: str) -> int:
    with get_db_context() as db:
        return store_secret(db, SecretInput(name=secret_name, value=SecretStr(value)), owner).id


@pytest.fixture
def lines(caplog):
    """What the move logs, read from core's logger, which does not propagate."""
    caplog.set_level(logging.INFO, logger=core_logger.name)
    core_logger.addHandler(caplog.handler)
    yield caplog
    core_logger.removeHandler(caplog.handler)


def test_the_move_at_start_moves_what_the_hub_takes_and_keeps_the_rest_with_the_hubs_sentence(hub, person, lines):
    person_id, access_token = person
    tag = uuid.uuid4().hex[:6]
    moves, taken, held = f"moves_{tag}", f"taken_{tag}", f"held_{tag}"
    hub.values[taken] = "the-hubs-own"
    hub.owners[taken] = 7
    secret_here(person_id, moves, "value-moves")
    secret_here(person_id, taken, "value-taken")
    held_id = secret_here(person_id, held, "value-held")
    refused, unknown = account(hub, 8), account(hub, 99)
    hub.people[8] = {"uid": 8, "username": "bo", "full_name": None, "node_admin": False}
    hub.refused_people.add(8)
    secret_here(refused, f"refused_{tag}", "value-refused")
    secret_here(unknown, f"unknown_{tag}", "value-unknown")
    with get_db_context() as db:
        db.add(db_models.DatabaseConnection(connection_name=held, password_id=held_id, user_id=person_id))
        db.commit()
    try:
        hub_secrets.move_to_hub()

        assert hub.values[moves] == "value-moves" and hub.owners[moves] == 7
        assert rows_named(moves) == 0
        assert hub.values[taken] == "the-hubs-own" and rows_named(taken) == 1
        assert held not in hub.values and rows_named(held) == 1
        assert f"refused_{tag}" not in hub.values and rows_named(f"refused_{tag}") == 1
        assert f"unknown_{tag}" not in hub.values and rows_named(f"unknown_{tag}") == 1
        [sent] = hub.imported
        assert {entry["name"] for entry in sent} == {moves, taken, f"refused_{tag}", f"unknown_{tag}"}

        kept = client.get("/secrets/kept", headers=bearer(access_token))
        assert kept.status_code == 200, kept.text
        assert {row["name"]: row["reason"] for row in kept.json()} == {
            taken: already_there(taken),
            held: hub_secrets.HELD_HERE,
        }
        assert {k.name: k.reason for k in hub_secrets.kept(refused)} == {f"refused_{tag}": NOT_ALLOWED}
        assert {k.name: k.reason for k in hub_secrets.kept(unknown)} == {f"unknown_{tag}": no_person(99)}
        logged = lines.text
        assert f"Moved secret {moves}" in logged and already_there(taken) in logged and no_person(99) in logged
        assert "value-" not in logged and "the-hubs-own" not in logged
    finally:
        with get_db_context() as db:
            db.query(db_models.DatabaseConnection).filter_by(connection_name=held).delete()
            db.commit()


def test_a_move_the_hub_cannot_take_keeps_every_secret_and_says_why(hub, person, lines):
    person_id, access_token = person
    name = f"stays_{uuid.uuid4().hex[:6]}"
    secret_here(person_id, name, "value-stays")
    hub.close()

    hub_secrets.move_to_hub()

    assert rows_named(name) == 1
    [kept] = client.get("/secrets/kept", headers=bearer(access_token)).json()
    assert kept["name"] == name and kept["reason"].startswith("the hub did not take it: Could not reach the hub")
    assert name in lines.text and "value-stays" not in lines.text


def test_a_server_that_never_joined_a_hub_moves_nothing(node_file, name, monkeypatch):
    monkeypatch.setenv("FLOWFILE_MASTER_KEY", Fernet.generate_key().decode())
    with get_db_context() as db:
        row_id = store_secret(db, SecretInput(name=name, value=SecretStr("local")), 1).id
    try:
        assert hub_secrets.start_move() is None
        hub_secrets.move_to_hub()
        assert rows_named(name) == 1
    finally:
        with get_db_context() as db:
            db.query(db_models.Secret).filter(db_models.Secret.id == row_id).delete()
            db.commit()
