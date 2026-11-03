"""A server that joined a hub signs a person in with the hub's one-time sign-in token and says how people sign in.

The core is in docker mode with its own JWT secret, as a server is. Its storage dir's node joins ``FakeJoinHub`` over
real HTTP with its own keys; the fake serves its signer's keys, signs the sign-in tokens with them, and answers
``/hub/token`` only for an assertion that verifies with the key the join sent. ``hub_node.json``, the keys and the
secure store live under the test's ``tmp_path``.
"""

import json
import uuid

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.auth import secrets as core_secrets
from flowfile_core.auth.jwt import invalidate_user_cache
from flowfile_core.auth.password import get_password_hash
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.hub_client import client as hub_client
from flowfile_core.hub_client import session
from shared import hub_node, hub_node_key, hub_tokens
from shared.tests.hub_join import TOKEN_EXCHANGE, TOKEN_PATH, FakeJoinHub
from shared.tests.hub_jwks import HubSigner

client = TestClient(main.app)

NODE = "team-1"
VARIABLES = ("FLOWFILE_HUB_JOIN_TOKEN", "FLOWFILE_HUB_URL", "FLOWFILE_NODE_NAME", "FLOWFILE_HUB_ISSUER")


def bearer(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def node_file(tmp_path, monkeypatch):
    """A server's storage dir of the test's own: docker mode, its own secure store, no hub settings in the variables."""
    monkeypatch.setenv("FLOWFILE_MODE", "docker")
    monkeypatch.setenv("JWT_SECRET_KEY", "test-secret-key-for-hub-sign-in-tests")
    for name in VARIABLES:
        monkeypatch.delenv(name, raising=False)
    target = tmp_path / "storage" / hub_node.FILE_NAME
    monkeypatch.setattr(hub_node, "path", lambda: target)
    monkeypatch.setattr(hub_tokens, "_client", None)
    monkeypatch.setattr(hub_client, "_module_http", None)
    secure = tmp_path / "secure_storage"
    monkeypatch.setenv("FLOWFILE_SECURE_STORAGE_PATH", str(secure))
    monkeypatch.setenv("SECURE_STORAGE_PATH", str(secure))
    monkeypatch.setattr(core_secrets, "_storage", core_secrets.SecureStorage())
    monkeypatch.setattr(session, "_access_tokens", {})
    invalidate_user_cache()
    yield target
    invalidate_user_cache()


@pytest.fixture
def hub(node_file):
    """A hub this server joined with its own keys, enabled; the accounts its people got are removed afterwards."""
    signer = HubSigner()
    fake = FakeJoinHub(signer)
    hub_node.join(
        fake.url,
        fake.mint(enable_on_join=True),
        name=NODE,
        worker_url="http://team-1.example:63579",
        core_url="http://team-1.example:63578",
    )
    try:
        yield fake
    finally:
        fake.close()
        signer.close()
        with get_db_context() as db:
            mappings = db.query(db_models.IssuerAccount).filter(db_models.IssuerAccount.issuer == fake.issuer)
            user_ids = [row.user_id for row in mappings]
            mappings.delete(synchronize_session=False)
            db.query(db_models.User).filter(db_models.User.id.in_(user_ids)).delete(synchronize_session=False)
            db.commit()


@pytest.fixture
def username():
    return f"ana-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def local_user():
    """Makes a local account with a password, as an administrator adds one; removed afterwards."""
    made = []

    def make(name: str, *, disabled: bool = False) -> int:
        with get_db_context() as db:
            row = db_models.User(
                username=name,
                email=f"{name}@flowfile.app",
                hashed_password=get_password_hash("local-password-1!"),
                disabled=disabled,
                is_admin=False,
            )
            db.add(row)
            db.commit()
            made.append(row.id)
            return row.id

    yield make
    with get_db_context() as db:
        db.query(db_models.User).filter(db_models.User.id.in_(made)).delete(synchronize_session=False)
        db.commit()


def sign_in(token: str):
    return client.post("/auth/hub/signin", json={"token": token})


def me(access_token: str) -> dict:
    response = client.get("/auth/users/me", headers=bearer(access_token))
    assert response.status_code == 200, response.text
    return response.json()


def mapping(hub: FakeJoinHub, uid: int) -> int | None:
    with get_db_context() as db:
        row = (
            db.query(db_models.IssuerAccount)
            .filter(db_models.IssuerAccount.issuer == hub.issuer, db_models.IssuerAccount.subject_id == uid)
            .first()
        )
        return None if row is None else row.user_id


def accounts_named(name: str) -> int:
    with get_db_context() as db:
        return db.query(db_models.User).filter(db_models.User.username.like(f"{name}%")).count()


def exchanges(hub: FakeJoinHub) -> list[dict]:
    return [request for request in hub.token_requests if request["grant_type"] == TOKEN_EXCHANGE]


def test_a_sign_in_token_for_this_node_signs_the_person_in_with_an_account_mapped_by_issuer_and_id(hub, username):
    token = hub.sign_in_token(NODE, uid=7, username=username, full_name="Ana Lima")

    response = sign_in(token)

    assert response.status_code == 200, response.text
    answer = response.json()
    assert set(answer) == {"access_token", "refresh_token", "token_type"} and answer["token_type"] == "bearer"
    person = me(answer["access_token"])
    assert (person["username"], person["full_name"]) == (username, "Ana Lima")
    assert (person["is_admin"], person["must_change_password"], person["disabled"]) == (False, False, False)
    assert mapping(hub, 7) == person["id"]
    [taken] = exchanges(hub)
    assert taken["header"]["kid"] == hub_node_key.load().sig_kid
    assert (taken["claims"]["sub"], taken["claims"]["aud"]) == (f"node:{NODE}", hub.issuer + TOKEN_PATH)
    stored = session.load(person["id"])
    assert (stored["kind"], stored["hub_url"]) == ("delegation", hub.url)
    assert stored["refresh_token"] in hub.live_refresh_tokens()
    assert set(stored) == {"kind", "hub_url", "refresh_token", "refresh_expires_at"}


def test_the_delegation_is_written_nowhere_but_the_session_store(hub, username):
    response = sign_in(hub.sign_in_token(NODE, uid=7, username=username))
    user_id = me(response.json()["access_token"])["id"]

    refresh_token = session.load(user_id)["refresh_token"]
    with get_db_context() as db:
        row = db.query(db_models.User).filter(db_models.User.id == user_id).one()
        columns = {column.name: str(getattr(row, column.name)) for column in row.__table__.columns}
    assert refresh_token not in json.dumps(columns) and refresh_token not in response.text
    assert columns["hashed_password"].startswith("$2b$")
    no_password = client.post("/auth/token", data={"username": columns["username"], "password": ""})
    assert no_password.status_code == 401


def test_a_second_sign_in_is_the_same_account_and_administration_follows_node_admin_each_time(hub, username):
    first = me(sign_in(hub.sign_in_token(NODE, uid=7, username=username, node_admin=True)).json()["access_token"])
    second = me(sign_in(hub.sign_in_token(NODE, uid=7, username=username, node_admin=False)).json()["access_token"])
    third = me(sign_in(hub.sign_in_token(NODE, uid=7, username=username, node_admin=True)).json()["access_token"])

    assert first["id"] == second["id"] == third["id"]
    assert [first["is_admin"], second["is_admin"], third["is_admin"]] == [True, False, True]
    assert accounts_named(username) == 1
    assert len(exchanges(hub)) == 3


def test_a_local_account_holding_the_hub_username_is_never_taken_over(hub, username, local_user):
    local_id = local_user(username)

    response = sign_in(hub.sign_in_token(NODE, uid=7, username=username))

    assert response.status_code == 200, response.text
    person = me(response.json()["access_token"])
    assert person["username"] == f"{username}@127.0.0.1" and person["id"] != local_id
    assert mapping(hub, 7) == person["id"]
    password = client.post("/auth/token", data={"username": username, "password": "local-password-1!"})
    assert me(password.json()["access_token"])["id"] == local_id


def test_a_person_whose_both_names_are_taken_is_refused_with_a_sentence(hub, username, local_user):
    local_user(username)
    local_user(f"{username}@127.0.0.1")

    response = sign_in(hub.sign_in_token(NODE, uid=7, username=username))

    assert response.status_code == 409
    assert response.json()["detail"] == (
        f"This server already has accounts named {username} and {username}@127.0.0.1; "
        "an administrator removes one of them before this person can sign in here."
    )
    assert mapping(hub, 7) is None


@pytest.mark.parametrize(
    ("token_for", "detail"),
    [
        ({"aud": "other-node"}, "The hub's sign-in token was refused: wrong audience."),
        ({"ttl": -60}, "The hub's sign-in token was refused: expired."),
        ({"scope": "run"}, "The hub's sign-in token was refused: a run token is not accepted here."),
    ],
    ids=["another node", "expired", "scope run"],
)
def test_a_token_this_node_does_not_take_is_401_and_signs_nobody_in(hub, username, token_for, detail):
    claims = {"aud": NODE, "ttl": 120, "scope": "signin", **token_for}
    token = hub.signer.token(sub=username, uid=7, may_act={"sub": f"node:{NODE}"}, **claims)

    response = sign_in(token)

    assert (response.status_code, response.json()["detail"]) == (401, detail)
    assert exchanges(hub) == [] and mapping(hub, 7) is None and accounts_named(username) == 0


def test_an_exchange_the_hub_refuses_is_401_with_its_sentence_and_signs_nobody_in(hub, username):
    hub.refused_people.add(7)

    response = sign_in(hub.sign_in_token(NODE, uid=7, username=username))

    assert response.status_code == 401
    assert response.json()["detail"] == "The hub refused this sign-in: this person may not use this node now."
    assert mapping(hub, 7) is None and accounts_named(username) == 0


def test_a_sign_in_token_is_taken_once(hub, username):
    token = hub.sign_in_token(NODE, uid=7, username=username)
    assert sign_in(token).status_code == 200

    response = sign_in(token)

    assert response.status_code == 401
    assert response.json()["detail"] == "The hub refused this sign-in: this sign-in token has been used."


def test_a_hub_that_stops_answering_after_the_keys_were_read_is_a_502(hub, username):
    token = hub.sign_in_token(NODE, uid=7, username=username)
    assert hub_tokens.verify(token, scopes=("signin",)).scope == "signin"
    hub.close()

    response = sign_in(token)

    assert (response.status_code, response.json()["detail"]) == (502, f"Could not reach the hub at {hub.url}.")
    assert mapping(hub, 7) is None


def test_a_disabled_local_account_is_refused_before_the_hub_is_asked(hub, username):
    user_id = me(sign_in(hub.sign_in_token(NODE, uid=7, username=username)).json()["access_token"])["id"]
    with get_db_context() as db:
        db.query(db_models.User).filter(db_models.User.id == user_id).update({"disabled": True})
        db.commit()

    response = sign_in(hub.sign_in_token(NODE, uid=7, username=username))

    assert (response.status_code, response.json()["detail"]) == (401, "This account is disabled on this server.")
    assert len(exchanges(hub)) == 1


def test_the_answer_of_the_hub_for_another_person_is_refused(hub, username, monkeypatch):
    real = hub._delegate
    monkeypatch.setattr(hub, "_delegate", lambda granted: {**real(granted), "user": {**hub.people[7], "uid": 8}})

    response = sign_in(hub.sign_in_token(NODE, uid=7, username=username))

    detail = "The hub answered this sign-in for another person."
    assert (response.status_code, response.json()["detail"]) == (401, detail)
    assert mapping(hub, 7) is None and mapping(hub, 8) is None


def test_a_server_that_did_not_join_a_hub_with_keys_signs_nobody_in_through_one(hub, node_file, username):
    token = hub.sign_in_token(NODE, uid=7, username=username)
    node_file.with_name(hub_node_key.FILE_NAME).unlink()

    without_keys = sign_in(token)
    node_file.unlink()
    not_joined = sign_in(token)

    for response in (without_keys, not_joined):
        assert (response.status_code, response.json()["detail"]) == (
            401,
            "This server does not sign people in through a hub.",
        )
    assert exchanges(hub) == []


def test_a_server_whose_keys_cannot_be_read_says_so_without_naming_its_storage_dir(hub, node_file, username, caplog):
    token = hub.sign_in_token(NODE, uid=7, username=username)
    key_file = node_file.with_name(hub_node_key.FILE_NAME)
    key_file.write_text("not a node's keys", encoding="utf-8")

    response = sign_in(token)

    assert (response.status_code, response.json()["detail"]) == (401, "This server cannot sign for itself at its hub.")
    assert str(node_file.parent) not in response.text
    assert f"could not read this node's keys at {key_file}" in caplog.text
    assert exchanges(hub) == [] and mapping(hub, 7) is None


def test_a_string_that_is_no_jwt_is_not_a_sign_in_token(hub):
    response = sign_in("not-a-token")

    assert (response.status_code, response.json()["detail"]) == (401, "This is not a sign-in token from the hub.")


def test_a_sign_in_token_is_no_users_token_on_any_other_route(hub, username):
    local = me(sign_in(hub.sign_in_token(NODE, uid=7, username=username)).json()["access_token"])
    token = hub.sign_in_token(NODE, uid=7, username=username)

    for path in ("/auth/users/me", "/auth/users"):
        response = client.get(path, headers=bearer(token))
        assert (response.status_code, response.json()["detail"]) == (
            401,
            "Hub token refused: a signin token is not accepted here",
        )
    assert local["username"] == username and len(exchanges(hub)) == 1


def test_deleting_an_account_drops_its_mapping_so_the_person_gets_a_new_one(hub, username):
    admin = sign_in(hub.sign_in_token(NODE, uid=1, username=f"{username}-admin", node_admin=True)).json()
    person = me(sign_in(hub.sign_in_token(NODE, uid=7, username=username)).json()["access_token"])
    assert session.load(person["id"]) is not None and session.access_token(person["id"]) is not None

    deleted = client.delete(f"/auth/users/{person['id']}", headers=bearer(admin["access_token"]))

    assert deleted.status_code == 200, deleted.text
    assert mapping(hub, 7) is None
    assert session.load(person["id"]) is None and session.access_token(person["id"]) is None
    again = me(sign_in(hub.sign_in_token(NODE, uid=7, username=username)).json()["access_token"])
    assert mapping(hub, 7) == again["id"]


def test_the_sign_in_method_is_the_password_until_the_server_joins_a_hub_with_keys(node_file):
    assert client.get("/auth/sign_in_method").json() == {"method": "password"}


def test_the_sign_in_method_of_a_joined_server_is_the_hubs_open_page_for_this_node(hub, node_file):
    node_id = hub_node.read().node_id

    answer = client.get("/auth/sign_in_method").json()

    assert answer == {"method": "hub", "url": f"{hub.url}/open/{node_id}", "hub_url": hub.url}
    node_file.with_name(hub_node_key.FILE_NAME).unlink()
    assert client.get("/auth/sign_in_method").json() == {"method": "password"}
