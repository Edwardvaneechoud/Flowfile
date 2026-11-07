"""A server that joined a hub signs a person in with the hub's one-time sign-in token, says how people sign in, keeps
that sign-in no longer than the hub does, and takes no password sign-in.

The core is in docker mode with its own JWT secret, as a server is. Its storage dir's node joins ``FakeJoinHub`` over
real HTTP with its own keys; the fake serves its signer's keys, signs the sign-in tokens with them, and answers
``/hub/token`` only for an assertion that verifies with the key the join sent. ``hub_node.json``, the keys and the
secure store live under the test's ``tmp_path``.
"""

import json
import time
import uuid

import jwt
import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.auth import secrets as core_secrets
from flowfile_core.auth.jwt import create_refresh_token, invalidate_user_cache
from flowfile_core.auth.password import get_password_hash, verify_password
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.hub_client import client as hub_client
from flowfile_core.hub_client import session
from shared import hub_node, hub_node_key, hub_tokens
from shared.tests import hub_join
from shared.tests.hub_join import REVOKE_PATH, TOKEN_EXCHANGE, TOKEN_PATH, FakeJoinHub
from shared.tests.hub_jwks import HubSigner

client = TestClient(main.app)

NODE = "team-1"
VARIABLES = ("FLOWFILE_HUB_JOIN_TOKEN", "FLOWFILE_HUB_URL", "FLOWFILE_NODE_NAME")
ENDED = "Your sign-in through 127.0.0.1 has ended; sign in again"
THROUGH_THE_HUB = "This server signs people in through 127.0.0.1"


def bearer(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def node_file(node_file, tmp_path, monkeypatch):
    """A server's storage dir of the test's own: docker mode, its own secure store, no hub settings in the variables."""
    monkeypatch.setenv("FLOWFILE_MODE", "docker")
    monkeypatch.setenv("JWT_SECRET_KEY", "test-secret-key-for-hub-sign-in-tests")
    for name in VARIABLES:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(hub_tokens, "_client", None)
    monkeypatch.setattr(hub_client, "_module_http", None)
    secure = tmp_path / "secure_storage"
    monkeypatch.setenv("FLOWFILE_SECURE_STORAGE_PATH", str(secure))
    monkeypatch.setenv("SECURE_STORAGE_PATH", str(secure))
    monkeypatch.setattr(core_secrets, "_storage", core_secrets.SecureStorage())
    monkeypatch.setattr(session, "_access_tokens", {})
    invalidate_user_cache()
    yield node_file
    invalidate_user_cache()


def join(fake: FakeJoinHub) -> None:
    """This server's storage dir joins ``fake`` as ``NODE`` with keys of its own, enabled."""
    hub_node.join(
        fake.url,
        fake.mint(enable_on_join=True),
        name=NODE,
        worker_url="http://team-1.example:63579",
        core_url="http://team-1.example:63578",
    )


@pytest.fixture
def hub(node_file):
    """A hub this server joined with its own keys, enabled; the accounts its people got are removed afterwards."""
    signer = HubSigner()
    fake = FakeJoinHub(signer)
    join(fake)
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
    assert set(answer) == {"access_token", "refresh_token", "token_type", "expires_at"}
    assert answer["token_type"] == "bearer"
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
    assert set(stored) == {"kind", "hub_url", "refresh_token", "refresh_expires_at", "sid"}
    assert jwt.decode(answer["refresh_token"], options={"verify_signature": False})["sid"] == stored["sid"]


def test_the_delegation_is_written_nowhere_but_the_session_store(hub, username):
    response = sign_in(hub.sign_in_token(NODE, uid=7, username=username))
    user_id = me(response.json()["access_token"])["id"]

    refresh_token = session.load(user_id)["refresh_token"]
    with get_db_context() as db:
        row = db.query(db_models.User).filter(db_models.User.id == user_id).one()
        columns = {column.name: str(getattr(row, column.name)) for column in row.__table__.columns}
    assert refresh_token not in json.dumps(columns) and refresh_token not in response.text
    assert columns["hashed_password"].startswith("$2b$")


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
    with get_db_context() as db:
        local = db.query(db_models.User).filter(db_models.User.id == local_id).one()
        assert local.username == username and verify_password("local-password-1!", local.hashed_password)


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


def test_a_hub_person_named_as_the_internal_service_gets_the_name_at_the_hub_host(hub):
    response = sign_in(hub.sign_in_token(NODE, uid=7, username="_internal_service"))

    assert response.status_code == 200, response.text
    person = me(response.json()["access_token"])
    assert person["username"] == "_internal_service@127.0.0.1"
    assert mapping(hub, 7) == person["id"]
    with get_db_context() as db:
        assert db.query(db_models.User).filter(db_models.User.username == "_internal_service").count() == 0


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


def test_a_sign_in_token_the_hub_signed_without_a_person_id_signs_nobody_in(hub, username):
    token = hub.signer.token(scope="signin", sub=username, uid=None, aud=NODE, ttl=120, may_act={"sub": f"node:{NODE}"})

    response = sign_in(token)

    assert (response.status_code, response.json()["detail"]) == (401, "This is not a sign-in token from the hub.")
    assert exchanges(hub) == [] and accounts_named(username) == 0


def test_a_sign_in_token_is_no_users_token_on_any_other_route(hub, username):
    local = me(sign_in(hub.sign_in_token(NODE, uid=7, username=username)).json()["access_token"])
    token = hub.sign_in_token(NODE, uid=7, username=username)

    for path in ("/auth/users/me", "/auth/users"):
        response = client.get(path, headers=bearer(token))
        assert (response.status_code, response.json()["detail"]) == (401, "Could not validate credentials")
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


def refresh(refresh_token: str):
    return client.post("/auth/refresh", data={"refresh_token": refresh_token})


def password_sign_in(name: str, password: str = "local-password-1!"):
    return client.post("/auth/token", data={"username": name, "password": password})


def lives(access_token: str) -> float:
    """Seconds until core's access token expires, read from its own ``exp``."""
    return jwt.decode(access_token, options={"verify_signature": False})["exp"] - time.time()


def answers_its_expiry(answer: dict) -> bool:
    """The answer's ``expires_at`` (milliseconds) is the access token's ``exp``, at most a second early."""
    exp = jwt.decode(answer["access_token"], options={"verify_signature": False})["exp"]
    return exp * 1000 - 1000 <= answer["expires_at"] <= exp * 1000


def refreshes(hub: FakeJoinHub) -> list[dict]:
    return [request for request in hub.token_requests if request["grant_type"] == "refresh_token"]


def revocations(hub: FakeJoinHub) -> list[dict]:
    return [form for path, form in hub.calls if path == REVOKE_PATH]


def test_cores_access_token_for_a_person_from_the_hub_lives_as_long_as_the_hub_says(hub, username, monkeypatch):
    monkeypatch.setattr(hub_join, "ACCESS_SECONDS", 7 * 60)

    answer = sign_in(hub.sign_in_token(NODE, uid=7, username=username)).json()

    assert 7 * 60 - 5 <= lives(answer["access_token"]) <= 7 * 60
    assert answers_its_expiry(answer)


def test_a_refresh_of_cores_session_refreshes_the_sign_in_at_the_hub_first(hub, username, monkeypatch):
    answer = sign_in(hub.sign_in_token(NODE, uid=7, username=username)).json()
    user_id = me(answer["access_token"])["id"]
    first = session.load(user_id)["refresh_token"]
    monkeypatch.setattr(hub_join, "ACCESS_SECONDS", 7 * 60)

    response = refresh(answer["refresh_token"])

    assert response.status_code == 200, response.text
    renewed = response.json()
    assert me(renewed["access_token"])["id"] == user_id
    assert (
        7 * 60 - 5 <= lives(renewed["access_token"]) <= 7 * 60
    ), "the refreshed token lives as long as the hub's answer"
    assert answers_its_expiry(renewed)
    [taken] = refreshes(hub)
    assert (taken["claims"]["sub"], taken["claims"]["aud"]) == (f"node:{NODE}", hub.issuer + TOKEN_PATH)
    rotated = session.load(user_id)["refresh_token"]
    assert rotated != first and rotated in hub.live_refresh_tokens() and first not in hub.live_refresh_tokens()
    assert refresh(renewed["refresh_token"]).status_code == 200
    assert len(refreshes(hub)) == 2


def test_a_refresh_through_the_hub_applies_what_the_hub_now_says_of_the_person(hub, username):
    answer = sign_in(hub.sign_in_token(NODE, uid=7, username=username, full_name="Ana Lima", node_admin=True)).json()
    assert me(answer["access_token"])["is_admin"] is True
    hub.people[7].update(node_admin=False, full_name="Ana Souza")

    demoted = refresh(answer["refresh_token"]).json()

    person = me(demoted["access_token"])
    assert (person["is_admin"], person["full_name"]) == (False, "Ana Souza")
    assert client.get("/auth/users", headers=bearer(answer["access_token"])).status_code == 403
    hub.people[7]["node_admin"] = True
    assert me(refresh(demoted["refresh_token"]).json()["access_token"])["is_admin"] is True


@pytest.mark.parametrize(
    "end",
    [lambda hub: setattr(hub, "refuse_next_refresh", True), lambda hub: hub.refused_people.add(7)],
    ids=["the hub ended the sign-in", "the person may no longer use this node"],
)
def test_once_the_hub_refuses_the_refresh_core_issues_nothing_and_keeps_nothing_of_the_sign_in(hub, username, end):
    answer = sign_in(hub.sign_in_token(NODE, uid=7, username=username)).json()
    user_id = me(answer["access_token"])["id"]
    end(hub)

    response = refresh(answer["refresh_token"])

    assert (response.status_code, response.json()) == (401, {"detail": ENDED})
    assert session.load(user_id) is None and session.access_token(user_id) is None
    calls = len(hub.calls)
    again = refresh(answer["refresh_token"])
    assert (again.status_code, again.json()["detail"]) == (401, ENDED)
    assert len(hub.calls) == calls


def test_a_refresh_the_hub_refuses_this_node_is_401_and_issues_nothing(hub, username):
    answer = sign_in(hub.sign_in_token(NODE, uid=7, username=username)).json()
    hub.set_enabled(NODE, False)

    response = refresh(answer["refresh_token"])

    assert (response.status_code, response.json()) == (401, {"detail": ENDED})


def test_a_refresh_while_the_hub_does_not_answer_is_a_502_and_keeps_the_sign_in(hub, username):
    answer = sign_in(hub.sign_in_token(NODE, uid=7, username=username)).json()
    user_id = me(answer["access_token"])["id"]
    before = session.load(user_id)
    hub.close()

    response = refresh(answer["refresh_token"])

    assert (response.status_code, response.json()) == (502, {"detail": f"Could not reach the hub at {hub.url}."})
    assert session.load(user_id) == before


def test_a_password_session_from_before_the_join_is_not_renewed_once_the_server_joined_with_keys(
    node_file, local_user, username
):
    local_user(username)
    before_the_join = password_sign_in(username)
    assert before_the_join.status_code == 200, before_the_join.text
    signer = HubSigner()
    fake = FakeJoinHub(signer)
    try:
        join(fake)

        response = refresh(before_the_join.json()["refresh_token"])
    finally:
        fake.close()
        signer.close()

    assert (response.status_code, response.json()) == (403, {"detail": THROUGH_THE_HUB})
    assert fake.token_requests == []


def test_a_refresh_of_an_account_not_from_the_hub_is_unchanged_on_a_server_that_did_not_join_with_keys(
    hub, node_file, local_user, username
):
    local_user(username)
    node_file.with_name(hub_node_key.FILE_NAME).unlink()
    without_keys = refresh(create_refresh_token(data={"sub": username}))
    node_file.unlink()
    not_joined = refresh(create_refresh_token(data={"sub": username}))

    for response in (without_keys, not_joined):
        assert response.status_code == 200, response.text
        assert lives(response.json()["access_token"]) > 60 * 60
        assert response.json()["expires_at"] is None
    assert hub.token_requests == []


def test_logout_revokes_the_sign_in_at_the_hub_and_forgets_it_here(hub, username):
    answer = sign_in(hub.sign_in_token(NODE, uid=7, username=username)).json()
    user_id = me(answer["access_token"])["id"]
    refresh_token = session.load(user_id)["refresh_token"]

    response = client.post("/auth/logout", headers=bearer(answer["access_token"]))

    assert (response.status_code, response.content) == (204, b"")
    [form] = revocations(hub)
    assert (form["token"], form["token_type_hint"]) == (refresh_token, "refresh_token")
    assert refresh_token not in hub.live_refresh_tokens()
    assert session.load(user_id) is None and session.access_token(user_id) is None
    after = refresh(answer["refresh_token"])
    assert (after.status_code, after.json()["detail"]) == (401, ENDED)


@pytest.mark.parametrize(
    "refuse",
    [lambda hub: hub.close(), lambda hub: hub.set_enabled(NODE, False)],
    ids=["the hub does not answer", "the hub refuses this node"],
)
def test_logout_answers_204_and_forgets_the_sign_in_when_the_hub_does_not_take_the_revocation(hub, username, refuse):
    answer = sign_in(hub.sign_in_token(NODE, uid=7, username=username)).json()
    user_id = me(answer["access_token"])["id"]
    refuse(hub)

    response = client.post("/auth/logout", headers=bearer(answer["access_token"]))

    assert response.status_code == 204
    assert session.load(user_id) is None and session.access_token(user_id) is None


def leave_the_hub(node_file) -> None:
    node_file.unlink()


def lose_the_keys(node_file) -> None:
    node_file.with_name(hub_node_key.FILE_NAME).unlink()


def garble_the_keys(node_file) -> None:
    node_file.with_name(hub_node_key.FILE_NAME).write_text("not a node's keys", encoding="utf-8")


cannot_sign = pytest.mark.parametrize(
    "lose", [leave_the_hub, lose_the_keys, garble_the_keys], ids=["left the hub", "keys gone", "keys unreadable"]
)


@cannot_sign
def test_logout_answers_204_and_forgets_the_sign_in_when_this_server_cannot_sign_for_itself(
    hub, node_file, username, lose, caplog
):
    answer = sign_in(hub.sign_in_token(NODE, uid=7, username=username)).json()
    user_id = me(answer["access_token"])["id"]
    lose(node_file)

    response = client.post("/auth/logout", headers=bearer(answer["access_token"]))

    assert (response.status_code, response.content) == (204, b"")
    assert session.load(user_id) is None and session.access_token(user_id) is None
    assert revocations(hub) == []
    assert "The hub was not told of a sign-out on this server" in caplog.text


@cannot_sign
def test_a_refresh_this_server_cannot_sign_for_at_its_hub_is_401_naming_the_hub_and_keeps_the_sign_in(
    hub, node_file, username, lose
):
    answer = sign_in(hub.sign_in_token(NODE, uid=7, username=username)).json()
    user_id = me(answer["access_token"])["id"]
    before = session.load(user_id)
    calls = len(hub.calls)
    lose(node_file)

    response = refresh(answer["refresh_token"])

    assert (response.status_code, response.json()) == (401, {"detail": ENDED})
    assert len(hub.calls) == calls and session.load(user_id) == before


def sign_out_everywhere(hub: FakeJoinHub, answer: dict) -> None:
    """The person signs out everywhere on the hub; this browser learns it at its next refresh."""
    hub.sign_out_everywhere(7)
    assert refresh(answer["refresh_token"]).json() == {"detail": ENDED}


def log_out(hub: FakeJoinHub, answer: dict) -> None:
    assert client.post("/auth/logout", headers=bearer(answer["access_token"])).status_code == 204


@pytest.mark.parametrize("end", [sign_out_everywhere, log_out], ids=["signed out everywhere", "logged out here"])
def test_a_sign_in_that_ended_is_not_renewed_by_a_later_sign_in_of_the_same_person(hub, username, end):
    ended = sign_in(hub.sign_in_token(NODE, uid=7, username=username)).json()
    end(hub, ended)
    later = sign_in(hub.sign_in_token(NODE, uid=7, username=username)).json()
    calls = len(hub.calls)

    response = refresh(ended["refresh_token"])

    assert (response.status_code, response.json()) == (401, {"detail": ENDED})
    assert len(hub.calls) == calls
    assert refresh(later["refresh_token"]).status_code == 200


def test_a_second_sign_in_of_the_same_person_ends_the_first_browsers_session_at_its_next_refresh(hub, username):
    first = sign_in(hub.sign_in_token(NODE, uid=7, username=username)).json()
    second = sign_in(hub.sign_in_token(NODE, uid=7, username=username)).json()
    calls = len(hub.calls)

    response = refresh(first["refresh_token"])

    assert (response.status_code, response.json()) == (401, {"detail": ENDED})
    assert len(hub.calls) == calls
    assert refresh(second["refresh_token"]).status_code == 200


def test_a_refresh_token_that_names_no_sign_in_renews_none(hub, username):
    me(sign_in(hub.sign_in_token(NODE, uid=7, username=username)).json()["access_token"])
    calls = len(hub.calls)

    response = refresh(create_refresh_token(data={"sub": username}))

    assert (response.status_code, response.json()) == (401, {"detail": ENDED})
    assert len(hub.calls) == calls


def test_logout_of_an_account_without_a_sign_in_through_the_hub_keeps_its_hub_connection(
    node_file, local_user, username
):
    user_id = local_user(username)
    signed_in = password_sign_in(username)
    assert signed_in.status_code == 200, signed_in.text
    session.save(user_id, {"hub_url": "https://hub.example.test", "refresh_token": "a desktop's refresh token"})
    before = session.load(user_id)

    response = client.post("/auth/logout", headers=bearer(signed_in.json()["access_token"]))

    assert response.status_code == 204
    assert session.load(user_id) == before


def test_no_password_signs_anyone_in_on_a_server_that_joined_its_hub_with_keys(hub, local_user, username):
    local_user(username)

    for response in (
        password_sign_in(username),
        password_sign_in(username, "not the password"),
        client.post("/auth/token"),
    ):
        assert (response.status_code, response.json()) == (403, {"detail": THROUGH_THE_HUB})


def test_password_sign_in_is_unchanged_on_a_server_that_did_not_join_with_keys(hub, node_file, local_user, username):
    local_user(username)
    node_file.with_name(hub_node_key.FILE_NAME).unlink()
    without_keys = password_sign_in(username)
    node_file.unlink()
    not_joined = password_sign_in(username)

    for response in (without_keys, not_joined):
        assert response.status_code == 200, response.text
        assert me(response.json()["access_token"])["username"] == username
    wrong = password_sign_in(username, "not the password")
    assert (wrong.status_code, wrong.json()["detail"]) == (401, "Incorrect username or password")


def test_the_desktops_own_sign_in_is_unchanged_on_a_storage_dir_that_joined_a_hub(hub, monkeypatch):
    monkeypatch.setenv("FLOWFILE_MODE", "electron")

    response = client.post("/auth/token")

    assert response.status_code == 200, response.text
    assert jwt.decode(response.json()["access_token"], options={"verify_signature": False})["sub"] == "local_user"
