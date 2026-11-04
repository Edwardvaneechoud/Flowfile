"""A server's delegation through its hub: the token exchange, the refresh and the revocation, each signed by the node,
and the session that keeps it, refreshed through the hub until the hub ends it.

The node joins ``FakeJoinHub`` over real HTTP, which keeps the public keys the join sent and checks every assertion
with them; the sign-in tokens are signed by the fake's own key and name the fake's issuer, which is not its URL, so an
audience built from the URL would be refused. ``hub_node.json`` and the keys live under the test's ``tmp_path``.
"""

import json
import logging
import time

import jwt
import pytest

from flowfile_core.auth import secrets as core_secrets
from flowfile_core.configs import logger as core_logger
from flowfile_core.hub_client import client, delegation, node_tokens, session
from flowfile_core.hub_client.client import HubRefused, HubUnreachable
from flowfile_core.hub_client.delegation import REVOKE_PATH, TOKEN_PATH, Delegation
from shared import hub_node, hub_node_key
from shared.tests.hub_join import ACCESS_SECONDS, DELEGATION_SECONDS, TOKEN_EXCHANGE, FakeJoinHub
from shared.tests.hub_jwks import HubSigner

NODE = "team-1"
USER_ID = 41
VARIABLES = ("FLOWFILE_HUB_JOIN_TOKEN", "FLOWFILE_HUB_URL", "FLOWFILE_NODE_NAME", "FLOWFILE_HUB_ISSUER")


@pytest.fixture
def node_file(tmp_path, monkeypatch):
    target = tmp_path / "storage" / hub_node.FILE_NAME
    monkeypatch.setattr(hub_node, "path", lambda: target)
    monkeypatch.setattr(client, "_module_http", None)
    for name in VARIABLES:
        monkeypatch.delenv(name, raising=False)
    return target


@pytest.fixture
def hub(node_file):
    """A hub with a signer, joined by this storage dir's node as ``NODE``, enabled."""
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


def sign_in(hub: FakeJoinHub, **claims) -> Delegation:
    token = hub.sign_in_token(NODE, uid=7, username="ana", full_name="Ana Lima", **claims)
    return delegation.exchange(hub.url, hub.issuer, token)


def refusal(call, *args) -> HubRefused:
    with pytest.raises(HubRefused) as refused:
        call(*args)
    return refused.value


def sent(hub: FakeJoinHub, path: str) -> list[dict]:
    return [form for called, form in hub.calls if called == path]


def test_a_sign_in_token_is_traded_for_a_delegation_with_an_assertion_the_hub_verifies(hub):
    token = hub.sign_in_token(NODE, uid=7, username="ana", full_name="Ana Lima", node_admin=True)

    answer = delegation.exchange(hub.url, hub.issuer, token)

    assert answer.access_token.startswith("hub-access-") and answer.refresh_token in hub.live_refresh_tokens()
    assert answer.expires_in == ACCESS_SECONDS
    assert DELEGATION_SECONDS - 5 <= answer.refresh_expires_in <= DELEGATION_SECONDS
    assert answer.user == {"uid": 7, "username": "ana", "full_name": "Ana Lima", "node_admin": True}
    [taken] = hub.token_requests
    assert taken["grant_type"] == TOKEN_EXCHANGE
    assert taken["header"] == {"alg": "ES256", "kid": hub_node_key.load().sig_kid, "typ": "JWT"}
    claims = taken["claims"]
    assert (claims["iss"], claims["sub"], claims["aud"]) == (f"node:{NODE}", f"node:{NODE}", hub.issuer + TOKEN_PATH)
    assert claims["exp"] - claims["iat"] == 60
    [form] = sent(hub, TOKEN_PATH)
    assert {name: form[name] for name in form if name != "client_assertion"} == {
        "grant_type": TOKEN_EXCHANGE,
        "subject_token": token,
        "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
        "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
    }


def test_the_repr_of_a_delegation_shows_neither_token(hub):
    answer = sign_in(hub)

    text = repr(answer)

    assert answer.access_token not in text and answer.refresh_token not in text
    assert "ana" in text


def test_a_spent_sign_in_token_is_the_hubs_invalid_grant_with_its_sentence(hub):
    token = hub.sign_in_token(NODE, uid=7, username="ana")
    delegation.exchange(hub.url, hub.issuer, token)

    refused = refusal(delegation.exchange, hub.url, hub.issuer, token)

    assert (refused.status, refused.detail) == (400, "this sign-in token has been used")
    assert refused.payload == {"error": "invalid_grant", "error_description": "this sign-in token has been used"}


def test_a_sign_in_token_for_another_node_is_refused(hub):
    token = hub.sign_in_token("other-node", uid=7, username="ana")

    refused = refusal(delegation.exchange, hub.url, hub.issuer, token)

    assert (refused.status, refused.detail) == (400, "this sign-in token is for another node")


def test_a_person_who_may_not_use_the_node_is_refused(hub):
    hub.refused_people.add(7)

    refused = refusal(sign_in, hub)

    assert (refused.status, refused.payload["error"]) == (400, "invalid_grant")
    assert refused.detail == "this person may not use this node now"


def test_a_node_the_hub_disabled_is_an_invalid_client(hub):
    hub.set_enabled(NODE, False)

    refused = refusal(sign_in, hub)

    assert (refused.status, refused.payload["error"]) == (401, "invalid_client")
    assert refused.detail == "this node is disabled on the hub"
    assert hub.token_requests == []


def test_an_assertion_addressed_to_the_hubs_url_instead_of_its_issuer_is_refused(hub):
    token = hub.sign_in_token(NODE, uid=7, username="ana")

    refused = refusal(delegation.exchange, hub.url, hub.url, token)

    assert (refused.status, refused.detail) == (401, "this node assertion is refused (InvalidAudienceError)")


def test_a_refresh_rotates_the_refresh_token_and_the_old_one_is_refused_after(hub):
    first = sign_in(hub)
    time.sleep(1.1)

    second = delegation.refresh(hub.url, hub.issuer, first.refresh_token)

    assert second.refresh_token != first.refresh_token and second.access_token != first.access_token
    assert second.refresh_expires_in < first.refresh_expires_in
    assert second.user["username"] == "ana"
    assert [request["grant_type"] for request in hub.token_requests] == [TOKEN_EXCHANGE, "refresh_token"]
    assert hub.token_requests[1]["claims"]["aud"] == hub.issuer + TOKEN_PATH
    form = sent(hub, TOKEN_PATH)[1]
    assert (form["grant_type"], form["refresh_token"]) == ("refresh_token", first.refresh_token)
    refused = refusal(delegation.refresh, hub.url, hub.issuer, first.refresh_token)
    assert (refused.status, refused.payload["error"]) == (400, "invalid_grant")


def test_a_refresh_answers_what_the_hub_knows_of_the_person_now(hub):
    first = sign_in(hub)
    hub.people[7] = {**hub.people[7], "full_name": "Ana Lima-Souza", "node_admin": True}

    second = delegation.refresh(hub.url, hub.issuer, first.refresh_token)

    assert (second.user["full_name"], second.user["node_admin"]) == ("Ana Lima-Souza", True)


def test_a_revoked_delegation_refreshes_no_more_and_the_revocation_is_signed_for_its_own_route(hub):
    answer = sign_in(hub)

    assert delegation.revoke(hub.url, hub.issuer, answer.refresh_token) is None

    assert answer.refresh_token not in hub.live_refresh_tokens()
    [form] = sent(hub, REVOKE_PATH)
    assert (form["token"], form["token_type_hint"]) == (answer.refresh_token, "refresh_token")
    claims = jwt.decode(form["client_assertion"], options={"verify_signature": False})
    assert claims["aud"] == hub.issuer + REVOKE_PATH
    refused = refusal(delegation.refresh, hub.url, hub.issuer, answer.refresh_token)
    assert refused.payload["error"] == "invalid_grant"


def test_revoking_a_token_the_hub_does_not_know_answers_the_same(hub):
    assert delegation.revoke(hub.url, hub.issuer, "hub-refresh-unknown") is None


def test_a_node_without_keys_of_its_own_sends_nothing(hub, node_file):
    token = hub.sign_in_token(NODE, uid=7, username="ana")
    node_file.with_name(hub_node_key.FILE_NAME).unlink()

    with pytest.raises(LookupError, match="this node has no keys of its own at"):
        delegation.exchange(hub.url, hub.issuer, token)

    assert sent(hub, TOKEN_PATH) == []
    assert not node_file.with_name(hub_node_key.FILE_NAME).exists()


def test_a_storage_dir_that_never_joined_sends_nothing(node_file):
    with pytest.raises(LookupError, match="no node has joined a hub from"):
        delegation.refresh("http://127.0.0.1:9", "http://127.0.0.1:9", "hub-refresh-x")


def test_a_hub_that_does_not_answer_is_unreachable(hub):
    token = hub.sign_in_token(NODE, uid=7, username="ana")

    with pytest.raises(HubUnreachable, match="Could not reach the hub at http://127.0.0.1:9."):
        delegation.exchange("http://127.0.0.1:9", hub.issuer, token)


@pytest.mark.parametrize(
    "answer",
    [
        {},
        {"access_token": "a", "refresh_token": "r", "token_type": "Bearer", "expires_in": 900, "refresh_expires_in": 9},
        {
            "access_token": "a",
            "refresh_token": "r",
            "token_type": "Bearer",
            "expires_in": "900",
            "refresh_expires_in": 9,
            "user": {"uid": 7, "username": "ana", "full_name": None, "node_admin": False},
        },
        {
            "access_token": "a",
            "refresh_token": "r",
            "token_type": "Bearer",
            "expires_in": 900,
            "refresh_expires_in": 9,
            "user": {"uid": 7, "username": "ana", "full_name": None, "node_admin": "yes"},
        },
    ],
)
def test_an_answer_that_is_not_a_delegation_is_refused(hub, monkeypatch, answer):
    monkeypatch.setattr(hub, "token", lambda form: (200, answer))

    refused = refusal(sign_in, hub)

    assert (refused.status, refused.detail) == (200, f"{hub.url} did not answer /hub/token as a Flowfile hub does.")


def test_the_log_names_the_route_and_status_and_never_a_token(hub, caplog):
    core_logger.addHandler(caplog.handler)  # core's logger does not propagate, so caplog sees it only through this
    try:
        with caplog.at_level(logging.DEBUG):
            answer = sign_in(hub)
            delegation.refresh(hub.url, hub.issuer, answer.refresh_token)
    finally:
        core_logger.removeHandler(caplog.handler)

    assert "hub POST /hub/token -> 200" in caplog.text
    assert "eyJ" not in caplog.text and "hub-refresh-" not in caplog.text and "hub-access-" not in caplog.text


def test_a_delegation_is_kept_as_the_users_session_with_its_access_token_in_memory_only(hub, store):
    answer = sign_in(hub)
    before = time.time()

    sid = session.save_delegation(USER_ID, hub.url + "/", answer)

    stored = json.loads(core_secrets.get_password(session.SERVICE, f"session:{USER_ID}"))
    assert set(stored) == {"kind", "hub_url", "refresh_token", "refresh_expires_at", "sid"}
    assert stored["sid"] == sid and len(sid) == 32
    assert (stored["kind"], stored["hub_url"], stored["refresh_token"]) == ("delegation", hub.url, answer.refresh_token)
    expires_at = stored["refresh_expires_at"]
    assert before + answer.refresh_expires_in - 1 <= expires_at <= time.time() + answer.refresh_expires_in
    assert answer.access_token not in json.dumps(stored)
    calls = len(hub.calls)
    assert session.access_token(USER_ID) == answer.access_token
    assert len(hub.calls) == calls


def test_a_delegation_replaces_a_desktop_connection_and_its_cached_access_token(hub, store):
    session._access_tokens[USER_ID] = ("an earlier session's token", time.time() + 900)
    core_secrets.set_password(session.SERVICE, f"session:{USER_ID}", json.dumps({"hub_url": "https://old.example"}))
    answer = sign_in(hub)

    session.save_delegation(USER_ID, hub.url, answer)

    assert session.load(USER_ID)["kind"] == "delegation"
    assert session.access_token(USER_ID) == answer.access_token


@pytest.fixture
def minted(monkeypatch) -> dict:
    """This process's node tokens, the test's own and empty."""
    tokens: dict = {}
    monkeypatch.setattr(node_tokens, "_tokens", tokens)
    return tokens


def mint_for_node_3(minted: dict) -> None:
    minted[(USER_ID, 3)] = ("a node token minted through the sign-in", time.time() + 3600)


def signed_in_session(hub: FakeJoinHub, **claims) -> Delegation:
    """A delegation kept as ``USER_ID``'s session, its cached access token made to be within a minute of its end."""
    answer = sign_in(hub, **claims)
    session.save_delegation(USER_ID, hub.url, answer)
    session._access_tokens[USER_ID] = (answer.access_token, time.time() + 30)
    return answer


def refreshes(hub: FakeJoinHub) -> list[dict]:
    return [request for request in hub.token_requests if request["grant_type"] == "refresh_token"]


def test_an_access_token_near_its_end_is_refreshed_through_the_hub_and_the_rotated_token_kept(hub, store):
    first = signed_in_session(hub)

    token = session.access_token(USER_ID)

    assert token.startswith("hub-access-") and token != first.access_token
    [refreshed] = refreshes(hub)
    assert refreshed["claims"]["aud"] == hub.issuer + TOKEN_PATH
    assert [form["refresh_token"] for form in sent(hub, TOKEN_PATH)[1:]] == [first.refresh_token]
    stored = session.load(USER_ID)
    assert stored["kind"] == "delegation" and stored["refresh_token"] in hub.live_refresh_tokens()
    assert first.refresh_token not in hub.live_refresh_tokens()
    assert time.time() + DELEGATION_SECONDS - 10 <= stored["refresh_expires_at"] <= time.time() + DELEGATION_SECONDS
    assert session.access_token(USER_ID) == token
    assert len(refreshes(hub)) == 1


def test_a_refresh_the_hub_answers_invalid_grant_clears_the_session_and_raises(hub, store, minted):
    signed_in_session(hub)
    mint_for_node_3(minted)
    hub.refuse_next_refresh = True

    refused = refusal(session.access_token, USER_ID)

    assert (refused.status, refused.detail) == (400, "this sign-in was ended on the hub")
    assert refused.payload["error"] == "invalid_grant"
    assert session.load(USER_ID) is None and USER_ID not in session._access_tokens
    assert minted == {}
    calls = len(hub.calls)
    assert session.access_token(USER_ID) is None
    assert len(hub.calls) == calls


def test_a_person_no_longer_allowed_on_the_node_has_the_session_cleared(hub, store):
    signed_in_session(hub)
    hub.refused_people.add(7)

    refused = refusal(session.access_token, USER_ID)

    assert (refused.detail, refused.payload["error"]) == ("this person may not use this node now", "invalid_grant")
    assert session.load(USER_ID) is None


def test_a_refresh_refused_for_another_reason_keeps_the_session(hub, store):
    signed_in_session(hub)
    before = session.load(USER_ID)
    hub.set_enabled(NODE, False)

    refused = refusal(session.access_token, USER_ID)

    assert (refused.status, refused.payload["error"]) == (401, "invalid_client")
    assert session.load(USER_ID) == before


def test_a_hub_that_does_not_answer_the_refresh_keeps_the_session(hub, store):
    signed_in_session(hub)
    before = session.load(USER_ID)
    hub.close()

    with pytest.raises(HubUnreachable):
        session.access_token(USER_ID)

    assert session.load(USER_ID) == before


def test_a_node_that_lost_its_keys_cannot_refresh_and_keeps_the_session(hub, store, node_file, caplog):
    signed_in_session(hub)
    before = session.load(USER_ID)
    calls = len(hub.calls)
    node_file.with_name(hub_node_key.FILE_NAME).unlink()

    refused = refusal(session.access_token, USER_ID)

    assert (refused.status, refused.detail) == (401, "This server cannot sign for itself at its hub.")
    assert len(hub.calls) == calls and session.load(USER_ID) == before
    assert "this node has no keys of its own at" in caplog.text


def test_refresh_delegation_asks_the_hub_whatever_access_token_is_cached(hub, store):
    first = sign_in(hub)
    sid = session.save_delegation(USER_ID, hub.url, first)

    refreshed = session.refresh_delegation(USER_ID, sid)

    assert refreshed.refresh_token == session.load(USER_ID)["refresh_token"] != first.refresh_token
    assert session.load(USER_ID)["sid"] == sid
    assert session.access_token(USER_ID) == refreshed.access_token
    assert len(refreshes(hub)) == 1


def test_refresh_delegation_without_a_sign_in_through_the_hub_asks_nothing(hub, store):
    assert session.refresh_delegation(USER_ID, "a sign-in") is None
    core_secrets.set_password(session.SERVICE, f"session:{USER_ID}", json.dumps({"hub_url": hub.url}))
    assert session.refresh_delegation(USER_ID, "a sign-in") is None
    assert sent(hub, TOKEN_PATH) == []


@pytest.mark.parametrize("sid", ["another sign-in", None], ids=["another sign-in", "no sign-in named"])
def test_refresh_delegation_of_a_sign_in_that_is_not_the_one_kept_asks_nothing_and_keeps_it(hub, store, sid):
    signed_in_session(hub)
    before = session.load(USER_ID)

    assert session.refresh_delegation(USER_ID, sid) is None

    assert session.load(USER_ID) == before
    assert sent(hub, TOKEN_PATH)[1:] == []


def test_refresh_delegation_the_hub_refuses_clears_the_session_and_its_node_tokens(hub, store, minted):
    signed_in_session(hub)
    mint_for_node_3(minted)
    hub.refuse_next_refresh = True

    refused = refusal(session.refresh_delegation, USER_ID, session.load(USER_ID)["sid"])

    assert refused.payload["error"] == "invalid_grant"
    assert session.load(USER_ID) is None and minted == {}


@pytest.mark.timeout(60)
def test_a_node_token_asked_for_when_the_hub_ended_the_sign_in_is_refused_and_forgotten(hub, store, minted):
    signed_in_session(hub)
    mint_for_node_3(minted)
    hub.refuse_next_refresh = True

    with pytest.raises(LookupError, match="the hub refused a token for node 'team-2': this sign-in was ended"):
        node_tokens.token_for(USER_ID, 4, "team-2")

    assert session.load(USER_ID) is None and minted == {}
