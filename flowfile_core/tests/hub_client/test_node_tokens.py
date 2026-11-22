"""The node tokens a connected desktop holds for the hub's compute nodes: minted on first use, kept for the hour,
replaced near expiry and dropped with the hub session. Against the fake hub, through a secure store of the test's own."""

import jwt
import pytest

from flowfile_core.hub_client import client, node_tokens, session
from tests.hub_client.conftest import HUB_URL, compute_node, sign_in_through_hub

USER_ID = 41
TEAM = compute_node(3, "team")
GPU = compute_node(4, "gpu")
OLD = compute_node(5, "old", enabled=False)


@pytest.fixture(autouse=True)
def no_node_tokens(monkeypatch):
    monkeypatch.setattr(node_tokens, "_tokens", {})


@pytest.fixture
def connected(store, hub):
    hub.nodes = [TEAM, GPU, OLD]
    tokens = client.sign_in(HUB_URL, "ana", "s3cret")
    session.save(
        USER_ID, {"hub_url": HUB_URL, "refresh_token": tokens["refresh_token"], "device_name": "laptop"}
    )
    hub.calls.clear()
    return hub


def _claims(token: str) -> dict:
    return jwt.decode(token, options={"verify_signature": False})


def test_the_first_use_mints_a_run_token_for_the_node_through_the_hub_session(connected):
    token = node_tokens.token_for(USER_ID, TEAM["id"], TEAM["name"])

    assert connected.calls == [("POST", "/auth/refresh"), ("POST", "/hub/node-tokens")]
    assert connected.minted == [{"user": "ana", "node_id": TEAM["id"], "scope": "run"}]
    claims = _claims(token)
    assert claims["aud"] == "team"
    assert claims["scope"] == "run"
    assert claims["iss"] == HUB_URL


def test_the_token_is_reused_within_the_hour_without_asking_the_hub(connected):
    first = node_tokens.token_for(USER_ID, TEAM["id"], TEAM["name"])
    connected.calls.clear()

    assert node_tokens.token_for(USER_ID, TEAM["id"], TEAM["name"]) == first
    assert connected.calls == []
    assert len(connected.minted) == 1


def test_a_token_within_a_minute_of_its_expiry_is_replaced(connected):
    connected.node_token_ttl = 30
    first = node_tokens.token_for(USER_ID, TEAM["id"], TEAM["name"])
    connected.calls.clear()

    second = node_tokens.token_for(USER_ID, TEAM["id"], TEAM["name"])

    assert second != first
    assert connected.calls == [("POST", "/hub/node-tokens")]
    assert _claims(second)["jti"] != _claims(first)["jti"]


def test_each_node_gets_its_own_token(connected):
    team = node_tokens.token_for(USER_ID, TEAM["id"], TEAM["name"])
    gpu = node_tokens.token_for(USER_ID, GPU["id"], GPU["name"])

    assert team != gpu
    assert _claims(gpu)["aud"] == "gpu"
    assert [m["node_id"] for m in connected.minted] == [TEAM["id"], GPU["id"]]


def test_clearing_the_session_drops_the_tokens_and_the_next_use_says_to_connect_again(connected):
    node_tokens.token_for(USER_ID, TEAM["id"], TEAM["name"])

    session.clear(USER_ID)

    assert node_tokens._tokens == {}
    with pytest.raises(LookupError, match="connect to the hub again"):
        node_tokens.token_for(USER_ID, TEAM["id"], TEAM["name"])


def test_a_new_session_drops_the_tokens_of_the_old_one(connected):
    node_tokens.token_for(USER_ID, TEAM["id"], TEAM["name"])
    tokens = client.sign_in(HUB_URL, "ana", "s3cret")

    session.save(
        USER_ID, {"hub_url": HUB_URL, "refresh_token": tokens["refresh_token"], "device_name": "laptop"}
    )

    assert node_tokens._tokens == {}


def test_without_a_session_nothing_is_minted(store, hub):
    with pytest.raises(LookupError, match="connect to the hub again"):
        node_tokens.token_for(USER_ID, TEAM["id"], TEAM["name"])
    assert hub.calls == []


def test_an_expired_hub_session_says_to_connect_again_and_caches_nothing(connected):
    connected.live_refresh.clear()

    with pytest.raises(LookupError, match="connect to the hub again"):
        node_tokens.token_for(USER_ID, TEAM["id"], TEAM["name"])
    assert node_tokens._tokens == {}
    assert connected.minted == []


def test_a_node_the_hub_refuses_is_a_lookup_error_with_the_hubs_reason(connected):
    with pytest.raises(LookupError, match="node old is disabled"):
        node_tokens.token_for(USER_ID, OLD["id"], OLD["name"])
    assert node_tokens._tokens == {}


def test_a_hub_that_is_down_is_a_lookup_error_and_caches_nothing(connected):
    connected.down = True

    with pytest.raises(LookupError, match="Could not reach the hub"):
        node_tokens.token_for(USER_ID, TEAM["id"], TEAM["name"])
    assert node_tokens._tokens == {}


def test_clear_forgets_one_users_tokens_only(connected):
    node_tokens.token_for(USER_ID, TEAM["id"], TEAM["name"])
    node_tokens._tokens[(USER_ID + 1, TEAM["id"])] = ("someone-elses", None)

    node_tokens.clear(USER_ID)

    assert list(node_tokens._tokens) == [(USER_ID + 1, TEAM["id"])]


def test_through_the_hub_a_desktop_shaped_session_mints_nothing_and_says_to_sign_in_through_the_hub_again(connected):
    with pytest.raises(LookupError, match="sign in through the hub again$"):
        node_tokens.token_for(USER_ID, TEAM["id"], TEAM["name"], through_hub=True)

    assert connected.minted == []


def test_through_the_hub_each_person_mints_with_their_own_sign_in_and_one_clear_leaves_the_other(store, hub):
    hub.nodes = [TEAM]
    hub.accounts["dee"] = "s3cret"
    sign_in_through_hub(hub, USER_ID, "ana")
    sign_in_through_hub(hub, USER_ID + 1, "dee")

    ana = node_tokens.token_for(USER_ID, TEAM["id"], TEAM["name"], through_hub=True)
    dee = node_tokens.token_for(USER_ID + 1, TEAM["id"], TEAM["name"], through_hub=True)
    session.clear(USER_ID)

    assert (_claims(ana)["sub"], _claims(dee)["sub"]) == ("ana", "dee")
    assert node_tokens.token_for(USER_ID + 1, TEAM["id"], TEAM["name"], through_hub=True) == dee
    with pytest.raises(LookupError, match="sign in through the hub again$"):
        node_tokens.token_for(USER_ID, TEAM["id"], TEAM["name"], through_hub=True)
    assert [minted["user"] for minted in hub.minted] == ["ana", "dee"]
