"""The stored hub session and the access tokens minted from it, in a secure store of the test's own."""

import json

import jwt
import pytest

from flowfile_core.auth import secrets as core_secrets
from flowfile_core.hub_client import client, session
from tests.hub_client.conftest import HUB_URL

USER_ID = 41


def _connect(hub) -> dict:
    tokens = client.sign_in(HUB_URL, "ana", "s3cret")
    session.save(
        USER_ID, {"hub_url": HUB_URL, "refresh_token": tokens["refresh_token"], "device_name": "laptop"}
    )
    return tokens


def _raw(user_id: int = USER_ID) -> dict:
    return json.loads(core_secrets.get_password(session.SERVICE, f"session:{user_id}"))


def test_save_stores_one_json_string_under_the_users_key(store, hub):
    tokens = _connect(hub)
    stored = _raw()
    assert set(stored) == {"hub_url", "refresh_token", "refresh_expires_at", "device_name"}
    assert stored["refresh_token"] == tokens["refresh_token"]
    exp = jwt.decode(tokens["refresh_token"], options={"verify_signature": False})["exp"]
    assert stored["refresh_expires_at"] == exp
    assert session.load(USER_ID) == stored
    assert session.load(USER_ID + 1) is None


def test_clear_forgets_the_session(store, hub):
    _connect(hub)
    session.clear(USER_ID)
    assert session.load(USER_ID) is None
    assert session.access_token(USER_ID) is None


def test_access_token_mints_through_refresh_when_none_is_cached_and_stores_the_rotated_token(store, hub):
    first = _connect(hub)
    hub.calls.clear()

    token = session.access_token(USER_ID)

    assert token
    assert hub.calls == [("POST", "/auth/refresh")]
    rotated = _raw()["refresh_token"]
    assert rotated != first["refresh_token"]
    assert rotated in hub.live_refresh


def test_access_token_reuses_a_cached_token_far_from_expiry(store, hub):
    _connect(hub)
    token = session.access_token(USER_ID)
    hub.calls.clear()

    assert session.access_token(USER_ID) == token
    assert hub.calls == []


def test_access_token_refreshes_within_a_minute_of_expiry(store, hub):
    hub.access_ttl = 30
    _connect(hub)
    first = session.access_token(USER_ID)
    stored_after_first = _raw()["refresh_token"]
    hub.calls.clear()

    second = session.access_token(USER_ID)

    assert second != first
    assert hub.calls == [("POST", "/auth/refresh")]
    assert _raw()["refresh_token"] != stored_after_first


def test_a_refused_refresh_raises_and_keeps_the_stored_session(store, hub):
    _connect(hub)
    before = _raw()
    hub.live_refresh.clear()

    with pytest.raises(client.HubRefused):
        session.access_token(USER_ID)
    assert _raw() == before


def test_saving_a_new_session_drops_the_cached_access_token(store, hub):
    _connect(hub)
    session.access_token(USER_ID)
    _connect(hub)
    hub.calls.clear()

    session.access_token(USER_ID)

    assert hub.calls == [("POST", "/auth/refresh")]


def test_expiry_reads_the_unverified_exp_claim():
    token = jwt.encode({"exp": 2_000_000_000}, "a key long enough for HS256 signing", algorithm="HS256")
    assert session._expiry(token) == 2_000_000_000
    assert session._expiry("not-a-jwt") is None
    assert session._expiry(jwt.encode({"sub": "x"}, "a key long enough for HS256 signing", algorithm="HS256")) is None
