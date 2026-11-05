"""Telemetry on a server that joined its hub with keys of its own follows ``FLOWFILE_TELEMETRY`` and the stored consent
alone, as on any other server: a joined server has no rule of its own.

The server joins ``FakeJoinHub`` over real HTTP with keys of its own (``tests/test_hub_sign_in``); the telemetry client
sends to a captured collector (``tests/test_telemetry_events.sent``). Its status answers as a docker server's does, an
admin manages consent there, a kill switch in the environment disables everything, and a true ``FLOWFILE_TELEMETRY``
grants nothing by itself.
"""

from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core import telemetry as glue
from shared import telemetry as client
from tests import test_hub_sign_in, test_telemetry_events
from tests.test_hub_sign_in import NODE, bearer

node_file = test_hub_sign_in.node_file
hub = test_hub_sign_in.hub
username = test_hub_sign_in.username
sent = test_telemetry_events.sent
names = test_telemetry_events.names

api = TestClient(main.app)
STATUS_KEYS = {"available", "consent", "env_kill_switch", "endpoint_configured", "can_manage"}


@pytest.fixture
def undecided(tmp_path, monkeypatch) -> Iterator[list[dict]]:
    """``sent``'s open gates and captured collector, with no consent stored: nobody was ever asked."""
    monkeypatch.setattr(client, "_settings_file", lambda: tmp_path / "telemetry.yaml")
    monkeypatch.setattr(client, "_spool_file", lambda: tmp_path / "telemetry_spool.jsonl")
    client._reset_for_tests()
    monkeypatch.delenv("TESTING", raising=False)
    monkeypatch.delenv(client.ENV_KILL_SWITCH, raising=False)
    monkeypatch.setenv(client.ENV_ENDPOINT, "https://example.invalid/events")
    captured: list[dict] = []
    monkeypatch.setattr(client, "_ensure_worker", lambda: None)
    monkeypatch.setattr(client, "_post", lambda url, json, timeout=None: captured.extend(json["events"]))
    yield captured
    client._reset_for_tests()


def signed_in(hub, name: str, *, admin: bool) -> str:
    """The access token of a person signed in through the hub, an administrator of this server when ``admin``."""
    token = hub.sign_in_token(NODE, uid=41 if admin else 42, username=name, node_admin=admin)
    response = api.post("/auth/hub/signin", json={"token": token})
    assert response.status_code == 200, response.text
    return response.json()["access_token"]


def test_a_joined_servers_status_is_a_docker_servers_with_nothing_of_the_hubs(hub, username, undecided):
    access_token = signed_in(hub, username, admin=True)

    status = api.get("/telemetry/status", headers=bearer(access_token))

    assert status.status_code == 200, status.text
    body = status.json()
    assert set(body) == STATUS_KEYS
    assert (body["consent"], body["can_manage"], body["env_kill_switch"]) == (None, True, False)


def test_an_admin_of_a_joined_server_manages_consent_and_a_member_does_not(hub, username, undecided):
    admin = signed_in(hub, username, admin=True)
    member = signed_in(hub, f"{username}-member", admin=False)

    refused = api.post("/telemetry/consent", json={"enabled": True}, headers=bearer(member))
    granted = api.post("/telemetry/consent", json={"enabled": True}, headers=bearer(admin))

    assert refused.status_code == 403, refused.text
    assert refused.json()["detail"]["error_code"] == "TELEMETRY_ADMIN_ONLY"
    assert granted.status_code == 200, granted.text
    assert (granted.json()["consent"], client.consent()) == (True, True)
    assert api.get("/telemetry/status", headers=bearer(member)).json()["can_manage"] is False


def test_a_stored_yes_still_sends_once_the_server_joined(hub, sent):
    assert client.consent() is True

    glue.emit("app_started")
    glue.emit_once("flow_created")

    assert names(sent) == ["app_started", "flow_created"]


def test_the_kill_switch_disables_telemetry_on_a_joined_server_as_anywhere(hub, username, sent, monkeypatch):
    monkeypatch.setenv(client.ENV_KILL_SWITCH, "0")
    access_token = signed_in(hub, username, admin=True)

    glue.emit("app_started")

    assert names(sent) == []
    body = api.get("/telemetry/status", headers=bearer(access_token)).json()
    assert (body["env_kill_switch"], body["available"]) == (True, False)


def test_a_true_flowfile_telemetry_grants_nothing_without_consent(hub, undecided, monkeypatch):
    monkeypatch.setenv(client.ENV_KILL_SWITCH, "1")

    glue.emit("app_started")

    assert names(undecided) == []
    assert client.consent() is None
