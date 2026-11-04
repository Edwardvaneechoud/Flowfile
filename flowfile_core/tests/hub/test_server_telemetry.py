"""On a server that joined its hub with keys of its own, no one is asked about telemetry: the hub's admin decides.

The server keeps telemetry off unless its environment turns it on (``FLOWFILE_TELEMETRY`` set to a true value),
whatever a visitor chose before it joined, and its consent route answers every person, an admin of the server
included, 403 with the sentence that the hub's admin decides. The server joins ``FakeJoinHub`` over real HTTP with
keys of its own (``tests/test_hub_sign_in``); the telemetry client sends to a captured collector
(``tests/test_telemetry_events.sent``). A desktop is unchanged (``tests/test_telemetry_routes``).
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
HUB_DECIDES = "This server is joined to a hub; the hub's admin decides whether it sends anonymous usage data."


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


def signed_in_admin(hub, name: str) -> str:
    """The access token of a person the hub lets administer this server, signed in through the hub."""
    token = hub.sign_in_token(NODE, uid=41, username=name, node_admin=True)
    signed_in = api.post("/auth/hub/signin", json={"token": token})
    assert signed_in.status_code == 200, signed_in.text
    return signed_in.json()["access_token"]


def test_nobody_on_a_joined_server_is_asked_and_telemetry_is_off(hub, username, undecided):
    access_token = signed_in_admin(hub, username)

    status = api.get("/telemetry/status", headers=bearer(access_token))

    assert status.status_code == 200, status.text
    body = status.json()
    assert (body["consent"], body["can_manage"], body["hub_decides"]) == (False, False, True)
    assert glue.hub_decision() is False


@pytest.mark.parametrize("enabled", [True, False])
def test_the_consent_route_answers_403_with_the_sentence_that_the_hubs_admin_decides(hub, username, undecided, enabled):
    access_token = signed_in_admin(hub, username)

    refused = api.post("/telemetry/consent", json={"enabled": enabled}, headers=bearer(access_token))

    assert refused.status_code == 403, refused.text
    assert refused.json()["detail"] == {"error_code": "TELEMETRY_HUB_DECIDES", "message": HUB_DECIDES}
    assert client.consent() is None


def test_an_earlier_visitors_yes_sends_nothing_once_the_server_joined(hub, sent):
    assert client.consent() is True

    glue.emit("app_started")
    glue.emit_once("flow_created")

    assert names(sent) == []


def test_the_servers_environment_turns_telemetry_on_without_asking_anyone(hub, username, undecided, monkeypatch):
    monkeypatch.setenv(client.ENV_KILL_SWITCH, "1")
    access_token = signed_in_admin(hub, username)

    glue.emit("app_started")

    assert names(undecided) == ["app_started"]
    assert client.consent() is True and client.install_id()
    body = api.get("/telemetry/status", headers=bearer(access_token)).json()
    assert (body["consent"], body["can_manage"], body["hub_decides"]) == (True, False, True)


def test_a_desktop_still_decides_for_itself(node_file, sent):
    assert glue.hub_decision() is None

    glue.emit("app_started")

    assert names(sent) == ["app_started"]
