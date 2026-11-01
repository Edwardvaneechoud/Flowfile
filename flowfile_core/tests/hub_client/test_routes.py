"""The ``/hub_client`` routes against a fake hub, with the local sign-in and a secure store of the test's own.

No hub state answers 401 here: the frontend reads a 401 as its own expired session.
"""

import json
import socket

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.auth import secrets as core_secrets
from flowfile_core.hub_client import HUB_CONTRACT_VERSION, session
from shared._version import get_version
from tests.hub_client.conftest import HUB_URL, INFO

CONNECT = {"hub_url": HUB_URL, "username": "ana", "password": "s3cret", "device_name": "ana-laptop"}


@pytest.fixture
def api(store) -> TestClient:
    """A client signed in to core; the token is minted after the store is rebuilt, so it verifies against it."""
    c = TestClient(main.app)
    token = c.post("/auth/token").json()["access_token"]
    c.headers["Authorization"] = f"Bearer {token}"
    return c


def _user_id(api: TestClient) -> int:
    return api.get("/auth/users/me").json()["id"]


def _stored_raw(api: TestClient) -> str | None:
    return core_secrets.get_password(session.SERVICE, f"session:{_user_id(api)}")


@pytest.mark.parametrize(
    "method,kwargs", [("GET", {}), ("POST", {"json": CONNECT}), ("DELETE", {})], ids=["GET", "POST", "DELETE"]
)
def test_every_route_requires_the_local_sign_in(store, method, kwargs):
    response = TestClient(main.app).request(method, "/hub_client/connection", **kwargs)
    assert response.status_code == 401


def test_not_connected_reports_the_hostname_as_the_default_device_name(api, hub):
    body = api.get("/hub_client/connection").json()
    assert body["connected"] is False
    assert body["hub_url"] is None
    assert body["device_name"] == socket.gethostname()
    assert body["client_version"] == get_version()
    assert body["contract_version"] == HUB_CONTRACT_VERSION
    assert body["reachable"] is False
    assert hub.calls == []


def test_connecting_stores_the_refresh_token_and_device_and_never_the_password(api, hub):
    response = api.post("/hub_client/connection", json=CONNECT)

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["connected"] is True
    assert body["hub_url"] == HUB_URL
    assert body["device_name"] == "ana-laptop"
    assert body["org_name"] == INFO["org_name"]
    assert body["vend_mode"] == "static"
    assert body["expires_at"] is not None
    assert "s3cret" not in response.text
    assert hub.calls == [("GET", "/hub/info"), ("POST", "/auth/token"), ("POST", "/hub/devices")]

    raw = _stored_raw(api)
    assert "s3cret" not in raw
    stored = json.loads(raw)
    assert stored["refresh_token"] in hub.live_refresh
    assert stored["device_id"] == hub.devices[("ana", "ana-laptop")]


def test_status_after_connecting_reports_the_hub_live(api, hub):
    api.post("/hub_client/connection", json=CONNECT)
    hub.info["org_name"] = "Acme Renamed"

    body = api.get("/hub_client/connection").json()

    assert body["connected"] is True
    assert body["reachable"] is True
    assert body["org_name"] == "Acme Renamed"
    assert body["hub_version"] == INFO["hub_version"]
    assert body["core_version"] == INFO["core_version"]
    assert body["vend_mode"] == "static"
    assert body["device_id"] == 1
    assert body["error"] is None


def test_a_hub_that_stopped_answering_still_reports_the_connection(api, hub):
    api.post("/hub_client/connection", json=CONNECT)
    hub.down = True

    body = api.get("/hub_client/connection").json()

    assert body["connected"] is True
    assert body["reachable"] is False
    assert body["error"]
    assert body["org_name"] is None and body["hub_version"] is None and body["vend_mode"] is None


def test_without_a_device_name_the_hostname_is_registered(api, hub):
    response = api.post("/hub_client/connection", json={**CONNECT, "device_name": None})
    assert response.status_code == 200, response.text
    assert response.json()["device_name"] == socket.gethostname()
    assert ("ana", socket.gethostname()) in hub.devices


def test_two_device_names_under_one_user_are_two_registrations(api, hub):
    first = api.post("/hub_client/connection", json=CONNECT).json()
    second = api.post("/hub_client/connection", json={**CONNECT, "device_name": "ana-desktop"}).json()
    again = api.post("/hub_client/connection", json=CONNECT).json()

    assert len(hub.devices) == 2
    assert first["device_id"] != second["device_id"]
    assert again["device_id"] == first["device_id"]


def test_a_version_refusal_answers_426_with_the_hubs_text_and_stores_nothing(api, hub):
    hub.version_refusal = f"client {get_version()} is not supported by this hub (core 0.30.0)"

    response = api.post("/hub_client/connection", json=CONNECT)

    assert response.status_code == 426
    assert response.json()["detail"] == hub.version_refusal
    assert _stored_raw(api) is None
    assert api.get("/hub_client/connection").json()["connected"] is False


def test_a_hub_that_is_down_answers_502_and_stores_nothing(api, hub):
    hub.down = True

    response = api.post("/hub_client/connection", json=CONNECT)

    assert response.status_code == 502
    assert isinstance(response.json()["detail"], str)
    assert _stored_raw(api) is None


def test_wrong_credentials_answer_400_not_401(api, hub):
    response = api.post("/hub_client/connection", json={**CONNECT, "password": "wrong"})

    assert response.status_code == 400
    assert "Incorrect username or password" in response.json()["detail"]
    assert "wrong" not in response.text
    assert _stored_raw(api) is None


def test_a_url_that_is_not_http_answers_422(api, hub):
    for url in ("ftp://hub.example.test", "hub.example.test", "https://"):
        response = api.post("/hub_client/connection", json={**CONNECT, "hub_url": url})
        assert response.status_code == 422, url
    assert "s3cret" not in response.text
    assert hub.calls == []


def test_a_url_that_answers_but_is_no_hub_answers_400(api, hub):
    response = api.post("/hub_client/connection", json={**CONNECT, "hub_url": HUB_URL + "/not-a-hub"})
    assert response.status_code == 400
    assert _stored_raw(api) is None


def test_disconnecting_clears_the_session_without_calling_the_hub(api, hub):
    api.post("/hub_client/connection", json=CONNECT)
    hub.calls.clear()

    response = api.delete("/hub_client/connection")

    assert response.status_code == 200
    assert response.json() == {"connected": False}
    assert hub.calls == []
    assert _stored_raw(api) is None
    assert api.get("/hub_client/connection").json()["connected"] is False
    assert api.get("/auth/users/me").status_code == 200


def test_a_hub_that_refuses_the_device_with_401_answers_400(api, hub, monkeypatch):
    monkeypatch.setattr(hub, "_bearer_user", lambda request: None)

    response = api.post("/hub_client/connection", json=CONNECT)

    assert response.status_code == 400
    assert _stored_raw(api) is None
