"""The ``/hub_client`` routes against a fake hub, with the local sign-in and a secure store of the test's own.

No hub state answers 401 here: the frontend reads a 401 as its own expired session.
"""

import json
import socket

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.auth import secrets as core_secrets
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import ConfiguredBackend
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake
from flowfile_core.hub_client import HUB_CONTRACT_VERSION, node_tokens, reachability, session
from flowfile_core.secret_manager.secret_manager import encrypt_secret
from shared._version import get_version
from tests.hub_client.conftest import HUB_URL, INFO, compute_node

CONNECT = {"hub_url": HUB_URL, "username": "ana", "password": "s3cret", "device_name": "ana-laptop"}
TEAM = compute_node(1, "team")
GPU = compute_node(2, "gpu")
OLD = compute_node(3, "old", enabled=False)


@pytest.fixture(autouse=True)
def no_configured_backends(monkeypatch):
    """Every backends row a test left is removed when it ends, the registry forgets it and no node token stays.

    The reachability check a sync queues is stood down here: the nodes' worker URLs are names nothing resolves, and
    ``test_reachability.py`` checks real workers.
    """
    monkeypatch.setattr(node_tokens, "_tokens", {})
    monkeypatch.setattr(reachability, "check_all", lambda backend_ids: None)
    yield
    with get_db_context() as db:
        for row in db.query(ConfiguredBackend).all():
            registry.forget(row.id)
            db.delete(row)
        db.commit()
    handshake.reset_handshake()
    reachability.clear()


def _rows() -> dict[str, ConfiguredBackend]:
    with get_db_context() as db:
        return {row.id: row for row in db.query(ConfiguredBackend).all()}


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
    assert body["node_name"] is None
    assert body["account"] is None
    assert hub.calls == []


def test_the_connection_names_the_node_this_core_is_connected_or_not(api, hub, monkeypatch):
    monkeypatch.setenv("FLOWFILE_NODE_NAME", "team-local")

    assert api.get("/hub_client/connection").json()["node_name"] == "team-local"
    assert api.post("/hub_client/connection", json=CONNECT).json()["node_name"] == "team-local"
    assert api.get("/hub_client/connection").json()["node_name"] == "team-local"


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
    assert body["account"] == "ana"
    assert "s3cret" not in response.text
    assert hub.calls == [
        ("GET", "/hub/info"),
        ("POST", "/auth/token"),
        ("POST", "/hub/devices"),
        ("GET", "/hub/compute-nodes"),
    ]

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
    assert body["account"] == "ana"


def test_a_hub_that_stopped_answering_still_reports_the_connection(api, hub):
    api.post("/hub_client/connection", json=CONNECT)
    hub.down = True

    body = api.get("/hub_client/connection").json()

    assert body["connected"] is True
    assert body["reachable"] is False
    assert body["error"]
    assert body["account"] == "ana"
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


def test_connecting_fills_the_backends_table_with_the_hubs_enabled_nodes(api, hub):
    hub.nodes = [TEAM, GPU, OLD]

    response = api.post("/hub_client/connection", json=CONNECT)

    assert response.status_code == 200, response.text
    assert response.json()["nodes_synced"] == 2
    assert response.json()["sync_error"] is None
    assert hub.calls[-1] == ("GET", "/hub/compute-nodes")
    rows = _rows()
    assert sorted(rows) == ["worker:gpu", "worker:team"]
    team = rows["worker:team"]
    assert team.url == TEAM["worker_url"]
    assert team.token is None and team.ca_cert is None
    assert team.hub_node_id == TEAM["id"]
    assert team.hub_user_id == _user_id(api)
    listed = api.get("/compute_backends").json()
    assert [(row["id"], row["source"], row["url"]) for row in listed] == [
        ("worker:gpu", "hub", GPU["worker_url"]),
        ("worker:team", "hub", TEAM["worker_url"]),
    ]
    assert hub.minted == []


def test_a_failed_node_sync_does_not_fail_the_connect(api, hub):
    hub.nodes = [TEAM]
    hub.nodes_failure = 500

    response = api.post("/hub_client/connection", json=CONNECT)

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["connected"] is True
    assert body["nodes_synced"] is None
    assert body["sync_error"] == "compute nodes unavailable"
    assert _rows() == {}
    assert api.get("/hub_client/connection").json()["connected"] is True


def test_sync_writes_updates_and_removes_rows_as_the_hub_changed(api, hub):
    hub.nodes = [TEAM, GPU]
    api.post("/hub_client/connection", json=CONNECT)
    team_backend = registry.backend("worker:team")
    moved = compute_node(TEAM["id"], "team", worker_url="https://team-moved.example.test:63579")
    etl = compute_node(4, "etl")
    hub.nodes = [moved, compute_node(GPU["id"], "gpu", enabled=False), etl]
    hub.calls.clear()

    response = api.post("/hub_client/compute_nodes/sync")

    assert response.status_code == 200, response.text
    body = response.json()
    assert hub.calls == [("GET", "/hub/compute-nodes")]
    assert body["skipped"] == []
    assert [(row["id"], row["source"], row["url"]) for row in body["backends"]] == [
        ("worker:etl", "hub", etl["worker_url"]),
        ("worker:team", "hub", moved["worker_url"]),
    ]
    rows = _rows()
    assert sorted(rows) == ["worker:etl", "worker:team"]
    assert rows["worker:team"].url == moved["worker_url"]
    remade = registry.backend("worker:team")
    assert remade is not team_backend
    assert isinstance(remade, WorkerBackend)
    assert remade.url == moved["worker_url"]
    with pytest.raises(LookupError):
        registry.backend("worker:gpu")


def test_sync_renames_a_node_by_removing_its_old_row(api, hub):
    hub.nodes = [TEAM]
    api.post("/hub_client/connection", json=CONNECT)
    hub.nodes = [compute_node(TEAM["id"], "team-two")]

    response = api.post("/hub_client/compute_nodes/sync")

    assert response.status_code == 200, response.text
    assert sorted(_rows()) == ["worker:team-two"]


def test_a_core_that_is_a_node_itself_never_lists_its_own_node_and_drops_a_row_written_before(api, hub, monkeypatch):
    hub.nodes = [compute_node(1, "team-local"), GPU]
    api.post("/hub_client/connection", json=CONNECT)
    assert sorted(_rows()) == ["worker:gpu", "worker:team-local"]
    monkeypatch.setenv("FLOWFILE_NODE_NAME", "team-local")

    response = api.post("/hub_client/compute_nodes/sync")

    assert response.status_code == 200, response.text
    assert response.json()["skipped"] == []
    assert [row["id"] for row in response.json()["backends"]] == ["worker:gpu"]
    assert sorted(_rows()) == ["worker:gpu"]
    reconnected = api.post("/hub_client/connection", json=CONNECT)
    assert reconnected.json()["nodes_synced"] == 1
    assert reconnected.json()["sync_error"] is None
    assert sorted(_rows()) == ["worker:gpu"]


def test_a_hand_added_row_with_a_hub_nodes_name_is_skipped_and_untouched(api, hub):
    stored = encrypt_secret("row-token", 1)
    with get_db_context() as db:
        db.add(ConfiguredBackend(id="worker:team", url="https://own-team.example.test:63579", token=stored))
        db.commit()
    hub.nodes = [TEAM, GPU]

    connected = api.post("/hub_client/connection", json=CONNECT)
    synced = api.post("/hub_client/compute_nodes/sync")

    assert connected.json()["nodes_synced"] == 1
    assert synced.status_code == 200, synced.text
    assert synced.json()["skipped"] == ["worker:team"]
    team = _rows()["worker:team"]
    assert team.url == "https://own-team.example.test:63579"
    assert team.token == stored
    assert team.hub_node_id is None and team.hub_user_id is None
    sources = {row["id"]: row["source"] for row in synced.json()["backends"]}
    assert sources == {"worker:team": "local", "worker:gpu": "hub"}


def test_sync_without_a_hub_connection_is_refused_with_400(api, hub):
    hub.nodes = [TEAM]

    response = api.post("/hub_client/compute_nodes/sync")

    assert response.status_code == 400
    assert "Connect to a hub first" in response.json()["detail"]
    assert hub.calls == []
    assert _rows() == {}


def test_sync_when_the_hub_is_down_is_502_and_keeps_the_rows(api, hub):
    hub.nodes = [TEAM]
    api.post("/hub_client/connection", json=CONNECT)
    hub.down = True

    response = api.post("/hub_client/compute_nodes/sync")

    assert response.status_code == 502
    assert sorted(_rows()) == ["worker:team"]


def test_sync_requires_the_local_sign_in(store):
    assert TestClient(main.app).post("/hub_client/compute_nodes/sync").status_code == 401


def test_a_hub_filled_backend_mints_a_run_token_on_first_use_and_reuses_it(api, hub):
    hub.nodes = [TEAM]
    api.post("/hub_client/connection", json=CONNECT)
    hub.calls.clear()

    backend = registry.backend("worker:team")

    assert isinstance(backend, WorkerBackend)
    assert backend.token
    assert hub.minted == [{"user": "ana", "node_id": TEAM["id"], "scope": "run"}]
    assert ("POST", "/hub/node-tokens") in hub.calls
    hub.calls.clear()
    assert registry.backend("worker:team") is backend
    assert hub.calls == []


def test_disconnecting_removes_the_callers_hub_filled_rows_and_the_token_cache_but_not_a_hand_added_row(api, hub):
    stored = encrypt_secret("row-token", 1)
    with get_db_context() as db:
        db.add(ConfiguredBackend(id="worker:own", url="https://own.example.test:63579", token=stored))
        db.commit()
    hub.nodes = [TEAM, GPU]
    api.post("/hub_client/connection", json=CONNECT)
    registry.backend("worker:team")
    assert node_tokens._tokens
    hub.calls.clear()

    response = api.delete("/hub_client/connection")

    assert response.status_code == 200
    assert hub.calls == []
    assert sorted(_rows()) == ["worker:own"]
    assert node_tokens._tokens == {}
    assert [row["id"] for row in api.get("/compute_backends").json()] == ["worker:own"]
    with pytest.raises(LookupError, match="no compute_backends row"):
        registry.backend("worker:team")


def test_a_hub_filled_row_is_not_removed_on_the_backends_page(api, hub):
    hub.nodes = [TEAM]
    api.post("/hub_client/connection", json=CONNECT)

    response = api.delete("/compute_backends/worker:team")

    assert response.status_code == 409, response.text
    assert "managed by the hub connection" in response.json()["detail"]
    assert sorted(_rows()) == ["worker:team"]
