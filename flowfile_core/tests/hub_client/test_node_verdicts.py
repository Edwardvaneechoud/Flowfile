"""The handshake verdict of the hub's compute nodes: each row a sync writes is checked once.

A fake worker on a loopback port answers its capabilities to any token and counts its requests; a second URL is a
loopback port nothing listens on. The fake hub (``conftest``) lists both as nodes and mints the tokens the check
signs with. A sync checks before it answers; the check a connect queues runs in the request's background tasks,
which the test client runs before it answers, so the listing right after either carries the verdict.
"""

import threading
import time
from collections.abc import Iterator

import pytest
import uvicorn
from fastapi import FastAPI
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.auth.models import User
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import ConfiguredBackend
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake
from flowfile_core.hub_client import node_tokens, session
from flowfile_core.routes import hub_client as hub_routes
from flowfile_core.routes.compute_backends import listed_backends
from flowfile_core.secret_manager.secret_manager import encrypt_secret
from shared.compute_protocol import local_capabilities
from tests.conftest import _free_port, closed_url
from tests.hub_client.conftest import HUB_URL, compute_node

CONNECT = {"hub_url": HUB_URL, "username": "ana", "password": "s3cret", "device_name": "ana-laptop"}
LISTING = "/compute_backends"


class FakeWorker:
    """A worker on a loopback port that answers its capabilities to any token and nothing else, so every probe plan
    lands in the verdict's ``absent`` and the verdict passes."""

    def __init__(self) -> None:
        self.hits = 0
        self.url = ""


@pytest.fixture(autouse=True)
def no_configured_backends(monkeypatch):
    """Every backends row a test left is removed when it ends; the registry, the verdicts and the node tokens are
    emptied."""
    monkeypatch.setattr(node_tokens, "_tokens", {})
    yield
    with get_db_context() as db:
        for row in db.query(ConfiguredBackend).all():
            registry.forget(row.id)
            db.delete(row)
        db.commit()
    handshake.reset_handshake()


@pytest.fixture
def worker() -> Iterator[FakeWorker]:
    fake = FakeWorker()
    app = FastAPI()
    answer = local_capabilities().model_dump(mode="json")

    @app.get("/capabilities")
    @app.get("/v1/capabilities")
    def capabilities() -> dict:
        fake.hits += 1
        return answer

    port = _free_port()
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 30
    while not server.started:
        if time.monotonic() > deadline:
            pytest.fail("the fake worker never became ready")
        time.sleep(0.05)
    fake.url = f"http://127.0.0.1:{port}"
    yield fake
    server.should_exit = True
    thread.join(timeout=15)


@pytest.fixture
def api(store) -> TestClient:
    c = TestClient(main.app)
    token = c.post("/auth/token").json()["access_token"]
    c.headers["Authorization"] = f"Bearer {token}"
    return c


def _user_id(api: TestClient) -> int:
    return api.get("/auth/users/me").json()["id"]


def _listed(api: TestClient) -> dict[str, dict]:
    response = api.get(LISTING)
    assert response.status_code == 200, response.text
    return {row["id"]: row for row in response.json()}


def _rows() -> list[ConfiguredBackend]:
    with get_db_context() as db:
        return db.query(ConfiguredBackend).order_by(ConfiguredBackend.id).all()


def test_a_connect_checks_each_node_once_and_the_listing_carries_the_verdict(api, hub, worker):
    closed = closed_url()
    hub.nodes = [compute_node(1, "team", worker_url=worker.url), compute_node(2, "gpu", worker_url=closed)]

    response = api.post("/hub_client/connection", json=CONNECT)

    assert response.status_code == 200, response.text
    assert response.json()["nodes_synced"] == 2
    assert worker.hits == 1
    assert [m["node_id"] for m in hub.minted] == [1, 2]
    listed = _listed(api)
    assert listed["worker:team"]["source"] == "hub"
    assert listed["worker:team"]["verdict"]["ok"] is True
    assert listed["worker:team"]["verdict"]["error"] is None
    gpu = listed["worker:gpu"]["verdict"]
    assert gpu["ok"] is False
    assert gpu["error_kind"] == "ComputeUnavailable"
    assert closed in gpu["error"]


def _synced(response) -> dict[str, dict]:
    assert response.status_code == 200, response.text
    return {row["id"]: row for row in response.json()["backends"]}


def test_a_syncs_answer_carries_the_verdict_of_each_node_it_wrote(api, hub, worker):
    closed = closed_url()
    hub.nodes = [compute_node(1, "team", worker_url=worker.url), compute_node(2, "gpu", worker_url=closed)]
    api.post("/hub_client/connection", json=CONNECT)
    handshake.reset_handshake()
    registry.forget("worker:team")
    registry.forget("worker:gpu")

    synced = _synced(api.post("/hub_client/compute_nodes/sync"))

    assert synced["worker:team"]["verdict"]["ok"] is True
    assert synced["worker:gpu"]["verdict"]["error_kind"] == "ComputeUnavailable"
    assert closed in synced["worker:gpu"]["verdict"]["error"]
    assert worker.hits == 2
    assert {k: row["verdict"] for k, row in _listed(api).items()} == {k: row["verdict"] for k, row in synced.items()}


def test_a_sync_keeps_a_passing_verdict_and_checks_a_node_the_hub_moved(api, hub, worker):
    hub.nodes = [compute_node(1, "team", worker_url=worker.url)]
    api.post("/hub_client/connection", json=CONNECT)
    assert _listed(api)["worker:team"]["verdict"]["ok"] is True

    synced = _synced(api.post("/hub_client/compute_nodes/sync"))

    assert worker.hits == 1
    assert synced["worker:team"]["verdict"]["ok"] is True

    closed = closed_url()
    hub.nodes = [compute_node(1, "team", worker_url=closed)]
    team = _synced(api.post("/hub_client/compute_nodes/sync"))["worker:team"]["verdict"]

    assert team["ok"] is False
    assert team["error_kind"] == "ComputeUnavailable"
    assert closed in team["error"]
    assert _listed(api)["worker:team"]["verdict"] == team
    assert worker.hits == 1


def test_a_row_added_here_is_never_checked_by_a_sync(api, hub, worker):
    with get_db_context() as db:
        db.add(ConfiguredBackend(id="worker:own", url=worker.url, token=encrypt_secret("row-token", 1)))
        db.commit()

    api.post("/hub_client/connection", json=CONNECT)
    api.post("/hub_client/compute_nodes/sync")

    assert _listed(api)["worker:own"]["verdict"] is None
    assert worker.hits == 0
    assert hub.minted == []


def test_a_row_whose_hub_session_is_gone_is_left_unchecked_and_calls_no_worker(api, hub, worker, monkeypatch):
    check_nodes = hub_routes._check_nodes
    monkeypatch.setattr(hub_routes, "_check_nodes", lambda backend_ids: None)
    hub.nodes = [compute_node(1, "team", worker_url=worker.url)]
    api.post("/hub_client/connection", json=CONNECT)
    session.clear(_user_id(api))

    check_nodes(["worker:team"])

    assert handshake.last_verdict("worker:team") is None
    assert _listed(api)["worker:team"]["verdict"] is None
    assert worker.hits == 0


def test_a_non_administrator_reads_an_unreachable_nodes_verdict_without_the_address(api, hub, worker):
    closed = closed_url()
    hub.nodes = [compute_node(1, "team", worker_url=worker.url), compute_node(2, "gpu", worker_url=closed)]
    api.post("/hub_client/connection", json=CONNECT)
    member = User(username="not_admin", id=2, disabled=False, is_admin=False, must_change_password=False)

    listed = {row.id: row for row in listed_backends(_rows(), member)}

    assert listed["worker:gpu"].url is None
    assert listed["worker:gpu"].verdict.ok is False
    assert listed["worker:gpu"].verdict.error_kind == "ComputeUnavailable"
    assert listed["worker:gpu"].verdict.error == "unreachable"
    assert closed not in listed["worker:gpu"].verdict.model_dump_json()
    assert listed["worker:team"].verdict.ok is True
    assert closed in _listed(api)["worker:gpu"]["verdict"]["error"]
