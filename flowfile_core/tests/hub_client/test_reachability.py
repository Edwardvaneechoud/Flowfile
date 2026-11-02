"""The reachability check of the hub's compute nodes: each node's capabilities asked once after a sync, kept in memory.

A fake worker on a loopback port answers its capabilities to any token and counts its requests; a second URL is a
loopback port nothing listens on. The fake hub (``conftest``) lists both as nodes and mints the tokens the check
signs with. The check a sync queues runs in the request's background tasks, which the test client runs before it
answers, so the listing right after a connect or a sync carries the outcome.
"""

import threading
import time
from collections.abc import Iterator

import pytest
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.auth.models import User
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import ConfiguredBackend
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake
from flowfile_core.hub_client import node_tokens, reachability, session
from flowfile_core.routes.compute_backends import listed_backends
from flowfile_core.secret_manager.secret_manager import encrypt_secret
from shared.compute_protocol import local_capabilities
from tests.conftest import _free_port, closed_url
from tests.hub_client.conftest import HUB_URL, compute_node

CONNECT = {"hub_url": HUB_URL, "username": "ana", "password": "s3cret", "device_name": "ana-laptop"}
LISTING = "/compute_backends"
CHECK = "/hub_client/compute_nodes/check"


class FakeWorker:
    """A worker on a loopback port that answers its capabilities to any token, or refuses every token when told to."""

    def __init__(self) -> None:
        self.hits = 0
        self.refuse = False
        self.url = ""


@pytest.fixture(autouse=True)
def no_configured_backends(monkeypatch):
    """Every backends row a test left is removed when it ends; the registry, the verdicts, the node tokens and the
    reachability outcomes are emptied."""
    monkeypatch.setattr(node_tokens, "_tokens", {})
    reachability.clear()
    yield
    with get_db_context() as db:
        for row in db.query(ConfiguredBackend).all():
            registry.forget(row.id)
            db.delete(row)
        db.commit()
    handshake.reset_handshake()
    reachability.clear()


@pytest.fixture
def worker() -> Iterator[FakeWorker]:
    fake = FakeWorker()
    app = FastAPI()
    answer = local_capabilities().model_dump(mode="json")

    @app.get("/capabilities")
    @app.get("/v1/capabilities")
    def capabilities() -> dict:
        fake.hits += 1
        if fake.refuse:
            raise HTTPException(status_code=401, detail="Invalid or missing internal token")
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


def test_a_connect_checks_each_node_once_and_the_listing_says_which_this_machine_reaches(api, hub, worker):
    closed = closed_url()
    hub.nodes = [compute_node(1, "team", worker_url=worker.url), compute_node(2, "gpu", worker_url=closed)]

    response = api.post("/hub_client/connection", json=CONNECT)

    assert response.status_code == 200, response.text
    assert response.json()["nodes_synced"] == 2
    assert worker.hits == 1
    assert [m["node_id"] for m in hub.minted] == [1, 2]
    listed = _listed(api)
    assert listed["worker:team"]["reachable"] is True
    assert listed["worker:team"]["reason"] is None
    assert listed["worker:gpu"]["reachable"] is False
    assert listed["worker:gpu"]["reason"].startswith(f"can't reach {closed} from this computer: ")
    assert "refused" in listed["worker:gpu"]["reason"].lower()
    assert handshake.last_verdict("worker:team") is None
    assert worker.hits == 1


def test_a_sync_checks_the_nodes_again_and_the_earlier_outcome_stands_until_it_answers(api, hub, worker):
    hub.nodes = [compute_node(1, "team", worker_url=worker.url)]
    api.post("/hub_client/connection", json=CONNECT)
    assert _listed(api)["worker:team"]["reachable"] is True
    closed = closed_url()
    hub.nodes = [compute_node(1, "team", worker_url=closed)]

    response = api.post("/hub_client/compute_nodes/sync")

    assert response.status_code == 200, response.text
    team = _listed(api)["worker:team"]
    assert team["reachable"] is False
    assert team["reason"].startswith(f"can't reach {closed} from this computer: ")
    assert worker.hits == 1


def test_check_runs_the_check_now_and_answers_the_hub_rows(api, hub, worker):
    closed = closed_url()
    hub.nodes = [compute_node(1, "team", worker_url=worker.url), compute_node(2, "gpu", worker_url=closed)]
    api.post("/hub_client/connection", json=CONNECT)
    with get_db_context() as db:
        db.add(ConfiguredBackend(id="worker:own", url=worker.url, token=encrypt_secret("row-token", 1)))
        db.commit()
    worker.refuse = True

    response = api.post(CHECK)

    assert response.status_code == 200, response.text
    rows = {row["id"]: row for row in response.json()}
    assert sorted(rows) == ["worker:gpu", "worker:team"]
    assert rows["worker:team"]["source"] == "hub"
    assert rows["worker:team"]["reachable"] is True
    assert "401" in rows["worker:team"]["reason"]
    assert rows["worker:gpu"]["reachable"] is False
    assert rows["worker:gpu"]["reason"].startswith(f"can't reach {closed} from this computer: ")
    assert worker.hits == 2
    assert rows == {row["id"]: row for row in api.get(LISTING).json() if row["source"] == "hub"}


def test_check_without_a_hub_connection_is_400(api, hub, worker):
    response = api.post(CHECK)

    assert response.status_code == 400
    assert "Connect to a hub first" in response.json()["detail"]
    assert hub.calls == []
    assert worker.hits == 0


def test_a_row_added_here_is_never_checked_and_answers_neither_field(api, hub, worker):
    with get_db_context() as db:
        db.add(ConfiguredBackend(id="worker:own", url=worker.url, token=encrypt_secret("row-token", 1)))
        db.commit()
    own = _listed(api)["worker:own"]
    assert (own["reachable"], own["reason"]) == (None, None)

    api.post("/hub_client/connection", json=CONNECT)
    api.post("/hub_client/compute_nodes/sync")
    checked = api.post(CHECK)

    assert checked.status_code == 200, checked.text
    assert checked.json() == []
    own = _listed(api)["worker:own"]
    assert (own["reachable"], own["reason"]) == (None, None)
    assert worker.hits == 0
    assert hub.minted == []


def test_a_node_not_checked_yet_answers_neither_field(api, hub, worker, monkeypatch):
    monkeypatch.setattr(reachability, "check_all", lambda backend_ids: None)
    hub.nodes = [compute_node(1, "team", worker_url=worker.url)]
    api.post("/hub_client/connection", json=CONNECT)

    team = _listed(api)["worker:team"]

    assert team["source"] == "hub"
    assert (team["reachable"], team["reason"]) == (None, None)
    assert reachability.of("worker:team") == (None, None)


def test_dropping_a_row_and_disconnecting_forget_the_outcome(api, hub, worker):
    closed = closed_url()
    hub.nodes = [compute_node(1, "team", worker_url=worker.url), compute_node(2, "gpu", worker_url=closed)]
    api.post("/hub_client/connection", json=CONNECT)
    assert reachability.of("worker:team") == (True, None)
    assert reachability.of("worker:gpu")[0] is False
    hub.nodes = [
        compute_node(1, "team", worker_url=worker.url),
        compute_node(2, "gpu", worker_url=closed, enabled=False),
    ]

    api.post("/hub_client/compute_nodes/sync")

    assert reachability.of("worker:gpu") == (None, None)
    assert reachability.of("worker:team") == (True, None)

    api.delete("/hub_client/connection")

    assert reachability.of("worker:team") == (None, None)
    assert _rows() == []


def test_a_row_whose_hub_session_is_gone_answers_none_with_the_reconnect_text_and_calls_no_worker(api, hub, worker):
    hub.nodes = [compute_node(1, "team", worker_url=worker.url)]
    api.post("/hub_client/connection", json=CONNECT)
    assert worker.hits == 1
    session.clear(_user_id(api))

    outcome = reachability.check("worker:team")

    assert outcome[0] is None
    assert "connect to the hub again" in outcome[1]
    assert worker.hits == 1
    team = _listed(api)["worker:team"]
    assert team["reachable"] is None
    assert "connect to the hub again" in team["reason"]


def test_a_non_administrator_reads_an_unreachable_nodes_reason_without_the_address(api, hub, worker):
    closed = closed_url()
    hub.nodes = [compute_node(1, "team", worker_url=worker.url), compute_node(2, "gpu", worker_url=closed)]
    api.post("/hub_client/connection", json=CONNECT)
    worker.refuse = True
    reachability.check("worker:team")
    member = User(username="not_admin", id=2, disabled=False, is_admin=False, must_change_password=False)

    listed = {row.id: row for row in listed_backends(_rows(), member)}

    assert listed["worker:gpu"].url is None
    assert listed["worker:gpu"].reachable is False
    assert listed["worker:gpu"].reason == "can't reach this node from this computer"
    assert closed not in listed["worker:gpu"].reason
    assert listed["worker:team"].reachable is True
    assert "401" in listed["worker:team"].reason
