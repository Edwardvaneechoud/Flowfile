"""The ``/notebook/sessions`` routes with the TestClient: mode and loopback gates, ownership, execute, events
replay and the live stream, interrupt, reset and schemas, against a real session process."""

from __future__ import annotations

import json
import threading
import time

import pytest
from fastapi.testclient import TestClient

from flowfile_core import flow_file_handler, main
from flowfile_core.auth.jwt import get_current_active_user, get_current_user
from flowfile_core.auth.models import User as PydanticUser
from flowfile_core.configs import settings
from flowfile_core.notebook import registry as session_registry
from shared.notebook_display import TABLE_MIME
from tests.notebook.session_helpers import small_flow

OWNER_ID = 1
OTHER_ID = 2
LOOPBACK = ("127.0.0.1", 50123)


@pytest.fixture
def flag():
    before = bool(settings.FEATURE_FLAG_CANVAS_NOTEBOOK)
    settings.FEATURE_FLAG_CANVAS_NOTEBOOK.set(True)
    yield
    settings.FEATURE_FLAG_CANVAS_NOTEBOOK.set(before)


@pytest.fixture
def electron(monkeypatch):
    monkeypatch.setenv("FLOWFILE_MODE", "electron")


@pytest.fixture
def open_flow(flag, electron):
    graph = small_flow()
    flow_file_handler._flows[graph.flow_id] = graph
    flow_file_handler._register_user_session(OWNER_ID, graph.flow_id)
    yield graph
    session_registry.get_registry().close(graph.flow_id)
    flow_file_handler.delete_flow(graph.flow_id)
    flow_file_handler._unregister_user_session(OWNER_ID, graph.flow_id)


@pytest.fixture
def client_as():
    def _as(user_id: int, client: tuple[str, int] = LOOPBACK, is_admin: bool = True) -> TestClient:
        user = PydanticUser(username=f"nb_{user_id}", id=user_id, disabled=False, is_admin=is_admin)
        main.app.dependency_overrides[get_current_active_user] = lambda: user
        main.app.dependency_overrides[get_current_user] = lambda: user
        return TestClient(main.app, client=client)

    yield _as
    main.app.dependency_overrides.pop(get_current_active_user, None)
    main.app.dependency_overrides.pop(get_current_user, None)


def _events(client: TestClient, session_id: str, after: int = 0) -> list[dict]:
    response = client.get(f"/notebook/sessions/{session_id}/events", params={"after": after, "follow": False})
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/x-ndjson")
    return [json.loads(line) for line in response.text.splitlines() if line]


def _until(client: TestClient, session_id: str, predicate, timeout: float = 60.0) -> list[dict]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        events = _events(client, session_id)
        if any(predicate(e) for e in events):
            return events
        time.sleep(0.1)
    raise AssertionError(f"no matching event; last: {events[-5:]}")


def _open(client: TestClient, flow_id: int) -> str:
    response = client.post("/notebook/sessions", json={"flow_id": flow_id})
    assert response.status_code == 200, response.text
    return response.json()["session_id"]


def test_electron_mode_refuses_a_non_loopback_client(open_flow, client_as):
    response = client_as(OWNER_ID, client=("192.168.1.20", 50000)).post(
        "/notebook/sessions", json={"flow_id": open_flow.flow_id}
    )
    assert response.status_code == 403


def test_multi_user_mode_refuses_without_the_opt_in(open_flow, client_as, monkeypatch):
    monkeypatch.setenv("FLOWFILE_MODE", "docker")
    monkeypatch.delenv("FLOWFILE_NOTEBOOK_SESSIONS_MULTIUSER", raising=False)
    response = client_as(OWNER_ID).post("/notebook/sessions", json={"flow_id": open_flow.flow_id})
    assert response.status_code == 403


def test_a_flow_the_caller_has_not_open_is_404(open_flow, client_as):
    response = client_as(OTHER_ID).post("/notebook/sessions", json={"flow_id": open_flow.flow_id})
    assert response.status_code == 404


def test_session_lifecycle_over_the_routes(open_flow, client_as):
    owner = client_as(OWNER_ID)
    session_id = _open(owner, open_flow.flow_id)
    assert _open(owner, open_flow.flow_id) == session_id

    ticket = owner.post(
        f"/notebook/sessions/{session_id}/execute",
        json={"cell_id": "c1", "code": "print('hi')\nframe = fl.from_dict({'v': [1, 2]})\nframe"},
    ).json()["ticket"]
    events = _until(owner, session_id, lambda e: e["type"] == "done" and e.get("ticket") == ticket)
    assert [e["seq"] for e in events] == sorted(e["seq"] for e in events)
    mine = [e for e in events if e.get("ticket") == ticket]
    assert [e["type"] for e in mine] == ["stream", "display", "done"]
    assert TABLE_MIME in mine[1]["payload"] and mine[-1]["ok"] is True

    replay = _events(owner, session_id, after=mine[0]["seq"])
    assert [e["seq"] for e in replay] == [e["seq"] for e in events if e["seq"] > mine[0]["seq"]]

    frames = owner.get(f"/notebook/sessions/{session_id}/schemas").json()["frames"]
    assert frames["frame"] == [{"name": "v", "data_type": "Int64"}]
    seeded = {name for name in frames if name != "frame"}
    assert len(seeded) == 2 and all(frames[name][0]["name"] == "id" for name in seeded)
    assert owner.post(f"/notebook/sessions/{session_id}/interrupt").status_code == 200

    ticket = owner.post(f"/notebook/sessions/{session_id}/reset").json()["ticket"]
    _until(owner, session_id, lambda e: e.get("ticket") == ticket and e["type"] == "done")
    assert set(owner.get(f"/notebook/sessions/{session_id}/schemas").json()["frames"]) == seeded


def test_another_user_gets_404_on_every_session_route(open_flow, client_as):
    session_id = _open(client_as(OWNER_ID), open_flow.flow_id)
    other = client_as(OTHER_ID)
    base = f"/notebook/sessions/{session_id}"
    assert other.post(f"{base}/execute", json={"cell_id": "c", "code": "1"}).status_code == 404
    assert other.post(f"{base}/interrupt").status_code == 404
    assert other.post(f"{base}/reset").status_code == 404
    assert other.get(f"{base}/schemas").status_code == 404
    assert other.get(f"{base}/events").status_code == 404


def test_the_live_stream_follows_until_the_session_closes(open_flow, client_as):
    owner = client_as(OWNER_ID)
    session_id = _open(owner, open_flow.flow_id)
    session = session_registry.get_registry().get(session_id, OWNER_ID)
    _until(owner, session_id, lambda e: e["type"] == "ready")
    after = _events(owner, session_id)[-1]["seq"]

    def drive():
        ticket = session.execute("live", "print('streamed')")
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if any(e["type"] == "done" and e.get("ticket") == ticket for e in session.events_after(after)):
                break
            time.sleep(0.05)
        session_registry.get_registry().close_session(session_id)

    threading.Timer(0.3, drive).start()
    response = owner.get(f"/notebook/sessions/{session_id}/events", params={"after": after})
    assert response.status_code == 200
    events = [json.loads(line) for line in response.text.splitlines() if line]
    assert all(e["seq"] > after for e in events)
    assert any(e["type"] == "stream" and e["text"] == "streamed\n" for e in events)


def test_closing_the_flow_closes_its_sessions(open_flow, client_as):
    owner = client_as(OWNER_ID)
    session_id = _open(owner, open_flow.flow_id)
    session = session_registry.get_registry().get(session_id, OWNER_ID)
    assert owner.post("/editor/close_flow/", params={"flow_id": open_flow.flow_id}).status_code == 200
    assert session.closed
    assert session_registry.get_registry().get(session_id, OWNER_ID) is None
