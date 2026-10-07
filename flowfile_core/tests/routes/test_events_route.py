"""``GET /editor/events``: a live stream of one flow's changes, for flows in the caller's session."""

import json
import socket
import threading

import httpx
import pytest
import uvicorn
from fastapi.testclient import TestClient

from flowfile_core import change_feed, main
from flowfile_core.routes.routes import flow_file_handler
from flowfile_core.schemas import input_schema


def _auth_headers() -> dict[str, str]:
    with TestClient(main.app) as client:
        token = client.post("/auth/token").json()["access_token"]
    return {"Authorization": f"Bearer {token}"}


client = TestClient(main.app)
headers = _auth_headers()


def _me() -> int:
    return client.get("/auth/users/me", headers=headers).json()["id"]


@pytest.fixture()
def live_core():
    """Core's app on a real loopback port: the TestClient buffers whole bodies, so it cannot read an open stream."""
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen()
    server = uvicorn.Server(uvicorn.Config(main.app, lifespan="off", log_level="warning"))
    thread = threading.Thread(target=server.run, kwargs={"sockets": [sock]}, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{sock.getsockname()[1]}"
    change_feed.close_all()
    server.should_exit = True
    thread.join(timeout=10)


@pytest.fixture()
def own_flow(tmp_path):
    flow_id = flow_file_handler.add_flow(name="events", flow_path=str(tmp_path / "events.yaml"), user_id=_me())
    flow = flow_file_handler.get_flow(flow_id)
    flow.flow_settings.execution_location = "local"
    flow.flow_settings.execution_mode = "Development"
    yield flow_id
    if flow_file_handler.get_flow(flow_id) is not None:
        flow_file_handler.delete_flow(flow_id)


class EventStream:
    """One open ``/editor/events`` connection; ``next()`` is the next event's data, ``None`` once the stream ended."""

    def __init__(self, base: str, flow_id: int):
        self.client = httpx.Client(timeout=httpx.Timeout(10.0))
        request = self.client.build_request("GET", f"{base}/editor/events", params={"flow_id": flow_id}, headers=headers)
        self.response = self.client.send(request, stream=True)
        assert self.response.status_code == 200, self.response.status_code
        assert self.response.headers["content-type"].startswith("text/event-stream")
        self.lines = self.response.iter_lines()

    def next(self) -> dict | None:
        data: list[str] = []
        for line in self.lines:
            if line == "":
                if data:
                    return json.loads("\n".join(data))
                continue  # a comment-only block (keepalive)
            if line.startswith("data: "):
                data.append(line[len("data: ") :])
        return None

    def close(self) -> None:
        self.response.close()
        self.client.close()


def _add_node(base: str, flow_id: int, node_id: int, extra_headers: dict | None = None) -> httpx.Response:
    return httpx.post(
        f"{base}/editor/add_node/",
        params={"flow_id": flow_id, "node_id": node_id, "node_type": "manual_input", "pos_x": 0, "pos_y": 0},
        headers={**headers, **(extra_headers or {})},
    )


def test_hello_then_every_foreign_change_with_its_origin(live_core, own_flow):
    stream = EventStream(live_core, own_flow)
    try:
        hello = stream.next()
        assert hello["kind"] == "hello" and hello["flow_id"] == own_flow and hello["is_running"] is False
        response = _add_node(live_core, own_flow, 1, {"X-Flowfile-Client": "tab-a"})
        assert response.status_code == 200, response.text
        event = stream.next()
        assert event == {"kind": "graph", "flow_id": own_flow, "revision": hello["revision"] + 1, "origin": "tab-a"}
        assert response.json()["history"]["revision"] == event["revision"]
        assert _add_node(live_core, own_flow, 2).status_code == 200
        assert stream.next() == {"kind": "graph", "flow_id": own_flow, "revision": event["revision"] + 1, "origin": None}
    finally:
        stream.close()


def test_a_run_is_bracketed_by_run_started_and_run_ended(live_core, own_flow):
    flow = flow_file_handler.get_flow(own_flow)
    flow.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=own_flow, node_id=1, raw_data_format=input_schema.RawData.from_pylist([{"a": 1}])
        )
    )
    stream = EventStream(live_core, own_flow)
    try:
        assert stream.next()["kind"] == "hello"
        started = httpx.post(
            f"{live_core}/flow/run/", params={"flow_id": own_flow}, headers={**headers, "X-Flowfile-Client": "tab-a"}
        )
        assert started.status_code == 200, started.text
        seen = []
        while len(seen) < 10:
            event = stream.next()
            assert event is not None
            seen.append(event)
            if event["kind"] == "run_ended":
                break
        kinds = [event["kind"] for event in seen]
        assert kinds.index("run_started") < kinds.index("run_ended")
        assert {event["origin"] for event in seen if event["kind"].startswith("run_")} == {"tab-a"}
    finally:
        stream.close()


def test_closing_the_flow_ends_the_stream(live_core, own_flow):
    stream = EventStream(live_core, own_flow)
    try:
        assert stream.next()["kind"] == "hello"
        closed = httpx.post(f"{live_core}/editor/close_flow/", params={"flow_id": own_flow}, headers=headers)
        assert closed.status_code == 200, closed.text
        assert stream.next() == {"kind": "closed", "flow_id": own_flow, "revision": None, "origin": None}
        assert stream.next() is None
    finally:
        stream.close()


def test_close_all_ends_an_open_stream(live_core, own_flow):
    stream = EventStream(live_core, own_flow)
    try:
        assert stream.next()["kind"] == "hello"
        change_feed.close_all()
        assert stream.next() is None
    finally:
        stream.close()


def test_a_flow_outside_the_callers_session_is_not_found(live_core, tmp_path):
    flow_id = flow_file_handler.add_flow(name="theirs", flow_path=str(tmp_path / "theirs.yaml"), user_id=987654)
    try:
        assert httpx.get(f"{live_core}/editor/events", params={"flow_id": flow_id}, headers=headers).status_code == 404
        assert httpx.get(f"{live_core}/editor/events", params={"flow_id": 987654321}, headers=headers).status_code == 404
    finally:
        flow_file_handler.delete_flow(flow_id)


def test_the_stream_requires_auth(live_core, own_flow):
    assert httpx.get(f"{live_core}/editor/events", params={"flow_id": own_flow}).status_code == 401
