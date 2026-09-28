"""Flow-log routes: the GET stream takes a Bearer header for the caller's own flows; writes need the internal token."""

import json
import socket
import threading
import time
from pathlib import Path

import pytest
import uvicorn
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.auth.jwt import get_internal_token
from flowfile_core.routes.routes import flow_file_handler

LOG_LINE = "hello from the log stream"


def _auth_headers() -> dict[str, str]:
    with TestClient(main.app) as client:
        token = client.post("/auth/token").json()["access_token"]
    return {"Authorization": f"Bearer {token}"}


client = TestClient(main.app)
headers = _auth_headers()


def _logged_flow(tmp_path, user_id: int) -> int:
    flow_id = flow_file_handler.add_flow(name="log_stream", flow_path=str(tmp_path / "logs.yaml"), user_id=user_id)
    flow_file_handler.get_flow(flow_id).flow_logger.info(LOG_LINE)
    return flow_id


def _flow_log_text(flow_id: int) -> str:
    return Path(flow_file_handler.get_flow(flow_id).flow_logger.get_log_filepath()).read_text()


def _raw_log(flow_id: int, message: str) -> dict:
    return {"flowfile_flow_id": flow_id, "log_message": message, "log_type": "WARNING", "node_id": 3}


@pytest.fixture()
def own_flow(tmp_path):
    flow_id = _logged_flow(tmp_path, client.get("/auth/users/me", headers=headers).json()["id"])
    yield flow_id
    flow_file_handler.delete_flow(flow_id)


@pytest.fixture()
def someone_elses_flow(tmp_path):
    flow_id = _logged_flow(tmp_path, user_id=987654)
    yield flow_id
    flow_file_handler.delete_flow(flow_id)


@pytest.fixture()
def live_core():
    """Core's app on a real loopback port, so the worker's own HTTP log shipping is what runs."""
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    server = uvicorn.Server(uvicorn.Config(main.app, lifespan="off", log_level="warning"))
    thread = threading.Thread(target=server.run, kwargs={"sockets": [sock]}, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started:
        assert time.monotonic() < deadline, "uvicorn did not start"
        time.sleep(0.05)
    yield f"http://127.0.0.1:{sock.getsockname()[1]}"
    server.should_exit = True
    thread.join(timeout=10)


def test_header_token_streams_own_flow(own_flow):
    response = client.get(f"/logs/{own_flow}", params={"idle_timeout": 0}, headers=headers, follow_redirects=False)
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    events = [json.loads(block.removeprefix("data: ")) for block in response.text.split("\n\n") if block]
    assert any(event.endswith(f"INFO - {LOG_LINE}") for event in events)


def test_query_token_is_refused(own_flow):
    token = headers["Authorization"].removeprefix("Bearer ")
    response = client.get(f"/logs/{own_flow}", params={"access_token": token, "idle_timeout": 0})
    assert response.status_code == 401


def test_flow_outside_the_callers_session_is_not_found(someone_elses_flow):
    response = client.get(f"/logs/{someone_elses_flow}", params={"idle_timeout": 0}, headers=headers)
    assert response.status_code == 404


@pytest.mark.parametrize(
    "request_headers",
    [{}, {"X-Internal-Token": "not-the-token"}, headers],
    ids=["no-token", "wrong-token", "user-jwt"],
)
def test_unsigned_raw_log_is_refused(own_flow, request_headers):
    response = client.post("/raw_logs", json=_raw_log(own_flow, "forged line"), headers=request_headers)
    assert response.status_code == 401
    assert "forged line" not in _flow_log_text(own_flow)


def test_raw_log_signed_with_the_internal_token_lands(own_flow):
    response = client.post(
        "/raw_logs", json=_raw_log(own_flow, "signed line"), headers={"X-Internal-Token": get_internal_token()}
    )
    assert response.status_code == 200
    assert "Node ID: 3 - signed line" in _flow_log_text(own_flow)


def test_unauthenticated_add_log_route_is_gone(own_flow):
    response = client.post(f"/logs/{own_flow}", params={"log_message": "forged line"})
    assert response.status_code == 405
    assert "forged line" not in _flow_log_text(own_flow)


def test_worker_log_handler_lands_lines_in_core(own_flow, live_core, monkeypatch):
    from flowfile_worker import flow_logger

    get_internal_token()  # core resolves (in electron: mints) the token at startup; the worker reads the same one
    monkeypatch.setattr(flow_logger, "LOGGING_URL", f"{live_core}/raw_logs")
    flow_logger.get_worker_logger(own_flow, 4).warning("shipped by the worker")
    assert "shipped by the worker" in _flow_log_text(own_flow)
