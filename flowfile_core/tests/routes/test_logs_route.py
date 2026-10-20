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
from flowfile_core.routes import logs
from flowfile_core.routes.routes import flow_file_handler
from flowfile_core.schemas import input_schema

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
    sock.listen()  # a request made before uvicorn serves waits in the backlog
    server = uvicorn.Server(uvicorn.Config(main.app, lifespan="off", log_level="warning"))
    thread = threading.Thread(target=server.run, kwargs={"sockets": [sock]}, daemon=True)
    thread.start()
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


@pytest.fixture()
def fresh_unsigned_warning():
    logs._warn_unsigned_raw_log.cache_clear()
    yield
    logs._warn_unsigned_raw_log.cache_clear()


def test_unsigned_raw_log_warns_once_per_process(own_flow, fresh_unsigned_warning, monkeypatch):
    warnings: list[str] = []
    monkeypatch.setattr(logs.logger, "warning", lambda message, *args, **kwargs: warnings.append(message))
    for _ in range(2):
        response = client.post("/raw_logs", json=_raw_log(own_flow, "unsigned line"))
        assert response.status_code == 401
    assert "unsigned line" not in _flow_log_text(own_flow)
    assert len(warnings) == 1
    assert "/raw_logs" in warnings[0] and "must be rebuilt" in warnings[0]
    assert "FLOWFILE_INTERNAL_TOKEN" in warnings[0]


def test_raw_log_signed_with_the_internal_token_lands(own_flow):
    response = client.post(
        "/raw_logs", json=_raw_log(own_flow, "signed line"), headers={"X-Internal-Token": get_internal_token()}
    )
    assert response.status_code == 200
    assert "Node ID: 3 - signed line" in _flow_log_text(own_flow)


def test_post_to_flow_log_route_is_not_allowed(own_flow):
    response = client.post(f"/logs/{own_flow}", params={"log_message": "forged line"})
    assert response.status_code == 405
    assert "forged line" not in _flow_log_text(own_flow)


def test_worker_log_handler_lands_lines_in_core(own_flow, live_core, monkeypatch):
    from flowfile_worker import flow_logger

    get_internal_token()  # core resolves (in electron: mints) the token at startup; the worker reads the same one
    monkeypatch.setattr(flow_logger, "LOGGING_URL", f"{live_core}/raw_logs")
    flow_logger.get_worker_logger(own_flow, 4).warning("shipped by the worker")
    assert "shipped by the worker" in _flow_log_text(own_flow)


def _events(response) -> list[str]:
    return [json.loads(block.removeprefix("data: ")) for block in response.text.split("\n\n") if block]


def _run(flow_id: int, after: float, line: str, for_seconds: float) -> threading.Thread:
    """Claim the flow after a delay, log a line into the run, release it later."""

    def run():
        flow = flow_file_handler.get_flow(flow_id)
        time.sleep(after)
        flow.flow_settings.is_running = True
        flow.flow_logger.info(line)
        time.sleep(for_seconds)
        flow.flow_settings.is_running = False

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread


def test_idle_stream_sends_the_file_once_and_closes(own_flow):
    started = time.monotonic()
    response = client.get(f"/logs/{own_flow}", headers=headers)
    events = _events(response)
    assert response.status_code == 200
    assert any(event.endswith(f"INFO - {LOG_LINE}") for event in events)
    assert time.monotonic() - started < 2  # closed with the file


def test_stream_opened_during_a_run_follows_it_and_ends_with_it(own_flow):
    thread = _run(own_flow, after=0, line="written while running", for_seconds=1.0)
    time.sleep(0.1)
    response = client.get(f"/logs/{own_flow}", headers=headers)
    thread.join()
    events = _events(response)
    assert any(event.endswith("written while running") for event in events)


def test_run_request_claims_the_run_and_rewrites_its_log_before_it_answers(own_flow, monkeypatch):
    """The queued task finds the run claimed and the log file truncated: so does a client the response reached."""
    from flowfile_core.routes import routes

    seen: dict[str, object] = {}

    def run_and_track(flow, user_id, node_ids=None):
        seen["running"] = flow.flow_settings.is_running
        seen["log"] = _flow_log_text(own_flow)
        flow.release_run()

    monkeypatch.setattr(routes, "_run_and_track", run_and_track)
    response = client.post("/flow/run/", params={"flow_id": own_flow}, headers=headers)
    assert response.status_code == 200
    assert seen == {"running": True, "log": ""}
    assert flow_file_handler.get_flow(own_flow).flow_settings.is_running is False


def test_the_log_is_truncated_before_the_claim_is_visible(own_flow, monkeypatch):
    """A stream opened on ``is_running`` must never read the previous run's file, so truncation comes first."""
    flow = flow_file_handler.get_flow(own_flow)
    seen: dict[str, object] = {}
    clear = flow.flow_logger.clear_log_file

    def clear_log_file():
        seen["running_at_truncation"] = flow.flow_settings.is_running
        clear()

    monkeypatch.setattr(flow.flow_logger, "clear_log_file", clear_log_file)
    assert LOG_LINE in _flow_log_text(own_flow)
    assert flow.try_claim_run()
    try:
        assert seen == {"running_at_truncation": False}
        assert _flow_log_text(own_flow) == ""
    finally:
        flow.release_run()


def test_a_run_whose_pre_work_fails_releases_the_claim(own_flow, monkeypatch):
    from flowfile_core.routes import routes

    def failing_pre_work(flow, user_id, node_ids):
        raise RuntimeError("registration broke")

    monkeypatch.setattr(routes, "_open_run_record", failing_pre_work)
    with pytest.raises(RuntimeError, match="registration broke"):
        client.post("/flow/run/", params={"flow_id": own_flow}, headers=headers)
    assert flow_file_handler.get_flow(own_flow).flow_settings.is_running is False


def _post_with_failing_send(path: str, query: str) -> None:
    """Drive the ASGI app directly with a ``send`` that fails on the response start."""
    import asyncio

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "root_path": "",
        "query_string": query.encode(),
        "headers": [(k.lower().encode(), v.encode()) for k, v in headers.items()],
        "client": ("127.0.0.1", 1),
        "server": ("127.0.0.1", 80),
    }

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        raise ConnectionResetError("the client went away")

    with pytest.raises(ConnectionResetError):
        asyncio.run(main.app(scope, receive, send))


def test_a_run_whose_answer_cannot_be_sent_still_runs_and_releases_the_claim(own_flow, monkeypatch):
    """Starlette runs background tasks only after the body went out: the claimed run must not depend on that."""
    from flowfile_core.routes import routes

    seen: dict[str, object] = {}

    def run_and_track(flow, user_id, node_ids=None):
        seen["running"] = flow.flow_settings.is_running
        flow.release_run()

    monkeypatch.setattr(routes, "_run_and_track", run_and_track)
    _post_with_failing_send("/flow/run/", f"flow_id={own_flow}")
    assert seen == {"running": True}
    assert flow_file_handler.get_flow(own_flow).flow_settings.is_running is False


def test_a_fetch_whose_answer_cannot_be_sent_still_releases_the_claim(own_flow, monkeypatch):
    flow = flow_file_handler.get_flow(own_flow)
    flow.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=own_flow, node_id=1, raw_data_format=input_schema.RawData.from_pylist([{"a": 1}])
        )
    )
    _post_with_failing_send("/node/trigger_fetch_data", f"flow_id={own_flow}&node_id=1")
    assert flow.flow_settings.is_running is False
    assert flow.get_node(1).node_stats.has_run_with_current_setup


def test_a_second_run_request_is_refused_while_the_first_is_claimed(own_flow):
    flow = flow_file_handler.get_flow(own_flow)
    assert flow.try_claim_run()
    try:
        response = client.post("/flow/run/", params={"flow_id": own_flow}, headers=headers)
        assert response.status_code == 422
    finally:
        flow.release_run()


def test_stream_opened_after_the_run_request_reads_that_run(own_flow, live_core):
    """Over a real server the task runs after the response: the stream still never sees the previous file."""
    import httpx

    posted = httpx.post(f"{live_core}/flow/run/", params={"flow_id": own_flow}, headers=headers, timeout=30)
    assert posted.status_code == 200, posted.text
    events: list[str] = []
    with httpx.stream("GET", f"{live_core}/logs/{own_flow}", headers=headers, timeout=60) as stream:
        for block in stream.iter_text():
            events.extend(json.loads(line.removeprefix("data: ")) for line in block.split("\n\n") if line.strip())
    assert not any(LOG_LINE in event for event in events)
    assert any("Flow completed!" in event for event in events), events
