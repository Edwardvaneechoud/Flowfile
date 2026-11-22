"""Core's ``POST /shutdown``: the internal token from any peer, or no token from the desktop shell that supervises it."""

import socket
import threading

import httpx
import pytest
import uvicorn
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.auth.jwt import get_internal_token


@pytest.fixture
def supervised(monkeypatch):
    """A core the desktop shell runs, which sets ``FLOWFILE_SUPERVISOR_PID`` for its sidecars."""
    monkeypatch.setenv("FLOWFILE_SUPERVISOR_PID", "1")


@pytest.fixture
def unsupervised(monkeypatch):
    monkeypatch.delenv("FLOWFILE_SUPERVISOR_PID", raising=False)


@pytest.fixture
def live_core(monkeypatch):
    """Core's app served by uvicorn on a loopback port of its own, as the server ``/shutdown`` stops."""
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen()
    server = main._Server(uvicorn.Config(main.app, lifespan="off", log_level="warning"))
    monkeypatch.setattr(main, "server_instance", server)
    thread = threading.Thread(target=server.run, kwargs={"sockets": [sock]}, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{sock.getsockname()[1]}"
    for _ in range(100):
        if server.started:
            break
        thread.join(timeout=0.05)
    assert server.started, "uvicorn did not start"
    yield url, thread
    server.should_exit = True
    thread.join(timeout=10)


def test_shutdown_refuses_a_non_loopback_peer_without_the_token(supervised):
    client = TestClient(main.app, client=("10.1.2.3", 4321))
    assert client.post("/shutdown").status_code == 403
    assert client.post("/shutdown", headers={"X-Internal-Token": "not-the-token"}).status_code == 403


def test_shutdown_ignores_a_forwarded_for_header(supervised):
    client = TestClient(main.app, client=("10.1.2.3", 4321))
    assert client.post("/shutdown", headers={"X-Forwarded-For": "127.0.0.1"}).status_code == 403


def test_a_core_no_shell_supervises_refuses_a_loopback_peer_without_the_token(unsupervised):
    """Behind a proxy on the same host that adds no forwarded header, every client is a loopback peer."""
    client = TestClient(main.app, client=("127.0.0.1", 4321))
    assert client.post("/shutdown").status_code == 403
    assert client.post("/shutdown", headers={"X-Internal-Token": "not-the-token"}).status_code == 403


def test_shutdown_answers_the_loopback_peer_of_a_supervised_core(supervised):
    assert main.server_instance is None
    client = TestClient(main.app, client=("127.0.0.1", 4321))
    response = client.post("/shutdown")
    assert response.status_code == 200
    assert response.json() == {"message": "Server is shutting down"}


def test_the_internal_token_shuts_down_from_any_peer(unsupervised):
    assert main.server_instance is None
    client = TestClient(main.app, client=("10.1.2.3", 4321))
    response = client.post("/shutdown", headers={"X-Internal-Token": get_internal_token()})
    assert response.status_code == 200
    assert response.json() == {"message": "Server is shutting down"}


def test_without_the_token_a_served_core_keeps_serving(unsupervised, live_core):
    url, thread = live_core
    assert httpx.post(f"{url}/shutdown", timeout=5).status_code == 403
    thread.join(timeout=2)
    assert thread.is_alive()
    assert httpx.get(f"{url}/docs", timeout=5).status_code == 200


def test_with_the_token_a_served_core_stops(unsupervised, live_core):
    url, thread = live_core
    response = httpx.post(f"{url}/shutdown", headers={"X-Internal-Token": get_internal_token()}, timeout=5)
    assert response.status_code == 200
    thread.join(timeout=10)
    assert not thread.is_alive(), "uvicorn kept serving after /shutdown"
