import pytest
from fastapi.testclient import TestClient

from flowfile_worker import main
from tests.conftest import INTERNAL_AUTH_HEADERS


@pytest.fixture
def supervised(monkeypatch):
    """A worker the desktop shell runs, which sets ``FLOWFILE_SUPERVISOR_PID`` for its sidecars."""
    monkeypatch.setenv("FLOWFILE_SUPERVISOR_PID", "1")


@pytest.fixture
def unsupervised(monkeypatch):
    monkeypatch.delenv("FLOWFILE_SUPERVISOR_PID", raising=False)


@pytest.mark.worker
def test_shutdown_refuses_non_loopback_peer(supervised):
    client = TestClient(main.app, client=("10.1.2.3", 4321))
    assert client.post("/shutdown").status_code == 403


@pytest.mark.worker
def test_shutdown_ignores_forwarded_for_header(supervised):
    client = TestClient(main.app, client=("10.1.2.3", 4321))
    response = client.post("/shutdown", headers={"X-Forwarded-For": "127.0.0.1"})
    assert response.status_code == 403


@pytest.mark.worker
def test_shutdown_answers_loopback_peer_of_a_supervised_worker(supervised):
    assert main.server_instance is None
    client = TestClient(main.app, client=("127.0.0.1", 4321))
    response = client.post("/shutdown")
    assert response.status_code == 200
    assert response.json() == {"message": "Shutting down"}


@pytest.mark.worker
def test_a_worker_no_shell_supervises_refuses_a_loopback_peer_without_the_token(unsupervised):
    """Behind a proxy on the same host that adds no forwarded header, every client is a loopback peer."""
    client = TestClient(main.app, client=("127.0.0.1", 4321))
    assert client.post("/shutdown").status_code == 403
    assert client.post("/shutdown", headers={"X-Flowfile-Internal": "not-the-token"}).status_code == 403


@pytest.mark.worker
def test_the_internal_token_shuts_down_from_any_peer(unsupervised):
    assert main.server_instance is None
    client = TestClient(main.app, client=("10.1.2.3", 4321))
    response = client.post("/shutdown", headers=INTERNAL_AUTH_HEADERS)
    assert response.status_code == 200
    assert response.json() == {"message": "Shutting down"}
