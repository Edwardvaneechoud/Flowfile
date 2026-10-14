import pytest
from fastapi.testclient import TestClient

from flowfile_worker import main


@pytest.mark.worker
def test_shutdown_refuses_non_loopback_peer():
    client = TestClient(main.app, client=("10.1.2.3", 4321))
    assert client.post("/shutdown").status_code == 403


@pytest.mark.worker
def test_shutdown_ignores_forwarded_for_header():
    client = TestClient(main.app, client=("10.1.2.3", 4321))
    response = client.post("/shutdown", headers={"X-Forwarded-For": "127.0.0.1"})
    assert response.status_code == 403


@pytest.mark.worker
def test_shutdown_answers_loopback_peer():
    assert main.server_instance is None
    client = TestClient(main.app, client=("127.0.0.1", 4321))
    response = client.post("/shutdown")
    assert response.status_code == 200
    assert response.json() == {"message": "Shutting down"}
