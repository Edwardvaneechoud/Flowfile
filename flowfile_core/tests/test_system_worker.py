"""Tests for the /system/worker_pool admin proxy.

Runs the REAL worker FastAPI app on a local uvicorn so the whole chain is
exercised without mocks: core route -> HTTP -> worker /pool -> TaskPool.
Auth fixtures mirror tests/ai/test_admin_routes.py (dependency overrides).
"""

from __future__ import annotations

import socket
import threading
import time
from collections.abc import Iterator

import pytest
import uvicorn
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.auth.jwt import get_current_active_user, get_current_user
from flowfile_core.auth.models import User as PydanticUser
from flowfile_core.routes import system_worker


@pytest.fixture
def admin_client() -> Iterator[TestClient]:
    user = PydanticUser(username="local_user", id=1, disabled=False, is_admin=True, must_change_password=False)
    main.app.dependency_overrides[get_current_active_user] = lambda: user
    main.app.dependency_overrides[get_current_user] = lambda: user
    try:
        yield TestClient(main.app)
    finally:
        main.app.dependency_overrides.pop(get_current_active_user, None)
        main.app.dependency_overrides.pop(get_current_user, None)


@pytest.fixture
def non_admin_client() -> Iterator[TestClient]:
    user = PydanticUser(username="not_admin", id=2, disabled=False, is_admin=False, must_change_password=False)
    main.app.dependency_overrides[get_current_active_user] = lambda: user
    main.app.dependency_overrides[get_current_user] = lambda: user
    try:
        yield TestClient(main.app)
    finally:
        main.app.dependency_overrides.pop(get_current_active_user, None)
        main.app.dependency_overrides.pop(get_current_user, None)


@pytest.fixture
def live_worker(monkeypatch, tmp_path) -> Iterator[str]:
    """Serve the real flowfile_worker app on a free local port; point the proxy at it."""
    from flowfile_worker import main as worker_main
    from flowfile_worker import pool as worker_pool

    # Keep persistence away from the developer's real ~/.flowfile and CI env.
    monkeypatch.setattr(worker_pool, "_settings_file", lambda: tmp_path / "worker_pool.yaml")
    monkeypatch.delenv("FLOWFILE_WORKER_POOL_SIZE", raising=False)

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]

    server = uvicorn.Server(uvicorn.Config(worker_main.app, host="127.0.0.1", port=port, log_level="error"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 30
    while not server.started:
        if time.monotonic() > deadline:
            pytest.fail("in-process worker never became ready")
        time.sleep(0.05)

    base_url = f"http://127.0.0.1:{port}"
    monkeypatch.setattr(system_worker, "WORKER_URL", base_url)
    try:
        yield base_url
    finally:
        from flowfile_worker.pool import task_pool

        task_pool.resize(0)
        server.should_exit = True
        thread.join(timeout=15)


def test_admin_reads_and_resizes_pool_end_to_end(admin_client: TestClient, live_worker: str) -> None:
    from flowfile_worker.pool import task_pool

    state = admin_client.get("/system/worker_pool")
    assert state.status_code == 200, state.text
    assert state.json()["persisted"] is False, "nothing saved yet on a fresh worker"

    resized = admin_client.post("/system/worker_pool", json={"size": 2})
    assert resized.status_code == 200, resized.text
    body = resized.json()
    assert body["enabled"] is True and body["size"] == 2
    assert body["persisted"] is True, "a UI resize must be saved for future boots"
    assert body["env_override"] is False
    assert task_pool.stats()["size"] == 2, "the real worker pool must have been resized"

    disabled = admin_client.post("/system/worker_pool", json={"size": 0})
    assert disabled.status_code == 200, disabled.text
    assert disabled.json()["enabled"] is False
    assert task_pool.stats() == {"size": 0, "idle": 0, "total": 0}


def test_non_admin_gets_403(non_admin_client: TestClient, live_worker: str) -> None:
    assert non_admin_client.get("/system/worker_pool").status_code == 403
    assert non_admin_client.post("/system/worker_pool", json={"size": 1}).status_code == 403


def test_unauthenticated_gets_401() -> None:
    client = TestClient(main.app)
    assert client.get("/system/worker_pool").status_code == 401


def test_worker_down_surfaces_typed_503(admin_client: TestClient, monkeypatch) -> None:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        dead_port = probe.getsockname()[1]
    monkeypatch.setattr(system_worker, "WORKER_URL", f"http://127.0.0.1:{dead_port}")

    response = admin_client.get("/system/worker_pool")
    assert response.status_code == 503, response.text
    assert response.json()["detail"]["error_code"] == "WORKER_UNAVAILABLE"


def test_out_of_range_size_returns_422(admin_client: TestClient, live_worker: str) -> None:
    assert admin_client.post("/system/worker_pool", json={"size": 99}).status_code == 422
