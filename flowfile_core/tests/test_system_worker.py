"""Tests for the /system/worker_pool admin proxy.

Runs the REAL worker FastAPI app on a local uvicorn so the whole chain is
exercised without mocks: core route -> HTTP -> worker /pool -> TaskPool.
Auth fixtures are tests/conftest.py's admin_client and non_admin_client.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator

import polars as pl
import pytest
import uvicorn
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake
from shared.compute_protocol import local_capabilities
from tests.conftest import _free_port, closed_url


def point_the_worker_backend_at(monkeypatch, url: str) -> None:
    """The proxy forwards to the worker the registry's worker backend runs on; make that ``url`` for the test."""
    monkeypatch.setattr(registry.backend(registry.WORKER_BACKEND_ID), "url", url)


@pytest.fixture
def live_worker(monkeypatch, tmp_path) -> Iterator[str]:
    """Serve the real flowfile_worker app on a free local port; point the worker backend, and so the proxy, at it."""
    from flowfile_worker import main as worker_main
    from flowfile_worker import pool as worker_pool

    # Keep persistence away from the developer's real ~/.flowfile and CI env.
    monkeypatch.setattr(worker_pool, "_settings_file", lambda: tmp_path / "worker_pool.yaml")
    monkeypatch.delenv("FLOWFILE_WORKER_POOL_SIZE", raising=False)

    port = _free_port()

    server = uvicorn.Server(uvicorn.Config(worker_main.app, host="127.0.0.1", port=port, log_level="error"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 30
    while not server.started:
        if time.monotonic() > deadline:
            pytest.fail("in-process worker never became ready")
        time.sleep(0.05)

    base_url = f"http://127.0.0.1:{port}"
    point_the_worker_backend_at(monkeypatch, base_url)
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
    point_the_worker_backend_at(monkeypatch, closed_url())

    response = admin_client.get("/system/worker_pool")
    assert response.status_code == 503, response.text
    assert response.json()["detail"]["error_code"] == "WORKER_UNAVAILABLE"


@pytest.fixture
def unchecked_worker() -> Iterator[None]:
    """The proxy's request is core's first worker contact, so it runs the handshake."""
    handshake.reset_handshake()
    yield
    handshake.reset_handshake()


def test_worker_on_other_versions_surfaces_typed_503(admin_client: TestClient, unchecked_worker, monkeypatch) -> None:
    reported = local_capabilities().model_copy(update={"polars_version": "0.0.1"})
    monkeypatch.setattr(WorkerBackend, "capabilities", lambda self: reported)

    response = admin_client.get("/system/worker_pool")

    assert response.status_code == 503, response.text
    assert response.json()["detail"] == {
        "error_code": "WORKER_UNAVAILABLE",
        "message": f'Backend "worker" does not match core (polars_version: core {pl.__version__}, backend 0.0.1)',
    }


def test_worker_down_on_first_contact_surfaces_typed_503(
    admin_client: TestClient, unchecked_worker, monkeypatch
) -> None:
    dead_url = closed_url()
    point_the_worker_backend_at(monkeypatch, dead_url)

    response = admin_client.get("/system/worker_pool")

    assert response.status_code == 503, response.text
    detail = response.json()["detail"]
    assert detail["error_code"] == "WORKER_UNAVAILABLE"
    assert detail["message"].startswith(f"Worker not reachable: Could not reach the worker at {dead_url}: ")
    assert detail["message"].endswith("Is the compute backend running? Start it, or check the Backends page.")


def test_out_of_range_size_returns_422(admin_client: TestClient, live_worker: str) -> None:
    assert admin_client.post("/system/worker_pool", json={"size": 99}).status_code == 422
