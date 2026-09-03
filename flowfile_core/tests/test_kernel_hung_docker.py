"""A hung Docker daemon must never freeze core: KernelManager construction is
bounded by a short-timeout probe, and the kernel routes fetch the manager off
the event loop so every other request keeps being answered meanwhile.

Docker-free and mock-free: the fake daemon is a real AF_UNIX socket that
accepts every connection and never writes a byte — the SDK's real transport.
"""

import os
import shutil
import socket
import sys
import tempfile
import threading
import time
from collections.abc import Iterator

import docker.errors
import pytest
from fastapi.testclient import TestClient

import flowfile_core.kernel as kernel_pkg
from flowfile_core import main
from flowfile_core.auth.jwt import get_current_active_user, get_current_user
from flowfile_core.auth.models import User as PydanticUser
from flowfile_core.kernel import manager as kernel_manager
from flowfile_core.kernel.manager import KernelManager

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="fake daemon is an AF_UNIX socket")

DOCKER_UNAVAILABLE_DETAIL = "Docker is not available. Please ensure Docker is installed and running."
# Long enough that a loop-blocking probe visibly delays /docs; the routes stay under the 3 s bound.
PROBE_TIMEOUT_SECONDS = 2.0


@pytest.fixture
def hung_docker(monkeypatch) -> Iterator[str]:
    """Point DOCKER_HOST at a daemon that accepts connections and never answers."""
    # mkdtemp (TMPDIR) keeps the path short: macOS caps AF_UNIX paths at 104 bytes.
    sock_dir = tempfile.mkdtemp(prefix="ffd-")
    sock_path = os.path.join(sock_dir, "d.sock")
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(sock_path)
    listener.listen(16)
    listener.settimeout(0.1)
    held: list[socket.socket] = []
    stop = threading.Event()

    def accept_forever():
        while not stop.is_set():
            try:
                conn, _ = listener.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            held.append(conn)

    thread = threading.Thread(target=accept_forever, name="hung-docker", daemon=True)
    thread.start()

    monkeypatch.setenv("DOCKER_HOST", f"unix://{sock_path}")
    monkeypatch.setattr(kernel_manager, "_DOCKER_PROBE_TIMEOUT_SECONDS", PROBE_TIMEOUT_SECONDS)
    monkeypatch.setattr(kernel_pkg, "_manager", None)
    try:
        yield sock_path
    finally:
        stop.set()
        thread.join(2.0)
        for conn in held:
            conn.close()
        listener.close()
        shutil.rmtree(sock_dir, ignore_errors=True)


@pytest.fixture
def admin_client() -> Iterator[TestClient]:
    """One shared TestClient: inside ``with`` every request runs on a single event loop."""
    user = PydanticUser(username="local_user", id=1, disabled=False, is_admin=True, must_change_password=False)
    main.app.dependency_overrides[get_current_active_user] = lambda: user
    main.app.dependency_overrides[get_current_user] = lambda: user
    try:
        with TestClient(main.app) as client:
            yield client
    finally:
        main.app.dependency_overrides.pop(get_current_active_user, None)
        main.app.dependency_overrides.pop(get_current_user, None)


def _request_while_docker_hangs(client: TestClient, path: str):
    """GET ``path`` from a background thread; meanwhile GET /docs must answer on the same loop."""
    outcome: dict = {}
    started = threading.Event()

    def fire():
        started.set()
        t0 = time.perf_counter()
        outcome["response"] = client.get(path)
        outcome["elapsed"] = time.perf_counter() - t0

    thread = threading.Thread(target=fire, name=f"hung-{path}")
    thread.start()
    started.wait(1.0)
    time.sleep(0.1)

    t0 = time.perf_counter()
    docs = client.get("/docs")
    docs_elapsed = time.perf_counter() - t0
    thread.join(10.0)

    assert not thread.is_alive(), f"{path} never returned"
    assert docs.status_code == 200
    assert docs_elapsed < PROBE_TIMEOUT_SECONDS / 2, f"/docs waited {docs_elapsed:.2f}s behind {path}"
    assert outcome["elapsed"] < 3.0, f"{path} took {outcome['elapsed']:.2f}s"
    return outcome["response"]


def test_manager_construction_fails_fast_on_hung_daemon(hung_docker):
    t0 = time.perf_counter()
    with pytest.raises(docker.errors.DockerException):
        KernelManager()
    assert time.perf_counter() - t0 < 3.0


def test_other_requests_are_answered_while_kernels_route_waits_on_docker(hung_docker, admin_client):
    response = _request_while_docker_hangs(admin_client, "/kernels/")

    assert response.status_code == 503
    assert response.json()["detail"] == DOCKER_UNAVAILABLE_DETAIL
    assert kernel_pkg._manager is None


def test_docker_status_reports_unavailable_fast_on_hung_daemon(hung_docker, admin_client):
    response = _request_while_docker_hangs(admin_client, "/kernels/docker-status")

    assert response.status_code == 200
    body = response.json()
    assert body["available"] is False
    assert body["image_available"] is False
    assert body["error"]
