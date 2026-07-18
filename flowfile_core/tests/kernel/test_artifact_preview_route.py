"""Hermetic tests for GET /kernels/{id}/artifact_preview.

Core proxies a session-only preview blob from the kernel and retains nothing.
No Docker required — the kernel manager is stubbed so the owner/state ladder and
the verbatim passthrough are exercised in-process.
"""

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.kernel import routes as kernel_routes
from flowfile_core.kernel.models import KernelState


@pytest.fixture(scope="module")
def token() -> str:
    with TestClient(main.app) as auth_c:
        return auth_c.post("/auth/token").json()["access_token"]


@pytest.fixture(scope="module")
def client(token: str) -> TestClient:
    c = TestClient(main.app)
    c.headers = {"Authorization": f"Bearer {token}"}
    return c


@pytest.fixture(scope="module")
def owner_id(client: TestClient) -> int:
    return client.get("/auth/users/me").json()["id"]


class _StubKernel:
    def __init__(self, state: KernelState):
        self.state = state


class _StubManager:
    def __init__(self, *, kernel, owner_id, preview=None, raises=False):
        self._kernel = kernel
        self._owner_id = owner_id
        self._preview = preview
        self._raises = raises

    async def get_kernel(self, kernel_id: str):
        return self._kernel

    def get_kernel_owner(self, kernel_id: str):
        return self._owner_id

    async def get_artifact_preview(self, kernel_id: str, flow_id: int, name: str):
        if self._raises:
            raise RuntimeError("kernel unreachable")
        return self._preview


def _install_manager(monkeypatch, manager: _StubManager) -> None:
    monkeypatch.setattr(kernel_routes, "_get_manager", lambda: manager)


def test_unknown_kernel_returns_404(client: TestClient, owner_id: int, monkeypatch):
    _install_manager(monkeypatch, _StubManager(kernel=None, owner_id=owner_id))
    resp = client.get("/kernels/no-such-kernel/artifact_preview", params={"flow_id": 1, "name": "x"})
    assert resp.status_code == 404


def test_non_owner_returns_403(client: TestClient, owner_id: int, monkeypatch):
    kernel = _StubKernel(KernelState.IDLE)
    _install_manager(monkeypatch, _StubManager(kernel=kernel, owner_id=owner_id + 999))
    resp = client.get("/kernels/k1/artifact_preview", params={"flow_id": 1, "name": "x"})
    assert resp.status_code == 403


def test_not_running_returns_null(client: TestClient, owner_id: int, monkeypatch):
    kernel = _StubKernel(KernelState.STOPPED)
    _install_manager(monkeypatch, _StubManager(kernel=kernel, owner_id=owner_id, preview={"data": "abc"}))
    resp = client.get("/kernels/k1/artifact_preview", params={"flow_id": 1, "name": "x"})
    assert resp.status_code == 200
    assert resp.json() is None


def test_running_kernel_passthrough_verbatim(client: TestClient, owner_id: int, monkeypatch):
    preview = {"mime_type": "image/png", "data": "abc", "title": "chart"}
    kernel = _StubKernel(KernelState.EXECUTING)
    _install_manager(monkeypatch, _StubManager(kernel=kernel, owner_id=owner_id, preview=preview))
    resp = client.get("/kernels/k1/artifact_preview", params={"flow_id": 1, "name": "chart"})
    assert resp.status_code == 200
    assert resp.json() == preview


def test_kernel_error_degrades_to_null(client: TestClient, owner_id: int, monkeypatch):
    kernel = _StubKernel(KernelState.IDLE)
    _install_manager(monkeypatch, _StubManager(kernel=kernel, owner_id=owner_id, raises=True))
    resp = client.get("/kernels/k1/artifact_preview", params={"flow_id": 1, "name": "x"})
    assert resp.status_code == 200
    assert resp.json() is None


def test_unauthenticated_is_rejected():
    c = TestClient(main.app)
    resp = c.get("/kernels/k1/artifact_preview", params={"flow_id": 1, "name": "x"})
    assert resp.status_code in (401, 403)
