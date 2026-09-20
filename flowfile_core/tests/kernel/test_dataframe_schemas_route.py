"""Hermetic tests for POST /kernels/{id}/lsp/dataframe_schemas.

Editor-facing, so every not-ready condition degrades to an `unavailable` 200 rather
than an error. No Docker required — the kernel manager is stubbed in-process.
"""

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.configs import settings
from flowfile_core.kernel import routes as kernel_routes
from flowfile_core.kernel.models import KernelState

UNAVAILABLE = {"namespace_generation": "", "revision": 0, "state": "unavailable", "dataframes": []}


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


@pytest.fixture(autouse=True)
def _restore_flag():
    original = bool(settings.FLOWFILE_LSP_ENABLED)
    settings.FLOWFILE_LSP_ENABLED.set(True)
    yield
    settings.FLOWFILE_LSP_ENABLED.set(original)


class _StubKernel:
    def __init__(self, state: KernelState):
        self.state = state


class _StubManager:
    def __init__(self, *, kernel, owner_id, payload=None):
        self._kernel = kernel
        self._owner_id = owner_id
        self._payload = payload
        self.calls: list[tuple[str, str, dict]] = []

    async def get_kernel(self, kernel_id: str):
        return self._kernel

    def get_kernel_owner(self, kernel_id: str):
        return self._owner_id

    async def lsp_request(self, kernel_id: str, op: str, payload: dict) -> dict:
        self.calls.append((kernel_id, op, payload))
        return self._payload or {}


def _install_manager(monkeypatch, manager: _StubManager) -> None:
    async def _get_manager():
        return manager

    monkeypatch.setattr(kernel_routes, "_get_manager", _get_manager)


def test_flag_off_degrades_to_unavailable(client: TestClient, owner_id: int, monkeypatch):
    settings.FLOWFILE_LSP_ENABLED.set(False)
    manager = _StubManager(kernel=_StubKernel(KernelState.IDLE), owner_id=owner_id)
    _install_manager(monkeypatch, manager)
    resp = client.post("/kernels/k1/lsp/dataframe_schemas", json={"flow_id": 1})
    assert resp.status_code == 200
    assert resp.json() == UNAVAILABLE
    assert manager.calls == []


def test_unknown_kernel_degrades_to_unavailable(client: TestClient, owner_id: int, monkeypatch):
    _install_manager(monkeypatch, _StubManager(kernel=None, owner_id=owner_id))
    resp = client.post("/kernels/no-such-kernel/lsp/dataframe_schemas", json={"flow_id": 1})
    assert resp.status_code == 200
    assert resp.json() == UNAVAILABLE


def test_non_owner_degrades_to_unavailable(client: TestClient, owner_id: int, monkeypatch):
    manager = _StubManager(kernel=_StubKernel(KernelState.IDLE), owner_id=owner_id + 999)
    _install_manager(monkeypatch, manager)
    resp = client.post("/kernels/k1/lsp/dataframe_schemas", json={"flow_id": 1})
    assert resp.status_code == 200
    assert resp.json() == UNAVAILABLE
    assert manager.calls == []


def test_kernel_payload_passes_through_verbatim(client: TestClient, owner_id: int, monkeypatch):
    payload = {
        "namespace_generation": "gen-abc",
        "revision": 12,
        "state": "ready",
        "dataframes": [
            {
                "name": "orders",
                "kind": "DataFrame",
                "state": "ready",
                "columns": [{"name": "amount", "dtype": "Float64"}],
                "truncated": False,
            },
            {"name": "lazy", "kind": "LazyFrame", "state": "unresolved", "columns": [], "truncated": False},
        ],
    }
    manager = _StubManager(kernel=_StubKernel(KernelState.EXECUTING), owner_id=owner_id, payload=payload)
    _install_manager(monkeypatch, manager)
    resp = client.post("/kernels/k1/lsp/dataframe_schemas", json={"flow_id": 42, "node_id": 7})
    assert resp.status_code == 200
    assert resp.json() == payload
    assert manager.calls == [("k1", "dataframe_schemas", {"flow_id": 42, "node_id": 7})]


def test_unauthenticated_is_rejected():
    c = TestClient(main.app)
    resp = c.post("/kernels/k1/lsp/dataframe_schemas", json={"flow_id": 1})
    assert resp.status_code in (401, 403)
