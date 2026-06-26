"""Hermetic tests for the core LSP surface: the /lsp/capabilities probe and the
owner-checked /kernels/{id}/lsp/* bridge degradation. No Docker required — the
bridge degrades to empty 200 whether or not a kernel/Docker is present.
"""

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.configs import settings


@pytest.fixture(scope="module")
def client() -> TestClient:
    with TestClient(main.app) as auth_c:
        token = auth_c.post("/auth/token").json()["access_token"]
    c = TestClient(main.app)
    c.headers = {"Authorization": f"Bearer {token}"}
    return c


@pytest.fixture(autouse=True)
def _restore_flag():
    original = bool(settings.FLOWFILE_LSP_ENABLED)
    yield
    settings.FLOWFILE_LSP_ENABLED.set(original)


def test_capabilities_enabled(client: TestClient):
    settings.FLOWFILE_LSP_ENABLED.set(True)
    resp = client.get("/lsp/capabilities")
    assert resp.status_code == 200
    body = resp.json()
    assert body["enabled"] is True
    assert "complete" in body["features"]


def test_capabilities_disabled(client: TestClient):
    settings.FLOWFILE_LSP_ENABLED.set(False)
    resp = client.get("/lsp/capabilities")
    assert resp.status_code == 200
    body = resp.json()
    assert body["enabled"] is False
    assert body["features"] == []


def _complete_payload() -> dict:
    return {"code": "pl.", "line": 1, "column": 3, "flow_id": -1}


def test_bridge_complete_degrades_when_flag_off(client: TestClient):
    settings.FLOWFILE_LSP_ENABLED.set(False)
    resp = client.post("/kernels/no-such-kernel/lsp/complete", json=_complete_payload())
    assert resp.status_code == 200
    assert resp.json() == {"items": []}


def test_bridge_complete_degrades_for_unknown_kernel(client: TestClient):
    settings.FLOWFILE_LSP_ENABLED.set(True)
    resp = client.post("/kernels/no-such-kernel/lsp/complete", json=_complete_payload())
    assert resp.status_code == 200
    assert resp.json() == {"items": []}


def test_bridge_hover_and_signature_degrade(client: TestClient):
    settings.FLOWFILE_LSP_ENABLED.set(True)
    hov = client.post("/kernels/no-such-kernel/lsp/hover", json=_complete_payload())
    assert hov.status_code == 200 and hov.json() == {"contents": None}
    sig = client.post("/kernels/no-such-kernel/lsp/signature", json=_complete_payload())
    assert sig.status_code == 200 and sig.json() == {"signatures": [], "active_signature": 0}


def test_bridge_diagnostics_degrades(client: TestClient):
    settings.FLOWFILE_LSP_ENABLED.set(True)
    resp = client.post("/kernels/no-such-kernel/lsp/diagnostics", json=_complete_payload())
    assert resp.status_code == 200
    assert resp.json() == {"diagnostics": []}


def test_admin_flag_get_and_set(client: TestClient):
    # Electron-mode local_user is admin, so the /system route is reachable with the test token.
    assert client.get("/system/feature_flags/lsp").json()["enabled"] is True

    off = client.post("/system/feature_flags/lsp", json={"enabled": False})
    assert off.status_code == 200
    assert off.json() == {"enabled": False, "persisted": False}
    # The live flip is visible to the kernel-independent capabilities probe.
    assert client.get("/lsp/capabilities").json()["enabled"] is False

    on = client.post("/system/feature_flags/lsp", json={"enabled": True})
    assert on.json()["enabled"] is True
    assert client.get("/lsp/capabilities").json()["enabled"] is True
