"""Hermetic tests for POST /kernels/match — no Docker required.

The route resolves the kernel manager lazily via
``flowfile_core.kernel.get_kernel_manager_if_initialized``; patching that
name swaps between a mocked live manager and the Docker-down DB fallback.
"""

from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

import flowfile_core.kernel as kernel_pkg
from flowfile_core import main
from flowfile_core.database.connection import get_db_context
from flowfile_core.kernel import persistence
from flowfile_core.kernel.models import ImageFlavour, KernelInfo, KernelState, ResolvedPackage


@pytest.fixture(scope="module")
def client() -> TestClient:
    with TestClient(main.app) as auth_c:
        token = auth_c.post("/auth/token").json()["access_token"]
    c = TestClient(main.app)
    c.headers = {"Authorization": f"Bearer {token}"}
    return c


@pytest.fixture(scope="module")
def user_id(client: TestClient) -> int:
    return client.get("/auth/users/me").json()["id"]


def _kernel(kernel_id: str, name: str, flavour=ImageFlavour.BASE, packages=None, resolved=None) -> KernelInfo:
    return KernelInfo(
        id=kernel_id,
        name=name,
        image_flavour=flavour,
        packages=packages or [],
        resolved_packages=[ResolvedPackage(name=n, version=v) for n, v in (resolved or [])],
    )


def _mock_manager(owned: list[KernelInfo], all_kernels: list[KernelInfo], local_image: str | None = "img:1"):
    manager = MagicMock()

    async def list_kernels(user_id=None):
        return owned if user_id is not None else all_kernels

    manager.list_kernels = list_kernels
    manager.resolve_local_image = MagicMock(return_value=local_image)
    return manager


class TestMatchWithManager:
    def test_ranked_best_first_shape(self, client, monkeypatch):
        ml = _kernel("ml1", "ML kernel", flavour=ImageFlavour.ML)
        base = _kernel("b1", "Base kernel")
        manager = _mock_manager(owned=[base, ml], all_kernels=[base, ml])
        monkeypatch.setattr(kernel_pkg, "get_kernel_manager_if_initialized", lambda: manager)

        resp = client.post("/kernels/match", json={"dependencies": ["scikit-learn>=1.0"], "node_name": "Clusterer"})
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["docker_available"] is True
        assert [m["kernel_id"] for m in body["matches"]] == ["ml1", "b1"]
        assert body["matches"][0]["level"] == "full"
        assert body["matches"][1]["level"] == "none"
        assert body["matches"][1]["missing"] == ["scikit-learn>=1.0"]
        suggestion = body["suggestion"]
        assert suggestion["config"]["id"] == "clusterer-kernel"
        assert suggestion["config"]["image_flavour"] == "ml"
        assert suggestion["config"]["packages"] == []
        assert suggestion["covered_by_flavour"] == ["scikit-learn>=1.0"]
        assert suggestion["flavour_image_available"] is True

    def test_ownership_filter_and_global_id_collision(self, client, monkeypatch):
        mine = _kernel("mine", "Mine")
        other = _kernel("clusterer-kernel", "Someone else's")
        manager = _mock_manager(owned=[mine], all_kernels=[mine, other])
        monkeypatch.setattr(kernel_pkg, "get_kernel_manager_if_initialized", lambda: manager)

        resp = client.post("/kernels/match", json={"dependencies": [], "node_name": "Clusterer"})
        assert resp.status_code == 200
        body = resp.json()
        # Another user's kernel never appears in the ranked matches...
        assert [m["kernel_id"] for m in body["matches"]] == ["mine"]
        # ...but its id still blocks the suggestion (create enforces global uniqueness).
        assert body["suggestion"]["config"]["id"] == "clusterer-kernel-2"

    def test_flavour_image_unavailable_reported(self, client, monkeypatch):
        manager = _mock_manager(owned=[], all_kernels=[], local_image=None)
        monkeypatch.setattr(kernel_pkg, "get_kernel_manager_if_initialized", lambda: manager)

        resp = client.post("/kernels/match", json={"dependencies": ["scikit-learn>=1.0"]})
        assert resp.status_code == 200
        assert resp.json()["suggestion"]["flavour_image_available"] is False

    def test_empty_dependencies(self, client, monkeypatch):
        kernel = _kernel("k1", "Kernel")
        manager = _mock_manager(owned=[kernel], all_kernels=[kernel])
        monkeypatch.setattr(kernel_pkg, "get_kernel_manager_if_initialized", lambda: manager)

        resp = client.post("/kernels/match", json={"dependencies": []})
        assert resp.status_code == 200
        body = resp.json()
        assert body["matches"][0]["level"] == "full"
        assert body["suggestion"]["config"]["packages"] == []
        assert body["suggestion"]["config"]["image_flavour"] == "base"


class TestMatchDockerDown:
    def test_db_fallback(self, client, user_id, monkeypatch):
        monkeypatch.setattr(kernel_pkg, "get_kernel_manager_if_initialized", lambda: None)

        owned = _kernel(
            "dbk-owned", "DB kernel", flavour=ImageFlavour.BASE, packages=["pandas"], resolved=[("pandas", "2.2.0")]
        )
        foreign = _kernel("dbk-foreign", "Foreign kernel")
        with get_db_context() as db:
            persistence.save_kernel(db, owned, user_id)
            persistence.save_kernel(db, foreign, user_id + 1)
        try:
            resp = client.post("/kernels/match", json={"dependencies": ["pandas>=2"], "node_name": "Dbk Owned"})
            assert resp.status_code == 200, resp.text
            body = resp.json()
            assert body["docker_available"] is False
            assert body["suggestion"]["flavour_image_available"] is None

            by_id = {m["kernel_id"]: m for m in body["matches"]}
            assert "dbk-owned" in by_id
            assert "dbk-foreign" not in by_id
            assert by_id["dbk-owned"]["level"] == "full"
            assert by_id["dbk-owned"]["state"] == KernelState.STOPPED.value

            # The suggestion still dodges every persisted id, including the caller's own.
            assert body["suggestion"]["config"]["id"] == "dbk-owned-kernel"
        finally:
            with get_db_context() as db:
                persistence.delete_kernel(db, "dbk-owned")
                persistence.delete_kernel(db, "dbk-foreign")

    def test_db_fallback_id_collision(self, client, user_id, monkeypatch):
        monkeypatch.setattr(kernel_pkg, "get_kernel_manager_if_initialized", lambda: None)

        clash = _kernel("colliding-node-kernel", "Clash")
        with get_db_context() as db:
            persistence.save_kernel(db, clash, user_id + 1)
        try:
            resp = client.post("/kernels/match", json={"dependencies": [], "node_name": "Colliding Node"})
            assert resp.status_code == 200
            assert resp.json()["suggestion"]["config"]["id"] == "colliding-node-kernel-2"
        finally:
            with get_db_context() as db:
                persistence.delete_kernel(db, "colliding-node-kernel")


class TestMatchBatch:
    def test_batch_summaries(self, client, monkeypatch):
        ml = _kernel("ml1", "ML kernel", flavour=ImageFlavour.ML)
        manager = _mock_manager(owned=[ml], all_kernels=[ml])
        monkeypatch.setattr(kernel_pkg, "get_kernel_manager_if_initialized", lambda: manager)

        resp = client.post(
            "/kernels/match/batch",
            json={"items": {"a": ["scikit-learn>=1.0"], "b": ["requests>=2"], "c": []}},
        )
        assert resp.status_code == 200, resp.text
        results = resp.json()["results"]
        assert results["a"]["level"] == "full"
        assert results["a"]["best_kernel_name"] == "ML kernel"
        assert results["b"]["level"] == "none"
        assert results["b"]["best_kernel_id"] is None
        assert results["c"]["level"] == "full"

    def test_batch_without_kernels(self, client, monkeypatch):
        manager = _mock_manager(owned=[], all_kernels=[])
        monkeypatch.setattr(kernel_pkg, "get_kernel_manager_if_initialized", lambda: manager)

        resp = client.post("/kernels/match/batch", json={"items": {"x": ["numpy"]}})
        assert resp.status_code == 200
        assert resp.json()["results"]["x"] == {
            "level": "none",
            "best_kernel_id": None,
            "best_kernel_name": None,
        }

    def test_batch_docker_down_uses_db(self, client, user_id, monkeypatch):
        monkeypatch.setattr(kernel_pkg, "get_kernel_manager_if_initialized", lambda: None)
        owned = _kernel("dbk-batch", "Batch kernel", flavour=ImageFlavour.ML)
        with get_db_context() as db:
            persistence.save_kernel(db, owned, user_id)
        try:
            resp = client.post("/kernels/match/batch", json={"items": {"a": ["scikit-learn>=1.0"]}})
            assert resp.status_code == 200
            body = resp.json()
            assert body["docker_available"] is False
            assert body["results"]["a"]["level"] == "full"
            assert body["results"]["a"]["best_kernel_id"] == "dbk-batch"
        finally:
            with get_db_context() as db:
                persistence.delete_kernel(db, "dbk-batch")


def test_match_requires_auth():
    resp = TestClient(main.app).post("/kernels/match", json={"dependencies": []})
    assert resp.status_code == 401
    resp = TestClient(main.app).post("/kernels/match/batch", json={"items": {}})
    assert resp.status_code == 401
