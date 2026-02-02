"""Tests for the Flow Catalog API endpoints.

Covers namespace CRUD, flow registration, favorites, follows,
run history, stats, and the default namespace seeding.
"""

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.init_db import init_db
from flowfile_core.database.models import (
    CatalogNamespace,
    FlowFavorite,
    FlowFollow,
    FlowRegistration,
    FlowRun,
    User,
)
from flowfile_core.routes.routes import _auto_register_flow


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_auth_token() -> str:
    with TestClient(main.app) as client:
        response = client.post("/auth/token")
        return response.json()["access_token"]


def _get_test_client() -> TestClient:
    token = _get_auth_token()
    client = TestClient(main.app)
    client.headers = {"Authorization": f"Bearer {token}"}
    return client


client = _get_test_client()


def _cleanup_catalog():
    """Remove all catalog-related rows so tests start clean."""
    with get_db_context() as db:
        db.query(FlowFollow).delete()
        db.query(FlowFavorite).delete()
        db.query(FlowRun).delete()
        db.query(FlowRegistration).delete()
        db.query(CatalogNamespace).delete()
        db.commit()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def clean_catalog():
    """Ensure a clean catalog state for every test."""
    _cleanup_catalog()
    yield
    _cleanup_catalog()


# ---------------------------------------------------------------------------
# Namespace tests
# ---------------------------------------------------------------------------


class TestNamespaces:
    def test_create_catalog(self):
        resp = client.post(
            "/catalog/namespaces",
            json={"name": "TestCatalog", "description": "A test catalog"},
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["name"] == "TestCatalog"
        assert data["level"] == 0
        assert data["parent_id"] is None

    def test_create_schema_under_catalog(self):
        # Create catalog first
        cat = client.post("/catalog/namespaces", json={"name": "Cat"}).json()
        resp = client.post(
            "/catalog/namespaces",
            json={"name": "Schema1", "parent_id": cat["id"]},
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["level"] == 1
        assert data["parent_id"] == cat["id"]

    def test_reject_deep_nesting(self):
        cat = client.post("/catalog/namespaces", json={"name": "Cat"}).json()
        schema = client.post(
            "/catalog/namespaces", json={"name": "S", "parent_id": cat["id"]}
        ).json()
        resp = client.post(
            "/catalog/namespaces", json={"name": "Deep", "parent_id": schema["id"]}
        )
        assert resp.status_code == 422

    def test_duplicate_name_same_parent_rejected(self):
        client.post("/catalog/namespaces", json={"name": "Dup"})
        resp = client.post("/catalog/namespaces", json={"name": "Dup"})
        assert resp.status_code == 409

    def test_list_namespaces(self):
        client.post("/catalog/namespaces", json={"name": "A"})
        client.post("/catalog/namespaces", json={"name": "B"})
        resp = client.get("/catalog/namespaces")
        assert resp.status_code == 200
        assert len(resp.json()) >= 2

    def test_update_namespace(self):
        ns = client.post("/catalog/namespaces", json={"name": "Old"}).json()
        resp = client.put(
            f"/catalog/namespaces/{ns['id']}", json={"name": "New"}
        )
        assert resp.status_code == 200
        assert resp.json()["name"] == "New"

    def test_delete_namespace(self):
        ns = client.post("/catalog/namespaces", json={"name": "Del"}).json()
        resp = client.delete(f"/catalog/namespaces/{ns['id']}")
        assert resp.status_code == 204

    def test_namespace_tree(self):
        cat = client.post("/catalog/namespaces", json={"name": "TreeCat"}).json()
        client.post(
            "/catalog/namespaces",
            json={"name": "TreeSchema", "parent_id": cat["id"]},
        )
        resp = client.get("/catalog/namespaces/tree")
        assert resp.status_code == 200
        tree = resp.json()
        root = next(n for n in tree if n["name"] == "TreeCat")
        assert len(root["children"]) == 1
        assert root["children"][0]["name"] == "TreeSchema"


# ---------------------------------------------------------------------------
# Flow registration tests
# ---------------------------------------------------------------------------


class TestFlowRegistration:
    def _make_namespace(self) -> int:
        cat = client.post("/catalog/namespaces", json={"name": "FC"}).json()
        schema = client.post(
            "/catalog/namespaces", json={"name": "FS", "parent_id": cat["id"]}
        ).json()
        return schema["id"]

    def test_register_flow(self):
        ns_id = self._make_namespace()
        resp = client.post(
            "/catalog/flows",
            json={
                "name": "my_flow",
                "flow_path": "/tmp/test_flow.yaml",
                "namespace_id": ns_id,
            },
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["name"] == "my_flow"
        assert data["namespace_id"] == ns_id

    def test_list_flows(self):
        ns_id = self._make_namespace()
        client.post(
            "/catalog/flows",
            json={"name": "f1", "flow_path": "/tmp/f1.yaml", "namespace_id": ns_id},
        )
        client.post(
            "/catalog/flows",
            json={"name": "f2", "flow_path": "/tmp/f2.yaml", "namespace_id": ns_id},
        )
        resp = client.get("/catalog/flows", params={"namespace_id": ns_id})
        assert resp.status_code == 200
        assert len(resp.json()) == 2

    def test_get_flow(self):
        ns_id = self._make_namespace()
        created = client.post(
            "/catalog/flows",
            json={"name": "single", "flow_path": "/tmp/single.yaml", "namespace_id": ns_id},
        ).json()
        resp = client.get(f"/catalog/flows/{created['id']}")
        assert resp.status_code == 200
        assert resp.json()["name"] == "single"

    def test_update_flow(self):
        ns_id = self._make_namespace()
        created = client.post(
            "/catalog/flows",
            json={"name": "old", "flow_path": "/tmp/old.yaml", "namespace_id": ns_id},
        ).json()
        resp = client.put(
            f"/catalog/flows/{created['id']}", json={"name": "new_name"}
        )
        assert resp.status_code == 200
        assert resp.json()["name"] == "new_name"

    def test_delete_flow(self):
        ns_id = self._make_namespace()
        created = client.post(
            "/catalog/flows",
            json={"name": "del", "flow_path": "/tmp/del.yaml", "namespace_id": ns_id},
        ).json()
        resp = client.delete(f"/catalog/flows/{created['id']}")
        assert resp.status_code == 204

    def test_flow_file_exists_false_for_missing_path(self):
        ns_id = self._make_namespace()
        created = client.post(
            "/catalog/flows",
            json={
                "name": "ghost",
                "flow_path": "/tmp/nonexistent_flow_xyz.yaml",
                "namespace_id": ns_id,
            },
        ).json()
        resp = client.get(f"/catalog/flows/{created['id']}")
        assert resp.json()["file_exists"] is False


# ---------------------------------------------------------------------------
# Favorites tests
# ---------------------------------------------------------------------------


class TestFavorites:
    def _make_flow(self) -> int:
        cat = client.post("/catalog/namespaces", json={"name": "FavCat"}).json()
        schema = client.post(
            "/catalog/namespaces", json={"name": "FavSch", "parent_id": cat["id"]}
        ).json()
        flow = client.post(
            "/catalog/flows",
            json={"name": "fav_flow", "flow_path": "/tmp/fav.yaml", "namespace_id": schema["id"]},
        ).json()
        return flow["id"]

    def test_add_favorite(self):
        fid = self._make_flow()
        resp = client.post(f"/catalog/flows/{fid}/favorite")
        assert resp.status_code == 201

    def test_list_favorites(self):
        fid = self._make_flow()
        client.post(f"/catalog/flows/{fid}/favorite")
        resp = client.get("/catalog/favorites")
        assert resp.status_code == 200
        assert any(f["id"] == fid for f in resp.json())

    def test_remove_favorite(self):
        fid = self._make_flow()
        client.post(f"/catalog/flows/{fid}/favorite")
        resp = client.delete(f"/catalog/flows/{fid}/favorite")
        assert resp.status_code == 204
        favs = client.get("/catalog/favorites").json()
        assert not any(f["id"] == fid for f in favs)

    def test_favorite_idempotent(self):
        fid = self._make_flow()
        client.post(f"/catalog/flows/{fid}/favorite")
        resp = client.post(f"/catalog/flows/{fid}/favorite")
        # Should not error, returns existing
        assert resp.status_code == 201


# ---------------------------------------------------------------------------
# Follows tests
# ---------------------------------------------------------------------------


class TestFollows:
    def _make_flow(self) -> int:
        cat = client.post("/catalog/namespaces", json={"name": "FolCat"}).json()
        schema = client.post(
            "/catalog/namespaces", json={"name": "FolSch", "parent_id": cat["id"]}
        ).json()
        flow = client.post(
            "/catalog/flows",
            json={"name": "fol_flow", "flow_path": "/tmp/fol.yaml", "namespace_id": schema["id"]},
        ).json()
        return flow["id"]

    def test_add_follow(self):
        fid = self._make_flow()
        resp = client.post(f"/catalog/flows/{fid}/follow")
        assert resp.status_code == 201

    def test_list_following(self):
        fid = self._make_flow()
        client.post(f"/catalog/flows/{fid}/follow")
        resp = client.get("/catalog/following")
        assert resp.status_code == 200
        assert any(f["id"] == fid for f in resp.json())

    def test_remove_follow(self):
        fid = self._make_flow()
        client.post(f"/catalog/flows/{fid}/follow")
        resp = client.delete(f"/catalog/flows/{fid}/follow")
        assert resp.status_code == 204


# ---------------------------------------------------------------------------
# Runs tests
# ---------------------------------------------------------------------------


class TestRuns:
    def test_list_runs_empty(self):
        resp = client.get("/catalog/runs")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_get_run_not_found(self):
        resp = client.get("/catalog/runs/999999")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Stats tests
# ---------------------------------------------------------------------------


class TestStats:
    def test_stats_returns_data(self):
        resp = client.get("/catalog/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert "total_namespaces" in data
        assert "total_flows" in data
        assert "total_runs" in data
        assert "total_favorites" in data
        assert "recent_runs" in data

    def test_stats_counts_only_catalogs(self):
        cat = client.post("/catalog/namespaces", json={"name": "StatCat"}).json()
        client.post(
            "/catalog/namespaces", json={"name": "StatSch", "parent_id": cat["id"]}
        )
        resp = client.get("/catalog/stats")
        # Only top-level catalogs should be counted
        assert resp.json()["total_namespaces"] == 1


# ---------------------------------------------------------------------------
# Default namespace tests
# ---------------------------------------------------------------------------


class TestDefaultNamespace:
    def test_default_namespace_after_init(self):
        """After init_db the General > user_flows namespace should exist."""
        init_db()
        resp = client.get("/catalog/default-namespace-id")
        assert resp.status_code == 200
        ns_id = resp.json()
        assert isinstance(ns_id, int)

        # Verify it points to user_flows under General
        with get_db_context() as db:
            ns = db.get(CatalogNamespace, ns_id)
            assert ns is not None
            assert ns.name == "user_flows"
            assert ns.level == 1
            parent = db.get(CatalogNamespace, ns.parent_id)
            assert parent is not None
            assert parent.name == "General"

    def test_default_namespace_idempotent(self):
        """Calling init_db twice should not duplicate the default namespace."""
        init_db()
        init_db()
        with get_db_context() as db:
            generals = (
                db.query(CatalogNamespace)
                .filter_by(name="General", parent_id=None)
                .all()
            )
            assert len(generals) == 1


# ---------------------------------------------------------------------------
# Auto-registration tests
# ---------------------------------------------------------------------------


class TestAutoRegisterFlow:
    """Tests for _auto_register_flow which registers flows in the default namespace."""

    @staticmethod
    def _ensure_default_namespace():
        init_db()

    @staticmethod
    def _get_local_user_id() -> int:
        with get_db_context() as db:
            user = db.query(User).filter_by(username="local_user").first()
            assert user is not None
            return user.id

    def test_registers_flow_in_default_namespace(self):
        """A new flow path is registered under General > user_flows."""
        self._ensure_default_namespace()
        user_id = self._get_local_user_id()

        _auto_register_flow("/tmp/auto_reg_test.yaml", "auto_flow", user_id)

        with get_db_context() as db:
            reg = db.query(FlowRegistration).filter_by(
                flow_path="/tmp/auto_reg_test.yaml"
            ).first()
            assert reg is not None
            assert reg.name == "auto_flow"
            assert reg.owner_id == user_id
            ns = db.get(CatalogNamespace, reg.namespace_id)
            assert ns is not None
            assert ns.name == "user_flows"

    def test_skips_duplicate_flow_path(self):
        """Calling twice with the same flow_path should not create a duplicate."""
        self._ensure_default_namespace()
        user_id = self._get_local_user_id()

        _auto_register_flow("/tmp/dup_auto.yaml", "first", user_id)
        _auto_register_flow("/tmp/dup_auto.yaml", "second", user_id)

        with get_db_context() as db:
            regs = db.query(FlowRegistration).filter_by(
                flow_path="/tmp/dup_auto.yaml"
            ).all()
            assert len(regs) == 1
            assert regs[0].name == "first"

    def test_skips_when_user_id_is_none(self):
        """Should return early without creating anything when user_id is None."""
        self._ensure_default_namespace()

        _auto_register_flow("/tmp/no_user.yaml", "no_user_flow", None)

        with get_db_context() as db:
            reg = db.query(FlowRegistration).filter_by(
                flow_path="/tmp/no_user.yaml"
            ).first()
            assert reg is None

    def test_skips_when_flow_path_is_none(self):
        """Should return early without creating anything when flow_path is None."""
        self._ensure_default_namespace()
        user_id = self._get_local_user_id()

        _auto_register_flow(None, "no_path", user_id)

        with get_db_context() as db:
            reg = db.query(FlowRegistration).filter_by(name="no_path").first()
            assert reg is None

    def test_skips_when_no_default_namespace(self):
        """Should silently do nothing when the default namespace doesn't exist."""
        user_id = self._get_local_user_id()

        _auto_register_flow("/tmp/no_ns.yaml", "orphan", user_id)

        with get_db_context() as db:
            reg = db.query(FlowRegistration).filter_by(
                flow_path="/tmp/no_ns.yaml"
            ).first()
            assert reg is None

    def test_uses_filename_stem_when_name_is_empty(self):
        """When name is falsy, should fall back to the filename stem."""
        self._ensure_default_namespace()
        user_id = self._get_local_user_id()

        _auto_register_flow("/tmp/my_pipeline.yaml", "", user_id)

        with get_db_context() as db:
            reg = db.query(FlowRegistration).filter_by(
                flow_path="/tmp/my_pipeline.yaml"
            ).first()
            assert reg is not None
            assert reg.name == "my_pipeline"
