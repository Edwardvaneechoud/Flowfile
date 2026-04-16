"""Tests for the Catalog API endpoints.

Covers namespace CRUD, flow registration, favorites, follows,
run history, stats, and the default namespace seeding.
"""

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.catalog import CatalogService
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.init_db import init_db
from flowfile_core.database.models import (
    CatalogNamespace,
    CatalogTable,
    CatalogTableReadLink,
    FlowFavorite,
    FlowFollow,
    FlowRegistration,
    FlowRun,
    FlowSchedule,
    ScheduleTriggerTable,
    User,
)
from flowfile_core.flowfile.catalog_helpers import auto_register_flow

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
        db.query(ScheduleTriggerTable).delete()
        db.query(FlowSchedule).delete()
        db.query(CatalogTableReadLink).delete()
        db.query(CatalogTable).delete()
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
        schema = client.post("/catalog/namespaces", json={"name": "S", "parent_id": cat["id"]}).json()
        resp = client.post("/catalog/namespaces", json={"name": "Deep", "parent_id": schema["id"]})
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
        resp = client.put(f"/catalog/namespaces/{ns['id']}", json={"name": "New"})
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
        schema = client.post("/catalog/namespaces", json={"name": "FS", "parent_id": cat["id"]}).json()
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
        resp = client.put(f"/catalog/flows/{created['id']}", json={"name": "new_name"})
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


class TestCatalogTableMaterialization:
    def test_register_table_uses_worker_metadata(self, monkeypatch):
        with get_db_context() as db:
            ns = CatalogNamespace(name="Cat", level=0, owner_id=1)
            db.add(ns)
            db.commit()
            db.refresh(ns)

            schema = CatalogNamespace(name="Schema", level=1, parent_id=ns.id, owner_id=1)
            db.add(schema)
            db.commit()
            db.refresh(schema)

            repo = SQLAlchemyCatalogRepository(db)
            service = CatalogService(repo)

            response_payload = {
                "parquet_path": "/tmp/fake.parquet",
                "schema": [{"name": "col_a", "dtype": "Int64"}],
                "row_count": 12,
                "column_count": 1,
                "size_bytes": 2048,
            }

            class FakeResponse:
                ok = True
                status_code = 200
                text = ""

                def json(self):
                    return response_payload

            def fake_trigger(*args, **kwargs):
                return FakeResponse()

            monkeypatch.setattr(
                "flowfile_core.catalog.service.trigger_catalog_materialize",
                fake_trigger,
            )

            table_out = service.register_table(
                name="test_table",
                file_path="/tmp/source.xlsx",
                owner_id=1,
                namespace_id=schema.id,
            )

            assert table_out.row_count == response_payload["row_count"]
            assert table_out.column_count == response_payload["column_count"]
            assert table_out.size_bytes == response_payload["size_bytes"]
            assert table_out.schema_columns[0].name == "col_a"
            assert table_out.schema_columns[0].dtype == "Int64"


# ---------------------------------------------------------------------------
# Favorites tests
# ---------------------------------------------------------------------------


class TestFavorites:
    def _make_flow(self) -> int:
        cat = client.post("/catalog/namespaces", json={"name": "FavCat"}).json()
        schema = client.post("/catalog/namespaces", json={"name": "FavSch", "parent_id": cat["id"]}).json()
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
        schema = client.post("/catalog/namespaces", json={"name": "FolSch", "parent_id": cat["id"]}).json()
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
        data = resp.json()
        assert "items" in data
        assert "total" in data
        assert isinstance(data["items"], list)
        assert data["total"] == 0

    def test_get_run_not_found(self):
        resp = client.get("/catalog/runs/999999")
        assert resp.status_code == 404

    def test_list_runs_pagination(self):
        """Create multiple runs and verify pagination works."""
        from datetime import datetime, timezone

        cat = client.post("/catalog/namespaces", json={"name": "RunCat"}).json()
        schema = client.post(
            "/catalog/namespaces", json={"name": "RunSch", "parent_id": cat["id"]}
        ).json()
        flow = client.post(
            "/catalog/flows",
            json={"name": "run_flow", "flow_path": "/tmp/run_flow.yaml", "namespace_id": schema["id"]},
        ).json()

        # Create 5 runs directly in DB
        with get_db_context() as db:
            for i in range(5):
                run = FlowRun(
                    registration_id=flow["id"],
                    flow_name="run_flow",
                    flow_path="/tmp/run_flow.yaml",
                    user_id=1,
                    started_at=datetime.now(timezone.utc),
                    number_of_nodes=3,
                    run_type="in_designer_run",
                )
                db.add(run)
            db.commit()

        # Page 1: limit=2, offset=0
        resp = client.get("/catalog/runs", params={"limit": 2, "offset": 0})
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["items"]) == 2
        assert data["total"] == 5

        # Page 2: limit=2, offset=2
        resp = client.get("/catalog/runs", params={"limit": 2, "offset": 2})
        data = resp.json()
        assert len(data["items"]) == 2
        assert data["total"] == 5

        # Page 3: limit=2, offset=4
        resp = client.get("/catalog/runs", params={"limit": 2, "offset": 4})
        data = resp.json()
        assert len(data["items"]) == 1
        assert data["total"] == 5

        # Filter by registration_id
        resp = client.get("/catalog/runs", params={"registration_id": flow["id"]})
        data = resp.json()
        assert data["total"] == 5

        # Filter by non-existent registration_id
        resp = client.get("/catalog/runs", params={"registration_id": 99999})
        data = resp.json()
        assert data["total"] == 0
        assert len(data["items"]) == 0


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
        assert "total_virtual_tables" in data
        assert "recent_runs" in data

    def test_stats_counts_only_catalogs(self):
        cat = client.post("/catalog/namespaces", json={"name": "StatCat"}).json()
        client.post("/catalog/namespaces", json={"name": "StatSch", "parent_id": cat["id"]})
        resp = client.get("/catalog/stats")
        # Only top-level catalogs should be counted
        assert resp.json()["total_namespaces"] == 1


# ---------------------------------------------------------------------------
# Default namespace tests
# ---------------------------------------------------------------------------


class TestDefaultNamespace:
    def test_default_namespace_after_init(self):
        """After init_db the General > default namespace should exist."""
        init_db()
        resp = client.get("/catalog/default-namespace-id")
        assert resp.status_code == 200
        ns_id = resp.json()
        assert isinstance(ns_id, int)

        # Verify it points to default under General
        with get_db_context() as db:
            ns = db.get(CatalogNamespace, ns_id)
            assert ns is not None
            assert ns.name == "default"
            assert ns.level == 1
            parent = db.get(CatalogNamespace, ns.parent_id)
            assert parent is not None
            assert parent.name == "General"

    def test_default_namespace_idempotent(self):
        """Calling init_db twice should not duplicate the default namespace."""
        init_db()
        init_db()
        with get_db_context() as db:
            generals = db.query(CatalogNamespace).filter_by(name="General", parent_id=None).all()
            assert len(generals) == 1

    def test_local_flows_namespace_created_on_init(self):
        """init_db should seed General > Local Flows for disk-backed flow registration."""
        init_db()
        with get_db_context() as db:
            general = db.query(CatalogNamespace).filter_by(name="General", parent_id=None).first()
            assert general is not None
            local_flows = (
                db.query(CatalogNamespace).filter_by(name="Local Flows", parent_id=general.id).first()
            )
            assert local_flows is not None
            assert local_flows.level == 1

    def test_local_flows_namespace_idempotent(self):
        """Running init_db twice shouldn't duplicate the Local Flows namespace."""
        init_db()
        init_db()
        with get_db_context() as db:
            general = db.query(CatalogNamespace).filter_by(name="General", parent_id=None).first()
            local_flows_rows = (
                db.query(CatalogNamespace).filter_by(name="Local Flows", parent_id=general.id).all()
            )
            assert len(local_flows_rows) == 1

    def test_ensure_local_flows_namespace_creates_when_missing(self):
        """ensure_local_flows_namespace() self-heals a missing namespace."""
        init_db()
        with get_db_context() as db:
            general = db.query(CatalogNamespace).filter_by(name="General", parent_id=None).first()
            # Delete the Local Flows namespace to simulate an older DB
            local_flows = (
                db.query(CatalogNamespace).filter_by(name="Local Flows", parent_id=general.id).first()
            )
            if local_flows is not None:
                db.delete(local_flows)
                db.commit()

            service = CatalogService(SQLAlchemyCatalogRepository(db))
            ns = service.ensure_local_flows_namespace()
            assert ns is not None
            assert ns.name == "Local Flows"
            assert ns.parent_id == general.id
            assert ns.owner_id == general.owner_id


# ---------------------------------------------------------------------------
# Auto-registration tests
# ---------------------------------------------------------------------------


class TestAutoRegisterFlow:
    """Tests for auto_register_flow which registers flows in the default namespace."""

    @staticmethod
    def _ensure_default_namespace():
        init_db()

    @staticmethod
    def _get_local_user_id() -> int:
        with get_db_context() as db:
            user = db.query(User).filter_by(username="local_user").first()
            assert user is not None
            return user.id

    def test_registers_flow_in_local_flows_namespace(self):
        """Disk-backed flows land under General > Local Flows by default."""
        self._ensure_default_namespace()
        user_id = self._get_local_user_id()

        auto_register_flow("/tmp/auto_reg_test.yaml", "auto_flow", user_id)

        with get_db_context() as db:
            reg = db.query(FlowRegistration).filter_by(flow_path="/tmp/auto_reg_test.yaml").first()
            assert reg is not None
            assert reg.name == "auto_flow"
            assert reg.owner_id == user_id
            ns = db.get(CatalogNamespace, reg.namespace_id)
            assert ns is not None
            assert ns.name == "Local Flows"

    def test_registers_unnamed_flow_in_unnamed_namespace(self):
        """Quick-created flows (under unnamed_flows/) land in General > Unnamed Flows."""
        self._ensure_default_namespace()
        user_id = self._get_local_user_id()

        auto_register_flow("/tmp/unnamed_flows/quick_flow.yaml", "quick_flow", user_id)

        with get_db_context() as db:
            reg = (
                db.query(FlowRegistration)
                .filter_by(flow_path="/tmp/unnamed_flows/quick_flow.yaml")
                .first()
            )
            assert reg is not None
            ns = db.get(CatalogNamespace, reg.namespace_id)
            assert ns is not None
            assert ns.name == "Unnamed Flows"

    def test_falls_back_to_default_when_local_flows_missing(self):
        """If the Local Flows namespace is missing (older DBs), fall back to default."""
        self._ensure_default_namespace()
        user_id = self._get_local_user_id()

        # Remove the Local Flows namespace to simulate a pre-existing DB that
        # predates this change (and prevent auto-create via ensure_*).
        with get_db_context() as db:
            general = db.query(CatalogNamespace).filter_by(name="General", parent_id=None).first()
            local_flows = (
                db.query(CatalogNamespace)
                .filter_by(name="Local Flows", parent_id=general.id)
                .first()
            )
            if local_flows is not None:
                db.delete(local_flows)
                db.commit()

        # Patch ensure_local_flows_namespace to return None to simulate the
        # older "default-only" catalog state.
        original_ensure = CatalogService.ensure_local_flows_namespace
        CatalogService.ensure_local_flows_namespace = lambda self: None
        try:
            auto_register_flow("/tmp/fallback_flow.yaml", "fallback_flow", user_id)
        finally:
            CatalogService.ensure_local_flows_namespace = original_ensure

        with get_db_context() as db:
            reg = db.query(FlowRegistration).filter_by(flow_path="/tmp/fallback_flow.yaml").first()
            assert reg is not None
            ns = db.get(CatalogNamespace, reg.namespace_id)
            assert ns is not None
            assert ns.name == "default"

    def test_skips_duplicate_flow_path(self):
        """Calling twice with the same flow_path should not create a duplicate."""
        self._ensure_default_namespace()
        user_id = self._get_local_user_id()

        auto_register_flow("/tmp/dup_auto.yaml", "first", user_id)
        auto_register_flow("/tmp/dup_auto.yaml", "second", user_id)

        with get_db_context() as db:
            regs = db.query(FlowRegistration).filter_by(flow_path="/tmp/dup_auto.yaml").all()
            assert len(regs) == 1
            assert regs[0].name == "first"

    def test_skips_when_user_id_is_none(self):
        """Should return early without creating anything when user_id is None."""
        self._ensure_default_namespace()

        auto_register_flow("/tmp/no_user.yaml", "no_user_flow", None)

        with get_db_context() as db:
            reg = db.query(FlowRegistration).filter_by(flow_path="/tmp/no_user.yaml").first()
            assert reg is None

    def test_skips_when_flow_path_is_none(self):
        """Should return early without creating anything when flow_path is None."""
        self._ensure_default_namespace()
        user_id = self._get_local_user_id()

        auto_register_flow(None, "no_path", user_id)

        with get_db_context() as db:
            reg = db.query(FlowRegistration).filter_by(name="no_path").first()
            assert reg is None

    def test_skips_when_no_default_namespace(self):
        """Should silently do nothing when the default namespace doesn't exist."""
        user_id = self._get_local_user_id()

        auto_register_flow("/tmp/no_ns.yaml", "orphan", user_id)

        with get_db_context() as db:
            reg = db.query(FlowRegistration).filter_by(flow_path="/tmp/no_ns.yaml").first()
            assert reg is None

    def test_uses_filename_stem_when_name_is_empty(self):
        """When name is falsy, should fall back to the filename stem."""
        self._ensure_default_namespace()
        user_id = self._get_local_user_id()

        auto_register_flow("/tmp/my_pipeline.yaml", "", user_id)

        with get_db_context() as db:
            reg = db.query(FlowRegistration).filter_by(flow_path="/tmp/my_pipeline.yaml").first()
            assert reg is not None
            assert reg.name == "my_pipeline"


# ---------------------------------------------------------------------------
# Read link / lineage repository tests
# ---------------------------------------------------------------------------


class TestReadLinks:
    """Tests for CatalogTableReadLink upsert and query methods."""

    @staticmethod
    def _setup_flow_and_table():
        """Create a namespace, flow registration, and catalog table for testing."""
        with get_db_context() as db:
            ns = CatalogNamespace(name="LinkCat", level=0, owner_id=1)
            db.add(ns)
            db.commit()
            db.refresh(ns)

            schema = CatalogNamespace(name="LinkSch", level=1, parent_id=ns.id, owner_id=1)
            db.add(schema)
            db.commit()
            db.refresh(schema)

            flow = FlowRegistration(
                name="reader_flow",
                flow_path="/tmp/reader.yaml",
                namespace_id=schema.id,
                owner_id=1,
            )
            db.add(flow)
            db.commit()
            db.refresh(flow)

            table = CatalogTable(
                name="test_table",
                namespace_id=schema.id,
                owner_id=1,
                file_path="/tmp/fake.parquet",
            )
            db.add(table)
            db.commit()
            db.refresh(table)

            return flow.id, table.id

    def test_upsert_read_link_creates_record(self):
        flow_id, table_id = self._setup_flow_and_table()
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            repo.upsert_read_link(table_id, flow_id)

        with get_db_context() as db:
            link = db.query(CatalogTableReadLink).filter_by(table_id=table_id, registration_id=flow_id).first()
            assert link is not None

    def test_upsert_read_link_is_idempotent(self):
        flow_id, table_id = self._setup_flow_and_table()
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            repo.upsert_read_link(table_id, flow_id)
            repo.upsert_read_link(table_id, flow_id)

        with get_db_context() as db:
            links = db.query(CatalogTableReadLink).filter_by(table_id=table_id, registration_id=flow_id).all()
            assert len(links) == 1

    def test_list_readers_for_table(self):
        flow_id, table_id = self._setup_flow_and_table()
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            repo.upsert_read_link(table_id, flow_id)
            readers = repo.list_readers_for_table(table_id)
            assert len(readers) == 1
            assert readers[0].id == flow_id

    def test_list_readers_for_table_empty(self):
        _, table_id = self._setup_flow_and_table()
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            readers = repo.list_readers_for_table(table_id)
            assert readers == []

    def test_list_read_tables_for_flow(self):
        flow_id, table_id = self._setup_flow_and_table()
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            repo.upsert_read_link(table_id, flow_id)
            tables = repo.list_read_tables_for_flow(flow_id)
            assert len(tables) == 1
            assert tables[0].id == table_id

    def test_list_read_tables_for_flow_empty(self):
        flow_id, _ = self._setup_flow_and_table()
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            tables = repo.list_read_tables_for_flow(flow_id)
            assert tables == []

    def test_multiple_flows_read_same_table(self):
        """Two flows reading the same table should both appear as readers."""
        flow_id_1, table_id = self._setup_flow_and_table()
        with get_db_context() as db:
            flow2 = FlowRegistration(
                name="second_reader",
                flow_path="/tmp/reader2.yaml",
                namespace_id=db.get(FlowRegistration, flow_id_1).namespace_id,
                owner_id=1,
            )
            db.add(flow2)
            db.commit()
            db.refresh(flow2)
            flow_id_2 = flow2.id

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            repo.upsert_read_link(table_id, flow_id_1)
            repo.upsert_read_link(table_id, flow_id_2)
            readers = repo.list_readers_for_table(table_id)
            reader_ids = {r.id for r in readers}
            assert reader_ids == {flow_id_1, flow_id_2}


# ---------------------------------------------------------------------------
# Table lineage tests (source_registration_id on CatalogTable)
# ---------------------------------------------------------------------------


class TestTableLineage:
    """Tests for source_registration_id tracking on catalog tables."""

    @staticmethod
    def _make_namespace_and_flow():
        """Create namespace hierarchy and a flow registration."""
        with get_db_context() as db:
            cat = CatalogNamespace(name="LinCat", level=0, owner_id=1)
            db.add(cat)
            db.commit()
            db.refresh(cat)

            schema = CatalogNamespace(name="LinSch", level=1, parent_id=cat.id, owner_id=1)
            db.add(schema)
            db.commit()
            db.refresh(schema)

            flow = FlowRegistration(
                name="producer_flow",
                flow_path="/tmp/producer.yaml",
                namespace_id=schema.id,
                owner_id=1,
            )
            db.add(flow)
            db.commit()
            db.refresh(flow)
            return schema.id, flow.id

    def test_table_stores_source_registration_id(self):
        schema_id, flow_id = self._make_namespace_and_flow()
        with get_db_context() as db:
            table = CatalogTable(
                name="produced_table",
                namespace_id=schema_id,
                owner_id=1,
                file_path="/tmp/produced.parquet",
                source_registration_id=flow_id,
            )
            db.add(table)
            db.commit()
            db.refresh(table)

            assert table.source_registration_id == flow_id

    def test_table_source_registration_id_nullable(self):
        schema_id, _ = self._make_namespace_and_flow()
        with get_db_context() as db:
            table = CatalogTable(
                name="orphan_table",
                namespace_id=schema_id,
                owner_id=1,
                file_path="/tmp/orphan.parquet",
            )
            db.add(table)
            db.commit()
            db.refresh(table)

            assert table.source_registration_id is None

    def test_list_tables_for_flow(self):
        schema_id, flow_id = self._make_namespace_and_flow()
        with get_db_context() as db:
            for i in range(3):
                db.add(
                    CatalogTable(
                        name=f"t_{i}",
                        namespace_id=schema_id,
                        owner_id=1,
                        file_path=f"/tmp/t_{i}.parquet",
                        source_registration_id=flow_id,
                    )
                )
            db.commit()

            repo = SQLAlchemyCatalogRepository(db)
            tables = repo.list_tables_for_flow(flow_id)
            assert len(tables) == 3

    def test_bulk_get_tables_for_flows(self):
        schema_id, flow_id = self._make_namespace_and_flow()
        with get_db_context() as db:
            # Create a second flow
            flow2 = FlowRegistration(
                name="producer2",
                flow_path="/tmp/producer2.yaml",
                namespace_id=schema_id,
                owner_id=1,
            )
            db.add(flow2)
            db.commit()
            db.refresh(flow2)

            db.add(
                CatalogTable(
                    name="t_a",
                    namespace_id=schema_id,
                    owner_id=1,
                    file_path="/tmp/t_a.parquet",
                    source_registration_id=flow_id,
                )
            )
            db.add(
                CatalogTable(
                    name="t_b",
                    namespace_id=schema_id,
                    owner_id=1,
                    file_path="/tmp/t_b.parquet",
                    source_registration_id=flow2.id,
                )
            )
            db.commit()

            repo = SQLAlchemyCatalogRepository(db)
            result = repo.bulk_get_tables_for_flows([flow_id, flow2.id])
            assert len(result[flow_id]) == 1
            assert result[flow_id][0].name == "t_a"
            assert len(result[flow2.id]) == 1
            assert result[flow2.id][0].name == "t_b"

    def test_bulk_get_tables_for_flows_empty_input(self):
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            result = repo.bulk_get_tables_for_flows([])
            assert result == {}


# ---------------------------------------------------------------------------
# Service-level lineage enrichment tests
# ---------------------------------------------------------------------------


class TestServiceLineageEnrichment:
    """Tests that CatalogService enriches outputs with lineage data."""

    @staticmethod
    def _setup():
        """Create namespace + flow + table with read link."""
        with get_db_context() as db:
            cat = CatalogNamespace(name="SvcCat", level=0, owner_id=1)
            db.add(cat)
            db.commit()
            db.refresh(cat)

            schema = CatalogNamespace(name="SvcSch", level=1, parent_id=cat.id, owner_id=1)
            db.add(schema)
            db.commit()
            db.refresh(schema)

            producer = FlowRegistration(
                name="the_producer",
                flow_path="/tmp/the_producer.yaml",
                namespace_id=schema.id,
                owner_id=1,
            )
            db.add(producer)
            db.commit()
            db.refresh(producer)

            reader = FlowRegistration(
                name="the_reader",
                flow_path="/tmp/the_reader.yaml",
                namespace_id=schema.id,
                owner_id=1,
            )
            db.add(reader)
            db.commit()
            db.refresh(reader)

            table = CatalogTable(
                name="lineage_table",
                namespace_id=schema.id,
                owner_id=1,
                file_path="/tmp/lineage.parquet",
                source_registration_id=producer.id,
            )
            db.add(table)
            db.commit()
            db.refresh(table)

            repo = SQLAlchemyCatalogRepository(db)
            repo.upsert_read_link(table.id, reader.id)

            return schema.id, producer.id, reader.id, table.id

    def test_table_out_includes_source_registration_name(self):
        from flowfile_core.catalog.service import CatalogService

        _, producer_id, _, table_id = self._setup()
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            table_out = svc.get_table(table_id)

        assert table_out.source_registration_id == producer_id
        assert table_out.source_registration_name == "the_producer"

    def test_table_out_includes_read_by_flows(self):
        from flowfile_core.catalog.service import CatalogService

        _, _, reader_id, table_id = self._setup()
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            table_out = svc.get_table(table_id)

        assert len(table_out.read_by_flows) == 1
        assert table_out.read_by_flows[0].id == reader_id
        assert table_out.read_by_flows[0].name == "the_reader"

    def test_flow_out_includes_tables_produced(self):
        from flowfile_core.catalog.service import CatalogService

        _, producer_id, _, table_id = self._setup()
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            flow_out = svc.get_flow(producer_id, user_id=1)

        assert len(flow_out.tables_produced) >= 1
        table_ids = [t.id for t in flow_out.tables_produced]
        assert table_id in table_ids


# ---------------------------------------------------------------------------
# Push-driven table trigger tests
# ---------------------------------------------------------------------------


class TestPushTableTrigger:
    """Tests for push-driven table_trigger schedule firing on overwrite_table_data."""

    @staticmethod
    def _setup_table_and_schedule(schedule_type="table_trigger"):
        """Create namespace, flow, table, and schedule. Returns (schema_id, flow_id, table_id, schedule_id)."""
        import tempfile

        import polars as pl

        # Write a real parquet file so overwrite_table_data can read it
        tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        pl.DataFrame({"a": [1, 2, 3]}).write_parquet(tmp.name)

        with get_db_context() as db:
            cat = CatalogNamespace(name="PushCat", level=0, owner_id=1)
            db.add(cat)
            db.commit()
            db.refresh(cat)

            schema = CatalogNamespace(name="PushSch", level=1, parent_id=cat.id, owner_id=1)
            db.add(schema)
            db.commit()
            db.refresh(schema)

            flow = FlowRegistration(
                name="triggered_flow",
                flow_path="/tmp/triggered.yaml",
                namespace_id=schema.id,
                owner_id=1,
            )
            db.add(flow)
            db.commit()
            db.refresh(flow)

            table = CatalogTable(
                name="watched_table",
                namespace_id=schema.id,
                owner_id=1,
                file_path=tmp.name,
            )
            db.add(table)
            db.commit()
            db.refresh(table)

            schedule = FlowSchedule(
                registration_id=flow.id,
                owner_id=1,
                enabled=True,
                schedule_type=schedule_type,
                trigger_table_id=table.id if schedule_type == "table_trigger" else None,
            )
            db.add(schedule)
            db.commit()
            db.refresh(schedule)

            return schema.id, flow.id, table.id, schedule.id, tmp.name

    def test_overwrite_fires_push_trigger(self, monkeypatch):
        """Overwriting a table should create a FlowRun for its table_trigger schedule."""
        schema_id, flow_id, table_id, schedule_id, parquet_path = self._setup_table_and_schedule()

        monkeypatch.setattr(CatalogService, "_spawn_flow_subprocess", staticmethod(lambda *a, **kw: None))

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            svc.overwrite_table_data(table_id, parquet_path)

            runs = db.query(FlowRun).filter_by(registration_id=flow_id, run_type="scheduled").all()
            assert len(runs) == 1

    def test_push_trigger_skips_active_run(self, monkeypatch):
        """If the flow already has an active run, no new FlowRun should be created."""
        from datetime import datetime, timezone

        schema_id, flow_id, table_id, schedule_id, parquet_path = self._setup_table_and_schedule()

        monkeypatch.setattr(CatalogService, "_spawn_flow_subprocess", staticmethod(lambda *a, **kw: None))

        # Create an active (unfinished) run
        with get_db_context() as db:
            active_run = FlowRun(
                registration_id=flow_id,
                flow_name="triggered_flow",
                flow_path="/tmp/triggered.yaml",
                user_id=1,
                started_at=datetime.now(timezone.utc),
                number_of_nodes=0,
                run_type="scheduled",
            )
            db.add(active_run)
            db.commit()

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            svc.overwrite_table_data(table_id, parquet_path)

            runs = db.query(FlowRun).filter_by(registration_id=flow_id, run_type="scheduled").all()
            # Only the pre-existing active run, no new one
            assert len(runs) == 1

    def test_push_trigger_updates_schedule_timestamps(self, monkeypatch):
        """After firing, the schedule's last_triggered_at and last_trigger_table_updated_at should be set."""
        schema_id, flow_id, table_id, schedule_id, parquet_path = self._setup_table_and_schedule()

        monkeypatch.setattr(CatalogService, "_spawn_flow_subprocess", staticmethod(lambda *a, **kw: None))

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            svc.overwrite_table_data(table_id, parquet_path)

            schedule = db.query(FlowSchedule).get(schedule_id)
            assert schedule.last_triggered_at is not None
            assert schedule.last_trigger_table_updated_at is not None

    def test_push_trigger_ignores_table_set_triggers(self, monkeypatch):
        """table_set_trigger schedules should NOT be fired by the push mechanism."""
        schema_id, flow_id, table_id, schedule_id, parquet_path = self._setup_table_and_schedule(
            schedule_type="table_set_trigger"
        )

        monkeypatch.setattr(CatalogService, "_spawn_flow_subprocess", staticmethod(lambda *a, **kw: None))

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            svc.overwrite_table_data(table_id, parquet_path)

            runs = db.query(FlowRun).filter_by(registration_id=flow_id, run_type="scheduled").all()
            assert len(runs) == 0
