"""Tests for the Catalog API endpoints.

Covers namespace CRUD, flow registration, favorites, follows,
run history, stats, and the default namespace seeding.
"""

from pathlib import Path

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
from flowfile_core.routes.routes import flow_file_handler
from shared.storage_config import storage

# Helpers


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


# Fixtures


@pytest.fixture(autouse=True)
def clean_catalog():
    """Ensure a clean catalog state for every test."""
    _cleanup_catalog()
    yield
    _cleanup_catalog()


# Namespace tests


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


# Flow registration tests


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

    def test_delete_flow_removes_file_when_requested(self, tmp_path):
        ns_id = self._make_namespace()
        flow_file = tmp_path / "to_delete.yaml"
        flow_file.write_text("flow: {}")
        created = client.post(
            "/catalog/flows",
            json={"name": "df", "flow_path": str(flow_file), "namespace_id": ns_id},
        ).json()
        resp = client.delete(f"/catalog/flows/{created['id']}", params={"delete_file": True})
        assert resp.status_code == 204
        assert not flow_file.exists()

    def test_delete_flow_keeps_file_by_default(self, tmp_path):
        ns_id = self._make_namespace()
        flow_file = tmp_path / "keep.yaml"
        flow_file.write_text("flow: {}")
        created = client.post(
            "/catalog/flows",
            json={"name": "kf", "flow_path": str(flow_file), "namespace_id": ns_id},
        ).json()
        resp = client.delete(f"/catalog/flows/{created['id']}")
        assert resp.status_code == 204
        assert flow_file.exists()

    def test_delete_flow_keeps_file_while_another_registration_points_at_it(self, tmp_path):
        ns_id = self._make_namespace()
        flow_file = tmp_path / "shared.yaml"
        flow_file.write_text("flow: {}")
        a = client.post(
            "/catalog/flows",
            json={"name": "a", "flow_path": str(flow_file), "namespace_id": ns_id},
        ).json()
        b = client.post(
            "/catalog/flows",
            json={"name": "b", "flow_path": str(flow_file), "namespace_id": ns_id},
        ).json()
        # Deleting one registration must not delete the file while another still points at it.
        client.delete(f"/catalog/flows/{a['id']}", params={"delete_file": True})
        assert flow_file.exists()
        # Deleting the last registration removes the file.
        client.delete(f"/catalog/flows/{b['id']}", params={"delete_file": True})
        assert not flow_file.exists()

    def test_delete_flow_detaches_runs_preserves_uuid(self):
        """Deleting a flow nulls FlowRun.registration_id but preserves flow_uuid.

        The orphaned runs stay in the global Run History (attributed via uuid)
        but never surface under another flow's per-flow page.
        """
        from datetime import datetime, timezone

        ns_id = self._make_namespace()
        flow = client.post(
            "/catalog/flows",
            json={"name": "victim", "flow_path": "/tmp/victim.yaml", "namespace_id": ns_id},
        ).json()

        with get_db_context() as db:
            reg_uuid = db.query(FlowRegistration.flow_uuid).filter_by(id=flow["id"]).scalar()
            assert reg_uuid is not None
            db.add(
                FlowRun(
                    registration_id=flow["id"],
                    flow_uuid=reg_uuid,
                    flow_name="victim",
                    flow_path="/tmp/victim.yaml",
                    user_id=1,
                    started_at=datetime.now(timezone.utc),
                    number_of_nodes=1,
                    run_type="in_designer_run",
                )
            )
            db.commit()

        client.delete(f"/catalog/flows/{flow['id']}")

        with get_db_context() as db:
            run = db.query(FlowRun).filter_by(flow_uuid=reg_uuid).one()
            assert run.registration_id is None
            assert run.flow_uuid == reg_uuid

    def test_run_history_does_not_leak_after_id_reuse(self):
        """A new flow that happens to take a deleted flow's registration_id
        must not inherit the deleted flow's run history.
        """
        from datetime import datetime, timezone

        ns_id = self._make_namespace()
        original = client.post(
            "/catalog/flows",
            json={"name": "first", "flow_path": "/tmp/first.yaml", "namespace_id": ns_id},
        ).json()
        original_id = original["id"]

        with get_db_context() as db:
            reg_uuid = db.query(FlowRegistration.flow_uuid).filter_by(id=original_id).scalar()
            db.add(
                FlowRun(
                    registration_id=original_id,
                    flow_uuid=reg_uuid,
                    flow_name="first",
                    flow_path="/tmp/first.yaml",
                    user_id=1,
                    started_at=datetime.now(timezone.utc),
                    number_of_nodes=1,
                    run_type="in_designer_run",
                )
            )
            db.commit()

        client.delete(f"/catalog/flows/{original_id}")

        # Force a new registration row to claim the same id (simulating SQLite
        # reusing the freed id, which is what was contaminating the user's
        # per-flow Run History page).
        with get_db_context() as db:
            new_reg = FlowRegistration(
                id=original_id,
                flow_uuid="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                name="reused",
                flow_path="/tmp/reused.yaml",
                namespace_id=ns_id,
                owner_id=1,
            )
            db.add(new_reg)
            db.commit()

        resp = client.get("/catalog/runs", params={"registration_id": original_id})
        assert resp.status_code == 200
        # Must be empty: the orphaned run still has the original flow_uuid,
        # but the new registration has a different flow_uuid.
        assert resp.json()["total"] == 0
        assert resp.json()["items"] == []

    def test_run_history_uses_current_registration_name(self):
        """Run-history list must show the registration's current user-typed name,
        not the file-stem snapshot baked into FlowRun.flow_name at run time.
        """
        from datetime import datetime, timezone

        ns_id = self._make_namespace()
        flow = client.post(
            "/catalog/flows",
            json={"name": "nice_name", "flow_path": "/tmp/nice.yaml", "namespace_id": ns_id},
        ).json()

        with get_db_context() as db:
            reg_uuid = db.query(FlowRegistration.flow_uuid).filter_by(id=flow["id"]).scalar()
            db.add(
                FlowRun(
                    registration_id=flow["id"],
                    flow_uuid=reg_uuid,
                    # Simulate the buggy historical capture: prefixed file stem.
                    flow_name="9_nice_name",
                    flow_path="/tmp/nice.yaml",
                    user_id=1,
                    started_at=datetime.now(timezone.utc),
                    number_of_nodes=1,
                    run_type="in_designer_run",
                )
            )
            db.commit()

        items = client.get("/catalog/runs").json()["items"]
        assert len(items) == 1
        # Display must use the registration's current name, not the snapshot.
        assert items[0]["flow_name"] == "nice_name"

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

    def test_missing_file_warning_is_deduped(self, caplog):
        """First lookup of a missing-file flow logs a warning; subsequent lookups
        within the same process must not re-log it (avoids per-page-load spam)."""
        import logging

        from flowfile_core.catalog.services import flows as flows_module

        ns_id = self._make_namespace()
        created = client.post(
            "/catalog/flows",
            json={
                "name": "ghost_dedupe",
                "flow_path": "/tmp/missing_dedupe_xyz.yaml",
                "namespace_id": ns_id,
            },
        ).json()

        # Reset the process-local memo so this test is order-independent.
        flows_module._warned_missing_paths.clear()
        # Drop any records left over from the POST above; we only want to
        # count what gets emitted by the GETs below.
        caplog.clear()

        with caplog.at_level(logging.WARNING, logger=flows_module.logger.name):
            client.get(f"/catalog/flows/{created['id']}")
            client.get(f"/catalog/flows/{created['id']}")
            client.get(f"/catalog/flows/{created['id']}")

        warnings = [r for r in caplog.records if "references missing file" in r.getMessage()]
        assert len(warnings) == 1, f"expected 1 warning, got {len(warnings)}: {warnings}"


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
                "table_path": "/tmp/fake_delta",
                "column_schema": [{"name": "col_a", "dtype": "Int64"}],
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


# Favorites tests


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
        assert resp.status_code == 201


# Follows tests


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


# Runs tests


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

        # Create 5 runs directly in DB. flow_uuid must be copied from the
        # registration so the per-flow filter (which routes through flow_uuid)
        # finds them.
        with get_db_context() as db:
            reg_uuid = db.query(FlowRegistration.flow_uuid).filter_by(id=flow["id"]).scalar()
            for i in range(5):
                run = FlowRun(
                    registration_id=flow["id"],
                    flow_uuid=reg_uuid,
                    flow_name="run_flow",
                    flow_path="/tmp/run_flow.yaml",
                    user_id=1,
                    started_at=datetime.now(timezone.utc),
                    number_of_nodes=3,
                    run_type="in_designer_run",
                )
                db.add(run)
            db.commit()

        resp = client.get("/catalog/runs", params={"limit": 2, "offset": 0})
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["items"]) == 2
        assert data["total"] == 5

        resp = client.get("/catalog/runs", params={"limit": 2, "offset": 2})
        data = resp.json()
        assert len(data["items"]) == 2
        assert data["total"] == 5

        resp = client.get("/catalog/runs", params={"limit": 2, "offset": 4})
        data = resp.json()
        assert len(data["items"]) == 1
        assert data["total"] == 5

        resp = client.get("/catalog/runs", params={"registration_id": flow["id"]})
        data = resp.json()
        assert data["total"] == 5

        resp = client.get("/catalog/runs", params={"registration_id": 99999})
        data = resp.json()
        assert data["total"] == 0
        assert len(data["items"]) == 0


# Stats tests


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
        assert resp.json()["total_namespaces"] == 1


# Default namespace tests


class TestDefaultNamespace:
    def test_default_namespace_after_init(self):
        """After init_db the General > default namespace should exist."""
        init_db()
        resp = client.get("/catalog/default-namespace-id")
        assert resp.status_code == 200
        ns_id = resp.json()
        assert isinstance(ns_id, int)

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


# Auto-registration tests


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

    def test_falls_back_to_default_when_local_flows_missing(self, monkeypatch):
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
        monkeypatch.setattr(CatalogService, "ensure_local_flows_namespace", lambda self: None)
        auto_register_flow("/tmp/fallback_flow.yaml", "fallback_flow", user_id)

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


# Read link / lineage repository tests


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


# Table lineage tests (source_registration_id on CatalogTable)


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


# Service-level lineage enrichment tests


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


# Push-driven table trigger tests


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

        monkeypatch.setattr(
            "flowfile_core.catalog.services.runs.spawn_flow_subprocess",
            lambda *a, **kw: None,
        )

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

        monkeypatch.setattr(
            "flowfile_core.catalog.services.runs.spawn_flow_subprocess",
            lambda *a, **kw: None,
        )

        # Create an active (unfinished) run. flow_uuid must match the
        # registration so has_active_run (which routes through flow_uuid) finds it.
        with get_db_context() as db:
            reg_uuid = db.query(FlowRegistration.flow_uuid).filter_by(id=flow_id).scalar()
            active_run = FlowRun(
                registration_id=flow_id,
                flow_uuid=reg_uuid,
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
            assert len(runs) == 1

    def test_push_trigger_updates_schedule_timestamps(self, monkeypatch):
        """After firing, the schedule's last_triggered_at and last_trigger_table_updated_at should be set."""
        schema_id, flow_id, table_id, schedule_id, parquet_path = self._setup_table_and_schedule()

        monkeypatch.setattr(
            "flowfile_core.catalog.services.runs.spawn_flow_subprocess",
            lambda *a, **kw: None,
        )

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

        monkeypatch.setattr(
            "flowfile_core.catalog.services.runs.spawn_flow_subprocess",
            lambda *a, **kw: None,
        )

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            svc.overwrite_table_data(table_id, parquet_path)

            runs = db.query(FlowRun).filter_by(registration_id=flow_id, run_type="scheduled").all()
            assert len(runs) == 0


# Cross-namespace table resolution


class TestCrossNamespaceResolution:
    """Two tables with the same name in different namespaces must be addressable
    without collision: via `ns.name`, via `name + default_namespace_id`, or via
    bare `name` (soft-picks + logs a warning; strict mode raises)."""

    def _seed_two_foo_tables(self) -> tuple[int, int, int, int]:
        """Create catalog with two schemas each containing a 'foo' table. Returns
        (catalog_id, schema_a_id, schema_b_id, foo_a_id)."""
        _cleanup_catalog()
        with get_db_context() as db:
            cat = CatalogNamespace(name="cat", level=0, owner_id=1)
            db.add(cat)
            db.commit()
            db.refresh(cat)
            schema_a = CatalogNamespace(name="ns_a", level=1, parent_id=cat.id, owner_id=1)
            schema_b = CatalogNamespace(name="ns_b", level=1, parent_id=cat.id, owner_id=1)
            db.add_all([schema_a, schema_b])
            db.commit()
            db.refresh(schema_a)
            db.refresh(schema_b)

            foo_a = CatalogTable(
                name="foo",
                namespace_id=schema_a.id,
                owner_id=1,
                file_path="/tmp/foo_a.parquet",
                storage_format="parquet",
            )
            foo_b = CatalogTable(
                name="foo",
                namespace_id=schema_b.id,
                owner_id=1,
                file_path="/tmp/foo_b.parquet",
                storage_format="parquet",
            )
            db.add_all([foo_a, foo_b])
            db.commit()
            db.refresh(foo_a)
            return cat.id, schema_a.id, schema_b.id, foo_a.id

    def test_list_tables_by_name_returns_both(self):
        self._seed_two_foo_tables()
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            matches = repo.list_tables_by_name("foo")
            assert len(matches) == 2
            assert {t.name for t in matches} == {"foo"}

    def test_resolve_qualified_disambiguates(self):
        _, schema_a_id, schema_b_id, _ = self._seed_two_foo_tables()
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            a = svc.resolve_table("ns_a.foo")
            b = svc.resolve_table("ns_b.foo")
            assert a.namespace_id == schema_a_id
            assert b.namespace_id == schema_b_id

    def test_resolve_bare_with_default_namespace(self):
        _, schema_a_id, _, _ = self._seed_two_foo_tables()
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            t = svc.resolve_table("foo", default_namespace_id=schema_a_id)
            assert t.namespace_id == schema_a_id

    def test_resolve_bare_ambiguous_warns_and_picks(self, caplog):
        _, schema_a_id, _, _ = self._seed_two_foo_tables()
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            with caplog.at_level("WARNING", logger="flowfile_core.catalog.service"):
                t = svc.resolve_table("foo")
            assert t.namespace_id == schema_a_id  # deterministic pick (lowest namespace_id)
            assert any("Ambiguous table reference 'foo'" in r.message for r in caplog.records)

    def test_resolve_bare_ambiguous_strict_raises(self):
        from flowfile_core.catalog.exceptions import AmbiguousTableError

        self._seed_two_foo_tables()
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            with pytest.raises(AmbiguousTableError) as excinfo:
                svc.resolve_table("foo", strict=True)
            assert len(excinfo.value.candidates) == 2

    def test_resolve_missing_raises_not_found(self):
        from flowfile_core.catalog.exceptions import TableNotFoundError

        _cleanup_catalog()
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            with pytest.raises(TableNotFoundError):
                svc.resolve_table("does_not_exist")

    def test_reject_dot_in_namespace_name(self):
        _cleanup_catalog()
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            with pytest.raises(ValueError, match="must not contain '.'"):
                svc.create_namespace(name="bad.name", owner_id=1)

    def test_reject_dot_in_table_name(self):
        _cleanup_catalog()
        with get_db_context() as db:
            cat = CatalogNamespace(name="cat2", level=0, owner_id=1)
            db.add(cat)
            db.commit()
            db.refresh(cat)

            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            with pytest.raises(ValueError, match="must not contain '.'"):
                svc._validate_table_registration("bad.table", cat.id)

    def test_catalog_table_out_exposes_full_table_name(self):
        _, schema_a_id, _, foo_a_id = self._seed_two_foo_tables()
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            out = svc.get_table(foo_a_id, user_id=1)
            assert out.namespace_name == "ns_a"
            assert out.full_table_name == "ns_a.foo"

    def test_resolve_endpoint_soft_picks_with_warnings(self):
        self._seed_two_foo_tables()
        response = client.get("/catalog/tables/resolve", params={"q": "foo"})
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["table"]["name"] == "foo"
        assert len(body["warnings"]) == 2
        names = [w["namespace_name"] for w in body["warnings"]]
        assert sorted(names) == ["ns_a", "ns_b"]

    def test_resolve_endpoint_strict_returns_409(self):
        self._seed_two_foo_tables()
        response = client.get(
            "/catalog/tables/resolve", params={"q": "foo", "strict": "true"}
        )
        assert response.status_code == 409, response.text
        body = response.json()
        assert body["detail"]["name"] == "foo"
        assert len(body["detail"]["candidates"]) == 2

    def test_resolve_endpoint_qualified(self):
        _, schema_a_id, _, _ = self._seed_two_foo_tables()
        response = client.get("/catalog/tables/resolve", params={"q": "ns_a.foo"})
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["table"]["namespace_id"] == schema_a_id
        assert body["warnings"] == []

    def test_execute_sql_rewrites_unquoted_qualified_names(self, monkeypatch):
        """``FROM default.catalog_tables`` (unquoted) should be rewritten to
        ``FROM "default.catalog_tables"`` before hitting the polars SQL engine,
        so users can write the form polars doesn't otherwise understand."""
        import flowfile_core.catalog.service as svc_module

        _cleanup_catalog()
        with get_db_context() as db:
            cat = CatalogNamespace(name="default", level=0, owner_id=1)
            db.add(cat)
            db.commit()
            db.refresh(cat)
            # Use a delta-shaped path so resolve_all_queryable_tables returns it.
            import tempfile

            tmpdir = tempfile.mkdtemp()
            # Make it look like a Delta table.
            import os

            os.makedirs(os.path.join(tmpdir, "_delta_log"))
            table = CatalogTable(
                name="catalog_tables",
                namespace_id=cat.id,
                owner_id=1,
                file_path=tmpdir,
                storage_format="delta",
            )
            db.add(table)
            db.commit()

            captured: dict = {}

            def fake_trigger(query, tables, max_rows, virtual_refs=None):
                captured["query"] = query
                return {
                    "columns": [],
                    "dtypes": [],
                    "rows": [],
                    "total_rows": 0,
                    "truncated": False,
                    "execution_time_ms": 0.0,
                    "used_tables": [],
                }

            monkeypatch.setattr(svc_module, "trigger_sql_query", fake_trigger)

            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            svc.execute_sql_query("SELECT * FROM default.catalog_tables")
            assert captured["query"] == 'SELECT * FROM "default.catalog_tables"'

            svc.execute_sql_query('SELECT * FROM "default.catalog_tables"')
            assert captured["query"] == 'SELECT * FROM "default.catalog_tables"'

            svc.execute_sql_query(
                'SELECT t.id FROM "default.catalog_tables" AS t WHERE t.id > 0'
            )
            assert captured["query"] == (
                'SELECT t.id FROM "default.catalog_tables" AS t WHERE t.id > 0'
            )

            svc.execute_sql_query('SELECT * FROM "default"."catalog_tables"')
            assert captured["query"] == 'SELECT * FROM "default.catalog_tables"'

            svc.execute_sql_query('SELECT * FROM "default" . "catalog_tables"')
            assert captured["query"] == 'SELECT * FROM "default.catalog_tables"'

            svc.execute_sql_query('SELECT * FROM default."catalog_tables"')
            assert captured["query"] == 'SELECT * FROM "default.catalog_tables"'

            svc.execute_sql_query('SELECT * FROM "default".catalog_tables')
            assert captured["query"] == 'SELECT * FROM "default.catalog_tables"'

    def test_queryable_tables_include_bare_and_qualified_keys(self):
        """resolve_all_queryable_tables should expose each table under its qualified
        name ('ns.table') and, when the bare name is unique, also under the bare name."""
        _, schema_a_id, schema_b_id, _ = self._seed_two_foo_tables()
        # Add a uniquely-named table in ns_a to cover the "bare is included" path.
        with get_db_context() as db:
            db.add(
                CatalogTable(
                    name="only_here",
                    namespace_id=schema_a_id,
                    owner_id=1,
                    file_path="/tmp/only_here.parquet",
                    storage_format="parquet",
                )
            )
            db.commit()

            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            # With our non-delta seeds the delta_map stays empty — assert against the
            # full catalog table set via a small helper instead.
            tables = repo.list_tables()
            bare_counts: dict[str, int] = {}
            for t in tables:
                bare_counts[t.name] = bare_counts.get(t.name, 0) + 1

            # foo collides across ns_a/ns_b → bare "foo" must be excluded.
            assert bare_counts["foo"] == 2
            # only_here is unique → bare "only_here" must be included alongside qualified.
            assert bare_counts["only_here"] == 1

            # Verify the real method skips the bare-collision case, keeps the uniquely-named
            # bare, and always emits qualified forms — use virtual tables so we exercise virtual_map.
            # Convert our seeded tables to virtual for the assertion.
            for t in tables:
                t.table_type = "virtual"
            db.commit()

            delta_map, virtual_map = svc.resolve_all_queryable_tables()
            assert "ns_a.foo" in virtual_map
            assert "ns_b.foo" in virtual_map
            assert "foo" not in virtual_map  # collision → bare dropped
            assert "ns_a.only_here" in virtual_map
            assert "only_here" in virtual_map  # unique → bare kept
            # Both qualified forms point to distinct ids.
            assert virtual_map["ns_a.foo"] != virtual_map["ns_b.foo"]
            # Bare alias for unique name points to the same id as its qualified alias.
            assert virtual_map["only_here"] == virtual_map["ns_a.only_here"]

    def test_schedule_out_includes_namespace_fields(self):
        _, schema_a_id, _, foo_a_id = self._seed_two_foo_tables()
        with get_db_context() as db:
            flow_reg = FlowRegistration(
                name="sched_flow", namespace_id=schema_a_id, owner_id=1, flow_path="/tmp/unused.yaml"
            )
            db.add(flow_reg)
            db.commit()
            db.refresh(flow_reg)

            schedule = FlowSchedule(
                registration_id=flow_reg.id,
                owner_id=1,
                schedule_type="table_trigger",
                trigger_table_id=foo_a_id,
                enabled=True,
            )
            db.add(schedule)
            db.commit()
            db.refresh(schedule)

            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            out = svc._schedule_to_out(schedule)
            assert out.trigger_namespace_id == schema_a_id
            assert out.trigger_namespace_name == "ns_a"
            assert out.trigger_full_table_name == "ns_a.foo"


class TestCronSchedules:
    """Cron schedule create/read/update via the catalog API."""

    @staticmethod
    def _register_flow() -> int:
        with get_db_context() as db:
            flow = FlowRegistration(name="cron_flow", flow_path="/tmp/cron_flow.flowfile", owner_id=1)
            db.add(flow)
            db.commit()
            db.refresh(flow)
            return flow.id

    def test_create_cron_schedule(self):
        flow_id = self._register_flow()
        resp = client.post(
            "/catalog/schedules",
            json={
                "registration_id": flow_id,
                "schedule_type": "cron",
                "cron_expression": "0 9 * * 1-5",
                "cron_timezone": "Europe/Amsterdam",
                "name": "Weekday morning",
            },
        )
        assert resp.status_code == 201, resp.text
        data = resp.json()
        assert data["schedule_type"] == "cron"
        assert data["cron_expression"] == "0 9 * * 1-5"
        assert data["cron_timezone"] == "Europe/Amsterdam"

        got = client.get(f"/catalog/schedules/{data['id']}")
        assert got.status_code == 200
        assert got.json()["cron_expression"] == "0 9 * * 1-5"

    def test_create_cron_invalid_expression_rejected(self):
        flow_id = self._register_flow()
        resp = client.post(
            "/catalog/schedules",
            json={
                "registration_id": flow_id,
                "schedule_type": "cron",
                "cron_expression": "not a cron",
                "cron_timezone": "UTC",
            },
        )
        assert resp.status_code == 422, resp.text

    def test_update_cron_expression(self):
        flow_id = self._register_flow()
        created = client.post(
            "/catalog/schedules",
            json={
                "registration_id": flow_id,
                "schedule_type": "cron",
                "cron_expression": "0 9 * * *",
                "cron_timezone": "UTC",
            },
        ).json()
        resp = client.put(
            f"/catalog/schedules/{created['id']}",
            json={"cron_expression": "0 18 * * *"},
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["cron_expression"] == "0 18 * * *"

    def test_update_cron_on_non_cron_schedule_rejected(self):
        # Cron fields must not be stored on a non-cron schedule (they'd be silently ignored).
        flow_id = self._register_flow()
        with get_db_context() as db:
            sched = FlowSchedule(
                registration_id=flow_id,
                owner_id=1,
                schedule_type="interval",
                interval_seconds=3600,
                enabled=True,
            )
            db.add(sched)
            db.commit()
            db.refresh(sched)
            sched_id = sched.id
        resp = client.put(
            f"/catalog/schedules/{sched_id}",
            json={"cron_expression": "0 9 * * *"},
        )
        assert resp.status_code == 422, resp.text

    def test_validate_cron_endpoint(self):
        # Same croniter rules as create, but returns a flag (200) instead of 422
        # so the builder can gate its submit button on the backend's verdict.
        ok = client.post(
            "/catalog/schedules/validate-cron",
            json={"cron_expression": "0 9 * * 1-5", "cron_timezone": "Europe/Amsterdam"},
        )
        assert ok.status_code == 200, ok.text
        assert ok.json() == {"valid": True, "error": None}

        bad_expr = client.post(
            "/catalog/schedules/validate-cron",
            json={"cron_expression": "not a cron", "cron_timezone": "UTC"},
        )
        assert bad_expr.status_code == 200, bad_expr.text
        assert bad_expr.json()["valid"] is False
        assert bad_expr.json()["error"]  # carries the backend's reason

        bad_tz = client.post(
            "/catalog/schedules/validate-cron",
            json={"cron_expression": "0 9 * * *", "cron_timezone": "Not/AZone"},
        )
        assert bad_tz.status_code == 200, bad_tz.text
        assert bad_tz.json()["valid"] is False


# Catalog table storage-deletion tests


class TestCatalogTableStorageDeletion:
    """delete_table only removes Flowfile-managed storage; external files are kept."""

    def _make_namespace(self) -> int:
        cat = client.post("/catalog/namespaces", json={"name": "TC"}).json()
        schema = client.post("/catalog/namespaces", json={"name": "TS", "parent_id": cat["id"]}).json()
        return schema["id"]

    def _insert_table(self, namespace_id: int, file_path: str, storage_format: str) -> int:
        with get_db_context() as db:
            owner_id = db.query(User).first().id
            table = CatalogTable(
                name=f"t_{file_path.split('/')[-1]}",
                namespace_id=namespace_id,
                owner_id=owner_id,
                file_path=file_path,
                storage_format=storage_format,
            )
            db.add(table)
            db.commit()
            return table.id

    def test_delete_managed_table_removes_storage(self):
        from shared.storage_config import storage

        ns_id = self._make_namespace()
        managed_dir = storage.catalog_tables_directory / "test_managed_tbl"
        managed_dir.mkdir(parents=True, exist_ok=True)
        (managed_dir / "data.parquet").write_bytes(b"x")
        table_id = self._insert_table(ns_id, str(managed_dir), "delta")

        resp = client.delete(f"/catalog/tables/{table_id}", params={"delete_file": True})
        assert resp.status_code == 204
        assert not managed_dir.exists()

    def test_delete_external_table_preserves_storage(self, tmp_path):
        ns_id = self._make_namespace()
        external = tmp_path / "my_own_data.parquet"
        external.write_bytes(b"x")
        table_id = self._insert_table(ns_id, str(external), "parquet")

        resp = client.delete(f"/catalog/tables/{table_id}", params={"delete_file": True})
        assert resp.status_code == 204
        assert external.exists()  # external/user-owned file must never be deleted

    def test_delete_managed_table_keeps_storage_without_flag(self):
        from shared.storage_config import storage

        ns_id = self._make_namespace()
        managed_dir = storage.catalog_tables_directory / "test_keep_tbl"
        managed_dir.mkdir(parents=True, exist_ok=True)
        (managed_dir / "data.parquet").write_bytes(b"x")
        table_id = self._insert_table(ns_id, str(managed_dir), "delta")

        resp = client.delete(f"/catalog/tables/{table_id}")  # delete_file defaults False
        assert resp.status_code == 204
        assert managed_dir.exists()
        # cleanup
        import shutil

        shutil.rmtree(managed_dir, ignore_errors=True)


class TestSaveFlowToCatalog:
    """``/save_flow_to_catalog`` Save-As semantics: a new name copies (leaves the
    original intact), and a name freed by a deleted file can be reclaimed."""

    def _make_namespace(self) -> int:
        cat = client.post("/catalog/namespaces", json={"name": "SaveCat"}).json()
        schema = client.post("/catalog/namespaces", json={"name": "SaveSchema", "parent_id": cat["id"]}).json()
        return schema["id"]

    def _user_id(self) -> int:
        with get_db_context() as db:
            return db.query(User).first().id

    def _new_inmemory_flow(self, name: str) -> int:
        """Create a quick-create scratch flow in the in-memory handler."""
        return flow_file_handler.add_flow(name=name, user_id=self._user_id())

    def _path_of(self, flow_id: int) -> Path:
        return Path(flow_file_handler.get_flow(flow_id).flow_settings.path)

    @staticmethod
    def _cleanup(flow_ids: tuple[int, ...], paths: tuple[Path, ...]) -> None:
        for p in paths:
            p.unlink(missing_ok=True)
        for fid in flow_ids:
            try:
                flow_file_handler.delete_flow(fid)
            except Exception:
                pass

    def test_save_as_new_name_keeps_original(self):
        ns_id = self._make_namespace()
        fid = self._new_inmemory_flow("sa_alpha")
        scratch = self._path_of(fid)
        assert str(scratch).startswith(str(storage.unnamed_flows_directory))

        # First catalog save (scratch -> managed): throwaway scratch file is cleaned up.
        r1 = client.post(
            "/save_flow_to_catalog",
            params={"flow_id": fid, "flow_name": "sa_alpha", "namespace_id": ns_id},
        )
        assert r1.status_code == 200, r1.text
        fid2 = r1.json()
        alpha_path = self._path_of(fid2)
        assert alpha_path.exists()
        assert not scratch.exists(), "scratch temp flow should be cleaned up on first catalog save"

        # Save-As under a NEW name: the original managed flow must survive (copy, not move).
        r2 = client.post(
            "/save_flow_to_catalog",
            params={"flow_id": fid2, "flow_name": "sa_beta", "namespace_id": ns_id},
        )
        assert r2.status_code == 200, r2.text
        fid3 = r2.json()
        beta_path = self._path_of(fid3)

        assert alpha_path.exists(), "Save-As under a new name must not delete the original file"
        assert beta_path.exists()

        flows = client.get("/catalog/flows", params={"namespace_id": ns_id}).json()
        by_name = {f["name"]: f for f in flows}
        assert {"sa_alpha", "sa_beta"} <= set(by_name)
        # Save-As is a copy: the new name is a brand-new registration row pointing
        # at its own file — never a repoint of the original's row onto the new path.
        assert by_name["sa_alpha"]["id"] != by_name["sa_beta"]["id"], (
            "Save-As must create a new catalog registration, not reuse the original's id"
        )
        assert by_name["sa_alpha"]["flow_path"] == str(alpha_path)
        assert by_name["sa_beta"]["flow_path"] == str(beta_path)

        self._cleanup((fid2, fid3), (alpha_path, beta_path))

    def test_save_as_over_ghost_reclaims_name(self):
        ns_id = self._make_namespace()
        ghost_path = str(storage.flows_directory / "999999999_ghost.yaml")
        assert not Path(ghost_path).exists()
        client.post(
            "/catalog/flows",
            json={"name": "ghost", "flow_path": ghost_path, "namespace_id": ns_id},
        )

        fid = self._new_inmemory_flow("sa_scratch_ghost")
        resp = client.post(
            "/save_flow_to_catalog",
            params={"flow_id": fid, "flow_name": "ghost", "namespace_id": ns_id},
        )
        assert resp.status_code == 200, resp.text  # ghost reclaimed instead of a dead-end 409
        new_fid = resp.json()
        new_path = self._path_of(new_fid)
        assert new_path.exists()

        flows = client.get("/catalog/flows", params={"namespace_id": ns_id}).json()
        ghosts = [f for f in flows if f["name"] == "ghost"]
        assert len(ghosts) == 1, "the dead ghost row should be replaced, not duplicated"
        # The id may be reused (SQLite recycles rowids); what matters is the
        # surviving row points at the new, existing file — not the dead path.
        assert ghosts[0]["flow_path"] == str(new_path)
        assert ghosts[0]["flow_path"] != ghost_path
        assert ghosts[0]["file_exists"] is True

        self._cleanup((new_fid,), (new_path,))

    def test_name_collision_with_live_file_still_409(self):
        ns_id = self._make_namespace()
        fid_a = self._new_inmemory_flow("sa_dup_a")
        ra = client.post(
            "/save_flow_to_catalog",
            params={"flow_id": fid_a, "flow_name": "sa_dup", "namespace_id": ns_id},
        )
        assert ra.status_code == 200, ra.text
        fid_a2 = ra.json()
        dup_path = self._path_of(fid_a2)
        assert dup_path.exists()

        # A different flow cannot silently take a name whose file is alive.
        fid_b = self._new_inmemory_flow("sa_dup_b")
        rb = client.post(
            "/save_flow_to_catalog",
            params={"flow_id": fid_b, "flow_name": "sa_dup", "namespace_id": ns_id},
        )
        assert rb.status_code == 409

        self._cleanup((fid_a2, fid_b), (dup_path,))
