"""Route-level tests for the Phase-2 project endpoints: dirty flag on /active,
version history, restore, reload, close, and standalone-secret upsert.

These are thin wrappers over ``project_sync``; one project per test, closed in a
finally so the singleton never leaks into other tests.
"""

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.project import git_ops, project_sync
from flowfile_core.secret_manager.secret_manager import (
    decrypt_secret,
    delete_secret,
    get_encrypted_secret,
)

OWNER = 1


def _get_auth_token() -> str:
    with TestClient(main.app) as client:
        return client.post("/auth/token").json()["access_token"]


def _make_client() -> TestClient:
    client = TestClient(main.app)
    client.headers = {"Authorization": f"Bearer {_get_auth_token()}"}
    return client


client = _make_client()


@pytest.fixture(autouse=True)
def _require_git():
    if not git_ops.git_available():
        pytest.skip("git not available")


def test_version_lifecycle_via_routes(tmp_path):
    root = tmp_path / "proj"
    project_sync.close_project(OWNER)
    try:
        # No active project: history is a 409.
        assert client.get("/project/versions").status_code == 409

        assert client.post("/project/init", json={"folder_path": str(root), "name": "Routes"}).status_code == 200

        active = client.get("/project/active").json()
        assert active["project"]["name"] == "Routes"
        assert active["dirty"] is False
        assert active["has_external_changes"] is False

        # A tracked file change between two saved versions gives history to restore.
        (root / "marker.txt").write_text("a", encoding="utf-8")
        assert client.get("/project/active").json()["dirty"] is True
        unsaved = client.get("/project/uncommitted").json()["changes"]
        assert any(c["path"] == "marker.txt" and c["change"] == "added" for c in unsaved), unsaved
        assert client.post("/project/versions", json={"message": "marker a"}).json()["sha"]

        (root / "marker.txt").write_text("b", encoding="utf-8")
        assert client.post("/project/versions", json={"message": "marker b"}).json()["sha"]
        assert client.get("/project/active").json()["dirty"] is False

        versions = client.get("/project/versions").json()["versions"]
        assert len(versions) >= 3
        for v in versions:
            assert set(v) == {"sha", "message", "committed_at"}
        version_a = versions[1]["sha"]  # newest-first: [b, a, init]

        # Restore the "a" version: files revert, DB rebuilds, and the restore is auto-committed
        # as a new version (so the tree is clean afterwards).
        restored = client.post("/project/restore", json={"sha": version_a})
        assert restored.status_code == 200
        assert set(restored.json()) == {"imported", "placeholder_secrets"}
        assert (root / "marker.txt").read_text(encoding="utf-8") == "a"
        assert client.get("/project/active").json()["dirty"] is False
        assert client.get("/project/versions").json()["versions"][0]["message"].startswith("Restore")

        # Reload accepts on-disk state and returns the same import shape.
        reloaded = client.post("/project/reload")
        assert reloaded.status_code == 200
        assert set(reloaded.json()) == {"imported", "placeholder_secrets"}

        assert client.post("/project/close").json() == {"ok": True}
        assert client.get("/project/active").json() == {"project": None}
    finally:
        project_sync.close_project(OWNER)


def test_restore_prunes_added_flow_and_diff_lists_it(tmp_path):
    from flowfile_core.database.models import FlowRegistration
    from flowfile_core.flowfile.catalog_helpers import auto_register_flow
    from flowfile_core.flowfile.handler import FlowfileHandler
    from flowfile_core.schemas import input_schema, schemas

    root = tmp_path / "proj"
    project_sync.close_project(OWNER)
    flow_uuid = None
    try:
        assert client.post("/project/init", json={"folder_path": str(root), "name": "Prune"}).status_code == 200
        empty_sha = client.get("/project/versions").json()["versions"][0]["sha"]

        # Build a real flow, then project + commit it as a new version.
        handler = FlowfileHandler()
        handler.register_flow(schemas.FlowSettings(flow_id=636363, name="demo", path="."))
        graph = handler.get_flow(636363)
        graph.add_node_promise(input_schema.NodePromise(flow_id=636363, node_id=1, node_type="manual_input"))
        graph.add_manual_input(
            input_schema.NodeManualInput(
                flow_id=636363, node_id=1, raw_data_format=input_schema.RawData.from_pylist([{"a": 1}])
            )
        )
        flow_path = str(tmp_path / "demo.flow.yaml")
        graph.save_flow(flow_path)
        auto_register_flow(flow_path, "demo", OWNER)
        with get_db_context() as db:
            reg = db.query(FlowRegistration).filter_by(flow_path=flow_path).first()
            assert reg is not None
            flow_uuid = reg.flow_uuid

        assert client.post("/project/versions", json={"message": "add demo"}).json()["sha"]
        assert list(root.glob("flows/demo*.flow.yaml")), "flow should be projected into the project"

        # The version's own changelog (vs its parent) lists the flow as an addition.
        add_demo_sha = client.get("/project/versions").json()["versions"][0]["sha"]
        version_diff = client.get(f"/project/versions/{add_demo_sha}/diff").json()["changes"]
        assert any(c["change"] == "added" and c["kind"] == "flow" for c in version_diff), version_diff

        # The diff preview for the empty version lists the flow as a removal.
        changes = client.get(f"/project/versions/{empty_sha}/changes").json()["changes"]
        assert any(
            c["change"] == "removed" and c["kind"] == "flow" and c["label"].startswith("demo")
            for c in changes
        ), changes

        # Restore the empty version: the flow is pruned from the DB and the files; auto-committed.
        restored = client.post("/project/restore", json={"sha": empty_sha, "label": "empty"})
        assert restored.status_code == 200
        with get_db_context() as db:
            assert db.query(FlowRegistration).filter_by(flow_uuid=flow_uuid).first() is None
        assert not list(root.glob("flows/demo*.flow.yaml")), "flow file should be gone after restore"
        assert client.get("/project/active").json()["dirty"] is False
        assert client.get("/project/versions").json()["versions"][0]["message"].startswith("Restore")
    finally:
        project_sync.close_project(OWNER)
        if flow_uuid is not None:
            with get_db_context() as db:
                for reg in db.query(FlowRegistration).filter_by(flow_uuid=flow_uuid).all():
                    db.delete(reg)
                db.commit()


def test_deleting_a_flow_shows_as_a_project_change(tmp_path):
    from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
    from flowfile_core.database.models import FlowRegistration
    from flowfile_core.flowfile.catalog_helpers import auto_register_flow
    from flowfile_core.flowfile.handler import FlowfileHandler
    from flowfile_core.schemas import input_schema, schemas

    root = tmp_path / "proj"
    project_sync.close_project(OWNER)
    flow_uuid = None
    try:
        assert client.post("/project/init", json={"folder_path": str(root), "name": "Del"}).status_code == 200

        handler = FlowfileHandler()
        handler.register_flow(schemas.FlowSettings(flow_id=646464, name="demo", path="."))
        graph = handler.get_flow(646464)
        graph.add_node_promise(input_schema.NodePromise(flow_id=646464, node_id=1, node_type="manual_input"))
        graph.add_manual_input(
            input_schema.NodeManualInput(
                flow_id=646464, node_id=1, raw_data_format=input_schema.RawData.from_pylist([{"a": 1}])
            )
        )
        flow_path = str(tmp_path / "demo.flow.yaml")
        graph.save_flow(flow_path)
        auto_register_flow(flow_path, "demo", OWNER)
        with get_db_context() as db:
            reg = db.query(FlowRegistration).filter_by(flow_path=flow_path).first()
            reg_id, flow_uuid = reg.id, reg.flow_uuid

        # Save the flow as a version → it's committed; the tree is clean.
        assert client.post("/project/versions", json={"message": "add demo"}).json()["sha"]
        assert list(root.glob("flows/*.flow.yaml")), "flow should be projected"
        assert client.get("/project/active").json()["dirty"] is False

        # Delete the flow → the projected file is removed, so it shows as an unsaved change.
        with get_db_context() as db:
            CatalogService(SQLAlchemyCatalogRepository(db)).delete_flow(reg_id, delete_file=True)
        assert not list(root.glob("flows/*.flow.yaml")), "projected flow file should be gone after delete"
        assert client.get("/project/active").json()["dirty"] is True
        unsaved = client.get("/project/uncommitted").json()["changes"]
        assert any(c["change"] == "removed" and c["kind"] == "flow" for c in unsaved), unsaved
        flow_uuid = None  # deleted; nothing to clean up
    finally:
        project_sync.close_project(OWNER)
        if flow_uuid is not None:
            with get_db_context() as db:
                for reg in db.query(FlowRegistration).filter_by(flow_uuid=flow_uuid).all():
                    db.delete(reg)
                db.commit()


def test_schedules_do_not_duplicate_on_reimport_and_clear_on_delete(tmp_path):
    from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
    from flowfile_core.database.models import FlowRegistration, FlowSchedule
    from flowfile_core.flowfile.catalog_helpers import auto_register_flow
    from flowfile_core.flowfile.handler import FlowfileHandler
    from flowfile_core.schemas import input_schema, schemas

    root = tmp_path / "proj"
    project_sync.close_project(OWNER)
    flow_uuid = None
    try:
        assert client.post("/project/init", json={"folder_path": str(root), "name": "Sched"}).status_code == 200

        handler = FlowfileHandler()
        handler.register_flow(schemas.FlowSettings(flow_id=656565, name="demo", path="."))
        graph = handler.get_flow(656565)
        graph.add_node_promise(input_schema.NodePromise(flow_id=656565, node_id=1, node_type="manual_input"))
        graph.add_manual_input(
            input_schema.NodeManualInput(
                flow_id=656565, node_id=1, raw_data_format=input_schema.RawData.from_pylist([{"a": 1}])
            )
        )
        flow_path = str(tmp_path / "demo.flow.yaml")
        graph.save_flow(flow_path)
        auto_register_flow(flow_path, "demo", OWNER)
        with get_db_context() as db:
            reg = db.query(FlowRegistration).filter_by(flow_path=flow_path).first()
            reg_id, flow_uuid = reg.id, reg.flow_uuid
            # A *nameless* interval schedule — the case that used to duplicate on every import.
            CatalogService(SQLAlchemyCatalogRepository(db)).create_schedule(
                registration_id=reg_id, owner_id=OWNER, schedule_type="interval", interval_seconds=3600, enabled=True
            )

        assert client.post("/project/versions", json={"message": "add demo + schedule"}).json()["sha"]
        # Re-import twice: the schedule must stay singular (no duplication).
        assert client.post("/project/reload").status_code == 200
        assert client.post("/project/reload").status_code == 200
        with get_db_context() as db:
            n = db.query(FlowSchedule).filter_by(registration_id=reg_id).count()
        assert n == 1, f"expected exactly 1 schedule after reloads, got {n}"

        # Deleting the flow clears its schedules (no orphans).
        with get_db_context() as db:
            CatalogService(SQLAlchemyCatalogRepository(db)).delete_flow(reg_id, delete_file=True)
            assert db.query(FlowSchedule).filter_by(registration_id=reg_id).count() == 0
        flow_uuid = None
    finally:
        project_sync.close_project(OWNER)
        if flow_uuid is not None:
            with get_db_context() as db:
                for reg in db.query(FlowRegistration).filter_by(flow_uuid=flow_uuid).all():
                    db.query(FlowSchedule).filter_by(registration_id=reg.id).delete()
                    db.delete(reg)
                db.commit()


def test_secret_upsert_overwrites_in_place(tmp_path):
    name = "proj_route_secret_upsert"
    with get_db_context() as db:
        if get_encrypted_secret(OWNER, name) is not None:
            delete_secret(db, name, OWNER)
    try:
        assert client.post("/project/secrets", json=[{"name": name, "value": "v1"}]).json() == {"updated": 1}
        assert decrypt_secret(get_encrypted_secret(OWNER, name)).get_secret_value() == "v1"

        # Second upsert overwrites the value rather than inserting a duplicate row.
        assert client.post("/project/secrets", json=[{"name": name, "value": "v2"}]).status_code == 200
        assert decrypt_secret(get_encrypted_secret(OWNER, name)).get_secret_value() == "v2"
        with get_db_context() as db:
            rows = db.query(db_models.Secret).filter_by(user_id=OWNER, name=name).all()
            assert len(rows) == 1
    finally:
        with get_db_context() as db:
            if get_encrypted_secret(OWNER, name) is not None:
                delete_secret(db, name, OWNER)
