"""DB-backed projection determinism + import round-trip + hook isolation.

Uses the session test DB and the real store functions. Each test sets up and
tears down its own project + resources so the project_sync singleton never
leaks projection side-effects into other tests.
"""

import os
from pathlib import Path

import pytest
import yaml

from flowfile_core import flow_file_handler
from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import CatalogNamespace, FlowRegistration, FlowSchedule
from flowfile_core.flowfile.catalog_helpers import auto_register_flow
from flowfile_core.flowfile.database_connection_manager.db_connections import (
    delete_database_connection,
    get_database_connection,
    store_database_connection,
)
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.project import project_sync, projection
from flowfile_core.schemas import input_schema, schemas
from shared.storage_config import storage

OWNER = 1


def _make_connection(name: str, password: str) -> None:
    with get_db_context() as db:
        if get_database_connection(db, name, OWNER) is None:
            store_database_connection(
                db,
                input_schema.FullDatabaseConnection(
                    connection_name=name,
                    database_type="postgresql",
                    username="etl_reader",
                    password=password,
                    host="db.internal",
                    port=5432,
                    database="sales",
                    ssl_enabled=True,
                ),
                OWNER,
            )


def _make_flow(tmp_path: Path, name: str) -> tuple[str, str]:
    handler = FlowfileHandler()
    handler.register_flow(schemas.FlowSettings(flow_id=424242, name=name, path="."))
    graph = handler.get_flow(424242)
    graph.add_node_promise(input_schema.NodePromise(flow_id=424242, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=424242, node_id=1, raw_data_format=input_schema.RawData.from_pylist([{"a": 1}])
        )
    )
    flow_path = str(tmp_path / f"{name}.yaml")
    graph.save_flow(flow_path)
    auto_register_flow(flow_path, name, OWNER)
    with get_db_context() as db:
        reg = db.query(FlowRegistration).filter_by(flow_path=flow_path).first()
        assert reg is not None, "flow registration not created (default namespace missing?)"
        return reg.flow_uuid, name


def _make_schedule(flow_uuid: str) -> None:
    with get_db_context() as db:
        reg = db.query(FlowRegistration).filter_by(flow_uuid=flow_uuid).first()
        svc = CatalogService(SQLAlchemyCatalogRepository(db))
        svc.create_schedule(
            registration_id=reg.id,
            owner_id=OWNER,
            schedule_type="interval",
            interval_seconds=3600,
            enabled=True,
            name="hourly",
        )


def _snapshot(root: Path) -> dict[str, bytes]:
    out: dict[str, bytes] = {}
    for sub in ("flows", "connections", "schedules"):
        for p in sorted((root / sub).rglob("*")):
            if p.is_file():
                out[str(p.relative_to(root))] = p.read_bytes()
    secrets = root / "secrets.yaml"
    if secrets.exists():
        out["secrets.yaml"] = secrets.read_bytes()
    return out


def _cleanup(conn_names: list[str], flow_uuids: list[str]) -> None:
    project_sync.close_project(OWNER)
    for name in conn_names:
        try:
            with get_db_context() as db:
                delete_database_connection(db, name, OWNER)
        except Exception:
            pass
    with get_db_context() as db:
        for flow_uuid in flow_uuids:
            for reg in db.query(FlowRegistration).filter_by(flow_uuid=flow_uuid).all():
                db.query(FlowSchedule).filter_by(registration_id=reg.id).delete()
                if reg.flow_path and "/project/" in reg.flow_path.replace("\\", "/"):
                    Path(reg.flow_path).unlink(missing_ok=True)
                db.delete(reg)
        db.commit()


def test_projection_is_deterministic_and_secret_free(tmp_path, monkeypatch):
    project_sync.close_project(OWNER)
    conn = "proj_db_det"
    _make_connection(conn, "s3cr3t")
    flow_uuid, flow_name = _make_flow(tmp_path, "proj_flow_det")
    _make_schedule(flow_uuid)
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Det Test", OWNER)

        snap1 = _snapshot(root)
        with get_db_context() as db:
            projection.project_all(db, root, OWNER)
        assert _snapshot(root) == snap1, "re-projecting unchanged DB must be byte-identical"

        conn_text = (root / "connections" / "database" / f"{conn}.yaml").read_text(encoding="utf-8")
        assert "${secret:" + conn + "}" in conn_text
        assert "s3cr3t" not in conn_text
        assert "$ffsec$" not in conn_text
        assert list(root.glob("flows/*.flow.yaml"))
        assert (root / "schedules" / f"{flow_name}.yaml").exists()

        # project -> import -> project must be byte-identical.
        monkeypatch.setenv("FLOWFILE_SECRET_PROJ_DB_DET", "s3cr3t")
        from flowfile_core.project.importer import import_project

        import_project(root, OWNER)
        with get_db_context() as db:
            projection.project_all(db, root, OWNER)
        assert _snapshot(root) == snap1, "project->import->project must round-trip"
    finally:
        _cleanup([conn], [flow_uuid])


def test_import_rebuilds_missing_connection_from_env(tmp_path, monkeypatch):
    project_sync.close_project(OWNER)
    conn = "proj_db_rebuild"
    _make_connection(conn, "orig_pw")
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Rebuild Test", OWNER)
        # Simulate a fresh machine: close the project (the file stays committed in git),
        # then drop the DB row, then rebuild from files + env.
        project_sync.close_project(OWNER)
        with get_db_context() as db:
            delete_database_connection(db, conn, OWNER)
        with get_db_context() as db:
            assert get_database_connection(db, conn, OWNER) is None
        assert (root / "connections" / "database" / f"{conn}.yaml").exists()

        monkeypatch.setenv("FLOWFILE_SECRET_PROJ_DB_REBUILD", "refilled_pw")
        from flowfile_core.project.importer import import_project

        result = import_project(root, OWNER)
        with get_db_context() as db:
            assert get_database_connection(db, conn, OWNER) is not None
        assert result.imported_connections >= 1
    finally:
        _cleanup([conn], [])


def test_import_creates_placeholder_when_secret_missing(tmp_path, monkeypatch):
    project_sync.close_project(OWNER)
    conn = "proj_db_placeholder"
    _make_connection(conn, "orig_pw")
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Placeholder Test", OWNER)
        project_sync.close_project(OWNER)
        with get_db_context() as db:
            delete_database_connection(db, conn, OWNER)

        monkeypatch.delenv("FLOWFILE_SECRET_PROJ_DB_PLACEHOLDER", raising=False)
        from flowfile_core.project.importer import import_project

        result = import_project(root, OWNER)
        # Setup completes; the connection exists with a placeholder secret to fix later.
        with get_db_context() as db:
            assert get_database_connection(db, conn, OWNER) is not None
        assert conn in result.placeholder_secrets
    finally:
        _cleanup([conn], [])


def test_projection_failure_does_not_break_the_primary_operation(tmp_path, monkeypatch):
    project_sync.close_project(OWNER)
    seed = "proj_db_seed"
    _make_connection(seed, "pw")
    root = tmp_path / "project"
    project_sync.init_project(str(root), "Isolation Test", OWNER)
    conn = "proj_db_isolation"
    try:
        with get_db_context() as db:
            delete_database_connection(db, conn, OWNER)

        def _boom(*args, **kwargs):
            raise RuntimeError("projection blew up")

        monkeypatch.setattr(projection, "project_database_connection", _boom)
        # The hook fires inside store_database_connection; it must swallow the error.
        _make_connection(conn, "pw2")
        with get_db_context() as db:
            assert get_database_connection(db, conn, OWNER) is not None
    finally:
        _cleanup([seed, conn], [])


def test_init_and_save_version_create_git_history(tmp_path):
    from flowfile_core.project import git_ops

    if not git_ops.git_available():
        pytest.skip("git not available")
    project_sync.close_project(OWNER)
    conn = "proj_db_git"
    _make_connection(conn, "pw")
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Git Test", OWNER)
        assert git_ops.is_repo(root)
        assert len(git_ops.log(root)) >= 1

        # A new connection projects automatically; "Save version" commits it.
        _make_connection("proj_db_git2", "pw2")
        sha = project_sync.save_version(OWNER, "Add second connection")
        assert sha
        assert len(git_ops.log(root)) >= 2
    finally:
        _cleanup([conn, "proj_db_git2"], [])


def test_no_op_when_no_active_project(monkeypatch):
    project_sync.close_project(OWNER)
    assert project_sync.get_active_project(OWNER) is None

    def _boom(*args, **kwargs):
        raise AssertionError("projection must not run without an active project")

    monkeypatch.setattr(projection, "project_flow", _boom)
    # No active project -> hook returns immediately, never touching projection.
    project_sync.flow_saved("/some/path.flow.yaml", OWNER)


# --- churn / naming-stability regressions (the reported bug) ------------------


def _make_flow_named(tmp_path: Path, flowfile_name: str, catalog_name: str, flow_id: int = 424242) -> str:
    """Create a flow whose internal flowfile_name differs from its catalog name (the demo's shape)."""
    handler = FlowfileHandler()
    handler.register_flow(schemas.FlowSettings(flow_id=flow_id, name=flowfile_name, path="."))
    graph = handler.get_flow(flow_id)
    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=flow_id, node_id=1, raw_data_format=input_schema.RawData.from_pylist([{"a": 1}])
        )
    )
    flow_path = str(tmp_path / f"{flowfile_name}.yaml")
    graph.save_flow(flow_path)
    auto_register_flow(flow_path, catalog_name, OWNER)
    with get_db_context() as db:
        reg = db.query(FlowRegistration).filter_by(flow_path=flow_path).first()
        assert reg is not None
        return reg.flow_uuid


def _delete_flow(flow_uuid: str) -> None:
    with get_db_context() as db:
        reg = db.query(FlowRegistration).filter_by(flow_uuid=flow_uuid).first()
        if reg is None:
            return
        svc = CatalogService(SQLAlchemyCatalogRepository(db))
        svc.delete_flow(reg.id, delete_file=True)


# --- namespace placement / projection (the catalog-mirroring feature) ----------


def _make_flow_in_namespace(
    tmp_path: Path, flowfile_name: str, catalog: str, schema: str, flow_id: int = 424242
) -> tuple[str, int, int]:
    """Build a flow and register it under an explicit custom ``catalog > schema`` (not Local Flows)."""
    handler = FlowfileHandler()
    handler.register_flow(schemas.FlowSettings(flow_id=flow_id, name=flowfile_name, path="."))
    graph = handler.get_flow(flow_id)
    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=flow_id, node_id=1, raw_data_format=input_schema.RawData.from_pylist([{"a": 1}])
        )
    )
    flow_path = str(tmp_path / f"{flowfile_name}.yaml")
    graph.save_flow(flow_path)
    with get_db_context() as db:
        svc = CatalogService(SQLAlchemyCatalogRepository(db))
        cat = svc.repo.get_namespace_by_name(catalog, None) or svc.create_namespace(name=catalog, owner_id=OWNER)
        sch = svc.repo.get_namespace_by_name(schema, cat.id) or svc.create_namespace(
            name=schema, owner_id=OWNER, parent_id=cat.id
        )
        reg = FlowRegistration(name=flowfile_name, flow_path=flow_path, namespace_id=sch.id, owner_id=OWNER)
        db.add(reg)
        db.commit()
        db.refresh(reg)
        return reg.flow_uuid, sch.id, cat.id


def _cleanup_custom_namespaces(catalog_names: list[str]) -> None:
    with get_db_context() as db:
        for cat_name in catalog_names:
            cat = db.query(CatalogNamespace).filter_by(name=cat_name, parent_id=None).first()
            if cat is None:
                continue
            ids = [cat.id] + [c.id for c in db.query(CatalogNamespace).filter_by(parent_id=cat.id).all()]
            db.query(FlowRegistration).filter(FlowRegistration.namespace_id.in_(ids)).delete(synchronize_session=False)
            db.query(CatalogNamespace).filter(CatalogNamespace.id.in_(ids)).delete(synchronize_session=False)
            db.commit()


def test_custom_namespace_round_trips_on_clean_db(tmp_path):
    project_sync.close_project(OWNER)
    flow_uuid, schema_id, catalog_id = _make_flow_in_namespace(tmp_path, "ns_flow", "Analytics", "Marts")
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "NS Test", OWNER)
        # The flow file carries its catalog placement by name.
        doc = yaml.safe_load((root / "flows" / "ns_flow.flow.yaml").read_text(encoding="utf-8"))
        assert doc["namespace"] == {"catalog": "Analytics", "schema": "Marts"}
        # namespaces.yaml mirrors the catalog tree: custom Analytics > Marts plus seeded system ones.
        cats = {c["catalog"]: c for c in yaml.safe_load((root / "namespaces.yaml").read_text(encoding="utf-8"))["namespaces"]}
        assert {s["name"] for s in cats["Analytics"]["schemas"]} == {"Marts"}
        assert not cats["Analytics"].get("is_public")
        assert cats["General"].get("is_public") is True  # seeded system namespaces are listed too

        # Simulate a fresh machine: close the project (files stay committed), drop the flow + custom
        # namespaces from the DB, then rebuild from the files.
        project_sync.close_project(OWNER)
        _delete_flow(flow_uuid)
        _cleanup_custom_namespaces(["Analytics"])
        with get_db_context() as db:
            assert db.get(CatalogNamespace, schema_id) is None

        from flowfile_core.project.importer import import_project

        import_project(root, OWNER)

        with get_db_context() as db:
            reg = db.query(FlowRegistration).filter_by(flow_uuid=flow_uuid).first()
            assert reg is not None
            ns = db.get(CatalogNamespace, reg.namespace_id)
            assert ns is not None and ns.name == "Marts"  # restored in place, NOT Local Flows
            parent = db.get(CatalogNamespace, ns.parent_id)
            assert parent is not None and parent.name == "Analytics"
    finally:
        _delete_flow(flow_uuid)
        _cleanup_custom_namespaces(["Analytics"])
        project_sync.close_project(OWNER)


def test_namespaced_flow_projection_round_trips_byte_identical(tmp_path):
    project_sync.close_project(OWNER)
    flow_uuid, _, _ = _make_flow_in_namespace(tmp_path, "det_ns_flow", "DetCat", "DetSchema")
    root = tmp_path / "project"

    def snap() -> dict[str, bytes]:
        s = _snapshot(root)
        ns = root / "namespaces.yaml"
        if ns.exists():
            s["namespaces.yaml"] = ns.read_bytes()
        return s

    try:
        project_sync.init_project(str(root), "Det NS Test", OWNER)
        snap1 = snap()
        with get_db_context() as db:
            projection.project_all(db, root, OWNER)
        assert snap() == snap1, "re-projecting an unchanged namespaced flow must be byte-identical"

        from flowfile_core.project.importer import import_project

        import_project(root, OWNER)
        with get_db_context() as db:
            projection.project_all(db, root, OWNER)
        assert snap() == snap1, "project->import->project must round-trip with a namespaced flow"
    finally:
        _delete_flow(flow_uuid)
        _cleanup_custom_namespaces(["DetCat"])
        project_sync.close_project(OWNER)


def test_reload_prunes_emptied_custom_namespace(tmp_path):
    project_sync.close_project(OWNER)
    flow_uuid, _, _ = _make_flow_in_namespace(tmp_path, "del_flow", "DelCat", "DelSchema")
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Del Test", OWNER)
        assert (root / "flows" / "del_flow.flow.yaml").exists()

        # Simulate restoring a version that no longer has the flow or its custom namespace.
        for p in (root / "flows").glob("*.flow.yaml"):
            p.unlink()
        from flowfile_core.project.normalize import write_yaml

        write_yaml(root / "namespaces.yaml", {"namespaces": []})

        project_sync.reload_from_disk(OWNER)

        with get_db_context() as db:
            assert db.query(FlowRegistration).filter_by(flow_uuid=flow_uuid).first() is None
            assert db.query(CatalogNamespace).filter_by(name="DelSchema").first() is None
            assert db.query(CatalogNamespace).filter_by(name="DelCat", parent_id=None).first() is None
            # Seeded public namespaces are never pruned.
            general = db.query(CatalogNamespace).filter_by(name="General", parent_id=None).first()
            assert general is not None
            assert db.query(CatalogNamespace).filter_by(name="Local Flows", parent_id=general.id).first() is not None
    finally:
        _delete_flow(flow_uuid)
        _cleanup_custom_namespaces(["DelCat"])
        project_sync.close_project(OWNER)


def test_namespace_hook_updates_manifest_under_active_project(tmp_path):
    project_sync.close_project(OWNER)
    root = tmp_path / "project"
    ns_file = root / "namespaces.yaml"

    def catalogs() -> dict:
        return {c["catalog"]: c for c in yaml.safe_load(ns_file.read_text(encoding="utf-8"))["namespaces"]}

    try:
        project_sync.init_project(str(root), "Hook Test", OWNER)
        assert "General" in catalogs() and "HookCat" not in catalogs()

        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            cat_id = svc.create_namespace(name="HookCat", owner_id=OWNER).id
            svc.create_namespace(name="HookSchema", owner_id=OWNER, parent_id=cat_id).id
        assert {s["name"] for s in catalogs()["HookCat"]["schemas"]} == {"HookSchema"}

        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            schema_id = svc.repo.get_namespace_by_name("HookSchema", cat_id).id
            svc.delete_namespace(schema_id)
            svc.delete_namespace(cat_id)
        assert "HookCat" not in catalogs() and "General" in catalogs()  # seeded untouched
    finally:
        _cleanup_custom_namespaces(["HookCat"])
        project_sync.close_project(OWNER)


def test_flow_move_reprojects_namespace(tmp_path):
    project_sync.close_project(OWNER)
    flow_uuid, _, catalog_id = _make_flow_in_namespace(tmp_path, "mv_flow", "MoveCat", "SchemaA")
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Move Test", OWNER)
        doc = yaml.safe_load((root / "flows" / "mv_flow.flow.yaml").read_text(encoding="utf-8"))
        assert doc["namespace"] == {"catalog": "MoveCat", "schema": "SchemaA"}

        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            schema_b_id = svc.create_namespace(name="SchemaB", owner_id=OWNER, parent_id=catalog_id).id
            reg = db.query(FlowRegistration).filter_by(flow_uuid=flow_uuid).first()
            svc.update_flow(registration_id=reg.id, requesting_user_id=OWNER, namespace_id=schema_b_id)

        doc = yaml.safe_load((root / "flows" / "mv_flow.flow.yaml").read_text(encoding="utf-8"))
        assert doc["namespace"] == {"catalog": "MoveCat", "schema": "SchemaB"}
    finally:
        _delete_flow(flow_uuid)
        _cleanup_custom_namespaces(["MoveCat"])
        project_sync.close_project(OWNER)


def test_flow_filed_by_flowfile_name_with_catalog_label(tmp_path):
    project_sync.close_project(OWNER)
    flow_uuid = _make_flow_named(tmp_path, "fa_internal", "FA Catalog")
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "FileName Test", OWNER)
        # Filed by the intrinsic flowfile_name, NOT the catalog name; no suffixed duplicate.
        assert (root / "flows" / "fa_internal.flow.yaml").exists()
        assert not (root / "flows" / "FA_Catalog.flow.yaml").exists()
        assert list((root / "flows").glob("fa_internal_*.flow.yaml")) == []
        doc = yaml.safe_load((root / "flows" / "fa_internal.flow.yaml").read_text(encoding="utf-8"))
        assert doc["catalog_name"] == "FA Catalog"
        assert doc["flowfile_name"] == "fa_internal"
    finally:
        _delete_flow(flow_uuid)
        project_sync.close_project(OWNER)


def test_no_rename_churn_after_restore(tmp_path):
    from flowfile_core.project import git_ops

    if not git_ops.git_available():
        pytest.skip("git not available")
    project_sync.close_project(OWNER)
    flow_uuid = _make_flow_named(tmp_path, "fb_internal", "FB Catalog", flow_id=424242)
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Churn Test", OWNER)
        sha_v1 = git_ops.head_sha(root)
        before = (root / "flows" / "fb_internal.flow.yaml").read_bytes()

        second_uuid = _make_flow_named(tmp_path, "fb_second", "FB Second", flow_id=424243)
        project_sync.save_version(OWNER, "add second")
        assert (root / "flows" / "fb_second.flow.yaml").exists()

        # Restore to v1, then Save again — the original bug surfaced on this post-restore save.
        project_sync.restore_version(OWNER, sha_v1, "v1")
        project_sync.save_version(OWNER, "after restore")

        # No rename churn: same single file, byte-identical, no suffixed twin, second flow pruned.
        assert (root / "flows" / "fb_internal.flow.yaml").read_bytes() == before
        assert list((root / "flows").glob("fb_internal_*.flow.yaml")) == []
        assert list((root / "flows").glob("fb_second*.flow.yaml")) == []
        # Catalog display name preserved across the round-trip.
        with get_db_context() as db:
            reg = db.query(FlowRegistration).filter_by(flow_uuid=flow_uuid).first()
            assert reg is not None and reg.name == "FB Catalog"
    finally:
        _delete_flow(flow_uuid)
        project_sync.close_project(OWNER)


def test_save_version_prunes_orphan_file(tmp_path):
    project_sync.close_project(OWNER)
    flow_uuid = _make_flow_named(tmp_path, "fc_internal", "FC Catalog")
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Prune Test", OWNER)
        assert (root / "flows" / "fc_internal.flow.yaml").exists()

        _delete_flow(flow_uuid)
        project_sync.save_version(OWNER, "removed flow")
        # Exact-mirror: the orphaned project file is swept on Save version.
        assert not (root / "flows" / "fc_internal.flow.yaml").exists()
    finally:
        project_sync.close_project(OWNER)


def test_root_commit_changes_lists_files(tmp_path):
    from flowfile_core.project import git_ops

    if not git_ops.git_available():
        pytest.skip("git not available")
    project_sync.close_project(OWNER)
    flow_uuid = _make_flow_named(tmp_path, "fd_internal", "FD Catalog")
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Root Diff Test", OWNER)
        sha = git_ops.head_sha(root)
        changes = git_ops.changes_in(root, sha)
        # The initial commit must list its files (not show an empty "No file changes").
        assert changes
        paths = {c["path"] for c in changes}
        assert "project.yaml" in paths
        assert any(p.startswith("flows/") for p in paths)
    finally:
        _delete_flow(flow_uuid)
        project_sync.close_project(OWNER)
