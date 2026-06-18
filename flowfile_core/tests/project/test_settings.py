"""Per-project track_data_artifacts toggle: projection, hooks, import/prune, and the DB mirror.

The toggle is all-or-nothing for catalog tables (tables.yaml) + global artifacts (models.yaml),
stored in project.yaml (source of truth) and mirrored onto the WorkspaceProject row. Default True
keeps the existing always-track behavior. When off, the two manifests are never written/committed,
the projection hooks no-op, and import leaves those DB resources entirely alone (never pruned).
"""

import json
from pathlib import Path
from uuid import uuid4

from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import CatalogTable, FlowRegistration, GlobalArtifact
from flowfile_core.flowfile.catalog_helpers import auto_register_flow
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.project import git_ops, project_sync, repository
from flowfile_core.project.manifest import read_manifest
from flowfile_core.schemas import input_schema, schemas
from shared.storage_config import storage

OWNER = 1


def _make_flow(tmp_path: Path, name: str) -> str:
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
        return db.query(FlowRegistration).filter_by(flow_path=flow_path).first().flow_uuid


def _reg_id(flow_uuid: str) -> int:
    with get_db_context() as db:
        return db.query(FlowRegistration).filter_by(flow_uuid=flow_uuid).first().id


def _make_table(name: str) -> int:
    import polars as pl

    table_dir = storage.catalog_tables_directory / f"{name}_{uuid4().hex[:8]}"
    df = pl.DataFrame({"id": [1, 2], "label": ["a", "b"]})
    df.write_delta(str(table_dir), mode="overwrite")
    schema = [{"name": c, "dtype": str(dt)} for c, dt in df.schema.items()]
    with get_db_context() as db:
        svc = CatalogService(SQLAlchemyCatalogRepository(db))
        out = svc.register_table_from_data(
            name=name,
            table_path=str(table_dir),
            owner_id=OWNER,
            storage_format="delta",
            schema=schema,
            row_count=df.height,
            column_count=df.width,
            size_bytes=0,
        )
    return out.id


def _make_artifact(name: str, src_reg_id: int) -> int:
    with get_db_context() as db:
        artifact = GlobalArtifact(
            name=name,
            owner_id=OWNER,
            version=1,
            status="active",
            source_registration_id=src_reg_id,
            serialization_format="joblib",
        )
        db.add(artifact)
        db.commit()
        db.refresh(artifact)
        return artifact.id


def _cleanup(table_name: str, model_name: str, flow_uuid: str) -> None:
    project_sync.close_project(OWNER)
    with get_db_context() as db:
        db.query(GlobalArtifact).filter_by(name=model_name).delete()
        db.commit()
    with get_db_context() as db:
        svc = CatalogService(SQLAlchemyCatalogRepository(db))
        for t in db.query(CatalogTable).filter_by(name=table_name).all():
            try:
                svc.delete_table(t.id, delete_file=True)
            except Exception:
                pass
    with get_db_context() as db:
        for reg in db.query(FlowRegistration).filter_by(flow_uuid=flow_uuid).all():
            db.delete(reg)
        db.commit()


def _db_flag(root: Path) -> bool:
    with get_db_context() as db:
        return repository.get_by_path(db, str(root)).track_data_artifacts


def test_tracking_on_by_default_projects_tables_and_models(tmp_path):
    project_sync.close_project(OWNER)
    table, model = f"on_tbl_{uuid4().hex[:6]}", f"on_mdl_{uuid4().hex[:6]}"
    flow_uuid = _make_flow(tmp_path, f"on_flow_{uuid4().hex[:6]}")
    _make_table(table)
    _make_artifact(model, _reg_id(flow_uuid))
    root = tmp_path / "project"
    try:
        proj = project_sync.init_project(str(root), "On Test", OWNER)
        assert proj.track_data_artifacts is True
        assert read_manifest(root).track_data_artifacts is True
        assert _db_flag(root) is True
        assert (root / "tables.yaml").exists()
        assert (root / "models.yaml").exists()
    finally:
        _cleanup(table, model, flow_uuid)


def test_tracking_off_at_init_excludes_tables_and_models(tmp_path):
    project_sync.close_project(OWNER)
    table, model = f"off_tbl_{uuid4().hex[:6]}", f"off_mdl_{uuid4().hex[:6]}"
    flow_uuid = _make_flow(tmp_path, f"off_flow_{uuid4().hex[:6]}")
    _make_table(table)
    _make_artifact(model, _reg_id(flow_uuid))
    root = tmp_path / "project"
    try:
        proj = project_sync.init_project(str(root), "Off Test", OWNER, track_data_artifacts=False)
        assert proj.track_data_artifacts is False
        assert read_manifest(root).track_data_artifacts is False
        assert _db_flag(root) is False
        assert not (root / "tables.yaml").exists()
        assert not (root / "models.yaml").exists()
        # Hooks must be no-ops while tracking is off (no file is (re)created on a table/model change).
        project_sync.tables_changed(OWNER)
        project_sync.artifacts_changed(OWNER)
        assert not (root / "tables.yaml").exists()
        assert not (root / "models.yaml").exists()
    finally:
        _cleanup(table, model, flow_uuid)


def test_update_settings_drops_then_recreates_files(tmp_path):
    project_sync.close_project(OWNER)
    table, model = f"tg_tbl_{uuid4().hex[:6]}", f"tg_mdl_{uuid4().hex[:6]}"
    flow_uuid = _make_flow(tmp_path, f"tg_flow_{uuid4().hex[:6]}")
    _make_table(table)
    _make_artifact(model, _reg_id(flow_uuid))
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Toggle Test", OWNER)
        assert (root / "tables.yaml").exists() and (root / "models.yaml").exists()

        project_sync.update_settings(OWNER, False)
        assert read_manifest(root).track_data_artifacts is False
        assert _db_flag(root) is False
        assert not (root / "tables.yaml").exists()
        assert not (root / "models.yaml").exists()

        project_sync.update_settings(OWNER, True)
        assert read_manifest(root).track_data_artifacts is True
        assert _db_flag(root) is True
        assert (root / "tables.yaml").exists()
        assert (root / "models.yaml").exists()
    finally:
        _cleanup(table, model, flow_uuid)


def test_reload_with_tracking_off_leaves_db_tables_and_models(tmp_path):
    """Safety: prune must not wipe untracked tables/models just because their manifests are absent."""
    project_sync.close_project(OWNER)
    table, model = f"safe_tbl_{uuid4().hex[:6]}", f"safe_mdl_{uuid4().hex[:6]}"
    flow_uuid = _make_flow(tmp_path, f"safe_flow_{uuid4().hex[:6]}")
    table_id = _make_table(table)
    artifact_id = _make_artifact(model, _reg_id(flow_uuid))
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Safe Test", OWNER, track_data_artifacts=False)
        assert not (root / "tables.yaml").exists() and not (root / "models.yaml").exists()

        project_sync.reload_from_disk(OWNER)  # prune=True

        with get_db_context() as db:
            assert db.query(CatalogTable).filter_by(id=table_id).first() is not None
            a = db.query(GlobalArtifact).filter_by(id=artifact_id).first()
            assert a is not None and a.status == "active"
    finally:
        _cleanup(table, model, flow_uuid)


def test_restore_to_tracked_version_readopts_setting(tmp_path):
    """Restoring to a version where tracking was ON brings the setting back everywhere. The restored
    manifest is authoritative, so the DB mirror AND the cached project follow it — not just the files.
    Regression: restore used to rebuild the files but leave the DB/cache flag stale (Settings UI then
    showed Off while project.yaml said On)."""
    project_sync.close_project(OWNER)
    table, model = f"rs_tbl_{uuid4().hex[:6]}", f"rs_mdl_{uuid4().hex[:6]}"
    flow_uuid = _make_flow(tmp_path, f"rs_flow_{uuid4().hex[:6]}")
    _make_table(table)
    _make_artifact(model, _reg_id(flow_uuid))
    root = tmp_path / "project"
    try:
        # v1: tracking ON — tables.yaml/models.yaml are committed.
        project_sync.init_project(str(root), "Restore Test", OWNER)
        tracked_sha = git_ops.head_sha(root)
        assert (root / "tables.yaml").exists() and (root / "models.yaml").exists()

        # v2: turn tracking OFF and commit it.
        project_sync.update_settings(OWNER, False)
        project_sync.save_version(OWNER, "Turn off tracking")
        assert _db_flag(root) is False
        assert project_sync.get_active_project(OWNER).track_data_artifacts is False
        assert not (root / "tables.yaml").exists()

        # Restore back to the tracked version.
        project_sync.restore_version(OWNER, tracked_sha)

        # The setting follows the restored version — manifest, DB mirror, AND in-memory cache.
        assert read_manifest(root).track_data_artifacts is True
        assert _db_flag(root) is True
        assert project_sync.get_active_project(OWNER).track_data_artifacts is True
        # The data-artifact manifests are back, too.
        assert (root / "tables.yaml").exists() and (root / "models.yaml").exists()
    finally:
        _cleanup(table, model, flow_uuid)


def test_open_project_mirrors_manifest_flag_into_db(tmp_path):
    project_sync.close_project(OWNER)
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "RT Test", OWNER, track_data_artifacts=False)
        project_sync.close_project(OWNER)

        proj, _ = project_sync.open_project(str(root), OWNER)
        assert proj.track_data_artifacts is False
        assert _db_flag(root) is False
        # The persisted manifest is authoritative and round-trips the flag.
        assert read_manifest(root).track_data_artifacts is False
    finally:
        project_sync.close_project(OWNER)


def test_manifest_omitting_flag_defaults_to_tracked(tmp_path):
    """An older project.yaml without the field keeps tracking (backward compatible)."""
    project_sync.close_project(OWNER)
    root = tmp_path / "project"
    root.mkdir(parents=True)
    (root / "project.yaml").write_text(
        json.dumps({"project_format": 1, "name": "Legacy", "project_id": "x", "created_with_version": "0.1"}),
        encoding="utf-8",
    )
    assert read_manifest(root).track_data_artifacts is True
