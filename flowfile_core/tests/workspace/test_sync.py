"""Integration tests for the workspace sync engine (export / apply / status).

These exercise the load-bearing guarantees: a clean, secret-free tree; an
idempotent + byte-stable round-trip (the determinism gate); and build-from-
scratch with env-refilled secrets.
"""

from __future__ import annotations

import shutil
import uuid
from pathlib import Path

import yaml
from pydantic import SecretStr

from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager.db_connections import store_database_connection
from flowfile_core.schemas.input_schema import FullDatabaseConnection
from flowfile_core.workspace.manifest import init_project
from flowfile_core.workspace.sync import WorkspaceSync

# -- builders ---------------------------------------------------------------


def _write_flow(flow_path: Path, name: str, abs_output: str | None) -> None:
    nodes = [
        {
            "id": 1,
            "type": "manual_input",
            "is_start_node": True,
            "x_position": 100,
            "y_position": 150,
            "setting_input": {"raw_data": []},
        }
    ]
    if abs_output:
        nodes.append(
            {
                "id": 2,
                "type": "output",
                "x_position": 400,
                "y_position": 150,
                "input_ids": [1],
                "setting_input": {
                    "output_settings": {
                        "name": "result.csv",
                        "directory": str(Path(abs_output).parent),
                        "file_type": "csv",
                        "abs_file_path": abs_output,
                    }
                },
            }
        )
    data = {
        "flowfile_version": "0.12.0",
        "flowfile_id": 999999,
        "flowfile_name": name,
        "flowfile_settings": {"description": "test flow", "execution_mode": "Development"},
        "nodes": nodes,
        "groups": [],
    }
    flow_path.parent.mkdir(parents=True, exist_ok=True)
    flow_path.write_text(yaml.safe_dump(data), encoding="utf-8")


def _register_flow(db, user_id: int, name: str, flow_path: Path, flow_uuid: str) -> db_models.FlowRegistration:
    reg = db_models.FlowRegistration(
        flow_uuid=flow_uuid, name=name, flow_path=str(flow_path), owner_id=user_id, namespace_id=None
    )
    db.add(reg)
    db.commit()
    db.refresh(reg)
    return reg


def _make_schedule(db, user_id: int, registration_id: int, name: str, cron: str) -> None:
    db.add(
        db_models.FlowSchedule(
            registration_id=registration_id,
            owner_id=user_id,
            enabled=True,
            name=name,
            schedule_type="cron",
            cron_expression=cron,
            cron_timezone="UTC",
        )
    )
    db.commit()


def _make_db_connection(db, user_id: int, name: str, password: str) -> None:
    store_database_connection(
        db,
        FullDatabaseConnection(
            connection_name=name,
            database_type="postgresql",
            username="etl",
            password=SecretStr(password),
            host="db.internal",
            port=5432,
            database="sales",
            ssl_enabled=True,
        ),
        user_id,
    )


def _snapshot(root: Path) -> dict[str, str]:
    """All tracked files (excludes the gitignored .flowfile/ fingerprints)."""
    out: dict[str, str] = {}
    for path in sorted(root.rglob("*")):
        if path.is_file() and ".flowfile" not in path.relative_to(root).parts:
            out[path.relative_to(root).as_posix()] = path.read_text(encoding="utf-8")
    return out


def _seed_project(db, user_id: int, project_root: str, flow_uuid: str, abs_output: str):
    from shared.storage_config import storage

    flow_path = storage.flows_directory / "sales.flow.yaml"
    _write_flow(flow_path, "sales", abs_output)
    reg = _register_flow(db, user_id, "sales", flow_path, flow_uuid)
    _make_schedule(db, user_id, reg.id, "nightly", "0 2 * * *")
    _make_db_connection(db, user_id, "prod_pg", "supersecret")
    init_project(project_root, "Test Project")
    return reg


# -- tests ------------------------------------------------------------------


def test_export_creates_secret_free_tree(ws_user, storage_tmp, project_root):
    from shared.storage_config import storage

    flow_uuid = str(uuid.uuid4())
    abs_output = str(storage.outputs_directory / "result.csv")
    with get_db_context() as db:
        _seed_project(db, ws_user, project_root, flow_uuid, abs_output)
        result = WorkspaceSync(db, ws_user, project_root).export()

    root = Path(project_root)
    assert (root / "flows" / "sales.flow.yaml").exists()
    assert (root / "flows" / "sales.layout.yaml").exists()
    assert (root / "connections" / "database" / "prod_pg.yaml").exists()
    assert (root / "schedules" / "sales.schedules.yaml").exists()
    assert (root / "secrets" / "secrets.manifest.yaml").exists()

    flow_text = (root / "flows" / "sales.flow.yaml").read_text()
    flow_doc = yaml.safe_load(flow_text)
    assert flow_doc["flow_uuid"] == flow_uuid
    assert flow_doc["flowfile_id"] == 0
    assert all(n["x_position"] == 0 and n["y_position"] == 0 for n in flow_doc["nodes"])
    assert "${outputs}/result.csv" in flow_text

    layout = yaml.safe_load((root / "flows" / "sales.layout.yaml").read_text())
    assert layout["nodes"][1] == {"x_position": 100, "y_position": 150}

    conn_text = (root / "connections" / "database" / "prod_pg.yaml").read_text()
    assert "${secret:prod_pg}" in conn_text
    assert "supersecret" not in conn_text

    manifest = yaml.safe_load((root / "secrets" / "secrets.manifest.yaml").read_text())
    assert [s["name"] for s in manifest["secrets"]] == ["prod_pg"]
    assert manifest["secrets"][0]["required_by"] == ["connections/database/prod_pg.yaml"]

    assert result.counts == {"flows": 1, "connections": 1, "schedules": 1, "secrets": 1}
    assert [s.name for s in result.secret_requirements] == ["prod_pg"]


def test_export_is_idempotent(ws_user, storage_tmp, project_root):
    from shared.storage_config import storage

    abs_output = str(storage.outputs_directory / "result.csv")
    with get_db_context() as db:
        _seed_project(db, ws_user, project_root, str(uuid.uuid4()), abs_output)
        sync = WorkspaceSync(db, ws_user, project_root)
        sync.export()
        second = sync.export()

    assert second.written == []
    assert second.removed == []
    assert len(second.unchanged) >= 5


def test_roundtrip_export_apply_export_is_byte_identical(ws_user, storage_tmp, project_root):
    """The determinism gate: export -> apply -> export must not change the tree."""
    from shared.storage_config import storage

    abs_output = str(storage.outputs_directory / "result.csv")
    with get_db_context() as db:
        _seed_project(db, ws_user, project_root, str(uuid.uuid4()), abs_output)
        sync = WorkspaceSync(db, ws_user, project_root)
        sync.export()
        before = _snapshot(Path(project_root))
        sync.apply()
        sync.export()
        after = _snapshot(Path(project_root))

    assert before == after


def test_build_from_scratch(ws_user, storage_tmp, project_root, monkeypatch):
    from shared.storage_config import storage

    flow_uuid = str(uuid.uuid4())
    abs_output = str(storage.outputs_directory / "result.csv")
    with get_db_context() as db:
        _seed_project(db, ws_user, project_root, flow_uuid, abs_output)
        WorkspaceSync(db, ws_user, project_root).export()

    # Wipe the runtime DB rows and the flow files -- simulate a fresh clone.
    with get_db_context() as db:
        db.query(db_models.FlowSchedule).filter(db_models.FlowSchedule.owner_id == ws_user).delete()
        db.query(db_models.FlowRegistration).filter(db_models.FlowRegistration.owner_id == ws_user).delete()
        db.query(db_models.DatabaseConnection).filter(db_models.DatabaseConnection.user_id == ws_user).delete()
        db.query(db_models.Secret).filter(db_models.Secret.user_id == ws_user).delete()
        db.commit()
    shutil.rmtree(storage.flows_directory, ignore_errors=True)

    # Refill the secret value from the environment.
    monkeypatch.setenv("FLOWFILE_SECRET_PROD_PG", "supersecret")

    with get_db_context() as db:
        result = WorkspaceSync(db, ws_user, project_root).apply()

    assert not result.missing_secrets
    assert result.counts.get("flow") == 1
    assert result.counts.get("schedule") == 1
    assert result.counts.get("database_connection") == 1

    with get_db_context() as db:
        secret = (
            db.query(db_models.Secret)
            .filter(db_models.Secret.user_id == ws_user, db_models.Secret.name == "prod_pg")
            .first()
        )
        assert secret is not None
        conn = (
            db.query(db_models.DatabaseConnection)
            .filter(
                db_models.DatabaseConnection.user_id == ws_user,
                db_models.DatabaseConnection.connection_name == "prod_pg",
            )
            .first()
        )
        assert conn is not None and conn.password_id == secret.id
        reg = db.query(db_models.FlowRegistration).filter(db_models.FlowRegistration.flow_uuid == flow_uuid).first()
        assert reg is not None and Path(reg.flow_path).exists()
        sched = db.query(db_models.FlowSchedule).filter(db_models.FlowSchedule.registration_id == reg.id).first()
        assert sched is not None and sched.cron_expression == "0 2 * * *"

    # The refilled secret decrypts (re-encrypted under this user's key).
    from flowfile_core.secret_manager.secret_manager import decrypt_secret, get_encrypted_secret

    encrypted = get_encrypted_secret(ws_user, "prod_pg")
    assert decrypt_secret(encrypted, ws_user).get_secret_value() == "supersecret"


def test_missing_secret_is_reported_not_fatal(ws_user, storage_tmp, project_root):
    with get_db_context() as db:
        _make_db_connection(db, ws_user, "prod_pg", "supersecret")
        init_project(project_root, "Test")
        WorkspaceSync(db, ws_user, project_root).export()

    with get_db_context() as db:
        db.query(db_models.DatabaseConnection).filter(db_models.DatabaseConnection.user_id == ws_user).delete()
        db.query(db_models.Secret).filter(db_models.Secret.user_id == ws_user).delete()
        db.commit()

    # No FLOWFILE_SECRET_PROD_PG in env -> reported missing, apply still succeeds.
    with get_db_context() as db:
        result = WorkspaceSync(db, ws_user, project_root).apply()

    assert [s.name for s in result.missing_secrets] == ["prod_pg"]
    with get_db_context() as db:
        conn = (
            db.query(db_models.DatabaseConnection)
            .filter(db_models.DatabaseConnection.connection_name == "prod_pg")
            .first()
        )
        assert conn is not None
        assert conn.password_id is None


def test_status_reports_drift(ws_user, storage_tmp, project_root):
    from shared.storage_config import storage

    abs_output = str(storage.outputs_directory / "result.csv")
    with get_db_context() as db:
        _seed_project(db, ws_user, project_root, str(uuid.uuid4()), abs_output)
        sync = WorkspaceSync(db, ws_user, project_root)
        sync.export()
        assert sync.status().drift.in_sync

        # Edit a file on disk -> files_ahead.
        conn_rel = "connections/database/prod_pg.yaml"
        conn_path = Path(project_root) / conn_rel
        conn_path.write_text(conn_path.read_text() + "\n# manual edit\n", encoding="utf-8")
        drift = sync.diff_drift()
        assert not drift.in_sync
        assert conn_rel in drift.files_ahead

        # Re-export to clear, then add a new connection in DB -> db_ahead.
        sync.export()
        _make_db_connection(db, ws_user, "second_pg", "pw2")
        drift2 = sync.diff_drift()
        assert "connections/database/second_pg.yaml" in drift2.db_ahead


def test_restore_recreates_deleted_flow(ws_user, storage_tmp, project_root):
    """Delete a flow → checkpoint → restore the earlier checkpoint → flow is back."""
    from flowfile_core.workspace.git_backend import GitBackend

    if not GitBackend.available():
        import pytest

        pytest.skip("git binary not installed")

    from shared.storage_config import storage

    uuid_x = str(uuid.uuid4())
    uuid_z = str(uuid.uuid4())
    with get_db_context() as db:
        for name, flow_uuid in [("flow_x", uuid_x), ("flow_z", uuid_z)]:
            flow_path = storage.flows_directory / f"{name}.flow.yaml"
            _write_flow(flow_path, name, None)
            _register_flow(db, ws_user, name, flow_path, flow_uuid)
        init_project(project_root, "Test")
        WorkspaceSync(db, ws_user, project_root).export()

    git = GitBackend(project_root)
    checkpoint_a = git.commit("A: both flows")

    # Remove flow_z, then checkpoint the removal (export prunes the file + commit).
    with get_db_context() as db:
        db.query(db_models.FlowRegistration).filter_by(flow_uuid=uuid_z).delete()
        db.commit()
        WorkspaceSync(db, ws_user, project_root).export()
    git.commit("B: removed flow_z")

    with get_db_context() as db:
        assert db.query(db_models.FlowRegistration).filter_by(flow_uuid=uuid_z).first() is None

    # Restore the earlier checkpoint and rebuild the DB from it.
    git.restore(checkpoint_a)
    git.commit("Restore to A")
    with get_db_context() as db:
        result = WorkspaceSync(db, ws_user, project_root).apply()
        assert result.counts.get("flow") == 2

    with get_db_context() as db:
        reg = db.query(db_models.FlowRegistration).filter_by(flow_uuid=uuid_z).first()
        assert reg is not None, "restored checkpoint must recreate the deleted flow"
        assert Path(reg.flow_path).exists()
