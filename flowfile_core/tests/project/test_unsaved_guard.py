"""A restore or reload names the database changes the last version does not hold before it loses them.

Real git, a temp folder and the test catalog database: a flow written with no hook leaves the folder
clean, yet a restore or reload answers 409 naming it, and with ``force`` its file is kept in a
recovery commit (restore) or a recovery ref (reload). A hand edit with nothing unsaved in the
database reloads without ``force``. A SQLite catalog is snapshotted before a pruning import, a server
catalog is not, and two processes saving the same folder at once both succeed.
"""

import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest
import yaml
from fastapi import HTTPException

from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
from flowfile_core.database import backup
from flowfile_core.database.connection import get_database_path, get_db_context
from flowfile_core.database.models import FlowRegistration, Secret
from flowfile_core.flowfile.catalog_helpers import auto_register_flow
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.project import git_ops, project_sync
from flowfile_core.schemas import input_schema, schemas

OWNER = 1


@pytest.fixture(autouse=True)
def _require_git():
    if not git_ops.git_available():
        pytest.skip("git not available")


@pytest.fixture
def project(tmp_path):
    root = tmp_path / "proj"
    project_sync.close_project(OWNER)
    project_sync.init_project(str(root), "Guard", OWNER)
    project_sync.save_version(OWNER, "baseline")
    created: list[str] = []
    try:
        yield root, created
    finally:
        project_sync.close_project(OWNER)
        with get_db_context() as db:
            for flow_uuid in created:
                reg = db.query(FlowRegistration).filter_by(flow_uuid=flow_uuid).first()
                if reg is not None:
                    CatalogService(SQLAlchemyCatalogRepository(db)).delete_flow(reg.id, delete_file=True)


def _register_without_hooks(tmp_path: Path, name: str, flow_id: int) -> str:
    handler = FlowfileHandler()
    handler.register_flow(schemas.FlowSettings(flow_id=flow_id, name=name, path="."))
    graph = handler.get_flow(flow_id)
    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=flow_id, node_id=1, raw_data_format=input_schema.RawData.from_pylist([{"a": 1}])
        )
    )
    flow_path = str(tmp_path / f"{name}.yaml")
    graph.save_flow(flow_path)
    with project_sync.suppress_projection(OWNER):
        auto_register_flow(flow_path, name, OWNER)
    with get_db_context() as db:
        return db.query(FlowRegistration).filter_by(flow_path=flow_path).one().flow_uuid


def _tree(root: Path) -> dict[str, bytes]:
    return {
        p.relative_to(root).as_posix(): p.read_bytes()
        for p in sorted(root.rglob("*"))
        if p.is_file() and ".git" not in p.relative_to(root).parts
    }


def _unsaved_409(call) -> dict:
    with pytest.raises(HTTPException) as caught:
        call()
    assert caught.value.status_code == 409
    detail = caught.value.detail
    assert detail["code"] == "UNSAVED_CHANGES"
    return detail


def _flow_change(detail: dict, name: str) -> dict:
    [change] = [c for c in detail["changes"] if c["kind"] == "flow" and c["label"] == name]
    return change


def test_restore_names_a_flow_written_without_a_hook_and_keeps_it_with_force(project, tmp_path):
    root, created = project
    oldest = git_ops.log(root)[-1]["sha"]
    created.append(_register_without_hooks(tmp_path, "unhooked", 717171))
    assert not git_ops.is_dirty(root)
    before = _tree(root)

    detail = _unsaved_409(lambda: project_sync.restore_version(OWNER, oldest))
    change = _flow_change(detail, "unhooked")
    assert change["change"] == "added"
    assert detail["database_snapshot"] is (get_database_path() is not None)
    assert _tree(root) == before
    with get_db_context() as db:
        assert db.query(FlowRegistration).filter_by(flow_uuid=created[0]).count() == 1

    head = git_ops.head_sha(root)
    result = project_sync.restore_version(OWNER, oldest, force=True)
    assert result.recovery_sha
    assert git_ops.read_blob(root, result.recovery_sha, change["path"]) is not None
    history = [c["sha"] for c in git_ops.log(root)]
    assert result.recovery_sha in history
    assert history.index(result.recovery_sha) < history.index(head)
    assert git_ops.log(root)[history.index(result.recovery_sha)]["message"] == "Database before restore"


def test_reload_names_a_flow_written_without_a_hook_and_keeps_it_in_a_recovery_ref(project, tmp_path):
    root, created = project
    created.append(_register_without_hooks(tmp_path, "unhooked_reload", 717172))
    before = _tree(root)

    detail = _unsaved_409(lambda: project_sync.reload_from_disk(OWNER))
    change = _flow_change(detail, "unhooked_reload")
    assert _tree(root) == before

    head = git_ops.head_sha(root)
    result = project_sync.reload_from_disk(OWNER, force=True)
    assert git_ops.head_sha(root) == head
    refs = subprocess.run(
        ["git", "for-each-ref", "--format=%(objectname) %(refname)", "refs/flowfile/recovery/"],
        cwd=root,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()
    assert refs[0] == result.recovery_sha
    assert refs[1].startswith("refs/flowfile/recovery/")
    assert git_ops.read_blob(root, result.recovery_sha, change["path"]) is not None


def test_a_hand_edit_with_a_clean_database_reloads_without_force(project):
    root, _ = project
    secrets_file = root / "secrets.yaml"
    data = yaml.safe_load(secrets_file.read_text(encoding="utf-8")) or {}
    data["required_secrets"] = sorted([*(data.get("required_secrets") or []), "GUARD_HAND_EDIT"])
    secrets_file.write_text(yaml.safe_dump(data), encoding="utf-8")
    assert git_ops.is_dirty(root)

    project_sync.reload_from_disk(OWNER)
    try:
        with get_db_context() as db:
            assert db.query(Secret).filter_by(user_id=OWNER, name="GUARD_HAND_EDIT").count() == 1
    finally:
        with get_db_context() as db:
            db.query(Secret).filter_by(user_id=OWNER, name="GUARD_HAND_EDIT").delete()
            db.commit()


def test_a_pruning_import_snapshots_a_sqlite_catalog_only(project, monkeypatch):
    calls: list[str] = []
    real = backup.create_snapshot

    def spy(db_path, reason):
        calls.append(reason)
        return real(db_path, reason)

    monkeypatch.setattr(backup, "create_snapshot", spy)
    project_sync.reload_from_disk(OWNER)

    db_path = get_database_path()
    if db_path is None:
        assert calls == []
        return
    assert calls == ["pre_apply"]
    listed = [s for s in backup.list_snapshots(db_path) if s["kind"] == "pre_apply"]
    assert listed and ".pre-apply." in listed[0]["file_name"]


def test_two_processes_saving_one_folder_both_succeed(project, tmp_path):
    root, created = project
    created.append(_register_without_hooks(tmp_path, "concurrent", 717173))
    script = textwrap.dedent(
        f"""
        from flowfile_core.project import project_sync
        project_sync.save_version({OWNER}, "concurrent save")
        """
    )
    procs = [
        subprocess.Popen([sys.executable, "-c", script], env=dict(os.environ), stderr=subprocess.PIPE, text=True)
        for _ in range(2)
    ]
    outcomes = [(p.wait(timeout=300), p.stderr.read()) for p in procs]
    assert [code for code, _ in outcomes] == [0, 0], [err[-2000:] for _, err in outcomes]
    assert not (root / ".git" / "index.lock").exists()
    assert not git_ops.is_dirty(root)
    assert [c["message"] for c in git_ops.log(root)].count("concurrent save") == 1
