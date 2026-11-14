"""A restore or reload applies exactly its plan: the objects the plan names are created, changed and deleted, and
nothing else.

Proven on the every-kind install of ``test_kinds.py``, real git and the test database: for an added, an edited
and a removed entry or file of each kind, the plan's rows are what the database's export changed by after the
reload, and a second plan is empty; a version listing fewer kinds than the folder is compared and applied over
its own kinds; the "Restore:" commit is the export after the apply; a failing delete rolls the reload back; the
CLI prints the plan and applies it.
"""

import dataclasses
import uuid

import pytest
import yaml

from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import CatalogTable, DatabaseConnection, Kernel
from flowfile_core.project import git_ops, kinds, project_sync, service
from flowfile_core.project.normalize import deterministic_flow_id, dump_yaml
from tests.project.test_kinds import KERNEL_ID
from tests.project.test_kinds_list import every_kind  # noqa: F401
from tests.project.test_restore_safety import _delete_db_connection, _make_db_connection

# kind: (file, list key or None for a file per object, key field, an edit)
SPECS = {
    "secrets": ("secrets.yaml", "required_secrets", None, None),
    "database_connection": ("connections/database/kinds_db.yaml", None, "connection_name", ("host", "otherhost")),
    "cloud_connection": ("connections/cloud/kinds_cloud.yaml", None, "connection_name", ("aws_region", "eu-west-1")),
    "namespaces": ("namespaces.yaml", "namespaces", "catalog", ("description", "changed")),
    "flow": ("flows/flow_a.flow.yaml", None, "flow_uuid", ("flowfile_settings", {"execution_mode": "Performance"})),
    "schedule": ("schedules/flow_a.yaml", "schedules", "id", ("name", "renamed")),
    "tables": ("tables.yaml", "tables", "name", ("sql_query", "SELECT 2 AS id")),
    "models": ("models.yaml", "models", "version", ("tags", ["b"])),
    "kernels": ("kernels.yaml", "kernels", "id", ("name", "renamed")),
    "visualizations": ("visualizations.yaml", "visualizations", "viz_uuid", ("name", "renamed")),
    "dashboards": ("dashboards.yaml", "dashboards", "dashboard_uuid", ("name", "renamed")),
    "notebook": ("notebooks/kinds_notebook.notebook.yaml", None, "notebook_uuid", ("description", "changed")),
}
NEW = "0d0d0d0d-0000-4000-8000-000000000001"
NEW_KEYS = {"models": 2, "namespaces": "NewCatalog", "secrets": "kinds_new", "tables": "new_table"}
NEW_KEYS |= {"database_connection": "new_db", "cloud_connection": "new_cloud", "kernels": "new-kernel"}


@pytest.fixture(autouse=True)
def _require_git():
    if not git_ops.git_available():
        pytest.skip("git not available")


def _edit(root, kind: str, change: str) -> None:
    """Add an object of ``kind``, edit one, or remove the one added, written into the folder in the export's form."""
    path, list_key, key, edit = SPECS[kind]
    new_key = NEW_KEYS.get(kind, NEW)
    if list_key is None:
        folder, _, name = path.rpartition("/")
        if change == "removed":
            for p in (root / folder).glob("*.yaml"):
                if yaml.safe_load(p.read_text(encoding="utf-8"))[key] == new_key:
                    p.unlink()
            return
        data = yaml.safe_load((root / path).read_text(encoding="utf-8"))
        if change == "added":
            old_key = data[key]
            data[key] = new_key
            data.update({"name": "nb_new"} if kind == "notebook" else {})
            data.update(
                {"catalog_name": "flow_new", "flowfile_name": "flow_new", "flowfile_id": deterministic_flow_id(NEW)}
                if kind == "flow"
                else {}
            )
            text = dump_yaml(data)
            text = text.replace(f"${{secret:{old_key}", f"${{secret:{new_key}") if "connection" in kind else text
            (root / folder / f"new_{name}").write_text(text, encoding="utf-8")
        else:
            data[edit[0]] = edit[1] if kind != "flow" else {**data[edit[0]], **edit[1]}
            (root / path).write_text(dump_yaml(data), encoding="utf-8")
        return
    data = yaml.safe_load((root / path).read_text(encoding="utf-8"))
    items = data[list_key]
    if kind == "secrets":
        data[list_key] = items + [new_key] if change == "added" else [i for i in items if i != new_key]
    elif change == "added":
        item = dict(items[-1], **{key: new_key})
        item.update({"schemas": []} if kind == "namespaces" else {})
        item.update({"name": f"new_{kind}"} if kind in ("kernels", "visualizations", "dashboards") else {})
        items.append(item)
    elif change == "modified":
        items[-1][edit[0]] = edit[1]
    else:
        data[list_key] = [i for i in items if i[key] != new_key]
    (root / path).write_text(dump_yaml(data), encoding="utf-8")


def _keys(changes) -> list[tuple]:
    return sorted((c.kind, str(c.key), c.change) for c in changes)


CASES = [(k, c) for k in SPECS for c in ("added", "modified", "removed") if (k, c) != ("secrets", "modified")]


@pytest.mark.parametrize("kind, change", CASES)
def test_the_plan_is_what_the_reload_changes(tmp_path, every_kind, kind, change):  # noqa: F811
    root = tmp_path / "proj"
    project_sync.init_project(str(root), "apply", every_kind)
    if change == "removed":
        _edit(root, kind, "added")
        project_sync.reload_from_disk(every_kind)
        project_sync.save_version(every_kind, "added")
    _edit(root, kind, change)
    before = service._snapshot(root, every_kind)
    plan = service._plan_changes(root, every_kind, None)
    assert plan == [] if (kind, change) == ("secrets", "removed") else len(plan) == 1

    project_sync.reload_from_disk(every_kind)

    after = service._snapshot(root, every_kind)
    assert _keys(plan) == _keys(kinds.compare(before, after))
    assert project_sync.plan(every_kind) == []
    assert kind != "secrets" or NEW_KEYS["secrets"] in after["secrets.yaml"]


def _table_count(owner_id: int) -> int:
    with get_db_context() as db:
        return db.query(CatalogTable).filter_by(owner_id=owner_id).count()


def test_a_version_tracking_data_compared_from_a_folder_that_does_not_reports_no_table(tmp_path, every_kind):  # noqa: F811
    root = tmp_path / "proj"
    project_sync.init_project(str(root), "apply", every_kind, True)
    tracked = git_ops.head_sha(root)
    project_sync.update_settings(every_kind, False)
    project_sync.save_version(every_kind, "data off")
    assert project_sync.plan(every_kind, tracked) == []


def test_a_restore_of_a_version_not_listing_tables_deletes_none(tmp_path, every_kind):  # noqa: F811
    root = tmp_path / "proj"
    project_sync.init_project(str(root), "apply", every_kind, False)
    untracked = git_ops.head_sha(root)
    project_sync.update_settings(every_kind, True)
    project_sync.save_version(every_kind, "data on")
    count = _table_count(every_kind)
    assert not [r for r in project_sync.plan(every_kind, untracked) if r["kind"] == "catalog tables"]
    project_sync.restore_version(every_kind, untracked)
    assert _table_count(every_kind) == count


def test_after_a_restore_nothing_is_unsaved(tmp_path, every_kind):  # noqa: F811
    root = tmp_path / "proj"
    project_sync.init_project(str(root), "apply", every_kind)
    first = git_ops.head_sha(root)
    with get_db_context() as db:
        db.query(Kernel).filter_by(id=KERNEL_ID).update({"name": "renamed"})
        db.commit()
    project_sync.save_version(every_kind, "kernel renamed")
    project_sync.restore_version(every_kind, first)
    assert project_sync.has_uncommitted_changes(every_kind) is False


def test_a_failing_delete_rolls_the_reload_back(tmp_path, monkeypatch, every_kind):  # noqa: F811
    root = tmp_path / "proj"
    project_sync.init_project(str(root), "apply", every_kind)
    _edit(root, "database_connection", "modified")
    (root / "kernels.yaml").write_text(dump_yaml({"kernels": []}), encoding="utf-8")

    def fail(ctx, keys):
        raise RuntimeError("refused")

    monkeypatch.setitem(kinds._KINDS, "kernels", dataclasses.replace(kinds.get("kernels"), delete=fail))
    with pytest.raises(ValueError, match='kernels "kinds_kernel"'):
        project_sync.reload_from_disk(every_kind)
    with get_db_context() as db:
        assert db.query(Kernel).filter_by(id=KERNEL_ID).count() == 1
        assert db.query(DatabaseConnection).filter_by(user_id=every_kind).one().host == "localhost"


def test_the_cli_prints_the_plan_and_applies_it(tmp_path, capsys):
    from flowfile.__main__ import _run_project_command

    name = f"cli_{uuid.uuid4().hex[:6]}"
    root = tmp_path / "proj"
    project_sync.close_project(1)
    _make_db_connection(name)
    try:
        project_sync.init_project(str(root), "cli", 1)
        (root / "connections" / "database" / f"{name}.yaml").unlink()
        _run_project_command("plan", ".")
        assert (
            f'removed database connection "{name}": its stored password is not in any version'
            in capsys.readouterr().out
        )
        _run_project_command("apply", None)
        assert "Imported:" in capsys.readouterr().out
        with get_db_context() as db:
            assert db.query(DatabaseConnection).filter_by(connection_name=name).count() == 0
    finally:
        project_sync.close_project(1)
        _delete_db_connection(name)



def test_the_cli_plan_of_a_malformed_sha_exits_1_with_one_line(tmp_path, capsys):
    from flowfile.__main__ import _run_project_command

    root = tmp_path / "proj"
    project_sync.close_project(1)
    try:
        project_sync.init_project(str(root), "cli", 1)
        capsys.readouterr()
        with pytest.raises(SystemExit) as caught:
            _run_project_command("plan", "not-a-sha")
        assert caught.value.code == 1
        captured = capsys.readouterr()
        assert captured.out == ""
        assert len(captured.err.strip().splitlines()) == 1
        assert "Traceback" not in captured.err
    finally:
        project_sync.close_project(1)


def test_a_removed_schedule_file_removes_its_schedules(tmp_path, every_kind):  # noqa: F811
    root = tmp_path / "proj"
    project_sync.init_project(str(root), "apply", every_kind)
    (root / "schedules" / "flow_a.yaml").unlink()
    plan = service._plan_changes(root, every_kind, None)
    assert plan and {(c.kind, c.change) for c in plan} == {("schedule", "removed")}
    project_sync.reload_from_disk(every_kind)
    assert project_sync.plan(every_kind) == []


def test_a_version_without_a_kinds_list_keeps_a_kind_it_has_no_file_for(tmp_path, every_kind):  # noqa: F811
    root = tmp_path / "proj"
    project_sync.init_project(str(root), "apply", every_kind)
    manifest = yaml.safe_load((root / "project.yaml").read_text(encoding="utf-8"))
    manifest.pop("kinds")
    (root / "project.yaml").write_text(dump_yaml(manifest), encoding="utf-8")
    (root / "kernels.yaml").unlink()
    assert service._plan_changes(root, every_kind, None) == []
