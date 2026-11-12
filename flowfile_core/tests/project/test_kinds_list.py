"""A version lists its kinds in ``project.yaml``, and an import applies only the kinds it lists.

Proven on the every-kind install of ``test_kinds.py``, real git and the test database: init and save write
``project_format: 2`` with the kinds a save exports; a format-1 folder opens and lists its kinds at the next
save; a kind a version leaves out is neither imported nor pruned; a listed kind no registry entry knows keeps
its committed files and is reported; a newer format is refused before anything changes; an entry or file owned
by another user, or under a reserved ``@<username>/`` level, is reported as not applied. Same-named flows and
notebooks are named by uuid order whatever the folder held, and project YAML keeps its list style whatever
``yaml.Dumper`` was given.
"""

import json
import shutil
from pathlib import Path

import pytest
import yaml
from git import Repo

from flowfile_core.catalog.services import notebook_store
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import (
    CatalogDashboard,
    CatalogNotebook,
    CatalogTable,
    FlowRegistration,
    Kernel,
    User,
    WorkspaceProject,
)
from flowfile_core.project import git_ops, importer, kinds, project_sync
from flowfile_core.project.manifest import read_manifest
from flowfile_core.project.normalize import dump_yaml
from flowfile_core.routes.project import _import_error
from tests.project.test_kinds import (
    FIXTURE,
    KERNEL_ID,
    USERNAME,
    _flow_file,
    _on_disk,
    _project_all,
    build_every_kind,
    drop_owner,
)

TWELVE = [
    "secrets",
    "database_connection",
    "cloud_connection",
    "namespaces",
    "flow",
    "schedule",
    "tables",
    "models",
    "kernels",
    "visualizations",
    "dashboards",
    "notebook",
]
TEN = [k for k in TWELVE if k not in ("tables", "models")]


@pytest.fixture(autouse=True)
def _require_git():
    if not git_ops.git_available():
        pytest.skip("git not available")


def _drop_owner_and_projects() -> None:
    """``drop_owner`` after the owner's project rows, which a server catalog's foreign key would refuse."""
    with get_db_context() as db:
        user = db.query(User).filter_by(username=USERNAME).first()
        if user is not None:
            db.query(WorkspaceProject).filter_by(owner_id=user.id).delete(synchronize_session=False)
            db.commit()
    drop_owner()


def _importable_dashboard(owner_id: int) -> None:
    """Give the builder's dashboard a layout an import validates and stores as written, so it round-trips."""
    with get_db_context() as db:
        dashboard = db.query(CatalogDashboard).filter_by(created_by=owner_id).one()
        layout = json.loads(dashboard.layout_json)
        flt = {
            "id": "f1",
            "field_name": "id",
            "kind": "categorical",
            "datasource_id": layout["filters"][0]["datasource_id"],
        }
        dashboard.layout_json = json.dumps({"tiles": layout["tiles"], "grid": layout["grid"], "filters": [flt]})
        db.commit()


@pytest.fixture
def recorded(tmp_path):
    owner_id = build_every_kind(tmp_path)
    try:
        yield owner_id
    finally:
        drop_owner()


@pytest.fixture
def every_kind(tmp_path):
    owner_id = build_every_kind(tmp_path)
    _importable_dashboard(owner_id)
    try:
        yield owner_id
    finally:
        project_sync.close_project(owner_id)
        _drop_owner_and_projects()


def _init(root: Path, owner_id: int, track: bool = True) -> None:
    project_sync.init_project(str(root), "kinds", owner_id, track)


def _raw_manifest(root: Path) -> dict:
    return yaml.safe_load((root / "project.yaml").read_text(encoding="utf-8"))


def _write_manifest(root: Path, data: dict) -> None:
    (root / "project.yaml").write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")


def _commit_everything(root: Path, message: str) -> str:
    """Commit the whole working tree, files no kind owns included, as a hand-made version would be."""
    repo = Repo(str(root))
    repo.git.add("-A")
    return repo.index.commit(message).hexsha


def _kernel_ids(owner_id: int) -> set[str]:
    with get_db_context() as db:
        return {k.id for k in db.query(Kernel).filter_by(user_id=owner_id)}


def _flow_uuids(owner_id: int) -> set[str]:
    with get_db_context() as db:
        return {r.flow_uuid for r in db.query(FlowRegistration).filter_by(owner_id=owner_id)}


def test_init_lists_every_kind_and_the_toggle_moves_the_data_kinds(tmp_path, every_kind):
    root = tmp_path / "project"
    _init(root, every_kind)
    raw = _raw_manifest(root)
    assert raw["project_format"] == 2
    assert raw["kinds"] == TWELVE
    assert "track_data_artifacts" not in raw

    project_sync.update_settings(every_kind, False)
    assert _raw_manifest(root)["kinds"] == TEN
    assert read_manifest(root).track_data_artifacts is False
    assert not (root / "tables.yaml").exists() and not (root / "models.yaml").exists()
    project_sync.update_settings(every_kind, True)
    assert _raw_manifest(root)["kinds"] == TWELVE
    assert read_manifest(root).track_data_artifacts is True
    assert (root / "tables.yaml").exists() and (root / "models.yaml").exists()

    untracked = tmp_path / "untracked"
    _init(untracked, every_kind, track=False)
    assert _raw_manifest(untracked)["kinds"] == TEN


@pytest.mark.parametrize("tracked", [True, False])
def test_a_format_1_folder_opens_and_lists_its_kinds_at_the_next_save(tmp_path, every_kind, tracked):
    root = tmp_path / "project"
    _init(root, every_kind)
    old = {"project_format": 1, "name": "kinds", "project_id": "p1", "created_with_version": "0.1.0"}
    if not tracked:
        old["track_data_artifacts"] = False
    _write_manifest(root, old)
    git_ops.commit_all(root, "format 1")
    project_sync.close_project(every_kind)

    _, result = project_sync.open_project(str(root), every_kind)
    assert result.imported_flows == 2
    expected = TWELVE if tracked else TEN
    assert read_manifest(root).kinds == expected
    assert ("tables" in result.by_kind) is tracked

    project_sync.save_version(every_kind, "next save")
    raw = _raw_manifest(root)
    assert raw["project_format"] == 2
    assert raw["kinds"] == expected
    assert "track_data_artifacts" not in raw
    assert raw["project_id"] == "p1"
    assert not git_ops.is_dirty(root)


def test_a_kind_the_version_leaves_out_is_neither_imported_nor_pruned(tmp_path, every_kind):
    root = tmp_path / "project"
    _init(root, every_kind)
    first = git_ops.head_sha(root)
    data = _raw_manifest(root)
    data["kinds"] = [k for k in TWELVE if k != "kernels"]
    _write_manifest(root, data)
    (root / "kernels.yaml").write_text("kernels: []\n", encoding="utf-8")
    without_kernels = git_ops.commit_all(root, "no kernels listed")
    git_ops.restore(root, first)
    git_ops.commit_all(root, "back to the first")

    restored = project_sync.restore_version(every_kind, without_kernels, force=True)
    assert _kernel_ids(every_kind) == {KERNEL_ID}
    assert "kernels" not in restored.by_kind
    assert (root / "kernels.yaml").read_text(encoding="utf-8") == "kernels: []\n"

    reloaded = project_sync.reload_from_disk(every_kind, force=True)
    assert _kernel_ids(every_kind) == {KERNEL_ID}
    assert "kernels" not in reloaded.by_kind
    assert reloaded.unknown_kinds == []


def test_a_listed_kind_no_entry_knows_keeps_its_files_and_is_reported(tmp_path, every_kind):
    root = tmp_path / "project"
    _init(root, every_kind)
    first = git_ops.head_sha(root)
    data = _raw_manifest(root)
    data["kinds"] = TWELVE + ["widgets"]
    _write_manifest(root, data)
    (root / "widgets").mkdir()
    widget = "kind: widget\nname: w\nsize: [1, 2]\n"
    (root / "widgets" / "w.yaml").write_text(widget, encoding="utf-8")
    with_widgets = _commit_everything(root, "widgets")
    project_sync.close_project(every_kind)

    _, opened = project_sync.open_project(str(root), every_kind)
    assert opened.unknown_kinds == ["widgets"]

    project_sync.save_version(every_kind, "a save")
    assert (root / "widgets" / "w.yaml").read_text(encoding="utf-8") == widget
    assert _raw_manifest(root)["kinds"] == TWELVE + ["widgets"]
    assert Repo(str(root)).git.show("HEAD:widgets/w.yaml") + "\n" == widget

    older = project_sync.restore_version(every_kind, first)
    assert older.unknown_kinds == []
    back = project_sync.restore_version(every_kind, with_widgets)
    assert back.unknown_kinds == ["widgets"]
    assert (root / "widgets" / "w.yaml").read_text(encoding="utf-8") == widget
    assert _raw_manifest(root)["kinds"] == TWELVE + ["widgets"]

    reloaded = project_sync.reload_from_disk(every_kind)
    assert reloaded.unknown_kinds == ["widgets"]
    assert reloaded.to_dict()["unknown_kinds"] == ["widgets"]


def _newer(root: Path) -> None:
    data = _raw_manifest(root)
    data["project_format"] = 3
    _write_manifest(root, data)


def _kinds_as_text(root: Path) -> None:
    data = _raw_manifest(root)
    data["kinds"] = "flows"
    _write_manifest(root, data)


def _db_state(owner_id: int) -> tuple:
    with get_db_context() as db:
        tables = sorted(t.name for t in db.query(CatalogTable).filter_by(owner_id=owner_id))
    return sorted(_flow_uuids(owner_id)), tables, sorted(_kernel_ids(owner_id))


def _refused(call) -> str:
    with pytest.raises(ValueError) as caught:
        call()
    assert _import_error(caught.value).status_code == 422
    assert "project.yaml" in str(caught.value)
    return str(caught.value)


@pytest.mark.parametrize("spoil", [_newer, _kinds_as_text], ids=["newer_format", "kinds_not_a_list"])
def test_a_manifest_this_flowfile_cannot_read_is_refused_with_nothing_changed(tmp_path, every_kind, spoil):
    root = tmp_path / "project"
    _init(root, every_kind)
    state = _db_state(every_kind)

    spoil(root)
    bad = git_ops.commit_all(root, "unreadable manifest")
    git_ops.restore(root, Repo(str(root)).commit("HEAD~1").hexsha)
    good = git_ops.commit_all(root, "readable again")
    files = _on_disk(root)
    message = _refused(lambda: project_sync.restore_version(every_kind, bad, force=True))
    if spoil is _newer:
        assert "newer Flowfile" in message and "3" in message and "2" in message
    assert git_ops.head_sha(root) == good
    assert _on_disk(root) == files
    assert _db_state(every_kind) == state

    spoil(root)
    files = _on_disk(root)
    _refused(lambda: project_sync.reload_from_disk(every_kind, force=True))
    assert _on_disk(root) == files
    assert _db_state(every_kind) == state

    project_sync.close_project(every_kind)
    _refused(lambda: project_sync.open_project(str(root), every_kind))
    assert _on_disk(root) == files
    assert _db_state(every_kind) == state


def test_by_kind_counts_what_an_open_into_a_fresh_owner_applied(tmp_path, every_kind):
    root = tmp_path / "project"
    _init(root, every_kind)
    project_sync.close_project(every_kind)
    copy = tmp_path / "copy"
    shutil.copytree(root, copy)
    _drop_owner_and_projects()
    with get_db_context() as db:
        user = User(username=USERNAME, email=f"{USERNAME}@flowfile.app", hashed_password="x")
        db.add(user)
        db.commit()
        fresh = user.id
    try:
        _, result = project_sync.open_project(str(copy), fresh)
        assert result.by_kind == {
            "secrets": 1,
            "database_connection": 1,
            "cloud_connection": 1,
            "namespaces": 2,
            "flow": 2,
            "schedule": 2,
            "tables": 2,
            "models": 1,
            "kernels": 1,
            "visualizations": 1,
            "dashboards": 1,
            "notebook": 1,
        }
        assert (result.imported_flows, result.imported_connections, result.imported_schedules) == (2, 2, 2)
        imported = result.to_dict()["imported"]
        assert (imported["flows"], imported["connections"], imported["schedules"]) == (2, 2, 2)
        assert imported["by_kind"] == result.by_kind
        assert result.not_applied == [] and result.unknown_kinds == []
    finally:
        project_sync.close_project(fresh)


def _flow_text_with(root: Path, uuid: str, name: str, owner: str | None) -> str:
    data = yaml.safe_load((root / "flows" / "flow_b.flow.yaml").read_text(encoding="utf-8"))
    data["flow_uuid"] = uuid
    data["catalog_name"] = data["flowfile_name"] = name
    if owner is not None:
        data["owner"] = owner
    return yaml.safe_dump(data, sort_keys=False)


def test_what_another_owner_holds_is_reported_as_not_applied(tmp_path, every_kind):
    root = tmp_path / "project"
    _init(root, every_kind)
    tables = yaml.safe_load((root / "tables.yaml").read_text(encoding="utf-8"))
    view = next(t for t in tables["tables"] if t["name"] == "kinds_view")
    tables["tables"] += [
        {**view, "name": "foreign_view", "owner": "alice"},
        {**view, "name": "own_view", "owner": USERNAME},
    ]
    (root / "tables.yaml").write_text(yaml.safe_dump(tables, sort_keys=False), encoding="utf-8")
    foreign, mine, reserved = (
        "1e5c0f00-0000-4000-8000-000000000001",
        "1e5c0f00-0000-4000-8000-000000000002",
        "1e5c0f00-0000-4000-8000-000000000003",
    )
    (root / "flows" / "foreign.flow.yaml").write_text(_flow_text_with(root, foreign, "foreign", "alice"), "utf-8")
    (root / "flows" / "mine.flow.yaml").write_text(_flow_text_with(root, mine, "mine", USERNAME), "utf-8")
    (root / "flows" / "@alice").mkdir()
    reserved_text = _flow_text_with(root, reserved, "x", None)
    (root / "flows" / "@alice" / "x.flow.yaml").write_text(reserved_text, "utf-8")

    result = project_sync.reload_from_disk(every_kind)
    assert sorted(result.not_applied) == [
        "flows/@alice/x.flow.yaml",
        "flows/foreign.flow.yaml",
        "tables.yaml: foreign_view",
    ]
    assert result.to_dict()["not_applied"] == sorted(result.not_applied)
    uuids = _flow_uuids(every_kind)
    assert mine in uuids and foreign not in uuids and reserved not in uuids
    with get_db_context() as db:
        names = {t.name for t in db.query(CatalogTable).filter_by(owner_id=every_kind)}
    assert "own_view" in names and "foreign_view" not in names

    project_sync.save_version(every_kind, "a save")
    assert (root / "flows" / "@alice" / "x.flow.yaml").read_text(encoding="utf-8") == reserved_text
    committed = set(git_ops.tree_blob_sizes(root, git_ops.head_sha(root)))
    assert "flows/@alice/x.flow.yaml" not in committed
    assert "flows/mine.flow.yaml" in committed

    (root / "flows" / "mine.flow.yaml").write_text(
        (root / "flows" / "mine.flow.yaml").read_text(encoding="utf-8") + "owner: 5\n", "utf-8"
    )
    with pytest.raises(importer.ProjectFileError, match="'owner' that is not text"):
        project_sync.reload_from_disk(every_kind)


def test_same_named_flows_and_notebooks_are_named_by_uuid_order(tmp_path, every_kind):
    early, late = "0a000000-0000-4000-8000-000000000001", "fa000000-0000-4000-8000-000000000001"
    nb_early, nb_late = "0b000000-0000-4000-8000-000000000001", "fb000000-0000-4000-8000-000000000001"
    for flow_uuid in (late, early):
        path = tmp_path / f"dup_{flow_uuid[:2]}.yaml"
        _flow_file(path, "dup", [])
        with get_db_context() as db:
            db.add(FlowRegistration(flow_uuid=flow_uuid, name="dup", flow_path=str(path), owner_id=every_kind))
            db.commit()
    for nb_uuid in (nb_late, nb_early):
        with get_db_context() as db:
            db.add(CatalogNotebook(notebook_uuid=nb_uuid, name="dupnb", owner_id=every_kind))
            db.commit()
        notebook_store.write_notebook_file(
            every_kind, nb_uuid, name="dupnb", description=None, namespace_name=None, default_kernel_id=None, cells=[]
        )
    root = tmp_path / "project"
    (root / "flows").mkdir(parents=True)
    (root / "notebooks").mkdir()
    (root / "flows" / "dup.flow.yaml").write_text(f"flow_uuid: {late}\n", encoding="utf-8")
    (root / "notebooks" / "dupnb.notebook.yaml").write_text(f"notebook_uuid: {nb_late}\n", encoding="utf-8")

    tree = _project_all(root, every_kind)
    assert yaml.safe_load(tree["flows/dup.flow.yaml"])["flow_uuid"] == early
    assert yaml.safe_load(tree[f"flows/dup_{late[:8]}.flow.yaml"])["flow_uuid"] == late
    assert yaml.safe_load(tree["notebooks/dupnb.notebook.yaml"])["notebook_uuid"] == nb_early
    assert yaml.safe_load(tree[f"notebooks/dupnb_{nb_late[:8]}.notebook.yaml"])["notebook_uuid"] == nb_late


def test_project_yaml_keeps_its_list_style_whatever_yaml_dumper_was_given(monkeypatch, recorded):
    monkeypatch.setitem(yaml.Dumper.yaml_representers, list, yaml.representer.SafeRepresenter.represent_list)
    assert yaml.dump({"a": [1]}, default_flow_style=False) == "a:\n- 1\n"

    assert dump_yaml({"a": [1]}) == "a: [1]\n"
    with get_db_context() as db:
        tree = kinds.snapshot(kinds.ExportContext(db=db, owner_id=recorded))
    assert tree == json.loads(FIXTURE.read_text(encoding="utf-8"))
