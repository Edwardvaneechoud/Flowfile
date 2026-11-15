"""An import is one database transaction, and writes its files only once that has committed.

A failure anywhere in a reload leaves every row of every table, ids included, as it was, and no
runtime flow file or notebook content file is written or removed. A flow that does not load fails
the import from a temp copy before anything is committed. On success, a flow's runtime file is
written and a pruned flow evicted only when the committed database already shows the change.
"""

from pathlib import Path
from uuid import uuid4

import pytest
import yaml
from sqlalchemy import select

from flowfile_core import flow_file_handler
from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
from flowfile_core.catalog.services import notebook_store
from flowfile_core.database.connection import engine, get_db_context
from flowfile_core.database.models import Base, CatalogNotebook, FlowRegistration
from flowfile_core.flowfile.catalog_helpers import auto_register_flow
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.project import git_ops, importer, manifest, project_sync
from flowfile_core.project.normalize import safe_stem
from flowfile_core.schemas import input_schema, schemas
from shared.storage_config import storage

OWNER = 1


@pytest.fixture(autouse=True)
def _require_git():
    if not git_ops.git_available():
        pytest.skip("git not available")


def _dump() -> dict[str, list[str]]:
    with get_db_context() as db:
        return {
            name: sorted(repr(tuple(row)) for row in db.execute(select(table)))
            for name, table in sorted(Base.metadata.tables.items())
        }


def _make_flow(tmp_path: Path, name: str, flow_id: int) -> str:
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
    auto_register_flow(flow_path, name, OWNER)
    with get_db_context() as db:
        return db.query(FlowRegistration).filter_by(flow_path=flow_path).one().flow_uuid


def _make_notebook(name: str) -> str:
    with get_db_context() as db:
        nb = CatalogNotebook(name=name, owner_id=OWNER)
        db.add(nb)
        db.commit()
        db.refresh(nb)
        nb_uuid = nb.notebook_uuid
    notebook_store.write_notebook_file(
        OWNER, nb_uuid, name=name, description=None, namespace_name=None, default_kernel_id=None, cells=[]
    )
    return nb_uuid


def _cleanup(flow_uuids: list[str], nb_uuids: list[str]) -> None:
    with get_db_context() as db:
        for flow_uuid in flow_uuids:
            reg = db.query(FlowRegistration).filter_by(flow_uuid=flow_uuid).first()
            if reg is not None:
                CatalogService(SQLAlchemyCatalogRepository(db)).delete_flow(reg.id, delete_file=True)
        for nb_uuid in nb_uuids:
            db.query(CatalogNotebook).filter_by(notebook_uuid=nb_uuid).delete()
            db.commit()
    for nb_uuid in nb_uuids:
        notebook_store.delete_notebook_file(OWNER, nb_uuid)


def _project_flow_file(root: Path, flow_uuid: str) -> Path:
    for p in manifest.flows_dir(root).glob("*.flow.yaml"):
        if yaml.safe_load(p.read_text(encoding="utf-8")).get("flow_uuid") == flow_uuid:
            return p
    raise AssertionError(f"no project file for flow {flow_uuid}")


def _add_flow_copy(root: Path, source_uuid: str, file_stem: str, **changes) -> tuple[str, str, Path]:
    """A new flow in the folder, copied from another one's file; returns its uuid, name and runtime path."""
    data = yaml.safe_load(_project_flow_file(root, source_uuid).read_text(encoding="utf-8"))
    new_uuid, name = str(uuid4()), f"copy_{uuid4().hex[:6]}"
    data.update(flow_uuid=new_uuid, flowfile_name=name, catalog_name=name, **changes)
    (manifest.flows_dir(root) / f"{file_stem}.flow.yaml").write_text(yaml.safe_dump(data), encoding="utf-8")
    return new_uuid, name, storage.flows_directory / "project" / f"{safe_stem(name)}_{new_uuid[:8]}.flow.yaml"


def _staged_leftovers(*folders: Path) -> list[Path]:
    return [p for folder in folders if folder.is_dir() for p in folder.glob(".*.staged")]


def _committed_path_registered(flow_path: str) -> bool:
    """Read on a connection of the app's engine, so only committed rows are seen."""
    with engine.connect() as connection:
        return (
            connection.execute(select(FlowRegistration.id).where(FlowRegistration.flow_path == flow_path)).first()
            is not None
        )


def test_a_failing_reload_changes_no_row_and_no_file(tmp_path, monkeypatch):
    project_sync.close_project(OWNER)
    root = tmp_path / "project"
    flow_uuid = _make_flow(tmp_path, f"tx_flow_{uuid4().hex[:6]}", 616161)
    nb_uuid = _make_notebook(f"tx-nb-{uuid4().hex[:6]}")
    pruned_nb_uuid = _make_notebook(f"tx-nb-pruned-{uuid4().hex[:6]}")
    new_uuid = None
    try:
        project_sync.init_project(str(root), "Apply Transaction", OWNER)
        project_sync.save_version(OWNER, "baseline")
        flow_file = _project_flow_file(root, flow_uuid)
        flow_file.write_text(
            flow_file.read_text(encoding="utf-8").replace("catalog_name: tx_flow_", "catalog_name: renamed_"),
            encoding="utf-8",
        )
        new_uuid, _, new_runtime = _add_flow_copy(root, flow_uuid, "zz_new")
        (root / "kernels.yaml").write_text(
            yaml.safe_dump({"kernels": [{"id": uuid4().hex, "name": "tx-kernel", "packages": []}]}), encoding="utf-8"
        )
        for nb_file in manifest.notebooks_dir(root).glob("*.notebook.yaml"):
            nb_data = yaml.safe_load(nb_file.read_text(encoding="utf-8"))
            if nb_data.get("notebook_uuid") == pruned_nb_uuid:
                nb_file.unlink()
            elif nb_data.get("notebook_uuid") == nb_uuid:
                nb_data["description"] = "edited in the folder"
                nb_file.write_text(yaml.safe_dump(nb_data), encoding="utf-8")

        with get_db_context() as db:
            runtime_flow = Path(db.query(FlowRegistration).filter_by(flow_uuid=flow_uuid).one().flow_path)
        nb_content = notebook_store._notebook_path(OWNER, nb_uuid)
        pruned_nb_content = notebook_store._notebook_path(OWNER, pruned_nb_uuid)
        flow_bytes, nb_bytes = runtime_flow.read_bytes(), nb_content.read_bytes()
        pruned_nb_bytes = pruned_nb_content.read_bytes()
        before = _dump()
        delete = importer._delete_removed

        def delete_then_fail(*args, **kwargs):
            delete(*args, **kwargs)
            raise RuntimeError("the delete failed")

        monkeypatch.setattr(importer, "_delete_removed", delete_then_fail)
        with pytest.raises(RuntimeError, match="the delete failed"):
            project_sync.reload_from_disk(OWNER)

        assert _dump() == before
        assert runtime_flow.read_bytes() == flow_bytes
        assert nb_content.read_bytes() == nb_bytes
        assert pruned_nb_content.read_bytes() == pruned_nb_bytes
        assert not new_runtime.exists()
        assert not _staged_leftovers(runtime_flow.parent, new_runtime.parent, nb_content.parent)
        assert not any(f.flow_settings.path == str(new_runtime) for f in flow_file_handler._flows.values())
    finally:
        monkeypatch.undo()
        project_sync.close_project(OWNER)
        _cleanup([u for u in (flow_uuid, new_uuid) if u], [nb_uuid, pruned_nb_uuid])


def test_a_reload_writes_flow_files_and_evicts_only_after_the_commit(tmp_path, monkeypatch):
    project_sync.close_project(OWNER)
    root = tmp_path / "project"
    kept_uuid = _make_flow(tmp_path, f"tx_kept_{uuid4().hex[:6]}", 626262)
    pruned_uuid = _make_flow(tmp_path, f"tx_pruned_{uuid4().hex[:6]}", 636363)
    with get_db_context() as db:
        pruned_path = db.query(FlowRegistration).filter_by(flow_uuid=pruned_uuid).one().flow_path
    new_uuid = None
    writes: list[tuple[str, bool]] = []
    evictions: list[tuple[str, bool]] = []
    move, evict = importer._move_into_place, flow_file_handler.evict_flow_by_path

    def recording_write(staged, path):
        if not Path(path).is_relative_to(storage.notebooks_directory):
            writes.append((str(path), _committed_path_registered(str(path))))
        move(staged, path)

    def recording_evict(flow_path):
        evictions.append((flow_path, _committed_path_registered(flow_path)))
        return evict(flow_path)

    try:
        project_sync.init_project(str(root), "Apply After Commit", OWNER)
        project_sync.save_version(OWNER, "baseline")
        _project_flow_file(root, pruned_uuid).unlink()
        new_uuid, _, new_runtime = _add_flow_copy(root, kept_uuid, "zz_new")

        monkeypatch.setattr(importer, "_move_into_place", recording_write)
        monkeypatch.setattr(flow_file_handler, "evict_flow_by_path", recording_evict)
        project_sync.reload_from_disk(OWNER)

        assert (str(new_runtime), True) in writes
        assert all(registered for _, registered in writes)
        assert evictions == [(pruned_path, False)]
        assert new_runtime.exists()
        assert not Path(pruned_path).exists()
        assert any(f.flow_settings.path == str(new_runtime) for f in flow_file_handler._flows.values())
    finally:
        monkeypatch.undo()
        project_sync.close_project(OWNER)
        _cleanup([u for u in (kept_uuid, pruned_uuid, new_uuid) if u], [])


def test_a_flow_that_does_not_load_rolls_the_apply_back(tmp_path):
    project_sync.close_project(OWNER)
    root = tmp_path / "project"
    flow_uuid = _make_flow(tmp_path, f"tx_load_{uuid4().hex[:6]}", 646464)
    try:
        project_sync.init_project(str(root), "Apply Load Check", OWNER)
        project_sync.save_version(OWNER, "baseline")
        flow_file = _project_flow_file(root, flow_uuid)
        flow_file.write_text(
            flow_file.read_text(encoding="utf-8").replace("catalog_name: tx_load_", "catalog_name: renamed_"),
            encoding="utf-8",
        )
        _, _, bad_runtime = _add_flow_copy(root, flow_uuid, "zz_bad", nodes="not a list of nodes")
        with get_db_context() as db:
            runtime_flow = Path(db.query(FlowRegistration).filter_by(flow_uuid=flow_uuid).one().flow_path)
        flow_bytes = runtime_flow.read_bytes()
        before = _dump()

        with pytest.raises(ValueError):
            project_sync.reload_from_disk(OWNER)

        assert _dump() == before
        assert runtime_flow.read_bytes() == flow_bytes
        assert not bad_runtime.exists()
    finally:
        project_sync.close_project(OWNER)
        _cleanup([flow_uuid], [])


def test_a_flow_file_that_cannot_be_written_rolls_the_apply_back(tmp_path, monkeypatch):
    """The runtime file is written beside its path inside the transaction, so a full disk is a failed reload."""
    project_sync.close_project(OWNER)
    root = tmp_path / "project"
    flow_uuid = _make_flow(tmp_path, f"tx_disk_{uuid4().hex[:6]}", 656565)
    new_uuid = None
    try:
        project_sync.init_project(str(root), "Apply Disk Full", OWNER)
        project_sync.save_version(OWNER, "baseline")
        flow_file = _project_flow_file(root, flow_uuid)
        flow_file.write_text(
            flow_file.read_text(encoding="utf-8").replace("catalog_name: tx_disk_", "catalog_name: renamed_"),
            encoding="utf-8",
        )
        new_uuid, _, new_runtime = _add_flow_copy(root, flow_uuid, "zz_disk")
        with get_db_context() as db:
            runtime_flow = Path(db.query(FlowRegistration).filter_by(flow_uuid=flow_uuid).one().flow_path)
        flow_bytes = runtime_flow.read_bytes()
        before = _dump()
        mkstemp = importer.tempfile.mkstemp

        def disk_full_for_the_new_flow(*args, **kwargs):
            if kwargs.get("prefix", "").startswith(f".{new_runtime.name}."):
                raise OSError(28, "No space left on device")
            return mkstemp(*args, **kwargs)

        monkeypatch.setattr(importer.tempfile, "mkstemp", disk_full_for_the_new_flow)
        with pytest.raises(OSError, match="No space left"):
            project_sync.reload_from_disk(OWNER)

        assert _dump() == before
        assert runtime_flow.read_bytes() == flow_bytes
        assert not new_runtime.exists()
        assert not _staged_leftovers(runtime_flow.parent, new_runtime.parent)
    finally:
        monkeypatch.undo()
        project_sync.close_project(OWNER)
        _cleanup([u for u in (flow_uuid, new_uuid) if u], [])


def test_a_step_that_fails_after_the_commit_is_reported_and_a_reload_finishes_it(tmp_path, monkeypatch):
    project_sync.close_project(OWNER)
    root = tmp_path / "project"
    flow_uuid = _make_flow(tmp_path, f"tx_late_{uuid4().hex[:6]}", 666666)
    new_uuid = None
    try:
        project_sync.init_project(str(root), "Apply Late Failure", OWNER)
        project_sync.save_version(OWNER, "baseline")
        new_uuid, new_name, new_runtime = _add_flow_copy(root, flow_uuid, "zz_late")
        move = importer._move_into_place

        def failing_move(staged, path):
            if Path(path) == new_runtime:
                raise OSError("the move failed")
            move(staged, path)

        monkeypatch.setattr(importer, "_move_into_place", failing_move)
        result = project_sync.reload_from_disk(OWNER)

        assert f"Could not write the flow file of {new_name!r}: the move failed" in result.incomplete
        assert any(line.startswith(f"Could not load the flow {new_name!r}") for line in result.incomplete)
        assert result.to_dict()["incomplete"] == result.incomplete
        assert _committed_path_registered(str(new_runtime))
        assert not new_runtime.exists()
        assert not _staged_leftovers(new_runtime.parent)

        monkeypatch.undo()
        assert project_sync.reload_from_disk(OWNER).incomplete == []
        assert new_runtime.exists()
    finally:
        monkeypatch.undo()
        project_sync.close_project(OWNER)
        _cleanup([u for u in (flow_uuid, new_uuid) if u], [])


def test_a_restore_left_unfinished_records_no_version_and_keeps_the_folder(tmp_path, monkeypatch):
    """The export after it would leave the flow out; the folder keeps the version's file for a reload instead."""
    project_sync.close_project(OWNER)
    root = tmp_path / "project"
    kept_uuid = _make_flow(tmp_path, f"tx_rkept_{uuid4().hex[:6]}", 676767)
    gone_uuid = None
    try:
        project_sync.init_project(str(root), "Restore Late Failure", OWNER)
        gone_uuid = _make_flow(tmp_path, f"tx_rgone_{uuid4().hex[:6]}", 686868)
        with_gone = project_sync.save_version(OWNER, "with the flow")
        gone_file = _project_flow_file(root, gone_uuid).name
        _cleanup([gone_uuid], [])
        without_gone = project_sync.save_version(OWNER, "without the flow")
        move = importer._move_into_place

        def failing_move(staged, path):
            if gone_uuid[:8] in Path(path).name:
                raise OSError("the move failed")
            move(staged, path)

        monkeypatch.setattr(importer, "_move_into_place", failing_move)
        result = project_sync.restore_version(OWNER, with_gone)

        assert any(line.startswith("Could not write the flow file of") for line in result.incomplete)
        assert git_ops.head_sha(root) == without_gone
        assert (manifest.flows_dir(root) / gone_file).exists()

        monkeypatch.undo()
        assert project_sync.reload_from_disk(OWNER).incomplete == []
        with get_db_context() as db:
            assert Path(db.query(FlowRegistration).filter_by(flow_uuid=gone_uuid).one().flow_path).exists()
    finally:
        monkeypatch.undo()
        project_sync.close_project(OWNER)
        _cleanup([u for u in (kept_uuid, gone_uuid) if u], [])


def test_a_registration_that_fails_rolls_the_apply_back(tmp_path, monkeypatch):
    project_sync.close_project(OWNER)
    root = tmp_path / "project"
    flow_uuid = _make_flow(tmp_path, f"tx_reg_{uuid4().hex[:6]}", 696969)
    new_uuid = None
    try:
        project_sync.init_project(str(root), "Apply Registration", OWNER)
        project_sync.save_version(OWNER, "baseline")
        new_uuid, _, new_runtime = _add_flow_copy(root, flow_uuid, "zz_reg")
        before = _dump()

        def refuse(*args, **kwargs):
            raise RuntimeError("the registration failed")

        monkeypatch.setattr(CatalogService, "auto_register_flow", refuse)
        with pytest.raises(RuntimeError, match="the registration failed"):
            project_sync.reload_from_disk(OWNER)

        assert _dump() == before
        assert not new_runtime.exists()
    finally:
        monkeypatch.undo()
        project_sync.close_project(OWNER)
        _cleanup([u for u in (flow_uuid, new_uuid) if u], [])
