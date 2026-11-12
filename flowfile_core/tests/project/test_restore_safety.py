"""Restore and reload never remove what the files do not describe, and never half-apply a bad tree.

Three rules, each proven against real git and the test database: a flow registration the export
cannot write (no file, a kernel scratch flow) is never pruned; a kind the imported tree has no file
for is never pruned (an empty manifest still is); and every file is validated before the first
write, so a malformed tree answers 422 with the folder and the database unchanged.
"""

import json
import shutil
from pathlib import Path
from uuid import uuid4

import pytest
import yaml
from fastapi.testclient import TestClient
from sqlalchemy import func, select

from flowfile_core import main
from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
from flowfile_core.catalog.services import notebook_store
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import (
    Base,
    CatalogNotebook,
    CloudStorageConnection,
    DatabaseConnection,
    FlowRegistration,
    GlobalArtifact,
    Kernel,
    Secret,
    WorkspaceProject,
)
from flowfile_core.flowfile.catalog_helpers import auto_register_flow
from flowfile_core.flowfile.database_connection_manager.db_connections import (
    delete_cloud_connection,
    delete_database_connection,
    store_cloud_connection,
    store_database_connection,
)
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.project import git_ops, project_sync
from flowfile_core.schemas import input_schema, schemas
from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection

OWNER = 1


def _make_client() -> TestClient:
    with TestClient(main.app) as c:
        token = c.post("/auth/token").json()["access_token"]
    client = TestClient(main.app)
    client.headers = {"Authorization": f"Bearer {token}"}
    return client


client = _make_client()


@pytest.fixture(autouse=True)
def _require_git():
    if not git_ops.git_available():
        pytest.skip("git not available")


def _row_counts() -> dict[str, int]:
    with get_db_context() as db:
        return {t.name: db.execute(select(func.count()).select_from(t)).scalar() for t in Base.metadata.sorted_tables}


def _tree_bytes(root: Path) -> dict[str, bytes]:
    return {
        p.relative_to(root).as_posix(): p.read_bytes()
        for p in sorted(root.rglob("*"))
        if p.is_file() and ".git" not in p.relative_to(root).parts
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


def _add_registration(name: str, flow_path: str) -> str:
    with get_db_context() as db:
        reg = FlowRegistration(name=name, flow_path=flow_path, owner_id=OWNER)
        db.add(reg)
        db.commit()
        db.refresh(reg)
        return reg.flow_uuid


def _reg(flow_uuid: str) -> FlowRegistration | None:
    with get_db_context() as db:
        return db.query(FlowRegistration).filter_by(flow_uuid=flow_uuid).first()


def _delete_registrations(flow_uuids: list[str]) -> None:
    with get_db_context() as db:
        for flow_uuid in flow_uuids:
            reg = db.query(FlowRegistration).filter_by(flow_uuid=flow_uuid).first()
            if reg is None:
                continue
            db.query(GlobalArtifact).filter_by(source_registration_id=reg.id).delete()
            db.commit()
            if Path(reg.flow_path).suffix in (".yaml", ".yml", ".json"):
                CatalogService(SQLAlchemyCatalogRepository(db)).delete_flow(reg.id, delete_file=True)
            else:
                db.delete(reg)
                db.commit()


def _make_db_connection(name: str) -> None:
    with get_db_context() as db:
        store_database_connection(
            db,
            input_schema.FullDatabaseConnection(
                connection_name=name,
                database_type="postgresql",
                username="etl_reader",
                password="s3cret-pw",
                host="db.internal",
                port=5432,
                database="sales",
            ),
            OWNER,
        )


def _db_connection(name: str) -> tuple[DatabaseConnection | None, Secret | None]:
    with get_db_context() as db:
        conn = db.query(DatabaseConnection).filter_by(connection_name=name, user_id=OWNER).first()
        secret = db.get(Secret, conn.password_id) if conn is not None and conn.password_id else None
        return conn, secret


def _make_kernel(name: str) -> str:
    kernel_id = uuid4().hex
    with get_db_context() as db:
        db.add(
            Kernel(
                id=kernel_id,
                name=name,
                user_id=OWNER,
                packages="[]",
                resolved_packages="[]",
                cpu_cores=1.0,
                memory_gb=1.0,
                gpu=False,
                image_flavour="base",
            )
        )
        db.commit()
    return kernel_id


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


def _delete_kernel(kernel_id: str) -> None:
    with get_db_context() as db:
        db.query(Kernel).filter_by(id=kernel_id).delete()
        db.commit()


def _delete_notebook(nb_uuid: str) -> None:
    with get_db_context() as db:
        db.query(CatalogNotebook).filter_by(notebook_uuid=nb_uuid).delete()
        db.commit()
    notebook_store.delete_notebook_file(OWNER, nb_uuid)


def _delete_db_connection(name: str) -> None:
    try:
        with get_db_context() as db:
            delete_database_connection(db, name, OWNER)
    except Exception:
        pass


def test_unexportable_registrations_survive_restore_and_reload(tmp_path):
    """A registration whose file is gone and a kernel scratch flow are never in the files, so a
    restore to the very version just saved, and a reload, must change nothing at all."""
    project_sync.close_project(OWNER)
    root = tmp_path / "project"
    missing_uuid = _add_registration(f"gone_{uuid4().hex[:6]}", str(tmp_path / "gone.yaml"))
    scratch_uuid = _add_registration(f"_kernel_scratch_{uuid4().hex[:6]}", "<kernel:restore-safety>")
    flow_uuid = _make_flow(tmp_path, f"kept_{uuid4().hex[:6]}", 515151)
    with get_db_context() as db:
        artifact = GlobalArtifact(
            name=f"scratch_model_{uuid4().hex[:6]}",
            owner_id=OWNER,
            version=1,
            status="active",
            source_registration_id=db.query(FlowRegistration).filter_by(flow_uuid=scratch_uuid).one().id,
            serialization_format="pickle",
        )
        db.add(artifact)
        db.commit()
        artifact_id = artifact.id
    try:
        project_sync.init_project(str(root), "Restore Safety", OWNER)
        project_sync.save_version(OWNER, "baseline")
        head = git_ops.head_sha(root)
        before = _row_counts()

        project_sync.restore_version(OWNER, head)
        assert _row_counts() == before
        project_sync.reload_from_disk(OWNER)
        assert _row_counts() == before

        assert _reg(missing_uuid) is not None
        assert _reg(scratch_uuid) is not None
        assert _reg(flow_uuid) is not None
        with get_db_context() as db:
            assert db.get(GlobalArtifact, artifact_id).status == "active"
    finally:
        project_sync.close_project(OWNER)
        _delete_registrations([missing_uuid, scratch_uuid, flow_uuid])


def test_restore_of_a_version_without_those_files_keeps_kernels_connections_and_notebooks(tmp_path):
    """An older version with no kernels.yaml and nothing under connections/ or notebooks/ says nothing
    about those kinds, so restoring it keeps every kernel, connection (with its secret) and notebook."""
    project_sync.close_project(OWNER)
    root = tmp_path / "project"
    db_name, cloud_name = f"rs_db_{uuid4().hex[:6]}", f"rs_cloud_{uuid4().hex[:6]}"
    kernel_id = _make_kernel(f"rs-kernel-{uuid4().hex[:6]}")
    nb_uuid = _make_notebook(f"rs-nb-{uuid4().hex[:6]}")
    _make_db_connection(db_name)
    with get_db_context() as db:
        store_cloud_connection(
            db,
            FullCloudStorageConnection(
                connection_name=cloud_name, storage_type="s3", auth_method="aws-cli", aws_profile="analytics"
            ),
            OWNER,
        )
    try:
        project_sync.init_project(str(root), "Older Version", OWNER)
        assert (root / "kernels.yaml").exists() and any((root / "connections").rglob("*.yaml"))
        (root / "kernels.yaml").unlink()
        shutil.rmtree(root / "connections")
        shutil.rmtree(root / "notebooks")
        old_sha = git_ops.commit_all(root, "an older version without kernels, connections or notebooks")
        assert old_sha is not None
        project_sync.save_version(OWNER, "everything again")
        _, secret_before = _db_connection(db_name)
        assert secret_before is not None

        project_sync.restore_version(OWNER, old_sha)

        with get_db_context() as db:
            assert db.query(Kernel).filter_by(id=kernel_id).first() is not None
            assert db.query(CloudStorageConnection).filter_by(connection_name=cloud_name, user_id=OWNER).first()
            assert db.query(CatalogNotebook).filter_by(notebook_uuid=nb_uuid).first() is not None
        conn, secret = _db_connection(db_name)
        assert conn is not None and secret is not None and secret.id == secret_before.id
    finally:
        project_sync.close_project(OWNER)
        _delete_kernel(kernel_id)
        _delete_notebook(nb_uuid)
        _delete_db_connection(db_name)
        try:
            with get_db_context() as db:
                delete_cloud_connection(db, cloud_name, OWNER)
        except Exception:
            pass


def test_present_manifest_with_an_empty_list_still_prunes(tmp_path):
    project_sync.close_project(OWNER)
    root = tmp_path / "project"
    kernel_id = _make_kernel(f"empty-list-{uuid4().hex[:6]}")
    try:
        project_sync.init_project(str(root), "Empty List", OWNER)
        (root / "kernels.yaml").write_text(yaml.safe_dump({"kernels": []}), encoding="utf-8")
        project_sync.reload_from_disk(OWNER, force=True)
        with get_db_context() as db:
            assert db.query(Kernel).filter_by(id=kernel_id).first() is None
    finally:
        project_sync.close_project(OWNER)
        _delete_kernel(kernel_id)


def _connection_file(root: Path, name: str) -> Path:
    for p in (root / "connections" / "database").glob("*.yaml"):
        if yaml.safe_load(p.read_text(encoding="utf-8")).get("connection_name") == name:
            return p
    raise AssertionError(f"no projected file for {name}")


def test_reload_with_a_malformed_file_answers_422_and_changes_nothing(tmp_path):
    project_sync.close_project(OWNER)
    root = tmp_path / "project"
    db_name = f"bad_db_{uuid4().hex[:6]}"
    _make_db_connection(db_name)
    flow_uuid = _make_flow(tmp_path, f"bad_flow_{uuid4().hex[:6]}", 525252)
    try:
        assert client.post("/project/init", json={"folder_path": str(root), "name": "Bad Reload"}).status_code == 201
        conn_file = _connection_file(root, db_name)
        good_conn = conn_file.read_text(encoding="utf-8")
        conn_file.write_text(good_conn.replace("port: 5432", "port: not-a-port"), encoding="utf-8")
        _, secret_before = _db_connection(db_name)
        before = _row_counts()

        response = client.post("/project/reload", json={"force": True})
        assert response.status_code == 422
        rel = conn_file.relative_to(root).as_posix()
        assert rel in response.json()["detail"]
        assert "not-a-port" not in response.json()["detail"]
        assert _row_counts() == before
        conn, secret = _db_connection(db_name)
        assert conn is not None and secret is not None and secret.id == secret_before.id

        conn_file.write_text(good_conn, encoding="utf-8")
        flow_file = next((root / "flows").glob("bad_flow_*.flow.yaml"))
        flow_file.write_text("flow_uuid: [unclosed\nnodes: {\n", encoding="utf-8")
        response = client.post("/project/reload", json={"force": True})
        assert response.status_code == 422
        assert flow_file.relative_to(root).as_posix() in response.json()["detail"]
        assert "not valid YAML" in response.json()["detail"]
        assert _row_counts() == before
        assert _reg(flow_uuid) is not None
    finally:
        project_sync.close_project(OWNER)
        _delete_db_connection(db_name)
        _delete_registrations([flow_uuid])


def test_restore_to_a_malformed_version_answers_422_and_leaves_the_tree(tmp_path):
    project_sync.close_project(OWNER)
    root = tmp_path / "project"
    db_name = f"bad_sha_{uuid4().hex[:6]}"
    _make_db_connection(db_name)
    try:
        assert client.post("/project/init", json={"folder_path": str(root), "name": "Bad Sha"}).status_code == 201
        conn_file = _connection_file(root, db_name)
        conn_file.write_text(json.dumps({"database_type": "postgresql"}), encoding="utf-8")
        bad_sha = git_ops.commit_all(root, "a version whose connection file has no name")
        assert bad_sha is not None
        project_sync.save_version(OWNER, "projected again")
        head = git_ops.head_sha(root)
        tree_before = _tree_bytes(root)
        before = _row_counts()

        response = client.post("/project/restore", json={"sha": bad_sha})
        assert response.status_code == 422
        assert conn_file.relative_to(root).as_posix() in response.json()["detail"]
        assert _tree_bytes(root) == tree_before
        assert git_ops.head_sha(root) == head
        assert not git_ops.is_dirty(root)
        assert _row_counts() == before
    finally:
        project_sync.close_project(OWNER)
        _delete_db_connection(db_name)


def test_open_of_a_folder_with_a_malformed_file_answers_422_and_activates_nothing(tmp_path):
    project_sync.close_project(OWNER)
    root = tmp_path / "handmade"
    (root / "connections" / "database").mkdir(parents=True)
    (root / "project.yaml").write_text(
        yaml.safe_dump({"project_format": 1, "name": "Handmade", "project_id": "x", "created_with_version": "0"}),
        encoding="utf-8",
    )
    (root / "connections" / "database" / "broken.yaml").write_text("connection_name: [x\n", encoding="utf-8")
    try:
        before = _row_counts()
        response = client.post("/project/open", json={"folder_path": str(root)})
        assert response.status_code == 422
        assert "connections/database/broken.yaml" in response.json()["detail"]
        assert _row_counts() == before
        assert not (root / ".gitignore").exists()
        with get_db_context() as db:
            assert db.query(WorkspaceProject).filter_by(folder_path=str(root.resolve())).first() is None
        assert project_sync.get_active_project(OWNER) is None
    finally:
        project_sync.close_project(OWNER)
