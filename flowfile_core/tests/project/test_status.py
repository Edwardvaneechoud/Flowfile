"""Status is ``compare(last version, snapshot)``: what the database holds that the last version does not.

On ``test_kinds_list.py``'s every-kind install, real git and the test database: a create, rename and delete of
each kind shows as unsaved and a save clears it; a write from another process shows; a warm read parses no flow
file, and a renamed table or a rewritten flow file re-renders exactly the flows it touches.
"""

import json
import os
import subprocess
import sys
from uuid import uuid4

import pytest
import yaml
from pydantic import SecretStr
from sqlalchemy import inspect

from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import (
    CatalogDashboard,
    CatalogNamespace,
    CatalogNotebook,
    CatalogTable,
    CatalogVisualization,
    FlowRegistration,
    FlowSchedule,
    GlobalArtifact,
    Kernel,
    Secret,
)
from flowfile_core.flowfile.database_connection_manager.db_connections import (
    store_cloud_connection,
    store_database_connection,
)
from flowfile_core.project import kinds, project_sync, projection
from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection
from flowfile_core.schemas.input_schema import FullDatabaseConnection
from flowfile_core.secret_manager.secret_manager import SecretInput, store_secret
from tests.project import test_kinds_list
from tests.project.test_kinds import FLOW_A, SCHEMA

every_kind = test_kinds_list.every_kind


def _schema_id(db, uid):
    return db.query(CatalogNamespace).filter_by(name=SCHEMA, owner_id=uid).one().id


def _flow(db, uid, tmp_path):
    path = tmp_path / "status_flow.yaml"
    path.write_text(yaml.safe_dump({"flowfile_name": "status_flow", "nodes": []}), encoding="utf-8")
    return FlowRegistration(name="status_flow", flow_path=str(path), owner_id=uid)


def _schedule(db, uid, tmp_path):
    reg = db.query(FlowRegistration).filter_by(flow_uuid=FLOW_A).one()
    return FlowSchedule(registration_id=reg.id, owner_id=uid, schedule_type="interval", interval_seconds=60)


def _notebook(db, uid, tmp_path):
    return CatalogNotebook(name="status_notebook", owner_id=uid, namespace_id=_schema_id(db, uid))


def _viz(db, uid, tmp_path):
    table = db.query(CatalogTable).filter_by(name="kinds_view", owner_id=uid).one()
    return CatalogVisualization(
        name="status_viz",
        chart_type="bar",
        spec_json="[]",
        source_type="table",
        catalog_table_id=table.id,
        namespace_id=table.namespace_id,
        created_by=uid,
    )


def _db_connection(db, uid, tmp_path):
    conn = FullDatabaseConnection(
        connection_name="status_db",
        database_type="postgresql",
        username="u",
        password=SecretStr("p"),
        host="h",
        port=5432,
        database="d",
    )
    return store_database_connection(db, conn, uid)


def _cloud_connection(db, uid, tmp_path):
    conn = FullCloudStorageConnection(
        connection_name="status_cloud",
        storage_type="s3",
        auth_method="access_key",
        aws_access_key_id="AKIA",
        aws_secret_access_key=SecretStr("p"),
        aws_region="us-east-1",
    )
    return store_cloud_connection(db, conn, uid)


# kind: (make the row, the field a rename changes)
OBJECTS = {
    "secrets": (lambda db, uid, _: store_secret(db, SecretInput(name="status_s", value=SecretStr("v")), uid), "name"),
    "database_connection": (_db_connection, "connection_name"),
    "cloud_connection": (_cloud_connection, "connection_name"),
    "namespaces": (lambda db, uid, _: CatalogNamespace(name="StatusCatalog", owner_id=uid, level=0), "name"),
    "flow": (_flow, "name"),
    "schedule": (_schedule, "name"),
    "tables": (
        lambda db, uid, _: CatalogTable(
            name="status_view",
            owner_id=uid,
            namespace_id=_schema_id(db, uid),
            table_type="virtual",
            sql_query="SELECT 1 AS id",
            schema_json=json.dumps([{"name": "id", "dtype": "Int64"}]),
        ),
        "name",
    ),
    "models": (
        lambda db, uid, _: GlobalArtifact(
            name="status_model",
            namespace_id=_schema_id(db, uid),
            version=1,
            owner_id=uid,
            status="active",
            serialization_format="pickle",
            source_registration_id=db.query(FlowRegistration).filter_by(flow_uuid=FLOW_A).one().id,
        ),
        "name",
    ),
    "kernels": (lambda db, uid, _: Kernel(id=uuid4().hex, name="status_kernel", user_id=uid), "name"),
    "visualizations": (_viz, "name"),
    "dashboards": (
        lambda db, uid, _: CatalogDashboard(
            name="status_dashboard", namespace_id=_schema_id(db, uid), layout_json='{"tiles": []}', created_by=uid
        ),
        "name",
    ),
    "notebook": (_notebook, "name"),
}


def _status(uid) -> tuple[bool, set[str]]:
    return project_sync.has_uncommitted_changes(uid), {r["kind"] for r in project_sync.unsaved_changes(uid)}


@pytest.mark.parametrize("kind", list(OBJECTS))
def test_a_create_rename_and_delete_shows_and_a_save_clears_it(tmp_path, every_kind, kind):
    uid = every_kind
    project_sync.init_project(str(tmp_path / "project"), "status", uid, True)
    assert _status(uid) == (False, set())
    make, field = OBJECTS[kind]
    label = kinds.get(kind).label

    with get_db_context() as db:
        row = make(db, uid, tmp_path)
        db.add(row)
        db.commit()
        cls, pk = type(row), inspect(row).identity
    for step in ("create", "rename", "delete"):
        if step != "create":
            with get_db_context() as db:
                row = db.get(cls, pk)
                if step == "rename":
                    setattr(row, field, f"{getattr(row, field)}_2")
                else:
                    db.delete(row)
                db.commit()
        dirty, labels = _status(uid)
        if kind == "secrets" and step == "delete":
            assert labels == set(), "a secret is never deleted, so its removal is not reported"
        else:
            assert dirty and label in labels, (step, labels)
        project_sync.save_version(uid, f"{kind} {step}")
        assert _status(uid) == (False, set()), step


def test_a_secret_written_by_another_process_shows(tmp_path, every_kind):
    project_sync.init_project(str(tmp_path / "project"), "status", every_kind, True)
    script = (
        "from pydantic import SecretStr\n"
        "from flowfile_core.database.connection import get_db_context\n"
        "from flowfile_core.secret_manager.secret_manager import SecretInput, store_secret\n"
        "with get_db_context() as db:\n"
        f"    store_secret(db, SecretInput(name='from_another_process', value=SecretStr('v')), {every_kind})\n"
    )
    subprocess.run([sys.executable, "-c", script], check=True, env=os.environ.copy(), timeout=120)
    try:
        assert [r["label"] for r in project_sync.unsaved_changes(every_kind)] == ["from_another_process"]
    finally:
        with get_db_context() as db:
            db.query(Secret).filter_by(name="from_another_process").delete()
            db.commit()


@pytest.mark.parametrize("content", [b"tables: [\n", b"\xff\xfe\x00tables"], ids=["not yaml", "not utf-8"])
def test_a_last_version_that_does_not_parse_still_answers(tmp_path, every_kind, content):
    root = tmp_path / "project"
    project_sync.init_project(str(root), "status", every_kind, True)
    (root / "tables.yaml").write_bytes(content)
    test_kinds_list._commit_everything(root, "by hand")
    rows = project_sync.unsaved_changes(every_kind)
    assert [(r["change"], r["path"]) for r in rows] == [("modified", "tables.yaml")]
    assert project_sync.has_uncommitted_changes(every_kind)


@pytest.fixture
def loads(monkeypatch):
    """The flow files ``projection._load_flow_data`` parses, by file name."""
    seen: list[str] = []
    real = projection._load_flow_data

    def counting(flow_path):
        seen.append(os.path.basename(flow_path))
        return real(flow_path)

    monkeypatch.setattr(projection, "_load_flow_data", counting)
    return seen


def test_a_warm_read_parses_no_flow_file(tmp_path, every_kind, loads):
    project_sync.init_project(str(tmp_path / "project"), "status", every_kind, True)
    project_sync.unsaved_changes(every_kind)
    loads.clear()
    assert project_sync.unsaved_changes(every_kind) == []
    assert loads == []


def test_a_renamed_table_re_renders_the_flow_that_names_it(tmp_path, every_kind, loads):
    project_sync.init_project(str(tmp_path / "project"), "status", every_kind, True)
    project_sync.unsaved_changes(every_kind)
    loads.clear()
    with get_db_context() as db:
        db.query(CatalogTable).filter_by(name="kinds_table", owner_id=every_kind).one().name = "kinds_renamed"
        db.commit()
    rows = project_sync.unsaved_changes(every_kind)
    assert loads == ["flow_a.yaml"]
    assert ("flow", "flow_a") in {(r["kind"], r["label"]) for r in rows}


def test_a_flow_file_rewritten_elsewhere_is_re_rendered(tmp_path, every_kind, loads):
    project_sync.init_project(str(tmp_path / "project"), "status", every_kind, True)
    project_sync.unsaved_changes(every_kind)
    loads.clear()
    path = tmp_path / "flow_b.yaml"
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    data["nodes"][0]["description"] = "edited elsewhere"
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")
    stat = path.stat()
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000_000))
    rows = project_sync.unsaved_changes(every_kind)
    assert loads == ["flow_b.yaml"]
    assert [(r["kind"], r["label"]) for r in rows] == [("flow", "flow_b")]

