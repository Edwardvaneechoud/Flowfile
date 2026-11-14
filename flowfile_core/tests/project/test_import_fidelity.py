"""An import changes only what the files describe, and an export of what it built is the same files.

An install holding every kind with a file is exported and imported with prune twice: every row of every
table, ids included, is as it was, a schedule keeps its notification rule, a dashboard's
``layout_version`` stays, and the export does not change. The cases around it: a flow registered from a
``.json`` path stays JSON, a flow renamed and moved in the catalog exports its new name, a table names
its producing flow only when that flow is in the project and keeps a lineage the files leave out, a
view's lineage and a table's data pointer survive a reload, a notebook follows its kernel's re-minted
id, and ``secrets.yaml`` names only the secrets no feature keeps for itself.
"""

import json
import uuid
from pathlib import Path

import pytest
import yaml
from pydantic import SecretStr
from sqlalchemy import select

from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
from flowfile_core.catalog.services import notebook_store
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import (
    AiProviderCredential,
    Base,
    CatalogDashboard,
    CatalogNamespace,
    CatalogNotebook,
    CatalogTable,
    CatalogVisualization,
    FlowRegistration,
    FlowSchedule,
    GlobalArtifact,
    KafkaConnection,
    Kernel,
    NotificationChannel,
    NotificationRule,
    Secret,
    User,
)
from flowfile_core.flowfile.catalog_helpers import auto_register_flow
from flowfile_core.flowfile.database_connection_manager.db_connections import (
    delete_cloud_connection,
    delete_database_connection,
    store_cloud_connection,
    store_database_connection,
)
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.project import git_ops, importer, manifest, project_sync, projection
from flowfile_core.project.importer import import_project
from flowfile_core.project.service import write_export
from flowfile_core.schemas import input_schema, schemas
from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection
from flowfile_core.schemas.input_schema import FullDatabaseConnection
from flowfile_core.secret_manager.secret_manager import SecretInput, decrypt_secret, store_secret
from shared.storage_config import storage

try:
    from test_utils.s3.fixtures import get_minio_client, is_docker_available
except ModuleNotFoundError:  # pragma: no cover - import shim for ad-hoc runs
    import os
    import sys

    sys.path.append(os.path.dirname(os.path.abspath("test_utils/s3/fixtures.py")))
    from test_utils.s3.fixtures import get_minio_client, is_docker_available

OWNER = 1
_WRITE_TIME_COLUMNS = {"created_at", "updated_at", "last_opened_at"}


def _minio_available() -> bool:
    if not is_docker_available():
        return False
    try:
        get_minio_client().list_buckets()
        return True
    except Exception:
        return False


requires_minio = pytest.mark.skipif(not _minio_available(), reason="MinIO mock S3 not available")


@pytest.fixture(autouse=True)
def _require_git():
    if not git_ops.git_available():
        pytest.skip("git not available")


@pytest.fixture(autouse=True)
def _closed_project():
    project_sync.close_project(OWNER)
    yield
    project_sync.close_project(OWNER)


def _dump() -> dict[str, list[str]]:
    """Every row of every table, ids included; write-time stamps left out and a secret by its value,
    since a connection import stores its password again under a new ciphertext."""
    out: dict[str, list[str]] = {}
    with get_db_context() as db:
        for name, table in sorted(Base.metadata.tables.items()):
            cols = [c for c in table.columns if c.name not in _WRITE_TIME_COLUMNS]
            rows = []
            for row in db.execute(select(*cols)):
                values = dict(zip([c.name for c in cols], row, strict=True))
                if name == Secret.__tablename__ and values.get("encrypted_value"):
                    values["encrypted_value"] = decrypt_secret(values["encrypted_value"]).get_secret_value()
                rows.append(repr(sorted(values.items())))
            out[name] = sorted(rows)
    return out


def _snapshot(root: Path) -> dict[str, bytes]:
    return {
        str(p.relative_to(root)): p.read_bytes()
        for p in sorted(root.rglob("*"))
        if p.is_file() and ".git" not in p.relative_to(root).parts
    }


def _export(root: Path) -> dict[str, bytes]:
    write_export(root, OWNER)
    return _snapshot(root)


def _make_flow(tmp_path: Path, name: str, flow_id: int, suffix: str = ".yaml", namespace_id: int | None = None) -> str:
    handler = FlowfileHandler()
    handler.register_flow(schemas.FlowSettings(flow_id=flow_id, name=name, path="."))
    graph = handler.get_flow(flow_id)
    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=flow_id, node_id=1, raw_data_format=input_schema.RawData.from_pylist([{"a": 1}])
        )
    )
    flow_path = str(tmp_path / f"{name}{suffix}")
    graph.save_flow(flow_path)
    auto_register_flow(flow_path, name, OWNER)
    with get_db_context() as db:
        reg = db.query(FlowRegistration).filter_by(flow_path=flow_path).one()
        if namespace_id is not None:
            reg.namespace_id = namespace_id
            db.commit()
        return reg.flow_uuid


def _reg(flow_uuid: str) -> FlowRegistration:
    with get_db_context() as db:
        reg = db.query(FlowRegistration).filter_by(flow_uuid=flow_uuid).one()
        db.expunge(reg)
        return reg


def _namespace(tag: str) -> tuple[int, int]:
    with get_db_context() as db:
        svc = CatalogService(SQLAlchemyCatalogRepository(db))
        cat = svc.create_namespace(name=f"FidCat_{tag}", owner_id=OWNER)
        sch = svc.create_namespace(name=f"FidSchema_{tag}", owner_id=OWNER, parent_id=cat.id)
        return cat.id, sch.id


def _add(row):
    with get_db_context() as db:
        db.add(row)
        db.commit()
        db.refresh(row)
        db.expunge(row)
        return row


def _table_row(name: str) -> CatalogTable:
    with get_db_context() as db:
        row = db.query(CatalogTable).filter_by(name=name, owner_id=OWNER).one()
        db.expunge(row)
        return row


def _tables_yaml(root: Path) -> dict[str, dict]:
    data = yaml.safe_load(manifest.tables_manifest_path(root).read_text(encoding="utf-8")) or {}
    return {e["name"]: e for e in data.get("tables") or []}


def _delete_all(tag: str, flow_uuids: list[str]) -> None:
    with get_db_context() as db:
        svc = CatalogService(SQLAlchemyCatalogRepository(db))
        for t in db.query(CatalogTable).filter(CatalogTable.name.like(f"%{tag}%")).all():
            svc.delete_table(t.id, delete_file=False)
        db.query(CatalogDashboard).filter(CatalogDashboard.name.like(f"%{tag}%")).delete(synchronize_session=False)
        db.query(CatalogVisualization).filter(CatalogVisualization.name.like(f"%{tag}%")).delete(
            synchronize_session=False
        )
        for nb in db.query(CatalogNotebook).filter(CatalogNotebook.name.like(f"%{tag}%")).all():
            notebook_store.delete_notebook_file(nb.owner_id, nb.notebook_uuid)
            db.delete(nb)
        db.query(GlobalArtifact).filter(GlobalArtifact.name.like(f"%{tag}%")).delete(synchronize_session=False)
        db.query(Kernel).filter(Kernel.name.like(f"%{tag}%")).delete(synchronize_session=False)
        db.commit()
        for flow_uuid in flow_uuids:
            reg = db.query(FlowRegistration).filter_by(flow_uuid=flow_uuid).first()
            if reg is None:
                continue
            db.query(NotificationRule).filter_by(registration_id=reg.id).delete(synchronize_session=False)
            db.commit()
            for s in svc.list_schedules(registration_id=reg.id):
                svc.delete_schedule(s.id)
            db.query(GlobalArtifact).filter_by(source_registration_id=reg.id).delete(synchronize_session=False)
            db.commit()
            svc.delete_flow(reg.id, delete_file=True)
        db.query(NotificationChannel).filter(NotificationChannel.name.like(f"%{tag}%")).delete(
            synchronize_session=False
        )
        for name in (f"db_{tag}",):
            try:
                delete_database_connection(db, name, OWNER)
            except Exception:
                pass
        for name in (f"cloud_{tag}",):
            try:
                delete_cloud_connection(db, name, OWNER)
            except Exception:
                pass
        db.query(Secret).filter(Secret.name.like(f"%{tag}%")).delete(synchronize_session=False)
        db.commit()
        for ns in (
            db.query(CatalogNamespace)
            .filter(CatalogNamespace.name.like(f"%{tag}%"))
            .order_by(CatalogNamespace.level.desc())
            .all()
        ):
            db.delete(ns)
            db.commit()


def _build_every_kind(tmp_path: Path, tag: str) -> tuple[str, int]:
    """One object of every kind with a file; returns the flow's uuid and the dashboard's id."""
    with get_db_context() as db:
        store_database_connection(
            db,
            FullDatabaseConnection(
                connection_name=f"db_{tag}",
                database_type="postgresql",
                username="u",
                password=SecretStr("db-pass"),
                host="localhost",
                port=5432,
                database="d",
            ),
            OWNER,
        )
        store_cloud_connection(
            db,
            FullCloudStorageConnection(
                connection_name=f"cloud_{tag}",
                storage_type="s3",
                auth_method="access_key",
                aws_access_key_id="AKIA",
                aws_secret_access_key=SecretStr("cloud-pass"),
                aws_region="us-east-1",
            ),
            OWNER,
        )
        store_secret(db, SecretInput(name=f"standalone_{tag}", value=SecretStr("v")), OWNER)
    _, schema_id = _namespace(tag)
    flow_uuid = _make_flow(tmp_path, f"flow_{tag}", 880001, namespace_id=schema_id)
    reg = _reg(flow_uuid)
    with get_db_context() as db:
        svc = CatalogService(SQLAlchemyCatalogRepository(db))
        sched = svc.create_schedule(
            registration_id=reg.id, owner_id=OWNER, schedule_type="interval", interval_seconds=3600
        )
        sched_id = sched.id
    channel = _add(
        NotificationChannel(owner_id=OWNER, name=f"chan_{tag}", channel_type="generic", webhook_url_encrypted="x")
    )
    _add(NotificationRule(owner_id=OWNER, channel_id=channel.id, schedule_id=sched_id))

    import polars as pl

    table_dir = storage.catalog_tables_directory / f"managed_{tag}"
    df = pl.DataFrame({"id": [1, 2]})
    df.write_delta(str(table_dir), mode="overwrite")
    with get_db_context() as db:
        CatalogService(SQLAlchemyCatalogRepository(db)).register_table_from_data(
            name=f"managed_{tag}",
            table_path=str(table_dir),
            owner_id=OWNER,
            namespace_id=schema_id,
            storage_format="delta",
            schema=[{"name": "id", "dtype": "Int64"}],
            row_count=2,
            column_count=1,
            size_bytes=1,
        )
    view = _add(
        CatalogTable(
            name=f"view_{tag}",
            owner_id=OWNER,
            namespace_id=schema_id,
            storage_format="delta",
            table_type="virtual",
            sql_query="SELECT 1 AS id",
            schema_json=json.dumps([{"name": "id", "dtype": "Int64"}]),
            column_count=1,
            source_registration_id=reg.id,
        )
    )
    _add(
        GlobalArtifact(
            name=f"model_{tag}",
            namespace_id=schema_id,
            version=1,
            owner_id=OWNER,
            source_registration_id=reg.id,
            serialization_format="pickle",
            status="active",
            tags="[]",
        )
    )
    kernel = _add(
        Kernel(id=f"kernel-{tag}", name=f"kernel_{tag}", user_id=OWNER, packages="[]", resolved_packages="[]")
    )
    viz = _add(
        CatalogVisualization(
            viz_uuid=str(uuid.uuid4()),
            name=f"viz_{tag}",
            chart_type="bar",
            spec_json="[]",
            source_type="table",
            catalog_table_id=view.id,
            namespace_id=schema_id,
            created_by=OWNER,
        )
    )
    tile = {"id": "t1", "type": "viz", "viz_id": viz.id, "chart_index": 0, "x": 0, "y": 0, "w": 4, "h": 4}
    dashboard = _add(
        CatalogDashboard(
            dashboard_uuid=str(uuid.uuid4()),
            name=f"dash_{tag}",
            namespace_id=schema_id,
            layout_json=json.dumps({"tiles": [tile], "grid": {"cols": 48, "row_height": 40, "version": 2}}),
            layout_version=3,
            created_by=OWNER,
        )
    )
    nb = _add(CatalogNotebook(name=f"nb_{tag}", owner_id=OWNER, namespace_id=schema_id, default_kernel_id=kernel.id))
    notebook_store.write_notebook_file(
        OWNER,
        nb.notebook_uuid,
        name=nb.name,
        description=None,
        namespace_name=f"FidSchema_{tag}",
        default_kernel_id=kernel.id,
        cells=[],
    )
    return flow_uuid, dashboard.id


def test_every_kind_imports_twice_without_a_change(tmp_path):
    tag = uuid.uuid4().hex[:6]
    root = tmp_path / "project"
    flow_uuid = None
    try:
        flow_uuid, dashboard_id = _build_every_kind(tmp_path, tag)
        project_sync.init_project(str(root), "Fidelity", OWNER)
        project_sync.close_project(OWNER)
        exported = _snapshot(root)
        before = _dump()
        for _ in range(2):
            import_project(root, OWNER)
            assert _dump() == before
            assert _export(root) == exported
        with get_db_context() as db:
            reg = db.query(FlowRegistration).filter_by(flow_uuid=flow_uuid).one()
            sched = db.query(FlowSchedule).filter_by(registration_id=reg.id).one()
            assert db.query(NotificationRule).filter_by(schedule_id=sched.id).count() == 1
            assert db.get(CatalogDashboard, dashboard_id).layout_version == 3

        dashboards = yaml.safe_load(manifest.dashboards_manifest_path(root).read_text(encoding="utf-8"))
        board = next(d for d in dashboards["dashboards"] if d["name"] == f"dash_{tag}")
        assert "kpi" not in board["layout"]["tiles"][0]
        board["layout"]["tiles"][0]["w"] = 6
        manifest.dashboards_manifest_path(root).write_text(yaml.safe_dump(dashboards), encoding="utf-8")
        import_project(root, OWNER)
        with get_db_context() as db:
            assert db.get(CatalogDashboard, dashboard_id).layout_version == 4
    finally:
        _delete_all(tag, [flow_uuid] if flow_uuid else [])


def test_a_flow_registered_from_a_json_path_stays_json(tmp_path):
    tag = uuid.uuid4().hex[:6]
    root = tmp_path / "project"
    flow_uuid = _make_flow(tmp_path, f"jsonflow_{tag}", 880002, suffix=".json")
    try:
        project_sync.init_project(str(root), "Json", OWNER)
        project_sync.close_project(OWNER)
        runtime = Path(_reg(flow_uuid).flow_path)
        assert runtime.suffix == ".json"
        import_project(root, OWNER)
        data = json.loads(runtime.read_text(encoding="utf-8"))
        assert data["flow_uuid"] == flow_uuid
        handler = FlowfileHandler()
        flow_id = handler.import_flow(runtime, user_id=OWNER, register_session=False)
        assert handler.get_flow(flow_id) is not None
        handler.delete_flow(flow_id)
    finally:
        _delete_all(tag, [flow_uuid])


def test_an_imported_flow_renamed_and_moved_exports_its_new_name(tmp_path):
    tag = uuid.uuid4().hex[:6]
    root = tmp_path / "project"
    flow_uuid = _make_flow(tmp_path, f"renamed_{tag}", 880003)
    try:
        project_sync.init_project(str(root), "Rename", OWNER)
        project_sync.close_project(OWNER)
        import_project(root, OWNER)
        _, schema_id = _namespace(tag)
        with get_db_context() as db:
            reg = db.query(FlowRegistration).filter_by(flow_uuid=flow_uuid).one()
            assert yaml.safe_load(Path(reg.flow_path).read_text(encoding="utf-8"))["catalog_name"] == reg.name
            reg.name = f"new_name_{tag}"
            reg.namespace_id = schema_id
            db.commit()
        _export(root)
        path = projection.flow_index(root)[flow_uuid][0]
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        assert data["catalog_name"] == f"new_name_{tag}"
        assert data["namespace"] == {"catalog": f"FidCat_{tag}", "schema": f"FidSchema_{tag}"}
        assert path.read_text(encoding="utf-8").startswith("flow_uuid:")
    finally:
        _delete_all(tag, [flow_uuid])


def test_table_lineage_and_pointers_through_a_reload(tmp_path):
    tag = uuid.uuid4().hex[:6]
    root = tmp_path / "project"
    flow_uuid = _make_flow(tmp_path, f"producer_{tag}", 880004)
    snapshot = _add(FlowRegistration(name=f"snap_{tag}", flow_path=str(tmp_path / "gone" / "run.yaml"), owner_id=OWNER))
    reg = _reg(flow_uuid)
    schema = json.dumps([{"name": "id", "dtype": "Int64"}])
    common = {"owner_id": OWNER, "storage_format": "delta", "schema_json": schema, "column_count": 1}
    _add(CatalogTable(name=f"snapped_{tag}", table_type="physical", source_registration_id=snapshot.id, **common))
    _add(
        CatalogTable(
            name=f"view_{tag}",
            table_type="virtual",
            sql_query="SELECT 1 AS id",
            source_registration_id=reg.id,
            **common,
        )
    )
    kept_path = str(tmp_path / "elsewhere" / f"pointer_{tag}")
    _add(CatalogTable(name=f"pointer_{tag}", table_type="physical", file_path=kept_path, **common))
    try:
        project_sync.init_project(str(root), "Lineage", OWNER)
        project_sync.close_project(OWNER)
        entries = _tables_yaml(root)
        assert "source_flow_uuid" not in entries[f"snapped_{tag}"]
        assert entries[f"view_{tag}"]["source_flow_uuid"] == flow_uuid
        assert entries[f"pointer_{tag}"]["pointer"]["type"] == "external"

        data = yaml.safe_load(manifest.tables_manifest_path(root).read_text(encoding="utf-8"))
        for e in data["tables"]:
            if e["name"] == f"pointer_{tag}":
                e["pointer"] = {"type": "none"}
        manifest.tables_manifest_path(root).write_text(yaml.safe_dump(data), encoding="utf-8")
        with get_db_context() as db:
            db.query(CatalogTable).filter_by(name=f"view_{tag}").update({"source_registration_id": None})
            db.commit()

        import_project(root, OWNER)
        assert _table_row(f"snapped_{tag}").source_registration_id == snapshot.id
        assert _table_row(f"view_{tag}").source_registration_id == reg.id
        assert _table_row(f"pointer_{tag}").file_path == kept_path
    finally:
        _delete_all(tag, [flow_uuid, snapshot.flow_uuid])


def test_a_notebook_follows_its_kernels_reminted_id(tmp_path):
    tag = uuid.uuid4().hex[:6]
    root = tmp_path / "project"
    user = _add(User(username=f"other_{tag}", email=f"other_{tag}@flowfile.app", hashed_password="x"))
    other = _add(
        Kernel(id=f"taken-{tag}", name=f"other_{tag}", user_id=user.id, packages="[]", resolved_packages="[]")
    )
    nb_uuid = str(uuid.uuid4())
    manifest.notebooks_dir(root).mkdir(parents=True)
    manifest.kernels_manifest_path(root).write_text(
        yaml.safe_dump({"kernels": [{"id": other.id, "name": f"mine_{tag}"}]}), encoding="utf-8"
    )
    (manifest.notebooks_dir(root) / f"nb_{tag}.notebook.yaml").write_text(
        yaml.safe_dump({"notebook_uuid": nb_uuid, "name": f"nb_{tag}", "default_kernel_id": other.id, "cells": []}),
        encoding="utf-8",
    )
    try:
        import_project(root, OWNER)
        with get_db_context() as db:
            mine = db.query(Kernel).filter_by(name=f"mine_{tag}", user_id=OWNER).one()
            nb = db.query(CatalogNotebook).filter_by(notebook_uuid=nb_uuid).one()
            assert mine.id != other.id
            assert nb.default_kernel_id == mine.id
        content = yaml.safe_load(notebook_store._notebook_path(OWNER, nb_uuid).read_text(encoding="utf-8"))
        assert content["default_kernel_id"] == mine.id
    finally:
        _delete_all(tag, [])
        with get_db_context() as db:
            db.query(User).filter_by(id=user.id).delete()
            db.commit()


def test_secrets_yaml_names_only_standalone_secrets(tmp_path):
    tag = uuid.uuid4().hex[:6]
    root = tmp_path / "project"
    from flowfile_core.routes.community_github import GITHUB_PUBLISH_LOGIN_KEY, GITHUB_PUBLISH_TOKEN_KEY

    with get_db_context() as db:
        store_secret(db, SecretInput(name=f"page_{tag}", value=SecretStr("v")), OWNER)
        ai = store_secret(db, SecretInput(name=f"ai:openrouter:api_key:{tag}", value=SecretStr("v")), OWNER)
        kafka = store_secret(db, SecretInput(name=f"kafka_{tag}", value=SecretStr("v")), OWNER)
        github = [
            store_secret(db, SecretInput(name=name, value=SecretStr("v")), OWNER)
            for name in (GITHUB_PUBLISH_TOKEN_KEY, GITHUB_PUBLISH_LOGIN_KEY)
            if db.query(Secret).filter_by(name=name, user_id=OWNER).first() is None
        ]
        github_ids = [s.id for s in github]
        db.add(AiProviderCredential(user_id=OWNER, provider=f"openrouter_{tag}", api_key_secret_id=ai.id))
        db.add(
            KafkaConnection(
                connection_name=f"kafka_{tag}",
                bootstrap_servers="localhost:9092",
                sasl_password_id=kafka.id,
                user_id=OWNER,
            )
        )
        db.commit()
    try:
        project_sync.init_project(str(root), "Secrets", OWNER)
        names = yaml.safe_load(manifest.secrets_manifest_path(root).read_text(encoding="utf-8"))["required_secrets"]
        assert f"page_{tag}" in names
        assert f"ai:openrouter:api_key:{tag}" not in names
        assert f"kafka_{tag}" not in names
        assert GITHUB_PUBLISH_TOKEN_KEY not in names and GITHUB_PUBLISH_LOGIN_KEY not in names
    finally:
        with get_db_context() as db:
            db.query(AiProviderCredential).filter_by(provider=f"openrouter_{tag}").delete()
            db.query(KafkaConnection).filter_by(connection_name=f"kafka_{tag}").delete()
            db.query(Secret).filter(Secret.id.in_(github_ids)).delete(synchronize_session=False)
            db.commit()
        _delete_all(tag, [])


@requires_minio
def test_a_table_on_object_storage_keeps_its_pointer_through_a_reload(tmp_path, monkeypatch):
    import polars as pl

    from flowfile_core.catalog.storage_backend import join_catalog_uri, resolve_for_namespace
    from shared.delta_utils import write_delta

    monkeypatch.delenv("FLOWFILE_CATALOG_STORAGE_URI", raising=False)
    monkeypatch.delenv("FLOWFILE_CATALOG_STORAGE_CONNECTION", raising=False)
    tag = uuid.uuid4().hex[:6]
    root = tmp_path / "project"
    bucket = "flowfile-test"
    try:
        get_minio_client().create_bucket(Bucket=bucket)
    except Exception:
        pass
    with get_db_context() as db:
        store_cloud_connection(
            db,
            FullCloudStorageConnection(
                connection_name=f"cloud_{tag}",
                storage_type="s3",
                auth_method="access_key",
                aws_access_key_id="minioadmin",
                aws_secret_access_key=SecretStr("minioadmin"),
                aws_region="us-east-1",
                endpoint_url="http://localhost:9000",
                aws_allow_unsafe_html=True,
            ),
            OWNER,
        )
        cat = CatalogService(SQLAlchemyCatalogRepository(db)).create_namespace(
            name=f"FidCat_{tag}",
            owner_id=OWNER,
            storage_uri=f"s3://{bucket}/fidelity_{tag}",
            storage_connection_name=f"cloud_{tag}",
        )
        cat_id = cat.id
    target = resolve_for_namespace(cat_id)
    dest = join_catalog_uri(target.base, f"t_{tag}")
    write_delta(pl.DataFrame({"a": [1, 2, 3]}), dest, mode="overwrite", storage_options=target.storage_options)
    _add(
        CatalogTable(
            name=f"cloud_table_{tag}",
            namespace_id=cat_id,
            owner_id=OWNER,
            file_path=dest,
            table_type="physical",
            storage_format="delta",
            schema_json=json.dumps([{"name": "a", "dtype": "Int64"}]),
            column_count=1,
        )
    )
    outside = f"s3://{bucket}/elsewhere_{tag}/t"
    try:
        project_sync.init_project(str(root), "Cloud", OWNER)
        project_sync.close_project(OWNER)
        assert _tables_yaml(root)[f"cloud_table_{tag}"]["pointer"] == {"type": "external", "path": dest}
        import_project(root, OWNER)
        assert _table_row(f"cloud_table_{tag}").file_path == dest
        assert pl.scan_delta(dest, storage_options=target.storage_options).collect().height == 3
        assert importer._resolve_table_path({"type": "external", "path": outside}, root, namespace_id=cat_id) is None
        assert (
            importer._resolve_table_path({"type": "external", "path": target.base}, root, namespace_id=cat_id) is None
        )
    finally:
        _delete_all(tag, [])
