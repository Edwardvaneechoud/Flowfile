"""A flow file names the catalog tables, schemas and flows its nodes use, never this install's row ids.

The canary install holds a marked value in every secret field and nodes that reference a table, a schema and a
flow by row id only: its export carries no marked value, no ciphertext and no row id, and is the same twice. The
shifted-ids case exports a catalog-reader flow, recreates its table under a new id with a decoy holding the old
one, opens the folder again and runs the flow: it reads the real table.
"""

import json
import os
import uuid
from pathlib import Path

import polars as pl
import pytest
import yaml
from pydantic import SecretStr

from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import CatalogNamespace, CatalogTable, FlowRegistration, Secret
from flowfile_core.flowfile.database_connection_manager.db_connections import (
    delete_cloud_connection,
    delete_database_connection,
    store_cloud_connection,
    store_database_connection,
)
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.project import git_ops, project_sync
from flowfile_core.project.service import write_export
from flowfile_core.schemas import input_schema, schemas
from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection
from flowfile_core.schemas.input_schema import FullDatabaseConnection
from flowfile_core.secret_manager.secret_manager import SecretInput, store_secret
from shared.storage_config import storage

OWNER = 1
CANARY = "CANARY-7f3a"
_ID_FIELDS = {"catalog_table_id", "catalog_namespace_id", "namespace_id"}


@pytest.fixture(autouse=True)
def _require_git():
    if not git_ops.git_available():
        pytest.skip("git not available")


@pytest.fixture(autouse=True)
def _closed_project():
    project_sync.close_project(OWNER)
    yield
    project_sync.close_project(OWNER)


def _walk(value, path="$"):
    yield path, value
    if isinstance(value, dict):
        for key, item in value.items():
            yield from _walk(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            yield from _walk(item, f"{path}[{index}]")


def _snapshot(root: Path) -> dict[str, bytes]:
    return {
        str(p.relative_to(root)): p.read_bytes()
        for p in sorted(root.rglob("*"))
        if p.is_file() and ".git" not in p.relative_to(root).parts
    }


def _namespace(tag: str, prefix: str = "Names") -> tuple[int, str]:
    with get_db_context() as db:
        svc = CatalogService(SQLAlchemyCatalogRepository(db))
        cat = svc.create_namespace(name=f"{prefix}Cat_{tag}", owner_id=OWNER)
        sch = svc.create_namespace(name=f"{prefix}Sch_{tag}", owner_id=OWNER, parent_id=cat.id)
        return sch.id, f"{cat.name}.{sch.name}"


def _table(name: str, namespace_id: int, rows: list[int], table_id: int | None = None) -> int:
    table_dir = storage.catalog_tables_directory / f"{name}_{uuid.uuid4().hex[:6]}"
    pl.DataFrame({"v": rows}).write_delta(str(table_dir), mode="overwrite")
    schema = [{"name": "v", "dtype": "Int64"}]
    with get_db_context() as db:
        if table_id is None:
            out = CatalogService(SQLAlchemyCatalogRepository(db)).register_table_from_data(
                name=name,
                table_path=str(table_dir),
                owner_id=OWNER,
                namespace_id=namespace_id,
                storage_format="delta",
                schema=schema,
                row_count=len(rows),
                column_count=1,
                size_bytes=1,
            )
            return out.id
        db.add(
            CatalogTable(
                id=table_id,
                name=name,
                owner_id=OWNER,
                namespace_id=namespace_id,
                file_path=str(table_dir),
                storage_format="delta",
                table_type="physical",
                schema_json=json.dumps(schema),
                row_count=len(rows),
                column_count=1,
            )
        )
        db.commit()
        return table_id


def _graph(flow_id: int, name: str) -> object:
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(flow_id=flow_id, name=name, path=".", execution_location="remote")
    )
    return handler.get_flow(flow_id)


def _register(flow_path: str, name: str, namespace_id: int) -> FlowRegistration:
    with get_db_context() as db:
        reg = FlowRegistration(name=name, flow_path=flow_path, namespace_id=namespace_id, owner_id=OWNER)
        db.add(reg)
        db.commit()
        db.refresh(reg)
        db.expunge(reg)
        return reg


def _manual_flow(tmp_path: Path, name: str, flow_id: int, namespace_id: int) -> FlowRegistration:
    graph = _graph(flow_id, name)
    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=flow_id, node_id=1, raw_data_format=input_schema.RawData.from_pylist([{"a": 1}])
        )
    )
    flow_path = str(tmp_path / f"{name}.yaml")
    graph.save_flow(flow_path)
    return _register(flow_path, name, namespace_id)


def _reader_flow(tmp_path: Path, name: str, flow_id: int, namespace_id: int, readers: list[dict]) -> str:
    graph = _graph(flow_id, name)
    for node_id, fields in enumerate(readers, start=1):
        graph.add_node_promise(
            input_schema.NodePromise(flow_id=flow_id, node_id=node_id, node_type="catalog_reader")
        )
        graph.add_catalog_reader(input_schema.NodeCatalogReader(flow_id=flow_id, node_id=node_id, **fields))
    flow_path = str(tmp_path / f"{name}.yaml")
    graph.save_flow(flow_path)
    return flow_path


def _append_nodes(flow_path: str, nodes: list[tuple[int, str, dict]]) -> None:
    doc = yaml.safe_load(Path(flow_path).read_text(encoding="utf-8"))
    for node_id, node_type, setting_input in nodes:
        doc["nodes"].append({"id": node_id, "type": node_type, "setting_input": setting_input})
    Path(flow_path).write_text(yaml.safe_dump(doc), encoding="utf-8")


def _flow_doc(root: Path, flow_uuid: str) -> dict:
    for p in (root / "flows").glob("*.flow.yaml"):
        doc = yaml.safe_load(p.read_text(encoding="utf-8"))
        if doc.get("flow_uuid") == flow_uuid:
            return doc
    raise AssertionError(f"no flow file for {flow_uuid}")


def _node_settings(doc: dict, node_id: int) -> dict:
    return next(n["setting_input"] for n in doc["nodes"] if n["id"] == node_id)


def _cleanup(tag: str) -> None:
    with get_db_context() as db:
        svc = CatalogService(SQLAlchemyCatalogRepository(db))
        for t in db.query(CatalogTable).filter(CatalogTable.name.like(f"%{tag}%")).all():
            svc.delete_table(t.id, delete_file=False)
        for reg in db.query(FlowRegistration).filter(FlowRegistration.name.like(f"%{tag}%")).all():
            svc.delete_flow(reg.id, delete_file=False)
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


def _store_canary_secrets(tag: str) -> None:
    with get_db_context() as db:
        store_database_connection(
            db,
            FullDatabaseConnection(
                connection_name=f"db_{tag}",
                database_type="postgresql",
                username="u",
                password=SecretStr(f"{CANARY}-db-password"),
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
                aws_secret_access_key=SecretStr(f"{CANARY}-aws-secret"),
                aws_session_token=SecretStr(f"{CANARY}-aws-session"),
                azure_account_key=SecretStr(f"{CANARY}-azure-key"),
                azure_client_secret=SecretStr(f"{CANARY}-azure-client"),
                azure_sas_token=SecretStr(f"{CANARY}-azure-sas"),
                gcs_service_account_key=SecretStr(f"{CANARY}-gcs-key"),
                aws_region="us-east-1",
            ),
            OWNER,
        )
        store_secret(db, SecretInput(name=f"standalone_{tag}", value=SecretStr(f"{CANARY}-standalone")), OWNER)


def test_export_names_every_reference_and_carries_no_secret_or_row_id(tmp_path):
    tag = uuid.uuid4().hex[:8]
    try:
        _store_canary_secrets(tag)
        schema_id, schema_name = _namespace(tag)
        table_id = _table(f"orders_{tag}", schema_id, [1, 2, 3])
        child = _manual_flow(tmp_path, f"child_{tag}", 870001, schema_id)
        flow_path = _reader_flow(
            tmp_path,
            f"refs_{tag}",
            870002,
            schema_id,
            [
                {"catalog_table_id": table_id},
                {"catalog_table_name": f"orders_{tag}", "catalog_namespace_id": schema_id},
            ],
        )
        _append_nodes(
            flow_path,
            [
                (
                    3,
                    "catalog_writer",
                    {"catalog_write_settings": {"table_name": f"out_{tag}", "namespace_id": schema_id}},
                ),
                (4, "train_model", {"train_input": {"model_name": f"m_{tag}", "namespace_id": schema_id}}),
                (5, "apply_model", {"apply_input": {"model_name": f"m_{tag}", "namespace_id": schema_id}}),
                (6, "run_flow", {"flow_reference": {"registration_id": child.id}}),
            ],
        )
        refs = _register(flow_path, f"refs_{tag}", schema_id)
        root = tmp_path / "project"
        project_sync.init_project(str(root), "Names", OWNER)
        tree = _snapshot(root)
        blob = b"\n".join(tree.values())
        assert CANARY.encode() not in blob, "a secret value reached a project file"
        assert b"$ffsec$" not in blob, "a ciphertext reached a project file"

        for rel, content in tree.items():
            if not rel.startswith("flows/"):
                continue
            for path, value in _walk(yaml.safe_load(content)):
                if isinstance(value, dict):
                    for key in _ID_FIELDS & set(value):
                        assert value[key] is None, f"{rel} {path}.{key} holds row id {value[key]!r}"
                    if "registration_id" in value:
                        assert value["registration_id"] == 0, f"{rel} {path} holds a registration id"

        doc = _flow_doc(root, refs.flow_uuid)
        full_table = f"{schema_name}.orders_{tag}"
        assert _node_settings(doc, 1)["catalog_full_table_name"] == full_table
        assert _node_settings(doc, 2)["catalog_full_table_name"] == full_table
        assert _node_settings(doc, 3)["catalog_write_settings"]["namespace_full_name"] == schema_name
        assert _node_settings(doc, 4)["train_input"]["namespace_full_name"] == schema_name
        assert _node_settings(doc, 5)["apply_input"]["namespace_full_name"] == schema_name
        assert _node_settings(doc, 6)["flow_reference"] == {
            "registration_id": 0,
            "flow_uuid": child.flow_uuid,
            "namespace": schema_name,
            "name": f"child_{tag}",
        }

        write_export(root, OWNER)
        assert _snapshot(root) == tree, "a second export changed a file"
    finally:
        project_sync.close_project(OWNER)
        _cleanup(tag)


def test_writer_of_a_renamed_schema_names_the_schema_its_id_resolves_to(tmp_path):
    tag = uuid.uuid4().hex[:8]
    try:
        schema_id, old_name = _namespace(tag)
        flow_path = _reader_flow(tmp_path, f"writer_{tag}", 870021, schema_id, [])
        stored = {"table_name": f"out_{tag}", "namespace_id": schema_id, "namespace_full_name": old_name}
        _append_nodes(
            flow_path,
            [
                (1, "catalog_writer", {"catalog_write_settings": dict(stored)}),
                (2, "train_model", {"train_input": {"model_name": f"m_{tag}", **stored}}),
                (3, "apply_model", {"apply_input": {"model_name": f"m_{tag}", **stored}}),
            ],
        )
        reg = _register(flow_path, f"writer_{tag}", schema_id)
        with get_db_context() as db:
            renamed = CatalogService(SQLAlchemyCatalogRepository(db)).update_namespace(
                schema_id, name=f"NamesRenamed_{tag}"
            )
            new_name = f"{old_name.split('.')[0]}.{renamed.name}"
        root = tmp_path / "project"
        project_sync.init_project(str(root), "Renamed", OWNER)
        doc = _flow_doc(root, reg.flow_uuid)
        for node_id, key in ((1, "catalog_write_settings"), (2, "train_input"), (3, "apply_input")):
            settings = _node_settings(doc, node_id)[key]
            assert settings["namespace_full_name"] == new_name, (node_id, settings)
            assert settings["namespace_id"] is None
    finally:
        project_sync.close_project(OWNER)
        _cleanup(tag)


@pytest.mark.skipif(os.environ.get("SKIP_WORKER_TESTS") == "1", reason="runs a flow on the worker")
def test_reader_reads_the_named_table_when_row_ids_shift(tmp_path):
    tag = uuid.uuid4().hex[:8]
    try:
        schema_id, _ = _namespace(tag)
        decoy_schema_id, _ = _namespace(tag, prefix="Decoy")
        old_id = _table(f"orders_{tag}", schema_id, [1, 2, 3])
        flow_path = _reader_flow(tmp_path, f"reader_{tag}", 870011, schema_id, [{"catalog_table_id": old_id}])
        reg = _register(flow_path, f"reader_{tag}", schema_id)
        root = tmp_path / "project"
        project_sync.init_project(str(root), "Shifted", OWNER)
        project_sync.close_project(OWNER)

        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            svc.delete_flow(reg.id, delete_file=True)
            svc.delete_table(old_id, delete_file=False)
        _table(f"decoy_{tag}", decoy_schema_id, [99], table_id=old_id)
        new_id = _table(f"orders_{tag}", schema_id, [1, 2, 3])
        assert new_id != old_id

        project_sync.open_project(str(root), OWNER)
        with get_db_context() as db:
            imported = db.query(FlowRegistration).filter_by(flow_uuid=reg.flow_uuid).one()
            imported_path = imported.flow_path
        graph = open_flow(Path(imported_path))
        run = graph.run_graph()
        assert run.success, [step.error for step in run.node_step_result if not step.success]
        rows = graph.get_node(1).get_resulting_data().collect()
        assert sorted(rows["v"].to_list()) == [1, 2, 3]
    finally:
        project_sync.close_project(OWNER)
        _cleanup(tag)
