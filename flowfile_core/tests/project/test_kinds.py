"""The kinds a project holds are one registry, and the export it drives is the tree the export wrote before.

An install holding every kind, with fixed names and identifiers, is built for a user of its own. Its tree, as
the export wrote it into an empty folder before the registry existed, is the fixture
``fixtures/every_kind_tree.json``: the export writes it byte for byte, ``kinds.snapshot`` returns it without the
disk, and a second export over it changes nothing. The values the registry now derives (the managed pathspecs,
the change-summary labels, the caps, the importer's file kinds) are pinned to what they were, and a file in a
kind's folder that no pattern matches is left alone by the export, status and a save.
"""

import json
import shutil
from pathlib import Path

import pytest
import yaml
from pydantic import SecretStr

from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
from flowfile_core.catalog.services import notebook_store
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import (
    CatalogDashboard,
    CatalogNamespace,
    CatalogNotebook,
    CatalogTable,
    CatalogVisualization,
    CloudStorageConnection,
    DatabaseConnection,
    FlowRegistration,
    FlowSchedule,
    GlobalArtifact,
    Kernel,
    Secret,
    User,
)
from flowfile_core.flowfile.database_connection_manager.db_connections import (
    store_cloud_connection,
    store_database_connection,
)
from flowfile_core.project import git_ops, importer
from flowfile_core.project.service import write_export
from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection
from flowfile_core.schemas.input_schema import FullDatabaseConnection
from flowfile_core.secret_manager.secret_manager import SecretInput, store_secret
from shared.storage_config import storage

FIXTURE = Path(__file__).parent / "fixtures" / "every_kind_tree.json"
USERNAME = "project_kinds_owner"
CATALOG, SCHEMA = "KindsCatalog", "KindsSchema"
FLOW_A, FLOW_B = "6b1d0c2e-0000-4000-8000-00000000000a", "6b1d0c2e-0000-4000-8000-00000000000b"
SCHEDULE_INTERVAL, SCHEDULE_CRON = "5c4e1a7f-0000-4000-8000-000000000001", "5c4e1a7f-0000-4000-8000-000000000002"
KERNEL_ID = "project-kinds-kernel"
VIZ_UUID = "7a2f3b4c-0000-4000-8000-000000000001"
DASHBOARD_UUID = "8b3e4c5d-0000-4000-8000-000000000001"
NOTEBOOK_UUID = "9c4f5d6e-0000-4000-8000-000000000001"
TABLE_DIR = "project_kinds_managed"


@pytest.fixture(autouse=True)
def _require_git():
    if not git_ops.git_available():
        pytest.skip("git not available")


def _flow_file(path: Path, name: str, nodes: list[dict]) -> None:
    data = {
        "flowfile_version": "0.0.0",
        "flowfile_id": 1,
        "flowfile_name": name,
        "flowfile_settings": {"execution_mode": "Development", "source_registration_id": 42},
        "nodes": nodes,
    }
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")


def drop_owner() -> None:
    """Remove the test user and everything it owns, so a run starts and ends with none of it."""
    with get_db_context() as db:
        user = db.query(User).filter_by(username=USERNAME).first()
        if user is not None:
            uid = user.id
            regs = [r.id for r in db.query(FlowRegistration).filter_by(owner_id=uid)]
            db.query(CatalogDashboard).filter_by(created_by=uid).delete(synchronize_session=False)
            db.query(CatalogVisualization).filter_by(created_by=uid).delete(synchronize_session=False)
            if regs:
                db.query(FlowSchedule).filter(FlowSchedule.registration_id.in_(regs)).delete(synchronize_session=False)
            db.query(GlobalArtifact).filter_by(owner_id=uid).delete(synchronize_session=False)
            db.query(CatalogTable).filter_by(owner_id=uid).delete(synchronize_session=False)
            for nb in db.query(CatalogNotebook).filter_by(owner_id=uid).all():
                notebook_store.delete_notebook_file(uid, nb.notebook_uuid)
                db.delete(nb)
            db.query(FlowRegistration).filter_by(owner_id=uid).delete(synchronize_session=False)
            db.query(DatabaseConnection).filter_by(user_id=uid).delete(synchronize_session=False)
            db.query(CloudStorageConnection).filter_by(user_id=uid).delete(synchronize_session=False)
            db.query(Secret).filter_by(user_id=uid).delete(synchronize_session=False)
            db.query(Kernel).filter_by(user_id=uid).delete(synchronize_session=False)
            db.commit()
            for ns in db.query(CatalogNamespace).filter_by(owner_id=uid).order_by(CatalogNamespace.level.desc()).all():
                db.delete(ns)
                db.commit()
            db.delete(user)
            db.commit()
    shutil.rmtree(storage.catalog_tables_directory / TABLE_DIR, ignore_errors=True)


def build_every_kind(tmp_path: Path) -> int:
    """One or more objects of every kind, with fixed names and identifiers; returns the owner's id."""
    import polars as pl

    drop_owner()
    with get_db_context() as db:
        user = User(username=USERNAME, email=f"{USERNAME}@flowfile.app", hashed_password="x")
        db.add(user)
        db.commit()
        uid = user.id
        store_database_connection(
            db,
            FullDatabaseConnection(
                connection_name="kinds_db",
                database_type="postgresql",
                username="u",
                password=SecretStr("db-pass"),
                host="localhost",
                port=5432,
                database="d",
            ),
            uid,
        )
        store_cloud_connection(
            db,
            FullCloudStorageConnection(
                connection_name="kinds_cloud",
                storage_type="s3",
                auth_method="access_key",
                aws_access_key_id="AKIA",
                aws_secret_access_key=SecretStr("cloud-pass"),
                aws_region="us-east-1",
            ),
            uid,
        )
        store_secret(db, SecretInput(name="kinds_standalone", value=SecretStr("v")), uid)
        svc = CatalogService(SQLAlchemyCatalogRepository(db))
        catalog = svc.create_namespace(name=CATALOG, owner_id=uid, description="every kind")
        schema_id = svc.create_namespace(name=SCHEMA, owner_id=uid, parent_id=catalog.id).id

    table_dir = storage.catalog_tables_directory / TABLE_DIR
    pl.DataFrame({"id": [1, 2]}).write_delta(str(table_dir), mode="overwrite")
    with get_db_context() as db:
        table_id = (
            CatalogService(SQLAlchemyCatalogRepository(db))
            .register_table_from_data(
                name="kinds_table",
                table_path=str(table_dir),
                owner_id=uid,
                namespace_id=schema_id,
                storage_format="delta",
                schema=[{"name": "id", "dtype": "Int64"}],
                row_count=2,
                column_count=1,
                size_bytes=1,
            )
            .id
        )

    path_a, path_b = tmp_path / "flow_a.yaml", tmp_path / "flow_b.yaml"
    reader = {"id": 1, "type": "catalog_reader", "setting_input": {"catalog_table_id": table_id}}
    _flow_file(path_a, "flow_a", [reader])
    with get_db_context() as db:
        reg_a = FlowRegistration(
            flow_uuid=FLOW_A, name="flow_a", flow_path=str(path_a), owner_id=uid, namespace_id=schema_id
        )
        db.add(reg_a)
        db.commit()
        reg_a_id = reg_a.id
    run_flow = {"id": 1, "type": "run_flow", "setting_input": {"flow_reference": {"registration_id": reg_a_id}}}
    _flow_file(path_b, "flow_b", [run_flow])
    with get_db_context() as db:
        db.add(FlowRegistration(flow_uuid=FLOW_B, name="flow_b", flow_path=str(path_b), owner_id=uid))
        db.add(
            FlowSchedule(
                schedule_uuid=SCHEDULE_INTERVAL,
                registration_id=reg_a_id,
                owner_id=uid,
                schedule_type="interval",
                interval_seconds=3600,
                name="hourly",
            )
        )
        db.add(
            FlowSchedule(
                schedule_uuid=SCHEDULE_CRON,
                registration_id=reg_a_id,
                owner_id=uid,
                schedule_type="cron",
                cron_expression="0 6 * * *",
                cron_timezone="Europe/Amsterdam",
                enabled=False,
            )
        )
        view = CatalogTable(
            name="kinds_view",
            owner_id=uid,
            namespace_id=schema_id,
            storage_format="delta",
            table_type="virtual",
            sql_query="SELECT 1 AS id",
            schema_json=json.dumps([{"name": "id", "dtype": "Int64"}]),
            column_count=1,
            source_registration_id=reg_a_id,
        )
        db.add(view)
        db.add(
            GlobalArtifact(
                name="kinds_model",
                namespace_id=schema_id,
                version=1,
                owner_id=uid,
                source_registration_id=reg_a_id,
                serialization_format="pickle",
                status="active",
                tags='["a"]',
            )
        )
        db.add(Kernel(id=KERNEL_ID, name="kinds_kernel", user_id=uid, packages='["polars"]', resolved_packages="[]"))
        db.commit()
        viz = CatalogVisualization(
            viz_uuid=VIZ_UUID,
            name="kinds_viz",
            chart_type="bar",
            spec_json="[]",
            source_type="table",
            catalog_table_id=view.id,
            namespace_id=schema_id,
            created_by=uid,
        )
        db.add(viz)
        db.commit()
        tile = {"id": "t1", "type": "viz", "viz_id": viz.id, "chart_index": 0, "x": 0, "y": 0, "w": 4, "h": 4}
        flt = {"id": "f1", "column": "id", "datasource_id": table_id}
        layout = {"tiles": [tile], "filters": [flt], "grid": {"cols": 48, "row_height": 40, "version": 2}}
        db.add(
            CatalogDashboard(
                dashboard_uuid=DASHBOARD_UUID,
                name="kinds_dashboard",
                namespace_id=schema_id,
                layout_json=json.dumps(layout),
                created_by=uid,
            )
        )
        db.add(
            CatalogNotebook(
                notebook_uuid=NOTEBOOK_UUID,
                name="kinds_notebook",
                owner_id=uid,
                namespace_id=schema_id,
                default_kernel_id=KERNEL_ID,
            )
        )
        db.commit()
    notebook_store.write_notebook_file(
        uid,
        NOTEBOOK_UUID,
        name="kinds_notebook",
        description=None,
        namespace_name=SCHEMA,
        default_kernel_id=KERNEL_ID,
        cells=[],
    )
    return uid


def _on_disk(root: Path) -> dict[str, str]:
    return {
        p.relative_to(root).as_posix(): p.read_text(encoding="utf-8")
        for p in sorted(root.rglob("*"))
        if p.is_file() and ".git" not in p.relative_to(root).parts
    }


def _project_all(root: Path, owner_id: int) -> dict[str, str]:
    write_export(root, owner_id)
    return _on_disk(root)


@pytest.fixture
def every_kind(tmp_path):
    owner_id = build_every_kind(tmp_path)
    try:
        yield owner_id
    finally:
        drop_owner()


def test_the_export_writes_the_recorded_tree(tmp_path, every_kind):
    expected = json.loads(FIXTURE.read_text(encoding="utf-8"))
    root = tmp_path / "project"
    assert _project_all(root, every_kind) == expected

    from flowfile_core.project import kinds

    with get_db_context() as db:
        everything = frozenset(k.name for k in kinds.registered())
        tree = kinds.snapshot(kinds.ExportContext(db=db, owner_id=every_kind, kinds=everything))
    assert tree == expected

    before = {p: (root / p).read_bytes() for p in expected}
    assert _project_all(root, every_kind) == expected
    assert {p: (root / p).read_bytes() for p in expected} == before


def test_data_artifact_kinds_leave_the_tree_when_not_tracked(tmp_path, every_kind):
    from flowfile_core.project import kinds

    root = tmp_path / "project"
    _project_all(root, every_kind)
    assert (root / "tables.yaml").exists() and (root / "models.yaml").exists()
    with get_db_context() as db:
        untracked = frozenset(k.name for k in kinds.registered()) - {"tables", "models"}
        tree = kinds.snapshot(kinds.ExportContext(db=db, owner_id=every_kind, kinds=untracked))
    assert "tables.yaml" not in tree and "models.yaml" not in tree
    kinds.write_tree(root, tree)
    assert not (root / "tables.yaml").exists() and not (root / "models.yaml").exists()
    assert _on_disk(root) == tree


def test_the_registry_holds_the_twelve_kinds_in_import_order():
    from flowfile_core.project import kinds

    assert [k.name for k in kinds.ordered()] == [
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
    patterns = [p for k in kinds.registered() for p in k.paths]
    assert len(patterns) == len(set(patterns))
    for k in kinds.registered():
        for pattern in k.paths:
            sample = pattern.replace("*", "x")
            assert [o.name for o in kinds.registered() if kinds.kind_for_path(sample)[0] is o] == [k.name]
    assert {k.name for k in kinds.registered() if k.tracked_by_data_flag} == {"tables", "models"}


def test_derived_values_are_what_they_were():
    from flowfile_core.project import kinds
    from flowfile_core.project.service import _classify_path

    assert git_ops._managed_pathspecs() == [
        ":(literal)project.yaml",
        ":(literal).gitignore",
        ":(literal)secrets.yaml",
        ":(literal)namespaces.yaml",
        ":(literal)tables.yaml",
        ":(literal)models.yaml",
        ":(literal)kernels.yaml",
        ":(literal)visualizations.yaml",
        ":(literal)dashboards.yaml",
        ":(glob)connections/database/*.yaml",
        ":(glob)connections/cloud/*.yaml",
        ":(glob)flows/*.flow.yaml",
        ":(glob)schedules/*.yaml",
        ":(glob)notebooks/*.notebook.yaml",
    ]
    cases = {
        "flows/sales.flow.yaml": (("flow", "sales"), "flow"),
        "schedules/sales.yaml": (("schedule", "sales"), "schedule"),
        "connections/database/prod.yaml": (("database connection", "prod"), "database_connection"),
        "connections/cloud/lake.yaml": (("cloud connection", "lake"), "cloud_connection"),
        "notebooks/eda.notebook.yaml": (("notebook", "eda"), "notebook"),
        "secrets.yaml": (("secrets", "secrets"), "secrets"),
        "namespaces.yaml": (("namespaces", "namespaces"), "namespaces"),
        "tables.yaml": (("catalog tables", "tables"), "tables"),
        "models.yaml": (("models", "models"), "models"),
        "kernels.yaml": (("kernels", "kernels"), "kernels"),
        "visualizations.yaml": (("visualizations", "visualizations"), "visualizations"),
        "dashboards.yaml": (("dashboards", "dashboards"), "dashboards"),
        "project.yaml": (("settings", "project.yaml"), "project"),
        ".gitignore": (("settings", ".gitignore"), None),
        "README.md": (("settings", "README.md"), None),
    }
    for path, (summary, file_kind) in cases.items():
        assert _classify_path(path) == summary, path
        assert importer._file_kind(path) == file_kind, path

    caps = {k.name: k.cap for k in kinds.registered()}
    assert caps == {
        "secrets": None,
        "database_connection": 200,
        "cloud_connection": 200,
        "namespaces": 200,
        "flow": 500,
        "schedule": 500,
        "tables": 500,
        "models": 500,
        "kernels": 50,
        "visualizations": 500,
        "dashboards": 200,
        "notebook": 500,
    }


def test_a_file_no_pattern_matches_is_left_alone(tmp_path, every_kind):
    from flowfile_core.project import kinds

    root = tmp_path / "project"
    _project_all(root, every_kind)
    git_ops.init(root)
    git_ops.commit_all(root, "first")
    strays = {"flows/notes.yaml": "a note\n", "notebooks/x.yaml": "not a notebook\n"}
    for rel, text in strays.items():
        (root / rel).write_text(text, encoding="utf-8")
        assert importer._file_kind(rel) is None
        assert kinds.kind_for_path(rel) is None

    tree = _project_all(root, every_kind)
    for rel, text in strays.items():
        assert tree[rel] == text
    with get_db_context() as db:
        assert not set(strays) & set(kinds.snapshot(kinds.ExportContext(db=db, owner_id=every_kind)))
    assert not git_ops.is_dirty(root)
    assert git_ops.uncommitted_changes(root) == []
    (root / "tables.yaml").write_text("tables: []\n", encoding="utf-8")
    git_ops.commit_all(root, "second")
    committed = set(git_ops.tree_blob_sizes(root, git_ops.head_sha(root)))
    assert "tables.yaml" in committed
    assert not set(strays) & committed


def test_a_rewrite_in_other_letter_case_leaves_one_file(tmp_path):
    from flowfile_core.project import kinds

    root = tmp_path / "project"
    kinds.write_tree(root, {"flows/Sales.flow.yaml": "old\n"})
    kinds.write_tree(root, {"flows/sales.flow.yaml": "new\n"})
    files = list((root / "flows").iterdir())
    assert len(files) == 1
    assert files[0].read_text(encoding="utf-8") == "new\n"
