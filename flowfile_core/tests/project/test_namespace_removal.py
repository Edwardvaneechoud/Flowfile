"""Which namespaces a restore or reload removes.

A public namespace Flowfile seeded outside ``General`` (the demo catalog's) is removed once the apply has emptied
it, like any other. The built-in ones the catalog never deletes (``General`` and its app-managed schemas) stay and
are in no plan, whether they existed before the first version or were made on demand after it.
"""

import json

import pytest
import yaml

from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
from flowfile_core.catalog.demo_seed import _ensure_namespace
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import CatalogNamespace, CatalogTable, FlowRegistration, User
from flowfile_core.project import git_ops, importer, kinds, project_sync, service
from flowfile_core.project.normalize import write_yaml
from flowfile_core.routes.kafka import _ensure_sync_namespace
from tests.project.test_kinds import _flow_file
from tests.project.test_kinds_list import every_kind  # noqa: F401

SEEDED_CATALOG, SEEDED_SCHEMA = "SeededCatalog", "seeded_schema"
SEEDED_FLOW = "4e5f6a7b-0000-4000-8000-000000000001"
BUILT_IN_SCHEMAS = ("default", "Unnamed Flows", "Local Flows", "Python Editor", "sync")


@pytest.fixture(autouse=True)
def _require_git():
    if not git_ops.git_available():
        pytest.skip("git not available")


def _seed_public_catalog(owner_id: int, tmp_path) -> None:
    """A public catalog and schema as the demo seeder makes them, the schema holding a flow and a query table."""
    with get_db_context() as db:
        repo = SQLAlchemyCatalogRepository(db)
        catalog_service = CatalogService(repo)
        catalog = _ensure_namespace(catalog_service, repo, db, SEEDED_CATALOG, None, owner_id, "seeded")
        schema = _ensure_namespace(catalog_service, repo, db, SEEDED_SCHEMA, catalog.id, owner_id, "seeded")
        schema_id = schema.id
    path = tmp_path / "seeded_flow.yaml"
    _flow_file(path, "seeded_flow", [])
    with get_db_context() as db:
        db.add(
            FlowRegistration(
                flow_uuid=SEEDED_FLOW, name="seeded_flow", flow_path=str(path), owner_id=owner_id, namespace_id=schema_id
            )
        )
        db.add(
            CatalogTable(
                name="seeded_view",
                owner_id=owner_id,
                namespace_id=schema_id,
                storage_format="delta",
                table_type="virtual",
                sql_query="SELECT 1 AS id",
                schema_json=json.dumps([{"name": "id", "dtype": "Int64"}]),
                column_count=1,
            )
        )
        db.commit()


def _namespace_names(owner_id: int) -> set[str]:
    with get_db_context() as db:
        return {ns.name for ns in db.query(CatalogNamespace).filter_by(owner_id=owner_id)}


def _local_user_id() -> int:
    with get_db_context() as db:
        return db.query(User).filter_by(username="local_user").one().id


def _built_in_rows(db) -> dict[str, CatalogNamespace]:
    general = db.query(CatalogNamespace).filter_by(name="General", parent_id=None).one()
    rows = {"General": general}
    for name in BUILT_IN_SCHEMAS:
        rows[name] = db.query(CatalogNamespace).filter_by(name=name, parent_id=general.id).first()
    return rows


@pytest.fixture
def built_ins_made_here():
    """Delete the built-in schemas the test made, so a later test that makes its own ``General/sync`` can."""
    with get_db_context() as db:
        existing = {name for name, row in _built_in_rows(db).items() if row is not None}
    yield
    with get_db_context() as db:
        for name, row in _built_in_rows(db).items():
            if row is not None and name not in existing:
                db.delete(row)
        db.commit()


def test_a_restore_to_before_a_seeded_public_catalog_removes_it(tmp_path, every_kind):  # noqa: F811
    root = tmp_path / "project"
    project_sync.init_project(str(root), "seeded", every_kind)
    before = git_ops.head_sha(root)
    _seed_public_catalog(every_kind, tmp_path)
    project_sync.save_version(every_kind, "seeded")
    assert {SEEDED_CATALOG, SEEDED_SCHEMA} <= _namespace_names(every_kind)

    plan = project_sync.plan(every_kind, before)
    removed = {r["label"]: r["consequence"] for r in plan if r["kind"] == "namespaces" and r["change"] == "removed"}
    assert removed == {
        SEEDED_CATALOG: "it is deleted once empty",
        f"{SEEDED_CATALOG}.{SEEDED_SCHEMA}": "it is deleted once empty",
    }

    project_sync.restore_version(every_kind, before)

    assert not {SEEDED_CATALOG, SEEDED_SCHEMA} & _namespace_names(every_kind)
    with get_db_context() as db:
        assert db.query(FlowRegistration).filter_by(flow_uuid=SEEDED_FLOW).first() is None
    assert project_sync.plan(every_kind, before) == []
    restored = git_ops.read_tree_at(root, "HEAD")["namespaces.yaml"]
    assert restored == git_ops.read_tree_at(root, before)["namespaces.yaml"]


def test_a_reload_keeps_the_built_in_namespaces_and_plans_no_removal(tmp_path, built_ins_made_here):
    """The files drop ``General`` whole, as a folder from an install without ``Python Editor`` or ``sync`` drops
    those two: none of them is in the plan, and the reload leaves every one in place."""
    owner = _local_user_id()
    project_sync.close_project(owner)
    with get_db_context() as db:
        catalog_service = CatalogService(SQLAlchemyCatalogRepository(db))
        catalog_service.ensure_python_editor_flows_namespace()
        _ensure_sync_namespace(catalog_service, owner)
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "built-ins", owner)
        data = yaml.safe_load((root / "namespaces.yaml").read_text(encoding="utf-8"))
        general = next(c for c in data["namespaces"] if c["catalog"] == "General")
        assert {"default", "Unnamed Flows", "Local Flows", "Python Editor"} <= {s["name"] for s in general["schemas"]}
        data["namespaces"] = [c for c in data["namespaces"] if c["catalog"] != "General"]
        write_yaml(root / "namespaces.yaml", data)

        plan = service._plan_changes(root, owner, None)
        assert [c for c in plan if c.kind == "namespaces"] == []

        project_sync.reload_from_disk(owner, force=True)

        with get_db_context() as db:
            assert all(row is not None for row in _built_in_rows(db).values())
    finally:
        project_sync.close_project(owner)


def test_a_delete_asked_for_a_built_in_namespace_is_skipped():
    """A caller that names one anyway gets no error, so an apply never fails on a namespace the catalog keeps."""
    owner = _local_user_id()
    with get_db_context() as db:
        rows = _built_in_rows(db)
        assert rows["General"].owner_id == owner and rows["default"].owner_id == owner
        importer._delete_namespaces(kinds.ExportContext(db=db, owner_id=owner), [("General", "default"), ("General",)])
        db.commit()
    with get_db_context() as db:
        rows = _built_in_rows(db)
        assert rows["General"] is not None and rows["default"] is not None
