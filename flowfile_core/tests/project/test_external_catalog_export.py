"""A catalog kept elsewhere is this install's own setting and holds a sign-in: a project leaves it out, and a reload of
a project that never held it leaves it alone."""

import yaml

from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import CatalogNamespace, ExternalCatalog
from flowfile_core.project import project_sync
from flowfile_core.project.normalize import write_yaml

OWNER = 1


def _rows() -> dict:
    with get_db_context() as db:
        catalog = CatalogNamespace(name="KeptElsewhereCat", level=0, owner_id=OWNER, kept_elsewhere=True)
        own = CatalogNamespace(name="OwnCatForControl", level=0, owner_id=OWNER)
        db.add_all([catalog, own])
        db.commit()
        db.add(CatalogNamespace(name="sales", parent_id=catalog.id, level=1, owner_id=OWNER, kept_elsewhere=True))
        db.add(ExternalCatalog(namespace_id=catalog.id, kind="iceberg_rest", uri="http://127.0.0.1:9/c", sign_in="none"))
        db.commit()
        return {"catalog": catalog.id, "own": own.id}


def _drop(ids: dict) -> None:
    with get_db_context() as db:
        db.query(ExternalCatalog).filter_by(namespace_id=ids["catalog"]).delete()
        db.query(CatalogNamespace).filter(CatalogNamespace.parent_id == ids["catalog"]).delete()
        db.query(CatalogNamespace).filter(CatalogNamespace.id.in_(ids.values())).delete(synchronize_session=False)
        db.commit()


def test_a_project_export_leaves_out_a_catalog_kept_elsewhere_and_a_reload_keeps_it(tmp_path):
    project_sync.close_project(OWNER)
    ids = _rows()
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Kept Elsewhere", OWNER)
        written = yaml.safe_load((root / "namespaces.yaml").read_text())
        names = [entry["catalog"] for entry in written["namespaces"]]
        assert "OwnCatForControl" in names
        assert "KeptElsewhereCat" not in names
        assert "KeptElsewhereCat" not in (root / "namespaces.yaml").read_text()

        write_yaml(root / "namespaces.yaml", {"namespaces": []})
        project_sync.reload_from_disk(OWNER, force=True)

        with get_db_context() as db:
            control_gone = db.get(CatalogNamespace, ids["own"]) is None
            kept = db.get(CatalogNamespace, ids["catalog"])
            schemas = db.query(CatalogNamespace).filter_by(parent_id=ids["catalog"]).count()
            row = db.query(ExternalCatalog).filter_by(namespace_id=ids["catalog"]).count()
        assert control_gone
        assert kept is not None and kept.kept_elsewhere
        assert (schemas, row) == (1, 1)
    finally:
        _drop(ids)
        project_sync.close_project(OWNER)


def test_a_table_row_of_a_catalog_kept_elsewhere_is_not_exported():
    from flowfile_core.catalog.services.external_catalogs import ensure_table_row, outside_table
    from flowfile_core.database.models import CatalogTable
    from flowfile_core.project.projection import export_tables

    ids = _rows()
    row_id = None
    try:
        with get_db_context() as db:
            row = ensure_table_row(db, outside_table(db, "KeptElsewhereCat.sales.orders_kept", None, None, source="local"))
            row_id = row.id
            exported = export_tables(db, OWNER)
        assert "orders_kept" not in yaml.safe_dump(exported)
    finally:
        with get_db_context() as db:
            db.query(CatalogTable).filter(CatalogTable.id == row_id).delete()
            db.commit()
        _drop(ids)
