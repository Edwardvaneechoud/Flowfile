"""End-to-end tests for flowfile_frame's catalog integration.

Exercises the "Python + catalog as a first-class citizen" path:
- Auto-registration of flowfile_frame scripts as FlowRegistration rows
- Physical and virtual catalog writes with lineage (source_registration_id)
- Namespace management (create/get/list) by name from Python
- Var-name inference for table_name and flow name
- FlowRun recording via FlowFrame.execute()
"""

from __future__ import annotations

import os

os.environ["TESTING"] = "True"

import pytest

import flowfile_frame as ff
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.init_db import create_default_catalog_namespace
from flowfile_core.database.models import (
    CatalogNamespace,
    CatalogTable,
    CatalogTableReadLink,
    FlowRegistration,
    FlowRun,
    FlowSchedule,
)

# --- helpers ----------------------------------------------------------------


def _catalog_cleanup() -> None:
    with get_db_context() as db:
        db.query(CatalogTableReadLink).delete()
        db.query(FlowSchedule).delete()
        db.query(FlowRun).delete()
        db.query(CatalogTable).delete()
        db.query(FlowRegistration).delete()
        db.query(CatalogNamespace).delete()
        db.commit()
        # Re-bootstrap the 'General' catalog + 'Local Flows' / 'Unnamed Flows'
        # schemas that auto_register_flow depends on.
        create_default_catalog_namespace(db)


@pytest.fixture(autouse=True)
def _clean():
    _catalog_cleanup()
    yield
    _catalog_cleanup()


# --- tests ------------------------------------------------------------------


def test_explicit_context_physical_and_virtual_write(tmp_path):
    """catalog_context with explicit name; physical + virtual writes land with lineage."""
    with ff.catalog_context(name="test_etl", flow_path=str(tmp_path / "etl.py")):
        ff.create_namespace("Test")
        schema_id = ff.create_namespace("stg", parent="Test")

        df = ff.from_dict({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        df.write_catalog_table("customers", namespace="Test.stg")

        customers = ff.read_catalog_table("customers", namespace="Test.stg")
        big = customers.filter(ff.col("a") > 1)
        big.write_catalog_table("customers_big", namespace="Test.stg", write_mode="virtual")
        big.execute()

    with get_db_context() as db:
        regs = db.query(FlowRegistration).filter_by(name="test_etl").all()
        assert len(regs) == 1
        reg_id = regs[0].id

        runs = db.query(FlowRun).filter_by(registration_id=reg_id).all()
        assert len(runs) >= 1
        assert runs[-1].run_type == "flowfile_frame_script"
        assert runs[-1].success is True

        physical = db.query(CatalogTable).filter_by(name="customers", namespace_id=schema_id).one()
        assert physical.table_type == "physical"
        assert physical.source_registration_id == reg_id

        virtual = db.query(CatalogTable).filter_by(name="customers_big", namespace_id=schema_id).one()
        assert virtual.table_type == "virtual"
        # Virtual writes record the producing registration on producer_registration_id
        assert virtual.producer_registration_id == reg_id


def test_var_name_inference_for_table_and_flow():
    """Without explicit names, receiver var name drives both table name and flow name."""
    ff.create_namespace("InferCat")
    ff.create_namespace("schema", parent="InferCat")

    orders = ff.from_dict({"id": [1, 2], "total": [10, 20]})
    orders.write_catalog_table(namespace="InferCat.schema")
    orders.execute()

    with get_db_context() as db:
        tables = db.query(CatalogTable).filter_by(name="orders").all()
        assert len(tables) == 1
        reg = db.query(FlowRegistration).filter_by(id=tables[0].source_registration_id).one()
        assert reg.name == "orders"
        runs = db.query(FlowRun).filter_by(registration_id=reg.id).all()
        assert any(r.success for r in runs)


def test_missing_table_name_with_chained_call_raises():
    """Chained call has no receiver var — should raise an actionable error."""
    ff.create_namespace("ChainCat")
    ff.create_namespace("schema", parent="ChainCat")

    with pytest.raises(ValueError, match="table_name"):
        ff.from_dict({"a": [1]}).write_catalog_table(namespace="ChainCat.schema")


def test_missing_table_name_with_blocklisted_var_raises():
    """Short/generic var names like 'df' should be rejected with a clear error."""
    ff.create_namespace("BlockCat")
    ff.create_namespace("schema", parent="BlockCat")

    df = ff.from_dict({"a": [1]})
    with pytest.raises(ValueError, match="table_name"):
        df.write_catalog_table(namespace="BlockCat.schema")


def test_backwards_compat_namespace_id_int(tmp_path):
    """Legacy namespace_id: int path still works."""
    with ff.catalog_context(name="compat", flow_path=str(tmp_path / "c.py")):
        ff.create_namespace("Raw")
        schema_id = ff.create_namespace("data", parent="Raw")
        df = ff.from_dict({"x": [1]})
        df.write_catalog_table("t", namespace_id=schema_id)
        df.execute()

    with get_db_context() as db:
        assert db.query(CatalogTable).filter_by(name="t", namespace_id=schema_id).count() == 1


def test_create_list_and_get_namespace():
    cat_id = ff.create_namespace("Cat1")
    schema_id = ff.create_namespace("s1", parent="Cat1")
    assert ff.get_namespace("Cat1.s1").id == schema_id
    assert ff.get_namespace(schema_id).name == "s1"

    cats = ff.list_namespaces()
    assert any(ns.id == cat_id for ns in cats)

    children = ff.list_namespaces(parent="Cat1")
    assert [ns.id for ns in children] == [schema_id]


def test_catalog_and_schema_helpers():
    """create_catalog / create_schema / list_catalogs / list_schemas / get_schema."""
    cat_id = ff.create_catalog("Warehouse", description="prod warehouse")
    stg_id = ff.create_schema("stg", catalog="Warehouse")
    mart_id = ff.create_schema("mart", catalog="Warehouse")

    cats = ff.list_catalogs()
    assert any(c.id == cat_id and c.level == 0 for c in cats)

    schemas_scoped = ff.list_schemas(catalog="Warehouse")
    assert {s.id for s in schemas_scoped} == {stg_id, mart_id}
    assert all(s.level == 1 for s in schemas_scoped)

    schemas_all = ff.list_schemas()
    assert {stg_id, mart_id}.issubset({s.id for s in schemas_all})

    assert ff.get_schema("Warehouse.stg").id == stg_id
    assert ff.get_schema(stg_id).name == "stg"


def test_list_tables(tmp_path):
    with ff.catalog_context(name="lister", flow_path=str(tmp_path / "l.py")):
        ff.create_catalog("Lakeshore")
        ff.create_schema("raw", catalog="Lakeshore")

        customers = ff.from_dict({"a": [1, 2]})
        customers.write_catalog_table(schema="Lakeshore.raw")

        orders = ff.from_dict({"b": [3, 4]})
        orders.write_catalog_table(schema="Lakeshore.raw")

        customers.execute()

    all_tables = ff.list_tables()
    names = {t.name for t in all_tables}
    assert {"customers", "orders"}.issubset(names)

    scoped = ff.list_tables(schema="Lakeshore.raw")
    assert {t.name for t in scoped} == {"customers", "orders"}
    for t in scoped:
        assert t.namespace_name == "raw"
        assert t.source_registration_name == "lister"
