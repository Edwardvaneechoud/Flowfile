"""Tests for CatalogReference / SchemaReference and the schema= integration."""

from __future__ import annotations

import pickle

import polars as pl
import pytest

from flowfile_core.catalog import (
    AmbiguousFlowError,
    CatalogService,
    FlowNotFoundError,
    NamespaceNotFoundError,
    SQLAlchemyCatalogRepository,
)
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import (
    CatalogNamespace,
    CatalogTable,
    FlowFavorite,
    FlowFollow,
    FlowRegistration,
    FlowRun,
)
from flowfile_frame import (
    CatalogReference,
    FlowInput,
    FlowRef,
    SchemaReference,
    default_schema,
    get_catalog,
    list_catalogs,
)
from flowfile_frame.catalog_reference import _resolve_namespace_id


def _cleanup_catalog() -> None:
    with get_db_context() as db:
        db.query(CatalogTable).delete()
        db.query(FlowFollow).delete()
        db.query(FlowFavorite).delete()
        db.query(FlowRun).delete()
        db.query(FlowRegistration).delete()
        db.query(CatalogNamespace).delete()
        db.commit()


@pytest.fixture(autouse=True)
def _clean_catalog():
    _cleanup_catalog()
    yield
    _cleanup_catalog()


def _seed_catalog(name: str = "TestCat") -> int:
    """Seed a catalog directly via the service; return its id."""
    with get_db_context() as db:
        service = CatalogService(SQLAlchemyCatalogRepository(db))
        ns = service.create_namespace(name=name, owner_id=1, parent_id=None)
        return ns.id


def _seed_schema(catalog_id: int, name: str = "raw") -> int:
    with get_db_context() as db:
        service = CatalogService(SQLAlchemyCatalogRepository(db))
        ns = service.create_namespace(name=name, owner_id=1, parent_id=catalog_id)
        return ns.id


class TestCatalogReferenceConstruction:
    def test_existing_catalog_resolves_to_id(self):
        cat_id = _seed_catalog("Sales")
        ref = CatalogReference("Sales")
        assert ref.name == "Sales"
        assert ref.id == cat_id

    def test_missing_catalog_raises(self):
        with pytest.raises(NamespaceNotFoundError):
            CatalogReference("NonExistent")

    def test_auto_create_creates_when_missing(self):
        ref = CatalogReference("Brand New", auto_create=True)
        assert ref.name == "Brand New"
        assert ref.id > 0

    def test_auto_create_is_idempotent(self):
        first = CatalogReference("Idem", auto_create=True)
        second = CatalogReference("Idem", auto_create=True)
        assert first == second
        assert first.id == second.id

    def test_auto_create_with_description_only_on_create(self):
        first = CatalogReference("Desc", auto_create=True, description="initial")
        second = CatalogReference("Desc", auto_create=True, description="ignored on hit")
        assert first.id == second.id
        with get_db_context() as db:
            ns = db.query(CatalogNamespace).filter_by(id=first.id).first()
            assert ns is not None
            assert ns.description == "initial"

    def test_dot_in_name_rejected(self):
        with pytest.raises(ValueError, match="cannot contain"):
            CatalogReference("bad.name", auto_create=True)


class TestCatalogReferenceImmutability:
    def test_setattr_raises(self):
        _seed_catalog("Frozen")
        ref = CatalogReference("Frozen")
        with pytest.raises(AttributeError):
            ref.name = "Other"  # type: ignore[misc]
        with pytest.raises(AttributeError):
            ref.id = 999  # type: ignore[misc]

    def test_delattr_raises(self):
        _seed_catalog("Frozen2")
        ref = CatalogReference("Frozen2")
        with pytest.raises(AttributeError):
            del ref.name  # type: ignore[misc]

    def test_hashable_and_eq(self):
        cat_id = _seed_catalog("Hash")
        a = CatalogReference("Hash")
        b = CatalogReference("Hash")
        assert a == b
        assert hash(a) == hash(b)
        assert {a, b} == {a}
        assert a.id == cat_id

    def test_repr_includes_name_and_id(self):
        _seed_catalog("ReprMe")
        ref = CatalogReference("ReprMe")
        text = repr(ref)
        assert "ReprMe" in text
        assert f"id={ref.id}" in text

    def test_pickle_roundtrip(self):
        _seed_catalog("Pickle")
        ref = CatalogReference("Pickle")
        revived = pickle.loads(pickle.dumps(ref))
        assert revived == ref
        assert revived.name == "Pickle"
        assert revived.id == ref.id


class TestSchemaReferenceConstruction:
    def test_via_catalog_method(self):
        cat = CatalogReference("CatA", auto_create=True)
        schema = cat.schema("raw", auto_create=True)
        assert isinstance(schema, SchemaReference)
        assert schema.catalog == cat
        assert schema.name == "raw"
        assert schema.id > 0

    def test_via_direct_constructor(self):
        cat = CatalogReference("CatB", auto_create=True)
        schema = SchemaReference(cat, "raw", auto_create=True)
        assert schema.catalog == cat
        assert schema.name == "raw"

    def test_missing_schema_raises(self):
        cat = CatalogReference("CatC", auto_create=True)
        with pytest.raises(NamespaceNotFoundError):
            cat.schema("missing")

    def test_two_catalogs_can_have_same_schema_name(self):
        cat1 = CatalogReference("Cat1", auto_create=True)
        cat2 = CatalogReference("Cat2", auto_create=True)
        s1 = cat1.schema("raw", auto_create=True)
        s2 = cat2.schema("raw", auto_create=True)
        assert s1.id != s2.id
        assert s1 != s2

    def test_pickle_roundtrip(self):
        cat = CatalogReference("PickCat", auto_create=True)
        schema = cat.schema("ps", auto_create=True)
        revived = pickle.loads(pickle.dumps(schema))
        assert revived == schema
        assert revived.catalog == cat
        assert revived.name == "ps"


class TestListOperations:
    def test_list_catalogs(self):
        _seed_catalog("Alpha")
        _seed_catalog("Beta")
        catalogs = list_catalogs()
        names = {c.name for c in catalogs}
        assert {"Alpha", "Beta"} <= names
        assert all(isinstance(c, CatalogReference) for c in catalogs)

    def test_list_schemas(self):
        cat_id = _seed_catalog("Listy")
        _seed_schema(cat_id, "s1")
        _seed_schema(cat_id, "s2")
        cat = CatalogReference("Listy")
        schemas = cat.list_schemas()
        names = {s.name for s in schemas}
        assert names == {"s1", "s2"}
        assert all(isinstance(s, SchemaReference) for s in schemas)
        assert all(s.catalog == cat for s in schemas)

    def test_catalog_list_tables_is_flat_across_schemas(self):
        cat = CatalogReference("FlatCat", auto_create=True)
        cat.schema("s1", auto_create=True)
        cat.schema("s2", auto_create=True)
        tables = cat.list_tables()
        assert tables == []

    def test_schema_list_tables_empty_when_none(self):
        cat = CatalogReference("EmptyCat", auto_create=True)
        schema = cat.schema("only", auto_create=True)
        assert schema.list_tables() == []


class TestNamespaceIdResolution:
    def test_schema_resolves_to_id(self):
        cat = CatalogReference("ResCat", auto_create=True)
        schema = cat.schema("res", auto_create=True)
        assert _resolve_namespace_id(schema, None) == schema.id

    def test_explicit_namespace_id_passes_through(self):
        assert _resolve_namespace_id(None, 42) == 42

    def test_neither_returns_none(self):
        assert _resolve_namespace_id(None, None) is None

    def test_both_raises(self):
        cat = CatalogReference("ConflictCat", auto_create=True)
        schema = cat.schema("c", auto_create=True)
        with pytest.raises(ValueError, match="Pass either"):
            _resolve_namespace_id(schema, schema.id)


class TestDefaultSchema:
    def test_raises_when_not_initialized(self):
        with pytest.raises(LookupError):
            default_schema()

    def test_returns_general_default_when_seeded(self):
        general_id = _seed_catalog("General")
        default_id = _seed_schema(general_id, "default")
        schema = default_schema()
        assert schema.name == "default"
        assert schema.id == default_id
        assert schema.catalog.name == "General"
        assert schema.catalog.id == general_id


def _child_flow():
    """A registrable child flow: one input passed straight to one output."""
    frame = FlowInput("orders", schema={"id": pl.Int64})
    return frame.to_flow_output("orders_out")


class TestCatalogNavigation:
    def test_get_catalog_resolves_eagerly(self):
        cat_id = _seed_catalog("NavCat")
        assert get_catalog("NavCat") == CatalogReference("NavCat")
        assert get_catalog("NavCat").id == cat_id
        with pytest.raises(NamespaceNotFoundError):
            get_catalog("NoSuchCatalog")

    def test_get_schema_is_an_alias_of_schema(self):
        cat = CatalogReference("AliasCat", auto_create=True)
        created = cat.get_schema("s", auto_create=True)
        assert created == cat.schema("s")
        assert isinstance(created, SchemaReference)
        with pytest.raises(NamespaceNotFoundError):
            cat.get_schema("missing")

    def test_register_get_and_list_flows(self):
        schema = CatalogReference("FlowCat", auto_create=True).schema("sales", auto_create=True)
        first = schema.register_flow(_child_flow(), name="Clean orders")
        second = schema.register_flow(_child_flow(), name="Archive orders")
        assert isinstance(first, FlowRef)
        assert get_catalog("FlowCat").get_schema("sales").get_flow("Clean orders") == first
        assert schema.list_flows() == [second, first]
        assert CatalogReference("FlowCat").schema("sales", auto_create=True).list_flows() == [second, first]
        with pytest.raises(FlowNotFoundError):
            schema.get_flow("Missing flow")

    def test_flow_ref_keeps_its_schema_handle(self):
        schema = CatalogReference("HandleCat", auto_create=True).schema("raw", auto_create=True)
        ref = schema.get_flow(schema.register_flow(_child_flow(), name="Loader").name)
        assert ref.schema == schema
        assert ref.schema.catalog == schema.catalog
        assert ref.namespace_full_name == "HandleCat.raw"
        assert ref.schema.list_tables() == []
        revived = pickle.loads(pickle.dumps(ref))
        assert revived == ref and revived.schema == schema and revived.flow_path == ref.flow_path

    def test_duplicate_name_in_one_schema_is_ambiguous(self):
        schema = CatalogReference("DupCat", auto_create=True).schema("raw", auto_create=True)
        ref = schema.register_flow(_child_flow(), name="Twice")
        with get_db_context() as db:
            copy = FlowRegistration(name="Twice", flow_path=f"{ref.flow_path}.copy", namespace_id=schema.id, owner_id=1)
            db.add(copy)
            db.commit()
        with pytest.raises(AmbiguousFlowError) as info:
            schema.get_flow("Twice")
        assert len(info.value.candidates) == 2
        assert {c["namespace_name"] for c in info.value.candidates} == {"DupCat.raw"}
