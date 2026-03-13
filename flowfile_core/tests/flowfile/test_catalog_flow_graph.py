"""Tests for catalog reader/writer nodes and lineage tracking within FlowGraph.

Covers:
- Catalog writer: materializes data to the catalog with correct lineage
- Catalog reader: reads catalog tables back into a flow
- _sync_catalog_read_links: records read links on save_flow
- Round-trip: write → read → verify data integrity
"""

import os
import tempfile

import polars as pl
import pytest

from flowfile_core.catalog import CatalogService
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import (
    CatalogNamespace,
    CatalogTable,
    CatalogTableReadLink,
    FlowRegistration,
)
from flowfile_core.flowfile.flow_graph import FlowGraph, RunInformation, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, schemas


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _cleanup():
    """Remove all catalog / flow-registration rows so tests start clean."""
    with get_db_context() as db:
        db.query(CatalogTableReadLink).delete()
        db.query(CatalogTable).delete()
        db.query(FlowRegistration).delete()
        db.query(CatalogNamespace).delete()
        db.commit()


@pytest.fixture(autouse=True)
def clean_state():
    _cleanup()
    yield
    _cleanup()


def _create_namespace():
    """Create a two-level namespace hierarchy and return the schema-level id."""
    with get_db_context() as db:
        cat = CatalogNamespace(name="TestCat", level=0, owner_id=1)
        db.add(cat)
        db.commit()
        db.refresh(cat)
        schema = CatalogNamespace(name="TestSch", level=1, parent_id=cat.id, owner_id=1)
        db.add(schema)
        db.commit()
        db.refresh(schema)
        return schema.id


def _create_flow_registration(namespace_id: int, name: str = "test_flow", path: str = "/tmp/test.yaml"):
    """Insert a FlowRegistration row and return its id."""
    with get_db_context() as db:
        reg = FlowRegistration(
            name=name,
            flow_path=path,
            namespace_id=namespace_id,
            owner_id=1,
        )
        db.add(reg)
        db.commit()
        db.refresh(reg)
        return reg.id


def _create_graph(
    flow_id: int = 1,
    source_registration_id: int | None = None,
) -> FlowGraph:
    """Create a FlowGraph with optional source_registration_id."""
    handler = FlowfileHandler()
    settings = schemas.FlowSettings(
        flow_id=flow_id,
        name="test_flow",
        path=".",
        execution_mode="Development",
        execution_location="local",
        source_registration_id=source_registration_id,
    )
    handler.register_flow(settings)
    return handler.get_flow(flow_id)


def _add_manual_input(graph: FlowGraph, data: list[dict], node_id: int = 1):
    """Add a manual input node with the given data."""
    promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="manual_input")
    graph.add_node_promise(promise)
    manual = input_schema.NodeManualInput(
        flow_id=graph.flow_id,
        node_id=node_id,
        raw_data_format=input_schema.RawData.from_pylist(data),
    )
    graph.add_manual_input(manual)


def _run_graph(graph: FlowGraph) -> RunInformation:
    run_info = graph.run_graph()
    if not run_info.success:
        errors = []
        for step in run_info.node_step_result:
            if not step.success:
                errors.append(f"node {step.node_id}: {step.error}")
        raise AssertionError(f"Graph execution failed:\n" + "\n".join(errors))
    return run_info


SAMPLE_DATA = [
    {"name": "Alice", "age": 30, "city": "Amsterdam"},
    {"name": "Bob", "age": 25, "city": "Berlin"},
    {"name": "Charlie", "age": 35, "city": "Copenhagen"},
]


# ---------------------------------------------------------------------------
# Catalog writer tests
# ---------------------------------------------------------------------------


class TestCatalogWriter:
    """Test that catalog_writer nodes materialize data and register tables."""

    def test_writer_creates_catalog_table(self):
        """Running a flow with a catalog_writer should create a CatalogTable row."""
        ns_id = _create_namespace()
        graph = _create_graph()

        # Node 1: manual input
        _add_manual_input(graph, SAMPLE_DATA, node_id=1)

        # Node 2: catalog writer
        promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="catalog_writer")
        graph.add_node_promise(promise)

        writer_settings = input_schema.CatalogWriteSettings(
            table_name="written_table",
            namespace_id=ns_id,
            description="Test table from flow",
        )
        writer = input_schema.NodeCatalogWriter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            catalog_write_settings=writer_settings,
            user_id=1,
        )
        graph.add_catalog_writer(writer)
        connection = input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=2)
        add_connection(graph, connection)

        _run_graph(graph)

        # Verify the table was registered
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            tables = repo.list_tables(namespace_id=ns_id)
            assert len(tables) == 1
            table = tables[0]
            assert table.name == "written_table"
            assert table.row_count == 3
            assert table.column_count == 3
            assert os.path.isfile(table.file_path)

    def test_writer_stores_source_registration_id(self):
        """When a flow has source_registration_id, the produced table should reference it."""
        ns_id = _create_namespace()
        reg_id = _create_flow_registration(ns_id, name="producer_flow")
        graph = _create_graph(source_registration_id=reg_id)

        _add_manual_input(graph, SAMPLE_DATA, node_id=1)

        promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="catalog_writer")
        graph.add_node_promise(promise)
        writer = input_schema.NodeCatalogWriter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            catalog_write_settings=input_schema.CatalogWriteSettings(
                table_name="lineage_table",
                namespace_id=ns_id,
            ),
            user_id=1,
        )
        graph.add_catalog_writer(writer)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=2))

        _run_graph(graph)

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            tables = repo.list_tables(namespace_id=ns_id)
            assert len(tables) == 1
            assert tables[0].source_registration_id == reg_id

    def test_writer_overwrite_mode_replaces_table(self):
        """With write_mode='overwrite', running twice should replace the table."""
        ns_id = _create_namespace()
        graph = _create_graph()

        _add_manual_input(graph, SAMPLE_DATA, node_id=1)

        promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="catalog_writer")
        graph.add_node_promise(promise)
        writer = input_schema.NodeCatalogWriter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            catalog_write_settings=input_schema.CatalogWriteSettings(
                table_name="overwrite_me",
                namespace_id=ns_id,
                write_mode="overwrite",
            ),
            user_id=1,
        )
        graph.add_catalog_writer(writer)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=2))

        _run_graph(graph)

        # Run again with different data
        graph2 = _create_graph(flow_id=2)
        _add_manual_input(graph2, [{"name": "Diana", "age": 40, "city": "Dublin"}], node_id=1)
        promise2 = input_schema.NodePromise(flow_id=2, node_id=2, node_type="catalog_writer")
        graph2.add_node_promise(promise2)
        writer2 = input_schema.NodeCatalogWriter(
            flow_id=2,
            node_id=2,
            depending_on_id=1,
            catalog_write_settings=input_schema.CatalogWriteSettings(
                table_name="overwrite_me",
                namespace_id=ns_id,
                write_mode="overwrite",
            ),
            user_id=1,
        )
        graph2.add_catalog_writer(writer2)
        add_connection(graph2, input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=2))

        _run_graph(graph2)

        # Should be only one table with updated data
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            tables = repo.list_tables(namespace_id=ns_id)
            assert len(tables) == 1
            assert tables[0].row_count == 1


# ---------------------------------------------------------------------------
# Catalog reader tests
# ---------------------------------------------------------------------------


class TestCatalogReader:
    """Test that catalog_reader nodes load data from catalog tables."""

    def _register_table(self, ns_id: int) -> int:
        """Register a test table via CatalogService and return its id."""
        # Create a temp parquet file
        df = pl.DataFrame(SAMPLE_DATA)
        tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        df.write_parquet(tmp.name)
        tmp.close()

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            table_out = svc.register_table(
                name="readable_table",
                file_path=tmp.name,
                owner_id=1,
                namespace_id=ns_id,
            )
        return table_out.id

    def test_reader_loads_data_by_id(self):
        """A catalog_reader node should load data when given a catalog_table_id."""
        ns_id = _create_namespace()
        table_id = self._register_table(ns_id)

        graph = _create_graph()
        promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="catalog_reader")
        graph.add_node_promise(promise)
        reader = input_schema.NodeCatalogReader(
            flow_id=graph.flow_id,
            node_id=1,
            catalog_table_id=table_id,
        )
        graph.add_catalog_reader(reader)

        _run_graph(graph)

        node = graph.get_node(1)
        result_df = node.get_resulting_data().collect()
        assert len(result_df) == 3
        assert set(result_df.columns) == {"name", "age", "city"}

    def test_reader_loads_data_by_name(self):
        """A catalog_reader node should also resolve a table by name + namespace."""
        ns_id = _create_namespace()
        self._register_table(ns_id)

        graph = _create_graph()
        promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="catalog_reader")
        graph.add_node_promise(promise)
        reader = input_schema.NodeCatalogReader(
            flow_id=graph.flow_id,
            node_id=1,
            catalog_table_name="readable_table",
            catalog_namespace_id=ns_id,
        )
        graph.add_catalog_reader(reader)

        _run_graph(graph)

        node = graph.get_node(1)
        result_df = node.get_resulting_data().collect()
        assert len(result_df) == 3


# ---------------------------------------------------------------------------
# sync_catalog_read_links tests
# ---------------------------------------------------------------------------


class TestSyncCatalogReadLinks:
    """Test that save_flow records read links for catalog_reader nodes."""

    def test_save_flow_records_read_links(self):
        """Saving a flow with catalog_reader nodes should upsert read links."""
        ns_id = _create_namespace()
        reg_id = _create_flow_registration(ns_id, name="reader_flow", path="/tmp/reader_flow.yaml")

        # Create a catalog table to read from
        df = pl.DataFrame(SAMPLE_DATA)
        tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        df.write_parquet(tmp.name)
        tmp.close()

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            table_out = svc.register_table(
                name="link_test_table",
                file_path=tmp.name,
                owner_id=1,
                namespace_id=ns_id,
            )
        table_id = table_out.id

        # Build a graph with a catalog_reader and save it
        graph = _create_graph(source_registration_id=reg_id)
        promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="catalog_reader")
        graph.add_node_promise(promise)
        reader = input_schema.NodeCatalogReader(
            flow_id=graph.flow_id,
            node_id=1,
            catalog_table_id=table_id,
        )
        graph.add_catalog_reader(reader)

        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as f:
            save_path = f.name

        graph.save_flow(save_path)

        # Verify the read link was created
        with get_db_context() as db:
            link = db.query(CatalogTableReadLink).filter_by(
                table_id=table_id, registration_id=reg_id
            ).first()
            assert link is not None

        os.unlink(save_path)

    def test_save_flow_skips_links_without_registration_id(self):
        """If the flow has no source_registration_id, no read links are created."""
        ns_id = _create_namespace()

        df = pl.DataFrame(SAMPLE_DATA)
        tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        df.write_parquet(tmp.name)
        tmp.close()

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            table_out = svc.register_table(
                name="no_link_table",
                file_path=tmp.name,
                owner_id=1,
                namespace_id=ns_id,
            )
        table_id = table_out.id

        graph = _create_graph(source_registration_id=None)
        promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="catalog_reader")
        graph.add_node_promise(promise)
        reader = input_schema.NodeCatalogReader(
            flow_id=graph.flow_id,
            node_id=1,
            catalog_table_id=table_id,
        )
        graph.add_catalog_reader(reader)

        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as f:
            save_path = f.name

        graph.save_flow(save_path)

        with get_db_context() as db:
            links = db.query(CatalogTableReadLink).all()
            assert len(links) == 0

        os.unlink(save_path)


# ---------------------------------------------------------------------------
# Round-trip: write → read
# ---------------------------------------------------------------------------


class TestCatalogRoundTrip:
    """Test writing data to the catalog then reading it back."""

    def test_write_then_read_preserves_data(self):
        """Data written by a catalog_writer should be readable by a catalog_reader."""
        ns_id = _create_namespace()

        # Step 1: Write data to catalog
        write_graph = _create_graph(flow_id=1)
        _add_manual_input(write_graph, SAMPLE_DATA, node_id=1)

        promise = input_schema.NodePromise(flow_id=1, node_id=2, node_type="catalog_writer")
        write_graph.add_node_promise(promise)
        writer = input_schema.NodeCatalogWriter(
            flow_id=1,
            node_id=2,
            depending_on_id=1,
            catalog_write_settings=input_schema.CatalogWriteSettings(
                table_name="roundtrip_table",
                namespace_id=ns_id,
            ),
            user_id=1,
        )
        write_graph.add_catalog_writer(writer)
        add_connection(write_graph, input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=2))

        _run_graph(write_graph)

        # Get the table id
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            tables = repo.list_tables(namespace_id=ns_id)
            assert len(tables) == 1
            table_id = tables[0].id

        # Step 2: Read data back from catalog
        read_graph = _create_graph(flow_id=2)
        read_promise = input_schema.NodePromise(flow_id=2, node_id=1, node_type="catalog_reader")
        read_graph.add_node_promise(read_promise)
        reader = input_schema.NodeCatalogReader(
            flow_id=2,
            node_id=1,
            catalog_table_id=table_id,
        )
        read_graph.add_catalog_reader(reader)

        _run_graph(read_graph)

        node = read_graph.get_node(1)
        result_df = node.get_resulting_data().collect()
        assert len(result_df) == 3
        assert set(result_df.columns) == {"name", "age", "city"}

        # Verify actual values
        names = sorted(result_df["name"].to_list())
        assert names == ["Alice", "Bob", "Charlie"]
