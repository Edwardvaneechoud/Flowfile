"""Tests for catalog reader/writer nodes and lineage tracking within FlowGraph.

Covers:
- Catalog writer: materializes data to the catalog with correct lineage
- Catalog reader: reads catalog tables back into a flow
- _sync_catalog_read_links: records read links on save_flow
- Round-trip: write → read → verify data integrity
"""

import io as _io
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from flowfile_core.catalog import CatalogService
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import (
    CatalogTable,
    CatalogTableReadLink,
    FlowSchedule,
)
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_graph import (
    _register_catalog_table,
    _resolve_virtual_table,
    _write_catalog_delta_local,
    add_connection,
)
from flowfile_core.schemas import input_schema
from tests.flowfile.conftest import (
    CATALOG_SAMPLE_DATA as SAMPLE_DATA,
)
from tests.flowfile.conftest import (
    add_test_manual_input as _add_manual_input,
)
from tests.flowfile.conftest import (
    catalog_cleanup as _cleanup,
)
from tests.flowfile.conftest import (
    create_test_flow_registration as _create_flow_registration,
)
from tests.flowfile.conftest import (
    create_test_graph as _create_graph,
)
from tests.flowfile.conftest import (
    create_test_namespace as _create_namespace,
)
from tests.flowfile.conftest import (
    run_test_graph as _run_graph,
)


@pytest.fixture(autouse=True)
def clean_state():
    _cleanup()
    yield
    _cleanup()


# ---------------------------------------------------------------------------
# Catalog writer tests
# ---------------------------------------------------------------------------


class TestCatalogWriter:
    """Test that catalog_writer nodes materialize data and register tables."""

    def test_writer_creates_catalog_table(self, execution_location):
        """Running a flow with a catalog_writer should create a CatalogTable row."""
        ns_id = _create_namespace()
        graph = _create_graph(execution_location=execution_location)
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
            assert os.path.isdir(table.file_path)
            assert "_delta_log" in os.listdir(table.file_path)

    def test_writer_stores_source_registration_id(self, execution_location):
        """When a flow has source_registration_id, the produced table should reference it."""
        ns_id = _create_namespace()
        reg_id = _create_flow_registration(ns_id, name="producer_flow")
        graph = _create_graph(source_registration_id=reg_id, execution_location=execution_location)

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

    def test_writer_overwrite_mode_replaces_table(self, execution_location):
        """With write_mode='overwrite', running twice should replace the table and preserve its ID."""
        ns_id = _create_namespace()
        graph = _create_graph(execution_location=execution_location)

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

        # Capture the original table id and updated_at
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            tables = repo.list_tables(namespace_id=ns_id)
            assert len(tables) == 1
            original_id = tables[0].id
            original_updated_at = tables[0].updated_at

        # Run again with different data
        graph2 = _create_graph(flow_id=2, execution_location=execution_location)
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

        # Should be only one table with updated data and the SAME id
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            tables = repo.list_tables(namespace_id=ns_id)
            assert len(tables) == 1
            assert tables[0].id == original_id
            assert tables[0].row_count == 1
            assert tables[0].updated_at >= original_updated_at

    def test_overwrite_preserves_trigger_table_reference(self, execution_location):
        """FlowSchedule.trigger_table_id still resolves after an overwrite."""
        ns_id = _create_namespace()
        reg_id = _create_flow_registration(ns_id, name="triggered_flow")

        # First write to create the table
        graph = _create_graph(execution_location=execution_location)
        _add_manual_input(graph, SAMPLE_DATA, node_id=1)
        promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="catalog_writer")
        graph.add_node_promise(promise)
        graph.add_catalog_writer(
            input_schema.NodeCatalogWriter(
                flow_id=graph.flow_id,
                node_id=2,
                depending_on_id=1,
                catalog_write_settings=input_schema.CatalogWriteSettings(
                    table_name="trigger_table",
                    namespace_id=ns_id,
                    write_mode="overwrite",
                ),
                user_id=1,
            )
        )
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=2))
        _run_graph(graph)

        # Create a schedule that triggers on this table
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            table = repo.get_table_by_name("trigger_table", ns_id)
            table_id = table.id
            sched = FlowSchedule(
                registration_id=reg_id,
                owner_id=1,
                enabled=True,
                schedule_type="table_trigger",
                trigger_table_id=table_id,
            )
            db.add(sched)
            db.commit()
            db.refresh(sched)
            schedule_id = sched.id

        # Overwrite the table with new data
        graph2 = _create_graph(flow_id=2, execution_location=execution_location)
        _add_manual_input(graph2, [{"name": "Eve", "age": 28, "city": "Edinburgh"}], node_id=1)
        promise2 = input_schema.NodePromise(flow_id=2, node_id=2, node_type="catalog_writer")
        graph2.add_node_promise(promise2)
        graph2.add_catalog_writer(
            input_schema.NodeCatalogWriter(
                flow_id=2,
                node_id=2,
                depending_on_id=1,
                catalog_write_settings=input_schema.CatalogWriteSettings(
                    table_name="trigger_table",
                    namespace_id=ns_id,
                    write_mode="overwrite",
                ),
                user_id=1,
            )
        )
        add_connection(graph2, input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=2))
        _run_graph(graph2)

        # The schedule's trigger_table_id should still resolve to a valid table
        with get_db_context() as db:
            sched = db.get(FlowSchedule, schedule_id)
            assert sched is not None
            assert sched.trigger_table_id == table_id
            table = db.get(CatalogTable, sched.trigger_table_id)
            assert table is not None
            assert table.row_count == 1

    def test_overwrite_preserves_read_links(self, execution_location):
        """CatalogTableReadLink entries survive a table overwrite."""
        ns_id = _create_namespace()
        reg_id = _create_flow_registration(ns_id, name="reader_flow", path="/tmp/reader.yaml")

        # First write
        graph = _create_graph(execution_location=execution_location)
        _add_manual_input(graph, SAMPLE_DATA, node_id=1)
        promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="catalog_writer")
        graph.add_node_promise(promise)
        graph.add_catalog_writer(
            input_schema.NodeCatalogWriter(
                flow_id=graph.flow_id,
                node_id=2,
                depending_on_id=1,
                catalog_write_settings=input_schema.CatalogWriteSettings(
                    table_name="linked_table",
                    namespace_id=ns_id,
                    write_mode="overwrite",
                ),
                user_id=1,
            )
        )
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=2))
        _run_graph(graph)

        # Create a read link referencing this table
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            table = repo.get_table_by_name("linked_table", ns_id)
            table_id = table.id
            link = CatalogTableReadLink(table_id=table_id, registration_id=reg_id)
            db.add(link)
            db.commit()

        # Overwrite the table
        graph2 = _create_graph(flow_id=2, execution_location=execution_location)
        _add_manual_input(graph2, [{"name": "Zara", "age": 22, "city": "Zurich"}], node_id=1)
        promise2 = input_schema.NodePromise(flow_id=2, node_id=2, node_type="catalog_writer")
        graph2.add_node_promise(promise2)
        graph2.add_catalog_writer(
            input_schema.NodeCatalogWriter(
                flow_id=2,
                node_id=2,
                depending_on_id=1,
                catalog_write_settings=input_schema.CatalogWriteSettings(
                    table_name="linked_table",
                    namespace_id=ns_id,
                    write_mode="overwrite",
                ),
                user_id=1,
            )
        )
        add_connection(graph2, input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=2))
        _run_graph(graph2)

        # Read link should still exist and point to the same table ID
        with get_db_context() as db:
            link = db.query(CatalogTableReadLink).filter_by(table_id=table_id, registration_id=reg_id).first()
            assert link is not None
            # Table should still be valid
            table = db.get(CatalogTable, table_id)
            assert table is not None
            assert table.row_count == 1


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

    def test_reader_loads_data_by_id(self, execution_location):
        """A catalog_reader node should load data when given a catalog_table_id."""
        ns_id = _create_namespace()
        table_id = self._register_table(ns_id)

        graph = _create_graph(execution_location=execution_location)
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

    def test_reader_loads_data_by_name(self, execution_location):
        """A catalog_reader node should also resolve a table by name + namespace."""
        ns_id = _create_namespace()
        self._register_table(ns_id)

        graph = _create_graph(execution_location=execution_location)
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
            link = db.query(CatalogTableReadLink).filter_by(table_id=table_id, registration_id=reg_id).first()
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

    def test_write_then_read_preserves_data(self, execution_location):
        """Data written by a catalog_writer should be readable by a catalog_reader."""
        ns_id = _create_namespace()

        # Step 1: Write data to catalog
        write_graph = _create_graph(flow_id=1, execution_location=execution_location)
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
        read_graph = _create_graph(flow_id=2, execution_location=execution_location)
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


# ---------------------------------------------------------------------------
# Catalog SQL reader tests
# ---------------------------------------------------------------------------


class TestCatalogSqlReader:
    """Test that catalog_reader nodes with sql_query execute SQL against catalog Delta tables."""

    _tmp_dirs: list = []

    @classmethod
    def teardown_method(cls):
        import shutil

        for d in cls._tmp_dirs:
            shutil.rmtree(d, ignore_errors=True)
        cls._tmp_dirs.clear()

    @classmethod
    def _register_delta_table(cls, ns_id: int, table_name: str, data: list[dict]) -> int:
        """Write a Delta table and register it in the catalog. Returns the table id."""
        import tempfile

        tmp_dir = tempfile.mkdtemp()
        cls._tmp_dirs.append(tmp_dir)
        delta_path = os.path.join(tmp_dir, table_name)
        pl.DataFrame(data).write_delta(delta_path)

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            table_out = svc.register_table_from_data(
                name=table_name,
                table_path=delta_path,
                owner_id=1,
                namespace_id=ns_id,
                storage_format="delta",
            )
        return table_out.id

    def test_sql_query_simple_select(self):
        """A catalog_reader with sql_query should execute SQL against catalog Delta tables."""
        ns_id = _create_namespace()
        self._register_delta_table(ns_id, "people", SAMPLE_DATA)

        graph = _create_graph()
        promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="catalog_reader")
        graph.add_node_promise(promise)
        reader = input_schema.NodeCatalogReader(
            flow_id=graph.flow_id,
            node_id=1,
            sql_query="SELECT * FROM people",
        )
        graph.add_catalog_reader(reader)

        _run_graph(graph)

        node = graph.get_node(1)
        result_df = node.get_resulting_data().collect()
        assert len(result_df) == 3
        assert set(result_df.columns) == {"name", "age", "city"}

    def test_sql_query_with_filter(self):
        """SQL query with a WHERE clause should return filtered results."""
        ns_id = _create_namespace()
        self._register_delta_table(ns_id, "people_filter", SAMPLE_DATA)

        graph = _create_graph()
        promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="catalog_reader")
        graph.add_node_promise(promise)
        reader = input_schema.NodeCatalogReader(
            flow_id=graph.flow_id,
            node_id=1,
            sql_query="SELECT name, age FROM people_filter WHERE age > 28",
        )
        graph.add_catalog_reader(reader)

        _run_graph(graph)

        node = graph.get_node(1)
        result_df = node.get_resulting_data().collect()
        assert len(result_df) == 2
        assert set(result_df.columns) == {"name", "age"}
        names = sorted(result_df["name"].to_list())
        assert names == ["Alice", "Charlie"]

    def test_sql_query_join_two_tables(self):
        """SQL query that JOINs two catalog tables."""
        ns_id = _create_namespace()
        self._register_delta_table(
            ns_id,
            "customers_sql",
            [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
            ],
        )
        self._register_delta_table(
            ns_id,
            "orders_sql",
            [
                {"customer_id": 1, "amount": 100},
                {"customer_id": 2, "amount": 200},
                {"customer_id": 1, "amount": 150},
            ],
        )

        graph = _create_graph()
        promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="catalog_reader")
        graph.add_node_promise(promise)
        reader = input_schema.NodeCatalogReader(
            flow_id=graph.flow_id,
            node_id=1,
            sql_query=(
                "SELECT c.name, SUM(o.amount) AS total "
                "FROM customers_sql c "
                "JOIN orders_sql o ON c.id = o.customer_id "
                "GROUP BY c.name"
            ),
        )
        graph.add_catalog_reader(reader)

        _run_graph(graph)

        node = graph.get_node(1)
        result_df = node.get_resulting_data().collect()
        assert len(result_df) == 2
        assert "name" in result_df.columns
        assert "total" in result_df.columns

    def test_sql_query_description(self):
        """NodeCatalogReader with sql_query should return SQL-based description."""
        reader = input_schema.NodeCatalogReader(
            flow_id=1,
            node_id=1,
            sql_query="SELECT * FROM my_table WHERE id > 10",
        )
        assert reader.get_default_description().startswith("SQL: SELECT")

    def test_sql_query_invalid_sql_sets_error(self):
        """Invalid SQL should store a validation error on the node without crashing."""
        ns_id = _create_namespace()
        self._register_delta_table(ns_id, "dummy_table", SAMPLE_DATA)

        graph = _create_graph()
        promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="catalog_reader")
        graph.add_node_promise(promise)
        reader = input_schema.NodeCatalogReader(
            flow_id=graph.flow_id,
            node_id=1,
            sql_query="SELEC * FORM people",  # intentionally broken SQL
        )
        graph.add_catalog_reader(reader)

        node = graph.get_node(1)
        assert node.results.errors is not None

    def test_sql_query_no_tables_raises(self):
        """When no Delta tables exist, executing the SQL node should raise ValueError."""
        graph = _create_graph()
        promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="catalog_reader")
        graph.add_node_promise(promise)
        reader = input_schema.NodeCatalogReader(
            flow_id=graph.flow_id,
            node_id=1,
            sql_query="SELECT 1",
        )
        graph.add_catalog_reader(reader)

        with pytest.raises(AssertionError, match="No catalog tables available to query"):
            _run_graph(graph)


class TestResolveVirtualTable:
    """Test _resolve_virtual_table helper."""

    def test_resolve_optimized_virtual_table(self):
        """An optimized virtual table should deserialize the stored LazyFrame."""
        lf = pl.LazyFrame({"x": [1, 2, 3]})
        buf = _io.BytesIO()
        lf.serialize(buf)
        serialized = buf.getvalue()
        result = _resolve_virtual_table(
            is_optimized=True, serialized_lf=serialized, catalog_table_id=-1, run_location="local"
        )

        assert isinstance(result, pl.LazyFrame)
        df = result.collect()
        assert df["x"].to_list() == [1, 2, 3]

    def test_resolve_non_optimized_virtual_table(self):
        """A non-optimized virtual table should call CatalogService.resolve_virtual_flow_table."""
        expected_lf = pl.LazyFrame({"y": [10, 20]})

        with patch("flowfile_core.flowfile.flow_graph.get_db_context") as mock_ctx:
            mock_db = MagicMock()
            mock_ctx.return_value.__enter__ = MagicMock(return_value=mock_db)
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)

            with patch("flowfile_core.flowfile.flow_graph.CatalogService") as MockSvc:
                mock_svc_instance = MagicMock()
                mock_svc_instance.resolve_virtual_flow_table.return_value = expected_lf
                MockSvc.return_value = mock_svc_instance

                result = _resolve_virtual_table(is_optimized=False, serialized_lf=None, catalog_table_id=42)

        assert isinstance(result, pl.LazyFrame)
        mock_svc_instance.resolve_virtual_flow_table.assert_called_once_with(42, run_location=None, node_logger=None)

    def test_resolve_optimized_without_serialized_lf_falls_back(self):
        """When is_optimized=True but serialized_lf is None, should fall back to service resolution."""
        expected_lf = pl.LazyFrame({"z": [5]})

        with patch("flowfile_core.flowfile.flow_graph.get_db_context") as mock_ctx:
            mock_db = MagicMock()
            mock_ctx.return_value.__enter__ = MagicMock(return_value=mock_db)
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)

            with patch("flowfile_core.flowfile.flow_graph.CatalogService") as MockSvc:
                mock_svc_instance = MagicMock()
                mock_svc_instance.resolve_virtual_flow_table.return_value = expected_lf
                MockSvc.return_value = mock_svc_instance

                _resolve_virtual_table(is_optimized=True, serialized_lf=None, catalog_table_id=99)

        mock_svc_instance.resolve_virtual_flow_table.assert_called_once_with(99, run_location=None, node_logger=None)


class TestWriteCatalogDeltaLocal:
    """Test _write_catalog_delta_local helper."""

    def test_write_delta_creates_table_and_returns_metadata(self, tmp_path):
        """Writing a new Delta table should return metadata with schema, row_count, etc."""
        df = FlowDataEngine(pl.LazyFrame({"name": ["Alice", "Bob"], "age": [30, 25]}))
        dest_path = tmp_path / "test_table"

        result = _write_catalog_delta_local(df, dest_path, delta_mode="overwrite", merge_keys=None)

        assert result is not None
        assert result["row_count"] == 2
        assert result["column_count"] == 2
        assert isinstance(result["schema"], list)
        assert len(result["schema"]) == 2
        assert result["size_bytes"] > 0

    def test_write_delta_append_mode(self, tmp_path):
        """Appending to an existing Delta table should return updated metadata."""
        dest_path = tmp_path / "append_table"
        df1 = FlowDataEngine(pl.LazyFrame({"x": [1, 2]}))
        _write_catalog_delta_local(df1, dest_path, delta_mode="overwrite", merge_keys=None)

        df2 = FlowDataEngine(pl.LazyFrame({"x": [3, 4]}))
        result = _write_catalog_delta_local(df2, dest_path, delta_mode="append", merge_keys=None)

        assert result is not None
        assert result["row_count"] == 2  # metadata reflects the appended batch


class TestRegisterCatalogTable:
    """Test _register_catalog_table helper."""

    def test_register_new_table(self):
        """Registering a new table should call register_table_from_data."""
        ns_id = _create_namespace()

        with tempfile.TemporaryDirectory() as tmp_dir:
            dest_path = Path(tmp_dir) / "new_table"
            pl.DataFrame({"a": [1]}).write_delta(str(dest_path))

            settings = input_schema.CatalogWriteSettings(
                table_name="reg_test_table",
                namespace_id=ns_id,
            )
            meta: dict = {
                "schema": [{"name": "a", "dtype": "Int64"}],
                "row_count": 1,
                "column_count": 1,
                "size_bytes": 100,
            }

            _register_catalog_table(
                existing=None,
                dest_path=dest_path,
                settings=settings,
                source_registration_id=None,
                user_id=1,
                meta_kwargs=meta,
            )

            with get_db_context() as db:
                repo = SQLAlchemyCatalogRepository(db)
                tables = repo.list_tables(namespace_id=ns_id)
                assert len(tables) == 1
                assert tables[0].name == "reg_test_table"

    def test_register_existing_table_overwrites(self):
        """Overwriting an existing table should call overwrite_table_data."""
        ns_id = _create_namespace()

        with tempfile.TemporaryDirectory() as tmp_dir:
            dest_path = Path(tmp_dir) / "overwrite_table"
            pl.DataFrame({"a": [1]}).write_delta(str(dest_path))

            # First, register the table
            with get_db_context() as db:
                repo = SQLAlchemyCatalogRepository(db)
                svc = CatalogService(repo)
                table_out = svc.register_table_from_data(
                    name="overwrite_test",
                    table_path=str(dest_path),
                    owner_id=1,
                    namespace_id=ns_id,
                    storage_format="delta",
                )
                existing = repo.get_table(table_out.id)

            # Overwrite with new data
            pl.DataFrame({"a": [2, 3]}).write_delta(str(dest_path), mode="overwrite")

            meta: dict = {
                "schema": [{"name": "a", "dtype": "Int64"}],
                "row_count": 2,
                "column_count": 1,
                "size_bytes": 200,
            }

            _register_catalog_table(
                existing=existing,
                dest_path=dest_path,
                settings=input_schema.CatalogWriteSettings(
                    table_name="overwrite_test",
                    namespace_id=ns_id,
                ),
                source_registration_id=None,
                user_id=1,
                meta_kwargs=meta,
            )

            with get_db_context() as db:
                repo = SQLAlchemyCatalogRepository(db)
                tables = repo.list_tables(namespace_id=ns_id)
                assert len(tables) == 1
                assert tables[0].id == table_out.id
                assert tables[0].row_count == 2


class TestHandleVirtualTableWrite:
    """Test _handle_virtual_table_write via full graph execution."""

    def test_virtual_write_validates_registration(self):
        """Virtual write without source_registration_id should raise ValueError."""
        graph = _create_graph(source_registration_id=None)
        _add_manual_input(graph, SAMPLE_DATA, node_id=1)

        promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="catalog_writer")
        graph.add_node_promise(promise)
        writer = input_schema.NodeCatalogWriter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            catalog_write_settings=input_schema.CatalogWriteSettings(
                table_name="should_fail",
                namespace_id=1,
                write_mode="virtual",
            ),
            user_id=1,
        )
        graph.add_catalog_writer(writer)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=2))

        with pytest.raises(AssertionError, match="not linked to a catalog registration"):
            _run_graph(graph)

    def test_virtual_write_creates_table(self):
        """Virtual write with valid registration should create a virtual table."""
        ns_id = _create_namespace()
        reg_id = _create_flow_registration(ns_id, name="vw_producer")
        graph = _create_graph(source_registration_id=reg_id)

        _add_manual_input(graph, SAMPLE_DATA, node_id=1)

        promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="catalog_writer")
        graph.add_node_promise(promise)
        writer = input_schema.NodeCatalogWriter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            catalog_write_settings=input_schema.CatalogWriteSettings(
                table_name="vw_table",
                namespace_id=ns_id,
                write_mode="virtual",
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
            assert tables[0].table_type == "virtual"
            assert tables[0].name == "vw_table"

    def test_virtual_write_updates_existing(self):
        """Running virtual write twice should update the existing virtual table."""
        ns_id = _create_namespace()
        reg_id = _create_flow_registration(ns_id, name="vw_update_producer")
        # First write
        graph1 = _create_graph(flow_id=1, source_registration_id=reg_id)
        _add_manual_input(graph1, SAMPLE_DATA, node_id=1)
        promise1 = input_schema.NodePromise(flow_id=1, node_id=2, node_type="catalog_writer")
        graph1.add_node_promise(promise1)
        writer1 = input_schema.NodeCatalogWriter(
            flow_id=1,
            node_id=2,
            depending_on_id=1,
            catalog_write_settings=input_schema.CatalogWriteSettings(
                table_name="vw_update_table",
                namespace_id=ns_id,
                write_mode="virtual",
            ),
            user_id=1,
        )
        graph1.add_catalog_writer(writer1)
        add_connection(graph1, input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=2))
        _run_graph(graph1)

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            tables = repo.list_tables(namespace_id=ns_id)
            assert len(tables) == 1
            original_id = tables[0].id

        # Second write (update)
        graph2 = _create_graph(flow_id=2, source_registration_id=reg_id)
        _add_manual_input(graph2, [{"name": "Diana", "age": 40, "city": "Dublin"}], node_id=1)
        promise2 = input_schema.NodePromise(flow_id=2, node_id=2, node_type="catalog_writer")
        graph2.add_node_promise(promise2)
        writer2 = input_schema.NodeCatalogWriter(
            flow_id=2,
            node_id=2,
            depending_on_id=1,
            catalog_write_settings=input_schema.CatalogWriteSettings(
                table_name="vw_update_table",
                namespace_id=ns_id,
                write_mode="virtual",
            ),
            user_id=1,
        )
        graph2.add_catalog_writer(writer2)
        add_connection(graph2, input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=2))
        _run_graph(graph2)

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            tables = repo.list_tables(namespace_id=ns_id)
            # Should still be only one virtual table (updated, not duplicated)
            assert len(tables) == 1
            assert tables[0].id == original_id
