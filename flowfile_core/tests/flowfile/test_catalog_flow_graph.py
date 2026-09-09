"""Tests for catalog reader/writer nodes and lineage tracking within FlowGraph.

Covers:
- Catalog writer: materializes data to the catalog with correct lineage
- Catalog reader: reads catalog tables back into a flow
- _sync_catalog_read_links: records read links on save_flow
- Round-trip: write → read → verify data integrity
"""

import io as _io
import json
import os
import shutil
import tempfile
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
import yaml

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
    _scd2_primitive_kwargs,
    _write_catalog_delta_local,
    add_connection,
)
from flowfile_core.schemas import input_schema, transform_schema
from tests.flowfile.conftest import (
    CATALOG_SAMPLE_DATA as SAMPLE_DATA,
)
from tests.flowfile.conftest import (
    add_test_catalog_writer as _add_catalog_writer,
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


# A resolved SCD2 catalog config, the shape _resolve_scd2_config produces.
_SCD2_CFG = {
    "business_keys": ["id"],
    "surrogate_key_column": "sk",
    "valid_from_column": "valid_from",
    "valid_to_column": "valid_to",
    "is_current_column": "is_current",
    "compare_columns": ["val"],
    "full_snapshot": False,
}


# Catalog writer tests


class TestCatalogWriter:
    """Test that catalog_writer nodes materialize data and register tables."""

    def test_writer_creates_catalog_table(self, execution_location):
        """Running a flow with a catalog_writer should create a CatalogTable row."""
        ns_id = _create_namespace()
        graph = _create_graph(execution_location=execution_location)
        _add_manual_input(graph, SAMPLE_DATA, node_id=1)

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

    def test_cli_runner_records_producer_lineage(self):
        """The subprocess flow runner (manual-trigger / scheduled runs) must stamp
        source_registration_id before run_graph so produced tables record producer
        lineage. Regression: canvas runs resolved the registration but the CLI
        runners did not, so manual/scheduled runs lost lineage.

        Exercises flowfile.__main__.run_flow (the non-frozen subprocess path);
        flowfile_core.main._run_flow_cli is an identical mirror for frozen builds.
        """
        from flowfile_core.configs.settings import OFFLOAD_TO_WORKER

        ns_id = _create_namespace()

        # Build a physical catalog_writer flow with NO in-memory
        # source_registration_id, then persist it — the state a manual/scheduled
        # run loads from disk before executing.
        graph = _create_graph(execution_location="local")
        _add_manual_input(graph, SAMPLE_DATA, node_id=1)
        graph.add_node_promise(
            input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="catalog_writer")
        )
        graph.add_catalog_writer(
            input_schema.NodeCatalogWriter(
                flow_id=graph.flow_id,
                node_id=2,
                depending_on_id=1,
                catalog_write_settings=input_schema.CatalogWriteSettings(
                    table_name="cli_produced_table",
                    namespace_id=ns_id,
                ),
                user_id=1,
            )
        )
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=2))

        with tempfile.TemporaryDirectory() as tmp:
            flow_path = Path(tmp) / "producer_flow.yaml"
            graph.save_flow(str(flow_path))
            # open_flow stamps the resolved path; register at that exact string so
            # resolve_source_registration_id finds the registration by path.
            resolved_path = str(flow_path.resolve())
            reg_id = _create_flow_registration(ns_id, name="producer_flow", path=resolved_path)

            prev_offload = OFFLOAD_TO_WORKER.value
            prev_env = {k: os.environ.get(k) for k in ("FLOWFILE_SINGLE_FILE_MODE", "FLOWFILE_WORKER_PORT")}
            try:
                from flowfile.__main__ import run_flow

                exit_code = run_flow(resolved_path, run_id=None)
            finally:
                OFFLOAD_TO_WORKER.set(prev_offload)
                for key, value in prev_env.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value

        assert exit_code == 0

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            tables = repo.list_tables(namespace_id=ns_id)
            assert len(tables) == 1
            assert tables[0].name == "cli_produced_table"
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

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            tables = repo.list_tables(namespace_id=ns_id)
            assert len(tables) == 1
            original_id = tables[0].id
            original_updated_at = tables[0].updated_at

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

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            table = repo.get_table_by_name("linked_table", ns_id)
            table_id = table.id
            link = CatalogTableReadLink(table_id=table_id, registration_id=reg_id)
            db.add(link)
            db.commit()

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

        with get_db_context() as db:
            link = db.query(CatalogTableReadLink).filter_by(table_id=table_id, registration_id=reg_id).first()
            assert link is not None
            table = db.get(CatalogTable, table_id)
            assert table is not None
            assert table.row_count == 1


# Catalog reader tests


class TestCatalogReader:
    """Test that catalog_reader nodes load data from catalog tables."""

    def _register_table(self, ns_id: int) -> int:
        """Register a test table via CatalogService and return its id."""
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


# sync_catalog_read_links tests


class TestSyncCatalogReadLinks:
    """Test that save_flow records read links for catalog_reader nodes."""

    @staticmethod
    def _register_table(ns_id: int, name: str) -> int:
        """Register a parquet-backed catalog table and return its id."""
        df = pl.DataFrame(SAMPLE_DATA)
        tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        df.write_parquet(tmp.name)
        tmp.close()

        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            table_out = svc.register_table(name=name, file_path=tmp.name, owner_id=1, namespace_id=ns_id)
        return table_out.id

    @staticmethod
    def _add_reader(graph, node_id: int, table_id: int) -> None:
        graph.add_node_promise(
            input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="catalog_reader")
        )
        graph.add_catalog_reader(
            input_schema.NodeCatalogReader(flow_id=graph.flow_id, node_id=node_id, catalog_table_id=table_id)
        )

    @staticmethod
    def _linked_table_ids(reg_id: int) -> set[int]:
        with get_db_context() as db:
            rows = db.query(CatalogTableReadLink.table_id).filter_by(registration_id=reg_id).all()
        return {row[0] for row in rows}

    @staticmethod
    def _temp_flow_path() -> str:
        """A save path the flow's registration can be registered at (they must match)."""
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as f:
            return f.name

    @staticmethod
    def _persisted_registration_id(flow_path: str):
        """The source_registration_id actually written into the flow file."""
        with open(flow_path, encoding="utf-8") as f:
            return yaml.safe_load(f)["flowfile_settings"]["source_registration_id"]

    def test_save_flow_prunes_removed_reader(self):
        """Removing one of two catalog_reader nodes drops only that reader's link."""
        ns_id = _create_namespace()
        save_path = self._temp_flow_path()
        reg_id = _create_flow_registration(ns_id, name="two_readers", path=save_path)
        table_a = self._register_table(ns_id, "prune_table_a")
        table_b = self._register_table(ns_id, "prune_table_b")

        graph = _create_graph(source_registration_id=reg_id)
        self._add_reader(graph, 1, table_a)
        self._add_reader(graph, 2, table_b)

        graph.save_flow(save_path)
        assert self._linked_table_ids(reg_id) == {table_a, table_b}

        graph.delete_node(2)
        graph.save_flow(save_path)
        assert self._linked_table_ids(reg_id) == {table_a}

        os.unlink(save_path)

    def test_save_flow_prunes_last_reader(self):
        """Removing the last catalog_reader leaves the flow with no read links."""
        ns_id = _create_namespace()
        save_path = self._temp_flow_path()
        reg_id = _create_flow_registration(ns_id, name="last_reader", path=save_path)
        table_id = self._register_table(ns_id, "last_reader_table")

        graph = _create_graph(source_registration_id=reg_id)
        self._add_reader(graph, 1, table_id)

        graph.save_flow(save_path)
        assert self._linked_table_ids(reg_id) == {table_id}

        graph.delete_node(1)
        graph.save_flow(save_path)
        assert self._linked_table_ids(reg_id) == set()

        os.unlink(save_path)

    def test_save_flow_restores_readded_reader(self):
        """A reader added back after a prune gets its link recreated."""
        ns_id = _create_namespace()
        save_path = self._temp_flow_path()
        reg_id = _create_flow_registration(ns_id, name="readded_reader", path=save_path)
        table_id = self._register_table(ns_id, "readd_table")

        graph = _create_graph(source_registration_id=reg_id)
        self._add_reader(graph, 1, table_id)

        graph.save_flow(save_path)
        graph.delete_node(1)
        graph.save_flow(save_path)
        assert self._linked_table_ids(reg_id) == set()

        self._add_reader(graph, 1, table_id)
        graph.save_flow(save_path)
        assert self._linked_table_ids(reg_id) == {table_id}

        os.unlink(save_path)

    def test_save_flow_leaves_other_flows_links_intact(self):
        """Pruning one flow's links must not touch another flow reading the same table."""
        ns_id = _create_namespace()
        path_a = self._temp_flow_path()
        path_b = self._temp_flow_path()
        reg_a = _create_flow_registration(ns_id, name="flow_a", path=path_a)
        reg_b = _create_flow_registration(ns_id, name="flow_b", path=path_b)
        table_id = self._register_table(ns_id, "shared_read_table")

        graph_a = _create_graph(flow_id=1, source_registration_id=reg_a)
        self._add_reader(graph_a, 1, table_id)
        graph_b = _create_graph(flow_id=2, source_registration_id=reg_b)
        self._add_reader(graph_b, 1, table_id)

        graph_a.save_flow(path_a)
        graph_b.save_flow(path_b)
        assert self._linked_table_ids(reg_a) == {table_id}
        assert self._linked_table_ids(reg_b) == {table_id}

        graph_a.delete_node(1)
        graph_a.save_flow(path_a)
        assert self._linked_table_ids(reg_a) == set()
        assert self._linked_table_ids(reg_b) == {table_id}

        os.unlink(path_a)
        os.unlink(path_b)

    def test_save_flow_ignores_registration_id_reused_by_another_flow(self):
        """A stale id that SQLite handed to another flow must not prune that flow's links.

        Registration ids are bare rowids, so deleting a registration frees the id for
        the next insert while an open FlowGraph (and its YAML) still carries it.
        """
        ns_id = _create_namespace()
        path_a = self._temp_flow_path()
        path_b = self._temp_flow_path()
        table_a = self._register_table(ns_id, "reuse_table_a")
        table_b = self._register_table(ns_id, "reuse_table_b")

        reg_a = _create_flow_registration(ns_id, name="flow_a", path=path_a)
        graph_a = _create_graph(flow_id=1, source_registration_id=reg_a)
        self._add_reader(graph_a, 1, table_a)
        graph_a.save_flow(path_a)
        assert self._linked_table_ids(reg_a) == {table_a}

        with get_db_context() as db:
            SQLAlchemyCatalogRepository(db).delete_flow(reg_a)

        reg_b = _create_flow_registration(ns_id, name="flow_b", path=path_b)
        assert reg_b == reg_a, "SQLite should hand the freed rowid to the next registration"

        graph_b = _create_graph(flow_id=2, source_registration_id=reg_b)
        self._add_reader(graph_b, 1, table_b)
        graph_b.save_flow(path_b)
        assert self._linked_table_ids(reg_b) == {table_b}

        # graph_a still points at the recycled id; its save must not touch flow B.
        graph_a.delete_node(1)
        graph_a.save_flow(path_a)
        assert self._linked_table_ids(reg_b) == {table_b}
        assert graph_a.flow_settings.source_registration_id is None
        assert self._persisted_registration_id(path_a) is None

        os.unlink(path_a)
        os.unlink(path_b)

    def test_save_flow_ignores_registration_id_of_a_copied_flow(self):
        """A copied YAML carries the original's id; saving the copy must not prune the original."""
        ns_id = _create_namespace()
        original_path = self._temp_flow_path()
        copy_path = self._temp_flow_path()
        table_id = self._register_table(ns_id, "copied_flow_table")

        reg_id = _create_flow_registration(ns_id, name="original", path=original_path)
        graph = _create_graph(source_registration_id=reg_id)
        self._add_reader(graph, 1, table_id)
        graph.save_flow(original_path)
        assert self._linked_table_ids(reg_id) == {table_id}

        copy_graph = _create_graph(flow_id=2, source_registration_id=reg_id)
        copy_graph.save_flow(copy_path)
        assert self._linked_table_ids(reg_id) == {table_id}
        assert copy_graph.flow_settings.source_registration_id is None
        # Persisted too: a stale id left in the file would be resurrected on reopen.
        assert self._persisted_registration_id(copy_path) is None

        os.unlink(original_path)
        os.unlink(copy_path)

    def test_save_flow_reresolves_foreign_registration_id_to_own_in_one_save(self):
        """A flow whose own path IS registered self-heals: the correct id is persisted and
        its links sync in the same save, leaving the foreign registration untouched."""
        ns_id = _create_namespace()
        foreign_path = self._temp_flow_path()
        own_path = self._temp_flow_path()
        foreign_table = self._register_table(ns_id, "heal_foreign_table")
        own_table = self._register_table(ns_id, "heal_own_table")

        foreign_reg = _create_flow_registration(ns_id, name="foreign_flow", path=foreign_path)
        foreign_graph = _create_graph(flow_id=1, source_registration_id=foreign_reg)
        self._add_reader(foreign_graph, 1, foreign_table)
        foreign_graph.save_flow(foreign_path)
        assert self._linked_table_ids(foreign_reg) == {foreign_table}

        own_reg = _create_flow_registration(ns_id, name="own_flow", path=own_path)
        graph = _create_graph(flow_id=2, source_registration_id=foreign_reg)
        self._add_reader(graph, 1, own_table)
        graph.save_flow(own_path)

        assert graph.flow_settings.source_registration_id == own_reg
        assert self._persisted_registration_id(own_path) == own_reg
        assert self._linked_table_ids(own_reg) == {own_table}
        assert self._linked_table_ids(foreign_reg) == {foreign_table}

        os.unlink(foreign_path)
        os.unlink(own_path)

    def test_reopened_flow_no_longer_resurrects_a_stale_registration_id(self):
        """The persisted None breaks the resurrection loop: reopening re-resolves by path."""
        from flowfile_core.flowfile.catalog_helpers import resolve_source_registration_id
        from flowfile_core.flowfile.manage.io_flowfile import open_flow

        ns_id = _create_namespace()
        foreign_path = self._temp_flow_path()
        own_path = self._temp_flow_path()
        table_id = self._register_table(ns_id, "loop_breaker_table")

        foreign_reg = _create_flow_registration(ns_id, name="loop_foreign", path=foreign_path)
        graph = _create_graph(source_registration_id=foreign_reg)
        self._add_reader(graph, 1, table_id)
        graph.save_flow(own_path)  # own_path has no registration yet -> unresolvable, cleared

        reopened = open_flow(Path(own_path))
        assert reopened.flow_settings.source_registration_id is None

        # open_flow stamps the resolved path; the by-path resolver matches on the exact string.
        own_reg = _create_flow_registration(ns_id, name="loop_own", path=os.path.realpath(own_path))
        resolve_source_registration_id(reopened)
        assert reopened.flow_settings.source_registration_id == own_reg

        reopened.save_flow(own_path)
        assert self._linked_table_ids(own_reg) == {table_id}
        assert self._linked_table_ids(foreign_reg) == set()

        os.unlink(foreign_path)
        os.unlink(own_path)

    def test_import_flow_strips_foreign_registration_id(self):
        """Opening/copying a YAML must not adopt the id baked into it."""
        from flowfile_core.flowfile.handler import FlowfileHandler

        ns_id = _create_namespace()
        original_path = self._temp_flow_path()
        table_id = self._register_table(ns_id, "imported_flow_table")

        reg_id = _create_flow_registration(ns_id, name="import_origin", path=original_path)
        graph = _create_graph(source_registration_id=reg_id)
        self._add_reader(graph, 1, table_id)
        graph.save_flow(original_path)

        copy_path = self._temp_flow_path()
        shutil.copyfile(original_path, copy_path)

        handler = FlowfileHandler()
        imported_id = handler.import_flow(copy_path, user_id=1)
        imported = handler.get_flow(imported_id)
        assert imported.flow_settings.source_registration_id is None

        # The copy's own save leaves the original's lineage alone.
        imported.save_flow(copy_path)
        assert self._linked_table_ids(reg_id) == {table_id}

        os.unlink(original_path)
        os.unlink(copy_path)

    def test_save_flow_syncs_links_after_registration_rename(self):
        """A renamed registration keeps its path, so the guard must not misfire on it."""
        ns_id = _create_namespace()
        save_path = self._temp_flow_path()
        reg_id = _create_flow_registration(ns_id, name="before_rename", path=save_path)
        table_a = self._register_table(ns_id, "rename_table_a")
        table_b = self._register_table(ns_id, "rename_table_b")

        graph = _create_graph(source_registration_id=reg_id)
        self._add_reader(graph, 1, table_a)
        graph.save_flow(save_path)
        assert self._linked_table_ids(reg_id) == {table_a}

        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            svc.update_flow(registration_id=reg_id, requesting_user_id=1, name="after_rename")

        self._add_reader(graph, 2, table_b)
        graph.save_flow(save_path)
        assert self._linked_table_ids(reg_id) == {table_a, table_b}
        assert graph.flow_settings.source_registration_id == reg_id

        os.unlink(save_path)

    def test_save_flow_records_read_links(self):
        """Saving a flow with catalog_reader nodes should record read links."""
        ns_id = _create_namespace()
        save_path = self._temp_flow_path()
        reg_id = _create_flow_registration(ns_id, name="reader_flow", path=save_path)

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

        graph = _create_graph(source_registration_id=reg_id)
        promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="catalog_reader")
        graph.add_node_promise(promise)
        reader = input_schema.NodeCatalogReader(
            flow_id=graph.flow_id,
            node_id=1,
            catalog_table_id=table_id,
        )
        graph.add_catalog_reader(reader)

        graph.save_flow(save_path)

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

    def test_reader_backfills_identity_from_full_name(self):
        """A reader referenced only by catalog_full_table_name gets its
        catalog_table_id / namespace_id / table_name back-filled on add, so the
        settings form populates on reopen and read-lineage can be recorded."""
        ns_id = _create_namespace()

        df = pl.DataFrame(SAMPLE_DATA)
        tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        df.write_parquet(tmp.name)
        tmp.close()

        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            table_out = svc.register_table(name="named_table", file_path=tmp.name, owner_id=1, namespace_id=ns_id)
        table_id = table_out.id

        graph = _create_graph()
        graph.add_node_promise(
            input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="catalog_reader")
        )
        reader = input_schema.NodeCatalogReader(
            flow_id=graph.flow_id,
            node_id=1,
            catalog_full_table_name="TestSch.named_table",
        )
        graph.add_catalog_reader(reader)

        assert reader.catalog_table_id == table_id
        assert reader.catalog_namespace_id == ns_id
        assert reader.catalog_table_name == "named_table"

        os.unlink(tmp.name)

    def test_register_python_editor_flow_records_named_reader_link(self):
        """register_python_editor_flow records read links for readers referenced
        only by full table name (regression: the demo flow's reader produced no
        read lineage)."""
        from flowfile_core.flowfile.catalog_helpers import register_python_editor_flow

        ns_id = _create_namespace()

        df = pl.DataFrame(SAMPLE_DATA)
        tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        df.write_parquet(tmp.name)
        tmp.close()

        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            table_out = svc.register_table(
                name="demo_named_table", file_path=tmp.name, owner_id=1, namespace_id=ns_id
            )
        table_id = table_out.id

        graph = _create_graph()
        graph.add_node_promise(
            input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="catalog_reader")
        )
        graph.add_catalog_reader(
            input_schema.NodeCatalogReader(
                flow_id=graph.flow_id,
                node_id=1,
                catalog_full_table_name="TestSch.demo_named_table",
                user_id=1,
            )
        )

        with tempfile.TemporaryDirectory() as tmpd:
            flow_path = str(Path(tmpd) / "named_reader_flow.yaml")
            reg_id = register_python_editor_flow(
                graph, name="named_reader_flow", namespace_id=ns_id, flow_path=flow_path, user_id=1
            )

        with get_db_context() as db:
            link = db.query(CatalogTableReadLink).filter_by(table_id=table_id, registration_id=reg_id).first()
            assert link is not None

        os.unlink(tmp.name)


# Round-trip: write → read


class TestCatalogRoundTrip:
    """Test writing data to the catalog then reading it back."""

    def test_write_then_read_preserves_data(self, execution_location):
        """Data written by a catalog_writer should be readable by a catalog_reader."""
        ns_id = _create_namespace()

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

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            tables = repo.list_tables(namespace_id=ns_id)
            assert len(tables) == 1
            table_id = tables[0].id

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

        names = sorted(result_df["name"].to_list())
        assert names == ["Alice", "Bob", "Charlie"]


# Catalog SQL reader tests


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
            sql_query="SELEC * FORM people",
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
        """An optimized virtual table with a current fingerprint deserializes the stored LazyFrame."""
        lf = pl.LazyFrame({"x": [1, 2, 3]})
        buf = _io.BytesIO()
        lf.serialize(buf)
        serialized = buf.getvalue()
        result = _resolve_virtual_table(
            is_optimized=True,
            serialized_lf=serialized,
            catalog_table_id=-1,
            run_location="local",
            source_table_versions="[]",
        )

        assert isinstance(result, pl.LazyFrame)
        df = result.collect()
        assert df["x"].to_list() == [1, 2, 3]

    def test_resolve_optimized_without_fingerprint_falls_back(self):
        """No recorded source versions → the pinned stored plan can't be trusted;
        resolution must fall back to re-executing the producer flow."""
        lf = pl.LazyFrame({"x": [1, 2, 3]})
        buf = _io.BytesIO()
        lf.serialize(buf)
        serialized = buf.getvalue()

        with patch("flowfile_core.flowfile.flow_graph.get_db_context") as mock_ctx:
            mock_db = MagicMock()
            mock_ctx.return_value.__enter__ = MagicMock(return_value=mock_db)
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)

            with patch("flowfile_core.flowfile.flow_graph.CatalogService") as MockSvc:
                mock_svc_instance = MagicMock()
                mock_svc_instance.resolve_virtual_flow_table.return_value = pl.LazyFrame({"y": [1]})
                MockSvc.return_value = mock_svc_instance

                _resolve_virtual_table(is_optimized=True, serialized_lf=serialized, catalog_table_id=42)

        mock_svc_instance.resolve_virtual_flow_table.assert_called_once()

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
        mock_svc_instance.resolve_virtual_flow_table.assert_called_once_with(
            42, user_id=None, run_location=None, node_logger=None
        )

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

        mock_svc_instance.resolve_virtual_flow_table.assert_called_once_with(
            99, user_id=None, run_location=None, node_logger=None
        )


class TestWriteCatalogDeltaLocal:
    """Test _write_catalog_delta_local helper."""

    def test_write_delta_creates_table_and_returns_metadata(self, tmp_path):
        """Writing a new Delta table should return metadata with schema, row_count, etc."""
        df = FlowDataEngine(pl.LazyFrame({"name": ["Alice", "Bob"], "age": [30, 25]}))
        dest_path = tmp_path / "test_table"

        result, scd2_version = _write_catalog_delta_local(df, dest_path, delta_mode="overwrite", merge_keys=None)

        assert result is not None
        assert scd2_version is None  # only an scd2 write names a version
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
        result, _ = _write_catalog_delta_local(df2, dest_path, delta_mode="append", merge_keys=None)

        assert result is not None
        assert result["row_count"] == 2  # metadata reflects the appended batch

    def test_scd2_initial_load_reports_post_write_shape(self, tmp_path):
        """The scd2 branch reports the table's own shape, not the input frame's."""
        df = FlowDataEngine(pl.LazyFrame({"id": [1, 2], "val": ["a", "b"]}))
        dest_path = tmp_path / "scd2_local"

        result, scd2_version = _write_catalog_delta_local(
            df,
            dest_path,
            delta_mode="scd2",
            merge_keys=["id"],
            scd2_kwargs=_scd2_primitive_kwargs(_SCD2_CFG, "2024-01-01T00:00:00+00:00"),
        )

        assert result is not None
        assert scd2_version == 0
        assert result["row_count"] == 2
        # 2 data columns + the 4 generated ones.
        assert result["column_count"] == 6
        assert {c["name"] for c in result["schema"]} == {"id", "val", "sk", "valid_from", "valid_to", "is_current"}
        assert result["size_bytes"] > 0
        assert result["scd2_metrics"]["rows_inserted"] == 2
        assert result["scd2_metrics"]["created"] is True

    def test_scd2_change_closes_and_inserts(self, tmp_path):
        dest_path = tmp_path / "scd2_local_change"
        _write_catalog_delta_local(
            FlowDataEngine(pl.LazyFrame({"id": [1, 2], "val": ["a", "b"]})),
            dest_path,
            delta_mode="scd2",
            merge_keys=["id"],
            scd2_kwargs=_scd2_primitive_kwargs(_SCD2_CFG, "2024-01-01T00:00:00+00:00"),
        )

        result, scd2_version = _write_catalog_delta_local(
            FlowDataEngine(pl.LazyFrame({"id": [1, 2], "val": ["A", "b"]})),
            dest_path,
            delta_mode="scd2",
            merge_keys=["id"],
            scd2_kwargs=_scd2_primitive_kwargs(_SCD2_CFG, "2024-02-01T00:00:00+00:00"),
        )

        assert result is not None
        assert scd2_version == 1
        assert result["row_count"] == 3
        assert result["scd2_metrics"] == {
            "rows_inserted": 1,
            "rows_closed": 1,
            "rows_total": 3,
            "rows_current": 2,
            "created": False,
        }

    def test_scd2_unchanged_returns_none(self, tmp_path):
        """The skip protocol: an unchanged batch returns no metadata, so nothing is re-registered."""
        dest_path = tmp_path / "scd2_local_skip"
        frame = pl.LazyFrame({"id": [1], "val": ["a"]})
        _write_catalog_delta_local(
            FlowDataEngine(frame),
            dest_path,
            delta_mode="scd2",
            merge_keys=["id"],
            scd2_kwargs=_scd2_primitive_kwargs(_SCD2_CFG, "2024-01-01T00:00:00+00:00"),
        )

        result, scd2_version = _write_catalog_delta_local(
            FlowDataEngine(frame),
            dest_path,
            delta_mode="scd2",
            merge_keys=["id"],
            scd2_kwargs=_scd2_primitive_kwargs(_SCD2_CFG, "2024-02-01T00:00:00+00:00"),
        )

        assert result is None
        # The skip still names the version the writer node reads its output back at.
        assert scd2_version == 0


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
            assert len(tables) == 1
            assert tables[0].id == original_id

    def test_virtual_write_auto_registers_python_flow(self, tmp_path):
        """Virtual write on an unregistered flow should auto-register it under Python Editor."""
        ns_id = _create_namespace()
        graph = _create_graph(source_registration_id=None)
        # Give the flow a writable yaml path so save_flow succeeds during auto-register.
        graph.flow_settings.path = str(tmp_path / "auto_reg.yaml")

        _add_manual_input(graph, SAMPLE_DATA, node_id=1)
        promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="catalog_writer")
        graph.add_node_promise(promise)
        writer = input_schema.NodeCatalogWriter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            catalog_write_settings=input_schema.CatalogWriteSettings(
                table_name="auto_registered_vt",
                namespace_id=ns_id,
                write_mode="virtual",
            ),
            user_id=1,
        )
        graph.add_catalog_writer(writer)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=2))

        _run_graph(graph)

        assert graph._flow_settings.source_registration_id is not None
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            tables = repo.list_tables(namespace_id=ns_id)
            assert len(tables) == 1
            assert tables[0].name == "auto_registered_vt"
            assert tables[0].table_type == "virtual"


SCD2_V1 = [
    {"id": 1, "name": "Alice", "city": "Amsterdam"},
    {"id": 2, "name": "Bob", "city": "Berlin"},
]
SCD2_V2 = [
    {"id": 1, "name": "Alice", "city": "Antwerp"},  # changed
    {"id": 2, "name": "Bob", "city": "Berlin"},  # unchanged
    {"id": 3, "name": "Carol", "city": "Copenhagen"},  # new
]


def _add_scd2_writer(graph, node_id, depending_on_id, table_name, namespace_id, **scd2_kwargs):
    """Attach an SCD2 catalog writer keyed on ``id``."""
    _add_catalog_writer(
        graph,
        node_id=node_id,
        depending_on_id=depending_on_id,
        table_name=table_name,
        namespace_id=namespace_id,
        write_mode="scd2",
        merge_keys=["id"],
        scd2=input_schema.Scd2Settings(**scd2_kwargs),
    )


def _table_row(namespace_id: int, name: str):
    with get_db_context() as db:
        repo = SQLAlchemyCatalogRepository(db)
        return next(t for t in repo.list_tables(namespace_id=namespace_id) if t.name == name)


SCD2_SYSTEM_COLUMNS = ["sk", "valid_from", "valid_to", "is_current"]


def _node_output(graph, node_id: int = 2) -> pl.DataFrame:
    """Materialize what a node passes downstream (the test's own collect, never core's)."""
    return graph.get_node(node_id).get_resulting_data().data_frame.collect()


def _add_code_node(graph, node_id: int, depending_on_id: int, code: str) -> None:
    """Insert a polars_code node — the shortest way to shape a batch the manual input can't."""
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="polars_code"))
    graph.add_polars_code(
        input_schema.NodePolarsCode(
            flow_id=graph.flow_id,
            node_id=node_id,
            depending_on_ids=[depending_on_id],
            polars_code_input=transform_schema.PolarsCodeInput(polars_code=code),
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id=depending_on_id, to_id=node_id))


class TestCatalogScd2Writer:
    """SCD2 catalog writes through the full graph, on both execution locations."""

    def test_scd2_writer_initial_load_and_change(self, execution_location):
        ns_id = _create_namespace()

        first = _create_graph(flow_id=1, execution_location=execution_location)
        _add_manual_input(first, SCD2_V1, node_id=1)
        _add_scd2_writer(first, 2, 1, "dim_customer", ns_id)
        _run_graph(first)

        table = _table_row(ns_id, "dim_customer")
        assert table.row_count == 2
        assert table.column_count == 7  # 3 data columns + 4 generated
        cfg = json.loads(table.scd2_config)
        assert cfg["business_keys"] == ["id"]
        assert sorted(cfg["compare_columns"]) == ["city", "name"]
        assert cfg["full_snapshot"] is False
        assert cfg["surrogate_key_column"] == "sk"

        df = pl.read_delta(table.file_path)
        assert df.height == 2
        assert df["is_current"].to_list() == [True, True]

        second = _create_graph(flow_id=2, execution_location=execution_location)
        _add_manual_input(second, SCD2_V2, node_id=1)
        _add_scd2_writer(second, 2, 1, "dim_customer", ns_id)
        _run_graph(second)

        table = _table_row(ns_id, "dim_customer")
        assert table.row_count == 4  # 2 original + closed-row rewrite is in place + 2 inserted

        df = pl.read_delta(table.file_path)
        assert df.height == 4
        current = df.filter(pl.col("is_current")).sort("id")
        assert current["id"].to_list() == [1, 2, 3]
        assert current["city"].to_list() == ["Antwerp", "Berlin", "Copenhagen"]
        assert current["valid_to"].null_count() == 3
        closed = df.filter(~pl.col("is_current"))
        assert closed["id"].to_list() == [1]
        assert closed["city"].to_list() == ["Amsterdam"]
        assert closed["valid_to"].null_count() == 0
        # Surrogate keys are unique per version.
        assert df["sk"].n_unique() == 4

        # The DTO the reader/UI consume carries the same config.
        with get_db_context() as db:
            out = CatalogService(SQLAlchemyCatalogRepository(db)).get_table(table.id)
        assert out.scd2 is not None
        assert out.scd2.business_keys == ["id"]
        assert sorted(out.scd2.compare_columns) == ["city", "name"]

    def test_scd2_writer_skips_when_nothing_changed(self, execution_location):
        """An unchanged re-run must not commit and must not bump the catalog record."""
        ns_id = _create_namespace()

        first = _create_graph(flow_id=1, execution_location=execution_location)
        _add_manual_input(first, SCD2_V1, node_id=1)
        _add_scd2_writer(first, 2, 1, "dim_stable", ns_id)
        _run_graph(first)

        before = _table_row(ns_id, "dim_stable")
        updated_at_before = before.updated_at
        # SQLite datetime granularity is coarse; make a real bump observable.
        time.sleep(1.05)

        second = _create_graph(flow_id=2, execution_location=execution_location)
        _add_manual_input(second, SCD2_V1, node_id=1)
        _add_scd2_writer(second, 2, 1, "dim_stable", ns_id)
        _run_graph(second)

        after = _table_row(ns_id, "dim_stable")
        assert after.updated_at == updated_at_before
        assert pl.read_delta(after.file_path).height == 2

    def test_scd2_full_snapshot_end_dates_absent_keys(self):
        ns_id = _create_namespace()

        first = _create_graph(flow_id=1, execution_location="local")
        _add_manual_input(first, SCD2_V1, node_id=1)
        _add_scd2_writer(first, 2, 1, "dim_snapshot", ns_id, full_snapshot=True)
        _run_graph(first)

        second = _create_graph(flow_id=2, execution_location="local")
        _add_manual_input(second, [SCD2_V1[0]], node_id=1)
        _add_scd2_writer(second, 2, 1, "dim_snapshot", ns_id, full_snapshot=True)
        _run_graph(second)

        table = _table_row(ns_id, "dim_snapshot")
        df = pl.read_delta(table.file_path)
        assert df.filter(pl.col("is_current"))["id"].to_list() == [1]
        assert df.filter(~pl.col("is_current"))["id"].to_list() == [2]
        assert json.loads(table.scd2_config)["full_snapshot"] is True

    def test_scd2_compare_columns_pin_change_detection(self):
        """A column outside the compare set changes without opening a new version."""
        ns_id = _create_namespace()

        first = _create_graph(flow_id=1, execution_location="local")
        _add_manual_input(first, SCD2_V1, node_id=1)
        _add_scd2_writer(first, 2, 1, "dim_pinned", ns_id, compare_columns=["city"])
        _run_graph(first)

        second = _create_graph(flow_id=2, execution_location="local")
        _add_manual_input(second, [{"id": 1, "name": "ALICE", "city": "Amsterdam"}, SCD2_V1[1]], node_id=1)
        _add_scd2_writer(second, 2, 1, "dim_pinned", ns_id, compare_columns=["city"])
        _run_graph(second)

        table = _table_row(ns_id, "dim_pinned")
        assert json.loads(table.scd2_config)["compare_columns"] == ["city"]
        assert pl.read_delta(table.file_path).height == 2


class TestScd2WriterOutput:
    """What an SCD2 catalog writer passes downstream, on both execution locations."""

    def test_input_mode_initial_load_carries_the_minted_keys(self, execution_location):
        ns_id = _create_namespace()

        graph = _create_graph(flow_id=1, execution_location=execution_location)
        _add_manual_input(graph, SCD2_V1, node_id=1)
        _add_scd2_writer(graph, 2, 1, "out_initial", ns_id)
        _run_graph(graph)

        out = _node_output(graph)
        assert out.columns == ["id", "name", "city", *SCD2_SYSTEM_COLUMNS]
        assert out["id"].to_list() == [1, 2]
        assert out["is_current"].to_list() == [True, True]
        assert out["valid_to"].null_count() == 2

        table = pl.read_delta(_table_row(ns_id, "out_initial").file_path).sort("id")
        assert out.sort("id")["sk"].to_list() == table["sk"].to_list()

    def test_input_mode_mixed_batch_keeps_order_and_maps_every_row(self, execution_location):
        ns_id = _create_namespace()

        first = _create_graph(flow_id=1, execution_location=execution_location)
        _add_manual_input(first, SCD2_V1, node_id=1)
        _add_scd2_writer(first, 2, 1, "out_mixed", ns_id)
        _run_graph(first)
        unchanged_sk_before = (
            _node_output(first).filter(pl.col("id") == 2)["sk"].item()
        )

        # Fed in an order that is neither sorted nor the table's, so "input order" is falsifiable.
        scrambled = [SCD2_V2[2], SCD2_V2[0], SCD2_V2[1]]
        second = _create_graph(flow_id=2, execution_location=execution_location)
        _add_manual_input(second, scrambled, node_id=1)
        _add_scd2_writer(second, 2, 1, "out_mixed", ns_id)
        _run_graph(second)

        out = _node_output(second)
        assert out.height == len(scrambled)
        assert out["id"].to_list() == [3, 1, 2]
        assert out["city"].to_list() == ["Copenhagen", "Antwerp", "Berlin"]
        assert out["sk"].null_count() == 0
        # The unchanged key keeps the surrogate key of the version it already had.
        assert out.filter(pl.col("id") == 2)["sk"].item() == unchanged_sk_before

        table = pl.read_delta(_table_row(ns_id, "out_mixed").file_path)
        current_sks = set(table.filter(pl.col("is_current"))["sk"].to_list())
        assert set(out["sk"].to_list()) == current_sks
        assert out["is_current"].to_list() == [True, True, True]

    def test_input_mode_skipped_write_still_returns_the_full_map(self, execution_location):
        ns_id = _create_namespace()

        first = _create_graph(flow_id=1, execution_location=execution_location)
        _add_manual_input(first, SCD2_V1, node_id=1)
        _add_scd2_writer(first, 2, 1, "out_skipped", ns_id)
        _run_graph(first)

        second = _create_graph(flow_id=2, execution_location=execution_location)
        _add_manual_input(second, SCD2_V1, node_id=1)
        _add_scd2_writer(second, 2, 1, "out_skipped", ns_id)
        _run_graph(second)

        out = _node_output(second)
        assert out.columns == ["id", "name", "city", *SCD2_SYSTEM_COLUMNS]
        assert out["id"].to_list() == [1, 2]
        assert out["sk"].null_count() == 0
        assert out.sort("id")["sk"].to_list() == (
            pl.read_delta(_table_row(ns_id, "out_skipped").file_path).sort("id")["sk"].to_list()
        )

    def test_input_mode_empty_batch_returns_no_rows_with_the_full_schema(self, execution_location):
        ns_id = _create_namespace()

        first = _create_graph(flow_id=1, execution_location=execution_location)
        _add_manual_input(first, SCD2_V1, node_id=1)
        _add_scd2_writer(first, 2, 1, "out_empty", ns_id)
        _run_graph(first)

        second = _create_graph(flow_id=2, execution_location=execution_location)
        _add_manual_input(second, SCD2_V1, node_id=1)
        _add_code_node(second, 2, 1, "output_df = input_df.head(0)")
        _add_scd2_writer(second, 3, 2, "out_empty", ns_id)
        _run_graph(second)

        out = _node_output(second, node_id=3)
        assert out.height == 0
        assert out.columns == ["id", "name", "city", *SCD2_SYSTEM_COLUMNS]
        assert out.schema["sk"] == pl.String
        assert out.schema["is_current"] == pl.Boolean

    def test_changed_mode_returns_the_closed_and_inserted_versions(self, execution_location):
        ns_id = _create_namespace()

        first = _create_graph(flow_id=1, execution_location=execution_location)
        _add_manual_input(first, SCD2_V1, node_id=1)
        _add_scd2_writer(first, 2, 1, "out_changed", ns_id, output_mode="changed")
        _run_graph(first)
        # An initial load touched every row it wrote.
        assert _node_output(first).height == 2

        second = _create_graph(flow_id=2, execution_location=execution_location)
        _add_manual_input(second, SCD2_V2, node_id=1)
        _add_scd2_writer(second, 2, 1, "out_changed", ns_id, output_mode="changed")
        _run_graph(second)

        out = _node_output(second)
        assert out.columns == ["id", "name", "city", *SCD2_SYSTEM_COLUMNS]
        # id=1 changed (its old version closed + a new one inserted) and id=3 is new; id=2 is
        # untouched. So: 1 closed + 2 inserted = 3 rows, and the changed key appears twice.
        assert out.height == 3
        assert sorted(out["id"].to_list()) == [1, 1, 3]
        assert sorted(out.filter(pl.col("is_current"))["id"].to_list()) == [1, 3]
        closed = out.filter(~pl.col("is_current"))
        assert closed["id"].to_list() == [1]
        assert closed["city"].to_list() == ["Amsterdam"]
        assert closed["valid_to"].null_count() == 0

    def test_changed_mode_skipped_write_returns_no_rows(self, execution_location):
        ns_id = _create_namespace()

        first = _create_graph(flow_id=1, execution_location=execution_location)
        _add_manual_input(first, SCD2_V1, node_id=1)
        _add_scd2_writer(first, 2, 1, "out_changed_skip", ns_id, output_mode="changed")
        _run_graph(first)

        second = _create_graph(flow_id=2, execution_location=execution_location)
        _add_manual_input(second, SCD2_V1, node_id=1)
        _add_scd2_writer(second, 2, 1, "out_changed_skip", ns_id, output_mode="changed")
        _run_graph(second)

        out = _node_output(second)
        assert out.height == 0
        assert out.columns == ["id", "name", "city", *SCD2_SYSTEM_COLUMNS]

    def test_changed_mode_includes_full_snapshot_close_outs(self):
        ns_id = _create_namespace()

        first = _create_graph(flow_id=1, execution_location="local")
        _add_manual_input(first, SCD2_V1, node_id=1)
        _add_scd2_writer(first, 2, 1, "out_changed_snap", ns_id, full_snapshot=True, output_mode="changed")
        _run_graph(first)

        second = _create_graph(flow_id=2, execution_location="local")
        _add_manual_input(second, [SCD2_V1[0]], node_id=1)
        _add_scd2_writer(second, 2, 1, "out_changed_snap", ns_id, full_snapshot=True, output_mode="changed")
        _run_graph(second)

        out = _node_output(second)
        # Nothing changed and nothing is new, but id=2 was end-dated for being absent.
        assert out["id"].to_list() == [2]
        assert out["is_current"].to_list() == [False]
        assert out["valid_to"].null_count() == 0

    def test_current_mode_includes_keys_absent_from_the_input(self, execution_location):
        ns_id = _create_namespace()

        first = _create_graph(flow_id=1, execution_location=execution_location)
        _add_manual_input(first, SCD2_V1, node_id=1)
        _add_scd2_writer(first, 2, 1, "out_current", ns_id)
        _run_graph(first)

        second = _create_graph(flow_id=2, execution_location=execution_location)
        _add_manual_input(second, [{"id": 3, "name": "Carol", "city": "Copenhagen"}], node_id=1)
        _add_scd2_writer(second, 2, 1, "out_current", ns_id, output_mode="current")
        _run_graph(second)

        out = _node_output(second)
        assert out.columns == ["id", "name", "city", *SCD2_SYSTEM_COLUMNS]
        assert sorted(out["id"].to_list()) == [1, 2, 3]
        assert out["is_current"].to_list() == [True, True, True]

    def test_current_mode_skipped_write_still_returns_the_current_slice(self):
        ns_id = _create_namespace()

        first = _create_graph(flow_id=1, execution_location="local")
        _add_manual_input(first, SCD2_V1, node_id=1)
        _add_scd2_writer(first, 2, 1, "out_current_skip", ns_id, output_mode="current")
        _run_graph(first)

        second = _create_graph(flow_id=2, execution_location="local")
        _add_manual_input(second, SCD2_V1, node_id=1)
        _add_scd2_writer(second, 2, 1, "out_current_skip", ns_id, output_mode="current")
        _run_graph(second)

        assert sorted(_node_output(second)["id"].to_list()) == [1, 2]

    def test_output_is_pinned_to_the_version_this_write_settled_on(self, execution_location):
        """A later commit cannot change what this run passed downstream — on either location.

        Also the end-to-end proof that the version survives the worker round-trip: without it the
        read falls back to "latest" and the intruder row leaks in.
        """
        ns_id = _create_namespace()

        graph = _create_graph(flow_id=1, execution_location=execution_location)
        _add_manual_input(graph, SCD2_V1, node_id=1)
        _add_scd2_writer(graph, 2, 1, "out_pinned", ns_id, output_mode="current")
        _run_graph(graph)
        plan = graph.get_node(2).get_resulting_data().data_frame

        path = _table_row(ns_id, "out_pinned").file_path
        intruder = pl.read_delta(path).head(1).with_columns(pl.lit(99, dtype=pl.Int64).alias("id"))
        intruder.write_delta(path, mode="append")

        assert 99 not in plan.collect()["id"].to_list()
        assert 99 in pl.read_delta(path)["id"].to_list()

    def test_non_scd2_write_still_passes_its_input_through(self, execution_location):
        ns_id = _create_namespace()

        graph = _create_graph(flow_id=1, execution_location=execution_location)
        _add_manual_input(graph, SCD2_V1, node_id=1)
        _add_catalog_writer(
            graph, node_id=2, depending_on_id=1, table_name="out_plain", namespace_id=ns_id, write_mode="overwrite"
        )
        _run_graph(graph)

        out = _node_output(graph)
        assert out.columns == ["id", "name", "city"]
        assert out["id"].to_list() == [1, 2]

    def test_wider_stored_business_key_still_joins_in_input_mode(self):
        """An Int32 batch against an Int64-keyed table: the table side is cast back to join."""
        ns_id = _create_namespace()

        first = _create_graph(flow_id=1, execution_location="local")
        _add_manual_input(first, SCD2_V1, node_id=1)  # manual input types `id` as Int64
        _add_scd2_writer(first, 2, 1, "out_narrow_key", ns_id)
        _run_graph(first)
        assert pl.read_delta(_table_row(ns_id, "out_narrow_key").file_path).schema["id"] == pl.Int64

        second = _create_graph(flow_id=2, execution_location="local")
        _add_manual_input(second, SCD2_V2, node_id=1)
        _add_code_node(second, 2, 1, "output_df = input_df.with_columns(pl.col('id').cast(pl.Int32))")
        _add_scd2_writer(second, 3, 2, "out_narrow_key", ns_id)
        _run_graph(second)

        out = _node_output(second, node_id=3)
        assert out.schema["id"] == pl.Int32
        assert out["id"].to_list() == [1, 2, 3]
        assert out["sk"].null_count() == 0
        current_sks = set(
            pl.read_delta(_table_row(ns_id, "out_narrow_key").file_path)
            .filter(pl.col("is_current"))["sk"]
            .to_list()
        )
        assert set(out["sk"].to_list()) == current_sks


class TestScd2WriterDesignTimeSchema:
    """The canvas must advertise the generated columns before the flow is ever run."""

    def _writer_graph(self, ns_id: int, **writer_kwargs) -> object:
        graph = _create_graph(flow_id=1, execution_location="local")
        _add_manual_input(graph, SCD2_V1, node_id=1)
        _add_catalog_writer(
            graph, node_id=2, depending_on_id=1, table_name="schema_probe", namespace_id=ns_id, **writer_kwargs
        )
        return graph

    def test_scd2_writer_advertises_the_generated_columns(self):
        graph = self._writer_graph(
            _create_namespace(), write_mode="scd2", merge_keys=["id"], scd2=input_schema.Scd2Settings()
        )

        names = [c.column_name for c in graph.get_node(2).schema]
        assert names == ["id", "name", "city", *SCD2_SYSTEM_COLUMNS]
        dtypes = {c.column_name: c.data_type for c in graph.get_node(2).schema}
        assert dtypes["sk"] == "String"
        assert dtypes["is_current"] == "Boolean"

    def test_renamed_generated_columns_are_advertised_under_their_new_names(self):
        graph = self._writer_graph(
            _create_namespace(),
            write_mode="scd2",
            merge_keys=["id"],
            scd2=input_schema.Scd2Settings(surrogate_key_column="row_key", is_current_column="active"),
        )

        names = [c.column_name for c in graph.get_node(2).schema]
        assert names == ["id", "name", "city", "row_key", "valid_from", "valid_to", "active"]

    def test_non_scd2_writer_advertises_its_input_schema_unchanged(self):
        graph = self._writer_graph(_create_namespace(), write_mode="overwrite")

        assert [c.column_name for c in graph.get_node(2).schema] == ["id", "name", "city"]


class TestScd2WriterOutputHandle:
    """The canvas handle a writer only has in SCD2 mode.

    The template declares ``output=0``; the settings raise it, so a saved flow must reopen with
    the handle intact or the downstream edge is orphaned.
    """

    def _writer_graph(self, **writer_kwargs):
        graph = _create_graph(flow_id=1, execution_location="local")
        _add_manual_input(graph, SCD2_V1, node_id=1)
        _add_catalog_writer(
            graph,
            node_id=2,
            depending_on_id=1,
            table_name="handle_probe",
            namespace_id=_create_namespace(),
            **writer_kwargs,
        )
        return graph

    def test_scd2_writer_declares_one_output_handle(self):
        graph = self._writer_graph(write_mode="scd2", merge_keys=["id"], scd2=input_schema.Scd2Settings())

        node_input = graph.get_node(2).get_node_input()
        assert node_input.output == 0
        assert node_input.output_names == ["main"]

    def test_non_scd2_writer_stays_a_sink(self):
        graph = self._writer_graph(write_mode="overwrite")

        node_input = graph.get_node(2).get_node_input()
        assert node_input.output == 0
        assert node_input.output_names is None


class TestScd2WriterDownstream:
    """A node wired behind an SCD2 writer runs on the frame the writer passes on."""

    def test_downstream_node_reads_the_generated_keys(self, execution_location):
        ns_id = _create_namespace()

        graph = _create_graph(flow_id=1, execution_location=execution_location)
        _add_manual_input(graph, SCD2_V1, node_id=1)
        _add_scd2_writer(graph, 2, 1, "downstream_scd2", ns_id)
        _add_code_node(graph, 3, 2, 'output_df = input_df.select("id", "sk")')
        _run_graph(graph)

        out = _node_output(graph, node_id=3)
        assert out.columns == ["id", "sk"]
        assert out["id"].to_list() == [1, 2]
        assert out["sk"].null_count() == 0

    def test_downstream_of_a_non_scd2_writer_sees_the_untouched_input(self, execution_location):
        ns_id = _create_namespace()

        graph = _create_graph(flow_id=1, execution_location=execution_location)
        _add_manual_input(graph, SCD2_V1, node_id=1)
        _add_catalog_writer(
            graph, node_id=2, depending_on_id=1, table_name="downstream_plain", namespace_id=ns_id
        )
        _add_code_node(graph, 3, 2, "output_df = input_df")
        _run_graph(graph)

        out = _node_output(graph, node_id=3)
        assert out.columns == ["id", "name", "city"]
        assert out["city"].to_list() == ["Amsterdam", "Berlin"]


class TestScd2WriterPreview:
    """The writer's own drawer preview and column stats.

    An output-group node previews its upstream input, but an SCD2 writer declares an output
    handle of its own, so its preview must be the frame it emits.
    """

    def test_scd2_writer_previews_the_frame_it_emits(self, execution_location):
        ns_id = _create_namespace()

        graph = _create_graph(flow_id=1, execution_location=execution_location)
        _add_manual_input(graph, SCD2_V1, node_id=1)
        _add_scd2_writer(graph, 2, 1, "preview_scd2", ns_id)
        _run_graph(graph)

        example = graph.get_node(2).get_table_example(include_data=True)
        assert example.columns == ["id", "name", "city", *SCD2_SYSTEM_COLUMNS]
        assert example.has_example_data is True
        assert len(example.data) == 2
        assert all(row["sk"] is not None for row in example.data)

    def test_non_scd2_writer_still_previews_its_input(self, execution_location):
        ns_id = _create_namespace()

        graph = _create_graph(flow_id=1, execution_location=execution_location)
        _add_manual_input(graph, SCD2_V1, node_id=1)
        _add_catalog_writer(graph, node_id=2, depending_on_id=1, table_name="preview_plain", namespace_id=ns_id)
        _run_graph(graph)

        example = graph.get_node(2).get_table_example(include_data=True)
        assert example.columns == ["id", "name", "city"]

    def test_scd2_writer_column_stats_cover_a_generated_column(self, execution_location):
        ns_id = _create_namespace()

        graph = _create_graph(flow_id=1, execution_location=execution_location)
        _add_manual_input(graph, SCD2_V1, node_id=1)
        _add_scd2_writer(graph, 2, 1, "stats_scd2", ns_id)
        _run_graph(graph)

        stats = graph.get_node(2).get_column_stats("sk")
        assert stats.name == "sk"
        assert stats.number_of_empty_values == 0
        assert stats.number_of_filled_values == 2
        assert stats.number_of_unique_values == 2


class TestScd2SettingsValidation:
    """Server-side name allowlist: these four names are generated, and they reach Delta merge SQL."""

    @pytest.mark.parametrize("field", ["surrogate_key_column", "valid_from_column", "valid_to_column",
                                       "is_current_column"])
    @pytest.mark.parametrize("bad", ['valid"x', "a b", "a;b", "col)"])
    def test_illegal_characters_are_rejected(self, field, bad):
        with pytest.raises(ValueError, match="may only contain letters"):
            input_schema.Scd2Settings(**{field: bad})

    def test_ordinary_names_are_accepted(self):
        cfg = input_schema.Scd2Settings(
            surrogate_key_column="row_key-1",
            valid_from_column="From2",
            valid_to_column="to_2",
            is_current_column="IS_CURRENT",
        )
        assert cfg.system_columns == ["row_key-1", "From2", "to_2", "IS_CURRENT"]


class TestCatalogScd2Guards:
    """The write-side refusals that keep an SCD2 table's history honest."""

    def _seed_plain_table(self, ns_id: int, name: str):
        graph = _create_graph(flow_id=1, execution_location="local")
        _add_manual_input(graph, SCD2_V1, node_id=1)
        _add_catalog_writer(graph, node_id=2, depending_on_id=1, table_name=name, namespace_id=ns_id)
        _run_graph(graph)

    def _seed_scd2_table(self, ns_id: int, name: str):
        graph = _create_graph(flow_id=1, execution_location="local")
        _add_manual_input(graph, SCD2_V1, node_id=1)
        _add_scd2_writer(graph, 2, 1, name, ns_id)
        _run_graph(graph)

    def _run_expecting_error(self, graph, fragment: str):
        run_info = graph.run_graph()
        assert not run_info.success
        errors = " ".join(str(step.error) for step in run_info.node_step_result if not step.success)
        assert fragment in errors, errors

    def test_scd2_over_existing_plain_table_raises(self):
        ns_id = _create_namespace()
        self._seed_plain_table(ns_id, "plain_first")

        graph = _create_graph(flow_id=2, execution_location="local")
        _add_manual_input(graph, SCD2_V1, node_id=1)
        _add_scd2_writer(graph, 2, 1, "plain_first", ns_id)
        self._run_expecting_error(graph, "is not an SCD2 table")

        assert _table_row(ns_id, "plain_first").scd2_config is None

    def test_append_over_scd2_table_raises(self):
        ns_id = _create_namespace()
        self._seed_scd2_table(ns_id, "dim_protected")

        graph = _create_graph(flow_id=2, execution_location="local")
        _add_manual_input(graph, SCD2_V2, node_id=1)
        _add_catalog_writer(
            graph, node_id=2, depending_on_id=1, table_name="dim_protected", namespace_id=ns_id, write_mode="append"
        )
        self._run_expecting_error(graph, "is SCD2-tracked")

        table = _table_row(ns_id, "dim_protected")
        assert table.scd2_config is not None
        assert pl.read_delta(table.file_path).height == 2

    def test_upsert_over_scd2_table_raises(self):
        ns_id = _create_namespace()
        self._seed_scd2_table(ns_id, "dim_protected_upsert")

        graph = _create_graph(flow_id=2, execution_location="local")
        _add_manual_input(graph, SCD2_V2, node_id=1)
        _add_catalog_writer(
            graph,
            node_id=2,
            depending_on_id=1,
            table_name="dim_protected_upsert",
            namespace_id=ns_id,
            write_mode="upsert",
            merge_keys=["id"],
        )
        self._run_expecting_error(graph, "is SCD2-tracked")

    def test_overwrite_over_scd2_table_clears_the_flag(self):
        ns_id = _create_namespace()
        self._seed_scd2_table(ns_id, "dim_rebuilt")

        graph = _create_graph(flow_id=2, execution_location="local")
        _add_manual_input(graph, SCD2_V2, node_id=1)
        _add_catalog_writer(
            graph, node_id=2, depending_on_id=1, table_name="dim_rebuilt", namespace_id=ns_id, write_mode="overwrite"
        )
        _run_graph(graph)

        table = _table_row(ns_id, "dim_rebuilt")
        assert table.scd2_config is None
        df = pl.read_delta(table.file_path)
        assert df.height == 3
        assert "is_current" not in df.columns

    def test_business_key_drift_raises(self):
        ns_id = _create_namespace()
        self._seed_scd2_table(ns_id, "dim_drift")

        graph = _create_graph(flow_id=2, execution_location="local")
        _add_manual_input(graph, SCD2_V2, node_id=1)
        _add_catalog_writer(
            graph,
            node_id=2,
            depending_on_id=1,
            table_name="dim_drift",
            namespace_id=ns_id,
            write_mode="scd2",
            merge_keys=["name"],
            scd2=input_schema.Scd2Settings(),
        )
        self._run_expecting_error(graph, "do not match the existing table")

    def test_generated_column_rename_raises(self):
        ns_id = _create_namespace()
        self._seed_scd2_table(ns_id, "dim_rename")

        graph = _create_graph(flow_id=2, execution_location="local")
        _add_manual_input(graph, SCD2_V2, node_id=1)
        _add_scd2_writer(graph, 2, 1, "dim_rename", ns_id, surrogate_key_column="row_key")
        self._run_expecting_error(graph, "do not match the existing table")

    def test_all_columns_are_keys_raises(self):
        ns_id = _create_namespace()
        graph = _create_graph(flow_id=1, execution_location="local")
        _add_manual_input(graph, [{"id": 1}, {"id": 2}], node_id=1)
        _add_scd2_writer(graph, 2, 1, "dim_keys_only", ns_id)
        self._run_expecting_error(graph, "no columns to compare")


class TestCatalogReaderFreshness:
    """catalog_reader nodes must see new Delta data on re-run (Development mode included)."""

    @staticmethod
    def _register_delta_table(ns_id: int, tmp_dir: str, name: str = "fresh_delta") -> tuple[int, str]:
        from shared.delta_utils import write_delta

        delta_path = f"{tmp_dir}/{name}"
        write_delta(pl.DataFrame(SAMPLE_DATA), delta_path, mode="overwrite")
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            table = svc.register_table_from_data(
                name=name,
                table_path=delta_path,
                owner_id=1,
                namespace_id=ns_id,
                storage_format="delta",
                schema=[{"name": "name", "dtype": "Utf8"}, {"name": "age", "dtype": "Int64"}],
                row_count=3,
                column_count=3,
                size_bytes=100,
            )
            return table.id, delta_path

    @staticmethod
    def _add_reader(graph, table_id: int, node_id: int = 1, **kwargs):
        promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="catalog_reader")
        graph.add_node_promise(promise)
        reader = input_schema.NodeCatalogReader(
            flow_id=graph.flow_id,
            node_id=node_id,
            catalog_table_id=table_id,
            **kwargs,
        )
        graph.add_catalog_reader(reader)
        return graph.get_node(node_id)

    def test_dev_mode_rerun_sees_out_of_band_write(self, execution_location):
        from shared.delta_utils import write_delta

        with tempfile.TemporaryDirectory() as tmp_dir:
            ns_id = _create_namespace()
            table_id, delta_path = self._register_delta_table(ns_id, tmp_dir)

            graph = _create_graph(execution_location=execution_location)
            node = self._add_reader(graph, table_id)

            _run_graph(graph)
            hash_run1 = node.hash
            assert len(node.get_resulting_data().collect()) == 3

            write_delta(pl.DataFrame({"name": ["Zed"], "age": [99], "city": ["X"]}), delta_path, mode="overwrite")

            _run_graph(graph)
            hash_run2 = node.hash
            assert hash_run2 != hash_run1
            assert len(node.get_resulting_data().collect()) == 1

            epoch_after_run2 = node._cache_epoch
            _run_graph(graph)
            assert node._cache_epoch == epoch_after_run2
            assert node.hash == hash_run2

    def test_cache_results_lookup_hash_rotates_on_version_bump(self):
        """Explicit cache_results caches are keyed by node hash on the worker;
        a Delta version bump must rotate the lookup hash so the stale entry
        becomes unreachable."""
        from shared.delta_utils import write_delta

        with tempfile.TemporaryDirectory() as tmp_dir:
            ns_id = _create_namespace()
            table_id, delta_path = self._register_delta_table(ns_id, tmp_dir, name="cached_delta")

            graph = _create_graph(execution_location="remote")
            node = self._add_reader(graph, table_id, cache_results=True)

            looked_up: list[str] = []

            def fake_results_exists(node_hash: str) -> bool:
                looked_up.append(node_hash)
                return True

            with patch(
                "flowfile_core.flowfile.flow_node.executor.results_exists",
                side_effect=fake_results_exists,
            ):
                graph.run_graph()
                write_delta(pl.DataFrame({"name": ["Zed"], "age": [99], "city": ["X"]}), delta_path, mode="overwrite")
                graph.run_graph()

            assert len(looked_up) >= 2
            assert looked_up[0] != looked_up[-1]

    def test_pinned_delta_version_never_invalidated(self):
        from shared.delta_utils import write_delta

        with tempfile.TemporaryDirectory() as tmp_dir:
            ns_id = _create_namespace()
            table_id, delta_path = self._register_delta_table(ns_id, tmp_dir, name="pinned_delta")

            graph = _create_graph(execution_location="local")
            node = self._add_reader(graph, table_id, delta_version=0)

            _run_graph(graph)
            hash_run1 = node.hash

            write_delta(pl.DataFrame({"name": ["Zed"], "age": [99], "city": ["X"]}), delta_path, mode="append")

            _run_graph(graph)
            assert node.hash == hash_run1
            assert node._execution_state.source_version_info is None
            assert len(node.get_resulting_data().collect()) == 3

    def test_probe_failure_fails_open(self):
        """A vanished table re-runs (and errors) instead of serving the frozen snapshot."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            ns_id = _create_namespace()
            table_id, delta_path = self._register_delta_table(ns_id, tmp_dir, name="doomed_delta")

            graph = _create_graph(execution_location="local")
            node = self._add_reader(graph, table_id)

            _run_graph(graph)
            epoch_before = node._cache_epoch

            shutil.rmtree(delta_path)

            run_info = graph.run_graph()
            assert node._cache_epoch == epoch_before + 1
            assert not run_info.success

    def test_sql_reader_invalidates_only_on_referenced_table_change(self):
        from shared.delta_utils import write_delta

        with tempfile.TemporaryDirectory() as tmp_dir:
            ns_id = _create_namespace()
            _, path_a = self._register_delta_table(ns_id, tmp_dir, name="sql_tbl_a")
            _, path_b = self._register_delta_table(ns_id, tmp_dir, name="sql_tbl_b")

            graph = _create_graph(execution_location="local")
            promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="catalog_reader")
            graph.add_node_promise(promise)
            reader = input_schema.NodeCatalogReader(
                flow_id=graph.flow_id,
                node_id=1,
                sql_query="SELECT * FROM sql_tbl_a",
            )
            graph.add_catalog_reader(reader)
            node = graph.get_node(1)

            _run_graph(graph)
            epoch_start = node._cache_epoch

            write_delta(pl.DataFrame({"name": ["Zed"], "age": [99], "city": ["X"]}), path_b, mode="overwrite")
            _run_graph(graph)
            assert node._cache_epoch == epoch_start

            write_delta(pl.DataFrame({"name": ["Zed"], "age": [99], "city": ["X"]}), path_a, mode="overwrite")
            _run_graph(graph)
            assert node._cache_epoch == epoch_start + 1
            assert len(node.get_resulting_data().collect()) == 1

    def test_fingerprint_is_canonical_and_stable(self):
        from flowfile_core.flowfile.flow_graph import _catalog_reader_source_fingerprint

        with tempfile.TemporaryDirectory() as tmp_dir:
            ns_id = _create_namespace()
            table_id, _ = self._register_delta_table(ns_id, tmp_dir, name="stable_delta")
            settings = input_schema.NodeCatalogReader(flow_id=1, node_id=1, catalog_table_id=table_id)

            fp1, force1 = _catalog_reader_source_fingerprint(settings, {}, {})
            fp2, force2 = _catalog_reader_source_fingerprint(settings, {}, {})
            assert force1 is force2 is False
            assert fp1 == fp2
            assert fp1 is not None

    def test_execution_state_round_trips_source_version_info(self):
        from flowfile_core.flowfile.flow_node.state import NodeExecutionState

        state = NodeExecutionState()
        state.source_version_info = '{"path": "/x", "version": 3}'
        restored = NodeExecutionState.from_dict(state.to_dict())
        assert restored.source_version_info == state.source_version_info
        restored.reset()
        assert restored.source_version_info == state.source_version_info
