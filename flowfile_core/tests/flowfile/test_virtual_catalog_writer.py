"""Tests for virtual flow table catalog writer and upstream laziness checking.

Covers:
- Virtual catalog writer: creates virtual tables without materializing data
- Virtual writer requires a source_registration_id
- Serialized lazy frame storage and is_optimized flag
- check_flow_laziness / check_upstream_laziness scoping
- FlowRegistration.flow_path usage (regression for file_path AttributeError)
- Source table version tracking and staleness detection
"""

import json
import tempfile

import polars as pl
import pytest

from flowfile_core.catalog import CatalogService
from flowfile_core.catalog.delta_utils import check_source_versions_current
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import (
    FlowRegistration,
    FlowSchedule,
)
from flowfile_core.flowfile.flow_graph import add_connection
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.transform_schema import BasicFilter, FilterInput, PivotInput
from shared.delta_models import SourceTableVersion
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


@pytest.fixture()
def catalog_service() -> CatalogService:
    with get_db_context() as db:
        repo = SQLAlchemyCatalogRepository(db)
        yield CatalogService(repo)


@pytest.fixture()
def lazy_virtual_table_id() -> int:
    ns_id = _create_namespace()
    with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as f:
        flow_path = f.name
    reg_id = _create_flow_registration(ns_id, name="lazy_flow", path=flow_path)
    graph = _create_graph(source_registration_id=reg_id)
    # Node 1: manual input
    _add_manual_input(graph, SAMPLE_DATA, node_id=1)

    # Node 2: filter (lazy operation)
    promise_filter = input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="filter")
    graph.add_node_promise(promise_filter)
    filter_settings = input_schema.NodeFilter(
        flow_id=graph.flow_id,
        node_id=2,
        depending_on_id=1,
        filter_input=FilterInput(
            mode="basic",
            basic_filter=BasicFilter(field="age", operator="greater_than", value="28"),
        ),
    )
    graph.add_filter(filter_settings)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=2))

    # Node 3: virtual catalog writer
    _add_catalog_writer(
        graph,
        node_id=3,
        depending_on_id=2,
        table_name="lazy_virtual",
        namespace_id=ns_id,
        write_mode="virtual",
    )

    _run_graph(graph)
    graph.save_flow(flow_path)

    with get_db_context() as db:
        repo = SQLAlchemyCatalogRepository(db)
        tables = repo.list_tables(namespace_id=ns_id)
        table = next(t for t in tables if t.name == "lazy_virtual")

    return table.id


@pytest.fixture()
def eager_virtual_table_id() -> int:
    ns_id = _create_namespace()
    with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as f:
        flow_path = f.name
    reg_id = _create_flow_registration(ns_id, name="eager_flow", path=flow_path)
    graph = _create_graph(source_registration_id=reg_id)

    # Node 1: manual input
    _add_manual_input(graph, SAMPLE_DATA, node_id=1)

    # Node 2: pivot (eager operation)
    promise_pivot = input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="pivot")
    graph.add_node_promise(promise_pivot)
    pivot_settings = input_schema.NodePivot(
        flow_id=graph.flow_id,
        node_id=2,
        depending_on_id=1,
        pivot_input=PivotInput(
            index_columns=["name"],
            pivot_column="city",
            value_col="age",
            aggregations=["first"],
        ),
    )
    graph.add_pivot(pivot_settings)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=2))

    # Node 3: virtual catalog writer
    _add_catalog_writer(
        graph,
        node_id=3,
        depending_on_id=2,
        table_name="eager_virtual",
        namespace_id=ns_id,
        write_mode="virtual",
    )

    _run_graph(graph)
    graph.save_flow(flow_path)

    with get_db_context() as db:
        repo = SQLAlchemyCatalogRepository(db)
        tables = repo.list_tables(namespace_id=ns_id)
        table = next(t for t in tables if t.name == "eager_virtual")

    return table.id


@pytest.fixture(autouse=True)
def clean_state():
    _cleanup()
    yield
    _cleanup()


class TestVirtualCatalogWriter:
    """Test that catalog_writer nodes in virtual mode create non-materialized tables."""

    def test_virtual_writer_creates_virtual_table(self):
        """Running a flow with a virtual catalog_writer should create a CatalogTable
        with table_type='virtual' and no physical file."""
        ns_id = _create_namespace()
        reg_id = _create_flow_registration(ns_id, name="producer_flow")
        graph = _create_graph(source_registration_id=reg_id)

        _add_manual_input(graph, SAMPLE_DATA, node_id=1)
        _add_catalog_writer(
            graph,
            node_id=2,
            depending_on_id=1,
            table_name="virtual_table",
            namespace_id=ns_id,
            write_mode="virtual",
        )

        _run_graph(graph)

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            tables = repo.list_tables(namespace_id=ns_id)
            assert len(tables) == 1
            table = tables[0]
            assert table.name == "virtual_table"
            assert table.table_type == "virtual"
            assert table.producer_registration_id == reg_id

    def test_virtual_writer_requires_source_registration(self):
        """When source_registration_id is not set, virtual write should fail
        with a clear error about missing registration."""
        ns_id = _create_namespace()
        graph = _create_graph(source_registration_id=None)

        _add_manual_input(graph, SAMPLE_DATA, node_id=1)
        _add_catalog_writer(
            graph,
            node_id=2,
            depending_on_id=1,
            table_name="should_fail",
            namespace_id=ns_id,
            write_mode="virtual",
        )

        with pytest.raises(AssertionError, match="not linked to a catalog registration"):
            _run_graph(graph)

    def test_virtual_writer_stores_serialized_lazyframe_when_lazy(self):
        """A fully lazy graph (manual_input → filter → virtual writer) should store
        a serialized lazy frame and set is_optimized=True."""
        ns_id = _create_namespace()
        reg_id = _create_flow_registration(ns_id, name="lazy_flow")
        graph = _create_graph(source_registration_id=reg_id)
        # Node 1: manual input
        _add_manual_input(graph, SAMPLE_DATA, node_id=1)

        # Node 2: filter (lazy operation)
        promise_filter = input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="filter")
        graph.add_node_promise(promise_filter)
        filter_settings = input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            filter_input=FilterInput(
                mode="basic",
                basic_filter=BasicFilter(field="age", operator="greater_than", value="28"),
            ),
        )
        graph.add_filter(filter_settings)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=2))

        # Node 3: virtual catalog writer
        _add_catalog_writer(
            graph,
            node_id=3,
            depending_on_id=2,
            table_name="lazy_virtual",
            namespace_id=ns_id,
            write_mode="virtual",
        )

        _run_graph(graph)

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            tables = repo.list_tables(namespace_id=ns_id)
            assert len(tables) == 1
            table = tables[0]
            assert table.table_type == "virtual"
            assert table.serialized_lazy_frame is not None
            assert table.is_optimized is True

    def test_virtual_writer_non_optimized_when_upstream_eager(self):
        """A graph with an eager node upstream of the virtual writer should
        set is_optimized=False."""
        ns_id = _create_namespace()
        reg_id = _create_flow_registration(ns_id, name="eager_flow")
        graph = _create_graph(source_registration_id=reg_id)

        # Node 1: manual input
        _add_manual_input(graph, SAMPLE_DATA, node_id=1)
        # Node 2: pivot (eager operation)
        promise_pivot = input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="pivot")
        graph.add_node_promise(promise_pivot)
        pivot_settings = input_schema.NodePivot(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            pivot_input=PivotInput(
                index_columns=["name"],
                pivot_column="city",
                value_col="age",
                aggregations=["first"],
            ),
        )
        graph.add_pivot(pivot_settings)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=2))

        # Node 3: virtual catalog writer
        _add_catalog_writer(
            graph,
            node_id=3,
            depending_on_id=2,
            table_name="eager_virtual",
            namespace_id=ns_id,
            write_mode="virtual",
        )

        _run_graph(graph)

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            tables = repo.list_tables(namespace_id=ns_id)
            assert len(tables) == 1
            table = tables[0]
            assert table.table_type == "virtual"
            assert table.is_optimized is False


class TestReadVirtualFlowTables:
    """Tests that virtual tables can be resolved and previewed."""

    def test_lazy_frame_table_preview(self, lazy_virtual_table_id, catalog_service):
        """Preview of an optimized virtual table should return filtered rows."""
        preview = catalog_service.get_table_preview(lazy_virtual_table_id)
        assert len(preview.columns) > 0
        # Filter is age > 28, so Alice (30) and Charlie (35) match
        assert len(preview.rows) == 2

    def test_lazy_frame_resolve(self, lazy_virtual_table_id, catalog_service):
        """resolve_virtual_flow_table on an optimized table returns a LazyFrame."""
        lf = catalog_service.resolve_virtual_flow_table(lazy_virtual_table_id)
        assert isinstance(lf, pl.LazyFrame)
        df = lf.collect()
        assert df.height == 2

    def test_eager_frame_table_preview(self, eager_virtual_table_id, catalog_service):
        """Preview of a non-optimized (eager) virtual table should re-execute the flow."""
        preview = catalog_service.get_table_preview(eager_virtual_table_id)
        assert len(preview.columns) > 0
        # Pivot of 3 rows by name produces 3 rows
        assert len(preview.rows) == 3

    def test_eager_frame_resolve(self, eager_virtual_table_id, catalog_service):
        """resolve_virtual_flow_table on a non-optimized table re-executes and returns LazyFrame."""
        lf = catalog_service.resolve_virtual_flow_table(eager_virtual_table_id)
        assert isinstance(lf, pl.LazyFrame)
        df = lf.collect()
        assert df.height == 3

    def test_catalog_reader_reads_lazy_virtual_table(self, lazy_virtual_table_id):
        """A catalog_reader node should be able to read from an optimized virtual table in a graph."""
        graph = _create_graph()
        promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="catalog_reader")
        graph.add_node_promise(promise)
        reader = input_schema.NodeCatalogReader(
            flow_id=graph.flow_id,
            node_id=1,
            catalog_table_id=lazy_virtual_table_id,
        )
        graph.add_catalog_reader(reader)

        _run_graph(graph)

        node = graph.get_node(1)
        result_df = node.get_resulting_data().collect()
        assert len(result_df) == 2
        assert set(result_df.columns) == {"name", "age", "city"}

    def test_catalog_reader_reads_eager_virtual_table(self, eager_virtual_table_id):
        """A catalog_reader node should be able to read from a non-optimized virtual table in a graph."""
        graph = _create_graph()
        promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="catalog_reader")
        graph.add_node_promise(promise)
        reader = input_schema.NodeCatalogReader(
            flow_id=graph.flow_id,
            node_id=1,
            catalog_table_id=eager_virtual_table_id,
        )
        graph.add_catalog_reader(reader)

        _run_graph(graph)

        node = graph.get_node(1)
        result_df = node.get_resulting_data().collect()
        assert len(result_df) == 3

    def test_lazy_frame_query(self, lazy_virtual_table_id, catalog_service):
        """SQL query against a virtual table should work."""
        catalog_service.execute_sql_query("select * from lazy_virtual")


class TestCheckUpstreamLaziness:
    """Test that laziness checking correctly scopes to upstream catalog writer deps."""

    def test_check_upstream_laziness_excludes_unrelated_branches(self):
        """check_flow_laziness should not include nodes from branches that don't
        feed into a catalog_writer (e.g. an explore_data node on a separate path)."""
        ns_id = _create_namespace()
        reg_id = _create_flow_registration(ns_id, name="scoped_flow")
        graph = _create_graph(source_registration_id=reg_id)

        # Branch 1: manual_input (1) → catalog_writer (2) — all lazy
        _add_manual_input(graph, SAMPLE_DATA, node_id=1)
        _add_catalog_writer(
            graph,
            node_id=2,
            depending_on_id=1,
            table_name="scoped_table",
            namespace_id=ns_id,
            write_mode="virtual",
        )

        # Branch 2: manual_input (3) → explore_data (4) — unrelated
        _add_manual_input(graph, SAMPLE_DATA, node_id=3)
        promise_explore = input_schema.NodePromise(flow_id=graph.flow_id, node_id=4, node_type="explore_data")
        graph.add_node_promise(promise_explore)
        explore = input_schema.NodeExploreData(flow_id=graph.flow_id, node_id=4)
        graph.add_explore_data(explore)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id=3, to_id=4))

        is_lazy, reasons = graph.check_flow_laziness()

        # explore_data should NOT appear in reasons — it's on a separate branch
        assert is_lazy is True
        assert len(reasons) == 0

    def test_check_upstream_laziness_reports_eager_upstream(self):
        """An eager node upstream of a catalog_writer should be reported by
        check_upstream_laziness."""
        ns_id = _create_namespace()
        reg_id = _create_flow_registration(ns_id, name="eager_check_flow")
        graph = _create_graph(source_registration_id=reg_id)

        # manual_input (1) → pivot (2, eager) → catalog_writer (3)
        _add_manual_input(graph, SAMPLE_DATA, node_id=1)

        promise_pivot = input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="pivot")
        graph.add_node_promise(promise_pivot)
        pivot_settings = input_schema.NodePivot(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            pivot_input=PivotInput(
                index_columns=["name"],
                pivot_column="city",
                value_col="age",
                aggregations=["first"],
            ),
        )
        graph.add_pivot(pivot_settings)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=2))

        _add_catalog_writer(
            graph,
            node_id=3,
            depending_on_id=2,
            table_name="eager_check",
            namespace_id=ns_id,
            write_mode="virtual",
        )

        writer_node = graph.get_node(3)
        is_lazy, reasons = writer_node.check_upstream_laziness()

        assert is_lazy is False
        assert len(reasons) >= 1
        assert any("eager" in r.lower() for r in reasons)

    def test_check_upstream_laziness_all_lazy(self):
        """When all upstream nodes are lazy, check_upstream_laziness should
        return (True, [])."""
        ns_id = _create_namespace()
        reg_id = _create_flow_registration(ns_id, name="all_lazy_flow")
        graph = _create_graph(source_registration_id=reg_id)

        # manual_input (1) → filter (2, lazy) → catalog_writer (3)
        _add_manual_input(graph, SAMPLE_DATA, node_id=1)

        promise_filter = input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="filter")
        graph.add_node_promise(promise_filter)
        filter_settings = input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            filter_input=FilterInput(
                mode="basic",
                basic_filter=BasicFilter(field="age", operator="greater_than", value="30"),
            ),
        )
        graph.add_filter(filter_settings)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=2))

        _add_catalog_writer(
            graph,
            node_id=3,
            depending_on_id=2,
            table_name="all_lazy",
            namespace_id=ns_id,
            write_mode="virtual",
        )

        writer_node = graph.get_node(3)
        is_lazy, reasons = writer_node.check_upstream_laziness()

        assert is_lazy is True
        assert reasons == []


# ---------------------------------------------------------------------------
# FlowRegistration attribute tests (regression for file_path → flow_path)
# ---------------------------------------------------------------------------


class TestFlowRegistrationAttributes:
    """Test that virtual table code correctly uses flow_path, not file_path."""

    def test_compute_laziness_blockers_uses_flow_path(self):
        """_compute_laziness_blockers should accept a flow_path without
        AttributeError (regression: previously accessed .file_path)."""
        ns_id = _create_namespace()

        # Save a minimal flow YAML to disk
        graph = _create_graph(flow_id=99)
        _add_manual_input(graph, SAMPLE_DATA, node_id=1)
        _add_catalog_writer(
            graph,
            node_id=2,
            depending_on_id=1,
            table_name="blockers_test",
            namespace_id=ns_id,
            write_mode="virtual",
        )
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as f:
            save_path = f.name
        graph.save_flow(save_path)

        reg_id = _create_flow_registration(ns_id, name="blocker_flow", path=save_path)

        # Look up the registration and call _compute_laziness_blockers via flow_path
        with get_db_context() as db:
            reg = db.get(FlowRegistration, reg_id)
            assert hasattr(reg, "flow_path"), "FlowRegistration should have flow_path attribute"
            assert not hasattr(reg, "file_path"), "FlowRegistration should NOT have file_path attribute"

            # This should not raise AttributeError
            blockers = CatalogService._compute_laziness_blockers(reg.flow_path)
            assert isinstance(blockers, list)

    def test_table_to_out_with_virtual_table(self):
        """_table_to_out should populate laziness_blockers for a virtual table
        with a producer_registration_id without raising AttributeError."""
        ns_id = _create_namespace()
        # Save a flow to disk so _compute_laziness_blockers can load it
        graph = _create_graph(flow_id=99)
        _add_manual_input(graph, SAMPLE_DATA, node_id=1)
        _add_catalog_writer(
            graph,
            node_id=2,
            depending_on_id=1,
            table_name="to_out_test",
            namespace_id=ns_id,
            write_mode="virtual",
        )

        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as f:
            save_path = f.name
        graph.save_flow(save_path)

        reg_id = _create_flow_registration(ns_id, name="to_out_flow", path=save_path)

        # Create a virtual CatalogTable with producer_registration_id
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            table_out = svc.create_virtual_flow_table(
                name="to_out_virtual",
                owner_id=1,
                producer_registration_id=reg_id,
                namespace_id=ns_id,
            )

        # The returned CatalogTableOut should have laziness_blockers without error
        assert table_out.table_type == "virtual"
        assert table_out.laziness_blockers is not None
        assert isinstance(table_out.laziness_blockers, list)


class TestVirtualTableTriggerFiring:
    """Test that updating a virtual table fires table_trigger schedules (push path)."""

    def test_update_virtual_table_fires_trigger(self):
        """When a virtual table is updated via update_virtual_flow_table,
        any enabled table_trigger schedule watching it should fire."""
        ns_id = _create_namespace()
        reg_id = _create_flow_registration(ns_id, name="producer_flow")
        downstream_reg_id = _create_flow_registration(ns_id, name="downstream_flow", path="/tmp/downstream.yaml")

        # Create a virtual table
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            table_out = svc.create_virtual_flow_table(
                name="trigger_test_table",
                owner_id=1,
                producer_registration_id=reg_id,
                namespace_id=ns_id,
            )
            table_id = table_out.id

        # Create a table_trigger schedule watching the virtual table
        with get_db_context() as db:
            schedule = FlowSchedule(
                registration_id=downstream_reg_id,
                schedule_type="table_trigger",
                trigger_table_id=table_id,
                enabled=True,
                owner_id=1,
            )
            db.add(schedule)
            db.commit()
            db.refresh(schedule)
        # Update the virtual table and check that _fire_table_trigger_schedules is called
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)

            fired_tables = []

            def mock_fire(table_id, table_updated_at):
                fired_tables.append(table_id)
                # Don't actually spawn a subprocess
                return 0

            svc._fire_table_trigger_schedules = mock_fire

            svc.update_virtual_flow_table(
                table_id=table_id,
                schema_json='[{"name": "x", "dtype": "Int64"}]',
            )

        assert table_id in fired_tables, "Updating a virtual table should fire table trigger schedules"


class TestVirtualTableOptimizationPropagation:
    """Test that reading a non-optimized virtual table propagates non-optimized status downstream."""

    def test_downstream_virtual_table_not_optimized_when_source_is_not_optimized(self, eager_virtual_table_id):
        """A flow that reads a non-optimized virtual table and writes a new virtual table
        should produce a non-optimized output, even when all other nodes are lazy."""
        ns_id = _create_namespace()
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as f:
            flow_path = f.name
        reg_id = _create_flow_registration(ns_id, name="downstream_eager_flow", path=flow_path)
        graph = _create_graph(source_registration_id=reg_id)

        # Node 1: catalog reader reading the non-optimized (eager) virtual table
        promise_reader = input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="catalog_reader")
        graph.add_node_promise(promise_reader)
        reader = input_schema.NodeCatalogReader(
            flow_id=graph.flow_id,
            node_id=1,
            catalog_table_id=eager_virtual_table_id,
        )
        graph.add_catalog_reader(reader)
        # Node 2: filter (lazy operation) — should not make the chain optimized
        promise_filter = input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="filter")
        graph.add_node_promise(promise_filter)
        filter_settings = input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            filter_input=FilterInput(
                mode="basic",
                basic_filter=BasicFilter(field="name", operator="not_equals", value="Alice"),
            ),
        )
        graph.add_filter(filter_settings)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=2))

        # Node 3: virtual catalog writer
        _add_catalog_writer(
            graph,
            node_id=3,
            depending_on_id=2,
            table_name="downstream_eager_virtual",
            namespace_id=ns_id,
            write_mode="virtual",
        )

        # The catalog_reader's setting_input should reflect non-optimized source
        reader_node = graph.get_node(1)
        assert reader_node.setting_input.is_virtual_optimized is False

        # The downstream writer's upstream laziness check should report non-lazy
        writer_node = graph.get_node(3)
        is_lazy, reasons = writer_node.check_upstream_laziness()
        assert is_lazy is False
        assert any("non-optimized virtual table" in r for r in reasons)
        _run_graph(graph)

    def test_downstream_virtual_table_optimized_when_source_is_optimized(self, lazy_virtual_table_id):
        """A flow that reads an optimized virtual table and writes a new virtual table
        should produce an optimized output when all nodes are lazy."""
        ns_id = _create_namespace()
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as f:
            flow_path = f.name
        reg_id = _create_flow_registration(ns_id, name="downstream_lazy_flow", path=flow_path)
        graph = _create_graph(source_registration_id=reg_id)

        # Node 1: catalog reader reading the optimized (lazy) virtual table
        promise_reader = input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="catalog_reader")
        graph.add_node_promise(promise_reader)
        reader = input_schema.NodeCatalogReader(
            flow_id=graph.flow_id,
            node_id=1,
            catalog_table_id=lazy_virtual_table_id,
        )
        graph.add_catalog_reader(reader)

        # Node 2: filter (lazy operation)
        promise_filter = input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="filter")
        graph.add_node_promise(promise_filter)
        filter_settings = input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            filter_input=FilterInput(
                mode="basic",
                basic_filter=BasicFilter(field="name", operator="not_equals", value="Alice"),
            ),
        )
        graph.add_filter(filter_settings)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=2))

        # Node 3: virtual catalog writer
        _add_catalog_writer(
            graph,
            node_id=3,
            depending_on_id=2,
            table_name="downstream_lazy_virtual",
            namespace_id=ns_id,
            write_mode="virtual",
        )

        # The catalog_reader's setting_input should reflect optimized source
        reader_node = graph.get_node(1)
        assert reader_node.setting_input.is_virtual_optimized is True

        # The downstream writer's upstream laziness check should report all lazy
        writer_node = graph.get_node(3)
        is_lazy, reasons = writer_node.check_upstream_laziness()
        assert is_lazy is True
        assert reasons == []

    def test_sql_reader_not_optimized_when_source_virtual_table_is_not_optimized(self, eager_virtual_table_id):
        """A SQL catalog reader referencing a non-optimized virtual table should propagate
        non-optimized status to downstream virtual writers."""
        ns_id = _create_namespace()
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as f:
            flow_path = f.name
        reg_id = _create_flow_registration(ns_id, name="downstream_sql_eager_flow", path=flow_path)
        graph = _create_graph(source_registration_id=reg_id)
        # Node 1: SQL catalog reader querying the non-optimized virtual table
        promise_reader = input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="catalog_reader")
        graph.add_node_promise(promise_reader)
        reader = input_schema.NodeCatalogReader(
            flow_id=graph.flow_id,
            node_id=1,
            sql_query="SELECT * FROM eager_virtual",
        )
        graph.add_catalog_reader(reader)
        # Node 2: filter (lazy operation)
        promise_filter = input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="filter")
        graph.add_node_promise(promise_filter)
        filter_settings = input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            filter_input=FilterInput(
                mode="basic",
                basic_filter=BasicFilter(field="name", operator="not_equals", value="Alice"),
            ),
        )
        graph.add_filter(filter_settings)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=2))

        # Node 3: virtual catalog writer
        _add_catalog_writer(
            graph,
            node_id=3,
            depending_on_id=2,
            table_name="downstream_sql_eager_virtual",
            namespace_id=ns_id,
            write_mode="virtual",
        )

        # The SQL catalog_reader's setting_input should reflect non-optimized source
        reader_node = graph.get_node(1)
        assert reader_node.setting_input.is_virtual_optimized is False

        # The downstream writer's upstream laziness check should report non-lazy
        writer_node = graph.get_node(3)
        is_lazy, reasons = writer_node.check_upstream_laziness()
        assert is_lazy is False
        assert any("non-optimized virtual table" in r for r in reasons)
        _run_graph(graph)


# ---------------------------------------------------------------------------
# Source table version tracking tests
# ---------------------------------------------------------------------------


class TestCheckSourceVersionsCurrent:
    """Unit tests for the check_source_versions_current utility."""

    def test_none_returns_true(self):
        """Backward compat: None source_table_versions should return True."""
        assert check_source_versions_current(None) is True

    def test_empty_string_returns_true(self):
        """Empty string should return True."""
        assert check_source_versions_current("") is True

    def test_empty_list_returns_true(self):
        """Empty JSON list should return True (no sources to check)."""
        assert check_source_versions_current("[]") is True

    def test_invalid_json_returns_false(self):
        """Malformed JSON should return False (treat as stale)."""
        assert check_source_versions_current("not json") is False

    def test_matching_versions_returns_true(self):
        """When source delta table versions match, should return True."""
        from shared.delta_utils import write_delta

        with tempfile.TemporaryDirectory() as tmp_dir:
            delta_path = f"{tmp_dir}/test_table"
            df = pl.DataFrame({"x": [1, 2, 3]})
            write_delta(df, delta_path, mode="overwrite")

            from deltalake import DeltaTable
            current_version = DeltaTable(delta_path, without_files=True).version()

            versions_json = json.dumps([
                SourceTableVersion(table_id=1, file_path=delta_path, version=current_version).model_dump()
            ])
            assert check_source_versions_current(versions_json) is True

    def test_stale_version_returns_false(self):
        """When source delta table has been updated, should return False."""
        from shared.delta_utils import write_delta

        with tempfile.TemporaryDirectory() as tmp_dir:
            delta_path = f"{tmp_dir}/test_table"
            df = pl.DataFrame({"x": [1, 2, 3]})
            write_delta(df, delta_path, mode="overwrite")

            # Record version 0
            versions_json = json.dumps([
                SourceTableVersion(table_id=1, file_path=delta_path, version=0).model_dump()
            ])

            # Write again to bump version
            write_delta(pl.DataFrame({"x": [4, 5, 6]}), delta_path, mode="overwrite")

            assert check_source_versions_current(versions_json) is False

    def test_missing_file_path_returns_false(self):
        """When the delta table path doesn't exist, should return False."""
        versions_json = json.dumps([
            SourceTableVersion(table_id=1, file_path="/nonexistent/path", version=0).model_dump()
        ])
        assert check_source_versions_current(versions_json) is False


class TestSourceTableVersionCapture:
    """Test that source_table_versions are captured when creating virtual tables
    from flows that read physical delta catalog tables."""

    def test_virtual_table_from_delta_source_stores_versions(self):
        """A virtual table created from a flow reading a physical delta table
        should have source_table_versions populated."""
        from shared.delta_utils import write_delta

        ns_id = _create_namespace()

        # Create a physical delta catalog table
        with tempfile.TemporaryDirectory() as tmp_dir:
            delta_path = f"{tmp_dir}/source_delta"
            df = pl.DataFrame({"name": ["Alice", "Bob"], "age": [30, 25]})
            write_delta(df, delta_path, mode="overwrite")

            with get_db_context() as db:
                repo = SQLAlchemyCatalogRepository(db)
                svc = CatalogService(repo)
                physical_table = svc.register_table_from_data(
                    name="source_physical",
                    table_path=delta_path,
                    owner_id=1,
                    namespace_id=ns_id,
                    storage_format="delta",
                    schema=[{"name": "name", "dtype": "Utf8"}, {"name": "age", "dtype": "Int64"}],
                    row_count=2,
                    column_count=2,
                    size_bytes=100,
                )
                physical_table_id = physical_table.id

            # Create a flow that reads from the physical table and writes a virtual table
            with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as f:
                flow_path = f.name
            reg_id = _create_flow_registration(ns_id, name="version_tracking_flow", path=flow_path)
            graph = _create_graph(source_registration_id=reg_id)

            # Node 1: catalog reader
            promise_reader = input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="catalog_reader")
            graph.add_node_promise(promise_reader)
            reader = input_schema.NodeCatalogReader(
                flow_id=graph.flow_id,
                node_id=1,
                catalog_table_id=physical_table_id,
            )
            graph.add_catalog_reader(reader)

            # Node 2: filter (lazy)
            promise_filter = input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="filter")
            graph.add_node_promise(promise_filter)
            filter_settings = input_schema.NodeFilter(
                flow_id=graph.flow_id,
                node_id=2,
                depending_on_id=1,
                filter_input=FilterInput(
                    mode="basic",
                    basic_filter=BasicFilter(field="age", operator="greater_than", value="20"),
                ),
            )
            graph.add_filter(filter_settings)
            add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=2))

            # Node 3: virtual catalog writer
            _add_catalog_writer(
                graph,
                node_id=3,
                depending_on_id=2,
                table_name="version_tracked_virtual",
                namespace_id=ns_id,
                write_mode="virtual",
            )

            _run_graph(graph)

            # Verify source_table_versions is populated
            with get_db_context() as db:
                repo = SQLAlchemyCatalogRepository(db)
                tables = repo.list_tables(namespace_id=ns_id)
                vt = next(t for t in tables if t.name == "version_tracked_virtual")
                assert vt.is_optimized is True
                assert vt.source_table_versions is not None

                versions = json.loads(vt.source_table_versions)
                assert len(versions) == 1
                assert versions[0]["table_id"] == physical_table_id
                assert versions[0]["file_path"] == delta_path
                assert isinstance(versions[0]["version"], int)

    def test_virtual_table_without_catalog_source_has_no_versions(self):
        """A virtual table from a flow with only manual input should have
        source_table_versions=None (no delta sources to track)."""
        ns_id = _create_namespace()
        reg_id = _create_flow_registration(ns_id, name="no_source_flow")
        graph = _create_graph(source_registration_id=reg_id)

        _add_manual_input(graph, SAMPLE_DATA, node_id=1)

        # Node 2: filter (lazy)
        promise_filter = input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="filter")
        graph.add_node_promise(promise_filter)
        filter_settings = input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            filter_input=FilterInput(
                mode="basic",
                basic_filter=BasicFilter(field="age", operator="greater_than", value="28"),
            ),
        )
        graph.add_filter(filter_settings)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=2))

        _add_catalog_writer(
            graph,
            node_id=3,
            depending_on_id=2,
            table_name="no_source_virtual",
            namespace_id=ns_id,
            write_mode="virtual",
        )

        _run_graph(graph)

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            tables = repo.list_tables(namespace_id=ns_id)
            vt = next(t for t in tables if t.name == "no_source_virtual")
            assert vt.is_optimized is True
            assert vt.source_table_versions is None
