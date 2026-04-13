"""Tests for virtual flow table catalog writer and upstream laziness checking.

Covers:
- Virtual catalog writer: creates virtual tables without materializing data
- Virtual writer requires a source_registration_id
- Serialized lazy frame storage and is_optimized flag
- check_flow_laziness / check_upstream_laziness scoping
- FlowRegistration.flow_path usage (regression for file_path AttributeError)
"""

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
    FlowSchedule,
)
from flowfile_core.flowfile.flow_graph import FlowGraph, RunInformation, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, schemas
from flowfile_core.schemas.transform_schema import BasicFilter, FilterInput, PivotInput


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


def _cleanup():
    """Remove all catalog / flow-registration rows so tests start clean."""
    with get_db_context() as db:
        db.query(CatalogTableReadLink).delete()
        db.query(FlowSchedule).delete()
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
    execution_location: str = "local",
) -> FlowGraph:
    """Create a FlowGraph with optional source_registration_id."""
    handler = FlowfileHandler()
    settings = schemas.FlowSettings(
        flow_id=flow_id,
        name="test_flow",
        path=".",
        execution_mode="Development",
        execution_location=execution_location,
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


def _add_catalog_writer(
    graph: FlowGraph,
    node_id: int,
    depending_on_id: int,
    table_name: str,
    namespace_id: int,
    write_mode: str = "overwrite",
    user_id: int = 1,
):
    """Add a catalog writer node and connect it to its input."""
    promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="catalog_writer")
    graph.add_node_promise(promise)
    writer = input_schema.NodeCatalogWriter(
        flow_id=graph.flow_id,
        node_id=node_id,
        depending_on_id=depending_on_id,
        catalog_write_settings=input_schema.CatalogWriteSettings(
            table_name=table_name,
            namespace_id=namespace_id,
            write_mode=write_mode,
        ),
        user_id=user_id,
    )
    graph.add_catalog_writer(writer)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id=depending_on_id, to_id=node_id))


def _run_graph(graph: FlowGraph) -> RunInformation:
    run_info = graph.run_graph()
    if not run_info.success:
        errors = []
        for step in run_info.node_step_result:
            if not step.success:
                errors.append(f"node {step.node_id}: {step.error}")
        raise AssertionError("Graph execution failed:\n" + "\n".join(errors))
    return run_info


SAMPLE_DATA = [
    {"name": "Alice", "age": 30, "city": "Amsterdam"},
    {"name": "Bob", "age": 25, "city": "Berlin"},
    {"name": "Charlie", "age": 35, "city": "Copenhagen"},
]


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
