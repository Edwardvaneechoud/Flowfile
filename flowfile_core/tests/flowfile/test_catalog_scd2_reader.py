"""Tests for the SCD2 history-view filter on catalog_reader nodes.

Per the product decision (overrides an earlier design draft): ``scd2_view=None`` means "no
filter" (every version), not "active". ``"active"``/``"active_at"`` opt into a filtered view.
Column names always come from the table's persisted ``scd2_config``, never the reader settings.
"""

import datetime
import json

import polars as pl
import pytest

from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.database.connection import get_db_context
from flowfile_core.schemas import input_schema
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


def _add_reader(graph, node_id, *, table_name, namespace_id, scd2_view=None, scd2_as_of=None):
    promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="catalog_reader")
    graph.add_node_promise(promise)
    reader = input_schema.NodeCatalogReader(
        flow_id=graph.flow_id,
        node_id=node_id,
        catalog_table_name=table_name,
        catalog_namespace_id=namespace_id,
        scd2_view=scd2_view,
        scd2_as_of=scd2_as_of,
    )
    graph.add_catalog_reader(reader)
    return reader


def _seed_history(namespace_id: int, table_name: str, execution_location: str) -> tuple:
    """Write V1 then V2 to *table_name*, and return the exact (t1, t2) instants used.

    ``t1`` is the initial-load ``valid_from``; ``t2`` is both the close ``valid_to`` of the
    changed row and the ``valid_from`` of its replacement version — the version boundary.
    """
    first = _create_graph(flow_id=1, execution_location=execution_location)
    _add_manual_input(first, SCD2_V1, node_id=1)
    _add_scd2_writer(first, 2, 1, table_name, namespace_id)
    _run_graph(first)

    second = _create_graph(flow_id=2, execution_location=execution_location)
    _add_manual_input(second, SCD2_V2, node_id=1)
    _add_scd2_writer(second, 2, 1, table_name, namespace_id)
    _run_graph(second)

    with get_db_context() as db:
        repo = SQLAlchemyCatalogRepository(db)
        table = next(t for t in repo.list_tables(namespace_id=namespace_id) if t.name == table_name)
        file_path = table.file_path

    df = pl.read_delta(file_path)
    t1 = df.filter(pl.col("id") == 2)["valid_from"][0]
    t2 = df.filter((pl.col("id") == 1) & (~pl.col("is_current")))["valid_to"][0]
    return t1, t2


class TestCatalogScd2ReaderView:
    """History-view filtering, wired at ``_add_catalog_table_reader``."""

    def test_unset_view_returns_all_versions(self, execution_location):
        """Product decision: scd2_view=None means no filter, not 'active'."""
        ns_id = _create_namespace()
        _seed_history(ns_id, "dim_all_default", execution_location)

        graph = _create_graph(flow_id=3, execution_location=execution_location)
        _add_reader(graph, 1, table_name="dim_all_default", namespace_id=ns_id)
        _run_graph(graph)

        result = graph.get_node(1).get_resulting_data().collect()
        assert len(result) == 4

    def test_all_view_returns_every_version(self, execution_location):
        ns_id = _create_namespace()
        _seed_history(ns_id, "dim_all", execution_location)

        graph = _create_graph(flow_id=3, execution_location=execution_location)
        _add_reader(graph, 1, table_name="dim_all", namespace_id=ns_id, scd2_view="all")
        _run_graph(graph)

        result = graph.get_node(1).get_resulting_data().collect()
        assert len(result) == 4

    def test_active_view_returns_current_rows_only(self, execution_location):
        ns_id = _create_namespace()
        _seed_history(ns_id, "dim_active", execution_location)

        graph = _create_graph(flow_id=3, execution_location=execution_location)
        _add_reader(graph, 1, table_name="dim_active", namespace_id=ns_id, scd2_view="active")
        _run_graph(graph)

        result = graph.get_node(1).get_resulting_data().collect()
        assert len(result) == 3
        assert result["valid_to"].null_count() == 3
        assert sorted(result["city"].to_list()) == ["Antwerp", "Berlin", "Copenhagen"]

    def test_active_at_now_accepts_the_ui_z_suffix_and_matches_active(self, execution_location):
        """The DateTimePicker emits "...Z", which Python 3.10's fromisoformat rejects unnormalized.

        "Active at <now>" must behave exactly like "Active records": every timestamp is a UTC
        instant, so the caller's wall-clock timezone never shifts which versions are visible.
        """
        ns_id = _create_namespace()
        _seed_history(ns_id, "dim_active_at_now", execution_location)
        now_utc = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=1)
        as_of = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

        graph = _create_graph(flow_id=3, execution_location=execution_location)
        _add_reader(
            graph, 1, table_name="dim_active_at_now", namespace_id=ns_id, scd2_view="active_at", scd2_as_of=as_of
        )
        _run_graph(graph)

        result = graph.get_node(1).get_resulting_data().collect()
        assert len(result) == 3
        assert sorted(result["city"].to_list()) == ["Antwerp", "Berlin", "Copenhagen"]

    def test_scd2_as_of_validator_accepts_a_z_suffixed_instant(self):
        reader = input_schema.NodeCatalogReader(
            flow_id=1,
            node_id=1,
            catalog_table_name="t",
            scd2_view="active_at",
            scd2_as_of="2026-08-01T08:39:01Z",
        )
        assert reader.scd2_as_of == "2026-08-01T08:39:01Z"

    def test_active_at_before_any_write_returns_nothing(self):
        ns_id = _create_namespace()
        t1, _t2 = _seed_history(ns_id, "dim_active_at_t0", "local")
        before = (t1 - datetime.timedelta(days=1)).isoformat()

        graph = _create_graph(flow_id=3, execution_location="local")
        _add_reader(graph, 1, table_name="dim_active_at_t0", namespace_id=ns_id, scd2_view="active_at", scd2_as_of=before)
        _run_graph(graph)

        result = graph.get_node(1).get_resulting_data().collect()
        assert len(result) == 0

    def test_active_at_between_versions_returns_the_older_version(self):
        ns_id = _create_namespace()
        t1, t2 = _seed_history(ns_id, "dim_active_at_mid", "local")
        mid = (t1 + (t2 - t1) / 2).isoformat()

        graph = _create_graph(flow_id=3, execution_location="local")
        _add_reader(graph, 1, table_name="dim_active_at_mid", namespace_id=ns_id, scd2_view="active_at", scd2_as_of=mid)
        _run_graph(graph)

        result = graph.get_node(1).get_resulting_data().collect()
        assert sorted(result["id"].to_list()) == [1, 2]
        row1 = result.filter(pl.col("id") == 1)
        assert row1["city"].to_list() == ["Amsterdam"]

    def test_active_at_exact_boundary_returns_the_newer_version(self):
        """Half-open [valid_from, valid_to): a row closed exactly at T is superseded at T."""
        ns_id = _create_namespace()
        _t1, t2 = _seed_history(ns_id, "dim_active_at_boundary", "local")

        graph = _create_graph(flow_id=3, execution_location="local")
        _add_reader(
            graph,
            1,
            table_name="dim_active_at_boundary",
            namespace_id=ns_id,
            scd2_view="active_at",
            scd2_as_of=t2.isoformat(),
        )
        _run_graph(graph)

        result = graph.get_node(1).get_resulting_data().collect()
        assert sorted(result["id"].to_list()) == [1, 2, 3]
        row1 = result.filter(pl.col("id") == 1)
        assert row1["city"].to_list() == ["Antwerp"]

    def test_scd2_view_on_non_scd2_table_is_ignored(self, execution_location):
        """Retargeting a reader to a plain table must never fail the flow."""
        ns_id = _create_namespace()

        graph = _create_graph(flow_id=1, execution_location=execution_location)
        _add_manual_input(graph, SCD2_V1, node_id=1)
        _add_catalog_writer(graph, node_id=2, depending_on_id=1, table_name="plain_table", namespace_id=ns_id)
        _run_graph(graph)

        read_graph = _create_graph(flow_id=2, execution_location=execution_location)
        _add_reader(read_graph, 1, table_name="plain_table", namespace_id=ns_id, scd2_view="active")
        _run_graph(read_graph)

        result = read_graph.get_node(1).get_resulting_data().collect()
        assert len(result) == 2

    def test_stale_scd2_flag_degrades_to_an_unfiltered_read(self, execution_location):
        """A record whose data lost its system columns must not fail the read with the filter."""
        ns_id = _create_namespace()
        _seed_history(ns_id, "dim_stale_flag", execution_location)

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            table = next(t for t in repo.list_tables(namespace_id=ns_id) if t.name == "dim_stale_flag")
            # Simulate a kernel/refresh overwrite that replaced the data with a plain table while
            # leaving the SCD2 record behind.
            table.schema_json = json.dumps([{"name": "id", "dtype": "Int64"}, {"name": "name", "dtype": "String"}])
            db.commit()
            assert table.scd2_config

        read_graph = _create_graph(flow_id=3, execution_location=execution_location)
        _add_reader(read_graph, 1, table_name="dim_stale_flag", namespace_id=ns_id, scd2_view="active")
        _run_graph(read_graph)

        result = read_graph.get_node(1).get_resulting_data().collect()
        assert len(result) == 4
