"""Tests for catalog change tracking (CDC): node settings, cursor storage, and reader behaviour.

The reader is strict by design — a change mode on an untracked table is an actionable error, never
a silent full read — and cursor advancement is at-least-once: only a full ``run_graph`` in which the
reader and everything downstream completed moves a cursor.
"""

import pytest
from pydantic import ValidationError

from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import CatalogCdcCursor, CatalogTable
from flowfile_core.flowfile.catalog_cdc import init_cursor_value, reset_cursor
from flowfile_core.flowfile.flow_graph import add_connection
from flowfile_core.flowfile.param_types import FlowParameter
from flowfile_core.schemas import input_schema, transform_schema
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

V1 = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
V2 = [{"id": 2, "name": "Bobby"}, {"id": 3, "name": "Carol"}]


@pytest.fixture(autouse=True)
def clean_state():
    _cleanup()
    yield
    _cleanup()


def _add_writer(graph, node_id, depending_on_id, table_name, namespace_id, *, track_changes=True):
    promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="catalog_writer")
    graph.add_node_promise(promise)
    writer = input_schema.NodeCatalogWriter(
        flow_id=graph.flow_id,
        node_id=node_id,
        depending_on_id=depending_on_id,
        catalog_write_settings=input_schema.CatalogWriteSettings(
            table_name=table_name,
            namespace_id=namespace_id,
            write_mode="upsert",
            merge_keys=["id"],
            track_changes=track_changes,
        ),
        user_id=1,
    )
    graph.add_catalog_writer(writer)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id=depending_on_id, to_id=node_id))


def _add_reader(graph, node_id, *, table_name, namespace_id, **cdc):
    promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="catalog_reader")
    graph.add_node_promise(promise)
    reader = input_schema.NodeCatalogReader(
        flow_id=graph.flow_id,
        node_id=node_id,
        catalog_table_name=table_name,
        catalog_namespace_id=namespace_id,
        **cdc,
    )
    graph.add_catalog_reader(reader)
    return reader


def _write(namespace_id, table_name, data, flow_id=1, track_changes=True):
    graph = _create_graph(flow_id=flow_id, execution_location="local")
    _add_manual_input(graph, data, node_id=1)
    _add_writer(graph, 2, 1, table_name, namespace_id, track_changes=track_changes)
    _run_graph(graph)


def _table(namespace_id, table_name) -> CatalogTable:
    with get_db_context() as db:
        repo = SQLAlchemyCatalogRepository(db)
        table = next(t for t in repo.list_tables(namespace_id=namespace_id) if t.name == table_name)
        db.expunge(table)
        return table


def _cursors(table_id) -> list[CatalogCdcCursor]:
    with get_db_context() as db:
        rows = SQLAlchemyCatalogRepository(db).list_cdc_cursors(table_id)
        for row in rows:
            db.expunge(row)
        return rows


class TestCdcSettingsValidators:
    def test_defaults_are_off(self):
        reader = input_schema.NodeCatalogReader(flow_id=1, node_id=1, catalog_table_id=1)
        assert reader.cdc_mode == "off"
        assert reader.cdc_start == "now"
        assert reader.cdc_include_preimage is False

    def test_since_version_requires_a_version(self):
        with pytest.raises(ValidationError, match="cdc_from_version is required"):
            input_schema.NodeCatalogReader(flow_id=1, node_id=1, catalog_table_id=1, cdc_mode="since_version")

    def test_since_timestamp_requires_a_timestamp(self):
        with pytest.raises(ValidationError, match="cdc_from_timestamp is required"):
            input_schema.NodeCatalogReader(flow_id=1, node_id=1, catalog_table_id=1, cdc_mode="since_timestamp")

    def test_since_timestamp_accepts_the_ui_z_suffix(self):
        reader = input_schema.NodeCatalogReader(
            flow_id=1,
            node_id=1,
            catalog_table_id=1,
            cdc_mode="since_timestamp",
            cdc_from_timestamp="2026-01-01T00:00:00Z",
        )
        assert reader.cdc_from_timestamp == "2026-01-01T00:00:00Z"

    def test_since_timestamp_rejects_garbage(self):
        with pytest.raises(ValidationError, match="ISO-8601"):
            input_schema.NodeCatalogReader(
                flow_id=1, node_id=1, catalog_table_id=1, cdc_mode="since_timestamp", cdc_from_timestamp="yesterday"
            )


    def test_since_fields_accept_a_flow_parameter_reference(self):
        by_version = input_schema.NodeCatalogReader(
            flow_id=1, node_id=1, catalog_table_name="orders", cdc_mode="since_version", cdc_from_version="${since}"
        )
        assert by_version.cdc_from_version == "${since}"
        assert "[changes since ${since}]" in by_version.get_default_description()
        by_time = input_schema.NodeCatalogReader(
            flow_id=1, node_id=1, catalog_table_id=1, cdc_mode="since_timestamp", cdc_from_timestamp="${since_ts}"
        )
        assert by_time.cdc_from_timestamp == "${since_ts}"
        with pytest.raises(ValueError, match="commit version or a"):
            input_schema.NodeCatalogReader(
                flow_id=1, node_id=1, catalog_table_id=1, cdc_mode="since_version", cdc_from_version="twelve"
            )

    def test_sql_readers_are_excluded(self):
        with pytest.raises(ValidationError, match="SQL catalog readers"):
            input_schema.NodeCatalogReader(
                flow_id=1, node_id=1, sql_query="select 1", cdc_mode="since_last_run"
            )

    def test_pinned_version_is_excluded(self):
        with pytest.raises(ValidationError, match="pinned table version"):
            input_schema.NodeCatalogReader(
                flow_id=1, node_id=1, catalog_table_id=1, delta_version=3, cdc_mode="since_last_run"
            )

    def test_scd2_history_view_is_excluded(self):
        with pytest.raises(ValidationError, match="SCD2 history view"):
            input_schema.NodeCatalogReader(
                flow_id=1, node_id=1, catalog_table_id=1, scd2_view="active", cdc_mode="since_last_run"
            )
        # "all" is no filter at all, so it is allowed.
        input_schema.NodeCatalogReader(
            flow_id=1, node_id=1, catalog_table_id=1, scd2_view="all", cdc_mode="since_last_run"
        )

    def test_consumer_name_is_stripped_and_charset_checked(self):
        reader = input_schema.NodeCatalogReader(
            flow_id=1, node_id=1, catalog_table_id=1, cdc_consumer_name="  nightly-load  "
        )
        assert reader.cdc_consumer_name == "nightly-load"
        blank = input_schema.NodeCatalogReader(flow_id=1, node_id=1, catalog_table_id=1, cdc_consumer_name="   ")
        assert blank.cdc_consumer_name is None
        with pytest.raises(ValidationError, match="cdc_consumer_name"):
            input_schema.NodeCatalogReader(flow_id=1, node_id=1, catalog_table_id=1, cdc_consumer_name="bad name!")

    def test_description_suffixes(self):
        base = dict(flow_id=1, node_id=1, catalog_table_name="t", catalog_namespace_id=1)
        assert input_schema.NodeCatalogReader(
            **base, cdc_mode="since_last_run"
        ).get_default_description().endswith("[changes since last run]")
        assert input_schema.NodeCatalogReader(
            **base, cdc_mode="since_version", cdc_from_version=4
        ).get_default_description().endswith("[changes since v4]")
        assert input_schema.NodeCatalogReader(
            **base, cdc_mode="since_timestamp", cdc_from_timestamp="2026-01-01T00:00:00Z"
        ).get_default_description().endswith("[changes since 2026-01-01T00:00:00Z]")

    @pytest.mark.parametrize("write_mode", ["overwrite", "virtual", "scd2"])
    def test_track_changes_rejected_for_incompatible_write_modes(self, write_mode):
        with pytest.raises(ValidationError, match="track_changes is not supported"):
            input_schema.CatalogWriteSettings(
                table_name="t", write_mode=write_mode, merge_keys=["id"], track_changes=True
            )

    def test_track_changes_allowed_for_merge_modes(self):
        settings = input_schema.CatalogWriteSettings(
            table_name="t", write_mode="upsert", merge_keys=["id"], track_changes=True
        )
        assert settings.track_changes is True


class TestCdcCursorRepository:
    def _table_id(self) -> int:
        ns_id = _create_namespace()
        with get_db_context() as db:
            table = CatalogTable(name="cursor_host", namespace_id=ns_id, owner_id=1, file_path="/tmp/cursor_host")
            db.add(table)
            db.commit()
            return table.id

    def test_upsert_creates_then_advances(self):
        table_id = self._table_id()
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            created = repo.upsert_cdc_cursor(table_id, "name:nightly", 3, consumer_label="nightly", table_path="/x")
            assert created.last_version == 3
            advanced = repo.upsert_cdc_cursor(table_id, "name:nightly", 7)
            assert advanced.last_version == 7
            # descriptive fields are kept when a later commit cannot resolve them
            assert advanced.consumer_label == "nightly"
            assert advanced.table_path == "/x"
            assert len(repo.list_cdc_cursors(table_id)) == 1

    def test_get_reset_and_delete(self):
        table_id = self._table_id()
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            repo.upsert_cdc_cursor(table_id, "name:a", 5)
            assert repo.get_cdc_cursor(table_id, "name:a").last_version == 5
            assert repo.get_cdc_cursor(table_id, "name:missing") is None
            assert repo.reset_cdc_cursor(table_id, "name:a", 0).last_version == 0
            assert repo.reset_cdc_cursor(table_id, "name:missing", 0) is None
            assert repo.delete_cdc_cursor(table_id, "name:a") is True
            assert repo.delete_cdc_cursor(table_id, "name:a") is False

    def test_set_cdc_enabled_keeps_the_first_floor(self):
        table_id = self._table_id()
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            table = repo.set_cdc_enabled(table_id, 2)
            assert table.cdc_enabled is True
            assert table.cdc_enabled_version == 2
            assert repo.set_cdc_enabled(table_id, 9).cdc_enabled_version == 2

    def test_delete_table_purges_its_cursors(self):
        table_id = self._table_id()
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            repo.upsert_cdc_cursor(table_id, "name:a", 1)
            repo.upsert_cdc_cursor(table_id, "name:b", 1)
            assert len(repo.list_cdc_cursors(table_id)) == 2
            repo.delete_table(table_id)
            assert repo.list_cdc_cursors(table_id) == []

    def test_reset_cursor_resolves_now_beginning_and_explicit(self):
        table_id = self._table_id()
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            repo.upsert_cdc_cursor(table_id, "name:a", 1)
            assert reset_cursor(db, table_id, "name:a", "now", head=9, floor=2).last_version == 9
            assert reset_cursor(db, table_id, "name:a", "beginning", head=9, floor=2).last_version == 1
            assert reset_cursor(db, table_id, "name:a", 4, head=9, floor=2).last_version == 4
            assert reset_cursor(db, table_id, "name:missing", "now", head=9, floor=2) is None


class TestInitCursorValue:
    def test_now_starts_at_head(self):
        assert init_cursor_value("now", head=7, cdc_enabled_version=3) == 7

    def test_beginning_starts_below_the_floor(self):
        assert init_cursor_value("beginning", head=7, cdc_enabled_version=3) == 2

    def test_beginning_without_a_floor_replays_everything(self):
        assert init_cursor_value("beginning", head=7, cdc_enabled_version=None) == -1


class TestCatalogChangeReader:
    """End-to-end: a tracked writer feeding change readers."""

    def test_writer_marks_the_table_tracked(self):
        ns_id = _create_namespace()
        _write(ns_id, "tracked", V1)
        table = _table(ns_id, "tracked")
        assert table.cdc_enabled is True
        assert table.cdc_enabled_version == 0

    def test_untracked_writer_leaves_the_table_alone(self):
        ns_id = _create_namespace()
        _write(ns_id, "plain", V1, track_changes=False)
        assert _table(ns_id, "plain").cdc_enabled is False

    def test_untracked_table_is_a_clear_error(self):
        ns_id = _create_namespace()
        _write(ns_id, "plain", V1, track_changes=False)

        graph = _create_graph(flow_id=3)
        _add_reader(graph, 1, table_name="plain", namespace_id=ns_id, cdc_mode="since_last_run",
                    cdc_consumer_name="c1")
        run_info = graph.run_graph()
        assert not run_info.success
        assert "Change tracking is not enabled" in str(run_info.node_step_result[0].error)

    def test_since_last_run_reads_then_advances_then_is_empty(self):
        ns_id = _create_namespace()
        _write(ns_id, "changes", V1)
        table_id = _table(ns_id, "changes").id

        graph = _create_graph(flow_id=3)
        _add_reader(
            graph, 1, table_name="changes", namespace_id=ns_id,
            cdc_mode="since_last_run", cdc_start="beginning", cdc_consumer_name="c1",
        )
        _run_graph(graph)
        first = graph.get_node(1).get_resulting_data().collect()
        assert sorted(first["id"].to_list()) == [1, 2]
        assert set(first["_change_type"].to_list()) == {"insert"}

        cursors = _cursors(table_id)
        assert len(cursors) == 1
        assert cursors[0].consumer_key == "name:c1"
        assert cursors[0].last_version == 0

        # Second run: nothing new.
        graph2 = _create_graph(flow_id=4)
        _add_reader(
            graph2, 1, table_name="changes", namespace_id=ns_id,
            cdc_mode="since_last_run", cdc_start="beginning", cdc_consumer_name="c1",
        )
        _run_graph(graph2)
        assert graph2.get_node(1).get_resulting_data().collect().height == 0

        # New data: only the delta.
        _write(ns_id, "changes", V2, flow_id=5)
        graph3 = _create_graph(flow_id=6)
        _add_reader(
            graph3, 1, table_name="changes", namespace_id=ns_id,
            cdc_mode="since_last_run", cdc_start="beginning", cdc_consumer_name="c1",
        )
        _run_graph(graph3)
        delta = graph3.get_node(1).get_resulting_data().collect().sort("id")
        assert delta["id"].to_list() == [2, 3]
        assert delta["_change_type"].to_list() == ["update_postimage", "insert"]
        assert _cursors(table_id)[0].last_version == 1

    def test_start_now_skips_existing_history(self):
        ns_id = _create_namespace()
        _write(ns_id, "fresh", V1)

        graph = _create_graph(flow_id=3)
        _add_reader(
            graph, 1, table_name="fresh", namespace_id=ns_id,
            cdc_mode="since_last_run", cdc_start="now", cdc_consumer_name="c1",
        )
        _run_graph(graph)
        assert graph.get_node(1).get_resulting_data().collect().height == 0

    def test_downstream_failure_does_not_advance_the_cursor(self):
        ns_id = _create_namespace()
        _write(ns_id, "guarded", V1)
        table_id = _table(ns_id, "guarded").id

        graph = _create_graph(flow_id=3)
        _add_reader(
            graph, 1, table_name="guarded", namespace_id=ns_id,
            cdc_mode="since_last_run", cdc_start="beginning", cdc_consumer_name="c1",
        )
        promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="polars_code")
        graph.add_node_promise(promise)
        graph.add_polars_code(
            input_schema.NodePolarsCode(
                flow_id=graph.flow_id,
                node_id=2,
                depending_on_ids=[1],
                polars_code_input=transform_schema.PolarsCodeInput(
                    polars_code="output_df = input_df.select(pl.col('no_such_column'))"
                ),
            )
        )
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=2))
        run_info = graph.run_graph()
        assert not run_info.success
        assert _cursors(table_id) == []

    def test_preview_does_not_advance_the_cursor(self):
        """Only ``run_graph`` fires post-execution callbacks; a single-node fetch never does."""
        ns_id = _create_namespace()
        _write(ns_id, "preview", V1)
        table_id = _table(ns_id, "preview").id

        graph = _create_graph(flow_id=3)
        _add_reader(
            graph, 1, table_name="preview", namespace_id=ns_id,
            cdc_mode="since_last_run", cdc_start="beginning", cdc_consumer_name="c1",
        )
        graph.get_node(1).get_resulting_data().collect()
        assert _cursors(table_id) == []

    def test_since_version_reads_everything_after_that_commit(self):
        ns_id = _create_namespace()
        _write(ns_id, "byversion", V1)
        _write(ns_id, "byversion", V2, flow_id=2)

        graph = _create_graph(flow_id=3)
        _add_reader(graph, 1, table_name="byversion", namespace_id=ns_id, cdc_mode="since_version",
                    cdc_from_version=0)
        _run_graph(graph)
        df = graph.get_node(1).get_resulting_data().collect().sort("id")
        assert df["id"].to_list() == [2, 3]
        # a version mode keeps no cursor
        assert _cursors(_table(ns_id, "byversion").id) == []


    def test_since_version_can_come_from_a_flow_parameter(self):
        ns_id = _create_namespace()
        _write(ns_id, "byparam", V1)
        _write(ns_id, "byparam", V2, flow_id=2)

        graph = _create_graph(flow_id=3)
        graph.flow_settings.parameters = [FlowParameter(name="since", default_value="0", type="integer")]
        _add_reader(graph, 1, table_name="byparam", namespace_id=ns_id, cdc_mode="since_version",
                    cdc_from_version="${since}")
        _run_graph(graph)
        df = graph.get_node(1).get_resulting_data().collect().sort("id")
        assert df["id"].to_list() == [2, 3]
        # the resolver restores the placeholder after the run, so the saved settings stay parameterised
        assert graph.get_node(1).setting_input.cdc_from_version == "${since}"

        graph.flow_settings.parameters = [FlowParameter(name="since", default_value="1", type="integer")]
        _run_graph(graph)
        assert graph.get_node(1).get_resulting_data().collect().height == 0

    def test_undefined_since_parameter_is_a_clear_error(self):
        ns_id = _create_namespace()
        _write(ns_id, "noparam", V1)
        graph = _create_graph(flow_id=3)
        _add_reader(graph, 1, table_name="noparam", namespace_id=ns_id, cdc_mode="since_version",
                    cdc_from_version="${missing}")
        run_info = graph.run_graph()
        assert not run_info.success
        assert "missing" in str(graph.get_node(1).results.errors)

    def test_since_timestamp_reads_from_an_instant(self):
        import datetime

        ns_id = _create_namespace()
        _write(ns_id, "bytime", V1)
        instant = datetime.datetime.now(datetime.timezone.utc).isoformat()
        _write(ns_id, "bytime", V2, flow_id=2)

        graph = _create_graph(flow_id=3)
        _add_reader(graph, 1, table_name="bytime", namespace_id=ns_id, cdc_mode="since_timestamp",
                    cdc_from_timestamp=instant)
        _run_graph(graph)
        df = graph.get_node(1).get_resulting_data().collect().sort("id")
        assert df["id"].to_list() == [2, 3]

    def test_since_timestamp_is_clamped_to_the_enablement_floor(self):
        import datetime

        ns_id = _create_namespace()
        before = datetime.datetime.now(datetime.timezone.utc).isoformat()
        _write(ns_id, "latetrack", V1, track_changes=False)  # v0, untracked
        _write(ns_id, "latetrack", V2, flow_id=2)  # enables first (v1 = floor), then merges (v2)
        table = _table(ns_id, "latetrack")
        assert table.cdc_enabled and table.cdc_enabled_version == 1

        graph = _create_graph(flow_id=3)
        _add_reader(graph, 1, table_name="latetrack", namespace_id=ns_id, cdc_mode="since_timestamp",
                    cdc_from_timestamp=before)
        _run_graph(graph)
        df = graph.get_node(1).get_resulting_data().collect().sort("id")
        # The instant predates v0, but nothing below the floor is read — no error, nothing synthesized.
        assert df["id"].to_list() == [2, 3]
        assert df["_commit_version"].min() == 2

    def test_include_preimage_keeps_the_before_rows(self):
        ns_id = _create_namespace()
        _write(ns_id, "withpre", V1)
        _write(ns_id, "withpre", V2, flow_id=2)

        graph = _create_graph(flow_id=3)
        _add_reader(graph, 1, table_name="withpre", namespace_id=ns_id, cdc_mode="since_version",
                    cdc_from_version=0, cdc_include_preimage=True)
        _run_graph(graph)
        types = graph.get_node(1).get_resulting_data().collect()["_change_type"].to_list()
        assert "update_preimage" in types

    def test_predicted_schema_adds_the_change_columns(self):
        ns_id = _create_namespace()
        _write(ns_id, "predicted", V1)

        graph = _create_graph(flow_id=3)
        _add_reader(graph, 1, table_name="predicted", namespace_id=ns_id, cdc_mode="since_version",
                    cdc_from_version=0)
        names = [c.column_name for c in graph.get_node(1).get_predicted_schema()]
        assert names[-3:] == ["_change_type", "_commit_version", "_commit_timestamp"]
        assert "id" in names and "name" in names
