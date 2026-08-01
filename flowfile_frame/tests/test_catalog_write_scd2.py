"""Verify SCD2 write/read kwargs thread from the frame API into the catalog node settings.

Frame is local-only (no execution_location fixture); see test_catalog_write_partition.py for
the settings-threading style this mirrors.
"""

from __future__ import annotations

import tempfile
from datetime import datetime, timezone

import polars as pl
import pytest

import flowfile_frame as ff
from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import CatalogNamespace, CatalogTable, CatalogTableReadLink, FlowRegistration


def _cleanup_catalog() -> None:
    with get_db_context() as db:
        db.query(CatalogTableReadLink).delete()
        db.query(CatalogTable).delete()
        db.query(FlowRegistration).delete()
        db.query(CatalogNamespace).delete()
        db.commit()


@pytest.fixture(autouse=True)
def _clean_catalog():
    _cleanup_catalog()
    yield
    _cleanup_catalog()


def _seed_namespace() -> int:
    with get_db_context() as db:
        service = CatalogService(SQLAlchemyCatalogRepository(db))
        cat = service.create_namespace(name="Scd2FrameCat", owner_id=1, parent_id=None)
        schema = service.create_namespace(name="Scd2FrameSch", owner_id=1, parent_id=cat.id)
        return schema.id


def _register_table(ns_id: int, name: str) -> int:
    """Register an already-materialized Delta table (no worker round-trip)."""
    tmp_dir = tempfile.mkdtemp()
    delta_path = f"{tmp_dir}/{name}"
    pl.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]}).write_delta(delta_path)
    with get_db_context() as db:
        service = CatalogService(SQLAlchemyCatalogRepository(db))
        table_out = service.register_table_from_data(
            name=name,
            table_path=delta_path,
            owner_id=1,
            namespace_id=ns_id,
            storage_format="delta",
        )
    return table_out.id


class TestWriteCatalogTableScd2Kwargs:
    def test_scd2_kwargs_thread_into_settings(self):
        df = ff.from_dict({"id": [1, 2], "name": ["a", "b"], "city": ["x", "y"]})
        child = df.write_catalog_table(
            "scd2_table",
            namespace_id=None,
            write_mode="scd2",
            merge_keys=["id"],
            scd2_compare_columns=["name", "city"],
            scd2_full_snapshot=True,
            scd2_surrogate_key_column="row_key",
            scd2_valid_from_column="effective_from",
            scd2_valid_to_column="effective_to",
            scd2_is_current_column="current_flag",
        )
        settings = df.flow_graph.get_node(child.node_id).setting_input.catalog_write_settings
        assert settings.write_mode == "scd2"
        assert settings.merge_keys == ["id"]
        assert settings.scd2 is not None
        assert settings.scd2.compare_columns == ["name", "city"]
        assert settings.scd2.full_snapshot is True
        assert settings.scd2.surrogate_key_column == "row_key"
        assert settings.scd2.valid_from_column == "effective_from"
        assert settings.scd2.valid_to_column == "effective_to"
        assert settings.scd2.is_current_column == "current_flag"

    def test_partition_on_current_threads_and_defaults_to_true(self):
        df = ff.from_dict({"id": [1, 2], "name": ["a", "b"]})
        child = df.write_catalog_table(
            "scd2_partition_toggle",
            namespace_id=None,
            write_mode="scd2",
            merge_keys=["id"],
            scd2_partition_on_current=False,
        )
        settings = df.flow_graph.get_node(child.node_id).setting_input.catalog_write_settings
        assert settings.scd2.partition_on_current is False

        df2 = ff.from_dict({"id": [1, 2], "name": ["a", "b"]})
        child2 = df2.write_catalog_table(
            "scd2_partition_default", namespace_id=None, write_mode="scd2", merge_keys=["id"]
        )
        settings2 = df2.flow_graph.get_node(child2.node_id).setting_input.catalog_write_settings
        assert settings2.scd2.partition_on_current is True

        with pytest.raises(ValueError, match="scd2_partition_on_current"):
            df2.write_catalog_table(
                "not_scd2",
                namespace_id=None,
                write_mode="overwrite",
                scd2_partition_on_current=False,
            )

    def test_scd2_block_absent_when_write_mode_is_not_scd2(self):
        """add_write_to_catalog is the sole Scd2Settings assembler: no scd2 kwargs, no block."""
        df = ff.from_dict({"id": [1, 2]})
        child = df.write_catalog_table(
            "not_scd2_table",
            namespace_id=None,
            write_mode="overwrite",
        )
        settings = df.flow_graph.get_node(child.node_id).setting_input.catalog_write_settings
        assert settings.write_mode == "overwrite"
        assert settings.scd2 is None

    def test_scd2_kwargs_with_other_write_mode_raise(self):
        """Non-default scd2 kwargs on a non-scd2 mode are a mistake, not a no-op."""
        df = ff.from_dict({"id": [1, 2]})
        with pytest.raises(ValueError, match="only apply when write_mode='scd2'"):
            df.write_catalog_table(
                "not_scd2_table_loud",
                namespace_id=None,
                write_mode="overwrite",
                scd2_compare_columns=["id"],
            )
        with pytest.raises(ValueError, match="scd2_full_snapshot"):
            df.write_catalog_table(
                "not_scd2_table_loud",
                namespace_id=None,
                write_mode="upsert",
                merge_keys=["id"],
                scd2_full_snapshot=True,
            )

    def test_scd2_defaults_produce_default_scd2_settings(self):
        df = ff.from_dict({"id": [1, 2], "name": ["a", "b"]})
        child = df.write_catalog_table(
            "scd2_defaults_table",
            namespace_id=None,
            write_mode="scd2",
            merge_keys=["id"],
        )
        settings = df.flow_graph.get_node(child.node_id).setting_input.catalog_write_settings
        assert settings.scd2.compare_columns == []
        assert settings.scd2.full_snapshot is False
        assert settings.scd2.surrogate_key_column == "sk"
        assert settings.scd2.valid_from_column == "valid_from"
        assert settings.scd2.valid_to_column == "valid_to"
        assert settings.scd2.is_current_column == "is_current"


class TestNamespaceFullNameThreading:
    def test_namespace_full_name_threads_through_write_catalog_table(self):
        df = ff.from_dict({"id": [1, 2]})
        child = df.write_catalog_table(
            "ns_full_name_table",
            namespace_id=None,
            namespace_full_name="Scd2FrameCat.Scd2FrameSch",
        )
        settings = df.flow_graph.get_node(child.node_id).setting_input.catalog_write_settings
        assert settings.namespace_full_name == "Scd2FrameCat.Scd2FrameSch"

    def test_namespace_full_name_default_is_none(self):
        df = ff.from_dict({"id": [1, 2]})
        child = df.write_catalog_table("ns_full_name_default_table", namespace_id=None)
        settings = df.flow_graph.get_node(child.node_id).setting_input.catalog_write_settings
        assert settings.namespace_full_name is None


class TestReadCatalogTableScd2Kwargs:
    def test_scd2_view_and_as_of_string_thread_into_settings(self):
        ns_id = _seed_namespace()
        _register_table(ns_id, "read_scd2_table")

        frame = ff.read_catalog_table(
            "read_scd2_table",
            namespace_id=ns_id,
            scd2_view="active_at",
            scd2_as_of="2024-01-01T00:00:00+00:00",
        )
        settings = frame.flow_graph.get_node(frame.node_id).setting_input
        assert settings.scd2_view == "active_at"
        assert settings.scd2_as_of == "2024-01-01T00:00:00+00:00"

    def test_scd2_as_of_datetime_is_converted_to_isoformat(self):
        ns_id = _seed_namespace()
        _register_table(ns_id, "read_scd2_table_dt")
        as_of = datetime(2024, 6, 15, 12, 30, 0, tzinfo=timezone.utc)

        frame = ff.read_catalog_table(
            "read_scd2_table_dt",
            namespace_id=ns_id,
            scd2_view="active_at",
            scd2_as_of=as_of,
        )
        settings = frame.flow_graph.get_node(frame.node_id).setting_input
        assert settings.scd2_as_of == as_of.isoformat()

    def test_scd2_view_defaults_to_none(self):
        ns_id = _seed_namespace()
        _register_table(ns_id, "read_scd2_table_default")

        frame = ff.read_catalog_table("read_scd2_table_default", namespace_id=ns_id)
        settings = frame.flow_graph.get_node(frame.node_id).setting_input
        assert settings.scd2_view is None
        assert settings.scd2_as_of is None
