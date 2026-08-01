"""Tests for Delta Lake–related additions to the Catalog system.

Covers:
- format_delta_timestamp / _parse_delta_history helpers
- CatalogService.register_table_from_data (with pre-computed metadata)
- CatalogService.overwrite_table_data (delta-aware, with pre-computed metadata)
- CatalogService.resolve_write_destination
- CatalogService.resolve_table_file_path
- CatalogService.get_table_history (non-delta fallback)
- CatalogService.get_table_preview (delta + version)
- CatalogMaterializationResult.storage_format field
- API: GET /catalog/tables/{id}/history
- API: GET /catalog/tables/{id}/preview?version=...
- Worker metadata offload (trigger_catalog_materialize with delta response)
"""

import json
import os
from datetime import datetime, timezone

import polars as pl
import pytest
from deltalake import DeltaTable
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.catalog import CatalogService, TableExistsError, TableVersionUnavailableError
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.catalog.service import _parse_delta_history
from flowfile_core.catalog.storage_backend import CatalogStorageTarget
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import (
    CatalogNamespace,
    CatalogTable,
    CatalogTableReadLink,
    FlowFavorite,
    FlowFollow,
    FlowRegistration,
    FlowRun,
    FlowSchedule,
    ScheduleTriggerTable,
    TableFavorite,
)
from flowfile_core.schemas.catalog_schema import DeltaTableHistory, DeltaVersionCommit
from shared.delta_utils import format_delta_timestamp

# Helpers


def _get_auth_token() -> str:
    with TestClient(main.app) as client:
        response = client.post("/auth/token")
        return response.json()["access_token"]


@pytest.fixture(scope="module")
def client() -> TestClient:
    token = _get_auth_token()
    c = TestClient(main.app)
    c.headers = {"Authorization": f"Bearer {token}"}
    return c


def _cleanup_catalog():
    with get_db_context() as db:
        db.query(ScheduleTriggerTable).delete()
        db.query(FlowSchedule).delete()
        db.query(TableFavorite).delete()
        db.query(CatalogTableReadLink).delete()
        db.query(CatalogTable).delete()
        db.query(FlowFollow).delete()
        db.query(FlowFavorite).delete()
        db.query(FlowRun).delete()
        db.query(FlowRegistration).delete()
        db.query(CatalogNamespace).delete()
        db.commit()


@pytest.fixture(autouse=True)
def clean_catalog():
    _cleanup_catalog()
    yield
    _cleanup_catalog()


def _make_namespace() -> tuple[int, int]:
    """Create a catalog + schema and return (catalog_id, schema_id)."""
    with get_db_context() as db:
        cat = CatalogNamespace(name="DeltaCat", level=0, owner_id=1)
        db.add(cat)
        db.commit()
        db.refresh(cat)
        schema = CatalogNamespace(name="DeltaSch", level=1, parent_id=cat.id, owner_id=1)
        db.add(schema)
        db.commit()
        db.refresh(schema)
        return cat.id, schema.id


# format_delta_timestamp


class TestFormatDeltaTimestamp:
    def test_none_returns_none(self):
        assert format_delta_timestamp(None) is None

    def test_string_passthrough(self):
        assert format_delta_timestamp("2024-01-01T00:00:00") == "2024-01-01T00:00:00"

    def test_datetime_to_iso(self):
        dt = datetime(2024, 6, 15, 12, 30, 0, tzinfo=timezone.utc)
        result = format_delta_timestamp(dt)
        assert "2024-06-15" in result
        assert "12:30" in result

    def test_epoch_millis_int(self):
        # 1700000000000 ms = 2023-11-14T22:13:20 UTC
        result = format_delta_timestamp(1700000000000)
        assert result is not None
        assert "2023-11-14" in result

    def test_epoch_millis_float(self):
        result = format_delta_timestamp(1700000000000.0)
        assert result is not None
        assert "2023" in result

    def test_other_type_stringified(self):
        result = format_delta_timestamp(42)
        # 42 ms since epoch — still returns something
        assert result is not None


# _parse_delta_history


class TestParseDeltaHistory:
    def test_parses_history_list(self):
        raw = [
            {"version": 0, "timestamp": "2024-01-01T00:00:00", "operation": "WRITE", "operationParameters": {"mode": "Overwrite"}},
            {"version": 1, "timestamp": 1700000000000, "operation": "WRITE", "operationParameters": None},
        ]
        result = _parse_delta_history(raw)
        assert len(result) == 2
        assert isinstance(result[0], DeltaVersionCommit)
        assert result[0].version == 0
        assert result[0].operation == "WRITE"
        assert result[0].parameters == {"mode": "Overwrite"}
        assert result[1].version == 1
        assert result[1].parameters is None

    def test_empty_list(self):
        assert _parse_delta_history([]) == []

    def test_missing_keys(self):
        """Handles dicts with missing keys gracefully."""
        raw = [{"version": 5}]
        result = _parse_delta_history(raw)
        assert result[0].version == 5
        assert result[0].timestamp is None
        assert result[0].operation is None


# CatalogMaterializationResult


class TestCatalogMaterializationResult:
    def test_default_storage_format_is_delta(self):
        result = CatalogService.CatalogMaterializationResult(
            table_path="/tmp/t",
            schema=[],
            row_count=0,
            column_count=0,
            size_bytes=0,
        )
        assert result.storage_format == "delta"

    def test_custom_storage_format(self):
        result = CatalogService.CatalogMaterializationResult(
            table_path="/tmp/t",
            schema=[],
            row_count=0,
            column_count=0,
            size_bytes=0,
            storage_format="parquet",
        )
        assert result.storage_format == "parquet"


# register_table_from_data (with pre-computed metadata)


class TestRegisterTableFromData:
    def test_registers_with_precomputed_metadata(self, tmp_path):
        """When schema/row_count/size_bytes are provided, no file read occurs."""
        _, schema_id = _make_namespace()
        table_path = str(tmp_path / "my_delta")

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            table_out = svc.register_table_from_data(
                name="meta_table",
                table_path=table_path,
                owner_id=1,
                namespace_id=schema_id,
                storage_format="delta",
                schema=[{"name": "a", "dtype": "Int64"}, {"name": "b", "dtype": "String"}],
                row_count=100,
                column_count=2,
                size_bytes=4096,
            )

        assert table_out.name == "meta_table"
        assert table_out.row_count == 100
        assert table_out.column_count == 2
        assert table_out.size_bytes == 4096
        assert len(table_out.schema_columns) == 2
        assert table_out.schema_columns[0].name == "a"

    def test_raises_table_exists(self, tmp_path):
        _, schema_id = _make_namespace()
        table_path = str(tmp_path / "t1")

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            svc.register_table_from_data(
                name="dup_table",
                table_path=table_path,
                owner_id=1,
                namespace_id=schema_id,
                storage_format="delta",
                schema=[{"name": "x", "dtype": "Int64"}],
                row_count=1,
                column_count=1,
                size_bytes=100,
            )
            with pytest.raises(TableExistsError):
                svc.register_table_from_data(
                    name="dup_table",
                    table_path=table_path,
                    owner_id=1,
                    namespace_id=schema_id,
                    storage_format="delta",
                    schema=[{"name": "x", "dtype": "Int64"}],
                    row_count=1,
                    column_count=1,
                    size_bytes=100,
                )

    def test_infers_column_count(self, tmp_path):
        """If column_count is None but schema is provided, it should be inferred."""
        _, schema_id = _make_namespace()

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            table_out = svc.register_table_from_data(
                name="infer_cols",
                table_path=str(tmp_path / "t2"),
                owner_id=1,
                namespace_id=schema_id,
                storage_format="delta",
                schema=[{"name": "a", "dtype": "Int64"}, {"name": "b", "dtype": "Float64"}, {"name": "c", "dtype": "String"}],
                row_count=50,
                column_count=None,
                size_bytes=2000,
            )
        assert table_out.column_count == 3


# register_table_from_parquet (backward compat alias)


class TestRegisterTableFromParquetAlias:
    def test_delegates_to_register_table_from_data(self, tmp_path, monkeypatch):
        """register_table_from_parquet should call register_table_from_data with storage_format='parquet'."""
        _, schema_id = _make_namespace()

        calls = []

        def fake_register_from_data(self_, **kwargs):
            calls.append(kwargs)
            return type("FakeOut", (), {
                "name": kwargs["name"], "row_count": 0, "column_count": 0,
                "size_bytes": 0, "schema_columns": [],
            })()

        monkeypatch.setattr(CatalogService, "register_table_from_data", fake_register_from_data)

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            svc.register_table_from_parquet(
                name="compat_table",
                parquet_path="/tmp/compat.parquet",
                owner_id=1,
                namespace_id=schema_id,
            )

        assert len(calls) == 1
        assert calls[0]["storage_format"] == "parquet"
        assert calls[0]["table_path"] == "/tmp/compat.parquet"


# overwrite_table_data (with pre-computed delta metadata)


class TestOverwriteTableDataDelta:
    def test_overwrite_with_precomputed_metadata(self, tmp_path, monkeypatch):
        """When all metadata is provided, overwrite should not read the file."""
        _, schema_id = _make_namespace()

        monkeypatch.setattr(CatalogService, "_fire_table_trigger_schedules", lambda *a, **kw: 0)

        delta_dir = tmp_path / "overwrite_delta"
        delta_dir.mkdir()

        with get_db_context() as db:
            table = CatalogTable(
                name="ow_table",
                namespace_id=schema_id,
                owner_id=1,
                file_path=str(delta_dir),
                storage_format="delta",
                schema_json=json.dumps([{"name": "old_col", "dtype": "Int64"}]),
                row_count=10,
                column_count=1,
                size_bytes=500,
            )
            db.add(table)
            db.commit()
            db.refresh(table)
            table_id = table.id

        new_path = tmp_path / "new_delta"
        new_path.mkdir()

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            result = svc.overwrite_table_data(
                table_id=table_id,
                table_path=str(new_path),
                storage_format="delta",
                schema=[{"name": "new_col", "dtype": "String"}],
                row_count=200,
                column_count=1,
                size_bytes=8000,
            )

        assert result.row_count == 200
        assert result.size_bytes == 8000
        assert result.schema_columns[0].name == "new_col"

    def test_overwrite_updates_storage_format(self, tmp_path, monkeypatch):
        """overwrite_table_data should update storage_format on the DB record."""
        _, schema_id = _make_namespace()
        monkeypatch.setattr(CatalogService, "_fire_table_trigger_schedules", lambda *a, **kw: 0)

        with get_db_context() as db:
            table = CatalogTable(
                name="format_table",
                namespace_id=schema_id,
                owner_id=1,
                file_path="/tmp/old.parquet",
                storage_format="parquet",
            )
            db.add(table)
            db.commit()
            db.refresh(table)
            table_id = table.id

        new_dir = tmp_path / "new_delta"
        new_dir.mkdir()

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            svc.overwrite_table_data(
                table_id=table_id,
                table_path=str(new_dir),
                storage_format="delta",
                schema=[{"name": "x", "dtype": "Int64"}],
                row_count=1,
                column_count=1,
                size_bytes=100,
            )

        with get_db_context() as db:
            updated = db.get(CatalogTable, table_id)
            assert updated.storage_format == "delta"
            assert updated.file_path == str(new_dir)


# resolve_write_destination


class TestResolveWriteDestination:
    def test_new_table_returns_none_and_passthrough_mode(self, tmp_path):
        _, schema_id = _make_namespace()
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            existing, dest_path, delta_mode = svc.resolve_write_destination(
                table_name="brand_new",
                namespace_id=schema_id,
                write_mode="overwrite",
                target=CatalogStorageTarget(is_cloud=False, base=str(tmp_path)),
            )
        assert existing is None
        assert delta_mode == "overwrite"
        assert str(dest_path).startswith(str(tmp_path))

    def test_existing_table_non_overwrite_raises(self, tmp_path):
        _, schema_id = _make_namespace()
        delta_dir = tmp_path / "existing_delta"
        delta_dir.mkdir()
        (delta_dir / "_delta_log").mkdir()

        with get_db_context() as db:
            table = CatalogTable(
                name="exist_table",
                namespace_id=schema_id,
                owner_id=1,
                file_path=str(delta_dir),
                storage_format="delta",
            )
            db.add(table)
            db.commit()

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            with pytest.raises(TableExistsError):
                svc.resolve_write_destination(
                    table_name="exist_table",
                    namespace_id=schema_id,
                    write_mode="error",
                    target=CatalogStorageTarget(is_cloud=False, base=str(tmp_path)),
                )

    def test_existing_delta_table_overwrite_returns_same_path(self, tmp_path):
        _, schema_id = _make_namespace()
        delta_dir = tmp_path / "delta_ow"
        delta_dir.mkdir()
        (delta_dir / "_delta_log").mkdir()

        with get_db_context() as db:
            table = CatalogTable(
                name="delta_ow_table",
                namespace_id=schema_id,
                owner_id=1,
                file_path=str(delta_dir),
                storage_format="delta",
            )
            db.add(table)
            db.commit()

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            existing, dest_path, delta_mode = svc.resolve_write_destination(
                table_name="delta_ow_table",
                namespace_id=schema_id,
                write_mode="overwrite",
                target=CatalogStorageTarget(is_cloud=False, base=str(tmp_path)),
            )
        assert existing is not None
        assert dest_path == str(delta_dir)
        assert delta_mode == "overwrite"

    def test_existing_legacy_parquet_overwrite(self, tmp_path):
        """Legacy parquet: resolve returns new dir path but does NOT delete old file.

        Deletion is deferred to ``overwrite_table_data`` so that the old data
        is preserved if the subsequent write fails.
        """
        _, schema_id = _make_namespace()
        pq_file = tmp_path / "legacy.parquet"
        pl.DataFrame({"a": [1]}).write_parquet(pq_file)

        with get_db_context() as db:
            table = CatalogTable(
                name="legacy_table",
                namespace_id=schema_id,
                owner_id=1,
                file_path=str(pq_file),
                storage_format="parquet",
            )
            db.add(table)
            db.commit()

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            existing, dest_path, delta_mode = svc.resolve_write_destination(
                table_name="legacy_table",
                namespace_id=schema_id,
                write_mode="overwrite",
                target=CatalogStorageTarget(is_cloud=False, base=str(tmp_path)),
            )
        assert existing is not None
        assert delta_mode == "overwrite"
        # Old parquet must still exist (deleted only after new write succeeds)
        assert pq_file.exists()
        # New path should be a directory at the same stem
        assert dest_path == str(tmp_path / "legacy")

    def test_existing_delta_table_append(self, tmp_path):
        _, schema_id = _make_namespace()
        delta_dir = tmp_path / "delta_append"
        delta_dir.mkdir()
        (delta_dir / "_delta_log").mkdir()

        with get_db_context() as db:
            table = CatalogTable(
                name="delta_append_table",
                namespace_id=schema_id,
                owner_id=1,
                file_path=str(delta_dir),
                storage_format="delta",
            )
            db.add(table)
            db.commit()

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            existing, dest_path, delta_mode = svc.resolve_write_destination(
                table_name="delta_append_table",
                namespace_id=schema_id,
                write_mode="append",
                target=CatalogStorageTarget(is_cloud=False, base=str(tmp_path)),
            )
        assert existing is not None
        assert dest_path == str(delta_dir)
        assert delta_mode == "append"

    def test_existing_delta_table_upsert(self, tmp_path):
        _, schema_id = _make_namespace()
        delta_dir = tmp_path / "delta_upsert"
        delta_dir.mkdir()
        (delta_dir / "_delta_log").mkdir()

        with get_db_context() as db:
            table = CatalogTable(
                name="delta_upsert_table",
                namespace_id=schema_id,
                owner_id=1,
                file_path=str(delta_dir),
                storage_format="delta",
            )
            db.add(table)
            db.commit()

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            existing, dest_path, delta_mode = svc.resolve_write_destination(
                table_name="delta_upsert_table",
                namespace_id=schema_id,
                write_mode="upsert",
                target=CatalogStorageTarget(is_cloud=False, base=str(tmp_path)),
            )
        assert existing is not None
        assert dest_path == str(delta_dir)
        assert delta_mode == "upsert"

    def test_scd2_mode_echoes_through_unchanged(self, tmp_path):
        """``resolve_write_destination`` is mode-agnostic: "scd2" must reach the dispatch verbatim."""
        _, schema_id = _make_namespace()
        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            existing, dest_path, delta_mode = svc.resolve_write_destination(
                table_name="new_scd2",
                namespace_id=schema_id,
                write_mode="scd2",
                target=CatalogStorageTarget(is_cloud=False, base=str(tmp_path)),
            )
        assert existing is None
        assert delta_mode == "scd2"
        assert str(dest_path).startswith(str(tmp_path))

    def test_existing_table_scd2_mode_reuses_path(self, tmp_path):
        _, schema_id = _make_namespace()
        delta_dir = tmp_path / "scd2_existing"
        delta_dir.mkdir()
        (delta_dir / "_delta_log").mkdir()
        with get_db_context() as db:
            db.add(
                CatalogTable(
                    name="scd2_existing_tbl",
                    namespace_id=schema_id,
                    owner_id=1,
                    file_path=str(delta_dir),
                    storage_format="delta",
                )
            )
            db.commit()

        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            existing, dest_path, delta_mode = svc.resolve_write_destination(
                table_name="scd2_existing_tbl",
                namespace_id=schema_id,
                write_mode="scd2",
                target=CatalogStorageTarget(is_cloud=False, base=str(tmp_path)),
            )
        assert existing is not None
        assert dest_path == str(delta_dir)
        assert delta_mode == "scd2"

    def test_new_table_with_merge_mode(self, tmp_path):
        _, schema_id = _make_namespace()
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            existing, dest_path, delta_mode = svc.resolve_write_destination(
                table_name="new_upsert",
                namespace_id=schema_id,
                write_mode="upsert",
                target=CatalogStorageTarget(is_cloud=False, base=str(tmp_path)),
            )
        assert existing is None
        assert delta_mode == "upsert"

    def test_existing_local_table_stays_local_under_cloud_target(self, tmp_path):
        """Forward-only: overwriting an existing LOCAL table keeps it local even when the
        global config now points at object storage — the table is not relocated."""
        _, schema_id = _make_namespace()
        delta_dir = tmp_path / "stays_local"
        delta_dir.mkdir()
        (delta_dir / "_delta_log").mkdir()
        with get_db_context() as db:
            db.add(
                CatalogTable(
                    name="stays_local_tbl",
                    namespace_id=schema_id,
                    owner_id=1,
                    file_path=str(delta_dir),
                    storage_format="delta",
                )
            )
            db.commit()

        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            existing, dest_path, delta_mode = svc.resolve_write_destination(
                table_name="stays_local_tbl",
                namespace_id=schema_id,
                write_mode="overwrite",
                target=CatalogStorageTarget(is_cloud=True, base="s3://bucket/catalog"),
            )
        assert existing is not None
        assert dest_path == str(delta_dir)  # local, not relocated to s3://

    def test_existing_cloud_table_uri_preserved_under_local_target(self):
        """An existing object-storage table reuses its s3:// URI verbatim (no Path() mangling)
        even when the global config is local/unset."""
        _, schema_id = _make_namespace()
        uri = "s3://bucket/catalog/cloud_tbl_abc123"
        with get_db_context() as db:
            db.add(
                CatalogTable(
                    name="cloud_tbl",
                    namespace_id=schema_id,
                    owner_id=1,
                    file_path=uri,
                    storage_format="delta",
                )
            )
            db.commit()

        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            existing, dest_path, delta_mode = svc.resolve_write_destination(
                table_name="cloud_tbl",
                namespace_id=schema_id,
                write_mode="overwrite",
                target=CatalogStorageTarget(is_cloud=False, base="/tmp/local_catalog"),
            )
        assert existing is not None
        assert dest_path == uri  # URI preserved, not corrupted to "s3:/bucket/..."


# resolve_table_file_path


class TestResolveTableFilePath:
    def test_by_id(self):
        _, schema_id = _make_namespace()
        with get_db_context() as db:
            table = CatalogTable(
                name="resolve_t",
                namespace_id=schema_id,
                owner_id=1,
                file_path="/data/resolve_t",
                storage_format="delta",
            )
            db.add(table)
            db.commit()
            db.refresh(table)

            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            path = svc.resolve_table_file_path(table_id=table.id)
        assert path == "/data/resolve_t"

    def test_by_name(self):
        _, schema_id = _make_namespace()
        with get_db_context() as db:
            table = CatalogTable(
                name="named_t",
                namespace_id=schema_id,
                owner_id=1,
                file_path="/data/named_t",
                storage_format="delta",
            )
            db.add(table)
            db.commit()

            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            path = svc.resolve_table_file_path(table_name="named_t", namespace_id=schema_id)
        assert path == "/data/named_t"

    def test_returns_none_for_missing(self):
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            assert svc.resolve_table_file_path(table_id=999999) is None
            assert svc.resolve_table_file_path(table_name="no_such_table") is None


# get_table_history (non-delta fallback)


class TestGetTableHistory:
    def test_non_delta_returns_empty_history(self, tmp_path):
        """For a non-delta table, get_table_history should return empty history."""
        _, schema_id = _make_namespace()
        pq = tmp_path / "hist.parquet"
        pl.DataFrame({"a": [1]}).write_parquet(pq)

        with get_db_context() as db:
            table = CatalogTable(
                name="hist_table",
                namespace_id=schema_id,
                owner_id=1,
                file_path=str(pq),
                storage_format="parquet",
            )
            db.add(table)
            db.commit()
            db.refresh(table)

            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            history = svc.get_table_history(table.id)
        assert isinstance(history, DeltaTableHistory)
        assert history.current_version == 0
        assert history.history == []

    def test_delta_table_returns_history(self, tmp_path):
        """For a real delta table, should return at least one version."""
        _, schema_id = _make_namespace()
        delta_dir = tmp_path / "hist_delta"
        pl.DataFrame({"x": [1, 2, 3]}).write_delta(str(delta_dir), mode="error")
        with get_db_context() as db:
            table = CatalogTable(
                name="delta_hist",
                namespace_id=schema_id,
                owner_id=1,
                file_path=str(delta_dir),
                storage_format="delta",
            )
            db.add(table)
            db.commit()
            db.refresh(table)

            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)

            # Disable worker offload so it reads locally
            import flowfile_core.configs.settings
            from flowfile_core.configs.utils import MutableBool
            orig = getattr(flowfile_core.configs.settings, "OFFLOAD_TO_WORKER", False)
            flowfile_core.configs.settings.OFFLOAD_TO_WORKER = MutableBool(False)

            try:
                history = svc.get_table_history(table.id)
            finally:
                flowfile_core.configs.settings.OFFLOAD_TO_WORKER = orig

        assert history.current_version >= 0
        assert len(history.history) >= 1
        assert history.history[0].version == 0

    def test_not_found_raises(self):
        from flowfile_core.catalog.exceptions import TableNotFoundError

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            with pytest.raises(TableNotFoundError):
                svc.get_table_history(999999)


# get_table_preview (delta + version)


class TestGetTablePreviewDelta:
    def test_preview_delta_table(self, tmp_path):
        _, schema_id = _make_namespace()
        delta_dir = tmp_path / "prev_delta"
        pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]}).write_delta(str(delta_dir))
        with get_db_context() as db:
            table = CatalogTable(
                name="prev_table",
                namespace_id=schema_id,
                owner_id=1,
                file_path=str(delta_dir),
                storage_format="delta",
                row_count=3,
            )
            db.add(table)
            db.commit()
            db.refresh(table)

            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)

            preview = svc.get_table_preview(table.id, limit=100)

        assert preview.columns == ["a", "b"]
        assert len(preview.rows) == 3
        assert preview.total_rows == 3

    def test_preview_nonexistent_data_returns_empty(self, tmp_path):
        _, schema_id = _make_namespace()
        with get_db_context() as db:
            table = CatalogTable(
                name="ghost_table",
                namespace_id=schema_id,
                owner_id=1,
                file_path=str(tmp_path / "gone"),
                storage_format="delta",
            )
            db.add(table)
            db.commit()
            db.refresh(table)

            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            preview = svc.get_table_preview(table.id)

        assert preview.columns == []
        assert preview.rows == []

    def test_preview_at_version(self, tmp_path):
        """Preview at a specific delta version should work."""
        _, schema_id = _make_namespace()
        delta_dir = tmp_path / "versioned"
        pl.DataFrame({"v": [1]}).write_delta(str(delta_dir), mode="error")
        pl.DataFrame({"v": [1, 2]}).write_delta(str(delta_dir), mode="overwrite")

        with get_db_context() as db:
            table = CatalogTable(
                name="ver_table",
                namespace_id=schema_id,
                owner_id=1,
                file_path=str(delta_dir),
                storage_format="delta",
                row_count=2,
            )
            db.add(table)
            db.commit()
            db.refresh(table)

            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)

            import flowfile_core.configs.settings
            from flowfile_core.configs.utils import MutableBool

            orig = getattr(flowfile_core.configs.settings, "OFFLOAD_TO_WORKER", False)
            flowfile_core.configs.settings.OFFLOAD_TO_WORKER = MutableBool(False)
            try:
                preview_v0 = svc.get_table_preview(table.id, version=0)
                preview_v1 = svc.get_table_preview(table.id, version=1)
            finally:
                flowfile_core.configs.settings.OFFLOAD_TO_WORKER = orig

        assert len(preview_v0.rows) == 1
        assert len(preview_v1.rows) == 2

    def test_preview_at_out_of_range_version_raises(self, tmp_path):
        """A version beyond the table's history should raise TableVersionUnavailableError."""
        _, schema_id = _make_namespace()
        delta_dir = tmp_path / "oor_version"
        pl.DataFrame({"v": [1]}).write_delta(str(delta_dir))

        with get_db_context() as db:
            table = CatalogTable(
                name="oor_table",
                namespace_id=schema_id,
                owner_id=1,
                file_path=str(delta_dir),
                storage_format="delta",
                row_count=1,
            )
            db.add(table)
            db.commit()
            db.refresh(table)

            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)

            import flowfile_core.configs.settings
            from flowfile_core.configs.utils import MutableBool

            orig = getattr(flowfile_core.configs.settings, "OFFLOAD_TO_WORKER", False)
            flowfile_core.configs.settings.OFFLOAD_TO_WORKER = MutableBool(False)
            try:
                with pytest.raises(TableVersionUnavailableError):
                    svc.get_table_preview(table.id, version=99)
            finally:
                flowfile_core.configs.settings.OFFLOAD_TO_WORKER = orig

    def test_preview_at_vacuumed_version_raises(self, tmp_path):
        """A version whose data files were vacuumed away should raise TableVersionUnavailableError."""
        _, schema_id = _make_namespace()
        delta_dir = tmp_path / "vacuumed_version"
        pl.DataFrame({"v": [1]}).write_delta(str(delta_dir), mode="error")
        pl.DataFrame({"v": [1, 2]}).write_delta(str(delta_dir), mode="overwrite")

        # Simulate a vacuum: physically delete version 0's data files (log entry stays).
        for uri in DeltaTable(str(delta_dir), version=0).file_uris():
            path = uri.replace("file://", "")
            if os.path.exists(path):
                os.remove(path)

        with get_db_context() as db:
            table = CatalogTable(
                name="vacuumed_table",
                namespace_id=schema_id,
                owner_id=1,
                file_path=str(delta_dir),
                storage_format="delta",
                row_count=2,
            )
            db.add(table)
            db.commit()
            db.refresh(table)

            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)

            import flowfile_core.configs.settings
            from flowfile_core.configs.utils import MutableBool

            orig = getattr(flowfile_core.configs.settings, "OFFLOAD_TO_WORKER", False)
            flowfile_core.configs.settings.OFFLOAD_TO_WORKER = MutableBool(False)
            try:
                # Latest version still reads fine; the vacuumed version does not.
                assert len(svc.get_table_preview(table.id, version=1).rows) == 2
                with pytest.raises(TableVersionUnavailableError):
                    svc.get_table_preview(table.id, version=0)
            finally:
                flowfile_core.configs.settings.OFFLOAD_TO_WORKER = orig


class TestTriggerDeltaVersionPreviewMapping:
    """The worker offload path maps a 404 to TableVersionUnavailableError."""

    def test_worker_404_maps_to_domain_exception(self, monkeypatch):
        from flowfile_core.flowfile.flow_data_engine.subprocess_operations import subprocess_operations as subops

        class _FakeResponse:
            status_code = 404
            ok = False
            text = '{"detail": "Version 99 is no longer available."}'

        monkeypatch.setattr(subops.requests, "post", lambda *a, **kw: _FakeResponse())

        with pytest.raises(TableVersionUnavailableError):
            subops.trigger_delta_version_preview("some_table", version=99, n_rows=100)


# table_exists in _table_to_out (delta-aware)


class TestTableExistsInOutput:
    def test_file_exists_true_for_delta(self, tmp_path):
        _, schema_id = _make_namespace()
        delta_dir = tmp_path / "exists_delta"
        pl.DataFrame({"a": [1]}).write_delta(str(delta_dir))

        with get_db_context() as db:
            table = CatalogTable(
                name="exists_table",
                namespace_id=schema_id,
                owner_id=1,
                file_path=str(delta_dir),
                storage_format="delta",
            )
            db.add(table)
            db.commit()
            db.refresh(table)

            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            out = svc.get_table(table.id)
        assert out.file_exists is True

    def test_file_exists_false_for_missing(self, tmp_path):
        _, schema_id = _make_namespace()
        with get_db_context() as db:
            table = CatalogTable(
                name="missing_table",
                namespace_id=schema_id,
                owner_id=1,
                file_path=str(tmp_path / "nonexistent_dir"),
                storage_format="delta",
            )
            db.add(table)
            db.commit()
            db.refresh(table)

            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            out = svc.get_table(table.id)
        assert out.file_exists is False


# Worker materialization now returns delta format


class TestMaterializeWorkerDeltaResponse:
    def test_register_table_uses_delta_response(self, monkeypatch):
        """trigger_catalog_materialize returning delta fields should work."""
        _, schema_id = _make_namespace()

        response_payload = {
            "table_path": "/tmp/delta_out",
            "column_schema": [{"name": "col_x", "dtype": "Float64"}],
            "row_count": 42,
            "column_count": 1,
            "size_bytes": 9999,
        }

        class FakeResponse:
            ok = True
            status_code = 200
            text = ""

            def json(self):
                return response_payload

        monkeypatch.setattr(
            "flowfile_core.catalog.service.trigger_catalog_materialize",
            lambda *args, **kwargs: FakeResponse(),
        )

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            table_out = svc.register_table(
                name="delta_worker_table",
                file_path="/tmp/source.csv",
                owner_id=1,
                namespace_id=schema_id,
            )

        assert table_out.row_count == 42
        assert table_out.size_bytes == 9999
        assert table_out.schema_columns[0].name == "col_x"


# API: GET /catalog/tables/{id}/history


class TestHistoryEndpoint:
    def test_history_404_for_missing_table(self, client):
        resp = client.get("/catalog/tables/999999/history")
        assert resp.status_code == 404

    def test_history_empty_for_parquet(self, client, tmp_path):
        _, schema_id = _make_namespace()
        pq = tmp_path / "api_hist.parquet"
        pl.DataFrame({"a": [1]}).write_parquet(pq)

        with get_db_context() as db:
            table = CatalogTable(
                name="api_hist_table",
                namespace_id=schema_id,
                owner_id=1,
                file_path=str(pq),
                storage_format="parquet",
            )
            db.add(table)
            db.commit()
            db.refresh(table)
            table_id = table.id

        resp = client.get(f"/catalog/tables/{table_id}/history")
        assert resp.status_code == 200
        data = resp.json()
        assert data["current_version"] == 0
        assert data["history"] == []


# API: GET /catalog/tables/{id}/preview?version=...


class TestPreviewVersionEndpoint:
    def test_preview_404_for_missing_table(self, client):
        resp = client.get("/catalog/tables/999999/preview")
        assert resp.status_code == 404

    def test_preview_delta_table_via_api(self, client, tmp_path):
        _, schema_id = _make_namespace()
        delta_dir = tmp_path / "api_delta"
        pl.DataFrame({"val": [10, 20, 30]}).write_delta(str(delta_dir))

        with get_db_context() as db:
            table = CatalogTable(
                name="api_prev_table",
                namespace_id=schema_id,
                owner_id=1,
                file_path=str(delta_dir),
                storage_format="delta",
                row_count=3,
            )
            db.add(table)
            db.commit()
            db.refresh(table)
            table_id = table.id

        resp = client.get(f"/catalog/tables/{table_id}/preview", params={"limit": 100})
        assert resp.status_code == 200
        data = resp.json()
        assert data["columns"] == ["val"]
        assert len(data["rows"]) == 3

    def test_preview_missing_version_returns_410(self, client, tmp_path):
        """Previewing a version that no longer exists returns 410 with a clear message."""
        _, schema_id = _make_namespace()
        delta_dir = tmp_path / "api_missing_version"
        pl.DataFrame({"val": [1, 2, 3]}).write_delta(str(delta_dir))

        with get_db_context() as db:
            table = CatalogTable(
                name="api_missing_ver_table",
                namespace_id=schema_id,
                owner_id=1,
                file_path=str(delta_dir),
                storage_format="delta",
                row_count=3,
            )
            db.add(table)
            db.commit()
            db.refresh(table)
            table_id = table.id

        import flowfile_core.configs.settings
        from flowfile_core.configs.utils import MutableBool

        orig = getattr(flowfile_core.configs.settings, "OFFLOAD_TO_WORKER", False)
        flowfile_core.configs.settings.OFFLOAD_TO_WORKER = MutableBool(False)
        try:
            resp = client.get(f"/catalog/tables/{table_id}/preview", params={"version": 99})
        finally:
            flowfile_core.configs.settings.OFFLOAD_TO_WORKER = orig

        assert resp.status_code == 410
        assert "no longer available" in resp.json()["detail"].lower()


# DB migration: storage_format column


class TestStorageFormatColumn:
    def test_catalog_table_has_storage_format(self):
        """Verify the CatalogTable model has the storage_format column."""
        _, schema_id = _make_namespace()
        with get_db_context() as db:
            table = CatalogTable(
                name="fmt_test",
                namespace_id=schema_id,
                owner_id=1,
                file_path="/tmp/fmt_test",
                storage_format="delta",
            )
            db.add(table)
            db.commit()
            db.refresh(table)
            assert table.storage_format == "delta"

    def test_default_storage_format_is_delta(self):
        _, schema_id = _make_namespace()
        with get_db_context() as db:
            table = CatalogTable(
                name="default_fmt",
                namespace_id=schema_id,
                owner_id=1,
                file_path="/tmp/default_fmt",
            )
            db.add(table)
            db.commit()
            db.refresh(table)
            assert table.storage_format == "delta"


# partition_columns persistence


class TestPartitionColumnsPersistence:
    def test_register_persists_partition_columns(self, tmp_path):
        _, schema_id = _make_namespace()
        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            out = svc.register_table_from_data(
                name="part_table",
                table_path=str(tmp_path / "pt"),
                owner_id=1,
                namespace_id=schema_id,
                storage_format="delta",
                schema=[{"name": "a", "dtype": "Int64"}, {"name": "b", "dtype": "String"}],
                row_count=3,
                column_count=2,
                size_bytes=512,
                partition_columns=["b"],
            )
            table_id = out.id
        assert out.partition_columns == ["b"]

        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            refetched = svc.get_table(table_id)
        assert refetched.partition_columns == ["b"]

    def test_no_partition_columns_is_none(self, tmp_path):
        _, schema_id = _make_namespace()
        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            out = svc.register_table_from_data(
                name="unpart_table",
                table_path=str(tmp_path / "up"),
                owner_id=1,
                namespace_id=schema_id,
                storage_format="delta",
                schema=[{"name": "a", "dtype": "Int64"}],
                row_count=1,
                column_count=1,
                size_bytes=100,
            )
        assert out.partition_columns is None


# SCD2 config persistence


_SCD2_CONFIG = {
    "business_keys": ["cust_id"],
    "surrogate_key_column": "sk",
    "valid_from_column": "valid_from",
    "valid_to_column": "valid_to",
    "is_current_column": "is_current",
    "compare_columns": ["city", "tier"],
    "full_snapshot": True,
}

# A schema that still carries every generated column of _SCD2_CONFIG.
_SCD2_SHAPED_SCHEMA = [
    {"name": "cust_id", "dtype": "Int64"},
    {"name": "city", "dtype": "String"},
    {"name": "sk", "dtype": "String"},
    {"name": "valid_from", "dtype": "Datetime"},
    {"name": "valid_to", "dtype": "Datetime"},
    {"name": "is_current", "dtype": "Boolean"},
]


class TestScd2ConfigPersistence:
    @staticmethod
    def _register(svc, schema_id, tmp_path, name, scd2_config):
        return svc.register_table_from_data(
            name=name,
            table_path=str(tmp_path / name),
            owner_id=1,
            namespace_id=schema_id,
            storage_format="delta",
            schema=[{"name": "cust_id", "dtype": "Int64"}, {"name": "city", "dtype": "String"}],
            row_count=3,
            column_count=2,
            size_bytes=512,
            scd2_config=scd2_config,
        )

    def test_register_persists_scd2_config(self, tmp_path):
        _, schema_id = _make_namespace()
        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            out = self._register(svc, schema_id, tmp_path, "scd2_dim", _SCD2_CONFIG)
            table_id = out.id

        assert out.scd2 is not None
        assert out.scd2.business_keys == ["cust_id"]
        assert out.scd2.compare_columns == ["city", "tier"]
        assert out.scd2.full_snapshot is True

        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            refetched = svc.get_table(table_id)
        assert refetched.scd2 is not None
        assert refetched.scd2.valid_to_column == "valid_to"

    def test_bulk_enrich_tables_carries_scd2(self, tmp_path):
        """The namespace tree serializes through ``bulk_enrich_tables`` — the flag must ride along."""
        _, schema_id = _make_namespace()
        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            self._register(svc, schema_id, tmp_path, "scd2_bulk", _SCD2_CONFIG)
            self._register(svc, schema_id, tmp_path, "plain_bulk", None)

            records = db.query(CatalogTable).order_by(CatalogTable.name).all()
            enriched = svc._bulk_enrich_tables(records, user_id=1)

        by_name = {t.name: t for t in enriched}
        assert by_name["plain_bulk"].scd2 is None
        assert by_name["scd2_bulk"].scd2 is not None
        assert by_name["scd2_bulk"].scd2.surrogate_key_column == "sk"

    def test_no_scd2_config_is_none(self, tmp_path):
        _, schema_id = _make_namespace()
        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            out = self._register(svc, schema_id, tmp_path, "plain_dim", None)
        assert out.scd2 is None

    def test_overwrite_default_preserves_the_flag(self, tmp_path):
        """``/refresh`` and the kernel paths call overwrite without the kwarg; the flag must survive."""
        _, schema_id = _make_namespace()
        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            table_id = self._register(svc, schema_id, tmp_path, "scd2_keep", _SCD2_CONFIG).id

        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            out = svc.overwrite_table_data(
                table_id=table_id,
                table_path=str(tmp_path / "scd2_keep"),
                storage_format="delta",
                schema=_SCD2_SHAPED_SCHEMA,
                row_count=5,
                column_count=len(_SCD2_SHAPED_SCHEMA),
                size_bytes=99,
            )
        assert out.scd2 is not None
        assert out.scd2.business_keys == ["cust_id"]

    def test_overwrite_default_clears_a_flag_whose_columns_are_gone(self, tmp_path):
        """A kernel/refresh overwrite that dropped the system columns must self-heal the record."""
        _, schema_id = _make_namespace()
        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            table_id = self._register(svc, schema_id, tmp_path, "scd2_stale", _SCD2_CONFIG).id

        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            out = svc.overwrite_table_data(
                table_id=table_id,
                table_path=str(tmp_path / "scd2_stale"),
                storage_format="delta",
                schema=[{"name": "cust_id", "dtype": "Int64"}, {"name": "city", "dtype": "String"}],
                row_count=5,
                column_count=2,
                size_bytes=99,
            )
        assert out.scd2 is None

        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            assert svc.get_table(table_id).scd2 is None

    def test_overwrite_default_clears_the_flag_when_one_system_column_is_gone(self, tmp_path):
        _, schema_id = _make_namespace()
        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            table_id = self._register(svc, schema_id, tmp_path, "scd2_partial", _SCD2_CONFIG).id

        partial = [c for c in _SCD2_SHAPED_SCHEMA if c["name"] != "valid_to"]
        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            out = svc.overwrite_table_data(
                table_id=table_id,
                table_path=str(tmp_path / "scd2_partial"),
                storage_format="delta",
                schema=partial,
                row_count=5,
                column_count=len(partial),
                size_bytes=99,
            )
        assert out.scd2 is None

    def test_overwrite_with_explicit_none_clears_the_flag(self, tmp_path):
        _, schema_id = _make_namespace()
        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            table_id = self._register(svc, schema_id, tmp_path, "scd2_clear", _SCD2_CONFIG).id

        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            out = svc.overwrite_table_data(
                table_id=table_id,
                table_path=str(tmp_path / "scd2_clear"),
                storage_format="delta",
                schema=[{"name": "cust_id", "dtype": "Int64"}],
                row_count=5,
                column_count=1,
                size_bytes=99,
                scd2_config=None,
            )
        assert out.scd2 is None

        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            assert svc.get_table(table_id).scd2 is None

    def test_overwrite_can_set_the_flag_on_a_previously_plain_table(self, tmp_path):
        _, schema_id = _make_namespace()
        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            table_id = self._register(svc, schema_id, tmp_path, "becomes_scd2", None).id

        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            out = svc.overwrite_table_data(
                table_id=table_id,
                table_path=str(tmp_path / "becomes_scd2"),
                storage_format="delta",
                schema=[{"name": "cust_id", "dtype": "Int64"}],
                row_count=1,
                column_count=1,
                size_bytes=10,
                scd2_config=_SCD2_CONFIG,
            )
        assert out.scd2 is not None
        assert out.scd2.compare_columns == ["city", "tier"]

    def test_unreadable_scd2_config_degrades_to_none(self, tmp_path):
        _, schema_id = _make_namespace()
        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            table_id = self._register(svc, schema_id, tmp_path, "broken_scd2", _SCD2_CONFIG).id

        with get_db_context() as db:
            record = db.query(CatalogTable).filter(CatalogTable.id == table_id).one()
            record.scd2_config = "{not json"
            db.commit()

        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            assert svc.get_table(table_id).scd2 is None


# optimize / vacuum service (local execution)


def _register_real_delta(svc, schema_id, name, tmp_path, *, partition_by=None):
    """Write a real multi-file delta table on disk and register it."""
    path = tmp_path / name
    df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "x"]})
    df.write_delta(str(path), delta_write_options={"partition_by": partition_by} if partition_by else None)
    pl.DataFrame({"a": [4], "b": ["y"]}).write_delta(str(path), mode="append")
    return svc.register_table_from_data(
        name=name,
        table_path=str(path),
        owner_id=1,
        namespace_id=schema_id,
        storage_format="delta",
        schema=[{"name": "a", "dtype": "Int64"}, {"name": "b", "dtype": "String"}],
        row_count=4,
        column_count=2,
        size_bytes=1,
        partition_columns=partition_by,
    )


class TestOptimizeVacuumService:
    def test_optimize_local(self, tmp_path, monkeypatch):
        import flowfile_core.catalog.services.tables as tables_mod

        monkeypatch.setattr(tables_mod, "_should_offload", lambda: False)
        _, schema_id = _make_namespace()
        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            out = _register_real_delta(svc, schema_id, "opt_tbl", tmp_path)
            result = svc.optimize_table(out.id)
        assert isinstance(result.metrics, dict)
        assert result.size_bytes is not None and result.size_bytes > 0

    def test_vacuum_local_dry_run(self, tmp_path, monkeypatch):
        import flowfile_core.catalog.services.tables as tables_mod

        monkeypatch.setattr(tables_mod, "_should_offload", lambda: False)
        _, schema_id = _make_namespace()
        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            out = _register_real_delta(svc, schema_id, "vac_tbl", tmp_path)
            result = svc.vacuum_table(out.id, retention_hours=0, dry_run=True)
        assert result.dry_run is True
        assert result.file_count == len(result.files_removed)

    def test_optimize_rejects_virtual(self, tmp_path):
        _, schema_id = _make_namespace()
        with get_db_context() as db:
            table = CatalogTable(
                name="virt_tbl",
                namespace_id=schema_id,
                owner_id=1,
                file_path=None,
                table_type="virtual",
                storage_format="delta",
            )
            db.add(table)
            db.commit()
            db.refresh(table)
            table_id = table.id
        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            with pytest.raises(ValueError, match="virtual"):
                svc.optimize_table(table_id)

    def test_vacuum_rejects_legacy_parquet(self, tmp_path):
        _, schema_id = _make_namespace()
        pq = tmp_path / "legacy.parquet"
        pl.DataFrame({"a": [1]}).write_parquet(pq)
        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            out = svc.register_table_from_data(
                name="legacy_tbl",
                table_path=str(pq),
                owner_id=1,
                namespace_id=schema_id,
                storage_format="parquet",
                schema=[{"name": "a", "dtype": "Int64"}],
                row_count=1,
                column_count=1,
                size_bytes=100,
            )
            with pytest.raises(ValueError, match="not a Delta table"):
                svc.vacuum_table(out.id)

    def test_optimize_preserves_updated_at(self, tmp_path, monkeypatch):
        """Maintenance must not bump updated_at: table-trigger schedules fire on it."""
        import flowfile_core.catalog.services.tables as tables_mod

        monkeypatch.setattr(tables_mod, "_should_offload", lambda: False)
        _, schema_id = _make_namespace()
        past = datetime(2020, 1, 1, 12, 0, 0)
        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            out = _register_real_delta(svc, schema_id, "opt_ts_tbl", tmp_path)
            db.query(CatalogTable).filter_by(id=out.id).update({"updated_at": past})
            db.commit()

        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            svc.optimize_table(out.id)

        with get_db_context() as db:
            table = db.query(CatalogTable).filter_by(id=out.id).one()
            assert table.updated_at == past
            assert table.size_bytes > 0
