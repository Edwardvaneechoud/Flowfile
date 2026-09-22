"""Routes for catalog change tracking (CDC).

Covers ``GET /catalog/tables/{id}/cdc``, ``POST .../cdc/enable``,
``POST .../cdc/cursors/reset``, ``DELETE .../cdc/cursors/{consumer_key}``
and the vacuum route's CDC_CURSORS_AT_RISK guard. Real Delta tables in tmp dirs.
"""

from datetime import datetime, timedelta

import polars as pl
import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import (
    CatalogCdcCursor,
    CatalogNamespace,
    CatalogTable,
    CatalogTableReadLink,
    TableFavorite,
)
from shared.delta_utils import get_delta_head_version, is_change_data_feed_enabled

_SCD2_CONFIG = {
    "business_keys": ["cust_id"],
    "surrogate_key_column": "sk",
    "valid_from_column": "valid_from",
    "valid_to_column": "valid_to",
    "is_current_column": "is_current",
    "compare_columns": ["city"],
    "full_snapshot": True,
}


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
        db.query(CatalogCdcCursor).delete()
        db.query(TableFavorite).delete()
        db.query(CatalogTableReadLink).delete()
        db.query(CatalogTable).delete()
        db.query(CatalogNamespace).delete()
        db.commit()


@pytest.fixture(autouse=True)
def clean_catalog():
    _cleanup_catalog()
    yield
    _cleanup_catalog()


@pytest.fixture
def schema_id() -> int:
    with get_db_context() as db:
        cat = CatalogNamespace(name="CdcCat", level=0, owner_id=1)
        db.add(cat)
        db.commit()
        db.refresh(cat)
        schema = CatalogNamespace(name="CdcSch", level=1, parent_id=cat.id, owner_id=1)
        db.add(schema)
        db.commit()
        db.refresh(schema)
        return schema.id


def _register_delta(schema_id: int, name: str, tmp_path, *, scd2_config=None, commits: int = 2) -> tuple[int, str]:
    """Write a real delta table with *commits* versions and register it; returns (id, path)."""
    path = tmp_path / name
    pl.DataFrame({"cust_id": [1, 2], "city": ["ams", "rtm"]}).write_delta(str(path))
    for i in range(commits - 1):
        pl.DataFrame({"cust_id": [10 + i], "city": ["utr"]}).write_delta(str(path), mode="append")
    with get_db_context() as db:
        svc = CatalogService(SQLAlchemyCatalogRepository(db))
        out = svc.register_table_from_data(
            name=name,
            table_path=str(path),
            owner_id=1,
            namespace_id=schema_id,
            storage_format="delta",
            schema=[{"name": "cust_id", "dtype": "Int64"}, {"name": "city", "dtype": "String"}],
            row_count=2,
            column_count=2,
            size_bytes=1,
            scd2_config=scd2_config,
        )
        return out.id, str(path)


def _add_cursor(table_id: int, consumer_key: str, last_version: int, *, table_path=None, owner_id=1, updated_at=None):
    with get_db_context() as db:
        cursor = CatalogCdcCursor(
            table_id=table_id,
            consumer_key=consumer_key,
            consumer_label="nightly",
            owner_id=owner_id,
            last_version=last_version,
            table_path=table_path,
        )
        db.add(cursor)
        db.commit()
        if updated_at is not None:
            db.query(CatalogCdcCursor).filter_by(id=cursor.id).update({"updated_at": updated_at})
            db.commit()


class TestGetCdcStatus:
    def test_untracked_table(self, client, schema_id, tmp_path):
        table_id, path = _register_delta(schema_id, "cdc_off", tmp_path)
        body = client.get(f"/catalog/tables/{table_id}/cdc").json()
        assert body["cdc_enabled"] is False
        assert body["cdc_enabled_version"] is None
        assert body["current_version"] == get_delta_head_version(path)
        assert body["cursors"] == []

    def test_unknown_table_404(self, client):
        assert client.get("/catalog/tables/99999/cdc").status_code == 404

    def test_cursor_pending_commits(self, client, schema_id, tmp_path):
        table_id, path = _register_delta(schema_id, "cdc_pending", tmp_path, commits=3)
        client.post(f"/catalog/tables/{table_id}/cdc/enable")
        _add_cursor(table_id, "name:nightly", 0)
        body = client.get(f"/catalog/tables/{table_id}/cdc").json()
        head = get_delta_head_version(path)
        assert body["current_version"] == head
        assert len(body["cursors"]) == 1
        cursor = body["cursors"][0]
        assert cursor["consumer_key"] == "name:nightly"
        assert cursor["consumer_label"] == "nightly"
        assert cursor["last_version"] == 0
        assert cursor["pending_commits"] == head


class TestEnableCdc:
    def test_enable_sets_property_and_floor(self, client, schema_id, tmp_path):
        table_id, path = _register_delta(schema_id, "cdc_enable", tmp_path)
        body = client.post(f"/catalog/tables/{table_id}/cdc/enable").json()
        assert body["cdc_enabled"] is True
        assert body["cdc_enabled_version"] == get_delta_head_version(path)
        assert is_change_data_feed_enabled(path) is True

    def test_enable_is_idempotent_and_keeps_floor(self, client, schema_id, tmp_path):
        table_id, path = _register_delta(schema_id, "cdc_twice", tmp_path)
        floor = client.post(f"/catalog/tables/{table_id}/cdc/enable").json()["cdc_enabled_version"]
        pl.DataFrame({"cust_id": [9], "city": ["dhg"]}).write_delta(str(path), mode="append")
        body = client.post(f"/catalog/tables/{table_id}/cdc/enable").json()
        assert body["cdc_enabled_version"] == floor
        assert body["current_version"] > floor

    def test_enable_rejects_scd2(self, client, schema_id, tmp_path):
        table_id, _ = _register_delta(schema_id, "cdc_scd2", tmp_path, scd2_config=_SCD2_CONFIG)
        response = client.post(f"/catalog/tables/{table_id}/cdc/enable")
        assert response.status_code == 409
        assert "SCD2" in response.json()["detail"]

    def test_enable_rejects_virtual(self, client, schema_id):
        with get_db_context() as db:
            table = CatalogTable(
                name="cdc_virtual",
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
        response = client.post(f"/catalog/tables/{table_id}/cdc/enable")
        assert response.status_code == 409
        assert "virtual" in response.json()["detail"]

    def test_enable_rejects_legacy_parquet(self, client, schema_id, tmp_path):
        pq = tmp_path / "legacy.parquet"
        pl.DataFrame({"a": [1]}).write_parquet(pq)
        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            out = svc.register_table_from_data(
                name="cdc_parquet",
                table_path=str(pq),
                owner_id=1,
                namespace_id=schema_id,
                storage_format="parquet",
                schema=[{"name": "a", "dtype": "Int64"}],
                row_count=1,
                column_count=1,
                size_bytes=10,
            )
        response = client.post(f"/catalog/tables/{out.id}/cdc/enable")
        assert response.status_code == 409
        assert "not a Delta table" in response.json()["detail"]

    def test_enable_unknown_table_404(self, client):
        assert client.post("/catalog/tables/99999/cdc/enable").status_code == 404


class TestResetCursor:
    def test_reset_to_now(self, client, schema_id, tmp_path):
        table_id, path = _register_delta(schema_id, "cdc_reset_now", tmp_path, commits=3)
        client.post(f"/catalog/tables/{table_id}/cdc/enable")
        _add_cursor(table_id, "name:nightly", 0)
        body = client.post(
            f"/catalog/tables/{table_id}/cdc/cursors/reset",
            json={"consumer_key": "name:nightly", "to": "now"},
        ).json()
        assert body["last_version"] == get_delta_head_version(path)
        assert body["pending_commits"] == 0

    def test_reset_to_beginning(self, client, schema_id, tmp_path):
        table_id, _ = _register_delta(schema_id, "cdc_reset_begin", tmp_path)
        floor = client.post(f"/catalog/tables/{table_id}/cdc/enable").json()["cdc_enabled_version"]
        _add_cursor(table_id, "name:nightly", 5)
        body = client.post(
            f"/catalog/tables/{table_id}/cdc/cursors/reset",
            json={"consumer_key": "name:nightly", "to": "beginning"},
        ).json()
        assert body["last_version"] == floor - 1

    def test_reset_to_explicit_version(self, client, schema_id, tmp_path):
        table_id, _ = _register_delta(schema_id, "cdc_reset_v", tmp_path, commits=4)
        client.post(f"/catalog/tables/{table_id}/cdc/enable")
        _add_cursor(table_id, "name:nightly", 0)
        body = client.post(
            f"/catalog/tables/{table_id}/cdc/cursors/reset",
            json={"consumer_key": "name:nightly", "to": 2},
        ).json()
        assert body["last_version"] == 2

    def test_reset_unknown_cursor_404(self, client, schema_id, tmp_path):
        table_id, _ = _register_delta(schema_id, "cdc_reset_404", tmp_path)
        response = client.post(
            f"/catalog/tables/{table_id}/cdc/cursors/reset",
            json={"consumer_key": "name:nope", "to": "now"},
        )
        assert response.status_code == 404


class TestDeleteCursor:
    def test_delete_then_gone(self, client, schema_id, tmp_path):
        table_id, _ = _register_delta(schema_id, "cdc_delete", tmp_path)
        _add_cursor(table_id, "name:nightly", 1)
        assert client.delete(f"/catalog/tables/{table_id}/cdc/cursors/name:nightly").status_code == 204
        assert client.get(f"/catalog/tables/{table_id}/cdc").json()["cursors"] == []
        assert client.delete(f"/catalog/tables/{table_id}/cdc/cursors/name:nightly").status_code == 404


class TestVacuumCursorGuard:
    @pytest.fixture(autouse=True)
    def _local_execution(self, monkeypatch):
        import flowfile_core.catalog.services.tables as tables_mod

        monkeypatch.setattr(tables_mod, "_should_offload", lambda: False)

    def test_stale_cursor_blocks_vacuum(self, client, schema_id, tmp_path):
        table_id, _ = _register_delta(schema_id, "cdc_vac_block", tmp_path, commits=3)
        client.post(f"/catalog/tables/{table_id}/cdc/enable")
        _add_cursor(table_id, "name:nightly", 0, updated_at=datetime.now() - timedelta(days=30))
        response = client.post(
            f"/catalog/tables/{table_id}/vacuum",
            json={"retention_hours": 168, "dry_run": True},
        )
        assert response.status_code == 409
        detail = response.json()["detail"]
        assert detail["error_code"] == "CDC_CURSORS_AT_RISK"
        assert [c["consumer_key"] for c in detail["cursors"]] == ["name:nightly"]

    def test_force_vacuums_anyway(self, client, schema_id, tmp_path):
        table_id, _ = _register_delta(schema_id, "cdc_vac_force", tmp_path, commits=3)
        client.post(f"/catalog/tables/{table_id}/cdc/enable")
        _add_cursor(table_id, "name:nightly", 0, updated_at=datetime.now() - timedelta(days=30))
        response = client.post(
            f"/catalog/tables/{table_id}/vacuum",
            json={"retention_hours": 168, "dry_run": True, "force": True},
        )
        assert response.status_code == 200
        assert response.json()["dry_run"] is True

    def test_caught_up_cursor_does_not_block(self, client, schema_id, tmp_path):
        table_id, path = _register_delta(schema_id, "cdc_vac_head", tmp_path, commits=3)
        client.post(f"/catalog/tables/{table_id}/cdc/enable")
        _add_cursor(
            table_id,
            "name:nightly",
            get_delta_head_version(path),
            updated_at=datetime.now() - timedelta(days=30),
        )
        response = client.post(
            f"/catalog/tables/{table_id}/vacuum",
            json={"retention_hours": 168, "dry_run": True},
        )
        assert response.status_code == 200

    def test_untracked_table_vacuums_normally(self, client, schema_id, tmp_path):
        table_id, _ = _register_delta(schema_id, "cdc_vac_plain", tmp_path, commits=3)
        response = client.post(
            f"/catalog/tables/{table_id}/vacuum",
            json={"retention_hours": 168, "dry_run": True},
        )
        assert response.status_code == 200
