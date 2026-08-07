"""Tests for the catalog table-edit surface.

Covers:
- CatalogService.apply_table_edits (keyed upserts / deletes / new columns, metadata refresh)
- CatalogService.add_table_key_column (row-id escape hatch)
- Guards: virtual, SCD2, legacy Parquet, unknown key columns, column collisions
- Optimistic concurrency (StaleWriteError -> API 409 stale_write payload)
- API: POST /catalog/tables/{id}/edits and /key-column

Everything runs with OFFLOAD_TO_WORKER off (no worker in the test env), which
exercises the shared ``apply_delta_edits`` path in-process — the same primitive
the worker subprocess task wraps.
"""

import contextlib
import json

import polars as pl
import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.catalog import CatalogService
from flowfile_core.catalog.exceptions import StaleWriteError
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import (
    CatalogNamespace,
    CatalogTable,
    CatalogTableReadLink,
    FlowRegistration,
    FlowRun,
    FlowSchedule,
    ScheduleTriggerTable,
    TableFavorite,
)


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
        db.query(FlowRun).delete()
        db.query(FlowRegistration).delete()
        db.query(CatalogNamespace).delete()
        db.commit()


@pytest.fixture(autouse=True)
def clean_catalog():
    _cleanup_catalog()
    yield
    _cleanup_catalog()


@pytest.fixture(autouse=True)
def offload_off():
    import flowfile_core.configs.settings as settings
    from flowfile_core.configs.utils import MutableBool

    orig = settings.OFFLOAD_TO_WORKER
    settings.OFFLOAD_TO_WORKER = MutableBool(False)
    yield
    settings.OFFLOAD_TO_WORKER = orig


def _make_namespace() -> int:
    with get_db_context() as db:
        cat = CatalogNamespace(name="EditCat", level=0, owner_id=1)
        db.add(cat)
        db.commit()
        db.refresh(cat)
        schema = CatalogNamespace(name="EditSch", level=1, parent_id=cat.id, owner_id=1)
        db.add(schema)
        db.commit()
        db.refresh(schema)
        return schema.id


def _make_delta_table(tmp_path, name="edit_table", **overrides) -> int:
    schema_id = overrides.pop("namespace_id", None) or _make_namespace()
    delta_dir = tmp_path / name
    df = pl.DataFrame({"id": [1, 2, 3], "text": ["a", "b", "c"]})
    df.write_delta(str(delta_dir))
    fields = dict(
        name=name,
        namespace_id=schema_id,
        owner_id=1,
        file_path=str(delta_dir),
        storage_format="delta",
        schema_json=json.dumps([{"name": "id", "dtype": "Int64"}, {"name": "text", "dtype": "String"}]),
        row_count=3,
        column_count=2,
    )
    fields.update(overrides)
    with get_db_context() as db:
        table = CatalogTable(**fields)
        db.add(table)
        db.commit()
        db.refresh(table)
        return table.id


@contextlib.contextmanager
def _service():
    with get_db_context() as db:
        yield CatalogService(SQLAlchemyCatalogRepository(db))


class TestApplyTableEditsService:
    def test_edit_add_column_and_delete(self, tmp_path):
        table_id = _make_delta_table(tmp_path)
        with _service() as svc:
            from flowfile_core.schemas.catalog_schema import CatalogTableEditsRequest, NewColumnSpec

            response = svc.apply_table_edits(
                table_id,
                CatalogTableEditsRequest(
                    key_columns=["id"],
                    expected_version=0,
                    upsert_columns=["id", "text", "label"],
                    upsert_rows=[[1, "a-edit", "spam"], [4, "d", "ham"]],
                    new_columns=[NewColumnSpec(name="label", dtype="String")],
                    delete_keys=[[2]],
                ),
                edited_by="tester",
            )

        assert response.rows_upserted == 2
        assert response.rows_deleted == 1
        assert response.new_version is not None and response.new_version >= 1
        assert response.table.row_count == 3
        assert any(c.name == "label" for c in response.table.schema_columns)

        with get_db_context() as db:
            table = db.get(CatalogTable, table_id)
            out = pl.read_delta(table.file_path).sort("id")
        assert out["id"].to_list() == [1, 3, 4]
        assert out["label"].to_list() == ["spam", None, "ham"]
        # The catalog record was refreshed alongside the data.
        assert table.row_count == 3
        assert table.column_count == 3

    def test_stale_version_raises_stale_write(self, tmp_path):
        table_id = _make_delta_table(tmp_path)
        with _service() as svc:
            from flowfile_core.schemas.catalog_schema import CatalogTableEditsRequest

            with pytest.raises(StaleWriteError) as exc:
                svc.apply_table_edits(
                    table_id,
                    CatalogTableEditsRequest(
                        key_columns=["id"],
                        expected_version=5,
                        upsert_columns=["id", "text"],
                        upsert_rows=[[1, "x"]],
                    ),
                )
        assert exc.value.expected == 5
        assert exc.value.current == 0

    def test_virtual_table_rejected(self, tmp_path):
        table_id = _make_delta_table(tmp_path, table_type="virtual", file_path=None, storage_format=None)
        with _service() as svc, pytest.raises(ValueError, match="virtual"):
            self._simple_edit(svc, table_id)

    def test_scd2_table_rejected(self, tmp_path):
        table_id = _make_delta_table(tmp_path, scd2_config=json.dumps({"business_keys": ["id"]}))
        with _service() as svc, pytest.raises(ValueError, match="SCD2"):
            self._simple_edit(svc, table_id)

    def test_legacy_parquet_rejected(self, tmp_path):
        parquet_path = tmp_path / "legacy.parquet"
        pl.DataFrame({"id": [1]}).write_parquet(parquet_path)
        table_id = _make_delta_table(tmp_path, file_path=str(parquet_path), storage_format="parquet")
        with _service() as svc, pytest.raises(ValueError, match="not a Delta table"):
            self._simple_edit(svc, table_id)

    def test_unknown_key_column_rejected(self, tmp_path):
        table_id = _make_delta_table(tmp_path)
        with _service() as svc:
            from flowfile_core.schemas.catalog_schema import CatalogTableEditsRequest

            with pytest.raises(ValueError, match="Key columns not in table schema"):
                svc.apply_table_edits(
                    table_id,
                    CatalogTableEditsRequest(
                        key_columns=["nope"],
                        upsert_columns=["nope", "text"],
                        upsert_rows=[[1, "x"]],
                    ),
                )

    def test_new_column_collision_rejected(self, tmp_path):
        table_id = _make_delta_table(tmp_path)
        with _service() as svc:
            from flowfile_core.schemas.catalog_schema import CatalogTableEditsRequest, NewColumnSpec

            with pytest.raises(ValueError, match="already exist"):
                svc.apply_table_edits(
                    table_id,
                    CatalogTableEditsRequest(
                        key_columns=["id"],
                        new_columns=[NewColumnSpec(name="text", dtype="String")],
                    ),
                )

    @staticmethod
    def _simple_edit(svc, table_id):
        from flowfile_core.schemas.catalog_schema import CatalogTableEditsRequest

        svc.apply_table_edits(
            table_id,
            CatalogTableEditsRequest(
                key_columns=["id"],
                upsert_columns=["id", "text"],
                upsert_rows=[[1, "x"]],
            ),
        )


class TestAddTableKeyColumnService:
    def test_adds_record_id(self, tmp_path):
        table_id = _make_delta_table(tmp_path)
        with _service() as svc:
            response = svc.add_table_key_column(table_id, column_name="record_id", expected_version=0)
        assert response.column_name == "record_id"
        assert response.new_version == 1
        assert any(c.name == "record_id" for c in response.table.schema_columns)
        with get_db_context() as db:
            table = db.get(CatalogTable, table_id)
            out = pl.read_delta(table.file_path)
        assert sorted(out["record_id"].to_list()) == [1, 2, 3]

    def test_collision_rejected(self, tmp_path):
        table_id = _make_delta_table(tmp_path)
        with _service() as svc, pytest.raises(ValueError, match="already exists"):
            svc.add_table_key_column(table_id, column_name="text")


class TestTableEditsApi:
    def test_edits_endpoint_happy_path(self, client, tmp_path):
        table_id = _make_delta_table(tmp_path)
        response = client.post(
            f"/catalog/tables/{table_id}/edits",
            json={
                "key_columns": ["id"],
                "expected_version": 0,
                "upsert_columns": ["id", "text", "label"],
                "upsert_rows": [[1, "edited", "spam"]],
                "new_columns": [{"name": "label", "dtype": "String"}],
            },
        )
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["rows_upserted"] == 1
        assert body["new_version"] >= 1
        assert any(c["name"] == "label" for c in body["table"]["schema_columns"])

    def test_edits_endpoint_stale_write_409(self, client, tmp_path):
        table_id = _make_delta_table(tmp_path)
        response = client.post(
            f"/catalog/tables/{table_id}/edits",
            json={
                "key_columns": ["id"],
                "expected_version": 9,
                "upsert_columns": ["id", "text"],
                "upsert_rows": [[1, "x"]],
            },
        )
        assert response.status_code == 409
        detail = response.json()["detail"]
        assert detail["error_type" if "error_type" in detail else "error"] == "stale_write"
        assert detail["expected"] == 9
        assert detail["current"] == 0

    def test_edits_endpoint_validation_422(self, client, tmp_path):
        table_id = _make_delta_table(tmp_path)
        response = client.post(
            f"/catalog/tables/{table_id}/edits",
            json={
                "key_columns": ["nope"],
                "upsert_columns": ["nope"],
                "upsert_rows": [[1]],
            },
        )
        assert response.status_code == 422

    def test_edits_endpoint_requires_auth(self, tmp_path):
        table_id = _make_delta_table(tmp_path)
        anon = TestClient(main.app)
        response = anon.post(
            f"/catalog/tables/{table_id}/edits",
            json={"key_columns": ["id"], "upsert_columns": ["id", "text"], "upsert_rows": [[1, "x"]]},
        )
        assert response.status_code == 401

    def test_key_column_endpoint(self, client, tmp_path):
        table_id = _make_delta_table(tmp_path)
        response = client.post(
            f"/catalog/tables/{table_id}/key-column",
            json={"column_name": "record_id", "expected_version": 0},
        )
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["column_name"] == "record_id"
        assert any(c["name"] == "record_id" for c in body["table"]["schema_columns"])

    def test_key_column_bad_name_rejected(self, client, tmp_path):
        table_id = _make_delta_table(tmp_path)
        response = client.post(
            f"/catalog/tables/{table_id}/key-column",
            json={"column_name": "bad name"},
        )
        assert response.status_code == 422

    def test_missing_table_404(self, client):
        response = client.post(
            "/catalog/tables/999999/edits",
            json={"key_columns": ["id"], "upsert_columns": ["id"], "upsert_rows": [[1]]},
        )
        assert response.status_code == 404
