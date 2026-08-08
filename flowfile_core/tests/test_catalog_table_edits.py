"""Tests for the catalog table-edit surface.

Covers:
- CatalogService.apply_table_edits (keyed upserts / deletes / new columns, metadata refresh)
- CatalogService.add_table_key_column (row-id escape hatch)
- Guards: virtual, SCD2, legacy Parquet, unknown key columns, column collisions
- Optimistic concurrency (StaleWriteError -> API 409 stale_write payload)
- API: POST /catalog/tables/{id}/edits and /key-column
- The prediction-table association (PUT /catalog/tables/{id} tri-state
  prediction_table_id, enrichment on both read paths, delete hygiene)

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


class TestPredictionTableAssociation:
    """PUT /catalog/tables/{id} maintains the labelled-table -> prediction-table link."""

    @staticmethod
    def _pair(tmp_path) -> tuple[int, int]:
        schema_id = _make_namespace()
        labeled = _make_delta_table(tmp_path, name="labeled", namespace_id=schema_id)
        predictions = _make_delta_table(tmp_path, name="predictions", namespace_id=schema_id)
        return labeled, predictions

    def test_set_association_and_read_back(self, client, tmp_path):
        labeled, predictions = self._pair(tmp_path)
        response = client.put(f"/catalog/tables/{labeled}", json={"prediction_table_id": predictions})
        assert response.status_code == 200, response.text
        assert response.json()["prediction_table_id"] == predictions
        assert response.json()["prediction_table_name"] == "predictions"

        fetched = client.get(f"/catalog/tables/{labeled}")
        assert fetched.status_code == 200, fetched.text
        assert fetched.json()["prediction_table_id"] == predictions
        assert fetched.json()["prediction_table_name"] == "predictions"

    def test_explicit_null_clears_omitted_field_keeps(self, client, tmp_path):
        """The tri-state contract: absent field -> keep, explicit null -> clear."""
        labeled, predictions = self._pair(tmp_path)
        assert client.put(f"/catalog/tables/{labeled}", json={"prediction_table_id": predictions}).status_code == 200

        kept = client.put(f"/catalog/tables/{labeled}", json={"description": "unrelated edit"})
        assert kept.status_code == 200, kept.text
        assert kept.json()["description"] == "unrelated edit"
        assert kept.json()["prediction_table_id"] == predictions
        assert kept.json()["prediction_table_name"] == "predictions"

        cleared = client.put(f"/catalog/tables/{labeled}", json={"prediction_table_id": None})
        assert cleared.status_code == 200, cleared.text
        assert cleared.json()["prediction_table_id"] is None
        assert cleared.json()["prediction_table_name"] is None
        assert client.get(f"/catalog/tables/{labeled}").json()["prediction_table_id"] is None

    def test_self_reference_rejected(self, client, tmp_path):
        labeled, _ = self._pair(tmp_path)
        response = client.put(f"/catalog/tables/{labeled}", json={"prediction_table_id": labeled})
        assert response.status_code == 422, response.text
        assert "own prediction table" in response.json()["detail"]
        with get_db_context() as db:
            assert db.get(CatalogTable, labeled).prediction_table_id is None

    def test_nonexistent_target_rejected(self, client, tmp_path):
        labeled, _ = self._pair(tmp_path)
        response = client.put(f"/catalog/tables/{labeled}", json={"prediction_table_id": 999999})
        assert response.status_code == 404, response.text
        with get_db_context() as db:
            assert db.get(CatalogTable, labeled).prediction_table_id is None

    def test_deleting_prediction_table_nulls_reference(self, client, tmp_path):
        """SQLite reuses rowids, so a surviving reference would later resolve to a stranger."""
        labeled, predictions = self._pair(tmp_path)
        assert client.put(f"/catalog/tables/{labeled}", json={"prediction_table_id": predictions}).status_code == 200

        deleted = client.delete(f"/catalog/tables/{predictions}")
        assert deleted.status_code == 204, deleted.text

        refetched = client.get(f"/catalog/tables/{labeled}")
        assert refetched.status_code == 200, refetched.text
        assert refetched.json()["prediction_table_id"] is None
        assert refetched.json()["prediction_table_name"] is None

    def test_delete_nulls_reference_inside_the_repository_transaction(self, tmp_path):
        """Row delete and reference nulling share one commit.

        Split across two commits, an interruption in between (desktop quit, container
        restart, a swallowed per-table exception in the project prune loop) leaves exactly
        the stale reference rowid reuse then re-points at an unrelated table.
        """
        labeled, predictions = self._pair(tmp_path)
        with _service() as svc:
            svc.update_table(labeled, prediction_table_id=predictions)

        with get_db_context() as db:
            SQLAlchemyCatalogRepository(db).delete_table(predictions)
        with get_db_context() as db:
            assert db.get(CatalogTable, predictions) is None
            assert db.get(CatalogTable, labeled).prediction_table_id is None

    def test_delete_leaves_no_open_write_transaction(self, tmp_path):
        """Deleting a table nothing references must not leave an uncommitted UPDATE behind.

        SQLite takes the RESERVED write lock for a zero-row UPDATE too and holds it until
        the session is torn down — across project sync and the whole storage removal — so a
        concurrent writer would fail with ``database is locked``.
        """
        labeled, predictions = self._pair(tmp_path)
        with _service() as svc:
            svc.delete_table(predictions, delete_file=True)
            with get_db_context() as other:
                other.get(CatalogTable, labeled).description = "written by a concurrent session"
                other.commit()

        with get_db_context() as db:
            assert db.get(CatalogTable, labeled).description == "written by a concurrent session"

    def test_list_tables_carries_association(self, client, tmp_path):
        """bulk_enrich_tables is a separate code path from table_to_out."""
        labeled, predictions = self._pair(tmp_path)
        assert client.put(f"/catalog/tables/{labeled}", json={"prediction_table_id": predictions}).status_code == 200

        listed = client.get("/catalog/tables")
        assert listed.status_code == 200, listed.text
        rows = {row["name"]: row for row in listed.json()}
        assert rows["labeled"]["prediction_table_id"] == predictions
        assert rows["labeled"]["prediction_table_name"] == "predictions"
        assert rows["predictions"]["prediction_table_id"] is None
        assert rows["predictions"]["prediction_table_name"] is None

    def test_dangling_reference_keeps_id_with_null_name(self, client, tmp_path):
        """A target row that vanished out-of-band must not break either read path."""
        labeled, _ = self._pair(tmp_path)
        with get_db_context() as db:
            db.get(CatalogTable, labeled).prediction_table_id = 987654
            db.commit()

        fetched = client.get(f"/catalog/tables/{labeled}")
        assert fetched.status_code == 200, fetched.text
        assert fetched.json()["prediction_table_id"] == 987654
        assert fetched.json()["prediction_table_name"] is None

        listed = client.get("/catalog/tables")
        assert listed.status_code == 200, listed.text
        row = next(r for r in listed.json() if r["id"] == labeled)
        assert row["prediction_table_id"] == 987654
        assert row["prediction_table_name"] is None
