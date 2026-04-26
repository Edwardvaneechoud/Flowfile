"""Tests for the catalog visualization API.

Covers CRUD on saved visualizations and the compute path with a mocked
worker. The worker compute is exercised in the dedicated worker test suite.
"""

import json
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import (
    CatalogNamespace,
    CatalogTable,
    CatalogTableReadLink,
    CatalogVisualization,
    FlowFavorite,
    FlowFollow,
    FlowRegistration,
    FlowRun,
    FlowSchedule,
    ScheduleTriggerTable,
    User,  # noqa: F401  (imported for symmetry with test_catalog.py cleanups)
)


def _get_auth_token() -> str:
    with TestClient(main.app) as c:
        response = c.post("/auth/token")
        return response.json()["access_token"]


def _get_test_client() -> TestClient:
    token = _get_auth_token()
    c = TestClient(main.app)
    c.headers = {"Authorization": f"Bearer {token}"}
    return c


client = _get_test_client()


def _cleanup_catalog():
    with get_db_context() as db:
        db.query(CatalogVisualization).delete()
        db.query(ScheduleTriggerTable).delete()
        db.query(FlowSchedule).delete()
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


def _make_table() -> int:
    """Insert a minimal physical CatalogTable row directly and return its id."""
    with get_db_context() as db:
        ns = CatalogNamespace(name="TestNs", parent_id=None, level=0, owner_id=1)
        db.add(ns)
        db.commit()
        db.refresh(ns)
        table = CatalogTable(
            name="t1",
            namespace_id=ns.id,
            owner_id=1,
            file_path="/tmp/flowfile-test/t1",
            storage_format="delta",
            schema_json=json.dumps([{"name": "value", "dtype": "Int64"}]),
            row_count=10,
            column_count=1,
            size_bytes=1024,
            table_type="physical",
        )
        db.add(table)
        db.commit()
        db.refresh(table)
        return table.id


SAMPLE_CHART = {
    "name": "sum chart",
    "encodings": {
        "rows": [{"fid": "value", "aggName": "sum"}],
        "columns": [{"fid": "category"}],
    },
}
SAMPLE_SPEC = [SAMPLE_CHART]


class TestVisualizationCRUD:
    def test_create_table_source_and_list(self):
        table_id = _make_table()
        resp = client.post(
            "/catalog/visualizations",
            json={
                "name": "viz1",
                "chart_type": "bar",
                "spec": SAMPLE_SPEC,
                "source_type": "table",
                "catalog_table_id": table_id,
            },
        )
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["name"] == "viz1"
        assert body["source_type"] == "table"
        assert body["catalog_table_id"] == table_id
        # spec is a list of charts (one per GW tab); we sent one.
        assert isinstance(body["spec"], list)
        assert body["spec"] == SAMPLE_SPEC
        # VisualizationOut now carries enriched table info so the viewer can
        # render "ns.tablename" without a second round-trip.
        assert body["table_name"] == "t1"
        assert body["table_full_name"].endswith("t1")

        # Library listing returns it.
        lib = client.get("/catalog/visualizations")
        assert lib.status_code == 200
        items = lib.json()
        assert len(items) == 1
        assert items[0]["id"] == body["id"]
        assert items[0]["table_name"] == "t1"

    def test_create_sql_source_no_table(self):
        resp = client.post(
            "/catalog/visualizations",
            json={
                "name": "sql viz",
                "spec": SAMPLE_SPEC,
                "source_type": "sql",
                "sql_query": "SELECT 1 AS x",
            },
        )
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["source_type"] == "sql"
        assert body["catalog_table_id"] is None
        assert body["sql_query"] == "SELECT 1 AS x"

    def test_multi_chart_spec_round_trip(self):
        """exportCode() returns one IChart per GW tab; we round-trip the array."""
        table_id = _make_table()
        chart_a = {**SAMPLE_CHART, "name": "chart A"}
        chart_b = {**SAMPLE_CHART, "name": "chart B"}
        resp = client.post(
            "/catalog/visualizations",
            json={
                "name": "multi",
                "spec": [chart_a, chart_b],
                "source_type": "table",
                "catalog_table_id": table_id,
            },
        )
        assert resp.status_code == 201, resp.text
        viz_id = resp.json()["id"]
        got = client.get(f"/catalog/visualizations/{viz_id}").json()
        assert got["spec"] == [chart_a, chart_b]

    def test_legacy_single_dict_spec_is_coerced(self):
        """008-era rows store a single IChart dict; reads coerce to a list."""
        with get_db_context() as db:
            ns = CatalogNamespace(name="LegacyNs", parent_id=None, level=0, owner_id=1)
            db.add(ns)
            db.commit()
            db.refresh(ns)
            row = CatalogVisualization(
                name="legacy",
                spec_json=json.dumps(SAMPLE_CHART),  # dict, not list
                source_type="sql",
                sql_query="SELECT 1",
                namespace_id=ns.id,
            )
            db.add(row)
            db.commit()
            db.refresh(row)
            legacy_id = row.id

        got = client.get(f"/catalog/visualizations/{legacy_id}").json()
        assert isinstance(got["spec"], list)
        assert got["spec"] == [SAMPLE_CHART]

    def test_create_sql_source_missing_query_returns_422(self):
        resp = client.post(
            "/catalog/visualizations",
            json={"name": "x", "spec": SAMPLE_SPEC, "source_type": "sql"},
        )
        assert resp.status_code == 422

    def test_update_name_and_spec(self):
        table_id = _make_table()
        created = client.post(
            "/catalog/visualizations",
            json={
                "name": "v1",
                "spec": SAMPLE_SPEC,
                "source_type": "table",
                "catalog_table_id": table_id,
            },
        ).json()
        new_spec = {**SAMPLE_SPEC, "extra": "x"}
        resp = client.put(
            f"/catalog/visualizations/{created['id']}",
            json={"name": "v1-renamed", "spec": new_spec},
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["name"] == "v1-renamed"
        assert body["spec"] == new_spec

    def test_update_unknown_returns_404(self):
        resp = client.put(
            "/catalog/visualizations/99999",
            json={"name": "renamed"},
        )
        assert resp.status_code == 404

    def test_delete(self):
        table_id = _make_table()
        viz = client.post(
            "/catalog/visualizations",
            json={
                "name": "v",
                "spec": SAMPLE_SPEC,
                "source_type": "table",
                "catalog_table_id": table_id,
            },
        ).json()
        with patch(
            "flowfile_core.catalog.service.trigger_visualize_evict",
            return_value=None,
        ):
            resp = client.delete(f"/catalog/visualizations/{viz['id']}")
        assert resp.status_code == 204
        list_resp = client.get("/catalog/visualizations")
        assert list_resp.json() == []

    def test_table_filtered_listing(self):
        table_id = _make_table()
        client.post(
            "/catalog/visualizations",
            json={
                "name": "v",
                "spec": SAMPLE_SPEC,
                "source_type": "table",
                "catalog_table_id": table_id,
            },
        )
        resp = client.get(f"/catalog/tables/{table_id}/visualizations")
        assert resp.status_code == 200
        assert len(resp.json()) == 1


class TestVisualizationCompute:
    def test_compute_saved_dispatches_with_table_session_key(self):
        table_id = _make_table()
        viz = client.post(
            "/catalog/visualizations",
            json={
                "name": "v",
                "spec": SAMPLE_SPEC,
                "source_type": "table",
                "catalog_table_id": table_id,
            },
        ).json()

        captured: dict = {}

        def fake_trigger(worker_source, payload, max_rows):
            captured["source"] = worker_source
            captured["payload"] = payload
            captured["max_rows"] = max_rows
            return {
                "rows": [{"category": "a", "value_sum": 4}],
                "total_rows": 1,
                "truncated": False,
                "elapsed_ms": 1.0,
                "cache_hit": False,
            }

        with patch(
            "flowfile_core.catalog.service.trigger_visualize_query",
            side_effect=fake_trigger,
        ):
            resp = client.post(
                f"/catalog/visualizations/{viz['id']}/compute",
                json={"max_rows": 1000},
            )

        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["error"] is None
        assert body["rows"] == [{"category": "a", "value_sum": 4}]
        # The worker source must be physical, with the table-derived session key.
        assert captured["source"]["kind"] == "physical"
        assert captured["source"]["table_path"] == "t1"
        assert captured["source"]["session_key"].startswith(f"tbl:{table_id}:")
        assert captured["max_rows"] == 1000

    def test_compute_saved_for_sql_source(self):
        viz = client.post(
            "/catalog/visualizations",
            json={
                "name": "sql v",
                "spec": SAMPLE_SPEC,
                "source_type": "sql",
                "sql_query": "SELECT 1 AS x",
            },
        ).json()

        captured: dict = {}

        def fake_trigger(worker_source, payload, max_rows):
            captured["source"] = worker_source
            return {
                "rows": [],
                "total_rows": 0,
                "truncated": False,
                "elapsed_ms": 0.5,
                "cache_hit": True,
            }

        with patch(
            "flowfile_core.catalog.service.trigger_visualize_query",
            side_effect=fake_trigger,
        ):
            resp = client.post(f"/catalog/visualizations/{viz['id']}/compute", json={})

        assert resp.status_code == 200, resp.text
        assert captured["source"]["kind"] == "sql"

    def test_compute_ad_hoc_with_table_source(self):
        table_id = _make_table()
        captured: dict = {}

        def fake_trigger(worker_source, payload, max_rows):
            captured["source"] = worker_source
            return {
                "rows": [],
                "total_rows": 0,
                "truncated": False,
                "elapsed_ms": 0.5,
                "cache_hit": True,
            }

        with patch(
            "flowfile_core.catalog.service.trigger_visualize_query",
            side_effect=fake_trigger,
        ):
            resp = client.post(
                "/catalog/visualizations/compute",
                json={
                    "source": {"source_type": "table", "table_id": table_id},
                    "payload": {
                        "workflow": [{"type": "view", "query": [{"op": "raw", "fields": ["*"]}]}]
                    },
                },
            )

        assert resp.status_code == 200, resp.text
        assert captured["source"]["kind"] == "physical"
        assert captured["source"]["session_key"].startswith(f"tbl:{table_id}:")
