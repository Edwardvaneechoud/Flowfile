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


SAMPLE_SPEC = {
    "name": "sum chart",
    "encodings": {
        "workflow": [
            {
                "type": "view",
                "query": [
                    {
                        "op": "aggregate",
                        "groupBy": ["category"],
                        "measures": [{"field": "value", "agg": "sum", "asFieldKey": "value_sum"}],
                    }
                ],
            }
        ],
    },
}


class TestVisualizationCRUD:
    def test_create_and_list(self):
        table_id = _make_table()
        resp = client.post(
            f"/catalog/tables/{table_id}/visualizations",
            json={"name": "viz1", "chart_type": "bar", "spec": SAMPLE_SPEC},
        )
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["name"] == "viz1"
        assert body["chart_type"] == "bar"
        assert body["spec"] == SAMPLE_SPEC

        list_resp = client.get(f"/catalog/tables/{table_id}/visualizations")
        assert list_resp.status_code == 200
        items = list_resp.json()
        assert len(items) == 1
        assert items[0]["id"] == body["id"]

    def test_duplicate_name_returns_409(self):
        table_id = _make_table()
        client.post(
            f"/catalog/tables/{table_id}/visualizations",
            json={"name": "dup", "spec": SAMPLE_SPEC},
        )
        resp = client.post(
            f"/catalog/tables/{table_id}/visualizations",
            json={"name": "dup", "spec": SAMPLE_SPEC},
        )
        assert resp.status_code == 409

    def test_update_name_and_spec(self):
        table_id = _make_table()
        created = client.post(
            f"/catalog/tables/{table_id}/visualizations",
            json={"name": "v1", "spec": SAMPLE_SPEC},
        ).json()
        new_spec = {**SAMPLE_SPEC, "extra": "x"}
        resp = client.put(
            f"/catalog/tables/{table_id}/visualizations/{created['id']}",
            json={"name": "v1-renamed", "spec": new_spec},
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["name"] == "v1-renamed"
        assert body["spec"] == new_spec

    def test_update_other_table_returns_404(self):
        a = _make_table()
        with get_db_context() as db:
            other = CatalogTable(
                name="other",
                namespace_id=db.query(CatalogNamespace).first().id,
                owner_id=1,
                file_path="/tmp/flowfile-test/other",
                storage_format="delta",
                table_type="physical",
            )
            db.add(other)
            db.commit()
            db.refresh(other)
            other_id = other.id
        viz = client.post(
            f"/catalog/tables/{a}/visualizations",
            json={"name": "v", "spec": SAMPLE_SPEC},
        ).json()
        resp = client.put(
            f"/catalog/tables/{other_id}/visualizations/{viz['id']}",
            json={"name": "renamed"},
        )
        assert resp.status_code == 404

    def test_delete(self):
        table_id = _make_table()
        viz = client.post(
            f"/catalog/tables/{table_id}/visualizations",
            json={"name": "v", "spec": SAMPLE_SPEC},
        ).json()
        with patch(
            "flowfile_core.catalog.service.trigger_visualize_evict",
            return_value=None,
        ):
            resp = client.delete(f"/catalog/tables/{table_id}/visualizations/{viz['id']}")
        assert resp.status_code == 204
        list_resp = client.get(f"/catalog/tables/{table_id}/visualizations")
        assert list_resp.json() == []

    def test_delete_table_cascades_visualizations(self):
        table_id = _make_table()
        client.post(
            f"/catalog/tables/{table_id}/visualizations",
            json={"name": "v", "spec": SAMPLE_SPEC},
        )
        client.delete(f"/catalog/tables/{table_id}")
        with get_db_context() as db:
            assert db.query(CatalogVisualization).count() == 0


class TestVisualizationCompute:
    def test_compute_saved_dispatches_with_table_session_key(self):
        table_id = _make_table()
        viz = client.post(
            f"/catalog/tables/{table_id}/visualizations",
            json={"name": "v", "spec": SAMPLE_SPEC},
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
                f"/catalog/tables/{table_id}/visualizations/{viz['id']}/compute",
                json={"max_rows": 1000},
            )

        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["error"] is None
        assert body["rows"] == [{"category": "a", "value_sum": 4}]
        # The worker source must be physical, and key must include the table id.
        assert captured["source"]["kind"] == "physical"
        assert captured["source"]["table_path"] == "t1"
        assert captured["source"]["session_key"].startswith(f"tbl:{table_id}:")
        assert captured["max_rows"] == 1000

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
