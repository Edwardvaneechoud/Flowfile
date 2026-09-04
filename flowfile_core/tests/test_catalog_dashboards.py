"""Tests for the catalog dashboard API.

Covers CRUD on saved dashboards. Dashboards are pure layout containers —
tile compute reuses ``/catalog/visualizations/{viz_id}/compute`` (already
covered by test_catalog_visualizations.py), so this suite stays focused
on persistence and shape.
"""

import json
import time
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import (
    CatalogDashboard,
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
    User,  # noqa: F401
)


@pytest.fixture(scope="session")
def client() -> TestClient:
    with TestClient(main.app) as auth_c:
        token = auth_c.post("/auth/token").json()["access_token"]
    c = TestClient(main.app)
    c.headers = {"Authorization": f"Bearer {token}"}
    return c


def _cleanup_catalog():
    with get_db_context() as db:
        db.query(CatalogDashboard).delete()
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


def _make_namespace() -> int:
    with get_db_context() as db:
        ns = CatalogNamespace(name="DashNs", parent_id=None, level=0, owner_id=1)
        db.add(ns)
        db.commit()
        db.refresh(ns)
        return ns.id


def _make_table(name: str = "Sales") -> int:
    with get_db_context() as db:
        t = CatalogTable(
            name=name,
            namespace_id=None,
            owner_id=1,
            file_path="/tmp/flowfile-test/sales",
            storage_format="delta",
            table_type="physical",
            schema_json=json.dumps(
                [{"name": "region", "dtype": "String"}, {"name": "amount", "dtype": "Float64"}]
            ),
        )
        db.add(t)
        db.commit()
        db.refresh(t)
        return t.id


def _empty_layout() -> dict:
    return {"tiles": [], "grid": {"cols": 48, "row_height": 40, "version": 2}, "filters": []}


def _layout_with_two_tiles() -> dict:
    return {
        "tiles": [
            {"id": "t1", "type": "viz", "viz_id": 1, "chart_index": 0, "x": 0, "y": 0, "w": 6, "h": 4},
            {"id": "t2", "type": "viz", "viz_id": 2, "chart_index": 0, "x": 6, "y": 0, "w": 6, "h": 4},
        ],
        "grid": {"cols": 12, "row_height": 40, "version": 1},
        "filters": [
            {
                "id": "f1",
                "field_name": "region",
                "label": "Region",
                "kind": "categorical",
                "state": {"selected": ["US", "EU"]},
                "target": "all",
                "target_tile_ids": [],
            }
        ],
    }


class TestDashboardCRUD:
    def test_empty_library(self, client):
        resp = client.get("/catalog/dashboards")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_create_minimal(self, client):
        resp = client.post(
            "/catalog/dashboards",
            json={"name": "Sales overview"},
        )
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["name"] == "Sales overview"
        assert body["description"] is None
        assert body["layout"] == _empty_layout()
        assert body["layout_version"] == 1
        assert body["created_by"] is not None
        assert body["created_at"] and body["updated_at"]

    def test_create_with_layout_and_filters(self, client):
        resp = client.post(
            "/catalog/dashboards",
            json={
                "name": "Two tiles",
                "description": "with a filter",
                "layout": _layout_with_two_tiles(),
            },
        )
        assert resp.status_code == 201
        body = resp.json()
        assert len(body["layout"]["tiles"]) == 2
        assert body["layout"]["filters"][0]["field_name"] == "region"
        # round-trip: reload via GET
        get_resp = client.get(f"/catalog/dashboards/{body['id']}")
        assert get_resp.status_code == 200
        assert get_resp.json()["layout"]["tiles"][1]["viz_id"] == 2

    def test_create_with_separator_tile(self, client):
        layout = _layout_with_two_tiles()
        layout["tiles"].append({"id": "sep-1", "type": "separator", "x": 0, "y": 6, "w": 12, "h": 1})
        layout["tiles"].append(
            {
                "id": "sep-2",
                "type": "separator",
                "orientation": "vertical",
                "thickness": 3,
                "line_color": "#dc2626",
                "x": 6,
                "y": 0,
                "w": 1,
                "h": 6,
            }
        )
        resp = client.post("/catalog/dashboards", json={"name": "With separator", "layout": layout})
        assert resp.status_code == 201, resp.text
        tiles = client.get(f"/catalog/dashboards/{resp.json()['id']}").json()["layout"]["tiles"]
        assert tiles[2]["type"] == "separator"
        assert tiles[2]["viz_id"] is None
        assert tiles[2]["orientation"] == "horizontal"
        assert tiles[2]["thickness"] is None
        assert tiles[3]["orientation"] == "vertical"
        assert tiles[3]["thickness"] == 3
        assert tiles[3]["line_color"] == "#dc2626"

    def test_separator_thickness_out_of_range_returns_422(self, client):
        layout = _layout_with_two_tiles()
        layout["tiles"].append(
            {"id": "sep", "type": "separator", "thickness": 0, "x": 0, "y": 6, "w": 12, "h": 1}
        )
        resp = client.post("/catalog/dashboards", json={"name": "Thin", "layout": layout})
        assert resp.status_code == 422

    def test_create_with_unknown_tile_type_returns_422(self, client):
        layout = _layout_with_two_tiles()
        layout["tiles"].append({"id": "bad", "type": "image", "x": 0, "y": 6, "w": 12, "h": 1})
        resp = client.post("/catalog/dashboards", json={"name": "Bad tile", "layout": layout})
        assert resp.status_code == 422

    def test_create_with_namespace(self, client):
        ns_id = _make_namespace()
        resp = client.post(
            "/catalog/dashboards",
            json={"name": "Scoped", "namespace_id": ns_id},
        )
        assert resp.status_code == 201
        body = resp.json()
        assert body["namespace_id"] == ns_id
        assert body["namespace_name"] == "DashNs"

    def test_create_with_unknown_namespace_returns_404(self, client):
        resp = client.post(
            "/catalog/dashboards",
            json={"name": "Bad ns", "namespace_id": 99999},
        )
        assert resp.status_code == 404

    def test_list_returns_newest_first(self, client):
        first = client.post("/catalog/dashboards", json={"name": "A"}).json()
        second = client.post("/catalog/dashboards", json={"name": "B"}).json()
        # touch the first so it's the most-recently-updated
        client.put(
            f"/catalog/dashboards/{first['id']}",
            json={"description": "touched"},
        )
        listed = client.get("/catalog/dashboards").json()
        ids = [d["id"] for d in listed]
        assert ids[0] == first["id"]
        assert ids[1] == second["id"]

    def test_update_bumps_updated_at_even_when_unchanged(self, client):
        created = client.post(
            "/catalog/dashboards",
            json={"name": "Static", "description": "same"},
        ).json()
        before = created["updated_at"]
        time.sleep(1.05)
        resp = client.put(
            f"/catalog/dashboards/{created['id']}",
            json={"description": "same"},
        )
        assert resp.status_code == 200
        after = resp.json()["updated_at"]
        assert after > before

    def test_get_unknown_returns_404(self, client):
        resp = client.get("/catalog/dashboards/99999")
        assert resp.status_code == 404

    def test_update_layout(self, client):
        created = client.post("/catalog/dashboards", json={"name": "L"}).json()
        new_layout = _layout_with_two_tiles()
        resp = client.put(
            f"/catalog/dashboards/{created['id']}",
            json={"layout": new_layout},
        )
        assert resp.status_code == 200
        assert len(resp.json()["layout"]["tiles"]) == 2
        # verify db is persisted
        with get_db_context() as db:
            row = db.get(CatalogDashboard, created["id"])
            stored = json.loads(row.layout_json)
            assert len(stored["tiles"]) == 2

    def test_update_partial_keeps_other_fields(self, client):
        created = client.post(
            "/catalog/dashboards",
            json={"name": "Original", "layout": _layout_with_two_tiles()},
        ).json()
        resp = client.put(
            f"/catalog/dashboards/{created['id']}",
            json={"name": "Renamed"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["name"] == "Renamed"
        assert len(body["layout"]["tiles"]) == 2  # untouched

    def test_update_clear_name_rejected(self, client):
        created = client.post("/catalog/dashboards", json={"name": "X"}).json()
        resp = client.put(
            f"/catalog/dashboards/{created['id']}",
            json={"name": None},
        )
        assert resp.status_code == 422

    def test_update_clear_layout_rejected(self, client):
        created = client.post("/catalog/dashboards", json={"name": "X"}).json()
        resp = client.put(
            f"/catalog/dashboards/{created['id']}",
            json={"layout": None},
        )
        assert resp.status_code == 422

    def test_update_unknown_returns_404(self, client):
        resp = client.put("/catalog/dashboards/99999", json={"name": "Y"})
        assert resp.status_code == 404

    def test_delete(self, client):
        created = client.post("/catalog/dashboards", json={"name": "D"}).json()
        resp = client.delete(f"/catalog/dashboards/{created['id']}")
        assert resp.status_code == 204
        assert client.get(f"/catalog/dashboards/{created['id']}").status_code == 404

    def test_delete_unknown_returns_404(self, client):
        resp = client.delete("/catalog/dashboards/99999")
        assert resp.status_code == 404


class TestDashboardConcurrency:
    def test_put_without_token_succeeds(self, client):
        created = client.post("/catalog/dashboards", json={"name": "NoToken"}).json()
        resp = client.put(f"/catalog/dashboards/{created['id']}", json={"description": "x"})
        assert resp.status_code == 200, resp.text

    def test_matching_token_chain_increments(self, client):
        created = client.post("/catalog/dashboards", json={"name": "Chain"}).json()
        assert created["layout_version"] == 1
        first = client.put(
            f"/catalog/dashboards/{created['id']}",
            json={"layout": _layout_with_two_tiles(), "expected_layout_version": 1},
        )
        assert first.status_code == 200, first.text
        assert first.json()["layout_version"] == 2
        second = client.put(
            f"/catalog/dashboards/{created['id']}",
            json={"description": "again", "expected_layout_version": 2},
        )
        assert second.status_code == 200, second.text
        assert second.json()["layout_version"] == 3

    def test_stale_token_returns_409_with_structured_detail(self, client):
        created = client.post("/catalog/dashboards", json={"name": "Stale"}).json()
        bump = client.put(
            f"/catalog/dashboards/{created['id']}",
            json={"description": "bump", "expected_layout_version": 1},
        )
        assert bump.status_code == 200
        resp = client.put(
            f"/catalog/dashboards/{created['id']}",
            json={"name": "clobber", "expected_layout_version": 1},
        )
        assert resp.status_code == 409, resp.text
        detail = resp.json()["detail"]
        assert isinstance(detail, dict)
        assert detail["error"] == "stale_write"
        assert detail["resource_type"] == "dashboard"
        assert detail["resource_id"] == created["id"]
        assert detail["expected"] == 1
        assert detail["current"] == 2
        assert "modified in another session" in detail["message"]

    def test_stale_write_leaves_row_unchanged(self, client):
        created = client.post(
            "/catalog/dashboards",
            json={"name": "Untouched", "layout": _layout_with_two_tiles()},
        ).json()
        bump = client.put(
            f"/catalog/dashboards/{created['id']}",
            json={"description": "bump", "expected_layout_version": 1},
        )
        assert bump.status_code == 200
        resp = client.put(
            f"/catalog/dashboards/{created['id']}",
            json={"name": "clobbered", "layout": _empty_layout(), "expected_layout_version": 1},
        )
        assert resp.status_code == 409
        after = client.get(f"/catalog/dashboards/{created['id']}").json()
        assert after["name"] == "Untouched"
        assert len(after["layout"]["tiles"]) == 2

    def test_description_only_put_increments_counter(self, client):
        created = client.post("/catalog/dashboards", json={"name": "DescOnly"}).json()
        resp = client.put(f"/catalog/dashboards/{created['id']}", json={"description": "note"})
        assert resp.status_code == 200, resp.text
        assert resp.json()["layout_version"] == 2


class TestDashboardFilterDatasource:
    def test_create_with_filter_bound_to_datasource(self, client):
        table_id = _make_table()
        layout = _layout_with_two_tiles()
        layout["filters"] = [
            {
                "id": "f1",
                "field_name": "region",
                "label": "Region",
                "kind": "categorical",
                "state": {"selected": ["US"]},
                "target": "all",
                "target_tile_ids": [],
                "datasource_id": table_id,
            }
        ]
        resp = client.post(
            "/catalog/dashboards",
            json={"name": "Bound", "layout": layout},
        )
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["layout"]["filters"][0]["datasource_id"] == table_id
        # round-trip
        get_resp = client.get(f"/catalog/dashboards/{body['id']}")
        assert get_resp.json()["layout"]["filters"][0]["datasource_id"] == table_id

    def test_filter_scope_defaults_to_datasource_and_round_trips_all(self, client):
        table_id = _make_table()
        layout = _layout_with_two_tiles()
        base = {
            "field_name": "region",
            "kind": "categorical",
            "state": {"selected": []},
            "target": "all",
            "target_tile_ids": [],
            "datasource_id": table_id,
        }
        layout["filters"] = [{"id": "f1", **base}, {"id": "f2", "scope": "all", **base}]
        resp = client.post("/catalog/dashboards", json={"name": "Scoped", "layout": layout})
        assert resp.status_code == 201, resp.text
        filters = client.get(f"/catalog/dashboards/{resp.json()['id']}").json()["layout"]["filters"]
        assert filters[0]["scope"] == "datasource"
        assert filters[1]["scope"] == "all"

    def test_filter_unknown_scope_returns_422(self, client):
        layout = _layout_with_two_tiles()
        layout["filters"] = [
            {
                "id": "f1",
                "field_name": "region",
                "kind": "categorical",
                "state": {},
                "target": "all",
                "target_tile_ids": [],
                "scope": "everywhere",
            }
        ]
        resp = client.post("/catalog/dashboards", json={"name": "Bad scope", "layout": layout})
        assert resp.status_code == 422

    def test_create_with_unknown_datasource_returns_422(self, client):
        layout = _empty_layout()
        layout["filters"] = [
            {
                "id": "f1",
                "field_name": "region",
                "kind": "categorical",
                "state": {"selected": []},
                "target": "all",
                "target_tile_ids": [],
                "datasource_id": 99999,
            }
        ]
        resp = client.post(
            "/catalog/dashboards",
            json={"name": "Bad ds", "layout": layout},
        )
        assert resp.status_code == 422

    def test_legacy_filter_without_datasource_still_works(self, client):
        # filters created before the field existed should round-trip with None
        resp = client.post(
            "/catalog/dashboards",
            json={"name": "Legacy", "layout": _layout_with_two_tiles()},
        )
        assert resp.status_code == 201
        body = resp.json()
        assert body["layout"]["filters"][0]["datasource_id"] is None

    def test_column_stats_dispatches_with_table_session_key(self, client):
        table_id = _make_table()
        captured: dict = {}

        def fake_trigger(worker_source, column, limit):
            captured["source"] = worker_source
            captured["column"] = column
            captured["limit"] = limit
            return {
                "dtype": "String",
                "values": ["EU", "NA", "APAC"],
                "truncated": False,
                "distinct_count": 3,
                "min": None,
                "max": None,
                "cache_hit": False,
            }

        with patch(
            "flowfile_core.catalog.service.trigger_visualize_column_stats",
            side_effect=fake_trigger,
        ):
            resp = client.get(
                f"/catalog/tables/{table_id}/columns/region/stats?limit=50"
            )

        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["values"] == ["EU", "NA", "APAC"]
        assert body["distinct_count"] == 3
        assert body["truncated"] is False
        assert captured["column"] == "region"
        assert captured["limit"] == 50
        assert captured["source"]["kind"] == "physical"
        assert captured["source"]["session_key"].startswith(f"tbl:{table_id}:")

    def test_column_stats_unknown_table_returns_404(self, client):
        resp = client.get("/catalog/tables/99999/columns/region/stats")
        assert resp.status_code == 404

    def test_column_stats_clamps_limit(self, client):
        # Pydantic Query(le=1000) should reject > 1000
        table_id = _make_table()
        resp = client.get(
            f"/catalog/tables/{table_id}/columns/region/stats?limit=99999"
        )
        assert resp.status_code == 422

    def test_update_with_unknown_datasource_returns_422(self, client):
        created = client.post("/catalog/dashboards", json={"name": "U"}).json()
        layout = _empty_layout()
        layout["filters"] = [
            {
                "id": "f1",
                "field_name": "region",
                "kind": "categorical",
                "state": {},
                "target": "all",
                "target_tile_ids": [],
                "datasource_id": 99999,
            }
        ]
        resp = client.put(
            f"/catalog/dashboards/{created['id']}",
            json={"layout": layout},
        )
        assert resp.status_code == 422

    def test_create_with_unknown_target_tile_id_returns_422(self, client):
        layout = _layout_with_two_tiles()
        layout["filters"] = [
            {
                "id": "f_bad",
                "field_name": "region",
                "kind": "categorical",
                "state": {},
                "target": "tiles",
                "target_tile_ids": ["bogus"],
            }
        ]
        resp = client.post(
            "/catalog/dashboards",
            json={"name": "Bad tile", "layout": layout},
        )
        assert resp.status_code == 422

    def test_update_with_unknown_target_tile_id_returns_422(self, client):
        created = client.post(
            "/catalog/dashboards",
            json={"name": "U", "layout": _layout_with_two_tiles()},
        ).json()
        layout = _layout_with_two_tiles()
        layout["filters"] = [
            {
                "id": "f_bad",
                "field_name": "region",
                "kind": "categorical",
                "state": {},
                "target": "tiles",
                "target_tile_ids": ["bogus"],
            }
        ]
        resp = client.put(
            f"/catalog/dashboards/{created['id']}",
            json={"layout": layout},
        )
        assert resp.status_code == 422
