"""Tests for the catalog dashboard API.

Covers CRUD on saved dashboards. Dashboards are pure layout containers —
tile compute reuses ``/catalog/visualizations/{viz_id}/compute`` (already
covered by test_catalog_visualizations.py), so this suite stays focused
on persistence and shape.
"""

import base64
import json
import struct
import time
import zlib
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
    return {
        "tiles": [],
        "grid": {"cols": 48, "row_height": 40, "version": 2},
        "filters": [],
        "color_assignments": [],
        "auto_refresh_seconds": None,
        "tabs": [],
    }


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

    def test_chart_tile_title_round_trips_and_defaults(self, client):
        layout = _layout_with_two_tiles()
        layout["tiles"][0].update({"title": "Revenue by region", "show_title": False})
        created = client.post("/catalog/dashboards", json={"name": "Titled", "layout": layout})
        assert created.status_code == 201, created.text
        tiles = client.get(f"/catalog/dashboards/{created.json()['id']}").json()["layout"]["tiles"]
        assert (tiles[0]["title"], tiles[0]["show_title"]) == ("Revenue by region", False)
        # A tile saved before titles existed shows its chart's own name.
        assert (tiles[1]["title"], tiles[1]["show_title"]) == (None, True)

    def test_chart_format_round_trips_and_defaults_to_none(self, client):
        layout = _layout_with_two_tiles()
        chart_format = {"number_format": ".3~s", "time_format": "%b %Y", "x_title": "Revenue", "y_title": None}
        layout["tiles"][0]["chart_format"] = chart_format
        created = client.post("/catalog/dashboards", json={"name": "Formatted", "layout": layout})
        assert created.status_code == 201, created.text
        tiles = client.get(f"/catalog/dashboards/{created.json()['id']}").json()["layout"]["tiles"]
        assert tiles[0]["chart_format"] == chart_format
        assert tiles[1]["chart_format"] is None

    def test_layout_saved_before_chart_formats_loads_unchanged(self, client):
        layout = _layout_with_two_tiles()
        with get_db_context() as db:
            row = CatalogDashboard(name="Old", layout_json=json.dumps(layout), layout_version=1, created_by=1)
            db.add(row)
            db.commit()
            db.refresh(row)
            dash_id = row.id
        tiles = client.get(f"/catalog/dashboards/{dash_id}").json()["layout"]["tiles"]
        assert [t["chart_format"] for t in tiles] == [None, None]
        assert [(t["viz_id"], t["title"]) for t in tiles] == [(1, None), (2, None)]

    @pytest.mark.parametrize(
        "bad_format",
        [{"number_format": "x" * 65}, {"time_format": "%Y" * 33}, {"x_title": "x" * 121}, {"y_title": "y" * 121}],
    )
    def test_chart_format_over_its_limits_returns_422(self, client, bad_format):
        layout = _layout_with_two_tiles()
        layout["tiles"][0]["chart_format"] = bad_format
        resp = client.post("/catalog/dashboards", json={"name": "Too long", "layout": layout})
        assert resp.status_code == 422

    def test_color_assignments_round_trip_with_their_value_types(self, client):
        layout = _layout_with_two_tiles()
        assignments = [
            {"field": "region", "value": "West", "slot": 0},
            {"field": "region", "value": "East", "slot": 1},
            {"field": "code", "value": 1, "slot": 0},
            {"field": "code", "value": "1", "slot": 1},
            {"field": "flag", "value": True, "slot": 0},
            {"field": "region", "value": None, "slot": 2},
        ]
        layout["color_assignments"] = assignments
        created = client.post("/catalog/dashboards", json={"name": "Coloured", "layout": layout})
        assert created.status_code == 201, created.text
        stored = client.get(f"/catalog/dashboards/{created.json()['id']}").json()["layout"]["color_assignments"]
        assert stored == assignments
        assert [type(a["value"]) for a in stored] == [str, str, int, str, bool, type(None)]

    def test_layout_saved_before_color_assignments_loads_with_none(self, client):
        with get_db_context() as db:
            row = CatalogDashboard(
                name="Uncoloured", layout_json=json.dumps(_layout_with_two_tiles()), layout_version=1, created_by=1
            )
            db.add(row)
            db.commit()
            db.refresh(row)
            dash_id = row.id
        layout = client.get(f"/catalog/dashboards/{dash_id}").json()["layout"]
        assert layout["color_assignments"] == []
        assert [t["viz_id"] for t in layout["tiles"]] == [1, 2]

    @pytest.mark.parametrize(
        "bad",
        [
            [{"field": "region", "value": "West", "slot": 0}, {"field": "region", "value": "West", "slot": 1}],
            [{"field": "code", "value": 1, "slot": 0}, {"field": "code", "value": 1.0, "slot": 1}],
            [{"field": "region", "value": "West", "slot": -1}],
            [{"field": "", "value": "West", "slot": 0}],
            [{"field": "region", "value": ["West"], "slot": 0}],
        ],
    )
    def test_invalid_color_assignments_return_422(self, client, bad):
        layout = _layout_with_two_tiles()
        layout["color_assignments"] = bad
        resp = client.post("/catalog/dashboards", json={"name": "Bad colours", "layout": layout})
        assert resp.status_code == 422

    def test_same_value_in_two_fields_may_take_one_slot_each(self, client):
        layout = _layout_with_two_tiles()
        layout["color_assignments"] = [
            {"field": "region", "value": "West", "slot": 0},
            {"field": "area", "value": "West", "slot": 3},
        ]
        resp = client.post("/catalog/dashboards", json={"name": "Two fields", "layout": layout})
        assert resp.status_code == 201, resp.text

    def test_auto_refresh_interval_round_trips(self, client):
        layout = _layout_with_two_tiles()
        layout["auto_refresh_seconds"] = 300
        created = client.post("/catalog/dashboards", json={"name": "Wall screen", "layout": layout})
        assert created.status_code == 201, created.text
        dash_id = created.json()["id"]
        assert client.get(f"/catalog/dashboards/{dash_id}").json()["layout"]["auto_refresh_seconds"] == 300

        layout["auto_refresh_seconds"] = None
        updated = client.put(f"/catalog/dashboards/{dash_id}", json={"layout": layout})
        assert updated.status_code == 200, updated.text
        assert client.get(f"/catalog/dashboards/{dash_id}").json()["layout"]["auto_refresh_seconds"] is None

    def test_layout_saved_before_auto_refresh_loads_with_it_off(self, client):
        with get_db_context() as db:
            row = CatalogDashboard(
                name="Static", layout_json=json.dumps(_layout_with_two_tiles()), layout_version=1, created_by=1
            )
            db.add(row)
            db.commit()
            db.refresh(row)
            dash_id = row.id
        layout = client.get(f"/catalog/dashboards/{dash_id}").json()["layout"]
        assert layout["auto_refresh_seconds"] is None
        assert [t["viz_id"] for t in layout["tiles"]] == [1, 2]

    @pytest.mark.parametrize("bad", [0, 1, 59, -60, 24 * 60 * 60 + 1, "often"])
    def test_auto_refresh_under_a_minute_returns_422(self, client, bad):
        layout = _layout_with_two_tiles()
        layout["auto_refresh_seconds"] = bad
        resp = client.post("/catalog/dashboards", json={"name": "Too often", "layout": layout})
        assert resp.status_code == 422

    def test_tile_note_round_trips_through_create_update_and_get(self, client):
        layout = _layout_with_two_tiles()
        layout["tiles"][0]["note_md"] = "**Revenue** excludes refunds. [Definition](https://example.com)"
        created = client.post("/catalog/dashboards", json={"name": "Noted", "layout": layout})
        assert created.status_code == 201, created.text
        dash_id = created.json()["id"]
        tiles = client.get(f"/catalog/dashboards/{dash_id}").json()["layout"]["tiles"]
        assert tiles[0]["note_md"] == layout["tiles"][0]["note_md"]
        assert tiles[1]["note_md"] is None

        layout["tiles"][0]["note_md"] = None
        layout["tiles"][1]["note_md"] = "x" * 4000
        updated = client.put(f"/catalog/dashboards/{dash_id}", json={"layout": layout})
        assert updated.status_code == 200, updated.text
        tiles = client.get(f"/catalog/dashboards/{dash_id}").json()["layout"]["tiles"]
        assert [t["note_md"] for t in tiles] == [None, "x" * 4000]

    def test_layout_saved_before_tile_notes_loads_without_them(self, client):
        layout = _layout_with_two_tiles()
        with get_db_context() as db:
            row = CatalogDashboard(name="Old", layout_json=json.dumps(layout), layout_version=1, created_by=1)
            db.add(row)
            db.commit()
            db.refresh(row)
            dash_id = row.id
        tiles = client.get(f"/catalog/dashboards/{dash_id}").json()["layout"]["tiles"]
        assert [t["note_md"] for t in tiles] == [None, None]

    def test_tile_note_over_4000_characters_returns_422(self, client):
        layout = _layout_with_two_tiles()
        layout["tiles"][0]["note_md"] = "x" * 4001
        resp = client.post("/catalog/dashboards", json={"name": "Too long", "layout": layout})
        assert resp.status_code == 422
        created = client.post("/catalog/dashboards", json={"name": "Short", "layout": _layout_with_two_tiles()})
        resp = client.put(f"/catalog/dashboards/{created.json()['id']}", json={"layout": layout})
        assert resp.status_code == 422

    def test_tile_title_over_120_characters_returns_422(self, client):
        layout = _layout_with_two_tiles()
        layout["tiles"][0]["title"] = "x" * 121
        resp = client.post("/catalog/dashboards", json={"name": "Too long", "layout": layout})
        assert resp.status_code == 422

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

    def test_create_with_kpi_tile(self, client):
        kpi = {
            "field": "amount",
            "agg": "distinctCount",
            "label": "Unique amounts",
            "prefix": "$",
            "suffix": " USD",
            "decimals": 2,
            "compact": False,
            "comparison": "target",
            "target": 1250.5,
            "higher_is_better": False,
            "value_size": "lg",
        }
        layout = _layout_with_two_tiles()
        layout["tiles"].append({"id": "kpi-1", "type": "kpi", "viz_id": 1, "kpi": kpi, "x": 0, "y": 6, "w": 3, "h": 2})
        layout["tiles"].append({"id": "kpi-2", "type": "kpi", "kpi": None, "x": 3, "y": 6, "w": 3, "h": 2})
        layout["tiles"].append(
            {"id": "kpi-3", "type": "kpi", "viz_id": 2, "kpi": {"agg": "count"}, "x": 6, "y": 6, "w": 3, "h": 2}
        )
        resp = client.post("/catalog/dashboards", json={"name": "With KPI", "layout": layout})
        assert resp.status_code == 201, resp.text
        tiles = client.get(f"/catalog/dashboards/{resp.json()['id']}").json()["layout"]["tiles"]
        assert tiles[0]["kpi"] is None
        assert tiles[2]["type"] == "kpi"
        assert tiles[2]["viz_id"] == 1
        assert tiles[2]["kpi"] == {**kpi, "trend": None}
        assert tiles[3]["viz_id"] is None
        assert tiles[3]["kpi"] is None
        assert tiles[4]["kpi"] == {
            "field": None,
            "agg": "count",
            "label": None,
            "prefix": None,
            "suffix": None,
            "decimals": None,
            "compact": True,
            "comparison": "none",
            "target": None,
            "higher_is_better": True,
            "value_size": "auto",
            "trend": None,
        }

    @pytest.mark.parametrize(
        "bad_kpi",
        [
            {"agg": "variance"},
            {"agg": "sum", "decimals": 7},
            {"agg": "sum", "comparison": "bogus"},
            {"agg": "sum", "label": "x" * 121},
            {"agg": "sum", "value_size": "huge"},
            {"agg": "sum", "trend": {"field": "order_date", "grain": "fortnight"}},
            {"agg": "sum", "trend": {"field": ""}},
            {"agg": "sum", "trend": {"grain": "month"}},
        ],
    )
    def test_invalid_kpi_config_returns_422(self, client, bad_kpi):
        layout = _layout_with_two_tiles()
        layout["tiles"].append({"id": "kpi", "type": "kpi", "viz_id": 1, "kpi": bad_kpi, "x": 0, "y": 6, "w": 3, "h": 2})
        resp = client.post("/catalog/dashboards", json={"name": "Bad KPI", "layout": layout})
        assert resp.status_code == 422

    def test_kpi_trend_round_trips_and_defaults_to_auto(self, client):
        layout = _layout_with_two_tiles()
        layout["tiles"].append(
            {
                "id": "kpi-trend",
                "type": "kpi",
                "viz_id": 1,
                "kpi": {"field": "amount", "comparison": "previous_period", "trend": {"field": "order_date", "grain": "week"}},
                "x": 0,
                "y": 6,
                "w": 12,
                "h": 3,
            }
        )
        layout["tiles"].append(
            {"id": "kpi-auto", "type": "kpi", "viz_id": 1, "kpi": {"trend": {"field": "order_date"}}, "x": 12, "y": 6, "w": 12, "h": 3}
        )
        resp = client.post("/catalog/dashboards", json={"name": "With trend", "layout": layout})
        assert resp.status_code == 201, resp.text
        tiles = client.get(f"/catalog/dashboards/{resp.json()['id']}").json()["layout"]["tiles"]
        assert tiles[2]["kpi"]["trend"] == {"field": "order_date", "grain": "week"}
        assert tiles[2]["kpi"]["comparison"] == "previous_period"
        assert tiles[3]["kpi"]["trend"] == {"field": "order_date", "grain": "auto"}

    def test_kpi_saved_before_trends_loads_without_one(self, client):
        from flowfile_core.schemas.catalog_schema import DashboardLayout

        old_kpi = {
            "field": "amount",
            "agg": "sum",
            "label": None,
            "prefix": None,
            "suffix": None,
            "decimals": None,
            "compact": True,
            "comparison": "none",
            "target": None,
            "higher_is_better": True,
            "value_size": "auto",
        }
        layout = DashboardLayout.model_validate(
            {"tiles": [{"id": "k", "type": "kpi", "viz_id": 1, "kpi": old_kpi, "x": 0, "y": 0, "w": 12, "h": 3}]}
        )
        assert layout.tiles[0].kpi.trend is None
        assert layout.tiles[0].kpi.model_dump(exclude={"trend"}) == old_kpi

    def test_filter_can_target_kpi_tile(self, client):
        layout = _layout_with_two_tiles()
        layout["tiles"].append(
            {"id": "kpi-1", "type": "kpi", "viz_id": 1, "kpi": {"field": "amount"}, "x": 0, "y": 6, "w": 3, "h": 2}
        )
        layout["filters"] = [
            {
                "id": "f1",
                "field_name": "region",
                "kind": "categorical",
                "state": {"selected": ["US"]},
                "target": "tiles",
                "target_tile_ids": ["kpi-1"],
            }
        ]
        resp = client.post("/catalog/dashboards", json={"name": "KPI filter", "layout": layout})
        assert resp.status_code == 201, resp.text
        filters = client.get(f"/catalog/dashboards/{resp.json()['id']}").json()["layout"]["filters"]
        assert filters[0]["target_tile_ids"] == ["kpi-1"]

    def test_create_with_table_tiles(self, client):
        table_id = _make_table()
        table_config = {
            "columns": [
                {"field": "region", "label": "Region", "decimals": None, "thousands": True},
                {"field": "amount", "label": None, "decimals": 2, "thousands": False},
            ],
            "sort_by": ["amount", "region"],
            "sort_direction": "descending",
            "page_size": 50,
        }
        layout = _layout_with_two_tiles()
        layout["tiles"] += [
            {"id": "tbl-1", "type": "table", "table_id": table_id, "table": table_config, "x": 0, "y": 6, "w": 6, "h": 6},
            {"id": "tbl-2", "type": "table", "viz_id": 1, "x": 6, "y": 6, "w": 6, "h": 6},
            {"id": "tbl-3", "type": "table", "x": 0, "y": 12, "w": 6, "h": 6},
            {"id": "kpi-t", "type": "kpi", "table_id": table_id, "kpi": {"agg": "count"}, "x": 6, "y": 12, "w": 3, "h": 2},
        ]
        resp = client.post("/catalog/dashboards", json={"name": "With table", "layout": layout})
        assert resp.status_code == 201, resp.text
        tiles = client.get(f"/catalog/dashboards/{resp.json()['id']}").json()["layout"]["tiles"]
        assert (tiles[2]["type"], tiles[2]["table_id"], tiles[2]["viz_id"]) == ("table", table_id, None)
        assert tiles[2]["table"] == table_config
        # A chart's source, and a table still to be set up, keep the defaults.
        assert (tiles[3]["viz_id"], tiles[3]["table_id"], tiles[3]["table"]) == (1, None, None)
        assert (tiles[4]["viz_id"], tiles[4]["table_id"]) == (None, None)
        assert (tiles[5]["table_id"], tiles[5]["viz_id"], tiles[5]["kpi"]["agg"]) == (table_id, None, "count")

    def test_layout_saved_before_table_tiles_loads_unchanged(self, client):
        layout = _layout_with_two_tiles()
        with get_db_context() as db:
            row = CatalogDashboard(name="Old", layout_json=json.dumps(layout), layout_version=1, created_by=1)
            db.add(row)
            db.commit()
            db.refresh(row)
            dash_id = row.id
        tiles = client.get(f"/catalog/dashboards/{dash_id}").json()["layout"]["tiles"]
        assert [(t["viz_id"], t["table_id"], t["table"]) for t in tiles] == [(1, None, None), (2, None, None)]

    @pytest.mark.parametrize(
        "bad_tile",
        [
            {"type": "table", "viz_id": 1, "table_id": 2},
            {"type": "kpi", "viz_id": 1, "table_id": 2, "kpi": {"agg": "count"}},
            {"type": "table", "table_id": 2, "table": {"page_size": 0}},
            {"type": "table", "table_id": 2, "table": {"page_size": 501}},
            {"type": "table", "table_id": 2, "table": {"sort_direction": "up"}},
            {"type": "table", "table_id": 2, "table": {"columns": [{"field": "a", "decimals": 7}]}},
            {"type": "table", "table_id": 2, "table": {"columns": [{"field": ""}]}},
        ],
    )
    def test_invalid_table_tile_returns_422(self, client, bad_tile):
        layout = _layout_with_two_tiles()
        layout["tiles"].append({"id": "bad", **bad_tile, "x": 0, "y": 6, "w": 6, "h": 6})
        resp = client.post("/catalog/dashboards", json={"name": "Bad table", "layout": layout})
        assert resp.status_code == 422

    def test_create_with_unknown_tile_type_returns_422(self, client):
        layout = _layout_with_two_tiles()
        layout["tiles"].append({"id": "bad", "type": "video", "x": 0, "y": 6, "w": 12, "h": 1})
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

        def fake_trigger(worker_source, column, limit, *, worker_url):
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


def _filter_tile(tile_id: str = "ft-1", filter_id: str = "f1", **extra) -> dict:
    return {"id": tile_id, "type": "filter", "filter_id": filter_id, "x": 0, "y": 6, "w": 12, "h": 2, **extra}


class TestDashboardFilterTiles:
    def test_filter_tile_round_trips(self, client):
        layout = _layout_with_two_tiles()
        layout["tiles"].append(_filter_tile())
        created = client.post("/catalog/dashboards", json={"name": "Placed filter", "layout": layout})
        assert created.status_code == 201, created.text
        tiles = client.get(f"/catalog/dashboards/{created.json()['id']}").json()["layout"]["tiles"]
        tile = tiles[2]
        assert (tile["type"], tile["filter_id"], tile["viz_id"], tile["table_id"]) == ("filter", "f1", None, None)
        assert (tile["x"], tile["y"], tile["w"], tile["h"]) == (0, 6, 12, 2)
        # Every other tile reads back with no filter.
        assert [t["filter_id"] for t in tiles[:2]] == [None, None]

    def test_layout_saved_before_filter_tiles_loads_unchanged(self, client):
        layout = _layout_with_two_tiles()
        with get_db_context() as db:
            row = CatalogDashboard(name="Old filters", layout_json=json.dumps(layout), layout_version=1, created_by=1)
            db.add(row)
            db.commit()
            db.refresh(row)
            dash_id = row.id
        body = client.get(f"/catalog/dashboards/{dash_id}").json()["layout"]
        assert [t["filter_id"] for t in body["tiles"]] == [None, None]
        assert [f["id"] for f in body["filters"]] == ["f1"]

    def test_filter_tile_without_a_filter_id_returns_422(self, client):
        layout = _layout_with_two_tiles()
        layout["tiles"].append(_filter_tile(filter_id=None))
        resp = client.post("/catalog/dashboards", json={"name": "No filter id", "layout": layout})
        assert resp.status_code == 422

    def test_filter_tile_on_an_unknown_filter_returns_422(self, client):
        layout = _layout_with_two_tiles()
        layout["tiles"].append(_filter_tile(filter_id="gone"))
        resp = client.post("/catalog/dashboards", json={"name": "Dangling", "layout": layout})
        assert resp.status_code == 422
        assert "unknown filter 'gone'" in resp.text

    def test_update_dropping_a_placed_filter_without_its_tile_returns_422(self, client):
        layout = _layout_with_two_tiles()
        layout["tiles"].append(_filter_tile())
        created = client.post("/catalog/dashboards", json={"name": "Drop", "layout": layout}).json()
        layout["filters"] = []
        resp = client.put(f"/catalog/dashboards/{created['id']}", json={"layout": layout})
        assert resp.status_code == 422
        # The filter and its tile go together.
        layout["tiles"] = layout["tiles"][:2]
        resp = client.put(f"/catalog/dashboards/{created['id']}", json={"layout": layout})
        assert resp.status_code == 200, resp.text

    def test_a_filter_placed_twice_returns_422(self, client):
        layout = _layout_with_two_tiles()
        layout["tiles"] += [_filter_tile("ft-1"), _filter_tile("ft-2", y=8)]
        resp = client.post("/catalog/dashboards", json={"name": "Twice", "layout": layout})
        assert resp.status_code == 422
        assert "more than once" in resp.text


def _png_bytes() -> bytes:
    """A real 1x1 transparent PNG."""

    def chunk(kind: bytes, data: bytes) -> bytes:
        return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", zlib.crc32(kind + data) & 0xFFFFFFFF)

    header = struct.pack(">IIBBBBB", 1, 1, 8, 6, 0, 0, 0)
    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", header)
        + chunk(b"IDAT", zlib.compress(b"\x00\x00\x00\x00\x00"))
        + chunk(b"IEND", b"")
    )


def _data_url(data: bytes, mime: str = "image/png") -> str:
    return f"data:{mime};base64,{base64.b64encode(data).decode()}"


def _image_tile(tile_id: str = "img-1", image: dict | None = None, **extra) -> dict:
    return {"id": tile_id, "type": "image", "image": image, "x": 0, "y": 6, "w": 8, "h": 4, **extra}


def _padded_png(size: int) -> str:
    """A data URL whose bytes start like a PNG and take ``size`` bytes, for the caps."""
    signature = b"\x89PNG\r\n\x1a\n"
    return _data_url(signature + b"\x00" * (size - len(signature)))


class TestDashboardImageTiles:
    def test_image_tile_round_trips_through_create_update_and_get(self, client):
        png = _data_url(_png_bytes())
        layout = _layout_with_two_tiles()
        layout["tiles"].append(_image_tile(image={"data_url": png, "alt": "Acme logo"}))
        created = client.post("/catalog/dashboards", json={"name": "Logo", "layout": layout})
        assert created.status_code == 201, created.text
        dash_id = created.json()["id"]
        tile = client.get(f"/catalog/dashboards/{dash_id}").json()["layout"]["tiles"][2]
        assert tile["type"] == "image"
        assert tile["image"] == {"data_url": png, "alt": "Acme logo", "fit": "contain"}

        svg = _data_url(b'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 4 4"/>', "image/svg+xml")
        layout["tiles"][2]["image"] = {"data_url": svg, "alt": None, "fit": "cover"}
        layout["tiles"].append(_image_tile("img-2"))
        updated = client.put(f"/catalog/dashboards/{dash_id}", json={"layout": layout})
        assert updated.status_code == 200, updated.text
        tiles = client.get(f"/catalog/dashboards/{dash_id}").json()["layout"]["tiles"]
        assert tiles[2]["image"] == {"data_url": svg, "alt": None, "fit": "cover"}
        # An image tile still to be filled is stored empty.
        assert (tiles[3]["type"], tiles[3]["image"]) == ("image", None)
        assert [t["image"] for t in tiles[:2]] == [None, None]

    def test_layout_saved_before_image_tiles_loads_unchanged(self, client):
        layout = _layout_with_two_tiles()
        with get_db_context() as db:
            row = CatalogDashboard(name="Old images", layout_json=json.dumps(layout), layout_version=1, created_by=1)
            db.add(row)
            db.commit()
            db.refresh(row)
            dash_id = row.id
        tiles = client.get(f"/catalog/dashboards/{dash_id}").json()["layout"]["tiles"]
        assert [t["image"] for t in tiles] == [None, None]

    @pytest.mark.parametrize(
        "data_url, message",
        [
            ("data:text/plain;base64,aGVsbG8=", "not a PNG, JPEG, WebP, GIF or SVG image"),
            ("https://example.com/logo.png", "not a PNG, JPEG, WebP, GIF or SVG image"),
            ("data:image/png,%89PNG", "not a PNG, JPEG, WebP, GIF or SVG image"),
            ("data:image/png;base64,iVBORw0K!!!", "not valid base64"),
            (_data_url(b"GIF89a" + b"\x00" * 16), "does not hold PNG data"),
            (_data_url(b"<html><script>alert(1)</script></html>", "image/svg+xml"), "does not hold SVG data"),
        ],
    )
    def test_an_image_that_is_not_one_returns_422(self, client, data_url, message):
        layout = _layout_with_two_tiles()
        layout["tiles"].append(_image_tile(image={"data_url": data_url}))
        resp = client.post("/catalog/dashboards", json={"name": "Not an image", "layout": layout})
        assert resp.status_code == 422
        assert message in resp.text

    def test_an_image_over_its_cap_returns_422_naming_both_sizes(self, client):
        layout = _layout_with_two_tiles()
        layout["tiles"].append(_image_tile(image={"data_url": _padded_png(300 * 1024 + 1)}))
        resp = client.post("/catalog/dashboards", json={"name": "Big", "layout": layout})
        assert resp.status_code == 422
        assert "the image is 301 KB; an image may be at most 300 KB" in resp.text
        layout["tiles"][2]["image"] = {"data_url": _padded_png(300 * 1024)}
        resp = client.post("/catalog/dashboards", json={"name": "At the cap", "layout": layout})
        assert resp.status_code == 201, resp.text

    def test_a_dashboard_over_the_total_cap_returns_422(self, client):
        layout = _layout_with_two_tiles()
        image = {"data_url": _padded_png(299 * 1024)}
        layout["tiles"] += [_image_tile(f"img-{i}", image=image, y=6 + i) for i in range(6)]
        created = client.post("/catalog/dashboards", json={"name": "Six", "layout": layout})
        assert created.status_code == 201, created.text
        layout["tiles"].append(_image_tile("img-6", image=image, y=12))
        resp = client.put(f"/catalog/dashboards/{created.json()['id']}", json={"layout": layout})
        assert resp.status_code == 422
        assert "the images on this dashboard take 2.1 MB; a dashboard may hold at most 2 MB" in resp.text

    def test_alt_text_over_300_characters_returns_422(self, client):
        layout = _layout_with_two_tiles()
        layout["tiles"].append(_image_tile(image={"data_url": _data_url(_png_bytes()), "alt": "x" * 301}))
        resp = client.post("/catalog/dashboards", json={"name": "Long alt", "layout": layout})
        assert resp.status_code == 422


def _tabbed_layout() -> dict:
    layout = _layout_with_two_tiles()
    layout["tabs"] = [{"id": "tab-a", "title": "Overview"}, {"id": "tab-b", "title": "Sales"}]
    layout["tiles"][0]["tab_id"] = "tab-a"
    layout["tiles"][1].update({"tab_id": "tab-b", "x": 0})
    return layout


class TestDashboardTabs:
    def test_tabs_and_tile_tabs_round_trip(self, client):
        created = client.post("/catalog/dashboards", json={"name": "Tabbed", "layout": _tabbed_layout()})
        assert created.status_code == 201, created.text
        body = client.get(f"/catalog/dashboards/{created.json()['id']}").json()["layout"]
        assert body["tabs"] == [{"id": "tab-a", "title": "Overview"}, {"id": "tab-b", "title": "Sales"}]
        assert [(t["id"], t["tab_id"], t["x"]) for t in body["tiles"]] == [("t1", "tab-a", 0), ("t2", "tab-b", 0)]

    def test_layout_saved_before_tabs_loads_as_one_canvas(self, client):
        with get_db_context() as db:
            row = CatalogDashboard(
                name="Old canvas", layout_json=json.dumps(_layout_with_two_tiles()), layout_version=1, created_by=1
            )
            db.add(row)
            db.commit()
            db.refresh(row)
            dash_id = row.id
        body = client.get(f"/catalog/dashboards/{dash_id}").json()["layout"]
        assert body["tabs"] == []
        assert [t["tab_id"] for t in body["tiles"]] == [None, None]

    def test_a_stored_tile_on_an_unknown_tab_still_reads(self, client):
        layout = _tabbed_layout()
        layout["tiles"][1]["tab_id"] = "gone"
        with get_db_context() as db:
            row = CatalogDashboard(name="Stray", layout_json=json.dumps(layout), layout_version=1, created_by=1)
            db.add(row)
            db.commit()
            db.refresh(row)
            dash_id = row.id
        resp = client.get(f"/catalog/dashboards/{dash_id}")
        assert resp.status_code == 200
        assert resp.json()["layout"]["tiles"][1]["tab_id"] == "gone"

    def test_duplicate_tab_ids_return_422(self, client):
        layout = _tabbed_layout()
        layout["tabs"][1]["id"] = "tab-a"
        layout["tiles"][1]["tab_id"] = "tab-a"
        resp = client.post("/catalog/dashboards", json={"name": "Twice", "layout": layout})
        assert resp.status_code == 422
        assert "tab 'tab-a' appears more than once" in resp.text

    @pytest.mark.parametrize(
        ("tab_id", "message"),
        [("gone", "is on unknown tab 'gone'"), (None, "is on no tab")],
    )
    def test_saving_a_tile_off_the_tabs_returns_422(self, client, tab_id, message):
        created = client.post("/catalog/dashboards", json={"name": "Tabbed", "layout": _tabbed_layout()}).json()
        layout = _tabbed_layout()
        layout["tiles"][1]["tab_id"] = tab_id
        resp = client.put(f"/catalog/dashboards/{created['id']}", json={"layout": layout})
        assert resp.status_code == 422
        assert message in resp.text

    def test_a_tile_on_a_tab_of_a_layout_without_tabs_returns_422(self, client):
        layout = _layout_with_two_tiles()
        layout["tiles"][0]["tab_id"] = "tab-a"
        resp = client.post("/catalog/dashboards", json={"name": "No tabs", "layout": layout})
        assert resp.status_code == 422
        assert "the layout has no tabs" in resp.text

    @pytest.mark.parametrize("bad_tab", [{"id": "", "title": "Empty id"}, {"id": "t", "title": ""}, {"id": "t"}])
    def test_invalid_tab_returns_422(self, client, bad_tab):
        layout = _empty_layout()
        layout["tabs"] = [bad_tab]
        resp = client.post("/catalog/dashboards", json={"name": "Bad tab", "layout": layout})
        assert resp.status_code == 422

    def test_a_filter_targets_tiles_on_every_tab(self, client):
        layout = _tabbed_layout()
        layout["filters"][0].update({"target": "tiles", "target_tile_ids": ["t1", "t2"]})
        layout["tiles"].append(_filter_tile(tab_id="tab-b"))
        created = client.post("/catalog/dashboards", json={"name": "Filtered tabs", "layout": layout})
        assert created.status_code == 201, created.text
        body = created.json()["layout"]
        assert body["filters"][0]["target_tile_ids"] == ["t1", "t2"]
        assert body["tiles"][2]["tab_id"] == "tab-b"


def _make_viz(source_type: str = "sql", table_id: int | None = None) -> int:
    with get_db_context() as db:
        viz = CatalogVisualization(
            name=f"chart-{source_type}-{time.time_ns()}",
            created_by=1,
            source_type=source_type,
            sql_query="SELECT region, amount FROM sales" if source_type == "sql" else None,
            catalog_table_id=table_id if source_type == "table" else None,
            spec_json=json.dumps([{"encodings": {}}]),
        )
        db.add(viz)
        db.commit()
        db.refresh(viz)
        return viz.id


def _region_filter(**extra) -> dict:
    return {
        "id": "f1",
        "field_name": "region",
        "kind": "categorical",
        "state": {"selected": []},
        "target": "all",
        "target_tile_ids": [],
        **extra,
    }


class TestDashboardFilterSingleSelect:
    def test_multiple_defaults_to_true_and_round_trips_false(self, client):
        layout = _layout_with_two_tiles()
        layout["filters"] = [_region_filter(), _region_filter(id="f2", multiple=False, state={"selected": ["EU"]})]
        created = client.post("/catalog/dashboards", json={"name": "Single", "layout": layout})
        assert created.status_code == 201, created.text
        filters = client.get(f"/catalog/dashboards/{created.json()['id']}").json()["layout"]["filters"]
        assert [f["multiple"] for f in filters] == [True, False]
        assert filters[1]["state"] == {"selected": ["EU"]}

    def test_layout_saved_before_single_select_loads_unchanged(self, client):
        layout = _layout_with_two_tiles()
        with get_db_context() as db:
            row = CatalogDashboard(name="Old", layout_json=json.dumps(layout), layout_version=1, created_by=1)
            db.add(row)
            db.commit()
            db.refresh(row)
            dash_id = row.id
        flt = client.get(f"/catalog/dashboards/{dash_id}").json()["layout"]["filters"][0]
        assert (flt["multiple"], flt["datasource_viz_id"], flt["state"]) == (True, None, {"selected": ["US", "EU"]})


class TestDashboardFilterChartSource:
    def test_filter_on_a_sql_chart_round_trips(self, client):
        viz_id = _make_viz("sql")
        layout = _layout_with_two_tiles()
        layout["filters"] = [_region_filter(datasource_viz_id=viz_id)]
        created = client.post("/catalog/dashboards", json={"name": "SQL source", "layout": layout})
        assert created.status_code == 201, created.text
        flt = client.get(f"/catalog/dashboards/{created.json()['id']}").json()["layout"]["filters"][0]
        assert (flt["datasource_viz_id"], flt["datasource_id"]) == (viz_id, None)

    def test_table_and_chart_together_returns_422(self, client):
        layout = _layout_with_two_tiles()
        layout["filters"] = [_region_filter(datasource_id=_make_table(), datasource_viz_id=_make_viz("sql"))]
        resp = client.post("/catalog/dashboards", json={"name": "Both", "layout": layout})
        assert resp.status_code == 422
        assert "not both" in resp.text

    def test_unknown_chart_returns_422(self, client):
        layout = _layout_with_two_tiles()
        layout["filters"] = [_region_filter(datasource_viz_id=99999)]
        resp = client.post("/catalog/dashboards", json={"name": "Gone", "layout": layout})
        assert resp.status_code == 422
        assert "unknown visualization_id=99999" in resp.text

    def test_chart_on_a_table_returns_422(self, client):
        viz_id = _make_viz("table", table_id=_make_table())
        layout = _layout_with_two_tiles()
        layout["filters"] = [_region_filter(datasource_viz_id=viz_id)]
        resp = client.post("/catalog/dashboards", json={"name": "Table chart", "layout": layout})
        assert resp.status_code == 422
        assert "bind the table" in resp.text

    def test_update_onto_an_unknown_chart_returns_422(self, client):
        created = client.post("/catalog/dashboards", json={"name": "Up", "layout": _layout_with_two_tiles()}).json()
        layout = _layout_with_two_tiles()
        layout["filters"] = [_region_filter(datasource_viz_id=99999)]
        resp = client.put(f"/catalog/dashboards/{created['id']}", json={"layout": layout})
        assert resp.status_code == 422
