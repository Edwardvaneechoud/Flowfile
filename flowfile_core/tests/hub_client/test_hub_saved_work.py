"""Notebooks, charts and dashboards saved in a schema of the hub's catalog, from a Flowfile signed in to the hub.

A save names a hub row by its negative id, so it goes to the hub's same route with every id the hub's own, and its
answer comes back with the ids negative again: a chart's table and dashboard, a dashboard's tiles and filters. A save
that names a row of this Flowfile's own catalog beside a hub row is refused here, before the hub is asked. The lists
of charts, dashboards and notebooks hold the hub's after this Flowfile's own.
"""

import httpx
import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.hub_client import catalog as hub_catalog
from flowfile_core.hub_client import client
from tests.hub_client.conftest import HUB_URL, NEVER_RUNS, FakeHubWithCatalog

CONNECT = {"hub_url": HUB_URL, "username": "ana", "password": "s3cret", "device_name": "ana-laptop"}
SPEC = [{"visId": "gw_1", "name": "Chart 1", "encodings": {"dimensions": [{"fid": "country"}]}}]


@pytest.fixture
def hub(monkeypatch) -> FakeHubWithCatalog:
    fake = FakeHubWithCatalog()
    monkeypatch.setattr(client, "_module_http", fake.http())
    return fake


@pytest.fixture
def api(store, hub) -> TestClient:
    """A person on this Flowfile, connected to the hub."""
    c = TestClient(main.app)
    token = c.post("/auth/token").json()["access_token"]
    c.headers["Authorization"] = f"Bearer {token}"
    assert c.post("/hub_client/connection", json=CONNECT).status_code == 200
    hub.calls.clear()
    return c


@pytest.fixture
def seeded(hub) -> dict:
    """The hub's catalog ``sales`` with a schema the person may write (``curated``), one they may only use (``raw``)
    and the table ``orders`` in it."""
    sales = hub.seed_catalog("sales", storage_uri="s3://acme/sales", storage_connection_name="acme-s3")
    raw = hub.seed_schema(sales, "raw")
    curated = hub.seed_schema(sales, "curated")
    hub.grants = {raw["id"]: "use", curated["id"]: "manage"}
    return {"raw": raw, "curated": curated, "orders": hub.seed_table(raw, "orders")}


def _chart_of_orders(seeded: dict, **fields) -> dict:
    return {
        "name": "orders by country",
        "spec": SPEC,
        "source_type": "table",
        "catalog_table_id": -seeded["orders"]["id"],
        "namespace_id": -seeded["curated"]["id"],
        **fields,
    }


def _sent(hub: FakeHubWithCatalog, method: str, path: str):
    """The one body the hub received on ``method`` ``path``."""
    [body] = [body for sent_method, sent_path, body in hub.saved if (sent_method, sent_path) == (method, path)]
    return body


def test_a_notebook_is_saved_opened_changed_and_deleted_in_a_hub_schema(api, hub, seeded):
    curated = seeded["curated"]
    cells = [{"id": "c1", "type": "sql", "source": "SELECT 1", "metadata": {"max_rows": 100}}]

    made = api.post("/catalog/notebooks", json={"name": "margins", "namespace_id": -curated["id"], "cells": cells})

    assert made.status_code == 201, made.text
    notebook = made.json()
    assert notebook["id"] < 0 and notebook["namespace_id"] == -curated["id"] and notebook["source"] == "hub"
    assert _sent(hub, "POST", "/catalog/notebooks")["namespace_id"] == curated["id"]
    their_id = -notebook["id"]

    opened = api.get(f"/catalog/notebooks/{notebook['id']}")
    changed = api.put(f"/catalog/notebooks/{notebook['id']}", json={"name": "margins by month"})

    assert opened.status_code == 200 and opened.json()["cells"] == cells
    assert changed.status_code == 200 and changed.json()["name"] == "margins by month"
    assert _sent(hub, "PUT", f"/catalog/notebooks/{their_id}") == {"name": "margins by month"}
    assert api.delete(f"/catalog/notebooks/{notebook['id']}").status_code == 204
    assert hub.notebooks == []


def test_a_chart_of_a_hub_table_is_saved_with_the_tables_own_id_on_the_hub(api, hub, seeded):
    orders, curated = seeded["orders"], seeded["curated"]

    made = api.post("/catalog/visualizations", json=_chart_of_orders(seeded))

    assert made.status_code == 201, made.text
    sent = _sent(hub, "POST", "/catalog/visualizations")
    assert (sent["catalog_table_id"], sent["namespace_id"]) == (orders["id"], curated["id"])
    chart = made.json()
    assert chart["id"] < 0 and chart["source"] == "hub"
    assert (chart["catalog_table_id"], chart["namespace_id"]) == (-orders["id"], -curated["id"])
    assert chart["spec"] == SPEC


def test_a_hub_table_lists_the_charts_saved_of_it(api, hub, seeded):
    made = api.post("/catalog/visualizations", json=_chart_of_orders(seeded)).json()

    listed = api.get(f"/catalog/tables/{-seeded['orders']['id']}/visualizations")

    assert listed.status_code == 200, listed.text
    assert [(chart["id"], chart["catalog_table_id"]) for chart in listed.json()] == [
        (made["id"], -seeded["orders"]["id"])
    ]


def test_a_dashboard_in_a_hub_schema_names_its_charts_and_tables_by_the_hubs_ids(api, hub, seeded):
    orders, curated = seeded["orders"], seeded["curated"]
    chart = hub.seed_chart(curated, "by country", orders)
    by_query = hub.seed_chart(curated, "by query", sql_query="SELECT * FROM sales.raw.orders")
    layout = {
        "tiles": [
            {"id": "t1", "type": "viz", "viz_id": -chart["id"], "x": 0, "y": 0, "w": 6, "h": 4},
            {"id": "t2", "type": "table", "table_id": -orders["id"], "x": 6, "y": 0, "w": 6, "h": 4},
        ],
        "filters": [
            {"id": "f1", "field_name": "country", "kind": "categorical", "datasource_id": -orders["id"]},
            {"id": "f2", "field_name": "amount", "kind": "numeric_range", "datasource_viz_id": -by_query["id"]},
        ],
    }

    made = api.post("/catalog/dashboards", json={"name": "sales", "namespace_id": -curated["id"], "layout": layout})

    assert made.status_code == 201, made.text
    sent = _sent(hub, "POST", "/catalog/dashboards")
    assert sent["namespace_id"] == curated["id"]
    assert [(t.get("viz_id"), t.get("table_id")) for t in sent["layout"]["tiles"]] == [
        (chart["id"], None),
        (None, orders["id"]),
    ]
    assert [(f.get("datasource_id"), f.get("datasource_viz_id")) for f in sent["layout"]["filters"]] == [
        (orders["id"], None),
        (None, by_query["id"]),
    ]
    dashboard = made.json()
    assert dashboard["id"] < 0 and dashboard["source"] == "hub"
    assert [(t["viz_id"], t["table_id"]) for t in dashboard["layout"]["tiles"]] == [
        (-chart["id"], None),
        (None, -orders["id"]),
    ]
    assert [(f["datasource_id"], f["datasource_viz_id"]) for f in dashboard["layout"]["filters"]] == [
        (-orders["id"], None),
        (None, -by_query["id"]),
    ]
    assert [t["id"] for t in dashboard["layout"]["tiles"]] == ["t1", "t2"]

    opened = api.get(f"/catalog/dashboards/{dashboard['id']}")
    saved = api.put(
        f"/catalog/dashboards/{dashboard['id']}", json={"layout": {**layout, "filters": []}, "expected_layout_version": 1}
    )

    assert opened.status_code == 200 and opened.json()["layout"] == dashboard["layout"]
    assert saved.status_code == 200, saved.text
    resent = _sent(hub, "PUT", f"/catalog/dashboards/{-dashboard['id']}")
    assert [t.get("viz_id") for t in resent["layout"]["tiles"]] == [chart["id"], None]


def test_a_chart_made_inside_a_hub_dashboard_belongs_to_it_by_the_hubs_id(api, hub, seeded):
    dashboard = hub.seed_dashboard(seeded["curated"], "sales")

    made = api.post("/catalog/visualizations", json=_chart_of_orders(seeded, dashboard_id=-dashboard["id"]))

    assert made.status_code == 201, made.text
    assert _sent(hub, "POST", "/catalog/visualizations")["dashboard_id"] == dashboard["id"]
    assert made.json()["dashboard_id"] == -dashboard["id"]


def test_every_reference_of_saved_work_is_the_hubs_on_the_way_out_and_negative_on_the_way_back():
    names = ("viz_id", "dashboard_id", "datasource_viz_id", "datasource_id", "catalog_table_id", "notebook_id")

    assert hub_catalog._theirs({name: -7 for name in names}) == {name: 7 for name in names}
    assert hub_catalog.marked({name: 7 for name in names}) == {name: -7 for name in names}


def test_a_schema_the_person_may_only_use_takes_no_save(api, hub, seeded):
    raw = seeded["raw"]

    answers = [
        api.post("/catalog/notebooks", json={"name": "n", "namespace_id": -raw["id"]}),
        api.post("/catalog/visualizations", json=_chart_of_orders(seeded, namespace_id=-raw["id"])),
        api.post("/catalog/dashboards", json={"name": "d", "namespace_id": -raw["id"]}),
    ]

    assert [answer.status_code for answer in answers] == [403, 403, 403]
    assert {answer.json()["detail"] for answer in answers} == {"Not authorized to create items in this namespace"}
    assert hub.notebooks == hub.charts == hub.dashboards == []


def _local_chart(api: TestClient) -> dict:
    made = api.post(
        "/catalog/visualizations",
        json={"name": "local query", "spec": SPEC, "source_type": "sql", "sql_query": "SELECT 1 AS n"},
    )
    assert made.status_code == 201, made.text
    return made.json()


def test_a_save_for_the_hub_that_names_a_row_of_this_flowfile_is_refused_before_the_hub_is_asked(api, hub, seeded):
    curated = seeded["curated"]
    own = _local_chart(api)
    on_hub = hub.seed_dashboard(curated, "sales")
    tile = {"id": "t1", "type": "viz", "viz_id": own["id"], "x": 0, "y": 0, "w": 6, "h": 4}
    of_a_table = {"id": "t2", "type": "table", "table_id": 7, "x": 0, "y": 4, "w": 6, "h": 4}
    hub.calls.clear()
    try:
        answers = [
            api.put(f"/catalog/visualizations/{own['id']}", json={"namespace_id": -curated["id"]}),
            api.post("/catalog/dashboards", json={"name": "d", "namespace_id": -curated["id"], "layout": {"tiles": [tile]}}),
            api.put(f"/catalog/dashboards/{-on_hub['id']}", json={"layout": {"tiles": [tile]}}),
            api.put(f"/catalog/dashboards/{-on_hub['id']}", json={"layout": {"tiles": [of_a_table]}}),
            api.post("/catalog/visualizations", json=_chart_of_orders(seeded, catalog_table_id=own["id"])),
            api.post("/catalog/visualizations", json=_chart_of_orders(seeded, dashboard_id=own["id"])),
            api.post("/catalog/visualizations", json=_chart_of_orders(seeded, namespace_id=2)),
        ]
        kept = api.get(f"/catalog/visualizations/{own['id']}").json()
    finally:
        api.delete(f"/catalog/visualizations/{own['id']}")

    assert [answer.status_code for answer in answers] == [422] * 7, [answer.text for answer in answers]
    assert all("own catalog" in answer.json()["detail"] for answer in answers)
    assert hub.calls == [] and hub.saved == []
    assert kept["namespace_id"] is None and hub.dashboards == [on_hub]


def test_the_hub_is_never_asked_to_draw_a_chart_through_the_relay(api, hub, seeded):
    """What the hub answers when asked: the refusal a server must never pass on as its own answer to a person."""
    chart = hub.seed_chart(seeded["curated"], "by country", seeded["orders"])
    resp = hub.http().post(
        f"{HUB_URL}/catalog/visualizations/{chart['id']}/compute",
        headers={"Authorization": f"Bearer {client.sign_in(HUB_URL, 'ana', 's3cret')['access_token']}"},
    )
    assert (resp.status_code, resp.json()["detail"]) == (403, NEVER_RUNS)


def test_a_hub_that_ended_the_sign_in_answers_400_and_never_401(api, hub, seeded):
    hub.sign_out_everywhere("ana")

    resp = api.post("/catalog/notebooks", json={"name": "n", "namespace_id": -seeded["curated"]["id"]})

    assert resp.status_code not in (401, 201), resp.text
    assert isinstance(resp.json()["detail"], str)


def test_without_a_hub_a_negative_id_is_a_row_this_flowfile_does_not_have(store, monkeypatch):
    def refuse(request):
        pytest.fail(f"the hub was called: {request.method} {request.url}")

    monkeypatch.setattr(client, "_module_http", httpx.Client(transport=httpx.MockTransport(refuse)))
    c = TestClient(main.app)
    c.headers["Authorization"] = f"Bearer {c.post('/auth/token').json()['access_token']}"

    resp = c.get("/catalog/notebooks/-5")

    assert resp.status_code == 400 and "not connected" in resp.json()["detail"]


def _drawing_requests(hub: FakeHubWithCatalog) -> list[tuple[str, str]]:
    return [(method, path) for method, path, _ in hub.saved if path.endswith(("/compute", "/fields"))]


@pytest.mark.parametrize("route", ["fields", "compute"])
def test_drawing_a_saved_hub_chart_asks_the_hub_for_its_row_and_for_nothing_it_would_compute(api, hub, seeded, route):
    """With no table on object storage in the query the request ends at this Flowfile's own worker, whatever it
    answers; the hub is asked for the chart's row only."""
    chart = hub.seed_chart(seeded["curated"], "one row", sql_query="SELECT 1 AS n")

    resp = api.post(f"/catalog/visualizations/{-chart['id']}/{route}", json={} if route == "compute" else None)

    assert resp.status_code != 403 and NEVER_RUNS not in resp.text, resp.text
    assert ("GET", f"/catalog/visualizations/{chart['id']}", None) in hub.saved
    assert _drawing_requests(hub) == []


@pytest.mark.parametrize("route", ["fields", "compute"])
def test_drawing_a_chart_the_hub_does_not_show_this_person_answers_as_the_hub_does(api, hub, seeded, route):
    resp = api.post(f"/catalog/visualizations/-999/{route}")

    assert resp.status_code == 404, resp.text
    assert _drawing_requests(hub) == []


def test_the_lists_hold_the_hubs_saved_work_after_this_flowfiles_own(api, hub, seeded):
    curated, orders = seeded["curated"], seeded["orders"]
    own = _local_chart(api)
    chart = hub.seed_chart(curated, "by country", orders)
    dashboard = hub.seed_dashboard(curated, "sales")
    notebook = hub.seed_notebook(curated, "margins")
    try:
        charts = api.get("/catalog/visualizations")
        dashboards = api.get("/catalog/dashboards")
        notebooks = api.get("/catalog/notebooks")
    finally:
        api.delete(f"/catalog/visualizations/{own['id']}")

    assert (charts.status_code, dashboards.status_code, notebooks.status_code) == (200, 200, 200)
    listed = [(row["id"], row["source"]) for row in charts.json()]
    assert (own["id"], "local") in listed and listed[-1] == (-chart["id"], "hub")
    assert listed.index((own["id"], "local")) < listed.index((-chart["id"], "hub"))
    assert charts.json()[-1]["catalog_table_id"] == -orders["id"]
    assert [(row["id"], row["source"]) for row in dashboards.json() if row["id"] < 0] == [(-dashboard["id"], "hub")]
    assert [(row["id"], row["source"]) for row in notebooks.json() if row["id"] < 0] == [(-notebook["id"], "hub")]


def test_the_tree_marks_a_hub_schemas_charts_and_notebooks_as_the_hubs(api, hub, seeded):
    curated, orders = seeded["curated"], seeded["orders"]
    chart = hub.seed_chart(curated, "by country", orders)
    notebook = hub.seed_notebook(curated, "margins")
    summary = {key: value for key, value in notebook.items() if key != "cells"}
    hub.tree_answer = [
        {**root, "children": [{**schema, "visualizations": [chart], "notebooks": [summary]} for schema in root["children"]]}
        for root in hub.tree()
    ]

    tree = api.get("/catalog/namespaces/tree").json()

    schema = next(s for root in tree for s in root["children"] if s["id"] == -curated["id"])
    assert [(c["id"], c["source"], c["catalog_table_id"]) for c in schema["visualizations"]] == [
        (-chart["id"], "hub", -orders["id"])
    ]
    assert [(n["id"], n["source"]) for n in schema["notebooks"]] == [(-notebook["id"], "hub")]


def test_without_a_hub_the_lists_are_this_flowfiles_own_and_no_hub_is_asked(store, monkeypatch):
    def refuse(request):
        pytest.fail(f"the hub was called: {request.method} {request.url}")

    monkeypatch.setattr(client, "_module_http", httpx.Client(transport=httpx.MockTransport(refuse)))
    c = TestClient(main.app)
    c.headers["Authorization"] = f"Bearer {c.post('/auth/token').json()['access_token']}"

    own = _local_chart(c)
    try:
        answers = [c.get(path) for path in ("/catalog/visualizations", "/catalog/dashboards", "/catalog/notebooks")]
    finally:
        c.delete(f"/catalog/visualizations/{own['id']}")

    assert [answer.status_code for answer in answers] == [200, 200, 200]
    assert (own["id"], "local") in [(row["id"], row["source"]) for row in answers[0].json()]
    assert all(row["id"] > 0 and row["source"] == "local" for answer in answers for row in answer.json())


GEO_SPEC = [
    {
        "visId": "gw_map",
        "id": 7,
        "layout": {"geojson": {"features": [{"type": "Feature", "id": 12, "properties": {"table_id": 3}}]}},
    }
]


def test_a_charts_spec_and_a_notebooks_cells_cross_the_relay_as_they_are(api, hub, seeded):
    """Both are documents of their own: an ``id`` or ``table_id`` inside them names no catalog row."""
    curated = seeded["curated"]
    cells = [{"id": "c1", "type": "python", "source": "x = 1", "metadata": {"table_id": 5, "id": 9}}]

    chart = api.post("/catalog/visualizations", json=_chart_of_orders(seeded, spec=GEO_SPEC))
    notebook = api.post("/catalog/notebooks", json={"name": "n", "namespace_id": -curated["id"], "cells": cells})

    assert (chart.status_code, notebook.status_code) == (201, 201), (chart.text, notebook.text)
    assert _sent(hub, "POST", "/catalog/visualizations")["spec"] == GEO_SPEC
    assert _sent(hub, "POST", "/catalog/notebooks")["cells"] == cells
    assert chart.json()["spec"] == GEO_SPEC
    assert notebook.json()["cells"] == cells
    assert api.get(f"/catalog/visualizations/{chart.json()['id']}").json()["spec"] == GEO_SPEC
