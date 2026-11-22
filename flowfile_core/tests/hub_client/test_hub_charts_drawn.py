"""A chart saved in the hub's catalog, drawn on this Flowfile: the hub keeps the chart's row and computes nothing.

Every test goes through the HTTP route against the fake hub served over loopback, with the hub's table ``orders``
written to MinIO (the fixtures of ``test_hub_tables_e2e``). The server asks the hub for the chart's row as the person,
then reads the table through a vend as any read of a hub table, and its own worker computes.

The module skips when MinIO is not available, and every test skips without the session worker, which reads the rows.
"""

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from tests.flowfile.execution import test_hub_tables_e2e as e2e
from tests.test_catalog_cloud_virtual import requires_minio

pytestmark = [requires_minio, pytest.mark.usefixtures("running_worker")]
serve_fake = e2e.serve_fake
store = e2e.store
hub = e2e.hub

COUNT_ROWS = {"workflow": [{"type": "view", "query": [{"op": "raw", "fields": ["country", "amount"]}]}]}


@pytest.fixture
def api() -> TestClient:
    """A client signed in as user 1, the person ``hub`` connects."""
    client = TestClient(main.app)
    token = client.post("/auth/token").json()["access_token"]
    client.headers = {"Authorization": f"Bearer {token}"}
    return client


def _asked_to_compute(fake) -> list[tuple[str, str]]:
    return [(method, path) for method, path in fake.calls if path.endswith(("/compute", "/fields", "/stats"))]


@pytest.fixture(params=["table", "sql"])
def chart(request, hub) -> dict:
    """A chart in the hub's catalog: of the hub table ``orders`` by its id, or of a query over it."""
    if request.param == "table":
        return hub.fake.seed_chart(hub.sales, "orders by country", hub.orders)
    query = f"SELECT id, country, amount FROM {hub.orders['qualified_name']}"
    return hub.fake.seed_chart(hub.sales, "orders by query", sql_query=query)


def test_a_saved_hub_chart_gets_its_fields_here(api, hub, chart):
    resp = api.post(f"/catalog/visualizations/{-chart['id']}/fields")

    assert resp.status_code == 200, resp.text
    answer = resp.json()
    assert answer["error"] is None
    assert {field["fid"] for field in answer["fields"]} == {"id", "country", "amount"}
    assert ("GET", f"/catalog/visualizations/{chart['id']}") in hub.fake.calls
    assert _asked_to_compute(hub.fake) == []
    assert [call["read_namespace_ids"] for call in hub.fake.vend_calls] == [[hub.sales["id"]]]


def test_a_saved_hub_chart_is_drawn_here(api, hub, chart):
    resp = api.post(f"/catalog/visualizations/{-chart['id']}/compute", json={"payload": COUNT_ROWS, "max_rows": 100})

    assert resp.status_code == 200, resp.text
    answer = resp.json()
    assert answer["error"] is None
    assert sorted((row["country"], row["amount"]) for row in answer["rows"]) == sorted(
        zip(e2e.ORDERS["country"], e2e.ORDERS["amount"], strict=True)
    )
    assert _asked_to_compute(hub.fake) == []
    assert [call["write_namespace_ids"] for call in hub.fake.vend_calls] == [[]]


def test_a_dashboard_filter_reads_the_values_of_a_hub_tables_column_here(api, hub):
    resp = api.get(f"/catalog/tables/{-hub.orders['id']}/columns/country/stats", params={"limit": 10})

    assert resp.status_code == 200, resp.text
    assert sorted(resp.json()["values"]) == ["BR", "DE", "NL", "US"]
    assert _asked_to_compute(hub.fake) == []
