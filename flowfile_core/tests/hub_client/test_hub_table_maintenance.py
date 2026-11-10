"""A table in the hub's catalog maintained from the Catalog page as a local one is: optimize, vacuum.

Each test goes through core's own route by the table's negative id, against the fake hub served over loopback and
the hub's table ``orders`` on MinIO (the fixtures of ``test_hub_tables_e2e``), and asserts what the route answers,
what is on storage afterwards, and what the hub was asked: a vend that may write the table's schema, and the row's
new facts on its own refresh route. The hub runs none of it: its own optimize and vacuum routes are never called.
"""

import polars as pl
import pytest
from deltalake import DeltaTable

from tests.flowfile.execution import test_hub_tables_e2e as e2e
from tests.hub_client import test_hub_table_reads as reads
from tests.test_catalog_cloud_virtual import requires_minio

pytestmark = [requires_minio, pytest.mark.usefixtures("running_worker")]
serve_fake = e2e.serve_fake
store = e2e.store
hub = e2e.hub
api = reads.api


@pytest.fixture
def orders(hub) -> dict:
    """``orders`` with a second commit, so it has two files to compact, and the counts the hub keeps on its row."""
    pl.DataFrame({"id": [6], "country": ["FR"], "amount": [60]}).write_delta(
        hub.orders["file_path"], mode="append", storage_options=e2e.MINIO_OPTIONS
    )
    hub.orders.update(row_count=6, column_count=3, size_bytes=1)
    return hub.orders


def _files(orders: dict) -> int:
    return len(DeltaTable(orders["file_path"], storage_options=e2e.MINIO_OPTIONS).file_uris())


def _objects(orders: dict) -> list[str]:
    prefix = orders["file_path"].split(f"{e2e._BUCKET}/", 1)[1]
    listed = e2e.get_minio_client().list_objects_v2(Bucket=e2e._BUCKET, Prefix=f"{prefix}/").get("Contents", [])
    return [obj["Key"] for obj in listed if obj["Key"].endswith(".parquet")]


def _hub_maintenance_calls(fake) -> list[tuple[str, str]]:
    return [call for call in fake.calls if call[1].endswith(("/optimize", "/vacuum"))]


def test_a_hub_table_is_optimized_on_this_cores_worker(api, hub, orders):
    assert _files(orders) == 2

    resp = api.post(f"/catalog/tables/{-orders['id']}/optimize", json={})

    assert resp.status_code == 200, resp.text
    assert resp.json()["size_bytes"] > 1
    assert _files(orders) == 1
    assert hub.fake.vend_calls[-1]["write_namespace_ids"] == [hub.sales["id"]]
    table_id, facts = hub.fake.refreshed[-1]
    assert table_id == orders["id"]
    assert facts["size_bytes"] == resp.json()["size_bytes"] == orders["size_bytes"]
    assert facts["row_count"] == 6 and "table_path" not in facts
    assert _hub_maintenance_calls(hub.fake) == []


def test_a_hub_table_is_vacuumed_on_this_cores_worker(api, hub, orders):
    api.post(f"/catalog/tables/{-orders['id']}/optimize", json={}).raise_for_status()
    assert len(_objects(orders)) == 3
    refreshes = len(hub.fake.refreshed)

    listed = api.post(f"/catalog/tables/{-orders['id']}/vacuum", json={"retention_hours": 0})
    removed = api.post(f"/catalog/tables/{-orders['id']}/vacuum", json={"retention_hours": 0, "dry_run": False})

    assert listed.status_code == 200, listed.text
    assert listed.json()["file_count"] == 2 and listed.json()["dry_run"] is True
    assert removed.status_code == 200, removed.text
    assert removed.json()["file_count"] == 2
    assert len(_objects(orders)) == 1
    assert len(hub.fake.refreshed) == refreshes + 1
    assert hub.fake.refreshed[-1][1]["size_bytes"] == removed.json()["size_bytes"]
    assert _hub_maintenance_calls(hub.fake) == []


def test_a_person_the_hub_gives_no_write_cannot_optimize_its_table(api, hub, orders):
    hub.fake.grants.update({hub.sales["id"]: "use"})

    resp = api.post(f"/catalog/tables/{-orders['id']}/optimize", json={})

    assert resp.status_code == 403, resp.text
    assert "manage" in resp.json()["detail"]
    assert _files(orders) == 2
    assert hub.fake.refreshed == []
