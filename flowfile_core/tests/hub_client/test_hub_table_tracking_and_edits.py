"""A table in the hub's catalog tracked and edited from the Catalog page as a local one is: Track changes, its
change-tracking status, Edit data and the key column the edit surface mints.

Each test goes through core's own route by the table's negative id, against the fake hub served over loopback and the
hub's table ``orders`` on MinIO (the fixtures of ``test_hub_tables_e2e``), and asserts what the route answers, what is
on storage afterwards and what the hub was asked: a vend that may write the table's schema, and the row's new facts on
its own refresh route. The hub's own change-tracking, edit and key-column routes are never called.
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


@pytest.fixture(autouse=True)
def _counted(hub):
    """The facts the hub keeps on ``orders``' row, which its refresh route takes only whole."""
    hub.orders.update(row_count=5, column_count=3, size_bytes=1)


def _delta(orders: dict) -> DeltaTable:
    return DeltaTable(orders["file_path"], storage_options=e2e.MINIO_OPTIONS)


def _rows(orders: dict) -> pl.DataFrame:
    return pl.read_delta(orders["file_path"], storage_options=e2e.MINIO_OPTIONS).sort("id")


def _hub_storage_calls(fake) -> list[tuple[str, str]]:
    """The hub's routes that read or change a table's storage that this core called; the list of the cursors the hub
    keeps (``GET .../cdc``) is not one, and reads no storage there."""
    routes = ("/cdc", "/cdc/enable", "/edits", "/key-column")
    return [call for call in fake.calls if call[1].endswith(routes) and call[0] != "GET"]


def test_change_tracking_is_turned_on_for_a_hub_table_on_this_core(api, hub):
    orders = hub.orders

    resp = api.post(f"/catalog/tables/{-orders['id']}/cdc/enable")

    assert resp.status_code == 200, resp.text
    status = resp.json()
    assert status["cdc_enabled"] is True
    assert _delta(orders).metadata().configuration.get("delta.enableChangeDataFeed") == "true"
    assert status["cdc_enabled_version"] == status["current_version"] == _delta(orders).version()
    assert hub.fake.vend_calls[0]["write_namespace_ids"] == [hub.sales["id"]]
    table_id, facts = hub.fake.refreshed[-1]
    assert table_id == orders["id"] and "table_path" not in facts
    assert facts["cdc_enabled_version"] == status["cdc_enabled_version"]
    assert facts["row_count"] == orders["row_count"] and facts["schema_columns"] == orders["schema_columns"]
    assert orders["cdc_enabled"] is True
    assert _hub_storage_calls(hub.fake) == []


def test_a_hub_tables_change_tracking_shows_its_rows_state_and_the_live_head(api, hub):
    orders = hub.orders
    api.post(f"/catalog/tables/{-orders['id']}/cdc/enable").raise_for_status()
    floor = orders["cdc_enabled_version"]
    pl.DataFrame({"id": [6], "country": ["FR"], "amount": [60]}).write_delta(
        orders["file_path"], mode="append", storage_options=e2e.MINIO_OPTIONS
    )

    resp = api.get(f"/catalog/tables/{-orders['id']}/cdc")

    assert resp.status_code == 200, resp.text
    assert resp.json() == {
        "cdc_enabled": True,
        "cdc_enabled_version": floor,
        "current_version": floor + 1,
        "cursors": [],
    }
    assert _hub_storage_calls(hub.fake) == []


def test_a_hub_tables_rows_are_edited_on_this_cores_worker(api, hub):
    orders = hub.orders
    version = _delta(orders).version()
    body = {
        "key_columns": ["id"],
        "expected_version": version,
        "upsert_columns": ["id", "country", "amount"],
        "upsert_rows": [[2, "US", 99], [6, "FR", 60]],
        "delete_keys": [[5]],
    }

    resp = api.post(f"/catalog/tables/{-orders['id']}/edits", json=body)

    assert resp.status_code == 200, resp.text
    answer = resp.json()
    assert answer["rows_upserted"] == 2 and answer["rows_deleted"] == 1
    assert answer["table"]["id"] == -orders["id"] and answer["table"]["source"] == "hub"
    assert answer["table"]["row_count"] == 5
    rows = _rows(orders)
    assert rows["id"].to_list() == [1, 2, 3, 4, 6]
    assert rows.filter(pl.col("id") == 2)["amount"].item() == 99
    assert hub.fake.vend_calls[-1]["write_namespace_ids"] == [hub.sales["id"]]
    table_id, facts = hub.fake.refreshed[-1]
    assert table_id == orders["id"] and "table_path" not in facts
    assert facts["row_count"] == 5 and facts["size_bytes"] > 0
    assert [c["name"] for c in facts["schema_columns"]] == ["id", "country", "amount"]
    assert _hub_storage_calls(hub.fake) == []


def test_an_edit_of_a_hub_table_that_moved_is_refused_as_a_local_one_is(api, hub):
    orders = hub.orders
    body = {"key_columns": ["id"], "expected_version": 0, "upsert_columns": ["id", "amount"], "upsert_rows": [[1, 7]]}
    pl.DataFrame({"id": [6], "country": ["FR"], "amount": [60]}).write_delta(
        orders["file_path"], mode="append", storage_options=e2e.MINIO_OPTIONS
    )

    resp = api.post(f"/catalog/tables/{-orders['id']}/edits", json=body)

    assert resp.status_code == 409, resp.text
    assert resp.json()["detail"]["error"] == "stale_write"
    assert _rows(orders).filter(pl.col("id") == 1)["amount"].item() == 10


def test_a_hub_table_gets_a_key_column_on_this_cores_worker(api, hub):
    orders = hub.orders

    resp = api.post(f"/catalog/tables/{-orders['id']}/key-column", json={"column_name": "record_id"})

    assert resp.status_code == 200, resp.text
    assert resp.json()["table"]["id"] == -orders["id"]
    assert "record_id" in _rows(orders).columns
    facts = hub.fake.refreshed[-1][1]
    assert "record_id" in [c["name"] for c in facts["schema_columns"]]
    assert "record_id" in [c["name"] for c in orders["schema_columns"]]
    assert _hub_storage_calls(hub.fake) == []


@pytest.mark.parametrize(
    ("route", "body"),
    [
        ("cdc/enable", None),
        ("edits", {"key_columns": ["id"], "upsert_columns": ["id", "amount"], "upsert_rows": [[1, 7]]}),
        ("key-column", {"column_name": "record_id"}),
    ],
)
def test_a_person_the_hub_gives_no_write_is_refused_as_on_a_local_table(api, hub, route, body):
    orders = hub.orders
    version = _delta(orders).version()
    hub.fake.grants.update({hub.sales["id"]: "use"})

    resp = api.post(f"/catalog/tables/{-orders['id']}/{route}", json=body)

    assert resp.status_code == 403, resp.text
    assert "manage" in resp.json()["detail"]
    assert _delta(orders).version() == version
    assert hub.fake.refreshed == []
