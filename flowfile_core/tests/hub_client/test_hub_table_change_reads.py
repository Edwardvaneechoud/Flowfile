"""A flow reads the changes of a table in the hub's catalog since its last run, as it reads a local table's.

The flow is a copy of a hub flow (its ``hub_origin``): a catalog reader of the hub's ``orders`` in change mode and a
catalog writer of what it read into ``orders_changes`` beside it, run through the designer's run path on the session
worker against the fake hub over loopback and MinIO (the fixtures of ``test_hub_tables_e2e``). The cursor is the hub's:
this core reads and advances it on core's own change-cursor routes there, which the fake keeps in ``cursors``, and
lists, resets and deletes it from the table page's Change tracking card through the same routes.
"""

import polars as pl
import pytest
from deltalake import DeltaTable

from flowfile_core.schemas import input_schema, schemas, transform_schema
from tests.flowfile.execution import test_hub_tables_e2e as e2e
from tests.flowfile.execution.graph_helpers import add_promise, connect, make_graph
from tests.hub_client import test_hub_table_reads as reads
from tests.test_catalog_cloud_virtual import requires_minio

pytestmark = [requires_minio, pytest.mark.usefixtures("running_worker", "protocol_on")]
serve_fake = e2e.serve_fake
store = e2e.store
hub = e2e.hub
api = reads.api
HUB_FLOW = "6b1f0c5e-0000-4000-8000-00000000c0de"


@pytest.fixture
def tracked(api, hub) -> dict:
    """``orders`` with change tracking turned on from this core, and one row appended after it, as a tracked write."""
    orders = hub.orders
    orders.update(row_count=5, column_count=3, size_bytes=1)
    api.post(f"/catalog/tables/{-orders['id']}/cdc/enable").raise_for_status()
    _append(orders, 6, "FR")
    return orders


def _append(orders: dict, row_id: int, country: str) -> None:
    pl.DataFrame({"id": [row_id], "country": [country], "amount": [row_id * 10]}).write_delta(
        orders["file_path"], mode="append", storage_options=e2e.MINIO_OPTIONS
    )


def _flow(hub, flow_id: int, flow_uuid: str = HUB_FLOW, failing: bool = False):
    """Reader of ``orders`` since its last run from the beginning of its tracked history → writer of
    ``orders_changes``; with ``failing`` a Polars code step beside the writer fails on the rows it is given."""
    graph = make_graph(flow_id, execution_location="remote")
    graph.flow_settings.hub_origin = schemas.HubOrigin(hub_url=hub.url, flow_uuid=flow_uuid, version=1)
    add_promise(graph, 1, "catalog_reader")
    graph.add_catalog_reader(
        input_schema.NodeCatalogReader(
            flow_id=flow_id,
            node_id=1,
            user_id=1,
            source="hub",
            catalog_full_table_name=hub.orders["qualified_name"],
            catalog_table_name="orders",
            cdc_mode="since_last_run",
            cdc_start="beginning",
        )
    )
    add_promise(graph, 2, "catalog_writer")
    graph.add_catalog_writer(
        input_schema.NodeCatalogWriter(
            flow_id=flow_id,
            node_id=2,
            depending_on_id=1,
            user_id=1,
            catalog_write_settings=input_schema.CatalogWriteSettings(
                table_name="orders_changes",
                source="hub",
                namespace_full_name=hub.fake._full_name(hub.sales["id"]),
                write_mode="overwrite",
            ),
        )
    )
    connect(graph, 1, 2)
    if failing:
        add_promise(graph, 3, "polars_code")
        graph.add_polars_code(
            input_schema.NodePolarsCode(
                flow_id=flow_id,
                node_id=3,
                depending_on_ids=[1],
                polars_code_input=transform_schema.PolarsCodeInput(
                    polars_code='output_df = input_df.with_columns(pl.col("_change_type").cast(pl.Int64))'
                ),
            )
        )
        connect(graph, 1, 3)
    return graph


def _read(hub) -> pl.DataFrame:
    """What the last run wrote into ``orders_changes``."""
    table = next(t for t in hub.fake.tables if t["name"] == "orders_changes")
    return pl.read_delta(table["file_path"], storage_options=e2e.MINIO_OPTIONS).sort("id")


def _cursor(hub, consumer_key: str) -> dict | None:
    return hub.fake.cursors.get(hub.orders["id"], {}).get(consumer_key)


KEY = f"flow:{HUB_FLOW}:node:1"


def test_a_flow_reads_a_hub_tables_changes_since_its_last_run_and_the_hub_keeps_its_cursor(api, hub, tracked):
    floor = tracked["cdc_enabled_version"]

    first = e2e.run(_flow(hub, 9910))

    assert first.success, first.node_step_result
    rows = _read(hub)
    assert rows["id"].to_list() == [6] and rows["_change_type"].to_list() == ["insert"]
    assert _cursor(hub, KEY)["last_version"] == floor + 1
    assert _cursor(hub, KEY)["table_path"] == tracked["file_path"]

    body = {
        "key_columns": ["id"],
        "expected_version": floor + 1,
        "upsert_columns": ["id", "country", "amount"],
        "upsert_rows": [[2, "US", 99]],
        "delete_keys": [[5]],
    }
    api.post(f"/catalog/tables/{-tracked['id']}/edits", json=body).raise_for_status()

    second = e2e.run(_flow(hub, 9910))

    assert second.success, second.node_step_result
    changed = _read(hub)
    assert sorted(zip(changed["id"].to_list(), changed["_change_type"].to_list(), strict=True)) == [
        (2, "update_postimage"),
        (5, "delete"),
    ]
    assert _cursor(hub, KEY)["last_version"] == DeltaTable(tracked["file_path"], storage_options=e2e.MINIO_OPTIONS).version()


def test_the_same_hub_flow_run_from_another_server_shares_its_cursor_and_another_flow_has_its_own(hub, tracked):
    assert e2e.run(_flow(hub, 9911)).success
    _append(tracked, 7, "BE")

    elsewhere = e2e.run(_flow(hub, 9912))
    other_flow = e2e.run(_flow(hub, 9913, flow_uuid="6b1f0c5e-0000-4000-8000-0000000000aa"))

    assert elsewhere.success and other_flow.success
    assert set(hub.fake.cursors[tracked["id"]]) == {KEY, "flow:6b1f0c5e-0000-4000-8000-0000000000aa:node:1"}
    assert _cursor(hub, KEY)["last_version"] == tracked["cdc_enabled_version"] + 2
    assert _read(hub)["id"].to_list() == [6, 7]


def test_a_run_that_fails_downstream_does_not_advance_the_hubs_cursor(hub, tracked):
    assert e2e.run(_flow(hub, 9914)).success
    read_up_to = _cursor(hub, KEY)["last_version"]
    _append(tracked, 7, "BE")

    failed = e2e.run(_flow(hub, 9914, failing=True))

    assert not failed.success
    assert _cursor(hub, KEY)["last_version"] == read_up_to
    again = e2e.run(_flow(hub, 9914))
    assert again.success and _read(hub)["id"].to_list() == [7]


def test_a_person_the_hub_does_not_let_read_the_table_is_refused_and_no_cursor_is_kept(hub, tracked):
    hub.fake.grants.clear()

    refused = e2e.run(_flow(hub, 9915))

    assert not refused.success
    assert hub.fake.cursors.get(tracked["id"], {}) == {}


def test_the_change_tracking_card_lists_resets_and_deletes_a_hub_tables_cursor(api, hub, tracked):
    assert e2e.run(_flow(hub, 9916)).success
    floor = tracked["cdc_enabled_version"]
    _append(tracked, 7, "BE")

    status = api.get(f"/catalog/tables/{-tracked['id']}/cdc")

    assert status.status_code == 200, status.text
    [cursor] = status.json()["cursors"]
    assert cursor["consumer_key"] == KEY and cursor["last_version"] == floor + 1
    assert status.json()["current_version"] == floor + 2 and cursor["pending_commits"] == 1

    reset = api.post(f"/catalog/tables/{-tracked['id']}/cdc/cursors/reset", json={"consumer_key": KEY, "to": "now"})

    assert reset.status_code == 200, reset.text
    assert reset.json()["last_version"] == floor + 2 and reset.json()["pending_commits"] == 0
    assert _cursor(hub, KEY)["last_version"] == floor + 2

    to_start = api.post(
        f"/catalog/tables/{-tracked['id']}/cdc/cursors/reset", json={"consumer_key": KEY, "to": "beginning"}
    )
    assert to_start.status_code == 200, to_start.text
    assert _cursor(hub, KEY)["last_version"] == floor - 1
    assert e2e.run(_flow(hub, 9916)).success
    assert _read(hub)["id"].to_list() == [6, 7]

    deleted = api.delete(f"/catalog/tables/{-tracked['id']}/cdc/cursors/{KEY}")

    assert deleted.status_code == 204, deleted.text
    assert api.get(f"/catalog/tables/{-tracked['id']}/cdc").json()["cursors"] == []
    missing = api.post(f"/catalog/tables/{-tracked['id']}/cdc/cursors/reset", json={"consumer_key": KEY, "to": 0})
    assert missing.status_code == 404, missing.text
