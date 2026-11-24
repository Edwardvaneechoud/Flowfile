"""A saved flow reads one Iceberg table and writes another, run by a real core on a real worker in two processes.

The flow names the catalog's address and two tables, nothing about storage. Each run, the process that reads or writes
asks the catalog for the table's keys; neither service is given one, and none ends up in the saved flow or a log.
"""

from __future__ import annotations

import json

import pytest

from test_utils.iceberg_rest import flow_demo
from test_utils.services import failed_nodes

pytestmark = pytest.mark.iceberg_rest


def _flow(namespace: str, location: str, write_mode: str = "overwrite") -> dict:
    flow = flow_demo.iceberg_flow(f"{namespace}.orders", f"{namespace}.orders_grp3_{location}", write_mode)
    flow["flowfile_settings"]["execution_location"] = location
    return flow


@pytest.mark.parametrize("location", ["remote", "local"])
def test_a_saved_flow_reads_one_catalog_table_and_writes_another_on_the_worker(stack, namespace, duck, location):
    orders = flow_demo.seed_orders(namespace)
    flow = _flow(namespace, location)
    target = flow["nodes"][2]["setting_input"]["cloud_storage_settings"]["resource_path"]

    flow_id = stack.import_flow(flow)
    columns = stack.get("/node", flow_id=flow_id, node_id=1, get_data=False)["main_output"]["columns"]
    run = stack.run(flow_id)

    assert columns == flow_demo.COLUMNS
    assert run["success"], failed_nodes(run)
    assert sorted(run["nodes"]) == [1, 2, 3] and all(node["success"] for node in run["nodes"].values())
    assert flow_demo.duck_count(duck, target) == 100

    keys = flow_demo.handed_keys(orders, target)
    handed = len(keys)
    assert handed == 6
    texts = {
        "the saved flow": flow_demo.saved_flow(stack, flow_id),
        "core's log": (stack.cwds["core"].parent / "core.log").read_text(),
        "the worker's log": (stack.cwds["worker"].parent / "worker.log").read_text(),
        "the flow file": json.dumps(flow),
        "core's environment": flow_demo.process_environment(stack.pids["core"]),
        "the worker's environment": flow_demo.process_environment(stack.pids["worker"]),
    }
    assert flow_demo.leaks(texts, keys) == []
    environment_shown = "AWS_EC2_METADATA_DISABLED" in texts["core's environment"]
    assert environment_shown, "ps did not show the environment"


def test_the_run_log_says_when_the_read_fell_back(stack, namespace, duck):
    """After DuckDB deletes rows across both data files, Polars' own reader refuses the table and PyIceberg reads it."""
    orders = flow_demo.seed_orders(namespace)
    flow = _flow(namespace, "remote")
    target = flow["nodes"][2]["setting_input"]["cloud_storage_settings"]["resource_path"]
    flow_id = stack.import_flow(flow)
    assert stack.run(flow_id)["success"]

    flow_demo.duck_count(duck, orders)
    duck.execute(f"DELETE FROM lake.{orders} WHERE id % 100 = 3")
    run = stack.run(flow_id)

    assert run["success"], failed_nodes(run)
    assert flow_demo.duck_count(duck, target) == 90
    log = flow_demo.flow_log(stack, flow_id)
    fell_back = flow_demo.FALLBACK in log and "position delete file not limited to one data file" in log
    assert fell_back, f"no fallback line in logs/flow_{flow_id}.log ({len(log)} characters)"


class _Values(list):
    """Secret values a test searches for; its ``repr`` holds none of them."""

    def __repr__(self) -> str:
        return f"<{len(self)} secret values>"


def test_a_saved_flow_names_a_signed_catalog_and_runs_on_the_worker(stack, namespace):
    """Polaris is added over HTTP with a client id and secret; the saved flow names it, and only its name."""
    import uuid

    import polars as pl

    from shared.iceberg_utils import scan_table, storage_properties, table_access, write_table
    from test_utils.iceberg_rest import fixtures

    if not fixtures.signed_catalog_ready():
        pytest.skip(fixtures.SIGNED_SKIP_REASON)
    principal = fixtures.new_principal()
    signed = fixtures.signed_catalog(principal)
    schema = f"e2e_signed_{uuid.uuid4().hex[:6]}"
    signed.create_namespace(schema)
    try:
        rows = flow_demo.orders_rows(1000)
        orders = signed.create_table(f"{schema}.orders", schema=rows.to_arrow(compat_level=pl.CompatLevel.oldest()).schema)
        write_table(orders, rows.lazy(), "append")
        name = f"polaris_{uuid.uuid4().hex[:6]}"
        added = stack.post("/catalog/external", {
            "name": name, "uri": fixtures.SIGNED_CATALOG_URI, "warehouse": fixtures.SIGNED_WAREHOUSE,
            "sign_in": "client_credentials", "client_id": principal.client_id,
            "client_secret": principal.client_secret, "scope": fixtures.SIGNED_SCOPE,
        })
        flow = flow_demo.iceberg_flow(f"{schema}.orders", f"{schema}.orders_grp3")
        for node in (flow["nodes"][0], flow["nodes"][2]):
            node["setting_input"]["cloud_storage_settings"]["iceberg_catalog"] = {"catalog": name}
        flow["flowfile_settings"]["execution_location"] = "remote"

        flow_id = stack.import_flow(flow)
        run = stack.run(flow_id)

        assert run["success"], failed_nodes(run)
        written = signed.load_table(f"{schema}.orders_grp3")
        assert scan_table(table_access(written)).select(pl.len()).collect().item() == 100
        token, _ = fixtures.fetch_token(principal)
        secrets = _Values([principal.client_secret, principal.credential, token])
        keys = _Values(fixtures.key_values(storage_properties(written)))
        saved = flow_demo.saved_flow(stack, flow_id)
        texts = {
            "the saved flow": saved,
            "the add answer": json.dumps(added),
            "core's log": (stack.cwds["core"].parent / "core.log").read_text(),
            "the worker's log": (stack.cwds["worker"].parent / "worker.log").read_text(),
        }
        environments = {
            "core's environment": flow_demo.process_environment(stack.pids["core"]),
            "the worker's environment": flow_demo.process_environment(stack.pids["worker"]),
        }
        assert flow_demo.leaks(texts, secrets) == []
        assert flow_demo.leaks({**texts, **environments}, keys) == []
        named_only = f'"catalog": "{name}"' in saved.replace("'", '"') or f"catalog: {name}" in saved
        assert named_only and fixtures.SIGNED_CATALOG_URI not in saved
    finally:
        for identifier in signed.list_tables(schema):
            signed.drop_table(identifier)
        signed.drop_namespace(schema)
        fixtures.drop_principal(principal.name)
