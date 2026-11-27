"""Helpers for a saved flow that reads one Iceberg table and writes another on Flowfile's worker, with keys only the
catalog hands out.

``tests/iceberg_e2e`` and the UI walk (``ui_check``) use them: they start Flowfile's core and worker as two processes of
their own (``test_utils.services``, which imports no Flowfile package) and stop only the processes they started.
"""

from __future__ import annotations

import contextlib
import re
import subprocess
import uuid
from collections.abc import Iterator
from pathlib import Path

from test_utils import services
from test_utils.iceberg_rest import fixtures

ORDERS = "demo.orders"
GRP3 = "demo.orders_grp3"
COLUMNS = ["id", "grp", "val", "name"]
KEY_NAMES = (
    "s3.secret-access-key",
    "s3.session-token",
    "aws_secret_access_key",
    "aws_session_token",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
)
FALLBACK = "reading it with PyIceberg"
# A token minted afresh differs from any held before, so a saved flow is also searched for a token's or key's shape.
SECRET_SHAPES = {
    "a JWT": re.compile(r"eyJ[\w-]+\.[\w-]+\."),
    "an access key": re.compile(r"(?<![A-Z0-9])[A-Z0-9]{20}(?![A-Z0-9])"),
}


def service_env(root: Path) -> dict[str, str]:
    """What both services are given about AWS: empty credential files, no metadata service, a closed endpoint."""
    aws = root / "aws"
    aws.mkdir(parents=True, exist_ok=True)
    for name in ("credentials", "config"):
        (aws / name).write_text("")
    return {
        "AWS_EC2_METADATA_DISABLED": "true",
        "AWS_ENDPOINT_URL": "http://127.0.0.1:9",
        "AWS_SHARED_CREDENTIALS_FILE": str(aws / "credentials"),
        "AWS_CONFIG_FILE": str(aws / "config"),
    }


@contextlib.contextmanager
def running_stack(root: Path) -> Iterator[services.Services]:
    """Core and a worker on free ports, in desktop mode, each in a working directory of its own under *root*."""
    with services.running_services(root, "electron", service_env(root), label="iceberg_e2e") as stack:
        yield stack


def _catalog_block() -> dict:
    return {"uri": fixtures.CATALOG_URI, "warehouse": fixtures.WAREHOUSE}


def iceberg_flow(source: str = ORDERS, target: str = GRP3, write_mode: str = "overwrite") -> dict:
    """Reader, filter ``[grp] = 3`` and writer, as the designer saves them; the flow names no storage at all."""
    reader = {
        "id": 1, "type": "cloud_storage_reader", "is_start_node": True, "description": f"Read {source} (iceberg)",
        "x_position": 0, "y_position": 0, "input_ids": None, "outputs": [2], "output_handles": ["output-0"],
        "setting_input": {
            "cache_results": False, "output_field_config": None, "fields": [],
            "cloud_storage_settings": {"resource_path": source, "file_format": "iceberg",
                                       "iceberg_catalog": _catalog_block()},
        },
    }
    keep = {
        "id": 2, "type": "filter", "is_start_node": False, "description": "[grp] = 3",
        "x_position": 200, "y_position": 0, "input_ids": [1], "outputs": [3], "output_handles": ["output-0"],
        "setting_input": {
            "cache_results": False, "output_field_config": None, "split_mode": False,
            "filter_input": {"mode": "advanced", "basic_filter": None, "advanced_filter": "[grp] = 3",
                             "filter_type": None},
        },
    }
    writer = {
        "id": 3, "type": "cloud_storage_writer", "is_start_node": False,
        "description": f"Write to {target} (iceberg, {write_mode})",
        "x_position": 400, "y_position": 0, "input_ids": [2], "outputs": [], "output_handles": [],
        "setting_input": {
            "cache_results": False, "output_field_config": None,
            "cloud_storage_settings": {"resource_path": target, "file_format": "iceberg", "write_mode": write_mode,
                                       "iceberg_catalog": _catalog_block()},
        },
    }
    flow_id = 10**9 + uuid.uuid4().int % 10**9
    return services.flow_document(flow_id, "iceberg flow", [reader, keep, writer])


def catalog_nodes_flow(source: str, target_schema: str, target_table: str, write_mode: str = "overwrite") -> dict:
    """Catalog Reader on ``source`` (``catalog.schema.table``), filter ``[grp] = 3``, Catalog Writer into
    ``target_schema`` (``catalog.schema``) as ``target_table``, as the drawers save them: names only, no id, no address.
    """
    reader = {
        "id": 1, "type": "catalog_reader", "is_start_node": True, "description": f"Read {source}",
        "x_position": 0, "y_position": 0, "input_ids": None, "outputs": [2], "output_handles": ["output-0"],
        "setting_input": {"cache_results": False, "output_field_config": None, "catalog_full_table_name": source},
    }
    keep = {
        "id": 2, "type": "filter", "is_start_node": False, "description": "[grp] = 3",
        "x_position": 200, "y_position": 0, "input_ids": [1], "outputs": [3], "output_handles": ["output-0"],
        "setting_input": {
            "cache_results": False, "output_field_config": None, "split_mode": False,
            "filter_input": {"mode": "advanced", "basic_filter": None, "advanced_filter": "[grp] = 3",
                             "filter_type": None},
        },
    }
    writer = {
        "id": 3, "type": "catalog_writer", "is_start_node": False,
        "description": f"Write {target_schema}.{target_table} ({write_mode})",
        "x_position": 400, "y_position": 0, "input_ids": [2], "outputs": [], "output_handles": [],
        "setting_input": {
            "cache_results": False, "output_field_config": None,
            "catalog_write_settings": {"table_name": target_table, "namespace_full_name": target_schema,
                                       "write_mode": write_mode},
        },
    }
    flow_id = 10**9 + uuid.uuid4().int % 10**9
    return services.flow_document(flow_id, "catalog nodes flow", [reader, keep, writer])


def catalog_flow_tables(stack: services.Services, flow_id: int) -> tuple[list[str], list[str]] | None:
    """The tables Flowfile's catalog says the registered flow behind open flow ``flow_id`` reads and writes, as
    ``(tables_read, tables_produced)`` names; None when the flow has no registration."""
    registration = stack.get("/flow_settings", flow_id=flow_id).get("source_registration_id")
    if registration is None:
        return None
    flow = stack.get(f"/catalog/flows/{registration}")
    return [t["name"] for t in flow.get("tables_read") or []], [t["name"] for t in flow.get("tables_produced") or []]


def catalog():
    from pyiceberg.catalog import load_catalog

    return load_catalog("flow_demo", **fixtures.catalog_properties())


def orders_rows(rows: int = 1000):
    import polars as pl

    frame = pl.DataFrame({"id": pl.int_range(0, rows, dtype=pl.Int64, eager=True)})
    return frame.with_columns(
        grp=pl.col("id") % 10, val=pl.col("id").cast(pl.Float64) * 0.5, name=pl.format("name-{}", pl.col("id"))
    )


def seed_orders(namespace: str = "demo") -> str:
    """Make ``<namespace>.orders`` afresh with PyIceberg: 1000 rows in two data files; drop ``orders_grp3``."""
    import polars as pl

    lake = catalog()
    lake.create_namespace_if_not_exists(namespace)
    for name in ("orders", "orders_grp3"):
        if lake.table_exists(f"{namespace}.{name}"):
            lake.purge_table(f"{namespace}.{name}")
    arrow = orders_rows().to_arrow(compat_level=pl.CompatLevel.oldest())
    table = lake.create_table(f"{namespace}.orders", schema=arrow.schema)
    table.append(arrow.slice(0, 500))
    table.append(arrow.slice(500, 500))
    return f"{namespace}.orders"


def handed_keys(*tables: str) -> list[str]:
    """The key values the catalog hands out for these tables now (it hands the same ones to every client)."""
    lake = catalog()
    names = ("s3.access-key-id", "s3.secret-access-key", "s3.session-token")
    found = []
    for table in tables:
        if lake.table_exists(table):
            properties = lake.load_table(table).io.properties
            found.extend(str(properties[name]) for name in names if properties.get(name))
    return found


def duck(extension_dir: Path):
    import duckdb

    connection = duckdb.connect()
    fixtures.attach_lake(connection, extension_dir)
    return connection


def duck_count(connection, table: str) -> int:
    fixtures.attach_lake(connection)
    return connection.execute(f"SELECT count(*) FROM lake.{table}").fetchone()[0]


def flow_log(stack: services.Services, flow_id: int) -> str:
    """The flow's run log as core keeps it: ``logs/flow_<id>.log`` under core's storage directory."""
    path = stack.storage["core"] / "logs" / f"flow_{flow_id}.log"
    return path.read_text() if path.exists() else ""


def saved_flow(stack: services.Services, flow_id: int) -> str:
    """The flow as core saves it, written to a file under core's storage and read back."""
    path = stack.storage["core"] / "e2e_flows" / f"saved_{flow_id}.yaml"
    response = stack.http.post(f"{stack.base}/save_flow", params={"flow_id": flow_id, "flow_path": str(path),
                                                                   "register_in_catalog": False})
    assert response.status_code == 200, response.text[:300]
    return path.read_text()


def process_environment(pid: int) -> str:
    """The environment a process was started with, as ``ps`` shows it (macOS and Linux)."""
    shown = subprocess.run(["ps", "eww", "-o", "command=", "-p", str(pid)], capture_output=True, text=True)
    return shown.stdout


def leaks(texts: dict[str, str], keys: list[str]) -> list[str]:
    """Which of the texts hold a key value or a secret key's name; names only, never a value."""
    return [where for where, text in texts.items() if any(key and key in text for key in [*keys, *KEY_NAMES])]


def secret_shapes(text: str) -> list[str]:
    """Which of ``SECRET_SHAPES`` the text holds; names only, never a value."""
    return [name for name, shape in SECRET_SHAPES.items() if shape.search(text)]


def _flag(passed: bool) -> str:
    return "" if passed else " -- NOT AS EXPECTED"

