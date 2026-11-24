"""python -m test_utils.iceberg_rest tables [--warehouse NAME]: what the rig's Polaris holds, so a person sees a write land.

Polaris has no web UI of its own. This lists every table of one Polaris warehouse (the shared ``signed`` one a person
adds by hand, unless ``--warehouse`` names another, such as a running ``ui --hold``'s) with its row count and its last
commit. The rows are counted by reading the table's data files with the keys Polaris hands out, never taken from a
snapshot summary; the time and operation are the current snapshot's. It writes no table or namespace: it lists as
Polaris's root principal and, since Polaris hands root no storage keys, reads as a principal of its own holding only
``TABLE_READ_DATA`` on that warehouse, made for the call and dropped after it. No AWS identity of this machine is used,
and no secret or storage key is printed.
"""

from __future__ import annotations

import os
import secrets
import tempfile
from datetime import datetime
from pathlib import Path

from test_utils.iceberg_rest import fixtures


def _cut_off_aws(folder: Path) -> None:
    for name in [key for key in os.environ if key.startswith("AWS_")]:
        os.environ.pop(name)
    for name in ("credentials", "config"):
        (folder / name).write_text("")
    os.environ.update(
        AWS_EC2_METADATA_DISABLED="true",
        AWS_ENDPOINT_URL="http://127.0.0.1:9",
        AWS_SHARED_CREDENTIALS_FILE=str(folder / "credentials"),
        AWS_CONFIG_FILE=str(folder / "config"),
    )


def _namespaces(catalog, parent: tuple[str, ...] = ()) -> list[tuple[str, ...]]:
    found: list[tuple[str, ...]] = []
    for namespace in catalog.list_namespaces(parent):
        found += [namespace, *_namespaces(catalog, namespace)]
    return found


def _line(table) -> tuple[str, str]:
    import polars as pl

    from shared.iceberg_utils import scan_table, table_access

    snapshot = table.current_snapshot()
    if snapshot is None:
        made = datetime.fromtimestamp(table.metadata.last_updated_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
        return "0", f"no commit yet (made {made})"
    rows = scan_table(table_access(table)).select(pl.first()).collect().height
    when = datetime.fromtimestamp(snapshot.timestamp_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
    operation = snapshot.summary.operation.value if snapshot.summary else "?"
    return f"{rows:,}", f"last commit {when} ({operation})"


def run_tables(argv: list[str]) -> int:
    warehouse = argv[argv.index("--warehouse") + 1] if "--warehouse" in argv else fixtures.SIGNED_WAREHOUSE
    if not fixtures.signed_catalog_ready():
        print(fixtures.SIGNED_SKIP_REASON)
        return 1
    root = fixtures.Principal("root", "root", fixtures.POLARIS_ROOT_SECRET)
    shared = " (the shared one a person adds by hand)" if warehouse == fixtures.SIGNED_WAREHOUSE else ""
    print(
        f"Polaris at {fixtures.SIGNED_CATALOG_URI}, warehouse {warehouse!r}{shared}. Rows are counted by reading each "
        "table with the keys Polaris hands out."
    )
    with tempfile.TemporaryDirectory(prefix="iceberg_tables_") as scratch:
        _cut_off_aws(Path(scratch))
        catalog = fixtures.signed_catalog(root, warehouse=warehouse)
        listed = [
            (".".join(identifier), identifier) for ns in _namespaces(catalog) for identifier in catalog.list_tables(ns)
        ]
        if not listed:
            print("  (no tables)")
            return 0
        reader_name = f"tables_{os.getpid()}_{secrets.token_hex(3)}"
        try:
            reader = fixtures.new_principal(reader_name, privilege="TABLE_READ_DATA", catalog=warehouse)
            reading = fixtures.signed_catalog(reader, warehouse=warehouse)
            width = max(len(name) for name, _ in listed)
            failed = False
            for name, identifier in sorted(listed):
                try:
                    rows, commit = _line(reading.load_table(identifier))
                except Exception as exc:  # noqa: BLE001
                    failed = True
                    hidden = [root.client_secret, reader.client_secret]
                    rows, commit = "?", f"could not be read: {fixtures.mask(str(exc), hidden)[:200]}"
                print(f"  {name:<{width}}  {rows:>9} rows  {commit}")
        finally:
            fixtures.drop_principal(reader_name, catalog=warehouse)
    return 1 if failed else 0
