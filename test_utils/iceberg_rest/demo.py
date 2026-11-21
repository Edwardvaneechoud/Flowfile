"""A two-minute walk through reading and writing a catalog's table with only the keys the catalog hands out.

Run ``python -m test_utils.iceberg_rest demo`` with the catalog running. Every read and write by Flowfile's helpers
happens in a fresh process whose environment holds no storage key; the tests in ``shared/tests/iceberg`` are the proof.
"""

import dataclasses
import itertools
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from test_utils.iceberg_rest import fixtures

CHILD = "shared.tests.iceberg.table_child"
TABLE = "demo.orders"
OTHER_TABLE = "demo.other"


def _duck_rows(duck) -> int:
    fixtures.attach_lake(duck)
    return duck.execute(f"SELECT count(*) FROM lake.{TABLE}").fetchone()[0]


def _duck(extension_dir: str):
    """A DuckDB connection with the catalog attached, or None with the reason when its extension is not available."""
    try:
        import duckdb

        connection = duckdb.connect()
        fixtures.attach_lake(connection, extension_dir)
        return connection, None
    except Exception as exc:  # noqa: BLE001
        return None, fixtures.mask(str(exc)).splitlines()[0][:120]


def run_demo() -> int:
    if not fixtures.catalog_ready():
        print(f"No Iceberg REST catalog answers at {fixtures.CATALOG_URI}.")
        print("Start it with: python -m test_utils.iceberg_rest start")
        return 1
    import polars as pl
    from pyiceberg.catalog import load_catalog

    from shared.iceberg_utils import table_access
    from shared.tests.iceberg.table_child import rows_frame

    catalog = load_catalog("demo", **fixtures.catalog_properties())
    catalog.create_namespace_if_not_exists("demo")
    arrow = rows_frame(10).collect().to_arrow(compat_level=pl.CompatLevel.oldest())
    for identifier in (TABLE, OTHER_TABLE):
        if catalog.table_exists(identifier):
            catalog.purge_table(identifier)
        catalog.create_table(identifier, schema=arrow.schema)
    catalog.load_table(OTHER_TABLE).append(arrow)
    step = itertools.count(1)

    print("Flowfile's Iceberg helpers (Polars underneath) against a Lakekeeper catalog on a local MinIO.")
    print("This catalog asks for no sign-in; everything below is about storage keys.\n")
    with tempfile.TemporaryDirectory() as scratch:
        env = fixtures.clean_env(Path(scratch) / "home")
        address = fixtures.catalog_properties()

        def write(mode: str, rows: int, start: int) -> None:
            payload = {"op": "write", "catalog": address, "identifier": TABLE, "mode": mode, "rows": rows}
            fixtures.run_child(CHILD, {**payload, "start": start}, env)

        def read_by_access(access=None) -> dict:
            access = access or table_access(catalog.load_table(TABLE))
            return fixtures.run_child(CHILD, {"op": "read", "access": dataclasses.asdict(access)}, env)

        write("append", 1000, 0)
        print(f"{next(step)}. This process created the empty table {TABLE}; a second process, given only the")
        print("   catalog's address, wrote its first 1,000 rows.")

        table = catalog.load_table(TABLE)
        access = table_access(table)
        own = access.storage_properties["s3.access-key-id"] == fixtures.MINIO_ROOT_USER
        until = int(access.storage_properties.get("s3.session-token-expires-at-ms", 0)) / 1000
        print(f"{next(step)}. Asked the catalog at {fixtures.CATALOG_URI} for table {TABLE}.")
        print(f"   It answered with a temporary credential for that table's files, "
              f"{'the storage' if own else 'not the storage'}'s own keys,")
        print(f"   valid until {datetime.fromtimestamp(until, UTC):%H:%M} UTC.")

        own_file = fixtures.refusal_code(access.storage_properties, *fixtures.first_data_file(table))
        other_file = fixtures.refusal_code(access.storage_properties, *fixtures.first_data_file(
            catalog.load_table(OTHER_TABLE)))
        print(f"{next(step)}. The same credential opened this table's own data file"
              f"{'' if own_file is None else f' NOT ({own_file})'} and was "
              f"{'NOT refused' if other_file is None else f'refused ({other_file})'} on another table's files.")

        answer = fixtures.run_child(CHILD, {"op": "read", "catalog": address, "identifier": TABLE}, env)
        held = [name for name in answer["aws_env"] if "KEY" in name or "TOKEN" in name]
        print(f"{next(step)}. A fresh process given only the catalog's address read {answer['rows']:,} rows with the "
              "credential the catalog gave it.")
        print(f"   Storage keys set in that process's own environment: {len(held) or 'none'}.")

        with fixtures.catalog_paused():
            answer = read_by_access(access)
        print(f"{next(step)}. With the catalog paused, a second process given only the table's location and the "
              f"credential read {answer['rows']:,} rows.")

        duck, reason = _duck(str(Path(scratch) / "duckdb_extensions"))
        if duck is None:
            print(f"   DuckDB's Iceberg extension is not available here ({reason}), so the steps that check with "
                  "another engine are left out.")
        else:
            before = _duck_rows(duck)
            duck.execute(f"DELETE FROM lake.{TABLE} WHERE id % 7 = 0")
            deleted = before - _duck_rows(duck)
            sees = read_by_access()["rows"]
            print(f"{next(step)}. Another engine (DuckDB) deleted {deleted:,} rows. Flowfile's reader sees {sees:,}.")

        writes = (("append", 500, 1000, "Appended"), ("overwrite", 300, 5000, "Replaced everything with"))
        for mode, rows, start, verb in writes:
            write(mode, rows, start)
            seen = f"Flowfile's reader sees {read_by_access()['rows']:,}"
            if duck is not None:
                seen += f", DuckDB sees {_duck_rows(duck):,}"
            print(f"{next(step)}. {verb} {rows:,} rows: {seen}.")
    return 0
