"""A property written on a table never becomes a storage setting: only the catalog's answer for the load names storage.

PyIceberg merges a table's own metadata properties into its file IO under the catalog's answer, so an ``s3.*`` property
on the table (``s3.proxy-uri``, an ``s3.endpoint`` of its own) would send the signed storage traffic elsewhere whenever
the catalog does not answer that key. Every test skips unless the catalog runs: ``python -m test_utils.iceberg_rest
start``.
"""

import polars as pl
import pytest

from shared import iceberg_utils
from shared.iceberg_utils import IcebergTableRef, scan_iceberg_table, storage_properties, table_access
from shared.tests.iceberg.table_child import expected, rows_frame
from test_utils.iceberg_rest import fixtures

pytestmark = pytest.mark.iceberg_rest

PLANTED = {
    "s3.proxy-uri": "http://127.0.0.1:9",
    "s3.x-flowfile-test": "planted",
    "s3.endpoint": "http://127.0.0.1:9",
    "client.region": "planted-region-1",
}
ANSWERED = ["s3.access-key-id", "s3.endpoint", "s3.path-style-access", "s3.secret-access-key", "s3.session-token"]


def _ref(namespace: str, name: str) -> IcebergTableRef:
    return IcebergTableRef(fixtures.CATALOG_URI, f"{namespace}.{name}", fixtures.WAREHOUSE)


@pytest.fixture
def handed_to_polars(monkeypatch):
    """The storage options every Polars scan and sink of an Iceberg table was given in this test, still run for real."""
    seen: list[dict[str, str]] = []
    scan, sink = pl.scan_iceberg, pl.LazyFrame.sink_iceberg

    def scanning(source, *args, **kwargs):
        seen.append(dict(kwargs.get("storage_options") or {}))
        return scan(source, *args, **kwargs)

    def sinking(self, target, *args, **kwargs):
        seen.append(dict(kwargs.get("storage_options") or {}))
        return sink(self, target, *args, **kwargs)

    monkeypatch.setattr(pl, "scan_iceberg", scanning)
    monkeypatch.setattr(pl.LazyFrame, "sink_iceberg", sinking)
    return seen


def _plant(catalog, identifier: str) -> None:
    """Write the planted properties on a table that already holds rows (a client that honours them could not seed it)."""
    catalog.load_table(identifier).transaction().set_properties(PLANTED).commit_transaction()


def _planted_in(properties: dict[str, str]) -> list[str]:
    return sorted(name for name, value in PLANTED.items() if properties.get(name) == value)


def test_a_storage_property_on_the_table_never_reaches_the_keys_a_read_or_write_uses(
    rest_catalog, namespace, seed_table, handed_to_polars
):
    seed_table("props_planted", 100)
    _plant(rest_catalog, f"{namespace}.props_planted")
    planted_on_table = sorted(name for name in PLANTED if rest_catalog.load_table(f"{namespace}.props_planted")
                              .metadata.properties.get(name) == PLANTED[name])
    assert planted_on_table == sorted(PLANTED), "the catalog did not keep the planted table properties"
    ref = _ref(namespace, "props_planted")

    table = iceberg_utils.load_table(ref)
    held = storage_properties(table)
    access = table_access(table)

    assert _planted_in(held) == []
    assert _planted_in(access.storage_properties) == []
    assert "s3.proxy-uri" not in held and "s3.x-flowfile-test" not in held
    assert [name for name in ANSWERED if not held.get(name)] == []
    assert held["s3.endpoint"].rstrip("/") == fixtures.S3_ENDPOINT
    assert _planted_in(table.io.properties) == []

    read = scan_iceberg_table(ref).select(pl.len(), pl.col("id").sum()).collect().row(0)
    iceberg_utils.write_iceberg_table(ref, rows_frame(10, 100), "append")
    after = scan_iceberg_table(ref).select(pl.len(), pl.col("id").sum()).collect().row(0)

    assert read == expected(100)
    assert after == expected(110)
    assert len(handed_to_polars) >= 3
    assert [_planted_in(options) for options in handed_to_polars if _planted_in(options)] == []
    assert all(options.get("s3.endpoint", "").rstrip("/") == fixtures.S3_ENDPOINT for options in handed_to_polars)


def test_a_table_created_by_a_write_and_reloaded_after_its_commit_keeps_only_the_catalogs_answer(
    rest_catalog, namespace, handed_to_polars
):
    """The table a write creates, and the one PyIceberg refreshes after the commit, come through the same seam."""
    ref = _ref(namespace, "props_planted_after_create")
    iceberg_utils.write_iceberg_table(ref, rows_frame(5), "append")
    _plant(rest_catalog, ref.table)

    iceberg_utils.write_iceberg_table(ref, rows_frame(5, 5), "append")
    table = iceberg_utils.load_table(ref)
    table.refresh()

    assert _planted_in(storage_properties(table)) == []
    assert _planted_in(table.io.properties) == []
    assert scan_iceberg_table(ref).select(pl.len(), pl.col("id").sum()).collect().row(0) == expected(10)
    assert [_planted_in(options) for options in handed_to_polars if _planted_in(options)] == []


def test_on_the_signed_catalog_a_planted_table_property_reaches_no_storage_setting(
    signed, signed_namespace, sealing, principal
):
    from shared.iceberg_utils import client_credentials_sign_in

    identifier = f"{signed_namespace}.props_planted"
    arrow = rows_frame(20).collect().to_arrow(compat_level=pl.CompatLevel.oldest())
    signed.create_table(identifier, schema=arrow.schema).append(arrow)
    _plant(signed, identifier)
    sign_in = sealing(client_credentials_sign_in(principal.client_id, principal.client_secret, fixtures.SIGNED_SCOPE))
    ref = IcebergTableRef(fixtures.SIGNED_CATALOG_URI, identifier, fixtures.SIGNED_WAREHOUSE, sign_in)

    held = storage_properties(iceberg_utils.load_table(ref))

    assert _planted_in(held) == []
    assert [name for name in ("s3.access-key-id", "s3.secret-access-key", "s3.session-token") if not held.get(name)] == []
    assert scan_iceberg_table(ref).select(pl.len(), pl.col("id").sum()).collect().row(0) == expected(20)
