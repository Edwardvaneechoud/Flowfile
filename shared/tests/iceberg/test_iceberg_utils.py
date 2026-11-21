"""The Iceberg helpers refuse a table that came with no key Polars can use, rather than fall back to the machine's.

These run without any service: a PyIceberg SQL catalog on a SQLite file gives real tables, one whose properties hold
no storage key and one whose catalog turned on remote signing.
"""

import polars as pl
import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog

from shared.iceberg_utils import storage_properties, table_access, write_table


def _table(tmp_path, **catalog_properties):
    warehouse = (tmp_path / "warehouse").as_uri()
    catalog = SqlCatalog("local", uri=f"sqlite:///{tmp_path}/catalog.db", warehouse=warehouse, **catalog_properties)
    catalog.create_namespace_if_not_exists("ns")
    if not catalog.table_exists("ns.t"):
        catalog.create_table("ns.t", schema=pa.schema([("id", pa.int64())]))
    return catalog.load_table("ns.t")


def test_a_table_without_handed_out_keys_is_refused(tmp_path):
    """A table that came with no storage key is refused, so a read never runs on the machine's own credentials."""
    table = _table(tmp_path)

    with pytest.raises(ValueError, match="handed out no storage keys"):
        table_access(table)
    with pytest.raises(ValueError, match="handed out no storage keys"):
        write_table(table, pl.LazyFrame({"id": [1]}), "append")


def test_a_table_whose_catalog_signs_requests_is_refused(tmp_path):
    """A catalog that signs each request itself is refused with a plain sentence, and its signer address is not kept."""
    table = _table(tmp_path, **{"s3.remote-signing-enabled": "true", "s3.signer.uri": "http://127.0.0.1:9/catalog"})

    assert "s3.signer.uri" not in storage_properties(table)
    with pytest.raises(ValueError, match="remote signing"):
        table_access(table)
    with pytest.raises(ValueError, match="remote signing"):
        write_table(table, pl.LazyFrame({"id": [1]}), "append")
