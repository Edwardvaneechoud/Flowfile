"""Tests for the SQL query execution in the worker."""

from pathlib import Path

import polars as pl
import pytest

from flowfile_worker.funcs import execute_sql_query
from shared.storage_config import storage
from tests.conftest import INTERNAL_AUTH_HEADERS


@pytest.fixture(autouse=True)
def _setup_storage(tmp_path: Path):
    """Point storage at tmp_path so catalog_tables_directory is inside tmp_path."""
    old_base, old_user = storage._base_dir, storage._user_data_dir
    storage._base_dir = tmp_path
    storage._user_data_dir = tmp_path
    storage.catalog_tables_directory.mkdir(parents=True, exist_ok=True)
    yield
    storage._base_dir = old_base
    storage._user_data_dir = old_user


@pytest.fixture
def delta_tables():
    """Create two Delta tables inside the catalog directory."""
    catalog_dir = storage.catalog_tables_directory

    df1 = pl.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "city": ["NYC", "LA", "NYC"],
    })
    df1.write_delta(str(catalog_dir / "customers"))
    df2 = pl.DataFrame({
        "order_id": [10, 20, 30, 40],
        "customer_id": [1, 2, 1, 3],
        "amount": [100.0, 200.0, 150.0, 300.0],
    })
    df2.write_delta(str(catalog_dir / "orders"))

    return {"customers": "customers", "orders": "orders"}


def test_simple_select(delta_tables):
    """Test a simple SELECT * query."""
    result = execute_sql_query("SELECT * FROM customers", delta_tables)

    assert result["error"] is None if "error" in result else True
    assert result["columns"] == ["id", "name", "city"]
    assert len(result["rows"]) == 3
    assert result["total_rows"] == 3
    assert result["truncated"] is False
    assert "customers" in result["used_tables"]


def test_join_query(delta_tables):
    """Test a JOIN query between two tables."""
    result = execute_sql_query(
        "SELECT c.name, o.amount FROM customers c JOIN orders o ON c.id = o.customer_id",
        delta_tables,
    )

    assert len(result["columns"]) == 2
    assert result["total_rows"] == 4
    assert "customers" in result["used_tables"]
    assert "orders" in result["used_tables"]


def test_aggregation_query(delta_tables):
    """Test an aggregation query."""
    result = execute_sql_query(
        "SELECT city, COUNT(*) as cnt FROM customers GROUP BY city",
        delta_tables,
    )

    assert "city" in result["columns"]
    assert "cnt" in result["columns"]
    assert result["total_rows"] == 2  # NYC and LA


def test_max_rows_truncation(delta_tables):
    """Test that max_rows truncation works."""
    result = execute_sql_query("SELECT * FROM orders", delta_tables, max_rows=2)

    assert len(result["rows"]) == 2
    assert result["total_rows"] == 4
    assert result["truncated"] is True


def test_invalid_sql(delta_tables):
    """Test that invalid SQL raises an exception."""
    with pytest.raises(Exception):
        execute_sql_query("THIS IS NOT SQL", delta_tables)


def test_non_select_rejected_server_side():
    """The worker re-validates: it must not trust the core caller (validation
    runs before any table is touched, hence the empty tables mapping)."""
    from shared.sql_validation import UnsafeSQLError

    with pytest.raises(UnsafeSQLError):
        execute_sql_query("DROP TABLE customers", {})


def test_table_function_rejected_server_side():
    """read_csv/read_parquet table functions (file read + SSRF) are refused at
    the worker, closing the core→worker trust gap."""
    from shared.sql_validation import UnsafeSQLError

    for query in [
        "SELECT * FROM read_csv('/etc/hosts')",
        "SELECT * FROM read_parquet('https://evil.example/x.parquet')",
    ]:
        with pytest.raises(UnsafeSQLError, match="table functions"):
            execute_sql_query(query, {})


def test_used_tables_only_referenced(delta_tables):
    """Test that used_tables only includes tables actually referenced in the query."""
    result = execute_sql_query("SELECT * FROM customers", delta_tables)

    assert "customers" in result["used_tables"]
    assert "orders" not in result["used_tables"]


def test_execution_time_reported(delta_tables):
    """Test that execution time is reported."""
    result = execute_sql_query("SELECT * FROM customers", delta_tables)

    assert result["execution_time_ms"] > 0


@pytest.fixture
def three_part_table():
    """A Delta table registered under a fully-qualified flat key, as core now ships it."""
    catalog_dir = storage.catalog_tables_directory
    df = pl.DataFrame({"pair": ["EURUSD", "GBPUSD"], "rate": [1.08, 1.27]})
    df.write_delta(str(catalog_dir / "fx_rates"))
    return {"Demo.market.fx_rates": "fx_rates"}


def test_three_part_quoted_identifier_resolves(three_part_table):
    """A flat 3-part key registered by core resolves when the query references it as a
    single double-quoted identifier (the form rewrite_qualified_references emits).

    (``used_tables`` is not asserted here: it matches the registered name against the
    plan text, which only ever contains the storage path — a pre-existing heuristic
    orthogonal to name resolution.)"""
    result = execute_sql_query('SELECT * FROM "Demo.market.fx_rates"', three_part_table)

    assert result["columns"] == ["pair", "rate"]
    assert result["total_rows"] == 2
    assert result["rows"] == [["EURUSD", 1.08], ["GBPUSD", 1.27]]


def test_physical_table_query_sees_out_of_band_delta_write(delta_tables):
    """Freshness regression tripwire: the raw SQL path must have NO cache.

    Query → overwrite the Delta table out-of-band (new version) → query again
    must return the new rows. If this ever fails, in-process state crept into
    execute_sql_query / open_catalog_table.
    """
    first = execute_sql_query("SELECT city, COUNT(*) AS n FROM customers GROUP BY city", delta_tables)
    assert first["total_rows"] == 2

    catalog_dir = storage.catalog_tables_directory
    pl.DataFrame({"id": [9], "name": ["Zed"], "city": ["ZZZ_TEST"]}).write_delta(
        str(catalog_dir / "customers"), mode="overwrite"
    )

    second = execute_sql_query("SELECT city, COUNT(*) AS n FROM customers GROUP BY city", delta_tables)
    assert second["total_rows"] == 1
    assert second["rows"] == [["ZZZ_TEST", 1]]


def test_physical_table_route_sees_out_of_band_delta_write(delta_tables):
    """Same tripwire through the FastAPI route (runs execute_sql_query in-process)."""
    from fastapi.testclient import TestClient

    from flowfile_worker import main

    client = TestClient(main.app, headers=INTERNAL_AUTH_HEADERS)
    payload = {"query": "SELECT city, COUNT(*) AS n FROM customers GROUP BY city", "tables": delta_tables}

    r1 = client.post("/catalog/sql_query", json=payload)
    assert r1.status_code == 200, r1.text
    assert r1.json()["total_rows"] == 2

    catalog_dir = storage.catalog_tables_directory
    pl.DataFrame({"id": [9], "name": ["Zed"], "city": ["ZZZ_TEST"]}).write_delta(
        str(catalog_dir / "customers"), mode="overwrite"
    )

    r2 = client.post("/catalog/sql_query", json=payload)
    assert r2.status_code == 200, r2.text
    body = r2.json()
    assert body["total_rows"] == 1
    assert body["rows"] == [["ZZZ_TEST", 1]]
