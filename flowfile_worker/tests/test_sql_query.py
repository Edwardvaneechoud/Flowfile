"""Tests for the SQL query execution in the worker."""

from pathlib import Path

import polars as pl
import pytest

from flowfile_worker.funcs import execute_sql_query
from shared.storage_config import storage


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

    return ["customers", "orders"]


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


def test_used_tables_only_referenced(delta_tables):
    """Test that used_tables only includes tables actually referenced in the query."""
    result = execute_sql_query("SELECT * FROM customers", delta_tables)

    assert "customers" in result["used_tables"]
    # orders should NOT be in used_tables since it wasn't referenced
    assert "orders" not in result["used_tables"]


def test_execution_time_reported(delta_tables):
    """Test that execution time is reported."""
    result = execute_sql_query("SELECT * FROM customers", delta_tables)

    assert result["execution_time_ms"] > 0
