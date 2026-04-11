"""Tests for the SQL query execution in the worker."""

import tempfile
from pathlib import Path

import polars as pl
import pytest

from flowfile_worker.funcs import execute_sql_query


@pytest.fixture
def delta_tables(tmp_path):
    """Create two temporary Delta tables for testing."""
    t1_path = tmp_path / "customers"
    t2_path = tmp_path / "orders"

    df1 = pl.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "city": ["NYC", "LA", "NYC"],
    })
    df1.write_delta(str(t1_path))

    df2 = pl.DataFrame({
        "order_id": [10, 20, 30, 40],
        "customer_id": [1, 2, 1, 3],
        "amount": [100.0, 200.0, 150.0, 300.0],
    })
    df2.write_delta(str(t2_path))

    return {"customers": str(t1_path), "orders": str(t2_path)}


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


def test_non_delta_path_rejected(tmp_path):
    """Test that a non-Delta path is rejected."""
    bad_path = tmp_path / "not_delta"
    bad_path.mkdir()

    with pytest.raises(ValueError, match="not a valid Delta table"):
        execute_sql_query("SELECT 1", {"bad": str(bad_path)})


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
