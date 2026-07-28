"""DuckDB source tests: URI construction, reads, and the fast-schema contract.

DuckDB is file-based and in-process, so unlike the postgres/mysql suites nothing
here needs Docker — these tests run on every CI OS.
"""

import polars as pl
import pytest

from flowfile_core.flowfile.sources.external_sources.sql_source.sql_source import (
    SqlSource,
    UnsafeSQLError,
    list_db_schemas,
    list_db_tables,
)
from flowfile_core.schemas.input_schema import DatabaseConnection, DatabaseSettings
from shared.db_dialects import get_dialect
from shared.sql_utils import construct_sql_uri, get_sqlalchemy_uri


class TestDuckDBURIConstruction:
    def test_construct_uri_from_path(self):
        assert construct_sql_uri(database_type="duckdb", database="/tmp/test.duckdb") == "duckdb:////tmp/test.duckdb"

    def test_construct_uri_strips_existing_prefix(self):
        uri = construct_sql_uri(database_type="duckdb", database="duckdb:///data/x.duckdb")
        assert uri == "duckdb:///data/x.duckdb"

    def test_get_sqlalchemy_uri_duckdb_unchanged(self):
        assert get_sqlalchemy_uri("duckdb:///x.duckdb") == "duckdb:///x.duckdb"


def _source(duckdb_db, **kwargs) -> SqlSource:
    return SqlSource(connection_string=f"duckdb:///{duckdb_db}", **kwargs)


class TestDuckDBIntegration:
    def test_table_mode_read(self, duckdb_db):
        df = _source(duckdb_db, table_name="movies").get_pl_df()
        assert df.height == 3
        assert df.columns == ["id", "title", "rating", "votes", "description", "is_active"]

    def test_query_mode_read(self, duckdb_db):
        df = _source(duckdb_db, query="SELECT title, rating FROM movies WHERE rating > 8.7").get_pl_df()
        assert df.height == 2

    def test_get_pl_df_caches(self, duckdb_db):
        source = _source(duckdb_db, table_name="actors")
        df = source.get_pl_df()
        assert source.get_pl_df() is df

    def test_validate_ok(self, duckdb_db):
        _source(duckdb_db, table_name="movies").validate()

    def test_validate_missing_table(self, duckdb_db):
        with pytest.raises(Exception):
            _source(duckdb_db, table_name="nope").validate()

    def test_validate_missing_file(self, tmp_path):
        source = SqlSource(connection_string=f"duckdb:///{tmp_path}/missing.duckdb", table_name="t")
        with pytest.raises(Exception):
            source.validate()

    def test_user_query_table_functions_rejected(self, duckdb_db):
        """validate_sql_query blocks duckdb's file-reading table functions in user SQL."""
        with pytest.raises(UnsafeSQLError):
            _source(duckdb_db, query="SELECT * FROM read_parquet('/etc/passwd')")


class TestDuckDBFastSchema:
    """The headline contract: real schema without reading data, in BOTH modes."""

    def test_table_mode_schema_types(self, duckdb_db):
        schema = _source(duckdb_db, table_name="movies").get_schema()
        by_name = {c.column_name: c for c in schema}
        assert len(schema) == 6
        assert by_name["rating"].data_type != "String", "table-mode schema must carry real types"

    def test_query_mode_schema_has_real_types(self, duckdb_db):
        """Query mode used to fall back to an all-String probe; duckdb must not."""
        schema = _source(duckdb_db, query="SELECT id, title, rating FROM movies").get_schema()
        data_types = [c.data_type for c in schema]
        assert data_types.count("String") == 1, f"only title should be String, got {data_types}"

    def test_predicted_schema_matches_materialized(self, duckdb_db):
        source = _source(duckdb_db, table_name="types_zoo")
        predicted = {c.column_name: c.data_type for c in source.get_schema()}
        materialized = _source(duckdb_db, table_name="types_zoo").get_pl_df()
        actual = {name: str(dtype) for name, dtype in materialized.schema.items()}
        for column, predicted_type in predicted.items():
            assert predicted_type == actual[column], (
                f"{column}: predicted {predicted_type} but materialized {actual[column]}"
            )

    def test_schema_prediction_reads_no_data(self, duckdb_db, monkeypatch):
        """Structural proof: schema must come from the LIMIT-0 hook, not a bulk read."""
        from shared.db_dialects.duckdb import DuckDBDialect

        def _no_read(*args, **kwargs):
            raise AssertionError("schema prediction must not perform a bulk read")

        monkeypatch.setattr(DuckDBDialect, "read", _no_read)
        schema = _source(duckdb_db, query="SELECT id, rating FROM movies").get_schema()
        assert len(schema) == 2

    def test_expected_polars_types(self, duckdb_db):
        df = _source(duckdb_db, table_name="types_zoo").get_pl_df()
        assert df.schema["c_uinteger"] == pl.UInt32
        assert df.schema["c_double"] == pl.Float64
        assert df.schema["c_bool"] == pl.Boolean
        assert df.schema["c_varchar"] == pl.Utf8
        assert df.schema["c_date"] == pl.Date
        assert isinstance(df.schema["c_list"], pl.List)
        assert isinstance(df.schema["c_struct"], pl.Struct)


class TestDuckDBBrowse:
    @staticmethod
    def _settings(duckdb_db, schema_name=None) -> DatabaseSettings:
        return DatabaseSettings(
            connection_mode="inline",
            database_connection=DatabaseConnection(database_type="duckdb", database=duckdb_db),
            query_mode="table",
            schema_name=schema_name,
        )

    def test_list_schemas(self, duckdb_db):
        schemas = list_db_schemas(self._settings(duckdb_db), user_id=1)
        assert "main" in schemas

    def test_list_tables_with_schema(self, duckdb_db):
        tables = list_db_tables(self._settings(duckdb_db, schema_name="main"), user_id=1)
        assert {"movies", "actors", "types_zoo"}.issubset(set(tables))

    def test_list_tables_qualified(self, duckdb_db):
        tables = list_db_tables(self._settings(duckdb_db), user_id=1)
        assert {"main.movies", "main.actors"}.issubset(set(tables))


def test_dialect_registered():
    dialect = get_dialect("duckdb")
    assert dialect.file_based is True
    assert dialect.default_port is None
    assert dialect.is_available() is True
