import polars as pl
import pytest
from sqlalchemy import create_engine

from flowfile_core.flowfile.sources.external_sources.sql_source.sql_source import (
    SqlSource,
    get_polars_type,
    get_query_columns,
    get_table_column_types,
)
from flowfile_core.flowfile.sources.external_sources.sql_source.utils import (
    construct_sql_uri,
    get_sqlalchemy_uri,
)

try:
    from test_utils.mssql.fixtures import is_docker_available, can_connect_to_db
except ImportError:
    import os
    import sys

    sys.path.append(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
    from test_utils.mssql.fixtures import is_docker_available, can_connect_to_db


MSSQL_CONN_STRING = "mssql://testuser:testpass@localhost:1434/testdb"
MSSQL_SA_CONN_STRING = "mssql+pymssql://testuser:testpass@localhost:1434/testdb"

mssql_available = pytest.mark.skipif(
    not is_docker_available() or not can_connect_to_db(),
    reason="SQL Server Docker container is not available or not running",
)


# URI Construction Tests (no Docker required)


class TestMSSQLURIConstruction:
    """Tests for SQL Server URI construction and SQLAlchemy URI conversion."""

    def test_construct_mssql_uri(self):
        """Test that construct_sql_uri builds correct SQL Server URIs."""
        uri = construct_sql_uri(
            database_type="mssql",
            host="localhost",
            port=1433,
            username="myuser",
            database="mydb",
        )
        assert uri == "mssql://myuser@localhost:1433/mydb"

    def test_construct_mssql_uri_with_password(self):
        """Test SQL Server URI construction with password."""
        from pydantic import SecretStr

        uri = construct_sql_uri(
            database_type="mssql",
            host="localhost",
            port=1433,
            username="myuser",
            password=SecretStr("myp@ss"),
            database="mydb",
        )
        assert uri == "mssql://myuser:myp%40ss@localhost:1433/mydb"

    def test_get_sqlalchemy_uri_mssql(self):
        """Test that SQL Server URIs are converted to the pymssql SQLAlchemy format."""
        base_uri = "mssql://user:pass@localhost:1433/db"
        sa_uri = get_sqlalchemy_uri(base_uri)
        assert sa_uri == "mssql+pymssql://user:pass@localhost:1433/db"


# SQL Server Type Mapping Tests (no Docker required)


class TestMSSQLTypeMappings:
    """Tests for the MSSQL type names in the generic SQL-type→Polars map."""

    def test_mssql_string_types(self):
        assert get_polars_type("uniqueidentifier") == pl.Utf8
        assert get_polars_type("ntext") == pl.Utf8
        assert get_polars_type("xml") == pl.Utf8
        assert get_polars_type("hierarchyid") == pl.Utf8

    def test_mssql_datetime_types(self):
        assert get_polars_type("datetime2") == pl.Datetime
        assert get_polars_type("smalldatetime") == pl.Datetime
        assert get_polars_type("datetimeoffset") == pl.Datetime

    def test_mssql_numeric_types(self):
        assert get_polars_type("money") == pl.Decimal
        assert get_polars_type("smallmoney") == pl.Decimal
        assert get_polars_type("tinyint") == pl.Int8

    def test_mssql_bit_type(self):
        assert get_polars_type("bit") == pl.Boolean

    def test_mssql_image_type(self):
        assert get_polars_type("image") == pl.Binary


# Sample-query shape tests (no Docker required)


class TestMSSQLSampleQuery:
    """T-SQL has no LIMIT clause; sample queries must use TOP."""

    def test_table_mode_sample_query_uses_top(self):
        source = SqlSource(connection_string=MSSQL_CONN_STRING, table_name="movies")
        assert source.get_sample_query() == "SELECT TOP 1 * FROM movies"

    def test_query_mode_sample_query_uses_top(self):
        source = SqlSource(connection_string=MSSQL_CONN_STRING, query="SELECT id FROM movies")
        assert source.get_sample_query() == "select TOP 1 * from (SELECT id FROM movies) as main_query"

    def test_dialect_resolved_from_uri_scheme(self):
        source = SqlSource(connection_string=MSSQL_CONN_STRING, table_name="movies")
        assert source.dialect.name == "mssql"
        assert source.database_type == "mssql"


# SQL Server Integration Tests (Docker required)


@mssql_available
class TestMSSQLIntegration:
    """Integration tests requiring a running SQL Server container."""

    @pytest.fixture
    def engine(self):
        return create_engine(MSSQL_SA_CONN_STRING)

    @pytest.fixture
    def sql_source(self):
        return SqlSource(connection_string=MSSQL_CONN_STRING, table_name="movies")

    def test_sql_source_with_table(self, sql_source):
        """Test SqlSource can read a SQL Server table."""
        schema = sql_source.get_schema()
        column_names = [col.column_name for col in schema]
        assert "id" in column_names
        assert "title" in column_names
        assert "release_year" in column_names
        assert "genre" in column_names

    def test_sql_source_with_query(self):
        """Test SqlSource with a custom query against SQL Server."""
        sql_source = SqlSource(
            connection_string=MSSQL_CONN_STRING,
            query="SELECT id, title, rating FROM movies WHERE rating > 8.5",
        )
        columns = [s.column_name for s in sql_source.get_schema()]
        assert columns == ["id", "title", "rating"]

    def test_fast_schema_predicts_real_types_without_reading(self):
        """The sp_describe-based hooks must return real types, not the all-String probe."""
        sql_source = SqlSource(
            connection_string=MSSQL_CONN_STRING,
            query="SELECT id, title, rating, is_active FROM movies",
        )
        by_name = {col.column_name: col for col in sql_source.get_schema()}
        assert by_name["id"].data_type == "Int32"
        assert by_name["title"].data_type == "String"
        assert by_name["is_active"].data_type == "Boolean"

    def test_predicted_schema_matches_materialized_schema(self, sql_source):
        """Prediction parity: the dialect's fast schema equals what a real read yields."""
        predicted = sql_source.dialect.table_schema(MSSQL_CONN_STRING, "movies", None)
        df = sql_source.get_pl_df()
        assert predicted is not None
        assert dict(predicted) == dict(df.schema)

    def test_uniqueidentifier_reads_as_string(self, sql_source):
        """UNIQUEIDENTIFIER must surface as String, never Object dtype."""
        df = sql_source.get_pl_df()
        assert df.schema["row_guid"] == pl.String

    def test_get_pl_df(self, sql_source):
        """Test reading SQL Server data into a Polars DataFrame."""
        df = sql_source.get_pl_df()
        assert isinstance(df, pl.DataFrame)
        assert len(df) > 0
        assert "title" in df.columns

    def test_get_query_columns(self, engine):
        """Test getting column names from a SQL Server query."""
        columns = get_query_columns(engine, "SELECT id, title FROM movies")
        assert columns == ["id", "title"]

    def test_get_table_column_types(self, engine):
        """Test getting column types from a SQL Server table."""
        column_types = get_table_column_types(engine, "movies")
        column_names = [name for name, _ in column_types]
        assert "id" in column_names
        assert "title" in column_names

    def test_get_sample(self, sql_source):
        """Test getting a sample of data from SQL Server (TOP-limited server-side)."""
        samples = list(sql_source.get_sample(n=3))
        assert len(samples) <= 3
        assert "title" in samples[0]

    def test_validate_succeeds(self, sql_source):
        """Test that validation succeeds for a valid SQL Server table."""
        sql_source.validate()  # Should not raise

    def test_validate_fails_for_missing_table(self):
        """Test that validation fails for a non-existent SQL Server table."""
        sql_source = SqlSource(
            connection_string=MSSQL_CONN_STRING,
            table_name="nonexistent_table",
        )
        with pytest.raises(Exception):
            sql_source.validate()

    def test_read_actors_table(self):
        """Test reading the actors table to verify INT and BIT types."""
        sql_source = SqlSource(
            connection_string=MSSQL_CONN_STRING,
            table_name="actors",
        )
        df = sql_source.get_pl_df()
        assert isinstance(df, pl.DataFrame)
        assert "name" in df.columns
        assert "birth_year" in df.columns
        assert len(df) == 5
        assert df.schema["active"] == pl.Boolean

    def test_write_read_roundtrip(self):
        """Write via the dialect's write strategy, read back, and verify prediction parity."""
        from shared.db_dialects import get_dialect, read_sql
        from shared.db_writer import write_dataframe_to_database

        import logging

        logger = logging.getLogger(__name__)
        dialect = get_dialect("mssql")
        df = pl.DataFrame({"id": [1, 2, 3], "score": [0.5, 1.5, 2.5], "label": ["a", "b", "c"]})

        write_dataframe_to_database(df, database_type="mssql", uri=MSSQL_CONN_STRING, table_name="rt", if_exists="replace")
        write_dataframe_to_database(df, database_type="mssql", uri=MSSQL_CONN_STRING, table_name="rt", if_exists="append")
        with pytest.raises(ValueError, match="Table 'rt' already exists"):
            write_dataframe_to_database(df, database_type="mssql", uri=MSSQL_CONN_STRING, table_name="rt", if_exists="fail")

        result = read_sql("SELECT * FROM rt", MSSQL_CONN_STRING, logger, database_type="mssql")
        assert result.height == 2 * df.height
        assert result.columns == df.columns

        predicted = dialect.table_schema(MSSQL_CONN_STRING, "rt", None)
        assert predicted is not None
        assert dict(predicted) == dict(result.schema)
