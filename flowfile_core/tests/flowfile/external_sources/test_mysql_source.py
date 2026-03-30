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
    from test_utils.mysql.fixtures import is_docker_available, can_connect_to_db
except ImportError:
    import os
    import sys

    sys.path.append(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
    from test_utils.mysql.fixtures import is_docker_available, can_connect_to_db


MYSQL_CONN_STRING = "mysql://testuser:testpass@localhost:3307/testdb"
MYSQL_SA_CONN_STRING = "mysql+pymysql://testuser:testpass@localhost:3307/testdb"

mysql_available = pytest.mark.skipif(
    not is_docker_available() or not can_connect_to_db(),
    reason="MySQL Docker container is not available or not running",
)


# ============================================================================
# URI Construction Tests (no Docker required)
# ============================================================================


class TestMySQLURIConstruction:
    """Tests for MySQL URI construction and SQLAlchemy URI conversion."""

    def test_construct_mysql_uri(self):
        """Test that construct_sql_uri builds correct MySQL URIs."""
        uri = construct_sql_uri(
            database_type="mysql",
            host="localhost",
            port=3306,
            username="myuser",
            database="mydb",
        )
        assert uri == "mysql://myuser@localhost:3306/mydb"

    def test_construct_mysql_uri_with_password(self):
        """Test MySQL URI construction with password."""
        from pydantic import SecretStr

        uri = construct_sql_uri(
            database_type="mysql",
            host="localhost",
            port=3306,
            username="myuser",
            password=SecretStr("myp@ss"),
            database="mydb",
        )
        assert uri == "mysql://myuser:myp%40ss@localhost:3306/mydb"

    def test_get_sqlalchemy_uri_mysql(self):
        """Test that MySQL URIs are converted to SQLAlchemy format."""
        base_uri = "mysql://user:pass@localhost:3306/db"
        sa_uri = get_sqlalchemy_uri(base_uri)
        assert sa_uri == "mysql+pymysql://user:pass@localhost:3306/db"

    def test_get_sqlalchemy_uri_postgresql_unchanged(self):
        """Test that PostgreSQL URIs are not modified."""
        pg_uri = "postgresql://user:pass@localhost:5432/db"
        assert get_sqlalchemy_uri(pg_uri) == pg_uri

    def test_get_sqlalchemy_uri_sqlite_unchanged(self):
        """Test that SQLite URIs are not modified."""
        sqlite_uri = "sqlite:///path/to/db"
        assert get_sqlalchemy_uri(sqlite_uri) == sqlite_uri


# ============================================================================
# MySQL Type Mapping Tests (no Docker required)
# ============================================================================


class TestMySQLTypeMappings:
    """Tests for MySQL-specific type mappings."""

    def test_mysql_unsigned_types(self):
        """Test that MySQL unsigned integer types map correctly."""
        assert get_polars_type("int unsigned") == pl.UInt64
        assert get_polars_type("bigint unsigned") == pl.UInt64
        assert get_polars_type("smallint unsigned") == pl.UInt16
        assert get_polars_type("tinyint unsigned") == pl.UInt8
        assert get_polars_type("mediumint unsigned") == pl.UInt32

    def test_mysql_year_type(self):
        """Test that MySQL YEAR type maps to Int16."""
        assert get_polars_type("year") == pl.Int16

    def test_mysql_enum_type(self):
        """Test that MySQL ENUM type maps to String."""
        assert get_polars_type("enum") == pl.String

    def test_mysql_set_type(self):
        """Test that MySQL SET type maps to List."""
        assert get_polars_type("set") == pl.List

    def test_mysql_tinyint(self):
        """Test that TINYINT maps correctly."""
        assert get_polars_type("tinyint") == pl.Int8

    def test_mysql_mediumint(self):
        """Test that MEDIUMINT maps correctly."""
        assert get_polars_type("mediumint") == pl.Int32

    def test_mysql_text_types(self):
        """Test MySQL text type variants."""
        assert get_polars_type("tinytext") == pl.Utf8
        assert get_polars_type("mediumtext") == pl.Utf8
        assert get_polars_type("longtext") == pl.Utf8

    def test_mysql_blob_types(self):
        """Test MySQL blob type variants."""
        assert get_polars_type("tinyblob") == pl.Binary
        assert get_polars_type("mediumblob") == pl.Binary
        assert get_polars_type("longblob") == pl.Binary

    def test_mysql_json_type(self):
        """Test that MySQL JSON type maps to Utf8."""
        assert get_polars_type("json") == pl.Utf8

    def test_mysql_bit_type(self):
        """Test that MySQL BIT type maps to Boolean."""
        assert get_polars_type("bit") == pl.Boolean


# ============================================================================
# MySQL Integration Tests (Docker required)
# ============================================================================


@mysql_available
class TestMySQLIntegration:
    """Integration tests requiring a running MySQL container."""

    @pytest.fixture
    def engine(self):
        return create_engine(MYSQL_SA_CONN_STRING)

    @pytest.fixture
    def sql_source(self):
        return SqlSource(connection_string=MYSQL_CONN_STRING, table_name="movies")

    def test_sql_source_with_table(self, sql_source):
        """Test SqlSource can read a MySQL table."""
        schema = sql_source.get_schema()
        column_names = [col.column_name for col in schema]
        assert "id" in column_names
        assert "title" in column_names
        assert "release_year" in column_names
        assert "genre" in column_names

    def test_sql_source_with_query(self):
        """Test SqlSource with a custom query against MySQL."""
        sql_source = SqlSource(
            connection_string=MYSQL_CONN_STRING,
            query="SELECT id, title, rating FROM movies WHERE rating > 8.5",
        )
        columns = [s.column_name for s in sql_source.get_schema()]
        assert columns == ["id", "title", "rating"]

    def test_get_pl_df(self, sql_source):
        """Test reading MySQL data into a Polars DataFrame."""
        df = sql_source.get_pl_df()
        assert isinstance(df, pl.DataFrame)
        assert len(df) > 0
        assert "title" in df.columns

    def test_get_pl_df_caching(self, sql_source):
        """Test that DataFrame results are cached."""
        df1 = sql_source.get_pl_df()
        df2 = sql_source.get_pl_df()
        assert df1 is df2

    def test_get_query_columns(self, engine):
        """Test getting column names from a MySQL query."""
        columns = get_query_columns(engine, "SELECT id, title FROM movies")
        assert columns == ["id", "title"]

    def test_get_table_column_types(self, engine):
        """Test getting column types from a MySQL table."""
        column_types = get_table_column_types(engine, "movies")
        column_names = [name for name, _ in column_types]
        assert "id" in column_names
        assert "title" in column_names

    def test_get_sample(self, sql_source):
        """Test getting a sample of data from MySQL."""
        samples = list(sql_source.get_sample(n=3))
        assert len(samples) <= 3
        assert "title" in samples[0]

    def test_validate_succeeds(self, sql_source):
        """Test that validation succeeds for valid MySQL table."""
        sql_source.validate()  # Should not raise

    def test_validate_fails_for_missing_table(self):
        """Test that validation fails for a non-existent MySQL table."""
        sql_source = SqlSource(
            connection_string=MYSQL_CONN_STRING,
            table_name="nonexistent_table",
        )
        with pytest.raises(Exception):
            sql_source.validate()

    def test_read_actors_table(self):
        """Test reading the actors table to verify MEDIUMINT and BOOLEAN types."""
        sql_source = SqlSource(
            connection_string=MYSQL_CONN_STRING,
            table_name="actors",
        )
        df = sql_source.get_pl_df()
        assert isinstance(df, pl.DataFrame)
        assert "name" in df.columns
        assert "birth_year" in df.columns
        assert len(df) == 5

    def test_data_getter(self, sql_source):
        """Test that data_getter yields dictionaries."""
        results = list(sql_source.data_getter())
        assert len(results) > 0
        assert isinstance(results[0], dict)
        assert "title" in results[0]
