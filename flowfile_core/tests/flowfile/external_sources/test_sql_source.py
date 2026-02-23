import polars as pl
import pytest
from sqlalchemy import create_engine

from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager.db_connections import (
    get_local_database_connection,
    store_database_connection,
)
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.flowfile.sources.external_sources.sql_source.sql_source import (
    SqlSource,
    UnsafeSQLError,
    create_sql_source_from_db_settings,
    get_polars_type,
    get_query_columns,
    get_table_column_types,
    validate_sql_identifier,
    validate_sql_query,
)
from flowfile_core.schemas.input_schema import (
    DatabaseConnection,
    DatabaseSettings,
    FullDatabaseConnection,
    MinimalFieldInfo,
)

try:
    from tests.flowfile_core_test_utils import ensure_password_is_available, is_docker_available
except ModuleNotFoundError:
    import os
    import sys
    sys.path.append(os.path.dirname(os.path.abspath("flowfile_core/tests/flowfile_core_test_utils.py")))
    # noinspection PyUnresolvedReferences
    from flowfile_core_test_utils import ensure_password_is_available, is_docker_available



@pytest.fixture
def expected_schema() -> list[FlowfileColumn]:
    minimal_schema = [MinimalFieldInfo(name='movie_id', data_type='Int64'),
                      MinimalFieldInfo(name='title', data_type='String'),
                      MinimalFieldInfo(name='cast', data_type='String'),
                      MinimalFieldInfo(name='crew', data_type='String')]
    return [FlowfileColumn.create_from_minimal_field_info(field) for field in minimal_schema]


@pytest.fixture
def engine():
    return create_engine("postgresql://testuser:testpass@localhost:5433/testdb")


@pytest.fixture
def sql_source():
    return SqlSource(connection_string="postgresql://testuser:testpass@localhost:5433/testdb",
                     table_name='credits')


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_sql_source_with_table_and_schema(expected_schema):
    sql_source = SqlSource(connection_string="postgresql://testuser:testpass@localhost:5433/testdb",
                           table_name='credits',
                           schema_name='public')
    assert sql_source.get_schema() == expected_schema, "Schema does not match expected schema"


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_sql_source_with_table_and_no_schema(expected_schema):
    sql_source = SqlSource(connection_string="postgresql://testuser:testpass@localhost:5433/testdb",
                           table_name='public.credits')
    assert sql_source.get_schema() == expected_schema, "Schema does not match expected schema"


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_sql_source_with_query(expected_schema):
    sql_source = SqlSource(connection_string="postgresql://testuser:testpass@localhost:5433/testdb",
                           query="SELECT * FROM credits")
    expected_columns = ['movie_id', 'title', 'cast', 'crew']
    assert [s.column_name for s in sql_source.get_schema()] == expected_columns, "Schema does not match expected schema"


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_get_query_columns(engine):
    expected_columns = ['movie_id', 'title', 'cast', 'crew']

    result_columns = get_query_columns(engine, query_text="SELECT * FROM credits")
    assert result_columns == expected_columns, "Query columns do not match expected columns"


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_get_table_column_types(engine):
    from sqlalchemy.dialects.postgresql import JSONB
    from sqlalchemy.sql.sqltypes import INTEGER, TEXT, Text
    expected = [
        ('movie_id', INTEGER()),
        ('title', TEXT()),
        ('cast', JSONB(astext_type=Text())),
        ('crew', JSONB(astext_type=Text()))  # Assuming the fourth column is named 'crew'
    ]
    result = get_table_column_types(engine, table_name='credits')
    assert str(result) == str(expected), "Table column types do not match expected types"


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_get_pl_df(sql_source):
    """Test that get_pl_df returns a polars DataFrame and caches the result."""
    df = sql_source.get_pl_df()
    assert isinstance(df, pl.DataFrame), "get_pl_df should return a polars DataFrame"

    # Test caching
    cached_df = sql_source.read_result
    assert cached_df is not None, "The result should be cached in read_result"
    assert cached_df is df, "Cached DataFrame should be the same object as returned DataFrame"


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_get_df(sql_source, monkeypatch):
    """Test that get_df converts polars DataFrame to pandas DataFrame."""
    # Mock the get_pl_df method
    mock_pl_df = pl.DataFrame({'col1': [1, 2, 3]})
    monkeypatch.setattr(sql_source, 'get_pl_df', lambda: mock_pl_df)

    # Call get_df
    result = sql_source.get_df()

    # Check it's a pandas DataFrame
    import pandas as pd
    assert isinstance(result, pd.DataFrame), "get_df should return a pandas DataFrame"
    assert len(result) == 3, "DataFrame should have the correct number of rows"


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_data_getter(sql_source, monkeypatch):
    """Test that data_getter yields dictionaries from the DataFrame."""
    # Mock the get_pl_df method
    mock_pl_df = pl.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
    monkeypatch.setattr(sql_source, 'get_pl_df', lambda: mock_pl_df)

    # Get results from data_getter
    results = list(sql_source.data_getter())

    # Check the results
    assert len(results) == 2, "Should have 2 rows of data"
    assert results[0] == {'col1': 1, 'col2': 'a'}, "First row should match expected values"
    assert results[1] == {'col1': 2, 'col2': 'b'}, "Second row should match expected values"


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_get_sample_with_table(monkeypatch):
    """Test get_sample with table mode."""
    # Create SqlSource with table mode
    sql_source = SqlSource(
        connection_string="postgresql://testuser:testpass@localhost:5433/testdb",
        table_name='credits'
    )

    # Mock pl.read_database_uri
    expected_data = [{'col1': 1, 'col2': 'a'}, {'col1': 2, 'col2': 'b'}]
    mock_df = pl.DataFrame(expected_data)
    monkeypatch.setattr(pl, 'read_database_uri', lambda query, conn: mock_df)

    # Get samples
    samples = list(sql_source.get_sample(n=10))

    # Check results
    assert len(samples) == 2, "Should return all rows in the sample DataFrame"
    assert samples == expected_data, "Sample data should match expected data"


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_get_sample_with_query(monkeypatch):
    """Test get_sample with query mode."""
    # Create SqlSource with query mode
    sql_source = SqlSource(
        connection_string="postgresql://testuser:testpass@localhost:5433/testdb",
        query="SELECT * FROM credits"
    )

    mock_df = pl.DataFrame({'col1': [1, 2, 3, 4, 5], 'col2': ['a', 'b', 'c', 'd', 'e']})
    monkeypatch.setattr(sql_source, 'get_pl_df', lambda: mock_df)

    # Get samples with n=3
    samples = list(sql_source.get_sample(n=3))

    # Check results
    assert len(samples) == 3, "Should return n rows in the sample"
    assert samples[0] == {'col1': 1, 'col2': 'a'}, "First sample should match expected data"


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_get_initial_data(sql_source):
    """Test that get_initial_data returns an empty list."""
    result = sql_source.get_initial_data()
    assert result == [], "get_initial_data should return an empty list"


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_get_iter(sql_source, monkeypatch):
    """Test that get_iter yields data from data_getter."""
    # Mock data_getter
    expected_data = [{'col1': 1}, {'col1': 2}]
    monkeypatch.setattr(sql_source, 'data_getter', lambda: (item for item in expected_data))

    # Get results from get_iter
    results = list(sql_source.get_iter())

    # Check results
    assert results == expected_data, "get_iter should yield data from data_getter"


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_invalid_initialization():
    """Test that SqlSource raises exceptions for invalid initialization parameters."""
    # Neither query nor table_name
    with pytest.raises(ValueError) as exc_info:
        SqlSource(connection_string="postgresql://testuser:testpass@localhost:5433/testdb")
    assert "Either table_name or query must be provided" in str(exc_info.value)

    # Both query and table_name
    with pytest.raises(ValueError) as exc_info:
        SqlSource(
            connection_string="postgresql://testuser:testpass@localhost:5433/testdb",
            query="SELECT * FROM credits",
            table_name="credits"
        )
    assert "Only one of table_name or query can be provided" in str(exc_info.value)


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_parse_table_name():
    """Test the _parse_table_name static method."""
    # Table with schema
    schema, table = SqlSource._parse_table_name("public.credits")
    assert schema == "public", "Schema should be 'public'"
    assert table == "credits", "Table should be 'credits'"

    # Table without schema
    schema, table = SqlSource._parse_table_name("credits")
    assert schema is None, "Schema should be None"
    assert table == "credits", "Table should be 'credits'"

    # Multi-part schema
    schema, table = SqlSource._parse_table_name("analytics.public.credits")
    assert schema == "analytics.public", "Schema should be 'analytics.public'"
    assert table == "credits", "Table should be 'credits'"


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_parse_schema(sql_source, expected_schema, monkeypatch):
    """Test that parse_schema calls get_flow_file_columns."""
    monkeypatch.setattr(sql_source, 'get_flow_file_columns', lambda: expected_schema)
    result = sql_source.parse_schema()
    assert result == expected_schema, "parse_schema should return result from get_flow_file_columns"


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_get_polars_type():
    """Test the get_polars_type function converts SQLAlchemy types to polars types."""
    import polars as pl
    from sqlalchemy.sql.sqltypes import BOOLEAN, DATE, DATETIME, FLOAT, INTEGER, VARCHAR

    # Test INTEGER type
    int_type = get_polars_type(INTEGER())
    assert isinstance(int_type(), pl.Int64), "INTEGER should convert to polars Int64"

    # Test FLOAT type
    float_type = get_polars_type(FLOAT())
    assert isinstance(float_type(), pl.Float64), "FLOAT should convert to polars Float64"

    # Test VARCHAR type
    string_type = get_polars_type(VARCHAR())
    assert isinstance(string_type(), pl.String), "VARCHAR should convert to polars String"

    # Test BOOLEAN type
    bool_type = get_polars_type(BOOLEAN())
    assert isinstance(bool_type(), pl.Boolean), "BOOLEAN should convert to polars Boolean"

    # Test DATE type
    date_type = get_polars_type(DATE())
    assert isinstance(date_type(), pl.Date), "DATE should convert to polars Date"

    # Test DATETIME type
    datetime_type = get_polars_type(DATETIME())
    assert isinstance(datetime_type(), pl.Datetime), "DATETIME should convert to polars Datetime"

    # Test unknown type (should default to String)
    from sqlalchemy.sql.sqltypes import Enum
    unknown_type = get_polars_type(Enum("A", "B"))
    assert isinstance(unknown_type(), pl.String), "Unknown type should convert to polars String"


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_create_sql_source_from_db_settings_connection():
    """Test the create_sql_source_from_db_settings function."""
    # Mock the settings

    database_connection = DatabaseConnection(database_type='postgresql',
                                             username='testuser',
                                             password_ref='test_database_pw',
                                             host='localhost',
                                             port=5433,
                                             database='testdb')
    database_settings = DatabaseSettings(database_connection=database_connection,
                                         schema_name='public', table_name='movies',
                                         connection_mode='inline')
    ensure_password_is_available()
    sql_source = create_sql_source_from_db_settings(database_settings, user_id=1)
    assert isinstance(sql_source, SqlSource), "Should create an instance of SqlSource"

    try:
        sql_source.validate()
    except Exception as e:
        raise AssertionError(f"Validation failed: {e}")


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_create_sql_source_from_db_settings_connection_reference():
    """Test the create_sql_source_from_db_settings function."""
    # Mock the settings
    database_connection = FullDatabaseConnection(database_type='postgresql',
                                                 username='testuser',
                                                 host='localhost',
                                                 port=5433,
                                                 database='testdb',
                                                 password='testpass',
                                                 connection_name="database_test_connection")

    database_settings = DatabaseSettings(database_connection_name='database_test_connection',
                                                      schema_name='public', table_name='movies',
                                                      connection_mode='reference')
    db_connection = get_local_database_connection('database_test_connection', 1)
    if db_connection is None:
        with get_db_context() as db:
            store_database_connection(db, connection=database_connection, user_id=1)

    sql_source = create_sql_source_from_db_settings(database_settings, user_id=1)
    assert isinstance(sql_source, SqlSource), "Should create an instance of SqlSource"

    try:
        sql_source.validate()
    except Exception as e:
        raise AssertionError(f"Validation failed: {e}")


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_error_sql_source_validate():
    database_connection = DatabaseConnection(database_type='postgresql',
                                             username='testuser',
                                             password_ref='test_database_pw',
                                             host='localhost',
                                             port=5433,
                                             database='testdb')
    database_settings = DatabaseSettings(database_connection=database_connection,
                                         schema_name='public', table_name='moviess',
                                         connection_mode='inline')
    ensure_password_is_available()
    sql_source = create_sql_source_from_db_settings(database_settings, user_id=1)
    assert isinstance(sql_source, SqlSource), "Should create an instance of SqlSource"
    with pytest.raises(Exception) as excinfo:
        sql_source.validate()
    error_message = str(excinfo.value)
    assert 'relation "public.moviess" does not exist' in error_message


# ============================================================================
# SQL Query Validation Tests (no Docker required)
# ============================================================================


class TestSQLQueryValidation:
    """Tests for SQL query validation - these don't require a database connection."""

    def test_valid_select_query(self):
        """Test that valid SELECT queries pass validation."""
        valid_queries = [
            "SELECT * FROM users",
            "SELECT id, name FROM users WHERE id = 1",
            "SELECT a.id, b.name FROM users a JOIN orders b ON a.id = b.user_id",
            "select * from users",  # lowercase
            "  SELECT * FROM users  ",  # whitespace
            "SELECT COUNT(*) FROM users GROUP BY status",
            "SELECT * FROM users ORDER BY id LIMIT 10",
        ]
        for query in valid_queries:
            validate_sql_query(query)  # Should not raise

    def test_valid_cte_query(self):
        """Test that valid CTE (WITH) queries pass validation."""
        valid_queries = [
            "WITH cte AS (SELECT * FROM users) SELECT * FROM cte",
            "WITH active_users AS (SELECT * FROM users WHERE active = true) SELECT * FROM active_users",
            "with cte as (select * from users) select * from cte",  # lowercase
        ]
        for query in valid_queries:
            validate_sql_query(query)  # Should not raise

    def test_empty_query_rejected(self):
        """Test that empty queries are rejected."""
        with pytest.raises(UnsafeSQLError) as exc_info:
            validate_sql_query("")
        assert "cannot be empty" in str(exc_info.value)

        with pytest.raises(UnsafeSQLError) as exc_info:
            validate_sql_query("   ")
        assert "cannot be empty" in str(exc_info.value)

    def test_drop_statement_rejected(self):
        """Test that DROP statements are blocked."""
        dangerous_queries = [
            "DROP TABLE users",
            "DROP DATABASE mydb",
            "DROP INDEX idx_users",
            "drop table users",  # lowercase
            "  DROP TABLE users",  # with whitespace
        ]
        for query in dangerous_queries:
            with pytest.raises(UnsafeSQLError) as exc_info:
                validate_sql_query(query)
            # DROP statements don't start with SELECT, so they get caught by that check
            error_msg = str(exc_info.value)
            assert "DROP" in error_msg or "Only SELECT" in error_msg

    def test_delete_statement_rejected(self):
        """Test that DELETE statements are blocked."""
        dangerous_queries = [
            "DELETE FROM users",
            "DELETE FROM users WHERE id = 1",
            "delete from users",  # lowercase
        ]
        for query in dangerous_queries:
            with pytest.raises(UnsafeSQLError) as exc_info:
                validate_sql_query(query)
            # DELETE doesn't start with SELECT, so it gets caught as non-SELECT
            assert "Only SELECT queries are allowed" in str(exc_info.value)

    def test_update_statement_rejected(self):
        """Test that UPDATE statements are blocked."""
        dangerous_queries = [
            "UPDATE users SET name = 'test'",
            "UPDATE users SET name = 'test' WHERE id = 1",
            "update users set name = 'test'",  # lowercase
        ]
        for query in dangerous_queries:
            with pytest.raises(UnsafeSQLError) as exc_info:
                validate_sql_query(query)
            # UPDATE doesn't start with SELECT
            assert "Only SELECT queries are allowed" in str(exc_info.value)

    def test_insert_statement_rejected(self):
        """Test that INSERT statements are blocked."""
        dangerous_queries = [
            "INSERT INTO users (name) VALUES ('test')",
            "INSERT INTO users SELECT * FROM other_table",
            "insert into users (name) values ('test')",  # lowercase
        ]
        for query in dangerous_queries:
            with pytest.raises(UnsafeSQLError) as exc_info:
                validate_sql_query(query)
            # INSERT doesn't start with SELECT
            assert "Only SELECT queries are allowed" in str(exc_info.value)

    def test_create_statement_rejected(self):
        """Test that CREATE statements are blocked."""
        dangerous_queries = [
            "CREATE TABLE users (id INT)",
            "CREATE INDEX idx_users ON users(id)",
            "CREATE DATABASE newdb",
            "create table users (id int)",  # lowercase
        ]
        for query in dangerous_queries:
            with pytest.raises(UnsafeSQLError) as exc_info:
                validate_sql_query(query)
            # CREATE doesn't start with SELECT
            assert "Only SELECT queries are allowed" in str(exc_info.value)

    def test_alter_statement_rejected(self):
        """Test that ALTER statements are blocked."""
        dangerous_queries = [
            "ALTER TABLE users ADD COLUMN email VARCHAR(255)",
            "ALTER TABLE users DROP COLUMN email",
            "alter table users add column email varchar(255)",  # lowercase
        ]
        for query in dangerous_queries:
            with pytest.raises(UnsafeSQLError) as exc_info:
                validate_sql_query(query)
            # ALTER doesn't start with SELECT
            assert "Only SELECT queries are allowed" in str(exc_info.value)

    def test_truncate_statement_rejected(self):
        """Test that TRUNCATE statements are blocked."""
        dangerous_queries = [
            "TRUNCATE TABLE users",
            "TRUNCATE users",
            "truncate table users",  # lowercase
        ]
        for query in dangerous_queries:
            with pytest.raises(UnsafeSQLError) as exc_info:
                validate_sql_query(query)
            # TRUNCATE doesn't start with SELECT
            assert "Only SELECT queries are allowed" in str(exc_info.value)

    def test_grant_revoke_rejected(self):
        """Test that GRANT and REVOKE statements are blocked."""
        dangerous_queries = [
            "GRANT SELECT ON users TO public",
            "REVOKE SELECT ON users FROM public",
        ]
        for query in dangerous_queries:
            with pytest.raises(UnsafeSQLError) as exc_info:
                validate_sql_query(query)
            assert "Only SELECT queries are allowed" in str(exc_info.value)

    def test_comments_are_stripped_before_validation(self):
        """Test that SQL comments are properly stripped before validation.

        This is correct security behavior - dangerous SQL inside comments
        is not executed by the database, so we strip comments first.
        """
        # These queries have dangerous SQL inside comments - after stripping,
        # they become valid SELECT queries
        safe_after_stripping = [
            "SELECT * FROM users; -- DROP TABLE users",
            "SELECT * FROM users /* DROP TABLE users */",
            "SELECT /* DROP */ * FROM users",
        ]
        for query in safe_after_stripping:
            validate_sql_query(query)  # Should not raise - comments are stripped

    def test_dangerous_sql_outside_comments_blocked(self):
        """Test that dangerous SQL outside of comments is blocked."""
        # This has DROP outside of any comment - should be caught
        with pytest.raises(UnsafeSQLError) as exc_info:
            validate_sql_query("SELECT * FROM users; DROP TABLE users")
        assert "DROP" in str(exc_info.value)

    def test_subquery_with_dangerous_statement_blocked(self):
        """Test that dangerous statements hidden in subqueries are blocked."""
        dangerous_queries = [
            "SELECT * FROM (DELETE FROM users RETURNING *) AS deleted",
            "SELECT * FROM users WHERE id IN (SELECT id FROM users; DROP TABLE users)",
        ]
        for query in dangerous_queries:
            with pytest.raises(UnsafeSQLError) as exc_info:
                validate_sql_query(query)
            error_msg = str(exc_info.value)
            assert "DELETE" in error_msg or "DROP" in error_msg

    def test_exec_call_statements_rejected(self):
        """Test that EXEC and CALL statements are blocked."""
        dangerous_queries = [
            "EXEC sp_some_procedure",
            "EXECUTE sp_some_procedure",
            "CALL some_procedure()",
        ]
        for query in dangerous_queries:
            with pytest.raises(UnsafeSQLError) as exc_info:
                validate_sql_query(query)
            assert "Only SELECT queries are allowed" in str(exc_info.value)

    def test_sql_source_rejects_unsafe_query(self):
        """Test that SqlSource constructor rejects unsafe queries."""
        with pytest.raises(UnsafeSQLError) as exc_info:
            SqlSource(
                connection_string="postgresql://test:test@localhost/test",
                query="DROP TABLE users"
            )
        assert "Only SELECT queries are allowed" in str(exc_info.value)

    def test_sql_source_accepts_safe_query(self):
        """Test that SqlSource constructor accepts safe SELECT queries."""
        # This should not raise - it's a valid SELECT query
        sql_source = SqlSource(
            connection_string="postgresql://test:test@localhost/test",
            query="SELECT * FROM users WHERE id = 1"
        )
        assert sql_source.query == "SELECT * FROM users WHERE id = 1"
        assert sql_source.query_mode == "query"

    def test_table_mode_bypasses_validation(self):
        """Test that table mode (auto-generated queries) bypasses validation."""
        # Table mode generates its own SELECT query, so no user input validation needed
        sql_source = SqlSource(
            connection_string="postgresql://test:test@localhost/test",
            table_name="users"
        )
        assert sql_source.query_mode == "table"
        assert "SELECT * FROM users" in sql_source.query


# ============================================================================
# SQL Identifier Validation Tests (no Docker required)
# ============================================================================


class TestSQLIdentifierValidation:
    """Tests for SQL identifier validation - these don't require a database connection."""

    def test_valid_identifiers(self):
        """Test that valid identifiers pass validation."""
        valid_identifiers = [
            "users",
            "my_table",
            "_private",
            "Table1",
            "a",
            "_",
            "UPPER_CASE",
            "schema123",
        ]
        for identifier in valid_identifiers:
            validate_sql_identifier(identifier)  # Should not raise

    def test_valid_dotted_identifiers(self):
        """Test that valid dotted identifiers (schema.table) pass validation."""
        valid_identifiers = [
            "public.users",
            "my_schema.my_table",
            "analytics.public.credits",
        ]
        for identifier in valid_identifiers:
            validate_sql_identifier(identifier)  # Should not raise

    def test_sql_injection_in_table_name_rejected(self):
        """Test that SQL injection attempts in table names are rejected."""
        malicious_names = [
            "users; DROP TABLE users;--",
            "users; DELETE FROM users",
            "users UNION SELECT * FROM secrets",
            "users' OR '1'='1",
        ]
        for name in malicious_names:
            with pytest.raises(UnsafeSQLError):
                validate_sql_identifier(name, "table name")

    def test_sql_injection_in_schema_name_rejected(self):
        """Test that SQL injection attempts in schema names are rejected."""
        malicious_names = [
            "public; DROP TABLE users;--",
            "public' OR '1'='1",
            "public; SELECT * FROM secrets",
        ]
        for name in malicious_names:
            with pytest.raises(UnsafeSQLError):
                validate_sql_identifier(name, "schema name")

    def test_empty_identifier_rejected(self):
        """Test that empty and whitespace-only identifiers are rejected."""
        with pytest.raises(UnsafeSQLError) as exc_info:
            validate_sql_identifier("", "table name")
        assert "cannot be empty" in str(exc_info.value)

        with pytest.raises(UnsafeSQLError) as exc_info:
            validate_sql_identifier("   ", "table name")
        assert "cannot be empty" in str(exc_info.value)

    def test_special_characters_rejected(self):
        """Test that special characters in identifiers are rejected."""
        invalid_names = [
            "table'name",
            'table"name',
            "table--name",
            "table;name",
            "table name",
            "table(name)",
            "table=name",
            "1table",
        ]
        for name in invalid_names:
            with pytest.raises(UnsafeSQLError):
                validate_sql_identifier(name, "table name")

    def test_sql_source_rejects_malicious_table_name(self):
        """Test that SqlSource constructor rejects malicious table names."""
        with pytest.raises(UnsafeSQLError):
            SqlSource(
                connection_string="postgresql://test:test@localhost/test",
                table_name="users; DROP TABLE users;--"
            )

    def test_sql_source_rejects_malicious_schema_name(self):
        """Test that SqlSource constructor rejects malicious schema names."""
        with pytest.raises(UnsafeSQLError):
            SqlSource(
                connection_string="postgresql://test:test@localhost/test",
                table_name="users",
                schema_name="public; DROP TABLE users;--"
            )

    def test_sql_source_accepts_valid_table_with_schema(self):
        """Test that SqlSource accepts valid table and schema names."""
        sql_source = SqlSource(
            connection_string="postgresql://test:test@localhost/test",
            table_name="users",
            schema_name="public"
        )
        assert sql_source.query == "SELECT * FROM public.users"

    def test_sql_source_accepts_dotted_table_name(self):
        """Test that SqlSource accepts dotted table names (schema.table notation)."""
        sql_source = SqlSource(
            connection_string="postgresql://test:test@localhost/test",
            table_name="public.users"
        )
        assert sql_source.query == "SELECT * FROM public.users"

    def test_pydantic_database_settings_rejects_malicious_table(self):
        """Test that DatabaseSettings Pydantic model rejects malicious table names."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            DatabaseSettings(
                table_name="users; DROP TABLE users;--",
                query_mode="table",
                connection_mode="inline",
                database_connection=DatabaseConnection(
                    database_type="postgresql",
                    username="test",
                    password_ref="test",
                    host="localhost",
                    port=5432,
                    database="testdb",
                ),
            )

    def test_pydantic_database_settings_rejects_malicious_schema(self):
        """Test that DatabaseSettings Pydantic model rejects malicious schema names."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            DatabaseSettings(
                table_name="users",
                schema_name="public; DROP TABLE users;--",
                query_mode="table",
                connection_mode="inline",
                database_connection=DatabaseConnection(
                    database_type="postgresql",
                    username="test",
                    password_ref="test",
                    host="localhost",
                    port=5432,
                    database="testdb",
                ),
            )

    def test_pydantic_database_settings_accepts_valid_identifiers(self):
        """Test that DatabaseSettings accepts valid table and schema names."""
        settings = DatabaseSettings(
            table_name="users",
            schema_name="public",
            query_mode="table",
            connection_mode="inline",
            database_connection=DatabaseConnection(
                database_type="postgresql",
                username="test",
                password_ref="test",
                host="localhost",
                port=5432,
                database="testdb",
            ),
        )
        assert settings.table_name == "users"
        assert settings.schema_name == "public"
