import re
from collections.abc import Generator
from typing import Any, Literal

import polars as pl
from sqlalchemy import Engine, create_engine, inspect, text

from flowfile_core.configs import logger
from flowfile_core.flowfile.database_connection_manager.db_connections import get_local_database_connection
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.flowfile.sources.external_sources.base_class import ExternalDataSource
from flowfile_core.flowfile.sources.external_sources.sql_source.utils import construct_sql_uri, get_polars_type
from flowfile_core.schemas.input_schema import DatabaseSettings, MinimalFieldInfo
from flowfile_core.secret_manager.secret_manager import decrypt_secret, get_encrypted_secret

QueryMode = Literal["table", "query"]


class UnsafeSQLError(ValueError):
    """Raised when a SQL query contains unsafe operations."""

    pass


def validate_sql_query(query: str) -> None:
    """
    Validate that a SQL query is safe for execution (read-only SELECT statements only).

    This function checks that the query:
    1. Is a SELECT statement (not INSERT, UPDATE, DELETE, etc.)
    2. Does not contain DDL statements (DROP, CREATE, ALTER, TRUNCATE)
    3. Does not contain other dangerous operations

    Args:
        query: The SQL query string to validate

    Raises:
        UnsafeSQLError: If the query contains unsafe operations
    """
    if not query or not query.strip():
        raise UnsafeSQLError("SQL query cannot be empty")

    # Normalize the query: remove comments and extra whitespace
    normalized = _remove_sql_comments(query)
    normalized = " ".join(normalized.split()).upper()

    # Check if query starts with SELECT (allowing for WITH clauses / CTEs)
    if not _is_select_query(normalized):
        raise UnsafeSQLError(
            "Only SELECT queries are allowed. "
            "The query must start with SELECT or WITH (for common table expressions)."
        )

    # Check for dangerous DDL statements
    ddl_patterns = [
        (r"\bDROP\s+", "DROP statements are not allowed"),
        (r"\bCREATE\s+", "CREATE statements are not allowed"),
        (r"\bALTER\s+", "ALTER statements are not allowed"),
        (r"\bTRUNCATE\s+", "TRUNCATE statements are not allowed"),
        (r"\bRENAME\s+", "RENAME statements are not allowed"),
    ]

    for pattern, error_msg in ddl_patterns:
        if re.search(pattern, normalized):
            raise UnsafeSQLError(error_msg)

    # Check for dangerous DML statements (these shouldn't appear in a SELECT)
    dml_patterns = [
        (r"\bINSERT\s+INTO\b", "INSERT statements are not allowed"),
        (r"\bUPDATE\s+\w+\s+SET\b", "UPDATE statements are not allowed"),
        (r"\bDELETE\s+FROM\b", "DELETE statements are not allowed"),
    ]

    for pattern, error_msg in dml_patterns:
        if re.search(pattern, normalized):
            raise UnsafeSQLError(error_msg)

    # Check for dangerous operations that could be used maliciously
    dangerous_patterns = [
        (r"\bEXEC(UTE)?\s*\(", "EXECUTE statements are not allowed"),
        (r"\bCALL\s+", "CALL statements (stored procedures) are not allowed"),
        (r"\bGRANT\s+", "GRANT statements are not allowed"),
        (r"\bREVOKE\s+", "REVOKE statements are not allowed"),
    ]

    for pattern, error_msg in dangerous_patterns:
        if re.search(pattern, normalized):
            raise UnsafeSQLError(error_msg)


def _remove_sql_comments(query: str) -> str:
    """
    Remove SQL comments from a query string.

    Handles:
    - Single line comments (-- comment)
    - Multi-line comments (/* comment */)
    """
    # Remove multi-line comments
    result = re.sub(r"/\*.*?\*/", " ", query, flags=re.DOTALL)
    # Remove single-line comments
    result = re.sub(r"--.*$", " ", result, flags=re.MULTILINE)
    return result


def _is_select_query(normalized_query: str) -> bool:
    """
    Check if a normalized (uppercase, whitespace-cleaned) query is a SELECT statement.

    Allows:
    - SELECT ...
    - WITH ... SELECT ... (CTEs)
    """
    # Check for direct SELECT
    if normalized_query.startswith("SELECT ") or normalized_query.startswith("SELECT\t"):
        return True

    # Check for WITH clause (CTE) that leads to SELECT
    if normalized_query.startswith("WITH ") or normalized_query.startswith("WITH\t"):
        # CTEs should eventually have a SELECT
        # Make sure there's a SELECT after the WITH clause and no dangerous statements
        if " SELECT " in normalized_query or "\tSELECT " in normalized_query:
            return True

    return False


def get_query_columns(engine: Engine, query_text: str):
    """
    Get column names from a query and assume string type for all columns

    Args:
        engine: SQLAlchemy engine object
        query_text: SQL query as a string

    Returns:
        Dictionary mapping column names to string type
    """
    with engine.connect() as connection:
        # Create a text object from the query
        query = text(query_text)

        # Execute the query to get column names
        result = connection.execute(query)
        column_names = result.keys()
        result.close()  # Close the result to avoid consuming the cursor

        return list(column_names)


def get_table_column_types(engine: Engine, table_name: str, schema: str = None) -> list[tuple[str, Any]]:
    """
    Get column types from a database table using a SQLAlchemy engine

    Args:
        engine: SQLAlchemy engine object
        table_name: Name of the table to inspect
        schema: Optional schema name (e.g., 'public' for PostgreSQL)

    Returns:
        Dictionary mapping column names to their SQLAlchemy types
    """
    inspector = inspect(engine)
    columns = inspector.get_columns(table_name, schema=schema)

    return [(column["name"], column["type"]) for column in columns]


class BaseSqlSource:
    """
    A simplified base class for SQL sources that handles query generation
    without requiring database connection details.
    """

    table_name: str | None = None
    query: str | None = None
    schema_name: str | None = None
    query_mode: QueryMode = "table"
    schema: list[FlowfileColumn] | None = None

    def __init__(
        self,
        query: str = None,
        table_name: str = None,
        schema_name: str = None,
        fields: list[MinimalFieldInfo] | None = None,
    ):
        """
        Initialize a BaseSqlSource object.

        Args:
            query: SQL query string (if query_mode is 'query')
            table_name: Name of the table to query (if query_mode is 'table')
            schema_name: Optional database schema name
            fields: Optional list of field information
        """
        if schema_name == "":
            schema_name = None

        # Validate inputs
        if query is not None and table_name is not None:
            raise ValueError("Only one of table_name or query can be provided")
        if query is None and table_name is None:
            raise ValueError("Either table_name or query must be provided")

        # Set query mode and build query if needed
        if query is not None:
            # Validate user-provided queries for safety (read-only SELECT only)
            validate_sql_query(query)
            self.query_mode = "query"
            self.query = query
        elif table_name is not None:
            self.query_mode = "table"
            self.table_name = table_name
            self.schema_name = schema_name

            # Generate the basic query
            if schema_name is not None and schema_name != "":
                self.query = f"SELECT * FROM {schema_name}.{table_name}"
            else:
                self.query = f"SELECT * FROM {table_name}"

        # Set schema if provided
        if fields:
            self.schema = [FlowfileColumn.from_input(column_name=col.name, data_type=col.data_type) for col in fields]

    def get_sample_query(self) -> str:
        """
        Get a sample query that returns a limited number of rows.
        """
        if self.query_mode == "query":
            return f"select * from ({self.query}) as main_query LIMIT 1"
        else:
            return f"{self.query} LIMIT 1"

    @staticmethod
    def _parse_table_name(table_name: str) -> tuple[str | None, str]:
        """
        Parse a table name that may include a schema.

        Args:
            table_name: Table name possibly in the format 'schema.table'

        Returns:
            Tuple of (schema, table_name)
        """
        table_parts = table_name.split(".")
        if len(table_parts) > 1:
            # Handle schema.table_name format
            schema = ".".join(table_parts[:-1])
            table = table_parts[-1]
            return schema, table
        else:
            return None, table_name


class SqlSource(BaseSqlSource, ExternalDataSource):
    connection_string: str | None
    read_result: pl.DataFrame | None = None

    def __init__(
        self,
        connection_string: str,
        query: str = None,
        table_name: str = None,
        schema_name: str = None,
        fields: list[MinimalFieldInfo] | None = None,
    ):
        # Initialize the base class first
        BaseSqlSource.__init__(self, query=query, table_name=table_name, schema_name=schema_name, fields=fields)

        # Set connection-specific attributes
        self.connection_string = connection_string
        self.read_result = None

    def get_initial_data(self) -> list[dict[str, Any]]:
        return []

    def validate(self) -> None:
        try:
            engine = create_engine(self.connection_string)
            if self.query_mode == "table":
                try:
                    if self.schema_name is not None:
                        self._get_columns_from_table_and_schema(engine, self.table_name, self.schema_name)
                    if self.table_name is not None:
                        self._get_columns_from_table(engine, self.table_name)
                except Exception as e:
                    logger.warning(f"Error getting column info for table {self.table_name}: {e}")
                    c = self._get_columns_from_query(engine, self.get_sample_query())
                    if len(c) == 0:
                        raise ValueError("No columns found in the query")
            else:
                c = self._get_columns_from_query(engine, self.get_sample_query())
                if len(c) == 0:
                    raise ValueError("No columns found in the query")
        except Exception as e:
            logger.error(f"Error validating SQL source: {e}")
            raise e

    def get_iter(self) -> Generator[dict[str, Any], None, None]:
        logger.warning("Getting data in iteration, this is suboptimal")
        data = self.data_getter()
        for row in data:
            yield row

    def get_df(self):
        df = self.get_pl_df()
        return df.to_pandas()

    def get_sample(self, n: int = 10000) -> Generator[dict[str, Any], None, None]:
        if self.query_mode == "table":
            query = f"{self.query} LIMIT {n}"
            try:
                df = pl.read_database_uri(query, self.connection_string)
                return (r for r in df.to_dicts())
            except Exception as e:
                logger.error(f"Error with query: {query}")
                raise e
        else:
            df = self.get_pl_df()
            rows = df.head(n).to_dicts()
            return (r for r in rows)

    def data_getter(self) -> Generator[dict[str, Any], None, None]:
        df = self.get_pl_df()
        rows = df.to_dicts()
        return (r for r in rows)

    def get_pl_df(self) -> pl.DataFrame:
        if self.read_result is None:
            self.read_result = pl.read_database_uri(self.query, self.connection_string)
        return self.read_result

    def get_flow_file_columns(self) -> list[FlowfileColumn]:
        """
        Get column information from the SQL source and convert to FlowfileColumn objects

        Returns:
            List of FlowfileColumn objects representing the columns in the SQL source
        """
        engine = create_engine(self.connection_string)

        if self.query_mode == "table":
            try:
                if self.schema_name is not None:
                    return self._get_columns_from_table_and_schema(engine, self.table_name, self.schema_name)
                if self.table_name is not None:
                    return self._get_columns_from_table(engine, self.table_name)
            except Exception as e:
                logger.error(f"Error getting column info for table {self.table_name}: {e}")

        return self._get_columns_from_query(engine, self.get_sample_query())

    @staticmethod
    def _get_columns_from_table(engine: Engine, table_name: str) -> list[FlowfileColumn]:
        """
        Get FlowfileColumn objects from a database table

        Args:
            engine: SQLAlchemy engine
            table_name: Name of the table (possibly including schema)

        Returns:
            List of FlowfileColumn objects
        """
        schema_name, table = BaseSqlSource._parse_table_name(table_name)
        column_types = get_table_column_types(engine, table, schema=schema_name)
        columns = [
            FlowfileColumn.create_from_polars_dtype(column_name, get_polars_type(column_type))
            for column_name, column_type in column_types
        ]

        return columns

    @staticmethod
    def _get_columns_from_table_and_schema(engine: Engine, table_name: str, schema_name: str):
        """
        Get FlowfileColumn objects from a database table

        Args:
            engine: SQLAlchemy engine
            table_name: Name of the table (possibly including schema)
            schema_name: Name of the schema
        Returns:
            List of FlowfileColumn objects
        """
        column_types = get_table_column_types(engine, table_name, schema=schema_name)
        columns = [
            FlowfileColumn.create_from_polars_dtype(column_name, get_polars_type(column_type))
            for column_name, column_type in column_types
        ]
        return columns

    @staticmethod
    def _get_columns_from_query(engine: Engine, query: str) -> list[FlowfileColumn]:
        """
        Get FlowfileColumn objects from a SQL query

        Args:
            engine: SQLAlchemy engine
            query: SQL query string

        Returns:
            List of FlowfileColumn objects
        """
        try:
            column_names = get_query_columns(engine, query)

            columns = [
                FlowfileColumn.create_from_polars_dtype(column_name, pl.String()) for column_name in column_names
            ]
            return columns
        except Exception as e:
            logger.error(f"Error getting column info for query: {e}")
            raise e

    def parse_schema(self) -> list[FlowfileColumn]:
        return self.get_schema()

    def get_schema(self) -> list[FlowfileColumn]:
        if self.schema is None:
            self.schema = self.get_flow_file_columns()
        return self.schema


def create_sql_source_from_db_settings(database_settings: DatabaseSettings, user_id: int) -> SqlSource:
    database_connection = database_settings.database_connection
    if database_settings.connection_mode == "inline":
        if database_connection is None:
            raise ValueError("Database connection is required in inline mode")
        encrypted_secret = get_encrypted_secret(current_user_id=user_id, secret_name=database_connection.password_ref)
    else:
        database_connection = get_local_database_connection(database_settings.database_connection_name, user_id)
        encrypted_secret = database_connection.password.get_secret_value()
    if encrypted_secret is None:
        raise ValueError(f"Secret with name {database_connection.password_ref} not found for user {user_id}")

    sql_source = SqlSource(
        connection_string=construct_sql_uri(
            database_type=database_connection.database_type,
            host=database_connection.host,
            port=database_connection.port,
            database=database_connection.database,
            username=database_connection.username,
            password=decrypt_secret(encrypted_secret),
        ),
        query=None if database_settings.query_mode == "table" else database_settings.query,
        table_name=database_settings.table_name,
        schema_name=database_settings.schema_name,
    )
    return sql_source
