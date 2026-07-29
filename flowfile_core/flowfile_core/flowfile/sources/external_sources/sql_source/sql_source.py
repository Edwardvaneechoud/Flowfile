import re
from collections.abc import Callable, Generator
from typing import Any, Literal, NamedTuple

import polars as pl
from sqlalchemy import Engine, create_engine, inspect, text

from flowfile_core.configs import logger
from flowfile_core.flowfile.database_connection_manager.db_connections import get_local_database_connection
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.flowfile.sources.external_sources.base_class import ExternalDataSource
from flowfile_core.flowfile.sources.external_sources.sql_source.utils import (
    construct_sql_uri,
    get_polars_type,
    get_sqlalchemy_uri,
)
from flowfile_core.schemas.input_schema import DatabaseSettings, MinimalFieldInfo
from flowfile_core.secret_manager.secret_manager import decrypt_secret, get_encrypted_secret
from shared.db_dialects import DbDialect, get_dialect_or_generic, read_sql
from shared.sql_validation import UnsafeSQLError, validate_sql_query

QueryMode = Literal["table", "query"]


def validate_sql_identifier(identifier: str, identifier_type: str = "identifier") -> None:
    """Validate that a SQL identifier (table name, schema name) is safe.

    Allows dotted identifiers (e.g., schema.table) by validating each part.

    Args:
        identifier: The SQL identifier to validate
        identifier_type: Description of the identifier type for error messages

    Raises:
        UnsafeSQLError: If the identifier contains unsafe characters
    """
    if not identifier or not identifier.strip():
        raise UnsafeSQLError(f"SQL {identifier_type} cannot be empty")

    # Split on dots to handle schema.table notation, validate each part
    parts = identifier.split(".")
    for part in parts:
        if not part:
            raise UnsafeSQLError(
                f"Invalid SQL {identifier_type}: '{identifier}'. " f"Identifier parts cannot be empty."
            )
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", part):
            raise UnsafeSQLError(
                f"Invalid SQL {identifier_type}: '{identifier}'. "
                f"Only letters, numbers, and underscores are allowed, "
                f"and each part must start with a letter or underscore."
            )


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
        database_type: str | None = None,
    ):
        """
        Initialize a BaseSqlSource object.

        Args:
            query: SQL query string (if query_mode is 'query')
            table_name: Name of the table to query (if query_mode is 'table')
            schema_name: Optional database schema name
            fields: Optional list of field information
            database_type: Optional dialect name; drives per-dialect SQL (limit clause)
                and the fast-schema hooks. None falls back to generic behavior.
        """
        self.database_type = database_type
        self.dialect: DbDialect = get_dialect_or_generic(database_type or "generic")
        if schema_name == "":
            schema_name = None

        if query is not None and table_name is not None:
            raise ValueError("Only one of table_name or query can be provided")
        if query is None and table_name is None:
            raise ValueError("Either table_name or query must be provided")

        if query is not None:
            # Validate user-provided queries for safety (read-only SELECT only)
            validate_sql_query(query)
            self.query_mode = "query"
            self.query = query
        elif table_name is not None:
            self.query_mode = "table"
            self.table_name = table_name
            self.schema_name = schema_name

            # Validate identifiers to prevent SQL injection
            validate_sql_identifier(table_name, "table name")
            if schema_name is not None and schema_name != "":
                validate_sql_identifier(schema_name, "schema name")

            if schema_name is not None and schema_name != "":
                self.query = f"SELECT * FROM {schema_name}.{table_name}"
            else:
                self.query = f"SELECT * FROM {table_name}"

        if fields:
            self.schema = [FlowfileColumn.from_input(column_name=col.name, data_type=col.data_type) for col in fields]

    def get_sample_query(self) -> str:
        """
        Get a sample query that returns a limited number of rows.
        """
        if self.query_mode == "query":
            return self.dialect.limit_query(f"select * from ({self.query}) as main_query", 1)
        else:
            return self.dialect.limit_query(self.query, 1)

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
        cancel_check: Callable[[], bool] | None = None,
        database_type: str | None = None,
    ):
        if database_type is None and connection_string and "://" in connection_string:
            # URIs built by construct_sql_uri carry the database_type as their scheme.
            database_type = connection_string.split("://", 1)[0].split("+", 1)[0]
        BaseSqlSource.__init__(
            self,
            query=query,
            table_name=table_name,
            schema_name=schema_name,
            fields=fields,
            database_type=database_type,
        )

        self.connection_string = connection_string
        self.read_result = None
        self.cancel_check = cancel_check

    def get_initial_data(self) -> list[dict[str, Any]]:
        return []

    def validate(self) -> None:
        try:
            dialect_schema = self._schema_from_dialect()
            if dialect_schema is not None:
                if len(dialect_schema) == 0:
                    raise ValueError("No columns found in the query")
                return
            engine = create_engine(get_sqlalchemy_uri(self.connection_string))
            if self.query_mode == "table":
                try:
                    if self.schema_name is not None:
                        self._get_columns_from_table_and_schema(engine, self.table_name, self.schema_name)
                    if self.table_name is not None:
                        self._get_columns_from_table(engine, self.table_name)
                except Exception as e:
                    logger.warning(f"Error getting column info for table {self.table_name}: {e}")
                    c = self._get_columns_from_polars(self.get_sample_query())
                    if len(c) == 0:
                        raise ValueError("No columns found in the query") from e
            else:
                c = self._get_columns_from_polars(self.get_sample_query())
                if len(c) == 0:
                    raise ValueError("No columns found in the query")
        except Exception as e:
            logger.error(f"Error validating SQL source: {e}")
            raise e

    def get_iter(self) -> Generator[dict[str, Any], None, None]:
        logger.warning("Getting data in iteration, this is suboptimal")
        data = self.data_getter()
        yield from data

    def get_sample(self, n: int = 10000) -> Generator[dict[str, Any], None, None]:
        if self.query_mode == "table":
            query = self.dialect.limit_query(self.query, n)
            try:
                df = read_sql(
                    query,
                    self.connection_string,
                    logger,
                    database_type=self.database_type,
                    cancel_check=self.cancel_check,
                )
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
            self.read_result = read_sql(
                self.query,
                self.connection_string,
                logger,
                database_type=self.database_type,
                cancel_check=self.cancel_check,
            )
        return self.read_result

    def _schema_from_dialect(self) -> list[FlowfileColumn] | None:
        """Schema via the dialect's fast hooks; None = use the SQLAlchemy inspection path."""
        schema = None
        if self.query_mode == "table":
            schema = self.dialect.table_schema(self.connection_string, self.table_name, self.schema_name)
        if schema is None:
            schema = self.dialect.query_schema(self.connection_string, self.query)
        if schema is None:
            return None
        return [FlowfileColumn.create_from_polars_dtype(name, dtype) for name, dtype in schema.items()]

    def get_flow_file_columns(self) -> list[FlowfileColumn]:
        """
        Get column information from the SQL source and convert to FlowfileColumn objects

        Returns:
            List of FlowfileColumn objects representing the columns in the SQL source
        """
        dialect_schema = self._schema_from_dialect()
        if dialect_schema is not None:
            return dialect_schema

        engine = create_engine(get_sqlalchemy_uri(self.connection_string))

        if self.query_mode == "table":
            try:
                if self.schema_name is not None:
                    return self._get_columns_from_table_and_schema(engine, self.table_name, self.schema_name)
                if self.table_name is not None:
                    return self._get_columns_from_table(engine, self.table_name)
            except Exception as e:
                logger.error(f"Error getting column info for table {self.table_name}: {e}")

        return self._get_columns_from_polars(self.get_sample_query())

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

    def _get_columns_from_polars(self, query: str) -> list[FlowfileColumn]:
        """
        Get FlowfileColumn objects from a SQL query using Polars.

        Uses pl.read_database_uri instead of SQLAlchemy's text() to avoid
        passing user-controlled data through text(), which is flagged by
        static analysis tools (CodeQL) as a SQL injection risk.

        Args:
            query: SQL query string (should include LIMIT for efficiency)

        Returns:
            List of FlowfileColumn objects
        """
        try:
            df = read_sql(
                query,
                self.connection_string,
                logger,
                database_type=self.database_type,
                cancel_check=self.cancel_check,
            )
            columns = [FlowfileColumn.create_from_polars_dtype(column_name, pl.String()) for column_name in df.columns]
            return columns
        except Exception as e:
            logger.error(f"Error getting column info for query: {e}")
            raise e

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


class ResolvedConnection(NamedTuple):
    uri: str
    database_type: str


def _resolve_connection(database_settings: DatabaseSettings, user_id: int) -> ResolvedConnection:
    """Resolve DatabaseSettings into a connection URI + dialect, handling inline/reference mode."""
    database_connection = database_settings.database_connection

    if database_settings.connection_mode == "inline":
        if database_connection is None:
            raise ValueError("Database connection is required in inline mode")
        is_file_based = get_dialect_or_generic(database_connection.database_type).file_based
        if is_file_based:
            password = None
        else:
            encrypted_secret = get_encrypted_secret(
                current_user_id=user_id, secret_name=database_connection.password_ref
            )
            if encrypted_secret is None:
                raise ValueError(f"Secret with name {database_connection.password_ref} not found for user {user_id}")
            password = decrypt_secret(encrypted_secret)
    else:
        database_connection = get_local_database_connection(database_settings.database_connection_name, user_id)
        if database_connection is None:
            raise ValueError(
                f"Database connection '{database_settings.database_connection_name}' not found "
                "or not accessible for this user"
            )
        encrypted_secret = database_connection.password.get_secret_value()
        password = decrypt_secret(encrypted_secret)

    uri = construct_sql_uri(
        database_type=database_connection.database_type,
        host=database_connection.host,
        port=database_connection.port,
        database=database_connection.database,
        username=database_connection.username,
        password=password,
        ssl_enabled=bool(getattr(database_connection, "ssl_enabled", False)),
        connect_timeout=10,
    )
    return ResolvedConnection(uri=uri, database_type=database_connection.database_type)


def _resolve_connection_string(database_settings: DatabaseSettings, user_id: int) -> str:
    """Resolve DatabaseSettings into a connection string, handling inline/reference mode and SQLite."""
    return _resolve_connection(database_settings, user_id).uri


def create_engine_from_db_settings(database_settings: DatabaseSettings, user_id: int) -> Engine:
    """Create a SQLAlchemy Engine from DatabaseSettings."""
    resolved = _resolve_connection(database_settings, user_id)
    return create_engine(get_sqlalchemy_uri(resolved.uri))


def create_sql_source_from_db_settings(database_settings: DatabaseSettings, user_id: int) -> SqlSource:
    resolved = _resolve_connection(database_settings, user_id)
    return SqlSource(
        connection_string=resolved.uri,
        query=None if database_settings.query_mode == "table" else database_settings.query,
        table_name=database_settings.table_name,
        schema_name=database_settings.schema_name,
        database_type=resolved.database_type,
    )


def list_db_schemas(database_settings: DatabaseSettings, user_id: int) -> list[str]:
    """Schema names for connection browsing; dialect fast path, else SQLAlchemy inspection."""
    resolved = _resolve_connection(database_settings, user_id)
    schemas = get_dialect_or_generic(resolved.database_type).list_schemas(resolved.uri)
    if schemas is not None:
        return sorted(schemas)
    engine = create_engine(get_sqlalchemy_uri(resolved.uri))
    try:
        return sorted(inspect(engine).get_schema_names())
    finally:
        engine.dispose()


def list_db_tables(database_settings: DatabaseSettings, user_id: int) -> list[str]:
    """Table names for browsing; plain names for an explicit schema, else schema-qualified.

    Mirrors the historical /db_tables behavior: when no schema is given, tables from every
    accessible schema are returned as ``schema.table``, skipping unreadable schemas.
    """
    resolved = _resolve_connection(database_settings, user_id)
    schema = database_settings.schema_name if database_settings.schema_name else None
    tables = get_dialect_or_generic(resolved.database_type).list_tables(resolved.uri, schema)
    if tables is not None:
        return sorted(tables)
    engine = create_engine(get_sqlalchemy_uri(resolved.uri))
    try:
        inspector = inspect(engine)
        if schema:
            return sorted(inspector.get_table_names(schema=schema))
        qualified: list[str] = []
        for s in inspector.get_schema_names():
            if s == "information_schema":
                continue
            try:
                schema_tables = inspector.get_table_names(schema=s)
            except Exception:
                # Skip schemas we don't have access to (e.g. performance_schema)
                continue
            for t in schema_tables:
                qualified.append(f"{s}.{t}")
        return sorted(qualified)
    finally:
        engine.dispose()
