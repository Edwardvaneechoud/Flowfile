import re
from collections.abc import Callable, Generator
from typing import Any, Literal, NamedTuple

import polars as pl
from pydantic import SecretStr
from sqlalchemy import Engine, create_engine, inspect, text

from flowfile_core.configs import logger
from flowfile_core.flowfile.database_connection_manager.db_connections import get_local_database_connection
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.flowfile.sources.external_sources.base_class import ExternalDataSource
from flowfile_core.flowfile.sources.external_sources.sql_source.utils import (
    construct_sql_uri,
    dialect_uri_options,
    get_polars_type,
    get_sqlalchemy_uri,
    refuse_inline_snowflake,
)
from flowfile_core.schemas.input_schema import DatabaseSettings, MinimalFieldInfo
from flowfile_core.secret_manager.secret_manager import decrypt_secret, get_encrypted_secret
from shared.db_dialects import DbDialect, get_dialect_or_generic, read_sql
from shared.sql_validation import UnsafeSQLError, validate_sql_query

QueryMode = Literal["table", "query"]

MISSING_SCHEMA_MESSAGE = "Choose a schema when a database is set."


def qualified_schema(database_name: str | None, schema_name: str | None) -> str | None:
    """The schema path a table lives under: ``database.schema`` when a database is set, else the schema as given.

    Concatenated verbatim, like ``schema.table`` always was; a database without a schema is refused.
    """
    if database_name:
        if not schema_name:
            raise ValueError(MISSING_SCHEMA_MESSAGE)
        return f"{database_name}.{schema_name}"
    return schema_name or None


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
    database_name: str | None = None
    schema_path: str | None = None
    query_mode: QueryMode = "table"
    schema: list[FlowfileColumn] | None = None

    def __init__(
        self,
        query: str = None,
        table_name: str = None,
        schema_name: str = None,
        fields: list[MinimalFieldInfo] | None = None,
        database_type: str | None = None,
        database_name: str | None = None,
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
            database_name: Optional database the table is in; table mode then reads
                ``database.schema.table`` and needs a schema. Query mode ignores it.
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
            self.database_name = database_name or None

            # Validate identifiers to prevent SQL injection
            validate_sql_identifier(table_name, "table name")
            if schema_name is not None and schema_name != "":
                validate_sql_identifier(schema_name, "schema name")
            if self.database_name:
                validate_sql_identifier(self.database_name, "database name")
            self.schema_path = qualified_schema(self.database_name, schema_name)

            if self.schema_path:
                self.query = f"SELECT * FROM {self.schema_path}.{table_name}"
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
        database_name: str | None = None,
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
            database_name=database_name,
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
                    if self.schema_path is not None:
                        self._get_columns_from_table_and_schema(engine, self.table_name, self.schema_path)
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
            schema = self.dialect.table_schema(self.connection_string, self.table_name, self.schema_path)
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
                if self.schema_path is not None:
                    return self._get_columns_from_table_and_schema(engine, self.table_name, self.schema_path)
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


def connection_uri(database_connection, password, database: str | None = None) -> str:
    """The URI of a connection given its decrypted password (a ``SecretStr``, a string or None).

    ``database``, when given, takes the place of the connection's own database (a node that picked
    another database on a dialect that browses databases); the caller has validated it.
    """
    if isinstance(password, str):
        password = SecretStr(password)
    return construct_sql_uri(
        database_type=database_connection.database_type,
        host=database_connection.host,
        port=database_connection.port,
        database=database or database_connection.database,
        username=database_connection.username,
        password=password,
        ssl_enabled=bool(getattr(database_connection, "ssl_enabled", False)),
        connect_timeout=10,
        **dialect_uri_options(database_connection),
    )


def _resolve_connection(
    database_settings: DatabaseSettings, user_id: int, database: str | None = None
) -> ResolvedConnection:
    """Resolve DatabaseSettings into a connection URI + dialect, handling inline/reference mode.

    ``database`` replaces the connection's database in the URI; see ``connection_uri``.
    """
    database_connection = database_settings.database_connection

    if database_settings.connection_mode == "inline":
        if database_connection is None:
            raise ValueError("Database connection is required in inline mode")
        refuse_inline_snowflake(database_connection)
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
        if database_connection.granted:
            from flowfile_core.flowfile.execution import run_secrets

            raise ValueError(run_secrets.refusal(database_connection.connection_name))
        encrypted_secret = database_connection.password.get_secret_value()
        password = decrypt_secret(encrypted_secret)

    uri = connection_uri(database_connection, password, database=database)
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
        database_name=database_settings.database_name,
    )


def _resolve_for_browsing(database_settings: DatabaseSettings, user_id: int) -> tuple[ResolvedConnection, DbDialect]:
    """The connection to browse, opened on the node's ``database_name`` for a dialect that browses databases.

    The name goes into the URI path, so the dialect checks it as one identifier first. A dialect with one
    database per connection keeps its own URI; the name then only qualifies schema paths.
    """
    resolved = _resolve_connection(database_settings, user_id)
    dialect = get_dialect_or_generic(resolved.database_type)
    database = database_settings.database_name
    if database and dialect.browses_databases:
        resolved = _resolve_connection(database_settings, user_id, database=dialect.checked_database(database))
    return resolved, dialect


def list_db_databases(database_settings: DatabaseSettings, user_id: int) -> list[str]:
    """Database names a node may pick; empty for a dialect with one database per connection."""
    resolved = _resolve_connection(database_settings, user_id)
    databases = get_dialect_or_generic(resolved.database_type).list_databases(resolved.uri)
    return list(databases) if databases is not None else []


def list_db_schemas(database_settings: DatabaseSettings, user_id: int) -> list[str]:
    """Schema names for connection browsing; dialect fast path, else SQLAlchemy inspection.

    With ``database_name`` set on a dialect that browses databases, the schemas are that database's.
    """
    resolved, dialect = _resolve_for_browsing(database_settings, user_id)
    schemas = dialect.list_schemas(resolved.uri)
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
    accessible schema are returned as ``schema.table``, skipping unreadable schemas. With
    ``database_name`` set, an explicit schema is browsed as ``database.schema``.
    """
    resolved, dialect = _resolve_for_browsing(database_settings, user_id)
    schema = database_settings.schema_name if database_settings.schema_name else None
    if schema and database_settings.database_name:
        schema = qualified_schema(database_settings.database_name, schema)
    tables = dialect.list_tables(resolved.uri, schema)
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
