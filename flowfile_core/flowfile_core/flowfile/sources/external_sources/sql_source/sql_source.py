from typing import Any, Dict, Generator, List, Optional, Literal, Tuple
import polars as pl
from flowfile_core.configs import logger
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.schemas.input_schema import MinimalFieldInfo, DatabaseSettings
from sqlalchemy import Engine, inspect, create_engine, text
from flowfile_core.secrets.secrets import get_encrypted_secret, decrypt_secret

from flowfile_core.flowfile.sources.external_sources.base_class import ExternalDataSource
from flowfile_core.flowfile.sources.external_sources.sql_source.utils import get_polars_type, construct_sql_uri
from flowfile_core.flowfile.database_connection_manager.db_connections import get_local_database_connection

QueryMode = Literal['table', 'query']


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


def get_table_column_types(engine: Engine, table_name: str, schema: str = None) -> List[Tuple[str, Any]]:
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

    return [(column['name'], column['type']) for column in columns]


class BaseSqlSource:
    """
    A simplified base class for SQL sources that handles query generation
    without requiring database connection details.
    """
    table_name: Optional[str] = None
    query: Optional[str] = None
    schema_name: Optional[str] = None
    query_mode: QueryMode = 'table'
    schema: Optional[List[FlowfileColumn]] = None

    def __init__(self,
                 query: str = None,
                 table_name: str = None,
                 schema_name: str = None,
                 fields: Optional[List[MinimalFieldInfo]] = None):
        """
        Initialize a BaseSqlSource object.

        Args:
            query: SQL query string (if query_mode is 'query')
            table_name: Name of the table to query (if query_mode is 'table')
            schema_name: Optional database schema name
            fields: Optional list of field information
        """
        if schema_name == '':
            schema_name = None

        # Validate inputs
        if query is not None and table_name is not None:
            raise ValueError("Only one of table_name or query can be provided")
        if query is None and table_name is None:
            raise ValueError("Either table_name or query must be provided")

        # Set query mode and build query if needed
        if query is not None:
            self.query_mode = 'query'
            self.query = query
        elif table_name is not None:
            self.query_mode = 'table'
            self.table_name = table_name
            self.schema_name = schema_name

            # Generate the basic query
            if schema_name is not None and schema_name != '':
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
        if self.query_mode == 'query':
            return f"select * from ({self.query}) as main_query LIMIT 1"
        else:
            return f"{self.query} LIMIT 1"

    @staticmethod
    def _parse_table_name(table_name: str) -> tuple[Optional[str], str]:
        """
        Parse a table name that may include a schema.

        Args:
            table_name: Table name possibly in the format 'schema.table'

        Returns:
            Tuple of (schema, table_name)
        """
        table_parts = table_name.split('.')
        if len(table_parts) > 1:
            # Handle schema.table_name format
            schema = '.'.join(table_parts[:-1])
            table = table_parts[-1]
            return schema, table
        else:
            return None, table_name


class SqlSource(BaseSqlSource, ExternalDataSource):
    connection_string: Optional[str]
    read_result: Optional[pl.DataFrame] = None

    def __init__(self,
                 connection_string: str,
                 query: str = None,
                 table_name: str = None,
                 schema_name: str = None,
                 fields: Optional[List[MinimalFieldInfo]] = None):

        # Initialize the base class first
        BaseSqlSource.__init__(self, query=query, table_name=table_name, schema_name=schema_name, fields=fields)

        # Set connection-specific attributes
        self.connection_string = connection_string
        self.read_result = None

    def get_initial_data(self) -> List[Dict[str, Any]]:
        return []

    def validate(self) -> None:
        try:
            engine = create_engine(self.connection_string)
            if self.query_mode == 'table':
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

    def get_iter(self) -> Generator[Dict[str, Any], None, None]:
        logger.warning('Getting data in iteration, this is suboptimal')
        data = self.data_getter()
        for row in data:
            yield row

    def get_df(self):
        df = self.get_pl_df()
        return df.to_pandas()

    def get_sample(self, n: int = 10000) -> Generator[Dict[str, Any], None, None]:
        if self.query_mode == 'table':
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

    def data_getter(self) -> Generator[Dict[str, Any], None, None]:
        df = self.get_pl_df()
        rows = df.to_dicts()
        return (r for r in rows)

    def get_pl_df(self) -> pl.DataFrame:
        if self.read_result is None:
            self.read_result = pl.read_database_uri(self.query, self.connection_string)
        return self.read_result

    def get_flow_file_columns(self) -> List[FlowfileColumn]:
        """
        Get column information from the SQL source and convert to FlowfileColumn objects

        Returns:
            List of FlowfileColumn objects representing the columns in the SQL source
        """
        engine = create_engine(self.connection_string)

        if self.query_mode == 'table':
            try:
                if self.schema_name is not None:
                    return self._get_columns_from_table_and_schema(engine, self.table_name, self.schema_name)
                if self.table_name is not None:
                    return self._get_columns_from_table(engine, self.table_name)
            except Exception as e:
                logger.error(f"Error getting column info for table {self.table_name}: {e}")

        return self._get_columns_from_query(engine, self.get_sample_query())

    @staticmethod
    def _get_columns_from_table(engine: Engine, table_name: str) -> List[FlowfileColumn]:
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
        columns = [FlowfileColumn.create_from_polars_dtype(column_name, get_polars_type(column_type))
                   for column_name, column_type in column_types]

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
        columns = [FlowfileColumn.create_from_polars_dtype(column_name, get_polars_type(column_type))
                   for column_name, column_type in column_types]
        return columns

    @staticmethod
    def _get_columns_from_query(engine: Engine, query: str) -> List[FlowfileColumn]:
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

            columns = [FlowfileColumn.create_from_polars_dtype(column_name, pl.String()) for column_name in
                       column_names]
            return columns
        except Exception as e:
            logger.error(f"Error getting column info for query: {e}")
            raise e

    def parse_schema(self) -> List[FlowfileColumn]:
        return self.get_schema()

    def get_schema(self) -> List[FlowfileColumn]:
        if self.schema is None:
            self.schema = self.get_flow_file_columns()
        return self.schema


def create_sql_source_from_db_settings(database_settings: DatabaseSettings, user_id: int) -> SqlSource:
    database_connection = database_settings.database_connection
    if database_settings.connection_mode == 'inline':
        if database_connection is None:
            raise ValueError("Database connection is required in inline mode")
        encrypted_secret = get_encrypted_secret(current_user_id=user_id,
                                                secret_name=database_connection.password_ref)
    else:
        database_connection = get_local_database_connection(database_settings.database_connection_name, user_id)
        encrypted_secret = database_connection.password.get_secret_value()
    if encrypted_secret is None:
        raise ValueError(f"Secret with name {database_connection.password_ref} not found for user {user_id}")

    sql_source = SqlSource(connection_string=
                           construct_sql_uri(database_type=database_connection.database_type,
                                             host=database_connection.host,
                                             port=database_connection.port,
                                             database=database_connection.database,
                                             username=database_connection.username,
                                             password=decrypt_secret(encrypted_secret)),
                           query=None if database_settings.query_mode == 'table' else database_settings.query,
                           table_name=database_settings.table_name,
                           schema_name=database_settings.schema_name,
                           )
    return sql_source