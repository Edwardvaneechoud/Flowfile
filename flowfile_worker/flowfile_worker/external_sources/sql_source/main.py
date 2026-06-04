import socket
from io import BytesIO

import polars as pl

from flowfile_worker.external_sources.sql_source.models import (
    DataBaseConnection,
    DatabaseReadSettings,
    DatabaseWriteSettings,
)

# Default ports per database type for the pre-flight connectivity check.
_DEFAULT_DB_PORTS = {
    "postgresql": 5432,
    "postgres": 5432,
    "redshift": 5439,
    "mysql": 3306,
    "mariadb": 3306,
    "mssql": 1433,
    "sqlserver": 1433,
    "oracle": 1521,
}


def verify_database_reachable(connection: DataBaseConnection, timeout: float = 3.0) -> None:
    """Fail fast if the database host:port can't be reached.

    connectorx/r2d2 retries a refused or unreachable connection for ~30s before
    giving up; a short TCP pre-check surfaces the failure immediately. Best-effort:
    skips file-based (sqlite) and url-based connections where host/port is unknown.
    """
    if connection.url or connection.database_type.lower() == "sqlite":
        return
    host = connection.host
    port = connection.port or _DEFAULT_DB_PORTS.get(connection.database_type.lower())
    if not host or not port:
        return
    try:
        with socket.create_connection((host, port), timeout=timeout):
            pass
    except OSError as e:
        raise ConnectionError(f"Cannot connect to database at {host}:{port}: {e}") from e


def write_df_to_database(df: pl.DataFrame, database_write_settings: DatabaseWriteSettings):
    """
    Writes a Polars DataFrame to a SQL database.
    Args:
        df (pl.DataFrame): The DataFrame to write.
        database_write_settings (DatabaseWriteSettings): The settings for the database connection and table.
    """
    df.write_database(
        table_name=database_write_settings.table_name,
        connection=database_write_settings.connection.create_sqlalchemy_uri(),
        if_table_exists=database_write_settings.if_exists,
    )
    return True


def write_serialized_df_to_database(serialized_df: bytes, database_write_settings: DatabaseWriteSettings):
    """
    Writes a Polars DataFrame to a SQL database.
    Args:
        serialized_df (bytes): The serialized Polars DataFrame to write.
        database_write_settings (DatabaseWriteSettings): The settings for the database connection and table.
    """
    df = pl.LazyFrame.deserialize(BytesIO(serialized_df)).collect()
    write_df_to_database(df, database_write_settings)
    return True


def read_query_as_pd_df(query: str, uri: str) -> pl.DataFrame:
    """
    Reads a URI into a Polars DataFrame.
    Args:
        query (str): The SQL query to execute.
        uri (str): The URI to read.
    Returns:
        pl.DataFrame: The resulting Polars DataFrame.
    """
    return pl.read_database_uri(query, uri)


def read_sql_source(database_read_settings: DatabaseReadSettings):
    """
    Connects to a database and executes a query to retrieve data.
    Args:
        database_read_settings (SQLSourceSettings): The SQL source settings containing connection details and query.
    Returns:
        pl.DataFrame: The resulting Polars DataFrame.
    """
    verify_database_reachable(database_read_settings.connection)
    df = read_query_as_pd_df(database_read_settings.query, database_read_settings.connection.create_uri())
    return df
