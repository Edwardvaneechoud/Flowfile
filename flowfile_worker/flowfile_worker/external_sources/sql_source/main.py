import polars as pl
from flowfile_worker.external_sources.sql_source.models import DatabaseReadSettings


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
    # Read the query into a DataFrame
    df = read_query_as_pd_df(database_read_settings.query, database_read_settings.connection.create_uri())
    return df

