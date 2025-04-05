import polars as pl
from flowfile_worker.external_sources.sql_source.models import SQLSourceSettings


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


def read_sql_source(sql_source_settings: SQLSourceSettings):
    """
    Connects to a database and executes a query to retrieve data.
    Args:
        sql_source_settings (SQLSourceSettings): The SQL source settings containing connection details and query.
    Returns:
        pl.DataFrame: The resulting Polars DataFrame.
    """
    # Read the query into a DataFrame
    df = read_query_as_pd_df(sql_source_settings.query, sql_source_settings.connection.create_uri())
    return df

