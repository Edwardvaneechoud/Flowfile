import polars as pl
from flowfile_worker.external_sources.sql_source.models import DatabaseReadSettings, DatabaseWriteSettings
from io import BytesIO


def write_df_to_database(df: pl.DataFrame, database_write_settings: DatabaseWriteSettings):
    """
    Writes a Polars DataFrame to a SQL database.
    Args:
        df (pl.DataFrame): The DataFrame to write.
        database_write_settings (DatabaseWriteSettings): The settings for the database connection and table.
    """
    # Write the DataFrame to the database
    df.write_database(table_name=database_write_settings.table_name,
                      connection=database_write_settings.connection.create_uri(),
                      if_table_exists=database_write_settings.if_exists)
    return True


def write_serialized_df_to_database(serialized_df: bytes, database_write_settings: DatabaseWriteSettings):
    """
    Writes a Polars DataFrame to a SQL database.
    Args:
        serialized_df (bytes): The serialized Polars DataFrame to write.
        database_write_settings (DatabaseWriteSettings): The settings for the database connection and table.
    """
    # Write the DataFrame to the database
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
    # Read the query into a DataFrame
    df = read_query_as_pd_df(database_read_settings.query, database_read_settings.connection.create_uri())
    return df

