import polars as pl

from flowfile_worker.external_sources.sql_source.models import DatabaseReadSettings, DataBaseConnection
from flowfile_worker.external_sources.sql_source.main import read_sql_source, read_query_as_pd_df


def test_database_connection_uri_parsing():
    database_connection = DataBaseConnection(host='localhost', password='testpass', username='testuser', port=5433,
                                             database='testdb')
    result_uri = database_connection.create_uri()
    expected_uri = 'postgresql://testuser:testpass@localhost:5433/testdb'
    assert result_uri == expected_uri, f"Expected URI: {expected_uri}, but got: {result_uri}"
    database_connection = DataBaseConnection(host='localhost', password='testpass', username='testuser', port=5433)
    result_uri = database_connection.create_uri()
    expected_uri = 'postgresql://testuser:testpass@localhost:5433'
    assert result_uri == expected_uri, f"Expected URI: {expected_uri}, but got: {result_uri}"


def test_read_sql_source():
    database_connection = DataBaseConnection(host='localhost', password='testpass', username='testuser', port=5433,
                                             database='testdb')
    database_read_settings = DatabaseReadSettings(connection=database_connection, query='SELECT * FROM public.movies')
    df = read_sql_source(database_read_settings)
    assert df is not None, "DataFrame should not be None"
    assert isinstance(df, pl.DataFrame), "Expected a Polars DataFrame"
    assert len(df) > 0, "DataFrame should not be empty"



