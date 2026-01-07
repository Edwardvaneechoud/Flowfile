import polars as pl
import pytest

from flowfile_worker.external_sources.sql_source.main import (
    read_sql_source,
    write_df_to_database,
    write_serialized_df_to_database,
)
from flowfile_worker.external_sources.sql_source.models import (
    DataBaseConnection,
    DatabaseReadSettings,
    DatabaseWriteSettings,
)
from flowfile_worker.secrets import encrypt_secret
from tests.utils import is_docker_available


@pytest.fixture
def pw():
    return encrypt_secret('testpass')


def test_database_connection_uri_parsing(pw):
    database_connection = DataBaseConnection(host='localhost', password=pw, username='testuser', port=5433,
                                             database='testdb')
    result_uri = database_connection.create_uri()
    expected_uri = 'postgresql://testuser:testpass@localhost:5433/testdb'
    assert result_uri == expected_uri, f"Expected URI: {expected_uri}, but got: {result_uri}"
    database_connection = DataBaseConnection(host='localhost', password=pw, username='testuser', port=5433)
    result_uri = database_connection.create_uri()
    expected_uri = 'postgresql://testuser:testpass@localhost:5433'
    assert result_uri == expected_uri, f"Expected URI: {expected_uri}, but got: {result_uri}"


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running so database connection cannot be established")
def test_read_sql_source(pw):
    database_connection = DataBaseConnection(host='localhost', password=pw, username='testuser', port=5433,
                                             database='testdb')
    database_read_settings = DatabaseReadSettings(connection=database_connection, query='SELECT * FROM public.movies')
    df = read_sql_source(database_read_settings)
    assert df is not None, "DataFrame should not be None"
    assert isinstance(df, pl.DataFrame), "Expected a Polars DataFrame"
    assert len(df) > 0, "DataFrame should not be empty"


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running so database connection cannot be established")
def test_write_df_to_sql(pw):
    df = pl.DataFrame({'id': [1, 2], 'title': ['Movie1', 'Movie2']})
    database_connection = DataBaseConnection(host='localhost', password=pw, username='testuser', port=5433,
                                             database='testdb')
    database_write_settings = DatabaseWriteSettings(connection=database_connection,
                                                   table_name='public.test_output', if_exists='replace')
    r = write_df_to_database(df, database_write_settings)
    assert r, "DataFrame should be written to the database"
    database_read_settings = DatabaseReadSettings(connection=database_connection, query='SELECT * FROM public.test_output')
    result_df = read_sql_source(database_read_settings)
    assert df.equals(result_df), "DataFrame written to the database should match the original DataFrame"


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running so database connection cannot be established")
def test_write_serialized_df_to_database(pw):
    df = pl.DataFrame({'id': [1, 2], 'title': ['Movie1', 'Movie2']})
    serialized_df = df.lazy().serialize()
    database_connection = DataBaseConnection(host='localhost', password=pw, username='testuser', port=5433,
                                             database='testdb')
    database_write_settings = DatabaseWriteSettings(connection=database_connection,
                                                   table_name='public.test_output', if_exists='replace')
    r = write_serialized_df_to_database(serialized_df, database_write_settings)
    assert r, "DataFrame should be written to the database"
    database_read_settings = DatabaseReadSettings(connection=database_connection, query='SELECT * FROM public.test_output')
    result_df = read_sql_source(database_read_settings)
    assert df.equals(result_df), "DataFrame written to the database should match the original DataFrame"
