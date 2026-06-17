import socket
import time

import polars as pl
import pytest

from flowfile_worker.external_sources.sql_source.main import (
    read_sql_source,
    verify_database_reachable,
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
    expected_uri = 'postgresql://testuser:testpass@localhost:5433/testdb?connect_timeout=10'
    assert result_uri == expected_uri, f"Expected URI: {expected_uri}, but got: {result_uri}"
    database_connection = DataBaseConnection(host='localhost', password=pw, username='testuser', port=5433)
    result_uri = database_connection.create_uri()
    expected_uri = 'postgresql://testuser:testpass@localhost:5433?connect_timeout=10'
    assert result_uri == expected_uri, f"Expected URI: {expected_uri}, but got: {result_uri}"


def test_database_connection_uri_with_ssl(pw):
    database_connection = DataBaseConnection(host='localhost', password=pw, username='testuser', port=5433,
                                             database='testdb', ssl_enabled=True)
    result_uri = database_connection.create_uri()
    expected_uri = 'postgresql://testuser:testpass@localhost:5433/testdb?sslmode=require&connect_timeout=10'
    assert result_uri == expected_uri, f"Expected URI: {expected_uri}, but got: {result_uri}"


def test_database_connection_uri_mysql_no_postgres_params(pw):
    database_connection = DataBaseConnection(host='localhost', password=pw, username='testuser', port=3306,
                                             database='testdb', database_type='mysql', ssl_enabled=True)
    result_uri = database_connection.create_uri()
    expected_uri = 'mysql://testuser:testpass@localhost:3306/testdb'
    assert result_uri == expected_uri, f"Expected URI: {expected_uri}, but got: {result_uri}"


def _free_port() -> int:
    """Return a TCP port that is (almost certainly) not listening."""
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def test_verify_database_reachable_raises_fast_on_closed_port():
    connection = DataBaseConnection(host="127.0.0.1", port=_free_port(), database_type="postgresql",
                                    username="x", database="d")
    start = time.perf_counter()
    with pytest.raises(ConnectionError):
        verify_database_reachable(connection, timeout=3.0)
    # connectorx would retry for ~30s; the pre-check must surface it immediately.
    assert time.perf_counter() - start < 5.0


def test_verify_database_reachable_skips_sqlite_and_url():
    # File-based and url-based connections have no usable host/port: must not raise.
    verify_database_reachable(DataBaseConnection(database_type="sqlite", database="sqlite:///x.db"))
    verify_database_reachable(DataBaseConnection(database_type="postgresql",
                                                 url="postgresql://u:p@unreachable-host:5432/d"))


def test_read_sql_source_fails_fast_on_unreachable_db():
    connection = DataBaseConnection(host="127.0.0.1", port=_free_port(), database_type="postgresql",
                                    username="testuser", database="testdb")
    settings = DatabaseReadSettings(connection=connection, query="SELECT 1")
    start = time.perf_counter()
    with pytest.raises(ConnectionError):
        read_sql_source(settings)
    assert time.perf_counter() - start < 5.0


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
def test_read_sql_source_sqlalchemy_fallback(pw, monkeypatch):
    """If connectorx is unusable (e.g. a transaction-mode pooler), the SQLAlchemy fallback still reads."""
    from shared import db_reader

    def broken_connectorx(query, uri):
        raise RuntimeError("simulated connectorx pooler incompatibility")

    monkeypatch.setattr(db_reader, "_read_connectorx", broken_connectorx)
    database_connection = DataBaseConnection(host='localhost', password=pw, username='testuser', port=5433,
                                             database='testdb')
    database_read_settings = DatabaseReadSettings(connection=database_connection, query='SELECT * FROM public.movies')
    df = read_sql_source(database_read_settings)
    assert isinstance(df, pl.DataFrame)
    assert len(df) > 0, "SQLAlchemy fallback should return rows"


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


