import socket
import sys
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


# SQLite writes use the stdlib sqlite3 driver: no pandas, no running server, run everywhere.

def _sqlite_settings(db_path, table_name, if_exists="replace"):
    connection = DataBaseConnection(database_type="sqlite", database=f"sqlite:///{db_path}")
    return DatabaseWriteSettings(connection=connection, table_name=table_name, if_exists=if_exists)


def test_write_df_to_sqlite_roundtrip(tmp_path):
    db = tmp_path / "out.db"
    df = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    assert write_df_to_database(df, _sqlite_settings(db, "people"))
    back = pl.read_database_uri("SELECT * FROM people", f"sqlite:///{db}")
    assert back.sort("id").equals(df)


def test_write_df_to_sqlite_mixed_dtypes(tmp_path):
    import datetime
    import decimal

    db = tmp_path / "mixed.db"
    df = pl.DataFrame({
        "i": pl.Series([1, 2], dtype=pl.Int64),
        "f": [1.5, None],
        "b": [True, False],
        "d": [datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)],
        "ts": [datetime.datetime(2024, 1, 1, 3, 4, 5), datetime.datetime(2024, 1, 2, 0, 0, 0)],
        "dec": pl.Series([decimal.Decimal("1.23"), decimal.Decimal("4.56")], dtype=pl.Decimal(10, 2)),
        "tags": [["x", "y"], []],
        "meta": [{"k": 1}, {"k": 2}],
    })
    # schema-qualified target (the user's real case was main.<table>)
    assert write_df_to_database(df, _sqlite_settings(db, "main.events"))
    back = pl.read_database_uri("SELECT * FROM main.events", f"sqlite:///{db}")
    assert len(back) == 2
    assert back["b"].to_list() == [1, 0]
    assert back["f"].to_list() == [1.5, None]
    assert back["tags"].to_list() == ['["x", "y"]', "[]"]
    assert back["meta"][0] == '{"k": 1}'
    assert back["ts"][0] == "2024-01-01 03:04:05.000000"


def test_write_df_to_sqlite_if_exists_modes(tmp_path):
    db = tmp_path / "modes.db"
    df = pl.DataFrame({"id": [1, 2]})

    def count():
        return pl.read_database_uri("SELECT count(*) c FROM t", f"sqlite:///{db}")["c"][0]

    write_df_to_database(df, _sqlite_settings(db, "t", "replace"))
    write_df_to_database(df, _sqlite_settings(db, "t", "append"))
    assert count() == 4
    write_df_to_database(df, _sqlite_settings(db, "t", "replace"))
    assert count() == 2
    with pytest.raises(ValueError, match="already exists"):
        write_df_to_database(df, _sqlite_settings(db, "t", "fail"))


def test_write_df_to_sqlite_empty_frame(tmp_path):
    db = tmp_path / "empty.db"
    df = pl.DataFrame({"id": [1]}).head(0)
    assert write_df_to_database(df, _sqlite_settings(db, "t"))
    assert pl.read_database_uri("SELECT count(*) c FROM t", f"sqlite:///{db}")["c"][0] == 0


def test_write_serialized_df_to_sqlite(tmp_path):
    db = tmp_path / "ser.db"
    df = pl.DataFrame({"id": [1, 2], "v": ["a", "b"]})
    assert write_serialized_df_to_database(df.lazy().serialize(), _sqlite_settings(db, "t"))
    back = pl.read_database_uri("SELECT * FROM t", f"sqlite:///{db}")
    assert back.sort("id").equals(df)


def test_sqlite_write_does_not_require_pandas(tmp_path, monkeypatch):
    # Blocking the pandas import proves the sqlite path is pandas-free (the bug we fixed).
    monkeypatch.setitem(sys.modules, "pandas", None)
    db = tmp_path / "nopandas.db"
    df = pl.DataFrame({"id": [1, 2], "v": ["a", "b"]})
    assert write_df_to_database(df, _sqlite_settings(db, "t"))
    with pytest.raises(ImportError):
        import pandas  # noqa: F401
    assert pl.read_database_uri("SELECT count(*) c FROM t", f"sqlite:///{db}")["c"][0] == 2


def _mysql_reachable() -> bool:
    s = socket.socket()
    s.settimeout(0.5)
    try:
        s.connect(("localhost", 3307))
        return True
    except OSError:
        return False
    finally:
        s.close()


@pytest.mark.skipif(not _mysql_reachable(), reason="MySQL test container is not running on localhost:3307")
def test_write_df_to_mysql(pw):
    df = pl.DataFrame({"id": [1, 2], "title": ["Movie1", "Movie2"]})
    connection = DataBaseConnection(host="localhost", password=pw, username="testuser", port=3307,
                                    database="testdb", database_type="mysql")
    settings = DatabaseWriteSettings(connection=connection, table_name="mysql_write_test", if_exists="replace")
    assert write_df_to_database(df, settings)
    result = read_sql_source(
        DatabaseReadSettings(connection=connection, query="SELECT * FROM mysql_write_test")
    )
    assert len(result) == 2
    assert sorted(result["title"].to_list()) == ["Movie1", "Movie2"]


