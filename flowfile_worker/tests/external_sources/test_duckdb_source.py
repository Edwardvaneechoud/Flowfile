"""Worker-side DuckDB read/write path (no Docker needed)."""

from io import BytesIO

import duckdb
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


@pytest.fixture
def duckdb_path(tmp_path):
    path = str(tmp_path / "worker.duckdb")
    con = duckdb.connect(path)
    con.execute("CREATE TABLE movies (id INTEGER, title VARCHAR, rating DOUBLE)")
    con.execute("INSERT INTO movies VALUES (1, 'The Matrix', 8.7), (2, 'Inception', 8.8)")
    con.close()
    return path


def _connection(path: str) -> DataBaseConnection:
    return DataBaseConnection(database_type="duckdb", database=path)


def test_read_sql_source_duckdb(duckdb_path):
    settings = DatabaseReadSettings(connection=_connection(duckdb_path), query="SELECT * FROM movies")
    df = read_sql_source(settings)
    assert df.height == 2
    assert df.schema["rating"] == pl.Float64


def test_write_df_to_duckdb(duckdb_path):
    df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    settings = DatabaseWriteSettings(connection=_connection(duckdb_path), table_name="written", if_exists="replace")
    write_df_to_database(df, settings)

    con = duckdb.connect(duckdb_path, read_only=True)
    try:
        assert con.execute("SELECT count(*) FROM written").fetchone()[0] == 3
    finally:
        con.close()


def test_write_serialized_df_to_duckdb(duckdb_path):
    lf = pl.LazyFrame({"a": [1, 2], "b": [0.5, 1.5]})
    settings = DatabaseWriteSettings(connection=_connection(duckdb_path), table_name="serialized", if_exists="replace")
    write_serialized_df_to_database(lf.serialize(), settings)

    read_back = read_sql_source(
        DatabaseReadSettings(connection=_connection(duckdb_path), query="SELECT * FROM serialized")
    )
    assert read_back.height == 2
    assert read_back.schema["b"] == pl.Float64
