"""FlowFrame API round-trip against DuckDB (file-based, no services needed)."""

import duckdb
import polars as pl
import pytest

from flowfile_frame.database.connection_manager import (
    create_database_connection,
    del_database_connection,
)
from flowfile_frame.database.frame_helpers import read_database, write_database

CONNECTION_NAME = "duckdb-frame-test"


@pytest.fixture
def duckdb_connection(tmp_path):
    path = str(tmp_path / "frame.duckdb")
    con = duckdb.connect(path)
    con.execute("CREATE TABLE movies (id INTEGER, title VARCHAR, rating DOUBLE)")
    con.execute("INSERT INTO movies VALUES (1, 'The Matrix', 8.7), (2, 'Inception', 8.8)")
    con.close()

    del_database_connection(CONNECTION_NAME)
    create_database_connection(CONNECTION_NAME, database_type="duckdb", database=path)
    yield path
    del_database_connection(CONNECTION_NAME)


def test_create_connection_rejects_unknown_dialect():
    with pytest.raises(ValueError, match="Unsupported database type"):
        create_database_connection("bad-dialect-conn", database_type="mariadb", host="h")


def test_read_database_duckdb(duckdb_connection):
    frame = read_database(CONNECTION_NAME, table_name="movies")
    result = frame.collect()
    assert result.height == 2
    assert result.schema["rating"] == pl.Float64


def test_write_database_duckdb_roundtrip(duckdb_connection):
    df = pl.LazyFrame({"a": [1, 2, 3], "label": ["x", "y", "z"]})
    write_database(df, CONNECTION_NAME, "written_from_frame", if_exists="replace")

    con = duckdb.connect(duckdb_connection, read_only=True)
    try:
        assert con.execute("SELECT count(*) FROM written_from_frame").fetchone()[0] == 3
    finally:
        con.close()
