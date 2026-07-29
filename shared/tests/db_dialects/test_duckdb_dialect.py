"""DuckDB dialect unit tests: writer semantics, fast schema, browse, error paths.

The generic contract legs (URI round-trip, if_exists matrix incl. the shared
error message, prediction parity) also run automatically for duckdb via
test_dialect_contract.py's file-based parametrization.
"""

import logging

import polars as pl
import pytest

from shared.db_dialects import get_dialect
from shared.db_writer import write_dataframe_to_database

logger = logging.getLogger(__name__)

dialect = get_dialect("duckdb")


@pytest.fixture
def db_uri(tmp_path):
    return dialect.build_uri(database=str(tmp_path / "unit.duckdb"))


def _write(df: pl.DataFrame, uri: str, table_name: str, if_exists: str) -> None:
    write_dataframe_to_database(df, database_type="duckdb", uri=uri, table_name=table_name, if_exists=if_exists)


def test_read_missing_file_raises_clean_error(tmp_path):
    uri = dialect.build_uri(database=str(tmp_path / "missing.duckdb"))
    with pytest.raises(FileNotFoundError, match="DuckDB file not found"):
        dialect.read("SELECT 1", uri, logger)


def test_read_does_not_create_the_file(tmp_path):
    import os

    path = str(tmp_path / "missing.duckdb")
    with pytest.raises(FileNotFoundError):
        dialect.read("SELECT 1", dialect.build_uri(database=path), logger)
    assert not os.path.exists(path), "a read must never create the database file"


def test_write_creates_file_and_append_accumulates(db_uri):
    df = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    _write(df, db_uri, "t", "append")
    _write(df, db_uri, "t", "append")
    assert dialect.read("SELECT * FROM t", db_uri, logger).height == 4


def test_write_replace_and_fail(db_uri):
    df = pl.DataFrame({"a": [1, 2, 3]})
    _write(df, db_uri, "t", "append")
    _write(df, db_uri, "t", "replace")
    assert dialect.read("SELECT * FROM t", db_uri, logger).height == 3
    with pytest.raises(ValueError, match="Table 't' already exists"):
        _write(df, db_uri, "t", "fail")


def test_write_schema_qualified_creates_schema(db_uri):
    df = pl.DataFrame({"a": [1]})
    _write(df, db_uri, "staging.t", "replace")
    assert dialect.read("SELECT * FROM staging.t", db_uri, logger).height == 1
    assert "staging" in dialect.list_schemas(db_uri)
    assert "staging.t" in dialect.list_tables(db_uri, None)


def test_write_preserves_rich_types_roundtrip(db_uri):
    df = pl.DataFrame(
        {
            "d": pl.Series([1.5, 2.5], dtype=pl.Float64),
            "i": pl.Series([1, 2], dtype=pl.Int64),
            "u": pl.Series([1, 2], dtype=pl.UInt32),
            "s": ["a", "b"],
            "flag": [True, False],
            "nested": [[1, 2], [3]],
        }
    )
    _write(df, db_uri, "typed", "replace")
    result = dialect.read("SELECT * FROM typed", db_uri, logger)
    assert result.schema["d"] == pl.Float64
    assert result.schema["u"] == pl.UInt32
    assert result.schema["flag"] == pl.Boolean
    assert isinstance(result.schema["nested"], pl.List), "nested types must survive (unlike sqlite's text coercion)"


def test_query_schema_real_types_without_execution(db_uri):
    _write(pl.DataFrame({"a": [1], "b": ["x"], "c": [1.5]}), db_uri, "t", "replace")
    schema = dialect.query_schema(db_uri, "SELECT a, c FROM t WHERE a > 0")
    assert schema is not None
    assert dict(schema) == {"a": pl.Int64, "c": pl.Float64}


def test_table_schema_with_schema_name(db_uri):
    _write(pl.DataFrame({"a": [1]}), db_uri, "staging.t", "replace")
    schema = dialect.table_schema(db_uri, "t", "staging")
    assert schema is not None and dict(schema) == {"a": pl.Int64}
    dotted = dialect.table_schema(db_uri, "staging.t", None)
    assert dotted is not None and dict(dotted) == {"a": pl.Int64}


def test_concurrent_read_only_connections(db_uri):
    import duckdb

    _write(pl.DataFrame({"a": [1]}), db_uri, "t", "replace")
    path = db_uri[len("duckdb:///") :]
    first = duckdb.connect(path, read_only=True)
    try:
        result = dialect.read("SELECT * FROM t", db_uri, logger)
        assert result.height == 1
    finally:
        first.close()


def test_quoted_identifier_handling(db_uri):
    _write(pl.DataFrame({"a": [1]}), db_uri, "Select", "replace")
    assert dialect.read('SELECT * FROM "Select"', db_uri, logger).height == 1


def test_interval_columns_surface_as_text(db_uri):
    """INTERVAL columns must read as String in both the read and schema paths."""
    import duckdb

    path = db_uri[len("duckdb:///") :]
    con = duckdb.connect(path)
    con.execute("CREATE TABLE t AS SELECT 1 AS id, INTERVAL 3 DAY AS gap")
    con.close()

    result = dialect.read("SELECT * FROM t", db_uri, logger)
    assert result.schema["id"] == pl.Int32
    assert result.schema["gap"] == pl.String
    assert result.height == 1

    schema = dialect.query_schema(db_uri, "SELECT * FROM t")
    assert schema is not None
    assert dict(schema) == {"id": pl.Int32, "gap": pl.String}, "predicted must match materialized"

    with_semicolon = dialect.read("SELECT * FROM t;", db_uri, logger)
    assert with_semicolon.schema["gap"] == pl.String


def test_probe_tolerates_shapes_it_cannot_parse(db_uri):
    """The DESCRIBE probe must never reject queries the raw read path accepts."""
    _write(pl.DataFrame({"a": [1, 2]}), db_uri, "t", "replace")
    assert dialect.read("SELECT * FROM t;", db_uri, logger).height == 2
    assert dialect.read("SELECT * FROM t -- trailing comment", db_uri, logger).height == 2

    schema = dialect.query_schema(db_uri, "SELECT a FROM t;")
    assert schema is not None
    assert dict(schema) == {"a": pl.Int64}

    schema = dialect.query_schema(db_uri, "SELECT a FROM t -- trailing comment")
    assert schema is not None
    assert dict(schema) == {"a": pl.Int64}
