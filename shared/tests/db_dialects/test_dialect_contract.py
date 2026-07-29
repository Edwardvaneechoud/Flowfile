"""Contract tests every registered database dialect must satisfy.

Parametrized over the registry: adding a dialect makes it subject to all of
these automatically. The file-based behavioral leg doubles as the regression
pin proving the sql_utils/db_writer -> registry migration kept byte-identical
behavior for the pre-existing dialects.
"""

import logging

import polars as pl
import pytest
import sqlglot

from shared.db_dialects import (
    KNOWN_DIALECT_NAMES,
    DialectInfo,
    GenericDialect,
    UnknownDialectError,
    dialect_catalog,
    get_dialect,
    get_dialect_or_generic,
    iter_dialects,
    read_sql,
)
from shared.db_writer import write_dataframe_to_database

logger = logging.getLogger(__name__)

DIALECTS = iter_dialects()
_ids = [d.name for d in DIALECTS]


def test_registry_names_are_unique_and_lowercase():
    names = [d.name for d in DIALECTS]
    assert len(names) == len(set(names))
    assert all(n == n.lower() for n in names)
    assert KNOWN_DIALECT_NAMES == tuple(names)


@pytest.mark.parametrize("dialect", DIALECTS, ids=_ids)
def test_metadata_sanity(dialect):
    assert dialect.display_name
    if dialect.file_based:
        assert dialect.default_port is None
    else:
        assert isinstance(dialect.default_port, int)
    assert isinstance(dialect.is_available(), bool)
    if not dialect.is_available():
        assert dialect.install_hint


@pytest.mark.parametrize("dialect", DIALECTS, ids=_ids)
def test_get_dialect_roundtrip(dialect):
    assert get_dialect(dialect.name) is dialect
    assert get_dialect(dialect.name.upper()) is dialect
    assert get_dialect_or_generic(dialect.name) is dialect


def test_unknown_dialect_raises_with_supported_list():
    with pytest.raises(UnknownDialectError) as exc:
        get_dialect("not-a-database")
    for name in KNOWN_DIALECT_NAMES:
        assert name in str(exc.value)


def test_generic_fallback_preserves_legacy_uri_behavior():
    dialect = get_dialect_or_generic("redshift")
    assert isinstance(dialect, GenericDialect)
    uri = dialect.build_uri(host="h", port=5439, username="u", database="d", ssl_enabled=True)
    assert uri == "redshift://u@h:5439/d?sslmode=require"


@pytest.mark.parametrize("dialect", DIALECTS, ids=_ids)
def test_build_uri(dialect):
    if dialect.file_based:
        uri = dialect.build_uri(database="/tmp/some_file.db")
        assert uri.startswith(f"{dialect.uri_scheme}://")
        assert "/tmp/some_file.db" in uri
    else:
        with pytest.raises(ValueError):
            dialect.build_uri(host=None)
        uri = dialect.build_uri(host="h", port=dialect.default_port, username="u", password="p", database="d")
        assert uri == f"{dialect.uri_scheme}://u:p@h:{dialect.default_port}/d"


@pytest.mark.parametrize("dialect", DIALECTS, ids=_ids)
def test_sqlalchemy_uri_applies_driver_suffix(dialect):
    uri = f"{dialect.uri_scheme}://u@h/d"
    converted = dialect.sqlalchemy_uri(uri)
    if dialect.sqlalchemy_driver:
        assert converted.startswith(f"{dialect.sqlalchemy_driver}://")
    else:
        assert converted == uri


@pytest.mark.parametrize("dialect", DIALECTS, ids=_ids)
def test_limit_query_parses_in_own_sqlglot_dialect(dialect):
    limited = dialect.limit_query("SELECT a, b FROM some_table", 1)
    parsed = sqlglot.parse(limited, read=dialect.sqlglot_name)
    assert parsed and parsed[0] is not None


def test_dialect_catalog_serializes():
    catalog = dialect_catalog()
    assert [info.name for info in catalog] == list(KNOWN_DIALECT_NAMES)
    for info in catalog:
        assert isinstance(info, DialectInfo)
        info.model_dump()


FILE_BASED = [d for d in DIALECTS if d.file_based]


@pytest.mark.parametrize("dialect", FILE_BASED, ids=[d.name for d in FILE_BASED])
def test_file_based_write_read_roundtrip(dialect, tmp_path):
    """Write via the dialect's write strategy, read back via its read strategy.

    Also asserts the if_exists contract (append/replace/fail incl. the shared
    error message) and, when the dialect implements fast-schema hooks, that
    predicted dtypes equal the dtypes of the actually-read frame.
    """
    path = str(tmp_path / f"{dialect.name}_roundtrip.db")
    uri = dialect.build_uri(database=path)
    df = pl.DataFrame({"id": [1, 2, 3], "score": [0.5, 1.5, 2.5], "label": ["a", "b", "c"]})

    write_dataframe_to_database(df, database_type=dialect.name, uri=uri, table_name="t", if_exists="replace")
    write_dataframe_to_database(df, database_type=dialect.name, uri=uri, table_name="t", if_exists="append")
    with pytest.raises(ValueError, match="Table 't' already exists"):
        write_dataframe_to_database(df, database_type=dialect.name, uri=uri, table_name="t", if_exists="fail")
    write_dataframe_to_database(df, database_type=dialect.name, uri=uri, table_name="t", if_exists="replace")

    query = "SELECT * FROM t"
    result = dialect.read(query, uri, logger)
    assert result.height == df.height
    assert result.columns == df.columns

    via_entrypoint = read_sql(query, uri, logger, database_type=dialect.name)
    assert via_entrypoint.columns == df.columns

    predicted = dialect.query_schema(uri, query)
    if predicted is not None:
        assert dict(predicted) == dict(result.schema)
    predicted_table = dialect.table_schema(uri, "t", None)
    if predicted_table is not None:
        assert dict(predicted_table) == dict(result.schema)


def test_read_sql_infers_dialect_from_uri_scheme(tmp_path):
    sqlite = get_dialect("sqlite")
    path = str(tmp_path / "infer.db")
    uri = sqlite.build_uri(database=path)
    df = pl.DataFrame({"x": [1, 2]})
    write_dataframe_to_database(df, database_type="sqlite", uri=uri, table_name="t", if_exists="replace")
    result = read_sql("SELECT * FROM t", uri, logger)
    assert result.height == 2


class _ProbingSQLiteDialect(type(get_dialect("sqlite"))):
    """Test-only dialect implementing the fast-schema hooks via a LIMIT-0 read.

    None of the built-in dialects implement the hooks yet, which would leave the
    prediction-parity assertions in test_file_based_write_read_roundtrip vacuous.
    This probe proves the harness catches predicted-vs-actual schema drift before
    the first hook-implementing dialect (duckdb) lands.
    """

    def query_schema(self, uri, query):
        return self.read(f"SELECT * FROM ({query}) AS q LIMIT 0", uri, logger).schema

    def table_schema(self, uri, table_name, schema_name):
        qualified = f"{schema_name}.{table_name}" if schema_name else table_name
        return self.query_schema(uri, f"SELECT * FROM {qualified}")


def test_fast_schema_hooks_parity_harness_is_not_vacuous(tmp_path):
    dialect = _ProbingSQLiteDialect()
    path = str(tmp_path / "probe.db")
    uri = dialect.build_uri(database=path)
    df = pl.DataFrame({"id": [1, 2], "score": [0.5, 1.5], "label": ["a", "b"]})
    write_dataframe_to_database(df, database_type="sqlite", uri=uri, table_name="t", if_exists="replace")

    result = dialect.read("SELECT * FROM t", uri, logger)
    predicted = dialect.query_schema(uri, "SELECT * FROM t")
    predicted_table = dialect.table_schema(uri, "t", None)
    assert predicted is not None and dict(predicted) == dict(result.schema)
    assert predicted_table is not None and dict(predicted_table) == dict(result.schema)
