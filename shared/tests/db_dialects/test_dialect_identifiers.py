"""Which database, schema and table names each dialect lets into its SQL, and the write backstop that enforces it.

Only a dialect that declares ``quoted_identifiers`` (Snowflake) takes ``$`` or double-quoted parts; every other
dialect keeps the plain rule. ``write_dataframe_to_database`` checks the target before any writer opens.
"""

from __future__ import annotations

import polars as pl
import pytest

from shared.db_dialects import dialect_catalog, get_dialect, get_dialect_or_generic
from shared.db_dialects.identifiers import identifier_parts, is_identifier_path, is_plain_path
from shared.db_writer import write_dataframe_to_database

PLAIN_DIALECTS = ("postgresql", "mysql", "sqlite", "duckdb", "mssql", "denodo")

SNOWFLAKE_ACCEPTS = [
    '"USER$evaneechoud"',
    '"my.db"."My Schema".T',
    "A$B",
    "ORDERS",
    "PUBLIC.ORDERS",
    '"quo""te"',
    '"a;b"',
]
SNOWFLAKE_REFUSES = ['a"b', '"unterminated', "a;b", "a b", "a--b", '""', "", "a..b", ".a", "a.", "1ST", '"a"b"']


@pytest.mark.parametrize("name", [*PLAIN_DIALECTS, "redshift"])
def test_plain_dialects_keep_the_historical_rule(name):
    dialect = get_dialect_or_generic(name)
    assert dialect.quoted_identifiers is False
    for ok in ("users", "public.users", "analytics.public.credits", "_x", "Table1"):
        assert dialect.checked_identifier_path(ok) == ok
    for bad in ['"USER$evaneechoud"', "A$B", '"t"', "a b", "a;b", "a--b", "1st", "a.", "", 'x"y']:
        with pytest.raises(ValueError, match="Only letters, numbers, and underscores are allowed"):
            dialect.checked_identifier_path(bad)


@pytest.mark.parametrize("value", SNOWFLAKE_ACCEPTS)
def test_snowflake_accepts_plain_dollar_and_quoted_parts(value):
    assert get_dialect("snowflake").checked_identifier_path(value, "table name") == value


@pytest.mark.parametrize("value", SNOWFLAKE_REFUSES)
def test_snowflake_refuses_anything_that_could_end_a_quote_or_a_statement(value):
    with pytest.raises(ValueError, match="put the name in double quotes"):
        get_dialect("snowflake").checked_identifier_path(value)


def test_the_shapes_and_the_quote_aware_split():
    assert is_plain_path("a.b") and not is_plain_path("a$b") and not is_plain_path('"a"')
    assert is_identifier_path('"my.db"."My Schema".T')
    assert identifier_parts('"my.db"."My Schema".T') == ['"my.db"', '"My Schema"', "T"]
    assert identifier_parts('"a""b".C$1') == ['"a""b"', "C$1"]
    with pytest.raises(ValueError):
        identifier_parts('"open.T')


def test_catalog_says_which_dialects_take_quoted_names():
    by_name = {info.name: info for info in dialect_catalog()}
    assert by_name["snowflake"].quoted_identifiers is True
    assert all(not info.quoted_identifiers for name, info in by_name.items() if name != "snowflake")


@pytest.mark.parametrize("database_type", ["sqlite", "duckdb"])
@pytest.mark.parametrize("table_name", ['"T"', "A$B", '"my.db"."S".T', "t; DROP TABLE x"])
def test_the_write_backstop_refuses_a_quoted_target_before_any_io(tmp_path, database_type, table_name):
    target = tmp_path / f"out.{database_type}"
    uri = f"{database_type}:///{target}"
    with pytest.raises(ValueError, match="Only letters, numbers, and underscores are allowed"):
        write_dataframe_to_database(pl.DataFrame({"a": [1]}), database_type=database_type, uri=uri, table_name=table_name)
    assert not target.exists()


def test_the_write_backstop_leaves_plain_writes_alone(tmp_path):
    uri = f"sqlite:///{tmp_path / 'ok.db'}"
    df = pl.DataFrame({"id": [1, 2]})
    write_dataframe_to_database(df, database_type="sqlite", uri=uri, table_name="main.people", if_exists="replace")
    assert pl.read_database_uri("SELECT * FROM main.people", uri)["id"].to_list() == [1, 2]
