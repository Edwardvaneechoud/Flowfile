"""SQL Server dialect unit tests: TOP-limit shapes, URI/driver wiring, type map.

The generic contract legs (registry sanity, URI round-trip, sqlglot-parseable
limit queries, catalog serialization) also run automatically for mssql via
test_dialect_contract.py. Live server coverage (reads, writes, fast-schema
parity) lives in flowfile_core/tests/flowfile/external_sources/test_mssql_source.py
against the test_utils/mssql Docker fixture.
"""

import polars as pl
import sqlglot

from shared.db_dialects import get_dialect
from shared.sql_utils import get_sqlalchemy_uri

dialect = get_dialect("mssql")


def test_metadata():
    assert dialect.display_name == "SQL Server"
    assert dialect.file_based is False
    assert dialect.default_port == 1433
    assert dialect.sqlalchemy_driver == "mssql+pymssql"
    assert dialect.sqlglot_name == "tsql"
    assert dialect.install_hint == "pip install pymssql"
    assert dialect.is_available() is True, "pymssql is a main dependency; the dialect must be available"


def test_limit_query_injects_top_after_select():
    assert dialect.limit_query("SELECT a, b FROM some_table", 1) == "SELECT TOP 1 a, b FROM some_table"


def test_limit_query_lowercase_wrapped_user_query():
    """Query-mode sampling wraps the user query in a lowercase derived table (sql_source.py)."""
    limited = dialect.limit_query("select * from (select x from t) as main_query", 1)
    assert limited == "select TOP 1 * from (select x from t) as main_query"


def test_limit_query_preserves_distinct():
    assert dialect.limit_query("SELECT DISTINCT a FROM t", 5) == "SELECT DISTINCT TOP 5 a FROM t"


def test_limit_query_tolerates_leading_whitespace():
    assert dialect.limit_query("\n  select a from t", 3) == "\n  select TOP 3 a from t"


def test_limit_query_non_select_head_wraps_instead_of_dropping_the_limit():
    cte = "WITH x AS (SELECT 1 AS a) SELECT a FROM x"
    assert dialect.limit_query(cte, 2) == f"SELECT TOP 2 * FROM ({cte}) AS _ff_limit"


def test_limit_query_shapes_parse_as_tsql():
    for shape in (
        "SELECT a, b FROM some_table",
        "select * from (select x from t) as main_query",
        "SELECT DISTINCT a FROM t",
    ):
        parsed = sqlglot.parse(dialect.limit_query(shape, 7), read=dialect.sqlglot_name)
        assert parsed and parsed[0] is not None


def test_build_uri_shape():
    uri = dialect.build_uri(host="h", port=1433, username="u", password="p", database="d")
    assert uri == "mssql://u:p@h:1433/d"


def test_sqlalchemy_uri_uses_pymssql_driver():
    assert dialect.sqlalchemy_uri("mssql://u@h/d") == "mssql+pymssql://u@h/d"
    assert get_sqlalchemy_uri("mssql://u:p@h:1433/d") == "mssql+pymssql://u:p@h:1433/d"


def test_polars_dtype_maps_tsql_type_names():
    assert dialect._polars_dtype("int") == pl.Int32
    assert dialect._polars_dtype("smallint") == pl.Int16
    assert dialect._polars_dtype("decimal(3,1)") == pl.Decimal(3, 1)
    assert dialect._polars_dtype("nvarchar(255)") == pl.String
    assert dialect._polars_dtype("datetime2(7)") == pl.Datetime("us")
    assert dialect._polars_dtype("bit") == pl.Boolean
    assert dialect._polars_dtype("uniqueidentifier") == pl.String, "projected to NVARCHAR in the read path"
    assert dialect._polars_dtype("timestamp") == pl.Binary, "T-SQL timestamp is rowversion, not a datetime"
    assert dialect._polars_dtype("geography") is None, "unmapped types must disable prediction, not guess"


def test_object_safe_query_projects_only_when_needed():
    plain = [("id", "int"), ("title", "nvarchar(255)")]
    assert dialect._object_safe_query("SELECT * FROM t", plain) == "SELECT * FROM t"

    with_guid = [("id", "int"), ("row_guid", "uniqueidentifier")]
    projected = dialect._object_safe_query("SELECT * FROM t", with_guid)
    assert projected == "SELECT [id], CAST([row_guid] AS NVARCHAR(MAX)) AS [row_guid] FROM (SELECT * FROM t\n) AS _ff_q"
