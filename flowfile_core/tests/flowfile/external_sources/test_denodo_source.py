"""Denodo dialect behavior against a real Postgres-wire server.

No Denodo server is available in CI, so the psycopg2 read path, the LIMIT-0
fast schema and the SqlSource integration are exercised against the Postgres
Docker fixture through a ``denodo://`` URI: Denodo's port 9996 speaks the same
wire protocol, and none of these paths touch pg_catalog or connectorx.
"""

import logging

import polars as pl
import pytest

from flowfile_core.flowfile.sources.external_sources.sql_source.sql_source import SqlSource
from shared.db_dialects import get_dialect, read_sql
from shared.db_reader import DatabaseReadCancelledError

try:
    from test_utils.postgres.fixtures import can_connect_to_db, is_docker_available
except ImportError:
    import os
    import sys

    sys.path.append(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
    from test_utils.postgres.fixtures import can_connect_to_db, is_docker_available

DENODO_URI = "denodo://testuser:testpass@localhost:5433/testdb"
PROBE_TABLE = "ff_denodo_probe"
TYPED_QUERY = (
    "SELECT 1::int4 AS i, 2::bigint AS b, 1.5::numeric(10,2) AS n, 2.25::float8 AS f, 'x'::text AS t, "
    "true AS ok, now() AS tsz, now()::timestamp AS ts, current_date AS d"
)

pytestmark = pytest.mark.skipif(
    not is_docker_available() or not can_connect_to_db(),
    reason="PostgreSQL Docker container is not available or not running",
)

logger = logging.getLogger(__name__)
dialect = get_dialect("denodo")


@pytest.fixture(scope="module")
def probe_table():
    import psycopg2

    conn = psycopg2.connect(dialect.libpq_dsn(DENODO_URI))
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS public.{PROBE_TABLE}")
        cur.execute(f"CREATE TABLE public.{PROBE_TABLE} (id int4, label text, amount numeric(8,2), seen timestamp)")
        cur.execute(
            f"INSERT INTO public.{PROBE_TABLE} VALUES (1, 'a', 1.25, now()), (2, 'b', 2.50, now()), (3, NULL, NULL, NULL)"
        )
    try:
        yield PROBE_TABLE
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS public.{PROBE_TABLE}")
        conn.close()


def test_read_applies_oid_map_and_fast_schema_matches():
    df = dialect.read(TYPED_QUERY, DENODO_URI, logger)
    assert df.height == 1
    assert df.schema["i"] == pl.Int32
    assert df.schema["n"] == pl.Decimal(10, 2)
    assert df.schema["tsz"] == pl.Datetime("us", "UTC")
    assert df.schema["ts"] == pl.Datetime("us")
    predicted = dialect.query_schema(DENODO_URI, TYPED_QUERY)
    assert predicted is not None and dict(predicted) == dict(df.schema)


def test_read_sql_entrypoint_resolves_denodo_from_scheme():
    df = read_sql(dialect.limit_query("SELECT id, title FROM public.movies", 5), DENODO_URI, logger)
    assert df.shape == (5, 2)
    assert df.schema["id"] == pl.Int32


def test_unmapped_types_fall_back_to_inference_without_prediction():
    query = "SELECT id, genres FROM public.movies LIMIT 2"
    assert dialect.query_schema(DENODO_URI, query) is None, "jsonb must disable prediction, not guess"
    df = dialect.read(query, DENODO_URI, logger)
    assert df.height == 2 and df.schema["id"] == pl.Int32


def test_empty_result_keeps_typed_columns():
    df = dialect.read("SELECT 1::int4 AS i, 'a'::text AS t WHERE false", DENODO_URI, logger)
    assert df.height == 0 and dict(df.schema) == {"i": pl.Int32, "t": pl.String}


def test_bad_query_probe_returns_none_instead_of_raising():
    assert dialect.query_schema(DENODO_URI, "SELECT * FROM no_such_table_xyz") is None


def test_cancel_check_abandons_the_read():
    with pytest.raises(DatabaseReadCancelledError):
        dialect.read("SELECT pg_sleep(5)", DENODO_URI, logger, cancel_check=lambda: True)


def test_table_schema_and_sql_source_table_mode(probe_table):
    df = dialect.read(f"SELECT * FROM public.{probe_table}", DENODO_URI, logger)
    predicted = dialect.table_schema(DENODO_URI, probe_table, "public")
    assert predicted is not None and dict(predicted) == dict(df.schema)

    source = SqlSource(
        connection_string=DENODO_URI, table_name=probe_table, schema_name="public", database_type="denodo"
    )
    source.validate()
    columns = {c.name: c.data_type for c in source.get_flow_file_columns()}
    assert columns["id"] != "String", f"fast schema must beat the all-String fallback: {columns}"
    assert len(list(source.get_sample(2))) == 2
    assert source.get_pl_df().height == 3


def test_sql_source_query_mode_predicts_real_types():
    source = SqlSource(
        connection_string=DENODO_URI,
        query="SELECT id, title, budget, vote_average FROM public.movies",
        database_type="denodo",
    )
    columns = {c.name: c.data_type for c in source.get_flow_file_columns()}
    assert columns["id"] != "String" and columns["budget"] != "String" and columns["vote_average"] != "String"
    assert len(list(source.get_sample(3))) == 3
