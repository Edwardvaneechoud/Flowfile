"""Snowflake dialect against a live account: opt-in, skipped unless the account is configured.

Run with ``poetry run pytest -m snowflake shared/tests/db_dialects/test_snowflake_live.py`` after
setting ``FLOWFILE_TEST_SNOWFLAKE_ACCOUNT`` and ``FLOWFILE_TEST_SNOWFLAKE_USER`` plus at least one
credential: ``FLOWFILE_TEST_SNOWFLAKE_TOKEN`` (a programmatic access token) and/or
``FLOWFILE_TEST_SNOWFLAKE_PRIVATE_KEY_FILE`` (with ``FLOWFILE_TEST_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE``
when the key is encrypted). Optional: ``_ROLE``, ``_WAREHOUSE``, ``_DATABASE``, ``_SCHEMA``. The
read and write tests need a warehouse and a database; every table they create is dropped.
"""

from __future__ import annotations

import datetime as dt
import logging
import os
import uuid
from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest

from shared.db_dialects import get_dialect
from shared.snowflake_auth import AUTH_ACCESS_TOKEN, AUTH_KEY_PAIR, normalize_account, normalize_private_key

pytestmark = pytest.mark.snowflake

logger = logging.getLogger(__name__)
dialect = get_dialect("snowflake")


def _env(name: str) -> str | None:
    return os.environ.get(f"FLOWFILE_TEST_SNOWFLAKE_{name}") or None


ACCOUNT = _env("ACCOUNT")
USER = _env("USER")
ROLE = _env("ROLE")
WAREHOUSE = _env("WAREHOUSE")
DATABASE = _env("DATABASE")
SCHEMA = _env("SCHEMA") or "PUBLIC"

if not (ACCOUNT and USER) or not (_env("TOKEN") or _env("PRIVATE_KEY_FILE")):
    pytest.skip("FLOWFILE_TEST_SNOWFLAKE_* is not configured", allow_module_level=True)
if not dialect.is_available():
    pytest.skip("snowflake-connector-python is not installed", allow_module_level=True)


def _secret(method: str) -> str | None:
    if method == AUTH_ACCESS_TOKEN:
        return _env("TOKEN")
    key_file = _env("PRIVATE_KEY_FILE")
    if not key_file:
        return None
    return normalize_private_key(Path(key_file).read_text(), _env("PRIVATE_KEY_PASSPHRASE"))


def _uri(method: str, *, database: str | None = DATABASE) -> str:
    secret = _secret(method)
    if secret is None:
        pytest.skip(f"no credential configured for {method}")
    return dialect.build_uri(
        host=normalize_account(ACCOUNT),
        port=443,
        username=USER,
        password=secret,
        database=database,
        auth=method,
        role=ROLE,
        warehouse=WAREHOUSE,
    )


@pytest.fixture(params=[AUTH_ACCESS_TOKEN, AUTH_KEY_PAIR])
def uri(request) -> str:
    return _uri(request.param)


@pytest.fixture
def data_uri() -> str:
    if not (WAREHOUSE and DATABASE):
        pytest.skip("reads and writes need FLOWFILE_TEST_SNOWFLAKE_WAREHOUSE and _DATABASE")
    method = AUTH_ACCESS_TOKEN if _env("TOKEN") else AUTH_KEY_PAIR
    return _uri(method)


@pytest.fixture
def table_name(data_uri):
    name = f"FLOWFILE_TEST_{uuid.uuid4().hex[:12].upper()}"
    qualified = f"{SCHEMA}.{name}"
    yield qualified
    with dialect._cursor(data_uri) as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {qualified}")


def test_connect_and_session_info(uri):
    info = dialect.session_info(uri)
    assert set(info) == {"user", "role", "warehouse", "database"}
    assert info["user"].upper() == USER.upper()
    if ROLE:
        assert info["role"].upper() == ROLE.upper()


def test_discover(uri):
    found = dialect.discover(uri)
    assert found["user"].upper() == USER.upper()
    assert isinstance(found["warehouses"], list) and isinstance(found["databases"], list)
    assert found["roles"], "a user can always use at least PUBLIC"
    if DATABASE:
        assert DATABASE.upper() in [d.upper() for d in found["databases"]]


def test_browse(uri):
    schemas = dialect.list_schemas(uri)
    assert schemas
    if DATABASE:
        assert SCHEMA.upper() in [s.strip('"').upper() for s in schemas]
        assert isinstance(dialect.list_tables(uri, SCHEMA), list)
    else:
        assert all("." in s for s in schemas), "without a database, schemas come back as DATABASE.SCHEMA"


def test_browse_without_database():
    method = AUTH_ACCESS_TOKEN if _env("TOKEN") else AUTH_KEY_PAIR
    schemas = dialect.list_schemas(_uri(method, database=None))
    assert schemas and all("." in s for s in schemas)


def test_every_mapped_type_reads_as_predicted(data_uri, table_name):
    with dialect._cursor(data_uri) as cursor:
        cursor.execute(
            f"CREATE TABLE {table_name} (I NUMBER(38,0), D NUMBER(10,2), F FLOAT, S VARCHAR, B BOOLEAN, DT DATE, "
            "T TIME, NTZ TIMESTAMP_NTZ, LTZ TIMESTAMP_LTZ, TZ TIMESTAMP_TZ, BIN BINARY, V VARIANT, O OBJECT, "
            "A ARRAY, G GEOGRAPHY)"
        )
        cursor.execute(
            f"INSERT INTO {table_name} SELECT 1, 12.34, 1.5, 'x', TRUE, '2024-01-02', '12:34:56', "
            "'2024-01-02 03:04:05', '2024-01-02 03:04:05 +00:00', '2024-01-02 03:04:05 +02:00', "
            "TO_BINARY('ABCD', 'HEX'), PARSE_JSON('{\"k\": 1}'), OBJECT_CONSTRUCT('a', 1), ARRAY_CONSTRUCT(1, 2), "
            "TO_GEOGRAPHY('POINT(1 2)')"
        )
        cursor.execute(f"INSERT INTO {table_name} (I) VALUES (NULL)")
    query = f"SELECT * FROM {table_name}"
    frame = dialect.read(query, data_uri, logger)
    assert frame.height == 2
    assert dialect.query_schema(data_uri, query) == frame.schema
    assert dialect.table_schema(data_uri, table_name.split(".")[1], SCHEMA) == frame.schema
    assert frame.schema["D"] == pl.Decimal(10, 2)
    assert frame.schema["TZ"] == pl.Datetime("us", "UTC")
    row = frame.filter(pl.col("I") == 1).row(0, named=True)
    assert row["D"] == Decimal("12.34")
    assert row["TZ"] == dt.datetime(2024, 1, 2, 1, 4, 5, tzinfo=dt.timezone.utc)

    empty = dialect.read(f"{query} WHERE 1 = 0", data_uri, logger)
    assert empty.height == 0
    assert empty.schema == frame.schema


def test_write_modes_round_trip(data_uri, table_name):
    df = pl.DataFrame(
        {
            "id": [1, 2],
            "score": [0.5, 1.5],
            "price": pl.Series([Decimal("1.10"), Decimal("2.20")], dtype=pl.Decimal(10, 2)),
            "label": ["a", "b"],
            "flag": [True, False],
            "day": [dt.date(2024, 1, 1), dt.date(2024, 1, 2)],
            "at": [dt.time(1, 2, 3), dt.time(4, 5, 6)],
            "naive": [dt.datetime(2024, 1, 1, 1), dt.datetime(2024, 1, 2, 2)],
            "aware": pl.Series([dt.datetime(2024, 1, 1, 1), dt.datetime(2024, 1, 2, 2)]).dt.replace_time_zone("UTC"),
            "raw": [b"\x00\x01", b"\x02"],
            "items": [[1, 2], [3]],
            "props": [{"k": 1}, {"k": 2}],
            "my col": ["q", "r"],
        }
    )
    dialect.write(df, uri=data_uri, table_name=table_name, if_exists="replace")
    dialect.write(df, uri=data_uri, table_name=table_name, if_exists="append")
    with pytest.raises(ValueError, match=f"Table '{table_name}' already exists"):
        dialect.write(df, uri=data_uri, table_name=table_name, if_exists="fail")
    query = f"SELECT * FROM {table_name}"
    assert dialect.read(query, data_uri, logger).height == 4

    dialect.write(df, uri=data_uri, table_name=table_name, if_exists="replace")
    frame = dialect.read(query, data_uri, logger)
    assert frame.height == 2
    assert frame.columns[:4] == ["ID", "SCORE", "PRICE", "LABEL"]
    assert "my col" in frame.columns
    assert dialect.query_schema(data_uri, query) == frame.schema
    first = frame.sort("ID").row(0, named=True)
    assert (first["ID"], first["PRICE"], first["LABEL"], first["FLAG"]) == (1, Decimal("1.10"), "a", True)
    assert first["NAIVE"] == dt.datetime(2024, 1, 1, 1)

    limited = dialect.read(dialect.limit_query(query, 1), data_uri, logger)
    assert limited.height == 1


def test_write_empty_frame_creates_the_table(data_uri, table_name):
    df = pl.DataFrame(schema={"id": pl.Int64, "label": pl.String})
    dialect.write(df, uri=data_uri, table_name=table_name, if_exists="fail")
    frame = dialect.read(f"SELECT * FROM {table_name}", data_uri, logger)
    assert frame.height == 0
    assert frame.schema == pl.Schema({"ID": pl.Int64, "LABEL": pl.String})
