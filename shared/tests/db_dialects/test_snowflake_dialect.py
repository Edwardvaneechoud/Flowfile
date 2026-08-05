"""Snowflake dialect unit tests: account-URI shapes, type map, fakesnow round trip.

The generic contract legs (registry sanity, URI shape, sqlglot-parseable limit
queries, catalog serialization) also run automatically for snowflake via
test_dialect_contract.py. The behavioral legs here run against fakesnow
(duckdb-backed connector emulation) so CI needs no Snowflake account; live
coverage (reads, writes, fast-schema parity against a real account) lives in
flowfile_core/tests/flowfile/external_sources/test_snowflake_source.py, gated
on FLOWFILE_TEST_SNOWFLAKE_* credentials.
"""

import logging
from types import SimpleNamespace

import polars as pl
import pytest
import sqlglot

from shared.db_dialects import get_dialect

dialect = get_dialect("snowflake")
logger = logging.getLogger(__name__)


def _meta(type_code, precision=None, scale=None, name="c"):
    return SimpleNamespace(name=name, type_code=type_code, precision=precision, scale=scale)


def _type_code(type_name: str) -> int:
    from snowflake.connector.constants import FIELD_ID_TO_NAME

    return next(code for code, name in FIELD_ID_TO_NAME.items() if name == type_name)


def test_metadata():
    assert dialect.display_name == "Snowflake"
    assert dialect.file_based is False
    assert dialect.default_port == 443, "catalog metadata only; the account URI carries no port"
    assert dialect.sqlalchemy_driver is None
    assert dialect.sqlglot_name == "snowflake"
    assert dialect.install_hint == "pip install snowflake-connector-python"
    assert dialect.is_available() is True, "snowflake-connector-python is a main dependency; must be available"
    assert [f.name for f in dialect.extra_fields] == ["account", "warehouse", "role"]
    assert dialect.extra_fields[0].required is True
    assert dialect.hidden_fields == ("host", "port", "ssl")
    assert dialect.auth_methods == ("password", "key_pair")


def test_build_uri_account_shape():
    uri = dialect.build_uri(
        username="u", password="p", database="d", account="my-org-acct", warehouse="WH", role="R"
    )
    assert uri == "snowflake://u:p@my-org-acct/d?warehouse=WH&role=R"


def test_build_uri_requires_account():
    with pytest.raises(ValueError, match="Account is required"):
        dialect.build_uri(username="u", password="p", database="d")


def test_build_uri_accepts_host_as_account_alias():
    assert dialect.build_uri(host="acct", username="u", password="p", database="d") == "snowflake://u:p@acct/d"


def test_build_uri_drops_blocked_extra_params():
    uri = dialect.build_uri(
        username="u",
        password="p",
        database="d",
        account="acct",
        warehouse="WH",
        authenticator="externalbrowser",
        private_key_file="/tmp/key.p8",
        user="EVIL",
        token="t",
    )
    assert uri == "snowflake://u:p@acct/d?warehouse=WH"


def test_build_uri_quotes_credentials():
    uri = dialect.build_uri(username="u@corp", password="p@ss word", account="acct", database="d")
    assert uri == "snowflake://u%40corp:p%40ss+word@acct/d"


def test_connect_kwargs_round_trip():
    uri = dialect.build_uri(
        username="u@corp", password="p@ss", database="db1", account="Acct-Id", warehouse="WH", role="R"
    )
    assert dialect._connect_kwargs(uri) == {
        "account": "Acct-Id",
        "user": "u@corp",
        "password": "p@ss",
        "database": "db1",
        "warehouse": "WH",
        "role": "R",
    }


def _throwaway_key(passphrase: bytes | None = None) -> tuple[str, bytes]:
    """Generate an RSA key; returns (PEM text as build_uri receives it, expected unencrypted PKCS#8 DER)."""
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    encryption = serialization.BestAvailableEncryption(passphrase) if passphrase else serialization.NoEncryption()
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=encryption,
    )
    der = key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return pem.decode("ascii"), der


def test_build_uri_key_pair_shape():
    pem, _ = _throwaway_key()
    uri = dialect.build_uri(
        username="u",
        password="should-not-appear",
        database="d",
        account="acct",
        warehouse="WH",
        auth_method="key_pair",
        private_key=pem,
    )
    assert "should-not-appear" not in uri, "key-pair URIs must not carry the password"
    assert uri.startswith("snowflake://u@acct/d?")
    assert "warehouse=WH" in uri
    assert "authenticator=snowflake_jwt" in uri
    assert "private_key=" in uri
    assert "private_key_passphrase" not in uri


def test_build_uri_key_pair_requires_key():
    with pytest.raises(ValueError, match="private key is required"):
        dialect.build_uri(username="u", database="d", account="acct", auth_method="key_pair")


def test_build_uri_private_key_implies_key_pair():
    pem, _ = _throwaway_key()
    uri = dialect.build_uri(username="u", password="p", database="d", account="acct", private_key=pem)
    assert "authenticator=snowflake_jwt" in uri
    assert ":p@" not in uri


def test_connect_kwargs_key_pair_round_trip():
    pem, der = _throwaway_key()
    uri = dialect.build_uri(
        username="u", database="db1", account="Acct-Id", warehouse="WH", auth_method="key_pair", private_key=pem
    )
    kwargs = dialect._connect_kwargs(uri)
    assert kwargs["authenticator"] == "SNOWFLAKE_JWT"
    assert kwargs["private_key"] == der, "PEM must round-trip to unencrypted PKCS#8 DER bytes"
    assert "password" not in kwargs
    assert kwargs["account"] == "Acct-Id"
    assert kwargs["user"] == "u"
    assert kwargs["warehouse"] == "WH"


def test_connect_kwargs_key_pair_passphrase_round_trip():
    passphrase = "s3cret pass+phrase"
    pem, der = _throwaway_key(passphrase.encode("utf-8"))
    uri = dialect.build_uri(
        username="u",
        database="db1",
        account="acct",
        auth_method="key_pair",
        private_key=pem,
        private_key_passphrase=passphrase,
    )
    kwargs = dialect._connect_kwargs(uri)
    assert kwargs["private_key"] == der, "encrypted PEM must decrypt to the same unencrypted PKCS#8 DER"
    assert kwargs["authenticator"] == "SNOWFLAKE_JWT"


def test_other_dialects_reject_key_pair():
    from shared.db_dialects import get_dialect as _get

    postgres = _get("postgresql")
    with pytest.raises(ValueError, match="does not support auth method"):
        postgres.build_uri(host="h", username="u", password="p", auth_method="key_pair")
    with pytest.raises(ValueError, match="key pair"):
        postgres.build_uri(host="h", username="u", password="p", private_key="-----BEGIN PRIVATE KEY-----")
    for file_based in ("sqlite", "duckdb"):
        with pytest.raises(ValueError, match="does not support"):
            _get(file_based).build_uri(database="/tmp/x.db", auth_method="key_pair")


def test_auth_and_key_material_keys_stay_blocked_as_extra_params():
    from shared.db_dialects import is_blocked_extra_param

    for key in ("auth_method", "authenticator", "token", "private_key", "private_key_file", "private_key_passphrase"):
        assert is_blocked_extra_param(key), key


def test_limit_query_is_plain_limit_and_parses_as_snowflake():
    limited = dialect.limit_query("SELECT a, b FROM some_table", 5)
    assert limited == "SELECT a, b FROM some_table LIMIT 5"
    parsed = sqlglot.parse(limited, read=dialect.sqlglot_name)
    assert parsed and parsed[0] is not None


def test_polars_dtype_maps_snowflake_types():
    fixed = _type_code("FIXED")
    assert dialect._polars_dtype(_meta(fixed, 38, 0)) == pl.Int64, "all Snowflake ints report NUMBER(38,0)"
    assert dialect._polars_dtype(_meta(fixed, 10, 2)) == pl.Decimal(10, 2)
    assert dialect._polars_dtype(_meta(_type_code("REAL"))) == pl.Float64
    assert dialect._polars_dtype(_meta(_type_code("TEXT"))) == pl.String
    assert dialect._polars_dtype(_meta(_type_code("DATE"))) == pl.Date
    assert dialect._polars_dtype(_meta(_type_code("TIMESTAMP_NTZ"))) == pl.Datetime("us")
    assert dialect._polars_dtype(_meta(_type_code("TIMESTAMP_TZ"))) == pl.Datetime("us", "UTC")
    assert dialect._polars_dtype(_meta(_type_code("BOOLEAN"))) == pl.Boolean
    assert dialect._polars_dtype(_meta(_type_code("VARIANT"))) == pl.String, "semi-structured reads as JSON text"
    assert dialect._polars_dtype(_meta(_type_code("ARRAY"))) == pl.String
    assert dialect._polars_dtype(_meta(9999)) is None, "unmapped types must disable prediction, not guess"


def test_snowflake_type_ddl_map():
    assert dialect._snowflake_type(pl.Int64) == "BIGINT"
    assert dialect._snowflake_type(pl.Decimal(10, 2)) == "NUMBER(10,2)"
    assert dialect._snowflake_type(pl.Float64) == "DOUBLE"
    assert dialect._snowflake_type(pl.Datetime("us")) == "TIMESTAMP_NTZ"
    assert dialect._snowflake_type(pl.Datetime("us", "UTC")) == "TIMESTAMP_TZ"
    assert dialect._snowflake_type(pl.List(pl.Int64)) == "VARCHAR", "nested columns are JSON-encoded text"


@pytest.fixture()
def snow_uri():
    fakesnow = pytest.importorskip("fakesnow")
    with fakesnow.patch():
        yield dialect.build_uri(
            username="u", password="p", database="db1", account="test", **{"schema": "public"}
        )


def test_fakesnow_write_read_roundtrip(snow_uri):
    df = pl.DataFrame({"id": [1, 2, 3], "score": [0.5, 1.5, 2.5], "label": ["a", "b", "c"]})

    dialect.write(df, uri=snow_uri, table_name="t", if_exists="replace")
    dialect.write(df, uri=snow_uri, table_name="t", if_exists="append")
    with pytest.raises(ValueError, match="Table 't' already exists"):
        dialect.write(df, uri=snow_uri, table_name="t", if_exists="fail")
    dialect.write(df, uri=snow_uri, table_name="t", if_exists="replace")

    result = dialect.read("SELECT * FROM t", snow_uri, logger)
    assert result.height == df.height
    assert result.columns == df.columns, "quoted column identifiers must round-trip exact names"


def test_fakesnow_fast_schema_parity(snow_uri):
    df = pl.DataFrame({"id": [1, 2], "score": [0.5, 1.5], "label": ["a", "b"]})
    dialect.write(df, uri=snow_uri, table_name="t", if_exists="replace")

    result = dialect.read("SELECT * FROM t", snow_uri, logger)
    predicted = dialect.query_schema(snow_uri, "SELECT * FROM t")
    if predicted is None:
        pytest.skip("fakesnow does not support cursor.describe for this query")
    assert dict(predicted) == dict(result.schema)
    predicted_table = dialect.table_schema(snow_uri, "t", None)
    assert predicted_table is not None and dict(predicted_table) == dict(result.schema)


def test_fakesnow_schema_qualified_write(snow_uri):
    df = pl.DataFrame({"x": [1, 2]})
    dialect.write(df, uri=snow_uri, table_name="reporting.facts", if_exists="replace")
    result = dialect.read("SELECT * FROM reporting.facts", snow_uri, logger)
    assert result.height == 2


def test_fakesnow_browse(snow_uri):
    df = pl.DataFrame({"x": [1]})
    dialect.write(df, uri=snow_uri, table_name="t_browse", if_exists="replace")

    schemas = dialect.list_schemas(snow_uri)
    assert schemas is not None and any(s.lower() == "public" for s in schemas)
    tables = dialect.list_tables(snow_uri, "public")
    assert tables is not None and any(t.lower() == "t_browse" for t in tables)
    qualified = dialect.list_tables(snow_uri, None)
    assert qualified is not None and any(t.lower() == "public.t_browse" for t in qualified)


@pytest.fixture()
def snow_uri_key_pair():
    fakesnow = pytest.importorskip("fakesnow")
    pem, _ = _throwaway_key()
    with fakesnow.patch():
        yield dialect.build_uri(
            username="u",
            database="db1",
            account="test",
            auth_method="key_pair",
            private_key=pem,
            **{"schema": "public"},
        )


def test_fakesnow_key_pair_uri_round_trip(snow_uri_key_pair):
    # fakesnow ignores auth kwargs, so this proves the key-pair parse path does not
    # break connections — auth correctness itself is covered by the DER unit tests
    # and the live leg in test_snowflake_source.py.
    df = pl.DataFrame({"x": [1, 2]})
    dialect.write(df, uri=snow_uri_key_pair, table_name="t_kp", if_exists="replace")
    result = dialect.read("SELECT * FROM t_kp", snow_uri_key_pair, logger)
    assert result.height == 2
