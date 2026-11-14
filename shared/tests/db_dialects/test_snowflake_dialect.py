"""Snowflake dialect and auth-helper tests that need neither a Snowflake account nor the connector.

The generic contract legs run for snowflake via test_dialect_contract.py. The OAuth
token exchange runs against a real local HTTP server standing in for Snowflake's
token endpoint. Everything that needs a live account is in test_snowflake_live.py.
"""

from __future__ import annotations

import base64
import datetime as dt
import hashlib
import json
import threading
from contextlib import contextmanager
from decimal import Decimal
from typing import NamedTuple
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

import polars as pl
import pyarrow as pa
import pytest
import sqlglot
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec, rsa

from shared import snowflake_auth
from shared.db_dialects import dialect_catalog, get_dialect
from shared.db_dialects.snowflake import (
    SnowflakeDialect,
    SnowflakeError,
    _cast_to_predicted,
    _parquet_ready,
    _parse_uri,
    _put_url,
    _translated,
    checked_identifier,
    ddl_identifier,
    polars_dtype,
    quote_identifier,
    snowflake_type,
    translate_error,
)
from shared.snowflake_auth import (
    AUTH_ACCESS_TOKEN,
    AUTH_KEY_PAIR,
    AUTH_METHODS,
    AUTH_OAUTH,
    SnowflakeAuthError,
    account_from_host,
    authorize_url,
    new_pkce_pair,
    normalize_account,
    normalize_private_key,
    private_key_der,
)
from shared.sql_utils import construct_sql_uri

dialect = get_dialect("snowflake")


def _connector_installed() -> bool:
    try:
        import snowflake.connector  # noqa: F401
    except ImportError:
        return False
    return True


def test_metadata():
    assert isinstance(dialect, SnowflakeDialect)
    assert dialect.display_name == "Snowflake"
    assert dialect.file_based is False
    assert dialect.default_port == 443
    assert dialect.supports_ssl is False
    assert dialect.sqlalchemy_driver is None
    assert dialect.sqlglot_name == "snowflake"
    assert dialect.install_hint == 'pip install "flowfile[snowflake]"'
    assert dialect.auth_methods == ("oauth", "access_token", "key_pair") == AUTH_METHODS
    assert dialect.saved_connection_only is True
    assert dialect.is_available() is _connector_installed()


def test_catalog_entry_carries_auth_methods():
    entry = next(info for info in dialect_catalog() if info.name == "snowflake")
    assert entry.auth_methods == ["oauth", "access_token", "key_pair"]
    assert entry.saved_connection_only is True
    assert entry.install_hint == 'pip install "flowfile[snowflake]"'
    others = [info for info in dialect_catalog() if info.name != "snowflake"]
    assert all(info.auth_methods == [] and info.saved_connection_only is False for info in others)


def test_base_hooks_are_no_ops_for_other_dialects():
    for name in ("postgresql", "mysql", "sqlite", "duckdb", "mssql", "denodo"):
        other = get_dialect(name)
        assert other.with_credentials("x://u@h/d", "secret") == "x://u@h/d"
        assert other.session_info("x://u@h/d") is None
        assert other.browses_databases is False
        assert other.list_databases("x://u@h/d") is None
        with pytest.raises(ValueError, match="one database"):
            other.checked_database("OTHER")


def test_catalog_entry_says_which_dialects_browse_databases():
    by_name = {info.name: info for info in dialect_catalog()}
    assert dialect.browses_databases is True
    assert by_name["snowflake"].browses_databases is True
    assert all(not info.browses_databases for name, info in by_name.items() if name != "snowflake")


@pytest.mark.parametrize(
    ("typed", "host"),
    [
        ("myorg-myaccount", "myorg-myaccount.snowflakecomputing.com"),
        ("  MyOrg-MyAccount  ", "myorg-myaccount.snowflakecomputing.com"),
        ("xy12345.eu-west-1", "xy12345.eu-west-1.snowflakecomputing.com"),
        ("xy12345.us-east-2.aws", "xy12345.us-east-2.aws.snowflakecomputing.com"),
        ("myorg-myaccount.snowflakecomputing.com", "myorg-myaccount.snowflakecomputing.com"),
        ("myorg-myaccount.snowflakecomputing.com:443", "myorg-myaccount.snowflakecomputing.com"),
        ("https://myorg-myaccount.snowflakecomputing.com/console/login#/", "myorg-myaccount.snowflakecomputing.com"),
        ("myorg-myaccount.snowflakecomputing.com/", "myorg-myaccount.snowflakecomputing.com"),
        ("https://app.snowflake.com/myorg/myaccount/#/homepage", "myorg-myaccount.snowflakecomputing.com"),
        ("https://app.snowflake.com/eu-west-1/xy12345/worksheets", "xy12345.eu-west-1.snowflakecomputing.com"),
        ("https://app.snowflake.com/us-east-2.aws/xy12345/", "xy12345.us-east-2.aws.snowflakecomputing.com"),
        ("myorg-my_account", "myorg-my-account.snowflakecomputing.com"),
        ("myorg-myaccount.snowflakecomputing.cn", "myorg-myaccount.snowflakecomputing.cn"),
        ("https://app.snowflake.cn/myorg/myaccount/", "myorg-myaccount.snowflakecomputing.cn"),
        ("myorg-myaccount.privatelink.snowflakecomputing.com", "myorg-myaccount.privatelink.snowflakecomputing.com"),
    ],
)
def test_normalize_account_accepts(typed, host):
    assert normalize_account(typed) == host


@pytest.mark.parametrize(
    "typed",
    [
        "",
        "   ",
        "evil.com/x.snowflakecomputing.com",
        "https://evil.com/myorg-myaccount",
        "https://evil.com/x.snowflakecomputing.com",
        "my org",
        "user@myorg-myaccount",
        "https://user@myorg-myaccount.snowflakecomputing.com",
        "https://app.snowflake.com/",
        "https://app.snowflake.com/myorg",
        "-myorg",
        "myorg..acct",
    ],
)
def test_normalize_account_rejects(typed):
    with pytest.raises(SnowflakeAuthError):
        normalize_account(typed)


def test_account_from_host():
    assert account_from_host("myorg-myaccount.snowflakecomputing.com") == "myorg-myaccount"
    assert account_from_host("XY12345.EU-WEST-1.snowflakecomputing.com") == "xy12345.eu-west-1"
    assert account_from_host("a.snowflakecomputing.cn") == "a"


@pytest.fixture(scope="module")
def rsa_key():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _stored(key) -> str:
    der = key.private_bytes(serialization.Encoding.DER, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    return base64.b64encode(der).decode()


def test_private_key_unencrypted_pkcs8(rsa_key):
    pem = rsa_key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    ).decode()
    stored = normalize_private_key(pem)
    assert stored == _stored(rsa_key)
    loaded = serialization.load_der_private_key(private_key_der(stored), password=None)
    assert loaded.private_numbers() == rsa_key.private_numbers()


def test_private_key_encrypted(rsa_key):
    pem = rsa_key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.BestAvailableEncryption(b"open")
    ).decode()
    assert normalize_private_key(pem, "open") == _stored(rsa_key)
    with pytest.raises(SnowflakeAuthError, match="passphrase does not open"):
        normalize_private_key(pem, "wrong")
    with pytest.raises(SnowflakeAuthError, match="encrypted. Enter its passphrase"):
        normalize_private_key(pem)
    with pytest.raises(SnowflakeAuthError, match="encrypted. Enter its passphrase"):
        normalize_private_key(pem, "")


def test_private_key_passphrase_on_unencrypted_key(rsa_key):
    pem = rsa_key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    ).decode()
    with pytest.raises(SnowflakeAuthError, match="not encrypted"):
        normalize_private_key(pem, "something")


def test_private_key_traditional_rsa_pem(rsa_key):
    pem = rsa_key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.TraditionalOpenSSL, serialization.NoEncryption()
    ).decode()
    assert "BEGIN RSA PRIVATE KEY" in pem
    assert normalize_private_key(pem) == _stored(rsa_key)


def test_private_key_stored_form_is_idempotent(rsa_key):
    stored = _stored(rsa_key)
    assert normalize_private_key(stored) == stored
    wrapped = "\n".join(stored[i : i + 64] for i in range(0, len(stored), 64))
    assert normalize_private_key(wrapped) == stored


def test_private_key_refuses_public_ec_and_garbage(rsa_key):
    public = (
        rsa_key.public_key()
        .public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
        .decode()
    )
    with pytest.raises(SnowflakeAuthError, match="public key"):
        normalize_private_key(public)
    ec_pem = (
        ec.generate_private_key(ec.SECP256R1())
        .private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
        .decode()
    )
    with pytest.raises(SnowflakeAuthError, match="RSA"):
        normalize_private_key(ec_pem)
    for garbage in ("not a key at all", "-----BEGIN PRIVATE KEY-----\ngarbage\n-----END PRIVATE KEY-----", "QUJD"):
        with pytest.raises(SnowflakeAuthError, match="not a private key"):
            normalize_private_key(garbage)
    with pytest.raises(SnowflakeAuthError, match="not a private key"):
        normalize_private_key("not a key", "with-a-passphrase")
    with pytest.raises(SnowflakeAuthError, match="Choose your private key"):
        normalize_private_key("  ")


def test_pkce_pair_is_s256():
    pair = new_pkce_pair()
    assert 43 <= len(pair.verifier) <= 128
    assert set(pair.verifier) <= set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~")
    expected = base64.urlsafe_b64encode(hashlib.sha256(pair.verifier.encode()).digest()).rstrip(b"=").decode()
    assert pair.challenge == expected
    assert new_pkce_pair().verifier != pair.verifier


def test_authorize_url_parameters():
    url = authorize_url(
        "myorg-acct.snowflakecomputing.com",
        redirect_uri="http://127.0.0.1:63578/snowflake/oauth/callback",
        state="st",
        code_challenge="ch",
        role="ANALYST",
    )
    parts = urlsplit(url)
    assert (parts.scheme, parts.netloc, parts.path) == ("https", "myorg-acct.snowflakecomputing.com", "/oauth/authorize")
    params = {key: values[0] for key, values in parse_qs(parts.query).items()}
    assert params == {
        "client_id": "LOCAL_APPLICATION",
        "response_type": "code",
        "redirect_uri": "http://127.0.0.1:63578/snowflake/oauth/callback",
        "state": "st",
        "scope": "refresh_token session:role:ANALYST",
        "code_challenge": "ch",
        "code_challenge_method": "S256",
    }
    no_role = parse_qs(urlsplit(authorize_url("h", redirect_uri="r", state="s", code_challenge="c")).query)
    assert no_role["scope"] == ["refresh_token"]


def test_build_uri_contract_shape():
    assert dialect.build_uri(host="h", port=443, username="u", password="p", database="d") == "snowflake://u:p@h:443/d"
    assert dialect.build_uri(host="h") == "snowflake://h:443"
    with pytest.raises(ValueError):
        dialect.build_uri(host=None)
    with pytest.raises(ValueError):
        dialect.build_uri(host="h", auth="password")


def test_build_uri_ignores_ssl_and_timeout_without_warning(caplog):
    uri = dialect.build_uri(host="h", port=443, username="u", ssl_enabled=True, connect_timeout=5)
    assert uri == "snowflake://u@h:443"
    assert "ssl" not in caplog.text.lower()


def test_uri_round_trip_with_email_user_and_awkward_secret():
    secret = "a/b+c=d@e:f g%"
    uri = dialect.build_uri(
        host="myorg-acct.snowflakecomputing.com",
        port=443,
        username="first.last@example.com",
        password=secret,
        database="ANALYTICS",
        auth=AUTH_KEY_PAIR,
        role="DATA ENGINEER",
        warehouse="COMPUTE_WH",
    )
    assert "None" not in uri
    target = _parse_uri(uri)
    assert target.user == "first.last@example.com"
    assert target.secret == secret
    assert target.host == "myorg-acct.snowflakecomputing.com"
    assert target.port == 443
    assert target.database == "ANALYTICS"
    assert (target.auth, target.role, target.warehouse) == (AUTH_KEY_PAIR, "DATA ENGINEER", "COMPUTE_WH")


def test_parse_uri_defaults():
    target = _parse_uri("snowflake://u:t@h")
    assert (target.auth, target.port, target.database, target.role, target.warehouse) == (
        AUTH_ACCESS_TOKEN,
        443,
        None,
        None,
        None,
    )
    assert _parse_uri("snowflake://h:443/d?auth=oauth").auth == AUTH_OAUTH
    with pytest.raises(ValueError):
        _parse_uri("snowflake://h:443/d?auth=password")
    with pytest.raises(ValueError):
        _parse_uri("postgresql://u@h/d")


def test_with_credentials():
    bare = dialect.build_uri(host="h", port=443, username="me@x.com", database="D", auth=AUTH_OAUTH, role="R")
    merged = dialect.with_credentials(bare, "re/fresh+tok=en")
    target = _parse_uri(merged)
    assert target.secret == "re/fresh+tok=en"
    assert (target.user, target.database, target.auth, target.role) == ("me@x.com", "D", AUTH_OAUTH, "R")

    full = dialect.build_uri(host="h", port=443, username="u", password="old", database="D")
    assert dialect.with_credentials(full, None) == full
    assert _parse_uri(dialect.with_credentials(full, "new")).secret == "new"
    assert _parse_uri(dialect.with_credentials("snowflake://h:443/D", "tok")).secret == "tok"


def test_construct_sql_uri_url_is_unchanged_for_existing_dialects():
    cases = {
        "postgresql": "postgresql://u:p@h:5432/d?sslmode=require",
        "mysql": "mysql://u:p@h:3306/d",
        "sqlite": "sqlite:////tmp/x.db",
        "duckdb": "duckdb:////tmp/x.duckdb",
        "mssql": "mssql://u:p@h:1433/d",
        "denodo": "denodo://u:p@h:9996/d",
        "redshift": "redshift://u@h:5439/d",
    }
    for database_type, url in cases.items():
        assert construct_sql_uri(database_type=database_type, url=url, password="other") == url
        assert construct_sql_uri(database_type=database_type, url=url) == url


def test_construct_sql_uri_merges_the_snowflake_secret():
    url = dialect.build_uri(host="h", port=443, username="u", database="D", auth=AUTH_ACCESS_TOKEN)
    uri = construct_sql_uri(database_type="snowflake", url=url, password="tok/en")
    assert _parse_uri(uri).secret == "tok/en"
    built = construct_sql_uri(database_type="snowflake", host="h", port=443, username="u", password="p", database="d")
    assert built == "snowflake://u:p@h:443/d"


@pytest.mark.parametrize(
    ("type_name", "precision", "scale", "expected"),
    [
        ("FIXED", 38, 0, pl.Int64),
        ("FIXED", 10, None, pl.Int64),
        ("FIXED", 10, 2, pl.Decimal(10, 2)),
        ("FIXED", 38, 37, pl.Decimal(38, 37)),
        ("REAL", None, None, pl.Float64),
        ("TEXT", None, None, pl.String),
        ("BOOLEAN", None, None, pl.Boolean),
        ("DATE", None, None, pl.Date),
        ("TIME", None, 9, pl.Time),
        ("TIMESTAMP", None, 9, pl.Datetime("us")),
        ("TIMESTAMP_NTZ", None, 9, pl.Datetime("us")),
        ("TIMESTAMP_LTZ", None, 9, pl.Datetime("us", "UTC")),
        ("TIMESTAMP_TZ", None, 9, pl.Datetime("us", "UTC")),
        ("BINARY", None, None, pl.Binary),
        ("VARIANT", None, None, pl.String),
        ("OBJECT", None, None, pl.String),
        ("ARRAY", None, None, pl.String),
        ("GEOGRAPHY", None, None, pl.String),
        ("GEOMETRY", None, None, pl.String),
        ("timestamp_ntz", None, 9, pl.Datetime("us")),
    ],
)
def test_type_map(type_name, precision, scale, expected):
    assert polars_dtype(type_name, precision, scale) == expected


def test_type_map_unknowns():
    assert polars_dtype("VECTOR", None, None) is None
    assert polars_dtype("", None, None) is None
    assert polars_dtype("FIXED", 0, 2) is None
    assert polars_dtype("FIXED", 2, 5) is None


def test_cast_to_predicted_matches_the_type_map():
    """Frames shaped like the connector's Arrow output land on exactly the predicted dtypes."""
    la = dt.datetime(2024, 1, 1, 12, tzinfo=dt.timezone(dt.timedelta(hours=-8)))
    table = pa.table(
        {
            "SMALL": pa.array([1, 2], pa.int8()),
            "BIG": pa.array([Decimal(3), Decimal(4)], pa.decimal128(38, 0)),
            "PRICE": pa.array([Decimal("1.25"), None], pa.decimal128(10, 2)),
            "TS_TZ": pa.array([la, None], pa.timestamp("us", tz="America/Los_Angeles")),
            "TS_NTZ": pa.array([dt.datetime(2024, 1, 1), None], pa.timestamp("ns")),
            "OBJ": pa.array([{"a": 1}, None], pa.struct([("a", pa.int64())])),
            "ARR": pa.array([[1, 2], None], pa.list_(pa.int64())),
            "VAR": pa.array(['{"k": 1}', None], pa.string()),
            "VEC": pa.array([[1.0, 2.0], None], pa.list_(pa.float32(), 2)),
        }
    )
    targets = [
        polars_dtype("FIXED", 38, 0),
        polars_dtype("FIXED", 38, 0),
        polars_dtype("FIXED", 10, 2),
        polars_dtype("TIMESTAMP_TZ", 0, 9),
        polars_dtype("TIMESTAMP_NTZ", 0, 9),
        polars_dtype("OBJECT", None, None),
        polars_dtype("ARRAY", None, None),
        polars_dtype("VARIANT", None, None),
        polars_dtype("VECTOR", None, None),
    ]
    frame = _cast_to_predicted(pl.from_arrow(table), targets)
    expected = {name: dtype for name, dtype in zip(table.column_names, targets, strict=True) if dtype is not None}
    assert {name: frame.schema[name] for name in expected} == expected
    assert frame.schema["VEC"] == pl.Array(pl.Float32, 2)
    assert frame["BIG"].to_list() == [3, 4]
    assert frame["TS_TZ"][0] == dt.datetime(2024, 1, 1, 20, tzinfo=dt.timezone.utc)
    assert json.loads(frame["OBJ"][0]) == {"a": 1}
    assert json.loads(frame["ARR"][0]) == [1, 2]
    assert frame["OBJ"][1] is None


def test_quote_identifier_for_browsing():
    assert quote_identifier("PUBLIC") == "PUBLIC"
    assert quote_identifier("MY_TABLE$1") == "MY_TABLE$1"
    assert quote_identifier("mixedCase") == '"mixedCase"'
    assert quote_identifier("has space") == '"has space"'
    assert quote_identifier('quo"te') == '"quo""te"'
    assert quote_identifier("1ST") == '"1ST"'
    assert quote_identifier("ORDER") == '"ORDER"'


def test_ddl_identifier():
    assert ddl_identifier("id") == "id"
    assert ddl_identifier("Customer_ID") == "Customer_ID"
    assert ddl_identifier("my col") == '"my col"'
    assert ddl_identifier("a-b") == '"a-b"'
    assert ddl_identifier('x"y') == '"x""y"'
    assert ddl_identifier("order") == '"ORDER"'
    assert ddl_identifier('"Already Quoted"') == '"Already Quoted"'


def test_snowflake_ddl_types():
    expected = {
        pl.Int8: "NUMBER(38,0)",
        pl.Int64: "NUMBER(38,0)",
        pl.UInt64: "NUMBER(38,0)",
        pl.Float32: "FLOAT",
        pl.Float64: "FLOAT",
        pl.Decimal(10, 2): "NUMBER(10,2)",
        pl.String: "VARCHAR",
        pl.Categorical: "VARCHAR",
        pl.Enum(["a", "b"]): "VARCHAR",
        pl.Boolean: "BOOLEAN",
        pl.Date: "DATE",
        pl.Time: "TIME",
        pl.Datetime("us"): "TIMESTAMP_NTZ",
        pl.Datetime("ns", "Europe/Amsterdam"): "TIMESTAMP_TZ",
        pl.Binary: "BINARY",
        pl.List(pl.Int64): "VARIANT",
        pl.Array(pl.Int64, 2): "VARIANT",
        pl.Struct({"a": pl.Int64}): "VARIANT",
        pl.Duration("us"): "VARCHAR",
        pl.Null: "VARCHAR",
    }
    for dtype, ddl in expected.items():
        assert snowflake_type(dtype) == ddl, dtype


def test_parquet_ready_frame_writes_as_parquet(tmp_path):
    df = pl.DataFrame(
        {
            "u64": pl.Series([2**63 + 1], dtype=pl.UInt64),
            "u8": pl.Series([3], dtype=pl.UInt8),
            "cat": pl.Series(["x"], dtype=pl.Categorical),
            "dur": pl.Series([dt.timedelta(hours=1)]),
            "arr": pl.Series([[1, 2]], dtype=pl.Array(pl.Int64, 2)),
            "nothing": pl.Series([None], dtype=pl.Null),
            "s": ["keep"],
        }
    )
    ready = _parquet_ready(df)
    assert ready.schema == pl.Schema(
        {
            "u64": pl.Decimal(38, 0),
            "u8": pl.Int64,
            "cat": pl.String,
            "dur": pl.String,
            "arr": pl.List(pl.Int64),
            "nothing": pl.String,
            "s": pl.String,
        }
    )
    path = tmp_path / "x.parquet"
    ready.write_parquet(path, compression="snappy")
    assert pl.read_parquet(path)["u64"].to_list() == [Decimal(2**63 + 1)]


def test_put_url():
    assert _put_url("/tmp/a b/x.parquet") == "file:///tmp/a b/x.parquet"
    assert _put_url(r"C:\Users\me\AppData\Local\Temp\x.parquet") == "file://C:/Users/me/AppData/Local/Temp/x.parquet"
    assert _put_url("/tmp/o'brien/x.parquet") == "file:///tmp/o''brien/x.parquet"


def test_limit_query():
    limited = dialect.limit_query("SELECT a FROM t", 5)
    assert limited == "SELECT a FROM t LIMIT 5"
    assert sqlglot.parse_one(limited, read="snowflake") is not None


def test_write_refuses_bad_input_before_connecting():
    df = pl.DataFrame({"a": [1]})
    no_database = dialect.build_uri(host="h", port=443, username="u", password="p")
    with pytest.raises(SnowflakeError, match="Pick a database"):
        dialect.write(df, uri=no_database, table_name="PUBLIC.T")
    with_database = dialect.build_uri(host="h", port=443, username="u", password="p", database="D")
    with pytest.raises(ValueError, match="write mode"):
        dialect.write(df, uri=with_database, table_name="T", if_exists="truncate")
    with pytest.raises(ValueError, match="no columns"):
        dialect.write(pl.DataFrame(), uri=with_database, table_name="T")
    with pytest.raises(ValueError, match="table name"):
        dialect.write(df, uri=with_database, table_name="a.b.c.d")
    with pytest.raises(ValueError, match="table name"):
        dialect.write(df, uri=with_database, table_name="a..b")


def test_missing_secret_is_reported_before_the_driver_is_needed():
    with pytest.raises(SnowflakeError, match="Sign in again"):
        dialect._connect("snowflake://u@h:443/D?auth=oauth")
    with pytest.raises(SnowflakeError, match="no credential saved"):
        dialect._connect("snowflake://u@h:443/D")


@pytest.mark.skipif(_connector_installed(), reason="checks the message shown when the connector is absent")
def test_missing_connector_names_the_install_hint():
    with pytest.raises(ImportError, match="pip install snowflake-connector-python"):
        dialect._connect("snowflake://u:tok@h:443/D")


def _driver_error(message: str, errno: int | None = None) -> Exception:
    """An exception shaped like the connector's (module and ``errno``), without the connector."""
    cls = type("ProgrammingError", (Exception,), {"__module__": "snowflake.connector.errors"})
    exc = cls(message)
    exc.errno = errno if errno is not None else -1
    return exc


@pytest.mark.parametrize(
    ("message", "errno", "reason"),
    [
        ("000606 (57P03): No active warehouse selected in the current session.", 606, "Pick a warehouse"),
        ("Role 'SYSADMIN' specified in the connect string is not granted to this user.", 390189, "not granted"),
        ("Network policy is required.", 390432, "requires a network policy"),
        ("Incoming request with IP/Token 10.0.0.1 is not allowed to access Snowflake.", 390422, "IP address"),
        ("Programmatic access token is invalid.", None, "access token is expired or invalid"),
        ("Programmatic access token has expired.", None, "access token is expired or invalid"),
        ("JWT token is invalid.", 390144, "public key is not registered"),
        ("OAuth access token expired.", 390318, "Sign in again"),
        ("Invalid OAuth access token.", 390303, "Sign in again"),
        ("Incorrect username or password was specified.", 390100, "did not accept the username"),
        ("250003: 404 Not Found: post https://x.snowflakecomputing.com:443/session/v1/login-request", None, "find"),
        ("Failed to establish a new connection: [Errno 8] nodename nor servname provided", None, "find this account"),
    ],
)
def test_translate_error_reasons(message, errno, reason):
    text = translate_error(_driver_error(message, errno))
    assert reason in text
    assert message in text


def test_translate_error_falls_through_and_masks_the_secret():
    assert translate_error(_driver_error("Something odd happened.")) == "Something odd happened."
    masked = translate_error(_driver_error("bad token sekrit-123 seen"), secret="sekrit-123")
    assert "sekrit-123" not in masked and "***" in masked


def test_translated_block_wraps_driver_errors_only():
    with pytest.raises(SnowflakeError, match="Pick a warehouse") as caught:
        with _translated("tok"):
            raise _driver_error("No active warehouse selected", 606)
    assert caught.value.__cause__ is not None
    with pytest.raises(SnowflakeAuthError, match="expired"):
        with _translated("tok"):
            raise SnowflakeAuthError("The Snowflake sign-in has expired or was revoked. Sign in again.")
    with pytest.raises(ValueError, match="Table 'T' already exists"):
        with _translated("tok"):
            raise ValueError("Table 'T' already exists")
    with pytest.raises(KeyError):
        with _translated("tok"):
            raise KeyError("x")


class _Column(NamedTuple):
    name: str


class _FakeCursor:
    """Answers the statements the browse hooks run: ``SELECT CURRENT_DATABASE()`` and SHOW listings."""

    def __init__(self, current_database: str | None, shows: dict[str, list[dict]]):
        self.current_database = current_database
        self.shows = shows
        self.executed: list[str] = []
        self.description: list[_Column] = []
        self._rows: list[tuple] = []

    def execute(self, sql: str) -> None:
        self.executed.append(sql)
        if sql == "SELECT CURRENT_DATABASE()":
            self.description = [_Column("CURRENT_DATABASE()")]
            self._rows = [(self.current_database,)]
            return
        rows = self.shows[sql]
        columns = list(rows[0]) if rows else ["name"]
        self.description = [_Column(c) for c in columns]
        self._rows = [tuple(row[c] for c in columns) for row in rows]

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


@pytest.fixture
def fake_session(monkeypatch):
    """Points the dialect's cursor at a ``_FakeCursor`` and records the URI each session was opened with."""
    opened: list[str] = []

    def install(current_database: str | None, shows: dict[str, list[dict]]) -> _FakeCursor:
        cursor = _FakeCursor(current_database, shows)

        @contextmanager
        def fake_cursor(self, uri):
            opened.append(uri)
            yield cursor

        monkeypatch.setattr(SnowflakeDialect, "_cursor", fake_cursor)
        return cursor

    install.opened = opened
    return install


def test_list_databases_quotes_and_sorts_ignoring_case(fake_session):
    cursor = fake_session(
        None,
        {
            "SHOW DATABASES": [
                {"created_on": "x", "name": "SALES", "owner": "R"},
                {"created_on": "x", "name": "analytics_dev", "owner": "R"},
                {"created_on": "x", "name": "My.DB", "owner": "R"},
                {"created_on": "x", "name": "ANALYTICS", "owner": "R"},
            ]
        },
    )
    assert dialect.list_databases("snowflake://u:t@h:443") == ["ANALYTICS", '"analytics_dev"', '"My.DB"', "SALES"]
    assert cursor.executed == ["SHOW DATABASES"]


def test_list_schemas_of_the_uri_database_are_plain(fake_session):
    fake_session("SALES", {"SHOW SCHEMAS IN DATABASE": [{"name": "PUBLIC"}, {"name": "raw"}]})
    assert dialect.list_schemas("snowflake://u:t@h:443/SALES") == ["PUBLIC", '"raw"']


def test_list_schemas_without_a_database_are_qualified(fake_session):
    fake_session(
        None,
        {
            "SHOW SCHEMAS IN ACCOUNT": [
                {"database_name": "SALES", "name": "PUBLIC"},
                {"database_name": "HR", "name": "X"},
            ]
        },
    )
    assert dialect.list_schemas("snowflake://u:t@h:443") == ["SALES.PUBLIC", "HR.X"]


def test_list_schemas_of_a_database_the_session_cannot_use_is_an_error(fake_session):
    cursor = fake_session(None, {})
    with pytest.raises(SnowflakeError, match="Database NOPE does not exist"):
        dialect.list_schemas("snowflake://u:t@h:443/NOPE")
    assert cursor.executed == ["SELECT CURRENT_DATABASE()"]


def test_a_database_qualified_schema_lists_and_reads_that_schema(fake_session, monkeypatch):
    cursor = fake_session("ANALYTICS", {"SHOW TERSE OBJECTS IN SCHEMA SALES.PUBLIC": [{"name": "ORDERS"}]})
    assert dialect.list_tables("snowflake://u:t@h:443/ANALYTICS", "SALES.PUBLIC") == ["ORDERS"]
    assert cursor.executed == ["SHOW TERSE OBJECTS IN SCHEMA SALES.PUBLIC"]

    described: list[str] = []
    monkeypatch.setattr(SnowflakeDialect, "query_schema", lambda self, uri, query: described.append(query))
    dialect.table_schema("snowflake://u:t@h:443/ANALYTICS", "ORDERS", "SALES.PUBLIC")
    dialect.table_schema("snowflake://u:t@h:443/ANALYTICS", "SALES.PUBLIC.ORDERS", None)
    assert described == ["SELECT * FROM SALES.PUBLIC.ORDERS", "SELECT * FROM SALES.PUBLIC.ORDERS"]


@pytest.mark.parametrize(
    "name", ["ANALYTICS", "analytics_dev", "DB$1", '"My.DB"', '"has space"', '"quo""te"', " SALES "]
)
def test_checked_identifier_accepts_one_name(name):
    assert checked_identifier(name, "database") == name.strip()
    assert dialect.checked_database(name) == name.strip()


@pytest.mark.parametrize(
    "name", ["", "  ", "A.B", "has space", "a/b", 'x"y', '"open', '"a"b"', "1ST", "A;DROP", "DB?x=1", "a#b"]
)
def test_checked_identifier_refuses_anything_else(name):
    with pytest.raises(ValueError, match="not a Snowflake database name"):
        checked_identifier(name, "database")


class _TokenStub:
    """A real HTTP server standing in for Snowflake's ``/oauth/token-request``."""

    def __init__(self):
        self.requests: list[dict] = []
        self.status = 200
        self.body: dict | str = {
            "access_token": "acc-1",
            "refresh_token": "ref-1",
            "expires_in": 600,
            "username": "ME@EXAMPLE.COM",
            "token_type": "Bearer",
        }
        stub = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers.get("Content-Length") or 0)
                stub.requests.append(
                    {
                        "path": self.path,
                        "headers": dict(self.headers),
                        "form": {k: v[0] for k, v in parse_qs(self.rfile.read(length).decode()).items()},
                    }
                )
                payload = stub.body if isinstance(stub.body, str) else json.dumps(stub.body)
                self.send_response(stub.status)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(payload.encode())

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.server.server_address[1]}"

    def close(self):
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def token_stub(monkeypatch):
    stub = _TokenStub()
    monkeypatch.setattr(snowflake_auth, "_base_url", lambda host: stub.base_url)
    snowflake_auth.forget_access_tokens()
    yield stub
    snowflake_auth.forget_access_tokens()
    stub.close()


def _basic(user: str, password: str) -> str:
    return "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()


def test_exchange_code_posts_the_documented_request(token_stub):
    tokens = snowflake_auth.exchange_code(
        "acct.snowflakecomputing.com", code="the-code", verifier="the-verifier", redirect_uri="http://127.0.0.1:1/cb"
    )
    assert tokens == snowflake_auth.OAuthTokens(
        access_token="acc-1", refresh_token="ref-1", expires_in=600, username="ME@EXAMPLE.COM"
    )
    (request,) = token_stub.requests
    assert request["path"] == "/oauth/token-request"
    assert request["headers"]["Content-Type"] == "application/x-www-form-urlencoded"
    assert request["headers"]["Authorization"] == _basic("LOCAL_APPLICATION", "LOCAL_APPLICATION")
    assert request["form"] == {
        "grant_type": "authorization_code",
        "code": "the-code",
        "redirect_uri": "http://127.0.0.1:1/cb",
        "code_verifier": "the-verifier",
    }
    assert snowflake_auth.access_token_for("acct.snowflakecomputing.com", "ref-1") == "acc-1"
    assert len(token_stub.requests) == 1, "the sign-in's access token is cached under its refresh token"


def test_refresh_is_cached_until_expiry(token_stub):
    host = "acct.snowflakecomputing.com"
    assert snowflake_auth.access_token_for(host, "stored-refresh") == "acc-1"
    (request,) = token_stub.requests
    assert request["form"] == {"grant_type": "refresh_token", "refresh_token": "stored-refresh"}
    assert request["headers"]["Authorization"] == _basic("LOCAL_APPLICATION", "LOCAL_APPLICATION")

    token_stub.body = {"access_token": "acc-2", "expires_in": 600}
    assert snowflake_auth.access_token_for(host, "stored-refresh") == "acc-1"
    assert len(token_stub.requests) == 1

    key = snowflake_auth._cache_key(host, "stored-refresh")
    snowflake_auth._access_tokens[key] = (snowflake_auth._access_tokens[key][0], 0.0)
    assert snowflake_auth.access_token_for(host, "stored-refresh") == "acc-2"
    assert len(token_stub.requests) == 2
    assert snowflake_auth.access_token_for(host, "other-refresh") == "acc-2"
    assert len(token_stub.requests) == 3, "another refresh token gets its own access token"


def test_short_lived_token_is_not_cached(token_stub):
    token_stub.body = {"access_token": "brief", "expires_in": 30}
    snowflake_auth.access_token_for("h", "r")
    snowflake_auth.access_token_for("h", "r")
    assert len(token_stub.requests) == 2


@pytest.mark.parametrize(
    ("status", "body", "message"),
    [
        (400, {"error": "invalid_client", "message": "Client not found"}, "does not offer the built-in sign-in"),
        (400, {"error": "invalid_grant", "message": "Refresh token expired"}, "expired or was revoked"),
        (400, {"error": "invalid_scope", "message": "Role not allowed"}, r"refused the sign-in \(invalid_scope\)"),
        (500, "<html>oops</html>", r"refused the sign-in \(500\)"),
        (200, {"token_type": "Bearer"}, "refused the sign-in"),
        (200, ["not", "a", "dict"], "refused the sign-in"),
    ],
)
def test_token_errors(token_stub, status, body, message):
    token_stub.status = status
    token_stub.body = body if isinstance(body, str | dict) else json.dumps(body)
    with pytest.raises(SnowflakeAuthError, match=message):
        snowflake_auth.access_token_for("h", "secret-refresh")
    with pytest.raises(SnowflakeAuthError, match=message):
        snowflake_auth.exchange_code("h", code="c", verifier="v", redirect_uri="r")


def test_unreachable_host(monkeypatch):
    stub = _TokenStub()
    dead = stub.base_url
    stub.close()
    monkeypatch.setattr(snowflake_auth, "_base_url", lambda host: dead)
    snowflake_auth.forget_access_tokens()
    with pytest.raises(SnowflakeAuthError, match="Could not reach Snowflake at acct.snowflakecomputing.com"):
        snowflake_auth.access_token_for("acct.snowflakecomputing.com", "r")
