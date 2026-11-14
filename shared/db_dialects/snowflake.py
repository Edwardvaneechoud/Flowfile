"""Snowflake dialect: snowflake-connector-python sessions, Arrow reads, stage + COPY writes.

Snowflake has no connectorx backend and no SQLAlchemy driver here, so every hook
owns its driver access through ``snowflake.connector`` (imported at use; the
dialect reports unavailable without it). The schema and browse hooks therefore
always return a result or raise, never ``None``: the generic fallback is
SQLAlchemy inspection, which cannot reach Snowflake.

A connection is one URI, as every hook takes:
``snowflake://<user>:<secret>@<host>:443/<database>?auth=<method>&role=<r>&warehouse=<w>``.
``<host>`` is the real ``…snowflakecomputing.com`` hostname, the user and secret
are percent-encoded (login names are often e-mail addresses), and ``auth`` picks
what the secret is: an access token (sent through the connector's
``PROGRAMMATIC_ACCESS_TOKEN`` authenticator), a private key in base64 DER
(``SNOWFLAKE_JWT``) or an OAuth refresh token, exchanged for an access token in
this process (``shared.snowflake_auth.access_token_for``). Core may ship the URL
without its secret; ``with_credentials`` puts the secret back where it is opened.

Reads run through ``db_reader.run_cancellable_read`` and fetch Arrow batches
(``fetch_arrow_batches``, which needs pyarrow but not pandas; ``fetch_arrow_all``
does need pandas, so it is not used). The session sets ``arrow_number_to_decimal``
so a scaled NUMBER arrives as a decimal rather than a float. Every column is then
cast to the dtype the type map predicts from ``cursor.description``, the map
``query_schema`` applies to ``cursor.describe`` (which plans without executing),
so the predicted schema equals the materialised one by construction. Columns of
a type the map does not know arrive as Arrow gives them on read and are
predicted as String.

Identifiers listed for browsing come back usable verbatim in core's unquoted
FROM clause: a name that is not a plain upper-case identifier is double-quoted.
Writes create the table from the Polars schema (unquoted column names, which
Snowflake upper-cases, unless a name is not a plain identifier or is reserved),
upload one Parquet file to a temporary stage in the table's schema, and
``COPY INTO … MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE``. Driver errors become
``SnowflakeError`` with a plain-language reason ahead of the driver's own text.

Not run against a live account yet: the connector calls, the Arrow types the
connector hands back per Snowflake type, the Parquet load of nested and time
columns, and the error numbers ``translate_error`` matches on.
"""

from __future__ import annotations

import json
import logging
import os
import re
import tempfile
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, ClassVar, NamedTuple
from urllib.parse import parse_qsl, quote, quote_plus, unquote, unquote_plus, urlencode, urlsplit

from shared.db_dialects.base import DbDialect
from shared.snowflake_auth import (
    AUTH_ACCESS_TOKEN,
    AUTH_KEY_PAIR,
    AUTH_METHODS,
    AUTH_OAUTH,
    SnowflakeAuthError,
    access_token_for,
    account_from_host,
    private_key_der,
)

if TYPE_CHECKING:
    import polars as pl

logger = logging.getLogger(__name__)

_LOGIN_TIMEOUT_SECONDS = 30
_APPLICATION = "Flowfile"

_PLAIN_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
_PLAIN_UPPER_IDENTIFIER = re.compile(r"^[A-Z_][A-Z0-9_$]*$")
_IDENTIFIER_PART = r'(?:[A-Za-z_][A-Za-z0-9_$]*|"(?:[^"]|"")+")'
_IDENTIFIER_PATH = re.compile(rf"^{_IDENTIFIER_PART}(?:\.{_IDENTIFIER_PART})*$")
_IDENTIFIER = re.compile(rf"^{_IDENTIFIER_PART}$")

# Snowflake's reserved keywords: unquoted, these cannot name a column or table.
_RESERVED = frozenset(
    """ACCOUNT ALL ALTER AND ANY AS BETWEEN BY CASE CAST CHECK COLUMN CONNECT CONNECTION CONSTRAINT CREATE
    CROSS CURRENT CURRENT_DATE CURRENT_TIME CURRENT_TIMESTAMP CURRENT_USER DATABASE DELETE DISTINCT DROP ELSE
    EXISTS FALSE FOLLOWING FOR FROM FULL GRANT GROUP GSCLUSTER HAVING ILIKE IN INCREMENT INNER INSERT INTERSECT
    INTO IS ISSUE JOIN LATERAL LEFT LIKE LOCALTIME LOCALTIMESTAMP MINUS NATURAL NOT NULL OF ON OR ORDER
    ORGANIZATION QUALIFY REGEXP REVOKE RIGHT RLIKE ROW ROWS SAMPLE SCHEMA SELECT SET SOME START TABLE
    TABLESAMPLE THEN TO TRIGGER TRUE TRY_CAST UNION UNIQUE UPDATE USING VALUES VIEW WHEN WHENEVER WHERE
    WINDOW WITH""".split()
)

_STRING_TYPES = {"TEXT", "VARIANT", "OBJECT", "ARRAY", "GEOGRAPHY", "GEOMETRY"}


class SnowflakeError(ValueError):
    """A Snowflake call failed; the message says why in plain words, then what Snowflake said."""


class _Target(NamedTuple):
    user: str | None
    secret: str | None
    host: str
    port: int
    database: str | None
    auth: str
    role: str | None
    warehouse: str | None


def _import_connector():
    try:
        import snowflake.connector
    except ImportError as exc:
        raise ImportError(
            "snowflake-connector-python is required for Snowflake connections. "
            "Install with: pip install snowflake-connector-python"
        ) from exc
    return snowflake.connector


def _parse_uri(uri: str) -> _Target:
    """Split a ``snowflake://`` URI into its parts; a missing ``auth`` means an access token."""
    parts = urlsplit(uri)
    if parts.scheme != "snowflake" or not parts.hostname:
        raise ValueError("Not a Snowflake connection URI.")
    query = dict(parse_qsl(parts.query))
    auth = query.get("auth") or AUTH_ACCESS_TOKEN
    if auth not in AUTH_METHODS:
        raise ValueError(f"Unknown Snowflake sign-in method {auth!r}; use one of {', '.join(AUTH_METHODS)}.")
    database = unquote(parts.path.lstrip("/")) or None
    return _Target(
        user=unquote(parts.username) if parts.username else None,
        secret=unquote_plus(parts.password) if parts.password else None,
        host=parts.hostname,
        port=parts.port or 443,
        database=database,
        auth=auth,
        role=query.get("role") or None,
        warehouse=query.get("warehouse") or None,
    )


def polars_dtype(type_name: str, precision: int | None, scale: int | None) -> pl.DataType | None:
    """The Polars dtype of a Snowflake result column; None for a type this map does not know.

    ``type_name`` is the connector's name for the column's type code (``FIXED``,
    ``TEXT``, ``TIMESTAMP_NTZ``…). An integer NUMBER is Int64, so a value past the
    Int64 range fails the read's cast rather than turning into a null.
    """
    import polars as pl

    name = (type_name or "").upper()
    if name == "FIXED":
        if not scale:
            return pl.Int64
        if precision is not None and 1 <= precision <= 38 and 0 < scale <= precision:
            return pl.Decimal(precision, scale)
        return None
    if name in _STRING_TYPES:
        return pl.String
    simple = {
        "REAL": pl.Float64,
        "BOOLEAN": pl.Boolean,
        "DATE": pl.Date,
        "TIME": pl.Time,
        "TIMESTAMP": pl.Datetime("us"),
        "TIMESTAMP_NTZ": pl.Datetime("us"),
        "TIMESTAMP_LTZ": pl.Datetime("us", "UTC"),
        "TIMESTAMP_TZ": pl.Datetime("us", "UTC"),
        "BINARY": pl.Binary,
    }
    return simple.get(name)


def _type_name(type_code: int) -> str:
    from snowflake.connector.constants import FIELD_ID_TO_NAME

    return FIELD_ID_TO_NAME.get(type_code, "")


def _column_dtype(column) -> pl.DataType | None:
    return polars_dtype(_type_name(column.type_code), column.precision, column.scale)


def _cast_to_predicted(frame: pl.DataFrame, targets: list[pl.DataType | None]) -> pl.DataFrame:
    """Cast each column to its predicted dtype; nested values predicted as text become JSON text."""
    import polars as pl

    exprs = []
    for name, current, target in zip(frame.columns, frame.dtypes, targets, strict=True):
        if target is None or current == target:
            continue
        if target == pl.String and current.is_nested():
            exprs.append(pl.struct(pl.col(name).alias("v")).struct.json_encode().str.json_path_match("$.v").alias(name))
        else:
            exprs.append(pl.col(name).cast(target))
    return frame.with_columns(exprs) if exprs else frame


def quote_identifier(name: str) -> str:
    """A name as it works verbatim in unquoted SQL: plain upper-case names as they are, others double-quoted."""
    if _PLAIN_UPPER_IDENTIFIER.match(name) and name not in _RESERVED:
        return name
    return '"' + name.replace('"', '""') + '"'


def ddl_identifier(name: str) -> str:
    """A column or table name for DDL: unquoted when plain (Snowflake upper-cases it), else double-quoted.

    A plain name that is a reserved word is quoted in upper case, so it means what
    the unquoted name would; a name already in double quotes is kept as given.
    """
    if len(name) >= 2 and name.startswith('"') and name.endswith('"'):
        return name
    if _PLAIN_IDENTIFIER.match(name):
        return f'"{name.upper()}"' if name.upper() in _RESERVED else name
    return '"' + name.replace('"', '""') + '"'


def snowflake_type(dtype: pl.DataType) -> str:
    """The Snowflake column type a Polars dtype is written as."""
    import polars as pl

    if dtype == pl.Boolean:
        return "BOOLEAN"
    if dtype.is_integer():
        return "NUMBER(38,0)"
    if dtype.is_float():
        return "FLOAT"
    if isinstance(dtype, pl.Decimal):
        return f"NUMBER({dtype.precision or 38},{dtype.scale or 0})"
    if dtype == pl.String or isinstance(dtype, pl.Categorical | pl.Enum):
        return "VARCHAR"
    if dtype == pl.Date:
        return "DATE"
    if dtype == pl.Time:
        return "TIME"
    if isinstance(dtype, pl.Datetime):
        return "TIMESTAMP_TZ" if dtype.time_zone else "TIMESTAMP_NTZ"
    if dtype == pl.Binary:
        return "BINARY"
    if isinstance(dtype, pl.List | pl.Array | pl.Struct):
        return "VARIANT"
    return "VARCHAR"


def _parquet_ready(df: pl.DataFrame) -> pl.DataFrame:
    """The frame with every column in a Parquet type Snowflake loads into the column ``snowflake_type`` names."""
    import polars as pl

    exprs = []
    for name, dtype in df.schema.items():
        col = pl.col(name)
        if dtype in (pl.UInt64, pl.Int128):
            exprs.append(col.cast(pl.Decimal(38, 0)))
        elif dtype.is_unsigned_integer():
            exprs.append(col.cast(pl.Int64))
        elif isinstance(dtype, pl.Categorical | pl.Enum) or dtype == pl.Null:
            exprs.append(col.cast(pl.String))
        elif isinstance(dtype, pl.Duration):
            exprs.append(col.dt.to_string())
        elif isinstance(dtype, pl.Array):
            exprs.append(col.cast(pl.List(dtype.inner)))
        elif dtype == pl.Object:
            exprs.append(col.map_elements(str, return_dtype=pl.String))
    return df.with_columns(exprs) if exprs else df


def _put_url(path: str) -> str:
    """The ``file://`` URL of a local file for ``PUT``: forward slashes, ``file://C:/…`` on Windows."""
    url = "file://" + path.replace("\\", "/")
    return url.replace("'", "''")


def _split_table_name(table_name: str) -> list[str]:
    parts = (table_name or "").strip().split(".")
    if any(not p for p in parts) or len(parts) > 3:
        raise ValueError(f"'{table_name}' is not a Snowflake table name; use table, schema.table or db.schema.table.")
    return [ddl_identifier(p) for p in parts]


def checked_identifier(name: str, what: str = "identifier") -> str:
    """One identifier, plain or double-quoted, as given; anything else (a dot outside quotes, a space) is refused.

    ``what`` names the identifier in the error ("database", "schema").
    """
    value = (name or "").strip()
    if not _IDENTIFIER.match(value):
        raise ValueError(f"'{name}' is not a Snowflake {what} name; use one name, in double quotes if it needs them.")
    return value


def _checked_path(name: str) -> str:
    """An identifier path from browsing or a user, refused when it is anything but dotted identifiers."""
    value = (name or "").strip()
    if not _IDENTIFIER_PATH.match(value):
        raise ValueError(f"'{name}' is not a Snowflake schema name.")
    return value


_HOST_NOT_FOUND_MARKS = (
    "nodename nor servname",
    "name or service not known",
    "getaddrinfo failed",
    "failed to resolve",
    "temporary failure in name resolution",
    "no address associated with hostname",
)


def _plain_reason(errno: int | None, text: str) -> str | None:
    if errno == 606 or "no active warehouse" in text:
        return "No warehouse is selected. Pick a warehouse on the connection, or ask an administrator for a default."
    if errno == 390189 or ("role" in text and "is not granted" in text):
        return "The role on this connection is not granted to this user. Pick another role, or leave it empty."
    if errno == 390432 or "network policy is required" in text:
        return (
            "Snowflake requires a network policy for access-token sign-in and this user has none. "
            "Ask an administrator to attach one that allows your IP address, or use a key-pair."
        )
    if errno == 390422 or "is not allowed to access snowflake" in text:
        return "Snowflake's network policy does not allow this IP address. Ask an administrator to allow it."
    if "programmatic access token" in text and ("expired" in text or "invalid" in text):
        return "The access token is expired or invalid. Generate a new one in Snowflake and paste it on the connection."
    if errno == 390144 or "jwt token is invalid" in text:
        return (
            "Snowflake did not accept the key-pair: the public key is not registered on this user, "
            "or the username is wrong."
        )
    if errno in (390303, 390318) or ("oauth access token" in text and ("expired" in text or "invalid" in text)):
        return "The Snowflake sign-in has expired or is no longer valid. Sign in again."
    if errno == 390100 or "incorrect username or password" in text:
        return "Snowflake did not accept the username or the credential on this connection."
    if ("404" in text and "login-request" in text) or any(mark in text for mark in _HOST_NOT_FOUND_MARKS):
        return "Snowflake could not find this account. Check the account on the connection."
    return None


def translate_error(exc: BaseException, *, secret: str | None = None) -> str:
    """A plain-language message for a driver error, with the driver's own text appended.

    Matches conservatively on the error number and text and falls through to the
    driver's message. ``secret``, when given, is masked out of the result.
    """
    errno = getattr(exc, "errno", None)
    driver_text = str(exc).strip() or type(exc).__name__
    reason = _plain_reason(errno if isinstance(errno, int) else None, driver_text.lower())
    message = f"{reason} Snowflake said: {driver_text}" if reason else driver_text
    if secret and len(secret) >= 4:
        message = message.replace(secret, "***")
    return message


def _is_driver_error(exc: BaseException) -> bool:
    return type(exc).__module__.startswith("snowflake.connector")


@contextmanager
def _translated(secret: str | None) -> Iterator[None]:
    """Turn a connector error raised inside the block into a ``SnowflakeError``; anything else passes."""
    try:
        yield
    except SnowflakeAuthError:
        raise
    except Exception as exc:
        if not _is_driver_error(exc):
            raise
        raise SnowflakeError(translate_error(exc, secret=secret)) from exc


def _already_exists(exc: BaseException) -> bool:
    return getattr(exc, "errno", None) == 2002 or "already exists" in str(exc).lower()


def _rows_by_name(cursor, sql: str) -> list[dict[str, Any]]:
    """Run a statement (a SHOW, typically) and return its rows keyed by lower-cased column name."""
    cursor.execute(sql)
    names = [str(c.name).lower() for c in (cursor.description or [])]
    return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


class SnowflakeDialect(DbDialect):
    name: ClassVar[str] = "snowflake"
    display_name: ClassVar[str] = "Snowflake"
    default_port: ClassVar[int | None] = 443
    supports_ssl: ClassVar[bool] = False
    sqlalchemy_driver: ClassVar[str | None] = None
    sqlglot_name: ClassVar[str] = "snowflake"
    install_hint: ClassVar[str | None] = 'pip install "flowfile[snowflake]"'
    auth_methods: ClassVar[tuple[str, ...]] = AUTH_METHODS
    saved_connection_only: ClassVar[bool] = True
    browses_databases: ClassVar[bool] = True

    def is_available(self) -> bool:
        try:
            import snowflake.connector  # noqa: F401
        except ImportError:
            return False
        return True

    def build_uri(
        self,
        *,
        host: str | None = None,
        port: int | None = None,
        username: str | None = None,
        password: str | None = None,
        database: str | None = None,
        ssl_enabled: bool = False,
        connect_timeout: int | None = None,
        auth: str | None = None,
        role: str | None = None,
        warehouse: str | None = None,
        **kwargs,
    ) -> str:
        """The connection URI; TLS is always on, so ``ssl_enabled`` and ``connect_timeout`` add nothing."""
        if not host:
            raise ValueError("Host is required to create a URI")
        if auth is not None and auth not in AUTH_METHODS:
            raise ValueError(f"Unknown Snowflake sign-in method {auth!r}; use one of {', '.join(AUTH_METHODS)}.")
        credentials = ""
        if username or password:
            credentials = quote(username or "", safe="")
            if password:
                credentials += f":{quote_plus(password)}"
            credentials += "@"
        uri = f"snowflake://{credentials}{host}:{port or self.default_port}"
        if database:
            uri += f"/{quote(database, safe='')}"
        params = {key: value for key, value in (("auth", auth), ("role", role), ("warehouse", warehouse)) if value}
        if params:
            uri += f"?{urlencode(params)}"
        return uri

    def with_credentials(self, url: str, password: str | None) -> str:
        """``url`` with ``password`` as its secret; a None password leaves the URL as it is."""
        if password is None:
            return url
        parts = urlsplit(url)
        userinfo, _, hostport = parts.netloc.rpartition("@")
        user = userinfo.split(":", 1)[0]
        return parts._replace(netloc=f"{user}:{quote_plus(password)}@{hostport}").geturl()

    def _connect(self, uri: str):
        """A connector session for ``uri``; raises ``SnowflakeError`` with the reason when sign-in fails."""
        target = _parse_uri(uri)
        with _translated(target.secret):
            return self._connect_target(target)

    def _connect_target(self, target: _Target):
        if not target.secret:
            if target.auth == AUTH_OAUTH:
                raise SnowflakeError("This Snowflake connection has no sign-in saved. Sign in again.")
            raise SnowflakeError("This Snowflake connection has no credential saved. Edit the connection to add it.")
        connector = _import_connector()
        kwargs: dict[str, Any] = {
            "account": account_from_host(target.host),
            "host": target.host,
            "port": target.port,
            "user": target.user,
            "database": target.database,
            "warehouse": target.warehouse,
            "role": target.role,
            "login_timeout": _LOGIN_TIMEOUT_SECONDS,
            "application": _APPLICATION,
            "arrow_number_to_decimal": True,
        }
        if target.auth == AUTH_KEY_PAIR:
            kwargs.update(authenticator="SNOWFLAKE_JWT", private_key=private_key_der(target.secret))
        elif target.auth == AUTH_OAUTH:
            kwargs.update(authenticator="oauth", token=access_token_for(target.host, target.secret))
        else:
            kwargs.update(authenticator="PROGRAMMATIC_ACCESS_TOKEN", token=target.secret)
        return connector.connect(**{key: value for key, value in kwargs.items() if value is not None})

    @contextmanager
    def _cursor(self, uri: str) -> Iterator[Any]:
        target = _parse_uri(uri)
        with _translated(target.secret):
            conn = self._connect_target(target)
            try:
                cursor = conn.cursor()
                try:
                    yield cursor
                finally:
                    cursor.close()
            finally:
                conn.close()

    def _read_frame(self, uri: str, query: str) -> pl.DataFrame:
        with self._cursor(uri) as cursor:
            cursor.execute(query)
            columns = list(cursor.description or [])
            targets = [_column_dtype(c) for c in columns]
            return _fetch_frame(cursor, columns, targets)

    def read(
        self,
        query: str,
        uri: str,
        logger: logging.Logger,
        cancel_check: Callable[[], bool] | None = None,
    ) -> pl.DataFrame:
        from shared.db_reader import run_cancellable_read

        return run_cancellable_read("snowflake", lambda: self._read_frame(uri, query), cancel_check)

    def query_schema(self, uri: str, query: str) -> pl.Schema:
        import polars as pl

        with self._cursor(uri) as cursor:
            columns = cursor.describe(query)
        if columns is None:
            raise SnowflakeError("Snowflake could not describe the columns of this query.")
        return pl.Schema({c.name: _column_dtype(c) or pl.String for c in columns})

    def table_schema(self, uri: str, table_name: str, schema_name: str | None) -> pl.Schema:
        # Unquoted on purpose: mirrors the exact FROM clause SqlSource executes.
        qualified = f"{schema_name}.{table_name}" if schema_name else table_name
        return self.query_schema(uri, f"SELECT * FROM {qualified}")

    def checked_database(self, name: str) -> str:
        return checked_identifier(name, "database")

    def list_databases(self, uri: str) -> list[str]:
        """The databases the session's role may see, each usable verbatim in a dotted path, sorted ignoring case."""
        with self._cursor(uri) as cursor:
            rows = _rows_by_name(cursor, "SHOW DATABASES")
        return [quote_identifier(name) for name in sorted((str(row["name"]) for row in rows), key=str.lower)]

    def list_schemas(self, uri: str) -> list[str]:
        """The schemas of the URI's database; with no database in the URI, every schema as ``DATABASE.SCHEMA``.

        A database the URI names that the session does not land in (it does not exist, or the role may
        not use it) is an error rather than a silent fall back to the whole account.
        """
        database = _parse_uri(uri).database
        with self._cursor(uri) as cursor:
            if _current_database(cursor):
                rows = _rows_by_name(cursor, "SHOW SCHEMAS IN DATABASE")
                return [quote_identifier(row["name"]) for row in rows]
            if database:
                raise SnowflakeError(
                    f"Database {database} does not exist, or the role on this connection cannot use it."
                )
            rows = _rows_by_name(cursor, "SHOW SCHEMAS IN ACCOUNT")
            return [f"{quote_identifier(row['database_name'])}.{quote_identifier(row['name'])}" for row in rows]

    def list_tables(self, uri: str, schema_name: str | None) -> list[str]:
        with self._cursor(uri) as cursor:
            if schema_name:
                rows = _rows_by_name(cursor, f"SHOW TERSE OBJECTS IN SCHEMA {_checked_path(schema_name)}")
                return [quote_identifier(row["name"]) for row in rows]
            if _current_database(cursor):
                rows = _rows_by_name(cursor, "SHOW TERSE OBJECTS IN DATABASE")
                return [f"{quote_identifier(row['schema_name'])}.{quote_identifier(row['name'])}" for row in rows]
            rows = _rows_by_name(cursor, "SHOW TERSE OBJECTS IN ACCOUNT")
            return [
                ".".join(quote_identifier(row[key]) for key in ("database_name", "schema_name", "name")) for row in rows
            ]

    def session_info(self, uri: str) -> dict[str, str]:
        with self._cursor(uri) as cursor:
            return _session_info(cursor)

    def discover(self, uri: str) -> dict[str, Any]:
        """The session's user, role, warehouse and database plus the warehouses, databases and roles on offer.

        Each list is best-effort: a listing the role may not run comes back empty.
        """
        with self._cursor(uri) as cursor:
            info: dict[str, Any] = dict(_session_info(cursor))
            info["warehouses"] = _best_effort(lambda: [r["name"] for r in _rows_by_name(cursor, "SHOW WAREHOUSES")])
            info["databases"] = _best_effort(lambda: [r["name"] for r in _rows_by_name(cursor, "SHOW DATABASES")])
            info["roles"] = _best_effort(lambda: _available_roles(cursor))
        return info

    def write(self, df: pl.DataFrame, *, uri: str, table_name: str, if_exists: str = "append") -> None:
        if if_exists not in ("append", "replace", "fail"):
            raise ValueError(f"Unknown write mode {if_exists!r}; use append, replace or fail.")
        if df.width == 0:
            raise ValueError("A frame with no columns cannot be written to Snowflake.")
        target = _parse_uri(uri)
        parts = _split_table_name(table_name)
        if not target.database and len(parts) < 3:
            raise SnowflakeError(
                "Pick a database on the Snowflake connection before writing: Snowflake needs one to hold "
                "the table and the stage the rows are uploaded to."
            )
        qualified = ".".join(parts)
        stage = ".".join([*parts[:-1], f"FLOWFILE_STAGE_{uuid.uuid4().hex.upper()}"])
        columns = ", ".join(f"{ddl_identifier(name)} {snowflake_type(dtype)}" for name, dtype in df.schema.items())
        create = {"fail": "CREATE TABLE", "replace": "CREATE OR REPLACE TABLE", "append": "CREATE TABLE IF NOT EXISTS"}

        with tempfile.TemporaryDirectory(prefix="flowfile_snowflake_") as tmp:
            path = os.path.join(tmp, f"{uuid.uuid4().hex}.parquet")
            if df.height:
                _parquet_ready(df).write_parquet(path, compression="snappy")
            with self._cursor(uri) as cursor:
                try:
                    cursor.execute(f"{create[if_exists]} {qualified} ({columns})")
                except Exception as exc:
                    if if_exists == "fail" and _is_driver_error(exc) and _already_exists(exc):
                        raise ValueError(f"Table '{table_name}' already exists") from None
                    raise
                if not df.height:
                    return
                cursor.execute(f"CREATE TEMPORARY STAGE {stage}")
                cursor.execute(f"PUT '{_put_url(path)}' @{stage} AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
                cursor.execute(
                    f"COPY INTO {qualified} FROM @{stage} FILE_FORMAT=(TYPE=PARQUET USE_LOGICAL_TYPE=TRUE) "
                    "MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE PURGE=TRUE"
                )


def _fetch_frame(cursor, columns: list, targets: list[pl.DataType | None]) -> pl.DataFrame:
    """The executed query's rows as a frame in the predicted dtypes, through Arrow batches when the result is Arrow."""
    import polars as pl
    from snowflake.connector.errors import NotSupportedError

    names = [c.name for c in columns]
    try:
        batches = [pl.from_arrow(table) for table in cursor.fetch_arrow_batches(force_microsecond_precision=True)]
    except NotSupportedError:
        overrides = {name: dtype for name, dtype in zip(names, targets, strict=True) if dtype is not None}
        rows = cursor.fetchall()
        return _cast_to_predicted(pl.DataFrame(rows, schema=names, schema_overrides=overrides, orient="row"), targets)
    if not batches:
        return pl.DataFrame(schema={name: dtype or pl.String for name, dtype in zip(names, targets, strict=True)})
    frames = [_cast_to_predicted(frame, targets) for frame in batches]
    return frames[0] if len(frames) == 1 else pl.concat(frames, how="vertical_relaxed")


def _current_database(cursor) -> str | None:
    cursor.execute("SELECT CURRENT_DATABASE()")
    row = cursor.fetchone()
    return row[0] if row and row[0] else None


def _session_info(cursor) -> dict[str, str]:
    cursor.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE()")
    row = cursor.fetchone() or (None, None, None, None)
    return {key: value or "" for key, value in zip(("user", "role", "warehouse", "database"), row, strict=True)}


def _available_roles(cursor) -> list[str]:
    cursor.execute("SELECT CURRENT_AVAILABLE_ROLES()")
    row = cursor.fetchone()
    roles = json.loads(row[0]) if row and row[0] else []
    return sorted((str(r) for r in roles), key=str.lower)


def _best_effort(listing: Callable[[], list[str]]) -> list[str]:
    try:
        return listing()
    except Exception as exc:
        logger.debug("Snowflake listing failed: %s", type(exc).__name__)
        return []
