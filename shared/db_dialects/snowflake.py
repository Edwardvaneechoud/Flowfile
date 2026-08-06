"""Snowflake dialect: native connector Arrow reads, describe()-based fast schema.

connectorx has no Snowflake backend and the generic SQLAlchemy paths would need
the separate snowflake-sqlalchemy package, so every operation (read, write,
browse) goes through ``snowflake-connector-python`` directly. Reads use the
connector's Arrow fetch; like duckdb, ``cancel_check`` is accepted for
interface parity but not polled (the fetch happens in one native call).

Snowflake connections don't fit the host/port shape: the locator is an
*account identifier* and warehouse/role select compute/authorization context.
Those arrive through the connection's guarded ``extra_params`` and land in the
URI as ``snowflake://user:pass@account/database?warehouse=...&role=...``.
``build_uri`` drops any blocked key (``base.is_blocked_extra_param``) so extra
params can never override credentials.

Key-pair (JWT) auth is first-class, not an extra param:
``build_uri(auth_method="key_pair", private_key=<PEM text>)`` omits the
password from the URI userinfo and appends dialect-generated
``authenticator=snowflake_jwt&private_key=<urlsafe-b64 PEM>`` (plus an optional
b64 passphrase) *after* the blocked-key filter, so user extra_params can never
inject them. ``_connect_kwargs`` decodes the PEM back and hands the connector
unencrypted PKCS#8 DER bytes with ``authenticator="SNOWFLAKE_JWT"``; key
material only ever exists in memory, never on disk.

OAuth (SSO) auth follows the same shape: ``build_uri(auth_method="oauth",
oauth_token=<access token>)`` omits the password and appends dialect-generated
``authenticator=oauth&token=<urlsafe-b64 token>`` after the blocked-key filter;
``_connect_kwargs`` decodes it back and hands the connector
``authenticator="oauth", token=...``. Token refresh happens in core — this
module only transports an already-minted short-lived access token.

Fast schema uses ``cursor.describe()`` — the query is compiled server-side but
never executed — plus a dialect-local map keyed on the connector's type codes.
``read`` casts the fetched frame through the same map (Snowflake's Arrow
results pick per-batch physical types, e.g. the smallest int width that fits,
so an uncast schema would be value-dependent), which makes predicted schema
equal materialized schema by construction. Semi-structured types
(VARIANT/OBJECT/ARRAY, GEOGRAPHY/GEOMETRY) materialize as JSON text and map to
String; every FIXED column with scale 0 is normalized to Int64 (Snowflake
reports precision 38 for all integer columns). A column whose type is not in
the map makes the schema hooks return ``None`` (the caller keeps its generic
path) while ``read`` still returns whatever the connector yields for it.

Writes create the table from the frame's schema and bulk-insert with qmark
binding (the connector converts large ``executemany`` batches into stage-based
loads internally). Column identifiers are written quoted so frames round-trip
with their exact column names; table/schema identifiers stay unquoted when
they are plain, keeping Snowflake's case-insensitive resolution for user SQL.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, ClassVar

from shared.db_dialects.base import DbDialect, DialectField, is_blocked_extra_param

if TYPE_CHECKING:
    import polars as pl

logger = logging.getLogger(__name__)

_PLAIN_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def _ident(name: str) -> str:
    """Quote only non-plain identifiers so plain names keep case-insensitive resolution."""
    if _PLAIN_IDENT.match(name):
        return name
    return '"' + name.replace('"', '""') + '"'


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


class SnowflakeDialect(DbDialect):
    name: ClassVar[str] = "snowflake"
    display_name: ClassVar[str] = "Snowflake"
    default_port: ClassVar[int | None] = 443
    sqlglot_name: ClassVar[str] = "snowflake"
    install_hint: ClassVar[str | None] = "pip install snowflake-connector-python"
    extra_fields: ClassVar[tuple[DialectField, ...]] = (
        DialectField("account", "Account", required=True),
        DialectField("warehouse", "Warehouse"),
        DialectField("role", "Role"),
    )
    hidden_fields: ClassVar[tuple[str, ...]] = ("host", "port", "ssl")
    auth_methods: ClassVar[tuple[str, ...]] = ("password", "key_pair", "oauth")

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
        auth_method: str | None = None,
        private_key: str | None = None,
        private_key_passphrase: str | None = None,
        oauth_token: str | None = None,
        **kwargs,
    ) -> str:
        """Account-shaped URI; ``account`` comes from extra_params (``host`` is accepted as an alias)."""
        import base64
        from urllib.parse import quote_plus

        self._check_auth_supported(auth_method, private_key, oauth_token)
        use_key_pair = auth_method == "key_pair" or bool(private_key and auth_method in (None, "", "password"))
        if auth_method == "key_pair" and not private_key:
            raise ValueError("A private key is required for Snowflake key-pair authentication")
        use_oauth = auth_method == "oauth"
        if use_oauth and not oauth_token:
            raise ValueError("An access token is required for Snowflake OAuth authentication")
        if use_key_pair or use_oauth:
            password = None

        account = kwargs.pop("account", None) or host
        if not account:
            raise ValueError("Account is required to create a Snowflake URI")

        credentials = ""
        if username:
            credentials = quote_plus(username)
            if password:
                credentials += f":{quote_plus(password)}"
            credentials += "@"

        uri = f"{self.uri_scheme}://{credentials}{quote_plus(str(account))}"
        if database:
            uri += f"/{database}"
        params = {k: v for k, v in kwargs.items() if v is not None and not is_blocked_extra_param(k)}
        if use_key_pair:
            # Dialect-generated auth params, added after the blocked-key filter so
            # user extra_params can never inject or override them.
            params["authenticator"] = "snowflake_jwt"
            params["private_key"] = base64.urlsafe_b64encode(private_key.encode("utf-8")).decode("ascii")
            if private_key_passphrase:
                params["private_key_passphrase"] = base64.urlsafe_b64encode(
                    private_key_passphrase.encode("utf-8")
                ).decode("ascii")
        if use_oauth:
            # Dialect-generated auth params, after the blocked-key filter ("token" and
            # "authenticator" are blocked keys, so user extra_params can never inject them).
            params["authenticator"] = "oauth"
            params["token"] = base64.urlsafe_b64encode(oauth_token.encode("utf-8")).decode("ascii")
        if params:
            uri += "?" + "&".join(f"{key}={quote_plus(str(value))}" for key, value in params.items())
        return uri

    @classmethod
    def _connect_kwargs(cls, uri: str) -> dict[str, Any]:
        import base64
        from urllib.parse import parse_qsl, unquote, urlparse

        parsed = urlparse(uri)
        # netloc split instead of .hostname: account identifiers should keep their case.
        kwargs: dict[str, Any] = {"account": unquote(parsed.netloc.rsplit("@", 1)[-1])}
        if parsed.username:
            kwargs["user"] = unquote(parsed.username)
        if parsed.password:
            kwargs["password"] = unquote(parsed.password)
        database = parsed.path.lstrip("/")
        if database:
            kwargs["database"] = unquote(database)
        query = dict(parse_qsl(parsed.query))
        for key in ("warehouse", "role", "schema"):
            if query.get(key):
                kwargs[key] = query[key]
        if query.get("authenticator") == "snowflake_jwt" and query.get("private_key"):
            pem = base64.urlsafe_b64decode(query["private_key"])
            passphrase = (
                base64.urlsafe_b64decode(query["private_key_passphrase"])
                if query.get("private_key_passphrase")
                else None
            )
            kwargs["private_key"] = cls._private_key_der(pem, passphrase)
            kwargs["authenticator"] = "SNOWFLAKE_JWT"
            kwargs.pop("password", None)
        elif query.get("authenticator") == "oauth" and query.get("token"):
            kwargs["token"] = base64.urlsafe_b64decode(query["token"]).decode("utf-8")
            kwargs["authenticator"] = "oauth"
            kwargs.pop("password", None)
        return kwargs

    @staticmethod
    def _private_key_der(pem: bytes, passphrase: bytes | None) -> bytes:
        """PEM text (optionally passphrase-encrypted) -> unencrypted PKCS#8 DER bytes.

        snowflake-connector-python accepts DER bytes directly; decrypting here keeps
        the passphrase out of the connector call and the key out of any file.
        """
        from cryptography.hazmat.primitives import serialization

        key = serialization.load_pem_private_key(pem, password=passphrase)
        return key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

    def _connect(self, uri: str):
        import snowflake.connector

        return snowflake.connector.connect(paramstyle="qmark", **self._connect_kwargs(uri))

    @staticmethod
    def _polars_dtype(column) -> Any:
        """Map a cursor-describe ResultMetadata entry to the dtype the read path materializes."""
        import polars as pl
        from snowflake.connector.constants import FIELD_ID_TO_NAME

        type_name = FIELD_ID_TO_NAME.get(column.type_code, "")
        if type_name == "FIXED":
            scale = column.scale or 0
            if scale == 0:
                return pl.Int64
            return pl.Decimal(column.precision or 38, scale)
        simple = {
            "REAL": pl.Float64,
            "TEXT": pl.String,
            "DATE": pl.Date,
            "TIME": pl.Time,
            "TIMESTAMP": pl.Datetime("us"),
            "TIMESTAMP_NTZ": pl.Datetime("us"),
            "TIMESTAMP_LTZ": pl.Datetime("us", "UTC"),
            "TIMESTAMP_TZ": pl.Datetime("us", "UTC"),
            "BOOLEAN": pl.Boolean,
            "BINARY": pl.Binary,
            "VARIANT": pl.String,
            "OBJECT": pl.String,
            "ARRAY": pl.String,
            "GEOGRAPHY": pl.String,
            "GEOMETRY": pl.String,
        }
        return simple.get(type_name)

    def _describe_columns(self, uri: str, query: str):
        """Cursor-describe metadata without executing; None if the query can't be described."""
        try:
            con = self._connect(uri)
            try:
                cur = con.cursor()
                try:
                    return cur.describe(query)
                finally:
                    cur.close()
            finally:
                con.close()
        except Exception as exc:
            logger.debug("Snowflake describe failed: %s", exc)
            return None

    def read(
        self,
        query: str,
        uri: str,
        logger: logging.Logger,
        cancel_check: Callable[[], bool] | None = None,
    ) -> pl.DataFrame:
        import polars as pl

        con = self._connect(uri)
        try:
            cur = con.cursor()
            try:
                cur.execute(query)
                description = cur.description or []
                if hasattr(cur, "fetch_arrow_all"):
                    table = cur.fetch_arrow_all()
                    df = pl.from_arrow(table) if table is not None else None
                else:
                    # No Arrow support on this cursor (e.g. fakesnow): row-based fallback.
                    names = [col.name for col in description]
                    df = pl.DataFrame(cur.fetchall(), schema=names, orient="row", strict=False)
            finally:
                cur.close()
        finally:
            con.close()

        if df is None:
            schema = {col.name: (self._polars_dtype(col) or pl.String) for col in description if col.name}
            return pl.DataFrame(schema=schema)
        overrides = {
            col.name: dtype
            for col in description
            if col.name and col.name in df.columns and (dtype := self._polars_dtype(col)) is not None
        }
        return df.cast(overrides) if overrides else df

    def query_schema(self, uri: str, query: str) -> pl.Schema | None:
        import polars as pl

        columns = self._describe_columns(uri, query)
        if not columns:
            return None
        dtypes: dict[str, pl.DataType] = {}
        for column in columns:
            dtype = self._polars_dtype(column) if column.name else None
            if dtype is None:
                return None
            dtypes[column.name] = dtype
        return pl.Schema(dtypes)

    def table_schema(self, uri: str, table_name: str, schema_name: str | None) -> pl.Schema | None:
        parts = [p for p in ([schema_name] if schema_name else []) + table_name.split(".") if p]
        qualified = ".".join(_ident(p) for p in parts)
        return self.query_schema(uri, f"SELECT * FROM {qualified}")

    @staticmethod
    def _snowflake_type(dtype) -> str:
        import polars as pl

        if isinstance(dtype, pl.Decimal):
            return f"NUMBER({dtype.precision or 38},{dtype.scale or 0})"
        if dtype.is_integer():
            return "BIGINT"
        if dtype.is_float():
            return "DOUBLE"
        if dtype == pl.Boolean:
            return "BOOLEAN"
        if dtype == pl.Date:
            return "DATE"
        if dtype == pl.Time:
            return "TIME"
        if isinstance(dtype, pl.Datetime):
            return "TIMESTAMP_TZ" if dtype.time_zone else "TIMESTAMP_NTZ"
        if dtype == pl.Binary:
            return "BINARY"
        return "VARCHAR"

    @staticmethod
    def _table_exists(cur, schema_name: str | None, table_name: str) -> bool:
        if schema_name:
            cur.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE UPPER(table_name) = UPPER(?) AND UPPER(table_schema) = UPPER(?) LIMIT 1",
                (table_name, schema_name),
            )
        else:
            cur.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE UPPER(table_name) = UPPER(?) AND table_schema = CURRENT_SCHEMA() LIMIT 1",
                (table_name,),
            )
        return cur.fetchone() is not None

    def write(self, df: pl.DataFrame, *, uri: str, table_name: str, if_exists: str = "append") -> None:
        from shared.db_writer import _text_encoders

        schema_name, _, bare_name = table_name.rpartition(".")
        con = self._connect(uri)
        try:
            cur = con.cursor()
            try:
                if schema_name:
                    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {_ident(schema_name)}")
                qualified = ".".join(_ident(p) for p in ([schema_name] if schema_name else []) + [bare_name])
                if if_exists == "fail" and self._table_exists(cur, schema_name or None, bare_name):
                    raise ValueError(f"Table '{table_name}' already exists")
                column_defs = ", ".join(
                    f"{_quote_ident(name)} {self._snowflake_type(dtype)}" for name, dtype in df.schema.items()
                )
                create = "CREATE OR REPLACE TABLE" if if_exists == "replace" else "CREATE TABLE IF NOT EXISTS"
                cur.execute(f"{create} {qualified} ({column_defs})")

                encoders = _text_encoders(df)
                encoder_idx = {df.columns.index(name): encode for name, encode in encoders.items()}
                column_list = ", ".join(_quote_ident(name) for name in df.columns)
                placeholders = ", ".join(["?"] * df.width)
                insert = f"INSERT INTO {qualified} ({column_list}) VALUES ({placeholders})"
                for batch in df.iter_slices(16_384):
                    rows = [
                        tuple(
                            encoder_idx[i](value) if i in encoder_idx and value is not None else value
                            for i, value in enumerate(row)
                        )
                        for row in batch.rows()
                    ]
                    if rows:
                        cur.executemany(insert, rows)
            finally:
                cur.close()
        finally:
            con.close()

    def _execute_rows(self, uri: str, sql: str, params: tuple = ()) -> list[tuple]:
        con = self._connect(uri)
        try:
            cur = con.cursor()
            try:
                cur.execute(sql, params or None)
                return cur.fetchall()
            finally:
                cur.close()
        finally:
            con.close()

    def list_schemas(self, uri: str) -> list[str] | None:
        rows = self._execute_rows(
            uri,
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name <> 'INFORMATION_SCHEMA' ORDER BY schema_name",
        )
        return [row[0] for row in rows]

    def list_tables(self, uri: str, schema_name: str | None) -> list[str] | None:
        if schema_name:
            rows = self._execute_rows(
                uri,
                "SELECT table_name FROM information_schema.tables "
                "WHERE UPPER(table_schema) = UPPER(?) ORDER BY table_name",
                (schema_name,),
            )
            return [row[0] for row in rows]
        rows = self._execute_rows(
            uri,
            "SELECT table_schema, table_name FROM information_schema.tables "
            "WHERE table_schema <> 'INFORMATION_SCHEMA' ORDER BY table_schema, table_name",
        )
        return [f"{schema}.{name}" for schema, name in rows]
