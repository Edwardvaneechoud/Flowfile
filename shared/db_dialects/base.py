"""Base class for database dialects.

A ``DbDialect`` bundles everything Flowfile needs to know about one database
vocabulary entry: catalog metadata (name, file_based, default port), URI
building, the read/write strategy, and optional fast-schema hooks. The base
class *is* the generic dialect — its method bodies are the historical
postgres-shaped code paths, so registering a dialect with no overrides changes
nothing about behavior.

Contract notes:
- All credential parameters are plain, already-decrypted strings; callers
  (core/worker) handle ``$ffsec$`` decryption before reaching this layer.
- Heavy or optional imports (drivers, db_reader/db_writer helpers) stay
  function-local so importing ``shared.db_dialects`` remains dependency-light.
- The four schema hooks return ``None`` to mean "no fast path — the caller
  keeps its existing generic implementation" (SQLAlchemy inspection in core).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    import polars as pl

logger = logging.getLogger(__name__)

# Database types speaking the postgres wire protocol, where libpq-style
# sslmode/connect_timeout query params are valid (pymysql rejects unknown params).
POSTGRES_FAMILY = {"postgresql", "postgres", "redshift"}

# Connection extra_params (dialect-specific settings like Snowflake's
# account/warehouse/role) must never be able to override credentials, the
# connection target, or transport security. Mirrors the Kafka blocked-config
# guard in shared/kafka/models.py: blocked keys are dropped at point of use;
# core additionally rejects them with a 422 at the API boundary.
_BLOCKED_EXTRA_PARAM_PREFIXES = ("private_key", "ssl")
_BLOCKED_EXTRA_PARAMS = frozenset(
    {
        "password",
        "user",
        "username",
        "host",
        "port",
        "database",
        "dbname",
        "auth_method",
        "authenticator",
        "token",
        "insecure_mode",
    }
)


def is_blocked_extra_param(key: str) -> bool:
    """Whether a connection extra_params key could override auth/target settings."""
    lowered = key.lower()
    return lowered in _BLOCKED_EXTRA_PARAMS or lowered.startswith(_BLOCKED_EXTRA_PARAM_PREFIXES)


@dataclass(frozen=True)
class DialectField:
    """A dialect-specific connection form field, carried in the connection's extra_params."""

    name: str
    label: str
    required: bool = False


class DbDialect:
    """One database dialect. Base behavior == the historical generic code paths."""

    name: ClassVar[str] = "generic"
    display_name: ClassVar[str] = "Generic"
    file_based: ClassVar[bool] = False
    default_port: ClassVar[int | None] = None
    supports_ssl: ClassVar[bool] = False
    sqlalchemy_driver: ClassVar[str | None] = None
    sqlglot_name: ClassVar[str] = "postgres"
    install_hint: ClassVar[str | None] = None
    # Dialect-specific connection fields (stored in extra_params) and standard form
    # fields the dialect does not use; served to the frontend via dialect_catalog()
    # so a new connection shape needs no frontend changes.
    extra_fields: ClassVar[tuple[DialectField, ...]] = ()
    hidden_fields: ClassVar[tuple[str, ...]] = ()
    # Authentication methods this dialect's build_uri understands. "password" is
    # the implicit default everywhere; dialects opting into more (e.g. Snowflake
    # key-pair JWT) extend this and handle the corresponding build_uri params.
    auth_methods: ClassVar[tuple[str, ...]] = ("password",)

    @property
    def uri_scheme(self) -> str:
        return self.name

    def is_available(self) -> bool:
        """Whether the driver stack for this dialect is importable."""
        return True

    def _check_auth_supported(self, auth_method: str | None, private_key: str | None) -> None:
        """Refuse credentials this dialect cannot honor — silent ignoring is worse than an error."""
        if auth_method not in (None, "", "password") and auth_method not in self.auth_methods:
            raise ValueError(f"{self.display_name} does not support auth method {auth_method!r}")
        if private_key and "key_pair" not in self.auth_methods:
            raise ValueError(f"{self.display_name} does not support private-key (key pair) authentication")

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
        **kwargs,
    ) -> str:
        """Build a base (connectorx-style) URI. ``password`` is a plain string."""
        from urllib.parse import quote_plus

        self._check_auth_supported(auth_method, private_key)
        if not host:
            raise ValueError("Host is required to create a URI")

        credentials = ""
        if username:
            credentials = username
            if password:
                credentials += f":{quote_plus(password)}"
            credentials += "@"

        port_section = f":{port}" if port else ""
        scheme = self.uri_scheme

        if database:
            base_uri = f"{scheme}://{credentials}{host}{port_section}/{database}"
        else:
            base_uri = f"{scheme}://{credentials}{host}{port_section}"

        query_params: dict[str, str] = {}
        if scheme.lower() in POSTGRES_FAMILY:
            if ssl_enabled:
                query_params["sslmode"] = "require"
            if connect_timeout is not None:
                query_params["connect_timeout"] = str(connect_timeout)
        elif ssl_enabled:
            logger.warning(
                "ssl_enabled was requested but database_type %r is not in the postgres family; "
                "no SSL parameter was applied and the connection may be unencrypted.",
                scheme,
            )
        query_params.update({k: v for k, v in kwargs.items() if v is not None and not is_blocked_extra_param(k)})

        if query_params:
            sep = "&" if "?" in base_uri else "?"
            params = "&".join(f"{key}={quote_plus(str(value))}" for key, value in query_params.items())
            base_uri += f"{sep}{params}"

        return base_uri

    def sqlalchemy_uri(self, uri: str) -> str:
        """Apply this dialect's SQLAlchemy driver suffix to a base URI."""
        if self.sqlalchemy_driver and uri.startswith(f"{self.uri_scheme}://"):
            return uri.replace(f"{self.uri_scheme}://", f"{self.sqlalchemy_driver}://", 1)
        return uri

    def read(
        self,
        query: str,
        uri: str,
        logger: logging.Logger,
        cancel_check: Callable[[], bool] | None = None,
    ) -> pl.DataFrame:
        from shared.db_reader import read_sql_with_fallback

        return read_sql_with_fallback(query, uri, logger, cancel_check=cancel_check)

    def write(self, df: pl.DataFrame, *, uri: str, table_name: str, if_exists: str = "append") -> None:
        from shared.db_writer import _write_df_via_sqlalchemy_core

        _write_df_via_sqlalchemy_core(df, uri, table_name, if_exists)

    def limit_query(self, select_sql: str, n: int) -> str:
        """Wrap a SELECT statement so it returns at most ``n`` rows."""
        return f"{select_sql} LIMIT {n}"

    def table_schema(self, uri: str, table_name: str, schema_name: str | None) -> pl.Schema | None:
        """Fast table schema without reading rows; None = caller uses its generic path."""
        return None

    def query_schema(self, uri: str, query: str) -> pl.Schema | None:
        """Fast schema for an arbitrary query without executing it; None = generic path."""
        return None

    def list_schemas(self, uri: str) -> list[str] | None:
        """Schema names for connection browsing; None = caller uses SQLAlchemy inspection."""
        return None

    def list_tables(self, uri: str, schema_name: str | None) -> list[str] | None:
        """Table names for connection browsing; None = caller uses SQLAlchemy inspection."""
        return None
