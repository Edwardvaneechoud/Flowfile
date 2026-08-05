"""Built-in dialects: PostgreSQL, MySQL, SQLite, plus the Generic compat valve."""

from __future__ import annotations

from typing import TYPE_CHECKING

from shared.db_dialects.base import DbDialect

if TYPE_CHECKING:
    import polars as pl


class PostgresDialect(DbDialect):
    name = "postgresql"
    display_name = "PostgreSQL"
    default_port = 5432
    supports_ssl = True
    sqlglot_name = "postgres"


class MySQLDialect(DbDialect):
    name = "mysql"
    display_name = "MySQL"
    default_port = 3306
    sqlalchemy_driver = "mysql+pymysql"
    sqlglot_name = "mysql"


class SQLiteDialect(DbDialect):
    name = "sqlite"
    display_name = "SQLite"
    file_based = True
    sqlglot_name = "sqlite"

    def build_uri(self, *, host=None, database=None, auth_method=None, private_key=None, **kwargs) -> str:
        self._check_auth_supported(auth_method, private_key)
        path = database or host or "./database.db"
        # Strip sqlite:/// prefix if the full URI was passed as the path
        if path.startswith("sqlite:///"):
            path = path[len("sqlite:///") :]
        return f"sqlite:///{path}"

    def write(self, df: pl.DataFrame, *, uri: str, table_name: str, if_exists: str = "append") -> None:
        from shared.db_writer import _write_df_to_sqlite

        _write_df_to_sqlite(df, uri, table_name, if_exists)


class GenericDialect(DbDialect):
    """Fallback for stored ``database_type`` values outside the registry.

    ``database_type`` was historically a free string (redshift, mariadb, ...)
    and stored connections with such values must keep building the exact same
    URIs they always did. The raw name becomes the URI scheme verbatim, and
    postgres-family membership is decided by the same lowercased set as before.
    """

    def __init__(self, name: str):
        self._raw_name = name

    @property
    def uri_scheme(self) -> str:
        return self._raw_name
