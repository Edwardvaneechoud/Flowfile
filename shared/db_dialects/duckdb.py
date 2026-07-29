"""DuckDB dialect: native driver, file-based, LIMIT-0 fast schema.

DuckDB has no connectorx support, so reads/writes use the native ``duckdb``
package (Arrow-backed, zero-copy into Polars). Concurrency discipline:

- Every read/schema/browse path opens ``read_only=True`` (DuckDB allows
  unlimited concurrent read-only connections on one file) and closes the
  connection immediately — no pooling, so writers are never starved.
- ``write`` is the only exclusive (read-write) connect; it retries briefly on
  transient lock conflicts (e.g. a schema callback overlapping a run).

The fast-schema hooks wrap the query in ``SELECT * FROM (...) LIMIT 0``:
DuckDB plans the query without executing it, so the REAL column types come
back for arbitrary queries at near-zero cost — including query mode, which
for other dialects falls back to an all-String sample probe.

INTERVAL columns are projected to VARCHAR in both the read and schema paths:
they arrive as Arrow ``month_day_nano_interval``, which Polars cannot import
(calendar intervals have no fixed duration), so text is the only lossless
representation. Both paths share the rewrite, keeping predicted schema equal
to materialized schema.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, ClassVar

from shared.db_dialects.base import DbDialect

if TYPE_CHECKING:
    import polars as pl

logger = logging.getLogger(__name__)

_URI_PREFIX = "duckdb:///"
_LOCK_RETRIES = 3
_LOCK_BACKOFF_SECONDS = 0.25


def _import_duckdb():
    try:
        import duckdb
    except ImportError as exc:
        raise ImportError("duckdb is required for DuckDB connections. Install with: pip install duckdb") from exc
    return duckdb


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


class DuckDBDialect(DbDialect):
    name: ClassVar[str] = "duckdb"
    display_name: ClassVar[str] = "DuckDB"
    file_based: ClassVar[bool] = True
    sqlglot_name: ClassVar[str] = "duckdb"
    install_hint: ClassVar[str | None] = "pip install duckdb"

    def is_available(self) -> bool:
        try:
            _import_duckdb()
        except ImportError:
            return False
        return True

    def build_uri(self, *, host=None, database=None, **kwargs) -> str:
        path = database or host or "./database.duckdb"
        if path.startswith(_URI_PREFIX):
            path = path[len(_URI_PREFIX) :]
        return f"{_URI_PREFIX}{path}"

    @staticmethod
    def _path_from_uri(uri: str) -> str:
        return uri[len(_URI_PREFIX) :] if uri.startswith(_URI_PREFIX) else uri

    def _connect(self, uri: str, *, read_only: bool):
        duckdb = _import_duckdb()
        path = self._path_from_uri(uri)
        if read_only:
            import os

            if not os.path.exists(path):
                # A read-write connect would silently create an empty file here;
                # never do that on a read path.
                raise FileNotFoundError(f"DuckDB file not found: {path}")
        last_error: Exception | None = None
        for attempt in range(_LOCK_RETRIES):
            try:
                return duckdb.connect(path, read_only=read_only)
            except duckdb.IOException as exc:
                last_error = exc
                if attempt < _LOCK_RETRIES - 1:
                    time.sleep(_LOCK_BACKOFF_SECONDS * (attempt + 1))
        raise last_error

    @staticmethod
    def _interval_safe_query(con, query: str) -> str:
        """Project INTERVAL columns (incl. nested in lists/structs) to VARCHAR.

        Arrow's month_day_nano_interval has no Polars equivalent and fails hard
        on import; DESCRIBE only plans the query, so this probe reads no data.
        """
        columns = con.execute(f"DESCRIBE SELECT * FROM ({query}) AS _ff_q").fetchall()
        if not any("INTERVAL" in row[1].upper() for row in columns):
            return query
        select_list = ", ".join(
            f"CAST({_quote_ident(row[0])} AS VARCHAR) AS {_quote_ident(row[0])}"
            if "INTERVAL" in row[1].upper()
            else _quote_ident(row[0])
            for row in columns
        )
        return f"SELECT {select_list} FROM ({query}) AS _ff_q"

    def read(
        self,
        query: str,
        uri: str,
        logger: logging.Logger,
        cancel_check: Callable[[], bool] | None = None,
    ) -> pl.DataFrame:
        # cancel_check is accepted for interface parity but not polled: reads are
        # local-file and typically fast; interrupt support is a follow-up.
        con = self._connect(uri, read_only=True)
        try:
            return con.execute(self._interval_safe_query(con, query)).pl()
        finally:
            con.close()

    def write(self, df: pl.DataFrame, *, uri: str, table_name: str, if_exists: str = "append") -> None:
        if "." in table_name:
            schema, _, table = table_name.partition(".")
        else:
            schema, table = None, table_name
        qualified = f"{_quote_ident(schema)}.{_quote_ident(table)}" if schema else _quote_ident(table)

        con = self._connect(uri, read_only=False)
        try:
            con.register("_ff_df", df)
            if schema:
                con.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote_ident(schema)}")
            if if_exists == "fail" and self._table_exists(con, schema, table):
                raise ValueError(f"Table '{table_name}' already exists")
            if if_exists == "replace":
                con.execute(f"CREATE OR REPLACE TABLE {qualified} AS SELECT * FROM _ff_df")
            else:
                con.execute(f"CREATE TABLE IF NOT EXISTS {qualified} AS SELECT * FROM _ff_df WHERE 1=0")
                con.execute(f"INSERT INTO {qualified} BY NAME SELECT * FROM _ff_df")
        finally:
            con.close()

    @staticmethod
    def _table_exists(con, schema: str | None, table: str) -> bool:
        row = con.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
            [schema or "main", table],
        ).fetchone()
        return row is not None

    def query_schema(self, uri: str, query: str) -> pl.Schema | None:
        con = self._connect(uri, read_only=True)
        try:
            safe_query = self._interval_safe_query(con, query)
            return con.execute(f"SELECT * FROM ({safe_query}) AS _ff_probe LIMIT 0").pl().schema
        finally:
            con.close()

    def table_schema(self, uri: str, table_name: str, schema_name: str | None) -> pl.Schema | None:
        parts = [p for p in ([schema_name] if schema_name else []) + table_name.split(".") if p]
        qualified = ".".join(_quote_ident(p) for p in parts)
        return self.query_schema(uri, f"SELECT * FROM {qualified}")

    def list_schemas(self, uri: str) -> list[str] | None:
        con = self._connect(uri, read_only=True)
        try:
            rows = con.execute(
                "SELECT schema_name FROM information_schema.schemata "
                "WHERE catalog_name = current_database() "
                "AND schema_name NOT IN ('information_schema', 'pg_catalog')"
            ).fetchall()
            return [row[0] for row in rows]
        finally:
            con.close()

    def list_tables(self, uri: str, schema_name: str | None) -> list[str] | None:
        con = self._connect(uri, read_only=True)
        try:
            if schema_name:
                rows = con.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_type = 'BASE TABLE' AND table_schema = ?",
                    [schema_name],
                ).fetchall()
                return [row[0] for row in rows]
            rows = con.execute(
                "SELECT table_schema, table_name FROM information_schema.tables "
                "WHERE table_type = 'BASE TABLE' "
                "AND table_schema NOT IN ('information_schema', 'pg_catalog')"
            ).fetchall()
            return [f"{row[0]}.{row[1]}" for row in rows]
        finally:
            con.close()
