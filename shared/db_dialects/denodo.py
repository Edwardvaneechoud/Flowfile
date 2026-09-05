"""Denodo dialect: psycopg2 reads over Denodo's PostgreSQL-compatible port (9996).

First version of the Denodo connector (issue #692). Reads and fast schema use psycopg2's DB-API
directly: connectorx needs the binary ``COPY`` protocol Denodo lacks, and
SQLAlchemy's postgres dialect runs ``pg_catalog``/``SHOW`` probes Denodo only
partly emulates. Type OIDs from ``cursor.description`` map to Polars dtypes,
applied as ``schema_overrides`` on read and via a ``LIMIT 0`` probe in
``query_schema``, so predicted schema equals materialized schema. Unknown OIDs
(or an unconstrained numeric) make the schema hooks return ``None`` so the
caller falls back to a sample read.

Writes and schema/table browsing keep the base SQLAlchemy paths and are
untested against a live Denodo server.

TODO(denodo-adbc): Phase 2 — Arrow Flight SQL via ADBC (port 9994) for large
extracts; the psycopg2 path stays as the fallback.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, ClassVar

from shared.db_dialects.base import DbDialect

if TYPE_CHECKING:
    import polars as pl

logger = logging.getLogger(__name__)

_NUMERIC_OID = 1700


def _import_psycopg2():
    try:
        import psycopg2
    except ImportError as exc:
        raise ImportError(
            "psycopg2 is required for Denodo connections. Install with: pip install psycopg2-binary"
        ) from exc
    return psycopg2


def _polars_dtype(column) -> pl.DataType | None:
    """Map a psycopg2 ``cursor.description`` entry (type OID + precision/scale) to a Polars dtype."""
    import polars as pl

    if column.type_code == _NUMERIC_OID:
        # An unconstrained numeric (typmod -1) surfaces as precision 65535 / scale 65531.
        precision, scale = column.precision, column.scale
        if precision is None or scale is None or not (1 <= precision <= 38) or not (0 <= scale <= precision):
            return None
        return pl.Decimal(precision, scale)
    simple = {
        16: pl.Boolean,
        20: pl.Int64,
        21: pl.Int16,
        23: pl.Int32,
        700: pl.Float32,
        701: pl.Float64,
        17: pl.Binary,
        19: pl.String,
        25: pl.String,
        1042: pl.String,
        1043: pl.String,
        2950: pl.String,
        1082: pl.Date,
        1083: pl.Time,
        1114: pl.Datetime("us"),
        1184: pl.Datetime("us", "UTC"),
    }
    return simple.get(column.type_code)


def _strip_terminator(query: str) -> str:
    return query.rstrip().rstrip(";").rstrip()


class DenodoDialect(DbDialect):
    name: ClassVar[str] = "denodo"
    display_name: ClassVar[str] = "Denodo"
    default_port: ClassVar[int | None] = 9996
    supports_ssl: ClassVar[bool] = True
    sqlalchemy_driver: ClassVar[str | None] = "postgresql+psycopg2"
    sqlglot_name: ClassVar[str] = "postgres"
    install_hint: ClassVar[str | None] = "pip install psycopg2-binary"

    def is_available(self) -> bool:
        try:
            import psycopg2  # noqa: F401
        except ImportError:
            return False
        return True

    def libpq_dsn(self, uri: str) -> str:
        """``denodo://`` → ``postgresql://`` so libpq/psycopg2 accept the URI verbatim."""
        if uri.startswith(f"{self.uri_scheme}://"):
            return "postgresql://" + uri[len(self.uri_scheme) + 3 :]
        return uri

    def _connect(self, uri: str):
        conn = _import_psycopg2().connect(self.libpq_dsn(uri))
        conn.autocommit = True  # read-only session; no BEGIN/COMMIT round-trips
        return conn

    def _read_frame(self, uri: str, query: str) -> pl.DataFrame:
        import polars as pl

        conn = self._connect(uri)
        try:
            with conn.cursor() as cur:
                cur.execute(query)
                columns = list(cur.description or [])
                rows = cur.fetchall()
        finally:
            conn.close()
        names = [c.name for c in columns]
        overrides = {c.name: dtype for c in columns if (dtype := _polars_dtype(c)) is not None}
        return pl.DataFrame(rows, schema=names, schema_overrides=overrides, orient="row")

    def read(
        self,
        query: str,
        uri: str,
        logger: logging.Logger,
        cancel_check: Callable[[], bool] | None = None,
    ) -> pl.DataFrame:
        from shared.db_reader import run_cancellable_read

        return run_cancellable_read("denodo", lambda: self._read_frame(uri, query), cancel_check)

    def _describe(self, uri: str, query: str) -> list | None:
        """``cursor.description`` of a LIMIT-0 probe; None when the probe fails."""
        try:
            conn = self._connect(uri)
            try:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT * FROM ({_strip_terminator(query)}\n) AS _ff_q LIMIT 0")
                    return list(cur.description or [])
            finally:
                conn.close()
        except Exception as exc:
            logger.debug("Denodo LIMIT 0 schema probe failed: %s", exc)
            return None

    def query_schema(self, uri: str, query: str) -> pl.Schema | None:
        import polars as pl

        columns = self._describe(uri, query)
        if not columns:
            return None
        dtypes: dict[str, pl.DataType] = {}
        for column in columns:
            dtype = _polars_dtype(column)
            if dtype is None:
                return None
            dtypes[column.name] = dtype
        return pl.Schema(dtypes)

    def table_schema(self, uri: str, table_name: str, schema_name: str | None) -> pl.Schema | None:
        # Unquoted on purpose: mirrors the exact FROM clause SqlSource executes.
        qualified = f"{schema_name}.{table_name}" if schema_name else table_name
        return self.query_schema(uri, f"SELECT * FROM {qualified}")
