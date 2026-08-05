"""SQL Server dialect: pymssql-backed reads, sp_describe fast schema, TOP-based limits.

Reads deliberately skip connectorx: its mssql backend (tiberius) segfaults when
successive reads run on fresh threads — exactly the hedged reader's
attempt-per-read model (reproduced with connectorx 0.4.5: first threaded read
succeeds, the second SIGSEGVs the process). Reads therefore go through the
SQLAlchemy/pymssql leg only (``shared.db_reader.read_sql_sqlalchemy``), which
keeps the abandonable-thread/cancel semantics. Writes inherit the base
SQLAlchemy Core insert path.

Fast schema comes from ``sp_describe_first_result_set`` — SQL Server plans the
query without executing it and reports every result column's T-SQL type — and a
dialect-local type map. ``read`` applies the same map as ``schema_overrides``
and projects the types pymssql yields as unusable Python objects
(``uniqueidentifier`` → ``uuid.UUID`` becomes an Object column, etc.) to
``NVARCHAR``, so predicted schema equals materialized schema by construction.
A column whose type is not in the map makes the schema hooks return ``None``
(the caller keeps its generic path) while ``read`` still returns whatever
pymssql yields for it.

T-SQL has no ``LIMIT`` clause, so ``limit_query`` injects ``TOP n`` after the
leading ``SELECT [ALL|DISTINCT]`` (case-insensitive: callers pass lowercase
``select * from (...) as main_query`` in query mode). A non-SELECT head falls
back to a derived-table wrap so the limit is never silently dropped.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable
from typing import TYPE_CHECKING, ClassVar

from shared.db_dialects.base import DbDialect

if TYPE_CHECKING:
    import polars as pl

logger = logging.getLogger(__name__)

_SELECT_HEAD = re.compile(r"^\s*select\b(\s+distinct\b|\s+all\b)?", re.IGNORECASE)
_TYPE_ARGS = re.compile(r"^([a-z0-9_ ]+?)\s*(?:\((.*)\))?$")

# Types pymssql returns as Python objects Polars can only hold as Object dtype;
# projected to NVARCHAR in the read path so frames stay Object-free.
_STRINGIFY_TYPES = {"uniqueidentifier", "datetimeoffset", "hierarchyid", "sql_variant"}


def _quote_ident(name: str) -> str:
    return "[" + name.replace("]", "]]") + "]"


def _split_type(system_type_name: str) -> tuple[str, str | None]:
    """``'decimal(3,1)'`` → ``('decimal', '3,1')``; unparseable names → ``('', None)``."""
    match = _TYPE_ARGS.match(system_type_name.strip().lower())
    if not match:
        return "", None
    return match.group(1).strip(), match.group(2)


class MSSQLDialect(DbDialect):
    name: ClassVar[str] = "mssql"
    display_name: ClassVar[str] = "SQL Server"
    default_port: ClassVar[int | None] = 1433
    sqlalchemy_driver: ClassVar[str | None] = "mssql+pymssql"
    sqlglot_name: ClassVar[str] = "tsql"
    install_hint: ClassVar[str | None] = "pip install pymssql"

    def is_available(self) -> bool:
        try:
            import pymssql  # noqa: F401
        except ImportError:
            return False
        return True

    def limit_query(self, select_sql: str, n: int) -> str:
        match = _SELECT_HEAD.match(select_sql)
        if match:
            return f"{select_sql[: match.end()]} TOP {n}{select_sql[match.end() :]}"
        return f"SELECT TOP {n} * FROM ({select_sql}) AS _ff_limit"

    @staticmethod
    def _polars_dtype(system_type_name: str):
        """Map an sp_describe system_type_name (e.g. ``decimal(3,1)``) to a Polars dtype.

        Follows core's sql_type_name_to_polars conventions, except ``timestamp``:
        in T-SQL that is rowversion (binary), not a datetime.
        """
        import polars as pl

        base, args = _split_type(system_type_name)
        if base in _STRINGIFY_TYPES:
            return pl.String
        if base in ("decimal", "numeric"):
            try:
                precision, scale = (int(a) for a in args.split(","))
            except (AttributeError, ValueError):
                return None
            return pl.Decimal(precision, scale)
        simple = {
            "bigint": pl.Int64,
            "int": pl.Int32,
            "smallint": pl.Int16,
            "tinyint": pl.Int8,
            "bit": pl.Boolean,
            "float": pl.Float64,
            "real": pl.Float32,
            "money": pl.Decimal(19, 4),
            "smallmoney": pl.Decimal(10, 4),
            "char": pl.String,
            "nchar": pl.String,
            "varchar": pl.String,
            "nvarchar": pl.String,
            "text": pl.String,
            "ntext": pl.String,
            "xml": pl.String,
            "date": pl.Date,
            "time": pl.Time,
            "datetime": pl.Datetime("us"),
            "datetime2": pl.Datetime("us"),
            "smalldatetime": pl.Datetime("us"),
            "binary": pl.Binary,
            "varbinary": pl.Binary,
            "image": pl.Binary,
            "timestamp": pl.Binary,
            "rowversion": pl.Binary,
        }
        return simple.get(base)

    def _describe_columns(self, uri: str, query: str) -> list[tuple[str, str]] | None:
        """Result columns as (name, system_type_name) without executing; None if undescribable."""
        from sqlalchemy import create_engine, text

        try:
            engine = create_engine(self.sqlalchemy_uri(uri))
            try:
                with engine.connect() as conn:
                    rows = conn.execute(text("EXEC sp_describe_first_result_set :q"), {"q": query}).fetchall()
            finally:
                engine.dispose()
        except Exception as exc:
            logger.debug("sp_describe_first_result_set failed: %s", exc)
            return None
        # row: is_hidden(0), column_ordinal(1), name(2), is_nullable(3), system_type_id(4), system_type_name(5)
        return [(row[2], row[5]) for row in rows if not row[0]]

    def _object_safe_query(self, query: str, columns: list[tuple[str, str]]) -> str:
        """Project Object-yielding columns to NVARCHAR; untouched when none are present."""
        if not any(_split_type(t)[0] in _STRINGIFY_TYPES for _, t in columns):
            return query
        if any(not name for name, _ in columns):
            return query
        select_list = ", ".join(
            f"CAST({_quote_ident(name)} AS NVARCHAR(MAX)) AS {_quote_ident(name)}"
            if _split_type(type_name)[0] in _STRINGIFY_TYPES
            else _quote_ident(name)
            for name, type_name in columns
        )
        return f"SELECT {select_list} FROM ({query.rstrip().rstrip(';').rstrip()}\n) AS _ff_q"

    def read(
        self,
        query: str,
        uri: str,
        logger: logging.Logger,
        cancel_check: Callable[[], bool] | None = None,
    ) -> pl.DataFrame:
        from shared.db_reader import read_sql_sqlalchemy

        columns = self._describe_columns(uri, query)
        overrides = None
        if columns:
            query = self._object_safe_query(query, columns)
            overrides = {
                name: dtype
                for name, type_name in columns
                if name and (dtype := self._polars_dtype(type_name)) is not None
            }
        return read_sql_sqlalchemy(query, uri, logger, cancel_check=cancel_check, schema_overrides=overrides)

    def query_schema(self, uri: str, query: str) -> pl.Schema | None:
        import polars as pl

        columns = self._describe_columns(uri, query)
        if not columns:
            return None
        dtypes: dict[str, pl.DataType] = {}
        for name, type_name in columns:
            dtype = self._polars_dtype(type_name) if name else None
            if dtype is None:
                return None
            dtypes[name] = dtype
        return pl.Schema(dtypes)

    def table_schema(self, uri: str, table_name: str, schema_name: str | None) -> pl.Schema | None:
        parts = [p for p in ([schema_name] if schema_name else []) + table_name.split(".") if p]
        qualified = ".".join(_quote_ident(p) for p in parts)
        return self.query_schema(uri, f"SELECT * FROM {qualified}")
