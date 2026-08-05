"""Database dialect registry.

Single source of truth for which database types Flowfile supports and how each
one behaves (URI building, read/write strategy, fast-schema hooks, catalog
metadata). Core, the worker, and flowfile_frame all resolve dialects here;
the frontend consumes ``dialect_catalog()`` via ``GET /db_dialects``.

Adding a dialect = one module with a ``DbDialect`` subclass + one entry in
``_BUILTIN_DIALECTS`` below (+ tests); the contract suite in
shared/tests/db_dialects/test_dialect_contract.py runs for every entry.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING

from pydantic import BaseModel

from shared.db_dialects.base import POSTGRES_FAMILY, DbDialect, DialectField, is_blocked_extra_param
from shared.db_dialects.builtin import (
    GenericDialect,
    MySQLDialect,
    PostgresDialect,
    SQLiteDialect,
)
from shared.db_dialects.duckdb import DuckDBDialect
from shared.db_dialects.mssql import MSSQLDialect
from shared.db_dialects.snowflake import SnowflakeDialect

if TYPE_CHECKING:
    import polars as pl

__all__ = [
    "POSTGRES_FAMILY",
    "DbDialect",
    "DialectField",
    "DialectFieldInfo",
    "DialectInfo",
    "DuckDBDialect",
    "GenericDialect",
    "KNOWN_DIALECT_NAMES",
    "MSSQLDialect",
    "SnowflakeDialect",
    "UnknownDialectError",
    "dialect_catalog",
    "get_dialect",
    "get_dialect_or_generic",
    "is_blocked_extra_param",
    "iter_dialects",
    "read_sql",
]


class UnknownDialectError(ValueError):
    """Raised when a database_type is not in the dialect registry."""


_BUILTIN_DIALECTS: tuple[DbDialect, ...] = (
    PostgresDialect(),
    MySQLDialect(),
    SQLiteDialect(),
    DuckDBDialect(),
    MSSQLDialect(),
    SnowflakeDialect(),
)

_REGISTRY: dict[str, DbDialect] = {d.name: d for d in _BUILTIN_DIALECTS}

KNOWN_DIALECT_NAMES: tuple[str, ...] = tuple(_REGISTRY)


class DialectFieldInfo(BaseModel):
    """A dialect-specific connection field the frontend renders into the form."""

    name: str
    label: str
    required: bool = False


class DialectInfo(BaseModel):
    """Catalog entry served to the frontend via GET /db_dialects."""

    name: str
    display_name: str
    file_based: bool
    default_port: int | None
    supports_ssl: bool
    available: bool
    extra_fields: list[DialectFieldInfo] = []
    hidden_fields: list[str] = []
    auth_methods: list[str] = ["password"]


def get_dialect(name: str) -> DbDialect:
    """Return the registered dialect for ``name`` or raise ``UnknownDialectError``."""
    dialect = _REGISTRY.get(name.lower())
    if dialect is None:
        raise UnknownDialectError(
            f"Unsupported database type {name!r}; supported types: {', '.join(KNOWN_DIALECT_NAMES)}"
        )
    return dialect


def get_dialect_or_generic(name: str) -> DbDialect:
    """Like ``get_dialect`` but unknown names get a ``GenericDialect`` compat shim.

    Stored connections predate the registry and ``database_type`` is a free
    string column, so values like ``redshift`` must keep building the same URIs
    they always did. Vocabulary enforcement happens at validation/UI boundaries
    (``KNOWN_DIALECT_NAMES``), never here.
    """
    return _REGISTRY.get(name.lower()) or GenericDialect(name)


def iter_dialects() -> tuple[DbDialect, ...]:
    return tuple(_REGISTRY.values())


def dialect_catalog() -> list[DialectInfo]:
    return [
        DialectInfo(
            name=d.name,
            display_name=d.display_name,
            file_based=d.file_based,
            default_port=d.default_port,
            supports_ssl=d.supports_ssl,
            available=d.is_available(),
            extra_fields=[DialectFieldInfo(name=f.name, label=f.label, required=f.required) for f in d.extra_fields],
            hidden_fields=list(d.hidden_fields),
            auth_methods=list(d.auth_methods),
        )
        for d in iter_dialects()
    ]


def _dialect_for_uri(uri: str) -> DbDialect:
    scheme = uri.split("://", 1)[0].split("+", 1)[0]
    return get_dialect_or_generic(scheme)


def read_sql(
    query: str,
    uri: str,
    logger: logging.Logger,
    *,
    database_type: str | None = None,
    cancel_check: Callable[[], bool] | None = None,
) -> pl.DataFrame:
    """Read ``query`` via the dialect's read strategy (resolved by type, else URI scheme)."""
    dialect = get_dialect_or_generic(database_type) if database_type else _dialect_for_uri(uri)
    return dialect.read(query, uri, logger, cancel_check=cancel_check)
