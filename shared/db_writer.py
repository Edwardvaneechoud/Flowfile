"""Pandas-free Polars-DataFrame -> SQL writer, shared by the worker and core.

SQLite uses stdlib ``sqlite3``; other backends use SQLAlchemy Core.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator

import polars as pl

from shared.sql_utils import get_sqlalchemy_uri


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _split_identifier(table_name: str) -> tuple[str | None, str]:
    if "." in table_name:
        schema, _, table = table_name.partition(".")
        return schema, table
    return None, table_name


def _sqlite_path_from_uri(uri: str) -> str:
    prefix = "sqlite:///"
    return uri[len(prefix) :] if uri.startswith(prefix) else uri


def _coerce_for_sqlite(df: pl.DataFrame) -> pl.DataFrame:
    casts = []
    for name, dtype in df.schema.items():
        if isinstance(dtype, pl.Decimal):
            casts.append(pl.col(name).cast(pl.Float64))
        elif dtype.is_temporal() or isinstance(dtype, pl.Categorical | pl.Enum):
            casts.append(pl.col(name).cast(pl.Utf8))
    return df.with_columns(casts) if casts else df


def _sqlite_affinity(dtype: pl.DataType) -> str:
    if dtype == pl.Boolean or dtype.is_integer():
        return "INTEGER"
    if dtype.is_float():
        return "REAL"
    if dtype == pl.Binary:
        return "BLOB"
    return "TEXT"


def _sqlite_table_exists(conn: sqlite3.Connection, schema: str | None, table: str) -> bool:
    master = "sqlite_master"
    if schema and schema.lower() not in ("main", "temp"):
        master = f"{_quote_ident(schema)}.sqlite_master"
    cur = conn.execute(f"SELECT 1 FROM {master} WHERE type='table' AND name=?", (table,))
    return cur.fetchone() is not None


def _iter_sqlite_rows(df: pl.DataFrame, nested_idx: list[int]) -> Iterator[tuple]:
    if not nested_idx:
        yield from df.iter_rows()
        return
    nested = set(nested_idx)
    for row in df.iter_rows():
        yield tuple(
            json.dumps(value, default=str) if i in nested and value is not None else value
            for i, value in enumerate(row)
        )


def _write_df_to_sqlite(df: pl.DataFrame, uri: str, table_name: str, if_exists: str) -> None:
    path = _sqlite_path_from_uri(uri)
    schema, table = _split_identifier(table_name)
    qualified = f"{_quote_ident(schema)}.{_quote_ident(table)}" if schema else _quote_ident(table)

    df = _coerce_for_sqlite(df)
    nested_idx = [i for i, dtype in enumerate(df.dtypes) if dtype.is_nested()]
    column_defs = ", ".join(f"{_quote_ident(name)} {_sqlite_affinity(dtype)}" for name, dtype in df.schema.items())
    column_list = ", ".join(_quote_ident(name) for name in df.columns)
    placeholders = ", ".join(["?"] * df.width)

    conn = sqlite3.connect(path)
    try:
        if if_exists == "fail" and _sqlite_table_exists(conn, schema, table):
            raise ValueError(f"Table '{table_name}' already exists")
        if if_exists == "replace":
            conn.execute(f"DROP TABLE IF EXISTS {qualified}")
        create = "CREATE TABLE IF NOT EXISTS" if if_exists == "append" else "CREATE TABLE"
        conn.execute(f"{create} {qualified} ({column_defs})")
        conn.executemany(
            f"INSERT INTO {qualified} ({column_list}) VALUES ({placeholders})",
            _iter_sqlite_rows(df, nested_idx),
        )
        conn.commit()
    finally:
        conn.close()


def _sqlalchemy_column_type(sa, dtype: pl.DataType):
    if dtype == pl.Boolean:
        return sa.Boolean()
    if dtype.is_integer():
        return sa.BigInteger()
    if dtype.is_float():
        return sa.Float()
    if isinstance(dtype, pl.Decimal):
        return sa.Numeric(dtype.precision or 38, dtype.scale or 0)
    if isinstance(dtype, pl.Datetime):
        return sa.DateTime()
    if dtype == pl.Date:
        return sa.Date()
    if dtype == pl.Time:
        return sa.Time()
    if dtype == pl.Binary:
        return sa.LargeBinary()
    return sa.Text()


def _text_encoders(df: pl.DataFrame) -> dict:
    encoders = {}
    for name, dtype in df.schema.items():
        if dtype.is_nested():
            encoders[name] = lambda value: json.dumps(value, default=str)
        elif isinstance(dtype, pl.Duration):
            encoders[name] = str
    return encoders


def _write_df_via_sqlalchemy_core(df: pl.DataFrame, uri: str, table_name: str, if_exists: str) -> None:
    import sqlalchemy as sa

    schema, table = _split_identifier(table_name)
    engine = sa.create_engine(get_sqlalchemy_uri(uri))
    try:
        columns = [sa.Column(name, _sqlalchemy_column_type(sa, dtype)) for name, dtype in df.schema.items()]
        sql_table = sa.Table(table, sa.MetaData(), *columns, schema=schema)
        exists = sa.inspect(engine).has_table(table, schema=schema)
        if if_exists == "fail" and exists:
            raise ValueError(f"Table '{table_name}' already exists")
        encoders = _text_encoders(df)
        with engine.begin() as conn:
            if if_exists == "replace" and exists:
                sql_table.drop(conn)
            sql_table.create(conn, checkfirst=(if_exists == "append"))
            for batch in df.iter_slices(50_000):
                rows = batch.to_dicts()
                for row in rows:
                    for name, encode in encoders.items():
                        if row[name] is not None:
                            row[name] = encode(row[name])
                if rows:
                    conn.execute(sql_table.insert(), rows)
    finally:
        engine.dispose()


def write_dataframe_to_database(
    df: pl.DataFrame,
    *,
    database_type: str,
    uri: str,
    table_name: str,
    if_exists: str = "append",
) -> None:
    """Write ``df`` to ``table_name``. ``uri`` is a base URI (``sqlite:///<path>`` or
    a base scheme; the SQLAlchemy driver suffix is applied internally)."""
    if_exists = if_exists or "append"
    if database_type.lower() == "sqlite":
        _write_df_to_sqlite(df, uri, table_name, if_exists)
    else:
        _write_df_via_sqlalchemy_core(df, uri, table_name, if_exists)
