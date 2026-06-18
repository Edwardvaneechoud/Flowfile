import json
import socket
import sqlite3
from collections.abc import Iterator
from io import BytesIO

import polars as pl

from flowfile_worker.external_sources.sql_source.models import (
    DataBaseConnection,
    DatabaseReadSettings,
    DatabaseWriteSettings,
)
from flowfile_worker.flow_logger import get_worker_logger
from shared.db_reader import read_sql_with_fallback

# Default ports per database type for the pre-flight connectivity check.
_DEFAULT_DB_PORTS = {
    "postgresql": 5432,
    "postgres": 5432,
    "redshift": 5439,
    "mysql": 3306,
    "mariadb": 3306,
    "mssql": 1433,
    "sqlserver": 1433,
    "oracle": 1521,
}


def verify_database_reachable(connection: DataBaseConnection, timeout: float = 3.0) -> None:
    """Fail fast if the database host:port can't be reached.

    connectorx/r2d2 retries a refused or unreachable connection for ~30s before
    giving up; a short TCP pre-check surfaces the failure immediately. Best-effort:
    skips file-based (sqlite) and url-based connections where host/port is unknown.
    """
    if connection.url or connection.database_type.lower() == "sqlite":
        return
    host = connection.host
    port = connection.port or _DEFAULT_DB_PORTS.get(connection.database_type.lower())
    if not host or not port:
        return
    try:
        with socket.create_connection((host, port), timeout=timeout):
            pass
    except OSError as e:
        raise ConnectionError(f"Cannot connect to database at {host}:{port}: {e}") from e


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
    # SQLite has no temporal/decimal/categorical types; store them as ISO text / floats.
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


def _write_df_to_sqlite(df: pl.DataFrame, settings: DatabaseWriteSettings) -> None:
    path = _sqlite_path_from_uri(settings.connection.create_uri())
    schema, table = _split_identifier(settings.table_name)
    qualified = f"{_quote_ident(schema)}.{_quote_ident(table)}" if schema else _quote_ident(table)
    if_exists = settings.if_exists or "append"

    df = _coerce_for_sqlite(df)
    nested_idx = [i for i, dtype in enumerate(df.dtypes) if dtype.is_nested()]
    column_defs = ", ".join(f"{_quote_ident(name)} {_sqlite_affinity(dtype)}" for name, dtype in df.schema.items())
    column_list = ", ".join(_quote_ident(name) for name in df.columns)
    placeholders = ", ".join(["?"] * df.width)

    conn = sqlite3.connect(path)
    try:
        if if_exists == "fail" and _sqlite_table_exists(conn, schema, table):
            raise ValueError(f"Table '{settings.table_name}' already exists")
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


def _write_df_via_sqlalchemy_core(df: pl.DataFrame, settings: DatabaseWriteSettings) -> None:
    import sqlalchemy as sa

    schema, table = _split_identifier(settings.table_name)
    if_exists = settings.if_exists or "append"

    engine = sa.create_engine(settings.connection.create_sqlalchemy_uri())
    try:
        columns = [sa.Column(name, _sqlalchemy_column_type(sa, dtype)) for name, dtype in df.schema.items()]
        sql_table = sa.Table(table, sa.MetaData(), *columns, schema=schema)
        exists = sa.inspect(engine).has_table(table, schema=schema)
        if if_exists == "fail" and exists:
            raise ValueError(f"Table '{settings.table_name}' already exists")
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


def write_df_to_database(df: pl.DataFrame, database_write_settings: DatabaseWriteSettings):
    """
    Writes a Polars DataFrame to a SQL database without requiring pandas.

    SQLite uses the stdlib ``sqlite3`` driver; other backends go through SQLAlchemy Core.
    Args:
        df (pl.DataFrame): The DataFrame to write.
        database_write_settings (DatabaseWriteSettings): The settings for the database connection and table.
    """
    if database_write_settings.connection.database_type.lower() == "sqlite":
        _write_df_to_sqlite(df, database_write_settings)
    else:
        _write_df_via_sqlalchemy_core(df, database_write_settings)
    return True


def write_serialized_df_to_database(serialized_df: bytes, database_write_settings: DatabaseWriteSettings):
    """
    Writes a Polars DataFrame to a SQL database.
    Args:
        serialized_df (bytes): The serialized Polars DataFrame to write.
        database_write_settings (DatabaseWriteSettings): The settings for the database connection and table.
    """
    df = pl.LazyFrame.deserialize(BytesIO(serialized_df)).collect()
    write_df_to_database(df, database_write_settings)
    return True


def read_query_as_pd_df(query: str, uri: str) -> pl.DataFrame:
    """
    Reads a URI into a Polars DataFrame.
    Args:
        query (str): The SQL query to execute.
        uri (str): The URI to read.
    Returns:
        pl.DataFrame: The resulting Polars DataFrame.
    """
    return pl.read_database_uri(query, uri)


def read_sql_source(database_read_settings: DatabaseReadSettings):
    """
    Connects to a database and executes a query to retrieve data.
    Args:
        database_read_settings (SQLSourceSettings): The SQL source settings containing connection details and query.
    Returns:
        pl.DataFrame: The resulting Polars DataFrame.
    """
    logger = get_worker_logger(
        database_read_settings.flowfile_flow_id, database_read_settings.flowfile_node_id
    )
    verify_database_reachable(database_read_settings.connection)
    return read_sql_with_fallback(
        database_read_settings.query, database_read_settings.connection.create_uri(), logger
    )
