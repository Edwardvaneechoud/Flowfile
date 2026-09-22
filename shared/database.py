"""Portable engine and local-file resolution for the catalog database."""

import threading
from pathlib import Path

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine, make_url

from shared.storage_config import get_database_url

SQLITE_POOL_ARGS = {"pool_size": 20, "max_overflow": 30, "pool_timeout": 5}

_engines: dict[str, Engine] = {}
_engines_lock = threading.Lock()


def _enable_wal(dbapi_connection, _record) -> None:
    dbapi_connection.execute("PRAGMA journal_mode=WAL")


def create_catalog_engine(url: str | None = None) -> Engine:
    """Create a catalog engine with driver-specific connection options.

    A file-backed SQLite catalog gets a wide, fail-fast pool (``SQLITE_POOL_ARGS``)
    and WAL journaling so readers never queue behind a writer. In-memory SQLite keeps
    SQLAlchemy's single-connection pool, which rejects queue-pool sizing.
    """
    parsed = make_url(url or get_database_url())
    is_sqlite = parsed.get_backend_name() == "sqlite"
    connect_args = {"check_same_thread": False} if is_sqlite else {}
    file_sqlite = is_sqlite and sqlite_database_path(parsed) is not None
    pool_args = SQLITE_POOL_ARGS if file_sqlite else {}
    engine = create_engine(parsed, connect_args=connect_args, pool_pre_ping=not is_sqlite, **pool_args)
    if file_sqlite:
        event.listen(engine, "connect", _enable_wal)
    return engine


def get_catalog_engine(url: str | None = None) -> Engine:
    """Return the process-wide engine for *url* (default: the configured catalog).

    Ad-hoc callers such as run completion and notification draining used to build a
    throwaway engine per call and never dispose it, leaking a pooled connection each
    time; sharing one engine per URL keeps every caller on the same pool.
    """
    key = make_url(url or get_database_url()).render_as_string(hide_password=False)
    with _engines_lock:
        engine = _engines.get(key)
        if engine is None:
            engine = _engines[key] = create_catalog_engine(key)
        return engine


def sqlite_database_path(url: str | None = None) -> Path | None:
    """Return a file path only for an on-disk SQLite catalog."""
    parsed = make_url(url or get_database_url())
    if parsed.get_backend_name() != "sqlite" or parsed.database in (None, "", ":memory:"):
        return None
    if parsed.query.get("mode") == "memory":
        return None
    return Path(parsed.database)
