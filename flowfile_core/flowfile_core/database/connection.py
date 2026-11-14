import sys
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path

from sqlalchemy import event
from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session, sessionmaker

from shared.database import create_catalog_engine, get_catalog_engine, sqlite_database_path
from shared.storage_config import get_database_url, storage


def get_app_data_dir() -> Path:
    """Get the appropriate application data directory for the current platform."""
    return storage.database_directory


def get_database_path() -> Path | None:
    """Get the actual path to the database file (useful for backup/info purposes)."""
    return sqlite_database_path()


engine = get_catalog_engine()

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

_apply_connection: ContextVar[Connection | None] = ContextVar("_apply_connection", default=None)


def get_db():
    """Dependency for FastAPI to get database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@contextmanager
def get_db_context():
    """Context manager for getting database session.

    Inside ``apply_transaction`` the session joins the apply's connection, so its ``commit()`` only
    releases a savepoint and its ``rollback()`` only undoes that savepoint.
    """
    connection = _apply_connection.get()
    if connection is None:
        db = SessionLocal()
    else:
        db = Session(bind=connection, join_transaction_mode="create_savepoint", autoflush=False)
    try:
        yield db
    finally:
        db.close()


def _sqlite_connect_for_apply(dbapi_connection, _record) -> None:
    dbapi_connection.isolation_level = None
    dbapi_connection.execute("PRAGMA journal_mode=WAL")


def _sqlite_begin_immediate(connection) -> None:
    connection.exec_driver_sql("BEGIN IMMEDIATE")


@contextmanager
def apply_transaction():
    """Hold one database transaction for the block: it commits once when the block ends, or rolls back.

    Sessions from ``get_db_context`` in this context join it as savepoints. It runs on an engine of its
    own so the app's engine keeps its settings. On SQLite the driver's own transaction handling is
    switched off so savepoints work, and ``BEGIN IMMEDIATE`` takes the write lock at the start: a
    deferred ``BEGIN`` could find, at its first write, that another writer moved the snapshot it read.
    """
    apply_engine = create_catalog_engine(engine.url.render_as_string(hide_password=False))
    if sqlite_database_path(apply_engine.url) is not None:
        event.listen(apply_engine, "connect", _sqlite_connect_for_apply)
        event.listen(apply_engine, "begin", _sqlite_begin_immediate)
    try:
        with apply_engine.connect() as connection, connection.begin():
            token = _apply_connection.set(connection)
            try:
                yield
            finally:
                _apply_connection.reset(token)
    finally:
        apply_engine.dispose()


def get_database_info():
    """Get information about the current database configuration."""
    return {
        "url": get_database_url(),
        "path": str(get_database_path()) if get_database_path() else None,
        "app_data_dir": str(get_app_data_dir()),
        "platform": sys.platform,
    }
