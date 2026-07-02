import os
import sys
import threading
from contextlib import contextmanager
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from shared.storage_config import get_database_url, storage


def get_app_data_dir() -> Path:
    """Get the appropriate application data directory for the current platform."""
    return storage.database_directory


def get_database_path() -> Path | None:
    """Get the actual path to the database file (useful for backup/info purposes)."""
    url = get_database_url()
    if url.startswith("sqlite:///"):
        return Path(url.replace("sqlite:///", ""))
    return None


engine = create_engine(
    get_database_url(), connect_args={"check_same_thread": False} if "sqlite" in get_database_url() else {}
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

_DB_INIT_LOCK = threading.Lock()
_db_initialized = False


def ensure_db_initialized() -> None:
    """Idempotently run the startup Alembic migration + seed default rows.

    Previously this ran as a side effect of ``import flowfile_core`` (via
    ``init_db.py``), which made importing the dataframe API drag in Alembic and
    create the catalog DB on disk. It is now deferred to first actual DB access
    (``get_db``/``get_db_context``) and to explicit server startup. Cheap on
    every call after the first. Honors ``FLOWFILE_SKIP_STARTUP_MIGRATION``.
    """
    global _db_initialized
    if _db_initialized:
        return
    with _DB_INIT_LOCK:
        if _db_initialized:
            return
        if not os.environ.get("FLOWFILE_SKIP_STARTUP_MIGRATION"):
            from flowfile_core.database.migration import run_startup_migration

            run_startup_migration()
        from flowfile_core.database.init_db import init_db

        init_db()
        _db_initialized = True


def get_db():
    """Dependency for FastAPI to get database session."""
    ensure_db_initialized()
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@contextmanager
def get_db_context():
    """Context manager for getting database session."""
    ensure_db_initialized()
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_database_info():
    """Get information about the current database configuration."""
    return {
        "url": get_database_url(),
        "path": str(get_database_path()) if get_database_path() else None,
        "app_data_dir": str(get_app_data_dir()),
        "platform": sys.platform,
    }
