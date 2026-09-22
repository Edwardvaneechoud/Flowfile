import sys
from contextlib import contextmanager
from pathlib import Path

from sqlalchemy.orm import sessionmaker

from shared.database import get_catalog_engine, sqlite_database_path
from shared.storage_config import get_database_url, storage


def get_app_data_dir() -> Path:
    """Get the appropriate application data directory for the current platform."""
    return storage.database_directory


def get_database_path() -> Path | None:
    """Get the actual path to the database file (useful for backup/info purposes)."""
    return sqlite_database_path()


engine = get_catalog_engine()

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    """Dependency for FastAPI to get database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@contextmanager
def get_db_context():
    """Context manager for getting database session."""
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
