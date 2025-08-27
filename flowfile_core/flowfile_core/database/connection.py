from sqlalchemy import create_engine
from contextlib import contextmanager
from sqlalchemy.orm import sessionmaker
import os
import sys
from pathlib import Path
from flowfile_core.configs import logger
from shared.storage_config import storage


def get_app_data_dir() -> Path:
    """Get the appropriate application data directory for the current platform."""

    return storage.database_directory


def get_database_url():
    """Get the database URL based on the current environment."""
    if os.environ.get("TESTING") == "True":
        # Use a temporary test database
        test_db_path = storage.temp_directory / "test_flowfile.db"
        logger.debug(f"Using TESTING database URL: sqlite:///{test_db_path}")
        return f"sqlite:///{test_db_path}"

    custom_db_path = os.environ.get("FLOWFILE_DB_PATH")
    if custom_db_path:
        # logger.error("Using database URL:", os.environ.get("FLOWFILE_DB_URL"))
        return f"sqlite:///{custom_db_path}"
    # Use centralized location
    app_dir = get_app_data_dir()

    db_path = app_dir / "flowfile.db"
    logger.debug(f"Using database URL: sqlite:///{db_path}")
    return f"sqlite:///{db_path}"


def get_database_path() -> Path:
    """Get the actual path to the database file (useful for backup/info purposes)."""
    url = get_database_url()
    if url.startswith("sqlite:///"):
        return Path(url.replace("sqlite:///", ""))
    return None


# Create database engine
engine = create_engine(
    get_database_url(),
    connect_args={"check_same_thread": False} if "sqlite" in get_database_url() else {}
)

# Create session factory
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
        "platform": sys.platform
    }
