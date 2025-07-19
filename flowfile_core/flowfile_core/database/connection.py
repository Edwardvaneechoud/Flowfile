from sqlalchemy import create_engine
from contextlib import contextmanager
from sqlalchemy.orm import sessionmaker
import os
import sys
from pathlib import Path
from flowfile_core.configs import logger


def get_app_data_dir() -> Path:
    """Get the appropriate application data directory for the current platform."""
    app_name = "Flowfile"

    if sys.platform == "win32":
        # Windows: C:\Users\{username}\AppData\Local\flowfile
        base_dir = os.environ.get("LOCALAPPDATA")
        if not base_dir:
            base_dir = os.path.join(os.path.expanduser("~"), "AppData", "Local")
    elif sys.platform == "darwin":
        # macOS: ~/Library/Application Support/flowfile
        base_dir = os.path.join(os.path.expanduser("~"), "Library", "Application Support")
    else:
        # Linux: ~/.local/share/flowfile or use XDG_DATA_HOME
        base_dir = os.environ.get("XDG_DATA_HOME")
        if not base_dir:
            base_dir = os.path.join(os.path.expanduser("~"), ".local", "share")

    app_dir = Path(base_dir) / app_name

    print(f"Using application data directory: {app_dir}")
    app_dir.mkdir(parents=True, exist_ok=True)

    return app_dir


def get_database_url():
    """Get the database URL based on the current environment."""
    if os.environ.get("TESTING") == "True":
        # Use a temporary test database
        test_db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../test_flowfile.db")
        return f"sqlite:///{test_db_path}"

    custom_db_path = os.environ.get("FLOWFILE_DB_PATH")
    if custom_db_path:
        # logger.error("Using database URL:", os.environ.get("FLOWFILE_DB_URL"))
        return f"sqlite:///{custom_db_path}"
    # Use centralized location
    app_dir = get_app_data_dir()

    db_path = app_dir / "flowfile.db"
    logger.info(f"Using database URL: sqlite:///{db_path}")
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