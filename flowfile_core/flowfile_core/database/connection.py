
from sqlalchemy import create_engine
from contextlib import contextmanager
from sqlalchemy.orm import sessionmaker
import os


def get_database_url():
    if os.environ.get("TESTING") == "True":
        # Use a file-based test database instead of in-memory
        test_db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../test_flowfile.db")
        return f"sqlite:///{test_db_path}"
    # elif os.environ.get("FLOWFILE_MODE") == "electron":

    return "sqlite:///./flowfile.db"

    # else:
    #     # Use PostgreSQL for Docker mode
    #     host = os.environ.get("DB_HOST", "localhost")
    #     port = os.environ.get("DB_PORT", "5432")
    #     user = os.environ.get("DB_USER", "postgres")
    #     password = os.environ.get("DB_PASSWORD", "postgres")
    #     db = os.environ.get("DB_NAME", "flowfile")
    #     return f"postgresql://{user}:{password}@{host}:{port}/{db}"
    #

# Create database engine
engine = create_engine(
    get_database_url(),
    connect_args={"check_same_thread": False} if os.environ.get("FLOWFILE_MODE") == "electron" else {}
)

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@contextmanager
def get_db_context():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()