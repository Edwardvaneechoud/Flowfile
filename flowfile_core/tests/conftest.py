# conftest.py
import logging
import os
import sys

# Patch bcrypt for passlib 1.7.4 / bcrypt 5.0.0+ compatibility
import bcrypt
_original_hashpw = bcrypt.hashpw
def _patched_hashpw(password, salt):
    if isinstance(password, bytes) and len(password) > 72:
        password = password[:72]
    return _original_hashpw(password, salt)
bcrypt.hashpw = _patched_hashpw

import pytest

os.environ['TESTING'] = 'True'
# Disable worker offloading for core tests. These tests run locally without a worker.
os.environ['FLOWFILE_OFFLOAD_TO_WORKER'] = '0'

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))
import socket

from test_utils.postgres import fixtures as pg_fixtures
from tests.flowfile_core_test_utils import is_docker_available


def is_port_in_use(port, host='localhost'):
    """Check if a port is in use on the specified host."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.connect((host, port))
            return True
        except ConnectionRefusedError:
            return False


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger("flowfile_fixture")


@pytest.fixture(scope="session", autouse=True)
def setup_test_db():
    """Setup the test database and clean up after tests"""
    # Just use your existing init_db function to create tables and set up the database
    from flowfile_core.database.connection import engine, get_database_url
    from flowfile_core.database.init_db import init_db
    from flowfile_core.database.models import Base

    init_db()

    yield

    # Cleanup after all tests
    if os.environ.get("TESTING") == "True" and "sqlite" in get_database_url():
        logger.info(f"Trying to cleanup: {get_database_url()}")
        try:
            # Drop all tables
            Base.metadata.drop_all(bind=engine)

            # If using file-based SQLite, remove the file
            db_path = get_database_url().replace("sqlite:///", "")
            if db_path != ":memory:" and os.path.exists(db_path):
                os.remove(db_path)
                logger.info("Removed test database file")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")


@pytest.fixture(scope="session", autouse=True)
def postgres_db():
    """
    Pytest fixture that ensures PostgreSQL container is running for the test session.
    Automatically starts and stops a PostgreSQL container with sample data.
    """
    if is_port_in_use(5433) or pg_fixtures.can_connect_to_db():
        print("PostgreSQL is already running on port 5433, skipping container creation")
        yield
        return

    elif not is_docker_available():
        print("Docker is not available, skipping PostgreSQL container creation")
        yield
        return

    with pg_fixtures.managed_postgres() as db_info:
        if not db_info:
            pytest.fail("PostgreSQL container could not be started")
        yield db_info
