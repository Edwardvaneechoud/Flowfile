# conftest.py
import logging
import os
import socket
import sys

import pytest

os.environ['TEST_MODE'] = '1'

from tests.utils import is_docker_available

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))
from test_utils.postgres import fixtures as pg_fixtures


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
