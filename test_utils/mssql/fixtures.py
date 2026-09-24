"""
SQL Server fixtures for tests.

This module provides utilities to set up, manage, and tear down SQL Server
containers with sample data for testing.

Unlike the mysql image, mcr.microsoft.com/mssql/server has no env-based
database/user provisioning: the container boots with only the ``sa`` login, so
``_init_sample_data`` connects as ``sa`` to create the test database, the
``testuser`` login (``CHECK_POLICY = OFF`` so the repo-wide ``testpass``
convention works), and the seeded sample tables. The image is amd64-only;
Apple Silicon runs it via Docker Desktop's Rosetta emulation.
"""

import logging
import os
import shutil
import subprocess
import time
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger("mssql_fixture")

# Configuration constants
MSSQL_HOST = os.environ.get("TEST_MSSQL_HOST", "localhost")
MSSQL_PORT = int(os.environ.get("TEST_MSSQL_PORT", 1434))
MSSQL_USER = os.environ.get("TEST_MSSQL_USER", "testuser")
MSSQL_PASSWORD = os.environ.get("TEST_MSSQL_PASSWORD", "testpass")
MSSQL_SA_PASSWORD = os.environ.get("TEST_MSSQL_SA_PASSWORD", "SaPassw0rd!")
MSSQL_DB = os.environ.get("TEST_MSSQL_DB", "testdb")
MSSQL_CONTAINER_NAME = os.environ.get("TEST_MSSQL_CONTAINER", "test-mssql-sample")
MSSQL_IMAGE = os.environ.get("TEST_MSSQL_IMAGE", "mcr.microsoft.com/mssql/server:2022-latest")
STARTUP_TIMEOUT = int(os.environ.get("TEST_MSSQL_STARTUP_TIMEOUT", 120))  # SQL Server boots slowly
SHUTDOWN_TIMEOUT = int(os.environ.get("TEST_MSSQL_SHUTDOWN_TIMEOUT", 15))  # seconds
STARTUP_CHECK_INTERVAL = 3  # seconds

# Operating system detection
IS_MACOS = os.uname().sysname == "Darwin" if hasattr(os, "uname") else False
IS_WINDOWS = os.name == "nt"

# Statements are executed one by one (T-SQL IF guards contain semicolons, so a
# split-on-";" blob would break). All guards make re-seeding a no-op.
PROVISION_SQL = [
    f"IF DB_ID('{MSSQL_DB}') IS NULL CREATE DATABASE [{MSSQL_DB}]",
    (
        f"IF SUSER_ID('{MSSQL_USER}') IS NULL "
        f"CREATE LOGIN [{MSSQL_USER}] WITH PASSWORD = '{MSSQL_PASSWORD}', CHECK_POLICY = OFF"
    ),
]

PROVISION_DB_SQL = [
    f"IF USER_ID('{MSSQL_USER}') IS NULL CREATE USER [{MSSQL_USER}] FOR LOGIN [{MSSQL_USER}]",
    f"ALTER ROLE db_owner ADD MEMBER [{MSSQL_USER}]",
]

SAMPLE_DATA_SQL = [
    """
    IF OBJECT_ID('movies', 'U') IS NULL
    CREATE TABLE movies (
        id INT PRIMARY KEY,
        title NVARCHAR(255) NOT NULL,
        release_year SMALLINT,
        genre NVARCHAR(20),
        rating DECIMAL(3, 1),
        votes INT,
        director NVARCHAR(255),
        duration SMALLINT,
        is_active BIT DEFAULT 1,
        metadata NVARCHAR(MAX),
        row_guid UNIQUEIDENTIFIER DEFAULT NEWID(),
        created_at DATETIME2 DEFAULT SYSDATETIME()
    )
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM movies)
    INSERT INTO movies (id, title, release_year, genre, rating, votes, director, duration, is_active, metadata) VALUES
    (1, 'The Matrix', 1999, 'Sci-Fi', 8.7, 1800000, 'Wachowski Sisters', 136, 1, '{"budget": 63000000}'),
    (2, 'Inception', 2010, 'Sci-Fi', 8.8, 2200000, 'Christopher Nolan', 148, 1, '{"budget": 160000000}'),
    (3, 'The Shawshank Redemption', 1994, 'Drama', 9.3, 2500000, 'Frank Darabont', 142, 1, '{"budget": 25000000}'),
    (4, 'Pulp Fiction', 1994, 'Drama', 8.9, 1900000, 'Quentin Tarantino', 154, 1, '{"budget": 8000000}'),
    (5, 'The Dark Knight', 2008, 'Action', 9.0, 2400000, 'Christopher Nolan', 152, 1, '{"budget": 185000000}')
    """,
    """
    IF OBJECT_ID('actors', 'U') IS NULL
    CREATE TABLE actors (
        id INT PRIMARY KEY,
        name NVARCHAR(255) NOT NULL,
        birth_year INT,
        country NVARCHAR(100),
        active BIT DEFAULT 1
    )
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM actors)
    INSERT INTO actors (id, name, birth_year, country, active) VALUES
    (1, 'Keanu Reeves', 1964, 'Canada', 1),
    (2, 'Leonardo DiCaprio', 1974, 'USA', 1),
    (3, 'Morgan Freeman', 1937, 'USA', 1),
    (4, 'Samuel L. Jackson', 1948, 'USA', 1),
    (5, 'Christian Bale', 1974, 'UK', 1)
    """,
]


def is_docker_available() -> bool:
    """
    Check if Docker is available on the system.

    Returns:
        bool: True if Docker is available and working, False otherwise
    """
    # Skip Docker on macOS and Windows in CI
    if (IS_MACOS or IS_WINDOWS) and os.environ.get("CI", "").lower() in ("true", "1", "yes"):
        logger.info("Skipping Docker on macOS/Windows in CI environment")
        return False

    if shutil.which("docker") is None:
        logger.warning("Docker executable not found in PATH")
        return False

    try:
        result = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            timeout=5,
            check=False,
        )

        if result.returncode != 0:
            logger.warning("Docker is not operational")
            return False

        return True
    except (subprocess.SubprocessError, OSError):
        logger.warning("Error running Docker command")
        return False


def is_container_running(container_name: str) -> bool:
    """Check if the SQL Server container is already running."""
    if not is_docker_available():
        return False

    try:
        result = subprocess.run(
            ["docker", "ps", "--filter", f"name={container_name}", "--format", "{{.Names}}"],
            capture_output=True,
            text=True,
            check=True,
        )
        return container_name in result.stdout.strip()
    except subprocess.CalledProcessError:
        logger.error("Failed to check if container is running.")
        return False


def can_connect_to_db() -> bool:
    """Check if we can connect to the SQL Server test database as the test user."""
    try:
        import pymssql

        conn = pymssql.connect(
            server=MSSQL_HOST,
            port=str(MSSQL_PORT),
            user=MSSQL_USER,
            password=MSSQL_PASSWORD,
            database=MSSQL_DB,
            login_timeout=5,
        )
        conn.close()
        return True
    except ImportError:
        logger.error("pymssql not installed. Run: pip install pymssql")
        return False
    except Exception as e:
        logger.debug(f"Could not connect to database: {e}")
        return False


def _can_connect_as_sa() -> bool:
    """Check if SQL Server accepts sa connections yet (readiness probe)."""
    try:
        import pymssql

        conn = pymssql.connect(
            server=MSSQL_HOST,
            port=str(MSSQL_PORT),
            user="sa",
            password=MSSQL_SA_PASSWORD,
            database="master",
            login_timeout=5,
        )
        conn.close()
        return True
    except Exception as e:
        logger.debug(f"Could not connect as sa: {e}")
        return False


def _execute_statements(conn, statements: list[str]) -> None:
    with conn.cursor() as cursor:
        for statement in statements:
            cursor.execute(statement)


def _init_sample_data() -> bool:
    """Provision the test database/login and seed sample data (connects as sa)."""
    try:
        import pymssql

        # CREATE DATABASE cannot run inside a transaction, hence autocommit
        conn = pymssql.connect(
            server=MSSQL_HOST,
            port=str(MSSQL_PORT),
            user="sa",
            password=MSSQL_SA_PASSWORD,
            database="master",
            login_timeout=10,
        )
        try:
            conn.autocommit(True)
            _execute_statements(conn, PROVISION_SQL)
        finally:
            conn.close()

        conn = pymssql.connect(
            server=MSSQL_HOST,
            port=str(MSSQL_PORT),
            user="sa",
            password=MSSQL_SA_PASSWORD,
            database=MSSQL_DB,
            login_timeout=10,
        )
        try:
            conn.autocommit(True)
            _execute_statements(conn, PROVISION_DB_SQL + SAMPLE_DATA_SQL)
        finally:
            conn.close()

        logger.info("Sample data initialized successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize sample data: {e}")
        return False


def start_mssql_container(
    container_name: str = MSSQL_CONTAINER_NAME,
    port: int = MSSQL_PORT,
    image: str = MSSQL_IMAGE,
) -> tuple[subprocess.Popen | None, bool]:
    """
    Start the SQL Server container with sample data.

    Args:
        container_name: Name to give the Docker container
        port: Port to expose SQL Server on
        image: Docker image to use

    Returns:
        Tuple containing the process object (or None) and a success flag
    """
    if not is_docker_available():
        logger.warning("Docker not available, skipping SQL Server container start")
        return None, False

    logger.info("Starting SQL Server container...")

    if is_container_running(container_name):
        logger.info(f"Container {container_name} is already running")
        return None, True

    # Remove any leftover stopped container with the same name
    # (docker run --name fails if a stopped container with the same name exists)
    subprocess.run(
        ["docker", "rm", "-f", container_name],
        capture_output=True,
        check=False,
    )

    # Pull the image first (may take a while on first run)
    try:
        logger.info(f"Pulling Docker image {image}...")
        subprocess.run(
            ["docker", "pull", image],
            capture_output=True,
            timeout=300,
            check=True,
        )
    except subprocess.TimeoutExpired:
        logger.error(f"Timed out pulling Docker image {image}")
        return None, False
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to pull Docker image {image}: {e}")
        return None, False

    try:
        result = subprocess.run(
            [
                "docker",
                "run",
                "--name",
                container_name,
                "-e",
                "ACCEPT_EULA=Y",
                "-e",
                f"MSSQL_SA_PASSWORD={MSSQL_SA_PASSWORD}",
                "-p",
                f"{port}:1433",
                "--rm",
                "-d",
                image,
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )

        if result.returncode != 0:
            logger.error(f"Failed to start container (rc={result.returncode}): {result.stderr.strip()}")
            return None, False
    except subprocess.TimeoutExpired:
        logger.error("Timed out starting SQL Server container")
        return None, False
    except Exception as e:
        logger.error(f"Error starting container: {e}")
        return None, False

    # Wait for the database to be ready (SQL Server takes longer than MySQL)
    start_time = time.time()
    max_retries = STARTUP_TIMEOUT // STARTUP_CHECK_INTERVAL

    for _i in range(max_retries):
        if _can_connect_as_sa():
            elapsed = time.time() - start_time
            logger.info(f"SQL Server container started successfully in {elapsed:.2f} seconds")

            if not _init_sample_data():
                return None, False
            return None, True

        elapsed = time.time() - start_time
        logger.info(f"Waiting for SQL Server to start... ({elapsed:.1f}s / {STARTUP_TIMEOUT}s)")
        time.sleep(STARTUP_CHECK_INTERVAL)

    logger.error(f"SQL Server failed to start within {STARTUP_TIMEOUT} seconds")
    return None, False


def stop_mssql_container(container_name: str = MSSQL_CONTAINER_NAME, timeout: int = SHUTDOWN_TIMEOUT) -> bool:
    """
    Stop the SQL Server container gracefully.

    Args:
        container_name: Name of the Docker container to stop
        timeout: Timeout for graceful container stop in seconds

    Returns:
        True if stop succeeds or container not running, False otherwise
    """
    if not is_docker_available():
        logger.warning("Docker not available, skipping SQL Server container stop")
        return True

    logger.info(f"Stopping SQL Server container {container_name}...")

    if not is_container_running(container_name):
        logger.info("Container is not running")
        return True

    try:
        subprocess.run(["docker", "stop", container_name], check=True, timeout=timeout)
        logger.info("Container stopped gracefully")
        return True
    except subprocess.TimeoutExpired:
        logger.warning(f"Container did not stop within {timeout} seconds, forcing removal")
        subprocess.run(["docker", "rm", "-f", container_name], check=False)
        return True
    except Exception as e:
        logger.warning(f"Error while stopping container: {e}")
        return False


def print_connection_info(
    host: str = MSSQL_HOST,
    port: int = MSSQL_PORT,
    db: str = MSSQL_DB,
    user: str = MSSQL_USER,
    container_name: str = MSSQL_CONTAINER_NAME,
) -> None:
    """
    Print connection information for easy reference.
    """
    if not is_docker_available():
        print("\n" + "=" * 50)
        print("SQL Server with Docker not available on this system")
        print("Tests requiring Docker will be skipped")
        print("=" * 50 + "\n")
        return

    print("\n" + "=" * 50)
    print("SQL Server Connection Information:")
    print("=" * 50)
    print(f"Host:     {host}")
    print(f"Port:     {port}")
    print(f"Database: {db}")
    print(f"User:     {user}")
    print("Password: ***")
    print(f"Connection string: mssql://{user}:***@{host}:{port}/{db}")
    print("=" * 50)
    print("\nTo stop the container, run:")
    print("poetry run stop_mssql")
    print("=" * 50 + "\n")


@contextmanager
def managed_mssql() -> Generator[dict[str, Any], None, None]:
    """
    Context manager for SQL Server container management.
    Ensures proper cleanup even when tests fail.

    Yields:
        Dictionary with database connection information or empty dict if Docker isn't available
    """
    if not is_docker_available():
        logger.warning("Docker not available, skipping managed_mssql context")
        yield {}
        return

    _, success = start_mssql_container()
    if not success:
        logger.error("Failed to start SQL Server container")
        yield {}
        return

    try:
        connection_info = {
            "host": MSSQL_HOST,
            "port": MSSQL_PORT,
            "dbname": MSSQL_DB,
            "user": MSSQL_USER,
            "password": MSSQL_PASSWORD,
            "connection_string": f"mssql://{MSSQL_USER}:{MSSQL_PASSWORD}@{MSSQL_HOST}:{MSSQL_PORT}/{MSSQL_DB}",
        }

        yield connection_info
    finally:
        stop_mssql_container()


def get_db_engine():
    """
    Create a SQLAlchemy engine connected to the test SQL Server database.

    Returns:
        SQLAlchemy engine object or None if Docker isn't available
    """
    if not is_docker_available():
        logger.warning("Docker not available, skipping get_db_engine")
        return None

    try:
        from sqlalchemy import create_engine

        connection_string = (
            f"mssql+pymssql://{MSSQL_USER}:{MSSQL_PASSWORD}@{MSSQL_HOST}:{MSSQL_PORT}/{MSSQL_DB}"
        )
        engine = create_engine(connection_string)
        return engine
    except ImportError:
        logger.error("SQLAlchemy or pymssql not installed.")
        raise
