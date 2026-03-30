"""
MySQL fixtures for tests.

This module provides utilities to set up, manage, and tear down MySQL
containers with sample data for testing.
"""

import logging
import os
import shutil
import subprocess
import time
from collections.abc import Generator
from contextlib import contextmanager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger("mysql_fixture")

# Configuration constants
MYSQL_HOST = os.environ.get("TEST_MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.environ.get("TEST_MYSQL_PORT", 3307))
MYSQL_USER = os.environ.get("TEST_MYSQL_USER", "testuser")
MYSQL_PASSWORD = os.environ.get("TEST_MYSQL_PASSWORD", "testpass")
MYSQL_ROOT_PASSWORD = os.environ.get("TEST_MYSQL_ROOT_PASSWORD", "rootpass")
MYSQL_DB = os.environ.get("TEST_MYSQL_DB", "testdb")
MYSQL_CONTAINER_NAME = os.environ.get("TEST_MYSQL_CONTAINER", "test-mysql-sample")
MYSQL_IMAGE = os.environ.get("TEST_MYSQL_IMAGE", "mysql:8")
STARTUP_TIMEOUT = int(os.environ.get("TEST_MYSQL_STARTUP_TIMEOUT", 60))  # MySQL takes longer to start
SHUTDOWN_TIMEOUT = int(os.environ.get("TEST_MYSQL_SHUTDOWN_TIMEOUT", 15))  # seconds
STARTUP_CHECK_INTERVAL = 3  # seconds

# Operating system detection
IS_MACOS = os.uname().sysname == "Darwin" if hasattr(os, "uname") else False
IS_WINDOWS = os.name == "nt"

# SQL to create sample data
SAMPLE_DATA_SQL = """
CREATE TABLE IF NOT EXISTS movies (
    id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    release_year YEAR,
    genre ENUM('Action', 'Comedy', 'Drama', 'Sci-Fi', 'Horror', 'Documentary'),
    rating DECIMAL(3, 1),
    votes INT UNSIGNED,
    director VARCHAR(255),
    duration SMALLINT,
    tags SET('classic', 'award-winner', 'cult', 'blockbuster'),
    is_active TINYINT(1) DEFAULT 1,
    metadata JSON,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

INSERT IGNORE INTO movies (id, title, release_year, genre, rating, votes, director, duration, tags, is_active, metadata) VALUES
(1, 'The Matrix', 1999, 'Sci-Fi', 8.7, 1800000, 'Wachowski Sisters', 136, 'classic,blockbuster', 1, '{"budget": 63000000}'),
(2, 'Inception', 2010, 'Sci-Fi', 8.8, 2200000, 'Christopher Nolan', 148, 'blockbuster', 1, '{"budget": 160000000}'),
(3, 'The Shawshank Redemption', 1994, 'Drama', 9.3, 2500000, 'Frank Darabont', 142, 'classic,award-winner', 1, '{"budget": 25000000}'),
(4, 'Pulp Fiction', 1994, 'Drama', 8.9, 1900000, 'Quentin Tarantino', 154, 'classic,cult', 1, '{"budget": 8000000}'),
(5, 'The Dark Knight', 2008, 'Action', 9.0, 2400000, 'Christopher Nolan', 152, 'blockbuster', 1, '{"budget": 185000000}');

CREATE TABLE IF NOT EXISTS actors (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    birth_year MEDIUMINT,
    country VARCHAR(100),
    active BOOLEAN DEFAULT TRUE
);

INSERT IGNORE INTO actors (id, name, birth_year, country, active) VALUES
(1, 'Keanu Reeves', 1964, 'Canada', TRUE),
(2, 'Leonardo DiCaprio', 1974, 'USA', TRUE),
(3, 'Morgan Freeman', 1937, 'USA', TRUE),
(4, 'Samuel L. Jackson', 1948, 'USA', TRUE),
(5, 'Christian Bale', 1974, 'UK', TRUE);
"""


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

    # If docker executable is not in PATH
    if shutil.which("docker") is None:
        logger.warning("Docker executable not found in PATH")
        return False

    # Try a simple docker command
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
    """Check if the MySQL container is already running."""
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
    """Check if we can connect to the MySQL database."""
    try:
        import pymysql

        conn = pymysql.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB,
            connect_timeout=5,
        )
        conn.close()
        return True
    except ImportError:
        logger.error("pymysql not installed. Run: pip install pymysql")
        return False
    except Exception as e:
        logger.debug(f"Could not connect to database: {e}")
        return False


def _init_sample_data() -> bool:
    """Initialize sample data in the MySQL database after container starts."""
    try:
        import pymysql

        conn = pymysql.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB,
            connect_timeout=10,
        )
        try:
            with conn.cursor() as cursor:
                # Execute each statement separately
                for statement in SAMPLE_DATA_SQL.strip().split(";"):
                    statement = statement.strip()
                    if statement:
                        cursor.execute(statement)
            conn.commit()
            logger.info("Sample data initialized successfully")
            return True
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"Failed to initialize sample data: {e}")
        return False


def start_mysql_container(
    container_name: str = MYSQL_CONTAINER_NAME,
    port: int = MYSQL_PORT,
    image: str = MYSQL_IMAGE,
) -> tuple[subprocess.Popen | None, bool]:
    """
    Start the MySQL container with sample data.

    Args:
        container_name: Name to give the Docker container
        port: Port to expose MySQL on
        image: Docker image to use

    Returns:
        Tuple containing the process object (or None) and a success flag
    """
    # Check Docker availability
    if not is_docker_available():
        logger.warning("Docker not available, skipping MySQL container start")
        return None, False

    logger.info("Starting MySQL container...")

    # Check if container is already running
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

    # Run the container in the background
    try:
        result = subprocess.run(
            [
                "docker",
                "run",
                "--name",
                container_name,
                "-e",
                f"MYSQL_ROOT_PASSWORD={MYSQL_ROOT_PASSWORD}",
                "-e",
                f"MYSQL_DATABASE={MYSQL_DB}",
                "-e",
                f"MYSQL_USER={MYSQL_USER}",
                "-e",
                f"MYSQL_PASSWORD={MYSQL_PASSWORD}",
                "-p",
                f"{port}:3306",
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
        logger.error("Timed out starting MySQL container")
        return None, False
    except Exception as e:
        logger.error(f"Error starting container: {e}")
        return None, False

    # Wait for the database to be ready (MySQL takes longer than PostgreSQL)
    start_time = time.time()
    max_retries = STARTUP_TIMEOUT // STARTUP_CHECK_INTERVAL

    for _i in range(max_retries):
        if can_connect_to_db():
            elapsed = time.time() - start_time
            logger.info(f"MySQL container started successfully in {elapsed:.2f} seconds")

            # Initialize sample data
            _init_sample_data()
            return None, True

        # Log progress
        elapsed = time.time() - start_time
        logger.info(f"Waiting for MySQL to start... ({elapsed:.1f}s / {STARTUP_TIMEOUT}s)")
        time.sleep(STARTUP_CHECK_INTERVAL)

    # Timeout reached
    logger.error(f"MySQL failed to start within {STARTUP_TIMEOUT} seconds")
    return None, False


def stop_mysql_container(container_name: str = MYSQL_CONTAINER_NAME, timeout: int = SHUTDOWN_TIMEOUT) -> bool:
    """
    Stop the MySQL container gracefully.

    Args:
        container_name: Name of the Docker container to stop
        timeout: Timeout for graceful container stop in seconds

    Returns:
        True if stop succeeds or container not running, False otherwise
    """
    # Check Docker availability
    if not is_docker_available():
        logger.warning("Docker not available, skipping MySQL container stop")
        return True

    logger.info(f"Stopping MySQL container {container_name}...")

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
    host: str = MYSQL_HOST,
    port: int = MYSQL_PORT,
    db: str = MYSQL_DB,
    user: str = MYSQL_USER,
    password: str = MYSQL_PASSWORD,
    container_name: str = MYSQL_CONTAINER_NAME,
) -> None:
    """
    Print connection information for easy reference.
    """
    if not is_docker_available():
        print("\n" + "=" * 50)
        print("MySQL with Docker not available on this system")
        print("Tests requiring Docker will be skipped")
        print("=" * 50 + "\n")
        return

    masked_password = password[0] + "***" + password[-1] if len(password) > 2 else "***"
    print("\n" + "=" * 50)
    print("MySQL Connection Information:")
    print("=" * 50)
    print(f"Host:     {host}")
    print(f"Port:     {port}")
    print(f"Database: {db}")
    print(f"User:     {user}")
    print(f"Password: {masked_password}")
    print(f"Connection string: mysql://{user}:***@{host}:{port}/{db}")
    print("=" * 50)
    print("\nTo stop the container, run:")
    print("poetry run stop_mysql")
    print("=" * 50 + "\n")


@contextmanager
def managed_mysql() -> Generator[dict[str, any], None, None]:
    """
    Context manager for MySQL container management.
    Ensures proper cleanup even when tests fail.

    Yields:
        Dictionary with database connection information or empty dict if Docker isn't available
    """
    # Check Docker availability
    if not is_docker_available():
        logger.warning("Docker not available, skipping managed_mysql context")
        yield {}
        return

    # Start container
    _, success = start_mysql_container()
    if not success:
        logger.error("Failed to start MySQL container")
        yield {}
        return

    try:
        # Create connection details
        connection_info = {
            "host": MYSQL_HOST,
            "port": MYSQL_PORT,
            "dbname": MYSQL_DB,
            "user": MYSQL_USER,
            "password": MYSQL_PASSWORD,
            "connection_string": f"mysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}",
        }

        yield connection_info
    finally:
        # Always try to stop the container
        stop_mysql_container()


def get_db_engine():
    """
    Create a SQLAlchemy engine connected to the test MySQL database.

    Returns:
        SQLAlchemy engine object or None if Docker isn't available
    """
    # Check Docker availability
    if not is_docker_available():
        logger.warning("Docker not available, skipping get_db_engine")
        return None

    try:
        from sqlalchemy import create_engine

        connection_string = (
            f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
        )
        engine = create_engine(connection_string)
        return engine
    except ImportError:
        logger.error("SQLAlchemy or pymysql not installed.")
        raise
