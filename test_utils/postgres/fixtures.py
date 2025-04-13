"""
PostgreSQL fixtures for tests.

This module provides utilities to set up, manage, and tear down PostgreSQL
containers with sample data for testing.
"""

import os
import time
import logging
import subprocess
import shutil
from contextlib import contextmanager
from typing import Dict, Generator, Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger("postgres_fixture")

# Configuration constants
POSTGRES_HOST = os.environ.get("TEST_POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.environ.get("TEST_POSTGRES_PORT", 5433))
POSTGRES_USER = os.environ.get("TEST_POSTGRES_USER", "testuser")
POSTGRES_PASSWORD = os.environ.get("TEST_POSTGRES_PASSWORD", "testpass")
POSTGRES_DB = os.environ.get("TEST_POSTGRES_DB", "testdb")
POSTGRES_SCHEMA = os.environ.get("TEST_POSTGRES_SCHEMA", "movies")  # or "stocks"
POSTGRES_CONTAINER_NAME = os.environ.get("TEST_POSTGRES_CONTAINER", "test-postgres-sample")
POSTGRES_IMAGE_TAG = os.environ.get("TEST_POSTGRES_IMAGE", "test-sample-db")
STARTUP_TIMEOUT = int(os.environ.get("TEST_POSTGRES_STARTUP_TIMEOUT", 30))  # seconds
SHUTDOWN_TIMEOUT = int(os.environ.get("TEST_POSTGRES_SHUTDOWN_TIMEOUT", 15))  # seconds
STARTUP_CHECK_INTERVAL = 2  # seconds

# Path to postgres-docker-samples repo
SAMPLES_REPO_URL = "https://github.com/zseta/postgres-docker-samples.git"
SAMPLES_REPO_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "postgres-docker-samples")

# Operating system detection
IS_MACOS = os.uname().sysname == 'Darwin' if hasattr(os, 'uname') else False
IS_WINDOWS = os.name == 'nt'


def is_docker_available() -> bool:
    """
    Check if Docker is available on the system.

    Returns:
        bool: True if Docker is available and working, False otherwise
    """
    # Skip Docker on macOS and Windows in CI
    if (IS_MACOS or IS_WINDOWS) and os.environ.get('CI', '').lower() in ('true', '1', 'yes'):
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
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=5,
            check=False  # Don't raise exception on non-zero return code
        )

        if result.returncode != 0:
            logger.warning("Docker is not operational")
            return False

        return True
    except (subprocess.SubprocessError, OSError):
        logger.warning("Error running Docker command")
        return False


def is_container_running(container_name: str) -> bool:
    """Check if the postgres container is already running."""
    if not is_docker_available():
        return False

    try:
        result = subprocess.run(
            ["docker", "ps", "--filter", f"name={container_name}", "--format", "{{.Names}}"],
            capture_output=True,
            text=True,
            check=True
        )
        return container_name in result.stdout.strip()
    except subprocess.CalledProcessError:
        logger.error("Failed to check if container is running.")
        return False


def can_connect_to_db() -> bool:
    """Check if we can connect to the PostgreSQL database."""
    try:
        # Importing here to avoid requiring psycopg2 at module level
        import psycopg2

        conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            connect_timeout=5
        )
        conn.close()
        return True
    except ImportError:
        logger.error("psycopg2 not installed. Run: pip install psycopg2-binary")
        return False
    except Exception as e:
        logger.debug(f"Could not connect to database: {e}")
        return False


def setup_postgres_samples(
        schema: str = POSTGRES_SCHEMA,
        port: int = POSTGRES_PORT,
        user: str = POSTGRES_USER,
        password: str = POSTGRES_PASSWORD,
        db: str = POSTGRES_DB,
        image_tag: str = POSTGRES_IMAGE_TAG
) -> bool:
    """
    Clone the postgres-docker-samples repository if it doesn't exist
    and prepare the environment.

    Args:
        schema: Sample schema to use (movies or stocks)
        port: Port to expose PostgreSQL on
        user: PostgreSQL username
        password: PostgreSQL password
        db: PostgreSQL database name
        image_tag: Docker image tag

    Returns:
        True if setup succeeds, False otherwise
    """
    # Check Docker availability
    if not is_docker_available():
        logger.warning("Docker not available, skipping Postgres sample setup")
        return False

    # Clone the repository if it doesn't exist
    if not os.path.exists(SAMPLES_REPO_DIR):
        logger.info(f"Cloning postgres-docker-samples repository to {SAMPLES_REPO_DIR}")
        try:
            subprocess.run(
                ["git", "clone", SAMPLES_REPO_URL, SAMPLES_REPO_DIR],
                check=True
            )
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to clone repository: {e}")
            return False

    # Change to the repository directory
    original_dir = os.getcwd()
    os.chdir(SAMPLES_REPO_DIR)
    print(os.getcwd())

    try:
        # Create a custom .env file for our test configuration
        logger.info(f"Configuring .env file with schema={schema}")
        with open('.env', 'w') as f:
            f.write(f"""
# choose a schema (must be the name of a folder)
SAMPLE_SCHEMA={schema}

# add database credentials
POSTGRES_PORT={port}
POSTGRES_USER={user}
POSTGRES_PASSWORD={password}
POSTGRES_DB={db}

# image tag
DOCKER_IMAGE_TAG={image_tag}
            """)

        # Make scripts executable
        subprocess.run(["chmod", "+x", "build.sh"], check=True)
        subprocess.run(["chmod", "+x", "run.sh"], check=True)

        # Build the Docker image - FIXED: Use bash to execute the script
        logger.info(f"Building Docker image {image_tag}")
        subprocess.run(["bash", "build.sh"], check=True)

        # Return to original directory
        os.chdir(original_dir)
        return True

    except Exception as e:
        logger.error(f"Error setting up postgres samples: {e}")
        os.chdir(original_dir)
        return False


def start_postgres_container(
        container_name: str = POSTGRES_CONTAINER_NAME,
        port: int = POSTGRES_PORT,
        image_tag: str = POSTGRES_IMAGE_TAG
) -> Tuple[Optional[subprocess.Popen], bool]:
    """
    Start the PostgreSQL container with sample data.

    Args:
        container_name: Name to give the Docker container
        port: Port to expose PostgreSQL on
        image_tag: Docker image tag to use

    Returns:
        Tuple containing the process object (or None) and a success flag
    """
    # Check Docker availability
    if not is_docker_available():
        logger.warning("Docker not available, skipping PostgreSQL container start")
        return None, False

    logger.info(f"Starting PostgreSQL container with sample data...")

    # Check if container is already running
    if is_container_running(container_name):
        logger.info(f"Container {container_name} is already running")
        return None, True

    # Run the container in the background
    try:
        proc = subprocess.Popen([
            "docker", "run", "--name", container_name,
            "-p", f"{port}:5432", "--rm", "-d", image_tag
        ])

        # Wait for the process to complete (should be quick as it just starts the container)
        proc.wait(timeout=5)

        if proc.returncode != 0:
            logger.error(f"Failed to start container with return code {proc.returncode}")
            return proc, False
    except Exception as e:
        logger.error(f"Error starting container: {e}")
        return None, False

    # Wait for the database to be ready
    start_time = time.time()
    max_retries = STARTUP_TIMEOUT // STARTUP_CHECK_INTERVAL

    for i in range(max_retries):
        if can_connect_to_db():
            elapsed = time.time() - start_time
            logger.info(f"PostgreSQL container started successfully in {elapsed:.2f} seconds")
            return None, True

        # Log progress
        elapsed = time.time() - start_time
        logger.info(f"Waiting for PostgreSQL to start... ({elapsed:.1f}s / {STARTUP_TIMEOUT}s)")
        time.sleep(STARTUP_CHECK_INTERVAL)

    # Timeout reached
    logger.error(f"PostgreSQL failed to start within {STARTUP_TIMEOUT} seconds")
    return None, False


def stop_postgres_container(container_name: str = POSTGRES_CONTAINER_NAME, timeout: int = SHUTDOWN_TIMEOUT) -> bool:
    """
    Stop the PostgreSQL container gracefully.

    Args:
        container_name: Name of the Docker container to stop
        timeout: Timeout for graceful container stop in seconds

    Returns:
        True if stop succeeds or container not running, False otherwise
    """
    # Check Docker availability
    if not is_docker_available():
        logger.warning("Docker not available, skipping PostgreSQL container stop")
        return True

    logger.info(f"Stopping PostgreSQL container {container_name}...")

    if not is_container_running(container_name):
        logger.info("Container is not running")
        return True

    try:
        subprocess.run(
            ["docker", "stop", container_name],
            check=True,
            timeout=timeout
        )
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
        host: str = POSTGRES_HOST,
        port: int = POSTGRES_PORT,
        db: str = POSTGRES_DB,
        user: str = POSTGRES_USER,
        password: str = POSTGRES_PASSWORD,
        container_name: str = POSTGRES_CONTAINER_NAME
) -> None:
    """
    Print connection information for easy reference.

    Args:
        host: PostgreSQL host
        port: PostgreSQL port
        db: PostgreSQL database name
        user: PostgreSQL username
        password: PostgreSQL password
        container_name: Docker container name
    """
    if not is_docker_available():
        print("\n" + "=" * 50)
        print("PostgreSQL with Docker not available on this system")
        print("Tests requiring Docker will be skipped")
        print("=" * 50 + "\n")
        return

    print("\n" + "=" * 50)
    print("PostgreSQL Connection Information:")
    print("=" * 50)
    print(f"Host:     {host}")
    print(f"Port:     {port}")
    print(f"Database: {db}")
    print(f"User:     {user}")
    print(f"Password: {password}")
    print(f"Connection string: postgresql://{user}:{password}@{host}:{port}/{db}")
    print("=" * 50)
    print("\nTo stop the container, run:")
    print(f"poetry run stop_postgres")
    print("=" * 50 + "\n")


@contextmanager
def managed_postgres() -> Generator[Dict[str, any], None, None]:
    """
    Context manager for PostgreSQL container management.
    Ensures proper cleanup even when tests fail.

    Yields:
        Dictionary with database connection information or empty dict if Docker isn't available
    """
    # Check Docker availability
    if not is_docker_available():
        logger.warning("Docker not available, skipping managed_postgres context")
        yield {}
        return

    # Setup
    if not setup_postgres_samples():
        logger.error("Failed to set up postgres samples")
        yield {}
        return

    # Start container
    _, success = start_postgres_container()
    if not success:
        logger.error("Failed to start PostgreSQL container")
        yield {}
        return

    try:
        # Create connection details
        connection_info = {
            "host": POSTGRES_HOST,
            "port": POSTGRES_PORT,
            "dbname": POSTGRES_DB,
            "user": POSTGRES_USER,
            "password": POSTGRES_PASSWORD,
            "connection_string": f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        }

        yield connection_info
    finally:
        # Always try to stop the container
        stop_postgres_container()


def get_db_engine():
    """
    Create a SQLAlchemy engine connected to the test database.

    Returns:
        SQLAlchemy engine object or None if Docker isn't available
    """
    # Check Docker availability
    if not is_docker_available():
        logger.warning("Docker not available, skipping get_db_engine")
        return None

    try:
        from sqlalchemy import create_engine

        connection_string = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        engine = create_engine(connection_string)
        return engine
    except ImportError:
        logger.error("SQLAlchemy not installed. Run: pip install sqlalchemy")
        raise