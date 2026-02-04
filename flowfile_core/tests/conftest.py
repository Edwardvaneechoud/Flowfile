# conftest.py
import logging
import os
import platform
import signal
import subprocess
import sys
import time
from collections.abc import Generator
from contextlib import contextmanager

# Patch bcrypt for passlib 1.7.4 / bcrypt 5.0.0+ compatibility
import bcrypt
_original_hashpw = bcrypt.hashpw
def _patched_hashpw(password, salt):
    if isinstance(password, bytes) and len(password) > 72:
        password = password[:72]
    return _original_hashpw(password, salt)
bcrypt.hashpw = _patched_hashpw

import pytest
import requests

os.environ['TESTING'] = 'True'

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))
import socket

from test_utils.postgres import fixtures as pg_fixtures
from tests.flowfile_core_test_utils import is_docker_available
from tests.kernel_fixtures import managed_kernel


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

# Configuration constants
WORKER_HOST = os.environ.get("FLOWFILE_WORKER_HOST", "0.0.0.0")
WORKER_PORT = int(os.environ.get("FLOWFILE_WORKER_PORT", 63579))
WORKER_URL = f"http://{WORKER_HOST}:{WORKER_PORT}/docs"
STARTUP_TIMEOUT = int(os.environ.get("FLOWFILE_STARTUP_TIMEOUT", 30))  # seconds
STARTUP_CHECK_INTERVAL = 2  # seconds
SHUTDOWN_TIMEOUT = int(os.environ.get("FLOWFILE_SHUTDOWN_TIMEOUT", 15))  # seconds


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


def is_worker_running() -> bool:
    """Check if the flowfile worker service is already running."""
    try:
        response = requests.get(WORKER_URL, timeout=5)
        return response.ok
    except requests.exceptions.RequestException:
        return False


def start_worker() -> tuple[subprocess.Popen, bool]:
    """
    Start the flowfile worker process.

    Returns:
        Tuple containing the process object and a success flag
    """
    logger.info("Starting flowfile_worker process...")

    # Determine the appropriate command based on platform
    if platform.system() == "Windows":
        # Use shell=True on Windows
        proc = subprocess.Popen(
            "poetry run flowfile_worker",
            shell=True,
            stdout=sys.stdout,
            stderr=sys.stderr,
            universal_newlines=True,
            # On Windows, CREATE_NEW_PROCESS_GROUP flag allows sending Ctrl+C to child process
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if hasattr(subprocess, 'CREATE_NEW_PROCESS_GROUP') else 0
        )
    else:
        # Use shell=False on Unix-like systems and provide the full args list
        # This is safer and allows for proper process group handling
        proc = subprocess.Popen(
            ["poetry", "run", "flowfile_worker"],
            shell=False,
            stdout=sys.stdout,
            stderr=sys.stderr,
            universal_newlines=True,
            # On Unix, start in a new process group for clean signal handling
            preexec_fn=os.setsid if hasattr(os, 'setsid') else None
        )

    # Check if process started successfully
    retcode = proc.poll()
    if retcode is not None:
        logger.error(f"Process failed to start with return code {retcode}")
        return proc, False

    # Wait for service to be available
    start_time = time.time()
    max_retries = STARTUP_TIMEOUT // STARTUP_CHECK_INTERVAL

    for i in range(max_retries):
        # Check if process is still running
        if proc.poll() is not None:
            logger.error(f"Process terminated unexpectedly with code {proc.poll()}")
            return proc, False

        # Try to connect to the service
        try:
            response = requests.get(WORKER_URL, timeout=5)
            if response.ok:
                elapsed = time.time() - start_time
                logger.info(f"flowfile_worker started successfully in {elapsed:.2f} seconds")
                return proc, True
        except requests.exceptions.RequestException:
            pass

        # Log progress
        elapsed = time.time() - start_time
        logger.info(f"Waiting for flowfile_worker to start... ({elapsed:.1f}s / {STARTUP_TIMEOUT}s)")
        time.sleep(STARTUP_CHECK_INTERVAL)

    # Timeout reached
    logger.error(f"flowfile_worker failed to start within {STARTUP_TIMEOUT} seconds")
    return proc, False


def stop_worker(proc: subprocess.Popen) -> None:
    """
    Stop the flowfile worker process gracefully.

    Args:
        proc: The process object to terminate
    """
    logger.info("Stopping flowfile_worker process...")

    if proc is None or proc.poll() is not None:
        logger.info("Process is already terminated")
        return

    # Try graceful termination first
    try:
        if platform.system() == "Windows":
            # On Windows, send Ctrl+C
            proc.send_signal(signal.CTRL_C_EVENT if hasattr(signal, 'CTRL_C_EVENT') else signal.SIGTERM)
        else:
            # On Unix, terminate the entire process group
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM) if hasattr(os, 'killpg') else proc.terminate()

        # Wait for process to terminate
        try:
            proc.wait(timeout=SHUTDOWN_TIMEOUT)
            logger.info("Process terminated gracefully")
        except subprocess.TimeoutExpired:
            logger.warning(f"Process did not terminate within {SHUTDOWN_TIMEOUT} seconds, forcing termination")
            if platform.system() != "Windows":
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL) if hasattr(os, 'killpg') else proc.kill()
            else:
                proc.kill()
            proc.wait(timeout=5)
            logger.info("Process forcefully terminated")
    except (ProcessLookupError, OSError) as e:
        logger.warning(f"Error while terminating process: {e}")


@contextmanager
def managed_worker() -> Generator[None, None, None]:
    """
    Context manager for flowfile worker process management.
    Ensures proper cleanup even when tests fail.
    """
    proc = None
    try:
        if is_worker_running():
            logger.info("flowfile_worker is already running, using existing instance")
            yield
        else:
            proc, success = start_worker()
            if not success:
                error_msg = "Failed to start flowfile_worker"
                logger.error(error_msg)
                if proc and proc.poll() is None:
                    stop_worker(proc)
                pytest.skip(error_msg)
            yield
    finally:
        if proc is not None and proc.poll() is None:
            stop_worker(proc)


@pytest.fixture(scope="session", autouse=True)
def flowfile_worker(request):
    """
    Pytest fixture that ensures flowfile_worker is running for the test session.
    Uses the managed_worker context manager for proper resource management.

    Can be skipped by setting SKIP_WORKER_TESTS=1 environment variable.
    """
    if os.environ.get("SKIP_WORKER_TESTS") == "1":
        yield
        return
    with managed_worker():
        yield


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


@pytest.fixture(scope="session")
def kernel_manager():
    """
    Pytest fixture that builds the flowfile-kernel Docker image, creates a
    KernelManager, starts a test kernel, and tears everything down afterwards.

    Yields a (KernelManager, kernel_id) tuple.

    Note: This fixture does NOT start the Core API. For tests that need
    global artifacts (publish_global, get_global, etc.), use the
    `kernel_manager_with_core` fixture instead.
    """
    if not is_docker_available():
        pytest.skip("Docker is not available, skipping kernel tests")

    try:
        with managed_kernel() as ctx:
            yield ctx
    except Exception as exc:
        pytest.skip(f"Kernel container could not be started: {exc}")


@pytest.fixture(scope="session")
def kernel_manager_with_core():
    """
    Pytest fixture for tests that need kernel + Core API integration.

    This fixture:
    - Starts the Core API server (for global artifacts endpoints)
    - Sets up authentication tokens for kernel ↔ Core communication
    - Builds and starts a kernel container
    - Tears everything down afterwards

    Use this fixture for tests that call:
    - flowfile.publish_global()
    - flowfile.get_global()
    - flowfile.list_global_artifacts()
    - flowfile.delete_global_artifact()

    Yields a (KernelManager, kernel_id) tuple.
    """
    if not is_docker_available():
        pytest.skip("Docker is not available, skipping kernel tests")

    try:
        with managed_kernel(start_core=True) as ctx:
            yield ctx
    except Exception as exc:
        pytest.skip(f"Kernel + Core could not be started: {exc}")


@pytest.fixture
def cleanup_global_artifacts():
    """Clean up global artifacts before and after each test.

    Use this fixture explicitly in tests that need artifact cleanup.
    """
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.database.models import GlobalArtifact

    def _cleanup():
        try:
            with get_db_context() as db:
                db.query(GlobalArtifact).delete()
                db.commit()
        except Exception:
            pass  # Table may not exist yet

    _cleanup()
    yield
    _cleanup()
