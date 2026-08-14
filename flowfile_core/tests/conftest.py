import logging
import os
import platform
import signal
import subprocess
import sys
import tempfile
import time
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

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
# TestClient lifespans open constantly in the suite; never let the kernel
# warm-up thread touch Docker (container reclaim / image GC) from tests.
os.environ.setdefault('FLOWFILE_KERNEL_WARMUP', '0')

# Keep jwt_secret / master_key / internal_token out of the developer's real
# ~/.config/flowfile store. Stable across sessions and outside storage.temp_directory,
# which the 24h sweep in shared.storage_config.cleanup_directories would eat.
# Skipped when a worker is already running: it resolved its own store at its
# import, and a split store breaks every worker-offloaded $ffsec$ decrypt.
from tests.secure_storage_isolation import (  # noqa: E402
    resolve_test_secure_storage_path,
    worker_is_listening,
)

_explicit_secure_store = bool(os.environ.get('FLOWFILE_SECURE_STORAGE_PATH'))
_test_secure_store = resolve_test_secure_storage_path(worker_is_listening(), os.environ)
if _test_secure_store is not None:
    os.environ['FLOWFILE_SECURE_STORAGE_PATH'] = _test_secure_store

# Pin before core imports / worker spawn so every process derives the same shared paths.
if 'FLOWFILE_SHARED_DIR' not in os.environ:
    os.environ['FLOWFILE_SHARED_DIR'] = str(Path(tempfile.mkdtemp(prefix='flowfile_test_shared_')).resolve())

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))
import socket

from test_utils.mssql import fixtures as mssql_fixtures
from test_utils.mysql import fixtures as mysql_fixtures
from test_utils.postgres import fixtures as pg_fixtures
from tests.core_log_sink import CoreLogSink, session_claims_core_port
from tests.flowfile_core_test_utils import is_docker_available
from tests.kernel_fixtures import managed_kernel


def _pin_default_execution_location_to_local() -> None:
    """Make flows that never asked for a location run local, and only those.

    ``FlowGraphConfig.execution_location``'s default_factory is the single seam that
    turns an ordinary test flow *incidentally* remote (it resolves to "remote" whenever
    the worker offload flag is on), and each remote node costs a worker round-trip.
    Repointing only that factory leaves ``get_global_execution_location`` itself
    untouched, so an explicit ``execution_location="remote"`` still validates to remote
    and the ``execution_location`` fixture's [remote] param keeps running genuinely
    remote — the offload contract's only coverage.
    """
    from flowfile_core.schemas.schemas import FlowGraphConfig

    def _model_tree(model):
        yield model
        for sub in model.__subclasses__():
            yield from _model_tree(sub)

    for model in _model_tree(FlowGraphConfig):
        field = model.model_fields.get("execution_location")
        if field is None or field.default_factory is None:
            continue
        field.default_factory = lambda: "local"
        model.model_rebuild(force=True)


_pin_default_execution_location_to_local()


_NOT_ISOLATED_MSG = (
    'Secure store NOT isolated: a flowfile_worker is already running, so tests use the real '
    '~/.config/flowfile store (core and that worker must share a master key, or every '
    'worker-offloaded $ffsec$ decrypt fails). Stop the worker, or export '
    'FLOWFILE_SECURE_STORAGE_PATH for both, to keep test secrets out of it.'
)


def _secure_store_is_isolated() -> bool:
    return _test_secure_store is not None or _explicit_secure_store


def pytest_report_header(config):
    """Surface which secure store the suite is using — a split store is silent otherwise."""
    if not _secure_store_is_isolated():
        return f'secure store: {_NOT_ISOLATED_MSG}'
    return f"secure store: isolated at {os.environ.get('FLOWFILE_SECURE_STORAGE_PATH')}"


def pytest_configure(config):
    # The header is suppressed under -q; a config-time warning still shows.
    if not _secure_store_is_isolated():
        config.issue_config_time_warning(pytest.PytestConfigWarning(_NOT_ISOLATED_MSG), stacklevel=2)


_core_port_claimed_by_tests = False


def pytest_collection_modifyitems(config, items):
    global _core_port_claimed_by_tests
    _core_port_claimed_by_tests = session_claims_core_port(items)


def is_port_in_use(port, host='localhost'):
    """Check if a port is in use on the specified host."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.connect((host, port))
            return True
        except ConnectionRefusedError:
            return False


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger("flowfile_fixture")

# Configuration constants
WORKER_HOST = os.environ.get(
    "FLOWFILE_WORKER_HOST", "0.0.0.0" if platform.system() != "Windows" else "127.0.0.1"
)
WORKER_PORT = int(os.environ.get("FLOWFILE_WORKER_PORT", 63579))
WORKER_URL = f"http://{WORKER_HOST}:{WORKER_PORT}/docs"
STARTUP_TIMEOUT = int(os.environ.get("FLOWFILE_STARTUP_TIMEOUT", 30))  # seconds
STARTUP_CHECK_INTERVAL = 2  # seconds
SHUTDOWN_TIMEOUT = int(os.environ.get("FLOWFILE_SHUTDOWN_TIMEOUT", 15))  # seconds


@pytest.fixture(scope="session", autouse=True)
def setup_test_db():
    """Setup the test database and clean up after tests"""
    from flowfile_core.database.connection import engine, get_database_url
    from flowfile_core.database.init_db import init_db
    from flowfile_core.database.models import Base

    init_db()

    yield

    if os.environ.get("TESTING") == "True" and "sqlite" in get_database_url():
        logger.info(f"Trying to cleanup: {get_database_url()}")
        try:
            from sqlalchemy import text

            Base.metadata.drop_all(bind=engine)
            # drop_all leaves alembic_version; without dropping it a failed unlink (WinError 32) strands a stamped but table-less DB.
            with engine.begin() as conn:
                conn.execute(text("DROP TABLE IF EXISTS alembic_version"))
            engine.dispose()

            db_path = get_database_url().replace("sqlite:///", "")
            if db_path != ":memory:" and os.path.exists(db_path):
                os.remove(db_path)
                logger.info("Removed test database file")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")


@pytest.fixture(autouse=True)
def restore_seeded_catalog_namespaces():
    """Restore the seeded 'General' catalog if a previous module wiped it.

    Several catalog test modules clean up with an unconditional
    ``db.query(CatalogNamespace).delete()``, which also removes the session-scoped
    seed that ``init_db()`` created. Auto-registration silently no-ops without it
    (``CatalogService.auto_register_flow`` returns None when 'General' is missing),
    so any later module that creates flows fails in ways that depend on collection
    order. Both seed helpers are idempotent get-or-creates; the guard query keeps
    the steady-state cost at one SELECT per test.
    """
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.database.init_db import (
        create_default_catalog_namespace,
        create_default_local_user,
    )
    from flowfile_core.database.models import CatalogNamespace

    with get_db_context() as db:
        seed_missing = (
            db.query(CatalogNamespace).filter_by(name="General", parent_id=None).first() is None
        )
        if seed_missing:
            create_default_local_user(db)
            create_default_catalog_namespace(db)
    yield


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

    retcode = proc.poll()
    if retcode is not None:
        logger.error(f"Process failed to start with return code {retcode}")
        return proc, False

    start_time = time.time()
    max_retries = STARTUP_TIMEOUT // STARTUP_CHECK_INTERVAL

    for i in range(max_retries):
        if proc.poll() is not None:
            logger.error(f"Process terminated unexpectedly with code {proc.poll()}")
            return proc, False

        try:
            response = requests.get(WORKER_URL, timeout=5)
            if response.ok:
                elapsed = time.time() - start_time
                logger.info(f"flowfile_worker started successfully in {elapsed:.2f} seconds")
                return proc, True
        except requests.exceptions.RequestException:
            pass

        elapsed = time.time() - start_time
        logger.info(f"Waiting for flowfile_worker to start... ({elapsed:.1f}s / {STARTUP_TIMEOUT}s)")
        time.sleep(STARTUP_CHECK_INTERVAL)

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

    try:
        if platform.system() == "Windows":
            # On Windows, send Ctrl+C
            proc.send_signal(signal.CTRL_C_EVENT if hasattr(signal, 'CTRL_C_EVENT') else signal.SIGTERM)
        else:
            # On Unix, terminate the entire process group
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM) if hasattr(os, 'killpg') else proc.terminate()

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

    A failure here aborts the session instead of skipping. This runs inside a
    session-scoped autouse fixture, so a ``pytest.skip`` would skip every test in
    the suite and still exit 0 — which is how a broken worker probe turned a whole
    Windows CI run green while running nothing. ``SKIP_WORKER_TESTS=1`` remains the
    explicit way to run without a worker.
    """
    proc = None
    try:
        if is_worker_running():
            logger.info("flowfile_worker is already running, using existing instance")
            yield
        else:
            proc, success = start_worker()
            if not success:
                error_msg = (
                    f"Failed to start flowfile_worker at {WORKER_HOST}:{WORKER_PORT}. "
                    "Set SKIP_WORKER_TESTS=1 to run the suite without one."
                )
                logger.error(error_msg)
                if proc and proc.poll() is None:
                    stop_worker(proc)
                pytest.exit(error_msg, returncode=1)
            yield
    finally:
        if proc is not None and proc.poll() is None:
            stop_worker(proc)


@pytest.fixture(scope="session", autouse=True)
def core_log_sink():
    """Serve ``/raw_logs`` on the core port so worker log shipping stays fast.

    Worker children POST every node log line to core; here core is in-process
    behind TestClient, so nothing listens and each POST eats the handler's 2s
    connect timeout on Windows (~7 lines x 2s per worker-backed test). The sink
    keeps the real delivery path running at loopback speed.

    Never starts when the port is already bound (a real core wins), when the
    session collected a test that binds the port itself, or when
    FLOWFILE_TEST_LOG_SINK=0.
    """
    disabled = os.environ.get("FLOWFILE_TEST_LOG_SINK", "1").lower() in ("0", "false", "no", "off")
    if disabled or os.environ.get("SKIP_WORKER_TESTS") == "1" or _core_port_claimed_by_tests:
        yield None
        return

    sink = CoreLogSink()
    if not sink.start():
        logger.info("Core port %s already in use, not starting the log sink", sink.port)
        yield None
        return

    logger.info("Serving worker log shipping at http://%s:%s/raw_logs", sink.host, sink.port)
    try:
        yield sink
    finally:
        sink.stop()


@pytest.fixture(scope="session", autouse=True)
def flowfile_worker(request, core_log_sink):
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


@pytest.fixture(scope="session", autouse=True)
def mysql_db():
    """
    Pytest fixture that ensures MySQL container is running for the test session.
    Automatically starts and stops a MySQL container with sample data.
    """
    if is_port_in_use(3307) or mysql_fixtures.can_connect_to_db():
        print("MySQL is already running on port 3307, skipping container creation")
        yield
        return

    elif not is_docker_available():
        print("Docker is not available, skipping MySQL container creation")
        yield
        return

    with mysql_fixtures.managed_mysql() as db_info:
        if not db_info:
            print("MySQL container could not be started, MySQL tests will be skipped")
            yield
            return
        yield db_info


@pytest.fixture(scope="session", autouse=True)
def mssql_db():
    """
    Pytest fixture that ensures SQL Server container is running for the test session.
    Automatically starts and stops a SQL Server container with sample data.
    """
    if is_port_in_use(1434) or mssql_fixtures.can_connect_to_db():
        print("SQL Server is already running on port 1434, skipping container creation")
        yield
        return

    elif not is_docker_available():
        print("Docker is not available, skipping SQL Server container creation")
        yield
        return

    with mssql_fixtures.managed_mssql() as db_info:
        if not db_info:
            print("SQL Server container could not be started, SQL Server tests will be skipped")
            yield
            return
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
    # In CI, we want to fail loudly to see what's wrong
    in_ci = os.environ.get("CI") == "true" or os.environ.get("TEST_MODE") == "1"

    if not is_docker_available():
        if in_ci:
            pytest.fail("Docker is not available in CI - this is unexpected")
        pytest.skip("Docker is not available, skipping kernel tests")

    try:
        with managed_kernel() as ctx:
            yield ctx
    except Exception as exc:
        if in_ci:
            # In CI, fail loudly so we can see the actual error
            pytest.fail(f"Kernel container could not be started in CI: {exc}")
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
    - flowfile_ctx.publish_global()
    - flowfile_ctx.get_global()
    - flowfile_ctx.list_global_artifacts()
    - flowfile_ctx.delete_global_artifact()

    Yields a (KernelManager, kernel_id) tuple.
    """
    # In CI, we want to fail loudly to see what's wrong
    in_ci = os.environ.get("CI") == "true" or os.environ.get("TEST_MODE") == "1"

    if not is_docker_available():
        if in_ci:
            pytest.fail("Docker is not available in CI - this is unexpected")
        pytest.skip("Docker is not available, skipping kernel tests")

    try:
        with managed_kernel(start_core=True) as ctx:
            yield ctx
    except Exception as exc:
        if in_ci:
            # In CI, fail loudly so we can see the actual error
            pytest.fail(f"Kernel + Core could not be started in CI: {exc}")
        pytest.skip(f"Kernel + Core could not be started: {exc}")


@pytest.fixture(params=["local", "remote"])
def execution_location(request):
    """Parametrize a test across local and remote execution modes.

    Tests receive this fixture as a parameter, and pytest runs them once per
    param. The `remote` variant is skipped automatically when no worker is
    running, so this is safe for contributors without a worker.

    A live worker is necessary but not sufficient: the worker fixture is
    independent of the offload flag, so with ``FLOWFILE_OFFLOAD_TO_WORKER=0`` a
    worker is still listening while ``get_prio_execution_location`` silently
    downgrades "remote" to "local". Both params would then exercise the same
    path and report twice the passes while proving nothing about remote.
    """
    from flowfile_core.schemas.schemas import (
        is_valid_execution_location_in_current_global_settings,
    )

    if request.param == "remote" and not (
        is_worker_running() and is_valid_execution_location_in_current_global_settings("remote")
    ):
        pytest.skip("Remote execution not active")
    return request.param


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
