"""
Kernel test fixtures.

Provides utilities to build the flowfile-kernel Docker image,
create a KernelManager, start/stop kernels, and clean up.
"""

import asyncio
import logging
import os
import secrets
import subprocess
import tempfile
import threading
import time
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

import httpx
import uvicorn

logger = logging.getLogger("kernel_fixture")

KERNEL_IMAGE = "flowfile-kernel"
KERNEL_TEST_ID = "integration-test"
KERNEL_TEST_ID_WITH_CORE = "integration-test-core"
CORE_TEST_PORT = 63578

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent

# Global reference to the Core server thread for cleanup
_core_server_thread: threading.Thread | None = None
_core_server_instance: uvicorn.Server | None = None


def _start_core_server() -> bool:
    """Start the Core API server in a background thread.

    Returns True if the server started successfully, False otherwise.
    """
    global _core_server_thread, _core_server_instance

    # Check if already running
    try:
        with httpx.Client(timeout=2.0) as client:
            resp = client.get(f"http://localhost:{CORE_TEST_PORT}/health/status")
            if resp.status_code == 200:
                logger.info("Core API already running on port %d", CORE_TEST_PORT)
                return True
    except (httpx.HTTPError, OSError):
        pass

    logger.info("Starting Core API server on port %d ...", CORE_TEST_PORT)

    # Import here to avoid circular imports
    from flowfile_core.main import app

    config = uvicorn.Config(
        app,
        host="0.0.0.0",
        port=CORE_TEST_PORT,
        log_level="warning",
    )
    _core_server_instance = uvicorn.Server(config)

    def run_server():
        _core_server_instance.run()

    _core_server_thread = threading.Thread(target=run_server, daemon=True)
    _core_server_thread.start()

    # Wait for server to become healthy
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            with httpx.Client(timeout=2.0) as client:
                resp = client.get(f"http://localhost:{CORE_TEST_PORT}/health/status")
                if resp.status_code == 200:
                    logger.info("Core API server started successfully")
                    return True
        except (httpx.HTTPError, OSError):
            pass
        time.sleep(0.5)

    logger.error("Core API server failed to start within timeout")
    return False


def _stop_core_server() -> None:
    """Stop the Core API server."""
    global _core_server_thread, _core_server_instance

    if _core_server_instance is not None:
        logger.info("Stopping Core API server...")
        _core_server_instance.should_exit = True
        if _core_server_thread is not None:
            _core_server_thread.join(timeout=5)
        _core_server_instance = None
        _core_server_thread = None
        logger.info("Core API server stopped")


def _build_kernel_image() -> bool:
    """Build the flowfile-kernel Docker image from kernel_runtime/."""
    dockerfile = _REPO_ROOT / "kernel_runtime" / "Dockerfile"
    context = _REPO_ROOT / "kernel_runtime"

    logger.info("Repo root: %s", _REPO_ROOT)
    logger.info("Looking for Dockerfile at %s", dockerfile)

    if not dockerfile.exists():
        logger.error("Dockerfile not found at %s", dockerfile)
        # List contents of kernel_runtime directory if it exists
        if context.exists():
            logger.error("Contents of %s: %s", context, list(context.iterdir()))
        else:
            logger.error("Context directory %s does not exist", context)
        return False

    logger.info("Building Docker image '%s' ...", KERNEL_IMAGE)
    try:
        result = subprocess.run(
            ["docker", "build", "-t", KERNEL_IMAGE, "-f", str(dockerfile), str(context)],
            check=True,
            capture_output=True,
            text=True,
            timeout=300,
        )
        logger.info("Docker image '%s' built successfully", KERNEL_IMAGE)
        logger.debug("Build stdout: %s", result.stdout)
        return True
    except subprocess.CalledProcessError as exc:
        logger.error("Failed to build Docker image: %s\nstdout: %s\nstderr: %s", exc, exc.stdout, exc.stderr)
        return False
    except subprocess.TimeoutExpired:
        logger.error("Docker build timed out")
        return False


def _remove_container(name: str) -> None:
    """Force-remove a container by name (ignore errors if it doesn't exist)."""
    subprocess.run(
        ["docker", "rm", "-f", name],
        capture_output=True,
        check=False,
    )


@contextmanager
def managed_kernel(
    packages: list[str] | None = None,
    start_core: bool = False,
) -> Generator[tuple, None, None]:
    """
    Context manager that:
      1. Optionally starts the Core API server (for global artifacts tests)
      2. Builds the flowfile-kernel Docker image
      3. Creates a KernelManager with a temp shared volume
      4. Creates and starts a kernel
      5. Yields (manager, kernel_id)
      6. Stops + deletes the kernel and cleans up

    Args:
        packages: List of Python packages to install in the kernel.
        start_core: If True, starts the Core API server and sets up auth tokens
                   for kernel ↔ Core communication. Required for global artifacts.

    Usage::

        # Kernel-only tests
        with managed_kernel(packages=["scikit-learn"]) as (manager, kernel_id):
            result = await manager.execute(kernel_id, request)

        # Tests requiring Core API (global artifacts)
        with managed_kernel(start_core=True) as (manager, kernel_id):
            # kernel can now call flowfile.publish_global() etc.
            result = await manager.execute(kernel_id, request)
    """
    from flowfile_core.kernel.manager import KernelManager
    from flowfile_core.kernel.models import KernelConfig

    # Use different kernel IDs for kernel-only vs kernel+Core tests to avoid conflicts
    kernel_id = KERNEL_TEST_ID_WITH_CORE if start_core else KERNEL_TEST_ID
    container_name = f"flowfile-kernel-{kernel_id}"

    # Track what we need to clean up
    core_started_by_us = False
    original_token = None
    original_core_url = None

    # 1 — Optionally start Core API server and set up auth
    if start_core:
        # Save original values to restore later
        original_token = os.environ.get("FLOWFILE_INTERNAL_TOKEN")
        internal_token = secrets.token_hex(32)
        os.environ["FLOWFILE_INTERNAL_TOKEN"] = internal_token
        logger.info("Set FLOWFILE_INTERNAL_TOKEN for kernel ↔ Core auth")

        # Set FLOWFILE_CORE_URL so kernel can reach Core
        original_core_url = os.environ.get("FLOWFILE_CORE_URL")
        os.environ["FLOWFILE_CORE_URL"] = f"http://host.docker.internal:{CORE_TEST_PORT}"

        # Start Core API server
        if not _start_core_server():
            raise RuntimeError("Could not start Core API server for integration tests")
        core_started_by_us = True

    # 2 — Build image
    if not _build_kernel_image():
        raise RuntimeError("Could not build flowfile-kernel Docker image")

    # 3 — Ensure stale container is removed
    _remove_container(container_name)

    # 4 — Temp shared volume
    shared_dir = tempfile.mkdtemp(prefix="kernel_test_shared_")

    manager = KernelManager(shared_volume_path=shared_dir)

    try:
        # 5 — Create + start
        loop = asyncio.new_event_loop()
        config = KernelConfig(
            id=kernel_id,
            name="Integration Test Kernel",
            packages=packages or [],
        )
        loop.run_until_complete(manager.create_kernel(config, user_id=1))
        loop.run_until_complete(manager.start_kernel(kernel_id))

        yield manager, kernel_id

    finally:
        # 6 — Tear down
        try:
            loop.run_until_complete(manager.stop_kernel(kernel_id))
        except Exception as exc:
            logger.warning("Error stopping kernel during teardown: %s", exc)
        try:
            loop.run_until_complete(manager.delete_kernel(kernel_id))
        except Exception as exc:
            logger.warning("Error deleting kernel during teardown: %s", exc)
        loop.close()

        # Belt-and-suspenders: force-remove the container
        _remove_container(container_name)

        # Clean up shared dir
        import shutil

        shutil.rmtree(shared_dir, ignore_errors=True)

        # Stop Core server if we started it
        if core_started_by_us:
            _stop_core_server()

        # Restore original environment (only if we modified it)
        if start_core:
            if original_token is not None:
                os.environ["FLOWFILE_INTERNAL_TOKEN"] = original_token
            else:
                os.environ.pop("FLOWFILE_INTERNAL_TOKEN", None)
            if original_core_url is not None:
                os.environ["FLOWFILE_CORE_URL"] = original_core_url
            else:
                os.environ.pop("FLOWFILE_CORE_URL", None)
