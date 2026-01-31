"""
Kernel test fixtures.

Provides utilities to build the flowfile-kernel Docker image,
create a KernelManager, start/stop kernels, and clean up.
"""

import asyncio
import logging
import os
import subprocess
import tempfile
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

logger = logging.getLogger("kernel_fixture")

KERNEL_IMAGE = "flowfile-kernel"
KERNEL_TEST_ID = "integration-test"
KERNEL_CONTAINER_NAME = f"flowfile-kernel-{KERNEL_TEST_ID}"

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent


def _build_kernel_image() -> bool:
    """Build the flowfile-kernel Docker image from kernel_runtime/."""
    dockerfile = _REPO_ROOT / "kernel_runtime" / "Dockerfile"
    context = _REPO_ROOT / "kernel_runtime"

    if not dockerfile.exists():
        logger.error("Dockerfile not found at %s", dockerfile)
        return False

    logger.info("Building Docker image '%s' ...", KERNEL_IMAGE)
    try:
        subprocess.run(
            ["docker", "build", "-t", KERNEL_IMAGE, "-f", str(dockerfile), str(context)],
            check=True,
            capture_output=True,
            text=True,
            timeout=300,
        )
        logger.info("Docker image '%s' built successfully", KERNEL_IMAGE)
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
) -> Generator[tuple, None, None]:
    """
    Context manager that:
      1. Builds the flowfile-kernel Docker image
      2. Creates a KernelManager with a temp shared volume
      3. Creates and starts a kernel
      4. Yields (manager, kernel_id)
      5. Stops + deletes the kernel and cleans up

    Usage::

        with managed_kernel(packages=["scikit-learn"]) as (manager, kernel_id):
            result = await manager.execute(kernel_id, request)
    """
    from flowfile_core.kernel.manager import KernelManager
    from flowfile_core.kernel.models import KernelConfig

    # 1 — Build image
    if not _build_kernel_image():
        raise RuntimeError("Could not build flowfile-kernel Docker image")

    # 2 — Ensure stale container is removed
    _remove_container(KERNEL_CONTAINER_NAME)

    # 3 — Temp shared volume
    shared_dir = tempfile.mkdtemp(prefix="kernel_test_shared_")

    manager = KernelManager(shared_volume_path=shared_dir)
    kernel_id = KERNEL_TEST_ID

    try:
        # 4 — Create + start
        loop = asyncio.new_event_loop()
        config = KernelConfig(
            id=kernel_id,
            name="Integration Test Kernel",
            packages=packages or [],
        )
        loop.run_until_complete(manager.create_kernel(config))
        loop.run_until_complete(manager.start_kernel(kernel_id))

        yield manager, kernel_id

    finally:
        # 5 — Tear down
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
        _remove_container(KERNEL_CONTAINER_NAME)

        # Clean up shared dir
        import shutil

        shutil.rmtree(shared_dir, ignore_errors=True)
