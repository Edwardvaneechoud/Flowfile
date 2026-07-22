"""Docker-backed regression tests for the parallel kernel-start race:
concurrent cold starts, 409-adoption of pre-existing containers, and
crash-safe manager boot with leftover containers."""

from __future__ import annotations

import threading
import time

import httpx
import pytest

from flowfile_core.kernel.manager import KernelManager
from flowfile_core.kernel.models import ExecuteRequest, KernelState

pytestmark = pytest.mark.kernel


def _containers_named(manager: KernelManager, kernel_id: str):
    # Docker's name filter matches substrings; compare exactly so the
    # "integration-test" kernel doesn't also count "integration-test-core".
    name = f"flowfile-kernel-{kernel_id}"
    candidates = manager._docker.containers.list(all=True, filters={"name": name})
    return [c for c in candidates if c.name == name]


def _assert_executes(manager: KernelManager, kernel_id: str, node_id: int) -> None:
    # A just-adopted container can drop the first connection while the kernel
    # app finishes booting; retry briefly before judging liveness.
    last_exc: Exception | None = None
    for _ in range(3):
        try:
            result = manager.execute_sync(
                kernel_id,
                ExecuteRequest(
                    node_id=node_id,
                    code="x = 1 + 1",
                    input_paths={},
                    output_dir=f"/shared/test_race_{node_id}",
                ),
            )
            assert result.success, result.error
            return
        except (httpx.HTTPError, OSError) as exc:
            last_exc = exc
            time.sleep(2)
    raise AssertionError(f"kernel '{kernel_id}' did not execute after adoption: {last_exc}")


class TestConcurrentKernelStart:
    def test_concurrent_cold_starts_create_one_container(self, kernel_manager: tuple[KernelManager, str]):
        """The incident shape: N workers racing to start the same stopped kernel
        must yield exactly one container and zero failures."""
        manager, kernel_id = kernel_manager
        kernel = manager._kernels[kernel_id]
        manager._cleanup_container(kernel_id)
        kernel.container_id = None
        kernel.state = KernelState.STOPPED

        n = 6
        barrier = threading.Barrier(n)
        errors: list[BaseException] = []

        def _start() -> None:
            barrier.wait()
            try:
                manager.start_kernel_sync(kernel_id)
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=_start) for _ in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert kernel.state == KernelState.IDLE
        assert len(_containers_named(manager, kernel_id)) == 1
        _assert_executes(manager, kernel_id, node_id=9101)

    def test_name_conflict_adopts_running_container(self, kernel_manager: tuple[KernelManager, str]):
        """A manager that lost track of a running container (crashed previous
        core) must adopt it on the 409 instead of failing."""
        manager, kernel_id = kernel_manager
        manager.start_kernel_sync(kernel_id)
        kernel = manager._kernels[kernel_id]
        live_id = kernel.container_id
        assert live_id is not None

        kernel.state = KernelState.STOPPED
        kernel.container_id = None

        result = manager.start_kernel_sync(kernel_id)

        assert result.state == KernelState.IDLE
        assert kernel.container_id == live_id
        assert len(_containers_named(manager, kernel_id)) == 1
        _assert_executes(manager, kernel_id, node_id=9102)

    def test_name_conflict_starts_stopped_container(self, kernel_manager: tuple[KernelManager, str]):
        """A stopped leftover occupying the name is started (or replaced) —
        never a hard 409 failure."""
        manager, kernel_id = kernel_manager
        manager.start_kernel_sync(kernel_id)
        kernel = manager._kernels[kernel_id]
        manager._docker.containers.get(kernel.container_id).stop(timeout=10)

        kernel.state = KernelState.STOPPED
        kernel.container_id = None

        result = manager.start_kernel_sync(kernel_id)

        assert result.state == KernelState.IDLE
        assert kernel.container_id is not None
        assert len(_containers_named(manager, kernel_id)) == 1
        _assert_executes(manager, kernel_id, node_id=9103)


class TestManagerBootWithLeftovers:
    def test_fresh_manager_boot_adopts_running_container(self, kernel_manager: tuple[KernelManager, str]):
        """Crash-loop regression: constructing a manager while a named kernel
        container and its DB row already exist must not raise, and must adopt
        the running container instead of recreating it."""
        manager, kernel_id = kernel_manager
        manager.start_kernel_sync(kernel_id)
        live_id = manager._kernels[kernel_id].container_id

        fresh = KernelManager(shared_volume_path=manager.shared_volume_path)

        kernel = fresh._kernels.get(kernel_id)
        assert kernel is not None
        assert kernel.state == KernelState.IDLE
        assert kernel.container_id == live_id
        assert len(_containers_named(fresh, kernel_id)) == 1
        assert fresh.start_kernel_sync(kernel_id).state == KernelState.IDLE
