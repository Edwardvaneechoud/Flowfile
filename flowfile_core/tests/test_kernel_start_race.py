"""Regression tests for the parallel kernel-start race: single-flight start,
409-adopt, build dedup, resolver cleanup, and startup reconcile. Docker is
mocked throughout — no daemon required."""

from __future__ import annotations

import threading
from unittest.mock import MagicMock, patch

import docker.errors
import pytest

from flowfile_core.kernel import manager as kernel_manager
from flowfile_core.kernel.manager import KernelManager
from flowfile_core.kernel.models import ImageFlavour, KernelInfo, KernelState


def _bare_manager() -> KernelManager:
    """Construct a KernelManager with a mocked docker client, no real init."""
    with patch.object(KernelManager, "__init__", lambda self, *a, **kw: None):
        mgr = KernelManager.__new__(KernelManager)
        mgr._docker = MagicMock()
        mgr._core_instance_id = "test-core-id"
        mgr._kernels = {}
        mgr._kernel_owners = {}
        mgr._kernels_lock = threading.RLock()
        mgr._scratch_flow_ids = {}
        mgr._scratch_flow_lock = threading.Lock()
        mgr._shared_volume = "/tmp/test"
        mgr._catalog_tables_dir = "/__catalog_tables_unused__"
        mgr._docker_network = None
        mgr._kernel_volume = None
        mgr._kernel_volume_type = None
        mgr._kernel_mount_target = None
        mgr._catalog_volume = None
        mgr._catalog_volume_type = None
        mgr._catalog_mount_target = None
        mgr._pull_state = {}
        mgr._pull_state_lock = threading.Lock()
        mgr._start_flights = {}
        mgr._start_flights_lock = threading.Lock()
        mgr._build_locks = {}
        mgr._build_locks_lock = threading.Lock()
    return mgr


def _kernel(
    kernel_id: str = "ml",
    state: KernelState = KernelState.STOPPED,
    packages: list[str] | None = None,
    port: int | None = 19001,
) -> KernelInfo:
    return KernelInfo(
        id=kernel_id,
        name=f"test-{kernel_id}",
        state=state,
        port=port,
        packages=packages or [],
        image_flavour=ImageFlavour.BASE,
    )


def _api_error(status_code: int, msg: str = "boom") -> docker.errors.APIError:
    resp = MagicMock()
    resp.status_code = status_code
    return docker.errors.APIError(msg, response=resp, explanation=msg)


def _fake_container(container_id: str = "c-1", status: str = "running", host_port: str | None = None):
    container = MagicMock()
    container.id = container_id
    container.short_id = container_id[:10]
    container.status = status
    ports = {"9999/tcp": [{"HostPort": host_port}]} if host_port else {}
    container.attrs = {"NetworkSettings": {"Ports": ports}}
    return container


def _start_ready_manager(kernel: KernelInfo) -> KernelManager:
    """Manager wired so _start_kernel_inner runs without touching env/health."""
    mgr = _bare_manager()
    mgr._kernels[kernel.id] = kernel
    mgr._kernel_owners[kernel.id] = 1
    mgr._build_kernel_env = MagicMock(return_value={})
    mgr._build_run_kwargs = MagicMock(return_value={"name": f"flowfile-kernel-{kernel.id}"})
    mgr._wait_for_healthy_sync = MagicMock()
    mgr._allocate_port = MagicMock(return_value=19099)
    return mgr


class TestSingleFlightStart:
    def test_concurrent_ensure_creates_exactly_one_container(self):
        """8 threads racing _ensure_running_sync must produce exactly one
        containers.run call — the 409 in the side_effect must never be hit."""
        kernel = _kernel()
        mgr = _start_ready_manager(kernel)
        mgr._docker.containers.run.side_effect = [_fake_container()] + [_api_error(409)] * 7

        n = 8
        barrier = threading.Barrier(n)
        errors: list[BaseException] = []
        results: list[KernelInfo] = []

        def _start() -> None:
            barrier.wait()
            try:
                results.append(mgr.start_kernel_sync("ml"))
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=_start) for _ in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert mgr._docker.containers.run.call_count == 1
        assert kernel.state == KernelState.IDLE
        assert kernel.container_id == "c-1"
        assert len(results) == n
        assert all(r is kernel for r in results)
        assert not mgr._start_flights

    def test_followers_receive_leader_error_without_retry(self):
        """A failed start is reported to every concurrent waiter; nobody retries
        inside the same flight."""
        kernel = _kernel()
        mgr = _start_ready_manager(kernel)

        release = threading.Event()

        def _blocking_fail(*args, **kwargs):
            release.wait(timeout=10)
            raise _api_error(500)

        mgr._docker.containers.run.side_effect = _blocking_fail

        waiter_count = [0]
        real_wait = mgr._wait_for_flight

        def _counting_wait(kernel_id, flight):
            waiter_count[0] += 1
            return real_wait(kernel_id, flight)

        mgr._wait_for_flight = _counting_wait

        n = 4
        errors: list[BaseException] = []
        barrier = threading.Barrier(n)

        def _ensure() -> None:
            barrier.wait()
            try:
                mgr._ensure_running_sync("ml")
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=_ensure) for _ in range(n)]
        for t in threads:
            t.start()
        deadline = 5.0
        while waiter_count[0] < n - 1 and deadline > 0:
            threading.Event().wait(0.01)
            deadline -= 0.01
        release.set()
        for t in threads:
            t.join()

        assert waiter_count[0] == n - 1
        assert len(errors) == n
        assert all(isinstance(e, docker.errors.APIError) for e in errors)
        assert mgr._docker.containers.run.call_count == 1
        assert kernel.state == KernelState.ERROR
        assert not mgr._start_flights

    def test_second_wave_after_error_retries_cleanly(self):
        """A failed flight must not poison the next attempt."""
        kernel = _kernel()
        mgr = _start_ready_manager(kernel)
        mgr._docker.containers.run.side_effect = [_api_error(500), _fake_container("c-2")]

        with pytest.raises(docker.errors.APIError):
            mgr.start_kernel_sync("ml")
        assert kernel.state == KernelState.ERROR

        result = mgr.start_kernel_sync("ml")
        assert result.state == KernelState.IDLE
        assert kernel.container_id == "c-2"

    def test_idle_kernel_short_circuits_without_docker(self):
        kernel = _kernel(state=KernelState.IDLE)
        mgr = _start_ready_manager(kernel)
        assert mgr.start_kernel_sync("ml") is kernel
        mgr._docker.containers.run.assert_not_called()

    def test_flight_wait_deadline_raises(self, monkeypatch):
        from concurrent.futures import Future

        kernel = _kernel()
        kernel.health_timeout = 0
        mgr = _bare_manager()
        mgr._kernels["ml"] = kernel
        monkeypatch.setattr(kernel_manager, "_START_FLIGHT_GRACE", 0)

        with pytest.raises(TimeoutError, match="Timed out waiting"):
            mgr._wait_for_flight("ml", Future())

    def test_flight_wait_propagates_leader_timeout(self):
        from concurrent.futures import Future

        kernel = _kernel()
        mgr = _bare_manager()
        mgr._kernels["ml"] = kernel
        flight = Future()
        flight.set_exception(TimeoutError("health poll gave up"))

        with pytest.raises(TimeoutError, match="health poll gave up"):
            mgr._wait_for_flight("ml", flight)


class TestConflictAdoption:
    def test_409_adopts_running_container(self):
        kernel = _kernel(port=None)
        mgr = _start_ready_manager(kernel)
        existing = _fake_container("adopted-1", status="running", host_port="19005")
        mgr._docker.containers.run.side_effect = _api_error(409)
        mgr._docker.containers.get.return_value = existing

        result = mgr.start_kernel_sync("ml")

        assert result.state == KernelState.IDLE
        assert kernel.container_id == "adopted-1"
        assert kernel.port == 19005
        existing.start.assert_not_called()
        existing.remove.assert_not_called()

    def test_409_adopts_running_container_dind_mode(self):
        kernel = _kernel(port=None)
        mgr = _start_ready_manager(kernel)
        mgr._kernel_volume = "flowfile-internal-storage"
        existing = _fake_container("adopted-dind", status="running")
        mgr._docker.containers.run.side_effect = _api_error(409)
        mgr._docker.containers.get.return_value = existing

        result = mgr.start_kernel_sync("ml")

        assert result.state == KernelState.IDLE
        assert kernel.container_id == "adopted-dind"
        assert kernel.port is None

    def test_409_starts_stopped_container(self):
        kernel = _kernel()
        mgr = _start_ready_manager(kernel)
        existing = _fake_container("adopted-2", status="exited")
        mgr._docker.containers.run.side_effect = _api_error(409)
        mgr._docker.containers.get.return_value = existing

        result = mgr.start_kernel_sync("ml")

        assert result.state == KernelState.IDLE
        assert kernel.container_id == "adopted-2"
        existing.start.assert_called_once()

    def test_409_replaces_unstartable_container(self):
        kernel = _kernel()
        mgr = _start_ready_manager(kernel)
        broken = _fake_container("broken-1", status="dead")
        broken.start.side_effect = _api_error(500, "cannot start")
        fresh = _fake_container("fresh-1")
        mgr._docker.containers.run.side_effect = [_api_error(409), fresh]
        mgr._docker.containers.get.return_value = broken

        result = mgr.start_kernel_sync("ml")

        assert result.state == KernelState.IDLE
        assert kernel.container_id == "fresh-1"
        broken.remove.assert_called_once_with(force=True)

    def test_409_with_vanished_container_recreates_once(self):
        kernel = _kernel()
        mgr = _start_ready_manager(kernel)
        fresh = _fake_container("fresh-2")
        mgr._docker.containers.run.side_effect = [_api_error(409), fresh]
        mgr._docker.containers.get.side_effect = docker.errors.NotFound("gone")

        result = mgr.start_kernel_sync("ml")

        assert result.state == KernelState.IDLE
        assert kernel.container_id == "fresh-2"
        assert mgr._docker.containers.run.call_count == 2

    def test_failed_create_removes_nothing(self):
        """The failure path may only remove a container the flight created."""
        kernel = _kernel()
        mgr = _start_ready_manager(kernel)
        mgr._docker.containers.run.side_effect = _api_error(500)

        with pytest.raises(docker.errors.APIError):
            mgr.start_kernel_sync("ml")

        mgr._docker.containers.get.assert_not_called()
        assert kernel.container_id is None

    def test_stale_container_id_cleaned_before_start(self):
        """A reclaim-recorded stopped container is removed before the create —
        this is what prevents the boot-leftover 409."""
        kernel = _kernel()
        kernel.container_id = "stale-1"
        mgr = _start_ready_manager(kernel)
        stale = _fake_container("stale-1", status="exited")
        mgr._docker.containers.get.return_value = stale
        mgr._docker.containers.run.return_value = _fake_container("fresh-3")

        result = mgr.start_kernel_sync("ml")

        assert result.state == KernelState.IDLE
        mgr._docker.containers.get.assert_called_once_with("stale-1")
        stale.remove.assert_called_once_with(force=True)
        assert kernel.container_id == "fresh-3"


class TestDerivedImageBuildDedup:
    def test_concurrent_builds_run_once(self):
        kernel = _kernel(packages=["matplotlib"])
        mgr = _bare_manager()
        mgr._kernels["ml"] = kernel

        built = threading.Event()

        def _images_get(tag):
            if tag.startswith("flowfile-kernel-derived-") and not built.is_set():
                raise docker.errors.ImageNotFound(tag)
            return MagicMock()

        def _images_build(**kwargs):
            built.set()
            return (MagicMock(), iter(()))

        mgr._docker.images.get.side_effect = _images_get
        mgr._docker.images.build.side_effect = _images_build

        n = 4
        barrier = threading.Barrier(n)
        tags: list[str] = []
        errors: list[BaseException] = []

        def _build() -> None:
            barrier.wait()
            try:
                tags.append(mgr._build_derived_image(kernel))
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=_build) for _ in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert mgr._docker.images.build.call_count == 1
        assert len(set(tags)) == 1


class TestResolverContainerCleanup:
    def test_resolver_removed_on_success(self):
        mgr = _bare_manager()
        container = MagicMock()
        container.logs.return_value = b'[{"name": "matplotlib", "version": "3.9.0"}]'
        mgr._docker.containers.create.return_value = container

        resolved = mgr._resolve_installed_versions("tag:1", ["matplotlib"])

        assert [(p.name, p.version) for p in resolved] == [("matplotlib", "3.9.0")]
        container.remove.assert_called_once_with(force=True)
        name = mgr._docker.containers.create.call_args.kwargs["name"]
        assert name.startswith("flowfile-pipresolve-")

    def test_resolver_removed_on_failure(self):
        mgr = _bare_manager()
        container = MagicMock()
        container.wait.side_effect = _api_error(500, "wait exploded")
        mgr._docker.containers.create.return_value = container

        assert mgr._resolve_installed_versions("tag:1", ["matplotlib"]) == []
        container.remove.assert_called_once_with(force=True)

    def test_resolver_removed_on_wait_timeout(self):
        import requests

        mgr = _bare_manager()
        container = MagicMock()
        container.wait.side_effect = requests.exceptions.ReadTimeout("pip list stuck")
        mgr._docker.containers.create.return_value = container

        assert mgr._resolve_installed_versions("tag:1", ["matplotlib"]) == []
        container.remove.assert_called_once_with(force=True)

    def test_resolver_create_failure_is_best_effort(self):
        mgr = _bare_manager()
        mgr._docker.containers.create.side_effect = _api_error(500)
        assert mgr._resolve_installed_versions("tag:1", ["matplotlib"]) == []


class TestReconcileVsFlight:
    def test_reconcile_skips_kernel_with_active_flight(self, monkeypatch):
        """A DB reconcile must not prune a kernel whose start flight is running."""
        from concurrent.futures import Future
        from contextlib import contextmanager

        mgr = _bare_manager()
        kernel = _kernel()
        kernel.container_id = "live-1"
        mgr._kernels["ml"] = kernel
        mgr._kernel_owners["ml"] = 1
        mgr._start_flights["ml"] = Future()

        @contextmanager
        def _fake_db():
            yield None

        monkeypatch.setattr("flowfile_core.database.connection.get_db_context", _fake_db)
        monkeypatch.setattr("flowfile_core.kernel.persistence.get_all_kernels", lambda db: [])

        mgr.reconcile_configs_from_db()
        assert "ml" in mgr._kernels
        mgr._docker.containers.get.assert_not_called()

        mgr._start_flights.pop("ml")
        mgr.reconcile_configs_from_db()
        assert "ml" not in mgr._kernels
        mgr._docker.containers.get.assert_called_once_with("live-1")


class TestStartupReconcile:
    def test_running_container_reclaimed_as_idle(self):
        mgr = _bare_manager()
        kernel = _kernel(port=None)
        mgr._kernels["ml"] = kernel
        container = _fake_container("live-1", status="running", host_port="19010")
        container.name = "flowfile-kernel-ml"
        mgr._docker.containers.list.return_value = [container]

        mgr._reclaim_running_containers()

        assert kernel.state == KernelState.IDLE
        assert kernel.container_id == "live-1"
        assert kernel.port == 19010

    def test_stopped_container_identity_recorded(self):
        """The unclean-shutdown leftover: registered so the first start replaces
        it instead of 409ing."""
        mgr = _bare_manager()
        kernel = _kernel()
        mgr._kernels["ml"] = kernel
        container = _fake_container("stale-9", status="exited")
        container.name = "flowfile-kernel-ml"
        mgr._docker.containers.list.return_value = [container]

        mgr._reclaim_running_containers()

        assert kernel.state == KernelState.STOPPED
        assert kernel.container_id == "stale-9"
        container.remove.assert_not_called()

    def test_orphan_container_removed(self):
        mgr = _bare_manager()
        container = _fake_container("ghost-1", status="running")
        container.name = "flowfile-kernel-ghost"
        mgr._docker.containers.list.return_value = [container]

        mgr._reclaim_running_containers()

        container.stop.assert_called_once()
        container.remove.assert_called_once_with(force=True)

    def test_reclaim_never_raises(self):
        mgr = _bare_manager()
        mgr._docker.containers.list.side_effect = docker.errors.APIError("daemon down")
        mgr._reclaim_running_containers()

    def test_build_orphan_sweep_skips_kernel_containers(self):
        mgr = _bare_manager()
        resolver = _fake_container("res-1", status="exited")
        resolver.name = "flowfile-pipresolve-abc123"
        bake_leftover = _fake_container("bake-1", status="exited")
        bake_leftover.name = "interesting_wilson"
        kernel_container = _fake_container("k-1", status="exited")
        kernel_container.name = "flowfile-kernel-ml"

        def _list(all=True, filters=None):  # noqa: A002
            if "name" in (filters or {}):
                return [resolver]
            return [bake_leftover, kernel_container]

        mgr._docker.containers.list.side_effect = _list

        mgr._remove_orphan_build_containers()

        resolver.remove.assert_called_once_with(force=True)
        bake_leftover.remove.assert_called_once_with(force=True)
        kernel_container.remove.assert_not_called()
