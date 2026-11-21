"""Regression tests for the parallel kernel-start race: single-flight start,
409 create-or-adopt convergence, per-kernel execution serialization, build
dedup, resolver cleanup, and startup reconcile. Docker is mocked throughout —
no daemon required."""

from __future__ import annotations

import asyncio
import threading
import time
from unittest.mock import MagicMock, patch

import docker.errors
import httpx
import pytest

from flowfile_core.kernel import manager as kernel_manager
from flowfile_core.kernel.manager import KernelManager
from flowfile_core.kernel.models import ExecuteRequest, ImageFlavour, KernelInfo, KernelState


@pytest.fixture(autouse=True)
def _fast_converge(monkeypatch):
    monkeypatch.setattr(kernel_manager, "_CONVERGE_DELAY_S", 0)


def _bare_manager() -> KernelManager:
    """Construct a KernelManager with a mocked docker client, no real init."""
    with patch.object(KernelManager, "__init__", lambda self, *a, **kw: None):
        mgr = KernelManager.__new__(KernelManager)
        mgr._docker = MagicMock()
        mgr._core_instance_id = "test-core-id"
        mgr._runtime_id = "test-runtime-id"
        mgr._started_here = set()
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
        mgr._exec_locks = {}
        mgr._exec_locks_lock = threading.Lock()
        mgr._active_execs = {}
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


def _fake_container(
    container_id: str = "c-1",
    status: str = "running",
    host_port: str | None = None,
    image_id: str = "sha256:desired",
):
    container = MagicMock()
    container.id = container_id
    container.short_id = container_id[:10]
    container.status = status
    ports = {"9999/tcp": [{"HostPort": host_port}]} if host_port else {}
    # Older than the GC grace window, so these stay reapable by default.
    container.attrs = {
        "NetworkSettings": {"Ports": ports},
        "Image": image_id,
        "Created": "2020-01-01T00:00:00.000000000Z",
    }
    container.labels = {"flowfile_core_instance": "test-core-id"}
    return container


def _start_ready_manager(kernel: KernelInfo, *, real_tokens: bool = False) -> KernelManager:
    """Manager wired so _start_kernel_inner runs without touching env/health.

    Its kernel has no row, and a kernel does not start without a token, so the token calls are stubbed;
    ``real_tokens`` keeps them, for a test that gave the kernel a row."""
    mgr = _bare_manager()
    mgr._kernels[kernel.id] = kernel
    mgr._kernel_owners[kernel.id] = 1
    if not real_tokens:
        mgr._kernel_run = MagicMock(return_value=("run-id", "run-token"))
        mgr._begin_kernel_run = MagicMock(return_value=True)
    mgr._build_kernel_env = MagicMock(return_value={})
    mgr._build_run_kwargs = MagicMock(return_value={"name": f"flowfile-kernel-{kernel.id}"})
    mgr._wait_for_healthy_sync = MagicMock()
    mgr._allocate_port = MagicMock(return_value=19099)
    mgr._docker.images.get.return_value = MagicMock(id="sha256:desired")
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
        existing = _fake_container("adopted-2", status="exited", host_port="19007")
        mgr._docker.containers.run.side_effect = _api_error(409)
        mgr._docker.containers.get.return_value = existing

        result = mgr.start_kernel_sync("ml")

        assert result.state == KernelState.IDLE
        assert kernel.container_id == "adopted-2"
        assert kernel.port == 19007
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

    def test_409_replaces_stale_image_container(self):
        """A holder built from an older image (e.g. derived tag rebuilt after a
        package edit) must be replaced, not adopted."""
        kernel = _kernel()
        mgr = _start_ready_manager(kernel)
        stale = _fake_container("stale-img", status="running", image_id="sha256:old")
        fresh = _fake_container("fresh-4")
        mgr._docker.containers.run.side_effect = [_api_error(409), fresh]
        mgr._docker.containers.get.return_value = stale

        result = mgr.start_kernel_sync("ml")

        assert result.state == KernelState.IDLE
        assert kernel.container_id == "fresh-4"
        stale.remove.assert_called_once_with(force=True)
        stale.start.assert_not_called()

    def test_409_notfound_window_converges_after_retries(self):
        """The incident shape: create 409s while get-by-name 404s (removal or
        create in progress elsewhere). The loop must retry until the name frees."""
        kernel = _kernel()
        mgr = _start_ready_manager(kernel)
        fresh = _fake_container("fresh-5")
        mgr._docker.containers.run.side_effect = [_api_error(409), _api_error(409), fresh]
        mgr._docker.containers.get.side_effect = docker.errors.NotFound("removing")

        result = mgr.start_kernel_sync("ml")

        assert result.state == KernelState.IDLE
        assert kernel.container_id == "fresh-5"
        assert mgr._docker.containers.run.call_count == 3

    def test_converge_exhaustion_raises_and_sets_error(self):
        kernel = _kernel()
        mgr = _start_ready_manager(kernel)
        mgr._docker.containers.run.side_effect = _api_error(409)
        mgr._docker.containers.get.side_effect = docker.errors.NotFound("still held")

        with pytest.raises(docker.errors.DockerException, match="Could not start or adopt"):
            mgr.start_kernel_sync("ml")

        assert kernel.state == KernelState.ERROR
        assert mgr._docker.containers.run.call_count == kernel_manager._CONVERGE_ATTEMPTS

    def test_409_replaces_portless_running_holder_local_mode(self):
        """A running holder with no published host port is unreachable in local
        mode — adopting it would burn the whole health timeout."""
        kernel = _kernel(port=None)
        mgr = _start_ready_manager(kernel)
        holder = _fake_container("portless-1", status="running")
        fresh = _fake_container("fresh-6", host_port="19012")
        mgr._docker.containers.run.side_effect = [_api_error(409), fresh]
        mgr._docker.containers.get.return_value = holder

        result = mgr.start_kernel_sync("ml")

        assert result.state == KernelState.IDLE
        assert kernel.container_id == "fresh-6"
        holder.remove.assert_called_once_with(force=True)


class _FakeResponse:
    def __init__(self, payload: dict):
        self._payload = payload

    def raise_for_status(self) -> None:
        pass

    def json(self) -> dict:
        return self._payload


class _RecordingTransport:
    """Stands in for httpx.Client; records concurrent in-flight posts."""

    def __init__(self):
        self._lock = threading.Lock()
        self._inflight = 0
        self.max_inflight = 0
        self.calls = 0
        self.behavior = None  # optional callable overriding the default response

    def client_factory(self):
        transport = self

        class _Client:
            def __init__(self, *args, **kwargs):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def post(self, url, json=None):
                return transport._post()

        return _Client

    def _post(self):
        with self._lock:
            self._inflight += 1
            self.calls += 1
            self.max_inflight = max(self.max_inflight, self._inflight)
        try:
            if self.behavior is not None:
                return self.behavior()
            time.sleep(0.05)
            return _FakeResponse({"success": True})
        finally:
            with self._lock:
                self._inflight -= 1


def _exec_ready_manager(kernel: KernelInfo) -> KernelManager:
    mgr = _start_ready_manager(kernel)
    mgr._check_oom_killed = MagicMock(return_value=False)
    mgr._is_container_running = MagicMock(return_value=True)
    return mgr


def _request() -> ExecuteRequest:
    return ExecuteRequest(node_id=1, code="x = 1", source_registration_id=1)


class TestExecuteSerialization:
    def test_concurrent_execute_sync_serialized(self, monkeypatch):
        """One kernel runs one cell at a time — the runtime execs cells into a
        shared namespace, so concurrent posts would race imports/globals."""
        kernel = _kernel(state=KernelState.IDLE)
        mgr = _exec_ready_manager(kernel)
        transport = _RecordingTransport()
        monkeypatch.setattr(kernel_manager.httpx, "Client", transport.client_factory())

        n = 6
        barrier = threading.Barrier(n)
        results: list = []

        def _run() -> None:
            barrier.wait()
            results.append(mgr.execute_sync("ml", _request()))

        threads = [threading.Thread(target=_run) for _ in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(results) == n
        assert all(r.success for r in results)
        assert transport.calls == n
        assert transport.max_inflight == 1
        mgr._docker.containers.run.assert_not_called()

    def test_cold_start_plus_execute_one_create_serial_posts(self, monkeypatch):
        kernel = _kernel()
        mgr = _exec_ready_manager(kernel)
        mgr._docker.containers.run.return_value = _fake_container("c-1", host_port="19001")
        transport = _RecordingTransport()
        monkeypatch.setattr(kernel_manager.httpx, "Client", transport.client_factory())

        n = 6
        barrier = threading.Barrier(n)
        results: list = []

        def _run() -> None:
            barrier.wait()
            results.append(mgr.execute_sync("ml", _request()))

        threads = [threading.Thread(target=_run) for _ in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert all(r.success for r in results)
        assert mgr._docker.containers.run.call_count == 1
        assert transport.max_inflight == 1

    def test_cancel_while_waiting_returns_without_interrupt(self, monkeypatch):
        """A waiter that cancels before acquiring the lock has submitted nothing
        — interrupting would kill the other node's running cell."""
        kernel = _kernel(state=KernelState.IDLE)
        mgr = _exec_ready_manager(kernel)
        mgr.interrupt_execution_sync = MagicMock(return_value=True)
        transport = _RecordingTransport()
        started = threading.Event()
        release = threading.Event()

        def _blocking():
            started.set()
            release.wait(timeout=10)
            return _FakeResponse({"success": True})

        transport.behavior = _blocking
        monkeypatch.setattr(kernel_manager.httpx, "Client", transport.client_factory())

        holder_result: list = []
        t = threading.Thread(target=lambda: holder_result.append(mgr.execute_sync("ml", _request())))
        t.start()
        assert started.wait(timeout=5)

        cancel = threading.Event()
        cancel.set()
        result = mgr.execute_sync("ml", _request(), cancel_event=cancel)

        assert result.success is False
        assert "cancelled" in (result.error or "")
        mgr.interrupt_execution_sync.assert_not_called()
        assert transport.calls == 1

        release.set()
        t.join(timeout=5)
        assert holder_result and holder_result[0].success

    def test_timeout_interrupts_and_releases_lock(self, monkeypatch):
        kernel = _kernel(state=KernelState.IDLE)
        mgr = _exec_ready_manager(kernel)
        mgr.interrupt_execution_sync = MagicMock(return_value=True)
        transport = _RecordingTransport()
        attempts = [0]

        def _behavior():
            attempts[0] += 1
            if attempts[0] == 1:
                raise httpx.ReadTimeout("cell overran")
            return _FakeResponse({"success": True})

        transport.behavior = _behavior
        monkeypatch.setattr(kernel_manager.httpx, "Client", transport.client_factory())

        request = _request()
        result = mgr.execute_sync("ml", request)
        assert result.success is False
        assert f"{kernel_manager._CELL_EXECUTION_TIMEOUT:.0f}s" in (result.error or "")
        mgr.interrupt_execution_sync.assert_called_once_with("ml", request.exec_token)

        follow_up = mgr.execute_sync("ml", _request())
        assert follow_up.success

    def test_kernel_death_preserves_container_id(self, monkeypatch):
        """The death path must keep the container reference so the next start's
        tracked cleanup replaces the name-holder instead of 409ing."""
        kernel = _kernel(state=KernelState.IDLE)
        kernel.container_id = "c-dead"
        mgr = _exec_ready_manager(kernel)
        mgr._is_container_running = MagicMock(return_value=False)
        transport = _RecordingTransport()

        def _behavior():
            raise httpx.ConnectError("kernel down")

        transport.behavior = _behavior
        monkeypatch.setattr(kernel_manager.httpx, "Client", transport.client_factory())

        result = mgr.execute_sync("ml", _request())

        assert result.success is False
        assert kernel.state == KernelState.STOPPED
        assert kernel.container_id == "c-dead"

    def test_async_execute_delegates(self, monkeypatch):
        kernel = _kernel(state=KernelState.IDLE)
        mgr = _exec_ready_manager(kernel)
        transport = _RecordingTransport()
        monkeypatch.setattr(kernel_manager.httpx, "Client", transport.client_factory())

        result = asyncio.run(mgr.execute("ml", _request()))

        assert result.success
        assert transport.calls == 1


class TestDerivedImageBuildDedup:
    def test_concurrent_builds_run_once(self):
        kernel = _kernel(packages=["matplotlib"])
        mgr = _bare_manager()
        mgr._kernels["ml"] = kernel

        built = threading.Event()
        base_image = kernel_manager._resolve_image(kernel.image_flavour, None)

        def _images_get(tag):
            if tag.startswith("flowfile-kernel-derived-"):
                if not built.is_set():
                    raise docker.errors.ImageNotFound(tag)
                # A built derived image carries the base it was baked on; waiters must reuse it.
                derived = MagicMock()
                derived.labels = {kernel_manager._IMAGE_LABEL_BASE_IMAGE: base_image}
                return derived
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

    def test_reconcile_skips_creating_kernel(self, monkeypatch):
        """A CREATING kernel has no DB row yet — reconcile must never prune it."""
        from contextlib import contextmanager

        mgr = _bare_manager()
        mgr._kernels["ml"] = _kernel(state=KernelState.CREATING)
        mgr._kernel_owners["ml"] = 1

        @contextmanager
        def _fake_db():
            yield None

        monkeypatch.setattr("flowfile_core.database.connection.get_db_context", _fake_db)
        monkeypatch.setattr("flowfile_core.kernel.persistence.get_all_kernels", lambda db: [])

        mgr.reconcile_configs_from_db(owner_id=1)
        assert mgr._kernels["ml"].state == KernelState.CREATING

        mgr.reconcile_configs_from_db()
        assert mgr._kernels["ml"].state == KernelState.CREATING
        assert mgr._kernel_owners["ml"] == 1


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


class TestAdoptedContainersToken:
    """A container found under the kernel's name takes calls with the token it was started with, so core signs its
    calls with that one, whatever token the kernel's row names by now."""

    @pytest.fixture
    def kernel_row(self):
        """A kernel row of user 1 in the test database, in run ``run-0``; yields its id."""
        import uuid

        from flowfile_core.database import models as db_models
        from flowfile_core.database.connection import get_db_context

        kernel_id = f"adopt-{uuid.uuid4().hex[:8]}"
        with get_db_context() as db:
            db.add(db_models.Kernel(id=kernel_id, name="k", user_id=1, token_id="run-0"))
            db.commit()
        yield kernel_id
        with get_db_context() as db:
            db.query(db_models.Kernel).filter(db_models.Kernel.id == kernel_id).delete()
            db.commit()

    @staticmethod
    def _started_with(container, token: str):
        container.attrs["Config"] = {"Env": ["KERNEL_PACKAGES=", f"FLOWFILE_KERNEL_TOKEN={token}"]}
        return container

    @staticmethod
    def _token_of(kernel_id: str, token_id: str) -> str:
        from flowfile_core.auth.jwt import create_kernel_token

        return create_kernel_token(1, kernel_id, token_id)

    def test_409_adopted_container_is_called_with_the_token_it_was_started_with(self, kernel_row):
        kernel = _kernel(kernel_row)
        mgr = _start_ready_manager(kernel, real_tokens=True)
        existing = self._started_with(_fake_container("adopted-t", host_port="19005"), "token-of-its-start")
        mgr._docker.containers.run.side_effect = _api_error(409)
        mgr._docker.containers.get.return_value = existing

        mgr.start_kernel_sync(kernel_row)

        assert kernel.container_id == "adopted-t"
        assert mgr._call_headers(kernel) == {"Authorization": "Bearer token-of-its-start"}

    def test_a_container_found_running_keeps_its_run(self, kernel_row):
        """Another core process may have a cell running in it, which signs its calls to core with that run's token."""
        kernel = _kernel(kernel_row)
        mgr = _start_ready_manager(kernel, real_tokens=True)
        mgr._docker.containers.run.side_effect = _api_error(409)
        mgr._docker.containers.get.return_value = _fake_container("adopted-r", host_port="19005")

        mgr.start_kernel_sync(kernel_row)

        assert mgr.kernel_token(kernel_row) == self._token_of(kernel_row, "run-0")

    def test_a_container_found_running_without_a_run_gets_one(self, kernel_row):
        kernel = _kernel(kernel_row)
        mgr = _start_ready_manager(kernel, real_tokens=True)
        mgr._end_kernel_token(kernel_row)
        mgr._docker.containers.run.side_effect = _api_error(409)
        mgr._docker.containers.get.return_value = _fake_container("adopted-n", host_port="19005")

        mgr.start_kernel_sync(kernel_row)

        assert mgr.kernel_token(kernel_row) is not None

    @pytest.mark.parametrize("found_stopped", [False, True], ids=["a new container", "a stopped one started again"])
    def test_a_container_that_was_not_running_begins_a_run(self, kernel_row, found_stopped):
        kernel = _kernel(kernel_row)
        mgr = _start_ready_manager(kernel, real_tokens=True)
        if found_stopped:
            mgr._docker.containers.run.side_effect = _api_error(409)
            mgr._docker.containers.get.return_value = _fake_container("stopped-1", status="exited", host_port="19005")
        else:
            mgr._docker.containers.run.return_value = _fake_container("new-1", host_port="19005")

        mgr.start_kernel_sync(kernel_row)

        started = mgr.kernel_token(kernel_row)
        assert started not in (None, self._token_of(kernel_row, "run-0")), "the run before it is over"
        assert mgr._build_kernel_env.call_args.args[2] == started, "the container is started with the run's token"

    def test_a_run_begins_only_once_its_container_exists(self, kernel_row):
        """Until then the run before it stays the kernel's, so a start that obtains no container ends nothing."""
        kernel = _kernel(kernel_row)
        mgr = _start_ready_manager(kernel, real_tokens=True)
        seen: list[str | None] = []

        def _run(*args, **kwargs):
            seen.append(mgr.kernel_token(kernel_row))
            raise _api_error(500, "no room")

        mgr._docker.containers.run.side_effect = _run

        with pytest.raises(docker.errors.APIError):
            mgr.start_kernel_sync(kernel_row)

        assert seen == [self._token_of(kernel_row, "run-0")]
        assert mgr.kernel_token(kernel_row) == self._token_of(kernel_row, "run-0")

    def test_a_failed_start_ends_the_run_of_the_container_it_removes(self, kernel_row):
        kernel = _kernel(kernel_row)
        mgr = _start_ready_manager(kernel, real_tokens=True)
        created = _fake_container("unhealthy-1", host_port="19005")
        mgr._docker.containers.run.return_value = created
        mgr._docker.containers.get.return_value = created
        mgr._wait_for_healthy_sync.side_effect = TimeoutError("never healthy")

        with pytest.raises(TimeoutError):
            mgr.start_kernel_sync(kernel_row)

        created.remove.assert_called_once_with(force=True)
        assert mgr.kernel_token(kernel_row) is None

    def test_a_kernel_core_can_mint_no_token_for_does_not_start(self):
        """Started without one it would take every call on its port, and core would refuse each of its own."""
        kernel = _kernel("no-row")
        mgr = _start_ready_manager(kernel, real_tokens=True)

        with pytest.raises(RuntimeError, match="Could not mint a token"):
            mgr.start_kernel_sync("no-row")

        assert kernel.state == KernelState.ERROR
        mgr._docker.containers.run.assert_not_called()

    def test_a_kernel_whose_token_cannot_be_recorded_does_not_start(self, kernel_row):
        kernel = _kernel(kernel_row)
        mgr = _start_ready_manager(kernel, real_tokens=True)
        created = _fake_container("unrecorded-1", host_port="19005")
        mgr._docker.containers.run.return_value = created
        mgr._docker.containers.get.return_value = created
        mgr._begin_kernel_run = MagicMock(return_value=False)

        with pytest.raises(RuntimeError, match="Could not record the token"):
            mgr.start_kernel_sync(kernel_row)

        assert kernel.state == KernelState.ERROR and kernel.container_id is None
        created.remove.assert_called_once_with(force=True)

    def test_a_kernel_found_running_at_startup_is_called_with_the_token_it_was_started_with(self, kernel_row):
        kernel = _kernel(kernel_row)
        mgr = _start_ready_manager(kernel, real_tokens=True)
        mgr._end_kernel_token(kernel_row)
        running = self._started_with(_fake_container("running-t", host_port="19005"), "token-of-its-start")
        running.name = f"flowfile-kernel-{kernel_row}"
        mgr._docker.containers.list.return_value = [running]

        mgr._reclaim_running_containers(prune_orphans=False)

        assert kernel.state == KernelState.IDLE
        assert mgr._call_headers(kernel) == {"Authorization": "Bearer token-of-its-start"}
        assert mgr.kernel_token(kernel_row) is not None, "a kernel found running has a run"

    def test_a_kernel_found_running_at_startup_keeps_its_run(self, kernel_row, monkeypatch):
        """Also in a process that cannot mint a token just then: what decides is the row, never a minted token."""
        from flowfile_core.auth import jwt as auth_jwt

        kernel = _kernel(kernel_row)
        mgr = _start_ready_manager(kernel, real_tokens=True)
        running = _fake_container("running-k", host_port="19005")
        running.name = f"flowfile-kernel-{kernel_row}"
        mgr._docker.containers.list.return_value = [running]

        with monkeypatch.context() as patched:
            patched.setattr(auth_jwt, "create_kernel_token", MagicMock(side_effect=RuntimeError("no key")))
            mgr._reclaim_running_containers(prune_orphans=False)

        assert mgr.kernel_token(kernel_row) == self._token_of(kernel_row, "run-0")

    @pytest.mark.parametrize("leftover", [False, True], ids=["no container", "a stopped container"])
    def test_startup_ends_the_run_of_a_kernel_with_no_running_container(self, kernel_row, leftover):
        """A core that was killed never ended it, and the kernel's token has no expiry of its own."""
        kernel = _kernel(kernel_row)
        mgr = _start_ready_manager(kernel, real_tokens=True)
        stopped = _fake_container("stale-t", status="exited")
        stopped.name = f"flowfile-kernel-{kernel_row}"
        mgr._docker.containers.list.return_value = [stopped] if leftover else []

        mgr._reclaim_running_containers(prune_orphans=False)

        assert mgr.kernel_token(kernel_row) is None

    def test_startup_leaves_a_run_begun_while_it_looked(self, kernel_row):
        """Another core process starts the kernel after this one listed the containers: that run is not over."""
        kernel = _kernel(kernel_row)
        mgr = _start_ready_manager(kernel, real_tokens=True)

        def _list(**kwargs):
            mgr._begin_kernel_run(kernel_row, "run-1")
            return []

        mgr._docker.containers.list.side_effect = _list

        mgr._reclaim_running_containers(prune_orphans=False)

        assert mgr.kernel_token(kernel_row) == self._token_of(kernel_row, "run-1")

    def test_a_removed_containers_token_is_forgotten(self, kernel_row):
        kernel = _kernel(kernel_row)
        mgr = _start_ready_manager(kernel, real_tokens=True)
        mgr._docker.containers.run.side_effect = [
            self._started_with(_fake_container("first"), "first-token"),
            self._started_with(_fake_container("second"), "second-token"),
        ]

        mgr.start_kernel_sync(kernel_row)
        assert mgr._call_headers(kernel) == {"Authorization": "Bearer first-token"}
        asyncio.run(mgr.stop_kernel(kernel_row))
        assert "first" not in mgr._held_tokens() and mgr._call_headers(kernel) == {}

        mgr.start_kernel_sync(kernel_row)
        assert mgr._call_headers(kernel) == {"Authorization": "Bearer second-token"}
