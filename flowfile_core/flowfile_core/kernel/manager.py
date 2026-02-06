import asyncio
import logging
import os
import socket
import time

import docker
import httpx

from flowfile_core.configs.flow_logger import FlowLogger
from flowfile_core.kernel.models import (
    ArtifactPersistenceInfo,
    CleanupRequest,
    CleanupResult,
    ClearNodeArtifactsResult,
    ExecuteRequest,
    ExecuteResult,
    KernelConfig,
    KernelInfo,
    KernelState,
    RecoveryStatus,
)
from shared.storage_config import storage

logger = logging.getLogger(__name__)

_KERNEL_IMAGE = "flowfile-kernel"
_BASE_PORT = 19000
_PORT_RANGE = 1000  # 19000-19999
_HEALTH_TIMEOUT = 120
_HEALTH_POLL_INTERVAL = 2


def _is_port_available(port: int) -> bool:
    """Check whether a TCP port is free on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("127.0.0.1", port))
            return True
        except OSError:
            return False


class KernelManager:
    def __init__(self, shared_volume_path: str | None = None):
        self._docker = docker.from_env()
        self._kernels: dict[str, KernelInfo] = {}
        self._kernel_owners: dict[str, int] = {}  # kernel_id -> user_id
        self._shared_volume = shared_volume_path or str(storage.cache_directory)
        self._restore_kernels_from_db()
        self._reclaim_running_containers()

    @property
    def shared_volume_path(self) -> str:
        return self._shared_volume

    # ------------------------------------------------------------------
    # Database persistence helpers
    # ------------------------------------------------------------------

    def _restore_kernels_from_db(self) -> None:
        """Load persisted kernel configs from the database on startup."""
        try:
            from flowfile_core.database.connection import get_db_context
            from flowfile_core.kernel.persistence import get_all_kernels

            with get_db_context() as db:
                for config, user_id in get_all_kernels(db):
                    if config.id in self._kernels:
                        continue
                    kernel = KernelInfo(
                        id=config.id,
                        name=config.name,
                        state=KernelState.STOPPED,
                        packages=config.packages,
                        memory_gb=config.memory_gb,
                        cpu_cores=config.cpu_cores,
                        gpu=config.gpu,
                    )
                    self._kernels[config.id] = kernel
                    self._kernel_owners[config.id] = user_id
                    logger.info("Restored kernel '%s' for user %d from database", config.id, user_id)
        except Exception as exc:
            logger.warning("Could not restore kernels from database: %s", exc)

    def _persist_kernel(self, kernel: KernelInfo, user_id: int) -> None:
        """Save a kernel record to the database."""
        try:
            from flowfile_core.database.connection import get_db_context
            from flowfile_core.kernel.persistence import save_kernel

            with get_db_context() as db:
                save_kernel(db, kernel, user_id)
        except Exception as exc:
            logger.warning("Could not persist kernel '%s': %s", kernel.id, exc)

    def _remove_kernel_from_db(self, kernel_id: str) -> None:
        """Remove a kernel record from the database."""
        try:
            from flowfile_core.database.connection import get_db_context
            from flowfile_core.kernel.persistence import delete_kernel

            with get_db_context() as db:
                delete_kernel(db, kernel_id)
        except Exception as exc:
            logger.warning("Could not remove kernel '%s' from database: %s", kernel_id, exc)

    # ------------------------------------------------------------------
    # Port allocation
    # ------------------------------------------------------------------

    def _reclaim_running_containers(self) -> None:
        """Discover running flowfile-kernel containers and reclaim their ports."""
        try:
            containers = self._docker.containers.list(
                filters={"name": "flowfile-kernel-", "status": "running"}
            )
        except (docker.errors.APIError, docker.errors.DockerException) as exc:
            logger.warning("Could not list running containers: %s", exc)
            return

        for container in containers:
            name = container.name
            if not name.startswith("flowfile-kernel-"):
                continue
            kernel_id = name[len("flowfile-kernel-"):]

            # Determine which host port is mapped
            port = None
            try:
                bindings = container.attrs["NetworkSettings"]["Ports"].get("9999/tcp")
                if bindings:
                    port = int(bindings[0]["HostPort"])
            except (KeyError, IndexError, TypeError, ValueError):
                pass

            if port is not None and kernel_id in self._kernels:
                # Kernel was restored from DB — update with runtime info
                self._kernels[kernel_id].container_id = container.id
                self._kernels[kernel_id].port = port
                self._kernels[kernel_id].state = KernelState.IDLE
                logger.info(
                    "Reclaimed running kernel '%s' on port %d (container %s)",
                    kernel_id, port, container.short_id,
                )
            elif port is not None and kernel_id not in self._kernels:
                # Orphan container with no DB record — stop it
                logger.warning(
                    "Found orphan kernel container '%s' with no database record, stopping it",
                    kernel_id,
                )
                try:
                    container.stop(timeout=10)
                    container.remove(force=True)
                except Exception as exc:
                    logger.warning("Error stopping orphan container '%s': %s", kernel_id, exc)

    def _allocate_port(self) -> int:
        """Find the next available port in the kernel port range."""
        used_ports = {k.port for k in self._kernels.values() if k.port is not None}
        for port in range(_BASE_PORT, _BASE_PORT + _PORT_RANGE):
            if port not in used_ports and _is_port_available(port):
                return port
        raise RuntimeError(
            f"No available ports in range {_BASE_PORT}-{_BASE_PORT + _PORT_RANGE - 1}"
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def _build_kernel_env(self, kernel_id: str, kernel: KernelInfo) -> dict[str, str]:
        """Build the environment dictionary for a kernel container.

        This centralizes all environment variables passed to kernel containers,
        including Core API connection, authentication, and persistence settings.
        """
        packages_str = " ".join(kernel.packages)
        env = {"KERNEL_PACKAGES": packages_str}
        # FLOWFILE_CORE_URL: how kernel reaches Core API from inside Docker
        core_url = os.environ.get("FLOWFILE_CORE_URL", "http://host.docker.internal:63578")
        env["FLOWFILE_CORE_URL"] = core_url
        # FLOWFILE_INTERNAL_TOKEN: service-to-service auth for kernel → Core
        internal_token = os.environ.get("FLOWFILE_INTERNAL_TOKEN")
        if internal_token:
            env["FLOWFILE_INTERNAL_TOKEN"] = internal_token
        # FLOWFILE_KERNEL_ID: pass kernel ID for lineage tracking
        env["FLOWFILE_KERNEL_ID"] = kernel_id
        # FLOWFILE_HOST_SHARED_DIR: host path for path translation (container has /shared)
        env["FLOWFILE_HOST_SHARED_DIR"] = self._shared_volume
        # Persistence settings from kernel config
        env["KERNEL_ID"] = kernel_id
        env["PERSISTENCE_ENABLED"] = "true" if kernel.persistence_enabled else "false"
        env["PERSISTENCE_PATH"] = "/shared/artifacts"
        env["RECOVERY_MODE"] = kernel.recovery_mode.value
        return env

    async def create_kernel(self, config: KernelConfig, user_id: int) -> KernelInfo:
        if config.id in self._kernels:
            raise ValueError(f"Kernel '{config.id}' already exists")

        port = self._allocate_port()
        kernel = KernelInfo(
            id=config.id,
            name=config.name,
            state=KernelState.STOPPED,
            port=port,
            packages=config.packages,
            memory_gb=config.memory_gb,
            cpu_cores=config.cpu_cores,
            gpu=config.gpu,
            health_timeout=config.health_timeout,
            persistence_enabled=config.persistence_enabled,
            recovery_mode=config.recovery_mode,
        )
        self._kernels[config.id] = kernel
        self._kernel_owners[config.id] = user_id
        self._persist_kernel(kernel, user_id)
        logger.info("Created kernel '%s' on port %d for user %d", config.id, port, user_id)
        return kernel

    async def start_kernel(self, kernel_id: str) -> KernelInfo:
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state == KernelState.IDLE:
            return kernel

        # Verify the kernel image exists before attempting to start
        try:
            self._docker.images.get(_KERNEL_IMAGE)
        except docker.errors.ImageNotFound:
            kernel.state = KernelState.ERROR
            kernel.error_message = (
                f"Docker image '{_KERNEL_IMAGE}' not found. "
                "Please build or pull the kernel image before starting a kernel."
            )
            raise RuntimeError(kernel.error_message)

        # Allocate a port if the kernel doesn't have one yet (e.g. restored from DB)
        if kernel.port is None:
            kernel.port = self._allocate_port()

        kernel.state = KernelState.STARTING
        kernel.error_message = None

        try:
            env = self._build_kernel_env(kernel_id, kernel)
            run_kwargs: dict = {
                "detach": True,
                "name": f"flowfile-kernel-{kernel_id}",
                "ports": {"9999/tcp": kernel.port},
                "volumes": {self._shared_volume: {"bind": "/shared", "mode": "rw"}},
                "environment": env,
                "mem_limit": f"{kernel.memory_gb}g",
                "nano_cpus": int(kernel.cpu_cores * 1e9),
                "extra_hosts": {"host.docker.internal": "host-gateway"},
            }
            container = self._docker.containers.run(_KERNEL_IMAGE, **run_kwargs)
            kernel.container_id = container.id
            await self._wait_for_healthy(kernel_id, timeout=kernel.health_timeout)
            kernel.state = KernelState.IDLE
            logger.info("Kernel '%s' is idle (container %s)", kernel_id, container.short_id)
        except (docker.errors.DockerException, httpx.HTTPError, TimeoutError, OSError) as exc:
            kernel.state = KernelState.ERROR
            kernel.error_message = str(exc)
            logger.error("Failed to start kernel '%s': %s", kernel_id, exc)
            self._cleanup_container(kernel_id)
            raise

        return kernel

    def start_kernel_sync(self, kernel_id: str, flow_logger: FlowLogger | None = None) -> KernelInfo:
        """Synchronous version of start_kernel() for use from non-async code."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state == KernelState.IDLE:
            return kernel

        try:
            self._docker.images.get(_KERNEL_IMAGE)
        except docker.errors.ImageNotFound:
            kernel.state = KernelState.ERROR
            kernel.error_message = (
                f"Docker image '{_KERNEL_IMAGE}' not found. "
                "Please build or pull the kernel image before starting a kernel."
            )
            flow_logger.error(f"Docker image '{_KERNEL_IMAGE}' not found. "
                              "Please build or pull the kernel image before starting a kernel.") if flow_logger else None
            raise RuntimeError(kernel.error_message)

        if kernel.port is None:
            kernel.port = self._allocate_port()

        kernel.state = KernelState.STARTING
        kernel.error_message = None

        try:
            env = self._build_kernel_env(kernel_id, kernel)
            run_kwargs: dict = {
                "detach": True,
                "name": f"flowfile-kernel-{kernel_id}",
                "ports": {"9999/tcp": kernel.port},
                "volumes": {self._shared_volume: {"bind": "/shared", "mode": "rw"}},
                "environment": env,
                "mem_limit": f"{kernel.memory_gb}g",
                "nano_cpus": int(kernel.cpu_cores * 1e9),
                "extra_hosts": {"host.docker.internal": "host-gateway"},
            }
            container = self._docker.containers.run(_KERNEL_IMAGE, **run_kwargs)
            kernel.container_id = container.id
            self._wait_for_healthy_sync(kernel_id, timeout=kernel.health_timeout)
            kernel.state = KernelState.IDLE
            flow_logger.info(f"Kernel  {kernel_id} is idle (container {container.short_id})") if flow_logger else None
        except (docker.errors.DockerException, httpx.HTTPError, TimeoutError, OSError) as exc:
            kernel.state = KernelState.ERROR
            kernel.error_message = str(exc)
            flow_logger.error(f"Failed to start kernel {kernel_id}: {exc}") if flow_logger else None
            self._cleanup_container(kernel_id)
            raise
        flow_logger.info(f"Kernel  {kernel_id} started (container {container.short_id})") if flow_logger else None
        return kernel

    async def stop_kernel(self, kernel_id: str) -> None:
        kernel = self._get_kernel_or_raise(kernel_id)
        self._cleanup_container(kernel_id)
        kernel.state = KernelState.STOPPED
        kernel.container_id = None
        logger.info("Stopped kernel '%s'", kernel_id)

    async def delete_kernel(self, kernel_id: str) -> None:
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state in (KernelState.IDLE, KernelState.EXECUTING):
            await self.stop_kernel(kernel_id)
        del self._kernels[kernel_id]
        self._kernel_owners.pop(kernel_id, None)
        self._remove_kernel_from_db(kernel_id)
        logger.info("Deleted kernel '%s'", kernel_id)

    def shutdown_all(self) -> None:
        """Stop and remove all running kernel containers. Called on core shutdown."""
        kernel_ids = list(self._kernels.keys())
        for kernel_id in kernel_ids:
            kernel = self._kernels.get(kernel_id)
            if kernel and kernel.state in (KernelState.IDLE, KernelState.EXECUTING, KernelState.STARTING):
                logger.info("Shutting down kernel '%s'", kernel_id)
                self._cleanup_container(kernel_id)
                kernel.state = KernelState.STOPPED
                kernel.container_id = None
        logger.info("All kernels have been shut down")

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    async def execute(self, kernel_id: str, request: ExecuteRequest) -> ExecuteResult:
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            await self._ensure_running(kernel_id)

        kernel.state = KernelState.EXECUTING
        try:
            url = f"http://localhost:{kernel.port}/execute"
            async with httpx.AsyncClient(timeout=httpx.Timeout(300.0)) as client:
                response = await client.post(url, json=request.model_dump())
                response.raise_for_status()
                return ExecuteResult(**response.json())
        finally:
            # Only return to IDLE if we haven't been stopped/errored in the meantime
            if kernel.state == KernelState.EXECUTING:
                kernel.state = KernelState.IDLE

    def execute_sync(self, kernel_id: str, request: ExecuteRequest,
                     flow_logger: FlowLogger | None = None) -> ExecuteResult:
        """Synchronous wrapper around execute() for use from non-async code."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            self._ensure_running_sync(kernel_id, flow_logger=flow_logger)

        kernel.state = KernelState.EXECUTING
        try:
            url = f"http://localhost:{kernel.port}/execute"
            with httpx.Client(timeout=httpx.Timeout(300.0)) as client:
                response = client.post(url, json=request.model_dump())
                response.raise_for_status()
                return ExecuteResult(**response.json())
        finally:
            if kernel.state == KernelState.EXECUTING:
                kernel.state = KernelState.IDLE

    async def clear_artifacts(self, kernel_id: str) -> None:
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            await self._ensure_running(kernel_id)

        url = f"http://localhost:{kernel.port}/clear"
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            response = await client.post(url)
            response.raise_for_status()

    def clear_artifacts_sync(self, kernel_id: str) -> None:
        """Synchronous wrapper around clear_artifacts() for use from non-async code."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            self._ensure_running_sync(kernel_id)

        url = f"http://localhost:{kernel.port}/clear"
        with httpx.Client(timeout=httpx.Timeout(30.0)) as client:
            response = client.post(url)
            response.raise_for_status()

    async def clear_node_artifacts(
        self, kernel_id: str, node_ids: list[int], flow_id: int | None = None,
    ) -> ClearNodeArtifactsResult:
        """Clear only artifacts published by the given node IDs."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            await self._ensure_running(kernel_id)

        url = f"http://localhost:{kernel.port}/clear_node_artifacts"
        payload: dict = {"node_ids": node_ids}
        if flow_id is not None:
            payload["flow_id"] = flow_id
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            response = await client.post(url, json=payload)
            response.raise_for_status()
            return ClearNodeArtifactsResult(**response.json())

    def clear_node_artifacts_sync(
        self, kernel_id: str, node_ids: list[int], flow_id: int | None = None,
        flow_logger: FlowLogger | None = None,
    ) -> ClearNodeArtifactsResult:
        """Synchronous wrapper for clearing artifacts by node IDs."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            self._ensure_running_sync(kernel_id, flow_logger=flow_logger)

        url = f"http://localhost:{kernel.port}/clear_node_artifacts"
        payload: dict = {"node_ids": node_ids}
        if flow_id is not None:
            payload["flow_id"] = flow_id
        with httpx.Client(timeout=httpx.Timeout(30.0)) as client:
            response = client.post(url, json=payload)
            response.raise_for_status()
            return ClearNodeArtifactsResult(**response.json())

    async def clear_namespace(self, kernel_id: str, flow_id: int) -> None:
        """Clear the execution namespace for a flow (variables, imports, etc.)."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            await self._ensure_running(kernel_id)

        url = f"http://localhost:{kernel.port}/clear_namespace"
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            response = await client.post(url, params={"flow_id": flow_id})
            response.raise_for_status()

    async def get_node_artifacts(self, kernel_id: str, node_id: int) -> dict:
        """Get artifacts published by a specific node."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            await self._ensure_running(kernel_id)

        url = f"http://localhost:{kernel.port}/artifacts/node/{node_id}"
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            response = await client.get(url)
            response.raise_for_status()
            return response.json()

    # ------------------------------------------------------------------
    # Artifact Persistence & Recovery
    # ------------------------------------------------------------------

    async def recover_artifacts(self, kernel_id: str) -> RecoveryStatus:
        """Trigger manual artifact recovery on a running kernel."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            raise RuntimeError(f"Kernel '{kernel_id}' is not running (state: {kernel.state})")

        url = f"http://localhost:{kernel.port}/recover"
        async with httpx.AsyncClient(timeout=httpx.Timeout(120.0)) as client:
            response = await client.post(url)
            response.raise_for_status()
            return RecoveryStatus(**response.json())

    async def get_recovery_status(self, kernel_id: str) -> RecoveryStatus:
        """Get the current recovery status of a kernel."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            raise RuntimeError(f"Kernel '{kernel_id}' is not running (state: {kernel.state})")

        url = f"http://localhost:{kernel.port}/recovery-status"
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            response = await client.get(url)
            response.raise_for_status()
            return RecoveryStatus(**response.json())

    async def cleanup_artifacts(self, kernel_id: str, request: CleanupRequest) -> CleanupResult:
        """Clean up old persisted artifacts on a kernel."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            raise RuntimeError(f"Kernel '{kernel_id}' is not running (state: {kernel.state})")

        url = f"http://localhost:{kernel.port}/cleanup"
        async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
            response = await client.post(url, json=request.model_dump())
            response.raise_for_status()
            return CleanupResult(**response.json())

    async def get_persistence_info(self, kernel_id: str) -> ArtifactPersistenceInfo:
        """Get persistence configuration and stats for a kernel."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            raise RuntimeError(f"Kernel '{kernel_id}' is not running (state: {kernel.state})")

        url = f"http://localhost:{kernel.port}/persistence"
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            response = await client.get(url)
            response.raise_for_status()
            return ArtifactPersistenceInfo(**response.json())

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    async def list_kernels(self, user_id: int | None = None) -> list[KernelInfo]:
        if user_id is not None:
            return [
                k for kid, k in self._kernels.items()
                if self._kernel_owners.get(kid) == user_id
            ]
        return list(self._kernels.values())

    async def get_kernel(self, kernel_id: str) -> KernelInfo | None:
        return self._kernels.get(kernel_id)

    def get_kernel_owner(self, kernel_id: str) -> int | None:
        return self._kernel_owners.get(kernel_id)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_kernel_or_raise(self, kernel_id: str) -> KernelInfo:
        kernel = self._kernels.get(kernel_id)
        if kernel is None:
            raise KeyError(f"Kernel '{kernel_id}' not found")
        return kernel

    async def _ensure_running(self, kernel_id: str) -> None:
        """Restart the kernel if it is STOPPED or ERROR, then wait until IDLE."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state in (KernelState.IDLE, KernelState.EXECUTING):
            return
        if kernel.state in (KernelState.STOPPED, KernelState.ERROR):
            logger.info(
                "Kernel '%s' is %s, attempting automatic restart...",
                kernel_id, kernel.state.value,
            )
            self._cleanup_container(kernel_id)
            kernel.container_id = None
            await self.start_kernel(kernel_id)
            return
        # STARTING — wait for it to finish
        if kernel.state == KernelState.STARTING:
            logger.info("Kernel '%s' is starting, waiting for it to become ready...", kernel_id)
            await self._wait_for_healthy(kernel_id)
            kernel.state = KernelState.IDLE

    def _ensure_running_sync(self, kernel_id: str, flow_logger: FlowLogger | None = None) -> None:
        """Synchronous version of _ensure_running."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state in (KernelState.IDLE, KernelState.EXECUTING):
            return
        if kernel.state in (KernelState.STOPPED, KernelState.ERROR):
            msg = f"Kernel '{kernel_id}' is {kernel.state.value}, attempting automatic restart..."
            logger.info(msg)
            if flow_logger:
                flow_logger.info(msg)
            self._cleanup_container(kernel_id)
            kernel.container_id = None
            self.start_kernel_sync(kernel_id, flow_logger=flow_logger)
            return
        # STARTING — wait for it to finish
        if kernel.state == KernelState.STARTING:
            logger.info("Kernel '%s' is starting, waiting for it to become ready...", kernel_id)
            self._wait_for_healthy_sync(kernel_id)
            kernel.state = KernelState.IDLE

    def _cleanup_container(self, kernel_id: str) -> None:
        kernel = self._kernels.get(kernel_id)
        if kernel is None or kernel.container_id is None:
            return
        try:
            container = self._docker.containers.get(kernel.container_id)
            container.stop(timeout=10)
            container.remove(force=True)
        except docker.errors.NotFound:
            pass
        except (docker.errors.APIError, docker.errors.DockerException) as exc:
            logger.warning("Error cleaning up container for kernel '%s': %s", kernel_id, exc)

    async def _wait_for_healthy(self, kernel_id: str, timeout: int = _HEALTH_TIMEOUT) -> None:
        kernel = self._get_kernel_or_raise(kernel_id)
        url = f"http://localhost:{kernel.port}/health"
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout

        while loop.time() < deadline:
            try:
                async with httpx.AsyncClient(timeout=httpx.Timeout(5.0)) as client:
                    response = await client.get(url)
                    if response.status_code == 200:
                        data = response.json()
                        kernel.kernel_version = data.get("version")
                        return
            except (httpx.HTTPError, OSError) as exc:
                logger.debug("Health poll for kernel '%s' failed: %s", kernel_id, exc)
            await asyncio.sleep(_HEALTH_POLL_INTERVAL)

        raise TimeoutError(f"Kernel '{kernel_id}' did not become healthy within {timeout}s")

    def _wait_for_healthy_sync(self, kernel_id: str, timeout: int = _HEALTH_TIMEOUT) -> None:
        """Synchronous version of _wait_for_healthy."""
        kernel = self._get_kernel_or_raise(kernel_id)
        url = f"http://localhost:{kernel.port}/health"
        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            try:
                with httpx.Client(timeout=httpx.Timeout(5.0)) as client:
                    response = client.get(url)
                    if response.status_code == 200:
                        data = response.json()
                        kernel.kernel_version = data.get("version")
                        return
            except (httpx.HTTPError, OSError) as exc:
                logger.debug("Health poll for kernel '%s' failed: %s", kernel_id, exc)
            time.sleep(_HEALTH_POLL_INTERVAL)

        raise TimeoutError(f"Kernel '{kernel_id}' did not become healthy within {timeout}s")
