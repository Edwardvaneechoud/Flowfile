import asyncio
import logging
import socket
from typing import Any

import docker
import httpx

from flowfile_core.kernel.models import (
    ExecuteRequest,
    ExecuteResult,
    KernelConfig,
    KernelInfo,
    KernelState,
)
from shared.storage_config import storage

logger = logging.getLogger(__name__)

_KERNEL_IMAGE = "flowfile-kernel"
_BASE_PORT = 19000
_PORT_RANGE = 1000  # 19000-19999
_HEALTH_TIMEOUT = 120
_HEALTH_POLL_INTERVAL = 2
_PERSISTENCE_CONTAINER_PATH = "/shared/artifacts"


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
        self._shared_volume = shared_volume_path or str(storage.cache_directory)
        self._reclaim_running_containers()

    @property
    def shared_volume_path(self) -> str:
        return self._shared_volume

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

            if port is not None and kernel_id not in self._kernels:
                self._kernels[kernel_id] = KernelInfo(
                    id=kernel_id,
                    name=kernel_id,
                    state=KernelState.IDLE,
                    container_id=container.id,
                    port=port,
                )
                logger.info(
                    "Reclaimed running kernel '%s' on port %d (container %s)",
                    kernel_id, port, container.short_id,
                )

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

    async def create_kernel(self, config: KernelConfig) -> KernelInfo:
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
            persistence_mode=config.persistence_mode,
        )
        self._kernels[config.id] = kernel
        logger.info("Created kernel '%s' on port %d", config.id, port)
        return kernel

    async def start_kernel(self, kernel_id: str) -> KernelInfo:
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state == KernelState.IDLE:
            return kernel

        kernel.state = KernelState.STARTING
        kernel.error_message = None

        try:
            packages_str = " ".join(kernel.packages)
            environment = {
                "KERNEL_PACKAGES": packages_str,
                "KERNEL_ID": kernel_id,
                "PERSISTENCE_ENABLED": str(kernel.persistence_enabled).lower(),
                "PERSISTENCE_PATH": _PERSISTENCE_CONTAINER_PATH,
                "PERSISTENCE_MODE": kernel.persistence_mode.value,
            }
            run_kwargs: dict = {
                "detach": True,
                "name": f"flowfile-kernel-{kernel_id}",
                "ports": {"9999/tcp": kernel.port},
                "volumes": {self._shared_volume: {"bind": "/shared", "mode": "rw"}},
                "environment": environment,
                "mem_limit": f"{kernel.memory_gb}g",
                "nano_cpus": int(kernel.cpu_cores * 1e9),
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
        logger.info("Deleted kernel '%s'", kernel_id)

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    async def execute(self, kernel_id: str, request: ExecuteRequest) -> ExecuteResult:
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            raise RuntimeError(f"Kernel '{kernel_id}' is not running (state: {kernel.state})")

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

    def execute_sync(self, kernel_id: str, request: ExecuteRequest) -> ExecuteResult:
        """Synchronous wrapper around execute() for use from non-async code."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            raise RuntimeError(f"Kernel '{kernel_id}' is not running (state: {kernel.state})")

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
            raise RuntimeError(f"Kernel '{kernel_id}' is not running (state: {kernel.state})")

        url = f"http://localhost:{kernel.port}/clear"
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            response = await client.post(url)
            response.raise_for_status()

    def clear_artifacts_sync(self, kernel_id: str) -> None:
        """Synchronous wrapper around clear_artifacts() for use from non-async code."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            raise RuntimeError(f"Kernel '{kernel_id}' is not running (state: {kernel.state})")

        url = f"http://localhost:{kernel.port}/clear"
        with httpx.Client(timeout=httpx.Timeout(30.0)) as client:
            response = client.post(url)
            response.raise_for_status()

    # ------------------------------------------------------------------
    # Recovery & persistence
    # ------------------------------------------------------------------

    async def recover_artifacts(self, kernel_id: str) -> dict[str, Any]:
        """Trigger artifact recovery on the kernel."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            raise RuntimeError(f"Kernel '{kernel_id}' is not running (state: {kernel.state})")

        url = f"http://localhost:{kernel.port}/recover"
        async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
            response = await client.post(url)
            response.raise_for_status()
            return response.json()

    async def get_recovery_status(self, kernel_id: str) -> dict[str, Any]:
        """Get recovery status from the kernel."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            raise RuntimeError(f"Kernel '{kernel_id}' is not running (state: {kernel.state})")

        url = f"http://localhost:{kernel.port}/recovery-status"
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            response = await client.get(url)
            response.raise_for_status()
            return response.json()

    async def cleanup_artifacts(self, kernel_id: str, max_age_hours: int = 24) -> dict[str, Any]:
        """Clean up old persisted artifacts on the kernel."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            raise RuntimeError(f"Kernel '{kernel_id}' is not running (state: {kernel.state})")

        url = f"http://localhost:{kernel.port}/cleanup"
        async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
            response = await client.post(url, json={"max_age_hours": max_age_hours})
            response.raise_for_status()
            return response.json()

    async def get_persistence_info(self, kernel_id: str) -> dict[str, Any]:
        """Get persistence info from the kernel."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            raise RuntimeError(f"Kernel '{kernel_id}' is not running (state: {kernel.state})")

        url = f"http://localhost:{kernel.port}/persistence"
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            response = await client.get(url)
            response.raise_for_status()
            return response.json()

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    async def list_kernels(self) -> list[KernelInfo]:
        return list(self._kernels.values())

    async def get_kernel(self, kernel_id: str) -> KernelInfo | None:
        return self._kernels.get(kernel_id)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_kernel_or_raise(self, kernel_id: str) -> KernelInfo:
        kernel = self._kernels.get(kernel_id)
        if kernel is None:
            raise KeyError(f"Kernel '{kernel_id}' not found")
        return kernel

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
                        return
            except (httpx.HTTPError, OSError) as exc:
                logger.debug("Health poll for kernel '%s' failed: %s", kernel_id, exc)
            await asyncio.sleep(_HEALTH_POLL_INTERVAL)

        raise TimeoutError(f"Kernel '{kernel_id}' did not become healthy within {timeout}s")
