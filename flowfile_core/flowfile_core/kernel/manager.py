import asyncio
import logging

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
_HEALTH_TIMEOUT = 120
_HEALTH_POLL_INTERVAL = 2


class KernelManager:
    def __init__(self, shared_volume_path: str | None = None):
        self._docker = docker.from_env()
        self._kernels: dict[str, KernelInfo] = {}
        self._next_port = _BASE_PORT
        self._shared_volume = shared_volume_path or str(storage.cache_directory)

    def _allocate_port(self) -> int:
        port = self._next_port
        self._next_port += 1
        return port

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
        )
        self._kernels[config.id] = kernel
        logger.info("Created kernel '%s' on port %d", config.id, port)
        return kernel

    async def start_kernel(self, kernel_id: str) -> KernelInfo:
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state == KernelState.RUNNING:
            return kernel

        kernel.state = KernelState.STARTING
        kernel.error_message = None

        try:
            packages_str = " ".join(kernel.packages)
            container = self._docker.containers.run(
                _KERNEL_IMAGE,
                detach=True,
                name=f"flowfile-kernel-{kernel_id}",
                ports={"9999/tcp": kernel.port},
                volumes={self._shared_volume: {"bind": "/shared", "mode": "rw"}},
                environment={"KERNEL_PACKAGES": packages_str},
                mem_limit=f"{self._get_memory_limit(kernel_id)}g",
            )
            kernel.container_id = container.id
            await self._wait_for_healthy(kernel_id, timeout=_HEALTH_TIMEOUT)
            kernel.state = KernelState.RUNNING
            logger.info("Kernel '%s' is running (container %s)", kernel_id, container.short_id)
        except Exception as exc:
            kernel.state = KernelState.ERROR
            kernel.error_message = str(exc)
            logger.error("Failed to start kernel '%s': %s", kernel_id, exc)
            # Clean up partial container
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
        if kernel.state == KernelState.RUNNING:
            await self.stop_kernel(kernel_id)
        del self._kernels[kernel_id]
        logger.info("Deleted kernel '%s'", kernel_id)

    async def execute(self, kernel_id: str, request: ExecuteRequest) -> ExecuteResult:
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state != KernelState.RUNNING:
            raise RuntimeError(f"Kernel '{kernel_id}' is not running (state: {kernel.state})")

        url = f"http://localhost:{kernel.port}/execute"
        async with httpx.AsyncClient(timeout=httpx.Timeout(300.0)) as client:
            response = await client.post(url, json=request.model_dump())
            response.raise_for_status()
            return ExecuteResult(**response.json())

    async def clear_artifacts(self, kernel_id: str) -> None:
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state != KernelState.RUNNING:
            raise RuntimeError(f"Kernel '{kernel_id}' is not running (state: {kernel.state})")

        url = f"http://localhost:{kernel.port}/clear"
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            response = await client.post(url)
            response.raise_for_status()

    async def list_kernels(self) -> list[KernelInfo]:
        return list(self._kernels.values())

    async def get_kernel(self, kernel_id: str) -> KernelInfo | None:
        return self._kernels.get(kernel_id)

    def _get_kernel_or_raise(self, kernel_id: str) -> KernelInfo:
        kernel = self._kernels.get(kernel_id)
        if kernel is None:
            raise KeyError(f"Kernel '{kernel_id}' not found")
        return kernel

    def _get_memory_limit(self, kernel_id: str) -> float:
        # Check if we stored config info; default to 4GB
        kernel = self._kernels.get(kernel_id)
        if kernel is None:
            return 4.0
        # Memory limit was not stored on KernelInfo, use default
        return 4.0

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
        except Exception as exc:
            logger.warning("Error cleaning up container for kernel '%s': %s", kernel_id, exc)

    async def _wait_for_healthy(self, kernel_id: str, timeout: int = _HEALTH_TIMEOUT) -> None:
        kernel = self._get_kernel_or_raise(kernel_id)
        url = f"http://localhost:{kernel.port}/health"
        deadline = asyncio.get_event_loop().time() + timeout

        while asyncio.get_event_loop().time() < deadline:
            try:
                async with httpx.AsyncClient(timeout=httpx.Timeout(5.0)) as client:
                    response = await client.get(url)
                    if response.status_code == 200:
                        return
            except (httpx.ConnectError, httpx.ReadError, httpx.ConnectTimeout):
                pass
            await asyncio.sleep(_HEALTH_POLL_INTERVAL)

        raise TimeoutError(f"Kernel '{kernel_id}' did not become healthy within {timeout}s")
