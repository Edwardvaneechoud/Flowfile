import asyncio
import logging
import os
import socket
import threading
import time

import docker
import docker.types
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
    KernelMemoryInfo,
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


def _is_docker_mode() -> bool:
    """Check if running in Docker mode based on FLOWFILE_MODE."""
    return os.environ.get("FLOWFILE_MODE") == "docker"


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

        # Docker-in-Docker settings: when core itself runs in a container,
        # kernel containers must use a named volume (not a bind mount) and
        # connect to the same Docker network for service discovery.
        self._docker_network: str | None = (
            os.environ.get("FLOWFILE_DOCKER_NETWORK") or self._detect_docker_network()
        )

        # In Docker mode, discover the volume that covers _shared_volume
        # (e.g. flowfile-internal-storage mounted at /app/internal_storage).
        # Kernel containers will mount the same volume at the same path so
        # all file paths are identical across core, worker, and kernel.
        self._kernel_volume: str | None = None
        self._kernel_volume_type: str | None = None
        self._kernel_mount_target: str | None = None  # mount point inside containers
        if _is_docker_mode():
            vol_name, vol_type, mount_dest = self._discover_volume_for_path(self._shared_volume)
            if vol_name:
                self._kernel_volume = vol_name
                self._kernel_volume_type = vol_type
                self._kernel_mount_target = mount_dest
                logger.info(
                    "Docker-in-Docker mode: volume=%s (type=%s) at %s covers shared_path=%s, network=%s",
                    vol_name,
                    vol_type,
                    mount_dest,
                    self._shared_volume,
                    self._docker_network,
                )
            else:
                logger.warning(
                    "Could not discover volume for shared_path=%s; "
                    "kernel containers will use bind mounts (local mode only)",
                    self._shared_volume,
                )

        self._restore_kernels_from_db()
        self._reclaim_running_containers()

    @property
    def shared_volume_path(self) -> str:
        return self._shared_volume

    # ------------------------------------------------------------------
    # Docker-in-Docker helpers
    # ------------------------------------------------------------------

    def _detect_docker_network(self) -> str | None:
        """Auto-detect the Docker network this container is connected to.

        When core runs inside Docker, we inspect the current container's
        network settings and return the first user-defined network.  This
        allows kernel containers to be attached to the same network without
        requiring an explicit FLOWFILE_DOCKER_NETWORK env var.
        """
        if not _is_docker_mode():
            return None
        try:
            hostname = socket.gethostname()
            container = self._docker.containers.get(hostname)
            networks = container.attrs["NetworkSettings"]["Networks"]
            for name in networks:
                if name not in ("bridge", "host", "none"):
                    return name
        except Exception as exc:
            logger.debug("Could not auto-detect Docker network: %s", exc)
        return None

    def _discover_volume_for_path(self, path: str) -> tuple[str | None, str | None, str | None]:
        """Find which Docker volume/bind covers *path* in this container.

        Inspects the current container's mounts and returns the one whose
        ``Destination`` is a prefix of *path* (longest match wins).

        Returns ``(source_or_name, mount_type, destination)`` or
        ``(None, None, None)`` if no mount covers the path.
        """
        try:
            hostname = socket.gethostname()
            container = self._docker.containers.get(hostname)
            mounts = container.attrs.get("Mounts", [])
            logger.debug("Container %s mounts: %s", hostname, mounts)

            best: dict | None = None
            for mount in mounts:
                dest = mount.get("Destination", "")
                if path.startswith(dest) and (best is None or len(dest) > len(best.get("Destination", ""))):
                    best = mount

            if best:
                mount_type = best.get("Type", "volume")
                dest = best["Destination"]
                name = best.get("Name") if mount_type == "volume" else best.get("Source")
                return name, mount_type, dest

            logger.warning("No mount covers path %s in container %s", path, hostname)
        except Exception as exc:
            logger.warning("Could not inspect container mounts: %s", exc)
        return None, None, None

    def _kernel_url(self, kernel: KernelInfo) -> str:
        """Return the base URL for communicating with a kernel container.

        In Docker-in-Docker mode, use the container name on the shared
        Docker network.  Otherwise, use localhost with the mapped host port.
        """
        if self._docker_network:
            return f"http://flowfile-kernel-{kernel.id}:9999"
        return f"http://localhost:{kernel.port}"

    def to_kernel_path(self, local_path: str) -> str:
        """Translate a local filesystem path to the path visible inside a kernel container.

        In Docker-in-Docker mode the volume is mounted at the same path in all
        containers, so paths are identical.  In local mode the host directory is
        bind-mounted at ``/shared`` inside the kernel, so we swap the prefix.
        """
        if self._kernel_volume:
            # Same volume, same mount point — no translation needed
            return local_path
        # Local mode: host shared_volume → /shared inside kernel
        return local_path.replace(self._shared_volume, "/shared", 1)

    def resolve_node_paths(self, request: "ExecuteRequest") -> None:
        """Populate ``input_paths`` and ``output_dir`` from ``flow_id``/``node_id``.

        When the frontend sends only ``flow_id`` and ``node_id`` (without
        pre-built filesystem paths), this method resolves the actual paths
        on the shared volume and translates them for the kernel container.
        If ``input_paths`` is already populated (e.g. from ``flow_graph.py``),
        this is a no-op.
        """
        if request.input_paths or not request.flow_id or not request.node_id:
            return

        input_dir = os.path.join(
            self._shared_volume,
            str(request.flow_id),
            str(request.node_id),
            "inputs",
        )
        output_dir = os.path.join(
            self._shared_volume,
            str(request.flow_id),
            str(request.node_id),
            "outputs",
        )

        # Discover parquet files in the input directory
        if os.path.isdir(input_dir):
            parquet_files = sorted(
                f for f in os.listdir(input_dir) if f.endswith(".parquet")
            )
            if parquet_files:
                request.input_paths = {
                    "main": [
                        self.to_kernel_path(os.path.join(input_dir, f))
                        for f in parquet_files
                    ]
                }

        request.output_dir = self.to_kernel_path(output_dir)

    def _build_run_kwargs(self, kernel_id: str, kernel: KernelInfo, env: dict) -> dict:
        """Build Docker ``containers.run()`` keyword arguments.

        Adapts volume mounts and networking for local vs Docker-in-Docker.
        """
        run_kwargs: dict = {
            "detach": True,
            "name": f"flowfile-kernel-{kernel_id}",
            "environment": env,
            "mem_limit": f"{kernel.memory_gb}g",
            "nano_cpus": int(kernel.cpu_cores * 1e9),
        }

        if self._kernel_volume:
            # Docker-in-Docker: mount the same volume at the same path so
            # all file paths are identical in core, worker, and kernel.
            mount_type = self._kernel_volume_type or "volume"
            mount_target = self._kernel_mount_target or "/app/internal_storage"
            run_kwargs["mounts"] = [
                docker.types.Mount(
                    target=mount_target,
                    source=self._kernel_volume,
                    type=mount_type,
                    read_only=False,
                )
            ]
            if self._docker_network:
                run_kwargs["network"] = self._docker_network
        else:
            # Local: bind-mount a host directory and map ports.
            run_kwargs["volumes"] = {
                self._shared_volume: {"bind": "/shared", "mode": "rw"},
            }
            run_kwargs["ports"] = {"9999/tcp": kernel.port}
            run_kwargs["extra_hosts"] = {"host.docker.internal": "host-gateway"}

        return run_kwargs

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
            containers = self._docker.containers.list(filters={"name": "flowfile-kernel-", "status": "running"})
        except (docker.errors.APIError, docker.errors.DockerException) as exc:
            logger.warning("Could not list running containers: %s", exc)
            return

        for container in containers:
            name = container.name
            if not name.startswith("flowfile-kernel-"):
                continue
            kernel_id = name[len("flowfile-kernel-") :]

            if kernel_id in self._kernels:
                # Determine which host port is mapped (not available in DinD mode)
                port = None
                if not self._kernel_volume:
                    try:
                        bindings = container.attrs["NetworkSettings"]["Ports"].get("9999/tcp")
                        if bindings:
                            port = int(bindings[0]["HostPort"])
                    except (KeyError, IndexError, TypeError, ValueError):
                        pass

                # Kernel was restored from DB — update with runtime info
                self._kernels[kernel_id].container_id = container.id
                if port is not None:
                    self._kernels[kernel_id].port = port
                self._kernels[kernel_id].state = KernelState.IDLE
                logger.info(
                    "Reclaimed running kernel '%s' (container %s)",
                    kernel_id,
                    container.short_id,
                )
            else:
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
        raise RuntimeError(f"No available ports in range {_BASE_PORT}-{_BASE_PORT + _PORT_RANGE - 1}")

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
        # FLOWFILE_CORE_URL: how kernel reaches Core API from inside Docker.
        # In Docker-in-Docker mode the kernel is on the same Docker network
        # as core, so it can reach core by service name.
        if self._docker_network:
            default_core_url = "http://flowfile-core:63578"
        else:
            default_core_url = "http://host.docker.internal:63578"
        core_url = os.environ.get("FLOWFILE_CORE_URL", default_core_url)
        env["FLOWFILE_CORE_URL"] = core_url
        # FLOWFILE_INTERNAL_TOKEN: service-to-service auth for kernel → Core
        # Use get_internal_token() instead of reading env directly so that in
        # Electron mode the token is auto-generated before the kernel starts.
        try:
            from flowfile_core.auth.jwt import get_internal_token

            env["FLOWFILE_INTERNAL_TOKEN"] = get_internal_token()
        except (ValueError, ImportError):
            # Token not configured (e.g. local dev without env var) – skip
            internal_token = os.environ.get("FLOWFILE_INTERNAL_TOKEN")
            if internal_token:
                env["FLOWFILE_INTERNAL_TOKEN"] = internal_token
        # FLOWFILE_KERNEL_ID: pass kernel ID for lineage tracking
        env["FLOWFILE_KERNEL_ID"] = kernel_id
        # FLOWFILE_HOST_SHARED_DIR tells the kernel how to translate Core
        # API paths to container paths.  Only needed in local mode where the
        # shared dir is bind-mounted at /shared.  In Docker-in-Docker mode
        # the volume is mounted at the *same* path in core, worker and
        # kernel, so no translation is required and the variable is omitted.
        if not self._kernel_volume:
            env["FLOWFILE_HOST_SHARED_DIR"] = self._shared_volume
        # Persistence settings from kernel config
        env["KERNEL_ID"] = kernel_id
        env["PERSISTENCE_ENABLED"] = "true" if kernel.persistence_enabled else "false"
        env["PERSISTENCE_PATH"] = self.to_kernel_path(os.path.join(self._shared_volume, "artifacts"))
        env["RECOVERY_MODE"] = kernel.recovery_mode.value
        return env

    async def create_kernel(self, config: KernelConfig, user_id: int) -> KernelInfo:
        if config.id in self._kernels:
            raise ValueError(f"Kernel '{config.id}' already exists")

        # In Docker-in-Docker mode we don't map host ports — kernels are
        # reached via container name on the shared Docker network.
        port = None if self._kernel_volume else self._allocate_port()
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
        logger.info("Created kernel '%s' on port %s for user %d", config.id, port, user_id)
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

        # Allocate a port if needed (local mode only, not needed for DinD)
        if kernel.port is None and not self._kernel_volume:
            kernel.port = self._allocate_port()

        kernel.state = KernelState.STARTING
        kernel.error_message = None

        try:
            env = self._build_kernel_env(kernel_id, kernel)
            run_kwargs = self._build_run_kwargs(kernel_id, kernel, env)
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
            flow_logger.error(
                f"Docker image '{_KERNEL_IMAGE}' not found. "
                "Please build or pull the kernel image before starting a kernel."
            ) if flow_logger else None
            raise RuntimeError(kernel.error_message)

        if kernel.port is None and not self._kernel_volume:
            kernel.port = self._allocate_port()

        kernel.state = KernelState.STARTING
        kernel.error_message = None

        try:
            env = self._build_kernel_env(kernel_id, kernel)
            run_kwargs = self._build_run_kwargs(kernel_id, kernel, env)
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

    def _check_oom_killed(self, kernel_id: str) -> bool:
        """Check if the kernel container was killed due to an out-of-memory condition."""
        kernel = self._kernels.get(kernel_id)
        if kernel is None or kernel.container_id is None:
            return False
        try:
            container = self._docker.containers.get(kernel.container_id)
            state = container.attrs.get("State", {})
            return state.get("OOMKilled", False)
        except (docker.errors.NotFound, docker.errors.APIError):
            return False

    async def execute(self, kernel_id: str, request: ExecuteRequest) -> ExecuteResult:
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            await self._ensure_running(kernel_id)

        kernel.state = KernelState.EXECUTING
        try:
            url = f"{self._kernel_url(kernel)}/execute"
            async with httpx.AsyncClient(timeout=httpx.Timeout(300.0)) as client:
                response = await client.post(url, json=request.model_dump())
                response.raise_for_status()
                return ExecuteResult(**response.json())
        except (httpx.HTTPError, OSError):
            if self._check_oom_killed(kernel_id):
                kernel.state = KernelState.ERROR
                kernel.error_message = "Kernel ran out of memory"
                oom_msg = (
                    f"Kernel ran out of memory. The container exceeded its {kernel.memory_gb} GB "
                    "memory limit and was terminated. Consider increasing the kernel's memory "
                    "allocation or reducing your data size."
                )
                return ExecuteResult(success=False, error=oom_msg)
            raise
        finally:
            # Only return to IDLE if we haven't been stopped/errored in the meantime
            if kernel.state == KernelState.EXECUTING:
                kernel.state = KernelState.IDLE

    def execute_sync(
        self,
        kernel_id: str,
        request: ExecuteRequest,
        flow_logger: FlowLogger | None = None,
        cancel_event: threading.Event | None = None,
    ) -> ExecuteResult:
        """Synchronous wrapper around execute() for use from non-async code.

        When *cancel_event* is provided the HTTP call runs in a daemon thread
        so the caller can be unblocked promptly when the event is set.
        """
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            self._ensure_running_sync(kernel_id, flow_logger=flow_logger)

        kernel.state = KernelState.EXECUTING
        try:
            url = f"{self._kernel_url(kernel)}/execute"

            if cancel_event is None:
                # Simple blocking call (no cancellation support)
                with httpx.Client(timeout=httpx.Timeout(300.0)) as client:
                    response = client.post(url, json=request.model_dump())
                    response.raise_for_status()
                    return ExecuteResult(**response.json())

            # --- cancellation-aware path ---
            result_holder: list[ExecuteResult | None] = [None]
            error_holder: list[BaseException | None] = [None]

            def _post() -> None:
                try:
                    with httpx.Client(timeout=httpx.Timeout(300.0)) as client:
                        resp = client.post(url, json=request.model_dump())
                        resp.raise_for_status()
                        result_holder[0] = ExecuteResult(**resp.json())
                except BaseException as exc:
                    error_holder[0] = exc

            t = threading.Thread(target=_post, daemon=True)
            t.start()

            while t.is_alive():
                t.join(timeout=0.5)
                if cancel_event.is_set():
                    # Best-effort interrupt, then return immediately
                    self.interrupt_execution_sync(kernel_id)
                    return ExecuteResult(success=False, error="Execution cancelled by user")

            if error_holder[0] is not None:
                raise error_holder[0]
            if result_holder[0] is not None:
                return result_holder[0]
            raise RuntimeError("Kernel execution returned no result")

        except (httpx.HTTPError, OSError):
            if self._check_oom_killed(kernel_id):
                kernel.state = KernelState.ERROR
                kernel.error_message = "Kernel ran out of memory"
                oom_msg = (
                    f"Kernel ran out of memory. The container exceeded its {kernel.memory_gb} GB "
                    "memory limit and was terminated. Consider increasing the kernel's memory "
                    "allocation or reducing your data size."
                )
                if flow_logger:
                    flow_logger.error(oom_msg)
                return ExecuteResult(success=False, error=oom_msg)
            raise
        finally:
            if kernel.state == KernelState.EXECUTING:
                kernel.state = KernelState.IDLE

    def interrupt_execution_sync(self, kernel_id: str) -> bool:
        """Interrupt running user code on a kernel.

        Tries the HTTP ``/interrupt`` endpoint first (works when the kernel
        runs user code in a background thread and keeps the event loop free).
        Falls back to sending ``SIGUSR1`` via Docker if the HTTP call fails
        (e.g. older kernel image, or the event loop is blocked).
        """
        kernel = self._kernels.get(kernel_id)
        if kernel is None or kernel.container_id is None:
            logger.warning("Cannot interrupt kernel '%s': not found or no container", kernel_id)
            return False
        if kernel.state != KernelState.EXECUTING:
            return False

        # --- Try HTTP /interrupt (preferred) ---
        try:
            url = f"{self._kernel_url(kernel)}/interrupt"
            with httpx.Client(timeout=httpx.Timeout(5.0)) as client:
                resp = client.post(url)
                if resp.status_code == 200:
                    logger.info("Interrupted kernel '%s' via HTTP", kernel_id)
                    return True
        except (httpx.HTTPError, OSError):
            logger.debug("HTTP /interrupt failed for kernel '%s', falling back to SIGUSR1", kernel_id)

        # --- Fallback: Docker SIGUSR1 ---
        try:
            container = self._docker.containers.get(kernel.container_id)
            container.kill(signal="SIGUSR1")
            logger.info("Sent SIGUSR1 to kernel '%s' (container %s)", kernel_id, kernel.container_id[:12])
            return True
        except docker.errors.NotFound:
            logger.warning("Container for kernel '%s' not found", kernel_id)
            return False
        except (docker.errors.APIError, docker.errors.DockerException) as exc:
            logger.error("Failed to send SIGUSR1 to kernel '%s': %s", kernel_id, exc)
            return False

    async def interrupt_execution(self, kernel_id: str) -> bool:
        """Async wrapper around :meth:`interrupt_execution_sync`."""
        return self.interrupt_execution_sync(kernel_id)

    async def clear_artifacts(self, kernel_id: str) -> None:
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            await self._ensure_running(kernel_id)

        url = f"{self._kernel_url(kernel)}/clear"
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            response = await client.post(url)
            response.raise_for_status()

    def clear_artifacts_sync(self, kernel_id: str) -> None:
        """Synchronous wrapper around clear_artifacts() for use from non-async code."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            self._ensure_running_sync(kernel_id)

        url = f"{self._kernel_url(kernel)}/clear"
        with httpx.Client(timeout=httpx.Timeout(30.0)) as client:
            response = client.post(url)
            response.raise_for_status()

    async def clear_node_artifacts(
        self,
        kernel_id: str,
        node_ids: list[int],
        flow_id: int | None = None,
    ) -> ClearNodeArtifactsResult:
        """Clear only artifacts published by the given node IDs."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            await self._ensure_running(kernel_id)

        url = f"{self._kernel_url(kernel)}/clear_node_artifacts"
        payload: dict = {"node_ids": node_ids}
        if flow_id is not None:
            payload["flow_id"] = flow_id
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            response = await client.post(url, json=payload)
            response.raise_for_status()
            return ClearNodeArtifactsResult(**response.json())

    def clear_node_artifacts_sync(
        self,
        kernel_id: str,
        node_ids: list[int],
        flow_id: int | None = None,
        flow_logger: FlowLogger | None = None,
    ) -> ClearNodeArtifactsResult:
        """Synchronous wrapper for clearing artifacts by node IDs."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            self._ensure_running_sync(kernel_id, flow_logger=flow_logger)

        url = f"{self._kernel_url(kernel)}/clear_node_artifacts"
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

        url = f"{self._kernel_url(kernel)}/clear_namespace"
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            response = await client.post(url, params={"flow_id": flow_id})
            response.raise_for_status()

    async def get_node_artifacts(self, kernel_id: str, node_id: int) -> dict:
        """Get artifacts published by a specific node."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            await self._ensure_running(kernel_id)

        url = f"{self._kernel_url(kernel)}/artifacts/node/{node_id}"
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

        url = f"{self._kernel_url(kernel)}/recover"
        async with httpx.AsyncClient(timeout=httpx.Timeout(120.0)) as client:
            response = await client.post(url)
            response.raise_for_status()
            return RecoveryStatus(**response.json())

    async def get_recovery_status(self, kernel_id: str) -> RecoveryStatus:
        """Get the current recovery status of a kernel."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            raise RuntimeError(f"Kernel '{kernel_id}' is not running (state: {kernel.state})")

        url = f"{self._kernel_url(kernel)}/recovery-status"
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            response = await client.get(url)
            response.raise_for_status()
            return RecoveryStatus(**response.json())

    async def cleanup_artifacts(self, kernel_id: str, request: CleanupRequest) -> CleanupResult:
        """Clean up old persisted artifacts on a kernel."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            raise RuntimeError(f"Kernel '{kernel_id}' is not running (state: {kernel.state})")

        url = f"{self._kernel_url(kernel)}/cleanup"
        async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
            response = await client.post(url, json=request.model_dump())
            response.raise_for_status()
            return CleanupResult(**response.json())

    async def get_persistence_info(self, kernel_id: str) -> ArtifactPersistenceInfo:
        """Get persistence configuration and stats for a kernel."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            raise RuntimeError(f"Kernel '{kernel_id}' is not running (state: {kernel.state})")

        url = f"{self._kernel_url(kernel)}/persistence"
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            response = await client.get(url)
            response.raise_for_status()
            return ArtifactPersistenceInfo(**response.json())

    async def get_memory_stats(self, kernel_id: str) -> KernelMemoryInfo:
        """Get current memory usage from a running kernel container."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            raise RuntimeError(f"Kernel '{kernel_id}' is not running (state: {kernel.state})")

        url = f"{self._kernel_url(kernel)}/memory"
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(5.0)) as client:
                response = await client.get(url)
                response.raise_for_status()
                return KernelMemoryInfo(**response.json())
        except (httpx.HTTPError, OSError) as exc:
            raise RuntimeError(
                f"Could not retrieve memory stats from kernel '{kernel_id}': {exc}"
            ) from exc

    async def list_kernel_artifacts(self, kernel_id: str) -> list:
        """List all artifacts in a running kernel."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            raise RuntimeError(f"Kernel '{kernel_id}' is not running (state: {kernel.state})")

        url = f"{self._kernel_url(kernel)}/artifacts"
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            response = await client.get(url)
            response.raise_for_status()
            return response.json()

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    async def list_kernels(self, user_id: int | None = None) -> list[KernelInfo]:
        if user_id is not None:
            return [k for kid, k in self._kernels.items() if self._kernel_owners.get(kid) == user_id]
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
                kernel_id,
                kernel.state.value,
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
        url = f"{self._kernel_url(kernel)}/health"
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
        url = f"{self._kernel_url(kernel)}/health"
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
