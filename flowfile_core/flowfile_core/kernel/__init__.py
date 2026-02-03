from flowfile_core.kernel.manager import KernelManager
from flowfile_core.kernel.models import (
    ClearNodeArtifactsRequest,
    ClearNodeArtifactsResult,
    DisplayOutput,
    DockerStatus,
    ExecuteRequest,
    ExecuteResult,
    KernelConfig,
    KernelInfo,
    KernelState,
)
from flowfile_core.kernel.routes import router

__all__ = [
    "KernelManager",
    "ClearNodeArtifactsRequest",
    "ClearNodeArtifactsResult",
    "DisplayOutput",
    "DockerStatus",
    "KernelConfig",
    "KernelInfo",
    "KernelState",
    "ExecuteRequest",
    "ExecuteResult",
    "router",
    "get_kernel_manager",
]

_manager: KernelManager | None = None


def get_kernel_manager() -> KernelManager:
    global _manager
    if _manager is None:
        from shared.storage_config import storage

        shared_path = str(storage.temp_directory / "kernel_shared")
        _manager = KernelManager(shared_volume_path=shared_path)
    return _manager
