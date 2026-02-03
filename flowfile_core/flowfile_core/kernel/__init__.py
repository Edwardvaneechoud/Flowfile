from flowfile_core.kernel.manager import KernelManager
from flowfile_core.kernel.models import (
    ArtifactIdentifier,
    ArtifactPersistenceInfo,
    CleanupRequest,
    CleanupResult,
    ClearNodeArtifactsRequest,
    ClearNodeArtifactsResult,
    DockerStatus,
    ExecuteRequest,
    ExecuteResult,
    KernelConfig,
    KernelInfo,
    KernelState,
    RecoveryMode,
    RecoveryStatus,
)
from flowfile_core.kernel.routes import router

__all__ = [
    "KernelManager",
    "ArtifactIdentifier",
    "ArtifactPersistenceInfo",
    "CleanupRequest",
    "CleanupResult",
    "ClearNodeArtifactsRequest",
    "ClearNodeArtifactsResult",
    "DockerStatus",
    "KernelConfig",
    "KernelInfo",
    "KernelState",
    "ExecuteRequest",
    "ExecuteResult",
    "RecoveryMode",
    "RecoveryStatus",
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
