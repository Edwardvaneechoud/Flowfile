import os

from flowfile_core.kernel.manager import KernelManager
from flowfile_core.kernel.models import (
    ArtifactIdentifier,
    ArtifactPersistenceInfo,
    CleanupRequest,
    CleanupResult,
    ClearNodeArtifactsRequest,
    ClearNodeArtifactsResult,
    DisplayOutput,
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
    "DisplayOutput",
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

        if os.environ.get("FLOWFILE_MODE") == "docker":
            # In Docker mode the kernel shared named volume is mounted at
            # the same path as storage.shared_directory (typically /shared).
            shared_path = str(storage.shared_directory)
        else:
            shared_path = str(storage.temp_directory / "kernel_shared")
        _manager = KernelManager(shared_volume_path=shared_path)
    return _manager
