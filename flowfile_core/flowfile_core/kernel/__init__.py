import threading
from collections.abc import Callable
from contextvars import ContextVar
from typing import NoReturn

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
    "get_kernel_manager_if_initialized",
    "kernel_manager_refusal",
]

_manager: KernelManager | None = None
_manager_lock = threading.Lock()

kernel_manager_refusal: ContextVar[Callable[[], NoReturn] | None] = ContextVar("kernel_manager_refusal", default=None)
"""A callable that raises, set for one context that must not reach Docker; :func:`get_kernel_manager` calls it first.

Context-local: it applies to the thread or task that set it (and to work started in a copy of its
context), never to other requests, so a cached manager stays available to them.
"""


def get_kernel_manager() -> KernelManager:
    """The process's kernel manager, constructed on first use; refused where :data:`kernel_manager_refusal` is set."""
    global _manager
    refusal = kernel_manager_refusal.get()
    if refusal is not None:
        refusal()
    # Double-checked: the startup warm-up thread can race the first /kernels/
    # request, and two constructions would double-run container reclaim + image GC.
    if _manager is None:
        with _manager_lock:
            if _manager is None:
                from shared.storage_config import storage

                # Use a sub-directory of the standard temp/internal_storage tree.
                # In Docker mode this resolves to /app/internal_storage/temp/kernel_shared
                # which is on the flowfile-internal-storage volume already shared
                # between core, worker, and (via KernelManager) kernel containers.
                shared_path = str(storage.temp_directory / "kernel_shared")
                _manager = KernelManager(shared_volume_path=shared_path)
    return _manager


def get_kernel_manager_if_initialized() -> KernelManager | None:
    """The manager if one was already constructed; never constructs one."""
    return _manager
