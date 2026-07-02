"""Backend registry: map an execution location to an ExecutionBackend."""

from __future__ import annotations

from flowfile_core.flowfile.execution.backends.base import ExecutionBackend
from flowfile_core.flowfile.execution.backends.local import LocalBackend
from flowfile_core.flowfile.execution.backends.worker import RemoteWorkerBackend
from flowfile_core.flowfile.execution.transport import WorkerTransport

_local_backend = LocalBackend()
_worker_backend: RemoteWorkerBackend | None = None


def resolve_backend(location: str, transport: WorkerTransport | None = None) -> ExecutionBackend:
    """Return the backend for an execution location ("local" or "remote")."""
    if location == "local":
        return _local_backend
    if transport is not None:
        return RemoteWorkerBackend(transport=transport)
    global _worker_backend
    if _worker_backend is None:
        _worker_backend = RemoteWorkerBackend()
    return _worker_backend


__all__ = ["ExecutionBackend", "LocalBackend", "RemoteWorkerBackend", "resolve_backend"]
