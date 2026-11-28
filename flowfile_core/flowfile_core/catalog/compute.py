"""Whether the catalog services' table I/O goes over HTTP to a worker or runs in this process.

The catalog services send a table's metadata read, previews, Delta history, optimize, vacuum and edits to the worker at
``registry.worker_url()``, or run them in this process, by ``uses_worker``. They run here where the context's backend is
``local``: a library host (a scheduled run, Flowfile in Python) has no worker to post to. A worker that cannot be
reached is a ``ComputeUnavailable``, never a read in this process instead.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.context import current_context

if TYPE_CHECKING:
    from flowfile_core.flowfile.execution.protocol import ComputeBackend


def uses_worker() -> bool:
    """Whether catalog I/O goes over HTTP to the worker: the current execution context's backend is not ``local``.

    Outside any ``use_context`` block this is the default backend (``registry.default_backend_id``).
    """
    return current_context().backend.id != registry.LOCAL_BACKEND_ID


def reads_core_disk(backend: ComputeBackend) -> bool:
    """Whether ``backend`` reads paths on this process's disk: ``file`` is among the schemes it reads.

    Taken from the backend's cached handshake verdict, else asked of it once. A worker on another host does not, so
    a plan that scans a local path fails there; the caller refuses such a plan instead of sending it.

    Raises:
        ComputeError: The backend had to be asked and did not answer.
    """
    from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake

    verdict = handshake.last_verdict(backend.id)
    reported = verdict.backend if verdict is not None else None
    if reported is None:
        reported = backend.capabilities()
    return "file" in reported.read_schemes
