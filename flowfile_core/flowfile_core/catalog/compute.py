"""Whether the catalog services' table I/O goes over HTTP to a worker or runs in this process.

The catalog services send a table's metadata read, previews, Delta history, optimize, vacuum and edits to the worker at
``registry.worker_url()``, or run them in this process, by ``uses_worker``. They run here where the context's backend is
``local``: a library host (a scheduled run, Flowfile in Python) has no worker to post to. A worker that cannot be
reached is a ``ComputeUnavailable``, never a read in this process instead.
"""

from __future__ import annotations

from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.context import current_context


def uses_worker() -> bool:
    """Whether catalog I/O goes over HTTP to the worker: the current execution context's backend is not ``local``.

    Outside any ``use_context`` block this is the default backend (``registry.default_backend_id``).
    """
    return current_context().backend.id != registry.LOCAL_BACKEND_ID
