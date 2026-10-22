"""Whether the catalog services' table I/O goes over HTTP to a worker or runs in this process.

The catalog services send a table's metadata read, its previews and Delta history, its optimize and vacuum, and its
edits to the worker at ``registry.worker_url()``, or run them in this process, by ``uses_worker``, so the choice follows
the execution context like every other piece of compute. They run in this process where the context's backend is
``local``, the worker embedded in this process: a library host (a scheduled run, Flowfile in Python) has no worker to
post to, and these reads are that embedded worker's catalog work. A serving core never defaults to ``local``, so its
catalog I/O goes to its worker. A worker that cannot be reached is a ``ComputeUnavailable``, never a read in this
process instead.
"""

from __future__ import annotations

from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.context import current_context


def uses_worker() -> bool:
    """Whether catalog I/O goes over HTTP to the worker: the current execution context's backend is not ``local``.

    Outside any ``use_context`` block this is the default backend (``registry.default_backend_id``): the one
    ``FLOWFILE_COMPUTE_BACKEND`` names, else the worker while the legacy offload setting is on and the embedded worker
    when it is off, which a process that serves never defaults to.
    """
    return current_context().backend.id != registry.LOCAL_BACKEND_ID
