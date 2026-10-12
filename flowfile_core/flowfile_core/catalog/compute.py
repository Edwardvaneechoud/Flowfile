"""Where the catalog services run their heavy table I/O: in core, or on the worker.

The catalog services send a table's metadata read, its previews and Delta history, its optimize and vacuum, and its
edits to the worker or run them in this process, and run a virtual table's producer flow remote or local when the
caller names no location. Each decides through ``uses_worker`` alone, so the choice follows the execution context
like every other piece of compute: a block under ``use_context`` with core's own backend keeps catalog I/O in core,
and so does a deployment whose default backend is core's own.
"""

from __future__ import annotations

from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.context import current_context


def uses_worker() -> bool:
    """Whether catalog I/O goes to the worker: the current execution context's backend is not core's own.

    Outside any ``use_context`` block this is the default backend (``registry.default_backend_id``): the one
    ``FLOWFILE_COMPUTE_BACKEND`` names, else the worker while the legacy offload setting is on and core when it is
    off.
    """
    return current_context().backend.id != registry.LOCAL_BACKEND_ID
