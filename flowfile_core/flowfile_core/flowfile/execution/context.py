"""Where a run executes: the backend, how many nodes of a stage run at once, and the preview size.

``current_context`` resolves in this order: the context a ``use_context`` block set in the calling context,
else the context of the graph passed in, else a default on ``default_backend_id()``. A ``FlowGraph`` derives
its own from its settings on each access, on the backend its ``execution_location`` names (``"auto"`` and
``"remote"`` name the default one); ``run_graph`` sets it for the run, and every stage thread runs under
a copy of the run's context, so a node sees it on whichever thread executes it.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING

from flowfile_core.flowfile.execution import registry

if TYPE_CHECKING:
    from flowfile_core.flowfile.execution.protocol import ComputeBackend
    from flowfile_core.flowfile.flow_graph.graph import FlowGraph
    from flowfile_core.schemas.schemas import FlowSettings

DEFAULT_PREVIEW_ROWS = 100


@dataclass(frozen=True)
class ExecutionContext:
    """Where a run executes.

    Attributes:
        backend_id: The registry id of the backend the run's work goes to.
        parallelism: How many nodes of one stage may run at once.
        preview_rows: How many rows a node's preview holds; ``None`` when the run keeps no previews.
    """

    backend_id: str
    parallelism: int
    preview_rows: int | None

    @property
    def backend(self) -> ComputeBackend:
        """The backend ``backend_id`` names, looked up in the registry on each access."""
        return registry.backend(self.backend_id)

    @classmethod
    def for_flow(cls, settings: FlowSettings) -> ExecutionContext:
        """The context of a flow with these settings.

        An ``"auto"`` or ``"remote"`` flow runs on the default backend, a flow with any other location on the
        backend of that id (``"local"`` on the worker embedded in this process, ``registry.LOCAL_BACKEND_ID``;
        ``"worker:team"`` on ``worker:team``), which is not looked up here. The embedded worker's jobs run in this
        process, where the registry makes it, so a stage runs one node at a time on it; on any other backend up to
        ``max_parallel_workers`` at once. Development mode keeps previews of ``DEFAULT_PREVIEW_ROWS`` rows, Performance
        mode none.
        """
        location = settings.execution_location
        backend_id = registry.default_backend_id() if location in ("auto", "remote") else location
        return cls(
            backend_id=backend_id,
            parallelism=1 if backend_id == registry.LOCAL_BACKEND_ID else settings.max_parallel_workers,
            preview_rows=DEFAULT_PREVIEW_ROWS if settings.execution_mode == "Development" else None,
        )


_current: ContextVar[ExecutionContext | None] = ContextVar("execution_context", default=None)


def current_context(graph: FlowGraph | None = None) -> ExecutionContext:
    """The context set by ``use_context``, else ``graph``'s, else one on the default backend."""
    context = _current.get()
    if context is not None:
        return context
    if graph is not None:
        return graph.execution_context
    return ExecutionContext(backend_id=registry.default_backend_id(), parallelism=1, preview_rows=DEFAULT_PREVIEW_ROWS)


@contextmanager
def use_context(context: ExecutionContext) -> Iterator[ExecutionContext]:
    """Make ``context`` the current one for the block, restoring the previous one after it."""
    token = _current.set(context)
    try:
        yield context
    finally:
        _current.reset(token)
