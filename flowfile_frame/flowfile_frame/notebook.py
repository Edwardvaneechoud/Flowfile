"""Notebook build mode: the frame builds onto one session graph and never runs, writes or registers.

While the mode is active every implicit graph (a source without ``flow_graph=``, ``ff.Node``
without inputs) is the session graph and a merge with any other graph is refused; every node
``native.notebook_defers`` names is seeded instead of executed at build; calls that write at
build, register a node type or run a flow raise ``NativeNodeError``; ``get_kernel_manager()``
raises, schema prefetches are blocked and every decorated ``add_*`` first passes the checks of
``flowfile_core.notebook.prechecks``. A *sync* (``sync=True``) also holds every node that would
read, connect, walk, run code or need data to predict, and seeds it without running anything
(``native.sync_seed_schemas``); the one read left is what the canvas reads to show a schema.

The mode is context-local (a ``ContextVar``): it holds for the context that entered it and for
copies of that context (schema callbacks), never for other threads or requests. The node-id
counter is the one process-wide piece; :data:`RUN_LOCK` serializes whole runs in a shared
process. Scripts outside the mode are unchanged. This module imports no other ``flowfile_frame``
module at import time, so every frame module can import :func:`current` at module level.
"""

from __future__ import annotations

import contextlib
import linecache
import logging
import threading
from collections.abc import Iterator
from contextvars import ContextVar, Token
from typing import Any, NoReturn

from flowfile_core.configs.flow_logger import FlowLogger, get_flow_log_file
from flowfile_core.flowfile.flow_graph import FlowGraph, placement_check
from flowfile_core.flowfile.flow_node.flow_node import schema_prefetch_blocked
from flowfile_core.flowfile.utils import create_unique_id

logger = logging.getLogger(__name__)

KERNEL_REFUSAL = "kernel nodes run on the canvas: use Run on canvas"

RUN_LOCK = threading.Lock()
"""Serializes notebook runs (a seed plus a clean run) that share a process, such as a server's.

The mode is context-local, so concurrent runs would not see each other's mode; the lock keeps
the rest one run at a time: the process-wide node-id counter, and one run's CPU and memory.
The runner takes it around the whole run. :func:`enter`, :func:`exit`, ``seed_session`` and
``clean_run`` never take it: a clean run sets a seeded mode aside and activates it again
inside the same run.
"""


class NotebookMode:
    """The active notebook build mode: the session graph, the session's user id, and what cells did.

    ``provenance`` holds ``(cell_id, node_type, node_id)`` for every node a cell created on the
    session graph; ``refusals`` every message raised through :func:`refuse`. Other notebook-mode
    errors (writer fallbacks, ``sink_*``, deferred ``collect()``, cross-graph merges, the refused
    ``run_graph`` and the kernel manager refusal) are raised directly and not recorded.
    ``snapshot`` holds the canvas nodes ``ff.canvas_node`` adopts and a sync seeds from
    (``notebook_cells.seed_session`` or ``enter_snapshot_session`` fills it), ``cell_files`` the
    ``linecache`` names of the cells run in the mode. ``owns_graph`` is set when :func:`enter`
    created the session graph. ``sync`` marks a sync; there ``expected`` maps a cell to the
    ``(node_type, canvas_id)`` pairs it rendered, ``cell_id`` and ``cell_nodes`` name the running
    cell and the node ids it created so far (in creation order), ``claimed`` maps each of those
    nodes to the canvas id it took as its twin, ``column_less`` holds the nodes seeded without
    columns, and ``unchecked`` the nodes seeded without columns because their build failed below
    one (node id -> ``(cell_id, node_type, error)``).
    """

    graph: FlowGraph
    user_id: int | None
    refusals: list[str]
    provenance: list[tuple[str, str, int]]
    snapshot: dict[int, Any]
    cell_files: list[str]
    owns_graph: bool
    sync: bool
    expected: dict[str, list[tuple[str, int]]]
    cell_id: str | None
    cell_nodes: list[int]
    claimed: dict[int, int]
    column_less: set[int]
    unchecked: dict[int, tuple[str | None, str, str]]

    def __init__(
        self, graph: FlowGraph, user_id: int | None = None, *, owns_graph: bool = False, sync: bool = False
    ) -> None:
        self.graph = graph
        self.user_id = user_id
        self.refusals = []
        self.provenance = []
        self.snapshot = {}
        self.cell_files = []
        self.owns_graph = owns_graph
        self.sync = sync
        self.expected = {}
        self.cell_id = None
        self.cell_nodes = []
        self.claimed = {}
        self.column_less = set()
        self.unchecked = {}

    def close(self) -> None:
        """Release what the mode's run left: the snapshot, the cells' ``linecache`` entries and an owned graph's logger.

        An owned session graph's flow id was picked free of any flow logger and log file, so its
        logger (kept in a class-wide registry with an open file) and its log file are discarded.
        A log file that cannot be removed is logged and left, so closing never raises from it.
        """
        self.snapshot.clear()
        for filename in self.cell_files:
            linecache.cache.pop(filename, None)
        self.cell_files.clear()
        if self.owns_graph:
            FlowLogger.cleanup_instance(self.graph.flow_id)
            log_file = get_flow_log_file(self.graph.flow_id)
            try:
                log_file.unlink(missing_ok=True)
            except OSError as exc:
                logger.warning("Could not remove the notebook session log %s: %s", log_file, exc)


_ACTIVE: ContextVar[NotebookMode | None] = ContextVar("notebook_mode", default=None)
_TOKENS: ContextVar[tuple[tuple[ContextVar, Token], ...] | None] = ContextVar("notebook_mode_tokens", default=None)


def current() -> NotebookMode | None:
    """The notebook mode active in this context, or ``None`` when the frame runs as a script."""
    return _ACTIVE.get()


def refuse(what: str, reason: str = "writes at build or runs a flow") -> None:
    """Raise ``NativeNodeError`` for ``what`` when notebook mode is active; a no-op otherwise.

    The message reads ``"<what> <reason>, so it is not available in a notebook"``. It is also
    recorded on the mode's ``refusals``, so a clean run reports a refusal that user code caught.
    """
    mode = current()
    if mode is None:
        return
    from flowfile_frame.native import NativeNodeError

    message = f"{what} {reason}, so it is not available in a notebook: run it from a script"
    mode.refusals.append(message)
    raise NativeNodeError(message)


def _check_placement(settings: Any) -> None:
    """Refuse a placement the canvas's path or connection rules refuse (recorded), as the user the settings carry.

    That is ``_identity.current_user_id()``: the mode's user, else 1, as every frame call stamps it.
    """
    from flowfile_core.notebook.prechecks import placement_refusal
    from flowfile_frame._identity import current_user_id
    from flowfile_frame.native import NativeNodeError

    mode = current()
    if mode is None:
        return
    message = placement_refusal(settings, current_user_id())
    if message is not None:
        mode.refusals.append(message)
        raise NativeNodeError(message)


def _refuse_kernel_manager() -> NoReturn:
    from flowfile_frame.native import NativeNodeError

    raise NativeNodeError(KERNEL_REFUSAL)


def _refused_run_graph(*args: Any, **kwargs: Any) -> Any:
    from flowfile_frame.native import NativeNodeError

    raise NativeNodeError(
        "The session graph does not run in a notebook: use Run on canvas, or run the flow from a script"
    )


def _refuse_nesting() -> None:
    if current() is not None:
        from flowfile_frame.native import NativeNodeError

        raise NativeNodeError("Notebook mode is already active; exit it before entering it again")


def _scratch_graph() -> FlowGraph:
    """A new local, history-off graph whose flow id no flow logger or log file uses yet."""
    from flowfile_frame.utils import create_flow_graph

    flow_id = create_unique_id()
    while FlowLogger.get_instance(flow_id) is not None or get_flow_log_file(flow_id).exists():
        flow_id = create_unique_id()
    return create_flow_graph(flow_id)


def enter(graph: FlowGraph | None = None, user_id: int | None = None, *, sync: bool = False) -> NotebookMode:
    """Activate notebook mode in this context on ``graph`` (a new local, history-off graph when omitted).

    The session graph's ``run_graph`` is replaced on the instance, ``get_kernel_manager()``
    refuses, schema prefetches are blocked and placements are checked in this context;
    :func:`exit` undoes all four. The node-id counter is moved past the graph's ids so fluent
    sources never reuse a canvas node's id. ``sync`` makes the mode a sync (see the module
    docstring). Modes do not nest: the check runs before a graph is created. The graph a mode
    creates is discarded when the mode ends (its flow logger and log file are released); pass
    your own graph to keep using it afterwards.
    """
    _refuse_nesting()
    if graph is None:
        return _activate(NotebookMode(_scratch_graph(), user_id, owns_graph=True, sync=sync))
    return _activate(NotebookMode(graph, user_id, sync=sync))


def _activate(mode: NotebookMode) -> NotebookMode:
    """Make ``mode`` the active one in this context (a new mode, or one a clean run set aside) and return it.

    The caller makes sure no mode is active. The tokens of every ``set`` call are kept in this
    context, so a copy of it that activates a mode of its own never replaces them.
    """
    import flowfile_core.kernel as kernel_package
    from flowfile_frame.utils import data

    data["c"] = max(data["c"], max((n.node_id for n in mode.graph.nodes), default=0))
    mode.graph.run_graph = _refused_run_graph
    settings = (
        (_ACTIVE, mode),
        (kernel_package.kernel_manager_refusal, _refuse_kernel_manager),
        (schema_prefetch_blocked, True),
        (placement_check, _check_placement),
    )
    _TOKENS.set(tuple((variable, variable.set(value)) for variable, value in settings))
    return mode


def _restore(variable: ContextVar, token: Token | None) -> None:
    """Undo the ``variable.set`` that returned ``token``: a reset where it was set, else its previous value.

    Modes never nest in a context, so in the context that set it the reset is last in, first out.
    A copy of that context inherits the mode with its tokens but cannot reset them, and a missing
    token cannot be reset at all: both fall back to setting the previous value.
    """
    if token is None:
        variable.set(None)
        return
    try:
        variable.reset(token)
    except ValueError:
        variable.set(None if token.old_value is Token.MISSING else token.old_value)


def _deactivate() -> NotebookMode | None:
    """Leave the active mode without releasing what it holds, and return it (``None`` when none is active).

    Restores the graph's ``run_graph`` and every context variable :func:`_activate` set, last set
    first. A clean run sets a seeded mode aside this way and activates it again.
    """
    import flowfile_core.kernel as kernel_package

    mode = current()
    if mode is None:
        return None
    tokens = _TOKENS.get()
    _TOKENS.set(None)
    if tokens is None:
        tokens = tuple(
            (variable, None)
            for variable in (_ACTIVE, kernel_package.kernel_manager_refusal, schema_prefetch_blocked, placement_check)
        )
    for variable, token in reversed(tokens):
        _restore(variable, token)
    mode.graph.__dict__.pop("run_graph", None)
    return mode


def exit() -> None:
    """End the active mode: deactivate it (restoring ``run_graph`` and the kernel manager), then close it.

    Closing releases the snapshot, the cells' ``linecache`` entries and an owned session graph's
    flow logger (:meth:`NotebookMode.close`).
    """
    mode = _deactivate()
    if mode is not None:
        mode.close()


def notebook_mode(
    graph: FlowGraph | None = None, user_id: int | None = None, *, sync: bool = False
) -> contextlib.AbstractContextManager[NotebookMode]:
    """Context manager around :func:`enter` / :func:`exit`; ``with notebook_mode() as mode: ...``.

    The graph a mode creates is discarded when the mode ends (its flow logger and log file are
    released); pass your own graph to keep using it afterwards.
    """
    return _notebook_mode(graph, user_id, sync)


@contextlib.contextmanager
def _notebook_mode(graph: FlowGraph | None, user_id: int | None, sync: bool) -> Iterator[NotebookMode]:
    mode = enter(graph, user_id, sync=sync)
    try:
        yield mode
    finally:
        exit()
