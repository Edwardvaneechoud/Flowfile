"""Notebook build mode: the frame builds onto one session graph and never runs, writes or registers.

A canvas notebook session runs cells in-process against one graph (the *session graph*). While
the mode is active:

- every implicit graph (a source without ``flow_graph=``, ``fl.Node`` without inputs) is the
  session graph, so new sources never renumber canvas nodes, and a merge with any other graph
  is refused;
- every node ``native.notebook_defers`` names is seeded from its predicted schema
  instead of executed at build (writers, subflows, kernel scripts, database / REST / Kafka
  sources, ``pivot``, ``polars_code``, virtual and SQL-mode catalog readers);
- calls that write YAML, DB rows or files at build, or that run a flow, raise ``NativeNodeError``
  (``register_flow``, ``RunFlow(<graph>, name=...)``, ``fl.custom_nodes.install``, the
  connection helpers, ``fl.open_graph_in_editor``, the session graph's ``run_graph`` and
  ``collect()`` on a deferred frame);
- ``flowfile_core.kernel.KernelManager`` is a sentinel whose construction raises, so nothing in
  the session reaches Docker;
- ``add_flow_parameter`` on the session graph upserts.

Scripts outside the mode are unchanged. This module imports no other ``flowfile_frame`` module
at import time, so every frame module can import :func:`current` at module level.
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from typing import Any

from flowfile_core.flowfile.flow_graph import FlowGraph

KERNEL_REFUSAL = "kernel nodes run on the canvas: use Run on canvas"


class NotebookMode:
    """The active notebook build mode: the session graph, the session's user id, and what cells did.

    ``provenance`` holds ``(cell_id, node_type, node_id)`` for every node a cell created on the
    session graph; ``refusals`` every message raised through :func:`refuse`. Other notebook-mode
    errors (writer fallbacks, ``sink_*``, deferred ``collect()``, cross-graph merges, the refused
    ``run_graph`` and the ``KernelManager`` sentinel) are raised directly and not recorded.
    """

    graph: FlowGraph
    user_id: int | None
    refusals: list[str]
    provenance: list[tuple[str, str, int]]

    def __init__(self, graph: FlowGraph, user_id: int | None = None) -> None:
        self.graph = graph
        self.user_id = user_id
        self.refusals: list[str] = []
        self.provenance: list[tuple[str, str, int]] = []
        self._saved_kernel_manager: Any = None


_ACTIVE: list[NotebookMode] = []


def current() -> NotebookMode | None:
    """The active notebook mode, or ``None`` when the frame runs as a script."""
    return _ACTIVE[-1] if _ACTIVE else None


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


class _KernelManagerSentinel:
    """Stands in for ``KernelManager`` while notebook mode is active; constructing it raises."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        from flowfile_frame.native import NativeNodeError

        raise NativeNodeError(KERNEL_REFUSAL)


def _refused_run_graph(*args: Any, **kwargs: Any) -> Any:
    from flowfile_frame.native import NativeNodeError

    raise NativeNodeError(
        "The session graph does not run in a notebook: use Run on canvas, or run the flow from a script"
    )


def enter(graph: FlowGraph | None = None, user_id: int | None = None) -> NotebookMode:
    """Activate notebook mode on ``graph`` (a new local, history-off graph when omitted) and return it.

    The session graph's ``run_graph`` is replaced on the instance and the kernel package's
    ``KernelManager`` by a sentinel; :func:`exit` restores both. The node-id counter is moved
    past the graph's ids so fluent sources never reuse a canvas node's id. Modes do not nest.
    """
    from flowfile_frame.utils import create_flow_graph

    return _activate(NotebookMode(graph if graph is not None else create_flow_graph(), user_id))


def _activate(mode: NotebookMode) -> NotebookMode:
    """Make ``mode`` the active one (a new mode, or one a clean run set aside) and return it."""
    import flowfile_core.kernel as kernel_package
    from flowfile_frame.native import NativeNodeError
    from flowfile_frame.utils import data

    if _ACTIVE:
        raise NativeNodeError("Notebook mode is already active; exit it before entering it again")
    data["c"] = max(data["c"], max((n.node_id for n in mode.graph.nodes), default=0))
    mode.graph.run_graph = _refused_run_graph
    mode._saved_kernel_manager = kernel_package.KernelManager
    kernel_package.KernelManager = _KernelManagerSentinel
    _ACTIVE.append(mode)
    return mode


def exit() -> None:
    """Deactivate notebook mode, restoring the graph's ``run_graph`` and the kernel manager class."""
    import flowfile_core.kernel as kernel_package

    if not _ACTIVE:
        return
    mode = _ACTIVE.pop()
    mode.graph.__dict__.pop("run_graph", None)
    kernel_package.KernelManager = mode._saved_kernel_manager


def notebook_mode(
    graph: FlowGraph | None = None, user_id: int | None = None
) -> contextlib.AbstractContextManager[NotebookMode]:
    """Context manager around :func:`enter` / :func:`exit`; ``with notebook_mode() as mode: ...``."""
    return _notebook_mode(graph, user_id)


@contextlib.contextmanager
def _notebook_mode(graph: FlowGraph | None, user_id: int | None) -> Iterator[NotebookMode]:
    mode = enter(graph, user_id)
    try:
        yield mode
    finally:
        exit()
