"""Cycle detection for the Run-flow node.

A sub-flow that (directly or transitively) runs the flow already executing would
loop forever. This tracks the resolved flow paths currently on the execution
stack via a ``ContextVar`` (which nested synchronous ``run_graph`` calls share)
and refuses to re-enter one.
"""

from __future__ import annotations

import contextvars
from collections.abc import Iterator
from contextlib import contextmanager

_active_flow_paths: contextvars.ContextVar[frozenset[str]] = contextvars.ContextVar(
    "run_flow_active_paths", default=frozenset()
)


class RecursiveSubFlowError(Exception):
    """Raised when a Run-flow node would re-enter a flow already executing."""


@contextmanager
def guard_sub_flow(flow_path: str) -> Iterator[None]:
    """Mark *flow_path* as executing for the duration of the block.

    Raises:
        RecursiveSubFlowError: if *flow_path* is already on the execution stack.
    """
    active = _active_flow_paths.get()
    if flow_path in active:
        raise RecursiveSubFlowError(
            f"Detected recursive sub-flow execution: '{flow_path}' is already running. "
            "A Run-flow node cannot (directly or transitively) run the flow that contains it."
        )
    token = _active_flow_paths.set(active | {flow_path})
    try:
        yield
    finally:
        _active_flow_paths.reset(token)
