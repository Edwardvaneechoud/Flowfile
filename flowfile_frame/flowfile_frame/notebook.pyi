# Auto-generated stub for flowfile_frame.notebook — do not edit.
# Run `make stubs` to regenerate from the Python source.
from __future__ import annotations

import contextlib
from flowfile_core.flowfile.flow_graph import FlowGraph

class NotebookMode:
    graph: FlowGraph
    user_id: int | None
    refusals: list[str]
    provenance: list[tuple[str, str, int]]
    def __init__(self, graph: FlowGraph, user_id: int | None=None) -> None: ...


def current() -> NotebookMode | None: ...
def refuse(what: str) -> None: ...
def enter(graph: FlowGraph | None=None, user_id: int | None=None) -> NotebookMode: ...
def exit() -> None: ...
def notebook_mode(graph: FlowGraph | None=None, user_id: int | None=None) -> contextlib.AbstractContextManager[NotebookMode]: ...
