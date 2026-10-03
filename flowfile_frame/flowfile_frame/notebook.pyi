# Auto-generated stub for flowfile_frame.notebook — do not edit.
# Run `make stubs` to regenerate from the Python source.
from __future__ import annotations

import contextlib
from collections.abc import Callable
from typing import Any
from flowfile_core.flowfile.flow_graph import FlowGraph

class NotebookMode:
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
    row_resolver: Callable[[Any], Any] | None
    schema_resolver: Callable[[Any], dict[str, list] | None] | None
    def __init__(self, graph: FlowGraph, user_id: int | None=None, *, owns_graph: bool=False, sync: bool=False) -> None: ...
    def close(self) -> None: ...


def current() -> NotebookMode | None: ...
def refuse(what: str, reason: str='writes at build or runs a flow') -> None: ...
def enter(graph: FlowGraph | None=None, user_id: int | None=None, *, sync: bool=False) -> NotebookMode: ...
def exit() -> None: ...
def notebook_mode(graph: FlowGraph | None=None, user_id: int | None=None, *, sync: bool=False) -> contextlib.AbstractContextManager[NotebookMode]: ...
def resumed(mode: NotebookMode) -> contextlib.AbstractContextManager[NotebookMode]: ...
def paths_as_written() -> contextlib.AbstractContextManager[None]: ...
def translate_path(path: str, table: dict[str, str]) -> str | None: ...
def kernel_path(path: str) -> str | None: ...
