# Auto-generated stub for flowfile_frame.notebook_cells — do not edit.
# Run `make stubs` to regenerate from the Python source.
from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_frame.flow_frame import FlowFrame
from flowfile_frame.native import NativeNode
from flowfile_frame.run_flow import FlowOutput
from shared.notebook_display import DISPLAY_MAX_ROWS

LIVE_SOURCE_TYPES: frozenset[str]
SEEDED_NODE_TYPES: frozenset[str]
KEPT_NODE_TYPES: frozenset[str]

class CellResult:
    cell_id: str
    filename: str
    error: str | None
    display: dict[str, Any] | None
    outputs: list[dict[str, Any]]
    created: list[tuple[str, int]]
    names: list[str]
    references: dict[int, str]
    @property
    def ok(self) -> bool: ...

class SeededNode(NativeNode):
    node_id: int
    node_type: str
    flow_graph: FlowGraph
    output_names: list[str]
    def __init__(self, flow_graph: FlowGraph, node_id: int, node_type: str, output_names: list[str], frames: dict[str, FlowFrame]) -> None: ...
    @property
    def then(self) -> FlowFrame: ...
    @property
    def otherwise(self) -> FlowFrame: ...
    @property
    def else_(self) -> FlowFrame: ...
    @property
    def output(self) -> FlowFrame: ...
    def __getitem__(self, name: str | FlowOutput) -> FlowFrame: ...
    def __getattr__(self, name: str) -> Any: ...


def seed_session(flowfile_data: dict[str, Any], parameters: list[Any], names: Mapping[int, str], schemas: Mapping[int, Mapping[str, list[Any]]], *, user_id: int | None=None) -> dict[str, Any]: ...
def canvas_node(node_id: int, *inputs: FlowFrame, output: str | FlowOutput | None=None) -> FlowFrame | SeededNode: ...
def display_payload(value: Any, max_rows: int=DISPLAY_MAX_ROWS) -> dict[str, Any]: ...
def display(value: Any) -> dict[str, Any] | None: ...
def new_namespace() -> dict[str, Any]: ...
def execute_cell(cell_id: str, code: str, namespace: dict[str, Any]) -> CellResult: ...
def clean_run(cells: list[tuple[str, str]], ceiling: int, provenance: Mapping[str, list[tuple[str, int]]] | None=None) -> dict[str, Any]: ...
