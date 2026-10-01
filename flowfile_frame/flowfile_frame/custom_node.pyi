# Auto-generated stub for flowfile_frame.custom_node — do not edit.
# Run `make stubs` to regenerate from the Python source.
from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_frame.native import NativeNode
from shared.node_designer.custom_node import CustomNodeBase
from polars._typing import PolarsDataType
from flowfile_frame.flow_frame import FlowFrame

class CustomNode(NativeNode):
    node_class: type[CustomNodeBase]
    settings: dict[str, dict[str, Any]]
    kernel: str | None
    def __init__(self, node: type[CustomNodeBase] | CustomNodeBase | str, *inputs: FlowFrame, settings: dict[str, dict[str, Any]] | None=None, kernel: str | Any | None=None, deferred: bool | None=None, schemas: Mapping[str, Mapping[str, PolarsDataType]] | None=None, description: str | None=None, flow_graph: FlowGraph | None=None) -> None: ...

class CustomNodeFactory:
    node_class: type[CustomNodeBase]
    node_type: str
    output_names: list[str]
    parameters: dict[str, tuple[str, str]]
    def __init__(self, node: type[CustomNodeBase] | CustomNodeBase | str) -> None: ...
    def __call__(self, *inputs: FlowFrame, kernel: str | Any | None=None, deferred: bool | None=None, schemas: Mapping[str, Mapping[str, PolarsDataType]] | None=None, description: str | None=None, settings: dict[str, dict[str, Any]] | None=None, flow_graph: FlowGraph | None=None, **components: Any) -> FlowFrame: ...
    def node(self, *inputs: FlowFrame, kernel: str | Any | None=None, deferred: bool | None=None, schemas: Mapping[str, Mapping[str, PolarsDataType]] | None=None, description: str | None=None, settings: dict[str, dict[str, Any]] | None=None, flow_graph: FlowGraph | None=None, **components: Any) -> CustomNode: ...


def custom_node(node: type[CustomNodeBase] | CustomNodeBase | str) -> CustomNodeFactory: ...
