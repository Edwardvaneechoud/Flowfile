# Auto-generated stub for flowfile_frame.parameters — do not edit.
# Run `make stubs` to regenerate from the Python source.
from __future__ import annotations

from typing import Any
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.param_types import FlowParameter
from flowfile_frame.enums import ParamType, ParamTypeLiteral
from flowfile_frame.flow_frame import FlowFrame

def add_flow_parameter(flow: FlowGraph | FlowFrame, name: str, *, default: Any='', type: ParamTypeLiteral | ParamType='string', description: str='', enum_values: list[str] | None=None) -> FlowParameter: ...
def set_flow_parameter(flow: FlowGraph | FlowFrame, name: str, value: Any) -> None: ...
