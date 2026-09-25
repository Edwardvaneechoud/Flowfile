# Auto-generated stub for flowfile_frame.python_script — do not edit.
# Run `make stubs` to regenerate from the Python source.
from __future__ import annotations

from typing import Any
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_frame.native import NativeNode
from flowfile_frame.flow_frame import FlowFrame

class PythonScript(NativeNode):
    code: str
    cells: list[str]
    kernel: str | None
    def __init__(self, *inputs: FlowFrame, code: str | None=None, cells: list[str] | None=None, kernel: str | Any | None=None, outputs: list[str] | None=None, description: str | None=None, flow_graph: FlowGraph | None=None) -> None: ...

