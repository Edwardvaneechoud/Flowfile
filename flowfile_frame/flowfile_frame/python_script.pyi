# Auto-generated stub for flowfile_frame.python_script — do not edit.
# Run `make stubs` to regenerate from the Python source.
from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_frame.native import NativeNode
from polars._typing import PolarsDataType
from flowfile_frame.flow_frame import FlowFrame

class PythonScript(NativeNode):
    code: str
    cells: list[str]
    kernel: str | None
    def __init__(self, *inputs: FlowFrame, code: str | None=None, cells: list[str] | None=None, kernel: str | Any | None=None, outputs: list[str] | None=None, schemas: Mapping[str, Mapping[str, PolarsDataType]] | None=None, description: str | None=None, flow_graph: FlowGraph | None=None) -> None: ...

class PythonScriptFunction:
    fn: Callable[..., Any]
    cells: list[str]
    def __init__(self, fn: Callable[..., Any], *, kernel: str | Any | None=None, outputs: list[str] | None=None, returns: Mapping[str, Any] | None=None, description: str | None=None, flow_graph: FlowGraph | None=None) -> None: ...
    def __call__(self, *frames: FlowFrame) -> FlowFrame: ...
    def node(self, *frames: FlowFrame) -> PythonScript: ...


def python_script(*, kernel: str | Any | None=None, outputs: list[str] | None=None, returns: Mapping[str, PolarsDataType] | Mapping[str, Mapping[str, PolarsDataType]] | None=None, description: str | None=None, flow_graph: FlowGraph | None=None) -> Callable[[Callable[..., Any]], PythonScriptFunction]: ...
