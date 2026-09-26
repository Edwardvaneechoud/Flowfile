# Auto-generated stub for flowfile_frame.gate — do not edit.
# Run `make stubs` to regenerate from the Python source.
from __future__ import annotations

from typing import Any
from flowfile_frame.enums import GateOperator, GateOperatorLiteral
from flowfile_frame.expr import Expr
from flowfile_frame.native import NativeNode
from flowfile_frame.parameters import Parameter
from flowfile_frame.flow_frame import FlowFrame

class Gate(NativeNode):
    def __init__(self, frame: FlowFrame, formula: str | Expr | None=None, *, parameter: str | Parameter | None=None, operator: GateOperatorLiteral | GateOperator='equals', value: Any=None, control: FlowFrame | None=None, else_output: bool=True, description: str | None=None) -> None: ...
    @property
    def then(self) -> FlowFrame: ...
    @property
    def otherwise(self) -> FlowFrame: ...
    @property
    def else_(self) -> FlowFrame: ...
    @property
    def output(self) -> FlowFrame: ...
    @property
    def is_open(self) -> bool: ...

