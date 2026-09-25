"""Declare and set flow parameters (the ``${name}`` values gates and node settings read).

Both helpers take the graph or any frame on it. A cross-graph merge (joining frames from two
graphs, or a native node over them) replaces the graph object, so after one pass a frame, or
re-read ``frame.flow_graph``, rather than keeping an old ``FlowGraph`` handle.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantic import ValidationError

from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.param_types import FlowParameter, coerce_param_value, stringify_param_value
from flowfile_frame.enums import ParamType, ParamTypeLiteral, _literal
from flowfile_frame.native import NativeNodeError

if TYPE_CHECKING:
    from flowfile_frame.flow_frame import FlowFrame


def _graph_of(flow: FlowGraph | FlowFrame) -> FlowGraph:
    graph = flow if isinstance(flow, FlowGraph) else getattr(flow, "flow_graph", None)
    if not isinstance(graph, FlowGraph):
        raise NativeNodeError(f"Expected a FlowGraph or a FlowFrame, got {type(flow).__name__}")
    return graph


def _as_parameter_string(value: Any) -> str:
    """The stored string form of a parameter value: booleans lowercase, ``None`` empty."""
    value = _literal(value)
    return "" if value is None else stringify_param_value(value)


def add_flow_parameter(
    flow: FlowGraph | FlowFrame,
    name: str,
    *,
    default: Any = "",
    type: ParamTypeLiteral | ParamType = "string",
    description: str = "",
    enum_values: list[str] | None = None,
) -> FlowParameter:
    """Declare a flow parameter and return it.

    ``default`` is stored as a string and checked against ``type`` (``enum`` needs
    ``enum_values``). A name that is already declared raises.
    """
    graph = _graph_of(flow)
    if any(p.name == name for p in graph.flow_settings.parameters):
        raise NativeNodeError(f"Flow parameter {name!r} is already declared; change it with fl.set_flow_parameter")
    try:
        parameter = FlowParameter(
            name=name,
            default_value=_as_parameter_string(default),
            description=description,
            type=_literal(type),
            enum_values=enum_values,
        )
    except ValidationError as exc:
        raise NativeNodeError(f"Invalid flow parameter {name!r}: {exc}") from exc
    graph.flow_settings.parameters.append(parameter)
    return parameter


def set_flow_parameter(flow: FlowGraph | FlowFrame, name: str, value: Any) -> None:
    """Set the value of a declared flow parameter (its stored default), checked against its type."""
    graph = _graph_of(flow)
    parameter = next((p for p in graph.flow_settings.parameters if p.name == name), None)
    if parameter is None:
        declared = [p.name for p in graph.flow_settings.parameters]
        raise NativeNodeError(
            f"Flow parameter {name!r} is not declared (declared: {declared}); add it with fl.add_flow_parameter"
        )
    raw = _as_parameter_string(value)
    if raw != "":
        try:
            coerce_param_value(parameter.type, raw, parameter.enum_values)
        except ValueError as exc:
            raise NativeNodeError(f"Flow parameter {name!r}: {exc}") from exc
    parameter.default_value = raw
