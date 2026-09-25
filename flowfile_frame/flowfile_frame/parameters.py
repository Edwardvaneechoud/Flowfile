"""Declare and set flow parameters (the ``${name}`` values gates and node settings read).

Both helpers take the graph or any frame on it. A cross-graph merge (joining frames from two
graphs, or a native node over them) replaces the graph object, so after one pass a frame, or
re-read ``frame.flow_graph``, rather than keeping an old ``FlowGraph`` handle.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

import polars as pl
from pydantic import ValidationError

from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.param_types import FlowParameter, ParamValue, coerce_param_value, stringify_param_value
from flowfile_core.flowfile.parameter_resolver import find_unresolved_in_model
from flowfile_frame.enums import ParamType, ParamTypeLiteral, _literal
from flowfile_frame.native import NativeNodeError

if TYPE_CHECKING:
    from flowfile_frame.expr import Expr
    from flowfile_frame.flow_frame import FlowFrame

_PARAM_DTYPES: dict[str, pl.DataType] = {
    "string": pl.String(),
    "enum": pl.String(),
    "integer": pl.Int64(),
    "float": pl.Float64(),
    "boolean": pl.Boolean(),
}


def _graph_of(flow: FlowGraph | FlowFrame) -> FlowGraph:
    graph = flow if isinstance(flow, FlowGraph) else getattr(flow, "flow_graph", None)
    if not isinstance(graph, FlowGraph):
        raise NativeNodeError(f"Expected a FlowGraph or a FlowFrame, got {type(flow).__name__}")
    return graph


def _as_parameter_string(value: Any) -> str:
    """The stored string form of a parameter value: booleans lowercase, ``None`` empty.

    A ``Parameter`` given as a value is its ``${name}`` reference, which the run substitutes.
    """
    if isinstance(value, Parameter):
        return value.ref
    value = _literal(value)
    return "" if value is None else stringify_param_value(value)


class Parameter:
    """A flow parameter, declared once and used wherever a value or a parameter name is taken.

    Holds core's validated ``FlowParameter`` (``model``): ``default`` is stored as a string and
    checked against ``type`` (``enum`` needs ``enum_values``). Add it to a graph with
    ``fl.add_flow_parameter(flow, parameter)``; use it as a value in
    expressions (``fl.col("amount") >= parameter``, ``fl.lit(parameter)``), in
    ``fl.Gate(parameter=...)``, ``fl.set_flow_parameter`` and as a ``fl.RunFlow(params=...)`` key.
    Nothing is looked up at construction: an expression over a parameter the graph does not
    declare fails when its node is built. Parameters are values, never column names.
    Equal and hashable by name.
    """

    model: FlowParameter

    def __init__(
        self,
        name: str,
        *,
        default: Any = "",
        type: ParamTypeLiteral | ParamType = "string",
        description: str = "",
        enum_values: list[str] | None = None,
    ) -> None:
        try:
            self.model = FlowParameter(
                name=name,
                default_value=_as_parameter_string(default),
                description=description,
                type=_literal(type),
                enum_values=enum_values,
            )
        except ValidationError as exc:
            raise NativeNodeError(f"Invalid flow parameter {name!r}: {exc}") from exc

    @property
    def name(self) -> str:
        """The parameter name."""
        return self.model.name

    @property
    def type(self) -> ParamTypeLiteral:
        """The parameter type (``string``, ``integer``, ``float``, ``boolean`` or ``enum``)."""
        return self.model.type

    @property
    def default(self) -> ParamValue:
        """The default value, typed (``""`` when there is none)."""
        return self.model.typed_default()

    @property
    def dtype(self) -> pl.DataType:
        """The Polars dtype of the value: ``String`` (string, enum), ``Int64``, ``Float64`` or ``Boolean``."""
        return _PARAM_DTYPES[self.model.type]

    @property
    def ref(self) -> str:
        """The ``${name}`` reference core substitutes."""
        return f"${{{self.name}}}"

    def to_expr(self) -> Expr:
        """An expression holding the parameter's value.

        Its formula form is the bare ``${name}``, which core renders as a typed literal, so a
        predicate over it still lowers onto a native Filter/Formula node. In Polars code it is the
        substituted text ``pl.lit("${name}")``, cast to the parameter's dtype (a boolean compares
        with ``"true"``: Polars cannot cast a string to a boolean). The live expression is a typed
        null; only schema checks read it.
        """
        from flowfile_frame.expr import Expr

        text = f'pl.lit("{self.ref}")'
        dtype = self.dtype
        if dtype == pl.Boolean:
            repr_str = f'({text} == "true")'
        elif dtype == pl.String:
            repr_str = text
        else:
            repr_str = Expr(None, repr_str=text).cast(dtype)._repr_str
        return Expr(pl.lit(None, dtype=dtype), repr_str=repr_str, agg_func=None, ff_repr=self.ref)

    def __repr__(self) -> str:
        return f"Parameter({self.name!r}, type={self.type!r}, default={self.default!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Parameter):
            return NotImplemented
        return self.name == other.name

    def __hash__(self) -> int:
        return hash(self.name)


def _param_name(parameter: str | Parameter) -> str:
    """The name of a parameter given by name or as a ``Parameter``."""
    return parameter.name if isinstance(parameter, Parameter) else parameter


def _column_parameter_error(where: str) -> NativeNodeError:
    return NativeNodeError(
        f"Flow parameters are values, not column names: `{where}` takes column names. "
        "Use them with fl.lit(...), in comparisons, in fl.Gate(parameter=...) or in fl.RunFlow(params=...)"
    )


def refuse_parameter_as_column(value: Any, where: str) -> None:
    """Raise when ``value`` puts a flow parameter where ``where`` takes column names.

    A parameter is a value: core substitutes ``${name}`` as text, so as a column name it becomes
    a broken reference or a literal column called ``${name}``. ``value`` is a ``Parameter``, a
    string, or any nesting of lists, tuples, sets and mappings (keys and values) of them; other
    objects (expressions, dtypes) pass.
    """
    if isinstance(value, Parameter) or (isinstance(value, str) and find_unresolved_in_model(value)):
        raise _column_parameter_error(where)
    if isinstance(value, Mapping):
        for key, item in value.items():
            refuse_parameter_as_column(key, where)
            refuse_parameter_as_column(item, where)
    elif isinstance(value, list | tuple | set | frozenset):
        for item in value:
            refuse_parameter_as_column(item, where)


def contains_parameter(value: Any) -> bool:
    """Whether ``value`` is a ``Parameter`` or nests one in lists, tuples, sets or mappings (keys and values)."""
    if isinstance(value, Parameter):
        return True
    if isinstance(value, Mapping):
        return any(contains_parameter(key) or contains_parameter(item) for key, item in value.items())
    if isinstance(value, list | tuple | set | frozenset):
        return any(contains_parameter(item) for item in value)
    return False


def refuse_parameter_column_in_formula(formula: str | None, where: str) -> None:
    """Raise when a flowfile formula names a column through a parameter (``[${name}]``).

    A bare ``${name}`` value in a formula stays supported; core renders it as a typed literal.
    """
    if formula and any(f"[${{{name}}}" in formula for name in find_unresolved_in_model(formula)):
        raise _column_parameter_error(where)


def add_flow_parameter(flow: FlowGraph | FlowFrame, parameter: Parameter) -> Parameter:
    """Declare ``parameter`` on the flow and return it.

    The graph stores its own copy of the declaration (``parameter.model``), so
    ``fl.set_flow_parameter`` on it never changes the ``Parameter`` or another graph it was
    added to. A name that is already declared raises.
    """
    graph = _graph_of(flow)
    if not isinstance(parameter, Parameter):
        raise NativeNodeError(
            f"add_flow_parameter takes a fl.Parameter, got {type(parameter).__name__}; "
            "declare it as fl.Parameter(name, default=..., type=...)"
        )
    if any(p.name == parameter.name for p in graph.flow_settings.parameters):
        raise NativeNodeError(
            f"Flow parameter {parameter.name!r} is already declared; change it with fl.set_flow_parameter"
        )
    graph.flow_settings.parameters.append(parameter.model.model_copy())
    return parameter


def set_flow_parameter(flow: FlowGraph | FlowFrame, name: str | Parameter, value: Any) -> None:
    """Set the value of a declared flow parameter (its stored default), checked against its type."""
    graph = _graph_of(flow)
    name = _param_name(name)
    parameter = next((p for p in graph.flow_settings.parameters if p.name == name), None)
    if parameter is None:
        declared = [p.name for p in graph.flow_settings.parameters]
        raise NativeNodeError(
            f"Flow parameter {name!r} is not declared (declared: {declared}); add it with fl.add_flow_parameter"
        )
    if isinstance(value, Parameter):
        raise NativeNodeError(
            f"Flow parameter {name!r}: a parameter's value cannot be another parameter ({value.ref}); "
            f"set a plain value, e.g. fl.set_flow_parameter(flow, {name!r}, {value.name}.default)"
        )
    raw = _as_parameter_string(value)
    if raw != "":
        try:
            coerce_param_value(parameter.type, raw, parameter.enum_values)
        except ValueError as exc:
            raise NativeNodeError(f"Flow parameter {name!r}: {exc}") from exc
    parameter.default_value = raw
