"""``fl.Gate``: a pass-through whose downstream only runs when its condition holds."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantic import ValidationError

from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.flow_node.multi_output import output_handle
from flowfile_core.flowfile.param_types import FlowParameter, typed_parameter_values
from flowfile_core.flowfile.util.skip_rules import parameter_gate_is_open
from flowfile_core.schemas import input_schema, transform_schema
from flowfile_frame.enums import GateOperator, GateOperatorLiteral, _literal
from flowfile_frame.expr import Expr
from flowfile_frame.native import NativeNode, NativeNodeError
from flowfile_frame.parameters import (
    Parameter,
    _as_parameter_string,
    _param_name,
    refuse_parameter_column_in_formula,
)

if TYPE_CHECKING:
    from flowfile_frame.flow_frame import FlowFrame


def _gate_value(value: Any, operator: str) -> str:
    """The string the canvas stores: booleans lowercase, lists comma-joined for ``in``/``not_in``."""
    if isinstance(value, list | tuple | set | frozenset):
        if operator not in ("in", "not_in"):
            raise NativeNodeError(f"A list value needs operator 'in' or 'not_in', not {operator!r}")
        return ",".join(_as_parameter_string(item) for item in value)
    return _as_parameter_string(value)


def _formula_text(formula: str | Expr | None, flow_graph: FlowGraph) -> str | None:
    """A formula condition as text: a string as given, an expression through its formula form (as ``filter``)."""
    if formula is None or isinstance(formula, str):
        return formula
    if not isinstance(formula, Expr):
        raise NativeNodeError(
            f"Gate formula takes a flowfile formula string or an expression such as fl.col('a') > 1, "
            f"got {type(formula).__name__}"
        )
    from flowfile_frame.flow_frame import _filter_exprs_to_formula

    text = _filter_exprs_to_formula([formula], typed_parameter_values(flow_graph.flow_settings.parameters))
    if text is None:
        raise NativeNodeError(
            "This Gate condition has no flowfile formula form; build it from comparisons, and/or/not, is_in and "
            'is_null on columns, or pass the formula as a string such as "[a] > 1"'
        )
    return text


def _parameter_gate_is_open(gate_input: transform_schema.GateInput, parameters: list[FlowParameter]) -> bool:
    """Core's run-time evaluation (``parameter_gate_is_open``), its ``ValueError`` as ``NativeNodeError``."""
    try:
        return parameter_gate_is_open(gate_input, parameters)
    except ValueError as exc:
        raise NativeNodeError(f"Gate condition on parameter {gate_input.parameter!r}: {exc}") from exc


class Gate(NativeNode):
    """A gate node: its data input flows through, and its downstream only runs when the condition holds.

    Give exactly one condition: a ``formula`` (a flowfile formula, or an expression with a formula
    form such as ``fl.col("a") > 1``, that opens the gate when at least one row of ``control``,
    else of ``frame``, matches) or a flow ``parameter`` compared
    with ``operator`` and ``value`` (declare it first with ``fl.add_flow_parameter``). With
    ``else_output`` (the default) the gate routes: ``.then`` is live when the condition holds,
    ``.otherwise`` when it does not. Both exits are pass-through frames while building;
    ``collect()`` on any frame below the gate runs the flow and returns only the live side.
    """

    def __init__(
        self,
        frame: FlowFrame,
        formula: str | Expr | None = None,
        *,
        parameter: str | Parameter | None = None,
        operator: GateOperatorLiteral | GateOperator = "equals",
        value: Any = None,
        control: FlowFrame | None = None,
        else_output: bool = True,
        description: str | None = None,
    ) -> None:
        if (formula is None) == (parameter is None):
            raise NativeNodeError("Gate takes exactly one condition: a formula, or parameter=... with operator/value")
        formula = _formula_text(formula, frame.flow_graph)
        if formula is not None and not formula.strip():
            raise NativeNodeError("Gate formula is empty")
        refuse_parameter_column_in_formula(formula, "Gate formula")
        parameter = None if parameter is None else _param_name(parameter)
        if control is not None and formula is None:
            raise NativeNodeError("control= only applies to a formula gate; a parameter gate reads no data")
        operator = _literal(operator)
        self._probe = control if control is not None else frame

        def make_settings(base: dict[str, Any]) -> input_schema.NodeGate:
            try:
                gate_input = transform_schema.GateInput(
                    condition_source="formula" if formula is not None else "parameter",
                    parameter=parameter or "",
                    operator=operator,
                    value=_gate_value(value, operator),
                    formula=formula or "",
                )
            except ValidationError as exc:
                raise NativeNodeError(f"Invalid gate condition: {exc}") from exc
            if parameter is not None:
                parameters = self.flow_graph.flow_settings.parameters
                if parameter not in {p.name for p in parameters}:
                    raise NativeNodeError(
                        f"Gate references flow parameter {parameter!r}, which is not declared; "
                        f"declare it with fl.add_flow_parameter(flow, fl.Parameter({parameter!r}, ...))"
                    )
                _parameter_gate_is_open(gate_input, parameters)
            return input_schema.NodeGate(gate_input=gate_input, else_output=else_output, **base)

        frames = [frame] if control is None else [frame, control]
        self._build("gate", input_schema.NodeGate, frames, make_settings, deferred=None, description=description)

    @property
    def then(self) -> FlowFrame:
        """The exit that is live when the condition holds (``output-0``)."""
        return self._frames[output_handle(0)]

    @property
    def otherwise(self) -> FlowFrame:
        """The exit that is live when the condition does not hold (``output-1``)."""
        if len(self.output_names) < 2:
            raise NativeNodeError(f"Gate {self.node_id} has no else exit; build it with else_output=True")
        return self._frames[output_handle(1)]

    @property
    def else_(self) -> FlowFrame:
        """Alias of :attr:`otherwise`."""
        return self.otherwise

    @property
    def output(self) -> FlowFrame:
        """The ``then`` exit."""
        return self.then

    @property
    def is_open(self) -> bool:
        """Whether the condition holds now.

        A parameter gate evaluates the graph's current parameter values. A formula gate runs
        its probed input (``control``, else the data frame) lazily plus a one-row collect; on
        a deferred frame that input only holds placeholder rows, so it raises instead.
        """
        gate_input = self.node.setting_input.gate_input
        parameters = self.flow_graph.flow_settings.parameters
        if gate_input.condition_source == "parameter":
            return _parameter_gate_is_open(gate_input, parameters)
        if self._probe._deferred:
            raise NativeNodeError(
                f"Gate {self.node_id} probes a frame that only holds placeholder rows until the flow runs; "
                "run the graph (collect a gate exit) to see where it routed"
            )
        typed = typed_parameter_values(parameters)
        try:
            return not self.flow_graph._formula_gate_is_closed(self.node, typed or None)
        except Exception as exc:
            raise NativeNodeError(f"Gate {self.node_id} formula could not be evaluated: {exc}") from exc
