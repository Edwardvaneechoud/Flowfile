"""Drift gates: the enum companions and hand-written literals match core's choices exactly."""

import typing

import flowfile_frame as ff
from flowfile_core.flowfile.param_types import ParamType as CoreParamType
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.schemas.schemas import NODE_TYPE_TO_SETTINGS_CLASS
from flowfile_core.schemas.transform_schema import GateOperator as CoreGateOperator
from flowfile_frame.enums import GateOperatorLiteral, NodeType, NodeTypeLiteral, ParamTypeLiteral, _literal

BUILT_IN_TYPES = [t for t in NODE_TYPE_TO_SETTINGS_CLASS if t not in ("promise", "user_defined")]


def test_gate_operator_enum_matches_core_literal():
    assert [m.value for m in ff.GateOperator] == list(typing.get_args(CoreGateOperator))
    assert GateOperatorLiteral is CoreGateOperator


def test_param_type_enum_matches_core_literal():
    assert [m.value for m in ff.ParamType] == list(typing.get_args(CoreParamType))
    assert ParamTypeLiteral is CoreParamType


def test_node_type_literal_matches_settings_classes():
    assert list(typing.get_args(NodeTypeLiteral)) == BUILT_IN_TYPES


def test_node_type_enum_matches_node_type_literal():
    assert [m.value for m in NodeType] == BUILT_IN_TYPES
    assert all(m.name == m.value.upper() for m in NodeType)


def test_every_node_type_has_an_add_method():
    assert [t for t in BUILT_IN_TYPES if not hasattr(FlowGraph, f"add_{t}")] == []


def test_literal_normalises_enum_members_only():
    assert _literal(ff.GateOperator.NOT_IN) == "not_in"
    assert type(_literal(NodeType.SQL_QUERY)) is str
    assert _literal("equals") == "equals"
    assert _literal(3) == 3


def test_gate_stores_the_enum_operator_as_its_string():
    frame = ff.from_dict({"a": [1]})
    ff.add_flow_parameter(frame, ff.Parameter("env", default="prod"))
    gate = ff.Gate(frame, parameter="env", operator=ff.GateOperator.EQUALS, value="prod")
    operator = gate.node.setting_input.gate_input.operator
    assert operator == "equals" and type(operator) is str


def test_add_flow_parameter_stores_the_enum_type_as_its_string():
    frame = ff.from_dict({"a": [1]})
    parameter = ff.add_flow_parameter(frame, ff.Parameter("limit", default=5, type=ff.ParamType.INTEGER))
    assert parameter.type == "integer" and type(parameter.type) is str
    stored = frame.flow_graph.flow_settings.parameters[-1]
    assert stored.type == "integer" and type(stored.type) is str
    assert stored.default_value == "5"
