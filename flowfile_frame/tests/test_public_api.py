import pytest

import flowfile
import flowfile_frame
from flowfile_frame.flow_frame import FlowFrame

NATIVE_NODE_API = [
    "Gate",
    "Node",
    "NativeNodeError",
    "Parameter",
    "add_flow_parameter",
    "set_flow_parameter",
    "GateOperator",
    "ParamType",
    "NodeTypes",
    "GateOperatorLiteral",
    "ParamTypeLiteral",
    "NodeType",
    "FlowInput",
    "FlowOutput",
    "FlowRef",
    "flow_ref",
    "register_flow",
    "RunFlow",
    "get_catalog",
    "list_catalogs",
    "default_schema",
    "CustomNode",
    "CustomNodeFactory",
    "custom_node",
    "PythonScript",
    "create_flow_graph",
    "FlowGraph",
]


@pytest.mark.parametrize("name", NATIVE_NODE_API)
def test_symbol_is_exported_by_both_packages(name):
    assert getattr(flowfile_frame, name) is getattr(flowfile, name)


def test_flowfile_all_lists_the_native_node_api():
    assert set(NATIVE_NODE_API) <= set(flowfile.__all__)


def test_custom_node_is_the_factory_function_not_the_submodule():
    assert callable(flowfile_frame.custom_node)
    assert flowfile_frame.custom_node.__module__ == "flowfile_frame.custom_node"


def test_flow_frame_has_to_flow_output():
    assert callable(FlowFrame.to_flow_output)


def test_native_node_error_is_a_value_error():
    assert issubclass(flowfile.NativeNodeError, ValueError)
