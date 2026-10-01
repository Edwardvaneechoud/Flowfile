import enum
import importlib
import importlib.util

import pytest

import flowfile
import flowfile_frame
from flowfile_frame.custom_nodes import CustomNodes
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
    "custom_node",
    "custom_nodes",
    "PythonScript",
    "python_script",
    "sql",
    "create_flow_graph",
    "FlowGraph",
]

SUBMODULE_ONLY = [
    ("GateOperatorLiteral", "flowfile_frame.enums"),
    ("ParamTypeLiteral", "flowfile_frame.enums"),
    ("NodeTypeLiteral", "flowfile_frame.enums"),
    ("CustomNodeFactory", "flowfile_frame.custom_node"),
    ("CustomNodes", "flowfile_frame.custom_nodes"),
    ("CustomNodeInfo", "flowfile_frame.custom_nodes"),
    ("PythonScriptFunction", "flowfile_frame.python_script"),
]


@pytest.mark.parametrize("name", NATIVE_NODE_API)
def test_symbol_is_exported_by_both_packages(name):
    assert getattr(flowfile_frame, name) is getattr(flowfile, name)


def test_flowfile_all_lists_the_native_node_api():
    assert set(NATIVE_NODE_API) <= set(flowfile.__all__)


@pytest.mark.parametrize("name, module", SUBMODULE_ONLY)
def test_type_only_names_stay_in_their_submodule(name, module):
    assert hasattr(importlib.import_module(module), name)
    assert not hasattr(flowfile, name)
    assert not hasattr(flowfile_frame, name)
    assert name not in flowfile.__all__


def test_node_type_is_the_enum():
    assert issubclass(flowfile.NodeType, enum.Enum)
    assert flowfile.NodeType("sql_query") is flowfile.NodeType.SQL_QUERY
    assert not hasattr(flowfile, "NodeTypes")
    assert not hasattr(flowfile_frame, "NodeTypes")


def test_register_flow_with_catalog_is_importable_from_flowfile_frame_only():
    assert callable(flowfile_frame.register_flow_with_catalog)
    assert not hasattr(flowfile, "register_flow_with_catalog")
    assert "register_flow_with_catalog" not in flowfile.__all__


def test_console_source_is_a_private_module():
    assert importlib.util.find_spec("flowfile_frame.console_source") is None
    assert importlib.util.find_spec("flowfile_frame._console_source") is not None
    assert not hasattr(flowfile_frame, "console_source")


def test_custom_node_is_the_factory_function_not_the_submodule():
    assert callable(flowfile_frame.custom_node)
    assert flowfile_frame.custom_node.__module__ == "flowfile_frame.custom_node"


def test_custom_nodes_is_the_registry_view_not_the_submodule():
    assert isinstance(flowfile_frame.custom_nodes, CustomNodes)
    assert flowfile.custom_nodes is flowfile_frame.custom_nodes


def test_python_script_is_the_decorator_not_the_submodule():
    assert callable(flowfile_frame.python_script)
    assert flowfile_frame.python_script.__module__ == "flowfile_frame.python_script"
    assert flowfile.python_script is flowfile_frame.python_script


def test_flow_frame_has_to_flow_output():
    assert callable(FlowFrame.to_flow_output)


def test_native_node_error_is_a_value_error():
    assert issubclass(flowfile.NativeNodeError, ValueError)
