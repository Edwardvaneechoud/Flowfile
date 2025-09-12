"""This script runs on run time and checks if all the nodes that are created have a function in the flow_graph as well
as have a component in flowfile_frontend"""

from flowfile_core.schemas import input_schema
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.configs.node_store import nodes_list, NodeTemplate
import inspect


def check_if_node_has_add_function_in_flow_graph(node: NodeTemplate):
    func_name = "add_" + node.item
    if not hasattr(FlowGraph, func_name):
        raise ValueError(
            f"Node {node.name} ({node.item}) does not have a corresponding function in FlowGraph: {func_name}"
            "Check if the function is implemented in flow_graph.py or if the node item is correct."
        )


def check_if_node_has_input_schema_definition(node: NodeTemplate):
    if "node"+node.item.replace("_","") not in {k.lower() for k in inspect.getmodule(input_schema).__dict__.keys()}:
        raise ValueError(
            f"Node {node.name} ({node.item}) does not have a corresponding input schema definition in input_schema.py."
            "Check if the schema is implemented or if the node item is correct."
        )


def validate_setup():
    """
    Validates the setup by checking if all nodes in the nodes_list have a corresponding function in FlowGraph
    and a corresponding input schema definition in input_schema.py.
    Raises ValueError if any node is missing either.
    """
    for node in nodes_list:
        if node.custom_node:
            continue
        check_if_node_has_add_function_in_flow_graph(node)
        check_if_node_has_input_schema_definition(node)


if __name__ == "__main__":
    validate_setup()
