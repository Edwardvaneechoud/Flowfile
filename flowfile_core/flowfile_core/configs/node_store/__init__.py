from flowfile_core.configs.node_store.user_defined_node_registry import (
    get_all_nodes_from_standard_location,
    load_single_node_from_file,
    unload_node_by_name,
)
from flowfile_core.configs.node_store.nodes import get_all_standard_nodes
from flowfile_core.schemas.schemas import NodeTemplate
from flowfile_core.flowfile.node_designer.custom_node import CustomNodeBase


nodes_with_defaults = {'sample', 'sort', 'union', 'select', 'record_count'}


def register_custom_node(node: NodeTemplate):
    nodes_list.append(node)
    node_dict[node.item] = node


def add_to_custom_node_store(custom_node: type[CustomNodeBase]):
    CUSTOM_NODE_STORE[custom_node().item] = custom_node
    if custom_node().item not in node_dict:
        register_custom_node(custom_node().to_node_template())


def remove_from_custom_node_store(node_key: str) -> bool:
    """
    Remove a custom node from both CUSTOM_NODE_STORE and node registries.

    Args:
        node_key: The key/item name of the node to remove

    Returns:
        True if the node was found and removed, False otherwise
    """
    removed = False

    # Remove from CUSTOM_NODE_STORE
    if node_key in CUSTOM_NODE_STORE:
        del CUSTOM_NODE_STORE[node_key]
        removed = True

    # Remove from node_dict
    if node_key in node_dict:
        del node_dict[node_key]

    # Remove from nodes_list
    for i, node in enumerate(nodes_list):
        if node.item == node_key:
            nodes_list.pop(i)
            break

    # Clean up module cache
    unload_node_by_name(node_key)

    return removed


CUSTOM_NODE_STORE = get_all_nodes_from_standard_location()
nodes_list, node_dict, node_defaults = get_all_standard_nodes()

for custom_node in CUSTOM_NODE_STORE.values():
    register_custom_node(custom_node().to_node_template())


def check_if_has_default_setting(node_item: str):

    return node_item in nodes_with_defaults
