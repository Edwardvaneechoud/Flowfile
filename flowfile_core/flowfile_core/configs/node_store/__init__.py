from flowfile_core.configs.node_store.user_defined_node_registry import get_all_nodes_from_standard_location
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


CUSTOM_NODE_STORE = get_all_nodes_from_standard_location()
nodes_list, node_dict, node_defaults = get_all_standard_nodes()

for custom_node in CUSTOM_NODE_STORE.values():
    register_custom_node(custom_node().to_node_template())


def check_if_has_default_setting(node_item: str):

    return node_item in nodes_with_defaults
