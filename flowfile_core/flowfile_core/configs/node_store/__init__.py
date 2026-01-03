from flowfile_core.configs.node_store.user_defined_node_registry import (
    get_all_nodes_from_standard_location,
    load_single_node_from_file,
    unload_node_by_name,
)
from flowfile_core.configs.node_store.nodes import get_all_standard_nodes
from flowfile_core.schemas.schemas import NodeTemplate
from flowfile_core.flowfile.node_designer.custom_node import CustomNodeBase
import logging

logger = logging.getLogger(__name__)

nodes_with_defaults = {'sample', 'sort', 'union', 'select', 'record_count'}


def register_custom_node(node: NodeTemplate):
    nodes_list.append(node)
    node_dict[node.item] = node


def add_to_custom_node_store(custom_node: type[CustomNodeBase]):
    CUSTOM_NODE_STORE[custom_node().item] = custom_node
    if custom_node().item not in node_dict:
        register_custom_node(custom_node().to_node_template())


def remove_from_custom_node_store(node_key: str, file_stem: str = None) -> bool:
    """
    Remove a custom node from both CUSTOM_NODE_STORE and node registries.

    Args:
        node_key: The key/item name of the node to remove
        file_stem: Optional file name stem (without .py) to use as fallback for matching

    Returns:
        True if the node was found and removed, False otherwise
    """
    removed = False

    logger.info(f"Attempting to remove node with key: '{node_key}' (file_stem: '{file_stem}')")
    logger.info(f"Current CUSTOM_NODE_STORE keys: {list(CUSTOM_NODE_STORE.keys())}")
    logger.info(f"Current nodes_list items: {[n.item for n in nodes_list if hasattr(n, 'item')]}")

    # Try to find the key - use exact match first, then fallback to file_stem
    actual_key = None
    if node_key in CUSTOM_NODE_STORE:
        actual_key = node_key
    elif file_stem and file_stem in CUSTOM_NODE_STORE:
        actual_key = file_stem
        logger.info(f"Using file_stem '{file_stem}' as key instead of '{node_key}'")

    # Remove from CUSTOM_NODE_STORE
    if actual_key and actual_key in CUSTOM_NODE_STORE:
        del CUSTOM_NODE_STORE[actual_key]
        logger.info(f"Removed '{actual_key}' from CUSTOM_NODE_STORE")
        removed = True
    else:
        logger.warning(f"Key '{node_key}' (or file_stem '{file_stem}') not found in CUSTOM_NODE_STORE")

    # Remove from node_dict - try both keys
    key_to_use = actual_key or node_key
    if key_to_use in node_dict:
        del node_dict[key_to_use]
        logger.info(f"Removed '{key_to_use}' from node_dict")
    elif file_stem and file_stem in node_dict:
        del node_dict[file_stem]
        logger.info(f"Removed '{file_stem}' from node_dict")

    # Remove from nodes_list - try both keys
    removed_from_list = False
    for i, node in enumerate(nodes_list):
        if node.item == key_to_use or (file_stem and node.item == file_stem):
            nodes_list.pop(i)
            logger.info(f"Removed '{node.item}' from nodes_list at index {i}")
            removed_from_list = True
            break

    if not removed_from_list:
        logger.warning(f"Key '{node_key}' not found in nodes_list")

    # Clean up module cache
    unload_node_by_name(node_key)
    if file_stem and file_stem != node_key:
        unload_node_by_name(file_stem)

    return removed


CUSTOM_NODE_STORE = get_all_nodes_from_standard_location()
nodes_list, node_dict, node_defaults = get_all_standard_nodes()

for custom_node in CUSTOM_NODE_STORE.values():
    register_custom_node(custom_node().to_node_template())


def check_if_has_default_setting(node_item: str):

    return node_item in nodes_with_defaults
