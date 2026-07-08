import logging

from flowfile_core.configs.node_store.nodes import get_all_standard_nodes
from flowfile_core.configs.node_store.user_defined_node_registry import (
    get_all_nodes_from_standard_location,
    load_single_node_from_file,
    unload_node_by_name,
)
from flowfile_core.flowfile.node_designer.custom_node import CustomNodeBase
from flowfile_core.flowfile.user_defined.registry import LoadedNode, registry
from flowfile_core.schemas.schemas import NodeTemplate

__all__ = [
    "load_single_node_from_file",
    "get_all_nodes_from_standard_location",
    "unload_node_by_name",
    "CustomNodeBase",
    "NodeTemplate",
    "register_custom_node",
    "register_missing_node_template",
    "add_to_custom_node_store",
    "remove_from_custom_node_store",
    "check_if_has_default_setting",
    "registry",
    "CUSTOM_NODE_STORE",
    "nodes_list",
    "node_dict",
    "node_defaults",
    "nodes_with_defaults",
]

logger = logging.getLogger(__name__)

nodes_with_defaults = {"sample", "sort", "union", "select", "record_count"}

CUSTOM_NODE_STORE: dict[str, type[CustomNodeBase]] = {}
nodes_list, node_dict, node_defaults = get_all_standard_nodes()


def register_custom_node(node: NodeTemplate):
    node_dict[node.item] = node
    for i, existing in enumerate(nodes_list):
        if existing.item == node.item:
            nodes_list[i] = node
            return
    nodes_list.append(node)


def register_missing_node_template(node_type: str, inputs: int = 1, outputs: int = 1) -> NodeTemplate:
    """Placeholder template for a custom node type that is not installed.

    Registered in ``node_dict`` only (never the palette) so flows referencing a
    missing node can still build their FlowNodes and render connections.
    """
    existing = node_dict.get(node_type)
    if existing is not None:
        return existing
    template = NodeTemplate(
        name=node_type.replace("_", " ").title(),
        item=node_type,
        input=inputs,
        output=outputs,
        image="user-defined-icon.png",
        multi=True,
        can_be_start=True,
        node_group="custom",
        node_type="process",
        transform_type="wide",
        custom_node=True,
        drawer_title=node_type.replace("_", " ").title(),
        drawer_intro="This custom node is not installed on this machine.",
    )
    node_dict[node_type] = template
    return template


def add_to_custom_node_store(custom_node: type[CustomNodeBase]):
    CUSTOM_NODE_STORE[custom_node().item] = custom_node
    if custom_node().item not in node_dict:
        register_custom_node(custom_node().to_node_template())


def remove_from_custom_node_store(node_key: str, file_stem: str = None) -> bool:
    """Remove a custom node from CUSTOM_NODE_STORE and the palette lists.

    ``file_stem`` is a legacy fallback key kept for callers that only know the
    file name.
    """
    actual_key = node_key if node_key in CUSTOM_NODE_STORE else None
    if actual_key is None and file_stem and file_stem in CUSTOM_NODE_STORE:
        actual_key = file_stem

    removed = False
    if actual_key is not None:
        del CUSTOM_NODE_STORE[actual_key]
        removed = True
    else:
        logger.warning("Key '%s' (or file_stem '%s') not found in CUSTOM_NODE_STORE", node_key, file_stem)

    key_to_use = actual_key or node_key
    if key_to_use in node_dict:
        del node_dict[key_to_use]
    elif file_stem and file_stem in node_dict:
        del node_dict[file_stem]

    for i, node in enumerate(nodes_list):
        if node.item == key_to_use or (file_stem and node.item == file_stem):
            nodes_list.pop(i)
            break

    unload_node_by_name(node_key)
    if file_stem and file_stem != node_key:
        unload_node_by_name(file_stem)

    return removed


def _register_loaded_node(entry: LoadedNode) -> None:
    CUSTOM_NODE_STORE[entry.node_key] = entry.node_class
    if entry.template is not None:
        register_custom_node(entry.template)


def _unregister_loaded_node(entry: LoadedNode) -> None:
    CUSTOM_NODE_STORE.pop(entry.node_key, None)
    node_dict.pop(entry.node_key, None)
    for i, node in enumerate(nodes_list):
        if node.item == entry.node_key:
            nodes_list.pop(i)
            break


registry.on_registered = _register_loaded_node
registry.on_unregistered = _unregister_loaded_node
registry.scan()


def check_if_has_default_setting(node_item: str):
    return node_item in nodes_with_defaults
