"""Compat shim — the custom-node loader moved to flowfile_core.flowfile.user_defined.registry."""

from flowfile_core.flowfile.user_defined.registry import (
    CustomNodeRegistry,
    LoadedNode,
    load_single_node_from_file,
    registry,
    unload_node_by_name,
)
from shared.node_designer.custom_node import CustomNodeBase

__all__ = [
    "CustomNodeRegistry",
    "LoadedNode",
    "get_all_nodes_from_standard_location",
    "load_single_node_from_file",
    "registry",
    "unload_node_by_name",
]


def get_all_nodes_from_standard_location() -> dict[str, type[CustomNodeBase]]:
    """Healthy registry entries as the legacy ``{node_key: node_class}`` mapping."""
    return {entry.node_key: entry.node_class for entry in registry.all(include_broken=False)}
