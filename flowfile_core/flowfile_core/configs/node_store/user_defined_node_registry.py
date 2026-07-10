"""Compat shim — the custom-node loader moved to flowfile_core.flowfile.user_defined.registry."""

from flowfile_core.flowfile.user_defined.registry import (
    CustomNodeRegistry,
    LoadedNode,
    load_single_node_from_file,
    registry,
    unload_node_by_name,
)

__all__ = [
    "CustomNodeRegistry",
    "LoadedNode",
    "load_single_node_from_file",
    "registry",
    "unload_node_by_name",
]
