"""Single source of truth for built-in node types.

``BUILTIN_REGISTRY`` holds one NodeSpec per node type; the legacy catalogs
(get_all_standard_nodes, NODE_TYPE_TO_SETTINGS_CLASS, nodes_with_defaults,
the AI classification map) are derived from it.
"""

from flowfile_core.flowfile.node_registry.registry import NodeRegistry
from flowfile_core.flowfile.node_registry.spec import InputArity, NodeSpec

BUILTIN_REGISTRY = NodeRegistry()


def _populate() -> None:
    from flowfile_core.flowfile.node_registry.builtin import ALL_SPECS

    for spec in ALL_SPECS:
        BUILTIN_REGISTRY.register(spec)


_populate()


def get_node_spec(node_type: str) -> NodeSpec | None:
    return BUILTIN_REGISTRY.get(node_type)


__all__ = ["BUILTIN_REGISTRY", "InputArity", "NodeRegistry", "NodeSpec", "get_node_spec"]
