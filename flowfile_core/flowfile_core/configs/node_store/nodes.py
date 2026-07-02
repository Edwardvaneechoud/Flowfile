"""Legacy node-catalog views, derived from the node registry.

The NodeTemplate literals moved to
``flowfile_core/flowfile/node_registry/builtin/`` (the single source of
truth); this module keeps the historical accessors for existing importers.
"""

from functools import lru_cache

from flowfile_core.schemas.schemas import NodeDefault, NodeTemplate


def get_all_standard_nodes() -> tuple[list[NodeTemplate], dict[str, NodeTemplate], dict[str, NodeDefault]]:
    """
    Initializes and returns the complete list, dict, and defaults for all nodes.
    """
    from flowfile_core.flowfile.node_registry import BUILTIN_REGISTRY

    nodes_list = BUILTIN_REGISTRY.drawer_templates()
    node_dict = BUILTIN_REGISTRY.template_dict()
    node_defaults = BUILTIN_REGISTRY.node_defaults()
    return nodes_list, node_dict, node_defaults


def get_nodes_with_default_settings() -> set[str]:
    """Node types that can be added with auto-generated default settings."""
    from flowfile_core.flowfile.node_registry import BUILTIN_REGISTRY

    return BUILTIN_REGISTRY.node_types_with_default_settings()


@lru_cache(maxsize=1)
def get_source_node_types() -> tuple[str, ...]:
    """Source-only node types (no input port), compiled once at startup from the
    registry. Single source of truth for the connection validator and AI prose;
    excludes the dict-only ``polars_lazy_frame``.
    """
    templates, _, _ = get_all_standard_nodes()
    return tuple(sorted(t.item for t in templates if t.input == 0))


@lru_cache(maxsize=1)
def get_source_node_types_str() -> str:
    """Comma-joined source node types for prose mentions (single build site)."""
    return ", ".join(get_source_node_types())
