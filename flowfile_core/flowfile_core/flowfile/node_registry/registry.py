"""Registry of built-in NodeSpecs, with the derived catalog views."""

from __future__ import annotations

from collections.abc import Iterator

from flowfile_core.flowfile.node_registry.spec import NodeSpec
from flowfile_core.schemas.schemas import NodeDefault, NodeTemplate


class NodeRegistry:
    def __init__(self):
        self._specs: dict[str, NodeSpec] = {}

    def register(self, spec: NodeSpec) -> None:
        if spec.node_type in self._specs:
            raise ValueError(f"Node type {spec.node_type!r} is already registered")
        self._specs[spec.node_type] = spec

    def get(self, node_type: str) -> NodeSpec | None:
        return self._specs.get(node_type)

    def __contains__(self, node_type: str) -> bool:
        return node_type in self._specs

    def __iter__(self) -> Iterator[NodeSpec]:
        return iter(self._specs.values())

    def __len__(self) -> int:
        return len(self._specs)

    # -- derived views over the legacy catalogs ---------------------------------

    def settings_class_map(self) -> dict[str, type]:
        return {s.node_type: s.settings_class for s in self if s.settings_class is not None}

    def drawer_templates(self) -> list[NodeTemplate]:
        templates = [s.template for s in self if s.template is not None and s.drawer_visible]
        templates.sort(key=lambda t: t.name)
        return templates

    def template_dict(self) -> dict[str, NodeTemplate]:
        return {s.node_type: s.template for s in self if s.template is not None}

    def node_defaults(self) -> dict[str, NodeDefault]:
        return {s.node_type: s.default for s in self if s.template is not None and s.drawer_visible}

    def node_types_with_default_settings(self) -> set[str]:
        return {s.node_type for s in self if s.has_default_settings}

    def ai_classification_map(self) -> dict[str, str]:
        return {s.node_type: s.ai_classification for s in self if s.ai_classification is not None}
