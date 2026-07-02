"""NodeSpec: single source of truth for a built-in node type.

One spec bundles what used to live in four hand-maintained catalogs:
the NodeTemplate literal (configs/node_store/nodes.py), the settings-class
map (schemas.NODE_TYPE_TO_SETTINGS_CLASS), the nodes-with-defaults sets,
and the AI classification map (ai/tools/classification._NODE_CLASS_MAP).
Those catalogs are now derived views over the registry.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Literal

from flowfile_core.schemas.schemas import NodeDefault, NodeTemplate

if TYPE_CHECKING:
    from pydantic import BaseModel

NodeAiClassification = Literal["static", "dynamic", "source", "passthrough"]


class InputArity(Enum):
    SOURCE = "source"  # no inputs (readers, manual_input)
    SINGLE = "single"  # one main input
    DOUBLE = "double"  # main + right (join, cross_join, fuzzy_match)
    MULTI = "multi"  # any number of main inputs (union)


@dataclass(frozen=True)
class NodeSpec:
    """Declarative description of one node type.

    ``template`` is None for settings-only types (promise, user_defined,
    datasource); ``drawer_visible=False`` keeps a templated type out of the
    UI drawer list (polars_lazy_frame).
    """

    node_type: str
    settings_class: type[BaseModel] | None
    template: NodeTemplate | None = None
    has_default_settings: bool = False
    ai_classification: NodeAiClassification | None = None
    drawer_visible: bool = True

    @property
    def input_arity(self) -> InputArity:
        if self.template is None:
            return InputArity.SINGLE
        if self.template.multi:
            return InputArity.MULTI
        if self.template.input == 0:
            return InputArity.SOURCE
        if self.template.input == 2:
            return InputArity.DOUBLE
        return InputArity.SINGLE

    @property
    def default(self) -> NodeDefault | None:
        """The NodeDefault view previously built in get_all_standard_nodes."""
        if self.template is None:
            return None
        return NodeDefault(
            node_name=self.template.name,
            node_type=self.template.node_type,
            transform_type=self.template.transform_type,
            has_default_settings=self.has_default_settings,
        )
