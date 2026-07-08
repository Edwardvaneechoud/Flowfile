"""
Tools for creating custom Flowfile nodes.

Side-effect-free authoring SDK: importable in core, the worker, and dry-run
children without touching flowfile_core (no DB, no migrations). The canonical
user-facing spelling is ``from flowfile import node_designer as nd``.
"""

from shared.node_designer.custom_node import (
    CustomNodeBase,
    NodeSettings,
    NodeSettingsBuilder,
    SectionBuilder,
    create_node_settings,
    create_section,
    to_frontend_schema,
)
from shared.node_designer.secrets import SecretResolutionError, SecretResolver
from shared.node_designer.types import DataType, Types
from shared.node_designer.ui_components import (
    ActionOption,
    AvailableArtifacts,
    AvailableSecrets,
    ColumnActionInput,
    ColumnSelector,
    FlowfileInComponent,
    IncomingColumns,
    MultiSelect,
    NumericInput,
    SecretSelector,
    Section,
    SingleSelect,
    SliderInput,
    TextInput,
    ToggleSwitch,
)

__all__ = [
    # Core Node Class
    "CustomNodeBase",
    # UI Components & Layout
    "Section",
    "TextInput",
    "NumericInput",
    "SliderInput",
    "ToggleSwitch",
    "SingleSelect",
    "MultiSelect",
    "NodeSettings",
    "ColumnSelector",
    "ColumnActionInput",
    "ActionOption",
    "IncomingColumns",
    "AvailableArtifacts",
    "AvailableSecrets",
    "SecretSelector",
    "FlowfileInComponent",
    "Types",
    "DataType",
    # Builders / factories
    "create_section",
    "create_node_settings",
    "SectionBuilder",
    "NodeSettingsBuilder",
    "to_frontend_schema",
    # Secret seam
    "SecretResolver",
    "SecretResolutionError",
]
