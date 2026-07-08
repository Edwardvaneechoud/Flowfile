"""Re-export shim — the custom-node authoring SDK moved to shared/node_designer.

Kept so existing user node files importing ``flowfile_core.flowfile.node_designer``
keep working inside core. New code (and generated nodes) should use
``from flowfile import node_designer as nd``.
"""

from shared.node_designer import (
    ActionOption,
    AvailableArtifacts,
    AvailableSecrets,
    ColumnActionInput,
    ColumnSelector,
    CustomNodeBase,
    IncomingColumns,
    MultiSelect,
    NodeSettings,
    NumericInput,
    SecretSelector,
    Section,
    SingleSelect,
    SliderInput,
    TextInput,
    ToggleSwitch,
    Types,
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
    "Types",
]
