"""Re-export shim — the custom-node authoring SDK moved to shared/node_designer.

Kept so existing user node files importing ``flowfile_core.flowfile.node_designer``
keep working inside core. New code (and generated nodes) should use
``from flowfile import node_designer as nd``.

Mirrors ``shared.node_designer.__all__`` exactly so both legacy and canonical
spellings expose the same public surface — keep it in lockstep.
"""

from shared.node_designer import (
    ActionOption,
    Artifact,
    AvailableArtifacts,
    AvailableSecrets,
    ColumnActionInput,
    ColumnActionRow,
    ColumnActionValue,
    ColumnSelector,
    CustomNodeBase,
    DataType,
    FlowfileInComponent,
    IncomingColumns,
    MultiSelect,
    NodeSettings,
    NodeSettingsBuilder,
    NumericInput,
    SecretResolutionError,
    SecretResolver,
    SecretSelector,
    Section,
    SectionBuilder,
    SingleSelect,
    SliderInput,
    TextInput,
    ToggleSwitch,
    Types,
    VisibleWhen,
    create_node_settings,
    create_section,
    to_frontend_schema,
)

__all__ = [
    # Core Node Class
    "CustomNodeBase",
    "Artifact",
    # UI Components & Layout
    "Section",
    "VisibleWhen",
    "TextInput",
    "NumericInput",
    "SliderInput",
    "ToggleSwitch",
    "SingleSelect",
    "MultiSelect",
    "NodeSettings",
    "ColumnSelector",
    "ColumnActionInput",
    "ColumnActionRow",
    "ColumnActionValue",
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
