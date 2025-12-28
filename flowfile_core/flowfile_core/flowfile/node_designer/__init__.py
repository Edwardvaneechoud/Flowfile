# flowfile_core/flowfile/node_designer/__init__.py

"""
Tools for creating custom Flowfile nodes.

This package provides all the necessary components for developers to build their own
custom nodes, define their UI, and implement their data processing logic.
"""

# Import the core base class for creating a new node
from .custom_node import CustomNodeBase, NodeSettings

# Import all UI components so they can be used directly
from .ui_components import (
    Section,
    TextInput,
    NumericInput,
    ToggleSwitch,
    SingleSelect,
    MultiSelect,
    ColumnSelector,
    IncomingColumns,  # Important marker class for dynamic dropdowns
)

# Import the main `Types` object for filtering in ColumnSelector
from flowfile_core.types import Types


# Define the public API of this package
__all__ = [
    # Core Node Class
    "CustomNodeBase",

    # UI Components & Layout
    "Section",
    "TextInput",
    "NumericInput",
    "ToggleSwitch",
    "SingleSelect",
    "MultiSelect",
    "NodeSettings",
    "ColumnSelector",
    "IncomingColumns",

    # Data Type Filtering
    "Types",
]