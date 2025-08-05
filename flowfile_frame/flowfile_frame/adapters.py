# flowframe/adapters.py
"""Adapters to connect FlowFrame with the flowfile-core library."""

# Import from your existing project
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.schemas import input_schema, schema_helpers, transform_schema

# Export these for use in FlowFrame
__all__ = [
    'FlowGraph',
    'add_connection',
    'FlowDataEngine',
    'input_schema',
    'schema_helpers.py',
    'transform_schema'
]