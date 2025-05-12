# flowframe/adapters.py
"""Adapters to connect FlowFrame with the flowfile-core library."""

# Import from your existing project
from flowfile_core.flowfile.FlowfileFlow import FlowGraph, add_connection
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.schemas import input_schema, schemas, transform_schema

# Export these for use in FlowFrame
__all__ = [
    'FlowGraph',
    'add_connection',
    'FlowDataEngine',
    'input_schema',
    'schemas',
    'transform_schema'
]