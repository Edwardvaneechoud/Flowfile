# flowframe/adapters.py
"""Adapters to connect FlowFrame with the flowfile-core library."""

# Import from your existing project
from flowfile_core.flowfile.FlowfileFlow import EtlGraph, add_connection
from flowfile_core.flowfile.flowfile_table.flowfile_table import FlowfileTable
from flowfile_core.schemas import input_schema, schemas, transform_schema

# Export these for use in FlowFrame
__all__ = [
    'EtlGraph',
    'add_connection',
    'FlowfileTable',
    'input_schema',
    'schemas',
    'transform_schema'
]