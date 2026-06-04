# flowframe/adapters.py
"""Adapters to connect FlowFrame with the flowfile-core library."""

from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.schemas import input_schema, schemas, transform_schema

__all__ = ["FlowGraph", "add_connection", "FlowDataEngine", "input_schema", "schemas", "transform_schema"]
