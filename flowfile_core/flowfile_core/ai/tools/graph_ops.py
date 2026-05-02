"""Graph-op tool surface — owned by W30.

Hand-written ``ToolSpec`` list for the graph-mutation operations the LLM
calls *outside* of the per-node-type ``flowfile.graph.add_<type>`` family
(those are auto-generated from ``NODE_TYPE_TO_SETTINGS_CLASS`` in
``registry.py``). MCP-shaped naming per D004: ``flowfile.graph.<op>``.

W30 only declares these specs. W31 will implement the executor that maps
each call into the corresponding ``FlowGraph`` method or ``POST /editor/...``
route. Schemas mirror the existing route bodies so the executor can reuse
them without translation.

Plan §4.2 enumerates the surface; this module is the source of truth.
"""

from __future__ import annotations

from typing import Final

from flowfile_core.ai.providers.base import ToolSpec

JSON_SCHEMA_DIALECT: Final[str] = "https://json-schema.org/draft/2020-12/schema"


def _schema(properties: dict, required: list[str]) -> dict:
    """Build a JSON-Schema-2020-12 object schema."""
    return {
        "$schema": JSON_SCHEMA_DIALECT,
        "type": "object",
        "additionalProperties": False,
        "properties": properties,
        "required": required,
    }


GRAPH_OPS_TOOLS: Final[list[ToolSpec]] = [
    ToolSpec(
        name="flowfile.graph.add_node",
        description=(
            "Add a new, unconfigured node ('promise') to the flow graph. "
            "Use this when you need a node to exist before attaching settings — "
            "the per-type tools (e.g. flowfile.graph.add_filter) bundle creation "
            "with settings and are usually preferred."
        ),
        parameters=_schema(
            properties={
                "flow_id": {"type": "integer", "description": "Target flow id."},
                "node_id": {"type": "integer", "description": "Client-generated id for the new node."},
                "node_type": {
                    "type": "string",
                    "description": "Node type from NODE_TYPE_TO_SETTINGS_CLASS (e.g. 'filter', 'join').",
                },
                "pos_x": {"type": "number", "default": 0},
                "pos_y": {"type": "number", "default": 0},
            },
            required=["flow_id", "node_id", "node_type"],
        ),
    ),
    ToolSpec(
        name="flowfile.graph.connect",
        description=(
            "Connect two nodes via a directed edge. Mirrors POST /editor/connect_node/. "
            "input_class defaults to 'input-0' (main input); use 'input-1' / 'input-2' for "
            "join's left/right inputs. output_class defaults to 'output-0'; nodes with "
            "split_mode (e.g. filter) have 'output-0' (pass) and 'output-1' (fail)."
        ),
        parameters=_schema(
            properties={
                "flow_id": {"type": "integer"},
                "from_node_id": {"type": "integer"},
                "to_node_id": {"type": "integer"},
                "input_class": {
                    "type": "string",
                    "enum": ["input-0", "input-1", "input-2"],
                    "default": "input-0",
                },
                "output_class": {
                    "type": "string",
                    "enum": ["output-0", "output-1"],
                    "default": "output-0",
                },
            },
            required=["flow_id", "from_node_id", "to_node_id"],
        ),
    ),
    ToolSpec(
        name="flowfile.graph.update_node_settings",
        description=(
            "Patch an existing node's settings. Pass the full settings object — "
            "the executor validates against the matching Pydantic class for the "
            "node's type and rejects unknown columns against the live upstream schema."
        ),
        parameters=_schema(
            properties={
                "flow_id": {"type": "integer"},
                "node_id": {"type": "integer"},
                "settings": {
                    "type": "object",
                    "description": "Full settings dict matching the node-type's Pydantic class.",
                    "additionalProperties": True,
                },
            },
            required=["flow_id", "node_id", "settings"],
        ),
    ),
    ToolSpec(
        name="flowfile.graph.delete_node",
        description="Remove a node from the flow graph. Mirrors POST /editor/delete_node/.",
        parameters=_schema(
            properties={
                "flow_id": {"type": "integer"},
                "node_id": {"type": "integer"},
            },
            required=["flow_id", "node_id"],
        ),
    ),
    ToolSpec(
        name="flowfile.graph.delete_connection",
        description="Remove an edge between two nodes. Mirrors POST /editor/delete_connection/.",
        parameters=_schema(
            properties={
                "flow_id": {"type": "integer"},
                "from_node_id": {"type": "integer"},
                "to_node_id": {"type": "integer"},
                "input_class": {
                    "type": "string",
                    "enum": ["input-0", "input-1", "input-2"],
                    "default": "input-0",
                },
                "output_class": {
                    "type": "string",
                    "enum": ["output-0", "output-1"],
                    "default": "output-0",
                },
            },
            required=["flow_id", "from_node_id", "to_node_id"],
        ),
    ),
    ToolSpec(
        name="flowfile.graph.run_node",
        description=(
            "Execute a single node and its required upstream chain. " "Mirrors POST /node/trigger_fetch_data."
        ),
        parameters=_schema(
            properties={
                "flow_id": {"type": "integer"},
                "node_id": {"type": "integer"},
            },
            required=["flow_id", "node_id"],
        ),
    ),
    ToolSpec(
        name="flowfile.graph.propose_subgraph",
        description=(
            "Stage a multi-node GraphDiff for atomic accept/reject by the user. "
            "Used by Level 3 (planner agent) — never auto-applies. The executor "
            "validates the entire diff before staging; failures are reported per-tool-call."
        ),
        parameters=_schema(
            properties={
                "flow_id": {"type": "integer"},
                "additions": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "node_id": {"type": "integer"},
                            "node_type": {"type": "string"},
                            "settings": {"type": "object", "additionalProperties": True},
                            "pos_x": {"type": "number", "default": 0},
                            "pos_y": {"type": "number", "default": 0},
                        },
                        "required": ["node_id", "node_type", "settings"],
                    },
                },
                "modifications": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "node_id": {"type": "integer"},
                            "settings": {"type": "object", "additionalProperties": True},
                        },
                        "required": ["node_id", "settings"],
                    },
                },
                "deletions": {
                    "type": "array",
                    "items": {"type": "integer"},
                    "description": "Node ids to delete.",
                },
                "connections_added": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "from_node_id": {"type": "integer"},
                            "to_node_id": {"type": "integer"},
                            "input_class": {
                                "type": "string",
                                "enum": ["input-0", "input-1", "input-2"],
                                "default": "input-0",
                            },
                            "output_class": {
                                "type": "string",
                                "enum": ["output-0", "output-1"],
                                "default": "output-0",
                            },
                        },
                        "required": ["from_node_id", "to_node_id"],
                    },
                },
                "connections_removed": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "from_node_id": {"type": "integer"},
                            "to_node_id": {"type": "integer"},
                        },
                        "required": ["from_node_id", "to_node_id"],
                    },
                },
                "rationale": {
                    "type": "string",
                    "description": "Short user-facing explanation of why this diff achieves the request.",
                },
            },
            required=["flow_id", "rationale"],
        ),
    ),
]


__all__ = ["GRAPH_OPS_TOOLS"]
