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
    # ``flowfile.graph.update_node_settings`` removed from the catalog in W46
    # (2026-05-05). The executor (W31) refused it with "not implemented", so
    # the LLM kept burning retries on a stub. Implementing it properly needs
    # ``GraphDiff.modifications`` (deferred from W41) — tracked under W47.
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
    # ``flowfile.graph.run_node`` removed from the catalog in W46 (2026-05-05).
    # Triggering full node execution autonomously from an LLM is unsafe — could
    # materialise large datasets, run user code, hit external systems. Stays
    # user-driven; not re-added.
    #
    # ``flowfile.graph.propose_subgraph`` removed from the catalog in W46
    # (2026-05-05). Redundant with W40's planner loop — multi-node staging
    # happens naturally via repeated add_/connect_ calls bundled into a
    # ``GraphDiff`` at completion via ``bundle_staged_results``. Not re-added.
]


__all__ = ["GRAPH_OPS_TOOLS"]
