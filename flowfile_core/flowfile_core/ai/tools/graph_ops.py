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
        long_description=(
            "Almost never the right tool. The typed per-node-type tools "
            "('flowfile.graph.add_filter', 'add_join', etc.) create the node *and* "
            "attach validated settings in a single step; that's what you want in "
            "almost every case. Use 'add_node' only when you genuinely need a "
            "placeholder ('promise') without settings — e.g. to reserve an id "
            "before a downstream connect step that will be retroactively wired. "
            "Don't use to start a flow — use a typed source node like add_read or "
            "add_manual_input instead. If you find yourself reaching for add_node, "
            "stop and check whether a typed alternative already covers the case."
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
        long_description=(
            "Wire one node's output to another node's input. Use after every "
            "'flowfile.graph.add_<type>' call so the new node has data to consume. "
            "Most nodes have a single input ('input-0') and a single output "
            "('output-0') — the defaults match. Reach for the named variants only "
            "when the node has multiple inputs / outputs: 'add_join' takes "
            "input-0 (left) and input-1 (right); a 'filter' node with "
            "split_mode=true emits output-0 (matched rows) and output-1 (rejected). "
            "Don't connect a node to itself; the host rejects self-loops. Example "
            "after 'add_filter': connect(from_node_id=read_id, to_node_id=filter_id) "
            "to give the filter its source rows."
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
        long_description=(
            "Remove a node and its edges from the staged graph. Use when an earlier "
            "step proved unnecessary or when the user explicitly asks to remove "
            "something. Don't use to 'undo' a step you regret — the user reviews "
            "the full diff before anything commits, so it's fine to leave the "
            "redundant node in and explain in the rationale; deleting a node you "
            "just added in the same session is usually noise. Don't delete live "
            "nodes (ones the user already had) without an explicit instruction "
            "from the user."
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
        name="flowfile.graph.delete_connection",
        description="Remove an edge between two nodes. Mirrors POST /editor/delete_connection/.",
        long_description=(
            "Remove a single edge between two nodes; both nodes remain. Use when "
            "rewiring an existing flow — disconnect first, then 'connect' a new "
            "edge. Don't use to remove a node entirely — that's 'delete_node', "
            "which also removes its edges. Specify the same input_class / "
            "output_class that was used to create the connection."
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
