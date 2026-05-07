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
    # 2026-05-07 — ``flowfile.graph.add_node`` removed. The tool advertised
    # itself as "Almost never the right tool" yet still appeared in the
    # catalog, which made it a hallucination magnet: live dogfood showed the
    # LLM emitting ``add_node`` with ``node_type="node"``, which the executor
    # can only refuse with ``unknown node type: 'node'`` because the dispatcher
    # has no separate promise-handler — every ``add_*`` call routes through
    # ``_handle_add_node`` and demands a real node type. The typed per-type
    # tools (``add_filter`` / ``add_join`` / ``add_group_by`` / etc., all
    # auto-generated in ``registry.py``) cover the legitimate use cases
    # without the placeholder confusion. ``_apply_add_node`` and
    # ``_handle_add_node`` are kept (internal Python plumbing); only the
    # agent-facing ``ToolSpec`` is gone.
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
    ToolSpec(
        name="flowfile.graph.update_node_settings",
        description=(
            "Patch an existing node's settings. Pass the full settings object — "
            "the executor validates against the matching Pydantic class for the "
            "node's type and rejects unknown columns against the live upstream "
            "schema. The node's type and existing wiring are preserved; use "
            "connect / delete_connection to rewire."
        ),
        long_description=(
            "Modify the configuration of a node that already exists in the flow. "
            "Use when the user asks to change a setting on a specific node — "
            "*'show only top 5 rows in node 9'*, *'change the join key to "
            "customer_id'*, *'switch the filter to keep amount > 100'*. The "
            "executor validates the new settings against the node's Pydantic "
            "settings class, runs the same network-egress check as add_* for "
            "code-bearing nodes, validates column references against the live "
            "upstream schema, and predicts the new output schema. Wiring "
            "(upstream / right-input connections) is preserved verbatim — this "
            "tool does NOT rewire the topology. To change which upstream a "
            "node consumes, use 'flowfile.graph.delete_connection' followed by "
            "'flowfile.graph.connect'. Don't reach for this tool to add a new "
            "node ('flowfile.graph.add_<type>' is the right choice) or to "
            "delete one ('flowfile.graph.delete_node'). Pass the FULL settings "
            "dict for the node's type — partial patches are not supported; the "
            "settings object replaces the existing one wholesale."
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
