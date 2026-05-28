"""Schema introspection tools.

Read-only surface so the LLM can ground references in the actual
schema before proposing edits. Reuses
``NodeData.main_output.table_schema`` from ``GET /node?get_data=…``.

This module declares the specs; the executor resolves each call
against the live ``FlowGraph`` state. MCP-shaped names:
``flowfile.schema.<op>``.
"""

from __future__ import annotations

from typing import Final

from flowfile_core.ai.providers.base import ToolSpec

JSON_SCHEMA_DIALECT: Final[str] = "https://json-schema.org/draft/2020-12/schema"


def _schema(properties: dict, required: list[str]) -> dict:
    return {
        "$schema": JSON_SCHEMA_DIALECT,
        "type": "object",
        "additionalProperties": False,
        "properties": properties,
        "required": required,
    }


SCHEMA_OPS_TOOLS: Final[list[ToolSpec]] = [
    ToolSpec(
        name="flowfile.schema.read_node_schema",
        description=(
            "Return the column-level schema (name, data type, nullability) of a node's "
            "primary output. Use this before proposing settings that reference columns — "
            "the executor will reject column references not in the upstream schema. "
            "Mirrors GET /node?get_data=false."
        ),
        long_description=(
            "Read the schema (column names + types) of a *live* node — one that "
            "already existed in the flow before this session began. The conversation "
            "context already includes schemas for live nodes when known, so you "
            "usually don't need to call this; reach for it only when the schema "
            "shows 'schema: unknown' in the subgraph and you need real columns to "
            "propose settings. Don't use this on nodes you staged earlier in this "
            "session — those don't exist in the live graph yet. Their predicted "
            "output schema arrives in the 'role=tool' reply on the call that "
            "added them, under 'predicted columns:'. Read once and remember the "
            "answer; repeat reads are wasted tokens."
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
        name="flowfile.schema.read_node_preview",
        description=(
            "Return up to n_rows sample rows from a node's primary output, plus its schema. "
            "Use sparingly — sample rows can be expensive and may contain user PII. "
            "Mirrors GET /node?get_data=true. Sample rows are gated by the per-flow "
            "samples_mode setting; when off, returns schema only."
        ),
        long_description=(
            "Read the schema *and* sample rows from a live node. Use when the "
            "schema alone isn't enough — e.g. you need to confirm a categorical "
            "column's actual values before writing a 'filter' predicate, or to "
            "inspect a date format before a cast. Don't use as a default schema "
            "lookup — 'read_node_schema' is cheaper and the columns are usually "
            "what you need. Don't use on staged nodes (see 'read_node_schema' "
            "guidance). Sample rows may be redacted to '<<redacted>>' or omitted "
            "entirely depending on the flow's samples_mode; never "
            "assume a particular value is present."
        ),
        parameters=_schema(
            properties={
                "flow_id": {"type": "integer"},
                "node_id": {"type": "integer"},
                "n_rows": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 100,
                    "default": 20,
                },
            },
            required=["flow_id", "node_id"],
        ),
    ),
]


__all__ = ["SCHEMA_OPS_TOOLS"]
