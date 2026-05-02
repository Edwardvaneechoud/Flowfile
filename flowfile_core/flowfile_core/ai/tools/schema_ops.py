"""Schema introspection tools — owned by W30.

Read-only surface so the LLM can ground references in the actual schema
before proposing edits. Reuses ``NodeData.main_output.table_schema`` from
``GET /node?get_data=…`` (``routes.py:978``).

W30 only declares the specs. W31 will implement the executor that resolves
each call against the live ``FlowGraph`` state. MCP-shaped names per D004:
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
            "samples_mode setting (D009); when off, returns schema only."
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
