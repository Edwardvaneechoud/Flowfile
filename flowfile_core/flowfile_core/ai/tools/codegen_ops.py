"""Codegen tool surface — owned by W30.

Tools for generating ``polars_code`` / ``python_script`` / ``sql_query``
node bodies. The reverse path (graph → Python) reuses the existing
``get_generated_flowframe_code()`` (``routes.py:746``,
``code_generator.py:2368``) and is **not** owned here — that is a UI surface.

W30 only declares the specs. W31 implements the executor; per D003 the
executor runs a 1-row dry-run via ``kernel_runtime`` so the prospective
output schema is known before the GraphDiff stages. MCP-shaped names per
D004: ``flowfile.codegen.<op>``.
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


_INTENT_PROP = {
    "type": "string",
    "description": "Natural-language description of what the generated code should do.",
}
_UPSTREAM_PROP = {
    "type": "array",
    "items": {"type": "integer"},
    "description": "Node ids whose outputs are available as named inputs.",
}
_EXPECTED_COLUMNS_PROP = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "data_type": {"type": "string"},
        },
        "required": ["name", "data_type"],
    },
    "description": (
        "Optional: columns the caller expects in the generated node's output. "
        "If supplied, the executor's 1-row dry-run validates the actual output "
        "schema against this list and rejects mismatches."
    ),
}


CODEGEN_OPS_TOOLS: Final[list[ToolSpec]] = [
    ToolSpec(
        name="flowfile.codegen.generate_polars_code",
        description=(
            "Generate the body of a polars_code node. Returns a node-settings payload "
            "ready to stage via flowfile.graph.add_polars_code. The executor runs the "
            "generated code on a single sample row before staging (D003) so downstream "
            "tool calls know the prospective output schema."
        ),
        parameters=_schema(
            properties={
                "intent": _INTENT_PROP,
                "upstream_node_ids": _UPSTREAM_PROP,
                "expected_columns": _EXPECTED_COLUMNS_PROP,
            },
            required=["intent", "upstream_node_ids"],
        ),
    ),
    ToolSpec(
        name="flowfile.codegen.generate_python_script",
        description=(
            "Generate the body of a python_script node. The generated script runs in the "
            "isolated kernel_runtime sandbox (Docker). Returns a node-settings payload ready "
            "to stage via flowfile.graph.add_python_script. The executor's 1-row dry-run "
            "(D003) verifies the script does not crash and produces a parsable output schema."
        ),
        parameters=_schema(
            properties={
                "intent": _INTENT_PROP,
                "upstream_node_ids": _UPSTREAM_PROP,
                "expected_columns": _EXPECTED_COLUMNS_PROP,
                "needs_network": {
                    "type": "boolean",
                    "default": False,
                    "description": (
                        "Set true only if the script genuinely needs outbound network "
                        "access. The user must approve before egress is enabled."
                    ),
                },
            },
            required=["intent", "upstream_node_ids"],
        ),
    ),
    ToolSpec(
        name="flowfile.codegen.generate_sql_query",
        description=(
            "Generate the body of a sql_query node. The generated SQL runs against Polars' "
            "embedded SQL engine over the upstream node outputs. Returns a node-settings "
            "payload ready to stage via flowfile.graph.add_sql_query. Dry-run on one row "
            "verifies the SELECT parses and produces a known schema (D003)."
        ),
        parameters=_schema(
            properties={
                "intent": _INTENT_PROP,
                "upstream_node_ids": _UPSTREAM_PROP,
                "expected_columns": _EXPECTED_COLUMNS_PROP,
            },
            required=["intent", "upstream_node_ids"],
        ),
    ),
]


__all__ = ["CODEGEN_OPS_TOOLS"]
