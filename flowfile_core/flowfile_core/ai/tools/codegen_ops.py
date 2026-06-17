"""Codegen tool surface.

Tools for generating ``polars_code`` / ``python_script`` /
``sql_query`` node bodies. The reverse path (graph → Python) reuses
the existing ``get_generated_flowframe_code()`` and is **not** owned
here — that's a UI surface.

This module declares the specs; the executor runs a 1-row dry-run via
``kernel_runtime`` so the prospective output schema is known before
the GraphDiff stages. MCP-shaped names: ``flowfile.codegen.<op>``.
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
            "generated code on a single sample row before staging (via 1-row dry-run) so downstream "
            "tool calls know the prospective output schema."
        ),
        long_description=(
            "Author a Polars-expression body to feed into 'flowfile.graph.add_polars_code'. "
            "Use when the transform doesn't fit a typed node ('formula' is single-column "
            "only; 'sql_query' is read-only) but doesn't need full Python either. Common "
            "uses: window functions, multi-column derived values, struct unpacking. "
            "Don't use for simple per-row formulas — 'formula' is typed and clearer. "
            "Don't use to issue arbitrary Python — that's 'generate_python_script'. "
            "Pass 'expected_columns' if you can; the executor's 1-row dry-run validates "
            "the actual output and rejects mismatches before staging."
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
            "(via 1-row dry-run) verifies the script does not crash and produces a parsable output schema."
        ),
        long_description=(
            "Author a full Python function body for an isolated sandbox. Use only "
            "when the task genuinely needs imperative Python — multi-step "
            "enrichment, calling a non-Polars library, complex control flow. "
            "Don't use for transforms a Polars expression can express; "
            "'generate_polars_code' is faster and lets the engine optimise. The "
            "sandbox blocks network egress by default — set 'needs_network=true' "
            "only when the script genuinely needs to fetch external data, and "
            "tell the user clearly so they can review before approving. The "
            "1-row dry-run (via 1-row dry-run) discovers the prospective output schema; "
            "supply 'expected_columns' to catch mismatches early."
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
            "verifies the SELECT parses and produces a known schema (via 1-row dry-run)."
        ),
        long_description=(
            "Author a SELECT statement against upstream nodes via Polars' embedded "
            "SQL engine. Use when the user thinks in SQL or when the task is "
            "naturally a multi-table SELECT (multiple JOINs that would otherwise "
            "be a chain of 'add_join' nodes). Each upstream node is an addressable "
            "table. Don't use for write operations — Polars SQL is read-only; "
            "'database_writer' / 'output' handle persistence. Don't use for "
            "windowed CTEs or PostgreSQL-specific features Polars SQL doesn't "
            "support — fall back to 'generate_polars_code'."
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
