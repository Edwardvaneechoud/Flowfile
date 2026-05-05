"""Meta-op tool surface — owned by W30.

The two-stage agent flow (D002) routes the model's intent through a single
``flowfile.meta.pick_category`` call before exposing the per-category
catalog. W30 declares the spec; the model invocation lives in W40's planner.

The accompanying heuristic fallback in ``registry.pick_category`` lets W31's
executor exercise the two-stage path in tests and degrade gracefully when no
provider is configured.

MCP-shaped names per D004: ``flowfile.meta.<op>``.
"""

from __future__ import annotations

from typing import Final

from flowfile_core.ai.providers.base import ToolSpec

JSON_SCHEMA_DIALECT: Final[str] = "https://json-schema.org/draft/2020-12/schema"

CATEGORY_NAMES: Final[tuple[str, ...]] = (
    "transformations",
    "joins",
    "aggregations",
    "io",
    "code",
    "ml",
    "meta",
    "graph",
)


META_OPS_TOOLS: Final[list[ToolSpec]] = [
    ToolSpec(
        name="flowfile.meta.pick_category",
        description=(
            "First-stage categoriser for the two-stage agent flow (D002). "
            "Given the user's intent, return the single category whose tool surface "
            "best fits the next step. The next call will be issued with only that "
            "category's tools available — pick conservatively. Use 'meta' when you "
            "need more information from the user before acting."
        ),
        long_description=(
            "Internal routing call for the two-stage agent (D002). The host expands "
            "the chosen category into a narrowed tool surface for your next turn. "
            "Categories: 'transformations' (filter/select/sort/formula/etc.), "
            "'joins' (join/cross_join/fuzzy_match/union), 'aggregations' "
            "(group_by/pivot/unpivot/record_count), 'io' (read/output/database/cloud), "
            "'code' (polars_code/python_script/sql_query), 'ml' "
            "(train/apply/evaluate_model), 'meta' (clarify with the user; nothing "
            "is staged), 'graph' (delete/connect — graph mutation outside add_*). "
            "Pick the category that fits the *immediate next step*; you can pick "
            "again on the following turn if the work spans categories. Don't "
            "narrate this call to the user — it's a routing primitive."
        ),
        parameters={
            "$schema": JSON_SCHEMA_DIALECT,
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "intent": {
                    "type": "string",
                    "description": "Short summary of what the user is trying to accomplish next.",
                },
                "rationale": {
                    "type": "string",
                    "description": (
                        "One sentence explaining why this category is the right fit. "
                        "Surfaced to the user when the agent shows its reasoning."
                    ),
                },
                "category": {
                    "type": "string",
                    "enum": list(CATEGORY_NAMES),
                    "description": "The chosen category. Must be one of the enumerated values.",
                },
            },
            "required": ["intent", "category", "rationale"],
        },
    ),
]


__all__ = ["CATEGORY_NAMES", "META_OPS_TOOLS"]
